from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel


from datetime import date, datetime, timedelta

class SalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg


    def list_bonus(self, users_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction'
                    AND event_ts > %(threshold)s;
                """,
                {
                    "threshold": users_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs 

    def list_rests(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                    SELECT *
                    FROM dds.dm_restaurants
                """
            )
            objs = cur.fetchall()
        return objs
    
    def list_times(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_timestamps
                """
            )
            objs = cur.fetchall()
        return objs 
    
    def list_users(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_users
                """
            )
            objs = cur.fetchall()
        return objs 
    
    def list_orders(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_orders
                """
            )
            objs = cur.fetchall()
        return objs 
    
    def list_products(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_products
                """
            )
            objs = cur.fetchall()
        return objs 
    



class SalesDestRepository:

    def insert_sales(self, conn: Connection, product_id: SalesObj, order_id: SalesObj, count: SalesObj, 
                price: SalesObj, total_sum: SalesObj, bonus_payment: SalesObj, bonus_grant: SalesObj)-> None:
       with conn.cursor() as cur:
            cur.execute( """
                    INSERT INTO dds.fct_product_sales (product_id, order_id, count, 
                        price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, 
                        %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);
                """,
                {
                    "product_id": product_id,
                    "order_id": order_id,
                    "count": count,
                    "price": price,
                    "total_sum": total_sum,
                    "bonus_payment": bonus_payment,
                    'bonus_grant': bonus_grant
                }
            )


class SalesLoader:
    WF_KEY = "sales_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = SalesOriginRepository(pg)
        self.dds = SalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_sales(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={

                        self.LAST_LOADED_TS_KEY: (date.today() - timedelta(days=7)).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            
            self.log.info(wf_setting.workflow_settings)
            self.log.info(last_loaded_ts)



            load_rest = self.stg.list_rests()
            load_time = self.stg.list_times() 
            load_users = self.stg.list_users()  
            load_orders = self.stg.list_orders()
            load_products = self.stg.list_products()
            bonus_events = self.stg.list_bonus(last_loaded_ts, self.BATCH_LIMIT)

            cat_time = {}
            for t in load_time:
                cat_time[t[1]]=t[0] 
            
            cat_rest = {}
            for r in load_rest:
                cat_rest[r[1]]=r[0]
                
            cat_users = {}
            for u in load_users:
                cat_users[u[1]] = u[0]
                
            cat_ord = {}
            for o in load_orders:
                cat_ord[o[1]] = o[0]
            
            cat_prod = {}
            for p in load_products:
                cat_prod[p[2]] = p[0]
                

            self.log.info(f"Found {len(bonus_events)} users to load.")
            if not bonus_events:
                self.log.info("Quitting.")
                return
            
            cat_bonus = {}
            for b in bonus_events:
                cat_bonus_inner = {}
                order_id = str2json(b[3])['order_id']
                for pib in str2json(b[3])["product_payments"]:
                    cat_bonus_inner[pib['product_id']] = [pib['bonus_payment'], pib['bonus_grant']]
                cat_bonus[order_id] = cat_bonus_inner

            for b in bonus_events:
                order_id = str2json(b[3])['order_id']
                order_id_i = cat_ord[order_id]
                product_payments = str2json(b[3])['product_payments']
                for p in product_payments:
                    product_id = cat_prod[p['product_id']]
                    count = p['quantity']
                    price = p['price']
                    total_sum = p['quantity'] * p['price']
                    bonus_payment = p['bonus_payment']
                    bonus_grant = p['bonus_grant']
                    self.dds.insert_sales(conn, product_id, order_id_i, count, 
                        price, total_sum, bonus_payment, bonus_grant)
                    
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[1] for t in bonus_events])    
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
