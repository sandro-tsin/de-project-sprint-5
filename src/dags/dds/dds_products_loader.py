from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


from datetime import date, datetime

class ProductsObj(BaseModel):
    id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: str
    active_to: str


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, users_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                    SELECT *
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
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

class ProductsDestRepository:

    def insert_products(self, conn: Connection, restaurant_id: ProductsObj, product_id: ProductsObj, product_name: ProductsObj, 
                     product_price: ProductsObj, active_from: ProductsObj, active_to: ProductsObj)-> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products (restaurant_id, product_id, 
                        product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, 
                        %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET 
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
                """,
                {
                    "restaurant_id": restaurant_id,
                    "product_id": product_id,
                    "product_name": product_name,
                    "product_price": product_price,
                    "active_from": active_from,
                    "active_to": active_to
                }
            )



class ProductsLoader:
    WF_KEY = "products_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = ProductsOriginRepository(pg)
        self.dds = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_products(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={

                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            
            self.log.info(wf_setting.workflow_settings)
            self.log.info(last_loaded_ts)

            list_rests = self.stg.list_rests()            
            load_queue = self.stg.list_products(last_loaded_ts, self.BATCH_LIMIT)
            
            cat_rest = {}
            for r in list_rests:
                cat_rest[r[1]]=r[0]
            self.log.info(cat_rest)

            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for orders in load_queue:
                rest = str2json(orders[2])['restaurant']
                order_items = str2json(orders[2])['order_items']
                
                for i in order_items:
                    product_id = i['id']
                    product_name = i['name']
                    product_price = i['price']
                    active_from = orders[3]
                    active_to = '2099-12-31 00:00:00.000'
                    restaurant_id = int(cat_rest[rest['id']])
                    
                self.dds.insert_products(conn, restaurant_id, product_id, product_name, product_price,
                                         active_from, active_to)
       


            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[3] for t in load_queue])    
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
