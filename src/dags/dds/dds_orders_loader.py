from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel


from datetime import date, datetime

class OrderstsObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: int 
    rate: float
    tip_sum: float


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, orders_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    WITH cte AS 
                    (SELECT 
                        object_value::json ->>'_id' AS order_key,
                        object_value::json ->>'final_status' AS order_status,
                        (object_value::json ->>'restaurant')::json->>'id' AS restaurant_id,
                        object_value::json ->>'date' AS date,
                        (object_value::json ->>'user')::json->>'id' AS user_id
                    FROM stg.ordersystem_orders oo)
                    SELECT 
                            o.order_key, 
                            o.order_status, 
                            dr.id  restaurant_id,
                            dt.id timestamp_id,
                            du.id user_id,
                            dc.id AS courier_id,
                            dd.rate,
                            dd.tip_sum,
                            o.date
                    FROM cte AS o
                    LEFT JOIN dds.dm_restaurants dr ON o.restaurant_id = dr.restaurant_id 
                    LEFT JOIN dds.dm_users du ON o.user_id = du.user_id 
                    LEFT JOIN dds.dm_timestamps dt ON o.date = TO_CHAR(dt.ts, 'YYYY-MM-DD HH24:MI:SS')
                    LEFT JOIN dds.dm_deliveries dd ON dd.order_id = o.order_key
                    LEFT JOIN dds.dm_couriers dc ON dc.courier_id = dd.courier_id 
                    WHERE TO_TIMESTAMP(o.date, 'YYYY-MM-DD HH24:MI:SS') > %(threshold)s
                    LIMIT %(limit)s
                """, {
                    "threshold": orders_threshold,
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
    
    def list_delvieries(self):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT *
                    FROM dds.dm_deliveries
                """
            )
            objs = cur.fetchall()
        return objs 

class OrdersDestRepository:

    def insert_orders(self, conn: Connection, order_key: OrderstsObj, order_status: OrderstsObj, restaurant_id: OrderstsObj, 
                     timestamp_id: OrderstsObj, user_id: OrderstsObj, courier_id: OrderstsObj, rate: OrderstsObj, 
                     tip_sum:OrderstsObj)-> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (order_key, order_status, 
                        restaurant_id, timestamp_id, user_id, courier_id, rate, tip_sum)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, 
                        %(timestamp_id)s, %(user_id)s, %(courier_id)s, %(rate)s,
                        %(tip_sum)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET 
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id,
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "order_key": order_key,
                    "order_status": order_status,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "user_id": user_id,
                    "courier_id": courier_id,
                    "rate": rate,
                    "tip_sum": tip_sum                 
                }
            )


class OrdersLoader:
    WF_KEY = "orders_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 15000

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = OrdersOriginRepository(pg)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_orders(self):
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
            self.log.info(wf_setting.workflow_settings)

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            

            self.log.info(wf_setting.workflow_settings)
            self.log.info(last_loaded_ts)

            # load_rest = self.stg.list_rests()    
            # load_time = self.stg.list_times() 
            # load_users = self.stg.list_users()    
            # load_deliveries = self.stg.list_delvieries()    
            load_queue = self.stg.list_orders(last_loaded_ts, self.BATCH_LIMIT)   

            # cat_time = {}
            # for t in load_time:
            #     cat_time[t[1]]=t[0] 
            
            # cat_rest = {}
            # for r in load_rest:
            #     cat_rest[r[1]]=r[0]
                
            # cat_users = {}
            # for u in load_users:
            #     cat_users[u[1]] = u[0]

            # cat_deliveries = {}
            # for d in load_deliveries:
            #     cat_deliveries[d[5]] = d[3]
            # self.log.info(cat_deliveries)

            
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                order_key = order[0]
                order_status = order[1]
                restaurant_id = order[2]
                timestamp_id = order[3]
                user_id = order[4]
                courier_id = order[5]
                rate = order[6]
                tip_sum = order[7]

                # order_status = str2json(order[2])['final_status']
                # user = str2json(order[2])['user']
                # rest = str2json(order[2])['restaurant']
                # date = datetime.fromisoformat(str2json(order[2])['date'])
                
                self.dds.insert_orders(conn, order_key, order_status, restaurant_id, 
                    timestamp_id, user_id, courier_id, rate, tip_sum)
                
                # self.dds.insert_orders(conn, order_key, order_status, int(cat_rest[rest['id']]), 
                #     cat_time[date], int(cat_users[user['id']]))
                    
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[8] for t in load_queue])
            self.log.info(wf_setting)
               
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
