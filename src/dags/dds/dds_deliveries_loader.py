from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel


from datetime import date, datetime


class DeliveriesObj(BaseModel):
    address: str
    courier_id: str
    delivery_id: str
    delivery_ts: date
    order_id: str
    order_ts: date
    rate: float
    sum: float
    tip_sum: float
   
class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, last_loaded: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, order_id, object_value
                    FROM stg.delivery_api_deliveries
                    WHERE id > %(threshold)s
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": last_loaded,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDestRepository:

    def insert_deliveries(self, conn: Connection, address: DeliveriesObj, courier_id: DeliveriesObj, 
                          delivery_id: DeliveriesObj, delivery_ts: DeliveriesObj, order_id: DeliveriesObj, 
                          order_ts: DeliveriesObj, rate: DeliveriesObj, sum: DeliveriesObj, tip_sum: DeliveriesObj) -> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries (delivery_id, address, courier_id, delivery_ts, 
                                         order_id, order_ts, rate, sum, tip_sum)
                    VALUES (%(delivery_id)s, %(address)s, %(courier_id)s,  %(delivery_ts)s, 
                                %(order_id)s, %(order_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        address = EXCLUDED.address,
                        courier_id = EXCLUDED.courier_id,
                        delivery_ts = EXCLUDED.delivery_ts,
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "delivery_id": delivery_id,
                    "address": address,
                    "courier_id": courier_id,
                    "delivery_ts": delivery_ts,
                    "order_id": order_id,
                    "order_ts": order_ts,
                    "rate": rate,
                    "sum": sum,
                    "tip_sum": tip_sum
                }
            )



class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = DeliveriesOriginRepository(pg)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def upload_deliveries(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            self.log.info(wf_setting.workflow_settings)
            self.log.info(last_loaded)

            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                
                address = str2json(delivery[2])['address']
                courier_id = str2json(delivery[2])['courier_id']
                delivery_id = str2json(delivery[2])['delivery_id']
                delivery_ts = str2json(delivery[2])['delivery_ts']
                order_id = str2json(delivery[2])['order_id']
                order_ts = str2json(delivery[2])['order_ts']
                rate =  str2json(delivery[2])['rate']
                sum = str2json(delivery[2])['sum']
                tip_sum = str2json(delivery[2])['tip_sum']

                self.dds.insert_deliveries(conn, address, courier_id, delivery_id, delivery_ts, 
                                         order_id, order_ts, rate, sum, tip_sum)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([d[0] for d in load_queue])   
            self.log.info(wf_setting) 
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
