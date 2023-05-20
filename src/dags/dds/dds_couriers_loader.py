from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel


from datetime import date, datetime


class CouriersObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str

class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, last_loaded: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, courier_id, object_value
                    FROM stg.delivery_api_couriers
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


class CouriersDestRepository:

    def insert_couriers(self, conn: Connection, courier_id: CouriersObj, courier_name: CouriersObj) -> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers (courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name
                """,
                {
                    "courier_id": courier_id,
                    "courier_name": courier_name
                }
            )



class CouriersLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = CouriersOriginRepository(pg)
        self.dds = CouriersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def upload_couriers(self):
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

            load_queue = self.stg.list_couriers(last_loaded, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier in load_queue:
                courier_id = str2json(courier[2])['_id']
                courier_name = str2json(courier[2])['name']

                self.dds.insert_couriers(conn, courier_id, courier_name)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])   
            self.log.info(wf_setting) 
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
