from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


from datetime import date, datetime


class RestsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: str
    active_to: str


class RestsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_rests(self, rest_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(threshold)s
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": rest_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestsDestRepository:

    def insert_users(self, conn: Connection, restaurant_id: BaseModel, restaurant_name: BaseModel, 
                     active_from: BaseModel, active_to: BaseModel) -> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "active_from": active_from,
                    "active_to": active_to
                }
            )



class RestsLoader:
    WF_KEY = "rests_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = RestsOriginRepository(pg)
        self.dds = RestsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_rests(self):
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

            load_queue = self.stg.list_rests(last_loaded_ts, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for rest in load_queue:
                rest = str2json(rest[1])
                restaurant_id = rest['_id']
                restaurant_name = rest['name']
                active_from = rest['update_ts']
                active_to = '2099-12-31 00:00:00.000'
                
            
                self.dds.insert_users(conn, restaurant_id, restaurant_name, active_from, active_to)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[2] for t in load_queue])    
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
