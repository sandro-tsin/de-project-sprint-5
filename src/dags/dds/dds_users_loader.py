from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


from datetime import date, datetime


class UsersObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, users_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, update_ts
                    FROM stg.ordersystem_users
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


class UsersDestRepository:

    def insert_users(self, conn: Connection, user_id: BaseModel, user_name: BaseModel, user_login: BaseModel) -> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_login": user_login
                }
            )



class UsersLoader:
    WF_KEY = "users_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 1000 

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = UsersOriginRepository(pg)
        self.dds = UsersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_users(self):
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
            load_queue = self.stg.list_users(last_loaded_ts, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for users in load_queue:
                users = str2json(users[1])
                id = users['_id']
                login = users['login']
                name = users['name']

                self.dds.insert_users(conn, id, name, login)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[2] for t in load_queue])    
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
