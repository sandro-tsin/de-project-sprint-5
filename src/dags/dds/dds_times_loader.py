from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


from datetime import date, datetime


class TimesObj(BaseModel):
    id: int
    ts: str
    year: int
    month: int
    day: int
    time: str
    date: str


class TimesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_times(self, users_threshold: int, limit: int):
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, update_ts
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





class TimesDestRepository:

    def insert_times(self, conn: Connection, ts: TimesObj, year: TimesObj, month: TimesObj, 
                     day: TimesObj, time: TimesObj, date: TimesObj)-> None:
       with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO UPDATE 
                    SET 
                        year = EXCLUDED.year,
                        month = EXCLUDED.month, 
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date
                """,
                {
                    "ts": ts,
                    "year": year,
                    "month": month,
                    "day": day,
                    "time": time,
                    "date": date
                }
            )



class TimesLoader:
    WF_KEY = "times_stb_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100000

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg_dest = pg
        self.stg = TimesOriginRepository(pg)
        self.dds = TimesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_times(self):
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
            load_queue = self.stg.list_times(last_loaded_ts, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for times in load_queue:
                date = str2json(times[1])['date']
                date = datetime.fromisoformat(date)
                # self.log.info(date)
                # self.log.info(datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S'))
                self.dds.insert_times(conn, date, date.year, date.month, date.day, date.time(), date.date())        


            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[2] for t in load_queue])    
            wf_setting_json = json2str(wf_setting.workflow_settings)
       
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
