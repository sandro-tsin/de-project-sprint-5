from lib import PgConnect
from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.stg_api_dag.pg_saver import PgSaver
from logging import Logger
from stg.stg_api_dag.api_reader import get_del
from lib.dict_util import json2str
from datetime import date, timedelta, datetime


class DeliveryLoader:

    WF_KEY = "delivery_api_deliveries_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = 'last_loaded_ts'

    def __init__(self, pg: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.pg = pg
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def upload_deliveries(self):
        with self.pg.connection() as conn: 
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            
            if not wf_setting: 
                wf_setting = EtlSetting(
                    id = 1,
                    workflow_key = self.WF_KEY,
                    workflow_settings = {
                        self.LAST_LOADED_TS_KEY: (date.today() - timedelta(days=7)).isoformat()
                    }
                )
            self.log.info(wf_setting)
        
            last_loaded_ts = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            date_from = datetime.fromisoformat(last_loaded_ts).strftime("%Y-%m-%d 00:00:00")
            date_to = date.today().strftime("%Y-%m-%d 00:00:00")

            self.log.info(date_from)
            self.log.info(date_to)

            offset=0
            load_queue = get_del(offset, date_from, date_to)

            if not load_queue:
                self.log.info("No data to load.")
                return 0

            while len(load_queue) > 0:
                for d in load_queue: 
                    self.pg_saver.save_del(self, conn, str(d['order_id']), d)

                self.log.info(f'Loaded {len(load_queue)} values')
                offset += len(load_queue)
                max_t = ([max(t['order_ts'] for t in load_queue)])
                max_t_str = datetime.strptime(max_t[0], "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
                load_queue = get_del(offset, date_from, date_to)
                

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_t_str

            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.log.info(wf_setting_json)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. {wf_setting_json}")
        
     
        
      