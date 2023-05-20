from lib import PgConnect
from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.stg_api_dag.pg_saver import PgSaver
from logging import Logger
from stg.stg_api_dag.api_reader import get_cour
from lib.dict_util import json2str



class CourierLoader:

    WF_KEY = "delivery_api_couriers_origin_to_stg_workflow"
    LAST_LOADED_OFFSET = 'last_loaded_offset'

    def __init__(self, pg: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.pg = pg
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def upload_couriers(self):
        with self.pg.connection() as conn: 
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            
            if not wf_setting: 
                wf_setting = EtlSetting(
                    id = 1,
                    workflow_key = self.WF_KEY,
                    workflow_settings = {
                        self.LAST_LOADED_OFFSET: 0
                    }
                )
            self.log.info(wf_setting)
        
            offset = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET]

            load_queue = get_cour(offset)

            if not load_queue:
                self.log.info("No data to load.")
                return 0

            while len(load_queue) > 0:
                for c in load_queue: 
                    self.pg_saver.save_cour(self, conn, c['_id'], c)
                self.log.info(f'Loaded {len(load_queue)} values')
                offset += len(load_queue)
                load_queue = get_cour(offset)

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET] = offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. {wf_setting_json}")
        
     
        
      