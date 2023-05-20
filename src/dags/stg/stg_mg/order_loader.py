from datetime import datetime
from logging import Logger

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.stg_mg.pg_saver import PgSaver
from stg.stg_mg.reader import OrderReader
from lib import PgConnect
from lib.dict_util import json2str

from datetime import date, timedelta


class OrderLoader:
    _LOG_THRESHOLD = 100
    _SESSION_LIMIT = 10000

    WF_KEY = "ordersystem_orders_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: OrderReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:

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
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_orders(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from orders collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_order(conn, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing orders.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)