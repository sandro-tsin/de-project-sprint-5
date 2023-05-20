from pydantic import BaseModel
from lib import PgConnect
from psycopg import Connection
from cdm.cdm_setting_repository import DdsEtlSettingsRepository, EtlSetting
from logging import Logger
from lib.dict_util import json2str
from datetime import datetime, timedelta



class SettlementObj(BaseModel):
    id: int
    restaurant_id: int
    restaurant_name: str
    orders_count: int 
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float 
    order_processing_fee: float
    restaurant_reward_sum: float

class SettlementRep: 

    def load_settlement(self, conn: Connection, threshold: str, limit: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 	
                        dr.restaurant_id, 
                        dr.restaurant_name, 
                        date settlement_date,
                        count(DISTINCT order_key) orders_count, 
                        sum(fps.count*fps.price) orders_total_sum,
                        sum(bonus_payment) orders_bonus_payment_sum, 
                        sum(bonus_grant) orders_bonus_granted_sum, 
                        sum((fps.count*fps.price)*0.25) order_processing_fee,
                        sum((fps.count*fps.price)-(fps.count*fps.price)*0.25 - bonus_payment) restaurant_reward_sum
                FROM dds.fct_product_sales fps 
                    LEFT JOIN dds.dm_orders do2 ON do2.id = fps.order_id 
                    LEFT JOIN dds.dm_restaurants dr ON dr.id = do2.restaurant_id 
                    LEFT JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id 
                    WHERE date > %(threshold)s
                    GROUP BY date, dr.restaurant_id, dr.restaurant_name
                    ORDER BY date asc
                    LIMIT %(limit)s
                """, {
                        "threshold": threshold,
                        "limit": limit
                    }
            )
            objs = cur.fetchall()
        return objs
    
    # def get_last_id(self, conn, threshold:int, limit: int):
    #     with conn.cursor() as cur:
    #         cur.execute(
    #         """
    #             SELECT id
    #             FROM dds.fct_product_sales fps 
    #             WHERE id> %(threshold)s
    #             ORDER BY id asc
    #             LIMIT %(limit)s; 
    #         """, {
    #             "limit": limit,
    #             "threshold": threshold
    #              }
    #         )
    #         objs = cur.fetchall()
    #     return objs
        

    def insert_settlement(self, conn: Connection, restaurant_id: BaseModel, restaurant_name: BaseModel,
                        settlement_date: BaseModel, orders_count: BaseModel,
                        orders_total_sum: BaseModel, orders_bonus_payment_sum: BaseModel, 
                        orders_bonus_granted_sum: BaseModel, order_processing_fee: BaseModel, 
                        restaurant_reward_sum: BaseModel) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, 
                            settlement_date, orders_count,
                            orders_total_sum, orders_bonus_payment_sum, 
                            orders_bonus_granted_sum, order_processing_fee, 
                            restaurant_reward_sum)	
                VALUES (%(restaurant_id)s, %(restaurant_name)s, 
                            %(settlement_date)s, %(orders_count)s,
                            %(orders_total_sum)s, %(orders_bonus_payment_sum)s, 
                            %(orders_bonus_granted_sum)s, %(order_processing_fee)s, 
                            %(restaurant_reward_sum)s)
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE 
                SET 
                    restaurant_name = EXCLUDED.restaurant_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum, 
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum, 
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum, 
                    order_processing_fee = EXCLUDED.order_processing_fee, 
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "settlement_date": settlement_date,
                    "orders_count": orders_count,
                    "orders_total_sum": orders_total_sum,
                    "orders_bonus_payment_sum": orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": orders_bonus_granted_sum,
                    "order_processing_fee": order_processing_fee,
                    "restaurant_reward_sum": restaurant_reward_sum
                },
            )


class SettlmentsLoader:

    WF_KEY = "settlement_dds_to_cdm_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50
    
    def __init__(self, pg: PgConnect, log: Logger):
        self.pg = pg
        self.cdm = SettlementRep()
        self.log = log
        self.setting_repository = DdsEtlSettingsRepository()

    def load_settl(self):
        with self.pg.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={

                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            self.log.info(wf_setting)

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str) - timedelta(days=1)
           


            load_queue = self.cdm.load_settlement(conn, last_loaded_ts, self.BATCH_LIMIT)   

            if not load_queue:
                self.log.info("Quitting.")
                return
            
            self.log.info(f'Found {len(load_queue)} to load')

            for s in load_queue:
                restaurant_id = s[0]
                restaurant_name = s[1]
                settlement_date = s[2]
                orders_count = s[3]
                orders_total_sum = s[4]
                orders_bonus_payment_sum = s[5]
                orders_bonus_granted_sum = s[6]
                order_processing_fee = s[7]
                restaurant_reward_sum = s[8]
                self.cdm.insert_settlement(conn, restaurant_id, restaurant_name, 
                            settlement_date, orders_count,
                            orders_total_sum, orders_bonus_payment_sum, 
                            orders_bonus_granted_sum, order_processing_fee, 
                            restaurant_reward_sum)         

            self.log.info(f'Loaded {len(load_queue)} items')

         
            
            # self.cdm.load_settlement(conn, last_loaded, self.BATCH_LIMIT)

            
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = str(max([d[2] for d in load_queue]))
            self.log.info(wf_setting)

            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.setting_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
