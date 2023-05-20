from pydantic import BaseModel
from lib import PgConnect
from psycopg import Connection
from cdm.cdm_setting_repository import DdsEtlSettingsRepository, EtlSetting
from logging import Logger
from lib.dict_util import json2str
from datetime import datetime, timedelta



class LedgerObj(BaseModel):
    id: int
    courier_id: int
    courier_name: str 
    settlement_year: int
    settlement_month: int
    orders_count: int 
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class LedgerRep: 

    def load_ledger(self, conn: Connection, threshold_m: int, threshold_y: int, limit: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH cte AS 
                (WITH rate_count AS (
                    SELECT 
                            do2.courier_id,
                            dt."year",
                            dt."month",
                            avg(do2.rate) rate_avg,
                            count(order_key) orders_count,
                            sum(tip_sum) tip_sum
                    FROM dds.dm_orders do2 
                    LEFT JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
                    GROUP BY courier_id, dt."month", dt."year"),
                sales AS (
                    SELECT 
                            order_id, 
                            sum(total_sum) total_sum
                    FROM dds.fct_product_sales fps 
                    GROUP BY order_id 
                )
                SELECT 
                        fps.order_id,
                        fps.total_sum,
                        dt2."year",
                        dt2."month",
                        do2.courier_id,
                        dc.courier_name,
                        r.rate_avg, 
                        r.orders_count,
                        r.tip_sum,
                        CASE 
                            WHEN r.rate_avg < 4 THEN GREATEST(fps.total_sum*0.05, 100) 
                            WHEN r.rate_avg < 4.5 AND r.rate_avg >=4  THEN GREATEST(fps.total_sum*0.07, 150)
                            WHEN r.rate_avg < 4.9 AND r.rate_avg >=4.5  THEN GREATEST(fps.total_sum*0.08, 175)
                            WHEN r.rate_avg >= 4.9 THEN GREATEST(fps.total_sum*0.1, 200)			
                            ELSE 100
                        END courier_order
                FROM sales fps 
                LEFT JOIN dds.dm_orders do2 ON fps.order_id = do2.id
                LEFT JOIN dds.dm_timestamps dt2 ON do2.timestamp_id = dt2.id 
                LEFT JOIN rate_count r ON r."year" = dt2."year" AND r."month" = dt2."month" AND do2.courier_id = r.courier_id
                LEFT JOIN dds.dm_couriers dc ON dc.id = r.courier_id
                WHERE do2.courier_id IS NOT NULL)
                SELECT 
                        courier_id, 
                        courier_name,
                        "year", 
                        "month",
                        max(orders_count) orders_count,
                        sum(total_sum) orders_total_sum,
                        max(rate_avg) rate_avg,
                        (sum(total_sum)* 0.25) order_processing_fee,
                        sum(courier_order) courier_order_sum,
                        max(tip_sum) courier_tips_sum,
                        sum(courier_order)+(max(tip_sum)*0.95) courier_reward_sum
                FROM cte c
                WHERE "year" >= %(threshold_y)s AND "month" >= %(threshold_m)s
                GROUP BY courier_id, courier_name,  "year", "month"
                    LIMIT %(limit)s
                """, {
                        "threshold_m": threshold_m,
                        "threshold_y": threshold_y,
                        "limit": limit
                    }
            )
            objs = cur.fetchall()
        return objs
        


    def insert_ledger(self, conn: Connection, courier_id: LedgerObj, 
                        courier_name: LedgerObj, settlement_year: LedgerObj,
                        settlement_month: LedgerObj, orders_count: LedgerObj, 
                        orders_total_sum: LedgerObj, rate_avg: LedgerObj, 
                        order_processing_fee: LedgerObj, courier_order_sum: LedgerObj, 
                        courier_tips_sum: LedgerObj, courier_reward_sum: LedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year,
                            settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee,
                            courier_order_sum, courier_tips_sum, courier_reward_sum)	
                VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s,
                            %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, 
                            %(rate_avg)s, %(order_processing_fee)s,
                            %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE 
                SET 
                    courier_name = EXCLUDED.courier_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum, 
                    rate_avg = EXCLUDED.rate_avg, 
                    order_processing_fee = EXCLUDED.order_processing_fee, 
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": courier_id,
                    "courier_name": courier_name,
                    "settlement_year": settlement_year,
                    "settlement_month": settlement_month,
                    "orders_count": orders_count,
                    "orders_total_sum": orders_total_sum,
                    "rate_avg": rate_avg,
                    "order_processing_fee": order_processing_fee,
                    "courier_order_sum": courier_order_sum,
                    "courier_tips_sum": courier_tips_sum,
                    "courier_reward_sum": courier_reward_sum
                },
            )


class LedgerLoader:

    WF_KEY = "ledger_dds_to_cdm_workflow"
    LAST_LOADED_MONTH_KEY = "last_loaded_month"
    LAST_LOADED_YEAR_KEY = "last_loaded_year"
    BATCH_LIMIT = 1000
    
    def __init__(self, pg: PgConnect, log: Logger):
        self.pg = pg
        self.cdm = LedgerRep()
        self.log = log
        self.setting_repository = DdsEtlSettingsRepository()

    def load_ledg(self):
        with self.pg.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={

                        self.LAST_LOADED_MONTH_KEY: 1,
                        self.LAST_LOADED_YEAR_KEY: 2022
                    }
                )

            self.log.info(wf_setting)

            last_loaded_month = wf_setting.workflow_settings[self.LAST_LOADED_MONTH_KEY]
            last_loaded_year = wf_setting.workflow_settings[self.LAST_LOADED_YEAR_KEY]
            
           


            load_queue = self.cdm.load_ledger(conn, last_loaded_month, last_loaded_year, self.BATCH_LIMIT)   

            if not load_queue:
                self.log.info("Quitting.")
                return
            
            self.log.info(f'Found {len(load_queue)} to load')




            for l in load_queue:
                courier_id = l[0]
                courier_name = l[1]
                settlement_year = l[2]
                settlement_month = l[3]
                orders_count = l[4]
                orders_total_sum = l[5]
                rate_avg = l[6]
                order_processing_fee = l[7]
                courier_order_sum = l[8]
                courier_tips_sum = l[9]
                courier_reward_sum = l[10]

                self.cdm.insert_ledger(conn, courier_id, courier_name, settlement_year,
                            settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee,
                            courier_order_sum, courier_tips_sum, courier_reward_sum)         

            self.log.info(f'Loaded {len(load_queue)} items')

         
            

            
            wf_setting.workflow_settings[self.LAST_LOADED_MONTH_KEY] = (max([l[3] for l in load_queue]))
            wf_setting.workflow_settings[self.LAST_LOADED_YEAR_KEY] = (max([l[2] for l in load_queue]))
           
            self.log.info(wf_setting)

            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.setting_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
