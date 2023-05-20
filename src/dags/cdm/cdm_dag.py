from airflow.decorators import dag, task
import logging
log = logging.getLogger(__name__)
from datetime import datetime

from lib import ConnectionBuilder
from cdm.settlement_report import SettlmentsLoader
from cdm.courier_ledger import LedgerLoader



@dag(
    schedule_interval = '30,35,40 0 * * *',
    start_date = datetime(2022, 1, 1),
    catchup = False, 
    tags = ['cdm']
)


def cdm_dag():
    pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")


    @task()
    def cdm_settlemant():
        loader = SettlmentsLoader(pg_connect, log)
        loader.load_settl()
    
    cdm_settlemant = cdm_settlemant()

    @task()
    def cdm_ledger():
        loader = LedgerLoader(pg_connect, log)
        loader.load_ledg()
    
    cdm_ledger = cdm_ledger()

    cdm_settlemant >> cdm_ledger

dag = cdm_dag()


