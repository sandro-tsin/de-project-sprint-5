from airflow.decorators import dag, task
from datetime import datetime
from lib import ConnectionBuilder
from stg.stg_api_dag.courier_loader import CourierLoader
from stg.stg_api_dag.delivery_loader import DeliveryLoader

from stg.stg_api_dag.pg_saver import PgSaver


import logging


log = logging.getLogger(__name__)

@dag(
    schedule_interval='0,5,10 0 * * *', 
    start_date=datetime(2021, 12, 1), 
    catchup=False,
    tags = ['stg', 'api']
)

def stg_api():
    dwh_connect = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

    @task()
    def courier_loader():
        loader = CourierLoader(dwh_connect, PgSaver, log)
        loader.upload_couriers()
    
    courier_loader = courier_loader()

    @task()
    def delivery_loader():
        loader = DeliveryLoader(dwh_connect, PgSaver, log)
        loader.upload_deliveries()
    
    delivery_loader = delivery_loader()

    courier_loader >> delivery_loader
    

stg_api = stg_api()