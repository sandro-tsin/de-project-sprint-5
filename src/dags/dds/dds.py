import logging

import pendulum
from airflow.decorators import dag, task
from dds.dds_users_loader import UsersLoader
from dds.dds_rests_loader import RestsLoader
from dds.dds_times_loader import TimesLoader
from dds.dds_products_loader import ProductsLoader
from dds.dds_orders_loader import OrdersLoader
from dds.dds_sales_loader import SalesLoader
from dds.dds_couriers_loader import CouriersLoader
from dds.dds_deliveries_loader import DeliveriesLoader



from lib import ConnectionBuilder


log = logging.getLogger(__name__)
 
@dag(
    schedule_interval='15,20,25 0 * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['dds', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def dds():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        users_loader = UsersLoader(dwh_pg_connect, log)
        users_loader.load_users()  

    dds_users_loader = load_users()

    @task(task_id="rests_load")
    def load_rests():
        rests_loader = RestsLoader(dwh_pg_connect, log)
        rests_loader.load_rests()

    dds_rests_loader = load_rests()


    @task(task_id="times_load")
    def load_rests():
        times_loader = TimesLoader(dwh_pg_connect, log)
        times_loader.load_times()

    dds_times_loader = load_rests()

    @task(task_id="products_load")
    def load_products():
        products_loader = ProductsLoader(dwh_pg_connect, log)
        products_loader.load_products()

    dds_products_loader = load_products()

    @task(task_id="orders_load")
    def load_orders():
        orders_loader = OrdersLoader(dwh_pg_connect, log)
        orders_loader.load_orders()

    dds_orders_loader = load_orders()


    @task(task_id="sales_load")
    def load_sales():
        sales_loader = SalesLoader(dwh_pg_connect, log)
        sales_loader.load_sales()

    dds_sales_loader = load_sales()

    @task(task_id="couriers_load")
    def load_couriers():
        couriers_loader = CouriersLoader(dwh_pg_connect, log)
        couriers_loader.upload_couriers()

    dds_couriers_loader = load_couriers()


    @task(task_id="deliveries_load")
    def load_deliveries():
        deliveries_loader = DeliveriesLoader(dwh_pg_connect, log)
        deliveries_loader.upload_deliveries()

    dds_deliveries_loader = load_deliveries()

    dds_users_loader >> dds_rests_loader >> dds_times_loader >> dds_products_loader >> dds_couriers_loader >> dds_deliveries_loader >> dds_orders_loader >> dds_sales_loader # type: ignore

dds = dds()


