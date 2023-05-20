import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.stg_mg.pg_saver import PgSaver
from stg.stg_mg.reader import RestaurantReader, UserReader, OrderReader
from stg.stg_mg.restaurant_loader import RestaurantLoader
from stg.stg_mg.user_loader import UserLoader
from stg.stg_mg.order_loader import OrderLoader



from stg.stg_mg.user_loader import UserLoader

from lib import ConnectionBuilder, MongoConnect


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0,5,10 0 * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False, 
    tags=['stg', 'mango'],  #
    is_paused_upon_creation=True  
)
def stg_mg():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()
        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)
        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()

    @task()
    def load_users():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    user_loader = load_users()

    @task()
    def load_orders():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = OrderReader(mongo_connect)
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    order_loader = load_orders()

    restaurant_loader  >> user_loader >> order_loader


stg_mg = stg_mg()  # noqa


