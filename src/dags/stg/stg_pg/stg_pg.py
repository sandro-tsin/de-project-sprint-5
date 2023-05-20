import logging

import pendulum
from airflow.decorators import dag, task
from stg.stg_pg.ranks_loader import RankLoader
from stg.stg_pg.users_loader import UsersLoader
from stg.stg_pg.events_loader import EventsLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0,5,10 0 * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['stg', 'pg'], 
    is_paused_upon_creation=True  
)

def stg_pg():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rank_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rank_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    ranks_load = load_ranks()

    @task(task_id="users_load")

    def load_users():
        users_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        users_loader.load_users()
    
    users_load = load_users()

    @task(task_id="events_load")

    def load_events():
        events_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        events_loader.load_events()


    events_load = load_events()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    ranks_load  >> users_load >> events_load # type: ignore

stg_pg = stg_pg()
