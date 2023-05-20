from psycopg import Connection
from lib.dict_util import json2str

from datetime import datetime
from typing import Any


class PgSaver:

    def save_cour(self, conn: Connection, courier_id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.delivery_api_couriers (courier_id, object_value)
                VALUES (%(courier_id)s, %(object_value)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET 
                    object_value = EXCLUDED.object_value
                """,
                {
                    'courier_id': courier_id,
                    'object_value': str_val
                }
            )
            
    def save_del(self, conn: Connection, order_id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.delivery_api_deliveries (order_id, object_value)
                VALUES (%(order_id)s, %(object_value)s)
                ON CONFLICT (order_id) DO UPDATE
                SET 
                    object_value = EXCLUDED.object_value
                """,
                {
                    'order_id': order_id,
                    'object_value': str_val
                }
            )