from datetime import datetime
import time
import json
import logging

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from psycopg2.extras import RealDictCursor

from settings import DBFileds, SQLiteDBTables, MOVIES_UPDATED_STATE_KEY


def sqlite_get_updated_movies_ids(ti: TaskInstance, **context):
    """Сбор обновленных записей в таблице с фильмами"""
    logging.info(f'sqlite_get_updated_movies_ids; context= , {context["params"]}')

    query = f"""
        SELECT id, updated_at
        FROM {SQLiteDBTables.film.value}
        WHERE updated_at >= ?
        ORDER BY updated_at
        LIMIT {context["params"]["chunk_size"]}
        """

    updated_state = ti.xcom_pull(
        key=MOVIES_UPDATED_STATE_KEY, include_prior_dates=True
    ) or str(datetime.min)
    updated_state_sqlite = time.mktime(
        datetime.strptime(updated_state[:25], "%Y-%m-%d %H:%M:%S.%f").timetuple())
    msg = f"updated_state_sqlite = {updated_state_sqlite}, {type(updated_state_sqlite)}"
    logging.info(msg)

    sqlite_hook = SqliteHook(sqlite_conn_id=context["params"]["in_db_id"])
    msg = f"sqlite_hook = {str(sqlite_hook)}, {str(type(sqlite_hook))}"
    logging.info(msg)
    sqlite_con = sqlite_hook.get_conn()
    sqlite_cur = sqlite_con.cursor()
    msg = f"sqlite_cur = {str(sqlite_cur)}, {str(type(sqlite_cur))}"
    logging.info(msg)

    sqlite_cur.execute(query, (updated_state_sqlite,))
    sqlite_tuples_list = sqlite_cur.fetchall()
    sqlite_dict_list = [dict(zip(['id', 'updated_at'], tuple)) for tuple in sqlite_tuples_list]

    msg = f"sqlite_items = {str(sqlite_dict_list)}, {str(type(sqlite_dict_list))}"
    logging.info(f'SQLITE_CURSOR SUCCESS;= {msg}')

    if sqlite_dict_list:
        ti.xcom_push(key=MOVIES_UPDATED_STATE_KEY, value=str(sqlite_dict_list[-1]["updated_at"]))
    return set([x["id"] for x in sqlite_dict_list])


def sqlite_get_films_data(ti: TaskInstance, **context):
    """Сбор агрегированных данных по фильмам"""
    logging.info(f'def sqlite_get_films_data= {ti}, {context}')

def sqlite_preprocess(ti: TaskInstance, **context):
    """Трансформация данных"""
    logging.info(f'def sqlite_preprocess= {ti}, {context}')

def sqlite_write(ti: TaskInstance, **context):
    """Запись данных"""
    logging.info(f'sqlite_write= {ti}, {context}')
