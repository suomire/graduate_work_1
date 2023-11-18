import sys
import pathlib
from datetime import datetime
import time
import json
import logging
import sqlite3

from contextlib import contextmanager, closing

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook

from settings import DBFileds, SQLiteDBTables, MOVIES_UPDATED_STATE_KEY


@contextmanager
def conn_context(db_name: str):
    if 'out' in db_name:
        db_path = db_name
    else:
        db_path = '/db/' + db_name # путь до каталога, где лежит скрипт
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row # row_factory - данные в формате «ключ-значение»
    yield conn
    conn.close()


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
    ) or str(datetime.min)+'.0'
    logging.info(f'{ti.xcom_pull(key=MOVIES_UPDATED_STATE_KEY, include_prior_dates=True)=}')
    logging.info(f'{str(datetime.min)=}')
    logging.info(f'{updated_state=}, {type(updated_state)=}')
    updated_state_sqlite = time.mktime(
        datetime.strptime(updated_state[:25], "%Y-%m-%d %H:%M:%S.%f").timetuple())
    msg = f"{updated_state_sqlite=}, {type(updated_state_sqlite)=}"
    logging.info(msg)

    #имя файла базы данных из Admin-Connections-Schema
    db_name = BaseHook.get_connection(context["params"]["in_db_id"]).schema
    logging.info(f"{db_name=}")

    with conn_context(db_name) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, (updated_state_sqlite,))
                # cursor.execute("""select * from person;""")
                data = cursor.fetchall()
                data_dict = [dict(i) for i in data]
                logging.info(f'{data_dict=}')
            except Exception as err:
                logging.error(f'<<SELECT ERROR>> {err}')

    if data_dict:
        ti.xcom_push(key=MOVIES_UPDATED_STATE_KEY, value=str(data_dict[-1]["updated_at"]))
    return set([x["id"] for x in data_dict])


def sqlite_get_films_data(ti: TaskInstance, **context):
    """Сбор агрегированных данных по фильмам"""
    FILEDS2SQL = {
        DBFileds.film_id.name: "fw.id",
        DBFileds.title.name: "fw.title",
        DBFileds.description.name: "fw.description",
        DBFileds.rating.name: "fw.rating",
        DBFileds.film_type.name: "fw.type",
        DBFileds.film_created_at.name: "fw.created_at",
        DBFileds.film_updated_at.name: "fw.updated_at",
        DBFileds.actors.name: "STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'actor')",
        DBFileds.writers.name: "STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'writer')",
        DBFileds.directors.name: "STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'director')",
        DBFileds.genre.name: "STRING_AGG(DISTINCT g.name, ', ')",
    }

    logging.info(f'context["params"]["fields"]= {context["params"]["fields"]}')
    fields_query = ", ".join([FILEDS2SQL[field] for field in context["params"]["fields"]])

    film_ids = ti.xcom_pull(task_ids="sqlite_get_updated_movies_ids")
    logging.info(f'film_ids= {film_ids}')
    if len(film_ids) == 0:
        logging.info("No records need to be updated")
        return

    query = f"""
        SELECT {fields_query}
        FROM {SQLiteDBTables.film.value} fw
        LEFT JOIN {SQLiteDBTables.film_person.value} pfw ON pfw.film_work_id = fw.id
        LEFT JOIN {SQLiteDBTables.person.value} p ON p.id = pfw.person_id
        LEFT JOIN {SQLiteDBTables.film_genre.value} gfw ON gfw.film_work_id = fw.id
        LEFT JOIN {SQLiteDBTables.genre.value} g ON g.id = gfw.genre_id
        WHERE fw.id IN {tuple(film_ids)}
        GROUP BY fw.id;
        """
    logging.info(f'query= {query}')

    #имя файла базы данных из Admin-Connections-Schema
    db_name = BaseHook.get_connection(context["params"]["in_db_id"]).schema
    logging.info(f"{db_name=}")

    with conn_context(db_name) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query)
                data = cursor.fetchall()
                data_dict = [dict(i) for i in data]
                logging.info(f'{data_dict=}')
            except Exception as err:
                logging.error(f'<<SELECT ERROR>> {err}')

    return json.dumps(data_dict, indent=4)


def sqlite_preprocess(ti: TaskInstance, **context):
    """Трансформация данных"""
    prev_task = ti.xcom_pull(task_ids="in_db_branch_task")[-1]
    logging.info(f'{prev_task=}')
    films_data = ti.xcom_pull(task_ids=prev_task)
    films_data = json.loads(films_data)
    logging.info(f'{films_data=}')
    if not films_data:
        logging.info("No records need to be updated")
        return

    transformed_films_data = films_data
    logging.info(f'{transformed_films_data=}')

    return json.dumps(transformed_films_data, indent=4)


def sqlite_write(ti: TaskInstance, **context):
    """Запись данных"""
    films_data = ti.xcom_pull(task_ids="sqlite_preprocess")
    logging.info(f'JSON {films_data=}')
    films_data = json.loads(films_data)
    logging.info(f'{type(films_data)=}, {films_data=}')
    if not films_data:
        logging.info("No records need to be updated")
        return

    #имя файла базы данных из Admin-Connections-Schema
    db_name = BaseHook.get_connection(context["params"]["out_db_id"]).schema
    logging.info(f"{db_name=}")

    creation_query = f"""
            CREATE TABLE IF NOT EXISTS {SQLiteDBTables.film.value} (    
                        id TEXT PRIMARY KEY,
                        title TEXT DEFAULT 'TITLE',
                        description TEXT DEFAULT 'DESCRIPTION',
                        creation_date DATE DEFAULT CURRENT_DATE,
                        file_path TEXT DEFAULT 'FILE_PATH',
                        rating FLOAT DEFAULT 1,
                        type TEXT DEFAULT 'TYPE',
                        created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
                        updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
                    );
    """

    insertion_query = f"""
            INSERT OR IGNORE INTO {SQLiteDBTables.film.value} tuple(context["params"]["fields"])
            VALUES ({'?'+',?'*(len(films_data[0])-1)});
    """
    logging.info(f'{len(films_data[0])=}')
    logging.info(f'{insertion_query}')

    with conn_context(db_name) as conn:
        with closing(conn.cursor()) as cursor:

            try:
                cursor.execute("""SELECT sql FROM sqlite_master WHERE name='film_work';""")
                schema=cursor.fetchall()
                logging.info(f'{schema=}')
                logging.info(f'{schema[0]=}')
            except Exception as err:
                logging.error(f'<<SELECT schema ERROR>> {err}')


            try:
                cursor.execute("""DROP TABLE film_work;""")
                conn.commit()
                logging.info('SUCCESS DROP TABLE')
            except Exception as err:
                logging.error(f'<<DROP TABLE ERROR>> {err}')


            try:
                cursor.execute(creation_query)
                conn.commit()
                logging.info('SUCCESS CREATE TABLE')
            except Exception as err:
                logging.error(f'<<CREATION TABLE ERROR>> {err}')

            try:
                cursor.executemany(insertion_query, films_data)
                logging.info('We have inserted', cursor.rowcount, 'records to the table.')
                conn.commit()
                logging.info('SUCCESS INSERT')
            except Exception as err:
                logging.error(f'<<INSERT ERROR>> {err}')


    # sqlite_hook = SqliteHook(sqlite_conn_id=context["params"]["out_db_id"])
    # sqlite_con = sqlite_hook.get_conn()
    # sqlite_cur = sqlite_con.cursor()
    # sqlite_cur.execute("""
    #     CREATE TABLE IF NOT EXISTS contacts (
    #     contact_id INTEGER PRIMARY KEY,
    #     first_name TEXT NOT NULL,
    #     last_name TEXT NOT NULL,
    #     email TEXT NOT NULL UNIQUE,
    #     phone TEXT NOT NULL UNIQUE
    # );""")
