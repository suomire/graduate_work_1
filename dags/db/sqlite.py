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
    sqlite_con = sqlite_hook.get_conn()
    sqlite_cur = sqlite_con.cursor()
    sqlite_cur.execute(query, (updated_state_sqlite,))
    sqlite_tuples_list = sqlite_cur.fetchall()
    logging.info(f'sqlite_tuples_list= {sqlite_tuples_list}')
    sqlite_dict_list = [dict(zip(['id', 'updated_at'], tuple)) for tuple in sqlite_tuples_list]
    logging.info(f'sqlite_dict_list= {sqlite_dict_list}')
    sqlite_con.close()
    msg = f"sqlite_items = {str(sqlite_dict_list)}, {str(type(sqlite_dict_list))}"
    logging.info(f'SQLITE_CURSOR SUCCESS;= {msg}')

    if sqlite_dict_list:
        ti.xcom_push(key=MOVIES_UPDATED_STATE_KEY, value=str(sqlite_dict_list[-1]["updated_at"]))
    return set([x["id"] for x in sqlite_dict_list])


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

    sqlite_hook = SqliteHook(sqlite_conn_id=context["params"]["in_db_id"])
    sqlite_con = sqlite_hook.get_conn()
    sqlite_cur = sqlite_con.cursor()
    sqlite_cur.execute(query)
    sqlite_tuples_list = sqlite_cur.fetchall()
    msg = f"sqlite_tuples_list = {sqlite_tuples_list}, {type(sqlite_tuples_list)}"
    logging.info(f'SQLITE_CURSOR SUCCESS;= {msg}')
    fields = [i.replace('film_', '') for i in context["params"]["fields"]]
    sqlite_dict_list = [dict(zip(fields, tuple)) for tuple in sqlite_tuples_list]
    sqlite_con.close()
    msg = f"sqlite_dict_list = {sqlite_dict_list}, {type(sqlite_dict_list)}"
    logging.info(f'SQLITE_CURSOR SUCCESS;= {msg}')

    return json.dumps(sqlite_dict_list, indent=4)


def sqlite_preprocess(ti: TaskInstance, **context):
    """Трансформация данных"""
    logging.info(f'def sqlite_preprocess= {ti.xcom_pull(task_ids="sqlite_get_films_data")}, context= {context["params"]}')

def sqlite_write(ti: TaskInstance, **context):
    """Запись данных"""
    logging.info(f'sqlite_write= {ti.xcom_pull(task_ids="sqlite_preprocess")}, context= {context["params"]}')
