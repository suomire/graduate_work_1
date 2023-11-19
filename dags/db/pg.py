from typing import List, Dict
from datetime import datetime
import json
import logging

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import RealDictCursor

from settings import (
    DBFileds,
    PGDBTables,
    MOVIES_UPDATED_STATE_KEY,
    MOVIES_UPDATED_STATE_KEY_TMP,
    DT_FMT_PG,
    DT_FMT,
)
from db_schemas.pg import MOVIE_FIELDS


def pg_get_updated_movies_ids(ti: TaskInstance, **context):
    """Сбор обновленных записей в таблице с фильмами"""

    query = f"""
        SELECT id, updated_at
        FROM {context["params"]["id_db_params"]["schema"]}.{PGDBTables.film.value}
        WHERE updated_at >= %s
        ORDER BY updated_at
        LIMIT {context["params"]["chunk_size"]};
        """

    pg_hook = PostgresHook(postgres_conn_id=context["params"]["in_db_id"])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    updated_state = (
        ti.xcom_pull(
            key=MOVIES_UPDATED_STATE_KEY,
        )
        or datetime.min.strftime(DT_FMT)
    )
    logging.info("Movies updated state: %s", updated_state)
    cursor.execute(query, (updated_state,))
    items = cursor.fetchall()
    logging.info(items)
    if items:
        ti.xcom_push(
            key=MOVIES_UPDATED_STATE_KEY_TMP,
            value=items[-1]["updated_at"].strftime(DT_FMT),
        )
    return set([x["id"] for x in items])


def pg_get_films_data(ti: TaskInstance, **context):
    """Сбор агрегированных данных по фильмам"""

    FILEDS2SQL = {
        DBFileds.film_id.name: "fw.id",
        DBFileds.title.name: "fw.title",
        DBFileds.description.name: "fw.description",
        DBFileds.rating.name: "fw.rating",
        DBFileds.film_type.name: "fw.type",
        DBFileds.film_created_at.name: "TO_CHAR(fw.created_at, %(dt_fmt)s) AS created_at",
        DBFileds.film_updated_at.name: "TO_CHAR(fw.updated_at, %(dt_fmt)s) AS updated_at",
        DBFileds.actors.name: "JSON_AGG(DISTINCT jsonb_build_object('id', p.id::text, 'full_name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors",
        DBFileds.writers.name: "JSON_AGG(DISTINCT jsonb_build_object('id', p.id::text, 'full_name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers",
        DBFileds.directors.name: "JSON_AGG(DISTINCT jsonb_build_object('id', p.id::text, 'full_name', p.full_name)) FILTER (WHERE pfw.role = 'director') AS directors",
        DBFileds.genre.name: "JSON_AGG(DISTINCT jsonb_build_object('id', g.id::text, 'name', g.name)) AS genre",
    }

    logging.info(context["params"]["fields"])
    fields_query = ", ".join(
        [FILEDS2SQL[field] for field in context["params"]["fields"]]
    )
    logging.info(fields_query)

    query = f"""
        SELECT {fields_query}
        FROM {context["params"]["id_db_params"]["schema"]}.{PGDBTables.film.value} fw
        LEFT JOIN {context["params"]["id_db_params"]["schema"]}.{PGDBTables.film_person.value} pfw ON pfw.film_work_id = fw.id
        LEFT JOIN {context["params"]["id_db_params"]["schema"]}.{PGDBTables.person.value} p ON p.id = pfw.person_id
        LEFT JOIN {context["params"]["id_db_params"]["schema"]}.{PGDBTables.film_genre.value} gfw ON gfw.film_work_id = fw.id
        LEFT JOIN {context["params"]["id_db_params"]["schema"]}.{PGDBTables.genre.value} g ON g.id = gfw.genre_id
        WHERE fw.id IN %(id)s
        GROUP BY fw.id;
        """

    film_ids = ti.xcom_pull(task_ids="pg_get_updated_movies_ids")
    logging.info(film_ids)
    if len(film_ids) == 0:
        logging.info("No records need to be updated")
        return

    pg_hook = PostgresHook(postgres_conn_id=context["params"]["in_db_id"])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute(
        query,
        {
            "id": tuple(film_ids),
            "dt_fmt": DT_FMT_PG,
        },
    )
    items = cursor.fetchall()
    logging.info(items)
    return json.dumps(items, indent=4)


def pg_create_schema(ti: TaskInstance, **context):
    pg_hook = PostgresHook(postgres_conn_id=context["params"]["out_db_id"])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    query = (
        f"CREATE SCHEMA IF NOT EXISTS {context['params']['out_db_params']['schema']}"
    )
    cursor.execute(query)
    logging.info(
        "Schema %s is successfully created",
        context["params"]["out_db_params"]["schema"],
    )

    field_properties = [
        v for k, v in MOVIE_FIELDS.items() if k in context["params"]["fields"]
    ]
    field_properties = ", ".join(field_properties)
    query = f"""
    CREATE TABLE IF NOT EXISTS {context['params']['out_db_params']['schema']}.{context['params']['out_db_params']['table']} ({field_properties})
    """
    logging.info(query)
    cursor.execute(query)
    pg_conn.commit()
    logging.info(
        "Table %s.%s is successfully created",
        (
            context["params"]["out_db_params"]["schema"],
            context["params"]["out_db_params"]["table"],
        ),
    )


def pg_preprocess(ti: TaskInstance, **context):
    prev_task = ti.xcom_pull(task_ids="in_db_branch_task")[-1]
    films_data = ti.xcom_pull(task_ids=prev_task)
    if not films_data:
        logging.info("No records need to be updated")
        return

    films_data = json.loads(films_data)
    transformed_films_data = []
    for film_data in films_data:
        transformed_film_data = {}
        for k, v in film_data.items():
            if k in [
                DBFileds.genre.value,
                DBFileds.actors.value,
                DBFileds.writers.value,
                DBFileds.directors.value,
            ]:
                v = json.dumps(v)
            transformed_film_data[k] = v
        transformed_films_data.append(transformed_film_data)
    return json.dumps(transformed_films_data, indent=4)


def pg_write(ti: TaskInstance, **context):

    films_data = ti.xcom_pull(task_ids="pg_preprocess")
    if not films_data:
        logging.info("No records need to be updated")
        return

    films_data = json.loads(films_data)
    logging.info(films_data)
    logging.info("Processing %x movie:", len(films_data))
    pg_hook = PostgresHook(postgres_conn_id=context["params"]["out_db_id"])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    field_properties = ", ".join(
        DBFileds[field] for field in context["params"]["fields"]
    )
    set_fields = [
        f"{DBFileds[field]} = EXCLUDED.{DBFileds[field]}"
        for field in context["params"]["fields"]
    ]
    set_fields = ", ".join(set_fields)

    query = (
        f"""
    INSERT INTO {context['params']['out_db_params']['schema']}.{context['params']['out_db_params']['table']} ({field_properties})
    """
        + """
    VALUES {} 
    ON CONFLICT (id) DO UPDATE
    """
        + f"""
    SET {set_fields};
    """
    )
    logging.info(
        [
            tuple([rec[DBFileds[k].value] for k in context["params"]["fields"]])
            for rec in films_data
        ]
    )
    query = cursor.mogrify(
        query.format(
            ", ".join(["%s"] * len(films_data)),
        ),
        [
            tuple([rec[DBFileds[k].value] for k in context["params"]["fields"]])
            for rec in films_data
        ],
    )
    logging.info(query)
    cursor.execute(query)
    pg_conn.commit()
    logging.info("Transfer completed, %x updated", len(films_data))
