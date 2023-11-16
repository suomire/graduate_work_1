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
)


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

    updated_state = ti.xcom_pull(
        key=MOVIES_UPDATED_STATE_KEY, include_prior_dates=True
    ) or str(datetime.min)
    logging.info("Movies updated state: %s", updated_state)
    cursor.execute(query, (updated_state,))
    items = cursor.fetchall()
    logging.info(items)
    if items:
        ti.xcom_push(
            key=MOVIES_UPDATED_STATE_KEY_TMP, value=str(items[-1]["updated_at"])
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

