from datetime import datetime, timedelta
import json
import logging

import airflow
from airflow import DAG

# from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.operators.postgres_operator import PostgresOperator
from elasticsearch import Elasticsearch, helpers
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from psycopg2.extras import RealDictCursor

from models import FilmWorkPG, FilmWorkES, LoadedFilmWorkPG
from utils import transform


DEFAULT_ARGS = {"owner": "airflow"}

MOVIES_PG_CONN_ID = "movies_db_id"
MOVIES_PG_DB_SCHEMA = "content"
MOVIES_PG_FILM_TABLE_NAME = "film_work"
MOVIES_PG_GENRE_TABLE_NAME = "genre"
MOVIES_PG_PERSON_TABLE_NAME = "person"
MOVIES_PG_FILM_GENRE_TABLE_NAME = "genre_film_work"
MOVIES_PG_FILM_PERSON_TABLE_NAME = "person_film_work"
MOVIES_UPDATED_STATE_KEY = "movies_state"
MOVIES_PG_CHUNK_SZ = 2

ES_HOSTS = ["http://movies_es_db:9200"]
MOVIES_ES_INDEX = "movies"


def get_updated_movies_ids(ti: TaskInstance):
    """Сбор обновленных записей в таблице с фильмами"""

    query = f"""
        SELECT id, updated_at
        FROM {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_FILM_TABLE_NAME}
        WHERE updated_at >= %s
        ORDER BY updated_at
        LIMIT {MOVIES_PG_CHUNK_SZ};
        """

    pg_hook = PostgresHook(postgres_conn_id=MOVIES_PG_CONN_ID)
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
        ti.xcom_push(key=MOVIES_UPDATED_STATE_KEY, value=str(items[-1]["updated_at"]))
    return set([x["id"] for x in items])


def get_films_data(ti: TaskInstance):
    """Сбор агрегированных данных по фильмам"""

    query = f"""
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'actor') as actors,
            STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'writer') as writers,
            STRING_AGG(DISTINCT p.id::text || ' : ' || p.full_name, ', ') FILTER (WHERE pfw.role = 'director') as directors,
            STRING_AGG(DISTINCT g.name, ', ') as genre
        FROM {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_FILM_TABLE_NAME} fw
        LEFT JOIN {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_FILM_PERSON_TABLE_NAME} pfw ON pfw.film_work_id = fw.id
        LEFT JOIN {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_PERSON_TABLE_NAME} p ON p.id = pfw.person_id
        LEFT JOIN {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_FILM_GENRE_TABLE_NAME} gfw ON gfw.film_work_id = fw.id
        LEFT JOIN {MOVIES_PG_DB_SCHEMA}.{MOVIES_PG_GENRE_TABLE_NAME} g ON g.id = gfw.genre_id
        WHERE fw.id IN %s
        GROUP BY fw.id;
        """

    film_ids = ti.xcom_pull(task_ids="get_updated_movies_ids")
    if len(film_ids) == 0:
        logging.info("No records need to be updated")
        return

    pg_hook = PostgresHook(postgres_conn_id=MOVIES_PG_CONN_ID)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute(query, (tuple(film_ids),))
    # items = [FilmWorkPG(**rec).dict() for rec in cursor.fetchall()]
    items = [LoadedFilmWorkPG(**rec).json() for rec in cursor.fetchall()]
    logging.info(items)
    return json.dumps(items, indent=4)

def preprocess_data(ti: TaskInstance):
    films_data = ti.xcom_pull(task_ids="get_films_data")
    if not films_data:
        logging.info("No records need to be updated")
        return
    
    films_data = json.loads(films_data)
    transformed_films_data = []
    for film_data in films_data:
        film_data = FilmWorkPG(**json.loads(film_data))
        transformed_films_data.append(FilmWorkES(
                        id=film_data.id,
                        imdb_rating=film_data.rating,
                        genre=film_data.genre.split(', '),
                        title=film_data.title,
                        description=film_data.description,
                        director=film_data.directors_names,
                        actors_names=film_data.actors_names,
                        writers_names=film_data.writers_names,
                        actors=transform.get_person_json(person_ids=film_data.actors_ids, person_names=film_data.actors_names),
                        writers=transform.get_person_json(person_ids=film_data.writers_ids, person_names=film_data.writers_names),
                        ).json()
        )
    return json.dumps(transformed_films_data, indent=4)


def write_to_es(ti: TaskInstance):
    es_hook = ElasticsearchPythonHook(hosts=ES_HOSTS)
    es_conn = es_hook.get_conn
    # elastic = Elasticsearch("http://elasticsearch:9200")

    films_data = ti.xcom_pull(task_ids="preprocess_data")
    if not films_data:
        logging.info("No records need to be updated")
        return

    films_data = json.loads(films_data)
    films_data = [FilmWorkES(**json.loads(film_data)) for film_data in films_data]
    logging.info(films_data)
    logging.info("Processing %x movie:", len(films_data))
    actions = [
        {
            "_index": MOVIES_ES_INDEX,
            "_id": film_data.id,
            "_source": film_data.dict(),
        }
        for film_data in films_data
    ]
    logging.info(actions)
    helpers.bulk(es_conn, actions)
    logging.info("Transfer completed, %x updated", len(actions))


with DAG(
    "movies-etl-dag",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=1),
    default_args=DEFAULT_ARGS,
    tags=["movies_etl"],
    catchup=False,
) as dag:

    init = DummyOperator(task_id="init")

    task_get_movies_ids = PythonOperator(
        task_id="get_updated_movies_ids",
        python_callable=get_updated_movies_ids,
        do_xcom_push=True,
    )

    task_get_films_data = PythonOperator(
        task_id="get_films_data", python_callable=get_films_data
    )

    task_preprocess_data = PythonOperator(
        task_id="preprocess_data", python_callable=preprocess_data
    )

    task_write_to_es = PythonOperator(
        task_id="write_to_es", python_callable=write_to_es
    )

    final = DummyOperator(task_id="final")

init >> task_get_movies_ids >> task_get_films_data >> task_preprocess_data >> task_write_to_es >> final
