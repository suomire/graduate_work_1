from datetime import datetime, timedelta
import json
import logging

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from es_schemas.index_schemas import MOVIES


DEFAULT_ARGS = {"owner": "airflow"}

MOVIES_ES_INDEX = "movies"
ES_HOSTS = ["http://movies_es_db:9200"]


def create_es_index(ti: TaskInstance):
    es_hook = ElasticsearchPythonHook(hosts=ES_HOSTS)
    es_conn = es_hook.get_conn
    response = es_conn.indices.create(index=MOVIES_ES_INDEX, body=MOVIES, ignore=400)
    if "acknowledged" in response and response["acknowledged"]:
        logging.info("Индекс создан: %s", response["index"])
    elif "error" in response:
        logging.info("Ошибка создания индекса: %s", response["error"]["root_cause"])
    logging.info(response)


with DAG(
    "es-schemas-creation-dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    tags=["movies_etl"],
    catchup=False,
) as dag:

    init = DummyOperator(task_id="init")

    task_create_es_index = PythonOperator(
        task_id="create_es_index", python_callable=create_es_index
    )

    final = DummyOperator(task_id="final")

init >> task_create_es_index >> final
