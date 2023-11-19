from typing import List, Dict
from datetime import datetime
import copy
import json
import logging

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch import Elasticsearch, helpers

from settings import (
    DBFileds,
    MOVIES_UPDATED_STATE_KEY,
    MOVIES_UPDATED_STATE_KEY_TMP,
    DT_FMT,
)
from db_schemas.es import MOVIES_BASE, MOVIE_FIELDS
from utils import transform


def _es_hosts(conn) -> List[str]:
    return [f"http://{conn.host}:{conn.port}"]


def es_get_films_data(ti: TaskInstance, **context):
    """Сбор обновленных данных"""

    conn = BaseHook.get_connection(context["params"]["in_db_id"])
    es_hook = ElasticsearchPythonHook(hosts=_es_hosts(conn))
    es_conn = es_hook.get_conn

    updated_state = (
        ti.xcom_pull(
            key=MOVIES_UPDATED_STATE_KEY,
        )
        or datetime.min.strftime(DT_FMT)
    )
    logging.info("Movies updated state: %s", updated_state)

    query = {
        "range": {
            "updated_at": {
                "gte": updated_state,
            }
        }
    }

    logging.info(query)
    items = es_conn.search(
        index=context["params"]["id_db_params"]["index"],
        query=query,
    )

    items = items["hits"]["hits"]
    logging.info(items)
    required_fields = [DBFileds[field].value for field in context["params"]["fields"]]
    logging.info(required_fields)

    transformed_items = []
    for item in items:
        transformed_item = {}
        for k, v in item["_source"].items():
            if k not in required_fields:
                continue
            if k == DBFileds.genre.value:
                v = [{"name": vi} for vi in v]
            transformed_item[k] = v
        transformed_items.append(transformed_item)

    logging.info(transformed_items)
    if transformed_items:
        ti.xcom_push(
            key=MOVIES_UPDATED_STATE_KEY_TMP,
            value=transformed_items[-1]["updated_at"],
        )
    return json.dumps(transformed_items, indent=4)


def get_index_schema(fields: List[str]) -> Dict:
    filed_properties = {
        DBFileds[k].value: v for k, v in MOVIE_FIELDS.items() if k in fields
    }
    schema = copy.deepcopy(MOVIES_BASE)
    schema["mappings"]["properties"] = filed_properties
    return schema


def es_create_index(ti: TaskInstance, **context):
    conn = BaseHook.get_connection(context["params"]["out_db_id"])
    es_hook = ElasticsearchPythonHook(hosts=[f"http://{conn.host}:{conn.port}"])
    es_conn = es_hook.get_conn
    logging.info(context["params"]["fields"])
    logging.info(get_index_schema(context["params"]["fields"]))
    response = es_conn.indices.create(
        index=context["params"]["out_db_params"]["index"],
        body=get_index_schema(context["params"]["fields"]),
        ignore=400,
    )
    if "acknowledged" in response:
        if response["acknowledged"]:
            logging.info("Индекс создан: {}".format(response["index"]))
    elif "error" in response:
        logging.error("Ошибка: {}".format(response["error"]["root_cause"]))
    logging.info(response)


def es_preprocess(ti: TaskInstance, **context):
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
            if k == DBFileds.genre.value:
                v = transform.get_genres(v)
            transformed_film_data[k] = v
        transformed_films_data.append(transformed_film_data)
    return json.dumps(transformed_films_data, indent=4)


def es_write(ti: TaskInstance, **context):
    conn = BaseHook.get_connection(context["params"]["out_db_id"])
    es_hook = ElasticsearchPythonHook(hosts=[f"http://{conn.host}:{conn.port}"])
    es_conn = es_hook.get_conn

    films_data = ti.xcom_pull(task_ids="es_preprocess")
    if not films_data:
        logging.info("No records need to be updated")
        return

    films_data = json.loads(films_data)
    logging.info(films_data)
    logging.info("Processing %x movie:", len(films_data))
    actions = [
        {
            "_index": context["params"]["out_db_params"]["index"],
            "_id": film_data["id"],
            "_source": film_data,
        }
        for film_data in films_data
    ]
    logging.info(actions)
    helpers.bulk(es_conn, actions)
    logging.info("Transfer completed, %x updated", len(actions))
