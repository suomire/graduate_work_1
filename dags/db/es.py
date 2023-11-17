from typing import List, Dict
import copy
import json
import logging

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from elasticsearch import Elasticsearch, helpers

from settings import DBFileds, MOVIES_BASE, MOVIE_FIELDS
from utils import transform


def get_index_schema(fields: List[str]) -> Dict:
    filed_properties = {DBFileds[k].value : v for k,v in MOVIE_FIELDS.items() if k in fields}
    schema = copy.deepcopy(MOVIES_BASE)
    schema["mappings"]["properties"] = filed_properties
    return schema


def es_create_index(ti: TaskInstance, **context):
    conn = BaseHook.get_connection(context["params"]["out_db_id"])
    es_hook = ElasticsearchPythonHook(hosts=[f"http://{conn.host}:{conn.port}"])
    es_conn = es_hook.get_conn
    logging.info(context["params"]["fields"])
    logging.info(get_index_schema(context["params"]["fields"]))
    response = es_conn.indices.create(index=context["params"]["out_db_params"]["index_name"], body=get_index_schema(context["params"]["fields"]), ignore=400)
    if 'acknowledged' in response:
        if response['acknowledged']:
            logging.info('Индекс создан: {}'.format(response['index']))
    elif 'error' in response:
        logging.error('Ошибка: {}'.format(response['error']['root_cause']))
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
            if k == "genre":
                v = v.split(', ')
            if k == "actors":
                v = transform.get_person_json(person_ids=film_data["actors_ids"], person_names=film_data["actors_names"])
            if k == "writers":
                v = transform.get_person_json(person_ids=film_data["writers_ids"], person_names=film_data["writers_names"])
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
    logging.info(f'{films_data=}')
    logging.info(f'{len(films_data)=}')
    logging.info(f'{type(films_data)=}')
    logging.info("Processing %x movie:", len(films_data))
    actions = [
        {
            "_index": context["params"]["out_db_params"]["index_name"],
            "_id": film_data["id"],
            "_source": film_data,
        }
        for film_data in films_data
    ]
    logging.info(actions)
    helpers.bulk(es_conn, actions)
    logging.info("Transfer completed, %x updated", len(actions))