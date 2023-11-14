from datetime import datetime, timedelta
from typing import List
import json
import logging

import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

from settings import DBFileds
from db.pg import pg_get_films_data, pg_get_updated_movies_ids
from db.es import es_create_index, es_preprocess, es_write


DEFAULT_ARGS = {"owner": "airflow"}


# AVAILABLE_DB_FILEDS = ["film_id", "title", "description", "rating", 
#                        "film_type", "film_created_at", "film_updated_at",
#                        "actors", "writers", "directors", "genre",]


@task.branch(task_id="in_db_branch_task")
def in_db_branch_func(**context):
    # https://www.restack.io/docs/airflow-faq-authoring-and-scheduling-connections-05
    conn = BaseHook.get_connection(context["params"]["in_db_id"])
    logging.info(conn)
    if conn.conn_type == "postgres":
        return ["pg_get_updated_movies_ids", "pg_get_films_data"]
    if conn.conn_type == "elasticsearch":
        return 
    if conn.conn_type == "sqlite":
        return 
        

@task.branch(task_id="out_db_branch_task")
def out_db_branch_func(**context):
    # if context["params"]["out_db_id"] is not None:
    
    # https://www.restack.io/docs/airflow-faq-authoring-and-scheduling-connections-05
    conn = BaseHook.get_connection(context["params"]["out_db_id"])
    logging.info(conn)
    if conn.conn_type == "postgres":
        return ["task_es_preprocess", "es_create_index", "task_es_write"]
    if conn.conn_type == "elasticsearch":
        return 
    if conn.conn_type == "sqlite":
        return 
    
    # else:
    #     conn_type = context["params"]["out_db_conn"]["db_type"]
    #     if conn_type == "ElasticSearch":
    #         return ["es_preprocess", "es_write"]
    #     elif conn_type == "":
    #         return
            

def in_param_validator(ti: TaskInstance, **context):
    conn = BaseHook.get_connection(context["params"]["in_db_id"])
    logging.info(conn)
    if conn.conn_type == "postgres":
        if context["params"]["id_db_params"].get("schema") is None:
            raise AirflowException("You must specify 'schema' in 'id_db_params' for Postgres")
    elif conn.conn_type == "elasticsearch":
        return 
    elif conn.conn_type == "sqlite":
        return 
    else:
        raise AirflowException("Unknown input db connection type %s", conn.conn_type)
    
    if context["params"]["out_db_id"] is None and len(context["params"]["out_db_conn"]) == 0:
        raise AirflowException("'out_db_id' or 'out_db_conn' must be not None")
    
    # if context["params"]["out_db_conn"].get("db_type") == "ElasticSearch":
    #     if context["params"]["out_db_conn"].get("hosts") is None:
    #         raise AirflowException("You must specify 'hosts' for ElasticSearch")
    #     if context["params"]["out_db_conn"].get("index_name") is None:
    #         raise AirflowException("You must specify 'index_name' for ElasticSearch")



with DAG(
    "movies-etl2-dag",
    start_date=days_ago(1),
    # schedule_interval=timedelta(minutes=1),
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    tags=["movies_etl"],
    catchup=False,
    params={
        "chunk_size": Param(11, type="integer", minimum=10),
        "in_db_id": Param(
            "movies_db_id", type="string", enum=["movies_pg_db", "movies_es_db"]
        ),
        "id_db_params": Param({"schema": "content"}, type=["object", "null"]),
        "fields": Param(["film_id", "title"], type="array", examples=DBFileds.keys()),
        # ["film_id"]
        # "out_db_id": Param(
        #     None, type=["string", "null"]
        #     # , enum=[None, "movies_db_id",]
        # ),
        "out_db_id": Param(
            "movies_es_db", type=["string", "null"], enum=["movies_es_db", "movies_pg_db",]
        ),
        # "out_db_conn": Param({"db_type": "ElasticSearch"}, type=["object", "null"]),
        "out_db_params": Param({"schema": "content"}, type=["object", "null"]),
    }
) as dag:

    init = DummyOperator(task_id="init")


    task_validate_params = PythonOperator(
        task_id="in_param_validator", 
        python_callable=in_param_validator, 
        provide_context=True,
    )

    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#branching
    in_branch_op = in_db_branch_func()

    task_pg_get_movies_ids = PythonOperator(
        task_id="pg_get_updated_movies_ids",
        python_callable=pg_get_updated_movies_ids,
        do_xcom_push=True,
        provide_context=True,
    )
    
    task_pg_get_films_data = PythonOperator(
        task_id="pg_get_films_data", 
        python_callable=pg_get_films_data,
        provide_context=True,
    )

    out_branch_op = out_db_branch_func()

    task_es_preprocess = PythonOperator(
        task_id="es_preprocess", 
        python_callable=es_preprocess,
        provide_context=True,
    )

    task_es_create_index = PythonOperator(
        task_id="es_create_index", 
        python_callable=es_create_index,
        provide_context=True,
    )
    
    task_es_write = PythonOperator(
        task_id="es_write", 
        python_callable=es_write,
        provide_context=True,
    )

    final = DummyOperator(task_id="final")

init >> task_validate_params >> in_branch_op

in_branch_op >> task_pg_get_movies_ids >> task_pg_get_films_data
task_pg_get_films_data >> out_branch_op

out_branch_op >> task_es_preprocess >> task_es_create_index >> task_es_write
task_es_write >> final