from datetime import datetime
import json
import logging

from airflow.models.taskinstance import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import RealDictCursor

from settings import DBFileds, PGDBTables, MOVIES_UPDATED_STATE_KEY


def sqlite_get_updated_movies_ids(ti: TaskInstance, **context):
    """Сбор обновленных записей в таблице с фильмами"""
    pass

def sqlite_get_films_data(ti: TaskInstance, **context):
    """Сбор агрегированных данных по фильмам"""
    pass

def sqlite_preprocess(ti: TaskInstance, **context):
    """Трансформация данных"""
    pass

def sqlite_write(ti: TaskInstance, **context):
    """Запись данных"""
    pass
