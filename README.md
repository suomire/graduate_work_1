# Проектная работа: диплом


## Запуск проекта
sudo docker compose up -d --build


## Airflow
http://0.0.0.0:8080


## Настройка Airflow-Admin-Connection
### Postgres
![Postgres](images%2Fmovies_pg_db.png)

### Elasticsearch
![Elasticsearch](images%2Fmovies_es_db.png)

### SQLite - база источник данных
![SQLite - база источник данных](images%2Fmovies_sqlite_db_in.png)

### SQLite - база получатель данных
![SQLite - база получатель данных](images%2Fmovies_sqlite_db_out.png)


##Настойка DAG Params
### movies_pg_db
id_db_params и out_db_params: 	{"schema": "content", "table": "film_work"}

### movies_es_db
id_db_params и out_db_params: 	{"index": "content"}

### movies_sqlite_db_in и movies_sqlite_db_out
id_db_params и out_db_params: можно не заполнять

### fields
film_id, title (выбрать из списка доступные поля)


## Доступ к Elasticsearch
http://localhost:9200/movies/_search?pretty=true
