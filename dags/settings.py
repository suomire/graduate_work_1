from enum import Enum


MOVIES_UPDATED_STATE_KEY = "movies_state"

class ExtendedEnum(Enum):
    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))
    
    @classmethod
    def keys(cls):
        return list(map(lambda c: c.name, cls))


class DBFileds(str, ExtendedEnum):
    film_id = "id"
    title = "title"
    description = "description"
    rating = "rating"
    film_type = "type"
    film_created_at = "created_at"
    film_updated_at = "updated_at"
    actors = "actors"
    writers = "writers"
    directors = "directors"
    genre = "genre"


class PGDBTables(str, ExtendedEnum):
    film = "film_work"
    genre = "genre"
    person = "person"
    film_genre = "genre_film_work"
    film_person = "person_film_work"

class SQLiteDBTables(str, ExtendedEnum):
    film = "film_work"
    genre = "genre"
    person = "person"
    film_genre = "genre_film_work"
    film_person = "person_film_work"

MOVIES_BASE = {
    "settings": {
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english",
                },
                "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                "russian_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                    ],
                }
            },
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            
        },
    },
}


MOVIE_FIELDS = {
    DBFileds.film_id.name: {"type": "keyword"},
    DBFileds.rating.name: {"type": "float"},
    DBFileds.genre.name: {"type": "keyword"},
    DBFileds.film_type.name: {"type": "keyword"},
    DBFileds.title.name: {
        "type": "text",
        "analyzer": "ru_en",
        "fields": {"raw": {"type": "keyword"}},
    },
    DBFileds.description.name: {"type": "text", "analyzer": "ru_en"},
    DBFileds.directors.name: {"type": "text", "analyzer": "ru_en"},
    # "actors_names": {"type": "text", "analyzer": "ru_en"},
    # "writers_names": {"type": "text", "analyzer": "ru_en"},
    DBFileds.actors.name: {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "ru_en"},
        },
    },
    DBFileds.writers.name: {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "ru_en"},
        },
    },
    DBFileds.film_created_at.name: {"type": "keyword"},
    DBFileds.film_updated_at.name: {"type": "keyword"},
}
