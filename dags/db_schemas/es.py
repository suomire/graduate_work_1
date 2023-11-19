from settings import DBFileds


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
        "properties": {},
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
    DBFileds.actors.name: {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "full_name": {"type": "text", "analyzer": "ru_en"},
        },
    },
    DBFileds.writers.name: {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "full_name": {"type": "text", "analyzer": "ru_en"},
        },
    },
    DBFileds.directors.name: {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "full_name": {"type": "text", "analyzer": "ru_en"},
        },
    },
    DBFileds.film_created_at.name: {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    DBFileds.film_updated_at.name: {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
}
