from typing import List, Tuple, Dict
from datetime import datetime
import logging

from pydantic import BaseModel, Field, validator


class UpdatedRecord(BaseModel):
    id: str
    updated_at: datetime

class _FilmWorkPG(BaseModel):
    id: str
    title: str
    type: str
    rating: float
    created_at: datetime
    updated_at: datetime
    genre: str = None
    actors_names: List[str] = None
    actors_ids: List[str] = None
    writers_names: List[str] = None
    writers_ids: List[str] = None
    directors_names: List[str] = None
    directors_ids: List[str] = None
    description: str = None

class LoadedFilmWorkPG(_FilmWorkPG):

    def __init__(self, **data):
        data["actors_ids"], data["actors_names"] = self.split_person_str(data["actors"])
        data["writers_ids"], data["writers_names"] = self.split_person_str(
            data["writers"]
        )
        data["directors_ids"], data["directors_names"] = self.split_person_str(
            data["directors"]
        )
        super().__init__(**data)

    @validator("rating", pre=True)
    def validate_rating(cls, value):
        return value or 0

    @staticmethod
    def split_person_str(attr: str) -> Tuple[List[str], List[str]]:
        if attr is None:
            return [], []
        ids, names = [], []
        for value in attr.split(", "):
            id, name = value.split(" : ")
            ids.append(id)
            names.append(name)
        return ids, names

class FilmWorkPG(_FilmWorkPG):
    pass

class FilmWorkES(BaseModel):
    id: str
    imdb_rating: float
    genre: List[str]
    title: str
    director: List[str] = None
    actors_names: List[str] = None
    writers_names: List[str] = None
    actors: List[Dict[str, str]] = None
    writers: List[Dict[str, str]] = None
    description: str = None


class Genre(BaseModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    film_ids: List[str]
    description: str = None

    def __init__(self, **data):
        data["film_ids"] = self.split_ids_str(data["film_ids"])
        super().__init__(**data)

    @staticmethod
    def split_ids_str(ids: str) -> List[str]:
        if ids is None:
            return []
        return ids.split(", ")


class Person(BaseModel):
    id: str
    full_name: str
    created_at: datetime
    updated_at: datetime
    films: List[Dict[str, str]]


class Action(BaseModel):
    _index: str
    _id: str
    _source: FilmWorkES
