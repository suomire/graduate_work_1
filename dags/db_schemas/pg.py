from settings import DBFileds


MOVIE_FIELDS = {
    DBFileds.film_id.name: f"{DBFileds.film_id.value} uuid NOT NULL PRIMARY KEY",
    DBFileds.rating.name: f"{DBFileds.rating.value} FLOAT",
    DBFileds.genre.name: f"{DBFileds.genre.value} json NOT NULL",
    DBFileds.film_type.name: f"{DBFileds.film_type.value} TEXT not null",
    DBFileds.title.name: f"{DBFileds.title.value} TEXT NOT NULL",
    DBFileds.description.name: f"{DBFileds.description.value} TEXT",
    DBFileds.actors.name: f"{DBFileds.actors.value} json NOT NULL",
    DBFileds.writers.name: f"{DBFileds.writers.value} json NOT NULL",
    DBFileds.directors.name: f"{DBFileds.directors.value} json NOT NULL",
    DBFileds.film_created_at.name: f"{DBFileds.film_created_at.value} timestamp with time zone",
    DBFileds.film_updated_at.name: f"{DBFileds.film_updated_at.value} timestamp with time zone",
}
