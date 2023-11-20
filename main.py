from fastapi import FastAPI, HTTPException
import sqlite3
from sqlite3 import Error
from contextlib import contextmanager

app = FastAPI()


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = sqlite3.connect('songBook.sqlite')
        yield conn
    except Error as e:
        print(f"Error al conectar a SQLite: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")
    finally:
        if conn:
            conn.close()


def execute_query(conn, query, params=None):
    params = params or []
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()


@app.get("/songs/")
async def get_songs(song_ids: str = None, artist_name: str = None, fields: str = "all"):
    valid_fields = ["id", "title", "artist", "album", "song_year", "original_key", "original_lead", "bpm",
                    "time_signature", "song_length", "man_key", "woman_key", "visibility", "intensity"]
    requested_fields = fields.split(',') if fields != "all" else valid_fields

    invalid_fields = [field for field in requested_fields if field not in valid_fields]
    if invalid_fields:
        raise HTTPException(status_code=400, detail=f"Campos no válidos: {', '.join(invalid_fields)}")

    query_fields = ", ".join(requested_fields)
    query = f"SELECT {query_fields} FROM songs"
    params = []

    if song_ids:
        song_ids_list = song_ids.split(',')
        query += " WHERE id IN ({})".format(", ".join(["?"] * len(song_ids_list)))
        params.extend(song_ids_list)

    if artist_name:
        artist_names_list = artist_name.split(',')
        artist_placeholders = ', '.join('?' for _ in artist_names_list)
        if 'WHERE' in query:
            query += f" AND artist IN ({artist_placeholders})"
        else:
            query += f" WHERE artist IN ({artist_placeholders})"
        params.extend(artist_names_list)

    with get_db_connection() as conn:
        results = execute_query(conn, query, params)

    if not results:
        return {"error": "No se encontraron canciones con los criterios proporcionados"}

    all_songs = [dict(zip(requested_fields, result)) for result in results]
    return all_songs


@app.get("/songs/stats")
async def get_songs_stats(artist_name: str = None):
    query_artist_count = "SELECT artist, COUNT(*) as song_count FROM songs"
    params = []

    if artist_name:
        # Si se especifican nombres de artistas
        artist_names_list = [name.lower() for name in artist_name.split(',')]
        artist_placeholders = ', '.join('?' for _ in artist_names_list)
        query_artist_count += f" WHERE LOWER(artist) IN ({artist_placeholders})"
        params.extend(artist_names_list)
    query_artist_count += " GROUP BY artist"

    with get_db_connection() as conn:
        artist_counts = execute_query(conn, query_artist_count, params)

    artist_stats = [{"artist": artist, "song_count": count} for artist, count in artist_counts]
    response = {"songs_by_artist": artist_stats}

    if not artist_name:
        # Contar el total de artistas distintos
        query_total_artists = "SELECT COUNT(DISTINCT artist) FROM songs"
        with get_db_connection() as conn:
            total_artists = execute_query(conn, query_total_artists)[0][0]
        response["total_artists"] = total_artists

    return response
