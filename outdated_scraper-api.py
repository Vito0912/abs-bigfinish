from fastapi import FastAPI, Query
from typing import List, Optional
from pydantic import BaseModel
from fuzzywuzzy import fuzz
import sqlite3

app = FastAPI()


class AudioBookResponse(BaseModel):
    title: Optional[str]
    subtitle: Optional[str] = None
    author: Optional[list[str]]
    narrator: Optional[list[str]]
    publisher: str = "Big Finish"
    cover: Optional[str]
    series: Optional[list[object]]
    language: str = "English"
    duration: Optional[int]
    abridged: bool = False
    explicit: bool = False


def get_db_connection():
    conn = sqlite3.connect('bigfinish.db')
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/search/", response_model=List[AudioBookResponse])
async def search_audiobooks(query: str = Query(..., description="Title to search for")):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT url, title, series, release_date, about, background, 
             production, duration, isbn, written_by, narrated_by, cover_url, series_tag
        FROM content
    ''')
    results = cursor.fetchall()

    matched_results = []
    for row in results:
        if row['title']:
            ratio = fuzz.ratio(query.lower(), row['title'].lower())
            if ratio > 60:
                matched_results.append((ratio, row))

    matched_results.sort(reverse=True, key=lambda x: x[0])
    top_matches = matched_results[:5]

    response_data = []
    for _, row in top_matches:
        duration_minutes = None
        if row['duration']:
            try:
                duration_minutes = int(row['duration'])
            except ValueError:
                pass

        book_data = AudioBookResponse(
            title=row['title'],
            subtitle=None,
            author=row['written_by'].split(', ') if row['written_by'] else None,
            narrator=row['narrated_by'].split(', ') if row['narrated_by'] else None,
            cover=row['cover_url'],
            series=[{'series': row['series'], 'sequence': row['series_tag']}],
            duration=duration_minutes * 60 if duration_minutes else None,
        )
        response_data.append(book_data)

    conn.close()
    return response_data


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
