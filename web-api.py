from operator import itemgetter

from fastapi import FastAPI, Query
from typing import List, Optional
from pydantic import BaseModel
from fuzzywuzzy import fuzz

from scraper import Search

app = FastAPI()


class AudioBookResponse(BaseModel):
    url: Optional[str]
    title: Optional[str]
    subtitle: Optional[str] = None
    author: Optional[str]
    authors: Optional[list[str]]
    narrators: Optional[list[str]]
    narrator: Optional[str]
    publisher: str = "Big Finish"
    cover: Optional[str]
    series: Optional[list[object]]
    language: str = "English"
    duration: Optional[int]
    isbn: Optional[str]
    description: Optional[str]
    publishedYear: Optional[str]
    publishedDate: Optional[str]
    abridged: bool = False
    explicit: bool = False


@app.get("/search/")
async def search_audiobooks(query: str = Query(..., description="Title to search for")):
    # Get search results
    top_matches = Search().search(query)

    # Calculate similarity scores and sort results
    scored_matches = []
    for match in top_matches:
        score = fuzz.ratio(query.lower(), match['title'].lower())
        scored_matches.append((score, match))

    # Sort by score in descending order
    scored_matches.sort(reverse=True, key=itemgetter(0))

    # Extract just the matches without scores
    sorted_matches = [match for score, match in scored_matches]

    response_data = []
    for row in sorted_matches:
        duration_minutes = None
        if row['duration']:
            try:
                duration_minutes = int(row['duration'])
            except ValueError:
                pass

        book_data = AudioBookResponse(
            url=row['url'],
            title=row['title'],
            subtitle=None,
            authors=row['written_by'].split(', ') if row['written_by'] else None,
            author=row['written_by'],
            narrators=row['narrated_by'].split(', ') if row['narrated_by'] else None,
            narrator=row['narrated_by'],
            cover=row['cover_url'],
            series=[{'series': row['series'], 'sequence': row['series_tag']}],
            duration=duration_minutes if duration_minutes else None,
            isbn=row['isbn'],
            description=row['about'],
            publishedYear=row['release_date'].split('-')[0] if row['release_date'] else None,
            publishedDate=row['release_date'] if row['release_date'] else None,
        )
        response_data.append(book_data)

    return {'matches': response_data}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=7777)
