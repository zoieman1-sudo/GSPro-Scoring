import os
from typing import Any

import requests

API_BASE = "https://api.golfcourseapi.com/v1"


class GolfApiError(Exception):
    pass


def _headers(api_key: str) -> dict[str, str]:
    key = api_key or os.getenv("GOLF_API_KEY", "")
    if not key:
        raise GolfApiError("Missing Golf Course API key.")
    return {"Authorization": f"Key {key}"}


def search_courses(query: str, api_key: str) -> dict[str, Any]:
    if not query:
        return {"courses": []}
    response = requests.get(
        f"{API_BASE}/search",
        params={"search_query": query},
        headers=_headers(api_key),
        timeout=15,
    )
    if response.status_code != 200:
        raise GolfApiError(f"Search failed: {response.status_code} {response.text}")
    return response.json()


def fetch_course(course_id: int, api_key: str) -> dict[str, Any]:
    response = requests.get(
        f"{API_BASE}/courses/{course_id}",
        headers=_headers(api_key),
        timeout=20,
    )
    if response.status_code != 200:
        raise GolfApiError(f"Course fetch failed: {response.status_code} {response.text}")
    payload = response.json()
    if not isinstance(payload, dict) or "course" not in payload:
        raise GolfApiError(
            f"Course fetch returned unexpected payload for id {course_id}: {response.text}"
        )
    course = payload["course"]
    if not isinstance(course, dict) or "id" not in course:
        raise GolfApiError(
            f"Course fetch returned unexpected course data for id {course_id}: {response.text}"
        )
    return course
