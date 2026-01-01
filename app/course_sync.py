from typing import Any

from app import db
from app.golf_api import GolfApiError, fetch_course


def _extract_location(course: dict[str, Any]) -> tuple[str | None, str | None, str | None, float | None, float | None]:
    location = course.get("location") or {}
    return (
        location.get("city"),
        location.get("state"),
        location.get("country"),
        location.get("latitude"),
        location.get("longitude"),
    )


def import_course_to_db(database_url: str, course_id: int, api_key: str) -> dict:
    course = fetch_course(course_id, api_key)
    city, state, country, lat, lon = _extract_location(course)
    db.upsert_course(
        database_url=database_url,
        course_id=course["id"],
        club_name=course.get("club_name", ""),
        course_name=course.get("course_name", ""),
        city=city,
        state=state,
        country=country,
        latitude=lat,
        longitude=lon,
        raw=course,
    )
    tees_payload = course.get("tees") or {}
    imported = []
    for gender in ("male",):
        for tee in tees_payload.get(gender, []) or []:
            tee_id = db.upsert_course_tee(database_url, course["id"], gender, tee)
            holes = []
            for idx, hole in enumerate(tee.get("holes") or [], 1):
                holes.append(
                    {
                        "hole_number": hole.get("hole_number") or idx,
                        "par": hole.get("par") or 4,
                        "handicap": hole.get("handicap") or idx,
                        "yardage": hole.get("yardage"),
                    }
                )
            db.replace_course_tee_holes(database_url, tee_id, holes)
            imported.append({"tee_id": tee_id, "gender": gender, "tee_name": tee.get("tee_name")})
    return {
        "course_id": course["id"],
        "course_name": course.get("course_name"),
        "club_name": course.get("club_name"),
        "imported_tees": imported,
    }
