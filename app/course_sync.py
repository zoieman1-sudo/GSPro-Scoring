from typing import Any, Iterable

from app import db
from app.db import (
    fetch_course_catalog,
    next_course_id,
    replace_course_tee_holes,
    upsert_course,
    upsert_course_tee,
)
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


COURSE_NAME = "Pebble Beach"
COURSE_CLUB = "Pebble Beach Golf Links"
COURSE_LOCATION = {"city": "Pebble Beach", "state": "California", "country": "United States"}
PEBBLE_HOLES = [
    {"hole": 1, "par": 4, "handicap": 6},
    {"hole": 2, "par": 5, "handicap": 10},
    {"hole": 3, "par": 4, "handicap": 12},
    {"hole": 4, "par": 4, "handicap": 16},
    {"hole": 5, "par": 3, "handicap": 14},
    {"hole": 6, "par": 5, "handicap": 2},
    {"hole": 7, "par": 3, "handicap": 18},
    {"hole": 8, "par": 4, "handicap": 4},
    {"hole": 9, "par": 4, "handicap": 8},
    {"hole": 10, "par": 4, "handicap": 3},
    {"hole": 11, "par": 4, "handicap": 9},
    {"hole": 12, "par": 3, "handicap": 17},
    {"hole": 13, "par": 4, "handicap": 7},
    {"hole": 14, "par": 5, "handicap": 1},
    {"hole": 15, "par": 4, "handicap": 13},
    {"hole": 16, "par": 4, "handicap": 11},
    {"hole": 17, "par": 3, "handicap": 15},
    {"hole": 18, "par": 5, "handicap": 5},
]
PEBBLE_TEES = [
    {
        "name": "Black",
        "rating": 75.5,
        "slope": 144,
        "total_yards": 7013,
        "yards": [380, 510, 419, 332, 195, 523, 109, 425, 505, 495, 390, 202, 401, 580, 397, 402, 208, 540],
    },
    {
        "name": "Blue",
        "rating": 74.9,
        "slope": 144,
        "total_yards": 6798,
        "yards": [377, 490, 405, 326, 191, 501, 106, 422, 481, 446, 373, 200, 392, 572, 396, 400, 177, 543],
    },
    {
        "name": "Yellow",
        "rating": 73.4,
        "slope": 137,
        "total_yards": 6434,
        "yards": [346, 460, 390, 307, 138, 488, 98, 398, 460, 429, 349, 186, 370, 560, 377, 376, 170, 532],
    },
    {
        "name": "White",
        "rating": 71.7,
        "slope": 135,
        "total_yards": 6060,
        "yards": [332, 430, 348, 295, 130, 466, 94, 373, 435, 409, 340, 178, 295, 548, 347, 368, 163, 509],
    },
    {
        "name": "Red",
        "rating": 67.3,
        "slope": 124,
        "total_yards": 5262,
        "yards": [309, 362, 291, 253, 112, 386, 90, 362, 333, 341, 303, 166, 290, 436, 316, 308, 148, 456],
    },
    {
        "name": "Junior",
        "rating": 80.0,
        "slope": 120,
        "total_yards": 3728,
        "yards": [183, 258, 188, 134, 110, 275, 67, 213, 265, 220, 189, 123, 190, 348, 191, 256, 151, 367],
    },
]


def _hole_rows(tee_yards: Iterable[int]) -> list[dict]:
    rows: list[dict] = []
    for layout, yardage in zip(PEBBLE_HOLES, tee_yards):
        rows.append(
            {
                "hole_number": layout["hole"],
                "par": layout["par"],
                "handicap": layout["handicap"],
                "yardage": yardage,
            }
        )
    return rows


def _build_tee_payload(tee_info: dict) -> dict:
    return {
        "tee_name": tee_info["name"],
        "course_rating": tee_info["rating"],
        "slope_rating": tee_info["slope"],
        "bogey_rating": None,
        "total_yards": tee_info["total_yards"],
        "total_meters": None,
        "number_of_holes": 18,
        "par_total": 72,
        "front_course_rating": None,
        "back_course_rating": None,
        "front_slope_rating": None,
        "back_slope_rating": None,
        "front_bogey_rating": None,
        "back_bogey_rating": None,
    }


def ensure_pebble_beach_course(database_url: str) -> int | None:
    catalog = fetch_course_catalog(database_url)
    existing = next(
        (entry for entry in catalog if (entry.get("course_name") or "").strip().lower() == COURSE_NAME.lower()),
        None,
    )
    course_id = existing["id"] if existing else next_course_id(database_url)

    upsert_course(
        database_url,
        course_id,
        club_name=COURSE_CLUB,
        course_name=COURSE_NAME,
        city=COURSE_LOCATION.get("city"),
        state=COURSE_LOCATION.get("state"),
        country=COURSE_LOCATION.get("country"),
        latitude=None,
        longitude=None,
        raw={},
    )

    for tee_info in PEBBLE_TEES:
        tee_payload = _build_tee_payload(tee_info)
        tee_id = upsert_course_tee(database_url, course_id, "male", tee_payload)
        hole_rows = _hole_rows(tee_info["yards"])
        replace_course_tee_holes(database_url, tee_id, hole_rows)
    return course_id


GEORGIA_COURSE_NAME = "Augusta National"
GEORGIA_CLUB_NAME = "Augusta National Golf Club"
GEORGIA_LOCATION = {"city": "Augusta", "state": "Georgia", "country": "United States"}
GEORGIA_HOLES = [
    {"hole": 1, "par": 4, "handicap": 9},
    {"hole": 2, "par": 5, "handicap": 1},
    {"hole": 3, "par": 4, "handicap": 13},
    {"hole": 4, "par": 3, "handicap": 15},
    {"hole": 5, "par": 4, "handicap": 5},
    {"hole": 6, "par": 3, "handicap": 17},
    {"hole": 7, "par": 4, "handicap": 11},
    {"hole": 8, "par": 5, "handicap": 3},
    {"hole": 9, "par": 4, "handicap": 7},
    {"hole": 10, "par": 4, "handicap": 6},
    {"hole": 11, "par": 4, "handicap": 8},
    {"hole": 12, "par": 3, "handicap": 16},
    {"hole": 13, "par": 5, "handicap": 4},
    {"hole": 14, "par": 4, "handicap": 12},
    {"hole": 15, "par": 5, "handicap": 2},
    {"hole": 16, "par": 3, "handicap": 18},
    {"hole": 17, "par": 4, "handicap": 14},
    {"hole": 18, "par": 4, "handicap": 10},
]
GEORGIA_TEES = [
    {
        "name": "Black",
        "rating": 76.5,
        "slope": 151,
        "total_yards": 7571,
        "yards": [447, 570, 346, 242, 501, 185, 444, 575, 465, 504, 528, 155, 525, 436, 553, 180, 447, 468],
    },
    {
        "name": "Blue",
        "rating": 74.2,
        "slope": 145,
        "total_yards": 7350,
        "yards": [422, 559, 337, 233, 491, 178, 432, 562, 455, 496, 516, 150, 491, 432, 542, 171, 429, 454],
    },
    {
        "name": "White",
        "rating": 72.5,
        "slope": 131,
        "total_yards": 6594,
        "yards": [372, 514, 330, 225, 411, 172, 330, 475, 401, 489, 399, 148, 449, 427, 479, 164, 370, 439],
    },
    {
        "name": "Yellow",
        "rating": 72.8,
        "slope": 135,
        "total_yards": 6277,
        "yards": [363, 497, 293, 168, 398, 161, 318, 465, 385, 455, 392, 144, 443, 422, 470, 155, 358, 390],
    },
    {
        "name": "Green",
        "rating": 69.8,
        "slope": 122,
        "total_yards": 5484,
        "yards": [310, 434, 259, 117, 275, 109, 287, 360, 354, 448, 376, 140, 383, 349, 445, 146, 314, 378],
    },
    {
        "name": "Red",
        "rating": 66.6,
        "slope": 115,
        "total_yards": 4695,
        "yards": [286, 387, 219, 85, 240, 90, 239, 317, 323, 394, 275, 139, 361, 297, 383, 138, 276, 246],
    },
    {
        "name": "Junior",
        "rating": 72.0,
        "slope": 120,
        "total_yards": 3680,
        "yards": [200, 297, 207, 80, 201, 81, 208, 279, 235, 229, 201, 94, 303, 253, 318, 62, 243, 189],
    },
]


def _georgia_hole_rows(tee_yards: Iterable[int]) -> list[dict]:
    rows: list[dict] = []
    for layout, yardage in zip(GEORGIA_HOLES, tee_yards):
        rows.append(
            {
                "hole_number": layout["hole"],
                "par": layout["par"],
                "handicap": layout["handicap"],
                "yardage": yardage,
            }
        )
    return rows


def _georgia_tee_payload(tee_info: dict) -> dict:
    return {
        "tee_name": tee_info["name"],
        "course_rating": tee_info["rating"],
        "slope_rating": tee_info["slope"],
        "bogey_rating": None,
        "total_yards": tee_info["total_yards"],
        "total_meters": None,
        "number_of_holes": 18,
        "par_total": 72,
        "front_course_rating": None,
        "back_course_rating": None,
        "front_slope_rating": None,
        "back_slope_rating": None,
        "front_bogey_rating": None,
        "back_bogey_rating": None,
    }


def ensure_georgia_course(database_url: str) -> int | None:
    catalog = fetch_course_catalog(database_url)
    existing = next(
        (entry for entry in catalog if (entry.get("course_name") or "").strip().lower() == GEORGIA_COURSE_NAME.lower()),
        None,
    )
    course_id = existing["id"] if existing else next_course_id(database_url)

    upsert_course(
        database_url,
        course_id,
        club_name=GEORGIA_CLUB_NAME,
        course_name=GEORGIA_COURSE_NAME,
        city=GEORGIA_LOCATION.get("city"),
        state=GEORGIA_LOCATION.get("state"),
        country=GEORGIA_LOCATION.get("country"),
        latitude=None,
        longitude=None,
        raw={},
    )

    for tee_info in GEORGIA_TEES:
        tee_id = upsert_course_tee(database_url, course_id, "male", _georgia_tee_payload(tee_info))
        replace_course_tee_holes(database_url, tee_id, _georgia_hole_rows(tee_info["yards"]))
    return course_id
