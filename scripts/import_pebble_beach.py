from __future__ import annotations

from app.course_sync import ensure_pebble_beach_course
from app.settings import load_settings


def main() -> None:
    settings = load_settings()
    course_id = ensure_pebble_beach_course(settings.database_url)
    if course_id:
        print(f"Pebble Beach course ensured with id {course_id}.")
    else:
        raise SystemExit("Failed to import Pebble Beach course.")


if __name__ == "__main__":
    main()
