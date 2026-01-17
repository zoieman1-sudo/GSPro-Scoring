#!/usr/bin/env python3
"""Ensure the bundled SQLite schema exists and echo the DDL for reference."""

from pathlib import Path

from app.db import SCHEMA_STATEMENTS
from app.seed_db import ensure_base_tournament
from app.settings import load_settings


def _sqlite_path(database_url: str) -> Path | None:
    if database_url.startswith("sqlite:///"):
        return Path(database_url[len("sqlite:///"):]).resolve()
    if database_url.startswith("sqlite://"):
        return Path(database_url[len("sqlite://"):]).resolve()
    if database_url.startswith("sqlite:"):
        return Path(database_url[len("sqlite:"):]).resolve()
    return None


def main() -> None:
    ensure_base_tournament()
    settings = load_settings()
    db_path = _sqlite_path(settings.database_url)
    print("SQLite schema ensured.")
    if db_path:
        print(f"Database file: {db_path}")
    else:
        print(f"Database url: {settings.database_url}")

    print("\nSchema DDL dump:")
    for statement in SCHEMA_STATEMENTS:
        print(statement.strip())


if __name__ == "__main__":
    main()
