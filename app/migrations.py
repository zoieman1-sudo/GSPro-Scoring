from typing import Callable, Tuple

import sqlite3

from app.db import _connect
from app.settings import load_settings

MigrationTask = Tuple[str, str, Callable[[sqlite3.Cursor], None]]


def _ensure_migrations_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )


MIGRATIONS: list[MigrationTask] = []


def apply_migrations(database_url: str) -> None:
    with _connect(database_url) as connection:
        cursor = connection.cursor()
        _ensure_migrations_table(cursor)
        existing = {row["id"] for row in cursor.execute("SELECT id FROM schema_migrations")}
        for migration_id, description, task in MIGRATIONS:
            if migration_id in existing:
                continue
            task(cursor)
            cursor.execute(
                """
                INSERT INTO schema_migrations (id, description)
                VALUES (?, ?);
                """,
                (migration_id, description),
            )
        connection.commit()


if __name__ == "__main__":
    apply_migrations(load_settings().database_url)
