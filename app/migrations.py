from typing import Callable, Tuple

import psycopg

MigrationTask = Tuple[str, str, Callable[[psycopg.Cursor], None]]


def _ensure_migrations_table(cursor: psycopg.Cursor) -> None:
    cursor.execute(
        """
        create table if not exists schema_migrations (
            id text primary key,
            description text not null,
            applied_at timestamptz not null default now()
        );
        """
    )


MIGRATIONS: list[MigrationTask] = []


def apply_migrations(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            _ensure_migrations_table(cursor)
        connection.commit()
        for migration_id, description, task in MIGRATIONS:
            with connection.cursor() as cursor:
                cursor.execute(
                    "select 1 from schema_migrations where id = %s;",
                    (migration_id,),
                )
                if cursor.fetchone():
                    continue
                task(cursor)
                cursor.execute(
                    """
                    insert into schema_migrations (id, description)
                    values (%s, %s);
                    """,
                    (migration_id, description),
                )
            connection.commit()


if __name__ == "__main__":
    from app.settings import load_settings

    apply_migrations(load_settings().database_url)
