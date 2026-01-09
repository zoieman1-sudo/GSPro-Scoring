import re
from typing import Callable, Tuple

import psycopg

MigrationTask = Tuple[str, str, Callable[[psycopg.Cursor], None]]


def _normalize_pair(pair_text: str) -> tuple[str, str] | None:
    if not pair_text:
        return None
    segments = re.split(r"\s+vs\s+", pair_text, flags=re.IGNORECASE)
    if len(segments) != 2:
        return None
    left, right = segments[0].strip(), segments[1].strip()
    if not left or not right:
        return None
    return left, right


def _match_key_for_pair(cursor: psycopg.Cursor, first: str, second: str) -> str | None:
    normalized = first.lower()
    normalized_second = second.lower()
    cursor.execute(
        """
        select m.match_key
        from matches m
        left join players pa on pa.id = m.player_a_id
        left join players pb on pb.id = m.player_b_id
        where (
            pa.name is not null
            and pb.name is not null
            and lower(pa.name) = %s
            and lower(pb.name) = %s
        )
        or (
            pa.name is not null
            and pb.name is not null
            and lower(pa.name) = %s
            and lower(pb.name) = %s
        )
        order by m.id
        limit 1;
        """,
        (normalized, normalized_second, normalized_second, normalized),
    )
    match_row = cursor.fetchone()
    return match_row[0] if match_row else None


def _backfill_match_group_keys(cursor: psycopg.Cursor) -> None:
    cursor.execute(
        """
        select
            id,
            match_keys,
            player_pairs
        from match_groups
        order by id;
        """
    )
    entries = cursor.fetchall()
    for entry_id, existing_keys, player_pairs in entries:
        match_keys = list(existing_keys or [])
        pairs = list(player_pairs or [])
        if not pairs:
            continue
        new_keys = match_keys[:]
        for pair in pairs:
            normalized_pair = _normalize_pair(pair)
            if not normalized_pair:
                continue
            first_name, second_name = normalized_pair
            candidate = _match_key_for_pair(cursor, first_name, second_name)
            if candidate and candidate not in new_keys:
                new_keys.append(candidate)
        if len(new_keys) > len(match_keys):
            cursor.execute(
                """
                update match_groups
                set match_keys = %s,
                    updated_at = now()
                where id = %s;
                """,
                (new_keys, entry_id),
            )


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


MIGRATIONS: list[MigrationTask] = [
    (
        "20240426_match_group_match_keys",
        "Backfill match group entries so both match keys are stored",
        _backfill_match_group_keys,
    ),
]


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
