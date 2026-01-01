from typing import Optional

import psycopg
from psycopg import sql


def ensure_schema(database_url: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists match_results (
                    id serial primary key,
                    match_name text not null,
                    player_a_name text not null,
                    player_b_name text not null,
                    match_key text not null,
                    player_a_points integer not null,
                    player_b_points integer not null,
                    player_a_bonus integer not null,
                    player_b_bonus integer not null,
                player_a_total integer not null,
                    player_b_total integer not null,
                    winner text not null,
                    submitted_at timestamptz not null default now()
                    );
                    """
                )
            cur.execute(
                """
                alter table match_results
                add column if not exists match_key text not null default '';
                """
            )
            cur.execute(
                """
                create table if not exists players (
                    id serial primary key,
                    name text not null unique,
                    division text not null,
                    handicap integer not null default 0,
                    seed integer not null default 0
                );
                """
            )
            cur.execute(
                """
                create table if not exists hole_scores (
                    id serial primary key,
                    match_result_id integer not null references match_results(id) on delete cascade,
                    hole_number smallint not null,
                    player_a_score smallint not null,
                    player_b_score smallint not null,
                    recorded_at timestamptz not null default now()
                );
                """
            )
            cur.execute(
                """
                alter table players
                add column if not exists seed integer not null default 0;
                """
            )
            cur.execute(
                """
                create table if not exists tournament_settings (
                    key text primary key,
                    value text not null
                );
                """
            )


def _row_to_result(row: tuple) -> dict:
    return {
        "id": row[0],
        "match_name": row[1],
        "player_a_name": row[2],
        "player_b_name": row[3],
        "player_a_points": row[4],
        "player_b_points": row[5],
        "player_a_bonus": row[6],
        "player_b_bonus": row[7],
        "player_a_total": row[8],
        "player_b_total": row[9],
        "winner": row[10],
        "submitted_at": row[11],
    }


def _fetch_results(database_url: str, limit: int | None = None) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            query = """
                select
                    id,
                    match_name,
                    player_a_name,
                    player_b_name,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    submitted_at
                from match_results
                order by submitted_at desc
            """
            params: tuple = ()
            if limit is not None:
                query += "\n                limit %s;"
                params = (limit,)
            else:
                query += ";"
            cur.execute(query, params)
            rows = cur.fetchall()
            return [_row_to_result(row) for row in rows]


def fetch_recent_results(database_url: str, limit: int = 20) -> list[dict]:
    return _fetch_results(database_url, limit=limit)


def fetch_all_match_results(database_url: str) -> list[dict]:
    return _fetch_results(database_url, limit=None)


def fetch_players(database_url: str) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, name, division, handicap, seed
                from players
                order by division, name;
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "division": row[2],
                    "handicap": row[3],
                    "seed": row[4],
                }
                for row in rows
            ]


def fetch_player_by_name(database_url: str, name: str) -> dict[str, int] | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, name, division, handicap, seed
                from players
                where name = %s
                limit 1;
                """,
                (name,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "division": row[2],
                "handicap": row[3],
                "seed": row[4],
            }


def upsert_player(
    database_url: str,
    player_id: int | None,
    name: str,
    division: str,
    handicap: int,
    seed: int,
) -> int:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if player_id:
                cur.execute(
                    """
                    update players
                    set name = %s,
                        division = %s,
                        handicap = %s,
                        seed = %s
                    where id = %s
                    returning id;
                    """,
                    (name, division, handicap, seed, player_id),
                )
            else:
                cur.execute(
                    """
                    insert into players (name, division, handicap, seed)
                    values (%s, %s, %s, %s)
                    on conflict (name) do update
                        set division = excluded.division,
                            handicap = excluded.handicap,
                            seed = excluded.seed
                    returning id;
                    """,
                    (name, division, handicap, seed),
                )
            row = cur.fetchone()
            return row[0] if row else 0


def delete_players_not_in(database_url: str, names: list[str]) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if not names:
                cur.execute("delete from players;")
                return

            placeholders = sql.SQL(", ").join(sql.Placeholder() * len(names))
            query = sql.SQL(
                """
                delete from players
                where name not in ({})
                """
            ).format(placeholders)
            cur.execute(query, names)


def insert_hole_scores(
    database_url: str,
    match_id: int,
    hole_entries: list[dict],
) -> None:
    values = [
        (
            match_id,
            entry["hole_number"],
            entry["player_a_score"],
            entry["player_b_score"],
        )
        for entry in hole_entries
    ]
    if not values:
        return
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into hole_scores (match_result_id, hole_number, player_a_score, player_b_score)
                values (%s, %s, %s, %s)
                """,
                values,
            )


def fetch_hole_scores(database_url: str, match_id: int) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select hole_number, player_a_score, player_b_score
                from hole_scores
                where match_result_id = %s
                order by hole_number;
                """,
                (match_id,),
            )
            return [
                {
                    "hole_number": row[0],
                    "player_a_score": row[1],
                    "player_b_score": row[2],
                }
                for row in cur.fetchall()
            ]


def fetch_match_result(database_url: str, match_id: int) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    match_name,
                    player_a_name,
                    player_b_name,
                    player_a_points,
                    player_b_points,
                    winner,
                    submitted_at
                from match_results
                where id = %s;
                """,
                (match_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "match_name": row[1],
                "player_a_name": row[2],
                "player_b_name": row[3],
                "player_a_points": row[4],
                "player_b_points": row[5],
                "winner": row[6],
                "submitted_at": row[7],
            }


def insert_match_result(
    database_url: str,
    match_name: str,
    player_a: str,
    player_b: str,
    match_key: str,
    player_a_points: int,
    player_b_points: int,
    player_a_bonus: int,
    player_b_bonus: int,
    player_a_total: int,
    player_b_total: int,
    winner: str,
) -> Optional[int]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into match_results (
                    match_name,
                    player_a_name,
                    player_b_name,
                    match_key,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id;
                """,
                (
                    match_name,
                    player_a,
                    player_b,
                    match_key,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
            ),
        )
        row = cur.fetchone()
        return row[0] if row else None


def fetch_match_result_by_key(database_url: str, match_key: str) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    match_name,
                    player_a_name,
                    player_b_name,
                    player_a_points,
                    player_b_points,
                    winner,
                    submitted_at
                from match_results
                where match_key = %s
                order by submitted_at desc
                limit 1;
                """,
                (match_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "match_name": row[1],
                "player_a_name": row[2],
                "player_b_name": row[3],
                "player_a_points": row[4],
                "player_b_points": row[5],
                "winner": row[6],
                "submitted_at": row[7],
            }


def upsert_setting(database_url: str, key: str, value: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into tournament_settings (key, value)
                values (%s, %s)
                on conflict (key) do update
                    set value = excluded.value;
                """,
                (key, value),
            )


def fetch_settings(database_url: str) -> dict[str, str]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select key, value
                from tournament_settings
                """
            )
            return {row[0]: row[1] for row in cur.fetchall()}
