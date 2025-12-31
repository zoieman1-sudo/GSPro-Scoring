from typing import Optional

import psycopg


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
                alter table players
                add column if not exists seed integer not null default 0;
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


def insert_match_result(
    database_url: str,
    match_name: str,
    player_a: str,
    player_b: str,
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
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id;
                """,
                (
                    match_name,
                    player_a,
                    player_b,
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
