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


def fetch_recent_results(database_url: str, limit: int = 20) -> list[dict]:
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
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    submitted_at
                from match_results
                order by submitted_at desc
                limit %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [
                {
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
                for row in rows
            ]


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
