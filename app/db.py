from typing import Optional

import random

import psycopg
from psycopg import sql
from psycopg.types.json import Json


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
                    player_a_points double precision not null,
                    player_b_points double precision not null,
                    player_a_bonus double precision not null,
                    player_b_bonus double precision not null,
                    player_a_total double precision not null,
                    player_b_total double precision not null,
                    winner text not null,
                    course_id integer null,
                    course_tee_id integer null,
                    player_a_handicap integer not null default 0,
                    player_b_handicap integer not null default 0,
                    submitted_at timestamptz not null default now()
                );
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_a_points type double precision using player_a_points::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_b_points type double precision using player_b_points::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_a_bonus type double precision using player_a_bonus::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_b_bonus type double precision using player_b_bonus::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_a_total type double precision using player_a_total::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                alter column player_b_total type double precision using player_b_total::double precision;
                """
            )
            cur.execute(
                """
                alter table match_results
                add column if not exists course_id integer null;
                """
            )
            cur.execute(
                """
                alter table match_results
                add column if not exists course_tee_id integer null;
                """
            )
            cur.execute(
                """
                alter table match_results
                add column if not exists player_a_handicap integer not null default 0;
                """
            )
            cur.execute(
                """
                alter table match_results
                add column if not exists player_b_handicap integer not null default 0;
                """
            )
            cur.execute(
                """
                alter table match_results
                add column if not exists match_code text not null default '';
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
            cur.execute(
                """
                create table if not exists course_holes (
                    hole_number smallint primary key,
                    par smallint not null,
                    handicap smallint not null
                );
                """
            )
            cur.execute(
                """
                create table if not exists courses (
                    id integer primary key,
                    club_name text not null,
                    course_name text not null,
                    city text,
                    state text,
                    country text,
                    latitude double precision,
                    longitude double precision,
                    raw jsonb
                );
                """
            )
            cur.execute(
                """
                create table if not exists course_tees (
                    id serial primary key,
                    course_id integer not null references courses(id) on delete cascade,
                    gender text not null,
                    tee_name text not null,
                    course_rating double precision,
                    slope_rating integer,
                    bogey_rating double precision,
                    total_yards integer,
                    total_meters integer,
                    number_of_holes integer,
                    par_total integer,
                    front_course_rating double precision,
                    back_course_rating double precision,
                    front_slope_rating integer,
                    back_slope_rating integer,
                    front_bogey_rating double precision,
                    back_bogey_rating double precision,
                    unique (course_id, gender, tee_name)
                );
                """
            )
            cur.execute(
                """
                create table if not exists course_tee_holes (
                    id serial primary key,
                    course_tee_id integer not null references course_tees(id) on delete cascade,
                    hole_number smallint not null,
                    par smallint not null,
                    handicap smallint not null,
                    yardage integer,
                    unique (course_tee_id, hole_number)
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


def fetch_course_holes(database_url: str) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select hole_number, par, handicap
                from course_holes
                order by hole_number;
                """
            )
            return [
                {
                    "hole_number": row[0],
                    "par": row[1],
                    "handicap": row[2],
                }
                for row in cur.fetchall()
            ]


def replace_course_holes(database_url: str, holes: list[dict]) -> None:
    if not holes:
        return
    values = [
        (entry["hole_number"], entry["par"], entry["handicap"])
        for entry in holes
    ]
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from course_holes;")
            cur.executemany(
                """
                insert into course_holes (hole_number, par, handicap)
                values (%s, %s, %s)
                """,
                values,
            )


def upsert_course(
    database_url: str,
    course_id: int,
    club_name: str,
    course_name: str,
    city: str | None,
    state: str | None,
    country: str | None,
    latitude: float | None,
    longitude: float | None,
    raw: dict | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into courses (id, club_name, course_name, city, state, country, latitude, longitude, raw)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (id) do update set
                    club_name = excluded.club_name,
                    course_name = excluded.course_name,
                    city = excluded.city,
                    state = excluded.state,
                    country = excluded.country,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    raw = excluded.raw;
                """,
                (
                    course_id,
                    club_name,
                    course_name,
                    city,
                    state,
                    country,
                    latitude,
                    longitude,
                    Json(raw),
                ),
            )


def upsert_course_tee(
    database_url: str,
    course_id: int,
    gender: str,
    tee: dict,
) -> int:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into course_tees (
                    course_id,
                    gender,
                    tee_name,
                    course_rating,
                    slope_rating,
                    bogey_rating,
                    total_yards,
                    total_meters,
                    number_of_holes,
                    par_total,
                    front_course_rating,
                    back_course_rating,
                    front_slope_rating,
                    back_slope_rating,
                    front_bogey_rating,
                    back_bogey_rating
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (course_id, gender, tee_name) do update set
                    course_rating = excluded.course_rating,
                    slope_rating = excluded.slope_rating,
                    bogey_rating = excluded.bogey_rating,
                    total_yards = excluded.total_yards,
                    total_meters = excluded.total_meters,
                    number_of_holes = excluded.number_of_holes,
                    par_total = excluded.par_total,
                    front_course_rating = excluded.front_course_rating,
                    back_course_rating = excluded.back_course_rating,
                    front_slope_rating = excluded.front_slope_rating,
                    back_slope_rating = excluded.back_slope_rating,
                    front_bogey_rating = excluded.front_bogey_rating,
                    back_bogey_rating = excluded.back_bogey_rating
                returning id;
                """,
                (
                    course_id,
                    gender,
                    tee.get("tee_name"),
                    tee.get("course_rating"),
                    tee.get("slope_rating"),
                    tee.get("bogey_rating"),
                    tee.get("total_yards"),
                    tee.get("total_meters"),
                    tee.get("number_of_holes"),
                    tee.get("par_total"),
                    tee.get("front_course_rating"),
                    tee.get("back_course_rating"),
                    tee.get("front_slope_rating"),
                    tee.get("back_slope_rating"),
                    tee.get("front_bogey_rating"),
                    tee.get("back_bogey_rating"),
                ),
            )
            row = cur.fetchone()
            return row[0] if row else 0


def replace_course_tee_holes(
    database_url: str,
    course_tee_id: int,
    holes: list[dict],
) -> None:
    if not holes:
        return
    values = [
        (course_tee_id, entry.get("hole_number"), entry.get("par"), entry.get("handicap"), entry.get("yardage"))
        for entry in holes
        if entry.get("hole_number")
    ]
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from course_tee_holes where course_tee_id = %s;", (course_tee_id,))
            cur.executemany(
                """
                insert into course_tee_holes (course_tee_id, hole_number, par, handicap, yardage)
                values (%s, %s, %s, %s, %s)
                """,
                values,
            )


def fetch_course_catalog(database_url: str) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select c.id, c.club_name, c.course_name, c.city, c.state, c.country, ct.id, ct.gender, ct.tee_name, ct.course_rating, ct.slope_rating, ct.par_total
                from courses c
                left join course_tees ct on c.id = ct.course_id
                order by c.course_name, ct.gender, ct.tee_name;
                """
            )
            catalog: dict[int, dict] = {}
            for row in cur.fetchall():
                course_id = row[0]
                course_entry = catalog.setdefault(
                    course_id,
                    {
                        "id": course_id,
                        "club_name": row[1],
                        "course_name": row[2],
                        "city": row[3],
                        "state": row[4],
                        "country": row[5],
                        "tees": [],
                    },
                )
                tee_id = row[6]
                if tee_id:
                    course_entry["tees"].append(
                        {
                            "id": tee_id,
                            "gender": row[7],
                            "tee_name": row[8],
                            "course_rating": row[9],
                            "slope_rating": row[10],
                            "par_total": row[11],
                        }
                    )
            return list(catalog.values())


def fetch_course_tee_holes(database_url: str, course_tee_id: int) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select hole_number, par, handicap, yardage
                from course_tee_holes
                where course_tee_id = %s
                order by hole_number;
                """,
                (course_tee_id,),
            )
            return [
                {
                    "hole_number": row[0],
                    "par": row[1],
                    "handicap": row[2],
                    "yardage": row[3],
                }
                for row in cur.fetchall()
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
    hole_numbers = [entry["hole_number"] for entry in hole_entries]
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from hole_scores where match_result_id = %s and hole_number = any(%s);",
                (match_id, hole_numbers),
            )
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
                    match_key,
                    match_code,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap,
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
                "match_key": row[4],
                "match_code": row[5],
                "player_a_points": row[6],
                "player_b_points": row[7],
                "player_a_bonus": row[8],
                "player_b_bonus": row[9],
                "player_a_total": row[10],
                "player_b_total": row[11],
                "winner": row[12],
                "course_id": row[13],
                "course_tee_id": row[14],
                "player_a_handicap": row[15],
                "player_b_handicap": row[16],
                "submitted_at": row[17],
            }


def insert_match_result(
    database_url: str,
    match_name: str,
    player_a: str,
    player_b: str,
    match_key: str,
    match_code: str | None,
    player_a_points: float,
    player_b_points: float,
    player_a_bonus: float,
    player_b_bonus: float,
    player_a_total: float,
    player_b_total: float,
    winner: str,
    course_id: int | None = None,
    course_tee_id: int | None = None,
    player_a_handicap: int = 0,
    player_b_handicap: int = 0,
) -> Optional[int]:
    def _next_code(cur) -> str:
        while True:
            code = "".join(str(random.randint(0, 9)) for _ in range(9))
            cur.execute("select 1 from match_results where match_code = %s limit 1;", (code,))
            if not cur.fetchone():
                return code

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            code = match_code or _next_code(cur)
            cur.execute(
                """
                insert into match_results (
                    match_name,
                    player_a_name,
                    player_b_name,
                    match_key,
                    match_code,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id;
                """,
                (
                    match_name,
                    player_a,
                    player_b,
                    match_key,
                    code,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap,
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
                    match_key,
                    match_code,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap,
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
                "match_key": row[4],
                "match_code": row[5],
                "player_a_points": row[6],
                "player_b_points": row[7],
                "player_a_bonus": row[8],
                "player_b_bonus": row[9],
                "player_a_total": row[10],
                "player_b_total": row[11],
                "winner": row[12],
                "course_id": row[13],
                "course_tee_id": row[14],
                "player_a_handicap": row[15],
                "player_b_handicap": row[16],
                "submitted_at": row[17],
            }


def fetch_match_result_by_code(database_url: str, match_code: str) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    match_name,
                    player_a_name,
                    player_b_name,
                    match_key,
                    match_code,
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    submitted_at
                from match_results
                where match_code = %s
                order by submitted_at desc
                limit 1;
                """,
                (match_code,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "match_name": row[1],
                "player_a_name": row[2],
                "player_b_name": row[3],
                "match_key": row[4],
                "match_code": row[5],
                "player_a_points": row[6],
                "player_b_points": row[7],
                "player_a_bonus": row[8],
                "player_b_bonus": row[9],
                "player_a_total": row[10],
                "player_b_total": row[11],
                "winner": row[12],
                "course_id": row[13],
                "course_tee_id": row[14],
                "player_a_handicap": row[15],
                "player_b_handicap": row[16],
                "submitted_at": row[17],
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
