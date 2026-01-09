from typing import Optional

import random

import psycopg
from psycopg import sql
from psycopg.types.json import Json


def ensure_schema(database_url: str) -> None:
    statements = [
        """
        create table if not exists tournaments (
            id serial primary key,
            name text not null unique,
            description text,
            status text not null default 'upcoming',
            settings jsonb not null default '{}'::jsonb,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        );
        """,
        """
        create table if not exists players (
            id serial primary key,
            name text not null unique,
            division text not null,
            handicap integer not null default 0,
            seed integer not null default 0,
            tournament_id integer null references tournaments(id)
        );
        """,
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
        """,
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
        """,
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
        """,
        """
        create table if not exists matches (
            id serial primary key,
            tournament_id integer not null references tournaments(id) on delete cascade,
            match_key text not null unique,
            match_code text not null default '',
            division text not null,
            player_a_id integer not null references players(id),
            player_b_id integer not null references players(id),
            player_c_id integer null references players(id),
            player_d_id integer null references players(id),
            course_id integer null references courses(id),
            course_tee_id integer null references course_tees(id),
            player_a_handicap integer not null default 0,
            player_b_handicap integer not null default 0,
            status text not null default 'not_started',
            strokes jsonb,
            hole_count smallint not null default 18,
            active boolean not null default false,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        );
        """,
        """
        alter table matches add column if not exists player_c_id integer null references players(id);
        """,
        """
        alter table matches add column if not exists player_d_id integer null references players(id);
        """,
        """
        create table if not exists match_results (
            id serial primary key,
            match_name text not null,
            player_a_name text not null,
            player_b_name text not null,
            match_key text not null,
            match_code text not null,
            player_a_points double precision not null,
            player_b_points double precision not null,
            player_a_bonus double precision not null,
            player_b_bonus double precision not null,
            player_a_total double precision not null,
            player_b_total double precision not null,
            winner text not null,
            course_id integer null,
            course_tee_id integer null,
            tournament_id integer null,
            player_a_handicap integer not null default 0,
            player_b_handicap integer not null default 0,
            finalized boolean not null default false,
            course_snapshot jsonb,
            scorecard_snapshot jsonb,
            submitted_at timestamptz not null default now()
        );
        """,
        """
        create table if not exists hole_scores (
            id serial primary key,
            match_result_id integer not null references match_results(id) on delete cascade,
            hole_number smallint not null,
            player_a_score smallint not null,
            player_b_score smallint not null,
            recorded_at timestamptz not null default now()
        );
        """,
        """
        create table if not exists tournament_settings (
            key text primary key,
            value text not null
        );
        """,
        """
        create table if not exists course_holes (
            hole_number smallint primary key,
            par smallint not null,
            handicap smallint not null
        );
        """,
        """
        create table if not exists tournament_event_settings (
            id serial primary key,
            tournament_id integer not null references tournaments(id) on delete cascade,
            key text not null,
            value text not null,
            unique(tournament_id, key)
        );
        """,
        """
        create table if not exists standings_cache (
            id serial primary key,
            tournament_id integer not null references tournaments(id) on delete cascade,
            player_name text not null,
            division text not null,
            seed integer not null default 0,
            matches integer not null default 0,
            wins integer not null default 0,
            ties integer not null default 0,
            losses integer not null default 0,
            points_for double precision not null default 0,
            points_against double precision not null default 0,
            point_diff double precision not null default 0,
            holes_played integer not null default 0,
            updated_at timestamptz not null default now(),
            unique (tournament_id, player_name)
        );
        """,
        """
        create table if not exists match_groups (
            id serial primary key,
            group_key text not null unique,
            label text,
            match_keys text[] not null,
            player_pairs text[] not null default '{}'::text[],
            course jsonb not null default '{}'::jsonb,
            tournament_id integer null references tournaments(id) on delete cascade,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        );
        """,
    ]
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


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
                select id, name, division, handicap, seed, tournament_id
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
                    "tournament_id": row[5],
                }
                for row in rows
            ]


def fetch_player_by_name(database_url: str, name: str) -> dict[str, int] | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, name, division, handicap, seed, tournament_id
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
                "tournament_id": row[5],
            }


def fetch_player_by_id(database_url: str, player_id: int) -> dict[str, int] | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, name, division, handicap, seed, tournament_id
                from players
                where id = %s
                limit 1;
                """,
                (player_id,),
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
                "tournament_id": row[5],
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


def next_course_id(database_url: str) -> int:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("select coalesce(max(id), 0) + 1 from courses;")
            row = cur.fetchone()
            return row[0] if row else 1


def fetch_course_catalog(database_url: str) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select c.id, c.club_name, c.course_name, c.city, c.state, c.country, ct.id, ct.gender, ct.tee_name, ct.course_rating, ct.slope_rating, ct.par_total, ct.total_yards
                from courses c
                left join course_tees ct on c.id = ct.course_id
                order by c.course_name, ct.gender, ct.tee_name;
                """
            )
            catalog: dict[int, dict] = {}
            rows = cur.fetchall()
            for row in rows:
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
                            "total_yards": row[12],
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
    tournament_id: int | None = None,
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
                        seed = %s,
                        tournament_id = %s
                    where id = %s
                    returning id;
                    """,
                    (name, division, handicap, seed, tournament_id, player_id),
                )
            else:
                cur.execute(
                    """
                    insert into players (name, division, handicap, seed, tournament_id)
                    values (%s, %s, %s, %s, %s)
                    on conflict (name) do update
                        set division = excluded.division,
                        handicap = excluded.handicap,
                            seed = excluded.seed,
                            tournament_id = excluded.tournament_id
                    returning id;
                    """,
                    (name, division, handicap, seed, tournament_id),
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


def delete_player(database_url: str, player_id: int) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from players where id = %s;", (player_id,))


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


def delete_hole_scores(database_url: str, match_id: int) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from hole_scores where match_result_id = %s;", (match_id,))


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
                    tournament_id,
                    player_a_handicap,
                    player_b_handicap,
                    finalized,
                    course_snapshot,
                    scorecard_snapshot,
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
                "tournament_id": row[15],
                "player_a_handicap": row[16],
                "player_b_handicap": row[17],
                "finalized": row[18],
                "course_snapshot": row[19],
                "scorecard_snapshot": row[20],
                "submitted_at": row[21],
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
    tournament_id: int | None = None,
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
                    tournament_id,
                    player_a_handicap,
                    player_b_handicap
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    tournament_id,
                    player_a_handicap,
                    player_b_handicap,
                ),
            )
            row = cur.fetchone()
            return row[0] if row else None


def finalize_match_result(
    database_url: str,
    match_id: int,
    *,
    course_snapshot: dict | None = None,
    scorecard_snapshot: dict | None = None,
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update match_results
                set
                    finalized = true,
                    course_snapshot = %s,
                    scorecard_snapshot = %s
                where id = %s;
                """,
                (
                    Json(course_snapshot) if course_snapshot is not None else None,
                    Json(scorecard_snapshot) if scorecard_snapshot is not None else None,
                    match_id,
                ),
            )


def reset_match_results(
    database_url: str,
    match_ids: list[int],
    *,
    player_a_points: float,
    player_b_points: float,
    player_a_bonus: float,
    player_b_bonus: float,
    player_a_total: float,
    player_b_total: float,
    winner: str,
) -> None:
    if not match_ids:
        return
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from hole_scores
                where match_result_id = any(%s);
                """,
                (match_ids,),
            )
            cur.execute(
                """
                update match_results
                set
                    player_a_points = %s,
                    player_b_points = %s,
                    player_a_bonus = %s,
                    player_b_bonus = %s,
                    player_a_total = %s,
                    player_b_total = %s,
                    winner = %s,
                    finalized = false,
                    scorecard_snapshot = null,
                    course_snapshot = null
                where id = any(%s);
                """,
                (
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    match_ids,
                ),
            )


def update_match_result_fields(
    database_url: str,
    match_key: str,
    *,
    course_id: int | None = None,
    course_tee_id: int | None = None,
    player_a_handicap: int | None = None,
    player_b_handicap: int | None = None,
) -> None:
    updates = []
    values: list[int | None] = []
    if course_id is not None:
        updates.append("course_id = %s")
        values.append(course_id)
    if course_tee_id is not None:
        updates.append("course_tee_id = %s")
        values.append(course_tee_id)
    if player_a_handicap is not None:
        updates.append("player_a_handicap = %s")
        values.append(player_a_handicap)
    if player_b_handicap is not None:
        updates.append("player_b_handicap = %s")
        values.append(player_b_handicap)
    if not updates:
        return
    values.append(match_key)
    query = f"""
        update match_results
        set {', '.join(updates)}
        where match_key = %s
        """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(values))


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
                    tournament_id,
                    player_a_handicap,
                    player_b_handicap,
                    finalized,
                    course_snapshot,
                    scorecard_snapshot,
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
                "tournament_id": row[15],
                "player_a_handicap": row[16],
                "player_b_handicap": row[17],
                "finalized": row[18],
                "course_snapshot": row[19],
                "scorecard_snapshot": row[20],
                "submitted_at": row[21],
            }


def fetch_match_result_ids_by_key(database_url: str, match_key: str) -> list[int]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id
                from match_results
                where match_key = %s
                """,
                (match_key,),
            )
            return [row[0] for row in cur.fetchall()]


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
                    course_id,
                    course_tee_id,
                    tournament_id,
                    player_a_handicap,
                    player_b_handicap,
                    finalized,
                    course_snapshot,
                    scorecard_snapshot,
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
                "tournament_id": row[15],
                "player_a_handicap": row[16],
                "player_b_handicap": row[17],
                "finalized": row[18],
                "course_snapshot": row[19],
                "scorecard_snapshot": row[20],
                "submitted_at": row[21],
            }


def _row_to_match(row: tuple) -> dict:
    return {
        "id": row[0],
        "tournament_id": row[1],
        "match_key": row[2],
        "match_code": row[3],
        "division": row[4],
        "player_a_id": row[5],
        "player_b_id": row[6],
        "player_c_id": row[7],
        "player_d_id": row[8],
        "course_id": row[9],
        "course_tee_id": row[10],
        "player_a_handicap": row[11],
        "player_b_handicap": row[12],
        "status": row[13],
        "strokes": row[14],
        "hole_count": row[15],
        "active": row[16],
        "created_at": row[17],
        "updated_at": row[18],
        "player_a_name": row[19],
        "player_b_name": row[20],
        "player_c_name": row[21],
        "player_d_name": row[22],
    }


def insert_match(
    database_url: str,
    *,
    tournament_id: int,
    match_key: str,
    division: str,
    player_a_id: int,
    player_b_id: int,
    player_c_id: int | None = None,
    player_d_id: int | None = None,
    course_id: int | None = None,
    course_tee_id: int | None = None,
    player_a_handicap: int = 0,
    player_b_handicap: int = 0,
    hole_count: int = 18,
    strokes: dict | None = None,
    status: str = "not_started",
    active: bool = False,
) -> int | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into matches (
                    tournament_id,
                    match_key,
                    division,
                    player_a_id,
                    player_b_id,
                    player_c_id,
                    player_d_id,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap,
                    hole_count,
                    status,
                    active,
                    strokes
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id;
                """,
                (
                    tournament_id,
                    match_key,
                    division,
                    player_a_id,
                    player_b_id,
                    player_c_id,
                    player_d_id,
                    course_id,
                    course_tee_id,
                    player_a_handicap,
                    player_b_handicap,
                    hole_count,
                    status,
                    active,
                    Json(strokes) if strokes else None,
                ),
            )
            row = cur.fetchone()
            return row[0] if row else None


def fetch_match_by_key(database_url: str, match_key: str) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    m.id,
                    m.tournament_id,
                    m.match_key,
                    m.match_code,
                    m.division,
                    m.player_a_id,
                    m.player_b_id,
                    m.player_c_id,
                    m.player_d_id,
                    m.course_id,
                    m.course_tee_id,
                    m.player_a_handicap,
                    m.player_b_handicap,
                    m.status,
                    m.strokes,
                    m.hole_count,
                    m.active,
                    m.created_at,
                    m.updated_at,
                    pa.name,
                    pb.name,
                    pc.name,
                    pd.name
                from matches m
                left join players pa on pa.id = m.player_a_id
                left join players pb on pb.id = m.player_b_id
                left join players pc on pc.id = m.player_c_id
                left join players pd on pd.id = m.player_d_id
                where m.match_key = %s
                limit 1;
                """,
                (match_key,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return _row_to_match(row)


def fetch_match_by_id(database_url: str, match_id: int) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    m.id,
                    m.tournament_id,
                    m.match_key,
                    m.match_code,
                    m.division,
                    m.player_a_id,
                    m.player_b_id,
                    m.player_c_id,
                    m.player_d_id,
                    m.course_id,
                    m.course_tee_id,
                    m.player_a_handicap,
                    m.player_b_handicap,
                    m.status,
                    m.strokes,
                    m.hole_count,
                    m.active,
                    m.created_at,
                    m.updated_at,
                    pa.name,
                    pb.name,
                    pc.name,
                    pd.name
                from matches m
                left join players pa on pa.id = m.player_a_id
                left join players pb on pb.id = m.player_b_id
                left join players pc on pc.id = m.player_c_id
                left join players pd on pd.id = m.player_d_id
                where m.id = %s
                limit 1;
                """,
                (match_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return _row_to_match(row)


def fetch_matches_by_tournament(database_url: str, tournament_id: int) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    m.id,
                    m.tournament_id,
                    m.match_key,
                    m.match_code,
                    m.division,
                    m.player_a_id,
                    m.player_b_id,
                    m.player_c_id,
                    m.player_d_id,
                    m.course_id,
                    m.course_tee_id,
                    m.player_a_handicap,
                    m.player_b_handicap,
                    m.status,
                    m.strokes,
                    m.hole_count,
                    m.active,
                    m.created_at,
                    m.updated_at,
                    pa.name,
                    pb.name,
                    pc.name,
                    pd.name
                from matches m
                left join players pa on pa.id = m.player_a_id
                left join players pb on pb.id = m.player_b_id
                left join players pc on pc.id = m.player_c_id
                left join players pd on pd.id = m.player_d_id
                where m.tournament_id = %s
                order by m.division, m.match_key;
                """,
                (tournament_id,),
            )
            rows = cur.fetchall()
    return [_row_to_match(row) for row in rows]


def fetch_matches_by_keys(database_url: str, match_keys: list[str]) -> list[dict]:
    if not match_keys:
        return []
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    m.id,
                    m.tournament_id,
                    m.match_key,
                    m.match_code,
                    m.division,
                    m.player_a_id,
                    m.player_b_id,
                    m.player_c_id,
                    m.player_d_id,
                    m.course_id,
                    m.course_tee_id,
                    m.player_a_handicap,
                    m.player_b_handicap,
                    m.status,
                    m.strokes,
                    m.hole_count,
                    m.active,
                    m.created_at,
                    m.updated_at,
                    pa.name,
                    pb.name,
                    pc.name,
                    pd.name
                from matches m
                left join players pa on pa.id = m.player_a_id
                left join players pb on pb.id = m.player_b_id
                left join players pc on pc.id = m.player_c_id
                left join players pd on pd.id = m.player_d_id
                where m.match_key = any(%s)
                order by m.match_key;
                """,
                (match_keys,),
            )
            rows = cur.fetchall()
    return [_row_to_match(row) for row in rows]


def update_match(
    database_url: str,
    match_id: int,
    *,
    division: str | None = None,
    player_a_id: int | None = None,
    player_b_id: int | None = None,
    player_c_id: int | None = None,
    player_d_id: int | None = None,
    course_id: int | None = None,
    course_tee_id: int | None = None,
    player_a_handicap: int | None = None,
    player_b_handicap: int | None = None,
    status: str | None = None,
    strokes: dict | None = None,
    hole_count: int | None = None,
    active: bool | None = None,
) -> None:
    updates = []
    values: list = []
    if division is not None:
        updates.append("division = %s")
        values.append(division)
    if player_a_id is not None:
        updates.append("player_a_id = %s")
        values.append(player_a_id)
    if player_b_id is not None:
        updates.append("player_b_id = %s")
        values.append(player_b_id)
    if player_c_id is not None:
        updates.append("player_c_id = %s")
        values.append(player_c_id)
    if player_d_id is not None:
        updates.append("player_d_id = %s")
        values.append(player_d_id)
    if course_id is not None:
        updates.append("course_id = %s")
        values.append(course_id)
    if course_tee_id is not None:
        updates.append("course_tee_id = %s")
        values.append(course_tee_id)
    if player_a_handicap is not None:
        updates.append("player_a_handicap = %s")
        values.append(player_a_handicap)
    if player_b_handicap is not None:
        updates.append("player_b_handicap = %s")
        values.append(player_b_handicap)
    if status is not None:
        updates.append("status = %s")
        values.append(status)
    if strokes is not None:
        updates.append("strokes = %s")
        values.append(Json(strokes))
    if hole_count is not None:
        updates.append("hole_count = %s")
        values.append(hole_count)
    if active is not None:
        updates.append("active = %s")
        values.append(active)
    if not updates:
        return
    updates.append("updated_at = now()")
    query = f"""
        update matches
        set {', '.join(updates)}
        where id = %s;
        """
    values.append(match_id)
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(values))


def delete_match(database_url: str, match_id: int) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from matches where id = %s;", (match_id,))


def delete_match_results_by_key(database_url: str, match_key: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from match_results where match_key = %s;", (match_key,))


def fetch_match_group_definitions(database_url: str, tournament_id: int | None = None) -> list[dict]:
    filters = ""
    params: tuple = ()
    if tournament_id is not None:
        filters = "where tournament_id = %s or tournament_id is null"
        params = (tournament_id,)
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                    group_key,
                    coalesce(label, '') as label,
                    match_keys,
                    coalesce(player_pairs, array[]::text[]) as player_pairs,
                    coalesce(course, '{{}}'::jsonb) as course,
                    tournament_id
                from match_groups
                {filters}
                order by created_at desc, group_key;
                """,
                params,
            )
            rows = cur.fetchall()
    return [
        {
            "group_key": row[0],
            "label": row[1],
            "match_keys": row[2] or [],
            "player_pairs": row[3] or [],
            "course": row[4] or {},
            "tournament_id": row[5],
        }
        for row in rows
    ]


def upsert_match_group_definition(database_url: str, definition: dict) -> None:
    match_keys = [str(key) for key in (definition.get("match_keys") or []) if key]
    if not match_keys:
        return
    group_key = str(definition.get("group_key") or match_keys[0])
    label = str(definition.get("label") or "")
    player_pairs = [str(pair) for pair in (definition.get("player_pairs") or []) if pair]
    course = definition.get("course") or {}
    tournament_id = definition.get("tournament_id")
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into match_groups (
                    group_key,
                    label,
                    match_keys,
                    player_pairs,
                    course,
                    tournament_id
                )
                values (%s, %s, %s, %s, %s, %s)
                on conflict (group_key) do update
                    set
                        label = excluded.label,
                        match_keys = excluded.match_keys,
                        player_pairs = excluded.player_pairs,
                        course = excluded.course,
                        tournament_id = excluded.tournament_id,
                        updated_at = now();
                """,
                (
                    group_key,
                    label,
                    match_keys,
                    player_pairs,
                    Json(course),
                    tournament_id,
                ),
            )


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


def fetch_tournaments(database_url: str) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    name,
                    coalesce(description, '') as description,
                    status,
                    settings,
                    created_at,
                    updated_at
                from tournaments
                order by created_at desc, id desc;
                """
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "status": row[3],
            "settings": row[4] or {},
            "created_at": row[5],
            "updated_at": row[6],
        }
        for row in rows
    ]


def insert_tournament(
    database_url: str,
    name: str,
    description: str | None = None,
    status: str = "upcoming",
    settings: dict | None = None,
) -> int | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into tournaments (name, description, status, settings)
                values (%s, %s, %s, %s)
                returning id;
                """,
                (
                    name,
                    description,
                    status,
                    Json(settings or {}),
                ),
            )
            row = cur.fetchone()
            return row[0] if row else None


def update_tournament_status(database_url: str, tournament_id: int, status: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update tournaments
                set status = %s,
                    updated_at = now()
                where id = %s;
                """,
                (status, tournament_id),
            )


def fetch_tournament_by_id(database_url: str, tournament_id: int) -> dict | None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, name, description, status, settings, created_at, updated_at
                from tournaments
                where id = %s
                limit 1;
                """,
                (tournament_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "status": row[3],
                "settings": row[4] or {},
                "created_at": row[5],
                "updated_at": row[6],
            }


def fetch_event_settings(database_url: str, tournament_id: int) -> dict[str, str]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select key, value
                from tournament_event_settings
                where tournament_id = %s;
                """,
                (tournament_id,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def upsert_event_setting(database_url: str, tournament_id: int, key: str, value: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into tournament_event_settings (tournament_id, key, value)
                values (%s, %s, %s)
                on conflict (tournament_id, key) do update
                    set value = excluded.value;
                """,
                (tournament_id, key, value),
        )


def delete_match_results_by_tournament(database_url: str, tournament_id: int) -> int:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from match_results
                where tournament_id = %s
                returning id;
                """,
                (tournament_id,),
            )
            deleted = cur.fetchall()
    return len(deleted)


def replace_standings_cache(
    database_url: str,
    tournament_id: int,
    entries: list[dict],
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from standings_cache
                where tournament_id = %s;
                """,
                (tournament_id,),
            )
            if not entries:
                return
            for entry in entries:
                cur.execute(
                    """
                    insert into standings_cache (
                        tournament_id,
                        player_name,
                        division,
                        seed,
                        matches,
                        wins,
                        ties,
                        losses,
                        points_for,
                        points_against,
                        point_diff,
                        holes_played
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        tournament_id,
                        entry["player_name"],
                        entry["division"],
                        entry.get("seed", 0),
                        entry["matches"],
                        entry["wins"],
                        entry["ties"],
                        entry["losses"],
                        entry["points_for"],
                        entry["points_against"],
                        entry["point_diff"],
                        entry["holes_played"],
                    ),
                )


def fetch_standings_cache(database_url: str, tournament_id: int) -> list[dict]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    player_name,
                    division,
                    seed,
                    matches,
                    wins,
                    ties,
                    losses,
                    points_for,
                    points_against,
                    point_diff,
                    holes_played
                from standings_cache
                where tournament_id = %s
                order by division, points_for desc, wins desc, ties desc, player_name;
                """,
                (tournament_id,),
            )
            rows = cur.fetchall()
    return [
        {
            "player_name": row[0],
            "division": row[1],
            "seed": row[2],
            "matches": row[3],
            "wins": row[4],
            "ties": row[5],
            "losses": row[6],
            "points_for": row[7],
            "points_against": row[8],
            "point_diff": row[9],
            "holes_played": row[10],
        }
        for row in rows
    ]


def update_match_result_scores(
    database_url: str,
    match_id: int,
    *,
    player_a_points: float,
    player_b_points: float,
    player_a_bonus: float,
    player_b_bonus: float,
    player_a_total: float,
    player_b_total: float,
    winner: str,
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update match_results
                set
                    player_a_points = %s,
                    player_b_points = %s,
                    player_a_bonus = %s,
                    player_b_bonus = %s,
                    player_a_total = %s,
                    player_b_total = %s,
                    winner = %s
                where id = %s;
                """,
                (
                    player_a_points,
                    player_b_points,
                    player_a_bonus,
                    player_b_bonus,
                    player_a_total,
                    player_b_total,
                    winner,
                    match_id,
                ),
            )
