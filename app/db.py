from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Optional, Sequence

import json
import random
import sqlite3
from pathlib import Path

DEFAULT_DB_FILE = Path(__file__).resolve().parent / "DATA" / "gspro_scoring.db"


def _database_path(database_url: str) -> Path:
    cleaned = database_url.strip()
    if cleaned.startswith("sqlite:///"):
        path = cleaned[len("sqlite:///") :]
    elif cleaned.startswith("sqlite://"):
        path = cleaned[len("sqlite://") :]
    elif cleaned.startswith("sqlite:"):
        path = cleaned[len("sqlite:") :]
    else:
        path = cleaned
    if path in ("", ":memory:"):
        return Path(path)
    return Path(path)


def _connect(database_url: str) -> sqlite3.Connection:
    path = _database_path(database_url) or DEFAULT_DB_FILE
    if path != Path(":memory:"):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def _prepare(query: str) -> str:
    return query.replace("%s", "?")


def _json_value(value: dict | None) -> Optional[str]:
    return json.dumps(value) if value is not None else None


@contextmanager
def _write_conn(database_url: str):
    conn = _connect(database_url)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_submitted_at_column(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(match_results);")
    columns = {row["name"] for row in cursor.fetchall()}
    if "submitted_at" not in columns:
        conn.execute(
            """
            ALTER TABLE match_results
            ADD COLUMN submitted_at TEXT NOT NULL DEFAULT (datetime('now'));
            """
        )


def ensure_schema(database_url: str) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'upcoming',
            settings TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            division TEXT NOT NULL,
            handicap INTEGER NOT NULL DEFAULT 0,
            seed INTEGER NOT NULL DEFAULT 0,
            tournament_id INTEGER REFERENCES tournaments(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY,
            club_name TEXT NOT NULL,
            course_name TEXT NOT NULL,
            city TEXT,
            state TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            raw TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS course_tees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
            gender TEXT NOT NULL,
            tee_name TEXT NOT NULL,
            course_rating REAL,
            slope_rating INTEGER,
            bogey_rating REAL,
            total_yards INTEGER,
            total_meters INTEGER,
            number_of_holes INTEGER,
            par_total INTEGER,
            front_course_rating REAL,
            back_course_rating REAL,
            front_slope_rating INTEGER,
            back_slope_rating INTEGER,
            front_bogey_rating REAL,
            back_bogey_rating REAL,
            UNIQUE (course_id, gender, tee_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS course_tee_holes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_tee_id INTEGER NOT NULL REFERENCES course_tees(id) ON DELETE CASCADE,
            hole_number INTEGER NOT NULL,
            par INTEGER NOT NULL,
            handicap INTEGER NOT NULL,
            yardage INTEGER,
            UNIQUE (course_tee_id, hole_number)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            match_key TEXT NOT NULL,
            division TEXT NOT NULL DEFAULT 'Open',
            player_a_id INTEGER NOT NULL REFERENCES players(id),
            player_b_id INTEGER NOT NULL REFERENCES players(id),
            player_c_id INTEGER REFERENCES players(id),
            player_d_id INTEGER REFERENCES players(id),
            course_id INTEGER REFERENCES courses(id),
            course_tee_id INTEGER REFERENCES course_tees(id),
            player_a_handicap INTEGER NOT NULL DEFAULT 0,
            player_b_handicap INTEGER NOT NULL DEFAULT 0,
            hole_count INTEGER NOT NULL DEFAULT 18,
            start_hole INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'not_started',
            finalized INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (tournament_id, match_key)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_name TEXT NOT NULL,
            player_a_name TEXT NOT NULL,
            player_b_name TEXT NOT NULL,
            match_key TEXT NOT NULL,
            match_code TEXT NOT NULL,
            player_a_points REAL NOT NULL,
            player_b_points REAL NOT NULL,
            player_a_bonus REAL NOT NULL,
            player_b_bonus REAL NOT NULL,
            player_a_total REAL NOT NULL,
            player_b_total REAL NOT NULL,
            winner TEXT NOT NULL,
            course_id INTEGER,
            course_tee_id INTEGER,
            tournament_id INTEGER,
            player_a_handicap INTEGER NOT NULL DEFAULT 0,
            player_b_handicap INTEGER NOT NULL DEFAULT 0,
            hole_count INTEGER NOT NULL DEFAULT 18,
            start_hole INTEGER NOT NULL DEFAULT 1,
            finalized INTEGER NOT NULL DEFAULT 0,
            player_a_id INTEGER,
            player_b_id INTEGER,
            player_c_id INTEGER,
            player_d_id INTEGER,
            course_snapshot TEXT,
            scorecard_snapshot TEXT,
            submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_bonus (
            match_result_id INTEGER PRIMARY KEY REFERENCES match_results(id) ON DELETE CASCADE,
            player_a_bonus REAL NOT NULL DEFAULT 0,
            player_b_bonus REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS hole_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_result_id INTEGER NOT NULL REFERENCES match_results(id) ON DELETE CASCADE,
            hole_number INTEGER NOT NULL,
            player_a_score INTEGER NOT NULL,
            player_b_score INTEGER NOT NULL,
            player_c_score INTEGER NOT NULL DEFAULT 0,
            player_d_score INTEGER NOT NULL DEFAULT 0,
            player_a_net REAL,
            player_b_net REAL,
            player_c_net REAL,
            player_d_net REAL,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS player_hole_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_result_id INTEGER NOT NULL REFERENCES match_results(id) ON DELETE CASCADE,
            match_key TEXT NOT NULL,
            player_index INTEGER NOT NULL,
            player_side TEXT NOT NULL,
            team_index INTEGER NOT NULL,
            player_name TEXT,
            opponent_name TEXT,
            hole_number INTEGER NOT NULL,
            gross_score INTEGER NOT NULL,
            stroke_adjustment REAL NOT NULL DEFAULT 0,
            course_id INTEGER,
            course_tee_id INTEGER,
            player_handicap INTEGER,
            net_score REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (match_result_id, player_index, hole_number)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tournament_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS course_holes (
            hole_number INTEGER PRIMARY KEY,
            par INTEGER NOT NULL,
            handicap INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tournament_event_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            UNIQUE(tournament_id, key)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS standings_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            player_name TEXT NOT NULL,
            division TEXT NOT NULL,
            seed INTEGER NOT NULL DEFAULT 0,
            matches INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            ties INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            points_for REAL NOT NULL DEFAULT 0,
            points_against REAL NOT NULL DEFAULT 0,
            point_diff REAL NOT NULL DEFAULT 0,
            holes_played INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (tournament_id, player_name)
        );
        """,
    ]
    with _write_conn(database_url) as conn:
        for statement in statements:
            conn.execute(statement)
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS matches_tournament_match_key_idx
            ON matches(tournament_id, match_key);
            """
        )
        _ensure_submitted_at_column(conn)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def _row_to_result(row: tuple) -> dict:
    return {
        "id": row[0],
        "match_name": row[1],
        "match_key": row[2],
        "player_a_id": row[3],
        "player_b_id": row[4],
        "player_a_name": row[5],
        "player_b_name": row[6],
        "player_a_points": row[7],
        "player_b_points": row[8],
        "player_a_bonus": row[9],
        "player_b_bonus": row[10],
        "player_a_total": row[11],
        "player_b_total": row[12],
        "winner": row[13],
        "tournament_id": row[14],
        "submitted_at": _parse_datetime(row[15]),
    }


def _fetch_results(database_url: str, limit: int | None = None) -> list[dict]:
    with _connect(database_url) as conn:
        query = """
            SELECT
                id,
                match_name,
                match_key,
                player_a_id,
                player_b_id,
                player_a_name,
                player_b_name,
                player_a_points,
                player_b_points,
                player_a_bonus,
                player_b_bonus,
                player_a_total,
                player_b_total,
                winner,
                tournament_id,
                submitted_at
            FROM match_results
            ORDER BY submitted_at DESC
        """
        params: tuple = ()
        if limit is not None:
            query += "\n            LIMIT ?;"
            params = (limit,)
        else:
            query += ";"
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        return [_row_to_result(row) for row in rows]


def fetch_recent_results(database_url: str, limit: int = 20) -> list[dict]:
    return _fetch_results(database_url, limit=limit)


def fetch_all_match_results(database_url: str) -> list[dict]:
    return _fetch_results(database_url, limit=None)


def delete_match_result(database_url: str, match_result_id: int) -> None:
    with _connect(database_url) as conn:
        conn.execute("DELETE FROM match_results WHERE id = ?;", (match_result_id,))


def fetch_players(database_url: str) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT id, name, division, handicap, seed, tournament_id
            FROM players
            ORDER BY division, name;
            """
        )
        rows = cursor.fetchall()
        return [
            {
                "id": row["id"],
                "name": row["name"],
                "division": row["division"],
                "handicap": row["handicap"],
                "seed": row["seed"],
                "tournament_id": row["tournament_id"],
            }
            for row in rows
        ]


def fetch_player_by_name(database_url: str, name: str) -> dict[str, int] | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT id, name, division, handicap, seed, tournament_id
            FROM players
            WHERE name = ?
            LIMIT 1;
            """,
            (name,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "division": row["division"],
            "handicap": row["handicap"],
            "seed": row["seed"],
            "tournament_id": row["tournament_id"],
        }


def fetch_player_by_id(database_url: str, player_id: int) -> dict[str, int] | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT id, name, division, handicap, seed, tournament_id
            FROM players
            WHERE id = ?
            LIMIT 1;
            """,
            (player_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "division": row["division"],
            "handicap": row["handicap"],
            "seed": row["seed"],
            "tournament_id": row["tournament_id"],
        }


def _row_to_match(row: tuple) -> dict:
    return {
        "id": row[0],
        "tournament_id": row[1],
        "match_key": row[2],
        "division": row[3],
        "player_a_id": row[4],
        "player_b_id": row[5],
        "player_c_id": row[6],
        "player_d_id": row[7],
        "course_id": row[8],
        "course_tee_id": row[9],
        "player_a_handicap": row[10],
        "player_b_handicap": row[11],
        "hole_count": row[12] or 18,
        "start_hole": row[13] or 1,
        "status": row[14],
        "finalized": row[15],
        "created_at": row[16],
        "updated_at": row[17],
        "player_a_name": row[18] or "",
        "player_b_name": row[19] or "",
        "player_c_name": row[20] or "",
        "player_d_name": row[21] or "",
        "course_name": row[22] or "",
        "tee_name": row[23] or "",
        "tee_yards": row[24] or "",
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
    start_hole: int = 1,
    status: str = "not_started",
) -> int | None:
    with _write_conn(database_url) as conn:
        conn.execute(
            _prepare(
                """
                INSERT INTO matches (
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
                    start_hole,
                    status
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT (tournament_id, match_key) DO UPDATE SET
                    division = excluded.division,
                    player_a_id = excluded.player_a_id,
                    player_b_id = excluded.player_b_id,
                    player_c_id = excluded.player_c_id,
                    player_d_id = excluded.player_d_id,
                    course_id = excluded.course_id,
                    course_tee_id = excluded.course_tee_id,
                    player_a_handicap = excluded.player_a_handicap,
                    player_b_handicap = excluded.player_b_handicap,
                    hole_count = excluded.hole_count,
                    start_hole = excluded.start_hole,
                    status = excluded.status,
                    updated_at = datetime('now')
                ;
                """
            ),
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
                start_hole,
                status,
            ),
        )
        cursor = conn.execute(
            "SELECT id FROM matches WHERE match_key = ?;",
            (match_key,),
        )
        row = cursor.fetchone()
        return row["id"] if row else None


def fetch_match_by_key(database_url: str, match_key: str) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                m.id,
                m.tournament_id,
                m.match_key,
                m.division,
                m.player_a_id,
                m.player_b_id,
                m.player_c_id,
                m.player_d_id,
                m.course_id,
                m.course_tee_id,
                m.player_a_handicap,
                m.player_b_handicap,
                m.hole_count,
                m.start_hole,
                m.status,
                m.finalized,
                m.created_at,
                m.updated_at,
                pa.name,
                pb.name,
                pc.name,
                pd.name,
                c.course_name,
                ct.tee_name,
                ct.total_yards
            FROM matches m
            LEFT JOIN players pa ON pa.id = m.player_a_id
            LEFT JOIN players pb ON pb.id = m.player_b_id
            LEFT JOIN players pc ON pc.id = m.player_c_id
            LEFT JOIN players pd ON pd.id = m.player_d_id
            LEFT JOIN courses c ON c.id = m.course_id
            LEFT JOIN course_tees ct ON ct.id = m.course_tee_id
            WHERE m.match_key = ?
            LIMIT 1;
            """,
            (match_key,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return _row_to_match(row)


def fetch_matches_by_tournament(database_url: str, tournament_id: int) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                m.id,
                m.tournament_id,
                m.match_key,
                m.division,
                m.player_a_id,
                m.player_b_id,
                m.player_c_id,
                m.player_d_id,
                m.course_id,
                m.course_tee_id,
                m.player_a_handicap,
                m.player_b_handicap,
                m.hole_count,
                m.start_hole,
                m.status,
                m.finalized,
                m.created_at,
                m.updated_at,
                pa.name,
                pb.name,
                pc.name,
                pd.name,
                c.course_name,
                ct.tee_name,
                ct.total_yards
            FROM matches m
            LEFT JOIN players pa ON pa.id = m.player_a_id
            LEFT JOIN players pb ON pb.id = m.player_b_id
            LEFT JOIN players pc ON pc.id = m.player_c_id
            LEFT JOIN players pd ON pd.id = m.player_d_id
            LEFT JOIN courses c ON c.id = m.course_id
            LEFT JOIN course_tees ct ON ct.id = m.course_tee_id
            WHERE m.tournament_id = ?
            ORDER BY m.match_key;
            """,
            (tournament_id,),
        )
        rows = cursor.fetchall()
    return [_row_to_match(row) for row in rows]


def set_match_finalized(database_url: str, match_key: str, finalized: bool) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            UPDATE matches
            SET finalized = ?
            WHERE match_key = ?;
            """,
            (int(finalized), match_key),
        )


def delete_match(database_url: str, match_id: int) -> None:
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM matches WHERE id = ?;", (match_id,))


def delete_match_results_by_key(database_url: str, match_key: str) -> None:
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM match_results WHERE match_key = ?;", (match_key,))


def fetch_course_holes(database_url: str) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT hole_number, par, handicap
            FROM course_holes
            ORDER BY hole_number;
            """
        )
        return [
            {
                "hole_number": row["hole_number"],
                "par": row["par"],
                "handicap": row["handicap"],
            }
            for row in cursor.fetchall()
        ]


def replace_course_holes(database_url: str, holes: list[dict]) -> None:
    if not holes:
        return
    values = [
        (entry["hole_number"], entry["par"], entry["handicap"])
        for entry in holes
    ]
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM course_holes;")
        conn.executemany(
            """
            INSERT INTO course_holes (hole_number, par, handicap)
            VALUES (?, ?, ?)
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
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            INSERT INTO courses (id, club_name, course_name, city, state, country, latitude, longitude, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
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
                _json_value(raw),
            ),
        )


def upsert_course_tee(
    database_url: str,
    course_id: int,
    gender: str,
    tee: dict,
) -> int:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            INSERT INTO course_tees (
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (course_id, gender, tee_name) DO UPDATE SET
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
            ;
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
        cursor = conn.execute(
            """
            SELECT id
            FROM course_tees
            WHERE course_id = ? AND gender = ? AND tee_name = ?
            LIMIT 1;
            """,
            (
                course_id,
                gender,
                tee.get("tee_name"),
            ),
        )
        row = cursor.fetchone()
        return row["id"] if row else 0


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
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM course_tee_holes WHERE course_tee_id = ?;", (course_tee_id,))
        conn.executemany(
            """
            INSERT INTO course_tee_holes (course_tee_id, hole_number, par, handicap, yardage)
            VALUES (?, ?, ?, ?, ?)
            """,
            values,
        )


def next_course_id(database_url: str) -> int:
    with _connect(database_url) as conn:
        cursor = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM courses;")
        row = cursor.fetchone()
        return int(row["next_id"]) if row else 1


def fetch_course_catalog(database_url: str) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                c.id AS course_id,
                c.club_name,
                c.course_name,
                c.city,
                c.state,
                c.country,
                ct.id AS tee_id,
                ct.gender,
                ct.tee_name,
                ct.course_rating,
                ct.slope_rating,
                ct.par_total,
                ct.total_yards
            FROM courses c
            LEFT JOIN course_tees ct ON c.id = ct.course_id
            ORDER BY c.course_name, ct.gender, ct.tee_name;
            """
        )
        catalog: dict[int, dict] = {}
        rows = cursor.fetchall()
        for row in rows:
            course_id = row["course_id"]
            course_entry = catalog.setdefault(
                course_id,
                {
                    "id": course_id,
                    "club_name": row["club_name"],
                    "course_name": row["course_name"],
                    "city": row["city"],
                    "state": row["state"],
                    "country": row["country"],
                    "tees": [],
                },
            )
            tee_id = row["tee_id"]
            if tee_id:
                course_entry["tees"].append(
                    {
                        "id": tee_id,
                        "gender": row["gender"],
                        "tee_name": row["tee_name"],
                        "course_rating": row["course_rating"],
                        "slope_rating": row["slope_rating"],
                        "par_total": row["par_total"],
                        "total_yards": row["total_yards"],
                    }
                )
        return list(catalog.values())


def fetch_course_tee_holes(database_url: str, course_tee_id: int) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT hole_number, par, handicap, yardage
            FROM course_tee_holes
            WHERE course_tee_id = ?
            ORDER BY hole_number;
            """,
            (course_tee_id,),
        )
        return [
            {
                "hole_number": row["hole_number"],
                "par": row["par"],
                "handicap": row["handicap"],
                "yardage": row["yardage"],
            }
            for row in cursor.fetchall()
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
    with _write_conn(database_url) as conn:
        if player_id:
            conn.execute(
                """
                UPDATE players
                SET name = ?,
                    division = ?,
                    handicap = ?,
                    seed = ?,
                    tournament_id = ?
                WHERE id = ?;
                """,
                (name, division, handicap, seed, tournament_id, player_id),
            )
            return player_id
        conn.execute(
            """
            INSERT INTO players (name, division, handicap, seed, tournament_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                division = excluded.division,
                handicap = excluded.handicap,
                seed = excluded.seed,
                tournament_id = excluded.tournament_id;
            """,
            (name, division, handicap, seed, tournament_id),
        )
        cursor = conn.execute(
            "SELECT id FROM players WHERE name = ? LIMIT 1;",
            (name,),
        )
        row = cursor.fetchone()
        return row["id"] if row else 0


def delete_players_not_in(database_url: str, names: list[str]) -> None:
    with _write_conn(database_url) as conn:
        if not names:
            conn.execute("DELETE FROM players;")
            return
        placeholders = ", ".join("?" for _ in names)
        query = f"DELETE FROM players WHERE name NOT IN ({placeholders});"
        conn.execute(query, names)


def delete_player(database_url: str, player_id: int) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            DELETE FROM match_results
            WHERE player_a_id = ?
               OR player_b_id = ?
               OR player_c_id = ?
               OR player_d_id = ?;
            """,
            (player_id, player_id, player_id, player_id),
        )
        conn.execute(
            """
            DELETE FROM matches
            WHERE player_a_id = ?
               OR player_b_id = ?
               OR player_c_id = ?
               OR player_d_id = ?;
            """,
            (player_id, player_id, player_id, player_id),
        )
        conn.execute("DELETE FROM players WHERE id = ?;", (player_id,))


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
            entry.get("player_c_score", 0),
            entry.get("player_d_score", 0),
        )
        for entry in hole_entries
    ]
    if not values:
        return
    hole_numbers = [entry["hole_number"] for entry in hole_entries]
    if not hole_numbers:
        hole_numbers = []
    placeholders = ", ".join("?" for _ in hole_numbers) if hole_numbers else None
    with _write_conn(database_url) as conn:
        if placeholders:
            conn.execute(
                f"DELETE FROM hole_scores WHERE match_result_id = ? AND hole_number IN ({placeholders});",
                (match_id, *hole_numbers),
            )
        conn.executemany(
            """
            INSERT INTO hole_scores (match_result_id, hole_number, player_a_score, player_b_score, player_c_score, player_d_score)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            values,
        )


def insert_player_hole_scores(
    database_url: str,
    match_id: int,
    match_key: str,
    entries: list[dict],
) -> None:
    if not entries:
        return
    values = [
        (
            match_id,
            match_key,
            entry["player_index"],
            entry["player_side"],
            entry["team_index"],
            entry.get("player_name"),
            entry.get("opponent_name"),
            entry["hole_number"],
            entry["gross_score"],
            entry.get("stroke_adjustment", 0),
            entry.get("course_id"),
            entry.get("course_tee_id"),
            entry.get("player_handicap"),
            entry.get("net_score"),
        )
        for entry in entries
    ]
    hole_numbers = sorted({entry["hole_number"] for entry in entries})
    with _write_conn(database_url) as conn:
        if hole_numbers:
            placeholders = ", ".join("?" for _ in hole_numbers)
            conn.execute(
                f"DELETE FROM player_hole_scores WHERE match_result_id = ? AND hole_number IN ({placeholders});",
                (match_id, *hole_numbers),
            )
        conn.executemany(
            """
            INSERT INTO player_hole_scores (
                match_result_id,
                match_key,
                player_index,
                player_side,
                team_index,
                player_name,
                opponent_name,
                hole_number,
                gross_score,
                stroke_adjustment,
                course_id,
                course_tee_id,
                player_handicap,
                net_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (match_result_id, player_index, hole_number) DO UPDATE SET
                player_name = excluded.player_name,
                opponent_name = excluded.opponent_name,
                gross_score = excluded.gross_score,
                stroke_adjustment = excluded.stroke_adjustment,
                course_id = excluded.course_id,
                course_tee_id = excluded.course_tee_id,
                player_handicap = excluded.player_handicap,
                net_score = excluded.net_score;
            """,
            values,
        )
        if hole_numbers:
            placeholders = ", ".join("?" for _ in hole_numbers)
            cursor = conn.execute(
                f"""
                SELECT hole_number, player_index, net_score
                FROM player_hole_scores
                WHERE match_result_id = ? AND hole_number IN ({placeholders});
                """,
                (match_id, *hole_numbers),
            )
            net_rows = cursor.fetchall()
            net_map: dict[int, dict[int, float | None]] = {}
            for row in net_rows:
                hole_number = row["hole_number"]
                player_index = row["player_index"]
                net_score = row["net_score"]
                hole_map = net_map.setdefault(hole_number, {})
                hole_map[player_index] = net_score
            for hole_number in hole_numbers:
                nets = net_map.get(hole_number) or {}
                conn.execute(
                    """
                    UPDATE hole_scores
                    SET
                        player_a_net = ?,
                        player_b_net = ?,
                        player_c_net = ?,
                        player_d_net = ?
                    WHERE match_result_id = ?
                      AND hole_number = ?;
                    """,
                    (
                        nets.get(0),
                        nets.get(1),
                        nets.get(2),
                        nets.get(3),
                        match_id,
                        hole_number,
                    ),
                )


def delete_player_hole_scores(database_url: str, match_id: int) -> None:
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM player_hole_scores WHERE match_result_id = ?;", (match_id,))


def fetch_player_hole_scores(database_url: str, match_id: int) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT player_index, player_side, team_index, player_name, opponent_name, hole_number, gross_score, stroke_adjustment, net_score
            FROM player_hole_scores
            WHERE match_result_id = ?
            ORDER BY hole_number, player_index;
            """,
            (match_id,),
        )
        return [
            {
                "player_index": row["player_index"],
                "player_side": row["player_side"],
                "team_index": row["team_index"],
                "player_name": row["player_name"],
                "opponent_name": row["opponent_name"],
                "hole_number": row["hole_number"],
                "gross_score": row["gross_score"],
                "stroke_adjustment": row["stroke_adjustment"],
                "net_score": row["net_score"],
            }
            for row in cursor.fetchall()
        ]


def delete_hole_scores(database_url: str, match_id: int) -> None:
    with _write_conn(database_url) as conn:
        conn.execute("DELETE FROM hole_scores WHERE match_result_id = ?;", (match_id,))
    delete_player_hole_scores(database_url, match_id)


def fetch_legacy_hole_scores(database_url: str, match_id: int) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT hole_number, player_a_score, player_b_score, player_c_score, player_d_score,
                   player_a_net, player_b_net, player_c_net, player_d_net
            FROM hole_scores
            WHERE match_result_id = ?
            ORDER BY hole_number;
            """,
            (match_id,),
        )
        return [
            {
                "hole_number": row["hole_number"],
                "player_a_score": row["player_a_score"],
                "player_b_score": row["player_b_score"],
                "player_c_score": row["player_c_score"],
                "player_d_score": row["player_d_score"],
                "player_a_net": row["player_a_net"],
                "player_b_net": row["player_b_net"],
                "player_c_net": row["player_c_net"],
                "player_d_net": row["player_d_net"],
            }
            for row in cursor.fetchall()
        ]


def fetch_hole_scores(database_url: str, match_id: int) -> list[dict]:
    player_rows = fetch_player_hole_scores(database_url, match_id)
    if player_rows:
        hole_map: dict[int, dict[str, float | None]] = {}
        score_keys = ["player_a_score", "player_b_score", "player_c_score", "player_d_score"]
        net_keys = ["player_a_net", "player_b_net", "player_c_net", "player_d_net"]
        for row in player_rows:
            hole_number = row["hole_number"]
            entry = hole_map.setdefault(
                hole_number,
                {key: None for key in score_keys + net_keys},
            )
            index = row["player_index"]
            if 0 <= index < len(score_keys):
                entry[score_keys[index]] = row["gross_score"]
                entry[net_keys[index]] = row.get("net_score")
        return [
            {"hole_number": number, **hole_map[number]}
            for number in sorted(hole_map)
        ]
    return fetch_legacy_hole_scores(database_url, match_id)


def fetch_match_result(database_url: str, match_id: int) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                id,
                match_name,
                match_key,
                match_code,
                player_a_id,
                player_b_id,
                player_a_name,
                player_b_name,
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
                hole_count,
                start_hole,
                finalized,
                course_snapshot,
                scorecard_snapshot,
                submitted_at
            FROM match_results
            WHERE id = ?;
            """,
            (match_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "match_name": row["match_name"],
            "match_key": row["match_key"],
            "match_code": row["match_code"],
            "player_a_id": row["player_a_id"],
            "player_b_id": row["player_b_id"],
            "player_a_name": row["player_a_name"],
            "player_b_name": row["player_b_name"],
            "player_a_points": row["player_a_points"],
            "player_b_points": row["player_b_points"],
            "player_a_bonus": row["player_a_bonus"],
            "player_b_bonus": row["player_b_bonus"],
            "player_a_total": row["player_a_total"],
            "player_b_total": row["player_b_total"],
            "winner": row["winner"],
            "course_id": row["course_id"],
            "course_tee_id": row["course_tee_id"],
            "tournament_id": row["tournament_id"],
            "player_a_handicap": row["player_a_handicap"],
            "player_b_handicap": row["player_b_handicap"],
            "hole_count": row["hole_count"],
            "start_hole": row["start_hole"],
            "finalized": row["finalized"],
            "course_snapshot": row["course_snapshot"],
            "scorecard_snapshot": row["scorecard_snapshot"],
            "submitted_at": _parse_datetime(row["submitted_at"]),
        }


def fetch_match_result_by_key_and_players(
    database_url: str,
    match_key: str,
    player_a: str,
    player_b: str,
) -> dict | None:
    if not match_key:
        return None
    normalized_a = (player_a or "").strip()
    normalized_b = (player_b or "").strip()
    if not normalized_a or not normalized_b:
        return None
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                id,
                match_name,
                match_key,
                match_code,
                player_a_id,
                player_b_id,
                player_a_name,
                player_b_name,
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
                hole_count,
                start_hole,
                finalized,
                course_snapshot,
                scorecard_snapshot,
                submitted_at
            FROM match_results
            WHERE match_key = ?
              AND player_a_name = ?
              AND player_b_name = ?
            ORDER BY id DESC
            LIMIT 1;
            """,
            (match_key, normalized_a, normalized_b),
        )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "match_name": row["match_name"],
        "match_key": row["match_key"],
        "match_code": row["match_code"],
        "player_a_id": row["player_a_id"],
        "player_b_id": row["player_b_id"],
        "player_a_name": row["player_a_name"],
        "player_b_name": row["player_b_name"],
        "player_a_points": row["player_a_points"],
        "player_b_points": row["player_b_points"],
        "player_a_bonus": row["player_a_bonus"],
        "player_b_bonus": row["player_b_bonus"],
        "player_a_total": row["player_a_total"],
        "player_b_total": row["player_b_total"],
        "winner": row["winner"],
        "course_id": row["course_id"],
        "course_tee_id": row["course_tee_id"],
        "tournament_id": row["tournament_id"],
        "player_a_handicap": row["player_a_handicap"],
        "player_b_handicap": row["player_b_handicap"],
        "hole_count": row["hole_count"],
        "start_hole": row["start_hole"],
        "finalized": row["finalized"],
        "course_snapshot": row["course_snapshot"],
        "scorecard_snapshot": row["scorecard_snapshot"],
            "submitted_at": _parse_datetime(row["submitted_at"]),
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
    player_a_id: int | None = None,
    player_b_id: int | None = None,
    player_c_id: int | None = None,
    player_d_id: int | None = None,
    tournament_id: int | None = None,
    hole_count: int = 18,
    start_hole: int = 1,
) -> Optional[int]:
    def _next_code(conn_cursor) -> str:
        while True:
            code = "".join(str(random.randint(0, 9)) for _ in range(9))
            conn_cursor.execute(
                "SELECT 1 FROM match_results WHERE match_code = ? LIMIT 1;",
                (code,),
            )
            if not conn_cursor.fetchone():
                return code

    with _write_conn(database_url) as conn:
        cursor = conn.cursor()
        code = match_code or _next_code(cursor)
        cursor.execute(
            """
            INSERT INTO match_results (
                match_name,
                player_a_id,
                player_b_id,
                player_c_id,
                player_d_id,
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
                hole_count,
                start_hole
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                match_name,
                player_a_id,
                player_b_id,
                player_c_id,
                player_d_id,
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
                hole_count,
                start_hole,
            ),
        )
        return cursor.lastrowid


def finalize_match_result(
    database_url: str,
    match_id: int,
    *,
    course_snapshot: dict | None = None,
    scorecard_snapshot: dict | None = None,
) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            UPDATE match_results
            SET
                finalized = 1,
                course_snapshot = ?,
                scorecard_snapshot = ?
            WHERE id = ?;
            """,
            (
                _json_value(course_snapshot),
                _json_value(scorecard_snapshot),
                match_id,
            ),
        )


def fetch_match_bonus(database_url: str, match_result_id: int) -> dict[str, float] | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT player_a_bonus, player_b_bonus
            FROM match_bonus
            WHERE match_result_id = ?
            LIMIT 1;
            """,
            (match_result_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {"player_a_bonus": row["player_a_bonus"] or 0.0, "player_b_bonus": row["player_b_bonus"] or 0.0}


def upsert_match_bonus(
    database_url: str,
    match_result_id: int,
    *,
    player_a_bonus: float,
    player_b_bonus: float,
) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            INSERT INTO match_bonus (match_result_id, player_a_bonus, player_b_bonus)
            VALUES (?, ?, ?)
            ON CONFLICT (match_result_id) DO UPDATE SET
                player_a_bonus = excluded.player_a_bonus,
                player_b_bonus = excluded.player_b_bonus,
                updated_at = datetime('now');
            """,
            (match_result_id, player_a_bonus, player_b_bonus),
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
    placeholders = ", ".join("?" for _ in match_ids)
    with _write_conn(database_url) as conn:
        conn.execute(
            f"DELETE FROM hole_scores WHERE match_result_id IN ({placeholders});",
            (*match_ids,),
        )
        conn.execute(
            f"""
            UPDATE match_results
            SET
                player_a_points = ?,
                player_b_points = ?,
                player_a_bonus = ?,
                player_b_bonus = ?,
                player_a_total = ?,
                player_b_total = ?,
                winner = ?,
                finalized = 0,
                scorecard_snapshot = NULL,
                course_snapshot = NULL
            WHERE id IN ({placeholders});
            """,
            (
                player_a_points,
                player_b_points,
                player_a_bonus,
                player_b_bonus,
                player_a_total,
                player_b_total,
                winner,
                *match_ids,
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
        updates.append("course_id = ?")
        values.append(course_id)
    if course_tee_id is not None:
        updates.append("course_tee_id = ?")
        values.append(course_tee_id)
    if player_a_handicap is not None:
        updates.append("player_a_handicap = ?")
        values.append(player_a_handicap)
    if player_b_handicap is not None:
        updates.append("player_b_handicap = ?")
        values.append(player_b_handicap)
    if not updates:
        return
    values.append(match_key)
    query = f"""
        update match_results
        set {', '.join(updates)}
        where match_key = ?
        """
    with _write_conn(database_url) as conn:
        conn.execute(query, tuple(values))


def fetch_match_result_by_key(database_url: str, match_key: str) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                id,
                match_name,
                match_key,
                match_code,
                player_a_id,
                player_b_id,
                player_a_name,
                player_b_name,
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
            FROM match_results
            WHERE match_key = ?
            ORDER BY submitted_at DESC
            LIMIT 1;
            """,
            (match_key,),
        )
        row = cursor.fetchone()
        if not row:
            return None
    return {
        "id": row["id"],
        "match_name": row["match_name"],
        "match_key": row["match_key"],
        "match_code": row["match_code"],
        "player_a_id": row["player_a_id"],
        "player_b_id": row["player_b_id"],
        "player_a_name": row["player_a_name"],
        "player_b_name": row["player_b_name"],
        "player_a_points": row["player_a_points"],
        "player_b_points": row["player_b_points"],
        "player_a_bonus": row["player_a_bonus"],
        "player_b_bonus": row["player_b_bonus"],
        "player_a_total": row["player_a_total"],
        "player_b_total": row["player_b_total"],
        "winner": row["winner"],
        "course_id": row["course_id"],
        "course_tee_id": row["course_tee_id"],
        "tournament_id": row["tournament_id"],
        "player_a_handicap": row["player_a_handicap"],
        "player_b_handicap": row["player_b_handicap"],
        "finalized": row["finalized"],
        "course_snapshot": row["course_snapshot"],
        "scorecard_snapshot": row["scorecard_snapshot"],
        "submitted_at": _parse_datetime(row["submitted_at"]),
    }


def fetch_match_by_id(database_url: str, match_id: int) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                m.id,
                m.tournament_id,
                m.match_key,
                m.division,
                m.player_a_id,
                m.player_b_id,
                m.player_c_id,
                m.player_d_id,
                m.course_id,
                m.course_tee_id,
                m.player_a_handicap,
                m.player_b_handicap,
                m.hole_count,
                m.start_hole,
                m.status,
                m.finalized,
                m.created_at,
                m.updated_at,
                pa.name,
                pb.name,
                pc.name,
                pd.name,
                c.course_name,
                ct.tee_name,
                ct.total_yards
            FROM matches m
            LEFT JOIN players pa ON pa.id = m.player_a_id
            LEFT JOIN players pb ON pb.id = m.player_b_id
            LEFT JOIN players pc ON pc.id = m.player_c_id
            LEFT JOIN players pd ON pd.id = m.player_d_id
            LEFT JOIN courses c ON c.id = m.course_id
            LEFT JOIN course_tees ct ON ct.id = m.course_tee_id
            WHERE m.id = ?
            LIMIT 1;
            """,
            (match_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return _row_to_match(row)


def fetch_match_result_ids_by_key(database_url: str, match_key: str) -> list[int]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT id
            FROM match_results
            WHERE match_key = ?
            """,
            (match_key,),
        )
        return [row["id"] for row in cursor.fetchall()]


def fetch_match_result_by_code(database_url: str, match_code: str) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                id,
                match_name,
                match_key,
                match_code,
                player_a_id,
                player_b_id,
                player_a_name,
                player_b_name,
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
            FROM match_results
            WHERE match_code = ?
            ORDER BY submitted_at DESC
            LIMIT 1;
            """,
            (match_code,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "match_name": row["match_name"],
            "match_key": row["match_key"],
            "match_code": row["match_code"],
            "player_a_id": row["player_a_id"],
            "player_b_id": row["player_b_id"],
            "player_a_name": row["player_a_name"],
            "player_b_name": row["player_b_name"],
            "player_a_points": row["player_a_points"],
            "player_b_points": row["player_b_points"],
            "player_a_bonus": row["player_a_bonus"],
            "player_b_bonus": row["player_b_bonus"],
            "player_a_total": row["player_a_total"],
            "player_b_total": row["player_b_total"],
            "winner": row["winner"],
            "course_id": row["course_id"],
            "course_tee_id": row["course_tee_id"],
            "tournament_id": row["tournament_id"],
            "player_a_handicap": row["player_a_handicap"],
            "player_b_handicap": row["player_b_handicap"],
            "finalized": row["finalized"],
            "course_snapshot": row["course_snapshot"],
            "scorecard_snapshot": row["scorecard_snapshot"],
            "submitted_at": _parse_datetime(row["submitted_at"]),
        }


def insert_tournament(
    database_url: str,
    name: str,
    description: str | None = None,
    status: str = "upcoming",
    settings: dict | None = None,
) -> int | None:
    with _write_conn(database_url) as conn:
        cursor = conn.execute(
            """
            INSERT INTO tournaments (name, description, status, settings)
            VALUES (?, ?, ?, ?)
            """,
            (
                name,
                description,
                status,
                _json_value(settings or {}),
            ),
        )
        return cursor.lastrowid or None


def fetch_tournament_by_id(database_url: str, tournament_id: int) -> dict | None:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT id, name, description, status, settings, created_at, updated_at
            FROM tournaments
            WHERE id = ?
            LIMIT 1;
            """,
            (tournament_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "status": row["status"],
            "settings": row["settings"] or {},
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


def fetch_tournaments(database_url: str) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
                id,
                name,
                COALESCE(description, '') AS description,
                status,
                settings,
                created_at,
                updated_at
            FROM tournaments
            ORDER BY created_at DESC, id DESC;
            """
        )
        rows = cursor.fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "status": row["status"],
            "settings": row["settings"] or {},
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def update_tournament_status(database_url: str, tournament_id: int, status: str) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            UPDATE tournaments
            SET status = ?, updated_at = datetime('now')
            WHERE id = ?;
            """,
            (status, tournament_id),
        )


def fetch_event_settings(database_url: str, tournament_id: int) -> dict[str, str]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT key, value
            FROM tournament_event_settings
            WHERE tournament_id = ?;
            """,
            (tournament_id,),
        )
        rows = cursor.fetchall()
    return {row["key"]: row["value"] for row in rows}


def upsert_event_setting(database_url: str, tournament_id: int, key: str, value: str) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            INSERT INTO tournament_event_settings (tournament_id, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT (tournament_id, key) DO UPDATE SET
                value = excluded.value;
            """,
            (tournament_id, key, value),
        )


def upsert_setting(database_url: str, key: str, value: str) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            INSERT INTO tournament_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value;
            """,
            (key, value),
        )


def fetch_settings(database_url: str) -> dict[str, str]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT key, value
            FROM tournament_settings
            """
        )
        rows = cursor.fetchall()
    return {row["key"]: row["value"] for row in rows}


def delete_match_results_by_tournament(database_url: str, tournament_id: int) -> int:
    with _write_conn(database_url) as conn:
        before = conn.total_changes
        conn.execute(
            """
            DELETE FROM match_results
            WHERE tournament_id = ?;
            """,
            (tournament_id,),
        )
        deleted = conn.total_changes - before
    return deleted


def replace_standings_cache(
    database_url: str,
    tournament_id: int,
    entries: list[dict],
) -> None:
    with _write_conn(database_url) as conn:
        conn.execute(
            """
            DELETE FROM standings_cache
            WHERE tournament_id = ?;
            """,
            (tournament_id,),
        )
        if not entries:
            return
        conn.executemany(
            """
            INSERT INTO standings_cache (
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
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
                )
                for entry in entries
            ],
        )


def fetch_standings_cache(database_url: str, tournament_id: int) -> list[dict]:
    with _connect(database_url) as conn:
        cursor = conn.execute(
            """
            SELECT
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
            FROM standings_cache
            WHERE tournament_id = ?
            ORDER BY division, points_for DESC, wins DESC, ties DESC, player_name;
            """,
            (tournament_id,),
        )
        rows = cursor.fetchall()
    return [
        {
            "player_name": row["player_name"],
            "division": row["division"],
            "seed": row["seed"],
            "matches": row["matches"],
            "wins": row["wins"],
            "ties": row["ties"],
            "losses": row["losses"],
            "points_for": row["points_for"],
            "points_against": row["points_against"],
            "point_diff": row["point_diff"],
            "holes_played": row["holes_played"],
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
    player_a_id: int | None = None,
    player_b_id: int | None = None,
    player_c_id: int | None = None,
    player_d_id: int | None = None,
) -> None:
    updates = [
        ("player_a_points", player_a_points),
        ("player_b_points", player_b_points),
        ("player_a_bonus", player_a_bonus),
        ("player_b_bonus", player_b_bonus),
        ("player_a_total", player_a_total),
        ("player_b_total", player_b_total),
        ("winner", winner),
    ]
    if player_a_id is not None:
        updates.append(("player_a_id", player_a_id))
    if player_b_id is not None:
        updates.append(("player_b_id", player_b_id))
    if player_c_id is not None:
        updates.append(("player_c_id", player_c_id))
    if player_d_id is not None:
        updates.append(("player_d_id", player_d_id))

    set_clause = ",\n                    ".join(f"{key} = ?" for key, _ in updates)
    values = [value for _, value in updates]
    values.append(match_id)
    with _write_conn(database_url) as conn:
        conn.execute(
            f"""
            UPDATE match_results
            SET
                {set_clause}
            WHERE id = ?;
            """,
            tuple(values),
        )
