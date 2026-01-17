"""Lightweight local store for the kiosk leaderboard so it can run without Postgres."""

from __future__ import annotations

import itertools
import sqlite3
from collections import defaultdict
from pathlib import Path

from app.demo_state import DEFAULT_DIVISION_COUNT, default_player_roster

KIOSK_DB_PATH = Path(__file__).resolve().parent / "DATA" / "kiosk_leaderboard.db"
MATCH_STATUS_LABELS = {
    "not_started": "Not started",
    "in_progress": "In progress",
    "completed": "Completed",
}


def _connect() -> sqlite3.Connection:
    """Return a sqlite3 connection pointing at the kiosk leaderboard file."""
    KIOSK_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(KIOSK_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the sandboxed tables if they do not already exist."""
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                division TEXT NOT NULL,
                points_for REAL NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                ties INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                match_key TEXT PRIMARY KEY,
                division TEXT NOT NULL,
                player_a TEXT NOT NULL,
                player_b TEXT NOT NULL,
                status TEXT NOT NULL,
                status_label TEXT NOT NULL,
                display TEXT NOT NULL,
                finalized INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def _seed_players(conn: sqlite3.Connection) -> None:
    """Populate the players table with the default roster and canned stats."""
    roster = default_player_roster(DEFAULT_DIVISION_COUNT)
    for index, name in enumerate(sorted(roster), start=1):
        division = roster[name]
        wins = (index % 4) + 1
        losses = index % 3
        ties = index % 2
        points_for = round(12 + wins * 1.5 - losses * 0.5 + ties * 0.75, 1)
        conn.execute(
            """
            INSERT OR REPLACE INTO players (name, division, points_for, wins, losses, ties)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (name, division, points_for, wins, losses, ties),
        )


def _seed_matches(conn: sqlite3.Connection) -> None:
    """Create a handful of matches so the leaderboard has active/finalized data."""
    roster = default_player_roster(DEFAULT_DIVISION_COUNT)
    divisions: dict[str, list[str]] = defaultdict(list)
    for name, division in roster.items():
        divisions[division].append(name)

    statuses = ["in_progress", "completed", "not_started"]
    for division in sorted(divisions):
        players = sorted(divisions[division])
        for index, (player_a, player_b) in enumerate(
            itertools.islice(itertools.combinations(players, 2), 4), start=1
        ):
            status = statuses[(index - 1) % len(statuses)]
            match_key = f"{division}-{index:02d}"
            status_label = MATCH_STATUS_LABELS.get(status, status.capitalize())
            display = f"Division {division}: {player_a} vs {player_b}"
            conn.execute(
                """
                INSERT OR REPLACE INTO matches
                (match_key, division, player_a, player_b, status, status_label, display, finalized)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (match_key, division, player_a, player_b, status, status_label, display, 1 if status == "completed" else 0),
            )


def _ensure_database() -> None:
    """Ensure schema exists and the default data is seeded."""
    with _connect() as conn:
        _ensure_schema(conn)
        _seed_players(conn)
        _seed_matches(conn)


def fetch_divisions() -> list[dict]:
    """Return cached standings grouped by division."""
    _ensure_database()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT name, division, points_for, wins, losses, ties
            FROM players;
            """
        ).fetchall()
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[row["division"]].append(
            {
                "name": row["name"],
                "points_for": float(row["points_for"]),
                "wins": int(row["wins"]),
                "losses": int(row["losses"]),
                "ties": int(row["ties"]),
            }
        )

    sorted_divisions: list[dict] = []
    for division in sorted(groups):
        players = sorted(
            groups[division],
            key=lambda entry: (
                -entry["points_for"],
                -entry["wins"],
                -entry["ties"],
                entry["name"],
            ),
        )
        sorted_divisions.append({"division": division, "players": players})
    return sorted_divisions


def fetch_matches() -> list[dict]:
    """Return the match list along with status labels for the kiosk view."""
    _ensure_database()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT match_key, division, player_a, player_b, status, status_label, display, finalized
            FROM matches
            ORDER BY updated_at DESC;
            """
        ).fetchall()
    return [
        {
            "match_key": row["match_key"],
            "division": row["division"],
            "player_a": row["player_a"],
            "player_b": row["player_b"],
            "status": row["status"],
            "status_label": row["status_label"],
            "display": row["display"],
            "finalized": bool(row["finalized"]),
        }
        for row in rows
    ]
