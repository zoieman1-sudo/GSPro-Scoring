from __future__ import annotations

from app.db import (
    ensure_schema,
    fetch_settings,
    fetch_tournaments,
    insert_tournament,
    upsert_event_setting,
    upsert_player,
    upsert_setting,
)
from app.demo_state import (
    ACTIVE_TOURNAMENT_ID_KEY,
    DEFAULT_DIVISION_COUNT,
    default_player_roster,
)
from app.settings import load_settings


def _ensure_active_tournament(database_url: str, tournament_id: int) -> None:
    settings = fetch_settings(database_url)
    active_value = (settings.get(ACTIVE_TOURNAMENT_ID_KEY) or "").strip()
    if active_value:
        return
    upsert_setting(database_url, ACTIVE_TOURNAMENT_ID_KEY, str(tournament_id))


def ensure_demo_tournament(database_url: str) -> int | None:
    tournaments = fetch_tournaments(database_url)
    demo = next(
        (
            entry
            for entry in tournaments
            if (entry.get("name") or "").strip().lower() == "demo".lower()
        ),
        None,
    )
    tournament_id = demo["id"] if demo else None
    if not tournament_id:
        tournament_id = insert_tournament(
            database_url,
            name="Demo",
            description="Demo event with seeded players.",
            status="active",
        )
    if not tournament_id:
        return None
    total_players = sum(DEFAULT_DIVISION_COUNT.values())
    total_divisions = len(DEFAULT_DIVISION_COUNT)
    upsert_event_setting(database_url, tournament_id, "player_count", str(total_players))
    upsert_event_setting(database_url, tournament_id, "division_count", str(total_divisions))
    _ensure_active_tournament(database_url, tournament_id)
    return tournament_id


def ensure_demo_players(database_url: str, tournament_id: int) -> None:
    roster = default_player_roster(DEFAULT_DIVISION_COUNT)
    for division in sorted(DEFAULT_DIVISION_COUNT):
        players_in_division = [name for name, div in roster.items() if div == division]
        for seed, name in enumerate(players_in_division, start=1):
            upsert_player(
                database_url,
                None,
                name,
                division,
                0,
                seed,
                tournament_id=tournament_id,
            )


def ensure_demo_fixture(database_url: str) -> None:
    tournament_id = ensure_demo_tournament(database_url)
    if not tournament_id:
        return
    ensure_demo_players(database_url, tournament_id)


def main() -> None:
    settings = load_settings()
    ensure_schema(settings.database_url)
    ensure_demo_fixture(settings.database_url)


if __name__ == "__main__":
    main()
