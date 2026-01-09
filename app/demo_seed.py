from __future__ import annotations

from typing import Iterable

from app.db import (
    ensure_schema,
    fetch_match_by_key,
    fetch_player_by_name,
    fetch_settings,
    fetch_tournaments,
    insert_match,
    insert_tournament,
    upsert_event_setting,
    upsert_match_group_definition,
    upsert_player,
    upsert_setting,
)
from app.demo_state import (
    ACTIVE_TOURNAMENT_ID_KEY,
    DEMO_MATCH_DEFINITIONS,
    DEMO_MATCH_GROUP_KEY,
    DEMO_MATCH_GROUP_LABEL,
    DEMO_TOURNAMENT_NAME,
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
            if (entry.get("name") or "").strip().lower() == DEMO_TOURNAMENT_NAME.lower()
        ),
        None,
    )
    tournament_id = demo["id"] if demo else None
    if not tournament_id:
        tournament_id = insert_tournament(
            database_url,
            name=DEMO_TOURNAMENT_NAME,
            description="Demo event with seeded players and matches.",
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


def ensure_demo_matches(database_url: str, tournament_id: int) -> list[str]:
    match_keys: list[str] = []
    for entry in DEMO_MATCH_DEFINITIONS:
        match_key = str(entry["match_key"])
        if fetch_match_by_key(database_url, match_key):
            match_keys.append(match_key)
            continue
        players = entry["players"]
        player_a = fetch_player_by_name(database_url, players[0])
        player_b = fetch_player_by_name(database_url, players[1])
        if not player_a or not player_b:
            continue
        created = insert_match(
            database_url,
            tournament_id=tournament_id,
            match_key=match_key,
            division=str(entry["division"]),
            player_a_id=player_a["id"],
            player_b_id=player_b["id"],
            active=False,
        )
        if created:
            match_keys.append(match_key)
    return match_keys


def ensure_demo_group(database_url: str, tournament_id: int, match_keys: Iterable[str]) -> None:
    sanitized_keys = [key for key in match_keys if key]
    if not sanitized_keys:
        return
    player_pairs = []
    key_set = set(sanitized_keys)
    for entry in DEMO_MATCH_DEFINITIONS:
        match_key = str(entry["match_key"])
        if match_key not in key_set:
            continue
        players = entry["players"]
        player_pairs.append(f"{players[0]} vs {players[1]}")
    upsert_match_group_definition(
        database_url,
        {
            "group_key": DEMO_MATCH_GROUP_KEY,
            "label": DEMO_MATCH_GROUP_LABEL,
            "match_keys": sanitized_keys,
            "player_pairs": player_pairs,
            "course": {},
            "tournament_id": tournament_id,
        },
    )


def ensure_demo_fixture(database_url: str) -> None:
    tournament_id = ensure_demo_tournament(database_url)
    if not tournament_id:
        return
    ensure_demo_players(database_url, tournament_id)
    match_keys = ensure_demo_matches(database_url, tournament_id)
    ensure_demo_group(database_url, tournament_id, match_keys)


def main() -> None:
    settings = load_settings()
    ensure_schema(settings.database_url)
    ensure_demo_fixture(settings.database_url)


if __name__ == "__main__":
    main()
