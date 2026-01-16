import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.demo_state import ACTIVE_TOURNAMENT_ID_KEY
from app.db import (
    fetch_hole_scores,
    fetch_match_bonus,
    fetch_match_result_by_key,
    fetch_matches_by_tournament,
    fetch_players,
    fetch_settings,
    fetch_tournament_by_id,
)
from app.settings import load_settings


def _safe_int(value: str | int | None) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def load_database_url() -> str:
    return load_settings().database_url


def active_tournament_id(db_url: str, override: int | None = None) -> int | None:
    if override:
        return override
    settings = fetch_settings(db_url)
    return _safe_int(settings.get(ACTIVE_TOURNAMENT_ID_KEY))


def export_snapshot(tournament_id: int) -> dict:
    db_url = load_database_url()
    tournament = fetch_tournament_by_id(db_url, tournament_id) or {"id": tournament_id}
    players = [player for player in fetch_players(db_url) if player.get("tournament_id") == tournament_id]
    matches = fetch_matches_by_tournament(db_url, tournament_id)

    match_results = []
    for match in matches:
        match_key = match.get("match_key")
        if not match_key:
            continue
        result = fetch_match_result_by_key(db_url, match_key)
        if result:
            holes = fetch_hole_scores(db_url, result["id"])
            bonus = fetch_match_bonus(db_url, result["id"])
            match_results.append(
                {
                    "match_key": match_key,
                    "match_id": match.get("id"),
                    "result": result,
                    "holes": holes,
                    "bonus": bonus or {"player_a_bonus": 0.0, "player_b_bonus": 0.0},
                }
            )

    return {
        "tournament": tournament,
        "players": players,
        "matches": matches,
        "match_results": match_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump current tournament state for reseeding.")
    parser.add_argument(
        "--tournament-id",
        "-t",
        type=int,
        help="Tournament ID to snapshot (defaults to active tournament).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Path to write the JSON export (defaults to stdout).",
    )
    args = parser.parse_args()

    db_url = load_database_url()
    tournament_id = active_tournament_id(db_url, args.tournament_id)
    if not tournament_id:
        raise SystemExit("Unable to determine tournament ID (please set active_tournament_id in settings or pass --tournament-id).")

    snapshot = export_snapshot(tournament_id)
    payload = json.dumps(snapshot, default=str, indent=2)

    if args.output:
        args.output.write_text(payload, encoding="utf-8")
        print(f"Snapshot saved to {args.output}")
    else:
        print(payload)


if __name__ == "__main__":
    main()
