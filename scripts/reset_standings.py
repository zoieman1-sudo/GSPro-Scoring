#!/usr/bin/env python3
"""Utility to clear match results for a tournament and recompute standings."""

from __future__ import annotations

import argparse
import sys

from app.db import fetch_tournaments
from app.main import (
    _get_active_tournament_id,
    _reset_tournament_match_history,
    settings,
)


def _resolve_tournament_id(name: str | None, explicit_id: int | None) -> int | None:
    if explicit_id:
        return explicit_id
    if name:
        tournaments = fetch_tournaments(settings.database_url)
        normalized = name.strip().lower()
        for entry in tournaments:
            if (entry.get("name") or "").strip().lower() == normalized:
                return entry.get("id")
        return None
    return _get_active_tournament_id()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete stored matches for a tournament and rebuild the standings cache."
    )
    parser.add_argument("--tournament-id", type=int, help="Target tournament ID.")
    parser.add_argument("--tournament-name", type=str, help="Lookup tournament by name.")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available tournaments from the database.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Require confirmation before deleting matches.",
    )
    args = parser.parse_args()

    if args.list:
        tournaments = fetch_tournaments(settings.database_url)
        if not tournaments:
            print("No tournaments found.")
            return
        print("Tournaments:")
        for entry in tournaments:
            print(f"  {entry['id']}: {entry['name']}")
        return

    if not args.confirm:
        parser.error("This command deletes match history. Re-run with --confirm to proceed.")

    tournament_id = _resolve_tournament_id(args.tournament_name, args.tournament_id)
    if not tournament_id:
        parser.error("Could not resolve a tournament ID. Provide --tournament-id or set an active tournament.")

    deleted = _reset_tournament_match_history(tournament_id)
    print(
        f"Cleared {deleted} match{'es' if deleted != 1 else ''} "
        f"and rebuilt standings for tournament {tournament_id}."
    )


if __name__ == "__main__":
    main()
