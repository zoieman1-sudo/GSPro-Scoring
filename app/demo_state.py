from __future__ import annotations

DEFAULT_DIVISION_COUNT: dict[str, int] = {"A": 5, "B": 5}
DEMO_TOURNAMENT_NAME = "Demo"
ACTIVE_TOURNAMENT_ID_KEY = "active_tournament_id"


def default_player_roster(division_counts: dict[str, int] | None = None) -> dict[str, str]:
    """Return a name → division mapping for every slot in the default roster."""
    counts = division_counts or DEFAULT_DIVISION_COUNT
    roster: dict[str, str] = {}
    for division, size in counts.items():
        for idx in range(1, size + 1):
            roster[f"Player {division}{idx}"] = division
    return roster
