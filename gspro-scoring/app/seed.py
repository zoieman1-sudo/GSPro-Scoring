from dataclasses import dataclass
from itertools import combinations


@dataclass(frozen=True)
class Match:
    match_id: str
    division: str
    player_a: str
    player_b: str


def _division_matches(division: str, players: list[str], start_index: int) -> list[Match]:
    matches: list[Match] = []
    counter = start_index
    for player_a, player_b in combinations(players, 2):
        match_id = f"{division}-{counter:02d}"
        matches.append(Match(match_id, division, player_a, player_b))
        counter += 1
    return matches


def get_matches() -> list[Match]:
    division_a = [f"Player A{i}" for i in range(1, 6)]
    division_b = [f"Player B{i}" for i in range(1, 6)]
    matches = _division_matches("A", division_a, 1)
    matches.extend(_division_matches("B", division_b, 1))
    return matches


def match_display(match: Match) -> str:
    return f"Division {match.division}: {match.player_a} vs {match.player_b}"


def get_match_by_id(match_id: str) -> Match | None:
    if not match_id:
        return None
    for match in get_matches():
        if match.match_id == match_id:
            return match
    return None
