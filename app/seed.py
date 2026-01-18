from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations


@dataclass(frozen=True)
class Match:
    match_id: str
    division: str
    player_a: str
    player_b: str
    player_c: str | None = None
    player_d: str | None = None
    course_id: int | None = None
    course_tee_id: int | None = None
    hole_count: int = 18
    start_hole: int = 1


def _division_matches(division: str, players: list[str], start_index: int) -> list[Match]:
    matches: list[Match] = []
    counter = start_index
    for player_a, player_b in combinations(players, 2):
        match_id = f"{division}-{counter:02d}"
        matches.append(Match(match_id, division, player_a, player_b, hole_count=18, start_hole=1))
        counter += 1
    return matches


def build_pairings_from_players(players: list[dict]) -> list[Match]:
    division_roster: dict[str, list[dict]] = defaultdict(list)
    for player in players:
        division_roster[player["division"]].append(player)

    matches: list[Match] = []
    for division in sorted(division_roster):
        roster = sorted(
            division_roster[division],
            key=lambda entry: (entry.get("seed", 0), entry["name"]),
        )
        names = [entry["name"] for entry in roster]
        matches.extend(_division_matches(division, names, 1))
    return matches


def match_display(match: Match) -> str:
    if match.player_c or match.player_d:
        team_a = " & ".join(filter(None, [match.player_a, match.player_c]))
        team_b = " & ".join(filter(None, [match.player_b, match.player_d]))
        return f"Division {match.division}: {team_a} vs {team_b}"
    return f"Division {match.division}: {match.player_a} vs {match.player_b}"


def find_match(match_id: str, matches: list[Match]) -> Match | None:
    if not match_id:
        return None
    for match in matches:
        if match.match_id == match_id:
            return match
    return None
