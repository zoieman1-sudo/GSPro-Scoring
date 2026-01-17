import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Settings:
    database_url: str
    scoring_pin: str
    golf_api_key: str


def _normalize_database_url(value: Optional[str]) -> str:
    if not value:
        return "sqlite:///app/DATA/gspro_scoring.db"
    normalized = value.strip()
    if normalized.startswith("sqlite://"):
        return normalized
    if Path(normalized).suffix:  # treat as direct path
        return f"sqlite:///{normalized}"
    return normalized


def load_settings() -> Settings:
    database_url = _normalize_database_url(os.getenv("DATABASE_URL"))
    scoring_pin = os.getenv("SCORING_PIN", "1234")
    golf_api_key = os.getenv("GOLF_API_KEY", "IGEEUMDFTUIYPPODWAO5XZSQNI")
    return Settings(
        database_url=database_url,
        scoring_pin=scoring_pin,
        golf_api_key=golf_api_key,
    )


def bonus_for_points(points: float, opponent_points: float) -> float:
    if points > opponent_points and points >= 5:
        return 1.0
    if points == opponent_points and points >= 4.5:
        return 0.5
    return 0.0


def compute_bonus_points(
    player_a_points: float,
    player_b_points: float,
    *,
    allow_bonus: bool = True,
) -> tuple[float, float]:
    if not allow_bonus:
        return 0.0, 0.0
    return (
        bonus_for_points(player_a_points, player_b_points),
        bonus_for_points(player_b_points, player_a_points),
    )


def score_outcome(
    player_a_points: float,
    player_b_points: float,
    *,
    allow_bonus: bool = True,
) -> dict:
    if player_a_points > player_b_points:
        winner = "A"
    elif player_a_points < player_b_points:
        winner = "B"
    else:
        winner = "T"

    player_a_bonus, player_b_bonus = compute_bonus_points(
        player_a_points, player_b_points, allow_bonus=allow_bonus
    )
    player_a_total = player_a_points + player_a_bonus
    player_b_total = player_b_points + player_b_bonus
    return {
        "player_a_points": player_a_points,
        "player_b_points": player_b_points,
        "player_a_bonus": player_a_bonus,
        "player_b_bonus": player_b_bonus,
        "player_a_total": player_a_total,
        "player_b_total": player_b_total,
        "winner": winner,
    }
