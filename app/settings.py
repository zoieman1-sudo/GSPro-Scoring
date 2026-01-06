import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    scoring_pin: str
    golf_api_key: str


def load_settings() -> Settings:
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/gspro_scoring")
    scoring_pin = os.getenv("SCORING_PIN", "1234")
    golf_api_key = os.getenv("GOLF_API_KEY", "IGEEUMDFTUIYPPODWAO5XZSQNI")
    return Settings(
        database_url=database_url,
        scoring_pin=scoring_pin,
        golf_api_key=golf_api_key,
    )


def score_outcome(player_a_points: float, player_b_points: float) -> dict:
    player_a_bonus = 0.0
    player_b_bonus = 0.0
    if player_a_points == player_b_points:
        winner = "T"
        if player_a_points == 4.5:
            player_a_bonus = player_b_bonus = 0.5
    elif player_a_points > player_b_points:
        winner = "A"
    else:
        winner = "B"

    if player_a_points >= 5:
        player_a_bonus = 1.0
    if player_b_points >= 5:
        player_b_bonus = 1.0

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
