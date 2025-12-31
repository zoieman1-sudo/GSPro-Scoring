from fastapi.testclient import TestClient

import app.main as main


def test_score_outcome_win():
    outcome = main.score_outcome(6, 4)
    assert outcome["player_a_bonus"] == 1
    assert outcome["player_b_bonus"] == 0
    assert outcome["winner"] == "A"
    assert outcome["player_a_total"] == 7
    assert outcome["player_b_total"] == 4


def test_score_outcome_tie():
    outcome = main.score_outcome(5, 5)
    assert outcome["player_a_bonus"] == 0
    assert outcome["player_b_bonus"] == 0
    assert outcome["winner"] == "T"


def test_api_pin_validation(monkeypatch):
    main.settings = main.load_settings()
    main.settings = main.settings.__class__(
        database_url=main.settings.database_url,
        scoring_pin="9999",
    )

    monkeypatch.setattr(main, "ensure_schema", lambda *_: None)
    monkeypatch.setattr(main, "insert_match_result", lambda *_args, **_kwargs: 1)

    client = TestClient(main.app)
    payload = {
        "match_id": "",
        "match_name": "Division A: Player A1 vs Player A2",
        "player_a": "Player A1",
        "player_b": "Player A2",
        "player_a_points": 5,
        "player_b_points": 4,
        "pin": "0000",
    }
    response = client.post("/api/scores", json=payload)
    assert response.status_code == 403
