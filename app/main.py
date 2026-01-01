from collections import defaultdict
from itertools import zip_longest

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError

from app.db import (
    ensure_schema,
    fetch_all_match_results,
    fetch_hole_scores,
    fetch_match_result,
    fetch_players,
    fetch_recent_results,
    insert_hole_scores,
    insert_match_result,
    delete_players_not_in,
    upsert_player,
)
from app.seed import get_match_by_id, get_matches, match_display
from app.settings import load_settings, score_outcome

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
settings = load_settings()
matches = get_matches()


def _build_player_divisions() -> dict[str, str]:
    divisions: dict[str, str] = {}
    for match in matches:
        divisions.setdefault(match.player_a, match.division)
        divisions.setdefault(match.player_b, match.division)
    return divisions


def _default_player_roster() -> dict[str, str]:
    roster: dict[str, str] = {}
    for match in matches:
        roster.setdefault(match.player_a, match.division)
        roster.setdefault(match.player_b, match.division)
    return roster


def _seed_default_players() -> list[dict]:
    division_roster: dict[str, list[str]] = defaultdict(list)
    for match in matches:
        for player in (match.player_a, match.player_b):
            if player not in division_roster[match.division]:
                division_roster[match.division].append(player)

    for division, players_list in division_roster.items():
        for idx, name in enumerate(players_list, 1):
            upsert_player(
                settings.database_url,
                None,
                name,
                division,
                0,
                idx,
            )
    return fetch_players(settings.database_url)


def _empty_stat(name: str, division: str) -> dict:
    return {
        "name": name,
        "division": division,
        "matches": 0,
        "wins": 0,
        "ties": 0,
        "losses": 0,
        "points_for": 0,
        "points_against": 0,
        "point_diff": 0,
    }


def _record_player(
    stats: dict[str, dict],
    player: str,
    division: str,
    player_points: int,
    opponent_points: int,
    winner: str,
    role: str,
) -> None:
    entry = stats.setdefault(player, _empty_stat(player, division))
    entry["matches"] += 1
    entry["points_for"] += player_points
    entry["points_against"] += opponent_points
    if winner == "T":
        entry["ties"] += 1
    elif winner == role:
        entry["wins"] += 1
    else:
        entry["losses"] += 1


def build_standings(results: list[dict]) -> list[dict]:
    divisions_by_player = _build_player_divisions()
    standings: dict[str, dict] = {
        name: _empty_stat(name, division)
        for name, division in divisions_by_player.items()
    }

    for result in results:
        _record_player(
            standings,
            result["player_a_name"],
            divisions_by_player.get(result["player_a_name"], "Open"),
            result["player_a_total"],
            result["player_b_total"],
            result["winner"],
            "A",
        )
        _record_player(
            standings,
            result["player_b_name"],
            divisions_by_player.get(result["player_b_name"], "Open"),
            result["player_b_total"],
            result["player_a_total"],
            result["winner"],
            "B",
        )

    for entry in standings.values():
        entry["point_diff"] = entry["points_for"] - entry["points_against"]

    division_groups: dict[str, list[dict]] = defaultdict(list)
    for entry in standings.values():
        division_groups[entry["division"]].append(entry)

    sorted_divisions = []
    for division in sorted(division_groups):
        players = sorted(
            division_groups[division],
            key=lambda item: (
                -item["points_for"],
                -item["wins"],
                -item["ties"],
                item["name"],
            ),
        )
        sorted_divisions.append({"division": division, "players": players})
    return sorted_divisions


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    default_match = matches[0] if matches else None
    return templates.TemplateResponse(
        "scoring.html",
        {
            "request": request,
            "match_name": match_display(default_match) if default_match else "Match",
            "match_id": default_match.match_id if default_match else "",
            "player_a": default_match.player_a if default_match else "Player A",
            "player_b": default_match.player_b if default_match else "Player B",
            "status": None,
            "matches": matches,
        },
    )


@app.on_event("startup")
def startup() -> None:
    ensure_schema(settings.database_url)
    _seed_default_players()


@app.post("/submit", response_class=HTMLResponse)
async def submit(
    request: Request,
    match_id: str = Form(""),
    match_name: str = Form(""),
    player_a: str = Form(""),
    player_b: str = Form(""),
    player_a_points: int = Form(...),
    player_b_points: int = Form(...),
    pin: str = Form(...),
):
    if pin != settings.scoring_pin:
        return templates.TemplateResponse(
            "scoring.html",
            {
                "request": request,
                "match_name": match_name or "Match",
                "match_id": match_id,
                "player_a": player_a or "Player A",
                "player_b": player_b or "Player B",
                "status": "Invalid PIN. Please try again.",
                "matches": matches,
            },
        )

    resolved = get_match_by_id(match_id)
    if resolved:
        match_name = match_display(resolved)
        player_a = resolved.player_a
        player_b = resolved.player_b

    outcome = score_outcome(player_a_points, player_b_points)
    insert_match_result(
        settings.database_url,
        match_name=match_name,
        player_a=player_a,
        player_b=player_b,
        player_a_points=player_a_points,
        player_b_points=player_b_points,
        **outcome,
    )

    return templates.TemplateResponse(
        "scoring.html",
        {
            "request": request,
            "match_name": match_name,
            "match_id": match_id,
            "player_a": player_a,
            "player_b": player_b,
            "status": "Match submitted. Totals saved.",
            "matches": matches,
        },
    )


class ScorePayload(BaseModel):
    match_id: str | None = None
    match_name: str
    player_a: str
    player_b: str
    player_a_points: int
    player_b_points: int
    pin: str


@app.post("/api/scores")
async def api_scores(request: Request):
    try:
        payload = ScorePayload.model_validate(await request.json())
    except ValidationError as exc:
        return JSONResponse({"error": "Invalid payload", "details": exc.errors()}, status_code=422)

    if payload.pin != settings.scoring_pin:
        return JSONResponse({"error": "Invalid PIN"}, status_code=403)

    resolved = get_match_by_id(payload.match_id or "")
    match_name = payload.match_name
    player_a = payload.player_a
    player_b = payload.player_b
    if resolved:
        match_name = match_display(resolved)
        player_a = resolved.player_a
        player_b = resolved.player_b

    outcome = score_outcome(payload.player_a_points, payload.player_b_points)
    record_id = insert_match_result(
        settings.database_url,
        match_name=match_name,
        player_a=player_a,
        player_b=player_b,
        player_a_points=payload.player_a_points,
        player_b_points=payload.player_b_points,
        **outcome,
    )
    return {
        "id": record_id,
        "match_name": match_name,
        "player_a_total": outcome["player_a_total"],
        "player_b_total": outcome["player_b_total"],
        "winner": outcome["winner"],
    }


def _admin_context(
    request: Request,
    pin: str,
    authorized: bool,
    status_message: str | None = None,
) -> dict:
    context = {
        "request": request,
        "authorized": authorized,
        "pin": pin,
        "results": [],
        "status": status_message,
    }
    if not authorized:
        context["status"] = context["status"] or "Invalid or missing PIN."
        return context

    context["results"] = fetch_recent_results(settings.database_url, limit=20)
    return context


def _setup_context(
    request: Request,
    pin: str,
    authorized: bool,
    setup_status: str | None = None,
) -> dict:
    context = {
        "request": request,
        "authorized": authorized,
        "pin": pin,
        "players": [],
        "setup_status": setup_status,
    }
    if not authorized:
        context["setup_status"] = context["setup_status"] or "Invalid or missing PIN."
        return context

    players = fetch_players(settings.database_url)
    if not players:
        players = _seed_default_players()
    context["players"] = players
    return context


@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request, pin: str = ""):
    authorized = pin == settings.scoring_pin
    return templates.TemplateResponse(
        "admin.html",
        _admin_context(request, pin, authorized),
    )


@app.get("/admin/setup", response_class=HTMLResponse)
async def admin_setup_page(request: Request, pin: str = ""):
    authorized = pin == settings.scoring_pin
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(request, pin, authorized),
    )


@app.post("/admin/setup", response_class=HTMLResponse)
async def admin_setup(
    request: Request,
    pin: str = Form(""),
    player_id: list[str] | None = Form(None),
    player_name: list[str] | None = Form(None),
    player_division: list[str] | None = Form(None),
    player_handicap: list[str] | None = Form(None),
    player_seed: list[str] | None = Form(None),
):
    authorized = pin == settings.scoring_pin
    if not authorized:
        return templates.TemplateResponse(
            "admin.html",
            _admin_context(request, pin, False),
        )

    player_id = player_id or []
    player_name = player_name or []
    player_division = player_division or []
    player_handicap = player_handicap or []

    processed = 0
    player_seed = player_seed or []
    processed_names: list[str] = []
    for name, division, handicap, seed, pid in zip_longest(
        player_name,
        player_division,
        player_handicap,
        player_seed,
        player_id,
        fillvalue="",
    ):
        cleaned_name = name.strip()
        if not cleaned_name:
            continue
        cleaned_division = division.strip() or "Open"
        try:
            parsed_handicap = int(handicap)
        except ValueError:
            parsed_handicap = 0
        parsed_id = int(pid) if pid.isdigit() else None
        try:
            parsed_seed = int(seed)
        except ValueError:
            parsed_seed = 0
        upsert_player(
            settings.database_url,
            parsed_id,
            cleaned_name,
            cleaned_division,
            parsed_handicap,
            parsed_seed,
        )
        processed_names.append(cleaned_name)
        processed += 1

    delete_players_not_in(settings.database_url, processed_names)

    setup_status = (
        f"Saved {processed} player{'s' if processed != 1 else ''}."
        if processed
        else "No players were updated."
    )
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(request, pin, True, setup_status=setup_status),
    )


@app.get("/matches", response_class=HTMLResponse)
async def matches_list(request: Request):
    results = fetch_recent_results(settings.database_url, limit=20)
    return templates.TemplateResponse(
        "matches.html",
        {"request": request, "matches": results},
    )


@app.get("/matches/{match_id}", response_class=HTMLResponse)
async def match_detail(request: Request, match_id: int):
    result = fetch_match_result(settings.database_url, match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    holes = fetch_hole_scores(settings.database_url, match_id)
    total_a = sum(h["player_a_score"] for h in holes)
    total_b = sum(h["player_b_score"] for h in holes)
    return templates.TemplateResponse(
        "match_detail.html",
        {
            "request": request,
            "match": result,
            "holes": holes,
            "hole_total_a": total_a,
            "hole_total_b": total_b,
        },
    )


@app.post("/matches/{match_id}/holes")
async def match_detail_submit(match_id: int, request: Request):
    payload = await request.json()
    entries = payload.get("holes", [])
    cleaned = []
    for entry in entries:
        try:
            hole_number = int(entry.get("hole_number", 0))
            player_a_score = int(entry.get("player_a_score", 0))
            player_b_score = int(entry.get("player_b_score", 0))
        except (TypeError, ValueError):
            continue
        if hole_number <= 0:
            continue
        cleaned.append(
            {
                "hole_number": hole_number,
                "player_a_score": player_a_score,
                "player_b_score": player_b_score,
            }
        )
    insert_hole_scores(settings.database_url, match_id, cleaned)
    return JSONResponse({"added": len(cleaned)})


@app.get("/standings", response_class=HTMLResponse)
async def standings(request: Request):
    results = fetch_all_match_results(settings.database_url)
    divisions = build_standings(results)
    return templates.TemplateResponse(
        "standings.html",
        {"request": request, "divisions": divisions},
    )
