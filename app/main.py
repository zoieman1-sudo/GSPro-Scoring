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
    fetch_match_result_by_key,
    fetch_player_by_name,
    fetch_players,
    fetch_recent_results,
    insert_hole_scores,
    insert_match_result,
    delete_players_not_in,
    upsert_player,
    fetch_settings,
    upsert_setting,
)
from app.seed import (
    Match,
    build_pairings_from_players,
    find_match,
    match_display,
)
from app.settings import load_settings, score_outcome

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
settings = load_settings()
DEFAULT_TOURNAMENT_SETTINGS = {
    "a_handicap_index": "8.4",
    "b_handicap_index": "14.2",
    "match_allowance": "1",
    "a_tee_slope": "132",
    "b_tee_slope": "125",
    "a_course_rating": "71.6",
    "b_course_rating": "70.2",
    "a_par": "72",
    "b_par": "72",
}
DEFAULT_DIVISION_COUNT = {"A": 5, "B": 5}


def _build_player_divisions() -> dict[str, str]:
    players = fetch_players(settings.database_url)
    if not players:
        players = _seed_default_players()
    return {player["name"]: player["division"] for player in players}


def _default_player_roster() -> dict[str, str]:
    roster: dict[str, str] = {}
    for division, size in DEFAULT_DIVISION_COUNT.items():
        for idx in range(1, size + 1):
            roster[f"Player {division}{idx}"] = division
    return roster


def _seed_default_players() -> list[dict]:
    roster = _default_player_roster()
    for division in sorted(DEFAULT_DIVISION_COUNT):
        players_in_division = [name for name, div in roster.items() if div == division]
        for idx, name in enumerate(players_in_division, 1):
            upsert_player(
                settings.database_url,
                None,
                name,
                division,
                0,
                idx,
            )
    return fetch_players(settings.database_url)


def _load_pairings() -> list[Match]:
    players = fetch_players(settings.database_url)
    if not players:
        players = _seed_default_players()
    if not players:
        return []
    return build_pairings_from_players(players)


def _load_tournament_settings() -> dict[str, str]:
    stored = fetch_settings(settings.database_url)
    result = DEFAULT_TOURNAMENT_SETTINGS.copy()
    result.update(stored)
    return result


def _resolve_match(match_id: str, matches: list[Match], match_name: str, player_a: str, player_b: str):
    resolved = find_match(match_id, matches)
    if resolved:
        match_name = match_display(resolved)
        player_a = resolved.player_a
        player_b = resolved.player_b
    return resolved, match_name, player_a, player_b



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


def _build_match_summary(match: Match | None, key: str | None = None) -> dict:
    match_key = key or (match.match_id if match else "")
    match_result = (
        fetch_match_result_by_key(settings.database_url, match_key) if match_key else None
    )
    summary = {
        "match_name": match_display(match) if match else "Match",
        "player_a": match.player_a if match else "Player A",
        "player_b": match.player_b if match else "Player B",
        "holes": [],
        "hole_total_a": 0,
        "hole_total_b": 0,
        "player_a_handicap": 0,
        "player_b_handicap": 0,
        "division": match.division if match else "Open",
        "match_key": match_key,
        "hole_diff": 0,
    }
    if match_result:
        summary.update(
            {
                "match_name": match_result["match_name"],
                "player_a": match_result["player_a_name"],
                "player_b": match_result["player_b_name"],
            }
        )
    player_a_info = fetch_player_by_name(settings.database_url, summary["player_a"]) or {}
    player_b_info = fetch_player_by_name(settings.database_url, summary["player_b"]) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    holes = (
        fetch_hole_scores(settings.database_url, match_result["id"])
        if match_result
        else []
    )
    enriched, total_net_a, total_net_b = _enrich_holes_with_net(holes, handicap_a, handicap_b)
    summary.update(
        {
            "holes": enriched,
            "hole_total_a": total_net_a,
            "hole_total_b": total_net_b,
            "player_a_handicap": handicap_a,
            "player_b_handicap": handicap_b,
            "hole_diff": total_net_a - total_net_b,
        }
    )
    return summary


def _render_scoring(request: Request) -> HTMLResponse:
    matches = _load_pairings()
    default_match = matches[0] if matches else None
    summary = _build_match_summary(default_match, default_match.match_id if default_match else None)
    tournament_settings = _load_tournament_settings()
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
            "tournament_settings": tournament_settings,
            "scorecard": summary,
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return _render_scoring(request)


@app.get("/scoring", response_class=HTMLResponse)
async def scoring_page(request: Request):
    return _render_scoring(request)


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
):

    matches = _load_pairings()
    resolved_match, match_name, player_a, player_b = _resolve_match(
        match_id, matches, match_name, player_a, player_b
    )

    outcome = score_outcome(player_a_points, player_b_points)
    insert_match_result(
        settings.database_url,
        match_name=match_name,
        player_a=player_a,
        player_b=player_b,
        match_key=match_id,
        player_a_points=player_a_points,
        player_b_points=player_b_points,
        **outcome,
    )

    return templates.TemplateResponse(
        "scoring.html",
        {
            "request": request,
            "match_name": match_name,
            "match_id": resolved_match.match_id if resolved_match else match_id,
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


@app.post("/api/scores")
async def api_scores(request: Request):
    try:
        payload = ScorePayload.model_validate(await request.json())
    except ValidationError as exc:
        return JSONResponse({"error": "Invalid payload", "details": exc.errors()}, status_code=422)

    matches = _load_pairings()
    resolved_match, match_name, player_a, player_b = _resolve_match(
        payload.match_id or "",
        matches,
        payload.match_name,
        payload.player_a,
        payload.player_b,
    )

    outcome = score_outcome(payload.player_a_points, payload.player_b_points)
    record_id = insert_match_result(
        settings.database_url,
        match_name=match_name,
        player_a=player_a,
        player_b=player_b,
        match_key=payload.match_id or "",
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


@app.get("/api/match-summary/{match_key}")
async def api_match_summary(match_key: str):
    matches = _load_pairings()
    match = next((item for item in matches if item.match_id == match_key), None)
    return _build_match_summary(match, match_key)


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
        "pairings": [],
    }
    if not authorized:
        context["setup_status"] = context["setup_status"] or "Invalid or missing PIN."
        return context

    players = fetch_players(settings.database_url)
    if not players:
        players = _seed_default_players()
    context["players"] = players
    context["pairings"] = build_pairings_from_players(players)
    context["tournament_settings"] = _load_tournament_settings()
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


@app.post("/admin/setup/settings", response_class=HTMLResponse)
async def admin_setup_settings(
    request: Request,
    pin: str = Form(""),
    a_handicap_index: str = Form(DEFAULT_TOURNAMENT_SETTINGS["a_handicap_index"]),
    b_handicap_index: str = Form(DEFAULT_TOURNAMENT_SETTINGS["b_handicap_index"]),
    match_allowance: str = Form(DEFAULT_TOURNAMENT_SETTINGS["match_allowance"]),
    a_tee_slope: str = Form(DEFAULT_TOURNAMENT_SETTINGS["a_tee_slope"]),
    b_tee_slope: str = Form(DEFAULT_TOURNAMENT_SETTINGS["b_tee_slope"]),
    a_course_rating: str = Form(DEFAULT_TOURNAMENT_SETTINGS["a_course_rating"]),
    b_course_rating: str = Form(DEFAULT_TOURNAMENT_SETTINGS["b_course_rating"]),
    a_par: str = Form(DEFAULT_TOURNAMENT_SETTINGS["a_par"]),
    b_par: str = Form(DEFAULT_TOURNAMENT_SETTINGS["b_par"]),
):
    authorized = pin == settings.scoring_pin
    if not authorized:
        return templates.TemplateResponse(
            "setup.html",
            _setup_context(request, pin, False),
        )
    for key, value in {
        "a_handicap_index": a_handicap_index,
        "b_handicap_index": b_handicap_index,
        "match_allowance": match_allowance,
        "a_tee_slope": a_tee_slope,
        "b_tee_slope": b_tee_slope,
        "a_course_rating": a_course_rating,
        "b_course_rating": b_course_rating,
        "a_par": a_par,
        "b_par": b_par,
    }.items():
        upsert_setting(settings.database_url, key, value)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status="Tournament settings saved.",
        ),
    )


@app.get("/matches", response_class=HTMLResponse)
async def matches_list(request: Request):
    results = fetch_recent_results(settings.database_url, limit=20)
    return templates.TemplateResponse(
        "matches.html",
        {"request": request, "matches": results},
    )


def _net_adjustment(handicap: int, hole_number: int) -> int:
    if hole_number <= 0:
        hole_number = 1
    base = handicap // 18
    remainder = handicap % 18
    extra = 1 if remainder and hole_number <= remainder else 0
    return base + extra


def _enrich_holes_with_net(
    holes: list[dict],
    handicap_a: int,
    handicap_b: int,
) -> tuple[list[dict], int, int]:
    enriched: list[dict] = []
    total_net_a = 0
    total_net_b = 0
    for hole in holes:
        hole_number = hole.get("hole_number", 1) or 1
        net_a = hole["player_a_score"] - _net_adjustment(handicap_a, hole_number)
        net_b = hole["player_b_score"] - _net_adjustment(handicap_b, hole_number)
        enriched.append(
            {
                **hole,
                "player_a_net": net_a,
                "player_b_net": net_b,
                "net_diff": net_a - net_b,
            }
        )
        total_net_a += net_a
        total_net_b += net_b
    return enriched, total_net_a, total_net_b


@app.get("/matches/{match_id}", response_class=HTMLResponse)
async def match_detail(request: Request, match_id: int):
    result = fetch_match_result(settings.database_url, match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    holes = fetch_hole_scores(settings.database_url, match_id)
    player_a = fetch_player_by_name(settings.database_url, result["player_a_name"])
    player_b = fetch_player_by_name(settings.database_url, result["player_b_name"])
    handicap_a = player_a["handicap"] if player_a else 0
    handicap_b = player_b["handicap"] if player_b else 0
    enriched_holes, total_net_a, total_net_b = _enrich_holes_with_net(
        holes,
        handicap_a,
        handicap_b,
    )
    return templates.TemplateResponse(
        "match_detail.html",
        {
            "request": request,
            "match": result,
            "holes": enriched_holes,
            "hole_total_a": total_net_a,
            "hole_total_b": total_net_b,
            "player_a_handicap": handicap_a,
            "player_b_handicap": handicap_b,
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
