import re
import zipfile
from collections import defaultdict
from itertools import zip_longest
from pathlib import Path
from xml.etree import ElementTree as ET

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError

from app import golf_api
from app.db import (
    ensure_schema,
    fetch_all_match_results,
    fetch_hole_scores,
    fetch_match_result,
    fetch_match_result_by_key,
    fetch_match_result_by_code,
    fetch_player_by_name,
    fetch_players,
    fetch_recent_results,
    insert_hole_scores,
    insert_match_result,
    delete_players_not_in,
    upsert_player,
    fetch_settings,
    upsert_setting,
    fetch_course_holes,
    replace_course_holes,
    upsert_course,
    upsert_course_tee,
    replace_course_tee_holes,
    fetch_course_catalog,
    fetch_course_tee_holes,
    fetch_match_result_by_code,
)
from app.course_sync import import_course_to_db
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
    "course_id": "",
    "course_tee_id": "",
}
DEFAULT_DIVISION_COUNT = {"A": 5, "B": 5}
COURSE_TEMPLATE_PATH = Path("app/DATA/Net_Match_Play_Scorecard.xlsx")
SETUP_SECTIONS = {
    "parameters": "Tournament Sheet",
    "players": "Players & Handicaps",
    "course": "Course Selection",
    "holes": "Holes",
    "tees": "Tees",
    "pairings": "Pairings",
}

MATCH_STATUS_LABELS = {
    "not_started": "Not started",
    "in_progress": "In progress",
    "completed": "Completed",
}
STATUS_PRIORITY = {
    "in_progress": 0,
    "not_started": 1,
    "completed": 2,
}


def _match_status_info(match_id: str) -> dict[str, int | str | None]:
    match_result = fetch_match_result_by_key(settings.database_url, match_id)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, match_id)
    if not match_result:
        return {"status": "not_started", "holes": 0, "match_result": None}
    holes = fetch_hole_scores(settings.database_url, match_result["id"])
    status = "completed" if len(holes) >= 18 else "in_progress"
    return {"status": status, "holes": len(holes), "match_result": match_result}


def _adjust_display_points(value: float | int) -> float:
    if value >= 5:
        return value + 1
    if value == 4.5:
        return value + 0.5
    return value


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


def _parse_course_workbook(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with zipfile.ZipFile(path) as workbook:
            sheet_xml = workbook.read("xl/worksheets/sheet1.xml")
            shared_xml = workbook.read("xl/sharedStrings.xml")
    except (FileNotFoundError, KeyError, zipfile.BadZipFile):
        return []

    try:
        sheet = ET.fromstring(sheet_xml)
        shared = ET.fromstring(shared_xml)
    except ET.ParseError:
        return []

    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    shared_strings = [
        "".join(text.text or "" for text in item.iter() if text.tag.endswith("t"))
        for item in shared.findall(f".//{ns}si")
    ]

    values: dict[str, str] = {}
    for cell in sheet.findall(f".//{ns}c"):
        ref = cell.get("r")
        if not ref:
            continue
        value_node = cell.find(f"{ns}v")
        if value_node is None:
            continue
        if cell.get("t") == "s":
            try:
                value = shared_strings[int(value_node.text or "0")]
            except (IndexError, ValueError):
                continue
        else:
            value = value_node.text or ""
        values[ref] = value

    holes: list[dict] = []
    for row in range(1, 100):
        hole_raw = values.get(f"A{row}", "").strip()
        par_raw = values.get(f"B{row}", "").strip()
        handicap_raw = values.get(f"C{row}", "").strip()
        if not hole_raw or not par_raw or not handicap_raw:
            continue
        if not re.match(r"^\\d+$", str(hole_raw)):
            continue
        try:
            hole_number = int(float(hole_raw))
            par = int(round(float(par_raw)))
            handicap = int(round(float(handicap_raw)))
        except ValueError:
            continue
        if hole_number <= 0:
            continue
        holes.append(
            {"hole_number": hole_number, "par": par, "handicap": handicap}
        )
    return sorted(holes, key=lambda item: item["hole_number"])


def _load_course_holes() -> list[dict]:
    holes = fetch_course_holes(settings.database_url)
    if holes:
        return holes
    parsed = _parse_course_workbook(COURSE_TEMPLATE_PATH)
    if parsed:
        replace_course_holes(settings.database_url, parsed)
        return parsed
    return [{"hole_number": idx, "par": 4, "handicap": idx} for idx in range(1, 19)]


def _active_course_holes(tournament_settings: dict | None = None) -> list[dict]:
    t_settings = tournament_settings or _load_tournament_settings()
    tee_id_raw = t_settings.get("course_tee_id") if t_settings else None
    try:
        tee_id = int(tee_id_raw) if tee_id_raw not in ("", None) else None
    except ValueError:
        tee_id = None
    if tee_id:
        holes = fetch_course_tee_holes(settings.database_url, tee_id)
        if holes:
            return holes
    return _load_course_holes()


def _seed_default_players() -> list[dict]:
    existing = fetch_players(settings.database_url)
    if existing:
        return existing
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


def _ensure_match_results_for_pairings() -> None:
    """
    Ensure every pairing has a persistent match_result with match_key and match_code.
    """
    matches = _load_pairings()
    for pairing in matches:
        try:
            existing = fetch_match_result_by_key(settings.database_url, pairing.match_id)
            if existing:
                continue
            outcome = score_outcome(0, 0)
            insert_match_result(
                settings.database_url,
                match_name=match_display(pairing),
                player_a=pairing.player_a,
                player_b=pairing.player_b,
                match_key=pairing.match_id,
                match_code=None,
                player_a_points=0,
                player_b_points=0,
                **outcome,
            )
        except Exception as exc:  # noqa: BLE001
            # Don't block startup if seeding fails; log and continue.
            print(f"WARNING: could not seed match_result for {pairing.match_id}: {exc}")


def _resolve_match(match_id: str, matches: list[Match], match_name: str, player_a: str, player_b: str):
    resolved = find_match(match_id, matches)
    if resolved:
        match_name = match_display(resolved)
        player_a = resolved.player_a
        player_b = resolved.player_b
    return resolved, match_name, player_a, player_b


def _resolve_setup_section(section: str | None) -> str:
    if not section:
        return "parameters"
    return section if section in SETUP_SECTIONS else "parameters"



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
        "holes_played": 0,
    }


def _record_player(
    stats: dict[str, dict],
    player: str,
    division: str,
    player_points: int,
    opponent_points: int,
    winner: str,
    role: str,
    holes_played: int = 0,
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
    entry["holes_played"] += holes_played


def build_standings(results: list[dict]) -> list[dict]:
    divisions_by_player = _build_player_divisions()
    standings: dict[str, dict] = {
        name: _empty_stat(name, division)
        for name, division in divisions_by_player.items()
    }

    course_holes = _active_course_holes()
    for result in results:
        player_a_info = fetch_player_by_name(settings.database_url, result["player_a_name"]) or {}
        player_b_info = fetch_player_by_name(settings.database_url, result["player_b_name"]) or {}
        handicap_a = player_a_info.get("handicap", 0)
        handicap_b = player_b_info.get("handicap", 0)
        hole_entries = fetch_hole_scores(settings.database_url, result["id"])
        hole_count = len(hole_entries)
        if hole_entries:
            computed = _build_scorecard_rows(hole_entries, handicap_a, handicap_b, course_holes)
            points_a = computed["meta"]["total_points_a"]
            points_b = computed["meta"]["total_points_b"]
        else:
            points_a = result["player_a_total"]
            points_b = result["player_b_total"]
        _record_player(
            standings,
            result["player_a_name"],
            divisions_by_player.get(result["player_a_name"], "Open"),
            points_a,
            points_b,
            result["winner"],
            "A",
            hole_count,
        )
        _record_player(
            standings,
            result["player_b_name"],
            divisions_by_player.get(result["player_b_name"], "Open"),
            points_b,
            points_a,
            result["winner"],
            "B",
            hole_count,
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
        for player in players:
            holes_played = player.get("holes_played", 0)
            player["pts_remaining"] = 40 - holes_played + player["points_for"]
        sorted_divisions.append({"division": division, "players": players})
    return sorted_divisions


def _build_match_summary(match: Match | None, key: str | None = None) -> dict:
    match_key = key or (match.match_id if match else "")
    match_result = (
        fetch_match_result_by_key(settings.database_url, match_key) if match_key else None
    )
    summary = {
        "match": {
            "match_name": match_display(match) if match else "Match",
            "player_a_name": match.player_a if match else "Player A",
            "player_b_name": match.player_b if match else "Player B",
            "match_key": match_key,
            "match_code": "",
        },
        "holes": [],
        "hole_total_a": 0,
        "hole_total_b": 0,
        "player_a_handicap": 0,
        "player_b_handicap": 0,
        "division": match.division if match else "Open",
        "match_key": match_key,
        "match_code": "",
        "hole_diff": 0,
    }
    status_info = _match_status_info(match.match_id if match else (key or ""))
    if match_result:
        summary["match"].update(
            {
                "match_name": match_result["match_name"],
                "player_a_name": match_result["player_a_name"],
                "player_b_name": match_result["player_b_name"],
                "match_code": match_result.get("match_code", "") if isinstance(match_result, dict) else "",
            }
        )
        summary.update(
            {
                "match_name": summary["match"]["match_name"],
                "player_a": summary["match"]["player_a_name"],
                "player_b": summary["match"]["player_b_name"],
                "match_code": summary["match"]["match_code"],
            }
        )
    player_a_name = summary["match"].get("player_a_name") or summary.get("player_a")
    player_b_name = summary["match"].get("player_b_name") or summary.get("player_b")
    player_a_info = fetch_player_by_name(settings.database_url, player_a_name) or {}
    player_b_info = fetch_player_by_name(settings.database_url, player_b_name) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    holes = (
        fetch_hole_scores(settings.database_url, match_result["id"])
        if match_result
        else []
    )
    course_holes = _active_course_holes()
    computed = _build_scorecard_rows(holes, handicap_a, handicap_b, course_holes)
    hole_total_a = sum((row.get("net_a") or 0) for row in computed["rows"])
    hole_total_b = sum((row.get("net_b") or 0) for row in computed["rows"])
    match_total_holes = 9 if computed["meta"].get("is_nine_hole") else 18
    summary["match"].update({"total_holes": match_total_holes})
    summary.update(
        {
            "holes": computed["rows"],
            "hole_total_a": hole_total_a,
            "hole_total_b": hole_total_b,
            "player_a_handicap": handicap_a,
            "player_b_handicap": handicap_b,
            "hole_diff": hole_total_a - hole_total_b,
            "course_holes": computed["course"],
            "meta": computed["meta"],
            "status": status_info["status"],
            "status_label": MATCH_STATUS_LABELS.get(status_info["status"], status_info["status"]),
            "holes_recorded": status_info["holes"],
        }
    )
    summary["point_chip_a"] = _adjust_display_points(summary["meta"]["total_points_a"])
    summary["point_chip_b"] = _adjust_display_points(summary["meta"]["total_points_b"])
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
            "course_holes": _active_course_holes(tournament_settings),
            "scorecard": summary,
        },
    )


@app.get("/", response_class=RedirectResponse)
async def index():
    return RedirectResponse(url="/standings")


@app.get("/scoring", response_class=HTMLResponse)
async def scoring_page(request: Request):
    return _render_scoring(request)


@app.get("/scoring/mobile", response_class=HTMLResponse)
async def scoring_mobile_page(request: Request):
    matches = _load_pairings()
    default_match = matches[0] if matches else None
    summary = _build_match_summary(default_match, default_match.match_id if default_match else None)
    match_options: list[dict] = []
    for m in matches:
        pa = fetch_player_by_name(settings.database_url, m.player_a) or {}
        pb = fetch_player_by_name(settings.database_url, m.player_b) or {}
        status_info = _match_status_info(m.match_id)
        match_options.append(
            {
                "match_key": m.match_id,
                "display": match_display(m),
                "handicaps": f"{pa.get('handicap', 0)}/{pb.get('handicap', 0)}",
                "division": m.division,
                "status": status_info["status"],
                "status_label": MATCH_STATUS_LABELS.get(status_info["status"], status_info["status"]),
                "holes": status_info["holes"],
            }
        )
    default_matches = [m["match_key"] for m in match_options if m["status"] != "completed"]
    return templates.TemplateResponse(
        "mobile_scoring_v2.html",
        {
            "request": request,
            "scorecard": summary,
            "matches": match_options,
            "default_match_keys": default_matches[:2],
        },
    )


@app.get("/scorecard", response_class=HTMLResponse)
async def scorecard_latest(request: Request, match_key: str | None = None):
    context = _scorecard_context(match_key)
    if not context["matches"]:
        return templates.TemplateResponse(
            "scorecard_empty.html",
            {"request": request},
        )
    return templates.TemplateResponse(
        "scorecard_view.html",
        {
            "request": request,
            "matches": context["matches"],
            "match_statuses": context["match_statuses"],
            "active_matches": context["active_matches"],
            "scorecard": context["scorecard"],
            "active_match_key": context["scorecard"]["match"]["match_key"],
        },
    )


@app.get("/api/courses/search")
@app.get("/api/courses/search/")
async def api_course_search(query: str):
    try:
        results = golf_api.search_courses(query, settings.golf_api_key)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(exc))
    return results


@app.get("/api/courses/catalog")
async def api_course_catalog():
    return {"courses": fetch_course_catalog(settings.database_url)}


@app.get("/api/scorecard/{match_key}")
async def api_scorecard(match_key: str):
    context = _scorecard_context(match_key)
    scorecard = context.get("scorecard")
    if not scorecard:
        raise HTTPException(status_code=404, detail="Scorecard not available")
    return JSONResponse(scorecard)


@app.post("/api/courses/import/{course_id}")
@app.post("/api/courses/import/{course_id}/")
async def api_course_import(course_id: int):
    try:
        summary = import_course_to_db(settings.database_url, course_id, settings.golf_api_key)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(exc))
    return summary


@app.on_event("startup")
def startup() -> None:
    ensure_schema(settings.database_url)
    _seed_default_players()
    _load_course_holes()


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
        match_code=None,
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
        match_code=None,
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
    resolved_key = match_key
    if not match:
        match_result = fetch_match_result_by_key(settings.database_url, match_key) or fetch_match_result_by_code(
            settings.database_url, match_key
        )
        if match_result and match_result.get("match_key"):
            resolved_key = match_result["match_key"]
            match = next((item for item in matches if item.match_id == resolved_key), None)
    return _build_match_summary(match, resolved_key)


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
    active_section: str | None = None,
) -> dict:
    tournament_settings = _load_tournament_settings()
    active = _resolve_setup_section(active_section)
    course_catalog = fetch_course_catalog(settings.database_url)
    selected_course_id = tournament_settings.get("course_id", "")
    selected_course_tee_id = tournament_settings.get("course_tee_id", "")
    selected_course = next((c for c in course_catalog if str(c["id"]) == str(selected_course_id)), None)
    selected_course_tee = None
    if selected_course:
        for tee in selected_course.get("tees") or []:
            if str(tee.get("id")) == str(selected_course_tee_id):
                selected_course_tee = tee
                break

    context = {
        "request": request,
        "authorized": authorized,
        "pin": pin,
        "players": [],
        "setup_status": setup_status,
        "pairings": [],
        "course_catalog": course_catalog,
        "selected_course_id": selected_course_id,
        "selected_course_tee_id": selected_course_tee_id,
        "selected_course": selected_course,
        "selected_course_tee": selected_course_tee,
        "active_section": active,
        "setup_sections": SETUP_SECTIONS,
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
    context["course_holes"] = _active_course_holes(tournament_settings)
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
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(request, pin, True, active_section="parameters"),
    )


@app.get("/admin/setup/{section}", response_class=HTMLResponse)
async def admin_setup_page_section(request: Request, section: str, pin: str = ""):
    active = _resolve_setup_section(section)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(request, pin, True, active_section=active),
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
        _setup_context(
            request,
            pin,
            True,
            setup_status=setup_status,
            active_section="players",
        ),
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
    course_id: str = Form(""),
    course_tee_id: str = Form(""),
):
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
        "course_id": course_id,
        "course_tee_id": course_tee_id,
    }.items():
        upsert_setting(settings.database_url, key, value)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status="Tournament settings saved.",
            active_section="parameters",
        ),
    )


@app.post("/admin/setup/course", response_class=HTMLResponse)
async def admin_setup_course(
    request: Request,
    pin: str = Form(""),
    course_id: str = Form(""),
    course_tee_id: str = Form(""),
):
    for key, value in {
        "course_id": course_id,
        "course_tee_id": course_tee_id,
    }.items():
        upsert_setting(settings.database_url, key, value)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status="Course selection saved.",
            active_section="course",
        ),
    )


@app.get("/matches", response_class=HTMLResponse)
async def matches_list(request: Request):
    results = fetch_recent_results(settings.database_url, limit=20)
    return templates.TemplateResponse(
        "matches.html",
        {"request": request, "matches": results},
    )


@app.get("/match-ids", response_class=HTMLResponse)
async def match_ids(request: Request):
    results = fetch_all_match_results(settings.database_url)
    return templates.TemplateResponse(
        "match_ids.html",
        {"request": request, "results": results},
    )


def _net_adjustment_for_hole(handicap: int, hole_handicap: int | None) -> int:
    """
    Allocate strokes by hole handicap ranking (1 = hardest). Distribute any remainder
    to the lowest handicap holes.
    """
    if handicap <= 0:
        return 0
    base = handicap // 18
    remainder = handicap % 18
    hole_hcp = hole_handicap or 18
    extra = 1 if remainder and hole_hcp <= remainder else 0
    return base + extra


def _round_half(value: float) -> float:
    return round(value * 2) / 2


def _stroke_allocation(total_strokes: float, course_holes: list[dict]) -> dict[int, float]:
    if not course_holes:
        return {}
    hole_count = len(course_holes)
    base = int(total_strokes // hole_count) if total_strokes > 0 else 0
    remainder = round(total_strokes - (base * hole_count), 3)
    allocation = {hole["hole_number"]: float(base) for hole in course_holes}
    sorted_holes = sorted(course_holes, key=lambda hole: hole["handicap"])
    idx = 0
    while remainder > 0 and sorted_holes:
        hole = sorted_holes[idx % hole_count]
        increment = 1.0 if remainder >= 1 else 0.5
        allocation[hole["hole_number"]] += increment
        remainder = round(remainder - increment, 3)
        idx += 1
    return allocation


def _build_scorecard_rows(
    holes: list[dict],
    handicap_a: int,
    handicap_b: int,
    course_holes: list[dict],
) -> dict:
    hole_map = {entry["hole_number"]: entry for entry in holes}
    max_recorded = max((entry.get("hole_number", 0) for entry in holes), default=0)
    is_nine_hole_match = max_recorded and max_recorded <= 9
    active_course = [
        hole for hole in course_holes if not is_nine_hole_match or hole["hole_number"] <= 9
    ]
    active_course = active_course[:9] if is_nine_hole_match else active_course[:18]
    if not active_course:
        active_course = course_holes[:9 if is_nine_hole_match else 18]
    stroke_diff = handicap_a - handicap_b
    total_strokes = abs(stroke_diff)
    if is_nine_hole_match:
        total_strokes = _round_half(total_strokes * 0.5)
    allocation = _stroke_allocation(total_strokes, active_course)
    strokes_for_a = allocation if stroke_diff > 0 else {hole["hole_number"]: 0.0 for hole in active_course}
    strokes_for_b = allocation if stroke_diff < 0 else {hole["hole_number"]: 0.0 for hole in active_course}
    totals = {
        "points_a": 0.0,
        "points_b": 0.0,
        "strokes_a": total_strokes if stroke_diff > 0 else 0.0,
        "strokes_b": total_strokes if stroke_diff < 0 else 0.0,
    }
    rows: list[dict] = []
    for hole in active_course:
        number = hole["hole_number"]
        entry = hole_map.get(number, {})
        gross_a = entry.get("player_a_score")
        gross_b = entry.get("player_b_score")
        strokes_a = strokes_for_a.get(number, 0.0)
        strokes_b = strokes_for_b.get(number, 0.0)
        net_a = (gross_a - strokes_a) if gross_a is not None else None
        net_b = (gross_b - strokes_b) if gross_b is not None else None
        result = "—"
        points_a = 0.0
        points_b = 0.0
        net_diff = None
        if net_a is not None and net_b is not None:
            diff = net_a - net_b
            net_diff = diff
            if diff < 0:
                result = "A"
                points_a = 1.0
            elif diff > 0:
                result = "B"
                points_b = 1.0
            else:
                result = "Halved"
                points_a = points_b = 0.5
        totals["points_a"] += points_a
        totals["points_b"] += points_b
        rows.append(
            {
                "hole_number": number,
                "par": hole["par"],
                "handicap": hole["handicap"],
                "gross_a": gross_a,
                "gross_b": gross_b,
                "strokes_a": strokes_a,
                "strokes_b": strokes_b,
                "net_a": net_a,
                "net_b": net_b,
                "net_diff": net_diff,
                "result": result,
                "points_a": points_a,
                "points_b": points_b,
            }
        )

    meta = {
        "stroke_owner": "A" if stroke_diff > 0 else "B" if stroke_diff < 0 else None,
        "stroke_count": total_strokes,
        "is_nine_hole": is_nine_hole_match,
        "total_points_a": totals["points_a"],
        "total_points_b": totals["points_b"],
    }
    return {"rows": rows, "course": active_course, "meta": meta}


def _enrich_holes_with_net(
    holes: list[dict],
    handicap_a: int,
    handicap_b: int,
    course_holes: list[dict] | None = None,
) -> tuple[list[dict], int, int]:
    enriched: list[dict] = []
    total_net_a = 0
    total_net_b = 0
    hole_hcp_map = {h["hole_number"]: h.get("handicap") for h in (course_holes or [])}
    for hole in holes:
        hole_number = hole.get("hole_number", 1) or 1
        hole_hcp = hole.get("hole_handicap") or hole_hcp_map.get(hole_number)
        net_a = hole["player_a_score"] - _net_adjustment_for_hole(handicap_a, hole_hcp)
        net_b = hole["player_b_score"] - _net_adjustment_for_hole(handicap_b, hole_hcp)
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


def _scorecard_context(match_key: str | None) -> dict:
    matches = _load_pairings()
    if not matches:
        return {"matches": [], "match_statuses": [], "active_matches": [], "scorecard": None}

    selected = next((item for item in matches if item.match_id == match_key), None) or matches[0]
    match_statuses: list[dict] = []
    selected_status: dict[str, int | str | None] | None = None
    for entry in matches:
        status_info = _match_status_info(entry.match_id)
        match_statuses.append(
            {
                "match_key": entry.match_id,
                "division": entry.division,
                "player_a": entry.player_a,
                "player_b": entry.player_b,
                "display": match_display(entry),
                "status": status_info["status"],
                "status_label": MATCH_STATUS_LABELS.get(status_info["status"], status_info["status"]),
                "holes": status_info["holes"],
            }
        )
        if entry.match_id == selected.match_id:
            selected_status = status_info
    if not selected_status:
        selected_status = {"status": "not_started", "holes": 0, "match_result": None}

    tournament_settings = _load_tournament_settings()
    selected_course_id = tournament_settings.get("course_id")
    selected_course_tee_id = tournament_settings.get("course_tee_id")
    course = None
    tee = None
    if selected_course_id:
        catalog = fetch_course_catalog(settings.database_url)
        course = next((c for c in catalog if str(c["id"]) == str(selected_course_id)), None)
        if course and selected_course_tee_id:
            tee = next((t for t in course.get("tees", []) if str(t.get("id")) == str(selected_course_tee_id)), None)
    match_result = fetch_match_result_by_key(settings.database_url, selected.match_id)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, selected.match_id)
    hole_records = (
        fetch_hole_scores(settings.database_url, match_result["id"])
        if match_result
        else []
    )
    player_a_name = match_result["player_a_name"] if match_result else selected.player_a
    player_b_name = match_result["player_b_name"] if match_result else selected.player_b
    player_a_info = fetch_player_by_name(settings.database_url, player_a_name) or {}
    player_b_info = fetch_player_by_name(settings.database_url, player_b_name) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    course_holes = _active_course_holes(tournament_settings)
    computed = _build_scorecard_rows(hole_records, handicap_a, handicap_b, course_holes)
    match_name = match_display(selected)
    total_holes = 9 if computed["meta"].get("is_nine_hole") else 18
    scorecard = {
        "match": {
            "id": match_result["id"] if match_result else None,
            "match_key": selected.match_id,
            "match_code": match_result.get("match_code") if match_result else "",
            "match_name": match_name,
            "player_a_name": player_a_name,
            "player_b_name": player_b_name,
            "division": selected.division,
            "status": selected_status["status"],
            "status_label": MATCH_STATUS_LABELS.get(selected_status["status"], selected_status["status"]),
            "holes_recorded": selected_status["holes"],
            "total_holes": total_holes,
        },
        "holes": computed["rows"],
        "meta": computed["meta"],
        "course": {
            "club_name": course.get("club_name") if course else None,
            "course_name": course.get("course_name") if course else None,
            "tee_name": tee.get("tee_name") if tee else None,
            "total_yards": tee.get("total_yards") if tee else None,
            "course_rating": tee.get("course_rating") if tee else None,
            "slope_rating": tee.get("slope_rating") if tee else None,
        },
        "player_a_handicap": handicap_a,
        "player_b_handicap": handicap_b,
    }
    scorecard["point_chip_a"] = _adjust_display_points(scorecard["meta"]["total_points_a"])
    scorecard["point_chip_b"] = _adjust_display_points(scorecard["meta"]["total_points_b"])
    match_statuses.sort(
        key=lambda entry: (STATUS_PRIORITY.get(entry["status"], 3), entry["display"])
    )
    active_matches = [entry for entry in match_statuses if entry["status"] != "completed"]
    return {
        "matches": matches,
        "match_statuses": match_statuses,
        "active_matches": active_matches,
        "scorecard": scorecard,
    }


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
    course_holes = _active_course_holes()
    enriched_holes, total_net_a, total_net_b = _enrich_holes_with_net(
        holes,
        handicap_a,
        handicap_b,
        course_holes,
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


@app.get("/matches/{match_id}/scorecard", response_class=HTMLResponse)
async def match_scorecard(request: Request, match_id: int):
    result = fetch_match_result(settings.database_url, match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    key = result["match_key"]
    return RedirectResponse(url=f"/scorecard?match_key={key}")


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


@app.post("/matches/key/{match_key}/holes")
async def match_detail_submit_by_key(match_key: str, request: Request):
    match_result = fetch_match_result_by_key(settings.database_url, match_key)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, match_key)
    if not match_result:
        pairing = next((m for m in _load_pairings() if m.match_id == match_key), None)
        if not pairing:
            raise HTTPException(status_code=404, detail="Match not found")
        outcome = score_outcome(0, 0)
        match_id = insert_match_result(
            settings.database_url,
            match_name=match_display(pairing),
            player_a=pairing.player_a,
            player_b=pairing.player_b,
            match_key=pairing.match_id,
            match_code=None,
            player_a_points=0,
            player_b_points=0,
            **outcome,
        )
        match_result = fetch_match_result(settings.database_url, match_id)
    match_id = match_result["id"]
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
