import json
import re
import zipfile
import random
import string
from collections import defaultdict
from itertools import zip_longest
from pathlib import Path
from xml.etree import ElementTree as ET
from datetime import datetime

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError
from urllib.parse import quote_plus
from psycopg.errors import UniqueViolation

from app import golf_api
from app.db import (
    delete_match_results_by_tournament,
    delete_player,
    delete_players_not_in,
    ensure_schema,
    fetch_all_match_results,
    fetch_course_catalog,
    fetch_course_holes,
    fetch_course_tee_holes,
    fetch_event_settings,
    fetch_hole_scores,
    fetch_legacy_hole_scores,
    fetch_match_bonus,
    fetch_match_result,
    fetch_match_result_by_code,
    fetch_match_result_by_key,
    fetch_match_result_by_key_and_players,
    fetch_match_by_id,
    fetch_matches_by_tournament,
    fetch_match_result_ids_by_key,
    fetch_player_by_id,
    fetch_player_by_name,
    fetch_players,
    fetch_player_hole_scores,
    fetch_recent_results,
    fetch_settings,
    fetch_standings_cache,
    fetch_tournament_by_id,
    fetch_tournaments,
    insert_hole_scores,
    insert_match_result,
    insert_player_hole_scores,
    insert_tournament,
    update_tournament_status,
    next_course_id,
    reset_match_results,
    replace_course_holes,
    replace_course_tee_holes,
    replace_standings_cache,
    upsert_course,
    upsert_course_tee,
    upsert_event_setting,
    upsert_player,
    upsert_setting,
    update_match_result_fields,
    update_match_result_scores,
    upsert_match_bonus,
    set_match_finalized,
    insert_match,
    delete_match,
    delete_match_results_by_key,
    delete_match_result,
    finalize_match_result,
)
from app.course_sync import ensure_georgia_course, ensure_pebble_beach_course, import_course_to_db
from app.seed import (
    Match,
    build_pairings_from_players,
    find_match,
    match_display,
)
from app.seed_db import ensure_base_tournament
from app.demo_seed import ensure_demo_fixture, ensure_demo_players, ensure_demo_tournament
from app.demo_state import ACTIVE_TOURNAMENT_ID_KEY, DEFAULT_DIVISION_COUNT
from app.settings import load_settings, score_outcome, compute_bonus_points
from app.migrations import apply_migrations

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/golf-ui", StaticFiles(directory="golf-ui"), name="golf-ui")
templates = Jinja2Templates(directory="app/templates")
settings = load_settings()
class ActiveTournamentPayload(BaseModel):
    tournament_id: int | None = None


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

TOURNAMENT_STATUSES = ["upcoming", "active", "completed", "inactive"]

ACTIVE_MATCH_SETTING_KEY = "active_match_key"

def _match_status_info(match_id: str) -> dict[str, int | str | None]:
    match_result = fetch_match_result_by_key(settings.database_url, match_id)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, match_id)
    if not match_result:
        return {"status": "not_started", "holes": 0, "match_result": None}
    holes = fetch_hole_scores(settings.database_url, match_result["id"])
    status = "completed" if len(holes) >= 18 else "in_progress"
    if match_result.get("finalized"):
        status = "completed"
    return {"status": status, "holes": len(holes), "match_result": match_result}


def _safe_int(value: str | int | None) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: str | float | None) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _player_handicap_by_name(name: str | None) -> int:
    if not name:
        return 0
    player = fetch_player_by_name(settings.database_url, name)
    return player.get("handicap", 0) if player else 0

def _player_name_by_id(player_id: int | None) -> str:
    if not player_id:
        return "Player"
    player = fetch_player_by_id(settings.database_url, player_id)
    if player:
        return player.get("name") or f"Player {player_id}"
    return f"Player {player_id}"


def _player_id_by_name(name: str | None) -> int | None:
    if not name:
        return None
    player = fetch_player_by_name(settings.database_url, name)
    return player.get("id") if player else None


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


def _target_player_roster_size(tournament_settings: dict | None) -> int:
    if not tournament_settings:
        return sum(DEFAULT_DIVISION_COUNT.values())
    raw_value = tournament_settings.get("player_count")
    try:
        count = int(raw_value or 0)
    except (TypeError, ValueError):
        count = 0
    if count <= 0:
        count = sum(DEFAULT_DIVISION_COUNT.values())
    return count


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


def _active_course_holes(
    tournament_settings: dict | None = None, course_tee_override: int | None = None
) -> list[dict]:
    override_id = course_tee_override
    if override_id:
        holes = fetch_course_tee_holes(settings.database_url, override_id)
        if holes:
            return holes
    t_settings = tournament_settings or _load_tournament_settings(_get_active_tournament_id())
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


def _course_holes_for_match(
    match_result: dict | None, tournament_settings: dict | None = None
) -> list[dict]:
    tournament_settings = tournament_settings or _load_tournament_settings(_get_active_tournament_id())
    course_tee_id = _safe_int(match_result.get("course_tee_id") if match_result else None)
    return _active_course_holes(tournament_settings, course_tee_id)


def _course_display_info(
    match_result: dict | None,
    tournament_settings: dict | None = None,
    catalog: list[dict] | None = None,
) -> dict[str, str | float | None]:
    snapshot = match_result.get("course_snapshot") if match_result else None
    if snapshot:
        return snapshot
    tournament_settings = tournament_settings or _load_tournament_settings(_get_active_tournament_id())
    catalog = catalog or fetch_course_catalog(settings.database_url)
    course_id = _safe_int(match_result.get("course_id") if match_result else None) or _safe_int(
        tournament_settings.get("course_id")
    )
    tee_id = _safe_int(match_result.get("course_tee_id") if match_result else None) or _safe_int(
        tournament_settings.get("course_tee_id")
    )
    if not course_id:
        return {}
    course_entry = next((entry for entry in catalog if entry.get("id") == course_id), None)
    tee_entry = None
    if course_entry and tee_id:
        tee_entry = next((tee for tee in course_entry.get("tees", []) if tee.get("id") == tee_id), None)
    return {
        "club_name": course_entry.get("club_name") if course_entry else None,
        "course_name": course_entry.get("course_name") if course_entry else None,
        "tee_name": tee_entry.get("tee_name") if tee_entry else None,
        "total_yards": tee_entry.get("total_yards") if tee_entry else None,
        "course_rating": tee_entry.get("course_rating") if tee_entry else None,
        "slope_rating": tee_entry.get("slope_rating") if tee_entry else None,
    }


def _build_course_tee_map(course_catalog: list[dict]) -> dict[str, list[dict]]:
    tee_map: dict[str, list[dict]] = {}
    for course in course_catalog:
        course_id = course.get("id")
        if course_id is None:
            continue
        key = str(course_id)
        tees = []
        for tee in course.get("tees") or []:
            if (tee.get("gender") or "").strip().lower() != "male":
                continue
            tee_name = tee.get("tee_name") or "Tee"
            total_yards = tee.get("total_yards")
            label_parts = [tee_name]
            if total_yards:
                label_parts.append(f" • {total_yards} yds")
            slope = tee.get("slope_rating")
            rating = tee.get("course_rating")
            if rating or slope:
                rating_text = []
                if rating:
                    rating_text.append(f"CR {rating}")
                if slope:
                    rating_text.append(f"SR {slope}")
                label_parts.append(f" ({' / '.join(rating_text)})")
            tees.append(
                {
                    "id": str(tee.get("id") or ""),
                    "label": "".join(label_parts),
                    "tee_name": tee_name,
                    "yards": total_yards,
                }
            )
        if tees:
            tee_map[key] = tees
    return tee_map


def _scorecard_data_for_match(
    match_result: dict,
    holes: list[dict],
    handicap_a: int,
    handicap_b: int,
    tournament_settings: dict | None = None,
    match_length: int | None = None,
    start_hole: int | None = None,
    use_snapshot: bool = True,
) -> dict:
    snapshot = (match_result.get("scorecard_snapshot") if match_result else None) or {}
    if use_snapshot and snapshot.get("rows"):
        return {
            "rows": snapshot.get("rows", []),
            "course": snapshot.get("course", []),
            "meta": snapshot.get("meta", {}),
        }
    return _build_scorecard_rows(
        holes,
        handicap_a,
        handicap_b,
        _course_holes_for_match(match_result, tournament_settings),
        match_length=match_length or 18,
        start_hole=start_hole or 1,
    )


def _estimate_points_from_raw_scores(holes: list[dict]) -> tuple[float, float]:
    points_a = 0.0
    points_b = 0.0
    for entry in holes:
        a = _numeric_score(entry.get("player_a_score"))
        b = _numeric_score(entry.get("player_b_score"))
        if a is None or b is None:
            continue
        if a < b:
            points_a += 1.0
        elif b < a:
            points_b += 1.0
        else:
            points_a += 0.5
            points_b += 0.5
    return points_a, points_b


def _seed_default_players() -> list[dict]:
    ensure_demo_fixture(settings.database_url)
    return fetch_players(settings.database_url)


def _team_label(match: Match, side: str) -> str:
    if side == "A":
        names = [match.player_a, match.player_c]
    else:
        names = [match.player_b, match.player_d]
    return " & ".join([name for name in names if name])


def _load_pairings(tournament_id: int | None = None) -> list[Match]:
    target_tournament = tournament_id if tournament_id is not None else _get_active_tournament_id()
    if target_tournament is None:
        return []
    scheduled = fetch_matches_by_tournament(settings.database_url, target_tournament)
    if scheduled:
        pairings: list[Match] = []
        for entry in scheduled:
            player_a_name = entry.get("player_a_name") or _player_name_by_id(entry.get("player_a_id"))
            player_b_name = entry.get("player_b_name") or _player_name_by_id(entry.get("player_b_id"))
            player_c_name = entry.get("player_c_name") or _player_name_by_id(entry.get("player_c_id"))
            player_d_name = entry.get("player_d_name") or _player_name_by_id(entry.get("player_d_id"))
            division = entry.get("division") or "Open"
            pairings.append(
                Match(
                    entry["match_key"],
                    division,
                    player_a_name,
                    player_b_name,
                    player_c_name,
                    player_d_name,
                    course_id=entry.get("course_id"),
                    course_tee_id=entry.get("course_tee_id"),
                    hole_count=entry.get("hole_count") or 18,
                    start_hole=entry.get("start_hole") or 1,
                )
            )
        return pairings
    players = _players_for_tournament(target_tournament)
    if not players:
        return []
    return build_pairings_from_players(players)


def _players_for_tournament(tournament_id: int | None) -> list[dict]:
    if not tournament_id:
        return []
    players = fetch_players(settings.database_url)
    return [player for player in players if player.get("tournament_id") == tournament_id]


def _ensure_match_result_for_pairing(pairing: Match, *, tournament_id: int | None = None) -> dict | None:
    if not pairing:
        return None
    match_result = fetch_match_result_by_key(settings.database_url, pairing.match_id)
    if match_result:
        return match_result
    tournament_id = tournament_id or _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)
    base_course_id = pairing.course_id or _safe_int(tournament_settings.get("course_id"))
    base_course_tee = pairing.course_tee_id or _safe_int(tournament_settings.get("course_tee_id"))
    match_length = pairing.hole_count or 18
    player_a_info = fetch_player_by_name(settings.database_url, pairing.player_a) or {}
    player_b_info = fetch_player_by_name(settings.database_url, pairing.player_b) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    player_a_id = player_a_info.get("id")
    player_b_id = player_b_info.get("id")
    player_c_id = _player_id_by_name(pairing.player_c)
    player_d_id = _player_id_by_name(pairing.player_d)
    outcome = score_outcome(0, 0)
    team_a_label = _team_label(pairing, "A")
    team_b_label = _team_label(pairing, "B")
    recorded_player_a = pairing.player_a or team_a_label or "Player A"
    recorded_player_b = pairing.player_b or team_b_label or "Player B"
    initial_outcome = _apply_bonus_constraints(
        outcome,
        0.0,
        0.0,
        0,
        match_length,
    )
    insert_match_result(
        settings.database_url,
        match_name=match_display(pairing),
        player_a=recorded_player_a,
        player_b=recorded_player_b,
        match_key=pairing.match_id,
        match_code=None,
        player_a_points=initial_outcome["player_a_points"],
        player_b_points=initial_outcome["player_b_points"],
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        player_c_id=player_c_id,
        player_d_id=player_d_id,
        player_a_bonus=initial_outcome["player_a_bonus"],
        player_b_bonus=initial_outcome["player_b_bonus"],
        player_a_total=initial_outcome["player_a_total"],
        player_b_total=initial_outcome["player_b_total"],
        winner=initial_outcome["winner"],
        course_id=base_course_id,
        course_tee_id=base_course_tee,
        tournament_id=tournament_id,
        player_a_handicap=handicap_a,
        player_b_handicap=handicap_b,
        hole_count=match_length,
        start_hole=pairing.start_hole or 1,
    )
    _ensure_cd_match_result_for_pairing(pairing, tournament_id, tournament_settings)
    return fetch_match_result_by_key(settings.database_url, pairing.match_id)


def _ensure_cd_match_result_for_pairing(
    pairing: Match,
    tournament_id: int | None,
    tournament_settings: dict[str, str],
) -> None:
    if not pairing or not pairing.player_c or not pairing.player_d:
        return
    cd_key = f"{pairing.match_id}-cd"
    existing = fetch_match_result_by_key(settings.database_url, cd_key)
    if existing:
        return
    player_c = pairing.player_c.strip()
    player_d = pairing.player_d.strip()
    if not player_c or not player_d:
        return
    player_c_id = _player_id_by_name(player_c)
    player_d_id = _player_id_by_name(player_d)
    handicap_c = _player_handicap_by_name(player_c)
    handicap_d = _player_handicap_by_name(player_d)
    course_id = pairing.course_id or _safe_int(tournament_settings.get("course_id"))
    course_tee_id = pairing.course_tee_id or _safe_int(tournament_settings.get("course_tee_id"))
    outcome = score_outcome(0, 0)
    match_length = pairing.hole_count or 18
    initial_cd_outcome = _apply_bonus_constraints(
        outcome,
        0.0,
        0.0,
        0,
        match_length,
    )
    insert_match_result(
        settings.database_url,
        match_name=f"{player_c} vs {player_d}",
        player_a=player_c,
        player_b=player_d,
        match_key=cd_key,
        match_code=None,
        player_a_points=initial_cd_outcome["player_a_points"],
        player_b_points=initial_cd_outcome["player_b_points"],
        player_a_id=player_c_id,
        player_b_id=player_d_id,
        player_c_id=None,
        player_d_id=None,
        player_a_handicap=handicap_c,
        player_b_handicap=handicap_d,
        course_id=course_id,
        course_tee_id=course_tee_id,
        tournament_id=tournament_id,
        hole_count=pairing.hole_count or 18,
        start_hole=pairing.start_hole or 1,
        player_a_bonus=initial_cd_outcome["player_a_bonus"],
        player_b_bonus=initial_cd_outcome["player_b_bonus"],
        player_a_total=initial_cd_outcome["player_a_total"],
        player_b_total=initial_cd_outcome["player_b_total"],
        winner=initial_cd_outcome["winner"],
    )


def _match_record_player_name(record: dict | None, name_key: str, id_key: str) -> str:
    if not record:
        return ""
    name = record.get(name_key)
    if name:
        return name
    player_id = record.get(id_key)
    if player_id:
        return _player_name_by_id(player_id)
    return ""


def _match_record_to_pairing(record: dict | None) -> Match | None:
    if not record:
        return None
    return Match(
        match_id=record.get("match_key") or "",
        division=record.get("division") or "Open",
        player_a=_match_record_player_name(record, "player_a_name", "player_a_id"),
        player_b=_match_record_player_name(record, "player_b_name", "player_b_id"),
        player_c=_match_record_player_name(record, "player_c_name", "player_c_id") or None,
        player_d=_match_record_player_name(record, "player_d_name", "player_d_id") or None,
        course_id=record.get("course_id"),
        course_tee_id=record.get("course_tee_id"),
        hole_count=record.get("hole_count") or 18,
        start_hole=record.get("start_hole") or 1,
    )


def _resolve_match_result_context(match_id: int) -> tuple[dict | None, dict | None]:
    result = fetch_match_result(settings.database_url, match_id)
    if result:
        return result, None
    match_record = fetch_match_by_id(settings.database_url, match_id)
    if not match_record:
        return None, None
    match_key = match_record.get("match_key")
    if not match_key:
        return None, match_record
    result = fetch_match_result_by_key(settings.database_url, match_key)
    if not result:
        pairing = _match_record_to_pairing(match_record)
        if pairing:
            result = _ensure_match_result_for_pairing(
                pairing,
                tournament_id=match_record.get("tournament_id"),
            )
    return result, match_record


def _load_tournament_settings(tournament_id: int | None = None) -> dict[str, str]:
    stored = fetch_settings(settings.database_url)
    result = DEFAULT_TOURNAMENT_SETTINGS.copy()
    result.update(stored)
    if tournament_id:
        event_settings = fetch_event_settings(settings.database_url, tournament_id)
        result.update(event_settings)
    return result


def _tournament_id_for_result(match_result: dict | None) -> int | None:
    if match_result:
        tournament_id = match_result.get("tournament_id")
        if tournament_id:
            return tournament_id
    return _get_active_tournament_id()


def _get_active_match_key() -> str | None:
    stored = fetch_settings(settings.database_url)
    value = (stored.get(ACTIVE_MATCH_SETTING_KEY) or "").strip()
    return value or None


def _set_active_match_key(match_key: str | None) -> None:
    upsert_setting(settings.database_url, ACTIVE_MATCH_SETTING_KEY, match_key or "")


def _get_active_tournament_id() -> int | None:
    stored = fetch_settings(settings.database_url)
    value = (stored.get(ACTIVE_TOURNAMENT_ID_KEY) or "").strip()
    return _safe_int(value)


def _set_active_tournament_id(tournament_id: int | None) -> None:
    upsert_setting(
        settings.database_url,
        ACTIVE_TOURNAMENT_ID_KEY,
        str(tournament_id) if tournament_id else "",
    )


def _ensure_match_results_for_pairings() -> None:
    """
    Ensure every pairing has a persistent match_result with match_key and match_code.
    """
    matches = _load_pairings()
    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(_get_active_tournament_id())
    course_catalog = fetch_course_catalog(settings.database_url)
    selected_course_id = tournament_settings.get("course_id", "")
    selected_course_tee_id = tournament_settings.get("course_tee_id", "")
    selected_course = next((c for c in course_catalog if str(c["id"]) == str(selected_course_id)), None)
    selected_course_tee = None
    if selected_course:
        selected_course_tee = next(
            (t for t in selected_course.get("tees", []) if str(t.get("id")) == str(selected_course_tee_id)),
            None,
        )
    course_id = _safe_int(tournament_settings.get("course_id"))
    course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
    for pairing in matches:
        try:
            existing = fetch_match_result_by_key(settings.database_url, pairing.match_id)
            if existing:
                continue
            outcome = score_outcome(0, 0)
            player_a_id = _player_id_by_name(pairing.player_a)
            player_b_id = _player_id_by_name(pairing.player_b)
            player_c_id = _player_id_by_name(pairing.player_c)
            player_d_id = _player_id_by_name(pairing.player_d)
            insert_match_result(
                settings.database_url,
                match_name=match_display(pairing),
                player_a=pairing.player_a,
                player_b=pairing.player_b,
                match_key=pairing.match_id,
                match_code=None,
                player_a_id=player_a_id,
                player_b_id=player_b_id,
                player_c_id=player_c_id,
                player_d_id=player_d_id,
                player_a_points=0,
                player_b_points=0,
                tournament_id=tournament_id,
                course_id=course_id,
                course_tee_id=course_tee_id,
                hole_count=pairing.hole_count or 18,
                start_hole=pairing.start_hole or 1,
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


def _normalize_player_name(value: str | None) -> str:
    return (value or "").strip().lower()


def _find_result_by_players(records: list[dict], player_a: str, player_b: str) -> dict | None:
    normalized_a = _normalize_player_name(player_a)
    normalized_b = _normalize_player_name(player_b)
    if not normalized_a or not normalized_b:
        return None
    for entry in records:
        row = entry["result"]
        name_a = _normalize_player_name(row.get("player_a_name"))
        name_b = _normalize_player_name(row.get("player_b_name"))
        if (name_a == normalized_a and name_b == normalized_b) or (
            name_a == normalized_b and name_b == normalized_a
        ):
            return entry
    return None
    return None


def _matches_by_key(tournament_id: int | None) -> dict[str, dict]:
    if not tournament_id:
        return {}
    matches = fetch_matches_by_tournament(settings.database_url, tournament_id)
    return {entry.get("match_key") or "": entry for entry in matches if entry.get("match_key")}


def _match_cleanup_cd_stats(
    result: dict,
    matches_by_key: dict[str, dict] | None = None,
) -> list[dict] | None:
    match_key = (result.get("match_key") or "").strip()
    if not match_key:
        return None
    tournament_id = result.get("tournament_id") or _tournament_id_for_result(result)
    if not tournament_id:
        return None
    lookup = matches_by_key or _matches_by_key(tournament_id)
    is_cd_result = match_key.lower().endswith("-cd")
    pairing_key = match_key[:-3] if is_cd_result else match_key
    pairing = lookup.get(pairing_key, {})
    if is_cd_result:
        player_c_name = (result.get("player_a_name") or "").strip()
        player_d_name = (result.get("player_b_name") or "").strip()
    else:
        player_c_name = (pairing.get("player_c_name") or "").strip()
        player_d_name = (pairing.get("player_d_name") or "").strip()
    if not player_c_name or not player_d_name:
        return None
    base_result = fetch_match_result_by_key(settings.database_url, pairing_key) if pairing_key else None
    holes = []
    if base_result:
        holes = fetch_hole_scores(settings.database_url, base_result["id"])
    if not holes:
        holes = fetch_hole_scores(settings.database_url, result["id"])
    if not holes:
        return None
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(result.get("hole_count")) or 18
    start_hole = _safe_int(result.get("start_hole")) or 1
    holes_for_cd = [
        {
            "hole_number": entry.get("hole_number"),
            "player_a_score": entry.get("player_c_score") or 0,
            "player_b_score": entry.get("player_d_score") or 0,
            "player_c_score": entry.get("player_c_score") or 0,
            "player_d_score": entry.get("player_d_score") or 0,
            "player_a_net": entry.get("player_c_net"),
            "player_b_net": entry.get("player_d_net"),
            "player_c_net": entry.get("player_c_net"),
            "player_d_net": entry.get("player_d_net"),
        }
        for entry in holes
    ]
    handicap_c = _player_handicap_by_name(player_c_name)
    handicap_d = _player_handicap_by_name(player_d_name)
    scorecard_cd = _scorecard_data_for_match(
        result,
        holes_for_cd,
        handicap_c,
        handicap_d,
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
        use_snapshot=False,
    )
    player_c_total = scorecard_cd["meta"]["total_points_a"]
    player_d_total = scorecard_cd["meta"]["total_points_b"]
    if player_c_total == 0 and player_d_total == 0:
        fallback_c, fallback_d = _estimate_points_from_raw_scores(holes_for_cd)
        if fallback_c or fallback_d:
            player_c_total = fallback_c
            player_d_total = fallback_d
            scorecard_cd.setdefault("meta", {})["total_points_a"] = fallback_c
            scorecard_cd.setdefault("meta", {})["total_points_b"] = fallback_d
    outcome = score_outcome(player_c_total, player_d_total)
    adjusted = _apply_bonus_constraints(outcome, player_c_total, player_d_total, len(holes_for_cd), match_length)
    return [
        {
            "name": player_c_name,
            "role": "C",
            "points": player_c_total,
            "bonus": adjusted["player_a_bonus"],
            "total": adjusted["player_a_total"],
        },
        {
            "name": player_d_name,
            "role": "D",
            "points": player_d_total,
            "bonus": adjusted["player_b_bonus"],
            "total": adjusted["player_b_total"],
        },
    ]


def _match_cleanup_ab_stats(result: dict) -> list[dict] | None:
    holes = fetch_hole_scores(settings.database_url, result["id"])
    if not holes:
        return None
    tournament_id = result.get("tournament_id")
    if not tournament_id:
        return None
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(result.get("hole_count")) or 18
    start_hole = _safe_int(result.get("start_hole")) or 1
    scorecard = _scorecard_data_for_match(
        result,
        holes,
        result.get("player_a_handicap") or _player_handicap_by_name(result.get("player_a_name")),
        result.get("player_b_handicap") or _player_handicap_by_name(result.get("player_b_name")),
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
        use_snapshot=False,
    )
    ab_stats = []
    points_a = scorecard["meta"]["total_points_a"]
    points_b = scorecard["meta"]["total_points_b"]
    outcome = score_outcome(points_a, points_b)
    adjusted = _apply_bonus_constraints(outcome, points_a, points_b, len(holes), match_length)
    bonus_a = adjusted["player_a_bonus"]
    bonus_b = adjusted["player_b_bonus"]
    total_a = adjusted["player_a_total"]
    total_b = adjusted["player_b_total"]
    ab_stats.append(
        {
            "name": result.get("player_a_name") or "Player A",
            "role": "A",
            "points": points_a,
            "bonus": bonus_a,
            "total": total_a,
        }
    )
    ab_stats.append(
        {
            "name": result.get("player_b_name") or "Player B",
            "role": "B",
            "points": points_b,
            "bonus": bonus_b,
            "total": total_b,
        }
    )
    return ab_stats


def _aggregate_standings_entries(results: list[dict], tournament_id: int | None) -> list[dict]:
    if not tournament_id:
        return []
    tournament_players = [
        player for player in fetch_players(settings.database_url) if player.get("tournament_id") == tournament_id
    ]
    if not tournament_players:
        return []
    divisions_by_player = {player["name"]: player["division"] for player in tournament_players}
    player_ids_by_name = {
        player["name"]: player["id"] for player in tournament_players if player.get("name") and player.get("id")
    }
    players_by_id = {player["id"]: player for player in tournament_players if player.get("id")}
    seeds = {player["name"]: player.get("seed", 0) for player in tournament_players}
    stats: dict[str, dict] = {
        name: _empty_stat(name, division)
        for name, division in divisions_by_player.items()
    }

    allowed_players = set(divisions_by_player.keys())
    matches_by_key = _matches_by_key(tournament_id)
    tournament_settings = _load_tournament_settings(tournament_id)

    for result in results:
        if result.get("tournament_id") != tournament_id:
            continue
        match_key = result.get("match_key") or ""
        if match_key.endswith("-cd"):
            continue
        hole_entries = fetch_hole_scores(settings.database_url, result["id"])
        if not hole_entries and not (result.get("player_a_total") or result.get("player_b_total")):
            continue
        hole_count = len(hole_entries)
        recorded_groups: list[list[dict]] = []
        if hole_entries:
            ab_stats = _match_cleanup_ab_stats(result)
            if ab_stats:
                recorded_groups.append(ab_stats)
            cd_stats = _match_cleanup_cd_stats(result, matches_by_key)
            if cd_stats:
                recorded_groups.append(cd_stats)
        if recorded_groups:
            for group in recorded_groups:
                if len(group) != 2:
                    continue
                first = group[0]
                second = group[1]
                first_total = first.get("total") or first.get("points") or 0
                second_total = second.get("total") or second.get("points") or 0
                first_points = first.get("points") or first_total
                second_points = second.get("points") or second_total
                first_recorded = first_total or first_points
                second_recorded = second_total or second_points
                winner = "A" if first_total > second_total else "B" if second_total > first_total else "T"
                if first["name"] in allowed_players:
                  _record_player(
                    stats,
                    first["name"],
                    divisions_by_player.get(first["name"], "Open"),
                    first_recorded,
                    second_points,
                    winner,
                    "A",
                    hole_count,
                  )
                if second["name"] in allowed_players:
                  _record_player(
                    stats,
                    second["name"],
                    divisions_by_player.get(second["name"], "Open"),
                    second_recorded,
                    first_points,
                    winner,
                    "B",
                    hole_count,
                  )
            continue
        player_a_total = result.get("player_a_total") or 0
        player_b_total = result.get("player_b_total") or 0
        winner = result.get("winner") or "T"
        if player_a_total == 0 and player_b_total == 0:
            player_a_total, player_b_total, winner = _resolve_result_totals(result)
        if player_a_total == 0 and player_b_total == 0:
            continue
        if result["player_a_name"] in allowed_players:
            _record_player(
                stats,
                result["player_a_name"],
                divisions_by_player.get(result["player_a_name"], "Open"),
                player_a_total,
                player_b_total,
                winner,
                "A",
                hole_count,
            )
        if result["player_b_name"] in allowed_players:
            _record_player(
                stats,
                result["player_b_name"],
                divisions_by_player.get(result["player_b_name"], "Open"),
                player_b_total,
                player_a_total,
                winner,
                "B",
                hole_count,
            )

    entries: list[dict] = []
    for entry in stats.values():
        point_diff = entry["points_for"] - entry["points_against"]
        entries.append(
            {
                "player_name": entry["name"],
                "division": entry["division"],
                "seed": seeds.get(entry["name"], 0),
                "matches": entry["matches"],
                "wins": entry["wins"],
                "ties": entry["ties"],
                "losses": entry["losses"],
                "points_for": entry["points_for"],
                "points_against": entry["points_against"],
                "point_diff": point_diff,
                "holes_played": entry["holes_played"],
            }
        )
    return entries


def _refresh_standings_cache(tournament_id: int | None, results: list[dict] | None = None) -> None:
    if not tournament_id:
        return
    source_results = results or fetch_all_match_results(settings.database_url)
    entries = _aggregate_standings_entries(source_results, tournament_id)
    replace_standings_cache(settings.database_url, tournament_id, entries)


def _reset_tournament_match_history(tournament_id: int) -> int:
    deleted = delete_match_results_by_tournament(settings.database_url, tournament_id)
    _refresh_standings_cache(tournament_id)
    return deleted


def _resolve_result_totals(result: dict) -> tuple[float, float, str]:
    player_a_total = result.get("player_a_total") or 0
    player_b_total = result.get("player_b_total") or 0
    winner = result.get("winner") or "T"
    if player_a_total == 0 and player_b_total == 0:
        holes = fetch_hole_scores(settings.database_url, result["id"])
        if holes:
            match_result = fetch_match_result(settings.database_url, result["id"])
            player_a_info = fetch_player_by_name(settings.database_url, result["player_a_name"]) or {}
            player_b_info = fetch_player_by_name(settings.database_url, result["player_b_name"]) or {}
            handicap_a = player_a_info.get("handicap", 0)
            handicap_b = player_b_info.get("handicap", 0)
            tournament_settings = _load_tournament_settings(_tournament_id_for_result(match_result))
            match_length = _safe_int(match_result.get("hole_count")) or 18
            match_start_hole = _safe_int(match_result.get("start_hole")) or 1
            computed = _scorecard_data_for_match(
                match_result,
                holes,
                handicap_a,
                handicap_b,
                tournament_settings,
                match_length=match_length,
                start_hole=match_start_hole,
                use_snapshot=False,
            )
            player_a_total = computed["meta"]["total_points_a"]
            player_b_total = computed["meta"]["total_points_b"]
            if player_a_total > player_b_total:
                winner = "A"
            elif player_b_total > player_a_total:
                winner = "B"
            else:
                winner = "T"
    return player_a_total, player_b_total, winner


def _bonus_allowed(recorded_count: int, expected_length: int) -> bool:
    if recorded_count >= expected_length:
        return True
    return recorded_count in {9, 18}


def build_standings(results: list[dict], tournament_id: int | None = None) -> list[dict]:
    if tournament_id is None:
        return []
    cache_rows = fetch_standings_cache(settings.database_url, tournament_id)
    tournament_players = [
        player for player in fetch_players(settings.database_url) if player.get("tournament_id") == tournament_id
    ]
    player_names = {player["name"] for player in tournament_players}
    if not cache_rows or (player_names and any(row["player_name"] not in player_names for row in cache_rows)):
        _refresh_standings_cache(tournament_id, results)
        cache_rows = fetch_standings_cache(settings.database_url, tournament_id)
    if not cache_rows:
        return []

    division_groups: dict[str, list[dict]] = defaultdict(list)
    for row in cache_rows:
        normalized = dict(row)
        normalized["name"] = normalized["player_name"]
        division_groups[normalized["division"]].append(normalized)

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
            remaining = 40 - holes_played
            player["pts_remaining"] = max(0.0, remaining)
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
    tournament_id = _tournament_id_for_result(match_result)
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(match_result.get("hole_count") if match_result else None) or 18
    match_start_hole = _safe_int(match_result.get("start_hole") if match_result else None) or 1
    computed = _scorecard_data_for_match(
        match_result if match_result else {},
        holes,
        handicap_a,
        handicap_b,
        tournament_settings,
        match_length=match_length,
        start_hole=match_start_hole,
        use_snapshot=False,
    )
    hole_total_a = sum((row.get("net_a") or 0) for row in computed["rows"])
    hole_total_b = sum((row.get("net_b") or 0) for row in computed["rows"])
    match_total_holes = match_length
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
    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)
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


@app.get("/api/active_match")
async def api_active_match():
    match_key = _get_active_match_key()
    if not match_key:
        return JSONResponse({"active_match": None})
    context = _scorecard_context(match_key)
    scorecard = context.get("scorecard")
    if not scorecard:
        return JSONResponse({"active_match": None})
    return JSONResponse({"active_match": _serialize_scorecard_for_studio(scorecard)})


@app.get("/api/match_scorecard")
async def api_match_scorecard(match_key: str | None = None, match_code: str | None = None):
    resolved_key = match_key
    if not resolved_key and match_code:
        result = fetch_match_result_by_code(settings.database_url, match_code)
        if not result:
            raise HTTPException(status_code=404, detail="Match not found for provided code")
        resolved_key = result["match_key"]
    if not resolved_key:
        raise HTTPException(status_code=400, detail="match_key or match_code is required")
    context = _scorecard_context(resolved_key)
    scorecard = context.get("scorecard")
    if not scorecard:
        raise HTTPException(status_code=404, detail="Scorecard not available")
    return JSONResponse(_serialize_scorecard_for_studio(scorecard))


def _serialize_tournament(entry: dict) -> dict:
    return {
        "id": entry.get("id"),
        "name": entry.get("name"),
        "description": entry.get("description") or "",
        "status": entry.get("status") or "",
    }


@app.get("/api/tournaments")
async def api_list_tournaments():
    tournaments = fetch_tournaments(settings.database_url)
    return JSONResponse(
        {"tournaments": [_serialize_tournament(entry) for entry in tournaments]}
    )


@app.get("/api/active_tournament")
async def api_get_active_tournament():
    active_id = _get_active_tournament_id()
    tournaments = fetch_tournaments(settings.database_url)
    active = next((entry for entry in tournaments if entry["id"] == active_id), None)
    return JSONResponse(
        {
            "active_tournament_id": active_id,
            "active_tournament": _serialize_tournament(active) if active else None,
        }
    )


@app.post("/api/active_tournament")
async def api_set_active_tournament(payload: ActiveTournamentPayload):
    tournament_id = payload.tournament_id
    if tournament_id:
        if not fetch_tournament_by_id(settings.database_url, tournament_id):
            raise HTTPException(status_code=404, detail="Tournament not found")
    _set_active_tournament_id(tournament_id)
    return JSONResponse(
        {
            "active_tournament_id": tournament_id,
        }
    )


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
    apply_migrations(settings.database_url)
    ensure_base_tournament()
    ensure_pebble_beach_course(settings.database_url)
    ensure_georgia_course(settings.database_url)
    _seed_default_players()
    ensure_demo_fixture(settings.database_url)
    _load_course_holes()
    _migrate_player_scorecards_from_legacy()


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

    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)

    outcome = score_outcome(player_a_points, player_b_points)
    match_length = resolved_match.hole_count if resolved_match else 18
    match_start_hole = resolved_match.start_hole if resolved_match else 1
    player_a_id = _player_id_by_name(player_a)
    player_b_id = _player_id_by_name(player_b)
    existing_result = fetch_match_result_by_key_and_players(
        settings.database_url,
        match_id,
        player_a,
        player_b,
    )
    if existing_result:
        update_match_result_scores(
            settings.database_url,
            existing_result["id"],
            player_a_points=player_a_points,
            player_b_points=player_b_points,
            player_a_bonus=outcome["player_a_bonus"],
            player_b_bonus=outcome["player_b_bonus"],
            player_a_total=outcome["player_a_total"],
            player_b_total=outcome["player_b_total"],
            winner=outcome["winner"],
            player_a_id=player_a_id,
            player_b_id=player_b_id,
        )
    else:
        insert_match_result(
            settings.database_url,
            match_name=match_name,
            player_a=player_a,
            player_b=player_b,
            match_key=match_id,
            match_code=None,
        player_a_points=player_a_points,
        player_b_points=player_b_points,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        player_a_handicap=_player_handicap_by_name(player_a),
        player_b_handicap=_player_handicap_by_name(player_b),
        course_id=_safe_int(tournament_settings.get("course_id")),
        course_tee_id=_safe_int(tournament_settings.get("course_tee_id")),
        tournament_id=tournament_id,
        hole_count=match_length,
        start_hole=match_start_hole,
        **(outcome if _bonus_allowed(len(holes := fetch_hole_scores(settings.database_url, match_id) if False else []), match_length) else {}),
        )
    _refresh_standings_cache(tournament_id)

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

    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)
    outcome = score_outcome(payload.player_a_points, payload.player_b_points)
    match_length = resolved_match.hole_count if resolved_match else 18
    match_start_hole = resolved_match.start_hole if resolved_match else 1
    player_a_id = _player_id_by_name(player_a)
    player_b_id = _player_id_by_name(player_b)
    existing_result = fetch_match_result_by_key_and_players(
        settings.database_url,
        payload.match_id or "",
        player_a,
        player_b,
    )
    if existing_result:
        record_id = existing_result["id"]
        update_match_result_scores(
            settings.database_url,
            record_id,
            player_a_points=payload.player_a_points,
            player_b_points=payload.player_b_points,
            player_a_bonus=outcome["player_a_bonus"],
            player_b_bonus=outcome["player_b_bonus"],
            player_a_total=outcome["player_a_total"],
            player_b_total=outcome["player_b_total"],
            winner=outcome["winner"],
            player_a_id=player_a_id,
            player_b_id=player_b_id,
        )
    else:
        record_id = insert_match_result(
            settings.database_url,
            match_name=match_name,
            player_a=player_a,
            player_b=player_b,
            match_key=payload.match_id or "",
            match_code=None,
            player_a_points=payload.player_a_points,
            player_b_points=payload.player_b_points,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
            player_a_handicap=_player_handicap_by_name(player_a),
            player_b_handicap=_player_handicap_by_name(player_b),
            course_id=_safe_int(tournament_settings.get("course_id")),
            course_tee_id=_safe_int(tournament_settings.get("course_tee_id")),
            tournament_id=tournament_id,
            hole_count=match_length,
            start_hole=match_start_hole,
            **outcome,
        )
    _refresh_standings_cache(tournament_id)
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
    tournament_id: int | None = None,
) -> dict:
    tournament_settings = _load_tournament_settings(tournament_id)
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
        "course_tee_map": _build_course_tee_map(course_catalog),
        "player_roster_size": _target_player_roster_size(tournament_settings),
    }
    if not authorized:
        context["setup_status"] = context["setup_status"] or "Invalid or missing PIN."
        return context

    players = fetch_players(settings.database_url)
    if not players:
        players = _seed_default_players()
    context["players"] = players
    context["pairings"] = build_pairings_from_players(players)
    context["tournament_settings"] = tournament_settings
    context["active_tournament_id"] = tournament_id
    context["active_tournament"] = (
        fetch_tournament_by_id(settings.database_url, tournament_id)
        if tournament_id
        else None
    )
    context["course_holes"] = _active_course_holes(tournament_settings)
    return context


def _manual_course_context(request: Request, pin: str, status: str | None = None) -> dict:
    return {
        "request": request,
        "pin": pin,
        "authorized": pin == settings.scoring_pin,
        "status": status,
    }


@app.get("/admin/courses/new", response_class=HTMLResponse)
async def manual_course_page(request: Request, pin: str = "", status: str | None = None):
    return templates.TemplateResponse(
        "manual_course.html",
        _manual_course_context(request, pin, status),
    )


@app.post("/admin/courses/new")
async def manual_course_submit(request: Request):
    form = await request.form()
    pin = (form.get("pin") or "").strip()
    if pin != settings.scoring_pin:
        return templates.TemplateResponse(
            "manual_course.html",
            _manual_course_context(request, pin, "Invalid PIN"),
        )
    club_name = (form.get("club_name") or "").strip()
    course_name = (form.get("course_name") or "").strip()
    tee_name = (form.get("tee_name") or "").strip()
    if not (club_name and course_name and tee_name):
        return templates.TemplateResponse(
            "manual_course.html",
            _manual_course_context(request, pin, "Club, course, and tee names are required."),
        )
    city = (form.get("city") or "").strip() or None
    state = (form.get("state") or "").strip() or None
    country = (form.get("country") or "").strip() or None
    course_rating = _safe_float(form.get("course_rating"))
    slope_rating = _safe_int(form.get("slope_rating"))
    par_total_input = _safe_int(form.get("par_total"))
    total_yards = _safe_int(form.get("total_yards"))
    latitude = _safe_float(form.get("latitude"))
    longitude = _safe_float(form.get("longitude"))
    override_id = form.get("course_id_override")
    try:
        course_id = int(override_id)
    except (TypeError, ValueError):
        course_id = next_course_id(settings.database_url)
    raw = None
    upsert_course(
        settings.database_url,
        course_id,
        club_name,
        course_name,
        city,
        state,
        country,
        latitude,
        longitude,
        raw,
    )
    holes: list[dict] = []
    total_par = 0
    for idx in range(1, 19):
        par = _safe_int(form.get(f"hole_{idx}_par"))
        handicap = _safe_int(form.get(f"hole_{idx}_hcp"))
        yardage = _safe_int(form.get(f"hole_{idx}_yardage"))
        if par is None and handicap is None and yardage is None:
            continue
        hole_entry: dict[str, int | None] = {"hole_number": idx}
        if par is not None:
            hole_entry["par"] = par
            total_par += par
        if handicap is not None:
            hole_entry["handicap"] = handicap
        if yardage is not None:
            hole_entry["yardage"] = yardage
        holes.append(hole_entry)
    gender = (form.get("gender") or "male").lower()
    computed_par_total = par_total_input or (total_par if total_par else None)
    tee = {
        "tee_name": tee_name,
        "course_rating": course_rating,
        "slope_rating": slope_rating,
        "bogey_rating": None,
        "total_yards": total_yards,
        "total_meters": None,
        "number_of_holes": len(holes) or None,
        "par_total": computed_par_total,
        "front_course_rating": None,
        "back_course_rating": None,
        "front_slope_rating": None,
        "back_slope_rating": None,
        "front_bogey_rating": None,
        "back_bogey_rating": None,
    }
    tee_id = upsert_course_tee(settings.database_url, course_id, gender, tee)
    if holes:
        replace_course_tee_holes(settings.database_url, tee_id, holes)
    setup_message = f"Manual course \"{club_name} — {course_name}\" added."
    return RedirectResponse(
        url=f"/admin/setup/course?pin={pin}&setup_status={quote_plus(setup_message)}",
        status_code=303,
    )


@app.get("/tournaments", response_class=HTMLResponse)
async def tournaments_page(request: Request, status: str | None = None):
    tournaments = fetch_tournaments(settings.database_url)
    active_tournament_id = _get_active_tournament_id()
    active_tournament = (
        fetch_tournament_by_id(settings.database_url, active_tournament_id)
        if active_tournament_id
        else None
    )
    return templates.TemplateResponse(
        "tournaments.html",
        {
            "request": request,
            "tournaments": tournaments,
            "status": status,
            "active_tournament_id": active_tournament_id,
            "active_tournament": active_tournament,
        },
    )


@app.post("/tournaments")
async def create_tournament(
    name: str = Form(...),
    description: str | None = Form(None),
    status: str = Form("upcoming"),
):
    clean_name = name.strip()
    try:
        tournament_id = insert_tournament(
            settings.database_url,
            name=clean_name,
            description=(description or "").strip() or None,
            status=status or "upcoming",
        )
    except UniqueViolation:
        message = f"Tournament '{clean_name}' already exists."
        return RedirectResponse(
            url=f"/tournaments?status={quote_plus(message)}",
            status_code=303,
        )
    if status == "active" and tournament_id:
        _set_active_tournament_id(tournament_id)
    message = f"Tournament '{clean_name}' created."
    return RedirectResponse(
        url=f"/tournaments?status={quote_plus(message)}",
        status_code=303,
    )


@app.post("/tournaments/{tournament_id}/activate")
async def activate_tournament(tournament_id: int):
    tournament = fetch_tournament_by_id(settings.database_url, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    update_tournament_status(settings.database_url, tournament_id, "active")
    _set_active_tournament_id(tournament_id)
    message = f"Tournament '{tournament['name']}' is now active."
    return RedirectResponse(
        url=f"/tournaments?status={quote_plus(message)}",
        status_code=303,
    )


@app.post("/tournaments/{tournament_id}/status")
async def update_tournament_status_route(tournament_id: int, status: str = Form(...)):
    normalized = (status or "").strip().lower()
    if normalized not in TOURNAMENT_STATUSES:
        normalized = "upcoming"
    tournament = fetch_tournament_by_id(settings.database_url, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    update_tournament_status(settings.database_url, tournament_id, normalized)
    if normalized == "active":
        _set_active_tournament_id(tournament_id)
    elif _get_active_tournament_id() == tournament_id:
        _set_active_tournament_id(None)
    message = f"Tournament '{tournament['name']}' marked {normalized}."
    return RedirectResponse(
        url=f"/tournaments?status={quote_plus(message)}",
        status_code=303,
    )


@app.get("/admin/tournament_setup", response_class=HTMLResponse)
async def tournament_setup_page(
    request: Request,
    tournament_id: int | None = None,
    status: str | None = None,
    show_player_editor: bool | None = Query(False, alias="show_editor"),
):
    tournaments = fetch_tournaments(settings.database_url)
    active_id = tournament_id or _get_active_tournament_id()
    selected_tournament = (
        fetch_tournament_by_id(settings.database_url, active_id) if active_id else None
    )
    players = _players_for_tournament(active_id) if active_id else []
    all_players = fetch_players(settings.database_url)
    event_settings = fetch_event_settings(settings.database_url, active_id) if active_id else {}
    player_count = int(event_settings.get("player_count") or 0)
    division_count = int(event_settings.get("division_count") or 0)
    context = {
        "request": request,
        "tournaments": tournaments,
        "active_tournament_id": active_id,
        "players": players,
        "status": status,
        "player_count": player_count,
        "division_count": division_count,
        "selected_tournament": selected_tournament,
        "all_players": all_players,
        "show_player_editor": show_player_editor,
    }
    return templates.TemplateResponse("tournament_setup.html", context)


@app.post("/admin/tournament_setup/settings")
async def tournament_settings_update(
    tournament_id: int = Form(...),
    player_count: int = Form(0),
    division_count: int = Form(0),
):
    upsert_event_setting(settings.database_url, tournament_id, "player_count", str(player_count))
    upsert_event_setting(settings.database_url, tournament_id, "division_count", str(division_count))
    return RedirectResponse(
        url=f"/admin/tournament_setup?status={quote_plus('Settings saved.')}",
        status_code=303,
    )


@app.post("/admin/tournament_setup/player")
async def add_tournament_player(
    tournament_id: int = Form(...),
    division: str = Form("A"),
    first_name: str = Form(...),
    last_name: str = Form(...),
    handicaps_index: str = Form("0"),
    seed: int = Form(0),
):
    name = f"{first_name.strip()} {last_name.strip()}".strip()
    if not name:
        raise HTTPException(status_code=400, detail="Player name required")
    upsert_player(settings.database_url, None, name, division.upper(), int(float(handicaps_index)), seed or 0, tournament_id=tournament_id)
    return RedirectResponse(
        url=f"/admin/tournament_setup?status={quote_plus('Player saved.')}",
        status_code=303,
    )


@app.get("/admin/player_entry", response_class=HTMLResponse)
async def player_entry_page(request: Request, tournament_id: int | None = None):
    active_id = tournament_id or _get_active_tournament_id()
    players = _players_for_tournament(active_id) if active_id else []
    selected_tournament = (
        fetch_tournament_by_id(settings.database_url, active_id) if active_id else None
    )
    tournaments = fetch_tournaments(settings.database_url)
    status = request.query_params.get("status")
    return templates.TemplateResponse(
        "player_entry.html",
        {
            "request": request,
            "active_tournament_id": active_id,
            "players": players,
            "selected_tournament": selected_tournament,
            "tournaments": tournaments,
            "status": status,
        },
    )


@app.post("/admin/player_entry")
async def player_entry_submit(
    tournament_id: int = Form(...),
    division: str = Form("A"),
    first_name: str = Form(...),
    last_name: str = Form(...),
    handicaps_index: str = Form("0"),
    seed: int = Form(0),
):
    name = f"{first_name.strip()} {last_name.strip()}".strip()
    if not name:
        raise HTTPException(status_code=400, detail="Player name required")
    upsert_player(
        settings.database_url,
        None,
        name,
        division.upper(),
        int(float(handicaps_index)),
        seed or 0,
        tournament_id=tournament_id,
    )
    return RedirectResponse(
        url=f"/admin/player_entry?tournament_id={tournament_id}",
        status_code=303,
    )


@app.get("/player_entry", include_in_schema=False)
async def player_entry_redirect():
    return RedirectResponse(url="/admin/player_entry", status_code=303)


@app.post("/admin/tournament_setup/player/edit")
async def edit_tournament_player(
    player_id: int = Form(...),
    name: str = Form(...),
    division: str = Form("A"),
    handicaps_index: str = Form("0"),
    seed: int = Form(0),
    visible: str | None = Form(None),
    tournament_id: int = Form(...),
    focus_player: int | None = Form(None),
    source: str | None = Form(None),
):
    tournament_assignment = tournament_id if visible else None
    upsert_player(
        settings.database_url,
        player_id,
        name.strip(),
        division.upper(),
        int(float(handicaps_index)),
        seed,
        tournament_id=tournament_assignment,
    )
    redirect_target = "/admin/tournament_setup"
    if source == "player_entry":
        redirect_target = "/admin/player_entry"
    query_params = [f"tournament_id={tournament_id}"]
    if source == "player_entry" and focus_player:
        query_params.append(f"focus_player={focus_player}")
    query_params.append(f"status={quote_plus('Player updated.')}")
    return RedirectResponse(
        url=f"{redirect_target}?{'&'.join(query_params)}",
        status_code=303,
    )


@app.post("/admin/tournament_setup/player/delete")
async def delete_tournament_player(
    player_id: int = Form(...),
    tournament_id: int = Form(...),
    source: str | None = Form(None),
):
    delete_player(settings.database_url, player_id)
    redirect_target = "/admin/tournament_setup"
    if source == "player_entry":
        redirect_target = "/admin/player_entry"
    return RedirectResponse(
        url=f"{redirect_target}?tournament_id={tournament_id}&status={quote_plus('Player removed.')}",
        status_code=303,
    )


@app.post("/admin/active_tournament")
async def set_active_tournament(
    tournament_id: int | None = Form(None),
    redirect: str | None = Form("/"),
):
    _set_active_tournament_id(tournament_id)
    return RedirectResponse(
        url=redirect or "/",
        status_code=303,
    )


@app.get("/admin/match_setup", response_class=HTMLResponse)
async def match_setup_page(request: Request, status: str | None = None):
    active_id = _get_active_tournament_id()
    active_tournament = (
        fetch_tournament_by_id(settings.database_url, active_id) if active_id else None
    )
    players = _players_for_tournament(active_id) if active_id else []
    matches = (
        fetch_matches_by_tournament(settings.database_url, active_id) if active_id else []
    )
    tournament_settings = _load_tournament_settings(active_id) if active_id else {}
    course_catalog = fetch_course_catalog(settings.database_url)
    course_tee_map = _build_course_tee_map(course_catalog)
    selected_course_id = _safe_int(tournament_settings.get("course_id"))
    selected_course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
    context = {
        "request": request,
        "status": status,
        "active_tournament": active_tournament,
        "players": players,
        "matches": matches,
        "course_catalog": course_catalog,
        "course_tee_map": course_tee_map,
        "selected_course_id": selected_course_id,
        "selected_course_tee_id": selected_course_tee_id,
        "selected_hole_count": 18,
    "selected_start_hole": 1,
    }
    return templates.TemplateResponse("match_setup.html", context)


@app.get("/admin/scheduled_matches", response_class=HTMLResponse)
async def scheduled_matches_page(request: Request):
    active_id = _get_active_tournament_id()
    active_tournament = (
        fetch_tournament_by_id(settings.database_url, active_id) if active_id else None
    )
    matches = (
        fetch_matches_by_tournament(settings.database_url, active_id) if active_id else []
    )
    active_matches = [match for match in matches if not match.get("finalized")]
    finalized_matches = [match for match in matches if match.get("finalized")]
    context = {
        "request": request,
        "active_tournament": active_tournament,
        "matches": matches,
        "active_matches": active_matches,
        "finalized_matches": finalized_matches,
    }
    return templates.TemplateResponse("scheduled_matches.html", context)


@app.get("/admin/match_results", response_class=HTMLResponse)
async def match_results_page(request: Request):
    raw_results = fetch_all_match_results(settings.database_url)
    seen_keys: dict[str, dict] = {}
    for entry in reversed(raw_results):
        key = entry.get("match_key") or f"match-{entry['id']}"
        if key not in seen_keys:
            seen_keys[key] = entry
    results = list(seen_keys.values())
    for result in results:
        match_key = (result.get("match_key") or "")
        is_cd_row = match_key.endswith("-cd")
        if is_cd_row:
            result["pair_stats"] = _match_cleanup_cd_stats(result) or []
        else:
            result["pair_stats"] = _match_cleanup_ab_stats(result) or []
    return templates.TemplateResponse(
        "admin_match_results.html",
        {
            "request": request,
            "results": results,
        },
    )


@app.post("/admin/match_results/delete")
async def delete_match_result_row(match_result_id: int = Form(...)):
    match_result = fetch_match_result(settings.database_url, match_result_id)
    if not match_result:
        raise HTTPException(status_code=404, detail="Match result not found")
    delete_match_result(settings.database_url, match_result_id)
    _refresh_standings_cache(match_result.get("tournament_id"))
    return RedirectResponse(url="/admin/match_results", status_code=303)


@app.post("/admin/match_setup")
async def match_setup_submit(
    player_a_id: int = Form(...),
    player_b_id: int = Form(...),
    division: str = Form(""),
    match_key: str | None = Form(None),
    match_mode: str = Form("four"),
    player_c_id: int | None = Form(None),
    player_d_id: int | None = Form(None),
    tournament_id: int | None = Form(None),
    hole_count: int = Form(18),
    start_hole: int = Form(1),
    course_id: str | None = Form(None),
    course_tee_id: str | None = Form(None),
):
    tournament_id = tournament_id or _get_active_tournament_id()
    if not tournament_id:
        raise HTTPException(status_code=400, detail="Active tournament required")
    is_two_person = match_mode == "two"
    if not is_two_person and (not player_c_id or not player_d_id):
        raise HTTPException(status_code=400, detail="Select four players")
    player_ids = {player_a_id, player_b_id}
    if not is_two_person:
        player_ids.add(player_c_id)
        player_ids.add(player_d_id)
    required_players = 2 if is_two_person else 4
    if len(player_ids) < required_players:
        raise HTTPException(
            status_code=400,
            detail=f"Select {required_players} distinct players",
        )
    player_a = fetch_player_by_id(settings.database_url, player_a_id)
    player_b = fetch_player_by_id(settings.database_url, player_b_id)
    player_c = fetch_player_by_id(settings.database_url, player_c_id) if player_c_id else None
    player_d = fetch_player_by_id(settings.database_url, player_d_id) if player_d_id else None
    if not player_a or not player_b:
        raise HTTPException(status_code=404, detail="Player not found")
    if (player_c_id and not player_c) or (player_d_id and not player_d):
        raise HTTPException(status_code=404, detail="Player not found")
    division_input = (division or "").strip()
    if not division_input:
        division_input = player_a.get("division") or player_b.get("division") or "Open"
    division_display = division_input.title()
    division_key = division_input.upper()
    course_id_value = _safe_int(course_id)
    course_tee_id_value = _safe_int(course_tee_id)
    match_length_value = 9 if hole_count == 9 else 18
    start_hole_value = 1
    if match_length_value == 9 and start_hole == 10:
        start_hole_value = 10
    scheduled = fetch_matches_by_tournament(settings.database_url, tournament_id)
    if not match_key or not match_key.strip():
        existing = [entry for entry in scheduled if entry["division"].lower() == division_display.lower()]
        count = len(existing) + 1
        match_key = f"{division_key}-{count:02d}"
        insert_match(
            settings.database_url,
            tournament_id=tournament_id,
            match_key=match_key,
            division=division_display,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
            player_c_id=player_c_id,
            player_d_id=player_d_id,
            course_id=course_id_value,
            course_tee_id=course_tee_id_value,
            hole_count=match_length_value,
            start_hole=start_hole_value,
        )
    delete_match_results_by_key(settings.database_url, match_key)
    message = f"Match {match_key} saved."
    return RedirectResponse(
        url=f"/admin/match_setup?status={quote_plus(message)}",
        status_code=303,
    )


@app.post("/admin/match_setup/delete")
async def match_setup_delete(match_id: int = Form(...)):
    delete_match(settings.database_url, match_id)
    return RedirectResponse(
        url=f"/admin/match_setup?status={quote_plus('Match removed.')}",
        status_code=303,
    )

@app.get("/admin/tournament_setup2", response_class=HTMLResponse)
async def tournament_setup_two(request: Request, status: str | None = None):
    tournaments = fetch_tournaments(settings.database_url)
    active_id = _get_active_tournament_id()
    return templates.TemplateResponse(
        "tournament_setup2.html",
        {
            "request": request,
            "tournaments": tournaments,
            "active_tournament_id": active_id,
            "status": status,
        },
    )


@app.post("/admin/tournament_setup2/save")
async def tournament_setup_two_save(
    name: str = Form(...),
    description: str | None = Form(None),
    player_count: int = Form(0),
    division_count: int = Form(0),
    status: str = Form("upcoming"),
):
    clean_name = name.strip()
    try:
        tournament_id = insert_tournament(
            settings.database_url,
            name=clean_name,
            description=(description or "").strip() or None,
            status=status or "upcoming",
        )
    except UniqueViolation:
        message = f"Tournament '{clean_name}' already exists."
        return RedirectResponse(
            url=f"/admin/tournament_setup2?status={quote_plus(message)}",
            status_code=303,
        )
    if tournament_id:
        upsert_event_setting(settings.database_url, tournament_id, "player_count", str(player_count))
        upsert_event_setting(settings.database_url, tournament_id, "division_count", str(division_count))
    if status == "active" and tournament_id:
        _set_active_tournament_id(tournament_id)
    message = f"Tournament '{clean_name}' created."
    return RedirectResponse(
        url=f"/admin/tournament_setup2?status={quote_plus(message)}",
        status_code=303,
    )


@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request, pin: str = ""):
    authorized = pin == settings.scoring_pin
    return templates.TemplateResponse(
        "admin.html",
        _admin_context(request, pin, authorized),
    )


@app.post("/admin/cleanup-standings", response_class=HTMLResponse)
async def admin_cleanup_standings(request: Request, pin: str = Form("")):
    authorized = pin == settings.scoring_pin
    status_message = None
    if not authorized:
        status_message = "Invalid admin pin."
    else:
        tournament_id = _get_active_tournament_id()
        if not tournament_id:
            status_message = "No active tournament selected."
        else:
            deleted = _reset_tournament_match_history(tournament_id)
            status_message = (
                f"Cleared {deleted} match{'es' if deleted != 1 else ''} and rebuilt standings."
            )
    return templates.TemplateResponse(
        "admin.html",
        _admin_context(request, pin, authorized, status_message=status_message),
    )


@app.get("/admin/setup", response_class=HTMLResponse)
async def admin_setup_page(request: Request, pin: str = ""):
    active_tournament_id = _get_active_tournament_id()
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            active_section="parameters",
            tournament_id=active_tournament_id,
        ),
    )


@app.get("/admin/setup/{section}", response_class=HTMLResponse)
async def admin_setup_page_section(request: Request, section: str, pin: str = ""):
    active = _resolve_setup_section(section)
    active_tournament_id = _get_active_tournament_id()
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            active_section=active,
            tournament_id=active_tournament_id,
        ),
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
    active_tournament_id = _get_active_tournament_id()
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status=setup_status,
            active_section="players",
            tournament_id=active_tournament_id,
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
    active_tournament_id = _get_active_tournament_id()
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
        if active_tournament_id:
            upsert_event_setting(settings.database_url, active_tournament_id, key, value)
        else:
            upsert_setting(settings.database_url, key, value)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status="Tournament settings saved.",
            active_section="parameters",
            tournament_id=active_tournament_id,
        ),
    )


@app.post("/admin/setup/course", response_class=HTMLResponse)
async def admin_setup_course(
    request: Request,
    pin: str = Form(""),
    course_id: str = Form(""),
    course_tee_id: str = Form(""),
):
    active_tournament_id = _get_active_tournament_id()
    for key, value in {
        "course_id": course_id,
        "course_tee_id": course_tee_id,
    }.items():
        if active_tournament_id:
            upsert_event_setting(settings.database_url, active_tournament_id, key, value)
        else:
            upsert_setting(settings.database_url, key, value)
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status="Course selection saved.",
            active_section="course",
            tournament_id=active_tournament_id,
        ),
    )


@app.post("/admin/tournaments/{tournament_id}/cleanup", response_class=HTMLResponse)
async def cleanup_tournament_data(
    request: Request,
    tournament_id: int,
    pin: str = Form(""),
):
    tournament = fetch_tournament_by_id(settings.database_url, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    deleted = delete_match_results_by_tournament(settings.database_url, tournament_id)
    status = f"Cleared {deleted} match{'es' if deleted != 1 else ''} for {tournament['name']}."
    active_tournament_id = _get_active_tournament_id()
    return templates.TemplateResponse(
        "setup.html",
        _setup_context(
            request,
            pin,
            True,
            setup_status=status,
            active_section="parameters",
            tournament_id=active_tournament_id,
        ),
    )


def _coerce_datetime(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value




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

def _numeric_score(value: str | int | float | None) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


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
    match_length: int = 18,
    start_hole: int | None = None,
) -> dict:
    hole_map = {entry["hole_number"]: entry for entry in holes}
    max_recorded = max((entry.get("hole_number", 0) for entry in holes), default=0)
    effective_length = match_length if match_length in (9, 18) else (9 if max_recorded and max_recorded <= 9 else 18)
    hole_limit = effective_length
    is_nine_hole_match = hole_limit == 9
    sorted_course = sorted(course_holes, key=lambda hole: hole.get("hole_number", 0) or 0)
    active_course: list[dict] = []
    selected_start_hole = start_hole or 1
    if sorted_course:
        hole_numbers = [hole.get("hole_number", 0) or 0 for hole in sorted_course]
        normalized_start = selected_start_hole
        if normalized_start not in hole_numbers:
            normalized_start = hole_numbers[0]
        start_index = hole_numbers.index(normalized_start)
        idx = start_index
        while len(active_course) < hole_limit and sorted_course:
            active_course.append(sorted_course[idx])
            idx = (idx + 1) % len(sorted_course)
    if not active_course and course_holes:
        active_course = course_holes[:hole_limit]
    stroke_diff = handicap_a - handicap_b
    total_strokes = abs(stroke_diff)
    if is_nine_hole_match:
        total_strokes = total_strokes / 2
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
    def _team_total(*values: float | int | None) -> float | None:
        numeric = [value for value in values if value is not None]
        return sum(numeric) if numeric else None
    for hole in active_course:
        number = hole["hole_number"]
        entry = hole_map.get(number, {})
        player_a_score = entry.get("player_a_score")
        player_b_score = entry.get("player_b_score")
        player_c_score = entry.get("player_c_score")
        player_d_score = entry.get("player_d_score")
        gross_a = _numeric_score(player_a_score)
        gross_b = _numeric_score(player_b_score)
        strokes_a = strokes_for_a.get(number, 0.0)
        strokes_b = strokes_for_b.get(number, 0.0)
        stored_net_a = _numeric_score(entry.get("player_a_net"))
        stored_net_b = _numeric_score(entry.get("player_b_net"))
        net_a = stored_net_a if stored_net_a is not None else (gross_a - strokes_a if gross_a is not None else None)
        net_b = stored_net_b if stored_net_b is not None else (gross_b - strokes_b if gross_b is not None else None)
        result = "—"
        points_a_display = None
        points_b_display = None
        points_a_value = 0.0
        points_b_value = 0.0
        net_diff = None
        if net_a is not None and net_b is not None:
            diff = net_a - net_b
            net_diff = diff
            if diff < 0:
                result = "A"
                points_a_display = 1.0
                points_a_value = 1.0
            elif diff > 0:
                result = "B"
                points_b_display = 1.0
                points_b_value = 1.0
            else:
                result = "Halved"
                points_a_display = points_b_display = 0.5
                points_a_value = points_b_value = 0.5
        totals["points_a"] += points_a_value
        totals["points_b"] += points_b_value
        rows.append(
            {
                "hole_number": number,
                "par": hole["par"],
                "handicap": hole["handicap"],
                "gross_a": gross_a,
                "gross_b": gross_b,
                "player_a_score": player_a_score,
                "player_b_score": player_b_score,
                "player_c_score": player_c_score,
                "player_d_score": player_d_score,
                "strokes_a": strokes_a,
                "strokes_b": strokes_b,
                "net_a": net_a,
                "net_b": net_b,
                "net_diff": net_diff,
                "result": result,
                "points_a": points_a_display,
                "points_b": points_b_display,
            }
        )

    meta = {
        "stroke_owner": "A" if stroke_diff > 0 else "B" if stroke_diff < 0 else None,
        "stroke_count": total_strokes,
        "is_nine_hole": is_nine_hole_match,
        "match_length": hole_limit,
        "total_points_a": totals["points_a"],
        "total_points_b": totals["points_b"],
        "strokes_a": totals["strokes_a"],
        "strokes_b": totals["strokes_b"],
        "start_hole": selected_start_hole,
    }
    return {"rows": rows, "course": active_course, "meta": meta}


def _stroke_map_for_result(match_result: dict | None) -> dict[int, dict[str, float]]:
    if not match_result:
        return {}
    tournament_id = _tournament_id_for_result(match_result)
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(match_result.get("hole_count")) or 18
    start_hole = _safe_int(match_result.get("start_hole")) or 1
    handicap_a = match_result.get("player_a_handicap") or _player_handicap_by_name(match_result.get("player_a_name"))
    handicap_b = match_result.get("player_b_handicap") or _player_handicap_by_name(match_result.get("player_b_name"))
    computed = _scorecard_data_for_match(
        match_result,
        [],
        handicap_a,
        handicap_b,
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
        use_snapshot=False,
    )
    return {
        row["hole_number"]: {
            "A": row.get("strokes_a") or 0,
            "B": row.get("strokes_b") or 0,
        }
        for row in computed["rows"]
    }


def _match_player_metadata(match_result: dict | None) -> list[dict]:
    metadata: list[dict] = []
    if not match_result:
        return [{"name": "", "handicap": 0}] * 4
    tournament_id = _tournament_id_for_result(match_result)
    pairing = _matches_by_key(tournament_id).get(match_result.get("match_key", "") or "", {})
    for side in ("a", "b", "c", "d"):
        name = (match_result.get(f"player_{side}_name") or "").strip()
        if not name and pairing:
            name = (pairing.get(f"player_{side}_name") or pairing.get(f"player_{side}") or "").strip()
        if not name:
            player_id = match_result.get(f"player_{side}_id")
            name = _player_name_by_id(player_id)
        if not name:
            name = ""
        handicap = match_result.get(f"player_{side}_handicap")
        if handicap is None:
            handicap = _player_handicap_by_name(name)
        metadata.append({"name": name, "handicap": handicap or 0})
    return metadata


def _player_scorecard_entries(match_result: dict, holes: list[dict]) -> list[dict]:
    if not match_result:
        return []
    metadata = _match_player_metadata(match_result)
    stroke_map = _stroke_map_for_result(match_result)
    cd_key = f"{match_result.get('match_key')}-cd"
    cd_result = fetch_match_result_by_key(settings.database_url, cd_key) if match_result.get("match_key") else None
    cd_stroke_map = _stroke_map_for_result(cd_result)
    field_keys = ["player_a_score", "player_b_score", "player_c_score", "player_d_score"]
    side_names = ["A", "B", "C", "D"]
    entries: list[dict] = []
    for hole in holes:
        hole_number = hole["hole_number"]
        for idx, field in enumerate(field_keys):
            raw_score = hole.get(field)
            if raw_score is None:
                continue
            meta = metadata[idx]
            if not meta["name"]:
                continue
            team_index = 0 if idx % 2 == 0 else 1
            stroke_data = stroke_map if idx < 2 else cd_stroke_map
            stroke_key = "A" if idx % 2 == 0 else "B"
            stroke_value = stroke_data.get(hole_number, {}).get(stroke_key, 0)
            opponent_index = idx + 1 if idx % 2 == 0 else idx - 1
            opponent_name = metadata[opponent_index]["name"] if 0 <= opponent_index < len(metadata) else ""
            entries.append(
                {
                    "player_index": idx,
                    "player_side": side_names[idx],
                    "team_index": team_index,
                    "player_name": meta["name"],
                    "opponent_name": opponent_name,
                    "hole_number": hole_number,
                    "gross_score": raw_score,
                    "stroke_adjustment": stroke_value,
                    "net_score": raw_score - stroke_value if raw_score is not None and stroke_value is not None else raw_score,
                    "course_id": match_result.get("course_id"),
                    "course_tee_id": match_result.get("course_tee_id"),
                    "player_handicap": meta["handicap"],
                }
            )
    return entries


def _holes_for_cd(entries: list[dict]) -> list[dict]:
    return [
        {
            "hole_number": entry.get("hole_number"),
            "player_a_score": entry.get("player_c_score"),
            "player_b_score": entry.get("player_d_score"),
            "player_c_score": entry.get("player_c_score"),
            "player_d_score": entry.get("player_d_score"),
            "player_a_net": entry.get("player_c_net"),
            "player_b_net": entry.get("player_d_net"),
            "player_c_net": entry.get("player_c_net"),
            "player_d_net": entry.get("player_d_net"),
        }
        for entry in entries
        if entry.get("player_c_score") is not None or entry.get("player_d_score") is not None
    ]


def _build_player_cards(match_result: dict | None, holes: list[dict]) -> list[dict]:
    metadata = _match_player_metadata(match_result)
    side_names = ["A", "B", "C", "D"]
    cards: dict[int, dict] = {}
    for index, player in enumerate(metadata):
        if not player["name"]:
            continue
        cards[index] = {
            "player_index": index,
            "player_side": side_names[index] if index < len(side_names) else f"Player{index + 1}",
            "player_name": player["name"],
            "player_handicap": player["handicap"],
            "opponent_name": "",
            "course_id": match_result.get("course_id") if match_result else None,
            "course_tee_id": match_result.get("course_tee_id") if match_result else None,
            "total_gross": 0.0,
            "total_net": 0.0,
            "total_strokes": 0.0,
            "holes": [],
        }
    entries = _player_scorecard_entries(match_result if match_result else {}, holes)
    for entry in entries:
        card = cards.get(entry["player_index"])
        if not card:
            continue
        gross = entry["gross_score"] or 0
        strokes = entry["stroke_adjustment"] or 0
        net_score = gross - strokes
        card["opponent_name"] = entry.get("opponent_name", card["opponent_name"] or "")
        card["total_gross"] += gross
        card["total_net"] += net_score
        card["total_strokes"] += strokes
        card["holes"].append(
            {
                "hole_number": entry["hole_number"],
                "gross_score": gross,
                "stroke_adjustment": strokes,
                "net_score": net_score,
            }
        )
    for card in cards.values():
        card["holes"].sort(key=lambda row: row["hole_number"])
        card["hole_count"] = len(card["holes"])
    return [cards[idx] for idx in sorted(cards)]


def _migrate_player_scorecards_from_legacy() -> None:
    results = fetch_all_match_results(settings.database_url)
    for result in results:
        match_id = result["id"]
        if fetch_player_hole_scores(settings.database_url, match_id):
            continue
        legacy_holes = fetch_legacy_hole_scores(settings.database_url, match_id)
        if not legacy_holes:
            continue
        entries = _player_scorecard_entries(result, legacy_holes)
        if entries:
            insert_player_hole_scores(
                settings.database_url,
                match_id,
                result.get("match_key") or "",
                entries,
            )

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



def _scorecard_context(match_key: str | None, tournament_id: int | None = None) -> dict:
    matches = _load_pairings()
    if not matches:
        if not match_key:
            return {"matches": [], "match_statuses": [], "active_matches": [], "scorecard": None}

    selected = next((item for item in matches if item.match_id == match_key), None) or matches[0]
    match_statuses: list[dict] = []
    selected_status: dict[str, int | str | None] | None = None
    for entry in matches:
        status_info = _match_status_info(entry.match_id)
        team_a_label = _team_label(entry, "A")
        team_b_label = _team_label(entry, "B")
        match_statuses.append(
            {
                "match_key": entry.match_id,
                "division": entry.division,
                "player_a": team_a_label,
                "player_b": team_b_label,
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

    team_a_label = _team_label(selected, "A")
    team_b_label = _team_label(selected, "B")
    match_result = _ensure_match_result_for_pairing(selected)
    if not match_result:
        match_result = fetch_match_result_by_key(settings.database_url, selected.match_id)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, selected.match_id)
    match_record = None
    match_record = None
    resolved_tournament_id = tournament_id or _tournament_id_for_result(match_result)
    if not resolved_tournament_id and manual_match_record:
        resolved_tournament_id = manual_match_record.get("tournament_id")
    tournament_settings = _load_tournament_settings(resolved_tournament_id)
    hole_records = (
        fetch_hole_scores(settings.database_url, match_result["id"])
        if match_result
        else []
    )
    player_a_name = (
        match_result["player_a_name"]
        if match_result and match_result.get("player_a_name")
        else team_a_label
    )
    player_b_name = (
        match_result["player_b_name"]
        if match_result and match_result.get("player_b_name")
        else team_b_label
    )
    player_c_name = selected.player_c or ""
    player_d_name = selected.player_d or ""
    recorded_handicap_a = match_result.get("player_a_handicap") if match_result else None
    recorded_handicap_b = match_result.get("player_b_handicap") if match_result else None
    if recorded_handicap_a is not None:
        handicap_a = recorded_handicap_a
    else:
        player_a_info = fetch_player_by_name(settings.database_url, player_a_name) or {}
        handicap_a = player_a_info.get("handicap", 0)
    if recorded_handicap_b is not None:
        handicap_b = recorded_handicap_b
    else:
        player_b_info = fetch_player_by_name(settings.database_url, player_b_name) or {}
        handicap_b = player_b_info.get("handicap", 0)
    player_c_info = {}
    player_d_info = {}
    if player_c_name:
        player_c_info = fetch_player_by_name(settings.database_url, player_c_name) or {}
    if player_d_name:
        player_d_info = fetch_player_by_name(settings.database_url, player_d_name) or {}
    handicap_c = player_c_info.get("handicap", 0)
    handicap_d = player_d_info.get("handicap", 0)
    match_length = selected.hole_count or 18
    start_hole = selected.start_hole or 1
    computed = _scorecard_data_for_match(
        match_result if match_result else {},
        hole_records,
        handicap_a,
        handicap_b,
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
    )
    computed_meta = computed.setdefault("meta", {})
    if match_result and match_result.get("finalized"):
        player_a_total = match_result.get("player_a_total")
        player_b_total = match_result.get("player_b_total")
        if player_a_total is not None:
            computed_meta["total_points_a"] = player_a_total
        if player_b_total is not None:
            computed_meta["total_points_b"] = player_b_total
    player_a_display = (selected.player_a or player_a_name or "Player A").strip()
    player_b_display = (selected.player_b or player_b_name or "Player B").strip()
    player_c_display = (selected.player_c or player_c_name or "").strip()
    player_d_display = (selected.player_d or player_d_name or "").strip()
    holes_for_cd = [
        {
            "hole_number": entry.get("hole_number"),
            "player_a_score": entry.get("player_c_score"),
            "player_b_score": entry.get("player_d_score"),
            "player_c_score": entry.get("player_c_score"),
            "player_d_score": entry.get("player_d_score"),
        }
        for entry in hole_records
    ]
    computed_cd = _scorecard_data_for_match(
        match_result if match_result else {},
        holes_for_cd,
        handicap_c,
        handicap_d,
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
        use_snapshot=False,
    )
    def _sum_value(rows: list[dict], key: str) -> float | None:
        total = 0.0
        seen = False
        for row in rows:
            value = row.get(key)
            if isinstance(value, (int, float)):
                total += value
                seen = True
        return total if seen else None

    def _build_pair_card(name_a: str, name_b: str, handicap_value_a: int, handicap_value_b: int, data: dict) -> dict:
        rows = data.get("rows", [])
        meta = data.get("meta", {})
        strokes_present_a = any(entry.get("strokes_a", 0) for entry in rows)
        strokes_present_b = any(entry.get("strokes_b", 0) for entry in rows)
        return {
            "players": [
                {"name": name_a, "handicap": handicap_value_a},
                {"name": name_b, "handicap": handicap_value_b},
            ],
            "rows": rows,
            "meta": meta,
            "totals": {
                "net": [
                    _sum_value(rows, "net_a"),
                    _sum_value(rows, "net_b"),
                ],
                "points": [
                    meta.get("total_points_a"),
                    meta.get("total_points_b"),
                ],
            },
            "strokes_present": [strokes_present_a, strokes_present_b],
        }
    pair_cards = [
        _build_pair_card(player_a_display, player_b_display, handicap_a, handicap_b, computed),
    ]
    if player_c_display and player_d_display:
        pair_cards.append(
            _build_pair_card(player_c_display, player_d_display, handicap_c, handicap_d, computed_cd),
        )
    course_info = _course_display_info(match_result if match_result else {}, tournament_settings)
    match_name = match_display(selected)
    total_holes = match_length
    scorecard = {
        "match": {
            "id": match_result["id"] if match_result else None,
            "match_key": selected.match_id,
            "match_code": match_result.get("match_code") if match_result else "",
            "match_name": match_name,
            "player_a_name": player_a_display,
            "player_b_name": player_b_display,
            "player_c_name": player_c_display,
            "player_d_name": player_d_display,
            "player_a_individual_name": player_a_display,
            "player_c_individual_name": player_c_display,
            "player_b_individual_name": player_b_display,
            "player_d_individual_name": player_d_display,
            "division": selected.division,
            "status": selected_status["status"],
            "status_label": MATCH_STATUS_LABELS.get(selected_status["status"], selected_status["status"]),
            "holes_recorded": selected_status["holes"],
            "total_holes": total_holes,
            "team_label_a": team_a_label,
            "team_label_b": team_b_label,
        },
        "holes": computed["rows"],
        "meta": computed["meta"],
        "course": {
            "club_name": course_info.get("club_name"),
            "course_name": course_info.get("course_name"),
            "tee_name": course_info.get("tee_name"),
            "total_yards": course_info.get("total_yards"),
            "course_rating": course_info.get("course_rating"),
            "slope_rating": course_info.get("slope_rating"),
        },
        "player_a_handicap": handicap_a,
        "player_b_handicap": handicap_b,
        "pair_cards": pair_cards,
        "players": [
            {"name": player_a_display, "role": "A", "team_index": 0, "handicap": handicap_a},
            {"name": player_b_display, "role": "B", "team_index": 1, "handicap": handicap_b},
            *(
                [
                    {"name": player_c_display, "role": "A", "team_index": 0, "handicap": handicap_c},
                    {"name": player_d_display, "role": "B", "team_index": 1, "handicap": handicap_d},
                ]
                if player_c_display and player_d_display
                else []
            ),
        ],
    }
    computed_meta = scorecard.setdefault("meta", {})
    base_total_a = computed_meta.get("total_points_a") or 0.0
    base_total_b = computed_meta.get("total_points_b") or 0.0
    allow_bonus = _bonus_allowed(len(hole_records), match_length)
    computed_bonus_a, computed_bonus_b = compute_bonus_points(
        base_total_a, base_total_b, allow_bonus=allow_bonus
    )
    bonus_record = None
    if match_result:
        bonus_record = fetch_match_bonus(settings.database_url, match_result["id"])
    bonus_a = bonus_record["player_a_bonus"] if bonus_record else computed_bonus_a
    bonus_b = bonus_record["player_b_bonus"] if bonus_record else computed_bonus_b
    computed_meta["bonus_points_a"] = bonus_a
    computed_meta["bonus_points_b"] = bonus_b
    computed_meta["total_points_base_a"] = base_total_a
    computed_meta["total_points_base_b"] = base_total_b
    computed_meta["total_points_with_bonus_a"] = base_total_a + bonus_a
    computed_meta["total_points_with_bonus_b"] = base_total_b + bonus_b
    per_player_bonus = [bonus_a, bonus_b]
    while len(per_player_bonus) < len(scorecard.get("players", [])):
        per_player_bonus.append(0.0)
    scorecard["bonus_points"] = {
        "player_a": bonus_a,
        "player_b": bonus_b,
        "per_player": per_player_bonus,
    }
    final_status_pair1 = "—"
    final_status_pair2 = "—"
    for entry in reversed(computed["rows"]):
        if final_status_pair1 == "—" and entry.get("status_active_pair1"):
            final_status_pair1 = entry.get("status_label_pair1") or final_status_pair1
        if final_status_pair2 == "—" and entry.get("status_active_pair2"):
            final_status_pair2 = entry.get("status_label_pair2") or final_status_pair2
        if final_status_pair1 != "—" and final_status_pair2 != "—":
            break
    scorecard["final_status_pair1"] = final_status_pair1
    scorecard["final_status_pair2"] = final_status_pair2
    scorecard["match"]["tournament_id"] = resolved_tournament_id
    scorecard["point_chip_a"] = _adjust_display_points(scorecard["meta"]["total_points_a"])
    scorecard["point_chip_b"] = _adjust_display_points(scorecard["meta"]["total_points_b"])
    scorecard["player_cards"] = _build_player_cards(match_result, hole_records)
    match_statuses.sort(
        key=lambda entry: (STATUS_PRIORITY.get(entry["status"], 3), entry["display"])
    )
    active_matches = [entry for entry in match_statuses if entry["status"] != "completed"]
    finalized_matches = [entry for entry in match_statuses if entry["status"] == "completed"]
    return {
        "matches": matches,
        "match_statuses": match_statuses,
        "active_matches": active_matches,
        "finalized_matches": finalized_matches,
        "scorecard": scorecard,
    }


@app.get("/scorecard_studio", response_class=HTMLResponse)
async def scorecard_studio(request: Request):
    context = _scorecard_context(None)
    matches = context.get("matches", [])
    statuses = context.get("match_statuses", [])
    studio_matches: list[dict] = []
    for entry in matches[:2]:
        serialized = _serialized_scorecard_by_key(entry.match_id) or {}
        scorecard = dict(serialized)
        scorecard.setdefault("players", [])
        scorecard.setdefault("holes", [])
        scorecard.setdefault("meta", {})
        scorecard["meta"].setdefault("start_hole", entry.start_hole or 1)
        scorecard["meta"].setdefault("is_nine_hole", (entry.hole_count or 18) == 9)
        scorecard.setdefault("course", {})
        scorecard.setdefault("match", {})
        scorecard["match"].setdefault("total_holes", entry.hole_count or 18)
        scorecard["match"].setdefault("match_key", entry.match_id)
        status_entry = next((item for item in statuses if item["match_key"] == entry.match_id), {})
        studio_matches.append(
            {
                "match_id": entry.match_id,
                "display": match_display(entry),
                "status": status_entry.get("status") or "not_started",
                "status_label": status_entry.get("status_label") or "Not started",
                "holes_recorded": status_entry.get("holes") or 0,
                "scorecard": scorecard,
            }
        )
    hero_scorecard = studio_matches[0]["scorecard"] if studio_matches else {}
    hero_holes = [
        {"number": hole.get("hole_number"), "par": hole.get("par"), "hcp": hole.get("handicap")}
        for hole in hero_scorecard.get("holes", [])[:9]
    ]
    hero = {
        "title": "Scorecard Studio",
        "subtitle": "Live coverage for the latest pairings",
        "meta": f"{len(studio_matches)} live match{'es' if len(studio_matches) != 1 else ''}",
        "holes": hero_holes,
        "course": hero_scorecard.get("course", {}) or {},
    }
    return templates.TemplateResponse(
        "new_card.html",
        {
            "request": request,
            "hero": hero,
            "studio_matches": studio_matches,
        },
    )


@app.get("/scorecard_data", response_class=HTMLResponse)
async def scorecard_data(request: Request, match_key: str | None = None):
    context = _scorecard_context(match_key)
    scorecard = context.get("scorecard") or {}
    matches = context.get("matches", [])
    match_statuses = context.get("match_statuses", [])
    match_status_map = {entry["match_key"]: entry for entry in match_statuses}
    return templates.TemplateResponse(
        "scorecard_data.html",
        {
            "request": request,
            "scorecard": scorecard,
            "matches": matches,
            "match_statuses": match_statuses,
            "match_status_map": match_status_map,
            "selected_match_key": match_key,
            "active_match_tiles": context.get("active_matches", []),
            "finalized_match_tiles": context.get("finalized_matches", []),
            "active_match_count": len(context.get("active_matches", [])),
            "finalized_match_count": len(context.get("finalized_matches", [])),
        },
    )


@app.get("/scorecard_data_source", response_class=HTMLResponse)
async def scorecard_data_source(request: Request, match_key: str | None = None):
    context = _scorecard_context(match_key)
    scorecard = context.get("scorecard") or {}
    return templates.TemplateResponse(
        "scorecard_data_source.html",
        {
            "request": request,
            "scorecard": scorecard,
        },
    )


def _format_winner_label(result: str | None, player_names: list[str]) -> str:
    if result == "A":
        return player_names[0]
    if result == "B":
        return player_names[1]
    if result == "Halved":
        return "AS"
    if result in (None, "—"):
        return "—"
    return result


def _serialize_scorecard_for_studio(scorecard: dict) -> dict:
    match_info = scorecard["match"]
    team_label_a = match_info.get("team_label_a") or match_info.get("player_a_name") or "Player A Team"
    team_label_b = match_info.get("team_label_b") or match_info.get("player_b_name") or "Player B Team"
    player_entries = scorecard.get("players") or []
    player_names: list[str] = []
    player_team_indexes: list[int] = []
    for idx, player in enumerate(player_entries):
        name = (player.get("name") or "").strip()
        if not name:
            continue
        player_names.append(name)
        team_index = player.get("team_index")
        if team_index is None:
            team_index = 0 if len(player_team_indexes) % 2 == 0 else 1
        player_team_indexes.append(team_index)
    if len(player_names) < 2:
        player_names = [team_label_a, team_label_b]
        player_team_indexes = [0, 1]
    player_count = len(player_names)
    total_rows = scorecard.get("holes", [])
    hole_entries: list[dict] = []
    gross_totals = [0.0, 0.0]
    net_totals = [0.0, 0.0]

    def _score_value(value: str | int | float | None) -> bool:
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            return value.isnumeric()
        return False

    def _accumulate(team_index: int, gross: str | int | float | None, net: str | int | float | None) -> None:
        if isinstance(gross, (int, float)):
            gross_totals[team_index] += gross
        elif isinstance(gross, str) and gross.isnumeric():
            gross_totals[team_index] += float(gross)
        if isinstance(net, (int, float)):
            net_totals[team_index] += net
        elif isinstance(net, str) and net.isnumeric():
            net_totals[team_index] += float(net)

    def _status_label(value: int) -> str:
        if value == 0:
            return "A/S"
        return f"{abs(value)} {'Up' if value > 0 else 'Down'}"

    def _row_has_scores(row: dict) -> bool:
        return any(
            row.get(key) is not None
            for key in (
                "player_a_score",
                "player_b_score",
                "player_c_score",
                "player_d_score",
            )
        )

    def _player_score_for_index(scores: list[dict], index: int) -> float | None:
        raw = scores[index]
        value = _numeric_score(raw.get("net"))
        if value is None:
            value = _numeric_score(raw.get("gross"))
        return value

    def _player_match_points(first_idx: int, second_idx: int, scores: list[dict]) -> tuple[float | None, float | None]:
        first_value = _player_score_for_index(scores, first_idx)
        second_value = _player_score_for_index(scores, second_idx)
        if first_value is None or second_value is None:
            return None, None
        if first_value < second_value:
            return 1.0, 0.0
        if first_value > second_value:
            return 0.0, 1.0
        return 0.5, 0.5

    pair_indexes: list[tuple[int, int]] = []
    for idx in range(0, player_count, 2):
        if idx + 1 < player_count:
            pair_indexes.append((idx, idx + 1))
    pair_statuses = [0] * len(pair_indexes)
    for row in total_rows:
        team_gross_a = row.get("gross_a")
        team_gross_b = row.get("gross_b")
        player_a_score = row.get("player_a_score", team_gross_a)
        player_b_score = row.get("player_b_score", team_gross_b)
        player_c_score = row.get("player_c_score")
        player_d_score = row.get("player_d_score")
        net_a = row.get("net_a")
        net_b = row.get("net_b")
        _accumulate(0, team_gross_a, net_a)
        _accumulate(1, team_gross_b, net_b)
        score_fields = [
            {"gross": player_a_score, "net": net_a},
            {"gross": player_b_score, "net": net_b},
            {"gross": player_c_score, "net": net_a},
            {"gross": player_d_score, "net": net_b},
        ]
        player_scores: list[dict] = []
        for idx, name in enumerate(player_names):
            source = score_fields[idx] if idx < len(score_fields) else {"gross": None, "net": None}
            player_scores.append(
                {
                    "name": name,
                    "gross": source["gross"],
                    "net": source["net"],
                }
            )
        pair_status_labels: list[str] = []
        pair_status_diffs: list[int] = []
        pair_status_active: list[bool] = []
        for idx, (first, second) in enumerate(pair_indexes):
            first_value = _numeric_score(player_scores[first].get("net"))
            second_value = _numeric_score(player_scores[second].get("net"))
            if first_value is None:
                first_value = _numeric_score(player_scores[first].get("gross"))
            if second_value is None:
                second_value = _numeric_score(player_scores[second].get("gross"))
            active = first_value is not None and second_value is not None
            if active:
                delta = first_value - second_value
                if delta < 0:
                    pair_statuses[idx] += 1
                elif delta > 0:
                    pair_statuses[idx] -= 1
            pair_status_labels.append(_status_label(pair_statuses[idx]))
            pair_status_diffs.append(pair_statuses[idx])
            pair_status_active.append(active)
        player_match_points: list[float | None] = []
        for first, second in pair_indexes:
            first_point, second_point = _player_match_points(first, second, player_scores)
            player_match_points.extend([first_point, second_point])

        strokes_a = _numeric_score(row.get("strokes_a")) or 0.0
        strokes_b = _numeric_score(row.get("strokes_b")) or 0.0
        stroke_owner = "A" if strokes_a > strokes_b else "B" if strokes_b > strokes_a else None
        stroke_count = strokes_a if stroke_owner == "A" else strokes_b if stroke_owner == "B" else None
        hole_entries.append(
            {
                "hole_number": row.get("hole_number"),
                "par": row.get("par"),
                "handicap": row.get("handicap"),
                "player_scores": player_scores,
                "winner": _format_winner_label(row.get("result"), player_names),
                "stroke_owner": stroke_owner,
                "stroke_count": stroke_count,
                "points_a": row.get("points_a"),
                "points_b": row.get("points_b"),
                "player_points": player_match_points,
                "status_label_pair1": pair_status_labels[0] if len(pair_status_labels) > 0 else None,
                "status_label_pair2": pair_status_labels[1] if len(pair_status_labels) > 1 else None,
                "status_diff_pair1": pair_status_diffs[0] if len(pair_status_diffs) > 0 else None,
                "status_diff_pair2": pair_status_diffs[1] if len(pair_status_diffs) > 1 else None,
                "status_active_pair1": pair_status_active[0] if len(pair_status_active) > 0 else None,
                "status_active_pair2": pair_status_active[1] if len(pair_status_active) > 1 else None,
            }
        )

    has_scores = any(
        any(_score_value(score.get("gross")) for score in hole.get("player_scores", []))
        for hole in hole_entries
    )

    player_point_totals = [0.0] * player_count
    for hole in hole_entries:
        for idx, value in enumerate(hole.get("player_points", []) if hole.get("player_points") else []):
            if isinstance(value, (int, float)) and idx < player_count:
                player_point_totals[idx] += value

    ordered_players = list(zip(player_names, player_team_indexes))
    players_data = []
    for name, team_idx in ordered_players:
        players_data.append(
            {
                "name": name,
                "course_handicap": scorecard.get("player_a_handicap", 0)
                if team_idx == 0
                else scorecard.get("player_b_handicap", 0),
                "gross_total": gross_totals[team_idx],
                "net_total": net_totals[team_idx],
                "role": "A" if team_idx == 0 else "B",
                "team_index": team_idx,
            }
        )

    matchup_label = f"{team_label_a} vs {team_label_b}"

    return {
        "match_id": match_info.get("id"),
        "match_key": match_info["match_key"],
        "match_code": match_info.get("match_code"),
        "label": match_info["match_name"],
        "matchup": matchup_label,
        "status": match_info["status"],
        "status_label": match_info["status_label"],
        "summary": f"Net match play - {match_info['total_holes']} holes - Auto net",
        "players": players_data,
        "holes": hole_entries,
        "meta": scorecard.get("meta", {}),
        "bonus_points": {
            "player_a": scorecard.get("bonus_points", {}).get("player_a", 0),
            "player_b": scorecard.get("bonus_points", {}).get("player_b", 0),
            "per_player": scorecard.get("bonus_points", {}).get("per_player", []),
        },
        "player_point_totals": player_point_totals,
        "phase": match_info["status"],
        "course": {
            "club_name": match_info.get("division"),  # placeholder if no club
            "course_name": scorecard.get("course", {}).get("course_name"),
            "tee_name": scorecard.get("course", {}).get("tee_name"),
            "total_yards": scorecard.get("course", {}).get("total_yards"),
            "course_rating": scorecard.get("course", {}).get("course_rating"),
            "slope_rating": scorecard.get("course", {}).get("slope_rating"),
        },
        "has_scores": has_scores,
    }


def _serialized_scorecard_by_key(match_key: str) -> dict | None:
    context = _scorecard_context(match_key)
    scorecard = context.get("scorecard")
    if not scorecard:
        return None
    return _serialize_scorecard_for_studio(scorecard)


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
    course_holes = _course_holes_for_match(result)
    tournament_id = _tournament_id_for_result(result)
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(result.get("hole_count")) or 18
    match_start_hole = _safe_int(result.get("start_hole")) or 1
    scorecard = _scorecard_data_for_match(
        result,
        holes,
        handicap_a,
        handicap_b,
        tournament_settings,
        match_length=match_length,
        start_hole=match_start_hole,
        use_snapshot=False,
    )
    total_points_a = scorecard["meta"]["total_points_a"]
    total_points_b = scorecard["meta"]["total_points_b"]
    player_a_total = total_points_a
    player_b_total = total_points_b
    if result.get("finalized"):
        stored_a = result.get("player_a_total")
        stored_b = result.get("player_b_total")
        if stored_a is not None:
            player_a_total = stored_a
        if stored_b is not None:
            player_b_total = stored_b
    if player_a_total > player_b_total:
        points_winner_label = result["player_a_name"]
    elif player_b_total > player_a_total:
        points_winner_label = result["player_b_name"]
    else:
        points_winner_label = "Draw"
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
            "match_points": {
                "a": player_a_total,
                "b": player_b_total,
                "winner_label": points_winner_label,
            },
        },
    )


@app.post("/matches/{match_id}/finalize")
async def finalize_match(
    match_id: int,
    redirect: str | None = Form(None),
    bonus_mode: str = Form("auto"),
):
    del bonus_mode
    result, match_record = _resolve_match_result_context(match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    match_key = result.get("match_key") or (match_record and match_record.get("match_key"))
    if not match_key:
        raise HTTPException(status_code=400, detail="Match key is required to finalize.")
    target_ids = {result["id"]}
    if match_key:
        target_ids.update(fetch_match_result_ids_by_key(settings.database_url, match_key))

    tournament_id = _tournament_id_for_result(result)
    if not tournament_id:
        raise HTTPException(status_code=400, detail="Tournament ID is required to finalize.")
    finalized_any = False
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(result.get("hole_count")) or 18
    match_start_hole = _safe_int(result.get("start_hole")) or 1
    course_info = _course_display_info(result, tournament_settings)
    match_entry = _matches_by_key(tournament_id).get(match_key or "")
    player_c_name = (match_entry.get("player_c_name") or "").strip() if match_entry else ""
    player_d_name = (match_entry.get("player_d_name") or "").strip() if match_entry else ""
    player_a_id = match_entry.get("player_a_id") if match_entry else None
    player_b_id = match_entry.get("player_b_id") if match_entry else None
    player_c_id = match_entry.get("player_c_id") if match_entry else None
    player_d_id = match_entry.get("player_d_id") if match_entry else None
    if not player_a_id:
        player_a_id = _player_id_by_name(result.get("player_a_name"))
    if not player_b_id:
        player_b_id = _player_id_by_name(result.get("player_b_name"))
    target_records: list[dict] = []
    scorecard_source_id: int | None = None
    scorecard_source_result: dict | None = None
    scorecard_source_holes: list[dict] | None = None
    for target_id in sorted(target_ids):
        target = fetch_match_result(settings.database_url, target_id)
        if not target:
            continue
        if target.get("finalized"):
            finalized_any = True
            continue
        holes = fetch_hole_scores(settings.database_url, target_id)
        if holes and scorecard_source_holes is None:
            scorecard_source_holes = holes
            scorecard_source_id = target_id
            scorecard_source_result = target
        scorecard_data, total_points_a, total_points_b = _match_scorecard_summary(
            target,
            holes,
            tournament_settings,
            match_length,
            match_start_hole,
            player_a_handicap=target.get("player_a_handicap") or 0,
            player_b_handicap=target.get("player_b_handicap") or 0,
        )
        outcome = score_outcome(total_points_a, total_points_b)
        bonus_override = fetch_match_bonus(settings.database_url, target_id)
        adjusted = _apply_bonus_constraints(
            outcome,
            total_points_a,
            total_points_b,
            match_length,
            match_length,
            bonus_override=bonus_override,
        )
        update_match_result_scores(
            settings.database_url,
            target_id,
            player_a_points=total_points_a,
            player_b_points=total_points_b,
            player_a_bonus=adjusted["player_a_bonus"],
            player_b_bonus=adjusted["player_b_bonus"],
            player_a_total=adjusted["player_a_total"],
            player_b_total=adjusted["player_b_total"],
            player_a_id=player_a_id or target.get("player_a_id"),
            player_b_id=player_b_id or target.get("player_b_id"),
            player_c_id=player_c_id,
            player_d_id=player_d_id,
            winner=outcome["winner"],
        )
        upsert_match_bonus(
            settings.database_url,
            target_id,
            player_a_bonus=adjusted["player_a_bonus"],
            player_b_bonus=adjusted["player_b_bonus"],
        )
        finalize_match_result(
            settings.database_url,
            target_id,
            course_snapshot=course_info or {},
            scorecard_snapshot=scorecard_data,
        )
        finalized_any = True
        target_records.append({"id": target_id, "result": target})

    if not finalized_any:
        raise HTTPException(status_code=400, detail="No matches were finalized.")

    if (
        scorecard_source_holes
        and scorecard_source_result
        and player_c_name
        and player_d_name
    ):
        holes_for_cd = [
            {
                "hole_number": entry.get("hole_number"),
                "player_a_score": entry.get("player_c_score"),
                "player_b_score": entry.get("player_d_score"),
                "player_c_score": entry.get("player_c_score"),
                "player_d_score": entry.get("player_d_score"),
            }
            for entry in scorecard_source_holes
        ]
        has_cd_scores = any(
            entry.get("player_c_score") is not None or entry.get("player_d_score") is not None
            for entry in scorecard_source_holes
        )
        if holes_for_cd and has_cd_scores:
            handicap_c = _player_handicap_by_name(player_c_name)
            handicap_d = _player_handicap_by_name(player_d_name)
            scorecard_cd, player_c_total, player_d_total = _match_scorecard_summary(
                scorecard_source_result,
                holes_for_cd,
                tournament_settings,
                match_length,
                match_start_hole,
                player_a_handicap=handicap_c,
                player_b_handicap=handicap_d,
            )
            outcome_cd = score_outcome(player_c_total, player_d_total)
            cd_key = f"{match_key}-cd" if match_key else ""
            pair_record = _find_result_by_players(
                target_records,
                player_c_name,
                player_d_name,
            )
            pair_result_id: int | None = pair_record["id"] if pair_record else None
            course_snapshot = course_info or {}
            bonus_override_cd = (
                fetch_match_bonus(settings.database_url, pair_result_id)
                if pair_result_id
                else {"player_a_bonus": 0.0, "player_b_bonus": 0.0}
            )
            adjusted_cd = _apply_bonus_constraints(
                outcome_cd,
                player_c_total,
                player_d_total,
                match_length,
                match_length,
                bonus_override=bonus_override_cd,
            )
            if pair_result_id:
                update_match_result_scores(
                    settings.database_url,
                    pair_result_id,
                    player_a_points=player_c_total,
                    player_b_points=player_d_total,
                    player_a_bonus=adjusted_cd["player_a_bonus"],
                    player_b_bonus=adjusted_cd["player_b_bonus"],
                    player_a_total=adjusted_cd["player_a_total"],
                    player_b_total=adjusted_cd["player_b_total"],
                    player_a_id=player_c_id,
                    player_b_id=player_d_id,
                    winner=adjusted_cd["winner"],
                )
                upsert_match_bonus(
                    settings.database_url,
                    pair_result_id,
                    player_a_bonus=adjusted_cd["player_a_bonus"],
                    player_b_bonus=adjusted_cd["player_b_bonus"],
                )
                finalize_match_result(
                    settings.database_url,
                    pair_result_id,
                    course_snapshot=course_snapshot,
                    scorecard_snapshot=scorecard_cd,
                )
                existing_cd_holes = fetch_hole_scores(settings.database_url, pair_result_id)
                if not existing_cd_holes and holes_for_cd:
                    insert_hole_scores(settings.database_url, pair_result_id, holes_for_cd)
                    cd_result = fetch_match_result(settings.database_url, pair_result_id)
                    if cd_result:
                        cd_entries = _player_scorecard_entries(cd_result, holes_for_cd)
                        insert_player_hole_scores(
                            settings.database_url,
                            pair_result_id,
                            cd_result.get("match_key") or cd_key,
                            cd_entries,
                        )
                finalized_any = True
            else:
                inserted_id = insert_match_result(
                    settings.database_url,
                    match_name=f"{player_c_name} vs {player_d_name}",
                    player_a=player_c_name,
                    player_b=player_d_name,
                    match_key=cd_key or match_key or "",
                    match_code=result.get("match_code"),
                    player_a_points=player_c_total,
                    player_b_points=player_d_total,
                    player_a_bonus=adjusted_cd["player_a_bonus"],
                    player_b_bonus=adjusted_cd["player_b_bonus"],
                    player_a_total=adjusted_cd["player_a_total"],
                    player_b_total=adjusted_cd["player_b_total"],
                    winner=adjusted_cd["winner"],
                    course_id=result.get("course_id"),
                    course_tee_id=result.get("course_tee_id"),
                    tournament_id=tournament_id,
                    player_a_handicap=handicap_c,
                    player_b_handicap=handicap_d,
                    hole_count=match_length,
                    start_hole=match_start_hole,
                    player_a_id=player_c_id,
                    player_b_id=player_d_id,
                    player_c_id=None,
                    player_d_id=None,
                )
                if inserted_id:
                    upsert_match_bonus(
                        settings.database_url,
                        inserted_id,
                        player_a_bonus=adjusted_cd["player_a_bonus"],
                        player_b_bonus=adjusted_cd["player_b_bonus"],
                    )
                    insert_hole_scores(settings.database_url, inserted_id, holes_for_cd)
                    cd_result = fetch_match_result(settings.database_url, inserted_id)
                    if cd_result:
                        cd_entries = _player_scorecard_entries(cd_result, holes_for_cd)
                        insert_player_hole_scores(
                            settings.database_url,
                            inserted_id,
                            cd_result.get("match_key") or cd_key,
                            cd_entries,
                        )
                    finalize_match_result(
                        settings.database_url,
                        inserted_id,
                        course_snapshot=course_snapshot,
                        scorecard_snapshot=scorecard_cd,
                    )
                    finalized_any = True

    _refresh_standings_cache(tournament_id)
    if match_key:
        set_match_finalized(settings.database_url, match_key, True)
    if redirect:
        return RedirectResponse(url=redirect, status_code=303)
    return JSONResponse({"finalized": True})


@app.post("/matches/{match_id}/reset")
async def reset_match_scores(
    match_id: int,
    redirect: str | None = Form("/admin/scheduled_matches"),
):
    result, match_record = _resolve_match_result_context(match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    match_key = result.get("match_key") or (match_record and match_record.get("match_key"))
    if not match_key:
        raise HTTPException(status_code=400, detail="Match key is required to reset.")
    target_ids: set[int] = {result["id"]}
    if match_key:
        target_ids.update(fetch_match_result_ids_by_key(settings.database_url, match_key))
    outcome = score_outcome(0, 0)
    reset_match_results(
        settings.database_url,
        list(target_ids),
        player_a_points=outcome["player_a_points"],
        player_b_points=outcome["player_b_points"],
        player_a_bonus=outcome["player_a_bonus"],
        player_b_bonus=outcome["player_b_bonus"],
        player_a_total=outcome["player_a_total"],
        player_b_total=outcome["player_b_total"],
        winner=outcome["winner"],
    )
    _refresh_standings_cache(result.get("tournament_id"))
    if match_key:
        set_match_finalized(settings.database_url, match_key, False)
    return RedirectResponse(url=redirect or "/admin/scheduled_matches", status_code=303)


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
            player_c_score = int(entry.get("player_c_score", 0))
            player_d_score = int(entry.get("player_d_score", 0))
        except (TypeError, ValueError):
            continue
        if hole_number <= 0:
            continue
        cleaned.append(
            {
                "hole_number": hole_number,
                "player_a_score": player_a_score,
                "player_b_score": player_b_score,
                "player_c_score": player_c_score,
                "player_d_score": player_d_score,
            }
        )
    insert_hole_scores(settings.database_url, match_id, cleaned)
    match_result = fetch_match_result(settings.database_url, match_id)
    if match_result:
        player_entries = _player_scorecard_entries(match_result, cleaned)
        match_key = match_result.get("match_key") or ""
        insert_player_hole_scores(settings.database_url, match_id, match_key, player_entries)
        cd_key = f"{match_key}-cd" if match_key else ""
        cd_result = fetch_match_result_by_key(settings.database_url, cd_key) if cd_key else None
        if cd_result:
            cd_holes = _holes_for_cd(cleaned)
            if cd_holes:
                cd_entries = _player_scorecard_entries(cd_result, cd_holes)
                insert_player_hole_scores(
                    settings.database_url,
                    cd_result["id"],
                    cd_result.get("match_key") or cd_key,
                    cd_entries,
                )
        tournament_id = match_result.get("tournament_id") or _get_active_tournament_id()
    _recompute_match_result_from_holes(match_id)
    _refresh_standings_cache(tournament_id)
    return JSONResponse({"added": len(cleaned)})


@app.post("/matches/{match_id}/bonus")
async def set_match_bonus(match_id: int, request: Request):
    payload = await request.json()
    bonus_data = payload.get("bonus") or {}

    def _parse_bonus(value: float | str | None) -> float:
        if value is None:
            return 0.0
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return 0.0
        return parsed

    player_a_bonus = _parse_bonus(bonus_data.get("player_a") or bonus_data.get("a"))
    player_b_bonus = _parse_bonus(bonus_data.get("player_b") or bonus_data.get("b"))
    match_result = fetch_match_result(settings.database_url, match_id)
    if not match_result:
        raise HTTPException(status_code=404, detail="Match not found")
    upsert_match_bonus(
        settings.database_url,
        match_id,
        player_a_bonus=player_a_bonus,
        player_b_bonus=player_b_bonus,
    )
    tournament_id = _tournament_id_for_result(match_result)
    if tournament_id:
        _refresh_standings_cache(tournament_id)
    return JSONResponse(
        {
            "match_id": match_id,
            "player_a_bonus": player_a_bonus,
            "player_b_bonus": player_b_bonus,
        }
    )


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
        tournament_id = _get_active_tournament_id()
        tournament_settings = _load_tournament_settings(tournament_id)
        course_id = _safe_int(tournament_settings.get("course_id"))
        course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
        match_length = pairing.hole_count if pairing else 18
        player_a_id = _player_id_by_name(pairing.player_a)
        player_b_id = _player_id_by_name(pairing.player_b)
        player_c_id = _player_id_by_name(pairing.player_c)
        player_d_id = _player_id_by_name(pairing.player_d)
        match_id = insert_match_result(
            settings.database_url,
            match_name=match_display(pairing),
            player_a=pairing.player_a,
            player_b=pairing.player_b,
            match_key=pairing.match_id,
            match_code=None,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
            player_c_id=player_c_id,
            player_d_id=player_d_id,
            player_a_handicap=_player_handicap_by_name(pairing.player_a),
            player_b_handicap=_player_handicap_by_name(pairing.player_b),
            course_id=course_id,
            course_tee_id=course_tee_id,
            tournament_id=tournament_id,
            hole_count=match_length,
            start_hole=pairing.start_hole or 1,
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
            player_c_score = int(entry.get("player_c_score", 0))
            player_d_score = int(entry.get("player_d_score", 0))
        except (TypeError, ValueError):
            continue
        if hole_number <= 0:
            continue
        cleaned.append(
            {
                "hole_number": hole_number,
                "player_a_score": player_a_score,
                "player_b_score": player_b_score,
                "player_c_score": player_c_score,
                "player_d_score": player_d_score,
            }
        )
    insert_hole_scores(settings.database_url, match_id, cleaned)
    entries = _player_scorecard_entries(match_result, cleaned)
    insert_player_hole_scores(settings.database_url, match_id, match_result.get("match_key") or "", entries)
    match_key = match_result.get("match_key") or ""
    cd_key = f"{match_key}-cd" if match_key else ""
    cd_result = fetch_match_result_by_key(settings.database_url, cd_key) if cd_key else None
    if cd_result:
        cd_holes = _holes_for_cd(cleaned)
        if cd_holes:
            cd_entries = _player_scorecard_entries(cd_result, cd_holes)
            insert_player_hole_scores(
                settings.database_url,
                cd_result["id"],
                cd_result.get("match_key") or cd_key,
                cd_entries,
            )
            insert_hole_scores(settings.database_url, cd_result["id"], cd_holes)
    tournament_id = match_result.get("tournament_id") or _get_active_tournament_id()
    _recompute_match_result_from_holes(match_id)
    if tournament_id:
        _refresh_standings_cache(tournament_id)
    return JSONResponse({"added": len(cleaned)})


def _apply_bonus_constraints(
    outcome: dict,
    total_a: float,
    total_b: float,
    recorded_holes: int,
    match_length: int,
    bonus_override: dict[str, float] | None = None,
) -> dict:
    if not _bonus_allowed(recorded_holes, match_length):
        return {
            **outcome,
            "player_a_bonus": 0.0,
            "player_b_bonus": 0.0,
            "player_a_total": total_a,
            "player_b_total": total_b,
        }
    player_a_bonus = outcome["player_a_bonus"]
    player_b_bonus = outcome["player_b_bonus"]
    if bonus_override:
        player_a_bonus = float(bonus_override.get("player_a_bonus") or 0.0)
        player_b_bonus = float(bonus_override.get("player_b_bonus") or 0.0)
    return {
        **outcome,
        "player_a_bonus": player_a_bonus,
        "player_b_bonus": player_b_bonus,
        "player_a_total": total_a + player_a_bonus,
        "player_b_total": total_b + player_b_bonus,
    }


def _match_scorecard_summary(
    match_result: dict,
    holes: list[dict],
    tournament_settings: dict[str, str],
    match_length: int,
    start_hole: int,
    player_a_handicap: int | None = None,
    player_b_handicap: int | None = None,
) -> tuple[dict, float, float]:
    if holes:
        scorecard_data = _scorecard_data_for_match(
            match_result,
            holes,
            player_a_handicap if player_a_handicap is not None else (
                match_result.get("player_a_handicap") or _player_handicap_by_name(match_result.get("player_a_name"))
            ),
            player_b_handicap if player_b_handicap is not None else (
                match_result.get("player_b_handicap") or _player_handicap_by_name(match_result.get("player_b_name"))
            ),
            tournament_settings,
            match_length=match_length,
            start_hole=start_hole,
            use_snapshot=False,
        )
        total_points_a = scorecard_data["meta"]["total_points_a"]
        total_points_b = scorecard_data["meta"]["total_points_b"]
        if total_points_a == 0 and total_points_b == 0:
            fallback_a, fallback_b = _estimate_points_from_raw_scores(holes)
            if fallback_a > 0 or fallback_b > 0:
                total_points_a = fallback_a
                total_points_b = fallback_b
                scorecard_data.setdefault("meta", {})["total_points_a"] = fallback_a
                scorecard_data.setdefault("meta", {})["total_points_b"] = fallback_b
    else:
        scorecard_data = match_result.get("scorecard_snapshot") or {}
        total_points_a = match_result.get("player_a_total") or match_result.get("player_a_points") or 0
        total_points_b = match_result.get("player_b_total") or match_result.get("player_b_points") or 0
    return scorecard_data, total_points_a, total_points_b


def _recompute_match_result_from_holes(match_result_id: int) -> None:
    match_result = fetch_match_result(settings.database_url, match_result_id)
    if not match_result:
        return
    tournament_id = _tournament_id_for_result(match_result)
    tournament_settings = _load_tournament_settings(tournament_id)
    match_length = _safe_int(match_result.get("hole_count")) or 18
    start_hole = _safe_int(match_result.get("start_hole")) or 1
    holes = fetch_hole_scores(settings.database_url, match_result_id)
    if not holes:
        return
    computed, total_a, total_b = _match_scorecard_summary(match_result, holes, tournament_settings, match_length, start_hole)
    outcome = score_outcome(total_a, total_b)
    bonus_override = fetch_match_bonus(settings.database_url, match_result_id)
    adjusted = _apply_bonus_constraints(outcome, total_a, total_b, len(holes), match_length, bonus_override=bonus_override)
    update_match_result_scores(
        settings.database_url,
        match_result_id,
        player_a_points=total_a,
        player_b_points=total_b,
        player_a_bonus=adjusted["player_a_bonus"],
        player_b_bonus=adjusted["player_b_bonus"],
        player_a_total=adjusted["player_a_total"],
        player_b_total=adjusted["player_b_total"],
        winner=adjusted["winner"],
    )
    upsert_match_bonus(
        settings.database_url,
        match_result_id,
        player_a_bonus=adjusted["player_a_bonus"],
        player_b_bonus=adjusted["player_b_bonus"],
    )
    _recompute_cd_match_result(match_result, tournament_settings, holes, match_length, start_hole)
    if tournament_id:
        _refresh_standings_cache(tournament_id)


def _recompute_cd_match_result(
    match_result: dict,
    tournament_settings: dict[str, str],
    holes: list[dict],
    match_length: int,
    start_hole: int,
) -> None:
    tournament_id = _tournament_id_for_result(match_result)
    match_key = match_result.get("match_key") or ""
    cd_key = f"{match_key}-cd"
    matches = _matches_by_key(tournament_id)
    pairing = matches.get(match_key)
    player_c_name = (pairing.get("player_c_name") or "").strip() if pairing else ""
    player_d_name = (pairing.get("player_d_name") or "").strip() if pairing else ""
    if not player_c_name or not player_d_name:
        return
    holes_for_cd = [
        {
            "hole_number": entry.get("hole_number"),
            "player_a_score": entry.get("player_c_score"),
            "player_b_score": entry.get("player_d_score"),
            "player_c_score": entry.get("player_c_score"),
            "player_d_score": entry.get("player_d_score"),
        }
        for entry in holes
    ]
    if not any(entry.get("player_a_score") is not None or entry.get("player_b_score") is not None for entry in holes_for_cd):
        return
    handicap_c = _player_handicap_by_name(player_c_name)
    handicap_d = _player_handicap_by_name(player_d_name)
    computed_cd = _scorecard_data_for_match(
        match_result,
        holes_for_cd,
        handicap_c,
        handicap_d,
        tournament_settings,
        match_length=match_length,
        start_hole=start_hole,
    )
    total_c = computed_cd["meta"]["total_points_a"]
    total_d = computed_cd["meta"]["total_points_b"]
    outcome_cd = score_outcome(total_c, total_d)
    cd_result = fetch_match_result_by_key(settings.database_url, cd_key)
    if not cd_result:
        return
    bonus_override_cd = fetch_match_bonus(settings.database_url, cd_result["id"])
    adjusted_cd = _apply_bonus_constraints(
        outcome_cd,
        total_c,
        total_d,
        len(holes_for_cd),
        match_length,
        bonus_override=bonus_override_cd,
    )
    update_match_result_scores(
        settings.database_url,
        cd_result["id"],
        player_a_points=total_c,
        player_b_points=total_d,
        player_a_bonus=adjusted_cd["player_a_bonus"],
        player_b_bonus=adjusted_cd["player_b_bonus"],
        player_a_total=adjusted_cd["player_a_total"],
        player_b_total=adjusted_cd["player_b_total"],
        winner=adjusted_cd["winner"],
    )
    upsert_match_bonus(
        settings.database_url,
        cd_result["id"],
        player_a_bonus=adjusted_cd["player_a_bonus"],
        player_b_bonus=adjusted_cd["player_b_bonus"],
    )


@app.post("/api/refresh-standings")
async def api_refresh_standings():
    tournament_id = _get_active_tournament_id()
    if not tournament_id:
        raise HTTPException(status_code=404, detail="No active tournament selected.")
    _refresh_standings_cache(tournament_id)
    return JSONResponse({"refreshed": True, "tournament_id": tournament_id})


@app.get("/standings", response_class=HTMLResponse)
async def standings(request: Request):
    results = fetch_all_match_results(settings.database_url)
    tournament_id = _get_active_tournament_id()
    if tournament_id is None:
        divisions = []
    else:
        divisions = build_standings(results, tournament_id=tournament_id)
    return templates.TemplateResponse(
        "standings.html",
        {"request": request, "divisions": divisions},
    )
