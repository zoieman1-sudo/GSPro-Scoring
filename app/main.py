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
    delete_match,
    delete_match_results_by_key,
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
    fetch_match_by_id,
    fetch_match_by_key,
    fetch_match_result,
    fetch_match_result_by_code,
    fetch_match_result_by_key,
    fetch_match_result_ids_by_key,
    fetch_matches_by_tournament,
    fetch_matches_by_keys,
    fetch_player_by_name,
    fetch_players,
    fetch_recent_results,
    fetch_settings,
    fetch_standings_cache,
    fetch_tournament_by_id,
    fetch_tournaments,
    insert_hole_scores,
    insert_match,
    insert_match_result,
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
    update_match,
    update_match_result_fields,
    update_match_result_scores,
)
from app.course_sync import ensure_georgia_course, ensure_pebble_beach_course, import_course_to_db
from app.seed import (
    Match,
    build_pairings_from_players,
    find_match,
    match_display,
)
from app.settings import load_settings, score_outcome

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/golf-ui", StaticFiles(directory="golf-ui"), name="golf-ui")
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
DEMO_TOURNAMENT_NAME = "Demo"
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

ACTIVE_TOURNAMENT_ID_KEY = "active_tournament_id"

ACTIVE_MATCH_SETTING_KEY = "active_match_key"

MATCH_GROUPS_SETTING_KEY = "match_groups"


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


def _generate_match_key() -> str:
    pool = string.ascii_uppercase + "123456789"
    while True:
        value = "".join(random.choice(pool) for _ in range(6))
        if not fetch_match_by_key(settings.database_url, value):
            return value


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

def _ensure_demo_tournament() -> int | None:
    tournaments = fetch_tournaments(settings.database_url)
    demo = next((entry for entry in tournaments if (entry["name"] or "").strip().lower() == DEMO_TOURNAMENT_NAME.lower()), None)
    if demo:
        tournament_id = demo["id"]
    else:
        tournament_id = insert_tournament(
            settings.database_url,
            name=DEMO_TOURNAMENT_NAME,
            description="Auto-generated demo event populated with seeded players.",
            status="active",
        )
    if tournament_id:
        total_players = sum(DEFAULT_DIVISION_COUNT.values())
        total_divisions = len(DEFAULT_DIVISION_COUNT)
        upsert_event_setting(settings.database_url, tournament_id, "player_count", str(total_players))
        upsert_event_setting(settings.database_url, tournament_id, "division_count", str(total_divisions))
        if _get_active_tournament_id() is None:
            _set_active_tournament_id(tournament_id)
    return tournament_id


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
            label_parts = [
                tee.get("gender", "").capitalize(),
                " — ",
                tee.get("tee_name") or "Tee",
            ]
            total_yards = tee.get("total_yards")
            if total_yards:
                label_parts.append(f" ({total_yards} yds)")
            tees.append(
                {
                    "id": str(tee.get("id") or ""),
                    "label": "".join(label_parts),
                    "course_id": key,
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
) -> dict:
    snapshot = (match_result.get("scorecard_snapshot") if match_result else None) or {}
    if snapshot.get("rows"):
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
    )


def _seed_default_players() -> list[dict]:
    existing = fetch_players(settings.database_url)
    if existing:
        return existing
    roster = _default_player_roster()
    demo_tournament_id = _ensure_demo_tournament()
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
                tournament_id=demo_tournament_id,
            )
    return fetch_players(settings.database_url)


def _load_pairings(tournament_id: int | None = None) -> list[Match]:
    target_tournament = tournament_id if tournament_id is not None else _get_active_tournament_id()
    if target_tournament is None:
        return []
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
    course_id = _safe_int(tournament_settings.get("course_id"))
    course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
    player_a_info = fetch_player_by_name(settings.database_url, pairing.player_a) or {}
    player_b_info = fetch_player_by_name(settings.database_url, pairing.player_b) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    outcome = score_outcome(0, 0)
    insert_match_result(
        settings.database_url,
        match_name=match_display(pairing),
        player_a=pairing.player_a,
        player_b=pairing.player_b,
        match_key=pairing.match_id,
        match_code=None,
        player_a_points=outcome["player_a_points"],
        player_b_points=outcome["player_b_points"],
        player_a_bonus=outcome["player_a_bonus"],
        player_b_bonus=outcome["player_b_bonus"],
        player_a_total=outcome["player_a_total"],
        player_b_total=outcome["player_b_total"],
        winner=outcome["winner"],
        course_id=course_id,
        course_tee_id=course_tee_id,
        tournament_id=tournament_id,
        player_a_handicap=handicap_a,
        player_b_handicap=handicap_b,
    )
    return fetch_match_result_by_key(settings.database_url, pairing.match_id)


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
            insert_match_result(
                settings.database_url,
                match_name=match_display(pairing),
                player_a=pairing.player_a,
                player_b=pairing.player_b,
                match_key=pairing.match_id,
                match_code=None,
                player_a_points=0,
                player_b_points=0,
                tournament_id=tournament_id,
                course_id=course_id,
                course_tee_id=course_tee_id,
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


def _aggregate_standings_entries(results: list[dict], tournament_id: int | None) -> list[dict]:
    if not tournament_id:
        return []
    tournament_players = [
        player for player in fetch_players(settings.database_url) if player.get("tournament_id") == tournament_id
    ]
    if not tournament_players:
        return []
    divisions_by_player = {player["name"]: player["division"] for player in tournament_players}
    seeds = {player["name"]: player.get("seed", 0) for player in tournament_players}
    stats: dict[str, dict] = {
        name: _empty_stat(name, division)
        for name, division in divisions_by_player.items()
    }

    allowed_players = set(divisions_by_player.keys())

    for result in results:
        if result.get("tournament_id") != tournament_id:
            continue
        hole_entries = fetch_hole_scores(settings.database_url, result["id"])
        if not hole_entries and not (result.get("player_a_total") or result.get("player_b_total")):
            continue
        player_a_total = result.get("player_a_total") or 0
        player_b_total = result.get("player_b_total") or 0
        winner = result.get("winner") or "T"
        if player_a_total == 0 and player_b_total == 0:
            player_a_total, player_b_total, winner = _resolve_result_totals(result)
        if player_a_total == 0 and player_b_total == 0:
            continue
        hole_count = len(hole_entries)
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
            computed = _scorecard_data_for_match(
                match_result,
                holes,
                handicap_a,
                handicap_b,
                tournament_settings,
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
    computed = _scorecard_data_for_match(
        match_result if match_result else {},
        holes,
        handicap_a,
        handicap_b,
        tournament_settings,
    )
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


@app.get("/play_match", response_class=HTMLResponse)
async def play_match_page(request: Request, status: str | None = None):
    matches, _ = _build_match_listing()
    active_key = _get_active_match_key()
    active_match = next((entry for entry in matches if entry["match_key"] == active_key), None)
    active_result = None
    if active_match:
        active_result = fetch_match_result_by_key(settings.database_url, active_match["match_key"])

    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)
    hero_course = None
    if active_result and active_result.get("course_id"):
        catalog = fetch_course_catalog(settings.database_url)
        course = next((c for c in catalog if c["id"] == active_result["course_id"]), None)
        hero_course = course
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
    active_tournament = (
        fetch_tournament_by_id(settings.database_url, tournament_id) if tournament_id else None
    )
    hero_course_info = _course_display_info(active_result or {}, tournament_settings)
    return templates.TemplateResponse(
        "play_match.html",
        {
            "request": request,
            "matches": matches,
            "status_message": status,
            "hero_course": hero_course,
            "hero_course_info": hero_course_info,
            "course_tee_map": _build_course_tee_map(course_catalog),
            "tournament_settings": tournament_settings,
            "active_match": active_match,
            "active_match_code": active_result and active_result.get("match_code"),
            "course_catalog": course_catalog,
            "selected_course_id": selected_course_id,
            "selected_course_tee_id": selected_course_tee_id,
            "selected_course": selected_course,
            "selected_course_tee": selected_course_tee,
            "active_tournament": active_tournament,
            "active_tournament_id": tournament_id,
        },
    )


@app.post("/play_match/activate", response_class=RedirectResponse)
async def play_match_activate(request: Request, match_key: str = Form(...)):
    pairing = next((item for item in _load_pairings() if item.match_id == match_key), None)
    if not pairing:
        raise HTTPException(status_code=404, detail="Match not found")
    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)
    course_id = _safe_int(tournament_settings.get("course_id"))
    course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
    player_a_info = fetch_player_by_name(settings.database_url, pairing.player_a) or {}
    player_b_info = fetch_player_by_name(settings.database_url, pairing.player_b) or {}
    handicap_a = player_a_info.get("handicap", 0)
    handicap_b = player_b_info.get("handicap", 0)
    match_result = fetch_match_result_by_key(settings.database_url, pairing.match_id)
    if not match_result:
        outcome = score_outcome(0, 0)
        insert_match_result(
            settings.database_url,
            match_name=match_display(pairing),
            player_a=pairing.player_a,
            player_b=pairing.player_b,
            match_key=pairing.match_id,
            match_code=None,
            player_a_points=outcome["player_a_points"],
            player_b_points=outcome["player_b_points"],
            player_a_bonus=outcome["player_a_bonus"],
            player_b_bonus=outcome["player_b_bonus"],
            player_a_total=outcome["player_a_total"],
            player_b_total=outcome["player_b_total"],
            winner=outcome["winner"],
            course_id=course_id,
            course_tee_id=course_tee_id,
            tournament_id=tournament_id,
            player_a_handicap=handicap_a,
            player_b_handicap=handicap_b,
        )
        match_result = fetch_match_result_by_key(settings.database_url, pairing.match_id)
    update_match_result_fields(
        settings.database_url,
        pairing.match_id,
        course_id=course_id,
        course_tee_id=course_tee_id,
        player_a_handicap=handicap_a,
        player_b_handicap=handicap_b,
    )
    _set_active_match_key(pairing.match_id)
    match_record = fetch_match_by_key(settings.database_url, pairing.match_id)
    if match_record:
        update_match(
            settings.database_url,
            match_record["id"],
            status="in_progress",
            active=True,
        )
    message = f"Active match set to {match_display(pairing)}"
    return RedirectResponse(url=f"/play_match?status={quote_plus(message)}", status_code=303)


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


@app.get("/new_card", response_class=HTMLResponse)
async def new_card(request: Request):
    return templates.TemplateResponse("other_stuff.html", _new_card_context(request))


@app.get("/other_stuff", response_class=HTMLResponse)
async def other_stuff(request: Request):
    return templates.TemplateResponse("other_stuff.html", _new_card_context(request))


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


@app.get("/api/match_groups/{group_key}/scorecard")
async def api_match_group_scorecard(group_key: str):
    context = _scorecard_context(None)
    group = next((entry for entry in context.get("match_groups", []) if entry.get("group_key") == group_key), None)
    manual_group = None
    if not group:
        manual_group = _manual_group_for_key(group_key, context.get("scorecard", {}).get("match", {}).get("tournament_id"))
        if manual_group:
            group = manual_group
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    matches_payload: list[dict] = []
    group_holes: list[dict] | None = None
    group_course: dict | None = group.get("course")
    for match_entry in group.get("matches", []):
        match_key = match_entry.get("match_key")
        if not match_key:
            continue
        serialized = _serialized_scorecard_by_key(match_key)
        if not serialized:
            continue
        if group_holes is None:
            group_holes = serialized.get("holes", [])
        if not group_course:
            group_course = serialized.get("course")
        serialized_players = serialized.get("players", [])
        players = []
        for idx, player in enumerate(serialized_players):
            players.append(
                {
                    "name": player.get("name"),
                    "handicap": player.get("course_handicap", 0),
                    "role": "A" if idx == 0 else "B",
                }
            )
        matches_payload.append(
            {
                "match_key": serialized.get("match_key"),
                "match_id": serialized.get("match_id"),
                "match_code": serialized.get("match_code"),
                "label": serialized.get("label"),
                "players": players,
                "holes": serialized.get("holes", []),
            }
        )
    if not matches_payload:
        raise HTTPException(status_code=404, detail="Group scorecard not available")
    return JSONResponse(
        {
            "group_key": group.get("group_key"),
            "label": group.get("label") or "",
            "course": group_course or {},
            "holes": group_holes or [],
            "matches": matches_payload,
        }
    )


class MatchGroupEntry(BaseModel):
    group_key: str | None = None
    label: str | None = None
    match_keys: list[str]
    course: dict | None = None
    tournament_id: int | None = None


class MatchGroupsPayload(BaseModel):
    groups: list[MatchGroupEntry]


@app.get("/api/match_groups")
async def api_match_groups():
    context = _scorecard_context(None)
    tournament_id = _get_active_tournament_id()
    return JSONResponse(
        {
            "match_groups": context.get("match_groups", []),
            "active_matches": context.get("active_matches", []),
            "group_definitions": context.get("group_definitions", []),
            "manual_groups": _manual_group_definitions(tournament_id),
        }
    )


@app.post("/api/match_groups")
async def api_store_match_groups(payload: MatchGroupsPayload):
    unique_keys: set[str] = set()
    processed: list[dict] = []
    for entry in payload.groups:
        match_keys = [key for key in entry.match_keys if key]
        if not match_keys:
            continue
        label = (entry.label or "").strip()
        base_key = (entry.group_key or "").strip()
        if not base_key:
            base_key = f"group-{'-'.join(match_keys)}"
        base_key = re.sub(r"[^a-z0-9]+", "-", base_key.lower()).strip("-")
        if not base_key:
            base_key = f"group-{int(datetime.utcnow().timestamp())}"
        suffix = 1
        candidate = base_key
        while candidate in unique_keys:
            candidate = f"{base_key}-{suffix}"
            suffix += 1
        unique_keys.add(candidate)
        processed.append(
            {
                "group_key": candidate,
                "label": label,
                "match_keys": match_keys,
                "course": entry.course or {},
                "tournament_id": entry.tournament_id,
            }
        )
    _persist_match_group_definitions(processed)
    return JSONResponse({"group_definitions": processed})


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
    ensure_pebble_beach_course(settings.database_url)
    ensure_georgia_course(settings.database_url)
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

    tournament_id = _get_active_tournament_id()
    tournament_settings = _load_tournament_settings(tournament_id)

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
        player_a_handicap=_player_handicap_by_name(player_a),
        player_b_handicap=_player_handicap_by_name(player_b),
        course_id=_safe_int(tournament_settings.get("course_id")),
        course_tee_id=_safe_int(tournament_settings.get("course_tee_id")),
        tournament_id=tournament_id,
        **outcome,
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
    record_id = insert_match_result(
        settings.database_url,
        match_name=match_name,
        player_a=player_a,
        player_b=player_b,
        match_key=payload.match_id or "",
        match_code=None,
        player_a_points=payload.player_a_points,
        player_b_points=payload.player_b_points,
        player_a_handicap=_player_handicap_by_name(player_a),
        player_b_handicap=_player_handicap_by_name(player_b),
        course_id=_safe_int(tournament_settings.get("course_id")),
        course_tee_id=_safe_int(tournament_settings.get("course_tee_id")),
        tournament_id=tournament_id,
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


@app.get("/admin/match_setup", response_class=HTMLResponse)
async def match_setup_page(
    request: Request,
    tournament_id: int | None = None,
    status: str | None = None,
    edit_match_id: int | None = Query(None),
):
    tournaments = fetch_tournaments(settings.database_url)
    active_id = tournament_id or _get_active_tournament_id()
    selected_players = _players_for_tournament(active_id) if active_id else fetch_players(settings.database_url)
    matches = fetch_matches_by_tournament(settings.database_url, active_id) if active_id else []
    course_catalog = fetch_course_catalog(settings.database_url)
    editable_match = None
    if edit_match_id:
        match_candidate = fetch_match_by_id(settings.database_url, edit_match_id)
        if match_candidate and match_candidate["tournament_id"] == active_id:
            editable_match = match_candidate
    return templates.TemplateResponse(
        "match_setup.html",
        {
            "request": request,
            "status": status,
            "tournaments": tournaments,
            "active_tournament_id": active_id,
            "players": selected_players,
            "matches": matches,
            "course_catalog": course_catalog,
            "editable_match": editable_match,
        },
    )


@app.get("/groups", response_class=HTMLResponse)
async def groups_page(
    request: Request,
    tournament_id: int | None = None,
    status: str | None = None,
):
    tournaments = fetch_tournaments(settings.database_url)
    active_id = tournament_id or _get_active_tournament_id()
    matches = fetch_matches_by_tournament(settings.database_url, active_id) if active_id else []
    return templates.TemplateResponse(
        "groups.html",
        {
            "request": request,
            "tournaments": tournaments,
            "active_tournament_id": active_id,
            "matches": matches,
            "status": status,
        },
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


@app.get("/mobile", response_class=HTMLResponse)
async def mobile_scoring_launcher(request: Request):
    return templates.TemplateResponse("mobile_groups.html", {"request": request})


@app.post("/admin/match_setup")
async def match_setup_submit(
    tournament_id: int = Form(...),
    division: str = Form(...),
    player_a_id: int = Form(...),
    player_b_id: int = Form(...),
    player_c_id: int = Form(...),
    player_d_id: int = Form(...),
    course_id: int | None = Form(None),
    course_tee_id: int | None = Form(None),
    hole_count: int | None = Form(18),
    active: str | None = Form(None),
    match_id: int | None = Form(None),
):
    if match_id:
        update_match(
            settings.database_url,
            match_id,
            division=division,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
            player_c_id=player_c_id,
            player_d_id=player_d_id,
            course_id=course_id,
            course_tee_id=course_tee_id,
            hole_count=hole_count or 18,
            active=bool(active),
        )
        message = "Match updated."
    else:
        match_key = _generate_match_key()
        insert_match(
            settings.database_url,
            tournament_id=tournament_id,
            match_key=match_key,
            division=division,
            player_a_id=player_a_id,
            player_b_id=player_b_id,
            player_c_id=player_c_id,
            player_d_id=player_d_id,
            course_id=course_id,
            course_tee_id=course_tee_id,
            hole_count=hole_count or 18,
            active=bool(active),
        )
        message = "Match saved."
    return RedirectResponse(
        url=f"/admin/match_setup?tournament_id={tournament_id}&status={quote_plus(message)}",
        status_code=303,
    )
    return RedirectResponse(
        url=f"/admin/match_setup?tournament_id={tournament_id}&status={quote_plus(message)}",
        status_code=303,
    )


@app.post("/admin/match_setup/delete")
async def match_setup_delete(
    match_id: int = Form(...),
    tournament_id: int = Form(...),
):
    match = fetch_match_by_id(settings.database_url, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    delete_match_results_by_key(settings.database_url, match["match_key"])
    delete_match(settings.database_url, match_id)
    return RedirectResponse(
        url=f"/admin/match_setup?tournament_id={tournament_id}&status={quote_plus('Match deleted.')}",
        status_code=303,
    )


@app.get("/live_matches", response_class=HTMLResponse)
async def live_matches_view(request: Request):
    tournament_id = _get_active_tournament_id()
    matches = fetch_matches_by_tournament(settings.database_url, tournament_id) if tournament_id else []
    live_matches = []
    for match in matches:
        if match["status"] in ("in_progress", "not_started"):
            match_result = fetch_match_result_by_key(settings.database_url, match["match_key"])
            live_matches.append({"match": match, "result": match_result})
    tournaments = fetch_tournaments(settings.database_url)
    return templates.TemplateResponse(
        "live_matches.html",
        {
            "request": request,
            "tournament_id": tournament_id,
            "tournaments": tournaments,
            "matches": live_matches,
        },
    )


@app.get("/tournament_history", response_class=HTMLResponse)
async def tournament_history(request: Request, tournament_id: int | None = None):
    selected_id = tournament_id or _get_active_tournament_id()
    tournaments = fetch_tournaments(settings.database_url)
    matches = fetch_matches_by_tournament(settings.database_url, selected_id) if selected_id else []
    history = []
    for match in matches:
        result = fetch_match_result_by_key(settings.database_url, match["match_key"])
        history.append({"match": match, "result": result})
    return templates.TemplateResponse(
        "tournament_history.html",
        {
            "request": request,
            "tournaments": tournaments,
            "selected_tournament_id": selected_id,
            "history": history,
        },
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


def _build_match_listing(player_filter: str | None = None) -> tuple[list[dict], list[str]]:
    pairings = _load_pairings()
    matches_data: list[dict] = []
    player_names: set[str] = set()
    for pairing in pairings:
        player_names.add(pairing.player_a)
        player_names.add(pairing.player_b)
        if player_filter and player_filter != "all":
            if player_filter not in (pairing.player_a, pairing.player_b):
                continue
        status_info = _match_status_info(pairing.match_id)
        result = status_info.get("match_result")
        winner = "Pending"
        totals = "—"
        submitted = None
        match_result_id = None
        phase = status_info["status"]
        if result:
            winner_code = result["winner"]
            if winner_code == "A":
                winner = result["player_a_name"]
            elif winner_code == "B":
                winner = result["player_b_name"]
            elif winner_code == "T":
                winner = "Draw"
            totals = f'{result["player_a_total"]} - {result["player_b_total"]}'
            submitted = result.get("submitted_at")
            match_result_id = result.get("id")
            if (result.get("player_a_total") == 0 and result.get("player_b_total") == 0) and status_info["holes"]:
                holes = fetch_hole_scores(settings.database_url, result["id"])
                if holes:
                    player_a_info = fetch_player_by_name(settings.database_url, result["player_a_name"]) or {}
                    player_b_info = fetch_player_by_name(settings.database_url, result["player_b_name"]) or {}
                    handicap_a = player_a_info.get("handicap", 0)
                    handicap_b = player_b_info.get("handicap", 0)
                    event_tournament_settings = _load_tournament_settings(
                        _tournament_id_for_result(result)
                    )
                    match_course_holes = _course_holes_for_match(result, event_tournament_settings)
                    computed = _build_scorecard_rows(holes, handicap_a, handicap_b, match_course_holes)
                    total_a = computed["meta"]["total_points_a"]
                    total_b = computed["meta"]["total_points_b"]
                    totals = f"{total_a} - {total_b}"
                    if total_a > total_b:
                        winner = result["player_a_name"]
                    elif total_b > total_a:
                        winner = result["player_b_name"]
                    else:
                        winner = "Draw"
        resolved_match_key = result["match_key"] if result and result.get("match_key") else pairing.match_id
        matches_data.append(
            {
                "match_key": resolved_match_key,
                "display": match_display(pairing),
                "player_a": pairing.player_a,
                "player_b": pairing.player_b,
                "division": pairing.division,
                "status": status_info["status"],
                "status_label": MATCH_STATUS_LABELS.get(status_info["status"], status_info["status"]),
                "holes": status_info["holes"],
                "winner": winner,
                "totals": totals,
            "submitted": _coerce_datetime(submitted),
                "result_id": match_result_id,
                "phase": phase,
            }
        )
    return matches_data, sorted(player_names)


@app.get("/matches", response_class=HTMLResponse)
async def matches_list(request: Request):
    results = fetch_recent_results(settings.database_url, limit=20)
    return templates.TemplateResponse(
        "matches.html",
        {"request": request, "matches": results},
    )


@app.get("/match_list", response_class=HTMLResponse)
async def match_list(request: Request):
    player_filter = request.query_params.get("player")
    matches_data, players = _build_match_listing(player_filter)
    return templates.TemplateResponse(
        "match_list.html",
        {
            "request": request,
            "matches": matches_data,
            "players": players,
            "active_player": player_filter or "all",
        },
    )


@app.get("/api/match_list")
async def api_match_list(player: str | None = None):
    matches_data, _ = _build_match_listing(player)
    return JSONResponse(matches_data)



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


def _build_match_groups(matches: list[dict]) -> list[dict]:
    groups: list[dict] = []
    for index in range(0, len(matches), 2):
        batch = matches[index : index + 2]
        if not batch:
            continue
        group_key = "-".join(entry["match_key"] for entry in batch)
        groups.append(
            {
                "group_key": f"group-{group_key}",
                "matches": batch,
            }
        )
    return groups


def _stored_match_group_definitions() -> list[dict]:
    value = fetch_settings(settings.database_url).get(MATCH_GROUPS_SETTING_KEY)
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    definitions: list[dict] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        match_keys = [str(k) for k in (entry.get("match_keys") or []) if k]
        if not match_keys:
            continue
        tournament_id = entry.get("tournament_id")
        definitions.append(
            {
                "group_key": str(entry.get("group_key") or ""),
                "label": str(entry.get("label") or "").strip(),
                "match_keys": match_keys,
                "course": entry.get("course") or {},
                "tournament_id": int(tournament_id) if tournament_id else None,
            }
        )
    return definitions


def _match_groups_from_definitions(definitions: list[dict], tournament_id: int | None = None) -> list[dict]:
    if not definitions:
        return []
    groups: list[dict] = []
    for definition in definitions:
        definition_tournament = definition.get("tournament_id")
        if definition_tournament is not None and tournament_id is not None and definition_tournament != tournament_id:
            continue
        match_keys = [key for key in definition.get("match_keys", []) if key]
        if not match_keys:
            continue
        matches = fetch_matches_by_keys(settings.database_url, match_keys)
        if not matches:
            continue
        groups.append(
            {
                "group_key": definition.get("group_key") or matches[0]["match_key"],
                "label": definition.get("label") or "",
                "course": definition.get("course") or {},
                "matches": matches,
            }
        )
    return groups

def _manual_group_definitions(tournament_id: int | None) -> list[dict]:
    if not tournament_id:
        return []
    matches = fetch_matches_by_tournament(settings.database_url, tournament_id)
    definitions: list[dict] = []
    for match in matches:
        if not match.get("match_key"):
            continue
        pairs: list[str] = []
        if match.get("player_a_name") and match.get("player_b_name"):
            pairs.append(f"{match['player_a_name']} vs {match['player_b_name']}")
        if match.get("player_c_name") and match.get("player_d_name"):
            pairs.append(f"{match['player_c_name']} vs {match['player_d_name']}")
        definitions.append(
            {
                "group_key": match["match_key"],
                "label": match.get("division") or match["match_key"],
                "match_keys": [match["match_key"]],
                "player_pairs": pairs,
                "manual": True,
            }
        )
    return definitions


def _manual_group_for_key(group_key: str, tournament_id: int | None) -> dict | None:
    match = fetch_match_by_key(settings.database_url, group_key)
    if not match:
        return None
    if tournament_id and match.get("tournament_id") and match["tournament_id"] != tournament_id:
        return None
    return {
        "group_key": match["match_key"],
        "label": match.get("division") or match["match_key"],
        "course": {
            "club_name": "",
            "course_name": "",
            "tee_name": "",
            "total_yards": None,
            "course_rating": None,
            "slope_rating": None,
        },
        "matches": [match],
    }


def _persist_match_group_definitions(definitions: list[dict]) -> None:
    serialized = json.dumps(definitions)
    upsert_setting(settings.database_url, MATCH_GROUPS_SETTING_KEY, serialized)


def _scorecard_context(match_key: str | None, tournament_id: int | None = None) -> dict:
    matches = _load_pairings()
    if not matches:
        # allow manual match to surface even if pairings are empty
        if not match_key:
            return {"matches": [], "match_statuses": [], "active_matches": [], "scorecard": None}

    manual_match_record: dict | None = None
    manual_match_entry: Match | None = None
    if match_key:
        manual_match_record = fetch_match_by_key(settings.database_url, match_key)
        if manual_match_record:
            manual_match_entry = Match(
                match_id=manual_match_record["match_key"],
                division=manual_match_record.get("division") or "A",
                player_a=manual_match_record.get("player_a_name") or "Player A",
                player_b=manual_match_record.get("player_b_name") or "Player B",
            )
            if not any(entry.match_id == manual_match_entry.match_id for entry in matches):
                matches = [manual_match_entry] + matches
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

    match_result = _ensure_match_result_for_pairing(selected)
    if not match_result:
        match_result = fetch_match_result_by_key(settings.database_url, selected.match_id)
    if not match_result:
        match_result = fetch_match_result_by_code(settings.database_url, selected.match_id)
    match_record = None
    if manual_match_record and manual_match_entry and selected.match_id == manual_match_entry.match_id:
        match_record = manual_match_record
    else:
        match_record = fetch_match_by_key(settings.database_url, selected.match_id)
    resolved_tournament_id = tournament_id or _tournament_id_for_result(match_result)
    if not resolved_tournament_id and manual_match_record:
        resolved_tournament_id = manual_match_record.get("tournament_id")
    tournament_settings = _load_tournament_settings(resolved_tournament_id)
    hole_records = (
        fetch_hole_scores(settings.database_url, match_result["id"])
        if match_result
        else []
    )
    player_a_name = match_result["player_a_name"] if match_result else selected.player_a
    player_b_name = match_result["player_b_name"] if match_result else selected.player_b
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
    computed = _scorecard_data_for_match(
        match_result if match_result else {},
        hole_records,
        handicap_a,
        handicap_b,
        tournament_settings,
    )
    course_info = _course_display_info(match_result if match_result else {}, tournament_settings)
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
            "club_name": course_info.get("club_name"),
            "course_name": course_info.get("course_name"),
            "tee_name": course_info.get("tee_name"),
            "total_yards": course_info.get("total_yards"),
            "course_rating": course_info.get("course_rating"),
            "slope_rating": course_info.get("slope_rating"),
        },
        "player_a_handicap": handicap_a,
        "player_b_handicap": handicap_b,
    }
    if match_record:
        scorecard["match"]["division"] = match_record.get("division")
        scorecard["match"]["course_id"] = match_record.get("course_id")
        scorecard["match"]["course_tee_id"] = match_record.get("course_tee_id")
    scorecard["match"]["tournament_id"] = resolved_tournament_id
    scorecard["point_chip_a"] = _adjust_display_points(scorecard["meta"]["total_points_a"])
    scorecard["point_chip_b"] = _adjust_display_points(scorecard["meta"]["total_points_b"])
    match_statuses.sort(
        key=lambda entry: (STATUS_PRIORITY.get(entry["status"], 3), entry["display"])
    )
    active_matches = [entry for entry in match_statuses if entry["status"] != "completed"]
    manual_definitions = _stored_match_group_definitions()
    active_definitions = [
        entry
        for entry in manual_definitions
        if not entry.get("tournament_id") or entry.get("tournament_id") == resolved_tournament_id
    ]
    match_groups = _match_groups_from_definitions(active_definitions, resolved_tournament_id)
    return {
        "matches": matches,
        "match_statuses": match_statuses,
        "active_matches": active_matches,
        "scorecard": scorecard,
        "match_groups": match_groups,
        "group_definitions": active_definitions,
    }


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
    player_names = [
        match_info["player_a_name"],
        match_info["player_b_name"],
    ]
    total_rows = scorecard.get("holes", [])
    hole_entries = []
    gross_totals = [0, 0]
    net_totals = [0, 0]

    def _score_value(value: str | int | float | None) -> bool:
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            return value.isnumeric()
        return False

    for row in total_rows:
        gross_a = row.get("gross_a")
        gross_b = row.get("gross_b")
        net_a = row.get("net_a")
        net_b = row.get("net_b")
        if isinstance(gross_a, (int, float)):
            gross_totals[0] += gross_a
        elif isinstance(gross_a, str) and gross_a.isnumeric():
            gross_totals[0] += float(gross_a)
        if isinstance(gross_b, (int, float)):
            gross_totals[1] += gross_b
        elif isinstance(gross_b, str) and gross_b.isnumeric():
            gross_totals[1] += float(gross_b)
        if isinstance(net_a, (int, float)):
            net_totals[0] += net_a
        elif isinstance(net_a, str) and net_a.isnumeric():
            net_totals[0] += float(net_a)
        if isinstance(net_b, (int, float)):
            net_totals[1] += net_b
        elif isinstance(net_b, str) and net_b.isnumeric():
            net_totals[1] += float(net_b)
        hole_entries.append(
            {
                "hole_number": row.get("hole_number"),
                "par": row.get("par"),
                "handicap": row.get("handicap"),
                "player_scores": [
                    {"name": player_names[0], "gross": gross_a, "net": net_a},
                    {"name": player_names[1], "gross": gross_b, "net": net_b},
                ],
                "winner": _format_winner_label(row.get("result"), player_names),
            }
        )

    has_scores = any(
        any(_score_value(score.get("gross")) for score in hole.get("player_scores", []))
        for hole in hole_entries
    )

    players_data = [
        {
            "name": player_names[0],
            "course_handicap": scorecard.get("player_a_handicap", 0),
            "gross_total": gross_totals[0],
            "net_total": net_totals[0],
            "role": "A",
        },
        {
            "name": player_names[1],
            "course_handicap": scorecard.get("player_b_handicap", 0),
            "gross_total": gross_totals[1],
            "net_total": net_totals[1],
            "role": "B",
        },
    ]

    return {
        "match_id": match_info.get("id"),
        "match_key": match_info["match_key"],
        "match_code": match_info.get("match_code"),
        "label": match_info["match_name"],
        "matchup": f"{player_names[0]} vs {player_names[1]}",
        "status": match_info["status"],
        "status_label": match_info["status_label"],
        "summary": f"Net match play - {match_info['total_holes']} holes - Auto net",
        "players": players_data,
        "holes": hole_entries,
        "meta": scorecard.get("meta", {}),
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


def _new_card_context(request: Request) -> dict:
    hole_layout = [
        {"hole_number": 1, "par": 4, "handicap": 5},
        {"hole_number": 2, "par": 5, "handicap": 1},
        {"hole_number": 3, "par": 3, "handicap": 9},
        {"hole_number": 4, "par": 4, "handicap": 3},
        {"hole_number": 5, "par": 4, "handicap": 2},
        {"hole_number": 6, "par": 4, "handicap": 6},
        {"hole_number": 7, "par": 3, "handicap": 7},
        {"hole_number": 8, "par": 5, "handicap": 4},
        {"hole_number": 9, "par": 4, "handicap": 8},
    ]

    matches_listing, _ = _build_match_listing()
    return {
        "request": request,
        "hole_reference": hole_layout,
        "matches_listing": matches_listing,
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
    course_holes = _course_holes_for_match(result)
    tournament_id = _tournament_id_for_result(result)
    tournament_settings = _load_tournament_settings(tournament_id)
    scorecard = _scorecard_data_for_match(result, holes, handicap_a, handicap_b, tournament_settings)
    total_points_a = scorecard["meta"]["total_points_a"]
    total_points_b = scorecard["meta"]["total_points_b"]
    if total_points_a > total_points_b:
        points_winner_label = result["player_a_name"]
    elif total_points_b > total_points_a:
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
                "a": total_points_a,
                "b": total_points_b,
                "winner_label": points_winner_label,
            },
        },
    )


@app.post("/matches/{match_id}/finalize")
async def finalize_match(match_id: int):
    result = fetch_match_result(settings.database_url, match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    if result.get("finalized"):
        return JSONResponse({"finalized": True})
    holes = fetch_hole_scores(settings.database_url, match_id)
    if not holes:
        raise HTTPException(status_code=400, detail="No hole data recorded for this match.")
    handicap_a = result.get("player_a_handicap") or 0
    handicap_b = result.get("player_b_handicap") or 0
    tournament_id = _tournament_id_for_result(result)
    tournament_settings = _load_tournament_settings(tournament_id)
    scorecard_data = _scorecard_data_for_match(result, holes, handicap_a, handicap_b, tournament_settings)
    course_info = _course_display_info(result, tournament_settings)
    total_points_a = scorecard_data["meta"]["total_points_a"]
    total_points_b = scorecard_data["meta"]["total_points_b"]
    outcome = score_outcome(total_points_a, total_points_b)
    update_match_result_scores(
        settings.database_url,
        match_id,
        player_a_points=total_points_a,
        player_b_points=total_points_b,
        player_a_bonus=outcome["player_a_bonus"],
        player_b_bonus=outcome["player_b_bonus"],
        player_a_total=outcome["player_a_total"],
        player_b_total=outcome["player_b_total"],
        winner=outcome["winner"],
    )
    finalize_match_result(
        settings.database_url,
        match_id,
        course_snapshot=course_info or {},
        scorecard_snapshot=scorecard_data,
    )
    _refresh_standings_cache(tournament_id)
    return JSONResponse({"finalized": True})


@app.post("/matches/{match_id}/reset")
async def reset_match_scores(match_id: int):
    result = fetch_match_result(settings.database_url, match_id)
    if not result:
        raise HTTPException(status_code=404, detail="Match not found")
    match_key = result.get("match_key")
    target_ids: set[int] = {match_id}
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
    return JSONResponse({"reset": True})


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
    match_result = fetch_match_result(settings.database_url, match_id)
    if match_result:
        tournament_id = match_result.get("tournament_id") or _get_active_tournament_id()
        _refresh_standings_cache(tournament_id)
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
        tournament_id = _get_active_tournament_id()
        tournament_settings = _load_tournament_settings(tournament_id)
        course_id = _safe_int(tournament_settings.get("course_id"))
        course_tee_id = _safe_int(tournament_settings.get("course_tee_id"))
        match_id = insert_match_result(
            settings.database_url,
            match_name=match_display(pairing),
            player_a=pairing.player_a,
            player_b=pairing.player_b,
            match_key=pairing.match_id,
            match_code=None,
            player_a_points=0,
            player_b_points=0,
            player_a_handicap=_player_handicap_by_name(pairing.player_a),
            player_b_handicap=_player_handicap_by_name(pairing.player_b),
            course_id=course_id,
            course_tee_id=course_tee_id,
            tournament_id=tournament_id,
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
    tournament_id = match_result.get("tournament_id") or _get_active_tournament_id()
    if tournament_id:
        _refresh_standings_cache(tournament_id)
    return JSONResponse({"added": len(cleaned)})


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
