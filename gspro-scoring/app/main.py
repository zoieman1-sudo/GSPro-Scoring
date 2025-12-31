from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError

from app.db import ensure_schema, fetch_recent_results, insert_match_result
from app.seed import get_match_by_id, get_matches, match_display
from app.settings import load_settings, score_outcome

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
settings = load_settings()
matches = get_matches()


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


@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request, pin: str = ""):
    if pin != settings.scoring_pin:
        return templates.TemplateResponse(
            "admin.html",
            {"request": request, "authorized": False, "results": []},
        )

    results = fetch_recent_results(settings.database_url, limit=20)
    return templates.TemplateResponse(
        "admin.html",
        {"request": request, "authorized": True, "results": results},
    )
