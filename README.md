# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create venv and install requirements from `requirements.txt`.
- Set `DATABASE_URL` and `SCORING_PIN` (see `.env.example`).
- Run FastAPI app locally with uvicorn.
- Submit a few match scores via `/` or `/matches`; the `/matches` view summaries each encounter and lets you drill into `/matches/{id}` for hole-by-hole score entry.
- Visit `/standings` to see the updated division leaderboard rendered as the familiar tabular format used in the `dashboard` sheet from `10-man_sim_golf_tournament_scoring_sheet_v2_single_round_robin_playoffs.xlsx`.

## Docker
- Copy `.env.example` to `.env` and update values as needed (_defaults point at the bundled Postgres service_).
- `docker compose up --build` (this now starts a `postgres:15` service with the configured password so the app can connect immediately).  
- The app now publishes on port `18000` (`http://localhost:18000`) and Postgres is exposed as `localhost:15433`; if you connect from an external tool (pgAdmin, psql, etc.) use those host ports when specifying the service.

## Admin
- Recent submissions: `/admin?pin=YOUR_PIN`
- Tournament setup now lives on its own page (`/admin/setup?pin=YOUR_PIN`), where seed data from the default match list populates the roster and lets you name players, assign divisions, set handicaps, and define a seeding order per division (1..N). Everything is stored in the bundled Postgres container so the scoring and standings pages stay accurate.
- The UI now uses a darker, high-contrast palette tuned for larger displays, matching the TV-friendly columnar layout from the official Excel dashboard so standings tables remain legible from across a room.

## Matches & hole scoring
- `/matches` displays every recorded match with totals, while `/matches/{id}` exposes the hole scores captured in the `hole_scores` table.
- The hole form lets you stage multiple entries before submitting; each hole stores the pair of scores so you can populate the scoreboard exactly as the Excel “Match Results” section expects.

## Tests
- Install `requirements-dev.txt` and run `pytest`.
- CI is handled via `.github/workflows/ci.yml`, which runs `pytest` on every `push`/`pull_request` to `main` so the scoring logic stays covered.

## Notes
- Our Compose stack now includes a Postgres service configured with `postgres/change_me` and `gspro_scoring`; update `DATABASE_URL` if you want to point at some other database.
- The app creates the `match_results` table on startup if it does not exist.

This repo is initialized by Codex and follows the canonical context provided by the user.
