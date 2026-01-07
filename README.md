# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create venv and install requirements from `requirements.txt`.
- Set `DATABASE_URL` and `SCORING_PIN` (see `.env.example`); only the admin/admin setup pages still require the PIN, while scoring submissions are open for now.
- Run `python -m app.seed_db` once before starting the server to create the schema and a baseline tournament so matches can be activated without hitting missing-table errors.
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
- `/scoring` now includes the drop-down matched to the Excel-style scorecard: after you select a pairing it shows the hole-by-hole list, the auto-net math, and HUD scores plus the base-point form still submits to the leaderboard.  
- The hole form lets you stage multiple entries before submitting; each hole stores the pair of scores so you can populate the scoreboard exactly as the Excel “Match Results” section expects.

## Tests
- Install `requirements-dev.txt` and run `pytest`.
- CI is handled via `.github/workflows/ci.yml`, which runs `pytest` on every `push`/`pull_request` to `main` so the scoring logic stays covered.

## Notes
- Our Compose stack now includes a Postgres service configured with `postgres/change_me` and `gspro_scoring`; update `DATABASE_URL` if you want to point at some other database.
- The app creates the `match_results` table on startup if it does not exist.
- pgAdmin is now attached to the `pgadmin` profile, so start it via `docker compose --profile pgadmin up -d` and browse `http://localhost:5050` (login `pgadmin@gspro.local`/`change_me`). Connect to `host=db`, port `5432`, database `gspro_scoring` and you’ll have GUI access to `hole_scores`, `match_results`, etc.

This repo is initialized by Codex and follows the canonical context provided by the user.
