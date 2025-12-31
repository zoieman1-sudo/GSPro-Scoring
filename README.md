# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create venv and install requirements from `requirements.txt`.
- Set `DATABASE_URL` and `SCORING_PIN` (see `.env.example`).
- Run FastAPI app locally with uvicorn.
- Visit `/standings` in your browser after recording scores to see the aggregated leaderboard for each division.

## Docker
- Copy `.env.example` to `.env` and update values as needed (_defaults point at the bundled Postgres service_).
- `docker compose up --build` (this now starts a `postgres:15` service with the configured password so the app can connect immediately).

## Admin
- Recent submissions: `/admin?pin=YOUR_PIN`
- Tournament setup now lives on its own page (`/admin/setup?pin=YOUR_PIN`), where seed data from the default match list populates the roster and lets you name players, assign divisions, set handicaps, and define a seeding order per division (1..N). Everything is stored in the bundled Postgres container so the scoring and standings pages stay accurate.

## Tests
- Install `requirements-dev.txt` and run `pytest`.
- CI is handled via `.github/workflows/ci.yml`, which runs `pytest` on every `push`/`pull_request` to `main` so the scoring logic stays covered.

## Notes
- Our Compose stack now includes a Postgres service configured with `postgres/change_me` and `gspro_scoring`; update `DATABASE_URL` if you want to point at some other database.
- The app creates the `match_results` table on startup if it does not exist.

This repo is initialized by Codex and follows the canonical context provided by the user.
