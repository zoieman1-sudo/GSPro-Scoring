# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create venv and install requirements from `requirements.txt`.
- Set `DATABASE_URL` and `SCORING_PIN` (see `.env.example`).
- Run FastAPI app locally with uvicorn.

## Docker
- Copy `.env.example` to `.env` and update values.
- `docker compose up --build`

## Admin
- Recent submissions: `/admin?pin=YOUR_PIN`

## Tests
- Install `requirements-dev.txt` and run `pytest`.

## Notes
- Postgres is assumed to run on the host and is not containerized.
- The app creates the `match_results` table on startup if it does not exist.

This repo is initialized by Codex and follows the canonical context provided by the user.
