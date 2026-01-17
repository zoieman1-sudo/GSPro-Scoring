# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create venv and install requirements from `requirements.txt`.
- Set `DATABASE_URL` and `SCORING_PIN` (see `.env.example`); by default the app uses the bundled SQLite file (`sqlite:///app/DATA/gspro_scoring.db`), so no external database service is required.
- Run `python -m app.seed_db` once before starting the server to create the schema and a baseline tournament so matches can be activated without hitting missing-table errors.
- Run `python -m app.demo_seed` if you want the demo tournament and players seeded before you open the UI; the FastAPI startup already executes this, but rerunning the script refreshes the demo fixtures.
- Run FastAPI app locally with `python -m app.server`, the launcher that wraps `uvicorn` and toggles HTTPS when `SSL_CERT_FILE`/`SSL_KEY_FILE` point at valid PEM files.
- Submit a few match scores via the primary scoring experience (`/` or `/scoring`); the `/matches/{id}` view lets you inspect hole-by-hole entries stored in `hole_scores`.
- Visit `/standings` to see the updated division leaderboard rendered as the familiar tabular format used in the `dashboard` sheet from `10-man_sim_golf_tournament_scoring_sheet_v2_single_round_robin_playoffs.xlsx`.

## Docker
- Copy `.env.example` to `.env` and update `DATABASE_URL` or `SCORING_PIN` if needed; by default the app uses the bundled SQLite file in `app/DATA`.
- `docker compose up --build` launches only the `app` service, which binds `app/DATA` into the container so the SQLite database file persists between runs.
- The app still seeds the demo tournament via `app.demo_seed` on startup, but you can `docker compose exec app python -m app.demo_seed` if you need to refresh that fixture.
- The app publishes on port `18000` (`http://localhost:18000`). Pass `SSL_CERT_FILE`/`SSL_KEY_FILE` (and optionally `SSL_CA_FILE`/`SSL_KEY_PASSWORD`) through `.env` when you need HTTPS.

## Running prod and dev simultaneously

The dev worktree (typically checked out to `New-Scoring`) can run beside a `master` worktree by way of `scripts/run-prod-dev-stacks.sh`. It boots two Compose projects — `gspro-dev` for the current worktree (`New-Scoring`) and `gspro-prod` for a dedicated `master` worktree — so you can iterate on the dev branch while the production stack keeps using the stable `master` branch.

1. From the `New-Scoring` worktree, run `./scripts/run-prod-dev-stacks.sh`.
2. The script mirrors the `master` branch into `../GSPro-Scoring-master` (override `PROD_WORKTREE_DIR` if you prefer a different path) and starts `gspro-dev` plus `gspro-prod` projects.
3. Default host bindings are `18000`/`15433` for dev and `19000`/`15434` for prod; override those with `DEV_APP_PORT`, `DEV_DB_PORT`, `PROD_APP_PORT`, or `PROD_DB_PORT` before launching.
4. Pass `--skip-prod` or `--skip-dev` when only one stack is needed, and use `PROD_PGADMIN_PORT`/`DEV_PGADMIN_PORT` when bringing up pgAdmin via `docker compose --profile pgadmin up -d`.

Each stack is tied to its Compose project name, so container identifiers keep the `gspro-dev`/`gspro-prod` prefixes instead of the old single `gspro-scoring` name that blocked parallel runs. You can also start the stacks manually by setting the shared `APP_HOST_PORT`, `DB_HOST_PORT`, and `PGADMIN_HOST_PORT` variables per compose invocation.

## Admin
- Recent submissions: `/admin?pin=YOUR_PIN`
- Tournament setup now lives on its own page (`/admin/setup?pin=YOUR_PIN`), where seed data from the default match list populates the roster and lets you name players, assign divisions, set handicaps, and define a seeding order per division (1..N). Everything is stored in the bundled Postgres container so the scoring and standings pages stay accurate.
- Use `/admin/player_entry` to register players with names, handicaps, seeds, and divisions. Each match now pairs two-player teams (four people total) on `/admin/match_setup` before you launch Scorecard Studio via the Golf UI link for live scoring.
- The UI now uses a darker, high-contrast palette tuned for larger displays, matching the TV-friendly columnar layout from the official Excel dashboard so standings tables remain legible from across a room.

## Matches & hole scoring
- `/matches` displays every recorded match with totals, while `/matches/{id}` exposes the hole scores captured in the `hole_scores` table.
- `/scoring` now includes the drop-down matched to the Excel-style scorecard: after you select a pairing it shows the hole-by-hole list, the auto-net math, and HUD scores plus the base-point form still submits to the leaderboard.  
- `/scorecard_studio` restores the Scorecard Studio layout and surfaces the first two live matches (with hole references and course meta) so the broadcast dashboard is always accessible from the app.
- The hole form lets you stage multiple entries before submitting; each hole stores the pair of scores so you can populate the scoreboard exactly as the Excel “Match Results” section expects.

## Tests
- Install `requirements-dev.txt` and run `pytest`.
- CI is handled via `.github/workflows/ci.yml`, which runs `pytest` on every `push`/`pull_request` to `main` so the scoring logic stays covered.

## Notes
- The application now persists all data in `app/DATA/gspro_scoring.db`; delete that file (or set `DATABASE_URL` to a different `sqlite:///` path) to reset the state.
- The `/standings/kiosk/leaderboard` view now seeds and reads from a lightweight SQLite store located at `app/DATA/kiosk_leaderboard.db`, so it can render without connecting to any external service.

## HTTPS support

Set `SSL_CERT_FILE` and `SSL_KEY_FILE` to PEM files you control and the `python -m app.server` entry point hands those files to `uvicorn` as the TLS certificate and key. You can also expose an intermediate CA bundle via `SSL_CA_FILE` or unlock encrypted keys with `SSL_KEY_PASSWORD`.

For a quick self-signed cert:

```bash
openssl req -x509 -nodes -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -subj "/CN=localhost"
SSL_CERT_FILE=server.crt SSL_KEY_FILE=server.key python -m app.server
```

If you prefer the previous `uvicorn` CLI, pass `--ssl-certfile`/`--ssl-keyfile` manually (`uvicorn app.main:app ... --ssl-certfile server.crt --ssl-keyfile server.key`). The new module simply wraps the same arguments so other tooling keeps working.

This repo is initialized by Codex and follows the canonical context provided by the user.
