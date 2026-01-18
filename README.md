# GSPro Tournament Scoring App

Starter scaffold for the GSPro Tournament Scoring App (FastAPI + Jinja2).

## Quick start
- Create a venv and install requirements from `requirements.txt`.
- Configure `DATABASE_URL`/`SCORING_PIN` (see `.env.example` if you run outside Docker); the app ignores Postgres/MySQL URLs and always writes to the bundled SQLite store (`sqlite:///app/DATA/gspro_scoring.db`) when you use the provided Compose file.
- Run `python scripts/create_schema.py` to seed the bundled SQLite schema, emit the SQL DDL for reference, and build the default “GSPro League” tournament.
- Optionally run `python -m app.demo_seed` if you want the demo tournament and players refreshed before opening the UI; startup already triggers the fixture, but rerunning the script can reset the demo state.
- Run FastAPI locally with `python -m app.server`, which wraps `uvicorn` and toggles HTTPS when `SSL_CERT_FILE`/`SSL_KEY_FILE` point at valid PEM files.
- Submit a few match scores via the primary scoring experience (`/` or `/scoring`); the `/matches/{id}` view lets you inspect hole-by-hole entries stored in `hole_scores`.
- Visit `/standings` to see the updated division leaderboard rendered as the familiar tabular format used in the `dashboard` sheet from `10-man_sim_golf_tournament_scoring_sheet_v2_single_round_robin_playoffs.xlsx`.

## Docker
- Copy `.env.example` to `.env` and adjust `SCORING_PIN` if needed; the Docker stack now hardcodes `DATABASE_URL=sqlite:///app/DATA/gspro_scoring.db`, so the Postgres path is ignored and the SQLite file is shared through `app/DATA`.
- `docker compose up --build` launches only the `app` service, which binds `app/DATA` into the container so the SQLite database file persists between runs.
- The app still seeds the demo tournament via `app.demo_seed` on startup, but you can `docker compose exec app python -m app.demo_seed` if you need to refresh that fixture.
- The app publishes on port `18000` (`http://localhost:18000`). Pass `SSL_CERT_FILE`/`SSL_KEY_FILE` (and optionally `SSL_CA_FILE`/`SSL_KEY_PASSWORD`) through `.env` when you need HTTPS.

## Schema and inspecting data
- Run `python scripts/create_schema.py` to seed the bundled schema, print the SQL DDL, and ensure the default `GSPro League` tournament exists without touching Postgres.
- Inspect the live SQLite file with the CLI (`sqlite3 app/DATA/gspro_scoring.db`) or install a GUI like DB Browser for SQLite (`sudo apt install sqlitebrowser`), remembering that Docker already binds `app/DATA` so `/app/DATA/gspro_scoring.db` is visible on the host.
- The kiosk leaderboard uses its own sandboxed store at `app/DATA/kiosk_leaderboard.db`, and the kiosk views seed this file automatically when they are rendered.

## Raspberry Pi appliance (baked image)
- Copy the entire repository onto the Pi’s SD card (e.g., `/opt/gspro`) so `app/`, `golf-ui/`, `start.sh`, and the SQLite files live together.
- Run `./start.sh` once to create the `.venv`, install Python dependencies, seed `gspro_scoring.db`, and launch the server; the script can also be wired into a plain shell shortcut or kiosk browser as the “run artifact.”
- Drop `scripts/gspro.service` into `/etc/systemd/system/gspro.service`, then enable it with `sudo systemctl enable gspro.service` so the Pi boots straight into the FastAPI server on port 18000.
- Optionally launch a kiosk browser pointing at `http://localhost:18000/standings` to make the Pi look like a TV-style scoreboard as soon as it finishes booting.
- When the Pi boots for the first time (or if it loses Wi-Fi), run `scripts/start_setup_network.sh` (or include it in the appliance startup) to bring up a temporary open SSID (default `GSPro-Setup`/`gspro1234`) and point your phone/tablet at `http://192.168.4.1/setup/wifi`. After saving the credentials, run `scripts/stop_setup_network.sh` to tear the hotspot down.
- The `start.sh` helper now detects when `app/DATA/wifi_config.json` is empty and automatically launches `scripts/start_setup_network.sh` before the FastAPI server starts, so the first boot brings up the setup SSID. Once `/setup/wifi` saves the network, the handler will call `scripts/stop_setup_network.sh`, allowing the Pi to join the configured network without manual hostapd interaction.
- The appliance also publishes an mDNS alias (default `gspro.local`) using `scripts/publish_mdns.sh`, so the scoreboard is reachable at `http://gspro.local:18000/standings` or any alias you pass via the `GSCORES_HOST` environment variable on boot. Install `avahi-daemon`/`avahi-utils` so `avahi-publish` can run, or tweak the script to register whatever domain (`gspro.scoring.com`) your network prefers.
- Visit `/setup/wifi` after the server is running to store your SSID/passphrase locally in `app/DATA/wifi_config.json`; the service will remind operators to apply those credentials using the OS-level Wi-Fi manager on the Pi.
- When following Workflow B (buildable image with `pi-gen`), copy the repo into `pi-gen/gspro-scoring`, add this provisioning stage (`stage2/05-gspro/00-run.sh`), and call `/opt/gspro/scripts/provision.sh` during the build so the resultant `.img` already contains the service, scripts, and prerequisites. The provisioning script installs `NetworkManager`, `avahi`, and others, copies the app into `/opt/gspro`, enables the `gspro.service`, and links `start.sh` under `/usr/local/bin` for a clean boot.
- The provisioning script now sets `DEBIAN_FRONTEND=noninteractive`, installs `locales`, and runs `locale-gen en_US.UTF-8`/`update-locale` so the build no longer emits “cannot set LC_*” warnings inside pi-gen. This keeps `apt` from trying to launch interactive dialogs during the image build.

## Running prod and dev simultaneously

The dev worktree (typically checked out to `New-Scoring`) can run beside a `master` worktree by way of `scripts/run-prod-dev-stacks.sh`. It boots two Compose projects — `gspro-dev` for the current worktree (`New-Scoring`) and `gspro-prod` for a dedicated `master` worktree — so you can iterate on the dev branch while the production stack keeps using the stable `master` branch.

1. From the `New-Scoring` worktree, run `./scripts/run-prod-dev-stacks.sh`.
2. The script mirrors the `master` branch into `../GSPro-Scoring-master` (override `PROD_WORKTREE_DIR` if you prefer a different path) and starts `gspro-dev` plus `gspro-prod` projects.
3. Default host bindings are `18000`/`15433` for dev and `19000`/`15434` for prod; override those with `DEV_APP_PORT`, `DEV_DB_PORT`, `PROD_APP_PORT`, or `PROD_DB_PORT` before launching.
4. Pass `--skip-prod` or `--skip-dev` when only one stack is needed, and use `PROD_PGADMIN_PORT`/`DEV_PGADMIN_PORT` when bringing up pgAdmin via `docker compose --profile pgadmin up -d`.

Each stack is tied to its Compose project name, so container identifiers keep the `gspro-dev`/`gspro-prod` prefixes instead of the old single `gspro-scoring` name that blocked parallel runs. You can also start the stacks manually by setting the shared `APP_HOST_PORT`, `DB_HOST_PORT`, and `PGADMIN_HOST_PORT` variables per compose invocation.

## Admin
- Recent submissions: `/admin?pin=YOUR_PIN`
- Tournament setup now lives on its own page (`/admin/setup?pin=YOUR_PIN`), where seed data from the default match list populates the roster and lets you name players, assign divisions, set handicaps, and define a seeding order per division (1..N). Everything is stored in the bundled SQLite file (`app/DATA/gspro_scoring.db`), so the scoring and standings pages stay accurate without an external database.
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
