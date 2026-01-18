# Match & Group Logic Map

This document summarizes where the existing match- and group-related logic lives so that you can understand the current signal helpers before reworking or removing them.

## Backend (app/main.py)
- **Active match context**: `_match_status_info`, `_scorecard_context`, `_summary`, and serialization helpers around lines 116–930 build the scorecard payload, compute match status, and service the `/scoring`, `/scorecard`, `/api/match_scorecard`, `/api/active_match`, and `/matches/...` endpoints.
- **Pairings & seeding**: `_load_pairings`, `_ensure_match_result_for_pairing`, `_resolve_match`, and `_ensure_match_results_for_pairings` depend on `app.seed` to provision `Match` objects and persist them through `insert_match_result` (lines 400–580 and 810–860). The `ActiveMatch` setting uses `_set_active_match_key`/`_get_active_match_key` to remember the selected match.
- **Score submission API**: `/submit`, `/api/scores`, and hole-score posting (`/matches/{match_id}/holes`, `/matches/key/{match_key}/holes`) are implemented after the startup hook (lines 980–1120 and 2245–2415). They rely on `insert_match_result`, `update_match_result_scores`, and `_refresh_standings_cache`.
- **Match detail and scorecard pages**: `/matches/{match_id}`, `/matches/{match_id}/scorecard`, `/matches/{match_id}/reset`, and `/matches/{match_id}/finalize` render `match_detail.html`/`scorecard_view.html` and update match/hole tables via `fetch_match_result`, `fetch_hole_scores`, and serialization helpers (lines 2245–2415).
- **Standings aggregation**: `build_standings` groups `fetch_all_match_results` into divisions and feeds `standings.html` (lines 640–720 and 2450–2458). It pulls `standings_cache` data populated by `_refresh_standings_cache`.

## Data Layer (app/db.py)
- **Match tables**: `match_results`, `hole_scores`, and `standings_cache` are created in `ensure_schema` (lines 14–158). `match_results` stores scores, winners, course snapshots, etc.; `hole_scores` keeps per-hole entries; `standings_cache` holds aggregated leaderboard rows for each tournament.
- **Match helpers**: `insert_match_result`, `fetch_match_result*`, `update_match_result_scores`, `reset_match_results`, `fetch_hole_scores`, and `delete_match_results_by_tournament` (lines 300–1150) orchestrate persistence and cleanup for every pairing.

## Templates & Static UX
- **scorecard & scoring UI**: `app/templates/scorecard_view.html`, `scoring.html`, `mobile_scoring.html`, `mobile_scoring_v2.html`, `new_card.html`, `match_detail.html`, `live_matches.html`, and `tournament_history.html` all expect match payloads, pairing dropdowns, or hole entry tables (match metadata, pairings, hole loop markup, etc.).
- **Admin pages**: `admin.html` shows recent match submissions, while `setup.html` still includes a “Pairings” tab that renders `_load_pairings()` results and the `Match` IDs, and `player_entry.html`/`tournament_setup.html` handle tournament and roster management.
- **Static assets**: `app/static/js/app.js` listens for `.match-dropdown`, populates match names, and submits `/api/scores`; `app/static/css/theme.css` includes `.match-*`, `.scorecard-*`, `.group-*`, and `.pairings-*` rules; `golf-ui/scoring.html` renders the mobile scoring view.

## Data flow summary
1. Player entries get persisted via `upsert_player` (app/main.py forms in `/admin/setup` and `/admin/player_entry`).
2. Pairings are generated with `build_pairings_from_players` (app/seed.py) and are used to seed match records via `_ensure_match_results_for_pairings`.
3. Matches are activated (`/api/active_match`) and the active key drives scoring/checkpoint flows (`_match_status_info`, `_scorecard_context`).
4. Submissions via `/api/scores` or `/matches/{match_id}/holes` persist hole data (`hole_scores`) and recalc standings cache to feed `/standings`.
5. Additional endpoints like `/matches/{match_id}/finalize` snapshot course/scorecard data, update totals, and flag winners.

Use this map to locate the functions and tables you will need to delete or replace when you strip out the match/group logic next.
