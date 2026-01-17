#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROD_WORKTREE_DIR="${PROD_WORKTREE_DIR:-$ROOT_DIR/../GSPro-Scoring-master}"

DEV_PROJECT="${DEV_PROJECT:-gspro-dev}"
PROD_PROJECT="${PROD_PROJECT:-gspro-prod}"

DEV_APP_PORT="${DEV_APP_PORT:-18000}"

PROD_APP_PORT="${PROD_APP_PORT:-19000}"

start_dev="1"
start_prod="1"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--skip-prod] [--skip-dev]

Starts the dev stack from the current worktree (New-Scoring) and the prod stack from a
dedicated master worktree so both can run in parallel. Environment variables override
the exposed ports and compose project names if needed.

Options:
  --skip-prod     Only start the dev stack
  --skip-dev      Only start the prod stack
  --help          Show this message

Environment overrides:
  PROD_WORKTREE_DIR  Path to the prod worktree (default: $PROD_WORKTREE_DIR)
  DEV_PROJECT        Compose project used for the dev stack (default: $DEV_PROJECT)
  PROD_PROJECT       Compose project used for the prod stack (default: $PROD_PROJECT)
  DEV_APP_PORT       HTTP port for dev app (default: $DEV_APP_PORT)
  PROD_APP_PORT      HTTP port for prod app (default: $PROD_APP_PORT)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-prod) start_prod="0"; shift ;;
    --skip-dev) start_dev="0"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not installed" >&2
  exit 1
fi

ensure_prod_worktree() {
  if [ -d "$PROD_WORKTREE_DIR" ]; then
    echo "Using existing prod worktree at $PROD_WORKTREE_DIR"
    return
  fi

  echo "Creating prod (master) worktree at $PROD_WORKTREE_DIR"
  git -C "$ROOT_DIR" fetch origin master >/dev/null 2>&1 || true
  if git -C "$ROOT_DIR" show-ref --verify --quiet refs/heads/master; then
    git -C "$ROOT_DIR" worktree add "$PROD_WORKTREE_DIR" master
  elif git -C "$ROOT_DIR" show-ref --verify --quiet refs/remotes/origin/master; then
    git -C "$ROOT_DIR" worktree add "$PROD_WORKTREE_DIR" origin/master
  else
    echo "master branch not found locally or on origin" >&2
    exit 1
  fi
}

run_stack() {
  local stack_dir="$1"
  local project="$2"
  local app_port="$3"

  echo "Bringing up $project stack from $stack_dir (app:$app_port)"
  (
    cd "$stack_dir"
    COMPOSE_PROJECT_NAME="$project" \
      APP_HOST_PORT="$app_port" \
      docker compose up --build -d
  )
}

if [ "$start_prod" = "1" ]; then
  ensure_prod_worktree
  run_stack "$PROD_WORKTREE_DIR" "$PROD_PROJECT" "$PROD_APP_PORT"
fi

if [ "$start_dev" = "1" ]; then
  run_stack "$ROOT_DIR" "$DEV_PROJECT" "$DEV_APP_PORT"
fi
