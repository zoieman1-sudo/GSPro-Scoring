#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$ROOT/.venv"
WIFI_CONFIG="$ROOT/app/DATA/wifi_config.json"
SETUP_SCRIPT="$ROOT/scripts/start_setup_network.sh"
MDNS_SCRIPT="$ROOT/scripts/publish_mdns.sh"
MDNS_ALIAS="${GSCORES_HOST:-gspro.local}"

if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
fi

source "$VENV/bin/activate"
pip install --upgrade pip
pip install -r "$ROOT/requirements.txt"

export PYTHONPATH="$ROOT"
python scripts/create_schema.py
if [ ! -s "$WIFI_CONFIG" ] && [ -x "$SETUP_SCRIPT" ]; then
  echo "No Wi-Fi configuration detected; spinning up the setup network..."
  "$SETUP_SCRIPT"
fi

if [ -x "$MDNS_SCRIPT" ]; then
  "$MDNS_SCRIPT" start "$MDNS_ALIAS" || true
fi

exec python -m app.server
