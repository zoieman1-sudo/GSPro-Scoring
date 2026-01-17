#!/usr/bin/env bash
set -euxo pipefail

SSID="${1:-GSPro-Setup}"
PASSWORD="${2:-gspro1234}"

if ! command -v nmcli >/dev/null 2>&1; then
  echo "nmcli is required to manage the hotspot."
  exit 1
fi

nmcli radio wifi on

if nmcli connection show "$SSID" >/dev/null 2>&1; then
  nmcli connection delete "$SSID"
fi

nmcli device wifi hotspot ssid "$SSID" password "$PASSWORD"
echo "Setup network \"$SSID\" is live (password: $PASSWORD)."
