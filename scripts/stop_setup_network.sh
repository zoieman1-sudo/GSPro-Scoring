#!/usr/bin/env bash
set -euxo pipefail

SSID="${1:-GSPro-Setup}"

if ! command -v nmcli >/dev/null 2>&1; then
  echo "nmcli is required to manage the hotspot."
  exit 1
fi

nmcli connection down "$SSID" || true
nmcli connection delete "$SSID" || true
nmcli radio wifi on
echo "Setup network \"$SSID\" has been stopped."
