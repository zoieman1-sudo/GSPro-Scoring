#!/usr/bin/env bash
set -euxo pipefail

ACTION="${1:-start}"
ALIAS="${2:-gspro.local}"
PID_FILE="/tmp/gspro-mdns.pid"

function start_alias() {
  if ! command -v avahi-publish >/dev/null 2>&1; then
    echo "avahi-publish is required to publish the mDNS alias."
    exit 1
  fi
  IP="$(hostname -I | awk '{print $1}')"
  if [ -z "$IP" ]; then
    echo "Could not determine IP address to publish."
    exit 1
  fi
  avahi-publish -a "$ALIAS" "$IP" &
  echo $! > "$PID_FILE"
  echo "Published alias $ALIAS pointing to $IP"
}

function stop_alias() {
  if [ -f "$PID_FILE" ]; then
    kill "$(cat "$PID_FILE")" || true
    rm -f "$PID_FILE"
    echo "Stopped mDNS alias"
  fi
}

case "$ACTION" in
start)
  start_alias
  ;;
stop)
  stop_alias
  ;;
restart)
  stop_alias
  start_alias
  ;;
*)
  echo "usage: $0 [start|stop|restart] [alias]"
  exit 1
  ;;
esac
