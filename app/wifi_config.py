from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

WIFI_CONFIG_PATH = Path(__file__).resolve().parent / "DATA" / "wifi_config.json"
STOP_NETWORK_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "stop_setup_network.sh"


def _ensure_directory() -> None:
    WIFI_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_wifi_config() -> dict[str, Any]:
    if not WIFI_CONFIG_PATH.exists():
        return {}
    try:
        with WIFI_CONFIG_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        return {}


def save_wifi_config(ssid: str, password: str | None, hidden: bool) -> dict[str, Any]:
    _ensure_directory()
    payload = {
        "ssid": ssid,
        "password": password or "",
        "hidden": hidden,
        "updated_at": datetime.utcnow().isoformat(),
    }
    with WIFI_CONFIG_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    if STOP_NETWORK_SCRIPT.exists():
        try:
            subprocess.run([str(STOP_NETWORK_SCRIPT)], check=False)
        except Exception:
            pass
    return payload
