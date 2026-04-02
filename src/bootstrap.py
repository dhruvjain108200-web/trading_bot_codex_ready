
from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

def now_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def ensure_dirs(project_root: Path) -> None:
    required = [
        "config", "state", "logs", "lists", "mappings", "instruments",
        "analytics", "data", "src", "lib_vault"
    ]
    for r in required:
        (project_root / r).mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception:
        return default

def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)

def load_secrets_colab() -> dict:
    """
    Uses Colab Secrets (recommended).
    In Colab: Runtime → Secrets → add keys.
    """
    secrets = {}
    try:
        from google.colab import userdata  # type: ignore
        # Add keys as you create them in Colab Secrets:
        # ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PIN, ANGEL_TOTP_SECRET
        for k in ["ANGEL_API_KEY", "ANGEL_CLIENT_ID", "ANGEL_PIN", "ANGEL_TOTP_SECRET",
                  "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]:
            try:
                v = userdata.get(k)
                if v:
                    secrets[k] = v
            except Exception:
                pass
    except Exception:
        pass
    return secrets

def bootstrap(project_root: str) -> dict:
    """
    Bootstraps the project:
    - ensures folder structure
    - loads configs
    - loads secrets from Colab Secrets
    - initializes basic state files if missing
    """
    root = Path(project_root).expanduser().resolve()
    ensure_dirs(root)

    runtime_cfg = load_json(root / "config/runtime_config.json", default={})
    backtest_cfg = load_json(root / "config/backtest_config.json", default={})
    replay_cfg = load_json(root / "config/replay_config.json", default={})

    secrets = load_secrets_colab()

    # Initialize system state if missing
    system_state_path = root / "state/system_state.json"
    system_state = load_json(system_state_path, default={})
    if not system_state:
        system_state = {
            "system_state": "STOPPED",
            "trade_gate": "PAUSE",
            "last_menu_id": None,
            "ts_ist": now_ist_str()
        }
        save_json(system_state_path, system_state)

    # heartbeat
    hb_path = root / "state/runtime_heartbeat.json"
    hb = load_json(hb_path, default={})
    if not hb:
        hb = {"ts_ist": now_ist_str(), "status": "BOOTSTRAPPED"}
        save_json(hb_path, hb)

    return {
        "project_root": str(root),
        "runtime_config": runtime_cfg,
        "backtest_config": backtest_cfg,
        "replay_config": replay_cfg,
        "secrets_loaded": sorted(list(secrets.keys())),
        "secrets": secrets,  # keep in memory; do not write to disk
    }
