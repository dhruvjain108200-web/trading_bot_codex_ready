
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

def now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def run_bot(project_root: str, mode: str = "paper") -> None:
    """
    Main entrypoint for the bot.
    In V1, this only wires bootstrap + prints mode.
    Later: instantiate Supervisor (Module 08) and start workers.
    """
    root = Path(project_root).resolve()
    print(f"[{now_ist()}] ✅ Bot starting. mode={mode} root={root}")

    # TODO (next implementation steps):
    # - from src.module08_supervisor.supervisor import Supervisor
    # - supervisor = Supervisor(root)
    # - supervisor.start(mode=mode)

    print(f"[{now_ist()}] ℹ️ Placeholder main.py running. Next: implement Supervisor + workers.")

def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project_root", type=str, required=True)
    ap.add_argument("--mode", type=str, default="paper", choices=["live", "paper", "replay"])
    args = ap.parse_args()
    run_bot(args.project_root, args.mode)

if __name__ == "__main__":
    cli()
