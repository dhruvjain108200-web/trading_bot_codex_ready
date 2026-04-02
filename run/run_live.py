from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def _looks_like_project_root(path: Path) -> bool:
    return (
        (path / "src").exists()
        and (path / "run").exists()
        and (path / "lists").exists()
        and (path / "config").exists()
        and (path / "mappings").exists()
    )


def _resolve_project_root(requested_root: Path) -> Path:
    candidates: List[Path] = [requested_root, requested_root / "trading_bot", requested_root.parent]

    try:
        for child in requested_root.iterdir():
            if child.is_dir():
                candidates.append(child)
    except Exception:
        pass

    seen = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        if _looks_like_project_root(candidate):
            return candidate

    return requested_root


def _load_secret(
    bootstrap_ctx: Dict[str, Any],
    name: str,
    *,
    aliases: Optional[List[str]] = None,
    required: bool = True,
) -> str:
    aliases = aliases or []
    secrets = bootstrap_ctx.get("secrets", {}) or {}

    for key in [name] + aliases:
        value = os.environ.get(key)
        if value:
            return str(value)

        secret_value = secrets.get(key)
        if secret_value:
            return str(secret_value)

    if required:
        alias_text = f" (aliases: {', '.join(aliases)})" if aliases else ""
        raise RuntimeError(f"Missing required secret: {name}{alias_text}")
    return ""


class ProjectJsonProvider:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.path_map = {
            "watchlist.json": self.root / "lists" / "watchlist.json",
            "potential_stocks.json": self.root / "lists" / "potential_stocks.json",
            "daily_movers_cache.json": self.root / "lists" / "daily_movers_cache.json",
            "daily_losers_cache.json": self.root / "lists" / "daily_losers_cache.json",
            "manual_additions.json": self.root / "lists" / "manual_additions.json",
            "discovered_symbols.json": self.root / "lists" / "discovered_symbols.json",
            "previous_day_movers.json": self.root / "lists" / "previous_day_movers.json",
            "previous_day_losers.json": self.root / "lists" / "previous_day_losers.json",
            "index_taxonomy.json": self.root / "lists" / "index_taxonomy.json",
            "runtime_config.json": self.root / "config" / "runtime_config.json",
            "symbol_to_token.json": self.root / "mappings" / "symbol_to_token.json",
            "token_to_symbol.json": self.root / "mappings" / "token_to_symbol.json",
            "watchlist_tokens.json": self.root / "mappings" / "watchlist_tokens.json",
            "index_tokens.json": self.root / "mappings" / "index_tokens.json",
        }

    def read_json(self, name: str) -> Any:
        path = self.path_map.get(name)
        if not path or not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


class DryRunTelegramSender:
    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def send_message(
        self,
        text: str,
        parse_mode: Optional[str] = None,
        disable_notification: bool = False,
        chat_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        message_id = len(self.sent) + 1
        self.sent.append(
            {
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": disable_notification,
                "chat_id": chat_id,
            }
        )
        return {"ok": True, "result": {"message_id": message_id}}

    def delete_message(self, message_id: int) -> Dict[str, Any]:
        return {"ok": True, "result": True, "message_id": int(message_id)}

    def get_updates(
        self,
        offset: Optional[int] = None,
        timeout: int = 2,
        allowed_updates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        return []


def build_live_runtime(project_root: Path, *, telegram_mode: str) -> Dict[str, Any]:
    src_root = project_root / "src"
    if not src_root.exists():
        raise RuntimeError(f"src folder not found at: {src_root}")

    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    from bootstrap import bootstrap
    from module01_broker.broker_connection import BrokerConnection
    from module02_ws.module_02_websocket_v2_pipeline import (
        WebSocketV2Pipeline,
        load_production_index_tokens,
        load_token_to_symbol_map,
    )
    from module03_context.module_03_market_context_monitor import MarketContextMonitor
    from module04_scanner.module_04_stock_universe_builder_scanner import (
        SimpleMarketContextProvider,
        StockUniverseBuilderScanner,
    )
    from module05_strategy.module_05_strategy_pattern_tradeplan_engine import (
        Module05StrategyPatternTradePlanEngine,
    )
    from module06_alerts.module_06_alert_engine import AlertEngine, AlertEngineConfig
    from module07_maintenance.maintenance_worker import BotPaths, MaintenanceWorker
    import module08_supervisor.module_08_supervisor_orchestrator as module08_supervisor

    bootstrap_ctx = bootstrap(str(project_root))

    api_key = _load_secret(bootstrap_ctx, "ANGEL_API_KEY")
    client_code = _load_secret(
        bootstrap_ctx,
        "ANGEL_CLIENT_ID",
        aliases=["ANGEL_CLIENT_CODE"],
    )
    pin = _load_secret(bootstrap_ctx, "ANGEL_PIN")
    totp_secret = _load_secret(bootstrap_ctx, "ANGEL_TOTP_SECRET")

    use_real_telegram = telegram_mode == "real"
    if telegram_mode == "auto":
        token_present = bool(_load_secret(bootstrap_ctx, "TELEGRAM_TOKEN", required=False))
        chat_present = bool(_load_secret(bootstrap_ctx, "TELEGRAM_CHAT_ID", required=False))
        use_real_telegram = token_present and chat_present

    telegram_bot_token = "DRYRUN"
    telegram_chat_id = "DRYRUN"
    if use_real_telegram:
        telegram_bot_token = _load_secret(bootstrap_ctx, "TELEGRAM_TOKEN")
        telegram_chat_id = _load_secret(bootstrap_ctx, "TELEGRAM_CHAT_ID")

    runtime_root = project_root / "runtime" / "live_runner"
    alert_root = runtime_root / "alert_engine"
    broker_state_root = runtime_root / "broker_state"

    runtime_root.mkdir(parents=True, exist_ok=True)
    broker_state_root.mkdir(parents=True, exist_ok=True)

    def _broker_alert(message: str, severity: str = "WARN", extra: Optional[Dict[str, Any]] = None) -> None:
        payload = json.dumps(extra or {}, sort_keys=True)
        print(f"[module01:{severity}] {message} | {payload}")

    def _module07_alert(level: str, title: str, message: str, tags: Optional[List[str]] = None) -> None:
        print(f"[module07:{level}] {title} | {message} | tags={tags or []}")

    broker = BrokerConnection(
        api_key=api_key,
        client_code=client_code,
        pin=pin,
        totp_secret=totp_secret,
        instrument_store_dir=str(project_root / "data" / "instruments"),
        state_dir=str(broker_state_root),
        alert_callback=_broker_alert,
    )
    broker.ensure_logged_in()

    provider = ProjectJsonProvider(project_root)
    token_to_symbol = load_token_to_symbol_map(str(project_root))
    index_tokens, live_index_to_token = load_production_index_tokens(str(project_root))

    ws_pipeline = WebSocketV2Pipeline(
        broker_context=broker.get_context(),
        token_to_symbol=token_to_symbol,
    )
    ws_pipeline.set_index_tokens(index_tokens)

    module03 = MarketContextMonitor(
        snapshot_source=ws_pipeline,
        taxonomy_path=str(project_root / "lists" / "index_taxonomy.json"),
        event_calendar_path=str(project_root / "data" / "event_calendar.json"),
    )

    scanner = StockUniverseBuilderScanner(
        broker=broker,
        pipeline=ws_pipeline,
        market_context_provider=SimpleMarketContextProvider(module03),
        maintenance_provider=provider,
        candle_provider=broker,
        config={},
    )

    strategy_engine = Module05StrategyPatternTradePlanEngine()

    alert_shared_state: Dict[str, Any] = {}
    alert_engine = AlertEngine(
        config=AlertEngineConfig(
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            journal_dir=str(alert_root / "journal"),
            state_dir=str(alert_root / "state"),
            deletion_registry_dir=str(alert_root / "deletion_registry"),
            log_dir=str(alert_root / "logs"),
        ),
        snapshot_getter=lambda: ws_pipeline.snapshot_store,
        market_context_getter=module03.get_state,
        health_getter=lambda: {
            "broker": broker.health(),
            "websocket": {
                "ws_connected": bool(getattr(ws_pipeline.health, "ws_connected", False)),
                "state": str(getattr(ws_pipeline.health, "state", "UNKNOWN")),
                "subscribed_count": int(getattr(ws_pipeline.health, "subscribed_count", 0) or 0),
            },
        },
        shared_state=alert_shared_state,
    )
    if not use_real_telegram:
        alert_engine.telegram = DryRunTelegramSender()

    maintenance = MaintenanceWorker(
        paths=BotPaths(str(project_root)),
        broker_ready_fn=lambda: (True, "broker logged in"),
        ws_ready_fn=lambda: (
            bool(getattr(ws_pipeline.health, "ws_connected", False)),
            str(getattr(ws_pipeline.health, "state", "UNKNOWN")),
        ),
        alert_fn=_module07_alert,
        market_lists_provider=None,
    )
    readiness_ok, readiness_reasons = maintenance.run_readiness_checks()
    if not readiness_ok:
        raise RuntimeError(
            "Module 07 readiness failed: "
            + ("; ".join(readiness_reasons) if readiness_reasons else "unknown")
        )

    module08_supervisor.STATE_FILE = str(runtime_root / "state" / "system_state.json")
    supervisor = module08_supervisor.Supervisor(
        broker=broker,
        ws_pipeline=ws_pipeline,
        module03=module03,
        scanner=scanner,
        strategy_engine=strategy_engine,
        alert_engine=alert_engine,
        maintenance=maintenance,
    )

    return {
        "project_root": project_root,
        "module03": module03,
        "alert_engine": alert_engine,
        "supervisor": supervisor,
        "ws_pipeline": ws_pipeline,
        "live_index_to_token": live_index_to_token,
        "telegram_mode": "real" if use_real_telegram else "dry-run",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Live runner for the modular trading bot.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to the trading_bot project root.",
    )
    parser.add_argument(
        "--telegram-mode",
        choices=("auto", "real", "dry-run"),
        default="auto",
        help="Use real Telegram if configured, or dry-run alerts.",
    )
    parser.add_argument(
        "--module03-warmup-sec",
        type=float,
        default=1.0,
        help="Optional warm-up time after starting Module 03.",
    )
    args = parser.parse_args()

    requested_root = Path(args.project_root).resolve()
    project_root = _resolve_project_root(requested_root)
    runtime = build_live_runtime(project_root, telegram_mode=args.telegram_mode)

    module03 = runtime["module03"]
    alert_engine = runtime["alert_engine"]
    supervisor = runtime["supervisor"]
    ws_pipeline = runtime["ws_pipeline"]

    print(f"Requested project root: {requested_root}")
    print(f"Resolved project root: {project_root}")
    print("Mode: LIVE")
    print("Execution mode: SIGNALS_ONLY")
    print(f"Telegram mode: {runtime['telegram_mode'].upper()}")
    print(f"Configured live index subscriptions: {len(runtime['live_index_to_token'])}")

    try:
        module03.start()
        time.sleep(max(0.0, float(args.module03_warmup_sec)))
        alert_engine.start()
        supervisor.start()

        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping live runner...")
    finally:
        try:
            supervisor.stop()
        except Exception:
            pass
        try:
            alert_engine.stop()
        except Exception:
            pass
        try:
            module03.stop()
        except Exception:
            pass
        try:
            ws_pipeline.stop()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
