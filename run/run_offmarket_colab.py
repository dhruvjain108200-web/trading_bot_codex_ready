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


def _load_secret(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return str(value)

    try:
        from google.colab import userdata  # type: ignore

        value = userdata.get(name)
        if value:
            return str(value)
    except Exception:
        pass

    raise RuntimeError(f"Missing required secret: {name}")


def _classify_offmarket_run(
    *,
    health_ok: bool,
    cycle_status: str,
    scanner_soft_degraded: bool,
) -> Dict[str, Any]:
    cycle_completed = cycle_status in {
        "OFFMARKET_CYCLE_RAN_SIGNALS_RELEASED",
        "OFFMARKET_CYCLE_RAN_SIGNALS_SUPPRESSED",
    }
    success = bool(health_ok and cycle_completed)
    return {
        "offmarket_success": success,
        "offmarket_status": "OFFMARKET_SUCCESS" if success else "OFFMARKET_FAILED",
        # Off-market soft-degrade notes are diagnostic, not a failed runtime state.
        "degraded": False if success else True,
        "scanner_soft_degraded": bool(scanner_soft_degraded),
    }


def _load_watchlist_tokens(project_root: Path) -> List[str]:
    path = project_root / "mappings" / "watchlist_tokens.json"
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(payload, list):
        return []

    out: List[str] = []
    seen = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("resolution_status", "")).strip().upper() != "RESOLVED":
            continue
        token = str(row.get("token", "")).strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


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


def _wait_for_alert_queue_drain(
    alert_engine: Any,
    *,
    timeout_sec: float,
    poll_sec: float = 0.5,
    settle_sec: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.time() + max(0.0, float(timeout_sec))
    settled_until: Optional[float] = None
    last_health: Dict[str, Any] = alert_engine.health_dict()

    while time.time() < deadline:
        health = alert_engine.health_dict()
        last_health = dict(health)
        queue_size = int(health.get("queue_size", 0) or 0)
        if queue_size <= 0:
            if settled_until is None:
                settled_until = time.time() + max(0.0, float(settle_sec))
            if time.time() >= settled_until:
                return {
                    "attempted": True,
                    "completed": True,
                    "timed_out": False,
                    "final_queue_size": queue_size,
                    "waited_sec": round(max(0.0, float(timeout_sec)) - max(0.0, deadline - time.time()), 2),
                    "health": last_health,
                }
        else:
            settled_until = None
        time.sleep(max(0.1, float(poll_sec)))

    return {
        "attempted": True,
        "completed": False,
        "timed_out": True,
        "final_queue_size": int(last_health.get("queue_size", 0) or 0),
        "waited_sec": round(max(0.0, float(timeout_sec)), 2),
        "health": last_health,
    }


def _hold_for_manual_telegram_commands(
    alert_engine: Any,
    *,
    shared_state: Optional[Dict[str, Any]] = None,
    hold_seconds: float,
    per_command_drain_timeout_sec: float = 6.0,
    poll_timeout_sec: int = 2,
    idle_sleep_sec: float = 0.25,
) -> Dict[str, Any]:
    deadline = time.time() + max(0.0, float(hold_seconds))
    processed_updates = 0
    plan_command_verifications: List[Dict[str, Any]] = []
    last_seen_diag_count = len(list((shared_state or {}).get("telegram_command_diagnostics", []) or []))

    while time.time() < deadline:
        remaining = max(0.0, deadline - time.time())
        timeout = max(1, min(int(poll_timeout_sec), int(remaining) if remaining >= 1 else 1))
        health_before_poll = alert_engine.health_dict()
        try:
            processed_updates += int(alert_engine.poll_telegram_once(timeout=timeout) or 0)
        except Exception:
            pass
        health_after_poll = alert_engine.health_dict()

        diagnostics = list((shared_state or {}).get("telegram_command_diagnostics", []) or [])
        if last_seen_diag_count < len(diagnostics):
            new_diags = diagnostics[last_seen_diag_count:]
            last_seen_diag_count = len(diagnostics)
            for diag in new_diags:
                if not isinstance(diag, dict):
                    continue
                if str(diag.get("parsed_action") or "").upper() != "PLAN":
                    continue

                plan_events = [x for x in (diag.get("plan_events") or []) if isinstance(x, dict)]
                reply_enqueued = any(str(x.get("reply_send") or "") == "enqueued" for x in plan_events)
                plan_generated = any(str(x.get("plan_source") or "none") != "none" for x in plan_events)
                verification = {
                    "raw_command": str(diag.get("raw_command") or ""),
                    "command_received": True,
                    "command_ok": bool(diag.get("ok")),
                    "plan_generated": bool(plan_generated),
                    "reply_enqueued": bool(reply_enqueued),
                    "queue_size_before": int(health_before_poll.get("queue_size", 0) or 0),
                    "queue_size_after": int(health_after_poll.get("queue_size", 0) or 0),
                    "messages_sent_before": int(health_before_poll.get("messages_sent", 0) or 0),
                    "messages_failed_before": int(health_before_poll.get("messages_failed", 0) or 0),
                    "messages_sent_delta": 0,
                    "messages_failed_delta": 0,
                    "last_error_after_command": "",
                    "queue_drain_attempted": False,
                    "queue_drain_completed": False,
                    "reply_sent_verified": False,
                }

                if verification["command_ok"] and verification["reply_enqueued"]:
                    drain = _wait_for_alert_queue_drain(
                        alert_engine,
                        timeout_sec=float(per_command_drain_timeout_sec),
                        poll_sec=0.25,
                        settle_sec=0.5,
                    )
                    drain_health = dict(drain.get("health") or alert_engine.health_dict() or {})
                    verification["queue_drain_attempted"] = True
                    verification["queue_drain_completed"] = bool(drain.get("completed", False))
                    verification["messages_sent_delta"] = max(
                        0,
                        int(drain_health.get("messages_sent", 0) or 0) - verification["messages_sent_before"],
                    )
                    verification["messages_failed_delta"] = max(
                        0,
                        int(drain_health.get("messages_failed", 0) or 0) - verification["messages_failed_before"],
                    )
                    verification["last_error_after_command"] = str(
                        drain_health.get("telegram_last_error")
                        or drain_health.get("last_error")
                        or ""
                    ).strip()
                    verification["reply_sent_verified"] = verification["messages_sent_delta"] > 0
                else:
                    verification["last_error_after_command"] = str(
                        health_after_poll.get("telegram_last_error")
                        or health_after_poll.get("last_error")
                        or ""
                    ).strip()

                verification["summary"] = (
                    f"command={verification['raw_command']!r} "
                    f"generated={verification['plan_generated']} "
                    f"enqueued={verification['reply_enqueued']} "
                    f"queue={verification['queue_size_before']}->{verification['queue_size_after']} "
                    f"sent_delta={verification['messages_sent_delta']} "
                    f"failed_delta={verification['messages_failed_delta']} "
                    f"sent_verified={verification['reply_sent_verified']} "
                    f"last_error={verification['last_error_after_command'] or '-'}"
                )
                print(f"[manual-plan-verify] {verification['summary']}")
                plan_command_verifications.append(verification)
        time.sleep(max(0.1, float(idle_sleep_sec)))

    return {
        "attempted": True,
        "hold_seconds": round(max(0.0, float(hold_seconds)), 2),
        "processed_updates": processed_updates,
        "plan_command_verifications": plan_command_verifications[-20:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Official off-market runner for one real Module 01-08 cycle in Colab."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to the trading_bot project root.",
    )
    parser.add_argument(
        "--module03-warmup-sec",
        type=float,
        default=1.0,
        help="Optional warmup time after starting Module 03.",
    )
    parser.add_argument(
        "--telegram-mode",
        choices=("dry-run", "real"),
        default="dry-run",
        help="Use dry-run Telegram by default, or real Telegram if explicitly enabled.",
    )
    parser.add_argument(
        "--telegram-drain-timeout-sec",
        type=float,
        default=20.0,
        help="How long REAL Telegram mode should wait for the alert queue to drain before shutdown.",
    )
    parser.add_argument(
        "--hold-seconds",
        type=float,
        default=0.0,
        help="Keep the off-market runner alive after menu publish for manual Telegram commands.",
    )
    args = parser.parse_args()

    def _get_telegram_token() -> Optional[str]:
        token = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
        if token:
            return str(token)
        for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN"):
            try:
                return _load_secret(name)
            except RuntimeError:
                continue
        return None

    telegram_mode = args.telegram_mode
    if telegram_mode == "dry-run" and _get_telegram_token() is not None:
        telegram_mode = "real"

    requested_root = Path(args.project_root).resolve()
    project_root = _resolve_project_root(requested_root)
    src_root = project_root / "src"

    if not src_root.exists():
        raise RuntimeError(f"src folder not found at: {src_root}")

    sys.path.insert(0, str(src_root))

    from module01_broker.broker_connection import BrokerConnection
    from module02_ws.module_02_websocket_v2_pipeline import (
        BrokerContext as Module02BrokerContext,
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
        Module05Config,
        Module05StrategyPatternTradePlanEngine,
    )
    from module06_alerts.module_06_alert_engine import AlertEngine, AlertEngineConfig
    from module07_maintenance.maintenance_worker import BotPaths, MaintenanceWorker
    import module08_supervisor.module_08_supervisor_orchestrator as module08_supervisor

    runtime_root = project_root / "runtime" / "offmarket_runner"
    alert_root = runtime_root / "alert_engine"
    broker_state_root = runtime_root / "broker_state"
    summary_path = runtime_root / "offmarket_run_summary.json"

    runtime_root.mkdir(parents=True, exist_ok=True)
    broker_state_root.mkdir(parents=True, exist_ok=True)

    def _broker_alert(message: str, severity: str = "WARN", extra: Optional[Dict[str, Any]] = None) -> None:
        extra = extra or {}
        print(f"[module01:{severity}] {message} | {json.dumps(extra, sort_keys=True)}")

    def _module07_alert(level: str, title: str, message: str, tags: Optional[List[str]] = None) -> None:
        tags = tags or []
        print(f"[module07:{level}] {title} | {message} | tags={tags}")

    print(f"Requested project root: {requested_root}")
    print(f"Resolved project root: {project_root}")
    print("Mode: OFFMARKET_TEST")
    print("Execution mode: SIGNALS_ONLY")
    print(f"Telegram mode: {'REAL' if telegram_mode == 'real' else 'DRY_RUN'}")
    print("WebSocket: not started")

    api_key = _load_secret("ANGEL_API_KEY")
    client_code = _load_secret("ANGEL_CLIENT_ID")
    pin = _load_secret("ANGEL_PIN")
    totp_secret = _load_secret("ANGEL_TOTP_SECRET")

    broker = BrokerConnection(
        api_key=api_key,
        client_code=client_code,
        pin=pin,
        totp_secret=totp_secret,
        instrument_store_dir=str(project_root / "data" / "instruments"),
        state_dir=str(broker_state_root),
        alert_callback=_broker_alert,
    )
    broker_ctx = broker.ensure_logged_in()

    provider = ProjectJsonProvider(project_root)
    token_to_symbol = load_token_to_symbol_map(str(project_root))
    index_tokens, live_index_to_token = load_production_index_tokens(str(project_root))
    watchlist_tokens = _load_watchlist_tokens(project_root)

    ws_pipeline = WebSocketV2Pipeline(
        broker_context=Module02BrokerContext(
            authToken=str(broker_ctx.authToken),
            feedToken=str(broker_ctx.feedToken),
            api_key=str(api_key),
            client_code=str(client_code),
            state=str(broker_ctx.state),
        ),
        token_to_symbol=token_to_symbol,
    )
    ws_pipeline.set_index_tokens(index_tokens)
    ws_pipeline.set_watchlist_tokens(watchlist_tokens)

    maintenance = MaintenanceWorker(
        paths=BotPaths(str(project_root)),
        broker_ready_fn=lambda: (True, "broker logged in"),
        ws_ready_fn=lambda: (False, "websocket intentionally not started for off-market run"),
        alert_fn=_module07_alert,
        market_lists_provider=None,
    )
    readiness_ok, readiness_reasons = maintenance.run_readiness_checks()
    if not readiness_ok:
        raise RuntimeError(
            "Module 07 readiness failed: " + ("; ".join(readiness_reasons) if readiness_reasons else "unknown")
        )

    module03 = MarketContextMonitor(
        snapshot_source=ws_pipeline,
        taxonomy_path=str(project_root / "lists" / "index_taxonomy.json"),
        event_calendar_path=str(project_root / "data" / "event_calendar.json"),
    )
    module03.start()
    time.sleep(max(0.0, float(args.module03_warmup_sec)))

    scanner = StockUniverseBuilderScanner(
        broker=broker,
        pipeline=ws_pipeline,
        market_context_provider=SimpleMarketContextProvider(module03),
        maintenance_provider=provider,
        candle_provider=broker,
        config={},
    )
    scanner._is_live_market = lambda: False  # type: ignore[attr-defined]

    strategy_engine = Module05StrategyPatternTradePlanEngine(
        config=Module05Config(
            offmarket_test_relax_quality=True,
            offmarket_test_relax_confidence=True,
        )
    )

    telegram_token = None
    if telegram_mode == "real":
        telegram_token = _get_telegram_token()
        if telegram_token is None:
            raise RuntimeError(
                "REAL Telegram mode requested but TELEGRAM_TOKEN or TELEGRAM_BOT_TOKEN is not configured."
            )

    if telegram_mode == "real":
        telegram_bot_token = telegram_token
        telegram_chat_id = _load_secret("TELEGRAM_CHAT_ID")
    else:
        telegram_bot_token = "DRYRUN"
        telegram_chat_id = "DRYRUN"

    alert_shared_state: Dict[str, Any] = {}
    alert_engine = AlertEngine(
        config=AlertEngineConfig(
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            journal_dir=str(alert_root / "journal"),
            state_dir=str(alert_root / "state"),
            deletion_registry_dir=str(alert_root / "deletion_registry"),
            log_dir=str(alert_root / "logs"),
            auto_delete_enabled=False,
            heartbeat_interval_sec=10**9,
            debug=False,
        ),
        snapshot_getter=lambda: ws_pipeline.snapshot_store,
        market_context_getter=module03.get_state,
        health_getter=lambda: {
            "broker": broker.health(),
            "websocket": {"ws_connected": False, "state": "OFFMARKET_TEST"},
        },
        shared_state=alert_shared_state,
    )
    if telegram_mode == "dry-run":
        alert_engine.telegram = DryRunTelegramSender()
    alert_engine.start()

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
    offmarket_ctx = {
        "live_market": False,
        "offmarket_test": True,
        "mode_name": "OFFMARKET_TEST",
    }
    supervisor._mode_context = lambda: dict(offmarket_ctx)  # type: ignore[assignment]
    supervisor.execution_mode = "SIGNALS_ONLY"
    supervisor.execution_enabled = False
    supervisor.system_state = "RUNNING"
    queue_drain_status = {
        "attempted": False,
        "completed": False,
        "timed_out": False,
        "final_queue_size": 0,
        "waited_sec": 0.0,
    }
    hold_status = {
        "attempted": False,
        "hold_seconds": 0.0,
        "processed_updates": 0,
    }

    try:
        supervisor._run_scan_cycle()
        if float(args.hold_seconds) > 0:
            print("")
            print(
                "Waiting for Telegram manual commands "
                f"for {float(args.hold_seconds):.0f} seconds. "
                'You can now send: "plan A", "plan M", or "plan RELIANCE".'
            )
            hold_status = _hold_for_manual_telegram_commands(
                alert_engine,
                shared_state=alert_shared_state,
                hold_seconds=float(args.hold_seconds),
            )
        if telegram_mode == "real":
            queue_drain_status = _wait_for_alert_queue_drain(
                alert_engine,
                timeout_sec=float(args.telegram_drain_timeout_sec),
            )
        else:
            time.sleep(1.0)
    finally:
        alert_health = alert_engine.health_dict()
        alert_engine.stop()
        module03.stop()

    module04_output = supervisor.last_module04_output or {}
    module05_output = supervisor.last_module05_output or {}
    scanner_diagnostics = module04_output.get("scanner_diagnostics", {}) or {}
    offmarket_health_ok = bool(supervisor._health_check())

    candidate_count = len(module04_output.get("candidate_list") or [])
    backup_candidate_count = len(module04_output.get("candidate_backup_list") or [])
    signal_count = len(module05_output.get("trade_signal_list") or [])
    menu_id = alert_health.get("last_menu_id")
    messages_sent = int(alert_health.get("messages_sent", 0) or 0)
    messages_failed = int(alert_health.get("messages_failed", 0) or 0)
    last_error = str(
        alert_health.get("telegram_last_error")
        or alert_health.get("last_error")
        or ""
    ).strip()
    status_flags = _classify_offmarket_run(
        health_ok=offmarket_health_ok,
        cycle_status=str(supervisor.last_cycle_status or ""),
        scanner_soft_degraded=bool(scanner_diagnostics.get("degraded", False)),
    )
    degraded = bool(status_flags["degraded"])
    menu_alert_status = (
        f"telegram_mode={'REAL' if telegram_mode == 'real' else 'DRY_RUN'}, "
        f"menu_id={menu_id}, messages_sent={messages_sent}, alert_state={alert_health.get('state', '-')}'"
        if menu_id or messages_sent
        else (
            f"telegram_mode={'REAL' if telegram_mode == 'real' else 'DRY_RUN'}, "
            f"no_menu_emitted, messages_sent={messages_sent}, alert_state={alert_health.get('state', '-')}"
        )
    )

    summary = {
        "mode_name": "OFFMARKET_TEST",
        "execution_mode": supervisor.execution_mode,
        "execution_enabled": supervisor.execution_enabled,
        "websocket_started": False,
        "offmarket_status": status_flags["offmarket_status"],
        "offmarket_success": status_flags["offmarket_success"],
        "offmarket_health_ok": offmarket_health_ok,
        "candidate_count": candidate_count,
        "backup_candidate_count": backup_candidate_count,
        "signal_count": signal_count,
        "messages_sent": messages_sent,
        "messages_failed": messages_failed,
        "last_error": last_error,
        "queue_drain_attempted": bool(queue_drain_status.get("attempted", False)),
        "queue_drain_completed": bool(queue_drain_status.get("completed", False)),
        "queue_drain_timed_out": bool(queue_drain_status.get("timed_out", False)),
        "menu_alert_status": menu_alert_status,
        "degraded": degraded,
        "scanner_soft_degraded": status_flags["scanner_soft_degraded"],
        "last_cycle_status": supervisor.last_cycle_status,
        "scanner_notes": scanner_diagnostics.get("notes", []) or [],
        "telegram_mode": "REAL" if telegram_mode == "real" else "DRY_RUN",
        "manual_command_hold": {
            "attempted": bool(hold_status.get("attempted", False)),
            "hold_seconds": float(hold_status.get("hold_seconds", 0.0) or 0.0),
            "processed_updates": int(hold_status.get("processed_updates", 0) or 0),
            "plan_command_verifications": list(hold_status.get("plan_command_verifications", []) or [])[-10:],
        },
        "telegram_command_diagnostics": list(alert_shared_state.get("telegram_command_diagnostics", []) or [])[-5:],
        "telegram_queue": {
            "final_queue_size": int(queue_drain_status.get("final_queue_size", alert_health.get("queue_size", 0) or 0)),
            "waited_sec": float(queue_drain_status.get("waited_sec", 0.0) or 0.0),
        },
        "module07_readiness": {
            "ready": bool(readiness_ok),
            "reasons": list(readiness_reasons or []),
        },
        "subscription_scope": {
            "live_index_count": len(index_tokens),
            "watchlist_token_count": len(watchlist_tokens),
            "resolved_live_indexes": len(live_index_to_token),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\nOFFMARKET SUMMARY")
    print(f"offmarket_status: {status_flags['offmarket_status']}")
    print(f"candidate_count: {candidate_count}")
    print(f"backup_candidate_count: {backup_candidate_count}")
    print(f"signal_count: {signal_count}")
    print(f"menu_alert_status: {menu_alert_status}")
    print(f"messages_sent: {messages_sent}")
    print(f"messages_failed: {messages_failed}")
    print(f"last_error: {last_error or '-'}")
    print(f"manual_command_hold_attempted: {bool(hold_status.get('attempted', False))}")
    print(f"manual_command_hold_seconds: {float(hold_status.get('hold_seconds', 0.0) or 0.0)}")
    print(f"manual_command_processed_updates: {int(hold_status.get('processed_updates', 0) or 0)}")
    recent_plan_verifications = list(hold_status.get("plan_command_verifications", []) or [])[-5:]
    if recent_plan_verifications:
        print("manual_plan_reply_verifications:")
        for item in recent_plan_verifications:
            summary_line = str((item or {}).get("summary") or "").strip()
            if summary_line:
                print(f"  - {summary_line}")
    recent_command_diags = list(alert_shared_state.get("telegram_command_diagnostics", []) or [])[-5:]
    if recent_command_diags:
        print("recent_telegram_command_diagnostics:")
        for item in recent_command_diags:
            summary_line = str((item or {}).get("summary") or "").strip()
            if summary_line:
                print(f"  - {summary_line}")
    print(f"queue_drain_attempted: {bool(queue_drain_status.get('attempted', False))}")
    print(f"degraded: {degraded}")
    print(f"scanner_soft_degraded: {status_flags['scanner_soft_degraded']}")
    print(f"summary_json: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
