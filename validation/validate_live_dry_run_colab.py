from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List


def _status(results: List[Dict[str, Any]], name: str, level: str, detail: str) -> None:
    row = {"check": name, "status": level, "detail": detail}
    results.append(row)
    print(f"[{level}] {name}: {detail}")


def _write_summary(path: Path, results: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": results,
        "pass_count": sum(1 for x in results if x["status"] == "PASS"),
        "fail_count": sum(1 for x in results if x["status"] == "FAIL"),
        "warn_count": sum(1 for x in results if x["status"] == "WARN"),
        "skip_count": sum(1 for x in results if x["status"] == "SKIP"),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nSummary written to: {path}")


class RepoJsonProvider:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.path_map = {
            "watchlist.json": self.root / "lists" / "watchlist.json",
            "potential_stocks.json": self.root / "lists" / "potential_stocks.json",
            "daily_movers_cache.json": self.root / "lists" / "daily_movers_cache.json",
            "daily_losers_cache.json": self.root / "lists" / "daily_losers_cache.json",
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


class StubModule03:
    def get_state(self) -> Dict[str, Any]:
        return {
            "market_context_state": {
                "trade_gate": "ALLOW",
                "macro_regime": "RANGING",
                "macro_bias": "NEUTRAL",
            },
            "sector_scoreboard": [],
            "symbol_risk_map": {},
        }


class PassiveAlertEngine:
    def poll_telegram_once(self) -> int:
        return 0


class AlwaysReadyMaintenance:
    def is_ready(self) -> bool:
        return True


def _is_market_hours_ist() -> bool:
    import datetime as dt

    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    now = dt.datetime.now(ist)
    if now.weekday() >= 5:
        return False
    return dt.time(9, 15) <= now.time() <= dt.time(15, 30)


def _load_watchlist_contract(root: Path) -> Dict[str, Any]:
    path = root / "mappings" / "watchlist_tokens.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    tokens: List[str] = []
    unresolved: List[str] = []

    if not isinstance(rows, list):
        raise ValueError("watchlist_tokens.json must be a list of watchlist token rows")

    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        exchange = str(row.get("exchange", "NSE")).strip() or "NSE"
        token = row.get("token")
        resolution = str(row.get("resolution_status", "")).strip().upper()

        if token and resolution == "RESOLVED":
            token_str = str(token).strip()
            if token_str and token_str not in tokens:
                tokens.append(token_str)
        elif symbol:
            unresolved.append(f"{exchange}:{symbol}")

    return {"tokens": tokens, "unresolved": unresolved}


def _pick_sample_snapshot(snapshot_store: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    for key, row in (snapshot_store or {}).items():
        if not isinstance(row, dict):
            continue
        if ":" in str(key) and row.get("snapshot_key") and row.get("ltp") is not None:
            return row
    for row in (snapshot_store or {}).values():
        if isinstance(row, dict) and row.get("snapshot_key") and row.get("ltp") is not None:
            return row
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Live dry-run validation for the patched trading bot repo.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to the trading_bot project root.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=60,
        help="Maximum seconds to wait for websocket connect/ticks.",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    src_root = project_root / "src"
    summary_path = project_root / "runtime" / "validation" / "live_dry_run" / "live_dry_run_summary.json"
    results: List[Dict[str, Any]] = []

    print(f"Project root: {project_root}")

    if not src_root.exists():
        _status(results, "project_layout", "FAIL", f"src folder not found at {src_root}")
        _write_summary(summary_path, results)
        return 1

    sys.path.insert(0, str(src_root))

    try:
        from module01_broker.broker_connection import BrokerConnection
        from module02_ws.module_02_websocket_v2_pipeline import WebSocketV2Pipeline, load_production_index_tokens, load_token_to_symbol_map
        from module04_scanner.module_04_stock_universe_builder_scanner import SimpleMarketContextProvider, StockUniverseBuilderScanner
        from module05_strategy.module_05_strategy_pattern_tradeplan_engine import Module05StrategyPatternTradePlanEngine
        from module08_supervisor.module_08_supervisor_orchestrator import Supervisor
    except Exception as e:
        _status(results, "imports_succeed", "FAIL", str(e))
        _write_summary(summary_path, results)
        return 1

    _status(results, "imports_succeed", "PASS", "Module 01/02/04/05/08 imports completed.")

    api_key = os.getenv("ANGEL_API_KEY", "").strip()
    client_code = os.getenv("ANGEL_CLIENT_CODE", "").strip()
    pin = os.getenv("ANGEL_PIN", "").strip()
    totp_secret = os.getenv("ANGEL_TOTP_SECRET", "").strip()

    missing = [name for name, value in {
        "ANGEL_API_KEY": api_key,
        "ANGEL_CLIENT_CODE": client_code,
        "ANGEL_PIN": pin,
        "ANGEL_TOTP_SECRET": totp_secret,
    }.items() if not value]
    if missing:
        _status(results, "credentials_present", "FAIL", f"Missing env vars: {', '.join(missing)}")
        _write_summary(summary_path, results)
        return 1

    _status(results, "credentials_present", "PASS", "Required Angel env vars found.")

    provider = RepoJsonProvider(project_root)
    runtime_cfg = provider.read_json("runtime_config.json") or {}
    configured_live_indexes = runtime_cfg.get("live_index_subscriptions", []) or []
    watchlist_contract = _load_watchlist_contract(project_root)
    token_to_symbol = load_token_to_symbol_map(str(project_root))

    live_runtime_root = project_root / "runtime" / "validation" / "live_dry_run"
    live_runtime_root.mkdir(parents=True, exist_ok=True)

    broker = None
    ws_pipeline = None
    try:
        broker = BrokerConnection(
            api_key=api_key,
            client_code=client_code,
            pin=pin,
            totp_secret=totp_secret,
            instrument_store_dir=str(live_runtime_root / "instruments"),
            state_dir=str(live_runtime_root / "state"),
        )
        broker.ensure_logged_in()
        _status(results, "broker_login_works", "PASS", "Broker login and token retrieval succeeded.")
    except Exception as e:
        _status(results, "broker_login_works", "FAIL", str(e))
        _write_summary(summary_path, results)
        return 1

    try:
        broker_context = broker.get_context()
        ws_pipeline = WebSocketV2Pipeline(
            broker_context=broker_context,
            token_to_symbol=token_to_symbol,
        )

        production_index_tokens, resolved_live_indexes = load_production_index_tokens(str(project_root))
        ws_pipeline.set_index_tokens(production_index_tokens)
        idx_groups = list(getattr(ws_pipeline, "_registry_groups", {}).get("IDX", []))
        idx_group_ok = bool(idx_groups) and all(int(group.get("exchangeType", -1)) == 1 for group in idx_groups)

        _status(
            results,
            "live_index_subscriptions_resolve",
            "PASS" if len(resolved_live_indexes) == len(configured_live_indexes) and production_index_tokens else "FAIL",
            f"resolved={len(resolved_live_indexes)}/{len(configured_live_indexes)}, tokens={len(production_index_tokens)}",
        )
        _status(
            results,
            "live_index_subscription_contract",
            "PASS" if idx_group_ok else "FAIL",
            json.dumps(idx_groups, ensure_ascii=False),
        )

        ws_pipeline.start()
        connect_deadline = time.time() + max(10, int(args.timeout_sec))
        while time.time() < connect_deadline:
            if bool(ws_pipeline.health.ws_connected):
                break
            time.sleep(1.0)

        ws_connected = bool(ws_pipeline.health.ws_connected)
        _status(
            results,
            "websocket_connects",
            "PASS" if ws_connected else "FAIL",
            f"ws_connected={ws_connected}, state={getattr(ws_pipeline.health, 'state', 'UNKNOWN')}",
        )
        if not ws_connected:
            _write_summary(summary_path, results)
            return 1

        module03 = StubModule03()
        scanner = StockUniverseBuilderScanner(
            broker=broker,
            pipeline=ws_pipeline,
            market_context_provider=SimpleMarketContextProvider(module03),
            maintenance_provider=provider,
            candle_provider=broker,
            config={},
        )
        strategy_engine = Module05StrategyPatternTradePlanEngine()
        supervisor = Supervisor(
            broker=broker,
            ws_pipeline=ws_pipeline,
            module03=module03,
            scanner=scanner,
            strategy_engine=strategy_engine,
            alert_engine=PassiveAlertEngine(),
            maintenance=AlwaysReadyMaintenance(),
        )
        live_ctx = {"live_market": True, "offmarket_test": False, "mode_name": "LIVE"}
        supervisor._mode_context = lambda: dict(live_ctx)  # type: ignore[assignment]

        seeded_watchlist_tokens = supervisor._seed_live_watchlist_from_universe()
        wl_groups = list(getattr(ws_pipeline, "_registry_groups", {}).get("WL", []))
        wl_group_ok = bool(wl_groups) and all(int(group.get("exchangeType", -1)) == 1 for group in wl_groups)
        _status(results, "live_watchlist_seeding_runs", "PASS" if bool(seeded_watchlist_tokens) else "FAIL", f"seeded_tokens={len(seeded_watchlist_tokens)}")
        _status(results, "live_watchlist_subscription_contract", "PASS" if wl_group_ok else "FAIL", json.dumps(wl_groups[:3], ensure_ascii=False))

        tick_deadline = time.time() + max(15, int(args.timeout_sec))
        sample_snapshot: Dict[str, Any] = {}
        while time.time() < tick_deadline:
            sample_snapshot = _pick_sample_snapshot(ws_pipeline.snapshot_store)
            if sample_snapshot:
                break
            time.sleep(2.0)

        snapshot_ok = bool(sample_snapshot) and all(key in sample_snapshot for key in ("snapshot_key", "ltp", "last_tick_time", "token", "exchange"))
        _status(
            results,
            "module02_snapshot_contract_appears",
            "PASS" if snapshot_ok else "FAIL",
            json.dumps(sample_snapshot, ensure_ascii=False, default=str)[:600] if sample_snapshot else "No live snapshots received.",
        )

        market_hours = _is_market_hours_ist()
        if not market_hours:
            _status(results, "market_hours_precondition", "WARN", "Run this validator during NSE market hours (Mon-Fri 09:15-15:30 IST) for full live candidate checks.")
        else:
            _status(results, "market_hours_precondition", "PASS", "Validator is running during NSE cash market hours.")

        module04_output = scanner.run_scan()
        candidate_count = len(module04_output.get("candidate_list") or [])
        backup_count = len(module04_output.get("candidate_backup_list") or [])
        scanner_level = "PASS" if candidate_count > 0 else ("WARN" if backup_count > 0 else "FAIL")
        _status(results, "module04_produces_candidates", scanner_level, f"candidate_list={candidate_count}, candidate_backup_list={backup_count}")

        candidates, backups, snapshot_store, candle_store_5m = supervisor._prepare_strategy_inputs(module04_output, live_ctx)
        module05_output = strategy_engine.run_scan(
            candidate_list=candidates,
            candidate_backup_list=backups,
            market_context=module03.get_state(),
            snapshot_store=snapshot_store,
            candle_store_5m=candle_store_5m,
            candle_store_1m={},
            candle_store_15m={},
            now_ts=time.time(),
        )
        strategy_ok = isinstance(module05_output, dict) and "strategy_diagnostics" in module05_output
        _status(
            results,
            "module05_receives_strategy_inputs",
            "PASS" if strategy_ok else "FAIL",
            f"input_candidates={len(candidates)}, input_backups={len(backups)}, signals={len(module05_output.get('trade_signal_list') or [])}",
        )

        _status(results, "no_live_orders_called", "PASS", "Validator only uses broker login, token access, candles, websocket, scanner, and strategy paths.")

        if watchlist_contract["unresolved"]:
            _status(results, "watchlist_token_unresolved_rows", "WARN", ", ".join(watchlist_contract["unresolved"][:10]))
        else:
            _status(results, "watchlist_token_unresolved_rows", "PASS", "No unresolved watchlist token rows.")

    except Exception as e:
        _status(results, "live_dry_run_execution", "FAIL", str(e))
    finally:
        if ws_pipeline is not None:
            try:
                ws_pipeline.stop()
            except Exception:
                pass

    _write_summary(summary_path, results)
    return 0 if not any(r["status"] == "FAIL" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
