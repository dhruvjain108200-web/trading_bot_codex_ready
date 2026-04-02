from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple


SOURCE_LIST_FILES = (
    "watchlist.json",
    "potential_stocks.json",
    "daily_movers_cache.json",
    "daily_losers_cache.json",
    "manual_additions.json",
    "discovered_symbols.json",
    "previous_day_movers.json",
    "previous_day_losers.json",
)


def _status(
    results: List[Dict[str, Any]],
    name: str,
    passed: bool,
    detail: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    label = "PASS" if passed else "FAIL"
    row = {"check": name, "status": label, "detail": detail}
    if extra:
        row["extra"] = extra
    results.append(row)
    print(f"[{label}] {name}: {detail}")
    if extra:
        print(json.dumps(extra, indent=2, sort_keys=True))


def _write_summary(path: Path, results: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": results,
        "pass_count": sum(1 for x in results if x["status"] == "PASS"),
        "fail_count": sum(1 for x in results if x["status"] == "FAIL"),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nSummary written to: {path}")


def _scanner_diag_payload(module04_output: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = module04_output.get("scanner_diagnostics", {}) or {}
    notes = diagnostics.get("notes", [])
    if not isinstance(notes, list):
        notes = [str(notes)]
    exclusions = diagnostics.get("exclusions_summary", {}) or {}
    if not isinstance(exclusions, dict):
        exclusions = {"raw_value": exclusions}

    candidate_list = module04_output.get("candidate_list") or []
    candidate_backup_list = module04_output.get("candidate_backup_list") or []

    candle_failure_notes = [
        str(note)
        for note in notes
        if "candle" in str(note).lower() or "degraded" in str(note).lower()
    ]

    return {
        "candidate_list_count": len(candidate_list),
        "candidate_backup_list_count": len(candidate_backup_list),
        "stage1_survivors_count": int(diagnostics.get("stage1_survivors_count", 0) or 0),
        "source_counts_raw": diagnostics.get("source_counts_raw", {}) or {},
        "source_counts_merged": diagnostics.get("source_counts_merged", {}) or {},
        "merged_unique_count": int(diagnostics.get("merged_unique_count", 0) or 0),
        "degraded": bool(diagnostics.get("degraded", False)),
        "notes": notes,
        "exclusions_summary": exclusions,
        "candle_failure_or_degraded_notes": candle_failure_notes,
    }


def _json_item_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return len(value)
    return 0


def _looks_like_project_root(path: Path) -> bool:
    return (
        (path / "src").exists()
        and (path / "lists").exists()
        and (path / "config").exists()
        and (path / "mappings").exists()
    )


def _resolve_project_root(requested_root: Path) -> Path:
    candidates: List[Path] = [requested_root]
    candidates.append(requested_root / "trading_bot")
    candidates.append(requested_root.parent)

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


def _prepare_workspace(project_root: Path, name: str) -> Tuple[Path, Dict[str, Any]]:
    workspace = project_root / "runtime" / "validation" / name / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    for folder in ("config", "lists", "mappings", "instruments"):
        src = project_root / folder
        dst = workspace / folder
        if src.exists():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            dst.mkdir(parents=True, exist_ok=True)

    for folder in ("state", "logs"):
        (workspace / folder).mkdir(parents=True, exist_ok=True)

    source_report: Dict[str, Any] = {
        "resolved_project_root": str(project_root),
        "workspace_root": str(workspace),
        "project_lists_dir_exists": (project_root / "lists").exists(),
        "workspace_lists_dir_exists": (workspace / "lists").exists(),
        "source_files": {},
    }

    for name in SOURCE_LIST_FILES:
        project_file = project_root / "lists" / name
        workspace_file = workspace / "lists" / name
        project_exists = project_file.exists()
        workspace_exists = workspace_file.exists()

        project_value = None
        workspace_value = None
        project_read_ok = False
        workspace_read_ok = False

        if project_exists:
            try:
                project_value = json.loads(project_file.read_text(encoding="utf-8"))
                project_read_ok = True
            except Exception:
                project_value = None

        if workspace_exists:
            try:
                workspace_value = json.loads(workspace_file.read_text(encoding="utf-8"))
                workspace_read_ok = True
            except Exception:
                workspace_value = None

        source_report["source_files"][name] = {
            "project_exists": project_exists,
            "workspace_exists": workspace_exists,
            "project_read_ok": project_read_ok,
            "workspace_read_ok": workspace_read_ok,
            "project_count": _json_item_count(project_value),
            "workspace_count": _json_item_count(workspace_value),
            "copied_ok": (not project_exists and not workspace_exists) or (
                project_exists
                and workspace_exists
                and project_read_ok
                and workspace_read_ok
                and _json_item_count(project_value) == _json_item_count(workspace_value)
            ),
        }

    source_report["workspace_source_files_ready"] = all(
        bool(info.get("copied_ok"))
        for info in source_report["source_files"].values()
    )

    return workspace, source_report


class RepoJsonProvider:
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


class ValidationBroker:
    def ensure_logged_in(self) -> bool:
        return True

    def get_tokens(self) -> Tuple[str, str, str]:
        return ("OFFMARKET_AUTH", "OFFMARKET_REFRESH", "OFFMARKET_FEED")

    def health(self) -> Dict[str, Any]:
        return {"state": "OK", "mode": "SIGNALS_ONLY", "note": "validation fixture only"}

    def get_5m_candles(
        self,
        *,
        symbol: str,
        token: str,
        exchange: str = "NSE",
        lookback: int = 80,
    ) -> List[Dict[str, Any]]:
        base = 100.0 + (sum(ord(ch) for ch in f"{exchange}:{symbol}") % 250)
        now = int(time.time())
        out: List[Dict[str, Any]] = []
        for idx in range(max(lookback, 12)):
            drift = idx * 0.22
            close = round(base + drift, 2)
            out.append(
                {
                    "time": now - ((lookback - idx) * 300),
                    "open": round(close - 0.35, 2),
                    "high": round(close + 0.55, 2),
                    "low": round(close - 0.65, 2),
                    "close": close,
                    "volume": 150000 + (idx * 1200),
                }
            )
        return out


class ValidationPipeline:
    def __init__(self, token_to_symbol: Dict[str, str], symbol_to_token: Dict[str, str]) -> None:
        self.token_to_symbol = token_to_symbol or {}
        self.symbol_to_token = symbol_to_token or {}
        self.health = SimpleNamespace(ws_connected=False, state="OFFMARKET_TEST")
        self._registry = {"IDX": [], "WL": [], "FOCUS": []}
        self._snapshots: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _split_identity(symbol_value: str, exchange: str) -> Tuple[str, str]:
        raw = str(symbol_value or "").strip()
        ex = str(exchange or "NSE").strip() or "NSE"
        sym = raw
        if ":" in raw:
            ex, sym = raw.split(":", 1)
        if sym.upper().endswith("-EQ"):
            sym = sym[:-3]
        return ex or "NSE", sym.strip()

    def _build_snapshot(self, token: str, symbol: str, exchange: str, tier: str = "WL") -> Dict[str, Any]:
        ex, sym = self._split_identity(symbol, exchange)
        token_key = str(token or "").strip()
        if not token_key:
            token_key = str(self.symbol_to_token.get(f"{ex}:{sym}") or self.symbol_to_token.get(sym) or "").strip()
        if not token_key:
            token_key = str(abs(hash(f"{ex}:{sym}")) % 999999 + 1000)

        now = time.time()
        ltp = round(100.0 + (sum(ord(ch) for ch in f"{ex}:{sym}") % 500) / 10.0, 2)
        volume = 200000 + (sum(ord(ch) for ch in sym) % 250000)
        row = {
            "token": token_key,
            "symbol": sym,
            "exchange": ex,
            "snapshot_key": f"{ex}:{sym}",
            "ltp": ltp,
            "volume": float(volume),
            "last_tick_time": now,
            "ts": now,
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "exchangeType": 1,
            "tier": tier,
        }
        self._snapshots[token_key] = row
        self._snapshots[row["snapshot_key"]] = row
        self._snapshots[sym] = row
        return row

    def set_watchlist_tokens(self, tokens: List[Any]) -> None:
        cleaned = [str(t).strip() for t in (tokens or []) if str(t).strip()]
        self._registry["WL"] = list(dict.fromkeys(cleaned))

    def get_registry(self) -> Dict[str, List[str]]:
        return {k: list(v) for k, v in self._registry.items()}

    def get_snapshot(self, token: Any = None, *, symbol: str | None = None, exchange: str = "NSE") -> Dict[str, Any]:
        if token is not None:
            token_key = str(token).strip()
            if token_key in self._snapshots:
                return self._snapshots[token_key]
            symbol_guess = self.token_to_symbol.get(token_key, "")
            ex, sym = self._split_identity(symbol_guess, exchange)
            return self._build_snapshot(token_key, sym, ex)
        if symbol:
            ex, sym = self._split_identity(symbol, exchange)
            return self._build_snapshot("", sym, ex)
        return {}

    @property
    def snapshot_store(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._snapshots)

    def get_snapshots_bulk(self, items: List[Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for item in items or []:
            token = ""
            symbol = ""
            exchange = "NSE"
            tier = "WL"
            if isinstance(item, dict):
                token = str(item.get("token", "")).strip()
                symbol = str(item.get("symbol", "")).strip()
                exchange = str(item.get("exchange", "NSE")).strip() or "NSE"
                tier = str(item.get("tier", "WL")).strip() or "WL"
            else:
                token = str(item).strip()
                symbol = self.token_to_symbol.get(token, "")
            row = self._build_snapshot(token, symbol, exchange, tier=tier)
            out[row["token"]] = row
            out[row["snapshot_key"]] = row
            out[row["symbol"]] = row
        return out


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


class DryRunTelegramSender:
    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def send_message(self, text: str, parse_mode: str | None = None, disable_notification: bool = False) -> Dict[str, Any]:
        message_id = len(self.sent) + 1
        self.sent.append(
            {
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": disable_notification,
            }
        )
        return {"ok": True, "result": {"message_id": message_id}}

    def delete_message(self, message_id: int) -> Dict[str, Any]:
        return {"ok": True, "result": True, "message_id": int(message_id)}

    def get_updates(self, offset: int | None = None, timeout: int = 2, allowed_updates: List[str] | None = None) -> List[Dict[str, Any]]:
        return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Off-market validation for the patched trading bot repo.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to the trading_bot project root.",
    )
    args = parser.parse_args()

    requested_root = Path(args.project_root).resolve()
    project_root = _resolve_project_root(requested_root)
    src_root = project_root / "src"
    summary_path = project_root / "runtime" / "validation" / "offmarket" / "offmarket_validation_summary.json"
    results: List[Dict[str, Any]] = []

    print(f"Requested project root: {requested_root}")
    print(f"Resolved project root: {project_root}")

    if not src_root.exists():
        _status(results, "project_layout", False, f"src folder not found at {src_root}")
        _write_summary(summary_path, results)
        return 1

    sys.path.insert(0, str(src_root))

    try:
        from module01_broker import broker_connection as module01_broker_connection  # noqa: F401
        from module02_ws import module_02_websocket_v2_pipeline as module02_ws_pipeline  # noqa: F401
        from module04_scanner.module_04_stock_universe_builder_scanner import SimpleMarketContextProvider, StockUniverseBuilderScanner
        from module05_strategy.module_05_strategy_pattern_tradeplan_engine import Module05Config, Module05StrategyPatternTradePlanEngine
        from module06_alerts.module_06_alert_engine import AlertEngine, AlertEngineConfig
        from module07_maintenance.maintenance_worker import BotPaths, MaintenanceWorker
        from module08_supervisor.module_08_supervisor_orchestrator import Supervisor
    except Exception as e:
        _status(results, "imports_succeed", False, str(e))
        _write_summary(summary_path, results)
        return 1

    _status(results, "imports_succeed", True, "Module 01/02/04/05/06/07/08 imports completed.")

    workspace_root, workspace_source_report = _prepare_workspace(project_root, "offmarket")
    provider = RepoJsonProvider(workspace_root)
    provider_source_counts = {
        name: _json_item_count(provider.read_json(name))
        for name in SOURCE_LIST_FILES
    }
    workspace_source_extra = dict(workspace_source_report)
    workspace_source_extra["provider_source_counts"] = provider_source_counts
    _status(
        results,
        "workspace_source_loading",
        bool(workspace_source_report.get("workspace_source_files_ready")),
        (
            f"resolved_project_root={project_root}, "
            f"workspace_source_files_ready={workspace_source_report.get('workspace_source_files_ready')}"
        ),
        extra=workspace_source_extra,
    )

    symbol_to_token = provider.read_json("symbol_to_token.json") or {}
    token_to_symbol = provider.read_json("token_to_symbol.json") or {}
    if not isinstance(symbol_to_token, dict):
        symbol_to_token = {}
    if not isinstance(token_to_symbol, dict):
        token_to_symbol = {}

    broker = ValidationBroker()
    ws_pipeline = ValidationPipeline(token_to_symbol=token_to_symbol, symbol_to_token=symbol_to_token)
    module03 = StubModule03()

    def _alert_fn(level: str, title: str, message: str, tags: List[str] | None = None) -> None:
        tags = tags or []
        print(f"[module07:{level}] {title} | {message} | tags={tags}")

    maintenance = MaintenanceWorker(
        paths=BotPaths(str(workspace_root)),
        broker_ready_fn=lambda: (True, "validation broker ready"),
        ws_ready_fn=lambda: (False, "websocket intentionally off for off-market validation"),
        alert_fn=_alert_fn,
        market_lists_provider=None,
    )

    try:
        readiness_ok, readiness_reasons = maintenance.run_readiness_checks()
        readiness_bool = maintenance.is_ready()
        _status(results, "maintenance_readiness_consistent", readiness_bool == readiness_ok, f"is_ready={readiness_bool}, run_readiness_checks={readiness_ok}")
        _status(
            results,
            "maintenance_readiness_works",
            readiness_ok,
            "ready" if readiness_ok else "; ".join(readiness_reasons) or "unknown readiness failure",
        )
    except Exception as e:
        _status(results, "maintenance_readiness_works", False, str(e))

    scanner = StockUniverseBuilderScanner(
        broker=broker,
        pipeline=ws_pipeline,
        market_context_provider=SimpleMarketContextProvider(module03),
        maintenance_provider=provider,
        candle_provider=broker,
        config={
            "stage1": {
                "max_stream_universe": 40,
                "min_price": 1.0,
                "max_price": 100000.0,
                "min_volume": 1,
                "min_traded_value": 1.0,
                "min_range_pct": 0.0,
                "max_spread_pct": 1.0,
                "stage1_target_min": 5,
                "stage1_target_max": 20,
            },
            "ranking": {"top_combined": 3},
        },
    )
    scanner._is_live_market = lambda: False  # type: ignore[attr-defined]

    strategy_engine = Module05StrategyPatternTradePlanEngine(
        config=Module05Config(offmarket_test_relax_quality=True, offmarket_test_relax_confidence=True)
    )

    alert_root = project_root / "runtime" / "validation" / "offmarket" / "alert_engine"
    alert_engine = AlertEngine(
        config=AlertEngineConfig(
            telegram_bot_token="DRYRUN",
            telegram_chat_id="DRYRUN",
            journal_dir=str(alert_root / "journal"),
            state_dir=str(alert_root / "state"),
            deletion_registry_dir=str(alert_root / "deletion_registry"),
            log_dir=str(alert_root / "logs"),
            auto_delete_enabled=False,
            heartbeat_interval_sec=10**9,
            debug=True,
        ),
        snapshot_getter=lambda: ws_pipeline.snapshot_store,
        market_context_getter=module03.get_state,
        health_getter=lambda: {
            "broker": broker.health(),
            "websocket": {"ws_connected": False, "state": "OFFMARKET_TEST"},
        },
        shared_state={},
    )
    alert_engine.telegram = DryRunTelegramSender()
    alert_engine.start()

    supervisor = Supervisor(
        broker=broker,
        ws_pipeline=ws_pipeline,
        module03=module03,
        scanner=scanner,
        strategy_engine=strategy_engine,
        alert_engine=alert_engine,
        maintenance=maintenance,
    )
    offmarket_ctx = {"live_market": False, "offmarket_test": True, "mode_name": "OFFMARKET_TEST"}
    supervisor._mode_context = lambda: dict(offmarket_ctx)  # type: ignore[assignment]

    try:
        health_ok = supervisor._health_check()
        _status(results, "supervisor_offmarket_health", health_ok, "off-market health stayed OK without websocket" if health_ok else "off-market health returned False")
    except Exception as e:
        _status(results, "supervisor_offmarket_health", False, str(e))

    try:
        module04_output = scanner.run_scan()
        candidate_count = len(module04_output.get("candidate_list") or [])
        backup_count = len(module04_output.get("candidate_backup_list") or [])
        scanner_extra = _scanner_diag_payload(module04_output if isinstance(module04_output, dict) else {})
        structural_ok = isinstance(module04_output, dict) and isinstance(module04_output.get("scanner_diagnostics", {}), dict)
        _status(
            results,
            "scanner_executes_offmarket",
            structural_ok,
            (
                f"candidate_list={candidate_count}, candidate_backup_list={backup_count}, "
                f"stage1_survivors={scanner_extra['stage1_survivors_count']}, degraded={scanner_extra['degraded']}"
            ),
            extra=scanner_extra,
        )
        candidate_generation_ok = (candidate_count + backup_count) > 0
        _status(
            results,
            "scanner_generates_candidates_offmarket",
            candidate_generation_ok,
            f"candidate_list={candidate_count}, candidate_backup_list={backup_count}",
            extra=scanner_extra,
        )
    except Exception as e:
        module04_output = {"candidate_list": [], "candidate_backup_list": [], "scanner_diagnostics": {"notes": [str(e)]}}
        scanner_extra = _scanner_diag_payload(module04_output)
        _status(results, "scanner_executes_offmarket", False, str(e), extra=scanner_extra)
        _status(results, "scanner_generates_candidates_offmarket", False, "candidate_list=0, candidate_backup_list=0", extra=scanner_extra)

    try:
        candidates, backups, snapshot_store, candle_store_5m = supervisor._prepare_strategy_inputs(module04_output, offmarket_ctx)
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
        signal_count = len(module05_output.get("trade_signal_list") or [])
        _status(results, "strategy_receives_inputs_offmarket", isinstance(module05_output, dict) and "strategy_diagnostics" in module05_output, f"candidates={len(candidates)}, backups={len(backups)}, signals={signal_count}")
    except Exception as e:
        module05_output = {"trade_signal_list": [], "trade_plan_map": {}, "strategy_diagnostics": {}, "errors": [str(e)]}
        _status(results, "strategy_receives_inputs_offmarket", False, str(e))

    alert_candidates = list(module04_output.get("candidate_list") or []) or list(module04_output.get("candidate_backup_list") or [])
    used_alert_fallback = not bool(alert_candidates)
    if not alert_candidates:
        fallback_snap = ws_pipeline.get_snapshots_bulk([{"symbol": "RELIANCE", "exchange": "NSE", "token": "2885"}])
        fallback_row = fallback_snap.get("NSE:RELIANCE") or fallback_snap.get("2885") or {}
        alert_candidates = [
            {
                "symbol": "RELIANCE",
                "token": "2885",
                "exchange": "NSE",
                "sector": "ENERGY",
                "tier": "VALIDATION",
                "score": 70.0,
                "composite_score": 70.0,
                "component_scores": {},
                "snapshot": fallback_row,
                "candles_5m": broker.get_5m_candles(symbol="RELIANCE", token="2885", exchange="NSE", lookback=80),
                "meta": {},
            }
        ]

    alert_module04_output = dict(module04_output)
    alert_module04_output["candidate_list"] = alert_candidates[:3]

    try:
        menu_id = alert_engine.publish_from_module_outputs(
            module04_output=alert_module04_output,
            module05_output=module05_output,
            module03_output=module03.get_state(),
            force_menu=True,
        )
        time.sleep(1.0)
        health = alert_engine.health_dict()
        sent_count = int(health.get("messages_sent", 0))
        _status(
            results,
            "alerts_format_test_output",
            bool(menu_id) and sent_count >= 1,
            f"menu_id={menu_id}, messages_sent={sent_count}, fallback_candidate_used={used_alert_fallback}",
            extra={"fallback_candidate_used": used_alert_fallback},
        )
    except Exception as e:
        _status(
            results,
            "alerts_format_test_output",
            False,
            str(e),
            extra={"fallback_candidate_used": used_alert_fallback},
        )
    finally:
        alert_engine.stop()

    _write_summary(summary_path, results)
    return 0 if all(r["status"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
