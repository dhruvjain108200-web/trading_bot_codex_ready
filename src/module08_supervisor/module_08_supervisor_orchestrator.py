# MODULE 08 — Supervisor / Orchestrator (Lifecycle + Governance + Scheduling + Recovery)
# Purpose: Coordinate Modules 01–07 in SIGNALS_ONLY mode with Angel One SmartAPI, enforce trade governance,
# supervise worker health, and run the 5-minute intraday control loop safely in Google Colab.

import os
import json
import time
import threading
from datetime import datetime


STATE_FILE = "/content/drive/MyDrive/trading_bot/state/system_state.json"


class Supervisor:

    def __init__(
        self,
        broker,
        ws_pipeline,
        module03,
        scanner,
        strategy_engine,
        alert_engine,
        maintenance
    ):
        """
        Dependencies injected from existing modules.
        """
        self.broker = broker
        self.ws_pipeline = ws_pipeline
        self.module03 = module03
        self.scanner = scanner
        self.strategy_engine = strategy_engine
        self.alert_engine = alert_engine
        self.maintenance = maintenance

        # execution lock
        self.execution_mode = "SIGNALS_ONLY"
        self.execution_enabled = False

        # runtime state
        self.system_state = "STOPPED"

        # pacing
        self.current_fraction = 1
        self.current_set = 1
        self.trades_taken = 0
        self.wins_in_set = 0
        self.losses_in_set = 0
        self.set_locked = False
        self.fraction_locked = False
        self.daily_trade_count = 0
        self.consecutive_losses = 0

        # runtime context
        self.last_scan_bucket = None
        self.last_scan_ts = None
        self.last_publish_ts = None
        self.last_cycle_status = None
        self.last_menu_id = None

        # module outputs
        self.last_module03_output = None
        self.last_module04_output = None
        self.last_module05_output = None

        # control
        self._stop_event = threading.Event()
        self._thread = None

        # safety
        self.max_daily_trades = 12
        self.max_consecutive_losses = 3

        try:
            shared_state = getattr(self.alert_engine, "shared_state", None)
            if isinstance(shared_state, dict):
                shared_state["strategy_engine"] = self.strategy_engine
        except Exception:
            pass

        self._load_state()

    # ---------------------------------------------------------
    # State persistence
    # ---------------------------------------------------------

    def _save_state(self):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

        state = {
            "system_state": self.system_state,
            "execution_mode": self.execution_mode,
            "execution_enabled": self.execution_enabled,
            "current_fraction": self.current_fraction,
            "current_set": self.current_set,
            "wins_in_set": self.wins_in_set,
            "losses_in_set": self.losses_in_set,
            "set_locked": self.set_locked,
            "fraction_locked": self.fraction_locked,
            "daily_trade_count": self.daily_trade_count,
            "consecutive_losses": self.consecutive_losses,
            "last_scan_bucket": self.last_scan_bucket,
            "last_scan_ts": self.last_scan_ts,
            "last_publish_ts": self.last_publish_ts,
            "last_cycle_status": self.last_cycle_status,
            "last_menu_id": self.last_menu_id,
            "updated_at": int(time.time())
        }

        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return

        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)

            self.system_state = state.get("system_state", "STOPPED")
            self.current_fraction = state.get("current_fraction", 1)
            self.current_set = state.get("current_set", 1)
            self.wins_in_set = state.get("wins_in_set", 0)
            self.losses_in_set = state.get("losses_in_set", 0)
            self.set_locked = state.get("set_locked", False)
            self.fraction_locked = state.get("fraction_locked", False)
            self.daily_trade_count = state.get("daily_trade_count", 0)
            self.consecutive_losses = state.get("consecutive_losses", 0)
            self.last_scan_bucket = state.get("last_scan_bucket")
            self.last_scan_ts = state.get("last_scan_ts")
            self.last_publish_ts = state.get("last_publish_ts")
            self.last_cycle_status = state.get("last_cycle_status")
            self.last_menu_id = state.get("last_menu_id")

        except Exception:
            pass

    # ---------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------

    def start(self):
        if self._thread and self._thread.is_alive():
            return

        self.system_state = "STARTING"
        self._stop_event.clear()

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.system_state = "STOPPING"
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=10)

        self.system_state = "STOPPED"
        self._save_state()

    def pause(self):
        self.system_state = "PAUSED"
        self._save_state()

    def resume(self):
        self.system_state = "RUNNING"
        self._save_state()


    # ---------------------------------------------------------
    # Mode Control (COMMON / LIVE / OFFMARKET)
    # ---------------------------------------------------------

    def _is_live_market(self) -> bool:
        try:
            if hasattr(self.scanner, "_mode_context"):
                ctx = self.scanner._mode_context()
                return bool(ctx.get("live_market"))
        except Exception:
            pass

        now = datetime.now()
        if now.weekday() >= 5:
            return False

        hhmm = now.hour * 100 + now.minute
        return 915 <= hhmm <= 1530

    def _mode_context(self):
        live_market = self._is_live_market()
        return {
            "live_market": live_market,
            "offmarket_test": not live_market,
            "mode_name": "LIVE" if live_market else "OFFMARKET_TEST",
        }

    # ---------------------------------------------------------
    # Health monitoring
    # ---------------------------------------------------------

    def _health_check(self):
        mode_ctx = self._mode_context()

        if mode_ctx.get("offmarket_test"):
            return True

        try:
            ws_health = self.ws_pipeline.health
            ws_connected = ws_health.ws_connected

            if not ws_connected:
                return False

        except Exception:
            return False

        return True

    @staticmethod
    def _validation_check_health_modes():
        class _DummyHealth:
            def __init__(self, ws_connected):
                self.ws_connected = ws_connected

        class _DummyPipeline:
            def __init__(self, ws_connected):
                self.health = _DummyHealth(ws_connected)

        sup = Supervisor.__new__(Supervisor)
        sup.ws_pipeline = _DummyPipeline(ws_connected=False)

        sup._mode_context = lambda: {
            "live_market": False,
            "offmarket_test": True,
            "mode_name": "OFFMARKET_TEST",
        }
        offmarket_ok = sup._health_check()

        sup._mode_context = lambda: {
            "live_market": True,
            "offmarket_test": False,
            "mode_name": "LIVE",
        }
        live_requires_ws = (sup._health_check() is False)

        return {
            "offmarket_without_ws_is_healthy": offmarket_ok,
            "live_without_ws_is_unhealthy": live_requires_ws,
        }

    # ---------------------------------------------------------
    # Governance
    # ---------------------------------------------------------

    def _signals_allowed(self, market_context):

        if self.system_state != "RUNNING":
            return False

        state = market_context.get("market_context_state", market_context) or {}
        trade_gate = str(state.get("trade_gate", "ALLOW")).upper()

        if trade_gate == "PAUSE":
            return False

        if self.set_locked:
            return False

        if self.fraction_locked:
            return False

        if self.daily_trade_count >= self.max_daily_trades:
            return False

        if self.consecutive_losses >= self.max_consecutive_losses:
            return False

        return True

    # ---------------------------------------------------------
    # Strategy Input Preparation (COMMON / LIVE / OFFMARKET)
    # ---------------------------------------------------------

    def _extract_candidates(self, module04_output):
        candidates = (
            module04_output.get("candidate_list")
            or module04_output.get("top_candidates")
            or module04_output.get("candidates")
            or module04_output.get("ranked_candidates")
            or []
        )

        backup = (
            module04_output.get("candidate_backup_list")
            or module04_output.get("backup_candidates")
            or []
        )
        return candidates, backup

    def _build_offmarket_snapshot_store(self, candidates):
        out = {}
        for c in candidates or []:
            symbol = c.get("symbol")
            if not symbol:
                continue
            out[symbol] = (c.get("snapshot") or {})
        return out

    def _build_offmarket_candle_store_5m(self, candidates):
        out = {}
        for c in candidates or []:
            symbol = c.get("symbol")
            if not symbol:
                continue
            out[symbol] = c.get("candles_5m", []) or []
        return out

    def _build_live_snapshot_store(self, candidates):
        try:
            return self.ws_pipeline.get_snapshots_bulk(candidates) or {}
        except Exception:
            return {}

    def _build_live_candle_store_5m(self, candidates):
        candle_store_5m = {}
        for c in candidates or []:
            symbol = c.get("symbol")
            token = c.get("token")
            exchange = c.get("exchange", "NSE")

            if not symbol or not token:
                continue

            try:
                candles = self.scanner._get_candles(symbol, str(token), exchange, 80)
            except Exception:
                candles = []

            candle_store_5m[symbol] = candles or []
        return candle_store_5m

    def _prepare_strategy_inputs(self, module04_output, mode_ctx):
        candidates, backup = self._extract_candidates(module04_output)

        if mode_ctx["live_market"]:
            snapshot_store = self._build_live_snapshot_store(candidates)
            candle_store_5m = self._build_live_candle_store_5m(candidates)
        else:
            snapshot_store = self._build_offmarket_snapshot_store(candidates)
            candle_store_5m = self._build_offmarket_candle_store_5m(candidates)

        return candidates, backup, snapshot_store, candle_store_5m

    # ---------------------------------------------------------
    # Live Watchlist Sync
    # ---------------------------------------------------------

    def _extract_watchlist_tokens_from_candidates(self, candidates):
        toks = []
        seen = set()

        for c in candidates or []:
            tok = str(c.get("token", "")).strip()
            if tok and tok not in seen:
                seen.add(tok)
                toks.append(tok)

        return toks

    def _extract_watchlist_tokens_from_universe(self, universe):
        toks = []
        seen = set()

        for rec in universe or []:
            tok = str(rec.get("token", "")).strip()
            if tok and tok not in seen:
                seen.add(tok)
                toks.append(tok)

        return toks

    def _extract_universe_records(self, universe_bundle):
        if isinstance(universe_bundle, dict):
            universe = universe_bundle.get("universe", [])
            return universe if isinstance(universe, list) else []
        if isinstance(universe_bundle, list):
            return universe_bundle
        return []

    def _seed_live_watchlist_from_universe(self):
        try:
            universe_bundle = self.scanner._build_merged_universe()
        except Exception:
            universe_bundle = []

        universe = self._extract_universe_records(universe_bundle)
        toks = self._extract_watchlist_tokens_from_universe(universe)
        if toks:
            self.ws_pipeline.set_watchlist_tokens(toks)

        return toks

    def _prime_live_runtime(self, connect_timeout_sec=20.0, snapshot_warmup_sec=3.0):
        seeded_tokens = self._seed_live_watchlist_from_universe()

        try:
            self.ws_pipeline.start()
        except Exception:
            return {
                "ws_connected": False,
                "seeded_tokens": seeded_tokens,
            }

        deadline = time.time() + max(1.0, float(connect_timeout_sec))
        while time.time() < deadline:
            try:
                if bool(self.ws_pipeline.health.ws_connected):
                    break
            except Exception:
                pass
            time.sleep(0.25)

        try:
            ws_connected = bool(self.ws_pipeline.health.ws_connected)
        except Exception:
            ws_connected = False

        if ws_connected:
            time.sleep(max(2.0, float(snapshot_warmup_sec)))

        return {
            "ws_connected": ws_connected,
            "seeded_tokens": seeded_tokens,
        }

    @staticmethod
    def _validation_check_live_watchlist_seed_contract():
        class _DummyScanner:
            @staticmethod
            def _build_merged_universe():
                return {
                    "universe": [
                        {"symbol": "RELIANCE", "exchange": "NSE", "token": "2885"},
                        {"symbol": "INFY", "exchange": "NSE", "token": "1594"},
                    ],
                    "merged_unique_count": 2,
                }

        class _DummyPipeline:
            def __init__(self):
                self.tokens = []

            def set_watchlist_tokens(self, tokens):
                self.tokens = list(tokens or [])

        sup = Supervisor.__new__(Supervisor)
        sup.scanner = _DummyScanner()
        sup.ws_pipeline = _DummyPipeline()

        seeded = sup._seed_live_watchlist_from_universe()
        return {
            "reads_universe_bundle": seeded == ["2885", "1594"],
            "writes_watchlist_tokens": sup.ws_pipeline.tokens == ["2885", "1594"],
        }

    def _sync_live_watchlist_tokens(self, module04_output):
        candidates, backup = self._extract_candidates(module04_output)
        combined = list(candidates or []) + list(backup or [])

        toks = self._extract_watchlist_tokens_from_candidates(combined)
        if toks:
            self.ws_pipeline.set_watchlist_tokens(toks)

        return toks    

    # ---------------------------------------------------------
    # Scan cycle
    # ---------------------------------------------------------

    def _run_scan_cycle(self):
        now_ts = int(time.time())
        mode_ctx = self._mode_context()

        try:
            module03_output = self.module03.get_state()
            self.last_module03_output = module03_output

            module04_output = self.scanner.run_scan()
            self.last_module04_output = module04_output
            if mode_ctx["live_market"]:
                seeded_wl_tokens = self._seed_live_watchlist_from_universe()
                if seeded_wl_tokens:
                    time.sleep(2)
            else:
                seeded_wl_tokens = []

            candidates, backup, snapshot_store, candle_store_5m = self._prepare_strategy_inputs(
                module04_output,
                mode_ctx,
            )

            module05_output = self.strategy_engine.run_scan(
                candidate_list=candidates,
                candidate_backup_list=backup,
                market_context=module03_output,
                snapshot_store=snapshot_store,
                candle_store_5m=candle_store_5m,
                candle_store_1m={},
                candle_store_15m={},
                now_ts=now_ts
            )

            self.last_module05_output = module05_output

            signals_allowed = self._signals_allowed(module03_output)

            if signals_allowed:
                force_menu = False
                self.last_cycle_status = (
                    "CYCLE_RAN_SIGNALS_RELEASED"
                    if mode_ctx["live_market"]
                    else "OFFMARKET_CYCLE_RAN_SIGNALS_RELEASED"
                )
            else:
                force_menu = True
                self.last_cycle_status = (
                    "CYCLE_RAN_SIGNALS_SUPPRESSED"
                    if mode_ctx["live_market"]
                    else "OFFMARKET_CYCLE_RAN_SIGNALS_SUPPRESSED"
                )

            self.alert_engine.publish_from_module_outputs(
                module04_output,
                module05_output,
                module03_output,
                force_menu=force_menu
            )

            self.last_scan_ts = now_ts
            self.last_publish_ts = now_ts

        except Exception as e:
            self.last_cycle_status = "CYCLE_FAILED"

            try:
                self.alert_engine.publish_from_module_outputs(
                    {},
                    {},
                    {"notes": [str(e)]},
                    force_menu=False
                )
            except Exception:
                pass

        self._save_state()

    # ---------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------

    def _run_loop(self):

        try:

            # maintenance readiness
            if not self.maintenance.is_ready():
                self.system_state = "PAUSED"
                return

            mode_ctx = self._mode_context()

            if mode_ctx["live_market"]:
                prime_status = self._prime_live_runtime()
                self.system_state = "RUNNING" if prime_status.get("ws_connected") else "DEGRADED"
            else:
                self.system_state = "RUNNING"

            while not self._stop_event.is_set():

                now_ts = int(time.time())

                # FAST LOOP
                try:
                    self.alert_engine.poll_telegram_once()
                except Exception:
                    pass

                try:
                    self.alert_engine.run_signal_lifecycle_check()
                except Exception:
                    pass

                # health check
                if self._health_check():
                    if self.system_state in ("STARTING", "DEGRADED", "RUNNING_IDLE"):
                        self.system_state = "RUNNING"
                else:
                     self.system_state = "DEGRADED"

                # 5-minute bucket control
                bucket = int(now_ts // 300)

                if bucket != self.last_scan_bucket:

                    if self.system_state == "RUNNING":
                        self._run_scan_cycle()

                    self.last_scan_bucket = bucket

                time.sleep(3)

        finally:
            self.system_state = "STOPPED"
            self._save_state()

    # ---------------------------------------------------------
    # Command handlers (from Module 06)
    # ---------------------------------------------------------

    def register_outcome(self, outcome):

        if outcome == "WIN":
            self.wins_in_set += 1
            self.consecutive_losses = 0

        elif outcome == "LOSS":
            self.losses_in_set += 1
            self.consecutive_losses += 1

        if self.wins_in_set >= 2:
            self.set_locked = True

        if self.consecutive_losses >= self.max_consecutive_losses:
            self.system_state = "PAUSED"

        self._save_state()

    def reset_day(self):

        self.current_fraction = 1
        self.current_set = 1
        self.trades_taken = 0
        self.wins_in_set = 0
        self.losses_in_set = 0
        self.set_locked = False
        self.fraction_locked = False
        self.daily_trade_count = 0
        self.consecutive_losses = 0

        self._save_state()

    # ---------------------------------------------------------
    # Status
    # ---------------------------------------------------------

    def status(self):

        return {
            "system_state": self.system_state,
            "execution_mode": self.execution_mode,
            "execution_enabled": self.execution_enabled,
            "fraction": self.current_fraction,
            "set": self.current_set,
            "wins_in_set": self.wins_in_set,
            "losses_in_set": self.losses_in_set,
            "set_locked": self.set_locked,
            "fraction_locked": self.fraction_locked,
            "daily_trade_count": self.daily_trade_count,
            "consecutive_losses": self.consecutive_losses,
            "last_scan_ts": self.last_scan_ts,
            "last_cycle_status": self.last_cycle_status,
        }
