# MODULE 06 — Alert Engine (Telegram + Health + Learning + Journaling)
# Purpose:
#   Real-time Telegram communication, operational visibility, alert journaling,
#   active signal tracking, menu registry, follow-up detection, and message cleanup metadata.
#
# Design notes:
# - Does NOT generate signals or place orders.
# - Does NOT import SmartAPI.
# - Consumes outputs/health/snapshots from Modules 01–05 and 07 or shared state.
# - Google Colab safe: bounded queue, lightweight workers, bounded registries, defensive retries.
# - Compatible with file-based state on Drive.
#
# Python 3.10+ compatible.

from __future__ import annotations

import os
import re
import json
import time
import math
import itertools
import queue
import hashlib
import logging
import threading
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests


# =============================================================================
# Time / Date Helpers
# =============================================================================

IST = timezone(timedelta(hours=5, minutes=30))


def now_ts() -> float:
    return time.time()


def now_ist() -> datetime:
    return datetime.now(IST)


def ts_to_ist_str(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), IST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def today_ist_str() -> str:
    return now_ist().strftime("%Y-%m-%d")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def compact_json_hash(obj: Any) -> str:
    try:
        raw = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        raw = str(obj)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def truncate_text(text: str, limit: int = 3500) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class AlertEngineConfig:
    # Telegram
    telegram_bot_token: str
    telegram_chat_id: str
    telegram_base_url: str = "https://api.telegram.org"
    telegram_timeout_sec: int = 15
    telegram_parse_mode: Optional[str] = None  # None, "Markdown", "HTML"

    # Storage / Paths
    journal_dir: str = "/content/drive/MyDrive/trading_bot/runtime/alert_journal"
    state_dir: str = "/content/drive/MyDrive/trading_bot/runtime/alert_state"
    deletion_registry_dir: str = "/content/drive/MyDrive/trading_bot/runtime/deletion_registry"
    log_dir: str = "/content/drive/MyDrive/trading_bot/runtime/logs"

    # Worker / Queue
    send_queue_maxsize: int = 300
    max_active_signals: int = 200
    max_menu_registry: int = 30
    max_message_registry_per_day: int = 1500
    sender_poll_sec: float = 0.35
    expiry_check_sec: float = 12.0
    followup_check_sec: float = 10.0
    cleanup_check_sec: float = 120.0
    heartbeat_interval_sec: int = 15 * 60

    # Retries / Rate Limit
    send_soft_cap_per_minute: int = 20
    retry_attempts: int = 3
    retry_backoff_sec: float = 1.5
    retry_backoff_multiplier: float = 1.8

    # Cooldowns / Dedup
    menu_cooldown_sec: int = 240
    plan_cooldown_sec: int = 8 * 60
    explain_cooldown_sec: int = 8 * 60
    heartbeat_cooldown_sec: int = 10 * 60
    symbol_alert_cooldown_sec: int = 180
    duplicate_hash_ttl_sec: int = 30 * 60

    # Price / Snapshot / Expiry / Follow-up
    snapshot_stale_sec: int = 20
    invalidation_sl_buffer_pct: float = 0.0005
    t1_rearm_buffer_pct: float = 0.0002
    t2_trigger_buffer_pct: float = 0.0002
    degraded_rest_fallback: bool = False  # kept false by default; module prefers snapshots

    # Menu / Publish Rules
    send_menu_on_every_cycle: bool = False
    menu_change_threshold_fraction: float = 0.30
    max_menu_candidates: int = 20
    max_recommended_signals_per_cycle: int = 3

    # Auto-delete / EOD
    auto_delete_enabled: bool = True
    market_close_hour: int = 15
    market_close_minute: int = 30
    auto_delete_buffer_min: int = 30
    keep_daily_summary: bool = True

    # Logging / Debug
    debug: bool = False
    logger_name: str = "module06_alert_engine"

    # Integration Flags
    allow_shared_state_read: bool = True
    heartbeat_dedup_enabled: bool = True


@dataclass
class AlertEngineHealth:
    state: str = "INIT"
    started_at: float = 0.0
    last_menu_id: Optional[str] = None
    last_send_time: Optional[float] = None
    last_send_iso: Optional[str] = None
    messages_sent: int = 0
    messages_failed: int = 0
    retries: int = 0
    queue_size: int = 0
    active_signal_count: int = 0
    last_error: Optional[str] = None
    degraded: bool = False
    last_heartbeat_time: Optional[float] = None
    last_followup_check: Optional[float] = None
    last_expiry_check: Optional[float] = None
    sender_loop_alive: bool = False
    expiry_loop_alive: bool = False
    followup_loop_alive: bool = False
    cleanup_loop_alive: bool = False
    telegram_last_error: Optional[str] = None
    telegram_last_error_time: Optional[float] = None
    messages_dropped: int = 0
    queue_highwater: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.last_send_time:
            d["last_send_iso"] = ts_to_ist_str(self.last_send_time)
        return d


@dataclass
class QueueItem:
    priority: int
    created_at: float
    kind: str
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    parse_mode: Optional[str] = None
    disable_notification: bool = False
    retry_count: int = 0
    dedup_key: Optional[str] = None
    bypass_cooldown: bool = False
    bypass_rate_limit: bool = False

    def pq_tuple(self) -> Tuple[int, float, str]:
        return (self.priority, self.created_at, self.kind)


@dataclass
class MenuCandidate:
    letter: str
    symbol: str
    tier: str = ""
    score: float = 0.0
    ltp: Optional[float] = None
    sector: str = ""
    regime_tag: str = ""
    recommended: bool = False
    direction: str = ""
    setup_name: str = ""


@dataclass
class MenuState:
    menu_id: str
    created_at: float
    menu_hash: str
    candidates: List[MenuCandidate] = field(default_factory=list)
    candidate_lookup: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    recommended_symbols: List[str] = field(default_factory=list)
    trade_gate: str = ""
    market_regime: str = ""
    macro_bias: str = ""
    warning_tag: str = ""
    source_cycle_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "menu_id": self.menu_id,
            "created_at": self.created_at,
            "created_at_ist": ts_to_ist_str(self.created_at),
            "menu_hash": self.menu_hash,
            "candidates": [asdict(c) for c in self.candidates],
            "candidate_lookup": self.candidate_lookup,
            "recommended_symbols": self.recommended_symbols,
            "trade_gate": self.trade_gate,
            "market_regime": self.market_regime,
            "macro_bias": self.macro_bias,
            "warning_tag": self.warning_tag,
            "source_cycle_id": self.source_cycle_id,
        }


@dataclass
class SignalState:
    symbol: str
    menu_id: str
    direction: str
    setup_name: str
    entry: Optional[float]
    sl: Optional[float]
    t1: Optional[float]
    t2: Optional[float]
    confidence: Optional[float]
    expires_at: Optional[float]
    created_at: float
    updated_at: float
    status: str = "ACTIVE"
    t1_sent: bool = False
    t2_sent: bool = False
    expiry_sent: bool = False
    invalidation_sent: bool = False
    trailing_sent: bool = False
    symbol_token: Optional[str] = None
    last_price: Optional[float] = None
    last_tick_time: Optional[float] = None
    signal_hash: Optional[str] = None
    source_plan: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["created_at_ist"] = ts_to_ist_str(self.created_at)
        d["updated_at_ist"] = ts_to_ist_str(self.updated_at)
        d["expires_at_ist"] = ts_to_ist_str(self.expires_at)
        return d


@dataclass
class MessageRegistryItem:
    timestamp: float
    menu_id: Optional[str]
    alert_type: str
    telegram_message_id: Optional[int]
    chat_id: str
    deletable: bool = True
    expiry_time: Optional[float] = None
    scheduled_delete_after: Optional[float] = None
    symbol: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamp_ist"] = ts_to_ist_str(self.timestamp)
        d["expiry_time_ist"] = ts_to_ist_str(self.expiry_time)
        d["scheduled_delete_after_ist"] = ts_to_ist_str(self.scheduled_delete_after)
        return d


# =============================================================================
# Telegram Adapter
# =============================================================================

class TelegramSenderAdapter:
    def __init__(self, config: AlertEngineConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()

    def _url(self, method: str) -> str:
        token = self.config.telegram_bot_token.strip()
        return f"{self.config.telegram_base_url}/bot{token}/{method}"

    def send_message(
        self,
        text: str,
        parse_mode: Optional[str] = None,
        disable_notification: bool = False,
        chat_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        target_chat_id = chat_id if chat_id is not None else self.config.telegram_chat_id
        payload = {
            "chat_id": target_chat_id,
            "text": truncate_text(text, 3900),
            "disable_notification": disable_notification,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = self.session.post(
            self._url("sendMessage"),
            json=payload,
            timeout=self.config.telegram_timeout_sec,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"ok": False, "status_code": resp.status_code, "text": resp.text[:500]}

        if not resp.ok or not data.get("ok", False):
            raise RuntimeError(f"Telegram send failed: {data}")
        return data

    def delete_message(self, message_id: int, chat_id: Optional[Any] = None) -> Dict[str, Any]:
        target_chat_id = chat_id if chat_id is not None else self.config.telegram_chat_id
        payload = {"chat_id": target_chat_id, "message_id": int(message_id)}
        resp = self.session.post(
            self._url("deleteMessage"),
            json=payload,
            timeout=self.config.telegram_timeout_sec,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"ok": False, "status_code": resp.status_code, "text": resp.text[:500]}
        if not resp.ok or not data.get("ok", False):
            raise RuntimeError(f"Telegram delete failed: {data}")
        return data

    def get_updates(
        self,
        offset: Optional[int] = None,
        timeout: int = 10,
        allowed_updates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "timeout": int(timeout),
        }
        if offset is not None:
            payload["offset"] = int(offset)
        if allowed_updates:
            payload["allowed_updates"] = allowed_updates

        resp = self.session.post(
            self._url("getUpdates"),
            json=payload,
            timeout=max(self.config.telegram_timeout_sec, timeout + 5),
        )
        try:
            data = resp.json()
        except Exception:
            data = {"ok": False, "status_code": resp.status_code, "text": resp.text[:500]}

        if not resp.ok or not data.get("ok", False):
            raise RuntimeError(f"Telegram getUpdates failed: {data}")

        result = data.get("result", [])
        return result if isinstance(result, list) else []


# =============================================================================
# Main Alert Engine
# =============================================================================

class AlertEngine:
    PRIORITY_CRITICAL = 0
    PRIORITY_MENU = 1
    PRIORITY_PLAN = 2
    PRIORITY_FOLLOWUP = 3
    PRIORITY_EXPLAIN = 4
    PRIORITY_HEARTBEAT = 5

    ALERT_MENU = "MENU"
    ALERT_PLAN = "PLAN"
    ALERT_EXPLAIN = "EXPLAIN"
    ALERT_FOLLOWUP = "FOLLOWUP"
    ALERT_HEARTBEAT = "HEARTBEAT"
    ALERT_ERROR = "ERROR"
    ALERT_DELETE = "DELETE"

    def __init__(
        self,
        config: AlertEngineConfig,
        snapshot_getter: Optional[Callable[[], Dict[str, Any]]] = None,
        market_context_getter: Optional[Callable[[], Dict[str, Any]]] = None,
        health_getter: Optional[Callable[[], Dict[str, Any]]] = None,
        shared_state: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.snapshot_getter = snapshot_getter
        self.market_context_getter = market_context_getter
        self.health_getter = health_getter
        self.shared_state = shared_state if shared_state is not None else {}

        ensure_dir(self.config.journal_dir)
        ensure_dir(self.config.state_dir)
        ensure_dir(self.config.deletion_registry_dir)
        ensure_dir(self.config.log_dir)

        self.logger = logger or self._build_logger()
        self.telegram = TelegramSenderAdapter(self.config, self.logger)

        self.health = AlertEngineHealth(started_at=now_ts())

        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        self._send_queue: "queue.PriorityQueue[Tuple[int, float, int, str, QueueItem]]" = queue.PriorityQueue(
            maxsize=self.config.send_queue_maxsize
        )
        self._queue_counter = itertools.count()

        self._sender_thread = threading.Thread(target=self._sender_loop, name="AlertEngineSender", daemon=True)
        self._expiry_thread = threading.Thread(target=self._expiry_loop, name="AlertEngineExpiry", daemon=True)
        self._followup_thread = threading.Thread(target=self._followup_loop, name="AlertEngineFollowup", daemon=True)
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, name="AlertEngineCleanup", daemon=True)
        self._poll_thread = threading.Thread(target=self._poll_updates_loop, name="AlertEngineTelegramPoll", daemon=True)

        self._menus: Dict[str, MenuState] = {}
        self._latest_menu_id: Optional[str] = None

        self._active_signals: Dict[str, SignalState] = {}
        self._message_registry: List[MessageRegistryItem] = []

        self._last_menu_hash: Optional[str] = None
        self._last_menu_time: Optional[float] = None
        self._last_heartbeat_hash: Optional[str] = None
        self._last_heartbeat_time: Optional[float] = None

        self._last_symbol_alert_ts: Dict[str, float] = {}
        self._last_plan_hash_by_symbol: Dict[str, str] = {}
        self._last_explain_hash_by_symbol: Dict[str, str] = {}
        self._dedup_cache: Dict[str, float] = {}
        self._send_timestamps: List[float] = []

        self._menu_serial_seed = now_ist().microsecond
        self._menu_counter = 0
        self._last_update_id: Optional[int] = None

        self._persist_file_menus = os.path.join(self.config.state_dir, "menus.json")
        self._persist_file_signals = os.path.join(self.config.state_dir, "active_signals.json")
        self._persist_file_health = os.path.join(self.config.state_dir, "module06_health.json")
        self._persist_file_message_registry = os.path.join(
            self.config.deletion_registry_dir, f"messages_{today_ist_str()}.json"
        )

        mode_ctx = self._mode_context()
        self._load_persisted_state(load_menus=not mode_ctx.get("offmarket_test"))
        if mode_ctx.get("offmarket_test"):
            self._reset_offmarket_menu_state(clear_persisted=True)

    # -------------------------------------------------------------------------
    # Logger
    # -------------------------------------------------------------------------

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.config.logger_name)
        logger.setLevel(logging.DEBUG if self.config.debug else logging.INFO)
        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )

            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)

            try:
                fh = logging.FileHandler(
                    os.path.join(self.config.log_dir, "module_06_alert_engine.log"),
                    encoding="utf-8",
                )
                fh.setFormatter(formatter)
                logger.addHandler(fh)
            except Exception:
                pass
        logger.propagate = False
        return logger

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        mode_ctx = self._mode_context()
        with self._lock:
            if self.health.state == "RUNNING":
                return
            self._stop_event.clear()
            self.health.state = "RUNNING"
            self.health.started_at = self.health.started_at or now_ts()

        if mode_ctx.get("offmarket_test"):
            self._reset_offmarket_menu_state(clear_persisted=True)

        if not self._sender_thread.is_alive():
            self._sender_thread = threading.Thread(target=self._sender_loop, name="AlertEngineSender", daemon=True)
            self._sender_thread.start()

        if not self._expiry_thread.is_alive():
            self._expiry_thread = threading.Thread(target=self._expiry_loop, name="AlertEngineExpiry", daemon=True)
            self._expiry_thread.start()

        if not self._followup_thread.is_alive():
            self._followup_thread = threading.Thread(target=self._followup_loop, name="AlertEngineFollowup", daemon=True)
            self._followup_thread.start()

        if not self._cleanup_thread.is_alive():
            self._cleanup_thread = threading.Thread(target=self._cleanup_loop, name="AlertEngineCleanup", daemon=True)
            self._cleanup_thread.start()

        if not self._poll_thread.is_alive():
            self._poll_thread = threading.Thread(target=self._poll_updates_loop, name="AlertEngineTelegramPoll", daemon=True)
            self._poll_thread.start()

        self._persist_health()
        self.logger.info("AlertEngine started.")

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            self.health.state = "PAUSED"
        self._persist_state()
        self.logger.info("AlertEngine stopped.")

    def health_dict(self) -> Dict[str, Any]:
       with self._lock:
            self.health.queue_size = self._safe_qsize()
            self.health.active_signal_count = len(self._active_signals)
            self.health.sender_loop_alive = self._sender_thread.is_alive()
            self.health.expiry_loop_alive = self._expiry_thread.is_alive()
            self.health.followup_loop_alive = self._followup_thread.is_alive()
            self.health.cleanup_loop_alive = self._cleanup_thread.is_alive()
            return self.health.to_dict()

    def _clean_signal_state_dict(self, item: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            "symbol",
            "menu_id",
            "direction",
            "setup_name",
            "entry",
            "sl",
            "t1",
            "t2",
            "confidence",
            "expires_at",
            "created_at",
            "updated_at",
            "status",
            "t1_sent",
            "t2_sent",
            "expiry_sent",
            "invalidation_sent",
            "trailing_sent",
            "symbol_token",
            "last_price",
            "last_tick_time",
            "signal_hash",
            "source_plan",
        }

        cleaned = {k: v for k, v in (item or {}).items() if k in allowed}

        now = now_ts()
        cleaned.setdefault("symbol", str(item.get("symbol", "")).strip() if isinstance(item, dict) else "")
        cleaned.setdefault("menu_id", "")
        cleaned.setdefault("direction", "LONG")
        cleaned.setdefault("setup_name", "SETUP")
        cleaned.setdefault("entry", None)
        cleaned.setdefault("sl", None)
        cleaned.setdefault("t1", None)
        cleaned.setdefault("t2", None)
        cleaned.setdefault("confidence", None)
        cleaned.setdefault("expires_at", None)
        cleaned.setdefault("created_at", now)
        cleaned.setdefault("updated_at", now)
        cleaned.setdefault("status", "ACTIVE")
        cleaned.setdefault("t1_sent", False)
        cleaned.setdefault("t2_sent", False)
        cleaned.setdefault("expiry_sent", False)
        cleaned.setdefault("invalidation_sent", False)
        cleaned.setdefault("trailing_sent", False)
        cleaned.setdefault("symbol_token", None)
        cleaned.setdefault("last_price", None)
        cleaned.setdefault("last_tick_time", None)
        cleaned.setdefault("signal_hash", None)
        cleaned.setdefault("source_plan", {})

        return cleaned

    def _clean_menu_state_dict(self, item: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            "menu_id",
            "created_at",
            "menu_hash",
            "candidates",
            "candidate_lookup",
            "recommended_symbols",
            "trade_gate",
            "market_regime",
            "macro_bias",
            "warning_tag",
            "source_cycle_id",
        }

        cleaned = {k: v for k, v in (item or {}).items() if k in allowed}

        cleaned.setdefault("menu_id", "")
        cleaned.setdefault("created_at", now_ts())
        cleaned.setdefault("menu_hash", "")
        cleaned.setdefault("candidates", [])
        cleaned.setdefault("candidate_lookup", {})
        cleaned.setdefault("recommended_symbols", [])
        cleaned.setdefault("trade_gate", "")
        cleaned.setdefault("market_regime", "")
        cleaned.setdefault("macro_bias", "")
        cleaned.setdefault("warning_tag", "")
        cleaned.setdefault("source_cycle_id", None)

        return cleaned

    # -------------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------------

    def _load_persisted_json_file(self, path: str, *, label: str, default: Any) -> Any:
        if not os.path.exists(path):
            return default
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self.logger.warning(
                "Persisted %s JSON is corrupted at %s: %s. Continuing with clean empty state.",
                label,
                path,
                e,
            )
            return default
        except Exception as e:
            self.logger.warning("Persisted %s load failed at %s: %s", label, path, e)
            return default

    def _reset_offmarket_menu_state(self, clear_persisted: bool = True) -> None:
        with self._lock:
            self._menus = {}
            self._latest_menu_id = None
            self._last_menu_hash = None
            self._last_menu_time = None
            self._dedup_cache = {}
            self.health.last_menu_id = None

        if isinstance(self.shared_state, dict):
            self.shared_state["last_menu"] = None
            self.shared_state["runtime_contract_map"] = {}
            self.shared_state["runtime_latest_menu_id"] = None

        if clear_persisted:
            self._persist_menus()

    def _load_persisted_state(self, load_menus: bool = True) -> None:
        try:
            if load_menus:
                raw = self._load_persisted_json_file(
                    self._persist_file_menus,
                    label="menus",
                    default=None,
                )
                if isinstance(raw, dict):
                    self._latest_menu_id = raw.get("latest_menu_id")
                    menus = raw.get("menus", {}) or {}

                    for mid, item in menus.items():
                        cleaned_menu = self._clean_menu_state_dict(item)
                        raw_cands = cleaned_menu.get("candidates", []) or []

                        cands = []
                        for c in raw_cands:
                            try:
                                cands.append(MenuCandidate(**c))
                            except Exception:
                                continue

                        self._menus[mid] = MenuState(
                            menu_id=cleaned_menu["menu_id"],
                            created_at=cleaned_menu["created_at"],
                            menu_hash=cleaned_menu["menu_hash"],
                            candidates=cands,
                            candidate_lookup=cleaned_menu.get("candidate_lookup", {}),
                            recommended_symbols=cleaned_menu.get("recommended_symbols", []),
                            trade_gate=cleaned_menu.get("trade_gate", ""),
                            market_regime=cleaned_menu.get("market_regime", ""),
                            macro_bias=cleaned_menu.get("macro_bias", ""),
                            warning_tag=cleaned_menu.get("warning_tag", ""),
                            source_cycle_id=cleaned_menu.get("source_cycle_id"),
                        )

            raw = self._load_persisted_json_file(
                self._persist_file_signals,
                label="active_signals",
                default=None,
            )
            if isinstance(raw, dict):
                for sym, item in (raw or {}).items():
                    try:
                        cleaned_signal = self._clean_signal_state_dict(item)
                        self._active_signals[sym] = SignalState(**cleaned_signal)
                    except Exception as inner_e:
                        self.logger.warning("Skipping bad persisted signal %s: %s", sym, inner_e)

        except Exception as e:
            self.logger.warning("State load failed: %s", e)

    def _persist_state(self) -> None:
        self._persist_menus()
        self._persist_signals()
        self._persist_health()
        self._persist_message_registry()

    def _persist_menus(self) -> None:
        try:
            data = {
                "latest_menu_id": self._latest_menu_id,
                "menus": {k: v.to_dict() for k, v in self._menus.items()},
            }
            with open(self._persist_file_menus, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning("Persist menus failed: %s", e)

    def _persist_signals(self) -> None:
        try:
            data = {k: v.to_dict() for k, v in self._active_signals.items()}
            with open(self._persist_file_signals, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning("Persist signals failed: %s", e)

    def _persist_health(self) -> None:
        try:
            with open(self._persist_file_health, "w", encoding="utf-8") as f:
                json.dump(self.health_dict(), f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning("Persist health failed: %s", e)

    def _persist_message_registry(self) -> None:
        try:
            path = os.path.join(self.config.deletion_registry_dir, f"messages_{today_ist_str()}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump([x.to_dict() for x in self._message_registry], f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning("Persist message registry failed: %s", e)

    def _journal_path(self, date_str: Optional[str] = None) -> str:
        date_str = date_str or today_ist_str()
        return os.path.join(self.config.journal_dir, f"alert_journal_{date_str}.jsonl")

    def _journal(self, payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload.setdefault("timestamp_ist", now_ist().strftime("%Y-%m-%d %H:%M:%S"))
        payload.setdefault("timestamp_ts", now_ts())
        payload.setdefault("module", "module_06_alert_engine")
        try:
            with open(self._journal_path(), "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            self.logger.warning("Journal write failed: %s", e)

    # -------------------------------------------------------------------------
    # Input Access / Shared State
    # -------------------------------------------------------------------------

    def _get_shared(self, key: str, default: Any = None) -> Any:
        try:
            if self.config.allow_shared_state_read and isinstance(self.shared_state, dict):
                return self.shared_state.get(key, default)
        except Exception:
            pass
        return default

    def get_snapshots(self) -> Dict[str, Any]:
        try:
            if callable(self.snapshot_getter):
                data = self.snapshot_getter()
                return data if isinstance(data, dict) else {}
        except Exception as e:
            self.logger.warning("snapshot_getter failed: %s", e)
        data = self._get_shared("snapshots", {})
        return data if isinstance(data, dict) else {}

    def get_market_context(self) -> Dict[str, Any]:
        try:
            if callable(self.market_context_getter):
                data = self.market_context_getter()
                return data if isinstance(data, dict) else {}
        except Exception as e:
            self.logger.warning("market_context_getter failed: %s", e)
        data = self._get_shared("market_context", {})
        return data if isinstance(data, dict) else {}

    def get_external_health(self) -> Dict[str, Any]:
        try:
            if callable(self.health_getter):
                data = self.health_getter()
                return data if isinstance(data, dict) else {}
        except Exception as e:
            self.logger.warning("health_getter failed: %s", e)
        data = self._get_shared("module_health", {})
        return data if isinstance(data, dict) else {}

    def _new_command_diagnostic(self, command_text: str) -> Dict[str, Any]:
        return {
            "timestamp": now_ts(),
            "raw_command": str(command_text or ""),
            "parsed_action": None,
            "source_chat_id": None,
            "requested_menu_id": None,
            "effective_menu_id": None,
            "requested_tokens": [],
            "resolved_items": [],
            "unresolved_tokens": [],
            "plan_events": [],
            "reply_events": [],
            "ok": False,
            "error": None,
            "stop_reason": None,
            "reply_target_chat_ids": [],
            "summary": "",
        }

    def _finalize_command_diagnostic(self, diagnostic: Optional[Dict[str, Any]]) -> None:
        if not isinstance(diagnostic, dict):
            return

        resolved_pairs = [
            f"{item.get('request')}->{item.get('symbol')}"
            for item in diagnostic.get("resolved_items", [])
            if isinstance(item, dict)
        ]
        plan_parts = []
        for event in diagnostic.get("plan_events", []):
            if not isinstance(event, dict):
                continue
            symbol = str(event.get("symbol") or "?")
            source = str(event.get("plan_source") or "none")
            on_demand = "yes" if event.get("on_demand_attempted") else "no"
            reply = str(event.get("reply_send") or "-")
            plan_parts.append(f"{symbol}:{source}:on_demand={on_demand}:reply={reply}")

        diagnostic["summary"] = (
            f"raw={diagnostic.get('raw_command')!r} "
            f"action={diagnostic.get('parsed_action') or '-'} "
            f"source_chat={diagnostic.get('source_chat_id') or '-'} "
            f"menu={diagnostic.get('effective_menu_id') or diagnostic.get('requested_menu_id') or '-'} "
            f"reply_chat={diagnostic.get('reply_target_chat_ids') or ['-']} "
            f"resolved={resolved_pairs or ['-']} "
            f"plans={plan_parts or ['-']} "
            f"stop={diagnostic.get('stop_reason') or '-'} "
            f"ok={bool(diagnostic.get('ok'))}"
        )

        if isinstance(self.shared_state, dict):
            items = self.shared_state.get("telegram_command_diagnostics", [])
            if not isinstance(items, list):
                items = []
            items.append(dict(diagnostic))
            self.shared_state["telegram_command_diagnostics"] = items[-20:]
            self.shared_state["last_telegram_command_diagnostic"] = dict(diagnostic)

        try:
            self.logger.info("telegram_command_diag | %s", diagnostic["summary"])
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Queue Helpers
    # -------------------------------------------------------------------------

    def _safe_qsize(self) -> int:
        try:
            return self._send_queue.qsize()
        except Exception:
            return 0

    def _enqueue(self, item: QueueItem) -> bool:
        with self._lock:
            self._prune_dedup_cache()

            if item.dedup_key:
                last_seen = self._dedup_cache.get(item.dedup_key)
                if last_seen and (now_ts() - last_seen) < self.config.duplicate_hash_ttl_sec:
                    return False

            if not item.bypass_cooldown:
                symbol = item.metadata.get("symbol")
                if symbol:
                    last_ts = self._last_symbol_alert_ts.get(symbol)
                    if last_ts and (now_ts() - last_ts) < self.config.symbol_alert_cooldown_sec:
                        if item.kind not in (self.ALERT_CRITICAL_KIND(), self.ALERT_FOLLOWUP):
                            return False

            if self._safe_qsize() >= self.config.send_queue_maxsize:
                if item.priority >= self.PRIORITY_HEARTBEAT:
                    self.health.messages_dropped += 1
                    self._journal({
                        "alert_type": item.kind,
                        "send_status": "DROPPED_QUEUE_FULL",
                        "priority": item.priority,
                        "symbol": item.metadata.get("symbol"),
                        "menu_id": item.metadata.get("menu_id"),
                        "error_details": "queue_full_low_priority_drop",
                    })
                    return False

            try:
                self._send_queue.put_nowait(
                    (item.priority, item.created_at, next(self._queue_counter), item.kind, item)
                )
                if item.dedup_key:
                    self._dedup_cache[item.dedup_key] = now_ts()
                self.health.queue_size = self._safe_qsize()
                self.health.queue_highwater = max(self.health.queue_highwater, self.health.queue_size)
                return True
            except queue.Full:
                self.health.messages_dropped += 1
                return False

    def ALERT_CRITICAL_KIND(self) -> str:
        return self.ALERT_ERROR

    def _prune_dedup_cache(self) -> None:
        cutoff = now_ts() - self.config.duplicate_hash_ttl_sec
        stale = [k for k, v in self._dedup_cache.items() if v < cutoff]
        for k in stale:
            self._dedup_cache.pop(k, None)

    def _rate_limit_sleep(self, bypass: bool = False) -> None:
        if bypass:
            return
        with self._lock:
            cutoff = now_ts() - 60
            self._send_timestamps = [x for x in self._send_timestamps if x >= cutoff]
            if len(self._send_timestamps) < self.config.send_soft_cap_per_minute:
                return
            oldest = self._send_timestamps[0]
            wait_sec = max(0.0, 60 - (now_ts() - oldest) + 0.2)
        if wait_sec > 0:
            time.sleep(wait_sec)

    def _record_send_timestamp(self) -> None:
        with self._lock:
            self._send_timestamps.append(now_ts())
            cutoff = now_ts() - 60
            self._send_timestamps = [x for x in self._send_timestamps if x >= cutoff]

    # -------------------------------------------------------------------------
    # Public Integration APIs
    # -------------------------------------------------------------------------

    def publish_candidate_menu(
        self,
        candidates: List[Dict[str, Any]],
        trade_signal_list: Optional[List[Dict[str, Any]]] = None,
        market_context: Optional[Dict[str, Any]] = None,
        source_cycle_id: Optional[str] = None,
        force: bool = False,
    ) -> Optional[str]:
        market_context = market_context or self.get_market_context()
        trade_signal_list = trade_signal_list or []

        menu_state, message_text = self._build_menu_state_and_text(
            candidates=candidates,
            trade_signal_list=trade_signal_list,
            market_context=market_context,
            source_cycle_id=source_cycle_id,
        )

        mode_ctx = self._mode_context()
        should_send = True if mode_ctx.get("offmarket_test") else (force or self._should_send_menu(menu_state))
        self._store_menu(menu_state)

        if not should_send:
            return menu_state.menu_id

        item = QueueItem(
            priority=self.PRIORITY_MENU,
            created_at=now_ts(),
            kind=self.ALERT_MENU,
            text=message_text,
            metadata={
                "menu_id": menu_state.menu_id,
                "menu_hash": menu_state.menu_hash,
                "recommended_symbols": menu_state.recommended_symbols,
            },
            parse_mode=None,
            dedup_key=f"menu:{menu_state.menu_hash}",
        )
        self._enqueue(item)
        return menu_state.menu_id

    def send_trade_plan_reply(
        self,
        selected_letters_or_symbols: List[str],
        trade_plan_map: Dict[str, Dict[str, Any]],
        explain_map: Optional[Dict[str, Any]] = None,
        menu_id: Optional[str] = None,
        send_explain: bool = False,
        user_requested: bool = False,
        reply_chat_id: Optional[Any] = None,
        command_diagnostic: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        sent_for: List[str] = []
        explain_map = explain_map or {}
        menu = self._resolve_menu(menu_id)

        contract = self._get_runtime_contract(menu.menu_id if menu else menu_id)
        if not trade_plan_map and contract:
            trade_plan_map = contract.get("trade_plan_map") or {}
        if not explain_map and contract:
            explain_map = contract.get("explain_map") or {}

        if menu is None:
            err = "No valid menu found. Use latest menu or request a fresh menu."
            send_ok = self._enqueue(self._error_item(err, bypass_cooldown=True, reply_chat_id=reply_chat_id))
            if isinstance(command_diagnostic, dict):
                target_chat_id = reply_chat_id if reply_chat_id is not None else self.config.telegram_chat_id
                command_diagnostic["reply_events"].append(
                    {"kind": "error", "send_ok": bool(send_ok), "message": err, "target_chat_id": target_chat_id}
                )
                command_diagnostic["reply_target_chat_ids"] = list(dict.fromkeys(
                    list(command_diagnostic.get("reply_target_chat_ids") or []) + [str(target_chat_id)]
                ))
                command_diagnostic["stop_reason"] = "menu_not_found"
            return sent_for

        if isinstance(command_diagnostic, dict):
            command_diagnostic["effective_menu_id"] = menu.menu_id
            command_diagnostic["requested_tokens"] = [str(x).strip().upper() for x in (selected_letters_or_symbols or []) if str(x).strip()]

        resolved = self._resolve_selection_to_symbols(selected_letters_or_symbols, menu)
        if not resolved:
            send_ok = self._send_menu_unavailable_reply(selected_letters_or_symbols, menu, reply_chat_id=reply_chat_id)
            if isinstance(command_diagnostic, dict):
                target_chat_id = reply_chat_id if reply_chat_id is not None else self.config.telegram_chat_id
                command_diagnostic["reply_events"].append(
                    {"kind": "menu_unavailable", "send_ok": bool(send_ok), "target_chat_id": target_chat_id}
                )
                command_diagnostic["reply_target_chat_ids"] = list(dict.fromkeys(
                    list(command_diagnostic.get("reply_target_chat_ids") or []) + [str(target_chat_id)]
                ))
                command_diagnostic["stop_reason"] = "no_menu_match"
            return sent_for

        unresolved = []
        for token in selected_letters_or_symbols:
            norm = str(token).strip().upper()
            if not norm:
                continue
            match = self._resolve_selection_to_symbols([norm], menu)
            if not match:
                unresolved.append(norm)
            elif isinstance(command_diagnostic, dict):
                command_diagnostic["resolved_items"].append({"request": norm, "symbol": match[0].get("symbol")})
        if unresolved:
            send_ok = self._send_menu_unavailable_reply(unresolved, menu, reply_chat_id=reply_chat_id)
            if isinstance(command_diagnostic, dict):
                command_diagnostic["unresolved_tokens"] = list(unresolved)
                target_chat_id = reply_chat_id if reply_chat_id is not None else self.config.telegram_chat_id
                command_diagnostic["reply_events"].append(
                    {
                        "kind": "partial_menu_unavailable",
                        "send_ok": bool(send_ok),
                        "tokens": list(unresolved),
                        "target_chat_id": target_chat_id,
                    }
                )
                command_diagnostic["reply_target_chat_ids"] = list(dict.fromkeys(
                    list(command_diagnostic.get("reply_target_chat_ids") or []) + [str(target_chat_id)]
                ))

        for token in resolved:
            symbol = token["symbol"]
            plan, generated_explain_payload, missing_reason, plan_event = self._get_or_generate_plan(
                symbol=symbol,
                menu=menu,
                contract=contract,
                trade_plan_map=trade_plan_map,
                explain_map=explain_map,
            )
            if isinstance(command_diagnostic, dict):
                command_diagnostic["plan_events"].append(dict(plan_event))
            if not plan:
                send_ok = self._enqueue(
                    QueueItem(
                        priority=self.PRIORITY_PLAN,
                        created_at=now_ts(),
                        kind=self.ALERT_PLAN,
                        text=(
                            f"Plan unavailable for {symbol}\n"
                            f"Latest menu: {menu.menu_id}\n"
                            f"Current runtime data could not build a plan right now ({missing_reason or 'unknown'})."
                        ),
                        metadata={
                            "menu_id": menu.menu_id,
                            "symbol": symbol,
                            "status": "PLAN_UNAVAILABLE",
                            "source_chat_id": reply_chat_id,
                            "reply_chat_id": reply_chat_id,
                        },
                        dedup_key=f"plan-unavailable:{menu.menu_id}:{symbol}:{missing_reason or 'unknown'}",
                    )
                )
                if isinstance(command_diagnostic, dict):
                    target_chat_id = reply_chat_id if reply_chat_id is not None else self.config.telegram_chat_id
                    command_diagnostic["reply_events"].append(
                        {
                            "kind": "plan_unavailable",
                            "symbol": symbol,
                            "send_ok": bool(send_ok),
                            "target_chat_id": target_chat_id,
                        }
                    )
                    command_diagnostic["reply_target_chat_ids"] = list(dict.fromkeys(
                        list(command_diagnostic.get("reply_target_chat_ids") or []) + [str(target_chat_id)]
                    ))
                continue

            plan_text, signal_state = self._build_trade_plan_text_and_state(symbol, plan, menu.menu_id)
            plan_hash = compact_json_hash({"symbol": symbol, "plan": self._plan_material_subset(plan), "menu_id": menu.menu_id})

            if not user_requested and not self._plan_should_send(symbol, plan_hash):
                if isinstance(command_diagnostic, dict):
                    command_diagnostic["reply_events"].append({"kind": "plan_suppressed", "symbol": symbol, "send_ok": False})
                continue

            self._last_plan_hash_by_symbol[symbol] = plan_hash

            send_ok = self._enqueue(
                QueueItem(
                    priority=self.PRIORITY_PLAN,
                    created_at=now_ts(),
                    kind=self.ALERT_PLAN,
                    text=plan_text,
                    metadata={
                        "menu_id": menu.menu_id,
                        "symbol": symbol,
                        "direction": signal_state.direction,
                        "setup_name": signal_state.setup_name,
                        "signal_hash": signal_state.signal_hash,
                        "source_chat_id": reply_chat_id,
                        "reply_chat_id": reply_chat_id,
                    },
                    parse_mode=None,
                    bypass_cooldown=user_requested,
                    dedup_key=None if user_requested else f"plan:{symbol}:{plan_hash}",
                )
            )
            if isinstance(command_diagnostic, dict):
                target_chat_id = reply_chat_id if reply_chat_id is not None else self.config.telegram_chat_id
                command_diagnostic["reply_events"].append(
                    {
                        "kind": "plan_reply",
                        "symbol": symbol,
                        "send_ok": bool(send_ok),
                        "target_chat_id": target_chat_id,
                    }
                )
                command_diagnostic["reply_target_chat_ids"] = list(dict.fromkeys(
                    list(command_diagnostic.get("reply_target_chat_ids") or []) + [str(target_chat_id)]
                ))
                if command_diagnostic["plan_events"]:
                    command_diagnostic["plan_events"][-1]["reply_send"] = "enqueued" if send_ok else "enqueue_failed"

            mode_ctx = self._mode_context()
            if mode_ctx["live_market"] and not user_requested:
                self._register_signal(signal_state)

            if send_explain:
                explain_payload = (
                    explain_map.get(symbol)
                    or explain_map.get(symbol.upper())
                    or generated_explain_payload
                )
                if explain_payload:
                    self.send_explain(symbol, explain_payload, menu.menu_id)

            sent_for.append(symbol)

        return sent_for

    def send_explain(self, symbol: str, explain_payload: Any, menu_id: Optional[str] = None) -> bool:
        text = self._build_explain_text(symbol, explain_payload, menu_id=menu_id)
        explain_hash = compact_json_hash({"symbol": symbol, "explain": explain_payload, "menu_id": menu_id})

        with self._lock:
            last_hash = self._last_explain_hash_by_symbol.get(symbol)
            last_ts = self._last_symbol_alert_ts.get(f"EXPLAIN:{symbol}")
            if last_hash == explain_hash and last_ts and (now_ts() - last_ts) < self.config.explain_cooldown_sec:
                return False
            self._last_explain_hash_by_symbol[symbol] = explain_hash

        return self._enqueue(
            QueueItem(
                priority=self.PRIORITY_EXPLAIN,
                created_at=now_ts(),
                kind=self.ALERT_EXPLAIN,
                text=text,
                metadata={"symbol": symbol, "menu_id": menu_id},
                dedup_key=f"explain:{symbol}:{explain_hash}",
            )
        )

    def send_status(self) -> bool:
        text = self._build_heartbeat_text()
        hb_hash = compact_json_hash(text)
        if self.config.heartbeat_dedup_enabled:
            with self._lock:
                if (
                    self._last_heartbeat_hash == hb_hash
                    and self._last_heartbeat_time
                    and (now_ts() - self._last_heartbeat_time) < self.config.heartbeat_cooldown_sec
                ):
                    return False
        return self._enqueue(
            QueueItem(
                priority=self.PRIORITY_HEARTBEAT,
                created_at=now_ts(),
                kind=self.ALERT_HEARTBEAT,
                text=text,
                metadata={},
                dedup_key=f"heartbeat:{hb_hash}",
            )
        )

    def critical_alert(self, message: str, extra: Optional[Dict[str, Any]] = None) -> bool:
        extra = extra or {}
        return self._enqueue(
            QueueItem(
                priority=self.PRIORITY_CRITICAL,
                created_at=now_ts(),
                kind=self.ALERT_ERROR,
                text=f"🚨 CRITICAL ALERT\n\n{message}",
                metadata=extra,
                bypass_cooldown=True,
                bypass_rate_limit=True,
                dedup_key=f"critical:{compact_json_hash({'m': message, 'x': extra})}",
            )
        )

    # -------------------------------------------------------------------------
    # Command Parsing
    # -------------------------------------------------------------------------

    def parse_and_handle_command(
        self,
        command_text: str,
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]] = None,
        explain_map: Optional[Dict[str, Any]] = None,
        source_chat_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        result = {
            "ok": False,
            "action": None,
            "symbols": [],
            "menu_id": None,
            "error": None,
        }
        command_diagnostic = self._new_command_diagnostic(command_text)
        command_diagnostic["source_chat_id"] = str(source_chat_id) if source_chat_id is not None else None

        try:
            parsed = self._parse_command(command_text)
            result["action"] = parsed["action"]
            result["menu_id"] = parsed.get("menu_id")
            command_diagnostic["parsed_action"] = parsed["action"]
            command_diagnostic["requested_menu_id"] = parsed.get("menu_id")
            command_diagnostic["requested_tokens"] = list(parsed.get("tokens") or [])

            if parsed["action"] == "STATUS":
                send_ok = self.send_status()
                result["ok"] = True
                command_diagnostic["ok"] = bool(send_ok)
                command_diagnostic["reply_events"].append({"kind": "status", "send_ok": bool(send_ok)})
                command_diagnostic["stop_reason"] = "status_sent"
                return result

            contract = self._get_runtime_contract(parsed.get("menu_id"))
            if contract is None:
                result["error"] = "No valid runtime contract found for requested menu."
                command_diagnostic["error"] = result["error"]
                command_diagnostic["stop_reason"] = "no_runtime_contract"
                return result

            effective_menu_id = contract.get("menu_id") or parsed.get("menu_id")
            result["menu_id"] = effective_menu_id
            command_diagnostic["effective_menu_id"] = effective_menu_id

            effective_trade_plan_map = trade_plan_map or contract.get("trade_plan_map") or {}
            effective_explain_map = explain_map or contract.get("explain_map") or {}

            if parsed["action"] == "PLAN":
                syms = self.send_trade_plan_reply(
                    selected_letters_or_symbols=parsed["tokens"],
                    trade_plan_map=effective_trade_plan_map,
                    explain_map=effective_explain_map,
                    menu_id=effective_menu_id,
                    send_explain=False,
                    user_requested=True,
                    reply_chat_id=source_chat_id,
                    command_diagnostic=command_diagnostic,
                )
                result["symbols"] = syms
                result["ok"] = len(syms) > 0
                command_diagnostic["ok"] = result["ok"]
                if not syms:
                    result["error"] = "No valid symbols resolved for PLAN command."
                    command_diagnostic["error"] = result["error"]
                    command_diagnostic["stop_reason"] = command_diagnostic.get("stop_reason") or "plan_not_sent"
                else:
                    command_diagnostic["stop_reason"] = "plan_sent"
                return result

            if parsed["action"] == "EXPLAIN":
                menu = self._resolve_menu(effective_menu_id, require_exact=bool(parsed.get("menu_id")))
                if not menu:
                    result["error"] = "No valid menu found."
                    command_diagnostic["error"] = result["error"]
                    command_diagnostic["stop_reason"] = "menu_not_found"
                    return result

                if not effective_explain_map:
                    result["error"] = "Explain payload contract is empty for this menu."
                    command_diagnostic["error"] = result["error"]
                    command_diagnostic["stop_reason"] = "empty_explain_map"
                    return result

                resolved = self._resolve_selection_to_symbols(parsed["tokens"], menu)
                for item in resolved:
                    sym = item["symbol"]
                    command_diagnostic["resolved_items"].append({"request": sym, "symbol": sym})
                    payload = effective_explain_map.get(sym) or effective_explain_map.get(sym.upper())
                    if payload:
                        send_ok = self.send_explain(sym, payload, menu_id=menu.menu_id)
                        command_diagnostic["reply_events"].append({"kind": "explain_reply", "symbol": sym, "send_ok": bool(send_ok)})
                        result["symbols"].append(sym)

                result["ok"] = len(result["symbols"]) > 0
                command_diagnostic["ok"] = result["ok"]
                if not result["ok"]:
                    result["error"] = "No explain payload found."
                    command_diagnostic["error"] = result["error"]
                    command_diagnostic["stop_reason"] = "explain_not_found"
                else:
                    command_diagnostic["stop_reason"] = "explain_sent"
                return result

            result["error"] = "Unsupported command."
            command_diagnostic["error"] = result["error"]
            command_diagnostic["stop_reason"] = "unsupported_command"
            return result

        except Exception as e:
            result["error"] = str(e)
            command_diagnostic["error"] = result["error"]
            command_diagnostic["stop_reason"] = "exception"
            self.logger.exception("parse_and_handle_command failed")
            return result
        finally:
            self._finalize_command_diagnostic(command_diagnostic)

    def _parse_command(self, text: str) -> Dict[str, Any]:
        raw = (text or "").strip().upper()
        raw = re.sub(r"\s+", " ", raw)

        if raw == "STATUS":
            return {"action": "STATUS", "tokens": []}

        menu_match = re.match(r"^(M[\d_]+)\s+(.+)$", raw)
        if menu_match:
            menu_id = menu_match.group(1)
            rest = menu_match.group(2).strip()
            tokens = re.split(r"[,\s]+", rest)
            return {"action": "PLAN", "menu_id": menu_id, "tokens": [t for t in tokens if t]}

        explain_match = re.match(r"^EXPLAIN\s+(.+)$", raw)
        if explain_match:
            tokens = re.split(r"[,\s]+", explain_match.group(1).strip())
            return {"action": "EXPLAIN", "menu_id": None, "tokens": [t for t in tokens if t]}

        plan_match = re.match(r"^PLAN\s+(.+)$", raw)
        if plan_match:
            tokens = re.split(r"[,\s]+", plan_match.group(1).strip())
            return {"action": "PLAN", "menu_id": None, "tokens": [t for t in tokens if t]}

        tokens = re.split(r"[,\s]+", raw)
        if all(re.fullmatch(r"[A-T]", t) for t in tokens if t):
            return {"action": "PLAN", "menu_id": None, "tokens": [t for t in tokens if t]}

        raise ValueError("Invalid command. Use: M### A C, PLAN A, EXPLAIN A, STATUS")

    def fetch_telegram_updates(self, timeout: int = 10) -> List[Dict[str, Any]]:
        if not self.telegram:
            return []

        offset = None
        if self._last_update_id is not None:
            offset = self._last_update_id + 1

        try:
            return self.telegram.get_updates(
                offset=offset,
                timeout=timeout,
                allowed_updates=["message", "edited_message"],
            )
        except Exception:
            self.logger.exception("fetch_telegram_updates failed")
            return []

    def process_telegram_updates(
        self,
        updates: Optional[List[Dict[str, Any]]],
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]] = None,
        explain_map: Optional[Dict[str, Any]] = None,
    ) -> int:
        updates = updates or []
        handled = 0
        max_update_id = self._last_update_id

        for upd in updates:
            try:
                update_id = upd.get("update_id")
                if isinstance(update_id, int):
                    if max_update_id is None or update_id > max_update_id:
                        max_update_id = update_id

                msg = upd.get("message") or upd.get("edited_message") or {}
                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                result = self.parse_and_handle_command(
                    command_text=text,
                    trade_plan_map=trade_plan_map,
                    explain_map=explain_map,
                    source_chat_id=(msg.get("chat") or {}).get("id"),
                )
                if result.get("ok"):
                    handled += 1

            except Exception:
                self.logger.exception("process_telegram_updates item failed")

        if max_update_id is not None:
            self._last_update_id = max_update_id

        return handled

    def poll_telegram_once(
        self,
        timeout: int = 2,
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]] = None,
        explain_map: Optional[Dict[str, Any]] = None,
    ) -> int:
        updates = self.fetch_telegram_updates(timeout=timeout)
        return self.process_telegram_updates(
            updates=updates,
            trade_plan_map=trade_plan_map,
            explain_map=explain_map,
        )

    def set_runtime_context(
        self,
        module04_output: Optional[Dict[str, Any]] = None,
        module05_output: Optional[Dict[str, Any]] = None,
        module03_output: Optional[Dict[str, Any]] = None,
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]] = None,
        explain_map: Optional[Dict[str, Any]] = None,
        menu_id: Optional[str] = None,
    ) -> None:
        if module04_output is not None:
            self._set_shared("module04_output", module04_output)
        if module05_output is not None:
            self._set_shared("module05_output", module05_output)
        if module03_output is not None:
            self._set_shared("module03_output", module03_output)

        if trade_plan_map is None:
            trade_plan_map = self.extract_trade_plan_map(module05_output)

        if explain_map is None:
            explain_map = self.extract_explain_map(module05_output)

        if trade_plan_map is not None:
            self._set_shared("trade_plan_map", trade_plan_map)
        if explain_map is not None:
            self._set_shared("explain_map", explain_map)

        if menu_id:
            self._bind_runtime_contract(
                menu_id=menu_id,
                trade_plan_map=trade_plan_map,
                explain_map=explain_map,
                module04_output=module04_output,
                module05_output=module05_output,
                module03_output=module03_output,
            )

    # -------------------------------------------------------------------------
    # Runtime Contract Helpers
    # -------------------------------------------------------------------------

    def _set_shared(self, key: str, value: Any) -> None:
        if isinstance(self.shared_state, dict):
            self.shared_state[key] = value

    def _runtime_contract_key(self, menu_id: Optional[str]) -> str:
        return str(menu_id or "").strip()

    def _build_runtime_contract(
        self,
        menu_id: Optional[str],
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]],
        explain_map: Optional[Dict[str, Any]],
        module04_output: Optional[Dict[str, Any]] = None,
        module05_output: Optional[Dict[str, Any]] = None,
        module03_output: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "menu_id": self._runtime_contract_key(menu_id),
            "trade_plan_map": trade_plan_map or {},
            "explain_map": explain_map or {},
            "module04_output": module04_output or {},
            "module05_output": module05_output or {},
            "module03_output": module03_output or {},
            "updated_at": now_ts(),
        }

    def extract_trade_plan_map(self, module05_output: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        module05_output = module05_output or {}
        raw = module05_output.get("trade_plan_map") or {}
        return raw if isinstance(raw, dict) else {}

    def extract_explain_map(self, module05_output: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        module05_output = module05_output or {}
        raw = module05_output.get("explain_map") or {}
        return raw if isinstance(raw, dict) else {}

    def _get_runtime_contract(self, menu_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        contract_map = self._get_shared("runtime_contract_map", {})
        if not isinstance(contract_map, dict):
            return None

        wanted = self._runtime_contract_key(menu_id)
        if wanted and wanted in contract_map:
            return contract_map[wanted]

        latest_menu_id = self._get_shared("runtime_latest_menu_id", None)
        latest_key = self._runtime_contract_key(latest_menu_id)
        if latest_key and latest_key in contract_map:
            return contract_map[latest_key]

        return None

    def _bind_runtime_contract(
        self,
        menu_id: Optional[str],
        trade_plan_map: Optional[Dict[str, Dict[str, Any]]],
        explain_map: Optional[Dict[str, Any]],
        module04_output: Optional[Dict[str, Any]] = None,
        module05_output: Optional[Dict[str, Any]] = None,
        module03_output: Optional[Dict[str, Any]] = None,
    ) -> None:
        key = self._runtime_contract_key(menu_id)
        if not key:
            return

        contract = self._build_runtime_contract(
            menu_id=menu_id,
            trade_plan_map=trade_plan_map,
            explain_map=explain_map,
            module04_output=module04_output,
            module05_output=module05_output,
            module03_output=module03_output,
        )

        contract_map = self._get_shared("runtime_contract_map", {})
        if not isinstance(contract_map, dict):
            contract_map = {}

        contract_map[key] = contract
        self._set_shared("runtime_contract_map", contract_map)
        self._set_shared("runtime_latest_menu_id", key)


    def run_signal_lifecycle_check(self) -> None:
        try:
            if hasattr(self, "_check_followups_once"):
                self._check_followups_once()
        except Exception:
            self.logger.exception("run_signal_lifecycle_check followups failed")

        try:
            if hasattr(self, "_check_signal_expiry_once"):
                self._check_signal_expiry_once()
        except Exception:
            self.logger.exception("run_signal_lifecycle_check expiry failed")

    # -------------------------------------------------------------------------
    # Mode Control (COMMON / LIVE / OFFMARKET)
    # -------------------------------------------------------------------------

    def _is_live_market(self) -> bool:
        dt = now_ist()
        if dt.weekday() >= 5:
            return False
        hhmm = dt.hour * 100 + dt.minute
        return 915 <= hhmm <= 1530

    def _mode_context(self) -> Dict[str, Any]:
        live_market = self._is_live_market()
        return {
            "live_market": live_market,
            "offmarket_test": not live_market,
            "mode_name": "LIVE" if live_market else "OFFMARKET_TEST",
        }

    def _publish_policy(self, mode_ctx: Dict[str, Any], force_menu: bool = False) -> Dict[str, Any]:
        if mode_ctx["live_market"]:
            return {
                "send_menu": True,
                "send_plans": True,
                "track_followups": True,
                "force_menu": force_menu,
            }

        return {
            "send_menu": True,
            "send_plans": True,
            "track_followups": False,
            "force_menu": True if force_menu else True,
        }

    # -------------------------------------------------------------------------
    # Menu Building / Registry
    # -------------------------------------------------------------------------

    def _build_menu_state_and_text(
        self,
        candidates: List[Dict[str, Any]],
        trade_signal_list: List[Dict[str, Any]],
        market_context: Dict[str, Any],
        source_cycle_id: Optional[str] = None,
    ) -> Tuple[MenuState, str]:
        cands = candidates[: self.config.max_menu_candidates]
        recommended_symbols = []
        signal_map = {}
        for sig in trade_signal_list[: self.config.max_recommended_signals_per_cycle]:
            sym = str(sig.get("symbol") or sig.get("ticker") or "").strip().upper()
            if sym:
                recommended_symbols.append(sym)
                signal_map[sym] = sig

        menu_candidates: List[MenuCandidate] = []
        candidate_lookup: Dict[str, Dict[str, Any]] = {}

        letters = [chr(ord("A") + i) for i in range(min(len(cands), 20))]
        for idx, cand in enumerate(cands):
            symbol = str(cand.get("symbol") or cand.get("ticker") or cand.get("tradingsymbol") or "").strip().upper()
            if not symbol:
                continue
            raw_score = safe_float(
                cand.get("rank_score")
                or cand.get("score")
                or cand.get("composite_score")
                or cand.get("composite")
                or cand.get("scanner_score")
                or ((cand.get("component_scores") or {}).get("composite_score")),
                0.0,
            )
            score = raw_score * 100.0 if 0 < raw_score <= 1.0 else raw_score

            snap = cand.get("snapshot") or {}
            candles_5m = cand.get("candles_5m") or ((cand.get("meta") or {}).get("candles_5m")) or []
            last_candle = candles_5m[-1] if candles_5m else {}

            ltp = safe_float(
                (snap.get("ltp") if safe_float(snap.get("ltp"), 0.0) > 0 else None)
                or cand.get("ltp")
                or cand.get("last_price")
                or cand.get("price")
                or last_candle.get("close"),
                None,
            )
            item = MenuCandidate(
                letter=letters[len(menu_candidates)],
                symbol=symbol,
                tier=str(cand.get("tier") or ""),
                score=round(score, 2),
                ltp=round(ltp, 2) if ltp is not None else None,
                sector=str(cand.get("sector") or ""),
                regime_tag=str(
                    cand.get("regime_tag")
                    or cand.get("regime_tag_used")
                    or cand.get("regime")
                    or cand.get("market_regime")
                    or cand.get("macro_regime")
                    or ""
                ),
                recommended=symbol in recommended_symbols,
                direction=str((signal_map.get(symbol) or {}).get("direction") or ""),
                setup_name=str((signal_map.get(symbol) or {}).get("setup_name") or ""),
            )

            menu_candidates.append(item)
            candidate_lookup[item.letter] = {
                "symbol": symbol,
                "candidate": cand,
                "recommended": item.recommended,
            }
            candidate_lookup[symbol] = {
                "symbol": symbol,
                "candidate": cand,
                "recommended": item.recommended,
            }

        mc_state = market_context.get("market_context_state") if isinstance(market_context, dict) else {}

        trade_gate = str(
            (mc_state or {}).get("trade_gate")
            or market_context.get("trade_gate")
            or "UNKNOWN"
        )
        market_regime = str(
            (mc_state or {}).get("market_regime")
            or (mc_state or {}).get("macro_regime")
            or market_context.get("market_regime")
            or market_context.get("macro_regime")
            or ""
        )
        macro_bias = str(
            (mc_state or {}).get("macro_bias")
            or market_context.get("macro_bias")
            or ""
        )
        warning_tag = "⚠️ CAUTION" if trade_gate == "CAUTION" else ("⛔ PAUSE" if trade_gate == "PAUSE" else "✅ ALLOW")

        serial = self._next_menu_serial()
        menu_id = serial
        menu_payload = {
            "menu_id": menu_id,
            "symbols": [c.symbol for c in menu_candidates],
            "scores": [c.score for c in menu_candidates],
            "recommended": recommended_symbols,
            "trade_gate": trade_gate,
            "market_regime": market_regime,
            "macro_bias": macro_bias,
        }
        menu_hash = compact_json_hash(menu_payload)

        menu_state = MenuState(
            menu_id=menu_id,
            created_at=now_ts(),
            menu_hash=menu_hash,
            candidates=menu_candidates,
            candidate_lookup=candidate_lookup,
            recommended_symbols=recommended_symbols,
            trade_gate=trade_gate,
            market_regime=market_regime,
            macro_bias=macro_bias,
            warning_tag=warning_tag,
            source_cycle_id=source_cycle_id,
        )

        text = self._format_menu_text(menu_state)
        return menu_state, text

    def _format_menu_text(self, menu: MenuState) -> str:
        lines: List[str] = []
        lines.append("📋 CANDIDATE MENU")
        lines.append(f"Menu ID: {menu.menu_id}")
        lines.append(f"Gate: {menu.trade_gate} | Regime: {menu.market_regime or '-'} | Bias: {menu.macro_bias or '-'}")
        lines.append(f"Status: {menu.warning_tag}")
        lines.append("")

        for c in menu.candidates:
            tags = []
            if c.tier:
                tags.append(c.tier)
            if c.sector:
                tags.append(c.sector)
            if c.regime_tag:
                tags.append(c.regime_tag)
            if c.recommended:
                tags.append("⭐ REC")
            extra = " | ".join(tags) if tags else "-"
            score_txt = f"{c.score:.2f}" if c.score is not None else "-"
            ltp_txt = f"₹{c.ltp:.2f}" if c.ltp is not None else "₹-"
            lines.append(f"{c.letter}) {c.symbol} | {ltp_txt} | Score {score_txt} | {extra}")

        lines.append("")
        if menu.recommended_symbols:
            lines.append("Recommended picks: " + ", ".join(menu.recommended_symbols[: self.config.max_recommended_signals_per_cycle]))
        lines.append('Reply examples: "A C", "PLAN A", "EXPLAIN A", "STATUS"')
        return "\n".join(lines)

    def _next_menu_serial(self) -> str:
        with self._lock:
            self._menu_counter += 1
            dt = now_ist()
            day_serial = dt.timetuple().tm_yday
            return f"M{day_serial:03d}_{dt.strftime('%H%M%S')}_{int(self._menu_serial_seed):06d}_{self._menu_counter:03d}"

    def _should_send_menu(self, menu_state: MenuState) -> bool:
        with self._lock:
            if self.config.send_menu_on_every_cycle:
                return True

            if self._last_menu_time is None or self._last_menu_hash is None:
                return True

            if (now_ts() - self._last_menu_time) >= self.config.menu_cooldown_sec:
                return True

            prev_menu = self._menus.get(self._latest_menu_id or "")
            if not prev_menu:
                return True

            prev_syms = [x.symbol for x in prev_menu.candidates]
            new_syms = [x.symbol for x in menu_state.candidates]
            if not prev_syms:
                return True

            changed = len(set(prev_syms).symmetric_difference(set(new_syms)))
            frac = changed / max(1, len(set(prev_syms).union(set(new_syms))))
            if frac >= self.config.menu_change_threshold_fraction:
                return True

            if prev_menu.trade_gate != menu_state.trade_gate or prev_menu.market_regime != menu_state.market_regime:
                return True

            if prev_menu.recommended_symbols != menu_state.recommended_symbols:
                return True

            return False

    def _store_menu(self, menu_state: MenuState) -> None:
        with self._lock:
            self._menus[menu_state.menu_id] = menu_state
            self._latest_menu_id = menu_state.menu_id
            self._last_menu_hash = menu_state.menu_hash
            self._last_menu_time = menu_state.created_at
            self.health.last_menu_id = menu_state.menu_id

            # Bound menu registry
            keys = sorted(self._menus.keys(), key=lambda k: self._menus[k].created_at)
            while len(keys) > self.config.max_menu_registry:
                old = keys.pop(0)
                self._menus.pop(old, None)

        self._persist_menus()
        if isinstance(self.shared_state, dict):
            self.shared_state["last_menu"] = menu_state.to_dict()

    def _resolve_menu(self, menu_id: Optional[str], require_exact: bool = False) -> Optional[MenuState]:
        with self._lock:
            if menu_id:
                if menu_id in self._menus:
                    return self._menus[menu_id]
                return None if require_exact else None

            if self._latest_menu_id and self._latest_menu_id in self._menus:
                return self._menus[self._latest_menu_id]
        return None

    def _resolve_selection_to_symbols(self, tokens: List[str], menu: MenuState) -> List[Dict[str, Any]]:
        out = []
        seen = set()

        for token in tokens:
            t = str(token).strip().upper()
            if not t:
                continue

            item = None

            # direct letter lookup
            if re.fullmatch(r"[A-T]", t):
                item = menu.candidate_lookup.get(t)

            # direct symbol lookup
            if item is None:
                item = menu.candidate_lookup.get(t)

            if item:
                sym = str(item.get("symbol", "")).strip().upper()
                if sym and sym not in seen:
                    seen.add(sym)
                    out.append(item)

        return out

    def _extract_named_plan_map(self, module05_output: Optional[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
        out = {}
        raw = (module05_output or {}).get(key) or {}
        if isinstance(raw, dict):
            for sym, plan in raw.items():
                norm = str(sym or "").strip().upper()
                if norm and isinstance(plan, dict):
                    out[norm] = plan
        return out

    def _extract_named_explain_map(self, module05_output: Optional[Dict[str, Any]], key: str) -> Dict[str, Any]:
        out = {}
        raw = (module05_output or {}).get(key) or {}
        if isinstance(raw, dict):
            for sym, payload in raw.items():
                norm = str(sym or "").strip().upper()
                if norm:
                    out[norm] = payload
        return out

    def _send_menu_unavailable_reply(
        self,
        requested_tokens: List[str],
        menu: Optional[MenuState],
        reply_chat_id: Optional[Any] = None,
    ) -> bool:
        cleaned = [str(token).strip().upper() for token in (requested_tokens or []) if str(token).strip()]
        requested = ", ".join(cleaned) if cleaned else "REQUEST"
        menu_id = menu.menu_id if menu else (self._latest_menu_id or "-")
        text = (
            f"Plan unavailable for: {requested}\n"
            f"Latest menu: {menu_id}\n"
            "That stock is not available in the current candidate menu. "
            "Use a menu letter like PLAN A or a stock symbol from the latest menu."
        )
        return self._enqueue(
            QueueItem(
                priority=self.PRIORITY_PLAN,
                created_at=now_ts(),
                kind=self.ALERT_PLAN,
                text=text,
                metadata={
                    "menu_id": menu_id,
                    "requested": cleaned,
                    "status": "MENU_UNAVAILABLE",
                    "source_chat_id": reply_chat_id,
                    "reply_chat_id": reply_chat_id,
                },
                dedup_key=f"menu-unavailable:{menu_id}:{compact_json_hash(cleaned)}",
            )
        )

    def _build_on_demand_candle_store(
        self,
        symbol: str,
        candidate: Dict[str, Any],
        direct_key: str,
        meta_key: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        candles = candidate.get(direct_key)
        if not isinstance(candles, list):
            candles = ((candidate.get("meta") or {}).get(meta_key)) or []
        return {symbol: candles if isinstance(candles, list) else []}

    def _prepare_on_demand_runtime_inputs(
        self,
        symbol: str,
        candidate: Dict[str, Any],
        contract: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        exchange = str(candidate.get("exchange") or "NSE").strip() or "NSE"
        snapshot_key = f"{exchange}:{symbol}"
        snapshot_store = dict(self.get_snapshots() or {})
        candidate_snapshot = dict(candidate.get("snapshot") or {})
        merged_snapshot = dict(snapshot_store.get(snapshot_key) or snapshot_store.get(symbol) or {})
        merged_snapshot.update(candidate_snapshot)
        merged_snapshot.setdefault("symbol", symbol)
        merged_snapshot.setdefault("exchange", exchange)
        merged_snapshot.setdefault("snapshot_key", snapshot_key)
        if merged_snapshot:
            snapshot_store[snapshot_key] = merged_snapshot
            snapshot_store[symbol] = merged_snapshot

        return {
            "market_context": (contract or {}).get("module03_output") or self.get_market_context() or {},
            "snapshot_store": snapshot_store,
            "candle_store_5m": self._build_on_demand_candle_store(symbol, candidate, "candles_5m", "candles_5m"),
            "candle_store_1m": self._build_on_demand_candle_store(symbol, candidate, "candles_1m", "candles_1m"),
            "candle_store_15m": self._build_on_demand_candle_store(symbol, candidate, "candles_15m", "candles_15m"),
        }

    def _get_on_demand_strategy_engine(self):
        shared_engine = self._get_shared("strategy_engine", None)
        if shared_engine is not None and hasattr(shared_engine, "build_on_demand_plan_for_candidate"):
            return shared_engine

        cached_engine = self._get_shared("telegram_plan_strategy_engine", None)
        if cached_engine is not None and hasattr(cached_engine, "build_on_demand_plan_for_candidate"):
            return cached_engine

        from module05_strategy.module_05_strategy_pattern_tradeplan_engine import (
            Module05StrategyPatternTradePlanEngine,
        )

        engine = Module05StrategyPatternTradePlanEngine()
        self._set_shared("telegram_plan_strategy_engine", engine)
        return engine

    def _find_runtime_candidate(
        self,
        symbol: str,
        menu: MenuState,
        contract: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        item = menu.candidate_lookup.get(symbol) or {}
        candidate = item.get("candidate")
        if isinstance(candidate, dict) and candidate.get("symbol"):
            return candidate

        module04_output = (contract or {}).get("module04_output") or {}
        search_rows = []
        for key in ("candidate_list", "candidate_backup_list", "top_candidates", "backup_candidates"):
            rows = module04_output.get(key) or []
            if isinstance(rows, list):
                search_rows.extend(rows)

        for row in search_rows:
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
            if row_symbol == symbol:
                return row
        return None

    def _cache_generated_plan(
        self,
        symbol: str,
        plan: Dict[str, Any],
        explain_payload: Optional[Dict[str, Any]],
        contract: Optional[Dict[str, Any]],
        trade_plan_map: Dict[str, Dict[str, Any]],
        explain_map: Dict[str, Any],
    ) -> None:
        trade_plan_map[symbol] = plan
        if explain_payload:
            explain_map[symbol] = explain_payload

        if isinstance(contract, dict):
            contract.setdefault("trade_plan_map", {})[symbol] = plan
            if explain_payload:
                contract.setdefault("explain_map", {})[symbol] = explain_payload

            module05_output = contract.get("module05_output")
            if isinstance(module05_output, dict):
                tp = module05_output.get("trade_plan_map")
                if not isinstance(tp, dict):
                    tp = {}
                    module05_output["trade_plan_map"] = tp
                tp[symbol] = plan
                if explain_payload:
                    ep = module05_output.get("explain_map")
                    if not isinstance(ep, dict):
                        ep = {}
                        module05_output["explain_map"] = ep
                    ep[symbol] = explain_payload

        shared_trade_plan_map = self._get_shared("trade_plan_map", {})
        if isinstance(shared_trade_plan_map, dict):
            shared_trade_plan_map[symbol] = plan
            self._set_shared("trade_plan_map", shared_trade_plan_map)

        if explain_payload:
            shared_explain_map = self._get_shared("explain_map", {})
            if isinstance(shared_explain_map, dict):
                shared_explain_map[symbol] = explain_payload
                self._set_shared("explain_map", shared_explain_map)

    def _get_or_generate_plan(
        self,
        symbol: str,
        menu: MenuState,
        contract: Optional[Dict[str, Any]],
        trade_plan_map: Dict[str, Dict[str, Any]],
        explain_map: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[str], Dict[str, Any]]:
        plan_event = {
            "symbol": symbol,
            "plan_source": "none",
            "cached_plan_hit": False,
            "backup_plan_hit": False,
            "on_demand_attempted": False,
            "on_demand_reason": None,
            "reply_send": None,
        }
        plan = trade_plan_map.get(symbol) or trade_plan_map.get(symbol.upper()) or {}
        explain_payload = explain_map.get(symbol) or explain_map.get(symbol.upper()) or {}
        if plan:
            plan_event["plan_source"] = "cached_primary"
            plan_event["cached_plan_hit"] = True
            return plan, explain_payload if isinstance(explain_payload, dict) else {}, None, plan_event

        module05_output = (contract or {}).get("module05_output") or {}
        backup_plan_map = self._extract_named_plan_map(module05_output, "backup_plan_map")
        backup_explain_map = self._extract_named_explain_map(module05_output, "backup_explain_map")
        plan = backup_plan_map.get(symbol) or {}
        explain_payload = backup_explain_map.get(symbol) or {}
        if plan:
            plan_event["plan_source"] = "cached_backup"
            plan_event["backup_plan_hit"] = True
            self._cache_generated_plan(
                symbol=symbol,
                plan=plan,
                explain_payload=explain_payload if isinstance(explain_payload, dict) else {},
                contract=contract,
                trade_plan_map=trade_plan_map,
                explain_map=explain_map,
            )
            return plan, explain_payload if isinstance(explain_payload, dict) else {}, None, plan_event

        candidate = self._find_runtime_candidate(symbol, menu, contract)
        if not isinstance(candidate, dict):
            plan_event["on_demand_reason"] = "candidate_not_found"
            return {}, {}, "candidate_not_found", plan_event

        try:
            strategy_engine = self._get_on_demand_strategy_engine()
            runtime_inputs = self._prepare_on_demand_runtime_inputs(symbol, candidate, contract)
            plan_event["on_demand_attempted"] = True
            generated = strategy_engine.build_on_demand_plan_for_candidate(
                candidate=candidate,
                market_context=runtime_inputs["market_context"],
                snapshot_store=runtime_inputs["snapshot_store"],
                candle_store_5m=runtime_inputs["candle_store_5m"],
                candle_store_1m=runtime_inputs["candle_store_1m"],
                candle_store_15m=runtime_inputs["candle_store_15m"],
                now_ts=now_ts(),
            )
        except Exception as e:
            self.logger.exception("on-demand plan generation failed for %s", symbol)
            plan_event["on_demand_reason"] = f"generation_error:{e}"
            return {}, {}, f"generation_error:{e}", plan_event

        if not isinstance(generated, dict) or not generated.get("ok"):
            reason = "generation_failed"
            if isinstance(generated, dict):
                reason = str(generated.get("reason") or reason)
            plan_event["on_demand_reason"] = reason
            return {}, {}, reason, plan_event

        plan = generated.get("plan") or {}
        explain_payload = generated.get("explain_payload") or {}
        if not isinstance(plan, dict) or not plan:
            plan_event["on_demand_reason"] = "empty_generated_plan"
            return {}, {}, "empty_generated_plan", plan_event

        if not generated.get("signal_eligible", True):
            warnings = plan.get("warnings") or []
            if not isinstance(warnings, list):
                warnings = [str(warnings)]
            warnings.append("On-demand plan preview for current menu stock; this setup was not auto-recommended this cycle.")
            plan["warnings"] = list(dict.fromkeys([str(x) for x in warnings if str(x).strip()]))

        self._cache_generated_plan(
            symbol=symbol,
            plan=plan,
            explain_payload=explain_payload if isinstance(explain_payload, dict) else {},
            contract=contract,
            trade_plan_map=trade_plan_map,
            explain_map=explain_map,
        )
        plan_event["plan_source"] = "on_demand_generated"
        plan_event["on_demand_reason"] = "generated"
        return plan, explain_payload if isinstance(explain_payload, dict) else {}, None, plan_event

    # -------------------------------------------------------------------------
    # Plan / Explain Builders
    # -------------------------------------------------------------------------

    def _normalize_plan(self, symbol: str, plan: Dict[str, Any]) -> Dict[str, Any]:
        expires_at = plan.get("expires_at")
        validity_minutes = safe_int(plan.get("validity_minutes"), 0)
        validity_candles = safe_int(plan.get("validity_candles"), 0)

        if expires_at is None:
            if validity_minutes > 0:
                expires_at = now_ts() + validity_minutes * 60
            elif validity_candles > 0:
                expires_at = now_ts() + validity_candles * 5 * 60
            else:
                expires_at = now_ts() + 10 * 60

        direction = str(plan.get("direction") or "LONG").upper()
        setup_name = str(plan.get("setup_name") or plan.get("setup") or "SETUP")
        confidence = safe_float(
            plan.get("confidence")
            or plan.get("confidence_score"),
            None
        )
        reasons = plan.get("reasons") or plan.get("top_reasons") or []
        warnings = plan.get("warnings") or []
        pattern_tags = plan.get("patterns") or plan.get("pattern_tags") or []

        normalized = {
            "symbol": symbol,
            "direction": direction,
            "setup_name": setup_name,
            "entry": safe_float(plan.get("entry") or plan.get("entry_trigger")),
            "sl": safe_float(plan.get("sl") or plan.get("stop_loss")),
            "t1": safe_float(plan.get("t1") or plan.get("T1") or plan.get("target1")),
            "t2": safe_float(plan.get("t2") or plan.get("T2") or plan.get("target2")),
            "validity_window": str(plan.get("validity_window") or ""),
            "expires_at": safe_float(expires_at),
            "confidence": confidence,
            "market_regime": str(
                plan.get("market_regime")
                or plan.get("regime")
                or ""
            ),
            "reasons": reasons if isinstance(reasons, list) else [str(reasons)],
            "warnings": warnings if isinstance(warnings, list) else [str(warnings)],
            "patterns": pattern_tags if isinstance(pattern_tags, list) else [str(pattern_tags)],
            "explain_payload": plan.get("explain_payload"),
            "raw": plan,
        }
        return normalized

    def _build_trade_plan_text_and_state(
        self,
        symbol: str,
        plan: Dict[str, Any],
        menu_id: str,
    ) -> Tuple[str, SignalState]:
        p = self._normalize_plan(symbol, plan)
        mc = self.get_market_context()
        mc_state = mc.get("market_context_state") if isinstance(mc, dict) else {}

        gate = str(
            p.get("raw", {}).get("trade_gate")
            or (mc_state or {}).get("trade_gate")
            or mc.get("trade_gate")
            or "-"
        )
        regime = str(
            p.get("market_regime")
            or (mc_state or {}).get("market_regime")
            or (mc_state or {}).get("macro_regime")
            or mc.get("market_regime")
            or mc.get("macro_regime")
            or "-"
        )

        reasons = p["reasons"][:5]
        warnings = p["warnings"][:4]

        lines = []
        lines.append(f"📌 TRADE PLAN | {symbol}")
        lines.append(f"Menu ID: {menu_id}")
        lines.append(f"Direction: {p['direction']}")
        lines.append(f"Setup: {p['setup_name']}")
        lines.append(f"Entry: {p['entry'] if p['entry'] is not None else '-'}")
        lines.append(f"Stop-loss: {p['sl'] if p['sl'] is not None else '-'}")
        lines.append(f"T1: {p['t1'] if p['t1'] is not None else '-'}")
        lines.append(f"T2: {p['t2'] if p['t2'] is not None else '-'}")
        lines.append(f"Validity: {p['validity_window'] or 'next 10 minutes / 2 candles'}")
        lines.append(f"Confidence: {p['confidence'] if p['confidence'] is not None else '-'}")
        lines.append(f"Market: Gate {gate} | Regime {regime}")
        if reasons:
            lines.append("")
            lines.append("Reasons:")
            for r in reasons:
                lines.append(f"- {r}")
        if warnings:
            lines.append("")
            lines.append("Warnings:")
            for w in warnings:
                lines.append(f"- {w}")
        lines.append("")
        lines.append("T2 continuation rule: follow-up will be sent only if live momentum continues.")

        signal_hash = compact_json_hash(self._plan_material_subset(p))
        state = SignalState(
            symbol=symbol,
            menu_id=menu_id,
            direction=p["direction"],
            setup_name=p["setup_name"],
            entry=p["entry"],
            sl=p["sl"],
            t1=p["t1"],
            t2=p["t2"],
            confidence=p["confidence"],
            expires_at=p["expires_at"],
            created_at=now_ts(),
            updated_at=now_ts(),
            status="ACTIVE",
            signal_hash=signal_hash,
            source_plan=p["raw"],
        )
        return "\n".join(lines), state

    def _plan_material_subset(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "direction": plan.get("direction"),
            "setup_name": plan.get("setup_name"),
            "entry": safe_float(plan.get("entry") or plan.get("entry_trigger")),
            "sl": safe_float(plan.get("sl") or plan.get("stop_loss")),
            "t1": safe_float(plan.get("t1") or plan.get("T1") or plan.get("target1")),
            "t2": safe_float(plan.get("t2") or plan.get("T2") or plan.get("target2")),
            "confidence": safe_float(
                plan.get("confidence")
                or plan.get("confidence_score")
            ),
            "validity_window": plan.get("validity_window"),
            "expires_at": safe_float(plan.get("expires_at")),
        }

    def _plan_should_send(self, symbol: str, new_hash: str) -> bool:
        with self._lock:
            old_hash = self._last_plan_hash_by_symbol.get(symbol)
            last_ts = self._last_symbol_alert_ts.get(symbol)
            if old_hash != new_hash:
                return True
            if not last_ts:
                return True
            return (now_ts() - last_ts) >= self.config.plan_cooldown_sec

    def _build_explain_text(self, symbol: str, explain_payload: Any, menu_id: Optional[str] = None) -> str:
        if isinstance(explain_payload, dict):
            setup_name = str(explain_payload.get("setup_name") or "")
            direction = str(explain_payload.get("direction") or "")
            market_regime = str(explain_payload.get("market_regime") or "")
            sector = str(explain_payload.get("sector") or "")
            quality_score = explain_payload.get("quality_score")
            confidence_score = explain_payload.get("confidence_score")
            rr_t1 = explain_payload.get("rr_t1")
            rr_t2 = explain_payload.get("rr_t2")

            patterns = (
                explain_payload.get("patterns")
                or explain_payload.get("pattern_meaning")
                or explain_payload.get("pattern_confirmations")
                or []
            )
            indicators = (
                explain_payload.get("indicators")
                or explain_payload.get("indicator_reasoning")
                or []
            )
            regime_fit = (
                explain_payload.get("regime_fit")
                or explain_payload.get("regime_reason")
                or market_regime
                or ""
            )
            notes = (
                explain_payload.get("notes")
                or explain_payload.get("why")
                or explain_payload.get("reasons")
                or []
            )
            warnings = explain_payload.get("warnings") or []
            indicator_snapshot = explain_payload.get("indicator_snapshot") or {}

            if not isinstance(patterns, list):
                patterns = [str(patterns)]
            if not isinstance(indicators, list):
                indicators = [str(indicators)]
            if not isinstance(notes, list):
                notes = [str(notes)]
            if not isinstance(warnings, list):
                warnings = [str(warnings)]

            if not indicators and isinstance(indicator_snapshot, dict) and indicator_snapshot:
                indicators = [f"{k}: {v}" for k, v in indicator_snapshot.items()]

            lines = [f"🧠 EXPLAIN | {symbol}"]
            if menu_id:
                lines.append(f"Menu ID: {menu_id}")
            if setup_name or direction:
                lines.append(f"Setup: {setup_name or '-'} | Direction: {direction or '-'}")
            if sector or market_regime:
                lines.append(f"Sector: {sector or '-'} | Regime: {market_regime or '-'}")
            if quality_score is not None or confidence_score is not None:
                lines.append(
                    f"Quality: {quality_score if quality_score is not None else '-'} | "
                    f"Confidence: {confidence_score if confidence_score is not None else '-'}"
                )
            if rr_t1 is not None or rr_t2 is not None:
                lines.append(
                    f"RR T1: {rr_t1 if rr_t1 is not None else '-'} | "
                    f"RR T2: {rr_t2 if rr_t2 is not None else '-'}"
                )

            if patterns:
                lines.append("")
                lines.append("Pattern confirmations:")
                for p in patterns[:5]:
                    lines.append(f"- {p}")

            if indicators:
                lines.append("")
                lines.append("Indicator context:")
                for i in indicators[:6]:
                    lines.append(f"- {i}")

            if regime_fit:
                lines.append("")
                lines.append(f"Regime fit: {regime_fit}")

            if notes:
                lines.append("")
                lines.append("Reasons:")
                for n in notes[:5]:
                    lines.append(f"- {n}")

            if warnings:
                lines.append("")
                lines.append("Warnings:")
                for w in warnings[:4]:
                    lines.append(f"- {w}")

            return "\n".join(lines)

        return f"🧠 EXPLAIN | {symbol}\n\n{str(explain_payload)}"

    # -------------------------------------------------------------------------
    # Active Signal Registry
    # -------------------------------------------------------------------------

    def _register_signal(self, state: SignalState) -> None:
        with self._lock:
            state.updated_at = now_ts()
            self._active_signals[state.symbol] = state
            self._bound_active_signals()
            self.health.active_signal_count = len(self._active_signals)
        self._persist_signals()
        if isinstance(self.shared_state, dict):
            self.shared_state["active_alerts"] = {k: v.to_dict() for k, v in self._active_signals.items()}

    def _bound_active_signals(self) -> None:
        if len(self._active_signals) <= self.config.max_active_signals:
            return
        ordered = sorted(self._active_signals.values(), key=lambda s: s.updated_at)
        while len(ordered) > self.config.max_active_signals:
            old = ordered.pop(0)
            self._active_signals.pop(old.symbol, None)

    # -------------------------------------------------------------------------
    # Snapshot Helpers / Follow-up Logic
    # -------------------------------------------------------------------------

    def _extract_snapshot(self, symbol: str) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        snapshots = self.get_snapshots()
        if not isinstance(snapshots, dict):
            return None, None, {}

        direct = snapshots.get(symbol) or snapshots.get(symbol.upper()) or {}
        if direct:
            price = safe_float(
                direct.get("ltp") or direct.get("last_price") or direct.get("price") or direct.get("close"),
                None,
            )
            tick_ts = safe_float(direct.get("ts") or direct.get("timestamp") or direct.get("tick_time"), None)
            return price, tick_ts, direct

        # try nested structures
        for key in ("snapshot_store", "latest", "symbols"):
            sub = snapshots.get(key)
            if isinstance(sub, dict):
                d = sub.get(symbol) or sub.get(symbol.upper())
                if isinstance(d, dict):
                    price = safe_float(
                        d.get("ltp") or d.get("last_price") or d.get("price") or d.get("close"),
                        None,
                    )
                    tick_ts = safe_float(d.get("ts") or d.get("timestamp") or d.get("tick_time"), None)
                    return price, tick_ts, d
        return None, None, {}

    def _snapshot_is_fresh(self, tick_ts: Optional[float]) -> bool:
        if tick_ts is None:
            return False
        return (now_ts() - tick_ts) <= self.config.snapshot_stale_sec

    def _followup_loop(self) -> None:
        self.logger.info("Follow-up loop started.")
        while not self._stop_event.is_set():
            try:
                self.health.followup_loop_alive = True
                self.health.last_followup_check = now_ts()
                self._check_followups_once()
            except Exception as e:
                self.health.last_error = f"followup_loop: {e}"
                self.logger.exception("Follow-up loop error")
            time.sleep(self.config.followup_check_sec)

    def _expiry_loop(self) -> None:
        self.logger.info("Expiry loop started.")
        while not self._stop_event.is_set():
            try:
                self.health.expiry_loop_alive = True
                self.health.last_expiry_check = now_ts()
                self._check_signal_expiry_once()
            except Exception as e:
                self.health.last_error = f"expiry_loop: {e}"
                self.logger.exception("Expiry loop error")
            time.sleep(self.config.expiry_check_sec)

    def _check_signal_expiry_once(self) -> None:
        to_expire: List[SignalState] = []
        with self._lock:
            for signal in self._active_signals.values():
                if signal.status in ("EXPIRED", "INVALIDATED", "T2_DONE"):
                    continue
                if signal.expires_at and now_ts() > signal.expires_at:
                    to_expire.append(signal)

        for signal in to_expire:
            with self._lock:
                live = self._active_signals.get(signal.symbol)
                if not live:
                    continue
                if live.status not in ("ACTIVE", "T1_DONE"):
                    continue
                live.status = "EXPIRED"
                live.updated_at = now_ts()

            if not signal.expiry_sent:
                msg = (
                    f"⌛ SIGNAL EXPIRED | {signal.symbol}\n"
                    f"Menu ID: {signal.menu_id}\n"
                    f"Setup: {signal.setup_name}\n"
                    f"No further T1/T2 follow-up will be tracked for this signal."
                )
                self._enqueue(
                    QueueItem(
                        priority=self.PRIORITY_FOLLOWUP,
                        created_at=now_ts(),
                        kind=self.ALERT_FOLLOWUP,
                        text=msg,
                        metadata={"symbol": signal.symbol, "menu_id": signal.menu_id, "signal_state": "EXPIRED"},
                        dedup_key=f"followup:expiry:{signal.symbol}:{signal.menu_id}",
                    )
                )
                with self._lock:
                    if signal.symbol in self._active_signals:
                        self._active_signals[signal.symbol].expiry_sent = True

        self._persist_signals()

    def _check_followups_once(self) -> None:
        signals = []
        with self._lock:
            signals = list(self._active_signals.values())

        for signal in signals:
            if signal.status in ("EXPIRED", "INVALIDATED", "T2_DONE"):
                continue

            price, tick_ts, _snap = self._extract_snapshot(signal.symbol)
            if price is None or not self._snapshot_is_fresh(tick_ts):
                continue

            with self._lock:
                live = self._active_signals.get(signal.symbol)
                if not live:
                    continue
                live.last_price = price
                live.last_tick_time = tick_ts
                live.updated_at = now_ts()

            self._evaluate_signal_followups(live, price)

        self._persist_signals()

    def _evaluate_signal_followups(self, signal: SignalState, price: float) -> None:
        if signal.direction == "LONG":
            self._evaluate_long_followups(signal, price)
        else:
            self._evaluate_short_followups(signal, price)

    def _evaluate_long_followups(self, signal: SignalState, price: float) -> None:
        # Invalidation
        if signal.sl is not None and price <= signal.sl * (1 - self.config.invalidation_sl_buffer_pct):
            if not signal.invalidation_sent:
                self._send_signal_invalidation(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.status = "INVALIDATED"
                        live.invalidation_sent = True
                        live.updated_at = now_ts()
            return

        # T1
        if signal.t1 is not None and price >= signal.t1 * (1 + self.config.t1_rearm_buffer_pct):
            if not signal.t1_sent:
                self._send_t1_followup(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.t1_sent = True
                        live.trailing_sent = True
                        live.status = "T1_DONE"
                        live.updated_at = now_ts()

        # T2
        if signal.t2 is not None and price >= signal.t2 * (1 + self.config.t2_trigger_buffer_pct):
            if signal.t1_sent and not signal.t2_sent:
                self._send_t2_followup(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.t2_sent = True
                        live.status = "T2_DONE"
                        live.updated_at = now_ts()

    def _evaluate_short_followups(self, signal: SignalState, price: float) -> None:
        if signal.sl is not None and price >= signal.sl * (1 + self.config.invalidation_sl_buffer_pct):
            if not signal.invalidation_sent:
                self._send_signal_invalidation(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.status = "INVALIDATED"
                        live.invalidation_sent = True
                        live.updated_at = now_ts()
            return

        if signal.t1 is not None and price <= signal.t1 * (1 - self.config.t1_rearm_buffer_pct):
            if not signal.t1_sent:
                self._send_t1_followup(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.t1_sent = True
                        live.trailing_sent = True
                        live.status = "T1_DONE"
                        live.updated_at = now_ts()

        if signal.t2 is not None and price <= signal.t2 * (1 - self.config.t2_trigger_buffer_pct):
            if signal.t1_sent and not signal.t2_sent:
                self._send_t2_followup(signal, price)
                with self._lock:
                    live = self._active_signals.get(signal.symbol)
                    if live:
                        live.t2_sent = True
                        live.status = "T2_DONE"
                        live.updated_at = now_ts()

    def _send_t1_followup(self, signal: SignalState, price: float) -> None:
        trail_hint = self._suggest_trail(signal)
        text = (
            f"🎯 T1 HIT | {signal.symbol}\n"
            f"Menu ID: {signal.menu_id}\n"
            f"Setup: {signal.setup_name}\n"
            f"Direction: {signal.direction}\n"
            f"Live Price: {price}\n"
            f"Guidance: {trail_hint}\n"
            f"T2 will be monitored only if momentum continues."
        )
        self._enqueue(
            QueueItem(
                priority=self.PRIORITY_FOLLOWUP,
                created_at=now_ts(),
                kind=self.ALERT_FOLLOWUP,
                text=text,
                metadata={"symbol": signal.symbol, "menu_id": signal.menu_id, "signal_state": "T1_DONE"},
                dedup_key=f"followup:t1:{signal.symbol}:{signal.menu_id}",
            )
        )

    def _send_t2_followup(self, signal: SignalState, price: float) -> None:
        text = (
            f"🚀 T2 POSSIBLE / REACHED | {signal.symbol}\n"
            f"Menu ID: {signal.menu_id}\n"
            f"Setup: {signal.setup_name}\n"
            f"Direction: {signal.direction}\n"
            f"Live Price: {price}\n"
            f"Momentum continuation confirmed from snapshot updates."
        )
        self._enqueue(
            QueueItem(
                priority=self.PRIORITY_FOLLOWUP,
                created_at=now_ts(),
                kind=self.ALERT_FOLLOWUP,
                text=text,
                metadata={"symbol": signal.symbol, "menu_id": signal.menu_id, "signal_state": "T2_DONE"},
                dedup_key=f"followup:t2:{signal.symbol}:{signal.menu_id}",
            )
        )

    def _send_signal_invalidation(self, signal: SignalState, price: float) -> None:
        text = (
            f"❌ SETUP INVALIDATED | {signal.symbol}\n"
            f"Menu ID: {signal.menu_id}\n"
            f"Setup: {signal.setup_name}\n"
            f"Direction: {signal.direction}\n"
            f"Live Price: {price}\n"
            f"Signal tracking stopped for this setup."
        )
        self._enqueue(
            QueueItem(
                priority=self.PRIORITY_FOLLOWUP,
                created_at=now_ts(),
                kind=self.ALERT_FOLLOWUP,
                text=text,
                metadata={"symbol": signal.symbol, "menu_id": signal.menu_id, "signal_state": "INVALIDATED"},
                dedup_key=f"followup:invalidate:{signal.symbol}:{signal.menu_id}",
            )
        )

    def _suggest_trail(self, signal: SignalState) -> str:
        if signal.entry is None or signal.sl is None:
            return "Consider tightening stop based on structure and live momentum."
        if signal.direction == "LONG":
            new_sl = max(signal.entry, signal.sl)
        else:
            new_sl = min(signal.entry, signal.sl)
        return f"Trail stop toward {new_sl} if spread/liquidity remains favorable."

    # -------------------------------------------------------------------------
    # Heartbeat / Status
    # -------------------------------------------------------------------------

    def _build_heartbeat_text(self) -> str:
        ext = self.get_external_health()
        mc = self.get_market_context()
        with self._lock:
            queue_size = self._safe_qsize()
            active_count = len(self._active_signals)
            last_menu_id = self._latest_menu_id or "-"
            sent = self.health.messages_sent
            failed = self.health.messages_failed
            retries = self.health.retries
            last_err = self.health.telegram_last_error or self.health.last_error or "-"
            latest_menu = self._menus.get(self._latest_menu_id or "") if self._latest_menu_id else None
            candidate_count = len(latest_menu.candidates) if latest_menu else 0
            recommended_count = len(latest_menu.recommended_symbols) if latest_menu else 0

        broker = ext.get("broker", ext.get("module01", {}))
        ws = ext.get("websocket", ext.get("module02", {}))

        lines = []
        lines.append("💓 SYSTEM HEARTBEAT")
        lines.append(f"Module06: {self.health.state}")
        lines.append(
            f"Broker: {broker.get('state', broker.get('session_state', '-'))} | "
            f"Token last: {broker.get('last_refresh_iso', broker.get('last_refresh', '-'))} | "
            f"Fail count: {broker.get('fail_count', broker.get('failures', '-'))}"
        )
        lines.append(
            f"WebSocket: {ws.get('state', '-')} | "
            f"Connected: {ws.get('ws_connected', ws.get('connected', '-'))} | "
            f"Last tick: {ws.get('last_tick_iso', ws.get('last_tick_time', '-'))} | "
            f"Reconnects: {ws.get('reconnect_count', ws.get('reconnects', '-'))}"
        )
        lines.append(
            f"Scan/Menu: Last menu {last_menu_id} | Candidates {candidate_count} | Recommended {recommended_count}"
        )
        lines.append(
            f"Market: Gate {mc.get('trade_gate', '-')} | Regime {mc.get('market_regime', '-')} | "
            f"Bias {mc.get('macro_bias', '-')}"
        )
        lines.append(
            f"AlertEngine: Queue {queue_size} | Active signals {active_count} | Sent {sent} | Failed {failed} | Retries {retries}"
        )
        lines.append(f"Telegram last error: {last_err}")

        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Sender / Cleanup
    # -------------------------------------------------------------------------

    def _sender_loop(self) -> None:
        self.logger.info("Sender loop started.")
        while not self._stop_event.is_set():
            self.health.sender_loop_alive = True
            try:
                try:
                    _, _, _, _, item = self._send_queue.get(timeout=self.config.sender_poll_sec)
                except queue.Empty:
                    if self._should_auto_heartbeat():
                        self.send_status()
                    continue

                self._rate_limit_sleep(bypass=item.bypass_rate_limit)
                self._send_item(item)
            except Exception as e:
                self.health.last_error = f"sender_loop: {e}"
                self.logger.exception("Sender loop error")
                time.sleep(1.0)

    def _poll_updates_loop(self) -> None:
        self.logger.info("Telegram poll loop started.")
        while not self._stop_event.is_set():
            try:
                if not self.config.telegram_bot_token or not self.config.telegram_bot_token.strip():
                    time.sleep(5.0)
                    continue

                self.poll_telegram_once(timeout=10)
            except Exception as e:
                self.health.telegram_last_error = str(e)
                self.health.telegram_last_error_time = now_ts()
                self.logger.exception("Telegram poll loop error")
                time.sleep(1.0)

    def _send_item(self, item: QueueItem) -> None:
        generated_at = item.created_at
        queue_wait_ms = int((now_ts() - generated_at) * 1000)
        send_ok = False
        telegram_message_id = None
        error_details = None
        sent_at = None
        message_expiry_time = None
        registry_item: Optional[MessageRegistryItem] = None

        for attempt in range(self.config.retry_attempts + 1):
            try:
                self._record_send_timestamp()
                resp = self.telegram.send_message(
                    item.text,
                    parse_mode=item.parse_mode or self.config.telegram_parse_mode,
                    disable_notification=item.disable_notification,
                    chat_id=item.metadata.get("reply_chat_id"),
                )
                sent_at = now_ts()
                send_ok = True
                telegram_message_id = resp.get("result", {}).get("message_id")
                break
            except Exception as e:
                error_details = str(e)
                self.health.telegram_last_error = error_details
                self.health.telegram_last_error_time = now_ts()
                if attempt < self.config.retry_attempts:
                    self.health.retries += 1
                    backoff = self.config.retry_backoff_sec * (self.config.retry_backoff_multiplier ** attempt)
                    time.sleep(backoff)
                else:
                    break

        with self._lock:
            if send_ok:
                self.health.messages_sent += 1
                self.health.last_send_time = sent_at
                self.health.last_send_iso = ts_to_ist_str(sent_at)
                symbol = item.metadata.get("symbol")
                if symbol:
                    self._last_symbol_alert_ts[symbol] = sent_at or now_ts()
                if item.kind == self.ALERT_EXPLAIN and symbol:
                    self._last_symbol_alert_ts[f"EXPLAIN:{symbol}"] = sent_at or now_ts()

                if item.kind == self.ALERT_HEARTBEAT:
                    self._last_heartbeat_hash = compact_json_hash(item.text)
                    self._last_heartbeat_time = sent_at or now_ts()
                    self.health.last_heartbeat_time = sent_at or now_ts()

                if item.kind == self.ALERT_MENU:
                    self._last_menu_time = sent_at or now_ts()
            else:
                self.health.messages_failed += 1
                self.health.last_error = error_details
                self.health.degraded = True

        if send_ok:
            message_expiry_time = self._resolve_message_expiry_time()
        if send_ok and telegram_message_id is not None:
            registry_item = self._record_message_registry(item, telegram_message_id, message_expiry_time)

        latency_ms = int(((sent_at or now_ts()) - generated_at) * 1000)
        self._journal(
            {
                "alert_type": item.kind,
                "menu_id": item.metadata.get("menu_id"),
                "symbol": item.metadata.get("symbol"),
                "symbols": item.metadata.get("symbols"),
                "setup_name": item.metadata.get("setup_name"),
                "direction": item.metadata.get("direction"),
                "entry": item.metadata.get("entry"),
                "sl": item.metadata.get("sl"),
                "t1": item.metadata.get("t1"),
                "t2": item.metadata.get("t2"),
                "confidence": item.metadata.get("confidence"),
                "market_regime": self.get_market_context().get("market_regime"),
                "trade_gate": self.get_market_context().get("trade_gate"),
                "message_id": telegram_message_id,
                "send_status": "SENT" if send_ok else "FAILED",
                "generated_at": generated_at,
                "generated_at_ist": ts_to_ist_str(generated_at),
                "sent_at": sent_at,
                "sent_at_ist": ts_to_ist_str(sent_at),
                "latency_ms": latency_ms,
                "priority": item.priority,
                "queue_wait_ms": queue_wait_ms,
                "retry_count": self.config.retry_attempts if (not send_ok and error_details) else item.retry_count,
                "signal_state": item.metadata.get("signal_state"),
                "expiry_at": item.metadata.get("expiry_at"),
                "expiry_time": message_expiry_time,
                "expiry_time_ist": ts_to_ist_str(message_expiry_time),
                "telegram_chat_id": self.config.telegram_chat_id,
                "source_chat_id": item.metadata.get("source_chat_id"),
                "target_chat_id": item.metadata.get("reply_chat_id") or self.config.telegram_chat_id,
                "deletion_scheduled_for": ts_to_ist_str((registry_item.scheduled_delete_after if registry_item else message_expiry_time)),
                "error_details": error_details,
                "module_health_snapshot": {
                    "state": self.health.state,
                    "queue_size": self._safe_qsize(),
                    "messages_sent": self.health.messages_sent,
                    "messages_failed": self.health.messages_failed,
                    "active_signal_count": len(self._active_signals),
                },
            }
        )

        self._persist_health()
        self._persist_message_registry()

    def _record_message_registry(
        self,
        item: QueueItem,
        message_id: int,
        expiry_time: Optional[float] = None,
    ) -> MessageRegistryItem:
        delete_after = expiry_time if expiry_time is not None else self._resolve_message_expiry_time()
        deletable = bool(self.config.auto_delete_enabled and message_id is not None)

        reg = MessageRegistryItem(
            timestamp=now_ts(),
            menu_id=item.metadata.get("menu_id"),
            alert_type=item.kind,
            telegram_message_id=message_id,
            chat_id=str(item.metadata.get("reply_chat_id") or self.config.telegram_chat_id),
            deletable=deletable,
            expiry_time=delete_after,
            scheduled_delete_after=delete_after,
            symbol=item.metadata.get("symbol"),
            extra={k: v for k, v in item.metadata.items() if k not in {"menu_id", "symbol"}},
        )

        with self._lock:
            self._message_registry.append(reg)
            while len(self._message_registry) > self.config.max_message_registry_per_day:
                self._message_registry.pop(0)
        return reg

    def _scheduled_delete_after(self) -> Optional[float]:
        return self._resolve_message_expiry_time()

    def _resolve_message_expiry_time(self) -> Optional[float]:
        dt = now_ist()
        mode_ctx = self._mode_context()

        if mode_ctx.get("offmarket_test"):
            forced_seconds = safe_float(os.getenv("FORCE_EXPIRY_SECONDS"), None)
            if forced_seconds is not None and forced_seconds > 0:
                return now_ts() + float(forced_seconds)
            return now_ts() + 180.0

        market_close = dt.replace(
            hour=self.config.market_close_hour,
            minute=self.config.market_close_minute,
            second=0,
            microsecond=0,
        )
        delete_at = market_close + timedelta(minutes=self.config.auto_delete_buffer_min)
        return delete_at.timestamp()

    def _cleanup_loop(self) -> None:
        self.logger.info("Cleanup loop started.")
        while not self._stop_event.is_set():
            try:
                self.health.cleanup_loop_alive = True
                self._cleanup_message_registry()
            except Exception:
                self.logger.exception("Cleanup loop error")
            time.sleep(self.config.cleanup_check_sec)

    def _cleanup_message_registry(self) -> None:
        if not self.config.auto_delete_enabled:
            return
        cutoff = now_ts()
        due = []
        with self._lock:
            for item in self._message_registry:
                expiry_time = item.expiry_time if item.expiry_time is not None else item.scheduled_delete_after
                if (
                    item.deletable
                    and item.telegram_message_id is not None
                    and expiry_time is not None
                    and expiry_time <= cutoff
                ):
                    due.append(item)

        deleted_ids: set = set()
        for item in due:
            expiry_time = item.expiry_time if item.expiry_time is not None else item.scheduled_delete_after
            self.logger.info(
                "message_expired -> deleting | message_id=%s | chat_id=%s | expiry_time=%s",
                item.telegram_message_id,
                item.chat_id,
                ts_to_ist_str(expiry_time),
            )
            if self.delete_message_now(
                message_id=int(item.telegram_message_id),
                chat_id=item.chat_id,
                menu_id=item.menu_id,
                symbol=item.symbol,
                expiry_time=expiry_time,
            ):
                deleted_ids.add(int(item.telegram_message_id))

        if deleted_ids:
            with self._lock:
                self._message_registry = [
                    item
                    for item in self._message_registry
                    if item.telegram_message_id not in deleted_ids
                ]
            self._persist_message_registry()

    def delete_message_now(
        self,
        message_id: int,
        menu_id: Optional[str] = None,
        symbol: Optional[str] = None,
        chat_id: Optional[Any] = None,
        expiry_time: Optional[float] = None,
    ) -> bool:
        target_chat_id = chat_id if chat_id is not None else self.config.telegram_chat_id
        try:
            self.telegram.delete_message(message_id, chat_id=target_chat_id)
            self.logger.info(
                "message_deleted_success | message_id=%s | chat_id=%s | expiry_time=%s",
                message_id,
                target_chat_id,
                ts_to_ist_str(expiry_time),
            )
            self._journal(
                {
                    "alert_type": self.ALERT_DELETE,
                    "menu_id": menu_id,
                    "symbol": symbol,
                    "message_id": message_id,
                    "expiry_time": expiry_time,
                    "expiry_time_ist": ts_to_ist_str(expiry_time),
                    "deletion_attempt": True,
                    "deletion_success": True,
                    "deletion_failure": False,
                    "target_chat_id": target_chat_id,
                    "send_status": "DELETED",
                }
            )
            return True
        except Exception as e:
            self.logger.warning(
                "message_deleted_failed | message_id=%s | chat_id=%s | expiry_time=%s | error=%s",
                message_id,
                target_chat_id,
                ts_to_ist_str(expiry_time),
                e,
            )
            self._journal(
                {
                    "alert_type": self.ALERT_DELETE,
                    "menu_id": menu_id,
                    "symbol": symbol,
                    "message_id": message_id,
                    "expiry_time": expiry_time,
                    "expiry_time_ist": ts_to_ist_str(expiry_time),
                    "deletion_attempt": True,
                    "deletion_success": False,
                    "deletion_failure": True,
                    "target_chat_id": target_chat_id,
                    "send_status": "DELETE_FAILED",
                    "error_details": str(e),
                }
            )
            return False

    def _should_auto_heartbeat(self) -> bool:
        with self._lock:
            if self._last_heartbeat_time is None:
                return True
            return (now_ts() - self._last_heartbeat_time) >= self.config.heartbeat_interval_sec

    # -------------------------------------------------------------------------
    # Error / Helpers
    # -------------------------------------------------------------------------

    def _error_item(
        self,
        message: str,
        symbol: Optional[str] = None,
        bypass_cooldown: bool = False,
        reply_chat_id: Optional[Any] = None,
    ) -> QueueItem:
        return QueueItem(
            priority=self.PRIORITY_CRITICAL if bypass_cooldown else self.PRIORITY_FOLLOWUP,
            created_at=now_ts(),
            kind=self.ALERT_ERROR,
            text=f"⚠️ ALERT ENGINE\n\n{message}",
            metadata={
                "symbol": symbol,
                "source_chat_id": reply_chat_id,
                "reply_chat_id": reply_chat_id,
            },
            bypass_cooldown=bypass_cooldown,
            bypass_rate_limit=bypass_cooldown,
            dedup_key=f"error:{compact_json_hash({'m': message, 's': symbol})}",
        )

    # -------------------------------------------------------------------------
    # Lightweight Integration Helpers for Modules 04/05
    # -------------------------------------------------------------------------

    def publish_from_module_outputs(
        self,
        module04_output: Optional[Dict[str, Any]],
        module05_output: Optional[Dict[str, Any]],
        module03_output: Optional[Dict[str, Any]],
        force_menu: bool = False,
    ) -> Optional[str]:
        module04_output = module04_output or {}
        module05_output = module05_output or {}
        module03_output = module03_output or {}

        # ---------------------------------------------------------------------
        # Phase 3 strict contract
        # ---------------------------------------------------------------------
        candidates = module04_output.get("candidate_list") or []
        signals = module05_output.get("trade_signal_list") or []
        trade_plan_map = self.extract_trade_plan_map(module05_output)
        explain_map = self.extract_explain_map(module05_output)

        diagnostics = module05_output.get("strategy_diagnostics") or {}
        scanner_diagnostics = module04_output.get("scanner_diagnostics") or {}
        market_state = module03_output.get("market_context_state") or {}
        trade_gate = market_state.get("trade_gate", diagnostics.get("trade_gate", scanner_diagnostics.get("trade_gate", "UNKNOWN")))

        # Normalize to lists/dicts
        if not isinstance(candidates, list):
            candidates = []
        if not isinstance(signals, list):
            signals = []
        if not isinstance(trade_plan_map, dict):
            trade_plan_map = {}
        if not isinstance(explain_map, dict):
            explain_map = {}
        mode_ctx = self._mode_context()
        publish_policy = self._publish_policy(mode_ctx, force_menu=force_menu)

        # Valid menu candidates are scanner candidates only
        valid_candidates = [c for c in candidates if isinstance(c, dict) and c.get("symbol")]

        # Valid plan symbols are those that actually have a bound trade plan
        valid_plan_symbols = {
            str(sym).strip().upper()
            for sym in trade_plan_map.keys()
            if str(sym).strip()
        }

        # Filter signal list to those with symbols
        valid_signals = [
            s for s in signals
            if isinstance(s, dict) and str(s.get("symbol", "")).strip()
        ]

        # If there is no real payload, do not publish an empty shell menu
        if not valid_candidates and not valid_signals:
            self.logger.info("publish_from_module_outputs skipped: no valid candidates or signals.")
            return None

        # Build menu candidate payload from scanner candidates only
        menu_candidates: List[Dict[str, Any]] = []
        for c in valid_candidates:
            sym = str(c.get("symbol", "")).strip().upper()
            if not sym:
                continue

            menu_item = {
                "symbol": sym,
                "token": c.get("token"),
                "exchange": c.get("exchange", "NSE"),
                "sector": c.get("sector"),
                "tier": c.get("tier"),
                "rank_score": c.get("rank_score"),
                "score": c.get("score"),
                "composite_score": c.get("composite_score"),
                "component_scores": c.get("component_scores") or {},
                "regime_tag": c.get("regime_tag"),
                "regime_tag_used": c.get("regime_tag_used"),
                "market_regime": c.get("market_regime"),
                "macro_regime": c.get("macro_regime"),
                "snapshot": c.get("snapshot") or {},
                "candles_5m": c.get("candles_5m") or ((c.get("meta") or {}).get("candles_5m")) or [],
                "meta": c.get("meta") or {},
            }
            menu_candidates.append(menu_item)

        # If there are no scanner candidates but there are signals, optionally build a minimal menu
        # only from symbols that actually exist in the trade plan map.
        if not menu_candidates and valid_signals:
            seen = set()
            for s in valid_signals:
                sym = str(s.get("symbol", "")).strip().upper()
                if not sym or sym in seen or sym not in valid_plan_symbols:
                    continue
                seen.add(sym)
                menu_candidates.append({
                    "symbol": sym,
                    "token": s.get("token"),
                    "exchange": s.get("exchange", "NSE"),
                    "sector": s.get("sector"),
                    "rank_score": s.get("rank_score"),
                    "snapshot": {},
                    "candles_5m": [],
                })

        if not menu_candidates:
            self.logger.info("publish_from_module_outputs skipped: menu_candidates empty after validation.")
            return None

        # Create/publish menu
        menu_id = None
        if publish_policy["send_menu"]:
            menu_id = self.publish_candidate_menu(
                candidates=menu_candidates,
                trade_signal_list=valid_signals,
                market_context=module03_output,
                source_cycle_id=module04_output.get("scan_cycle_id") or module05_output.get("scan_cycle_id"),
                force=publish_policy["force_menu"],
            )

        # Bind runtime contract only when a real menu was created
        if menu_id:
            self._bind_runtime_contract(
                menu_id=menu_id,
                trade_plan_map=trade_plan_map,
                explain_map=explain_map,
                module04_output=module04_output,
                module05_output=module05_output,
                module03_output=module03_output,
            )

        # Publish individual plans if appropriate
        # Keep this behavior conservative: only send plans that actually exist in trade_plan_map
        if publish_policy["send_plans"]:
            for sig in valid_signals:
                sym = str(sig.get("symbol", "")).strip().upper()
                plan = trade_plan_map.get(sym)
                if not sym or not isinstance(plan, dict):
                    continue
                try:
                    self.send_trade_plan_reply(
                        selected_letters_or_symbols=[sym],
                        trade_plan_map=trade_plan_map,
                        explain_map=explain_map,
                        menu_id=menu_id,
                        send_explain=False,
                    )
                except Exception:
                    self.logger.exception("send_trade_plan_reply failed for %s", sym)

        return menu_id

    def extract_trade_plan_map(self, module05_output: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        plan_map = {}
        raw = (
            module05_output.get("trade_plan_map")
            or module05_output.get("plans")
            or module05_output.get("per_symbol_trade_plans")
            or {}
        )
        if isinstance(raw, dict):
            for k, v in raw.items():
                plan_map[str(k).upper()] = v
        elif isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("symbol") or item.get("ticker") or "").upper()
                if sym:
                    plan_map[sym] = item
        return plan_map

    def extract_explain_map(self, module05_output: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        raw = module05_output.get("explain_map") or module05_output.get("explanations") or {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                out[str(k).upper()] = v
        elif isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("symbol") or "").upper()
                if sym:
                    out[sym] = item
        return out

    @staticmethod
    def _validation_check_on_demand_plan_retrieval() -> Dict[str, Any]:
        base_dir = os.path.join(os.getcwd(), "runtime", "validation", "module06_on_demand")
        cfg = AlertEngineConfig(
            telegram_bot_token="TEST",
            telegram_chat_id="TEST",
            journal_dir=os.path.join(base_dir, "journal"),
            state_dir=os.path.join(base_dir, "state"),
            deletion_registry_dir=os.path.join(base_dir, "deletion"),
            log_dir=os.path.join(base_dir, "logs"),
            debug=False,
        )
        shared_state: Dict[str, Any] = {}
        engine = AlertEngine(
            config=cfg,
            snapshot_getter=lambda: {},
            market_context_getter=lambda: {
                "trade_gate": "OPEN",
                "market_context_state": {
                    "trade_gate": "OPEN",
                    "market_regime": "TRENDING",
                    "macro_bias": "BULLISH",
                },
            },
            health_getter=lambda: {},
            shared_state=shared_state,
        )

        captured: List[Dict[str, Any]] = []

        def _capture(item: QueueItem) -> bool:
            captured.append({"text": item.text, "metadata": dict(item.metadata or {})})
            return True

        engine._enqueue = _capture  # type: ignore[method-assign]

        class _FakeStrategyEngine:
            def __init__(self):
                self.calls: List[str] = []

            def build_on_demand_plan_for_candidate(self, candidate: Dict[str, Any], **_: Any) -> Dict[str, Any]:
                symbol = str(candidate.get("symbol") or "").strip().upper()
                self.calls.append(symbol)
                return {
                    "ok": True,
                    "plan": {
                        "symbol": symbol,
                        "direction": "LONG",
                        "setup_name": "On-Demand Preview",
                        "entry_trigger": 101.0,
                        "stop_loss": 98.0,
                        "T1": 104.0,
                        "T2": 107.0,
                        "validity_window": "2 candles (10 minutes)",
                        "confidence_score": 62.0,
                        "market_regime": "TRENDING",
                        "reasons": ["menu_candidate"],
                        "warnings": [],
                    },
                    "explain_payload": {"symbol": symbol, "setup_name": "On-Demand Preview"},
                    "signal_eligible": False,
                    "eligibility_reasons": ["preview_only"],
                }

        fake_strategy = _FakeStrategyEngine()
        shared_state["strategy_engine"] = fake_strategy

        candles = [
            {
                "ts": i,
                "open": 100 + i * 0.01,
                "high": 101 + i * 0.01,
                "low": 99 + i * 0.01,
                "close": 100 + i * 0.01,
                "volume": 100000,
            }
            for i in range(60)
        ]
        module04_output = {
            "candidate_list": [
                {
                    "symbol": "RELIANCE",
                    "exchange": "NSE",
                    "token": "2885",
                    "tier": "T1",
                    "score": 92.0,
                    "sector": "ENERGY",
                    "snapshot": {"ltp": 100.0, "last_tick_time": now_ts()},
                    "candles_5m": candles,
                    "component_scores": {},
                },
                {
                    "symbol": "SBIN",
                    "exchange": "NSE",
                    "token": "3045",
                    "tier": "T2",
                    "score": 88.0,
                    "sector": "BANKING",
                    "snapshot": {"ltp": 100.0, "last_tick_time": now_ts()},
                    "candles_5m": candles,
                    "component_scores": {},
                },
            ]
        }
        module05_output = {
            "trade_signal_list": [],
            "trade_plan_map": {
                "RELIANCE": {
                    "symbol": "RELIANCE",
                    "direction": "LONG",
                    "setup_name": "Cached Plan",
                    "entry": 100.0,
                    "sl": 98.0,
                    "t1": 103.0,
                    "t2": 106.0,
                    "validity_window": "2 candles (10 minutes)",
                    "confidence": 70.0,
                    "market_regime": "TRENDING",
                    "reasons": ["cached_plan"],
                    "warnings": [],
                }
            },
            "explain_map": {"RELIANCE": {"symbol": "RELIANCE", "setup_name": "Cached Plan"}},
            "backup_plan_map": {},
            "backup_explain_map": {},
        }
        module03_output = {
            "market_context_state": {
                "trade_gate": "OPEN",
                "market_regime": "TRENDING",
                "macro_bias": "BULLISH",
            }
        }

        menu_id = engine.publish_from_module_outputs(module04_output, module05_output, module03_output, force_menu=True)
        captured.clear()

        plan_a = engine.parse_and_handle_command("plan A", source_chat_id=12345)
        plan_symbol = engine.parse_and_handle_command("  plan   sbin ", source_chat_id=12345)
        invalid = engine.parse_and_handle_command("plan xyz")
        invalid_text = captured[-1]["text"] if captured else ""
        reply_target_chat_ids = [str((item.get("metadata") or {}).get("reply_chat_id")) for item in captured]

        return {
            "plan_a_returns_correct_stock_plan": plan_a.get("ok") and plan_a.get("symbols") == ["RELIANCE"],
            "plan_by_stock_symbol_works": plan_symbol.get("ok") and plan_symbol.get("symbols") == ["SBIN"],
            "non_recommended_menu_stock_returns_plan": "SBIN" in fake_strategy.calls,
            "invalid_menu_request_handled_cleanly": (not invalid.get("ok")) and ("not available in the current candidate menu" in invalid_text.lower()),
            "manual_plan_reply_routes_to_source_chat": "12345" in reply_target_chat_ids,
            "menu_id_created": bool(menu_id),
        }

    @staticmethod
    def _validation_check_message_expiry_cleanup() -> Dict[str, Any]:
        base_dir = os.path.join(os.getcwd(), "runtime", "validation", "module06_expiry_cleanup")
        cfg = AlertEngineConfig(
            telegram_bot_token="TEST",
            telegram_chat_id="TEST_CHAT",
            journal_dir=os.path.join(base_dir, "journal"),
            state_dir=os.path.join(base_dir, "state"),
            deletion_registry_dir=os.path.join(base_dir, "deletion"),
            log_dir=os.path.join(base_dir, "logs"),
            auto_delete_enabled=True,
            debug=False,
        )
        engine = AlertEngine(
            config=cfg,
            snapshot_getter=lambda: {},
            market_context_getter=lambda: {},
            health_getter=lambda: {},
            shared_state={},
        )

        class _FakeTelegram:
            def __init__(self) -> None:
                self.deleted: List[Dict[str, Any]] = []

            def delete_message(self, message_id: int, chat_id: Optional[Any] = None) -> Dict[str, Any]:
                self.deleted.append({"message_id": int(message_id), "chat_id": str(chat_id)})
                return {"ok": True}

        fake_telegram = _FakeTelegram()
        engine.telegram = fake_telegram
        engine._mode_context = lambda: {
            "live_market": False,
            "offmarket_test": True,
            "mode_name": "OFFMARKET_TEST",
        }

        old_force_expiry = os.environ.get("FORCE_EXPIRY_SECONDS")
        try:
            os.environ["FORCE_EXPIRY_SECONDS"] = "2"
            forced_expiry = engine._resolve_message_expiry_time()

            expired_at = now_ts() - 1
            engine._message_registry = [
                MessageRegistryItem(
                    timestamp=now_ts(),
                    menu_id="MENU_TEST",
                    alert_type=engine.ALERT_PLAN,
                    telegram_message_id=777,
                    chat_id="12345",
                    deletable=True,
                    expiry_time=expired_at,
                    scheduled_delete_after=expired_at,
                    symbol="RELIANCE",
                )
            ]
            engine._cleanup_message_registry()
        finally:
            if old_force_expiry is None:
                os.environ.pop("FORCE_EXPIRY_SECONDS", None)
            else:
                os.environ["FORCE_EXPIRY_SECONDS"] = old_force_expiry

        forced_delta = None if forced_expiry is None else max(0.0, forced_expiry - now_ts())
        return {
            "force_expiry_seconds_applied_offmarket": forced_delta is not None and forced_delta <= 3.0,
            "expired_message_delete_called": fake_telegram.deleted == [{"message_id": 777, "chat_id": "12345"}],
            "expired_message_removed_from_registry": len(engine._message_registry) == 0,
        }

    @staticmethod
    def _validation_check_offmarket_menu_bypass() -> Dict[str, Any]:
        base_dir = os.path.join(os.getcwd(), "runtime", "validation", "module06_menu_bypass")
        cfg = AlertEngineConfig(
            telegram_bot_token="TEST",
            telegram_chat_id="TEST_CHAT",
            journal_dir=os.path.join(base_dir, "journal"),
            state_dir=os.path.join(base_dir, "state"),
            deletion_registry_dir=os.path.join(base_dir, "deletion"),
            log_dir=os.path.join(base_dir, "logs"),
            debug=False,
        )
        engine = AlertEngine(
            config=cfg,
            snapshot_getter=lambda: {},
            market_context_getter=lambda: {},
            health_getter=lambda: {},
            shared_state={},
        )

        queued: List[QueueItem] = []
        menu_seq = {"value": 0}

        def _fake_build(*_: Any, **__: Any) -> Tuple[MenuState, str]:
            menu_seq["value"] += 1
            menu_id = f"TEST_MENU_{menu_seq['value']}"
            return (
                MenuState(
                    menu_id=menu_id,
                    created_at=now_ts(),
                    menu_hash=f"HASH_{menu_seq['value']}",
                    candidates=[],
                    candidate_lookup={},
                    recommended_symbols=[],
                ),
                "TEST MENU",
            )

        engine._build_menu_state_and_text = _fake_build
        engine._enqueue = lambda item: queued.append(item) or True
        engine._should_send_menu = lambda menu_state: False

        engine._mode_context = lambda: {"live_market": False, "offmarket_test": True, "mode_name": "OFFMARKET_TEST"}
        offmarket_menu_id = engine.publish_candidate_menu(candidates=[{"symbol": "RELIANCE"}], force=False)
        offmarket_sent = len(queued) == 1 and queued[0].kind == engine.ALERT_MENU
        offmarket_latest_ok = engine._latest_menu_id == offmarket_menu_id

        queued.clear()
        engine._mode_context = lambda: {"live_market": True, "offmarket_test": False, "mode_name": "LIVE"}
        live_menu_id = engine.publish_candidate_menu(candidates=[{"symbol": "SBIN"}], force=False)
        live_suppressed = len(queued) == 0
        live_latest_ok = engine._latest_menu_id == live_menu_id

        return {
            "offmarket_menu_forces_send": offmarket_sent,
            "offmarket_menu_id_updates": bool(offmarket_menu_id) and offmarket_latest_ok,
            "live_menu_dedup_unchanged": live_suppressed,
            "live_menu_id_still_updates": bool(live_menu_id) and live_latest_ok,
        }

    @staticmethod
    def _validation_check_offmarket_menu_reset_and_fresh_ids() -> Dict[str, Any]:
        base_dir = os.path.join(os.getcwd(), "runtime", "validation", "module06_offmarket_reset")
        cfg = AlertEngineConfig(
            telegram_bot_token="TEST",
            telegram_chat_id="TEST_CHAT",
            journal_dir=os.path.join(base_dir, "journal"),
            state_dir=os.path.join(base_dir, "state"),
            deletion_registry_dir=os.path.join(base_dir, "deletion"),
            log_dir=os.path.join(base_dir, "logs"),
            debug=False,
        )
        ensure_dir(cfg.state_dir)
        stale_menu_path = os.path.join(cfg.state_dir, "menus.json")
        with open(stale_menu_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "latest_menu_id": "STALE_MENU",
                    "menus": {
                        "STALE_MENU": {
                            "menu_id": "STALE_MENU",
                            "created_at": now_ts() - 600,
                            "menu_hash": "STALE_HASH",
                            "candidates": [],
                            "candidate_lookup": {},
                            "recommended_symbols": [],
                            "trade_gate": "OPEN",
                            "market_regime": "CHOPPY",
                            "macro_bias": "NEUTRAL",
                            "warning_tag": "",
                            "source_cycle_id": "OLD",
                        }
                    },
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        shared_state: Dict[str, Any] = {
            "runtime_contract_map": {"STALE_MENU": {"menu_id": "STALE_MENU"}},
            "runtime_latest_menu_id": "STALE_MENU",
        }
        engine = AlertEngine(
            config=cfg,
            snapshot_getter=lambda: {},
            market_context_getter=lambda: {},
            health_getter=lambda: {},
            shared_state=shared_state,
        )
        engine._mode_context = lambda: {"live_market": False, "offmarket_test": True, "mode_name": "OFFMARKET_TEST"}
        engine._reset_offmarket_menu_state(clear_persisted=True)

        with open(stale_menu_path, "r", encoding="utf-8") as f:
            persisted_after_reset = json.load(f)

        menu_one = engine.publish_candidate_menu(candidates=[{"symbol": "RELIANCE"}], force=True)
        menu_two = engine.publish_candidate_menu(candidates=[{"symbol": "SBIN"}], force=True)

        return {
            "stale_menu_cleared_in_memory": engine._latest_menu_id == menu_two and "STALE_MENU" not in engine._menus,
            "stale_menu_persisted_file_overwritten": persisted_after_reset.get("latest_menu_id") is None and persisted_after_reset.get("menus") == {},
            "runtime_contracts_reset": shared_state.get("runtime_contract_map") == {} and shared_state.get("runtime_latest_menu_id") is None,
            "fresh_menu_ids_per_publish": bool(menu_one) and bool(menu_two) and menu_one != menu_two and "STALE_MENU" not in {menu_one, menu_two},
        }

    # -------------------------------------------------------------------------
    # Daily Summary
    # -------------------------------------------------------------------------

    def build_end_of_day_summary(self) -> str:
        with self._lock:
            total_messages = self.health.messages_sent + self.health.messages_failed
            avg_latency = self._estimate_avg_latency_from_journal()
            active_expired = sum(1 for s in self._active_signals.values() if s.status == "EXPIRED")
            active_completed = sum(1 for s in self._active_signals.values() if s.status == "T2_DONE")
            critical_count = self._count_journal_alert_type(self.ALERT_ERROR)
            menu_publish_count = self._count_journal_alert_type(self.ALERT_MENU)
            followups = self._count_journal_alert_type(self.ALERT_FOLLOWUP)

        lines = [
            "📘 END OF DAY SUMMARY",
            f"Menus sent: {menu_publish_count}",
            f"Plans requested/sent: {self._count_journal_alert_type(self.ALERT_PLAN)}",
            f"Follow-ups: {followups}",
            f"Messages sent/failed: {self.health.messages_sent}/{self.health.messages_failed}",
            f"Average alert latency: {avg_latency} ms",
            f"Critical alerts: {critical_count}",
            f"Active signals expired/completed: {active_expired}/{active_completed}",
            f"Deletion records logged: {self._count_journal_alert_type(self.ALERT_DELETE)}",
        ]
        return "\n".join(lines)

    def _count_journal_alert_type(self, alert_type: str) -> int:
        path = self._journal_path()
        if not os.path.exists(path):
            return 0
        c = 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        if obj.get("alert_type") == alert_type:
                            c += 1
                    except Exception:
                        continue
        except Exception:
            return 0
        return c

    def _estimate_avg_latency_from_journal(self) -> int:
        path = self._journal_path()
        vals = []
        if not os.path.exists(path):
            return 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        x = safe_int(obj.get("latency_ms"), -1)
                        if x >= 0:
                            vals.append(x)
                    except Exception:
                        continue
        except Exception:
            return 0
        if not vals:
            return 0
        return int(sum(vals) / len(vals))


# =============================================================================
# Minimal Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example shared state for local testing / Colab integration.
    bot_state = {
        "snapshots": {
            "RELIANCE": {"ltp": 2957.2, "timestamp": now_ts()},
            "INFY": {"ltp": 1654.1, "timestamp": now_ts()},
        },
        "market_context": {
            "trade_gate": "ALLOW",
            "market_regime": "TREND",
            "macro_bias": "BULLISH",
        },
        "module_health": {
            "broker": {
                "state": "RUNNING",
                "last_refresh_iso": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                "fail_count": 0,
            },
            "websocket": {
                "state": "RUNNING",
                "ws_connected": True,
                "last_tick_iso": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                "reconnect_count": 0,
            },
        },
    }

    config = AlertEngineConfig(
        telegram_bot_token="YOUR_TELEGRAM_BOT_TOKEN",
        telegram_chat_id="YOUR_CHAT_ID",
        debug=True,
    )

    alert_engine = AlertEngine(
        config=config,
        snapshot_getter=lambda: bot_state["snapshots"],
        market_context_getter=lambda: bot_state["market_context"],
        health_getter=lambda: bot_state["module_health"],
        shared_state=bot_state,
    )

    # Start workers
    alert_engine.start()

    # Module 04 example output
    module04_output = {
        "top_candidates": [
            {"symbol": "RELIANCE", "tier": "T1", "score": 92.4, "sector": "ENERGY", "regime_tag": "TREND"},
            {"symbol": "INFY", "tier": "T2", "score": 88.1, "sector": "IT", "regime_tag": "TREND"},
            {"symbol": "SBIN", "tier": "T2", "score": 84.6, "sector": "BANKING", "regime_tag": "PULLBACK"},
        ],
        "scan_cycle_id": "SCAN_001",
    }

    # Module 05 example output
    module05_output = {
        "trade_signal_list": [
            {"symbol": "RELIANCE", "direction": "LONG", "setup_name": "VWAP Bounce"},
            {"symbol": "INFY", "direction": "LONG", "setup_name": "EMA Pullback"},
        ],
        "trade_plan_map": {
            "RELIANCE": {
                "symbol": "RELIANCE",
                "direction": "LONG",
                "setup_name": "VWAP Bounce",
                "entry": 2948.0,
                "sl": 2940.0,
                "t1": 2956.0,
                "t2": 2966.0,
                "validity_window": "next 10 minutes / 2 candles",
                "validity_minutes": 10,
                "confidence": 8.4,
                "reasons": [
                    "Above VWAP with supportive volume",
                    "Market regime trend-aligned",
                    "Sector showing relative strength",
                ],
                "warnings": [
                    "Avoid chase entry if spread widens",
                ],
            },
            "INFY": {
                "symbol": "INFY",
                "direction": "LONG",
                "setup_name": "EMA Pullback",
                "entry": 1650.0,
                "sl": 1643.0,
                "t1": 1658.0,
                "t2": 1666.0,
                "validity_minutes": 10,
                "confidence": 7.9,
                "reasons": [
                    "Pullback to short EMA cluster",
                    "Trend continuation structure",
                ],
                "warnings": ["Check index stability"],
            },
        },
        "explain_map": {
            "RELIANCE": {
                "patterns": ["Bullish recovery after shallow pullback"],
                "indicators": ["VWAP reclaim", "EMA alignment", "Healthy intraday momentum"],
                "regime_fit": "Works best in trend regime with strong sector support.",
            },
            "INFY": {
                "patterns": ["EMA pullback continuation"],
                "indicators": ["Supportive RSI", "ATR room for T1/T2"],
                "regime_fit": "Fits controlled trending sessions.",
            },
        },
    }

    # Publish menu
    menu_id = alert_engine.publish_from_module_outputs(
        module04_output=module04_output,
        module05_output=module05_output,
        module03_output=bot_state["market_context"],
        force_menu=True,
    )
    print("Published menu:", menu_id)

    # Parse example command
    plan_map = alert_engine.extract_trade_plan_map(module05_output)
    explain_map = alert_engine.extract_explain_map(module05_output)
    result = alert_engine.parse_and_handle_command("A C", trade_plan_map=plan_map, explain_map=explain_map)
    print("Command result:", result)

    # Optional explain
    alert_engine.parse_and_handle_command("EXPLAIN A", trade_plan_map=plan_map, explain_map=explain_map)

    # Manual status
    alert_engine.send_status()

    # Keep alive briefly for test
    time.sleep(2)

    print("Health:", json.dumps(alert_engine.health_dict(), indent=2, ensure_ascii=False))
