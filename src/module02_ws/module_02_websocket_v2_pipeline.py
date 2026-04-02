# MODULE 02 — WebSocket v2 Market Data Pipeline (Angel One SmartAPI)
# Purpose: Reliable SmartWebSocketV2 market-data stream + normalization + snapshots + health.

from __future__ import annotations

import os
import time
import json
import threading
from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import datetime as _dt

def _is_market_hours_ist() -> bool:
    ist = _dt.timezone(_dt.timedelta(hours=5, minutes=30))
    now = _dt.datetime.now(ist)
    t = now.time()
    # NSE cash market session
    return (_dt.time(9, 15) <= t <= _dt.time(15, 30))

# Angel One SDK
# NOTE: Package name can vary by install; this is the common import for SmartAPI Python.
from SmartApi.smartWebSocketV2 import SmartWebSocketV2  # type: ignore


# ---------------------------
# Small file / config helpers
# ---------------------------

def _read_json_file(path: str, default: Any = None) -> Any:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _norm_index_name(s: Any) -> str:
    s = str(s or "").strip().upper()
    s = s.replace("&", " AND ")
    s = s.replace("-", " ")
    s = s.replace("_", " ")
    s = s.replace("/", " ")
    s = s.replace(":", " ")
    s = " ".join(s.split())

    replacements = {
        "FINANCIAL SERVICES": "FIN SERVICE",
        "FINANCIAL SERVICE": "FIN SERVICE",
        "FIN SERVICES": "FIN SERVICE",
        "PRIVATE BANK": "PVT BANK",
        "INFRASTRUCTURE": "INFRA",
        "MIDCAP SELECT": "MID SELECT",
        "SERVICES SECTOR": "SERV SECTOR",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)

    return " ".join(s.split())

def _live_index_variants(name: str) -> List[str]:
    base = _norm_index_name(name)
    out = {
        base,
        f"NSE {base}",
    }

    alias_map = {
        "NIFTY 50": {"NSE NIFTY 50", "NSE NIFTY"},
        "NIFTY BANK": {"NSE NIFTY BANK", "NSE BANKNIFTY"},
        "NIFTY FIN SERVICE": {"NSE NIFTY FIN SERVICE", "NSE FINNIFTY"},
        "NIFTY MID SELECT": {"NSE NIFTY MID SELECT", "NSE MIDCPNIFTY"},
        "NIFTY INFRA": {"NSE NIFTY INFRA", "NSE NIFTYINFRA"},
        "NIFTY PVT BANK": {"NSE NIFTY PVT BANK", "NSE NIFTYPVTBANK"},
        "NIFTY PSU BANK": {"NSE NIFTY PSU BANK", "NSE NIFTYPSUBANK"},
        "INDIA VIX": {"INDIA VIX", "NSE INDIA VIX"},
        "NIFTY IT": {"NSE NIFTY IT"},
        "NIFTY AUTO": {"NSE NIFTY AUTO"},
        "NIFTY FMCG": {"NSE NIFTY FMCG"},
        "NIFTY PHARMA": {"NSE NIFTY PHARMA"},
        "NIFTY METAL": {"NSE NIFTY METAL"},
        "NIFTY REALTY": {"NSE NIFTY REALTY"},
        "NIFTY MEDIA": {"NSE NIFTY MEDIA"},
        "NIFTY ENERGY": {"NSE NIFTY ENERGY"},
        "NIFTY MIDCAP 100": {"NSE NIFTY MIDCAP 100"},
        "NIFTY MIDCAP 150": {"NSE NIFTY MIDCAP 150"},
        "NIFTY MIDCAP 50": {"NSE NIFTY MIDCAP 50"},
    }

    out |= alias_map.get(base, set())
    return list(out)

def load_production_index_tokens(project_root: str) -> Tuple[List[str], Dict[str, str]]:
    """
    Reads:
      - config/runtime_config.json
      - mappings/index_tokens.json

    Returns:
      (production_index_tokens, live_index_to_token)
    """
    config_path = os.path.join(project_root, "config", "runtime_config.json")
    index_tokens_path = os.path.join(project_root, "mappings", "index_tokens.json")

    cfg = _read_json_file(config_path, default={}) or {}
    token_map = _read_json_file(index_tokens_path, default={}) or {}

    live_indexes = cfg.get("live_index_subscriptions", []) or []

    norm_to_entry: Dict[str, Tuple[str, str]] = {}
    for raw_key, token in token_map.items():
        if token is None:
            continue
        nk = _norm_index_name(raw_key)
        norm_to_entry[nk] = (raw_key, str(token))

    resolved: Dict[str, str] = {}
    production_tokens: List[str] = []

    for name in live_indexes:
        hit: Optional[Tuple[str, str]] = None
        for candidate in _live_index_variants(name):
            if candidate in norm_to_entry:
                hit = norm_to_entry[candidate]
                break

        if hit is None:
            continue

        _, token = hit
        resolved[str(name)] = token
        if token not in production_tokens:
            production_tokens.append(token)

    return production_tokens, resolved

def load_token_to_symbol_map(project_root: str) -> Dict[str, str]:
    path = os.path.join(project_root, "mappings", "token_to_symbol.json")
    data = _read_json_file(path, default={}) or {}
    out: Dict[str, str] = {}
    for k, v in data.items():
        if k is None or v is None:
            continue
        out[str(k)] = str(v)
    return out


# ---------------------------
# Broker context (Module 01)
# ---------------------------

@dataclass
class BrokerContext:
    authToken: str
    feedToken: str
    api_key: str
    client_code: str
    state: str = "OK"


def _getattr_any(obj: Any, names: List[str]) -> Optional[Any]:
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def load_broker_context_from_module(mod) -> BrokerContext:
    """
    Module 2 MUST import Module 1 and obtain authToken, feedToken, client_code even if
    Module 1 does not expose get_broker_context().

    This loader tries, in order:
      1) mod.get_broker_context()
      2) mod.BROKER_CONTEXT (dict or BrokerContext-like)
      3) mod.broker_context (dict or BrokerContext-like)
      4) direct attributes: authToken/auth_token/jwt, feedToken/feed_token, api_key/apiKey, client_code/clientCode
    """
    # 1) function
    if hasattr(mod, "get_broker_context") and callable(mod.get_broker_context):
        ctx = mod.get_broker_context()
        return _coerce_broker_context(ctx)

    # 2/3) common globals
    for name in ("BROKER_CONTEXT", "broker_context", "CONTEXT", "context"):
        if hasattr(mod, name):
            ctx = getattr(mod, name)
            try:
                return _coerce_broker_context(ctx)
            except Exception:
                pass

    # 4) raw attributes
    auth = _getattr_any(mod, ["authToken", "auth_token", "jwt", "AUTH_TOKEN"])
    feed = _getattr_any(mod, ["feedToken", "feed_token", "FEED_TOKEN"])
    api_key = _getattr_any(mod, ["api_key", "apiKey", "API_KEY"])
    client = _getattr_any(mod, ["client_code", "clientCode", "CLIENT_CODE"])

    missing = [k for k, v in [("authToken", auth), ("feedToken", feed), ("api_key", api_key), ("client_code", client)] if not v]
    if missing:
        raise RuntimeError(
            f"Module 01 context missing keys: {missing}. "
            f"Expose them as attributes or as BROKER_CONTEXT dict/object, or implement get_broker_context()."
        )

    state = _getattr_any(mod, ["state", "STATE"])
    if state is None:
        state = "OK"
    return BrokerContext(authToken=str(auth), feedToken=str(feed), api_key=str(api_key), client_code=str(client), state=str(state))


def _coerce_broker_context(ctx: Any) -> BrokerContext:
    if isinstance(ctx, BrokerContext):
        return ctx
    if isinstance(ctx, dict):
        # allow both camelCase and snake_case
        auth = ctx.get("authToken") or ctx.get("auth_token") or ctx.get("jwt")
        feed = ctx.get("feedToken") or ctx.get("feed_token")
        api_key = ctx.get("api_key") or ctx.get("apiKey")
        client = ctx.get("client_code") or ctx.get("clientCode")
        state = ctx.get("state", "OK")
        if not (auth and feed and api_key and client):
            raise ValueError("Incomplete broker_context dict")
        return BrokerContext(authToken=str(auth), feedToken=str(feed), api_key=str(api_key), client_code=str(client), state=str(state))

    # object with attributes
    auth = _getattr_any(ctx, ["authToken", "auth_token", "jwt"])
    feed = _getattr_any(ctx, ["feedToken", "feed_token"])
    api_key = _getattr_any(ctx, ["api_key", "apiKey"])
    client = _getattr_any(ctx, ["client_code", "clientCode"])
    state = _getattr_any(ctx, ["state"]) or "OK"
    if not (auth and feed and api_key and client):
        raise ValueError("Incomplete broker_context object")
    return BrokerContext(authToken=str(auth), feedToken=str(feed), api_key=str(api_key), client_code=str(client), state=str(state))


# ---------------------------
# Normalized tick + health
# ---------------------------

@dataclass
class NormalizedTick:
    ts: float                     # unix seconds (receive time)
    token: str
    exchangeType: Optional[int]    # Angel exchangeType int when present
    ltp: Optional[float]
    volume: Optional[float]
    event_time: Optional[str]      # exchange timestamp string when present
    symbol: Optional[str]          # from token map if available
    tier: str                      # "IDX" | "WL" | "FOCUS" | "UNKNOWN"
    raw: Dict[str, Any]


@dataclass
class WSPipelineHealth:
    ws_connected: bool = False
    state: str = "INIT"  # INIT | OK | DEGRADED | PAUSED
    last_tick_time: float = 0.0
    reconnect_count: int = 0
    subscribed_count: int = 0
    last_error: Optional[str] = None
    # tier level
    tier_last_tick: Dict[str, float] = None
    tier_tick_count: Dict[str, int] = None

    def __post_init__(self):
        if self.tier_last_tick is None:
            self.tier_last_tick = {"IDX": 0.0, "WL": 0.0, "FOCUS": 0.0}
        if self.tier_tick_count is None:
            self.tier_tick_count = {"IDX": 0, "WL": 0, "FOCUS": 0}


# ---------------------------
# WebSocket v2 Pipeline
# ---------------------------

class WebSocketV2Pipeline:
    """
    Reliable SmartWebSocketV2 connection with:
      - subscription registry (tier -> token list)
      - hot swap focus list (Tier 3) without reconnect
      - normalization + raw preservation
      - snapshot store (latest tick per token)
      - stale detection + health states
      - controlled shutdown
    """

    def __init__(
        self,
        broker_context: BrokerContext,
        token_to_symbol: Optional[Dict[str, str]] = None,
        on_tick: Optional[Callable[[NormalizedTick], None]] = None,
        on_state: Optional[Callable[[WSPipelineHealth], None]] = None,
        *,
        mode: int = 1,
        correlation_prefix: str = "W2",
        stale_after_sec: int = 8,
        pause_after_sec: int = 20,
        reconnect_backoff: Tuple[float, float, float] = (1.0, 2.0, 5.0),
        max_watchlist_tokens: int = 500,
    ):
        self.ctx = broker_context
        self.mode = mode
        self.correlation_prefix = correlation_prefix

        self.token_to_symbol = token_to_symbol or {}
        self.on_tick_cb = on_tick
        self.on_state_cb = on_state

        self.stale_after_sec = float(stale_after_sec)
        self.pause_after_sec = float(pause_after_sec)
        self.reconnect_backoff = reconnect_backoff
        self.max_watchlist_tokens = max_watchlist_tokens

        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        # registry: tier -> flat token list (public compatibility)
        self._registry: Dict[str, List[str]] = {"IDX": [], "WL": [], "FOCUS": []}
        # internal subscribe contract: tier -> [{"exchangeType": int, "tokens": [str, ...]}, ...]
        self._registry_groups: Dict[str, List[Dict[str, Any]]] = {"IDX": [], "WL": [], "FOCUS": []}
        self._registry_meta: Dict[str, Dict[str, Any]] = {
            "IDX": {"correlation_id": f"{self.correlation_prefix}-IDX", "last_subscribe": 0.0},
            "WL": {"correlation_id": f"{self.correlation_prefix}-WL", "last_subscribe": 0.0},
            "FOCUS": {"correlation_id": f"{self.correlation_prefix}-FOCUS", "last_subscribe": 0.0},
        }

        # token -> snapshot
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self.health = WSPipelineHealth()

        # websocket instance
        self._ws: Optional[SmartWebSocketV2] = None
        self._connect_thread: Optional[threading.Thread] = None
        self._reconnect_lock = threading.Lock()
        self._reconnect_in_progress = False

        # background monitor thread
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)

    @staticmethod
    def _split_symbol_identity(value: Any) -> Tuple[str, str, str]:
        raw = str(value or "").strip()
        if not raw:
            return "NSE", "", ""

        exchange = "NSE"
        symbol = raw

        if ":" in raw:
            exchange, symbol = raw.split(":", 1)
            exchange = str(exchange).strip() or "NSE"
            symbol = str(symbol).strip()

        if symbol.upper().endswith("-EQ"):
            symbol = symbol[:-3]

        snapshot_key = f"{exchange}:{symbol}" if symbol else ""
        return exchange, symbol, snapshot_key

    def _snapshot_row(
        self,
        snap: Dict[str, Any],
        *,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> Dict[str, Any]:
        row = dict(snap or {})

        base_exchange, base_symbol, base_key = self._split_symbol_identity(row.get("symbol"))
        if symbol:
            base_symbol = str(symbol).strip()
        if exchange:
            base_exchange = str(exchange).strip() or base_exchange or "NSE"

        snapshot_key = f"{base_exchange}:{base_symbol}" if base_symbol else base_key

        row["symbol"] = base_symbol or row.get("symbol")
        row["exchange"] = base_exchange or row.get("exchange") or "NSE"
        row["snapshot_key"] = snapshot_key or row.get("snapshot_key")

        tick_ts = row.get("last_tick_time", row.get("ts"))
        row["last_tick_time"] = tick_ts
        row["ts"] = tick_ts

        return row

    @property
    def snapshot_store(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            for token, snap in self._snapshots.items():
                row = self._snapshot_row(snap)
                token_key = str(token).strip()
                snapshot_key = str(row.get("snapshot_key", "")).strip()
                symbol_key = str(row.get("symbol", "")).strip()

                if token_key:
                    out[token_key] = row
                if snapshot_key:
                    out[snapshot_key] = row
                if symbol_key:
                    out[symbol_key] = row

        return out

    # ---- public API ----

    def set_index_tokens(self, tokens: List[Any]) -> None:
        groups = self._normalize_subscription_groups("IDX", tokens)
        with self._lock:
            self._registry["IDX"] = self._flatten_subscription_groups(groups)
            self._registry_groups["IDX"] = groups

    def set_watchlist_tokens(self, tokens: List[Any]) -> None:
        groups = self._normalize_subscription_groups("WL", tokens)
        toks = self._flatten_subscription_groups(groups)
        if len(toks) > self.max_watchlist_tokens:
            toks = toks[: self.max_watchlist_tokens]
            groups = self._normalize_subscription_groups("WL", toks)

        with self._lock:
            self._registry["WL"] = toks
            self._registry_groups["WL"] = groups

        if self.health.ws_connected and self._ws is not None:
            try:
                if toks:
                    self._subscribe_tier("WL", groups)
                with self._lock:
                    self.health.subscribed_count = sum(len(self._registry[t]) for t in ("IDX", "WL", "FOCUS"))
                    self._emit_state()
            except Exception as e:
                self._set_error(f"WL subscribe failed: {e}")

    def update_focus_tokens(self, tokens: List[Any]) -> None:
        """
        Hot swap Tier 3 without reconnect:
        - computes changes and sends subscribe (idempotent on server-side)
        - keeps registry updated
        """
        groups = self._normalize_subscription_groups("FOCUS", tokens)
        new_tokens = self._flatten_subscription_groups(groups)
        with self._lock:
            self._registry["FOCUS"] = new_tokens
            self._registry_groups["FOCUS"] = groups

        # If connected, resubscribe Tier 3 immediately
        if self.health.ws_connected and self._ws is not None:
            try:
                self._subscribe_tier("FOCUS", groups)
            except Exception as e:
                self._set_error(f"FOCUS hot swap subscribe failed: {e}")

    def start(self) -> None:
        with self._lock:
            self._stop_event.clear()
            self.health.state = "INIT"
            self._build_ws()

        if not self._monitor_thread.is_alive():
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()

        self._connect()

    def stop(self) -> None:
        """
        Controlled shutdown: stop monitor, close socket, notify state.
        """
        self._stop_event.set()
        with self._lock:
            try:
                if self._ws is not None:
                    # SmartWebSocketV2 exposes close_connection in many builds
                    if hasattr(self._ws, "close_connection"):
                        self._ws.close_connection()
                    elif hasattr(self._ws, "close"):
                        self._ws.close()
            except Exception:
                pass

            self.health.ws_connected = False
            self.health.state = "PAUSED"
            self._emit_state()


    def get_snapshot(self, token: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            snap = self._snapshots.get(str(token))
            return self._snapshot_row(snap) if snap else None

    def get_snapshots_bulk(self, items: List[Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}

        def _candidate_keys(item: Any) -> Tuple[List[str], Optional[str], Optional[str]]:
            keys: List[str] = []
            symbol = None
            exchange = None

            if isinstance(item, dict):
                token = item.get("token")
                symbol = str(item.get("symbol", "")).strip() or None
                exchange = str(item.get("exchange", "NSE")).strip() or "NSE"
                if token is not None:
                    keys.append(str(token))
                if symbol:
                    keys.append(str(symbol).strip().upper())
            else:
                s = str(item).strip()
                if s:
                    keys.append(s)
                    keys.append(s.upper())

            return keys, symbol, exchange

        with self._lock:
            for item in items or []:
                keys, symbol, exchange = _candidate_keys(item)
                snap = None

                for k in keys:
                    if k in self._snapshots:
                        snap = dict(self._snapshots[k])
                        break

                if not snap:
                    continue

                row = self._snapshot_row(snap, symbol=symbol, exchange=exchange)
                token_key = str(row.get("token", "")).strip()
                snapshot_key = str(row.get("snapshot_key", "")).strip()
                symbol_key = str(row.get("symbol", "")).strip()

                if token_key:
                    out[token_key] = row
                if snapshot_key:
                    out[snapshot_key] = row
                if symbol_key:
                    out[symbol_key] = row

        return out

    def get_registry(self) -> Dict[str, List[str]]:
        with self._lock:
            return {k: list(v) for k, v in self._registry.items()}

    # ---- internals ----

    def _build_ws(self) -> None:
        self._ws = SmartWebSocketV2(
            self.ctx.authToken,
            self.ctx.api_key,
            self.ctx.client_code,
            self.ctx.feedToken,
        )

        # Patch SDK internal close handler too, because some SmartAPI builds
        # internally call self._on_close with websocket-client's 3-arg signature.
        try:
            self._ws._on_close = self._on_close
        except Exception:
            pass

        # register callbacks
        self._ws.on_open = self._on_open
        self._ws.on_data = self._on_data
        self._ws.on_error = self._on_error
        self._ws.on_close = self._on_close

    def _connect(self) -> None:
        if self._ws is None:
            self._build_ws()
        try:
            if self._connect_thread is not None and self._connect_thread.is_alive():
                return

            # connect() usually blocks until close; SDK handles reconnect internally.
            self._connect_thread = threading.Thread(target=self._ws.connect, daemon=True)  # type: ignore
            self._connect_thread.start()
        except Exception as e:
            self._set_error(f"connect failed: {e}")

    def _on_open(self, *args: Any, **kwargs: Any) -> None:
         # Update connection state under lock
         with self._lock:
             self.health.ws_connected = True
             self.health.last_error = None
             self.health.state = "OK"
             self._emit_state()

             tier_groups = {
                 "IDX": [dict(g) for g in self._registry_groups.get("IDX", [])],
                 "WL": [dict(g) for g in self._registry_groups.get("WL", [])],
                 "FOCUS": [dict(g) for g in self._registry_groups.get("FOCUS", [])],
             }


         # Subscribe outside the lock to avoid deadlock
         for tier in ("IDX", "WL", "FOCUS"):
             groups = tier_groups.get(tier, [])
             if groups:
                 try:
                     self._subscribe_tier(tier, groups)
                 except Exception as e:
                     self._set_error(f"subscribe {tier} failed: {e}")

    def _on_close(self, *args: Any, **kwargs: Any) -> None:
        if self._stop_event.is_set():
            return

        with self._lock:
            self.health.ws_connected = False
            if not self._stop_event.is_set():
                self.health.state = "DEGRADED"
            self._emit_state()

    def _on_error(self, *args: Any, **kwargs: Any) -> None:
        if self._stop_event.is_set():
            return

        error = args[-1] if args else kwargs.get("error")
        self._set_error(f"ws error: {error}")

        with self._lock:
            self.health.ws_connected = False
            if not self._stop_event.is_set():
                self.health.state = "DEGRADED"
            self._emit_state()

    def _on_data(self, *args: Any, **kwargs: Any) -> None:
        """
        Normalize + snapshot + callbacks.
        Message can be dict or JSON string depending on SDK build.
        """
        message = args[-1] if args else kwargs.get("message")

        try:
            raw = self._parse_message(message)
            tick = self._normalize(raw)
            if tick is None:
                return

            with self._lock:
                now = time.time()
                self.health.last_tick_time = now
                self.health.tier_last_tick[tick.tier] = now
                self.health.tier_tick_count[tick.tier] += 1
                self.health.state = "OK"  # receiving ticks => OK

                # snapshot
                self._snapshots[tick.token] = self._snapshot_row({
                    "ts": tick.ts,
                    "last_tick_time": tick.ts,
                    "token": tick.token,
                    "symbol": tick.symbol,
                    "exchangeType": tick.exchangeType,
                    "ltp": tick.ltp,
                    "volume": tick.volume,
                    "event_time": tick.event_time,
                    "tier": tick.tier,
                })

            # deliver tick
            if self.on_tick_cb:
                self.on_tick_cb(tick)

        except Exception as e:
            self._set_error(f"on_data normalize failed: {e}")

    def _parse_message(self, message: Any) -> Dict[str, Any]:
        if isinstance(message, dict):
            return message
        if isinstance(message, (bytes, bytearray)):
            try:
                return json.loads(message.decode("utf-8", errors="ignore"))
            except Exception:
                return {"raw": message.decode("utf-8", errors="ignore")}
        if isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return {"raw": message}
        return {"raw": str(message)}

    def _normalize(self, raw: Dict[str, Any]) -> Optional[NormalizedTick]:
        """
        Extract:
          token, exchangeType, timestamp, ltp (+ other fields if present)
        Preserve raw; attach symbol using token map if available.
        """
        token = raw.get("token") or raw.get("tk") or raw.get("symbolToken") or raw.get("instrumentToken")
        if token is None:
            return None
        token = str(token)

        exchange_type = raw.get("exchangeType") or raw.get("et") or raw.get("exchange_type")
        try:
            exchange_type = int(exchange_type) if exchange_type is not None else None
        except Exception:
            exchange_type = None

        # LTP fields vary by mode/build
        ltp = raw.get("ltp")
        if ltp is None:
            ltp = raw.get("last_traded_price") or raw.get("lp") or raw.get("LTP")
        try:
            ltp = float(ltp) if ltp is not None else None
        except Exception:
            ltp = None

        vol = raw.get("volume")
        if vol is None:
            vol = raw.get("v") or raw.get("vol") or raw.get("tradedVolume")
        try:
            vol = float(vol) if vol is not None else None
        except Exception:
            vol = None

        event_time = raw.get("exchange_timestamp") or raw.get("timestamp") or raw.get("ts") or raw.get("last_trade_time")
        event_time = str(event_time) if event_time is not None else None

        symbol = self.token_to_symbol.get(token)

        tier = self._tier_for_token(token)
        now = time.time()

        # integrity filters: drop totally invalid ticks (no ltp and no vol)
        if ltp is None and vol is None:
            return None

        return NormalizedTick(
            ts=now,
            token=token,
            exchangeType=exchange_type,
            ltp=ltp,
            volume=vol,
            event_time=event_time,
            symbol=symbol,
            tier=tier,
            raw=raw,
        )

    def _tier_for_token(self, token: str) -> str:
        with self._lock:
            if token in self._registry["IDX"]:
                return "IDX"
            if token in self._registry["WL"]:
                return "WL"
            if token in self._registry["FOCUS"]:
                return "FOCUS"
        return "UNKNOWN"

    def _subscribe_tier(self, tier: str, groups: List[Dict[str, Any]]) -> None:
        if not groups or self._ws is None:
            return
        cid = self._registry_meta[tier]["correlation_id"]
        payloads = self._build_subscription_payloads(groups)

        for token_list in payloads:
            self._ws.subscribe(
                correlation_id=cid,
                mode=self.mode,
                token_list=[token_list],
            )  # type: ignore
            time.sleep(0.2)

        with self._lock:
            self._registry_meta[tier]["last_subscribe"] = time.time()
            self.health.subscribed_count = sum(len(self._registry[t]) for t in ("IDX", "WL", "FOCUS"))
            self._emit_state()

    def _schedule_reconnect(self) -> None:
        with self._lock:
            self.health.reconnect_count += 1
            self._emit_state()

        def _reconnect_worker(self) -> None:
            return

    def _monitor_loop(self) -> None:
        """
        Stale-data detection:
          - during market hours only:
              * no ticks for stale_after_sec => DEGRADED
              * no ticks for pause_after_sec => PAUSED (+ optional hard reconnect)
          - outside market hours:
              * keep state OK (do not PAUSE/DEGRADE just because there are no ticks)
        """
        while not self._stop_event.is_set():
            time.sleep(1.0)

            with self._lock:
                if not self.health.ws_connected:
                    continue

                # ✅ IMPORTANT: gate stale logic BEFORE gap checks
                if not _is_market_hours_ist():
                    if self.health.state != "OK":
                        self.health.state = "OK"
                        self._emit_state()
                    continue

                now = time.time()
                last = self.health.last_tick_time
                if last <= 0:
                    continue

                gap = now - last

                if gap >= self.pause_after_sec:
                    if self.health.state != "PAUSED":
                        self.health.state = "PAUSED"
                        self._emit_state()
                    # (optional) hard reconnect ONLY during market hours
                    # if self.health.ws_connected and not self._stop_event.is_set():
                    #     self._set_error(f"stale-feed hard reconnect (gap={gap:.1f}s)")
                    #     try:
                    #         if self._ws is not None and hasattr(self._ws, "close_connection"):
                    #             self._ws.close_connection()
                    #     except Exception:
                    #         pass
                    #     self._schedule_reconnect()

                elif gap >= self.stale_after_sec:
                    if self.health.state != "DEGRADED":
                        self.health.state = "DEGRADED"
                        self._emit_state()
                else:
                    if self.health.state != "OK":
                        self.health.state = "OK"
                        self._emit_state()

    def _emit_state(self) -> None:
        if self.on_state_cb:
            try:
                self.on_state_cb(self.health)
            except Exception:
                pass

    def _set_error(self, msg: str) -> None:
        with self._lock:
            self.health.last_error = msg
            # do not always flip state here; monitor/on_close handles
            self._emit_state()

    def _clean_tokens(self, tokens: List[Any]) -> List[str]:
        out: List[str] = []
        seen = set()
        for t in tokens or []:
            if t is None:
                continue
            s = str(t).strip()
            if not s:
                continue
            if s not in seen:
                out.append(s)
                seen.add(s)
        return out

    def _default_exchange_type_for_tier(self, tier: str) -> int:
        # Current production scope is NSE cash/index subscriptions only.
        if tier in ("IDX", "WL", "FOCUS"):
            return 1
        return 1

    def _flatten_subscription_groups(self, groups: List[Dict[str, Any]]) -> List[str]:
        out: List[str] = []
        for group in groups or []:
            out.extend(self._clean_tokens(group.get("tokens", [])))
        return self._clean_tokens(out)

    def _normalize_subscription_groups(self, tier: str, subscriptions: List[Any]) -> List[Dict[str, Any]]:
        default_exchange_type = self._default_exchange_type_for_tier(tier)

        if not subscriptions:
            return []

        groups_by_exchange: Dict[int, List[str]] = {}

        for item in subscriptions:
            if isinstance(item, dict) and "tokens" in item:
                try:
                    exchange_type = int(item.get("exchangeType", default_exchange_type))
                except Exception:
                    exchange_type = default_exchange_type
                item_tokens = self._clean_tokens(item.get("tokens", []))
            else:
                exchange_type = default_exchange_type
                item_tokens = self._clean_tokens([item])

            if not item_tokens:
                continue

            bucket = groups_by_exchange.setdefault(exchange_type, [])
            seen = set(bucket)
            for token in item_tokens:
                if token not in seen:
                    bucket.append(token)
                    seen.add(token)

        return [
            {"exchangeType": exchange_type, "tokens": tokens}
            for exchange_type, tokens in groups_by_exchange.items()
            if tokens
        ]

    def _build_subscription_payloads(self, groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []

        for group in groups or []:
            try:
                exchange_type = int(group.get("exchangeType", 1))
            except Exception:
                exchange_type = 1
            tokens = self._clean_tokens(group.get("tokens", []))
            chunks = [tokens[i:i + 50] for i in range(0, len(tokens), 50)]

            for chunk in chunks:
                if chunk:
                    payloads.append({"exchangeType": exchange_type, "tokens": chunk})

        return payloads

    @staticmethod
    def _validation_check_subscription_contract() -> Dict[str, bool]:
        pipe = WebSocketV2Pipeline.__new__(WebSocketV2Pipeline)
        pipe.max_watchlist_tokens = 500

        index_groups = WebSocketV2Pipeline._normalize_subscription_groups(
            pipe,
            "IDX",
            ["99926000", "99926009"],
        )
        watchlist_groups = WebSocketV2Pipeline._normalize_subscription_groups(
            pipe,
            "WL",
            ["2885", "11536"],
        )
        index_payloads = WebSocketV2Pipeline._build_subscription_payloads(pipe, index_groups)
        watchlist_payloads = WebSocketV2Pipeline._build_subscription_payloads(pipe, watchlist_groups)

        return {
            "idx_nse_group_payload": index_payloads == [{"exchangeType": 1, "tokens": ["99926000", "99926009"]}],
            "wl_nse_group_payload": watchlist_payloads == [{"exchangeType": 1, "tokens": ["2885", "11536"]}],
        }


# ---------------------------
# Convenience builder (imports Module 01)
# ---------------------------

def build_pipeline_from_module01(
    module01,
    *,
    token_to_symbol: Optional[Dict[str, str]] = None,
    on_tick: Optional[Callable[[NormalizedTick], None]] = None,
    on_state: Optional[Callable[[WSPipelineHealth], None]] = None,
    mode: int = 1,
    stale_after_sec: int = 8,
    pause_after_sec: int = 20,
    project_root: str = "/content/drive/MyDrive/trading_bot",
    auto_set_production_indexes: bool = True,
) -> WebSocketV2Pipeline:
    """
    Use this in Colab:
        import module_01_broker_connection as m1
        from module_02_websocket_v2_pipeline import build_pipeline_from_module01
        w2 = build_pipeline_from_module01(m1, token_to_symbol=token_map)
        w2.set_watchlist_tokens([...])
        w2.start()

    If auto_set_production_indexes=True, Module 02 will automatically:
      - read config/runtime_config.json
      - read mappings/index_tokens.json
      - resolve live_index_subscriptions
      - set them as IDX tier tokens
    """
    ctx = load_broker_context_from_module(module01)

    if token_to_symbol is None:
        token_to_symbol = load_token_to_symbol_map(project_root)

    pipeline = WebSocketV2Pipeline(
        broker_context=ctx,
        token_to_symbol=token_to_symbol,
        on_tick=on_tick,
        on_state=on_state,
        mode=mode,
        stale_after_sec=stale_after_sec,
        pause_after_sec=pause_after_sec,
    )

    if auto_set_production_indexes:
        production_index_tokens, _ = load_production_index_tokens(project_root)
        pipeline.set_index_tokens(production_index_tokens)

    return pipeline
