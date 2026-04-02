# MODULE 01 — Broker Connection (Angel One SmartAPI)
# Purpose: Single token authority for the entire trading bot (login/refresh/tokens/health + single-flight lock)

from __future__ import annotations

import json
import os
import time
import math
import random
import threading
from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, Optional, Tuple

# Optional dependency: pyotp (recommended)
try:
    import pyotp  # type: ignore
except Exception:
    pyotp = None

# SmartAPI import (Angel One)
# pip install smartapi-python (usually preinstalled by user)
try:
    from SmartApi import SmartConnect  # type: ignore
except Exception as e:
    SmartConnect = None


# -----------------------------
# States / Exceptions
# -----------------------------

STATE_OK = "OK"
STATE_DEGRADED = "DEGRADED"
STATE_PAUSE = "PAUSE"

MODE_SIGNALS_ONLY = "SIGNALS_ONLY"

CAP_EQUITY = "EQUITY"
CAP_OPTIONS = "OPTIONS"


class BrokerPause(Exception):
    """Hard stop for the bot: token/login unsafe to continue."""
    pass


class BrokerNotReady(Exception):
    """Raised when tokens are not available yet."""
    pass


# -----------------------------
# Small utilities
# -----------------------------

def _now_ts() -> int:
    return int(time.time())


def _iso(ts: Optional[int]) -> Optional[str]:
    if not ts:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return None


def _sleep_jitter(seconds: float) -> None:
    time.sleep(max(0.0, seconds) * (0.85 + 0.30 * random.random()))


def _backoff_seconds(attempt: int, base: float = 0.8, cap: float = 12.0) -> float:
    # exponential backoff with cap
    return min(cap, base * (2 ** max(0, attempt)))


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# -----------------------------
# Broker context bundle
# -----------------------------

@dataclass
class BrokerContext:
    authToken: Optional[str] = None
    refreshToken: Optional[str] = None
    feedToken: Optional[str] = None
    api_key: Optional[str] = None
    client_code: Optional[str] = None

    # observability / health
    state: str = STATE_DEGRADED
    fail_count: int = 0
    last_login: Optional[int] = None
    last_refresh: Optional[int] = None
    last_success_call: Optional[int] = None
    last_error: Optional[str] = None

    # capability flags
    mode: str = MODE_SIGNALS_ONLY
    instruments: Tuple[str, str] = (CAP_EQUITY, CAP_OPTIONS)

    # misc metadata
    timestamps: Dict[str, Optional[int]] = None
    exchanges: Tuple[str, ...] = ("NSE", "BSE", "NFO", "BFO", "MCX", "CDS")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamps"] = self.timestamps or {
            "last_login": self.last_login,
            "last_refresh": self.last_refresh,
            "last_success_call": self.last_success_call,
        }
        return d


# -----------------------------
# Instrument access contract (read-only concept)
# -----------------------------

class InstrumentStore:
    """
    Read-only lookup interface concept.

    Contract:
      - A separate worker (Module 07 / maintenance) refreshes instrument mapping files daily.
      - This module only *reads* them.
      - Default path: /content/drive/MyDrive/trading_bot/data/instruments/

    Suggested files (examples):
      - equity_tokens.json  (symbol -> token, exchange, etc.)
      - option_tokens.json  (key -> token; key like "NIFTY|2026-03-26|CE|22500")
      - instrument_master.json (full dump if you keep it)
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        _safe_mkdir(self.base_dir)
        self._cache: Dict[str, Any] = {}
        self._cache_mtime: Dict[str, float] = {}

    def _load_json_cached(self, filename: str) -> Any:
        path = os.path.join(self.base_dir, filename)
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        if filename in self._cache and self._cache_mtime.get(filename) == mtime:
            return self._cache[filename]
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self._cache[filename] = data
        self._cache_mtime[filename] = mtime
        return data

    def equity_lookup(self, symbol: str) -> Optional[Dict[str, Any]]:
        data = self._load_json_cached("equity_tokens.json")
        if not data:
            return None
        return data.get(symbol)

    def option_lookup(self, key: str) -> Optional[Dict[str, Any]]:
        data = self._load_json_cached("option_tokens.json")
        if not data:
            return None
        return data.get(key)

    def master(self) -> Any:
        return self._load_json_cached("instrument_master.json")


# -----------------------------
# Broker Connection (Single token authority)
# -----------------------------

class BrokerConnection:
    """
    Single token authority:
      - Only this module logs in / refreshes.
      - Other workers request tokens via get_context()/get_tokens().
      - Includes single-flight lock to prevent concurrent login/refresh.
      - Includes TokenException/403 auto-refresh via session expiry hook (best effort).

    Alerts:
      - Pass alert_callback(msg: str, severity: str="WARN", extra: dict|None=None)
    """

    def __init__(
        self,
        *,
        api_key: str,
        client_code: str,
        pin: str,
        totp_secret: str,
        instrument_store_dir: str = "/content/drive/MyDrive/trading_bot/data/instruments",
        state_dir: str = "/content/drive/MyDrive/trading_bot/state",
        alert_callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]] = None,
        proactive_refresh_seconds: int = 45 * 60,   # 45 minutes default
        refresh_cooldown_seconds: int = 60,         # cooldown after repeated failures
        max_fail_before_pause: int = 3,
    ):
        if SmartConnect is None:
            raise RuntimeError(
                "SmartConnect import failed. Ensure SmartApi package is installed and importable."
            )

        self.api_key = api_key
        self.client_code = client_code
        self.pin = pin
        self.totp_secret = totp_secret

        self.alert_callback = alert_callback or (lambda msg, severity="WARN", extra=None: None)

        self.proactive_refresh_seconds = int(proactive_refresh_seconds)
        self.refresh_cooldown_seconds = int(refresh_cooldown_seconds)
        self.max_fail_before_pause = int(max_fail_before_pause)

        self.state_dir = state_dir
        _safe_mkdir(self.state_dir)

        self.instrument_store = InstrumentStore(instrument_store_dir)

        # SmartAPI client
        self.smart = SmartConnect(api_key=self.api_key)

        # Single-flight lock + condition so other workers can wait for tokens
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._inflight: Optional[str] = None  # "login"|"refresh"|None

        self.ctx = BrokerContext(
            api_key=self.api_key,
            client_code=self.client_code,
            state=STATE_DEGRADED,
            fail_count=0,
            timestamps={
                "last_login": None,
                "last_refresh": None,
                "last_success_call": None,
            },
        )

        # background refresher
        self._bg_thread: Optional[threading.Thread] = None
        self._bg_stop = threading.Event()

        # session validation flags
        self._session_validated = False
        self._loaded_from_disk = False

        # Install expiry hook (best effort)
        self._install_session_expiry_hook()

        # Persisted tokens (optional warm start)
        self._token_file = os.path.join(self.state_dir, "broker_tokens.json")
        self._load_tokens_from_disk()


    # -----------------------------
    # Public API for other workers
    # -----------------------------

    def get_context(self, *, require_ready: bool = True) -> BrokerContext:
        """
        Read-only access to current broker context.
        If require_ready=True and tokens aren't ready, raises BrokerNotReady.
        """
        with self._lock:
            if require_ready and not self._has_tokens_locked():
                raise BrokerNotReady("Broker tokens not ready. Call ensure_logged_in() first.")
            return self._snapshot_locked()

    def get_tokens(self) -> Tuple[str, str, str]:
        """Return (authToken, refreshToken, feedToken) or raise BrokerNotReady."""
        c = self.get_context(require_ready=True)
        assert c.authToken and c.refreshToken and c.feedToken
        return c.authToken, c.refreshToken, c.feedToken

    def health(self) -> Dict[str, Any]:
        """Health/observability payload for heartbeat."""
        with self._lock:
            c = self._snapshot_locked()
        return {
            "state": c.state,
            "fail_count": c.fail_count,
            "last_login": c.last_login,
            "last_login_iso": _iso(c.last_login),
            "last_refresh": c.last_refresh,
            "last_refresh_iso": _iso(c.last_refresh),
            "last_success_call": c.last_success_call,
            "last_success_call_iso": _iso(c.last_success_call),
            "client_code": c.client_code,
            "mode": c.mode,
            "instruments": list(c.instruments),
            "exchanges": list(c.exchanges),
            "last_error": c.last_error,
        }

    def ensure_logged_in(self, *, force: bool = False) -> BrokerContext:
        """
        Ensure tokens exist. Single-flight guarded.
        If force=True, re-login (used rarely).
        """
        with self._lock:
            if self.ctx.state == STATE_PAUSE:
                raise BrokerPause(f"Broker in PAUSE state: {self.ctx.last_error}")

        if force:
            return self._singleflight("login", self._login_flow)

        with self._lock:
            has_tokens = self._has_tokens_locked()
            session_validated = self._session_validated
            loaded_from_disk = self._loaded_from_disk

        if has_tokens and session_validated:
            return self.get_context(require_ready=True)

        if has_tokens and not session_validated:
            if loaded_from_disk:
                with self._lock:
                    # Do not probe persisted refresh tokens with getProfile() first.
                    # Stale on-disk token state often produces SmartAPI "Invalid Token"
                    # noise before we fall back to the correct fresh login path.
                    self._clear_tokens_locked()
                    self._loaded_from_disk = False
                    self.ctx.state = STATE_DEGRADED
                    self.ctx.last_error = "Persisted broker session skipped at startup; fresh login required."
                    self._persist_tokens_locked()
            else:
                ok = self._validate_current_session()
                if ok:
                    with self._lock:
                        self._session_validated = True
                    return self.get_context(require_ready=True)

                with self._lock:
                    self._clear_tokens_locked()
                    self.ctx.state = STATE_DEGRADED
                    self.ctx.last_error = "Persisted broker session invalid; fresh login required."
                    self._persist_tokens_locked()

        return self._singleflight("login", self._login_flow)

    def get_5m_candles(
        self,
        *,
        symbol: str,
        token: str,
        exchange: str = "NSE",
        lookback: int = 80,
    ) -> list[dict]:
        """
        Fetch recent 5-minute candles from SmartAPI using IST-aligned timestamps.

        Returns:
        [
            {
                "ts": <epoch float>,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": int,
            },
            ...
        ]
        """
        self.ensure_logged_in()

        token = str(token).strip()
        exchange = str(exchange or "NSE").strip().upper()
        lookback = max(3, int(lookback))

        import datetime as dt

        IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
        now_ist = dt.datetime.now(IST)

        weekday = now_ist.weekday()  # Mon=0 ... Sun=6

        # Weekend -> use last Friday close as anchor
        if weekday == 5:   # Saturday
            session_day = now_ist.date() - dt.timedelta(days=1)
        elif weekday == 6: # Sunday
            session_day = now_ist.date() - dt.timedelta(days=2)
        else:
            session_day = now_ist.date()

        market_open = dt.datetime.combine(session_day, dt.time(9, 15), tzinfo=IST)
        market_close = dt.datetime.combine(session_day, dt.time(15, 30), tzinfo=IST)

        # Pick the request end first
        if weekday >= 5:
            end_dt = market_close
        else:
            if now_ist < market_open:
              # before today's market open -> use previous trading day's close
              prev_day = session_day - dt.timedelta(days=1)
              while prev_day.weekday() >= 5:
                  prev_day -= dt.timedelta(days=1)
              end_dt = dt.datetime.combine(prev_day, dt.time(15, 30), tzinfo=IST)

            elif now_ist > market_close:
                end_dt = market_close
            else:
                end_dt = now_ist.replace(second=0, microsecond=0)

        # Then derive from_dt from end_dt so fromdate is always earlier than todate
        from_dt = end_dt - dt.timedelta(minutes=(lookback * 5 + 10))

        # Safety clamp
        if from_dt >= end_dt:
            from_dt = end_dt - dt.timedelta(minutes=15)

        params = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": "FIVE_MINUTE",
            "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
            "todate": end_dt.strftime("%Y-%m-%d %H:%M"),
        }

        def _do():
            return self.smart.getCandleData(params)

        res = self.safe_call(_do, purpose="getCandleData", max_attempts=2)

        if not isinstance(res, dict):
            return []

        if res.get("status") is False or res.get("success") is False:
            return []

        data = res.get("data") or []
        if not isinstance(data, list):
            return []

        out = []
        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue

            ts_raw = str(row[0]).strip()
            ts_val = 0.0

            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
                try:
                    if fmt == "%Y-%m-%dT%H:%M:%S%z":
                        ts_val = dt.datetime.strptime(ts_raw, fmt).timestamp()
                    else:
                        parsed = dt.datetime.strptime(ts_raw[:19], fmt)
                        parsed = parsed.replace(tzinfo=IST)
                        ts_val = parsed.timestamp()
                    break
                except Exception:
                    continue

            try:
                volume_val = int(float(row[5]))
            except Exception:
                volume_val = 0

            try:
                out.append({
                    "ts": ts_val,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": volume_val,
                })
            except Exception:
                continue

        return out

    def _normalize_gainers_losers_rows(self, rows: Any, *, source: str) -> list[dict]:
        out: list[dict] = []
        if not isinstance(rows, list):
            return out

        for row in rows:
            if not isinstance(row, dict):
                continue

            symbol = str(
                row.get("tradingsymbol")
                or row.get("tradingSymbol")
                or row.get("symbol")
                or row.get("name")
                or ""
            ).strip().upper()

            if not symbol:
                continue

            token = str(
                row.get("symboltoken")
                or row.get("symbolToken")
                or row.get("token")
                or ""
            ).strip()

            item = {
                "symbol": symbol,
                "token": token,
                "exchange": str(row.get("exchange") or "NSE").strip().upper(),
                "source": source,
            }

            for k in (
                "ltp",
                "close",
                "open",
                "high",
                "low",
                "netChange",
                "percentChange",
                "avgPrice",
                "tradeVolume",
            ):
                if k in row:
                    item[k] = row.get(k)

            out.append(item)

        return out

    def get_daily_movers(self, *, datatype: str = "PercPriceGainers", count: int = 50) -> list[dict]:
        self.ensure_logged_in()

        params = {
            "datatype": str(datatype).strip(),
            "expirytype": "NEAR",
        }

        def _do():
            if hasattr(self.smart, "gainersLosers"):
                return self.smart.gainersLosers(params)
            raise AttributeError("SmartConnect.gainersLosers not available in installed SDK")

        res = self.safe_call(_do, purpose="gainersLosers", max_attempts=2)

        if not isinstance(res, dict):
            return []

        data = res.get("data")

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("gainers") or data.get("topGainers") or []
        else:
            rows = []
        out = self._normalize_gainers_losers_rows(rows, source="movers")
        return out[: max(1, int(count))]


    def get_daily_losers(self, *, datatype: str = "PercPriceLosers", count: int = 50) -> list[dict]:
        self.ensure_logged_in()

        params = {
            "datatype": str(datatype).strip(),
            "expirytype": "NEAR",
        }

        def _do():
            if hasattr(self.smart, "gainersLosers"):
                return self.smart.gainersLosers(params)
            raise AttributeError("SmartConnect.gainersLosers not available in installed SDK")

        res = self.safe_call(_do, purpose="gainersLosers", max_attempts=2)

        if not isinstance(res, dict):
            return []

        data = res.get("data")

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("losers") or data.get("topLosers") or []
        else:
            rows = []
        out = self._normalize_gainers_losers_rows(rows, source="losers")
        return out[: max(1, int(count))]     

    def refresh(self, *, force: bool = False) -> BrokerContext:
        """
        Refresh tokens using refreshToken. Single-flight guarded.
        If force=True, refresh even if recently refreshed.
        """
        with self._lock:
            if self.ctx.state == STATE_PAUSE:
                raise BrokerPause(f"Broker in PAUSE state: {self.ctx.last_error}")

        return self._singleflight("refresh", lambda: self._refresh_flow(force=force))

    def start_background_refresher(self) -> None:
        """
        Starts a lightweight background thread that proactively refreshes tokens.
        Safe for Colab. Stop with stop_background_refresher().
        """
        if self._bg_thread and self._bg_thread.is_alive():
            return

        self._bg_stop.clear()
        t = threading.Thread(target=self._bg_loop, name="broker_refresher", daemon=True)
        self._bg_thread = t
        t.start()

    def stop_background_refresher(self) -> None:
        self._bg_stop.set()

    # -----------------------------
    # Shared request policy (retry/backoff + cooldown)
    # -----------------------------

    def safe_call(
        self,
        fn: Callable[[], Any],
        *,
        purpose: str = "smartapi_call",
        max_attempts: int = 3,
        retry_on: Tuple[type, ...] = (Exception,),
    ) -> Any:
        """
        Standard retry/backoff rules.
        - If response indicates auth issues (403/TokenException), attempt refresh then retry.
        """
        last_exc = None
        auth_purposes = {"generateSession", "generateToken", "getProfile"}

        for attempt in range(max_attempts):
            try:
                out = fn()
                with self._lock:
                    self.ctx.last_success_call = _now_ts()
                    self.ctx.timestamps["last_success_call"] = self.ctx.last_success_call
                    if self.ctx.state != STATE_PAUSE:
                        self.ctx.state = STATE_OK
                    self.ctx.last_error = None
                    self._persist_tokens_locked()  # keep disk updated
                return out
            except retry_on as e:
                last_exc = e
                msg = str(e).lower()

                # Do not recursively refresh during auth flows themselves.
                if purpose not in auth_purposes and (("token" in msg) or ("403" in msg) or ("unauthorized" in msg)):
                    try:
                        self.refresh(force=True)
                    except Exception as re:
                        # if refresh itself fails, break and handle below
                        last_exc = re
                        break

                _sleep_jitter(_backoff_seconds(attempt))
                continue

        # failure accounting
        self._on_failure(f"{purpose} failed: {last_exc}")
        raise last_exc  # type: ignore

    # -----------------------------
    # Internal: single-flight coordination
    # -----------------------------

    def _singleflight(self, kind: str, work: Callable[[], BrokerContext]) -> BrokerContext:
        """
        Ensures only one login/refresh at a time.
        Others wait and then consume updated tokens.
        """
        with self._lock:
            while self._inflight and self._inflight != kind:
                # wait for other inflight to complete
                self._cond.wait(timeout=10)

            if self._inflight == kind:
                # someone else is doing same work; wait
                self._cond.wait(timeout=20)
                if self._has_tokens_locked():
                    return self._snapshot_locked()
                # fallthrough if still no tokens

            self._inflight = kind

        try:
            ctx = work()
            return ctx
        finally:
            with self._lock:
                self._inflight = None
                self._cond.notify_all()

    # -----------------------------
    # Internal: login/refresh flows
    # -----------------------------

    def _get_totp(self) -> str:
        if not self.totp_secret:
            raise BrokerPause("Missing totp_secret.")
        if pyotp is None:
            raise BrokerPause("pyotp not installed. Install with: pip install pyotp")
        # Time guard: generate a current TOTP and avoid spamming re-logins
        totp = pyotp.TOTP(self.totp_secret).now()
        return str(totp)

    def _validate_current_session(self) -> bool:
        with self._lock:
            rtk = self.ctx.refreshToken

        if not rtk:
            return False

        try:
            res = self.smart.getProfile(rtk)
        except Exception:
            return False

        if not isinstance(res, dict):
            return False

        data = res.get("data")
        if isinstance(data, dict):
            return True

        if res.get("status") is False or res.get("success") is False:
            return False

        return bool(data)

    def _clear_tokens_locked(self) -> None:
        self.ctx.authToken = None
        self.ctx.refreshToken = None
        self.ctx.feedToken = None
        self._session_validated = False

    def _login_flow(self) -> BrokerContext:
        """
        Login using generateSession(client_code, pin, totp).
        Failure policy: if OTP/time issue -> PAUSE + alert; do not spam.
        """
        with self._lock:
            if self.ctx.state == STATE_PAUSE:
                raise BrokerPause(f"Broker in PAUSE state: {self.ctx.last_error}")

        try:
            totp = self._get_totp()
        except Exception as e:
            self._pause(f"TOTP generation failed: {e}", severity="CRITICAL")
            raise BrokerPause(str(e))

        def do_login():
            return self.smart.generateSession(self.client_code, self.pin, totp)

        try:
            data = self.safe_call(do_login, purpose="generateSession", max_attempts=2)
        except Exception as e:
            # If likely OTP/time, pause
            em = str(e).lower()
            if ("otp" in em) or ("totp" in em) or ("time" in em) or ("pin" in em):
                self._pause(f"Login failed (OTP/time/pin): {e}", severity="CRITICAL")
                raise BrokerPause(str(e))
            self._on_failure(f"Login failed: {e}")
            raise

        if not isinstance(data, dict):
            self._pause(f"Login response invalid type: {type(data)}", severity="CRITICAL")
            raise BrokerPause("Login response invalid type.")

        # SDK returns jwtToken/refreshToken/feedToken typically
        data_block = data.get("data") if isinstance(data.get("data"), dict) else {}
        jwt = data_block.get("jwtToken") or data.get("jwtToken")
        rtk = data_block.get("refreshToken") or data.get("refreshToken")
        ftk = data_block.get("feedToken") or data.get("feedToken")

        # official SDK pattern: feed token can also be pulled from smart client after login
        if not ftk:
            try:
                ftk = self.smart.getfeedToken()
            except Exception:
                ftk = None

        if not (jwt and rtk and ftk):
            self._pause(f"Login response missing tokens: {data}", severity="CRITICAL")
            raise BrokerPause("Login response missing tokens.")

        with self._lock:
            self.ctx.authToken = jwt
            self.ctx.refreshToken = rtk
            self.ctx.feedToken = ftk
            self.ctx.last_login = _now_ts()
            self.ctx.timestamps["last_login"] = self.ctx.last_login
            self.ctx.state = STATE_OK
            self.ctx.last_error = None
            self.ctx.fail_count = 0
            self._session_validated = True
            self._loaded_from_disk = False
            self._persist_tokens_locked()

        # Best-effort profile fetch (validates refreshToken)
        try:
            self.safe_call(lambda: self.smart.getProfile(rtk), purpose="getProfile", max_attempts=2)
        except Exception:
            # do not fail login for profile issues
            pass

        return self.get_context(require_ready=True)

    def _refresh_flow(self, *, force: bool = False) -> BrokerContext:
        """
        Refresh using generateToken(refreshToken).
        - Proactively refresh schedule can call this.
        - After refresh: ensure feedToken validity for websocket usage.
        Failure policy: if refresh fails repeatedly -> fresh login fallback, then PAUSE if login also fails.
        """
        with self._lock:
            if not self.ctx.refreshToken:
                # no refresh token: must login
                return self._login_flow()

            # if not force, skip if recently refreshed
            if not force and self.ctx.last_refresh:
                age = _now_ts() - self.ctx.last_refresh
                if age < max(60, self.proactive_refresh_seconds // 3):
                    return self._snapshot_locked()

            rtk = self.ctx.refreshToken

        def do_refresh():
            return self.smart.generateToken(rtk)

        try:
            data = self.safe_call(do_refresh, purpose="generateToken", max_attempts=2)
        except Exception:
            with self._lock:
                self._clear_tokens_locked()
                self.ctx.last_error = "Refresh token invalid or refresh failed; falling back to fresh login."
                self._persist_tokens_locked()
            return self._login_flow()

        if not isinstance(data, dict):
            with self._lock:
                self._clear_tokens_locked()
                self.ctx.last_error = f"Refresh response invalid type: {type(data)}; falling back to fresh login."
                self._persist_tokens_locked()
            return self._login_flow()

        if data.get("status") is False or data.get("success") is False:
            with self._lock:
                self._clear_tokens_locked()
                self.ctx.last_error = f"Refresh rejected by broker: {data}"
                self._persist_tokens_locked()
            return self._login_flow()

        data_block = data.get("data")
        if not isinstance(data_block, dict):
            with self._lock:
                self._clear_tokens_locked()
                self.ctx.last_error = f"Refresh response missing data block: {data}"
                self._persist_tokens_locked()
            return self._login_flow()

        # Some SDK versions return new auth token; official SDK also updates feed token.
        new_jwt = data_block.get("jwtToken") or data.get("jwtToken") or None
        new_rtk = data_block.get("refreshToken") or data.get("refreshToken") or None
        new_feed = data_block.get("feedToken") or data.get("feedToken") or None

        # Ensure feedToken validity for websocket usage
        if not new_feed:
            try:
                # getfeedToken exists in SmartConnect
                new_feed = self.smart.getfeedToken()
            except Exception:
                new_feed = None

        if not (new_jwt and new_feed):
            with self._lock:
                self._clear_tokens_locked()
                self.ctx.last_error = f"Refresh response missing tokens: {data}"
                self._persist_tokens_locked()
            return self._login_flow()

        with self._lock:
            self.ctx.authToken = new_jwt
            if new_rtk:
                self.ctx.refreshToken = new_rtk
            if new_feed:
                self.ctx.feedToken = new_feed
            self.ctx.last_refresh = _now_ts()
            self.ctx.timestamps["last_refresh"] = self.ctx.last_refresh
            if self.ctx.state != STATE_PAUSE:
                self.ctx.state = STATE_OK
            self.ctx.last_error = None
            self.ctx.fail_count = 0
            self._session_validated = True
            self._loaded_from_disk = False
            self._persist_tokens_locked()

        return self.get_context(require_ready=True)

    # -----------------------------
    # Expiry hook (auto-handle TokenException / 403)
    # -----------------------------

    def _install_session_expiry_hook(self) -> None:
        """
        setSessionExpiryHook is supported by some SmartAPI SDK builds.
        We register a hook that triggers refresh single-flight.
        """
        try:
            # signature varies; we keep it simple
            def _hook():
                try:
                    self.refresh(force=True)
                except Exception as e:
                    self._on_failure(f"Session expiry hook refresh failed: {e}")

            self.smart.setSessionExpiryHook(_hook)
        except Exception:
            # not supported; ignore
            pass

    # -----------------------------
    # Background proactive refresh loop
    # -----------------------------

    def _bg_loop(self) -> None:
        # Wait until at least one login is done; but don't block forever
        # If no tokens, it will just sleep and retry.
        while not self._bg_stop.is_set():
            try:
                with self._lock:
                    paused = (self.ctx.state == STATE_PAUSE)
                    has = self._has_tokens_locked()
                if paused:
                    _sleep_jitter(10)
                    continue
                if not has:
                    _sleep_jitter(10)
                    continue

                # Proactive refresh
                self.refresh(force=False)
            except BrokerPause:
                # Already moved to PAUSE; loop will idle
                pass
            except Exception:
                # keep loop alive
                pass

            # main interval
            for _ in range(max(1, int(self.proactive_refresh_seconds // 5))):
                if self._bg_stop.is_set():
                    break
                _sleep_jitter(5)

    # -----------------------------
    # Persistence / snapshots
    # -----------------------------

    def _snapshot_locked(self) -> BrokerContext:
        # return a shallow copy (safe to share)
        c = BrokerContext(**self.ctx.to_dict())
        return c

    def _has_tokens_locked(self) -> bool:
        return bool(self.ctx.authToken and self.ctx.refreshToken and self.ctx.feedToken)

    def _persist_tokens_locked(self) -> None:
        # persist minimal + health fields; ok if disk write fails
        payload = {
            "authToken": self.ctx.authToken,
            "refreshToken": self.ctx.refreshToken,
            "feedToken": self.ctx.feedToken,
            "client_code": self.ctx.client_code,
            "last_login": self.ctx.last_login,
            "last_refresh": self.ctx.last_refresh,
            "last_success_call": self.ctx.last_success_call,
            "fail_count": self.ctx.fail_count,
            "state": self.ctx.state,
        }
        try:
            with open(self._token_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _load_tokens_from_disk(self) -> None:
        if not os.path.exists(self._token_file):
            return
        try:
            with open(self._token_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            with self._lock:
                self.ctx.authToken = payload.get("authToken")
                self.ctx.refreshToken = payload.get("refreshToken")
                self.ctx.feedToken = payload.get("feedToken")
                self.ctx.client_code = payload.get("client_code") or self.client_code
                self.ctx.last_login = payload.get("last_login")
                self.ctx.last_refresh = payload.get("last_refresh")
                self.ctx.last_success_call = payload.get("last_success_call")
                self.ctx.fail_count = int(payload.get("fail_count") or 0)
                self.ctx.state = payload.get("state") or (STATE_OK if self._has_tokens_locked() else STATE_DEGRADED)
                self.ctx.timestamps = {
                    "last_login": self.ctx.last_login,
                    "last_refresh": self.ctx.last_refresh,
                    "last_success_call": self.ctx.last_success_call,
                }
                self._loaded_from_disk = self._has_tokens_locked()
                self._session_validated = False
        except Exception:
            return

    @staticmethod
    def _validation_check_skip_stale_disk_session() -> Dict[str, Any]:
        broker = BrokerConnection.__new__(BrokerConnection)
        broker._lock = threading.Lock()
        broker.ctx = BrokerContext(
            authToken="STALE_AUTH",
            refreshToken="STALE_REFRESH",
            feedToken="STALE_FEED",
            api_key="KEY",
            client_code="CLIENT",
            state=STATE_DEGRADED,
            timestamps={},
        )
        broker._session_validated = False
        broker._loaded_from_disk = True

        calls = {"validate": 0, "login": 0}

        def _validate() -> bool:
            calls["validate"] += 1
            return False

        def _login() -> BrokerContext:
            calls["login"] += 1
            with broker._lock:
                broker.ctx.authToken = "FRESH_AUTH"
                broker.ctx.refreshToken = "FRESH_REFRESH"
                broker.ctx.feedToken = "FRESH_FEED"
                broker._session_validated = True
                broker._loaded_from_disk = False
                return broker._snapshot_locked()

        broker._validate_current_session = _validate
        broker._login_flow = _login
        broker._singleflight = lambda kind, work: work()
        broker._persist_tokens_locked = lambda: None

        ctx = BrokerConnection.ensure_logged_in(broker)
        return {
            "stale_disk_session_skips_profile_validation": calls["validate"] == 0,
            "fresh_login_triggered": calls["login"] == 1,
            "tokens_ready_after_login": bool(
                ctx.authToken and ctx.refreshToken and ctx.feedToken
            ),
        }

    # -----------------------------
    # Failure policy + pause
    # -----------------------------

    def _on_failure(self, error_msg: str) -> None:
        with self._lock:
            self.ctx.fail_count += 1
            self.ctx.last_error = error_msg
            if self.ctx.state != STATE_PAUSE:
                self.ctx.state = STATE_DEGRADED
            self._persist_tokens_locked()

        severity = "WARN"
        if self.ctx.fail_count >= self.max_fail_before_pause:
            severity = "CRITICAL"

        self.alert_callback(
            error_msg,
            severity,
            {
                "state": self.ctx.state,
                "fail_count": self.ctx.fail_count,
                "client_code": self.client_code,
            },
        )

    def _pause(self, reason: str, *, severity: str = "CRITICAL") -> None:
        with self._lock:
            self.ctx.state = STATE_PAUSE
            self.ctx.last_error = reason
            self._persist_tokens_locked()

        self.alert_callback(
            f"PAUSE: {reason}",
            severity,
            {
                "state": self.ctx.state,
                "fail_count": self.ctx.fail_count,
                "client_code": self.client_code,
            },
        )


# -----------------------------
# Convenience factory for Colab secrets
# -----------------------------

def build_broker_from_colab_secrets(
    *,
    alert_callback: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]] = None,
    proactive_refresh_seconds: int = 45 * 60,
) -> BrokerConnection:
    """
    Expected secrets (Colab Secrets or env vars):
      - ANGEL_API_KEY
      - ANGEL_CLIENT_ID
      - ANGEL_PIN
      - ANGEL_TOTP_SECRET
    """
    # Load from environment first
    api_key = os.environ.get("ANGEL_API_KEY")
    client_code = os.environ.get("ANGEL_CLIENT_ID")
    pin = os.environ.get("ANGEL_PIN")
    totp_secret = os.environ.get("ANGEL_TOTP_SECRET")

    # If running in Colab, try userdata
    try:
        from google.colab import userdata  # type: ignore
        api_key = api_key or userdata.get("ANGEL_API_KEY")
        client_code = client_code or userdata.get("ANGEL_CLIENT_ID")
        pin = pin or userdata.get("ANGEL_PIN")
        totp_secret = totp_secret or userdata.get("ANGEL_TOTP_SECRET")
    except Exception:
        pass

    missing = [
        k for k, v in [
            ("ANGEL_API_KEY", api_key),
            ("ANGEL_CLIENT_ID", client_code),
            ("ANGEL_PIN", pin),
            ("ANGEL_TOTP_SECRET", totp_secret),
        ]
        if not v
    ]
    if missing:
        raise RuntimeError(f"Missing secrets: {missing}")

    bc = BrokerConnection(
        api_key=str(api_key),
        client_code=str(client_code),
        pin=str(pin),
        totp_secret=str(totp_secret),
        alert_callback=alert_callback,
        proactive_refresh_seconds=proactive_refresh_seconds,
    )
    return bc
