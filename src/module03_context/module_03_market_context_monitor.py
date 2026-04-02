# MODULE 03 — Market Context Monitor
# Purpose: Top-down market intelligence (Indexes + India VIX + Event/News Risk)
# Notes:
# - Does NOT generate trades.
# - Consumes snapshots from Module 02 (WebSocket v2 pipeline).
# - Produces market_context_state + sector_scoreboard + symbol_risk_map for Modules 04/05/06.

from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Callable, Dict, List, Optional, Tuple


# =========================
# Data contracts (outputs)
# =========================

@dataclass
class MarketContextState:
    macro_regime: str = "RANGING"         # TRENDING / RANGING / HIGH_VOL / LOW_VOL
    macro_bias: str = "NEUTRAL"           # BULLISH / BEARISH / NEUTRAL
    volatility_state: str = "NORMAL"      # LOW / NORMAL / HIGH
    divergence_flag: str = "NONE"         # NONE / MILD / HIGH
    trade_gate: str = "ALLOW"             # ALLOW / CAUTION / PAUSE
    updated_at: float = 0.0
    reason_tags: List[str] = field(default_factory=list)


@dataclass
class SectorScoreRow:
    sector_name: str
    strength_score: float
    volatility_score: float
    trend_score: float
    confidence_level: str                 # LOW / MEDIUM / HIGH
    reason_tags: List[str]


@dataclass
class SectorScoreboard:
    sector_rankings: List[str] = field(default_factory=list)
    rows: List[SectorScoreRow] = field(default_factory=list)
    updated_at: float = 0.0


@dataclass
class SymbolRiskRow:
    symbol: str
    risk_level: str                       # NORMAL / CAUTION / AVOID
    reason: str
    event_type: str = ""
    impact_level: str = ""
    event_date: str = ""


# =========================
# Snapshot input contract
# =========================
# Module 02 should expose a snapshot getter. We accept either:
#   - a callable: get_snapshot() -> dict
#   - or an object with: get_snapshot() method returning dict
#
# Expected snapshot shape (flexible):
# snapshot = {
#   "ts": 1234567890.0,
#   "ltp": { "NIFTY 50": 22500.5, "BANKNIFTY": 48210.0, ... } OR { token:int -> ltp:float }
#   "ohlc": { "NIFTY 50": {"o":..., "h":..., "l":..., "c":...}, ... }  (optional)
#   "volume": { "NIFTY 50": 12345, ... } (optional)
# }
#
# This module also supports token-based maps via taxonomy (token -> name).


# =========================
# Helpers
# =========================

_FALLBACK_MACRO_NAMES = [
    "NIFTY 50",
    "Nifty Bank",
    "Nifty Financial Services",
    "Nifty Midcap Select",
]

_FALLBACK_MICRO_NAMES = {
    "Nifty IT",
    "Nifty Auto",
    "Nifty FMCG",
    "Nifty Pharma",
    "Nifty Metal",
    "Nifty Realty",
    "Nifty Media",
    "Nifty Energy",
    "Nifty Infrastructure",
    "Nifty PSU Bank",
    "Nifty Private Bank",
}

_FALLBACK_OTHER_NAMES = {
    "Nifty 100",
    "Nifty Next 50",
    "Nifty 200",
    "Nifty 500",
    "Nifty Midcap 50",
    "Nifty Midcap 100",
    "Nifty Midcap 150",
}

def _now() -> float:
    return time.time()


def _safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _zscore(value: float, mean: float, std: float) -> float:
    if std <= 1e-9:
        return 0.0
    return (value - mean) / std


def _trend_label(trend_score: float, vol_state: str) -> str:
    # Simple mapping for macro_regime decision
    # trend_score ~ abs return over window, normalized
    if vol_state == "HIGH":
        return "HIGH_VOL"
    if vol_state == "LOW":
        # In low vol, if trend exists -> TRENDING else LOW_VOL/RANGING
        return "TRENDING" if trend_score >= 0.55 else "LOW_VOL"
    return "TRENDING" if trend_score >= 0.6 else "RANGING"


def _confidence_label(conf: float) -> str:
    if conf >= 0.75:
        return "HIGH"
    if conf >= 0.5:
        return "MEDIUM"
    return "LOW"


def _days_to_event(epoch_now: float, event_date_ymd: str) -> Optional[int]:
    # event_date_ymd: "YYYY-MM-DD"
    try:
        y, m, d = event_date_ymd.split("-")
        y, m, d = int(y), int(m), int(d)
        # naive local-date difference using epoch midnight approximation
        # good enough for gating (no timezone precision needed)
        t_now = time.localtime(epoch_now)
        # compute "today" date tuple
        today = (t_now.tm_year, t_now.tm_mon, t_now.tm_mday)
        # convert to ordinal-like count via time.mktime at midnight
        today_mid = time.mktime((today[0], today[1], today[2], 0, 0, 0, 0, 0, -1))
        evt_mid = time.mktime((y, m, d, 0, 0, 0, 0, 0, -1))
        return int(round((evt_mid - today_mid) / 86400.0))
    except Exception:
        return None


def _infer_pillar(name: str) -> str:
    if name in _FALLBACK_MACRO_NAMES or name == "INDIA VIX":
        return "macro"
    if name in _FALLBACK_MICRO_NAMES:
        return "micro"
    if name in _FALLBACK_OTHER_NAMES:
        return "other"
    return "other"


def _infer_sector(name: str) -> str:
    mapping = {
        "Nifty IT": "IT",
        "Nifty Auto": "AUTO",
        "Nifty FMCG": "FMCG",
        "Nifty Pharma": "PHARMA",
        "Nifty Metal": "METAL",
        "Nifty Realty": "REALTY",
        "Nifty Media": "MEDIA",
        "Nifty Energy": "ENERGY",
        "Nifty Infrastructure": "INFRA",
        "Nifty PSU Bank": "PSU_BANK",
        "Nifty Private Bank": "PVT_BANK",
        "NIFTY 50": "BROAD_MARKET",
        "Nifty Bank": "BANKS",
        "Nifty Financial Services": "FINANCIALS",
        "Nifty Midcap Select": "MIDCAPS",
        "Nifty Midcap 100": "MIDCAPS",
        "Nifty Midcap 150": "MIDCAPS",
        "Nifty Midcap 50": "MIDCAPS",
        "Nifty 100": "BREADTH",
        "Nifty Next 50": "BREADTH",
        "Nifty 200": "BREADTH",
        "Nifty 500": "BREADTH",
        "INDIA VIX": "VOLATILITY",
    }
    return mapping.get(name, "UNKNOWN")


# =========================
# Core Monitor
# =========================

class MarketContextMonitor:
    """
    Module 03 — Market Context Monitor

    Features:
    - 3A: Index & Sector Context Monitor
      - Macro pillar monitor (4 macro indexes)
      - Micro pillar monitors (top sector indexes)
      - Remaining sector opportunity scanner (other indexes)
      - Divergence detection + Trade gate + Volatility state
    - 3B: Event/News Risk Monitor (JSON event calendar maintained by Maintenance Worker)

    Threading model:
    - One coordinator thread (runs schedules)
    - Multiple worker loops (macro, micro groups, opportunity scanner, event risk)
    """

    def __init__(
        self,
        snapshot_source: Any,
        taxonomy_path: str,
        event_calendar_path: str,
        *,
        macro_interval_s: int = 90,          # 1–2 minutes
        micro_interval_s: int = 240,         # 3–5 minutes
        opp_interval_s: int = 420,           # 5–10 minutes
        event_interval_s: int = 60,          # event risk refresh
        window_s: int = 15 * 60,             # rolling window for returns/vol
        max_points: int = 240,               # cap history per symbol
        india_vix_name: str = "INDIA VIX",
        on_update: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.snapshot_source = snapshot_source
        self.taxonomy_path = taxonomy_path
        self.event_calendar_path = event_calendar_path
        self.india_vix_name = india_vix_name

        self.macro_interval_s = int(macro_interval_s)
        self.micro_interval_s = int(micro_interval_s)
        self.opp_interval_s = int(opp_interval_s)
        self.event_interval_s = int(event_interval_s)

        self.window_s = int(window_s)
        self.max_points = int(max_points)

        self.on_update = on_update

        self._stop_event = threading.Event()
        self._lock = threading.RLock()

        # Taxonomy
        # self.taxonomy = {
        #   "indexes": [{"name":..., "token":..., "sector":..., "pillar":"macro/micro/other", "priority":...}, ...],
        #   "macro": [...names...],
        #   "micro": [...names...],
        #   "other": [...names...],
        #   "token_to_name": {token: name},
        #   "name_to_sector": {name: sector},
        # }
        self.taxonomy: Dict[str, Any] = {}

        # History buffers: name -> list[(ts, price, volume?)]
        self._hist: Dict[str, List[Tuple[float, float, float]]] = {}

        # Output state
        self.market_context_state = MarketContextState()
        self.sector_scoreboard = SectorScoreboard()
        self.symbol_risk_map: Dict[str, SymbolRiskRow] = {}

        # last run timestamps
        self._last_macro = 0.0
        self._last_micro = 0.0
        self._last_opp = 0.0
        self._last_event = 0.0

        # worker thread
        self._thread: Optional[threading.Thread] = None

        # load taxonomy once at init (and allow refresh method)
        self.reload_taxonomy()

    # --------- Public API ---------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="MKT_CTX_MONITOR", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=3.0)

    def reload_taxonomy(self) -> None:
        with self._lock:
            self.taxonomy = self._load_taxonomy(self.taxonomy_path)

    def get_outputs(self) -> Dict[str, Any]:
        with self._lock:
           return {
               "market_context_state": asdict(self.market_context_state),
               "sector_scoreboard": {
                   "sector_rankings": list(self.sector_scoreboard.sector_rankings),
                   "rows": [asdict(r) for r in self.sector_scoreboard.rows],
                   "updated_at": self.sector_scoreboard.updated_at,
               },
               "symbol_risk_map": {k: asdict(v) for k, v in self.symbol_risk_map.items()},
           }

    def get_state(self):
        return self.get_outputs()

    # --------- Internals ---------

    def _get_snapshot(self) -> Dict[str, Any]:
        src = self.snapshot_source  # expected: WebSocketV2Pipeline or compatible source

        # direct callable returning full snapshot
        if callable(src):
            try:
                return src() or {}
            except Exception:
                return {}

        # preferred bulk path
        if hasattr(src, "get_snapshots_bulk") and callable(getattr(src, "get_snapshots_bulk")):
            all_subscribed_tokens = []

            if hasattr(src, "get_registry") and callable(getattr(src, "get_registry")):
                try:
                    registry = src.get_registry() or {}
                    for tier_tokens in registry.values():
                        if tier_tokens:
                            all_subscribed_tokens.extend(tier_tokens)
                except Exception:
                    pass

            # fallback: use taxonomy tokens if registry is empty / unavailable
            if not all_subscribed_tokens:
                try:
                    all_subscribed_tokens = list(self.taxonomy.get("token_to_name_str", {}).keys())
                except Exception:
                    all_subscribed_tokens = []

            if not all_subscribed_tokens:
                return {}

            try:
                raw_snapshots_map = src.get_snapshots_bulk(all_subscribed_tokens) or {}
            except Exception:
                raw_snapshots_map = {}

            ltp_map: Dict[str, float] = {}
            volume_map: Dict[str, float] = {}
            latest_ts = 0.0

            for token, snap_data in raw_snapshots_map.items():
                if not isinstance(snap_data, dict):
                    continue

                price = snap_data.get("ltp")
                vol = snap_data.get("volume")
                ts_val = snap_data.get("ts")

                if price is not None:
                    ltp_map[str(token)] = price
                if vol is not None:
                    volume_map[str(token)] = vol

                ts_num = _safe_float(ts_val, 0.0)
                if ts_num > latest_ts:
                    latest_ts = ts_num

            if ltp_map:
                return {
                    "ts": latest_ts if latest_ts > 0 else _now(),
                    "ltp": ltp_map,
                    "volume": volume_map,
                }

            return {}

        # secondary compatibility path: source exposes get_snapshot(token)
        if hasattr(src, "get_snapshot") and callable(getattr(src, "get_snapshot")):
            token_to_name = self.taxonomy.get("token_to_name_str", {})
            tokens = list(token_to_name.keys())
            if not tokens:
                return {}

            ltp_map: Dict[str, float] = {}
            volume_map: Dict[str, float] = {}
            latest_ts = 0.0

            for tok in tokens:
                try:
                    snap_data = src.get_snapshot(tok) or {}
                except Exception:
                    continue

                if not isinstance(snap_data, dict):
                    continue

                price = snap_data.get("ltp")
                vol = snap_data.get("volume")
                ts_val = snap_data.get("ts")

                if price is not None:
                    ltp_map[str(tok)] = price
                if vol is not None:
                    volume_map[str(tok)] = vol

                ts_num = _safe_float(ts_val, 0.0)
                if ts_num > latest_ts:
                    latest_ts = ts_num

            if ltp_map:
                return {
                    "ts": latest_ts if latest_ts > 0 else _now(),
                    "ltp": ltp_map,
                    "volume": volume_map,
                }

            return {}

        return {}

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            now = _now()
            snap = self._get_snapshot()
            self._ingest_snapshot(snap)

            # scheduled tasks
            if now - self._last_macro >= self.macro_interval_s:
                self._last_macro = now
                self._compute_macro_context(now)

            if now - self._last_micro >= self.micro_interval_s:
                self._last_micro = now
                self._compute_micro_sectors(now)

            if now - self._last_opp >= self.opp_interval_s:
                self._last_opp = now
                self._compute_opportunity_sectors(now)

            if now - self._last_event >= self.event_interval_s:
                self._last_event = now
                self._compute_event_risk(now)

            # emit update
            if self.on_update:
                try:
                    self.on_update(self.get_outputs())
                except Exception:
                    pass

            time.sleep(1.0)

    def _ingest_snapshot(self, snap: Dict[str, Any]) -> None:
        """
        Store rolling price history for indexes present in taxonomy.
        Supports name-keyed or token-keyed ltp maps.
        """
        if not snap:
            return

        ts = _safe_float(snap.get("ts", _now()))
        ltp = snap.get("ltp") or {}
        volmap = snap.get("volume") or {}

        token_to_name_int = self.taxonomy.get("token_to_name", {})
        token_to_name_str = self.taxonomy.get("token_to_name_str", {})
        tracked_names = set(self.taxonomy.get("all_names", []))

        # Build name->price
        name_price: Dict[str, float] = {}

        # if keys are tokens (int/str digits), map to names
        for k, v in ltp.items():
            price = _safe_float(v)
            if math.isnan(price) or price <= 0:
                continue
            name: Optional[str] = None

            k_str = str(k).strip()
            if k_str in token_to_name_str:
                name = token_to_name_str.get(k_str)
            else:
                try:
                    tok = int(k_str)
                    name = token_to_name_int.get(tok)
                except Exception:
                    name = str(k)

            if name and name in tracked_names:
                name_price[name] = price

        if not name_price:
            return

        with self._lock:
            for name, price in name_price.items():
                volume = _safe_float(volmap.get(name), 0.0)
                buf = self._hist.setdefault(name, [])
                buf.append((ts, price, volume))

                # trim by time window + max points
                cutoff = ts - self.window_s
                # drop old
                while buf and buf[0][0] < cutoff:
                    buf.pop(0)
                # cap length
                if len(buf) > self.max_points:
                    del buf[: len(buf) - self.max_points]

    def _load_taxonomy(self, path: str) -> Dict[str, Any]:
        """
        Taxonomy JSON should contain list of index entries.
        Minimal accepted format:
        {
          "indexes": [
            {"name":..., "token":..., "sector":..., "pillar":"macro/micro/other", "priority":1},
            ...
          ]
        }
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {"indexes": []}

        indexes = data.get("indexes") or []
        token_to_name: Dict[int, str] = {}
        token_to_name_str: Dict[str, str] = {}
        name_to_sector: Dict[str, str] = {}
        macro: List[str] = []
        micro: List[str] = []
        other: List[str] = []
        all_names: List[str] = []

        for it in indexes:
            name = str(it.get("name", "")).strip()
            if not name:
                continue

            tok = it.get("token")

            sector = str(it.get("sector", "")).strip() or _infer_sector(name)
            pillar_raw = str(it.get("pillar", "")).strip().lower()
            pillar = pillar_raw if pillar_raw in ("macro", "micro", "other") else _infer_pillar(name)

            all_names.append(name)
            name_to_sector[name] = sector

            if tok is not None:
                tok_str = str(tok).strip()
                if tok_str:
                    token_to_name_str[tok_str] = name
                    try:
                        token_to_name[int(tok_str)] = name
                    except Exception:
                        pass

            if pillar == "macro":
                macro.append(name)
            elif pillar == "micro":
                micro.append(name)
            else:
                other.append(name)

        return {
            "indexes": indexes,
            "token_to_name": token_to_name,
            "token_to_name_str": token_to_name_str,
            "name_to_sector": name_to_sector,
            "macro": macro,
            "micro": micro,
            "other": other,
            "all_names": all_names,
        }

    # -----------------------------
    # 3A — Index & Sector Context
    # -----------------------------

    def _compute_macro_context(self, now: float) -> None:
        macro_names = list(self.taxonomy.get("macro", []))
        if not macro_names:
            # fallback defaults if taxonomy missing
            macro_names = list(_FALLBACK_MACRO_NAMES)

        # Compute per-macro return/vol signals
        per: List[Tuple[str, float, float]] = []  # (name, ret, vol)
        with self._lock:
            for name in macro_names:
                r, v = self._calc_return_and_vol(name)
                if r is None:
                    continue
                per.append((name, r, v))

        if not per:
            return

        # Aggregate
        rets = [x[1] for x in per]
        vols = [x[2] for x in per]

        avg_ret = sum(rets) / max(1, len(rets))
        avg_vol = sum(vols) / max(1, len(vols))

        # India VIX (if available) as volatility amplifier (optional)
        vix_val = None
        with self._lock:
            r_vix, _ = self._calc_return_and_vol(self.india_vix_name)
            # if we have price history, estimate last price to infer high/low
            buf = self._hist.get(self.india_vix_name, [])
            if buf:
                vix_val = buf[-1][1]

        # Volatility state heuristic:
        # - avg_vol is scaled vol of returns (0..something)
        # - adjust by VIX if present (India VIX ~ 10-25 typical, >20 elevated)
        vol_state = "NORMAL"
        reason_tags: List[str] = []

        if avg_vol >= 0.0045:  # ~0.45% std over window
            vol_state = "HIGH"
            reason_tags.append("VOL_SPIKE")
        elif avg_vol <= 0.0020:
            vol_state = "LOW"
            reason_tags.append("LOW_VOL")

        if vix_val is not None:
            if vix_val >= 20:
                vol_state = "HIGH"
                reason_tags.append("VIX_ELEVATED")
            elif vix_val <= 13 and vol_state != "HIGH":
                vol_state = "LOW"
                reason_tags.append("VIX_CALM")

        # Macro bias from avg_ret
        bias = "NEUTRAL"
        if avg_ret >= 0.0035:
            bias = "BULLISH"
            reason_tags.append("MACRO_UP")
        elif avg_ret <= -0.0035:
            bias = "BEARISH"
            reason_tags.append("MACRO_DOWN")

        # Trend score based on magnitude of avg_ret scaled into 0..1
        trend_score = _clamp(abs(avg_ret) / 0.008, 0.0, 1.0)
        regime = _trend_label(trend_score, vol_state)

        with self._lock:
            self.market_context_state.macro_bias = bias
            self.market_context_state.volatility_state = vol_state
            self.market_context_state.macro_regime = regime
            self.market_context_state.updated_at = now
            # divergence/trade_gate updated later after micro/opp
            self.market_context_state.reason_tags = reason_tags

            # trade_gate baseline from macro-only
            gate = "ALLOW"
            if vol_state == "HIGH":
                gate = "CAUTION"
            if vol_state == "HIGH" and abs(avg_ret) >= 0.010:
                gate = "PAUSE"
                self.market_context_state.reason_tags.append("EXTREME_MOVE")
            self.market_context_state.trade_gate = gate

    def _compute_micro_sectors(self, now: float) -> None:
        micro_names = list(self.taxonomy.get("micro", []))
        if not micro_names:
            return

        # Build sector aggregates from micro pillar indexes
        name_to_sector = self.taxonomy.get("name_to_sector", {})

        sector_stats: Dict[str, Dict[str, float]] = {}  # sector -> sums
        with self._lock:
            for name in micro_names:
                r, v = self._calc_return_and_vol(name)
                if r is None:
                    continue
                sector = name_to_sector.get(name, "UNKNOWN")
                s = sector_stats.setdefault(sector, {"ret_sum": 0.0, "vol_sum": 0.0, "n": 0.0})
                s["ret_sum"] += r
                s["vol_sum"] += v
                s["n"] += 1.0

        if not sector_stats:
            return

        rows = self._score_sectors(sector_stats)

        # Update scoreboard partially (micro-driven)
        with self._lock:
            self.sector_scoreboard.rows = rows
            self.sector_scoreboard.sector_rankings = [r.sector_name for r in rows]
            self.sector_scoreboard.updated_at = now

        # Divergence & gate update (macro vs micro breadth)
        self._update_divergence_and_gate(now)

    def _compute_opportunity_sectors(self, now: float) -> None:
        other_names = list(self.taxonomy.get("other", []))
        if not other_names:
            return

        name_to_sector = self.taxonomy.get("name_to_sector", {})

        # Aggregate additional sector signals from "other" indexes
        sector_stats: Dict[str, Dict[str, float]] = {}
        with self._lock:
            for name in other_names:
                r, v = self._calc_return_and_vol(name)
                if r is None:
                    continue
                sector = name_to_sector.get(name, "UNKNOWN")
                s = sector_stats.setdefault(sector, {"ret_sum": 0.0, "vol_sum": 0.0, "n": 0.0})
                # opportunity wants unusual momentum → weight return a bit more
                s["ret_sum"] += 1.2 * r
                s["vol_sum"] += v
                s["n"] += 1.0

        if not sector_stats:
            return

        opp_rows = self._score_sectors(sector_stats, tag_prefix="OPP")

        # Merge with existing scoreboard (micro rows) by sector:
        with self._lock:
            base = {r.sector_name: r for r in self.sector_scoreboard.rows}
            for r in opp_rows:
                if r.sector_name not in base:
                    base[r.sector_name] = r
                else:
                    # blend scores
                    b = base[r.sector_name]
                    base[r.sector_name] = SectorScoreRow(
                        sector_name=b.sector_name,
                        strength_score=0.6 * b.strength_score + 0.4 * r.strength_score,
                        volatility_score=0.6 * b.volatility_score + 0.4 * r.volatility_score,
                        trend_score=0.6 * b.trend_score + 0.4 * r.trend_score,
                        confidence_level=_confidence_label(
                            0.6 * self._conf_from_row(b) + 0.4 * self._conf_from_row(r)
                        ),
                        reason_tags=list(dict.fromkeys(b.reason_tags + r.reason_tags)),
                    )

            merged = list(base.values())
            merged.sort(key=lambda x: (x.strength_score + x.trend_score - 0.6 * x.volatility_score), reverse=True)

            self.sector_scoreboard.rows = merged
            self.sector_scoreboard.sector_rankings = [r.sector_name for r in merged]
            self.sector_scoreboard.updated_at = now

        self._update_divergence_and_gate(now)

    def _calc_return_and_vol(self, name: str) -> Tuple[Optional[float], float]:
        """
        Return:
          ret: (last - first) / first over available history window
          vol: std dev of simple returns in window
        """
        buf = self._hist.get(name)
        if not buf or len(buf) < 5:
            return None, 0.0

        p0 = buf[0][1]
        p1 = buf[-1][1]
        if p0 <= 0:
            return None, 0.0

        ret = (p1 - p0) / p0

        # compute simple return series
        rs: List[float] = []
        for i in range(1, len(buf)):
            prev = buf[i - 1][1]
            cur = buf[i][1]
            if prev > 0:
                rs.append((cur - prev) / prev)

        if len(rs) < 3:
            return ret, 0.0

        mean = sum(rs) / max(1, len(rs))
        var = sum((x - mean) ** 2 for x in rs) / max(1, (len(rs) - 1))
        vol = math.sqrt(max(0.0, var))
        return ret, vol

    def _score_sectors(self, sector_stats: Dict[str, Dict[str, float]], tag_prefix: str = "MICRO") -> List[SectorScoreRow]:
        """
        Convert sector aggregated returns/vol into ranked scoreboard rows.
        Strength score: scaled positive return + breadth
        Trend score: absolute return scaled
        Volatility score: scaled vol (higher vol reduces desirability unless trending)
        """
        # prepare raw arrays
        sectors = []
        rets = []
        vols = []
        ns = []
        for sec, s in sector_stats.items():
            n = max(1.0, s["n"])
            r = s["ret_sum"] / n
            v = s["vol_sum"] / n
            sectors.append(sec)
            rets.append(r)
            vols.append(v)
            ns.append(n)

        # stats
        mean_r = sum(rets) / max(1, len(rets))
        mean_v = sum(vols) / max(1, len(vols))
        std_r = math.sqrt(sum((x - mean_r) ** 2 for x in rets) / max(1, len(rets) - 1)) if len(rets) > 1 else 1e-9
        std_v = math.sqrt(sum((x - mean_v) ** 2 for x in vols) / max(1, len(vols) - 1)) if len(vols) > 1 else 1e-9

        rows: List[SectorScoreRow] = []
        for sec, r, v, n in zip(sectors, rets, vols, ns):
            zr = _zscore(r, mean_r, std_r)     # higher better
            zv = _zscore(v, mean_v, std_v)     # higher = more volatile

            # scores in 0..1
            strength = _clamp(0.5 + 0.18 * zr + 0.05 * math.log1p(n), 0.0, 1.0)
            trend = _clamp(abs(r) / 0.01, 0.0, 1.0)
            vol_score = _clamp(0.5 + 0.18 * zv, 0.0, 1.0)

            # confidence: trend + breadth, penalize extreme vol without trend
            conf = _clamp(0.45 * trend + 0.20 * math.log1p(n) / 4.0 + 0.35 * (1.0 - vol_score if trend < 0.4 else 0.7), 0.0, 1.0)
            conf_label = _confidence_label(conf)

            tags = [f"{tag_prefix}_SECTOR"]
            if r >= 0.004:
                tags.append("MOMENTUM_UP")
            if r <= -0.004:
                tags.append("MOMENTUM_DOWN")
            if vol_score >= 0.7:
                tags.append("VOLATILE")
            if trend >= 0.7:
                tags.append("TRENDING")

            rows.append(
                SectorScoreRow(
                    sector_name=sec,
                    strength_score=float(strength),
                    volatility_score=float(vol_score),
                    trend_score=float(trend),
                    confidence_level=conf_label,
                    reason_tags=tags,
                )
            )

        # rank: prefer strength + trend, penalize volatility modestly
        rows.sort(key=lambda x: (x.strength_score + x.trend_score - 0.6 * x.volatility_score), reverse=True)
        return rows

    def _conf_from_row(self, r: SectorScoreRow) -> float:
        return {"LOW": 0.3, "MEDIUM": 0.6, "HIGH": 0.85}.get(r.confidence_level, 0.5)

    def _update_divergence_and_gate(self, now: float) -> None:
        """
        Divergence: compare macro bias vs sector breadth.
        Breadth proxy: share of top sectors with positive momentum.
        """
        with self._lock:
            bias = self.market_context_state.macro_bias
            vol_state = self.market_context_state.volatility_state
            rows = list(self.sector_scoreboard.rows)

        if not rows:
            return

        top = rows[:10] if len(rows) >= 10 else rows
        pos = sum(1 for r in top if "MOMENTUM_UP" in r.reason_tags)
        neg = sum(1 for r in top if "MOMENTUM_DOWN" in r.reason_tags)
        breadth = (pos - neg) / max(1, len(top))  # -1..+1

        divergence = "NONE"
        div_reason = ""

        if bias == "BULLISH" and breadth < -0.2:
            divergence = "HIGH"
            div_reason = "Macro bullish but top sectors weak"
        elif bias == "BEARISH" and breadth > 0.2:
            divergence = "HIGH"
            div_reason = "Macro bearish but sectors strong"
        elif abs(breadth) < 0.1 and bias in ("BULLISH", "BEARISH"):
            divergence = "MILD"
            div_reason = "Macro directional but sector breadth mixed"

        # trade gate logic:
        gate = "ALLOW"
        tags: List[str] = []
        if vol_state == "HIGH":
            gate = "CAUTION"
            tags.append("VOL_HIGH")

        if divergence == "MILD":
            gate = "CAUTION" if gate == "ALLOW" else gate
            tags.append("DIVERGENCE_MILD")
        if divergence == "HIGH":
            gate = "PAUSE" if vol_state == "HIGH" else "CAUTION"
            tags.append("DIVERGENCE_HIGH")

        # tighten gate if top sector confidence low overall
        avg_conf = sum(self._conf_from_row(r) for r in top) / max(1, len(top))
        if avg_conf < 0.45:
            gate = "CAUTION" if gate == "ALLOW" else gate
            tags.append("LOW_CONF_BREADTH")

        with self._lock:
            self.market_context_state.divergence_flag = divergence
            if div_reason:
                self.market_context_state.reason_tags.append(div_reason)
            # merge tags uniquely
            self.market_context_state.reason_tags = list(dict.fromkeys(self.market_context_state.reason_tags + tags))
            self.market_context_state.trade_gate = gate
            self.market_context_state.updated_at = now

    # -----------------------------
    # 3B — Event/News Risk Monitor
    # -----------------------------

    def _compute_event_risk(self, now: float) -> None:
        """
        Reads event_calendar.json and produces:
        - symbol_risk_map
        - market_news_risk -> applied into trade_gate (can elevate CAUTION/PAUSE)
        """
        try:
            with open(self.event_calendar_path, "r", encoding="utf-8") as f:
                cal = json.load(f)
        except Exception:
            cal = {}

        events = cal.get("events") or cal.get("data") or []
        symbol_map: Dict[str, SymbolRiskRow] = {}

        # market-wide risk
        market_risk = "NORMAL"
        market_reason = ""

        for evt in events:
            date = str(evt.get("date", "")).strip()
            etype = str(evt.get("event_type", "")).strip()
            impact = str(evt.get("impact_level", "")).strip().upper()  # LOW/MED/HIGH
            affected = evt.get("affected_symbols") or evt.get("symbols") or []
            if not date:
                continue

            d = _days_to_event(now, date)
            if d is None:
                continue

            # risk windows
            # - high impact: AVOID on event day (d==0), CAUTION within 1 day
            # - medium: CAUTION on day and +/-1
            # - low: CAUTION only on day
            def risk_for(impact_level: str, days_to: int) -> Optional[str]:
                if impact_level == "HIGH":
                    if days_to == 0:
                        return "AVOID"
                    if abs(days_to) <= 1:
                        return "CAUTION"
                if impact_level == "MEDIUM":
                    if abs(days_to) <= 1:
                        return "CAUTION"
                if impact_level == "LOW":
                    if days_to == 0:
                        return "CAUTION"
                return None

            rlevel = risk_for(impact, d)
            if not rlevel:
                continue

            # update affected symbols
            for sym in affected:
                sym = str(sym).strip()
                if not sym:
                    continue
                prev = symbol_map.get(sym)
                reason = f"{etype} ({impact}) on {date}"
                row = SymbolRiskRow(
                    symbol=sym,
                    risk_level=rlevel,
                    reason=reason,
                    event_type=etype,
                    impact_level=impact,
                    event_date=date,
                )

                # escalate risk if multiple events
                if prev:
                    order = {"NORMAL": 0, "CAUTION": 1, "AVOID": 2}
                    if order[row.risk_level] > order[prev.risk_level]:
                        symbol_map[sym] = row
                    else:
                        # keep higher, but append reason
                        prev.reason = f"{prev.reason}; {reason}"
                        symbol_map[sym] = prev
                else:
                    symbol_map[sym] = row

            # market-wide (if event affects many or explicitly market)
            is_market = bool(evt.get("market_wide")) or ("RBI" in etype.upper()) or ("CPI" in etype.upper())
            if is_market:
                if impact == "HIGH" and d == 0:
                    market_risk = "PAUSE"
                    market_reason = f"Market event today: {etype} ({impact})"
                elif impact in ("HIGH", "MEDIUM") and abs(d) <= 1 and market_risk != "PAUSE":
                    market_risk = "CAUTION"
                    market_reason = f"Market event window: {etype} ({impact})"

        with self._lock:
            self.symbol_risk_map = symbol_map

            # apply market risk to trade_gate
            if market_risk == "PAUSE":
                self.market_context_state.trade_gate = "PAUSE"
                self.market_context_state.reason_tags.append(market_reason or "MARKET_EVENT_PAUSE")
            elif market_risk == "CAUTION" and self.market_context_state.trade_gate == "ALLOW":
                self.market_context_state.trade_gate = "CAUTION"
                self.market_context_state.reason_tags.append(market_reason or "MARKET_EVENT_CAUTION")

            # keep tags unique
            self.market_context_state.reason_tags = list(dict.fromkeys(self.market_context_state.reason_tags))
            self.market_context_state.updated_at = now


# =========================
# Convenience: default usage
# =========================

def build_market_context_monitor(
    module02_pipeline: Any,
    taxonomy_path: str,
    event_calendar_path: str,
    *,
    on_update: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> MarketContextMonitor:
    """
    Factory helper.
    Pass your Module 02 pipeline object (must expose get_snapshot()).
    """
    return MarketContextMonitor(
        snapshot_source=module02_pipeline,
        taxonomy_path=taxonomy_path,
        event_calendar_path=event_calendar_path,
        on_update=on_update,
    )
