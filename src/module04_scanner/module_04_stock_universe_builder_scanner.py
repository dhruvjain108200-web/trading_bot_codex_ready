# MODULE 04 — Stock Universe Builder + Scanner (Hybrid)
# Purpose: Convert a large merged universe into top intraday candidates every 5 minutes.

from __future__ import annotations

import math
import time
import json
import statistics
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import defaultdict, deque


# ============================================================
# Helpers
# ============================================================

def _now_ts() -> float:
    return time.time()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _avg(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return default
    return sum(vals) / len(vals)


def _median(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return default
    try:
        return statistics.median(vals)
    except Exception:
        return default


def _stddev(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if isinstance(v, (int, float))]
    if len(vals) < 2:
        return default
    try:
        return statistics.pstdev(vals)
    except Exception:
        return default


def _ema(series: List[float], period: int) -> List[float]:
    if not series:
        return []
    if period <= 1:
        return list(series)
    alpha = 2 / (period + 1)
    out = [series[0]]
    for x in series[1:]:
        out.append((x * alpha) + (out[-1] * (1 - alpha)))
    return out


def _sma(series: List[float], period: int) -> List[float]:
    if not series:
        return []
    out = []
    running = 0.0
    q = deque()
    for x in series:
        q.append(x)
        running += x
        if len(q) > period:
            running -= q.popleft()
        out.append(running / len(q))
    return out


def _rsi(closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)

    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(abs(min(ch, 0.0)))

    avg_gain = _sma(gains, period)
    avg_loss = _sma(losses, period)

    out = []
    for g, l in zip(avg_gain, avg_loss):
        if l == 0:
            out.append(100.0 if g > 0 else 50.0)
        else:
            rs = g / l
            out.append(100 - (100 / (1 + rs)))
    return out


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(
        high - low,
        abs(high - prev_close),
        abs(low - prev_close),
    )


def _atr(candles: List[Dict[str, Any]], period: int = 14) -> List[float]:
    if not candles:
        return []
    trs = []
    prev_close = _safe_float(candles[0].get("close"), 0.0)
    for c in candles:
        h = _safe_float(c.get("high"))
        l = _safe_float(c.get("low"))
        tr = _true_range(h, l, prev_close)
        trs.append(tr)
        prev_close = _safe_float(c.get("close"), prev_close)
    return _sma(trs, period)


def _bollinger(closes: List[float], period: int = 20, mult: float = 2.0) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    mid = _sma(closes, period)
    upper, lower = [], []
    for i in range(len(closes)):
        start = max(0, i - period + 1)
        window = closes[start:i + 1]
        sd = _stddev(window, 0.0)
        upper.append(mid[i] + (mult * sd))
        lower.append(mid[i] - (mult * sd))
    return upper, mid, lower


def _normalize_score(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return _clamp((x - lo) / (hi - lo), 0.0, 1.0)


def _as_symbol_record(item: Any) -> Dict[str, Any]:
    if isinstance(item, str):
        return {"symbol": item.strip()}
    if isinstance(item, dict):
        return {
            "symbol": str(item.get("symbol", "")).strip(),
            "exchange": str(item.get("exchange", "NSE")).strip() or "NSE",
            "sector": item.get("sector"),
            "token": item.get("token"),
            **item,
        }
    return {"symbol": ""}


# ============================================================
# Data Contracts
# ============================================================

@dataclass
class ScannerHealth:
    state: str = "INIT"
    last_scan_ts: float = 0.0
    last_scan_iso: str = ""
    degraded: bool = False
    last_error: Optional[str] = None
    scan_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CandidateScore:
    trend_score: float = 0.0
    mr_score: float = 0.0
    vol_score: float = 0.0
    volume_score: float = 0.0
    whale_score: float = 0.0
    sector_bonus: float = 0.0
    macro_bonus: float = 0.0
    risk_penalty: float = 0.0
    spread_penalty: float = 0.0
    persistence_bonus: float = 0.0
    composite_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ScanCandidate:
    symbol: str
    token: str
    exchange: str
    sector: str
    tier: str
    regime_tag_used: str
    composite_score: float
    component_scores: Dict[str, Any]
    snapshot: Dict[str, Any]
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "token": self.token,
            "exchange": self.exchange,
            "sector": self.sector,
            "tier": self.tier,
            "composite_score": round(self.composite_score, 4),
            "component_scores": self.component_scores,
            "snapshot": self.snapshot,
            "regime_tag_used": self.regime_tag_used,
            "meta": self.meta,
        }


# ============================================================
# Default Thresholds / Config
# ============================================================

DEFAULT_SCANNER_CONFIG: Dict[str, Any] = {
    "scan_interval_sec": 300,
    "stage1": {
        "max_stream_universe": 120,
        "min_price": 20.0,
        "max_price": 5000.0,
        "min_volume": 50000,
        "min_traded_value": 5000000.0,   # 50 lakh
        "min_range_pct": 0.0075,         # 0.75%
        "max_spread_pct": 0.0060,        # 0.60%
        "caution_liquidity_multiplier": 1.25,
        "caution_volatility_multiplier": 1.15,
        "near_circuit_pct": 0.019,       # proxy ~ within 1.9% of upper/lower freeze ref
        "stage1_target_min": 100,
        "stage1_target_max": 140,
    },
    "stage2": {
        "lookback_candles": 80,
        "ema_fast": 20,
        "ema_mid": 50,
        "ema_slow": 200,
        "rsi_period": 14,
        "atr_period": 14,
        "bb_period": 20,
        "bb_mult": 2.0,
    },
    "ranking": {
        "top_combined": 20,
        "backup_count": 20,
        "tier1_count": 10,
        "cooldown_min": 20,
        "persistence_lookback_scans": 6,
    },
    "weights": {
        "TRENDING": {
            "trend": 0.32,
            "mr": 0.08,
            "vol": 0.18,
            "volume": 0.16,
            "whale": 0.10,
            "sector": 0.07,
            "macro": 0.06,
            "persistence": 0.03,
        },
        "RANGING": {
            "trend": 0.12,
            "mr": 0.28,
            "vol": 0.16,
            "volume": 0.14,
            "whale": 0.08,
            "sector": 0.08,
            "macro": 0.08,
            "persistence": 0.06,
        },
        "HIGH_VOL": {
            "trend": 0.22,
            "mr": 0.12,
            "vol": 0.24,
            "volume": 0.16,
            "whale": 0.10,
            "sector": 0.07,
            "macro": 0.06,
            "persistence": 0.03,
        },
        "LOW_VOL": {
            "trend": 0.22,
            "mr": 0.18,
            "vol": 0.12,
            "volume": 0.14,
            "whale": 0.08,
            "sector": 0.10,
            "macro": 0.10,
            "persistence": 0.06,
        },
    },
}


# ============================================================
# Module 04 Scanner
# ============================================================

class StockUniverseBuilderScanner:
    """
    Module 04 — Stock Universe Builder + Scanner (Hybrid)

    Expected integrations:
    - broker.ensure_logged_in()
    - broker.get_tokens()
    - pipeline.get_snapshot(symbol or token) / pipeline.get_bulk_snapshots()
    - market_context_provider.get_state()
    - maintenance_provider.read_json(name)
    - candle_provider.get_5m_candles(symbol, token, exchange, lookback)
    """

    def __init__(
        self,
        broker: Any,
        pipeline: Any,
        market_context_provider: Any,
        maintenance_provider: Any,
        candle_provider: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.broker = broker
        self.pipeline = pipeline
        self.market_context_provider = market_context_provider
        self.maintenance_provider = maintenance_provider
        self.candle_provider = candle_provider

        self.config = self._deep_merge(DEFAULT_SCANNER_CONFIG, config or {})
        self.health = ScannerHealth(state="READY")

        self._shortlist_history: Dict[str, deque] = defaultdict(deque)  # symbol -> recent scan timestamps
        self._last_scores: Dict[str, float] = {}
        self._last_candidates: List[Dict[str, Any]] = []
        self._candle_cache: Dict[str, Dict[str, Any]] = {}
        self._candle_cache_ttl_sec = 240

    # --------------------------------------------------------
    # Public API
    # --------------------------------------------------------

    def health_dict(self) -> Dict[str, Any]:
        return self.health.to_dict()

    def run_scan(self) -> Dict[str, Any]:
        scan_ts = _now_ts()
        diagnostics: Dict[str, Any] = {
            "scan_time": scan_ts,
            "universe_size": 0,
            "stage1_survivors_count": 0,
            "exclusions_summary": {
                "missing_mapping_count": 0,
                "risk_avoid_count": 0,
                "spread_filtered_count": 0,
                "illiquid_filtered_count": 0,
                "flat_filtered_count": 0,
                "price_filtered_count": 0,
                "halt_circuit_filtered_count": 0,
            },
            "source_counts_raw": {},
            "source_counts_merged": {},
            "merged_unique_count": 0,
            "top_sectors_for_scan": [],
            "trade_gate": "UNKNOWN",
            "notes": [],
            "degraded": False,
        }

        try:
            self.health.state = "SCANNING"
            self.broker.ensure_logged_in()
            _ = self.broker.get_tokens()

            market_state = self._get_market_context()
            mode_ctx = self._mode_context()
            diagnostics["trade_gate"] = market_state["market_context_state"].get("trade_gate", "UNKNOWN")

            if diagnostics["trade_gate"] == "PAUSE":
                diagnostics["notes"].append("Trade gate is PAUSE; scanner returned empty candidate lists.")
                out = {
                    "candidate_list": [],
                    "candidate_backup_list": [],
                    "scanner_diagnostics": diagnostics,
                }
                self._finalize_health(scan_ts, degraded=False, error=None)
                return out

            universe_bundle = self._build_merged_universe()
            universe = universe_bundle.get("universe", [])
            diagnostics["universe_size"] = len(universe)
            diagnostics["source_counts_raw"] = universe_bundle.get("source_counts_raw", {})
            diagnostics["source_counts_merged"] = universe_bundle.get("source_counts_merged", {})
            diagnostics["merged_unique_count"] = universe_bundle.get("merged_unique_count", len(universe))

            raw_counts = diagnostics.get("source_counts_raw", {})
            if raw_counts.get("potential", 0) == 0:
                diagnostics["notes"].append("potential_stocks.json is empty.")
            if raw_counts.get("movers", 0) == 0:
                diagnostics["notes"].append("daily_movers source is empty.")
            if raw_counts.get("losers", 0) == 0:
                diagnostics["notes"].append("daily_losers source is empty.")
            if raw_counts.get("manual_additions", 0) == 0:
                diagnostics["notes"].append("manual_additions.json is empty.")
            if raw_counts.get("discovered_symbols", 0) == 0:
                diagnostics["notes"].append("discovered_symbols.json is empty.")
            if raw_counts.get("previous_day_movers", 0) == 0:
                diagnostics["notes"].append("previous_day_movers.json is empty.")
            if raw_counts.get("previous_day_losers", 0) == 0:
                diagnostics["notes"].append("previous_day_losers.json is empty.")    

            if not universe:
                diagnostics["notes"].append("Merged universe is empty.")
                diagnostics["degraded"] = True
                out = {
                    "candidate_list": [],
                    "candidate_backup_list": [],
                    "scanner_diagnostics": diagnostics,
                }
                self._finalize_health(scan_ts, degraded=True, error=None)
                return out

            snapshots, snapshot_stale = self._get_universe_snapshots(universe)
            if snapshot_stale:
                diagnostics["degraded"] = True
                diagnostics["notes"].append("WebSocket snapshot stale/unavailable; running in DEGRADED mode.")

            stage1_survivors = self._run_stage1_soft_filter(
                universe=universe,
                snapshots=snapshots,
                market_state=market_state,
                diagnostics=diagnostics,
                mode_ctx=mode_ctx,
            )
            diagnostics["stage1_survivors_count"] = len(stage1_survivors)

            if not stage1_survivors:
                diagnostics["notes"].append("No Stage-1 survivors after soft filtering.")

                if mode_ctx["live_market"] and diagnostics["degraded"] and self._last_candidates:
                    diagnostics["notes"].append("Reusing last good candidates due to live-market stale WS condition.")
                    out = {
                        "candidate_list": self._last_candidates[: self.config["ranking"]["top_combined"]],
                        "candidate_backup_list": self._last_candidates[
                            self.config["ranking"]["top_combined"]:
                            self.config["ranking"]["top_combined"] + self.config["ranking"]["backup_count"]
                        ],
                        "scanner_diagnostics": diagnostics,
                    }
                    self._finalize_health(scan_ts, degraded=True, error=None)
                    return out

                out = {
                    "candidate_list": [],
                    "candidate_backup_list": [],
                    "scanner_diagnostics": diagnostics,
                }
                self._finalize_health(scan_ts, degraded=diagnostics["degraded"], error=None)
                return out

            ranked = self._run_stage2_deep_scan(
                survivors=stage1_survivors,
                snapshots=snapshots,
                market_state=market_state,
                diagnostics=diagnostics,
                mode_ctx=mode_ctx,
            )

            top_n = self.config["ranking"]["top_combined"]
            backup_n = self.config["ranking"]["backup_count"]
            selected = ranked[:top_n]
            backup = ranked[top_n:top_n + backup_n]

            candle_store_5m = {}

            for item in selected + backup:
                symbol = str(item.get("symbol", "")).strip().upper()
                candles = item.get("candles_5m") or item.get("meta", {}).get("candles_5m") or []
                if symbol and candles:
                    candle_store_5m[symbol] = candles

            self._update_persistence_and_cooldown(selected, scan_ts)
            self._last_candidates = selected

            out = {
                "candidate_list": selected,
                "candidate_backup_list": backup,
                "candle_store_5m": candle_store_5m,
                "scanner_diagnostics": diagnostics,
            }
            self._finalize_health(scan_ts, degraded=diagnostics["degraded"], error=None)
            return out

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            diagnostics["degraded"] = True
            diagnostics["notes"].append(f"Scanner exception: {msg}")
            self._finalize_health(scan_ts, degraded=True, error=msg)
            return {
                "candidate_list": [],
                "candidate_backup_list": [],
                "scanner_diagnostics": diagnostics,
            }

    # --------------------------------------------------------
    # Core Scan Steps
    # --------------------------------------------------------

    def _build_merged_universe(self) -> Dict[str, Any]:
        watchlist = self._safe_json_read("watchlist.json", default=[])
        potential = self._safe_json_read("potential_stocks.json", default=[])
        runtime_cfg = self._safe_json_read("runtime_config.json", default={})
        taxonomy = self._safe_json_read("index_taxonomy.json", default={})
        manual_additions = self._safe_json_read("manual_additions.json", default=[])
        discovered_symbols = self._safe_json_read("discovered_symbols.json", default=[])
        previous_day_movers = self._safe_json_read("previous_day_movers.json", default=[])
        previous_day_losers = self._safe_json_read("previous_day_losers.json", default=[])

        movers = self._fetch_daily_movers(runtime_cfg=runtime_cfg)
        losers = self._fetch_daily_losers(runtime_cfg=runtime_cfg)
        mapping = self._get_runtime_mappings(runtime_cfg=runtime_cfg, taxonomy=taxonomy)

        merged: Dict[str, Dict[str, Any]] = {}
        source_counts_raw = {
            "watchlist": len(watchlist) if isinstance(watchlist, list) else 0,
            "potential": len(potential) if isinstance(potential, list) else 0,
            "manual_additions": len(manual_additions) if isinstance(manual_additions, list) else 0,
            "discovered_symbols": len(discovered_symbols) if isinstance(discovered_symbols, list) else 0,
            "previous_day_movers": len(previous_day_movers) if isinstance(previous_day_movers, list) else 0,
            "previous_day_losers": len(previous_day_losers) if isinstance(previous_day_losers, list) else 0,
            "movers": len(movers) if isinstance(movers, list) else 0,
            "losers": len(losers) if isinstance(losers, list) else 0,
        }

        source_counts_merged = {
            "watchlist": 0,
            "potential": 0,
            "manual_additions": 0,
            "discovered_symbols": 0,
            "previous_day_movers": 0,
            "previous_day_losers": 0,
            "movers": 0,
            "losers": 0,
        }

        for src_name, items in (
            ("watchlist", watchlist),
            ("potential", potential),
            ("manual_additions", manual_additions),
            ("discovered_symbols", discovered_symbols),
            ("previous_day_movers", previous_day_movers),
            ("previous_day_losers", previous_day_losers),
            ("movers", movers),
            ("losers", losers),
        ):
            for raw in items:
                rec = _as_symbol_record(raw)
                symbol = rec.get("symbol", "").strip().upper()
                if not symbol:
                    continue

                exchange = str(rec.get("exchange", "NSE")).strip() or "NSE"
                key = f"{exchange}:{symbol}"
                mapped = mapping.get(key) or mapping.get(symbol) or {}

                existing = merged.get(key, {})
                existing_flags = set(existing.get("source_flags", []))
                new_flags = sorted(existing_flags | {src_name})

                merged[key] = {
                    "symbol": symbol,
                    "exchange": exchange or mapped.get("exchange", "NSE"),
                    "token": str(rec.get("token") or mapped.get("token") or "").strip(),
                    "sector": rec.get("sector") or mapped.get("sector") or "UNKNOWN",
                    "source_flags": new_flags,
                }

                if src_name not in existing_flags:
                    source_counts_merged[src_name] += 1

        return {
            "universe": list(merged.values()),
            "source_counts_raw": source_counts_raw,
            "source_counts_merged": source_counts_merged,
            "merged_unique_count": len(merged),
        }

    def _run_stage1_soft_filter(
        self,
        universe: List[Dict[str, Any]],
        snapshots: Dict[str, Dict[str, Any]],
        market_state: Dict[str, Any],
        diagnostics: Dict[str, Any],
        mode_ctx: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        cfg = self.config["stage1"]
        trade_gate = market_state["market_context_state"].get("trade_gate", "ALLOW")
        risk_map = market_state.get("symbol_risk_map", {})

        min_volume = cfg["min_volume"]
        min_traded_value = cfg["min_traded_value"]
        min_range_pct = cfg["min_range_pct"]
        max_spread_pct = cfg["max_spread_pct"]

        if trade_gate == "CAUTION":
            min_volume *= cfg["caution_liquidity_multiplier"]
            min_traded_value *= cfg["caution_liquidity_multiplier"]
            min_range_pct *= cfg["caution_volatility_multiplier"]

        survivors: List[Dict[str, Any]] = []
        live_market = bool(mode_ctx.get("live_market"))
        stage1_policy = self._stage1_policy(mode_ctx)
        stale_cutoff = 20.0

        ws_missing_or_stale_count = 0
        ws_soft_pass_count = 0
        offmarket_seed_cap = min(
            max(self.config["ranking"]["top_combined"] * 2, 20),
            40,
        )
        offmarket_seed_used = 0

        for rec in universe:
            symbol = rec["symbol"]
            token = str(rec.get("token", "")).strip()
            exchange = rec.get("exchange", "NSE")
            snap_key = f"{exchange}:{symbol}"
            snap = snapshots.get(snap_key, {}) or snapshots.get(symbol, {}) or {}

            if not token:
                diagnostics["exclusions_summary"]["missing_mapping_count"] += 1
                if live_market:
                    continue

            sym_risk = str(risk_map.get(symbol, "NORMAL")).upper()
            if sym_risk == "AVOID":
                diagnostics["exclusions_summary"]["risk_avoid_count"] += 1
                continue

            raw_ltp = _safe_float(snap.get("ltp"), 0.0)
            raw_day_high = _safe_float(snap.get("day_high"), 0.0)
            raw_day_low = _safe_float(snap.get("day_low"), 0.0)
            raw_bid = _safe_float(snap.get("bid"), 0.0)
            raw_ask = _safe_float(snap.get("ask"), 0.0)
            raw_prev_close = _safe_float(snap.get("prev_close"), 0.0)
            raw_volume = self._extract_snapshot_volume(snap)

            snap_ts = _safe_float(snap.get("ts", snap.get("last_tick_time", 0.0)), 0.0)
            snapshot_age = _now_ts() - snap_ts if snap_ts > 0 else 999999.0
            ws_missing_or_stale = (raw_ltp <= 0) or (snapshot_age > stale_cutoff)

            if ws_missing_or_stale:
                ws_missing_or_stale_count += 1

                if stage1_policy["strict_snapshot_required"]:
                    continue

                if raw_ltp <= 0:
                    if (not stage1_policy["allow_seed_pass"]) or (offmarket_seed_used >= offmarket_seed_cap):
                        continue

                    offmarket_seed_used += 1
                    ws_soft_pass_count += 1

                    rec["sym_risk"] = sym_risk
                    rec["snapshot"] = {
                        "ltp": 0.0,
                        "volume": 0,
                        "day_high": 0.0,
                        "day_low": 0.0,
                        "bid": 0.0,
                        "ask": 0.0,
                        "prev_close": 0.0,
                        "last_tick_time": 0.0,
                        "spread_pct": 0.0,
                        "intraday_range_pct": 0.0,
                        "traded_value": 0.0,
                        "ws_missing_or_stale": True,
                        "offmarket_seed_pass": True,
                    }
                    survivors.append(rec)
                    continue

            price_scale = 1.0
            if raw_ltp > 0:
                raw_in_range = cfg["min_price"] <= raw_ltp <= cfg["max_price"]
                paise_in_range = cfg["min_price"] <= (raw_ltp / 100.0) <= cfg["max_price"]
                if (not raw_in_range) and paise_in_range:
                    price_scale = 100.0

            ltp = raw_ltp / price_scale if raw_ltp > 0 else 0.0
            day_high = raw_day_high / price_scale if raw_day_high > 0 else ltp
            day_low = raw_day_low / price_scale if raw_day_low > 0 else ltp
            bid = raw_bid / price_scale if raw_bid > 0 else 0.0
            ask = raw_ask / price_scale if raw_ask > 0 else 0.0
            prev_close = raw_prev_close / price_scale if raw_prev_close > 0 else ltp

            vol = raw_volume
            last_tick_time = snap_ts

            if ltp <= 0 or ltp < cfg["min_price"] or ltp > cfg["max_price"]:
                diagnostics["exclusions_summary"]["price_filtered_count"] += 1
                continue

            if vol <= 0:
                if not stage1_policy["allow_soft_liquidity_pass"]:
                    diagnostics["exclusions_summary"]["illiquid_filtered_count"] += 1
                    continue
                vol = min_volume
                ws_soft_pass_count += 1

            traded_value = ltp * max(vol, 1)
            if traded_value < min_traded_value:
                if not stage1_policy["allow_soft_liquidity_pass"]:
                    diagnostics["exclusions_summary"]["illiquid_filtered_count"] += 1
                    continue
                traded_value = float(min_traded_value)
                ws_soft_pass_count += 1

            has_real_day_range = (
                ("day_high" in snap and "day_low" in snap)
                and raw_day_high > 0
                and raw_day_low > 0
                and raw_day_high > raw_day_low
            )

            intraday_range_pct = ((day_high - day_low) / ltp) if ltp > 0 else 0.0

            if has_real_day_range and intraday_range_pct < min_range_pct:
                if live_market:
                    diagnostics["exclusions_summary"]["flat_filtered_count"] += 1
                    continue

            normalized_snap = dict(snap)
            normalized_snap["ltp"] = ltp
            normalized_snap["day_high"] = day_high
            normalized_snap["day_low"] = day_low
            normalized_snap["bid"] = bid
            normalized_snap["ask"] = ask
            normalized_snap["prev_close"] = prev_close

            spread_pct = self._estimate_spread_pct(normalized_snap)
            if spread_pct > max_spread_pct:
                if live_market:
                    diagnostics["exclusions_summary"]["spread_filtered_count"] += 1
                    continue

            if (not ws_missing_or_stale) and self._is_halt_or_near_circuit(
                ltp, day_high, day_low, prev_close, cfg["near_circuit_pct"], last_tick_time
            ):
                diagnostics["exclusions_summary"]["halt_circuit_filtered_count"] += 1
                continue

            rec["sym_risk"] = sym_risk
            rec["snapshot"] = {
                "ltp": ltp,
                "volume": vol,
                "day_high": day_high,
                "day_low": day_low,
                "bid": bid,
                "ask": ask,
                "prev_close": prev_close,
                "last_tick_time": last_tick_time,
                "spread_pct": spread_pct,
                "intraday_range_pct": intraday_range_pct,
                "traded_value": traded_value,
                "ws_missing_or_stale": ws_missing_or_stale,
            }
            survivors.append(rec)

        target_max = cfg["stage1_target_max"]
        if len(survivors) > target_max:
            survivors.sort(
                key=lambda x: (
                    _safe_float(x["snapshot"].get("traded_value"), 0.0),
                    _safe_float(x["snapshot"].get("intraday_range_pct"), 0.0),
                ),
                reverse=True,
            )
            survivors = survivors[:target_max]

        if ws_missing_or_stale_count:
            diagnostics["notes"].append(
                f"Stage1 snapshot-only mode: {ws_missing_or_stale_count} symbols had missing/stale WS data."
            )
        if (not live_market) and ws_soft_pass_count:
            diagnostics["notes"].append(
                f"OFFMARKET_TEST soft-passed {ws_soft_pass_count} symbols with incomplete WS liquidity/range fields."
            )
        if (not live_market) and offmarket_seed_used:
            diagnostics["notes"].append(
                f"OFFMARKET_TEST seeded {offmarket_seed_used} WS-missing symbols into Stage2 shortlist path."
            )

        return survivors

    def _run_stage2_deep_scan(
        self,
        survivors: List[Dict[str, Any]],
        snapshots: Dict[str, Dict[str, Any]],
        market_state: Dict[str, Any],
        diagnostics: Dict[str, Any],
        mode_ctx: Dict[str, Any],
    ) -> List[ScanCandidate]:
        regime = market_state["market_context_state"].get("macro_regime", "TRENDING")
        sector_board = market_state.get("sector_scoreboard", [])
        sector_score_map = self._build_sector_score_map(sector_board)
        risk_map = market_state.get("symbol_risk_map", {})
        weights = self.config["weights"].get(regime, self.config["weights"]["TRENDING"])
        lookback = self.config["stage2"]["lookback_candles"]

        live_market = bool(mode_ctx.get("live_market"))

        stage2_fetch_cap = self._stage2_fetch_cap(mode_ctx, len(survivors))

        if live_market:
            if len(survivors) > stage2_fetch_cap:
                diagnostics["notes"].append(
                    f"Stage2 candle fetch capped to {stage2_fetch_cap} symbols during live market to avoid rate limits."
                )
        else:
            if len(survivors) > stage2_fetch_cap:
                diagnostics["notes"].append(
                    f"OFFMARKET_TEST Stage2 candle fetch capped to {stage2_fetch_cap} symbols."
                )

        survivors = survivors[:stage2_fetch_cap]

        ranked: List[Dict[str, Any]] = []
        candle_failures = 0

        for rec in survivors:
            symbol = rec["symbol"]
            token = rec["token"]
            exchange = rec.get("exchange", "NSE")
            sector = rec.get("sector", "UNKNOWN")
            snap = rec.get("snapshot", {})

            # ---- SAFE CANDLE FETCH PATCH ----
            candles = []

            try:
                candles = self._get_candles_cached(symbol, token, exchange, lookback)
                if live_market:
                    time.sleep(0.35)
            except Exception:
                candles = []

            # Live fallback: if this symbol has no usable candles, skip it,
            # but do not let the whole cycle collapse if we still have last good candidates.
            if not candles or len(candles) < 12:
                candle_failures += 1
                continue

            score = self._compute_symbol_score(
                symbol=symbol,
                token=token,
                exchange=exchange,
                sector=sector,
                candles=candles,
                snapshot=snap,
                regime=regime,
                market_state=market_state["market_context_state"],
                sector_score=sector_score_map.get(sector, 0.0),
                symbol_risk=str(risk_map.get(symbol, rec.get("sym_risk", "NORMAL"))).upper(),
                weights=weights,
            )

            if live_market:
                cooldown_ok, cooldown_meta = self._cooldown_check(symbol, score.composite_score)
                if not cooldown_ok:
                    continue
            else:
                cooldown_ok = True
                cooldown_meta = {
                    "cooldown_active": False,
                    "persistence_count": 0,
                    "last_score": round(self._last_scores.get(symbol, 0.0), 6),
                    "offmarket_cooldown_bypassed": True,
                }

            tier = self._assign_tier(score, regime, snap)
            candidate = ScanCandidate(
                symbol=symbol,
                token=token,
                exchange=exchange,
                sector=sector,
                tier=tier,
                regime_tag_used=regime,
                composite_score=score.composite_score,
                component_scores=score.to_dict(),
                snapshot={
                    "ltp": round(_safe_float(snap.get("ltp"), 0.0), 4),
                    "volume": _safe_int(snap.get("volume"), 0),
                    "last_tick_time": _safe_float(snap.get("last_tick_time", snap.get("ts")), 0.0),
                    "day_high": round(_safe_float(snap.get("day_high"), 0.0), 4),
                    "day_low": round(_safe_float(snap.get("day_low"), 0.0), 4),
                    "bid": round(_safe_float(snap.get("bid"), 0.0), 4),
                    "ask": round(_safe_float(snap.get("ask"), 0.0), 4),
                    "prev_close": round(_safe_float(snap.get("prev_close"), 0.0), 4),
                    "spread_pct": _safe_float(snap.get("spread_pct"), 0.0),
                    "intraday_range_pct": _safe_float(snap.get("intraday_range_pct"), 0.0),
                    "traded_value": _safe_float(snap.get("traded_value"), 0.0),
                },
                meta={
                    "source_flags": rec.get("source_flags", []),
                    "candles_5m": candles,
                    **cooldown_meta,
                },
            )
            candidate_dict = candidate.to_dict()
            candidate_dict["candles_5m"] = candles
            ranked.append(candidate_dict)


        if candle_failures:
            diagnostics["notes"].append(f"Candle data insufficient/failed for {candle_failures} survivors.")
            if candle_failures >= max(5, int(len(survivors) * 0.25)):
                diagnostics["degraded"] = True

        ranked.sort(
            key=lambda c: (
                c.get("composite_score", 0.0),
                c.get("component_scores", {}).get("whale_score", 0.0),
                 c.get("component_scores", {}).get("volume_score", 0.0),
            ),
            reverse=True,
        )

        if live_market and not ranked and candle_failures > 0 and self._last_candidates:
            diagnostics["notes"].append(
                "Reusing last good live candidates due to Stage2 candle fetch failure/rate limit."
            )
            return self._last_candidates[: self.config["ranking"]["top_combined"]]

        tier1_limit = self.config["ranking"]["tier1_count"]
        tier1_assigned = 0
        final_ranked: List[Dict[str, Any]] = []
        for c in ranked:
            if c.get("tier") == "TIER1" and tier1_assigned < tier1_limit:
                tier1_assigned += 1
                final_ranked.append(c)
            elif c.get("tier") == "TIER1" and tier1_assigned >= tier1_limit:
                c["tier"] = "TIER2"
                final_ranked.append(c)
            else:
                final_ranked.append(c)

        diagnostics["top_sectors_for_scan"] = self._top_sector_names(sector_board)
        return final_ranked

    # --------------------------------------------------------
    # Scoring
    # --------------------------------------------------------

    def _compute_symbol_score(
        self,
        symbol: str,
        token: str,
        exchange: str,
        sector: str,
        candles: List[Dict[str, Any]],
        snapshot: Dict[str, Any],
        regime: str,
        market_state: Dict[str, Any],
        sector_score: float,
        symbol_risk: str,
        weights: Dict[str, float],
    ) -> CandidateScore:
        closes = [_safe_float(c["close"]) for c in candles]
        highs = [_safe_float(c["high"]) for c in candles]
        lows = [_safe_float(c["low"]) for c in candles]
        volumes = [_safe_float(c.get("volume"), 0.0) for c in candles]

        ema_fast = _ema(closes, self.config["stage2"]["ema_fast"])
        ema_mid = _ema(closes, self.config["stage2"]["ema_mid"])
        ema_slow = _ema(closes, max(50, min(len(closes), self.config["stage2"]["ema_slow"])))
        rsi = _rsi(closes, self.config["stage2"]["rsi_period"])
        atr = _atr(candles, self.config["stage2"]["atr_period"])
        bb_upper, bb_mid, bb_lower = _bollinger(
            closes,
            self.config["stage2"]["bb_period"],
            self.config["stage2"]["bb_mult"],
        )

        close = closes[-1]
        prev_close = closes[-2] if len(closes) >= 2 else close
        last_vol = volumes[-1] if volumes else 0.0
        avg_vol_20 = _avg(volumes[-20:], 1.0)
        atr_last = atr[-1] if atr else 0.0
        range_last = highs[-1] - lows[-1] if highs and lows else 0.0

        # Trend score
        trend_alignment = 0.0
        if ema_fast and ema_mid and ema_slow:
            if ema_fast[-1] > ema_mid[-1] > ema_slow[-1]:
                trend_alignment = 1.0
            elif ema_fast[-1] < ema_mid[-1] < ema_slow[-1]:
                trend_alignment = 0.9
            else:
                trend_alignment = 0.35

        vwap_proxy = _avg(closes[-10:], close)
        vwap_relation = _normalize_score(abs(close - vwap_proxy) / max(close, 1e-9), 0.0, 0.02)
        momentum_pct = abs((close - prev_close) / max(prev_close, 1e-9))
        trend_score = (
            0.50 * trend_alignment +
            0.25 * _normalize_score(momentum_pct, 0.001, 0.02) +
            0.25 * vwap_relation
        )

        # Mean reversion score
        rsi_last = rsi[-1] if rsi else 50.0
        bb_pos = 0.5
        if bb_upper and bb_lower:
            denom = max(bb_upper[-1] - bb_lower[-1], 1e-9)
            bb_pos = _clamp((close - bb_lower[-1]) / denom, 0.0, 1.0)

        rsi_extreme = 1.0 if (rsi_last <= 30 or rsi_last >= 70) else _normalize_score(abs(rsi_last - 50), 5, 25)
        bb_edge = max(abs(bb_pos - 0.5) * 2.0, 0.0)
        contraction = 0.0
        if bb_upper and bb_lower and bb_mid:
            width_now = (bb_upper[-1] - bb_lower[-1]) / max(bb_mid[-1], 1e-9)
            width_prev = ((bb_upper[-2] - bb_lower[-2]) / max(bb_mid[-2], 1e-9)) if len(bb_mid) >= 2 else width_now
            if width_prev > 0:
                contraction = _normalize_score((width_prev - width_now), -0.01, 0.02)
        mr_score = (0.45 * rsi_extreme) + (0.35 * bb_edge) + (0.20 * contraction)

        # Volatility / speed
        atr_pct = atr_last / max(close, 1e-9)
        range_pct = range_last / max(close, 1e-9)
        acceleration = abs(closes[-1] - closes[-3]) / max(closes[-3], 1e-9) if len(closes) >= 3 else 0.0
        vol_score = (
            0.40 * _normalize_score(atr_pct, 0.003, 0.03) +
            0.35 * _normalize_score(range_pct, 0.003, 0.025) +
            0.25 * _normalize_score(acceleration, 0.001, 0.02)
        )

        # Volume
        vol_spike = last_vol / max(avg_vol_20, 1.0)
        vol_acceleration = _avg(volumes[-3:], 0.0) / max(_avg(volumes[-10:-3], 1.0), 1.0) if len(volumes) >= 10 else 1.0
        volume_score = (
            0.65 * _normalize_score(vol_spike, 1.0, 4.0) +
            0.35 * _normalize_score(vol_acceleration, 1.0, 3.0)
        )

        # Whale proxy
        tick_acc_proxy = _normalize_score(acceleration, 0.001, 0.015)
        vwap_disp = _normalize_score(abs(close - vwap_proxy) / max(close, 1e-9), 0.0, 0.015)
        whale_score = (
            0.35 * _normalize_score(vol_spike, 1.2, 4.0) +
            0.25 * _normalize_score(range_pct, 0.004, 0.03) +
            0.20 * tick_acc_proxy +
            0.20 * vwap_disp
        )

        # Context bonuses
        sector_bonus = _clamp(sector_score, 0.0, 1.0)

        macro_bonus = 0.0
        macro_bias = market_state.get("macro_bias", "NEUTRAL")
        divergence_flag = market_state.get("divergence_flag", "NONE")
        vol_state = market_state.get("volatility_state", "NORMAL")

        directional_up = ema_fast[-1] >= ema_mid[-1] if ema_fast and ema_mid else False
        directional_down = ema_fast[-1] < ema_mid[-1] if ema_fast and ema_mid else False

        if macro_bias == "BULLISH" and directional_up:
            macro_bonus += 0.55
        elif macro_bias == "BEARISH" and directional_down:
            macro_bonus += 0.55
        else:
            macro_bonus += 0.20

        if regime == "HIGH_VOL" and vol_state == "HIGH":
            macro_bonus += 0.20
        elif regime == "LOW_VOL" and vol_state == "LOW":
            macro_bonus += 0.20
        else:
            macro_bonus += 0.10

        if divergence_flag == "HIGH":
            macro_bonus -= 0.20
        elif divergence_flag == "MILD":
            macro_bonus -= 0.08

        macro_bonus = _clamp(macro_bonus, 0.0, 1.0)

        # Penalties
        risk_penalty = 0.0
        if symbol_risk == "CAUTION":
            risk_penalty = 0.10

        spread_penalty = _clamp(snapshot.get("spread_pct", 0.0) / 0.01, 0.0, 0.20)

        # Persistence
        persistence_bonus = self._persistence_score(symbol)

        composite = (
            (weights["trend"] * trend_score) +
            (weights["mr"] * mr_score) +
            (weights["vol"] * vol_score) +
            (weights["volume"] * volume_score) +
            (weights["whale"] * whale_score) +
            (weights["sector"] * sector_bonus) +
            (weights["macro"] * macro_bonus) +
            (weights["persistence"] * persistence_bonus) -
            risk_penalty -
            spread_penalty
        )

        composite = round(_clamp(composite, 0.0, 1.5), 6)

        return CandidateScore(
            trend_score=round(trend_score, 6),
            mr_score=round(mr_score, 6),
            vol_score=round(vol_score, 6),
            volume_score=round(volume_score, 6),
            whale_score=round(whale_score, 6),
            sector_bonus=round(sector_bonus, 6),
            macro_bonus=round(macro_bonus, 6),
            risk_penalty=round(risk_penalty, 6),
            spread_penalty=round(spread_penalty, 6),
            persistence_bonus=round(persistence_bonus, 6),
            composite_score=composite,
        )

    def _assign_tier(self, score: CandidateScore, regime: str, snapshot: Dict[str, Any]) -> str:
        base = score.composite_score
        spread = snapshot.get("spread_pct", 0.0)
        vol = snapshot.get("intraday_range_pct", 0.0)

        if (
            base >= 0.62 and
            score.whale_score >= 0.35 and
            score.volume_score >= 0.35 and
            spread <= 0.0045 and
            vol >= 0.009
        ):
            return "TIER1"

        if regime == "RANGING" and score.mr_score >= 0.55 and spread <= 0.004:
            return "TIER1"

        return "TIER2"

    # --------------------------------------------------------
    # Market / Data Access
    # --------------------------------------------------------

    def _get_market_context(self) -> Dict[str, Any]:
        """
        Expected provider contract:
        market_context_provider.get_state() -> {
            "market_context_state": {...},
            "sector_scoreboard": [...],
            "symbol_risk_map": {...},
        }
        """
        state = self.market_context_provider.get_state()
        return {
            "market_context_state": state.get("market_context_state", {}),
            "sector_scoreboard": state.get("sector_scoreboard", []),
            "symbol_risk_map": state.get("symbol_risk_map", {}),
        }

    def _get_universe_snapshots(self, universe: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], bool]:
        """
        Expected pipeline options:
        - get_snapshots_bulk(list_of_tokens_or_keys)
        - get_snapshot(symbol=..., token=..., exchange=...)
        - snapshot_store dict-like

        Runtime note:
        - Our live Module 02 pipeline is token-centric for snapshot retrieval.
        - Scanner still stores snapshots under "EXCHANGE:SYMBOL" keys internally.
        """
        stale = False
        snapshots: Dict[str, Dict[str, Any]] = {}

        try:
            # Build stable internal key map first
            keyed_universe: List[Tuple[str, str, str, str]] = []
            bulk_items: List[Dict[str, str]] = []

            for u in universe:
                sym = str(u.get("symbol", "")).strip()
                ex = str(u.get("exchange", "NSE")).strip() or "NSE"
                tk = str(u.get("token", "")).strip()
                if not sym:
                    continue

                snap_key = f"{ex}:{sym}"
                keyed_universe.append((snap_key, sym, ex, tk))

                bulk_items.append({
                    "token": tk,
                    "symbol": sym,
                    "exchange": ex,
                })

            # Prefer bulk fetch if available, preserving the same EXCHANGE:SYMBOL keys.
            if hasattr(self.pipeline, "get_snapshots_bulk") and bulk_items:
                bulk_snaps = self.pipeline.get_snapshots_bulk(bulk_items) or {}

                if isinstance(bulk_snaps, dict):
                    for snap_key, sym, ex, tk in keyed_universe:
                        snap = (
                            bulk_snaps.get(snap_key)
                            or bulk_snaps.get(tk)
                            or bulk_snaps.get(str(tk))
                            or bulk_snaps.get(sym)
                            or {}
                        )
                        snapshots[snap_key] = snap

            else:
                for snap_key, sym, ex, tk in keyed_universe:
                    snap = {}

                    if hasattr(self.pipeline, "get_snapshot"):
                        # First preference: token-centric lookup
                        if tk:
                            try:
                                snap = self.pipeline.get_snapshot(tk) or {}
                            except TypeError:
                                # Fallback for alternate pipeline signature
                                snap = self.pipeline.get_snapshot(symbol=sym, token=tk, exchange=ex) or {}
                        else:
                            try:
                                snap = self.pipeline.get_snapshot(symbol=sym, token=tk, exchange=ex) or {}
                            except TypeError:
                                snap = {}

                    elif hasattr(self.pipeline, "snapshot_store"):
                        if tk:
                            snap = (
                                self.pipeline.snapshot_store.get(tk, {})
                                or self.pipeline.snapshot_store.get(str(tk), {})
                                or self.pipeline.snapshot_store.get(snap_key, {})
                                or self.pipeline.snapshot_store.get(sym, {})
                            )
                        else:
                            snap = (
                                self.pipeline.snapshot_store.get(snap_key, {})
                                or self.pipeline.snapshot_store.get(sym, {})
                            )

                    snapshots[snap_key] = snap or {}

        except Exception:
            stale = True

        # staleness check
        now = _now_ts()
        fresh_count = 0
        for v in snapshots.values():
            last_tick = _safe_float(v.get("ts", v.get("last_tick_time", 0.0)), 0.0)
            if last_tick > 0 and (now - last_tick) <= 180:
                fresh_count += 1

        if not snapshots or fresh_count < max(5, int(len(universe) * 0.2)):
            stale = True

        return snapshots, stale

    def _get_candles(self, symbol: str, token: str, exchange: str, lookback: int) -> List[Dict[str, Any]]:
        """
        Supports both:
        1) candle_provider.get_5m_candles(...)
        2) broker wrapper with candle_provider.smart.getCandleData(...)
        Returns normalized list[dict] with keys:
        time, open, high, low, close, volume
        """
        data = None

        # Path 1: dedicated candle adapter
        if hasattr(self.candle_provider, "get_5m_candles"):
            data = self.candle_provider.get_5m_candles(
                symbol=symbol,
                token=token,
                exchange=exchange,
                lookback=lookback,
            )

        # Path 2: broker wrapper -> SmartAPI historical candles
        elif hasattr(self.candle_provider, "smart") and hasattr(self.candle_provider.smart, "getCandleData"):
            from datetime import datetime, timedelta

            now = datetime.now()
            weekday = now.weekday()  # Mon=0 ... Sun=6

            # Weekend -> use last Friday session
            if weekday == 5:   # Saturday
                session_day = now.date() - timedelta(days=1)
            elif weekday == 6: # Sunday
                session_day = now.date() - timedelta(days=2)
            else:
                session_day = now.date()

            market_open = datetime.combine(session_day, datetime.strptime("09:15", "%H:%M").time())
            market_close = datetime.combine(session_day, datetime.strptime("15:30", "%H:%M").time())

            # If today is a weekday but outside market hours, use full session
            if weekday < 5:
                if now < market_open:
                    to_dt = market_close
                    from_dt = market_open
                elif now > market_close:
                    to_dt = market_close
                    from_dt = market_open
                else:
                    to_dt = now
                    from_dt = max(market_open, to_dt - timedelta(minutes=max(lookback * 10, 600)))
            else:
               # Weekend always use last completed session
                to_dt = market_close
                from_dt = market_open

            params = {
                "exchange": exchange,
                "symboltoken": str(token),
                "interval": "FIVE_MINUTE",
                "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
                "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
           }

            resp = self.candle_provider.smart.getCandleData(params)

            if isinstance(resp, dict):
                data = resp.get("data") or resp.get("candles") or []
            else:
                data = resp

        else:
            raise AttributeError(
                "No candle fetch path found. Expected get_5m_candles() or smart.getCandleData()."
            )

        if data is None:
            return []

        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                data = data["data"]
            elif isinstance(data.get("candles"), list):
                data = data["candles"]
            else:
                return []

        if not isinstance(data, list):
            return []

        normalized: List[Dict[str, Any]] = []

        for row in data:
            if isinstance(row, dict):
                if "close" in row:
                    normalized.append({
                        "time": row.get("time"),
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                        "volume": row.get("volume", 0),
                    })

            elif isinstance(row, (list, tuple)) and len(row) >= 6:
                normalized.append({
                    "time": row[0],
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                })

        return normalized

    def _get_candles_cached(self, symbol: str, token: str, exchange: str, lookback: int) -> List[Dict[str, Any]]:
        token = str(token or "").strip()
        if not token:
            return []

        key = f"{exchange}:{symbol}:{lookback}"
        now = _now_ts()

        cached = self._candle_cache.get(key)

        # Fresh good cache
        if cached and cached.get("data") and (now - cached.get("ts", 0.0) <= self._candle_cache_ttl_sec):
            return cached.get("data", [])

        data = []
        try:
            data = self._get_candles(symbol, token, exchange, lookback)
        except Exception:
            data = []

        # Good fresh payload -> cache and return
        if data and len(data) >= 12:
            self._candle_cache[key] = {
                "ts": now,
                "data": data,
            }
            return data

        # Reuse stale last-good cache on failure/rate-limit
        if cached and cached.get("data"):
            return cached.get("data", [])

        return []

    def _fetch_daily_movers(self, runtime_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        movers: List[Dict[str, Any]] = []
        try:
            cached = self._safe_json_read("daily_movers_cache.json", default=[])
            if isinstance(cached, list):
                for x in cached:
                    movers.append(_as_symbol_record(x))
        except Exception:
            pass
        return movers

    def _fetch_daily_losers(self, runtime_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        losers: List[Dict[str, Any]] = []
        try:
            cached = self._safe_json_read("daily_losers_cache.json", default=[])
            if isinstance(cached, list):
                for x in cached:
                    losers.append(_as_symbol_record(x))
        except Exception:
            pass
        return losers

    def _safe_json_read(self, name: str, default: Any) -> Any:
        try:
            if hasattr(self.maintenance_provider, "read_json"):
                val = self.maintenance_provider.read_json(name)
                return default if val is None else val
            if hasattr(self.maintenance_provider, "get_json"):
                val = self.maintenance_provider.get_json(name)
                return default if val is None else val
        except Exception:
            return default
        return default

    def _get_runtime_mappings(self, runtime_cfg: Dict[str, Any], taxonomy: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        mapping: Dict[str, Dict[str, Any]] = {}

        # Preferred direct sources from maintenance / runtime
        for key in (
            "symbol_token_map",
            "token_symbol_map",
            "equity_symbol_map",
            "resolved_symbol_mapping",
        ):
            block = runtime_cfg.get(key, {})
            if isinstance(block, dict):
                for k, v in block.items():
                    if isinstance(v, dict):
                        symbol = str(v.get("symbol", "")).strip() or k.split(":")[-1]
                        exchange = str(v.get("exchange", "NSE")).strip() or "NSE"
                        token = str(v.get("token", "")).strip()
                        sector = v.get("sector", "UNKNOWN")

                        if symbol and token:
                            record = {
                                "symbol": symbol,
                                "exchange": exchange,
                                "token": token,
                                "sector": sector,
                            }
                            mapping[k] = record
                            mapping[symbol] = record
                            mapping[f"{exchange}:{symbol}"] = record

        # Project mapping fallback: symbol_to_token.json
        sym_to_tok = self._safe_json_read("symbol_to_token.json", default={})
        if isinstance(sym_to_tok, dict):
            for raw_key, raw_token in sym_to_tok.items():
                raw_key_str = str(raw_key).strip()
                token = str(raw_token).strip() if raw_token is not None else ""
                if not raw_key_str or not token:
                    continue

                if ":" in raw_key_str:
                    exchange, symbol = raw_key_str.split(":", 1)
                    exchange = exchange.strip() or "NSE"
                    symbol = symbol.strip()
                else:
                    exchange = "NSE"
                    symbol = raw_key_str

                if not symbol:
                    continue

                sector = mapping.get(symbol, {}).get("sector", "UNKNOWN")
                record = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "token": token,
                    "sector": sector,
                }
                mapping[raw_key_str] = record
                mapping[symbol] = record
                mapping[f"{exchange}:{symbol}"] = record

        # Sector enrichment from watchlist / potential lists
        for fname in ("watchlist.json", "potential_stocks.json"):
            items = self._safe_json_read(fname, default=[])
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol", "")).strip()
                    exchange = str(item.get("exchange", "NSE")).strip() or "NSE"
                    sector = str(item.get("sector", "UNKNOWN")).strip() or "UNKNOWN"

                    if not symbol:
                        continue

                    mapping.setdefault(symbol, {
                        "symbol": symbol,
                        "exchange": exchange,
                        "token": "",
                        "sector": sector,
                    })
                    mapping[symbol]["sector"] = sector

                    mapping.setdefault(f"{exchange}:{symbol}", {
                        "symbol": symbol,
                        "exchange": exchange,
                        "token": mapping[symbol].get("token", ""),
                        "sector": sector,
                    })
                    mapping[f"{exchange}:{symbol}"]["sector"] = sector

        # Taxonomy fallback
        if isinstance(taxonomy, dict):
            sectors = taxonomy.get("symbol_sector_map", {})
            if isinstance(sectors, dict):
                for k, sec in sectors.items():
                    mapping.setdefault(k, {"symbol": k.split(":")[-1], "exchange": "NSE", "token": "", "sector": sec})
                    mapping[k]["sector"] = sec

        return mapping

    # --------------------------------------------------------
    # Persistence / Cooldown
    # --------------------------------------------------------

    def _persistence_score(self, symbol: str) -> float:
        q = self._shortlist_history.get(symbol, deque())
        max_scans = self.config["ranking"]["persistence_lookback_scans"]
        if not q:
            return 0.0
        recent_hits = min(len(q), max_scans)
        return _clamp(recent_hits / max_scans, 0.0, 1.0)

    def _cooldown_check(self, symbol: str, new_score: float) -> Tuple[bool, Dict[str, Any]]:
        cooldown_sec = self.config["ranking"]["cooldown_min"] * 60
        q = self._shortlist_history.get(symbol, deque())
        now = _now_ts()

        while q and (now - q[0]) > (cooldown_sec * 3):
            q.popleft()

        if not q:
            return True, {"cooldown_active": False, "persistence_count": 0}

        last_seen = q[-1]
        recently_seen = (now - last_seen) < cooldown_sec
        last_score = self._last_scores.get(symbol, 0.0)

        if recently_seen and new_score < (last_score + 0.06):
            return False, {
                "cooldown_active": True,
                "persistence_count": len(q),
                "last_score": round(last_score, 6),
            }

        return True, {
            "cooldown_active": recently_seen,
            "persistence_count": len(q),
            "last_score": round(last_score, 6),
        }

    def _update_persistence_and_cooldown(self, selected: List[Dict[str, Any]], scan_ts: float) -> None:
        for c in selected:
            symbol = str(c.get("symbol", "")).strip()
            if not symbol:
                continue

            q = self._shortlist_history[symbol]
            q.append(scan_ts)

            maxlen = max(10, self.config["ranking"]["persistence_lookback_scans"] * 2)
            while len(q) > maxlen:
                q.popleft()

            self._last_scores[symbol] = _safe_float(c.get("composite_score", 0.0), 0.0)

    # --------------------------------------------------------
    # Diagnostics / Finalize
    # --------------------------------------------------------

    def _finalize_health(self, scan_ts: float, degraded: bool, error: Optional[str]) -> None:
        self.health.last_scan_ts = scan_ts
        self.health.last_scan_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(scan_ts))
        self.health.degraded = degraded
        self.health.last_error = error
        self.health.scan_count += 1
        self.health.state = "DEGRADED" if degraded else "OK"

    # --------------------------------------------------------
    # Mode Control (COMMON / LIVE / OFFMARKET)
    # --------------------------------------------------------

    def _mode_context(self) -> Dict[str, Any]:
        live_market = self._is_live_market()
        return {
            "live_market": live_market,
            "offmarket_test": not live_market,
            "mode_name": "LIVE" if live_market else "OFFMARKET_TEST",
        }

    def _stage1_policy(self, mode_ctx: Dict[str, Any]) -> Dict[str, Any]:
        live_market = bool(mode_ctx.get("live_market"))

        if live_market:
            return {
                "allow_seed_pass": False,
                "allow_soft_liquidity_pass": True,
                "strict_snapshot_required": True,
            }

        return {
            "allow_seed_pass": True,
            "allow_soft_liquidity_pass": True,
            "strict_snapshot_required": False,
        }

    def _stage2_fetch_cap(self, mode_ctx: Dict[str, Any], survivor_count: int) -> int:
        live_market = bool(mode_ctx.get("live_market"))

        if live_market:
            return min(3, self.config["ranking"]["top_combined"], survivor_count)

        return min(
            max(self.config["ranking"]["top_combined"], 15),
            20,
            survivor_count,
        )

    def _is_live_market(self) -> bool:
        now = time.time() + (5.5 * 60 * 60)  # IST
        lt = time.gmtime(now)

        weekday = lt.tm_wday
        hour = lt.tm_hour
        minute = lt.tm_min

        if weekday >= 5:
            return False
        if hour < 9 or hour > 15:
            return False
        if hour == 9 and minute < 15:
            return False
        if hour == 15 and minute > 30:
            return False
        return True

    # --------------------------------------------------------
    # Utilities
    # --------------------------------------------------------

    def _extract_snapshot_volume(self, snap: Dict[str, Any]) -> int:
        for key in (
            "volume",
            "day_volume",
            "dayVolume",
            "total_volume",
            "totalVolume",
            "traded_volume",
            "tradedVolume",
            "volume_traded_today",
            "totalTradedVolume",
            "vol",
        ):
            v = _safe_int(snap.get(key), 0)
            if v > 0:
                return v
        return 0

    def _estimate_spread_pct(self, snap: Dict[str, Any]) -> float:
        bid = _safe_float(snap.get("bid"), 0.0)
        ask = _safe_float(snap.get("ask"), 0.0)
        ltp = _safe_float(snap.get("ltp"), 0.0)

        if bid > 0 and ask > 0 and ask >= bid:
            mid = (bid + ask) / 2.0
            return (ask - bid) / max(mid, 1e-9)

        # fallback proxy
        day_high = _safe_float(snap.get("day_high"), ltp)
        day_low = _safe_float(snap.get("day_low"), ltp)
        rng = max(day_high - day_low, 0.0)
        if ltp <= 0:
            return 1.0
        proxy = min((rng / ltp) * 0.10, 0.02)
        return proxy

    def _is_halt_or_near_circuit(
        self,
        ltp: float,
        day_high: float,
        day_low: float,
        prev_close: float,
        near_circuit_pct: float,
        last_tick_time: float,
    ) -> bool:
        now = _now_ts()

        if last_tick_time > 0 and (now - last_tick_time) > 600:
            return True

        if ltp <= 0 or prev_close <= 0:
            return True

        # crude abnormal print / freeze proxy
        up_move = abs((day_high - prev_close) / prev_close)
        dn_move = abs((prev_close - day_low) / prev_close)

        if up_move >= 0.19 or dn_move >= 0.19:
            if abs(day_high - ltp) / max(ltp, 1e-9) < near_circuit_pct:
                return True
            if abs(ltp - day_low) / max(ltp, 1e-9) < near_circuit_pct:
                return True

        return False

    def _build_sector_score_map(self, sector_board: Any) -> Dict[str, float]:
        score_map: Dict[str, float] = {}

        if isinstance(sector_board, dict):
            if isinstance(sector_board.get("rows"), list):
                sector_board = sector_board["rows"]
            elif isinstance(sector_board.get("sector_rankings"), list):
                sector_board = sector_board["sector_rankings"]

        if isinstance(sector_board, list):
            for item in sector_board:
                if isinstance(item, dict):
                    name = str(item.get("sector", item.get("name", ""))).strip()
                    score = _safe_float(item.get("score", item.get("strength", 0.0)), 0.0)
                    if name:
                        score_map[name] = _clamp(score, 0.0, 1.0)

        elif isinstance(sector_board, dict):
            for k, v in sector_board.items():
                if isinstance(v, dict):
                    score_map[k] = _clamp(_safe_float(v.get("score", 0.0), 0.0), 0.0, 1.0)
                else:
                    score_map[k] = _clamp(_safe_float(v, 0.0), 0.0, 1.0)

        return score_map

    def _top_sector_names(self, sector_board: Any, top_n: int = 5) -> List[str]:
        pairs: List[Tuple[str, float]] = []

        if isinstance(sector_board, dict):
            if isinstance(sector_board.get("rows"), list):
                sector_board = sector_board["rows"]
            elif isinstance(sector_board.get("sector_rankings"), list):
                sector_board = sector_board["sector_rankings"]

        if isinstance(sector_board, list):
            for item in sector_board:
                if isinstance(item, dict):
                    name = str(item.get("sector", item.get("name", ""))).strip()
                    score = _safe_float(item.get("score", item.get("strength", 0.0)), 0.0)
                    if name:
                        pairs.append((name, score))

        elif isinstance(sector_board, dict):
            for k, v in sector_board.items():
                if isinstance(v, dict):
                    pairs.append((k, _safe_float(v.get("score", 0.0), 0.0)))
                else:
                    pairs.append((k, _safe_float(v, 0.0)))

        pairs.sort(key=lambda x: x[1], reverse=True)
        return [p[0] for p in pairs[:top_n]]

    def _deep_merge(self, a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(a)
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = self._deep_merge(out[k], v)
            else:
                out[k] = v
        return out

    @staticmethod
    def _validation_check_offmarket_missing_token_survival() -> Dict[str, Any]:
        class _Broker:
            def __init__(self) -> None:
                self.calls: List[Dict[str, Any]] = []

            def ensure_logged_in(self) -> bool:
                return True

            def get_tokens(self) -> Tuple[str, str, str]:
                return ("OFFMARKET_AUTH", "OFFMARKET_REFRESH", "OFFMARKET_FEED")

            def get_5m_candles(
                self,
                *,
                symbol: str,
                token: str,
                exchange: str = "NSE",
                lookback: int = 80,
            ) -> List[Dict[str, Any]]:
                self.calls.append(
                    {
                        "symbol": str(symbol),
                        "token": str(token),
                        "exchange": str(exchange),
                        "lookback": int(lookback),
                    }
                )
                base = 100.0 + (sum(ord(ch) for ch in f"{exchange}:{symbol}") % 50)
                out: List[Dict[str, Any]] = []
                now = int(time.time())
                for idx in range(max(lookback, 12)):
                    close = round(base + (idx * 0.20), 2)
                    out.append(
                        {
                            "time": now - ((lookback - idx) * 300),
                            "open": round(close - 0.30, 2),
                            "high": round(close + 0.50, 2),
                            "low": round(close - 0.40, 2),
                            "close": close,
                            "volume": 100000 + (idx * 1000),
                        }
                    )
                return out

        class _Pipeline:
            def get_snapshots_bulk(self, items: List[Any]) -> Dict[str, Dict[str, Any]]:
                out: Dict[str, Dict[str, Any]] = {}
                now = _now_ts()
                for item in items or []:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol", "")).strip().upper()
                    exchange = str(item.get("exchange", "NSE")).strip() or "NSE"
                    token = str(item.get("token", "")).strip()
                    if not symbol:
                        continue
                    row = {
                        "token": token,
                        "symbol": symbol,
                        "exchange": exchange,
                        "snapshot_key": f"{exchange}:{symbol}",
                        "ltp": 150.0,
                        "volume": 250000.0,
                        "last_tick_time": now,
                        "ts": now,
                        "bid": 149.8,
                        "ask": 150.2,
                        "day_high": 151.5,
                        "day_low": 148.5,
                        "prev_close": 149.0,
                    }
                    if token:
                        out[token] = row
                    out[row["snapshot_key"]] = row
                    out[symbol] = row
                return out

        class _Maintenance:
            def read_json(self, name: str) -> Any:
                if name == "watchlist.json":
                    return [
                        {"symbol": "RELIANCE", "exchange": "NSE", "sector": "ENERGY", "token": ""},
                        {"symbol": "INFY", "exchange": "NSE", "sector": "IT", "token": "1594"},
                    ]
                if name in {
                    "potential_stocks.json",
                    "daily_movers_cache.json",
                    "daily_losers_cache.json",
                    "manual_additions.json",
                    "discovered_symbols.json",
                    "previous_day_movers.json",
                    "previous_day_losers.json",
                }:
                    return []
                if name == "runtime_config.json":
                    return {}
                if name == "index_taxonomy.json":
                    return {}
                if name == "symbol_to_token.json":
                    return {}
                return None

        class _MarketContext:
            def get_state(self) -> Dict[str, Any]:
                return {
                    "market_context_state": {
                        "trade_gate": "ALLOW",
                        "macro_regime": "RANGING",
                        "macro_bias": "NEUTRAL",
                    },
                    "sector_scoreboard": {
                        "sectors": {},
                        "symbol_to_sector": {
                            "RELIANCE": "ENERGY",
                            "INFY": "IT",
                        },
                    },
                    "symbol_risk_map": {},
                }

        broker = _Broker()
        scanner = StockUniverseBuilderScanner(
            broker=broker,
            pipeline=_Pipeline(),
            market_context_provider=_MarketContext(),
            maintenance_provider=_Maintenance(),
            candle_provider=broker,
            config={
                "stage1": {
                    "min_price": 1.0,
                    "max_price": 100000.0,
                    "min_volume": 1,
                    "min_traded_value": 1.0,
                    "min_range_pct": 0.0,
                    "max_spread_pct": 1.0,
                    "stage1_target_min": 1,
                    "stage1_target_max": 5,
                },
                "ranking": {
                    "top_combined": 3,
                },
            },
        )
        scanner._is_live_market = lambda: False  # type: ignore[attr-defined]
        out = scanner.run_scan()
        diag = out.get("scanner_diagnostics", {})
        blank_token_fetches = [
            call for call in broker.calls
            if not str(call.get("token", "")).strip()
        ]
        return {
            "blank_token_candle_fetch_blocked": len(blank_token_fetches) == 0,
            "offmarket_candidates_survive_with_valid_rows": len(out.get("candidate_list") or []) > 0,
            "missing_mapping_count_recorded": int(
                ((diag.get("exclusions_summary") or {}).get("missing_mapping_count", 0))
            ) >= 1,
        }


# ============================================================
# Optional thin adapter examples
# ============================================================

class SimpleMaintenanceProvider:
    """
    Reads JSON files from a base directory.
    """
    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir.rstrip("/")

    def read_json(self, name: str) -> Any:
        path = f"{self.base_dir}/{name}"
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


class SimpleMarketContextProvider:
    """
    Wrap Module 03 if it exposes a .get_state() method.
    """
    def __init__(self, module03: Any) -> None:
        self.module03 = module03

    def get_state(self) -> Dict[str, Any]:
        if hasattr(self.module03, "get_state"):
            return self.module03.get_state()
        return {
            "market_context_state": {},
            "sector_scoreboard": [],
            "symbol_risk_map": {},
        }


# ============================================================
# Example usage pattern
# ============================================================

if __name__ == "__main__":
    print("Module 04 loaded. Instantiate StockUniverseBuilderScanner(...) and call run_scan().")
