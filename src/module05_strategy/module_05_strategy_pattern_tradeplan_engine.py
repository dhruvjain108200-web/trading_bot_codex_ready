# MODULE 05 â€” Strategy + Pattern + Trade Plan Engine
# Purpose:
# Convert ranked candidates from Module 04 into structured trade plans.
# Compatible with:
# - Module 02 snapshot store
# - Module 03 market context
# - Module 04 candidate scanner
# - Module 06 alert engine
# - Module 07 maintenance / housekeeping
#
# Optimized for Google Colab runtime stability:
# - bounded caches
# - no heavy external deps
# - defensive missing-data handling
# - optional 1m / 15m confirmation
# - snapshot freshness guard

from __future__ import annotations

import math
import time
import statistics
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# CONFIG
# ============================================================

@dataclass
class Module05Config:
    max_primary_signals: int = 3
    max_sector_signals: int = 2

    # Freshness / validity
    snapshot_stale_seconds: int = 20
    allow_stale_snapshot_offmarket: bool = True
    validity_window_candles: int = 2
    candle_interval_minutes: int = 5

    # Minimum bars needed
    min_bars_5m: int = 60
    min_bars_15m: int = 30
    min_bars_1m: int = 20

    # Liquidity guards
    min_avg_volume: float = 50000
    min_atr_pct: float = 0.35
    max_spread_pct: float = 0.60
    allow_liquidity_proxy_offmarket: bool = True

    # Indicator lengths
    ema_fast: int = 20
    ema_mid: int = 50
    ema_slow: int = 200
    rsi_length: int = 14
    atr_length: int = 14
    bb_length: int = 20
    bb_std: float = 2.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    volume_avg_length: int = 20
    consolidation_lookback: int = 8

    # Relative strength thresholds
    rs_positive_threshold: float = 0.20
    rs_negative_threshold: float = -0.20

    # Setup scoring thresholds
    min_quality_score: float = 55.0
    min_confidence_score: float = 58.0

    # Confidence penalties / boosts
    caution_penalty: float = 8.0
    avoid_penalty: float = 100.0
    weak_sector_penalty: float = 8.0
    strong_sector_boost: float = 8.0
    whale_boost_cap: float = 12.0
    stale_penalty: float = 100.0

    # Risk-reward controls
    min_rr_for_signal: float = 1.20
    default_t2_rr: float = 2.0
    pullback_t1_rr: float = 1.20
    breakout_t1_rr: float = 1.50
    mean_reversion_t1_rr: float = 1.10
    crash_hunter_t1_rr: float = 1.30

    # Entry precision
    allow_1m_precision: bool = True
    allow_15m_confirmation: bool = True

    # Caching
    cache_ttl_seconds: int = 240
    max_cache_symbols: int = 300

    # Off-market / validation mode
    enable_offmarket_test_plans: bool = True
    offmarket_test_relax_quality: bool = False
    offmarket_test_relax_confidence: bool = False

    # Relaxed mean-reversion trigger for weekend / historical validation only
    test_mr_long_rsi: float = 48.0
    test_mr_short_rsi: float = 52.0
    test_mr_band_proximity_pct: float = 2.0

    # Tagging / diagnostics
    include_debug_reject_meta: bool = True


# ============================================================
# DATA CONTRACTS
# ============================================================

@dataclass
class Candle:
    ts: Any
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class CandidateInput:
    symbol: str
    exchange: str = "NSE"
    token: Optional[str] = None

    # Module 04 scores
    trend_score: float = 0.0
    momentum_score: float = 0.0
    volatility_score: float = 0.0
    volume_score: float = 0.0
    whale_score: float = 0.0

    component_scores: Dict[str, float] = field(default_factory=dict)

    # Snapshot fields from Module 04 / Module 02
    ltp: Optional[float] = None
    last_tick_time: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None

    # Optional metadata
    sector: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MarketContext:
    market_regime: str = "CHOPPY"   # TRENDING / FALLING / CHOPPY / CRASHING etc.
    macro_bias: str = "NEUTRAL"
    trade_gate: str = "OPEN"        # OPEN / PAUSE
    sector_scoreboard: Dict[str, Any] = field(default_factory=dict)
    symbol_risk_map: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Snapshot:
    ltp: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    last_tick_time: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradePlan:
    symbol: str
    token: Optional[str]
    direction: str
    setup_name: str
    entry_type: str
    entry_trigger: float
    stop_loss: float
    T1: float
    T2: Optional[float]
    confidence_score: float
    expected_move: float
    reasons: List[str]
    warnings: List[str]
    validity_window: str

    # Extended fields
    sector: Optional[str] = None
    quality_score: float = 0.0
    rr_t1: float = 0.0
    rr_t2: Optional[float] = None
    relative_strength: float = 0.0
    market_regime: Optional[str] = None
    risk_flag: Optional[str] = None
    timestamp: float = 0.0
    expires_at: float = 0.0
    pattern_confirmations: List[str] = field(default_factory=list)
    indicator_snapshot: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyDiagnostics:
    scan_time: float = 0.0
    candidates_evaluated: int = 0
    signals_generated: int = 0
    rejected_signals_count: int = 0
    top_setup_type: Optional[str] = None
    reject_reasons_breakdown: Dict[str, int] = field(default_factory=dict)
    cache_hits: int = 0
    cache_misses: int = 0
    market_regime: Optional[str] = None
    trade_gate: Optional[str] = None


# ============================================================
# HELPERS
# ============================================================

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _mean(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else default


def _std(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return default
    try:
        return statistics.pstdev(vals)
    except Exception:
        return default


def _last(values: List[float], default: float = 0.0) -> float:
    return values[-1] if values else default


# ============================================================
# INDICATOR ENGINE
# ============================================================

class IndicatorEngine:
    @staticmethod
    def close_series(candles: List[Candle]) -> List[float]:
        return [_safe_float(c.close) for c in candles]

    @staticmethod
    def high_series(candles: List[Candle]) -> List[float]:
        return [_safe_float(c.high) for c in candles]

    @staticmethod
    def low_series(candles: List[Candle]) -> List[float]:
        return [_safe_float(c.low) for c in candles]

    @staticmethod
    def volume_series(candles: List[Candle]) -> List[float]:
        return [_safe_float(c.volume) for c in candles]

    @staticmethod
    def ema(values: List[float], length: int) -> List[float]:
        if not values:
            return []
        if length <= 1:
            return values[:]

        alpha = 2 / (length + 1)
        out = [values[0]]
        for i in range(1, len(values)):
            out.append(alpha * values[i] + (1 - alpha) * out[-1])
        return out

    @staticmethod
    def sma(values: List[float], length: int) -> List[float]:
        if length <= 0:
            return values[:]
        out = []
        running = 0.0
        for i, v in enumerate(values):
            running += v
            if i >= length:
                running -= values[i - length]
            denom = min(i + 1, length)
            out.append(running / denom)
        return out

    @staticmethod
    def rsi(values: List[float], length: int = 14) -> List[float]:
        if len(values) < 2:
            return [50.0] * len(values)

        gains = [0.0]
        losses = [0.0]
        for i in range(1, len(values)):
            diff = values[i] - values[i - 1]
            gains.append(max(diff, 0.0))
            losses.append(abs(min(diff, 0.0)))

        avg_gain = IndicatorEngine.sma(gains, length)
        avg_loss = IndicatorEngine.sma(losses, length)

        out = []
        for g, l in zip(avg_gain, avg_loss):
            if l == 0:
                out.append(100.0 if g > 0 else 50.0)
            else:
                rs = g / l
                out.append(100 - (100 / (1 + rs)))
        return out

    @staticmethod
    def atr(candles: List[Candle], length: int = 14) -> List[float]:
        if not candles:
            return []

        trs = []
        prev_close = None
        for c in candles:
            h = _safe_float(c.high)
            l = _safe_float(c.low)
            cl = _safe_float(c.close)

            if prev_close is None:
                tr = h - l
            else:
                tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            trs.append(max(tr, 0.0))
            prev_close = cl

        return IndicatorEngine.sma(trs, length)

    @staticmethod
    def macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[float]]:
        ema_fast = IndicatorEngine.ema(values, fast)
        ema_slow = IndicatorEngine.ema(values, slow)
        macd_line = [a - b for a, b in zip(ema_fast, ema_slow)]
        signal_line = IndicatorEngine.ema(macd_line, signal)
        hist = [a - b for a, b in zip(macd_line, signal_line)]
        return {
            "macd": macd_line,
            "signal": signal_line,
            "hist": hist,
        }

    @staticmethod
    def bollinger(values: List[float], length: int = 20, std_mult: float = 2.0) -> Dict[str, List[float]]:
        mid = IndicatorEngine.sma(values, length)
        upper = []
        lower = []

        for i in range(len(values)):
            start = max(0, i - length + 1)
            window = values[start:i + 1]
            s = _std(window, default=0.0)
            upper.append(mid[i] + std_mult * s)
            lower.append(mid[i] - std_mult * s)

        width = [u - l for u, l in zip(upper, lower)]
        return {
            "mid": mid,
            "upper": upper,
            "lower": lower,
            "width": width,
        }

    @staticmethod
    def rolling_max(values: List[float], length: int) -> List[float]:
        out = []
        for i in range(len(values)):
            start = max(0, i - length + 1)
            out.append(max(values[start:i + 1]))
        return out

    @staticmethod
    def rolling_min(values: List[float], length: int) -> List[float]:
        out = []
        for i in range(len(values)):
            start = max(0, i - length + 1)
            out.append(min(values[start:i + 1]))
        return out

    @staticmethod
    def vwap(candles: List[Candle]) -> List[float]:
        out = []
        cum_pv = 0.0
        cum_vol = 0.0
        for c in candles:
            typical = (_safe_float(c.high) + _safe_float(c.low) + _safe_float(c.close)) / 3.0
            vol = max(_safe_float(c.volume), 0.0)
            cum_pv += typical * vol
            cum_vol += vol
            out.append(cum_pv / cum_vol if cum_vol > 0 else typical)
        return out


# ============================================================
# PATTERN ENGINE
# ============================================================

class PatternEngine:
    @staticmethod
    def detect_last(candles: List[Candle]) -> List[str]:
        if len(candles) < 2:
            return []

        c = candles[-1]
        p = candles[-2]

        o = _safe_float(c.open)
        h = _safe_float(c.high)
        l = _safe_float(c.low)
        cl = _safe_float(c.close)

        po = _safe_float(p.open)
        pc = _safe_float(p.close)

        body = abs(cl - o)
        rng = max(h - l, 1e-9)
        upper_wick = h - max(o, cl)
        lower_wick = min(o, cl) - l

        patterns = []

        # Doji
        if body / rng <= 0.10:
            patterns.append("DOJI")

        # Hammer
        if lower_wick > body * 2.0 and upper_wick <= body * 1.2 and cl >= o:
            patterns.append("HAMMER")

        # Shooting Star
        if upper_wick > body * 2.0 and lower_wick <= body * 1.2 and o >= cl:
            patterns.append("SHOOTING_STAR")

        # Bullish engulfing
        if pc < po and cl > o and cl >= po and o <= pc:
            patterns.append("BULLISH_ENGULFING")

        # Bearish engulfing
        if pc > po and cl < o and o >= pc and cl <= po:
            patterns.append("BEARISH_ENGULFING")

        return patterns


# ============================================================
# MODULE 05 CORE
# ============================================================

class Module05StrategyPatternTradePlanEngine:
    def __init__(self, config: Optional[Module05Config] = None):
        self.config = config or Module05Config()
        self._indicator_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    # --------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------

    def _build_explain_payload(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "symbol": plan.get("symbol"),
            "direction": plan.get("direction"),
            "setup_name": plan.get("setup_name"),
            "entry_type": plan.get("entry_type"),
            "market_regime": plan.get("market_regime"),
            "sector": plan.get("sector"),
            "quality_score": plan.get("quality_score"),
            "confidence_score": plan.get("confidence_score"),
            "rr_t1": plan.get("rr_t1"),
            "rr_t2": plan.get("rr_t2"),
            "reasons": list(plan.get("reasons", []) or []),
            "warnings": list(plan.get("warnings", []) or []),
            "pattern_confirmations": list(plan.get("pattern_confirmations", []) or []),
            "indicator_snapshot": dict(plan.get("indicator_snapshot", {}) or {}),
            "validity_window": plan.get("validity_window"),
            "timestamp": plan.get("timestamp"),
            "expires_at": plan.get("expires_at"),
        }

    def run_scan(
        self,
        candidate_list: List[Dict[str, Any]],
        candidate_backup_list: Optional[List[Dict[str, Any]]] = None,
        market_context: Optional[Dict[str, Any]] = None,
        snapshot_store: Optional[Dict[str, Dict[str, Any]]] = None,
        candle_store_5m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        candle_store_1m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        candle_store_15m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        start_ts = time.time()
        now_ts = now_ts or start_ts

        market = self._parse_market_context(market_context or {})
        diagnostics = StrategyDiagnostics(
            market_regime=market.market_regime,
            trade_gate=market.trade_gate,
        )

        # Gate from Module 03
        if str(market.trade_gate).upper() == "PAUSE":
            diagnostics.scan_time = round(time.time() - start_ts, 4)
            return {
                "trade_signal_list": [],
                "backup_signal_list": [],
                "trade_plan_map": {},
                "backup_plan_map": {},
                "explain_map": {},
                "backup_explain_map": {},
                "plan_count": 0,
                "errors": [],
                "strategy_diagnostics": asdict(diagnostics),
            }

        parsed_candidates = [self._parse_candidate(c) for c in (candidate_list or [])]
        parsed_backups = [self._parse_candidate(c) for c in (candidate_backup_list or [])]

        primary_plans: List[TradePlan] = []
        backup_plans: List[TradePlan] = []
        reject_counts: Dict[str, int] = {}
        sector_counts: Dict[str, int] = {}
        errors: List[str] = []

        # Evaluate primary first
        for candidate in parsed_candidates:
            diagnostics.candidates_evaluated += 1

            try:
                result = self._evaluate_candidate(
                    candidate=candidate,
                    market=market,
                    snapshot_store=snapshot_store or {},
                    candle_store_5m=candle_store_5m or {},
                    candle_store_1m=candle_store_1m or {},
                    candle_store_15m=candle_store_15m or {},
                    now_ts=now_ts,
                )

                if result["accepted"]:
                    plan: TradePlan = result["trade_plan"]
                    sec = plan.sector or "UNKNOWN"
                    if sector_counts.get(sec, 0) >= self.config.max_sector_signals:
                        reject_counts["sector_crowding"] = reject_counts.get("sector_crowding", 0) + 1
                        diagnostics.rejected_signals_count += 1
                        continue

                    primary_plans.append(plan)
                    sector_counts[sec] = sector_counts.get(sec, 0) + 1
                else:
                    reason = result.get("reject_reason", "unknown")
                    reject_counts[reason] = reject_counts.get(reason, 0) + 1
                    diagnostics.rejected_signals_count += 1

            except Exception as e:
                sym = getattr(candidate, "symbol", None) or "UNKNOWN"
                errors.append(f"{sym}: {type(e).__name__}: {e}")
                reject_counts["evaluation_error"] = reject_counts.get("evaluation_error", 0) + 1
                diagnostics.rejected_signals_count += 1
                continue

        # Rank and keep top 3
        primary_plans = self._rank_trade_plans(primary_plans)[: self.config.max_primary_signals]

        # Build backup list from overflow + backup candidates
        if len(primary_plans) < self.config.max_primary_signals:
            already_symbols = {p.symbol for p in primary_plans}
            for candidate in parsed_backups:
                if candidate.symbol in already_symbols:
                    continue

                try:
                    result = self._evaluate_candidate(
                        candidate=candidate,
                        market=market,
                        snapshot_store=snapshot_store or {},
                        candle_store_5m=candle_store_5m or {},
                        candle_store_1m=candle_store_1m or {},
                        candle_store_15m=candle_store_15m or {},
                        now_ts=now_ts,
                    )
                    if result["accepted"]:
                        backup_plans.append(result["trade_plan"])

                except Exception as e:
                    sym = getattr(candidate, "symbol", None) or "UNKNOWN"
                    errors.append(f"{sym}: {type(e).__name__}: {e}")
                    continue

        # If we had more than 3 good primarys, rest become backup
        if len(primary_plans) >= self.config.max_primary_signals:
            extra_candidates = self._rank_trade_plans([
                p for p in self._rank_trade_plans(primary_plans + backup_plans)
                if p.symbol not in {x.symbol for x in primary_plans}
            ])
            backup_plans = extra_candidates

        backup_plans = self._rank_trade_plans(backup_plans)

        diagnostics.signals_generated = len(primary_plans)
        diagnostics.reject_reasons_breakdown = reject_counts
        diagnostics.cache_hits = self._cache_hits
        diagnostics.cache_misses = self._cache_misses
        diagnostics.top_setup_type = primary_plans[0].setup_name if primary_plans else None
        diagnostics.scan_time = round(time.time() - start_ts, 4)

        trade_signal_list = [asdict(p) for p in primary_plans]
        backup_signal_list = [asdict(p) for p in backup_plans]

        trade_plan_map = {
            str(plan["symbol"]).strip().upper(): plan
            for plan in trade_signal_list
            if plan.get("symbol")
        }
        backup_plan_map = {
            str(plan["symbol"]).strip().upper(): plan
            for plan in backup_signal_list
            if plan.get("symbol")
        }
        explain_map = {
            symbol: self._build_explain_payload(plan)
            for symbol, plan in trade_plan_map.items()
        }
        backup_explain_map = {
            symbol: self._build_explain_payload(plan)
            for symbol, plan in backup_plan_map.items()
        }

        return {
            "trade_signal_list": trade_signal_list,
            "backup_signal_list": backup_signal_list,
            "trade_plan_map": trade_plan_map,
            "backup_plan_map": backup_plan_map,
            "plan_count": len(primary_plans),
            "explain_map": explain_map,
            "backup_explain_map": backup_explain_map,           
            "errors": errors,
            "strategy_diagnostics": asdict(diagnostics),
        }

    def build_on_demand_plan_for_candidate(
        self,
        candidate: Dict[str, Any],
        market_context: Optional[Dict[str, Any]] = None,
        snapshot_store: Optional[Dict[str, Dict[str, Any]]] = None,
        candle_store_5m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        candle_store_1m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        candle_store_15m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        now_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        now_ts = now_ts or time.time()
        parsed_candidate = self._parse_candidate(candidate or {})
        if not parsed_candidate.symbol:
            return {"ok": False, "reason": "missing_symbol"}

        market = self._parse_market_context(market_context or {})
        snapshot = self._get_snapshot(parsed_candidate, snapshot_store or {})
        if not self._snapshot_is_fresh(snapshot, now_ts):
            return {"ok": False, "reason": "stale_snapshot"}

        symbol = parsed_candidate.symbol
        candles_5m = self._normalize_candles((candle_store_5m or {}).get(symbol, []))
        if len(candles_5m) < self.config.min_bars_5m:
            return {"ok": False, "reason": "insufficient_5m_data"}

        candles_1m = self._normalize_candles((candle_store_1m or {}).get(symbol, []))
        candles_15m = self._normalize_candles((candle_store_15m or {}).get(symbol, []))

        indicator_bundle = self._get_indicator_bundle(
            symbol=symbol,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            candles_15m=candles_15m,
            now_ts=now_ts,
        )

        mode_ctx = self._mode_context(now_ts)
        liquidity_ok, liquidity_warnings, spread_pct = self._liquidity_validation(
            snapshot=snapshot,
            candles_5m=candles_5m,
            indicators=indicator_bundle,
            offmarket=bool(mode_ctx.get("offmarket")),
        )
        if not liquidity_ok:
            return {"ok": False, "reason": "liquidity_fail"}

        risk_flag = self._resolve_risk_flag(symbol, market.symbol_risk_map)
        if risk_flag == "AVOID":
            return {"ok": False, "reason": "risk_avoid"}

        sector_name = self._resolve_sector(parsed_candidate, market)
        relative_strength = self._compute_relative_strength(
            candles_5m=candles_5m,
            sector_name=sector_name,
            sector_scoreboard=market.sector_scoreboard,
        )

        setup_eval = self._evaluate_setup(
            strategy=self._select_strategy(market.market_regime),
            candidate=parsed_candidate,
            snapshot=snapshot,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            candles_15m=candles_15m,
            indicators=indicator_bundle,
            market=market,
            sector_name=sector_name,
            relative_strength=relative_strength,
            now_ts=now_ts,
        )
        if not setup_eval.get("valid"):
            return {"ok": False, "reason": setup_eval.get("reject_reason", "setup_fail")}

        quality_score = self._quality_score(
            candidate=parsed_candidate,
            setup_eval=setup_eval,
            market=market,
            sector_name=sector_name,
            spread_pct=spread_pct,
            risk_flag=risk_flag,
            relative_strength=relative_strength,
        )
        confidence_score = self._confidence_score(
            candidate=parsed_candidate,
            setup_eval=setup_eval,
            market=market,
            sector_name=sector_name,
            relative_strength=relative_strength,
            risk_flag=risk_flag,
            spread_pct=spread_pct,
        )
        quality_score, confidence_score = self._apply_mode_score_adjustments(
            quality_score=quality_score,
            confidence_score=confidence_score,
            mode_ctx=mode_ctx,
        )

        trade_plan = self._build_trade_plan(
            candidate=parsed_candidate,
            market=market,
            snapshot=snapshot,
            setup_eval=setup_eval,
            sector_name=sector_name,
            relative_strength=relative_strength,
            quality_score=quality_score,
            confidence_score=confidence_score,
            risk_flag=risk_flag,
            liquidity_warnings=liquidity_warnings,
            now_ts=now_ts,
        )

        threshold_profile = self._threshold_profile(mode_ctx)
        eligibility_reasons: List[str] = []
        if quality_score < threshold_profile["quality_threshold"]:
            eligibility_reasons.append("quality_below_threshold")
        if confidence_score < threshold_profile["confidence_threshold"]:
            eligibility_reasons.append("confidence_below_threshold")
        if trade_plan.rr_t1 < threshold_profile["rr_threshold"]:
            eligibility_reasons.append("rr_too_low")

        plan_dict = asdict(trade_plan)
        return {
            "ok": True,
            "plan": plan_dict,
            "explain_payload": self._build_explain_payload(plan_dict),
            "signal_eligible": len(eligibility_reasons) == 0,
            "eligibility_reasons": eligibility_reasons,
        }

    # --------------------------------------------------------
    # PARSERS
    # --------------------------------------------------------

    def _parse_candidate(self, raw: Dict[str, Any]) -> CandidateInput:
        snapshot = raw.get("snapshot", {}) or {}
        component_scores = raw.get("component_scores", {}) or {}

        return CandidateInput(
            symbol=str(raw.get("symbol", "")).strip(),
            exchange=str(raw.get("exchange", "NSE")).strip() or "NSE",
            token=raw.get("token"),

            trend_score=_safe_float(
                raw.get("trend_score",
                component_scores.get("trend_score",
                component_scores.get("trend", 0.0)))
            ),
            momentum_score=_safe_float(
                raw.get("momentum_score",
                component_scores.get("mr_score",
                component_scores.get("momentum", 0.0)))
            ),
            volatility_score=_safe_float(
                raw.get("volatility_score",
                component_scores.get("vol_score",
                component_scores.get("volatility", 0.0)))
            ),
            volume_score=_safe_float(
                raw.get("volume_score",
                component_scores.get("volume_score",
                component_scores.get("volume", 0.0)))
           ),
            whale_score=_safe_float(
                raw.get("whale_score",
                component_scores.get("whale_score",
                component_scores.get("whale", 0.0)))
           ),

           component_scores=component_scores,

           ltp=raw.get("ltp", snapshot.get("ltp")),
           last_tick_time=raw.get("last_tick_time", snapshot.get("last_tick_time", snapshot.get("ts"))),
           bid=raw.get("bid", snapshot.get("bid")),
           ask=raw.get("ask", snapshot.get("ask")),

           sector=raw.get("sector"),
           meta=raw.get("meta", {}) or {},
       )

    def _parse_market_context(self, raw: Dict[str, Any]) -> MarketContext:
        state = raw.get("market_context_state", raw) or {}

        macro_regime = str(state.get("macro_regime", state.get("market_regime", "CHOPPY"))).upper()
        macro_bias = str(state.get("macro_bias", "NEUTRAL")).upper()
        trade_gate = str(state.get("trade_gate", raw.get("trade_gate", "OPEN"))).upper()

        regime_map = {
            "RANGING": "CHOPPY",
            "SIDEWAYS": "CHOPPY",
            "TRENDING": "TRENDING",
            "BULLISH": "TRENDING",
            "BEARISH": "TRENDING",
            "CHOPPY": "CHOPPY",
        }

        return MarketContext(
            market_regime=regime_map.get(macro_regime, macro_regime),
            macro_bias=macro_bias,
            trade_gate=trade_gate,
            sector_scoreboard=raw.get("sector_scoreboard", {}) or {},
            symbol_risk_map=raw.get("symbol_risk_map", {}) or {},
        )

    def _normalize_candles(self, raw_candles: List[Dict[str, Any]]) -> List[Candle]:
        out = []
        for c in raw_candles or []:
            try:
                out.append(Candle(
                    ts=c.get("ts") or c.get("time") or c.get("datetime"),
                    open=_safe_float(c.get("open")),
                    high=_safe_float(c.get("high")),
                    low=_safe_float(c.get("low")),
                    close=_safe_float(c.get("close")),
                    volume=_safe_float(c.get("volume")),
                ))
            except Exception:
                continue
        return out

    # --------------------------------------------------------
    # CANDIDATE EVALUATION
    # --------------------------------------------------------

    def _evaluate_candidate(
        self,
        candidate: CandidateInput,
        market: MarketContext,
        snapshot_store: Dict[str, Dict[str, Any]],
        candle_store_5m: Dict[str, List[Dict[str, Any]]],
        candle_store_1m: Dict[str, List[Dict[str, Any]]],
        candle_store_15m: Dict[str, List[Dict[str, Any]]],
        now_ts: float,
    ) -> Dict[str, Any]:
        symbol = candidate.symbol
        if not symbol:
            return {"accepted": False, "reject_reason": "missing_symbol"}

        mode_ctx = self._mode_context(now_ts)
        threshold_profile = self._threshold_profile(mode_ctx)
        offmarket = bool(mode_ctx.get("offmarket"))

        snapshot = self._get_snapshot(candidate, snapshot_store)
        if not self._snapshot_is_fresh(snapshot, now_ts):
            return {
                "accepted": False,
                "reject_reason": "stale_snapshot",
            }

        candles_5m = self._normalize_candles(candle_store_5m.get(symbol, []))
        if len(candles_5m) < self.config.min_bars_5m:
            return {"accepted": False, "reject_reason": "insufficient_5m_data"}

        candles_1m = self._normalize_candles(candle_store_1m.get(symbol, []))
        candles_15m = self._normalize_candles(candle_store_15m.get(symbol, []))

        indicator_bundle = self._get_indicator_bundle(
            symbol=symbol,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            candles_15m=candles_15m,
            now_ts=now_ts,
        )

        liquidity_ok, liquidity_warnings, spread_pct = self._liquidity_validation(
            snapshot=snapshot,
            candles_5m=candles_5m,
            indicators=indicator_bundle,
            offmarket=offmarket,
        )
        if not liquidity_ok:
            return {"accepted": False, "reject_reason": "liquidity_fail"}

        risk_flag = self._resolve_risk_flag(symbol, market.symbol_risk_map)
        if risk_flag == "AVOID":
            return {"accepted": False, "reject_reason": "risk_avoid"}

        sector_name = self._resolve_sector(candidate, market)
        relative_strength = self._compute_relative_strength(
            candles_5m=candles_5m,
            sector_name=sector_name,
            sector_scoreboard=market.sector_scoreboard,
        )

        strategy = self._select_strategy(market.market_regime)
        setup_eval = self._evaluate_setup(
            strategy=strategy,
            candidate=candidate,
            snapshot=snapshot,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            candles_15m=candles_15m,
            indicators=indicator_bundle,
            market=market,
            sector_name=sector_name,
            relative_strength=relative_strength,
            now_ts=now_ts,
        )

        if not setup_eval["valid"]:
            return {"accepted": False, "reject_reason": setup_eval.get("reject_reason", "setup_fail")}

        quality_score = self._quality_score(
            candidate=candidate,
            setup_eval=setup_eval,
            market=market,
            sector_name=sector_name,
            spread_pct=spread_pct,
            risk_flag=risk_flag,
            relative_strength=relative_strength,
        )

        confidence_score = self._confidence_score(
            candidate=candidate,
            setup_eval=setup_eval,
            market=market,
            sector_name=sector_name,
            relative_strength=relative_strength,
            risk_flag=risk_flag,
            spread_pct=spread_pct,
        )

        quality_score, confidence_score = self._apply_mode_score_adjustments(
            quality_score=quality_score,
            confidence_score=confidence_score,
            mode_ctx=mode_ctx,
        )

        if quality_score < threshold_profile["quality_threshold"]:
            return {"accepted": False, "reject_reason": "quality_below_threshold"}

        if confidence_score < threshold_profile["confidence_threshold"]:
            return {"accepted": False, "reject_reason": "confidence_below_threshold"}

        trade_plan = self._build_trade_plan(
            candidate=candidate,
            market=market,
            snapshot=snapshot,
            setup_eval=setup_eval,
            sector_name=sector_name,
            relative_strength=relative_strength,
            quality_score=quality_score,
            confidence_score=confidence_score,
            risk_flag=risk_flag,
            liquidity_warnings=liquidity_warnings,
            now_ts=now_ts,
        )

        if trade_plan.rr_t1 < threshold_profile["rr_threshold"]:
            return {"accepted": False, "reject_reason": "rr_too_low"}

        return {
            "accepted": True,
            "trade_plan": trade_plan,
        }

    # --------------------------------------------------------
    # SNAPSHOT / CACHING
    # --------------------------------------------------------

    def _get_snapshot(self, candidate: CandidateInput, snapshot_store: Dict[str, Dict[str, Any]]) -> Snapshot:
        snapshot_key = f"{candidate.exchange}:{candidate.symbol}" if candidate.symbol else ""
        raw_store = (
            snapshot_store.get(snapshot_key, {})
            or snapshot_store.get(candidate.symbol, {})
            or {}
        )

        store_tick = _safe_float(raw_store.get("last_tick_time", raw_store.get("ts")), 0.0)
        cand_tick = _safe_float(candidate.last_tick_time, 0.0)

        # Prefer the fresher source
        if cand_tick > store_tick:
            raw = {
                "ltp": candidate.ltp,
                "bid": candidate.bid,
                "ask": candidate.ask,
                "last_tick_time": candidate.last_tick_time,
                "ts": candidate.last_tick_time,
                "symbol": candidate.symbol,
                "exchange": candidate.exchange,
                "snapshot_key": snapshot_key,
            }
            raw.update(raw_store)
            raw["ltp"] = candidate.ltp if candidate.ltp is not None else raw_store.get("ltp")
            raw["bid"] = candidate.bid if candidate.bid is not None else raw_store.get("bid")
            raw["ask"] = candidate.ask if candidate.ask is not None else raw_store.get("ask")
            raw["last_tick_time"] = candidate.last_tick_time
            raw["ts"] = candidate.last_tick_time
        else:
            raw = dict(raw_store)
            raw["last_tick_time"] = raw.get("last_tick_time", raw.get("ts"))
            raw["ts"] = raw.get("ts", raw.get("last_tick_time"))

        return Snapshot(
            ltp=raw.get("ltp", candidate.ltp),
            bid=raw.get("bid", candidate.bid),
            ask=raw.get("ask", candidate.ask),
            volume=raw.get("volume"),
            last_tick_time=raw.get("last_tick_time", raw.get("ts", candidate.last_tick_time)),
            raw=raw,
        )

    @staticmethod
    def _validation_check_snapshot_contract() -> Dict[str, Any]:
        engine = Module05StrategyPatternTradePlanEngine.__new__(Module05StrategyPatternTradePlanEngine)
        candidate = CandidateInput(
            symbol="RELIANCE",
            exchange="NSE",
            token="2885",
            ltp=100.0,
            last_tick_time=100.0,
        )
        snapshot_store = {
            "NSE:RELIANCE": {
                "token": "2885",
                "symbol": "RELIANCE",
                "exchange": "NSE",
                "snapshot_key": "NSE:RELIANCE",
                "ltp": 101.25,
                "ts": 125.0,
                "last_tick_time": 125.0,
                "volume": 50,
            }
        }
        snapshot = engine._get_snapshot(candidate, snapshot_store)
        return {
            "module04_key_readable": snapshot.raw.get("snapshot_key") == "NSE:RELIANCE",
            "module02_fields_readable": snapshot.ltp == 101.25 and snapshot.last_tick_time == 125.0,
        }

    def _is_offmarket_context(self, now_ts: float) -> bool:
        ist_ts = now_ts + (5.5 * 60 * 60)
        lt = time.gmtime(ist_ts)

        weekday = lt.tm_wday
        hour = lt.tm_hour
        minute = lt.tm_min

        is_weekend = weekday >= 5
        before_open = (hour < 9) or (hour == 9 and minute < 15)
        after_close = (hour > 15) or (hour == 15 and minute > 30)

        return is_weekend or before_open or after_close

    # --------------------------------------------------------
    # MODE CONTROL (COMMON / LIVE / OFFMARKET)
    # --------------------------------------------------------

    def _mode_context(self, now_ts: float) -> Dict[str, Any]:
        offmarket = self.config.enable_offmarket_test_plans and self._is_offmarket_context(now_ts)
        return {
            "offmarket": offmarket,
            "mode_name": "OFFMARKET_TEST" if offmarket else "LIVE",
        }

    def _threshold_profile(self, mode_ctx: Dict[str, Any]) -> Dict[str, float]:
        offmarket = bool(mode_ctx.get("offmarket"))

        quality_threshold = self.config.min_quality_score
        confidence_threshold = self.config.min_confidence_score
        rr_threshold = self.config.min_rr_for_signal

        if offmarket and self.config.offmarket_test_relax_quality:
            quality_threshold = min(quality_threshold, 35.0)

        if offmarket and self.config.offmarket_test_relax_confidence:
            confidence_threshold = min(confidence_threshold, 38.0)

        if offmarket and self.config.enable_offmarket_test_plans:
            rr_threshold = min(rr_threshold, 1.0)

        return {
            "quality_threshold": quality_threshold,
            "confidence_threshold": confidence_threshold,
            "rr_threshold": rr_threshold,
        }

    def _apply_mode_score_adjustments(
        self,
        *,
        quality_score: float,
        confidence_score: float,
        mode_ctx: Dict[str, Any],
    ) -> Tuple[float, float]:
        offmarket = bool(mode_ctx.get("offmarket"))

        if offmarket and self.config.offmarket_test_relax_quality:
            quality_score = min(100.0, quality_score + 12.0)

        if offmarket and self.config.offmarket_test_relax_confidence:
            confidence_score = min(100.0, confidence_score + 14.0)

        return quality_score, confidence_score

    def _snapshot_is_fresh(self, snapshot: Snapshot, now_ts: float) -> bool:
        mode_ctx = self._mode_context(now_ts)
        offmarket = bool(mode_ctx.get("offmarket"))

        last_tick = snapshot.last_tick_time
        if last_tick is None:
            return offmarket and self.config.allow_stale_snapshot_offmarket

        try:
            age = now_ts - float(last_tick)
        except Exception:
            return offmarket and self.config.allow_stale_snapshot_offmarket

        if age <= self.config.snapshot_stale_seconds:
            return True

        if offmarket and self.config.allow_stale_snapshot_offmarket:
            return True

        return False

    def _get_indicator_bundle(
        self,
        symbol: str,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        now_ts: float,
    ) -> Dict[str, Any]:
        cached = self._indicator_cache.get(symbol)
        if cached and (now_ts - cached.get("_cache_ts", 0)) <= self.config.cache_ttl_seconds:
            self._cache_hits += 1
            return cached["bundle"]

        self._cache_misses += 1

        close_5 = IndicatorEngine.close_series(candles_5m)
        high_5 = IndicatorEngine.high_series(candles_5m)
        low_5 = IndicatorEngine.low_series(candles_5m)
        vol_5 = IndicatorEngine.volume_series(candles_5m)

        ema20_5 = IndicatorEngine.ema(close_5, self.config.ema_fast)
        ema50_5 = IndicatorEngine.ema(close_5, self.config.ema_mid)
        ema200_5 = IndicatorEngine.ema(close_5, self.config.ema_slow)
        rsi_5 = IndicatorEngine.rsi(close_5, self.config.rsi_length)
        atr_5 = IndicatorEngine.atr(candles_5m, self.config.atr_length)
        bb_5 = IndicatorEngine.bollinger(close_5, self.config.bb_length, self.config.bb_std)
        macd_5 = IndicatorEngine.macd(close_5, self.config.macd_fast, self.config.macd_slow, self.config.macd_signal)
        vwap_5 = IndicatorEngine.vwap(candles_5m)
        avg_vol_5 = IndicatorEngine.sma(vol_5, self.config.volume_avg_length)
        rolling_high = IndicatorEngine.rolling_max(high_5, self.config.consolidation_lookback)
        rolling_low = IndicatorEngine.rolling_min(low_5, self.config.consolidation_lookback)

        bundle = {
            "5m": {
                "close": close_5,
                "high": high_5,
                "low": low_5,
                "volume": vol_5,
                "ema20": ema20_5,
                "ema50": ema50_5,
                "ema200": ema200_5,
                "rsi": rsi_5,
                "atr": atr_5,
                "bb": bb_5,
                "macd": macd_5,
                "vwap": vwap_5,
                "avg_volume": avg_vol_5,
                "rolling_high": rolling_high,
                "rolling_low": rolling_low,
                "patterns": PatternEngine.detect_last(candles_5m),
            }
        }

        if candles_1m:
            close_1 = IndicatorEngine.close_series(candles_1m)
            bundle["1m"] = {
                "close": close_1,
                "ema20": IndicatorEngine.ema(close_1, min(20, max(3, len(close_1)))),
                "rsi": IndicatorEngine.rsi(close_1, min(14, max(3, len(close_1)))),
                "patterns": PatternEngine.detect_last(candles_1m),
            }

        if candles_15m:
            close_15 = IndicatorEngine.close_series(candles_15m)
            bundle["15m"] = {
                "close": close_15,
                "ema20": IndicatorEngine.ema(close_15, min(20, max(3, len(close_15)))),
                "ema50": IndicatorEngine.ema(close_15, min(50, max(5, len(close_15)))),
                "rsi": IndicatorEngine.rsi(close_15, min(14, max(3, len(close_15)))),
                "patterns": PatternEngine.detect_last(candles_15m),
            }

        self._indicator_cache[symbol] = {
            "_cache_ts": now_ts,
            "bundle": bundle,
        }

        # Bound cache size for Colab stability
        if len(self._indicator_cache) > self.config.max_cache_symbols:
            oldest_key = min(self._indicator_cache, key=lambda k: self._indicator_cache[k].get("_cache_ts", 0))
            self._indicator_cache.pop(oldest_key, None)

        return bundle

    # --------------------------------------------------------
    # FILTERS
    # --------------------------------------------------------

    def _liquidity_validation(
        self,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        indicators: Dict[str, Any],
        offmarket: bool = False,
    ) -> Tuple[bool, List[str], float]:
        warnings = []

        snap_ltp = _safe_float(snapshot.ltp, 0.0)
        bid = _safe_float(snapshot.bid, 0.0)
        ask = _safe_float(snapshot.ask, 0.0)

        # COMMON: derive a candle-based fallback price
        candle_ltp = _safe_float(candles_5m[-1].close, 0.0) if candles_5m else 0.0

        # LIVE: require real snapshot LTP
        # OFFMARKET_TEST: allow candle-derived LTP when snapshot is stale/missing
        if offmarket and self.config.allow_liquidity_proxy_offmarket:
            ltp = snap_ltp if snap_ltp > 0 else candle_ltp
            if snap_ltp <= 0 and candle_ltp > 0:
                warnings.append("proxy_ltp_used")
        else:
            ltp = snap_ltp

        if ltp <= 0:
            return False, ["invalid_ltp"], 999.0

        # Treat bid/ask as "real" only if they look like an actual live market book.
        has_real_bid_ask = (
            bid > 0
            and ask > 0
            and ask >= bid
            and abs(ask - bid) > 1e-9
        )

        using_proxy_liquidity = False

        if has_real_bid_ask:
            spread_pct = ((ask - bid) / ltp * 100.0) if ltp > 0 else 999.0
        else:
            using_proxy_liquidity = True

            recent_high = max((_safe_float(c.high, 0.0) for c in candles_5m[-5:]), default=ltp)
            recent_low = min((_safe_float(c.low, ltp) for c in candles_5m[-5:]), default=ltp)
            recent_range_pct = ((recent_high - recent_low) / ltp * 100.0) if ltp > 0 else 999.0

            spread_pct = min(recent_range_pct * 0.08, self.config.max_spread_pct)

            if offmarket and self.config.allow_liquidity_proxy_offmarket:
                warnings.append("proxy_spread_used")

        avg_vol = _last(indicators["5m"]["avg_volume"], 0.0)
        atr = _last(indicators["5m"]["atr"], 0.0)
        atr_pct = (atr / ltp * 100.0) if ltp > 0 else 0.0

        ok = True

        if spread_pct > self.config.max_spread_pct:
            ok = False
            warnings.append("wide_spread")

        if avg_vol < self.config.min_avg_volume:
            ok = False
            warnings.append("low_volume")

        if atr_pct < self.config.min_atr_pct:
            ok = False
            warnings.append("low_atr")

        # OFFMARKET_TEST: do not hard-fail on live-style liquidity checks
        # when we are already using candle/proxy liquidity.
        if offmarket and using_proxy_liquidity and self.config.allow_liquidity_proxy_offmarket:
            ok = True

        return ok, warnings, spread_pct

    def _resolve_risk_flag(self, symbol: str, symbol_risk_map: Dict[str, Any]) -> str:
        raw = symbol_risk_map.get(symbol, "NORMAL")
        if isinstance(raw, dict):
            raw = raw.get("risk_level", "NORMAL")
        return str(raw).upper()

    def _resolve_sector(self, candidate: CandidateInput, market: MarketContext) -> Optional[str]:
        if candidate.sector:
            return candidate.sector

        symbol_map = market.sector_scoreboard.get("symbol_to_sector", {})
        if isinstance(symbol_map, dict):
            return symbol_map.get(candidate.symbol)
        return None

    def _compute_relative_strength(
        self,
        candles_5m: List[Candle],
        sector_name: Optional[str],
        sector_scoreboard: Dict[str, Any],
    ) -> float:
        if len(candles_5m) < 6:
            return 0.0

        closes = IndicatorEngine.close_series(candles_5m)
        stock_ret = ((closes[-1] - closes[-6]) / closes[-6] * 100.0) if closes[-6] else 0.0

        sector_ret = 0.0
        if sector_name:
            sectors = sector_scoreboard.get("sectors", {})
            if isinstance(sectors, dict):
                sec = sectors.get(sector_name, {})
                if isinstance(sec, dict):
                    sector_ret = _safe_float(sec.get("return_5", sec.get("change_pct", 0.0)))

        return stock_ret - sector_ret

    # --------------------------------------------------------
    # STRATEGY SELECTION
    # --------------------------------------------------------

    def _select_strategy(self, market_regime: str) -> str:
        reg = str(market_regime).upper()

        if reg in {"TRENDING", "BULLISH", "UPTREND"}:
            return "TRENDING"
        if reg in {"FALLING", "CRASHING", "BEARISH", "DOWNTREND"}:
            return "CRASH_HUNTER"
        return "MEAN_REVERSION"

    def _evaluate_setup(
        self,
        strategy: str,
        candidate: CandidateInput,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        indicators: Dict[str, Any],
        market: MarketContext,
        sector_name: Optional[str],
        relative_strength: float,
        now_ts: float = 0.0,
    ) -> Dict[str, Any]:
        if strategy == "TRENDING":
            breakout = self._check_breakout_continuation(snapshot, candles_5m, candles_1m, candles_15m, indicators)
            pullback = self._check_trend_pullback(snapshot, candles_5m, candles_1m, candles_15m, indicators)

            candidates = [x for x in [breakout, pullback] if x["valid"]]
            if not candidates:
                return {"valid": False, "reject_reason": "no_trending_setup"}

            # prefer higher raw setup score
            return sorted(candidates, key=lambda x: x.get("setup_score", 0.0), reverse=True)[0]

        if strategy == "CRASH_HUNTER":
            return self._check_crash_hunter(snapshot, candles_5m, candles_1m, candles_15m, indicators)

        return self._check_mean_reversion(
            snapshot,
            candles_5m,
            candles_1m,
            candles_15m,
            indicators,
            now_ts=now_ts,
        )
    # --------------------------------------------------------
    # SETUP DETECTORS
    # --------------------------------------------------------

    def _check_trend_pullback(
        self,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        close = _last(indicators["5m"]["close"])
        ema20 = _last(indicators["5m"]["ema20"])
        ema50 = _last(indicators["5m"]["ema50"])
        ema200 = _last(indicators["5m"]["ema200"])
        vwap = _last(indicators["5m"]["vwap"])
        rsi = _last(indicators["5m"]["rsi"])
        atr = _last(indicators["5m"]["atr"])
        patterns = indicators["5m"]["patterns"]

        # 15m trend alignment
        if self.config.allow_15m_confirmation and "15m" in indicators and len(candles_15m) >= self.config.min_bars_15m:
            ema20_15 = _last(indicators["15m"]["ema20"])
            ema50_15 = _last(indicators["15m"]["ema50"])
            trend_ok_15 = ema20_15 >= ema50_15
        else:
            trend_ok_15 = True

        bullish_trend = close > ema20 > ema50 and ema50 >= ema200 * 0.98
        pullback_zone = abs(close - ema20) <= atr * 0.8 or abs(close - vwap) <= atr * 0.8
        momentum_ok = rsi >= 50
        pattern_ok = any(p in patterns for p in ["HAMMER", "BULLISH_ENGULFING", "DOJI"])

        if not (bullish_trend and pullback_zone and momentum_ok and trend_ok_15):
            return {"valid": False, "reject_reason": "trend_pullback_fail"}

        entry = max(close, ema20, vwap)
        stop = min(ema50, close - atr)
        if stop >= entry:
            stop = entry - max(atr * 0.8, 0.01)

        risk = entry - stop
        t1 = entry + risk * self.config.pullback_t1_rr
        t2 = entry + risk * self.config.default_t2_rr

        reasons = [
            "uptrend_structure_intact",
            "pullback_to_dynamic_support",
            "vwap_or_ema_reclaim",
        ]
        if pattern_ok:
            reasons.append("bullish_candle_confirmation")

        return {
            "valid": True,
            "setup_name": "Trend Pullback",
            "direction": "LONG",
            "entry_type": "RECLAIM_CONFIRMATION",
            "entry_trigger": round(entry, 2),
            "stop_loss": round(stop, 2),
            "T1": round(t1, 2),
            "T2": round(t2, 2),
            "rr_t1": (t1 - entry) / risk if risk > 0 else 0.0,
            "rr_t2": (t2 - entry) / risk if risk > 0 else None,
            "setup_score": 72 + (6 if pattern_ok else 0),
            "reasons": reasons,
            "warnings": [],
            "patterns": patterns,
            "indicator_snapshot": {
                "close": round(close, 2),
                "ema20": round(ema20, 2),
                "ema50": round(ema50, 2),
                "ema200": round(ema200, 2),
                "vwap": round(vwap, 2),
                "rsi_5m": round(rsi, 2),
                "atr_5m": round(atr, 2),
            },
        }

    def _check_breakout_continuation(
        self,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        close = _last(indicators["5m"]["close"])
        rsi = _last(indicators["5m"]["rsi"])
        atr = _last(indicators["5m"]["atr"])
        vol = _last(indicators["5m"]["volume"])
        avg_vol = _last(indicators["5m"]["avg_volume"])
        rolling_high_prev = indicators["5m"]["rolling_high"][-2] if len(indicators["5m"]["rolling_high"]) >= 2 else close
        bb_width_now = _last(indicators["5m"]["bb"]["width"])
        bb_width_prev = indicators["5m"]["bb"]["width"][-2] if len(indicators["5m"]["bb"]["width"]) >= 2 else bb_width_now
        macd_hist = _last(indicators["5m"]["macd"]["hist"])
        patterns = indicators["5m"]["patterns"]

        if self.config.allow_15m_confirmation and "15m" in indicators and len(candles_15m) >= self.config.min_bars_15m:
            trend_ok_15 = _last(indicators["15m"]["ema20"]) >= _last(indicators["15m"]["ema50"])
        else:
            trend_ok_15 = True

        breakout = close > rolling_high_prev
        volume_expansion = avg_vol > 0 and vol >= avg_vol * 1.25
        momentum_ok = rsi >= 58 and macd_hist >= 0
        squeeze_release = bb_width_now >= bb_width_prev * 1.05

        # 1m precision optional
        one_min_ok = True
        if self.config.allow_1m_precision and "1m" in indicators and candles_1m:
            one_min_ok = _last(indicators["1m"]["rsi"]) >= 50

        if not (breakout and volume_expansion and momentum_ok and squeeze_release and trend_ok_15 and one_min_ok):
            return {"valid": False, "reject_reason": "breakout_fail"}

        entry = close
        stop = close - max(atr, close * 0.003)
        risk = entry - stop
        t1 = entry + risk * self.config.breakout_t1_rr
        t2 = entry + risk * self.config.default_t2_rr

        reasons = [
            "range_breakout",
            "volume_expansion",
            "momentum_confirmation",
            "volatility_expansion",
        ]
        if "BULLISH_ENGULFING" in patterns:
            reasons.append("bullish_candle_confirmation")

        return {
            "valid": True,
            "setup_name": "Breakout Continuation",
            "direction": "LONG",
            "entry_type": "BREAKOUT_CONFIRMATION",
            "entry_trigger": round(entry, 2),
            "stop_loss": round(stop, 2),
            "T1": round(t1, 2),
            "T2": round(t2, 2),
            "rr_t1": (t1 - entry) / risk if risk > 0 else 0.0,
            "rr_t2": (t2 - entry) / risk if risk > 0 else None,
            "setup_score": 78 + (4 if "BULLISH_ENGULFING" in patterns else 0),
            "reasons": reasons,
            "warnings": [],
            "patterns": patterns,
            "indicator_snapshot": {
                "close": round(close, 2),
                "rsi_5m": round(rsi, 2),
                "atr_5m": round(atr, 2),
                "avg_vol_5m": round(avg_vol, 2),
                "curr_vol_5m": round(vol, 2),
                "breakout_level": round(rolling_high_prev, 2),
            },
        }

    def _check_crash_hunter(
        self,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        close = _last(indicators["5m"]["close"])
        ema20 = _last(indicators["5m"]["ema20"])
        ema50 = _last(indicators["5m"]["ema50"])
        ema200 = _last(indicators["5m"]["ema200"])
        rsi = _last(indicators["5m"]["rsi"])
        atr = _last(indicators["5m"]["atr"])
        vwap = _last(indicators["5m"]["vwap"])
        patterns = indicators["5m"]["patterns"]

        if self.config.allow_15m_confirmation and "15m" in indicators and len(candles_15m) >= self.config.min_bars_15m:
            trend_ok_15 = _last(indicators["15m"]["ema20"]) <= _last(indicators["15m"]["ema50"])
        else:
            trend_ok_15 = True

        bearish_trend = close < ema20 < ema50 and ema50 <= ema200 * 1.02
        bounce_to_resistance = abs(close - ema20) <= atr * 0.8 or abs(close - vwap) <= atr * 0.8
        weak_bounce = rsi <= 50
        candle_confirm = any(p in patterns for p in ["SHOOTING_STAR", "BEARISH_ENGULFING", "DOJI"])

        if not (bearish_trend and bounce_to_resistance and weak_bounce and trend_ok_15):
            return {"valid": False, "reject_reason": "crash_hunter_fail"}

        entry = min(close, ema20, vwap)
        stop = max(ema50, close + atr)
        if stop <= entry:
            stop = entry + max(atr * 0.8, 0.01)

        risk = stop - entry
        t1 = entry - risk * self.config.crash_hunter_t1_rr
        t2 = entry - risk * self.config.default_t2_rr

        reasons = [
            "bearish_market_structure",
            "dead_cat_bounce_into_resistance",
            "short_near_vwap_or_ema",
        ]
        if candle_confirm:
            reasons.append("bearish_candle_confirmation")

        return {
            "valid": True,
            "setup_name": "Crash Hunter",
            "direction": "SHORT",
            "entry_type": "RESISTANCE_REJECTION",
            "entry_trigger": round(entry, 2),
            "stop_loss": round(stop, 2),
            "T1": round(t1, 2),
            "T2": round(t2, 2),
            "rr_t1": (entry - t1) / risk if risk > 0 else 0.0,
            "rr_t2": (entry - t2) / risk if risk > 0 else None,
            "setup_score": 74 + (5 if candle_confirm else 0),
            "reasons": reasons,
            "warnings": [],
            "patterns": patterns,
            "indicator_snapshot": {
                "close": round(close, 2),
                "ema20": round(ema20, 2),
                "ema50": round(ema50, 2),
                "ema200": round(ema200, 2),
                "vwap": round(vwap, 2),
                "rsi_5m": round(rsi, 2),
                "atr_5m": round(atr, 2),
            },
        }

    def _check_mean_reversion(
        self,
        snapshot: Snapshot,
        candles_5m: List[Candle],
        candles_1m: List[Candle],
        candles_15m: List[Candle],
        indicators: Dict[str, Any],
        now_ts: float,
    ) -> Dict[str, Any]:
        close = _last(indicators["5m"]["close"])
        vwap = _last(indicators["5m"]["vwap"])
        rsi = _last(indicators["5m"]["rsi"])
        atr = _last(indicators["5m"]["atr"])
        bb_upper = _last(indicators["5m"]["bb"]["upper"])
        bb_lower = _last(indicators["5m"]["bb"]["lower"])
        patterns = indicators["5m"]["patterns"]

        dev_from_vwap = close - vwap
        mode_ctx = self._mode_context(now_ts)
        offmarket = bool(mode_ctx.get("offmarket"))

        if offmarket:
            band_buffer = close * (self.config.test_mr_band_proximity_pct / 100.0)
            extreme_up = close >= (bb_upper - band_buffer) and rsi >= self.config.test_mr_short_rsi
            extreme_down = close <= (bb_lower + band_buffer) and rsi <= self.config.test_mr_long_rsi
        else:
            extreme_up = close >= bb_upper and rsi >= 68
            extreme_down = close <= bb_lower and rsi <= 32

        one_min_long_ok = True
        one_min_short_ok = True
        if self.config.allow_1m_precision and "1m" in indicators and candles_1m:
            rsi1 = _last(indicators["1m"]["rsi"])
            one_min_long_ok = rsi1 >= 35
            one_min_short_ok = rsi1 <= 65

        if extreme_down and one_min_long_ok:
            entry = close
            stop = close - max(atr * 0.8, close * 0.003)
            risk = entry - stop
            t1 = min(vwap, entry + risk * self.config.mean_reversion_t1_rr)
            t2 = vwap if vwap > t1 else entry + risk * self.config.default_t2_rr
            reasons = [
                "far_below_vwap",
                "rsi_oversold",
                "bollinger_lower_band_extension",
            ]
            warnings = ["mean_reversion_has_lower_follow_through"]
            if offmarket:
                reasons.append("offmarket_test_relaxed_trigger")
                warnings.append("offmarket_validation_plan")

            return {
                "valid": True,
                "setup_name": "Mean Reversion",
                "direction": "LONG",
                "entry_type": "EXTREME_REVERSION",
                "entry_trigger": round(entry, 2),
                "stop_loss": round(stop, 2),
                "T1": round(t1, 2),
                "T2": round(t2, 2),
                "rr_t1": (t1 - entry) / risk if risk > 0 else 0.0,
                "rr_t2": (t2 - entry) / risk if risk > 0 else None,
                "setup_score": (80 if offmarket else 70) + (4 if "HAMMER" in patterns else 0),
                "reasons": reasons,
                "warnings": warnings,
                "patterns": patterns,
                "indicator_snapshot": {
                    "close": round(close, 2),
                    "vwap": round(vwap, 2),
                    "rsi_5m": round(rsi, 2),
                    "atr_5m": round(atr, 2),
                    "dev_from_vwap": round(dev_from_vwap, 2),
                },
            }

        if extreme_up and one_min_short_ok:
            entry = close
            stop = close + max(atr * 0.8, close * 0.003)
            risk = stop - entry
            t1 = max(vwap, entry - risk * self.config.mean_reversion_t1_rr)
            t2 = vwap if vwap < t1 else entry - risk * self.config.default_t2_rr
            reasons = [
                "far_above_vwap",
                "rsi_overbought",
                "bollinger_upper_band_extension",
            ]
            warnings = ["mean_reversion_has_lower_follow_through"]
            if offmarket:
                reasons.append("offmarket_test_relaxed_trigger")
                warnings.append("offmarket_validation_plan")

            return {
                "valid": True,
                "setup_name": "Mean Reversion",
                "direction": "SHORT",
                "entry_type": "EXTREME_REVERSION",
                "entry_trigger": round(entry, 2),
                "stop_loss": round(stop, 2),
                "T1": round(t1, 2),
                "T2": round(t2, 2),
                "rr_t1": (entry - t1) / risk if risk > 0 else 0.0,
                "rr_t2": (entry - t2) / risk if risk > 0 else None,
                "setup_score": (80 if offmarket else 70) + (4 if "SHOOTING_STAR" in patterns else 0),
                "reasons": reasons,
                "warnings": warnings,
                "patterns": patterns,
                "indicator_snapshot": {
                    "close": round(close, 2),
                    "vwap": round(vwap, 2),
                    "rsi_5m": round(rsi, 2),
                    "atr_5m": round(atr, 2),
                    "dev_from_vwap": round(dev_from_vwap, 2),
                },
            }

        return {"valid": False, "reject_reason": "mean_reversion_fail"}

    # --------------------------------------------------------
    # SCORING
    # --------------------------------------------------------

    def _quality_score(
        self,
        candidate: CandidateInput,
        setup_eval: Dict[str, Any],
        market: MarketContext,
        sector_name: Optional[str],
        spread_pct: float,
        risk_flag: str,
        relative_strength: float,
    ) -> float:
        score = 0.0

        score += setup_eval.get("setup_score", 0.0) * 0.45
        score += candidate.trend_score * 0.12
        score += candidate.momentum_score * 0.10
        score += candidate.volatility_score * 0.10
        score += candidate.volume_score * 0.08
        score += min(candidate.whale_score, 100.0) * 0.05

        # Relative strength
        if relative_strength >= self.config.rs_positive_threshold:
            score += 7.0
        elif relative_strength <= self.config.rs_negative_threshold:
            score -= 7.0

        # Sector alignment
        sec_strength = self._sector_strength(sector_name, market.sector_scoreboard)
        if sec_strength >= 70:
            score += 6.0
        elif sec_strength <= 40:
            score -= self.config.weak_sector_penalty

        # Spread penalty
        score -= min(max(spread_pct - 0.15, 0.0) * 10.0, 10.0)

        # Risk penalty
        if risk_flag == "CAUTION":
            score -= self.config.caution_penalty
        elif risk_flag == "AVOID":
            score -= self.config.avoid_penalty

        return round(_clamp(score, 0.0, 100.0), 2)

    def _confidence_score(
        self,
        candidate: CandidateInput,
        setup_eval: Dict[str, Any],
        market: MarketContext,
        sector_name: Optional[str],
        relative_strength: float,
        risk_flag: str,
        spread_pct: float,
    ) -> float:
        score = 20.0

        score += candidate.trend_score * 0.18
        score += candidate.momentum_score * 0.16
        score += candidate.volatility_score * 0.12
        score += candidate.volume_score * 0.10
        score += min(candidate.whale_score, 100.0) * 0.12
        score += setup_eval.get("setup_score", 0.0) * 0.20

        if relative_strength >= self.config.rs_positive_threshold:
            score += 6.0
        elif relative_strength <= self.config.rs_negative_threshold:
            score -= 6.0

        sec_strength = self._sector_strength(sector_name, market.sector_scoreboard)
        if sec_strength >= 70:
            score += self.config.strong_sector_boost
        elif sec_strength <= 40:
            score -= self.config.weak_sector_penalty

        if risk_flag == "CAUTION":
            score -= self.config.caution_penalty
        elif risk_flag == "AVOID":
            score -= self.config.avoid_penalty

        if spread_pct > 0.30:
            score -= min((spread_pct - 0.30) * 10.0, 8.0)

        return round(_clamp(score, 0.0, 100.0), 2)

    def _sector_strength(self, sector_name: Optional[str], sector_scoreboard: Dict[str, Any]) -> float:
        if not sector_name:
            return 50.0

        sectors = sector_scoreboard.get("sectors", {})
        if not isinstance(sectors, dict):
            return 50.0

        sec = sectors.get(sector_name, {})
        if not isinstance(sec, dict):
            return 50.0

        return _safe_float(sec.get("score", 50.0))

    # --------------------------------------------------------
    # TRADE PLAN
    # --------------------------------------------------------

    def _build_trade_plan(
        self,
        candidate: CandidateInput,
        market: MarketContext,
        snapshot: Snapshot,
        setup_eval: Dict[str, Any],
        sector_name: Optional[str],
        relative_strength: float,
        quality_score: float,
        confidence_score: float,
        risk_flag: str,
        liquidity_warnings: List[str],
        now_ts: float,
    ) -> TradePlan:
        reasons = list(setup_eval.get("reasons", []))
        warnings = list(setup_eval.get("warnings", []))

        # Relative strength interpretation
        if relative_strength >= self.config.rs_positive_threshold:
            reasons.append("sector_outperformance")
        elif relative_strength <= self.config.rs_negative_threshold:
            warnings.append("sector_underperformance")

        # Sector alignment interpretation
        sec_strength = self._sector_strength(sector_name, market.sector_scoreboard)
        if sec_strength >= 70:
            reasons.append("strong_sector_alignment")
        elif sec_strength <= 40:
            warnings.append("weak_sector_alignment")

        if risk_flag == "CAUTION":
            warnings.append("event_risk_caution")

        warnings.extend(liquidity_warnings)
        warnings = list(dict.fromkeys(warnings))
        reasons = list(dict.fromkeys(reasons))

        entry = _safe_float(setup_eval["entry_trigger"])
        sl = _safe_float(setup_eval["stop_loss"])
        t1 = _safe_float(setup_eval["T1"])
        t2 = setup_eval.get("T2")
        t2 = round(_safe_float(t2), 2) if t2 is not None else None

        if setup_eval["direction"] == "LONG":
            expected_move = max(t1 - entry, 0.0)
        else:
            expected_move = max(entry - t1, 0.0)

        expires_at = now_ts + (self.config.validity_window_candles * self.config.candle_interval_minutes * 60)

        return TradePlan(
            symbol=candidate.symbol,
            token=candidate.token,
            direction=setup_eval["direction"],
            setup_name=setup_eval["setup_name"],
            entry_type=setup_eval["entry_type"],
            entry_trigger=round(entry, 2),
            stop_loss=round(sl, 2),
            T1=round(t1, 2),
            T2=t2,
            confidence_score=round(confidence_score, 2),
            expected_move=round(expected_move, 2),
            reasons=reasons,
            warnings=warnings,
            validity_window=f"{self.config.validity_window_candles} candles ({self.config.validity_window_candles * self.config.candle_interval_minutes} minutes)",
            sector=sector_name,
            quality_score=round(quality_score, 2),
            rr_t1=round(_safe_float(setup_eval.get("rr_t1")), 2),
            rr_t2=round(_safe_float(setup_eval.get("rr_t2")), 2) if setup_eval.get("rr_t2") is not None else None,
            relative_strength=round(relative_strength, 2),
            market_regime=market.market_regime,
            risk_flag=risk_flag,
            timestamp=now_ts,
            expires_at=expires_at,
            pattern_confirmations=setup_eval.get("patterns", []),
            indicator_snapshot=setup_eval.get("indicator_snapshot", {}),
        )

    # --------------------------------------------------------
    # RANKING
    # --------------------------------------------------------

    def _rank_trade_plans(self, plans: List[TradePlan]) -> List[TradePlan]:
        return sorted(
            plans,
            key=lambda p: (
                p.confidence_score,
                p.quality_score,
                p.rr_t1,
                p.expected_move,
                p.relative_strength,
            ),
            reverse=True,
        )

    # --------------------------------------------------------
    # OPTIONAL FOLLOW-UP RULE FOR MODULE 06/EXECUTOR
    # --------------------------------------------------------

    def build_t1_followup_message(self, plan: Dict[str, Any]) -> str:
        symbol = plan.get("symbol", "")
        direction = plan.get("direction", "")
        t2 = plan.get("T2")
        if t2:
            return (
                f"{symbol} {direction}: T1 achieved. Stoploss moved to entry. "
                f"Potential continuation toward T2 = {t2}."
            )
        return f"{symbol} {direction}: T1 achieved. Stoploss moved to entry."

    # --------------------------------------------------------
    # DEBUG / HEALTH
    # --------------------------------------------------------

    def cache_health(self) -> Dict[str, Any]:
        return {
            "cache_size": len(self._indicator_cache),
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_ttl_seconds": self.config.cache_ttl_seconds,
            "max_cache_symbols": self.config.max_cache_symbols,
        }


# ============================================================
# EXAMPLE ADAPTER CONTRACT
# ============================================================
# This helper is optional. It shows how Module 04 / 03 / 02 data
# can be passed into Module 05 with minimal glue code.

def run_module_05(
    candidate_list: List[Dict[str, Any]],
    candidate_backup_list: List[Dict[str, Any]],
    market_context: Dict[str, Any],
    snapshot_store: Dict[str, Dict[str, Any]],
    candle_store_5m: Dict[str, List[Dict[str, Any]]],
    candle_store_1m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    candle_store_15m: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    config: Optional[Module05Config] = None,
) -> Dict[str, Any]:
    engine = Module05StrategyPatternTradePlanEngine(config=config)
    return engine.run_scan(
        candidate_list=candidate_list,
        candidate_backup_list=candidate_backup_list,
        market_context=market_context,
        snapshot_store=snapshot_store,
        candle_store_5m=candle_store_5m,
        candle_store_1m=candle_store_1m or {},
        candle_store_15m=candle_store_15m or {},
    )