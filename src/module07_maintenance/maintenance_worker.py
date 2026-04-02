# MODULE 07 — Maintenance Worker (Ops + Schedules + Truth Files)

import os, json, time, shutil, hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

IST = ZoneInfo("Asia/Kolkata")

# ---------------------------
# Index normalization / alias helpers
# ---------------------------
_INDEX_ALIAS_GROUPS = [
    [
        "NIFTY 50",
        "NIFTY",
        "NSE:NIFTY",
        "NIFTY50",
        "NIFTY 50 INDEX",
        "NIFTY50 INDEX",
    ],
    [
        "NIFTY BANK",
        "BANKNIFTY",
        "NSE:BANKNIFTY",
    ],
    [
        "NIFTY FIN SERVICE",
        "NIFTY FINANCIAL SERVICES",
        "NIFTY FIN SERVICE INDEX",
        "NIFTY FIN SERVICES",
        "NIFTY FIN SERVICE",
        "NIFTY FINANCIAL SERVICE",
        "FINNIFTY",
        "NSE:FINNIFTY",
    ],
    [
        "NIFTY MID SELECT",
        "NIFTY MIDCAP SELECT",
        "NIFTY MIDCAP SELECT INDEX",
        "MIDCPNIFTY",
        "NSE:MIDCPNIFTY",
    ],
    [
    "NIFTY INFRA",
    "NIFTYINFRA",
    "NSE:NIFTYINFRA",
    "NIFTY INFRASTRUCTURE",
    "NIFTY INFRA INDEX",
    "NIFTY INFRASTRUCTURE INDEX",
    "Nifty Infrastructure",
    "Nifty Infra",
    ],
    [
        "NIFTY PVT BANK",
        "NIFTY PRIVATE BANK",
        "NIFTY PVT BANK INDEX",
        "NIFTYPVTBANK",
        "NSE:NIFTYPVTBANK",
    ],
    [
        "NIFTY PSU BANK",
        "NIFTY PSU BANK INDEX",
        "NIFTYPSUBANK",
        "NSE:NIFTYPSUBANK",
    ],
    [
        "INDIA VIX",
        "INDIAVIX",
        "NSE:INDIA VIX",
    ],
]

_ACTIVE_SYNTHETIC_INDEXES = {
    "NIFTY INDIA DEFENCE",
    "NIFTY INDIA DIGITAL",
    "NIFTY INDIA MANUFACTURING",
    "NIFTY CAPITAL MARKETS",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY HOUSING",
    "NIFTY CORE HOUSING",
    "NIFTY TRANSPORTATION AND LOGISTICS",
    "NIFTY OIL AND GAS",
    "NIFTY HEALTHCARE",
    "NIFTY SERVICES SECTOR",
    "NIFTY RURAL",
}

_WATCHLIST_SYNTHETIC_INDEXES = {
    "NIFTY CONGLOMERATE 50",
    "NIFTY NON CYCLICAL CONSUMER",
    "NIFTY ALPHA 50",
    "NIFTY200 ALPHA 30",
    "NIFTY ALPHA LOW VOLATILITY 30",
    "NIFTY100 LOW VOLATILITY 30",
    "NIFTY LOW VOLATILITY 50",
    "NIFTY QUALITY LOW VOLATILITY 30",
    "NIFTY200 QUALITY 30",
    "NIFTY100 QUALITY 30",
    "NIFTY QUALITY 30",
    "NIFTY200 MOMENTUM 30",
    "NIFTY500 MOMENTUM 50",
    "NIFTY MIDCAP150 MOMENTUM 50",
    "NIFTY200 VALUE 30",
    "NIFTY500 VALUE 50",
    "NIFTY DIVIDEND OPPORTUNITIES 50",
    "NIFTY50 EQUAL WEIGHT",
    "NIFTY100 EQUAL WEIGHT",
    "NIFTY500 EQUAL WEIGHT",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 50",
    "NIFTY SMALLCAP 100",
    "NIFTY SMALLCAP 250",
    "NIFTY SMALLCAP 500",
    "NIFTY MICROCAP 250",
    "NIFTY LARGEMIDCAP 250",
    "NIFTY MIDCAP 100",
}

def _norm_index_text(x: Any) -> str:
    s = str(x or "").strip().upper()
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
        "SERV SECTOR": "SERVICES SECTOR",
        "DIV OPPS": "DIVIDEND OPPORTUNITIES",
        "EQL WGT": "EQUAL WEIGHT",
        "LOWVOL": "LOW VOLATILITY",
        "QUALTY": "QUALITY",
        "MIDSML": "MIDSMALL",
        " AND ": " AND ",
    }

    for old, new in replacements.items():
        s = s.replace(old, new)

    s = " ".join(s.split())
    return s

def _index_alias_variants(*values: Any) -> List[str]:
    variants = set()

    for v in values:
        base = _norm_index_text(v)
        if not base:
            continue
        variants.add(base)

    expanded = set(variants)
    for base in list(variants):
        for group in _INDEX_ALIAS_GROUPS:
            norm_group = {_norm_index_text(x) for x in group}
            if base in norm_group:
                expanded.update(norm_group)

    return [x for x in expanded if x]

def _classify_unresolved_index(item: Dict[str, Any]) -> str:
    candidates = _index_alias_variants(
        item.get("name"),
        item.get("symbol"),
        item.get("tradingsymbol"),
    )

    for c in candidates:
        if c in _ACTIVE_SYNTHETIC_INDEXES:
            return "active_synthetic"
        if c in _WATCHLIST_SYNTHETIC_INDEXES:
            return "watchlist"

    return "ignore_for_now"

# ---------------------------
# Paths / IO helpers
# ---------------------------
# imports ...

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _read_json(path: str, default=None):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _write_json(path: str, data: Any, indent: int = 2):
    _ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
    os.replace(tmp, path)

def _append_jsonl(path: str, obj: Dict[str, Any]):
    _ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class BotPaths:
    root: str

    @property
    def config_dir(self): return os.path.join(self.root, "config")
    @property
    def instruments_dir(self): return os.path.join(self.root, "instruments")
    @property
    def mappings_dir(self): return os.path.join(self.root, "mappings")
    @property
    def lists_dir(self): return os.path.join(self.root, "lists")
    @property
    def state_dir(self): return os.path.join(self.root, "state")
    @property
    def logs_dir(self): return os.path.join(self.root, "logs")

    @property
    def daily_movers_path(self):
        return os.path.join(self.lists_dir, "daily_movers_cache.json")

    @property
    def daily_losers_path(self):
        return os.path.join(self.lists_dir, "daily_losers_cache.json")

    @property
    def potential_stocks_path(self):
        return os.path.join(self.lists_dir, "potential_stocks.json")

    @property
    def manual_additions_path(self):
        return os.path.join(self.lists_dir, "manual_additions.json")

    @property
    def discovered_symbols_path(self):
        return os.path.join(self.lists_dir, "discovered_symbols.json")

    @property
    def previous_day_movers_path(self):
        return os.path.join(self.lists_dir, "previous_day_movers.json")

    @property
    def previous_day_losers_path(self):
        return os.path.join(self.lists_dir, "previous_day_losers.json")

    def day_logs_dir(self, date_str: str):
        return os.path.join(self.logs_dir, date_str)


    def _ensure_dir(path: str):
        os.makedirs(path, exist_ok=True)


    def _read_json(path: str, default=None):
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


    def _write_json(path: str, data: Any, indent: int = 2):
        _ensure_dir(os.path.dirname(path))
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        os.replace(tmp, path)


    def _append_jsonl(path: str, obj: Dict[str, Any]):
        _ensure_dir(os.path.dirname(path))
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


    def _sha256_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

# ---------------------------
# State machine
# ---------------------------
class BotState:
    RUNNING = "RUNNING"
    READY = "READY"
    NOT_READY = "NOT_READY"
    DEGRADED = "DEGRADED"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"

# ---------------------------
# Module 07 Core
# ---------------------------
class MaintenanceWorker:
    def __init__(
        self,
        paths: BotPaths,
        broker_ready_fn,          # Module 01 readiness callable -> (ok:bool, reason:str)
        ws_ready_fn,              # Module 02 quick test callable -> (ok:bool, reason:str)
        alert_fn,                 # Module 06 alert callable: alert_fn(level, title, message, tags=None)
        telegram_delete_fn=None,  # optional callable(message_id) -> (ok, reason)
        market_lists_provider=None,

    ):
        self.paths = paths
        self.broker_ready_fn = broker_ready_fn
        self.ws_ready_fn = ws_ready_fn
        self.alert_fn = alert_fn
        self.telegram_delete_fn = telegram_delete_fn
        self.market_lists_provider = market_lists_provider

        self._init_folders_and_defaults()

    def is_ready(self):
        """
        Readiness check used by Supervisor.
        This matches run_readiness_checks() for the current SIGNALS_ONLY bot scope.
        WebSocket connectivity is runtime/live health, not a startup readiness gate.
        """
        ok, _reasons = self.run_readiness_checks()
        return bool(ok)
    # -----------------------
    # Bootstrap / defaults
    # -----------------------
    def _init_folders_and_defaults(self):
        for d in [
            self.paths.config_dir, self.paths.instruments_dir, self.paths.mappings_dir,
            self.paths.lists_dir, self.paths.state_dir, self.paths.logs_dir
        ]:
            _ensure_dir(d)

        # minimal defaults (you can expand later)
        runtime_cfg_path = os.path.join(self.paths.config_dir, "runtime_config.json")
        if not os.path.exists(runtime_cfg_path):
            _write_json(runtime_cfg_path, {
                "heartbeat_minutes": 15,
                "instrument_refresh_cooldown_minutes": 30,
                "heartbeat_stale_minutes": 20,
                "readiness_window": {"start": "08:45", "end": "09:10"},
                "instrument_download_time": "08:30",
                "market_list_preopen_time": "09:00",
                "market_list_postopen_time": "09:20",
                "market_list_hourly_times": ["10:15", "11:15", "12:15", "13:15", "14:15"],
                "market_list_keep_last_good": True,
                "post_close_delete_delay_minutes": 30,
                "max_failures_before_pause": 3
            })

        # create empty truth lists if missing (safe placeholders)
        for fn, default in [
            ("watchlist.json", []),
            ("potential_stocks.json", []),
            ("daily_movers_cache.json", []),
            ("daily_losers_cache.json", []),
            ("manual_additions.json", []),
            ("discovered_symbols.json", []),
            ("previous_day_movers.json", []),
            ("previous_day_losers.json", []),
            ("index_taxonomy.json", {"indexes": []}),
        ]:
            p = os.path.join(self.paths.lists_dir, fn)
            if not os.path.exists(p):
                _write_json(p, default)

    # -----------------------
    # Time helpers
    # -----------------------
    def now_ist(self) -> datetime:
        return datetime.now(tz=IST)

    def today_str(self) -> str:
        return self.now_ist().strftime("%Y-%m-%d")

    def _time_hhmm(self) -> str:
        return self.now_ist().strftime("%H:%M")

    # -----------------------
    # Daily state / heartbeat
    # -----------------------
    def write_daily_state(self, status: str, reasons: List[str], extras: Optional[Dict[str, Any]] = None):
        date_str = self.today_str()
        path = os.path.join(self.paths.state_dir, f"daily_state_{date_str}.json")
        payload = {
            "date": date_str,
            "timestamp_ist": self.now_ist().isoformat(),
            "status": status,
            "reasons": reasons,
            "extras": extras or {}
        }
        _write_json(path, payload)

    def write_heartbeat(self, bot_state: str, ws_snapshot: Dict[str, Any], last_scan: Optional[str], last_menu_id: Optional[str]):
        path = os.path.join(self.paths.state_dir, "runtime_heartbeat.json")
        payload = {
            "timestamp_ist": self.now_ist().isoformat(),
            "bot_state": bot_state,
            "ws_snapshot": ws_snapshot,
            "last_scan_time_ist": last_scan,
            "last_menu_id": last_menu_id
        }
        _write_json(path, payload)

    def heartbeat_is_stale(self) -> bool:
        cfg = _read_json(os.path.join(self.paths.config_dir, "runtime_config.json"), {})
        stale_min = int(cfg.get("heartbeat_stale_minutes", 20))
        hb = _read_json(os.path.join(self.paths.state_dir, "runtime_heartbeat.json"), None)
        if not hb or "timestamp_ist" not in hb:
            return True
        try:
            ts = datetime.fromisoformat(hb["timestamp_ist"])
            return (self.now_ist() - ts) > timedelta(minutes=stale_min)
        except Exception:
            return True

    def refresh_daily_movers(self) -> Dict[str, Any]:
        provider = self.market_lists_provider
        if provider is None or not hasattr(provider, "get_daily_movers"):
            return {"ok": False, "count": 0, "reason": "market_lists_provider.get_daily_movers missing"}

        try:
            rows = provider.get_daily_movers() or []
            if not isinstance(rows, list):
                return {"ok": False, "count": 0, "reason": "daily movers payload is not a list"}

            _write_json(self.paths.daily_movers_path, rows)
            return {"ok": True, "count": len(rows), "reason": ""}
        except Exception as e:
            return {"ok": False, "count": 0, "reason": f"daily movers refresh failed: {e}"}

    def refresh_daily_losers(self) -> Dict[str, Any]:
        provider = self.market_lists_provider
        if provider is None or not hasattr(provider, "get_daily_losers"):
            return {"ok": False, "count": 0, "reason": "market_lists_provider.get_daily_losers missing"}

        try:
            rows = provider.get_daily_losers() or []
            if not isinstance(rows, list):
                return {"ok": False, "count": 0, "reason": "daily losers payload is not a list"}

            _write_json(self.paths.daily_losers_path, rows)
            return {"ok": True, "count": len(rows), "reason": ""}
        except Exception as e:
            return {"ok": False, "count": 0, "reason": f"daily losers refresh failed: {e}"}

    def refresh_potential_stocks(self) -> Dict[str, Any]:
        provider = self.market_lists_provider
        if provider is None:
            return {"ok": False, "count": 0, "reason": "market_lists_provider missing"}

        if hasattr(provider, "build_potential_stocks"):
            try:
                rows = provider.build_potential_stocks() or []
                if not isinstance(rows, list):
                    return {"ok": False, "count": 0, "reason": "potential stocks payload is not a list"}

                _write_json(self.paths.potential_stocks_path, rows)
                return {"ok": True, "count": len(rows), "reason": ""}
            except Exception as e:
                return {"ok": False, "count": 0, "reason": f"potential stocks refresh failed: {e}"}

        return {"ok": False, "count": 0, "reason": "market_lists_provider.build_potential_stocks missing"}

    def refresh_daily_market_lists(self) -> Dict[str, Any]:
        movers = self.refresh_daily_movers()
        losers = self.refresh_daily_losers()
        potential = self.refresh_potential_stocks()

        summary = {
            "timestamp_ist": self.now_ist().isoformat(),
            "movers": movers,
            "losers": losers,
            "potential": potential,
        }

        _write_json(
            os.path.join(self.paths.state_dir, "daily_market_lists_status.json"),
            summary
        )

        return summary

    def _read_existing_list_count(self, path: str) -> int:
        data = _read_json(path, default=[])
        return len(data) if isinstance(data, list) else 0

    def _preserve_last_good_market_lists(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        keep_last_good = bool(
            _read_json(os.path.join(self.paths.config_dir, "runtime_config.json"), {}).get("market_list_keep_last_good", True)
        )
        if not keep_last_good:
            return summary

        for key, path in [
            ("movers", self.paths.daily_movers_path),
            ("losers", self.paths.daily_losers_path),
            ("potential", self.paths.potential_stocks_path),
        ]:
            info = summary.get(key, {}) or {}
            count = int(info.get("count", 0) or 0)

            if count > 0:
                continue

            existing_count = self._read_existing_list_count(path)
            if existing_count > 0:
                info["ok"] = True
                info["count"] = existing_count
                info["reason"] = f"kept last good cache ({existing_count})"
                summary[key] = info

        return summary

    def refresh_daily_market_lists_safe(self) -> Dict[str, Any]:
        summary = self.refresh_daily_market_lists()
        summary = self._preserve_last_good_market_lists(summary)

        _write_json(
            os.path.join(self.paths.state_dir, "daily_market_lists_status.json"),
            summary
        )
        return summary

    def _normalize_symbol(self, symbol: Any) -> str:
        s = str(symbol or "").strip().upper()
        s = " ".join(s.split())
        return s

    def _load_symbol_map(self) -> Dict[str, Any]:
        return _read_json(os.path.join(self.paths.mappings_dir, "symbol_to_token.json"), {}) or {}

    def _symbol_exists_in_mappings(self, symbol: str) -> bool:
        sym = self._normalize_symbol(symbol)
        if not sym:
            return False

        symbol_map = self._load_symbol_map()
        if sym in symbol_map:
            return True

        nse_key = f"NSE:{sym}"
        return nse_key in symbol_map

    def _merge_symbol_rows(self, existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen = set()

        for row in (existing or []) + (incoming or []):
            if isinstance(row, str):
                sym = self._normalize_symbol(row)
                payload = {"symbol": sym}
            elif isinstance(row, dict):
                sym = self._normalize_symbol(row.get("symbol"))
                if not sym:
                    continue
                payload = dict(row)
                payload["symbol"] = sym
                payload.setdefault("exchange", "NSE")
            else:
                continue

            if not sym or sym in seen:
                continue
            seen.add(sym)
            out.append(payload)

        return out

    def add_manual_symbol(self, symbol: str, source: str = "telegram_manual") -> Dict[str, Any]:
        sym = self._normalize_symbol(symbol)
        if not sym:
            return {"ok": False, "symbol": "", "reason": "empty symbol"}

        if not self._symbol_exists_in_mappings(sym):
            return {"ok": False, "symbol": sym, "reason": "symbol not found in Angel mappings"}

        row = {
            "symbol": sym,
            "exchange": "NSE",
            "source": source,
            "added_at_ist": self.now_ist().isoformat(),
        }

        manual_existing = _read_json(self.paths.manual_additions_path, default=[]) or []
        manual_out = self._merge_symbol_rows(manual_existing, [row])
        _write_json(self.paths.manual_additions_path, manual_out)

        discovered_existing = _read_json(self.paths.discovered_symbols_path, default=[]) or []
        discovered_out = self._merge_symbol_rows(discovered_existing, [row])
        _write_json(self.paths.discovered_symbols_path, discovered_out)

        return {"ok": True, "symbol": sym, "reason": ""}

    def archive_current_market_lists(self) -> Dict[str, Any]:
        movers = _read_json(self.paths.daily_movers_path, default=[]) or []
        losers = _read_json(self.paths.daily_losers_path, default=[]) or []

        prev_movers_out = self._merge_symbol_rows([], movers)
        prev_losers_out = self._merge_symbol_rows([], losers)

        _write_json(self.paths.previous_day_movers_path, prev_movers_out)
        _write_json(self.paths.previous_day_losers_path, prev_losers_out)

        discovered_existing = _read_json(self.paths.discovered_symbols_path, default=[]) or []
        discovered_out = self._merge_symbol_rows(discovered_existing, prev_movers_out + prev_losers_out)
        _write_json(self.paths.discovered_symbols_path, discovered_out)

        return {
            "ok": True,
            "movers_count": len(prev_movers_out),
            "losers_count": len(prev_losers_out),
            "discovered_count": len(discovered_out),
        }

    def clear_daily_market_lists(self) -> Dict[str, Any]:
        _write_json(self.paths.daily_movers_path, [])
        _write_json(self.paths.daily_losers_path, [])
        return {"ok": True, "reason": ""}

    def archive_and_rotate_daily_market_lists(self) -> Dict[str, Any]:
        archive_status = self.archive_current_market_lists()
        clear_status = self.clear_daily_market_lists()

        summary = {
            "timestamp_ist": self.now_ist().isoformat(),
            "archive": archive_status,
            "clear": clear_status,
        }

        _write_json(
            os.path.join(self.paths.state_dir, "daily_market_lists_archive_status.json"),
            summary
        )
        return summary

    # -----------------------
    # Instrument master workflow
    # -----------------------
    def download_instrument_master(self) -> str:
        """
        Download Angel instrument master JSON and return path to instrument_master_new.json
        """
        import urllib.request

        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        new_path = os.path.join(self.paths.instruments_dir, "instrument_master_new.json")

        _ensure_dir(self.paths.instruments_dir)

        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )

        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()

        data = json.loads(raw.decode("utf-8"))

        _write_json(new_path, data)

        return new_path

    def validate_instrument_master(self, path: str, sample_symbols: List[str], index_taxonomy: Any) -> Tuple[bool, List[str]]:
        reasons = []
        if not os.path.exists(path) or os.path.getsize(path) < 1000:
            return False, ["instrument master missing/empty"]

        try:
            data = _read_json(path, default=None)
        except Exception as e:
            return False, [f"instrument master JSON parse failed: {e}"]

        if not isinstance(data, list) or len(data) < 1000:
            reasons.append("instrument master list too small or wrong type")

        required = {"token", "symbol", "name", "exch_seg"}
        for row in (data[:50] if isinstance(data, list) else []):
            if not isinstance(row, dict):
                reasons.append("instrument master contains non-dict rows")
                break
            missing = required - set(row.keys())
            if missing:
                reasons.append(f"missing fields: {sorted(list(missing))}")
                break

        sym_to_token = {}
        if isinstance(data, list):
            for r in data:
                if not isinstance(r, dict):
                    continue
                sym = r.get("symbol")
                tk = r.get("token")
                name = r.get("name")
                ex = r.get("exch_seg")

                if sym and tk:
                    sym_str = str(sym).strip()
                    tk_str = str(tk).strip()
                    ex_str = str(ex).strip() if ex else ""

                    if ex_str:
                        sym_to_token.setdefault(f"{ex_str}:{sym_str}", tk_str)

                    sym_to_token.setdefault(sym_str, tk_str)

                if name and tk:
                    sym_to_token.setdefault(str(name).strip(), str(tk).strip())

        for s in sample_symbols:
            if not s:
                continue

            key = str(s).strip()
            key_nse = key if ":" in key else f"NSE:{key}"

            if key not in sym_to_token and key_nse not in sym_to_token:
                reasons.append(f"sample symbol token not found: {s}")

        if isinstance(index_taxonomy, dict):
            taxonomy_items = index_taxonomy.get("indexes", [])
        elif isinstance(index_taxonomy, list):
            taxonomy_items = index_taxonomy
        else:
            taxonomy_items = None

        if taxonomy_items is None or not isinstance(taxonomy_items, list):
            reasons.append("index taxonomy invalid type")
        else:
            unresolved = 0
            valid_items = [x for x in taxonomy_items if isinstance(x, dict)]

            for item in valid_items:
                sym = item.get("symbol") or item.get("tradingsymbol") or item.get("name")
                ex = item.get("exchange", "NSE")

                if not sym:
                    continue

                key = str(sym).strip()
                key_ex = key if ":" in key else f"{ex}:{key}"

                if key not in sym_to_token and key_ex not in sym_to_token:
                    unresolved += 1

            if valid_items and unresolved == len(valid_items):
                reasons.append("no index taxonomy entries resolvable from instrument master")

        ok = (len(reasons) == 0)
        return ok, reasons

    def promote_instrument_master(self, new_path: str):
        cur = os.path.join(self.paths.instruments_dir, "instrument_master.json")
        prev = os.path.join(self.paths.instruments_dir, "instrument_master_prev.json")
        meta = os.path.join(self.paths.instruments_dir, "instrument_master_meta.json")

        if os.path.exists(cur):
            shutil.copy2(cur, prev)

        shutil.move(new_path, cur)

        meta_payload = {
            "updated_at_ist": self.now_ist().isoformat(),
            "sha256": _sha256_file(cur),
            "status": "VALID"
        }
        _write_json(meta, meta_payload)

    # -----------------------
    # Token map generation
    # -----------------------
    def regenerate_token_maps(self):
        master_path = os.path.join(self.paths.instruments_dir, "instrument_master.json")
        data = _read_json(master_path, default=[])

        symbol_to_token = {}
        token_to_symbol = {}
        index_lookup = {}
        watchlist_symbol_lookup = {}
        watchlist_name_lookup = {}

        for r in data:
            if not isinstance(r, dict):
                continue

            sym = r.get("symbol")
            tk = r.get("token")
            name = r.get("name")
            ex = r.get("exch_seg")
            instrument_type = r.get("instrumenttype")

            if not sym or not tk:
                continue

            sym_str = str(sym).strip()
            tk_str = str(tk).strip()
            ex_str = str(ex).strip() if ex else ""

            key = f"{ex_str}:{sym_str}" if ex_str else sym_str
            symbol_to_token.setdefault(key, tk_str)
            token_to_symbol.setdefault(tk_str, key)

            symbol_to_token.setdefault(sym_str, tk_str)

            if name:
                symbol_to_token.setdefault(str(name).strip(), tk_str)

            # Watchlist rows use cash symbols like RELIANCE while the master often
            # stores NSE/BSE cash instruments as RELIANCE-EQ with name=RELIANCE.
            if ex_str:
                watchlist_symbol_lookup.setdefault(f"{ex_str}:{sym_str}", tk_str)
                if name:
                    watchlist_name_lookup.setdefault(f"{ex_str}:{str(name).strip()}", tk_str)

            if ex_str == "NSE" and str(instrument_type or "").strip() == "AMXIDX":
                for candidate in _index_alias_variants(sym_str, name, key):
                    index_lookup.setdefault(candidate, r)

        _write_json(os.path.join(self.paths.mappings_dir, "symbol_to_token.json"), symbol_to_token)
        _write_json(os.path.join(self.paths.mappings_dir, "token_to_symbol.json"), token_to_symbol)

        # watchlist_tokens
        wl = _read_json(os.path.join(self.paths.lists_dir, "watchlist.json"), default=[])
        wl_tokens = []
        for item in wl:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).strip()
            exchange = str(item.get("exchange", "NSE")).strip() or "NSE"
            sector = str(item.get("sector", "")).strip()

            if not symbol:
                continue

            lookup_key = f"{exchange}:{symbol}"
            resolved_token = (
                watchlist_symbol_lookup.get(lookup_key)
                or watchlist_name_lookup.get(lookup_key)
            )

            wl_tokens.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "sector": sector,
                    "token": str(resolved_token).strip() if resolved_token is not None else None,
                    "resolution_status": "RESOLVED" if resolved_token is not None else "UNRESOLVED",
                }
            )
        _write_json(os.path.join(self.paths.mappings_dir, "watchlist_tokens.json"), wl_tokens)

        # index_tokens
        taxonomy = _read_json(os.path.join(self.paths.lists_dir, "index_taxonomy.json"), default=[])
        if isinstance(taxonomy, dict):
            taxonomy_items = taxonomy.get("indexes", [])
        elif isinstance(taxonomy, list):
            taxonomy_items = taxonomy
        else:
            taxonomy_items = []

        idx_tokens = {}
        taxonomy_with_tokens = []
        seen_index_tokens = set()

        unresolved_buckets = {
            "active_synthetic": [],
            "watchlist": [],
            "ignore_for_now": [],
        }

        for item in taxonomy_items:
            if not isinstance(item, dict):
                continue

            ex = str(item.get("exchange", "NSE")).strip() or "NSE"
            name = item.get("name")
            sym = item.get("symbol") or item.get("tradingsymbol") or name

            resolved_token = None
            resolved_key = None
            matched_row = None

            if sym:
                sym_str = str(sym).strip()
                key_ex = sym_str if ":" in sym_str else f"{ex}:{sym_str}"

                resolved_token = (
                    symbol_to_token.get(key_ex)
                    or symbol_to_token.get(sym_str)
                    or (symbol_to_token.get(str(name).strip()) if name else None)
                )

                if resolved_token is not None:
                    resolved_key = key_ex if symbol_to_token.get(key_ex) == resolved_token else sym_str

            if resolved_token is None:
                alias_candidates = _index_alias_variants(name, sym, item.get("tradingsymbol"))
                for alias in alias_candidates:
                    row = index_lookup.get(alias)
                    if row:
                        matched_row = row
                        resolved_token = str(row.get("token")).strip()
                        row_ex = str(row.get("exch_seg", ex)).strip() or ex
                        row_sym = str(row.get("symbol", "")).strip()
                        resolved_key = f"{row_ex}:{row_sym}" if row_sym else None
                        break

            new_item = dict(item)

            if resolved_token is not None:
                if resolved_token in seen_index_tokens:
                    continue

                seen_index_tokens.add(resolved_token)
                new_item["token"] = str(resolved_token)
                new_item["resolution_status"] = "RESOLVED"

                if matched_row:
                    new_item["resolved_symbol"] = str(matched_row.get("symbol", "")).strip()
                    new_item["resolved_name"] = str(matched_row.get("name", "")).strip()
                    new_item["resolved_exchange"] = str(matched_row.get("exch_seg", "")).strip()
                    new_item["resolved_instrumenttype"] = str(matched_row.get("instrumenttype", "")).strip()
                else:
                    if sym:
                        new_item["resolved_symbol"] = str(sym).strip()
                    if name:
                        new_item["resolved_name"] = str(name).strip()
                    new_item["resolved_exchange"] = ex

                if resolved_key is not None:
                    idx_tokens[resolved_key] = resolved_token
            else:
                bucket = _classify_unresolved_index(item)
                new_item["resolution_status"] = "UNRESOLVED"
                new_item["theme_bucket"] = bucket

                unresolved_buckets[bucket].append({
                    "name": item.get("name"),
                    "symbol": item.get("symbol"),
                    "tradingsymbol": item.get("tradingsymbol"),
                    "exchange": item.get("exchange", "NSE"),
                })

                if sym:
                    sym_str = str(sym).strip()
                    key_ex = sym_str if ":" in sym_str else f"{ex}:{sym_str}"
                    resolved_key = key_ex
                    idx_tokens[resolved_key] = None

            taxonomy_with_tokens.append(new_item)

        _write_json(os.path.join(self.paths.mappings_dir, "index_tokens.json"), idx_tokens)
        _write_json(
            os.path.join(self.paths.mappings_dir, "index_unresolved_buckets.json"),
            {
                "generated_at_ist": self.now_ist().isoformat(),
                "counts": {k: len(v) for k, v in unresolved_buckets.items()},
                "buckets": unresolved_buckets,
            },
        )

        # persist resolved tokens back into taxonomy file
        if isinstance(taxonomy, dict):
            taxonomy_out = dict(taxonomy)
            taxonomy_out["indexes"] = taxonomy_with_tokens
        else:
            taxonomy_out = taxonomy_with_tokens

        _write_json(os.path.join(self.paths.lists_dir, "index_taxonomy.json"), taxonomy_out)

    # -----------------------
    # Readiness window
    # -----------------------
    def _call_ready_fn(self, fn) -> Tuple[bool, str]:
        try:
            result = fn()
            if isinstance(result, tuple):
                ok, reason = result
                return bool(ok), str(reason or "")
            return bool(result), ""
        except Exception as e:
            return False, str(e)

    def _taxonomy_has_module03_shape(self, data: Any) -> bool:
        return isinstance(data, dict) and isinstance(data.get("indexes", []), list)

    def _readiness_contract_issues(self) -> List[str]:
        reasons: List[str] = []

        ok1, r1 = self._call_ready_fn(self.broker_ready_fn)
        if not ok1:
            reasons.append(f"broker: {r1}")

        # WebSocket state is intentionally not a readiness blocker here:
        # - Supervisor starts Module 02 after the maintenance gate in live mode
        # - off-market validation intentionally runs without websocket
        self._call_ready_fn(self.ws_ready_fn)

        list_contracts = {
            "watchlist.json": list,
            "potential_stocks.json": list,
        }
        for fn, expected_type in list_contracts.items():
            p = os.path.join(self.paths.lists_dir, fn)
            try:
                data = _read_json(p, default=None)
            except Exception as e:
                reasons.append(f"{fn} invalid JSON: {e}")
                continue

            if not isinstance(data, expected_type):
                reasons.append(f"{fn} invalid type: expected {expected_type.__name__}")

        taxonomy_path = os.path.join(self.paths.lists_dir, "index_taxonomy.json")
        try:
            taxonomy = _read_json(taxonomy_path, default=None)
        except Exception as e:
            reasons.append(f"index_taxonomy.json invalid JSON: {e}")
            taxonomy = None

        if not self._taxonomy_has_module03_shape(taxonomy):
            reasons.append("index_taxonomy.json invalid type: expected {'indexes': [...]}")

        try:
            self.regenerate_token_maps()
        except Exception as e:
            reasons.append(f"token map generation failed: {e}")

        return reasons

    def run_readiness_checks(self) -> Tuple[bool, List[str]]:
        reasons = self._readiness_contract_issues()
        return (len(reasons) == 0), reasons

    @staticmethod
    def _validation_check_readiness_contract() -> Dict[str, bool]:
        worker = MaintenanceWorker.__new__(MaintenanceWorker)
        worker.broker_ready_fn = lambda: (True, "broker ready")
        worker.ws_ready_fn = lambda: (False, "websocket disconnected")
        worker.regenerate_token_maps = lambda: None
        worker.paths = type(
            "_Paths",
            (),
            {"lists_dir": "__validation__"},
        )()

        original_read_json = globals()["_read_json"]

        def _fake_read_json(path: str, default=None):
            if path.endswith("watchlist.json"):
                return []
            if path.endswith("potential_stocks.json"):
                return []
            if path.endswith("index_taxonomy.json"):
                return {"indexes": []}
            return default

        try:
            globals()["_read_json"] = _fake_read_json
            ready_bool = MaintenanceWorker.is_ready(worker)
            ready_tuple = MaintenanceWorker.run_readiness_checks(worker)[0]
        finally:
            globals()["_read_json"] = original_read_json

        return {
            "readiness_consistent_without_ws": (ready_bool is True and ready_tuple is True),
            "default_taxonomy_matches_module03": MaintenanceWorker._taxonomy_has_module03_shape(worker, {"indexes": []}),
        }

    # -----------------------
    # Telegram delete queue processing
    # -----------------------
    def process_delete_queue(self, keep_message_ids: Optional[set] = None) -> Dict[str, Any]:
        keep_message_ids = keep_message_ids or set()
        qpath = os.path.join(self.paths.state_dir, "delete_queue.jsonl")
        if not os.path.exists(qpath) or not self.telegram_delete_fn:
            return {"processed": 0, "deleted": 0, "failed": 0, "skipped": 0}

        processed = deleted = failed = skipped = 0
        remaining_lines = []

        with open(qpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                processed += 1
                try:
                    obj = json.loads(line)
                    mid = obj.get("message_id")
                    if mid in keep_message_ids:
                        skipped += 1
                        continue

                    ok, reason = self.telegram_delete_fn(mid)
                    if ok:
                        deleted += 1
                    else:
                        failed += 1
                        obj["delete_failed_reason"] = reason
                        remaining_lines.append(json.dumps(obj, ensure_ascii=False))
                except Exception as e:
                    failed += 1
                    remaining_lines.append(line)

        # rewrite queue with failures only
        tmp = qpath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for l in remaining_lines:
                f.write(l + "\n")
        os.replace(tmp, qpath)

        return {"processed": processed, "deleted": deleted, "failed": failed, "skipped": skipped}

    # -----------------------
    # Main scheduler loop (Colab-safe)
    # -----------------------
    def maintenance_loop(self, supervisor_state_getter):
        """
        supervisor_state_getter should return dict:
        {
          "bot_state": "...",
          "ws_snapshot": {...},
          "last_scan_time_ist": "...",
          "last_menu_id": "..."
        }
        """
        cfg_path = os.path.join(self.paths.config_dir, "runtime_config.json")
        last_heartbeat_minute = None
        executed_today = {
            "instrument_download": False,
            "readiness": False,
            "post_close": False,
            "market_list_slots": set(),
        }

        while True:
            cfg = _read_json(cfg_path, {})
            now = self.now_ist()
            date_str = now.strftime("%Y-%m-%d")
            hhmm = now.strftime("%H:%M")

            # reset daily flags on new day
            if executed_today.get("_date") != date_str:
                executed_today = {
                    "_date": date_str,
                    "instrument_download": False,
                    "readiness": False,
                    "post_close": False,
                    "market_list_slots": set(),
                }

            # heartbeat every N minutes
            hb_mins = int(cfg.get("heartbeat_minutes", 15))
            if now.minute % hb_mins == 0 and last_heartbeat_minute != now.minute:
                snap = supervisor_state_getter() or {}
                self.write_heartbeat(
                    bot_state=snap.get("bot_state", BotState.NOT_READY),
                    ws_snapshot=snap.get("ws_snapshot", {}),
                    last_scan=snap.get("last_scan_time_ist"),
                    last_menu_id=snap.get("last_menu_id")
                )
                last_heartbeat_minute = now.minute

            # A) 08:30 instrument master download
            if (hhmm == cfg.get("instrument_download_time", "08:30")) and not executed_today["instrument_download"]:
                try:
                    # NOTE: implement download_instrument_master() before enabling
                    new_path = self.download_instrument_master()
                    watchlist = _read_json(os.path.join(self.paths.lists_dir, "watchlist.json"), default=[])
                    taxonomy = _read_json(os.path.join(self.paths.lists_dir, "index_taxonomy.json"), default=[])
                    ok, reasons = self.validate_instrument_master(new_path, sample_symbols=watchlist[:10], index_taxonomy=taxonomy)

                    if ok:
                        self.promote_instrument_master(new_path)
                        self.regenerate_token_maps()
                        market_lists_status = self.refresh_daily_market_lists_safe()
                        self.write_daily_state(
                            BotState.NOT_READY,
                            ["instrument master updated; awaiting readiness window"],
                            extras={"market_lists_status": market_lists_status},
                        )
                    else:
                        self.write_daily_state(BotState.PAUSED, reasons)
                        self.alert_fn("CRITICAL", "Instrument master validation failed", "\n".join(reasons), tags=["module07", "instruments"])
                except Exception as e:
                    self.write_daily_state(BotState.PAUSED, [f"instrument download failed: {e}"])
                    self.alert_fn("CRITICAL", "Instrument master download failed", str(e), tags=["module07", "instruments"])
                executed_today["instrument_download"] = True

            # B) readiness window (08:45–09:10)
            win = cfg.get("readiness_window", {"start": "08:45", "end": "09:10"})
            if win["start"] <= hhmm <= win["end"] and not executed_today["readiness"]:
                ok, reasons = self.run_readiness_checks()
                if ok:
                    self.write_daily_state(BotState.READY, [])
                    self.alert_fn("INFO", "Pre-market readiness OK", "READY ✅", tags=["module07", "readiness"])
                else:
                    self.write_daily_state(BotState.PAUSED, reasons)
                    self.alert_fn("CRITICAL", "Pre-market readiness failed", "\n".join(reasons), tags=["module07", "readiness"])
                executed_today["readiness"] = True

            # C) market-list refresh schedule (pre-open, post-open, hourly)
            preopen_time = cfg.get("market_list_preopen_time", "09:00")
            postopen_time = cfg.get("market_list_postopen_time", "09:20")
            hourly_times = cfg.get("market_list_hourly_times", ["10:15", "11:15", "12:15", "13:15", "14:15"]) or []

            scheduled_slots = [preopen_time, postopen_time] + list(hourly_times)

            if hhmm in scheduled_slots and hhmm not in executed_today["market_list_slots"]:
                try:
                    market_lists_status = self.refresh_daily_market_lists_safe()
                    self.alert_fn(
                        "INFO",
                        "Market lists refreshed",
                        json.dumps(market_lists_status),
                        tags=["module07", "market_lists"]
                    )
                except Exception as e:
                    self.alert_fn(
                        "WARNING",
                        "Market lists refresh failed",
                        str(e),
                        tags=["module07", "market_lists"]
                    )
                executed_today["market_list_slots"].add(hhmm)

            # D) post-close +30m (hook this to your market-close logic or config)
            # For now this is a placeholder trigger; you can compute close time from calendar later.
            # Example: run at 16:00 IST (3:30 close + 30m)
            if hhmm == "16:00" and not executed_today["post_close"]:
                archive_status = self.archive_and_rotate_daily_market_lists()

                self.alert_fn(
                    "INFO",
                    "Archived previous-day market lists",
                    json.dumps(archive_status),
                    tags=["module07", "market_lists", "archive"]
                )

                self.write_daily_state(BotState.PAUSED, ["post-close housekeeping"])
                # keep summary message id if stored somewhere; pass it in keep_message_ids
                stats = self.process_delete_queue(keep_message_ids=set())
                self.alert_fn("INFO", "Delete queue processed", json.dumps(stats), tags=["module07", "telegram"])
                executed_today["post_close"] = True

            time.sleep(30)
