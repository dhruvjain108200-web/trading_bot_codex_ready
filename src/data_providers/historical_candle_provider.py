from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


@dataclass
class HistoricalCandleProvider:
    broker: Any
    exchange_map: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        if self.exchange_map is None:
            self.exchange_map = {
                "NSE": "NSE",
                "BSE": "BSE",
                "NFO": "NFO",
                "MCX": "MCX",
                "CDS": "CDS",
            }

    def get_5m_candles(
        self,
        symbol: str,
        token: str,
        exchange: str,
        lookback: int,
    ) -> List[Dict[str, Any]]:
        """
        Return normalized 5-minute candles:
        [
            {
                "ts": "...",
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": int
            }
        ]
        """
        if not token:
            return []

        exch = self.exchange_map.get(str(exchange).strip().upper(), str(exchange).strip().upper())
        now = datetime.now()
        days = max(3, int((lookback * 5) / 375) + 2)  # safe small buffer for intraday lookback
        from_dt = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
        to_dt = now.strftime("%Y-%m-%d %H:%M")

        params = {
            "exchange": exch,
            "symboltoken": str(token),
            "interval": "FIVE_MINUTE",
            "fromdate": from_dt,
            "todate": to_dt,
        }

        data = self.broker.safe_call(
            lambda: self.broker.smart.getCandleData(params),
            purpose="getCandleData"
        )
        if not data:
            return []

        rows = data.get("data", []) if isinstance(data, dict) else []
        out: List[Dict[str, Any]] = []

        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            try:
                out.append({
                    "ts": row[0],
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": int(float(row[5])),
                })
            except Exception:
                continue

        if lookback > 0:
            out = out[-lookback:]

        return out
