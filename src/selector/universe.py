"""유니버스 스캐너: OKX USDT 무기한 선물 중 후보 코인 추출.

소스 2가지:
  1. 신규 상장: listTime 최근 N일 이내
  2. 탑 게이너: 24h price change 상위 K개

각 코인의 메타데이터 (상장일, 거래량, 가격 변동) 정규화 반환.
"""
from __future__ import annotations
import time
import logging
from dataclasses import dataclass, asdict
from typing import Optional
import ccxt

logger = logging.getLogger(__name__)


@dataclass
class CoinCandidate:
    symbol: str
    base: str
    list_time_ms: int
    days_listed: float
    last_price: float
    quote_volume_24h_usd: float
    price_change_24h_pct: float
    contract_size: float
    min_amount: float
    source: str   # "new_listing" | "top_gainer" | "both"

    def asdict(self) -> dict:
        return asdict(self)


class UniverseScanner:
    """OKX 마켓에서 후보 코인을 발굴.

    Args:
        new_listing_max_days: 상장 N일 이내만 신규 후보로
        new_listing_min_days: 최소 N일 (rug 1차 통과)
        top_gainer_top_k: 탑 게이너 상위 K개
        min_volume_usd: 최소 24h 거래량 (USD)
    """
    def __init__(
        self,
        new_listing_max_days: int = 120,
        new_listing_min_days: int = 14,
        top_gainer_top_k: int = 20,
        min_volume_usd: float = 1_000_000,
        ex: Optional[ccxt.Exchange] = None,
        excluded_bases: Optional[set[str]] = None,
    ):
        self.new_listing_max_days = new_listing_max_days
        self.new_listing_min_days = new_listing_min_days
        self.top_gainer_top_k = top_gainer_top_k
        self.min_volume_usd = min_volume_usd
        # 다른 전략 점유 + 고정 종목 — 스캔 결과에서 제외할 base 코인 셋
        self.excluded_bases = {b.upper() for b in (excluded_bases or set())}
        self.ex = ex or ccxt.okx({
            "options": {"defaultType": "swap"},
            "enableRateLimit": True,
        })

    def _all_swaps(self) -> list[dict]:
        markets = self.ex.load_markets(reload=True)
        return [m for m in markets.values()
                if m.get("type")=="swap"
                and m.get("settle")=="USDT"
                and m.get("active")]

    def _meta(self, m: dict, ticker: dict, source: str) -> Optional[CoinCandidate]:
        info = ticker.get("info", {}) if ticker else {}
        list_time = m.get("info", {}).get("listTime")
        if not list_time: return None
        list_time = int(list_time)
        days = (time.time() * 1000 - list_time) / 86_400_000

        quote_vol = float(info.get("volCcy24h") or ticker.get("quoteVolume") or 0)
        last = float(ticker.get("last") or info.get("last") or 0)
        if last == 0: return None
        # 24h change
        open_24h = float(info.get("open24h") or 0)
        change_pct = (last/open_24h - 1) * 100 if open_24h > 0 else 0

        return CoinCandidate(
            symbol=m["symbol"],
            base=m.get("base", ""),
            list_time_ms=list_time,
            days_listed=round(days, 1),
            last_price=last,
            quote_volume_24h_usd=round(quote_vol, 0),
            price_change_24h_pct=round(change_pct, 2),
            contract_size=float(m.get("contractSize") or 1),
            min_amount=float((m.get("limits", {}).get("amount", {}) or {}).get("min") or 0.1),
            source=source,
        )

    def scan(self) -> list[CoinCandidate]:
        """전체 유니버스 스캔. 신규 + 탑게이너 통합. 제외 base 코인은 누락."""
        swaps = self._all_swaps()
        if self.excluded_bases:
            swaps = [m for m in swaps if (m.get("base") or "").upper() not in self.excluded_bases]
        symbols = [m["symbol"] for m in swaps]
        logger.info("Total active USDT swaps: %d (excluded %d bases)",
                    len(swaps), len(self.excluded_bases))

        tickers = self.ex.fetch_tickers(symbols)

        results: dict[str, CoinCandidate] = {}

        # 1) 신규 상장
        new_count = 0
        for m in swaps:
            list_time = m.get("info", {}).get("listTime")
            if not list_time: continue
            days = (time.time() * 1000 - int(list_time)) / 86_400_000
            if not (self.new_listing_min_days <= days <= self.new_listing_max_days):
                continue
            t = tickers.get(m["symbol"], {})
            cand = self._meta(m, t, source="new_listing")
            if cand and cand.quote_volume_24h_usd >= self.min_volume_usd:
                results[cand.symbol] = cand
                new_count += 1

        # 2) 탑 게이너 (24h)
        all_with_change = []
        for m in swaps:
            t = tickers.get(m["symbol"], {})
            info = t.get("info", {}) if t else {}
            quote_vol = float(info.get("volCcy24h") or t.get("quoteVolume") or 0)
            if quote_vol < self.min_volume_usd: continue
            cand = self._meta(m, t, source="top_gainer")
            if cand:
                all_with_change.append(cand)
        # 절대값 변동 (양/음 모두 큰 것)
        all_with_change.sort(key=lambda x: abs(x.price_change_24h_pct), reverse=True)
        top_count = 0
        for cand in all_with_change[:self.top_gainer_top_k]:
            if cand.symbol in results:
                results[cand.symbol].source = "both"
            else:
                results[cand.symbol] = cand
            top_count += 1

        logger.info("Scan: %d 신규 + %d 탑게이너 → 총 %d (중복 제거)",
                    new_count, top_count, len(results))
        return list(results.values())
