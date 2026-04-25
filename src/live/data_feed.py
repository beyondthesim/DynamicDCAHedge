"""실시간 1분봉 데이터 피드.

매 분 마감 후:
  1. 최근 N개 1분봉 fetch
  2. BTC 4h 트렌드 fetch
  3. prepare()로 지표 계산
  4. 마지막 봉 (방금 마감한 봉)을 반환

전략은 prepare된 마지막 봉의 지표 값을 보고 진입/청산 결정.
"""
from __future__ import annotations
import time
import logging
import warnings
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from .okx_trader import OKXTrader
from ..indicators.compute import prepare, compute_btc_trend

logger = logging.getLogger(__name__)


class DataFeed:
    def __init__(self, trader: OKXTrader, symbol: str,
                 lookback_bars: int = 2000):
        """
        Args:
            lookback_bars: 1분봉 몇 개를 fetch 할지. 지표 워밍업 위해 충분히
        """
        self.trader = trader
        self.symbol = symbol
        self.lookback = lookback_bars
        self._btc_cache_at = 0
        self._btc_trend = None

    def _fetch_btc_trend(self) -> pd.Series:
        """BTC 4h 트렌드를 1시간 캐시."""
        now = time.time()
        if self._btc_trend is not None and (now - self._btc_cache_at) < 3600:
            return self._btc_trend
        # BTC 4h 약 250개 (40일치) — EMA200 + slope_window
        rows = self.trader.fetch_ohlcv("BTC/USDT:USDT", "4h", limit=300)
        df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df.set_index("t")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self._btc_trend = compute_btc_trend(df, ema_period=200, slope_filter=True, slope_window=5)
        self._btc_cache_at = now
        return self._btc_trend

    def fetch_prepared(self, params) -> pd.DataFrame:
        """전략 파라미터에 맞춰 지표 계산된 DataFrame 반환."""
        btc = self._fetch_btc_trend()
        rows = self.trader.fetch_ohlcv(self.symbol, "1m", limit=self.lookback)
        df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df = df.set_index("t")
        # 마지막 봉이 아직 진행 중일 수 있어 봉 마감 시각 기준으로 자름
        last_complete = df.index[-2]   # 직전 봉이 closed
        df = df[df.index <= last_complete]
        # config 임포트해서 regime 사용
        from ..utils.config import load_config
        cfg = load_config()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_feat = prepare(df, params, cfg.regime, btc)
        return df_feat

    @staticmethod
    def wait_for_next_bar(buffer_sec: int = 5):
        """다음 분의 buffer_sec 시점까지 대기 (봉 마감 후 데이터 반영 시간)."""
        now = datetime.now(timezone.utc)
        secs_to_next = 60 - now.second + buffer_sec
        if secs_to_next > 60: secs_to_next -= 60
        logger.info("다음 봉까지 %ds 대기...", secs_to_next)
        time.sleep(secs_to_next)
