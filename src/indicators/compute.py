"""
All indicators computed in one place.
Uses the `ta` library (pip install ta).
No look-ahead bias: every indicator is shifted by 1 bar before use.
"""
import pandas as pd
import numpy as np
import ta
from typing import Optional
from scipy.signal import argrelmin, argrelmax


def add_indicators(df: pd.DataFrame, params) -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    # --- RSI ---
    rsi_source = getattr(params, "rsi_source", "close")
    if rsi_source == "ohlc4":
        rsi_src = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    else:
        rsi_src = close
    rsi_ind = ta.momentum.RSIIndicator(rsi_src, window=params.rsi_period)
    rsi_raw = rsi_ind.rsi()
    df["rsi"]      = rsi_raw.shift(1)       # 1봉 래그 (룩어헤드 방지)
    df["rsi_prev"] = rsi_raw.shift(2)       # 2봉 래그 → "이전 봉의 RSI"

    # --- Bollinger Bands ---
    bb_ind = ta.volatility.BollingerBands(close, window=params.bb_period, window_dev=params.bb_std)
    df["bb_lower"] = bb_ind.bollinger_lband().shift(1)
    df["bb_upper"] = bb_ind.bollinger_hband().shift(1)

    # --- MACD ---
    macd_ind = ta.trend.MACD(
        close,
        window_fast=params.macd_fast,
        window_slow=params.macd_slow,
        window_sign=params.macd_signal,
    )
    df["macd_hist"] = macd_ind.macd_diff().shift(1)

    # --- Volume ratio ---
    vol_sma = volume.rolling(params.vol_window).mean()
    df["vol_ratio"] = (volume / vol_sma).shift(1)

    return df


def add_divergence_signals(df: pd.DataFrame, params) -> pd.DataFrame:
    """역추세 다이버전스 신호: 연속 음봉/양봉, EMA 진입 필터, 불리쉬/베어리쉬 다이버전스."""
    df = df.copy()
    close  = df["close"].to_numpy(dtype=np.float64)
    opens  = df["open"].to_numpy(dtype=np.float64)
    rsi_arr   = df["rsi"].to_numpy(dtype=np.float64)    # 이미 shift(1)
    macd_arr  = df["macd_hist"].to_numpy(dtype=np.float64)  # 이미 shift(1)

    # ── 연속 음봉/양봉 카운트 ─────────────────────────────────────────────
    is_red   = close < opens
    is_green = close > opens
    red_cnt   = np.zeros(len(df), dtype=np.int32)
    green_cnt = np.zeros(len(df), dtype=np.int32)
    for i in range(1, len(df)):
        red_cnt[i]   = red_cnt[i - 1]   + 1 if is_red[i]   else 0
        green_cnt[i] = green_cnt[i - 1] + 1 if is_green[i] else 0
    df["red_candle_count"]   = pd.Series(red_cnt,   index=df.index).shift(1).fillna(0).astype(np.int32)
    df["green_candle_count"] = pd.Series(green_cnt, index=df.index).shift(1).fillna(0).astype(np.int32)

    # ── EMA 진입 필터 ─────────────────────────────────────────────────────
    ema_period = int(getattr(params, "ema_entry_period", 30))
    df["ema_entry"] = ta.trend.EMAIndicator(df["close"], window=ema_period).ema_indicator().shift(1)

    # ── 불리쉬/베어리쉬 다이버전스 (Pivot Low/High 기반) ─────────────────
    pivot_order = 5
    lookback    = 100

    pl_idx = argrelmin(close, order=pivot_order)[0]
    ph_idx = argrelmax(close, order=pivot_order)[0]

    # 불리쉬: 가격 Lower Low + RSI/MACD Higher Low → 하락 약화 → 롱 진입
    bull_sig = np.zeros(len(df), dtype=np.int8)
    for cur in pl_idx:
        prev_pls = pl_idx[(pl_idx >= cur - lookback) & (pl_idx < cur - pivot_order)]
        if len(prev_pls) == 0:
            continue
        prev = prev_pls[-1]
        price_ll = close[cur]   < close[prev]
        rsi_hl   = rsi_arr[cur] > rsi_arr[prev]
        macd_hl  = macd_arr[cur] > macd_arr[prev]
        if price_ll and (rsi_hl or macd_hl):
            bull_sig[cur] = 1

    # 베어리쉬: 가격 Higher High + RSI/MACD Lower High → 상승 약화 → 숏 진입
    bear_sig = np.zeros(len(df), dtype=np.int8)
    for cur in ph_idx:
        prev_phs = ph_idx[(ph_idx >= cur - lookback) & (ph_idx < cur - pivot_order)]
        if len(prev_phs) == 0:
            continue
        prev = prev_phs[-1]
        price_hh = close[cur]   > close[prev]
        rsi_lh   = rsi_arr[cur] < rsi_arr[prev]
        macd_lh  = macd_arr[cur] < macd_arr[prev]
        if price_hh and (rsi_lh or macd_lh):
            bear_sig[cur] = 1

    df["bull_div"] = pd.Series(bull_sig, index=df.index).shift(1).fillna(0).astype(np.int8)
    df["bear_div"] = pd.Series(bear_sig, index=df.index).shift(1).fillna(0).astype(np.int8)

    return df


def add_trend_signals(df: pd.DataFrame, params) -> pd.DataFrame:
    """추세매매 신호: EMA fast/slow 크로스, ADX 강도, 추세 방향.

    - ema_fast, ema_slow: 단기/장기 EMA
    - ema_cross_up:   fast가 slow를 상향 돌파한 봉 (bullish cross)
    - ema_cross_dn:   fast가 slow를 하향 돌파한 봉 (bearish cross)
    - trend_adx:      ADX 값 (추세 강도)
    - ema_fast_slope: fast EMA slope (방향 확인용)
    모두 shift(1)로 lookahead 방지.
    """
    df = df.copy()
    fast = int(getattr(params, "trend_ema_fast", 20))
    slow = int(getattr(params, "trend_ema_slow", 50))
    adx_win = int(getattr(params, "trend_adx_period", 14))

    close = df["close"]
    ema_f = ta.trend.EMAIndicator(close, window=fast).ema_indicator()
    ema_s = ta.trend.EMAIndicator(close, window=slow).ema_indicator()

    cross_up = (ema_f > ema_s) & (ema_f.shift(1) <= ema_s.shift(1))
    cross_dn = (ema_f < ema_s) & (ema_f.shift(1) >= ema_s.shift(1))

    adx = ta.trend.ADXIndicator(df["high"], df["low"], close, window=adx_win).adx()
    ema_f_slope = ema_f.pct_change(5) * 100   # 5봉 slope %

    df["ema_fast"]        = ema_f.shift(1)
    df["ema_slow"]        = ema_s.shift(1)
    df["ema_cross_up"]    = cross_up.shift(1).fillna(False).astype(np.int8)
    df["ema_cross_dn"]    = cross_dn.shift(1).fillna(False).astype(np.int8)
    df["trend_adx"]       = adx.shift(1)
    df["ema_fast_slope"]  = ema_f_slope.shift(1)
    return df


def add_regime(df: pd.DataFrame, regime_cfg) -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # ADX
    adx_ind = ta.trend.ADXIndicator(high, low, close, window=regime_cfg.adx_period)
    adx = adx_ind.adx()
    dmp = adx_ind.adx_pos()
    dmn = adx_ind.adx_neg()

    # EMA slope
    ema = ta.trend.EMAIndicator(close, window=regime_cfg.ema_period).ema_indicator()
    ema_slope = ema.diff(5) / ema.shift(5) * 100

    # ATR ratio
    atr = ta.volatility.AverageTrueRange(high, low, close, window=regime_cfg.atr_period).average_true_range()
    atr_mean = atr.rolling(regime_cfg.atr_lookback).mean()
    atr_ratio = atr / (atr_mean + 1e-10)

    # Regime classification
    # 0 = ranging/neutral
    # 1 = weak uptrend
    # 2 = weak downtrend
    # 3 = STRONG downtrend (block DCA entries)
    regime = pd.Series(0, index=close.index, dtype=np.int8)

    strong_trend = adx > regime_cfg.adx_strong_thresh
    bearish_di = dmn > dmp
    bearish_slope = ema_slope < -0.05

    regime[strong_trend & bearish_di & bearish_slope] = 3
    regime[(~strong_trend) & bearish_di & (ema_slope < 0)] = 2
    regime[(~strong_trend) & (~bearish_di) & (ema_slope > 0)] = 1

    df["regime"] = regime.shift(1).fillna(0).astype(np.int8)

    return df


def compute_btc_trend(
    btc_4h: pd.DataFrame,
    ema_period: int = 200,
    slope_filter: bool = False,
    slope_window: int = 5,
) -> pd.Series:
    """BTC 4h EMA200 기반 추세: 가격 < EMA200 이면 하락(-1), 아니면 중립/상승(1).

    slope_filter=True 이면 EMA 기울기까지 음수여야 차단 (차단율 ~25%).
    반환값: 1 / -1  (1m 인덱스에 forward-fill 필요)
    """
    close_4h = btc_4h["close"]
    ema = ta.trend.EMAIndicator(close_4h, window=ema_period).ema_indicator()
    below_ema = close_4h < ema
    if slope_filter:
        bearish_slope = ema.diff(slope_window) < 0
        block = below_ema & bearish_slope
    else:
        block = below_ema
    trend = pd.Series(1, index=close_4h.index, dtype=np.int8)
    trend[block] = -1
    return trend.shift(1).fillna(1).astype(np.int8)  # 1봉 래그


def add_btc_filter(df_1m: pd.DataFrame, btc_trend_4h: pd.Series) -> pd.DataFrame:
    """BTC 4h 추세를 1m 프레임에 forward-fill로 병합."""
    df = df_1m.copy()
    trend_1m = btc_trend_4h.reindex(df.index, method="ffill").fillna(1).astype(np.int8)
    df["btc_trend"] = trend_1m
    return df


def prepare(
    df: pd.DataFrame,
    strategy_params,
    regime_cfg,
    btc_trend_4h: Optional[pd.Series] = None,
) -> pd.DataFrame:
    df = add_indicators(df, strategy_params)
    df = add_regime(df, regime_cfg)
    if btc_trend_4h is not None:
        df = add_btc_filter(df, btc_trend_4h)
    else:
        df["btc_trend"] = np.int8(1)
    entry_mode = getattr(strategy_params, "entry_mode", "rsi_bb")
    if entry_mode in ("divergence", "short_divergence"):
        df = add_divergence_signals(df, strategy_params)
    if entry_mode in ("trend_long", "trend_short"):
        df = add_trend_signals(df, strategy_params)
    df.dropna(inplace=True)
    return df
