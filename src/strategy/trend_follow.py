"""
Trend-following simulator. Single-entry long/short with strict SL + trailing TP.

역추세(DCA) 전략의 반대방향 헤지용. 추세가 강하게 나올 때만 진입해서
- Fixed TP / Fixed SL
- Trailing stop (peak 대비 역행 시 종료)
- 반대 EMA crossover 시 조기 종료

중요: DCA 없음. 단일 진입. 손절 명확.
"""
from __future__ import annotations
from typing import NamedTuple
import numpy as np
import pandas as pd


class TrendTrade(NamedTuple):
    open_time: pd.Timestamp
    close_time: pd.Timestamp
    dca_levels_used: int      # always 1
    avg_long_price: float
    avg_short_price: float
    close_price: float
    long_notional: float
    short_notional: float
    pnl_usd: float
    pnl_pct_price: float
    exit_reason: str          # "TP" | "SL" | "TRAIL" | "REV" | "EOD"


class TrendFollowSimulator:
    def __init__(self, params, backtest_cfg):
        self.p = params
        self.bc = backtest_cfg

    def run(self, df: pd.DataFrame):
        p = self.p
        bc = self.bc
        direction = p.entry_mode   # "trend_long" | "trend_short"
        is_long = (direction == "trend_long")

        leverage = float(p.leverage)
        fee_rate = float(bc.fee_rate)
        slippage = float(bc.slippage)
        init_cash = float(bc.init_cash)

        closes   = df["close"].to_numpy(dtype=np.float64)
        ema_f    = df["ema_fast"].to_numpy(dtype=np.float64)
        ema_s    = df["ema_slow"].to_numpy(dtype=np.float64)
        adx      = df["trend_adx"].to_numpy(dtype=np.float64)
        slope    = df["ema_fast_slope"].to_numpy(dtype=np.float64)
        cross_up = df["ema_cross_up"].to_numpy(dtype=np.int8)
        cross_dn = df["ema_cross_dn"].to_numpy(dtype=np.int8)
        timestamps = df.index

        cross_window = int(getattr(p, "trend_cross_window", 60))   # crossover 후 N봉 내만 진입
        cooldown     = int(getattr(p, "trend_cooldown", 120))      # 청산 후 cooldown 봉

        # BTC 필터
        if getattr(p, "btc_filter", False) and "btc_trend" in df.columns:
            btc_trends = df["btc_trend"].to_numpy(dtype=np.int8)
        else:
            btc_trends = np.ones(len(df), dtype=np.int8)

        notional_base = init_cash * float(getattr(p, "trend_margin_pct", 0.02)) * leverage

        adx_thresh   = float(getattr(p, "trend_adx_thresh", 25.0))
        slope_thresh = float(getattr(p, "trend_slope_thresh", 0.05))
        tp_pct       = float(getattr(p, "trend_tp_pct", 0.02))
        sl_pct       = float(getattr(p, "trend_sl_pct", 0.01))
        trail_pct    = float(getattr(p, "trend_trail_pct", 0.005))

        slip1p = 1.0 + slippage
        slip1m = 1.0 - slippage

        capital = init_cash
        min_capital = init_cash * 0.05

        pos_notional = 0.0
        pos_qty      = 0.0
        entry_price  = 0.0
        peak_price   = 0.0      # 롱: 최고가, 숏: 최저가
        open_time = None
        last_cross_bar = -10**9   # 최근 crossover 봉
        last_exit_bar  = -10**9   # 최근 청산 봉 (cooldown용)

        equity = np.empty(len(df), dtype=np.float64)
        trades: list[TrendTrade] = []

        for i in range(len(df)):
            price = closes[i]

            # equity
            if pos_qty == 0.0:
                equity[i] = capital
            else:
                if is_long:
                    upnl = (price - entry_price) * pos_qty
                else:
                    upnl = (entry_price - price) * pos_qty
                equity[i] = capital + upnl

            # crossover 발생 봉 기록 (포지션 유무와 관계없이)
            if is_long and cross_up[i] == 1:
                last_cross_bar = i
            elif (not is_long) and cross_dn[i] == 1:
                last_cross_bar = i

            if pos_qty == 0.0:
                if capital < min_capital:
                    continue

                # 진입 조건
                if np.isnan(ema_f[i]) or np.isnan(ema_s[i]) or np.isnan(adx[i]) or np.isnan(slope[i]):
                    continue

                within_window = (i - last_cross_bar) <= cross_window
                cooled        = (i - last_exit_bar)  >= cooldown

                if is_long:
                    entry_ok = (
                        within_window
                        and cooled
                        and ema_f[i] > ema_s[i]
                        and price > ema_f[i]
                        and adx[i] >= adx_thresh
                        and slope[i] >= slope_thresh
                        and btc_trends[i] >= 0
                    )
                else:
                    entry_ok = (
                        within_window
                        and cooled
                        and ema_f[i] < ema_s[i]
                        and price < ema_f[i]
                        and adx[i] >= adx_thresh
                        and slope[i] <= -slope_thresh
                        and btc_trends[i] <= 0
                    )

                if entry_ok:
                    ep = price * (slip1p if is_long else slip1m)
                    qty = notional_base / ep
                    pos_notional = ep * qty
                    pos_qty = qty
                    entry_price = ep
                    peak_price = price
                    capital -= notional_base * fee_rate * slip1p
                    open_time = timestamps[i]

            else:
                # 포지션 보유 중 - 청산 판정
                # 최고/최저 갱신
                if is_long:
                    if price > peak_price:
                        peak_price = price
                else:
                    if price < peak_price:
                        peak_price = price

                reason = None
                # TP
                if is_long:
                    if price >= entry_price * (1.0 + tp_pct):
                        reason = "TP"
                    elif price <= entry_price * (1.0 - sl_pct):
                        reason = "SL"
                    elif peak_price > entry_price and price <= peak_price * (1.0 - trail_pct):
                        reason = "TRAIL"
                    elif ema_f[i] < ema_s[i]:
                        reason = "REV"
                else:
                    if price <= entry_price * (1.0 - tp_pct):
                        reason = "TP"
                    elif price >= entry_price * (1.0 + sl_pct):
                        reason = "SL"
                    elif peak_price < entry_price and price >= peak_price * (1.0 + trail_pct):
                        reason = "TRAIL"
                    elif ema_f[i] > ema_s[i]:
                        reason = "REV"

                if reason is not None:
                    ep = price * (slip1m if is_long else slip1p)
                    if is_long:
                        pnl = (ep - entry_price) * pos_qty
                        pct = (price - entry_price) / entry_price
                    else:
                        pnl = (entry_price - ep) * pos_qty
                        pct = (entry_price - price) / entry_price
                    fee = pos_notional * fee_rate * slip1p
                    capital += pnl - fee
                    trades.append(TrendTrade(
                        open_time=open_time, close_time=timestamps[i],
                        dca_levels_used=1,
                        avg_long_price=entry_price if is_long else 0.0,
                        avg_short_price=entry_price if not is_long else 0.0,
                        close_price=price,
                        long_notional=pos_notional if is_long else 0.0,
                        short_notional=pos_notional if not is_long else 0.0,
                        pnl_usd=pnl - fee,
                        pnl_pct_price=pct,
                        exit_reason=reason,
                    ))
                    pos_notional = pos_qty = entry_price = peak_price = 0.0
                    last_exit_bar = i

        # EOD
        if pos_qty > 0.0:
            price = closes[-1]
            ep = price * (slip1m if is_long else slip1p)
            if is_long:
                pnl = (ep - entry_price) * pos_qty
                pct = (price - entry_price) / entry_price
            else:
                pnl = (entry_price - ep) * pos_qty
                pct = (entry_price - price) / entry_price
            fee = pos_notional * fee_rate * slip1p
            capital += pnl - fee
            trades.append(TrendTrade(
                open_time=open_time, close_time=timestamps[-1],
                dca_levels_used=1,
                avg_long_price=entry_price if is_long else 0.0,
                avg_short_price=entry_price if not is_long else 0.0,
                close_price=price,
                long_notional=pos_notional if is_long else 0.0,
                short_notional=pos_notional if not is_long else 0.0,
                pnl_usd=pnl - fee,
                pnl_pct_price=pct,
                exit_reason="EOD",
            ))

        return pd.Series(equity, index=df.index), trades
