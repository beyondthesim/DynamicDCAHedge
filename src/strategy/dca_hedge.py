"""
DCA + Hedge Strategy Simulator.

Long DCA up to 7 levels. At the `hedge_entry`-th DCA level, simultaneously
open a short position equal to `hedge_ratio` × total long notional.

Key design principles:
  - DCA entries are checked BEFORE TP/SL on every bar (avoids premature stops)
  - Multiple DCA levels can trigger on the same bar if price drops a lot
  - TP/SL are price-based relative to avg long entry for cleaner logic
  - Regime filter blocks entries in strong downtrends

Accounting model (futures, leveraged):
  - Opening a position only costs fees (margin is collateral, not spent)
  - equity = init_cash + sum(realized_pnl) + unrealized_pnl - fees
  - All "notional" values are in USD (already leverage-adjusted)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple
import numpy as np
import pandas as pd


class TradeRecord(NamedTuple):
    open_time: pd.Timestamp
    close_time: pd.Timestamp
    dca_levels_used: int
    avg_long_price: float
    avg_short_price: float   # 0 if no hedge
    close_price: float
    long_notional: float
    short_notional: float    # 0 if no hedge
    pnl_usd: float
    pnl_pct_price: float     # (close - avg_long) / avg_long
    exit_reason: str         # "TP" | "SL" | "EOD"


@dataclass
class _LegPosition:
    entries: list[tuple[float, float]] = field(default_factory=list)  # (exec_price, notional)

    @property
    def total_notional(self) -> float:
        return sum(n for _, n in self.entries)

    @property
    def avg_price(self) -> float:
        if not self.entries:
            return 0.0
        return sum(p * n for p, n in self.entries) / self.total_notional

    def add(self, exec_price: float, notional: float) -> None:
        self.entries.append((exec_price, notional))

    def unrealized_long_pnl(self, current_price: float) -> float:
        ap = self.avg_price
        if ap == 0:
            return 0.0
        qty = self.total_notional / ap
        return (current_price - ap) * qty

    def unrealized_short_pnl(self, current_price: float) -> float:
        ap = self.avg_price
        if ap == 0:
            return 0.0
        qty = self.total_notional / ap
        return (ap - current_price) * qty


class DCAHedgeSimulator:
    def __init__(self, params, backtest_cfg):
        self.p = params
        self.bc = backtest_cfg

    def run(self, df: pd.DataFrame) -> tuple[pd.Series, list[TradeRecord]]:
        p = self.p
        bc = self.bc

        leverage = float(p.leverage)
        fee_rate = float(bc.fee_rate)
        slippage = float(bc.slippage)
        init_cash = float(bc.init_cash)

        dca_drops = np.array(p.dca_drops, dtype=np.float64)   # (6,) cumulative
        size_mults = np.array(p.size_mults, dtype=np.float64)  # (7,)

        # Pre-extract columns to numpy arrays — avoids pandas per-row overhead
        closes    = df["close"].to_numpy(dtype=np.float64)
        regimes   = df["regime"].to_numpy(dtype=np.int8)
        rsis      = df["rsi"].to_numpy(dtype=np.float64)
        bb_lowers = df["bb_lower"].to_numpy(dtype=np.float64)
        macd_hist = df["macd_hist"].to_numpy(dtype=np.float64)
        vol_ratio = df["vol_ratio"].to_numpy(dtype=np.float64)
        timestamps = df.index

        # Pre-compute per-level notional bases (constant across loop)
        notional_base = init_cash * p.base_margin_pct * leverage  # level-1 base

        # Running account value (fees reduce this)
        capital = init_cash
        min_capital = init_cash * 0.05  # stop trading if 95% is lost

        long_pos = _LegPosition()
        short_pos = _LegPosition()
        dca_level = 0
        first_entry_price = 0.0
        open_time: pd.Timestamp | None = None

        equity = np.empty(len(df), dtype=np.float64)
        trades: list[TradeRecord] = []

        rsi_thresh    = float(p.rsi_thresh)
        vol_thresh    = float(p.vol_thresh)
        tp_pct        = float(p.tp_pct)
        sl_pct        = float(p.sl_pct)
        sl_active     = bool(p.sl_active)
        hedge_entry   = int(p.hedge_entry)
        hedge_ratio   = float(p.hedge_ratio)
        dynamic_hedge = bool(getattr(p, "dynamic_hedge", False))
        # ── 고급 옵션 ──────────────────────────────────────────────────────
        rsi_mode      = getattr(p, "rsi_mode", "below")   # "below" | "rising"
        tp1_pct       = float(getattr(p, "tp1_pct", 0.0))
        tp1_size      = float(getattr(p, "tp1_size", 0.5))
        use_btc_filt  = bool(getattr(p, "btc_filter", False))
        slip1p = 1.0 + slippage
        slip1m = 1.0 - slippage

        # BTC 필터 컬럼 (없으면 전부 허용)
        if use_btc_filt and "btc_trend" in df.columns:
            btc_trends = df["btc_trend"].to_numpy(dtype=np.int8)
        else:
            btc_trends = np.ones(len(df), dtype=np.int8)

        # ── 역추세 다이버전스 모드 파라미터 ──────────────────────────────
        entry_mode     = getattr(p, "entry_mode", "rsi_bb")
        red_candle_n   = int(getattr(p, "red_candle_n", 4))
        dca_next_drop  = float(getattr(p, "dca_next_drop", 0.013))
        all_close_pct  = float(getattr(p, "all_close_pct", 0.040))
        safe_close_n   = int(getattr(p, "safe_close_n", 4))
        safe_close_pct = float(getattr(p, "safe_close_pct", 0.020))
        loss_close_n   = int(getattr(p, "loss_close_n", 9))
        loss_close_pct = float(getattr(p, "loss_close_pct", 0.005))

        if entry_mode in ("divergence", "short_divergence"):
            red_counts   = df["red_candle_count"].to_numpy(dtype=np.int32)    if "red_candle_count"   in df.columns else np.zeros(len(df), dtype=np.int32)
            green_counts = df["green_candle_count"].to_numpy(dtype=np.int32)  if "green_candle_count" in df.columns else np.zeros(len(df), dtype=np.int32)
            ema_entry    = df["ema_entry"].to_numpy(dtype=np.float64)         if "ema_entry"          in df.columns else np.full(len(df), np.inf)
            bull_divs    = df["bull_div"].to_numpy(dtype=np.int8)             if "bull_div"           in df.columns else np.zeros(len(df), dtype=np.int8)
            bear_divs    = df["bear_div"].to_numpy(dtype=np.int8)             if "bear_div"           in df.columns else np.zeros(len(df), dtype=np.int8)

        # RSI prev 컬럼 (rising 모드용)
        if "rsi_prev" in df.columns:
            rsis_prev = df["rsi_prev"].to_numpy(dtype=np.float64)
        else:
            rsis_prev = rsis  # fallback: prev = current (rising 비활성)

        # BB 상단 컬럼 (독립 숏용)
        if "bb_upper" in df.columns:
            bb_uppers = df["bb_upper"].to_numpy(dtype=np.float64)
        else:
            bb_uppers = np.full(len(df), np.inf)

        # 독립 숏 파라미터
        enable_short     = bool(getattr(p, "enable_short", False))
        short_rsi_thresh = float(getattr(p, "short_rsi_thresh", 75.0))
        short_tp_pct     = float(getattr(p, "short_tp_pct", 0.020))
        short_sl_pct     = float(getattr(p, "short_sl_pct", 0.050))
        short_notl_base  = init_cash * float(getattr(p, "short_margin_pct", 0.010)) * leverage

        # Flat-position state — 롱
        long_notional  = 0.0
        long_qty       = 0.0
        short_notional = 0.0
        short_qty      = 0.0
        tp1_done       = False   # 부분 TP 발동 여부 (포지션당 1회)
        last_entry_price = 0.0   # 마지막 DCA 진입가 (divergence 모드)
        # 독립 숏 상태 (롱과 상호 배타적)
        ind_s_notional = 0.0
        ind_s_qty      = 0.0
        ind_s_open_t   = None

        for i in range(len(df)):
            price = closes[i]

            # Equity snapshot — skip uPnL computation when flat
            if dca_level == 0 and ind_s_qty == 0.0:
                equity[i] = capital
            else:
                avg_long_p  = long_notional  / long_qty  if long_qty  > 0 else 0.0
                avg_short_p = short_notional / short_qty if short_qty > 0 else 0.0
                avg_ind_s   = ind_s_notional / ind_s_qty if ind_s_qty > 0 else 0.0
                upnl = (
                    (price - avg_long_p)  * long_qty
                    - (price - avg_short_p) * short_qty
                    - (price - avg_ind_s)  * ind_s_qty   # 독립 숏 uPnL
                )
                equity[i] = capital + upnl

            if dca_level == 0 and ind_s_qty == 0.0:
                if capital < min_capital:
                    continue  # bankrupt — stop trading

                # ── 1차 진입 신호 판정 ────────────────────────────────────
                if entry_mode == "divergence":
                    entry_ok = (
                        red_counts[i] >= red_candle_n
                        and price <= ema_entry[i]
                        and btc_trends[i] >= 0
                        and regimes[i] != 3
                    )
                elif entry_mode == "short_divergence":
                    # 숏 역추세: 연속 양봉 >= N + close >= EMA + BTC 비강세
                    entry_ok = (
                        green_counts[i] >= red_candle_n
                        and price >= ema_entry[i]
                        and btc_trends[i] <= 0
                        and regimes[i] != 3
                    )
                else:
                    if rsi_mode == "rising":
                        rsi_ok = (rsis_prev[i] < rsi_thresh) and (rsis[i] >= rsi_thresh)
                    else:
                        rsi_ok = rsis[i] < rsi_thresh
                    entry_ok = (
                        regimes[i] != 3
                        and rsi_ok
                        and btc_trends[i] >= 0
                        and price <= bb_lowers[i]
                        and macd_hist[i] < 0.0
                        and vol_ratio[i] >= vol_thresh
                    )

                # ── Primary Entry signal ──────────────────────────────────
                if entry_ok:
                    notional = notional_base * size_mults[0]
                    fee = notional * fee_rate * slip1p
                    if entry_mode == "short_divergence":
                        ep = price * slip1m   # 숏은 낮은 가격 체결
                        qty = notional / ep
                        short_notional += ep * qty
                        short_qty      += qty
                    else:
                        ep = price * slip1p
                        qty = notional / ep
                        long_notional += ep * qty
                        long_qty      += qty
                    capital -= fee
                    dca_level = 1
                    first_entry_price = price
                    last_entry_price  = price
                    open_time = timestamps[i]
                    tp1_done = False

                # ── 독립 숏 Entry signal (롱이 없을 때만, short_divergence 아닐 때만) ─
                elif enable_short and ind_s_qty == 0.0 and entry_mode != "short_divergence":
                    if (rsis[i] > short_rsi_thresh
                            and price > bb_uppers[i] * 1.002
                            and macd_hist[i] > 0.0
                            and vol_ratio[i] >= vol_thresh
                            and regimes[i] != 3):
                        ep = price * slip1m          # 숏은 낮은 가격에 진입
                        qty = short_notl_base / ep
                        ind_s_notional = ep * qty
                        ind_s_qty      = qty
                        capital -= ind_s_notional * fee_rate * slip1p
                        ind_s_open_t = timestamps[i]

            elif ind_s_qty > 0.0:
                # ── 독립 숏 TP / SL ───────────────────────────────────────
                avg_ind_s = ind_s_notional / ind_s_qty
                move = (avg_ind_s - price) / avg_ind_s   # 숏이익: 하락이 양수
                if move >= short_tp_pct:
                    ep = price * slip1p
                    pnl = (avg_ind_s - ep) * ind_s_qty
                    fee = ind_s_notional * fee_rate * slip1p
                    capital += pnl - fee
                    trades.append(TradeRecord(
                        open_time=ind_s_open_t, close_time=timestamps[i],
                        dca_levels_used=1,
                        avg_long_price=0.0, avg_short_price=avg_ind_s,
                        close_price=price,
                        long_notional=0.0, short_notional=ind_s_notional,
                        pnl_usd=pnl - fee, pnl_pct_price=move,
                        exit_reason="TP_S",
                    ))
                    ind_s_qty = ind_s_notional = 0.0
                elif -move >= short_sl_pct:
                    ep = price * slip1p
                    pnl = (avg_ind_s - ep) * ind_s_qty
                    fee = ind_s_notional * fee_rate * slip1p
                    capital += pnl - fee
                    trades.append(TradeRecord(
                        open_time=ind_s_open_t, close_time=timestamps[i],
                        dca_levels_used=1,
                        avg_long_price=0.0, avg_short_price=avg_ind_s,
                        close_price=price,
                        long_notional=0.0, short_notional=ind_s_notional,
                        pnl_usd=pnl - fee, pnl_pct_price=move,
                        exit_reason="SL_S",
                    ))
                    ind_s_qty = ind_s_notional = 0.0

            else:
                # ── Step 1: DCA entries (checked BEFORE TP/SL) ────────────
                max_dca = len(size_mults) - 1  # size_mults[0]은 1차용

                if entry_mode == "divergence":
                    # 다이버전스 모드: bull_div 발생 + 마지막 진입가 대비 drop
                    if (dca_level < max_dca
                            and bull_divs[i] == 1
                            and price < last_entry_price * (1.0 - dca_next_drop)):
                        new_level = dca_level + 1
                        notional = notional_base * size_mults[new_level - 1]
                        fee = notional * fee_rate * slip1p
                        ep = price * slip1p
                        qty = notional / ep
                        long_notional += ep * qty
                        long_qty      += qty
                        capital -= fee
                        dca_level = new_level
                        last_entry_price = price
                        # 헤지 로직 (롱 primary → 숏 헤지)
                        if dynamic_hedge:
                            if dca_level >= hedge_entry:
                                target = long_notional * hedge_ratio
                                delta  = target - short_notional
                                if delta > 1e-8:
                                    hep  = price * slip1m
                                    hqty = delta / hep
                                    short_notional += hep * hqty
                                    short_qty      += hqty
                                    capital -= delta * fee_rate * slip1p
                        else:
                            if dca_level == hedge_entry:
                                h_notional = long_notional * hedge_ratio
                                if h_notional > 0:
                                    hedge_fee = h_notional * fee_rate * slip1p
                                    hep  = price * slip1m
                                    hqty = h_notional / hep
                                    short_notional += hep * hqty
                                    short_qty      += hqty
                                    capital -= hedge_fee
                elif entry_mode == "short_divergence":
                    # 숏 다이버전스: bear_div + 마지막 진입가 대비 상승
                    if (dca_level < max_dca
                            and bear_divs[i] == 1
                            and price > last_entry_price * (1.0 + dca_next_drop)):
                        new_level = dca_level + 1
                        notional = notional_base * size_mults[new_level - 1]
                        fee = notional * fee_rate * slip1p
                        ep = price * slip1m
                        qty = notional / ep
                        short_notional += ep * qty
                        short_qty      += qty
                        capital -= fee
                        dca_level = new_level
                        last_entry_price = price
                        # 헤지: 숏 primary → 롱 헤지
                        if dynamic_hedge:
                            if dca_level >= hedge_entry:
                                target = short_notional * hedge_ratio
                                delta  = target - long_notional
                                if delta > 1e-8:
                                    hep  = price * slip1p
                                    hqty = delta / hep
                                    long_notional += hep * hqty
                                    long_qty      += hqty
                                    capital -= delta * fee_rate * slip1p
                        else:
                            if dca_level == hedge_entry:
                                h_notional = short_notional * hedge_ratio
                                if h_notional > 0:
                                    hedge_fee = h_notional * fee_rate * slip1p
                                    hep  = price * slip1p
                                    hqty = h_notional / hep
                                    long_notional += hep * hqty
                                    long_qty      += hqty
                                    capital -= hedge_fee
                else:
                    if dca_level < 7:
                        drop = (first_entry_price - price) / first_entry_price
                        while dca_level < 7:
                            if drop < dca_drops[dca_level - 1]:
                                break
                            new_level = dca_level + 1
                            notional = notional_base * size_mults[new_level - 1]
                            fee = notional * fee_rate * slip1p
                            ep = price * slip1p
                            qty = notional / ep
                            long_notional += ep * qty
                            long_qty      += qty
                            capital -= fee
                            dca_level = new_level

                            if dynamic_hedge:
                                if dca_level >= hedge_entry:
                                    target = long_notional * hedge_ratio
                                    delta  = target - short_notional
                                    if delta > 1e-8:
                                        hep   = price * slip1m
                                        hqty  = delta / hep
                                        short_notional += hep * hqty
                                        short_qty      += hqty
                                        capital -= delta * fee_rate * slip1p
                            else:
                                if dca_level == hedge_entry:
                                    h_notional = long_notional * hedge_ratio
                                    if h_notional > 0:
                                        hedge_fee = h_notional * fee_rate * slip1p
                                        hep = price * slip1m
                                        hqty = h_notional / hep
                                        short_notional += hep * hqty
                                        short_qty      += hqty
                                        capital -= hedge_fee

                # ── Step 2a: 역추세 청산 조건 (divergence 모드) ───────────
                if entry_mode == "divergence" and long_qty > 0:
                    avg_long_p = long_notional / long_qty
                    closed_div = False
                    # all_close: 평단가 대비 all_close_pct 이상 상승
                    if (price - avg_long_p) / avg_long_p >= all_close_pct:
                        capital, rec = self._close_all_fast(
                            capital, long_notional, long_qty,
                            short_notional, short_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "TP"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0
                        closed_div = True
                    # safe_close: dca_level >= safe_close_n이고 safe_close_pct 이상
                    elif (not closed_div
                          and dca_level >= safe_close_n
                          and (price - avg_long_p) / avg_long_p >= safe_close_pct):
                        capital, rec = self._close_all_fast(
                            capital, long_notional, long_qty,
                            short_notional, short_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "TP"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0
                        closed_div = True
                    # loss_close: dca_level >= loss_close_n이고 마지막 진입가 대비 loss_close_pct 하락
                    elif (not closed_div
                          and sl_active
                          and dca_level >= loss_close_n
                          and last_entry_price > 0
                          and (last_entry_price - price) / last_entry_price >= loss_close_pct):
                        capital, rec = self._close_all_fast(
                            capital, long_notional, long_qty,
                            short_notional, short_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "SL"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0

                # ── Step 2a': 숏 역추세 청산 조건 (short_divergence 모드) ──
                elif entry_mode == "short_divergence" and short_qty > 0:
                    avg_short_p = short_notional / short_qty
                    closed_div = False
                    # all_close: 평단가 대비 all_close_pct 이상 하락 (숏 이익)
                    if (avg_short_p - price) / avg_short_p >= all_close_pct:
                        capital, rec = self._close_short_primary(
                            capital, short_notional, short_qty,
                            long_notional, long_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "TP"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0
                        closed_div = True
                    # safe_close
                    elif (not closed_div
                          and dca_level >= safe_close_n
                          and (avg_short_p - price) / avg_short_p >= safe_close_pct):
                        capital, rec = self._close_short_primary(
                            capital, short_notional, short_qty,
                            long_notional, long_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "TP"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0
                        closed_div = True
                    # loss_close: 마지막 진입가 대비 상승 시 손절
                    elif (not closed_div
                          and sl_active
                          and dca_level >= loss_close_n
                          and last_entry_price > 0
                          and (price - last_entry_price) / last_entry_price >= loss_close_pct):
                        capital, rec = self._close_short_primary(
                            capital, short_notional, short_qty,
                            long_notional, long_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "SL"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0

                # ── Step 2: 부분 TP (tp1_pct > 0 이면서 미발동 상태) ─────────
                if tp1_pct > 0 and not tp1_done and long_qty > 0:
                    avg_long_p = long_notional / long_qty
                    if (price - avg_long_p) / avg_long_p >= tp1_pct:
                        # tp1_size 비율만큼 롱 포지션 부분 청산
                        ep_sell   = price * slip1m
                        cl_qty    = long_qty       * tp1_size
                        cl_notl   = long_notional  * tp1_size
                        long_pnl  = (ep_sell - avg_long_p) * cl_qty
                        cl_fee    = cl_notl * fee_rate * slip1p
                        capital  += long_pnl - cl_fee
                        long_qty      -= cl_qty
                        long_notional -= cl_notl
                        # 숏도 같은 비율로 부분 청산
                        if short_qty > 0:
                            avg_sh = short_notional / short_qty
                            cl_sq  = short_qty      * tp1_size
                            cl_sn  = short_notional * tp1_size
                            sh_pnl = (avg_sh - price * slip1p) * cl_sq
                            capital += sh_pnl - cl_sn * fee_rate * slip1p
                            short_qty      -= cl_sq
                            short_notional -= cl_sn
                        tp1_done = True   # 이 포지션에서 한 번만

                # ── Step 3: TP / SL (rsi_bb 모드) ────────────────────────
                if entry_mode in ("divergence", "short_divergence"):
                    pass  # 다이버전스 모드는 Step 2a에서 처리
                elif long_qty > 0:
                    avg_long_p = long_notional / long_qty
                    price_move = (price - avg_long_p) / avg_long_p

                    if price_move >= tp_pct:
                        capital, rec = self._close_all_fast(
                            capital, long_notional, long_qty,
                            short_notional, short_qty,
                            price, fee_rate, slippage, dca_level,
                            open_time, timestamps[i], "TP"
                        )
                        trades.append(rec)
                        long_notional = long_qty = 0.0
                        short_notional = short_qty = 0.0
                        tp1_done = False
                        dca_level = 0

                    elif sl_active:
                        # tp1 발동 후에는 손절선을 원가(break-even)로 상향
                        sl_threshold = 0.0 if tp1_done else -sl_pct
                        if price_move <= sl_threshold:
                            capital, rec = self._close_all_fast(
                                capital, long_notional, long_qty,
                                short_notional, short_qty,
                                price, fee_rate, slippage, dca_level,
                                open_time, timestamps[i], "SL"
                            )
                            trades.append(rec)
                            long_notional = long_qty = 0.0
                            short_notional = short_qty = 0.0
                            tp1_done = False
                            dca_level = 0

        # Close open positions at end
        if dca_level > 0:
            price = closes[-1]
            if entry_mode == "short_divergence":
                capital, rec = self._close_short_primary(
                    capital, short_notional, short_qty,
                    long_notional, long_qty,
                    price, fee_rate, slippage, dca_level,
                    open_time or timestamps[-1], timestamps[-1], "EOD"
                )
            else:
                capital, rec = self._close_all_fast(
                    capital, long_notional, long_qty,
                    short_notional, short_qty,
                    price, fee_rate, slippage, dca_level,
                    open_time or timestamps[-1], timestamps[-1], "EOD"
                )
            trades.append(rec)
        if ind_s_qty > 0.0:
            price = closes[-1]
            avg_ind_s = ind_s_notional / ind_s_qty
            ep = price * (1.0 + slippage)
            pnl = (avg_ind_s - ep) * ind_s_qty
            fee = ind_s_notional * fee_rate * (1.0 + slippage)
            capital += pnl - fee
            trades.append(TradeRecord(
                open_time=ind_s_open_t, close_time=timestamps[-1],
                dca_levels_used=1,
                avg_long_price=0.0, avg_short_price=avg_ind_s,
                close_price=price,
                long_notional=0.0, short_notional=ind_s_notional,
                pnl_usd=pnl - fee, pnl_pct_price=(avg_ind_s - price) / avg_ind_s,
                exit_reason="EOD_S",
            ))

        return pd.Series(equity, index=df.index), trades

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _close_all_fast(
        capital: float,
        long_notional: float, long_qty: float,
        short_notional: float, short_qty: float,
        price: float, fee_rate: float, slippage: float,
        dca_level: int, open_time, close_time, reason: str,
    ) -> tuple[float, "TradeRecord"]:
        ep_long  = price * (1.0 - slippage)
        ep_short = price * (1.0 + slippage)

        avg_long_p  = long_notional  / long_qty  if long_qty  > 0 else 0.0
        avg_short_p = short_notional / short_qty if short_qty > 0 else 0.0

        long_pnl  = (ep_long  - avg_long_p)  * long_qty
        short_pnl = (avg_short_p - ep_short) * short_qty
        total_exit_notional = long_notional + short_notional
        exit_fee = total_exit_notional * fee_rate * (1.0 + slippage)

        new_capital = capital + long_pnl + short_pnl - exit_fee
        price_ret = (price - avg_long_p) / avg_long_p if avg_long_p > 0 else 0.0

        record = TradeRecord(
            open_time=open_time,
            close_time=close_time,
            dca_levels_used=dca_level,
            avg_long_price=avg_long_p,
            avg_short_price=avg_short_p,
            close_price=price,
            long_notional=long_notional,
            short_notional=short_notional,
            pnl_usd=long_pnl + short_pnl - exit_fee,
            pnl_pct_price=price_ret,
            exit_reason=reason,
        )
        return new_capital, record

    @staticmethod
    def _close_short_primary(
        capital: float,
        short_notional: float, short_qty: float,    # primary (short)
        long_notional: float,  long_qty: float,     # hedge (long)
        price: float, fee_rate: float, slippage: float,
        dca_level: int, open_time, close_time, reason: str,
    ) -> tuple[float, "TradeRecord"]:
        """숏 primary + 롱 헤지 동시 청산. short_divergence 모드 전용."""
        ep_long  = price * (1.0 - slippage)   # 롱 청산은 낮게 체결
        ep_short = price * (1.0 + slippage)   # 숏 커버는 높게 체결

        avg_long_p  = long_notional  / long_qty  if long_qty  > 0 else 0.0
        avg_short_p = short_notional / short_qty if short_qty > 0 else 0.0

        short_pnl = (avg_short_p - ep_short) * short_qty  # 숏 primary PnL
        long_pnl  = (ep_long  - avg_long_p)  * long_qty   # 롱 헤지 PnL
        total_exit_notional = long_notional + short_notional
        exit_fee = total_exit_notional * fee_rate * (1.0 + slippage)

        new_capital = capital + long_pnl + short_pnl - exit_fee
        # 숏 primary 기준 수익률
        price_ret = (avg_short_p - price) / avg_short_p if avg_short_p > 0 else 0.0

        record = TradeRecord(
            open_time=open_time,
            close_time=close_time,
            dca_levels_used=dca_level,
            avg_long_price=avg_long_p,
            avg_short_price=avg_short_p,
            close_price=price,
            long_notional=long_notional,
            short_notional=short_notional,
            pnl_usd=long_pnl + short_pnl - exit_fee,
            pnl_pct_price=price_ret,
            exit_reason=reason,
        )
        return new_capital, record

    def _entry_signal(self, row) -> bool:
        p = self.p
        try:
            return (
                int(row["regime"]) != 3
                and float(row["rsi"]) < p.rsi_thresh
                and float(row["close"]) <= float(row["bb_lower"])
                and float(row["macd_hist"]) < 0.0
                and float(row["vol_ratio"]) >= p.vol_thresh
            )
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _close_all(
        capital: float,
        long_pos: _LegPosition,
        short_pos: _LegPosition,
        price: float,
        fee_rate: float,
        slippage: float,
        dca_level: int,
        open_time: pd.Timestamp,
        close_time: pd.Timestamp,
        reason: str,
    ) -> tuple[float, TradeRecord]:
        exec_price_long = price * (1 - slippage)
        exec_price_short = price * (1 + slippage)

        long_pnl = long_pos.unrealized_long_pnl(exec_price_long)
        short_pnl = short_pos.unrealized_short_pnl(exec_price_short)
        total_notional = long_pos.total_notional + short_pos.total_notional
        exit_fee = total_notional * fee_rate * (1 + slippage)

        new_capital = capital + long_pnl + short_pnl - exit_fee

        avg_lp = long_pos.avg_price
        price_ret = (price - avg_lp) / avg_lp if avg_lp > 0 else 0.0

        record = TradeRecord(
            open_time=open_time,
            close_time=close_time,
            dca_levels_used=dca_level,
            avg_long_price=avg_lp,
            avg_short_price=short_pos.avg_price,
            close_price=price,
            long_notional=long_pos.total_notional,
            short_notional=short_pos.total_notional,
            pnl_usd=long_pnl + short_pnl - exit_fee,
            pnl_pct_price=price_ret,
            exit_reason=reason,
        )
        return new_capital, record
