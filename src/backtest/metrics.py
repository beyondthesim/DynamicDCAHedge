"""Performance metrics computed from an equity curve and trade list."""
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class BacktestResult:
    total_return_pct: float
    annual_return_pct: float
    max_drawdown_pct: float
    calmar_ratio: float
    sharpe_ratio: float
    sortino_ratio: float
    win_rate: float
    profit_factor: float
    total_trades: int
    avg_dca_levels: float
    hedge_used_pct: float
    tp_count: int
    sl_count: int
    eod_count: int
    equity: pd.Series
    trades: list

    def summary(self) -> str:
        return (
            f"Ret: {self.total_return_pct:+.1f}% | "
            f"Ann: {self.annual_return_pct:+.1f}% | "
            f"MDD: {self.max_drawdown_pct:.1f}% | "
            f"Calmar: {self.calmar_ratio:.2f} | "
            f"Sharpe: {self.sharpe_ratio:.2f} | "
            f"WR: {self.win_rate:.1%} | "
            f"PF: {self.profit_factor:.2f} | "
            f"N: {self.total_trades} | "
            f"AvgDCA: {self.avg_dca_levels:.1f} | "
            f"Hedge%: {self.hedge_used_pct:.1%} | "
            f"TP/SL/EOD: {self.tp_count}/{self.sl_count}/{self.eod_count}"
        )


def compute(equity: pd.Series, trades: list, init_cash: float) -> BacktestResult:
    eq = equity.values.astype(float)

    total_return = (eq[-1] - eq[0]) / eq[0] * 100
    n_days = max((equity.index[-1] - equity.index[0]).total_seconds() / 86400, 1)
    annual_return = ((eq[-1] / eq[0]) ** (365 / n_days) - 1) * 100

    roll_max = np.maximum.accumulate(eq)
    drawdown = (eq - roll_max) / roll_max * 100
    max_dd = float(np.min(drawdown))

    calmar = annual_return / abs(max_dd) if max_dd != 0 else 0.0

    # Sharpe on 1-min returns (~525,600 periods/year)
    ret = np.diff(eq) / np.maximum(eq[:-1], 1e-10)
    ann_factor = np.sqrt(525_600)
    mu = float(np.mean(ret))
    sigma = float(np.std(ret))
    sharpe = mu / (sigma + 1e-12) * ann_factor

    downside = ret[ret < 0]
    sortino = mu / (float(np.std(downside)) + 1e-12) * ann_factor

    if trades:
        pnls = [t.pnl_usd for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        win_rate = len(wins) / len(pnls)
        profit_factor = sum(wins) / abs(sum(losses)) if losses else float("inf")
        avg_dca = float(np.mean([t.dca_levels_used for t in trades]))
        hedge_used = sum(1 for t in trades if t.short_notional > 0) / len(trades)
        tp_count = sum(1 for t in trades if t.exit_reason == "TP")
        sl_count = sum(1 for t in trades if t.exit_reason == "SL")
        eod_count = sum(1 for t in trades if t.exit_reason == "EOD")
    else:
        win_rate = profit_factor = avg_dca = hedge_used = 0.0
        tp_count = sl_count = eod_count = 0

    return BacktestResult(
        total_return_pct=total_return,
        annual_return_pct=annual_return,
        max_drawdown_pct=max_dd,
        calmar_ratio=calmar,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        win_rate=win_rate,
        profit_factor=profit_factor,
        total_trades=len(trades),
        avg_dca_levels=avg_dca,
        hedge_used_pct=hedge_used,
        tp_count=tp_count,
        sl_count=sl_count,
        eod_count=eod_count,
        equity=equity,
        trades=trades,
    )
