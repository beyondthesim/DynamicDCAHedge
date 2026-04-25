"""
Backtest engine: glues data preparation, strategy simulation, and metrics.
"""
from __future__ import annotations

import logging
import pandas as pd

from ..indicators.compute import prepare
from ..strategy.dca_hedge import DCAHedgeSimulator
from ..backtest.metrics import compute, BacktestResult

logger = logging.getLogger(__name__)


def run(
    df: pd.DataFrame,
    strategy_params,
    backtest_cfg,
    regime_cfg,
) -> BacktestResult:
    logger.info("Preparing indicators on %d bars …", len(df))
    df_feat = prepare(df, strategy_params, regime_cfg)
    logger.info("Bars after indicator warmup: %d", len(df_feat))

    sim = DCAHedgeSimulator(strategy_params, backtest_cfg)
    equity, trades = sim.run(df_feat)

    result = compute(equity, trades, backtest_cfg.init_cash)
    logger.info(result.summary())
    return result


def run_no_hedge(
    df: pd.DataFrame,
    strategy_params,
    backtest_cfg,
    regime_cfg,
) -> BacktestResult:
    """Same strategy but hedge_ratio=0 for comparison."""
    import dataclasses
    p_no_hedge = dataclasses.replace(strategy_params, hedge_ratio=0.0)
    return run(df, p_no_hedge, backtest_cfg, regime_cfg)


def run_prepared(
    df_feat: pd.DataFrame,
    strategy_params,
    backtest_cfg,
) -> BacktestResult:
    """Run simulation on already-prepared (indicator-computed) DataFrame.

    Routes to TrendFollowSimulator for trend_long/trend_short,
    DCAHedgeSimulator otherwise.
    """
    mode = getattr(strategy_params, "entry_mode", "rsi_bb")
    if mode in ("trend_long", "trend_short"):
        from ..strategy.trend_follow import TrendFollowSimulator
        sim = TrendFollowSimulator(strategy_params, backtest_cfg)
    else:
        sim = DCAHedgeSimulator(strategy_params, backtest_cfg)
    equity, trades = sim.run(df_feat)
    return compute(equity, trades, backtest_cfg.init_cash)
