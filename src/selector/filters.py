"""품질 필터 (양방향 지원).

각 후보 코인에 대해:
  - ``params_short``로 짧은 백테스트
  - (옵션) ``params_long``으로도 백테스트
  - 거래 수·MDD 기준 통과한 결과 중 **higher Calmar (= ret/|mdd|)** 방향 선택
  - 통과 코인의 ``best_direction``과 백테스트 메트릭을 attach해서 반환

위험 코인 자동 제외 패턴:
  - 거래량 0건 (신호 안 잡힘)
  - MDD 임계 초과 (시뮬상 폭파)
  - 데이터 부족
"""
from __future__ import annotations

import dataclasses
import logging
import warnings
from typing import Optional

import pandas as pd

from .universe import CoinCandidate

logger = logging.getLogger(__name__)


def _calmar(ret_pct: float, mdd_pct: float) -> float:
    """Calmar 근사 — 동률·0 처리 포함."""
    denom = abs(mdd_pct) if abs(mdd_pct) > 1e-6 else 1.0
    return ret_pct / denom


class QualityFilter:
    """양방향 짧은 백테스트로 코인 품질·방향 동시 선택.

    Args:
        min_trades:    백테스트 기간 동안 최소 거래 수
        max_mdd_pct:   허용 가능한 최대 드로다운 (절대값 %)
        backtest_days: 검증용 lookback 일수
        dual_direction: True면 long/short 둘 다 시도해서 더 좋은 쪽 선택
    """

    def __init__(
        self,
        min_trades: int = 8,
        max_mdd_pct: float = 60.0,
        backtest_days: int = 45,
        dual_direction: bool = True,
    ):
        self.min_trades = min_trades
        self.max_mdd_pct = max_mdd_pct
        self.backtest_days = backtest_days
        self.dual_direction = dual_direction

    # ── 단일 방향 백테스트 ──────────────────────────────────────────
    def _run_single(
        self,
        candidate: CoinCandidate,
        strategy_params,
        backtest_cfg,
        regime_cfg,
        btc_trend: pd.Series,
        fetch_ohlcv_fn,
        prepare_fn,
        run_prepared_fn,
    ) -> Optional[dict]:
        """반환: ``{"trades", "ret", "mdd", "calmar"}`` 또는 None (실패)."""
        try:
            lb = max(min(int(candidate.days_listed) - 1, self.backtest_days), 5)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df_raw = fetch_ohlcv_fn(candidate.symbol, "1m", lb)
            if len(df_raw) < 1000:
                return None
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df_feat = prepare_fn(df_raw, strategy_params, regime_cfg, btc_trend)
            bc = dataclasses.replace(backtest_cfg, init_cash=1000.0)
            result = run_prepared_fn(df_feat, strategy_params, bc)
        except Exception as e:
            logger.warning("[FILTER] %s: 백테스트 실패 %s", candidate.base, e)
            return None

        return {
            "trades": int(result.total_trades),
            "ret": float(result.total_return_pct),
            "mdd": float(result.max_drawdown_pct),
            "calmar": _calmar(float(result.total_return_pct), float(result.max_drawdown_pct)),
        }

    def _passes(self, m: Optional[dict]) -> bool:
        if m is None:
            return False
        if m["trades"] < self.min_trades:
            return False
        if abs(m["mdd"]) > self.max_mdd_pct:
            return False
        return True

    # ── 양방향 필터 ─────────────────────────────────────────────────
    def filter(
        self,
        candidates: list[CoinCandidate],
        strategy_params,
        backtest_cfg,
        regime_cfg,
        btc_trend: pd.Series,
        fetch_ohlcv_fn,
        prepare_fn,
        run_prepared_fn,
        params_long=None,
    ) -> list[CoinCandidate]:
        """후보 코인에 1방향 또는 양방향 백테스트 → 통과한 코인만 반환.

        ``params_long``이 주어지면 ``strategy_params``는 short 측, ``params_long``은 long 측으로 사용.
        통과 코인엔 ``best_direction`` 속성과 ``best_metrics`` 딕셔너리가 attach 된다.
        """
        passed: list[CoinCandidate] = []

        for c in candidates:
            short_m = self._run_single(
                c, strategy_params, backtest_cfg, regime_cfg, btc_trend,
                fetch_ohlcv_fn, prepare_fn, run_prepared_fn,
            )
            long_m = None
            if self.dual_direction and params_long is not None:
                long_m = self._run_single(
                    c, params_long, backtest_cfg, regime_cfg, btc_trend,
                    fetch_ohlcv_fn, prepare_fn, run_prepared_fn,
                )

            short_ok = self._passes(short_m)
            long_ok = self._passes(long_m)

            if not short_ok and not long_ok:
                if short_m is not None:
                    logger.info(
                        "[FILTER] %s: 양방향 미통과 (s.trades=%d ret=%+.1f%% mdd=%.1f%% / l=%s)",
                        c.base, short_m["trades"], short_m["ret"], short_m["mdd"],
                        "n/a" if long_m is None else f"trades={long_m['trades']} mdd={long_m['mdd']:.1f}%",
                    )
                continue

            # 둘 다 통과면 calmar 큰 쪽
            if short_ok and long_ok:
                if long_m["calmar"] > short_m["calmar"]:
                    chosen, direction = long_m, "long"
                else:
                    chosen, direction = short_m, "short"
            elif short_ok:
                chosen, direction = short_m, "short"
            else:
                chosen, direction = long_m, "long"

            # CoinCandidate는 frozen이 아닌 dataclass니까 setattr 가능
            c.best_direction = direction  # type: ignore[attr-defined]
            c.best_metrics = chosen        # type: ignore[attr-defined]
            logger.info(
                "[FILTER PASS] %s [%s]: trades=%d ret=%+.1f%% mdd=%.1f%% calmar=%.2f",
                c.base, direction, chosen["trades"], chosen["ret"], chosen["mdd"],
                chosen["calmar"],
            )
            passed.append(c)

        return passed
