"""백테스트 동작과 1:1 동일한 미니멀 리스크 매니저.

백테스트(``dca_hedge.py``)의 유일한 자본 보호는
``min_capital = init_cash * 0.05`` — 95% 손실 시 거래 중단.

이 매니저도 동일하게 동작:
  - 진입/DCA/사이즈 게이팅 없음 (size_mult=1.0, allow_new=True, allow_dca=True)
  - 변동성 타겟·상관 락 없음
  - 자본이 시드의 5% 미만으로 떨어지면 ``catastrophic`` 단일 한도로 비상 청산
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskLimits:
    """비상 청산 단일 임계. 기본값은 백테스트 ``min_capital * 0.05`` 보호와 동등."""
    catastrophic_loss_pct: float = 95.0   # 시드 대비 −95% (=잔고 5%) 도달 시 전체 청산


@dataclass
class RiskState:
    """executor에 전달되는 결정 스냅샷. 백테스트 동작과 동일하게 항상 통과."""
    size_mult: float = 1.0
    hedge_ratio_override: float | None = None
    daily_pnl_pct: float = 0.0
    drawdown_pct: float = 0.0
    notional_pct: float = 0.0
    breached: bool = False
    reason: str = ""

    # CoinExecutor가 호출하는 인터페이스 — 백테스트와 동일하게 항상 허용
    def block_new_entry(self) -> bool:
        return False

    def block_dca(self) -> bool:
        return False


class RiskManager:
    """백테스트와 동일하게 동작 + 시드 −95% 도달 시만 비상 청산."""

    def __init__(self, total_seed: float, limits: RiskLimits | None = None):
        self.total_seed = float(total_seed)
        self.limits = limits or RiskLimits()
        self.peak_capital = self.total_seed
        self.state = RiskState()

    @staticmethod
    def total_capital(executors) -> float:
        return sum(e.state.capital for e in executors)

    @staticmethod
    def total_notional(executors) -> float:
        return sum(e.state.primary_notional + e.state.hedge_notional for e in executors)

    def update(self, executors) -> RiskState:
        executors = list(executors)
        current = self.total_capital(executors)
        if current > self.peak_capital:
            self.peak_capital = current
        dd = (current / self.peak_capital - 1.0) * 100.0 if self.peak_capital > 0 else 0.0
        notl_pct = self.total_notional(executors) / self.total_seed * 100.0 if self.total_seed > 0 else 0.0
        seed_loss_pct = (1.0 - current / self.total_seed) * 100.0 if self.total_seed > 0 else 0.0

        breached = False
        reason = ""
        if seed_loss_pct >= self.limits.catastrophic_loss_pct:
            breached = True
            reason = f"catastrophic seed loss {seed_loss_pct:.1f}%"
            logger.error("RISK breached: %s", reason)

        self.state = RiskState(
            size_mult=1.0,
            hedge_ratio_override=None,
            daily_pnl_pct=0.0,
            drawdown_pct=dd,
            notional_pct=notl_pct,
            breached=breached,
            reason=reason,
        )
        return self.state

    def is_breached(self) -> tuple[bool, str]:
        return (self.state.breached, self.state.reason)

    @staticmethod
    def emergency_close_all(executors) -> None:
        logger.warning("=== EMERGENCY CLOSE ALL ===")
        for e in executors:
            try:
                e.force_close()
            except Exception as exc:
                logger.error("force_close failed %s: %s", getattr(e, "coin", "?"), exc)
