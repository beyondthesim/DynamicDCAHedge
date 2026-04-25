"""포지션 상태 영속화. 재시작 후에도 DCA 레벨/평단가 복원."""
from __future__ import annotations
import json
import logging
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class StrategyState:
    """단일 전략 상태."""
    name: str
    symbol: str
    init_cash: float
    # primary direction: "long" | "short" | "flat"
    primary_side: str = "flat"
    primary_notional: float = 0.0
    primary_qty: float = 0.0
    primary_avg_price: float = 0.0
    # hedge (반대방향) 상태
    hedge_notional: float = 0.0
    hedge_qty: float = 0.0
    hedge_avg_price: float = 0.0
    # DCA 메타
    dca_level: int = 0
    first_entry_price: float = 0.0
    last_entry_price: float = 0.0
    open_time: Optional[str] = None    # ISO timestamp
    # 추세 매매용
    peak_price: float = 0.0
    last_cross_bar: int = -10**9
    last_exit_bar: int = -10**9
    # 자본 추적 (수수료 누적)
    capital: float = 0.0
    # 거래 카운트
    trade_count: int = 0
    realized_pnl: float = 0.0


def load_state(path: str | Path, default: StrategyState) -> StrategyState:
    p = Path(path)
    if not p.exists():
        return default
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        merged = asdict(default)
        merged.update(data)
        return StrategyState(**merged)
    except Exception as e:
        logger.warning("상태 로드 실패: %s — 기본값 사용", e)
        return default


def save_state(path: str | Path, state: StrategyState):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(asdict(state), f, indent=2, ensure_ascii=False)
    tmp.replace(p)
