"""OKX 활성 포지션 + 풀 코인 레버리지 일괄 정규화 (default 20x).

Usage:
    python scripts/normalize_leverage.py                # 활성 포지션만 20x
    python scripts/normalize_leverage.py --target 10    # 다른 타겟
    python scripts/normalize_leverage.py --dry-run      # 시뮬

OKX max < target인 코인은 max로 fallback (CoinExecutor가 base_margin 자동 보정).
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.live.okx_trader import OKXTrader


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=int, default=20, help="목표 레버리지 (default 20)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sandbox", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    trader = OKXTrader(dry_run=args.dry_run, sandbox=args.sandbox)
    target = int(args.target)

    if args.dry_run:
        logger.info("[DRY-RUN] 실제 변경 없음")

    positions = trader.fetch_all_positions()
    if not positions:
        logger.info("활성 포지션 없음 — 정규화 스킵")
        return

    print(f"\n=== 활성 포지션 {len(positions)}개 → {target}x 강제 ===")
    print(f"{'symbol':<24} {'side':<6} {'contracts':>10} {'notional($)':>12} {'lev':>6} {'-> new':>7}")
    print("-" * 70)
    for p in positions:
        actual = trader.set_leverage_safe(p.symbol, target=target)
        print(f"{p.symbol:<24} {p.side:<6} {p.contracts:>10.2f} {p.notional:>12.2f} "
              f"{int(p.leverage):>5}x {actual:>5}x")

    print(f"\n완료. base_margin 보정은 DynamicRunner 재시작 시 자동 적용됩니다.")


if __name__ == "__main__":
    main()
