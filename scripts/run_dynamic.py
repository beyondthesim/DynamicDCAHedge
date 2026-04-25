"""DynamicDCAHedge 진입점 — 동적 코인 회전 실거래.

Usage:
    python scripts/run_dynamic.py             # 실거래 (default)
    python scripts/run_dynamic.py --dry-run   # 시뮬 (주문 X)
    python scripts/run_dynamic.py --sandbox   # OKX demo 환경
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.live.dynamic_runner import DynamicRunner


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="results/PRODUCTION_config.json")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="시뮬레이션 모드 (주문 안 나감). 미지정 시 실거래.",
    )
    parser.add_argument(
        "--sandbox", action="store_true",
        help="OKX demo 환경",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    Path("runtime").mkdir(exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("runtime/dynamic.log", encoding="utf-8"),
        ],
    )

    is_live = not args.dry_run
    if is_live:
        print("\n" + "=" * 60)
        print("  DynamicDCAHedge - 실거래 (LIVE)")
        print("  매일 코인 자동 갱신 + 양방향 역추세 + 30% 헷지")
        print("=" * 60)
    else:
        print("\n[DRY-RUN] 시뮬레이션. 실주문 안 나감.")

    runner = DynamicRunner(args.config, dry_run=not is_live, sandbox=args.sandbox)
    runner.run()


if __name__ == "__main__":
    main()
