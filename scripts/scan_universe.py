"""유니버스 스캔 단독 실행. 오늘의 후보 코인 출력 (실거래 X)."""
from __future__ import annotations
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

from src.selector import UniverseScanner, CompositeRanker

scanner = UniverseScanner(
    new_listing_max_days=120,
    new_listing_min_days=14,
    top_gainer_top_k=20,
    min_volume_usd=1_000_000,
)
print("\n=== 유니버스 스캔 ===")
cands = scanner.scan()
print(f"\n총 {len(cands)} 후보")

print("\n=== Top-15 (composite ranker) ===")
ranked = CompositeRanker().rank(cands, top_n=15)
print(f"{'코인':<14} {'상장(일)':>9} {'24h $M':>10} {'24h%':>8} {'소스':<12}")
print("-" * 60)
for c in ranked:
    print(f"{c.base:<14} {c.days_listed:>9.1f} {c.quote_volume_24h_usd/1e6:>10.1f} {c.price_change_24h_pct:>+8.2f}  {c.source:<12}")
