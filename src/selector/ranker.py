"""코인 점수 매기기. Top-N 선정용.

Composite ranker = 변동성 + 거래량 + 절대 변동률.
신규 보너스 (less days_listed = higher rank).
"""
from __future__ import annotations
import logging
from .universe import CoinCandidate

logger = logging.getLogger(__name__)


class CompositeRanker:
    """변동성/거래량/일일 변동/신규 보너스 z-score 합산."""
    def __init__(
        self,
        weight_volatility: float = 1.0,   # |24h change|
        weight_volume: float = 0.5,
        weight_newness: float = 0.5,      # 짧을수록 ↑
    ):
        self.w_vol = weight_volatility
        self.w_volu = weight_volume
        self.w_new = weight_newness

    def _zscore(self, vals: list[float]) -> list[float]:
        if not vals: return []
        n = len(vals)
        mu = sum(vals) / n
        var = sum((v - mu) ** 2 for v in vals) / max(n - 1, 1)
        sigma = var ** 0.5 if var > 0 else 1.0
        return [(v - mu) / sigma for v in vals]

    def rank(self, cands: list[CoinCandidate], top_n: int = 10) -> list[CoinCandidate]:
        if not cands: return []
        vol_change = [abs(c.price_change_24h_pct) for c in cands]
        volume     = [c.quote_volume_24h_usd for c in cands]
        # 신규성: 1/days_listed → 짧을수록 큰 값
        newness    = [1.0 / max(c.days_listed, 1.0) for c in cands]

        z_vol  = self._zscore(vol_change)
        z_volu = self._zscore(volume)
        z_new  = self._zscore(newness)

        scored = []
        for i, c in enumerate(cands):
            score = (self.w_vol * z_vol[i]
                     + self.w_volu * z_volu[i]
                     + self.w_new * z_new[i])
            scored.append((score, c))
        scored.sort(key=lambda x: -x[0])
        ranked = [c for _, c in scored[:top_n]]
        logger.info("Top %d picks: %s", top_n, [c.base for c in ranked])
        return ranked
