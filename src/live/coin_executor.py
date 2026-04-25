"""단일 코인 역추세 + 헷지 실행기 (양방향 지원).

방향(direction)
  - "short": 숏 역추세 + 롱 헷지 (연속 양봉 + EMA 위 돌파에서 진입)
  - "long":  롱 역추세 + 숏 헷지 (연속 음봉 + EMA 아래 이탈에서 진입)

RiskManager가 산출한 ``RiskState``를 매 step 입력으로 받아
1차 신규 진입 / DCA / 사이즈 / 헷지비율을 동적으로 게이팅한다.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from .data_feed import DataFeed
from .okx_trader import OKXTrader
from .state import StrategyState, load_state, save_state

logger = logging.getLogger(__name__)


class CoinExecutor:
    """단일 코인 역추세 실행기.

    Args:
        symbol:        OKX 페어 (예: ``"TRIA/USDT:USDT"``)
        trader:        OKXTrader 인스턴스
        params:        StrategyParams (entry_mode 등 포함)
        init_cash:     이 코인에 배정된 초기 자본
        state_dir:     상태 저장 디렉터리
        direction:     ``"short"`` (default) 또는 ``"long"``
    """

    def __init__(
        self,
        symbol: str,
        trader: OKXTrader,
        params,
        init_cash: float,
        state_dir: Path,
        direction: str = "short",
    ):
        if direction not in ("short", "long"):
            raise ValueError(f"direction must be 'short' or 'long', got {direction!r}")
        self.symbol = symbol
        self.coin = symbol.split("/")[0]
        self.trader = trader
        self.p = params
        self.direction = direction
        self.feed = DataFeed(trader, symbol)
        self.state_path = state_dir / f"state_{self.coin}.json"
        self.state = load_state(self.state_path, StrategyState(
            name=f"{direction}_div_{self.coin}",
            symbol=symbol,
            init_cash=init_cash,
            capital=init_cash,
        ))
        # RiskManager가 읽어가는 라이브 메트릭
        self.last_atr_pct: float = 0.0
        self.last_unrealized_pnl_pct: float = 0.0

    # ── 보조 ─────────────────────────────────────────────────────────────
    def _save(self) -> None:
        save_state(self.state_path, self.state)

    @property
    def is_open(self) -> bool:
        return self.state.dca_level > 0

    @property
    def primary_position_side(self) -> str:
        """OKX position_side 매핑."""
        return "short" if self.direction == "short" else "long"

    @property
    def hedge_position_side(self) -> str:
        return "long" if self.direction == "short" else "short"

    @property
    def primary_order_side(self) -> str:
        """1차/추가 진입 주문 side."""
        return "sell" if self.direction == "short" else "buy"

    @property
    def hedge_order_side(self) -> str:
        return "buy" if self.direction == "short" else "sell"

    # ── 메인 step ────────────────────────────────────────────────────────
    def step(self, risk_state=None) -> None:
        """1분봉 마감 시 1회 호출.

        ``risk_state``는 ``catastrophic_loss_pct`` 도달 여부 판정용으로만 의미를 가지며,
        진입/DCA/사이즈는 백테스트와 동일하게 항상 통과한다.
        """
        try:
            df = self.feed.fetch_prepared(self.p)
        except Exception as e:
            logger.error("[%s] data fetch 실패: %s", self.coin, e)
            return
        if len(df) < 2:
            return

        last = df.iloc[-1]
        price = float(last["close"])

        # ATR% (로그/모니터링용)
        try:
            recent = df.iloc[-30:]
            tr = (recent["high"] - recent["low"]).abs()
            self.last_atr_pct = float(tr.mean() / price) if price > 0 else 0.0
        except Exception:
            self.last_atr_pct = 0.0

        # 진입 신호 컬럼
        green_cnt = int(last.get("green_candle_count", 0))
        red_cnt = int(last.get("red_candle_count", 0))
        ema_entry = float(last.get("ema_entry", price))
        bear_div = int(last.get("bear_div", 0))
        bull_div = int(last.get("bull_div", 0))
        regime = int(last.get("regime", 0))
        btc_trend = int(last.get("btc_trend", 0))

        p = self.p
        s = self.state

        # 사이즈 스케일 (vol target × tier guard) — risk_state가 없으면 1.0
        size_mult_global = 1.0
        hedge_ratio_eff = float(p.hedge_ratio)
        block_new = False
        block_dca = False
        if risk_state is not None:
            size_mult_global = float(getattr(risk_state, "size_mult", 1.0))
            ovr = getattr(risk_state, "hedge_ratio_override", None)
            if ovr is not None:
                hedge_ratio_eff = float(ovr)
            block_new = bool(risk_state.block_new_entry()) if hasattr(risk_state, "block_new_entry") else False
            block_dca = bool(risk_state.block_dca()) if hasattr(risk_state, "block_dca") else False

        notional_base = (
            s.init_cash * float(p.base_margin_pct) * float(p.leverage) * size_mult_global
        )
        size_mults = list(p.size_mults)
        max_dca = len(size_mults) - 1

        # ── 1차 진입 ────────────────────────────────────────────────
        if s.dca_level == 0:
            self.last_unrealized_pnl_pct = 0.0
            if block_new:
                return

            if self.direction == "short":
                entry_ok = (
                    green_cnt >= int(p.red_candle_n)
                    and price >= ema_entry
                    and btc_trend <= 0
                    and regime != 3
                )
            else:  # long 역추세
                entry_ok = (
                    red_cnt >= int(p.red_candle_n)
                    and price <= ema_entry
                    and btc_trend >= 0
                    and regime != 3
                )

            if entry_ok:
                notional = notional_base * size_mults[0]
                if notional <= 0:
                    return
                logger.info(
                    "[%s][%s] 1차 진입: price=%.6f notional=$%.2f size×%.2f",
                    self.coin, self.direction, price, notional, size_mult_global,
                )
                order = self.trader.market_order(
                    self.symbol, self.primary_order_side, notional,
                    position_side=self.primary_position_side,
                )
                s.primary_side = self.primary_position_side
                s.primary_notional = order.cost
                s.primary_qty = order.amount
                s.primary_avg_price = order.price
                s.dca_level = 1
                s.first_entry_price = price
                s.last_entry_price = price
                s.open_time = datetime.now(timezone.utc).isoformat()
                s.capital -= order.fee
                s.trade_count += 1
                self._save()
            return

        # ── 미실현 PnL% (상관 락 판단용) ────────────────────────────
        avg_primary = s.primary_avg_price or price
        if self.direction == "short":
            self.last_unrealized_pnl_pct = (avg_primary - price) / avg_primary
        else:
            self.last_unrealized_pnl_pct = (price - avg_primary) / avg_primary

        # ── DCA 추가 ────────────────────────────────────────────────
        if not block_dca and s.dca_level < max_dca:
            if self.direction == "short":
                dca_trigger = (
                    bear_div == 1
                    and price > s.last_entry_price * (1.0 + float(p.dca_next_drop))
                )
            else:
                dca_trigger = (
                    bull_div == 1
                    and price < s.last_entry_price * (1.0 - float(p.dca_next_drop))
                )

            if dca_trigger:
                new_level = s.dca_level + 1
                notional = notional_base * size_mults[new_level - 1]
                if notional > 0:
                    logger.info(
                        "[%s][%s] DCA L%d: price=%.6f notional=$%.2f",
                        self.coin, self.direction, new_level, price, notional,
                    )
                    order = self.trader.market_order(
                        self.symbol, self.primary_order_side, notional,
                        position_side=self.primary_position_side,
                    )
                    new_qty = s.primary_qty + order.amount
                    s.primary_avg_price = (
                        s.primary_avg_price * s.primary_qty
                        + order.price * order.amount
                    ) / new_qty
                    s.primary_qty = new_qty
                    s.primary_notional += order.cost
                    s.dca_level = new_level
                    s.last_entry_price = price
                    s.capital -= order.fee

                    # 헷지 진입/증액 (effective hedge_ratio 사용)
                    if s.dca_level >= int(p.hedge_entry) and hedge_ratio_eff > 0:
                        target_hedge = s.primary_notional * hedge_ratio_eff
                        delta = target_hedge - s.hedge_notional
                        if delta > 1e-8:
                            logger.info(
                                "[%s][%s] 헷지 %s 증액: +$%.2f → $%.2f (ratio=%.2f)",
                                self.coin, self.direction, self.hedge_position_side,
                                delta, target_hedge, hedge_ratio_eff,
                            )
                            h = self.trader.market_order(
                                self.symbol, self.hedge_order_side, delta,
                                position_side=self.hedge_position_side,
                            )
                            new_h_qty = s.hedge_qty + h.amount
                            s.hedge_avg_price = (
                                (s.hedge_avg_price * s.hedge_qty + h.price * h.amount)
                                / new_h_qty if new_h_qty > 0 else h.price
                            )
                            s.hedge_qty = new_h_qty
                            s.hedge_notional += h.cost
                            s.capital -= h.fee
                    self._save()

        # ── 청산 조건 ───────────────────────────────────────────────
        avg_p = s.primary_avg_price
        if avg_p == 0:
            return
        if self.direction == "short":
            tp_move = (avg_p - price) / avg_p
            sl_move = (price - s.last_entry_price) / s.last_entry_price if s.last_entry_price else 0.0
        else:
            tp_move = (price - avg_p) / avg_p
            sl_move = (s.last_entry_price - price) / s.last_entry_price if s.last_entry_price else 0.0

        all_close_pct = float(p.all_close_pct)
        safe_close_n = int(p.safe_close_n)
        safe_close_pct = float(p.safe_close_pct)
        loss_close_n = int(p.loss_close_n)
        loss_close_pct = float(p.loss_close_pct)

        reason = None
        if tp_move >= all_close_pct:
            reason = "TP_ALL"
        elif s.dca_level >= safe_close_n and tp_move >= safe_close_pct:
            reason = "TP_SAFE"
        elif s.dca_level >= loss_close_n and sl_move >= loss_close_pct:
            reason = "SL"

        if reason:
            logger.info(
                "[%s][%s] 청산 (%s): price=%.6f avg=%.6f L%d move=%+.3f%%",
                self.coin, self.direction, reason, price, avg_p, s.dca_level,
                tp_move * 100,
            )
            self.trader.close_position(self.symbol, position_side=self.primary_position_side)
            if s.hedge_qty > 0:
                self.trader.close_position(self.symbol, position_side=self.hedge_position_side)

            if self.direction == "short":
                primary_pnl = (avg_p - price) * s.primary_qty
                hedge_pnl = (price - s.hedge_avg_price) * s.hedge_qty if s.hedge_qty > 0 else 0.0
            else:
                primary_pnl = (price - avg_p) * s.primary_qty
                hedge_pnl = (s.hedge_avg_price - price) * s.hedge_qty if s.hedge_qty > 0 else 0.0
            fee = (s.primary_notional + s.hedge_notional) * 0.0005 * 2
            realized = primary_pnl + hedge_pnl - fee
            s.capital += realized
            s.realized_pnl += realized

            s.primary_side = "flat"
            s.primary_notional = s.primary_qty = s.primary_avg_price = 0
            s.hedge_notional = s.hedge_qty = s.hedge_avg_price = 0
            s.dca_level = 0
            s.first_entry_price = s.last_entry_price = 0
            s.open_time = None
            self.last_unrealized_pnl_pct = 0.0
            self._save()

    # ── 강제 청산 ───────────────────────────────────────────────────
    def force_close(self) -> None:
        if self.state.dca_level == 0:
            return
        s = self.state
        logger.warning("[%s][%s] 강제 청산", self.coin, self.direction)
        self.trader.close_position(self.symbol, position_side=self.primary_position_side)
        if s.hedge_qty > 0:
            self.trader.close_position(self.symbol, position_side=self.hedge_position_side)
        s.primary_side = "flat"
        s.dca_level = 0
        s.primary_notional = s.primary_qty = 0
        s.hedge_notional = s.hedge_qty = 0
        s.open_time = None
        self.last_unrealized_pnl_pct = 0.0
        self._save()
