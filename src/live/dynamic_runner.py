"""동적 다중 코인 러너 (v2).

매 ``refresh_hours`` 시간마다:
  1. 유니버스 스캔 (신규+탑게이너)
  2. 양방향(long/short) 품질 필터 → 코인별 best 방향 선택
  3. Composite 점수로 Top-K 선정
  4. 새 코인 추가 / 제외 코인 강제 청산
  5. 매 분: RiskManager.update() 후 모든 활성 코인 step(risk_state)

각 코인 자본 = SEED / N (균등 배분).
"""
from __future__ import annotations

import dataclasses
import json
import logging
import signal
import warnings
from datetime import datetime, timezone
from pathlib import Path

from ..backtest.engine import run_prepared
from ..data.fetcher import fetch_historical
from ..indicators.compute import compute_btc_trend, prepare
from ..selector import CompositeRanker, QualityFilter, UniverseScanner
from ..utils.config import load_config
from .coin_executor import CoinExecutor
from .data_feed import DataFeed
from .okx_trader import OKXTrader
from .risk_manager import RiskLimits, RiskManager

logger = logging.getLogger(__name__)


class DynamicRunner:
    """매일 코인 갱신 + 매 분 다중 코인 step."""

    def __init__(
        self,
        config_path: str,
        dry_run: bool = True,
        sandbox: bool = False,
    ):
        with open(config_path, encoding="utf-8") as f:
            self.config = json.load(f)
        self.cfg = load_config()  # config/default.yaml

        self.total_seed = float(self.config["total_seed_usd"])
        self.top_k = int(self.config.get("top_k_coins", 8))
        self.refresh_hours = int(self.config.get("refresh_hours", 12))
        self.dry_run = dry_run
        self.sandbox = sandbox
        self.dual_direction = bool(self.config.get("dual_direction", True))

        self.trader = OKXTrader(dry_run=dry_run, sandbox=sandbox)
        self.scanner = UniverseScanner(
            new_listing_max_days=int(self.config.get("new_listing_max_days", 180)),
            new_listing_min_days=int(self.config.get("new_listing_min_days", 7)),
            top_gainer_top_k=int(self.config.get("top_gainer_top_k", 40)),
            min_volume_usd=float(self.config.get("min_volume_usd", 500_000)),
        )
        self.quality = QualityFilter(
            min_trades=int(self.config.get("min_trades", 8)),
            max_mdd_pct=float(self.config.get("max_mdd_pct", 60.0)),
            backtest_days=int(self.config.get("filter_backtest_days", 45)),
            dual_direction=self.dual_direction,
        )
        self.ranker = CompositeRanker()

        # ── 미니멀 RiskManager: 백테스트와 1:1 동일, catastrophic 비상정지만 ──
        self.risk = RiskManager(
            self.total_seed,
            limits=RiskLimits(
                catastrophic_loss_pct=float(self.config.get("catastrophic_loss_pct", 95.0)),
            ),
        )

        # symbol → CoinExecutor (방향 정보는 executor.direction에 보존)
        self.executors: dict[str, CoinExecutor] = {}
        self.last_refresh: datetime | None = None
        self._stopping = False

        self.state_dir = Path(self.config.get("state_dir", "runtime"))
        self.state_dir.mkdir(parents=True, exist_ok=True)

        signal.signal(signal.SIGINT, self._on_sigint)
        signal.signal(signal.SIGTERM, self._on_sigint)

    def _on_sigint(self, signum, frame):
        logger.warning("종료 시그널 (%s)", signum)
        self._stopping = True

    # ── 전략 파라미터 빌드 ─────────────────────────────────
    def _build_params(self, direction: str = "short"):
        """양방향 공통 파라미터. ``entry_mode``는 quality filter 호출 시점에 변경."""
        sp = self.config["strategy_params"]
        entry_mode = "short_divergence" if direction == "short" else "divergence"
        return dataclasses.replace(
            self.cfg.strategy,
            entry_mode=entry_mode,
            size_mults=sp.get("size_mults", [1, 1, 1, 1, 2, 3, 5, 8, 13, 21]),
            leverage=float(sp["leverage"]),
            sl_active=True,
            tp_pct=0.99,
            tp1_pct=float(sp.get("tp1_pct", 0.0)),
            tp1_size=float(sp.get("tp1_size", 0.5)),
            btc_filter=True,
            enable_short=False,
            dynamic_hedge=False,
            hedge_entry=int(sp["hedge_entry"]),
            hedge_ratio=float(sp["hedge_ratio"]),
            red_candle_n=int(sp["red_candle_n"]),
            ema_entry_period=int(sp["ema_entry_period"]),
            dca_next_drop=float(sp["dca_next_drop"]),
            all_close_pct=float(sp["all_close_pct"]),
            safe_close_n=int(sp["safe_close_n"]),
            safe_close_pct=float(sp["safe_close_pct"]),
            loss_close_n=int(sp["loss_close_n"]),
            loss_close_pct=float(sp["loss_close_pct"]),
            base_margin_pct=float(sp["base_margin_pct"]),
            sl_pct=float(sp["sl_pct"]),
        )

    # ── 코인 풀 갱신 ─────────────────────────────────────
    def _refresh_universe(self) -> None:
        logger.info("=== 유니버스 갱신 시작 ===")
        params_short = self._build_params("short")
        params_long = self._build_params("long")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_btc = fetch_historical("BTC/USDT:USDT", "4h", 200)
            btc_trend = compute_btc_trend(
                df_btc, ema_period=200, slope_filter=True, slope_window=5,
            )

        cands = self.scanner.scan()
        logger.info("스캔 결과: %d 후보", len(cands))

        cands.sort(key=lambda x: -x.quote_volume_24h_usd)
        pre_n = int(self.config.get("pre_filter_top_n", 50))
        cands = cands[:pre_n]
        logger.info("거래량 상위 %d만 양방향 품질 필터", len(cands))

        passed = self.quality.filter(
            cands, params_short, self.cfg.backtest, self.cfg.regime,
            btc_trend, fetch_historical, prepare, run_prepared,
            params_long=params_long if self.dual_direction else None,
        )
        logger.info("품질 필터 통과: %d", len(passed))

        ranked = self.ranker.rank(passed, top_n=self.top_k)
        new_keys = {(c.symbol, getattr(c, "best_direction", "short")) for c in ranked}
        # executor key = symbol|direction (같은 코인 양방향 운용 가능)
        old_keys = {(sym, ex.direction) for sym, ex in self.executors.items()}

        for sym, direction in (old_keys - new_keys):
            ex = self.executors.pop(sym)
            ex.force_close()
            logger.info("코인 제외: %s (%s)", sym, direction)

        per_coin_cash = self.total_seed / max(len(new_keys), 1)
        for c in ranked:
            direction = getattr(c, "best_direction", "short")
            params = params_short if direction == "short" else params_long
            if c.symbol not in self.executors:
                logger.info(
                    "코인 추가: %s [%s] cash=$%.2f", c.symbol, direction, per_coin_cash,
                )
                self.trader.set_leverage(c.symbol, params.leverage)
                self.executors[c.symbol] = CoinExecutor(
                    c.symbol, self.trader, params, per_coin_cash, self.state_dir,
                    direction=direction,
                )

        for ex in self.executors.values():
            ex.state.init_cash = per_coin_cash

        self.last_refresh = datetime.now(timezone.utc)
        logger.info("=== 유니버스 갱신 완료: %d 코인 ===", len(self.executors))

    # ── 매 분 step ──────────────────────────────────────
    def _step_all(self, risk_state) -> None:
        for ex in self.executors.values():
            try:
                ex.step(risk_state=risk_state)
            except Exception as e:
                logger.error("[%s] step 오류: %s", ex.coin, e, exc_info=True)

    def _log_status(self, risk_state) -> None:
        cap = sum(e.state.capital for e in self.executors.values())
        n_open = sum(1 for e in self.executors.values() if e.is_open)
        logger.info(
            "=== 자본 $%.2f / 시드 $%.0f (%+.2f%%)  활성 %d/%d  dd=%.2f%% notl=%.0f%% ===",
            cap, self.total_seed, (cap / self.total_seed - 1) * 100,
            n_open, len(self.executors),
            risk_state.drawdown_pct, risk_state.notional_pct,
        )
        for ex in self.executors.values():
            s = ex.state
            mark = "*" if ex.is_open else "."
            logger.info(
                "  %s %s [%-10s %-5s] cap=$%.2f L%d notl=$%.0f trades=%d PnL=$%.2f uPnL=%+.2f%%",
                mark, ex.coin, ex.direction[:5],
                ("OPEN" if ex.is_open else "flat"),
                s.capital, s.dca_level,
                s.primary_notional + s.hedge_notional,
                s.trade_count, s.realized_pnl,
                ex.last_unrealized_pnl_pct * 100,
            )

    # ── 메인 루프 ───────────────────────────────────────
    def run(self) -> None:
        logger.info(
            "DynamicRunner 시작. dry_run=%s sandbox=%s seed=$%.0f top_k=%d dual=%s",
            self.dry_run, self.sandbox, self.total_seed, self.top_k, self.dual_direction,
        )
        self._refresh_universe()

        DataFeed.wait_for_next_bar(buffer_sec=5)
        while not self._stopping:
            if (
                self.last_refresh is None
                or (datetime.now(timezone.utc) - self.last_refresh).total_seconds()
                >= self.refresh_hours * 3600
            ):
                try:
                    self._refresh_universe()
                except Exception as e:
                    logger.error("유니버스 갱신 실패: %s", e, exc_info=True)

            ex_list = list(self.executors.values())
            risk_state = self.risk.update(ex_list)

            self._step_all(risk_state)

            breached, reason = self.risk.is_breached()
            if breached:
                logger.error("Catastrophic 위반 → 비상 청산: %s", reason)
                self.risk.emergency_close_all(self.executors.values())
                self._stopping = True
                break

            self._log_status(risk_state)
            DataFeed.wait_for_next_bar(buffer_sec=5)

        logger.warning("러너 종료 — 모든 포지션 청산 시도")
        for ex in self.executors.values():
            ex.force_close()
