"""OKX 무기한 선물 거래 클라이언트 (REST 기반).

OKXClient를 확장: 주문, 포지션, 잔고 조회.
ccxt 라이브러리 사용. 주의: dry-run 모드에서는 _ex.create_order 호출 안 됨.
"""
from __future__ import annotations
import time
import logging
import os
from typing import Optional, Literal
from dataclasses import dataclass, field
import ccxt
from dotenv import load_dotenv
from pathlib import Path

# .env.secret 우선 → 없으면 .env
_secret_path = Path(__file__).resolve().parents[2] / ".env.secret"
if _secret_path.exists():
    load_dotenv(_secret_path)
else:
    load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class Order:
    """체결 결과."""
    id: str
    symbol: str
    side: Literal["buy", "sell"]
    type: str
    price: float
    amount: float       # 계약수
    cost: float         # USDT 명목가
    fee: float
    timestamp: int
    status: str
    raw: dict = field(default_factory=dict, repr=False)


@dataclass
class Position:
    """포지션 정보."""
    symbol: str
    side: Literal["long", "short", "flat"]
    contracts: float    # 계약수
    notional: float     # USDT 명목가
    avg_price: float
    unrealized_pnl: float
    leverage: float
    raw: dict = field(default_factory=dict, repr=False)


class OKXTrader:
    """OKX 선물 거래용 확장 클라이언트.

    Args:
        dry_run: True면 실제 주문 호출 차단 (로그만)
        sandbox: True면 OKX demo (sandbox) 환경 사용
    """
    _RATE_LIMIT_DELAY = 0.15

    def __init__(self, dry_run: bool = True, sandbox: bool = False):
        self.dry_run = dry_run
        params = {
            "apiKey":  os.getenv("OKX_API_KEY", ""),
            "secret":  os.getenv("OKX_SECRET", ""),
            "password":os.getenv("OKX_PASSPHRASE", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        self._ex = ccxt.okx(params)
        if sandbox:
            self._ex.set_sandbox_mode(True)
        self._last_call = 0.0
        if not dry_run and not params["apiKey"]:
            raise RuntimeError("실거래 모드에서 OKX API 키가 설정되지 않았습니다 (.env)")
        mode = "DRY-RUN" if dry_run else ("SANDBOX" if sandbox else "LIVE")
        logger.info("OKXTrader 초기화: mode=%s", mode)

    # ── 내부 헬퍼 ─────────────────────────────────────────
    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self._RATE_LIMIT_DELAY:
            time.sleep(self._RATE_LIMIT_DELAY - elapsed)
        self._last_call = time.time()

    def _retry(self, fn, *args, max_retries: int = 5, **kwargs):
        for attempt in range(max_retries):
            try:
                self._throttle()
                return fn(*args, **kwargs)
            except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
                wait = 2 ** attempt
                logger.warning("retry %d/%d: %s — wait %ds", attempt+1, max_retries, e, wait)
                time.sleep(wait)
        raise RuntimeError(f"failed after {max_retries} retries")

    # ── OHLCV ─────────────────────────────────────────────
    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300) -> list[list]:
        return self._retry(self._ex.fetch_ohlcv, symbol, timeframe, limit=limit)

    def fetch_ticker(self, symbol: str) -> dict:
        return self._retry(self._ex.fetch_ticker, symbol)

    # ── 잔고/포지션 ───────────────────────────────────────
    def fetch_balance(self) -> dict:
        if self.dry_run:
            return {"USDT": {"free": 10000.0, "used": 0.0, "total": 10000.0}}
        return self._retry(self._ex.fetch_balance)

    def fetch_position(self, symbol: str) -> Position:
        """단일 심볼 포지션 조회. 없으면 flat."""
        if self.dry_run:
            return Position(symbol=symbol, side="flat", contracts=0, notional=0,
                            avg_price=0, unrealized_pnl=0, leverage=20)
        positions = self._retry(self._ex.fetch_positions, [symbol])
        for p in positions:
            if p["symbol"] == symbol and float(p.get("contracts") or 0) > 0:
                side = "long" if p["side"] == "long" else "short"
                return Position(
                    symbol=symbol, side=side,
                    contracts=float(p["contracts"]),
                    notional=float(p.get("notional") or 0),
                    avg_price=float(p["entryPrice"]),
                    unrealized_pnl=float(p.get("unrealizedPnl") or 0),
                    leverage=float(p.get("leverage") or 20),
                    raw=p,
                )
        return Position(symbol=symbol, side="flat", contracts=0, notional=0,
                        avg_price=0, unrealized_pnl=0, leverage=20)

    # ── 레버리지 설정 ─────────────────────────────────────
    def set_leverage(self, symbol: str, leverage: float, margin_mode: str = "cross"):
        """레버리지 + 마진 모드 설정."""
        if self.dry_run:
            logger.info("[DRY] set_leverage %s lev=%s mode=%s", symbol, leverage, margin_mode)
            return
        try:
            self._throttle()
            self._ex.set_leverage(int(leverage), symbol,
                                   params={"mgnMode": margin_mode})
        except Exception as e:
            logger.warning("set_leverage 실패: %s", e)

    # ── 주문 ──────────────────────────────────────────────
    def market_order(
        self,
        symbol: str,
        side: Literal["buy", "sell"],
        notional_usd: float,
        reduce_only: bool = False,
        position_side: Optional[Literal["long", "short"]] = None,
    ) -> Order:
        """시장가 주문. notional_usd로 사이즈 자동 계산.

        position_side: hedged 모드일 때 'long' 또는 'short' 명시
        """
        ticker = self.fetch_ticker(symbol)
        price = float(ticker["last"])
        market = self._ex.market(symbol)
        contract_size = float(market.get("contractSize") or 1)
        contracts = notional_usd / (price * contract_size)
        # 마켓의 최소 amount + precision 적용
        min_amount = float((market.get("limits", {}).get("amount", {}) or {}).get("min") or 0.1)
        contracts = max(contracts, min_amount)
        try:
            contracts = float(self._ex.amount_to_precision(symbol, contracts))
        except Exception:
            contracts = round(contracts, 1)

        params = {}
        if reduce_only:
            params["reduceOnly"] = True
        if position_side:
            params["posSide"] = position_side
        else:
            params["posSide"] = "net"

        if self.dry_run:
            logger.info("[DRY] market %s %s notional=$%.2f contracts=%.0f price=%.6f reduce=%s",
                        symbol, side, notional_usd, contracts, price, reduce_only)
            return Order(
                id=f"dry-{int(time.time()*1000)}",
                symbol=symbol, side=side, type="market",
                price=price, amount=contracts,
                cost=notional_usd, fee=notional_usd*0.0005,
                timestamp=int(time.time()*1000), status="filled",
            )

        result = self._retry(self._ex.create_order, symbol, "market", side, contracts, None, params)
        return Order(
            id=str(result["id"]),
            symbol=symbol, side=side, type="market",
            price=float(result.get("average") or price),
            amount=float(result.get("filled") or contracts),
            cost=float(result.get("cost") or notional_usd),
            fee=float((result.get("fee") or {}).get("cost") or 0),
            timestamp=int(result.get("timestamp") or time.time()*1000),
            status=str(result.get("status") or "filled"),
            raw=result,
        )

    def close_position(self, symbol: str, position_side: str = "net") -> Optional[Order]:
        """현재 포지션을 시장가로 전량 청산. 포지션 없으면 None."""
        pos = self.fetch_position(symbol)
        if pos.side == "flat" or pos.contracts == 0:
            return None
        side = "sell" if pos.side == "long" else "buy"
        ticker = self.fetch_ticker(symbol)
        price = float(ticker["last"])
        market = self._ex.market(symbol)
        cs = float(market.get("contractSize") or 1)
        notional = pos.contracts * price * cs

        if self.dry_run:
            logger.info("[DRY] close %s %s contracts=%.0f notional=$%.2f",
                        symbol, side, pos.contracts, notional)
            return Order(
                id=f"dry-close-{int(time.time()*1000)}",
                symbol=symbol, side=side, type="market",
                price=price, amount=pos.contracts,
                cost=notional, fee=notional*0.0005,
                timestamp=int(time.time()*1000), status="filled",
            )

        params = {"reduceOnly": True, "posSide": position_side}
        result = self._retry(self._ex.create_order, symbol, "market", side,
                             pos.contracts, None, params)
        return Order(
            id=str(result["id"]),
            symbol=symbol, side=side, type="market",
            price=float(result.get("average") or price),
            amount=float(result.get("filled") or pos.contracts),
            cost=float(result.get("cost") or notional),
            fee=float((result.get("fee") or {}).get("cost") or 0),
            timestamp=int(result.get("timestamp") or time.time()*1000),
            status=str(result.get("status") or "filled"),
            raw=result,
        )
