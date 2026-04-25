from pathlib import Path
import yaml
from dataclasses import dataclass, field


@dataclass
class DataConfig:
    symbol: str = "TURBO/USDT:USDT"
    timeframe: str = "1m"
    lookback_days: int = 730


@dataclass
class BacktestConfig:
    init_cash: float = 10000.0
    fee_rate: float = 0.0005
    slippage: float = 0.0001


@dataclass
class RegimeConfig:
    adx_period: int = 14
    adx_strong_thresh: float = 30.0
    ema_period: int = 200
    atr_period: int = 14
    atr_lookback: int = 100


@dataclass
class StrategyParams:
    rsi_period: int = 14
    rsi_thresh: float = 30.0
    bb_period: int = 20
    bb_std: float = 2.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    vol_window: int = 20
    vol_thresh: float = 1.5
    dca_drops: list = field(default_factory=lambda: [0.02, 0.04, 0.07, 0.11, 0.16, 0.22])
    size_mults: list = field(default_factory=lambda: [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0])
    hedge_entry: int = 4
    hedge_ratio: float = 0.5
    tp_pct: float = 0.03
    sl_pct: float = 0.25
    sl_active: bool = True
    base_margin_pct: float = 0.01
    leverage: float = 10.0
    dynamic_hedge: bool = False
    rsi_source: str = "close"    # "close" | "ohlc4"
    # ── 고급 진입/청산 옵션 ──────────────────────────────────────────────────
    rsi_mode: str = "below"      # "below": RSI<thresh | "rising": RSI<thresh AND 상승 반전
    tp1_pct: float = 0.0         # 부분 TP 레벨 (0=비활성). 예: 0.025 = +2.5%
    tp1_size: float = 0.5        # 부분 TP 시 청산 비율 (0.5 = 50%)
    btc_filter: bool = False     # True: BTC 4h 하락 추세이면 신규 진입 차단
    # ── 독립 숏 전략 ─────────────────────────────────────────────────────────
    enable_short: bool = False       # True: RSI 과매수 시 독립 숏 진입
    short_rsi_thresh: float = 75.0   # 숏 진입 RSI 임계값 (과매수)
    short_tp_pct: float = 0.020      # 숏 TP: 진입가 대비 하락률
    short_sl_pct: float = 0.050      # 숏 SL: 진입가 대비 상승률
    short_margin_pct: float = 0.010  # 숏 기본 마진 (자본 대비)
    # ── 역추세 다이버전스 진입 모드 ─────────────────────────────────────────
    entry_mode: str = "rsi_bb"       # "rsi_bb" (기존) | "divergence" (역추세)
    red_candle_n: int = 4            # 연속 음봉 수 (1차 진입 조건)
    ema_entry_period: int = 30       # EMA 진입 필터 기간
    dca_next_drop: float = 0.013     # 추가 DCA 드롭 기준 (마지막 진입가 대비)
    all_close_pct: float = 0.040     # 전체 익절 (평단가 대비)
    safe_close_n: int = 4            # safe close 최소 DCA 레벨
    safe_close_pct: float = 0.020    # safe close 익절 비율
    loss_close_n: int = 9            # 손절 최소 DCA 레벨
    loss_close_pct: float = 0.005    # 손절 기준 (마지막 진입가 대비 하락)
    # ── 추세매매 (trend_long/trend_short) 파라미터 ──────────────────────────
    trend_ema_fast: int = 20         # 단기 EMA
    trend_ema_slow: int = 50         # 장기 EMA
    trend_adx_period: int = 14       # ADX 기간
    trend_adx_thresh: float = 25.0   # 추세 강도 임계값
    trend_slope_thresh: float = 0.05 # EMA slope 최소값 (%/봉)
    trend_tp_pct: float = 0.020      # 추세 TP
    trend_sl_pct: float = 0.010      # 추세 SL (엄격)
    trend_trail_pct: float = 0.005   # trailing stop (peak 대비 역행)
    trend_margin_pct: float = 0.020  # 추세 포지션 마진


@dataclass
class AppConfig:
    data: DataConfig = field(default_factory=DataConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    strategy: StrategyParams = field(default_factory=StrategyParams)
    regime: RegimeConfig = field(default_factory=RegimeConfig)


def load_config(path: str | Path | None = None) -> AppConfig:
    if path is None:
        path = Path(__file__).parent.parent.parent / "config" / "default.yaml"

    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    cfg = AppConfig()

    if "data" in raw:
        cfg.data = DataConfig(**raw["data"])
    if "backtest" in raw:
        cfg.backtest = BacktestConfig(**raw["backtest"])
    if "strategy" in raw:
        s = raw["strategy"]
        cfg.strategy = StrategyParams(**s)
    if "regime" in raw:
        cfg.regime = RegimeConfig(**raw["regime"])

    return cfg
