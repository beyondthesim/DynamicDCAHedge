# DynamicDCAHedge

OKX USDT-perp 시장에서 매일 신규 상장 / 탑게이너 코인을 동적으로 발굴하고,
양방향 역추세(롱/숏) + DCA + 30% 헷지 전략을 자동 운용하는 시스템.

## 핵심 아이디어

- **동적 코인 회전**: 12시간마다 OKX 전체 USDT-perp 시장에서 후보를 스캔
  - 신규 상장 7~180일
  - 24h price-change 절대값 상위 40
  - 24h quote-volume ≥ $500K
- **품질 필터**: 후보 코인에 짧은 백테스트(45일)를 돌려 거래 수·MDD 기준 필터링
- **양방향 자동 선택**: 코인별로 long_divergence / short_divergence 둘 다 백테스트해서
  Calmar(`ret/|mdd|`)가 더 좋은 방향으로 진입
- **DCA + 정적 헷지**: 1차 진입 후 다이버전스 추가 발생 시 단계적 DCA, L3 도달 시
  반대방향 30% 헷지 자동 진입
- **부분 익절**: 평단 +1.2% 도달 시 40% 부분 익절 + 잔여 포지션 손절선 BE 자동 상향

## 백테스트 결과 (2025-12-31 ~ 2026-04-25, 4개월)

19개 신규 상장 코인 / 양방향 자동 선택 / 코인당 평균 64일

| 지표 | 값 |
|------|----|
| 평균 누적 수익률 | **+29.2%** (연환산 ≈ +387%) |
| 평균 MDD | **-23.8%** |
| 승률 | **84.2%** (16/19 코인) |
| 거래 수 합계 | 3,073건 |

### 코인별 성과 발췌

| 코인 | 방향 | 일수 | 수익률 | MDD |
|------|------|-----:|-------:|----:|
| SPACE | short | 91 | +85.4% | -41.0% |
| ACU | short | 91 | +77.0% | -20.2% |
| ZAMA | short | 104 | +75.0% | -18.0% |
| RIVER | long | 105 | +70.3% | -24.1% |
| TRIA | short | 78 | +49.0% | -26.7% |
| ROBO | long | 55 | +41.8% | -20.3% |

상세: [`results/v2_best_per_coin.csv`](results/v2_best_per_coin.csv)

## 설치

```bash
# 의존성
pip install -r requirements.txt

# 환경 변수
cp .env.example .env
# .env에 OKX API 키 입력
```

`.env` 형식:
```
OKX_API_KEY=...
OKX_SECRET=...
OKX_PASSPHRASE=...
OKX_SANDBOX=false
```

## 실행

```bash
# 실거래 (옵션 없으면 default)
python scripts/run_dynamic.py

# 시뮬 (주문 안 나감)
python scripts/run_dynamic.py --dry-run

# OKX demo 환경
python scripts/run_dynamic.py --sandbox

# 오늘의 코인 풀 미리보기
python scripts/scan_universe.py
```

로그: `runtime/dynamic.log`
상태 파일: `runtime/state_<COIN>.json` (재시작 후 포지션 복원)

## 설정

[`results/PRODUCTION_config.json`](results/PRODUCTION_config.json):

```json
{
  "version": "2.1",
  "total_seed_usd": 100.0,
  "top_k_coins": 8,
  "refresh_hours": 12,
  "dual_direction": true,
  "catastrophic_loss_pct": 95.0,
  "strategy_params": {
    "red_candle_n": 1,
    "dca_next_drop": 0.005,
    "all_close_pct": 0.025,
    "safe_close_n": 2,
    "safe_close_pct": 0.018,
    "loss_close_n": 7,
    "loss_close_pct": 0.006,
    "base_margin_pct": 0.008,
    "leverage": 20.0,
    "hedge_entry": 3,
    "hedge_ratio": 0.3,
    "tp1_pct": 0.012,
    "tp1_size": 0.4,
    "size_mults": [1, 1, 1, 1, 2, 3, 5, 8, 13, 21]
  }
}
```

## 디렉터리

```
src/
├── data/         # OKX 클라이언트 / 캐시 / fetcher
├── indicators/   # 지표 계산 (RSI/BB/MACD/ATR/regime/divergence/EMA)
├── strategy/     # DCA-Hedge 시뮬레이터 (백테스트 엔진과 공유)
├── backtest/     # run_prepared / metrics
├── selector/     # 유니버스 스캐너 / 양방향 품질 필터 / 랭커
├── live/         # OKXTrader / DataFeed / CoinExecutor / RiskManager / DynamicRunner
└── utils/        # config 로더

scripts/
├── run_dynamic.py       # 실거래 진입점
└── scan_universe.py     # 코인 풀 스캔만

config/default.yaml      # 백테스트 / 지표 기본값
results/                 # PRODUCTION_config + 백테스트 산출물
runtime/                 # 실행 상태 (.gitignore)
```

## 리스크 가드

`catastrophic_loss_pct: 95` 단 하나만 — 시드 대비 −95% 도달 시 전체 청산 + 종료.
이는 백테스트의 `min_capital = init_cash × 0.05`(95% 손실 시 거래 중단)와 등가입니다.

진입/사이즈/헷지 결정은 모두 백테스트 엔진과 1:1 동일.

## 면책

이 코드는 연구·교육 목적으로 공개합니다.
실거래 사용 시 발생하는 손실은 본인 책임이며, 충분한 dry-run / sandbox 테스트 후
소액으로 시작하는 것을 권장합니다.
