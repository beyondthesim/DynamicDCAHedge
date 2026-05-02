"""거래 단위 CSV 로거 (DynamicDCAHedge).

컬럼: timestamp, strategy, symbol, action, direction,
      price, qty, notional, pnl, fee, reason, dca_level,
      entry_seed, entry_total_equity, pnl_pct

entry_seed         : 진입 시 코인별 시드($) — 손익률 산출의 분모
entry_total_equity : 진입 시 OKX 총 자본($)
pnl_pct            : 청산 시 entry_seed 대비 실현 손익률(%)

기존 CSV는 자동 마이그레이션 (백업 후 새 헤더로 재작성).
"""
from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Final

logger = logging.getLogger(__name__)

_FIELDS: Final[list[str]] = [
    "timestamp", "strategy", "symbol", "action", "direction",
    "price", "qty", "notional", "pnl", "fee", "reason", "dca_level",
    "entry_seed", "entry_total_equity", "pnl_pct",
]

_lock = Lock()
_migrated: set[str] = set()


def _path() -> Path:
    return Path("runtime") / "trade_log.csv"


def _migrate_if_needed(p: Path) -> None:
    if str(p) in _migrated:
        return
    if not p.exists() or p.stat().st_size == 0:
        _migrated.add(str(p))
        return
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            existing_fields = list(reader.fieldnames or [])
            rows = list(reader)
        missing = [c for c in _FIELDS if c not in existing_fields]
        if not missing:
            _migrated.add(str(p))
            return
        bak = p.with_suffix(p.suffix + ".bak")
        p.replace(bak)
        with p.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_FIELDS, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in _FIELDS})
        logger.info("trade_log 마이그레이션 완료: %d rows, +%s", len(rows), missing)
    except (OSError, csv.Error) as e:
        logger.warning("trade_log 마이그레이션 실패: %s", e)
    finally:
        _migrated.add(str(p))


def _ensure_header(p: Path) -> None:
    if p.exists() and p.stat().st_size > 0:
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(_FIELDS)


def _fmt(v: float | int | None) -> str:
    if v is None:
        return ""
    return f"{v:.8f}".rstrip("0").rstrip(".") or "0"


def log_event(
    *,
    strategy: str,
    symbol: str,
    action: str,
    direction: str = "",
    price: float = 0.0,
    qty: float = 0.0,
    notional: float = 0.0,
    pnl: float = 0.0,
    fee: float = 0.0,
    reason: str = "",
    dca_level: int = 0,
    entry_seed: float | None = None,
    entry_total_equity: float | None = None,
    pnl_pct: float | None = None,
) -> None:
    p = _path()
    try:
        with _lock:
            _migrate_if_needed(p)
            _ensure_header(p)
            with p.open("a", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow([
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    strategy, symbol, action, direction,
                    _fmt(price), _fmt(qty), f"{notional:.4f}",
                    f"{pnl:.6f}", f"{fee:.6f}",
                    reason, int(dca_level),
                    _fmt(entry_seed), _fmt(entry_total_equity),
                    "" if pnl_pct is None else f"{pnl_pct:.4f}",
                ])
    except (OSError, ValueError) as e:
        logger.warning("trade_log 기록 실패: %s", e)
