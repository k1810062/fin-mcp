"""
MCP 工具：运维。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from sqlalchemy import Engine, func
from sqlalchemy.orm import Session

from ..db.models import DailyQuote, EtfComponent, EtfDailyQuote, EtfInfo, UpdateLog

logger = logging.getLogger(__name__)


def handle_check_integrity(engine: Engine, checks: list[str] | None = None) -> dict:
    """数据完整性检查"""
    if not checks:
        checks = ["missing_quotes", "date_gaps", "future_function", "integrity"]

    results = {}
    for check in checks:
        if check == "missing_quotes":
            results["missing_quotes"] = _check_missing_quotes(engine)
        elif check == "date_gaps":
            results["date_gaps"] = _check_date_gaps(engine)
        elif check == "future_function":
            results["future_function"] = _check_future_function(engine)
        elif check == "integrity":
            results["integrity"] = _check_integrity(engine)

    return {"status": "ok", "checks": results}


def handle_db_stats(engine: Engine) -> dict:
    """数据库统计"""
    with Session(engine) as session:
        etf_count = session.query(EtfInfo).count()
        stock_count = (
            session.query(EtfComponent.stock_code)
            .filter(EtfComponent.substitute_flag != "2")
            .distinct()
            .count()
        )
        quote_rows = session.query(DailyQuote).count()
        etf_quote_rows = session.query(EtfDailyQuote).count()
        comp_rows = session.query(EtfComponent).count()

        first_date = session.query(func.min(DailyQuote.trade_date)).scalar()
        last_date = session.query(func.max(DailyQuote.trade_date)).scalar()

    return {
        "status": "ok",
        "stats": {
            "etf_count": etf_count,
            "unique_stocks": stock_count,
            "stock_quote_rows": quote_rows,
            "etf_quote_rows": etf_quote_rows,
            "component_rows": comp_rows,
            "date_range": f"{first_date} ~ {last_date}" if first_date else "无数据",
        },
    }


def _check_missing_quotes(engine: Engine) -> dict:
    """检查：有成分股记录但没有行情的股票"""
    with Session(engine) as session:
        comp_stocks = set(
            r[0] for r in session.query(EtfComponent.stock_code)
            .filter(EtfComponent.substitute_flag != "2")
            .distinct()
        )
        quote_stocks = set(
            r[0] for r in session.query(DailyQuote.stock_code).distinct()
        )
        missing = comp_stocks - quote_stocks
        return {"count": len(missing), "stocks": list(missing)[:50]}


def _check_date_gaps(engine: Engine) -> dict:
    """检查：行情数据是否有日期断层"""
    with Session(engine) as session:
        dates = sorted(
            r[0] for r in session.query(DailyQuote.trade_date).distinct()
            if r[0]
        )
        if len(dates) < 2:
            return {"count": 0, "gaps": []}
        gaps = []
        for i in range(1, len(dates)):
            diff = (dates[i] - dates[i - 1]).days
            if diff > 5:  # 超过 5 天视为异常断层
                gaps.append({"from": str(dates[i - 1]), "to": str(dates[i]), "days": diff})
        return {"count": len(gaps), "gaps": gaps[:20]}


def _check_future_function(engine: Engine) -> dict:
    """检查：pct_chg 是否使用了未来数据（通过日志扫描，留作扩展）"""
    return {"status": "需手动验证核心逻辑,当前core已无未来函数"}


def _check_integrity(engine: Engine) -> dict:
    """检查：外键完整性、异常值"""
    issues = []
    with Session(engine) as session:
        # 孤立的成分股
        orphans = (
            session.query(EtfComponent)
            .outerjoin(EtfInfo)
            .filter(EtfInfo.id.is_(None))
            .count()
        )
        if orphans:
            issues.append(f"{orphans} 条成分股无对应 ETF")
        # 涨跌幅异常
        bad = (
            session.query(EtfDailyQuote)
            .filter(
                EtfDailyQuote.pct_chg.isnot(None),
                abs(EtfDailyQuote.pct_chg) > 30,
            )
            .count()
        )
        if bad:
            issues.append(f"{bad} 条 ETF 涨跌幅 > 30%")
    return {"issues": issues, "count": len(issues)}
