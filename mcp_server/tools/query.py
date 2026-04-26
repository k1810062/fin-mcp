"""
MCP 工具：数据查询。
"""
from __future__ import annotations

from sqlalchemy import Engine

from ..core import querier as q


def handle_etf_info(engine: Engine, etf_code: str | None = None) -> dict:
    """查询 ETF 基本信息"""
    return q.get_etf_info(engine, etf_code)


def handle_daily_quotes(
    engine: Engine,
    asset_type: str = "etf",
    codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
) -> dict:
    """查询行情"""
    return q.get_daily_quotes(engine, asset_type, codes, start_date, end_date, days)


def handle_components(
    engine: Engine,
    etf_code: str,
    trade_date: str | None = None,
    source: str = "pcf",
) -> dict:
    """查询成分股"""
    return q.get_components(engine, etf_code, trade_date, source)


def handle_summary(
    engine: Engine,
    trade_date: str | None = None,
    industry: str | None = None,
) -> dict:
    """涨跌幅排名"""
    return q.get_summary(engine, trade_date, industry)


def handle_changes(engine: Engine, etf_code: str, days: int = 30) -> dict:
    """持仓变更历史"""
    return q.get_changes(engine, etf_code, days)


def handle_basket(engine: Engine, etf_code: str, trade_date: str) -> dict:
    """篮子价值计算"""
    return q.get_basket(engine, etf_code, trade_date)
