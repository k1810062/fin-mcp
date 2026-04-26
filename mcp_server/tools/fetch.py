"""
MCP 工具：数据获取。
校验参数 → 调 core → 格式化返回。很薄。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..core.fetcher import batch_fetch_market_data, query_fetch_market_data, batch_fetch_pcf
from ..db.models import EtfDailyQuote
from ..ds.router import DataSourceRouter


def handle_fetch_market_data(
    engine: Engine,
    router: DataSourceRouter,
    asset_type: str = "all",
    codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    mode: str = "batch",
    save_to_db: bool = True,
) -> dict:
    """
    MCP 工具: fetch.market_data
    Mode A (batch): 获取 → 入库 → 返回摘要
    Mode B (query): 获取 → 返回明细（不入库）
    """
    # 参数校验
    if asset_type not in ("etf", "stock", "all"):
        return {"status": "error", "message": f"asset_type 必须为 etf/stock/all: {asset_type}"}
    if mode not in ("batch", "query"):
        return {"status": "error", "message": f"mode 必须为 batch/query: {mode}"}

    # 日期处理：没传日期就用今天
    today = date.today()
    s = date.fromisoformat(start_date) if start_date else today
    e = date.fromisoformat(end_date) if end_date else s

    # 非交易日时向前找最近交易日
    if mode == "batch":
        s = _prev_trade_day(router, s)
        e = _prev_trade_day(router, e) if e != s else s

        result = batch_fetch_market_data(engine, router, asset_type, codes, s, e)
        # 交易日但API无数据
        total_rows = result.get("summary", {}).get("etf_rows", 0) + result.get("summary", {}).get("stock_rows", 0)
        if total_rows == 0:
            result["message"] = "交易日，但API暂无数据返回（可能未到收盘或数据延迟）"
        return result
    else:
        if not codes:
            return {"status": "error", "message": "Mode B (query) 需要指定 codes"}
        return query_fetch_market_data(engine, router, asset_type, codes, s, e)


def handle_fetch_pcf(
    engine: Engine,
    router: DataSourceRouter,
    etf_codes: list[str] | None = None,
    trade_date: str | None = None,
    mode: str = "batch",
    detect_changes: bool = True,
    fetch_stock_quotes: bool = False,
    end_date: str | None = None,
) -> dict:
    """
    MCP 工具: fetch.pcf_components
    trade_date: 起始日期（或单日）
    end_date: 结束日期，指定后遍历该范围内每个交易日逐个拉取 PCF
    fetch_stock_quotes=True 时，PCF 获取后自动拉成分股日线行情。
    """
    raw_td = date.fromisoformat(trade_date) if trade_date else date.today()
    ed = date.fromisoformat(end_date) if end_date else None

    if mode == "batch":
        if ed:
            # 范围模式：不调 _prev_trade_day，让 trading_days 自动过滤非交易日
            result = batch_fetch_pcf(engine, router, etf_codes, raw_td, detect_changes, end_date=ed)
        else:
            td = _prev_trade_day(router, raw_td)
            result = batch_fetch_pcf(engine, router, etf_codes, td, detect_changes)

            # 单日模式全失败时回退到 DB 最新日期
            if result.get("summary", {}).get("ok", 0) == 0:
                with Session(engine) as session:
                    latest = session.query(EtfDailyQuote.trade_date).order_by(EtfDailyQuote.trade_date.desc()).first()
                    if latest and latest[0] != td:
                        td = latest[0]
                        result = batch_fetch_pcf(engine, router, etf_codes, td, detect_changes)

        # 自动拉成分股行情
        if fetch_stock_quotes and result.get("status") == "ok":
            _fetch_component_quotes(engine, router, result, td)

        return result
    else:
        # Mode B: 获取单只并返回明细
        code = etf_codes[0] if etf_codes else None
        if not code:
            return {"status": "error", "message": "Mode B 需要指定 etf_codes"}
        result = router.fetch_pcf(code, td)
        if not result:
            return {"status": "ok", "records": [], "row_count": 0}
        return {
            "status": "ok",
            "records": result.get("components", []),
            "row_count": len(result.get("components", [])),
            "nav": result.get("nav"),
            "nav_per_cu": result.get("nav_per_cu"),
        }


def _fetch_component_quotes(engine: Engine, router: DataSourceRouter, pcf_result: dict, trade_date: date | None = None):
    """PCF 更新后，自动拉成分股日线行情（日期和 PCF 一致，去重）。"""
    ds = str(trade_date) if trade_date else None
    r = handle_fetch_market_data(engine, router, asset_type="stock", mode="batch", start_date=ds, end_date=ds)
    s = r.get("summary", {})
    updated = s.get("stock_updated", 0)
    rows = s.get("stock_rows", 0)
    if updated or rows:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"成分股行情更新: {updated}只 {rows}条")


def _prev_trade_day(router: DataSourceRouter, d: date, max_lookback: int = 10) -> date:
    """非交易日向前找最近交易日，至多找 max_lookback 天。已是交易日直接返回。"""
    if router.is_trading_day(d):
        return d
    for i in range(1, max_lookback + 1):
        prev = d - timedelta(days=i)
        if router.is_trading_day(prev):
            return prev
    return d  # 找不到就返回原日期
