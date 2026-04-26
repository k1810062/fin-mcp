#!/usr/bin/env python
"""
fin-mcp MCP Server 入口。
使用 FastMCP 实现，注册所有金融数据工具。
通过 MCP 协议暴露，任何 MCP 客户端均可调用。
"""
from __future__ import annotations

import logging
import sys
from typing import Optional

from mcp.server.fastmcp import FastMCP

from .config import load_config, get_config
from .db.engine import get_engine
from .db.sync import sync_etf_config
from .ds.router import DataSourceRouter

# ── 全局状态（由 start() 初始化） ────────────────────────────────

_engine = None
_router = None

# ── MCP Server 实例 ─────────────────────────────────────────────

mcp = FastMCP("fin-data")


# ── 工具注册 ─────────────────────────────────────────────────────


@mcp.tool(
    name="fetch.market_data",
    description="""获取ETF/股票日线行情。
Mode A (batch): 获取→入库→返回摘要（不占上下文）
Mode B (query): 直接返回明细数据
参数: asset_type (etf/stock/all), codes, start_date, end_date, mode (batch/query), save_to_db
""",
)
def fetch_market_data(
    asset_type: str = "all",
    codes: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    mode: str = "batch",
    save_to_db: bool = True,
) -> dict:
    from .tools.fetch import handle_fetch_market_data
    return handle_fetch_market_data(
        _engine, _router, asset_type, codes,
        start_date, end_date, mode, save_to_db,
    )


@mcp.tool(
    name="fetch.pcf_components",
    description="""获取PCF成分股快照。
Mode A (batch): 批量获取全部ETF→入库→返回摘要
Mode B (query): 获取单只ETF明细
参数: etf_codes, trade_date, mode, detect_changes
""",
)
def fetch_pcf_components(
    etf_codes: Optional[list[str]] = None,
    trade_date: Optional[str] = None,
    mode: str = "batch",
    detect_changes: bool = True,
) -> dict:
    from .tools.fetch import handle_fetch_pcf
    return handle_fetch_pcf(
        _engine, _router, etf_codes, trade_date, mode, detect_changes,
    )


@mcp.tool(
    name="query.etf_info",
    description="查询ETF基本信息，不传code返回全部",
)
def query_etf_info(etf_code: Optional[str] = None) -> dict:
    from .tools.query import handle_etf_info
    return handle_etf_info(_engine, etf_code)


@mcp.tool(
    name="query.daily_quotes",
    description="查询行情数据，支持ETF和股票",
)
def query_daily_quotes(
    asset_type: str = "etf",
    codes: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: Optional[int] = None,
) -> dict:
    from .tools.query import handle_daily_quotes
    return handle_daily_quotes(_engine, asset_type, codes, start_date, end_date, days)


@mcp.tool(
    name="query.components",
    description="查询指定日期ETF成分股，trade_date为空取最新",
)
def query_components(
    etf_code: str,
    trade_date: Optional[str] = None,
    source: str = "pcf",
) -> dict:
    from .tools.query import handle_components
    return handle_components(_engine, etf_code, trade_date, source)


@mcp.tool(
    name="query.summary",
    description="涨跌幅排名，支持行业筛选",
)
def query_summary(
    trade_date: Optional[str] = None,
    industry: Optional[str] = None,
) -> dict:
    from .tools.query import handle_summary
    return handle_summary(_engine, trade_date, industry)


@mcp.tool(
    name="query.changes",
    description="成分股持仓变更历史",
)
def query_changes(etf_code: str, days: int = 30) -> dict:
    from .tools.query import handle_changes
    return handle_changes(_engine, etf_code, days)


@mcp.tool(
    name="query.basket",
    description="ETF篮子价值计算：成分股×收盘价，对比净值",
)
def query_basket(etf_code: str, trade_date: str) -> dict:
    from .tools.query import handle_basket
    return handle_basket(_engine, etf_code, trade_date)


@mcp.tool(
    name="components.manage",
    description="自定义成分股管理。action: set/reset/list",
)
def manage_components(
    action: str,
    etf_code: Optional[str] = None,
    stocks: Optional[list[dict]] = None,
    trade_date: Optional[str] = None,
) -> dict:
    from .tools.components import handle_manage_components
    return handle_manage_components(_engine, action, etf_code, stocks, trade_date)


@mcp.tool(
    name="export.to_excel",
    description="导出Excel报告。type: summary/etf_quotes/components/changes",
)
def export_to_excel(
    export_type: str,
    etf_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    output_path: Optional[str] = None,
) -> dict:
    from .tools.export import handle_export_excel
    return handle_export_excel(_engine, export_type, etf_code, trade_date, output_path)


@mcp.tool(
    name="maintenance.check",
    description="数据完整性检查",
)
def maintenance_check(checks: Optional[list[str]] = None) -> dict:
    from .tools.maintenance import handle_check_integrity
    return handle_check_integrity(_engine, checks)


@mcp.tool(
    name="maintenance.stats",
    description="数据库统计概览",
)
def maintenance_stats() -> dict:
    from .tools.maintenance import handle_db_stats
    return handle_db_stats(_engine)


# ── 启动 ─────────────────────────────────────────────────────────


def start():
    """初始化并启动 MCP Server（stdio 模式）"""
    global _engine, _router

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    # 确保 UTF-8
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    # 加载配置
    config = load_config()
    logger.info(f"fin-mcp 启动 | 数据源: preferred={config.data_sources.preferred}, "
                f"fallback={config.data_sources.fallback}")

    # 初始化数据库 & 数据源
    _engine = get_engine(config.db_url)
    sync_etf_config(_engine, config)  # 同步 config ETF 列表到 DB（去重）
    _router = DataSourceRouter(
        preferred=config.data_sources.preferred,
        fallback=config.data_sources.fallback,
        pcf_source=config.data_sources.pcf_source,
    )

    # 以 stdio 模式运行
    logger.info("MCP Server 就绪，等待请求...")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    start()
