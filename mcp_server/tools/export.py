"""
MCP 工具：数据导出。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..core import querier as q
from ..db.models import EtfDailyQuote, EtfInfo


def handle_export_excel(
    engine: Engine,
    export_type: str,
    etf_code: str | None = None,
    trade_date: str | None = None,
    output_path: str | None = None,
) -> dict:
    """
    导出 Excel 报告。
    export_type: summary / etf_quotes / components / changes
    """
    td = date.fromisoformat(trade_date) if trade_date else date.today()

    # 确定输出路径
    if not output_path:
        output_path = f"fin_export_{td.strftime('%Y%m%d')}.xlsx"

    df = None
    if export_type == "summary":
        data = q.get_summary(engine, str(td))
        df = pd.DataFrame(data["records"])
    elif export_type == "etf_quotes" and etf_code:
        data = q.get_daily_quotes(engine, "etf", [etf_code])
        df = pd.DataFrame(data["records"])
    elif export_type == "components" and etf_code:
        data = q.get_components(engine, etf_code, str(td))
        df = pd.DataFrame(data["records"])
    elif export_type == "changes" and etf_code:
        data = q.get_changes(engine, etf_code)
        df = pd.DataFrame(data["records"])

    if df is None or df.empty:
        return {"status": "error", "message": "没有数据可导出"}

    df.to_excel(output_path, index=False)
    return {"status": "ok", "path": str(Path(output_path).resolve()), "rows": len(df)}
