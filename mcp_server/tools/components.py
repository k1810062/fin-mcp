"""
MCP 工具：自定义成分股管理。
"""
from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..db.models import EtfComponent, EtfInfo

logger = logging.getLogger(__name__)


def handle_manage_components(
    engine: Engine,
    action: str,
    etf_code: str | None = None,
    stocks: list[dict] | None = None,
    trade_date: str | None = None,
) -> dict:
    """
    自定义成分股管理。
    action: set / reset / list
    """
    td = date.fromisoformat(trade_date) if trade_date else date.today()

    if action == "list":
        return _list_manual(engine)

    if not etf_code:
        return {"status": "error", "message": "set/reset 需要 etf_code"}

    if action == "set":
        return _set_manual(engine, etf_code, stocks or [], td)
    elif action == "reset":
        return _reset_to_pcf(engine, etf_code)
    else:
        return {"status": "error", "message": f"未知 action: {action}"}


def _set_manual(engine: Engine, etf_code: str, stocks: list[dict],
                trade_date: date) -> dict:
    with Session(engine) as session:
        etf = session.query(EtfInfo).filter_by(code=etf_code).first()
        if not etf:
            return {"status": "error", "message": f"ETF {etf_code} 不存在"}

        session.query(EtfComponent).filter_by(
            etf_id=etf.id, source="manual",
        ).delete()

        for s in stocks:
            session.add(EtfComponent(
                etf_id=etf.id,
                stock_code=s["stock_code"],
                stock_name=s.get("stock_name", s["stock_code"]),
                weight=s.get("weight"),
                quantity=s.get("quantity"),
                substitute_flag=s.get("substitute_flag", "1"),
                trade_date=trade_date,
                source="manual",
            ))
        session.commit()
        logger.info(f"自定义成分股: {etf_code} 已写入 {len(stocks)} 只")
        return {"status": "ok", "etf_code": etf_code, "count": len(stocks)}


def _reset_to_pcf(engine: Engine, etf_code: str) -> dict:
    with Session(engine) as session:
        etf = session.query(EtfInfo).filter_by(code=etf_code).first()
        if not etf:
            return {"status": "error", "message": f"ETF {etf_code} 不存在"}
        deleted = session.query(EtfComponent).filter_by(
            etf_id=etf.id, source="manual",
        ).delete()
        session.commit()
        return {"status": "ok", "etf_code": etf_code, "deleted": deleted}


def _list_manual(engine: Engine) -> dict:
    with Session(engine) as session:
        rows = session.query(EtfInfo, EtfComponent).join(EtfComponent).filter(
            EtfComponent.source == "manual",
        ).all()
        groups = {}
        for etf, comp in rows:
            code = etf.code
            if code not in groups:
                groups[code] = {"etf_name": etf.name, "stocks": []}
            groups[code]["stocks"].append({
                "stock_code": comp.stock_code,
                "stock_name": comp.stock_name,
                "weight": comp.weight,
            })
        return {"status": "ok", "etfs": list(groups.values()), "count": len(groups)}
