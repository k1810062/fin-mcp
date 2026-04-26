"""
数据查询核心逻辑。
只读 DB，不获取外部数据。不知道 MCP 存在。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..db.models import (
    ComponentChange, DailyQuote, EtfComponent,
    EtfDailyQuote, EtfInfo, Industry,
)


def to_date(d) -> date:
    if isinstance(d, date):
        return d
    return date.fromisoformat(str(d).replace("-", "").replace("/", ""))


def get_etf_info(engine: Engine, etf_code: str | None = None) -> dict:
    """查询 ETF 基本信息。etf_code=None 返回全部。"""
    with Session(engine) as session:
        q = session.query(EtfInfo).order_by(EtfInfo.code)
        if etf_code:
            q = q.filter(EtfInfo.code == etf_code)
        rows = q.all()
        records = []
        for etf in rows:
            industries = [link.industry.industry_name for link in etf.industry_links
                          if link.industry]
            records.append({
                "code": etf.code,
                "name": etf.name,
                "industries": industries,
            })
        return {"records": records, "row_count": len(records)}


def get_daily_quotes(
    engine: Engine,
    asset_type: str,
    codes: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
) -> dict:
    """查询行情。asset_type: 'etf' | 'stock'"""
    with Session(engine) as session:
        if asset_type == "etf":
            model = EtfDailyQuote
            code_col = EtfDailyQuote.etf_code
        else:
            model = DailyQuote
            code_col = DailyQuote.stock_code

        q = session.query(model)
        if codes:
            q = q.filter(code_col.in_(codes))
        if start_date:
            q = q.filter(model.trade_date >= to_date(start_date))
        if end_date:
            q = q.filter(model.trade_date <= to_date(end_date))
        if days:
            q = q.filter(model.trade_date >= date.today() - timedelta(days=days))

        rows = q.order_by(model.trade_date.desc(), code_col).all()
        records = []
        for r in rows:
            rec = {"trade_date": str(r.trade_date)}
            if asset_type == "etf":
                rec["code"] = r.etf_code
            else:
                rec["code"] = r.stock_code
            rec["open"] = r.open
            rec["high"] = r.high
            rec["low"] = r.low
            rec["close"] = r.close
            rec["pre_close"] = r.pre_close
            rec["pct_chg"] = r.pct_chg
            rec["volume"] = r.volume
            rec["amount"] = r.amount
            if asset_type == "etf":
                rec["nav"] = r.nav
            records.append(rec)

        return {"records": records, "row_count": len(records)}


def get_components(
    engine: Engine,
    etf_code: str,
    trade_date: str | None = None,
    source: str = "pcf",
) -> dict:
    """查询成分股。trade_date=None 取最新。"""
    with Session(engine) as session:
        etf = session.query(EtfInfo).filter_by(code=etf_code).first()
        if not etf:
            return {"records": [], "row_count": 0}

        if trade_date:
            td = to_date(trade_date)
        else:
            latest = session.query(EtfComponent.trade_date).filter(
                EtfComponent.etf_id == etf.id,
                EtfComponent.source == source,
            ).order_by(EtfComponent.trade_date.desc()).first()
            if not latest:
                return {"records": [], "row_count": 0}
            td = latest[0]

        rows = session.query(EtfComponent).filter(
            EtfComponent.etf_id == etf.id,
            EtfComponent.trade_date == td,
            EtfComponent.source == source,
        ).order_by(EtfComponent.stock_code).all()

        records = []
        for r in rows:
            records.append({
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "quantity": r.quantity,
                "weight": r.weight,
                "substitute_flag": r.substitute_flag,
            })
        return {"records": records, "row_count": len(records), "trade_date": str(td)}


def get_summary(
    engine: Engine,
    trade_date: str | None = None,
    industry: str | None = None,
) -> dict:
    """涨跌幅排名。trade_date=None 取最新。"""
    with Session(engine) as session:
        if trade_date:
            td = to_date(trade_date)
        else:
            latest = session.query(EtfDailyQuote.trade_date).filter(
                EtfDailyQuote.pct_chg.isnot(None),
            ).order_by(EtfDailyQuote.trade_date.desc()).first()
            if not latest:
                return {"records": [], "row_count": 0}
            td = latest[0]

        q = session.query(EtfDailyQuote, EtfInfo).join(EtfInfo).filter(
            EtfDailyQuote.trade_date == td,
        )

        rows = q.all()
        records = []
        for eq, etf in rows:
            # 行业筛选
            if industry:
                industry_names = [link.industry.industry_name for link in etf.industry_links if link.industry]
                if industry not in industry_names:
                    continue
            industries = [link.industry.industry_name for link in etf.industry_links if link.industry]
            records.append({
                "code": etf.code,
                "name": etf.name,
                "industries": industries,
                "close": eq.close,
                "pct_chg": eq.pct_chg,
                "amount": eq.amount,
                "nav": eq.nav,
            })

        records.sort(key=lambda r: (r["pct_chg"] or 0), reverse=True)
        for i, rec in enumerate(records, 1):
            rec["rank"] = i

        return {"records": records, "row_count": len(records), "trade_date": str(td)}


def get_changes(engine: Engine, etf_code: str, days: int = 30) -> dict:
    """成分股变更历史。"""
    with Session(engine) as session:
        etf = session.query(EtfInfo).filter_by(code=etf_code).first()
        if not etf:
            return {"records": [], "row_count": 0}

        rows = session.query(ComponentChange).filter(
            ComponentChange.etf_id == etf.id,
        ).order_by(ComponentChange.trade_date.desc()).limit(days).all()

        records = []
        for r in rows:
            records.append({
                "date": str(r.trade_date),
                "change_type": r.change_type,
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "old_quantity": r.old_quantity,
                "new_quantity": r.new_quantity,
            })
        return {"records": records, "row_count": len(records)}


def get_basket(engine: Engine, etf_code: str, trade_date: str) -> dict:
    """ETF 篮子价值计算。"""
    td = to_date(trade_date)
    result = {"etf": etf_code, "trade_date": str(td)}

    with Session(engine) as session:
        etf = session.query(EtfInfo).filter_by(code=etf_code).first()
        if not etf:
            return result

        eq = session.query(EtfDailyQuote).filter(
            EtfDailyQuote.etf_id == etf.id,
            EtfDailyQuote.trade_date == td,
        ).first()
        if eq:
            result["etf_close"] = eq.close
            result["nav"] = eq.nav
            result["nav_per_cu"] = eq.nav_per_cu

        comps = session.query(EtfComponent, DailyQuote).outerjoin(
            DailyQuote,
            (EtfComponent.stock_code == DailyQuote.stock_code) &
            (DailyQuote.trade_date == td),
        ).filter(
            EtfComponent.etf_id == etf.id,
            EtfComponent.trade_date == td,
            EtfComponent.source == "pcf",
            EtfComponent.substitute_flag != "2",
        ).all()

        rows = []
        total_value = 0.0
        for comp, dq in comps:
            qty = comp.quantity or 0
            price = dq.close if dq and dq.close else None
            value = qty * price if price and qty else None
            if value:
                total_value += value
            rows.append({
                "stock_code": comp.stock_code,
                "stock_name": comp.stock_name,
                "quantity": qty,
                "close": price,
                "value": value,
            })

        result["basket_value"] = total_value if total_value else None
        if total_value:
            result["basket_value_per_cu"] = round(total_value / 1_000_000, 4)
        result["components"] = rows

    return result
