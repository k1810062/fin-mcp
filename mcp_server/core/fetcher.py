"""
数据获取核心逻辑。
不知道 MCP 存在，纯粹的 Python 函数：入参 dict，出参 dict。
负责：调数据源 → 处理 → 写库 → 返回结果
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..config import get_config
from ..db.models import (
    ComponentChange, DailyQuote, EtfComponent,
    EtfDailyQuote, EtfInfo, EtfIndustryLink,
    Industry, UpdateLog,
)
from ..ds.router import DataSourceRouter

logger = logging.getLogger(__name__)


def _ensure_etf_info(engine: Engine):
    """确保 config.json 中所有 ETF 写入 etf_info + 行业映射（冷启动用）"""
    config = get_config()
    with Session(engine) as session:
        for item in config.etfs:
            industry_name = item["industry"]
            code = item["code"]
            name = item.get("name", code)

            ind = session.query(Industry).filter_by(industry_name=industry_name).first()
            if ind is None:
                ind = Industry(industry_name=industry_name)
                session.add(ind)
                session.flush()

            etf = session.query(EtfInfo).filter_by(code=code).first()
            if etf is None:
                etf = EtfInfo(code=code, name=name)
                session.add(etf)
                session.flush()

            link = session.query(EtfIndustryLink).filter_by(
                etf_id=etf.id, industry_id=ind.id
            ).first()
            if link is None:
                session.add(EtfIndustryLink(etf_id=etf.id, industry_id=ind.id))

        session.commit()
    logger.info(f"ETF信息同步完成: {len({e['code'] for e in config.etfs})} 只")


# ── 批量获取行情（Mode A：静默入库） ──────────────────────────────


def batch_fetch_market_data(
    engine: Engine,
    router: DataSourceRouter,
    asset_type: str,
    codes: list[str] | None,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Mode A 入口。
    获取数据 → 写入 DB → 返回统计摘要（不返回明细，节省上下文）。
    """
    _t0 = time.time()
    _ensure_etf_info(engine)
    result = {"status": "ok", "asset_type": asset_type, "start": str(start_date), "end": str(end_date)}

    if asset_type in ("etf", "all"):
        etf_result = _batch_fetch_etf_quotes(engine, router, codes, start_date, end_date)
        result["etf"] = etf_result

    if asset_type in ("stock", "all"):
        stock_result = _batch_fetch_stock_quotes(engine, router, codes, start_date, end_date)
        result["stock"] = stock_result

    _write_log(engine, "quote", result)

    return {
        "status": "ok",
        "summary": {
            "etf_updated": result.get("etf", {}).get("updated", 0),
            "etf_rows": result.get("etf", {}).get("rows", 0),
            "stock_updated": result.get("stock", {}).get("updated", 0),
            "stock_rows": result.get("stock", {}).get("rows", 0),
            "duration_s": round(time.time() - _t0, 1),
        },
    }


def _batch_fetch_etf_quotes(
    engine: Engine, router: DataSourceRouter,
    codes: list[str] | None, start: date, end: date,
) -> dict:
    logger.info(f"批量获取ETF行情: {start} ~ {end}")
    config = get_config()
    if codes is None:
        codes = list({e["code"] for e in config.etfs})

    df = router.fetch_etf_daily(codes, start, end)
    if df.empty:
        return {"updated": 0, "rows": 0}

    with Session(engine) as session:
        count = 0
        updated_etfs = set()
        for _, row in df.iterrows():
            td = row["date"]
            if isinstance(td, str):
                td = date.fromisoformat(td)
            code = str(row["code"]).zfill(6)

            etf = session.query(EtfInfo).filter_by(code=code).first()
            if etf is None:
                continue

            close_val = _f(row.get("close"))
            pre_close_val = _get_prev_close_etf(session, etf.id, td)
            pct_chg = _calc_pct_chg(close_val, pre_close_val)

            # UPSERT：记录存在则更新，不存在则插入
            record = session.query(EtfDailyQuote).filter_by(etf_id=etf.id, trade_date=td).first()
            if record:
                record.open = _f(row.get("open"))
                record.high = _f(row.get("high"))
                record.low = _f(row.get("low"))
                record.close = close_val
                record.pre_close = pre_close_val
                record.volume = _f(row.get("volume"))
                record.amount = _f(row.get("amount"))
                record.pct_chg = pct_chg
            else:
                session.add(EtfDailyQuote(
                    etf_id=etf.id,
                    etf_code=code,
                    trade_date=td,
                    open=_f(row.get("open")),
                    high=_f(row.get("high")),
                    low=_f(row.get("low")),
                    close=close_val,
                    pre_close=pre_close_val,
                    volume=_f(row.get("volume")),
                    amount=_f(row.get("amount")),
                    pct_chg=pct_chg,
                ))
            count += 1
            updated_etfs.add(code)

        session.commit()

    return {"updated": len(updated_etfs), "rows": count}


def _batch_fetch_stock_quotes(
    engine: Engine, router: DataSourceRouter,
    codes: list[str] | None, start: date, end: date,
) -> dict:
    logger.info(f"批量获取股票行情: {start} ~ {end}")
    with Session(engine) as session:
        if codes is None:
            rows = (
                session.query(EtfComponent.stock_code)
                .filter(EtfComponent.substitute_flag != "2")
                .distinct()
                .all()
            )
            codes = sorted(set(r[0] for r in rows if r[0]))

    codes = [c for c in (codes or []) if c]
    if not codes:
        return {"updated": 0, "rows": 0}

    # 先去重：只拉 DB 中缺失的记录
    with Session(engine) as session:
        existing = set()
        for r in session.query(DailyQuote.stock_code, DailyQuote.trade_date).filter(
            DailyQuote.trade_date.between(start, end)
        ).all():
            existing.add((r.stock_code, r.trade_date))

    missing = []
    for code in codes:
        cur = start
        while cur <= end:
            if (code, cur) not in existing:
                missing.append(code)
                break
            cur += timedelta(days=1)

    if not missing:
        logger.info(f"股票行情 {start}~{end} 全部已存在，跳过")
        return {"updated": 0, "rows": 0}

    df = router.fetch_stock_daily(missing, start, end)
    if df.empty:
        return {"updated": 0, "rows": 0}

    with Session(engine) as session:
        existing = set()
        for r in session.query(DailyQuote.stock_code, DailyQuote.trade_date).all():
            existing.add((r.stock_code, r.trade_date))

        count = 0
        for _, row in df.iterrows():
            td = row["date"]
            if isinstance(td, str):
                td = date.fromisoformat(td)
            code = str(row["code"]).zfill(6)
            if (code, td) in existing:
                continue

            close_val = _f(row.get("close"))
            pre_val = _get_prev_close_stock(session, code, td)
            pct_chg = _calc_pct_chg(close_val, pre_val)

            session.add(DailyQuote(
                stock_code=code,
                trade_date=td,
                open=_f(row.get("open")),
                high=_f(row.get("high")),
                low=_f(row.get("low")),
                close=close_val,
                pre_close=pre_val,
                volume=_f(row.get("volume")),
                amount=_f(row.get("amount")),
                pct_chg=pct_chg,
                change=close_val - pre_val if close_val is not None and pre_val is not None else None,
                turnover=_f(row.get("turnover")),
            ))
            count += 1

        session.commit()

    return {"updated": 1 if count > 0 else 0, "rows": count}


# ── 定向查询获取（Mode B：返回明细数据） ─────────────────────────


def query_fetch_market_data(
    engine: Engine,
    router: DataSourceRouter,
    asset_type: str,
    codes: list[str],
    start_date: date,
    end_date: date,
) -> dict:
    """
    Mode B 入口。
    获取数据 → 直接返回结构化数据（不进数据库）。
    """
    if asset_type in ("etf", "all"):
        df = router.fetch_etf_daily(codes, start_date, end_date)
    else:
        df = router.fetch_stock_daily(codes, start_date, end_date)

    if df.empty:
        return {"status": "ok", "records": [], "columns": [], "row_count": 0}

    columns = [c for c in df.columns if c != "code"]
    records = []
    for _, row in df.iterrows():
        rec = {c: _safe_val(row.get(c)) for c in columns}
        rec["code"] = str(row.get("code", "")).zfill(6)
        rec["date"] = str(row.get("date", ""))
        records.append(rec)

    return {
        "status": "ok",
        "records": records,
        "columns": ["code", "date"] + columns,
        "row_count": len(records),
    }


# ── PCF ────────────────────────────────────────────────────────────


def batch_fetch_pcf(
    engine: Engine,
    router: DataSourceRouter,
    etf_codes: list[str] | None,
    trade_date: date,
    detect_changes: bool,
    end_date: date | None = None,
) -> dict:
    """批量获取 PCF 成分股快照 + 可选变更检测。

    单日模式：trade_date 为指定日期。
    多日模式：trade_date 为起始日，end_date 为结束日，遍历每个交易日依次获取并留存快照。
    """
    _ensure_etf_info(engine)
    config = get_config()
    if etf_codes is None:
        etf_codes = list({e["code"] for e in config.etfs})

    # 确定交易日列表
    if end_date and end_date > trade_date:
        trade_days = router.trading_days(trade_date, end_date)
        if not trade_days:
            return {"status": "ok", "summary": {"total": 0, "ok": 0, "failed": 0,
                    "total_components": 0, "total_changes": 0}, "details": [],
                    "message": "范围内无交易日"}
        logger.info(f"PCF 多日模式: {trade_days[0]} ~ {trade_days[-1]} 共{len(trade_days)}个交易日")
    else:
        trade_days = [trade_date]

    all_results = []
    grand_total = {"ok": 0, "failed": 0, "components": 0, "changes": 0}

    for day_idx, td in enumerate(trade_days, 1):
        t0 = time.time()
        logger.info(f"  PCF [{day_idx}/{len(trade_days)}] {td}")

        # 第一步：并发拉取
        fetched: list[tuple[str, dict | None]] = []
        def _fetch(code):
            return code, router.fetch_pcf(code, td)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch, code) for code in etf_codes]
            for idx, future in enumerate(as_completed(futures), 1):
                code, result = future.result()
                fetched.append((code, result))

        # 第二步：串行入库
        day_results = []
        for code, result in fetched:
            if result is None:
                day_results.append({"code": code, "status": "failed"})
                continue
            with Session(engine) as session:
                summary = _save_pcf(session, result, td, detect_changes)
            day_results.append(summary)

        day_ok = sum(1 for r in day_results if r.get("status") == "ok")
        day_failed = sum(1 for r in day_results if r.get("status") == "failed")
        day_components = sum(r.get("component_count", 0) for r in day_results)
        day_changes = sum(r.get("changes", 0) for r in day_results)
        grand_total["ok"] += day_ok
        grand_total["failed"] += day_failed
        grand_total["components"] += day_components
        grand_total["changes"] += day_changes
        elapsed = time.time() - t0
        logger.info(f"    → {day_ok} OK, {day_failed} 失败, {day_components} 成分股, {day_changes} 变更 ({elapsed:.0f}s)")
        all_results.append({
            "trade_date": str(td), "ok": day_ok, "failed": day_failed,
            "total_components": day_components, "total_changes": day_changes,
        })

    _write_log(engine, "pcf", grand_total)
    return {
        "status": "ok",
        "summary": {
            "total_days": len(trade_days),
            "total": grand_total["ok"] + grand_total["failed"],
            "ok": grand_total["ok"],
            "failed": grand_total["failed"],
            "total_components": grand_total["components"],
            "total_changes": grand_total["changes"],
        },
        "details": all_results,
    }


def _save_pcf(session: Session, result: dict, trade_date: date, detect_changes: bool) -> dict:
    """保存单只 ETF 的 PCF 数据"""
    etf_code = result["etf_code"]
    etf = session.query(EtfInfo).filter_by(code=etf_code).first()
    if not etf:
        return {"code": etf_code, "status": "skipped", "reason": "not_in_db"}

    components = result["components"]
    existing = set(
        r[0] for r in session.query(EtfComponent.stock_code)
        .filter(EtfComponent.etf_id == etf.id, EtfComponent.trade_date == trade_date,
                EtfComponent.source == "pcf").all()
    )

    new_count = 0
    for comp in components:
        if comp["stock_code"] in existing:
            continue
        session.add(EtfComponent(
            etf_id=etf.id, etf_code=etf_code,
            stock_code=comp["stock_code"],
            stock_name=comp.get("stock_name", ""),
            quantity=comp.get("quantity"),
            substitute_flag=comp.get("substitute_flag"),
            trade_date=trade_date, source="pcf",
        ))
        new_count += 1

    if new_count:
        session.commit()

    # 变更检测
    changes = []
    if detect_changes:
        changes = _detect_changes(session, etf.id, trade_date, components)
        for ch in changes:
            ch["etf_code"] = etf_code
            session.add(ComponentChange(**ch))
        if changes:
            session.commit()

    # 净值：UPSERT，记录存在则更新 NAV，不存在则插入
    nav = result.get("nav")
    nav_per_cu = result.get("nav_per_cu")
    if nav is not None or nav_per_cu is not None:
        record = session.query(EtfDailyQuote).filter(
            EtfDailyQuote.etf_id == etf.id,
            EtfDailyQuote.trade_date == trade_date,
        ).first()
        if record:
            if nav is not None:
                record.nav = nav
            if nav_per_cu is not None:
                record.nav_per_cu = nav_per_cu
        else:
            session.add(EtfDailyQuote(
                etf_id=etf.id, trade_date=trade_date,
                nav=nav, nav_per_cu=nav_per_cu,
            ))
        session.commit()

    return {
        "code": etf_code, "name": etf.name, "status": "ok",
        "component_count": len(components),
        "new_count": new_count, "changes": len(changes),
    }


def _detect_changes(session: Session, etf_id: int, trade_date: date,
                    new_components: list[dict]) -> list[dict]:
    """对比上交易日成分股，返回变更列表"""
    prev_date = session.query(EtfComponent.trade_date).filter(
        EtfComponent.etf_id == etf_id,
        EtfComponent.trade_date < trade_date,
    ).order_by(EtfComponent.trade_date.desc()).first()
    if not prev_date:
        return []
    prev_date = prev_date[0]

    prev_rows = session.query(EtfComponent).filter(
        EtfComponent.etf_id == etf_id,
        EtfComponent.trade_date == prev_date,
        EtfComponent.substitute_flag != "2",
    ).all()
    prev_map = {r.stock_code: r for r in prev_rows}

    new_map = {c["stock_code"]: c for c in new_components if c.get("substitute_flag") != "2"}
    prev_codes = set(prev_map.keys())
    new_codes = set(new_map.keys())

    changes = []
    for code in new_codes - prev_codes:
        changes.append({"etf_id": etf_id, "trade_date": trade_date,
                        "stock_code": code, "stock_name": new_map[code].get("stock_name", ""),
                        "change_type": "added", "old_quantity": None,
                        "new_quantity": new_map[code].get("quantity")})
    for code in prev_codes - new_codes:
        changes.append({"etf_id": etf_id, "trade_date": trade_date,
                        "stock_code": code, "stock_name": prev_map[code].stock_name,
                        "change_type": "removed", "old_quantity": prev_map[code].quantity,
                        "new_quantity": None})
    for code in prev_codes & new_codes:
        old_q = prev_map[code].quantity
        new_q = new_map[code].get("quantity")
        if old_q != new_q:
            changes.append({"etf_id": etf_id, "trade_date": trade_date,
                            "stock_code": code, "stock_name": new_map[code].get("stock_name", ""),
                            "change_type": "quantity_changed",
                            "old_quantity": old_q, "new_quantity": new_q})
    return changes


# ── 辅助函数 ──────────────────────────────────────────────────────


def _f(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_val(val):
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, (date, datetime)):
        return str(val)
    return val


def _get_prev_close_etf(session: Session, etf_id: int, td: date) -> float | None:
    """获取 ETF 前收盘价（只查更早数据，不修改已有记录）"""
    prev = session.query(EtfDailyQuote.close).filter(
        EtfDailyQuote.etf_id == etf_id,
        EtfDailyQuote.trade_date < td,
        EtfDailyQuote.close.isnot(None),
    ).order_by(EtfDailyQuote.trade_date.desc()).first()
    return prev[0] if prev else None


def _get_prev_close_stock(session: Session, code: str, td: date) -> float | None:
    """获取股票前收盘价（只查更早数据，不修改已有记录）"""
    prev = session.query(DailyQuote.close).filter(
        DailyQuote.stock_code == code,
        DailyQuote.trade_date < td,
        DailyQuote.close.isnot(None),
    ).order_by(DailyQuote.trade_date.desc()).first()
    return prev[0] if prev else None


def _calc_pct_chg(close: float | None, pre_close: float | None) -> float | None:
    if close is not None and pre_close is not None and pre_close != 0:
        return round((close - pre_close) / pre_close * 100, 2)
    return None


def _write_log(engine: Engine, update_type: str, detail: dict):
    """写入 UpdateLog"""
    import json
    with Session(engine) as session:
        session.add(UpdateLog(
            update_type=update_type,
            started_at=datetime.now(),
            finished_at=datetime.now(),
            status="ok",
            detail=json.dumps(detail, ensure_ascii=False, default=str),
        ))
        session.commit()
