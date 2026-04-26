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

    # 后处理：回填缺失的 pre_close / pct_chg / change
    backfill_result = backfill_quote_fields(engine)
    # 后处理：计算成分股权重
    weight_result = backfill_component_weights(engine)

    _write_log(engine, "quote", result)

    return {
        "status": "ok",
        "summary": {
            "etf_updated": result.get("etf", {}).get("updated", 0),
            "etf_rows": result.get("etf", {}).get("rows", 0),
            "stock_updated": result.get("stock", {}).get("updated", 0),
            "stock_rows": result.get("stock", {}).get("rows", 0),
            "backfill_fixed": sum(backfill_result.values()),
            "weights_calculated": sum(weight_result.values()),
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
            change_val = close_val - pre_close_val if close_val is not None and pre_close_val is not None else None

            # UPSERT：记录存在则更新，不存在则插入
            record = session.query(EtfDailyQuote).filter_by(etf_id=etf.id, trade_date=td).first()
            if record:
                record.etf_code = code
                record.open = _f(row.get("open"))
                record.high = _f(row.get("high"))
                record.low = _f(row.get("low"))
                record.close = close_val
                record.pre_close = pre_close_val
                record.volume = _f(row.get("volume"))
                record.amount = _f(row.get("amount"))
                record.pct_chg = pct_chg
                record.change = change_val
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
                    change=change_val,
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

    # 查已有记录，判断哪些 code 需要拉：缺失日期 or 最后交易日 pct_chg 为 NULL
    with Session(engine) as session:
        existing = set()
        incomplete = set()
        for r in session.query(DailyQuote).filter(
            DailyQuote.trade_date.between(start, end)
        ).all():
            existing.add((r.stock_code, r.trade_date))
            # 只检查最后一天的数据是否完整，避免首日无 pre_close 误判
            if r.trade_date == end and r.pct_chg is None:
                incomplete.add(r.stock_code)

    missing = []
    for code in codes:
        if code in incomplete:
            missing.append(code)
            continue
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

    # 按 date ASC 排序，确保 pre_close 逐步可查
    df = df.sort_values("date")

    with Session(engine) as session:
        count = 0
        for _, row in df.iterrows():
            td = row["date"]
            if isinstance(td, str):
                td = date.fromisoformat(td)
            code = str(row["code"]).zfill(6)

            close_val = _f(row.get("close"))
            pre_val = _get_prev_close_stock(session, code, td)
            pct_chg = _calc_pct_chg(close_val, pre_val)

            # UPSERT：已存在则更新缺失字段，不存在则插入
            record = session.query(DailyQuote).filter_by(
                stock_code=code, trade_date=td
            ).first()
            if record:
                if record.pct_chg is None:
                    record.open = _f(row.get("open"))
                    record.high = _f(row.get("high"))
                    record.low = _f(row.get("low"))
                    record.close = close_val
                    record.pre_close = pre_val
                    record.volume = _f(row.get("volume"))
                    record.amount = _f(row.get("amount"))
                    record.pct_chg = pct_chg
                    record.change = close_val - pre_val if close_val is not None and pre_val is not None else None
                    count += 1
            else:
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
                etf_code=etf_code,
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


def backfill_quote_fields(engine: Engine) -> dict:
    """回填行情衍生字段：pre_close / pct_chg / change。

    扫描 DailyQuote 和 EtfDailyQuote 中为 NULL 的字段，
    用已有数据计算填入。只填 NULL，不覆盖现有值。
    幂等，可安全重复运行。
    """
    logger.info("开始回填行情衍生字段...")
    t0 = time.time()
    result = {"stock": 0, "etf": 0}

    with Session(engine) as session:
        # ── DailyQuote: pre_close ──
        rows = session.query(DailyQuote).filter(
            DailyQuote.pre_close.is_(None)
        ).order_by(DailyQuote.stock_code, DailyQuote.trade_date).all()

        last_close: dict[str, float | None] = {}
        for row in rows:
            code = row.stock_code
            if code not in last_close:
                prev = session.query(DailyQuote.close).filter(
                    DailyQuote.stock_code == code,
                    DailyQuote.trade_date < row.trade_date,
                    DailyQuote.close.isnot(None),
                ).order_by(DailyQuote.trade_date.desc()).first()
                last_close[code] = prev[0] if prev else None

            if last_close[code] is not None:
                row.pre_close = last_close[code]
                result["stock"] += 1

            if row.close is not None:
                last_close[code] = row.close
        session.commit()

        # ── DailyQuote: pct_chg / change ──
        rows = session.query(DailyQuote).filter(
            DailyQuote.pct_chg.is_(None),
            DailyQuote.close.isnot(None),
            DailyQuote.pre_close.isnot(None),
        ).all()
        for row in rows:
            row.pct_chg = _calc_pct_chg(row.close, row.pre_close)
            row.change = round(row.close - row.pre_close, 4)
            result["stock"] += 1
        session.commit()

        # ── EtfDailyQuote: pre_close ──
        erows = session.query(EtfDailyQuote).filter(
            EtfDailyQuote.pre_close.is_(None)
        ).order_by(EtfDailyQuote.etf_id, EtfDailyQuote.trade_date).all()

        last_close_etf: dict[int, float | None] = {}
        for row in erows:
            eid = row.etf_id
            if eid not in last_close_etf:
                prev = session.query(EtfDailyQuote.close).filter(
                    EtfDailyQuote.etf_id == eid,
                    EtfDailyQuote.trade_date < row.trade_date,
                    EtfDailyQuote.close.isnot(None),
                ).order_by(EtfDailyQuote.trade_date.desc()).first()
                last_close_etf[eid] = prev[0] if prev else None

            if last_close_etf[eid] is not None:
                row.pre_close = last_close_etf[eid]
                result["etf"] += 1

            if row.close is not None:
                last_close_etf[eid] = row.close
        session.commit()

        # ── EtfDailyQuote: pct_chg / change ──
        erows = session.query(EtfDailyQuote).filter(
            EtfDailyQuote.pct_chg.is_(None),
            EtfDailyQuote.close.isnot(None),
            EtfDailyQuote.pre_close.isnot(None),
        ).all()
        for row in erows:
            row.pct_chg = _calc_pct_chg(row.close, row.pre_close)
            row.change = round(row.close - row.pre_close, 4)
            result["etf"] += 1
        session.commit()

    elapsed = time.time() - t0
    total = sum(result.values())
    logger.info(f"行情回填完成: {total} 条 ({elapsed:.1f}s)")
    return result


def backfill_component_changes(
    engine: Engine,
    etf_codes: list[str] | None = None,
) -> dict:
    """从现有 PCF 数据回填缺失的 ComponentChange 记录。

    遍历每只 ETF 的所有 PCF 快照日期（升序），
    逐对连续日期比较，记录成分股变更。
    若某 (etf_id, trade_date) 已存在变更记录则跳过。
    幂等，可安全重复运行。
    """
    logger.info("开始回填成分股变更...")
    t0 = time.time()

    config = get_config()
    if etf_codes is None:
        etf_codes = list({e["code"] for e in config.etfs})

    total_new = 0
    total_skipped = 0

    with Session(engine) as session:
        for code in etf_codes:
            etf = session.query(EtfInfo).filter_by(code=code).first()
            if not etf:
                continue

            dates_r = session.query(EtfComponent.trade_date).filter(
                EtfComponent.etf_id == etf.id,
                EtfComponent.source == "pcf",
            ).distinct().order_by(EtfComponent.trade_date.asc()).all()

            dates = [r[0] for r in dates_r]
            if len(dates) < 2:
                continue

            for i in range(1, len(dates)):
                prev_td, curr_td = dates[i - 1], dates[i]

                existing = session.query(ComponentChange).filter(
                    ComponentChange.etf_id == etf.id,
                    ComponentChange.trade_date == curr_td,
                ).first()
                if existing:
                    total_skipped += 1
                    continue

                prev_rows = session.query(EtfComponent).filter(
                    EtfComponent.etf_id == etf.id,
                    EtfComponent.trade_date == prev_td,
                    EtfComponent.substitute_flag != "2",
                ).all()
                curr_rows = session.query(EtfComponent).filter(
                    EtfComponent.etf_id == etf.id,
                    EtfComponent.trade_date == curr_td,
                    EtfComponent.substitute_flag != "2",
                ).all()
                if not prev_rows or not curr_rows:
                    continue

                prev_map = {r.stock_code: r for r in prev_rows}
                curr_map = {r.stock_code: r for r in curr_rows}
                prev_codes = set(prev_map.keys())
                curr_codes = set(curr_map.keys())

                changes = []
                for sc in curr_codes - prev_codes:
                    c = curr_map[sc]
                    changes.append(ComponentChange(
                        etf_id=etf.id, etf_code=code, trade_date=curr_td,
                        stock_code=sc, stock_name=c.stock_name,
                        change_type="added",
                        old_quantity=None, new_quantity=c.quantity,
                    ))
                for sc in prev_codes - curr_codes:
                    p = prev_map[sc]
                    changes.append(ComponentChange(
                        etf_id=etf.id, etf_code=code, trade_date=curr_td,
                        stock_code=sc, stock_name=p.stock_name,
                        change_type="removed",
                        old_quantity=p.quantity, new_quantity=None,
                    ))
                for sc in prev_codes & curr_codes:
                    p, c = prev_map[sc], curr_map[sc]
                    if p.quantity != c.quantity:
                        changes.append(ComponentChange(
                            etf_id=etf.id, etf_code=code, trade_date=curr_td,
                            stock_code=sc, stock_name=c.stock_name,
                            change_type="quantity_changed",
                            old_quantity=p.quantity, new_quantity=c.quantity,
                        ))

                for ch in changes:
                    session.add(ch)
                total_new += len(changes)

            session.commit()

    elapsed = time.time() - t0
    logger.info(f"成分股变更回填完成: {total_new} 新增, {total_skipped} 跳过 ({elapsed:.1f}s)")
    return {"new_changes": total_new, "skipped_dates": total_skipped, "duration_s": round(elapsed, 1)}


def backfill_component_weights(engine: Engine) -> dict:
    """计算所有 ETF 成分股权重。

    对每只 ETF 每个交易日，用 PCF 数量 × 当日收盘价 计算：

        weight_i = quantity_i × close_i / Σ(quantity_j × close_j) × 100

    只填 NULL 或重新计算全量。幂等。
    """
    logger.info("开始回填成分股权重...")
    t0 = time.time()

    with Session(engine) as session:
        # 获取所有有 quantity 的成分股记录
        rows = session.query(EtfComponent).filter(
            EtfComponent.quantity.isnot(None),
        ).all()

        # 按 (etf_id, trade_date) 分组
        groups: dict[tuple[int, date], list[EtfComponent]] = {}
        for r in rows:
            groups.setdefault((r.etf_id, r.trade_date), []).append(r)

        total_updated = 0
        total_skipped = 0

        for (etf_id, td), comps in groups.items():
            # 查每只成分股的收盘价
            codes = [c.stock_code for c in comps]
            quotes = {
                r.stock_code: r.close
                for r in session.query(DailyQuote).filter(
                    DailyQuote.stock_code.in_(codes),
                    DailyQuote.trade_date == td,
                    DailyQuote.close.isnot(None),
                ).all()
            }

            # 计算篮子总价值
            basket_values = []
            for c in comps:
                close = quotes.get(c.stock_code)
                if close is not None and c.quantity is not None:
                    basket_values.append(c.quantity * close)
                else:
                    basket_values.append(0.0)

            total_value = sum(basket_values)
            if total_value == 0:
                total_skipped += len(comps)
                continue

            for c, bv in zip(comps, basket_values):
                weight = round(bv / total_value * 100, 4) if bv > 0 else 0.0
                c.weight = weight
                total_updated += 1

        session.commit()

    elapsed = time.time() - t0
    logger.info(f"成分股权重回填完成: {total_updated} 更新, {total_skipped} 跳过 ({elapsed:.1f}s)")
    return {"updated": total_updated, "skipped": total_skipped, "duration_s": round(elapsed, 1)}
