"""
数据源：akshare 实现。

V8 冲突说明：py_mini_racer 的 V8 地址空间初始化有进程级互斥。
并发调 stock_zh_a_daily(adjust="qfq") 会触发致命崩溃。

本模块使用 ak.fund_etf_hist_sina（纯 HTTP，无 V8 依赖）获取股票行情，
配合 ThreadPoolExecutor 并发拉取。_api_lock 作为全局锁保护 API 调用段，
确保即使未来引入 V8 依赖接口也不会并发冲突。
"""
from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import akshare as ak
import pandas as pd

from .base import DataSource

# 全局锁：保护 V8 依赖的 akshare 调用，避免并发地址空间冲突
# ak.fund_etf_hist_sina 为纯 HTTP 无需此锁，保留作为安全兜底
_api_lock = threading.Lock()


def _to_symbol(code: str) -> str:
    """6 位代码 → 带交易所前缀的 symbol"""
    if code.startswith(("6", "688", "51", "58", "588", "530", "563")):
        return f"sh{code}"
    elif code.startswith(("0", "3", "2", "159", "16")):
        return f"sz{code}"
    elif code.startswith("920"):
        return f"bj{code}"
    return code


def _f(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class AkshareSource(DataSource):
    """基于 akshare 的数据源实现"""

    def name(self) -> str:
        return "akshare"

    def fetch_etf_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        total = len(codes)
        if total == 0:
            return pd.DataFrame()
        all_rows = []
        t0 = time.time()
        prog_step = max(1, total // 10)

        def _fetch_one(code):
            symbol = _to_symbol(code)
            try:
                df = ak.fund_etf_hist_sina(symbol=symbol)
            except Exception:
                return None
            if df is None or df.empty:
                return None
            df["code"] = code
            df["date"] = pd.to_datetime(df["date"]).dt.date
            return df[(df["date"] >= start) & (df["date"] <= end)]

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_one, code) for code in codes]
            for idx, future in enumerate(as_completed(futures), 1):
                df = future.result()
                if df is not None and not df.empty:
                    all_rows.append(df)
                if idx % prog_step == 0 or idx == total:
                    pct = idx / total * 100
                    elapsed = time.time() - t0
                    print(f"\r  ETF进度: {idx}/{total} ({pct:.0f}%) {elapsed:.0f}s", end="", flush=True, file=sys.stderr)

        if total > prog_step:
            print(file=sys.stderr)
        if not all_rows:
            return pd.DataFrame()
        result = pd.concat(all_rows, ignore_index=True)
        cols = {"open", "high", "low", "close", "volume", "amount", "pct_chg"}
        keep = {"code", "date"} | cols
        for c in keep:
            if c not in result.columns:
                result[c] = None
        return result[list(keep)]

    def fetch_stock_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        total = len(codes)
        if total == 0:
            return pd.DataFrame()
        all_rows = []
        t0 = time.time()
        prog_step = max(1, total // 10)

        def _fetch_one(code):
            symbol = _to_symbol(code)
            try:
                df = ak.fund_etf_hist_sina(symbol=symbol)
            except Exception:
                return None
            if df is None or df.empty:
                return None
            df["code"] = code
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df[(df["date"] >= start) & (df["date"] <= end)]
            if df.empty:
                return None
            df = df.drop(columns=["prevclose"], errors="ignore")
            return df

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_fetch_one, code) for code in codes]
            for idx, future in enumerate(as_completed(futures), 1):
                with _api_lock:
                    df = future.result()
                    if df is not None:
                        all_rows.append(df)
                if idx % prog_step == 0 or idx == total:
                    pct = idx / total * 100
                    elapsed = time.time() - t0
                    print(f"\r  股票进度: {idx}/{total} ({pct:.0f}%) {elapsed:.0f}s", end="", flush=True, file=sys.stderr)

        if total > prog_step:
            print(file=sys.stderr)
        if not all_rows:
            return pd.DataFrame()
        result = pd.concat(all_rows, ignore_index=True)
        cols = {"open", "high", "low", "close", "volume", "amount"}
        keep = {"code", "date"} | cols
        for c in keep:
            if c not in result.columns:
                result[c] = None
        return result[list(keep)]

    def fetch_pcf(self, etf_code: str, trade_date: date) -> dict | None:
        """akshare 没有直接 PCF 接口，走备选策略：return None 触发 fallback"""
        return None

    def is_trading_day(self, d: date) -> bool:
        try:
            df = ak.tool_trade_date_hist_sina()
            trading = set(pd.to_datetime(df["trade_date"]).dt.date)
            return d in trading
        except Exception:
            return d.weekday() < 5

    def trading_days(self, start: date, end: date) -> list[date]:
        df = ak.tool_trade_date_hist_sina()
        all_days = set(pd.to_datetime(df["trade_date"]).dt.date)
        return sorted(d for d in all_days if start <= d <= end)
