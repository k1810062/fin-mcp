"""
数据源：akshare 实现。
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import akshare as ak
import pandas as pd

from .base import DataSource


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
        prog_step = max(1, total // 10)  # 每10%打一次日志

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
                    print(f"\r  ETF进度: {idx}/{total} ({pct:.0f}%) {elapsed:.0f}s", end="", flush=True)

        if total > prog_step:
            print()
        if not all_rows:
            return pd.DataFrame()
        result = pd.concat(all_rows, ignore_index=True)
        # 统一列名
        cols = {
            "open": "open", "high": "high", "low": "low",
            "close": "close", "volume": "volume", "amount": "amount",
            "pct_chg": "pct_chg",
        }
        keep = {"code", "date"} | set(cols.keys())
        for c in keep:
            if c not in result.columns:
                result[c] = None
        return result[list(keep)]

    def fetch_stock_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        total = len(codes)
        if total == 0:
            return pd.DataFrame()
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        all_rows = []
        t0 = time.time()
        prog_step = max(1, total // 20)  # 每5%打一次日志

        def _fetch_one(code):
            symbol = _to_symbol(code)
            try:
                df = ak.stock_zh_a_daily(
                    symbol=symbol, start_date=start_str,
                    end_date=end_str, adjust="qfq",
                )
            except Exception:
                return None
            if df is None or df.empty:
                return None
            df["code"] = code
            return df

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_one, code) for code in codes]
            for idx, future in enumerate(as_completed(futures), 1):
                df = future.result()
                if df is not None and not df.empty:
                    all_rows.append(df)
                if idx % prog_step == 0 or idx == total:
                    pct = idx / total * 100
                    elapsed = time.time() - t0
                    eta = elapsed / idx * (total - idx) if idx else 0
                    print(f"\r  股票进度: {idx}/{total} ({pct:.0f}%) {elapsed:.0f}s 剩余~{eta:.0f}s", end="", flush=True)

        if total > prog_step:
            print()
        if not all_rows:
            return pd.DataFrame()
        result = pd.concat(all_rows, ignore_index=True)
        result["date"] = pd.to_datetime(result["date"]).dt.date
        cols = {
            "open": "open", "high": "high", "low": "low",
            "close": "close", "volume": "volume", "amount": "amount",
            "pct_chg": "pct_chg", "pre_close": "pre_close",
            "turnover": "turnover",
        }
        keep = {"code", "date"} | set(cols.keys())
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
