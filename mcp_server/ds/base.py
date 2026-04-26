"""
数据源抽象基类。
所有数据源（akshare、新浪直连、交易所官方等）必须实现此接口。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class DataSource(ABC):
    """数据源统一接口"""

    @abstractmethod
    def name(self) -> str:
        """数据源唯一标识，如 'akshare'、'sina'、'pcf_official'"""
        ...

    # ── 日线行情 ─────────────────────────────────────────────

    @abstractmethod
    def fetch_etf_daily(
        self, codes: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """获取 ETF 日线行情。
        返回列: code, date, open, high, low, close, pre_close, volume, amount, pct_chg
        """
        ...

    @abstractmethod
    def fetch_stock_daily(
        self, codes: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """获取股票日线行情。
        返回列: code, date, open, high, low, close, pre_close, volume, amount, pct_chg
        """
        ...

    # ── PCF ──────────────────────────────────────────────────

    @abstractmethod
    def fetch_pcf(self, etf_code: str, trade_date: date) -> dict | None:
        """获取 PCF 申购赎回清单。
        返回: { etf_code, trading_day, nav, nav_per_cu, components: [{stock_code, stock_name, quantity, substitute_flag}] }
        """
        ...

    # ── 工具 ──────────────────────────────────────────────────

    @abstractmethod
    def is_trading_day(self, d: date) -> bool:
        """判断是否为交易日"""
        ...

    @abstractmethod
    def trading_days(self, start: date, end: date) -> list[date]:
        """获取指定范围内的交易日列表"""
        ...
