"""
数据源路由器。
统一出入口，背后自动切换数据源 + fallback。
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import pandas as pd

from .base import DataSource
from .akshare_source import AkshareSource
from .pcf_official import PCFOfficialSource

logger = logging.getLogger(__name__)

_SOURCE_MAP: dict[str, type[DataSource]] = {}


def _register_builtins():
    _SOURCE_MAP["akshare"] = AkshareSource
    _SOURCE_MAP["pcf_official"] = PCFOfficialSource


_register_builtins()


def register_source(name: str, cls: type[DataSource]):
    """注册自定义数据源（扩展用）"""
    _SOURCE_MAP[name] = cls


class DataSourceRouter:
    """统一路由：优选源 → fallback 源 → 自动降级"""

    def __init__(self, preferred: str, fallback: str, pcf_source: str = "pcf_official"):
        self.preferred = self._init_source(preferred)
        self.fallback = self._init_source(fallback) if fallback and fallback != preferred else None
        self.pcf_source = self._init_source(pcf_source)

    def _init_source(self, name: str) -> DataSource:
        cls = _SOURCE_MAP.get(name)
        if not cls:
            raise ValueError(f"未知数据源: {name}，可用: {list(_SOURCE_MAP.keys())}")
        return cls()

    def fetch_etf_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        for src in (self.preferred, self.fallback):
            if src is None:
                continue
            try:
                df = src.fetch_etf_daily(codes, start, end)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"[{src.name()}] ETF日线获取失败: {e}")
        return pd.DataFrame()

    def fetch_stock_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        for src in (self.preferred, self.fallback):
            if src is None:
                continue
            try:
                df = src.fetch_stock_daily(codes, start, end)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"[{src.name()}] 股票日线获取失败: {e}")
        return pd.DataFrame()

    def fetch_pcf(self, etf_code: str, trade_date: date) -> Optional[dict]:
        # PCF 有独立数据源
        try:
            result = self.pcf_source.fetch_pcf(etf_code, trade_date)
            if result:
                return result
        except Exception as e:
            logger.warning(f"[{self.pcf_source.name()}] PCF获取失败: {e}")
        # fallback 到优选源
        for src in (self.preferred, self.fallback):
            if src is None or src.name() == self.pcf_source.name():
                continue
            try:
                result = src.fetch_pcf(etf_code, trade_date)
                if result:
                    return result
            except Exception:
                continue
        return None

    def is_trading_day(self, d: date) -> bool:
        return self.preferred.is_trading_day(d)

    def trading_days(self, start: date, end: date) -> list[date]:
        return self.preferred.trading_days(start, end)
