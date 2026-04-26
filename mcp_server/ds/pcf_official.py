"""
数据源：交易所官方 PCF 接口。
单独拆出来因为 PCF 获取逻辑和日线行情完全不同。
"""
from __future__ import annotations

import ssl
import xml.etree.ElementTree as ET
import urllib.request
from datetime import date
from typing import Optional

from .base import DataSource
import pandas as pd


_ctx = ssl.create_default_context()
_opener = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=_ctx)
)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def _is_sse(code: str) -> bool:
    return code.startswith(("51", "58", "588", "530", "563"))


def _is_szse(code: str) -> bool:
    return code.startswith(("159", "16"))


def _request(url: str, referer: str = "https://www.szse.cn/") -> Optional[str]:
    headers = dict(_HEADERS)
    headers["Referer"] = referer
    req = urllib.request.Request(url, headers=headers)
    try:
        with _opener.open(req, timeout=30) as r:
            return r.read().decode("utf-8")
    except Exception:
        return None


def _elem_text(parent, tag: str, ns: Optional[dict] = None):
    ele = parent.find(tag, ns) if ns else parent.find(tag)
    return ele.text.strip() if ele is not None and ele.text else ""


def _float_or_none(parent, tag: str, ns: Optional[dict] = None):
    ele = parent.find(tag, ns) if ns else parent.find(tag)
    if ele is not None and ele.text:
        try:
            return float(ele.text.strip())
        except ValueError:
            return None
    return None


def _parse_sse(xml_text: str) -> Optional[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    comp_list = root.find("ComponentList")
    if comp_list is None:
        return None
    components = []
    for comp in comp_list.findall("Component"):
        components.append({
            "stock_code": _elem_text(comp, "InstrumentID"),
            "stock_name": _elem_text(comp, "InstrumentName"),
            "quantity": _float_or_none(comp, "Quantity"),
            "substitute_flag": _elem_text(comp, "SubstitutionFlag"),
        })
    return {
        "etf_code": _elem_text(root, "FundInstrumentID"),
        "trading_day": _elem_text(root, "TradingDay"),
        "nav": _float_or_none(root, "NAV"),
        "nav_per_cu": _float_or_none(root, "NAVperCU"),
        "components": components,
    }


def _parse_szse(xml_text: str) -> Optional[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    ns = {"ns": "http://ts.szse.cn/Fund"}
    comps_node = root.find("ns:Components", ns)
    if comps_node is None:
        return None
    components = []
    for comp in comps_node.findall("ns:Component", ns):
        components.append({
            "stock_code": _elem_text(comp, "ns:UnderlyingSecurityID", ns),
            "stock_name": _elem_text(comp, "ns:UnderlyingSymbol", ns),
            "quantity": _float_or_none(comp, "ns:ComponentShare", ns),
            "substitute_flag": _elem_text(comp, "ns:SubstituteFlag", ns),
        })
    return {
        "etf_code": _elem_text(root, "ns:SecurityID", ns),
        "trading_day": _elem_text(root, "ns:TradingDay", ns),
        "nav": _float_or_none(root, "ns:NAV", ns),
        "nav_per_cu": _float_or_none(root, "ns:NAVperCU", ns),
        "components": components,
    }


class PCFOfficialSource(DataSource):
    """交易所官方 PCF 数据源"""

    def name(self) -> str:
        return "pcf_official"

    def fetch_etf_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_stock_daily(self, codes: list[str], start: date, end: date) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_pcf(self, etf_code: str, trade_date: date) -> dict | None:
        date_str = trade_date.strftime("%Y%m%d")
        if _is_sse(etf_code):
            url = f"https://query.sse.com.cn/etfDownload/downloadETF2Bulletin.do?fundCode={etf_code}"
            xml = _request(url, referer="https://query.sse.com.cn/")
            if not xml:
                return None
            return _parse_sse(xml)
        elif _is_szse(etf_code):
            url = f"https://reportdocs.static.szse.cn/files/text/ETFDown/pcf_{etf_code}_{date_str}.xml"
            xml = _request(url)
            if not xml:
                return None
            return _parse_szse(xml)
        return None

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5

    def trading_days(self, start: date, end: date) -> list[date]:
        return [d for i in range((end - start).days + 1)
                if (d := date.fromordinal(start.toordinal() + i)).weekday() < 5]
