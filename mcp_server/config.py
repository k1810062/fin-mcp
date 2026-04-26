"""
配置模块。
全局只读配置对象，所有模块共用。支持环境变量覆盖。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class DataSourceConfig:
    preferred: str = "akshare"
    fallback: str = "akshare"
    pcf_source: str = "pcf_official"


@dataclass
class AutomationConfig:
    enabled: bool = False
    auto_update_pcf: bool = False
    auto_update_quotes: bool = False
    auto_detect_changes: bool = False
    schedule_pcf_time: str = "08:35"
    schedule_quote_time: str = "15:35"


@dataclass
class AppConfig:
    project_root: str = ""
    db_url: str = ""

    etfs: list[dict] = field(default_factory=list)
    data_sources: DataSourceConfig = field(default_factory=DataSourceConfig)
    automation: AutomationConfig = field(default_factory=AutomationConfig)


_CONFIG: Optional[AppConfig] = None


def load_config(project_root: str | None = None) -> AppConfig:
    """
    加载配置。
    优先级：环境变量 > config.json > 默认值
    """
    root = project_root or os.environ.get("PROJECT_ROOT", str(Path.cwd()))

    config = AppConfig(project_root=root)
    config.db_url = os.environ.get("DB_URL", f"sqlite:///{Path(root) / 'fin_data.db'}")

    # 加载 config.json
    cfg_path = Path(root) / "config.json"
    if cfg_path.exists():
        with open(cfg_path, encoding="utf-8") as f:
            raw = json.load(f)

        config.etfs = raw.get("etfs", [])

        ds = raw.get("data_sources", {})
        config.data_sources.preferred = ds.get("preferred", "akshare")
        config.data_sources.fallback = ds.get("fallback", "akshare")
        config.data_sources.pcf_source = ds.get("pcf", "pcf_official")

        auto = raw.get("automation", {})
        config.automation.enabled = auto.get("enabled", False)
        config.automation.auto_update_pcf = auto.get("auto_update_pcf", False)
        config.automation.auto_update_quotes = auto.get("auto_update_quotes", False)
        config.automation.auto_detect_changes = auto.get("auto_detect_changes", False)
        config.automation.schedule_pcf_time = auto.get("schedule", {}).get("pcf_time", "08:35")
        config.automation.schedule_quote_time = auto.get("schedule", {}).get("quote_time", "15:35")

    # 环境变量可覆盖数据源选择
    if "DS_PREFERRED" in os.environ:
        config.data_sources.preferred = os.environ["DS_PREFERRED"]
    if "DS_FALLBACK" in os.environ:
        config.data_sources.fallback = os.environ["DS_FALLBACK"]

    return config


def get_config() -> AppConfig:
    """获取全局配置（懒加载单例）"""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG


def reload_config():
    """重新加载配置（调试用）"""
    global _CONFIG
    _CONFIG = load_config()
    return _CONFIG
