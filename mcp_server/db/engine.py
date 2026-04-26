"""
数据库引擎管理。
SQLite ↔ PostgreSQL 无缝切换：改连接串即可，ORM 代码无需变动。
"""
from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import Engine, event
from sqlalchemy.engine import Engine as SAEngine

from .models import Base, init_db as _init_db

_engine: Engine | None = None


def get_engine(db_url: str | None = None) -> Engine:
    """
    获取数据库引擎（单例）。
    未传 db_url 时从环境变量 DB_URL 读取，仍为空则用默认 SQLite 路径。
    """
    global _engine
    if _engine is not None:
        return _engine

    url = db_url or os.environ.get("DB_URL")
    if not url:
        project_root = os.environ.get("PROJECT_ROOT", str(Path.cwd()))
        db_path = Path(project_root) / "fin_data.db"
        url = f"sqlite:///{db_path}"

    _engine = _init_db(url)

    # SQLite 专用：启用 WAL + 外键
    if url.startswith("sqlite"):
        _enable_sqlite_compat(_engine)

    return _engine


def _enable_sqlite_compat(engine: SAEngine):
    """SQLite 优化参数，不影响 PostgreSQL"""

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def close_engine():
    """关闭引擎（一般不需要手动调用）"""
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None
