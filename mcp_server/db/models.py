"""
SQLAlchemy 模型定义
支持 SQLite ↔ PostgreSQL 无痛切换
"""
from datetime import date, datetime

from sqlalchemy import (
    Column, Date, DateTime, Float, ForeignKey, Index,
    Integer, String, Text, UniqueConstraint, create_engine, func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Industry(Base):
    """行业分类"""
    __tablename__ = "industries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry_name = Column(String(50), unique=True, nullable=False, comment="行业名称")

    etf_links = relationship("EtfIndustryLink", lazy="selectin")


class EtfIndustryLink(Base):
    """ETF ⇄ 行业 多对多"""
    __tablename__ = "etf_industry_links"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etf_id = Column(Integer, ForeignKey("etf_info.id"), nullable=False)
    industry_id = Column(Integer, ForeignKey("industries.id"), nullable=False)

    industry = relationship("Industry", lazy="selectin", overlaps="etf_links")

    __table_args__ = (UniqueConstraint("etf_id", "industry_id", name="uq_etf_industry"),)


class EtfInfo(Base):
    """ETF 基本信息"""
    __tablename__ = "etf_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True, comment="ETF代码")
    name = Column(String(100), nullable=False, comment="ETF名称")

    industry_links = relationship("EtfIndustryLink", lazy="selectin")
    components = relationship("EtfComponent", back_populates="etf")
    daily_quotes = relationship("EtfDailyQuote", back_populates="etf")


class EtfComponent(Base):
    """成分股快照（按天 + 数据源，全历史可追溯）"""
    __tablename__ = "etf_components"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etf_id = Column(Integer, ForeignKey("etf_info.id"), nullable=False)
    etf_code = Column(String(10), nullable=True, comment="ETF代码（冗余提速）")
    stock_code = Column(String(10), nullable=False, comment="成分股代码")
    stock_name = Column(String(50), nullable=False, comment="成分股名称")
    quantity = Column(Float, nullable=True, comment="申赎股数(PCF)")
    substitute_flag = Column(String(2), nullable=True, comment="替代标志: 1=证券 2=现金")
    weight = Column(Float, nullable=True, comment="权重(%)")
    trade_date = Column(Date, nullable=False, comment="快照日期")
    source = Column(String(20), nullable=False, default="pcf", comment="数据来源: pcf/manual")

    etf = relationship("EtfInfo", back_populates="components")

    __table_args__ = (
        Index("idx_comp_etf_date", "etf_id", "trade_date"),
        Index("idx_comp_stock", "stock_code"),
    )


class ComponentChange(Base):
    """成分股变更记录（只追加，不修改）"""
    __tablename__ = "component_changes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etf_id = Column(Integer, ForeignKey("etf_info.id"), nullable=False)
    etf_code = Column(String(10), nullable=True)
    trade_date = Column(Date, nullable=False)
    stock_code = Column(String(10), nullable=False)
    stock_name = Column(String(50), nullable=True)
    change_type = Column(String(20), nullable=False, comment="added/removed/quantity_changed")
    old_quantity = Column(Float, nullable=True)
    new_quantity = Column(Float, nullable=True)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (Index("idx_change_etf_date", "etf_id", "trade_date"),)


class DailyQuote(Base):
    """股票日线行情（只追加，不覆盖，不修改已有记录）"""
    __tablename__ = "daily_quotes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False)
    trade_date = Column(Date, nullable=False)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    pre_close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    pct_chg = Column(Float, nullable=True, comment="涨跌幅(%)")
    change = Column(Float, nullable=True, comment="涨跌额")
    turnover = Column(Float, nullable=True, comment="换手率(%)")

    __table_args__ = (
        UniqueConstraint("stock_code", "trade_date", name="uq_stock_date"),
        Index("idx_dq_stock", "stock_code"),
        Index("idx_dq_date", "trade_date"),
    )


class EtfDailyQuote(Base):
    """ETF 日线行情（只追加，不覆盖，不修改已有记录）"""
    __tablename__ = "etf_daily_quotes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etf_id = Column(Integer, ForeignKey("etf_info.id"), nullable=False)
    etf_code = Column(String(10), nullable=True)
    trade_date = Column(Date, nullable=False)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    pre_close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    pct_chg = Column(Float, nullable=True)
    nav = Column(Float, nullable=True, comment="单位净值")
    nav_per_cu = Column(Float, nullable=True, comment="每百万份净值")

    etf = relationship("EtfInfo", back_populates="daily_quotes")

    __table_args__ = (
        UniqueConstraint("etf_id", "trade_date", name="uq_etf_date"),
        Index("idx_eq_etf", "etf_id"),
        Index("idx_eq_date", "trade_date"),
    )


class UpdateLog(Base):
    """操作日志"""
    __tablename__ = "update_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    update_type = Column(String(20), nullable=False, comment="pcf/quote/manual")
    trade_date = Column(Date, nullable=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="ok")
    detail = Column(Text, nullable=True)

    __table_args__ = (Index("idx_log_type_date", "update_type", "trade_date"),)


def init_db(db_path: str = "sqlite:///fin_data.db"):
    """初始化引擎 + 建表。不存在就新建，存在就复用。"""
    engine = create_engine(db_path, echo=False)
    Base.metadata.create_all(engine)
    return engine
