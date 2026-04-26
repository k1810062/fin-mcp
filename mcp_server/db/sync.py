"""
配置同步：将 config.json 中的 ETF 列表写入 DB（去重）。
启动时调用一次即可，后续 config.json 变更后也可手动调用。
"""
from __future__ import annotations

import logging

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from ..config import AppConfig
from .models import EtfInfo, EtfIndustryLink, Industry

logger = logging.getLogger(__name__)


def sync_etf_config(engine: Engine, config: AppConfig):
    """将 config.etfs 同步到 etf_info / industries / etf_industry_links 表，自动去重。"""
    with Session(engine) as session:
        # 按 code 去重，保留所有行业映射
        etf_map: dict[str, dict] = {}  # code -> {name, industries:set}
        for e in config.etfs:
            code = e["code"]
            if code not in etf_map:
                etf_map[code] = {"code": code, "name": e["name"], "industries": set()}
            etf_map[code]["industries"].add(e.get("industry", ""))
            # 名称可能不同，取最后一次出现的
            etf_map[code]["name"] = e["name"]

        added = 0
        for code, info in etf_map.items():
            # 查找或创建 ETF
            etf = session.query(EtfInfo).filter_by(code=code).first()
            if etf is None:
                etf = EtfInfo(code=code, name=info["name"])
                session.add(etf)
                session.flush()
                added += 1
            else:
                # 更新名称
                etf.name = info["name"]

            # 同步行业分类
            for ind_name in info["industries"]:
                if not ind_name:
                    continue
                ind = session.query(Industry).filter_by(industry_name=ind_name).first()
                if ind is None:
                    ind = Industry(industry_name=ind_name)
                    session.add(ind)
                    session.flush()

                # 检查关联是否已存在
                link = session.query(EtfIndustryLink).filter_by(
                    etf_id=etf.id, industry_id=ind.id,
                ).first()
                if link is None:
                    session.add(EtfIndustryLink(etf_id=etf.id, industry_id=ind.id))

        session.commit()
        logger.info(
            f"ETF 配置同步完成: config={len(config.etfs)}条(去重{len(etf_map)}只), "
            f"新增{added}只"
        )
