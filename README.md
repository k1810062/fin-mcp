# fin-mcp

A股金融数据 MCP Server。通过 MCP 协议提供 ETF 和股票的行情、成分股、PCF 等数据查询与更新能力，支持 Claude Code 原生调用。

## 架构

```
Claude Code → MCP 协议 → fin-data server → akshare / 交易所 PCF → SQLite
```

- **MCP Server**: `mcp_server/server.py`，注册为 `.mcp.json` 中的 `fin-data` 服务
- **数据源**: akshare（日线行情）+ 交易所官方 PCF（成分股快照）
- **存储**: SQLite（`fin_data.db`），自动建表
- **配置**: `config.json` 动态管理 ETF 列表和行业映射

## 快速开始

### 依赖

```bash
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

### 启动

MCP Server 通过 `.mcp.json` 注册，Claude Code 自动管理生命周期。在项目目录下启动 Claude Code 即可使用。

## 配置

编辑 `config.json`，`etfs` 列表管理所有 ETF 标的：

```json
{
  "etfs": [
    { "industry": "半导体", "code": "512480", "name": "国联安半导体ETF" },
    { "industry": "医药", "code": "512010", "name": "易方达医药ETF" }
  ]
}
```

每个条目包含行业、代码、名称。一个代码可映射到多个行业（如 `159825` 同时属于农业/猪肉/种业）。

## MCP 工具

### 数据获取

| 工具 | 说明 |
|------|------|
| `fetch.market_data` | 获取 ETF/股票日线行情。Mode A 批量入库，Mode B 返回明细 |
| `fetch.pcf_components` | 获取 PCF 成分股快照。支持单日/多日模式 |

### 查询

| 工具 | 说明 |
|------|------|
| `query.summary` | 涨跌幅排名，支持行业筛选 |
| `query.daily_quotes` | 查询行情数据 |
| `query.components` | 查询成分股（含权重） |
| `query.changes` | 成分股持仓变更历史 |
| `query.basket` | 篮子价值计算（成分股×收盘价 vs 净值） |
| `query.etf_info` | ETF 基本信息 |

### 运维

| 工具 | 说明 |
|------|------|
| `maintenance.stats` | 数据库统计概览 |
| `maintenance.check` | 数据完整性检查 |
| `maintenance.backfill_quotes` | 回填缺失的 pre_close/pct_chg/change |
| `maintenance.backfill_changes` | 补全成分股变更记录（幂等） |
| `maintenance.backfill_weights` | 计算成分股权重（幂等） |

### 其他

| 工具 | 说明 |
|------|------|
| `components.manage` | 自定义成分股管理 |
| `export.to_excel` | 导出 Excel 报告 |

## 数据模型

```
etf_info          — ETF 基本信息（代码、名称）
industries        — 行业分类
etf_industry_links — ETF ⇄ 行业 多对多关联
daily_quotes      — 股票日线行情（只追加）
etf_daily_quotes  — ETF 日线行情（含 NAV）
etf_components    — 成分股快照（按天 + 数据源，全历史可追溯）
component_changes — 成分股变更记录
update_logs       — 操作日志
```

## 数据更新流程

```
1. fetch.pcf_components(mode="batch", trade_date=start, end_date=end)
   → 每个交易日 PCF 快照 + 变更检测

2. fetch.market_data(asset_type="all", mode="batch", ...)
   → 股票 + ETF 行情入库
   → 自动回填 pre_close/pct_chg/change
   → 自动计算成分股权重

3. maintenance.backfill_changes
   → 补全遗漏的变更记录（可选）

4. maintenance.backfill_weights
   → 确保权重已计算（可选）
```

所有操作幂等，可安全重复运行。

## 项目结构

```
fin-mcp/
├── .mcp.json                    # MCP 服务注册
├── .claude/skills/etf_ds/       # Claude Code 技能
├── config.json                  # ETF 配置
├── mcp_server/
│   ├── server.py                # MCP 入口
│   ├── config.py                # 配置加载
│   ├── core/
│   │   ├── fetcher.py           # 数据获取 + 回填逻辑
│   │   └── querier.py           # 查询逻辑
│   ├── db/
│   │   ├── models.py            # SQLAlchemy 模型
│   │   ├── engine.py            # 数据库引擎
│   │   └── sync.py              # 配置同步 + 迁移
│   ├── ds/
│   │   ├── router.py            # 数据源路由
│   │   ├── akshare_source.py    # akshare 实现
│   │   └── pcf_official.py      # 交易所 PCF
│   └── tools/
│       ├── fetch.py             # 获取工具
│       ├── query.py             # 查询工具
│       ├── maintenance.py       # 运维工具
│       ├── components.py        # 成分股管理
│       └── export.py            # 导出
└── fin_data.db                  # SQLite 数据库（自动创建）
```

## License

MIT
