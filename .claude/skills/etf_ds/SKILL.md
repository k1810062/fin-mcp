---
name: etf_ds
description: "金融数据查询与更新技能。当用户查询行情、涨跌幅、成分股、持仓变更、篮子价值，或要求更新金融数据时触发。支持ETF和股票，行业和标的由config.json动态配置。"
---

# ETF 金融数据技能

## 数据来源

MCP Server `fin-data`（`.mcp.json`），数据源 akshare + 交易所 PCF，SQLite 自动建表。**ETF 列表与行业映射** 由 `config.json` 动态配置。

## 核心流程

### 模式选择

| 触发词 | 模式 | 行为 |
|--------|------|------|
| 更新/同步/补全/批量/初始化 | **Mode A** | 静默入库，返回摘要（后台运行） |
| 查/看/分析/排名/持仓/多少 | **Mode B** | 定向查询 + 分析解读 |
| 导出/Excel/日报 | **导出** | export.to_excel |
| 检查/修复/统计 | **运维** | maintenance.* |

### Mode A（更新）

执行顺序（同一时间段，后台异步）：
1. `fetch.pcf_components(mode="batch", trade_date=start, end_date=end)` — 多日 PCF
2. `fetch.market_data(asset_type="all", mode="batch")` — 行情入库，自动算权重和衍生字段

不提日期 → 默认今天。首次拉取传 `start_date="30天前"`。

### Mode B（查询）

查不到数据时主动问用户是否要拉取，不要替用户做决定。

行业 → ETF code 映射在 config.json 的 `etfs` 里。

## 业务规则

### 涨跌幅
展示领涨/领跌 TOP3 板块 + 宽基指数（沪深300/中证500/上证50/科创50）。

### 成分股
标注数据来源（pcf/manual）和权重，有变更时用 `query.changes` 看历史。

### 持仓变更
新增/剔除 → 重点提示。数量变化 → 只展示幅度 >5% 的。

### 提示规则
- PCF 有数据但当日行情 NULL → "该日只有成分股快照，无价格数据"
- 所有数据为零 → "交易日但API暂无数据返回"

## 约束

1. 行业和标的从 config.json 读取，不写死
2. Mode A 只入库不占上下文，Mode B 只查用户指定的
3. 所有涨跌幅只依赖更早交易日数据，不修改已有记录
4. 成分股按天+来源快照，行情只追加不覆盖
