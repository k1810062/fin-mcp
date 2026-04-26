---
name: etf_ds
description: "金融数据查询与更新技能。当用户查询行情、涨跌幅、成分股、持仓变更、篮子价值，或要求更新金融数据时触发。支持ETF和股票，行业和标的由config.json动态配置。"
---

# ETF 金融数据技能

## 数据来源

所有数据来自 MCP Server `fin-data`（已注册在 settings.local.json 中）。数据库自动创建，无需手动初始化。

**ETF 列表与行业映射** 由 `config.json` 动态配置，不写死在此文件中。首次使用时会自动建库建表。

## 核心逻辑：先查目标数据，没有再问

用户请求进入后，先判断模式，然后执行对应操作。

**所有查询操作（Mode B）统一走这个流程：**

```
1. 按用户指定的目标去查（指定行业/ETF代码，不查全量）
   → 如：问"半导体" → query.summary(industry="半导体")
   → 如：问"512480" → query.daily_quotes(codes=["512480"])

2. 检查返回结果
   → records 有数据 → 输出给 AI 做分析解读
   → records 为空 → 告知用户"XX暂无数据，是否要先拉取？"
        → 用户同意 → 查出目标对应的 ETF code，只拉目标 ETF
             fetch.market_data(codes=["目标ETF代码"], start_date="30天前")
        → 用户拒绝 → 结束
   → 调用出错（如数据库异常）→ 同样视为无数据，告知用户并询问是否重试
```

行业 → ETF code 的映射在 config.json 中的 etfs 列表里。问行业"稀土"就知道要拉 159713。

适用所有场景：首次使用（数据库为空的冷启动）、有部分数据但目标缺失、工具调用异常。

## 模式选择

### Mode A 运行规则

Mode A 涉及批量网络请求和数据库写入，运行时间较长，**始终使用 `run_in_background=true` 在后台执行**。执行完成后通知用户摘要结果，不展示原始数据。

**更新按时间段统一执行，保证 PCF + 股票行情 + ETF 行情覆盖同一时间段：**

```
包含词: 更新/同步/补全/批量/跑数据/全量/初始化
→ Mode A（静默入库，后台运行）
  执行顺序（同一时间段）：
    1. fetch.pcf_components(mode="batch") — PCF 成分股 + NAV
       每个交易日单独保留快照，自动与前一个交易日对比检测变更
    2. fetch.market_data(asset_type="stock", mode="batch") — 成分股行情
    3. fetch.market_data(asset_type="etf", mode="batch") — ETF 行情

  不提日期 → 默认今天，非交易日向前找最近交易日
  提日期 → 按指定范围，取出所有交易日逐一操作
  首次拉取（DB为空走询问流程后同意）需传 start_date="30天前"

  提示规则：
  - PCF 有数据但当日行情没有 → 提示用户该日只有成分股快照，无价格数据
  - 所有数据均为零 → 提示"交易日但API暂无数据返回"
```

包含词: 查/看/分析/比较/多少/怎么样/排名/持仓/成分股/PE/溢价
→ Mode B（定向查询）
  只查用户指定的目标数据（行业/代码），不拉全量
  返回完整数据 + 分析解读
  如返回空 → 告知用户 + 询问是否更新

包含词: 导出/Excel/日报
→ Mode B 导出分支
  调用 export.to_excel

包含词: 检查/修复/统计/磁盘/完整性
→ 运维
  调用 maintenance.* 工具
```

Mode A 的精髓：**只入库不占上下文**。返回类似 `"已更新50只ETF，新增1200条记录，耗时45s"`，不展示原始数据。

Mode B 的精髓：**只拿用户要的，拿完就分析**。拿到结构化数据后直接解读。不查不需要的数据。

## 数据缺失交互规则

当查询结果为空时，用一句话说清 + 问是否更新：

> "XX行业暂无数据，需要拉取最新行情吗？"
>
> 用户同意 → 查 config.json 找到目标 ETF code → fetch.market_data(codes=["目标ETF"], start_date="30天前") → 重新查询 → 输出分析
> 用户拒绝 → "好的，有需要时告诉我"

注意：必须等用户明确回答后操作，不要替用户做决定。

## 工具调用规则

### 数据获取

```markdown
批量更新行情:
  fetch.market_data(asset_type="all", mode="batch")
  → 返回 {"summary": {"etf_updated": N, "stock_rows": N}}

批量更新PCF:
  fetch.pcf_components(mode="batch")
  → 返回 {"summary": {"total": N, "ok": N, "total_changes": N}}

定向查某ETF行情:
  query.daily_quotes(asset_type="etf", codes=["512480"], days=5)
  → 返回 records

指定日期查询:
  query.daily_quotes(asset_type="etf", codes=["512480"], start_date="2026-04-01", end_date="2026-04-24")
  → 返回 records
```

### 涨跌幅排名

```markdown
今日排名（最新交易日）:
  query.summary()
  → 会附带分析：领涨板块TOP3、领跌板块TOP3、宽基指数表现

按行业筛选:
  query.summary(industry="半导体")
```

### 成分股

```markdown
最新持仓:
  query.components(etf_code="512480")
指定日期:
  query.components(etf_code="512480", trade_date="2026-04-24")
自定义持仓:
  components.manage(action="set", etf_code="512480", stocks=[...])
恢复PCF:
  components.manage(action="reset", etf_code="512480")
查看所有自定义:
  components.manage(action="list")

持仓变更历史:
  query.changes(etf_code="512480")
```

### 篮子价值

```markdown
计算篮子价值（成分股×收盘价，对比净值看溢价）:
  query.basket(etf_code="512480", trade_date="2026-04-24")
```

### 运维

```markdown
完整性检查:
  maintenance.check(checks=["missing_quotes", "date_gaps"])
数据库统计:
  maintenance.stats()
```

## 结果解读规则

### 涨跌幅分析

展示排名后，额外输出：
- 领涨板块 TOP3（按行业维度汇总）
- 领跌板块 TOP3
- 宽基指数表现（沪深300/中证500/上证50/科创50）

### 成分股解读

每次展示成分股时标注数据来源（pcf/manual），如果有变更检测记录，用 query.changes 查看历史。

### 持仓变更解读

变更展示规则：
- 新增/剔除 → 重点提示，标注股票名称和代码
- 数量变化 → 只展示幅度 >5% 的

## 自动化规则

配置 `config.json` 中的 automation 开关为 true 后生效：

- 如果用户连续 3 天未更新 PCF，首次查询时提示"PCF数据可能已过期"
- 如果用户查询日期>A股有交易日但数据库无数据，主动询问是否需要先更新
- 盘后查询"今天怎么样"时，自动判断交易日并先用 Mode A 更新后再用 Mode B 查询

## 关键约束

1. **不写死行业和标的** — 所有行业/ETF/股票信息从 config.json 读取，由用户自行维护
2. **不获取多余数据** — Mode A 只入库不占上下文，Mode B 只拿用户指定的
3. **无未来函数** — 所有涨跌幅只依赖更早交易日的数据，不修改已有历史记录
4. **数据可追溯** — 成分股按天+来源快照，行情只追加不覆盖
