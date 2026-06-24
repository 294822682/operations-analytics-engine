# 历史指标 SQLite 承接层方案 v1

## 背景

本方案面向 OAE 历史文件里的跨月数据拉取和聚合。SQLite 在这里不是一次性结果表，也不是只服务某个固定月份范围；它是一个可重跑的本地承接层：

- 换月份：按新的 `--start-month` / `--end-month` 重跑构建命令，重新扫描对应月份源文件并重建 SQLite。
- 换指标：如果指标所需字段已经进入 staging 表，直接新增查询或视图；如果还没进入 staging，需要扩展字段映射和 staging contract，然后重跑构建。
- 换源文件版本：替换或补充 `历史文件/*` 后重跑，`hist_source_registry` 会重新记录本次参与构建的源文件、sheet、行数和缺口。

`2026年1月` 到 `2026年5月` 只是第一版验收样本，用来证明这层可以承接已核对的直播小时、曝光、线索、抖音来客唯一线索订单、成交，以及后续可补的 A3 人群增长。

当前 `output/lead_daily.db` 的 SQL 层主要服务 `output/fact_attribution.csv` 之后的日报查询。它适合承接 fact 快照后的 SQL 诊断和看板查询，但不应该直接承担历史 Excel/CSV 原始文件的读取、字段对齐、缺口审计和跨源聚合。

## 目标

- 让历史文件聚合进入可审计的 SQL 承接层。
- 保留源文件、sheet、字段缺口、参与聚合状态，避免只留下脚本结果。
- 复用现有 OAE 业务口径，不重写复杂归因逻辑。
- 不污染现有日报主链路 `fact_attribution.csv -> lead_daily.db -> sql_reports`。
- 第一版先覆盖月度汇总所需的核心源文件和字段；后续新增指标时扩展 staging contract，而不是手工改 SQLite 结果。

## 非目标

- 不替换现有 `f_actual_detail`。
- 不把历史 Excel/CSV 直接追加进 `output/fact_attribution.csv`。
- 不在第一版用纯 SQL 重写抖音来客直播窗口匹配。
- 不把缺失字段补 0，例如 A3 人群增长缺源字段时必须保留缺口状态。
- 不改现有日报、Feishu 输出、dashboard API 的正式口径。
- 不把 SQLite 当成手工维护的长期事实表；它应由源文件和构建命令重建。

## 建议落地位置

第一版建议单独生成历史指标数据库：

- `output/historical_metrics.db`

如果后续要并入主库，也应保持独立表前缀：

- `hist_source_registry`
- `stg_*`
- `v_monthly_*`

## 表结构草案

这些表是可重跑构建产物。每次构建会按指定月份范围重建 registry、staging 和视图，SQLite 文件本身不作为人工编辑对象。

### hist_source_registry

记录每个历史源文件的可审计身份。

| 字段 | 说明 |
|---|---|
| source_id | 稳定源文件 ID |
| month | 业务月份，如 `2026-05` |
| source_kind | `live_progress` / `seed_ledger` / `lead_csv` / `deal_csv` |
| source_path | 相对仓库路径 |
| sheet_name | Excel sheet；CSV 留空 |
| row_count | 原始行数 |
| required_columns_status | `passed` / `missing` |
| missing_columns | 缺失字段列表 |
| included_in_rollup | 是否参与聚合 |
| note | 口径或异常说明 |

### stg_live_sessions

来自 `*直播进度表*.xlsx`。

| 字段 | 说明 |
|---|---|
| month | 业务月份 |
| date | 直播日期 |
| account | 开播账号 |
| platform_component | 平台&挂载组件 |
| host | 本场主播 |
| start_time | 开播时间 |
| end_time | 下播时间 |
| duration_hours | 直播小时 |
| impressions | 曝光人数 |
| raw_live_leads | 全场景线索人数 |
| source_id | 来源文件 ID |

### stg_seed_sessions

来自 `EXEED星途台账*.xlsx`。

| 字段 | 说明 |
|---|---|
| month | 业务月份 |
| date | 直播日期或创建时间 |
| account | 开播账号 |
| host | 本场主播 |
| start_time | 开播时间 |
| end_time | 下播时间 |
| duration_hours | 直播小时 |
| impressions | 曝光人数 |
| raw_live_leads | 直播全场景商机量 |
| a3_growth | A3 人群增长；缺源字段时为 NULL |
| a3_source_status | `available` / `missing_source_field` |
| source_id | 来源文件 ID |

### stg_leads

来自 `总部新媒体线索*.csv`。

| 字段 | 说明 |
|---|---|
| month | 按创建时间归属的业务月份 |
| lead_id | 线索 ID |
| phone_key | 手机号归一 key，可复用 PHONE9 口径 |
| create_time | 创建时间 |
| channel2 | 渠道2 |
| channel3 | 渠道3 |
| account | 账号归一结果 |
| source_id | 来源文件 ID |

### stg_deals

来自 `总部新媒体成交*.csv`。

| 字段 | 说明 |
|---|---|
| month | 按成交时间归属的业务月份 |
| lead_id | 线索 ID |
| order_id | 订单编号 |
| order_status | 订单状态 |
| order_time | 下订时间 |
| deal_time | 成交时间 |
| deal_model | 成交车型 |
| source_id | 来源文件 ID |

### stg_douyin_laike_orders

第一版由 OAE 现有 Python 口径计算后落表，不用 SQL 重写匹配算法。

| 字段 | 说明 |
|---|---|
| month | 业务月份 |
| total_orders | 抖音来客唯一线索订单 |
| method | 固定为 `build_douyin_laike_order_metrics` |
| source_id_live | 直播进度表 source_id |
| source_id_leads | 线索表 source_id |
| note | 口径说明 |

## 聚合视图草案

视图是第一版默认指标口径。计算其他字段时，优先复用 staging 明细表写新的查询或视图；只有当源字段没有进入 staging 时，才需要更新代码里的字段映射和表结构。

### v_monthly_live_metrics

```sql
SELECT
  month,
  SUM(duration_hours) AS live_hours,
  SUM(impressions) AS impressions,
  SUM(raw_live_leads) AS raw_live_leads
FROM (
  SELECT month, duration_hours, impressions, raw_live_leads FROM stg_live_sessions
  UNION ALL
  SELECT month, duration_hours, impressions, raw_live_leads FROM stg_seed_sessions
)
GROUP BY month;
```

### v_monthly_leads

```sql
SELECT
  month,
  COUNT(*) AS lead_rows,
  COUNT(DISTINCT lead_id) AS unique_lead_ids,
  COUNT(DISTINCT phone_key) AS unique_phone_keys
FROM stg_leads
GROUP BY month;
```

第一版对外汇总句使用 `lead_rows`，因为当前 1-5 月历史汇总采用“总部新媒体线索源文件按创建时间原始条数”。

### v_monthly_deals

```sql
SELECT
  month,
  COUNT(DISTINCT lead_id) AS delivered_deals
FROM stg_deals
WHERE order_status = '已交车'
  AND deal_time IS NOT NULL
GROUP BY month;
```

### v_monthly_summary

```sql
SELECT
  l.month,
  l.live_hours,
  l.impressions,
  le.lead_rows,
  dko.total_orders AS douyin_laike_orders,
  d.delivered_deals,
  ss.a3_growth,
  ss.a3_source_status
FROM v_monthly_live_metrics l
LEFT JOIN v_monthly_leads le ON le.month = l.month
LEFT JOIN stg_douyin_laike_orders dko ON dko.month = l.month
LEFT JOIN v_monthly_deals d ON d.month = l.month
LEFT JOIN (
  SELECT
    month,
    SUM(a3_growth) AS a3_growth,
    CASE
      WHEN SUM(CASE WHEN a3_source_status = 'available' THEN 1 ELSE 0 END) > 0
      THEN 'available'
      ELSE 'missing_source_field'
    END AS a3_source_status
  FROM stg_seed_sessions
  GROUP BY month
) ss ON ss.month = l.month;
```

## 第一版验收样本

月度结果必须能输出 1-5 月明细，并汇总回当前已核对数字。这里的 1-5 月不是系统边界，只是验收样本：

| 指标 | 1-5 月总计 |
|---|---:|
| 直播小时 | 4482.0 |
| 曝光 | 28376.6w |
| 线索 | 139824 |
| 抖音来客唯一线索订单 | 2084 |
| 成交 | 260 |

5 月结果必须对齐当前正式输出：

- `output/sql_reports/feishu_dashboard_source_latest_2026-05-31.tsv`
- 曝光：`71555952`
- 线索：`23452`
- 抖音来客唯一线索订单：`94`
- 成交：`111`

## 质量规则

- 每个源文件必须进入 `hist_source_registry`。
- 缺字段不能静默跳过，必须写入 `missing_columns`。
- A3 缺源字段时，`a3_source_status = 'missing_source_field'`，`a3_growth = NULL`。
- 来客订单必须保留 method，不把 `直播进度表订单数`、`成交表订单` 和 `抖音来客唯一线索订单` 混成一个指标。
- 所有聚合结果必须能反查 source_id。

## 实施顺序

1. 先做只读 source registry 扫描。
2. 再落 4 张 staging 明细表。
3. 抖音来客唯一线索订单先沿用 Python 口径计算后落表。
4. 建月度视图和总计查询。
5. 用 1-5 月总计和 5 月正式 TSV 做验收。

## 重跑和扩展规则

### 换月份

不需要改 SQLite 结构。直接重跑：

```bash
PYTHONPATH=.:src .venv/bin/python -m oae.cli.build_historical_metrics_db \
  --workspace . \
  --start-month 2026-03 \
  --end-month 2026-06 \
  --db output/historical_metrics.db
```

这会按指定月份范围重新扫描 `历史文件/`，重建 registry、staging 和视图。

### 换已有字段的聚合方式

如果字段已经在 staging 表中，例如 `duration_hours`、`impressions`、`lead_rows`、`delivered_deals`，不需要更新 SQLite 文件或重新导源，只需要写新的 SQL 查询或新增视图。

### 新增源字段或新指标

如果要算的字段目前没有进 staging，例如未来要补某个新的线索质量字段，就需要：

1. 在 `SOURCE_SPECS` 中补字段别名和缺口检查。
2. 在对应 `stg_*` 表中增加字段。
3. 在读取逻辑中从源文件抽取该字段。
4. 新增或更新 `v_monthly_*` 视图。
5. 重跑构建命令，让 SQLite 从源文件重新物化。

原则是：更新代码里的承接 contract，然后重跑；不要直接手改 SQLite 表里的数据。

## 边界

这条链路是历史聚合承接层，不是日报主链路替换。现有日报继续使用：

`output/fact_attribution.csv -> output/lead_daily.db -> output/sql_reports`

历史指标 SQLite 承接层使用：

`历史文件/* -> output/historical_metrics.db -> v_monthly_summary`
