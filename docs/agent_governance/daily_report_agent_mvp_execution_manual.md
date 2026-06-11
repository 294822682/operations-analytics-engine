# Daily Report Agent MVP 执行手册草案

日期：2026-06-11

状态：第二版草案

适用范围：`/Users/ahs/Desktop/Operations Analytics Engine`

## 1. 手册目的

这份手册只解决一个问题：

**后续 OAE Daily Report Agent MVP 和相关 Codex/agent 任务，应该如何理解代码、执行链路、判断状态和停止扩展。**

`codegraph` 在这里是代码理解层，不是业务执行层。

它用于：

- 快速定位 OAE 代码结构。
- 追踪调用链和影响半径。
- 支撑读审、异常排查和受控 agent 编排设计。
- 降低 Codex 反复全文 grep / 误读链路的概率。

它不用于：

- 跑日报。
- 替代 `oae.jobs.daily_pipeline`。
- 判断日报是否可发布。
- 判断业务日期是否有效。
- 替代 Excel / workbook-visible 验收。
- 替代 Feishu 发布状态验证。
- 替代人工业务口径确认。

## 2. 固定主链路

OAE Daily Report 主链路固定为：

```text
fact -> snapshot -> ledger -> analysis -> export -> quality -> run manifest
```

任何 agent 不能把这条链路简化成“有 Markdown/TSV/PNG 就完成”。

日报生成、业务可读状态、发布候选状态必须分开判断：

- 代码链路是否能执行。
- artifact 是否生成。
- `quality_status` 是 `pass` / `warning` / `fail`。
- `quality_decision` 是 `safe` / `investigate` / `block`。
- `release_readiness` 是 `ready` / `review` / `blocked` / `unknown`。
- 业务日期是否是最新有效业务日期。
- workbook-visible / Feishu-visible 状态是否经过人工或可见验收。

## 3. 真相源分层

Agent 输出时必须分开报告以下真相源。

### 3.1 代码真相

代码真相来自 Git 工作区和源码文件。

优先证据：

- `git status --short`
- `codegraph status .`
- `codegraph_explore`
- `codegraph_node`
- 具体源码文件和行号

代码真相只能说明：

- 入口在哪里。
- 调用链如何连接。
- 哪些函数影响哪些模块。
- 测试覆盖面在哪里。

代码真相不能说明：

- 今天日报可以发布。
- Excel 里可见数据正确。
- Feishu 应用已经生效。
- 业务口径已经确认。

### 3.2 Artifact 真相

Artifact 真相来自 OAE 管线写出的文件。

常见路径：

- `output/`
- `output/sql_reports/`
- `artifacts/snapshots/`
- `artifacts/exports/`
- `artifacts/runs/`
- `全量分析/`

Artifact 真相必须说明：

- 具体文件名。
- 具体 run id。
- 具体 report date。
- 质量状态字段。
- 是否存在 required artifacts 缺失。

Artifact 真相不能自动等于发布可用。

### 3.3 Workbook-visible 真相

Excel / CSV / workbook 任务以可见 workbook 状态为准。

必须报告：

- workbook 文件。
- sheet。
- range。
- 行列数量。
- 关键单元格或可见表头。
- 验证命令。

不能只用脚本结论替代 workbook-visible 结论。

### 3.4 Feishu / 浏览器可见真相

Feishu 发布、网页应用、嵌入页和 `/oae` 页面必须用可见状态或接口状态证明。

必须分开：

- 本地 HTML/API 状态。
- Render / deploy 状态。
- Feishu Open Platform 已保存状态。
- Feishu app 版本发布状态。
- 真实浏览器可见页面状态。

“已保存”不等于“已发布生效”。

### 3.5 人工业务口径真相

以下内容优先于自动规则：

- 人工专项归属确认。
- 手动 override。
- 用户已明确纠偏的业务口径。
- 已确认的 PHONE9 / 去重 / mouthpiece 规则。

字段不存在时必须保持缺失或 `未提供`，不能用相近字段硬补。

## 4. 代码理解层：codegraph 使用规则

### 4.1 适用场景

使用 `codegraph` 的场景：

- 读审 OAE 主链路。
- 找某个 CLI / service / export 的入口。
- 查询调用者、被调用者和影响半径。
- 判断一个变更会影响哪些测试或模块。
- 设计 Daily Report Agent 的受控入口。
- 排查 quality / manifest / dashboard annotation 的代码流。

不使用 `codegraph` 的场景：

- 需要读取最新业务数据。
- 需要检查 workbook-visible 状态。
- 需要跑管线生成 artifact。
- 需要验证 Feishu 线上页面。
- 需要判定业务发布结论。

### 4.2 固定检查命令

每次 codegraph 相关任务先检查：

```bash
git status --short
codegraph --version
codegraph status .
```

期望当前 codegraph 版本：

```text
0.9.9
```

OAE 当前索引期望状态：

```text
Files: 149
Nodes: 1795
Edges: 4545
Journal: wal
Index is up to date
```

如果 `.codegraph/` 不存在，先确认根目录 `.gitignore` 包含：

```text
.codegraph/
```

然后才能执行：

```bash
codegraph init -i
```

`.codegraph/` 必须保持本地索引目录，不能纳入 Git。

### 4.3 建议查询入口

主链路总览：

```text
daily_pipeline export_feishu_report verify_report_tsv dashboard_daily_service OAE main chain fact snapshot ledger analysis export quality run manifest
```

导出和 manifest：

```text
export_feishu_report write_feishu_manifests write_export_manifest dashboard_source feishu_report_latest feishu_table_latest
```

质量和发布状态：

```text
run_business_quality_checks build_quality_report build_doctor_manifest load_release_candidate_evidence quality_status quality_decision release_readiness
```

Dashboard 消费：

```text
DashboardDailyService quality_annotation_source run_manifest quality_report feishu_dashboard_source_latest
```

## 5. 主链路代码地图

### 5.1 总入口

主入口：

- `src/oae/jobs/daily_pipeline.py:51` - `main()`

职责：

- 发现 runtime inputs。
- 建立 `run_id`。
- 执行 fact / sqlite / sql daily / target daily / analysis。
- 执行 Feishu report export。
- 执行 TSV verify。
- 生成 quality report。
- 生成 doctor manifest。
- 写出 run bundle。

禁止事项：

- Agent 不能绕过 `daily_pipeline.main` 私自拼日报。
- Agent 不能只调用 export 层后直接宣布日报完成。
- Agent 不能把 artifact 存在等同于业务发布可用。

### 5.2 fact

入口：

- `src/oae/jobs/daily_pipeline.py:129` - `build_fact_step`
- `src/oae/cli/build_fact.py:125` - `run()`
- `src/oae/facts/pipeline.py:16` - `build_fact_artifacts()`
- `src/oae/facts/assembler.py:13` - `build_fact()`

主要产物：

- `output/fact_attribution.csv`
- `output/fact_attribution.xlsx`
- `output/host_counts_weighted.csv`
- `output/host_counts_weighted.xlsx`

检查重点：

- 输入文件是否来自正确 runtime input。
- `run_id` / schema / metric metadata 是否贯穿。
- 手动归属 override 是否在 fact 后被检查。

### 5.3 snapshot / ledger

入口：

- `src/oae/jobs/daily_pipeline.py:154` - `downstream_steps`
- `oae.cli.export_target_daily`

主要产物：

- `artifacts/snapshots/daily_performance_snapshot_latest_*.csv`
- `artifacts/snapshots/compensation_ledger_*.csv`

检查重点：

- snapshot 和 ledger 是否使用同一 `run_id`。
- ledger 是否能和 snapshot 对账。
- freeze_id 是否明确。

### 5.4 analysis

入口：

- `src/oae/jobs/daily_pipeline.py:206` - `oae.cli.run_analysis`

主要产物：

- `全量分析/analysis_workbook_unified-fact_latest_*.xlsx`
- `artifacts/snapshots/analysis_snapshot_unified-fact_latest_*.csv`
- `artifacts/exports/analysis/*.manifest.json`

检查重点：

- analysis snapshot 是否包含必须主题。
- workbook 是否是最终 intended attachment，不能被临时截图替代。

### 5.5 export

入口：

- `src/oae/jobs/daily_pipeline.py:273` - `export_step`
- `src/oae/exports/feishu_report.py:200` - `main()`

导出层写出：

- `output/sql_reports/feishu_report_latest_{report_date}.md`
- `output/sql_reports/feishu_table_latest_{report_date}.tsv`
- `output/sql_reports/feishu_dashboard_source_latest_{report_date}.tsv`

Manifest 写出：

- `src/oae/exports/feishu_report.py:407` - `write_feishu_manifests()`
- `src/oae/exports/feishu_manifest.py:11` - `write_feishu_manifests()`
- `src/oae/exports/manifest.py:11` - `write_export_manifest()`

对应 manifest：

- `artifacts/exports/feishu_report_latest_{report_date}.manifest.json`
- `artifacts/exports/feishu_table_latest_{report_date}.manifest.json`
- `artifacts/exports/feishu_dashboard_source_latest_{report_date}.manifest.json`

检查重点：

- `report_date` 和最新有效业务日期必须分开。
- dashboard source TSV 是 BI-facing source contract，不等于日报文本模板本身。
- manifest 必须带 schema / metric / template / freeze / run metadata。

### 5.6 TSV verify

入口：

- `src/oae/jobs/daily_pipeline.py:319` - `verify_step`
- `src/oae/quality/tsv_verify.py:116` - `main()`

成功信号：

```text
STATUS=PASSED
```

失败信号：

```text
STATUS=FAILED
```

检查重点：

- verify 使用的 fact / snapshot / seed workbook / seed targets 必须和 export 路径一致。
- 如果 accepted latest report 和 verify 不一致，先查 snapshot / seed / manual override 输入漂移。
- 不要直接把 TSV verify 失败解释成日报业务失败；先定位是哪一层输入或 contract 漂移。

### 5.7 quality

入口：

- `src/oae/jobs/daily_pipeline.py:400` - `run_business_quality_checks()`
- `src/oae/quality/business.py:16` - `run_business_quality_checks()`
- `src/oae/jobs/daily_pipeline.py:417` - `build_quality_report()`
- `src/oae/quality/reports.py:9` - `build_quality_report()`

质量检查覆盖：

- fact layer。
- snapshot layer。
- ledger layer。
- export manifest contracts。
- analysis snapshot。

关键字段：

- `overall_status`
- `summary.operational_decision`
- `summary.key_alerts`
- `summary.attention_items`
- `summary.configured_threshold_alerts`

状态解释：

- `overall_status=pass` 且 `operational_decision=safe`：质量层可继续。
- `overall_status=warning` 或 `operational_decision=investigate`：需要人工 review。
- `overall_status=fail` 或 `operational_decision=block`：阻断发布候选。

### 5.8 doctor manifest / run manifest

入口：

- `src/oae/jobs/daily_pipeline.py:448` - `run_manifest`
- `src/oae/jobs/daily_pipeline.py:488` - `build_doctor_manifest()`
- `src/oae/services/execution_doctor_logic.py:9` - `build_doctor_manifest()`

写出位置：

- `src/oae/jobs/daily_pipeline.py:512` - `run_manifest_{run_id}.json`
- `src/oae/jobs/daily_pipeline.py:513` - `quality_report_{run_id}.json`
- `src/oae/jobs/daily_pipeline.py:514` - `doctor_manifest_{run_id}.json`

关键回填：

- `preflight_status`
- `quality_status`
- `quality_decision`
- `release_readiness`

Required artifacts：

- `src/oae/jobs/daily_pipeline.py:531` - `_daily_report_required_artifacts()`

至少包含：

- `feishu_report_latest_{report_date}.md`
- `feishu_dashboard_source_latest_{report_date}.tsv`
- `feishu_dashboard_visual_p1_p5_long_compact_latest_{report_date}.svg`
- `feishu_dashboard_visual_p1_p5_long_compact_latest_{report_date}.png`

### 5.9 release gate

入口：

- `src/oae/cli/run_release_gates.py:40` - `main()`
- `src/oae/services/release_gate_logic.py:103` - `load_release_candidate_evidence()`
- `src/oae/services/release_gate_logic.py:205` - `classify_release_candidate_status()`
- `src/oae/services/release_gate_logic.py:231` - `evaluate_gate_run()`

职责：

- 读取最新 run bundle。
- 判断 missing / blocked / review / ready。
- 在 strict release profile 下强制要求 ready。

注意：

- 工程测试通过不等于发布候选 ready。
- release profile 需要同时看 tests 和 release candidate evidence。

### 5.10 dashboard / API 消费

入口：

- `src/oae/api/dashboard_app.py:45` - `/oae` redirect
- `src/oae/api/dashboard_app.py:60` - `/dashboard/daily/latest/feishu-link`
- `src/oae/api/dashboard_app.py:64` - `/dashboard/daily/latest`
- `src/oae/services/dashboard_daily_service.py:54` - `DashboardDailyService`
- `src/oae/services/dashboard_daily_service.py:2282` - `_trend_quality_annotations()`

读取：

- `output/sql_reports/feishu_dashboard_source_latest_*.tsv`
- `artifacts/runs/run_manifest_*.json`
- `artifacts/runs/quality_report_*.json`

检查重点：

- dashboard source TSV 负责 BI-facing payload。
- quality annotation 来自 run manifest 和 quality report。
- `/oae` 页面验收必须用真实页面结构，不能只看代码 diff。

## 6. Daily Report Agent MVP 执行边界

### 6.1 Agent 可以做

Daily Report Agent MVP 可以：

- 检查 Git 工作区状态。
- 使用 codegraph 读取调用链。
- 执行项目既有命令。
- 跑 `oae.jobs.daily_pipeline`，但只能在明确授权时。
- 读取 `artifacts/runs/*manifest*.json`。
- 读取 `output/sql_reports/*latest*.tsv/md/png`。
- 汇总 run bundle 里的质量状态。
- 生成排查报告、读审报告或执行回收报告。

### 6.2 Agent 不能做

Daily Report Agent MVP 不能：

- 自己拼接日报正文替代 pipeline 输出。
- 自己发明业务口径。
- 用缺失字段硬补。
- 跳过 TSV verify。
- 跳过 run manifest / doctor manifest。
- 把 `warning/investigate` 说成 ready。
- 把 `fail/block` 说成只是提醒。
- 未授权 stage / commit / push。
- 未授权修改 `output/`、`artifacts/`、`全量分析/`。
- 未授权发布 Feishu app。

### 6.3 触发执行前置条件

跑日报管线前必须确认：

- 用户明确要求跑。
- 当前任务不是 review-only / docs-only / inspection-only。
- 工作区 dirty state 已说明。
- 输入目录和关键源文件明确。
- `.env` 或外部凭证不需要暴露。
- 业务日期要求明确，或者允许使用最新有效业务日期。

## 7. 异常处理规则

### 7.1 codegraph 失败

如果 `codegraph status .` 显示 not initialized：

1. 检查 `.gitignore` 是否包含 `.codegraph/`。
2. 如果没有，只添加 `.codegraph/`。
3. 执行 `codegraph init -i`。
4. 再跑 `codegraph status .`。

如果初始化失败：

- 停止。
- 报告失败命令和错误。
- 给唯一下一步建议。
- 不切换到业务管线。

### 7.2 输入缺失

如果 pipeline 报缺输入：

- 先确认缺的是哪个 input contract。
- 报告路径。
- 不创建假输入。
- 不用旧文件代替，除非用户明确确认。

### 7.3 TSV verify 失败

如果 `verify_report_tsv` 失败：

排查顺序：

1. TSV 文件是否是本 run 导出。
2. fact CSV 是否是本 run 生成。
3. snapshot CSV 是否同一 report date / run。
4. seed targets / seed workbook 是否一致。
5. manual override 是否被同一路径使用。
6. 再看模板字段或业务口径。

### 7.4 quality warning / fail

如果 `quality_status=warning`：

- 输出 `quality_decision`。
- 输出 `attention_items`。
- 标记 `release_readiness=review` 或实际字段。
- 不宣布可发布。

如果 `quality_status=fail`：

- 输出 `key_alerts`。
- 输出 `blocking_reasons`。
- 标记 blocked。
- 不继续包装成业务日报。

### 7.5 manifest 缺失

如果 `run_manifest`、`quality_report`、`doctor_manifest` 缺失：

- 优先报告 missing。
- 不从零散 artifact 拼发布状态。
- 不把 dashboard TSV 当 run bundle。

### 7.6 外部链路失败

如果错误来自 OAuth、push timeout、Cloudflare、Render 或 Feishu 发布：

- 单独归类为外部链路问题。
- 不扩大 repo diff。
- 不把本地代码 truth 和远端可见 truth 混在一起。

## 8. 读审和排查固定输出

每次 Daily Report Agent / Codex 执行后，固定输出：

1. 当前任务结论。
2. 使用的入口命令。
3. run id。
4. 输入批次日期。
5. 最新有效业务日期。
6. 关键 artifact 文件名。
7. `quality_status`。
8. `quality_decision`。
9. `release_readiness`。
10. 失败或 review 原因。
11. workbook-visible / browser-visible / Feishu-visible 是否已验收。
12. `git status --short`。
13. 是否违反禁止事项。
14. 下一条唯一建议。

如果只是 codegraph 读审，不跑业务管线，则必须明确：

```text
本轮只做代码理解层检查，未生成业务 artifact，不能据此判断日报可发布。
```

## 9. 受控 Agent 编排建议

MVP 阶段建议拆成 4 个 agent 角色，不要混成一个无边界 agent。

### 9.1 Code Map Agent

职责：

- 使用 codegraph 查询结构。
- 输出调用链和影响半径。
- 不运行业务管线。

停止条件：

- 找到入口和调用路径。
- 或 codegraph 初始化/查询失败。

### 9.2 Pipeline Runner Agent

职责：

- 在用户明确授权后运行 OAE 既有命令。
- 不改业务代码。
- 不绕过 pipeline。

停止条件：

- pipeline 成功并写出 run bundle。
- 或任何输入/质量/命令失败。

### 9.3 Quality Auditor Agent

职责：

- 读取 quality report、doctor manifest、run manifest。
- 判断 ready / review / blocked。
- 给出排查优先级。

停止条件：

- 输出质量状态和阻断原因。
- 不继续发布。

### 9.4 Handoff Writer Agent

职责：

- 把已验证结果整理成文本、TSV 或汇报。
- 只使用已生成 artifact 和人工确认口径。

停止条件：

- 输出 handoff。
- 或发现 quality / business truth 不足，退回 review。

## 10. 固定禁止事项

- 不使用 `git add .`。
- 不默认 stage / commit / push。
- 不修改 `.env`。
- 不暴露凭证。
- 不清理 `output/`、`outputs/`、`artifacts/`、`全量分析/`、`源文件/`、`历史文件/`，除非用户明确授权。
- 不把 codegraph 查询结果说成业务验收结果。
- 不把 artifact 生成说成发布完成。
- 不把 Feishu 已保存说成已发布生效。
- 不把 browser/API 未验收的页面说成可见正确。

## 11. 最小可用 prompt 模板

### 11.1 代码理解层

```text
只做 OAE 代码理解层检查，不跑业务管线。
用 codegraph 查询 daily_pipeline -> export_feishu_report -> verify_report_tsv -> quality/run manifest。
输出入口、调用链、影响半径、相关测试和下一步唯一建议。
```

### 11.2 跑最新日报

```text
先记录 git status --short。
在明确输入目录后运行 OAE 正式 daily_pipeline。
完成后只基于 run_manifest、quality_report、doctor_manifest 和生成 artifact 输出结果。
必须分开报告 business date、artifact、quality_status、quality_decision、release_readiness。
如果不是 ready，不要说可发布。
```

### 11.3 异常排查

```text
不要改代码。
先读取最近 run_manifest、quality_report、doctor_manifest。
按 input -> fact -> snapshot -> ledger -> analysis -> export -> verify -> quality 顺序定位第一个失败点。
输出证据路径、失败字段、最小下一步。
```

## 12. 命令 -> 产物 -> 验收字段 -> 对应测试矩阵

本矩阵不是“每轮都跑完”的 checklist。

使用规则：

- 只运行当前任务明确授权的命令。
- 代码理解层任务只运行 codegraph / 读审命令，不跑业务管线。
- docs-only 任务只改文档，不刷新业务 artifact。
- “对应测试”是验收证据索引，不代表每次都必须执行。
- 如果修改业务逻辑、API、数据结构、报告、分类、权限、配置、存储或前端行为，必须补充或更新对应测试。
- 如果某个验收面只有间接测试覆盖，输出时要明确标记为间接覆盖或覆盖缺口。

| 主链路面 | 常用命令 | 主要产物 | 必看验收字段 / 证据 | 对应测试 / gate |
| --- | --- | --- | --- | --- |
| 代码理解层 | `codegraph status .` | `.codegraph/` 本地索引 | `files`、`nodes`、`edges`、`status=up to date`；只能证明索引可用，不能证明日报可发布 | 无 pytest；作为读审前置证据 |
| 代码理解层 | `codegraph_explore "daily_pipeline export_feishu_report verify_report_tsv dashboard_daily_service"` | MCP 查询结果 | 入口、调用链、影响半径、相关测试面；不得替代 artifact / Feishu / workbook 验收 | 无 pytest；用于定位和读审 |
| fact | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.build_fact` | `output/fact_attribution.csv`、`output/fact_attribution.xlsx`、`output/host_counts_weighted.csv`、`output/host_counts_weighted.xlsx` | 行数、`_lead_key` 唯一性、`business_subject_key` 覆盖率、手机号缺失率、归属成功率、无主线索比例 | `tests/baseline/reference/fact_attribution.csv`；业务质量由 `run_business_quality_checks` 间接覆盖 |
| snapshot | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.export_target_daily` | `artifacts/snapshots/daily_performance_snapshot_latest_*.csv` | `report_date` / 月份、目标口径、账号维度、snapshot row count、schema / metric version | `tests/test_daily_pipeline_release_contract.py`；release gate targeted suite |
| ledger | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.export_target_daily` | `artifacts/snapshots/compensation_ledger_*.csv` | ledger 必填字段、唯一作用域、与 snapshot 对账结果、freeze id | `tests/test_daily_pipeline_release_contract.py`；release gate targeted suite |
| analysis | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.run_analysis --analysis-mode unified-fact` | `全量分析/analysis_workbook_unified-fact_latest_*.xlsx`、`artifacts/snapshots/analysis_snapshot_unified-fact_latest_*.csv`、`artifacts/exports/analysis/*` | analysis mode、subject areas、row count、raw evidence topic coverage、schema / metric version | 当前主要由 full pytest / release gate 间接覆盖；如改分析口径，需补直接测试 |
| export | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.export_feishu_report` | `output/sql_reports/feishu_table_latest_*.tsv`、Markdown、`feishu_dashboard_source_latest_*.tsv`、`artifacts/exports/*manifest*.json` | `run_id`、`schema_version`、`metric_version`、`template_version`、`freeze_id`、`report_date`、dashboard source row count、topline 指标 | `tests/test_feishu_dashboard_source.py`、`tests/test_feishu_table_adapters.py`、`tests/test_feishu_topline.py`、`tests/test_feishu_content.py`、`tests/test_feishu_douyin_laike.py` |
| quality verify | `PYTHONPATH=.:src .venv/bin/python -m oae.cli.verify_report_tsv` | 校验输出；必要时关联 `output/sql_reports/feishu_table_latest_*.tsv` | `STATUS=PASSED`；如果失败，按 snapshot / seed / manual override / export schema 顺序找第一差异点 | `tests/test_feishu_seed_dashboard.py`；相关 export 测试；漂移修复后必须补回归 |
| quality report | 由 `oae.jobs.daily_pipeline` 调用 `build_quality_report`、`run_business_quality_checks` | `artifacts/runs/quality_report_<run_id>.json` | `overall_status`、`summary.operational_decision`、`contract_checks`、`metric_checks`、`structural_checks`、`files[].exists`、threshold profile/source | `tests/test_release_gate_logic.py`、`tests/test_daily_pipeline_release_contract.py`；当前 main 对 quality builder 的直接单测仍是覆盖缺口 |
| run manifest / doctor | 由 `oae.jobs.daily_pipeline` 调用 `build_doctor_manifest` 并写 run manifest | `artifacts/runs/run_manifest_<run_id>.json`、`artifacts/runs/doctor_manifest_<run_id>.json` | `run_id`、`canonical_report_date`、`quality_status`、`quality_decision`、`release_readiness`、`required_artifact_count`、`present_artifact_count`、`missing_required_artifacts` | `tests/test_execution_doctor_logic.py`、`tests/test_release_gate_logic.py`、`tests/test_daily_pipeline_release_contract.py` |
| full pipeline | `PYTHONPATH=.:src .venv/bin/python -m oae.jobs.daily_pipeline --workspace .` | fact、snapshot、ledger、analysis、export、quality、run manifest 全链路 artifact | `run_id`、`canonical_report_date`、`quality_status`、`quality_decision`、`release_readiness`、所有 required artifacts 是否存在 | `make test-targeted`、`make test`、`make ci-pr`；发布前看 `make ci-release` |
| dashboard / API | 本地 API 启动命令按 README / 当前部署脚本确认；读审入口为 `src/oae/api/dashboard_app.py` | `/dashboard/daily/latest`、`/dashboard/daily/latest/feishu-link`、互动 HTML | API response、dashboard source、quality annotation、run manifest linkage；浏览器可见状态必须单独验收 | `tests/api/test_dashboard_daily_api.py`、`tests/api/test_dashboard_app.py`、`tests/test_feishu_dashboard_interactive_html.py`、`make smoke` |
| Feishu embed 辅助 | `PYTHONPATH=.:src .venv/bin/python scripts/prepare_feishu_embed_data.py`、`PYTHONPATH=.:src .venv/bin/python scripts/verify_feishu_embed.py` | embed data / verify 输出 | same-origin、字段映射、Feishu 页面配置；“已保存”不等于“已发布生效” | `tests/test_prepare_feishu_embed_data.py`、`tests/test_verify_feishu_embed.py`、`tests/api/test_dashboard_app.py` |
| release gates | `make test-targeted`、`make test`、`make smoke`、`make ci-pr`、`make ci-release` | `artifacts/runs/release_gate_summary_<profile>.json`（`make ci*`） | exit code、`engineering_status`、`release_candidate_status`、`overall_status`、blocking / review reasons | `src/oae/services/release_gate_logic.py` 定义的 suites；full pytest 由 `make test` / `make ci*` 覆盖 |

### 12.1 矩阵读法

读审时按主链路顺序找第一个失败点：

```text
fact -> snapshot -> ledger -> analysis -> export -> quality -> run manifest
```

如果某一层失败，只报告该层最小证据和唯一下一步，不继续扩大到下一阶段。

示例：

- fact 行数或 `_lead_key` 失败：先回 fact 输入和归属规则，不讨论 Feishu 发布。
- `verify_report_tsv` 失败：先回 snapshot / seed / manual override / export schema 差异，不说日报可发布。
- `quality_status=warning`：artifact 可以存在，但发布状态只能是 review / investigate。
- `release_readiness!=ready`：不能输出“可发布”。
- Feishu 页面只保存未发布：只能说“配置已保存”，不能说“线上生效”。

### 12.2 当前覆盖缺口

以下缺口不阻塞本手册第二版，但后续如果进入测试补强任务，应单独处理：

- `build_quality_report` 的直接单测覆盖不足。
- `run_business_quality_checks` 的直接单测覆盖不足。
- 部分 CLI 入口主要靠 `daily_pipeline`、release gate 或 full pytest 间接覆盖。
- workbook-visible / Feishu-visible 状态不能靠 pytest 替代，仍需要可见验收或人工确认。

## 13. 当前草案后续补强点

下一轮如果继续完善手册，只做一件事：

**把矩阵里的覆盖缺口转成一组最小测试补强任务，并确认是否把已有测试 worktree 合回主线。**
