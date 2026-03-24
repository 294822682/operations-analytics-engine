# 产品化后业务使用地图

这份文档不是工程设计文档，而是业务负责人视角的使用说明。

目标只有四个：

1. 你现在要什么内容，应该去哪里拿。
2. 哪个是正式入口，哪个只是兼容壳。
3. 哪些文件是给 Excel、飞书、老板汇报用的。
4. 如果要让 Codex 继续帮你出汇报，应该怎么下指令。

如果你更想直接抄现成模板，而不是自己组织语言，可以配合看：

- `docs/CODEX_COLLAB_TEMPLATES.md`

---

## 1. 先记住这套系统的正式主链

当前正式主链是：

`fact -> snapshot -> ledger -> analysis -> export -> quality -> run manifest`

翻译成业务语言：

- `fact`
  - 统一事实层，解决“线索到底算到谁、成交回挂到谁”。
- `snapshot`
  - 日报快照，解决“今天和本月经营结果是多少”。
- `ledger`
  - 绩效台账，解决“哪些结果进入结算口径”。
- `analysis`
  - 后链路分析，解决“为什么会这样、问题在哪、该怎么复盘”。
- `export`
  - 给 Excel、飞书、人工消费的标准输出。
- `quality`
  - 质量报告，判断这次跑数是不是安全。
- `run manifest`
  - 运行清单，记录这次到底跑了什么、用了哪些路径。

---

## 1.1 你最常用的三条业务链路

这一节只回答三个问题：

1. 以后我要做日报，应该走到哪一步。
2. 以后我要算绩效，应该看哪个对象。
3. 以后我要做月度大复盘，应该用哪条链。

### A. 做日报

做日报时，不需要把整套链路都跑到最深。

你真正要走的是：

`fact -> snapshot -> export`

如果要正式发出去，建议按下面这条理解：

`fact -> snapshot -> export -> quality`

原因：

- `fact`
  - 先把线索归因、成交回挂、经营主体关系算清楚。
- `snapshot`
  - 再把日报需要的经营结果沉淀成日报快照。
- `export`
  - 再把日报转成 Markdown、TSV、账号层表、到人层表。
- `quality`
  - 最后确认这版日报有没有跑偏。

做日报时你最常看的正式产物是：

- `output/sql_reports/feishu_table_latest_{date}.tsv`
  - 适合直接粘 Excel
- `output/sql_reports/feishu_report_latest_{date}.md`
  - 适合直接发文本版
- `output/sql_reports/daily_goal_account_latest_{date}.csv`
  - 账号层明细
- `output/sql_reports/daily_goal_anchor_latest_{date}.csv`
  - 到人层明细

一句话记忆：

**日报 = fact 算清楚 + snapshot 固化结果 + export 产出给人看。**

### B. 算绩效

算绩效时，核心不是 export，而是 ledger。

你真正要走的是：

`fact -> snapshot -> ledger`

如果要更稳妥，建议按下面这条理解：

`fact -> snapshot -> ledger -> quality`

原因：

- `fact`
  - 先保证经营主体、归因、成交回挂没有错。
- `snapshot`
  - 先形成经营结果快照。
- `ledger`
  - 再把快照沉淀成正式结算对象。
- `quality`
  - 最后确认台账没有唯一性、空值、对账异常。

算绩效时最关键的正式对象是：

- `artifacts/snapshots/compensation_ledger_{month}_{date}.csv`

同时要配合看的对象是：

- `artifacts/snapshots/daily_performance_snapshot_latest_{date}.csv`

怎么理解：

- `daily_performance_snapshot`
  - 经营结果快照，可以重跑
- `compensation_ledger`
  - 绩效结算对象，不应该被当成普通展示表

一句话记忆：

**绩效 = snapshot 看经营结果，ledger 看结算结果。**

### C. 做月度大复盘

月度大复盘的核心不是日报表，而是 analysis。

你真正要走的是：

`fact -> analysis -> export`

如果要做正式复盘会材料，建议按下面这条理解：

`fact -> analysis -> export -> quality`

原因：

- `fact`
  - 先把统一经营主体和归因事实层准备好。
- `analysis`
  - 再做漏斗、时效、渠道、主播、车型路径、区域、回收链路、健康度这些后链路分析。
- `export`
  - 再把复盘结果变成工作簿、快照和可消费产物。
- `quality`
  - 最后确认分析结果没有明显漂移。

月度大复盘最常看的正式产物是：

- `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
  - 统一事实层复盘成品
- `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`
  - 统一事实层分析快照

如果要做原始过程链专项复盘，还要看：

- `全量分析_round*_raw*/analysis_workbook_raw-evidence_latest_{date}.xlsx`
- `全量分析_round*_raw*/analysis_anomaly_report_raw-evidence_latest_{date}.xlsx`
- `artifacts/snapshots/round*_raw*/analysis_snapshot_raw-evidence_latest_{date}.csv`

一句话记忆：

**月度大复盘 = 用 analysis 回答“为什么会这样、问题在哪、下一步怎么干”。**

### D. 三条链路一张表记住

| 业务动作 | 核心链路 | 关键正式产物 |
|---|---|---|
| 日报 | `fact -> snapshot -> export` | `feishu_table / feishu_report / daily_goal_account / daily_goal_anchor` |
| 绩效 | `fact -> snapshot -> ledger` | `compensation_ledger` |
| 月度大复盘 | `fact -> analysis -> export` | `analysis_workbook_unified-fact / analysis_snapshot_unified-fact` |

如果你不想记太多，就只记这三句：

- 日报看 `export`
- 绩效看 `ledger`
- 复盘看 `analysis`

---

## 2. 正式入口和兼容壳怎么区分

### 2.1 正式入口

正式入口都在 `src/oae/cli/` 下，也就是现在真正推荐使用的入口。

- 事实层生成
  - `python -m oae.cli.build_fact`
- SQLite 装载
  - `python -m oae.cli.build_sqlite_db`
- SQL 日报导出
  - `python -m oae.cli.export_sql_daily`
- 目标日报 / 快照 / 台账
  - `python -m oae.cli.export_target_daily`
- 后链路分析
  - `python -m oae.cli.run_analysis`
- 飞书 / TSV 输出
  - `python -m oae.cli.export_feishu_report`
- TSV 校验
  - `python -m oae.cli.verify_report_tsv`
- 全链路执行
  - `python -m oae.jobs.daily_pipeline`

### 2.2 兼容壳

根目录这些文件还在，但不要再把它们当成主入口：

- `build_fact_from_three_sources.py`
- `build_sqlite_db.py`
- `run_sql_daily_export.py`
- `run_target_daily_export.py`
- `generate_feishu_report.py`
- `verify_report_tsv.py`
- `lead_analysis.py`

它们现在的角色是：

- 历史调用兼容
- 回滚壳
- 老路径过渡

一句话：**平时用正式 CLI，兼容壳只在历史习惯或回滚时看。**

---

## 3. 我要什么内容，应该看哪里

### 3.1 我要日报给领导

看这几个正式产物：

- `output/sql_reports/feishu_table_latest_{date}.tsv`
  - 适合直接粘到 Excel
- `output/sql_reports/feishu_report_latest_{date}.md`
  - 适合直接看文本版日报
- `output/sql_reports/daily_goal_account_latest_{date}.csv`
  - 账号层日报明细
- `output/sql_reports/daily_goal_anchor_latest_{date}.csv`
  - 到人层日报明细

如果你要的是“你之前习惯的那种日报结构”，优先看：

- `feishu_table_latest_{date}.tsv`

---

### 3.2 我要目标达成 / 绩效口径

正式对象是：

- `artifacts/snapshots/daily_performance_snapshot_latest_{date}.csv`
  - 日报快照
- `artifacts/snapshots/compensation_ledger_{month}_{date}.csv`
  - 绩效台账

怎么理解：

- `daily_performance_snapshot`
  - 可以重跑，是经营结果快照
- `compensation_ledger`
  - 结算对象，不只是展示表

如果你是在看“这月做到哪里了”，先看 snapshot。  
如果你是在看“绩效到底怎么算”，看 ledger。

---

### 3.3 我要后链路分析 / 月度复盘

这块现在不要再找旧 `analysis_tables.xlsx`。

正式路径是：

- 统一事实层复盘工作簿
  - `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
- 统一事实层分析快照
  - `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`
- 对应导出清单
  - `artifacts/exports/analysis/analysis_workbook_unified-fact_latest_{date}.manifest.json`

如果你要的是“复盘成品”，看工作簿。  
如果你要的是“让 Codex 再给你写一版文本汇报”，看分析快照。

---

### 3.4 我要原始证据链专项分析

`raw-evidence` 路径也是正式路径，但它是专项分析，不是主日报链。

正式路径是：

- 原始证据链分析工作簿
  - `全量分析_round*_raw*/analysis_workbook_raw-evidence_latest_{date}.xlsx`
- 原始证据链异常报告
  - `全量分析_round*_raw*/analysis_anomaly_report_raw-evidence_latest_{date}.xlsx`
- 原始证据链分析快照
  - `artifacts/snapshots/round*_raw*/analysis_snapshot_raw-evidence_latest_{date}.csv`

适用场景：

- 完整过程链 SLA
- 再激活
- 时间异常
- 车型路径
- 跟进强度

---

### 3.5 我要判断今天是不是跑成功了

看两个文件就够了：

- `artifacts/runs/run_manifest_{run_id}.json`
- `artifacts/runs/quality_report_{run_id}.json`

最关键看这两个字段：

- `overall_status`
- `operational_decision`

如果是：

- `overall_status = pass`
- `operational_decision = safe`

就说明这轮主链可用。

---

### 3.6 我要判断正式命名和兼容命名现在是什么关系

看：

- `config/analysis_output_naming.json`
- `artifacts/exports/analysis/analysis_output_naming_status_unified-fact_latest_{date}.json`
- `docs/rules/analysis_output_naming.md`

当前结论：

- canonical 是唯一正式主路径
- compatibility analysis 文件名已经停写
- compatibility shell 还保留，但只是历史入口

---

## 4. 现在这套系统里，哪些名字不要再用了

以下名字不要再作为正式依赖：

- `analysis_tables.xlsx`
- `time_chain_anomaly_report.xlsx`
- `analysis_snapshot_latest_{date}.csv`
- `raw_analysis_snapshot_latest_{date}.csv`

这些名字现在只属于：

- 历史兼容说明
- 老轮次产物
- 归档样本

一句话：**你现在找分析产物，优先找带 `unified-fact` 或 `raw-evidence` 的 canonical 文件名。**

---

## 5. 后链路分析现在到底包含什么

### 5.1 已经并入统一事实层的主题

- funnel
- sla
- quality
- host_anchor
- channel
- ops_review

### 5.2 仍然保留原始证据链依赖的主题

- process_sla
- reactivation
- time_anomaly
- model_path
- followup_intensity

所以你以后判断时要分两类：

- 我要统一经营分析复盘
  - 用 `unified-fact`
- 我要原始过程链专项诊断
  - 用 `raw-evidence`

---

## 6. 你以后最常见的 6 个使用场景

### 场景 A：我要发日报

直接拿：

- `output/sql_reports/feishu_table_latest_{date}.tsv`
- `output/sql_reports/feishu_report_latest_{date}.md`

---

### 场景 B：我要做 Excel 图表

优先拿：

- `output/sql_reports/feishu_table_latest_{date}.tsv`

如果是后链路复盘图表：

- `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`

---

### 场景 C：我要看绩效结算

拿：

- `artifacts/snapshots/compensation_ledger_{month}_{date}.csv`

---

### 场景 D：我要做后链路复盘会汇报

拿：

- `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
- `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`

如果要让 Codex 帮你写汇报，最好基于这两个文件。

---

### 场景 E：我要排查今天为什么数字不对

先看：

- `artifacts/runs/quality_report_{run_id}.json`
- `artifacts/runs/run_manifest_{run_id}.json`

再看：

- `output/fact_attribution.csv`
- `artifacts/snapshots/daily_performance_snapshot_latest_{date}.csv`

---

### 场景 F：我要确认这份东西到底是不是正式产物

优先检查三个点：

1. 路径是不是 canonical
2. 有没有 manifest
3. 有没有出现在最新运行清单里

如果三者都满足，基本就是正式产物。

---

## 7. 以后你怎么给 Codex 下指令，才能得到你真正想要的内容

### 7.1 如果你要日报版文本

可以直接说：

`请基于最新 feishu_table_latest_{date}.tsv，输出日报文本版，不要 HTML。`

---

### 7.2 如果你要后链路复盘汇报

可以直接说：

`请基于最新 analysis_workbook_unified-fact_latest_{date}.xlsx 和 analysis_snapshot_unified-fact_latest_{date}.csv，生成一份线索经营后链路复盘汇报，面向老板，纯文本输出，不要 HTML，不要逐表解释。`

---

### 7.3 如果你要口播稿

可以直接说：

`请基于最新 unified-fact analysis 产物，生成一份 8-12 段的老板口播稿，每段先说结论，再补关键数据。`

---

### 7.4 如果你要 Excel 可直接用的内容

可以直接说：

`请基于最新日报产物，按日报版式输出 TSV，方便我直接粘贴到 Excel。`

---

### 7.5 如果你怕我又给你技术摘要

一定要额外加一句：

`不要按工程文件解释，不要逐表复述，要按经营汇报方式输出。`

---

## 8. 你现在最应该形成的新心智

以前你靠“脚本名记忆”工作。  
现在你应该改成靠“业务产物类型”工作。

不要再先问：

- 哪个脚本在干这个？

而应该先问：

- 我要的是日报、台账、分析快照、分析工作簿，还是质量报告？

新的正确路径是：

1. 先确定你要的业务产物类型
2. 再找对应 canonical 文件
3. 再决定是否需要 CLI 重跑

---

## 9. 一句话版本

现在这套系统已经不是“找某个脚本吐某个表”，而是“按业务目标找到正式产物”：

- 日报看 `feishu_table / feishu_report`
- 绩效看 `compensation_ledger`
- 后链路复盘看 `analysis_workbook_unified-fact / analysis_snapshot_unified-fact`
- 跑数是否成功看 `quality_report / run_manifest`
- compatibility shell 只当历史入口，不当主路径

---

## 10. 小白版：从零开始怎么用

这一节假设你不是写这套系统的人，也不想先读代码。

你只需要知道三件事：

1. 先把输入文件放对位置。
2. 再跑正式入口。
3. 最后去看对应产物。

### 10.1 跑之前你至少要准备什么

最少需要这几类输入：

- 直播进度表
  - 例子：`2026年直播进度表3月.xlsx`
- 线索主表
  - 例子：`总部新媒体线索2026-03-13.csv`
- 成交主表
  - 例子：`总部新媒体成交2026-03-13.csv`
- 月目标配置
  - `config/monthly_targets.csv`
- 消耗配置
  - `config/daily_spend.csv`
- 质量阈值配置
  - `config/quality_thresholds.json`

如果这些文件不全，不要急着跑全链。

先检查：

- 本月直播进度表是否在根目录
- 最新线索和成交文件是否在根目录
- `monthly_targets.csv` 里是否已经写了这个月的账号和主播目标
- `daily_spend.csv` 是否已经补了消耗

### 10.2 最简单的正式运行方式

如果你只是想“把今天完整跑一遍”，用这条：

```bash
PYTHONPATH=src python3 -m oae.jobs.daily_pipeline \
  --workspace . \
  --data-dir . \
  --live-file '2026年直播进度表3月.xlsx' \
  --freeze-id provisional-manual-run \
  --quality-thresholds config/quality_thresholds.json \
  --quality-threshold-profile operational
```

这条命令会把主链全部跑完：

- fact
- snapshot
- ledger
- analysis
- export
- quality
- run manifest

如果你不是要完整重跑，而是只做某个环节，后面看第 11 节。

### 10.3 跑完之后第一眼先看什么

第一眼不要先打开一堆 csv。

先看这两个文件：

- `artifacts/runs/run_manifest_{run_id}.json`
- `artifacts/runs/quality_report_{run_id}.json`

你重点只看下面几项：

- `overall_status`
- `operational_decision`
- `threshold_breach_count`

如果结果是：

- `overall_status = pass`
- `operational_decision = safe`

就说明本轮主链可以继续往下看。

如果不是，就先不要往外发日报，也不要直接拿去算绩效。

### 10.4 跑完以后怎么快速找到结果

你可以按“我要什么”直接找：

- 我要日报
  - 看 `output/sql_reports/`
- 我要绩效
  - 看 `artifacts/snapshots/compensation_ledger_*.csv`
- 我要后链路复盘
  - 看 `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
- 我要原始过程链专项分析
  - 看 `全量分析_round*_raw*/`

### 10.5 小白最容易犯的错

最常见的错不是代码报错，而是口径用错。

常见误区：

1. 把 `snapshot` 当成 `ledger`
   - 错
   - `snapshot` 是经营快照，`ledger` 才是结算对象
2. 拿旧 compatibility 文件名去找结果
   - 错
   - 现在应该找 canonical 名
3. 跑完不看 `quality_report`
   - 错
   - 先看安全，再看结果
4. 直接看某个 csv 就开始下结论
   - 错
   - 先确认自己要的是日报、绩效、还是复盘
5. 看到 raw 分析和 unified 分析数字不一样就认为系统错了
   - 错
   - 它们回答的是不同问题

---

## 11. 按任务拆开的最短操作路径

这一节是“照着做就行”的版本。

### 11.1 我今天只想发日报

步骤：

1. 跑主链，或者至少跑到 `export`
2. 先看 `quality_report`
3. 再看 `output/sql_reports/feishu_table_latest_{date}.tsv`
4. 如果要发文本，直接看 `feishu_report_latest_{date}.md`

最短理解：

- 结果对外发的时候，用 `feishu_table` 和 `feishu_report`
- 明细核对的时候，看 `daily_goal_account` 和 `daily_goal_anchor`

### 11.2 我今天只想看绩效

步骤：

1. 跑到 `ledger`
2. 先确认 `quality_report` 里 ledger 对账正常
3. 打开 `compensation_ledger_{month}_{date}.csv`
4. 如果想追来源，再回看 `daily_performance_snapshot`

最短理解：

- 先看台账
- 再看快照
- 不要反过来

### 11.3 我今天只想做月度大复盘

步骤：

1. 跑到 `analysis`
2. 打开 unified 工作簿：
   - `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
3. 如果需要写经营汇报，配合打开：
   - `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`
4. 如果要看过程链、异常、回收、车型路径，再看 raw 工作簿

最短理解：

- 统一事实层分析用来做“经营复盘”
- raw 分析用来做“过程链专项诊断”

### 11.4 我今天只想确认系统有没有正常跑完

步骤：

1. 看 `run_manifest`
2. 看 `quality_report`
3. 看 `analysis_output_naming_status`
4. 随机检查一个 canonical 工作簿是否真的生成了

最短理解：

- 运行清单确认“有没有跑”
- 质量报告确认“能不能用”

---

## 12. 以后让 Codex 帮你干活，最容易成功的指令怎么写

### 12.1 要日报文本

直接说：

`请基于最新 feishu_table_latest_{date}.tsv，生成一份日报文本版，按部门总体、成交账号、线索质量、账号层、到人层输出，不要 HTML。`

### 12.2 要老板看的后链路复盘汇报

直接说：

`请基于最新 analysis_workbook_unified-fact_latest_{date}.xlsx 和 analysis_snapshot_unified-fact_latest_{date}.csv，生成一份线索经营后链路复盘汇报，面向老板，纯文本输出，不要逐表解释，要有结论、风险和管理动作。`

### 12.3 要原始过程链专项诊断

直接说：

`请基于最新 raw-evidence analysis 产物，输出过程链专项复盘，重点看 SLA、时间异常、再激活、车型路径、跟进强度。`

### 12.4 要 Excel 可直接做图的数据

直接说：

`请基于最新日报产物，按日报版式输出 TSV，我要直接粘贴到 Excel 做图。`

### 12.5 如果你不想看到工程解释

最后一定补一句：

`不要按脚本和文件解释，要按经营汇报方式输出。`

---

## 13. 给别的部门复用时，什么情况下可以直接用

这一节非常重要。

这套系统不是只能给当前部门用。  
如果后面要给别的部门使用，只要满足下面这个前提，就可以复用现有产品骨架：

### 13.1 可以直接复用的前提

必须同时满足：

1. 数据口径不变
   - 手机号仍然是经营主体优先
   - lead_id 仍然只是事件证据
   - 成交回挂逻辑不变
   - 日报、绩效、复盘仍共用同一经营口径
2. 表结构基本一致
   - 线索表字段含义一致
   - 成交表字段含义一致
   - 直播进度表字段含义一致
3. 变化主要只是：
   - 账号不同
   - 主播不同
   - 目标不同
   - 消耗不同

如果满足这三个条件，就不应该重写系统，而应该做“配置迁移”。

### 13.2 什么叫“只换账号和主播，不换口径”

下面这些变化，属于正常可复用范围：

- 新部门有新的直播账号
- 新部门有新的主播名单
- 新部门的目标池不同
- 新部门的每月目标不同
- 新部门的每日消耗不同
- 同样是直播获客、线索归因、成交回挂、主播贡献、经营复盘

这种情况下，系统骨架不用动，主要改配置和映射。

### 13.3 给别的部门用时，优先改哪些地方

#### 第一类：目标与费用配置

先改：

- `config/monthly_targets.csv`
- `config/daily_spend.csv`

你要做的是：

- 把账号目标换成新部门账号
- 把主播目标换成新部门主播
- 把 `parent_account`、`target_pool` 换成新部门结构
- 把月目标、费用目标、CPL、CPS 换成新部门口径

#### 第二类：账号映射

再看：

- `src/oae/rules/account_mapping.py`

如果新部门账号存在：

- 别名很多
- 同一个账号有多种写法
- 平台前缀不统一

就要把新部门账号映射补到：

- `ACCOUNT_MAP`
- `NON_LIVE_ACCOUNTS`

这一步非常关键。  
如果账号映射不补，后面日报、主播归属、分析汇总都容易乱。

#### 第三类：直播进度表和原始字段

再确认：

- 直播进度表列名是否还是当前这套
- 线索表列名是否还能被系统识别
- 成交表列名是否还能被系统识别

相关规则主要在：

- `src/oae/rules/columns.py`

如果新部门只是账号和主播变了，但列名没变，那这里不用改。  
如果列名叫法变了，就要补列名别名，但这已经属于“轻度适配”，不是重做产品。

### 13.4 哪些情况下不能直接复用

下面这些情况，不能简单理解成“换个账号就行”：

1. 经营主体不再是手机号
2. 线索和成交之间没有可回挂关系
3. 直播进度表不再存在
4. 绩效不是按账号/主播目标来算
5. 部门根本不是直播线索经营场景

出现这些情况，就不是“迁移配置”，而是“要重新定义产品边界”。

### 13.5 给别的部门复用时，最安全的迁移顺序

推荐顺序：

1. 先复制一份新部门的数据样本
2. 先补 `monthly_targets.csv`
3. 再补 `daily_spend.csv`
4. 再补 `account_mapping.py`
5. 跑 `fact`
6. 看 `quality_report`
7. 再看 `snapshot`
8. 再看 `ledger`
9. 最后再看 `analysis`

不要一上来就去看大复盘。  
先保证 fact 和 snapshot 没错，再去看 analysis。

### 13.6 给别的部门复用时，最应该先验什么

先验四个点：

1. 无主占比是不是异常升高
2. 归因成功率是不是明显掉下来了
3. 账号汇总是不是能对得上目标池
4. 主播是不是都正确归属到账号

如果这四个点正常，基本就说明“只换账号和主播”的迁移是成立的。

### 13.7 给别的部门复用时，不要乱动哪些东西

如果只是同口径迁移，下面这些不要乱改：

- 经营主体优先级
- 手机号优先去重逻辑
- 成交回挂逻辑
- snapshot / ledger / analysis 的对象关系
- 质量阈值的基本结构
- canonical 输出命名机制

这些属于产品骨架。  
部门切换通常改配置，不改骨架。

---

## 14. 给别的部门复用时，一句话判断法

如果新部门满足：

- 数据结构差不多
- 经营口径不变
- 只是账号和主播变了

那么正确做法是：

**改配置、改映射、跑质量校验，不要重写系统。**

如果新部门连“手机号是经营主体”这个前提都不成立，那就不要硬套。

---

## 15. 最后给真正的小白的操作口诀

如果你什么都记不住，就记下面这几句：

1. 想发日报，看 `feishu_table`
2. 想算绩效，看 `compensation_ledger`
3. 想做复盘，看 `analysis_workbook_unified-fact`
4. 想知道这次能不能信，看 `quality_report`
5. 想知道这次到底跑了什么，看 `run_manifest`
6. 想给别的部门复用，先改目标、费用、账号映射，不要先改代码

---

## 10. 文档语言说明

这份文档以后默认尽量用中文表达。

保留英文的只有三类内容：

1. 文件名
2. 对象名
3. 命令行入口名

原因很简单：这些名字本身就是系统里的正式标识，不能随便翻译。
