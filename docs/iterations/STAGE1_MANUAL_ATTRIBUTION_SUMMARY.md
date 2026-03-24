# 阶段1 / 任务2：专项人工确认归属流程化

## 1. 本轮目标

把“业务确认优先于自动归因”的特殊样本正式纳入产品体系，但不污染自动归因主规则。

本轮只做三件事：

- 建正式对象
- 建正式输入
- 让日报 / 分析 / 导出 / 运行清单都能识别这层 override

## 2. 实际完成项

- 新增正式对象 `ManualAttributionOverride`
- 新增固定输入文件 `config/manual_attribution_overrides.csv`
- 新增 override 加载、校验、应用模块
- 保留原始自动归因结果，同时生成最终消费口径
- 日报快照、统一事实分析、Feishu 导出、TSV 校验均已接入 override
- 新增 `manual_override_manifest_{run_id}.json`
- 在 `run_manifest` 和 `quality_report` 中加入专项归属摘要
- 文档化专项人工确认归属的使用方式

## 3. 修改 / 新增文件清单

### 配置

- `/Users/ahs/Desktop/Operations Analytics Engine/config/manual_attribution_overrides.csv`
- `/Users/ahs/Desktop/Operations Analytics Engine/config/input_sources.json`

### 契约

- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/contracts/models.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/contracts/specs.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/contracts/__init__.py`

### override 模块

- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/overrides/__init__.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/overrides/override_loader.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/overrides/override_validator.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/overrides/manual_attribution.py`

### 消费链接线

- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/performance/fact_loader.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/performance/runtime.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/analysis/unified_fact.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/analysis/runtime.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/cli/run_analysis.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/exports/feishu_report.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/exports/feishu_narrative.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/quality/tsv_verify.py`
- `/Users/ahs/Desktop/Operations Analytics Engine/src/oae/jobs/daily_pipeline.py`

### 文档

- `/Users/ahs/Desktop/Operations Analytics Engine/docs/INPUT_GUIDE.md`
- `/Users/ahs/Desktop/Operations Analytics Engine/docs/MANUAL_ATTRIBUTION_GUIDE.md`
- `/Users/ahs/Desktop/Operations Analytics Engine/docs/iterations/STAGE1_MANUAL_ATTRIBUTION_SUMMARY.md`

## 4. 专项人工确认归属如何工作

### 自动归因层

- `fact_attribution.csv` 继续保留原始自动归因结果
- 不回写、不篡改自动归因主规则

### 专项覆盖层

- 固定从 `config/manual_attribution_overrides.csv` 加载
- 通过 `business_subject_key / phone / lead_id` 定位样本
- 允许只补账号、只补主播、或同时补账号+主播

### 最终消费层

日报快照、分析、导出和 TSV 校验都消费“最终口径”：

- 原始自动结果保留为 `自动标准账号 / 自动本场主播 / 自动归属状态 / 自动无匹配原因`
- 最终消费结果保留为 `最终标准账号 / 最终本场主播 / 最终归属状态 / 最终归属来源`
- 工作字段 `标准账号 / 本场主播 / 归属状态 / 无匹配原因` 在消费层被替换为最终口径

## 5. 本次新增输入 / 清单 / 产物

### 新增正式输入

- `/Users/ahs/Desktop/Operations Analytics Engine/config/manual_attribution_overrides.csv`

### 新增运行产物

- `/Users/ahs/Desktop/Operations Analytics Engine/artifacts/runs/manual_override_manifest_run-20260317T125249Z.json`

### 本次样例命中

- `17835126680`
- 原始自动结果：`抖音-星途汽车直营中心 -> 【无主线索】`
- 专项人工确认结果：`抖音-星途汽车直营中心 -> 丁俐佳`

## 6. 主链验证结果

本次已重跑正式主链：

- fact
- snapshot
- ledger
- analysis
- export
- quality
- run manifest

运行产物：

- `/Users/ahs/Desktop/Operations Analytics Engine/artifacts/runs/run_manifest_run-20260317T125249Z.json`
- `/Users/ahs/Desktop/Operations Analytics Engine/artifacts/runs/quality_report_run-20260317T125249Z.json`
- `/Users/ahs/Desktop/Operations Analytics Engine/artifacts/runs/manual_override_manifest_run-20260317T125249Z.json`

关键结果：

- `manual overrides: applied=1, affected_rows=1`
- 日报线索质量口径已显示：`本次专项人工确认归属 1 条，影响样本 1 行`
- `TSV 校验 = PASSED`

## 7. 仍未解决的归属风险

- override 当前仍是按固定文件全量生效，尚未做更细的审批流或多人修改控制
- 如果同一条样本被两条 active override 同时命中，系统会直接报错中止，需要人工清理冲突
- raw-evidence 分析链当前未消费这层 override，本轮只接入了正式 unified 主链
- 主播“归属账号”在日报 latest 表里仍受当前 latest 面板建模方式影响，专项补到历史某天时，累计指标会变，但 latest 行展示的 parent_account 仍可能以当日面板为主

## 8. 下一步建议

只接阶段1下一任务的自然延续，建议做：

1. 把 override 冲突和未命中样本做成更直接的异常提示
2. 明确 raw 分析链是否也需要消费这层 override
3. 如果专项确认开始增多，再补更细的审批/撤销操作约束
