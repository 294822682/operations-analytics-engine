# 阶段1 / 任务3 交接综述：override 异常提示与治理

## 1. 本轮目标

把专项人工确认归属从“可用机制”升级成“可治理机制”：

- 冲突不再只是裸报错
- 未命中不再靠人工猜
- 部分生效 / 无效配置 / 风险提示有正式异常对象
- 异常摘要正式进入 `run_manifest / quality_report / 独立异常清单`

## 2. 实际完成项

- 新增正式异常对象 `ManualOverrideIssue`
- 新增契约 `manual_override_issue`
- 新增 override 配置检测入口 `inspect_manual_attribution_overrides`
- 新增 override 应用治理入口 `inspect_manual_override_application`
- 新增正式异常清单：
  - `manual_override_issue_manifest_{run_id}.json`
- 冲突 override 现在会先形成正式 issue，再阻断主链
- 未命中 / 生效区间未覆盖 / 历史样本风险 / 部分生效 都会生成中文 issue
- `quality_report` 已新增 `override_issue_summary`
- `run_manifest` 已新增 `manual_override_issues`

## 3. 修改/新增文件清单

### 契约层

- `src/oae/contracts/models.py`
- `src/oae/contracts/specs.py`
- `src/oae/contracts/schemas/manual_override_issue.schema.json`

### override 治理层

- `src/oae/overrides/override_validator.py`
- `src/oae/overrides/override_loader.py`
- `src/oae/overrides/manual_attribution.py`
- `src/oae/overrides/__init__.py`

### 作业与质量

- `src/oae/jobs/daily_pipeline.py`
- `src/oae/quality/reports.py`

### 文档

- `docs/MANUAL_ATTRIBUTION_GUIDE.md`
- `docs/OVERRIDE_EXCEPTION_GUIDE.md`
- `docs/iterations/STAGE1_OVERRIDE_GOVERNANCE_SUMMARY.md`

## 4. 本轮治理后的异常分层

### blocking

- 文件缺失
- 文件缺字段
- 必填字段缺失
- 生效日期非法
- `metric_version` 与当前事实层口径不一致
- 多条 active override 命中同一样本

### warning

- 当前主链找不到样本
- 声明为 `account_host` 但只填了一侧目标

### info

- status 为空，系统按 active 处理
- 样本不在生效区间
- 只命中历史样本，会影响累计但不一定影响 latest 面板解释

## 5. 新增产物

本轮正式新增：

- `artifacts/runs/manual_override_issue_manifest_{run_id}.json`

其中至少包含：

- `issue_summary`
- `issues`
- `validation_summary`
- `runtime_summary`

## 6. 主链运行状态

本轮验证运行：

- `run_manifest_run-20260320T094618Z.json`
- `quality_report_run-20260320T094618Z.json`
- `manual_override_manifest_run-20260320T094618Z.json`
- `manual_override_issue_manifest_run-20260320T094618Z.json`

结果：

- 主链跑通
- `TSV = PASSED`
- 当前 override 治理结果：
  - `issue_count = 3`
  - `blocking_count = 0`
  - `warning_count = 0`
  - `info_count = 3`
  - `risk_count = 3`

这 3 条都是“历史样本风险提示”，不是阻断错误。

## 7. 当前仍未解决的风险

- raw-evidence 分析链还没有消费同一套 override 异常治理结果
- 当前 `historical_only_risk` 只做了“历史样本影响累计”的提示，还没细分到“会不会影响账号 latest 标签 / 主播 latest 标签”
- 未命中项现在已正式清单化，但还没进一步区分“本轮不存在”与“配置很可能写错”的更细判断

## 8. 下一步建议

只接阶段1下一任务的自然延续，建议：

- 把 override 异常再压成“今日最值得处理的专项归属问题”摘要
- 或继续把 raw 分析链接入同一套 override 治理层
