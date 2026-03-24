# 阶段1 / 任务4：今日最值得处理的专项归属问题摘要

## 本轮目标

把现有 override issue 治理结果压成业务/运营可直接使用的“今日问题摘要”，而不是继续让人翻完整 issue manifest。

## 实际完成项

- 新增正式 `override daily digest` 摘要层
- 摘要会按优先级输出“今天最值得处理的 3 条专项归属问题”
- `historical_only_risk` 已细化为：
  - `historical_account_cumulative_risk`
  - `historical_host_latest_explain_risk`
  - `historical_account_host_risk`
  - `historical_general_risk`
- `run_manifest` 已接入 digest
- `quality_report` 已接入 digest 摘要视图
- 保留原 `manual_override_issue_manifest` 不变，digest 只是其上层摘要

## 新增产物

- `artifacts/runs/manual_override_daily_digest_{run_id}.json`

## 当前摘要能直接回答的问题

- 今天有没有阻断专项归属问题
- 今天最值得先查的 3 条专项归属问题
- 今天主要影响哪些账号 / 主播
- 今天更偏累计口径影响，还是更偏 latest 标签解释风险
- 今天专项归属是否实际改变了最终正式口径

## 本轮验证结果

- unified 主链仍可正常跑通
- digest 已成功落盘并接入：
  - `run_manifest`
  - `quality_report`

## 仍保留的限制

- raw 分析链暂未接入这层摘要
- 历史风险细分目前仍基于目标账号/主播和 latest 日期做规则判断，不是样本级推理
- digest 当前只取 top 3 问题，完整追溯仍需看 issue manifest

## 当前结论

override 机制现在已经不是“能用的覆盖层”而已，而是具备：

- 正式问题清单
- 正式优先级摘要
- 运营可读中文输出
- 主链可追溯接线

可以直接进入日常使用。 
