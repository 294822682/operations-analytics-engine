# 阶段1 / 任务5：unmatched_override 细分治理

## 本轮目标

把原来单一的 `unmatched_override` 拆成更可操作的子类型，让业务/运营能直接区分：

- 当天必须修
- 可以延后
- 需要人工核实

## 实际完成项

- `unmatched_override` 已细分为：
  - `unmatched_not_in_current_run`
  - `unmatched_probable_misconfig`
  - `unmatched_outside_effective_window`
  - `unmatched_insufficient_locator`
  - `unmatched_needs_manual_review`
- `issue_manifest` 已输出这些子类型的正式计数
- `daily_digest` 已按不同 unmatched 子类型给出不同优先级和建议动作
- `quality_report / run_manifest` 已接入 unmatched 子类型计数

## 当前解释规则

- `unmatched_not_in_current_run`
  - 本轮大概率本来就没有这条业务样本
  - 通常可以延后，不属于当天必须处理
- `unmatched_probable_misconfig`
  - 高概率配置误填
  - 建议当天优先修
- `unmatched_outside_effective_window`
  - 样本存在，但不在生效区间
  - 是否处理取决于本轮是否也要生效
- `unmatched_insufficient_locator`
  - 定位键不足，无法可靠命中
  - 建议当天补充手机号或唯一线索ID
- `unmatched_needs_manual_review`
  - 系统无法自动判定
  - 建议当天人工核实

## 本轮验证结果

- unified 主链已回归通过
- 当前运行中没有 unmatched 样本，因此新子类型计数均为 0
- 这说明：
  - 新字段已经稳定接线
  - 不是没实现，而是当前样本集里确实没有 unmatched 问题

## 当前产物

- `artifacts/runs/manual_override_issue_manifest_{run_id}.json`
- `artifacts/runs/manual_override_daily_digest_{run_id}.json`
- `artifacts/runs/run_manifest_{run_id}.json`
- `artifacts/runs/quality_report_{run_id}.json`

## 当前限制

- 当前 unmatched 细分仍基于规则判断，不是样本级智能推理
- raw 链还没有接这套 unmatched 细分治理

## 当前结论

override 治理层现在已经能明确区分：

- 本轮本来不存在
- 高概率误填
- 生效期之外
- 定位键不足
- 需要人工复核

已经可以支持日常运营判断。 
