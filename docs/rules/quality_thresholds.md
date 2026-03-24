# Quality Thresholds

阈值配置文件位置：

- `config/quality_thresholds.json`

当前阈值按 profile 分层：

- `regression`
  - 用于 baseline / 回归验收
  - 最严格，优先发现结果漂移
- `operational`
  - 用于日常运营监控
  - 允许一定业务自然波动
- `settlement`
  - 用于冻结/结算场景
  - 对台账与关键经营指标更保守

每个 profile 下仍分为四组：

- `fact`
- `snapshot`
- `ledger`
- `analysis`

字段说明：

- `mode=relative`
  - 以相对变化率判断，例如 `abs(current-baseline)/baseline`
- `mode=absolute`
  - 以绝对变化值判断
- `mode=minimum`
  - 用于最小值门槛，低于门槛时触发

使用方式：

- 日常调整阈值时，优先修改 `config/quality_thresholds.json`
- 不要直接修改 `src/oae/quality/business.py`

选择方式：

- `python -m oae.jobs.daily_pipeline --quality-threshold-profile operational`
- `python -m oae.jobs.daily_pipeline --quality-threshold-profile regression`
- `python -m oae.jobs.daily_pipeline --quality-threshold-profile settlement`

报告中会记录：

- `threshold_profile`
- `threshold_source`
- `threshold_rule`

当前主要门槛覆盖：

- 归因率波动
- 无主占比波动
- leads / deals / spend 波动
- ledger 唯一性 / 空值 / 对账风险
- analysis 主题完整性和最小行数

说明：

- 如果是结构变化但核心指标稳定，质量报告会尽量落入 `safe_changes`
- 如果是超出配置阈值的经营指标变化，会进入 `threshold_breaches` 和 `key_alerts / attention_items`
