# 今日专项归属问题摘要说明

这份说明只回答一件事：

**每天主链跑完后，业务/运营先看哪份 override 摘要，以及怎么决定今天先处理什么。**

## 1. 每天先看哪份文件

固定先看：

- `artifacts/runs/manual_override_daily_digest_{run_id}.json`

这份摘要不是完整问题清单，而是把现有 override issue 压成：

- 今天有没有阻断问题
- 今天最值得处理的 3 条专项归属问题
- 主要影响了哪些账号 / 主播
- 今天更偏累计风险，还是更偏 latest 标签解释风险

如果需要追溯明细，再看：

- `artifacts/runs/manual_override_issue_manifest_{run_id}.json`

## 2. digest 里最重要的几个字段

### `summary_status`

常见值：

- `blocking`
- `warning`
- `info_only`
- `clear`

理解方式：

- `blocking`：今天专项归属有阻断项，正式口径不要直接发
- `warning`：今天没有阻断，但有高优先级修复问题
- `info_only`：今天没有硬错误，主要是解释风险
- `clear`：今天专项归属没有值得处理的问题

### `top_priority_issues`

这是今天最值得先看的 3 条问题。

每条都会告诉你：

- 这是阻断 / 高优先级 / 可延后关注
- 对应哪条 override
- 影响哪个账号 / 主播
- 为什么会被排到前面
- 建议怎么处理

如果是 unmatched，现在会进一步细分成：

- `unmatched_probable_misconfig`
- `unmatched_not_in_current_run`
- `unmatched_outside_effective_window`
- `unmatched_insufficient_locator`
- `unmatched_needs_manual_review`

你可以直接按名字判断：

- `probable_misconfig`：当天优先修
- `not_in_current_run`：通常可延后
- `outside_effective_window`：看是否需要调整生效日期
- `insufficient_locator`：建议补定位键
- `needs_manual_review`：当天人工核实

### `account_impact_summary`

告诉你：

- 今天哪些账号受专项归属影响
- 更偏累计口径影响，还是还要注意 latest 解释

### `host_impact_summary`

告诉你：

- 今天哪些主播受专项归属影响
- 是否需要特别注意 latest 标签解释

### `latest_panel_risk_summary`

这部分只回答一个问题：

**今天解释 latest 面板时，哪些专项归属最容易让人误读。**

## 3. 每天的处理顺序

### 第一步：先看 blocking

如果 `blocking_count > 0`：

- 今天先修专项归属配置
- 不要直接拿正式口径发出去

### 第二步：再看 warning

如果没有 blocking，但 `warning_count > 0`：

- 今天优先处理 warning 项
- 处理完再决定是否继续使用正式专项归属口径

### 第三步：最后看 info

如果只有 `info_only`：

- 今天通常可以继续出数
- 但要注意摘要里提到的 latest 风险和累计解释风险
- 如果 `issue_counts.unmatched_not_in_current_run_count > 0`，通常可以不打断当天出数

## 4. latest 风险和累计风险怎么理解

### 更偏累计风险

意思是：

- 今天 latest 面板不一定直接变
- 但累计线索、累计主播贡献、累计账号表现会被专项归属改变

### 更偏 latest 标签解释风险

意思是：

- 这条专项只命中历史样本
- 但如果今天拿 latest 面板解释主播/账号表现，可能会和累计结果不完全一致

### 同时影响两边

如果摘要写的是：

- “同时影响累计口径和主播 latest 解释”

那今天就应该优先看这条。

## 5. 哪些问题当天必须处理

- blocking 项
- 明显冲突 override
- 明显配置写错
- `unmatched_probable_misconfig`
- `unmatched_insufficient_locator`
- `unmatched_needs_manual_review`

## 6. 哪些问题可以延后

- 历史样本风险
- 只影响累计、不影响今天 latest 的提示
- `unmatched_not_in_current_run`
- `unmatched_outside_effective_window`
- 业务上已明确知道、但今天不需要拿 latest 面板解释的专项

一句话：

**先看 daily digest 决定今天先查什么；需要追根溯源时，再下钻 issue manifest。**
