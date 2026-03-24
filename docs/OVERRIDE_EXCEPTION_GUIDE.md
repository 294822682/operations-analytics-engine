# 专项人工确认归属异常清单说明

这份说明只回答一件事：

**当专项人工确认归属出现异常时，系统会怎么提示，你该怎么看，怎么处理。**

## 1. 正式异常产物在哪里

每次主链跑完后，都会生成：

- `artifacts/runs/manual_override_issue_manifest_{run_id}.json`
- `artifacts/runs/manual_override_daily_digest_{run_id}.json`

这份清单会记录：

- 本次专项归属异常总数
- 阻断数 / 警告数 / 提示数
- 冲突数
- 未命中数
- 部分生效数
- 无效配置数
- 风险提示数
- 每条异常的中文说明和建议动作

同时摘要也会写进：

- `run_manifest`
- `quality_report`

日常使用时，建议先看：

1. `manual_override_daily_digest_{run_id}.json`
2. 再看 `manual_override_issue_manifest_{run_id}.json`

也就是：

- `daily_digest` 负责告诉你今天先查什么
- `issue_manifest` 负责给你完整明细和追溯信息

## 2. 严重级别怎么理解

### blocking

会阻断本次正式口径。

典型情况：

- override 文件缺失
- override 文件缺字段
- active override 缺少必填字段
- 生效日期非法
- 多条 active override 同时命中同一样本
- metric_version 与当前事实层口径不一致

### warning

不会直接阻断，但应优先排查。

典型情况：

- 当前主链找不到对应样本
- 明明写成 account_host，但只填了账号或只填了主播

### info

属于解释提示，不一定要处理。

典型情况：

- status 为空，系统自动按 active 处理
- override 只命中历史样本，会影响累计但不一定影响 latest 面板解释
- 本轮样本不在生效区间内

## 3. 常见 issue_type 说明

### conflict_override

含义：

- 多条 active override 同时命中同一样本

建议：

- 只保留唯一有效的一条
- 其他改成 `inactive` 或 `revoked`
- 或者把定位条件写得更细

### unmatched_probable_misconfig

含义：

- 当前未命中，但系统判断这条 override 很可能是配置误填
- 常见原因是手机号 / 唯一线索ID / business_subject_key 其中一项写错

建议：

- 这是当天优先级最高的 unmatched
- 优先修配置，再重跑正式口径

### unmatched_not_in_current_run

含义：

- 定位字段格式正常
- 当前主链里大概率本来就没有这条业务样本

建议：

- 如果这是历史样本或未来样本，可先忽略
- 一般不属于当天必须处理问题

### unmatched_outside_effective_window

含义：

- 当前主链里能找到对应样本
- 但样本日期不在 `effective_from / effective_to` 生效区间内

建议：

- 如果本轮也要生效，就调整生效区间
- 如果本来就是历史专项或过期专项，可以先不处理

### unmatched_insufficient_locator

含义：

- 当前只提供了不足以可靠命中的定位键
- 典型情况是只填 `business_subject_key`

建议：

- 这类问题建议当天补手机号或唯一线索ID
- 不然系统很难判断是样本不存在还是配置写错

### unmatched_needs_manual_review

含义：

- 当前未命中
- 系统无法自动判断是“本轮不存在”还是“配置有误”

建议：

- 当天人工核实
- 优先回看登记表和业务确认记录

### unmatched_override（旧概念）

旧的 `unmatched_override` 现在已经拆成：

- `unmatched_probable_misconfig`
- `unmatched_not_in_current_run`
- `unmatched_outside_effective_window`
- `unmatched_insufficient_locator`
- `unmatched_needs_manual_review`

以前它的含义是：

- override 文件里有这条配置，但当前主链找不到对应样本

建议：

- 现在不要再笼统理解成一类问题，而是看它属于上面哪个细分类别

### partial_apply

含义：

- 只覆盖账号，主播仍沿用自动归因
- 或只覆盖主播，账号仍沿用自动归因

建议：

- 如果业务上另一侧也已经明确，就补齐
- 如果本来就是只想改一侧，可以保留

### historical_account_cumulative_risk

含义：

- override 只命中历史样本
- 更偏账号累计口径影响
- 对今天 latest 标签影响有限

建议：

- 如果今天主要看累计结果，可以延后处理
- 如果今天要解释账号累计变化，建议核最新日期是否也需要补专项归属

### historical_host_latest_explain_risk

含义：

- override 只命中历史样本
- 更偏主播累计与 latest 标签解释风险

建议：

- 如果今天要解释主播 latest 面板，建议优先复核

### historical_account_host_risk

含义：

- override 只命中历史样本
- 同时影响累计口径和主播 latest 解释

建议：

- 这是当前最值得在 daily digest 里优先看的 info 风险

### historical_general_risk

含义：

- override 只命中历史样本
- 当前还无法明确更偏账号累计影响还是主播 latest 风险

建议：

- 先按一般性历史风险看待
- 如果业务上要解释今天面板，建议补充更明确的账号/主播目标

### metric_version_mismatch

含义：

- override 行上的 `metric_version` 与当前事实层口径不一致

建议：

- 先确认这条专项是否仍适用
- 旧口径遗留的 override 不要直接带进新口径

## 4. 业务上怎么用这份清单

最简单的方法：

1. 先看 `manual_override_daily_digest`
2. 再看 `blocking_count / warning_count`
3. 最后才下钻 `manual_override_issue_manifest`

判断优先级：

- `blocking_count > 0`
  - 本次正式口径不要直接发
- `warning_count > 0`
  - 可以继续出数，但最好先看清楚是否会影响解释
- 只有 `info`
  - 说明当前更多是解释提示，不是系统错误

## 5. 哪些情况可以不处理

下面这些不一定需要动作：

- 历史样本风险提示
- `unmatched_not_in_current_run`
- `unmatched_outside_effective_window`
- 业务上本来就只想改账号、不想改主播的 partial_apply

## 6. 哪些情况必须处理

- 冲突 override
- `unmatched_probable_misconfig`
- `unmatched_insufficient_locator`
- `unmatched_needs_manual_review`
- 文件缺失 / 缺字段
- 定位字段缺失
- 目标字段缺失
- 生效日期写错
- metric_version 不一致

一句话：

**能阻断正式口径的先处理；只影响解释、不影响结果的可以后处理。**
