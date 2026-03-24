# 专项人工确认归属说明

这套机制是正式主链上的“覆盖层”，专门处理下面这类情况：

- 系统自动归因判成无主，但业务上已经确认应该归给某个账号/主播
- 延迟核销、延迟入库，导致自动直播时间窗没挂上
- 主播自己挖潜，业务确认归属明确
- 特定渠道样本业务上明确归属，但不适合改脏主规则

它的核心原则只有一句话：

**专项人工确认归属优先于自动归因结果，但不会回写污染事实层主规则。**

## 1. 什么情况可以用

可以用的场景：

- 业务方已经明确确认归属
- 自动结果不符合业务事实
- 这是专项例外，不适合写进全局主规则
- 需要把这条样本正式纳入日报/复盘口径

典型例子：

- 抖音来客直播延迟核销
- 主播自己挖潜
- 系统自动显示“无主”，但业务知道这条线索是谁带来的

## 2. 什么情况不能用

下面这些情况不要直接走专项人工确认归属：

- 只是“感觉像是这位主播的”，但没有业务确认
- 想用专项确认去替代主规则长期治理
- 想批量修正一整类普遍问题，但规则边界还没想清楚
- 还没确认账号/主播，只是先想把无主消掉

一句话：

**专项人工确认归属只能处理“业务已经拍板”的例外，不能拿来代替主规则。**

## 3. 正式输入文件在哪里

固定文件：

- `/Users/ahs/Desktop/Operations Analytics Engine/config/manual_attribution_overrides.csv`

这是正式输入，不参与“自动找最新文件”，只认这一份固定路径。

## 4. 要填哪些字段

正式字段如下：

- `override_id`
- `business_subject_key`
- `phone`
- `lead_id`
- `override_scope`
- `target_account`
- `target_host`
- `reason`
- `evidence_note`
- `confirmed_by`
- `confirmed_at`
- `effective_from`
- `effective_to`
- `status`
- `metric_version`
- `run_id`

## 5. 字段怎么理解

### 定位样本

下面三个字段至少填一个：

- `business_subject_key`
- `phone`
- `lead_id`

如果填多个，系统会按“同时满足”来匹配，避免误命中。

### 归属目标

下面两个字段至少填一个：

- `target_account`
- `target_host`

常见写法：

- 只改主播：`target_host` 填，`target_account` 留空
- 只改账号：`target_account` 填，`target_host` 留空
- 账号和主播都改：两个都填

### 归属范围

`override_scope` 常见值：

- `account`
- `host`
- `account_host`
- `channel`
- `other`

如果你没填，系统会自动按目标字段推断：

- 只填账号 -> `account`
- 只填主播 -> `host`
- 账号和主播都填 -> `account_host`

### 业务说明

- `reason`：为什么要做这条专项确认
- `evidence_note`：依据是什么
- `confirmed_by`：谁确认的
- `confirmed_at`：什么时候确认的

这几项是追溯信息，不要省。

### 生效时间

- `effective_from`
- `effective_to`

不填就表示长期有效。  
如果是某个日期范围内的专项归属，可以填生效区间。

### 状态

`status` 可选值：

- `active`：生效
- `inactive`：暂不生效
- `revoked`：已撤销

## 6. 一条最常见的填写示例

```csv
override_id,business_subject_key,phone,lead_id,override_scope,target_account,target_host,reason,evidence_note,confirmed_by,confirmed_at,effective_from,effective_to,status,metric_version,run_id
manual-override-20260315-17835126680,PHONE:17835126680,17835126680,ID2033132100202205186,account_host,抖音-星途汽车直营中心,丁俐佳,主播自己挖潜,抖音来客直播延迟核销后业务确认应归属本场,业务人工确认,2026-03-17 12:00:00,2026-03-15,,active,metric-v1,
```

## 7. 它是怎么生效的

主链关系是：

1. 自动归因先照常生成事实层
2. 专项人工确认归属作为覆盖层加载
3. 日报快照 / 分析 / 导出在消费事实层时叠加这层覆盖

所以：

- `fact_attribution.csv` 保留原始自动归因结果
- 日报、分析、导出看到的是“最终消费口径”
- 两套结果都可追溯

## 8. 系统里会保留哪些痕迹

每次跑链后，系统都会额外生成：

- `artifacts/runs/manual_override_manifest_{run_id}.json`
- `artifacts/runs/manual_override_issue_manifest_{run_id}.json`

里面会记录：

- 本次配置了多少条专项确认
- 生效了多少条
- 命中了哪些样本
- 原始自动归因是什么
- 最终消费口径改成了什么
- 影响了哪些账号/主播

同时还会写入：

- `run_manifest`
- `quality_report`

所以后面追溯时，不需要再靠口头回忆。

## 9. 哪些异常会阻断，哪些只提示

### 会阻断正式口径的情况

- override 文件不存在
- override 文件缺字段
- active override 缺少定位字段
- active override 缺少目标账号/目标主播
- active override 缺少 `override_id / reason / confirmed_by / confirmed_at / metric_version`
- 日期字段写错
- `effective_to` 早于 `effective_from`
- `metric_version` 与当前事实层口径不一致
- 多条 active override 同时命中同一样本，且目标不唯一

这些情况会在主链里前置拦住，并生成：

- `manual_override_issue_manifest_{run_id}.json`

### 只提示、不阻断的情况

- 当前主链找不到这条 override 对应样本
- 样本日期不在当前生效区间
- 只覆盖账号、主播仍沿用自动归因
- 只覆盖主播、账号仍沿用自动归因
- 只命中历史样本，会影响累计但不一定影响当日 latest 面板解释

这些会进入异常清单和质量摘要，但不会把主链直接打断。

## 10. 如何撤销

最简单的方式是把对应行的 `status` 改成：

- `inactive`
- 或 `revoked`

这样这条记录就不会继续作用于最终消费口径。

## 11. override 异常怎么排查

### 冲突 override

表现：

- 系统提示“多条 active override 同时命中同一样本”

优先排查：

- 是否同一个手机号/唯一线索ID被配了两条 active override
- 是否一条按手机号配、一条按 lead_id 配，但其实命中的是同一条业务样本
- 是否旧专项没撤销，又新加了一条

建议动作：

- 保留唯一有效的一条
- 其余改成 `inactive` 或 `revoked`
- 或把定位条件写得更细

### 未命中 override

表现：

- 系统提示“当前主链找不到这条 override 对应样本”

优先排查：

- 手机号是否写错
- 唯一线索ID 是否写错
- 本轮数据日期里是否本来就没有这条样本
- 是否其实属于历史样本

建议动作：

- 如果本轮本来就不存在，可先不处理
- 如果本轮应该存在但没命中，优先修手机号 / lead_id / business_subject_key

### 历史样本风险提示

表现：

- 系统提示“只命中历史样本，会影响累计结果，但不一定改变今日 latest 面板标签解释”

这不是错误，更像解释提示。

适用处理：

- 如果你只关心累计口径，可以不处理
- 如果你还要解释“今天这条为什么没显示在 latest 面板”，就需要补看最新日期是否也要确认归属

## 12. 和主规则是什么关系

请始终记住：

- 主规则是系统默认底座
- 专项人工确认归属只是覆盖层
- 覆盖层处理的是例外，不是通用逻辑

所以正确用法是：

- 普遍问题 -> 未来再考虑主规则治理
- 个别已确认例外 -> 走专项人工确认归属
