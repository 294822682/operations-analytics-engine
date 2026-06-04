# OAE main cleanup handoff

日期：2026-06-04

## 背景

本轮清理发生在 BI dashboard source 和抖音来客订单接入完成、PR #7 已合并之后。
远端 `origin/main` 最新确认点为：

- `80236cb Merge pull request #7 from 294822682/codex/dashboard-laike-orders-contract`

清理前主工作区混有多类本地状态：

- A：已废弃的本地 ahead 提交和旧 dashboard 修复线。
- B：每次产出的模板、输出、证据和业务交付类文件。
- C：仍可能有价值、但不应继续挂在 `main` 脏工作区里的本地工程改动。
- D：旧治理文档、一次性说明、生成噪声和确认可删除的本地残留。

用户已确认处理原则：

- A 删除。
- B 不要动。
- C 迁移；如果影响链路，再从迁移集中拉回。
- D 删除。

## 已执行动作

1. 恢复清理计划

- 原 `/private/tmp/oae_cleanup_plan_20260603` 和 `/private/tmp/oae_c_migration_20260603` 已不存在。
- 已重建清理计划到 `/private/tmp/oae_cleanup_plan_20260604`。

2. C 迁移

- 已创建迁移 worktree：`/private/tmp/oae_c_migration_20260604`
- 已创建本地分支：`codex/main-dirty-c-migration-20260604`
- 已基于 `origin/main` 迁移非冲突 C 改动。
- 已提交本地迁移保护提交：`10a23ea Preserve migrated main cleanup C changes`
- 该分支未 push。

3. C 链路影响检查

迁移时先发现 4 个冲突文件会影响主链路，未纳入 C 迁移提交：

- `config/monthly_targets.csv`
- `src/oae/exports/feishu_report.py`
- `src/oae/exports/feishu_topline.py`
- `src/oae/jobs/daily_pipeline.py`

随后 targeted tests 继续暴露 3 个 C 迁移引入的导入链路影响文件，也已从 C 迁移提交中拉回：

- `src/oae/exports/feishu_panels.py`
- `src/oae/exports/feishu_table_adapters.py`
- `src/oae/services/__init__.py`

处理原则：以上 7 个文件已视为链路影响文件，保留 `origin/main` 版本或主线缺省状态，不在本轮主清理中拉入 C 迁移。

4. C 迁移验证

在 C 迁移 worktree 中执行过 dashboard targeted tests：

```bash
PYTHONPATH=src /Users/ahs/Desktop/Operations\ Analytics\ Engine/.venv/bin/python -m pytest tests/test_feishu_douyin_laike.py tests/test_feishu_dashboard_source.py tests/api/test_dashboard_daily_api.py tests/test_feishu_dashboard_interactive_html.py -q -p no:cacheprovider
```

最终结果：`70 passed in 2.78s`。

5. 主工作区清理

- 主工作区 `main` 已通过 mixed reset 回到 `origin/main`，不再保留本地 ahead 提交。
- 非 B 的 tracked 改动已恢复到 `origin/main`。
- C 清单中的非冲突本地工程改动已迁移到 C worktree，不再留在主工作区。
- D 清单中的旧治理文档和确认删除项已从主工作区移除。
- 代码目录下的 `__pycache__` 和 `.DS_Store` 生成噪声已清理。

## B 类保留边界

以下路径属于 B 或保护路径，本轮刻意不动：

- `output/`
- `outputs/`
- `artifacts/`
- `源文件/`
- `历史文件/`
- `全量分析/`
- `tests/baseline/reference/`
- `tests/baseline/reference_manifest.json`
- `engines/customer_service_performance_engine/input/`
- `engines/customer_service_performance_engine/output/`
- `final_upload/`

这些路径里仍可能显示为 modified、deleted 或 untracked。不要在没有用户再次明确授权的情况下清理、恢复、删除或提交。

## 当前主工作区预期状态

主工作区应满足：

- `main` 跟踪 `origin/main`，无本地 ahead/behind。
- 非 B 的 tracked 改动为空。
- 本文件 `docs/agent_governance/clean up.md` 是本轮新增治理文档。
- B 类输出、模板、证据仍保持用户现场状态。
- 非本轮计划内的历史包、归档包、legacy 兼容面等不在本轮处理范围内。

## 后续线程规则

后续线程继续处理时，先按以下顺序检查：

1. `git -c core.quotePath=false status --short --branch`
2. `git -C /private/tmp/oae_c_migration_20260604 status --short --branch`
3. `git -C /private/tmp/oae_c_migration_20260604 log --oneline -1`

如果要继续 C：

- 从分支 `codex/main-dirty-c-migration-20260604` 开始。
- 不要直接把上述 7 个链路影响文件合回 main。
- 先重新验证 C 对 dashboard、release gate、daily pipeline 入口的影响。

如果要继续清 B：

- 必须先重新出 dry-run。
- 必须列出每个候选路径、原因、保护路径排除、预计 git status 影响。
- 必须等待用户再次明确批准。

如果要提交本治理文档：

- 只 stage `docs/agent_governance/clean up.md`。
- 不要使用 `git add .`。
- 不要顺手提交 B 类输出、模板或源文件。

## 禁止事项

- 不运行 `python -m oae.jobs.daily_pipeline`。
- 不写真实 `output/`、`outputs/`、`artifacts/`、`源文件/`、`历史文件/`。
- 不删除 B 类产物。
- 不 push C 迁移分支，除非用户明确授权。
- 不 merge C 迁移分支，除非用户明确说 merge。
