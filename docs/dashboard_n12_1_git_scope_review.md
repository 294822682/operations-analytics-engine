# N12.1 UI Git Scope And Prerelease Review

生成日期：2026-05-28

## 1. Git 状态总览

本轮任务为 `N14-N12-UI-GIT-SCOPE-AND-PRERELEASE-REVIEW`，仅做审查、归类、报告和提交建议；未 stage、未 commit、未发布。

已执行只读 Git 命令：

- `git status --short`
- `git status --short | wc -l`
- `git diff --stat`
- `git diff --name-only`
- `git ls-files --others --exclude-standard`
- `git ls-files --stage`
- `git diff --cached --name-only`

状态统计：

- `git status --short`：236 行。
- tracked dirty status 行：84。
- untracked status 行：152。
- `git ls-files --others --exclude-standard` 展开后未跟踪文件：310 个。
- staged status 行：24。
- `git diff --cached --name-only`：24 个 staged 路径。
- `git diff --stat`：60 个 tracked 文件变更，约 138,797 insertions / 39,340 deletions。

结论：当前仓库不是干净工作区，且 index 中已有 unrelated staged 文件。进入 N12.1 提交前，必须人工确认 index，只 stage 明确文件，不能直接 commit 当前 index。

## 2. N13 基准文件核查

`docs/dashboard_n12_1_ui_acceptance_baseline.md` 已存在，并包含：

- N12.1 UI 交互阶段基准结论。
- 唯一验收端口和 URL。
- 测试命令和结果。
- 浏览器 DOM 验收摘要。
- 响应式验收摘要。
- 数据原则确认。
- 账号模块、主播模块、种草模块验收摘要。
- 截图路径。
- 修改文件清单。
- dirty / untracked 状态说明。
- 未运行 `daily_pipeline`。
- 未 commit / push / tag。
- 不作为正式发布说明，只作为阶段基准。

本轮无需修正 N13 基准文档内容。

## 3. N12.1 相关文件分类

| 文件 | 当前状态 | tracked 情况 | 行数 | 大小 | sha256 前缀 | 是否属 N12.1 | 建议 |
|---|---:|---:|---:|---:|---:|---|---|
| `src/oae/exports/feishu_dashboard_interactive_html.py` | `??` | 当前未跟踪，未发现同名 tracked index 记录 | 6255 | 223,221 B | `4c34a3d5f3a80e34` | 是，UI 实现 | 人工确认后纳入 |
| `tests/test_feishu_dashboard_interactive_html.py` | `??` | 当前未跟踪，未发现同名 tracked index 记录 | 1107 | 49,150 B | `ec4603148792e57f` | 是，focused HTML 测试 | 人工确认后纳入 |
| `tests/api/test_dashboard_daily_api.py` | `??` | 当前未跟踪，位于未跟踪 `tests/api/` 目录内，未发现同名 tracked index 记录 | 1363 | 56,136 B | `986ead10ddb068f6` | 是，API HTML / 趋势范围测试 | 人工确认后纳入 exact file，不要 stage 整个 `tests/api/` |
| `docs/dashboard_ui_benchmark.md` | `??` | 当前未跟踪，未发现同名 tracked index 记录 | 281 | 21,732 B | `a8462ea088487945` | 是，N12.1 benchmark / design boundary | 人工确认后纳入 |
| `docs/dashboard_business_metric_map.md` | `??` | 当前未跟踪，未发现同名 tracked index 记录 | 67 | 7,974 B | `6b9c00840d0c1629` | 是，数据原则和指标口径映射 | 人工确认后纳入 |
| `docs/dashboard_n12_1_ui_acceptance_baseline.md` | `??` | 当前未跟踪，未发现同名 tracked index 记录 | 159 | 5,794 B | `ba3a62d53d82c6e4` | 是，N13 冻结基准 | 人工确认后纳入 |
| `docs/dashboard_n12_1_git_scope_review.md` | 新增于 N14 | 当前未跟踪 | 本文件 | 本文件 | 本文件 | 是，N14 审查证据 | 建议随审查证据纳入 |

说明：

- 上述 `??` 表示当前仓库没有跟踪这些路径；不是 tracked 文件的普通修改。
- N14 本轮只新增 `docs/dashboard_n12_1_git_scope_review.md`。
- 其他 N12.1 相关 `??` 文件在 N14 开始前已经存在，需人工确认来源后再 stage。
- 未发现候选路径存在同名 tracked 文件被误复制覆盖的问题。

## 4. 源码一致性核查

`src/oae/exports/feishu_dashboard_interactive_html.py` 已只读核查。

确认存在：

- N12.1-A：`trend-filter-toolbar`、`kpi-card`、`kpi-card-help`、成本比值文案、指标说明、92 天日期范围规则。
- N12.1-B：`trend-panel`、`monthly-matrix`、`monthly-comparison`、`monthly-month-coverage`、`chart-tooltip`、`chart-legend`、`chart-axis`。
- N12.1-C1：`account-toolbar`、`account-search-input`、`account-sort-select`、`account-filter-chip`、`account-clear-filters`、`account-filter-summary`、`account-detail-panel`、`featured-account-card`、`HIDDEN_ACCOUNT_NAMES`、当前账号列表不因高比例数据进行隐藏。
- N12.1-C2：`anchor-toolbar`、`anchor-search-input`、`anchor-sort-select`、`anchor-filter-chip`、`anchor-clear-filters`、`anchor-filter-summary`、`anchor-detail-panel`、`anchor-card`。
- N12.1-C3：`seed-toolbar`、`seed-search-input`、`seed-sort-select`、`seed-filter-chip`、`seed-clear-filters`、`seed-filter-summary`、`seed-detail-panel`、`seed-account-card`。
- DOM 清洁：`chart-tooltip` 和 KPI tooltip 默认隐藏；account trend pane 使用单 active panel 懒渲染；不存在旧 `data-account-trend-pane="leads"` / `data-account-trend-pane="deals"` 双 pane 预渲染污染。

补充说明：源码中未使用 `MAX_TREND_RANGE_DAYS` 常量名；92 天规则以 JS `days > 92` 和页面文案 / API 测试共同体现。

## 5. 测试一致性核查

`tests/test_feishu_dashboard_interactive_html.py` 与 `tests/api/test_dashboard_daily_api.py` 已只读核查。

确认覆盖：

- N12.1-A 设计 token / 筛选栏 / KPI / tooltip。
- N12.1-B `trend-panel` / `monthly-matrix`。
- N12.1-C1 account toolbar / 搜索 / 排序 / 筛选 / 展开。
- N12.1-C2 anchor toolbar / 搜索 / 排序 / 筛选 / 展开。
- N12.1-C3 seed toolbar / 搜索 / 排序 / 筛选 / 展开。
- 高比例真实展示，不以“口径待确认”替换。
- DOM 文本粘连禁用词。
- 原始字段正文不泄露。
- 响应式 CSS 标记由源码 HTML 测试覆盖；API prototype 测试覆盖只读 HTML 基线。

## 6. 测试结果

执行命令：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_feishu_dashboard_interactive_html.py tests/api/test_dashboard_daily_api.py -q
```

结果：

```text
60 passed in 5.31s
```

执行命令：

```bash
git diff --check -- src/oae/exports/feishu_dashboard_interactive_html.py tests/test_feishu_dashboard_interactive_html.py tests/api/test_dashboard_daily_api.py docs/dashboard_ui_benchmark.md docs/dashboard_business_metric_map.md docs/dashboard_n12_1_ui_acceptance_baseline.md
```

结果：exit 0，无输出。

注意：上述 N12.1 候选文件当前都是 untracked，`git diff --check -- <path>` 不会检查未跟踪文件内容，只能证明 tracked diff 中没有这些路径的 whitespace error。提交前建议人工 stage 后再跑一次同命令或 `git diff --cached --check -- <exact files>`。

## 7. Protected Path 检查结果

候选 N12.1 提交范围不包含 protected path。

当前全仓状态中存在 protected path 变更，全部列为 unrelated / excluded，不建议纳入 N12.1 提交：

- `artifacts/snapshots/daily_performance_snapshot_2026-03.csv`
- `output/fact_attribution.csv`
- `output/fact_attribution.xlsx`
- `output/host_counts_weighted.csv`
- `output/host_counts_weighted.xlsx`
- `output/lead_daily.db`
- `output/lead_daily.db-shm`
- `output/lead_daily.db-wal`
- `output/sql_reports/daily_goal_account_2026-03.csv`
- `output/sql_reports/daily_goal_anchor_2026-03.csv`
- `源文件/.DS_Store`
- `源文件/2026年直播进度表3月.xlsx`
- `源文件/总部新媒体成交2026-03-24.csv`
- `源文件/总部新媒体线索2026-03-24.csv`

不得纳入提交：

- `output/`
- `outputs/`
- `artifacts/`
- `final_upload/`
- `源文件/`
- `历史文件/`
- `legacy/`
- `old/`
- `.env`
- SSH keys / credentials / secrets
- 大型原始数据文件
- `/tmp` 截图
- `.playwright-cli/`

## 8. 建议提交范围

### A. 建议纳入 N12.1 UI 基准提交

人工确认后建议纳入：

- `src/oae/exports/feishu_dashboard_interactive_html.py`
- `tests/test_feishu_dashboard_interactive_html.py`
- `tests/api/test_dashboard_daily_api.py`
- `docs/dashboard_ui_benchmark.md`
- `docs/dashboard_business_metric_map.md`
- `docs/dashboard_n12_1_ui_acceptance_baseline.md`
- `docs/dashboard_n12_1_git_scope_review.md`

### B. 建议排除

- 所有 protected path 变更。
- 既有 unrelated staged 文件，例如 `.github/workflows/release-gates.yml`、`Makefile`、`requirements-dev.txt`、`src/oae/cli/run_release_gates.py`、`src/oae/services/release_gate_logic.py` 等。
- 其他 unrelated docs、engines、scripts、service、schema、review、pilot、seed_live、transform、fixtures、baseline reference 等未跟踪文件。
- `/tmp` 临时清单、截图和 patch 预览。

### C. 需要人工确认

- 所有当前显示 `??` 但路径像正式源码的 N12.1 文件。
- `tests/api/test_dashboard_daily_api.py`：属于未跟踪 `tests/api/` 目录，必须 exact-file stage，不能使用 `git add tests/api/`。
- `src/oae/api/`、`src/oae/services/dashboard_daily_service.py`、`src/oae/schemas/dashboard.py` 等未跟踪正式源码路径不属于本次 N12.1 UI 提交建议范围，除非另有阶段证据证明它们属于前置 API / 服务层基线。
- 当前已有 24 个 staged unrelated 路径；任何提交前必须先人工处理 index。

## 9. Commit 策略建议

不建议直接 commit 当前状态。

推荐方案：拆分 commit。

原因：

- 当前工作区和 index 都很脏。
- N12.1 候选文件均为 untracked，需要人工确认来源。
- docs / UI / tests 分开后更容易 code review 和回滚。

建议拆分：

1. docs
   - `docs/dashboard_ui_benchmark.md`
   - `docs/dashboard_business_metric_map.md`
   - `docs/dashboard_n12_1_ui_acceptance_baseline.md`
   - `docs/dashboard_n12_1_git_scope_review.md`
   - message: `docs(dashboard): record N12.1 UI benchmark and prerelease review`

2. UI implementation
   - `src/oae/exports/feishu_dashboard_interactive_html.py`
   - message: `feat(dashboard): add interactive trend dashboard module controls`

3. tests
   - `tests/test_feishu_dashboard_interactive_html.py`
   - `tests/api/test_dashboard_daily_api.py`
   - message: `test(dashboard): cover N12.1 trend dashboard UI contracts`

如果业务上希望把 N12.1 作为一个完整阶段原子提交，可使用单 commit：

```text
feat(dashboard): add N12.1 interactive business BI UI baseline
```

但只应在 index 清空 unrelated staged 文件、并 exact-file stage 上述候选文件后使用。

## 10. Rollback 建议

仅给方案，不执行。

1. 如果已 stage 但未 commit：
   - 对误 stage 路径使用 `git restore --staged <path>`。
   - 不要使用 `git reset --hard`。

2. 如果已 commit 但未 push：
   - 优先用 `git revert <commit>` 生成反向提交，保留审计链。
   - 如只是整理本地提交，可用 `git reset --soft HEAD~1` 保留改动，或 `git reset --mixed HEAD~1` 退回未 staged 状态。

3. 如果页面需要回退到 N11 / N12 前：
   - 重点涉及 `src/oae/exports/feishu_dashboard_interactive_html.py`、`tests/test_feishu_dashboard_interactive_html.py`、`tests/api/test_dashboard_daily_api.py`。
   - 文档可按需要保留为历史基准，不必随 UI 一起回退。

4. 如何保留 docs 基准但回退 UI：
   - 回退 UI 实现和测试文件。
   - 保留 `docs/dashboard_ui_benchmark.md`、`docs/dashboard_business_metric_map.md`、`docs/dashboard_n12_1_ui_acceptance_baseline.md`、`docs/dashboard_n12_1_git_scope_review.md` 作为审查记录。

5. 回滚后验证：
   - 重新运行 focused tests。
   - 重新运行 `git diff --check` 或 `git diff --cached --check`。
   - 如涉及页面行为，重新启动唯一 preview 端口做真实 DOM 检查。

## 11. 发布前评审 Checklist

- 业务负责人确认指标口径。
- 确认超过 100% 比率如实展示原则。
- 确认隐藏名单说明。
- 确认 92 天日期范围。
- 确认移动端展示。
- 确认不接飞书 API、不入库、不发布。
- 确认 `daily_pipeline` 未运行。
- 确认无 protected path 误提交。
- 确认 focused tests 通过。
- 确认截图路径和基准文档。
- 确认当前 24 个 unrelated staged 文件不会混入 N12.1 提交。

## 12. 本轮边界确认

- 未修改业务代码。
- 未继续 UI polish。
- 未新增功能。
- 未改 KPI 口径。
- 未改服务层聚合逻辑。
- 未新增 API 字段。
- 未运行 `daily_pipeline`。
- 未写入 `output/`、`outputs/`、`artifacts/`、`源文件/`、`历史文件/`。
- 未 stage、未 commit、未 push、未 merge、未 tag。
