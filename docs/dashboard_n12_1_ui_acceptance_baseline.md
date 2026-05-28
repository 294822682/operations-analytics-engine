# N12.1 UI 交互阶段验收基准

生成日期：2026-05-28

## 1. 阶段结论

N12.1 UI 交互阶段已完成基准核验，可作为后续提交、归档或发布前评审的 UI 交互验收基准。

本文件仅冻结 N12.1-A / B / C / D 的页面 UI、真实 DOM、模块交互、响应式和文本清洁验收结果；不作为正式发布说明。

## 2. 唯一验收端口和 URL

- 旧端口检查范围：8015、8016、8017、8018、8019。
- 检查结果：上述端口均无监听进程，未关闭任何旧服务。
- 唯一验收端口：8020。
- 唯一验收 URL：http://127.0.0.1:8020/dashboard/daily/trends/prototype?end_date=2026-05-22
- 旧端口不作为本次验收依据。

## 3. 测试命令和结果

命令：

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_feishu_dashboard_interactive_html.py tests/api/test_dashboard_daily_api.py -q
```

结果：60 passed in 6.38s。

## 4. 浏览器 DOM 验收摘要

真实浏览器 URL：http://127.0.0.1:8020/dashboard/daily/trends/prototype?end_date=2026-05-22

- 页面标题：经营趋势看板。
- start input value：2026-03-01。
- end input value：2026-05-22。
- 当前范围：2026-03-01 至 2026-05-22。
- 查看天数：83 / 92。
- 核心 KPI 卡片：6 张。
- 历史趋势 `trend-panel`：存在。
- 月度对比 `monthly-matrix`：存在。
- 车型结构：存在。
- `account-toolbar`、`anchor-toolbar`、`seed-toolbar`：均存在。
- 底部指标说明：存在。
- console：0 errors，0 warnings。

## 5. 响应式验收摘要

已检查宽度：1440px、1280px、1024px、768px、390px。

每个宽度均确认：

- body 无横向溢出。
- 顶部筛选区不溢出。
- 日期 input 可操作。
- 快捷筛选按钮不重叠。
- KPI 6 张卡正常换行。
- 历史趋势图不横向撑破页面。
- 月度对比可用。
- 账号 / 主播 / 种草工具栏不重叠。
- 搜索框和排序控件可操作。
- 卡片文字无明显溢出。
- 展开详情不撑破页面。
- tooltip 节点存在，默认隐藏。

## 6. 数据原则确认

- 未修改 KPI 口径。
- 未修改指标公式。
- 未修改服务层聚合逻辑。
- 未新增 API 字段。
- 超过 100% 的真实比例如实显示。
- 当前页面保留真实高比例值，包括 333.33%、3,300.00% 以及多个种草当前 / 目标超过 100% 的比例。
- 当前真实源数据未将 332.72% 作为硬性基准值。
- 正文不出现“口径待确认”。
- 真实 0 显示为 0。
- 缺失值显示为“未提供”。
- 字段未接入显示为“未接入”。
- 趋势缺失点不补 0。
- 搜索 / 筛选 / 排序只改变当前模块可见项和顺序。

## 7. 账号模块验收摘要

- 账号搜索框、排序控件、筛选 chips、清除条件、当前条件摘要均存在。
- 普通账号详情默认折叠。
- 快手-EXEED星途重点趋势卡存在于账号汇总顶部。
- 抖音-星途极速拍档保留。
- 333.33% 和 3,300.00% 如实显示。
- 搜索“极速拍档”只影响账号模块，主播和种草模块数量不变。
- 点击清除条件后账号列表恢复。
- “比率超过 100%”筛选后高比例账号仍显示。
- 账号模块不出现“口径待确认 / 异常 / 风险 / 预警”。

## 8. 主播模块验收摘要

- 主播搜索框、排序控件、筛选 chips、清除条件、当前条件摘要均存在。
- 主播详情默认折叠。
- 搜索“徐幻”只影响主播模块，账号和种草模块数量不变。
- 点击清除条件后主播列表恢复。
- 按成交数排序可用。
- 有成交、有费用、EX7 有成交、比率超过 100% 筛选均可用。
- 搜索不存在主播时显示“无匹配主播”和“可尝试清除搜索词或筛选条件。”。
- 主播所属账号长文本未撑爆页面。

## 9. 种草模块验收摘要

- 种草搜索框、排序控件、筛选 chips、清除条件、当前条件摘要均存在。
- 种草详情默认折叠。
- 搜索“王雪”只影响种草模块，账号和主播模块数量不变。
- 点击清除条件后种草列表恢复。
- 账号总曝光卡默认置顶。
- 主播曝光筛选下不显示账号总曝光。
- 当前 / 目标超过 100% 筛选后真实高比例仍显示。
- 搜索不存在种草项时显示“无匹配种草项”和“可尝试清除搜索词或筛选条件。”。
- 正文不出现 account / anchor / metric_type / source_type。

## 10. 截图路径

- /tmp/oae_n13_n12_baseline_desktop_1440.png
- /tmp/oae_n13_n12_baseline_account.png
- /tmp/oae_n13_n12_baseline_anchor.png
- /tmp/oae_n13_n12_baseline_seed.png
- /tmp/oae_n13_n12_baseline_mobile_390.png

## 11. 修改文件清单

本次冻结任务仅新增本文件：

- docs/dashboard_n12_1_ui_acceptance_baseline.md

本次未修改源码、服务层、schema、API 路由或既有测试文件。

## 12. 未跟踪 / dirty 状态说明

仓库开始和结束时均存在大量既有 dirty / untracked 文件。本次未清理、未 stage、未删除 unrelated dirty / untracked 文件。

与 N12.1 UI 基准相关的关键文件仍处于未跟踪状态，包括：

- src/oae/exports/feishu_dashboard_interactive_html.py
- tests/test_feishu_dashboard_interactive_html.py
- tests/api/
- docs/dashboard_ui_benchmark.md
- docs/dashboard_business_metric_map.md
- docs/dashboard_n12_1_ui_acceptance_baseline.md

## 13. 未运行 daily_pipeline

本次未运行：

```bash
python -m oae.jobs.daily_pipeline
```

## 14. 未 commit / push / tag

本次未执行 commit、push、merge、tag，未使用 `git add .`。

## 15. 基准用途说明

本文件只作为 N12.1 UI 交互阶段的可追溯验收基准，用于后续提交、归档或发布前评审前的状态冻结。是否进入正式发布仍需单独走发布前评审和业务确认流程。
