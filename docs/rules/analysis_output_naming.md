# Analysis Output Naming Strategy

## 1. 当前默认命名

Round 8 起，系统内部默认优先使用 canonical 命名，并且 pipeline / manifest / analysis runtime 已将 canonical 路径作为默认输出路径：

- unified snapshot:
  - `analysis_snapshot_unified-fact_latest_{date}.csv`
- unified workbook:
  - `analysis_workbook_unified-fact_latest_{date}.xlsx`
- raw snapshot:
  - `analysis_snapshot_raw-evidence_latest_{date}.csv`
- raw workbook:
  - `analysis_workbook_raw-evidence_latest_{date}.xlsx`
- raw anomaly report:
  - `analysis_anomaly_report_raw-evidence_latest_{date}.xlsx`

主链当前默认引用：

1. `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`
2. `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
3. 对应 canonical manifest：
   - `artifacts/exports/analysis/analysis_snapshot_unified-fact_latest_{date}.manifest.json`
   - `artifacts/exports/analysis/analysis_workbook_unified-fact_latest_{date}.manifest.json`

## 2. 兼容命名

以下文件名仍继续落盘，但只作为 compatibility/deprecated 输出：

- `analysis_snapshot_latest_{date}.csv`
- `raw_analysis_snapshot_latest_{date}.csv`
- `analysis_tables.xlsx`
- `time_chain_anomaly_report.xlsx`
- `analysis_snapshot_latest_{date}.manifest.json`
- `raw_analysis_snapshot_latest_{date}.manifest.json`

保留原因：

- 现有 Excel/人工消费链仍可能直接依赖这些文件名。
- `tests/baseline/reference_manifest.json` 还没有完全切到 canonical 名。

## 3. 工程化切换开关

Round 8 已将命名治理从文档约定升级为工程机制：

- 配置文件：
  - `config/analysis_output_naming.json`
- 人工消费迁移说明：
  - `docs/rules/analysis_manual_consumption.md`
- 状态输出：
  - unified:
    - `artifacts/exports/analysis/analysis_output_naming_status_unified-fact_latest_{date}.json`
  - raw:
    - `artifacts/exports/analysis_round8_raw/analysis_output_naming_status_raw-evidence_latest_{date}.json`

当前受控字段包括：

- `canonical_default`
- `compatibility_write_enabled`
- `dry_run_disable_compatibility`
- `allow_disable_only_when_ready`
- `manual_consumer_clearance`
- `baseline_manifest`

状态文件会明确输出：

- `compatibility_write_requested`
- `compatibility_write_effective`
- `can_disable_now`
- `dry_run_result`
- `pre_disable_check`
- `blockers`
- `required_actions`

这意味着 compatibility 名不再是“默认顺手一起写”，而是“有配置、有状态、有阻塞判断”的受控兼容输出。

## 4. 默认优先级

1. 系统内调度、pipeline、quality、export、analysis runtime：优先 canonical 名
2. 人工查看、旧脚本、历史兼容链：允许继续读取 compatibility 名
3. compatibility 停写前，必须通过 readiness check；即使配置里请求停写，只要阻塞项未清除，仍会保持 `compatibility_write_effective=true`

## 5. 停写前检查

停写 compatibility 名前，至少要满足：

1. pipeline 已消费 canonical 名
2. quality 已消费 canonical 名
3. canonical outputs 与 canonical manifests 全部存在
4. baseline/reference 切换到 canonical 名
5. Excel/人工消费链完成 canonical workbook / snapshot 切换确认

当前状态样例可见：

- `can_disable_now = false`
- blockers:
  - `baseline/reference 仍绑定 compatibility 文件名`
  - `全量分析/analysis_tables.xlsx 仍被人工/Excel 消费链直接打开`
  - `全量分析_round*_raw/time_chain_anomaly_report.xlsx 仍被人工/Excel 消费链直接打开`

## 6. 风险点

- `analysis_tables.xlsx` 和 `time_chain_anomaly_report.xlsx` 仍是高风险兼容点，因为人工消费最可能直接打开这两个文件。
- snapshot 文件名切换风险较低，因为主链已经通过 manifest 和 pipeline 明确定位 canonical 文件。
- manifest 命名切换必须与 snapshot/workbook 文件切换同步，否则会产生“文件名已切、manifest 仍旧”的混乱状态。
- compatibility 名停写前，必须先完成 baseline 与人工消费链切换；否则主链虽然能跑，人工复盘链会断。

## 7. Round 8 落地状态

- 已落地：
  - canonical 文件持续落盘
  - pipeline 默认优先 canonical unified snapshot 与 workbook
  - raw/unified 命名状态文件已落盘
  - compatibility 输出已进入显式治理状态
- 未落地：
  - compatibility 文件尚未停写
  - baseline/reference 仍未完全切换到 canonical 名
  - Excel/人工消费链尚未完成 canonical workbook / anomaly report 的切换确认

## 8. Round 9 状态更新

- 已完成：
  - `tests/baseline/reference_manifest.json` 已切到 canonical analysis 文件名
  - 人工 / Excel 消费说明已切到 canonical workbook / anomaly / snapshot 名
  - naming status 已支持 `dry_run_disable_compatibility` 与 `pre_disable_check`
- 当前结论：
  - `canonical_default = true`
  - `can_disable_now = true`
  - `dry_run_result.status = ready`
  - compatibility 仍保留写出，但已处于“可关闭、可回滚”的受控状态

## 9. 真正停写与回滚方式

当前仓库已经满足“可以停写 compatibility 名”的前置条件，但默认仍保持写出，原因是本轮只推进到 dry-run ready，不直接强停。

真正停写动作：

1. 将 `config/analysis_output_naming.json` 中的 `compatibility_write_enabled` 改为 `false`
2. 保持 `allow_disable_only_when_ready = true`
3. 重新运行 unified / raw analysis 或主 pipeline

停写后的行为：

- compatibility workbook / snapshot / anomaly / manifest 不再写出
- 若旧 compatibility 文件仍存在，会在本轮输出阶段被清理，避免残留旧 alias 造成误判

回滚方式：

1. 将 `compatibility_write_enabled` 改回 `true`
2. 重新运行 unified / raw analysis 或主 pipeline
3. compatibility 别名会再次落盘

## 10. Round 10 cutover 状态

- 已执行正式 cutover：
  - `compatibility_write_enabled = false`
- 当前正式结论：
  - canonical 名是唯一正式默认入口
  - compatibility 名已停止写出
  - compatibility 名仅作为历史说明和少量 compatibility shell 保留
- 验证结果：
  - `compatibility_write_requested = false`
  - `compatibility_write_effective = false`
  - `can_disable_now = true`
  - `dry_run_result.status = ready`
  - active output 目录中的 compatibility workbook / snapshot / anomaly / manifest 已被清理
