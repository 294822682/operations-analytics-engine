# Analysis Manual Consumption Migration

## 1. 当前默认人工消费文件名

Round 9 起，人工 / Excel 消费链默认应使用 canonical 文件名：

- unified workbook
  - `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
- unified snapshot
  - `artifacts/snapshots/analysis_snapshot_unified-fact_latest_{date}.csv`
- raw workbook
  - `全量分析_round*_raw/analysis_workbook_raw-evidence_latest_{date}.xlsx`
- raw anomaly report
  - `全量分析_round*_raw/analysis_anomaly_report_raw-evidence_latest_{date}.xlsx`
- raw snapshot
  - `artifacts/snapshots/round*_raw/analysis_snapshot_raw-evidence_latest_{date}.csv`

## 2. 兼容文件名

以下旧名字已停止写出，只作为历史说明或旧轮次产物存在：

- `analysis_tables.xlsx`
- `time_chain_anomaly_report.xlsx`
- `analysis_snapshot_latest_{date}.csv`
- `raw_analysis_snapshot_latest_{date}.csv`

## 3. 迁移映射

| 旧名字 | 新默认名字 |
|---|---|
| `analysis_tables.xlsx` | `analysis_workbook_unified-fact_latest_{date}.xlsx` |
| `analysis_snapshot_latest_{date}.csv` | `analysis_snapshot_unified-fact_latest_{date}.csv` |
| `time_chain_anomaly_report.xlsx` | `analysis_anomaly_report_raw-evidence_latest_{date}.xlsx` |
| `raw_analysis_snapshot_latest_{date}.csv` | `analysis_snapshot_raw-evidence_latest_{date}.csv` |

## 4. 停写兼容名的前提

只有在以下条件全部满足后，才允许关闭 compatibility 写出：

1. baseline 已切到 canonical analysis 文件名
2. pipeline / quality / export 默认消费 canonical 名
3. 人工 / Excel 操作说明已经切到 canonical 名
4. naming status 的 `can_disable_now = true`
5. naming dry-run 的 `dry_run_result.status = ready`

## 5. 当前状态

- 默认消费路径：canonical
- compatibility 名：已停止写出，只保留历史说明和少量兼容 shell
- 推荐操作：所有人工打开 workbook / anomaly / snapshot 时都只使用 canonical 名
