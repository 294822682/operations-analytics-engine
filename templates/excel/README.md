# Excel Template Contract

V1 继续采用 `TSV -> Excel` 的消费策略。

Analysis workbook / anomaly 的默认人工消费路径已切到 canonical 名：

- unified workbook：
  - `全量分析/analysis_workbook_unified-fact_latest_{date}.xlsx`
- raw workbook：
  - `全量分析_round*_raw/analysis_workbook_raw-evidence_latest_{date}.xlsx`
- raw anomaly report：
  - `全量分析_round*_raw/analysis_anomaly_report_raw-evidence_latest_{date}.xlsx`

兼容名已停止写出，当前仅作历史说明或旧轮次产物存在：

- `analysis_tables.xlsx`
- `time_chain_anomaly_report.xlsx`

当前约束：

- `template_version`: `excel-tsv-v1`
- 导出 manifest 中必须记录：
  - `schema_version`
  - `metric_version`
  - `template_version`
  - `freeze_id`
  - `run_id`

后续这里应补：

- 正式 Excel 模板文件
- 字段映射说明
- 模板变更记录
- 粘贴/刷新操作说明
- canonical workbook / anomaly 的消费确认记录
