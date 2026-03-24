# Baseline Assets

本目录冻结第一轮产品化改造前后的关键业务样本，用于回归比对，避免经营口径漂移。

当前纳入基线的参考产物：

- `reference/fact_attribution.csv`
- `reference/daily_goal_account_latest_2026-03-12.csv`
- `reference/feishu_table_latest_2026-03-12.tsv`
- `reference/analysis_workbook_unified-fact_latest_2026-03-12.xlsx`
- `reference/analysis_workbook_raw-evidence_latest_2026-03-12.xlsx`
- `reference/analysis_anomaly_report_raw-evidence_latest_2026-03-12.xlsx`

已切换到 canonical 命名的 baseline 项：

- unified workbook：`analysis_workbook_unified-fact_latest_2026-03-12.xlsx`
- raw workbook：`analysis_workbook_raw-evidence_latest_2026-03-12.xlsx`
- raw anomaly report：`analysis_anomaly_report_raw-evidence_latest_2026-03-12.xlsx`

已归档的历史 compatibility 样本：

- `legacy/archive/analysis_tables_baseline_compatibility_2026-03-12.xlsx`
  - 仅作历史审计对照，不再位于默认 baseline 目录，也不再参与默认 baseline 比对。

配套清单：

- `reference_manifest.json`：记录文件名、绝对路径、`sha256`、文件大小。

使用方式：

1. 运行主链路生成新产物。
2. 用 `src/oae/quality/baseline.py` 对照 `reference_manifest.json` 做差异检查。
3. 只有在经营口径明确升级且得到确认后，才允许重置 baseline。
