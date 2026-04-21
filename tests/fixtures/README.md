# FastAPI Execution Fixtures

本目录存放第二阶段第 1 轮的最小执行链路回归样本。

原则：

- 优先使用“已知能跑通的真实样本裁剪版 / 冻结版”，不凭空手写业务数据。
- fixture 仅用于 API 执行回归，不作为口径 baseline。
- 负向样本尽量在测试里动态生成，不额外落库。

当前样本：

- `pilot/EXEED星途台账（三月）.xlsx`
  - 来源：仓库中的 `历史文件/EXEED星途台账（三月） .xlsx`
  - 用途：`POST /pilot/diagnose` smoke
  - 选择理由：已有成功运行证据，对应历史产物见 `artifacts/pilot/pilot_run_latest_2026-03-23.json`

- `pipeline/bundle_utf8_smoke.zip`
  - 来源：仓库中的 `bundle_utf8.zip`
  - 用途：`POST /pipeline/run` smoke
  - 选择理由：是当前最小的 3 文件 bundle，且 zip 内 UTF-8 文件名正常
  - 纳管方式：由 `pipeline/smoke_bundle_contract.json` 固定 `bundle_path / bundle_size_bytes / bundle_sha256`
  - 持续化边界：`tests/test_smoke_bundle_contract.py` 已进入统一门禁，`make smoke` 与 `make ci` 都会持续校验这份合同

- `report_runs/current_review_candidate/*.json`
  - 来源：基于 `run-20260417T091828Z` 这组真实运行证据裁剪出的最小 registry seed fixture
  - 用途：`/reports/status`、`/reports/quality`、registry read/write split 等 API 测试
  - 选择理由：只保留 `run_manifest + quality_report + doctor_manifest` 的最小证据，不再依赖工作区真实 `artifacts/runs/`

附加说明：

- `POST /reports/run-latest-if-missing` smoke 复用 `pipeline/bundle_utf8_smoke.zip` 内的 3 个动态输入文件，并在测试时解压到临时 repo 的 `源文件/`。
- 这样可以保证 `/pipeline/run` 与 `/reports/run-latest-if-missing` 使用同一套已知可跑通的 source snapshot，避免受当前工作区实时源文件波动影响。
- 如果后续要进一步缩小 pipeline fixture，应基于现有真实 bundle 裁剪，并保留文件命名契约不变。
- `pipeline/bundle_utf8_smoke.zip` 只用于最小执行链 smoke，不承担 baseline/reference、配置覆盖或 release 证据职责。
- 这份 bundle 不得打入 `monthly_targets.csv`、`daily_spend.csv`、`manual_attribution_overrides.csv`、`report_topline_config.json`、`quality_thresholds.json`、`input_sources.json`、`analysis_output_naming.json`。
- 如需替换 smoke bundle，必须同步更新 `bundle_utf8_smoke.zip`、`smoke_bundle_contract.json` 与相关 smoke 测试期望；证据不足时保持现状并标记 `待核实`。
- `report_runs/current_review_candidate/` 当前是最小的“可 seed registry”样本；后续如果切换到新的真实发布候选，应替换这一组固定 fixture，而不是重新让测试回连工作区真实 `artifacts/runs/`。
