"""Contract checks for exports and analysis snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from oae.contracts import CONTRACT_SCHEMAS, contract_required_columns


def run_raw_contract_checks(
    *,
    raw_snapshot_path: Path,
    snapshot_manifest_path: Path,
    workbook_manifest_path: Path,
    anomaly_manifest_path: Path,
    workbook_path: Path,
    anomaly_report_path: Path,
    theme_manifest_path: Path,
    expected_schema_version: str,
    expected_metric_version: str,
    expected_run_id: str,
) -> list[dict[str, object]]:
    checks = []
    checks.append(
        check_snapshot_contract(
            raw_snapshot_path,
            contract_name="raw_analysis_snapshot",
            expected_fields={
                "schema_version": expected_schema_version,
                "metric_version": expected_metric_version,
                "run_id": expected_run_id,
                "analysis_mode": "raw-evidence",
                "evidence_mode": "raw",
            },
            required_true_fields=["raw_evidence_required"],
            name="raw.contract.snapshot_schema",
        )
    )
    checks.extend(
        check_export_manifest_contracts(
            [
                {
                    "path": snapshot_manifest_path,
                    "name": "raw.contract.snapshot_manifest",
                    "expected_consumer": "analysis-snapshot.raw-evidence",
                    "expected_export_name": "analysis_snapshot",
                    "expected_output_path": raw_snapshot_path,
                },
                {
                    "path": workbook_manifest_path,
                    "name": "raw.contract.workbook_manifest",
                    "expected_consumer": "excel_workbook.raw-evidence",
                    "expected_export_name": "analysis_workbook",
                    "expected_output_path": workbook_path,
                },
                {
                    "path": anomaly_manifest_path,
                    "name": "raw.contract.anomaly_manifest",
                    "expected_consumer": "excel_workbook.raw-evidence",
                    "expected_export_name": "analysis_anomaly_report",
                    "expected_output_path": anomaly_report_path,
                },
            ],
            expected_schema_version=expected_schema_version,
            expected_metric_version=expected_metric_version,
            expected_template_version="",
            expected_run_id=expected_run_id,
            expected_freeze_id="",
        )
    )
    checks.append(
        check_excel_manifest_alignment(
            workbook_path=workbook_path,
            manifest_path=workbook_manifest_path,
            name="raw.contract.workbook_alignment",
        )
    )
    checks.append(
        check_excel_manifest_alignment(
            workbook_path=anomaly_report_path,
            manifest_path=anomaly_manifest_path,
            name="raw.contract.anomaly_alignment",
        )
    )
    checks.append(check_theme_manifest(theme_manifest_path))
    return checks


def check_snapshot_contract(
    path: Path,
    *,
    contract_name: str,
    expected_fields: dict[str, object],
    required_true_fields: list[str] | None = None,
    name: str,
) -> dict[str, object]:
    if not path.exists():
        return _check(
            name,
            "contract violation",
            "fail",
            {
                "summary": "analysis snapshot 缺失",
                "threshold_breaches": [f"缺少 analysis snapshot: {path.name}"],
                "path": str(path),
            },
        )

    frame = pd.read_csv(path)
    missing_fields = [field for field in contract_required_columns(contract_name) if field not in frame.columns]
    mismatches: dict[str, object] = {}
    for field, expected in expected_fields.items():
        if field not in frame.columns:
            continue
        actual_values = sorted({str(value) for value in frame[field].dropna().astype(str).unique()})
        if actual_values and actual_values != [str(expected)]:
            mismatches[field] = {"expected": str(expected), "actual": actual_values}
    truth_failures = []
    for field in required_true_fields or []:
        if field in frame.columns and not frame[field].fillna(False).astype(bool).all():
            truth_failures.append(field)

    breaches = [f"缺少字段: {field}" for field in missing_fields]
    breaches.extend([f"{field} 不一致" for field in mismatches])
    breaches.extend([f"{field} 存在非 true 值" for field in truth_failures])
    return _check(
        name,
        "contract violation",
        "fail" if breaches else "pass",
        {
            "summary": "analysis snapshot contract 校验",
            "path": str(path),
            "row_count": int(len(frame)),
            "missing_fields": missing_fields,
            "mismatches": mismatches,
            "truth_failures": truth_failures,
            "threshold_breaches": breaches,
        },
    )


def check_export_manifest_contracts(
    manifest_specs: list[dict[str, object]],
    *,
    expected_schema_version: str,
    expected_metric_version: str,
    expected_template_version: str,
    expected_run_id: str,
    expected_freeze_id: str,
) -> list[dict[str, object]]:
    required = CONTRACT_SCHEMAS["export_manifest"]["required"]  # type: ignore[index]
    checks = []
    for spec in manifest_specs:
        path = Path(spec["path"])
        if not path.exists():
            checks.append(
                _check(
                    str(spec["name"]),
                    "contract violation",
                    "fail",
                    {
                        "summary": "manifest 缺失",
                        "path": str(path),
                        "threshold_breaches": [f"缺少 manifest: {path.name}"],
                    },
                )
            )
            continue

        data = json.loads(path.read_text(encoding="utf-8"))
        missing = [field for field in required if field not in data]
        mismatches = {}
        for field, expected in {
            "schema_version": expected_schema_version,
            "metric_version": expected_metric_version,
            "template_version": expected_template_version,
            "run_id": expected_run_id,
            "freeze_id": expected_freeze_id,
            "consumer": spec.get("expected_consumer", ""),
            "export_name": spec.get("expected_export_name", ""),
        }.items():
            if expected == "":
                continue
            actual = data.get(field, "")
            if actual != expected:
                mismatches[field] = {"expected": expected, "actual": actual}

        output_path = Path(data.get("output_path", ""))
        expected_output_path = Path(spec.get("expected_output_path", output_path))
        if output_path != expected_output_path:
            mismatches["output_path"] = {"expected": str(expected_output_path), "actual": str(output_path)}
        elif not output_path.exists():
            mismatches["output_path"] = {"expected": "existing file", "actual": str(output_path)}

        breaches = [f"缺少字段: {field}" for field in missing]
        breaches.extend([f"{field} 不一致" for field in mismatches])
        checks.append(
            _check(
                str(spec["name"]),
                "contract violation",
                "fail" if breaches else "pass",
                {
                    "summary": "manifest contract 校验",
                    "path": str(path),
                    "missing_fields": missing,
                    "mismatches": mismatches,
                    "threshold_breaches": breaches,
                },
            )
        )
    return checks


def check_excel_manifest_alignment(*, workbook_path: Path, manifest_path: Path, name: str) -> dict[str, object]:
    if not workbook_path.exists() or not manifest_path.exists():
        return _check(
            name,
            "contract violation",
            "fail",
            {
                "summary": "workbook/anomaly 对齐校验失败",
                "threshold_breaches": [f"缺少文件: {workbook_path.name if not workbook_path.exists() else manifest_path.name}"],
            },
        )

    sheet_count = len(pd.ExcelFile(workbook_path).sheet_names)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_row_count = int(manifest.get("row_count", -1))
    breaches = []
    if manifest_row_count != sheet_count:
        breaches.append(f"manifest row_count={manifest_row_count} 与工作簿 sheet_count={sheet_count} 不一致")
    return _check(
        name,
        "contract violation",
        "fail" if breaches else "pass",
        {
            "summary": "workbook/anomaly 与 manifest 对齐",
            "sheet_count": sheet_count,
            "manifest_row_count": manifest_row_count,
            "threshold_breaches": breaches,
        },
    )


def check_theme_manifest(path: Path) -> dict[str, object]:
    if not path.exists():
        return _check(
            "raw.contract.theme_manifest",
            "contract violation",
            "fail",
            {"summary": "theme manifest 缺失", "threshold_breaches": [f"缺少 raw_analysis_theme_manifest: {path.name}"]},
        )

    data = json.loads(path.read_text(encoding="utf-8"))
    required = {"analysis_mode", "raw_evidence_topics", "raw_evidence_groups"}
    missing = sorted(required - set(data.keys()))
    return _check(
        "raw.contract.theme_manifest",
        "contract violation",
        "fail" if missing else "pass",
        {
            "summary": "theme manifest contract 校验",
            "missing_fields": missing,
            "threshold_breaches": [f"缺少字段: {field}" for field in missing],
        },
    )


def _check(name: str, category: str, status: str, details: object) -> dict[str, object]:
    return {"name": name, "category": category, "status": status, "details": details}
