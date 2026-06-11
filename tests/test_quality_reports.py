from __future__ import annotations

from pathlib import Path

from oae.quality.reports import build_quality_report


def _check(
    name: str,
    category: str,
    status: str,
    *,
    breaches: list[str] | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "category": category,
        "status": status,
        "details": {
            "summary": f"{name} summary",
            "threshold_breaches": breaches or [],
            "threshold_source": "config/quality_thresholds.json",
            "threshold_profile": "operational",
        },
    }


def test_build_quality_report_marks_pass_and_records_output_files(tmp_path: Path) -> None:
    output_file = tmp_path / "feishu_table_latest_2026-06-08.tsv"
    output_file.write_text("metric\tvalue\n", encoding="utf-8")

    report = build_quality_report(
        run_id="run-20260608T010203Z",
        output_files=[output_file],
        baseline_result={"status": "pass", "details": []},
        extra_checks=[_check("fact.structural", "structural change", "pass")],
    )

    assert report["overall_status"] == "pass"
    assert report["summary"]["operational_decision"] == "safe"
    assert report["files"] == [{"path": str(output_file), "exists": True, "size_bytes": output_file.stat().st_size}]
    assert report["categories"] == {"structural change": 1}
    assert [item["name"] for item in report["structural_checks"]] == ["fact.structural"]


def test_build_quality_report_marks_warning_as_investigate() -> None:
    report = build_quality_report(
        run_id="run-20260608T010203Z",
        output_files=[],
        baseline_result={"status": "pass", "details": []},
        extra_checks=[
            _check(
                "snapshot.baseline_metrics",
                "metric drift",
                "warning",
                breaches=["total_mtd_leads drift exceeded warning"],
            )
        ],
    )

    assert report["overall_status"] == "warning"
    assert report["summary"]["operational_decision"] == "investigate"
    assert report["summary"]["threshold_breach_count"] == 1
    assert report["summary"]["attention_items"] == [
        "snapshot.baseline_metrics: total_mtd_leads drift exceeded warning"
    ]
    assert report["summary"]["configured_threshold_alerts"] == [
        "snapshot.baseline_metrics: total_mtd_leads drift exceeded warning"
    ]
    assert [item["name"] for item in report["metric_checks"]] == ["snapshot.baseline_metrics"]


def test_build_quality_report_marks_fail_as_block() -> None:
    report = build_quality_report(
        run_id="run-20260608T010203Z",
        output_files=[],
        baseline_result={"status": "pass", "details": []},
        extra_checks=[
            _check(
                "ledger.snapshot_reconcile",
                "metric drift",
                "fail",
                breaches=["ledger and snapshot mismatch"],
            )
        ],
    )

    assert report["overall_status"] == "fail"
    assert report["summary"]["operational_decision"] == "block"
    assert report["summary"]["key_alerts"] == ["ledger.snapshot_reconcile: ledger and snapshot mismatch"]


def test_build_quality_report_treats_safe_only_baseline_warning_as_pass() -> None:
    report = build_quality_report(
        run_id="run-20260608T010203Z",
        output_files=[],
        baseline_result={
            "status": "warning",
            "details": [
                {
                    "name": "fact_attribution.csv",
                    "category": "metadata-only change",
                    "status": "metadata-only change",
                }
            ],
        },
        extra_checks=[],
    )

    assert report["overall_status"] == "pass"
    assert report["summary"]["operational_decision"] == "safe"
    assert report["summary"]["safe_changes"] == ["fact_attribution.csv: 仅元数据变化，业务结果安全"]
