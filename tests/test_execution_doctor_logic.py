from __future__ import annotations

from pathlib import Path

from oae.services.execution_doctor_logic import build_doctor_manifest


def test_build_doctor_manifest_marks_ready_when_quality_passes_and_artifacts_exist(tmp_path: Path) -> None:
    required_artifact = tmp_path / "feishu_table_latest_2026-05-29.tsv"
    required_artifact.write_text("account\tleads\n", encoding="utf-8")

    manifest = build_doctor_manifest(
        run_manifest={
            "run_id": "run-20260530T051549Z",
            "workspace": str(tmp_path),
            "canonical_report_date": "2026-05-29",
        },
        quality_report={
            "overall_status": "pass",
            "summary": {"operational_decision": "safe"},
        },
        required_artifacts=[required_artifact],
    )

    assert manifest["release_readiness"] == "ready"
    assert manifest["preflight_status"] == "pass"
    assert manifest["summary"]["required_artifact_count"] == 1
    assert manifest["summary"]["present_artifact_count"] == 1
    assert manifest["summary"]["missing_required_artifacts"] == []


def test_build_doctor_manifest_blocks_failed_quality_even_when_artifacts_exist(tmp_path: Path) -> None:
    required_artifact = tmp_path / "feishu_table_latest_2026-05-29.tsv"
    required_artifact.write_text("account\tleads\n", encoding="utf-8")

    manifest = build_doctor_manifest(
        run_manifest={
            "run_id": "run-20260530T051549Z",
            "workspace": str(tmp_path),
            "canonical_report_date": "2026-05-29",
        },
        quality_report={
            "overall_status": "fail",
            "summary": {"operational_decision": "block"},
        },
        required_artifacts=[required_artifact],
    )

    assert manifest["release_readiness"] == "blocked"
    assert manifest["summary"]["blocking_reasons"] == ["quality_decision_block", "quality_status_fail"]

