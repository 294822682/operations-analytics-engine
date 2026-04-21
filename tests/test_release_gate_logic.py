from __future__ import annotations

import json
from pathlib import Path

from oae.services.release_gate_logic import (
    classify_release_candidate_status,
    default_gate_suites,
    evaluate_gate_run,
    load_release_candidate_evidence,
    resolve_gate_profile,
)


def test_default_gate_suites_include_full_by_default() -> None:
    suites = default_gate_suites()

    assert [suite.key for suite in suites] == [
        "quality_targeted",
        "smoke_bundle_contract",
        "execution_targeted",
        "full_pytest",
    ]


def test_default_gate_suites_can_skip_full_pytest() -> None:
    suites = default_gate_suites(include_full_pytest=False)

    assert [suite.key for suite in suites] == [
        "quality_targeted",
        "smoke_bundle_contract",
        "execution_targeted",
    ]


def test_resolve_gate_profile_supports_pr_and_release() -> None:
    assert resolve_gate_profile("pr").strict_release_ready is False
    assert resolve_gate_profile("release").strict_release_ready is True


def test_load_release_candidate_evidence_returns_missing_when_no_run_exists(tmp_path: Path) -> None:
    evidence = load_release_candidate_evidence(tmp_path)

    assert evidence["exists"] is False
    assert evidence["status"] == "missing"
    assert evidence["blocking_reasons"] == ["run_manifest_missing"]


def test_load_release_candidate_evidence_reads_matching_run_bundle(tmp_path: Path) -> None:
    runs_path = tmp_path / "artifacts" / "runs"
    runs_path.mkdir(parents=True)
    run_id = "run-20260417T091828Z"
    run_manifest_path = runs_path / f"run_manifest_{run_id}.json"
    doctor_manifest_path = runs_path / f"doctor_manifest_{run_id}.json"
    quality_report_path = runs_path / f"quality_report_{run_id}.json"

    run_manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "workspace": str(tmp_path),
                "report_date": "2026-04-16",
                "canonical_report_date": "2026-04-16",
                "resolved_report_date": "2026-04-16",
                "status": "degraded",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    doctor_manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "release_readiness": "review",
                "preflight_status": "pass",
                "summary": {
                    "required_artifact_count": 10,
                    "present_artifact_count": 10,
                    "missing_required_artifacts": [],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    quality_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "overall_status": "warning",
                "summary": {
                    "operational_decision": "investigate",
                    "key_alerts": [],
                    "attention_items": ["snapshot.baseline_metrics: drift"],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    evidence = load_release_candidate_evidence(tmp_path)

    assert evidence["exists"] is True
    assert evidence["run_id"] == run_id
    assert evidence["report_date"] == "2026-04-16"
    assert evidence["run_status"] == "degraded"
    assert evidence["status"] == "review"
    assert evidence["release_readiness"] == "review"
    assert evidence["quality_status"] == "warning"
    assert evidence["quality_decision"] == "investigate"
    assert evidence["present_artifact_count"] == 10
    assert evidence["blocking_reasons"] == []
    assert set(evidence["review_reasons"]) >= {
        "release_readiness_review",
        "quality_decision_investigate",
        "quality_status_warning",
        "run_status_degraded",
    }


def test_classify_release_candidate_status_prefers_blocking() -> None:
    status = classify_release_candidate_status(
        blocking_reasons=["quality_status_fail"],
        review_reasons=["quality_status_warning"],
        release_readiness="review",
        quality_status="warning",
    )

    assert status == "blocked"


def test_evaluate_gate_run_only_enforces_release_ready_in_strict_mode() -> None:
    suite_results = [
        {"key": "quality_targeted", "passed": True},
        {"key": "execution_targeted", "passed": True},
    ]
    release_candidate = {"status": "review"}

    default_eval = evaluate_gate_run(
        suite_results=suite_results,
        release_candidate=release_candidate,
        strict_release_ready=False,
    )
    strict_eval = evaluate_gate_run(
        suite_results=suite_results,
        release_candidate=release_candidate,
        strict_release_ready=True,
    )

    assert default_eval["overall_status"] == "pass"
    assert default_eval["exit_code"] == 0
    assert strict_eval["overall_status"] == "fail"
    assert strict_eval["exit_code"] == 1
