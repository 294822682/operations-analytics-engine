from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from oae.adapters import runs_dir
from oae.services.report_date_semantics import resolve_manifest_report_dates


@dataclass(frozen=True)
class GateSuite:
    key: str
    description: str
    pytest_args: tuple[str, ...]


@dataclass(frozen=True)
class GateProfile:
    key: str
    description: str
    include_full_pytest: bool
    strict_release_ready: bool


QUALITY_TARGETED_SUITE = GateSuite(
    key="quality_targeted",
    description="quality / baseline / business targeted regression",
    pytest_args=(
        "tests/test_quality_baseline.py",
        "tests/test_quality_reports.py",
        "tests/test_quality_business.py",
        "-q",
    ),
)

SMOKE_BUNDLE_CONTRACT_SUITE = GateSuite(
    key="smoke_bundle_contract",
    description="controlled smoke bundle contract regression",
    pytest_args=(
        "tests/test_smoke_bundle_contract.py",
        "-q",
    ),
)

EXECUTION_TARGETED_SUITE = GateSuite(
    key="execution_targeted",
    description="preflight / doctor / registry / latest / execution smoke",
    pytest_args=(
        "tests/test_input_preflight.py",
        "tests/test_execution_doctor_logic.py",
        "tests/test_report_latest_logic.py",
        "tests/test_report_registry.py",
        "tests/api/test_execution_smoke.py",
        "tests/api/test_execution_contracts.py",
        "tests/api/test_report_status.py",
        "tests/api/test_report_quality.py",
        "tests/api/test_readable_report_resolution.py",
        "tests/api/test_report_compare.py",
        "tests/api/test_registry_read_write_split.py",
        "-q",
    ),
)

FULL_PYTEST_SUITE = GateSuite(
    key="full_pytest",
    description="full repository pytest regression",
    pytest_args=("-q",),
)


PR_GATE_PROFILE = GateProfile(
    key="pr",
    description="engineering gate for pull requests and mainline pushes",
    include_full_pytest=True,
    strict_release_ready=False,
)

RELEASE_GATE_PROFILE = GateProfile(
    key="release",
    description="release gate requiring engineering pass plus ready release evidence",
    include_full_pytest=True,
    strict_release_ready=True,
)

_GATE_PROFILES = {
    PR_GATE_PROFILE.key: PR_GATE_PROFILE,
    RELEASE_GATE_PROFILE.key: RELEASE_GATE_PROFILE,
}


def gate_profile_choices() -> tuple[str, ...]:
    return tuple(_GATE_PROFILES)


def resolve_gate_profile(profile: str | None) -> GateProfile:
    normalized = str(profile or PR_GATE_PROFILE.key).strip().lower()
    try:
        return _GATE_PROFILES[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported gate profile: {profile}") from exc


def default_gate_suites(*, include_full_pytest: bool = True) -> List[GateSuite]:
    suites = [QUALITY_TARGETED_SUITE, SMOKE_BUNDLE_CONTRACT_SUITE, EXECUTION_TARGETED_SUITE]
    if include_full_pytest:
        suites.append(FULL_PYTEST_SUITE)
    return suites


def load_release_candidate_evidence(repo_root: Path) -> Dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    runs_path = runs_dir(repo_root)
    manifest_path = _latest_run_manifest(runs_path)
    if manifest_path is None:
        return {
            "exists": False,
            "status": "missing",
            "recommended_action": "run_pipeline_and_refresh_evidence",
            "blocking_reasons": ["run_manifest_missing"],
            "review_reasons": [],
            "paths": {
                "runs_dir": str(runs_path),
                "run_manifest": "",
                "doctor_manifest": "",
                "quality_report": "",
            },
        }

    run_manifest = _load_json(manifest_path)
    run_id = str(run_manifest.get("run_id", "")).strip()
    doctor_path = runs_path / f"doctor_manifest_{run_id}.json"
    quality_path = runs_path / f"quality_report_{run_id}.json"
    doctor_manifest = _load_json(doctor_path) if doctor_path.exists() else {}
    quality_report = _load_json(quality_path) if quality_path.exists() else {}

    report_dates = resolve_manifest_report_dates(run_manifest)
    doctor_summary = doctor_manifest.get("summary", {}) if isinstance(doctor_manifest.get("summary", {}), dict) else {}
    quality_summary = quality_report.get("summary", {}) if isinstance(quality_report.get("summary", {}), dict) else {}

    required_artifact_count = int(doctor_summary.get("required_artifact_count", 0) or 0)
    present_artifact_count = int(doctor_summary.get("present_artifact_count", 0) or 0)
    missing_required_artifacts = list(doctor_summary.get("missing_required_artifacts", []) or [])
    release_readiness = str(
        doctor_manifest.get("release_readiness", run_manifest.get("release_readiness", "unknown"))
    ).strip() or "unknown"
    quality_status = str(quality_report.get("overall_status", run_manifest.get("quality_status", "unknown"))).strip() or "unknown"
    quality_decision = str(quality_summary.get("operational_decision", run_manifest.get("quality_decision", ""))).strip()

    blocking_reasons: List[str] = []
    review_reasons: List[str] = []

    if not doctor_path.exists():
        blocking_reasons.append("doctor_manifest_missing")
    if not quality_path.exists():
        blocking_reasons.append("quality_report_missing")
    if missing_required_artifacts:
        blocking_reasons.append("missing_required_artifacts")
    if release_readiness == "blocked":
        blocking_reasons.append("release_readiness_blocked")
    if quality_decision == "block":
        blocking_reasons.append("quality_decision_block")
    if quality_status == "fail":
        blocking_reasons.append("quality_status_fail")

    if release_readiness == "review":
        review_reasons.append("release_readiness_review")
    if quality_decision == "investigate":
        review_reasons.append("quality_decision_investigate")
    if quality_status in {"warning", "warn"}:
        review_reasons.append("quality_status_warning")
    run_status = str(run_manifest.get("status", doctor_manifest.get("status", "unknown"))).strip() or "unknown"

    if run_status == "degraded":
        review_reasons.append("run_status_degraded")

    status = classify_release_candidate_status(
        blocking_reasons=blocking_reasons,
        review_reasons=review_reasons,
        release_readiness=release_readiness,
        quality_status=quality_status,
    )
    return {
        "exists": True,
        "run_id": run_id,
        "report_date": str(report_dates["canonical_report_date"]),
        "manifest_report_date": str(report_dates["manifest_report_date"]),
        "canonical_report_date": str(report_dates["canonical_report_date"]),
        "resolved_report_date": str(report_dates["resolved_report_date"]),
        "run_status": run_status,
        "preflight_status": str(doctor_manifest.get("preflight_status", run_manifest.get("preflight_status", "unknown"))),
        "quality_status": quality_status,
        "quality_decision": quality_decision,
        "release_readiness": release_readiness,
        "required_artifact_count": required_artifact_count,
        "present_artifact_count": present_artifact_count,
        "missing_required_artifacts": missing_required_artifacts,
        "blocking_reasons": blocking_reasons,
        "review_reasons": review_reasons,
        "status": status,
        "recommended_action": recommended_release_action(status),
        "key_alerts": list(quality_summary.get("key_alerts", []) or [])[:5],
        "attention_items": list(quality_summary.get("attention_items", []) or [])[:5],
        "paths": {
            "runs_dir": str(runs_path),
            "run_manifest": str(manifest_path),
            "doctor_manifest": str(doctor_path) if doctor_path.exists() else "",
            "quality_report": str(quality_path) if quality_path.exists() else "",
        },
    }


def classify_release_candidate_status(
    *,
    blocking_reasons: Sequence[str],
    review_reasons: Sequence[str],
    release_readiness: str,
    quality_status: str,
) -> str:
    if blocking_reasons:
        return "blocked"
    if review_reasons:
        return "review"
    if release_readiness == "ready" and quality_status == "pass":
        return "ready"
    return "unknown"


def recommended_release_action(status: str) -> str:
    if status == "ready":
        return "eligible_for_release_candidate_confirmation"
    if status == "review":
        return "manual_review_required"
    if status == "blocked":
        return "fix_gate_or_refresh_run_evidence"
    return "inspect_release_candidate_evidence"


def evaluate_gate_run(
    *,
    suite_results: Iterable[Dict[str, Any]],
    release_candidate: Dict[str, Any],
    strict_release_ready: bool,
) -> Dict[str, Any]:
    suite_results = list(suite_results)
    failed_suites = [item["key"] for item in suite_results if not item.get("passed", False)]
    engineering_status = "pass" if not failed_suites else "fail"
    release_status = str(release_candidate.get("status", "missing"))
    release_gate_status = "pass"
    release_gate_reason = ""
    if strict_release_ready and release_status != "ready":
        release_gate_status = "fail"
        release_gate_reason = f"strict_release_ready requires ready, got {release_status}"

    overall_status = "pass" if engineering_status == "pass" and release_gate_status == "pass" else "fail"
    return {
        "engineering_status": engineering_status,
        "failed_suites": failed_suites,
        "release_candidate_status": release_status,
        "release_gate_status": release_gate_status,
        "release_gate_reason": release_gate_reason,
        "overall_status": overall_status,
        "exit_code": 0 if overall_status == "pass" else 1,
    }


def _latest_run_manifest(runs_path: Path) -> Path | None:
    if not runs_path.exists():
        return None
    candidates = sorted(runs_path.glob("run_manifest_*.json"))
    return candidates[-1] if candidates else None


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
