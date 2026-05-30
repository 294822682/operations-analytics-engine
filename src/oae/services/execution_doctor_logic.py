from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


def build_doctor_manifest(
    *,
    run_manifest: Mapping[str, Any],
    quality_report: Mapping[str, Any],
    required_artifacts: Iterable[Path | str],
) -> dict[str, Any]:
    artifacts = [_artifact_record(Path(path)) for path in required_artifacts]
    missing_required_artifacts = [item["path"] for item in artifacts if not item["exists"]]
    quality_summary = quality_report.get("summary", {})
    if not isinstance(quality_summary, Mapping):
        quality_summary = {}

    quality_status = str(quality_report.get("overall_status", "unknown")).strip() or "unknown"
    quality_decision = str(quality_summary.get("operational_decision", "")).strip()

    blocking_reasons: list[str] = []
    review_reasons: list[str] = []
    if missing_required_artifacts:
        blocking_reasons.append("missing_required_artifacts")
    if quality_decision == "block":
        blocking_reasons.append("quality_decision_block")
    if quality_status == "fail":
        blocking_reasons.append("quality_status_fail")
    if quality_decision == "investigate":
        review_reasons.append("quality_decision_investigate")
    if quality_status in {"warning", "warn"}:
        review_reasons.append("quality_status_warning")

    release_readiness = "unknown"
    if blocking_reasons:
        release_readiness = "blocked"
    elif review_reasons:
        release_readiness = "review"
    elif quality_status == "pass" and quality_decision in {"", "safe"}:
        release_readiness = "ready"

    preflight_status = "fail" if missing_required_artifacts else "pass"
    run_status = "pass" if release_readiness == "ready" else release_readiness

    return {
        "run_id": str(run_manifest.get("run_id", "")),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "workspace": str(run_manifest.get("workspace", "")),
        "status": run_status,
        "preflight_status": preflight_status,
        "quality_status": quality_status,
        "quality_decision": quality_decision,
        "release_readiness": release_readiness,
        "summary": {
            "required_artifact_count": len(artifacts),
            "present_artifact_count": len(artifacts) - len(missing_required_artifacts),
            "missing_required_artifacts": missing_required_artifacts,
            "blocking_reasons": blocking_reasons,
            "review_reasons": review_reasons,
        },
        "artifacts": artifacts,
    }


def dump_doctor_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(manifest), ensure_ascii=False, indent=2), encoding="utf-8")


def _artifact_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else 0,
    }

