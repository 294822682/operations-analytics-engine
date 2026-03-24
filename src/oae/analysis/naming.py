"""Analysis output naming policy, readiness checks, and compatibility gating."""

from __future__ import annotations

import json
from pathlib import Path


DEFAULT_POLICY = {
    "profile": "canonical_preferred",
    "canonical_default": True,
    "compatibility_write_enabled": True,
    "dry_run_disable_compatibility": False,
    "allow_disable_only_when_ready": True,
    "baseline_manifest": "tests/baseline/reference_manifest.json",
    "manual_consumer_clearance": False,
    "manual_consumer_clearance_doc": "",
    "manual_consumer_blockers": [],
    "readiness_requirements": {
        "pipeline_uses_canonical": True,
        "quality_uses_canonical": True,
        "canonical_manifests_target_canonical_outputs": True,
        "canonical_manifests_present": True,
        "baseline_uses_canonical": True,
        "manual_consumer_clearance": True,
    },
}


def load_analysis_naming_policy(workspace_dir: Path, config_path: Path | None = None) -> dict[str, object]:
    resolved = config_path or workspace_dir / "config" / "analysis_output_naming.json"
    policy = json.loads(json.dumps(DEFAULT_POLICY))
    if resolved.exists():
        user_policy = json.loads(resolved.read_text(encoding="utf-8"))
        policy.update({k: v for k, v in user_policy.items() if k != "readiness_requirements"})
        if "readiness_requirements" in user_policy:
            policy["readiness_requirements"] = {
                **DEFAULT_POLICY["readiness_requirements"],
                **user_policy["readiness_requirements"],
            }
    policy["config_path"] = str(resolved)
    return policy


def evaluate_analysis_naming_status(
    *,
    workspace_dir: Path,
    policy: dict[str, object],
    analysis_mode: str,
    snapshot_date: str,
    default_outputs: dict[str, Path],
    compatibility_outputs: dict[str, Path],
    default_manifests: dict[str, Path],
    compatibility_manifests: dict[str, Path],
    pipeline_uses_canonical: bool,
    quality_uses_canonical: bool,
) -> dict[str, object]:
    baseline_manifest = _resolve_policy_path(workspace_dir, policy.get("baseline_manifest", DEFAULT_POLICY["baseline_manifest"]))
    manual_doc_value = str(policy.get("manual_consumer_clearance_doc", "")).strip()
    manual_consumer_doc = _resolve_policy_path(workspace_dir, manual_doc_value) if manual_doc_value else None
    baseline_names = _read_baseline_names(baseline_manifest)
    compatibility_names = {path.name for path in compatibility_outputs.values()} | {path.name for path in compatibility_manifests.values()}
    manifests_target_default_outputs = _manifests_target_default_outputs(default_outputs=default_outputs, default_manifests=default_manifests)
    manual_doc_present = manual_consumer_doc.exists() if manual_consumer_doc else bool(policy.get("manual_consumer_clearance", False))

    checks = {
        "pipeline_uses_canonical": bool(pipeline_uses_canonical),
        "quality_uses_canonical": bool(quality_uses_canonical),
        "canonical_outputs_present": all(path.exists() for path in default_outputs.values()),
        "canonical_manifests_present": all(path.exists() for path in default_manifests.values()),
        "canonical_manifests_target_canonical_outputs": manifests_target_default_outputs,
        "baseline_uses_canonical": not any(name in baseline_names for name in compatibility_names),
        "manual_consumer_clearance": bool(policy.get("manual_consumer_clearance", False)) and manual_doc_present,
        "manual_consumer_clearance_doc_present": manual_doc_present,
    }
    blockers: list[str] = []
    if not checks["pipeline_uses_canonical"]:
        blockers.append("pipeline 尚未完全消费 canonical analysis 命名")
    if not checks["quality_uses_canonical"]:
        blockers.append("quality / validation 仍存在 compatibility 命名依赖")
    if not checks["canonical_outputs_present"]:
        blockers.append("canonical analysis 输出未完整落盘")
    if not checks["canonical_manifests_present"]:
        blockers.append("canonical analysis manifest 未完整落盘")
    if not checks["canonical_manifests_target_canonical_outputs"]:
        blockers.append("canonical analysis manifest 的 output_path 仍未完全指向 canonical 文件")
    if not checks["baseline_uses_canonical"]:
        blockers.append("baseline/reference 仍绑定 compatibility 文件名")
    if not checks["manual_consumer_clearance"]:
        if manual_consumer_doc and not manual_doc_present:
            blockers.append("manual consumer clearance 文档不存在，无法确认人工消费链已切换")
        blockers.extend([str(item) for item in policy.get("manual_consumer_blockers", [])])

    requirements = policy.get("readiness_requirements", {})
    can_disable_now = True
    for check_name, required in requirements.items():
        if required and not checks.get(check_name, False):
            can_disable_now = False

    requested_compatibility = bool(policy.get("compatibility_write_enabled", True))
    effective_compatibility = requested_compatibility
    disable_request_blocked = False
    if not requested_compatibility and not can_disable_now and bool(policy.get("allow_disable_only_when_ready", True)):
        effective_compatibility = True
        disable_request_blocked = True

    required_actions = []
    if not checks["baseline_uses_canonical"]:
        required_actions.append("将 baseline/reference 从 compatibility 名切换到 canonical 名")
    if not checks["manual_consumer_clearance"]:
        required_actions.append("完成 Excel / 人工消费链对 canonical workbook / snapshot 的切换确认")
    if not checks["canonical_manifests_present"]:
        required_actions.append("补齐 canonical workbook / snapshot / anomaly manifest")
    if not checks["canonical_manifests_target_canonical_outputs"]:
        required_actions.append("修正 canonical manifest 的 output_path，使其与 canonical 文件名完全一致")
    if not checks["pipeline_uses_canonical"]:
        required_actions.append("让 pipeline 全量读取 canonical analysis 输出")
    if not checks["quality_uses_canonical"]:
        required_actions.append("让 quality / export 校验默认消费 canonical analysis 输出")

    dry_run_requested = bool(policy.get("dry_run_disable_compatibility", False))
    dry_run_result = {
        "requested": dry_run_requested,
        "status": "not_requested",
        "would_disable": False,
        "effective_compatibility_write_if_applied": effective_compatibility,
        "blocked": False,
    }
    if dry_run_requested:
        dry_run_result = {
            "requested": True,
            "status": "ready" if can_disable_now else "blocked",
            "would_disable": bool(can_disable_now),
            "effective_compatibility_write_if_applied": False if can_disable_now else True,
            "blocked": not can_disable_now,
        }

    disable_blockers_summary = "ready_to_disable" if can_disable_now else " | ".join(blockers)
    pre_disable_check = {
        "ready": can_disable_now,
        "check_count": len(checks),
        "blocker_count": len(blockers),
        "checks": checks,
        "blockers": blockers,
        "required_actions": required_actions,
    }

    return {
        "report_type": "analysis_output_naming_status",
        "analysis_mode": analysis_mode,
        "snapshot_date": snapshot_date,
        "policy_profile": policy.get("profile", ""),
        "config_path": policy.get("config_path", ""),
        "canonical_default": bool(policy.get("canonical_default", True)),
        "compatibility_write_requested": requested_compatibility,
        "compatibility_write_effective": effective_compatibility,
        "disable_request_blocked": disable_request_blocked,
        "can_disable_now": can_disable_now,
        "dry_run_disable_compatibility": dry_run_requested,
        "dry_run_result": dry_run_result,
        "pre_disable_check": pre_disable_check,
        "disable_blockers_summary": disable_blockers_summary,
        "checks": checks,
        "blockers": blockers,
        "required_actions": required_actions,
        "manual_consumer_clearance_doc": str(manual_consumer_doc) if manual_consumer_doc else "",
        "default_outputs": {key: str(path) for key, path in default_outputs.items()},
        "compatibility_outputs": {key: str(path) for key, path in compatibility_outputs.items()},
        "default_manifests": {key: str(path) for key, path in default_manifests.items()},
        "compatibility_manifests": {key: str(path) for key, path in compatibility_manifests.items()},
        "baseline_manifest": str(baseline_manifest),
    }


def write_analysis_naming_status(status_path: Path, status: dict[str, object]) -> Path:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return status_path


def _read_baseline_names(manifest_path: Path) -> set[str]:
    if not manifest_path.exists():
        return set()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()
    return {str(item.get("name", "")) for item in manifest.get("files", [])}


def _resolve_policy_path(workspace_dir: Path, configured_path: object) -> Path:
    path = Path(str(configured_path)).expanduser()
    return path if path.is_absolute() else workspace_dir / path


def _manifests_target_default_outputs(
    *,
    default_outputs: dict[str, Path],
    default_manifests: dict[str, Path],
) -> bool:
    for key, manifest_path in default_manifests.items():
        if key not in default_outputs:
            continue
        if not manifest_path.exists():
            return False
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return False
        if str(manifest.get("output_path", "")) != str(default_outputs[key]):
            return False
    return True
