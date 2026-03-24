"""Discover and validate runtime inputs before the pipeline starts."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from oae.rules.file_discovery import parse_date_from_filename, parse_year_month_from_live_filename

from .input_validator import validate_source_file
from .source_registry import InputRegistry, InputSourceContract, load_input_registry


def discover_runtime_inputs(
    *,
    workspace: Path,
    run_id: str,
    config_path: Path | None = None,
    dynamic_dir_override: str = "",
    path_overrides: dict[str, str] | None = None,
) -> tuple[dict[str, object], dict[str, Path]]:
    registry = load_input_registry(workspace, config_path=config_path)
    resolved_inputs: dict[str, Path] = {}
    records: list[dict[str, object]] = []
    path_overrides = path_overrides or {}

    for key, contract in registry.sources.items():
        record, selected_path = _resolve_one_source(
            workspace=workspace,
            registry=registry,
            contract=contract,
            dynamic_dir_override=dynamic_dir_override,
            explicit_path=path_overrides.get(key, "").strip(),
        )
        records.append(record)
        resolved_inputs[key] = selected_path

    manifest = {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "pass",
        "source_config": str(registry.config_path),
        "dynamic_input_root": str((workspace / (dynamic_dir_override or registry.default_dynamic_input_dir)).resolve()),
        "sources": records,
        "selected_inputs": {item["source_key"]: item["path"] for item in records},
    }
    return manifest, resolved_inputs


def _resolve_one_source(
    *,
    workspace: Path,
    registry: InputRegistry,
    contract: InputSourceContract,
    dynamic_dir_override: str,
    explicit_path: str,
) -> tuple[dict[str, object], Path]:
    if contract.kind not in {"dynamic", "fixed"}:
        raise SystemExit(f"[ERROR] 输入契约非法：{contract.label} 的 kind 必须是 dynamic/fixed")

    if explicit_path:
        path = Path(explicit_path).expanduser()
        selected_path = path if path.is_absolute() else (workspace / path).resolve()
        if not selected_path.exists():
            raise SystemExit(f"[ERROR] {contract.label} 指定路径不存在：{selected_path}")
        naming = _check_naming(selected_path, contract)
        validation = validate_source_file(selected_path, contract)
        _raise_if_validation_failed(contract, selected_path, validation, [selected_path], naming["anomalies"])
        return (
            _build_record(
                contract=contract,
                path=selected_path,
                candidates=[selected_path],
                validation=validation,
                naming=naming,
                selection_basis="显式路径优先",
                business_date=_extract_business_date(selected_path, contract),
                used_directory=str(selected_path.parent),
                selected_by="explicit_path",
            ),
            selected_path,
        )

    if contract.kind == "fixed":
        if not contract.path:
            raise SystemExit(f"[ERROR] 输入契约缺少固定路径：{contract.label}")
        selected_path = (workspace / contract.path).resolve()
        if not selected_path.exists():
            raise SystemExit(f"[ERROR] 缺少固定输入文件：{contract.label}，应在 {selected_path}")
        naming = _check_naming(selected_path, contract)
        validation = validate_source_file(selected_path, contract)
        _raise_if_validation_failed(contract, selected_path, validation, [selected_path], naming["anomalies"])
        return (
            _build_record(
                contract=contract,
                path=selected_path,
                candidates=[selected_path],
                validation=validation,
                naming=naming,
                selection_basis="固定路径",
                business_date=_extract_business_date(selected_path, contract),
                used_directory=str(selected_path.parent),
                selected_by="fixed_path",
            ),
            selected_path,
        )

    directory = (workspace / (dynamic_dir_override or contract.directory or registry.default_dynamic_input_dir)).resolve()
    if not directory.exists():
        raise SystemExit(f"[ERROR] {contract.label} 输入目录不存在：{directory}")

    candidates = _scan_candidates(directory, contract)
    if not candidates:
        patterns_text = "，".join(contract.glob_patterns)
        raise SystemExit(f"[ERROR] 未找到 {contract.label}：扫描目录 {directory}，规则 {patterns_text}")

    canonical, anomalies = _split_canonical_candidates(candidates, contract)
    if not canonical:
        names = "，".join(path.name for path in candidates)
        raise SystemExit(
            f"[ERROR] {contract.label} 扫描到文件，但命名均不符合规范：目录={directory}，候选={names}，"
            f"规范={contract.naming_regex or contract.naming_exact}"
        )

    selected_path, selection_basis = _pick_latest_candidate(contract, canonical)
    validation = validate_source_file(selected_path, contract)
    _raise_if_validation_failed(contract, selected_path, validation, candidates, anomalies)
    naming = {
        "status": "pass" if not anomalies else "warning",
        "anomalies": [path.name for path in anomalies],
    }
    return (
        _build_record(
            contract=contract,
            path=selected_path,
            candidates=candidates,
            validation=validation,
            naming=naming,
            selection_basis=selection_basis,
            business_date=_extract_business_date(selected_path, contract),
            used_directory=str(directory),
            selected_by="auto_discovery",
        ),
        selected_path,
    )


def _scan_candidates(directory: Path, contract: InputSourceContract) -> list[Path]:
    matched: list[Path] = []
    for pattern in contract.glob_patterns:
        matched.extend(path.resolve() for path in directory.glob(pattern))
    unique = sorted(set(path for path in matched if path.is_file()))
    if contract.file_types:
        unique = [path for path in unique if path.suffix.lower() in contract.file_types]
    return unique


def _split_canonical_candidates(candidates: list[Path], contract: InputSourceContract) -> tuple[list[Path], list[Path]]:
    canonical: list[Path] = []
    anomalies: list[Path] = []
    for path in candidates:
        naming = _check_naming(path, contract)
        if naming["status"] == "pass":
            canonical.append(path)
        else:
            anomalies.append(path)
    return canonical, anomalies


def _check_naming(path: Path, contract: InputSourceContract) -> dict[str, object]:
    if contract.naming_exact:
        return {"status": "pass" if path.name == contract.naming_exact else "fail", "anomalies": [] if path.name == contract.naming_exact else [path.name]}
    if contract.naming_regex:
        matched = re.match(contract.naming_regex, path.name)
        return {"status": "pass" if matched else "fail", "anomalies": [] if matched else [path.name]}
    return {"status": "pass", "anomalies": []}


def _pick_latest_candidate(contract: InputSourceContract, candidates: list[Path]) -> tuple[Path, str]:
    by_business_date: dict[str, list[Path]] = {}
    for path in candidates:
        business_date = _extract_business_date(path, contract)
        if not business_date:
            raise SystemExit(
                f"[ERROR] {contract.label} 文件命名不完整，无法识别业务日期：{path.name}；"
                f"请按规范命名后重试"
            )
        by_business_date.setdefault(business_date, []).append(path)

    latest_business_date = sorted(by_business_date)[-1]
    latest_candidates = sorted(by_business_date[latest_business_date], key=lambda item: item.stat().st_mtime, reverse=True)
    if len(latest_candidates) > 1:
        names = "，".join(path.name for path in latest_candidates)
        raise SystemExit(
            f"[ERROR] {contract.label} 存在多个同业务日期候选，无法安全判定：业务日期={latest_business_date}，候选={names}；"
            f"请保留一个正式文件或显式指定路径"
        )
    selected = latest_candidates[0]
    return selected, f"{contract.selection_rule}；本次选中业务日期 {latest_business_date}"


def _extract_business_date(path: Path, contract: InputSourceContract) -> str:
    if contract.business_date_type == "date":
        parsed = parse_date_from_filename(path)
        return parsed.isoformat() if parsed is not None else ""
    if contract.business_date_type == "month":
        year_month = parse_year_month_from_live_filename(path)
        if year_month is None:
            return ""
        return f"{year_month[0]:04d}-{year_month[1]:02d}"
    return ""


def _raise_if_validation_failed(
    contract: InputSourceContract,
    selected_path: Path,
    validation: dict[str, object],
    candidates: list[Path],
    anomalies: list[Path],
) -> None:
    if validation.get("status") != "pass":
        missing_messages = validation.get("missing_messages", [])
        columns = validation.get("columns", [])
        candidates_text = "，".join(path.name for path in candidates)
        raise SystemExit(
            f"[ERROR] {contract.label} 字段校验失败：文件={selected_path.name}；候选={candidates_text}；"
            f"缺失/异常={missing_messages}；当前字段={columns}"
        )
    if anomalies:
        anomaly_names = "，".join(path.name for path in anomalies)
        print(f"[WARN] {contract.label} 发现命名不规范候选，已忽略：{anomaly_names}")
    for message in validation.get("warnings", []):
        print(f"[WARN] {contract.label}：{message}")


def _build_record(
    *,
    contract: InputSourceContract,
    path: Path,
    candidates: list[Path],
    validation: dict[str, object],
    naming: dict[str, object],
    selection_basis: str,
    business_date: str,
    used_directory: str,
    selected_by: str,
) -> dict[str, object]:
    return {
        "source_key": contract.key,
        "label": contract.label,
        "kind": contract.kind,
        "path": str(path),
        "filename": path.name,
        "directory": used_directory,
        "last_modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
        "business_date": business_date,
        "selected_by": selected_by,
        "selection_basis": selection_basis,
        "allow_multiple_versions": contract.allow_multiple_versions,
        "candidate_count": len(candidates),
        "candidates": [candidate.name for candidate in candidates],
        "naming_status": naming.get("status", "pass"),
        "naming_anomalies": naming.get("anomalies", []),
        "validation_status": validation.get("status", "pass"),
        "validation_summary": validation.get("summary", ""),
        "sheet_name": validation.get("sheet_name", ""),
        "matched_fields": {
            item["alias_key"]: item.get("matched_column", "")
            for item in validation.get("alias_results", [])
            if item.get("matched_column")
        },
        "header_columns": validation.get("columns", []),
        "validation_warnings": validation.get("warnings", []),
    }


def dump_input_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
