"""Baseline freezing and comparison helpers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


METADATA_COLUMNS = {"schema_version", "metric_version", "run_id", "template_version", "freeze_id", "snapshot_date"}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_frame(frame: pd.DataFrame) -> str:
    normalized = frame.copy()
    normalized = normalized.reindex(sorted(normalized.columns), axis=1)
    normalized = normalized.fillna("")
    payload = normalized.to_csv(index=False, encoding="utf-8").encode("utf-8")
    return _sha256_bytes(payload)


def _describe_tabular_file(path: Path) -> dict[str, object]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path)
        delimiter = ","
    elif suffix == ".tsv":
        frame = pd.read_csv(path, sep="\t")
        delimiter = "\t"
    else:
        raise ValueError(f"unsupported tabular file: {path}")

    core_columns = [column for column in frame.columns if column not in METADATA_COLUMNS]
    core_frame = frame[core_columns].copy()
    return {
        "kind": "tabular",
        "delimiter": delimiter,
        "row_count": int(len(frame)),
        "columns": list(frame.columns),
        "core_columns": core_columns,
        "core_sha256": _hash_frame(core_frame),
    }


def _describe_excel_file(path: Path) -> dict[str, object]:
    workbook = pd.read_excel(path, sheet_name=None)
    sheets = []
    for sheet_name, frame in workbook.items():
        sheets.append(
            {
                "sheet_name": sheet_name,
                "row_count": int(len(frame)),
                "columns": list(frame.columns),
                "core_sha256": _hash_frame(frame),
            }
        )
    return {"kind": "excel", "sheets": sheets}


def describe_file(path: Path) -> dict[str, object]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    descriptor = {
        "path": str(path),
        "name": path.name,
        "sha256": digest,
        "size_bytes": path.stat().st_size,
    }
    try:
        if path.suffix.lower() in {".csv", ".tsv"}:
            descriptor.update(_describe_tabular_file(path))
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            descriptor.update(_describe_excel_file(path))
    except Exception as exc:
        descriptor["signature_error"] = str(exc)
    return descriptor


def freeze_reference_manifest(reference_files: list[Path], manifest_path: Path) -> Path:
    manifest = {"files": [describe_file(path) for path in reference_files if path.exists()]}
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def compare_files_against_manifest(
    manifest_path: Path,
    generated_files: list[Path],
) -> dict[str, object]:
    if not manifest_path.exists():
        return {"status": "missing-baseline-manifest", "details": []}

    expected = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_map = {item["name"]: item for item in expected.get("files", [])}
    details: list[dict[str, object]] = []
    for path in generated_files:
        if not path.exists():
            details.append({"name": path.name, "status": "missing-generated", "category": "contract violation"})
            continue
        current = describe_file(path)
        baseline = expected_map.get(path.name)
        if baseline is None:
            details.append({"name": path.name, "status": "new-file", "category": "structural change", "sha256": current["sha256"]})
            continue
        if baseline.get("sha256") == current["sha256"]:
            details.append({"name": path.name, "status": "match", "category": "pass"})
            continue

        if baseline.get("kind") == "tabular" and current.get("kind") == "tabular":
            if baseline.get("core_sha256") == current.get("core_sha256"):
                details.append(
                    {
                        "name": path.name,
                        "status": "metadata-only change",
                        "category": "metadata-only change",
                        "baseline_sha256": baseline["sha256"],
                        "current_sha256": current["sha256"],
                    }
                )
                continue
            if _is_safe_tabular_extension(Path(str(baseline.get("path", ""))), path):
                details.append(
                    {
                        "name": path.name,
                        "status": "safe structural change",
                        "category": "safe structural change",
                        "reason": "schema extension with stable shared-core data",
                    }
                )
                continue
            if baseline.get("core_columns") != current.get("core_columns") or baseline.get("row_count") != current.get("row_count"):
                details.append(
                    {
                        "name": path.name,
                        "status": "structural change",
                        "category": "structural change",
                        "baseline_core_columns": baseline.get("core_columns"),
                        "current_core_columns": current.get("core_columns"),
                        "baseline_row_count": baseline.get("row_count"),
                        "current_row_count": current.get("row_count"),
                    }
                )
                continue
        if baseline.get("kind") == "excel" and current.get("kind") == "excel":
            if baseline.get("sheets") == current.get("sheets"):
                details.append(
                    {
                        "name": path.name,
                        "status": "metadata-only change",
                        "category": "metadata-only change",
                        "baseline_sha256": baseline["sha256"],
                        "current_sha256": current["sha256"],
                    }
                )
                continue
            if _is_safe_excel_extension(Path(str(baseline.get("path", ""))), path):
                details.append(
                    {
                        "name": path.name,
                        "status": "safe structural change",
                        "category": "safe structural change",
                        "reason": "sheet/column extension with stable shared-core data",
                    }
                )
                continue
        details.append(
            {
                "name": path.name,
                "status": "structural change",
                "category": "structural change",
                "baseline_sha256": baseline["sha256"],
                "current_sha256": current["sha256"],
            }
        )
    overall = "pass"
    if any(item["category"] == "contract violation" for item in details):
        overall = "fail"
    elif any(item["category"] in {"structural change", "metadata-only change"} and item["status"] != "match" for item in details):
        overall = "warning"
    return {"status": overall, "details": details}


def _is_safe_tabular_extension(baseline_path: Path, current_path: Path) -> bool:
    if not baseline_path.exists() or not current_path.exists():
        return False
    sep = "\t" if baseline_path.suffix.lower() == ".tsv" else ","
    baseline = pd.read_csv(baseline_path, sep=sep)
    current = pd.read_csv(current_path, sep=sep)
    baseline_core_columns = [column for column in baseline.columns if column not in METADATA_COLUMNS]
    current_core_columns = [column for column in current.columns if column not in METADATA_COLUMNS]
    if len(baseline) != len(current):
        return False
    if not set(baseline_core_columns).issubset(set(current_core_columns)):
        return False
    shared_current = current[baseline_core_columns].copy()
    return _hash_frame(shared_current) == _hash_frame(baseline[baseline_core_columns].copy())


def _is_safe_excel_extension(baseline_path: Path, current_path: Path) -> bool:
    if not baseline_path.exists() or not current_path.exists():
        return False
    baseline_book = pd.read_excel(baseline_path, sheet_name=None)
    current_book = pd.read_excel(current_path, sheet_name=None)
    if not set(baseline_book).issubset(set(current_book)):
        return False
    for sheet_name, baseline_frame in baseline_book.items():
        current_frame = current_book[sheet_name]
        if len(baseline_frame) != len(current_frame):
            return False
        if not set(baseline_frame.columns).issubset(set(current_frame.columns)):
            return False
        shared_current = current_frame[list(baseline_frame.columns)].copy()
        if _hash_frame(shared_current) != _hash_frame(baseline_frame.copy()):
            return False
    return True
