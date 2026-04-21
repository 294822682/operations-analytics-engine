from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Mapping

from oae.utils import extract_date_from_text


REPORT_DATE_SEMANTICS_VERSION = "report-date-v1"


def build_report_date_semantics() -> dict[str, object]:
    return {
        "contract_version": REPORT_DATE_SEMANTICS_VERSION,
        "canonical_field": "canonical_report_date",
        "compatibility_alias_fields": ["report_date", "resolved_report_date"],
        "raw_value_fields": ["manifest_report_date"],
        "latest_alias_semantics": "latest in file names is a naming alias; the date token maps to canonical_report_date.",
    }


def build_report_date_contract(
    *,
    manifest_report_date: str,
    canonical_report_date: str,
    resolved_report_date: str = "",
) -> dict[str, Any]:
    canonical = str(canonical_report_date or "").strip()
    manifest = str(manifest_report_date or "").strip()
    resolved = canonical if canonical else str(resolved_report_date or "").strip()
    return {
        "report_date": canonical,
        "manifest_report_date": manifest,
        "canonical_report_date": canonical,
        "resolved_report_date": resolved,
        "report_date_semantics": build_report_date_semantics(),
    }


def build_report_date_resolution(
    *,
    manifest_report_date: str,
    primary_paths: Mapping[str, str | Path],
    fallback_paths: Mapping[str, str | Path] | None = None,
) -> dict[str, Any]:
    primary_output_dates = _extract_output_dates(primary_paths)
    fallback_output_dates = _extract_output_dates(fallback_paths or {})

    selected_source, canonical_report_date = _pick_first_date(primary_output_dates)
    selected_source_tier = "primary"
    if not canonical_report_date:
        selected_source, canonical_report_date = _pick_first_date(fallback_output_dates)
        selected_source_tier = "fallback"
    if not canonical_report_date:
        selected_source = "manifest_report_date"
        canonical_report_date = manifest_report_date
        selected_source_tier = "manifest"

    return {
        "manifest_report_date": manifest_report_date,
        "canonical_report_date": canonical_report_date,
        "resolved_report_date": canonical_report_date,
        "selected_source": selected_source,
        "selected_source_tier": selected_source_tier,
        "primary_output_dates": primary_output_dates,
        "fallback_output_dates": fallback_output_dates,
        "has_primary_conflict": _has_date_conflict(primary_output_dates),
        "has_fallback_conflict": _has_date_conflict(fallback_output_dates),
    }


def resolve_manifest_report_dates(manifest: Mapping[str, Any]) -> dict[str, Any]:
    manifest_report_date = str(
        manifest.get("manifest_report_date", "") or manifest.get("report_date", "")
    ).strip()
    canonical_report_date = str(manifest.get("canonical_report_date", "")).strip()
    resolved_report_date = str(manifest.get("resolved_report_date", "")).strip()
    report_date_resolution = manifest.get("report_date_resolution", {})
    if canonical_report_date:
        return {
            **build_report_date_contract(
                manifest_report_date=manifest_report_date,
                canonical_report_date=canonical_report_date,
                resolved_report_date=resolved_report_date,
            ),
            "report_date_resolution": report_date_resolution if isinstance(report_date_resolution, dict) else {},
        }
    if resolved_report_date:
        return {
            **build_report_date_contract(
                manifest_report_date=manifest_report_date,
                canonical_report_date=resolved_report_date,
                resolved_report_date=resolved_report_date,
            ),
            "report_date_resolution": report_date_resolution if isinstance(report_date_resolution, dict) else {},
        }

    primary_paths = manifest.get("report_output_paths", {})
    if not isinstance(primary_paths, dict):
        primary_paths = {}
    analysis_output_paths = manifest.get("analysis_output_default_paths", {})
    if not isinstance(analysis_output_paths, dict):
        analysis_output_paths = {}
    fallback_paths = {
        "analysis_snapshot": str(analysis_output_paths.get("snapshot", "")),
        "analysis_workbook": str(analysis_output_paths.get("workbook", "")),
    }
    derived_resolution = build_report_date_resolution(
        manifest_report_date=manifest_report_date,
        primary_paths={key: str(value) for key, value in primary_paths.items()},
        fallback_paths=fallback_paths,
    )
    return {
        **build_report_date_contract(
            manifest_report_date=manifest_report_date,
            canonical_report_date=str(derived_resolution.get("canonical_report_date", "")).strip(),
            resolved_report_date=str(derived_resolution.get("resolved_report_date", "")).strip(),
        ),
        "report_date_resolution": derived_resolution,
    }


def derive_target_report_date(
    *,
    source_report_date: str,
    targets_path: Path,
) -> dict[str, str]:
    target_month = load_latest_target_month(targets_path)
    canonical_report_date = source_report_date
    alignment_rule = "source_report_date"
    if target_month and source_report_date and source_report_date[:7] < target_month:
        canonical_report_date = f"{target_month}-01"
        alignment_rule = "target_month_start"
    return {
        "source_report_date": source_report_date,
        "canonical_report_date": canonical_report_date,
        "target_month": target_month,
        "alignment_rule": alignment_rule,
    }


def load_latest_target_month(targets_path: Path) -> str:
    months: set[str] = set()
    with targets_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            month = str((row or {}).get("month", "")).strip()
            if month:
                months.add(month)
    return sorted(months)[-1] if months else ""


def _extract_output_dates(paths: Mapping[str, str | Path]) -> dict[str, str]:
    extracted: dict[str, str] = {}
    for key, value in paths.items():
        raw_value = str(value or "").strip()
        if not raw_value:
            continue
        extracted[str(key)] = extract_date_from_text(raw_value)
    return extracted


def _pick_first_date(dates: Mapping[str, str]) -> tuple[str, str]:
    for key, value in dates.items():
        if value:
            return str(key), str(value)
    return "", ""


def _has_date_conflict(dates: Mapping[str, str]) -> bool:
    non_empty = {value for value in dates.values() if value}
    return len(non_empty) > 1
