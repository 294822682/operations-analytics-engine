from __future__ import annotations

import re
from typing import Any, Mapping


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

    derived_date = _derive_report_date(manifest)
    return {
        **build_report_date_contract(
            manifest_report_date=manifest_report_date,
            canonical_report_date=derived_date or manifest_report_date,
            resolved_report_date=derived_date or manifest_report_date,
        ),
        "report_date_resolution": report_date_resolution if isinstance(report_date_resolution, dict) else {},
    }


def _derive_report_date(manifest: Mapping[str, Any]) -> str:
    for key in ("report_output_paths", "analysis_output_default_paths"):
        value = manifest.get(key, {})
        if isinstance(value, Mapping):
            for path_value in value.values():
                extracted = _extract_date(str(path_value or ""))
                if extracted:
                    return extracted
    return ""


def _extract_date(value: str) -> str:
    match = re.search(r"\d{4}-\d{2}-\d{2}", value)
    return match.group(0) if match else ""
