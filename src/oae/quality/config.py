"""Quality-threshold configuration loading with profile support."""

from __future__ import annotations

import json
from pathlib import Path


DEFAULT_QUALITY_THRESHOLD_PROFILES = {
    "regression": {
        "fact": {
            "row_count": {"mode": "relative", "warning": 0.005, "fail": 0.02},
            "phone_missing_rate": {"mode": "absolute", "warning": 0.005, "fail": 0.01},
            "unowned_ratio": {"mode": "absolute", "warning": 0.02, "fail": 0.03},
            "attribution_success_rate": {"mode": "absolute", "warning": 0.02, "fail": 0.03},
        },
        "snapshot": {
            "total_mtd_leads": {"mode": "relative", "warning": 0.02, "fail": 0.05},
            "total_mtd_deals": {"mode": "absolute", "warning": 1.0, "fail": 2.0},
            "total_mtd_spend": {"mode": "relative", "warning": 0.02, "fail": 0.05},
            "mtd_lead_attain": {"mode": "absolute", "warning": 0.01, "fail": 0.03},
            "mtd_deal_attain": {"mode": "absolute", "warning": 0.01, "fail": 0.03},
        },
        "ledger": {
            "duplicate_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "null_field_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "mismatch_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
        },
        "analysis": {
            "missing_subject_area_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "min_total_row_count": {"mode": "minimum", "warning": 1.0, "fail": 1.0},
        },
    },
    "operational": {
        "fact": {
            "row_count": {"mode": "relative", "warning": 0.02, "fail": 0.05},
            "phone_missing_rate": {"mode": "absolute", "warning": 0.01, "fail": 0.02},
            "unowned_ratio": {"mode": "absolute", "warning": 0.03, "fail": 0.05},
            "attribution_success_rate": {"mode": "absolute", "warning": 0.03, "fail": 0.05},
        },
        "snapshot": {
            "total_mtd_leads": {"mode": "relative", "warning": 0.05, "fail": 0.10},
            "total_mtd_deals": {"mode": "absolute", "warning": 1.0, "fail": 3.0},
            "total_mtd_spend": {"mode": "relative", "warning": 0.05, "fail": 0.10},
            "mtd_lead_attain": {"mode": "absolute", "warning": 0.02, "fail": 0.05},
            "mtd_deal_attain": {"mode": "absolute", "warning": 0.02, "fail": 0.05},
        },
        "ledger": {
            "duplicate_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "null_field_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "mismatch_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
        },
        "analysis": {
            "missing_subject_area_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "min_total_row_count": {"mode": "minimum", "warning": 1.0, "fail": 1.0},
        },
    },
    "settlement": {
        "fact": {
            "row_count": {"mode": "relative", "warning": 0.01, "fail": 0.02},
            "phone_missing_rate": {"mode": "absolute", "warning": 0.005, "fail": 0.01},
            "unowned_ratio": {"mode": "absolute", "warning": 0.02, "fail": 0.03},
            "attribution_success_rate": {"mode": "absolute", "warning": 0.02, "fail": 0.03},
        },
        "snapshot": {
            "total_mtd_leads": {"mode": "relative", "warning": 0.02, "fail": 0.05},
            "total_mtd_deals": {"mode": "absolute", "warning": 1.0, "fail": 2.0},
            "total_mtd_spend": {"mode": "relative", "warning": 0.02, "fail": 0.05},
            "mtd_lead_attain": {"mode": "absolute", "warning": 0.01, "fail": 0.03},
            "mtd_deal_attain": {"mode": "absolute", "warning": 0.01, "fail": 0.03},
        },
        "ledger": {
            "duplicate_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "null_field_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "mismatch_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
        },
        "analysis": {
            "missing_subject_area_count": {"mode": "absolute", "warning": 0.0, "fail": 0.0},
            "min_total_row_count": {"mode": "minimum", "warning": 1.0, "fail": 1.0},
        },
    },
}


def load_quality_thresholds(
    config_path: Path | None = None,
    *,
    profile: str = "operational",
) -> tuple[dict[str, object], str, str]:
    profiles = json.loads(json.dumps(DEFAULT_QUALITY_THRESHOLD_PROFILES))
    source = "built-in defaults"
    if config_path is not None and config_path.exists():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if "profiles" in data:
            _deep_update(profiles, data["profiles"])
        else:
            _deep_update(profiles.setdefault("operational", {}), data)
        source = str(config_path)
    selected_profile = profile if profile in profiles else "operational"
    return profiles[selected_profile], source, selected_profile


def _deep_update(base: dict[str, object], updates: dict[str, object]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)  # type: ignore[index]
        else:
            base[key] = value
