"""Monthly metric contract projections for report and BI consumers."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_CONTRACT_FILENAME = "monthly_metric_contract.json"

MONTHLY_TARGET_COLUMNS = [
    "month",
    "scope_type",
    "scope_name",
    "parent_account",
    "lead_target_month",
    "deal_target_month",
    "lead_cost_target_month",
    "cpl_target",
    "cps_target",
    "target_pool",
    "order_target_month",
]

SEED_MONTHLY_TARGET_COLUMNS = [
    "month",
    "scope_type",
    "scope_name",
    "parent_scope",
    "parent_account",
    "impression_target_month",
    "spend_target_month",
    "cpm_target",
    "target_pool",
]

REPORT_TOPLINE_REQUIRED_KEYS = {"full_account_targets", "ex7_rules", "pending_rules"}


def monthly_metric_contract_path_for(reference_path: str | Path) -> Path:
    """Return the sibling monthly contract path for an existing legacy config path."""

    return Path(reference_path).expanduser().resolve().parent / DEFAULT_CONTRACT_FILENAME


def load_monthly_metric_contract(path: str | Path) -> dict[str, Any]:
    contract_path = Path(path).expanduser().resolve()
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"monthly metric contract JSON invalid: {contract_path}, err={exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"monthly metric contract root must be an object: {contract_path}")
    months = payload.get("months")
    if not isinstance(months, dict) or not months:
        raise ValueError(f"monthly metric contract missing months: {contract_path}")
    return payload


def contract_has_month(contract: dict[str, Any], month: str | None) -> bool:
    if not month:
        return True
    months = contract.get("months")
    return isinstance(months, dict) and str(month).strip() in months


def project_monthly_targets(contract: dict[str, Any], month: str | None = None) -> pd.DataFrame:
    frames = [
        _records_to_frame(
            month_payload.get("monthly_targets", []),
            month=selected_month,
            columns=MONTHLY_TARGET_COLUMNS,
        )
        for selected_month, month_payload in _selected_month_payloads(contract, month)
    ]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=MONTHLY_TARGET_COLUMNS)


def project_seed_monthly_targets(contract: dict[str, Any], month: str | None = None) -> pd.DataFrame:
    frames = [
        _records_to_frame(
            month_payload.get("seed_monthly_targets", []),
            month=selected_month,
            columns=SEED_MONTHLY_TARGET_COLUMNS,
        )
        for selected_month, month_payload in _selected_month_payloads(contract, month)
    ]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=SEED_MONTHLY_TARGET_COLUMNS)


def project_report_topline_config(contract: dict[str, Any], month: str | None = None) -> dict[str, Any]:
    _, month_payload = _month_payload(contract, month)
    topline_config = month_payload.get("report_topline_config")
    if not isinstance(topline_config, dict):
        raise ValueError("monthly metric contract missing report_topline_config")
    missing = REPORT_TOPLINE_REQUIRED_KEYS - set(topline_config.keys())
    if missing:
        raise ValueError(f"monthly metric contract report_topline_config missing keys: {sorted(missing)}")
    return deepcopy(topline_config)


def _month_payload(contract: dict[str, Any], month: str | None) -> tuple[str, dict[str, Any]]:
    return _selected_month_payloads(contract, month or _latest_month(contract))[0]


def _selected_month_payloads(contract: dict[str, Any], month: str | None) -> list[tuple[str, dict[str, Any]]]:
    months = contract.get("months")
    if not isinstance(months, dict) or not months:
        raise ValueError("monthly metric contract missing months")

    selected_months = [str(month).strip()] if month else sorted(str(key) for key in months.keys())
    payloads: list[tuple[str, dict[str, Any]]] = []
    for selected_month in selected_months:
        payload = months.get(selected_month)
        if not isinstance(payload, dict):
            raise ValueError(f"monthly metric contract missing month: {selected_month}")
        payloads.append((selected_month, payload))
    return payloads


def _latest_month(contract: dict[str, Any]) -> str:
    months = contract.get("months")
    if not isinstance(months, dict) or not months:
        raise ValueError("monthly metric contract missing months")
    return sorted(str(key) for key in months.keys())[-1]


def _records_to_frame(records: Any, *, month: str, columns: list[str]) -> pd.DataFrame:
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError(f"monthly metric contract section must be a list for month: {month}")

    rows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"monthly metric contract record must be an object for month: {month}")
        row = {column: record.get(column) for column in columns}
        row["month"] = month
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)
