"""Validate input headers against formal contracts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.rules.columns import COLUMN_ALIASES, pick_col
from oae.rules.io_utils import read_table_header_auto

from .source_registry import InputSourceContract


def validate_source_file(path: Path, contract: InputSourceContract) -> dict[str, object]:
    columns, sheet_name = read_table_header_auto(path, preferred_sheets=contract.preferred_sheets)
    header_frame = pd.DataFrame(columns=columns)

    alias_results: list[dict[str, object]] = []
    exact_results: list[dict[str, object]] = []
    missing_messages: list[str] = []
    warnings: list[str] = []

    for alias_key in contract.required_alias_keys:
        alias_name = COLUMN_ALIASES.get(alias_key, [])
        try:
            matched_column = pick_col(header_frame, alias_name, required=True)
            alias_results.append(
                {
                    "alias_key": alias_key,
                    "accepted_names": alias_name,
                    "matched_column": matched_column,
                    "required": True,
                    "status": "pass",
                }
            )
        except ValueError as exc:
            alias_results.append(
                {
                    "alias_key": alias_key,
                    "accepted_names": alias_name,
                    "matched_column": "",
                    "required": True,
                    "status": "fail",
                    "message": str(exc),
                }
            )
            missing_messages.append(f"{alias_key} 缺失，可接受列名={alias_name}")

    for alias_key in contract.optional_alias_keys:
        alias_name = COLUMN_ALIASES.get(alias_key, [])
        try:
            matched_column = pick_col(header_frame, alias_name, required=False)
            alias_results.append(
                {
                    "alias_key": alias_key,
                    "accepted_names": alias_name,
                    "matched_column": matched_column or "",
                    "required": False,
                    "status": "pass" if matched_column else "warning",
                }
            )
            if not matched_column:
                warnings.append(f"{alias_key} 未命中可选列，可接受列名={alias_name}")
        except ValueError as exc:
            alias_results.append(
                {
                    "alias_key": alias_key,
                    "accepted_names": alias_name,
                    "matched_column": "",
                    "required": False,
                    "status": "warning",
                    "message": str(exc),
                }
            )
            warnings.append(f"{alias_key} 列名存在冲突：{exc}")

    for exact_name in contract.required_exact_fields:
        exists = exact_name in columns
        exact_results.append(
            {
                "field": exact_name,
                "required": True,
                "status": "pass" if exists else "fail",
            }
        )
        if not exists:
            missing_messages.append(f"缺少固定字段 {exact_name}")

    for exact_name in contract.recommended_exact_fields:
        exists = exact_name in columns
        exact_results.append(
            {
                "field": exact_name,
                "required": False,
                "status": "pass" if exists else "warning",
            }
        )
        if not exists:
            warnings.append(f"建议字段 {exact_name} 未出现")

    status = "pass" if not missing_messages else "fail"
    summary = "字段校验通过" if status == "pass" else "字段校验失败"
    return {
        "status": status,
        "summary": summary,
        "sheet_name": sheet_name,
        "columns": columns,
        "alias_results": alias_results,
        "exact_results": exact_results,
        "warnings": warnings,
        "missing_messages": missing_messages,
    }
