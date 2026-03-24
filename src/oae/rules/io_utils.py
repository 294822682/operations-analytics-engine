"""Shared IO helpers for mixed Excel/CSV inputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def resolve_path(base: Path, raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else (base / path).resolve()


def read_csv_auto(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "gb18030", "gbk"]
    last_error: Exception | None = None
    for encoding in encodings:
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    raise ValueError(f"无法识别 CSV 编码: {path}; 最近错误: {last_error}")


def read_csv_header_auto(path: Path) -> list[str]:
    encodings = ["utf-8-sig", "gb18030", "gbk"]
    last_error: Exception | None = None
    for encoding in encodings:
        try:
            return [str(col).strip() for col in pd.read_csv(path, encoding=encoding, nrows=0).columns]
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    raise ValueError(f"无法识别 CSV 编码: {path}; 最近错误: {last_error}")


def read_table_auto(path: Path, preferred_sheets: list[str] | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_auto(path)
    if suffix not in {".xlsx", ".xls"}:
        raise ValueError(f"不支持的文件格式: {path}")

    workbook = pd.ExcelFile(path)
    sheet_name = workbook.sheet_names[0]
    for candidate in preferred_sheets or []:
        if candidate in workbook.sheet_names:
            sheet_name = candidate
            break
    return pd.read_excel(path, sheet_name=sheet_name)


def read_table_header_auto(path: Path, preferred_sheets: list[str] | None = None) -> tuple[list[str], str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_header_auto(path), ""
    if suffix not in {".xlsx", ".xls"}:
        raise ValueError(f"不支持的文件格式: {path}")

    workbook = pd.ExcelFile(path)
    sheet_name = workbook.sheet_names[0]
    for candidate in preferred_sheets or []:
        if candidate in workbook.sheet_names:
            sheet_name = candidate
            break
    columns = [str(col).strip() for col in pd.read_excel(path, sheet_name=sheet_name, nrows=0).columns]
    return columns, sheet_name
