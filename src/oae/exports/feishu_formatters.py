"""Formatting helpers for Feishu markdown/TSV exports."""

from __future__ import annotations

import re

import pandas as pd


def pct(v) -> str:
    if pd.isna(v):
        return "N/A"
    return f"{float(v) * 100:.2f}%"


def num(v, digits: int = 2) -> str:
    if pd.isna(v):
        return "N/A"
    return f"{float(v):.{digits}f}"


def num_trim(v, digits: int = 2) -> str:
    if pd.isna(v):
        return "N/A"
    value = f"{float(v):.{digits}f}"
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return value


def pct_text(v) -> str:
    if pd.isna(v):
        return "N/A"
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return "N/A"
    if s.endswith("%"):
        return s
    try:
        return f"{float(s) * 100:.2f}%"
    except (ValueError, TypeError):
        return s


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    if df.empty:
        return "\n".join(lines)
    lines.extend(("| " + df[cols].astype(str).agg(" | ".join, axis=1) + " |").tolist())
    return "\n".join(lines)


def tsv_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["\t".join(cols)]
    if df.empty:
        return "\n".join(lines)
    lines.extend(df[cols].astype(str).agg("\t".join, axis=1).tolist())
    return "\n".join(lines)


def sort_by_order(df: pd.DataFrame, order: list[str], scope_col: str = "scope_name") -> pd.DataFrame:
    out = df[df[scope_col].isin(order)].copy()
    out[scope_col] = pd.Categorical(out[scope_col], categories=order, ordered=True)
    return out.sort_values(scope_col)


def build_xy_column(
    df: pd.DataFrame,
    out_col: str,
    actual_col: str,
    target_col: str,
    actual_digits: int,
    target_digits: int,
    target_override_digits: int | None = None,
    target_override_mask: pd.Series | None = None,
) -> None:
    actual_text = df[actual_col].map(lambda v, d=actual_digits: f"{v:.{d}f}")
    target_text = df[target_col].map(lambda v, d=target_digits: f"{v:.{d}f}")
    if target_override_digits is not None and target_override_mask is not None:
        override = df[target_col].map(lambda v, d=target_override_digits: f"{v:.{d}f}")
        target_text = target_text.where(~target_override_mask, override)
    df[out_col] = actual_text + " / " + target_text


def format_pct_columns(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        df[column] = df[column].map(pct_text)


def add_num_columns(df: pd.DataFrame, config: dict[str, tuple[str, int]]) -> None:
    for out_col, (src_col, digits) in config.items():
        df[out_col] = df[src_col].map(lambda value, d=digits: num(value, d))


def format_parent_account_label(v, account_label_map: dict[str, str]) -> str:
    s = str(v).strip() if not pd.isna(v) else ""
    if not s:
        return ""
    parts = [x.strip() for x in re.split(r"[|/、,，;；]+", s) if x.strip()]
    if not parts:
        return account_label_map.get(s, s)

    seen = set()
    labels: list[str] = []
    for part in parts:
        label = account_label_map.get(part, part)
        if label and label not in seen:
            seen.add(label)
            labels.append(label)
    return " / ".join(labels)

