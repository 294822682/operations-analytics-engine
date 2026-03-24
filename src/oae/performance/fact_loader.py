"""Fact loader for performance runtime."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.overrides import load_fact_with_manual_overrides
from oae.performance.loader_utils import normalize_account


def load_fact(path: Path, manual_override_path: Path | None = None) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[ERROR] fact csv not found: {path}")
    df = load_fact_with_manual_overrides(path, manual_override_path=manual_override_path)

    if "线索ID_norm" not in df.columns:
        if "线索ID" in df.columns:
            df["线索ID_norm"] = df["线索ID"].astype(str).str.strip()
        else:
            raise SystemExit("[ERROR] fact csv 缺少 线索ID_norm/线索ID")

    if "date" not in df.columns:
        raise SystemExit("[ERROR] fact csv 缺少 date 列")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    if "成交时间" in df.columns:
        df["成交时间"] = pd.to_datetime(df["成交时间"], errors="coerce")
    else:
        df["成交时间"] = pd.NaT
    df["deal_date"] = df["成交时间"].dt.normalize()
    df["标准账号"] = df["标准账号"].apply(normalize_account) if "标准账号" in df.columns else ""
    for col in ["自动标准账号", "最终标准账号", "专项归属目标账号"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_account)
    df["is_deal"] = pd.to_numeric(df["is_deal"], errors="coerce").fillna(0) if "is_deal" in df.columns else 0
    return df
