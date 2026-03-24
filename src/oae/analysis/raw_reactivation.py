"""Raw-evidence reactivation analysis."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from oae.rules.io_utils import read_table_auto
from transform.lead_transform import ensure_str, parse_excel_mixed_datetime


def build_reactivation_tables(
    input_file: str,
    sheet_name: str,
    expected_columns: list[str],
    time_date_columns: list[str],
):
    required_cols = [
        "线索ID", "手机号", "渠道3", "线索状态", "创建时间", "创建日期",
        "到店时间", "到店日期", "成交时间", "成交日期", "战败时间", "战败日期",
    ]
    path = Path(input_file)
    if not path.exists():
        summary = pd.DataFrame([{"指标": "原始文件不存在，无法计算战败回收", "数值": np.nan}])
        detail = pd.DataFrame(columns=["渠道3", "再激活线索量", "再激活后到店率", "再激活后成交率"])
        return summary, detail

    raw = read_table_auto(path, preferred_sheets=[sheet_name] if sheet_name else None)
    miss = [column for column in required_cols if column not in raw.columns]
    if miss:
        summary = pd.DataFrame([{"指标": f"缺少字段: {','.join(miss)}", "数值": np.nan}])
        detail = pd.DataFrame(columns=["渠道3", "再激活线索量", "再激活后到店率", "再激活后成交率"])
        return summary, detail

    raw = raw[[column for column in expected_columns if column in raw.columns]].copy()
    for column in [column for column in time_date_columns if column in raw.columns]:
        raw[column] = parse_excel_mixed_datetime(raw[column])

    raw["_创建DT"] = raw["创建时间"].combine_first(raw["创建日期"])
    raw["_到店DT"] = raw["到店时间"].combine_first(raw["到店日期"])
    raw["_成交DT"] = raw["成交时间"].combine_first(raw["成交日期"])
    raw["_战败DT"] = raw["战败时间"].combine_first(raw["战败日期"])

    raw["手机号_norm"] = raw["手机号"].fillna("").astype(str).str.replace(r"\D+", "", regex=True).str.strip()
    raw["线索ID_norm"] = raw["线索ID"].fillna("").astype(str).str.strip()
    raw["_创建key"] = raw["_创建DT"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    raw["_dedup_key"] = raw["手机号_norm"] + "|" + raw["线索ID_norm"] + "|" + raw["_创建key"]
    raw = raw.sort_values(["手机号_norm", "_创建DT"], na_position="last").drop_duplicates("_dedup_key", keep="first")

    status_text = raw["线索状态"].fillna("").astype(str)
    raw["是否无效战败"] = (raw["_战败DT"].notna() | status_text.str.contains("战败|无效|失效", regex=True)).astype(int)
    raw["是否到店"] = raw["_到店DT"].notna().astype(int)
    raw["是否成交"] = raw["_成交DT"].notna().astype(int)
    raw["手机号有效"] = raw["手机号_norm"].ne("").astype(int)

    base = raw[raw["手机号有效"] == 1].copy()
    if base.empty:
        summary = pd.DataFrame([{"指标": "有效手机号样本量", "数值": 0}])
        detail = pd.DataFrame(columns=["渠道3", "再激活线索量", "再激活后到店率", "再激活后成交率"])
        return summary, detail

    base["历史无效战败累计"] = base.groupby("手机号_norm")["是否无效战败"].cumsum()
    base["历史无效战败累计_上条"] = base.groupby("手机号_norm")["历史无效战败累计"].shift(1).fillna(0)
    base["是否再激活"] = ((base["历史无效战败累计_上条"] > 0) & (base["是否无效战败"] == 0)).astype(int)

    invalid_rows = int(base["是否无效战败"].sum())
    react_rows = int(base["是否再激活"].sum())
    react_df = base[base["是否再激活"] == 1].copy()

    summary = pd.DataFrame(
        [
            {"指标": "有效手机号样本量", "数值": int(len(base))},
            {"指标": "无效/战败线索量", "数值": invalid_rows},
            {"指标": "再激活线索量", "数值": react_rows},
            {"指标": "再激活率(再激活/无效战败)%", "数值": (react_rows / invalid_rows * 100) if invalid_rows > 0 else np.nan},
            {"指标": "再激活后到店率%", "数值": (react_df["是否到店"].mean() * 100) if len(react_df) > 0 else np.nan},
            {"指标": "再激活后成交率%", "数值": (react_df["是否成交"].mean() * 100) if len(react_df) > 0 else np.nan},
        ]
    ).round(2)

    if len(react_df) == 0:
        detail = pd.DataFrame(columns=["渠道3", "再激活线索量", "再激活后到店率", "再激活后成交率"])
    else:
        detail = (
            ensure_str(react_df, ["渠道3"])
            .groupby("渠道3", dropna=False)
            .agg(
                再激活线索量=("线索ID", "size"),
                再激活后到店率=("是否到店", lambda x: x.mean() * 100),
                再激活后成交率=("是否成交", lambda x: x.mean() * 100),
            )
            .reset_index()
            .sort_values("再激活线索量", ascending=False)
            .round(2)
        )
    return summary, detail
