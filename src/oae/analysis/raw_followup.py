"""Raw-evidence follow-up intensity analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_followup_intensity_table(base_df: pd.DataFrame) -> pd.DataFrame:
    gdf = base_df.copy()
    follow_cnt = pd.to_numeric(gdf["跟进次数"], errors="coerce").fillna(0)
    gdf["跟进强度分桶"] = np.select(
        [
            follow_cnt <= 0,
            follow_cnt.between(1, 2, inclusive="both"),
            follow_cnt.between(3, 5, inclusive="both"),
            follow_cnt >= 6,
        ],
        ["0次", "1-2次", "3-5次", "6次+"],
        default="未知",
    )
    order = ["0次", "1-2次", "3-5次", "6次+", "未知"]
    gdf["跟进强度分桶"] = pd.Categorical(gdf["跟进强度分桶"], categories=order, ordered=True)

    out = (
        gdf.groupby("跟进强度分桶", dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            平均跟进次数=("跟进次数", "mean"),
            平均响应延迟_小时=("响应延迟_小时", "mean"),
            无效战败率=("线索状态分组", lambda x: (x == "无效战败").mean() * 100),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
        )
        .reset_index()
        .sort_values("跟进强度分桶")
    )
    out["线索占比(%)"] = np.where(len(gdf) > 0, out["线索量"] / len(gdf) * 100, np.nan)
    return out[
        ["跟进强度分桶", "线索量", "线索占比(%)", "平均跟进次数", "平均响应延迟_小时", "无效战败率", "到店率", "试驾率", "成交率"]
    ].round(2)
