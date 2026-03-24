"""Raw-evidence SLA builders extracted from the legacy monolith."""

from __future__ import annotations

import numpy as np
import pandas as pd

from transform.lead_transform import ensure_str


def rate_within(series: pd.Series, threshold: float) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) == 0:
        return np.nan
    return float((values <= threshold).mean() * 100)


def enrich_process_sla_columns(base_df: pd.DataFrame) -> pd.DataFrame:
    df = base_df.copy()
    df["创建到下发_分钟"] = (df["_下发DT"] - df["_创建DT"]).dt.total_seconds() / 60
    df["下发到首次跟进_分钟"] = (df["_首次跟进DT"] - df["_下发DT"]).dt.total_seconds() / 60
    df["首次跟进到到店_小时"] = (df["_到店DT"] - df["_首次跟进DT"]).dt.total_seconds() / 3600
    for column in ["创建到下发_分钟", "下发到首次跟进_分钟", "首次跟进到到店_小时"]:
        df.loc[df[column] < 0, column] = np.nan
    return df


def build_process_sla_tables(base_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = enrich_process_sla_columns(base_df)

    sla_overall = pd.DataFrame(
        [
            {
                "环节": "创建->下发",
                "样本N": int(df["创建到下发_分钟"].notna().sum()),
                "平均时长": df["创建到下发_分钟"].mean(),
                "P50时长": df["创建到下发_分钟"].median(),
                "P90时长": df["创建到下发_分钟"].quantile(0.90),
                "单位": "分钟",
                "<=5分钟达标率(%)": rate_within(df["创建到下发_分钟"], 5),
                "<=15分钟达标率(%)": rate_within(df["创建到下发_分钟"], 15),
                "<=60分钟达标率(%)": rate_within(df["创建到下发_分钟"], 60),
                "<=24小时达标率(%)": rate_within(df["创建到下发_分钟"], 1440),
            },
            {
                "环节": "下发->首次跟进",
                "样本N": int(df["下发到首次跟进_分钟"].notna().sum()),
                "平均时长": df["下发到首次跟进_分钟"].mean(),
                "P50时长": df["下发到首次跟进_分钟"].median(),
                "P90时长": df["下发到首次跟进_分钟"].quantile(0.90),
                "单位": "分钟",
                "<=5分钟达标率(%)": rate_within(df["下发到首次跟进_分钟"], 5),
                "<=15分钟达标率(%)": rate_within(df["下发到首次跟进_分钟"], 15),
                "<=60分钟达标率(%)": rate_within(df["下发到首次跟进_分钟"], 60),
                "<=24小时达标率(%)": rate_within(df["下发到首次跟进_分钟"], 1440),
            },
            {
                "环节": "首次跟进->到店",
                "样本N": int(df["首次跟进到到店_小时"].notna().sum()),
                "平均时长": df["首次跟进到到店_小时"].mean(),
                "P50时长": df["首次跟进到到店_小时"].median(),
                "P90时长": df["首次跟进到到店_小时"].quantile(0.90),
                "单位": "小时",
                "<=1小时达标率(%)": rate_within(df["首次跟进到到店_小时"], 1),
                "<=6小时达标率(%)": rate_within(df["首次跟进到到店_小时"], 6),
                "<=24小时达标率(%)": rate_within(df["首次跟进到到店_小时"], 24),
                "<=72小时达标率(%)": rate_within(df["首次跟进到到店_小时"], 72),
            },
        ]
    ).round(2)

    sla_channel3 = (
        ensure_str(df.copy(), ["渠道3"])
        .groupby("渠道3", dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            平均创建到下发_分钟=("创建到下发_分钟", "mean"),
            创建到下发_15分钟达标率=("创建到下发_分钟", lambda x: rate_within(x, 15)),
            平均下发到首次跟进_分钟=("下发到首次跟进_分钟", "mean"),
            下发到首次跟进_15分钟达标率=("下发到首次跟进_分钟", lambda x: rate_within(x, 15)),
            下发到首次跟进_60分钟达标率=("下发到首次跟进_分钟", lambda x: rate_within(x, 60)),
            平均首次跟进到到店_小时=("首次跟进到到店_小时", "mean"),
            首次跟进到到店_24小时达标率=("首次跟进到到店_小时", lambda x: rate_within(x, 24)),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
        )
        .reset_index()
        .sort_values("线索量", ascending=False)
        .round(2)
    )
    return sla_overall, sla_channel3
