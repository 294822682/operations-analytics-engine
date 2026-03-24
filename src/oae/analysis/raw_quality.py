"""Raw-evidence quality, freshness, and health analysis helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd

from oae.analysis.raw_process_sla import enrich_process_sla_columns, rate_within
from transform.lead_transform import ensure_str


def build_channel_quality_table(base_df: pd.DataFrame) -> pd.DataFrame:
    qdf = ensure_str(enrich_process_sla_columns(base_df), ["渠道2", "渠道3", "线索状态分组"])
    out = (
        qdf.groupby(["渠道2", "渠道3"], dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            无效战败率=("线索状态分组", lambda x: (x == "无效战败").mean() * 100),
            创建到下发_15分钟达标率=("创建到下发_分钟", lambda x: rate_within(x, 15)),
            下发到首次跟进_60分钟达标率=("下发到首次跟进_分钟", lambda x: rate_within(x, 60)),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
            平均响应延迟_分钟=("响应延迟_分钟", "mean"),
        )
        .reset_index()
    )
    out["有效率"] = 100 - out["无效战败率"].fillna(100)
    out["质量评分"] = (
        out["有效率"] * 0.30
        + out["创建到下发_15分钟达标率"].fillna(0) * 0.20
        + out["下发到首次跟进_60分钟达标率"].fillna(0) * 0.20
        + out["到店率"].fillna(0) * 0.15
        + out["成交率"].fillna(0) * 0.15
    )
    out["质量评分排名"] = out["质量评分"].rank(method="dense", ascending=False).astype(int)
    out = out.sort_values(["质量评分排名", "线索量"], ascending=[True, False]).round(2)
    return out[
        [
            "质量评分排名",
            "渠道2",
            "渠道3",
            "线索量",
            "有效率",
            "无效战败率",
            "创建到下发_15分钟达标率",
            "下发到首次跟进_60分钟达标率",
            "到店率",
            "试驾率",
            "成交率",
            "平均响应延迟_分钟",
            "质量评分",
        ]
    ].copy()


def build_lead_freshness_table(base_df: pd.DataFrame) -> pd.DataFrame:
    fdf = base_df.copy()
    delay_hrs = pd.to_numeric(fdf["响应延迟_小时"], errors="coerce")
    fdf["线索新鲜度分层"] = np.select(
        [
            delay_hrs.between(0, 1, inclusive="both"),
            delay_hrs.gt(1) & delay_hrs.le(6),
            delay_hrs.gt(6) & delay_hrs.le(24),
            delay_hrs.gt(24),
        ],
        ["0-1h", "1-6h", "6-24h", "24h+"],
        default="未跟进/未知",
    )
    order = ["0-1h", "1-6h", "6-24h", "24h+", "未跟进/未知"]
    fdf["线索新鲜度分层"] = pd.Categorical(fdf["线索新鲜度分层"], categories=order, ordered=True)
    out = (
        fdf.groupby("线索新鲜度分层", dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            平均响应延迟_小时=("响应延迟_小时", "mean"),
            P50响应延迟_小时=("响应延迟_小时", "median"),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
        )
        .reset_index()
        .sort_values("线索新鲜度分层")
    )
    out["线索占比(%)"] = np.where(len(fdf) > 0, out["线索量"] / len(fdf) * 100, np.nan)
    return out[
        ["线索新鲜度分层", "线索量", "线索占比(%)", "平均响应延迟_小时", "P50响应延迟_小时", "到店率", "试驾率", "成交率"]
    ].round(2)


def build_region_potential_tables(base_df: pd.DataFrame):
    rdf = ensure_str(base_df.copy(), ["大区", "省份", "城市"])
    total_leads = len(rdf)
    province = (
        rdf.groupby(["大区", "省份"], dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
            平均响应延迟_分钟=("响应延迟_分钟", "mean"),
        )
        .reset_index()
    )
    province["线索占比(%)"] = np.where(total_leads > 0, province["线索量"] / total_leads * 100, np.nan)
    province["区域潜力指数"] = (
        province["线索占比(%)"].fillna(0) * 0.35
        + province["到店率"].fillna(0) * 0.25
        + province["试驾率"].fillna(0) * 0.20
        + province["成交率"].fillna(0) * 0.20
    )
    province = province.sort_values(["区域潜力指数", "线索量"], ascending=[False, False]).round(2)
    city = (
        rdf.groupby(["省份", "城市"], dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
        )
        .reset_index()
    )
    city["线索占比(%)"] = np.where(total_leads > 0, city["线索量"] / total_leads * 100, np.nan)
    city = city.sort_values(["线索量", "成交率"], ascending=[False, False]).round(2)
    return province, city


def build_data_quality_tables(base_df: pd.DataFrame, clean_stats: dict):
    qdf = base_df.copy()
    phone_norm = qdf["手机号"].fillna("").astype(str).str.replace(r"\D+", "", regex=True).str.strip()
    phone_missing_rate = (phone_norm == "").mean() * 100 if len(qdf) > 0 else np.nan
    chain_abnormal = (
        ((qdf["_下发DT"] < qdf["_创建DT"]) & qdf["_下发DT"].notna() & qdf["_创建DT"].notna())
        | ((qdf["_首次跟进DT"] < qdf["_下发DT"]) & qdf["_首次跟进DT"].notna() & qdf["_下发DT"].notna())
        | ((qdf["_到店DT"] < qdf["_首次跟进DT"]) & qdf["_到店DT"].notna() & qdf["_首次跟进DT"].notna())
        | ((qdf["_试驾DT"] < qdf["_到店DT"]) & qdf["_试驾DT"].notna() & qdf["_到店DT"].notna())
        | ((qdf["_下订DT"] < qdf["_试驾DT"]) & qdf["_下订DT"].notna() & qdf["_试驾DT"].notna())
        | ((qdf["_成交DT"] < qdf["_下订DT"]) & qdf["_成交DT"].notna() & qdf["_下订DT"].notna())
    )
    time_abnormal_rate = chain_abnormal.mean() * 100 if len(qdf) > 0 else np.nan
    non_empty_rows = clean_stats.get("non_empty_rows", np.nan)
    dedup_rows = clean_stats.get("dedup_rows", np.nan)
    duplicate_rate = ((non_empty_rows - dedup_rows) / non_empty_rows * 100) if (isinstance(non_empty_rows, (int, float)) and non_empty_rows > 0) else np.nan
    summary = pd.DataFrame(
        [
            {"指标": "清洗后线索量", "数值": dedup_rows},
            {"指标": "原始非空行数", "数值": non_empty_rows},
            {"指标": "重复线索率(去重前后)%", "数值": duplicate_rate},
            {"指标": "手机号缺失率%", "数值": phone_missing_rate},
            {"指标": "时间链路异常率%", "数值": time_abnormal_rate},
        ]
    ).round(2)
    key_cols = ["渠道1", "渠道2", "渠道3", "线索状态", "线索等级", "创建时间", "首次意向车型", "意向车型", "车系", "大区", "省份", "城市"]
    null_rows = []
    for column in key_cols:
        if column not in qdf.columns:
            continue
        series = qdf[column]
        blank = series.isna() | series.astype(str).str.strip().isin(["", "nan", "None", "NaT"])
        null_rows.append({"字段": column, "空值率(%)": blank.mean() * 100, "非空率(%)": 100 - blank.mean() * 100})
    key_null = pd.DataFrame(null_rows).sort_values("空值率(%)", ascending=False).round(2)
    return summary, key_null


def build_business_health_tables(base_df: pd.DataFrame):
    hdf = base_df.copy()
    hdf["创建日"] = pd.to_datetime(hdf["_创建DT"], errors="coerce").dt.floor("D")
    hdf = hdf[hdf["创建日"].notna()].copy()
    if not hdf.empty:
        latest_day = hdf["创建日"].max()
        hdf = hdf[hdf["创建日"] >= (latest_day - pd.Timedelta(days=120))].copy()
    if hdf.empty:
        empty_daily = pd.DataFrame(columns=["创建日", "线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(创建->成交)%", "异常告警"])
        empty_monthly = pd.DataFrame(columns=["月份", "线索量", "成交率(创建->成交)%", "线索量月环比(%)", "成交率月环比(百分点)"])
        return empty_daily, empty_monthly
    daily = (
        hdf.groupby("创建日", dropna=False)
        .agg(线索量=("线索ID", "size"), 到店量=("是否到店", "sum"), 试驾量=("是否试驾", "sum"), 成交量=("是否成交", "sum"))
        .reset_index()
        .sort_values("创建日")
    )
    full_days = pd.date_range(daily["创建日"].min(), daily["创建日"].max(), freq="D")
    daily = daily.set_index("创建日").reindex(full_days).rename_axis("创建日").reset_index()
    for column in ["线索量", "到店量", "试驾量", "成交量"]:
        daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0.0)
    daily["到店率(创建->到店)%"] = np.where(daily["线索量"] > 0, daily["到店量"] / daily["线索量"] * 100, np.nan)
    daily["试驾率(创建->试驾)%"] = np.where(daily["线索量"] > 0, daily["试驾量"] / daily["线索量"] * 100, np.nan)
    daily["成交率(创建->成交)%"] = np.where(daily["线索量"] > 0, daily["成交量"] / daily["线索量"] * 100, np.nan)
    daily["线索量日环比(%)"] = daily["线索量"].pct_change() * 100
    daily["成交率日环比(百分点)"] = daily["成交率(创建->成交)%"].diff()
    daily["线索量_7日均值"] = daily["线索量"].rolling(7, min_periods=1).mean().shift(1)
    daily["成交率_7日均值"] = daily["成交率(创建->成交)%"].rolling(7, min_periods=1).mean().shift(1)
    daily["线索量较7日均值(%)"] = np.where(daily["线索量_7日均值"] > 0, (daily["线索量"] - daily["线索量_7日均值"]) / daily["线索量_7日均值"] * 100, np.nan)
    daily["成交率较7日均值(百分点)"] = daily["成交率(创建->成交)%"] - daily["成交率_7日均值"]

    def make_alert(row):
        if row["线索量"] <= 0:
            return "无投放/无线索"
        alerts = []
        if pd.notna(row["线索量较7日均值(%)"]) and row["线索量较7日均值(%)"] <= -30:
            alerts.append("线索量较7日均值下降>=30%")
        if pd.notna(row["成交率较7日均值(百分点)"]) and row["成交率较7日均值(百分点)"] <= -2:
            alerts.append("成交率较7日均值下降>=2pct")
        if row["线索量"] >= 20 and row["成交量"] == 0:
            alerts.append("高线索日无成交")
        return "；".join(alerts) if alerts else "正常"

    daily["异常告警"] = daily.apply(make_alert, axis=1)
    daily_out = daily[
        ["创建日", "线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(创建->成交)%", "线索量日环比(%)", "成交率日环比(百分点)", "线索量较7日均值(%)", "成交率较7日均值(百分点)", "异常告警"]
    ].round(2)
    daily["月份"] = daily["创建日"].dt.to_period("M").astype(str)
    monthly = (
        daily.groupby("月份", dropna=False)
        .agg(线索量=("线索量", "sum"), 到店量=("到店量", "sum"), 试驾量=("试驾量", "sum"), 成交量=("成交量", "sum"))
        .reset_index()
        .sort_values("月份")
    )
    monthly["到店率(创建->到店)%"] = np.where(monthly["线索量"] > 0, monthly["到店量"] / monthly["线索量"] * 100, np.nan)
    monthly["试驾率(创建->试驾)%"] = np.where(monthly["线索量"] > 0, monthly["试驾量"] / monthly["线索量"] * 100, np.nan)
    monthly["成交率(创建->成交)%"] = np.where(monthly["线索量"] > 0, monthly["成交量"] / monthly["线索量"] * 100, np.nan)
    monthly["线索量月环比(%)"] = monthly["线索量"].pct_change() * 100
    monthly["成交率月环比(百分点)"] = monthly["成交率(创建->成交)%"].diff()
    monthly = monthly.replace([np.inf, -np.inf], np.nan)
    monthly_out = monthly[["月份", "线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(创建->成交)%", "线索量月环比(%)", "成交率月环比(百分点)"]].round(2)
    return daily_out, monthly_out
