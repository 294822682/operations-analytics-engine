"""Raw-evidence model-path analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_model_path_tables(base_df: pd.DataFrame):
    mdf = base_df.copy()
    mdf["首次意向车型_标准"] = mdf["首次意向车型"].fillna("").astype(str).str.strip().replace("", "未知意向")
    mdf["试驾车型_标准"] = np.where(
        mdf["是否试驾"] == 1,
        mdf["试驾车型"].fillna("").astype(str).str.strip().replace("", "未知试驾车型"),
        "无试驾",
    )
    mdf["成交车型_标准"] = np.where(
        mdf["是否成交"] == 1,
        mdf["成交车型"].fillna("").astype(str).str.strip().replace("", "未知成交车型"),
        "无成交",
    )

    mdf["意向试驾可比"] = (
        (mdf["是否试驾"] == 1)
        & (~mdf["首次意向车型_标准"].isin(["未知意向"]))
        & (~mdf["试驾车型_标准"].isin(["未知试驾车型", "无试驾"]))
    ).astype(int)
    mdf["试驾成交可比"] = (
        (mdf["是否成交"] == 1)
        & (~mdf["试驾车型_标准"].isin(["未知试驾车型", "无试驾"]))
        & (~mdf["成交车型_标准"].isin(["未知成交车型", "无成交"]))
    ).astype(int)
    mdf["意向成交可比"] = (
        (mdf["是否成交"] == 1)
        & (~mdf["首次意向车型_标准"].isin(["未知意向"]))
        & (~mdf["成交车型_标准"].isin(["未知成交车型", "无成交"]))
    ).astype(int)
    mdf["全链路可比"] = ((mdf["意向试驾可比"] == 1) & (mdf["试驾成交可比"] == 1) & (mdf["意向成交可比"] == 1)).astype(int)
    mdf["意向到试驾跳转"] = ((mdf["意向试驾可比"] == 1) & (mdf["首次意向车型_标准"] != mdf["试驾车型_标准"])).astype(int)
    mdf["试驾到成交跳转"] = ((mdf["试驾成交可比"] == 1) & (mdf["试驾车型_标准"] != mdf["成交车型_标准"])).astype(int)
    mdf["意向到成交稳定"] = ((mdf["意向成交可比"] == 1) & (mdf["首次意向车型_标准"] == mdf["成交车型_标准"])).astype(int)
    mdf["全链路稳定"] = (
        (mdf["全链路可比"] == 1)
        & (mdf["首次意向车型_标准"] == mdf["试驾车型_标准"])
        & (mdf["试驾车型_标准"] == mdf["成交车型_标准"])
    ).astype(int)

    summary = (
        mdf.groupby("首次意向车型_标准", dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店量=("是否到店", "sum"),
            试驾量=("是否试驾", "sum"),
            成交量=("是否成交", "sum"),
            意向成交可比样本N=("意向成交可比", "sum"),
            意向到成交稳定量=("意向到成交稳定", "sum"),
            意向试驾可比样本N=("意向试驾可比", "sum"),
            意向到试驾跳转量=("意向到试驾跳转", "sum"),
        )
        .reset_index()
    )
    summary["到店率(创建->到店)%"] = np.where(summary["线索量"] > 0, summary["到店量"] / summary["线索量"] * 100, np.nan)
    summary["试驾率(创建->试驾)%"] = np.where(summary["线索量"] > 0, summary["试驾量"] / summary["线索量"] * 100, np.nan)
    summary["成交率(创建->成交)%"] = np.where(summary["线索量"] > 0, summary["成交量"] / summary["线索量"] * 100, np.nan)
    summary["意向到成交稳定率(%)"] = np.where(summary["意向成交可比样本N"] > 0, summary["意向到成交稳定量"] / summary["意向成交可比样本N"] * 100, np.nan)
    summary["意向到试驾跳转率(%)"] = np.where(summary["意向试驾可比样本N"] > 0, summary["意向到试驾跳转量"] / summary["意向试驾可比样本N"] * 100, np.nan)
    summary["无成交占比(%)"] = np.where(summary["线索量"] > 0, (summary["线索量"] - summary["成交量"]) / summary["线索量"] * 100, np.nan)
    summary = summary.sort_values(["线索量", "成交量"], ascending=[False, False]).round(2)

    mdf["车型路径"] = mdf["首次意向车型_标准"] + " -> " + mdf["试驾车型_标准"] + " -> " + mdf["成交车型_标准"]
    detail = (
        mdf.groupby(["首次意向车型_标准", "车型路径"], dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店量=("是否到店", "sum"),
            试驾量=("是否试驾", "sum"),
            成交量=("是否成交", "sum"),
            全链路可比样本N=("全链路可比", "sum"),
            全链路稳定量=("全链路稳定", "sum"),
        )
        .reset_index()
    )
    detail["路径成交率(创建->成交)%"] = np.where(detail["线索量"] > 0, detail["成交量"] / detail["线索量"] * 100, np.nan)
    detail["全链路稳定率(%)"] = np.where(detail["全链路可比样本N"] > 0, detail["全链路稳定量"] / detail["全链路可比样本N"] * 100, np.nan)
    detail = detail.sort_values(["线索量", "成交量"], ascending=[False, False]).round(2)

    total_n = len(mdf)
    arrive_n = int(mdf["是否到店"].sum())
    test_n = int(mdf["是否试驾"].sum())
    deal_n = int(mdf["是否成交"].sum())
    comparable_intent_test_n = int(mdf["意向试驾可比"].sum())
    comparable_test_deal_n = int(mdf["试驾成交可比"].sum())
    comparable_intent_deal_n = int(mdf["意向成交可比"].sum())
    comparable_full_n = int(mdf["全链路可比"].sum())

    overall_metrics = pd.DataFrame(
        [
            {"指标": "意向->到店流失率%", "分子": total_n - arrive_n, "分母": total_n, "数值": (1 - arrive_n / total_n) * 100 if total_n > 0 else np.nan},
            {"指标": "到店->试驾流失率%", "分子": arrive_n - test_n, "分母": arrive_n, "数值": (1 - test_n / arrive_n) * 100 if arrive_n > 0 else np.nan},
            {"指标": "试驾->成交流失率%", "分子": test_n - deal_n, "分母": test_n, "数值": (1 - deal_n / test_n) * 100 if test_n > 0 else np.nan},
            {"指标": "意向->试驾跳转率%(可比样本)", "分子": int(mdf["意向到试驾跳转"].sum()), "分母": comparable_intent_test_n, "数值": (mdf["意向到试驾跳转"].sum() / comparable_intent_test_n * 100) if comparable_intent_test_n > 0 else np.nan},
            {"指标": "试驾->成交跳转率%(可比样本)", "分子": int(mdf["试驾到成交跳转"].sum()), "分母": comparable_test_deal_n, "数值": (mdf["试驾到成交跳转"].sum() / comparable_test_deal_n * 100) if comparable_test_deal_n > 0 else np.nan},
            {"指标": "意向->成交稳定率%(可比样本)", "分子": int(mdf["意向到成交稳定"].sum()), "分母": comparable_intent_deal_n, "数值": (mdf["意向到成交稳定"].sum() / comparable_intent_deal_n * 100) if comparable_intent_deal_n > 0 else np.nan},
            {"指标": "全链路稳定率(意向=试驾=成交)%", "分子": int(mdf["全链路稳定"].sum()), "分母": comparable_full_n, "数值": (mdf["全链路稳定"].sum() / comparable_full_n * 100) if comparable_full_n > 0 else np.nan},
        ]
    ).round(2)

    by_intent = (
        mdf.groupby("首次意向车型_标准", dropna=False)
        .agg(
            意向样本N=("线索ID", "size"),
            到店N=("是否到店", "sum"),
            试驾N=("是否试驾", "sum"),
            成交N=("是否成交", "sum"),
            意向试驾可比样本N=("意向试驾可比", "sum"),
            意向到试驾跳转N=("意向到试驾跳转", "sum"),
            试驾成交可比样本N=("试驾成交可比", "sum"),
            试驾到成交跳转N=("试驾到成交跳转", "sum"),
            意向成交可比样本N=("意向成交可比", "sum"),
            意向到成交稳定N=("意向到成交稳定", "sum"),
        )
        .reset_index()
    )
    by_intent["意向->到店流失率(%)"] = np.where(by_intent["意向样本N"] > 0, (1 - by_intent["到店N"] / by_intent["意向样本N"]) * 100, np.nan)
    by_intent["到店->试驾流失率(%)"] = np.where(by_intent["到店N"] > 0, (1 - by_intent["试驾N"] / by_intent["到店N"]) * 100, np.nan)
    by_intent["试驾->成交流失率(%)"] = np.where(by_intent["试驾N"] > 0, (1 - by_intent["成交N"] / by_intent["试驾N"]) * 100, np.nan)
    by_intent["意向->试驾跳转率(%)"] = np.where(by_intent["意向试驾可比样本N"] > 0, by_intent["意向到试驾跳转N"] / by_intent["意向试驾可比样本N"] * 100, np.nan)
    by_intent["试驾->成交跳转率(%)"] = np.where(by_intent["试驾成交可比样本N"] > 0, by_intent["试驾到成交跳转N"] / by_intent["试驾成交可比样本N"] * 100, np.nan)
    by_intent["意向->成交稳定率(%)"] = np.where(by_intent["意向成交可比样本N"] > 0, by_intent["意向到成交稳定N"] / by_intent["意向成交可比样本N"] * 100, np.nan)
    by_intent = by_intent.sort_values(["意向样本N", "成交N"], ascending=[False, False]).round(2)
    return summary, detail, overall_metrics, by_intent
