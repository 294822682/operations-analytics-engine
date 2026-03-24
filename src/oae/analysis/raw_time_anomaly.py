"""Raw-evidence time-chain anomaly analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd

from transform.lead_transform import ensure_str


def build_time_chain_anomaly_tables(base_df: pd.DataFrame):
    adf = base_df.copy()
    checks = [
        ("下发早于创建", "_下发DT", "_创建DT"),
        ("首次跟进早于下发", "_首次跟进DT", "_下发DT"),
        ("到店早于首次跟进", "_到店DT", "_首次跟进DT"),
        ("试驾早于到店", "_试驾DT", "_到店DT"),
        ("下订早于试驾", "_下订DT", "_试驾DT"),
        ("成交早于下订", "_成交DT", "_下订DT"),
    ]

    details = []
    for anomaly_type, cur_col, pre_col in checks:
        mask = adf[cur_col].notna() & adf[pre_col].notna() & (adf[cur_col] < adf[pre_col])
        if not mask.any():
            continue
        sub = adf.loc[
            mask,
            [
                "线索ID", "手机号", "渠道2", "渠道3", "大区", "省份", "城市", "ERP", "店名", "销售姓名",
                "线索状态", "首次意向车型", "意向车型",
                "_创建DT", "_下发DT", "_首次跟进DT", "_到店DT", "_试驾DT", "_下订DT", "_成交DT",
            ],
        ].copy()
        sub["异常类型"] = anomaly_type
        sub["异常节点"] = cur_col + " < " + pre_col
        sub["时间差_分钟"] = (sub[cur_col] - sub[pre_col]).dt.total_seconds() / 60
        sub["提前分钟(绝对值)"] = sub["时间差_分钟"].abs()
        details.append(sub)

    if len(details) == 0:
        summary = pd.DataFrame(columns=["异常类型", "异常条数", "异常占比(%)", "平均提前分钟", "P50提前分钟", "最大提前天数"])
        detail = pd.DataFrame(columns=[
            "异常类型", "线索ID", "手机号", "渠道2", "渠道3", "大区", "省份", "城市", "ERP", "店名", "销售姓名",
            "线索状态", "首次意向车型", "意向车型",
            "时间差_分钟", "提前分钟(绝对值)", "风险等级", "创建时间", "下发时间", "首次跟进时间", "到店时间", "试驾时间", "下订时间", "成交时间",
        ])
        return summary, detail

    detail = pd.concat(details, ignore_index=True)
    total_n = len(adf)
    summary = (
        detail.groupby("异常类型", dropna=False)
        .agg(
            异常条数=("线索ID", "size"),
            平均提前分钟=("提前分钟(绝对值)", "mean"),
            P50提前分钟=("提前分钟(绝对值)", "median"),
            最大提前分钟=("提前分钟(绝对值)", "max"),
        )
        .reset_index()
    )
    summary["异常占比(%)"] = np.where(total_n > 0, summary["异常条数"] / total_n * 100, np.nan)
    summary["最大提前天数"] = summary["最大提前分钟"] / (60 * 24)
    summary = summary[["异常类型", "异常条数", "异常占比(%)", "平均提前分钟", "P50提前分钟", "最大提前天数"]]
    summary = summary.sort_values(["异常条数", "最大提前天数"], ascending=[False, False]).round(2)

    detail["风险等级"] = np.select(
        [
            detail["提前分钟(绝对值)"] >= 7 * 24 * 60,
            detail["提前分钟(绝对值)"] >= 24 * 60,
            detail["提前分钟(绝对值)"] >= 60,
        ],
        ["高风险(>=7天)", "中高风险(>=1天)", "中风险(>=1小时)"],
        default="低风险(<1小时)",
    )
    detail = detail.rename(
        columns={
            "_创建DT": "创建时间",
            "_下发DT": "下发时间",
            "_首次跟进DT": "首次跟进时间",
            "_到店DT": "到店时间",
            "_试驾DT": "试驾时间",
            "_下订DT": "下订时间",
            "_成交DT": "成交时间",
        }
    )
    for column in ["创建时间", "下发时间", "首次跟进时间", "到店时间", "试驾时间", "下订时间", "成交时间"]:
        detail[column] = pd.to_datetime(detail[column], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    detail = detail[
        [
            "异常类型", "线索ID", "手机号", "渠道2", "渠道3", "大区", "省份", "城市", "ERP", "店名", "销售姓名",
            "线索状态", "首次意向车型", "意向车型",
            "时间差_分钟", "提前分钟(绝对值)", "风险等级", "创建时间", "下发时间", "首次跟进时间", "到店时间", "试驾时间", "下订时间", "成交时间",
        ]
    ].sort_values(["提前分钟(绝对值)", "异常类型"], ascending=[False, True]).round(2)
    return summary, detail


def build_anomaly_responsibility_tables(base_df: pd.DataFrame, time_anomaly_detail: pd.DataFrame):
    cols_shop = ["渠道2", "省份", "城市", "ERP", "店名"]
    cols_channel = ["渠道2", "渠道3", "大区", "省份"]

    base = ensure_str(base_df.copy(), cols_shop + cols_channel)
    base_shop = base.groupby(cols_shop, dropna=False).agg(基础线索量=("线索ID", "size")).reset_index()
    base_channel = base.groupby(cols_channel, dropna=False).agg(基础线索量=("线索ID", "size")).reset_index()

    if time_anomaly_detail is None or time_anomaly_detail.empty:
        shop_empty = pd.DataFrame(columns=cols_shop + ["基础线索量", "异常条数", "高风险条数", "异常率(%)", "平均提前分钟", "最大提前分钟"])
        channel_empty = pd.DataFrame(columns=cols_channel + ["基础线索量", "异常条数", "高风险条数", "异常率(%)", "平均提前分钟", "最大提前分钟"])
        return shop_empty, channel_empty

    an = ensure_str(time_anomaly_detail.copy(), cols_shop + cols_channel + ["风险等级", "异常类型"])
    an["提前分钟(绝对值)"] = pd.to_numeric(an["提前分钟(绝对值)"], errors="coerce")
    an["高风险标记"] = (an["风险等级"] == "高风险(>=7天)").astype(int)

    shop_an = (
        an.groupby(cols_shop, dropna=False)
        .agg(
            异常条数=("线索ID", "size"),
            高风险条数=("高风险标记", "sum"),
            平均提前分钟=("提前分钟(绝对值)", "mean"),
            最大提前分钟=("提前分钟(绝对值)", "max"),
        )
        .reset_index()
    )
    shop_out = base_shop.merge(shop_an, on=cols_shop, how="left")
    for column in ["异常条数", "高风险条数", "平均提前分钟", "最大提前分钟"]:
        shop_out[column] = pd.to_numeric(shop_out[column], errors="coerce").fillna(0)
    shop_out["异常率(%)"] = np.where(shop_out["基础线索量"] > 0, shop_out["异常条数"] / shop_out["基础线索量"] * 100, np.nan)
    shop_out = shop_out.sort_values(["高风险条数", "异常条数", "异常率(%)"], ascending=[False, False, False]).round(2)

    channel_an = (
        an.groupby(cols_channel, dropna=False)
        .agg(
            异常条数=("线索ID", "size"),
            高风险条数=("高风险标记", "sum"),
            平均提前分钟=("提前分钟(绝对值)", "mean"),
            最大提前分钟=("提前分钟(绝对值)", "max"),
        )
        .reset_index()
    )
    channel_out = base_channel.merge(channel_an, on=cols_channel, how="left")
    for column in ["异常条数", "高风险条数", "平均提前分钟", "最大提前分钟"]:
        channel_out[column] = pd.to_numeric(channel_out[column], errors="coerce").fillna(0)
    channel_out["异常率(%)"] = np.where(channel_out["基础线索量"] > 0, channel_out["异常条数"] / channel_out["基础线索量"] * 100, np.nan)
    channel_out = channel_out.sort_values(["高风险条数", "异常条数", "异常率(%)"], ascending=[False, False, False]).round(2)
    return shop_out, channel_out
