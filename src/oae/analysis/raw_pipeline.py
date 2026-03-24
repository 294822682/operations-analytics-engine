"""Formal raw-evidence analysis pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


WORKSPACE_DIR = Path(__file__).resolve().parents[3]
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

from oae.analysis.raw_evidence import RAW_EVIDENCE_TOPICS, summarize_raw_evidence_topics, write_raw_evidence_manifest
from oae.analysis.raw_followup import build_followup_intensity_table
from oae.analysis.raw_live import build_host_trace_table_latest, build_live_operation_table
from oae.analysis.raw_model_path import build_model_path_tables
from oae.analysis.raw_process_sla import build_process_sla_tables
from oae.analysis.raw_quality import (
    build_business_health_tables,
    build_channel_quality_table,
    build_data_quality_tables,
    build_lead_freshness_table,
    build_region_potential_tables,
)
from oae.analysis.raw_reactivation import build_reactivation_tables
from oae.analysis.raw_snapshot import build_raw_analysis_snapshot
from oae.analysis.raw_time_anomaly import build_anomaly_responsibility_tables, build_time_chain_anomaly_tables
from oae.contracts.models import RunMetadata
from oae.exports.raw_analysis import write_raw_analysis_outputs
from oae.version import METRIC_VERSION, SCHEMA_VERSION, build_run_id
from transform.lead_transform import ensure_str, prepare_lead_dataframe


DEFAULT_INPUT_FILE = r"/Users/ahs/Downloads/总部新媒体线索2026-03-11.xlsx"
DEFAULT_SHEET_NAME = "总部新媒体线索2026-03-11"
DEFAULT_OUTPUT_DIR = "./全量分析"

EXPECTED_COLUMNS = [
    "序列", "线索创建人", "线索ID", "客户姓名", "手机号", "责任部门", "渠道1", "渠道2", "渠道3", "活动",
    "创建时间", "下发时间", "到店时间", "试驾时间", "下订时间", "成交时间", "首次跟进时间", "战败时间",
    "创建日期", "下发日期", "到店日期", "试驾日期", "下订日期", "成交日期", "首次跟进日期", "战败日期",
    "首次意向车型", "意向车型", "试驾车型", "下订车型", "成交车型", "车系", "大区", "省份", "城市",
    "ERP", "店名", "线索状态", "线索等级", "销售姓名", "销售手机号", "跟进次数", "直播间ID", "源ERP",
    "源线索ID", "源订单编号", "组合"
]
TIME_DATE_COLUMNS = [
    "创建时间", "下发时间", "到店时间", "试驾时间", "下订时间", "成交时间", "首次跟进时间", "战败时间",
    "创建日期", "下发日期", "到店日期", "试驾日期", "下订日期", "成交日期", "首次跟进日期", "战败日期"
]
LEVEL_SCORE_MAP = {"O": 8, "H": 7, "A": 6, "B": 5, "C": 4, "D": 3, "F": 2, "NA": 1}
LEVEL_DESC_MAP = {
    "O": "最高等级（已交车/成交订单）",
    "H": "第二高（高意向）",
    "A": "第三（中高意向）",
    "B": "中意向",
    "C": "中低意向",
    "D": "低意向",
    "F": "战败（明确失败）",
    "NA": "未分级/无效/已分配未跟进",
}
ACCOUNT_MAP = {
    "抖音-星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "抖音-星途星纪元": "抖音-星途汽车官方直播间",
    "抖音来客直播": "抖音-星途汽车官方直播间",
}
RAW_TOPIC_ORDER = ["process_sla", "model_path", "followup_intensity", "reactivation", "time_anomaly"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw-evidence analysis pipeline")
    parser.add_argument("--analysis-mode", default="raw-evidence", help="兼容参数，raw pipeline 固定使用 raw-evidence")
    parser.add_argument("--fact-csv", default="", help="兼容参数，raw pipeline 不消费统一事实层")
    parser.add_argument("--input-file", default="", help="原始线索文件路径")
    parser.add_argument("--sheet-name", default="", help="原始线索 Excel sheet")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="分析输出目录")
    parser.add_argument("--snapshot-dir", default="artifacts/snapshots", help="分析快照目录")
    parser.add_argument("--manifest-dir", default="artifacts/exports/analysis", help="analysis manifest 目录")
    parser.add_argument("--run-id", default="", help="运行编号")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION, help="analysis schema 版本")
    parser.add_argument("--metric-version", default=METRIC_VERSION, help="经营口径版本")
    return parser.parse_args()


def resolve_input_source(workspace_dir: Path, default_input_file: str, default_sheet_name: str):
    candidates = []
    p_default = Path(default_input_file)
    if p_default.exists():
        candidates.append(p_default)
    search_dirs = [Path("/Users/ahs/Downloads"), Path("/Users/ahs/Downloads/try"), workspace_dir]
    patterns = ["总部新媒体线索*.xlsx", "总部新媒体线索*.xls", "总部新媒体线索*.csv"]
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for pattern in patterns:
            candidates.extend(search_dir.glob(pattern))
    if not candidates:
        raise FileNotFoundError("未找到线索文件（xlsx/csv）。请将文件放到 Downloads 或工程目录。")
    chosen = sorted(set(candidates), key=lambda path: path.stat().st_mtime, reverse=True)[0]
    sheet_name = default_sheet_name
    if chosen.suffix.lower() in {".xlsx", ".xls"}:
        try:
            xls = pd.ExcelFile(chosen)
            if default_sheet_name not in xls.sheet_names:
                sheet_name = xls.sheet_names[0]
        except Exception:
            sheet_name = default_sheet_name
    else:
        sheet_name = ""
    return str(chosen), sheet_name


def run_raw_evidence_analysis(
    *,
    workspace_dir: Path,
    input_file: str,
    sheet_name: str,
    output_dir: Path,
    snapshot_dir: Path,
    manifest_dir: Path,
    metadata: RunMetadata,
) -> dict[str, Path]:
    resolved_input_file, resolved_sheet_name = resolve_input_source(
        workspace_dir=workspace_dir,
        default_input_file=input_file or DEFAULT_INPUT_FILE,
        default_sheet_name=sheet_name or DEFAULT_SHEET_NAME,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    df, clean_stats = prepare_lead_dataframe(
        input_file=resolved_input_file,
        sheet_name=resolved_sheet_name,
        expected_columns=EXPECTED_COLUMNS,
        time_date_columns=TIME_DATE_COLUMNS,
        level_score_map=LEVEL_SCORE_MAP,
        level_desc_map=LEVEL_DESC_MAP,
    )

    stage_order = ["创建", "下发", "到店", "试驾", "下订", "成交"]
    stage_counts = {
        "创建": len(df),
        "下发": int(df["_下发DT"].notna().sum()),
        "到店": int(df["是否到店"].sum()),
        "试驾": int(df["是否试驾"].sum()),
        "下订": int(df["是否下订"].sum()),
        "成交": int(df["是否成交"].sum()),
    }
    funnel_rows = []
    base_count = stage_counts["创建"]
    prev_count = None
    for stage in stage_order:
        count = stage_counts[stage]
        step_rate = 100.0 if prev_count is None else ((count / prev_count * 100) if prev_count > 0 else np.nan)
        cum_rate = (count / base_count * 100) if base_count > 0 else np.nan
        funnel_rows.append([stage, count, step_rate, cum_rate])
        prev_count = count
    funnel_table = pd.DataFrame(funnel_rows, columns=["阶段", "人数", "阶段转化率(%)", "累计转化率(%)"]).round(2)

    channel_df = ensure_str(df.copy(), ["渠道2", "渠道3"])
    channel_perf = (
        channel_df.groupby(["渠道2", "渠道3"], dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
            平均成交周期=("成交周期_天", "mean"),
            平均跟进次数=("跟进次数", "mean"),
        )
        .reset_index()
        .sort_values(["成交率", "线索量"], ascending=[False, False])
        .round(2)
    )

    sla_overall, sla_channel3 = build_process_sla_tables(df)
    channel_quality = build_channel_quality_table(df)

    account_df = ensure_str(df.copy(), ["渠道2", "渠道3"])
    account_df["账号_原始"] = account_df["渠道2"]
    account_df["标准账号"] = account_df["账号_原始"].map(ACCOUNT_MAP).fillna(account_df["账号_原始"])
    account_funnel = (
        account_df.groupby("标准账号", dropna=False)
        .agg(
            创建量=("线索ID", "size"),
            下发量=("_下发DT", lambda x: x.notna().sum()),
            到店量=("是否到店", "sum"),
            试驾量=("是否试驾", "sum"),
            下订量=("是否下订", "sum"),
            成交量=("是否成交", "sum"),
            平均响应延迟_分钟=("响应延迟_分钟", "mean"),
            平均转化周期_天=("转化周期_天", "mean"),
        )
        .reset_index()
    )
    account_funnel["下发率(创建->下发)%"] = np.where(account_funnel["创建量"] > 0, account_funnel["下发量"] / account_funnel["创建量"] * 100, np.nan)
    account_funnel["到店率(下发->到店)%"] = np.where(account_funnel["下发量"] > 0, account_funnel["到店量"] / account_funnel["下发量"] * 100, np.nan)
    account_funnel["试驾率(到店->试驾)%"] = np.where(account_funnel["到店量"] > 0, account_funnel["试驾量"] / account_funnel["到店量"] * 100, np.nan)
    account_funnel["下订率(试驾->下订)%"] = np.where(account_funnel["试驾量"] > 0, account_funnel["下订量"] / account_funnel["试驾量"] * 100, np.nan)
    account_funnel["成交率(下订->成交)%"] = np.where(account_funnel["下订量"] > 0, account_funnel["成交量"] / account_funnel["下订量"] * 100, np.nan)
    account_funnel["总转化率(创建->成交)%"] = np.where(account_funnel["创建量"] > 0, account_funnel["成交量"] / account_funnel["创建量"] * 100, np.nan)
    account_funnel = account_funnel.sort_values(["创建量", "成交量"], ascending=[False, False]).round(2)

    host_cum_latest = build_host_trace_table_latest(df, ACCOUNT_MAP, workspace_dir)
    live_operation = build_live_operation_table(df, ACCOUNT_MAP, workspace_dir)
    model_path_summary, model_path_detail, model_path_metrics, model_path_by_intent = build_model_path_tables(df)
    lead_freshness = build_lead_freshness_table(df)
    followup_intensity = build_followup_intensity_table(df)
    region_province, region_city = build_region_potential_tables(df)
    reactivation_summary, reactivation_channel3 = build_reactivation_tables(
        input_file=resolved_input_file,
        sheet_name=resolved_sheet_name,
        expected_columns=EXPECTED_COLUMNS,
        time_date_columns=TIME_DATE_COLUMNS,
    )
    data_quality_summary, data_quality_keynull = build_data_quality_tables(df, clean_stats)
    time_anomaly_summary, time_anomaly_detail = build_time_chain_anomaly_tables(df)
    anomaly_shop_summary, anomaly_channel_summary = build_anomaly_responsibility_tables(df, time_anomaly_detail)
    biz_health_daily, biz_health_monthly = build_business_health_tables(df)
    level_perf = (
        df.groupby("线索等级标准", dropna=False)
        .agg(
            线索量=("线索ID", "size"),
            到店率=("是否到店", lambda x: x.mean() * 100),
            试驾率=("是否试驾", lambda x: x.mean() * 100),
            下订率=("是否下订", lambda x: x.mean() * 100),
            成交率=("是否成交", lambda x: x.mean() * 100),
            平均跟进次数=("跟进次数", "mean"),
        )
        .reset_index()
    )
    level_perf["线索等级得分"] = level_perf["线索等级标准"].map(LEVEL_SCORE_MAP).astype(int)
    level_perf["线索等级说明"] = level_perf["线索等级标准"].map(LEVEL_DESC_MAP)
    level_perf = level_perf.sort_values("线索等级得分", ascending=False).reset_index(drop=True).round(2)

    topic_tables = {
        "process_sla": {"sla_overall": sla_overall, "sla_channel3": sla_channel3},
        "model_path": {
            "model_path_summary": model_path_summary,
            "model_path_detail": model_path_detail,
            "model_path_metrics": model_path_metrics,
            "model_path_by_intent": model_path_by_intent,
        },
        "followup_intensity": {"followup_intensity": followup_intensity},
        "reactivation": {"reactivation_summary": reactivation_summary, "reactivation_channel3": reactivation_channel3},
        "time_anomaly": {
            "time_anomaly_summary": time_anomaly_summary,
            "time_anomaly_detail": time_anomaly_detail,
            "anomaly_shop_summary": anomaly_shop_summary,
            "anomaly_channel_summary": anomaly_channel_summary,
        },
    }

    latest_date = pd.to_datetime(df["_创建DT"], errors="coerce").dt.normalize().max()
    if pd.isna(latest_date):
        raise ValueError("原始线索中没有可用的创建时间")
    snapshot_date = latest_date.strftime("%Y-%m-%d")
    raw_snapshot = build_raw_analysis_snapshot(
        topic_tables=topic_tables,
        snapshot_date=snapshot_date,
        metadata=metadata,
        source_scope=f"raw-input:{Path(resolved_input_file).name}",
    )

    workbook_tables = [
        ("整体漏斗", funnel_table),
        ("渠道表现", channel_perf),
        ("SLA时效维度", sla_overall),
        ("SLA时效_渠道3", sla_channel3),
        ("渠道质量评分", channel_quality),
        ("账号层级漏斗", account_funnel),
        ("主播追踪_累计", host_cum_latest),
        ("直播运营_账号主播时段", live_operation),
        ("车型路径总览", model_path_summary),
        ("车型路径明细", model_path_detail),
        ("车型路径指标_总体", model_path_metrics),
        ("车型路径指标_分车型", model_path_by_intent),
        ("线索新鲜度", lead_freshness),
        ("跟进强度", followup_intensity),
        ("区域潜力_省份", region_province),
        ("区域潜力_城市", region_city),
        ("战败回收总览", reactivation_summary),
        ("战败回收_渠道3", reactivation_channel3),
        ("数据质量总览", data_quality_summary),
        ("数据质量_关键字段", data_quality_keynull),
        ("时间链路异常_总览", time_anomaly_summary),
        ("时间链路异常_明细", time_anomaly_detail),
        ("异常归属_店名汇总", anomaly_shop_summary),
        ("异常归属_渠道省份", anomaly_channel_summary),
        ("业务健康_日趋势", biz_health_daily),
        ("业务健康_月环比", biz_health_monthly),
        ("线索等级表现", level_perf),
    ]
    anomaly_tables = [
        ("异常总览", time_anomaly_summary),
        ("异常明细", time_anomaly_detail),
        ("归属_店名汇总", anomaly_shop_summary),
        ("归属_渠道省份", anomaly_channel_summary),
    ]
    raw_manifest_path = write_raw_evidence_manifest(output_dir)
    outputs = write_raw_analysis_outputs(
        output_dir=output_dir,
        snapshot_dir=snapshot_dir,
        manifest_dir=manifest_dir,
        snapshot_date=snapshot_date,
        metadata=metadata,
        workbook_tables=workbook_tables,
        anomaly_tables=anomaly_tables,
        raw_snapshot=raw_snapshot,
        topic_manifest={
            "analysis_mode": "raw-evidence",
            "raw_evidence_topics": RAW_EVIDENCE_TOPICS,
            "raw_evidence_groups": summarize_raw_evidence_topics(),
            "snapshot_date": snapshot_date,
            "source_scope": f"raw-input:{Path(resolved_input_file).name}",
            "topic_order": RAW_TOPIC_ORDER,
        },
    )
    outputs["raw_evidence_manifest"] = raw_manifest_path
    return outputs


def main() -> int:
    args = parse_args()
    metadata = RunMetadata(
        run_id=args.run_id or build_run_id(),
        schema_version=args.schema_version,
        metric_version=args.metric_version,
    )
    outputs = run_raw_evidence_analysis(
        workspace_dir=WORKSPACE_DIR,
        input_file=args.input_file,
        sheet_name=args.sheet_name,
        output_dir=Path(args.output_dir).expanduser().resolve(),
        snapshot_dir=Path(args.snapshot_dir).expanduser().resolve(),
        manifest_dir=Path(args.manifest_dir).expanduser().resolve(),
        metadata=metadata,
    )
    print(
        "analysis_mode=raw-evidence\n"
        f"workbook={outputs['default_workbook']}\n"
        f"workbook_compatibility={outputs['workbook_compatibility']}\n"
        f"anomaly_report={outputs['default_anomaly_report']}\n"
        f"anomaly_report_compatibility={outputs['anomaly_report_compatibility']}\n"
        f"raw_snapshot={outputs['default_snapshot']}\n"
        f"raw_snapshot_compatibility={outputs['raw_snapshot_compatibility']}\n"
        f"snapshot_manifest={outputs['default_snapshot_manifest']}\n"
        f"snapshot_manifest_compatibility={outputs['snapshot_manifest_compatibility']}\n"
        f"theme_manifest={outputs['theme_manifest']}\n"
        f"analysis_naming_status={outputs['naming_status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
