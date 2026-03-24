"""Topline summary builder for Feishu daily reports."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from oae.exports.feishu_panel_utils import ACCOUNT_LABEL_MAP
from oae.performance.loader_utils import pick_live_column
from oae.rules.file_discovery import pick_latest_file


@dataclass
class FullAccountTopline:
    impression_target: float
    impression_actual: float
    impression_attain: float | None
    lead_target: float
    lead_actual: int
    lead_attain: float | None
    deal_target: float
    deal_actual: int
    deal_attain: float | None
    cpl_target: float
    cpl_actual: float | None
    cps_target: float
    cps_actual: float | None
    pending_day: int
    pending_cumulative: int


@dataclass
class SegmentTopline:
    label: str
    leads: int
    deals: int
    cpl_actual: float | None
    cps_actual: float | None


@dataclass
class ToplineSummary:
    full_account: FullAccountTopline
    excluding_ex7: SegmentTopline
    ex7: SegmentTopline


def load_topline_config(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"[ERROR] 日报顶部汇报配置不存在: {path}")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"[ERROR] 日报顶部汇报配置 JSON 非法: {path}, err={exc}") from exc

    required = {"full_account_targets", "ex7_rules", "pending_rules"}
    missing = required - set(config.keys())
    if missing:
        raise SystemExit(f"[ERROR] 日报顶部汇报配置缺少字段: {sorted(missing)}")
    return config


def resolve_latest_source_file(explicit_path: str | Path | None, search_dirs: list[Path], pattern_expr: str, label: str) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if path.exists():
            return path

    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        try:
            return pick_latest_file(search_dir, pattern_expr)
        except FileNotFoundError:
            continue
    raise SystemExit(f"[ERROR] 未找到{label}文件，搜索模式={pattern_expr}，搜索目录={[str(item) for item in search_dirs]}")


def load_source_csv(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "gb18030", "utf-8"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    raise SystemExit(f"[ERROR] 读取源文件失败: {path}, err={last_error}")


def load_leads_source(path: Path) -> pd.DataFrame:
    data = load_source_csv(path)
    required = {"线索ID", "创建日期"}
    missing = required - set(data.columns)
    if missing:
        raise SystemExit(f"[ERROR] 线索源文件缺少字段: {sorted(missing)}, file={path}")
    return data.copy()


def load_deals_source(path: Path) -> pd.DataFrame:
    data = load_source_csv(path)
    required = {"线索ID", "订单状态"}
    missing = required - set(data.columns)
    if missing:
        raise SystemExit(f"[ERROR] 成交源文件缺少字段: {sorted(missing)}, file={path}")
    return data.copy()


def _normalize_id(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip()
    out = out.replace({"nan": "", "None": "", "none": "", "null": "", "NaT": ""})
    return out


def _keyword_pattern(keywords: list[str]) -> re.Pattern[str]:
    escaped = [re.escape(str(item).strip().upper()) for item in keywords if str(item).strip()]
    if not escaped:
        raise SystemExit("[ERROR] EX7 识别规则未配置有效关键词")
    return re.compile("|".join(escaped))


def _contains_keywords(series: pd.Series, pattern: re.Pattern[str]) -> pd.Series:
    text = series.fillna("").astype(str).str.upper()
    return text.str.contains(pattern, na=False)


def _build_model_lookup(source: pd.DataFrame, id_col: str, model_fields: list[str], pattern: re.Pattern[str]) -> pd.DataFrame:
    existing_fields = [field for field in model_fields if field in source.columns]
    if not existing_fields:
        raise SystemExit(f"[ERROR] EX7 识别缺少车型字段，候选={model_fields}，当前={list(source.columns)}")

    out = source[[id_col] + existing_fields].copy()
    out["线索ID_norm"] = _normalize_id(out[id_col])
    out = out[out["线索ID_norm"] != ""].copy()
    if out.empty:
        return pd.DataFrame(columns=["线索ID_norm", "is_ex7_source"])

    hit = pd.Series(False, index=out.index)
    for field in existing_fields:
        hit = hit | _contains_keywords(out[field], pattern)
    out["is_ex7_source"] = hit.astype(bool)
    return out.groupby("线索ID_norm", as_index=False)["is_ex7_source"].max()


def annotate_fact_with_ex7_partition(
    fact: pd.DataFrame,
    leads_source: pd.DataFrame,
    deals_source: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    pattern = _keyword_pattern(config["ex7_rules"]["keywords"])
    lead_lookup = _build_model_lookup(
        leads_source,
        id_col="线索ID",
        model_fields=config["ex7_rules"]["lead_model_field_candidates"],
        pattern=pattern,
    ).rename(columns={"is_ex7_source": "is_ex7_from_leads"})
    deal_lookup = _build_model_lookup(
        deals_source,
        id_col="线索ID",
        model_fields=config["ex7_rules"]["deal_model_field_candidates"],
        pattern=pattern,
    ).rename(columns={"is_ex7_source": "is_ex7_from_deals"})

    out = fact.copy()
    out["线索ID_norm"] = _normalize_id(out["线索ID_norm"])
    out = out.merge(lead_lookup, on="线索ID_norm", how="left")
    out = out.merge(deal_lookup, on="线索ID_norm", how="left")

    if "成交车型" in out.columns:
        deal_model_fallback = _contains_keywords(out["成交车型"], pattern)
    else:
        deal_model_fallback = pd.Series(False, index=out.index)

    # EX7 优先按原始线索表的结构化车型字段判断，缺失时再回退到成交表/事实表成交车型。
    out["is_ex7_partition"] = (
        out["is_ex7_from_leads"]
        .where(out["is_ex7_from_leads"].notna(), out["is_ex7_from_deals"])
        .where(lambda s: s.notna(), deal_model_fallback)
        .fillna(False)
        .astype(bool)
    )
    return out


def _safe_div(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _parse_date_series(df: pd.DataFrame, field: str) -> pd.Series:
    if field not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[field], errors="coerce").dt.normalize()


def build_pending_account_summary(
    fact: pd.DataFrame,
    deals_source: pd.DataFrame,
    report_date: pd.Timestamp,
    target_accounts: list[str],
    config: dict,
) -> tuple[int, int, str, str, str]:
    pending_rules = config["pending_rules"]
    pending_source = deals_source.copy()
    pending_source["线索ID_norm"] = _normalize_id(pending_source["线索ID"])
    pending_source = pending_source[pending_source["线索ID_norm"] != ""].copy()
    if pending_source.empty:
        return 0, 0, "无", "无", "无"

    primary = pending_rules["primary_date_field"]
    fallbacks = pending_rules.get("fallback_date_fields", [])

    # 待交车累计按“订单所属月份”判断，因此优先使用下订日期；缺失时依次回退到下订时间/成交日期/成交时间。
    order_date = _parse_date_series(pending_source, primary)
    for field in fallbacks:
        fallback_date = _parse_date_series(pending_source, field)
        order_date = order_date.where(order_date.notna(), fallback_date)
    pending_source["order_date"] = order_date
    pending_source = pending_source[pending_source["订单状态"].astype(str).str.strip() == "待交车"].copy()
    if pending_source.empty:
        return 0, 0, "无", "无", "无"

    base = fact[["线索ID_norm", "标准账号"]].drop_duplicates().copy()
    pending = base.merge(pending_source[["线索ID_norm", "order_date"]], on="线索ID_norm", how="inner")
    if pending.empty:
        return 0, 0, "无", "无", "无"

    current_month_start = report_date.to_period("M").to_timestamp().normalize()
    previous_month_start = (current_month_start - pd.DateOffset(months=1)).normalize()

    recent = pending[
        pending["order_date"].notna()
        & (pending["order_date"] >= previous_month_start)
        & (pending["order_date"] <= report_date)
    ].copy()
    if recent.empty:
        return 0, 0, "无", "无", "无"

    day = recent[recent["order_date"] == report_date].copy()
    return (
        int(day["线索ID_norm"].nunique()),
        int(recent["线索ID_norm"].nunique()),
        _format_account_counts(day[day["标准账号"].isin(target_accounts)]),
        _format_account_counts(recent[recent["标准账号"].isin(target_accounts)]),
        _format_account_counts(recent),
    )


def _format_account_counts(df: pd.DataFrame) -> str:
    if df.empty or "标准账号" not in df.columns:
        return "无"
    grouped = df.groupby("标准账号")["线索ID_norm"].nunique().sort_values(ascending=False)
    if grouped.empty:
        return "无"
    return "、".join(f"{ACCOUNT_LABEL_MAP.get(account, account)}({int(count)}台)" for account, count in grouped.items())


def _load_live_metrics(live_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    raw = live_df.copy()
    raw.columns = [str(col).strip() for col in raw.columns]
    date_col = pick_live_column(raw, ["日期", "直播日期", "创建时间"])
    spend_col = pick_live_column(raw, ["消耗", "实际消耗", "当日消耗", "花费", "费用", "投放消耗", "总消耗"])
    exposure_col = pick_live_column(raw, ["曝光", "展现", "曝光量"])
    model_col = pick_live_column(raw, config["ex7_rules"]["live_model_field_candidates"])

    metrics = raw[[date_col, spend_col, exposure_col, model_col]].copy()
    metrics.columns = ["date", "spend", "impressions", "model"]
    metrics["date"] = pd.to_datetime(metrics["date"], errors="coerce").dt.normalize()
    metrics["spend"] = pd.to_numeric(metrics["spend"], errors="coerce").fillna(0.0)
    metrics["impressions"] = pd.to_numeric(metrics["impressions"], errors="coerce").fillna(0.0)
    metrics["model"] = metrics["model"].fillna("").astype(str).str.strip()
    return metrics.dropna(subset=["date"])


def _segment_fact_counts(
    fact: pd.DataFrame,
    month_start: pd.Timestamp,
    report_date: pd.Timestamp,
    is_ex7: bool | None,
) -> tuple[int, int]:
    source = fact.copy()
    if is_ex7 is not None:
        source = source[source["is_ex7_partition"] == is_ex7].copy()

    lead_mask = (
        source["date"].between(month_start, report_date)
        & source["线索ID_norm"].astype(str).str.strip().ne("")
    )
    deal_mask = (
        pd.to_numeric(source["is_deal"], errors="coerce").fillna(0).eq(1)
        & source["deal_date"].notna()
        & source["deal_date"].between(month_start, report_date)
    )
    return (
        int(source.loc[lead_mask, "线索ID_norm"].nunique()),
        int(source.loc[deal_mask, "线索ID_norm"].nunique()),
    )


def _segment_live_metrics(
    live_metrics: pd.DataFrame,
    month_start: pd.Timestamp,
    report_date: pd.Timestamp,
    config: dict,
    is_ex7: bool | None,
) -> tuple[float, float]:
    month_live = live_metrics[live_metrics["date"].between(month_start, report_date)].copy()
    if month_live.empty:
        return 0.0, 0.0

    pattern = _keyword_pattern(config["ex7_rules"]["keywords"])
    month_live["is_ex7_partition"] = _contains_keywords(month_live["model"], pattern)
    if is_ex7 is not None:
        month_live = month_live[month_live["is_ex7_partition"] == is_ex7].copy()
    if month_live.empty:
        return 0.0, 0.0
    return float(month_live["impressions"].sum()), float(month_live["spend"].sum())


def build_topline_summary(
    fact: pd.DataFrame,
    live_df: pd.DataFrame,
    leads_source: pd.DataFrame,
    deals_source: pd.DataFrame,
    report_date: pd.Timestamp,
    config: dict,
) -> ToplineSummary:
    month_start = report_date.to_period("M").to_timestamp().normalize()
    fact_ready = fact.copy()
    fact_ready["date"] = pd.to_datetime(fact_ready["date"], errors="coerce").dt.normalize()
    if "deal_date" in fact_ready.columns:
        fact_ready["deal_date"] = pd.to_datetime(fact_ready["deal_date"], errors="coerce").dt.normalize()
    elif "成交时间" in fact_ready.columns:
        fact_ready["deal_date"] = pd.to_datetime(fact_ready["成交时间"], errors="coerce").dt.normalize()
    else:
        fact_ready["deal_date"] = pd.NaT
    if "is_deal" in fact_ready.columns:
        fact_ready["is_deal"] = pd.to_numeric(fact_ready["is_deal"], errors="coerce").fillna(0)
    else:
        fact_ready["is_deal"] = 0

    fact_partitioned = annotate_fact_with_ex7_partition(fact_ready, leads_source, deals_source, config)
    live_metrics = _load_live_metrics(live_df, config)

    full_impressions, full_spend = _segment_live_metrics(live_metrics, month_start, report_date, config, None)
    ex7_impressions, ex7_spend = _segment_live_metrics(live_metrics, month_start, report_date, config, True)
    non_ex7_spend = max(full_spend - ex7_spend, 0.0)

    full_leads, full_deals = _segment_fact_counts(fact_partitioned, month_start, report_date, None)
    ex7_leads, ex7_deals = _segment_fact_counts(fact_partitioned, month_start, report_date, True)
    non_ex7_leads = max(full_leads - ex7_leads, 0)
    non_ex7_deals = max(full_deals - ex7_deals, 0)

    pending_day, pending_cumulative, _, _, _ = build_pending_account_summary(
        fact=fact_partitioned,
        deals_source=deals_source,
        report_date=report_date,
        target_accounts=[],
        config=config,
    )

    targets = config["full_account_targets"]
    return ToplineSummary(
        full_account=FullAccountTopline(
            impression_target=float(targets["impressions"]),
            impression_actual=full_impressions,
            impression_attain=_safe_div(full_impressions, float(targets["impressions"])),
            lead_target=float(targets["leads"]),
            lead_actual=full_leads,
            lead_attain=_safe_div(full_leads, float(targets["leads"])),
            deal_target=float(targets["deals"]),
            deal_actual=full_deals,
            deal_attain=_safe_div(full_deals, float(targets["deals"])),
            cpl_target=float(targets["cpl"]),
            cpl_actual=_safe_div(full_spend, full_leads),
            cps_target=float(targets["cps"]),
            cps_actual=_safe_div(full_spend, full_deals),
            pending_day=pending_day,
            pending_cumulative=pending_cumulative,
        ),
        excluding_ex7=SegmentTopline(
            label="不含 EX7",
            leads=non_ex7_leads,
            deals=non_ex7_deals,
            cpl_actual=_safe_div(non_ex7_spend, non_ex7_leads),
            cps_actual=_safe_div(non_ex7_spend, non_ex7_deals),
        ),
        ex7=SegmentTopline(
            label="EX7 专项",
            leads=ex7_leads,
            deals=ex7_deals,
            cpl_actual=_safe_div(ex7_spend, ex7_leads),
            cps_actual=_safe_div(ex7_spend, ex7_deals),
        ),
    )


def format_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.2f}%"


def format_metric(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    text = f"{float(value):.{digits}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def format_impressions(value: float) -> str:
    text = f"{value / 10000:.2f}".rstrip("0").rstrip(".")
    return f"{text}万"


def build_markdown_topline_lines(summary: ToplineSummary) -> list[str]:
    full = summary.full_account
    non_ex7 = summary.excluding_ex7
    ex7 = summary.ex7
    return [
        "**全量账号**",
        f"- 曝光：目标 {format_impressions(full.impression_target)}，实际 {format_impressions(full.impression_actual)}，达成率 {format_pct(full.impression_attain)}",
        f"- 线索：目标 {int(full.lead_target)}，实际达成 {int(full.lead_actual)}，达成率 {format_pct(full.lead_attain)}",
        f"- 实销：目标 {int(full.deal_target)}，实际达成 {int(full.deal_actual)}，达成率 {format_pct(full.deal_attain)}",
        f"- 总体 CPL：目标 {format_metric(full.cpl_target, 0)}，实际 {format_metric(full.cpl_actual)}",
        f"- 总体 CPS：目标 {format_metric(full.cps_target, 0)}，实际 {format_metric(full.cps_actual)}",
        f"- 待交车（当日）：{int(full.pending_day)}",
        f"- 待交车（累计）：{int(full.pending_cumulative)}",
        "",
        "**不含 EX7**",
        f"- 线索：{int(non_ex7.leads)}",
        f"- 实销：{int(non_ex7.deals)}",
        f"- 实际 CPL：{format_metric(non_ex7.cpl_actual)}",
        f"- 实际 CPS：{format_metric(non_ex7.cps_actual)}",
        "",
        "**EX7 专项**",
        f"- 线索：{int(ex7.leads)}",
        f"- 实销：{int(ex7.deals)}",
        f"- 实际 CPL：{format_metric(ex7.cpl_actual)}",
        f"- 实际 CPS：{format_metric(ex7.cps_actual)}",
    ]


def build_tsv_topline_lines(report_date_str: str, summary: ToplineSummary) -> list[str]:
    full = summary.full_account
    non_ex7 = summary.excluding_ex7
    ex7 = summary.ex7
    return [
        f"日报日期\t{report_date_str}",
        "",
        "全量账号",
        "指标\t目标\t实际达成\t达成率",
        f"曝光\t{format_impressions(full.impression_target)}\t{format_impressions(full.impression_actual)}\t{format_pct(full.impression_attain)}",
        f"线索\t{int(full.lead_target)}\t{int(full.lead_actual)}\t{format_pct(full.lead_attain)}",
        f"实销\t{int(full.deal_target)}\t{int(full.deal_actual)}\t{format_pct(full.deal_attain)}",
        f"总体 CPL\t{format_metric(full.cpl_target, 0)}\t{format_metric(full.cpl_actual)}\t-",
        f"总体 CPS\t{format_metric(full.cps_target, 0)}\t{format_metric(full.cps_actual)}\t-",
        f"待交车（当日）\t-\t{int(full.pending_day)}\t-",
        f"待交车（累计）\t-\t{int(full.pending_cumulative)}\t-",
        "",
        "不含 EX7",
        "指标\t结果",
        f"线索\t{int(non_ex7.leads)}",
        f"实销\t{int(non_ex7.deals)}",
        f"实际 CPL\t{format_metric(non_ex7.cpl_actual)}",
        f"实际 CPS\t{format_metric(non_ex7.cps_actual)}",
        "",
        "EX7 专项",
        "指标\t结果",
        f"线索\t{int(ex7.leads)}",
        f"实销\t{int(ex7.deals)}",
        f"实际 CPL\t{format_metric(ex7.cpl_actual)}",
        f"实际 CPS\t{format_metric(ex7.cps_actual)}",
        "",
    ]
