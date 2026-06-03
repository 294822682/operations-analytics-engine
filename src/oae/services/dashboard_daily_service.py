from __future__ import annotations

import calendar
from datetime import date, timedelta
import json
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd

from oae.exports.feishu_dashboard_interactive_html import DashboardSource, Metric
from oae.exports.feishu_douyin_laike import build_douyin_laike_order_metrics
from oae.exports.feishu_panel_utils import ACCOUNT_LABEL_MAP
from oae.exports.feishu_topline import annotate_fact_with_ex7_partition, load_topline_config
from oae.performance.fact_loader import load_fact
from oae.performance.loader_utils import normalize_account, normalize_text, pick_live_column, split_hosts
from oae.rules.columns import COLUMN_ALIASES, pick_col
from oae.rules.io_utils import read_table_auto
from oae.utils import ApiError, InvalidReportDateError, normalize_report_date


MAX_TREND_RANGE_DAYS = 92
HIDDEN_ACCOUNT_SUMMARY_NAMES = {
    normalize_account(name)
    for name in [
        "视频号-星途星纪元",
        "星途星纪元",
        "星途星纪元直播营销中心+",
        "抖音",
        "快手-星途星纪元",
        "抖店",
    ]
}
HIDDEN_ANCHOR_SUMMARY_NAMES = {normalize_text(name) for name in ["王君如"]}
HIDDEN_ANCHOR_PERFORMANCE_NAMES = HIDDEN_ANCHOR_SUMMARY_NAMES | {normalize_text(name) for name in ["桂婕"]}
FIXED_SEED_ANCHOR_NAMES = {normalize_text(name) for name in ["桂婕"]}
SOURCE_ENTITY_SUPPLEMENT_METRICS = {
    "visits",
    "visit_rate",
    "visit_deal_rate",
    "ex7_leads",
    "ex7_deals",
    "ex7_deal_rate",
}


class DashboardDailyService:
    def __init__(self, *, repo_root: Path) -> None:
        self.repo_root = repo_root.expanduser().resolve()

    def get_daily_dashboard(self, report_date: str) -> dict[str, Any]:
        try:
            report_date = normalize_report_date(report_date)
        except ValueError as exc:
            raise InvalidReportDateError(report_date) from exc

        source_path = self._source_path(report_date)
        if not source_path.exists():
            raise ApiError(
                "DASHBOARD_SOURCE_NOT_FOUND",
                f"Dashboard source TSV not found for report_date={report_date}",
                404,
                {
                    "report_date": report_date,
                    "expected_path": self._public_path(source_path),
                },
            )

        source = DashboardSource.from_tsv(source_path)
        overview = self._overview(source)
        return {
            "report_date": report_date,
            "available_report_dates": self._available_report_dates_from_sources(),
            "source": {
                "type": "feishu_dashboard_source_tsv",
                "path": self._public_path(source_path),
                "rows": len(source.rows),
            },
            "overview": overview,
            "funnel": self._funnel(overview),
            "segments": self._segments(source),
            "lead_anchors": self._anchors(
                source,
                "lead_anchor",
                ["mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_deals", "mtd_cpl", "mtd_cps"],
            ),
            "seed_account": self._metric_payload(source.metric("account", "EXEED星途", "mtd_impressions")),
            "seed_anchors": self._anchors(source, "seed_anchor", ["mtd_impressions", "daily_impressions"]),
            "interactions": {
                "module_anchors": ["overview", "funnel", "segment-compare", "lead-anchors", "seed-exposure"],
                "lead_anchor_sort_keys": ["mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_cpl"],
                "seed_anchor_sort_keys": ["mtd_impressions", "mtd_impressions_attain_rate"],
            },
        }

    def get_latest_daily_dashboard(self) -> dict[str, Any]:
        report_date = self._latest_report_date_from_sources()
        if not report_date:
            raise ApiError(
                "DASHBOARD_SOURCE_NOT_FOUND",
                "No dashboard source TSV found",
                404,
                {
                    "report_date": "latest",
                    "expected_glob": self._public_path(self.repo_root / "output" / "sql_reports")
                    + "/feishu_dashboard_source_latest_*.tsv",
                },
            )
        return self.get_daily_dashboard(report_date)

    def get_daily_dashboard_trends(self, *, start_date: str | None = None, end_date: str | None = None) -> dict[str, Any]:
        window = self._resolve_trend_window(start_date=start_date, end_date=end_date)
        report_dates = self._filter_report_dates(
            self._available_report_dates_from_sources(),
            start_date=window["start_date"],
            end_date=window["end_date"],
        )
        if not report_dates:
            reports_dir = self.repo_root / "output" / "sql_reports"
            raise ApiError(
                "DASHBOARD_SOURCE_NOT_FOUND",
                "No dashboard source TSV found for trend view",
                404,
                {
                    "report_date": "trend",
                    "expected_glob": self._public_path(reports_dir) + "/feishu_dashboard_source_latest_*.tsv",
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

        sources = [(report_date, self._source_path(report_date), DashboardSource.from_tsv(self._source_path(report_date))) for report_date in report_dates]
        source_rows = sum(len(source.rows) for _, _, source in sources)
        source_paths = [self._public_path(path) for _, path, _ in sources]
        missing_dates = self._missing_calendar_dates(report_dates)
        quality_annotations = self._trend_quality_annotations(report_dates)
        source_range_label = (
            f"{report_dates[0]} 至 {report_dates[-1]}"
            if report_dates
            else f"{window['start_date']} 至 {window['end_date']}"
        )
        core_kpis = self._trend_metrics(
            sources,
            "topline",
            "department",
            "全量",
            ["impressions", "mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"],
            quality_annotations,
        )
        segments = {
            "ex7": self._trend_entity(
                sources,
                "topline_segment",
                "segment",
                ["EX7 专项", "EX7专项"],
                ["mtd_unique_leads", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"],
                quality_annotations,
            ),
            "non_ex7": self._trend_entity(
                sources,
                "topline_segment",
                "segment",
                ["不含 EX7", "不含EX7"],
                ["mtd_unique_leads", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"],
                quality_annotations,
            ),
        }
        accounts = self._trend_entities_for_source(
            sources,
            "lead_account",
            "account",
            ["mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"],
            quality_annotations,
        )
        lead_anchors = self._trend_entities_for_source(
            sources,
            "lead_anchor",
            "anchor",
            ["mtd_unique_leads", "mtd_deals", "mtd_douyin_laike_orders", "mtd_spend", "mtd_cpl", "mtd_cps"],
            quality_annotations,
        )
        seed_account = self._trend_entity(
            sources,
            "seed_account",
            "account",
            ["EXEED星途"],
            ["daily_impressions", "mtd_impressions"],
            quality_annotations,
        )
        seed_anchors = self._trend_entities_for_source(
            sources,
            "seed_anchor",
            "anchor",
            ["daily_impressions", "mtd_impressions"],
            quality_annotations,
        )
        payload = {
            "contract_version": "n8-v1-trend-1",
            "date_range": {
                "start": window["start_date"],
                "end": window["end_date"],
                "start_date": window["start_date"],
                "end_date": window["end_date"],
                "days": window["days"],
                "selected_range_days": window["days"],
                "date_count": len(report_dates),
                "available_dates": report_dates,
                "missing_dates": missing_dates,
            },
            "selected_range_days": window["days"],
            "available_dates": report_dates,
            "missing_dates": missing_dates,
            "source_type": "feishu_dashboard_source_tsv_history",
            "available_report_dates": report_dates,
            "source": {
                "type": "feishu_dashboard_source_tsv_history",
                "paths": source_paths,
                "rows": source_rows,
                "date_range_label": source_range_label,
                "quality_annotation_source": "run_manifest_and_quality_report",
            },
            "quality_annotations": quality_annotations,
            "core_kpis": core_kpis,
            "segments": segments,
            "accounts": accounts,
            "lead_anchors": lead_anchors,
            "seed_account": seed_account,
            "seed_anchors": seed_anchors,
            "quality_note": "质量状态仅用于人工提示；N8 v0 不改写 dashboard source 数据。",
        }
        payload.update(
            self._business_trend_payload_from_dashboard_sources(
                window=window,
                core_kpis=core_kpis,
                segments=segments,
                accounts=accounts,
                lead_anchors=lead_anchors,
                seed_account=seed_account,
                seed_anchors=seed_anchors,
            )
        )
        return payload

    def _source_path(self, report_date: str) -> Path:
        return self.repo_root / "output" / "sql_reports" / f"feishu_dashboard_source_latest_{report_date}.tsv"

    def _latest_report_date_from_sources(self) -> str:
        candidates = self._available_report_dates_from_sources()
        return candidates[-1] if candidates else ""

    def _available_report_dates_from_sources(self) -> list[str]:
        reports_dir = self.repo_root / "output" / "sql_reports"
        if not reports_dir.exists():
            return []
        pattern = re.compile(r"^feishu_dashboard_source_latest_(\d{4}-\d{2}-\d{2})\.tsv$")
        candidates: list[str] = []
        for path in reports_dir.glob("feishu_dashboard_source_latest_*.tsv"):
            matched = pattern.match(path.name)
            if not matched:
                continue
            report_date = matched.group(1)
            try:
                normalize_report_date(report_date)
            except ValueError:
                continue
            candidates.append(report_date)
        return sorted(set(candidates))

    @staticmethod
    def _filter_report_dates(
        report_dates: list[str],
        *,
        start_date: str | None,
        end_date: str | None,
    ) -> list[str]:
        normalized_start = ""
        normalized_end = ""
        if start_date:
            try:
                normalized_start = normalize_report_date(start_date)
            except ValueError as exc:
                raise InvalidReportDateError(start_date) from exc
        if end_date:
            try:
                normalized_end = normalize_report_date(end_date)
            except ValueError as exc:
                raise InvalidReportDateError(end_date) from exc
        return [
            report_date
            for report_date in report_dates
            if (not normalized_start or report_date >= normalized_start)
            and (not normalized_end or report_date <= normalized_end)
        ]

    def _resolve_trend_window(self, *, start_date: str | None, end_date: str | None) -> dict[str, Any]:
        normalized_start = self._normalize_optional_date(start_date)
        normalized_end = self._normalize_optional_date(end_date)
        latest_available = self._latest_available_business_date()

        if not normalized_end:
            normalized_end = latest_available or normalized_start
        if not normalized_start and normalized_end:
            normalized_start = self._quarter_default_start(normalized_end)
        if normalized_start and not normalized_end:
            normalized_end = normalized_start

        if not normalized_start or not normalized_end:
            raise ApiError(
                "DASHBOARD_SOURCE_NOT_FOUND",
                "No dashboard source TSV found for trend view",
                404,
                {
                    "report_date": "trend",
                    "expected_glob": self._public_path(self.repo_root / "output" / "sql_reports")
                    + "/feishu_dashboard_source_latest_*.tsv",
                },
            )

        start = date.fromisoformat(normalized_start)
        end = date.fromisoformat(normalized_end)
        if start > end:
            raise ApiError(
                "DASHBOARD_INVALID_RANGE",
                "开始日期不能晚于结束日期，请调整日期范围。",
                400,
                {"start_date": normalized_start, "end_date": normalized_end},
            )
        days = (end - start).days + 1
        if days > MAX_TREND_RANGE_DAYS:
            raise ApiError(
                "DASHBOARD_RANGE_TOO_LONG",
                "单次查看范围建议不超过一个季度，请缩小日期范围。",
                400,
                {
                    "start_date": normalized_start,
                    "end_date": normalized_end,
                    "days": days,
                    "max_days": MAX_TREND_RANGE_DAYS,
                },
            )
        return {"start_date": normalized_start, "end_date": normalized_end, "days": days}

    @staticmethod
    def _quarter_default_start(end_date: str) -> str:
        end = date.fromisoformat(end_date)
        month_index = end.year * 12 + end.month - 1 - 2
        year = month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1).isoformat()

    @staticmethod
    def _normalize_optional_date(value: str | None) -> str:
        if not value:
            return ""
        try:
            return normalize_report_date(value)
        except ValueError as exc:
            raise InvalidReportDateError(value) from exc

    def _latest_available_business_date(self) -> str:
        candidates = self._available_report_dates_from_sources()
        return sorted(set(candidates))[-1] if candidates else ""

    @staticmethod
    def _calendar_date_strings(start_date: str, end_date: str) -> list[str]:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        out: list[str] = []
        current = start
        while current <= end:
            out.append(current.isoformat())
            current += timedelta(days=1)
        return out

    @staticmethod
    def _previous_trend_window(window: dict[str, Any]) -> dict[str, Any]:
        start = date.fromisoformat(window["start_date"])
        days = int(window["days"])
        previous_end = start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=days - 1)
        return {
            "start_date": previous_start.isoformat(),
            "end_date": previous_end.isoformat(),
            "days": days,
        }

    @classmethod
    def _business_window_context(
        cls,
        *,
        fact: pd.DataFrame,
        raw_deals: pd.DataFrame,
        live_sessions: pd.DataFrame,
        seed_sessions: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        date_strings = cls._calendar_date_strings(start_date, end_date)
        start_ts = pd.to_datetime(start_date).normalize()
        end_ts = pd.to_datetime(end_date).normalize()
        live_range = cls._filter_frame_dates(live_sessions, start_ts, end_ts)
        seed_range = cls._filter_frame_dates(seed_sessions, start_ts, end_ts)
        core_live_range = (
            pd.concat(
                [frame for frame in [live_range, seed_range] if not frame.empty],
                ignore_index=True,
                sort=False,
            )
            if (not live_range.empty or not seed_range.empty)
            else pd.DataFrame()
        )

        if fact.empty or "date" not in fact.columns:
            lead_rows = pd.DataFrame()
            visit_rows = pd.DataFrame()
        else:
            lead_rows = fact[
                fact["date"].between(start_ts, end_ts)
                & (pd.to_numeric(fact.get("is_perf_primary_lead", 0), errors="coerce").fillna(0).eq(1))
                & fact["_perf_lead_key"].astype(str).str.strip().ne("")
            ].copy()
            visit_rows = fact[
                fact["visit_date"].notna()
                & fact["visit_date"].between(start_ts, end_ts)
                & (pd.to_numeric(fact.get("is_perf_primary_lead", 0), errors="coerce").fillna(0).eq(1))
            ].copy()
        deal_rows = cls._deal_rows_for_window(fact, raw_deals, start_ts, end_ts)
        return {
            "date_strings": date_strings,
            "lead_rows": lead_rows,
            "deal_rows": deal_rows,
            "visit_rows": visit_rows,
            "live_range": live_range,
            "seed_range": seed_range,
            "core_live_range": core_live_range,
        }

    def _business_trend_payload_from_dashboard_sources(
        self,
        *,
        window: dict[str, Any],
        core_kpis: dict[str, dict[str, Any]],
        segments: dict[str, dict[str, Any]],
        accounts: list[dict[str, Any]],
        lead_anchors: list[dict[str, Any]],
        seed_account: dict[str, Any],
        seed_anchors: list[dict[str, Any]],
    ) -> dict[str, Any]:
        daily_trends = self._core_daily_trends_from_dashboard_sources(core_kpis)
        previous_window = self._previous_trend_window(window)
        previous_report_dates = self._filter_report_dates(
            self._available_report_dates_from_sources(),
            start_date=previous_window["start_date"],
            end_date=previous_window["end_date"],
        )
        previous_quality = self._trend_quality_annotations(previous_report_dates)
        previous_sources = [
            (report_date, self._source_path(report_date), DashboardSource.from_tsv(self._source_path(report_date)))
            for report_date in previous_report_dates
        ]
        previous_core_kpis = (
            self._trend_metrics(
                previous_sources,
                "topline",
                "department",
                "全量",
                ["impressions", "mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"],
                previous_quality,
            )
            if previous_sources
            else {}
        )
        previous_daily_trends = (
            self._core_daily_trends_from_dashboard_sources(previous_core_kpis) if previous_core_kpis else []
        )
        previous_has_data = self._trend_payloads_have_data(previous_daily_trends)
        previous_period = {
            **previous_window,
            "has_data": previous_has_data,
            "message": "" if previous_has_data else "上一周期数据不足",
        }
        supplemental_payload = self._build_business_trend_payload(window)
        seed_exposure = self._seed_exposure_from_dashboard_sources(seed_account, seed_anchors)
        account_summary = [self._source_entity_summary(item, scope_type="account") for item in accounts]
        anchor_summary = [self._source_entity_summary(item, scope_type="anchor") for item in lead_anchors]
        if supplemental_payload:
            account_summary = self._merge_source_entity_supplements(
                account_summary,
                supplemental_payload.get("account_summary", []),
                scope_type="account",
            )
            anchor_summary = self._merge_source_entity_supplements(
                anchor_summary,
                supplemental_payload.get("anchor_summary", []),
                scope_type="anchor",
            )
        return {
            "daily_trends": daily_trends,
            "core_kpi_summary": self._core_summary_from_dashboard_sources(core_kpis),
            "previous_period": previous_period,
            "previous_period_trends": previous_daily_trends if previous_has_data else [],
            "monthly_comparison": self._merge_monthly_comparison(
                self._monthly_comparison(daily_trends, window, aggregation="latest"),
                supplemental_payload.get("monthly_comparison", []) if supplemental_payload else [],
            ),
            "model_segment_summary": self._segment_summary_from_dashboard_sources(segments),
            "account_summary": account_summary,
            "account_daily_trends": [
                {"name": item["name"], "parent_scope": item.get("parent_scope", ""), "daily_trends": item["daily_trends"]}
                for item in account_summary
            ],
            "anchor_summary": anchor_summary,
            "anchor_daily_trends": [
                {"name": item["name"], "parent_scope": item.get("parent_scope", ""), "daily_trends": item["daily_trends"]}
                for item in anchor_summary
            ],
            "seed_exposure_summary": seed_exposure["summary"],
            "seed_exposure_daily_trends": seed_exposure["daily_trends"],
            "metric_source_status": self._dashboard_source_status(),
        }

    @classmethod
    def _core_summary_from_dashboard_sources(cls, core_kpis: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            cls._summary_from_series(core_kpis.get(source_key, {}), business_key, label)
            for source_key, business_key, label in cls._core_metric_specs()
        ]

    @classmethod
    def _core_daily_trends_from_dashboard_sources(cls, core_kpis: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            cls._daily_trend_from_series(core_kpis.get(source_key, {}), business_key, label)
            for source_key, business_key, label in cls._core_metric_specs()
        ]

    @staticmethod
    def _core_metric_specs() -> list[tuple[str, str, str]]:
        return [
            ("impressions", "impressions", "曝光"),
            ("mtd_unique_leads", "leads", "线索"),
            ("mtd_douyin_laike_orders", "douyin_laike_orders", "来客订单"),
            ("mtd_deals", "deals", "实销"),
            ("mtd_spend", "spend", "费用"),
            ("mtd_cpl", "cpl", "CPL"),
            ("mtd_cps", "cps", "CPS"),
        ]

    @classmethod
    def _segment_summary_from_dashboard_sources(cls, segments: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            cls._segment_business_summary("EX7", segments.get("ex7", {})),
            cls._segment_business_summary("不含 EX7", segments.get("non_ex7", {})),
        ]

    @classmethod
    def _segment_business_summary(cls, name: str, entity: dict[str, Any]) -> dict[str, Any]:
        metrics = entity.get("metrics", {})
        return {
            "name": name,
            "parent_scope": entity.get("parent_scope", ""),
            "metrics": cls._source_entity_metrics(metrics),
            "daily_trends": cls._source_entity_daily_trends(metrics),
        }

    @classmethod
    def _source_entity_summary(cls, entity: dict[str, Any], *, scope_type: str) -> dict[str, Any]:
        metrics = cls._source_entity_metrics(entity.get("metrics", {}))
        metric_groups = cls._entity_metric_groups(metrics)
        return {
            "name": entity.get("name", ""),
            "display_type": "账号表现" if scope_type == "account" else "主播表现",
            "parent_scope": entity.get("parent_scope", ""),
            "metrics": metrics,
            "metric_groups": metric_groups,
            "daily_trends": cls._source_entity_daily_trends(entity.get("metrics", {})),
        }

    @staticmethod
    def _entity_metric_groups(metrics: dict[str, dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
        return {
            "线索": {
                "leads": metrics["leads"],
                "unique_leads": metrics["unique_leads"],
            },
            "来客订单": {
                "douyin_laike_orders": metrics["douyin_laike_orders"],
            },
            "到店": {
                "visits": metrics["visits"],
                "visit_rate": metrics["visit_rate"],
            },
            "成交": {
                "deals": metrics["deals"],
                "lead_deal_rate": metrics["lead_deal_rate"],
                "visit_deal_rate": metrics["visit_deal_rate"],
            },
            "成本": {
                "spend": metrics["spend"],
                "cpl": metrics["cpl"],
                "cps": metrics["cps"],
            },
            "EX7": {
                "ex7_leads": metrics["ex7_leads"],
                "ex7_deals": metrics["ex7_deals"],
                "ex7_deal_rate": metrics["ex7_deal_rate"],
            },
        }

    @classmethod
    def _merge_source_entity_supplements(
        cls,
        source_entities: list[dict[str, Any]],
        supplemental_entities: list[dict[str, Any]],
        *,
        scope_type: str,
    ) -> list[dict[str, Any]]:
        supplemental_by_key = {
            cls._source_entity_supplement_key(item, scope_type=scope_type): item
            for item in supplemental_entities
            if cls._source_entity_supplement_key(item, scope_type=scope_type)
        }
        merged: list[dict[str, Any]] = []
        for entity in source_entities:
            key = cls._source_entity_supplement_key(entity, scope_type=scope_type)
            supplement = supplemental_by_key.get(key)
            if not supplement and scope_type == "account":
                supplement = cls._line_summary_supplement(entity, supplemental_entities)
            if not supplement:
                merged.append(entity)
                continue
            metrics = dict(entity.get("metrics", {}))
            supplement_metrics = supplement.get("metrics", {})
            for metric_key in SOURCE_ENTITY_SUPPLEMENT_METRICS:
                supplement_metric = supplement_metrics.get(metric_key)
                if cls._metric_has_actual(supplement_metric):
                    metrics[metric_key] = supplement_metric
            daily_trends = dict(entity.get("daily_trends", {}))
            for metric_key in SOURCE_ENTITY_SUPPLEMENT_METRICS:
                if metric_key in supplement.get("daily_trends", {}):
                    daily_trends[metric_key] = supplement["daily_trends"][metric_key]
            merged.append(
                {
                    **entity,
                    "metrics": metrics,
                    "metric_groups": cls._entity_metric_groups(metrics),
                    "daily_trends": daily_trends,
                }
            )
        return merged

    @staticmethod
    def _source_entity_supplement_key(entity: dict[str, Any], *, scope_type: str) -> str:
        name = str(entity.get("name") or "").strip()
        if not name:
            return ""
        if scope_type == "account":
            return normalize_account(ACCOUNT_LABEL_MAP.get(name, name))
        return normalize_text(name)

    @classmethod
    def _line_summary_supplement(
        cls,
        source_entity: dict[str, Any],
        supplemental_entities: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if cls._source_entity_supplement_key(source_entity, scope_type="account") != normalize_account("线索组汇总"):
            return None
        visits = cls._sum_metric_actuals(supplemental_entities, "visits")
        ex7_leads = cls._sum_metric_actuals(supplemental_entities, "ex7_leads")
        ex7_deals = cls._sum_metric_actuals(supplemental_entities, "ex7_deals")
        if visits is None and ex7_leads is None and ex7_deals is None:
            return None
        metrics = source_entity.get("metrics", {})
        leads = cls._json_number(metrics.get("leads", {}).get("actual"))
        deals = cls._json_number(metrics.get("deals", {}).get("actual"))
        return {
            "metrics": {
                "visits": cls._metric_summary("visits", "到店数", visits, None, "条"),
                "visit_rate": cls._metric_summary("visit_rate", "到店率", cls._safe_div_value(visits, leads), None, "比例"),
                "visit_deal_rate": cls._metric_summary("visit_deal_rate", "到店成交率", cls._safe_div_value(deals, visits), None, "比例"),
                "ex7_leads": cls._metric_summary("ex7_leads", "EX7 线索数", ex7_leads, None, "条"),
                "ex7_deals": cls._metric_summary("ex7_deals", "EX7 成交数", ex7_deals, None, "台"),
                "ex7_deal_rate": cls._metric_summary("ex7_deal_rate", "EX7 成交率", cls._safe_div_value(ex7_deals, ex7_leads), None, "比例"),
            }
        }

    @classmethod
    def _sum_metric_actuals(cls, entities: list[dict[str, Any]], metric_key: str) -> float | None:
        values = [
            cls._json_number(item.get("metrics", {}).get(metric_key, {}).get("actual"))
            for item in entities
        ]
        present = [value for value in values if value is not None]
        return float(sum(present)) if present else None

    @staticmethod
    def _metric_has_actual(metric: Any) -> bool:
        return isinstance(metric, dict) and DashboardDailyService._json_number(metric.get("actual")) is not None

    @staticmethod
    def _merge_monthly_comparison(
        source_monthly: list[dict[str, Any]],
        supplemental_monthly: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_month: dict[str, dict[str, Any]] = {}
        for item in supplemental_monthly:
            month = str(item.get("month") or "")
            if month:
                by_month[month] = item
        for item in source_monthly:
            month = str(item.get("month") or "")
            if month:
                by_month[month] = item
        return [by_month[month] for month in sorted(by_month)]

    @classmethod
    def _source_entity_metrics(cls, series_by_key: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        leads = cls._summary_from_series(series_by_key.get("mtd_unique_leads", {}), "leads", "线索")
        unique_leads = {**leads, "key": "unique_leads", "label": "唯一线索"}
        douyin_laike_orders = cls._summary_from_series(
            series_by_key.get("mtd_douyin_laike_orders", {}),
            "douyin_laike_orders",
            "来客订单",
        )
        deals = cls._summary_from_series(series_by_key.get("mtd_deals", {}), "deals", "实销")
        spend = cls._summary_from_series(series_by_key.get("mtd_spend", {}), "spend", "费用")
        cpl = cls._summary_from_series(series_by_key.get("mtd_cpl", {}), "cpl", "CPL")
        cps = cls._summary_from_series(series_by_key.get("mtd_cps", {}), "cps", "CPS")
        lead_deal_rate = cls._metric_summary(
            "lead_deal_rate",
            "线索成交率",
            cls._safe_div_value(deals.get("actual"), leads.get("actual")),
            None,
            "比例",
        )
        not_connected = {
            "visits": cls._metric_summary("visits", "到店", None, None, "次", source_status="not_connected"),
            "visit_rate": cls._metric_summary("visit_rate", "到店率", None, None, "比例", source_status="not_connected"),
            "visit_deal_rate": cls._metric_summary("visit_deal_rate", "到店成交率", None, None, "比例", source_status="not_connected"),
            "ex7_leads": cls._metric_summary("ex7_leads", "EX7 线索", None, None, "条", source_status="not_connected"),
            "ex7_deals": cls._metric_summary("ex7_deals", "EX7 实销", None, None, "台", source_status="not_connected"),
            "ex7_deal_rate": cls._metric_summary("ex7_deal_rate", "EX7 成交率", None, None, "比例", source_status="not_connected"),
        }
        return {
            "leads": leads,
            "unique_leads": unique_leads,
            "douyin_laike_orders": douyin_laike_orders,
            "deals": deals,
            "spend": spend,
            "cpl": cpl,
            "cps": cps,
            "lead_deal_rate": lead_deal_rate,
            **not_connected,
        }

    @classmethod
    def _source_entity_daily_trends(cls, series_by_key: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        return {
            "leads": cls._daily_points_from_series(series_by_key.get("mtd_unique_leads", {})),
            "unique_leads": cls._daily_points_from_series(series_by_key.get("mtd_unique_leads", {})),
            "douyin_laike_orders": cls._daily_points_from_series(series_by_key.get("mtd_douyin_laike_orders", {})),
            "deals": cls._daily_points_from_series(series_by_key.get("mtd_deals", {})),
            "spend": cls._daily_points_from_series(series_by_key.get("mtd_spend", {})),
            "cpl": cls._daily_points_from_series(series_by_key.get("mtd_cpl", {})),
            "cps": cls._daily_points_from_series(series_by_key.get("mtd_cps", {})),
        }

    @classmethod
    def _seed_exposure_from_dashboard_sources(
        cls,
        seed_account: dict[str, Any],
        seed_anchors: list[dict[str, Any]],
    ) -> dict[str, Any]:
        account_items = []
        if cls._trend_entity_has_data(seed_account):
            account_items.append(cls._seed_item_from_entity(seed_account, display_type="账号曝光"))
        anchor_items = [
            cls._seed_item_from_entity(item, display_type="主播曝光")
            for item in seed_anchors
            if cls._trend_entity_has_data(item)
        ]
        return {
            "summary": {"accounts": account_items, "anchors": anchor_items},
            "daily_trends": {
                "accounts": [
                    {"name": item["name"], "points": item["daily_trends"]["impressions"]} for item in account_items
                ],
                "anchors": [
                    {"name": item["name"], "points": item["daily_trends"]["impressions"]} for item in anchor_items
                ],
            },
        }

    @classmethod
    def _seed_item_from_entity(cls, entity: dict[str, Any], *, display_type: str) -> dict[str, Any]:
        metrics = entity.get("metrics", {})
        source_series = metrics.get("mtd_impressions", {}) if cls._series_has_data(metrics.get("mtd_impressions", {})) else metrics.get("daily_impressions", {})
        impressions = cls._summary_from_series(source_series, "impressions", "曝光")
        return {
            "name": entity.get("name", ""),
            "display_type": display_type,
            "parent_scope": entity.get("parent_scope", ""),
            "metrics": {
                "impressions": impressions,
                "lead_conversion_rate": cls._metric_summary(
                    "lead_conversion_rate",
                    "曝光到线索转化率",
                    None,
                    None,
                    "比例",
                    source_status="not_connected",
                ),
            },
            "daily_trends": {"impressions": cls._daily_points_from_series(source_series)},
        }

    @classmethod
    def _summary_from_series(cls, series: dict[str, Any], key: str, label: str) -> dict[str, Any]:
        point = cls._latest_available_point(series)
        unit = str(series.get("unit") or (point or {}).get("unit") or "")
        if point is None:
            return cls._metric_summary(key, label, None, None, unit, source_status="not_connected")
        actual = cls._json_number(point.get("actual"))
        target = cls._json_number(point.get("target"))
        attain_rate = cls._json_number(point.get("attain_rate"))
        if attain_rate is None and key in {"cpl", "cps"}:
            attain_rate = cls._json_number(cls._safe_div_value(target, actual))
        return {
            "key": key,
            "label": label,
            "actual": actual,
            "target": target,
            "attain_rate": attain_rate,
            "unit": unit,
            "source_status": "available",
        }

    @classmethod
    def _daily_trend_from_series(cls, series: dict[str, Any], key: str, label: str) -> dict[str, Any]:
        return {
            "key": key,
            "label": label,
            "unit": str(series.get("unit") or ""),
            "points": cls._daily_points_from_series(series),
        }

    @classmethod
    def _daily_points_from_series(cls, series: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "date": str(point.get("report_date") or ""),
                "value": None if point.get("is_missing") else cls._json_number(point.get("actual")),
            }
            for point in series.get("points", [])
        ]

    @staticmethod
    def _latest_available_point(series: dict[str, Any]) -> dict[str, Any] | None:
        for point in reversed(series.get("points", [])):
            if not point.get("is_missing"):
                return point
        return None

    @classmethod
    def _trend_entity_has_data(cls, entity: dict[str, Any]) -> bool:
        return any(cls._series_has_data(series) for series in entity.get("metrics", {}).values())

    @staticmethod
    def _series_has_data(series: dict[str, Any]) -> bool:
        return any(not point.get("is_missing") for point in series.get("points", []))

    @staticmethod
    def _dashboard_source_status() -> list[dict[str, Any]]:
        return [
            {
                "metric": "曝光/线索/实销/费用/CPL/CPS/账号/主播",
                "status": "available",
                "source": "output/sql_reports/feishu_dashboard_source_latest_*.tsv",
            },
            {
                "metric": "到店/EX7账号细分/曝光到线索转化率",
                "status": "not_connected",
                "source": "dashboard source 当前未提供这些派生字段",
            },
        ]

    def _build_business_trend_payload(self, window: dict[str, Any]) -> dict[str, Any]:
        fact = self._load_dashboard_fact()
        raw_leads = self._load_raw_source_table("leads")
        raw_deals = self._load_raw_source_table("deals")
        live_sessions = self._load_live_sessions()
        live_source = self._load_live_source_table()
        seed_sessions = self._load_seed_sessions()
        targets = self._load_targets()
        seed_targets = self._load_seed_targets()

        if fact.empty and raw_deals.empty and live_sessions.empty and seed_sessions.empty:
            return {}

        fact = self._prepare_fact_for_trends(fact, raw_leads=raw_leads, raw_deals=raw_deals)
        current = self._business_window_context(
            fact=fact,
            raw_deals=raw_deals,
            live_sessions=live_sessions,
            seed_sessions=seed_sessions,
            start_date=window["start_date"],
            end_date=window["end_date"],
        )

        core = self._core_business_summary(
            current["date_strings"],
            lead_rows=current["lead_rows"],
            deal_rows=current["deal_rows"],
            live_range=current["core_live_range"],
            window=window,
            target_config=self._load_topline_targets(),
            douyin_laike_order_daily=self._douyin_laike_order_mtd_points(
                live_source,
                raw_leads,
                window,
            ),
        )
        accounts = self._entity_summaries(
            "account",
            current["date_strings"],
            lead_rows=current["lead_rows"],
            visit_rows=current["visit_rows"],
            deal_rows=current["deal_rows"],
            spend_rows=current["live_range"],
            targets=targets,
            window=window,
        )
        anchors = self._entity_summaries(
            "anchor",
            current["date_strings"],
            lead_rows=current["lead_rows"],
            visit_rows=current["visit_rows"],
            deal_rows=current["deal_rows"],
            spend_rows=current["live_range"],
            targets=targets,
            window=window,
        )
        seed_exposure = self._seed_exposure_summary(
            current["date_strings"],
            seed_range=current["seed_range"],
            seed_targets=seed_targets,
            lead_rows=current["lead_rows"],
            window=window,
        )
        model_segments = self._model_segment_summary(
            current["date_strings"],
            lead_rows=current["lead_rows"],
            deal_rows=current["deal_rows"],
            live_range=current["core_live_range"],
        )
        previous_window = self._previous_trend_window(window)
        previous = self._business_window_context(
            fact=fact,
            raw_deals=raw_deals,
            live_sessions=live_sessions,
            seed_sessions=seed_sessions,
            start_date=previous_window["start_date"],
            end_date=previous_window["end_date"],
        )
        previous_core = self._core_business_summary(
            previous["date_strings"],
            lead_rows=previous["lead_rows"],
            deal_rows=previous["deal_rows"],
            live_range=previous["core_live_range"],
            window=previous_window,
            target_config=self._load_topline_targets(),
            douyin_laike_order_daily=self._douyin_laike_order_mtd_points(
                live_source,
                raw_leads,
                previous_window,
            ),
        )
        previous_has_data = self._trend_payloads_have_data(previous_core["daily_trends"])
        previous_period = {
            **previous_window,
            "has_data": previous_has_data,
            "message": "" if previous_has_data else "上一周期数据不足",
        }
        return {
            "daily_trends": core["daily_trends"],
            "core_kpi_summary": core["summary"],
            "previous_period": previous_period,
            "previous_period_trends": previous_core["daily_trends"] if previous_has_data else [],
            "monthly_comparison": self._monthly_comparison(core["daily_trends"], window),
            "model_segment_summary": model_segments,
            "account_summary": accounts,
            "account_daily_trends": [
                {"name": item["name"], "parent_scope": item.get("parent_scope", ""), "daily_trends": item["daily_trends"]}
                for item in accounts
            ],
            "anchor_summary": anchors,
            "anchor_daily_trends": [
                {"name": item["name"], "parent_scope": item.get("parent_scope", ""), "daily_trends": item["daily_trends"]}
                for item in anchors
            ],
            "seed_exposure_summary": seed_exposure["summary"],
            "seed_exposure_daily_trends": seed_exposure["daily_trends"],
            "metric_source_status": self._metric_source_status(fact, raw_leads, raw_deals, live_sessions, seed_sessions),
        }

    def _load_dashboard_fact(self) -> pd.DataFrame:
        fact_path = self.repo_root / "output" / "fact_attribution.csv"
        if not fact_path.exists():
            return pd.DataFrame()
        override_path = self.repo_root / "config" / "manual_attribution_overrides.csv"
        try:
            return load_fact(fact_path, manual_override_path=override_path if override_path.exists() else None)
        except Exception:
            return pd.DataFrame()

    def _load_raw_source_table(self, source_kind: str) -> pd.DataFrame:
        if source_kind == "leads":
            patterns = ("总部新媒体线索*.csv", "总部新媒体线索*.xlsx", "总部新媒体线索*.xls")
            preferred = ["总部新媒体线索", "线索", "Sheet1"]
        else:
            patterns = ("总部新媒体成交*.csv", "总部新媒体成交*.xlsx", "总部新媒体成交*.xls")
            preferred = ["成交", "Sheet1"]

        paths: set[Path] = set()
        for root in (self.repo_root / "历史文件", self.repo_root / "源文件"):
            if not root.exists():
                continue
            for pattern in patterns:
                paths.update(path.resolve() for path in root.rglob(pattern) if path.is_file())

        frames: list[pd.DataFrame] = []
        for path in sorted(paths):
            try:
                frame = read_table_auto(path, preferred_sheets=preferred)
            except Exception:
                continue
            frame = frame.copy()
            frame.columns = [str(column).strip() for column in frame.columns]
            frame["_source_file"] = path.name
            frames.append(frame)
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

    def _load_live_sessions(self) -> pd.DataFrame:
        paths: set[Path] = set()
        for root in (self.repo_root / "历史文件", self.repo_root / "源文件"):
            if not root.exists():
                continue
            paths.update(path.resolve() for path in root.rglob("*直播进度表*.xlsx") if path.is_file())
            paths.update(path.resolve() for path in root.rglob("*直播进度表*.xls") if path.is_file())
        return self._load_session_workbooks(paths, source_kind="live_progress")

    def _load_live_source_table(self) -> pd.DataFrame:
        paths: set[Path] = set()
        for root in (self.repo_root / "历史文件", self.repo_root / "源文件"):
            if not root.exists():
                continue
            paths.update(path.resolve() for path in root.rglob("*直播进度表*.xlsx") if path.is_file())
            paths.update(path.resolve() for path in root.rglob("*直播进度表*.xls") if path.is_file())

        frames: list[pd.DataFrame] = []
        for path in sorted(paths):
            try:
                workbook = pd.ExcelFile(path)
                raw = pd.read_excel(path, sheet_name=workbook.sheet_names[0])
            except Exception:
                continue
            raw = raw.copy()
            raw.columns = [str(column).strip() for column in raw.columns]
            raw["_source_file"] = path.name
            frames.append(raw)
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

    def _load_seed_sessions(self) -> pd.DataFrame:
        paths: set[Path] = set()
        for root in (self.repo_root / "历史文件", self.repo_root / "源文件"):
            if not root.exists():
                continue
            paths.update(path.resolve() for path in root.rglob("*EXEED星途台账*.xlsx") if path.is_file())
            paths.update(path.resolve() for path in root.rglob("*EXEED星途台账*.xls") if path.is_file())
        return self._load_session_workbooks(paths, source_kind="seed_ledger")

    @staticmethod
    def _load_session_workbooks(paths: set[Path], *, source_kind: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for path in sorted(paths):
            try:
                workbook = pd.ExcelFile(path)
                raw = pd.read_excel(path, sheet_name=workbook.sheet_names[0])
            except Exception:
                continue
            raw = raw.copy()
            raw.columns = [str(column).strip() for column in raw.columns]
            date_col = pick_live_column(raw, ["日期", "直播日期", "创建时间"], required=False)
            account_col = pick_live_column(raw, ["开播账号", "账号", "直播账号", "账号名称"], required=False)
            host_col = pick_live_column(raw, ["本场主播", "主播", "主播名称"], required=False)
            model_col = pick_live_column(raw, ["车型"], required=False)
            spend_col = pick_live_column(raw, ["消耗", "实际消耗", "当日消耗", "花费", "费用", "投放消耗", "总消耗"], required=False)
            exposure_col = pick_live_column(raw, ["曝光人数", "曝光次数", "曝光", "展现", "曝光量"], required=False)
            lead_col = pick_live_column(raw, ["直播全场景商机量", "全场景线索人数", "唯一线索"], required=False)
            if not date_col:
                continue
            frame = pd.DataFrame(
                {
                    "date": pd.to_datetime(raw[date_col], errors="coerce").dt.normalize(),
                    "account": raw[account_col].map(normalize_account) if account_col else "",
                    "hosts_raw": raw[host_col].map(normalize_text) if host_col else "",
                    "model": raw[model_col].map(normalize_text) if model_col else "",
                    "spend": pd.to_numeric(raw[spend_col], errors="coerce") if spend_col else np.nan,
                    "impressions": pd.to_numeric(raw[exposure_col], errors="coerce") if exposure_col else np.nan,
                    "source_leads": pd.to_numeric(raw[lead_col], errors="coerce") if lead_col else np.nan,
                    "source_kind": source_kind,
                    "source_file": path.name,
                }
            )
            frame = frame[frame["date"].notna()].copy()
            if frame.empty:
                continue
            frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=["date", "account", "hosts_raw", "model", "spend", "impressions", "source_leads", "source_kind", "source_file"])
        out = pd.concat(frames, ignore_index=True, sort=False)
        out["spend"] = pd.to_numeric(out["spend"], errors="coerce")
        out["impressions"] = pd.to_numeric(out["impressions"], errors="coerce")
        out["source_leads"] = pd.to_numeric(out["source_leads"], errors="coerce")
        return out

    def _load_targets(self) -> pd.DataFrame:
        path = self.repo_root / "config" / "monthly_targets.csv"
        if not path.exists():
            return pd.DataFrame()
        try:
            out = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
        out.columns = [str(column).strip() for column in out.columns]
        if "month" not in out.columns or "scope_type" not in out.columns or "scope_name" not in out.columns:
            return pd.DataFrame()
        out["month"] = out["month"].astype(str).str.strip()
        out["scope_type"] = out["scope_type"].astype(str).str.strip()
        out["scope_name"] = out["scope_name"].astype(str).str.strip()
        for column in ["lead_target_month", "deal_target_month", "lead_cost_target_month", "cpl_target", "cps_target"]:
            if column not in out.columns:
                out[column] = np.nan
            out[column] = pd.to_numeric(out[column], errors="coerce")
        return out

    def _load_seed_targets(self) -> pd.DataFrame:
        path = self.repo_root / "config" / "seed_monthly_targets.csv"
        if not path.exists():
            return pd.DataFrame()
        try:
            out = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            return pd.DataFrame()
        out.columns = [str(column).strip() for column in out.columns]
        if "month" not in out.columns or "scope_type" not in out.columns or "scope_name" not in out.columns:
            return pd.DataFrame()
        out["month"] = out["month"].astype(str).str.strip()
        out["scope_type"] = out["scope_type"].astype(str).str.strip()
        out["scope_name"] = out["scope_name"].astype(str).str.strip()
        if "parent_account" not in out.columns:
            out["parent_account"] = ""
        out["parent_account"] = out["parent_account"].astype(str).str.strip()
        if "impression_target_month" not in out.columns:
            out["impression_target_month"] = np.nan
        out["impression_target_month"] = pd.to_numeric(out["impression_target_month"], errors="coerce")
        return out

    def _prepare_fact_for_trends(self, fact: pd.DataFrame, *, raw_leads: pd.DataFrame, raw_deals: pd.DataFrame) -> pd.DataFrame:
        if fact.empty:
            return fact
        out = fact.copy()
        if "deal_date" not in out.columns:
            out["deal_date"] = pd.to_datetime(out.get("成交时间"), errors="coerce").dt.normalize()
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
        out["deal_date"] = pd.to_datetime(out["deal_date"], errors="coerce").dt.normalize()
        out["标准账号"] = out.get("标准账号", "").map(normalize_account) if "标准账号" in out.columns else ""
        out["本场主播"] = out.get("本场主播", "").map(normalize_text) if "本场主播" in out.columns else ""
        if "_perf_lead_key" not in out.columns:
            out["_perf_lead_key"] = out.get("线索ID_norm", out.get("线索ID", pd.Series("", index=out.index))).astype(str).str.strip()
        else:
            out["_perf_lead_key"] = out["_perf_lead_key"].astype(str).str.strip()
        if "is_perf_primary_lead" not in out.columns:
            if "is_perf_lead_scope" in out.columns:
                out["is_perf_primary_lead"] = pd.to_numeric(out["is_perf_lead_scope"], errors="coerce").fillna(0)
            else:
                out["is_perf_primary_lead"] = out["_perf_lead_key"].ne("").astype(int)
        out["visit_date"] = self._fact_visit_dates(out, raw_leads)
        try:
            config_path = self.repo_root / "config" / "report_topline_config.json"
            config = load_topline_config(config_path) if config_path.exists() and not raw_leads.empty and not raw_deals.empty else {}
            if config:
                out = annotate_fact_with_ex7_partition(out, raw_leads, raw_deals, config)
            else:
                out["is_ex7_partition"] = self._contains_ex7(out.get("成交车型", pd.Series("", index=out.index)))
        except Exception:
            out["is_ex7_partition"] = self._contains_ex7(out.get("成交车型", pd.Series("", index=out.index)))
        out["is_ex7_partition"] = out["is_ex7_partition"].fillna(False).astype(bool)
        return out

    @staticmethod
    def _fact_visit_dates(fact: pd.DataFrame, raw_leads: pd.DataFrame) -> pd.Series:
        direct = pd.Series(pd.NaT, index=fact.index, dtype="datetime64[ns]")
        for column in ("到店日期", "到店时间"):
            if column in fact.columns:
                direct = direct.where(direct.notna(), pd.to_datetime(fact[column], errors="coerce").dt.normalize())
        if raw_leads.empty or "线索ID" not in raw_leads.columns:
            return direct
        source = raw_leads.copy()
        source.columns = [str(column).strip() for column in source.columns]
        visit = pd.Series(pd.NaT, index=source.index, dtype="datetime64[ns]")
        for column in ("到店日期", "到店时间"):
            if column in source.columns:
                visit = visit.where(visit.notna(), pd.to_datetime(source[column], errors="coerce").dt.normalize())
        source["线索ID_norm"] = source["线索ID"].astype(str).str.strip()
        source["visit_date"] = visit
        lookup = (
            source[source["线索ID_norm"].ne("") & source["visit_date"].notna()]
            .sort_values("visit_date")
            .drop_duplicates(subset=["线索ID_norm"], keep="first")
            .set_index("线索ID_norm")["visit_date"]
            .to_dict()
        )
        fact_ids = fact.get("线索ID_norm", fact.get("线索ID", pd.Series("", index=fact.index))).astype(str).str.strip()
        mapped = fact_ids.map(lookup)
        return direct.where(direct.notna(), pd.to_datetime(mapped, errors="coerce").dt.normalize())

    @classmethod
    def _deal_rows_for_window(
        cls,
        fact: pd.DataFrame,
        raw_deals: pd.DataFrame,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        raw_rows = cls._raw_deal_rows_for_trends(raw_deals, fact)
        if not raw_rows.empty:
            frames.append(raw_rows)
        if not fact.empty and "deal_date" in fact.columns:
            fact_deals = fact[
                pd.to_numeric(fact.get("is_deal", 0), errors="coerce").fillna(0).eq(1)
                & fact["deal_date"].notna()
            ].copy()
            if not fact_deals.empty:
                fact_deals["_deal_source_priority"] = 1
                frames.append(fact_deals)
        if not frames:
            return cls._empty_deal_rows()
        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined["deal_date"] = pd.to_datetime(combined["deal_date"], errors="coerce").dt.normalize()
        combined["线索ID_norm"] = combined.get("线索ID_norm", pd.Series("", index=combined.index)).astype(str).str.strip()
        combined = combined[combined["deal_date"].notna() & combined["线索ID_norm"].ne("")]
        if combined.empty:
            return cls._empty_deal_rows()
        combined["_deal_source_priority"] = pd.to_numeric(combined.get("_deal_source_priority", 1), errors="coerce").fillna(1)
        combined = combined.sort_values(["_deal_source_priority", "deal_date"]).drop_duplicates(subset=["线索ID_norm"], keep="first")
        scoped = combined[combined["deal_date"].between(start_ts, end_ts)].copy()
        return scoped if not scoped.empty else cls._empty_deal_rows()

    @staticmethod
    def _empty_deal_rows() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "deal_date",
                "线索ID_norm",
                "_perf_lead_key",
                "is_deal",
                "_deal_source_priority",
                "成交车型",
                "标准账号",
                "本场主播",
                "is_ex7_partition",
            ]
        )

    @classmethod
    def _raw_deal_rows_for_trends(cls, raw_deals: pd.DataFrame, fact: pd.DataFrame) -> pd.DataFrame:
        if raw_deals.empty or "线索ID" not in raw_deals.columns:
            return cls._empty_deal_rows()
        source = raw_deals.copy()
        source.columns = [str(column).strip() for column in source.columns]
        deal_date = pd.Series(pd.NaT, index=source.index, dtype="datetime64[ns]")
        for column in ("成交日期", "成交时间"):
            if column in source.columns:
                deal_date = deal_date.where(deal_date.notna(), pd.to_datetime(source[column], errors="coerce").dt.normalize())
        lead_ids = source["线索ID"].astype(str).str.strip()
        out = pd.DataFrame(
            {
                "_raw_row_index": source.index,
                "线索ID_norm": lead_ids,
                "_perf_lead_key": lead_ids,
                "deal_date": deal_date,
                "is_deal": 1,
                "_deal_source_priority": 0,
                "成交车型": cls._first_text_series(source, ["成交车型", "下订车型", "车系"]),
            }
        )
        out = out[out["线索ID_norm"].ne("") & out["deal_date"].notna()].copy()
        if out.empty:
            return cls._empty_deal_rows()

        fact_lookup = cls._fact_lookup_for_raw_deals(fact)
        if fact_lookup.empty:
            out["标准账号"] = cls._raw_deal_account_series(source.loc[out["_raw_row_index"]])
            out["本场主播"] = ""
        else:
            out = out.merge(fact_lookup, how="left", on="线索ID_norm")
            fallback_account = cls._raw_deal_account_series(source.loc[out["_raw_row_index"]])
            out["标准账号"] = out["标准账号"].where(out["标准账号"].astype(str).str.strip().ne(""), fallback_account.values)
            out["本场主播"] = out["本场主播"].fillna("").map(normalize_text)
        out["标准账号"] = out["标准账号"].fillna("").map(normalize_account)
        out["本场主播"] = out["本场主播"].fillna("").map(normalize_text)
        out["is_ex7_partition"] = cls._contains_ex7(out["成交车型"])
        return out

    @staticmethod
    def _first_text_series(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
        out = pd.Series("", index=frame.index, dtype=object)
        for column in columns:
            if column not in frame.columns:
                continue
            candidate = frame[column].fillna("").astype(str).str.strip()
            out = out.where(out.astype(str).str.strip().ne(""), candidate)
        return out

    @classmethod
    def _raw_deal_account_series(cls, frame: pd.DataFrame) -> pd.Series:
        return cls._first_text_series(frame, ["渠道2", "渠道1", "责任部门"]).map(normalize_account)

    @staticmethod
    def _fact_lookup_for_raw_deals(fact: pd.DataFrame) -> pd.DataFrame:
        if fact.empty:
            return pd.DataFrame()
        ids = fact.get("线索ID_norm", fact.get("线索ID", pd.Series("", index=fact.index))).astype(str).str.strip()
        lookup = pd.DataFrame(
            {
                "线索ID_norm": ids,
                "标准账号": fact.get("标准账号", pd.Series("", index=fact.index)).fillna("").map(normalize_account),
                "本场主播": fact.get("本场主播", pd.Series("", index=fact.index)).fillna("").map(normalize_text),
            }
        )
        lookup = lookup[lookup["线索ID_norm"].ne("")]
        if lookup.empty:
            return lookup
        return lookup.drop_duplicates(subset=["线索ID_norm"], keep="first")

    @staticmethod
    def _contains_ex7(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.upper().str.contains("EX7", na=False)

    @staticmethod
    def _filter_frame_dates(frame: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        if frame.empty or "date" not in frame.columns:
            return frame.copy()
        data = frame.copy()
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
        return data[data["date"].between(start_ts, end_ts)].copy()

    @classmethod
    def _douyin_laike_order_mtd_points(
        cls,
        live_source: pd.DataFrame,
        raw_leads: pd.DataFrame,
        window: dict[str, Any],
    ) -> dict[str, float]:
        if live_source.empty or raw_leads.empty:
            return {}
        lead_time_col = pick_col(raw_leads, [*COLUMN_ALIASES["lead_time"], "创建日期"], required=False)
        if not lead_time_col:
            return {}
        lead_time = pd.to_datetime(raw_leads[lead_time_col], errors="coerce")
        date_strings = cls._calendar_date_strings(window["start_date"], window["end_date"])
        if not date_strings:
            return {}
        end_date = date.fromisoformat(window["end_date"])
        months = sorted({date_key[:7] for date_key in date_strings})
        out: dict[str, float] = {}
        for month in months:
            year, month_number = (int(part) for part in month.split("-"))
            month_end = date(year, month_number, calendar.monthrange(year, month_number)[1])
            report_date = min(month_end, end_date)
            report_date_key = report_date.isoformat()
            if report_date_key not in date_strings:
                continue
            month_start = pd.Timestamp(report_date).to_period("M").to_timestamp().normalize()
            month_leads = raw_leads[
                lead_time.notna()
                & lead_time.dt.normalize().between(month_start, pd.Timestamp(report_date).normalize())
            ].copy()
            total_orders, _, _ = build_douyin_laike_order_metrics(
                live_source,
                month_leads,
                pd.Timestamp(report_date),
            )
            out[report_date_key] = float(total_orders)
        return out

    @classmethod
    def _core_business_summary(
        cls,
        date_strings: list[str],
        *,
        lead_rows: pd.DataFrame,
        deal_rows: pd.DataFrame,
        live_range: pd.DataFrame,
        window: dict[str, Any],
        target_config: dict[str, float],
        douyin_laike_order_daily: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        douyin_laike_order_daily = douyin_laike_order_daily or {}
        lead_daily = cls._daily_count(lead_rows, "date", "_perf_lead_key")
        deal_daily = cls._daily_count(deal_rows, "deal_date", "线索ID_norm")
        spend_daily = cls._session_daily(live_range, "spend")
        impression_daily = cls._session_daily(live_range, "impressions")
        cpl_daily = cls._ratio_maps(spend_daily, lead_daily)
        cps_daily = cls._ratio_maps(spend_daily, deal_daily)
        trend_specs = [
            ("impressions", "曝光", "人次", impression_daily),
            ("leads", "线索", "条", lead_daily),
            ("douyin_laike_orders", "来客订单", "个", douyin_laike_order_daily),
            ("deals", "实销", "台", deal_daily),
            ("spend", "费用", "元", spend_daily),
            ("cpl", "CPL", "元/条", cpl_daily),
            ("cps", "CPS", "元/台", cps_daily),
        ]
        daily_trends = [cls._trend_payload(key, label, unit, values, date_strings) for key, label, unit, values in trend_specs]
        totals = {
            "impressions": cls._sum_present(impression_daily),
            "leads": cls._sum_present(lead_daily),
            "douyin_laike_orders": cls._sum_present(douyin_laike_order_daily),
            "deals": cls._sum_present(deal_daily),
            "spend": cls._sum_present(spend_daily),
        }
        totals["cpl"] = cls._safe_div_value(totals["spend"], totals["leads"])
        totals["cps"] = cls._safe_div_value(totals["spend"], totals["deals"])
        summary = [
            cls._metric_summary("impressions", "曝光", totals["impressions"], cls._range_target(target_config.get("impressions"), window), "人次"),
            cls._metric_summary("leads", "线索", totals["leads"], cls._range_target(target_config.get("leads"), window), "条"),
            cls._metric_summary("douyin_laike_orders", "来客订单", totals["douyin_laike_orders"], None, "个"),
            cls._metric_summary("deals", "实销", totals["deals"], cls._range_target(target_config.get("deals"), window), "台"),
            cls._metric_summary("spend", "费用", totals["spend"], None, "元"),
            cls._metric_summary("cpl", "CPL", totals["cpl"], target_config.get("cpl"), "元/条"),
            cls._metric_summary("cps", "CPS", totals["cps"], target_config.get("cps"), "元/台"),
        ]
        for item, trend in zip(summary, daily_trends):
            item["trend"] = trend["points"]
        return {"summary": summary, "daily_trends": daily_trends}

    @staticmethod
    def _trend_payloads_have_data(daily_trends: list[dict[str, Any]]) -> bool:
        return any(
            point.get("value") is not None
            for trend in daily_trends
            for point in trend.get("points", [])
        )

    @classmethod
    def _monthly_comparison(
        cls,
        daily_trends: list[dict[str, Any]],
        window: dict[str, Any],
        *,
        aggregation: str = "sum",
    ) -> list[dict[str, Any]]:
        if int(window["days"]) <= 31:
            return []
        trend_by_key = {str(trend.get("key", "")): trend for trend in daily_trends}
        months = sorted(
            {
                str(point.get("date", ""))[:7]
                for trend in daily_trends
                for point in trend.get("points", [])
                if point.get("value") is not None and str(point.get("date", ""))[:7]
            }
        )

        def monthly_value(key: str, month: str) -> float | None:
            values: list[tuple[str, float]] = []
            for point in trend_by_key.get(key, {}).get("points", []):
                point_date = str(point.get("date", ""))
                if point_date[:7] != month:
                    continue
                value = point.get("value")
                if value is None:
                    continue
                parsed = cls._json_number(value)
                if parsed is not None:
                    values.append((point_date, parsed))
            if not values:
                return None
            if aggregation == "latest":
                return sorted(values, key=lambda item: item[0])[-1][1]
            return float(sum(value for _, value in values))

        def metric(key: str, label: str, unit: str, value: float | None) -> dict[str, Any]:
            return {"key": key, "label": label, "unit": unit, "value": cls._json_number(value)}

        rows: list[dict[str, Any]] = []
        for month in months:
            impressions = monthly_value("impressions", month)
            leads = monthly_value("leads", month)
            douyin_laike_orders = monthly_value("douyin_laike_orders", month)
            deals = monthly_value("deals", month)
            spend = monthly_value("spend", month)
            cpl = cls._safe_div_value(spend, leads)
            cps = cls._safe_div_value(spend, deals)
            year, month_number = month.split("-")
            rows.append(
                {
                    "month": month,
                    "label": f"{year}年{int(month_number)}月",
                    "metrics": {
                        "impressions": metric("impressions", "曝光", "人次", impressions),
                        "leads": metric("leads", "线索", "条", leads),
                        "douyin_laike_orders": metric("douyin_laike_orders", "来客订单", "个", douyin_laike_orders),
                        "deals": metric("deals", "实销", "台", deals),
                        "spend": metric("spend", "费用", "元", spend),
                        "cpl": metric("cpl", "CPL", "元/条", cpl),
                        "cps": metric("cps", "CPS", "元/台", cps),
                    },
                }
            )
        return rows

    def _load_topline_targets(self) -> dict[str, float]:
        path = self.repo_root / "config" / "report_topline_config.json"
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        targets = payload.get("full_account_targets") if isinstance(payload, dict) else {}
        if not isinstance(targets, dict):
            return {}
        out: dict[str, float] = {}
        for key in ("impressions", "leads", "deals", "cpl", "cps"):
            try:
                out[key] = float(targets[key])
            except Exception:
                continue
        return out

    @classmethod
    def _entity_summaries(
        cls,
        scope_type: str,
        date_strings: list[str],
        *,
        lead_rows: pd.DataFrame,
        visit_rows: pd.DataFrame,
        deal_rows: pd.DataFrame,
        spend_rows: pd.DataFrame,
        targets: pd.DataFrame,
        window: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if scope_type == "account":
            lead_daily = cls._entity_daily_count(lead_rows, "date", "标准账号", "_perf_lead_key")
            visit_daily = cls._entity_daily_count(visit_rows, "visit_date", "标准账号", "_perf_lead_key")
            deal_daily = cls._entity_daily_count(deal_rows, "deal_date", "标准账号", "线索ID_norm")
            ex7_lead_daily = cls._entity_daily_count(lead_rows[lead_rows["is_ex7_partition"]], "date", "标准账号", "_perf_lead_key")
            ex7_deal_daily = cls._entity_daily_count(deal_rows[deal_rows["is_ex7_partition"]], "deal_date", "标准账号", "线索ID_norm")
            spend_daily = cls._entity_session_daily(spend_rows, scope_type="account", value_col="spend")
            parent_map: dict[str, str] = {}
        else:
            lead_daily = cls._anchor_daily_count(lead_rows, "date", "_perf_lead_key")
            visit_daily = cls._anchor_daily_count(visit_rows, "visit_date", "_perf_lead_key")
            deal_daily = cls._anchor_daily_count(deal_rows, "deal_date", "线索ID_norm")
            ex7_lead_daily = cls._anchor_daily_count(lead_rows[lead_rows["is_ex7_partition"]], "date", "_perf_lead_key")
            ex7_deal_daily = cls._anchor_daily_count(deal_rows[deal_rows["is_ex7_partition"]], "deal_date", "线索ID_norm")
            spend_daily = cls._entity_session_daily(spend_rows, scope_type="anchor", value_col="spend")
            parent_map = cls._anchor_parent_map(lead_rows, visit_rows, deal_rows, spend_rows)

        names = sorted(
            set(lead_daily)
            | set(visit_daily)
            | set(deal_daily)
            | set(ex7_lead_daily)
            | set(ex7_deal_daily)
            | set(spend_daily),
            key=lambda value: (value == "【无主线索】", value),
        )
        rows: list[dict[str, Any]] = []
        for name in names:
            if not name:
                continue
            if scope_type == "account" and normalize_account(name) in HIDDEN_ACCOUNT_SUMMARY_NAMES:
                continue
            if scope_type == "anchor" and cls._is_hidden_anchor_performance_name(name):
                continue
            leads = cls._sum_present(lead_daily.get(name, {}))
            visits = cls._sum_present(visit_daily.get(name, {}))
            deals = cls._sum_present(deal_daily.get(name, {}))
            spend = cls._sum_present(spend_daily.get(name, {}))
            ex7_leads = cls._sum_present(ex7_lead_daily.get(name, {}))
            ex7_deals = cls._sum_present(ex7_deal_daily.get(name, {}))
            lead_target = cls._target_for_entity(targets, window, scope_type, name, "lead_target_month")
            deal_target = cls._target_for_entity(targets, window, scope_type, name, "deal_target_month")
            cost_target = cls._target_for_entity(targets, window, scope_type, name, "lead_cost_target_month")
            cpl_target = cls._target_scalar_for_entity(targets, window, scope_type, name, "cpl_target")
            cps_target = cls._target_scalar_for_entity(targets, window, scope_type, name, "cps_target")
            metrics = {
                "leads": cls._metric_summary("leads", "线索数", leads, lead_target, "条"),
                "unique_leads": cls._metric_summary("unique_leads", "唯一线索数", leads, lead_target, "条"),
                "visits": cls._metric_summary("visits", "到店数", visits, None, "条"),
                "visit_rate": cls._metric_summary("visit_rate", "到店率", cls._safe_div_value(visits, leads), None, "比例"),
                "deals": cls._metric_summary("deals", "成交数", deals, deal_target, "台"),
                "lead_deal_rate": cls._metric_summary("lead_deal_rate", "线索成交率", cls._safe_div_value(deals, leads), None, "比例"),
                "visit_deal_rate": cls._metric_summary("visit_deal_rate", "到店成交率", cls._safe_div_value(deals, visits), None, "比例"),
                "spend": cls._metric_summary("spend", "费用", spend, cost_target, "元"),
                "cpl": cls._metric_summary("cpl", "CPL", cls._safe_div_value(spend, leads), cpl_target, "元/条"),
                "cps": cls._metric_summary("cps", "CPS", cls._safe_div_value(spend, deals), cps_target, "元/台"),
                "ex7_leads": cls._metric_summary("ex7_leads", "EX7 线索数", ex7_leads, None, "条"),
                "ex7_deals": cls._metric_summary("ex7_deals", "EX7 成交数", ex7_deals, None, "台"),
                "ex7_deal_rate": cls._metric_summary("ex7_deal_rate", "EX7 成交率", cls._safe_div_value(ex7_deals, ex7_leads), None, "比例"),
            }
            daily_trends = {
                "leads": cls._points_for_dates(lead_daily.get(name, {}), date_strings),
                "deals": cls._points_for_dates(deal_daily.get(name, {}), date_strings),
            }
            rows.append(
                {
                    "name": name,
                    "parent_scope": parent_map.get(name, "") if parent_map else "",
                    "metrics": metrics,
                    "metric_groups": {
                        "线索": {"leads": metrics["leads"], "unique_leads": metrics["unique_leads"]},
                        "到店": {"visits": metrics["visits"], "visit_rate": metrics["visit_rate"]},
                        "成交": {
                            "deals": metrics["deals"],
                            "lead_deal_rate": metrics["lead_deal_rate"],
                            "visit_deal_rate": metrics["visit_deal_rate"],
                        },
                        "成本": {"spend": metrics["spend"], "cpl": metrics["cpl"], "cps": metrics["cps"]},
                        "EX7": {
                            "ex7_leads": metrics["ex7_leads"],
                            "ex7_deals": metrics["ex7_deals"],
                            "ex7_deal_rate": metrics["ex7_deal_rate"],
                        },
                    },
                    "daily_trends": daily_trends,
                }
            )
        return rows

    @classmethod
    def _model_segment_summary(
        cls,
        date_strings: list[str],
        *,
        lead_rows: pd.DataFrame,
        deal_rows: pd.DataFrame,
        live_range: pd.DataFrame,
    ) -> list[dict[str, Any]]:
        segments = [(True, "EX7"), (False, "不含 EX7")]
        rows: list[dict[str, Any]] = []
        for is_ex7, label in segments:
            leads = cls._daily_count(lead_rows[lead_rows["is_ex7_partition"] == is_ex7], "date", "_perf_lead_key")
            deals = cls._daily_count(deal_rows[deal_rows["is_ex7_partition"] == is_ex7], "deal_date", "线索ID_norm")
            scoped_live = live_range[cls._contains_ex7(live_range.get("model", pd.Series("", index=live_range.index))) == is_ex7] if not live_range.empty else live_range
            spend = cls._session_daily(scoped_live, "spend")
            cpl = cls._ratio_maps(spend, leads)
            cps = cls._ratio_maps(spend, deals)
            rows.append(
                {
                    "name": label,
                    "metrics": {
                        "leads": cls._metric_summary("leads", "线索数", cls._sum_present(leads), None, "条"),
                        "deals": cls._metric_summary("deals", "实销数", cls._sum_present(deals), None, "台"),
                        "spend": cls._metric_summary("spend", "费用", cls._sum_present(spend), None, "元"),
                        "cpl": cls._metric_summary("cpl", "CPL", cls._safe_div_value(cls._sum_present(spend), cls._sum_present(leads)), None, "元/条"),
                        "cps": cls._metric_summary("cps", "CPS", cls._safe_div_value(cls._sum_present(spend), cls._sum_present(deals)), None, "元/台"),
                    },
                    "daily_trends": {
                        "leads": cls._points_for_dates(leads, date_strings),
                        "deals": cls._points_for_dates(deals, date_strings),
                        "cpl": cls._points_for_dates(cpl, date_strings),
                        "cps": cls._points_for_dates(cps, date_strings),
                    },
                }
            )
        return rows

    @classmethod
    def _seed_exposure_summary(
        cls,
        date_strings: list[str],
        *,
        seed_range: pd.DataFrame,
        seed_targets: pd.DataFrame,
        lead_rows: pd.DataFrame,
        window: dict[str, Any],
    ) -> dict[str, Any]:
        account_daily = cls._entity_session_daily(seed_range, scope_type="account", value_col="impressions")
        anchor_daily = cls._entity_session_daily(seed_range, scope_type="anchor", value_col="impressions")
        account_items: list[dict[str, Any]] = []
        for name in sorted(account_daily):
            impressions = cls._sum_present(account_daily[name])
            target = cls._seed_target_for_entity(seed_targets, window, "account", name)
            account_items.append(
                {
                    "name": name,
                    "type": "account",
                    "display_type": "账号总曝光",
                    "metrics": {
                        "impressions": cls._metric_summary("impressions", "曝光", impressions, target, "人次"),
                        "lead_conversion_rate": cls._metric_summary("lead_conversion_rate", "曝光到线索转化率", None, None, "比例", source_status="not_connected"),
                    },
                    "daily_trends": {"impressions": cls._points_for_dates(account_daily[name], date_strings)},
                }
            )
        anchor_items: list[dict[str, Any]] = []
        parent_map = cls._seed_parent_map(seed_range)
        anchor_names = set(anchor_daily) | cls._fixed_seed_anchor_names_for_window(seed_targets, window)
        for name in sorted(anchor_names):
            if cls._is_hidden_anchor_name(name):
                continue
            impressions = cls._sum_present(anchor_daily.get(name, {}))
            target = cls._seed_target_for_entity(seed_targets, window, "host", name)
            parent_scope = parent_map.get(name, "") or cls._seed_target_parent_for_entity(seed_targets, window, "host", name)
            anchor_items.append(
                {
                    "name": name,
                    "type": "anchor",
                    "display_type": "主播曝光",
                    "parent_scope": parent_scope,
                    "metrics": {
                        "impressions": cls._metric_summary("impressions", "曝光", impressions, target, "人次"),
                        "lead_conversion_rate": cls._metric_summary("lead_conversion_rate", "曝光到线索转化率", None, None, "比例", source_status="not_connected"),
                    },
                    "daily_trends": {"impressions": cls._points_for_dates(anchor_daily.get(name, {}), date_strings)},
                }
            )
        return {
            "summary": {"accounts": account_items, "anchors": anchor_items},
            "daily_trends": {
                "accounts": [{"name": item["name"], "points": item["daily_trends"]["impressions"]} for item in account_items],
                "anchors": [{"name": item["name"], "points": item["daily_trends"]["impressions"]} for item in anchor_items],
            },
        }

    @staticmethod
    def _daily_count(frame: pd.DataFrame, date_col: str, value_col: str) -> dict[str, float]:
        if frame.empty or date_col not in frame.columns or value_col not in frame.columns:
            return {}
        data = frame[[date_col, value_col]].copy()
        data[date_col] = pd.to_datetime(data[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        data[value_col] = data[value_col].astype(str).str.strip()
        data = data[data[date_col].notna() & data[value_col].ne("")]
        if data.empty:
            return {}
        return {str(key): float(value) for key, value in data.groupby(date_col)[value_col].nunique().items()}

    @staticmethod
    def _session_daily(frame: pd.DataFrame, value_col: str) -> dict[str, float]:
        if frame.empty or "date" not in frame.columns or value_col not in frame.columns:
            return {}
        data = frame[["date", value_col]].copy()
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
        data = data[data["date"].notna() & data[value_col].notna()]
        if data.empty:
            return {}
        return {str(key): float(value) for key, value in data.groupby("date")[value_col].sum().items()}

    @staticmethod
    def _entity_daily_count(frame: pd.DataFrame, date_col: str, entity_col: str, value_col: str) -> dict[str, dict[str, float]]:
        if frame.empty or date_col not in frame.columns or entity_col not in frame.columns or value_col not in frame.columns:
            return {}
        data = frame[[date_col, entity_col, value_col]].copy()
        data[date_col] = pd.to_datetime(data[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        data[entity_col] = data[entity_col].map(normalize_text)
        data[value_col] = data[value_col].astype(str).str.strip()
        data = data[data[date_col].notna() & data[entity_col].ne("") & data[value_col].ne("")]
        out: dict[str, dict[str, float]] = {}
        if data.empty:
            return out
        grouped = data.groupby([entity_col, date_col])[value_col].nunique()
        for (entity, date_key), value in grouped.items():
            out.setdefault(str(entity), {})[str(date_key)] = float(value)
        return out

    @classmethod
    def _anchor_daily_count(cls, frame: pd.DataFrame, date_col: str, value_col: str) -> dict[str, dict[str, float]]:
        if frame.empty or date_col not in frame.columns or value_col not in frame.columns or "本场主播" not in frame.columns:
            return {}
        exploded = cls._explode_hosts(frame, date_col=date_col, value_col=value_col, include_value=True)
        if exploded.empty:
            return {}
        grouped = exploded.groupby(["scope_name", "date"])["weight"].sum()
        out: dict[str, dict[str, float]] = {}
        for (name, date_key), value in grouped.items():
            out.setdefault(str(name), {})[str(date_key)] = float(value)
        return out

    @classmethod
    def _entity_session_daily(cls, frame: pd.DataFrame, *, scope_type: str, value_col: str) -> dict[str, dict[str, float]]:
        if frame.empty or value_col not in frame.columns:
            return {}
        if scope_type == "account":
            data = frame[["date", "account", value_col]].copy()
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            data["account"] = data["account"].map(normalize_account)
            data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
            data = data[data["date"].notna() & data["account"].ne("") & data[value_col].notna()]
            grouped = data.groupby(["account", "date"])[value_col].sum()
        else:
            exploded = cls._explode_session_hosts(frame, value_col=value_col)
            if exploded.empty:
                return {}
            grouped = exploded.groupby(["scope_name", "date"])["weighted_value"].sum()
        out: dict[str, dict[str, float]] = {}
        for (name, date_key), value in grouped.items():
            out.setdefault(str(name), {})[str(date_key)] = float(value)
        return out

    @staticmethod
    def _explode_session_hosts(frame: pd.DataFrame, *, value_col: str) -> pd.DataFrame:
        if frame.empty or "hosts_raw" not in frame.columns or value_col not in frame.columns:
            return pd.DataFrame(columns=["date", "account", "scope_name", "weighted_value"])
        data = frame[["date", "account", "hosts_raw", value_col]].copy()
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        data["account"] = data["account"].map(normalize_account)
        data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
        data["hosts"] = data["hosts_raw"].apply(split_hosts)
        data["host_count"] = data["hosts"].apply(len)
        data = data[data["date"].notna() & data[value_col].notna() & (data["host_count"] > 0)].copy()
        if data.empty:
            return pd.DataFrame(columns=["date", "account", "scope_name", "weighted_value"])
        data["weighted_value"] = data[value_col] / data["host_count"]
        data = data.explode("hosts", ignore_index=True)
        data["scope_name"] = data["hosts"].map(normalize_text)
        data = data[data["scope_name"].ne("")]
        return data[["date", "account", "scope_name", "weighted_value"]]

    @staticmethod
    def _explode_hosts(frame: pd.DataFrame, *, date_col: str, value_col: str, include_value: bool) -> pd.DataFrame:
        data = frame[[date_col, "标准账号", "本场主播", value_col]].copy()
        data["date"] = pd.to_datetime(data[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        data["parent_account"] = data["标准账号"].map(normalize_account)
        data["hosts"] = data["本场主播"].apply(split_hosts)
        data["host_count"] = data["hosts"].apply(len)
        data[value_col] = data[value_col].astype(str).str.strip()
        data = data[data["date"].notna() & (data["host_count"] > 0) & data[value_col].ne("")].copy()
        if data.empty:
            return pd.DataFrame(columns=["date", "parent_account", "scope_name", "weight"])
        data["weight"] = 1.0 / data["host_count"] if include_value else 0.0
        data = data.explode("hosts", ignore_index=True)
        data["scope_name"] = data["hosts"].map(normalize_text)
        data = data[data["scope_name"].ne("")]
        return data[["date", "parent_account", "scope_name", "weight"]]

    @staticmethod
    def _anchor_parent_map(*frames: pd.DataFrame) -> dict[str, str]:
        mapping: dict[str, list[str]] = {}
        for frame in frames:
            if frame.empty:
                continue
            if "hosts_raw" in frame.columns:
                exploded = DashboardDailyService._explode_session_hosts(frame, value_col="spend" if "spend" in frame.columns else "impressions")
                for _, row in exploded.iterrows():
                    mapping.setdefault(str(row["scope_name"]), []).append(str(row["account"]))
            elif "本场主播" in frame.columns and "标准账号" in frame.columns:
                exploded = DashboardDailyService._explode_hosts(frame, date_col="date" if "date" in frame.columns else "deal_date", value_col="线索ID_norm", include_value=True)
                for _, row in exploded.iterrows():
                    mapping.setdefault(str(row["scope_name"]), []).append(str(row["parent_account"]))
        return {name: " / ".join(dict.fromkeys(account for account in accounts if account)) for name, accounts in mapping.items()}

    @staticmethod
    def _seed_parent_map(seed_range: pd.DataFrame) -> dict[str, str]:
        exploded = DashboardDailyService._explode_session_hosts(seed_range, value_col="impressions") if not seed_range.empty else pd.DataFrame()
        if exploded.empty:
            return {}
        mapping: dict[str, list[str]] = {}
        for _, row in exploded.iterrows():
            mapping.setdefault(str(row["scope_name"]), []).append(str(row["account"]))
        return {name: " / ".join(dict.fromkeys(account for account in accounts if account)) for name, accounts in mapping.items()}

    @staticmethod
    def _is_hidden_anchor_name(name: str) -> bool:
        return normalize_text(name) in HIDDEN_ANCHOR_SUMMARY_NAMES

    @staticmethod
    def _is_hidden_anchor_performance_name(name: str) -> bool:
        return normalize_text(name) in HIDDEN_ANCHOR_PERFORMANCE_NAMES

    @staticmethod
    def _ratio_maps(numerator: dict[str, float], denominator: dict[str, float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for key in sorted(set(numerator) | set(denominator)):
            value = DashboardDailyService._safe_div_value(numerator.get(key), denominator.get(key))
            if value is not None:
                out[key] = value
        return out

    @staticmethod
    def _sum_present(values: dict[str, float] | None) -> float | None:
        if not values:
            return None
        return float(sum(float(value) for value in values.values()))

    @staticmethod
    def _safe_div_value(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator is None:
            return None
        if float(denominator) == 0:
            return None
        return float(numerator) / float(denominator)

    @staticmethod
    def _trend_payload(key: str, label: str, unit: str, values: dict[str, float], date_strings: list[str]) -> dict[str, Any]:
        return {"key": key, "label": label, "unit": unit, "points": DashboardDailyService._points_for_dates(values, date_strings)}

    @staticmethod
    def _points_for_dates(values: dict[str, float] | None, date_strings: list[str]) -> list[dict[str, Any]]:
        values = values or {}
        return [{"date": date_key, "value": DashboardDailyService._json_number(values[date_key]) if date_key in values else None} for date_key in date_strings]

    @staticmethod
    def _metric_summary(
        key: str,
        label: str,
        actual: float | None,
        target: float | None,
        unit: str,
        *,
        source_status: str = "available",
    ) -> dict[str, Any]:
        actual_value = DashboardDailyService._json_number(actual)
        target_value = DashboardDailyService._json_number(target)
        return {
            "key": key,
            "label": label,
            "actual": actual_value,
            "target": target_value,
            "attain_rate": DashboardDailyService._json_number(DashboardDailyService._safe_div_value(actual_value, target_value)),
            "unit": unit,
            "source_status": source_status if actual_value is None else "available",
        }

    @staticmethod
    def _json_number(value: object) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(number):
            return None
        return number

    @staticmethod
    def _range_target(monthly_target: float | None, window: dict[str, Any]) -> float | None:
        if monthly_target is None or pd.isna(monthly_target):
            return None
        start = date.fromisoformat(window["start_date"])
        end = date.fromisoformat(window["end_date"])
        total = 0.0
        current = start
        while current <= end:
            month_days = calendar.monthrange(current.year, current.month)[1]
            total += float(monthly_target) / month_days
            current += timedelta(days=1)
        return total

    @classmethod
    def _target_for_entity(
        cls,
        targets: pd.DataFrame,
        window: dict[str, Any],
        scope_type: str,
        scope_name: str,
        column: str,
    ) -> float | None:
        if targets.empty or column not in targets.columns:
            return None
        start = date.fromisoformat(window["start_date"])
        end = date.fromisoformat(window["end_date"])
        total = 0.0
        found = False
        current = start
        while current <= end:
            month = f"{current.year:04d}-{current.month:02d}"
            month_days = calendar.monthrange(current.year, current.month)[1]
            matched = targets[
                targets["month"].eq(month)
                & targets["scope_type"].eq(scope_type)
                & targets["scope_name"].eq(scope_name)
            ]
            if not matched.empty:
                value = pd.to_numeric(matched.iloc[0].get(column), errors="coerce")
                if pd.notna(value):
                    total += float(value) / month_days
                    found = True
            current += timedelta(days=1)
        return total if found else None

    @staticmethod
    def _target_scalar_for_entity(
        targets: pd.DataFrame,
        window: dict[str, Any],
        scope_type: str,
        scope_name: str,
        column: str,
    ) -> float | None:
        if targets.empty or column not in targets.columns:
            return None
        months = sorted({date_key[:7] for date_key in DashboardDailyService._calendar_date_strings(window["start_date"], window["end_date"])})
        matched = targets[
            targets["month"].isin(months)
            & targets["scope_type"].eq(scope_type)
            & targets["scope_name"].eq(scope_name)
        ]
        values = pd.to_numeric(matched[column], errors="coerce").dropna()
        return float(values.iloc[-1]) if not values.empty else None

    @classmethod
    def _seed_target_for_entity(
        cls,
        targets: pd.DataFrame,
        window: dict[str, Any],
        scope_type: str,
        scope_name: str,
    ) -> float | None:
        if targets.empty or "impression_target_month" not in targets.columns:
            return None
        target_scope = scope_name
        if scope_type == "account":
            target_scope = normalize_account(scope_name)
        matched_scope_type = "host" if scope_type in {"host", "anchor"} else scope_type
        start = date.fromisoformat(window["start_date"])
        end = date.fromisoformat(window["end_date"])
        total = 0.0
        found = False
        current = start
        while current <= end:
            month = f"{current.year:04d}-{current.month:02d}"
            month_days = calendar.monthrange(current.year, current.month)[1]
            matched = targets[
                targets["month"].eq(month)
                & targets["scope_type"].eq(matched_scope_type)
                & targets["scope_name"].eq(target_scope)
            ]
            values = pd.to_numeric(matched["impression_target_month"], errors="coerce").dropna()
            if not values.empty:
                total += float(values.iloc[-1]) / month_days
                found = True
            current += timedelta(days=1)
        return total if found else None

    @classmethod
    def _fixed_seed_anchor_names_for_window(cls, targets: pd.DataFrame, window: dict[str, Any]) -> set[str]:
        if targets.empty or "month" not in targets.columns or "scope_name" not in targets.columns or "scope_type" not in targets.columns:
            return set()
        months = sorted({date_key[:7] for date_key in cls._calendar_date_strings(window["start_date"], window["end_date"])})
        matched = targets[
            targets["month"].isin(months)
            & targets["scope_type"].eq("host")
            & targets["scope_name"].map(normalize_text).isin(FIXED_SEED_ANCHOR_NAMES)
        ]
        return {normalize_text(name) for name in matched["scope_name"].dropna().astype(str)}

    @classmethod
    def _seed_target_parent_for_entity(
        cls,
        targets: pd.DataFrame,
        window: dict[str, Any],
        scope_type: str,
        scope_name: str,
    ) -> str:
        if targets.empty or "month" not in targets.columns or "scope_name" not in targets.columns or "scope_type" not in targets.columns:
            return ""
        months = sorted({date_key[:7] for date_key in cls._calendar_date_strings(window["start_date"], window["end_date"])})
        matched = targets[
            targets["month"].isin(months)
            & targets["scope_type"].eq("host" if scope_type in {"host", "anchor"} else scope_type)
            & targets["scope_name"].eq(scope_name)
        ]
        for column in ("parent_account", "parent_scope"):
            if column not in matched.columns:
                continue
            values = matched[column].dropna().astype(str).map(normalize_text)
            values = values[values.ne("")]
            if not values.empty:
                return normalize_account(values.iloc[-1]) if column == "parent_account" else values.iloc[-1]
        return ""

    @staticmethod
    def _metric_source_status(
        fact: pd.DataFrame,
        raw_leads: pd.DataFrame,
        raw_deals: pd.DataFrame,
        live_sessions: pd.DataFrame,
        seed_sessions: pd.DataFrame,
    ) -> list[dict[str, Any]]:
        return [
            {"metric": "线索/成交/账号/主播", "status": "available" if not fact.empty else "not_connected", "source": "output/fact_attribution.csv"},
            {"metric": "到店", "status": "available" if not raw_leads.empty and any(column in raw_leads.columns for column in ["到店日期", "到店时间"]) else "not_connected", "source": "总部新媒体线索"},
            {"metric": "EX7", "status": "available" if not raw_leads.empty or not raw_deals.empty else "not_connected", "source": "report_topline_config + 源线索/成交车型字段"},
            {"metric": "费用", "status": "available" if not live_sessions.empty and "spend" in live_sessions.columns else "not_connected", "source": "直播进度表"},
            {"metric": "种草曝光", "status": "available" if not seed_sessions.empty else "not_connected", "source": "EXEED星途台账"},
            {"metric": "曝光到线索转化率", "status": "not_connected", "source": "种草曝光与线索尚无可靠关联键"},
        ]

    @staticmethod
    def _missing_calendar_dates(report_dates: list[str]) -> list[str]:
        if not report_dates:
            return []
        start = date.fromisoformat(report_dates[0])
        end = date.fromisoformat(report_dates[-1])
        present = set(report_dates)
        missing: list[str] = []
        current = start
        while current <= end:
            formatted = current.isoformat()
            if formatted not in present:
                missing.append(formatted)
            current += timedelta(days=1)
        return missing

    def _trend_quality_annotations(self, report_dates: list[str]) -> dict[str, dict[str, Any]]:
        wanted_dates = set(report_dates)
        records: dict[str, dict[str, Any]] = {}
        runs_dir = self.repo_root / "artifacts" / "runs"
        if runs_dir.exists():
            for path in sorted(runs_dir.glob("run_manifest_*.json")):
                payload = self._read_json(path)
                run_id = str(payload.get("run_id") or self._run_id_from_evidence_path(path))
                report_date = self._evidence_report_date(payload)
                if report_date not in wanted_dates:
                    continue
                record = records.setdefault(run_id, self._empty_quality_record(report_date, run_id))
                record["sort_key"] = self._quality_sort_key(payload, run_id)
                record["evidence_paths"].append(self._public_path(path))
                record["quality_status"] = self._first_text(
                    payload.get("quality_status"),
                    payload.get("release_readiness_evaluation", {}).get("quality_status")
                    if isinstance(payload.get("release_readiness_evaluation"), dict)
                    else None,
                    record.get("quality_status"),
                )
                record["quality_decision"] = self._first_text(
                    payload.get("quality_decision"),
                    payload.get("release_readiness_evaluation", {}).get("quality_decision")
                    if isinstance(payload.get("release_readiness_evaluation"), dict)
                    else None,
                    record.get("quality_decision"),
                )
                record["release_readiness"] = self._first_text(payload.get("release_readiness"), record.get("release_readiness"))

            for path in sorted(runs_dir.glob("quality_report_*.json")):
                payload = self._read_json(path)
                run_id = str(payload.get("run_id") or self._run_id_from_evidence_path(path))
                report_date = self._evidence_report_date(payload)
                if report_date not in wanted_dates:
                    continue
                record = records.setdefault(run_id, self._empty_quality_record(report_date, run_id))
                record["sort_key"] = max(record.get("sort_key", ""), self._quality_sort_key(payload, run_id))
                record["evidence_paths"].append(self._public_path(path))
                summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
                record["quality_status"] = self._first_text(
                    payload.get("quality_status"),
                    payload.get("overall_status"),
                    summary.get("overall_status"),
                    record.get("quality_status"),
                )
                record["quality_decision"] = self._first_text(
                    payload.get("quality_decision"),
                    summary.get("operational_decision"),
                    record.get("quality_decision"),
                )
                record["release_readiness"] = self._first_text(payload.get("release_readiness"), record.get("release_readiness"))

        by_date: dict[str, list[dict[str, Any]]] = {report_date: [] for report_date in report_dates}
        for record in records.values():
            report_date = str(record.get("report_date", ""))
            if report_date in by_date:
                by_date[report_date].append(record)

        annotations: dict[str, dict[str, Any]] = {}
        for report_date in report_dates:
            candidates = by_date.get(report_date, [])
            if candidates:
                chosen = max(candidates, key=lambda item: (str(item.get("sort_key", "")), str(item.get("run_id", ""))))
                annotations[report_date] = {
                    "report_date": report_date,
                    "run_id": str(chosen.get("run_id", "")),
                    "quality_status": str(chosen.get("quality_status") or "unknown"),
                    "quality_decision": str(chosen.get("quality_decision") or "unknown"),
                    "release_readiness": str(chosen.get("release_readiness") or "unknown"),
                    "evidence_paths": sorted(
                        dict.fromkeys(str(path) for path in chosen.get("evidence_paths", [])),
                        key=lambda item: (0 if "/run_manifest_" in item else 1, item),
                    ),
                }
            else:
                annotations[report_date] = self._empty_quality_record(report_date, "")
                annotations[report_date].pop("sort_key", None)
        return annotations

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _run_id_from_evidence_path(path: Path) -> str:
        stem = path.stem
        if stem.startswith("run_manifest_"):
            return stem.removeprefix("run_manifest_")
        if stem.startswith("quality_report_"):
            return stem.removeprefix("quality_report_")
        return stem

    @staticmethod
    def _evidence_report_date(payload: dict[str, Any]) -> str:
        for key in ("canonical_report_date", "report_date", "resolved_report_date", "manifest_report_date"):
            value = payload.get(key)
            if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                return value
        resolution = payload.get("report_date_resolution")
        if isinstance(resolution, dict):
            for key in ("canonical_report_date", "report_date", "resolved_report_date", "manifest_report_date"):
                value = resolution.get(key)
                if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    return value
        return ""

    @staticmethod
    def _quality_sort_key(payload: dict[str, Any], run_id: str) -> str:
        for key in ("generated_at", "finished_at", "started_at"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        return run_id

    @staticmethod
    def _empty_quality_record(report_date: str, run_id: str) -> dict[str, Any]:
        return {
            "report_date": report_date,
            "run_id": run_id,
            "quality_status": "unknown",
            "quality_decision": "unknown",
            "release_readiness": "unknown",
            "evidence_paths": [],
            "sort_key": run_id,
        }

    @staticmethod
    def _first_text(*values: object) -> str:
        for value in values:
            if isinstance(value, str) and value:
                return value
        return "unknown"

    def _public_path(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.repo_root))
        except ValueError:
            return path.name

    @staticmethod
    def _overview(source: DashboardSource) -> dict[str, dict[str, Any]]:
        return {
            "impressions": DashboardDailyService._metric_payload(source.metric("department", "全量", "impressions")),
            "mtd_unique_leads": DashboardDailyService._metric_payload(
                source.metric("department", "全量", "mtd_unique_leads")
            ),
            "mtd_deals": DashboardDailyService._metric_payload(source.metric("department", "全量", "mtd_deals")),
            "mtd_douyin_laike_orders": DashboardDailyService._metric_payload(
                source.metric("department", "全量", "mtd_douyin_laike_orders")
            ),
            "mtd_spend": DashboardDailyService._metric_payload(source.metric("department", "全量", "mtd_spend")),
            "mtd_cpl": DashboardDailyService._metric_payload(source.metric("department", "全量", "mtd_cpl")),
            "mtd_cps": DashboardDailyService._metric_payload(source.metric("department", "全量", "mtd_cps")),
            "pending_day": DashboardDailyService._metric_payload(source.metric("department", "全量", "pending_day")),
            "pending_cumulative": DashboardDailyService._metric_payload(
                source.metric("department", "全量", "pending_cumulative")
            ),
            "raw_leads": DashboardDailyService._metric_payload(source.metric("department", "全量", "raw_leads")),
            "unique_rate": DashboardDailyService._metric_payload(source.metric("department", "全量", "unique_rate")),
            "unowned_leads": DashboardDailyService._metric_payload(
                source.metric("department", "全量", "unowned_leads")
            ),
            "manual_overrides": DashboardDailyService._metric_payload(
                source.metric("department", "全量", "manual_overrides")
            ),
        }

    @staticmethod
    def _funnel(overview: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        steps = [
            overview["impressions"],
            overview["raw_leads"],
            overview["mtd_unique_leads"],
            overview["mtd_douyin_laike_orders"],
            overview["mtd_deals"],
        ]
        out: list[dict[str, Any]] = []
        previous_actual = 0.0
        for metric in steps:
            actual = float(metric["actual"])
            out.append(
                {
                    "key": metric["key"],
                    "label": metric["label"],
                    "actual": actual,
                    "unit": metric["unit"],
                    "conversion_from_previous": round(actual / previous_actual, 6) if previous_actual else None,
                }
            )
            previous_actual = actual
        return out

    @staticmethod
    def _segments(source: DashboardSource) -> dict[str, Any]:
        ex7 = DashboardDailyService._segment(source, ["EX7 专项", "EX7专项"])
        non_ex7 = DashboardDailyService._segment(source, ["不含 EX7", "不含EX7"])
        return {
            "ex7": ex7,
            "non_ex7": non_ex7,
            "deltas": {
                "leads_delta": round(
                    ex7["metrics"]["mtd_unique_leads"]["actual"] - non_ex7["metrics"]["mtd_unique_leads"]["actual"],
                    6,
                ),
                "deals_delta": round(
                    ex7["metrics"]["mtd_deals"]["actual"] - non_ex7["metrics"]["mtd_deals"]["actual"],
                    6,
                ),
                "cpl_delta": round(ex7["metrics"]["mtd_cpl"]["actual"] - non_ex7["metrics"]["mtd_cpl"]["actual"], 6),
                "cps_delta": round(ex7["metrics"]["mtd_cps"]["actual"] - non_ex7["metrics"]["mtd_cps"]["actual"], 6),
            },
        }

    @staticmethod
    def _segment(source: DashboardSource, names: list[str]) -> dict[str, Any]:
        chosen = names[0]
        for name in names:
            if source.metric("segment", name, "mtd_unique_leads").actual:
                chosen = name
                break
        keys = ["mtd_unique_leads", "mtd_deals", "mtd_spend", "mtd_cpl", "mtd_cps"]
        return {
            "label": chosen,
            "metrics": {key: DashboardDailyService._metric_payload(source.metric("segment", chosen, key)) for key in keys},
        }

    @staticmethod
    def _anchors(source: DashboardSource, source_table: str, metric_keys: list[str]) -> list[dict[str, Any]]:
        anchors = source.anchor_rows(source_table, metric_keys)
        out: list[dict[str, Any]] = []
        for anchor in anchors:
            name = str(anchor["name"])
            if DashboardDailyService._is_hidden_anchor_name(name):
                continue
            out.append(
                {
                    "name": name,
                    "parent_scope": str(anchor.get("parent_scope", "")),
                    "metrics": {
                        key: DashboardDailyService._metric_payload(source.metric("anchor", name, key))
                        for key in metric_keys
                    },
                }
            )
        return out

    @staticmethod
    def _metric_payload(metric: Metric) -> dict[str, Any]:
        return {
            "key": metric.key,
            "label": metric.label,
            "actual": metric.actual,
            "target": metric.target,
            "attain_rate": metric.rate,
            "unit": metric.unit,
            "source_column": metric.note,
        }

    @classmethod
    def _trend_entity(
        cls,
        sources: list[tuple[str, Path, DashboardSource]],
        source_table: str,
        scope_type: str,
        names: list[str],
        metric_keys: list[str],
        quality_annotations: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        chosen = names[0]
        for name in names:
            if any(cls._source_metric(source, source_table, scope_type, name, metric_keys[0]).actual for _, _, source in sources):
                chosen = name
                break
        return {
            "name": chosen,
            "parent_scope": cls._parent_scope_for_latest(sources, source_table, scope_type, chosen),
            "metrics": cls._trend_metrics(sources, source_table, scope_type, chosen, metric_keys, quality_annotations),
        }

    @classmethod
    def _trend_entities_for_source(
        cls,
        sources: list[tuple[str, Path, DashboardSource]],
        source_table: str,
        scope_type: str,
        metric_keys: list[str],
        quality_annotations: dict[str, dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        names: dict[str, str] = {}
        for _, _, source in sources:
            for row in source.rows:
                if row.get("source_table") != source_table or row.get("scope_type") != scope_type:
                    continue
                name = row.get("scope_name", "")
                if scope_type == "account" and normalize_account(name) in HIDDEN_ACCOUNT_SUMMARY_NAMES:
                    continue
                if scope_type == "anchor":
                    if source_table == "lead_anchor" and cls._is_hidden_anchor_performance_name(name):
                        continue
                    if source_table != "lead_anchor" and cls._is_hidden_anchor_name(name):
                        continue
                if name and name not in names:
                    names[name] = row.get("parent_scope", "")
        return [
            {
                "name": name,
                "parent_scope": parent_scope,
                "metrics": cls._trend_metrics(sources, source_table, scope_type, name, metric_keys, quality_annotations),
            }
            for name, parent_scope in names.items()
        ]

    @classmethod
    def _trend_metrics(
        cls,
        sources: list[tuple[str, Path, DashboardSource]],
        source_table: str,
        scope_type: str,
        scope_name: str,
        metric_keys: list[str],
        quality_annotations: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        return {
            metric_key: cls._trend_series(sources, source_table, scope_type, scope_name, metric_key, quality_annotations)
            for metric_key in metric_keys
        }

    @classmethod
    def _trend_series(
        cls,
        sources: list[tuple[str, Path, DashboardSource]],
        source_table: str,
        scope_type: str,
        scope_name: str,
        metric_key: str,
        quality_annotations: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        points: list[dict[str, Any]] = []
        label = metric_key
        unit = ""
        for report_date, _, source in sources:
            row = cls._source_row(source, source_table, scope_type, scope_name, metric_key)
            metric = cls._metric_from_row(metric_key, row)
            if metric.label != metric_key:
                label = metric.label
            if metric.unit:
                unit = metric.unit
            annotation = (quality_annotations or {}).get(report_date, {})
            points.append(
                {
                    "report_date": report_date,
                    "actual": metric.actual,
                    "target": metric.target,
                    "attain_rate": metric.rate,
                    "unit": metric.unit,
                    "source_column": metric.note,
                    "is_missing": not bool(row),
                    "quality_status": str(annotation.get("quality_status") or "unknown"),
                }
            )
        return {
            "key": metric_key,
            "label": label,
            "unit": unit,
            "points": points,
        }

    @classmethod
    def _source_metric(
        cls,
        source: DashboardSource,
        source_table: str,
        scope_type: str,
        scope_name: str,
        metric_key: str,
    ) -> Metric:
        row = cls._source_row(source, source_table, scope_type, scope_name, metric_key)
        return cls._metric_from_row(metric_key, row)

    @classmethod
    def _metric_from_row(cls, metric_key: str, row: dict[str, str]) -> Metric:
        return Metric(
            key=metric_key,
            label=row.get("metric_name") or metric_key,
            actual=cls._parse_num(row.get("actual")),
            target=cls._parse_optional_num(row.get("target")),
            rate=cls._parse_optional_num(row.get("attain_rate")),
            unit=row.get("unit", ""),
            note=row.get("source_column", ""),
        )

    @staticmethod
    def _source_row(
        source: DashboardSource,
        source_table: str,
        scope_type: str,
        scope_name: str,
        metric_key: str,
    ) -> dict[str, str]:
        for row in source.rows:
            if (
                row.get("source_table") == source_table
                and row.get("scope_type") == scope_type
                and row.get("scope_name") == scope_name
                and row.get("metric_key") == metric_key
            ):
                return row
        return {}

    @classmethod
    def _parent_scope_for_latest(
        cls,
        sources: list[tuple[str, Path, DashboardSource]],
        source_table: str,
        scope_type: str,
        scope_name: str,
    ) -> str:
        for _, _, source in reversed(sources):
            for row in source.rows:
                if (
                    row.get("source_table") == source_table
                    and row.get("scope_type") == scope_type
                    and row.get("scope_name") == scope_name
                ):
                    return row.get("parent_scope", "")
        return ""

    @staticmethod
    def _parse_num(value: object) -> float:
        text = "" if value is None else str(value).strip().replace(",", "")
        if not text or text in {"-", "nan", "NaN", "N/A"}:
            return 0.0
        scale = 0.01 if text.endswith("%") else 1.0
        if text.endswith("%"):
            text = text[:-1]
        try:
            return float(text) * scale
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_optional_num(value: object) -> float | None:
        text = "" if value is None else str(value).strip().replace(",", "")
        if not text or text in {"-", "nan", "NaN", "N/A"}:
            return None
        scale = 0.01 if text.endswith("%") else 1.0
        if text.endswith("%"):
            text = text[:-1]
        try:
            return float(text) * scale
        except ValueError:
            return None
