"""Thin CLI for fact-layer generation.

Round 3 goal:
- keep the root `build_fact_from_three_sources.py` as a compatibility shell
- run production fact build from `src/oae`
"""

from __future__ import annotations

import argparse
import logging
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter

import pandas as pd

from oae.contracts import attach_contract_metadata
from oae.contracts.models import RunMetadata
from oae.facts import build_fact_artifacts
from oae.rules.account_mapping import NON_LIVE_ACCOUNTS
from oae.rules.columns import COLUMN_ALIASES
from oae.rules.file_discovery import pick_latest_file, pick_latest_live_file
from oae.rules.hosts import count_hosts_in_text, split_hosts_text
from oae.rules.io_utils import read_csv_auto, read_table_auto, resolve_path
from oae.version import METRIC_VERSION, SCHEMA_VERSION, build_run_id


ALLOWED_LEAD_CHANNEL3 = {"直播", "其他", "主页", "星途星纪元直播营销中心"}
LEAD_ACCOUNT_FALLBACK_CHANNEL2_VALUE = "抖音来客直播"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从3个源文件生成统一事实层 fact_attribution")
    parser.add_argument("--workspace", default=".", help="工作目录")
    parser.add_argument("--data-dir", default="源文件", help="线索/成交文件目录（默认按固定源文件目录自动选最新）")
    parser.add_argument("--host-count-input-file", default="", help="仅统计主播加权计数时使用的输入文件（csv/xlsx）")
    parser.add_argument("--host-count-input-sheet", default="", help="仅统计主播加权计数时使用的sheet，默认第一张")
    parser.add_argument("--live-file", default="", help="直播进度表路径（xlsx）；留空则自动选最新")
    parser.add_argument("--live-sheet", default="", help="直播进度表sheet，默认第一张")
    parser.add_argument("--leads-file", default="", help="线索文件路径（csv/xlsx）；留空则按 pattern 自动选最新")
    parser.add_argument("--deals-file", default="", help="成交文件路径（csv/xlsx）；留空则按 pattern 自动选最新")
    parser.add_argument(
        "--leads-pattern",
        default="总部新媒体线索20??-??-??.csv,总部新媒体线索20??-??-??.xlsx,总部新媒体线索*.csv,总部新媒体线索*.xlsx",
        help="线索文件通配符，可逗号分隔多个pattern",
    )
    parser.add_argument(
        "--deals-pattern",
        default="总部新媒体成交20??-??-??.csv,总部新媒体成交20??-??-??.xlsx,总部新媒体成交*.csv,总部新媒体成交*.xlsx",
        help="成交文件通配符，可逗号分隔多个pattern",
    )
    parser.add_argument("--buffer-minutes", type=int, default=5, help="直播匹配时间窗口前后缓冲")
    parser.add_argument("--default-duration-minutes", type=int, default=240, help="缺失下播时间时的默认时长")
    parser.add_argument("--max-duration-hours", type=int, default=24, help="最大直播时长熔断")
    parser.add_argument(
        "--match-mode",
        choices=["generate_report", "process_deal_data"],
        default="process_deal_data",
        help="匹配口径：generate_report=命中场次平分；process_deal_data=最近场次且保留该场主播",
    )
    parser.add_argument("--output-dir", default="output", help="输出目录")
    parser.add_argument("--run-id", default="", help="产品化运行编号，留空自动生成")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION, help="事实表 schema 版本")
    parser.add_argument("--metric-version", default=METRIC_VERSION, help="经营口径版本")
    return parser.parse_args()


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("oae.build_fact")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


@contextmanager
def timed_step(logger: logging.Logger, label: str):
    start = perf_counter()
    try:
        yield
    finally:
        logger.info("%s 耗时: %.3fs", label, perf_counter() - start)


def build_weighted_host_counts(fact: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    if "本场主播" not in fact.columns:
        logger.warning("事实表缺少 本场主播 列，跳过主播加权统计")
        return pd.DataFrame(columns=["名称", "计数", "展示"])

    counts_in_cents: dict[str, int] = {}
    for hosts_value in fact["本场主播"]:
        hosts = split_hosts_text(hosts_value)
        if not hosts:
            continue
        share_cents = 100 // len(hosts)
        for host in hosts:
            counts_in_cents[host] = counts_in_cents.get(host, 0) + share_cents

    if not counts_in_cents:
        return pd.DataFrame(columns=["名称", "计数", "展示"])

    result = pd.DataFrame({"名称": list(counts_in_cents.keys()), "计数分": list(counts_in_cents.values())})
    result = result.sort_values(["计数分", "名称"], ascending=[False, True]).reset_index(drop=True)
    result["计数"] = result["计数分"] / 100.0
    result = result[["名称", "计数"]]
    result["展示"] = result["名称"] + " (" + result["计数"].map(lambda value: f"{value:.2f}") + ")"
    logger.info("主播加权统计行数: %s", len(result))
    return result


def load_host_count_input(path: Path, sheet_name: str = "") -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_auto(path)
    if suffix in {".xlsx", ".xls"}:
        workbook = pd.ExcelFile(path)
        use_sheet = sheet_name if sheet_name and sheet_name in workbook.sheet_names else workbook.sheet_names[0]
        return pd.read_excel(path, sheet_name=use_sheet)
    raise ValueError(f"不支持的文件格式: {path}")


def run(args: argparse.Namespace | None = None) -> int:
    parsed = args or parse_args()
    logger = setup_logger()
    metadata = RunMetadata(
        run_id=parsed.run_id or build_run_id(),
        schema_version=parsed.schema_version,
        metric_version=parsed.metric_version,
    )

    workspace = Path(parsed.workspace).resolve()
    data_dir = resolve_path(workspace, parsed.data_dir)
    output_dir = resolve_path(workspace, parsed.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if parsed.host_count_input_file:
        try:
            input_path = resolve_path(workspace, parsed.host_count_input_file)
            if not input_path.exists():
                logger.error("主播统计输入文件不存在: %s", input_path)
                return 1
            with timed_step(logger, "读取主播统计输入"):
                host_source = load_host_count_input(input_path, sheet_name=parsed.host_count_input_sheet)
            with timed_step(logger, "计算主播加权统计"):
                host_counts = build_weighted_host_counts(host_source, logger)
            host_counts_xlsx = output_dir / "host_counts_weighted.xlsx"
            host_counts_csv = output_dir / "host_counts_weighted.csv"
            with pd.ExcelWriter(host_counts_xlsx, engine="openpyxl") as writer:
                host_counts.to_excel(writer, sheet_name="host_counts_weighted", index=False, float_format="%.2f")
            host_counts.to_csv(host_counts_csv, index=False, encoding="utf-8-sig", float_format="%.2f")
            logger.info("输出完成: %s", host_counts_xlsx)
            logger.info("输出完成: %s", host_counts_csv)
            return 0
        except Exception as exc:  # pragma: no cover - compatibility mode
            logger.exception("执行失败: %s", exc)
            return 1

    live_path = resolve_path(workspace, parsed.live_file) if parsed.live_file else Path("")
    if not parsed.live_file or not live_path.exists():
        try:
            live_path = pick_latest_live_file([workspace, data_dir])
            if parsed.live_file:
                logger.warning("指定直播进度表不存在，自动改用最新文件: %s", live_path)
            else:
                logger.info("未显式指定直播进度表，自动选用最新文件: %s", live_path)
        except FileNotFoundError:
            logger.error("直播进度表不存在: %s", live_path)
            return 1

    leads_path = resolve_path(workspace, parsed.leads_file) if parsed.leads_file else pick_latest_file(data_dir, parsed.leads_pattern)
    if not leads_path.exists():
        logger.error("线索文件不存在: %s", leads_path)
        return 1

    deals_path: Path | None
    if parsed.deals_file:
        deals_path = resolve_path(workspace, parsed.deals_file)
    else:
        try:
            deals_path = pick_latest_file(data_dir, parsed.deals_pattern)
        except FileNotFoundError:
            deals_path = None
    if deals_path is not None and not deals_path.exists():
        logger.warning("成交文件不存在，按空成交表处理: %s", deals_path)
        deals_path = None

    logger.info("直播进度表: %s", live_path)
    logger.info("线索文件: %s", leads_path)
    logger.info("成交文件: %s", deals_path if deals_path is not None else "未提供")
    logger.info("匹配口径: %s", parsed.match_mode)

    try:
        with timed_step(logger, "读取输入文件"):
            live_book = pd.ExcelFile(live_path)
            live_sheet = parsed.live_sheet if parsed.live_sheet and parsed.live_sheet in live_book.sheet_names else live_book.sheet_names[0]
            live_raw = pd.read_excel(live_path, sheet_name=live_sheet)
            leads_raw = read_table_auto(leads_path, preferred_sheets=["总部新媒体线索", "线索", "Sheet1"])
            deals_raw = read_table_auto(deals_path, preferred_sheets=["成交", "Sheet1"]) if deals_path is not None else pd.DataFrame()

        with timed_step(logger, "事实层构建（src/oae/facts）"):
            artifacts = build_fact_artifacts(
                live_raw=live_raw,
                leads_raw=leads_raw,
                deals_raw=deals_raw,
                logger=logger,
                buffer_minutes=parsed.buffer_minutes,
                default_duration_minutes=parsed.default_duration_minutes,
                max_duration_hours=parsed.max_duration_hours,
                match_mode=parsed.match_mode,
                column_aliases=COLUMN_ALIASES,
                allowed_channel3=ALLOWED_LEAD_CHANNEL3,
                fallback_channel2_value=LEAD_ACCOUNT_FALLBACK_CHANNEL2_VALUE,
                non_live_accounts=NON_LIVE_ACCOUNTS,
            )
            fact = attach_contract_metadata(artifacts.fact, metadata)

        with timed_step(logger, "写出事实表"):
            out_xlsx = output_dir / "fact_attribution.xlsx"
            out_csv = output_dir / "fact_attribution.csv"
            with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
                fact.to_excel(writer, sheet_name="fact_attribution", index=False)
            fact.to_csv(out_csv, index=False, encoding="utf-8-sig")

        with timed_step(logger, "生成主播统计"):
            host_counts = build_weighted_host_counts(fact, logger)
            host_counts_xlsx = output_dir / "host_counts_weighted.xlsx"
            host_counts_csv = output_dir / "host_counts_weighted.csv"
            with pd.ExcelWriter(host_counts_xlsx, engine="openpyxl") as writer:
                host_counts.to_excel(writer, sheet_name="host_counts_weighted", index=False, float_format="%.2f")
            host_counts.to_csv(host_counts_csv, index=False, encoding="utf-8-sig", float_format="%.2f")

        logger.info("输出完成: %s", out_xlsx)
        logger.info("输出完成: %s", out_csv)
        logger.info("输出完成: %s", host_counts_xlsx)
        logger.info("输出完成: %s", host_counts_csv)
        return 0
    except Exception as exc:  # pragma: no cover - operational CLI
        logger.exception("执行失败: %s", exc)
        return 1


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
