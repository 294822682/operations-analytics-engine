"""
发布前一键复核：
- 对比 feishu_table_latest_{date}.tsv 与 latest CSV / fact / live 源数据的一致性
- 覆盖部门总体、成交/待交车账号、线索质量、账号层、到人层
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

import oae.exports.feishu_report as g
from oae.exports.feishu_topline import (
    build_topline_summary,
    build_tsv_topline_lines,
    load_deals_source,
    load_leads_source,
    load_topline_config,
    resolve_latest_source_file,
)
from oae.overrides import load_fact_with_manual_overrides


def _expand_search_dirs(base_dirs: list[Path]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for base in base_dirs:
        for candidate in [base, base / "源文件"]:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
            if key not in seen:
                seen.add(key)
                out.append(candidate)
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="发布前一键复核 TSV 报表")
    p.add_argument("--reports-dir", default="output/sql_reports", help="日报目录")
    p.add_argument("--fact-csv", default="output/fact_attribution.csv", help="事实表 CSV")
    p.add_argument("--manual-override-file", default="config/manual_attribution_overrides.csv", help="专项人工确认归属配置")
    p.add_argument("--live-file", default="2026年直播进度表.xlsx", help="直播进度表路径")
    p.add_argument("--leads-file", default="", help="原始线索明细 CSV")
    p.add_argument("--deals-file", default="", help="原始成交明细 CSV")
    p.add_argument("--topline-config", default="config/report_topline_config.json", help="顶部核心汇报配置")
    p.add_argument("--tsv", default="", help="指定待复核 TSV；留空自动取最新")
    return p.parse_args()


def find_latest_tsv(reports_dir: Path) -> tuple[Path, str]:
    pattern = re.compile(r"^feishu_table_latest_(\d{4}-\d{2}-\d{2})\.tsv$")
    candidates: list[tuple[str, Path]] = []
    for p in reports_dir.glob("feishu_table_latest_*.tsv"):
        m = pattern.match(p.name)
        if m:
            candidates.append((m.group(1), p))
    if not candidates:
        raise SystemExit(f"[ERROR] no tsv matched in {reports_dir}")
    date_str, path = sorted(candidates, key=lambda x: x[0])[-1]
    return path, date_str


def parse_section_df(lines: list[str], title: str) -> pd.DataFrame:
    if title not in lines:
        raise ValueError(f"missing section title: {title}")
    idx = lines.index(title)
    header = lines[idx + 1].split("\t")
    data = []
    j = idx + 2
    while j < len(lines):
        line = lines[j]
        if not line.strip():
            break
        if "（母集）" in line or "（子集）" in line:
            break
        data.append(line.split("\t"))
        j += 1
    return pd.DataFrame(data, columns=header)


def as_str_df(df: pd.DataFrame) -> pd.DataFrame:
    # Categorical 列直接 fillna(\"\") 会报错，先转 object 再统一转字符串。
    return df.astype("object").fillna("").astype(str).reset_index(drop=True)


def assert_df_equal(name: str, expected: pd.DataFrame, got: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    e = as_str_df(expected)
    gdf = as_str_df(got)

    if list(e.columns) != list(gdf.columns):
        errors.append(f"{name}列名不一致: expected={list(e.columns)} got={list(gdf.columns)}")
        return errors
    if e.shape != gdf.shape:
        errors.append(f"{name}形状不一致: expected={e.shape} got={gdf.shape}")
        return errors

    for i in range(len(e)):
        for c in e.columns:
            if e.at[i, c] != gdf.at[i, c]:
                errors.append(f"{name}不一致 row={i+1} col={c}: expected={e.at[i, c]} got={gdf.at[i, c]}")
                return errors
    return errors


def main() -> int:
    args = parse_args()
    reports_dir = Path(args.reports_dir).expanduser().resolve()
    fact_path = Path(args.fact_csv).expanduser().resolve()
    manual_override_path = Path(args.manual_override_file).expanduser().resolve() if str(args.manual_override_file).strip() else None
    live_path = Path(args.live_file).expanduser().resolve()
    topline_config_path = Path(args.topline_config).expanduser().resolve()

    if not reports_dir.exists():
        print(f"[ERROR] reports dir not found: {reports_dir}")
        return 1
    if not fact_path.exists():
        print(f"[ERROR] fact csv not found: {fact_path}")
        return 1

    if args.tsv:
        tsv_path = Path(args.tsv).expanduser().resolve()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", tsv_path.name)
        if not m:
            print(f"[ERROR] 无法从 TSV 文件名解析日期: {tsv_path.name}")
            return 1
        report_date_str = m.group(1)
    else:
        tsv_path, report_date_str = find_latest_tsv(reports_dir)

    if not tsv_path.exists():
        print(f"[ERROR] tsv not found: {tsv_path}")
        return 1

    search_dirs = [
        Path.cwd().resolve(),
        reports_dir.parent.resolve(),
        reports_dir.parent.parent.resolve() if reports_dir.parent.parent.exists() else reports_dir.parent.resolve(),
        fact_path.parent.resolve(),
        fact_path.parent.parent.resolve() if fact_path.parent.parent.exists() else fact_path.parent.resolve(),
    ]
    search_dirs = _expand_search_dirs(search_dirs)
    if not live_path.exists():
        try:
            live_path = g.pick_latest_live_file(search_dirs)
            print(f"[WARN] live file not found; auto use: {live_path}")
        except FileNotFoundError:
            print(f"[ERROR] 日报顶部汇报需要直播进度表，但未找到可用文件: {args.live_file}")
            return 1

    leads_path = resolve_latest_source_file(args.leads_file, search_dirs, "总部新媒体线索*.csv", "原始线索明细")
    deals_path = resolve_latest_source_file(args.deals_file, search_dirs, "总部新媒体成交*.csv", "原始成交明细")

    try:
        acc = g.load_panel_for_date(reports_dir=reports_dir, report_date_str=report_date_str, scope="account")
        anc = g.load_panel_for_date(reports_dir=reports_dir, report_date_str=report_date_str, scope="anchor")
    except SystemExit as exc:
        print(str(exc))
        return 1
    fact = load_fact_with_manual_overrides(fact_path, manual_override_path=manual_override_path)
    manual_override_summary = fact.attrs.get("manual_override_summary", {})

    if not g.validate_columns(acc, g.ACCOUNT_REQUIRED_COLUMNS, "账号层日报"):
        return 1
    if not g.validate_columns(anc, g.ANCHOR_REQUIRED_COLUMNS, "主播层日报"):
        return 1
    if not g.validate_columns(fact, g.FACT_REQUIRED_COLUMNS, "事实表"):
        return 1

    lines = tsv_path.read_text(encoding="utf-8").splitlines()
    report_date = pd.to_datetime(report_date_str)
    month_start = pd.to_datetime(f"{report_date_str[:7]}-01")
    live_df = pd.read_excel(live_path) if live_path.exists() else pd.DataFrame()
    leads_source = load_leads_source(leads_path)
    deals_source = load_deals_source(deals_path)
    topline_config = load_topline_config(topline_config_path)
    topline_summary = build_topline_summary(
        fact=fact,
        live_df=live_df,
        leads_source=leads_source,
        deals_source=deals_source,
        report_date=report_date,
        config=topline_config,
    )

    errors: list[str] = []

    # 1) 顶部核心汇报区
    expected_top_lines = build_tsv_topline_lines(report_date_str, topline_summary)
    if "成交账号\t结果" not in lines:
        errors.append("缺少板块: 成交账号")
    else:
        top_end = lines.index("成交账号\t结果")
        got_top_lines = lines[:top_end]
        if got_top_lines != expected_top_lines:
            max_len = max(len(got_top_lines), len(expected_top_lines))
            for idx in range(max_len):
                got = got_top_lines[idx] if idx < len(got_top_lines) else "<缺失>"
                exp = expected_top_lines[idx] if idx < len(expected_top_lines) else "<多余>"
                if got != exp:
                    errors.append(f"顶部核心汇报区第{idx + 1}行不一致: expected={exp} got={got}")
                    break

    # 2) 成交账号 / 待交车账号 / 线索质量
    if "成交账号\t结果" in lines:
        idx = lines.index("成交账号\t结果")
        got_rows = [lines[idx + i] for i in range(1, 7)]
        got_lead_quality = lines[idx + 7]

        target_accounts = g.get_target_accounts(acc)
        day_target_deal, mtd_target_deal, mtd_all_deal = g.deal_accounts_text(
            fact=fact,
            report_date=report_date,
            month_start=month_start,
            target_accounts=target_accounts,
        )
        _, _, day_pending_acc, mtd_pending_acc, mtd_pending_all = g.pending_accounts_text(
            fact=fact,
            report_date=report_date,
            month_start=month_start,
            target_accounts=target_accounts,
            deals_source=deals_source,
            topline_config=topline_config,
        )
        lead_quality = g.lead_quality_text(
            fact=fact,
            live_df=live_df,
            report_date=report_date,
            month_start=month_start,
            live_file_label=(live_path.stem if live_path.exists() else "直播进度表"),
            manual_override_summary=manual_override_summary,
        )

        exp_rows = [
            f"当日成交账号（线索组目标账号）\t{day_target_deal}",
            f"累计成交账号（线索组目标账号）\t{mtd_target_deal}",
            f"累计成交账号（全量账号）\t{mtd_all_deal}",
            f"当日待交车账号（线索组目标账号）\t{day_pending_acc}",
            f"累计待交车账号（线索组目标账号）\t{mtd_pending_acc}",
            f"累计待交车账号（全量账号）\t{mtd_pending_all}",
        ]
        for i, (got, exp) in enumerate(zip(got_rows, exp_rows), start=1):
            if got != exp:
                errors.append(f"成交账号第{i}行不一致: expected={exp} got={got}")

        exp_lead_quality = f"线索质量口径\t{lead_quality}"
        if got_lead_quality != exp_lead_quality:
            errors.append(f"线索质量口径不一致: expected={exp_lead_quality} got={got_lead_quality}")

    # 3) 账号层（母集）逐单元复核
    try:
        got_acc_tsv = parse_section_df(lines, "账号层（母集）")
        exp_acc_tsv = g.account_table_tsv(acc, target_accounts=g.get_target_accounts(acc))
        errors.extend(assert_df_equal("账号层（母集）", exp_acc_tsv, got_acc_tsv))
    except Exception as exc:
        errors.append(f"账号层（母集）解析失败: {exc}")

    # 4) 到人层（子集）逐单元复核
    try:
        got_anc_tsv = parse_section_df(lines, "到人层（子集）")
        exp_anc_tsv = g.anchor_table_tsv(anc)
        errors.extend(assert_df_equal("到人层（子集）", exp_anc_tsv, got_anc_tsv))
    except Exception as exc:
        errors.append(f"到人层（子集）解析失败: {exc}")

    print(f"REPORT={report_date_str}")
    print(f"TSV={tsv_path}")
    if errors:
        print("STATUS=FAILED")
        for e in errors:
            print(f"ERR: {e}")
        return 1

    print("STATUS=PASSED")
    print("OK: 顶部核心汇报 / 成交与待交车账号 / 线索质量 / 账号层 / 到人层 全部一致")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
