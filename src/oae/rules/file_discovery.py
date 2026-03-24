"""Input discovery rules for date-stamped source files."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path


def split_patterns(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def parse_date_from_filename(path: Path) -> date | None:
    matched = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if not matched:
        return None
    try:
        return datetime.strptime(matched.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_year_month_from_live_filename(path: Path) -> tuple[int, int] | None:
    matched = re.search(r"(20\d{2})年.*?([01]?\d)月", path.name)
    if not matched:
        return None
    try:
        year = int(matched.group(1))
        month = int(matched.group(2))
    except ValueError:
        return None
    if 1 <= month <= 12:
        return year, month
    return None


def pick_latest_file(data_dir: Path, pattern_expr: str) -> Path:
    matched: list[Path] = []
    for pattern in split_patterns(pattern_expr):
        matched.extend(data_dir.glob(pattern))
    unique = sorted(set(path.resolve() for path in matched))
    if not unique:
        raise FileNotFoundError(f"目录 {data_dir} 下未找到文件: {pattern_expr}")

    def sort_key(path: Path) -> tuple[date, float]:
        return parse_date_from_filename(path) or date.min, path.stat().st_mtime

    return max(unique, key=sort_key)


def pick_latest_live_file(search_dir_or_dirs: Path | list[Path]) -> Path:
    patterns = ["*直播进度表*月.xlsx", "*直播进度表*月.xls", "*直播进度表*.xlsx", "*直播进度表*.xls"]
    search_dirs = (
        [search_dir_or_dirs]
        if isinstance(search_dir_or_dirs, Path)
        else [path for path in search_dir_or_dirs if isinstance(path, Path)]
    )
    matched: list[Path] = []
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for pattern in patterns:
            matched.extend(path.resolve() for path in search_dir.glob(pattern))
    unique = sorted(set(matched))
    if not unique:
        raise FileNotFoundError("未找到可用直播进度表文件")

    def sort_key(path: Path) -> tuple[tuple[int, int], date, float]:
        year_month = parse_year_month_from_live_filename(path)
        parsed_date = parse_date_from_filename(path) or date.min
        ym_key = year_month if year_month is not None else (parsed_date.year, parsed_date.month)
        return ym_key, parsed_date, path.stat().st_mtime

    return max(unique, key=sort_key)
