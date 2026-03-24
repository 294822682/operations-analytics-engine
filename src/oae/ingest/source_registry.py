"""Load formal input contracts from config."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class InputSourceContract:
    key: str
    label: str
    kind: str
    directory: str
    path: str
    glob_patterns: list[str]
    naming_regex: str
    naming_exact: str
    file_types: list[str]
    business_date_type: str
    allow_multiple_versions: bool
    selection_rule: str
    required_alias_keys: list[str]
    required_exact_fields: list[str]
    optional_alias_keys: list[str]
    recommended_exact_fields: list[str]
    preferred_sheets: list[str]


@dataclass(frozen=True)
class InputRegistry:
    config_path: Path
    default_dynamic_input_dir: str
    sources: dict[str, InputSourceContract]


def load_input_registry(workspace: Path, config_path: Path | None = None) -> InputRegistry:
    resolved_config = (config_path or (workspace / "config" / "input_sources.json")).expanduser().resolve()
    if not resolved_config.exists():
        raise SystemExit(f"[ERROR] 输入契约配置不存在：{resolved_config}")

    try:
        raw = json.loads(resolved_config.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"[ERROR] 输入契约配置解析失败：{resolved_config}，err={exc}") from exc

    default_dynamic_dir = str(raw.get("default_dynamic_input_dir", "源文件")).strip() or "源文件"
    sources_raw = raw.get("sources")
    if not isinstance(sources_raw, dict) or not sources_raw:
        raise SystemExit(f"[ERROR] 输入契约配置缺少 sources：{resolved_config}")

    sources: dict[str, InputSourceContract] = {}
    for key, item in sources_raw.items():
        if not isinstance(item, dict):
            raise SystemExit(f"[ERROR] 输入契约配置非法：sources.{key} 必须是对象")
        sources[key] = InputSourceContract(
            key=key,
            label=str(item.get("label", key)).strip() or key,
            kind=str(item.get("kind", "")).strip().lower(),
            directory=str(item.get("directory", default_dynamic_dir)).strip(),
            path=str(item.get("path", "")).strip(),
            glob_patterns=[str(part).strip() for part in item.get("glob_patterns", []) if str(part).strip()],
            naming_regex=str(item.get("naming_regex", "")).strip(),
            naming_exact=str(item.get("naming_exact", "")).strip(),
            file_types=[str(part).strip().lower() for part in item.get("file_types", []) if str(part).strip()],
            business_date_type=str(item.get("business_date_type", "none")).strip().lower(),
            allow_multiple_versions=bool(item.get("allow_multiple_versions", False)),
            selection_rule=str(item.get("selection_rule", "")).strip(),
            required_alias_keys=[str(part).strip() for part in item.get("required_alias_keys", []) if str(part).strip()],
            required_exact_fields=[str(part).strip() for part in item.get("required_exact_fields", []) if str(part).strip()],
            optional_alias_keys=[str(part).strip() for part in item.get("optional_alias_keys", []) if str(part).strip()],
            recommended_exact_fields=[str(part).strip() for part in item.get("recommended_exact_fields", []) if str(part).strip()],
            preferred_sheets=[str(part).strip() for part in item.get("preferred_sheets", []) if str(part).strip()],
        )

    return InputRegistry(
        config_path=resolved_config,
        default_dynamic_input_dir=default_dynamic_dir,
        sources=sources,
    )
