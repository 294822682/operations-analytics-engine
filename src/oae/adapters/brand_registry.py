"""Minimal brand registry/profile metadata for optional adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


SEED_LEDGER_DISPLAY_NAME = "EXEED 星途台账"


DEFAULT_BRAND_REGISTRY: dict[str, Any] = {
    "registry_version": "brand-registry-v1",
    "adapters": {
        "exeed.seed_live": {
            "brand_key": "exeed",
            "label": "EXEED seed_live",
            "module": "oae.adapters.exeed.seed_live",
            "optional": True,
            "scope": "seed_live",
            "profiles": {
                "feishu_display": {
                    "kind": "feishu_display",
                    "config_path": "config/feishu_display_profile.json",
                    "profile": "default",
                },
                "feishu_topline": {
                    "kind": "feishu_topline",
                    "config_path": "config/feishu_topline_profile.json",
                    "profile": "default",
                }
            },
            "support_inputs": [
                {
                    "key": "seed_live_ledger",
                    "label": SEED_LEDGER_DISPLAY_NAME,
                    "required": False,
                    "resolver": "exeed.seed_live.latest_workbook",
                    "feature_scope": [
                        "exports.feishu_report.seed_panels",
                        "quality.feishu.seed_anchor_mtd_exposure",
                    ],
                    "degraded_reason": f"未找到 {SEED_LEDGER_DISPLAY_NAME}，种草曝光补充面板和对账会跳过",
                }
            ],
        }
    },
}


@dataclass(frozen=True)
class BrandProfileRef:
    key: str
    kind: str
    config_path: str
    profile: str

    def to_manifest_dict(self, workspace: Path) -> dict[str, object]:
        resolved = Path(self.config_path).expanduser()
        if not resolved.is_absolute():
            resolved = (workspace / resolved).resolve()
        return {
            "key": self.key,
            "kind": self.kind,
            "profile": self.profile,
            "config_path": str(resolved),
        }


@dataclass(frozen=True)
class BrandSupportInputSpec:
    key: str
    label: str
    required: bool
    resolver: str
    feature_scope: tuple[str, ...]
    degraded_reason: str


@dataclass(frozen=True)
class BrandAdapterSpec:
    key: str
    brand_key: str
    label: str
    module: str
    optional: bool
    scope: str
    profiles: tuple[BrandProfileRef, ...]
    support_inputs: tuple[BrandSupportInputSpec, ...]


@dataclass(frozen=True)
class BrandRegistry:
    config_path: Path
    source: str
    registry_version: str
    adapters: dict[str, BrandAdapterSpec]


BrandSupportResolver = Callable[[list[Path]], Path]


SUPPORT_INPUT_RESOLVERS: dict[str, BrandSupportResolver] = {
    "exeed.seed_live.latest_workbook": lambda search_dirs: _resolve_exeed_seed_live_workbook(search_dirs),
}


def default_brand_registry_path(workspace: Path) -> Path:
    return (workspace / "config" / "brand_registry.json").resolve()


def load_brand_registry(workspace: Path, config_path: Path | None = None) -> BrandRegistry:
    resolved_config = (config_path or default_brand_registry_path(workspace)).expanduser().resolve()
    raw = json.loads(json.dumps(DEFAULT_BRAND_REGISTRY))
    source = "built-in defaults"

    if resolved_config.exists():
        data = json.loads(resolved_config.read_text(encoding="utf-8"))
        _deep_merge_brand_registry(raw, data)
        source = str(resolved_config)

    adapters: dict[str, BrandAdapterSpec] = {}
    for key, item in dict(raw.get("adapters", {})).items():
        profiles = tuple(
            BrandProfileRef(
                key=str(profile_key),
                kind=str(values.get("kind", "")).strip(),
                config_path=str(values.get("config_path", "")).strip(),
                profile=str(values.get("profile", "")).strip(),
            )
            for profile_key, values in dict(item.get("profiles", {})).items()
            if isinstance(values, dict)
        )
        support_inputs = tuple(
            BrandSupportInputSpec(
                key=str(values.get("key", "")).strip(),
                label=str(values.get("label", "")).strip(),
                required=bool(values.get("required", False)),
                resolver=str(values.get("resolver", "")).strip(),
                feature_scope=tuple(str(scope).strip() for scope in values.get("feature_scope", []) if str(scope).strip()),
                degraded_reason=str(values.get("degraded_reason", "")).strip(),
            )
            for values in item.get("support_inputs", [])
            if isinstance(values, dict) and str(values.get("key", "")).strip()
        )
        adapters[str(key)] = BrandAdapterSpec(
            key=str(key),
            brand_key=str(item.get("brand_key", "")).strip(),
            label=str(item.get("label", key)).strip() or str(key),
            module=str(item.get("module", "")).strip(),
            optional=bool(item.get("optional", True)),
            scope=str(item.get("scope", "")).strip(),
            profiles=profiles,
            support_inputs=support_inputs,
        )

    return BrandRegistry(
        config_path=resolved_config,
        source=source,
        registry_version=str(raw.get("registry_version", "brand-registry-v1")).strip() or "brand-registry-v1",
        adapters=adapters,
    )


def discover_optional_brand_support_inputs(
    *,
    workspace: Path,
    input_manifest: dict[str, object],
    config_path: Path | None = None,
) -> list[dict[str, object]]:
    registry = load_brand_registry(workspace, config_path=config_path)
    dynamic_input_root = Path(str(input_manifest.get("dynamic_input_root", ""))).expanduser()
    search_dirs = _support_search_dirs(workspace, dynamic_input_root)
    records: list[dict[str, object]] = []

    for adapter in registry.adapters.values():
        if not adapter.optional:
            continue
        for support_input in adapter.support_inputs:
            records.append(
                _build_support_input_record(
                    workspace=workspace,
                    registry=registry,
                    adapter=adapter,
                    spec=support_input,
                    search_dirs=search_dirs,
                )
            )
    return records


def resolve_optional_brand_support_input_path(
    *,
    workspace: Path,
    input_manifest: dict[str, object],
    adapter_key: str,
    support_input_key: str,
    config_path: Path | None = None,
) -> Path | None:
    registry = load_brand_registry(workspace, config_path=config_path)
    adapter = registry.adapters.get(adapter_key)
    if adapter is None:
        return None
    dynamic_input_root = Path(str(input_manifest.get("dynamic_input_root", ""))).expanduser()
    search_dirs = _support_search_dirs(workspace, dynamic_input_root)
    for support_input in adapter.support_inputs:
        if support_input.key != support_input_key:
            continue
        return _resolve_support_input_path(support_input, search_dirs)
    return None


def _build_support_input_record(
    *,
    workspace: Path,
    registry: BrandRegistry,
    adapter: BrandAdapterSpec,
    spec: BrandSupportInputSpec,
    search_dirs: list[Path],
) -> dict[str, object]:
    path = _resolve_support_input_path(spec, search_dirs)
    record = {
        "key": spec.key,
        "label": spec.label,
        "required": spec.required,
        "feature_scope": list(spec.feature_scope),
        "brand_adapter_key": adapter.key,
        "brand_key": adapter.brand_key,
        "brand_label": adapter.label,
        "adapter_scope": adapter.scope,
        "adapter_module": adapter.module,
        "brand_profiles": [profile.to_manifest_dict(workspace) for profile in adapter.profiles],
        "brand_registry_source": registry.source,
        "brand_registry_version": registry.registry_version,
    }
    if path is None:
        return {
            **record,
            "status": "missing",
            "path": "",
            "degraded_reason": spec.degraded_reason or f"缺少 optional support input：{spec.label}",
        }
    return {
        **record,
        "status": "pass",
        "path": str(path),
        "filename": path.name,
    }


def _resolve_support_input_path(spec: BrandSupportInputSpec, search_dirs: list[Path]) -> Path | None:
    resolver = SUPPORT_INPUT_RESOLVERS.get(spec.resolver)
    if resolver is None:
        raise SystemExit(f"[ERROR] 未注册 optional support resolver：{spec.resolver}")
    try:
        return resolver(search_dirs)
    except FileNotFoundError:
        return None


def _support_search_dirs(workspace: Path, dynamic_input_root: Path) -> list[Path]:
    candidates = [dynamic_input_root, workspace / "源文件", workspace]
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        resolved = path.expanduser()
        key = str(resolved.resolve()) if resolved.exists() else str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _resolve_exeed_seed_live_workbook(search_dirs: list[Path]) -> Path:
    from oae.adapters.exeed.seed_live import pick_latest_seed_live_file

    return pick_latest_seed_live_file(search_dirs)


def _deep_merge_brand_registry(target: dict[str, Any], override: dict[str, Any]) -> None:
    if not isinstance(override, dict):
        return
    for key, value in override.items():
        if key == "adapters" and isinstance(value, dict):
            target.setdefault("adapters", {})
            for adapter_key, adapter_value in value.items():
                if not isinstance(adapter_value, dict):
                    continue
                existing = dict(target["adapters"].get(adapter_key, {}))
                merged = json.loads(json.dumps(existing))
                for adapter_field, adapter_field_value in adapter_value.items():
                    if adapter_field == "profiles" and isinstance(adapter_field_value, dict):
                        merged.setdefault("profiles", {})
                        for profile_key, profile_value in adapter_field_value.items():
                            if not isinstance(profile_value, dict):
                                continue
                            profile_existing = dict(merged["profiles"].get(profile_key, {}))
                            profile_existing.update(profile_value)
                            merged["profiles"][profile_key] = profile_existing
                    elif adapter_field == "support_inputs" and isinstance(adapter_field_value, list):
                        merged["support_inputs"] = adapter_field_value
                    else:
                        merged[adapter_field] = adapter_field_value
                target["adapters"][adapter_key] = merged
        else:
            target[key] = value
