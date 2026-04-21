"""Config-backed display profile for Feishu topline labels."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


DEFAULT_FEISHU_TOPLINE_PROFILES: dict[str, dict[str, str]] = {
    "default": {
        "full_account_label": "全量账号",
        "excluding_ex7_label": "不含 EX7",
        "ex7_label": "EX7 专项",
        "douyin_laike_label": "抖音-来客订单数",
    }
}


@dataclass(frozen=True)
class FeishuToplineProfile:
    full_account_label: str
    excluding_ex7_label: str
    ex7_label: str
    douyin_laike_label: str


def default_feishu_topline_profile_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "feishu_topline_profile.json"


def load_feishu_topline_profile(
    config_path: Path | None = None,
    *,
    profile: str = "",
) -> tuple[FeishuToplineProfile, str, str]:
    profiles = json.loads(json.dumps(DEFAULT_FEISHU_TOPLINE_PROFILES))
    selected_profile = profile or "default"
    source = "built-in defaults"

    if config_path is not None and config_path.exists():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if "profiles" in data:
            for name, values in data["profiles"].items():
                merged = json.loads(json.dumps(DEFAULT_FEISHU_TOPLINE_PROFILES.get("default", {})))
                if isinstance(values, dict):
                    merged.update({str(key): str(value) for key, value in values.items()})
                profiles[str(name)] = merged
            selected_profile = profile or str(data.get("profile", selected_profile or "default"))
        elif isinstance(data, dict):
            merged = json.loads(json.dumps(DEFAULT_FEISHU_TOPLINE_PROFILES.get("default", {})))
            merged.update({str(key): str(value) for key, value in data.items()})
            profiles["default"] = merged
            selected_profile = profile or "default"
        source = str(config_path)

    if selected_profile not in profiles:
        selected_profile = "default"

    raw = profiles[selected_profile]
    resolved = FeishuToplineProfile(
        full_account_label=str(raw.get("full_account_label", DEFAULT_FEISHU_TOPLINE_PROFILES["default"]["full_account_label"])),
        excluding_ex7_label=str(raw.get("excluding_ex7_label", DEFAULT_FEISHU_TOPLINE_PROFILES["default"]["excluding_ex7_label"])),
        ex7_label=str(raw.get("ex7_label", DEFAULT_FEISHU_TOPLINE_PROFILES["default"]["ex7_label"])),
        douyin_laike_label=str(raw.get("douyin_laike_label", DEFAULT_FEISHU_TOPLINE_PROFILES["default"]["douyin_laike_label"])),
    )
    return resolved, source, selected_profile
