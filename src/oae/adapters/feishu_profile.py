"""Config-backed Feishu display profile."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_FEISHU_DISPLAY_PROFILES: dict[str, dict[str, Any]] = {
    "default": {
        "seed_account_order": ["抖音-EXEED星途"],
        "display_account_order": ["抖音-星途汽车官方直播间", "抖音-星途汽车直播营销中心", "抖音-星途汽车直营中心"],
        "account_label_map": {
            "抖音-EXEED星途": "EXEED星途",
            "抖音-星途汽车官方直播间": "星途汽车官方直播间",
            "抖音-星途汽车直播营销中心": "星途汽车直播营销中心",
            "抖音-星途汽车直营中心": "星途汽车直营中心",
            "快手-EXEED星途": "快手-EXEED星途汽车",
            "快手-EXEED星途汽车": "快手-EXEED星途汽车",
            "线索组汇总": "线索组汇总",
        },
        "seed_anchor_order": ["王雪", "刘花旗", "桂婕", "岳浩然"],
        "anchor_order": ["丁俐佳", "孙慧敏", "何雯", "徐幻", "侯翩翩", "王馨", "曹嘉洋", "徐欣悦"],
    }
}


@dataclass(frozen=True)
class FeishuDisplayProfile:
    seed_account_order: tuple[str, ...]
    display_account_order: tuple[str, ...]
    account_label_map: dict[str, str]
    seed_anchor_order: tuple[str, ...]
    anchor_order: tuple[str, ...]


def default_feishu_display_profile_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "feishu_display_profile.json"


def load_feishu_display_profile(
    config_path: Path | None = None,
    *,
    profile: str = "",
) -> tuple[FeishuDisplayProfile, str, str]:
    profiles = json.loads(json.dumps(DEFAULT_FEISHU_DISPLAY_PROFILES))
    selected_profile = profile or "default"
    source = "built-in defaults"

    if config_path is not None and config_path.exists():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if "profiles" in data:
            for name, values in data["profiles"].items():
                merged = json.loads(json.dumps(DEFAULT_FEISHU_DISPLAY_PROFILES.get("default", {})))
                if isinstance(values, dict):
                    merged.update(values)
                profiles[str(name)] = merged
            selected_profile = profile or str(data.get("profile", selected_profile or "default"))
        elif isinstance(data, dict):
            merged = json.loads(json.dumps(DEFAULT_FEISHU_DISPLAY_PROFILES.get("default", {})))
            merged.update(data)
            profiles["default"] = merged
            selected_profile = profile or "default"
        source = str(config_path)

    if selected_profile not in profiles:
        selected_profile = "default"

    raw = profiles[selected_profile]
    resolved = FeishuDisplayProfile(
        seed_account_order=tuple(str(item) for item in raw.get("seed_account_order", [])),
        display_account_order=tuple(str(item) for item in raw.get("display_account_order", [])),
        account_label_map={str(key): str(value) for key, value in dict(raw.get("account_label_map", {})).items()},
        seed_anchor_order=tuple(str(item) for item in raw.get("seed_anchor_order", [])),
        anchor_order=tuple(str(item) for item in raw.get("anchor_order", [])),
    )
    return resolved, source, selected_profile
