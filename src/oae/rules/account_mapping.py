"""Canonical account mapping shared by fact, reporting, and analysis layers."""

from __future__ import annotations

from .common import normalize_text


ACCOUNT_MAP = {
    "抖音-星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "抖音-星途星纪元": "抖音-星途极速拍档",
    "星途汽车直播营销中心": "抖音-星途汽车直播营销中心",
    "EXEED星途": "抖音-EXEED星途",
    "抖音来客直播": "抖音-星途汽车官方直播间",
}

DOUYIN_LAIKE_CHANNEL3_MAP = {
    "抖音-星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "星途星纪元直播营销中心": "抖音-星途汽车官方直播间",
    "抖音-星途汽车官方直播间": "抖音-星途汽车直营中心",
    "星途汽车官方直播间": "抖音-星途汽车直营中心",
}

NON_LIVE_ACCOUNTS = {
    "抖音来客直播",
    "抖店",
    "视频号-星途星纪元",
    "快手-EXEED星途",
    "快手-星途星纪元",
}


def normalize_account(value: object) -> str:
    raw = normalize_text(value).replace("（", "(").replace("）", ")")
    raw = raw.replace("—", "-").replace("－", "-").replace("_", "-")
    return ACCOUNT_MAP.get(raw, raw)


def remap_douyin_laike_channel3(value: object) -> str:
    raw = normalize_text(value).replace("（", "(").replace("）", ")")
    raw = raw.replace("—", "-").replace("－", "-").replace("_", "-")
    return DOUYIN_LAIKE_CHANNEL3_MAP.get(raw, raw)


def canonical_account_name(value: object) -> str:
    text = normalize_account(value)
    if not text:
        return ""
    text = "".join(text.split())
    for prefix in [
        "抖音-",
        "抖音",
        "快手-",
        "快手",
        "视频号-",
        "视频号",
        "微信视频号-",
        "微信视频号",
    ]:
        if text.startswith(prefix):
            return text[len(prefix) :]
    return text
