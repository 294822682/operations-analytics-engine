"""Explicit registry and helpers for themes that still require raw evidence."""

from __future__ import annotations

import json
from pathlib import Path


RAW_EVIDENCE_TOPICS = [
    {
        "topic": "process_sla",
        "raw_evidence_required": True,
        "migration_status": "partially_unified_capable",
        "shared_components": ["rate_within", "SLA table assembly", "渠道聚合规则"],
        "unified_capable_parts": ["SLA总览汇总口径", "渠道级时效统计结构"],
        "raw_required_parts": ["创建/下发/首次跟进/到店 多节点原始时间链"],
        "reason": "SLA 汇总结构已可模块化，但完整过程链仍依赖原始时间字段。",
    },
    {
        "topic": "reactivation",
        "raw_evidence_required": True,
        "migration_status": "fully_raw_required",
        "shared_components": ["手机号标准化", "原始时间解析"],
        "unified_capable_parts": [],
        "raw_required_parts": ["战败时间", "战败状态", "再入池事件回放"],
        "reason": "再激活识别依赖同手机号完整状态回放，统一事实层当前未保留战败后再激活事件链。",
    },
    {
        "topic": "time_anomaly",
        "raw_evidence_required": True,
        "migration_status": "migration_candidate",
        "shared_components": ["异常聚合", "责任归属汇总", "风险等级规则"],
        "unified_capable_parts": ["异常聚合汇总", "风险等级分层"],
        "raw_required_parts": ["逐节点原始时间字段", "异常根因定位"],
        "reason": "异常责任与风险分层逻辑可复用，但逐节点异常识别仍依赖原始时间链。",
    },
    {
        "topic": "model_path",
        "raw_evidence_required": True,
        "migration_status": "partially_unified_capable",
        "shared_components": ["路径稳定率", "跳转率", "流失率计算"],
        "unified_capable_parts": ["车型路径总览和总体指标框架"],
        "raw_required_parts": ["首次意向车型", "试驾车型", "成交车型 的完整路径字段"],
        "reason": "车型路径算法已可抽离，但统一事实层尚未沉淀完整车型演进链。",
    },
    {
        "topic": "followup_intensity",
        "raw_evidence_required": True,
        "migration_status": "partially_unified_capable",
        "shared_components": ["跟进强度分桶", "转化率汇总"],
        "unified_capable_parts": ["分桶规则和结果表结构"],
        "raw_required_parts": ["跟进次数", "销售维度后端字段"],
        "reason": "分桶和聚合逻辑可复用，但原始跟进次数和销售字段尚未进入统一事实层。",
    },
]


def summarize_raw_evidence_topics() -> dict[str, list[dict[str, object]]]:
    groups = {
        "fully_raw_required": [],
        "partially_unified_capable": [],
        "migration_candidate": [],
    }
    for item in RAW_EVIDENCE_TOPICS:
        groups.setdefault(str(item["migration_status"]), []).append(item)
    return groups


def write_raw_evidence_manifest(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "raw_evidence_topics.json"
    path.write_text(
        json.dumps(
            {
                "topics": RAW_EVIDENCE_TOPICS,
                "groups": summarize_raw_evidence_topics(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
