from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List

from oae.services.release_gate_logic import (
    default_gate_suites,
    evaluate_gate_run,
    gate_profile_choices,
    load_release_candidate_evidence,
    resolve_gate_profile,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一执行工程门禁与发布候选证据检查")
    parser.add_argument("--workspace", default=".", help="仓库根目录")
    parser.add_argument(
        "--gate-profile",
        default="pr",
        choices=gate_profile_choices(),
        help="门禁 profile：pr=工程门禁，release=工程门禁+发布候选 ready",
    )
    parser.add_argument("--skip-full-pytest", action="store_true", help="只跑 targeted suites，不跑全仓 pytest")
    parser.add_argument(
        "--strict-release-ready",
        action="store_true",
        help="除测试通过外，还要求最新发布候选状态为 ready，否则返回非零退出码",
    )
    parser.add_argument("--json-out", default="", help="可选：把门禁结果写入 JSON 文件")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.workspace).expanduser().resolve()
    gate_profile = resolve_gate_profile(args.gate_profile)
    include_full_pytest = gate_profile.include_full_pytest and not args.skip_full_pytest
    strict_release_ready = gate_profile.strict_release_ready or args.strict_release_ready
    suite_results = run_gate_suites(repo_root, include_full_pytest=include_full_pytest)
    release_candidate = load_release_candidate_evidence(repo_root)
    evaluation = evaluate_gate_run(
        suite_results=suite_results,
        release_candidate=release_candidate,
        strict_release_ready=strict_release_ready,
    )
    payload = {
        "workspace": str(repo_root),
        "gate_profile": gate_profile.key,
        "gate_profile_description": gate_profile.description,
        "include_full_pytest": include_full_pytest,
        "strict_release_ready": strict_release_ready,
        "suites": suite_results,
        "release_candidate": release_candidate,
        "evaluation": evaluation,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.json_out:
        output_path = Path(args.json_out).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return int(evaluation["exit_code"])


def run_gate_suites(repo_root: Path, *, include_full_pytest: bool) -> List[Dict[str, Any]]:
    env = os.environ.copy()
    pythonpath_parts = [str(repo_root), str(repo_root / "src")]
    if env.get("PYTHONPATH"):
        pythonpath_parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    results: List[Dict[str, Any]] = []
    for suite in default_gate_suites(include_full_pytest=include_full_pytest):
        command = [sys.executable, "-m", "pytest", *suite.pytest_args]
        started = perf_counter()
        process = subprocess.run(
            command,
            cwd=repo_root,
            env=env,
            check=False,
        )
        duration_seconds = round(perf_counter() - started, 3)
        results.append(
            {
                "key": suite.key,
                "description": suite.description,
                "command": command,
                "returncode": process.returncode,
                "duration_seconds": duration_seconds,
                "passed": process.returncode == 0,
            }
        )
        if process.returncode != 0:
            break
    return results


if __name__ == "__main__":
    raise SystemExit(main())
