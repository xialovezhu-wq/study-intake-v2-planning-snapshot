#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import math
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "数学一回滚复习系统" / "scripts" / "quick_intake.py"
SPEC = importlib.util.spec_from_file_location("quick_intake_benchmark", SCRIPT_PATH)
assert SPEC and SPEC.loader
quick_intake = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(quick_intake)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * quantile) - 1)
    return ordered[index]


def payload(attempt: int) -> dict:
    return {
        "schema_version": quick_intake.CAPTURE_SCHEMA,
        "attempt_id": f"benchmark:GS-999:attempt-{attempt}",
        "study_date": "2026-07-18",
        "target": {
            "kind": "formal_card",
            "formal_id": "GS-999",
            "source_locator": None,
            "source_hash_before": None,
        },
        "score_event_id": None,
        "requested_action": "record_wrong",
        "thread_ref": "isolated-benchmark",
        "evidence": {
            "result": "wrong",
            "user_facts": [
                {
                    "text": "隔离基准中的真实用户证据占位，不进入正式仓库",
                    "origin": "user_observed",
                }
            ],
            "independent_correct_steps": [],
            "first_break": {
                "kind": "method_trigger",
                "text": "未独立触发第一动作",
                "origin": "user_confirmed",
            },
            "later_breaks": [],
            "hints_needed": [],
            "self_corrections": [],
            "mastery_score": 2,
            "mastery_source": "benchmark_fixture",
            "score_basis": {
                "text": "隔离性能基准",
                "origin": "source_verified",
            },
            "unresolved": [],
        },
    }


def run(iterations: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="math-quick-intake-benchmark-") as temp_name:
        base = Path(temp_name)
        rollback = base / "数学一回滚复习系统"
        cards = base / "错题知识网络" / "错题卡"
        rollback.mkdir(parents=True)
        cards.mkdir(parents=True)
        card = cards / "GS-999_fixture.md"
        card.write_text("---\nid: GS-999\ntitle: fixture\n---\n", encoding="utf-8")
        review_log = rollback / "复习记录.jsonl"
        review_log.write_text("", encoding="utf-8")

        quick_intake.ROOT = rollback
        quick_intake.REPO_ROOT = base
        quick_intake.EVENTS_PATH = rollback / "快速入库事件.jsonl"
        quick_intake.LOCK_PATH = rollback / ".快速入库事件.jsonl.lock"
        quick_intake.REVIEW_LOG_PATH = review_log
        quick_intake.CARDS_DIR = cards

        durations: list[float] = []
        writer_elapsed: list[float] = []
        for index in range(iterations):
            payload_path = base / "payload.json"
            payload_path.write_text(
                json.dumps(payload(index), ensure_ascii=False),
                encoding="utf-8",
            )
            output = io.StringIO()
            started = time.perf_counter()
            with contextlib.redirect_stdout(output):
                quick_intake.cmd_record(argparse.Namespace(payload_file=str(payload_path)))
            durations.append((time.perf_counter() - started) * 1000)
            writer_elapsed.append(json.loads(output.getvalue())["elapsed_ms"])

        events = quick_intake.load_jsonl(quick_intake.EVENTS_PATH)
        return {
            "schema_version": "math-fast-intake-benchmark-v1",
            "benchmark_scope": "deterministic_writer_only",
            "excludes": [
                "model evidence synthesis",
                "warmup scoring",
                "source attachment staging",
                "temporary payload creation",
                "nightly freeze and closeout",
            ],
            "iterations": iterations,
            "event_count": len(events),
            "payload_bytes": quick_intake.EVENTS_PATH.stat().st_size,
            "command_ms": {
                "p50": round(percentile(durations, 0.50), 3),
                "p95": round(percentile(durations, 0.95), 3),
                "max": round(max(durations), 3),
            },
            "writer_reported_ms": {
                "p50": round(percentile(writer_elapsed, 0.50), 3),
                "p95": round(percentile(writer_elapsed, 0.95), 3),
                "max": round(max(writer_elapsed), 3),
            },
            "ledger_hash": hashlib.sha256(quick_intake.EVENTS_PATH.read_bytes()).hexdigest(),
            "isolated": True,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="隔离运行快速入库 writer 延迟基准")
    parser.add_argument("--iterations", type=int, default=50)
    args = parser.parse_args()
    if args.iterations < 1:
        raise SystemExit("iterations 必须大于 0")
    print(json.dumps(run(args.iterations), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
