#!/usr/bin/env python3
"""Read-only GS-111 deterministic retrieval acceptance.

This script never invokes a model and never writes to the mathematics repo.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    Candidate,
    build_math_knowledge_snapshot,
    retrieve_math_relationship_context,
)


EXPECTED_COMPARISON = {"GS-110", "GS-112", "GS-119"}
REJECTED_TOP_FIVE = {"GS-143", "GS-464", "GS-499", "GS-531", "GS-539"}


def signal(index: int, signal_type: str, term: str) -> dict[str, object]:
    return {
        "signal_id": f"SIG-GS111-{index:02d}",
        "signal_type": signal_type,
        "canonical_term": term,
        "surface_form": term,
        "importance": "primary",
        "provenance": "formal_card",
        "evidence_refs": ["golden.gs111"],
        "confidence": "high",
        "error_role": "none",
        "specificity": term,
        "applicability_boundary": "GS-111 冻结正式题源",
        "truth_library_match_status": "exact",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo",
        default="/Users/xiazhibin/Documents/kaoyan-math",
    )
    args = parser.parse_args()
    repo = Path(args.repo).expanduser().resolve()
    snapshot_config = {
        "enabled": True,
        "max_source_bytes": 8 * 1024 * 1024,
        "max_distribution_terms": 64,
        "max_local_neighbors": 8,
        "max_relationship_candidates": 5,
        "golden_regressions": {
            "GS-111": sorted(EXPECTED_COMPARISON),
        },
        "max_snapshot_bytes": 128 * 1024,
        "sources": {
            "graph": "错题知识网络/生成/知识网络图.mmd",
            "projection": "错题知识网络/生成/wrong_questions.json",
            "taxonomy": "错题知识网络/知识点库.md",
            "relationship_policy": (
                "错题知识网络/schema/relationship_signal_policy.json"
            ),
        },
    }
    snapshot, private = build_math_knowledge_snapshot(
        repo, snapshot_config, formal_id="GS-111"
    )
    atomic_signals = [
        signal(1, "knowledge", "高阶导数"),
        signal(2, "method", "部分分式拆分"),
        signal(3, "question_type", "有理函数部分分式高阶导数"),
        signal(4, "trap", "第二问对 x 偏导时 y^2 是常数"),
    ]
    analysis = {"atomic_signals": atomic_signals}
    candidate = Candidate(
        subject="math",
        capture_id="GS-111",
        study_date="2026-08-06",
        recorded_at=None,
        input_fingerprint="f" * 64,
        input_binding={},
        model_input={
            "target_identity": {"formal_card_id": "GS-111"},
            "knowledge_distribution_snapshot": snapshot,
        },
        allowed_evidence_refs=("golden.gs111",),
        image_paths=(),
        target_label="GS-111",
        canonical_state="read_only_golden",
        sol_state="not_applicable",
        private_context=private,
    )
    context = retrieve_math_relationship_context(
        candidate,
        analysis,
        {"math_knowledge_snapshot": snapshot_config},
    )
    selected = [str(row["candidate_id"]) for row in context["candidates"]]
    comparison = {
        str(row["candidate_id"]) for row in context["comparison_pool"]
    }
    failures: list[str] = []
    if len(selected) > 5:
        failures.append("more_than_five_candidates")
    if REJECTED_TOP_FIVE & set(selected):
        failures.append("irrelevant_candidates_still_selected")
    if not EXPECTED_COMPARISON.issubset(comparison):
        failures.append("expected_comparison_candidates_missing")
    if context["retrieval_diagnosis"]["status"] != "complete":
        failures.append("retrieval_diagnosis_failed")
    if snapshot["coverage_manifest"]["complete"] is not True:
        failures.append("coverage_incomplete")
    if not any(
        row["signal_type"] == "trap"
        and "y^2 是常数" in str(row["canonical_term"])
        for row in atomic_signals
    ):
        failures.append("fixed_y_semantics_missing")
    result = {
        "schema_version": "study-intake-gs111-golden-acceptance-v1",
        "status": "pass" if not failures else "fail",
        "source_set_sha256": snapshot["source_set_sha256"],
        "selected_candidate_ids": selected,
        "expected_comparison_ids": sorted(EXPECTED_COMPARISON),
        "expected_comparison_ids_present": sorted(
            EXPECTED_COMPARISON & comparison
        ),
        "rejected_top_five_ids": sorted(REJECTED_TOP_FIVE),
        "fixed_y_semantics": "confirmed_from_atomic_signal",
        "coverage_complete": snapshot["coverage_manifest"]["complete"],
        "model_call_count": 0,
        "formal_write_count": 0,
        "failures": failures,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
