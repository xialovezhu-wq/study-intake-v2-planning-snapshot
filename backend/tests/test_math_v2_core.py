from __future__ import annotations

import copy
import base64
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import math_shadow_replay as replay  # noqa: E402
from historical_test_input import (  # noqa: E402
    HistoricalTestInput,
    render_shadow_test_config,
)
from concurrent_dispatch import ConcurrentDispatcher, LeaseStore  # noqa: E402
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateSubprocessRunner,
    scan_eligible_candidates,
)
from preprocessor_core import (  # noqa: E402
    ANALYSIS_SCHEMA,
    Candidate,
    CodexRunner,
    ModelResult,
    PreprocessorError,
    StructuredStageResult,
    Worker,
    _analysis_review_refs,
    _canonicalize_math_image_provenance,
    _coalesce_math_split_correction_resolutions,
    _normalize_math_correction_delta_wrappers,
    _normalize_math_model_transport,
    _normalize_math_model_text_transport,
    _math_writable_correction_paths,
    _receipt_id,
    atomic_publish_json_no_clobber,
    atomic_write_json,
    build_math_mcp_relationship_context,
    build_math_knowledge_snapshot,
    consume_package,
    decode_correction_delta,
    load_config,
    load_json,
    normalize_math_format_warnings,
    retrieve_math_relationship_context,
    sha256_file,
    sha256_value,
    verify_math_knowledge_snapshot_sources,
    validate_math_analysis_v2,
    validate_math_critical_review_v2,
    validate_math_performance_budget,
    validate_math_text_integrity,
    validate_math_three_replay_p95,
)


def legacy_analysis(ref: str) -> dict:
    claim = {"text": "只读候选", "evidence_refs": [ref], "confidence": "high"}
    return {
        "schema_version": ANALYSIS_SCHEMA,
        "summary": "保留 v1 基线处理结果。",
        "independent_correct_steps": [claim],
        "first_break": {
            "kind": "method_trigger",
            "text": "第一断点候选",
            "evidence_refs": [ref],
            "confidence": "medium",
        },
        "later_breaks": [],
        "hint_dependencies": [],
        "contradictions": [],
        "unresolved": [],
        "candidate_updates": [],
        "nightly_checks": [claim],
        "risk_flags": [],
    }


def math_claim(ref: str) -> dict:
    return {
        "claim_type": "evidence_bound_inference",
        "text": "当前结论只绑定冻结证据，保留对象角色、条件边界与提示依赖，必须由 Sol 重开原证据核验。",
        "provenance": "capture",
        "evidence_refs": [ref],
        "confidence": "medium",
        "counterevidence_or_boundary": "若冻结证据或身份映射冲突，则不得采用该候选。",
        "sol_verification_action": "逐项核对冻结证据、正式目标、交付目标和推理顺序。",
    }


def publish_fake_raw_refs(
    runner: CodexRunner, *, stage_name: str, raw_output: bytes
) -> None:
    """Complete the raw-first side effect of a mocked subprocess call."""

    digest = hashlib.sha256(raw_output).hexdigest()
    runner._provider_raw_refs[stage_name] = {
        "raw_output_object_sha256": digest,
        "raw_output_object_ref": f"test-raw://sha256/{digest}",
    }


def math_analysis(ref: str) -> dict:
    claim = math_claim(ref)
    formal_fields = {
        key: (
            [copy.deepcopy(claim)]
            if key in {
                "safe_summary", "question_body", "source_and_answer",
                "wrong_point", "methods",
            }
            else []
        )
        for key in (
            "safe_summary", "question_body", "source_and_answer",
            "independent_performance", "wrong_point", "error_causes",
            "methods", "traps", "method_gap", "wrong_history",
            "mastery_evidence", "relationship_proposals",
        )
    }


    return {
        "schema_version": "study-intake-luna-math-analysis-v2",
        "report_profile": "math_deep",
        "executive_summary": "证据有限，本报告只保留冻结事实、证据约束推断、明确缺口和 Sol 核验动作。",
        "target_identity": {
            "formal_target": [copy.deepcopy(claim)],
            "delivered_target": [copy.deepcopy(claim)],
            "knowledge_fallback_anchor": [],
            "identity_boundary": [copy.deepcopy(claim)],
        },
        "question_structure": {
            "objects": [copy.deepcopy(claim)],
            "conditions": [copy.deepcopy(claim)],
            "asked_task": [copy.deepcopy(claim)],
            "source_answer": [copy.deepcopy(claim)],
        },
        "correct_reasoning_reconstruction": [copy.deepcopy(claim)],
        "evidence_assessment": {
            "completeness": "limited_by_evidence",
            "evidence_inventory": [copy.deepcopy(claim)],
            "observed_facts": [copy.deepcopy(claim)],
            "inferences": [copy.deepcopy(claim)],
            "contradictions": [],
            "gaps": [copy.deepcopy(claim)],
        },
        "reasoning_diagnosis": {
            "independent_correct_steps": [copy.deepcopy(claim)],
            "first_break": copy.deepcopy(claim),
            "later_breaks": [],
            "hint_dependencies": [copy.deepcopy(claim)],
            "self_corrections": [],
            "history_merge": [copy.deepcopy(claim)],
        },
        "knowledge_error_signatures": {
            "primary_knowledge": [copy.deepcopy(claim)],
            "secondary_knowledge": [],
            "methods": [copy.deepcopy(claim)],
            "first_error": copy.deepcopy(claim),
            "later_errors": [],
            "historical_errors": [],
            "current_errors": [],
            "relationship_search_intents": [copy.deepcopy(claim)],
        },
        "concept_method_analysis": {
            "mechanisms": [copy.deepcopy(claim)],
            "method_triggers": [copy.deepcopy(claim)],
            "applicability_conditions": [copy.deepcopy(claim)],
            "boundary_conditions": [copy.deepcopy(claim)],
            "common_confusions": [copy.deepcopy(claim)],
            "transfer_risks": [copy.deepcopy(claim)],
        },
        "formalization_candidates": formal_fields,
        "sol_verification_plan": {
            "must_verify": [copy.deepcopy(claim)],
            "reject_if": [copy.deepcopy(claim)],
            "source_checks": [copy.deepcopy(claim)],
            "identity_checks": [copy.deepcopy(claim)],
            "recommended_disposition": "insufficient_evidence",
        },
        "risk_flags": [],
        "unresolved": [copy.deepcopy(claim)],
        "atomic_signals": [
            {
                "signal_id": "SIG-MATH-0001",
                "signal_type": "knowledge",
                "canonical_term": "高阶导数",
                "surface_form": "高阶导数",
                "importance": "primary",
                "provenance": "capture",
                "evidence_refs": [ref],
                "confidence": "high",
                "error_role": "none",
                "specificity": "具体高阶导数知识信号",
                "applicability_boundary": "仅当前冻结题源",
                "truth_library_match_status": "exact",
            },
            {
                "signal_id": "SIG-MATH-0002",
                "signal_type": "method",
                "canonical_term": "部分分式",
                "surface_form": "部分分式",
                "importance": "primary",
                "provenance": "capture",
                "evidence_refs": [ref],
                "confidence": "high",
                "error_role": "none",
                "specificity": "具体代数拆分方法",
                "applicability_boundary": "仅有理函数可拆分时",
                "truth_library_match_status": "exact",
            },
        ],
    }


def model_driven_math_validator_fixture(
    ref: str = "capture.event_id",
) -> tuple[Candidate, SimpleNamespace]:
    """Build a validator fixture with MCP-derived relationship context only."""

    candidate = Candidate(
        subject="math",
        capture_id="MFI-CAP-VALIDATOR-00000001",
        study_date="2026-08-05",
        recorded_at="2026-08-05T00:01:00Z",
        input_fingerprint="1" * 64,
        input_binding={"image_evidence_refs": []},
        model_input={"source_bundle": None},
        allowed_evidence_refs=(ref,),
        image_paths=(),
        target_label="validator-only",
        canonical_state="captured",
        sol_state="pending_review",
        private_context=None,
    )
    draft = math_analysis(ref)
    review = {
        "schema_version": "study-intake-luna-math-critical-review-v2",
        "verdict": "pass",
        "summary": "独立批判审查完成。",
        "revised_analysis": copy.deepcopy(draft),
        "relationship_decisions": [],
        "unsupported_claims": [],
        "evidence_misreads": [],
        "mathematical_errors": [],
        "visual_findings": [],
        "provenance_findings": [],
        "missing_analysis": [],
        "required_corrections": [],
        "sol_priority_checks": [],
        "correction_resolutions": [],
    }
    relationship_call = {
        "arguments": {
            "collection": "relationships",
            "page_size": 24,
            "query": None,
            "ids": [],
            "cursor": None,
        },
        "result": {"items": [], "complete": True, "truncated": False},
        "result_sha256": "2" * 64,
    }
    stage = StructuredStageResult(
        payload=copy.deepcopy(draft),
        duration_ms=1,
        runtime_model=None,
        runtime_reasoning_effort=None,
        runtime_metadata_provenance="unavailable",
        runtime_identity_status="requested_unverified",
        output_sha256="3" * 64,
        mcp_transcript_sha256="4" * 64,
        mcp_calls=(relationship_call,),
    )
    relationship_context = build_math_mcp_relationship_context(
        (stage,), draft_analysis=draft
    )
    return candidate, SimpleNamespace(
        draft_analysis=draft,
        critical_review=review,
        relationship_context=relationship_context,
    )


class FakeMathRunner:
    def __init__(self, *, confirmed: bool = False) -> None:
        self.baseline_calls = 0
        self.math_calls = 0
        self.confirmed = confirmed

    def run(self, candidate):
        self.baseline_calls += 1
        return ModelResult(
            analysis=legacy_analysis(candidate.allowed_evidence_refs[0]),
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
        )

    def run_math_v2(self, candidate):
        self.math_calls += 1
        ref = next(
            value
            for value in candidate.allowed_evidence_refs
            if value.startswith("target_group.captures[")
        )
        draft = math_analysis(ref)
        image_refs = list(candidate.input_binding.get("image_evidence_refs") or [])
        if image_refs:
            image_claim = math_claim(ref)
            image_claim["provenance"] = "mixed"
            image_claim["evidence_refs"] = [ref, *image_refs]
            draft["question_structure"]["objects"][-1] = image_claim
        review = {
            "schema_version": "study-intake-luna-math-critical-review-v2",
            "verdict": "pass",
            "summary": "独立批判审查完成。",
            "revised_analysis": copy.deepcopy(draft),
            "relationship_decisions": [],
            "unsupported_claims": [],
            "evidence_misreads": [],
            "mathematical_errors": [],
            "visual_findings": [],
            "provenance_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        image_refs = tuple(candidate.input_binding.get("image_evidence_refs") or [])
        # The active contract builds relationship evidence only from rows the
        # model consumed through MCP.  An empty transcript is the truthful
        # evidence-incomplete fixture for validator-only tests; the retired
        # host-prefetch relationship fixture is intentionally not used here.
        relationship_context = build_math_mcp_relationship_context(
            (), draft_analysis=draft
        )
        _, analysis_schema_sha256 = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas/luna-math-analysis-v2.json",
            stage_name="math_analysis",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            image_evidence_refs=image_refs,
            allowed_relationship_refs=[],
            allowed_relationship_candidate_ids=[],
        )
        _, review_schema_sha256 = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas/luna-math-critical-review-v2.json",
            stage_name="math_critical_review",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            allowed_analysis_refs=_analysis_review_refs(draft),
            image_evidence_refs=image_refs,
        )

        def receipt(result_sha256: str, schema_sha256: str) -> dict:
            return {
                "status": "ready",
                "prompt_version": "test-math-v2",
                "prompt_sha256": "a" * 64,
                "schema_sha256": schema_sha256,
                "result_sha256": result_sha256,
                "output_sha256": "c" * 64,
                "duration_ms": 1,
                "requested_model": "gpt-5.6-luna",
                "requested_reasoning_effort": "max",
                "runtime_model": "gpt-5.6-luna" if self.confirmed else None,
                "runtime_reasoning_effort": "max" if self.confirmed else None,
                "runtime_metadata_provenance": (
                    "codex_json_attestation_v1" if self.confirmed else "unavailable"
                ),
                "runtime_identity_status": (
                    "confirmed" if self.confirmed else "requested_unverified"
                ),
            }
        return ModelResult(
            analysis=copy.deepcopy(draft),
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=draft,
            critical_review=review,
            stage_receipts={
                "analysis": receipt(
                    sha256_value(draft), analysis_schema_sha256
                ),
                "critical_review": receipt(
                    sha256_value(review), review_schema_sha256
                ),
            },
            relationship_context=relationship_context,
        )


class MathV2CoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.runtime = self.base / "runtime"
        self.repo = self.base / "math"
        self.repo.mkdir()
        cards = self.repo / "错题知识网络" / "错题卡"
        cards.mkdir(parents=True)
        generated = self.repo / "错题知识网络" / "生成"
        generated.mkdir(parents=True)
        detail_dir = self.repo / "错题知识网络" / "可视化错题详情" / "高等数学"
        detail_dir.mkdir(parents=True)
        question_image = self.repo / "错题知识网络" / "可视化错题详情" / "GS-001-question.png"
        question_image.write_bytes(
            base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
            )
        )
        solution_image = (
            self.repo
            / "错题知识网络"
            / "可视化错题详情"
            / "GS-001-solution.png"
        )
        solution_image.write_bytes(
            base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
            )
        )
        detail = detail_dir / "GS-001.md"
        detail.write_text(
            "# 题目\n\n这是用于测试的完整题干文本，包含足够对象、条件和所求描述。\n\n"
            "# 解析\n\n这是经过验证的完整解析文本，用于测试旧题无需独立解析图的证据路线。\n",
            encoding="utf-8",
        )
        self.card = cards / "GS-001_测试.md"
        self.card.write_text(
            "---\nid: \"GS-001\"\nsubject: 高等数学\nlecture_refs:\n"
            "  - 错题知识网络/可视化错题详情/高等数学/GS-001.md\n"
            "  - 错题知识网络/可视化错题详情/GS-001-question.png\n"
            "  - 错题知识网络/可视化错题详情/GS-001-solution.png\n"
            "---\n# 题目\n\n测试题目文本。\n",
            encoding="utf-8",
        )
        projection = {
            "updated_at": "2026-08-05",
            "cards": [
                {
                    "id": "GS-001",
                    "path": "错题知识网络/错题卡/GS-001_测试.md",
                    "meta": {
                        "subject": "高等数学",
                        "knowledge": ["高阶导数"],
                        "error_causes": ["错点识别"],
                        "methods": ["部分分式"],
                        "traps": ["符号"],
                        "question_type": ["求导"],
                        "related": [],
                    },
                    "topic_chains": ["一元函数微分学"],
                }
            ],
            "similarities": [],
            "relation_summary": {},
        }
        (generated / "wrong_questions.json").write_text(
            json.dumps(projection, ensure_ascii=False), encoding="utf-8"
        )
        (generated / "知识网络图.mmd").write_text(
            "graph TD\n  GS001[高阶导数]\n", encoding="utf-8"
        )
        (self.repo / "错题知识网络" / "知识点库.md").write_text(
            "# 高等数学\n\n- 高阶导数\n", encoding="utf-8"
        )
        schema_dir = self.repo / "错题知识网络" / "schema"
        schema_dir.mkdir()
        policy = {
            "schema_version": "math_relationship_signal_policy_v1",
            "ruleset_version": "test-v1",
            "strong_edge_minimum": {"never_sufficient_alone": ["broad_knowledge"]},
            "broad_knowledge": ["高等数学"],
            "broad_question_types": ["综合题"],
            "generic_error_causes": ["知识缺口"],
            "generic_methods": ["先判型"],
            "generic_traps": ["符号"],
            "non_evidence_markers": ["待确认"],
            "storage_policy": {"automatic_edge_may_write_formal_related": False},
            "execution_policy": {"current_mode": "SHADOW", "formal_write_in_shadow": False},
        }
        (schema_dir / "relationship_signal_policy.json").write_text(
            json.dumps(policy, ensure_ascii=False), encoding="utf-8"
        )
        self.status_path = self.repo / "status.json"
        self.status_script = self.repo / "status.py"
        self.status_script.write_text(
            "import json\nfrom pathlib import Path\n"
            "print(json.dumps(json.loads((Path(__file__).parent/'status.json').read_text())))\n",
            encoding="utf-8",
        )
        fake_codex = self.base / "codex"
        fake_codex.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        fake_codex.chmod(0o700)
        self.config = {
            "schema_version": "study-intake-preprocessor-config-v1",
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "dispatch": {
                "authority_required": True,
                "heartbeat_interval_seconds": 15,
                "lease_ttl_seconds": 120,
                "infrastructure_recovery_attempts": 1,
            },
            "worker": {
                "enabled": True, "poll_interval_seconds": 1,
                "debounce_seconds": 0, "status_timeout_seconds": 5,
                "model_timeout_seconds": 5, "max_attempts": 3,
                "retry_base_seconds": 0, "max_jobs_per_scan": 4,
                "log_path": str(self.runtime / "logs/worker.log"),
                "lock_path": str(self.runtime / "state/worker.lock"),
            },
            "model": {
                "codex_path": str(fake_codex), "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "output_schema": str(ROOT / "schemas/luna-analysis-v1.json"),
                "prompt_version": "test-v1", "max_prompt_bytes": 131072,
                "max_images": 8,
            },
            "math_deep_v2": {
                "enabled": True, "mode": "shadow", "canary_percent": 10,
                "shadow_evaluation_start_at": "2026-08-05T00:00:00+08:00",
                "shadow_evaluation_target_count": 20,
                "soft_runtime_warning_seconds": 1800,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
                "analysis_output_schema": str(ROOT / "schemas/luna-math-analysis-v2.json"),
                "critical_review_output_schema": str(ROOT / "schemas/luna-math-critical-review-v2.json"),
                "package_output_schema": str(ROOT / "schemas/preprocess-package-v3.json"),
                "analysis_prompt_version": "test-math-analysis-v2",
                "critical_review_prompt_version": "test-math-review-v2",
                "max_prompt_bytes": 524288, "max_output_bytes": 262144,
                "min_complete_chinese_chars": 0, "max_chinese_chars": 7000,
                "gs111_cold_replay_max_seconds": 1800,
                "three_replay_p95_max_seconds": 1200,
            },
            "math_knowledge_snapshot": {
                "enabled": True,
                "max_source_bytes": 8388608,
                "max_distribution_terms": 64,
                "max_local_neighbors": 8,
                "max_relationship_candidates": 5,
                "max_snapshot_bytes": 131072,
                "sources": {
                    "graph": "错题知识网络/生成/知识网络图.mmd",
                    "projection": "错题知识网络/生成/wrong_questions.json",
                    "taxonomy": "错题知识网络/知识点库.md",
                    "relationship_policy": "错题知识网络/schema/relationship_signal_policy.json",
                },
            },
            "dashboard": {
                "projection_path": str(self.runtime / "state/dashboard_projection.json"),
                "max_items_per_subject": 200,
            },
            "adapters": {
                "math": {
                    "enabled": True, "adapter_version": "math-test-v1",
                    "python_path": sys.executable, "repo_root": str(self.repo),
                    "status_script": str(self.status_script),
                },
                "cs408": {
                    "enabled": False, "adapter_version": "cs408-test-v1",
                    "python_path": sys.executable, "repo_root": str(self.base / "cs408"),
                    "status_script": str(self.base / "unused.py"),
                },
            },
        }
        self._write_status(1)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def legacy_contract_mermaid_neighborhood_resolves_hyphenated_formal_id(self) -> None:
        snapshot, _private = build_math_knowledge_snapshot(
            self.repo,
            self.config["math_knowledge_snapshot"],
            formal_id="GS-001",
        )
        neighborhood = snapshot["current_graph_neighborhood"]
        self.assertEqual(neighborhood["requested_seed_ids"], ["GS-001"])
        self.assertEqual(neighborhood["seed_node_ids"], ["GS001"])
        self.assertEqual(neighborhood["nodes"][0]["label"], "高阶导数")

    def _capture(self, index: int) -> dict:
        return {
            "event_id": f"MFI-CAP-{index:024d}",
            "study_date": "2026-08-05",
            "recorded_at": f"2026-08-05T00:0{index}:00Z",
            "formal_id": "GS-001",
            "source_locator": None,
            "source_hash_before": sha256_file(self.card),
            "identity_state": "verified_formal",
            "source_bundle": None,
            "requested_action": "record_wrong",
            "score_ref": {"delivered_card_id": "GS-001"},
            "original_content_hash": hashlib.sha256(
                f"original:{index}".encode("utf-8")
            ).hexdigest(),
            "amendment_count": 0,
            "amendment_event_ids": [],
            "effective_evidence_hash": hashlib.sha256(
                f"evidence:{index}".encode("utf-8")
            ).hexdigest(),
            "effective_target_hash": hashlib.sha256(
                f"target:{index}".encode("utf-8")
            ).hexdigest(),
            "evidence": {"user_facts": [{"text": f"第{index}次断点"}]},
            "episode_evidence": {
                "teaching_dialogue": [
                    {
                        "speaker": "user",
                        "kind": "reasoning",
                        "text": f"第{index}次我先检查定义。",
                    }
                ]
            },
            "active_freeze_ids": [],
        }

    def _write_status(self, count: int) -> None:
        rows = [self._capture(index) for index in range(1, count + 1)]
        atomic_write_json(
            self.status_path,
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-05",
                "pending_count": len(rows), "closed_count": 0,
                "pending": rows, "closed": [], "active_freezes": [],
                "ledger_hash": "f" * 64,
            },
        )

    def _new_source_capture(
        self, missing: str | set[str] | None = None
    ) -> dict:
        missing_roles = (
            {missing}
            if isinstance(missing, str)
            else set(missing or set())
        )
        staging = self.repo / "staging"
        staging.mkdir(exist_ok=True)
        question_path = staging / "new-question.png"
        solution_path = staging / "new-solution.png"
        solution_text_path = staging / "new-solution.md"
        raw = (
            self.repo
            / "错题知识网络"
            / "可视化错题详情"
            / "GS-001-question.png"
        ).read_bytes()
        question_path.write_bytes(raw)
        solution_path.write_bytes(
            (
                self.repo
                / "错题知识网络"
                / "可视化错题详情"
                / "GS-001-solution.png"
            ).read_bytes()
        )
        solution_text_path.write_text(
            "# 解析\n\n与来源 manifest 绑定的完整文字解析。\n",
            encoding="utf-8",
        )
        artifacts = []
        if "question_image" not in missing_roles:
            artifacts.append(
                {
                    "role": "question",
                    "path": str(question_path.relative_to(self.repo)),
                    "sha256": sha256_file(question_path),
                    "media_type": "image/png",
                }
            )
        if "solution_image" not in missing_roles:
            artifacts.append(
                {
                    "role": "solution",
                    "path": str(solution_path.relative_to(self.repo)),
                    "sha256": sha256_file(solution_path),
                    "media_type": "image/png",
                }
            )
        if "solution_text" not in missing_roles:
            artifacts.append(
                {
                    "role": "solution_text",
                    "path": str(solution_text_path.relative_to(self.repo)),
                    "sha256": sha256_file(solution_text_path),
                    "media_type": "text/markdown",
                }
            )
        manifest = {
            "schema_version": "math-intake-source-manifest-v2",
            "source_locator": "new-source-test",
            "artifacts": artifacts,
        }
        missing_label = "-".join(sorted(missing_roles)) or "complete"
        manifest_path = staging / f"manifest-{missing_label}.json"
        atomic_write_json(manifest_path, manifest)
        return {
            "event_id": "MFI-CAP-NEW00000000000000000001",
            "capture_schema_version": "math-fast-intake-capture-v2",
            "study_date": "2026-08-05",
            "recorded_at": "2026-08-05T01:00:00Z",
            "formal_id": None,
            "source_locator": "new-source-test",
            "source_hash_before": None,
            "identity_state": "new_source",
            "source_bundle": {
                "manifest_path": str(manifest_path.relative_to(self.repo)),
                "manifest_hash": sha256_file(manifest_path),
            },
            "episode_evidence": {
                "solution_text": (
                    "" if "solution_text" in missing_roles else "完整解析文本。"
                ),
                "user_answer_text": (
                    ""
                    if "user_answer_text" in missing_roles
                    else "用户原始作答与推导。"
                ),
                "teaching_dialogue": [
                    {"speaker": "user", "kind": "reasoning", "text": "我先拆项。"}
                ],
            },
            "requested_action": "record_wrong",
            "score_ref": None,
            "original_content_hash": "8" * 64,
            "amendment_count": 0,
            "amendment_event_ids": [],
            "effective_evidence_hash": "9" * 64,
            "effective_target_hash": "a" * 64,
            "evidence": {"user_facts": [{"text": "新题第一断点"}]},
            "active_freeze_ids": [],
        }

    def test_new_source_requires_question_and_one_solution_evidence_route(self) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        complete = self._new_source_capture()
        status = {
            "schema_version": "math-fast-intake-status-v1",
            "study_date": "2026-08-05",
            "pending": [complete],
        }
        rows = adapter.candidates(status)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].model_input["formal_card"])
        self.assertEqual(
            rows[0].model_input["source_bundle"]["source_kind"], "new_intake"
        )
        self.assertEqual(rows[0].input_binding["source_route"], "new_intake")
        self.assertEqual(
            [row["role"] for row in rows[0].model_input["source_bundle"]["artifacts"]],
            ["question", "solution"],
        )
        self.assertEqual(
            [
                row["role"]
                for row in rows[0].model_input["source_bundle"]["text_sources"]
            ],
            ["solution_text"],
        )
        new_identity = CodexRunner._capture_identity(rows[0])
        self.assertNotIn("formal_id", new_identity)
        self.assertEqual(
            new_identity["content_fingerprint"], rows[0].input_fingerprint
        )
        new_runner = CodexRunner(
            {
                **self.config["model"],
                "math_repo_root": str(self.repo),
            },
            self.runtime,
        )
        new_artifact_kinds = {
            row["artifact_kind"]
            for row in new_runner._capture_artifacts(rows[0])
        }
        self.assertTrue(
            {
                "question_image",
                "learning_record",
                "dialogue",
                "solution_text",
            }.issubset(new_artifact_kinds)
        )
        for accepted_missing in ("solution_image", "solution_text"):
            with self.subTest(accepted_missing=accepted_missing):
                accepted = self._new_source_capture(accepted_missing)
                self.assertEqual(
                    len(adapter.candidates({**status, "pending": [accepted]})),
                    1,
                )
        for missing in (
            {"question_image"},
            {"solution_image", "solution_text"},
            {"user_answer_text"},
        ):
            with self.subTest(missing=missing):
                broken = self._new_source_capture(missing)
                with self.assertRaisesRegex(
                    PreprocessorError, "math_new_source_evidence_incomplete"
                ):
                    adapter.candidates({**status, "pending": [broken]})
        inline_only = self._new_source_capture(
            {"solution_image", "solution_text"}
        )
        inline_only["episode_evidence"]["solution_text"] = (
            "只有 capture_facts 内联文字，不能绕过独立 artifact 门禁。"
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_new_source_evidence_incomplete"
        ):
            adapter.candidates({**status, "pending": [inline_only]})

    def test_existing_formal_route_is_canonical_old_existing(self) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        status = adapter.status("2026-08-05")
        candidate = adapter.candidates(status)[0]
        self.assertEqual(
            candidate.model_input["source_bundle"]["source_kind"],
            "old_existing",
        )
        self.assertEqual(candidate.input_binding["source_route"], "old_existing")

    def test_existing_formal_observation_accepts_substantive_teaching_turns_without_capture_images(
        self,
    ) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters[
            "math"
        ]
        capture = self._capture(3)
        teaching_turns = [
            {
                "speaker": "user",
                "kind": "reasoning",
                "text": "我把题设条件代入后，发现这里把充分条件误当成必要条件。",
            }
        ]
        capture["episode_evidence"] = {"teaching_turns": teaching_turns}
        self.assertIsNone(capture["source_bundle"])
        rows = adapter.candidates(
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-05",
                "pending": [capture],
            }
        )
        self.assertEqual(len(rows), 1)
        contract = rows[0].model_input["source_bundle"][
            "math_evidence_contract"
        ]
        self.assertEqual(contract["stable_formal_id"], "GS-001")
        self.assertEqual(
            contract["full_dialogue_sha256"], sha256_value(teaching_turns)
        )
        self.assertEqual(contract["evidence_status"], "ready")
        self.assertEqual(contract["missing_roles"], [])

    def test_observation_metadata_cannot_impersonate_record_or_dialogue(self) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters[
            "math"
        ]
        metadata_record = self._capture(1)
        metadata_record["evidence"] = {
            "schema_version": "math-capture-metadata-v1",
            "capture_id": metadata_record["event_id"],
            "content_sha256": "a" * 64,
            "status": "ready",
        }
        status = {
            "schema_version": "math-fast-intake-status-v1",
            "study_date": "2026-08-05",
            "pending": [metadata_record],
        }
        with self.assertRaisesRegex(
            PreprocessorError, "math_observation_evidence_incomplete"
        ):
            adapter.candidates(status)

        metadata_dialogue = self._capture(2)
        metadata_dialogue["episode_evidence"] = {
            "interaction_trace": [
                {
                    "event_id": "TURN-ONLY-METADATA",
                    "source_sha256": "b" * 64,
                    "status": "recorded",
                }
            ]
        }
        with self.assertRaisesRegex(
            PreprocessorError, "math_observation_evidence_incomplete"
        ):
            adapter.candidates({**status, "pending": [metadata_dialogue]})

        empty_teaching_turns = self._capture(3)
        empty_teaching_turns["episode_evidence"] = {"teaching_turns": []}
        with self.assertRaisesRegex(
            PreprocessorError, "math_observation_evidence_incomplete"
        ):
            adapter.candidates({**status, "pending": [empty_teaching_turns]})

        metadata_teaching_turns = self._capture(4)
        metadata_teaching_turns["episode_evidence"] = {
            "teaching_turns": [
                {
                    "event_id": "TURN-ONLY-METADATA",
                    "source_sha256": "c" * 64,
                    "status": "recorded",
                }
            ]
        }
        with self.assertRaisesRegex(
            PreprocessorError, "math_observation_evidence_incomplete"
        ):
            adapter.candidates({**status, "pending": [metadata_teaching_turns]})

    def test_existing_formal_route_accepts_indentless_yaml_lecture_refs(self) -> None:
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(original.replace("  - ", "- "), encoding="utf-8")
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
        readiness = candidate.model_input["source_bundle"]["readiness"]
        self.assertGreaterEqual(readiness["question_image_count"], 1)
        self.assertGreaterEqual(readiness["solution_text_count"], 1)

    def test_existing_formal_route_accepts_standard_answer_heading_as_solution_text(
        self,
    ) -> None:
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(
            original.replace("# 解析\n", "# 标准答案\n"),
            encoding="utf-8",
        )
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
        readiness = candidate.model_input["source_bundle"]["readiness"]
        self.assertGreaterEqual(readiness["solution_text_count"], 1)

    def test_existing_formal_route_ignores_exact_unconfirmed_ref_sentinel(self) -> None:
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(
            original.replace("lecture_refs:\n", "lecture_refs:\n  - 待确认\n"),
            encoding="utf-8",
        )
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
        readiness = candidate.model_input["source_bundle"]["readiness"]
        self.assertGreaterEqual(readiness["question_image_count"], 1)
        self.assertGreaterEqual(readiness["solution_text_count"], 1)

    def test_complete_staged_source_tolerates_missing_optional_formal_ref(self) -> None:
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(
            original.replace(
                "lecture_refs:\n",
                "lecture_refs:\n  - missing-inside-repo.md\n",
            ),
            encoding="utf-8",
        )
        capture = self._new_source_capture()
        capture.update(
            {
                "formal_id": "GS-001",
                "identity_state": "verified_formal",
                "source_locator": str(self.card.relative_to(self.repo)),
                "source_hash_before": sha256_file(self.card),
            }
        )
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        rows = adapter.candidates(
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-05",
                "pending": [capture],
            }
        )
        self.assertEqual(len(rows), 1)
        readiness = rows[0].model_input["source_bundle"]["readiness"]
        self.assertGreaterEqual(readiness["question_image_count"], 1)
        self.assertGreaterEqual(readiness["solution_text_count"], 1)

    def test_existing_formal_observation_allows_optional_question_and_solution_evidence(
        self,
    ) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(
            "\n".join(
                line
                for line in original.splitlines()
                if "GS-001-solution.png" not in line
            )
            + "\n",
            encoding="utf-8",
        )
        text_only = adapter.candidates(adapter.status("2026-08-05"))[0]
        self.assertEqual(
            text_only.model_input["source_bundle"]["readiness"][
                "solution_image_count"
            ],
            0,
        )
        self.assertGreater(
            text_only.model_input["source_bundle"]["readiness"][
                "solution_text_count"
            ],
            0,
        )

        self.card.write_text(
            "\n".join(
                line
                for line in original.splitlines()
                if "GS-001-question.png" not in line
            )
            + "\n",
            encoding="utf-8",
        )
        without_question = adapter.candidates(adapter.status("2026-08-05"))[0]
        self.assertEqual(
            without_question.model_input["source_bundle"]["readiness"][
                "question_image_count"
            ],
            0,
        )

        self.card.write_text(
            "\n".join(
                line
                for line in original.splitlines()
                if "GS-001-solution.png" not in line
                and "GS-001-question.png" not in line
                and "高等数学/GS-001.md" not in line
            )
            + "\n",
            encoding="utf-8",
        )
        without_source_artifacts = adapter.candidates(
            adapter.status("2026-08-05")
        )[0]
        readiness = without_source_artifacts.model_input["source_bundle"][
            "readiness"
        ]
        self.assertEqual(readiness["question_image_count"], 0)
        self.assertEqual(readiness["solution_image_count"], 0)
        self.assertEqual(readiness["solution_text_count"], 0)
        self.card.write_text(original, encoding="utf-8")

    def test_math_runner_freezes_solution_text_as_mcp_artifact_without_path_leak(
        self,
    ) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        original = self.card.read_text(encoding="utf-8")
        self.card.write_text(
            "\n".join(
                line
                for line in original.splitlines()
                if "GS-001-solution.png" not in line
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
            runner = CodexRunner(
                {
                    **self.config["model"],
                    "math_repo_root": str(self.repo),
                },
                self.runtime,
            )
            artifacts = runner._capture_artifacts(candidate)
            self.assertEqual(
                {
                    row["artifact_kind"]
                    for row in artifacts
                },
                {
                    "dialogue",
                    "learning_record",
                    "question_image",
                    "solution_text",
                },
            )
            solution = next(
                row
                for row in artifacts
                if row["artifact_kind"] == "solution_text"
            )
            self.assertEqual(
                sha256_file(Path(solution["path"])), solution["sha256"]
            )
            learning_record = next(
                row
                for row in artifacts
                if row["artifact_kind"] == "learning_record"
            )
            self.assertEqual(
                learning_record["content"]["source_payload_sha256"],
                sha256_value(candidate.model_input["capture"]),
            )
            self.assertNotIn(
                str(self.repo),
                json.dumps(learning_record["content"], ensure_ascii=False),
            )
            self.assertNotIn(
                "测试题目文本",
                json.dumps(learning_record["content"], ensure_ascii=False),
            )

            capture_facts = runner._capture_mcp_facts(candidate)
            self.assertNotIn(
                "current_question_evidence", capture_facts["facts"]
            )
            self.assertNotIn("capture", capture_facts["facts"])
            self.assertEqual(
                capture_facts["facts"]["learning_record_evidence"][
                    "content_route"
                ],
                "mcp_read_task_artifact",
            )
            self.assertEqual(
                capture_facts["facts"]["dialogue_evidence"][
                    "content_route"
                ],
                "mcp_read_task_artifact",
            )
            text_index = capture_facts["facts"]["source_bundle"][
                "text_sources"
            ][0]
            self.assertNotIn("content", text_index)
            self.assertNotIn("relative_path", text_index)
            self.assertEqual(
                text_index["content_route"], "mcp_read_task_artifact"
            )
            self.assertNotIn(
                str(self.repo),
                json.dumps(capture_facts, ensure_ascii=False),
            )
        finally:
            self.card.write_text(original, encoding="utf-8")

    def test_math_runner_freezes_episode_solution_text_as_artifact(self) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
        model_input = copy.deepcopy(candidate.model_input)
        model_input["source_bundle"]["text_sources"] = []
        model_input["source_bundle"]["artifacts"] = [
            row
            for row in model_input["source_bundle"]["artifacts"]
            if row.get("role") != "solution"
        ]
        model_input["current_question_evidence"] = [
            {
                "capture_id": candidate.capture_id,
                "episode_evidence": {
                    "solution_text": "先完成换元，再按原变量确定性回代。"
                },
            }
        ]
        candidate = replace(
            candidate,
            model_input=model_input,
            image_paths=tuple(
                path for path in candidate.image_paths
                if "solution" not in path.name
            ),
        )
        runner = CodexRunner(
            {**self.config["model"], "math_repo_root": str(self.repo)},
            self.runtime,
        )
        solution = next(
            row
            for row in runner._capture_artifacts(candidate)
            if row["artifact_kind"] == "solution_text"
        )
        self.assertEqual(
            solution["content"]["solution_text"],
            "先完成换元，再按原变量确定性回代。",
        )
        self.assertEqual(solution["content"]["formal_write_count"], 0)

    def test_math_runner_does_not_invent_missing_solution_artifact(self) -> None:
        adapter = Worker(self.config, model_runner=FakeMathRunner()).adapters["math"]
        candidate = adapter.candidates(adapter.status("2026-08-05"))[0]
        model_input = copy.deepcopy(candidate.model_input)
        model_input["source_bundle"]["text_sources"] = []
        model_input["source_bundle"]["artifacts"] = [
            row
            for row in model_input["source_bundle"]["artifacts"]
            if row.get("role") != "solution"
        ]
        model_input["current_question_evidence"] = [
            {
                "capture_id": candidate.capture_id,
                "episode_evidence": {"user_answer_text": "只保留真实作答。"},
            }
        ]
        candidate = replace(
            candidate,
            model_input=model_input,
            image_paths=tuple(
                path for path in candidate.image_paths
                if "solution" not in path.name
            ),
        )
        runner = CodexRunner(
            {**self.config["model"], "math_repo_root": str(self.repo)},
            self.runtime,
        )
        self.assertFalse(
            any(
                row["artifact_kind"] in {"solution_text", "solution_image"}
                for row in runner._capture_artifacts(candidate)
            )
        )

    def legacy_contract_relationship_retrieval_is_deterministic_bounded_and_specific(self) -> None:
        projection_path = (
            self.repo / "错题知识网络" / "生成" / "wrong_questions.json"
        )
        projection = json.loads(projection_path.read_text(encoding="utf-8"))

        def card(
            card_id: str,
            *,
            knowledge: list[str] | None = None,
            methods: list[str] | None = None,
            errors: list[str] | None = None,
            quality_hold: bool = False,
        ) -> dict:
            return {
                "id": card_id,
                "path": f"错题知识网络/错题卡/{card_id}.md",
                "meta": {
                    "subject": "高等数学",
                    "knowledge": knowledge or [],
                    "methods": methods or [],
                    "error_causes": errors or [],
                    "traps": [],
                    "question_type": ["综合题"],
                    "quality_gate": "quality_hold" if quality_hold else "pass",
                },
                "topic_chains": [],
            }

        projection["cards"].extend(
            [
                card("GS-002", knowledge=["高等数学"]),
                card("GS-003", knowledge=["高阶导数"], methods=["部分分式"]),
                card(
                    "GS-004",
                    knowledge=["高阶导数"],
                    methods=["部分分式"],
                    errors=["错点识别"],
                ),
                card("GS-005", knowledge=["高阶导数"], methods=["部分分式"], quality_hold=True),
                *[
                    card(
                        f"GS-{index:03d}",
                        knowledge=["高阶导数"],
                        methods=["部分分式"],
                    )
                    for index in range(6, 13)
                ],
            ]
        )
        projection_path.write_text(
            json.dumps(projection, ensure_ascii=False), encoding="utf-8"
        )
        worker = Worker(self.config, model_runner=FakeMathRunner())
        status = worker.adapters["math"].status("2026-08-05")
        deep = worker.adapters["math"].deep_candidate(
            worker.adapters["math"].candidates(status)[0]
        )
        draft = math_analysis(deep.allowed_evidence_refs[0])
        first = retrieve_math_relationship_context(deep, draft, self.config)
        second = retrieve_math_relationship_context(deep, draft, self.config)
        self.assertEqual(first, second)
        ids = [row["candidate_id"] for row in first["candidates"]]
        self.assertLessEqual(len(ids), 5)
        self.assertNotIn("GS-002", ids)
        self.assertNotIn("GS-005", ids)
        self.assertIn("GS-003", ids)
        self.assertIn("GS-004", ids)
        verify_math_knowledge_snapshot_sources(deep, self.config)
        graph_path = self.repo / "错题知识网络" / "生成" / "知识网络图.mmd"
        graph_path.write_text("graph TD\n  CHANGED[已变更]\n", encoding="utf-8")
        with self.assertRaisesRegex(PreprocessorError, "math_knowledge_source_changed"):
            verify_math_knowledge_snapshot_sources(deep, self.config)

    def legacy_contract_group_result_refreshes_all_aliases_and_persists(self) -> None:
        runner = FakeMathRunner()
        worker = Worker(self.config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        first_id = self._capture(1)["event_id"]
        first_generation = load_json(
            worker.store.math_shadow_latest_path(first_id)
        )["group_generation_id"]
        self.assertEqual(runner.baseline_calls, 1)
        self.assertEqual(runner.math_calls, 1)

        self._write_status(2)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        second_id = self._capture(2)["event_id"]
        first_pointer = load_json(worker.store.math_shadow_latest_path(first_id))
        second_pointer = load_json(worker.store.math_shadow_latest_path(second_id))
        self.assertNotEqual(first_generation, first_pointer["group_generation_id"])
        self.assertEqual(
            first_pointer["group_generation_id"], second_pointer["group_generation_id"]
        )
        self.assertNotEqual(
            first_pointer["report_json_sha256"], second_pointer["report_json_sha256"]
        )
        self.assertEqual(runner.baseline_calls, 2)
        self.assertEqual(runner.math_calls, 2)
        for capture_id in (first_id, second_id):
            baseline = load_json(worker.store.latest_path("math", capture_id))
            self.assertEqual(baseline["schema_version"], "study-intake-preprocess-latest-v1")
            self.assertNotIn("/shadow/", baseline["package_path"])
        projection = load_json(Path(self.config["dashboard"]["projection_path"]))
        progress = projection["subjects"]["math"]["shadow_evaluation"]
        self.assertEqual((progress["processed"], progress["remaining"], progress["failures"]), (2, 18, 0))

        replay_runner = FakeMathRunner()
        restarted = Worker(self.config, model_runner=replay_runner)
        status = restarted.adapters["math"].status("2026-08-05")
        base = restarted.adapters["math"].candidates(status)[0]
        restarted.publish_math_shadow_candidate(base)
        self.assertEqual(replay_runner.math_calls, 0)

    def legacy_contract_two_initial_group_members_use_one_math_run(self) -> None:
        self._write_status(2)
        runner = FakeMathRunner()
        worker = Worker(self.config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        self.assertEqual(runner.baseline_calls, 2)
        self.assertEqual(runner.math_calls, 1)
        pointers = [
            load_json(worker.store.math_shadow_latest_path(self._capture(i)["event_id"]))
            for i in (1, 2)
        ]
        self.assertEqual(pointers[0]["group_generation_id"], pointers[1]["group_generation_id"])

    def legacy_contract_math_shadow_rejects_wrong_dynamic_schema_sha_before_publication(self) -> None:
        class WrongSchemaRunner(FakeMathRunner):
            def run_math_v2(inner_self, candidate):
                result = super().run_math_v2(candidate)
                receipts = copy.deepcopy(result.stage_receipts)
                receipts["analysis"]["schema_sha256"] = "f" * 64
                return replace(result, stage_receipts=receipts)

        runner = WrongSchemaRunner()
        worker = Worker(self.config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        capture_id = self._capture(1)["event_id"]
        status = worker.adapters["math"].status("2026-08-05")
        candidate = worker.adapters["math"].candidates(status)[0]
        shadow_state, _ = worker._math_shadow_state(candidate)
        self.assertNotEqual(shadow_state, "ready")
        self.assertFalse(worker.store.math_shadow_latest_path(capture_id).exists())
        projection = load_json(Path(self.config["dashboard"]["projection_path"]))
        progress = projection["subjects"]["math"]["shadow_evaluation"]
        self.assertEqual(progress["processed"], 0)
        self.assertEqual(runner.baseline_calls, 1)
        self.assertEqual(runner.math_calls, 1)

    def legacy_contract_corrupt_shadow_pointer_is_fail_soft_for_ready_baseline(self) -> None:
        runner = FakeMathRunner()
        worker = Worker(self.config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        capture_id = self._capture(1)["event_id"]
        baseline_before = load_json(worker.store.latest_path("math", capture_id))
        job_before = worker.store.read_job("math", capture_id)
        worker.store.math_shadow_latest_path(capture_id).write_text(
            "not-json", encoding="utf-8"
        )
        result = worker.run_once(
            subject="math", capture_id=capture_id,
            study_date="2026-08-05", force=True,
        )
        self.assertEqual(result["processed"][0]["status"], "shadow_two_pass_ready")
        self.assertEqual(load_json(worker.store.latest_path("math", capture_id)), baseline_before)
        self.assertEqual(worker.store.read_job("math", capture_id), job_before)

    def legacy_contract_baseline_receipt_precedes_shadow_dispatch_and_is_unique(self) -> None:
        def isolated_config(name: str) -> dict:
            config = copy.deepcopy(self.config)
            runtime = self.base / name
            config["runtime_root"] = str(runtime)
            config["worker"]["log_path"] = str(runtime / "logs/worker.log")
            config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
            config["dashboard"]["projection_path"] = str(
                runtime / "state/dashboard_projection.json"
            )
            return config

        shadow_started = threading.Event()
        release_shadow = threading.Event()

        class BlockingShadowWorker(Worker):
            def _process_math_shadow(inner_self, candidate):
                shadow_started.set()
                release_shadow.wait(5)
                return {"status": "synthetic_shadow_released"}

        blocking_config = isolated_config("runtime-blocking-shadow")
        blocking_worker = BlockingShadowWorker(
            blocking_config, model_runner=FakeMathRunner()
        )
        outcomes = []
        errors = []

        def run_blocking() -> None:
            try:
                outcomes.append(
                    blocking_worker.run_once(
                        subject="math", study_date="2026-08-05", force=True
                    )
                )
            except BaseException as exc:  # pragma: no cover - diagnostic path
                errors.append(exc)

        thread = threading.Thread(target=run_blocking)
        thread.start()
        self.assertTrue(shadow_started.wait(2))
        blocking_receipt_root = (
            Path(blocking_config["runtime_root"])
            / "receipts/math/2026-08-05"
        )
        blocking_receipts = sorted(blocking_receipt_root.glob("*.json"))
        self.assertEqual(len(blocking_receipts), 1)
        baseline_receipt = load_json(blocking_receipts[0])
        self.assertEqual(baseline_receipt["status"], "ready")
        self.assertEqual(baseline_receipt["formal_write_count"], 0)
        release_shadow.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(len(list(blocking_receipt_root.glob("*.json"))), 1)

        throw_calls = []

        class ThrowingShadowWorker(Worker):
            def _process_math_shadow(inner_self, candidate):
                throw_calls.append(candidate.capture_id)
                raise RuntimeError("synthetic shadow dispatch failure")

        throwing_config = isolated_config("runtime-throwing-shadow")
        throwing_worker = ThrowingShadowWorker(
            throwing_config, model_runner=FakeMathRunner()
        )
        result = throwing_worker.run_once(
            subject="math", study_date="2026-08-05", force=True
        )
        self.assertEqual(result["processed"][0]["status"], "ready")
        self.assertEqual(throw_calls, [self._capture(1)["event_id"]])
        throwing_receipt_root = (
            Path(throwing_config["runtime_root"])
            / "receipts/math/2026-08-05"
        )
        throwing_receipts = sorted(throwing_receipt_root.glob("*.json"))
        self.assertEqual(len(throwing_receipts), 1)
        self.assertEqual(load_json(throwing_receipts[0])["status"], "ready")
        self.assertEqual(
            throwing_worker.store.read_job(
                "math", self._capture(1)["event_id"]
            )["status"],
            "ready",
        )

    def legacy_contract_missing_ready_baseline_receipt_is_recovered_before_shadow(self) -> None:
        runner = FakeMathRunner()
        worker = Worker(self.config, model_runner=runner)
        failed_once = False

        def fail_first_baseline_receipt(path, value):
            nonlocal failed_once
            target = Path(path)
            if (
                not failed_once
                and target.parent.parts[-3:]
                == ("receipts", "math", "2026-08-05")
            ):
                failed_once = True
                raise OSError("synthetic baseline receipt publication failure")
            return atomic_publish_json_no_clobber(target, value)

        with mock.patch(
            "preprocessor_core.atomic_publish_json_no_clobber",
            side_effect=fail_first_baseline_receipt,
        ):
            with self.assertRaisesRegex(
                PreprocessorError,
                "math_baseline_receipt_recovery_write_failed",
            ):
                worker.run_once(
                    subject="math", study_date="2026-08-05", force=True
                )

        capture_id = self._capture(1)["event_id"]
        job_path = worker.store.job_path("math", capture_id)
        latest_path = worker.store.latest_path("math", capture_id)
        job_before = job_path.read_bytes()
        latest_before = latest_path.read_bytes()
        job = load_json(job_path)
        package_path = Path(job["package_path"])
        package_before = package_path.read_bytes()
        receipt_root = self.runtime / "receipts" / "math" / "2026-08-05"
        self.assertEqual(runner.baseline_calls, 1)
        self.assertEqual(runner.math_calls, 0)
        self.assertEqual(list(receipt_root.glob("*.json")), [])
        self.assertEqual(job["status"], "ready")
        self.assertEqual(job["formal_write_count"], 0)

        restarted = Worker(self.config, model_runner=runner)
        result = restarted.run_once(
            subject="math", capture_id=capture_id,
            study_date="2026-08-05", force=True,
        )
        self.assertEqual(result["processed"][0]["status"], "shadow_two_pass_ready")
        receipts = sorted(receipt_root.glob("*.json"))
        self.assertEqual(len(receipts), 1)
        recovered = load_json(receipts[0])
        self.assertEqual(recovered["receipt_id"], _receipt_id(
            restarted.adapters["math"].candidates(
                restarted.adapters["math"].status("2026-08-05")
            )[0],
            int(job["attempts"]),
            str(job["started_at"]),
        ))
        self.assertEqual(recovered["status"], "ready")
        self.assertEqual(recovered["formal_write_count"], 0)
        self.assertEqual(runner.baseline_calls, 1)
        self.assertEqual(runner.math_calls, 1)
        self.assertEqual(job_path.read_bytes(), job_before)
        self.assertEqual(latest_path.read_bytes(), latest_before)
        self.assertEqual(package_path.read_bytes(), package_before)

        again = Worker(self.config, model_runner=runner)
        again.run_once(
            subject="math", capture_id=capture_id,
            study_date="2026-08-05", force=True,
        )
        self.assertEqual(len(list(receipt_root.glob("*.json"))), 1)
        self.assertEqual(runner.baseline_calls, 1)
        self.assertEqual(runner.math_calls, 1)

    def legacy_contract_initial_shadow_dispatch_never_clobbers_same_id_receipt(self) -> None:
        for variant in ("conflict", "exact"):
            with self.subTest(variant=variant):
                config = copy.deepcopy(self.config)
                runtime = self.base / f"initial-receipt-{variant}"
                config["runtime_root"] = str(runtime)
                config["worker"]["log_path"] = str(runtime / "logs/worker.log")
                config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
                config["dashboard"]["projection_path"] = str(
                    runtime / "state/dashboard_projection.json"
                )
                runner = FakeMathRunner()
                worker = Worker(config, model_runner=runner)
                original_ensure = worker._ensure_ready_math_baseline_receipt
                injected = False
                receipt_path: Path | None = None
                conflict_bytes: bytes | None = None

                def inject_before_dispatch(candidate):
                    nonlocal injected, receipt_path, conflict_bytes
                    if not injected:
                        injected = True
                        job = load_json(
                            worker.store.job_path("math", candidate.capture_id)
                        )
                        receipt_path = worker.store.receipt_path(
                            {
                                "subject": "math",
                                "study_date": candidate.study_date,
                                "receipt_id": _receipt_id(
                                    candidate,
                                    int(job["attempts"]),
                                    str(job["started_at"]),
                                ),
                            }
                        )
                        if variant == "conflict":
                            atomic_write_json(receipt_path, {"conflict": True})
                            conflict_bytes = receipt_path.read_bytes()
                        else:
                            original_ensure(candidate)
                    return original_ensure(candidate)

                with mock.patch.object(
                    worker,
                    "_ensure_ready_math_baseline_receipt",
                    side_effect=inject_before_dispatch,
                ):
                    if variant == "conflict":
                        with self.assertRaisesRegex(
                            PreprocessorError,
                            "math_baseline_receipt_recovery_conflict",
                        ):
                            worker.run_once(
                                subject="math",
                                study_date="2026-08-05",
                                force=True,
                            )
                    else:
                        worker.run_once(
                            subject="math",
                            study_date="2026-08-05",
                            force=True,
                        )

                self.assertIsNotNone(receipt_path)
                capture_id = self._capture(1)["event_id"]
                job = load_json(worker.store.job_path("math", capture_id))
                self.assertEqual(job["status"], "ready")
                self.assertEqual(job["formal_write_count"], 0)
                self.assertTrue(
                    worker.store.latest_path("math", capture_id).is_file()
                )
                self.assertTrue(Path(job["package_path"]).is_file())
                self.assertEqual(runner.baseline_calls, 1)
                if variant == "conflict":
                    self.assertEqual(runner.math_calls, 0)
                    self.assertEqual(receipt_path.read_bytes(), conflict_bytes)
                else:
                    self.assertEqual(runner.math_calls, 1)

    def legacy_contract_baseline_receipt_recovery_fails_closed_on_publication_conflict(self) -> None:
        def isolated_config(name: str) -> dict:
            config = copy.deepcopy(self.config)
            runtime = self.base / name
            config["runtime_root"] = str(runtime)
            config["worker"]["log_path"] = str(runtime / "logs/worker.log")
            config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
            config["dashboard"]["projection_path"] = str(
                runtime / "state/dashboard_projection.json"
            )
            return config

        for variant in (
            "job_publication", "latest_hash", "package_bytes", "receipt_conflict"
        ):
            with self.subTest(variant=variant):
                config = isolated_config(f"runtime-receipt-{variant}")
                runtime = Path(config["runtime_root"])
                runner = FakeMathRunner()
                worker = Worker(config, model_runner=runner)
                failed_once = False

                def fail_first_baseline_receipt(path, value):
                    nonlocal failed_once
                    target = Path(path)
                    if (
                        not failed_once
                        and target.parent.parts[-3:]
                        == ("receipts", "math", "2026-08-05")
                    ):
                        failed_once = True
                        raise OSError(
                            "synthetic baseline receipt publication failure"
                        )
                    return atomic_publish_json_no_clobber(target, value)

                with mock.patch(
                    "preprocessor_core.atomic_publish_json_no_clobber",
                    side_effect=fail_first_baseline_receipt,
                ):
                    with self.assertRaisesRegex(
                        PreprocessorError,
                        "math_baseline_receipt_recovery_write_failed",
                    ):
                        worker.run_once(
                            subject="math", study_date="2026-08-05", force=True
                        )

                capture_id = self._capture(1)["event_id"]
                job_path = worker.store.job_path("math", capture_id)
                latest_path = worker.store.latest_path("math", capture_id)
                job = load_json(job_path)
                latest = load_json(latest_path)
                package_path = Path(job["package_path"])
                status = worker.adapters["math"].status("2026-08-05")
                candidate = worker.adapters["math"].candidates(status)[0]
                receipt = {
                    "subject": "math",
                    "study_date": "2026-08-05",
                    "receipt_id": _receipt_id(
                        candidate,
                        int(job["attempts"]),
                        str(job["started_at"]),
                    ),
                }
                receipt_path = worker.store.receipt_path(receipt)

                if variant == "job_publication":
                    job["publication_id"] = "SIP-PUB-" + "A" * 32
                    atomic_write_json(job_path, job)
                elif variant == "latest_hash":
                    latest["package_sha256"] = "a" * 64
                    atomic_write_json(latest_path, latest)
                elif variant == "package_bytes":
                    package_path.write_bytes(package_path.read_bytes() + b" ")
                else:
                    atomic_write_json(receipt_path, {"conflict": True})
                    conflict_before = receipt_path.read_bytes()

                restarted = Worker(config, model_runner=runner)
                with self.assertRaises(PreprocessorError):
                    restarted.run_once(
                        subject="math", capture_id=capture_id,
                        study_date="2026-08-05", force=True,
                    )
                self.assertEqual(runner.baseline_calls, 1)
                self.assertEqual(runner.math_calls, 0)
                if variant == "receipt_conflict":
                    self.assertEqual(receipt_path.read_bytes(), conflict_before)
                else:
                    self.assertFalse(receipt_path.exists())

    def legacy_contract_ready_shadow_revalidates_and_recovers_baseline_receipt(self) -> None:
        runner = FakeMathRunner()
        worker = Worker(self.config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        capture_id = self._capture(1)["event_id"]
        receipt_root = self.runtime / "receipts" / "math" / "2026-08-05"
        receipts = sorted(receipt_root.glob("*.json"))
        self.assertEqual(len(receipts), 1)
        receipt_path = receipts[0]
        receipt_bytes = receipt_path.read_bytes()

        receipt_path.unlink()
        progress_worker = Worker(self.config, model_runner=runner)
        self.assertEqual(
            progress_worker._math_shadow_evaluation_progress()["processed"], 1
        )
        self.assertEqual(receipt_path.read_bytes(), receipt_bytes)
        self.assertEqual((runner.baseline_calls, runner.math_calls), (1, 1))

        receipt_path.unlink()
        state_worker = Worker(self.config, model_runner=runner)
        status = state_worker.adapters["math"].status("2026-08-05")
        candidate = state_worker.adapters["math"].candidates(status)[0]
        shadow_state, _ = state_worker._math_shadow_state(candidate)
        self.assertEqual(shadow_state, "ready")
        self.assertEqual(receipt_path.read_bytes(), receipt_bytes)
        self.assertEqual((runner.baseline_calls, runner.math_calls), (1, 1))

        atomic_write_json(receipt_path, {"conflict": True})
        conflict_bytes = receipt_path.read_bytes()
        conflict_worker = Worker(self.config, model_runner=runner)
        conflict_status = conflict_worker.adapters["math"].status("2026-08-05")
        conflict_candidate = conflict_worker.adapters["math"].candidates(
            conflict_status
        )[0]
        conflict_state, _ = conflict_worker._math_shadow_state(
            conflict_candidate
        )
        self.assertEqual(conflict_state, "stale")
        self.assertEqual(
            conflict_worker._math_shadow_evaluation_progress()["processed"], 0
        )
        with self.assertRaises(PreprocessorError):
            conflict_worker.run_once(
                subject="math", capture_id=capture_id,
                study_date="2026-08-05", force=True,
            )
        self.assertEqual(receipt_path.read_bytes(), conflict_bytes)
        self.assertEqual((runner.baseline_calls, runner.math_calls), (1, 1))

    def legacy_contract_shadow_ready_and_progress_require_full_artifact_closure(self) -> None:
        self._write_status(2)
        worker = Worker(self.config, model_runner=FakeMathRunner())
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        capture_id = self._capture(1)["event_id"]
        status = worker.adapters["math"].status("2026-08-05")
        base_candidate = worker.adapters["math"].candidates(status)[0]
        pointer = load_json(worker.store.math_shadow_latest_path(capture_id))
        package_path = Path(pointer["package_path"])
        package = load_json(package_path)
        group_pointer = load_json(
            worker.store.math_shadow_group_latest_path(
                pointer["group_processing_key"]
            )
        )
        artifact_paths = [
            worker.store.report_json_path(package["report_json_sha256"]),
            worker.store.report_markdown_path(package["report_markdown_sha256"]),
            worker.store.report_stage_path(
                package["stage_receipts"]["analysis"]["artifact_sha256"]
            ),
            worker.store.report_stage_path(
                package["stage_receipts"]["critical_review"]["artifact_sha256"]
            ),
            Path(group_pointer["result_path"]),
        ]
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "ready")
        self.assertEqual(worker._math_shadow_evaluation_progress()["processed"], 2)
        for artifact_path in artifact_paths:
            with self.subTest(artifact=artifact_path.name):
                original = artifact_path.read_bytes()
                artifact_path.unlink()
                self.assertEqual(
                    worker._math_shadow_state(base_candidate)[0], "stale"
                )
                self.assertEqual(
                    worker._math_shadow_evaluation_progress()["processed"], 0
                )
                artifact_path.write_bytes(original)
                self.assertEqual(
                    worker._math_shadow_state(base_candidate)[0], "ready"
                )

        original_package = package_path.read_bytes()
        tampered_package = copy.deepcopy(package)
        tampered_package["quality_receipt"]["char_count"] += 1
        atomic_write_json(package_path, tampered_package)
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "stale")
        self.assertEqual(worker._math_shadow_evaluation_progress()["processed"], 0)
        package_path.write_bytes(original_package)
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "ready")

        generation_path = worker.store.math_shadow_group_publication_path(
            pointer["group_generation_id"]
        )
        generation_bytes = generation_path.read_bytes()
        generation_path.unlink()
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "stale")
        self.assertEqual(worker._math_shadow_evaluation_progress()["processed"], 0)
        generation_path.write_bytes(generation_bytes)
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "ready")

        other_capture_id = self._capture(2)["event_id"]
        other_pointer_path = worker.store.math_shadow_latest_path(other_capture_id)
        other_pointer_bytes = other_pointer_path.read_bytes()
        other_pointer_path.unlink()
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "stale")
        self.assertEqual(worker._math_shadow_evaluation_progress()["processed"], 0)
        other_pointer_path.write_bytes(other_pointer_bytes)
        self.assertEqual(worker._math_shadow_state(base_candidate)[0], "ready")

    def legacy_contract_future20_eligibility_uses_hashed_generation_member(self) -> None:
        def isolated_config(name: str, start_at: str) -> dict:
            config = copy.deepcopy(self.config)
            runtime = self.base / name
            config["runtime_root"] = str(runtime)
            config["worker"]["log_path"] = str(runtime / "logs/worker.log")
            config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
            config["dashboard"]["projection_path"] = str(
                runtime / "state/dashboard_projection.json"
            )
            config["math_deep_v2"]["shadow_evaluation_start_at"] = start_at
            return config

        before_config = isolated_config(
            "runtime-before-window", "2026-08-05T09:00:00+08:00"
        )
        before_worker = Worker(before_config, model_runner=FakeMathRunner())
        before_worker.run_once(
            subject="math", study_date="2026-08-05", force=True
        )
        before_status = before_worker.adapters["math"].status("2026-08-05")
        before_base = before_worker.adapters["math"].candidates(before_status)[0]
        before_pointer_path = before_worker.store.math_shadow_latest_path(
            before_base.capture_id
        )
        original_before = load_json(before_pointer_path)
        self.assertFalse(original_before["evaluation_eligible"])
        self.assertEqual(
            before_worker._math_shadow_evaluation_progress()["processed"], 0
        )

        forged_after = copy.deepcopy(original_before)
        forged_after["capture_recorded_at"] = "2026-08-05T02:00:00Z"
        forged_after["evaluation_eligible"] = True
        atomic_write_json(before_pointer_path, forged_after)
        self.assertEqual(before_worker._math_shadow_state(before_base)[0], "stale")
        self.assertEqual(
            before_worker._math_shadow_evaluation_progress()["processed"], 0
        )

        eligibility_only = copy.deepcopy(original_before)
        eligibility_only["evaluation_eligible"] = True
        atomic_write_json(before_pointer_path, eligibility_only)
        self.assertEqual(before_worker._math_shadow_state(before_base)[0], "ready")
        self.assertEqual(
            before_worker._math_shadow_evaluation_progress()["processed"], 0
        )

        forged_history = copy.deepcopy(original_before)
        forged_history["historical_replay"] = True
        atomic_write_json(before_pointer_path, forged_history)
        self.assertEqual(before_worker._math_shadow_state(before_base)[0], "stale")
        self.assertEqual(
            before_worker._math_shadow_evaluation_progress()["processed"], 0
        )

        after_config = isolated_config(
            "runtime-after-window", "2026-08-05T00:00:00+08:00"
        )
        after_worker = Worker(after_config, model_runner=FakeMathRunner())
        after_worker.run_once(
            subject="math", study_date="2026-08-05", force=True
        )
        after_status = after_worker.adapters["math"].status("2026-08-05")
        after_base = after_worker.adapters["math"].candidates(after_status)[0]
        after_pointer_path = after_worker.store.math_shadow_latest_path(
            after_base.capture_id
        )
        original_after = load_json(after_pointer_path)
        self.assertTrue(original_after["evaluation_eligible"])
        self.assertEqual(
            after_worker._math_shadow_evaluation_progress()["processed"], 1
        )

        forged_before = copy.deepcopy(original_after)
        forged_before["capture_recorded_at"] = "2026-08-04T15:00:00Z"
        forged_before["evaluation_eligible"] = False
        atomic_write_json(after_pointer_path, forged_before)
        self.assertEqual(after_worker._math_shadow_state(after_base)[0], "stale")
        self.assertEqual(
            after_worker._math_shadow_evaluation_progress()["processed"], 0
        )

        eligibility_only = copy.deepcopy(original_after)
        eligibility_only["evaluation_eligible"] = False
        atomic_write_json(after_pointer_path, eligibility_only)
        self.assertEqual(after_worker._math_shadow_state(after_base)[0], "ready")
        self.assertEqual(
            after_worker._math_shadow_evaluation_progress()["processed"], 1
        )

    def legacy_contract_future20_requires_current_contract_and_live_baseline(self) -> None:
        worker = Worker(self.config, model_runner=FakeMathRunner())
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        self.assertEqual(worker._math_shadow_evaluation_progress()["processed"], 1)

        self._write_status(0)
        absent_worker = Worker(self.config, model_runner=FakeMathRunner())
        self.assertEqual(
            absent_worker._math_shadow_evaluation_progress()["processed"], 1
        )

        start_drift = copy.deepcopy(self.config)
        start_drift["math_deep_v2"]["shadow_evaluation_start_at"] = (
            "2026-08-05T09:00:00+08:00"
        )
        self.assertEqual(
            Worker(
                start_drift, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            0,
        )

        prompt_drift = copy.deepcopy(self.config)
        prompt_drift["math_deep_v2"]["analysis_prompt_version"] = (
            "test-math-analysis-v2-drift"
        )
        self.assertEqual(
            Worker(
                prompt_drift, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            0,
        )

        timeout_drift = copy.deepcopy(self.config)
        timeout_drift["math_deep_v2"]["soft_runtime_warning_seconds"] = 1799
        self.assertEqual(
            Worker(
                timeout_drift, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            0,
        )

        schema_drift = copy.deepcopy(self.config)
        schema_copy = self.base / "math-analysis-schema-drift.json"
        schema_value = load_json(ROOT / "schemas/luna-math-analysis-v2.json")
        schema_value["title"] = "drifted schema for contract test"
        atomic_write_json(schema_copy, schema_value)
        schema_drift["math_deep_v2"]["analysis_output_schema"] = str(
            schema_copy
        )
        self.assertEqual(
            Worker(
                schema_drift, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            0,
        )

        capture_id = self._capture(1)["event_id"]
        baseline_job_path = absent_worker.store.job_path("math", capture_id)
        baseline_job_bytes = baseline_job_path.read_bytes()
        baseline_job_path.unlink()
        self.assertEqual(
            Worker(
                self.config, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            0,
        )
        baseline_job_path.write_bytes(baseline_job_bytes)
        self.assertEqual(
            Worker(
                self.config, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["processed"],
            1,
        )

    def legacy_contract_future20_failure_receipt_rejects_forgery_and_drift(self) -> None:
        class FailingMathRunner(FakeMathRunner):
            def run_math_v2(inner_self, candidate):
                inner_self.math_calls += 1
                raise PreprocessorError("synthetic_math_shadow_failure")

        worker = Worker(self.config, model_runner=FailingMathRunner())
        worker.run_once(
            subject="math", study_date="2026-08-05", force=True
        )
        result = worker.run_once(
            subject="math", study_date="2026-08-05", force=True
        )
        self.assertEqual(result["processed"][0]["status"], "shadow_retrying")
        progress = worker._math_shadow_evaluation_progress()
        self.assertEqual((progress["processed"], progress["failures"]), (0, 1))

        capture_id = self._capture(1)["event_id"]
        latest_path = worker.store.math_shadow_failure_path(
            "2026-08-05", capture_id
        )
        latest_bytes = latest_path.read_bytes()
        latest = load_json(latest_path)
        receipt_path = Path(latest["receipt_path"])
        receipt_bytes = receipt_path.read_bytes()

        atomic_write_json(
            latest_path,
            {
                "capture_id": capture_id,
                "study_date": "2026-08-05",
                "status": "failed",
                "capture_recorded_at": "2026-08-05T02:00:00Z",
                "historical_replay": False,
                "evaluation_eligible": True,
            },
        )
        self.assertEqual(worker._math_shadow_evaluation_progress()["failures"], 0)
        latest_path.write_bytes(latest_bytes)

        tampered_receipt = load_json(receipt_path)
        tampered_receipt["capture_recorded_at"] = "2026-08-05T02:00:00Z"
        atomic_write_json(receipt_path, tampered_receipt)
        self.assertEqual(worker._math_shadow_evaluation_progress()["failures"], 0)
        receipt_path.write_bytes(receipt_bytes)
        self.assertEqual(worker._math_shadow_evaluation_progress()["failures"], 1)

        old_contract = copy.deepcopy(self.config)
        old_contract["math_deep_v2"]["critical_review_prompt_version"] = (
            "test-math-review-v2-drift"
        )
        self.assertEqual(
            Worker(
                old_contract, model_runner=FakeMathRunner()
            )._math_shadow_evaluation_progress()["failures"],
            0,
        )

        stale_status = load_json(self.status_path)
        stale_status["pending"][0]["effective_evidence_hash"] = "9" * 64
        atomic_write_json(self.status_path, stale_status)
        stale_worker = Worker(self.config, model_runner=FakeMathRunner())
        canonical = stale_worker.adapters["math"].status("2026-08-05")
        stale_worker.adapters["math"].candidates(canonical)
        self.assertEqual(
            stale_worker._math_shadow_evaluation_progress()["failures"], 0
        )

    def legacy_contract_requested_luna_max_receipts_are_preserved_without_fabricated_attestation(self) -> None:
        runner = FakeMathRunner(confirmed=False)
        worker = Worker(self.config, model_runner=runner)
        status = worker.adapters["math"].status("2026-08-05")
        base = worker.adapters["math"].candidates(status)[0]
        deep = worker.adapters["math"].deep_candidate(base)
        result = runner.run_math_v2(deep)
        canary = copy.deepcopy(self.config)
        canary["math_deep_v2"]["mode"] = "canary"
        canary_worker = Worker(canary, model_runner=runner)
        canary_status = canary_worker.adapters["math"].status("2026-08-05")
        canary_base = canary_worker.adapters["math"].candidates(canary_status)[0]
        canary_deep = canary_worker.adapters["math"].deep_candidate(canary_base)
        artifacts = canary_worker._write_math_v2_artifacts(
            canary_deep, result
        )
        package = artifacts[0]
        self.assertEqual(package["model"], "gpt-5.6-luna")
        self.assertEqual(package["reasoning_effort"], "max")
        self.assertEqual(
            package["stage_receipts"]["analysis"][
                "runtime_identity_status"
            ],
            "requested_unverified",
        )
        self.assertTrue(package["quality_receipt"]["consumable"])
        self.assertFalse(package["quality_receipt"]["runtime_identity_attested"])
        self.assertEqual(
            package["quality_receipt"]["runtime_display"], "运行时未确认"
        )
        self.assertTrue(
            canary_worker.store.report_json_path(
                package["report_json_sha256"]
            ).is_file()
        )
        for stage in ("analysis", "critical_review"):
            self.assertTrue(
                canary_worker.store.report_stage_path(
                    package["stage_receipts"][stage]["artifact_sha256"]
                ).is_file()
            )

        shadow_worker = Worker(self.config, model_runner=runner)
        shadow_worker.run_once(subject="math", study_date="2026-08-05", force=True)
        projection = load_json(Path(self.config["dashboard"]["projection_path"]))
        item = projection["subjects"]["math"]["items"][0]
        self.assertEqual(item["processor_attribution"], "requested_unverified")
        self.assertEqual(item["runtime_identity_status"], "requested_unverified")

    def legacy_contract_math_runtime_identity_controls_consumption_end_to_end(self) -> None:
        capture_id = self._capture(1)["event_id"]
        freeze_id = "FREEZE-RUNTIME-IDENTITY-001"
        effective_evidence_hash = self._capture(1)["effective_evidence_hash"]

        for confirmed in (False, True):
            with self.subTest(confirmed=confirmed):
                self._write_status(1)
                config = copy.deepcopy(self.config)
                runtime = self.base / (
                    "runtime-confirmed" if confirmed else "runtime-unverified"
                )
                config["runtime_root"] = str(runtime)
                config["worker"]["log_path"] = str(runtime / "logs/worker.log")
                config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
                config["dashboard"]["projection_path"] = str(
                    runtime / "state/dashboard_projection.json"
                )
                config["math_deep_v2"]["mode"] = "production"
                class ProductionRunner(FakeMathRunner):
                    def run(inner_self, candidate):
                        return inner_self.run_math_v2(candidate)

                runner = ProductionRunner(confirmed=confirmed)

                with mock.patch(
                    "preprocessor_core.CODEX_RUNTIME_ATTESTATION_SUPPORTED",
                    confirmed,
                ):
                    worker = Worker(config, model_runner=runner)
                    worker.run_once(
                        subject="math",
                        study_date="2026-08-05",
                        force=True,
                    )
                    job = worker.store.read_job("math", capture_id)
                    self.assertIsInstance(job, dict)
                    package = load_json(Path(str(job["package_path"])))
                    self.assertTrue(package["quality_receipt"]["consumable"])
                    self.assertEqual(
                        package["quality_receipt"]["runtime_identity"],
                        {
                            "analysis": (
                                "confirmed" if confirmed else "requested_unverified"
                            ),
                            "critical_review": (
                                "confirmed" if confirmed else "requested_unverified"
                            ),
                        },
                    )
                    for stage in ("analysis", "critical_review"):
                        receipt = package["stage_receipts"][stage]
                        self.assertEqual(
                            receipt["runtime_identity_status"],
                            "confirmed" if confirmed else "requested_unverified",
                        )
                        self.assertTrue(
                            worker.store.report_stage_path(
                                receipt["artifact_sha256"]
                            ).is_file()
                        )

                    canonical = load_json(self.status_path)
                    canonical["pending"][0]["active_freeze_ids"] = [freeze_id]
                    atomic_write_json(self.status_path, canonical)
                    consumed = consume_package(
                        config,
                        subject="math",
                        capture_id=capture_id,
                        study_date="2026-08-05",
                        freeze_id=freeze_id,
                        effective_evidence_hash=effective_evidence_hash,
                    )

                self.assertEqual(consumed["status"], "ready")
                self.assertIsNotNone(consumed["adoption_token"])
                self.assertEqual(
                    consumed["validation"]["runtime_identity_confirmed"],
                    confirmed,
                )

    def legacy_contract_production_routes_directly_to_two_pass_without_legacy_call(self) -> None:
        config = copy.deepcopy(self.config)
        config["math_deep_v2"]["mode"] = "production"

        class RoutingRunner(FakeMathRunner):
            def run(self, candidate):
                if candidate.input_binding.get("processing_contract_sha256"):
                    return self.run_math_v2(candidate)
                return super().run(candidate)

        runner = RoutingRunner()
        worker = Worker(config, model_runner=runner)
        worker.run_once(subject="math", study_date="2026-08-05", force=True)
        self.assertEqual(runner.baseline_calls, 0)
        self.assertEqual(runner.math_calls, 1)
        pointer = load_json(
            worker.store.latest_path("math", self._capture(1)["event_id"])
        )
        self.assertEqual(pointer["schema_version"], "study-intake-preprocess-latest-v2")
        self.assertEqual(pointer["pipeline_status"], "two_pass_ready")

    def test_production_scanner_keeps_ten_math_captures_independent(
        self,
    ) -> None:
        self._write_status(10)
        config = copy.deepcopy(self.config)
        config["math_deep_v2"]["mode"] = "production"
        frozen, decisions = scan_eligible_candidates(config, "math")
        self.assertEqual(len(frozen), 10, decisions)
        self.assertEqual(len(decisions), 10)
        self.assertEqual(len({row.task.unit_sha256 for row in frozen}), 10)
        self.assertEqual(
            len({row.task.frozen_payload_sha256 for row in frozen}), 10
        )
        self.assertEqual(
            len({row["content_processing_id"] for row in decisions}), 10
        )
        self.assertTrue(
            all(row["model_enqueue_allowed"] for row in decisions)
        )
        self.assertTrue(
            all(not row["reuses_group_owner"] for row in decisions)
        )
        self.assertTrue(
            all(
                row["group_owner_capture_id"] == row["capture_id"]
                for row in decisions
            )
        )
        for row in frozen:
            payload = row.task.frozen_payload
            self.assertEqual(
                payload["content_group_capture_ids"],
                [row.candidate.capture_id],
            )
            self.assertEqual(
                payload["dispatch_contract"]["math_group_capture_ids"],
                [row.candidate.capture_id],
            )
            self.assertEqual(len(payload["math_group_members"]), 1)

    def legacy_contract_production_math_checkpoint_crash_resumes_review_only(self) -> None:
        counter_path = self.base / "math-recovery-stage-counts.json"
        draft_path = self.base / "math-recovery-draft.json"
        atomic_write_json(
            draft_path, math_analysis("target_group.captures[0]")
        )
        fake_codex = self.base / "fake-codex-math-recovery"
        fake_codex.write_text(
            """#!/usr/bin/env python3
import json, pathlib, sys
counter_path = pathlib.Path(%r)
draft = json.loads(pathlib.Path(%r).read_text())
try:
    counts = json.loads(counter_path.read_text())
except FileNotFoundError:
    counts = {"analysis": 0, "critical_review": 0}
args = sys.argv[1:]
schema_path = pathlib.Path(args[args.index("--output-schema") + 1])
output_path = pathlib.Path(args[args.index("--output-last-message") + 1])
is_review = "critical_review" in schema_path.name
stage = "critical_review" if is_review else "analysis"
counts[stage] += 1
counter_path.write_text(json.dumps(counts, sort_keys=True))
if is_review:
    payload = {
        "schema_version": "study-intake-luna-math-critical-review-v2",
        "verdict": "pass",
        "summary": "独立批判审查完成。",
        "revised_analysis": draft,
        "relationship_decisions": [],
        "unsupported_claims": [],
        "evidence_misreads": [],
        "mathematical_errors": [],
        "visual_findings": [],
        "provenance_findings": [],
        "missing_analysis": [],
        "required_corrections": [],
        "sol_priority_checks": [],
        "correction_resolutions": [],
    }
else:
    payload = draft
output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))
"""
            % (str(counter_path), str(draft_path)),
            encoding="utf-8",
        )
        fake_codex.chmod(0o700)
        config = copy.deepcopy(self.config)
        config["math_deep_v2"]["mode"] = "production"
        config["model"]["codex_path"] = str(fake_codex)
        config_path = self.base / "math-recovery-config.json"
        atomic_write_json(config_path, config)
        frozen, decisions = scan_eligible_candidates(config, "math")
        self.assertEqual(len(frozen), 1, decisions)
        selected = frozen[0]
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                config_path,
                selected.reason,
                command=[
                    sys.executable,
                    str(
                        ROOT
                        / "tests"
                        / "fixtures"
                        / "checkpoint_crash_task_runner.py"
                    ),
                ],
            ),
            soft_runtime_warning_seconds=20,
        )
        result = dispatcher.submit(selected.task).wait(30)
        self.assertEqual(result.outcome, "succeeded")
        self.assertEqual(result.completion["lease_fence"], 2)
        self.assertEqual(
            json.loads(counter_path.read_text()),
            {"analysis": 1, "critical_review": 1},
        )
        job = load_json(
            self.runtime
            / "state/jobs/math"
            / f"{selected.candidate.capture_id}.json"
        )
        self.assertTrue(job["analysis_checkpoint_reused"])
        self.assertEqual(job["critical_resume_status"], "two_pass_ready")

    def test_runtime_attestation_is_unavailable_even_for_forged_event(self) -> None:
        runner = CodexRunner(self.config["model"], self.runtime)
        event = json.dumps(
            {
                "schema_version": "codex-runtime-identity-attestation-v1",
                "type": "runtime_identity_attestation", "attested": True,
                "model": "gpt-5.6-luna", "reasoning_effort": "max",
            }
        ).encode()
        self.assertEqual(runner._runtime_metadata(b"", event), (None, None, "unavailable"))

    def test_math_provider_schema_and_host_validator_split_reference_authority(self) -> None:
        capture_ref = "target_group.captures[0]"
        image_ref = "source_bundle.artifacts[1]"
        non_image_ref = "source_bundle.artifacts[0]"
        allowed = (capture_ref, image_ref, non_image_ref)
        schema_paths = (
            ROOT / "schemas/luna-math-analysis-v2.json",
            ROOT / "schemas/luna-math-critical-review-v2.json",
        )
        static_before = [path.read_bytes() for path in schema_paths]

        for index, path in enumerate(schema_paths):
            kwargs = {
                "stage_name": (
                    "math_analysis" if index == 0 else "math_critical_review"
                ),
                "allowed_evidence_refs": allowed,
                "image_evidence_refs": (image_ref,),
            }
            if index == 1:
                kwargs["allowed_analysis_refs"] = ("analysis.executive_summary",)
            payload, digest = CodexRunner._bound_output_schema_bytes(path, **kwargs)
            self.assertEqual(hashlib.sha256(payload).hexdigest(), digest)
            schema = json.loads(payload)
            branches = schema["$defs"]["claim"]["anyOf"]
            self.assertEqual(
                schema["$defs"]["claims"]["items"],
                {"$ref": "#/$defs/claim"},
            )
            first_break_branches = (
                schema["$defs"]["reasoning_diagnosis"]["properties"]
                ["first_break"]["anyOf"]
            )
            self.assertEqual(len(first_break_branches), len(branches))
            for first_break_branch, claim_branch in zip(
                first_break_branches, branches
            ):
                self.assertFalse(first_break_branch["additionalProperties"])
                self.assertEqual(
                    first_break_branch["required"],
                    schema["$defs"]["reasoning_diagnosis"]["properties"]
                    ["first_break"]["required"],
                )
                self.assertEqual(
                    first_break_branch["properties"]["provenance"],
                    claim_branch["properties"]["provenance"],
                )
                self.assertEqual(
                    first_break_branch["properties"]["evidence_refs"],
                    claim_branch["properties"]["evidence_refs"],
                )
            self.assertEqual(
                schema["$defs"]["non_image_evidence_ref"]["enum"],
                sorted((capture_ref, non_image_ref)),
            )
            # The Provider Schema must remain portable while the model calls
            # MCP: evidence refs returned during the stage are not knowable
            # when the request schema is submitted.  Exact membership is a
            # canonical host-validator responsibility below.
            self.assertNotIn("enum", schema["$defs"]["evidence_ref"])
            self.assertEqual(schema["$defs"]["evidence_ref"]["type"], "string")
            self.assertIn("pattern", schema["$defs"]["evidence_ref"])
            self.assertEqual(
                branches[0]["properties"]["evidence_refs"]["items"],
                {"$ref": "#/$defs/non_image_evidence_ref"},
            )
            self.assertEqual(
                branches[0]["properties"]["provenance"]["enum"],
                ["capture", "formal_card", "source_bundle", "model_inference"],
            )
            self.assertEqual(
                branches[1]["properties"]["evidence_refs"]["items"],
                {"$ref": "#/$defs/evidence_ref"},
            )
            self.assertEqual(
                branches[1]["properties"]["provenance"]["enum"],
                ["image", "mixed"],
            )
            self.assertNotIn(
                image_ref,
                schema["$defs"]["non_image_evidence_ref"]["enum"],
            )

        self.assertEqual([path.read_bytes() for path in schema_paths], static_before)

        mcp_ref = "mcp-item:math:" + "a" * 64
        alien_ref = "mcp-item:math:" + "b" * 64
        grounded = math_analysis(capture_ref)
        grounded_claim = grounded["concept_method_analysis"]["common_confusions"][0]
        grounded_claim["provenance"] = "model_inference"
        grounded_claim["evidence_refs"] = [mcp_ref]
        self.assertEqual(
            validate_math_analysis_v2(
                grounded,
                (capture_ref, mcp_ref),
                image_evidence_refs=(),
            ),
            grounded,
        )
        ungrounded = copy.deepcopy(grounded)
        ungrounded["concept_method_analysis"]["common_confusions"][0][
            "evidence_refs"
        ] = [alien_ref]
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_evidence_refs_invalid"
        ):
            validate_math_analysis_v2(
                ungrounded,
                (capture_ref, mcp_ref),
                image_evidence_refs=(),
            )

    def test_math_dynamic_schema_without_images_leaves_claim_shapes_unchanged(self) -> None:
        path = ROOT / "schemas/luna-math-analysis-v2.json"
        static = load_json(path)
        payload, _ = CodexRunner._bound_output_schema_bytes(
            path,
            stage_name="math_analysis",
            allowed_evidence_refs=("target_group.captures[0]",),
            image_evidence_refs=(),
        )
        schema = json.loads(payload)

        def without_integrity_patterns(value):
            if isinstance(value, dict):
                return {
                    key: without_integrity_patterns(nested)
                    for key, nested in value.items()
                    if key != "pattern"
                }
            if isinstance(value, list):
                return [without_integrity_patterns(nested) for nested in value]
            return value

        self.assertEqual(
            without_integrity_patterns(schema["$defs"]["claim"]),
            static["$defs"]["claim"],
        )
        self.assertEqual(
            without_integrity_patterns(
                schema["$defs"]["reasoning_diagnosis"]["properties"]["first_break"]
            ),
            static["$defs"]["reasoning_diagnosis"]["properties"]["first_break"],
        )

    def test_math_dynamic_schema_max_refs_stays_within_enum_budget(self) -> None:
        allowed = tuple(f"target_group.evidence[{index:03d}]" for index in range(192))
        images = allowed[:4]
        analysis_refs = tuple(
            f"analysis.section_{index:03d}" for index in range(192)
        )

        def enum_count(value) -> int:
            if isinstance(value, dict):
                return sum(
                    len(nested) if key == "enum" and isinstance(nested, list)
                    else enum_count(nested)
                    for key, nested in value.items()
                )
            if isinstance(value, list):
                return sum(enum_count(nested) for nested in value)
            return 0

        for stage_name, path, extra in (
            (
                "math_analysis",
                ROOT / "schemas/luna-math-analysis-v2.json",
                {},
            ),
            (
                "math_critical_review",
                ROOT / "schemas/luna-math-critical-review-v2.json",
                {"allowed_analysis_refs": analysis_refs},
            ),
        ):
            with self.subTest(stage_name=stage_name):
                payload, _ = CodexRunner._bound_output_schema_bytes(
                    path,
                    stage_name=stage_name,
                    allowed_evidence_refs=allowed,
                    image_evidence_refs=images,
                    **extra,
                )
                self.assertLessEqual(enum_count(json.loads(payload)), 1000)

    def test_dynamic_schema_rejects_enum_budget_before_model_call(self) -> None:
        schema = load_json(ROOT / "schemas/luna-math-analysis-v2.json")
        schema["$defs"]["budget_probe"] = {
            "type": "string",
            "enum": [f"value-{index}" for index in range(1001)],
        }
        path = self.base / "over-budget-schema.json"
        atomic_write_json(path, schema)
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_output_schema_enum_value_limit_exceeded",
        ):
            CodexRunner._bound_output_schema_bytes(
                path,
                stage_name="math_analysis",
                allowed_evidence_refs=("target_group.captures[0]",),
            )

    def test_math_dynamic_schema_rejects_controls_in_all_free_text(self) -> None:
        capture_ref = "target_group.captures[0]"
        analysis_ref = "analysis.executive_summary"
        schemas = {}
        for stage_name, relative_path in (
            ("math_analysis", "schemas/luna-math-analysis-v2.json"),
            ("math_critical_review", "schemas/luna-math-critical-review-v2.json"),
        ):
            kwargs = {
                "stage_name": stage_name,
                "allowed_evidence_refs": (capture_ref,),
            }
            if stage_name == "math_critical_review":
                kwargs["allowed_analysis_refs"] = (analysis_ref,)
            payload, _ = CodexRunner._bound_output_schema_bytes(
                ROOT / relative_path,
                **kwargs,
            )
            schemas[stage_name] = json.loads(payload)

        analysis = schemas["math_analysis"]
        critical = schemas["math_critical_review"]

        def assert_all_free_text_is_bounded(value, path="root"):
            if isinstance(value, dict):
                raw_type = value.get("type")
                is_string = raw_type == "string" or (
                    isinstance(raw_type, list) and "string" in raw_type
                )
                if is_string:
                    with self.subTest(schema_path=path):
                        if "enum" in value or "const" in value:
                            self.assertNotIn("pattern", value)
                        else:
                            self.assertIn("pattern", value)
                for key, nested in value.items():
                    assert_all_free_text_is_bounded(nested, f"{path}.{key}")
            elif isinstance(value, list):
                for index, nested in enumerate(value):
                    assert_all_free_text_is_bounded(nested, f"{path}[{index}]")

        assert_all_free_text_is_bounded(analysis, "analysis")
        assert_all_free_text_is_bounded(critical, "critical")
        text_schemas = (
            analysis["properties"]["executive_summary"],
            analysis["$defs"]["claim"]["properties"]["text"],
            analysis["$defs"]["claim"]["properties"]
            ["counterevidence_or_boundary"],
            analysis["$defs"]["claim"]["properties"]["sol_verification_action"],
            critical["properties"]["summary"],
            critical["$defs"]["finding"]["properties"]["text"],
            critical["$defs"]["analysis"]["properties"]["executive_summary"],
            critical["$defs"]["claim"]["properties"]["text"],
        )
        for text_schema in text_schemas:
            pattern = text_schema["pattern"]
            self.assertIsNone(re.fullmatch(pattern, "错误\t文本"))
            self.assertIsNotNone(re.fullmatch(pattern, r"保留合法的 \theta"))
            self.assertIsNotNone(re.fullmatch(pattern, "第一行\n第二行"))

        self.assertIn("pattern", analysis["$defs"]["evidence_ref"])
        self.assertIn("pattern", critical["$defs"]["evidence_ref"])
        self.assertNotIn("pattern", critical["$defs"]["analysis_ref"])
        self.assertEqual(
            critical["$defs"]["analysis"]["properties"]["executive_summary"]
            ["pattern"],
            critical["$defs"]["finding"]["properties"]["text"]["pattern"],
        )

    def test_math_critical_review_rejects_controls_after_schema_boundary(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = result.draft_analysis
        self.assertIsInstance(draft, dict)
        mutations = (
            ("summary_tab", lambda review: review.__setitem__("summary", "bad\ttext")),
            ("summary_del", lambda review: review.__setitem__("summary", "bad\x7ftext")),
            (
                "finding_control",
                lambda review: review["unsupported_claims"].append(
                    {
                        "finding_id": "FIND-CONTROL",
                        "severity": "warning",
                        "text": "bad\x01text",
                        "analysis_refs": [],
                        "evidence_refs": [],
                        "affected_json_paths": [],
                    }
                ),
            ),
        )
        for name, mutate in mutations:
            with self.subTest(name=name):
                review = copy.deepcopy(result.critical_review)
                mutate(review)
                with self.assertRaisesRegex(
                    PreprocessorError, "math_analysis_control_character_invalid"
                ):
                    validate_math_critical_review_v2(
                        review,
                        allowed_evidence_refs=candidate.allowed_evidence_refs,
                        draft_analysis=draft,
                        candidate=candidate,
                        relationship_context=result.relationship_context,
                    )

    def test_math_patch_only_critical_review_reuses_validated_draft(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = copy.deepcopy(result.draft_analysis)
        review = {
            key: copy.deepcopy(value)
            for key, value in result.critical_review.items()
            if key != "revised_analysis"
        }
        review["schema_version"] = "study-intake-luna-math-critical-review-v3"
        validated = validate_math_critical_review_v2(
            review,
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            draft_analysis=draft,
            candidate=candidate,
            relationship_context=result.relationship_context,
        )
        self.assertEqual(
            validated["schema_version"],
            "study-intake-luna-math-critical-review-v3",
        )
        self.assertEqual(validated["revised_analysis"], draft)

    def test_math_patch_only_provider_schema_binds_writable_paths(self) -> None:
        payload, _ = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas/luna-math-critical-review-v3.json",
            stage_name="math_critical_review",
            allowed_evidence_refs=("target_group.captures[0]",),
            allowed_analysis_refs=("analysis.executive_summary",),
            allowed_correction_paths=("$.executive_summary",),
            image_evidence_refs=("target_group.captures[0]",),
        )
        schema = json.loads(payload)
        self.assertNotIn("revised_analysis", schema["properties"])
        self.assertNotIn("analysis", schema["$defs"])
        self.assertEqual(
            schema["$defs"]["writable_correction_path"]["enum"],
            ["$.executive_summary"],
        )

    def test_math_atomic_signal_edits_use_scalar_leaf_paths(self) -> None:
        draft = math_analysis("target_group.captures[0]")
        paths = set(_math_writable_correction_paths(draft))
        self.assertIn("$.atomic_signals", paths)
        self.assertNotIn("$.atomic_signals[0]", paths)
        self.assertIn("$.atomic_signals[0].applicability_boundary", paths)
        self.assertIn("$.atomic_signals[0].error_role", paths)
        self.assertNotIn("$.atomic_signals[0].evidence_refs", paths)

    def test_math_atomic_signal_leaf_delta_preserves_host_defaults(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = copy.deepcopy(result.draft_analysis)
        path = "$.atomic_signals[0].applicability_boundary"
        before = draft["atomic_signals"][0]["applicability_boundary"]
        after = "仅适用于当前冻结题源及其明确变量角色。"

        def delta(value):
            return {
                "encoding": "canonical_json",
                "canonical_json": json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }

        finding = {
            "finding_id": "MATH-LEAF-001",
            "severity": "blocking",
            "text": "收窄 atomic signal 的适用边界。",
            "analysis_refs": ["analysis.atomic_signals[0]"],
            "evidence_refs": [candidate.allowed_evidence_refs[0]],
            "affected_json_paths": [path],
        }
        review = {
            key: copy.deepcopy(value)
            for key, value in result.critical_review.items()
            if key != "revised_analysis"
        }
        review["schema_version"] = "study-intake-luna-math-critical-review-v3"
        review["verdict"] = "pass_with_warnings"
        review["required_corrections"] = [finding]
        review["correction_resolutions"] = [
            {
                "finding_id": finding["finding_id"],
                "resolution": "applied",
                "affected_json_paths": [path],
                "before": delta(before),
                "after": delta(after),
                "reason": "仅替换主机允许的最窄标量叶子。",
            }
        ]
        validated = validate_math_critical_review_v2(
            review,
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            draft_analysis=draft,
            candidate=candidate,
            relationship_context=result.relationship_context,
        )
        signal = validated["revised_analysis"]["atomic_signals"][0]
        self.assertEqual(signal["applicability_boundary"], after)
        self.assertEqual(signal["error_role"], "none")

    def test_math_patch_only_critical_review_applies_exact_delta(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = copy.deepcopy(result.draft_analysis)
        path = "$.formalization_candidates.safe_summary"
        before = copy.deepcopy(draft["formalization_candidates"]["safe_summary"])
        after = copy.deepcopy(before)
        after[0]["text"] = "修正后的安全摘要仍只绑定冻结证据，并等待 Sol 独立核验。"

        def delta(value):
            return {
                "encoding": "canonical_json",
                "canonical_json": json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }

        finding = {
            "finding_id": "MATH-PATCH-001",
            "severity": "blocking",
            "text": "安全摘要需要缩小结论边界。",
            "analysis_refs": [
                "analysis.formalization_candidates.safe_summary[0]"
            ],
            "evidence_refs": [candidate.allowed_evidence_refs[0]],
            "affected_json_paths": [path],
        }
        review = {
            key: copy.deepcopy(value)
            for key, value in result.critical_review.items()
            if key != "revised_analysis"
        }
        review["schema_version"] = "study-intake-luna-math-critical-review-v3"
        review["verdict"] = "pass_with_warnings"
        review["required_corrections"] = [finding]
        review["correction_resolutions"] = [
            {
                "finding_id": finding["finding_id"],
                "resolution": "applied",
                "affected_json_paths": [path],
                "before": delta(before),
                "after": delta(after),
                "reason": "使用主机允许的最窄完整列表路径。",
            }
        ]
        validated = validate_math_critical_review_v2(
            review,
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            draft_analysis=draft,
            candidate=candidate,
            relationship_context=result.relationship_context,
        )
        self.assertEqual(
            validated["revised_analysis"]["formalization_candidates"][
                "safe_summary"
            ],
            after,
        )
        self.assertEqual(
            draft["formalization_candidates"]["safe_summary"], before
        )

    def test_math_patch_repairs_only_unescaped_nested_math_delimiters(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = copy.deepcopy(result.draft_analysis)
        path = "$.formalization_candidates.safe_summary"
        before = copy.deepcopy(draft["formalization_candidates"]["safe_summary"])
        after = copy.deepcopy(before)
        after[0]["text"] = r"复核 \(n!\) 后仍只形成 proposal。"

        def delta(value):
            canonical = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            return {
                "encoding": "canonical_json",
                "canonical_json": canonical,
            }

        malformed_after = delta(after)
        malformed_after["canonical_json"] = malformed_after[
            "canonical_json"
        ].replace(r"\\(", r"\(").replace(r"\\)", r"\)")
        with self.assertRaises(json.JSONDecodeError):
            json.loads(malformed_after["canonical_json"])

        finding = {
            "finding_id": "MATH-NESTED-ESCAPE-001",
            "severity": "blocking",
            "text": "保留数学定界符并收窄摘要。",
            "analysis_refs": [
                "analysis.formalization_candidates.safe_summary[0]"
            ],
            "evidence_refs": [candidate.allowed_evidence_refs[0]],
            "affected_json_paths": [path],
        }
        review = {
            key: copy.deepcopy(value)
            for key, value in result.critical_review.items()
            if key != "revised_analysis"
        }
        review["schema_version"] = "study-intake-luna-math-critical-review-v3"
        review["verdict"] = "pass_with_warnings"
        review["required_corrections"] = [finding]
        review["correction_resolutions"] = [
            {
                "finding_id": finding["finding_id"],
                "resolution": "applied",
                "affected_json_paths": [path],
                "before": delta(before),
                "after": malformed_after,
                "reason": "外层 JSON 合法，但内层行内数学定界符少一层转义。",
            }
        ]
        validated = validate_math_critical_review_v2(
            review,
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            draft_analysis=draft,
            candidate=candidate,
            relationship_context=result.relationship_context,
        )
        self.assertEqual(
            validated["revised_analysis"]["formalization_candidates"][
                "safe_summary"
            ],
            after,
        )
        self.assertEqual(
            validated["correction_resolutions"][0]["after"], delta(after)
        )

        invalid_review = copy.deepcopy(review)
        invalid_review["correction_resolutions"][0]["after"] = {
            "encoding": "canonical_json",
            "canonical_json": r'"非法 \q 转义"',
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_critical_review_correction_delta_invalid",
        ):
            validate_math_critical_review_v2(
                invalid_review,
                allowed_evidence_refs=candidate.allowed_evidence_refs,
                draft_analysis=draft,
                candidate=candidate,
                relationship_context=result.relationship_context,
            )

    def test_math_patch_coalesces_only_exact_split_path_partition(self) -> None:
        def delta(value):
            return {
                "encoding": "canonical_json",
                "canonical_json": json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }

        rows = [
            {
                "finding_id": "MATH-SPLIT-001",
                "resolution": "applied",
                "affected_json_paths": ["$.alpha"],
                "before": delta([]),
                "after": delta(["alpha"]),
                "reason": "第一条单路径修正。",
            },
            {
                "finding_id": "MATH-SPLIT-001",
                "resolution": "applied",
                "affected_json_paths": ["$.beta"],
                "before": delta("old"),
                "after": delta("new"),
                "reason": "第二条单路径修正。",
            },
        ]
        coalesced = _coalesce_math_split_correction_resolutions(
            rows,
            finding_paths={"MATH-SPLIT-001": ["$.beta", "$.alpha"]},
        )
        self.assertEqual(len(coalesced), 1)
        self.assertEqual(
            coalesced[0]["affected_json_paths"], ["$.beta", "$.alpha"]
        )
        self.assertEqual(
            json.loads(coalesced[0]["before"]["canonical_json"]),
            {"$.alpha": [], "$.beta": "old"},
        )
        self.assertEqual(
            json.loads(coalesced[0]["after"]["canonical_json"]),
            {"$.alpha": ["alpha"], "$.beta": "new"},
        )

        with self.assertRaisesRegex(
            PreprocessorError,
            "math_critical_review_correction_paths_mismatch",
        ):
            _coalesce_math_split_correction_resolutions(
                rows[:1],
                finding_paths={
                    "MATH-SPLIT-001": ["$.alpha", "$.beta"]
                },
            )

    def test_math_critical_review_rejects_corrupted_inline_latex(self) -> None:
        candidate, result = model_driven_math_validator_fixture()
        draft = result.draft_analysis
        self.assertIsInstance(draft, dict)
        mutations = (
            (
                "line_split",
                "复核 \\(1-e^{-2\\\npi}\\)。",
                "math_analysis_latex_line_split_invalid",
            ),
            (
                "double_escape",
                r"复核 \(1-e^{-2\\pi}\)。",
                "math_analysis_latex_double_escape_invalid",
            ),
            (
                "unclosed_open",
                r"复核 \(1-e^{-2\pi}。",
                "math_analysis_inline_math_delimiter_invalid",
            ),
            (
                "orphan_close",
                r"复核 1-e^{-2\pi}\)。",
                "math_analysis_inline_math_delimiter_invalid",
            ),
            (
                "command_outside",
                r"复核 \pi。",
                "math_analysis_latex_command_outside_delimiter",
            ),
            (
                "double_command_outside",
                r"复核 \\pi。",
                "math_analysis_latex_command_outside_delimiter",
            ),
        )
        for name, malformed, error_code in mutations:
            with self.subTest(name=name):
                review = copy.deepcopy(result.critical_review)
                review["revised_analysis"]["correct_reasoning_reconstruction"][0][
                    "sol_verification_action"
                ] = malformed
                with self.assertRaisesRegex(PreprocessorError, error_code):
                    validate_math_critical_review_v2(
                        review,
                        allowed_evidence_refs=candidate.allowed_evidence_refs,
                        draft_analysis=draft,
                        candidate=candidate,
                        relationship_context=result.relationship_context,
                    )

    def test_math_model_transport_repairs_bounded_double_json_encoding(self) -> None:
        raw = (
            "来源解析：\\n$$\\n"
            r"\\frac{d^n y}{dx^n}=n!\\left["
            r"\\frac{(-1)^n}{x^{n+1}}+\\frac{1}{(1-x)^{n+1}}"
            r"\\right]"
            "\\n$$"
        )
        normalized = _normalize_math_model_text_transport(raw)
        self.assertEqual(
            normalized,
            "来源解析：\n"
            r"\(\frac{d^n y}{dx^n}=n!\left["
            r"\frac{(-1)^n}{x^{n+1}}+\frac{1}{(1-x)^{n+1}}"
            r"\right]\)",
        )
        validate_math_text_integrity(normalized)

    def test_math_model_transport_keeps_unknown_command_fail_closed(self) -> None:
        normalized = _normalize_math_model_text_transport(
            r"非法块：$$\\q{x}$$"
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_latex_double_escape_invalid",
        ):
            validate_math_text_integrity(normalized)

    def test_math_model_transport_canonicalizes_only_bare_ellipsis(self) -> None:
        raw = {
            "analysis": [
                r"条件为 n=1,2,\ldots，继续讨论。",
                r"合法公式为 \(n=1,2,\ldots\)。",
            ],
            "unsafe": [
                r"双转义 \\ldots。",
                r"命令前缀 \ldotsfoo。",
                r"参数形式 \ldots{x}。",
                r"其他命令 \pi。",
                r"未知命令 \write。",
            ],
        }
        before = copy.deepcopy(raw)
        classified = normalize_math_format_warnings(raw)
        normalized = classified["normalized_payload"]
        self.assertEqual(raw, before)
        self.assertEqual(normalized["analysis"][0], "条件为 n=1,2,…，继续讨论。")
        self.assertEqual(normalized["analysis"][1], raw["analysis"][1])
        self.assertEqual(
            classified["format_status"],
            "completed_needs_review",
        )
        self.assertEqual(classified["format_warning_count"], 2)
        validate_math_text_integrity(
            normalized["analysis"],
            format_warnings=tuple(
                {
                    **warning,
                    "path": "$" + warning["path"].removeprefix(
                        "$.analysis"
                    ),
                }
                for warning in classified["warnings"]
                if warning["path"].startswith("$.analysis")
            ),
        )
        self.assertEqual(normalized["unsafe"], raw["unsafe"])
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_latex_command_outside_delimiter",
        ):
            validate_math_text_integrity(
                normalized,
                format_warnings=classified["warnings"],
            )
        for text in normalized["unsafe"]:
            with self.subTest(text=text), self.assertRaises(PreprocessorError):
                validate_math_text_integrity(text)

        unbalanced = r"未闭合 \(n=1,2,\ldots"
        self.assertEqual(
            _normalize_math_model_text_transport(unbalanced),
            unbalanced,
        )
        ambiguous = normalize_math_format_warnings({"text": unbalanced})
        self.assertEqual(
            ambiguous["format_status"], "completed_needs_review"
        )
        validate_math_text_integrity(
            ambiguous["normalized_payload"],
            format_warnings=ambiguous["warnings"],
        )

    def test_math_review_canonical_json_normalizes_only_bare_ellipsis(self) -> None:
        encoded = {
            "encoding": "canonical_json",
            "canonical_json": json.dumps(
                {"text": r"枚举 n=1,2,\ldots。"},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        resolutions = [{"before": encoded, "after": encoded}]
        classified = normalize_math_format_warnings(
            {"correction_resolutions": resolutions}
        )
        normalized = classified["normalized_payload"][
            "correction_resolutions"
        ]
        self.assertEqual(resolutions[0]["before"], encoded)
        self.assertEqual(
            classified["format_status"],
            "completed_with_format_warnings",
        )
        self.assertEqual(classified["format_warning_count"], 2)
        for field in ("before", "after"):
            decoded = decode_correction_delta(normalized[0][field])
            self.assertEqual(decoded, {"text": "枚举 n=1,2,…。"})
            validate_math_text_integrity(decoded)
        dangerous = normalize_math_format_warnings(
            {
                "correction_resolutions": [
                    {
                        "before": encoded,
                        "after": {
                            "encoding": "canonical_json",
                            "canonical_json": json.dumps(
                                {"text": "危险 " + chr(92) + "write18{x}"},
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        },
                    }
                ]
            }
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_latex_command_outside_delimiter",
        ):
            validate_math_text_integrity(
                dangerous["normalized_payload"],
                format_warnings=dangerous["warnings"],
            )

    def test_math_validator_requires_exact_mixed_provenance_binding(self) -> None:
        non_image_ref = "capture.event_id"
        image_ref = "source_bundle.artifacts[0]"
        allowed = (non_image_ref, image_ref)
        for refs in ([image_ref], [non_image_ref]):
            with self.subTest(refs=refs):
                analysis = math_analysis(non_image_ref)
                claim = analysis["question_structure"]["objects"][0]
                claim["provenance"] = "mixed"
                claim["evidence_refs"] = refs
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_analysis_mixed_provenance_binding_invalid",
                ):
                    validate_math_analysis_v2(
                        analysis,
                        allowed,
                        image_evidence_refs=(image_ref,),
                    )

        analysis = math_analysis(non_image_ref)
        claim = analysis["question_structure"]["objects"][0]
        claim["provenance"] = "mixed"
        claim["evidence_refs"] = [image_ref, non_image_ref]
        self.assertEqual(
            validate_math_analysis_v2(
                analysis,
                allowed,
                image_evidence_refs=(image_ref,),
            ),
            analysis,
        )

    def test_math_host_canonicalizes_provenance_only_from_bound_image_refs(self) -> None:
        non_image_ref = "capture.event_id"
        image_ref = "source_bundle.artifacts[0]"
        analysis = math_analysis(non_image_ref)
        non_image_claim = analysis["concept_method_analysis"]["common_confusions"][0]
        non_image_claim["provenance"] = "mixed"
        mixed_claim = analysis["question_structure"]["objects"][0]
        mixed_claim["provenance"] = "source_bundle"
        mixed_claim["evidence_refs"] = [image_ref, non_image_ref]
        before = copy.deepcopy(analysis)

        normalized = _canonicalize_math_image_provenance(
            analysis,
            (image_ref,),
        )

        self.assertEqual(
            normalized["concept_method_analysis"]["common_confusions"][0]["provenance"],
            "model_inference",
        )
        self.assertEqual(
            normalized["question_structure"]["objects"][0]["provenance"],
            "mixed",
        )
        self.assertEqual(analysis, before)

    def test_math_validator_downgrades_inferred_observed_fact(self) -> None:
        evidence_ref = "capture.event_id"
        analysis = math_analysis(evidence_ref)
        claim = analysis["evidence_assessment"]["observed_facts"][0]
        claim["claim_type"] = "observed_fact"
        claim["provenance"] = "model_inference"

        validated = validate_math_analysis_v2(analysis, (evidence_ref,))

        normalized = validated["evidence_assessment"]["observed_facts"][0]
        self.assertEqual(normalized["claim_type"], "evidence_bound_inference")
        self.assertEqual(normalized["provenance"], "model_inference")
        self.assertEqual(claim["claim_type"], "observed_fact")

    def test_math_schema_and_validator_reject_empty_identity_boundary(self) -> None:
        for schema_name, expected_max in (
            ("luna-math-analysis-v2.json", 2),
            ("luna-math-critical-review-v2.json", 32),
        ):
            schema = load_json(ROOT / "schemas" / schema_name)
            identity_boundary = (
                schema["$defs"]["target_identity"]["properties"]
                ["identity_boundary"]
            )
            self.assertEqual(identity_boundary["type"], "array")
            self.assertEqual(identity_boundary["minItems"], 1)
            self.assertEqual(identity_boundary["maxItems"], expected_max)
            self.assertEqual(
                identity_boundary["items"], {"$ref": "#/$defs/claim"}
            )

        evidence_ref = "capture.event_id"
        analysis = math_analysis(evidence_ref)
        analysis["target_identity"]["identity_boundary"] = []
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_identity_boundary_missing",
        ):
            validate_math_analysis_v2(analysis, (evidence_ref,))

    def test_math_validator_preserves_valid_analysis_without_rewrite(self) -> None:
        evidence_ref = "capture.event_id"
        analysis = math_analysis(evidence_ref)
        before = copy.deepcopy(analysis)

        validated = validate_math_analysis_v2(analysis, (evidence_ref,))

        self.assertEqual(validated, before)
        self.assertEqual(analysis, before)

    def test_math_validator_enforces_compact_semantic_budget(self) -> None:
        evidence_ref = "capture.event_id"

        too_many_claims = math_analysis(evidence_ref)
        too_many_claims["risk_flags"].extend(
            math_claim(evidence_ref) for _ in range(29)
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_claim_budget_exceeded"
        ):
            validate_math_analysis_v2(too_many_claims, (evidence_ref,))

        too_many_steps = math_analysis(evidence_ref)
        too_many_steps["correct_reasoning_reconstruction"] = [
            math_claim(evidence_ref) for _ in range(7)
        ]
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_reasoning_step_budget_exceeded",
        ):
            validate_math_analysis_v2(too_many_steps, (evidence_ref,))

        long_claim = math_analysis(evidence_ref)
        long_claim["question_structure"]["objects"][0]["text"] = "甲" * 301
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_claim_text_invalid"
        ):
            validate_math_analysis_v2(long_claim, (evidence_ref,))

    def test_math_validator_uses_exact_image_refs_not_artifact_prefix(self) -> None:
        non_image_ref = "source_bundle.artifacts[0]"
        image_ref = "source_bundle.artifacts[1]"
        allowed = (non_image_ref, image_ref)

        non_image_analysis = math_analysis(non_image_ref)
        self.assertEqual(
            validate_math_analysis_v2(
                non_image_analysis,
                allowed,
                image_evidence_refs=(image_ref,),
            ),
            non_image_analysis,
        )

        bad_image_provenance = math_analysis(non_image_ref)
        bad_image_provenance["reasoning_diagnosis"]["first_break"] = math_claim(
            image_ref
        )
        bad_image_provenance["reasoning_diagnosis"]["first_break"][
            "provenance"
        ] = "source_bundle"
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_image_provenance_invalid"
        ):
            validate_math_analysis_v2(
                bad_image_provenance,
                allowed,
                image_evidence_refs=(image_ref,),
            )

        missing_image_ref = math_analysis(non_image_ref)
        missing_image_ref["reasoning_diagnosis"]["first_break"]["provenance"] = (
            "image"
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_image_ref_missing"
        ):
            validate_math_analysis_v2(
                missing_image_ref,
                allowed,
                image_evidence_refs=(image_ref,),
            )

    def legacy_contract_run_math_v2_passes_exact_image_refs_to_both_schema_stages(self) -> None:
        worker = Worker(self.config, model_runner=FakeMathRunner())
        status = worker.adapters["math"].status("2026-08-05")
        base = worker.adapters["math"].candidates(status)[0]
        candidate = worker.adapters["math"].deep_candidate(base)
        capture_ref = next(
            ref
            for ref in candidate.allowed_evidence_refs
            if ref.startswith("target_group.captures[")
        )
        image_ref = "source_bundle.artifacts[0]"
        image_path = self.base / "question.png"
        image_path.write_bytes(b"fake image")
        binding = copy.deepcopy(candidate.input_binding)
        binding["image_evidence_refs"] = [image_ref]
        model_input = copy.deepcopy(candidate.model_input)
        model_input["image_evidence_refs"] = [image_ref]
        candidate = replace(
            candidate,
            input_binding=binding,
            model_input=model_input,
            allowed_evidence_refs=(*candidate.allowed_evidence_refs, image_ref),
            image_paths=(image_path,),
        )
        draft = math_analysis(capture_ref)
        image_claim = math_claim(image_ref)
        image_claim["provenance"] = "image"
        draft["question_structure"]["objects"][0] = image_claim
        review = {
            "schema_version": "study-intake-luna-math-critical-review-v2",
            "verdict": "pass",
            "summary": "已独立核对图像证据来源。",
            "revised_analysis": copy.deepcopy(draft),
            "relationship_decisions": [],
            "unsupported_claims": [],
            "evidence_misreads": [],
            "mathematical_errors": [],
            "visual_findings": [],
            "provenance_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        stage_payloads = (draft, review)
        stage_calls = []

        def fake_execute(**kwargs):
            index = len(stage_calls)
            stage_calls.append(kwargs)
            return StructuredStageResult(
                payload=copy.deepcopy(stage_payloads[index]),
                duration_ms=1,
                runtime_model=None,
                runtime_reasoning_effort=None,
                runtime_metadata_provenance="unavailable",
                runtime_identity_status="requested_unverified",
                output_sha256=str(index + 1) * 64,
                schema_sha256=str(index + 3) * 64,
            )

        model_config = copy.deepcopy(self.config["model"])
        model_config["math_deep_v2"] = copy.deepcopy(self.config["math_deep_v2"])
        model_config["math_deep_v2"]["soft_runtime_warning_seconds"] = 3600
        runner = CodexRunner(model_config, self.runtime)
        with mock.patch.object(runner, "_execute_prompt", side_effect=fake_execute):
            result = runner.run_math_v2(candidate)
        self.assertEqual(result.pipeline_status, "two_pass_ready")
        self.assertEqual(len(stage_calls), 2)
        for call in stage_calls:
            self.assertEqual(call["image_evidence_refs"], (image_ref,))
            self.assertEqual(call["timeout_seconds"], 3600)

    def test_execute_prompt_never_turns_legacy_wall_values_into_hard_timeout(
        self,
    ) -> None:
        model_config = copy.deepcopy(self.config["model"])
        model_config["timeout_seconds"] = 321
        runner = CodexRunner(model_config, self.runtime)
        captured_timeouts = []

        def fake_invoke(command, **kwargs):
            captured_timeouts.append(kwargs["timeout"])
            output_path = Path(
                command[command.index("--output-last-message") + 1]
            )
            output_path.write_text("{}", encoding="utf-8")
            publish_fake_raw_refs(
                runner,
                stage_name=kwargs["stage_name"],
                raw_output=b"{}",
            )
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        schema_path = ROOT / "schemas/luna-math-analysis-v2.json"
        with mock.patch.object(
            runner, "_invoke_subprocess", side_effect=fake_invoke
        ):
            runner._execute_prompt(
                prompt="bounded math prompt",
                output_schema=schema_path,
                image_paths=[],
                stage_name="math_analysis",
                max_prompt_bytes=4096,
                max_output_bytes=4096,
                allowed_evidence_refs=("capture.evidence.user_facts[0].text",),
                timeout_seconds=1800,
            )
            runner._execute_prompt(
                prompt="bounded cs408 prompt",
                output_schema=schema_path,
                image_paths=[],
                stage_name="math_analysis",
                max_prompt_bytes=4096,
                max_output_bytes=4096,
                allowed_evidence_refs=("capture.evidence.user_facts[0].text",),
            )
        self.assertEqual(captured_timeouts, [None, None])

    def test_legacy_timeout_argument_is_soft_for_direct_math_stage(self) -> None:
        model_config = copy.deepcopy(self.config["model"])
        runner = CodexRunner(model_config, self.runtime)
        captured = []

        def stop_after_capture(_command, **kwargs):
            captured.append(kwargs["timeout"])
            raise RuntimeError("stop-after-soft-warning-capture")

        with mock.patch.object(
            runner, "_invoke_subprocess", side_effect=stop_after_capture
        ), self.assertRaisesRegex(RuntimeError, "stop-after-soft-warning-capture"):
            runner._execute_prompt(
                prompt="bounded direct math prompt",
                output_schema=ROOT / "schemas/luna-math-analysis-v2.json",
                image_paths=(),
                stage_name="math_analysis",
                max_prompt_bytes=4096,
                max_output_bytes=4096,
                allowed_evidence_refs=("capture.evidence.user_facts[0].text",),
                timeout_seconds=3600,
                subject="math",
            )
        self.assertEqual(captured, [None])

    def test_subprocess_completion_racing_cancel_is_rejected(self) -> None:
        model_config = copy.deepcopy(self.config["model"])
        runner = CodexRunner(model_config, self.runtime)

        class NaturalExitAtCancel:
            returncode = 0
            pid = 12345

            class CancelOnRead:
                def __init__(inner_self, handle):
                    inner_self.handle = handle

                def fileno(inner_self):
                    runner._cancel_requested.set()
                    return inner_self.handle.fileno()

                @property
                def closed(inner_self):
                    return inner_self.handle.closed

                def close(inner_self):
                    inner_self.handle.close()

            def __init__(inner_self):
                inner_self.stdin = tempfile.TemporaryFile()
                stdout = tempfile.TemporaryFile()
                stderr = tempfile.TemporaryFile()
                stdout.write(b"late stdout")
                stderr.write(b"late stderr")
                stdout.seek(0)
                stderr.seek(0)
                inner_self.stdout = inner_self.CancelOnRead(stdout)
                inner_self.stderr = inner_self.CancelOnRead(stderr)

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                del timeout
                return self.returncode

        process = NaturalExitAtCancel()
        with mock.patch.dict(
            os.environ, {"STUDY_INTAKE_FIXTURE_EXECUTION": ""}
        ), mock.patch(
            "preprocessor_core.subprocess.Popen", return_value=process
        ), mock.patch(
            "preprocessor_core.os.getpgid", return_value=process.pid
        ), mock.patch(
            "preprocessor_core.kernel_process_start_token",
            return_value=(
                "darwin-libproc-bsdinfo-v1:12345:1700000000:000001"
            ),
        ):
            with self.assertRaisesRegex(OSError, "runner_cancelled"):
                runner._invoke_subprocess(
                    ["synthetic-codex"],
                    input=b"{}",
                    timeout=3600,
                    cwd=self.runtime,
                    stage_name="math_analysis",
                )

    def test_more_than_eight_group_images_fails_closed(self) -> None:
        artifacts = []
        for index in range(9):
            path = self.repo / f"image-{index}.png"
            path.write_bytes(f"image-{index}".encode())
            artifacts.append(
                {
                    "role": "question", "media_type": "image/png",
                    "path": path.name, "sha256": sha256_file(path),
                }
            )
        manifest = {"source_locator": "frozen-source", "artifacts": artifacts}
        manifest_path = self.repo / "manifest.json"
        atomic_write_json(manifest_path, manifest)
        status = load_json(self.status_path)
        status["pending"][0]["source_bundle"] = {
            "manifest_path": manifest_path.name,
            "manifest_hash": sha256_file(manifest_path),
        }
        atomic_write_json(self.status_path, status)
        worker = Worker(self.config, model_runner=FakeMathRunner())
        adapter = worker.adapters["math"]
        rows = adapter.candidates(adapter.status("2026-08-05"))
        with self.assertRaisesRegex(
            PreprocessorError, "math_target_group_images_exceed_limit"
        ):
            adapter.deep_candidate(rows[0])

    def legacy_contract_config_requires_fixed_future_twenty_window(self) -> None:
        path = self.base / "config.json"
        atomic_write_json(path, self.config)
        self.assertEqual(
            load_config(path)["math_deep_v2"]["shadow_evaluation_target_count"], 20
        )
        bad = copy.deepcopy(self.config)
        bad["math_deep_v2"]["shadow_evaluation_target_count"] = 19
        atomic_write_json(path, bad)
        with self.assertRaisesRegex(
            PreprocessorError, "config_math_shadow_evaluation_target_invalid"
        ):
            load_config(path)

    def test_config_validates_soft_warning_and_stall_ranges(self) -> None:
        path = self.base / "config-progress-aware-stall.json"
        atomic_write_json(path, self.config)
        self.assertEqual(
            load_config(path)["math_deep_v2"]["soft_runtime_warning_seconds"],
            1800,
        )
        for value in (True, "1800", 59):
            with self.subTest(value=value):
                bad = copy.deepcopy(self.config)
                bad["math_deep_v2"]["soft_runtime_warning_seconds"] = value
                atomic_write_json(path, bad)
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "config_soft_runtime_warning_seconds_invalid",
                ):
                    load_config(path)
        for key, value in (
            ("stall_timeout_seconds", 59),
            ("stall_probe_interval_seconds", 0),
            ("stall_probe_required_consecutive_failures", 1),
            ("stall_probe_required_consecutive_failures", 3),
        ):
            with self.subTest(key=key, value=value):
                bad = copy.deepcopy(self.config)
                bad["math_deep_v2"][key] = value
                atomic_write_json(path, bad)
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"config_{key}_invalid",
                ):
                    load_config(path)

        legacy_alias = copy.deepcopy(self.config)
        legacy_alias["math_deep_v2"].pop("soft_runtime_warning_seconds")
        legacy_alias["math_deep_v2"]["stage_timeout_seconds"] = 3600
        atomic_write_json(path, legacy_alias)
        self.assertEqual(
            load_config(path)["math_deep_v2"]["stage_timeout_seconds"],
            3600,
        )

    def test_gs111_performance_budgets_fail_closed(self) -> None:
        profile = self.config["math_deep_v2"]
        candidate = SimpleNamespace(
            capture_id="GS-111",
            target_label="GS-111",
            model_input={"target_identity": {"formal_card_id": "GS-111"}},
        )
        self.assertEqual(
            validate_math_performance_budget(
                candidate,
                {
                    "analysis": {"duration_ms": 700_000},
                    "critical_review": {"duration_ms": 1_099_000},
                },
                profile,
            ),
            1_799_000,
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_gs111_cold_replay_budget_exceeded"
        ):
            validate_math_performance_budget(
                candidate,
                {
                    "analysis": {"duration_ms": 900_000},
                    "critical_review": {"duration_ms": 901_000},
                },
                profile,
            )
        self.assertEqual(
            validate_math_three_replay_p95(
                [800_000, 900_000, 1_199_000], profile
            ),
            1_199_000,
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_three_replay_p95_budget_exceeded"
        ):
            validate_math_three_replay_p95(
                [800_000, 900_000, 1_201_000], profile
            )

    def test_historical_publish_uses_only_manifest_candidate(self) -> None:
        test_input = HistoricalTestInput.from_env()
        protected_before = test_input.snapshot()
        manifest = test_input.load_historical_manifest()
        config = render_shadow_test_config(ROOT, test_input)
        config = copy.deepcopy(config)
        config["runtime_root"] = str(self.runtime / "historical")
        config["worker"]["log_path"] = str(self.runtime / "historical/logs/worker.log")
        config["worker"]["lock_path"] = str(self.runtime / "historical/state/worker.lock")
        candidate = replay.build_replay_candidate(
            manifest=manifest,
            item=manifest["items"][0],
            config=config,
            read_guard=test_input,
        )
        runner = FakeMathRunner()
        worker = Worker(config, model_runner=runner)
        self.assertEqual(
            candidate.input_binding["backaudit_manifest_sha256"],
            manifest["manifest_sha256"],
        )
        self.assertEqual(
            candidate.input_binding["replay_input_sha256"],
            manifest["items"][0]["replay_input_sha256"],
        )
        self.assertFalse(
            candidate.model_input["knowledge_distribution_snapshot"]["coverage_manifest"][
                "network_coverage_complete"
            ]
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_critical_review_dynamic_schema_mismatch",
        ):
            worker.publish_math_shadow_candidate(candidate)
        self.assertFalse(worker.store.latest_path("math", candidate.capture_id).exists())
        self.assertFalse(
            any((Path(config["runtime_root"]) / "packages/math/shadow/historical").rglob("*.json"))
        )
        self.assertEqual(protected_before, test_input.snapshot())


if __name__ == "__main__":
    unittest.main()
