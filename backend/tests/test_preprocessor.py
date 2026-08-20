from __future__ import annotations

import copy
import errno
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import preprocessor_core as core  # noqa: E402
from semantic_contract_v3 import encode_correction_delta  # noqa: E402
from preprocessor_core import (  # noqa: E402
    ANALYSIS_SCHEMA,
    ANALYSIS_SCHEMA_V2,
    CodexRunner,
    Cs408Adapter,
    FileLock,
    ModelResult,
    PreprocessorError,
    StructuredStageResult,
    Worker,
    atomic_write_json,
    build_408_knowledge_snapshot,
    chinese_character_count,
    cs408_processing_contract,
    json_file_bytes,
    consume_package,
    load_json,
    load_config,
    record_adoption,
    render_analysis_markdown,
    renderer_build_sha256,
    run_loop,
    prompt_for,
    publish_trace_supplement,
    semantic_evidence_refs_v2,
    sha256_file,
    sha256_value,
    validate_analysis_v2,
    validate_v2_candidate_route,
)
from preprocess_worker import (  # noqa: E402
    CurrentDateWorker,
    migrate_analysis_checkpoint_generation,
    migrate_v2_publication_generation,
    recover_analysis_checkpoint_job,
)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def publish_fake_raw_refs(
    runner: CodexRunner, *, stage_name: str, raw_output: bytes
) -> None:
    """Complete the raw-first side effect of a mocked Provider subprocess."""

    digest = sha(raw_output)
    runner._provider_raw_refs[stage_name] = {
        "raw_output_object_sha256": digest,
        "raw_output_object_ref": f"test-raw://sha256/{digest}",
    }


def analysis(ref: str) -> dict:
    claim = {"text": "候选事实", "evidence_refs": [ref], "confidence": "high"}
    return {
        "schema_version": ANALYSIS_SCHEMA,
        "summary": "已生成只读预处理候选。",
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


def v2_claim(ref: str, text: str) -> dict:
    return {
        "claim_type": "evidence_bound_inference",
        "text": text,
        "evidence_refs": [ref],
        "confidence": "medium",
        "counterevidence_or_boundary": "只适用于当前题已绑定证据，若原始证据冲突则保留未知并交给 Sol 处理。",
        "sol_verification_action": "重开当前题证据并逐项核对，不得直接信任 Luna 结论。",
    }


def v2_analysis(ref: str, *, complete: bool = True) -> dict:
    long_text = (
        "当前题的机制、条件、状态变化与用户推理证据必须逐层对应，分析仅描述这道题中已经绑定的对象，"
        "并把观察事实、合理推断、边界条件、反例方向以及仍需正式核验的部分明确分开。"
    )
    claim = v2_claim(ref, long_text)
    short = v2_claim(ref, "当前题答案安全的正式编纂候选，具体答案与完整解析不进入候选字段。")
    main_short = v2_claim(ref, "OS04-03 文件空闲空间管理。")
    return {
        "schema_version": ANALYSIS_SCHEMA_V2,
        "report_profile": "cs408_deep",
        "executive_summary": long_text * 3,
        "question_structure": {
            "objects": [claim], "conditions": [claim], "asked_task": [claim],
            "mechanism_structure": [claim],
        },
        "correct_reasoning_reconstruction": [claim, claim],
        "evidence_assessment": {
            "completeness": "complete" if complete else "limited_by_evidence",
            "evidence_inventory": [claim], "observed_facts": [claim],
            "inferences": [claim], "contradictions": [],
            "gaps": [] if complete else [claim],
        },
        "reasoning_diagnosis": {
            "independent_correct_steps": [claim], "first_break": claim,
            "later_breaks": [claim], "hint_dependencies": [claim],
            "self_corrections": [], "root_cause_hypotheses": [claim],
            "alternative_explanations": [claim],
        },
        "concept_method_analysis": {
            "mechanism": [claim], "method_trigger": [claim],
            "applicability_conditions": [claim], "boundary_conditions": [claim],
            "common_confusions": [claim], "transfer_risks": [claim],
        },
        "knowledge_network_context": {
            "knowledge_alignment": [claim],
            "exposed_problem_summary": [claim],
            "prior_mistake_comparison": [claim],
            "subject_chapter_classification": [claim],
            "chapter_history_summary": [claim],
            "relationship_candidates": [claim],
            "limitations": [claim],
        },
        "formalization_candidates": {
            "safe_summary": [short], "key_parameters": [short],
            "question_type": [short], "main_knowledge": [main_short],
            "secondary_knowledge": [], "hit_knowledge": [short],
            "fuzzy_concepts": [short], "error_tags": [short],
            "redo_first_action": [short], "topic_intents": [short],
            "relationship_search_intents": [short],
        },
        "sol_verification_plan": {
            "must_verify": [claim],
            "reject_if": [claim] if not complete else [claim],
            "source_checks": [claim], "conflict_handling_suggestions": [claim],
            "recommended_disposition": "new_candidate",
        },
        "risk_flags": [claim],
        "unresolved": [] if complete else [claim],
        "atomic_signals": [
            {
                "signal_id": "SIG-0001",
                "signal_type": "knowledge",
                "canonical_term": "OS04-03",
                "surface_form": "文件空闲空间管理",
                "importance": "primary",
                "provenance": "current_question_evidence",
                "evidence_refs": [ref],
                "confidence": "high",
                "error_role": "none",
                "specificity": "具体到可稳定匹配的知识节点",
                "applicability_boundary": "仅用于当前题已绑定证据",
                "truth_library_match_status": "exact",
            }
        ],
    }


class FakeV2Runner:
    def __init__(self, *, complete: bool = True) -> None:
        self.calls = 0
        self.complete = complete

    def run(self, candidate):
        self.calls += 1
        value = v2_analysis(candidate.allowed_evidence_refs[0], complete=self.complete)
        if not self.complete:
            visited = set()

            def shorten_claims(node):
                if isinstance(node, dict):
                    if "claim_type" in node and id(node) not in visited:
                        visited.add(id(node))
                        node["text"] = "当前证据有限，只保留可核验候选。"
                        node["counterevidence_or_boundary"] = "不得外推到未绑定事实。"
                        node["sol_verification_action"] = "由 Sol 重开原证据核验。"
                    else:
                        for nested in node.values():
                            shorten_claims(nested)
                elif isinstance(node, list):
                    for nested in node:
                        shorten_claims(nested)

            shorten_claims(value)
            value["executive_summary"] = "证据有限，报告仅保留已绑定事实、缺口和核验动作。"
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass",
            "summary": "第二阶段已按同一证据完成批判审查。",
            "revised_analysis": value,
            "unsupported_claims": [], "evidence_misreads": [],
            "answer_safety_findings": [], "missing_analysis": [],
            "required_corrections": [], "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        receipt = {
            "status": "ready", "prompt_version": "test-v2",
            "prompt_sha256": "a" * 64, "schema_sha256": "b" * 64,
            "result_sha256": "c" * 64, "output_sha256": "d" * 64,
            "duration_ms": 50, "requested_model": "gpt-5.6-luna",
            "requested_reasoning_effort": "max", "runtime_model": "gpt-5.6-luna",
            "runtime_reasoning_effort": "max",
            "runtime_metadata_provenance": "codex_json_event",
            "runtime_identity_status": "confirmed",
        }
        return ModelResult(
            analysis=value, duration_ms=100, runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            pipeline_status="two_pass_ready", draft_analysis=value,
            critical_review=critical,
            stage_receipts={"analysis": receipt, "critical_review": receipt},
        )


class FakeRunner:
    def __init__(self, failures: int = 0) -> None:
        self.calls = 0
        self.failures = failures

    def run(self, candidate):
        self.calls += 1
        if self.calls <= self.failures:
            raise PreprocessorError("synthetic_model_failure")
        return ModelResult(
            analysis=analysis(candidate.allowed_evidence_refs[0]),
            duration_ms=321,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
        )


class PreprocessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.runtime = self.base / "runtime"
        self.math_repo = self.base / "math"
        self.cs_repo = self.base / "cs408"
        self.math_repo.mkdir()
        self.cs_repo.mkdir()
        self.card_rel = Path("错题知识网络/错题卡/GS-001_测试.md")
        card_path = self.math_repo / self.card_rel
        card_path.parent.mkdir(parents=True)
        card_path.write_text(
            "---\nid: GS-001\n"
            "lecture_refs:\n"
            "  - test-fixtures/complete-capture/question.png\n"
            "---\n"
            "## 题目\n"
            "这是一道用于验证后台预处理与证据门禁的数学测试题，题干文本完整且可独立识别。\n"
            "## 解析\n"
            "已验证的解析文本用于测试只读候选包，不代表任何正式写入。\n",
            encoding="utf-8",
        )
        self.card_hash = sha256_file(card_path)
        generated = self.math_repo / "错题知识网络" / "生成"
        generated.mkdir(parents=True)
        atomic_write_json(
            generated / "wrong_questions.json",
            {
                "updated_at": "2026-08-04",
                "cards": [
                    {
                        "id": "GS-001",
                        "path": str(self.card_rel),
                        "meta": {
                            "subject": "高等数学",
                            "knowledge": ["测试知识点"],
                            "error_causes": ["方法入口未识别"],
                            "methods": ["先判型"],
                            "traps": [],
                            "question_type": ["测试题"],
                            "related": [],
                        },
                        "topic_chains": [],
                    }
                ],
                "similarities": [],
                "relation_summary": {},
            },
        )
        (generated / "知识网络图.mmd").write_text(
            "graph TD\n  GS001[测试知识点]\n", encoding="utf-8"
        )
        (self.math_repo / "错题知识网络" / "知识点库.md").write_text(
            "# 高等数学\n\n- 测试知识点\n", encoding="utf-8"
        )
        schema_root = self.math_repo / "错题知识网络" / "schema"
        schema_root.mkdir()
        atomic_write_json(
            schema_root / "relationship_signal_policy.json",
            {
                "schema_version": "math_relationship_signal_policy_v1",
                "ruleset_version": "test-v1",
                "strong_edge_minimum": {
                    "never_sufficient_alone": ["broad_knowledge"]
                },
                "broad_knowledge": ["高等数学"],
                "broad_question_types": ["综合题"],
                "generic_error_causes": ["知识缺口"],
                "generic_methods": ["先判型"],
                "generic_traps": [],
                "non_evidence_markers": ["待确认"],
                "storage_policy": {
                    "automatic_edge_may_write_formal_related": False
                },
                "execution_policy": {
                    "current_mode": "SHADOW",
                    "formal_write_in_shadow": False,
                },
            },
        )
        self.math_status_path = self.math_repo / "status.json"
        self.cs_status_path = self.cs_repo / "status.json"
        self.math_script = self.math_repo / "status.py"
        self.cs_script = self.cs_repo / "status.py"
        script = (
            "import json\n"
            "from pathlib import Path\n"
            "print(json.dumps(json.loads((Path(__file__).parent/'status.json').read_text())))\n"
        )
        self.math_script.write_text(script, encoding="utf-8")
        self.cs_script.write_text(script, encoding="utf-8")
        self.capture_id = "MFI-CAP-0123456789abcdef01234567"
        math_evidence_root = self.math_repo / "test-fixtures" / "complete-capture"
        math_evidence_root.mkdir(parents=True)
        question_path = math_evidence_root / "question.png"
        question_path.write_bytes(
            bytes.fromhex(
                "89504e470d0a1a0a0000000d494844520000000100000001"
                "0804000000b51c0c020000000b4944415478da6364f80f00"
                "010501012718e3660000000049454e44ae426082"
            )
        )
        solution_text_path = math_evidence_root / "solution.md"
        solution_text_path.write_text(
            "# 解析\n\n"
            "先根据已知条件判定方法入口，再按照定义逐步验证结论。\n",
            encoding="utf-8",
        )
        source_manifest_path = math_evidence_root / "manifest.json"
        atomic_write_json(
            source_manifest_path,
            {
                "schema_version": "math-test-source-manifest-v1",
                "source_locator": "fixture:generic-existing-math-card",
                "artifacts": [
                    {
                        "role": "question",
                        "path": str(question_path.relative_to(self.math_repo)),
                        "sha256": sha256_file(question_path),
                        "media_type": "image/png",
                    },
                    {
                        "role": "solution_text",
                        "path": str(solution_text_path.relative_to(self.math_repo)),
                        "sha256": sha256_file(solution_text_path),
                        "media_type": "text/markdown",
                    },
                ],
            },
        )
        self.math_status = {
            "schema_version": "math-fast-intake-status-v1",
            "study_date": "2026-08-04",
            "pending_count": 1,
            "closed_count": 0,
            "pending": [
                {
                    "event_id": self.capture_id,
                    "study_date": "2026-08-04",
                    "recorded_at": "2026-08-04T00:00:00Z",
                    "formal_id": "GS-001",
                    "source_locator": str(self.card_rel),
                    "source_hash_before": self.card_hash,
                    "identity_state": "verified_formal",
                    "source_bundle": {
                        "manifest_path": str(
                            source_manifest_path.relative_to(self.math_repo)
                        ),
                        "manifest_hash": sha256_file(source_manifest_path),
                    },
                    "capture_schema_version": "math-fast-intake-capture-v2",
                    "requested_action": "record_wrong",
                    "score_ref": None,
                    "original_content_hash": "1" * 64,
                    "amendment_count": 0,
                    "amendment_event_ids": [],
                    "effective_evidence_hash": "2" * 64,
                    "effective_target_hash": "3" * 64,
                    "evidence": {
                        "user_facts": [
                            {"text": "没有识别方法入口", "origin": "user_observed"}
                        ]
                    },
                    "episode_evidence": {
                        "schema_version": "math-test-dialogue-v1",
                        "completeness": "complete",
                        "turns": [
                            {
                                "speaker": "user",
                                "text": "我没有识别这道题的方法入口。",
                            },
                            {
                                "speaker": "assistant",
                                "text": "请先核对已知条件与方法的适用边界。",
                            },
                        ],
                    },
                    "active_freeze_ids": [],
                }
            ],
            "closed": [],
            "active_freezes": [],
            "ledger_hash": "4" * 64,
        }
        atomic_write_json(self.math_status_path, self.math_status)
        self.cs_capture = "CAP-408-001"
        self.cs_status = {
            "schema": "intake_fact_capture_state_v2",
            "study_date": "2026-08-04",
            "capture_count": 1,
            "pending_capture_ids": [self.cs_capture],
            "captures": {
                self.cs_capture: {
                    "capture_id": self.cs_capture,
                    "study_date": "2026-08-04",
                    "recorded_at": "2026-08-04T00:00:00Z",
                    "quality_status": "awaiting_daily_curation",
                    "current_batch_id": None,
                    "formalization_authorized": True,
                    "payload_sha256": "5" * 64,
                    "capture": {
                        "schema": "intake_fact_capture_v1",
                        "study_date": "2026-08-04",
                        "formalization_authorized": True,
                        "source_facts": {
                            "subject": "data_structure",
                            "locator": "/Users/example/nested-private-source",
                        },
                        "user_facts": {
                            "user_error_entry": "未识别状态边界",
                            "private_path": "/Users/example/nested-private-user",
                        },
                        "identity_hint": {
                            "status": "existing",
                            "formal_id": "OS_2020_001",
                            "mode": "redo",
                            "basis": "答案安全的精确身份依据",
                            "idempotency_key": "nested-private-identity",
                        },
                        "missing_fields": [],
                        "idempotency_key": "secret-idempotency-value",
                        "stable_evidence_refs": [
                            {
                                "kind": "review_outcome",
                                "locator": "/Users/example/private/review.jsonl#event=1",
                                "sha256": "7" * 64,
                            }
                        ],
                    },
                }
            },
            "neutral_saves": {},
            "active_batch": None,
            "latest_date_batch": None,
        }
        atomic_write_json(self.cs_status_path, self.cs_status)
        fake_codex = self.base / "codex"
        fake_codex.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        fake_codex.chmod(0o700)
        output_schema = self.base / "schema.json"
        atomic_write_json(output_schema, {"type": "object"})
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
                "enabled": True,
                "poll_interval_seconds": 1,
                "debounce_seconds": 0,
                "status_timeout_seconds": 5,
                "model_timeout_seconds": 5,
                "max_attempts": 3,
                "retry_base_seconds": 0,
                "max_jobs_per_scan": 4,
                "log_path": str(self.runtime / "logs/worker.log"),
                "lock_path": str(self.runtime / "state/worker.lock"),
            },
            "model": {
                "codex_path": str(fake_codex),
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "output_schema": str(output_schema),
                "prompt_version": "test-prompt-v1",
                "max_prompt_bytes": 131072,
                "max_images": 8,
            },
            "dashboard": {
                "projection_path": str(self.runtime / "state/dashboard_projection.json"),
                "max_items_per_subject": 200,
            },
            "math_knowledge_snapshot": {
                "enabled": True,
                "max_source_bytes": 1024 * 1024,
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
            "adapters": {
                "math": {
                    "enabled": True,
                    "adapter_version": "math-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(self.math_repo),
                    "status_script": str(self.math_script),
                },
                "cs408": {
                    "enabled": False,
                    "adapter_version": "cs408-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(self.cs_repo),
                    "status_script": str(self.cs_script),
                },
            },
        }
        self.config_path = self.base / "config.json"
        atomic_write_json(self.config_path, self.config)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_background_mcp_failure_prevents_model_call_and_preserves_receipt_ref(
        self,
    ) -> None:
        runner = object.__new__(CodexRunner)
        runner.config = {
            core.MATH_V2_PROFILE: {
                "enabled": True,
                "analysis_output_schema": str(
                    ROOT / "schemas/luna-math-analysis-v2.json"
                ),
                "critical_review_output_schema": str(
                    ROOT / "schemas/luna-math-critical-review-v3.json"
                ),
            }
        }
        failure_sha256 = "7" * 64
        processing_host = mock.Mock()
        processing_host.open_read_session.side_effect = (
            core.ProcessingPluginError(
                "background_mcp_timeout",
                diagnostic={
                    "background_mcp_failure_receipt_sha256": failure_sha256,
                    "background_mcp_failure_receipt_ref": (
                        "study-intake-background-mcp-failure://sha256/"
                        + failure_sha256
                    ),
                },
            )
        )
        runner._processing_host = processing_host
        runner._execute_prompt = mock.Mock()
        candidate = core.Candidate(
            subject="math",
            capture_id="GS-111",
            study_date="2026-08-07",
            recorded_at="2026-08-07T10:00:00+08:00",
            input_fingerprint="1" * 64,
            input_binding={"processing_contract_sha256": "2" * 64},
            model_input={"capture": {"facts": "current-only"}},
            allowed_evidence_refs=(),
            image_paths=(),
            target_label="GS-111",
            canonical_state="awaiting_background_analysis",
            sol_state="pending_review",
        )

        with self.assertRaisesRegex(
            PreprocessorError, "background_mcp_timeout"
        ) as raised:
            runner.run_math_v2(candidate)

        self.assertEqual(
            raised.exception.diagnostic[
                "background_mcp_failure_receipt_sha256"
            ],
            failure_sha256,
        )
        self.assertEqual(
            raised.exception.diagnostic[
                "background_mcp_failure_receipt_ref"
            ],
            "study-intake-background-mcp-failure://sha256/" + failure_sha256,
        )
        runner._execute_prompt.assert_not_called()

    def test_cs408_publication_generation_fence_rejects_changed_input(self) -> None:
        worker = Worker(self.config, model_runner=FakeV2Runner())
        original = core.Candidate(
            subject="cs408",
            capture_id="CAP-FENCE-001",
            study_date="2026-08-04",
            recorded_at="2026-08-04T08:00:00+08:00",
            input_fingerprint="1" * 64,
            input_binding={"binding": "old"},
            model_input={"bounded": True},
            allowed_evidence_refs=(),
            image_paths=(),
            target_label="CAP-FENCE-001",
            canonical_state="awaiting_daily_curation",
            sol_state="pending_review",
        )
        changed = replace(
            original,
            input_fingerprint="2" * 64,
            input_binding={"binding": "new"},
        )
        with mock.patch.object(worker, "scan_statuses", return_value={}), mock.patch.object(
            worker, "candidates", return_value=[changed]
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "stale_input_superseded"
            ):
                worker._assert_current_candidate_generation(original)

    def make_v2_config(self) -> dict:
        config = copy.deepcopy(self.config)
        config["adapters"]["math"]["enabled"] = False
        config["adapters"]["cs408"]["enabled"] = True
        evidence_root = self.runtime / "private/current-question-evidence"
        for name in ("", "manifests", "objects", "attachments"):
            path = evidence_root / name
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.chmod(0o700)
        source_id = "SRC-408-001"
        details_id = "DETAIL-408-001"
        feedback = "当前题快速反馈已经冻结。"
        bundle = {
            "schema_version": "current-question-evidence-bundle-v1",
            "context_id": details_id,
            "request_id": "REQ-408-001",
            "session_id": "SESSION-408-001",
            "item_id": "ITEM-408-001",
            "source_id": source_id,
            "source_kind": "morning_review",
            "study_date": "2026-08-04",
            "event_time": "2026-08-04T08:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "source_binding_sha256": "8" * 64,
            "current_question": {
                "public_text": "当前题只讨论一个状态转换机制。",
                "options": [],
                "response_instruction": "说明第一步判断。",
                "public_surface_sha256": "9" * 64,
                "attachment_sha256s": [],
            },
            "learner_evidence": {
                "answer_text": "我的判断不确定。", "choice": None,
                "confidence": "low", "first_action": "先判断状态",
                "reasoning": "推理在边界条件处中断。", "prompt_level": "L0",
                "observed_at": "2026-08-04T08:01:00+08:00",
            },
            "evaluation_evidence": {
                "grader_capsule_id": "GRADER-001", "correct_answer": "私有答案证据",
                "correct_option": None, "standard_explanation": "私有详细解析",
                "grader_result": "partial", "grader_basis": "当前题绑定评分",
                "provided_by": "current_question_grader", "missing_fields": [],
            },
            "assessment": {
                "first_result": "partial", "choice_result": None,
                "reasoning_result": "break", "confidence": "low",
                "prompt_level": "L0", "first_break": "边界条件识别中断",
                "first_break_provenance": "observed", "first_action": "先判断状态",
                "first_action_provenance": "observed",
            },
            "frozen_assistant_feedback": {
                "text": feedback, "sha256": sha(feedback.encode("utf-8")),
            },
            "provenance": {
                "source_locator": "current-question-capsule:001",
                "source_sha256": "a" * 64, "context_sha256": "b" * 64,
                "attachment_provenance": [],
                "created_at": "2026-08-04T08:02:00+08:00", "missing_items": [],
            },
            "missing_fields": [], "attachment_objects": [],
            "formal_write_count": 0,
        }
        object_bytes = json_file_bytes(bundle)
        object_hash = sha(object_bytes)
        atomic_write_json(evidence_root / "objects" / f"{object_hash}.json", bundle)
        manifest = {
            "schema_version": "current-question-evidence-manifest-v1",
            "bundle_schema_version": "current-question-evidence-bundle-v1",
            "context_id": details_id, "request_id": "REQ-408-001",
            "session_id": "SESSION-408-001", "item_id": "ITEM-408-001",
            "source_id": source_id, "source_kind": "morning_review",
            "study_date": "2026-08-04", "source_binding_sha256": "8" * 64,
            "object_sha256": object_hash, "object_size_bytes": len(object_bytes),
            "attachment_objects": [], "created_at": "2026-08-04T08:02:00+08:00",
            "answer_safe_manifest": True, "formal_write_count": 0,
        }
        manifest_hash = sha(json_file_bytes(manifest))
        atomic_write_json(
            evidence_root / "manifests" / f"{manifest_hash}.json", manifest
        )
        capture = self.cs_status["captures"][self.cs_capture]["capture"]
        capture["source_facts"].update(
            {"source_id": source_id, "details_id": details_id}
        )
        capture["stable_evidence_refs"] = [{
            "kind": "current_question_evidence_bundle_v1",
            "locator": f"current-question-evidence://sha256/{manifest_hash}",
            "sha256": manifest_hash,
        }]
        publish_trace_supplement(
            private_root=evidence_root,
            capture_id=self.cs_capture,
            context_id=details_id,
            item_id="ITEM-408-001",
            evidence_manifest_sha256=manifest_hash,
            created_at="2026-08-04T08:03:00+08:00",
            supplement_kind="legacy_backfill",
            resolution_receipt_sha256=None,
            events=[
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": "推理在边界条件处中断。",
                    "observed_at": None,
                }
            ],
        )
        atomic_write_json(self.cs_status_path, self.cs_status)
        config["private_evidence"] = {
            "current_question_root": str(evidence_root),
            "max_bundle_bytes": 262144,
        }
        config["cs408_deep_v2"] = {
            "enabled": True,
            "analysis_output_schema": str(ROOT / "schemas/luna-analysis-v2.json"),
            "critical_review_output_schema": str(ROOT / "schemas/luna-critical-review-v2.json"),
            "package_output_schema": str(ROOT / "schemas/preprocess-package-v3.json"),
            "controlled_contract_path": str(
                ROOT / "schemas/luna-cs408-controlled-contract-v3.json"
            ),
            "analysis_prompt_version": "test-cs408-analysis-v2",
            "critical_review_prompt_version": "test-cs408-review-v2",
            "soft_runtime_warning_seconds": 1800,
            "stall_timeout_seconds": 1800,
            "stall_probe_interval_seconds": 60,
            "stall_probe_required_consecutive_failures": 2,
            "max_prompt_bytes": 524288,
            "max_output_bytes": 262144,
        }
        (self.cs_repo / "知识点标签表.md").write_text(
            "## OS04 文件管理\n- OS04-03 文件空闲空间管理\n",
            encoding="utf-8",
        )
        (self.cs_repo / "节点总表.md").write_text(
            "| ID | 科目 | 主模块 | 主知识点 | 副知识点 | 命中知识点 | 核心考点 | 模糊概念 | 错因标签 | 首次做题日期 | 最近复做日期 | 最近错误记录 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| OS_2020_001 | 操作系统 | 文件管理 | OS04-03 文件空闲空间管理 |  | OS04-03 | 成组链接 | 块角色 | E03 | 2026-01-01 | 2026-07-01 | 未区分组首块 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "关系边表.md").write_text(
            "| 起点ID | 终点ID | 关系类型 | 联系强度 | 关联原因 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| OS_2020_001 | OS_2020_002 | R02 | 强 | 同一块角色边界 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "错题复做记录.md").write_text(
            "| 日期 | ID | 来源ID | 动作 | 错误记录 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 2026-07-12 | OS_2020_001 | HOS_0093 | 复做再次错误 | 未建立成组链接分配边界 |\n"
            "| 2026-07-25 | OS_2020_001 | HOS_0093 | 复做再次错误 | 未处理最后一组边界 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "复习单元节点映射.md").write_text(
            "| 主复习单元ID | 次复习单元ID | 正式节点ID |\n"
            "| --- | --- | --- |\n"
            "| OS_2020_001 |  | OS_2020_001 |\n",
            encoding="utf-8",
        )
        config["cs408_knowledge_snapshot"] = {
            "enabled": True,
            "max_source_bytes": 1024 * 1024,
            "max_taxonomy_candidates": 12,
            "max_history_candidates": 12,
            "max_relation_candidates": 16,
            "max_review_timeline_entries": 32,
            "sources": {
                "taxonomy": "知识点标签表.md",
                "nodes": "节点总表.md",
                "relations": "关系边表.md",
                "review_records": "错题复做记录.md",
                "review_unit_mapping": "复习单元节点映射.md",
            },
            "controlled_aliases": {},
        }
        return config

    def publish_degraded_v2(self, config: dict) -> tuple[Worker, dict, list]:
        worker = Worker(config)
        status = worker.adapters["cs408"].status("2026-08-04")
        candidate = worker.adapters["cs408"].candidates(status)[0]
        draft = v2_analysis(candidate.allowed_evidence_refs[0])
        calls = []
        real_run = subprocess.run

        def fake_run(command, **kwargs):
            if "--output-schema" not in command:
                return real_run(command, **kwargs)
            calls.append(list(command))
            if len(calls) == 1:
                output_path = Path(
                    command[command.index("--output-last-message") + 1]
                )
                output_path.write_text(json.dumps(draft), encoding="utf-8")
                return SimpleNamespace(
                    returncode=0,
                    stdout=b'{"model":"gpt-5.6-luna","reasoning_effort":"max"}\n',
                    stderr=b"",
                )
            embedded = json.dumps(
                {
                    "type": "error",
                    "message": (
                        "You've hit your usage limit. Try again at "
                        "2026-08-05T00:00:00Z"
                    ),
                }
            )
            return SimpleNamespace(
                returncode=1,
                stdout=(
                    json.dumps({"type": "error", "message": embedded}) + "\n"
                ).encode("utf-8"),
                stderr=b"usage details are not persisted",
            )

        with mock.patch.object(
            worker.runner, "_invoke_subprocess", side_effect=fake_run
        ):
            result = self.run_exact_cs408_force(worker)
        self.assertEqual(result["processed"][0]["status"], "single_pass_degraded")
        self.assertEqual(len(calls), 2)
        return worker, draft, calls

    def run_exact_cs408_force(
        self, worker: Worker, capture_id: str | None = None
    ) -> dict:
        requested_capture = capture_id or self.cs_capture
        status = worker.adapters["cs408"].status("2026-08-04")
        candidate = next(
            row
            for row in worker.adapters["cs408"].candidates(status)
            if row.capture_id == requested_capture
        )
        return worker.run_once(
            subject="cs408",
            capture_id=requested_capture,
            study_date=candidate.study_date,
            force=True,
            expected_input_fingerprint=candidate.input_fingerprint,
        )

    def test_config_requires_luna_max_and_kill_switch(self) -> None:
        loaded = load_config(self.config_path)
        self.assertTrue(loaded["worker"]["enabled"])
        for effort in ("low", "medium", "high", "xhigh"):
            with self.subTest(reasoning_effort=effort):
                bad = copy.deepcopy(self.config)
                bad["model"]["reasoning_effort"] = effort
                atomic_write_json(self.config_path, bad)
                with self.assertRaisesRegex(
                    PreprocessorError, "config_reasoning_must_be_max"
                ):
                    load_config(self.config_path)
        for tier in ("priority", "standard", "default"):
            with self.subTest(service_tier=tier):
                bad = copy.deepcopy(self.config)
                bad["model"]["service_tier"] = tier
                atomic_write_json(self.config_path, bad)
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "config_service_tier_must_be_absent",
                ):
                    load_config(self.config_path)
        atomic_write_json(self.config_path, self.config)

    def legacy_contract_math_worker_is_idempotent_and_writes_private_package(self) -> None:
        runner = FakeRunner()
        worker = Worker(self.config, model_runner=runner)
        first = worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        self.assertEqual(first["processed"][0]["status"], "ready")
        second = worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        self.assertEqual(second["processed"], [])
        self.assertEqual(runner.calls, 1)
        package_path = Path(
            worker.store.read_job("math", self.capture_id)["package_path"]
        )
        package = json.loads(package_path.read_text(encoding="utf-8"))
        self.assertEqual(package["formal_write_count"], 0)
        self.assertEqual(package["model_receipt"]["requested_model"], "gpt-5.6-luna")
        self.assertEqual(package["model_receipt"]["runtime_model"], "gpt-5.6-luna")

    def test_retry_is_bounded_and_terminal(self) -> None:
        runner = FakeRunner(failures=9)
        worker = Worker(self.config, model_runner=runner)
        statuses = []
        for _ in range(4):
            value = worker.run_once(subject="math", capture_id=self.capture_id, force=True)
            statuses.append(value["processed"][0]["status"] if value["processed"] else None)
        self.assertEqual(statuses, ["retrying", "retrying", "failed", None])
        self.assertEqual(runner.calls, 3)

    def legacy_contract_usage_limit_is_terminal_on_first_attempt_until_exact_force(self) -> None:
        config = self.make_v2_config()
        real_run = subprocess.run
        model_calls = []

        def usage_limited(command, **kwargs):
            if "--output-schema" not in command:
                return real_run(command, **kwargs)
            model_calls.append(list(command))
            embedded = json.dumps(
                {
                    "type": "error",
                    "message": (
                        "You've hit your usage limit. "
                        "Try again Aug 8th, 2026 11:37 AM"
                    ),
                }
            )
            return SimpleNamespace(
                returncode=1,
                stdout=(
                    json.dumps({"type": "error", "message": embedded}) + "\n"
                ).encode("utf-8"),
                stderr=b"quota detail",
            )

        usage_worker = Worker(config)
        with mock.patch.object(
            usage_worker.runner, "_invoke_subprocess", side_effect=usage_limited
        ):
            first = self.run_exact_cs408_force(usage_worker)
        self.assertEqual(len(model_calls), 1)
        self.assertEqual(first["processed"][0]["status"], "failed")
        store = Worker(config, model_runner=FakeV2Runner()).store
        failed_job = store.read_job("cs408", self.cs_capture)
        self.assertEqual(failed_job["attempts"], 1)
        self.assertEqual(
            failed_job["last_error_code"], "cs408_analysis_usage_limit"
        )
        self.assertIsNone(failed_job["next_retry_at"])
        self.assertEqual(
            failed_job["retry_at_hint"], "Aug 8th, 2026 11:37 AM"
        )
        receipts = sorted(
            (self.runtime / "receipts/cs408/2026-08-04").glob("*.json")
        )
        self.assertEqual(len(receipts), 1)
        self.assertEqual(
            json.loads(receipts[0].read_text(encoding="utf-8"))["retry_at_hint"],
            "Aug 8th, 2026 11:37 AM",
        )

        class MustNotRun:
            def run(inner_self, candidate):
                raise AssertionError("regular scan must not retry quota failure")

        regular = Worker(config, model_runner=MustNotRun()).run_once(
            subject="cs408", capture_id=self.cs_capture
        )
        self.assertEqual(regular["selected_count"], 0)
        self.assertEqual(regular["decisions"][0]["reason"], "usage_limit")

        forced_runner = FakeV2Runner()
        forced = self.run_exact_cs408_force(
            Worker(config, model_runner=forced_runner)
        )
        self.assertEqual(forced["processed"][0]["status"], "two_pass_ready")
        self.assertEqual(forced_runner.calls, 1)

    def test_orphan_processing_requires_explicit_exact_force(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config)
        status = worker.adapters["cs408"].status("2026-08-04")
        candidate = worker.adapters["cs408"].candidates(status)[0]
        worker.store.write_job(candidate, worker._processing_job(candidate, 1))

        class MustNotRun:
            def __init__(inner_self) -> None:
                inner_self.calls = 0

            def run(inner_self, current):
                inner_self.calls += 1
                raise AssertionError("startup recovery must not invoke a model")

        guarded_runner = MustNotRun()
        recovered_worker = Worker(config, model_runner=guarded_runner)
        recovered = recovered_worker.run_once(
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
        )
        self.assertEqual(recovered["selected_count"], 0)
        self.assertEqual(
            recovered["decisions"][0]["reason"],
            "manual_recovery_required",
        )
        self.assertEqual(
            recovered["decisions"][0]["input_fingerprint"],
            candidate.input_fingerprint,
        )
        self.assertEqual(guarded_runner.calls, 0)
        interrupted = recovered_worker.store.read_job(
            "cs408", self.cs_capture
        )
        self.assertEqual(interrupted["status"], "worker_interrupted")
        self.assertEqual(
            interrupted["critical_resume_status"],
            "manual_recovery_required",
        )
        self.assertEqual(interrupted["formal_write_count"], 0)

    def test_cs408_force_requires_exact_worker_and_cli_binding(self) -> None:
        config = self.make_v2_config()
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_force_exact_binding_required"
        ):
            Worker(config, model_runner=FakeV2Runner()).run_once(force=True)
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_force_exact_binding_required"
        ):
            Worker(config, model_runner=FakeV2Runner()).run_once(
                subject="cs408",
                capture_id=self.cs_capture,
                force=True,
            )

        atomic_write_json(self.config_path, config)
        omitted_subject = subprocess.run(
            [
                sys.executable,
                str(ROOT / "bin/preprocess_worker.py"),
                "--config",
                str(self.config_path),
                "run-once",
                "--force",
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(omitted_subject.returncode, 2)
        self.assertEqual(
            json.loads(omitted_subject.stdout)["error_code"],
            "cs408_force_subject_required",
        )
        missing_fingerprint = subprocess.run(
            [
                sys.executable,
                str(ROOT / "bin/preprocess_worker.py"),
                "--config",
                str(self.config_path),
                "run-once",
                "--subject",
                "cs408",
                "--capture-id",
                self.cs_capture,
                "--date",
                "2026-08-04",
                "--force",
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(missing_fingerprint.returncode, 2)
        self.assertEqual(
            json.loads(missing_fingerprint.stdout)["error_code"],
            "cs408_exact_operation_binding_required",
        )

    def test_daemon_worker_recomputes_local_date_and_rejects_date_drift(self) -> None:
        daemon = CurrentDateWorker(self.config, model_runner=FakeRunner())
        with mock.patch(
            "preprocess_worker.current_date",
            side_effect=["2026-08-05", "2026-08-06"],
        ) as today, mock.patch.object(
            Worker,
            "run_once",
            return_value={"status": "ok", "formal_write_count": 0},
        ) as base_run:
            daemon.run_once()
            daemon.run_once()
        self.assertEqual(
            base_run.call_args_list,
            [
                mock.call(study_date="2026-08-05"),
                mock.call(study_date="2026-08-06"),
            ],
        )
        self.assertEqual(
            today.call_args_list,
            [mock.call("Asia/Shanghai"), mock.call("Asia/Shanghai")],
        )

        daemon._daemon_study_date = "2026-08-05"
        historical = SimpleNamespace(study_date="2026-08-04")
        with mock.patch.object(
            Worker, "candidates", return_value=[historical]
        ), self.assertRaisesRegex(
            PreprocessorError, "daemon_candidate_date_drift"
        ):
            daemon.candidates({})
        daemon._daemon_study_date = None

    def test_sigterm_handler_cancels_owned_process_group(self) -> None:
        self.runtime.mkdir(parents=True, exist_ok=True)
        runner = CodexRunner({}, self.runtime)
        handlers = {}

        class LoopWorker:
            def __init__(inner_self) -> None:
                inner_self.runner = runner
                inner_self.worker_config = {"poll_interval_seconds": 60}
                inner_self.logger = mock.Mock()
                inner_self.completed = None

            def run_once(inner_self):
                inner_self.completed = runner._invoke_subprocess(
                    [
                        sys.executable,
                        "-c",
                        "import time; time.sleep(30)",
                    ],
                    input=b"",
                    timeout=30,
                    cwd=self.runtime,
                    stage_name="test_sigterm_owned_process_group",
                )

        loop_worker = LoopWorker()

        def install_handler(signum, handler):
            handlers[signum] = handler

        outcome = {}

        def target() -> None:
            outcome["returncode"] = run_loop(loop_worker)

        thread = threading.Thread(target=target, daemon=True)
        with mock.patch(
            "preprocessor_core.signal.signal", side_effect=install_handler
        ):
            thread.start()
            deadline = time.monotonic() + 3
            while (
                signal.SIGTERM not in handlers
                or not runner._active_processes
            ) and time.monotonic() < deadline:
                time.sleep(0.01)
            try:
                self.assertIn(signal.SIGTERM, handlers)
                self.assertTrue(runner._active_processes)
                handlers[signal.SIGTERM](signal.SIGTERM, None)
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
            finally:
                if thread.is_alive():
                    runner.cancel_active()
                    thread.join(timeout=5)
        self.assertEqual(outcome["returncode"], 0)
        self.assertEqual(runner._active_processes, set())
        # The cancellation fence intentionally rejects all subprocess bytes;
        # a killed return code is not promoted to a consumable stage result.
        self.assertIsNone(loop_worker.completed)
        self.assertTrue(runner._cancel_requested.is_set())

    def test_worker_kill_switch_makes_zero_model_calls(self) -> None:
        config = copy.deepcopy(self.config)
        config["worker"]["enabled"] = False
        runner = FakeRunner()
        value = Worker(config, model_runner=runner).run_once(force=True)
        self.assertEqual(value["status"], "paused")
        self.assertEqual(runner.calls, 0)
        projection = json.loads(
            Path(config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(projection["worker"]["status"], "paused")

    def test_cs408_adapter_is_complete_but_disabled_by_default(self) -> None:
        disabled = consume_package(
            self.config,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
        )
        self.assertEqual(disabled["status"], "disabled")
        config = copy.deepcopy(self.config)
        config["adapters"]["math"]["enabled"] = False
        config["adapters"]["cs408"]["enabled"] = True
        adapter = Worker(config, model_runner=FakeRunner()).adapters["cs408"]
        status = adapter.status("2026-08-04")
        candidate = adapter.candidates(status)[0]
        self.assertEqual(candidate.target_label, "OS_2020_001")
        rendered = prompt_for(candidate, "test-prompt-v1")
        self.assertNotIn("/Users/example/private", rendered)
        self.assertNotIn("nested-private", rendered)
        self.assertNotIn("secret-idempotency-value", rendered)
        self.assertNotIn('"locator"', rendered)
        self.assertIn('"sha256": "777777', rendered)

    def test_cs408_adapter_requires_authorized_unbatched_pending_capture(self) -> None:
        config = copy.deepcopy(self.config)
        config["adapters"]["math"]["enabled"] = False
        config["adapters"]["cs408"]["enabled"] = True
        adapter = Worker(config, model_runner=FakeRunner()).adapters["cs408"]
        status = copy.deepcopy(self.cs_status)

        unauthorized = copy.deepcopy(status["captures"][self.cs_capture])
        unauthorized["formalization_authorized"] = False
        unauthorized["capture"]["formalization_authorized"] = False
        status["captures"]["CAP-408-UNAUTHORIZED"] = unauthorized

        unconfirmed = copy.deepcopy(status["captures"][self.cs_capture])
        unconfirmed["quality_status"] = "captured_unconfirmed"
        status["captures"]["CAP-408-UNCONFIRMED"] = unconfirmed

        batched = copy.deepcopy(status["captures"][self.cs_capture])
        batched["current_batch_id"] = "CUR-408-001"
        status["captures"]["CAP-408-BATCHED"] = batched
        status["neutral_saves"]["SAVE-408-001"] = {"status": "saved_neutral"}

        candidates = adapter.candidates(status)
        self.assertEqual([row.capture_id for row in candidates], [self.cs_capture])

    def test_adapter_uses_configured_python_runtime_and_fails_closed(self) -> None:
        config = copy.deepcopy(self.config)
        config["adapters"]["math"]["enabled"] = False
        config["adapters"]["cs408"]["enabled"] = True
        adapter = Worker(config, model_runner=FakeRunner()).adapters["cs408"]
        with mock.patch(
            "preprocessor_core.run_json_command", return_value=self.cs_status
        ) as run:
            adapter.status("2026-08-04")
        command = run.call_args.args[0]
        self.assertEqual(command[0], str(Path(sys.executable).resolve()))

        config["adapters"]["cs408"]["python_path"] = str(
            self.base / "missing-python"
        )
        with self.assertRaisesRegex(
            PreprocessorError, "adapter_python_not_executable"
        ):
            Worker(config, model_runner=FakeRunner())

    def legacy_contract_subject_status_failure_is_isolated(self) -> None:
        config = copy.deepcopy(self.config)
        config["adapters"]["cs408"]["enabled"] = True
        runner = FakeRunner()
        worker = Worker(config, model_runner=runner)
        with mock.patch.object(
            worker.adapters["cs408"],
            "status",
            side_effect=PreprocessorError("canonical_status_failed"),
        ):
            result = worker.run_once(
                subject="math", capture_id=self.capture_id, force=True
            )
        self.assertEqual(result["processed"][0]["status"], "ready")
        self.assertEqual(result["adapter_errors"], {
            "cs408": "canonical_status_failed"
        })
        self.assertEqual(runner.calls, 1)
        projection = json.loads(
            Path(config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(projection["subjects"]["cs408"]["status"], "source_error")
        self.assertEqual(
            projection["subjects"]["cs408"]["error_code"],
            "canonical_status_failed",
        )

    def legacy_contract_consume_requires_current_math_freeze_and_records_adoption(self) -> None:
        worker = Worker(self.config, model_runner=FakeRunner())
        worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        frozen = copy.deepcopy(self.math_status)
        frozen["pending"][0]["active_freeze_ids"] = ["MFI-FREEZE-001"]
        atomic_write_json(self.math_status_path, frozen)
        result = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
            freeze_id="MFI-FREEZE-001",
            effective_evidence_hash="2" * 64,
        )
        self.assertEqual(result["status"], "ready")
        self.assertIsNotNone(result["adoption_token"])
        offered_projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        offered_item = offered_projection["subjects"]["math"]["items"][0]
        self.assertEqual(offered_item["delivery_status"], "offered")
        self.assertEqual(offered_item["adoption_status"], "unknown")
        adoption = record_adoption(
            self.config,
            adoption_token=result["adoption_token"],
            outcome="modified_adopted",
            reason_code="sol-corrected-wording",
        )
        self.assertEqual(adoption["status"], "recorded")
        noop = record_adoption(
            self.config,
            adoption_token=result["adoption_token"],
            outcome="modified_adopted",
            reason_code="sol-corrected-wording",
        )
        self.assertEqual(noop["status"], "noop")
        projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        item = projection["subjects"]["math"]["items"][0]
        self.assertEqual(item["delivery_status"], "consumed")
        self.assertEqual(item["adoption_status"], "modified_adopted")
        self.assertTrue(item["adoption_receipt_id"].startswith("SIP-ADOPTION-"))

    def legacy_contract_consume_and_adoption_do_not_share_the_global_worker_lock(self) -> None:
        worker = Worker(self.config, model_runner=FakeRunner())
        worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        frozen = copy.deepcopy(self.math_status)
        frozen["pending"][0]["active_freeze_ids"] = ["MFI-FREEZE-001"]
        atomic_write_json(self.math_status_path, frozen)
        lock_path = Path(self.config["worker"]["lock_path"])
        with FileLock(lock_path):
            offered = consume_package(
                self.config,
                subject="math",
                capture_id=self.capture_id,
                study_date="2026-08-04",
                freeze_id="MFI-FREEZE-001",
                effective_evidence_hash="2" * 64,
            )
        self.assertEqual(offered["status"], "ready")
        with FileLock(lock_path):
            recorded = record_adoption(
                self.config,
                adoption_token=offered["adoption_token"],
                outcome="direct_adopted",
                reason_code=None,
            )
        adoption_root = (
            self.runtime / "state/adoptions/math" / self.capture_id
        )
        self.assertTrue(adoption_root.exists())
        self.assertEqual(recorded["status"], "recorded")

    def legacy_contract_old_offer_is_rejected_after_a_new_publication(self) -> None:
        first_worker = Worker(self.config, model_runner=FakeRunner())
        first_worker.run_once(
            subject="math", capture_id=self.capture_id, force=True
        )
        first_frozen = copy.deepcopy(self.math_status)
        first_frozen["pending"][0]["active_freeze_ids"] = [
            "MFI-FREEZE-001"
        ]
        atomic_write_json(self.math_status_path, first_frozen)
        first_offer = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
            freeze_id="MFI-FREEZE-001",
            effective_evidence_hash="2" * 64,
        )
        self.assertEqual(first_offer["status"], "ready")
        first_offer_file = load_json(
            self.runtime
            / "state/offers"
            / f"{first_offer['adoption_token']}.json"
        )
        first_publication_id = first_offer_file["publication_id"]

        advanced = copy.deepcopy(first_frozen)
        advanced["pending"][0]["effective_evidence_hash"] = "9" * 64
        advanced["pending"][0]["active_freeze_ids"] = []
        atomic_write_json(self.math_status_path, advanced)
        second_worker = Worker(self.config, model_runner=FakeRunner())
        second = second_worker.run_once(
            subject="math", capture_id=self.capture_id, force=True
        )
        self.assertEqual(second["processed"][0]["status"], "ready")
        current_job = second_worker.store.read_job("math", self.capture_id)
        self.assertNotEqual(
            current_job["publication_id"], first_publication_id
        )
        with self.assertRaisesRegex(
            PreprocessorError, "adoption_offer_stale_publication"
        ):
            record_adoption(
                self.config,
                adoption_token=first_offer["adoption_token"],
                outcome="direct_adopted",
                reason_code=None,
            )

        advanced["pending"][0]["active_freeze_ids"] = [
            "MFI-FREEZE-001"
        ]
        atomic_write_json(self.math_status_path, advanced)
        second_offer = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
            freeze_id="MFI-FREEZE-001",
            effective_evidence_hash="9" * 64,
        )
        self.assertEqual(second_offer["status"], "ready")
        second_offer_file = load_json(
            self.runtime
            / "state/offers"
            / f"{second_offer['adoption_token']}.json"
        )
        self.assertNotEqual(
            second_offer_file["publication_id"], first_publication_id
        )
        recorded = record_adoption(
            self.config,
            adoption_token=second_offer["adoption_token"],
            outcome="direct_adopted",
            reason_code=None,
        )
        self.assertEqual(recorded["status"], "recorded")
        self.assertEqual(
            recorded["publication_id"], second_offer_file["publication_id"]
        )

    def legacy_contract_consume_marks_amended_package_stale(self) -> None:
        Worker(self.config, model_runner=FakeRunner()).run_once(
            subject="math", capture_id=self.capture_id, force=True
        )
        changed = copy.deepcopy(self.math_status)
        changed["pending"][0]["effective_evidence_hash"] = "9" * 64
        changed["pending"][0]["active_freeze_ids"] = ["MFI-FREEZE-001"]
        atomic_write_json(self.math_status_path, changed)
        result = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
            freeze_id="MFI-FREEZE-001",
            effective_evidence_hash="2" * 64,
        )
        self.assertEqual(result["status"], "stale")

    def test_consumer_requires_nightly_binding_before_ready(self) -> None:
        Worker(self.config, model_runner=FakeRunner()).run_once(
            subject="math", capture_id=self.capture_id, force=True
        )
        result = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
        )
        self.assertEqual(result["status"], "stale")
        self.assertFalse(result["validation"]["required_freeze_id"])

    def legacy_contract_consumer_rejects_pointer_package_identity_drift(self) -> None:
        Worker(self.config, model_runner=FakeRunner()).run_once(
            subject="math", capture_id=self.capture_id, force=True
        )
        frozen = copy.deepcopy(self.math_status)
        frozen["pending"][0]["active_freeze_ids"] = ["MFI-FREEZE-001"]
        atomic_write_json(self.math_status_path, frozen)
        pointer_path = (
            self.runtime / "state/latest/math" / f"{self.capture_id}.json"
        )
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["package_id"] = "SIP-PKG-FFFFFFFFFFFFFFFFFFFFFFFF"
        atomic_write_json(pointer_path, pointer)
        result = consume_package(
            self.config,
            subject="math",
            capture_id=self.capture_id,
            study_date="2026-08-04",
            freeze_id="MFI-FREEZE-001",
            effective_evidence_hash="2" * 64,
        )
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["validation"]["package_contract"])

    def test_model_schema_uses_supported_structured_output_subset(self) -> None:
        schema = json.loads((ROOT / "schemas/luna-analysis-v1.json").read_text())
        encoded = json.dumps(schema)
        self.assertNotIn("uniqueItems", encoded)
        self.assertNotIn("oneOf", encoded)
        self.assertEqual(schema["properties"]["schema_version"]["type"], "string")

    def test_v2_model_schemas_use_strict_structured_output_objects(self) -> None:
        analysis = json.loads(
            (ROOT / "schemas/luna-analysis-v2.json").read_text()
        )
        review = json.loads(
            (ROOT / "schemas/luna-critical-review-v2.json").read_text()
        )

        def assert_strict_objects(value, path="$"):
            if isinstance(value, dict):
                raw_type = value.get("type")
                is_object = raw_type == "object" or (
                    isinstance(raw_type, list) and "object" in raw_type
                )
                if is_object:
                    self.assertIs(
                        value.get("additionalProperties"),
                        False,
                        f"{path} must reject additional properties",
                    )
                    properties = value.get("properties")
                    self.assertIsInstance(properties, dict, f"{path} properties")
                    self.assertEqual(
                        set(value.get("required") or []),
                        set(properties),
                        f"{path} must require every declared property",
                    )
                for key, nested in value.items():
                    assert_strict_objects(nested, f"{path}.{key}")
            elif isinstance(value, list):
                for index, nested in enumerate(value):
                    assert_strict_objects(nested, f"{path}[{index}]")

        assert_strict_objects(analysis)
        assert_strict_objects(review)
        writable_path = review["$defs"]["writable_correction_path"]
        self.assertIsNotNone(
            re.fullmatch(writable_path["pattern"], "$.atomic_signals[0]")
        )
        self.assertIsNone(
            re.fullmatch(
                writable_path["pattern"], "$.existing_formal_edges[0]"
            )
        )
        self.assertEqual(
            review["properties"]["sol_priority_checks"]["$ref"],
            "#/$defs/sol_priority_findings",
        )
        sol_priority = review["$defs"]["sol_priority_finding"]
        self.assertEqual(sol_priority["properties"]["severity"]["enum"], ["info", "warning"])
        self.assertEqual(
            sol_priority["properties"]["affected_json_paths"]["maxItems"], 0
        )
        self.assertEqual(
            review["$defs"]["finding"]["properties"]["affected_json_paths"]
            ["items"]["$ref"],
            "#/$defs/writable_correction_path",
        )
        self.assertEqual(
            review["$defs"]["correction_resolution"]["properties"]
            ["affected_json_paths"]["items"]["$ref"],
            "#/$defs/writable_correction_path",
        )
        for name, schema in (("analysis", analysis), ("review", review)):
            encoded = json.dumps(schema)
            for unsupported in (
                "uniqueItems",
                "oneOf",
                "unevaluatedProperties",
                '"format"',
            ):
                self.assertNotIn(
                    unsupported,
                    encoded,
                    f"{name} schema uses unsupported structured-output keyword",
                )
        embedded = review["$defs"]["analysis"]
        self.assertTrue(set(analysis["required"]).issubset(embedded["required"]))
        for key, schema in analysis["properties"].items():
            self.assertEqual(embedded["properties"][key], schema)
        self.assertEqual(review["$defs"]["claim"], analysis["$defs"]["claim"])
        self.assertEqual(review["$defs"]["claims"], analysis["$defs"]["claims"])

    def test_dashboard_distinguishes_offer_from_adoption(self) -> None:
        worker = Worker(self.config, model_runner=FakeRunner())
        worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        item = projection["subjects"]["math"]["items"][0]
        self.assertIsNone(item["delivery_status"])
        self.assertEqual(item["adoption_status"], "unknown")
        self.assertIsNone(item["adoption_receipt_id"])

    def test_dashboard_projection_has_no_legacy_item_limit_setting(self) -> None:
        config = copy.deepcopy(self.config)
        config["dashboard"].pop("max_items_per_subject", None)
        worker = Worker(config, model_runner=FakeRunner())
        worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        projection = json.loads(
            Path(config["dashboard"]["projection_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            projection["subjects"]["math"]["items"][0]["capture_id"],
            self.capture_id,
        )

    @mock.patch("preprocessor_core.current_date", return_value="2026-08-05")
    def test_exact_capture_projection_keeps_candidate_study_date(
        self, _mock_current_date
    ) -> None:
        worker = Worker(self.config, model_runner=FakeRunner())
        worker.run_once(subject="math", capture_id=self.capture_id, force=True)
        projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(projection["study_date"], "2026-08-04")
        self.assertEqual(
            projection["subjects"]["math"]["items"][0]["capture_id"],
            self.capture_id,
        )
        archived = (
            Path(self.config["dashboard"]["projection_path"]).parent
            / "dashboard-projections/2026-08-04.json"
        )
        self.assertTrue(archived.is_file())
        self.assertEqual(
            json.loads(archived.read_text(encoding="utf-8"))["study_date"],
            "2026-08-04",
        )

    def test_processing_projection_is_visible_while_180_second_runner_is_blocked(self) -> None:
        started = threading.Event()
        release = threading.Event()

        class BlockingRunner:
            def run(inner_self, candidate):
                started.set()
                release.wait(180)
                return ModelResult(
                    analysis=analysis(candidate.allowed_evidence_refs[0]),
                    duration_ms=10,
                    runtime_model=None,
                    runtime_reasoning_effort=None,
                    runtime_metadata_provenance="unavailable",
                )

        worker = Worker(self.config, model_runner=BlockingRunner())
        thread = threading.Thread(
            target=lambda: worker.run_once(
                subject="math", capture_id=self.capture_id, force=True
            )
        )
        thread.start()
        self.assertTrue(started.wait(2))
        projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(projection["subjects"]["math"]["items"][0]["luna_status"], "processing")
        self.assertEqual(
            projection["worker"]["requested_model"], "gpt-5.6-luna"
        )
        self.assertEqual(
            projection["worker"]["runtime_identity_status"],
            "requested_unverified",
        )
        self.assertFalse(projection["worker"]["paused"])
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_codex_runner_has_explicit_tool_isolation_and_uses_output_file(self) -> None:
        worker = Worker(self.config, model_runner=FakeRunner())
        status = worker.adapters["math"].status("2026-08-04")
        candidate = worker.adapters["math"].candidates(status)[0]
        first_image = self.runtime / "question.png"
        second_image = self.runtime / "solution.png"
        first_image.write_bytes(b"question")
        second_image.write_bytes(b"solution")
        candidate = replace(
            candidate, image_paths=(first_image, second_image)
        )
        captured_command = []

        def fake_run(command, **kwargs):
            captured_command.extend(command)
            output_path = Path(command[command.index("--output-last-message") + 1])
            output_path.write_text(
                json.dumps(analysis(candidate.allowed_evidence_refs[0])),
                encoding="utf-8",
            )
            return SimpleNamespace(
                returncode=0,
                stdout=b'{"model":"gpt-5.6-luna","reasoning_effort":"max"}\n',
                stderr=b"non-json warning that must not be parsed as the final answer",
            )

        runner = CodexRunner(
            {**self.config["model"], "timeout_seconds": 5}, self.runtime
        )
        with mock.patch.object(runner, "_invoke_subprocess", side_effect=fake_run):
            result = runner.run(candidate)
        self.assertEqual(result.analysis["schema_version"], ANALYSIS_SCHEMA)
        for gate in (
            "project_doc_max_bytes=0",
            "project_doc_fallback_filenames=[]",
            "features.shell_tool=false",
            "features.plugins=false",
            "agents.enabled=false",
            'web_search="disabled"',
        ):
            self.assertIn(gate, captured_command)
        self.assertIn("--ignore-user-config", captured_command)
        self.assertEqual(
            captured_command[captured_command.index("--model") + 1],
            "gpt-5.6-luna",
        )
        self.assertIn('model_reasoning_effort="max"', captured_command)
        self.assertFalse(
            any("service_tier" in value for value in captured_command),
            captured_command,
        )
        self.assertIn("--ignore-rules", captured_command)
        self.assertIn("--ephemeral", captured_command)
        self.assertIn("read-only", captured_command)
        image_values = [
            captured_command[index + 1]
            for index, value in enumerate(captured_command)
            if value == "--image"
        ]
        self.assertEqual(image_values, [str(first_image), str(second_image)])

    def test_attested_runtime_mismatch_is_rejected_for_english_stage(self) -> None:
        runner = CodexRunner(
            {**self.config["model"], "timeout_seconds": 5}, self.runtime
        )

        def fake_run(command, **kwargs):
            output_path = Path(
                command[command.index("--output-last-message") + 1]
            )
            output_path.write_text("{}", encoding="utf-8")
            publish_fake_raw_refs(
                runner,
                stage_name=kwargs["stage_name"],
                raw_output=b"{}",
            )
            event = {
                "schema_version": core.CODEX_RUNTIME_ATTESTATION_SCHEMA,
                "type": "runtime_identity_attestation",
                "attested": True,
                "model": "gpt-5.6-luna",
                "reasoning_effort": "high",
            }
            return SimpleNamespace(
                returncode=0,
                stdout=(json.dumps(event) + "\n").encode(),
                stderr=b"",
            )

        with (
            mock.patch.object(
                core, "CODEX_RUNTIME_ATTESTATION_SUPPORTED", True
            ),
            mock.patch.object(
                runner, "_invoke_subprocess", side_effect=fake_run
            ),
        ):
            with self.assertRaisesRegex(
                PreprocessorError,
                "english_analysis_runtime_identity_mismatch",
            ):
                runner._execute_prompt(
                    prompt="return the schema object",
                    output_schema=ROOT
                    / "schemas/luna-english-candidate-draft-v1.json",
                    image_paths=(),
                    stage_name="english_analysis",
                    max_prompt_bytes=4096,
                    max_output_bytes=4096,
                    allowed_evidence_refs=(),
                    bind_evidence_schema=False,
                    timeout_seconds=5,
                )

    def test_contract_v2_runner_uses_portable_provider_with_exact_analysis_ref_enum(self) -> None:
        config = self.make_v2_config()
        model_config = copy.deepcopy(config["model"])
        model_config["timeout_seconds"] = 5
        runner = CodexRunner(model_config, self.runtime)
        exact_ref = "analysis.question_structure.mechanism_structure[0]"
        tainted_ref = exact_ref + "чаты?"
        schema_path = Path(
            config["cs408_deep_v2"]["critical_review_output_schema"]
        )
        static_before = schema_path.read_bytes()
        schema_records: list[dict[str, object]] = []

        def fake_run(command, **kwargs):
            provider_path = Path(
                command[command.index("--output-schema") + 1]
            )
            schema_raw = provider_path.read_bytes()
            schema_records.append(
                {
                    "path": provider_path,
                    "raw": schema_raw,
                    "value": json.loads(schema_raw),
                    "mode": provider_path.stat().st_mode & 0o777,
                }
            )
            output_path = Path(command[command.index("--output-last-message") + 1])
            output_path.write_text("{}", encoding="utf-8")
            publish_fake_raw_refs(
                runner,
                stage_name=kwargs["stage_name"],
                raw_output=b"{}",
            )
            return SimpleNamespace(
                returncode=0,
                stdout=b'{"model":"gpt-5.6-luna","reasoning_effort":"max"}\n',
                stderr=b"",
            )

        with mock.patch.object(runner, "_invoke_subprocess", side_effect=fake_run):
            result = runner._execute_prompt(
                prompt="return the critical review schema object",
                output_schema=schema_path,
                image_paths=(),
                stage_name="cs408_critical_review",
                max_prompt_bytes=4096,
                max_output_bytes=4096,
                allowed_evidence_refs=(
                    "current_question_evidence.current_question",
                ),
                allowed_analysis_refs=(exact_ref,),
                allowed_correction_paths=("$.question_structure",),
                timeout_seconds=5,
                subject="cs408",
            )
        self.assertEqual(len(schema_records), 1)
        record = schema_records[0]
        self.assertEqual(record["mode"], 0o600)
        self.assertFalse(record["path"].exists())
        self.assertNotIn(
            "enum", record["value"]["$defs"]["evidence_ref"]
        )
        canonical_raw, canonical_sha256 = runner._bound_output_schema_bytes(
            schema_path,
            stage_name="cs408_critical_review",
            allowed_evidence_refs=(
                "current_question_evidence.current_question",
            ),
            allowed_analysis_refs=(exact_ref,),
            allowed_correction_paths=("$.question_structure",),
        )
        self.assertEqual(
            result.provider_schema_sha256,
            sha(record["raw"]),
        )
        self.assertEqual(
            result.schema_sha256,
            result.provider_schema_sha256,
        )
        self.assertEqual(
            result.canonical_schema_sha256,
            canonical_sha256,
        )
        self.assertNotEqual(
            result.provider_schema_sha256,
            result.canonical_schema_sha256,
        )
        analysis_enum = json.loads(canonical_raw)["$defs"]["analysis_ref"]["enum"]
        self.assertEqual(analysis_enum, [exact_ref])
        self.assertNotIn(tainted_ref, analysis_enum)
        self.assertEqual(
            record["value"]["$defs"]["analysis_ref"]["enum"],
            analysis_enum,
        )
        self.assertNotIn(
            "enum",
            record["value"]["$defs"][
                "writable_correction_path"
            ],
        )
        self.assertEqual(schema_path.read_bytes(), static_before)

    def test_contract_v2_checkpoint_resume_rebinds_exact_analysis_ref_enum(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        candidate = worker.adapters["cs408"].candidates(
            worker.adapters["cs408"].status("2026-08-04")
        )[0]
        draft = v2_analysis(candidate.allowed_evidence_refs[0])
        exact_ref = "analysis.question_structure.mechanism_structure[0]"
        tainted_ref = exact_ref + "чаты?"

        def grounded_stage(
            *,
            payload: dict,
            generation: str,
            stable_id: str,
            source_hash: str,
            transcript_sha256: str,
        ) -> StructuredStageResult:
            evidence_ref = core.model_mcp_item_ref(
                subject="cs408",
                generation=generation,
                collection="formal_problem_records",
                stable_id=stable_id,
                source_hash=source_hash,
            )
            result = {
                "subject": "cs408",
                "generation": generation,
                "items": [
                    {
                        "collection": "formal_problem_records",
                        "stable_id": stable_id,
                        "source_hash": source_hash,
                        "data_role": "formal_problem",
                        "evidence_ref": evidence_ref,
                    }
                ],
            }
            call = {
                "sequence": 1,
                "arguments": {"ids": [stable_id]},
                "result": result,
                "result_sha256": core.sha256_value(result),
            }
            return StructuredStageResult(
                payload=payload,
                duration_ms=1,
                runtime_model="gpt-5.6-luna",
                runtime_reasoning_effort="max",
                runtime_metadata_provenance="codex_json_event",
                runtime_identity_status="requested_unverified",
                output_sha256="9" * 64,
                mcp_transcript_sha256=transcript_sha256,
                mcp_transcript_ref=(
                    "study-intake-mcp-stage-transcript://sha256/"
                    + transcript_sha256
                ),
                provider_request_count=2,
                mcp_tool_call_count=1,
                mcp_calls=(call,),
            )

        analysis_stage = grounded_stage(
            payload=draft,
            generation="cs408-analysis-generation",
            stable_id="ITEM-ANALYSIS",
            source_hash="1" * 64,
            transcript_sha256="2" * 64,
        )
        analysis_grounding = core.mcp_grounding_manifest((analysis_stage,))
        analysis_receipt = {
            "status": "ready",
            "prompt_version": "test-cs408-analysis-v2",
            "provider_request_count": 2,
            "mcp_tool_call_count": 1,
            "mcp_grounding_manifest": analysis_grounding,
            "mcp_grounding_manifest_sha256": analysis_grounding[
                "manifest_sha256"
            ],
        }
        critic_stage = grounded_stage(
            payload={},
            generation="cs408-critic-generation",
            stable_id="ITEM-CRITIC",
            source_hash="3" * 64,
            transcript_sha256="4" * 64,
        )
        critic_ref = core.mcp_grounding_refs((critic_stage,))[0]
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "恢复阶段使用冻结 Analysis 的精确引用集合。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-RESUME-EXACT-REF",
                    "severity": "warning",
                    "text": "恢复后的 critic 重新读取了正式记录。",
                    "analysis_refs": [exact_ref],
                    "evidence_refs": [critic_ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        schema_path = Path(
            config["cs408_deep_v2"]["critical_review_output_schema"]
        )
        static_before = schema_path.read_bytes()
        model_config = copy.deepcopy(config["model"])
        model_config["cs408_deep_v2"] = copy.deepcopy(
            config["cs408_deep_v2"]
        )
        runner = CodexRunner(model_config, self.runtime)
        observed: dict[str, object] = {}

        def fake_execute(**kwargs):
            allowed_analysis_refs = tuple(kwargs["allowed_analysis_refs"])
            self.assertIn(exact_ref, allowed_analysis_refs)
            self.assertNotIn(tainted_ref, allowed_analysis_refs)
            canonical_payload, canonical_sha256 = (
                runner._bound_output_schema_bytes(
                    kwargs["output_schema"],
                    stage_name=kwargs["stage_name"],
                    allowed_evidence_refs=kwargs["allowed_evidence_refs"],
                    allowed_analysis_refs=allowed_analysis_refs,
                    allowed_correction_paths=kwargs[
                        "allowed_correction_paths"
                    ],
                )
            )
            provider_payload = (
                runner._cs408_portable_critical_review_provider_schema_bytes(
                    kwargs["output_schema"],
                    canonical_schema_payload=canonical_payload,
                )
            )
            provider_schema = json.loads(provider_payload)
            observed["analysis_enum"] = provider_schema["$defs"][
                "analysis_ref"
            ]["enum"]
            observed["provider_sha256"] = sha(provider_payload)
            observed["canonical_sha256"] = canonical_sha256
            return replace(
                critic_stage,
                payload=critical,
                schema_sha256=sha(provider_payload),
                provider_schema_sha256=sha(provider_payload),
                provider_schema_ref=(
                    "study-intake-provider-schema://sha256/"
                    + sha(provider_payload)
                ),
                canonical_schema_sha256=canonical_sha256,
            )

        with mock.patch.object(
            runner,
            "_execute_prompt",
            side_effect=fake_execute,
        ):
            result = runner.resume_critical(
                candidate,
                draft_analysis=draft,
                analysis_receipt=analysis_receipt,
            )
        self.assertEqual(result.pipeline_status, "two_pass_ready")
        self.assertEqual(
            result.stage_receipts["analysis"],
            analysis_receipt,
        )
        self.assertEqual(observed["analysis_enum"], list(
            core._analysis_review_refs(draft)
        ))
        self.assertNotEqual(
            observed["provider_sha256"],
            observed["canonical_sha256"],
        )
        self.assertEqual(
            result.stage_receipts["critical_review"][
                "provider_schema_sha256"
            ],
            observed["provider_sha256"],
        )
        self.assertEqual(schema_path.read_bytes(), static_before)

    def test_analysis_review_refs_include_only_existing_collection_items(self) -> None:
        draft = v2_analysis("capture://sha256/" + "a" * 64)
        draft["evidence_assessment"]["contradictions"].append(
            v2_claim(
                "capture://sha256/" + "a" * 64,
                "当前题存在一条需要审查的证据矛盾。",
            )
        )
        refs = core._analysis_review_refs(draft)
        self.assertIn("analysis.atomic_signals", refs)
        self.assertIn("analysis.atomic_signals[0]", refs)
        self.assertIn("analysis.evidence_assessment.contradictions[0]", refs)
        self.assertNotIn("analysis.atomic_signals[1]", refs)
        self.assertNotIn("analysis.atomic_signals[0].canonical_term", refs)

    def test_analysis_review_refs_do_not_expand_host_owned_projection_items(self) -> None:
        draft = v2_analysis("capture://sha256/" + "a" * 64)
        draft["truth_match_matrix"] = [
            {"signal_id": "SIG-0001", "match_status": "exact"}
        ]
        refs = core._analysis_review_refs(draft)
        self.assertIn("analysis.truth_match_matrix", refs)
        self.assertNotIn("analysis.truth_match_matrix[0]", refs)

    def test_analysis_review_refs_fail_closed_instead_of_truncating(self) -> None:
        oversized = {
            "atomic_signals": [
                {"signal_id": f"SIG-{index:04d}"}
                for index in range(core.ANALYSIS_REVIEW_REF_MAX_COUNT)
            ]
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "semantic_analysis_refs_limit_exceeded",
        ):
            core._analysis_review_refs(oversized)

    def test_analysis_review_refs_have_separate_capacity_from_evidence_refs(self) -> None:
        draft = {
            "question_structure": {
                "objects": list(range(32)),
                "conditions": list(range(32)),
                "asked_task": list(range(32)),
            },
            "concept_method_analysis": {
                "mechanism": list(range(32)),
                "method_trigger": list(range(32)),
                "boundary_conditions": list(range(32)),
            },
        }
        refs = core._analysis_review_refs(draft)
        self.assertGreater(len(refs), core.SEMANTIC_REF_MAX_COUNT)
        self.assertLessEqual(len(refs), core.ANALYSIS_REVIEW_REF_MAX_COUNT)

    def test_critical_review_rejects_nonexistent_collection_item_ref(self) -> None:
        evidence_ref = "capture://sha256/" + "a" * 64
        draft = v2_analysis(evidence_ref)
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "引用不存在的分析数组项必须由主机拒绝。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [
                {
                    "finding_id": "FIND-OUT-OF-RANGE-REF",
                    "severity": "warning",
                    "text": "此 finding 故意引用不存在的原子信号。",
                    "analysis_refs": ["analysis.atomic_signals[999]"],
                    "evidence_refs": [evidence_ref],
                    "affected_json_paths": [],
                }
            ],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_refs_invalid",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=(evidence_ref,),
                draft_analysis=draft,
            )

    def test_critical_review_rejects_duplicate_correction_paths(self) -> None:
        evidence_ref = "capture://sha256/" + "a" * 64
        draft = v2_analysis(evidence_ref)
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "reject",
            "summary": "重复 correction path 不能伪装成有效修正。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [
                {
                    "finding_id": "FIND-DUPLICATE-PATH",
                    "severity": "blocking",
                    "text": "此 finding 故意重复同一个路径。",
                    "analysis_refs": ["analysis.report_profile"],
                    "evidence_refs": [evidence_ref],
                    "affected_json_paths": [
                        "$.report_profile",
                        "$.report_profile",
                    ],
                }
            ],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_correction_paths_duplicate",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=(evidence_ref,),
                draft_analysis=draft,
            )

    def test_critical_review_rejects_resolution_for_sol_priority_advisory(self) -> None:
        evidence_ref = "capture://sha256/" + "a" * 64
        draft = v2_analysis(evidence_ref)
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "Sol advisory 不属于 correction resolution 的闭合范围。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "SOL-ADVISORY-ONLY",
                    "severity": "warning",
                    "text": "仅提醒 Sol 独立复核，不修改语义分析。",
                    "analysis_refs": ["analysis.atomic_signals"],
                    "evidence_refs": [evidence_ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "SOL-ADVISORY-ONLY",
                    "resolution": "not_applicable",
                    "affected_json_paths": ["$.atomic_signals"],
                    "before": encode_correction_delta(None),
                    "after": encode_correction_delta(None),
                    "reason": "此 resolution 必须被拒绝。",
                }
            ],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_sol_priority_resolution_forbidden",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=(evidence_ref,),
                draft_analysis=draft,
            )

    def test_v2_runner_surfaces_safe_api_code_and_cleans_failed_schema(self) -> None:
        config = self.make_v2_config()
        candidate = Worker(config, model_runner=FakeV2Runner()).adapters[
            "cs408"
        ].candidates(self.cs_status)[0]
        model_config = copy.deepcopy(config["model"])
        model_config["timeout_seconds"] = 5
        runner = CodexRunner(model_config, self.runtime)
        schema_paths = []

        def fail_run(command, **kwargs):
            schema_paths.append(Path(command[command.index("--output-schema") + 1]))
            publish_fake_raw_refs(
                runner,
                stage_name=kwargs["stage_name"],
                raw_output=b"",
            )
            embedded = json.dumps(
                {
                    "type": "error",
                    "error": {
                        "code": "invalid_json_schema",
                        "message": "must not be persisted",
                    },
                }
            )
            return SimpleNamespace(
                returncode=1,
                stdout=(
                    json.dumps({"type": "error", "message": embedded}) + "\n"
                ).encode("utf-8"),
                stderr=b"private stderr must not be persisted",
            )

        with mock.patch.object(runner, "_invoke_subprocess", side_effect=fail_run):
            with self.assertRaisesRegex(
                PreprocessorError, "cs408_analysis_invalid_json_schema"
            ):
                runner._execute_prompt(
                    prompt="bounded prompt",
                    output_schema=Path(
                        config["cs408_deep_v2"]["analysis_output_schema"]
                    ),
                    image_paths=[],
                    stage_name="cs408_analysis",
                    max_prompt_bytes=4096,
                    max_output_bytes=4096,
                    allowed_evidence_refs=candidate.allowed_evidence_refs,
                )
        self.assertEqual(len(schema_paths), 1)
        self.assertFalse(schema_paths[0].exists())

    def test_codex_popen_resource_errors_keep_stable_retry_codes(self) -> None:
        runner = CodexRunner(
            {**self.config["model"], "timeout_seconds": 5}, self.runtime
        )
        cases = (
            (errno.EAGAIN, "process_resource_eagain"),
            (errno.ENOMEM, "process_resource_enomem"),
            (errno.EMFILE, "process_resource_emfile"),
            (errno.ENFILE, "process_resource_enfile"),
        )
        for raw_errno, expected_code in cases:
            with self.subTest(expected_code=expected_code), mock.patch.object(
                core.subprocess,
                "Popen",
                side_effect=OSError(raw_errno, "synthetic process limit"),
            ):
                with self.assertRaises(PreprocessorError) as raised:
                    runner._execute_prompt(
                        prompt="bounded prompt",
                        output_schema=Path(self.config["model"]["output_schema"]),
                        image_paths=[],
                        stage_name="cs408_analysis",
                        max_prompt_bytes=4096,
                        max_output_bytes=4096,
                        allowed_evidence_refs=[],
                        bind_evidence_schema=False,
                    )
                self.assertEqual(raised.exception.code, expected_code)

    def legacy_contract_cs408_consumer_rejects_legacy_v1_even_with_active_batch(self) -> None:
        config = copy.deepcopy(self.config)
        config["adapters"]["math"]["enabled"] = False
        config["adapters"]["cs408"]["enabled"] = True
        worker = Worker(config, model_runner=FakeRunner())
        self.run_exact_cs408_force(worker)
        batched = copy.deepcopy(self.cs_status)
        batched["pending_capture_ids"] = []
        batched["captures"][self.cs_capture]["quality_status"] = "curating"
        batched["captures"][self.cs_capture]["current_batch_id"] = "BATCH-001"
        batched["active_batch"] = {
            "batch_id": "BATCH-001",
            "study_date": "2026-08-04",
            "status": "active",
            "capture_set_sha256": "6" * 64,
            "completed_count": 0,
            "total_count": 1,
        }
        atomic_write_json(self.cs_status_path, batched)
        result = consume_package(
            config,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
            batch_id="BATCH-001",
            capture_set_hash="6" * 64,
        )
        self.assertEqual(result["status"], "stale")
        self.assertFalse(result["validation"]["cs408_v2_package_required"])
        self.assertIsNone(result["adoption_token"])

    def legacy_contract_cs408_v2_private_two_stage_report_and_safe_package(self) -> None:
        config = self.make_v2_config()
        runner = FakeV2Runner()
        worker = Worker(config, model_runner=runner)
        result = self.run_exact_cs408_force(worker)
        self.assertEqual(result["processed"][0]["status"], "two_pass_ready")
        job = worker.store.read_job("cs408", self.cs_capture)
        package = json.loads(Path(job["package_path"]).read_text(encoding="utf-8"))
        self.assertNotIn("analysis", package)
        self.assertNotIn("critical_review", package)
        self.assertEqual(
            set(package["report_json_ref"].rsplit("/", 1)) & {package["report_json_sha256"]},
            {package["report_json_sha256"]},
        )
        report_path = (
            self.runtime / "private/reports/objects"
            / f"{package['report_json_sha256']}.json"
        )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["schema_version"], ANALYSIS_SCHEMA_V2)
        self.assertNotIn("capture_id", report)
        self.assertEqual(report_path.stat().st_mode & 0o077, 0)
        markdown_path = (
            self.runtime / "private/reports/markdown"
            / f"{package['report_markdown_sha256']}.md"
        )
        markdown = markdown_path.read_text(encoding="utf-8")
        self.assertIn(package["report_json_sha256"], markdown)
        self.assertIn(package["renderer_build_sha256"], markdown)
        quality = package["quality_receipt"]
        self.assertGreater(quality["char_count"], 0)
        self.assertLessEqual(quality["char_count"], 6000)
        self.assertEqual(quality["unknown_evidence_refs"], [])
        self.assertTrue(quality["section_coverage"]["formalization_candidates"])
        self.assertNotEqual(
            package["processing_fingerprint"],
            package["processing_contract_sha256"],
        )
        for stage in ("analysis", "critical_review"):
            receipt = package["stage_receipts"][stage]
            for key in (
                "prompt_sha256", "schema_sha256", "result_sha256",
                "output_sha256", "requested_model", "requested_reasoning_effort",
                "runtime_model", "runtime_reasoning_effort", "duration_ms",
                "artifact_ref", "artifact_sha256",
            ):
                self.assertIn(key, receipt)
            stage_path = (
                self.runtime / "private/reports/stages"
                / f"{receipt['artifact_sha256']}.json"
            )
            self.assertTrue(stage_path.is_file())
            self.assertEqual(stage_path.stat().st_mode & 0o077, 0)
        projection = json.loads(
            Path(config["dashboard"]["projection_path"]).read_text(encoding="utf-8")
        )
        item = projection["subjects"]["cs408"]["items"][0]
        self.assertEqual(item["safe_summary"], (
            "当前题答案安全的正式编纂候选，具体答案与完整解析不进入候选字段。"
        ))
        for forbidden in (
            "result_summary", "first_break", "suggestion_summary", "risks",
            "unresolved", "evidence_refs",
        ):
            self.assertNotIn(forbidden, item)
        self.assertEqual(
            item["runtime_identity_status"], "requested_unverified"
        )

    def legacy_contract_cs408_v2_consumer_reopens_report_and_binds_adoption(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        self.run_exact_cs408_force(worker)
        batched = copy.deepcopy(self.cs_status)
        batched["pending_capture_ids"] = []
        row = batched["captures"][self.cs_capture]
        row["quality_status"] = "curating"
        row["current_batch_id"] = "BATCH-V2-001"
        batched["active_batch"] = {
            "batch_id": "BATCH-V2-001", "study_date": "2026-08-04",
            "status": "active", "capture_set_sha256": "6" * 64,
            "completed_count": 0, "total_count": 1,
        }
        atomic_write_json(self.cs_status_path, batched)
        consumed = consume_package(
            config, subject="cs408", capture_id=self.cs_capture,
            study_date="2026-08-04", batch_id="BATCH-V2-001",
            capture_set_hash="6" * 64,
        )
        self.assertEqual(consumed["status"], "ready")
        current_job = Worker(
            config, model_runner=FakeV2Runner()
        ).store.read_job("cs408", self.cs_capture)
        self.assertEqual(
            consumed["publication_id"], current_job["publication_id"]
        )
        self.assertNotIn("report", consumed)
        self.assertNotIn("formalization_candidates", consumed)
        self.assertEqual(consumed["capture_set_sha256"], "6" * 64)
        self.assertEqual(consumed["capture_payload_sha256"], "5" * 64)
        receipt = record_adoption(
            config, adoption_token=consumed["adoption_token"],
            outcome="modified_adopted", reason_code="sol-revalidated",
            sol_decision_sha256="1" * 64,
            canonical_package_sha256="2" * 64,
            item_result_event_id="CURATION-ITEM-001",
            item_result_event_sha256="3" * 64,
            terminal_outcome="curated", formal_id="OS_2020_001",
            normal_receipt_sha256="4" * 64,
        )
        self.assertEqual(receipt["status"], "recorded")
        self.assertEqual(receipt["batch_id"], "BATCH-V2-001")
        self.assertEqual(receipt["terminal_outcome"], "curated")
        self.assertEqual(receipt["report_json_sha256"], consumed["report_json_sha256"])

    def legacy_contract_consumer_rejects_pointer_when_job_publication_is_not_committed(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        self.run_exact_cs408_force(worker)
        batched = copy.deepcopy(self.cs_status)
        batched["pending_capture_ids"] = []
        row = batched["captures"][self.cs_capture]
        row["quality_status"] = "curating"
        row["current_batch_id"] = "BATCH-PUBLICATION-001"
        batched["active_batch"] = {
            "batch_id": "BATCH-PUBLICATION-001",
            "study_date": "2026-08-04",
            "status": "active",
            "capture_set_sha256": "c" * 64,
            "completed_count": 0,
            "total_count": 1,
        }
        atomic_write_json(self.cs_status_path, batched)
        job = worker.store.read_job("cs408", self.cs_capture)
        job.update(
            {
                "status": "processing",
                "package_id": None,
                "package_path": None,
                "package_sha256": None,
                "publication_id": None,
            }
        )
        atomic_write_json(worker.store.job_path("cs408", self.cs_capture), job)
        result = consume_package(
            config,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
            batch_id="BATCH-PUBLICATION-001",
            capture_set_hash="c" * 64,
        )
        self.assertEqual(result["status"], "stale")
        self.assertIsNone(result["adoption_token"])
        self.assertFalse(result["validation"]["job_publication_binding"])
        offers = self.runtime / "state/offers"
        self.assertFalse(offers.exists() and any(offers.glob("*.json")))

    def legacy_contract_single_pass_degraded_is_checkpoint_not_consumable_offer(self) -> None:
        config = self.make_v2_config()
        worker, _, _ = self.publish_degraded_v2(config)
        batched = copy.deepcopy(self.cs_status)
        batched["pending_capture_ids"] = []
        row = batched["captures"][self.cs_capture]
        row["quality_status"] = "curating"
        row["current_batch_id"] = "BATCH-DEGRADED-001"
        batched["active_batch"] = {
            "batch_id": "BATCH-DEGRADED-001",
            "study_date": "2026-08-04",
            "status": "active",
            "capture_set_sha256": "d" * 64,
            "completed_count": 0,
            "total_count": 1,
        }
        atomic_write_json(self.cs_status_path, batched)
        result = consume_package(
            config,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
            batch_id="BATCH-DEGRADED-001",
            capture_set_hash="d" * 64,
        )
        self.assertEqual(result["status"], "single_pass_degraded")
        self.assertIsNone(result["adoption_token"])
        self.assertFalse(result["validation"]["two_stage_ready"])
        self.assertTrue(result["validation"]["critical_resume_required"])
        self.assertNotIn(
            "offered_at", worker.store.read_job("cs408", self.cs_capture)
        )
        failed_job = worker.store.read_job("cs408", self.cs_capture)
        failed_job.update(
            {
                "status": "failed",
                "last_error_code": "cs408_analysis_usage_limit",
                "package_id": None,
                "package_path": None,
                "package_sha256": None,
                "publication_id": None,
            }
        )
        atomic_write_json(
            worker.store.job_path("cs408", self.cs_capture), failed_job
        )
        stale = consume_package(
            config,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
            batch_id="BATCH-DEGRADED-001",
            capture_set_hash="d" * 64,
        )
        self.assertEqual(stale["status"], "stale")
        self.assertFalse(stale["validation"]["job_publication_binding"])
        self.assertIsNone(stale["adoption_token"])

    def test_cs408_v2_rejects_wrong_context_before_model_call(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        status = worker.adapters["cs408"].status("2026-08-04")
        wrong = copy.deepcopy(status)
        wrong["captures"][self.cs_capture]["capture"]["source_facts"][
            "details_id"
        ] = "DETAIL-OTHER"
        self.assertEqual(worker.adapters["cs408"].candidates(wrong), [])
        self.assertEqual(
            worker.adapters["cs408"].candidate_errors[self.cs_capture],
            "current_question_context_binding_mismatch",
        )

    def test_cs408_v2_depth_and_limited_evidence_gates(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        candidate = worker.adapters["cs408"].candidates(
            worker.adapters["cs408"].status("2026-08-04")
        )[0]
        self.assertEqual(candidate.model_input["candidate_kind"], "failure_capture")
        self.assertEqual(candidate.input_binding["candidate_kind"], "failure_capture")
        self.assertEqual(validate_v2_candidate_route(candidate), "failure_capture")
        prompt_runner = CodexRunner(
            {**config["model"], "cs408_deep_v2": config["cs408_deep_v2"]},
            self.runtime,
        )
        analysis_prompt = prompt_runner._cs408_analysis_prompt(
            candidate, config["cs408_deep_v2"]
        )
        self.assertIn(
            "versioned_processing.processing_skill.skill",
            analysis_prompt,
        )
        self.assertNotIn("standing policy", analysis_prompt)
        processing_skill = (
            ROOT
            / "plugin/kaoyan-study-intake/skills/"
            "background-cs408-processing/SKILL.md"
        ).read_text(encoding="utf-8")
        self.assertIn("`state.json` is a projection", processing_skill)
        self.assertIn("fragile", processing_skill)
        wrong_route = replace(
            candidate,
            model_input={**candidate.model_input, "candidate_kind": "study_observation"},
        )
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_v2_candidate_route_mismatch"
        ):
            validate_v2_candidate_route(wrong_route)
        limited = v2_analysis(candidate.allowed_evidence_refs[0], complete=False)
        validate_analysis_v2(limited, candidate.allowed_evidence_refs)
        missing_gap = copy.deepcopy(limited)
        missing_gap["evidence_assessment"]["gaps"] = []
        with self.assertRaisesRegex(
            PreprocessorError, "analysis_v2_limited_evidence_gates_missing"
        ):
            validate_analysis_v2(missing_gap, candidate.allowed_evidence_refs)

        refs = candidate.allowed_evidence_refs
        self.assertEqual(refs, semantic_evidence_refs_v2(candidate.model_input))
        self.assertLess(len(refs), 200)
        self.assertLess(sum(map(len, refs)), 15_000)
        for required_anchor in (
            "capture.source_facts",
            "current_question_evidence.current_question",
            "interaction_trace_evidence.events[0]",
            "study_observation.interaction_trace[0]",
            "study_observation.result_observation",
        ):
            self.assertIn(required_anchor, refs)
        unknown = v2_analysis(refs[0])
        unknown["risk_flags"][0]["evidence_refs"] = ["unknown.synthetic.ref"]
        with self.assertRaisesRegex(
            PreprocessorError, "analysis_v2_evidence_refs_invalid"
        ):
            validate_analysis_v2(unknown, refs)

    def legacy_contract_cs408_v2_conflict_fixture_preserves_counterevidence(self) -> None:
        config = self.make_v2_config()

        class ConflictRunner(FakeV2Runner):
            def run(inner_self, candidate):
                result = super().run(candidate)
                conflict = v2_claim(
                    candidate.allowed_evidence_refs[0],
                    "当前题的用户推理与评分依据在边界条件上存在冲突，不能把任一侧直接提升为已确认事实。",
                )
                result.analysis["evidence_assessment"]["contradictions"].append(
                    conflict
                )
                return result

        worker = Worker(config, model_runner=ConflictRunner())
        result = self.run_exact_cs408_force(worker)
        self.assertEqual(result["processed"][0]["status"], "two_pass_ready")
        job = worker.store.read_job("cs408", self.cs_capture)
        package = json.loads(Path(job["package_path"]).read_text(encoding="utf-8"))
        preservation = package["quality_receipt"]["contradictions_preserved"]
        self.assertEqual(preservation["draft_count"], 1)
        self.assertEqual(preservation["final_count"], 1)
        self.assertTrue(preservation["preserved"])
        report_path = (
            self.runtime / "private/reports/objects"
            / f"{package['report_json_sha256']}.json"
        )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(len(report["evidence_assessment"]["contradictions"]), 1)
        self.assertTrue(report["sol_verification_plan"]["conflict_handling_suggestions"])

    def legacy_contract_limited_evidence_full_json_gets_bounded_markdown_projection(self) -> None:
        config = self.make_v2_config()

        class OverlongLimitedRunner(FakeV2Runner):
            def __init__(inner_self):
                super().__init__(complete=False)

            def run(inner_self, candidate):
                result = super().run(candidate)
                result.analysis["question_structure"]["objects"][0]["text"] = (
                    "边界证据重复扩写" * 450
                )
                return result

        worker = Worker(config, model_runner=OverlongLimitedRunner())
        outcome = self.run_exact_cs408_force(worker)
        self.assertEqual(outcome["processed"][0]["status"], "two_pass_ready")
        job = worker.store.read_job("cs408", self.cs_capture)
        package = load_json(Path(job["package_path"]))
        report_path = worker.store.report_json_path(
            package["report_json_sha256"]
        )
        report = load_json(report_path)
        self.assertEqual(
            report["question_structure"]["objects"][0]["text"],
            "边界证据重复扩写" * 450,
        )
        markdown_path = worker.store.report_markdown_path(
            package["report_markdown_sha256"]
        )
        markdown = markdown_path.read_text(encoding="utf-8")
        expected = render_analysis_markdown(
            report,
            report_json_sha256=package["report_json_sha256"],
            renderer_build_sha256_value=package["renderer_build_sha256"],
        )
        self.assertEqual(markdown, expected)
        self.assertLessEqual(chinese_character_count(markdown), 6000)
        self.assertEqual(
            package["quality_receipt"]["char_count"],
            chinese_character_count(markdown),
        )

    def test_markdown_budget_is_deterministic_and_contract_bound(self) -> None:
        report = v2_analysis("capture.source_facts", complete=False)

        def expand_claim_lists(node):
            if isinstance(node, dict):
                for nested in node.values():
                    expand_claim_lists(nested)
            elif (
                isinstance(node, list)
                and node
                and isinstance(node[0], dict)
                and "claim_type" in node[0]
            ):
                node.extend(copy.deepcopy(node[0]) for _ in range(31))
            elif isinstance(node, list):
                for nested in node:
                    expand_claim_lists(nested)

        expand_claim_lists(report)
        before = hashlib.sha256(json_file_bytes(report)).hexdigest()
        renderer_sha = renderer_build_sha256()
        first = render_analysis_markdown(
            report,
            report_json_sha256=before,
            renderer_build_sha256_value=renderer_sha,
        )
        second = render_analysis_markdown(
            report,
            report_json_sha256=before,
            renderer_build_sha256_value=renderer_sha,
        )
        self.assertEqual(first, second)
        self.assertLessEqual(chinese_character_count(first), 6000)
        self.assertIn("完整内容见 JSON", first)
        self.assertEqual(hashlib.sha256(json_file_bytes(report)).hexdigest(), before)
        contract = cs408_processing_contract(self.make_v2_config())
        self.assertEqual(
            contract["markdown_budget_policy"][
                "projection_hard_max_chinese_chars"
            ],
            6000,
        )
        self.assertEqual(contract["renderer_build_sha256"], renderer_sha)

    def test_cs408_contract_freezes_loaded_implementation_identity(self) -> None:
        config = self.make_v2_config()
        with mock.patch.object(
            core.inspect,
            "getsource",
            side_effect=AssertionError(
                "publication must reuse the process-frozen implementation identity"
            ),
        ):
            first = cs408_processing_contract(config)
            second = cs408_processing_contract(config)

        self.assertNotIn("loaded_core_sha256", first)
        self.assertEqual(
            first["semantic_contract_schema_version"],
            "study-intake-subject-semantic-processing-contract-v2",
        )
        self.assertEqual(
            len(first["subject_semantic_code_closure_sha256"]), 64
        )
        self.assertEqual(
            first["status_script_sha256"], sha256_file(self.cs_script)
        )
        self.assertEqual(second, first)

    def test_cs408_status_script_binding_rejects_symlink_and_runtime_drift(
        self,
    ) -> None:
        symlink_config = self.make_v2_config()
        symlink_path = self.cs_repo / "status-link.py"
        symlink_path.symlink_to(self.cs_script)
        symlink_config["adapters"]["cs408"]["status_script"] = str(
            symlink_path
        )
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_status_script_invalid"
        ):
            cs408_processing_contract(symlink_config)

        config = self.make_v2_config()
        adapter = core.make_adapters(config)["cs408"]
        self.cs_script.write_text("raise SystemExit(23)\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_status_script_binding_mismatch"
        ):
            adapter.status("2026-08-04")

    def test_cs408_candidate_requires_timezone_aware_recorded_at(self) -> None:
        config = self.make_v2_config()
        adapter = core.make_adapters(config)["cs408"]

        missing = copy.deepcopy(self.cs_status)
        missing["captures"][self.cs_capture].pop("recorded_at")
        self.assertEqual(adapter.candidates(missing), [])
        self.assertEqual(
            adapter.candidate_errors[self.cs_capture],
            "cs408_capture_recorded_at_invalid",
        )

        naive = copy.deepcopy(self.cs_status)
        naive["captures"][self.cs_capture]["recorded_at"] = (
            "2026-08-04T08:00:00"
        )
        self.assertEqual(adapter.candidates(naive), [])
        self.assertEqual(
            adapter.candidate_errors[self.cs_capture],
            "cs408_capture_recorded_at_invalid",
        )

        valid = copy.deepcopy(self.cs_status)
        valid["captures"][self.cs_capture]["recorded_at"] = (
            "2026-08-04T08:00:00+08:00"
        )
        candidates = adapter.candidates(valid)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(
            candidates[0].recorded_at, "2026-08-04T08:00:00+08:00"
        )
        contract = cs408_processing_contract(config)
        self.assertEqual(
            candidates[0].input_binding["processing_contract_sha256"],
            contract["processing_contract_sha256"],
        )
        self.assertEqual(
            contract["status_script_sha256"], sha256_file(self.cs_script)
        )

    def legacy_contract_cs408_v2_thin_fixture_publishes_truthful_limited_report(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner(complete=False))
        result = self.run_exact_cs408_force(worker)
        self.assertEqual(result["processed"][0]["status"], "two_pass_ready")
        job = worker.store.read_job("cs408", self.cs_capture)
        package = json.loads(Path(job["package_path"]).read_text(encoding="utf-8"))
        report_path = (
            self.runtime / "private/reports/objects"
            / f"{package['report_json_sha256']}.json"
        )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(
            report["evidence_assessment"]["completeness"],
            "limited_by_evidence",
        )
        self.assertTrue(report["evidence_assessment"]["gaps"])
        self.assertTrue(report["unresolved"])
        self.assertTrue(report["sol_verification_plan"]["reject_if"])
        self.assertTrue(package["quality_receipt"]["section_coverage"]["evidence_assessment"])

    def legacy_contract_cs408_v2_second_stage_failures_degrade_only_a_valid_draft(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config, model_runner=FakeV2Runner())
        candidate = worker.adapters["cs408"].candidates(
            worker.adapters["cs408"].status("2026-08-04")
        )[0]
        valid_draft = StructuredStageResult(
            payload=v2_analysis(candidate.allowed_evidence_refs[0]),
            duration_ms=20,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            runtime_identity_status="confirmed",
            output_sha256="a" * 64,
        )
        model_config = copy.deepcopy(config["model"])
        model_config["timeout_seconds"] = 5
        model_config["cs408_deep_v2"] = copy.deepcopy(config["cs408_deep_v2"])
        for error_code in (
            "cs408_critical_review_timeout",
            "cs408_critical_review_output_invalid_json",
            "cs408_critical_review_nonzero_exit",
        ):
            runner = CodexRunner(model_config, self.runtime)
            with mock.patch.object(
                runner,
                "_execute_prompt",
                side_effect=[valid_draft, PreprocessorError(error_code)],
            ):
                result = runner.run(candidate)
            self.assertEqual(result.pipeline_status, "single_pass_degraded")
            self.assertEqual(
                result.stage_receipts["critical_review"]["error_code"],
                error_code,
            )

        runner = CodexRunner(model_config, self.runtime)
        with mock.patch.object(
            runner,
            "_execute_prompt",
            side_effect=[
                valid_draft,
                PreprocessorError(
                    "critical_review_correction_paths_mismatch"
                ),
            ],
        ):
            with self.assertRaisesRegex(
                PreprocessorError,
                "critical_review_correction_paths_mismatch",
            ):
                runner.run(candidate)

        invalid_draft = copy.deepcopy(valid_draft)
        invalid_draft = replace(invalid_draft, payload={"schema_version": "invalid"})
        runner = CodexRunner(model_config, self.runtime)
        with mock.patch.object(
            runner,
            "_execute_prompt",
            return_value=invalid_draft,
        ):
            with self.assertRaises(PreprocessorError):
                runner.run(candidate)

    def legacy_contract_analysis_checkpoint_survives_interrupt_and_resumes_only_critical(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config)
        status = worker.adapters["cs408"].status("2026-08-04")
        candidate = worker.adapters["cs408"].candidates(status)[0]
        worker.store.write_job(candidate, worker._processing_job(candidate, 1))
        draft = v2_analysis(candidate.allowed_evidence_refs[0])
        analysis_stage = StructuredStageResult(
            payload=draft,
            duration_ms=23,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            runtime_identity_status="confirmed",
            output_sha256="a" * 64,
        )
        stage_calls = []

        def interrupt_after_analysis(**kwargs):
            stage_calls.append(kwargs["stage_name"])
            if kwargs["stage_name"] == "cs408_analysis":
                return analysis_stage
            raise KeyboardInterrupt("synthetic worker interruption")

        with mock.patch.object(
            worker.runner,
            "_execute_prompt",
            side_effect=interrupt_after_analysis,
        ):
            with self.assertRaises(KeyboardInterrupt):
                worker.runner.run(candidate)
        self.assertEqual(
            stage_calls, ["cs408_analysis", "cs408_critical_review"]
        )
        checkpoint_job = worker.store.read_job("cs408", self.cs_capture)
        self.assertEqual(checkpoint_job["status"], "analysis_checkpoint_ready")
        checkpoint_sha256 = checkpoint_job["analysis_checkpoint_sha256"]
        binding_key = checkpoint_job["analysis_checkpoint_binding_key"]
        checkpoint_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/objects"
            / f"{checkpoint_sha256}.json"
        )
        binding_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/bindings"
            / f"{binding_key}.json"
        )
        self.assertEqual(sha256_file(checkpoint_path), checkpoint_sha256)
        self.assertEqual(
            sha256_file(binding_path),
            checkpoint_job["analysis_checkpoint_binding_sha256"],
        )
        self.assertEqual(checkpoint_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(binding_path.stat().st_mode & 0o777, 0o600)
        checkpoint_bytes = checkpoint_path.read_bytes()
        binding_bytes = binding_path.read_bytes()

        class MustNotRun:
            def __init__(inner_self) -> None:
                inner_self.calls = 0

            def run(inner_self, current):
                inner_self.calls += 1
                raise AssertionError("normal startup must not rerun analysis")

        guarded_runner = MustNotRun()
        normal_worker = Worker(config, model_runner=guarded_runner)
        normal = normal_worker.run_once(
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
        )
        self.assertEqual(normal["selected_count"], 0)
        self.assertEqual(
            normal["decisions"][0]["reason"], "critical_resume_required"
        )
        self.assertEqual(guarded_runner.calls, 0)
        interrupted_job = normal_worker.store.read_job(
            "cs408", self.cs_capture
        )
        self.assertEqual(interrupted_job["status"], "worker_interrupted")
        self.assertEqual(
            interrupted_job["critical_resume_status"],
            "manual_recovery_required",
        )

        critical_review = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass",
            "summary": "只复用已校验的第一阶段检查点完成第二阶段。",
            "revised_analysis": draft,
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        critical_stage = StructuredStageResult(
            payload=critical_review,
            duration_ms=31,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            runtime_identity_status="confirmed",
            output_sha256="b" * 64,
        )
        resume_worker = Worker(config)
        with mock.patch.object(
            resume_worker.runner,
            "_execute_prompt",
            return_value=critical_stage,
        ) as execute:
            resumed = resume_worker.run_once(
                subject="cs408",
                capture_id=self.cs_capture,
                study_date=candidate.study_date,
                resume_critical=True,
                expected_input_fingerprint=candidate.input_fingerprint,
            )
        self.assertEqual(execute.call_count, 1)
        self.assertEqual(
            execute.call_args.kwargs["stage_name"],
            "cs408_critical_review",
        )
        self.assertEqual(resumed["processed"][0]["status"], "two_pass_ready")
        final_job = resume_worker.store.read_job("cs408", self.cs_capture)
        self.assertEqual(final_job["status"], "two_pass_ready")
        self.assertTrue(final_job["analysis_checkpoint_reused"])
        self.assertEqual(final_job["formal_write_count"], 0)
        self.assertEqual(checkpoint_path.read_bytes(), checkpoint_bytes)
        self.assertEqual(binding_path.read_bytes(), binding_bytes)

    def test_analysis_checkpoint_is_scoped_by_release_and_authority_generation(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config)
        candidate = worker.adapters["cs408"].candidates(
            worker.adapters["cs408"].status("2026-08-04")
        )[0]
        receipt = {
            "prompt_version": "checkpoint-authority-test-v1",
            "prompt_sha256": "1" * 64,
            "schema_sha256": "2" * 64,
            "ordered_image_sha256s": [],
        }
        first_runner = CodexRunner(
            {
                "authority_release_id": "a" * 64,
                "authority_generation_id": "b" * 64,
            },
            self.runtime,
        )
        first = first_runner._write_analysis_checkpoint(
            candidate,
            draft={"generation": "first"},
            analysis_receipt=receipt,
        )
        first_binding_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/bindings"
            / f"{first['binding_key']}.json"
        )
        first_object_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/objects"
            / f"{first['checkpoint_sha256']}.json"
        )
        first_binding_bytes = first_binding_path.read_bytes()
        first_object_bytes = first_object_path.read_bytes()

        next_release_runner = CodexRunner(
            {
                "authority_release_id": "c" * 64,
                "authority_generation_id": "d" * 64,
            },
            self.runtime,
        )
        next_release = next_release_runner._write_analysis_checkpoint(
            candidate,
            draft={"generation": "next-release"},
            analysis_receipt=receipt,
        )
        self.assertNotEqual(first["binding_key"], next_release["binding_key"])

        next_generation_runner = CodexRunner(
            {
                "authority_release_id": "c" * 64,
                "authority_generation_id": "e" * 64,
            },
            self.runtime,
        )
        next_generation = next_generation_runner._write_analysis_checkpoint(
            candidate,
            draft={"generation": "next-authority-generation"},
            analysis_receipt=receipt,
        )
        self.assertNotEqual(
            next_release["binding_key"], next_generation["binding_key"]
        )
        self.assertEqual(first_binding_path.read_bytes(), first_binding_bytes)
        self.assertEqual(first_object_path.read_bytes(), first_object_bytes)
        self.assertEqual(
            load_json(first_binding_path)["generation_schema_version"],
            "study-intake-analysis-checkpoint-generation-v2",
        )
        self.assertEqual(
            load_json(first_binding_path)["authority_release_id"],
            "a" * 64,
        )
        self.assertEqual(
            load_json(first_binding_path)["authority_generation_id"],
            "b" * 64,
        )

    def legacy_contract_exact_checkpoint_job_recovery_repairs_drift_without_model(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config)
        status = worker.adapters["cs408"].status("2026-08-04")
        candidate = worker.adapters["cs408"].candidates(status)[0]
        worker.store.write_job(candidate, worker._processing_job(candidate, 1))
        draft = v2_analysis(candidate.allowed_evidence_refs[0])
        analysis_stage = StructuredStageResult(
            payload=draft,
            duration_ms=23,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            runtime_identity_status="confirmed",
            output_sha256="a" * 64,
        )

        def interrupt_after_analysis(**kwargs):
            if kwargs["stage_name"] == "cs408_analysis":
                return analysis_stage
            raise KeyboardInterrupt("synthetic worker interruption")

        with mock.patch.object(
            worker.runner,
            "_execute_prompt",
            side_effect=interrupt_after_analysis,
        ):
            with self.assertRaises(KeyboardInterrupt):
                worker.runner.run(candidate)
        checkpoint_job = worker.store.read_job("cs408", self.cs_capture)
        checkpoint_sha256 = checkpoint_job["analysis_checkpoint_sha256"]

        drifted_job = dict(checkpoint_job)
        drifted_job.update(
            {
                "status": "retrying",
                "input_fingerprint": "f" * 64,
                "input_binding": {"stale": True},
                "last_error_code": "cs408_analysis_nonzero_exit",
                "next_retry_at": "2026-08-05T00:00:00Z",
            }
        )
        for key in (
            "analysis_checkpoint_sha256",
            "analysis_checkpoint_ref",
            "analysis_checkpoint_binding_key",
            "analysis_checkpoint_binding_sha256",
        ):
            drifted_job.pop(key, None)
        worker.store.write_job(candidate, drifted_job)

        with mock.patch.object(
            worker.runner,
            "run",
            side_effect=AssertionError("recovery must not invoke a model"),
        ) as model_run:
            recovered = recover_analysis_checkpoint_job(
                worker,
                subject="cs408",
                capture_id=self.cs_capture,
                study_date="2026-08-04",
                expected_input_fingerprint=candidate.input_fingerprint,
            )
        self.assertEqual(model_run.call_count, 0)
        self.assertEqual(recovered["status"], "recovered")
        self.assertEqual(
            recovered["analysis_checkpoint_sha256"], checkpoint_sha256
        )
        receipt_path = Path(recovered["receipt_path"])
        self.assertEqual(sha256_file(receipt_path), recovered["receipt_sha256"])
        repaired = worker.store.read_job("cs408", self.cs_capture)
        self.assertEqual(repaired["status"], "worker_interrupted")
        self.assertEqual(
            repaired["critical_resume_status"], "manual_recovery_required"
        )
        self.assertEqual(repaired["input_fingerprint"], candidate.input_fingerprint)
        self.assertEqual(repaired["input_binding"], candidate.input_binding)
        self.assertEqual(repaired["analysis_checkpoint_sha256"], checkpoint_sha256)
        self.assertEqual(repaired["formal_write_count"], 0)

        second = recover_analysis_checkpoint_job(
            worker,
            subject="cs408",
            capture_id=self.cs_capture,
            study_date="2026-08-04",
            expected_input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(second["status"], "noop")
        self.assertEqual(sha256_file(receipt_path), recovered["receipt_sha256"])

    def test_cross_generation_migration_functions_fail_before_worker_access(self) -> None:
        worker = mock.Mock()
        common = {
            "worker": worker,
            "subject": "cs408",
            "capture_id": self.cs_capture,
            "study_date": "2026-08-04",
            "expected_input_fingerprint": "a" * 64,
            "source_input_fingerprint": "b" * 64,
            "source_checkpoint_sha256": "c" * 64,
            "source_binding_key": "d" * 64,
            "source_binding_sha256": "e" * 64,
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "checkpoint_migration_retired_legacy_only",
        ):
            migrate_analysis_checkpoint_generation(**common)
        with self.assertRaisesRegex(
            PreprocessorError,
            "publication_migration_retired_legacy_only",
        ):
            migrate_v2_publication_generation(
                **common,
                source_package_sha256="f" * 64,
                source_publication_id="legacy-publication",
            )
        self.assertEqual(worker.mock_calls, [])

    def test_cross_generation_migration_cli_is_explicit_legacy_only(self) -> None:
        atomic_write_json(self.config_path, self.make_v2_config())
        common = [
            "--subject", "cs408",
            "--capture-id", self.cs_capture,
            "--date", "2026-08-04",
            "--expected-input-fingerprint", "a" * 64,
            "--source-input-fingerprint", "b" * 64,
            "--source-checkpoint-sha256", "c" * 64,
            "--source-binding-key", "d" * 64,
            "--source-binding-sha256", "e" * 64,
        ]
        cases = (
            (
                "migrate-analysis-checkpoint",
                common,
                "checkpoint_migration_retired_legacy_only",
            ),
            (
                "migrate-v2-publication",
                common + [
                    "--source-package-sha256", "f" * 64,
                    "--source-publication-id", "legacy-publication",
                ],
                "publication_migration_retired_legacy_only",
            ),
        )
        for command, arguments, error_code in cases:
            with self.subTest(command=command):
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "bin/preprocess_worker.py"),
                        "--config",
                        str(self.config_path),
                        command,
                        *arguments,
                    ],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                self.assertEqual(completed.returncode, 2)
                result = json.loads(completed.stdout)
                self.assertEqual(result["status"], "error")
                self.assertEqual(result["error_code"], error_code)
                self.assertEqual(result["formal_write_count"], 0)

    def legacy_contract_analysis_checkpoint_migrates_only_exact_analysis_identity(self) -> None:
        source_config = self.make_v2_config()
        source_worker = Worker(source_config)
        source_status = source_worker.adapters["cs408"].status("2026-08-04")
        source_candidate = source_worker.adapters["cs408"].candidates(
            source_status
        )[0]
        source_worker.store.write_job(
            source_candidate,
            source_worker._processing_job(source_candidate, 1),
        )
        draft = v2_analysis(source_candidate.allowed_evidence_refs[0])
        analysis_stage = StructuredStageResult(
            payload=draft,
            duration_ms=23,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_event",
            runtime_identity_status="confirmed",
            output_sha256="a" * 64,
        )

        def interrupt_after_analysis(**kwargs):
            if kwargs["stage_name"] == "cs408_analysis":
                return analysis_stage
            raise KeyboardInterrupt("synthetic worker interruption")

        with mock.patch.object(
            source_worker.runner,
            "_execute_prompt",
            side_effect=interrupt_after_analysis,
        ):
            with self.assertRaises(KeyboardInterrupt):
                source_worker.runner.run(source_candidate)
        source_job = source_worker.store.read_job("cs408", self.cs_capture)
        source_checkpoint_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/objects"
            / f"{source_job['analysis_checkpoint_sha256']}.json"
        )
        source_checkpoint_bytes = source_checkpoint_path.read_bytes()

        target_config = copy.deepcopy(source_config)
        target_config["cs408_deep_v2"]["critical_review_prompt_version"] = (
            "test-critical-review-downstream-v3"
        )
        target_worker = Worker(target_config)
        target_candidate = target_worker.adapters["cs408"].candidates(
            target_worker.adapters["cs408"].status("2026-08-04")
        )[0]
        self.assertNotEqual(
            target_candidate.input_fingerprint,
            source_candidate.input_fingerprint,
        )
        self.assertEqual(
            sorted(
                key
                for key in source_candidate.input_binding
                if source_candidate.input_binding[key]
                != target_candidate.input_binding[key]
            ),
            ["processing_contract_sha256"],
        )
        with mock.patch.object(
            target_worker.runner,
            "_execute_prompt",
            side_effect=AssertionError("migration must not invoke a model"),
        ) as execute:
            migrated = migrate_analysis_checkpoint_generation(
                target_worker,
                subject="cs408",
                capture_id=self.cs_capture,
                study_date="2026-08-04",
                expected_input_fingerprint=target_candidate.input_fingerprint,
                source_input_fingerprint=source_candidate.input_fingerprint,
                source_checkpoint_sha256=source_job[
                    "analysis_checkpoint_sha256"
                ],
                source_binding_key=source_job[
                    "analysis_checkpoint_binding_key"
                ],
                source_binding_sha256=source_job[
                    "analysis_checkpoint_binding_sha256"
                ],
            )
        self.assertEqual(execute.call_count, 0)
        self.assertEqual(migrated["status"], "migrated")
        self.assertEqual(migrated["model_call_count"], 0)
        self.assertNotEqual(
            migrated["analysis_checkpoint_sha256"],
            source_job["analysis_checkpoint_sha256"],
        )
        self.assertEqual(source_checkpoint_path.read_bytes(), source_checkpoint_bytes)
        target_job = target_worker.store.read_job("cs408", self.cs_capture)
        self.assertEqual(target_job["status"], "analysis_checkpoint_ready")
        self.assertEqual(
            target_job["input_fingerprint"], target_candidate.input_fingerprint
        )
        self.assertTrue(target_job["analysis_stage_identity_reused"])
        loaded_draft, loaded_receipt, _ = (
            target_worker.runner.load_analysis_checkpoint(target_candidate)
        )
        self.assertEqual(
            {
                key: value for key, value in loaded_draft.items()
                if key not in core.CS408_CANDIDATE_V3_FIELDS
            },
            draft,
        )
        self.assertEqual(
            loaded_draft["candidate_schema_version"],
            "study-intake-candidate-v3",
        )
        self.assertEqual(
            loaded_receipt["result_sha256"], sha256_value(loaded_draft)
        )
        migration_receipt = Path(migrated["receipt_path"])
        self.assertEqual(
            sha256_file(migration_receipt), migrated["receipt_sha256"]
        )

    def legacy_contract_two_stage_publication_migrates_without_model_call(self) -> None:
        source_config = self.make_v2_config()
        source_worker = Worker(source_config)
        source_candidate = source_worker.adapters["cs408"].candidates(
            source_worker.adapters["cs408"].status("2026-08-04")
        )[0]
        draft = v2_analysis(source_candidate.allowed_evidence_refs[0])
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass",
            "summary": "两阶段结果已按同一冻结证据复核。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        stage_results = {
            "cs408_analysis": StructuredStageResult(
                payload=draft,
                duration_ms=23,
                runtime_model="gpt-5.6-luna",
                runtime_reasoning_effort="max",
                runtime_metadata_provenance="codex_json_event",
                runtime_identity_status="confirmed",
                output_sha256="a" * 64,
            ),
            "cs408_critical_review": StructuredStageResult(
                payload=critical,
                duration_ms=29,
                runtime_model="gpt-5.6-luna",
                runtime_reasoning_effort="max",
                runtime_metadata_provenance="codex_json_event",
                runtime_identity_status="confirmed",
                output_sha256="b" * 64,
            ),
        }

        with mock.patch.object(
            source_worker.runner,
            "_execute_prompt",
            side_effect=lambda **kwargs: stage_results[kwargs["stage_name"]],
        ):
            source_result = self.run_exact_cs408_force(source_worker)
        self.assertEqual(
            source_result["processed"][0]["status"], "two_pass_ready"
        )
        source_job = source_worker.store.read_job("cs408", self.cs_capture)
        source_package_path = Path(source_job["package_path"])
        source_package_bytes = source_package_path.read_bytes()
        source_checkpoint_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/objects"
            / f"{source_job['analysis_checkpoint_sha256']}.json"
        )
        source_checkpoint_bytes = source_checkpoint_path.read_bytes()

        target_config = copy.deepcopy(source_config)
        target_config["cs408_deep_v2"]["max_prompt_bytes"] += 1
        target_worker = Worker(target_config)
        target_candidate = target_worker.adapters["cs408"].candidates(
            target_worker.adapters["cs408"].status("2026-08-04")
        )[0]
        self.assertNotEqual(
            target_candidate.input_fingerprint,
            source_candidate.input_fingerprint,
        )
        self.assertEqual(
            sorted(
                key
                for key in source_candidate.input_binding
                if source_candidate.input_binding[key]
                != target_candidate.input_binding[key]
            ),
            ["processing_contract_sha256"],
        )

        with mock.patch.object(
            target_worker.runner,
            "_execute_prompt",
            side_effect=AssertionError(
                "two-stage publication migration must not invoke a model"
            ),
        ) as execute:
            migrated = migrate_v2_publication_generation(
                target_worker,
                subject="cs408",
                capture_id=self.cs_capture,
                study_date="2026-08-04",
                expected_input_fingerprint=(
                    target_candidate.input_fingerprint
                ),
                source_input_fingerprint=(
                    source_candidate.input_fingerprint
                ),
                source_checkpoint_sha256=source_job[
                    "analysis_checkpoint_sha256"
                ],
                source_binding_key=source_job[
                    "analysis_checkpoint_binding_key"
                ],
                source_binding_sha256=source_job[
                    "analysis_checkpoint_binding_sha256"
                ],
                source_package_sha256=source_job["package_sha256"],
                source_publication_id=source_job["publication_id"],
            )
        self.assertEqual(execute.call_count, 0)
        self.assertEqual(migrated["status"], "two_pass_ready")
        self.assertEqual(migrated["model_call_count"], 0)
        self.assertEqual(migrated["formal_write_count"], 0)
        self.assertEqual(source_package_path.read_bytes(), source_package_bytes)
        self.assertEqual(
            source_checkpoint_path.read_bytes(), source_checkpoint_bytes
        )

        target_job = target_worker.store.read_job("cs408", self.cs_capture)
        self.assertEqual(target_job["status"], "two_pass_ready")
        self.assertEqual(
            target_job["input_fingerprint"],
            target_candidate.input_fingerprint,
        )
        self.assertEqual(
            target_job["input_binding"], target_candidate.input_binding
        )
        self.assertEqual(
            target_job["publication_migration_model_call_count"], 0
        )
        target_package = load_json(Path(target_job["package_path"]))
        self.assertEqual(
            target_package["report_json_sha256"],
            source_job["report_json_sha256"],
        )
        self.assertEqual(
            target_package["report_markdown_sha256"],
            source_job["report_markdown_sha256"],
        )
        self.assertNotEqual(
            target_job["package_sha256"], source_job["package_sha256"]
        )
        receipt_path = Path(migrated["receipt_path"])
        self.assertEqual(
            sha256_file(receipt_path), migrated["receipt_sha256"]
        )

    def legacy_contract_408_critical_semantic_correction_recomputes_host_candidate_v3(self) -> None:
        config = self.make_v2_config()
        worker = Worker(config)
        candidate = worker.adapters["cs408"].candidates(
            worker.adapters["cs408"].status("2026-08-04")
        )[0]
        ref = candidate.allowed_evidence_refs[0]
        draft = v2_analysis(ref)
        draft["atomic_signals"].append(
            {
                "signal_id": "SIG-BASE-METHOD",
                "signal_type": "method",
                "canonical_term": "已有具体方法候选",
                "surface_form": "已有方法",
                "importance": "secondary",
                "provenance": "current_question_evidence",
                "evidence_refs": [ref],
                "confidence": "medium",
                "error_role": "none",
                "specificity": "具体到当前题的已有方法步骤",
                "applicability_boundary": "只适用于当前题已冻结证据",
                "truth_library_match_status": "unmatched",
            }
        )
        revised = copy.deepcopy(draft)
        revised["atomic_signals"].append(
            {
                "signal_id": "SIG-NEW-METHOD",
                "signal_type": "method",
                "canonical_term": "具体的新方法候选",
                "surface_form": "新的具体方法",
                "importance": "secondary",
                "provenance": "current_question_evidence",
                "evidence_refs": [ref],
                "confidence": "medium",
                "error_role": "none",
                "specificity": "具体到当前题的方法步骤",
                "applicability_boundary": "只适用于当前题已冻结证据",
                "truth_library_match_status": "unmatched",
            }
        )
        finding = {
            "finding_id": "FIND-MISSING-METHOD",
            "severity": "error",
            "text": "第一轮遗漏了一个有直接证据的方法信号。",
            "analysis_refs": ["analysis.atomic_signals"],
            "evidence_refs": [ref],
            "affected_json_paths": ["$.atomic_signals"],
        }
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "已补入遗漏的原子方法信号，由宿主重算候选字段。",
            "revised_analysis": revised,
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [finding],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-HOST-PROJECTION-ADVISORY",
                    "severity": "warning",
                    "text": "宿主应按修订后的原子信号重算确定性匹配投影。",
                    "analysis_refs": ["analysis.truth_match_matrix"],
                    "evidence_refs": [ref],
                    "affected_json_paths": ["$.truth_match_matrix"],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": finding["finding_id"],
                    "resolution": "applied",
                    "affected_json_paths": finding["affected_json_paths"],
                    "before": encode_correction_delta(None),
                    "after": encode_correction_delta(None),
                    "reason": "修正后的语义数组已经包含该信号。",
                }
            ],
        }
        stage_results = {
            "cs408_analysis": StructuredStageResult(
                payload=draft,
                duration_ms=23,
                runtime_model="gpt-5.6-luna",
                runtime_reasoning_effort="max",
                runtime_metadata_provenance="codex_json_event",
                runtime_identity_status="confirmed",
                output_sha256="a" * 64,
            ),
            "cs408_critical_review": StructuredStageResult(
                payload=critical,
                duration_ms=29,
                runtime_model="gpt-5.6-luna",
                runtime_reasoning_effort="max",
                runtime_metadata_provenance="codex_json_event",
                runtime_identity_status="confirmed",
                output_sha256="b" * 64,
            ),
        }
        with mock.patch.object(
            worker.runner,
            "_execute_prompt",
            side_effect=lambda **kwargs: stage_results[kwargs["stage_name"]],
        ):
            result = self.run_exact_cs408_force(worker)

        self.assertEqual(result["processed"][0]["status"], "two_pass_ready")
        job = worker.store.read_job("cs408", self.cs_capture)
        report = load_json(worker.store.report_json_path(job["report_json_sha256"]))
        self.assertIn(
            "SIG-NEW-METHOD",
            {
                row["signal_id"]
                for row in report["novel_knowledge_candidates"]
            },
        )
        self.assertEqual(
            report["candidate_schema_version"], "study-intake-candidate-v3"
        )
        self.assertEqual(job["formal_write_count"], 0)

    def test_408_host_owned_advisory_cannot_have_correction_resolution(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        draft["truth_match_matrix"] = []
        finding = {
            "finding_id": "FIND-HOST-PROJECTION-ADVISORY",
            "severity": "warning",
            "text": "宿主应重算确定性匹配投影。",
            "analysis_refs": ["analysis.truth_match_matrix"],
            "evidence_refs": [ref],
            "affected_json_paths": ["$.truth_match_matrix"],
        }
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "只读审计提示不得携带宿主字段 correction delta。",
            "revised_analysis": v2_analysis(ref),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [finding],
            "correction_resolutions": [
                {
                    "finding_id": finding["finding_id"],
                    "resolution": "applied",
                    "affected_json_paths": finding["affected_json_paths"],
                    "before": encode_correction_delta(None),
                    "after": encode_correction_delta(None),
                    "reason": "非法尝试改写宿主字段。",
                }
            ],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_sol_priority_resolution_forbidden",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=[ref],
                draft_analysis=draft,
            )

    def test_408_host_owned_advisory_uses_analysis_ref_without_path(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        draft["existing_formal_edges"] = []
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "宿主确定性投影只进入 Sol 只读提示。",
            "revised_analysis": v2_analysis(ref),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-HOST-PROJECTION-ADVISORY",
                    "severity": "warning",
                    "text": "由宿主复核完整直接边。",
                    "analysis_refs": ["analysis.existing_formal_edges"],
                    "evidence_refs": [ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        validated = core.validate_critical_review_v2(
            critical,
            allowed_evidence_refs=[ref],
            draft_analysis=draft,
        )
        self.assertEqual(validated["verdict"], "pass_with_warnings")
        self.assertEqual(
            validated["sol_priority_checks"][0]["affected_json_paths"], []
        )

    def test_408_real_host_owned_item_ref_fails_without_canonicalization(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        draft["unmatched_signals"] = [
            {
                "signal_id": "SIG-QTYPE-0001",
                "status": "needs_user_decision",
                "reason": "question_type_has_no_exact_truth_node",
                "evidence_refs": [ref],
            }
        ]
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "宿主投影中的项目引用不属于精确允许集合。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-HOST-ITEM-NOT-ALLOWED",
                    "severity": "warning",
                    "text": "由宿主复核未匹配题型投影。",
                    "analysis_refs": ["analysis.unmatched_signals[0]"],
                    "evidence_refs": [ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        original = copy.deepcopy(critical)
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_refs_invalid",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=[ref],
                draft_analysis=draft,
            )
        self.assertEqual(
            critical,
            original,
        )

    def test_408_tainted_analysis_ref_fails_without_normalization(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        exact_ref = "analysis.question_structure.mechanism_structure[0]"
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "污染的引用必须失败关闭，不能自动裁剪。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-TAINTED-ANALYSIS-REF",
                    "severity": "warning",
                    "text": "引用带有额外字符。",
                    "analysis_refs": [exact_ref + "чаты?"],
                    "evidence_refs": [ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        original = copy.deepcopy(critical)
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_refs_invalid",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=[ref],
                draft_analysis=draft,
            )
        self.assertEqual(critical, original)

    def test_408_exact_analysis_ref_passes_without_rewrite(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        exact_ref = "analysis.question_structure.mechanism_structure[0]"
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "精确引用保持原字节通过。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-EXACT-ANALYSIS-REF",
                    "severity": "warning",
                    "text": "精确引用属于冻结 Analysis 集合。",
                    "analysis_refs": [exact_ref],
                    "evidence_refs": [ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        validated = core.validate_critical_review_v2(
            critical,
            allowed_evidence_refs=[ref],
            draft_analysis=draft,
        )
        self.assertEqual(
            validated["sol_priority_checks"][0]["analysis_refs"],
            [exact_ref],
        )

    def test_408_missing_host_owned_item_ref_still_fails_closed(self) -> None:
        ref = "current_question_evidence.current_question"
        draft = v2_analysis(ref)
        draft["unmatched_signals"] = []
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "不存在的宿主投影项目不能被折叠为合法 section anchor。",
            "revised_analysis": copy.deepcopy(draft),
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [
                {
                    "finding_id": "FIND-HOST-ITEM-MISSING",
                    "severity": "warning",
                    "text": "不存在的投影项目必须失败关闭。",
                    "analysis_refs": ["analysis.unmatched_signals[0]"],
                    "evidence_refs": [ref],
                    "affected_json_paths": [],
                }
            ],
            "correction_resolutions": [],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_refs_invalid",
        ):
            core.validate_critical_review_v2(
                critical,
                allowed_evidence_refs=[ref],
                draft_analysis=draft,
            )


    def legacy_contract_resume_critical_reuses_verified_analysis_and_upgrades_package(self) -> None:
        config = self.make_v2_config()
        worker, draft, initial_calls = self.publish_degraded_v2(config)
        old_job = worker.store.read_job("cs408", self.cs_capture)
        old_package_path = Path(old_job["package_path"])
        old_package_raw = old_package_path.read_bytes()
        old_package = json.loads(old_package_raw)
        self.assertEqual(
            old_package["stage_receipts"]["critical_review"]["error_code"],
            "cs408_critical_review_usage_limit",
        )
        self.assertEqual(
            old_package["stage_receipts"]["critical_review"]["retry_at_hint"],
            "2026-08-05T00:00:00Z",
        )
        forced = self.run_exact_cs408_force(Worker(config))
        self.assertEqual(forced["selected_count"], 0)
        self.assertEqual(
            forced["decisions"][0]["reason"], "critical_resume_required"
        )
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass",
            "summary": "额度恢复后只执行第二阶段。",
            "revised_analysis": draft,
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        resume_calls = []
        real_run = subprocess.run

        def resume_run(command, **kwargs):
            if "--output-schema" not in command:
                return real_run(command, **kwargs)
            resume_calls.append(list(command))
            schema_path = Path(command[command.index("--output-schema") + 1])
            self.assertIn("analysis_ref", json.loads(schema_path.read_text())["$defs"])
            output_path = Path(command[command.index("--output-last-message") + 1])
            output_path.write_text(json.dumps(critical), encoding="utf-8")
            return SimpleNamespace(
                returncode=0,
                stdout=b'{"model":"gpt-5.6-luna","reasoning_effort":"max"}\n',
                stderr=b"",
            )

        resume_worker = Worker(config)
        resume_candidate = resume_worker.adapters["cs408"].candidates(
            resume_worker.adapters["cs408"].status("2026-08-04")
        )[0]
        with mock.patch.object(
            resume_worker.runner, "_invoke_subprocess", side_effect=resume_run
        ):
            resumed = resume_worker.run_once(
                subject="cs408",
                capture_id=self.cs_capture,
                study_date="2026-08-04",
                resume_critical=True,
                expected_input_fingerprint=resume_candidate.input_fingerprint,
            )
        self.assertEqual(len(initial_calls), 2)
        self.assertEqual(len(resume_calls), 1)
        self.assertEqual(resumed["processed"][0]["status"], "two_pass_ready")
        new_job = Worker(config, model_runner=FakeV2Runner()).store.read_job(
            "cs408", self.cs_capture
        )
        new_package_path = Path(new_job["package_path"])
        self.assertNotEqual(new_package_path, old_package_path)
        self.assertEqual(old_package_path.read_bytes(), old_package_raw)
        new_package = json.loads(new_package_path.read_text(encoding="utf-8"))
        self.assertEqual(new_package["pipeline_status"], "two_pass_ready")
        self.assertEqual(
            new_package["stage_receipts"]["analysis"],
            old_package["stage_receipts"]["analysis"],
        )
        self.assertEqual(
            new_package["stage_receipts"]["critical_review"]["status"], "ready"
        )
        self.assertEqual(new_package["formal_write_count"], 0)

    def legacy_contract_resume_critical_drift_or_failure_preserves_degraded_state(self) -> None:
        config = self.make_v2_config()
        worker, _, _ = self.publish_degraded_v2(config)
        job_path = worker.store.job_path("cs408", self.cs_capture)
        pointer_path = worker.store.latest_path("cs408", self.cs_capture)
        old_job_raw = job_path.read_bytes()
        old_pointer_raw = pointer_path.read_bytes()
        old_job = json.loads(old_job_raw)
        old_package_path = Path(old_job["package_path"])
        old_package_raw = old_package_path.read_bytes()
        drifted = copy.deepcopy(config)
        drifted["cs408_deep_v2"]["critical_review_prompt_version"] = (
            "drifted-review-v3"
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_resume_job_binding_mismatch|critical_resume_processing_contract_drift",
        ):
            Worker(drifted).run_once(
                subject="cs408",
                capture_id=self.cs_capture,
                study_date="2026-08-04",
                resume_critical=True,
                expected_input_fingerprint=(
                    Worker(drifted).adapters["cs408"].candidates(
                        Worker(drifted).adapters["cs408"].status("2026-08-04")
                    )[0].input_fingerprint
                ),
            )
        drift_job = json.loads(job_path.read_text(encoding="utf-8"))
        self.assertEqual(drift_job["status"], old_job["status"])
        self.assertEqual(drift_job["package_id"], old_job["package_id"])
        self.assertEqual(drift_job["package_sha256"], old_job["package_sha256"])
        self.assertEqual(drift_job["publication_id"], old_job["publication_id"])
        self.assertEqual(drift_job["critical_resume_status"], "failed")
        self.assertEqual(drift_job["critical_resume_attempts"], 1)
        self.assertRegex(
            drift_job["critical_resume_error_code"],
            "critical_resume_job_binding_mismatch|critical_resume_processing_contract_drift",
        )
        drift_receipt_path = (
            worker.root
            / "receipts"
            / "critical-resume"
            / "cs408"
            / "2026-08-04"
            / self.cs_capture
            / f"{drift_job['critical_resume_receipt_sha256']}.json"
        )
        self.assertTrue(drift_receipt_path.exists())
        self.assertEqual(
            sha256_file(drift_receipt_path),
            drift_job["critical_resume_receipt_sha256"],
        )
        drift_receipt = json.loads(drift_receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(drift_receipt["status"], "failed")
        self.assertEqual(drift_receipt["formal_write_count"], 0)
        self.assertEqual(pointer_path.read_bytes(), old_pointer_raw)
        self.assertEqual(old_package_path.read_bytes(), old_package_raw)

        real_run = subprocess.run
        resume_calls = []

        def fail_resume(command, **kwargs):
            if "--output-schema" not in command:
                return real_run(command, **kwargs)
            resume_calls.append(list(command))
            embedded = json.dumps(
                {"type": "error", "message": "You've hit your usage limit."}
            )
            return SimpleNamespace(
                returncode=1,
                stdout=(
                    json.dumps({"type": "error", "message": embedded}) + "\n"
                ).encode("utf-8"),
                stderr=b"private error details",
            )

        failed_resume_worker = Worker(config)
        failed_resume_candidate = failed_resume_worker.adapters[
            "cs408"
        ].candidates(
            failed_resume_worker.adapters["cs408"].status("2026-08-04")
        )[0]
        with mock.patch.object(
            failed_resume_worker.runner,
            "_invoke_subprocess",
            side_effect=fail_resume,
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "cs408_critical_review_usage_limit"
            ):
                failed_resume_worker.run_once(
                    subject="cs408",
                    capture_id=self.cs_capture,
                    study_date="2026-08-04",
                    resume_critical=True,
                    expected_input_fingerprint=(
                        failed_resume_candidate.input_fingerprint
                    ),
                )
        self.assertEqual(len(resume_calls), 1)
        failed_job = json.loads(job_path.read_text(encoding="utf-8"))
        self.assertEqual(failed_job["status"], old_job["status"])
        self.assertEqual(failed_job["package_id"], old_job["package_id"])
        self.assertEqual(failed_job["package_sha256"], old_job["package_sha256"])
        self.assertEqual(failed_job["publication_id"], old_job["publication_id"])
        self.assertEqual(failed_job["critical_resume_status"], "failed")
        self.assertEqual(failed_job["critical_resume_attempts"], 2)
        self.assertEqual(
            failed_job["critical_resume_error_code"],
            "cs408_critical_review_usage_limit",
        )
        failed_receipt_path = (
            worker.root
            / "receipts"
            / "critical-resume"
            / "cs408"
            / "2026-08-04"
            / self.cs_capture
            / f"{failed_job['critical_resume_receipt_sha256']}.json"
        )
        self.assertTrue(failed_receipt_path.exists())
        self.assertEqual(
            sha256_file(failed_receipt_path),
            failed_job["critical_resume_receipt_sha256"],
        )
        failed_receipt = json.loads(failed_receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(failed_receipt["status"], "failed")
        self.assertEqual(
            failed_receipt["error_code"], "cs408_critical_review_usage_limit"
        )
        self.assertIsNone(failed_receipt["next_retry_at"])
        self.assertEqual(failed_receipt["formal_write_count"], 0)
        self.assertEqual(pointer_path.read_bytes(), old_pointer_raw)
        self.assertEqual(old_package_path.read_bytes(), old_package_raw)
        with self.assertRaisesRegex(
            PreprocessorError, "critical_resume_exact_args_required"
        ):
            Worker(config).run_once(
                subject="cs408",
                capture_id=self.cs_capture,
                resume_critical=True,
            )

    def legacy_contract_terminal_knowledge_advance_requires_explicit_replay_authority(
        self,
    ) -> None:
        config = self.make_v2_config()
        runner = FakeV2Runner()
        worker = Worker(config, model_runner=runner)
        first = self.run_exact_cs408_force(worker)
        self.assertEqual(first["processed"][0]["status"], "two_pass_ready")
        second = worker.run_once(subject="cs408", capture_id=self.cs_capture)
        self.assertEqual(second["selected_count"], 0)
        self.assertEqual(runner.calls, 1)

        with (self.cs_repo / "节点总表.md").open("a", encoding="utf-8") as handle:
            handle.write(
                "| OS_2020_002 | 操作系统 | 文件管理 | OS04-03 文件空闲空间管理 |  | OS04-03 | 新证据 | 边界 | E03 | 2026-01-01 | 2026-07-02 | 新记录 |\n"
            )
        third = worker.run_once(subject="cs408", capture_id=self.cs_capture)
        self.assertEqual(third["selected_count"], 0)
        self.assertEqual(runner.calls, 1)

        fourth = self.run_exact_cs408_force(worker)
        self.assertEqual(fourth["selected_count"], 1)
        self.assertEqual(
            fourth["processed"][0]["status"], "two_pass_ready"
        )
        self.assertEqual(runner.calls, 2)
        self.assertEqual(
            worker.adapters["cs408"].snapshot_statuses[self.cs_capture],
            "current",
        )

    def test_legacy_trace_cli_schema_and_mq05_verbatim_are_consumed(self) -> None:
        config = self.make_v2_config()
        ref = self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ][0]
        evidence_root = Path(config["private_evidence"]["current_question_root"])
        binding_path = (
            evidence_root
            / "trace-supplements/bindings"
            / f"{hashlib.sha256(self.cs_capture.encode()).hexdigest()}.json"
        )
        binding_path.unlink()
        events = [
            {"role": "learner", "kind": "utterance", "text": "成组链接法有点模糊，不清楚是什么、有什么作用", "observed_at": None},
            {"role": "learner", "kind": "reasoning", "text": "应该先读取块 120 里面的下一组清单", "observed_at": None},
            {"role": "learner", "kind": "answer", "text": "C", "observed_at": None},
        ]
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "bin/publish_trace_supplement.py"),
                "--private-root", str(evidence_root),
                "--capture-id", self.cs_capture,
                "--context-id", "DETAIL-408-001",
                "--item-id", "ITEM-408-001",
                "--evidence-manifest-sha256", ref["sha256"],
                "--created-at", "2026-08-04T08:04:00+08:00",
                "--supplement-kind", "legacy_backfill",
                "--events-json", json.dumps(events, ensure_ascii=False),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["status"], "published")
        binding = json.loads(binding_path.read_text(encoding="utf-8"))
        self.assertEqual(set(binding) & {"locator", "object_sha"}, {"locator", "object_sha"})
        obj = json.loads(
            (evidence_root / "trace-supplements/objects" / f"{binding['object_sha']}.json").read_text(encoding="utf-8")
        )
        self.assertIsNone(obj["resolution_receipt_sha256"])
        self.assertEqual(
            obj["interaction_trace_sha256"],
            hashlib.sha256(
                json.dumps(obj["events"], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
            ).hexdigest(),
        )
        candidate = Worker(config, model_runner=FakeV2Runner()).adapters[
            "cs408"
        ].candidates(self.cs_status)[0]
        trace_text = "\n".join(
            row["text"]
            for row in candidate.model_input["study_observation"]["interaction_trace"]
        )
        self.assertIn("块 120", trace_text)
        self.assertIn("成组链接法", trace_text)

    def test_independent_correct_observation_routes_without_model_call(self) -> None:
        config = self.make_v2_config()
        evidence_root = Path(config["private_evidence"]["current_question_root"])
        old_ref = self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ][0]
        old_manifest = json.loads(
            (evidence_root / "manifests" / f"{old_ref['sha256']}.json").read_text(encoding="utf-8")
        )
        old_bundle = json.loads(
            (evidence_root / "objects" / f"{old_manifest['object_sha256']}.json").read_text(encoding="utf-8")
        )
        bundle = copy.deepcopy(old_bundle)
        bundle["schema_version"] = "current-question-evidence-bundle-v2"
        bundle["assessment"]["first_result"] = "independent_correct"
        bundle["evaluation_evidence"]["grader_result"] = "independent_correct"
        bundle["interaction_trace"] = {
            "schema": "current-question-interaction-trace-v1",
            "events": [
                {"ordinal": 1, "role": "learner", "kind": "answer", "text": "C"}
            ],
            "event_count": 1,
            "truncated": False,
        }
        bundle_bytes = json_file_bytes(bundle)
        bundle_sha = sha(bundle_bytes)
        atomic_write_json(evidence_root / "objects" / f"{bundle_sha}.json", bundle)
        manifest = copy.deepcopy(old_manifest)
        manifest["bundle_schema_version"] = bundle["schema_version"]
        manifest["object_sha256"] = bundle_sha
        manifest["object_size_bytes"] = len(bundle_bytes)
        manifest_sha = sha(json_file_bytes(manifest))
        atomic_write_json(evidence_root / "manifests" / f"{manifest_sha}.json", manifest)
        observation = {
            "schema_version": "current-question-study-observation-v1",
            "observation_id": "OBS-408-001",
            "context_id": bundle["context_id"],
            "request_id": bundle["request_id"],
            "session_id": bundle["session_id"],
            "item_id": bundle["item_id"],
            "source_id": bundle["source_id"],
            "study_date": bundle["study_date"],
            "event_time": bundle["event_time"],
            "first_result": "independent_correct",
            "evidence_locator": f"current-question-evidence://sha256/{manifest_sha}",
            "evidence_manifest_sha256": manifest_sha,
            "first_answer_receipt_sha256": "d" * 64,
            "processing_status": "awaiting_background_analysis",
            "formal_write_count": 0,
        }
        observations = evidence_root / "observations"
        observations.mkdir(mode=0o700)
        observation_sha = sha(json_file_bytes(observation))
        atomic_write_json(observations / f"{observation_sha}.json", observation)
        status = copy.deepcopy(self.cs_status)
        status["captures"] = {}
        status["pending_capture_ids"] = []
        atomic_write_json(self.cs_status_path, status)

        worker = Worker(config, model_runner=FakeV2Runner())
        observation_candidate = worker.adapters["cs408"].candidates(status)[0]
        self.assertEqual(
            observation_candidate.model_input["candidate_kind"],
            "study_observation",
        )
        self.assertEqual(
            validate_v2_candidate_route(observation_candidate),
            "study_observation",
        )
        self.assertEqual(observation_candidate.capture_id, "OBS-408-001")
        self.assertEqual(
            observation_candidate.canonical_state,
            "awaiting_background_analysis",
        )
        self.assertEqual(observation_candidate.sol_state, "not_applicable")

    def test_observation_scan_does_not_hide_tasks_after_first_two_hundred(
        self,
    ) -> None:
        config = self.make_v2_config()
        evidence_root = Path(
            config["private_evidence"]["current_question_root"]
        )
        observations = evidence_root / "observations"
        observations.mkdir(parents=True, exist_ok=True, mode=0o700)
        for index in range(205):
            receipt = {
                "schema_version": (
                    "current-question-study-observation-v1"
                ),
                "observation_id": f"OBS-408-{index:04d}",
                "context_id": f"CTX-{index:04d}",
                "request_id": f"REQ-{index:04d}",
                "session_id": f"SESSION-{index:04d}",
                "item_id": f"ITEM-{index:04d}",
                "source_id": f"SOURCE-{index:04d}",
                "study_date": "2026-08-04",
                "event_time": "2026-08-04T08:00:00+08:00",
                "first_result": "independent_correct",
                "evidence_locator": (
                    "current-question-evidence://sha256/" + "a" * 64
                ),
                "evidence_manifest_sha256": "a" * 64,
                "first_answer_receipt_sha256": "b" * 64,
                "processing_status": "awaiting_background_analysis",
                "formal_write_count": 0,
            }
            digest = sha(json_file_bytes(receipt))
            atomic_write_json(observations / f"{digest}.json", receipt)

        worker = Worker(config, model_runner=FakeV2Runner())
        adapter = worker.adapters["cs408"]
        bundle = {
            "schema_version": "current-question-evidence-bundle-v3",
            "interaction_trace": {
                "schema": "current-question-interaction-trace-v2",
                "events": [],
            },
        }
        evidence_binding = {
            "evidence_manifest_sha256": "a" * 64,
            "evidence_bundle_sha256": "c" * 64,
            "evidence_status": "ready",
        }

        def observation_value(*_args, **_kwargs):
            return {
                "trace_source": "bundle_interaction_trace",
                "observation_payload_sha256": "d" * 64,
            }

        with (
            mock.patch.object(
                adapter,
                "_current_question_bundle",
                return_value=(bundle, evidence_binding, ()),
            ),
            mock.patch.object(adapter, "_record_evidence_readiness"),
            mock.patch.object(
                adapter,
                "_pinned_knowledge_snapshot",
                return_value=({}, {}),
            ),
            mock.patch.object(
                core,
                "build_study_observation",
                side_effect=observation_value,
            ),
        ):
            candidates = adapter._observation_candidates(
                {"study_date": "2026-08-04"}
            )
        self.assertEqual(len(candidates), 205)
        self.assertEqual(
            {candidate.capture_id for candidate in candidates},
            {f"OBS-408-{index:04d}" for index in range(205)},
        )

    def legacy_contract_legacy_snapshot_binding_remains_read_only_and_v2_is_rebuilt(
        self,
    ) -> None:
        config = self.make_v2_config()
        evidence_root = Path(
            config["private_evidence"]["current_question_root"]
        )
        manifest_sha256 = self.cs_status["captures"][self.cs_capture][
            "capture"
        ]["stable_evidence_refs"][0]["sha256"]
        legacy_path = (
            evidence_root
            / "knowledge-snapshots"
            / "bindings"
            / f"{manifest_sha256}.json"
        )
        atomic_write_json(
            legacy_path,
            {
                "schema_version": (
                    "study-intake-408-knowledge-snapshot-binding-v1"
                ),
                "historical_fixture": True,
                "formal_write_count": 0,
            },
        )
        legacy_before = legacy_path.read_bytes()
        adapter = Worker(config, model_runner=FakeV2Runner()).adapters[
            "cs408"
        ]
        candidates = adapter.candidates(self.cs_status)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(legacy_path.read_bytes(), legacy_before)
        v2_paths = list(
            (
                evidence_root
                / "knowledge-snapshots"
                / "bindings-v2"
            ).glob("*.json")
        )
        self.assertEqual(len(v2_paths), 1)
        self.assertRegex(v2_paths[0].stem, r"^[0-9a-f]{64}$")

    def test_mq05_snapshot_prioritizes_exact_method_and_bounded_timeline(self) -> None:
        config = self.make_v2_config()
        (self.cs_repo / "知识点标签表.md").write_text(
            "## OS04 文件管理\n"
            "- OS04-04 inode\n"
            "- OS04-25 外存空闲空间管理\n"
            "- OS04-28 成组链接法\n"
            "## CN02 数据链路层\n"
            "- CN02-02 数据帧\n",
            encoding="utf-8",
        )
        (self.cs_repo / "节点总表.md").write_text(
            "| ID | 科目 | 主模块 | 主知识点 | 副知识点 | 命中知识点 | 核心考点 | 模糊概念 | 错因标签 | 首次做题日期 | 最近复做日期 | 最近错误记录 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| OS_OPEN_001 | 操作系统 | 文件管理 | OS04-04 inode |  | OS04-04 | open | inode | E01 | 未记录 | 2026-07-20 | open 噪声 |\n"
            "| OS_UNK_074 | 操作系统 | 文件管理 | OS04-25 外存空闲空间管理 |  | OS04-25 | 全局空闲空间 | 管理范围 | E01 | 未记录 | 2026-07-12 | 层级混淆 |\n"
            "| OS_UNK_075 | 操作系统 | 文件管理 | OS04-28 成组链接法 | OS04-25 外存空闲空间管理 | OS04-28；OS04-25 | 空闲块号栈 | 组间边界 | E01 | 未记录 | 2026-07-25 | 最后一组边界错误 |\n"
            "| CN_NOISE_001 | 计算机网络 | 数据链路层 | CN02-02 数据帧 |  | CN02-02 | 数据 | 帧 | E01 | 未记录 | 2026-07-20 | 数据噪声 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "关系边表.md").write_text(
            "| 起点ID | 终点ID | 关系类型 | 联系强度 | 关联原因 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| OS_UNK_074 | OS_UNK_075 | R06 | 强 | 上下游 |\n"
            "| OS_OPEN_001 | CN_NOISE_001 | R01 | 弱 | 噪声 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "错题复做记录.md").write_text(
            "| 日期 | ID | 来源ID | 动作 | 错误记录 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 2026-07-12 | OS_UNK_075 | HOS_0093 | 复做再次错误 | 未建立分配弹栈机制 |\n"
            "| 2026-07-20 | OS_OPEN_001 | X | 复做再次错误 | open 噪声 |\n"
            "| 2026-07-25 | OS_UNK_075 | HOS_0093 | 复做再次错误 | 未先读取下一组清单 |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "复习单元节点映射.md").write_text(
            "| 主复习单元ID | 次复习单元ID | 正式节点ID |\n"
            "| --- | --- | --- |\n"
            "| RU_OS04_FREE_SPACE |  | OS_UNK_074 |\n"
            "| RU_OS04_FREE_SPACE |  | OS_UNK_075 |\n",
            encoding="utf-8",
        )
        bundle = {
            "source_id": "RU_OS04_FREE_SPACE",
            "current_question": {
                "public_text": "关于成组链接法管理外存空闲空间，哪一项描述成立？",
                "options": [
                    {"label": "A", "text": "每个文件的 inode 保存数据块"},
                    {"label": "C", "text": "空闲块号栈和组间链接"},
                ],
                "response_instruction": "回复选项即可",
                "public_surface_sha256": "f" * 64,
            },
        }
        snapshot = build_408_knowledge_snapshot(
            self.cs_repo, config["cs408_knowledge_snapshot"], bundle
        )
        taxonomy_ids = [row["knowledge_id"] for row in snapshot["taxonomy_candidates"]]
        self.assertEqual(taxonomy_ids[:2], ["OS04-28", "OS04-25"])
        self.assertNotIn("CN02-02", taxonomy_ids)
        self.assertEqual(
            snapshot["historical_wrong_candidates"][0]["formal_id"],
            "OS_UNK_075",
        )
        self.assertEqual(
            snapshot["source_identity"]["primary_formal_node_ids"],
            ["OS_UNK_075"],
        )
        self.assertEqual(
            snapshot["source_identity"]["secondary_formal_node_ids"],
            ["OS_UNK_074"],
        )
        self.assertEqual(
            snapshot["source_identity"]["primary_knowledge_ids"],
            ["OS04-28"],
        )
        self.assertIn(
            "OS04-25",
            snapshot["source_identity"]["secondary_knowledge_ids"],
        )
        timeline = snapshot["historical_review_timeline"]
        self.assertEqual(
            [(row["review_date"], row["formal_id"]) for row in timeline],
            [("2026-07-12", "OS_UNK_075"), ("2026-07-25", "OS_UNK_075")],
        )
        self.assertTrue(
            all(
                "OS_UNK_075" in {row["start_id"], row["end_id"]}
                for row in snapshot["relationship_candidates"]
            )
        )
        for row in snapshot["relationship_candidates"]:
            self.assertEqual(row["source_edge_status"], "existing_formal_edge")
            self.assertEqual(
                row["current_capture_action_status"], "proposal_only"
            )
            self.assertNotIn("candidate_only", row)
        mq05_refs = semantic_evidence_refs_v2(
            {
                "capture": {"source_facts": {"source_id": "RU_OS04_FREE_SPACE"}},
                "current_question_evidence": {
                    "current_question": bundle["current_question"],
                    "assessment": {"first_result": "fragile_correct"},
                },
                "interaction_trace_evidence": {
                    "events": [
                        {"text": "成组链接法有点模糊"},
                        {"text": "应该先读取块 120 里面的下一组清单"},
                        {"text": "C"},
                    ]
                },
                "study_observation": {
                    "result_observation": {"first_result": "fragile_correct"},
                    "interaction_trace": [{"text": "C"}],
                },
                "knowledge_snapshot": snapshot,
            }
        )
        self.assertLess(len(mq05_refs), 200)
        self.assertLess(sum(map(len, mq05_refs)), 15_000)
        for required_anchor in (
            "interaction_trace_evidence.events[2]",
            "knowledge_snapshot.taxonomy_candidates[0]",
            "knowledge_snapshot.historical_wrong_candidates[0]",
            "knowledge_snapshot.historical_review_timeline[0]",
            "knowledge_snapshot.relationship_candidates[0]",
        ):
            self.assertIn(required_anchor, mq05_refs)

    def test_controlled_alias_matches_public_surface_term(self) -> None:
        config = self.make_v2_config()
        (self.cs_repo / "知识点标签表.md").write_text(
            "## DS02 线性表\n"
            "- DS02-02 顺序表\n"
            "## DS07 查找\n"
            "- DS07-04 折半查找\n",
            encoding="utf-8",
        )
        (self.cs_repo / "节点总表.md").write_text(
            "| ID | 科目 | 主模块 | 主知识点 | 副知识点 | 命中知识点 | 核心考点 | 模糊概念 | 错因标签 | 首次做题日期 | 最近复做日期 | 最近错误记录 |\n"
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
            "| DS_2023_002 | 数据结构 | 线性表 | DS02-02 顺序表 |  | DS02-02 | 插入 |  | E01 | 2026-01-01 | 2026-07-01 |  |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "关系边表.md").write_text(
            "| 起点ID | 终点ID | 关系类型 | 联系强度 | 关联原因 |\n"
            "| --- | --- | --- | --- | --- |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "错题复做记录.md").write_text(
            "| 日期 | ID | 来源ID | 动作 | 错误记录 |\n"
            "| --- | --- | --- | --- | --- |\n",
            encoding="utf-8",
        )
        (self.cs_repo / "复习单元节点映射.md").write_text(
            "| 主复习单元ID | 次复习单元ID | 正式节点ID |\n"
            "| --- | --- | --- |\n",
            encoding="utf-8",
        )
        config["cs408_knowledge_snapshot"]["controlled_aliases"] = {
            "二分查找": ["DS07-04"]
        }
        snapshot = build_408_knowledge_snapshot(
            self.cs_repo,
            config["cs408_knowledge_snapshot"],
            {
                "source_id": "DS_2023_002",
                "current_question": {
                    "public_text": "有序顺序表先用二分查找定位插入位置。",
                    "options": [],
                    "response_instruction": "回复选项即可",
                    "public_surface_sha256": "f" * 64,
                },
            },
        )
        by_id = {
            row["knowledge_id"]: row
            for row in snapshot["taxonomy_candidates"]
        }
        self.assertEqual(by_id["DS07-04"]["match_status"], "alias")
        self.assertEqual(
            by_id["DS07-04"]["match_basis"], "controlled_alias"
        )

    def test_primary_knowledge_consistency_accepts_canonical_name_without_id(self) -> None:
        snapshot = {
            "source_identity": {
                "resolved_formal_node_ids": ["DS_2023_002"],
            },
            "historical_wrong_candidates": [
                {
                    "formal_id": "DS_2023_002",
                    "main_knowledge_ids": ["DS02-02"],
                }
            ],
            "taxonomy_candidates": [
                {
                    "knowledge_id": "DS02-02",
                    "knowledge_name": "顺序表",
                }
            ],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        signal = {
            "signal_id": "SIG-DS-PRIMARY",
            "signal_type": "knowledge",
            "importance": "primary",
        }
        matrix = [
            {
                "signal_id": "SIG-DS-PRIMARY",
                "match_status": "exact",
                "matched_node_id": "DS02-02",
            }
        ]
        candidate = {
            "candidate_schema_version": "study-intake-candidate-v3",
            "atomic_signals": [signal],
            "current_error_points": [],
            "historical_source_error_points": [],
            "truth_match_matrix": matrix,
            "existing_knowledge_matches": copy.deepcopy(matrix),
            "novel_knowledge_candidates": [],
            "novel_error_candidates": [],
            "unmatched_signals": [],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "new_relation_proposals": [],
            "coverage_manifest": {"complete": True},
            "formalization_candidates": {
                "main_knowledge": [
                    {"text": "主知识候选为顺序表及其有序插入。"}
                ]
            },
        }
        core.validate_cs408_candidate_v3(candidate, snapshot=snapshot)

        candidate["atomic_signals"][0]["importance"] = "secondary"
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_primary_knowledge_consistency_failed"
        ):
            core.validate_cs408_candidate_v3(candidate, snapshot=snapshot)

    def test_candidate_v3_matches_stable_id_embedded_in_canonical_label(self) -> None:
        taxonomy_ref = "knowledge_snapshot.taxonomy_candidates[0]"
        snapshot = {
            "source_identity": {
                "source_id": "DS_2023_002",
                "resolved_formal_node_ids": ["DS_2023_002"],
                "primary_knowledge_ids": ["DS02-02"],
            },
            "historical_wrong_candidates": [],
            "taxonomy_candidates": [
                {
                    "knowledge_id": "DS02-02",
                    "knowledge_name": "顺序表",
                }
            ],
            "controlled_aliases": {},
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        analysis = {
            "atomic_signals": [
                {
                    "signal_id": "SIG-DS-PRIMARY",
                    "signal_type": "knowledge",
                    "surface_form": "有序顺序表",
                    "canonical_term": "DS02-02 顺序表",
                    "importance": "primary",
                    "specificity": "主知识点",
                    "applicability_boundary": "顺序表操作",
                    "provenance": "冻结知识快照",
                    "evidence_refs": [taxonomy_ref],
                    "confidence": "high",
                    "error_role": "none",
                    "truth_library_match_status": "exact",
                }
            ],
            "formalization_candidates": {
                "main_knowledge": [{"text": "主知识点候选为 DS02-02 顺序表。"}]
            },
        }

        candidate = core.enrich_cs408_candidate_v3(
            analysis,
            snapshot=snapshot,
            allowed_evidence_refs=[taxonomy_ref],
        )

        self.assertEqual(
            candidate["truth_match_matrix"],
            [
                {
                    "signal_id": "SIG-DS-PRIMARY",
                    "match_status": "exact",
                    "matched_node_id": "DS02-02",
                    "matched_node_name": "顺序表",
                    "match_method": "stable_id_or_canonical_term",
                    "supporting_evidence_refs": [taxonomy_ref, taxonomy_ref],
                    "counterevidence": [],
                }
            ],
        )
        self.assertEqual(candidate["novel_knowledge_candidates"], [])
        core.validate_cs408_candidate_v3(candidate, snapshot=snapshot)

    def test_primary_knowledge_consistency_uses_ranked_review_unit_identity(self) -> None:
        snapshot = {
            "source_identity": {
                "resolved_formal_node_ids": ["OS_UNK_074", "OS_UNK_075"],
                "primary_formal_node_ids": ["OS_UNK_075"],
                "secondary_formal_node_ids": ["OS_UNK_074"],
                "primary_knowledge_ids": ["OS04-28"],
                "secondary_knowledge_ids": ["OS04-25"],
            },
            "historical_wrong_candidates": [
                {
                    "formal_id": "OS_UNK_075",
                    "main_knowledge_ids": ["OS04-28"],
                },
                {
                    "formal_id": "OS_UNK_074",
                    "main_knowledge_ids": ["OS04-25"],
                },
            ],
            "taxonomy_candidates": [
                {"knowledge_id": "OS04-28", "knowledge_name": "成组链接法"},
                {
                    "knowledge_id": "OS04-25",
                    "knowledge_name": "外存空闲空间管理",
                },
            ],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        signal = {
            "signal_id": "SIG-OS04-28",
            "signal_type": "knowledge",
            "importance": "primary",
        }
        matrix = [
            {
                "signal_id": "SIG-OS04-28",
                "match_status": "exact",
                "matched_node_id": "OS04-28",
            }
        ]
        candidate = {
            "candidate_schema_version": "study-intake-candidate-v3",
            "atomic_signals": [signal],
            "current_error_points": [],
            "historical_source_error_points": [],
            "truth_match_matrix": matrix,
            "existing_knowledge_matches": copy.deepcopy(matrix),
            "novel_knowledge_candidates": [],
            "novel_error_candidates": [],
            "unmatched_signals": [],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "new_relation_proposals": [],
            "coverage_manifest": {"complete": True},
            "formalization_candidates": {
                "main_knowledge": [
                    {"text": "主知识候选为 OS04-28 成组链接法。"}
                ]
            },
        }
        core.validate_cs408_candidate_v3(candidate, snapshot=snapshot)
        self.assertEqual(
            core.cs408_primary_knowledge_semantic_findings(
                candidate, snapshot=snapshot
            ),
            [],
        )

    def test_primary_knowledge_consistency_rejects_unrelated_main_name(self) -> None:
        snapshot = {
            "source_identity": {
                "resolved_formal_node_ids": ["OS_2009_003"],
            },
            "historical_wrong_candidates": [
                {
                    "formal_id": "OS_2009_003",
                    "main_knowledge_ids": ["OS04-15"],
                }
            ],
            "taxonomy_candidates": [
                {
                    "knowledge_id": "OS04-15",
                    "knowledge_name": "索引分配",
                }
            ],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        matrix = [
            {
                "signal_id": "SIG-OS-PRIMARY",
                "match_status": "exact",
                "matched_node_id": "OS04-15",
            }
        ]
        candidate = {
            "candidate_schema_version": "study-intake-candidate-v3",
            "atomic_signals": [
                {
                    "signal_id": "SIG-OS-PRIMARY",
                    "signal_type": "knowledge",
                    "importance": "primary",
                }
            ],
            "current_error_points": [],
            "historical_source_error_points": [],
            "truth_match_matrix": matrix,
            "existing_knowledge_matches": copy.deepcopy(matrix),
            "novel_knowledge_candidates": [],
            "novel_error_candidates": [],
            "unmatched_signals": [],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "new_relation_proposals": [],
            "coverage_manifest": {"complete": True},
            "formalization_candidates": {
                "main_knowledge": [{"text": "主知识候选为文件物理结构。"}]
            },
        }
        with self.assertRaisesRegex(
            PreprocessorError, "cs408_primary_knowledge_consistency_failed"
        ):
            core.validate_cs408_candidate_v3(candidate, snapshot=snapshot)

        findings = core.cs408_primary_knowledge_semantic_findings(
            candidate, snapshot=snapshot
        )
        self.assertEqual(
            [row["finding_id"] for row in findings],
            ["HOST-PRIMARY-FORMALIZATION"],
        )
        core.validate_cs408_candidate_v3(
            candidate,
            snapshot=snapshot,
            require_primary_knowledge_consistency=False,
        )

    def test_critical_review_must_apply_deferred_primary_knowledge_fix(self) -> None:
        current_ref = "current_question_evidence.current_question"
        taxonomy_ref = "knowledge_snapshot.taxonomy_candidates[0]"
        history_ref = "knowledge_snapshot.historical_wrong_candidates[0]"
        allowed_refs = [current_ref, taxonomy_ref, history_ref]
        snapshot = {
            "source_identity": {
                "resolved_formal_node_ids": ["OS_2009_003"],
            },
            "historical_wrong_candidates": [
                {
                    "formal_id": "OS_2009_003",
                    "main_knowledge_ids": ["OS04-15"],
                }
            ],
            "taxonomy_candidates": [
                {
                    "knowledge_id": "OS04-15",
                    "knowledge_name": "索引分配",
                }
            ],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        semantic_draft = v2_analysis(current_ref)
        semantic_draft["atomic_signals"][0].update(
            {
                "signal_id": "SIG-OS04-15",
                "canonical_term": "OS04-15",
                "surface_form": "索引分配",
            }
        )
        semantic_draft["formalization_candidates"]["main_knowledge"][0][
            "text"
        ] = "主知识候选为文件物理结构。"
        draft = core.enrich_cs408_candidate_v3(
            semantic_draft,
            snapshot=snapshot,
            allowed_evidence_refs=allowed_refs,
        )
        core.validate_cs408_candidate_v3(
            draft,
            snapshot=snapshot,
            require_primary_knowledge_consistency=False,
        )
        semantic_revised = copy.deepcopy(semantic_draft)
        semantic_revised["formalization_candidates"]["main_knowledge"][0][
            "text"
        ] = "主知识候选为 OS04-15 索引分配。"
        host_finding = {
            "finding_id": "HOST-PRIMARY-FORMALIZATION",
            "severity": "error",
            "text": "主知识字段未落实冻结身份解析出的 OS04-15 索引分配。",
            "analysis_refs": [
                "analysis.formalization_candidates.main_knowledge"
            ],
            "evidence_refs": [history_ref, taxonomy_ref],
            "affected_json_paths": [
                "$.formalization_candidates.main_knowledge"
            ],
        }
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "已把冻结身份解析出的主知识落实到最终语义字段。",
            "revised_analysis": semantic_revised,
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        with self.assertRaisesRegex(
            PreprocessorError,
            "critical_review_host_semantic_finding_missing",
        ):
            core.validate_critical_review_v2(
                copy.deepcopy(critical),
                allowed_evidence_refs=allowed_refs,
                draft_analysis=draft,
                require_network_context=True,
                knowledge_snapshot=snapshot,
            )

        critical["correction_resolutions"] = [
            {
                "finding_id": host_finding["finding_id"],
                "resolution": "applied",
                "affected_json_paths": host_finding[
                    "affected_json_paths"
                ],
                "before": encode_correction_delta(None),
                "after": encode_correction_delta(None),
                "reason": "已落实主机给出的精确主知识修正。",
            }
        ]
        materialized_host_finding = core.validate_critical_review_v2(
            copy.deepcopy(critical),
            allowed_evidence_refs=allowed_refs,
            draft_analysis=draft,
            require_network_context=True,
            knowledge_snapshot=snapshot,
        )
        self.assertEqual(
            [
                row["finding_id"]
                for row in materialized_host_finding[
                    "required_corrections"
                ]
            ],
            [host_finding["finding_id"]],
        )

        critical["required_corrections"] = [host_finding]
        critical["correction_resolutions"] = [
            {
                "finding_id": host_finding["finding_id"],
                "resolution": "applied",
                "affected_json_paths": host_finding[
                    "affected_json_paths"
                ],
                "before": encode_correction_delta(None),
                "after": encode_correction_delta(None),
                "reason": "revised_analysis 已明确写入 OS04-15 索引分配。",
            }
        ]
        validated = core.validate_critical_review_v2(
            critical,
            allowed_evidence_refs=allowed_refs,
            draft_analysis=draft,
            require_network_context=True,
            knowledge_snapshot=snapshot,
        )
        self.assertIn(
            "OS04-15",
            validated["revised_analysis"]["formalization_candidates"][
                "main_knowledge"
            ][0]["text"],
        )
        core.validate_cs408_candidate_v3(
            validated["revised_analysis"], snapshot=snapshot
        )

    def test_critical_review_binds_frozen_primary_identity_after_safe_generalization(self) -> None:
        current_ref = "current_question_evidence.current_question"
        source_identity_ref = "knowledge_snapshot.source_identity"
        taxonomy_ref = "knowledge_snapshot.taxonomy_candidates[0]"
        history_ref = "knowledge_snapshot.historical_wrong_candidates[0]"
        allowed_refs = [
            current_ref,
            source_identity_ref,
            taxonomy_ref,
            history_ref,
        ]
        snapshot = {
            "source_identity": {
                "resolved_formal_node_ids": ["OS_2009_003"],
                "primary_knowledge_ids": ["OS04-15"],
            },
            "historical_wrong_candidates": [
                {
                    "formal_id": "OS_2009_003",
                    "main_knowledge_ids": ["OS04-15"],
                }
            ],
            "taxonomy_candidates": [
                {
                    "knowledge_id": "OS04-15",
                    "knowledge_name": "索引分配",
                }
            ],
            "related_formal_questions": [],
            "existing_formal_edges": [],
            "coverage_manifest": {"complete": True},
        }
        semantic_draft = v2_analysis(current_ref)
        semantic_draft["atomic_signals"][0].update(
            {
                "signal_id": "SIG-OS04-15",
                "canonical_term": "OS04-15",
                "surface_form": "索引分配",
            }
        )
        semantic_draft["formalization_candidates"]["main_knowledge"][0].update(
            {
                "text": "主知识候选为文件物理结构。",
                "evidence_refs": [current_ref],
            }
        )
        draft = core.enrich_cs408_candidate_v3(
            semantic_draft,
            snapshot=snapshot,
            allowed_evidence_refs=allowed_refs,
        )
        semantic_revised = copy.deepcopy(semantic_draft)
        semantic_revised["formalization_candidates"]["main_knowledge"][0][
            "text"
        ] = "主知识候选：文件物理组织中的位置映射机制。"
        host_finding = {
            "finding_id": "HOST-PRIMARY-FORMALIZATION",
            "severity": "error",
            "text": "主知识字段必须遵循冻结身份解析。",
            "analysis_refs": [
                "analysis.formalization_candidates.main_knowledge"
            ],
            "evidence_refs": [history_ref, taxonomy_ref],
            "affected_json_paths": [
                "$.formalization_candidates.main_knowledge"
            ],
        }
        critical = {
            "schema_version": "study-intake-luna-critical-review-v2",
            "verdict": "pass_with_warnings",
            "summary": "模型保留机制描述，主机绑定冻结身份。",
            "revised_analysis": semantic_revised,
            "unsupported_claims": [],
            "evidence_misreads": [],
            "answer_safety_findings": [],
            "missing_analysis": [],
            "required_corrections": [host_finding],
            "sol_priority_checks": [],
            "correction_resolutions": [
                {
                    "finding_id": host_finding["finding_id"],
                    "resolution": "applied",
                    "affected_json_paths": host_finding[
                        "affected_json_paths"
                    ],
                    "before": encode_correction_delta(None),
                    "after": encode_correction_delta(None),
                    "reason": "已改为答案安全的机制描述。",
                }
            ],
        }
        validated = core.validate_critical_review_v2(
            critical,
            allowed_evidence_refs=allowed_refs,
            draft_analysis=draft,
            require_network_context=True,
            knowledge_snapshot=snapshot,
        )
        main_row = validated["revised_analysis"]["formalization_candidates"][
            "main_knowledge"
        ][0]
        self.assertIn("OS04-15", main_row["text"])
        self.assertIn("索引分配", main_row["text"])
        self.assertIn("文件物理组织中的位置映射机制", main_row["text"])
        self.assertIn(taxonomy_ref, main_row["evidence_refs"])
        core.validate_cs408_candidate_v3(
            validated["revised_analysis"], snapshot=snapshot
        )

    def test_v2_failure_capture_waits_for_background_handoff(self) -> None:
        config = self.make_v2_config()
        evidence_root = Path(config["private_evidence"]["current_question_root"])
        ref = self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ][0]
        manifest_path = evidence_root / "manifests" / f"{ref['sha256']}.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        bundle = json.loads(
            (evidence_root / "objects" / f"{manifest['object_sha256']}.json").read_text(encoding="utf-8")
        )
        bundle["schema_version"] = "current-question-evidence-bundle-v2"
        bundle["assessment"]["first_result"] = "wrong"
        bundle["interaction_trace"] = {
            "schema": "current-question-interaction-trace-v1",
            "events": [
                {"ordinal": 1, "role": "learner", "kind": "answer", "text": "A"},
                {"ordinal": 2, "role": "assistant", "kind": "correction", "text": "请继续纠正"},
            ],
            "event_count": 2,
            "truncated": False,
        }
        bundle_bytes = json_file_bytes(bundle)
        bundle_sha = sha(bundle_bytes)
        atomic_write_json(evidence_root / "objects" / f"{bundle_sha}.json", bundle)
        manifest["bundle_schema_version"] = bundle["schema_version"]
        manifest["object_sha256"] = bundle_sha
        manifest["object_size_bytes"] = len(bundle_bytes)
        manifest_sha = sha(json_file_bytes(manifest))
        atomic_write_json(evidence_root / "manifests" / f"{manifest_sha}.json", manifest)
        self.cs_status["captures"][self.cs_capture]["capture"]["stable_evidence_refs"] = [
            {
                "kind": "current_question_evidence_bundle_v1",
                "locator": f"current-question-evidence://sha256/{manifest_sha}",
                "sha256": manifest_sha,
            }
        ]
        trace_binding = (
            evidence_root
            / "trace-supplements/bindings"
            / f"{hashlib.sha256(self.cs_capture.encode()).hexdigest()}.json"
        )
        trace_binding.unlink()
        adapter = Worker(config, model_runner=FakeV2Runner()).adapters["cs408"]
        self.assertEqual(adapter.candidates(self.cs_status), [])
        self.assertEqual(
            adapter.candidate_errors[self.cs_capture],
            "awaiting_teaching_resolution",
        )

    def test_legacy_bundle_accepts_verified_resolved_trace_handoff(self) -> None:
        config = self.make_v2_config()
        evidence_root = Path(config["private_evidence"]["current_question_root"])
        ref = self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ][0]
        manifest_sha = ref["sha256"]
        manifest = json.loads(
            (evidence_root / "manifests" / f"{manifest_sha}.json").read_text(
                encoding="utf-8"
            )
        )
        bundle = json.loads(
            (
                evidence_root
                / "objects"
                / f"{manifest['object_sha256']}.json"
            ).read_text(encoding="utf-8")
        )
        bundle["assessment"]["first_result"] = "fragile_correct"
        bundle_bytes = json_file_bytes(bundle)
        bundle_sha = sha(bundle_bytes)
        atomic_write_json(evidence_root / "objects" / f"{bundle_sha}.json", bundle)
        manifest["object_sha256"] = bundle_sha
        manifest["object_size_bytes"] = len(bundle_bytes)
        manifest_sha = sha(json_file_bytes(manifest))
        atomic_write_json(
            evidence_root / "manifests" / f"{manifest_sha}.json", manifest
        )
        self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ] = [
            {
                "kind": "current_question_evidence_bundle_v1",
                "locator": f"current-question-evidence://sha256/{manifest_sha}",
                "sha256": manifest_sha,
            }
        ]
        trace_events = [
            {
                "role": "learner",
                "kind": "answer",
                "text": "C",
                "observed_at": None,
            }
        ]
        (
            evidence_root
            / "trace-supplements/bindings"
            / f"{hashlib.sha256(self.cs_capture.encode()).hexdigest()}.json"
        ).unlink()
        resolution_core = {
            "schema": "current-question-turn-receipt-v1",
            "status": "teaching_resolved",
            "context_id": bundle["context_id"],
            "session_id": bundle["session_id"],
            "item_id": bundle["item_id"],
            "capture_id": self.cs_capture,
            "capture_receipt_sha256": "e" * 64,
            "evidence_manifest_sha256": manifest_sha,
            "resolution_attestation_locator": (
                "current-question-turn://sha256/" + "3" * 64
            ),
            "resolution_attestation_sha256": "3" * 64,
            "trace_supplement": {},
            "session_resolution_receipt_sha256": "1" * 64,
            "mastery_effect": "none",
            "retention_effect": "none",
            "independent_repair": False,
            "feedback_sha256": "2" * 64,
            "advance_allowed": True,
            "formal_write_count": 0,
        }
        trace = publish_trace_supplement(
            private_root=evidence_root,
            capture_id=self.cs_capture,
            context_id=bundle["context_id"],
            item_id=bundle["item_id"],
            evidence_manifest_sha256=manifest_sha,
            created_at="2026-08-04T08:04:00+08:00",
            supplement_kind="resolved_trace",
            resolution_receipt_sha256="3" * 64,
            events=trace_events,
        )
        resolution_core["trace_supplement"] = {
            "schema_version": (
                "current-question-trace-supplement-binding-v1"
            ),
            "capture_id": self.cs_capture,
            "locator": trace["locator"],
            "object_sha": trace["object_sha"],
            "binding_sha256": trace["binding_sha256"],
            "interaction_trace_sha256": trace[
                "interaction_trace_sha256"
            ],
            "formal_write_count": 0,
        }
        resolution_sha = sha(json_file_bytes(resolution_core))
        atomic_write_json(
            evidence_root / "turns" / f"{resolution_sha}.json",
            resolution_core,
        )
        handoff = {
            "schema_version": "current-question-background-handoff-v1",
            "capture_id": self.cs_capture,
            "context_id": bundle["context_id"],
            "item_id": bundle["item_id"],
            "evidence_manifest_sha256": manifest_sha,
            "capture_receipt_sha256": "e" * 64,
            "status": "ready",
            "completion_kind": "teaching_resolved",
            "created_at": "2026-08-04T08:02:00+08:00",
            "updated_at": "2026-08-04T08:05:00+08:00",
            "trace_supplement_locator": trace["locator"],
            "trace_supplement_object_sha256": trace["object_sha"],
            "interaction_trace_sha256": trace["interaction_trace_sha256"],
            "resolution_receipt_sha256": resolution_sha,
            "formal_write_count": 0,
        }
        handoff_sha = sha(json_file_bytes(handoff))
        atomic_write_json(
            evidence_root
            / "background-handoffs/objects"
            / f"{handoff_sha}.json",
            handoff,
        )
        atomic_write_json(
            evidence_root
            / "background-handoffs/bindings"
            / f"{hashlib.sha256(self.cs_capture.encode()).hexdigest()}.json",
            {
                "schema_version": (
                    "current-question-background-handoff-binding-v1"
                ),
                "capture_id": self.cs_capture,
                "status": "ready",
                "object_sha256": handoff_sha,
                "locator": (
                    "current-question-background-handoff://sha256/"
                    f"{handoff_sha}"
                ),
                "updated_at": "2026-08-04T08:05:00+08:00",
                "formal_write_count": 0,
            },
        )
        for private_dir in (
            evidence_root / "background-handoffs",
            evidence_root / "background-handoffs/objects",
            evidence_root / "background-handoffs/bindings",
            evidence_root / "turns",
        ):
            private_dir.chmod(0o700)
        adapter = Worker(config, model_runner=FakeV2Runner()).adapters["cs408"]
        candidates = adapter.candidates(self.cs_status)
        self.assertEqual(
            [self.cs_capture],
            [row.capture_id for row in candidates],
            adapter.candidate_errors,
        )
        self.assertEqual(
            "resolved_content_addressed_supplement",
            candidates[0].input_binding["interaction_trace_source"],
        )
        compatibility = candidates[0].model_input[
            "legacy_current_question_compatibility"
        ]
        self.assertEqual(
            "study-intake-408-legacy-current-question-compatibility-v1",
            compatibility["schema_version"],
        )
        self.assertEqual(0, compatibility["formal_write_count"])
        self.assertEqual(
            compatibility["compatibility_receipt_sha256"],
            candidates[0].input_binding[
                "legacy_compatibility_receipt_sha256"
            ],
        )
        from core_dispatch_bridge import validate_concurrent_cs408_candidate

        validate_concurrent_cs408_candidate(candidates[0])

    def test_background_handoff_rejects_hash_valid_forged_turn_receipt(self) -> None:
        config = self.make_v2_config()
        evidence_root = Path(config["private_evidence"]["current_question_root"])
        ref = self.cs_status["captures"][self.cs_capture]["capture"][
            "stable_evidence_refs"
        ][0]
        manifest_sha = ref["sha256"]
        manifest = json.loads(
            (evidence_root / "manifests" / f"{manifest_sha}.json").read_text(
                encoding="utf-8"
            )
        )
        bundle = json.loads(
            (
                evidence_root
                / "objects"
                / f"{manifest['object_sha256']}.json"
            ).read_text(encoding="utf-8")
        )
        bundle["schema_version"] = "current-question-evidence-bundle-v2"
        bundle["assessment"]["first_result"] = "fragile_correct"
        bundle["interaction_trace"] = {
            "schema": "current-question-interaction-trace-v1",
            "events": [
                {
                    "ordinal": 1,
                    "role": "learner",
                    "kind": "answer",
                    "text": "C",
                }
            ],
            "event_count": 1,
            "truncated": False,
        }
        trace_events = [
            {
                "role": "learner",
                "kind": "answer",
                "text": "C",
                "observed_at": None,
            }
        ]
        trace_payload = json.dumps(
            trace_events,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        trace_sha = sha(trace_payload)
        capture_receipt_sha = "e" * 64
        forged_receipt = {
            "schema": "current-question-turn-receipt-v1",
            "status": "teaching_resolved",
            "completion_kind": "first_turn_complete",
            "context_id": bundle["context_id"],
            "session_id": bundle["session_id"],
            "item_id": bundle["item_id"],
            "capture_id": self.cs_capture,
            "capture_receipt_sha256": capture_receipt_sha,
            "evidence_manifest_sha256": manifest_sha,
            "interaction_trace_sha256": trace_sha,
            "event_time": "2026-08-04T08:03:00+08:00",
            "advance_allowed": False,
            "formal_write_count": 0,
        }
        resolution_sha = sha(json_file_bytes(forged_receipt))
        atomic_write_json(
            evidence_root / "turns" / f"{resolution_sha}.json",
            forged_receipt,
        )
        handoff = {
            "schema_version": "current-question-background-handoff-v1",
            "capture_id": self.cs_capture,
            "context_id": bundle["context_id"],
            "item_id": bundle["item_id"],
            "evidence_manifest_sha256": manifest_sha,
            "capture_receipt_sha256": capture_receipt_sha,
            "status": "ready",
            "completion_kind": "first_turn_complete",
            "created_at": "2026-08-04T08:02:00+08:00",
            "updated_at": "2026-08-04T08:03:00+08:00",
            "trace_supplement_locator": None,
            "trace_supplement_object_sha256": None,
            "interaction_trace_sha256": trace_sha,
            "resolution_receipt_sha256": resolution_sha,
            "formal_write_count": 0,
        }
        handoff_sha = sha(json_file_bytes(handoff))
        atomic_write_json(
            evidence_root
            / "background-handoffs/objects"
            / f"{handoff_sha}.json",
            handoff,
        )
        atomic_write_json(
            evidence_root
            / "background-handoffs/bindings"
            / f"{hashlib.sha256(self.cs_capture.encode()).hexdigest()}.json",
            {
                "schema_version": (
                    "current-question-background-handoff-binding-v1"
                ),
                "capture_id": self.cs_capture,
                "status": "ready",
                "object_sha256": handoff_sha,
                "locator": (
                    "current-question-background-handoff://sha256/"
                    f"{handoff_sha}"
                ),
                "updated_at": "2026-08-04T08:03:00+08:00",
                "formal_write_count": 0,
            },
        )
        for private_dir in (
            evidence_root / "background-handoffs",
            evidence_root / "background-handoffs/objects",
            evidence_root / "background-handoffs/bindings",
            evidence_root / "turns",
        ):
            private_dir.chmod(0o700)
        adapter = Worker(config, model_runner=FakeV2Runner()).adapters["cs408"]
        with self.assertRaisesRegex(
            PreprocessorError,
            "background_handoff_resolution_receipt_invalid",
        ):
            adapter._background_handoff(
                capture_id=self.cs_capture,
                bundle=bundle,
                evidence_binding={"evidence_manifest_sha256": manifest_sha},
                trace_binding={},
            )


class ControlledReplayDateBoundaryTests(unittest.TestCase):
    def test_exact_replay_scans_all_dates_after_local_midnight(self):
        worker = Worker.__new__(Worker)
        candidate = object()
        worker.scan_statuses = mock.Mock(return_value={"english": {}})
        worker.candidates = mock.Mock(return_value=[candidate])

        rows = Worker.eligible_candidates(
            worker,
            "english",
            "2026-08-07",
            capture_allowlist=frozenset({"EVT-20260806-EXACT"}),
            controlled_replay=True,
        )

        self.assertEqual(rows, [(candidate, "controlled_replay")])
        worker.scan_statuses.assert_called_once_with(
            None,
            only_subject="english",
        )
        worker.candidates.assert_called_once_with(
            {"english": {}},
            subject="english",
            capture_allowlist=frozenset({"EVT-20260806-EXACT"}),
            controlled_replay=True,
        )

    def test_normal_claim_scan_can_discover_delayed_intake_across_dates(self):
        worker = Worker.__new__(Worker)
        worker.worker_config = {
            "max_attempts": 3,
            "debounce_seconds": 0,
        }
        worker.scan_statuses = mock.Mock(return_value={"math": {}})
        worker.candidates = mock.Mock(return_value=[])

        rows = Worker.eligible_candidates(
            worker,
            "math",
            "2026-08-07",
            scan_all_study_dates=True,
        )

        self.assertEqual(rows, [])
        worker.scan_statuses.assert_called_once_with(
            None,
            only_subject="math",
        )


if __name__ == "__main__":
    unittest.main()
