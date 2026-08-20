from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import FrozenTask, LeaseStore, StageResult  # noqa: E402
from dashboard_projection import update_subject_and_main_projection  # noqa: E402
from preprocessor_core import (  # noqa: E402
    Candidate,
    CodexRunner,
    PreprocessorError,
    StructuredStageResult,
    atomic_write_json,
    english_review_semantic_draft,
    mcp_grounding_manifest,
    model_mcp_item_ref,
    processing_publication_fields,
    sha256_value,
)
from processing_plugin import ProcessingPluginError, ProcessingPluginHost  # noqa: E402


def _value_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _file_json_sha256(value: object) -> str:
    return hashlib.sha256(
        (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    ).hexdigest()


def _bind_stage_execution(
    stage: StructuredStageResult,
    processing_context: dict,
) -> StructuredStageResult:
    session = processing_context["mcp_read_session"]
    grounding = mcp_grounding_manifest((stage,))
    return replace(
        stage,
        authority_snapshot_manifest_sha256=session.get(
            "authority_snapshot_manifest_sha256"
        ),
        mcp_grounding_manifest_sha256=grounding["manifest_sha256"],
        capture_freeze_receipt_sha256=processing_context[
            "capture_freeze_receipt_sha256"
        ],
        mcp_read_session_receipt_sha256=processing_context[
            "mcp_read_session_receipt_sha256"
        ],
    )


class EnglishCaptureAuthorityE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="english-p0-authority-e2e-"
        )
        self.runtime = Path(self.temporary.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.authority_key = self.runtime / "dispatch/state/authority.key"
        self.authority_key.parent.mkdir(parents=True)
        self.authority_key.write_bytes(b"e" * 32)
        self.authority_key.chmod(0o600)
        plugin_overlay = (
            Path(self.temporary.name) / "plugin-overlay/kaoyan-study-intake"
        )
        shutil.copytree(
            ROOT / "plugin/kaoyan-study-intake", plugin_overlay
        )
        lock_path = plugin_overlay / "component-lock.json"
        component_lock = json.loads(lock_path.read_text(encoding="utf-8"))
        english_skill = (
            plugin_overlay
            / "skills/background-english-processing/SKILL.md"
        )
        component_lock["skills"]["background-english-processing"] = {
            "sha256": hashlib.sha256(english_skill.read_bytes()).hexdigest(),
            "version": "3.1.1",
        }
        lock_path.chmod(0o600)
        lock_path.write_text(
            json.dumps(
                component_lock,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        lock_path.chmod(0o400)
        self.release_id = "a" * 64
        self.mcp_release_id = str(component_lock["mcp_release_id"])
        self.mcp_server_release = str(component_lock["mcp_server_release"])
        mcp_source_root = Path(
            "/Users/xiazhibin/Documents/Codex/local-study-read-mcp"
        )
        host_config = {
            "enabled": True,
            "root": str(plugin_overlay),
            "component_lock_path": str(lock_path),
            "mcp_client_python": str(mcp_source_root / ".venv/bin/python"),
            "mcp_project_root": str(
                Path("/Users/xiazhibin/.codex/local-study-read-mcp/releases")
                / self.mcp_release_id
            ),
            "authority_key_path": str(self.authority_key),
            "profile": "background",
            "timeout_seconds": 5,
        }
        self.host = ProcessingPluginHost(
            host_config,
            runtime_root=self.runtime,
            candidate_release_id=self.release_id,
        )
        self.dashboard_config = {
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "active_release_id": self.release_id,
            "dashboard": {
                "projection_path": str(
                    self.runtime / "state/dashboard_projection.json"
                )
            },
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _authority_response(
        self, subject: str, tool: str, arguments: dict
    ) -> dict:
        self.assertEqual(subject, "english")
        self.assertEqual(tool, "authority_bundle")
        self.assertEqual(arguments["subjects"], ["english"])
        return {
            "ok": True,
            "schema_version": "study-read-mcp.v3",
            "profile": "background",
            "read_route": arguments["route"],
            "generation": "english-generation-fixture",
            "authority_fingerprint": "b" * 64,
            "server_release": self.mcp_server_release,
            "preprocessor_release": "c" * 64,
            "formal_write_count": 0,
            "model_call_count": 0,
            "items": [
                {
                    "subject": "english",
                    "available": True,
                    "generation": "english-generation-fixture",
                    "authority_fingerprint": "b" * 64,
                    "adapter_release": self.mcp_server_release,
                }
            ],
        }

    def _task_bound_context(self, suffix: str) -> dict:
        facts = {
            "schema_version": "english_p0_zero_model_fixture_v1",
            "capture_suffix": suffix,
            "article_id": "RAW-ARTICLE-P0-E2E",
            "formal_write_count": 0,
        }
        with mock.patch.object(
            self.host, "_call", side_effect=self._authority_response
        ):
            context = self.host.open_read_session(
                subject="english",
                capture_id="EN-P0-E2E-CAPTURE",
                study_date="2026-08-09",
                input_fingerprint=hashlib.sha256(
                    f"fingerprint:{suffix}".encode("utf-8")
                ).hexdigest(),
                input_binding={
                    "article_id": "RAW-ARTICLE-P0-E2E",
                    "capture_suffix": suffix,
                },
                capture_facts_sha256=_file_json_sha256(facts),
                capture_facts=facts,
                capture_scene="intensive_reading",
                capture_identity={
                    "article_id": "RAW-ARTICLE-P0-E2E",
                    "content_fingerprint": hashlib.sha256(
                        suffix.encode("utf-8")
                    ).hexdigest(),
                },
                capture_artifacts=(),
                captured_at="2026-08-09T10:00:00+08:00",
                provider_schema_sha256="2" * 64,
                canonical_schema_sha256="3" * 64,
                validator_sha256="4" * 64,
            )
        self.host.validate_read_session_context(
            subject="english", context=context
        )
        receipt = context["capture_freeze_receipt"]
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(receipt["hmac_key_id"], hashlib.sha256(b"e" * 32).hexdigest())
        self.assertEqual(
            context["mcp_read_session"]["schema_version"],
            "study-read-mcp-read-session.v2",
        )
        return context

    def _task(self, suffix: str) -> FrozenTask:
        context = self._task_bound_context(suffix)
        session = context["mcp_read_session"]
        input_fingerprint = hashlib.sha256(
            f"fingerprint:{suffix}".encode("utf-8")
        ).hexdigest()
        rule_version = "study-intake-concurrent-dispatch-contract-v1"
        rule_sha256 = _value_sha256(
            {"rule_version": rule_version, "subject": "english"}
        )
        return FrozenTask(
            {
                "subject": "english",
                "capture_id": "EN-P0-E2E-CAPTURE",
                "study_date": "2026-08-09",
                "recorded_at": "2026-08-09T10:00:00+08:00",
                "input_fingerprint": input_fingerprint,
                "input_binding": {
                    "article_id": "RAW-ARTICLE-P0-E2E",
                    "capture_freeze_receipt_sha256": context[
                        "capture_freeze_receipt_sha256"
                    ],
                    "read_session_manifest_sha256": session[
                        "manifest_sha256"
                    ],
                },
                "model_input": {
                    "capture_manifest_sha256": session[
                        "capture_manifest_sha256"
                    ],
                    "fixture_mode": "zero_model",
                },
                "allowed_evidence_refs": [],
                "image_paths": [],
                "target_label": "English P0 authority E2E",
                "canonical_state": "event_written",
                "sol_state": "not_started",
                "dispatch_contract": {
                    "schema_version": "study-intake-dispatch-release-binding-v1",
                    "release_id": self.release_id,
                    "loaded_core_sha256": "5" * 64,
                    "rule_version": rule_version,
                    "rule_version_sha256": rule_sha256,
                    "subject_processing_contract_sha256": "6" * 64,
                    "dispatch_reason": "english_p0_zero_model_fixture",
                },
            }
        )

    def _decision(self, task: FrozenTask, updated_at: str) -> dict:
        payload = task.frozen_payload
        contract = payload["dispatch_contract"]
        return {
            "subject": "english",
            "capture_id": payload["capture_id"],
            "study_date": payload["study_date"],
            "target_label": payload["target_label"],
            "input_fingerprint": payload["input_fingerprint"],
            "eligible": True,
            "reason": "english_p0_zero_model_fixture",
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "release_id": self.release_id,
            "rule_version": contract["rule_version"],
            "rule_version_sha256": contract["rule_version_sha256"],
            "subject_processing_contract_sha256": contract[
                "subject_processing_contract_sha256"
            ],
            "phase": "frozen_evidence",
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "model_enqueue_allowed": True,
            "projection_origin": "english_p0_zero_model_fixture",
            "updated_at": updated_at,
        }

    def _project(self, decision: dict, updated_at: str) -> dict:
        store = LeaseStore(self.runtime)
        return update_subject_and_main_projection(
            self.dashboard_config,
            "english",
            study_date="2026-08-09",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[decision],
            lease_status=store.subject_status("english"),
            updated_at=updated_at,
        )

    @staticmethod
    def _english_item(projection: dict) -> dict:
        items = projection["subjects"]["english"]["items"]
        if len(items) != 1:
            raise AssertionError(f"expected one English item, got {len(items)}")
        return items[0]

    @staticmethod
    def _stage_result(payload: dict) -> StageResult:
        return StageResult(
            payload=payload,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            duration_ms=0,
            semantic_stage_count=1,
            provider_request_count=1,
            mcp_tool_call_count=0,
            model_call_count=1,
            consumed_terminal_duplicate_read_count=0,
        )

    def test_resume_english_critical_reuses_signed_checkpoint_and_closes_session(
        self,
    ) -> None:
        capture_id = "EN-RESUME-GROUNDING-E2E"
        event_id = "EVT-20260809-RESUME000000001"
        article_id = "RAW-ARTICLE-RESUME-E2E"
        source_sentence = "Lasting effort brings durable progress."
        article_sha256 = hashlib.sha256(article_id.encode("utf-8")).hexdigest()
        event = {
            "schema_version": "english_capture_event_v1",
            "event_id": event_id,
            "event_type": "sentence_captured",
            "article": {
                "article_id": article_id,
                "source_article": "articles/resume-grounding-e2e.md",
                "source_id": article_id,
                "source_hash": article_sha256,
            },
            "source": {
                "sentence_id": "S-001",
                "source_sentence": source_sentence,
                "source_kind": "article",
                "sentence_hash": hashlib.sha256(
                    source_sentence.encode("utf-8")
                ).hexdigest(),
            },
            "learning": {
                "first_translation": None,
                "user_evidence_verbatim": "lasting 不会",
                "user_evidence": ["unknown"],
                "evidence_states": ["unknown_observed"],
                "evidence_origin": "synthetic_fixture",
                "answer_protection": "practice_safe",
                "hint_level": 0,
                "translation": "",
                "explanation": "",
                "first_breakpoint": "",
                "restatement": "",
            },
            "candidates": [
                {
                    "item": "lasting",
                    "candidate_type": "单词",
                    "meaning": "持久的",
                    "decision": "long_term_candidate",
                }
            ],
            "formal_write_count": 0,
            "formal_writeback": "none",
        }
        input_binding = {
            "processing_contract_sha256": "6" * 64,
            "candidate_document_id": "EN-CAND-RESUME-GROUNDING-E2E",
            "source_id": article_id,
            "article_id": article_id,
            "article_source_hash": article_sha256,
            "capture_event_ids": [event_id],
            "capture_event_sha256": {event_id: sha256_value(event)},
        }
        input_fingerprint = sha256_value(input_binding)
        candidate = Candidate(
            subject="english",
            capture_id=capture_id,
            study_date="2026-08-09",
            recorded_at="2026-08-09T10:00:00+08:00",
            input_fingerprint=input_fingerprint,
            input_binding=input_binding,
            model_input={
                "batch_events": [event],
                "review_status_proposals": {
                    "review_exclusion_proposals": [],
                    "reactivation_proposals": [],
                    "needs_user_decision": [],
                },
            },
            allowed_evidence_refs=(),
            image_paths=(),
            target_label="English resume grounding E2E",
            canonical_state="event_written",
            sol_state="not_started",
        )
        capture_facts = {
            "schema_version": "english_resume_grounding_fixture_v1",
            "capture_id": capture_id,
            "event_id": event_id,
            "formal_write_count": 0,
        }
        with mock.patch.object(
            self.host, "_call", side_effect=self._authority_response
        ):
            processing_context = self.host.open_read_session(
                subject="english",
                capture_id=capture_id,
                study_date=candidate.study_date,
                input_fingerprint=input_fingerprint,
                input_binding=input_binding,
                capture_facts_sha256=_file_json_sha256(capture_facts),
                capture_facts=capture_facts,
                capture_scene="intensive_reading",
                capture_identity={
                    "article_id": article_id,
                    "content_fingerprint": input_fingerprint,
                },
                capture_artifacts=(),
                captured_at=str(candidate.recorded_at),
                provider_schema_sha256="2" * 64,
                canonical_schema_sha256="3" * 64,
                validator_sha256="4" * 64,
            )
        processing_context = self.host.validate_read_session_context(
            subject="english", context=processing_context
        )
        profile = {
            "enabled": True,
            "analysis_output_schema": str(
                ROOT / "schemas/luna-english-candidate-draft-v1.json"
            ),
            "critical_review_output_schema": str(
                ROOT / "schemas/luna-english-critical-review-v1.json"
            ),
            "controlled_contract_path": str(
                ROOT / "schemas/luna-english-controlled-contract-v1.json"
            ),
            "analysis_prompt_version": "english-analysis-resume-e2e-v1",
            "critical_review_prompt_version": (
                "english-critical-review-resume-e2e-v1"
            ),
            "stage_timeout_seconds": 5,
            "max_prompt_bytes": 1_048_576,
            "max_output_bytes": 524_288,
        }
        runner = CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "max_images": 8,
                "authority_release_id": self.release_id,
                "english_two_pass_v1": profile,
            },
            self.runtime,
        )
        runner._processing_host = self.host
        session = processing_context["mcp_read_session"]

        def stage_calls(
            *, library_stable_id: str, library_source_hash: str
        ) -> tuple[tuple[dict, ...], tuple[str, str]]:
            route = {
                "caller_skill_id": session["skill_id"],
                "caller_skill_version": session["skill_version"],
                "plugin_version": session["plugin_version"],
                "route_request_id": session["read_session_id"],
                "evidence_scope_hash": session["manifest_sha256"],
                "read_route": "mcp_model_driven",
                "read_session_id": session["read_session_id"],
                "consumed_duplicate_read_count": 0,
            }
            read_session = {
                "read_session_id": session["read_session_id"],
                "manifest_sha256": session["manifest_sha256"],
                "capture_id": session["capture_id"],
                "capture_manifest_sha256": session[
                    "capture_manifest_sha256"
                ],
                "artifact_ids": session["artifact_ids"],
                "candidate_release_id": session["candidate_release_id"],
                "subject": session["subject"],
                "generation": session["generation"],
                "authority_fingerprint": session[
                    "authority_fingerprint"
                ],
                "skill_id": session["skill_id"],
                "skill_version": session["skill_version"],
                "formal_write_count": 0,
            }
            specs = (
                ("get_task_context", {}, "task_context", capture_id, "7" * 64),
                (
                    "read_task_artifact",
                    {"artifact_id": "capture-facts", "max_bytes": 16_384},
                    "task_artifact",
                    "capture-facts",
                    "8" * 64,
                ),
                (
                    "list_records",
                    {"collection": "vocabulary", "page_size": 48},
                    "vocabulary",
                    library_stable_id,
                    library_source_hash,
                ),
            )
            calls = []
            refs = []
            for sequence, (
                tool,
                arguments,
                collection,
                stable_id,
                source_hash,
            ) in enumerate(specs, start=1):
                evidence_ref = model_mcp_item_ref(
                    subject="english",
                    generation=session["generation"],
                    collection=collection,
                    stable_id=stable_id,
                    source_hash=source_hash,
                )
                result = {
                    "ok": True,
                    "schema_version": "study-read-mcp.v3",
                    "profile": "luna",
                    "subject": "english",
                    "server_release": session["mcp_server_release"],
                    "adapter_release": session["mcp_server_release"],
                    "generation": session["generation"],
                    "authority_fingerprint": session[
                        "authority_fingerprint"
                    ],
                    "read_route": route,
                    "read_session": read_session,
                    "total_count": 1,
                    "returned_count": 1,
                    "offset": 0,
                    "next_cursor": None,
                    "truncated": False,
                    "complete": True,
                    "items": [
                        {
                            "collection": collection,
                            "stable_id": stable_id,
                            "source_hash": source_hash,
                            "data_role": collection,
                            "evidence_ref": evidence_ref,
                        }
                    ],
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "mcp_tool_call_count": 1,
                }
                calls.append(
                    {
                        "sequence": sequence,
                        "server": "kaoyan_english_read",
                        "tool": tool,
                        "arguments": arguments,
                        "arguments_sha256": _value_sha256(arguments),
                        "result": result,
                        "result_sha256": _value_sha256(result),
                    }
                )
                if collection in {"task_artifact", "vocabulary"}:
                    refs.append(evidence_ref)
            return tuple(calls), (refs[0], refs[1])

        analysis_calls, analysis_refs = stage_calls(
            library_stable_id="VOCAB-LASTING-ANALYSIS",
            library_source_hash="9" * 64,
        )
        analysis_item = {
            "item_id": "ITEM-LASTING",
            "sequence": 1,
            "item": "lasting",
            "candidate_type": "单词",
            "candidate_status": "familiarity_candidate",
            "tier": "A",
            "source_event_id": event_id,
            "bank_status": "new_candidate",
            "bank_match_ids": [],
            "mastered_status": "clear",
            "mastery_proposal": None,
            "grounding": {
                "status": "supported",
                "user_evidence_ref": event_id,
                "mcp_evidence_refs": list(analysis_refs),
                "writing_pattern": {"status": "not_requested"},
                "writing_vocabulary": {"status": "not_requested"},
                "syllabus_occurrence": {"status": "not_requested"},
                "sentence_pattern": {"status": "not_requested"},
                "old_word_sources": [],
                "naturalness_check": "not_requested",
            },
            "card": {
                "meaning": "持久的",
                "source_translation": "持久的努力",
                "usage": "形容词",
                "review_note": "与 lasting effort 搭配复习。",
            },
        }
        analysis_payload = {"items": [analysis_item]}
        (
            analysis_transcript_sha256,
            analysis_transcript_ref,
        ) = runner._persist_mcp_stage_transcript(
            stage_name="english_analysis",
            subject="english",
            processing_context=processing_context,
            calls=analysis_calls,
        )
        _, analysis_schema_sha256 = (
            runner._bound_english_source_event_schema_bytes(
                Path(profile["analysis_output_schema"]),
                stage_name="english_analysis",
                allowed_source_event_ids=(event_id,),
            )
        )
        analysis_stage = StructuredStageResult(
            payload=analysis_payload,
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256=sha256_value(analysis_payload),
            schema_sha256=analysis_schema_sha256,
            semantic_stage_count=1,
            provider_request_count=4,
            mcp_tool_call_count=3,
            mcp_transcript_sha256=analysis_transcript_sha256,
            mcp_transcript_ref=analysis_transcript_ref,
            mcp_calls=analysis_calls,
        )
        analysis_stage = _bind_stage_execution(
            analysis_stage, processing_context
        )
        draft = runner._finalize_english_document(
            candidate,
            analysis_payload,
            stage=analysis_stage,
            prompt_version=str(profile["analysis_prompt_version"]),
        )
        analysis_prompt = runner._english_analysis_prompt(
            candidate, profile, processing_context
        )
        analysis_receipt = runner._stage_receipt(
            analysis_stage,
            prompt_version=str(profile["analysis_prompt_version"]),
            prompt_sha256=hashlib.sha256(
                analysis_prompt.encode("utf-8")
            ).hexdigest(),
            schema_sha256=analysis_schema_sha256,
            result_sha256=sha256_value(draft),
            processing_context=processing_context,
        )
        self.host.validate_model_mcp_call_receipt(
            subject="english",
            context=processing_context,
            receipt_sha256=analysis_receipt["mcp_call_receipt_sha256"],
            expected_stage_name=str(profile["analysis_prompt_version"]),
            expected_transcript_sha256=analysis_transcript_sha256,
            expected_mcp_tool_call_count=3,
            expected_provider_request_count=4,
            require_success=True,
        )
        job_path = (
            self.runtime / "state/jobs/english" / f"{capture_id}.json"
        )
        atomic_write_json(
            job_path,
            {
                "subject": "english",
                "capture_id": capture_id,
                "study_date": candidate.study_date,
                "input_fingerprint": input_fingerprint,
                "input_binding": input_binding,
                "status": "processing",
                "formal_write_count": 0,
            },
        )
        checkpoint_root = (
            self.runtime / "private/reports/analysis-checkpoints"
        )
        self.assertFalse(checkpoint_root.exists())
        with mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            side_effect=ProcessingPluginError("mcp_stage_transcript_invalid"),
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "mcp_stage_transcript_invalid"
            ):
                runner._write_analysis_checkpoint(
                    candidate,
                    draft=draft,
                    analysis_receipt=analysis_receipt,
                    processing_context=processing_context,
                )
        self.assertFalse(checkpoint_root.exists())
        with mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            wraps=self.host.validate_persisted_model_mcp_stage,
        ) as checkpoint_validator:
            checkpoint = runner._write_analysis_checkpoint(
                candidate,
                draft=draft,
                analysis_receipt=analysis_receipt,
                processing_context=processing_context,
            )
        checkpoint_validator.assert_called_once()
        self.assertEqual(
            checkpoint_validator.call_args.kwargs["stage"], "analysis"
        )
        with mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            side_effect=ProcessingPluginError("mcp_stage_transcript_invalid"),
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "mcp_stage_transcript_invalid"
            ):
                runner.load_analysis_checkpoint(candidate)
        with mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            wraps=self.host.validate_persisted_model_mcp_stage,
        ) as load_validator:
            loaded_draft, loaded_receipt, loaded_checkpoint = (
                runner.load_analysis_checkpoint(candidate)
            )
        load_validator.assert_called_once()
        self.assertEqual(load_validator.call_args.kwargs["stage"], "analysis")
        self.assertEqual(loaded_draft, draft)
        self.assertEqual(loaded_receipt, analysis_receipt)
        self.assertEqual(
            loaded_checkpoint["checkpoint_sha256"],
            checkpoint["checkpoint_sha256"],
        )

        critical_calls, critical_refs = stage_calls(
            library_stable_id="VOCAB-LASTING-CRITICAL",
            library_source_hash="c" * 64,
        )
        self.assertEqual(critical_refs[0], analysis_refs[0])
        self.assertNotEqual(critical_refs[1], analysis_refs[1])
        review_prompt = runner._english_review_prompt(
            candidate, profile, draft, processing_context
        )
        self.assertIn(str(profile["critical_review_prompt_version"]), review_prompt)
        semantic_draft = english_review_semantic_draft(draft)
        revised_item = copy.deepcopy(semantic_draft["items"][0])
        revised_item["grounding"]["mcp_evidence_refs"] = list(critical_refs)
        critical_payload = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(semantic_draft),
            "findings": [
                {
                    "correction_id": "CR-GROUNDING-STAGE-LOCAL",
                    "code": "grounding_stage_local_refresh",
                    "severity": "blocking",
                    "item_id": "ITEM-LASTING",
                    "message": "Use evidence returned by this critic stage.",
                    "affected_json_paths": [
                        "$.items[0].grounding.mcp_evidence_refs"
                    ],
                }
            ],
            "correction_resolutions": [],
            "revised_items": [revised_item],
        }
        reused_analysis_payload = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "confirmed",
            "draft_analysis_sha256": sha256_value(semantic_draft),
            "findings": [],
            "correction_resolutions": [],
            "revised_items": copy.deepcopy(semantic_draft["items"]),
        }
        (
            critical_transcript_sha256,
            critical_transcript_ref,
        ) = runner._persist_mcp_stage_transcript(
            stage_name="english_critical_review",
            subject="english",
            processing_context=processing_context,
            calls=critical_calls,
        )
        _, critical_schema_sha256 = (
            runner._bound_english_source_event_schema_bytes(
                Path(profile["critical_review_output_schema"]),
                stage_name="english_critical_review",
                allowed_source_event_ids=(event_id,),
            )
        )
        critical_stage = StructuredStageResult(
            payload=critical_payload,
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256=sha256_value(critical_payload),
            schema_sha256=critical_schema_sha256,
            semantic_stage_count=1,
            provider_request_count=4,
            mcp_tool_call_count=3,
            mcp_transcript_sha256=critical_transcript_sha256,
            mcp_transcript_ref=critical_transcript_ref,
            mcp_calls=critical_calls,
        )
        critical_stage = _bind_stage_execution(
            critical_stage, processing_context
        )
        critical_receipt_for_gate = runner._stage_receipt(
            critical_stage,
            prompt_version=str(profile["critical_review_prompt_version"]),
            prompt_sha256=hashlib.sha256(
                review_prompt.encode("utf-8")
            ).hexdigest(),
            schema_sha256=critical_schema_sha256,
            result_sha256=sha256_value(critical_payload),
            processing_context=processing_context,
        )
        real_persisted_validator = (
            self.host.validate_persisted_model_mcp_stage
        )

        def fail_critical_persisted_stage(**kwargs):
            if kwargs.get("stage") == "critical_review":
                raise ProcessingPluginError("mcp_stage_transcript_invalid")
            return real_persisted_validator(**kwargs)

        gate_stage_receipts = {
            "analysis": analysis_receipt,
            "critical_review": critical_receipt_for_gate,
        }
        with mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            side_effect=fail_critical_persisted_stage,
        ), mock.patch.object(
            self.host,
            "finalize_model_read_session",
            wraps=self.host.finalize_model_read_session,
        ) as finalizer:
            with self.assertRaisesRegex(
                PreprocessorError, "mcp_stage_transcript_invalid"
            ):
                runner._finalize_read_session_receipt(
                    subject="english",
                    processing_context=processing_context,
                    stage_receipts=gate_stage_receipts,
                )
        finalizer.assert_not_called()
        self.assertNotIn("read_session", gate_stage_receipts)
        reused_analysis_stage = StructuredStageResult(
            payload=reused_analysis_payload,
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256=sha256_value(reused_analysis_payload),
            schema_sha256=critical_schema_sha256,
            semantic_stage_count=1,
            provider_request_count=4,
            mcp_tool_call_count=3,
            mcp_transcript_sha256=critical_transcript_sha256,
            mcp_transcript_ref=(
                "study-intake-mcp-stage-transcript://sha256/"
                + critical_transcript_sha256
            ),
            mcp_calls=critical_calls,
        )
        reused_analysis_stage = _bind_stage_execution(
            reused_analysis_stage, processing_context
        )
        with mock.patch.object(
            runner, "_execute_prompt", return_value=reused_analysis_stage
        ) as reused_execute_prompt:
            with self.assertRaisesRegex(
                PreprocessorError, "english_mcp_grounding_invalid"
            ):
                runner.resume_critical(
                    candidate,
                    draft_analysis=loaded_draft,
                    analysis_receipt=loaded_receipt,
                    processing_context=loaded_checkpoint[
                        "processing_context"
                    ],
                )
        reused_execute_prompt.assert_called_once()
        with mock.patch.object(
            runner, "_execute_prompt", return_value=critical_stage
        ) as execute_prompt, mock.patch.object(
            self.host,
            "validate_persisted_model_mcp_stage",
            wraps=self.host.validate_persisted_model_mcp_stage,
        ) as final_stage_validator:
            resumed = runner.resume_critical(
                candidate,
                draft_analysis=loaded_draft,
                analysis_receipt=loaded_receipt,
                processing_context=loaded_checkpoint["processing_context"],
            )
        execute_prompt.assert_called_once()
        self.assertEqual(
            [
                call.kwargs["stage"]
                for call in final_stage_validator.call_args_list
            ],
            ["analysis", "critical_review"],
        )
        self.assertEqual(
            execute_prompt.call_args.kwargs["stage_name"],
            "english_critical_review",
        )
        self.assertEqual(resumed.pipeline_status, "two_pass_ready")
        self.assertEqual(resumed.stage_receipts["analysis"], analysis_receipt)
        critical_receipt = resumed.stage_receipts["critical_review"]
        self.assertNotEqual(
            critical_receipt["mcp_grounding_manifest_sha256"],
            analysis_receipt["mcp_grounding_manifest_sha256"],
        )
        for row in critical_receipt["mcp_grounding_manifest"]["items"]:
            self.assertEqual(
                {
                    binding["transcript_sha256"]
                    for binding in row["consumed_in"]
                },
                {critical_transcript_sha256},
            )
        final_session = resumed.stage_receipts["read_session"]
        final_receipt = self.host.validate_final_model_read_session(
            subject="english",
            context=processing_context,
            finalized={
                "receipt": final_session["receipt"],
                "receipt_sha256": final_session["receipt_sha256"],
                "receipt_ref": final_session["receipt_ref"],
            },
        )
        self.assertEqual(final_receipt["phase"], "complete")
        self.assertEqual(final_receipt["model_mcp_tool_call_count"], 6)
        self.assertEqual(final_receipt["provider_request_count"], 8)
        self.assertEqual(final_receipt["formal_write_count"], 0)
        # This fixture intentionally exercises the historical read-session v2
        # resume path.  It may close and be audited, but current successor
        # publication must reject it because it has no frozen authority
        # snapshot.  Production v3 publication/reopen is covered by the sealed
        # three-subject integration suite.
        with self.assertRaisesRegex(
            PreprocessorError, "processing_stage_binding_incomplete"
        ):
            processing_publication_fields(resumed.stage_receipts)

    def test_en_p0_005_three_stages_are_distinct_and_only_package_is_complete(
        self,
    ) -> None:
        task = self._task("success")
        decision = self._decision(task, "2026-08-09T02:00:00Z")
        event_only = self._english_item(
            self._project(decision, "2026-08-09T02:00:00Z")
        )
        self.assertTrue(event_only["event_written"])
        self.assertFalse(event_only["dispatcher_accepted"])
        self.assertFalse(event_only["package_visible"])
        self.assertFalse(event_only["quick_intake_complete"])
        self.assertEqual(
            event_only["foreground_completion_stage"], "event_written"
        )

        store = LeaseStore(self.runtime)
        claim = store.claim(
            task.unit_sha256,
            "english-p0-e2e",
            subject="english",
            task=task,
        )
        self.assertEqual(claim.status, "claimed")
        assert claim.lease is not None
        store.record_task_event(task, claim.lease, "claim")
        accepted = self._english_item(
            self._project(decision, "2026-08-09T02:01:00Z")
        )
        self.assertTrue(accepted["event_written"])
        self.assertTrue(accepted["dispatcher_accepted"])
        self.assertFalse(accepted["package_visible"])
        self.assertFalse(accepted["quick_intake_complete"])
        self.assertEqual(
            accepted["foreground_completion_stage"], "dispatcher_accepted"
        )

        proposal = {
            "schema_version": "luna_proposal_v2",
            "subject": "english",
            "capture_id": "EN-P0-E2E-CAPTURE",
            "formal_write_count": 0,
        }
        proposal_sha256 = _value_sha256(proposal)
        completion = store.publish_terminal(
            claim.lease,
            task=task,
            outcome="succeeded",
            error_code=None,
            analysis=self._stage_result({"fixture": "analysis"}),
            critical_review=self._stage_result(
                {
                    "verdict": "accepted",
                    "luna_proposal": proposal,
                    "luna_proposal_sha256": proposal_sha256,
                }
            ),
            started_at="2026-08-09T02:01:00Z",
            finished_at="2026-08-09T02:02:00Z",
        )
        store.record_task_event(
            task,
            claim.lease,
            "published",
            artifact_refs={
                "completion_sha256": hashlib.sha256(
                    store._completion_path(task.unit_sha256).read_bytes()
                ).hexdigest(),
                "package_sha256": completion["package_sha256"],
            },
        )
        visible_projection = self._project(
            decision, "2026-08-09T02:02:00Z"
        )
        visible = self._english_item(visible_projection)
        self.assertTrue(visible["event_written"])
        self.assertTrue(visible["dispatcher_accepted"])
        self.assertTrue(visible["package_visible"])
        self.assertTrue(visible["quick_intake_complete"])
        self.assertEqual(
            visible["foreground_completion_stage"], "package_visible"
        )
        self.assertEqual(visible["selected_by"], "signed_latest_authoritative")
        self.assertEqual(visible["selection_authority_status"], "hmac_verified")
        self.assertEqual(visible["processing_outcome"], "succeeded")
        self.assertEqual(visible["proposal_sha256"], proposal_sha256)
        self.assertEqual(
            visible["authoritative_package_sha256"],
            completion["package_sha256"],
        )
        self.assertEqual(visible["package_sha256"], completion["package_sha256"])
        self.assertRegex(visible["selector_sha256"], r"^[0-9a-f]{64}$")
        persisted = json.loads(
            (
                self.runtime
                / "state/dashboard-projections/2026-08-09.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            self._english_item(persisted)["selector_sha256"],
            visible["selector_sha256"],
        )

    def test_en_p0_009_signed_newer_failure_never_selects_older_success(
        self,
    ) -> None:
        store = LeaseStore(self.runtime)
        old_task = self._task("historical-success")
        old_claim = store.claim(
            old_task.unit_sha256,
            "english-p0-old",
            subject="english",
            task=old_task,
        )
        assert old_claim.lease is not None
        store.record_task_event(old_task, old_claim.lease, "claim")
        old_completion = store.publish_terminal(
            old_claim.lease,
            task=old_task,
            outcome="succeeded",
            error_code=None,
            analysis=self._stage_result({"fixture": "old-analysis"}),
            critical_review=self._stage_result({"verdict": "accepted"}),
            started_at="2026-08-09T01:00:00Z",
            finished_at="2026-08-09T01:01:00Z",
        )
        store.record_task_event(old_task, old_claim.lease, "published")
        old_package_path = Path(str(old_completion["package_path"]))
        old_package_sha256 = str(old_completion["package_sha256"])
        self.assertTrue(old_package_path.is_file())

        current_task = self._task("newer-failure")
        current_decision = self._decision(
            current_task, "2026-08-09T03:00:00Z"
        )
        current_claim = store.claim(
            current_task.unit_sha256,
            "english-p0-current",
            subject="english",
            task=current_task,
        )
        assert current_claim.lease is not None
        store.record_task_event(current_task, current_claim.lease, "claim")
        failure = store.publish_terminal(
            current_claim.lease,
            task=current_task,
            outcome="failed",
            error_code="english_required_correction_unresolved",
            analysis=None,
            critical_review=None,
            started_at="2026-08-09T03:00:00Z",
            finished_at="2026-08-09T03:01:00Z",
        )
        self.assertIsNone(failure["package_sha256"])
        store.record_task_event(
            current_task,
            current_claim.lease,
            "failed",
            error_code="english_required_correction_unresolved",
        )

        # Make the obsolete success newer by mtime.  Authority, not directory
        # order or mtime, must still select the signed failed pointer.
        os.utime(old_package_path, (2_000_000_000, 2_000_000_000))
        projection = self._project(
            current_decision, "2026-08-09T03:01:00Z"
        )
        item = self._english_item(projection)
        latest_path = (
            self.runtime
            / "dispatch/state/latest-authoritative/english"
            / "EN-P0-E2E-CAPTURE.json"
        )
        self.assertTrue(item["event_written"])
        self.assertTrue(item["dispatcher_accepted"])
        self.assertFalse(item["package_visible"])
        self.assertFalse(item["quick_intake_complete"])
        self.assertEqual(item["selected_by"], "none")
        self.assertEqual(item["selector_type"], "signed_latest_authoritative")
        self.assertEqual(
            item["selector_sha256"],
            hashlib.sha256(latest_path.read_bytes()).hexdigest(),
        )
        self.assertIsNone(item["proposal_sha256"])
        self.assertIsNone(item["authoritative_package_sha256"])
        self.assertNotIn("package_sha256", item)
        self.assertEqual(item["processing_outcome"], "failed")
        self.assertEqual(item["selection_authority_status"], "hmac_verified")
        self.assertTrue(old_package_path.is_file())
        self.assertEqual(
            hashlib.sha256(old_package_path.read_bytes()).hexdigest(),
            old_package_sha256,
        )

        original_latest = latest_path.read_bytes()
        try:
            tampered = json.loads(original_latest)
            tampered["authority"]["hmac_sha256"] = "0" * 64
            latest_path.write_text(
                json.dumps(tampered, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            closed = self._english_item(
                self._project(current_decision, "2026-08-09T03:02:00Z")
            )
            self.assertFalse(closed["package_visible"])
            self.assertEqual(closed["selected_by"], "none")
            self.assertIsNone(closed["selector_sha256"])
            self.assertEqual(closed["selection_authority_status"], "invalid")
            self.assertEqual(
                closed["selection_error_code"], "authority_hmac_invalid"
            )
        finally:
            latest_path.write_bytes(original_latest)
            latest_path.chmod(0o600)


if __name__ == "__main__":
    unittest.main()
