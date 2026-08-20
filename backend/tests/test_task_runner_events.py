from __future__ import annotations

import json
import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import (  # noqa: E402
    DispatchError,
    FrozenTask,
    LeaseStore,
    dispatch_rule_binding,
)
from preprocess_task_runner import (  # noqa: E402
    _AnalysisCheckpointEventRecorder,
    _StageEventRecorder,
    run_request,
)
from preprocess_dispatcher import _stage_runtime_contract  # noqa: E402
from preprocessor_core import ModelResult  # noqa: E402


class TaskRunnerEventTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name)
        self.release_id = "a" * 64
        self.processing_contract_sha256 = "c" * 64
        rule_binding = dispatch_rule_binding(
            release_id=self.release_id,
            subject="english",
            subject_processing_contract_sha256=(
                self.processing_contract_sha256
            ),
        )
        self.task = FrozenTask(
            {
                "subject": "english",
                "capture_id": "EN-TEST",
                "study_date": "2026-08-05",
                "recorded_at": "2026-08-05T00:00:00Z",
                "input_fingerprint": "b" * 64,
                "input_binding": {
                    "processing_contract_sha256": (
                        self.processing_contract_sha256
                    )
                },
                "model_input": {},
                "allowed_evidence_refs": [],
                "image_paths": [],
                "target_label": "English test",
                "canonical_state": "queued",
                "sol_state": "pending_review",
                "dispatch_contract": {
                    "schema_version": (
                        "study-intake-dispatch-release-binding-v1"
                    ),
                    **rule_binding,
                    "dispatch_reason": "eligible",
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                },
            }
        )
        self.store = LeaseStore(self.runtime)
        decision = self.store.claim(
            self.task.unit_sha256,
            "child-owner",
            subject="english",
        )
        self.lease = decision.lease
        assert self.lease is not None
        self.context_root = (
            self.runtime
            / "dispatch"
            / "contexts"
            / self.task.unit_sha256
            / "fence-1"
        )
        self.context_root.mkdir(parents=True)
        self.store.record_task_event(
            self.task, self.lease, "process_started"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _events(self) -> list[dict]:
        root = (
            self.runtime
            / "dispatch"
            / "state"
            / "task-events"
            / self.task.unit_sha256
            / "fence-1"
        )
        rows = []
        for index_path in sorted(root.glob("*.json")):
            index = json.loads(index_path.read_text())
            rows.append(json.loads(Path(index["event_path"]).read_text()))
        return rows

    def test_records_real_analysis_and_review_boundaries_with_authority(self) -> None:
        calls = []

        class Runner:
            def _execute_prompt(inner_self, **kwargs):
                calls.append(kwargs["stage_name"])
                return {"stage": kwargs["stage_name"]}

            def _write_analysis_checkpoint(inner_self, *_args, **_kwargs):
                return {
                    "checkpoint_sha256": "1" * 64,
                    "checkpoint_ref": (
                        "study-intake-analysis-checkpoint://sha256/"
                        + "1" * 64
                    ),
                    "binding_key": "2" * 64,
                    "binding_sha256": "3" * 64,
                }

        runner = Runner()
        recorder = _StageEventRecorder(
            runner, task=self.task, lease=self.lease, store=self.store
        )
        checkpoint_recorder = _AnalysisCheckpointEventRecorder(
            runner, task=self.task, lease=self.lease, store=self.store
        )
        recorder(stage_name="english_analysis")
        checkpoint_recorder(None, draft={}, analysis_receipt={})
        recorder(stage_name="english_critical_review")
        rows = self._events()
        self.assertEqual(
            [row["event"] for row in rows],
            [
                "process_started",
                "analysis_submitted",
                "analysis_completed",
                "critical_started",
                "critical_completed",
            ],
        )
        self.assertEqual(
            calls, ["english_analysis", "english_critical_review"]
        )
        self.assertNotIn("heartbeat", [row["event"] for row in rows])
        for row in rows:
            self.assertEqual(row["unit_sha256"], self.task.unit_sha256)
            self.assertEqual(row["owner_id"], "child-owner")
            self.assertEqual(row["fence"], 1)
            self.assertEqual(row["release_id"], self.release_id)
            self.assertEqual(row["formal_write_count"], 0)
            self.assertEqual(
                row["authority"]["purpose"], "dispatch-task-event"
            )
        verified = self.store.verify_authoritative_task_detail(
            self.task.unit_sha256,
            expected_release_id=self.release_id,
        )
        self.assertEqual(
            verified["latest_event"]["event"], "critical_completed"
        )

    def test_failed_review_has_start_without_false_completion(self) -> None:
        class Runner:
            def _execute_prompt(inner_self, **_kwargs):
                raise RuntimeError("simulated-review-failure")

        recorder = _StageEventRecorder(
            Runner(), task=self.task, lease=self.lease, store=self.store
        )
        with self.assertRaisesRegex(RuntimeError, "simulated-review-failure"):
            recorder(stage_name="english_critical_review")
        self.assertEqual(
            [row["event"] for row in self._events()],
            ["process_started", "critical_started"],
        )

    def test_stale_fence_cannot_write_core_analysis_checkpoint(self) -> None:
        calls = []

        class Runner:
            def _write_analysis_checkpoint(inner_self, *_args, **_kwargs):
                calls.append("write")
                return {}

        recorder = _AnalysisCheckpointEventRecorder(
            Runner(), task=self.task, lease=self.lease, store=self.store
        )
        self.store.recover_after_infrastructure_crash(
            self.lease, error_code="synthetic_crash"
        )
        with self.assertRaisesRegex(DispatchError, "stale_lease_fence"):
            recorder(None, draft={}, analysis_receipt={})
        self.assertEqual(calls, [])

    def test_run_request_wraps_both_actual_execute_prompt_calls(self) -> None:
        def stage_receipt(stage_name: str) -> dict:
            evidence_ref = "mcp-item:english:" + hashlib.sha256(
                f"{stage_name}-evidence".encode()
            ).hexdigest()
            transcript_sha256 = hashlib.sha256(
                f"transcript:{stage_name}".encode()
            ).hexdigest()
            grounding_core = {
                "schema_version": "model_mcp_grounding_manifest_v1",
                "items": [
                    {
                        "evidence_ref": evidence_ref,
                        "subject": "english",
                        "generation": "english-test-generation",
                        "collection": "capture-facts",
                        "stable_id": f"{stage_name}-evidence",
                        "source_hash": "7" * 64,
                        "data_role": "capture_fact",
                        "consumed_in": [
                            {
                                "transcript_sha256": transcript_sha256,
                                "call_sequence": 1,
                                "result_sha256": "8" * 64,
                            }
                        ],
                    }
                ],
                "item_count": 1,
                "host_semantic_prefetch": False,
                "formal_write_count": 0,
            }
            grounding = {
                **grounding_core,
                "manifest_sha256": hashlib.sha256(
                    json.dumps(
                        grounding_core,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest(),
            }
            return {
                "status": "ready",
                "requested_model": "gpt-5.6-luna",
                "requested_reasoning_effort": "max",
                "runtime_model": "gpt-5.6-luna",
                "runtime_reasoning_effort": "max",
                "runtime_metadata_provenance": "codex_json_attestation_v1",
                "runtime_identity_status": "confirmed",
                "duration_ms": 2,
                "processing_binding": {
                    "candidate_release_id": self.release_id,
                },
                "read_session_id": "READ-TEST",
                "read_session_manifest_sha256": "1" * 64,
                "authority_snapshot_manifest_sha256": "2" * 64,
                "capture_freeze_receipt_sha256": "3" * 64,
                "mcp_read_session_receipt_sha256": "4" * 64,
                "mcp_transcript_sha256": transcript_sha256,
                "evidence_generation": "english-test-generation",
                "evidence_authority_fingerprint": "5" * 64,
                "mcp_grounding_manifest": grounding,
                "mcp_grounding_manifest_sha256": grounding["manifest_sha256"],
                "semantic_stage_count": 1,
                "provider_request_count": 2,
                "mcp_tool_call_count": 1,
                "consumed_terminal_duplicate_read_count": 0,
            }

        stage_receipts = {
            stage_name: stage_receipt(stage_name)
            for stage_name in ("analysis", "critical_review")
        }

        class Runner:
            def _execute_prompt(inner_self, **kwargs):
                return {"stage": kwargs["stage_name"]}

            def _write_analysis_checkpoint(inner_self, *_args, **_kwargs):
                return {
                    "checkpoint_sha256": "1" * 64,
                    "checkpoint_ref": (
                        "study-intake-analysis-checkpoint://sha256/"
                        + "1" * 64
                    ),
                    "binding_key": "2" * 64,
                    "binding_sha256": "3" * 64,
                }

            def run(inner_self, _candidate):
                inner_self._execute_prompt(stage_name="english_analysis")
                inner_self._write_analysis_checkpoint(
                    _candidate, draft={}, analysis_receipt={}
                )
                inner_self._execute_prompt(
                    stage_name="english_critical_review"
                )
                return ModelResult(
                    analysis={"final": True},
                    duration_ms=4,
                    runtime_model="gpt-5.6-luna",
                    runtime_reasoning_effort="max",
                    runtime_metadata_provenance="codex_json_attestation_v1",
                    pipeline_status="two_pass_ready",
                    draft_analysis={
                        "draft": True,
                        "evidence": stage_receipts["analysis"][
                            "mcp_grounding_manifest"
                        ]["items"][0]["evidence_ref"],
                    },
                    critical_review={
                        "review": True,
                        "evidence": stage_receipts["critical_review"][
                            "mcp_grounding_manifest"
                        ]["items"][0]["evidence_ref"],
                    },
                    stage_receipts=stage_receipts,
                )

        class Worker:
            release_id = self.release_id

            def __init__(inner_self, _config):
                inner_self.runner = Runner()

            def process_claimed_candidate(
                inner_self, candidate, _reason, *, write_dashboard
            ):
                self.assertFalse(write_dashboard)
                inner_self.runner.run(candidate)
                return {"status": "ready"}

        config = {
            "runtime_root": str(self.runtime),
            "model": {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
        }
        request = {
            "schema_version": "study-intake-production-task-request-v1",
            "task": self.task.as_dict(),
            "reason": "eligible",
            "unit_sha256": self.task.unit_sha256,
            "lease_fence": self.lease.fence,
            "lease_owner_id": self.lease.owner_id,
        }
        environment = {
            "STUDY_PREPROCESS_RUNTIME_ROOT": str(self.runtime.resolve()),
            "STUDY_PREPROCESS_UNIT_SHA256": self.task.unit_sha256,
            "STUDY_PREPROCESS_LEASE_FENCE": str(self.lease.fence),
            "STUDY_PREPROCESS_LEASE_OWNER_ID": self.lease.owner_id,
            "STUDY_PREPROCESS_CONTEXT_ROOT": str(self.context_root),
        }
        with (
            mock.patch("preprocess_task_runner.load_config", return_value=config),
            mock.patch("preprocess_task_runner.Worker", Worker),
            mock.patch.dict(os.environ, environment, clear=False),
        ):
            result = run_request(self.runtime / "config.json", request)
        self.assertEqual(result["formal_write_count"], 0)
        self.assertEqual(result["analysis"]["payload"]["draft"], True)
        self.assertEqual(
            result["critical_review"]["payload"]["review"], True
        )
        self.assertEqual(
            [row["event"] for row in self._events()],
            [
                "process_started",
                "analysis_submitted",
                "analysis_completed",
                "critical_started",
                "critical_completed",
            ],
        )

    def test_run_request_preserves_worker_service_limit_without_result(self) -> None:
        class Runner:
            def _execute_prompt(inner_self, **_kwargs):
                raise AssertionError("model must not be called")

            def _write_analysis_checkpoint(inner_self, *_args, **_kwargs):
                raise AssertionError("checkpoint must not be written")

        class Worker:
            release_id = self.release_id

            def __init__(inner_self, _config):
                inner_self.runner = Runner()

            def process_claimed_candidate(
                inner_self, _candidate, _reason, *, write_dashboard
            ):
                self.assertFalse(write_dashboard)
                return {
                    "status": "retrying",
                    "last_error_code": "english_analysis_rate_limited",
                }

        config = {
            "runtime_root": str(self.runtime),
            "model": {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
        }
        request = {
            "schema_version": "study-intake-production-task-request-v1",
            "task": self.task.as_dict(),
            "reason": "eligible",
            "unit_sha256": self.task.unit_sha256,
            "lease_fence": self.lease.fence,
            "lease_owner_id": self.lease.owner_id,
        }
        environment = {
            "STUDY_PREPROCESS_RUNTIME_ROOT": str(self.runtime.resolve()),
            "STUDY_PREPROCESS_UNIT_SHA256": self.task.unit_sha256,
            "STUDY_PREPROCESS_LEASE_FENCE": str(self.lease.fence),
            "STUDY_PREPROCESS_LEASE_OWNER_ID": self.lease.owner_id,
            "STUDY_PREPROCESS_CONTEXT_ROOT": str(self.context_root),
        }
        with (
            mock.patch("preprocess_task_runner.load_config", return_value=config),
            mock.patch("preprocess_task_runner.Worker", Worker),
            mock.patch.dict(os.environ, environment, clear=False),
            self.assertRaises(DispatchError) as raised,
        ):
            run_request(self.runtime / "config.json", request)
        self.assertEqual(
            raised.exception.code, "english_analysis_rate_limited"
        )

    def test_production_child_rejects_legacy_408_evidence_with_zero_model_calls(
        self,
    ) -> None:
        processing_contract = "d" * 64
        rule_binding = dispatch_rule_binding(
            release_id=self.release_id,
            subject="cs408",
            subject_processing_contract_sha256=processing_contract,
        )
        task = FrozenTask(
            {
                "subject": "cs408",
                "capture_id": "CS408-LEGACY-EVIDENCE",
                "study_date": "2026-08-05",
                "recorded_at": "2026-08-05T00:00:00Z",
                "input_fingerprint": "e" * 64,
                "input_binding": {
                    "processing_contract_sha256": processing_contract,
                    "evidence_status": "ready",
                },
                "model_input": {
                    "current_question_evidence": {
                        "schema_version": (
                            "current-question-evidence-bundle-v2"
                        ),
                        "interaction_trace": {
                            "schema": (
                                "current-question-interaction-trace-v1"
                            )
                        },
                    }
                },
                "allowed_evidence_refs": [],
                "image_paths": [],
                "target_label": "legacy evidence",
                "canonical_state": "awaiting_daily_curation",
                "sol_state": "pending_review",
                "dispatch_contract": {
                    "schema_version": (
                        "study-intake-dispatch-release-binding-v1"
                    ),
                    **rule_binding,
                    "dispatch_reason": "eligible",
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                },
            }
        )
        store = LeaseStore(self.runtime)
        decision = store.claim(
            task.unit_sha256, "legacy-child-owner", subject="cs408"
        )
        assert decision.lease is not None
        context_root = (
            self.runtime
            / "dispatch"
            / "contexts"
            / task.unit_sha256
            / "fence-1"
        )
        context_root.mkdir(parents=True)
        config = {
            "runtime_root": str(self.runtime),
            "model": {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
        }
        request = {
            "schema_version": "study-intake-production-task-request-v1",
            "task": task.as_dict(),
            "reason": "eligible",
            "unit_sha256": task.unit_sha256,
            "lease_fence": 1,
            "lease_owner_id": "legacy-child-owner",
        }
        worker_calls = []

        class ForbiddenWorker:
            def __init__(inner_self, _config):
                worker_calls.append("worker-created")

        environment = {
            "STUDY_PREPROCESS_RUNTIME_ROOT": str(self.runtime.resolve()),
            "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
            "STUDY_PREPROCESS_LEASE_FENCE": "1",
            "STUDY_PREPROCESS_LEASE_OWNER_ID": "legacy-child-owner",
            "STUDY_PREPROCESS_CONTEXT_ROOT": str(context_root),
        }
        with (
            mock.patch(
                "preprocess_task_runner.load_config", return_value=config
            ),
            mock.patch("preprocess_task_runner.Worker", ForbiddenWorker),
            mock.patch.dict(os.environ, environment, clear=False),
            self.assertRaisesRegex(
                DispatchError, "concurrent_cs408_evidence_v3_required"
            ),
        ):
            run_request(self.runtime / "config.json", request)
        self.assertEqual(worker_calls, [])

    def test_subject_progress_aware_runtime_contract(self) -> None:
        config = {
            "math_deep_v2": {
                "soft_runtime_warning_seconds": 3600,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
            },
            "cs408_deep_v2": {
                "soft_runtime_warning_seconds": 1800,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
            },
            "english_two_pass_v1": {
                "soft_runtime_warning_seconds": 1800,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
            },
            "worker": {"model_timeout_seconds": 900},
        }
        self.assertEqual(
            _stage_runtime_contract(config, "math"),
            {
                "soft_runtime_warning_seconds": 3600.0,
                "stall_timeout_seconds": 1800.0,
                "stall_probe_interval_seconds": 60.0,
                "stall_probe_required_consecutive_failures": 2,
            },
        )
        self.assertEqual(
            _stage_runtime_contract(config, "cs408")[
                "soft_runtime_warning_seconds"
            ],
            1800.0,
        )
        self.assertEqual(
            _stage_runtime_contract(config, "english")[
                "stall_probe_required_consecutive_failures"
            ],
            2,
        )


if __name__ == "__main__":
    unittest.main()
