from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import http.client
import importlib.util
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
)
from english_legacy_recuration import publish_run_summary  # noqa: E402
from dashboard_projection import (  # noqa: E402
    _apply_batch_task_state,
    _queue_and_stage,
    restore_dashboard_projection_migration,
    update_subject_and_main_projection,
)
import dashboard_projection as dashboard_projection_module  # noqa: E402


def _load_dashboard_module():
    path = ROOT / "dashboard" / "server.py"
    spec = importlib.util.spec_from_file_location("projection_merge_dashboard_server", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


dashboard = _load_dashboard_module()


class _SuccessRunner:
    def run_analysis(self, task, _context):
        subject = task.frozen_payload["subject"]
        payload = (
            {"items": [{"kind": "sentence", "value": "draft"}]}
            if subject == "english"
            else {"kind": "analysis", "unit_sha256": task.unit_sha256}
        )
        return StageResult(
            payload=payload,
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            duration_ms=1,
        )

    def run_critical_review(self, task, analysis, _context):
        subject = task.frozen_payload["subject"]
        payload = (
            {"revised_items": analysis["items"], "verdict": "accept"}
            if subject == "english"
            else {"revised_analysis": dict(analysis), "verdict": "accept"}
        )
        return StageResult(
            payload=payload,
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            duration_ms=1,
        )


class _BlockedRunner(_SuccessRunner):
    def __init__(self, entered: threading.Event, release: threading.Event) -> None:
        self.entered = entered
        self.release = release

    def run_critical_review(self, task, analysis, context):
        self.entered.set()
        self.release.wait(10)
        return super().run_critical_review(task, analysis, context)


class _RequestedUnverifiedRunner(_SuccessRunner):
    @staticmethod
    def _unverified(result: StageResult) -> StageResult:
        return StageResult(
            payload=dict(result.payload),
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            duration_ms=result.duration_ms,
        )

    def run_analysis(self, task, context):
        return self._unverified(super().run_analysis(task, context))

    def run_critical_review(self, task, analysis, context):
        return self._unverified(
            super().run_critical_review(task, analysis, context)
        )


class SemanticReuseProjectionTests(unittest.TestCase):
    def test_three_subject_quality_findings_keep_execution_succeeded(self):
        for subject in ("math", "english", "cs408"):
            with self.subTest(subject=subject):
                task = {
                    "status": "workflow_complete_with_warnings",
                    "analysis_execution_receipt_sha256": "1" * 64,
                    "analysis_raw_output_sha256": "2" * 64,
                    "analysis_normalization_receipt_sha256": "3" * 64,
                    "analysis_report_sha256": "4" * 64,
                    "critical_review_execution_receipt_sha256": (
                        "5" * 64 if subject == "cs408" else None
                    ),
                    "critical_review_raw_output_sha256": (
                        "6" * 64 if subject == "cs408" else None
                    ),
                    "critical_review_normalization_receipt_sha256": (
                        "7" * 64 if subject == "cs408" else None
                    ),
                    "critical_review_report_sha256": (
                        "8" * 64 if subject == "cs408" else None
                    ),
                    "package_sha256": "9" * 64,
                    "sol_handoff_envelope_sha256": None,
                    "terminal_receipt_sha256": "a" * 64,
                    "warning_codes": [f"{subject}:quality_finding"],
                    "error_code": None,
                }
                item = {
                    "queue_state": "running",
                    "current_stage": "quality_ready",
                    "luna_status": "processing",
                }

                _apply_batch_task_state(item, task)

                self.assertEqual(item["queue_state"], "ready")
                self.assertEqual(item["luna_status"], "succeeded")
                self.assertEqual(item["terminal_status"], "succeeded")
                self.assertEqual(item["execution_status"], "succeeded")
                self.assertEqual(item["quality_status"], "issues_found")
                self.assertEqual(item["quality_outcome"], "needs_sol_review")
                self.assertEqual(
                    item["report_disposition"], "needs_sol_review"
                )
                self.assertEqual(item["sol_review_status"], "pending")
                self.assertFalse(item["formal_write_eligible"])
                self.assertFalse(item["production_accepted"])
                self.assertEqual(
                    item["review_execution_status"],
                    "completed" if subject == "cs408" else "not_started",
                )
                self.assertNotIn(
                    item["luna_status"],
                    {"needs_rework", "failed", "retrying", "rejected"},
                )

    def test_semantic_reuse_stays_published_without_running_state(self):
        self.assertEqual(
            ("ready", "quality_ready", "two_pass_ready"),
            _queue_and_stage(
                {
                    "eligible": False,
                    "model_enqueue_allowed": False,
                    "reason": "semantic_package_reused",
                    "semantic_reuse_receipt_sha256": "1" * 64,
                    "semantic_reuse_source_release_id": "2" * 64,
                    "package_sha256": "3" * 64,
                },
                None,
            ),
        )

    def test_real_events_override_stale_selected_batch_and_unknown_server_queue(self):
        decision = {
            "eligible": False,
            "model_enqueue_allowed": False,
            "reason": "production_canary_queued_not_selected",
        }
        analysis_event = {
            "event": "provider_process_started",
            "stage_name": "analysis",
        }
        self.assertEqual(
            ("running", "analysis", "processing"),
            _queue_and_stage(decision, analysis_event),
        )
        item = {
            "queue_state": "running",
            "current_stage": "analysis",
            "luna_status": "processing",
            "local_dispatch_status": "running",
            "model_stage": "analysis",
            "terminal_status": None,
            "last_state_change_at": "2026-08-12T14:03:40+08:00",
        }
        _apply_batch_task_state(
            item,
            {"status": "selected", "quality_outcome": None},
        )
        self.assertEqual(item["queue_state"], "running")
        self.assertEqual(item["current_stage"], "analysis")
        self.assertEqual(item["luna_status"], "processing")

    def test_consumer_paused_ready_capture_is_not_waiting_for_evidence(self):
        self.assertEqual(
            ("queued", "ready_for_selection", "paused"),
            _queue_and_stage(
                {
                    "eligible": False,
                    "model_enqueue_allowed": False,
                    "reason": "production_canary_queued_not_selected",
                },
                None,
            ),
        )

    def test_terminal_event_cannot_be_replaced_by_stale_batch_terminal(self):
        item = {
            "queue_state": "ready",
            "current_stage": "quality_ready",
            "luna_status": "two_pass_ready",
            "local_dispatch_status": "terminal",
            "model_stage": "quality_closed",
            "terminal_status": "ready",
            "last_state_change_at": "2026-08-12T14:18:00+08:00",
        }
        _apply_batch_task_state(
            item,
            {"status": "failed", "quality_outcome": "failed"},
        )
        self.assertEqual(item["queue_state"], "ready")
        self.assertEqual(item["terminal_status"], "ready")

    def test_semantic_reuse_without_verified_bindings_fails_closed(self):
        self.assertEqual(
            ("failed", "failed", "failed"),
            _queue_and_stage(
                {
                    "eligible": False,
                    "model_enqueue_allowed": False,
                    "reason": "semantic_package_reused",
                },
                None,
            ),
        )


class ProductionCanaryProjectionTests(unittest.TestCase):
    @staticmethod
    def gate() -> dict:
        authority = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": "1" * 64,
            "purpose": "dispatch-production-canary-state",
            "hmac_sha256": "2" * 64,
        }
        return {
            "schema_version": "study-intake-production-canary-state-v2",
            "status": "production_canary_active",
            "state": "armed",
            "subject": "math",
            "release_id": "a" * 64,
            "activation_id": "3" * 64,
            "activated_at": "2026-08-11T04:00:00+00:00",
            "producer_authority_fingerprint": "4" * 64,
            "producer_high_watermark_sha256": "5" * 64,
            "activation_receipt_sha256": "6" * 64,
            "activation_receipt_path": "/private/runtime/activation.json",
            "activation_gate_authority_sha256": "7" * 64,
            "terminal_index_sha256": "8" * 64,
            "terminal_index_path": "/private/runtime/terminal-index.json",
            "terminal_task_count": 0,
            "terminal_by_outcome": {
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "timed_out": 0,
                "needs_rework": 0,
            },
            "luna_consumer_enabled": True,
            "producer_capture_enabled": True,
            "sol_formal_curation_enabled": False,
            "queue_depth": 0,
            "oldest_pending_age_seconds": None,
            "active_task_count": 0,
            "active_selections": {},
            "last_success_at": None,
            "last_failure_at": None,
            "last_preclaim_failure_at": None,
            "last_preclaim_failure_stage": None,
            "last_preclaim_failure_error_code": None,
            "last_preclaim_failure_receipt_sha256": None,
            "last_preclaim_failure_evidence_sha256": None,
            "preclaim_failure_resume_ack_sha256": None,
            "blocking_reason": None,
            "backpressure_reason": None,
            "next_action": "await_first_post_activation_capture",
            "last_analysis_status": "not_started",
            "last_critical_review_status": "not_started",
            "last_report_status": "not_started",
            "last_read_session_id": None,
            "last_evidence_generation": None,
            "last_evidence_authority_fingerprint": None,
            "last_analysis_grounding_manifest_sha256": None,
            "last_critical_review_grounding_manifest_sha256": None,
            "last_analysis_evidence_refs": [],
            "last_critical_review_evidence_refs": [],
            "last_emergency_cancel_at": None,
            "last_emergency_cancel_receipt_sha256": None,
            "late_result_fence_status": "not_required",
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "keep_backlog_drained": True,
            "selected": None,
            "last_selected": None,
            "unlocked_once": False,
            "production_accepted": False,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "requested_service_tier": None,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "observability_counter_scope": "current_activation_cumulative",
            "observed_model_call_count": 0,
            "observed_provider_request_count": 0,
            "observed_mcp_tool_call_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "updated_at": "2026-08-11T04:00:00+00:00",
            "authority": authority,
        }

    def test_canary_projection_is_idempotent_and_removes_private_paths(self):
        first = dashboard_projection_module._public_canary_gate(
            self.gate(), subject="math", release_id="a" * 64
        )
        second = dashboard_projection_module._public_canary_gate(
            first, subject="math", release_id="a" * 64
        )
        self.assertEqual(first, second)
        self.assertNotIn("activation_receipt_path", first)
        self.assertNotIn("terminal_index_path", first)
        self.assertEqual(first["queue_depth"], 0)
        self.assertEqual(first["control_plane_provider_request_count"], 0)
        self.assertFalse(first["production_accepted"])

    def test_canary_v3_projection_is_accepted_without_downgrading_schema(self):
        gate = self.gate()
        gate["schema_version"] = "study-intake-production-canary-state-v3"
        public = dashboard_projection_module._public_canary_gate(
            gate, subject="math", release_id="a" * 64
        )
        self.assertEqual(
            public["schema_version"],
            "study-intake-production-canary-state-v3",
        )
        self.assertEqual(public["terminal_by_outcome"]["stalled"], 0)

        gate["terminal_by_outcome"]["stalled"] = 1
        gate["terminal_task_count"] = 1
        public = dashboard_projection_module._public_canary_gate(
            gate, subject="math", release_id="a" * 64
        )
        self.assertEqual(public["terminal_by_outcome"]["stalled"], 1)

    def test_execution_success_keeps_quality_review_grounding_independent(self):
        gate = self.gate()
        gate["state"] = "continuous_concurrent_unlocked"
        gate["unlocked_once"] = True
        gate["last_read_session_id"] = "MCPRS-MATH-EXACT"
        gate["last_evidence_generation"] = "math-exact-generation"
        gate["last_evidence_authority_fingerprint"] = "9" * 64
        gate["last_analysis_grounding_manifest_sha256"] = "a" * 64
        gate["last_analysis_evidence_refs"] = ["b" * 64]
        gate["last_critical_review_grounding_manifest_sha256"] = None
        gate["last_critical_review_evidence_refs"] = []

        public = dashboard_projection_module._public_canary_gate(
            gate, subject="math", release_id="a" * 64
        )

        self.assertEqual(
            public["last_analysis_evidence_refs"],
            [f"mcp-item:math:{'b' * 64}"],
        )
        self.assertEqual(public["last_critical_review_evidence_refs"], [])

    def test_exact_math_migration_pending_projects_before_first_claim(self):
        gate = self.gate()
        gate["state"] = "continuous_concurrent_unlocked"
        gate["unlocked_once"] = True
        gate["queue_depth"] = 4
        gate["oldest_pending_age_seconds"] = 0
        gate["next_action"] = "consume_pending_post_activation_queue"

        public = dashboard_projection_module._public_canary_gate(
            gate, subject="math", release_id="a" * 64
        )

        self.assertEqual(public["state"], "continuous_concurrent_unlocked")
        self.assertEqual(public["queue_depth"], 4)
        self.assertEqual(public["last_analysis_evidence_refs"], [])

        selection = {
            "producer_unit_id": "MFI-CAP-EXACT-MIGRATION",
            "producer_recorded_at": "2026-08-15T00:00:00+00:00",
            "producer_input_contract_sha256": "1" * 64,
            "source_event_set_sha256": "2" * 64,
            "unit_sha256": "3" * 64,
            "frozen_payload_sha256": "4" * 64,
        }
        gate["queue_depth"] = 3
        gate["active_task_count"] = 1
        gate["active_selections"] = {"3" * 64: selection}
        gate["last_selected"] = selection
        public = dashboard_projection_module._public_canary_gate(
            gate, subject="math", release_id="a" * 64
        )
        self.assertEqual(public["queue_depth"], 3)
        self.assertEqual(public["active_task_count"], 1)

        gate["terminal_task_count"] = 1
        gate["terminal_by_outcome"]["failed"] = 1
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_success_grounding_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                gate, subject="math", release_id="a" * 64
            )

    def test_canary_cross_release_fails_closed(self):
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_state_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                self.gate(), subject="math", release_id="b" * 64
            )

    def test_live_v2_requires_explicit_non_fast_counter_scope(self):
        old_fast = self.gate()
        old_fast["fast_mode_requested"] = True
        old_fast["fast_mode_effective"] = "requested_unverified"
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_state_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                old_fast, subject="math", release_id="a" * 64
            )

        missing_tier = self.gate()
        missing_tier.pop("requested_service_tier")
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_state_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                missing_tier, subject="math", release_id="a" * 64
            )

        missing_scope = self.gate()
        missing_scope.pop("observability_counter_scope")
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_state_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                missing_scope, subject="math", release_id="a" * 64
            )

    def test_emergency_cancel_fence_is_public_and_must_be_closed(self):
        sealed = self.gate()
        sealed["last_emergency_cancel_at"] = "2026-08-11T04:01:00+00:00"
        sealed["last_emergency_cancel_receipt_sha256"] = "8" * 64
        sealed["late_result_fence_status"] = "sealed"
        public = dashboard_projection_module._public_canary_gate(
            sealed, subject="math", release_id="a" * 64
        )
        self.assertEqual(public["late_result_fence_status"], "sealed")
        self.assertEqual(
            public["last_emergency_cancel_receipt_sha256"], "8" * 64
        )

        incomplete = self.gate()
        incomplete["late_result_fence_status"] = "sealed"
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_late_fence_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                incomplete, subject="math", release_id="a" * 64
            )

    def test_preclaim_failure_closure_is_public_and_partial_state_rejected(self):
        failed = self.gate()
        failed.update(
            {
                "state": "failed_drained",
                "luna_consumer_enabled": False,
                "last_failure_at": "2026-08-11T04:02:00+00:00",
                "last_preclaim_failure_at": "2026-08-11T04:02:00+00:00",
                "last_preclaim_failure_stage": "pre_claim",
                "last_preclaim_failure_error_code": "generation_fence_failed",
                "last_preclaim_failure_receipt_sha256": "9" * 64,
                "last_preclaim_failure_evidence_sha256": "a" * 64,
                "blocking_reason": "generation_fence_failed",
                "next_action": "explicit_subject_resume_required",
            }
        )
        public = dashboard_projection_module._public_canary_gate(
            failed, subject="math", release_id="a" * 64
        )
        self.assertEqual(public["last_preclaim_failure_stage"], "pre_claim")
        self.assertEqual(
            public["last_preclaim_failure_receipt_sha256"], "9" * 64
        )

        partial = self.gate()
        partial["last_preclaim_failure_stage"] = "pre_claim"
        with self.assertRaisesRegex(
            dashboard_projection_module.DashboardProjectionError,
            "production_canary_preclaim_invalid",
        ):
            dashboard_projection_module._public_canary_gate(
                partial, subject="math", release_id="a" * 64
            )

class _RateLimitedRunner(_SuccessRunner):
    def run_analysis(self, _task, _context):
        raise DispatchError("luna_rate_limited")


def _task(
    subject: str,
    capture_id: str,
    release_id: str,
    *,
    with_visual_roles: bool = False,
) -> FrozenTask:
    model_input = {"bounded_test": True}
    if with_visual_roles:
        model_input["current_question_evidence"] = {
            "question_mode": "image_question",
            "attachment_objects": [
                {"ordinal": 1, "role": "question_image"},
                {"ordinal": 2, "role": "solution_image"},
                {"ordinal": 3, "role": "solution_image"},
            ],
        }
    return FrozenTask(
        {
            "subject": subject,
            "capture_id": capture_id,
            "study_date": "2026-08-05",
            "input_fingerprint": hashlib.sha256(capture_id.encode()).hexdigest(),
            "input_binding": {"capture_id": capture_id},
            "model_input": model_input,
            "allowed_evidence_refs": [],
            "image_paths": [],
            "dispatch_contract": {
                "schema_version": "study-intake-dispatch-release-binding-v1",
                "release_id": release_id,
            },
        }
    )


def _decision(task: FrozenTask, release_id: str, *, eligible: bool = True) -> dict:
    payload = task.frozen_payload
    return {
        "subject": payload["subject"],
        "capture_id": payload["capture_id"],
        "target_label": f"真实标签 {payload['capture_id']}",
        "study_date": "2026-08-05",
        "input_fingerprint": payload["input_fingerprint"],
        "eligible": eligible,
        "reason": "integration_fixture",
        "unit_sha256": task.unit_sha256,
        "frozen_payload_sha256": task.frozen_payload_sha256,
        "release_id": release_id,
        "rule_version": "study-intake-concurrent-dispatch-contract-v1",
        "phase": "frozen_evidence",
        "model": REQUIRED_MODEL,
        "reasoning_effort": REQUIRED_REASONING_EFFORT,
        "model_enqueue_allowed": eligible,
    }


class DashboardProjectionMergeIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="dashboard-merge-integration-")
        self.runtime = Path(self.temp.name) / "runtime"
        self.release_id = "a" * 64
        self.projection_path = self.runtime / "state" / "dashboard_projection.json"
        self.config = {
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "dashboard": {
                "projection_path": str(self.projection_path),
            },
        }
        self.block_release = threading.Event()
        self.blocked_dispatcher = None
        self.blocked_handle = None

    def tearDown(self) -> None:
        self.block_release.set()
        if self.blocked_handle is not None:
            self.blocked_handle.wait(5)
        if self.blocked_dispatcher is not None:
            self.blocked_dispatcher.drain(5)
        self.temp.cleanup()

    def _en_p0_work_item_batch(self, target_count: int = 98) -> tuple[Path, str]:
        rows = [
            {
                "ordinal": ordinal,
                "target_id": f"master_bank_row:TARGET-{ordinal:03d}",
                "work_item_sha256": f"{ordinal:064x}",
                "work_item_path": f"/candidate/work-items/{ordinal:03d}.json",
            }
            for ordinal in range(1, target_count + 1)
        ]
        inventory_sha = "1" * 64
        target_set_sha = "2" * 64
        authorization_sha = "3" * 64
        closure_sha = "4" * 64
        batch = {
            "schema_version": "english_legacy_recuration_work_item_batch_v1",
            "remediation_gate": {
                "schema_version": "en_p0_006_remediation_gate_v1",
                "issue_id": "EN-P0-006",
                "subject": "english",
                "status": "authorization_ready_execution_pending",
                "authorization_gate": "passed",
                "execution_gate": "pending",
                "decision": "allow_en_p0_006_luna_recuration_only",
                "allowed_lanes": ["en_p0_006_luna_recuration"],
                "blocked_lanes": [
                    "ordinary_english_luna",
                    "english_golden_replay",
                    "english_sol_apply_before_quality_ready",
                ],
                "inventory_sha256": inventory_sha,
                "target_set_sha256": target_set_sha,
                "target_count": target_count,
                "batch_authorization_sha256": authorization_sha,
                "authorization_expansion_closure_sha256": closure_sha,
                "model_call_count": 0,
                "formal_write_count": 0,
            },
            "authorization_expansion_closure_sha256": closure_sha,
            "batch_authorization_sha256": authorization_sha,
            "inventory_sha256": inventory_sha,
            "target_set_sha256": target_set_sha,
            "authority": {
                "generation": "english-legacy-en-p0-006-write-set-v3",
                "authority_fingerprint": "5" * 64,
            },
            "target_count": target_count,
            "work_items": rows,
            "model_call_count": 0,
            "formal_write_count": 0,
            "created_at": "2026-08-09T07:51:31+00:00",
        }
        raw = dashboard_projection_module._canonical_bytes(batch)
        digest = hashlib.sha256(raw).hexdigest()
        path = self.runtime / "candidate-authority" / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return path, digest

    def test_en_p0_projection_uses_explicit_no_data_instead_of_fake_zero(self) -> None:
        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
        )
        self.assertEqual(projection["en_p0_006_status"], "no_data")
        section = projection["en_p0_006"]
        self.assertFalse(section["state_available"])
        self.assertIsNone(section["inventory"]["target_count"])
        self.assertIsNone(section["luna"]["selected_count"])
        self.assertIsNone(section["sol"]["committed_count"])

    def test_en_p0_projection_reads_pinned_98_target_batch_without_private_items(self) -> None:
        batch_path, digest = self._en_p0_work_item_batch()
        self.config["dashboard"]["en_p0_006"] = {
            "work_item_batch_path": str(batch_path),
            "work_item_batch_sha256": digest,
        }
        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
        )
        section = projection["en_p0_006"]
        self.assertEqual(section["status"], "authorization_ready")
        self.assertEqual(section["inventory"]["target_count"], 98)
        self.assertEqual(
            section["inventory"]["source"],
            "candidate_bound_work_item_batch",
        )
        self.assertEqual(section["luna"]["status"], "no_data")
        self.assertIsNone(section["luna"]["selected_count"])
        self.assertIsNone(section["luna"]["remaining_count"])
        self.assertFalse(section["sol"]["state_available"])
        self.assertNotIn("work_items", section)
        self.assertIsNone(dashboard._projection_contract_error(projection))

    def test_en_p0_projection_accepts_exact_hash_pinned_pretty_json_batch(self) -> None:
        batch_path, _ = self._en_p0_work_item_batch()
        batch = json.loads(batch_path.read_text(encoding="utf-8"))
        pretty = (json.dumps(batch, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        digest = hashlib.sha256(pretty).hexdigest()
        pretty_path = self.runtime / "candidate-authority" / f"{digest}.json"
        pretty_path.write_bytes(pretty)
        self.config["dashboard"]["en_p0_006"] = {
            "work_item_batch_path": str(pretty_path),
            "work_item_batch_sha256": digest,
        }

        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
        )

        self.assertEqual(projection["en_p0_006_status"], "authorization_ready")
        self.assertEqual(projection["en_p0_006"]["inventory"]["target_count"], 98)
        self.assertEqual(projection["en_p0_006"]["luna"]["status"], "no_data")
        self.assertIsNone(dashboard._projection_contract_error(projection))

    def test_en_p0_pinned_batch_tamper_fails_closed_without_counts(self) -> None:
        batch_path, digest = self._en_p0_work_item_batch()
        self.config["dashboard"]["en_p0_006"] = {
            "work_item_batch_path": str(batch_path),
            "work_item_batch_sha256": digest,
        }
        batch_path.write_text("{}\n", encoding="utf-8")
        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
        )
        section = projection["en_p0_006"]
        self.assertEqual(section["status"], "invalid")
        self.assertEqual(
            section["blocker_codes"], ["en_p0_006_work_item_batch_invalid"]
        )
        self.assertIsNone(section["inventory"]["target_count"])
        self.assertIsNone(section["luna"]["selected_count"])
        self.assertIsNone(dashboard._projection_contract_error(projection))

    def test_en_p0_luna_counts_reconcile_claim_quality_and_signed_failure(self) -> None:
        batch_path, digest = self._en_p0_work_item_batch(target_count=3)
        key = b"k" * 32
        key_path = self.runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(key)
        self.config["processing_plugin"] = {
            "authority_key_path": str(key_path),
        }
        self.config["dashboard"]["en_p0_006"] = {
            "work_item_batch_path": str(batch_path),
            "work_item_batch_sha256": digest,
        }
        base = self.runtime / "dispatch/english-legacy-recuration"
        batch = json.loads(batch_path.read_text(encoding="utf-8"))
        for row in batch["work_items"]:
            claim = {
                "schema_version": "english_legacy_recuration_attempt_claim_v1",
                "work_item_sha256": row["work_item_sha256"],
                "target_id": row["target_id"],
                "attempt": 1,
                "claimed_at": "2026-08-09T08:00:00+00:00",
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            claim_path = base / "attempt-claims" / f"{row['work_item_sha256']}.json"
            claim_path.parent.mkdir(parents=True, exist_ok=True)
            claim_path.write_bytes(dashboard_projection_module._canonical_bytes(claim))

        package = {"schema_version": "test-safe-package-v1"}
        package_raw = dashboard_projection_module._canonical_bytes(package)
        package_sha = hashlib.sha256(package_raw).hexdigest()
        package_path = base / "packages/sha256" / package_sha[:2] / f"{package_sha}.json"
        package_path.parent.mkdir(parents=True, exist_ok=True)
        package_path.write_bytes(package_raw)
        first = batch["work_items"][0]
        quality_core = {
            "schema_version": "english_legacy_recuration_quality_receipt_v1",
            "work_item_sha256": first["work_item_sha256"],
            "target_id": first["target_id"],
            "target_kind": "master_bank_row",
            "ordinal": 1,
            "attempt": 1,
            "inventory_sha256": batch["inventory_sha256"],
            "batch_authorization_sha256": batch["batch_authorization_sha256"],
            "target_authorization_receipt_sha256": "6" * 64,
            "authority": batch["authority"],
            "analysis": {},
            "critical_review": {},
            "read_session_receipt_sha256": "7" * 64,
            "proposal_action": "already_current_proposal",
            "proposal_sha256": "8" * 64,
            "package_sha256": package_sha,
            "quality_outcome": "accepted",
            "model_call_count": 2,
            "formal_write_count": 0,
            "issued_at": "2026-08-09T08:01:00+00:00",
        }
        quality = {
            **quality_core,
            "hmac_key_id": hashlib.sha256(key).hexdigest(),
            "hmac_sha256": hmac.new(
                key,
                dashboard_projection_module._canonical_bytes({
                    "purpose": "english_legacy_recuration_quality_receipt_v1",
                    "payload": quality_core,
                }) + b"\n",
                hashlib.sha256,
            ).hexdigest(),
        }
        quality_raw = dashboard_projection_module._canonical_bytes(quality)
        quality_sha = hashlib.sha256(quality_raw).hexdigest()
        quality_path = (
            base / "quality-receipts/sha256" / quality_sha[:2]
            / f"{quality_sha}.json"
        )
        quality_path.parent.mkdir(parents=True, exist_ok=True)
        quality_path.write_bytes(quality_raw)
        binding = {
            "schema_version": "english_legacy_recuration_completion_binding_v1",
            "work_item_sha256": first["work_item_sha256"],
            "package_sha256": package_sha,
            "quality_receipt_sha256": quality_sha,
            "formal_write_count": 0,
        }
        binding_path = base / "completion-bindings" / f"{first['work_item_sha256']}.json"
        binding_path.parent.mkdir(parents=True, exist_ok=True)
        binding_path.write_bytes(dashboard_projection_module._canonical_bytes(binding))

        def concurrency_attestation(
            ordinal: int,
            target_id: str,
            *,
            passed: bool,
            minute: int,
        ) -> dict:
            barrier_at = f"2026-08-09T08:{minute:02d}:00+00:00"
            released_at = f"2026-08-09T08:{minute:02d}:00.500000+00:00"
            submitted_at = (
                f"2026-08-09T08:{minute:02d}:01+00:00" if passed else None
            )
            stage_events = (
                [
                    {
                        "name": name,
                        "observed_at": f"2026-08-09T08:{minute:02d}:{second:02d}+00:00",
                    }
                    for second, name in enumerate(
                        (
                            "analysis_submitted",
                            "analysis_completed",
                            "critical_review_submitted",
                            "critical_review_completed",
                        ),
                        start=1,
                    )
                ]
                if passed
                else []
            )
            return {
                "schema_version": "english_legacy_recuration_concurrency_attestation_v1",
                "run_mode": "smoke_1",
                "max_workers": 1,
                "barrier_timeout_seconds": 30,
                "barrier_expected": 1,
                "barrier_arrived": 1,
                "barrier_released_at": released_at,
                "submitted_at_first": submitted_at,
                "submitted_at_last": submitted_at,
                "submitted_at_spread_ms": 0 if passed else None,
                "peak_active": 1 if passed else 0,
                "item_traces": [{
                    "ordinal": ordinal,
                    "target_id": target_id,
                    "barrier_arrived_at": barrier_at,
                    "submitted_at": submitted_at,
                    "stage_events": stage_events,
                    "strict_stage_order_verified": passed,
                    "exact_two_calls_verified": passed,
                }],
                "strict_stage_order_count": 1 if passed else 0,
                "exact_two_call_count": 1 if passed else 0,
                "smoke_prerequisites": {
                    "smoke_1_summary_sha256": None,
                    "smoke_10_summary_sha256": None,
                },
                "gate_status": "passed" if passed else "failed_closed",
                "failure_code": None if passed else "luna_service_unavailable",
            }

        common_summary = {
            "work_item_batch_sha256": digest,
            "authorization_expansion_closure_sha256": batch[
                "authorization_expansion_closure_sha256"
            ],
            "batch_authorization_sha256": batch["batch_authorization_sha256"],
            "inventory_sha256": batch["inventory_sha256"],
            "target_set_sha256": batch["target_set_sha256"],
            "attempt": 1,
            "batch_target_count": 3,
        }
        publish_run_summary(
            self.config,
            self.runtime,
            **common_summary,
            selected_ordinals=[1],
            results=[{
                "ordinal": 1,
                "target_id": first["target_id"],
                "status": "succeeded",
                "error_code": None,
                "package_sha256": package_sha,
                "quality_receipt_sha256": quality_sha,
                "failure_receipt_sha256": None,
                "model_call_count": 2,
                "formal_write_count": 0,
            }],
            concurrency_attestation=concurrency_attestation(
                1, first["target_id"], passed=True, minute=0
            ),
            started_at="2026-08-09T08:00:00+00:00",
            completed_at="2026-08-09T08:00:10+00:00",
        )

        third = batch["work_items"][2]
        publish_run_summary(
            self.config,
            self.runtime,
            **common_summary,
            selected_ordinals=[3],
            results=[{
                "ordinal": 3,
                "target_id": third["target_id"],
                "status": "failed",
                "error_code": "luna_service_unavailable",
                "package_sha256": None,
                "quality_receipt_sha256": None,
                "failure_receipt_sha256": None,
                "model_call_count": 0,
                "formal_write_count": 0,
            }],
            concurrency_attestation=concurrency_attestation(
                3, third["target_id"], passed=False, minute=10
            ),
            started_at="2026-08-09T08:10:00+00:00",
            completed_at="2026-08-09T08:10:10+00:00",
        )

        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
        )
        luna = projection["en_p0_006"]["luna"]
        self.assertEqual(projection["en_p0_006_status"], "luna_running")
        self.assertEqual(luna["selected_count"], 3)
        self.assertEqual(luna["running_count"], 1)
        self.assertEqual(luna["terminal_count"], 2)
        self.assertEqual(luna["quality_passed_count"], 1)
        self.assertEqual(luna["failed_count"], 1)
        self.assertEqual(luna["remaining_count"], 1)
        self.assertIsNone(dashboard._projection_contract_error(projection))

    @staticmethod
    def _lease_status(active: int = 0, completed: int = 0) -> dict:
        return {
            "active_count": active,
            "stale_count": 0,
            "completed_count": completed,
            "max_fence": 1,
            "draining": False,
            "heartbeat_interval_seconds": 15,
            "lease_ttl_seconds": 120,
        }

    def _seal_dispatch_object(self, value: dict, *, purpose: str) -> dict:
        key_path = self.runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        if not key_path.exists():
            key_path.write_bytes(b"k" * 32)
            key_path.chmod(0o600)
        key = key_path.read_bytes()
        core = dict(value)
        core.pop("authority", None)
        return {
            **core,
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "purpose": purpose,
                "hmac_sha256": hmac.new(
                    key,
                    dashboard_projection_module._canonical_bytes(
                        {"purpose": purpose, "payload": core}
                    ).rstrip(b"\n"),
                    hashlib.sha256,
                ).hexdigest(),
            },
        }

    def _publish_preclaim_attempt(
        self,
        *,
        activation_id: str,
        unit_sha256: str,
        frozen_payload_sha256: str,
        producer_contract_sha256: str,
        producer_unit_id: str,
        source_event_set_sha256: str,
        failure_stage: str,
        error_code: str,
        failed_at: str,
        preserved: bool,
    ) -> tuple[dict, str]:
        receipt = self._seal_dispatch_object(
            {
                "schema_version": "study-intake-production-canary-preclaim-failure-receipt-v2",
                "failure_id": hashlib.sha256(
                    f"{failure_stage}:{error_code}:{unit_sha256}".encode()
                ).hexdigest(),
                "activation_id": activation_id,
                "subject": "english",
                "release_id": self.release_id,
                "producer_authority_fingerprint": "2" * 64,
                "producer_high_watermark_sha256": "3" * 64,
                "activation_receipt_sha256": "4" * 64,
                "activation_gate_authority_sha256": "5" * 64,
                "failure_stage": failure_stage,
                "error_code": error_code,
                "failure_evidence": {
                    "capture_id": producer_unit_id,
                    "task_subject": "english",
                    "unit_sha256": unit_sha256,
                    "frozen_payload_sha256": frozen_payload_sha256,
                    "producer_unit_id": producer_unit_id,
                    "producer_recorded_at": "2026-08-05T08:00:00+00:00",
                    "producer_input_contract_sha256": producer_contract_sha256,
                    "source_event_set_sha256": source_event_set_sha256,
                    "queue_entry_sha256": "6" * 64 if preserved else None,
                    "queue_status_before": "pending" if preserved else None,
                    "canary_gate_sha256": None,
                    "lease_fence": None,
                    "evidence_sha256": "7" * 64,
                },
                "queue_entry_preserved": preserved,
                "queue_status_after": "pending" if preserved else None,
                "model_submission_started": False,
                "failed_at": failed_at,
                "state_after": "failed_drained",
                "producer_capture_enabled": True,
                "luna_consumer_enabled": False,
                "post_activation_only": True,
                "initial_canary_inflight_limit": 1,
                "continuous_concurrency_limit": 20,
                "keep_backlog_drained": True,
                "production_accepted": False,
                "fast_mode_requested": False,
                "fast_mode_effective": "not_requested",
                "requested_service_tier": None,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "sol_formal_curation_enabled": False,
            },
            purpose="dispatch-production-canary-preclaim-failure",
        )
        raw = dashboard_projection_module._canonical_bytes(receipt)
        return receipt, hashlib.sha256(raw).hexdigest()

    def _get(self, port: int, path: str) -> tuple[int, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=4)
        connection.request("GET", path, headers={"Host": "127.0.0.1:8767"})
        response = connection.getresponse()
        body = json.loads(response.read().decode("utf-8"))
        connection.close()
        return response.status, body

    def test_math_six_stage_projection_is_hash_verified_and_failure_is_isolated(self) -> None:
        capture_id = "MATH-PIPELINE-001"
        source_set_sha = "1" * 64
        snapshot_core = {
            "schema_version": "study-intake-math-knowledge-distribution-snapshot-v1",
            "source_fingerprints": {},
            "source_set_sha256": source_set_sha,
            "corpus_stats": {"card_count": 37},
            "formal_write_count": 0,
        }
        snapshot = {
            **snapshot_core,
            "snapshot_sha256": dashboard._content_value_sha256(snapshot_core),
        }
        relationship_core = {
            "schema_version": "study-intake-math-relationship-context-snapshot-v1",
            "source_set_sha256": source_set_sha,
            "knowledge_snapshot_sha256": snapshot["snapshot_sha256"],
            "analysis_sha256": "2" * 64,
            "policy": {"mode": "SHADOW", "maximum_candidates": 5},
            "candidates": [
                {"candidate_id": "GS-002"},
                {"candidate_id": "GS-003"},
            ],
            "formal_write_count": 0,
        }
        relationship = {
            **relationship_core,
            "relationship_context_sha256": dashboard._content_value_sha256(
                relationship_core
            ),
        }
        report = {
            "schema_version": "study-intake-luna-math-candidate-v3",
            "capture_id": capture_id,
            "study_date": "2026-08-05",
            "source_binding": {
                "knowledge_snapshot_sha256": snapshot["snapshot_sha256"],
                "knowledge_source_set_sha256": source_set_sha,
                "relationship_context_sha256": relationship[
                    "relationship_context_sha256"
                ],
            },
            "evidence_inventory": {
                "source_kind": "old_existing",
                "artifacts": [{"role": "question"}, {"role": "solution"}],
                "text_sources": [{"role": "solution_text"}],
            },
            "knowledge_distribution_snapshot": snapshot,
            "knowledge_and_error_extraction": {
                "primary_knowledge": ["高阶导数"],
                "methods": ["拆项"],
                "first_error": "链式求导系数遗漏",
            },
            "relationship_context": relationship,
            "relationship_decisions": [
                {"candidate_id": "GS-002"}, {"candidate_id": "GS-003"}
            ],
            "stage_receipts": {
                "analysis": {"runtime_identity_status": "requested_unverified"},
                "critical_review": {"runtime_identity_status": "requested_unverified"},
            },
            "relationship_mode": "SHADOW",
            "formal_write_count": 0,
        }
        report_path = self.runtime / "private" / "reports" / "objects" / "pending.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_bytes = (
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")
        report_sha = hashlib.sha256(report_bytes).hexdigest()
        report_path = report_path.with_name(f"{report_sha}.json")
        report_path.write_bytes(report_bytes)
        quality = {
            "candidate_schema_version": "study-intake-luna-math-candidate-v3",
            "knowledge_snapshot_sha256": snapshot["snapshot_sha256"],
            "knowledge_source_set_sha256": source_set_sha,
            "relationship_context_sha256": relationship[
                "relationship_context_sha256"
            ],
            "formal_write_count": 0,
        }
        core_package = {
            "schema_version": "study-intake-preprocess-package-v2",
            "subject": "math",
            "capture_id": capture_id,
            "report_json_sha256": report_sha,
            "quality_receipt": quality,
            "quality_receipt_sha256": dashboard._content_value_sha256(quality),
            "formal_write_count": 0,
        }
        package_bytes = (
            json.dumps(core_package, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n"
        ).encode("utf-8")
        package_sha = hashlib.sha256(package_bytes).hexdigest()
        package_path = (
            self.runtime
            / "shadow"
            / "packages"
            / "objects"
            / f"{package_sha}.json"
        )
        package_path.parent.mkdir(parents=True, exist_ok=True)
        package_path.write_bytes(package_bytes)
        pointer_path = (
            self.runtime
            / "shadow"
            / "state"
            / "latest"
            / "math"
            / f"{capture_id}.json"
        )
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        pointer_path.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-math-shadow-latest-v1",
                    "package_sha256": package_sha,
                    "report_json_sha256": report_sha,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        summary = dashboard._math_pipeline_summary(
            self.runtime, capture_id, None, phase="complete"
        )
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["source_type"], "old_existing")
        self.assertEqual(summary["candidate_file_validation"], "content_hash_verified")
        self.assertEqual(
            [row["stage"] for row in summary["stages"]],
            [
                "evidence_assembly",
                "knowledge_snapshot",
                "knowledge_error_extraction",
                "relationship_retrieval",
                "independent_review",
                "sol_candidate",
            ],
        )
        self.assertEqual(summary["stages"][3]["item_count"], 2)
        self.assertEqual(summary["formal_write_count"], 0)

        report_path.write_bytes(report_bytes + b" ")
        failed = dashboard._math_pipeline_summary(
            self.runtime, capture_id, None, phase="complete"
        )
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error_code"], "math_pipeline_candidate_hash_invalid")
        self.assertEqual(len(failed["stages"]), 6)

    def test_math_pipeline_rejects_old_fingerprint_pointer(self) -> None:
        capture_id = "GS-240"
        pointer_path = (
            self.runtime
            / "shadow/state/latest/math"
            / f"{capture_id}.json"
        )
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        pointer_path.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-math-shadow-latest-v1",
                    "capture_id": capture_id,
                    "input_fingerprint": "old-fingerprint",
                    "processing_contract_sha256": "4" * 64,
                    "authority_release_id": self.release_id,
                    "package_sha256": "5" * 64,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        summary = dashboard._math_pipeline_summary(
            self.runtime,
            capture_id,
            None,
            phase="failed",
            failure_error_code="math_latex_gate_failed",
            expected_input_fingerprint="current-fingerprint",
            expected_processing_contract_sha256="4" * 64,
            expected_release_id=self.release_id,
        )
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(
            summary["error_code"],
            "math_pipeline_pointer_generation_mismatch",
        )
        statuses = [row["status"] for row in summary["stages"]]
        self.assertNotIn("completed", statuses[4:])

    def test_disabled_drained_dispatcher_preserves_processing_error(self) -> None:
        task = _task("math", "GS-240", self.release_id)
        decision = _decision(task, self.release_id, eligible=False)
        decision["error_code"] = "math_latex_gate_failed"
        projection = update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="drained",
            eligible_count=0,
            submitted_count=0,
            decisions=[decision],
            lease_status={**self._lease_status(), "draining": True},
            error_code="math_latex_gate_failed",
            control_reason="release_hold",
            updated_at="2026-08-05T09:00:00+00:00",
        )
        dispatcher = projection["dispatchers"]["math"]
        subject = projection["subjects"]["math"]
        self.assertFalse(dispatcher["enabled"])
        self.assertEqual(dispatcher["status"], "drained")
        self.assertEqual(dispatcher["control_state"], "drained")
        self.assertFalse(subject["enabled"])
        self.assertEqual(
            subject["processing_error_code"], "math_latex_gate_failed"
        )
        self.assertEqual(subject["control_reason"], "release_hold")

    def test_preserved_preclaim_task_is_primary_and_replays_are_attempt_history(self) -> None:
        self.config["dashboard"]["projection_schema_version"] = (
            dashboard_projection_module.MAIN_SCHEMA
        )
        activation_id = "1" * 64
        capture_id = "EN-PRESERVED-001"
        source_event_set = "8" * 64
        original = _task("english", capture_id, self.release_id)
        producer_contract_sha = "9" * 64
        task_raw = dashboard_projection_module._canonical_bytes(original.as_dict())
        task_sha = hashlib.sha256(task_raw).hexdigest()
        task_path = (
            self.runtime
            / "dispatch/production-canary/tasks/english/sha256"
            / task_sha[:2]
            / f"{task_sha}.json"
        )
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_bytes(task_raw)

        primary, primary_sha = self._publish_preclaim_attempt(
            activation_id=activation_id,
            unit_sha256=original.unit_sha256,
            frozen_payload_sha256=original.frozen_payload_sha256,
            producer_contract_sha256=producer_contract_sha,
            producer_unit_id=capture_id,
            source_event_set_sha256=source_event_set,
            failure_stage="pre_claim",
            error_code="subject_luna_batch_already_current",
            failed_at="2026-08-05T08:01:00+00:00",
            preserved=True,
        )
        replay_task = _task("english", capture_id, self.release_id)
        replay, replay_sha = self._publish_preclaim_attempt(
            activation_id=activation_id,
            unit_sha256=replay_task.unit_sha256,
            frozen_payload_sha256=replay_task.frozen_payload_sha256,
            producer_contract_sha256="a" * 64,
            producer_unit_id=capture_id,
            source_event_set_sha256=source_event_set,
            failure_stage="materialize",
            error_code="production_canary_producer_replay",
            failed_at="2026-08-05T08:02:00+00:00",
            preserved=False,
        )
        state_attempt_root = (
            self.runtime
            / "dispatch/state/production-canary-preclaim-failures/english"
            / activation_id
        )
        state_attempt_root.mkdir(parents=True, exist_ok=True)
        for receipt, receipt_sha in ((primary, primary_sha), (replay, replay_sha)):
            (state_attempt_root / f"{receipt['failure_id']}.json").write_bytes(
                dashboard_projection_module._canonical_bytes(receipt)
            )
        receipt_path = (
            self.runtime
            / "dispatch/production-canary/receipts/english"
            / activation_id
            / "sha256"
            / primary_sha[:2]
            / f"{primary_sha}.json"
        )
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(dashboard_projection_module._canonical_bytes(primary))
        queue = self._seal_dispatch_object(
            {
                "schema_version": "study-intake-production-canary-queue-entry-v2",
                "activation_id": activation_id,
                "subject": "english",
                "release_id": self.release_id,
                "producer_high_watermark_sha256": "3" * 64,
                "producer_authority_fingerprint": "2" * 64,
                "producer_input_contract_sha256": producer_contract_sha,
                "producer_unit_id": capture_id,
                "producer_recorded_at": "2026-08-05T08:00:00+00:00",
                "source_event_set_sha256": source_event_set,
                "source_event_ids": ["EVENT-001"],
                "unit_sha256": original.unit_sha256,
                "frozen_payload_sha256": original.frozen_payload_sha256,
                "task_object_sha256": task_sha,
                "task_object_path": str(task_path),
                "queue_status": "pending",
                "discovered_at": "2026-08-05T08:00:01+00:00",
                "claimed_at": None,
                "finished_at": None,
                "canary_gate_sha256": None,
                "canary_gate_path": None,
                "canary_gate_authority_sha256": None,
                "lease_owner_id": None,
                "lease_fence": None,
                "context_root": None,
                "mcp_session_root": None,
                "report_root": None,
                "process_identity_sha256": None,
                "process_identity_path": None,
                "terminal_receipt_sha256": primary_sha,
                "terminal_receipt_path": str(receipt_path),
                "terminal_outcome": "failed",
                "terminal_error_code": "subject_luna_batch_already_current",
                "formal_write_count": 0,
            },
            purpose="dispatch-production-canary-queue",
        )
        queue_path = (
            self.runtime
            / "dispatch/state/production-canary-queue/english"
            / activation_id
            / f"{producer_contract_sha}.json"
        )
        queue_path.parent.mkdir(parents=True, exist_ok=True)
        queue_path.write_bytes(dashboard_projection_module._canonical_bytes(queue))

        gate = ProductionCanaryProjectionTests.gate()
        gate.update(
            {
                "subject": "english",
                "release_id": self.release_id,
                "activation_id": activation_id,
                "state": "failed_drained",
                "luna_consumer_enabled": False,
                "queue_depth": 1,
                "oldest_pending_age_seconds": 1,
                "last_failure_at": "2026-08-05T08:02:00+00:00",
                "last_preclaim_failure_at": "2026-08-05T08:02:00+00:00",
                "last_preclaim_failure_stage": "materialize",
                "last_preclaim_failure_error_code": "production_canary_producer_replay",
                "last_preclaim_failure_receipt_sha256": replay_sha,
                "last_preclaim_failure_evidence_sha256": "b" * 64,
                "blocking_reason": "production_canary_producer_replay",
                "backpressure_reason": "luna_consumer_disabled",
                "next_action": "explicit_subject_resume_required",
            }
        )
        later = _decision(replay_task, self.release_id, eligible=False)
        later.update(
            {
                "error_code": "production_canary_producer_replay",
                "updated_at": "2026-08-05T08:02:00+00:00",
            }
        )
        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[later],
            lease_status={**self._lease_status(), "canary_gate": gate},
            updated_at="2026-08-05T08:03:00+00:00",
        )
        item = projection["subjects"]["english"]["items"][0]
        self.assertEqual(item["unit_sha256"], original.unit_sha256)
        self.assertEqual(item["exact_error_code"], "subject_luna_batch_already_current")
        self.assertEqual(item["local_dispatch_status"], "terminal")
        self.assertEqual(item["model_stage"], "not_started")
        self.assertFalse(item["local_model_submitted"])
        self.assertTrue(item["queue_preserved_for_recovery"])
        self.assertEqual(item["preclaim_attempt_count"], 2)
        self.assertTrue(item["preclaim_attempt_history"][0]["primary"])
        self.assertEqual(
            item["preclaim_attempt_history"][1]["error_code"],
            "production_canary_producer_replay",
        )
        partition = projection["subjects"]["english"]["batch_partition"]
        self.assertEqual(partition["outside_batch_pending_task_count"], 0)
        self.assertEqual(partition["outside_batch_failed_preserved_task_count"], 1)
        # One-subject integration fixture intentionally lacks canary gates for
        # math and 408; task-level v4 validation is exercised separately.

    def test_previous_release_heartbeat_is_explicitly_separated(self) -> None:
        manifest_path = self.runtime / "release.json"
        dashboard_projection_module._atomic_json(
            manifest_path,
            {"release_id": self.release_id},
        )
        self.config["release"] = {"manifest_path": str(manifest_path)}
        heartbeat_path = (
            self.runtime
            / "dispatch/state/subject-projections/math.json"
        )
        dashboard_projection_module._atomic_json(
            heartbeat_path,
            {
                "schema_version": dashboard_projection_module.SUBJECT_SCHEMA,
                "subject": "math",
                "study_date": "2026-08-05",
                "daemon_status": "drained",
                "draining": True,
                "release_id": "b" * 64,
                "decisions": [],
                "canary_gate": {
                    **ProductionCanaryProjectionTests.gate(),
                    "release_id": "b" * 64,
                },
                "updated_at": "2026-08-05T09:00:00+00:00",
                "formal_write_count": 0,
            },
        )
        projection = dashboard_projection_module._main_projection(
            self.config,
            self.runtime,
            study_date="2026-08-05",
            generated_at="2026-08-05T09:01:00+00:00",
        )
        dispatcher = projection["dispatchers"]["math"]
        self.assertEqual(dispatcher["release_id"], self.release_id)
        self.assertEqual(dispatcher["active_release_id"], self.release_id)
        self.assertEqual(dispatcher["heartbeat_release_id"], "b" * 64)
        self.assertEqual(
            dispatcher["heartbeat_generation_status"],
            "previous_release_heartbeat",
        )
        self.assertTrue(dispatcher["previous_release_heartbeat"])
        self.assertIsNone(dispatcher["canary_gate"])
        self.assertIsNone(projection["subjects"]["math"]["canary_gate"])

    def test_math_legacy_source_types_project_as_canonical_routes(self) -> None:
        self.assertEqual(
            dashboard._canonical_math_source_type("existing_formal"),
            "old_existing",
        )
        self.assertEqual(
            dashboard._canonical_math_source_type("new_source"),
            "new_intake",
        )
        self.assertEqual(
            dashboard._canonical_math_source_type("unrecognized"),
            "unknown",
        )

    def test_math_failed_critical_review_is_projected_without_package(self) -> None:
        summary = dashboard._math_pipeline_summary(
            self.runtime,
            "MATH-FAILED-001",
            None,
            phase="failed",
            stage_statuses={
                "analysis": {"status": "completed"},
                "critical_review": {
                    "status": "failed",
                    "error_code": "math_critical_review_invalid_json_schema",
                },
            },
            failure_error_code="math_critical_review_invalid_json_schema",
        )
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(
            summary["error_code"], "math_critical_review_invalid_json_schema"
        )
        statuses = {row["stage"]: row["status"] for row in summary["stages"]}
        self.assertEqual(statuses["evidence_assembly"], "completed")
        self.assertEqual(statuses["model_mcp_investigation"], "completed")
        self.assertEqual(statuses["knowledge_error_extraction"], "completed")
        self.assertEqual(statuses["relationship_retrieval"], "completed")
        self.assertEqual(statuses["independent_review"], "failed")
        self.assertEqual(statuses["sol_candidate"], "pending")
        self.assertEqual(summary["formal_write_count"], 0)

    def test_historical_v1_projection_migrates_to_verified_v2_without_overwriting_current(self) -> None:
        current_task = _task("math", "MATH-CURRENT-001", self.release_id)
        update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=0,
            decisions=[_decision(current_task, self.release_id)],
            lease_status=self._lease_status(),
            updated_at="2026-08-05T09:00:00+00:00",
        )
        capture_id = "CAP-HISTORY-001"
        fingerprint = "1" * 64
        package_path = self.runtime / "packages" / "objects" / ("2" * 64 + ".json")
        package_path.parent.mkdir(parents=True, exist_ok=True)
        package_path.write_text('{"verified":true}\n', encoding="utf-8")
        package_sha = hashlib.sha256(package_path.read_bytes()).hexdigest()
        job = {
            "subject": "cs408",
            "capture_id": capture_id,
            "study_date": "2026-08-04",
            "input_fingerprint": fingerprint,
            "package_path": str(package_path),
            "package_sha256": package_sha,
        }
        latest = dict(job)
        for kind, value in (("jobs", job), ("latest", latest)):
            path = self.runtime / "state" / kind / "cs408" / f"{capture_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value) + "\n", encoding="utf-8")
        archive = self.runtime / "state" / "dashboard-projections" / "2026-08-04.json"
        archive.parent.mkdir(parents=True, exist_ok=True)
        legacy = {
            "schema_version": "study-intake-dashboard-projection-v1",
            "study_date": "2026-08-04",
            "generated_at": "2026-08-04T09:00:00+00:00",
            "subjects": {
                "cs408": {
                    "items": [
                        {
                            "subject": "cs408",
                            "capture_id": capture_id,
                            "study_date": "2026-08-04",
                            "input_fingerprint": fingerprint,
                            "package_sha256": package_sha,
                            "task_status": "two_pass_ready",
                            "luna_status": "two_pass_ready",
                            "runtime_identity_status": "requested_unverified",
                            "updated_at": "2026-08-04T09:00:00+00:00",
                        },
                        {
                            "subject": "cs408",
                            "capture_id": "CAP-HISTORY-UNBOUND",
                            "study_date": "2026-08-04",
                            "input_fingerprint": "3" * 64,
                            "package_sha256": "4" * 64,
                            "task_status": "two_pass_ready",
                            "luna_status": "two_pass_ready",
                            "updated_at": "2026-08-04T08:00:00+00:00",
                        },
                    ]
                }
            },
        }
        legacy_raw = (json.dumps(legacy, sort_keys=True) + "\n").encode()
        archive.write_bytes(legacy_raw)
        update_subject_and_main_projection(
            self.config,
            "cs408",
            study_date="2026-08-04",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
            updated_at="2026-08-06T02:00:00+00:00",
        )
        historical = json.loads(archive.read_text(encoding="utf-8"))
        self.assertEqual("study-intake-dashboard-projection-v3", historical["schema_version"])
        items = {
            row["capture_id"]: row
            for row in historical["subjects"]["cs408"]["items"]
        }
        item = items[capture_id]
        self.assertEqual("ready", item["queue_state"])
        self.assertEqual("two_pass_ready", item["luna_status"])
        self.assertEqual("legacy_v1_migration", item["projection_origin"])
        self.assertEqual("requested_unverified", item["runtime_identity_status"])
        stale = items["CAP-HISTORY-UNBOUND"]
        self.assertEqual("stale", stale["queue_state"])
        self.assertEqual(
            "legacy_projection_binding_mismatch", stale["last_error_code"]
        )
        current = json.loads(self.projection_path.read_text(encoding="utf-8"))
        self.assertEqual("2026-08-05", current["study_date"])
        source_sha = hashlib.sha256(legacy_raw).hexdigest()
        backup = (
            self.runtime
            / "dispatch/state/dashboard-migrations/backups/sha256"
            / source_sha[:2]
            / f"{source_sha}.json"
        )
        self.assertEqual(legacy_raw, backup.read_bytes())
        receipt = json.loads(
            (
                self.runtime
                / "dispatch/state/dashboard-migrations/receipts/2026-08-04.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(source_sha, receipt["source_projection_sha256"])
        self.assertEqual(0, receipt["formal_write_count"])
        rollback = restore_dashboard_projection_migration(
            self.runtime, "2026-08-04"
        )
        self.assertEqual(source_sha, rollback["restored_projection_sha256"])
        self.assertEqual(legacy_raw, archive.read_bytes())

    def test_three_subjects_merge_to_v3_and_blocked_subject_does_not_block_others(self) -> None:
        math_task = _task("math", "MATH-MERGE-001", self.release_id)
        math_dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _SuccessRunner(),
            stage_timeout_seconds=5,
        )
        math_result = math_dispatcher.submit(math_task).wait(5)
        self.assertEqual(math_result.outcome, "succeeded")

        cs_ready_task = _task(
            "cs408",
            "CS-MERGE-READY-001",
            self.release_id,
            with_visual_roles=True,
        )
        cs_ready_dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _SuccessRunner(),
            stage_timeout_seconds=5,
        )
        cs_ready_result = cs_ready_dispatcher.submit(cs_ready_task).wait(5)
        self.assertEqual(cs_ready_result.outcome, "succeeded")

        cs_task = _task("cs408", "CS-MERGE-001", self.release_id)
        entered = threading.Event()
        self.blocked_dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _BlockedRunner(entered, self.block_release),
            stage_timeout_seconds=20,
        )
        self.blocked_handle = self.blocked_dispatcher.submit(cs_task)
        self.assertTrue(entered.wait(5))

        english_pending = _task("english", "EN-MERGE-001", self.release_id)
        pending_decision = _decision(english_pending, self.release_id, eligible=False)
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()

        update_subject_and_main_projection(
            self.config,
            "cs408",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[
                _decision(cs_task, self.release_id),
                _decision(cs_ready_task, self.release_id),
            ],
            lease_status=self._lease_status(active=1, completed=1),
            updated_at=timestamp,
        )
        update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(math_task, self.release_id)],
            lease_status=self._lease_status(completed=1),
            updated_at=timestamp,
        )
        update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=[pending_decision],
            lease_status=self._lease_status(),
            updated_at=timestamp,
        )

        projection = json.loads(self.projection_path.read_text(encoding="utf-8"))
        self.assertEqual(projection["schema_version"], "study-intake-dashboard-projection-v3")
        self.assertEqual(set(projection["dispatchers"]), {"math", "cs408", "english"})
        self.assertEqual(projection["subjects"]["math"]["current_stage"], "quality_ready")
        self.assertEqual(
            projection["subjects"]["cs408"]["current_stage"],
            "critical_review",
        )
        self.assertEqual(
            projection["subjects"]["english"]["current_stage"],
            "frozen_evidence",
        )
        self.assertEqual(projection["subjects"]["math"]["counts"]["quality_passed"], 1)
        self.assertEqual(projection["subjects"]["cs408"]["counts"]["critical_review_running"], 1)
        self.assertEqual(projection["subjects"]["cs408"]["counts"]["quality_passed"], 1)
        self.assertEqual(
            projection["subjects"]["english"]["counts"]["evidence_pending"],
            1,
        )
        for subject in ("math", "cs408", "english"):
            counts = projection["subjects"][subject]["counts"]
            self.assertEqual(
                counts["selected"],
                counts["queued"]
                + counts["analysis_running"]
                + counts["critical_review_running"]
                + counts["terminal"]
                + counts["evidence_pending"],
            )

        blocked_raw = next(
            row
            for row in projection["subjects"]["cs408"]["items"]
            if row["capture_id"] == "CS-MERGE-001"
        )
        blocked_public = dashboard._public_item(blocked_raw, "cs408")
        self.assertIsNotNone(blocked_public)
        blocked_detail = dashboard._load_dispatch_task_detail(
            self.projection_path,
            blocked_raw,
            blocked_public,
            include_raw=False,
        )
        self.assertIsNotNone(blocked_detail)
        self.assertEqual(blocked_detail["result"]["status"], "pending")
        self.assertFalse(blocked_detail["result"]["consumable"])
        self.assertEqual(
            blocked_detail["stage_statuses"]["analysis"]["status"],
            "completed",
        )
        self.assertEqual(
            blocked_detail["stage_statuses"]["critical_review"]["status"],
            "running",
        )
        self.assertEqual(
            blocked_detail["checkpoints"][-1]["authority_status"],
            "hmac_verified",
        )
        self.assertEqual(
            blocked_detail["structured_stages"]["analysis"]["source"],
            "hmac_checkpoint",
        )
        self.assertNotIn("critical_review", blocked_detail["structured_stages"])

        httpd = dashboard.DashboardHTTPServer(
            ("127.0.0.1", 0),
            dashboard.DashboardHandler,
            store=dashboard.ProjectionStore(self.projection_path),
            expected_release_id=self.release_id,
        )
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            with mock.patch.object(
                dashboard,
                "_verify_completion_authority",
                wraps=dashboard._verify_completion_authority,
            ) as completion_verify:
                status, listing = self._get(
                    port,
                    "/api/v1/items?date=2026-08-05&subject=all",
                )
                self.assertEqual(status, 200, listing)
                self.assertEqual(len(listing["items"]), 4)
                self.assertEqual(completion_verify.call_count, 0)
                by_capture = {row["capture_id"]: row for row in listing["items"]}
                self.assertEqual(by_capture["MATH-MERGE-001"]["queue_state"], "ready")
                self.assertEqual(
                    by_capture["MATH-MERGE-001"]["target_label"],
                    "真实标签 MATH-MERGE-001",
                )
                self.assertTrue(by_capture["MATH-MERGE-001"]["local_model_submitted"])
                self.assertIn("started_at", by_capture["MATH-MERGE-001"])
                self.assertIn("last_state_change_at", by_capture["MATH-MERGE-001"])
                self.assertGreaterEqual(by_capture["MATH-MERGE-001"]["elapsed_seconds"], 0)
                self.assertEqual(
                    by_capture["CS-MERGE-001"]["current_stage"],
                    "critical_review",
                )
                self.assertEqual(
                    by_capture["EN-MERGE-001"]["queue_state"],
                    "evidence_pending",
                )
                self.assertEqual(
                    by_capture["EN-MERGE-001"]["server_queue_confirmation"],
                    "unconfirmed",
                )
                self.assertFalse(by_capture["EN-MERGE-001"]["detail_available"])
                self.assertNotIn("started_at", by_capture["EN-MERGE-001"])
                self.assertNotIn("elapsed_seconds", by_capture["EN-MERGE-001"])
                self.assertFalse(by_capture["EN-MERGE-001"]["local_model_submitted"])

                status, detail = self._get(
                    port,
                    "/api/v1/items/MATH-MERGE-001?date=2026-08-05&subject=math",
                )
                self.assertEqual(status, 200, detail)
                self.assertEqual(completion_verify.call_count, 1)
                self.assertTrue(detail["available"])
                task_detail = detail["item"]["task_detail"]
                self.assertEqual(task_detail["evidence_integrity"]["authority_status"], "hmac_verified")
                self.assertEqual(task_detail["result"]["status"], "ready")
                self.assertTrue(task_detail["result"]["consumable"])
                self.assertEqual(task_detail["deterministic_diff"]["status"], "confirmed")
                self.assertEqual(
                    set(task_detail["structured_stages"]),
                    {"analysis", "critical_review", "final_result"},
                )
                self.assertEqual(
                    task_detail["runtime"]["identity_confirmation"],
                    "requested_unverified",
                )
                self.assertEqual(
                    task_detail["stage_receipts"]["analysis"]["runtime_identity_status"],
                    "requested_unverified",
                )
                self.assertNotIn("model_call_count", detail)
                self.assertNotIn("formal_write_count", detail)
                self.assertEqual(
                    detail["dashboard_request_counter_scope"],
                    "dashboard_read_only_request",
                )
                self.assertEqual(detail["dashboard_request_model_call_count"], 0)
                self.assertEqual(detail["dashboard_request_formal_write_count"], 0)

                status, cs_detail = self._get(
                    port,
                    "/api/v1/items/CS-MERGE-READY-001?date=2026-08-05&subject=cs408",
                )
                self.assertEqual(status, 200, cs_detail)
                self.assertEqual(completion_verify.call_count, 2)
                visual = cs_detail["item"]["task_detail"]["cs408_visual"]
                self.assertEqual(visual["status"], "confirmed")
                self.assertEqual(visual["role_counts"]["question_image"], 1)
                self.assertEqual(visual["role_counts"]["solution_image"], 2)
                self.assertEqual(
                    visual["role_order"],
                    ["question_image", "solution_image", "solution_image"],
                )

                status, raw_detail = self._get(
                    port,
                    "/api/v1/items/MATH-MERGE-001?date=2026-08-05&subject=math&raw=1",
                )
                self.assertEqual(status, 200, raw_detail)
                self.assertEqual(completion_verify.call_count, 3)
                raw_sections = raw_detail["item"]["task_detail"]["raw_sections"]
                self.assertEqual(len(raw_sections), 3)
                self.assertTrue(all(
                    row["verification"] == "hmac_verified_package"
                    and row["content_type"] == "structured_output_not_hidden_reasoning"
                    for row in raw_sections
                ))
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=3)

        # Rewriting one subject with no current scan results retains its terminal history.
        before_refresh = json.loads(self.projection_path.read_text(encoding="utf-8"))
        terminal_elapsed = before_refresh["subjects"]["math"]["items"][0][
            "elapsed_seconds"
        ]
        future_timestamp = (
            dt.datetime.fromisoformat(timestamp) + dt.timedelta(hours=1)
        ).isoformat()
        update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(completed=1),
            updated_at=future_timestamp,
        )
        retained = json.loads(self.projection_path.read_text(encoding="utf-8"))
        self.assertEqual(retained["subjects"]["math"]["counts"]["quality_passed"], 1)
        self.assertEqual(
            retained["subjects"]["math"]["items"][0]["elapsed_seconds"],
            terminal_elapsed,
        )

    def test_requested_unverified_result_is_viewable_but_not_consumable(self) -> None:
        task = _task("math", "MATH-UNVERIFIED-001", self.release_id)
        result = ConcurrentDispatcher(
            self.runtime,
            lambda _task_value, _context: _RequestedUnverifiedRunner(),
            stage_timeout_seconds=5,
        ).submit(task).wait(5)
        self.assertEqual(result.outcome, "succeeded")
        update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(task, self.release_id)],
            lease_status=self._lease_status(completed=1),
        )
        projection = json.loads(self.projection_path.read_text(encoding="utf-8"))
        raw_item = projection["subjects"]["math"]["items"][0]
        public_item = dashboard._public_item(raw_item, "math")
        self.assertIsNotNone(public_item)
        self.assertNotIn("consumable", public_item)
        detail = dashboard._load_dispatch_task_detail(
            self.projection_path,
            raw_item,
            public_item,
            include_raw=False,
        )
        self.assertIsNotNone(detail)
        self.assertEqual(detail["result"]["status"], "ready")
        self.assertTrue(detail["result"]["consumable"])
        self.assertEqual(
            detail["runtime"]["identity_confirmation"],
            "requested_unverified",
        )
        self.assertEqual(
            {
                stage: receipt["runtime_identity_status"]
                for stage, receipt in detail["stage_receipts"].items()
            },
            {
                "analysis": "requested_unverified",
                "critical_review": "requested_unverified",
            },
        )
        self.assertEqual(
            set(detail["structured_stages"]),
            {"analysis", "critical_review", "final_result"},
        )
        self.assertTrue(detail["raw_available"])
        self.assertEqual(detail["model_call_count"], 2)
        self.assertEqual(detail["mcp_tool_call_count"], 0)
        self.assertEqual(detail["formal_write_count"], 0)

    def test_hmac_retry_wait_is_the_only_confirmed_rate_limit(self) -> None:
        task = _task("math", "MATH-RATE-LIMIT-001", self.release_id)
        result = ConcurrentDispatcher(
            self.runtime,
            lambda _task_value, _context: _RateLimitedRunner(),
            stage_timeout_seconds=2,
        ).submit(task).wait(3)
        self.assertEqual(result.status, "retry_wait")
        update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(task, self.release_id)],
            lease_status=self._lease_status(active=0),
        )
        projection = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = projection["subjects"]["math"]["items"][0]
        self.assertEqual(item["server_queue_status"], "rate_limited")
        self.assertEqual(item["server_queue_confirmation"], "confirmed_event")

        httpd = dashboard.DashboardHTTPServer(
            ("127.0.0.1", 0),
            dashboard.DashboardHandler,
            store=dashboard.ProjectionStore(self.projection_path),
            expected_release_id=self.release_id,
        )
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            status, detail = self._get(
                httpd.server_address[1],
                "/api/v1/items/MATH-RATE-LIMIT-001?date=2026-08-05&subject=math",
            )
            self.assertEqual(status, 200, detail)
            queue = detail["item"]["task_detail"]["server_queue"]
            self.assertEqual(queue["declared_status"], "rate_limited")
            self.assertEqual(queue["confirmation"], "confirmed_event")
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=3)

    def test_current_day_task_list_is_not_cut_off_at_two_hundred(self) -> None:
        decisions = []
        for index in range(250):
            task = _task(
                "english", f"EN-NO-CAP-{index:04d}", self.release_id
            )
            decisions.append(
                _decision(task, self.release_id, eligible=False)
            )
        update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=decisions,
            lease_status=self._lease_status(),
        )
        projection = json.loads(
            self.projection_path.read_text(encoding="utf-8")
        )
        section = projection["subjects"]["english"]
        self.assertEqual(len(section["items"]), 250)
        self.assertEqual(section["counts"]["selected"], 250)
        self.assertEqual(section["counts"]["evidence_pending"], 250)

    def test_subject_batch_and_global_sol_control_state_are_projected_without_fake_defaults(self) -> None:
        task = _task("math", "MATH-BATCH-CONTROL-001", self.release_id)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _SuccessRunner(),
            stage_timeout_seconds=5,
        )
        self.assertEqual(dispatcher.submit(task).wait(5).outcome, "succeeded")
        dispatcher.drain(5)

        batch_path = (
            self.runtime
            / "dispatch/state/subject-luna-batches/math.json"
        )
        batch_path.parent.mkdir(parents=True, exist_ok=True)
        batch_path.write_text(
            json.dumps({
                "schema_version": "subject_luna_batch_v1",
                "batch_id": "MATH-CONTROL-BATCH-001",
                "subject": "math",
                "study_date": "2026-08-05",
                "status": "frozen",
                "capture_high_watermark": "a" * 64,
                "scan_snapshot_sha256": "f" * 64,
                "authority_generation": self.release_id,
                "authority_fingerprint": "b" * 64,
                "tasks": [{
                    "capture_id": "MATH-BATCH-CONTROL-001",
                    "unit_sha256": task.unit_sha256,
                    "input_fingerprint": task.frozen_payload["input_fingerprint"],
                    "study_date": "2026-08-05",
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "status": "quality_passed",
                    "proposal_sha256": "e" * 64,
                    "package_sha256": "c" * 64,
                    "quality_receipt_sha256": "d" * 64,
                    "terminal_receipt_sha256": None,
                    "error_code": None,
                }],
                "exclusion_receipt_sha256s": [],
                "all_terminal": True,
                "sol_ready": True,
                "blocking_task_ids": [],
                "formal_write_count": 0,
                "revision": 1,
                "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }),
            encoding="utf-8",
        )
        global_path = self.runtime / "dispatch/state/global-sol-writer.json"
        global_path.write_text(
            json.dumps({
                "schema_version": "global_sol_writer_lease_v1",
                "revision": 1,
                "next_fencing_token": 1,
                "queue": [],
                "active_writer": None,
                "active_writer_count": 0,
                "formal_write_count": 0,
                "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }),
            encoding="utf-8",
        )

        projection = update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(task, self.release_id)],
            lease_status=self._lease_status(completed=1),
        )
        math = projection["subjects"]["math"]
        self.assertTrue(math["batch_state_available"])
        self.assertTrue(math["all_terminal"])
        self.assertFalse(math["sol_ready"])
        self.assertIn("quality_receipt_authority_invalid", math["blockers"])
        self.assertEqual(math["scan_snapshot_sha256"], "f" * 64)
        self.assertEqual(math["counts"]["quality_passed"], 1)
        self.assertEqual(math["sol_handoff_status"], "not_ready")
        self.assertTrue(projection["global_sol"]["state_available"])
        self.assertEqual(projection["global_sol"]["active_writer_count"], 0)

        batch_value = json.loads(batch_path.read_text(encoding="utf-8"))
        batch_value["tasks"][0]["status"] = "quality_pending"
        batch_value["tasks"][0]["proposal_sha256"] = None
        batch_value["tasks"][0]["package_sha256"] = None
        batch_value["tasks"][0]["quality_receipt_sha256"] = None
        batch_value["all_terminal"] = False
        batch_value["sol_ready"] = False
        batch_value["blocking_task_ids"] = ["MATH-BATCH-CONTROL-001"]
        batch_path.write_text(json.dumps(batch_value), encoding="utf-8")
        projection = update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(task, self.release_id)],
            lease_status=self._lease_status(completed=1),
        )
        self.assertEqual(
            projection["subjects"]["math"]["counts"]["critical_review_running"],
            1,
        )
        self.assertEqual(projection["subjects"]["math"]["counts"]["failed"], 0)

        batch_value["formal_write_count"] = 1
        batch_path.write_text(json.dumps(batch_value), encoding="utf-8")
        projection = update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-05",
            daemon_status="running",
            eligible_count=1,
            submitted_count=1,
            decisions=[_decision(task, self.release_id)],
            lease_status=self._lease_status(completed=1),
        )
        math = projection["subjects"]["math"]
        self.assertFalse(math["batch_state_available"])
        self.assertIsNone(math["all_terminal"])
        self.assertIsNone(math["sol_ready"])

    def test_cross_date_batch_and_writer_remain_visible_as_current_blocker(self) -> None:
        self.config["dashboard"]["projection_schema_version"] = (
            dashboard_projection_module.MAIN_SCHEMA
        )
        batch_path = self.runtime / "dispatch/state/subject-luna-batches/english.json"
        batch_path.parent.mkdir(parents=True, exist_ok=True)
        batch_path.write_text(
            json.dumps(
                {
                    "schema_version": "subject_luna_batch_v1",
                    "batch_id": "ENGLISH-OLD-BATCH-001",
                    "subject": "english",
                    "study_date": "2026-08-04",
                    "status": "frozen",
                    "capture_high_watermark": "1" * 64,
                    "scan_snapshot_sha256": "2" * 64,
                    "authority_generation": "english-old-generation",
                    "authority_fingerprint": "3" * 64,
                    "tasks": [
                        {
                            "capture_id": "EN-OLD-001",
                            "unit_sha256": "4" * 64,
                            "input_fingerprint": "5" * 64,
                            "study_date": "2026-08-04",
                            "frozen_payload_sha256": "6" * 64,
                            "status": "failed",
                            "proposal_sha256": None,
                            "package_sha256": None,
                            "quality_receipt_sha256": None,
                            "terminal_receipt_sha256": "7" * 64,
                            "error_code": "old_failure",
                        }
                    ],
                    "exclusion_receipt_sha256s": [],
                    "all_terminal": True,
                    "sol_ready": False,
                    "blocking_task_ids": ["EN-OLD-001"],
                    "formal_write_count": 0,
                    "revision": 2,
                    "updated_at": "2026-08-04T12:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )
        writer_path = self.runtime / "dispatch/state/subject-sol/english.json"
        writer_path.parent.mkdir(parents=True, exist_ok=True)
        writer_path.write_text(
            json.dumps(
                {
                    "schema_version": "subject_sol_writer_state_v2",
                    "subject": "english",
                    "revision": 1,
                    "handoff_status": "awaiting_luna",
                    "batch_id": "ENGLISH-OLD-BATCH-001",
                    "writer_adapter": "english_daily_intake_writer_v1",
                    "fencing_token": 0,
                    "authorization_receipt_sha256": None,
                    "daily_sol_batch_sha256": None,
                    "review_receipt_sha256": None,
                    "commit_receipt_sha256": None,
                    "generation_fence": {
                        "blocked": False,
                        "source_generation": None,
                        "next_generation": None,
                    },
                    "formal_write_count": 0,
                    "updated_at": "2026-08-04T12:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )
        projection = update_subject_and_main_projection(
            self.config,
            "english",
            study_date="2026-08-05",
            daemon_status="disabled",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self._lease_status(),
            updated_at="2026-08-05T08:00:00+00:00",
        )
        section = projection["subjects"]["english"]
        self.assertFalse(section["batch_state_available"])
        self.assertEqual(section["blocking_batch"]["study_date"], "2026-08-04")
        self.assertTrue(section["blocking_batch"]["writer_bound"])
        self.assertEqual(
            section["blocking_batch"]["blocker_code"],
            "cross_date_batch_writer_bound",
        )
        self.assertIn("cross_date_batch_writer_bound", section["blockers"])
        self.assertIsNone(dashboard._projection_contract_error(projection))


if __name__ == "__main__":
    unittest.main()
