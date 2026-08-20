from __future__ import annotations

import json
import hashlib
import sys
import tempfile
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "dashboard"))

import dashboard_projection as projection  # noqa: E402
import server as dashboard_server  # noqa: E402


class DashboardConcurrencyProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(
            prefix="dashboard-concurrency-projection-"
        )
        self.runtime = Path(self.temp.name) / "runtime"
        self.main_path = self.runtime / "state/dashboard_projection.json"
        self.config = {
            "runtime_root": str(self.runtime),
            "timezone": "UTC",
            "dashboard": {"projection_path": str(self.main_path)},
        }
        self.release_id = "a" * 64
        manifest_path = Path(self.temp.name) / "release.json"
        manifest_path.write_text(
            json.dumps({"release_id": self.release_id}), encoding="utf-8"
        )
        self.config["release"] = {"manifest_path": str(manifest_path)}

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def lease_status(
        subject: str,
        active: int,
        *,
        second: int = 0,
        canary_gate: dict | None = None,
        telemetry: dict | None = None,
    ) -> dict:
        return {
            "schema_version": "study-intake-dispatch-subject-status-v1",
            "subject": subject,
            "draining": False,
            "active_count": active,
            "stale_count": 0,
            "claimed_total": active,
            "completed_count": 0,
            "retry_wait_count": 0,
            "max_fence": active,
            "heartbeat_interval_seconds": 15,
            "lease_ttl_seconds": 120,
            "canary_gate": canary_gate,
            "canary_concurrency_telemetry": telemetry,
            "effective_concurrency_limit": 1 if canary_gate else 0,
            "available_concurrency_slots": (
                max(0, 1 - active) if canary_gate else 0
            ),
            "subject_peak_active": (
                telemetry["subject_peak_active"][subject] if telemetry else 0
            ),
            "verified_runner_active_task_count": (
                telemetry["verified_runner_active_by_subject"][subject]
                if telemetry else 0
            ),
            "runner_evidenced_task_count": (
                telemetry["runner_evidenced_task_count_by_subject"][subject]
                if telemetry else 0
            ),
            "runner_interval_missing_count": (
                telemetry["runner_interval_missing_count_by_subject"][subject]
                if telemetry else 0
            ),
            "scheduler_claim_subject_peak_active": (
                telemetry["scheduler_claim_subject_peak_active"][subject]
                if telemetry else 0
            ),
            "terminal_index_sha256": (
                telemetry["terminal_index_sha256_by_subject"][subject]
                if telemetry else None
            ),
            "terminal_task_count": (
                telemetry["terminal_task_count_by_subject"][subject]
                if telemetry else 0
            ),
            "terminal_by_outcome": (
                telemetry["terminal_by_outcome_by_subject"][subject]
                if telemetry else {
                    "succeeded": 0,
                    "failed": 0,
                    "cancelled": 0,
                    "timed_out": 0,
                    "stalled": 0,
                    "needs_rework": 0,
                }
            ),
            "terminal_failure_bindings": (
                telemetry["terminal_failure_bindings_by_subject"][subject]
                if telemetry else []
            ),
            "global_active_task_count": (
                telemetry["global_active_task_count"] if telemetry else 0
            ),
            "global_peak_active": (
                telemetry["global_peak_active"] if telemetry else 0
            ),
            "backpressure_reason": (
                canary_gate.get("backpressure_reason")
                if canary_gate is not None
                else None
            ),
            "observed_at": f"2026-08-11T05:00:{second:02d}+00:00",
        }

    def update(
        self,
        subject: str,
        active: int,
        *,
        second: int = 0,
        canary_gate: dict | None = None,
        telemetry: dict | None = None,
    ) -> dict:
        return projection.update_subject_and_main_projection(
            self.config,
            subject,
            study_date="2026-08-11",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status=self.lease_status(
                subject,
                active,
                second=second,
                canary_gate=canary_gate,
                telemetry=telemetry,
            ),
            updated_at=f"2026-08-11T05:01:{second:02d}+00:00",
        )

    def canary_gate(self, subject: str) -> dict:
        index = {"math": "1", "cs408": "2", "english": "3"}[subject]
        unit_sha256 = index * 64
        selected = {
            "producer_unit_id": f"CANARY-{subject}",
            "producer_recorded_at": "2026-08-11T05:00:00+00:00",
            "producer_input_contract_sha256": "4" * 64,
            "source_event_set_sha256": "5" * 64,
            "unit_sha256": unit_sha256,
            "frozen_payload_sha256": "6" * 64,
            "canary_gate_sha256": "7" * 64,
            "canary_gate_authority_sha256": "8" * 64,
        }
        return {
            "schema_version": "study-intake-production-canary-state-v3",
            "status": "production_canary_active",
            "state": "canary_in_flight",
            "subject": subject,
            "release_id": self.release_id,
            "activation_id": index * 64,
            "activated_at": "2026-08-11T04:00:00+00:00",
            "producer_authority_fingerprint": "9" * 64,
            "producer_high_watermark_sha256": "a" * 64,
            "activation_receipt_sha256": "b" * 64,
            "activation_gate_authority_sha256": "c" * 64,
            "terminal_index_sha256": index * 64,
            "terminal_task_count": 0,
            "terminal_by_outcome": {
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "timed_out": 0,
                "stalled": 0,
                "needs_rework": 0,
            },
            "luna_consumer_enabled": True,
            "producer_capture_enabled": True,
            "sol_formal_curation_enabled": False,
            "queue_depth": 2,
            "oldest_pending_age_seconds": 10,
            "active_task_count": 1,
            "active_selections": {unit_sha256: selected},
            "last_success_at": None,
            "last_failure_at": None,
            "last_preclaim_failure_at": None,
            "last_preclaim_failure_stage": None,
            "last_preclaim_failure_error_code": None,
            "last_preclaim_failure_receipt_sha256": None,
            "last_preclaim_failure_evidence_sha256": None,
            "preclaim_failure_resume_ack_sha256": None,
            "blocking_reason": None,
            "backpressure_reason": "initial_canary_inflight_limit_reached",
            "next_action": "await_active_task_terminal",
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
            "selected": selected,
            "last_selected": selected,
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
            "updated_at": "2026-08-11T05:00:00+00:00",
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": "d" * 64,
                "purpose": "dispatch-production-canary-state",
                "hmac_sha256": "e" * 64,
            },
        }

    def telemetry(self, *, updated_at: str, global_peak: int) -> dict:
        authority = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": "f" * 64,
            "purpose": "dispatch-production-canary-concurrency-telemetry",
            "hmac_sha256": "0" * 64,
        }
        return {
            "schema_version": (
                "study-intake-production-canary-concurrency-telemetry-v1"
            ),
            "release_id": self.release_id,
            "activation_ids": {
                "math": "1" * 64,
                "cs408": "2" * 64,
                "english": "3" * 64,
            },
            "active_by_subject": {"math": 1, "cs408": 1, "english": 1},
            "subject_peak_active": {"math": 1, "cs408": 1, "english": 1},
            "global_active_task_count": 3,
            "global_peak_active": global_peak,
            "verified_runner_active_by_subject": {
                "math": 1, "cs408": 1, "english": 1,
            },
            "verified_runner_global_active_task_count": 3,
            "scheduler_claim_subject_peak_active": {
                "math": 1, "cs408": 1, "english": 1,
            },
            "scheduler_claim_global_peak_active": 3,
            "runner_evidenced_task_count_by_subject": {
                "math": 1, "cs408": 1, "english": 1,
            },
            "runner_evidenced_task_count_global": 3,
            "runner_interval_missing_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "runner_interval_missing_count_global": 0,
            "zero_duration_runner_interval_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "zero_duration_runner_interval_count_global": 0,
            "terminal_index_sha256_by_subject": {
                "math": "1" * 64,
                "cs408": "2" * 64,
                "english": "3" * 64,
            },
            "terminal_task_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "terminal_task_count_global": 0,
            "terminal_by_outcome_by_subject": {
                subject: {
                    "succeeded": 0,
                    "failed": 0,
                    "cancelled": 0,
                    "timed_out": 0,
                    "stalled": 0,
                    "needs_rework": 0,
                }
                for subject in ("math", "cs408", "english")
            },
            "terminal_by_outcome_global": {
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "timed_out": 0,
                "stalled": 0,
                "needs_rework": 0,
            },
            "terminal_failure_bindings_by_subject": {
                "math": [], "cs408": [], "english": [],
            },
            "counter_scope": (
                "same_release_current_activation_task_process_lifecycle"
            ),
            "peak_source": (
                "hmac_task_process_identity_and_exit_half_open_intervals"
            ),
            "runner_interval_policy": (
                "half_open_end_before_start_zero_duration_nonoverlap"
            ),
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "updated_at": updated_at,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority": authority,
        }

    def producer_authority(self, subject: str) -> dict:
        core = {
            "schema_version": "study-intake-producer-authority-v1",
            "subject": subject,
            "release_id": self.release_id,
            "loaded_core_sha256": "1" * 64,
            "processing_contract_sha256": "2" * 64,
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "formal_write_count": 0,
        }
        return {
            **core,
            "authority_fingerprint": hashlib.sha256(
                json.dumps(
                    core,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        }

    def test_missing_lease_status_is_unavailable_not_zero(self) -> None:
        result = projection.update_subject_and_main_projection(
            self.config,
            "math",
            study_date="2026-08-11",
            daemon_status="running",
            eligible_count=0,
            submitted_count=0,
            decisions=[],
            lease_status={},
            updated_at="2026-08-11T05:01:00+00:00",
        )
        math = result["subjects"]["math"]
        self.assertEqual(math["concurrency_status"], "unavailable")
        self.assertIsNone(math["subject_active_task_count"])
        self.assertIsNone(math["subject_pending_task_count"])
        self.assertIsNone(result["concurrency"]["global_active_task_count"])
        self.assertNotEqual(math["backpressure_reason"], None)

    def test_real_lease_store_v2_status_projects_without_model_work(self) -> None:
        store = projection.LeaseStore(self.runtime)
        for subject in ("math", "cs408", "english"):
            store.begin_subject_drain(subject)
            store.activate_production_canary(
                subject,
                release_id=self.release_id,
                producer_authority=self.producer_authority(subject),
                activated_at="2026-08-11T04:00:00+00:00",
            )
        result = None
        for second, subject in enumerate(("math", "cs408", "english"), 1):
            result = projection.update_subject_and_main_projection(
                self.config,
                subject,
                study_date="2026-08-11",
                daemon_status="running",
                eligible_count=0,
                submitted_count=0,
                decisions=[],
                lease_status=store.subject_status(subject),
                updated_at=f"2026-08-11T05:01:{second:02d}+00:00",
            )
        assert result is not None
        self.assertEqual(result["concurrency"]["status"], "verified")
        self.assertEqual(result["concurrency"]["global_active_task_count"], 0)
        self.assertEqual(result["concurrency"]["global_peak_active"], 0)
        self.assertEqual(result["concurrency"]["effective_concurrency_limit"], 3)
        for subject in ("math", "cs408", "english"):
            row = result["subjects"][subject]
            self.assertEqual(row["concurrency_status"], "verified")
            self.assertEqual(row["concurrency_state"], "first_canary_armed")
            self.assertEqual(row["effective_concurrency_limit"], 1)
            self.assertEqual(row["available_concurrency_slots"], 1)
            self.assertFalse(row["canary_gate"]["fast_mode_requested"])
            self.assertEqual(
                row["canary_gate"]["fast_mode_effective"],
                "not_requested",
            )

    def test_concurrent_subject_updates_aggregate_without_peak_regression(self) -> None:
        barrier = threading.Barrier(3)
        failures: list[BaseException] = []

        telemetry = self.telemetry(
            updated_at="2026-08-11T05:00:00+00:00",
            global_peak=5,
        )

        def worker(subject: str, second: int) -> None:
            try:
                barrier.wait(timeout=3)
                self.update(
                    subject,
                    1,
                    second=second,
                    canary_gate=self.canary_gate(subject),
                    telemetry=telemetry,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        threads = [
            threading.Thread(target=worker, args=("math", 1)),
            threading.Thread(target=worker, args=("cs408", 2)),
            threading.Thread(target=worker, args=("english", 3)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertFalse(failures)
        self.assertTrue(all(not thread.is_alive() for thread in threads))

        combined = json.loads(self.main_path.read_text(encoding="utf-8"))
        self.assertEqual(combined["concurrency"]["global_active_task_count"], 3)
        self.assertEqual(combined["concurrency"]["global_peak_active"], 5)
        self.assertEqual(combined["concurrency"]["effective_concurrency_limit"], 3)
        self.assertEqual(combined["concurrency"]["available_concurrency_slots"], 0)
        self.assertIsNone(dashboard_server._projection_contract_error(combined))
        self.assertEqual(
            {
                subject: combined["subjects"][subject]["subject_peak_active"]
                for subject in ("math", "cs408", "english")
            },
            {"math": 1, "cs408": 1, "english": 1},
        )

        stale = self.telemetry(
            updated_at="2026-08-11T04:59:00+00:00",
            global_peak=3,
        )
        combined = self.update(
            "math",
            1,
            second=10,
            canary_gate=self.canary_gate("math"),
            telemetry=stale,
        )
        self.assertEqual(combined["concurrency"]["global_peak_active"], 5)

        mismatched = self.telemetry(
            updated_at="2026-08-11T05:02:00+00:00",
            global_peak=5,
        )
        mismatched["activation_ids"]["math"] = "f" * 64
        combined = self.update(
            "math",
            1,
            second=11,
            canary_gate=self.canary_gate("math"),
            telemetry=mismatched,
        )
        self.assertEqual(
            combined["subjects"]["math"]["concurrency_status"],
            "unavailable",
        )
        self.assertIsNone(
            combined["subjects"]["math"]["subject_active_task_count"]
        )
        self.assertIsNone(combined["concurrency"]["global_active_task_count"])
        self.assertEqual(combined["concurrency"]["status"], "partial")


if __name__ == "__main__":
    unittest.main()
