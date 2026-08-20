from __future__ import annotations

import http.client
import json
import shutil
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import urlencode


DASHBOARD_DIR = Path(__file__).resolve().parents[1]
FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "dashboard_projection.json"
sys.path.insert(0, str(DASHBOARD_DIR))

import server as dashboard  # noqa: E402


class DashboardServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="dashboard-test-")
        cls.runtime_root = Path(cls.temp_dir.name)
        cls.projection_path = cls.runtime_root / "state" / "dashboard_projection.json"
        cls.projection_path.parent.mkdir(parents=True)
        shutil.copy2(FIXTURE_PATH, cls.projection_path)
        cls.httpd = dashboard.DashboardHTTPServer(
            ("127.0.0.1", 0),
            dashboard.DashboardHandler,
            store=dashboard.ProjectionStore(cls.projection_path),
            expected_release_id="1" * 64,
        )
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.httpd.server_address[1]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)
        cls.temp_dir.cleanup()

    def setUp(self) -> None:
        dispatch_root = self.runtime_root / "dispatch"
        if dispatch_root.exists():
            shutil.rmtree(dispatch_root)
        shutil.copy2(FIXTURE_PATH, self.projection_path)
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["generated_at"] = datetime.now(timezone.utc).isoformat()
        payload["concurrency"]["generated_at"] = payload["generated_at"]
        payload["global_sol"]["updated_at"] = payload["generated_at"]
        for dispatcher in payload["dispatchers"].values():
            dispatcher["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        archive_dir = self.projection_path.parent / "dashboard-projections"
        if archive_dir.exists():
            shutil.rmtree(archive_dir)
        self.httpd.store = dashboard.ProjectionStore(self.projection_path)

    def request(self, method: str, path: str, *, host: str = "127.0.0.1:8767"):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.request(method, path, headers={"Host": host})
        response = connection.getresponse()
        body = response.read()
        headers = dict(response.getheaders())
        connection.close()
        return response.status, headers, body

    def json_request(self, method: str, path: str, *, host: str = "127.0.0.1:8767"):
        status, headers, body = self.request(method, path, host=host)
        return status, headers, json.loads(body.decode("utf-8"))

    def test_multi_agent_v2_control_projection_is_locked_and_zero_call(self) -> None:
        status, _, payload = self.json_request(
            "GET", "/api/v1/multi-agent-v2"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["schema_version"],
            "study-intake-dashboard-multi-agent-v1",
        )
        self.assertEqual(payload["execution_mode"], "offline")
        self.assertTrue(payload["live_gate_locked"])
        self.assertFalse(payload["manual_authorization_present"])
        self.assertEqual(payload["branch_level"]["logical_branch_count"], 0)
        self.assertEqual(payload["branch_level"]["active_branch_count"], 0)
        self.assertEqual(payload["safety_counts"]["real_terra_call_count"], 0)
        self.assertEqual(payload["safety_counts"]["real_luna_call_count"], 0)
        self.assertEqual(
            payload["safety_counts"]["real_provider_model_request_count"], 0
        )
        self.assertEqual(payload["safety_counts"]["production_mcp_task_count"], 0)
        self.assertEqual(payload["safety_counts"]["formal_write_count"], 0)
        self.assertFalse(payload["production_accepted"])

    def test_release_manifest_accepts_valid_manifest_larger_than_64_kib(self) -> None:
        release_id = "a" * 64
        manifest_path = self.runtime_root / "large-valid-release.json"
        payload = json.dumps(
            {"release_id": release_id, "padding": "x" * (64 * 1024)},
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertGreater(len(payload), 64 * 1024)
        self.assertLessEqual(len(payload), dashboard.MAX_RELEASE_MANIFEST_BYTES)
        manifest_path.write_bytes(payload)

        self.assertEqual(
            dashboard._release_id_from_manifest(manifest_path),
            release_id,
        )

    def test_release_manifest_rejects_payload_over_new_bound(self) -> None:
        manifest_path = self.runtime_root / "oversized-release.json"
        manifest_path.write_bytes(
            b'{"release_id":"'
            + b"a" * 64
            + b'","padding":"'
            + b"x" * dashboard.MAX_RELEASE_MANIFEST_BYTES
            + b'"}'
        )
        self.assertGreater(
            manifest_path.stat().st_size,
            dashboard.MAX_RELEASE_MANIFEST_BYTES,
        )

        self.assertIsNone(dashboard._release_id_from_manifest(manifest_path))

    def test_release_manifest_rejects_bad_json(self) -> None:
        manifest_path = self.runtime_root / "bad-json-release.json"
        manifest_path.write_text('{"release_id":', encoding="utf-8")

        self.assertIsNone(dashboard._release_id_from_manifest(manifest_path))

    def test_release_manifest_rejects_invalid_release_id(self) -> None:
        manifest_path = self.runtime_root / "bad-release-id.json"
        manifest_path.write_text(
            json.dumps({"release_id": "g" * 64}),
            encoding="utf-8",
        )

        self.assertIsNone(dashboard._release_id_from_manifest(manifest_path))

    @staticmethod
    def production_canary_gate(
        subject: str,
        *,
        state: str = "armed",
        queue_depth: int = 0,
        oldest_pending_age_seconds: int | None = None,
    ) -> dict:
        index = {"math": "2", "cs408": "3", "english": "4"}[subject]
        now = datetime.now(timezone.utc).isoformat()
        authority = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": index * 64,
            "purpose": "dispatch-production-canary-state",
            "hmac_sha256": "5" * 64,
        }
        unlocked = state == "continuous_concurrent_unlocked"
        failed = state == "failed_drained"
        paused = state == "paused_drained"
        selected = None
        last_selected = None
        active_count = 0
        terminal_task_count = 1 if unlocked or failed else 0
        terminal_by_outcome = {
            "succeeded": 1 if unlocked else 0,
            "failed": 1 if failed else 0,
            "cancelled": 0,
            "timed_out": 0,
            "needs_rework": 0,
        }
        if state in {
            "canary_in_flight",
            "continuous_concurrent_unlocked",
            "failed_drained",
        }:
            last_selected = {
                "producer_unit_id": f"CANARY-{subject}",
                "producer_recorded_at": now,
                "producer_input_contract_sha256": "6" * 64,
                "source_event_set_sha256": "7" * 64,
                "unit_sha256": "8" * 64,
                "frozen_payload_sha256": "9" * 64,
                "canary_gate_sha256": "a" * 64,
                "canary_gate_authority_sha256": "b" * 64,
            }
        if state == "canary_in_flight":
            active_count = 1
            selected = last_selected
        return {
            "schema_version": "study-intake-production-canary-state-v2",
            "status": "production_canary_active",
            "state": state,
            "subject": subject,
            "release_id": "1" * 64,
            "activation_id": index * 64,
            "activated_at": now,
            "producer_authority_fingerprint": "c" * 64,
            "producer_high_watermark_sha256": "d" * 64,
            "activation_receipt_sha256": "e" * 64,
            "activation_gate_authority_sha256": "f" * 64,
            "terminal_index_sha256": index * 64,
            "terminal_task_count": terminal_task_count,
            "terminal_by_outcome": terminal_by_outcome,
            "last_terminal_receipt_sha256": "0" * 64 if unlocked or failed else None,
            "last_report_sha256": "a" * 64 if unlocked else None,
            "last_package_sha256": "b" * 64 if unlocked else None,
            "last_success_at": now if unlocked else None,
            "last_failure_at": now if failed else None,
            "producer_capture_enabled": True,
            "luna_consumer_enabled": not (failed or paused),
            "sol_formal_curation_enabled": False,
            "queue_depth": queue_depth,
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
            "active_task_count": active_count,
            "active_selections": (
                {selected["unit_sha256"]: selected}
                if selected is not None
                else {}
            ),
            "blocking_reason": (
                "canary_task_failed" if failed else "paused_by_user" if paused else None
            ),
            "backpressure_reason": None,
            "next_action": (
                "explicit_subject_resume_required"
                if failed or paused
                else "consume_pending_post_activation_queue"
                if unlocked
                else "await_active_task_terminal"
                if state == "canary_in_flight"
                else "await_first_post_activation_capture"
            ),
            "last_analysis_status": (
                "completed" if unlocked else "failed_or_incomplete" if failed else "not_started"
            ),
            "last_critical_review_status": (
                "completed" if unlocked else "failed_or_incomplete" if failed else "not_started"
            ),
            "last_report_status": (
                "reopen_verified" if unlocked else "not_verified" if failed else "not_started"
            ),
            "last_read_session_id": "READ-CANARY" if unlocked else None,
            "last_evidence_generation": "GEN-CANARY" if unlocked else None,
            "last_evidence_authority_fingerprint": "c" * 64 if unlocked else None,
            "last_analysis_grounding_manifest_sha256": "d" * 64 if unlocked else None,
            "last_critical_review_grounding_manifest_sha256": "e" * 64 if unlocked else None,
            "last_analysis_evidence_refs": (
                [f"mcp-item:{subject}:" + "1" * 64] if unlocked else []
            ),
            "last_critical_review_evidence_refs": (
                [f"mcp-item:{subject}:" + "2" * 64] if unlocked else []
            ),
            "last_preclaim_failure_at": None,
            "last_preclaim_failure_stage": None,
            "last_preclaim_failure_error_code": None,
            "last_preclaim_failure_receipt_sha256": None,
            "last_preclaim_failure_evidence_sha256": None,
            "preclaim_failure_resume_ack_sha256": None,
            "last_emergency_cancel_at": None,
            "last_emergency_cancel_receipt_sha256": None,
            "late_result_fence_status": "not_required",
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "keep_backlog_drained": True,
            "selected": selected,
            "last_selected": last_selected,
            "unlocked_once": unlocked,
            "production_accepted": False,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "requested_service_tier": None,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "control_plane_model_call_count": 0,
            "control_plane_provider_request_count": 0,
            "control_plane_mcp_tool_call_count": 0,
            "observability_counter_scope": "current_activation_cumulative",
            "observed_model_call_count": 2 if unlocked else 0,
            "observed_provider_request_count": 2 if unlocked else 0,
            "observed_mcp_tool_call_count": 3 if unlocked else 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "updated_at": now,
            "state_authority_sha256": dashboard.sha256_bytes(
                dashboard.canonical_bytes(authority)
            ),
            "authority": authority,
        }

    def install_production_canary_gates(self, *, failed_subject: str | None = None) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        for subject in ("math", "cs408", "english"):
            gate = self.production_canary_gate(
                subject,
                state="failed_drained" if subject == failed_subject else "armed",
                queue_depth=2 if subject == failed_subject else 0,
                oldest_pending_age_seconds=45 if subject == failed_subject else None,
            )
            payload["dispatchers"][subject]["canary_gate"] = gate
            payload["subjects"][subject]["canary_gate"] = gate
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

    def install_verified_concurrency_projection(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        observed_at = datetime.now(timezone.utc).isoformat()
        gates: dict[str, dict] = {}
        for subject in ("math", "cs408", "english"):
            gate = self.production_canary_gate(subject, state="armed")
            gates[subject] = gate
            payload["dispatchers"][subject]["canary_gate"] = gate
            payload["subjects"][subject]["canary_gate"] = gate
        authority = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": "6" * 64,
            "purpose": "dispatch-production-canary-concurrency-telemetry",
            "hmac_sha256": "7" * 64,
        }
        authority_sha256 = dashboard.sha256_bytes(
            dashboard.canonical_bytes(authority)
        )
        telemetry = {
            "schema_version": (
                "study-intake-production-canary-concurrency-telemetry-v1"
            ),
            "release_id": "1" * 64,
            "activation_ids": {
                subject: gates[subject]["activation_id"]
                for subject in ("math", "cs408", "english")
            },
            "active_by_subject": {"math": 0, "cs408": 0, "english": 0},
            "subject_peak_active": {"math": 0, "cs408": 0, "english": 0},
            "global_active_task_count": 0,
            "global_peak_active": 0,
            "verified_runner_active_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "verified_runner_global_active_task_count": 0,
            "scheduler_claim_subject_peak_active": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "scheduler_claim_global_peak_active": 0,
            "runner_evidenced_task_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "runner_evidenced_task_count_global": 0,
            "runner_interval_missing_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "runner_interval_missing_count_global": 0,
            "zero_duration_runner_interval_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "zero_duration_runner_interval_count_global": 0,
            "terminal_index_sha256_by_subject": {
                subject: gates[subject]["terminal_index_sha256"]
                for subject in ("math", "cs408", "english")
            },
            "terminal_task_count_by_subject": {
                "math": 0, "cs408": 0, "english": 0,
            },
            "terminal_task_count_global": 0,
            "terminal_by_outcome_by_subject": {
                subject: dict(gates[subject]["terminal_by_outcome"])
                for subject in ("math", "cs408", "english")
            },
            "terminal_by_outcome_global": {
                "succeeded": 0, "failed": 0, "cancelled": 0,
                "timed_out": 0, "needs_rework": 0,
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
            "updated_at": observed_at,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority_sha256": authority_sha256,
            "authority": authority,
        }
        for subject in ("math", "cs408", "english"):
            payload["subjects"][subject].update(
                {
                    "concurrency_status": "verified",
                    "concurrency_state": "first_canary_armed",
                    "concurrency_source": "lease_store_subject_status_v1",
                    "concurrency_observed_at": observed_at,
                    "concurrency_error_code": None,
                    "subject_active_task_count": 0,
                    "subject_pending_task_count": 0,
                    "subject_peak_active": telemetry["subject_peak_active"][subject],
                    "subject_verified_runner_active_task_count": 0,
                    "subject_runner_evidenced_task_count": 0,
                    "subject_runner_interval_missing_count": 0,
                    "scheduler_claim_subject_peak_active": 0,
                    "terminal_index_sha256": gates[subject]["terminal_index_sha256"],
                    "terminal_task_count": 0,
                    "terminal_by_outcome": dict(
                        gates[subject]["terminal_by_outcome"]
                    ),
                    "terminal_failure_bindings": [],
                    "lease_retry_wait_task_count": 0,
                    "lease_stale_task_count": 0,
                    "effective_concurrency_limit": 1,
                    "effective_concurrency_limit_mode": "bounded",
                    "available_concurrency_slots": 1,
                    "backpressure_reason": None,
                }
            )
        payload["concurrency"] = {
            "schema_version": "study-intake-dashboard-concurrency-v1",
            "status": "verified",
            "release_id": "1" * 64,
            "generated_at": payload["generated_at"],
            "source": "lease_store_subject_status_v1",
            "source_telemetry": telemetry,
            "telemetry_authority_sha256": authority_sha256,
            "peak_semantics": "hmac_task_process_half_open_intervals",
            "global_active_task_count": 0,
            "global_peak_active": 0,
            "verified_runner_global_active_task_count": 0,
            "scheduler_claim_global_peak_active": 0,
            "runner_evidenced_task_count_global": 0,
            "runner_interval_missing_count_global": 0,
            "zero_duration_runner_interval_count_global": 0,
            "terminal_task_count_global": 0,
            "terminal_by_outcome_global": dict(
                telemetry["terminal_by_outcome_global"]
            ),
            "terminal_failure_binding_count_global": 0,
            "effective_concurrency_limit": 3,
            "effective_concurrency_limit_mode": "bounded",
            "available_concurrency_slots": 3,
            "backpressure_reason": None,
            "subject_observed_at": {
                subject: observed_at
                for subject in ("math", "cs408", "english")
            },
        }
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_manual_subject_pause_keeps_producer_visible_and_consumer_off(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        for subject in ("math", "cs408", "english"):
            gate = self.production_canary_gate(
                subject,
                state="paused_drained" if subject == "math" else "armed",
            )
            payload["dispatchers"][subject]["canary_gate"] = gate
            payload["subjects"][subject]["canary_gate"] = gate
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        math = body["subjects"]["math"]
        self.assertEqual(math["production_status"], "paused")
        self.assertTrue(math["producer_capture_enabled"])
        self.assertFalse(math["luna_consumer_enabled"])
        self.assertEqual(math["blocking_reason"], "paused_by_user")
        health_status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(health_status, 503)
        self.assertEqual(health["required_dispatchers"], 3)
        self.assertEqual(health["ready_dispatchers"], 2)

    def test_legacy_projection_does_not_fabricate_concurrency_zeroes(self) -> None:
        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        self.assertEqual(body["concurrency"]["status"], "unavailable")
        self.assertIsNone(body["global_active_task_count"])
        self.assertIsNone(body["global_peak_active"])
        self.assertIsNone(body["effective_concurrency_limit"])
        self.assertEqual(
            body["subjects"]["math"]["concurrency_status"],
            "unavailable",
        )
        self.assertIsNone(
            body["subjects"]["math"]["subject_active_task_count"]
        )

    def test_summary_exposes_verified_runtime_concurrency_without_canary_limit_alias(self) -> None:
        self.install_verified_concurrency_projection()
        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        self.assertEqual(body["concurrency"]["status"], "verified")
        self.assertEqual(body["global_active_task_count"], 0)
        self.assertEqual(body["global_peak_active"], 0)
        self.assertEqual(
            body["concurrency"]["verified_runner_global_active_task_count"],
            0,
        )
        self.assertEqual(
            body["concurrency"]["scheduler_claim_global_peak_active"],
            0,
        )
        self.assertEqual(
            body["concurrency"]["runner_interval_missing_count_global"],
            0,
        )
        self.assertEqual(
            body["concurrency"]["terminal_by_outcome_global"]["failed"],
            0,
        )
        self.assertEqual(body["effective_concurrency_limit"], 3)
        self.assertEqual(body["available_concurrency_slots"], 3)
        math = body["subjects"]["math"]
        self.assertEqual(math["subject_active_task_count"], 0)
        self.assertEqual(math["subject_pending_task_count"], 0)
        self.assertEqual(math["subject_peak_active"], 0)
        self.assertEqual(math["subject_verified_runner_active_task_count"], 0)
        self.assertEqual(math["subject_runner_interval_missing_count"], 0)
        self.assertEqual(math["scheduler_claim_subject_peak_active"], 0)
        self.assertEqual(math["terminal_task_count"], 0)
        self.assertEqual(math["terminal_failure_bindings"], [])
        self.assertEqual(math["effective_concurrency_limit"], 1)
        self.assertEqual(math["available_concurrency_slots"], 1)
        self.assertEqual(math["canary_gate"]["initial_canary_inflight_limit"], 1)
        self.assertNotIn("per_subject_limit", math["canary_gate"])
        self.assertFalse(math["canary_gate"]["fast_mode_requested"])
        self.assertEqual(
            math["canary_gate"]["fast_mode_effective"], "not_requested"
        )

    def test_failed_drained_siblings_are_visible_but_new_claim_limit_is_zero(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        gate = payload["subjects"]["math"]["canary_gate"]
        first = self.production_canary_gate(
            "math", state="canary_in_flight"
        )["selected"]
        second = dict(first)
        second["unit_sha256"] = "f" * 64
        second["producer_unit_id"] = "CANARY-math-sibling"
        gate.update(
            {
                "state": "failed_drained",
                "luna_consumer_enabled": False,
                "active_task_count": 2,
                "active_selections": {
                    first["unit_sha256"]: first,
                    second["unit_sha256"]: second,
                },
                "selected": None,
                "last_failure_at": datetime.now(timezone.utc).isoformat(),
                "blocking_reason": "canary_task_failed",
                "next_action": "explicit_subject_resume_required",
                "last_analysis_status": "failed_or_incomplete",
                "last_critical_review_status": "failed_or_incomplete",
                "last_report_status": "not_verified",
                "last_terminal_receipt_sha256": "0" * 64,
                "terminal_task_count": 1,
                "terminal_by_outcome": {
                    "succeeded": 0,
                    "failed": 1,
                    "cancelled": 0,
                    "timed_out": 0,
                    "needs_rework": 0,
                },
            }
        )
        payload["dispatchers"]["math"]["canary_gate"] = gate
        section = payload["subjects"]["math"]
        section.update(
            {
                "concurrency_state": "failed_drained",
                "subject_active_task_count": 2,
                "subject_peak_active": 2,
                "subject_verified_runner_active_task_count": 2,
                "subject_runner_evidenced_task_count": 3,
                "scheduler_claim_subject_peak_active": 2,
                "terminal_task_count": 1,
                "terminal_by_outcome": dict(gate["terminal_by_outcome"]),
                "terminal_failure_bindings": [
                    {
                        "unit_sha256": "5" * 64,
                        "outcome": "failed",
                        "error_code": "canary_task_failed",
                        "terminal_kind": "normal",
                        "terminal_receipt_sha256": "0" * 64,
                    }
                ],
                "effective_concurrency_limit": 0,
                "available_concurrency_slots": 0,
                "backpressure_reason": "canary_task_failed",
            }
        )
        telemetry = payload["concurrency"]["source_telemetry"]
        telemetry["active_by_subject"]["math"] = 2
        telemetry["subject_peak_active"]["math"] = 2
        telemetry["verified_runner_active_by_subject"]["math"] = 2
        telemetry["verified_runner_global_active_task_count"] = 2
        telemetry["scheduler_claim_subject_peak_active"]["math"] = 2
        telemetry["scheduler_claim_global_peak_active"] = 2
        telemetry["runner_evidenced_task_count_by_subject"]["math"] = 3
        telemetry["runner_evidenced_task_count_global"] = 3
        telemetry["terminal_task_count_by_subject"]["math"] = 1
        telemetry["terminal_task_count_global"] = 1
        telemetry["terminal_by_outcome_by_subject"]["math"] = dict(
            gate["terminal_by_outcome"]
        )
        telemetry["terminal_by_outcome_global"] = dict(
            gate["terminal_by_outcome"]
        )
        telemetry["terminal_failure_bindings_by_subject"]["math"] = list(
            section["terminal_failure_bindings"]
        )
        telemetry["global_active_task_count"] = 2
        telemetry["global_peak_active"] = 2
        payload["concurrency"].update(
            {
                "global_active_task_count": 2,
                "global_peak_active": 2,
                "verified_runner_global_active_task_count": 2,
                "scheduler_claim_global_peak_active": 2,
                "runner_evidenced_task_count_global": 3,
                "terminal_task_count_global": 1,
                "terminal_by_outcome_global": dict(
                    gate["terminal_by_outcome"]
                ),
                "terminal_failure_binding_count_global": 1,
                "effective_concurrency_limit": 2,
                "available_concurrency_slots": 0,
                "backpressure_reason": "math:canary_task_failed",
            }
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        math = body["subjects"]["math"]
        self.assertEqual(math["subject_active_task_count"], 2)
        self.assertEqual(math["effective_concurrency_limit"], 0)
        self.assertEqual(math["available_concurrency_slots"], 0)
        self.assertFalse(math["luna_consumer_enabled"])
        self.assertEqual(math["terminal_task_count"], 1)
        self.assertEqual(math["terminal_by_outcome"]["failed"], 1)
        self.assertEqual(len(math["terminal_failure_bindings"]), 1)
        self.assertNotIn("path", json.dumps(math["terminal_failure_bindings"]))

    def test_terminal_outcomes_bind_one_failed_unit_among_twenty_closed_tasks(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        gate = self.production_canary_gate(
            "math", state="continuous_concurrent_unlocked"
        )
        outcomes = {
            "succeeded": 19,
            "failed": 1,
            "cancelled": 0,
            "timed_out": 0,
            "needs_rework": 0,
        }
        failure_binding = {
            "unit_sha256": "5" * 64,
            "outcome": "failed",
            "error_code": "analysis_failed",
            "terminal_kind": "normal",
            "terminal_receipt_sha256": "6" * 64,
        }
        gate["terminal_task_count"] = 20
        gate["terminal_by_outcome"] = dict(outcomes)
        payload["dispatchers"]["math"]["canary_gate"] = gate
        payload["subjects"]["math"]["canary_gate"] = gate
        payload["subjects"]["math"].update(
            {
                "concurrency_status": "verified",
                "concurrency_state": "continuous_concurrent_unlocked",
                "concurrency_error_code": None,
                "subject_active_task_count": 0,
                "subject_pending_task_count": 0,
                "subject_peak_active": 4,
                "subject_verified_runner_active_task_count": 0,
                "subject_runner_evidenced_task_count": 20,
                "subject_runner_interval_missing_count": 0,
                "scheduler_claim_subject_peak_active": 5,
                "terminal_index_sha256": gate["terminal_index_sha256"],
                "terminal_task_count": 20,
                "terminal_by_outcome": dict(outcomes),
                "terminal_failure_bindings": [dict(failure_binding)],
                "effective_concurrency_limit": 20,
                "available_concurrency_slots": 20,
                "backpressure_reason": None,
            }
        )
        telemetry = payload["concurrency"]["source_telemetry"]
        telemetry["subject_peak_active"]["math"] = 4
        telemetry["global_peak_active"] = 4
        telemetry["scheduler_claim_subject_peak_active"]["math"] = 5
        telemetry["scheduler_claim_global_peak_active"] = 5
        telemetry["runner_evidenced_task_count_by_subject"]["math"] = 20
        telemetry["runner_evidenced_task_count_global"] = 20
        telemetry["terminal_task_count_by_subject"]["math"] = 20
        telemetry["terminal_task_count_global"] = 20
        telemetry["terminal_by_outcome_by_subject"]["math"] = dict(outcomes)
        telemetry["terminal_by_outcome_global"] = dict(outcomes)
        telemetry["terminal_failure_bindings_by_subject"]["math"] = [
            dict(failure_binding)
        ]
        payload["concurrency"].update(
            {
                "global_peak_active": 4,
                "scheduler_claim_global_peak_active": 5,
                "runner_evidenced_task_count_global": 20,
                "terminal_task_count_global": 20,
                "terminal_by_outcome_global": dict(outcomes),
                "terminal_failure_binding_count_global": 1,
                "effective_concurrency_limit": 22,
                "available_concurrency_slots": 22,
            }
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        math = body["subjects"]["math"]
        self.assertEqual(math["terminal_task_count"], 20)
        self.assertEqual(math["terminal_by_outcome"]["succeeded"], 19)
        self.assertEqual(math["terminal_by_outcome"]["failed"], 1)
        self.assertEqual(math["terminal_failure_bindings"], [failure_binding])
        self.assertEqual(
            body["concurrency"]["terminal_failure_binding_count_global"], 1
        )
        serialized = json.dumps(body, ensure_ascii=False)
        self.assertNotIn("terminal_index_path", serialized)
        self.assertNotIn("terminal_receipt_path", serialized)

    def test_missing_runner_interval_preserves_live_count_but_nulls_actual_peak(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        gate = self.production_canary_gate("math", state="canary_in_flight")
        payload["dispatchers"]["math"]["canary_gate"] = gate
        payload["subjects"]["math"]["canary_gate"] = gate
        payload["subjects"]["math"].update(
            {
                "concurrency_status": "partial",
                "concurrency_state": "first_canary_single_in_flight",
                "concurrency_error_code": "runner_interval_evidence_incomplete",
                "subject_active_task_count": 1,
                "subject_pending_task_count": 0,
                "subject_peak_active": None,
                "subject_verified_runner_active_task_count": 0,
                "subject_runner_evidenced_task_count": 0,
                "subject_runner_interval_missing_count": 1,
                "scheduler_claim_subject_peak_active": 1,
                "terminal_index_sha256": gate["terminal_index_sha256"],
                "terminal_task_count": 0,
                "terminal_by_outcome": dict(gate["terminal_by_outcome"]),
                "terminal_failure_bindings": [],
                "effective_concurrency_limit": 1,
                "available_concurrency_slots": 0,
                "backpressure_reason": "runner_interval_evidence_incomplete",
            }
        )
        telemetry = payload["concurrency"]["source_telemetry"]
        telemetry["active_by_subject"]["math"] = 1
        telemetry["subject_peak_active"]["math"] = 0
        telemetry["global_active_task_count"] = 1
        telemetry["global_peak_active"] = 0
        telemetry["scheduler_claim_subject_peak_active"]["math"] = 1
        telemetry["scheduler_claim_global_peak_active"] = 1
        telemetry["runner_interval_missing_count_by_subject"]["math"] = 1
        telemetry["runner_interval_missing_count_global"] = 1
        payload["concurrency"].update(
            {
                "status": "partial",
                "global_active_task_count": 1,
                "global_peak_active": None,
                "scheduler_claim_global_peak_active": 1,
                "runner_interval_missing_count_global": 1,
                "available_concurrency_slots": 2,
                "backpressure_reason": (
                    "math:runner_interval_evidence_incomplete"
                ),
            }
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, body = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        self.assertEqual(body["concurrency"]["status"], "partial")
        self.assertEqual(body["global_active_task_count"], 1)
        self.assertIsNone(body["global_peak_active"])
        self.assertEqual(
            body["concurrency"]["scheduler_claim_global_peak_active"], 1
        )
        self.assertEqual(
            body["subjects"]["math"]["subject_runner_interval_missing_count"],
            1,
        )
        health_status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(health_status, 503)
        self.assertFalse(health["production_canary_runtime_ready"])

    def test_subject_task_counters_require_reopened_stage_receipts(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        item = self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-COUNTER-SCOPE",
            ordinal=91,
            phase="published",
        )
        for subject in ("math", "cs408", "english"):
            gate = self.production_canary_gate(
                subject,
                state=(
                    "continuous_concurrent_unlocked"
                    if subject == "math"
                    else "armed"
                ),
            )
            if subject == "math":
                gate["last_selected"].update(
                    {
                        "producer_unit_id": item["capture_id"],
                        "unit_sha256": item["unit_sha256"],
                        "frozen_payload_sha256": item[
                            "frozen_payload_sha256"
                        ],
                    }
                )
            payload["dispatchers"][subject]["canary_gate"] = gate
            payload["subjects"][subject]["canary_gate"] = gate
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        detail = json.loads(Path(item["task_detail_path"]).read_text(encoding="utf-8"))
        completion = {
            "unit_sha256": item["unit_sha256"],
            "lease_fence": detail["fence"],
            "subject": "math",
            "capture_id": item["capture_id"],
            "release_id": "1" * 64,
            "outcome": "completed",
            "package_sha256": "c" * 64,
            "receipt_sha256": "d" * 64,
        }
        receipt = {
            "model_contract": {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            "observed_stage_runtime": {
                "analysis": {
                    "model_call_count": 1,
                    "provider_request_count": 1,
                    "mcp_tool_call_count": 2,
                    "consumed_terminal_duplicate_read_count": 0,
                },
                "critical_review": {
                    "model_call_count": 1,
                    "provider_request_count": 1,
                    "mcp_tool_call_count": 3,
                    "consumed_terminal_duplicate_read_count": 0,
                },
            },
        }
        with mock.patch.object(
            dashboard,
            "_verify_completion_authority",
            return_value={"completion": completion, "receipt": receipt},
        ):
            status, _, body = self.json_request(
                "GET", "/api/v1/summary?date=2026-08-04&subject=math"
            )
        self.assertEqual(status, 200)
        math = body["subjects"]["math"]
        self.assertEqual(math["task_counter_scope"], "verified_stage_receipts")
        self.assertEqual(math["task_counter_stage_receipt_count"], 2)
        self.assertEqual(math["model_call_count"], 2)
        self.assertEqual(math["provider_request_count"], 2)
        self.assertEqual(math["mcp_tool_call_count"], 5)
        self.assertEqual(math["activation_observed_mcp_tool_call_count"], 3)
        self.assertEqual(math["control_plane_model_call_count"], 0)
        self.assertEqual(math["control_plane_mcp_tool_call_count"], 0)

        with mock.patch.object(
            dashboard,
            "_verify_completion_authority",
            return_value={"completion": completion},
        ):
            status, _, body = self.json_request(
                "GET", "/api/v1/summary?date=2026-08-04&subject=math"
            )
        self.assertEqual(status, 200)
        math = body["subjects"]["math"]
        self.assertEqual(math["task_counter_scope"], "unavailable")
        self.assertIsNone(math["model_call_count"])
        self.assertIsNone(math["provider_request_count"])
        self.assertIsNone(math["mcp_tool_call_count"])

    def add_dispatch_detail(
        self,
        payload: dict,
        *,
        subject: str,
        capture_id: str,
        ordinal: int,
        phase: str = "analysis_submitted",
        forbidden: bool = False,
    ) -> dict:
        unit_sha = f"{ordinal:064x}"
        frozen_sha = f"{ordinal + 1000:064x}"
        release_id = "1" * 64
        fence = ordinal + 10
        attempt = 1
        event_names = ["claim", "process_started", phase]
        index_root = (
            self.runtime_root
            / "dispatch/state/task-events"
            / unit_sha
            / f"fence-{fence}"
        )
        index_root.mkdir(parents=True, exist_ok=True)
        latest_event_path = None
        latest_event_sha = None
        for sequence, event_name in enumerate(event_names, start=1):
            event = {
                "schema_version": "study-intake-dispatch-task-event-v1",
                "event": event_name,
                "unit_sha256": unit_sha,
                "fence": fence,
                "attempt": attempt,
                "subject": subject,
                "capture_id": capture_id,
                "release_id": release_id,
                "frozen_payload_sha256": frozen_sha,
                "rule_version": "study-intake-concurrent-dispatch-contract-v1",
                "error_code": None,
                "occurred_at": f"2026-08-04T10:{sequence:02d}:00+00:00",
                "formal_write_count": 0,
                "authority": {
                    "algorithm": "HMAC-SHA256",
                    "purpose": "dispatch-task-event",
                    "hmac_sha256": "a" * 64,
                },
            }
            event_bytes = dashboard.canonical_bytes(event)
            event_sha = dashboard.sha256_bytes(event_bytes)
            event_path = (
                self.runtime_root
                / "dispatch/events/sha256"
                / event_sha[:2]
                / f"{event_sha}.json"
            )
            event_path.parent.mkdir(parents=True, exist_ok=True)
            event_path.write_bytes(event_bytes)
            index = {
                "schema_version": "study-intake-dispatch-task-event-index-v1",
                "sequence": sequence,
                "event": event_name,
                "event_sha256": event_sha,
                "event_path": str(event_path),
                "unit_sha256": unit_sha,
                "fence": fence,
                "attempt": attempt,
                "formal_write_count": 0,
            }
            (index_root / f"{sequence:04d}-{event_sha}.json").write_text(
                json.dumps(index), encoding="utf-8"
            )
            latest_event_path = event_path
            latest_event_sha = event_sha
        detail_path = (
            self.runtime_root
            / "dispatch/state/task-details"
            / f"{unit_sha}.json"
        )
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail = {
            "schema_version": "study-intake-dispatch-task-detail-v1",
            "unit_sha256": unit_sha,
            "fence": fence,
            "attempt": attempt,
            "subject": subject,
            "capture_id": capture_id,
            "release_id": release_id,
            "frozen_payload_sha256": frozen_sha,
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "evidence_integrity": "frozen_and_hmac_bound",
            "phase": phase,
            "latest_event_path": str(latest_event_path),
            "latest_event_sha256": latest_event_sha,
            "event_index_root": str(index_root),
            "server_queue_status": "unknown",
            "artifacts": {},
            "updated_at": "2026-08-04T10:04:00+00:00",
            "formal_write_count": 0,
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": "test-key",
                "purpose": "dispatch-task-detail",
                "hmac_sha256": "b" * 64,
            },
        }
        if forbidden:
            detail["private_current_answer"] = "PRIVATE-ANSWER-SENTINEL"
        detail_path.write_text(json.dumps(detail), encoding="utf-8")
        item = {
            "capture_id": capture_id,
            "subject": subject,
            "study_date": "2026-08-04",
            "captured_at": "2026-08-04T10:00:00+08:00",
            "updated_at": "2026-08-04T10:04:00+08:00",
            "target_label": capture_id,
            "queue_state": "running",
            "current_stage": "analysis",
            "luna_status": "processing",
            "unit_sha256": unit_sha,
            "generation": attempt,
            "attempt": attempt,
            "fence": fence,
            "frozen_payload_sha256": frozen_sha,
            "release_id": release_id,
            "input_fingerprint": f"sha256:{frozen_sha}",
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "server_queue_status": "unknown",
            "task_detail_path": str(detail_path),
            "requested_model": "gpt-5.6-luna",
            "requested_reasoning_effort": "max",
        }
        payload["subjects"][subject]["items"].append(item)
        payload["subjects"][subject]["counts"] = dashboard._v3_counts(
            payload["subjects"][subject]["items"]
        )
        return item

    def test_healthz_and_security_headers(self) -> None:
        self.install_verified_concurrency_projection()
        status, headers, payload = self.json_request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["http_alive"])
        self.assertTrue(payload["dispatchers_ready"])
        self.assertEqual(payload["ready_dispatchers"], 3)
        self.assertTrue(payload["projection_available"])
        self.assertEqual(
            payload["dispatchers"]["math"]["requested_model"],
            "gpt-5.6-luna",
        )
        self.assertEqual(
            payload["dispatchers"]["math"]["runtime_identity_status"],
            "requested_unverified",
        )
        self.assertNotIn("private_log_path", payload["dispatchers"]["math"])
        self.assertEqual(headers["Cache-Control"], "no-store, max-age=0")
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertIn("connect-src 'self'", headers["Content-Security-Policy"])
        self.assertNotIn("Access-Control-Allow-Origin", headers)

    def test_summary_exposes_path_free_independent_production_canary_state(self) -> None:
        self.install_production_canary_gates(failed_subject="cs408")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            summary["subjects"]["math"]["production_status"],
            "production_canary_active",
        )
        failed = summary["subjects"]["cs408"]
        self.assertEqual(failed["production_status"], "failed")
        self.assertTrue(failed["producer_capture_enabled"])
        self.assertFalse(failed["luna_consumer_enabled"])
        self.assertEqual(failed["queue_depth"], 2)
        self.assertEqual(failed["oldest_pending_age_seconds"], 45)
        self.assertEqual(failed["formal_write_count"], 0)
        self.assertFalse(failed["sol_enabled"])
        self.assertEqual(failed["blocking_reason"], "canary_task_failed")
        self.assertNotIn("authority", failed["canary_gate"])
        self.assertNotIn("activation_receipt_path", failed["canary_gate"])
        self.assertEqual(
            summary["dispatchers"]["english"]["stage_timeout_seconds"],
            1800,
        )
        self.assertEqual(
            summary["dispatchers"]["math"]["stage_timeout_seconds"],
            3600,
        )

    def test_legacy_priority_campaign_is_historical_never_live(self) -> None:
        projection = {
            "schema_version": "study-intake-three-subject-concurrency-campaign-v1",
            "status": "passed",
            "result_label": "three_subject_concurrency_runtime_accepted",
            "production_accepted": False,
            "fast_mode": {
                "requested": True,
                "service_tier": "priority",
                "effective_status": "requested_unverified",
            },
            "subjects": {
                subject: {"subject": subject}
                for subject in ("math", "cs408", "english")
            },
        }
        store = mock.Mock()
        store.load.return_value = mock.Mock(
            available=True,
            projection=projection,
            error=None,
            error_code=None,
        )
        with mock.patch.object(self.httpd, "campaign_store", store):
            status, _, body = self.json_request(
                "GET", "/api/v1/concurrency-campaign?subject=all"
            )
        self.assertEqual(status, 200)
        self.assertTrue(body["available"])
        self.assertEqual(body["status"], "historical_legacy")
        self.assertEqual(body["historical_status"], "passed")
        self.assertEqual(body["evidence_scope"], "historical_legacy_v1")
        self.assertFalse(body["current_release_usable"])
        self.assertFalse(body["production_accepted"])

    def test_summary_exposes_closed_preclaim_failure_receipt(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        for subject in ("math", "cs408", "english"):
            gate = self.production_canary_gate(
                subject,
                state="failed_drained" if subject == "math" else "armed",
                queue_depth=1 if subject == "math" else 0,
                oldest_pending_age_seconds=10 if subject == "math" else None,
            )
            if subject == "math":
                gate.update(
                    {
                        "blocking_reason": "generation_fence_failed",
                        "last_preclaim_failure_at": gate["last_failure_at"],
                        "last_preclaim_failure_stage": "submit_generation_fence",
                        "last_preclaim_failure_error_code": "generation_fence_failed",
                        "last_preclaim_failure_receipt_sha256": "9" * 64,
                        "last_preclaim_failure_evidence_sha256": "8" * 64,
                    }
                )
            payload["dispatchers"][subject]["canary_gate"] = gate
            payload["subjects"][subject]["canary_gate"] = gate
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        gate = summary["subjects"]["math"]["canary_gate"]
        self.assertEqual(
            gate["last_preclaim_failure_stage"], "submit_generation_fence"
        )
        self.assertEqual(
            gate["last_preclaim_failure_receipt_sha256"], "9" * 64
        )
        self.assertEqual(summary["subjects"]["math"]["production_status"], "failed")

    def test_preclaim_failure_does_not_advance_analysis_or_critical_review(self) -> None:
        items = [
            {
                "queue_state": "failed",
                "current_stage": "failed",
                "local_model_submitted": False,
                "luna_status": "failed",
            }
        ]
        counts = dashboard._counts(items)
        self.assertEqual(
            dashboard._pipeline(items, counts),
            {
                "capture": 1,
                "analysis": 0,
                "critical_review": 0,
                "quality_ready": 0,
                "sol_reviewed": 0,
                "formal_apply": 0,
            },
        )

    def test_canary_capacity_distinguishes_current_and_unlocked_limits(self) -> None:
        self.install_production_canary_gates()
        status, _, summary = self.json_request("GET", "/api/v1/summary")
        self.assertEqual(status, 200)
        self.assertEqual(summary["concurrency"]["capacity_phase"], "initial_canary")
        self.assertEqual(summary["concurrency"]["initial_canary_global_limit"], 3)
        self.assertEqual(
            summary["concurrency"]["configured_continuous_global_limit"], 60
        )
        for subject in ("math", "cs408", "english"):
            section = summary["subjects"][subject]
            self.assertEqual(section["initial_canary_inflight_limit"], 1)
            self.assertEqual(section["continuous_concurrency_limit"], 20)
            self.assertEqual(section["concurrency_capacity_phase"], "initial_canary")

    def test_partial_production_canary_topology_fails_closed(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        gate = self.production_canary_gate("math")
        payload["dispatchers"]["math"]["canary_gate"] = gate
        payload["subjects"]["math"]["canary_gate"] = gate
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )

        self.assertEqual(status, 503)
        self.assertEqual(summary["error"], "projection_v3_canary_topology_partial")

    def test_cross_release_production_canary_fails_closed(self) -> None:
        self.install_production_canary_gates()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["english"]["canary_gate"]["release_id"] = "9" * 64
        payload["dispatchers"]["english"]["canary_gate"] = payload["subjects"]["english"]["canary_gate"]
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )

        self.assertEqual(status, 503)
        self.assertEqual(summary["error"], "projection_v3_canary_identity_invalid")

    def test_item_release_must_match_subject_dispatcher_release(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"][0]["release_id"] = "2" * 64
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        for path in (
            "/api/v1/summary?date=2026-08-04&subject=math",
            "/api/v1/items?date=2026-08-04&subject=math",
            "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
        ):
            status, _, response = self.json_request("GET", path)
            self.assertEqual(status, 503)
            self.assertEqual(
                response["error"], "projection_v3_task_release_mismatch"
            )

    def test_live_v2_missing_requested_service_tier_fails_closed(self) -> None:
        self.install_production_canary_gates()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["canary_gate"].pop(
            "requested_service_tier"
        )
        payload["dispatchers"]["math"]["canary_gate"] = payload["subjects"][
            "math"
        ]["canary_gate"]
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )

        self.assertEqual(status, 503)
        self.assertEqual(
            summary["error"], "projection_v3_canary_runtime_scope_invalid"
        )

    def test_healthz_stale_heartbeat_is_degraded_while_http_is_alive(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["dispatchers"]["math"]["last_heartbeat"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dashboard.HEARTBEAT_READY_MAX_AGE_SECONDS + 5)
        ).isoformat()
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertEqual(health["status"], "degraded")
        self.assertTrue(health["http_alive"])
        self.assertFalse(health["worker_ready"])
        self.assertTrue(health["projection_available"])
        self.assertEqual(health["error"], "dispatcher_topology_degraded")
        self.assertGreaterEqual(
            health["services"]["math"]["heartbeat_age_seconds"],
            dashboard.HEARTBEAT_READY_MAX_AGE_SECONDS,
        )
        self.assertEqual(
            health["services"]["math"]["error"],
            "dispatcher_heartbeat_stale",
        )

    def test_healthz_release_mismatch_is_degraded(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["dispatchers"]["math"]["release_id"] = "2" * 64
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["error"], "projection_v3_release_topology_invalid")

    def test_summary_recomputes_real_counts_and_pipeline(self) -> None:
        status, _, payload = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=all",
        )
        self.assertEqual(status, 200)
        self.assertTrue(payload["available"])
        self.assertEqual(payload["counts"]["selected"], 5)
        self.assertEqual(payload["counts"]["queued"], 1)
        self.assertEqual(payload["counts"]["analysis_running"], 1)
        self.assertEqual(payload["counts"]["critical_review_running"], 0)
        self.assertEqual(payload["counts"]["quality_passed"], 3)
        self.assertEqual(payload["counts"]["terminal"], 3)
        self.assertEqual(payload["counts"]["reviewed"], 1)
        self.assertEqual(payload["pipeline"], {
            "capture": 5,
            "analysis": 4,
            "critical_review": 3,
            "quality_ready": 3,
            "sol_reviewed": 1,
            "formal_apply": 0,
        })
        self.assertEqual(payload["counts"]["processed"], 3)
        self.assertEqual(payload["counts"]["remaining"], 2)
        self.assertEqual(payload["counts"]["failures"], 0)
        observability = payload["observability"]
        self.assertEqual(observability["processed"], 3)
        self.assertEqual(observability["remaining"], 2)
        self.assertEqual(observability["two_pass"], {
            "ready": 1,
            "single_pass_degraded": 0,
        })
        self.assertEqual(observability["evidence"], {
            "items_reported": 1,
            "claims": 10,
            "claims_with_refs": 10,
            "coverage_pct": 100.0,
        })
        self.assertEqual(observability["visual"], {
            "items_reported": 1,
            "claims": 4,
            "claims_with_refs": 4,
            "coverage_pct": 100.0,
            "required_items": 1,
        })
        self.assertEqual(observability["adoption"], {
            "direct": 1,
            "minor": 0,
            "major": 0,
            "legacy_modified": 0,
            "rejected": 0,
            "fallback": 0,
            "unknown": 4,
            "receipted": 1,
        })
        self.assertEqual(observability["receipts"], {
            "adoption": 1,
            "stage": 1,
            "quality": 1,
        })
        self.assertEqual(observability["runtime"]["confirmed_models"], [])
        self.assertTrue(payload["subjects"]["cs408"]["enabled"])
        self.assertEqual(payload["subjects"]["cs408"]["status"], "active")
        self.assertEqual(payload["subjects"]["cs408"]["counts"]["selected"], 1)

    def test_v3_exposes_closed_subject_buckets_stage_and_dispatcher_identity(self) -> None:
        status, _, payload = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        for subject, section in payload["subjects"].items():
            with self.subTest(subject=subject):
                self.assertEqual(
                    section["counts"]["queued"]
                    + section["counts"]["analysis_running"]
                    + section["counts"]["critical_review_running"]
                    + section["counts"]["terminal"]
                    + section["counts"]["evidence_pending"],
                    section["counts"]["selected"],
                )
                self.assertEqual(
                    section["counts"]["quality_passed"]
                    + section["counts"]["needs_rework"]
                    + section["counts"]["failed"],
                    section["counts"]["terminal"],
                )
                self.assertIn(section["current_stage"], dashboard.CURRENT_STAGES)
                self.assertEqual(
                    payload["dispatchers"][subject]["requested_model"],
                    "gpt-5.6-luna",
                )
                self.assertEqual(
                    payload["dispatchers"][subject]["requested_reasoning_effort"],
                    "max",
                )

    def test_en_p0_006_status_is_explicit_no_data_without_source_data(self) -> None:
        status, _, payload = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["en_p0_006_status"], "no_data")
        section = payload["en_p0_006"]
        self.assertFalse(section["state_available"])
        self.assertIsNone(section["inventory"]["target_count"])
        self.assertIsNone(section["luna"]["selected_count"])
        self.assertIsNone(section["sol"]["committed_count"])

    def test_en_p0_006_status_is_allowlisted_and_invalid_values_fail_closed(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["en_p0_006_status"] = "authorization_ready"
        payload["en_p0_006"].update({
            "state_available": True,
            "status": "authorization_ready",
        })
        payload["en_p0_006"]["inventory"] = {
            "state_available": True,
            "target_count": 98,
            "inventory_sha256": "a" * 64,
            "target_set_sha256": "b" * 64,
            "batch_authorization_sha256": "c" * 64,
            "source": "candidate_bound_work_item_batch",
        }
        payload["en_p0_006"]["luna"] = {
            "state_available": True,
            "status": "not_started",
            "selected_count": 0,
            "running_count": 0,
            "terminal_count": 0,
            "quality_passed_count": 0,
            "failed_count": 0,
            "remaining_count": 98,
            "all_terminal": False,
            "quality_ready": False,
        }
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["en_p0_006_status"], "authorization_ready")
        self.assertEqual(summary["en_p0_006"]["inventory"]["target_count"], 98)
        self.assertEqual(summary["en_p0_006"]["luna"]["selected_count"], 0)

        payload["en_p0_006_status"] = "closed_without_verified_receipts"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 503)
        self.assertEqual(
            response["error"], "projection_v3_en_p0_006_status_invalid"
        )

        payload["en_p0_006_status"] = {"status": "verified_complete"}
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 503)
        self.assertEqual(
            response["error"], "projection_v3_en_p0_006_status_invalid"
        )

        payload["en_p0_006_status"] = "authorization_ready"
        payload["en_p0_006"]["luna"]["remaining_count"] = 0
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 503)
        self.assertEqual(
            response["error"], "projection_v3_en_p0_006_luna_counts_invalid"
        )

    def test_en_p0_006_recovery_and_verified_closure_are_public_and_closed(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["en_p0_006_status"] = "recovering"
        payload["en_p0_006"] = {
            "state_available": True,
            "status": "recovering",
            "blocker_codes": [],
            "inventory": {
                "state_available": True,
                "target_count": 98,
                "inventory_sha256": "a" * 64,
                "target_set_sha256": "b" * 64,
                "batch_authorization_sha256": "c" * 64,
                "source": "verified_sol_batch",
            },
            "luna": {
                "state_available": True,
                "status": "quality_ready",
                "selected_count": 98,
                "running_count": 0,
                "terminal_count": 98,
                "quality_passed_count": 98,
                "failed_count": 0,
                "remaining_count": 0,
                "all_terminal": True,
                "quality_ready": True,
            },
            "sol": {
                "state_available": True,
                "status": "recovering",
                "batch_id": "EN-P0-006-BATCH-001",
                "current_ordinal": 42,
                "current_target_id": "sentence_pattern_card:SP-022",
                "fencing_token": 7,
                "attempt": 2,
                "committed_count": 38,
                "already_current_count": 3,
                "failed_count": 0,
                "failure_attempt_count": 1,
                "recovery_count": 1,
                "recovery_pending_count": 1,
                "formal_write_count": 38,
                "final_closure_status": None,
                "execution_closure_sha256": None,
                "updated_at": "2026-08-09T09:10:11+00:00",
            },
        }
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["en_p0_006_status"], "recovering")
        sol = summary["en_p0_006"]["sol"]
        self.assertEqual(sol["current_ordinal"], 42)
        self.assertEqual(sol["committed_count"], 38)
        self.assertEqual(sol["already_current_count"], 3)
        self.assertEqual(sol["recovery_pending_count"], 1)

        payload["en_p0_006_status"] = "verified_complete"
        payload["en_p0_006"]["status"] = "verified_complete"
        payload["en_p0_006"]["sol"].update({
            "status": "complete",
            "current_ordinal": None,
            "current_target_id": None,
            "committed_count": 87,
            "already_current_count": 11,
            "failed_count": 0,
            "recovery_pending_count": 0,
            "formal_write_count": 87,
            "final_closure_status": "verified_complete",
            "execution_closure_sha256": "d" * 64,
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["en_p0_006_status"], "verified_complete")
        self.assertEqual(
            summary["en_p0_006"]["sol"]["final_closure_status"],
            "verified_complete",
        )

        payload["en_p0_006"]["sol"]["recovery_pending_count"] = 1
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 503)
        self.assertEqual(
            response["error"], "projection_v3_en_p0_006_verified_complete_invalid"
        )

    def test_health_never_uses_one_dispatcher_as_whole_system_health(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["dispatchers"]["english"]["last_heartbeat"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dashboard.HEARTBEAT_READY_MAX_AGE_SECONDS + 5)
        ).isoformat()
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertEqual(health["ready_dispatchers"], 2)
        self.assertTrue(health["services"]["math"]["ready"])
        self.assertTrue(health["services"]["cs408"]["ready"])
        self.assertFalse(health["services"]["english"]["ready"])

    def test_health_requires_verified_concurrency_and_sol_safety(self) -> None:
        self.install_verified_concurrency_projection()
        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertTrue(health["http_ready"])
        self.assertTrue(health["projection_fresh"])
        self.assertTrue(health["luna_processing_ready"])
        self.assertTrue(health["sol_writer_ready"])
        self.assertTrue(health["sol_safety_ready"])
        self.assertTrue(health["production_canary_runtime_ready"])

        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["global_sol"] = {
            "state_available": False,
            "status": "no_data",
            "active_subject": None,
            "active_batch_id": None,
            "current_item": None,
            "authorized_queue": [],
            "fencing_token": None,
            "active_writer_count": None,
            "committed_count": None,
            "remaining_count": None,
            "formal_write_count": None,
            "updated_at": None,
        }
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertTrue(health["http_ready"])
        self.assertTrue(health["projection_fresh"])
        self.assertTrue(health["luna_processing_ready"])
        self.assertTrue(health["sol_safety_ready"])

        payload["global_sol"] = json.loads(
            (DASHBOARD_DIR / "tests/fixtures/dashboard_projection.json").read_text(
                encoding="utf-8"
            )
        )["global_sol"]
        payload["global_sol"].update(
            {
                "status": "applying",
                "active_subject": "math",
                "active_batch_id": "MATH-BATCH-001",
                "current_item": "MFI-20260804-001",
                "fencing_token": 1,
                "active_writer_count": 1,
                "committed_count": 0,
                "remaining_count": 1,
                "formal_write_count": 0,
            }
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertFalse(health["sol_safety_ready"])

    def test_health_missing_concurrency_is_never_ready(self) -> None:
        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertFalse(health["production_canary_runtime_ready"])
        self.assertEqual(health["error"], "production_canary_runtime_unverified")

    def test_health_idle_immutable_telemetry_may_be_older_than_90_seconds(
        self,
    ) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dashboard.PROJECTION_READY_MAX_AGE_SECONDS + 5)
        ).isoformat()
        payload["concurrency"]["source_telemetry"]["updated_at"] = stale
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertTrue(health["projection_fresh"])
        self.assertTrue(health["production_canary_runtime_ready"])

    def test_health_stale_runtime_observation_is_never_ready(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        stale = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dashboard.PROJECTION_READY_MAX_AGE_SECONDS + 5)
        ).isoformat()
        payload["subjects"]["math"]["concurrency_observed_at"] = stale
        payload["concurrency"]["subject_observed_at"]["math"] = stale
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertFalse(health["production_canary_runtime_ready"])
        self.assertEqual(health["error"], "production_canary_runtime_unverified")

    def test_health_rejects_source_telemetry_too_far_in_future(self) -> None:
        self.install_verified_concurrency_projection()
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        future = (
            datetime.now(timezone.utc)
            + timedelta(seconds=dashboard.HEARTBEAT_FUTURE_SKEW_SECONDS + 5)
        ).isoformat()
        payload["concurrency"]["source_telemetry"]["updated_at"] = future
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, health = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertFalse(health["production_canary_runtime_ready"])
        self.assertEqual(health["error"], "production_canary_runtime_unverified")

    def test_health_rejects_source_telemetry_release_activation_and_hmac_drift(
        self,
    ) -> None:
        mutations = {
            "release": (
                lambda telemetry: telemetry.update({"release_id": "9" * 64}),
                "projection_v3_concurrency_telemetry_invalid",
            ),
            "activation": (
                lambda telemetry: telemetry["activation_ids"].update(
                    {"math": "9" * 64}
                ),
                "projection_v3_concurrency_binding_invalid",
            ),
            "hmac": (
                lambda telemetry: telemetry["authority"].update(
                    {"hmac_sha256": "9" * 64}
                ),
                "projection_v3_concurrency_telemetry_invalid",
            ),
        }
        for drift, (mutate, expected_error) in mutations.items():
            with self.subTest(drift=drift):
                self.install_verified_concurrency_projection()
                payload = json.loads(
                    self.projection_path.read_text(encoding="utf-8")
                )
                mutate(payload["concurrency"]["source_telemetry"])
                self.projection_path.write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )

                status, _, health = self.json_request("GET", "/healthz")
                self.assertEqual(status, 503)
                self.assertFalse(health["production_canary_runtime_ready"])
                self.assertEqual(health["error"], expected_error)

    def test_all_terminal_with_failure_is_not_sol_ready(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        items = payload["subjects"]["math"]["items"]
        for item in items:
            item["queue_state"] = "ready"
            item["current_stage"] = "quality_ready"
            item["quality_outcome"] = "accepted"
        items[-1]["queue_state"] = "failed"
        items[-1]["current_stage"] = "failed"
        items[-1]["quality_outcome"] = "failed"
        section = payload["subjects"]["math"]
        section["counts"] = dashboard._v3_counts(items)
        section.update({
            "batch_state_available": True,
            "batch_id": "MATH-BATCH-001",
            "batch_status": "frozen",
            "capture_high_watermark": {
                "value": "watermark-7",
                "authority_generation": "generation-7",
            },
            "authority_generation": "generation-7",
            "all_terminal": True,
            "sol_ready": False,
            "blockers": ["blocking_task:MFI-20260804-004"],
            "sol_handoff_status": "not_ready",
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        math = summary["subjects"]["math"]
        self.assertTrue(math["all_terminal"])
        self.assertFalse(math["sol_ready"])
        self.assertEqual(math["counts"]["terminal"], 4)
        self.assertEqual(math["counts"]["failed"], 1)

    def test_all_quality_passed_is_separately_sol_ready(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        items = payload["subjects"]["math"]["items"]
        for item in items:
            item["queue_state"] = "ready"
            item["current_stage"] = "quality_ready"
            item["quality_outcome"] = "accepted"
        section = payload["subjects"]["math"]
        section["counts"] = dashboard._v3_counts(items)
        section.update({
            "batch_state_available": True,
            "batch_id": "MATH-BATCH-READY",
            "batch_status": "frozen",
            "capture_high_watermark": {
                "value": "watermark-8",
                "authority_generation": "generation-8",
            },
            "authority_generation": "generation-8",
            "all_terminal": True,
            "sol_ready": True,
            "blockers": [],
            "sol_handoff_status": "awaiting_authorization",
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        self.assertTrue(summary["subjects"]["math"]["sol_ready"])
        self.assertEqual(
            summary["subjects"]["math"]["counts"]["quality_passed"], 4
        )

    def test_other_subjects_continuing_requires_active_sol_and_fresh_current_heartbeats(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["global_sol"].update({
            "status": "applying",
            "active_subject": "math",
            "active_batch_id": "MATH-BATCH-001",
            "current_item": "MFI-20260804-001",
            "fencing_token": 9,
            "active_writer_count": 1,
            "committed_count": 1,
            "remaining_count": 3,
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        continuity = summary["other_subjects_luna_continuing"]
        self.assertTrue(continuity["confirmed"])
        self.assertEqual(continuity["subjects"], ["cs408", "english"])

        payload["dispatchers"]["english"]["previous_release_heartbeat"] = True
        payload["dispatchers"]["english"]["heartbeat_release_id"] = "2" * 64
        payload["dispatchers"]["english"]["heartbeat_generation_status"] = (
            "previous_release_heartbeat"
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertFalse(summary["other_subjects_luna_continuing"]["confirmed"])
        self.assertEqual(
            summary["service_health"]["services"]["english"]["error"],
            "previous_release_heartbeat",
        )

    def test_global_sol_review_and_commit_are_not_conflated(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["sol_reviewed_count"] = 3
        payload["subjects"]["math"]["sol_committed_count"] = 1
        payload["global_sol"]["formal_write_count"] = 1
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["subjects"]["math"]["sol_reviewed_count"], 3)
        self.assertEqual(summary["subjects"]["math"]["sol_committed_count"], 1)
        self.assertEqual(summary["global_sol"]["formal_write_count"], 1)

    def test_single_pass_degraded_is_not_a_deliverable_ready_package(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"][0]["luna_status"] = (
            "single_pass_degraded"
        )
        payload["subjects"]["math"]["items"][0]["queue_state"] = (
            "evidence_pending"
        )
        payload["subjects"]["math"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["math"]["items"]
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["single_pass_degraded"], 1)
        self.assertEqual(summary["counts"]["package_ready"], 2)
        self.assertEqual(summary["pipeline"]["critical_review"], 2)
        self.assertEqual(summary["pipeline"]["quality_ready"], 2)

        _, _, javascript = self.request("GET", "/app.js")
        source = javascript.decode("utf-8")
        self.assertNotIn("single_pass_degraded", source)
        self.assertIn('item.execution_status === "succeeded"', source)
        self.assertIn('item.report_available !== true', source)
        self.assertIn(
            'item.quality_status === "passed" && item.report_disposition === "accepted"',
            source,
        )

    def test_cs408_view_exposes_real_subject_item(self) -> None:
        status, _, summary = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=cs408",
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["selected"], 1)
        self.assertEqual(summary["counts"]["quality_passed"], 1)
        self.assertTrue(summary["subjects"]["cs408"]["enabled"])

        status, _, rows = self.json_request(
            "GET",
            "/api/v1/items?date=2026-08-04&subject=cs408",
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(rows["items"]), 1)
        self.assertEqual(rows["items"][0]["subject"], "cs408")
        self.assertEqual(rows["items"][0]["target_label"], "OS_2020_001")

    def test_public_items_expose_shared_execution_and_quality_axes(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][0]
        capture_id = item["capture_id"]
        item.update(
            {
                "queue_state": "ready",
                "terminal_status": "succeeded",
                "execution_status": "succeeded",
                "quality_status": "issues_found",
                "quality_outcome": "needs_sol_review",
                "report_disposition": "needs_sol_review",
                "report_available": True,
                "sol_review_status": "pending",
                "formal_write_eligible": False,
                "production_accepted": False,
                "terminal_error_code": None,
                "warning_codes": ["math:quality_finding"],
            }
        )
        payload["subjects"]["math"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["math"]["items"]
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        public = next(
            row for row in rows["items"] if row["capture_id"] == capture_id
        )
        self.assertEqual(public["execution_status"], "succeeded")
        self.assertEqual(public["quality_status"], "issues_found")
        self.assertEqual(public["report_disposition"], "needs_sol_review")
        self.assertEqual(public["sol_review_status"], "pending")
        self.assertTrue(public["report_available"])
        self.assertFalse(public["formal_write_eligible"])
        self.assertFalse(public["production_accepted"])
        self.assertNotIn("terminal_error_code", public)
        self.assertEqual(public["warning_codes"], ["math:quality_finding"])

        _, _, javascript = self.request("GET", "/app.js")
        source = javascript.decode("utf-8")
        self.assertIn('item.execution_status === "succeeded"', source)
        self.assertIn('item.quality_status === "issues_found"', source)
        self.assertIn("等待 Sol 质量复核", source)

    def test_english_empty_state_is_a_real_enabled_subject(self) -> None:
        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["selected"], 0)
        self.assertTrue(summary["subjects"]["english"]["enabled"])
        self.assertEqual(summary["subjects"]["english"]["status"], "active")

        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        self.assertEqual(rows["items"], [])

    def test_english_article_sentence_status_and_receipt_are_public_but_content_is_private(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["english"]["items"].append({
            "capture_id": "EN-BATCH-2011-T4-001",
            "unit_sha256": "e1" * 32,
            "generation": 0,
            "frozen_payload_sha256": "e2" * 32,
            "release_id": "1" * 64,
            "input_fingerprint": "sha256:english-batch-001",
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "server_queue_status": "unknown",
            "subject": "english",
            "study_date": "2026-08-04",
            "captured_at": "2026-08-04T20:00:00+08:00",
            "updated_at": "2026-08-04T20:03:00+08:00",
            "target_label": "2011 Text 4 · S01–S03",
            "article_id": "2011-english-i-text-4",
            "sentence_id": "S01-S03",
            "sentence_count": 3,
            "capture_count": 5,
            "batch_trigger": "capture_threshold",
            "english_status": "luna_ready",
            "luna_status": "ready",
            "queue_state": "ready",
            "current_stage": "quality_ready",
            "pipeline_status": "ready",
            "candidate_validation_status": "PASS",
            "candidate_count": 5,
            "candidate_item_count": 3,
            "package_sha256": "a" * 64,
            "event_written": True,
            "dispatcher_accepted": True,
            "package_visible": True,
            "quick_intake_complete": True,
            "foreground_completion_stage": "package_visible",
            "selected_by": "signed_latest_authoritative",
            "selector_type": "signed_latest_authoritative",
            "selector_sha256": "b" * 64,
            "proposal_sha256": "c" * 64,
            "authoritative_package_sha256": "a" * 64,
            "processing_outcome": "succeeded",
            "selection_authority_status": "hmac_verified",
            "receipt_status": "published",
            "receipt_count": 1,
            "blocker_count": 0,
            "requested_model": "gpt-5.6-luna",
            "requested_reasoning_effort": "max",
            "runtime_identity_status": "confirmed",
            "observed_model": "gpt-5.6-luna",
            "observed_reasoning_effort": "max",
            "observed_identity_provenance": "codex_json_attestation_v1",
            "safe_summary": "3 个句子候选已通过确定性校验。",
            "source_sentence": "must-not-leak sentence",
            "user_verbatim": "must-not-leak learner text",
            "final_answer": "must-not-leak answer",
            "candidate_path": "/Users/example/private-candidate.json",
        })
        payload["subjects"]["english"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["english"]["items"]
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(rows["items"]), 1)
        item = rows["items"][0]
        self.assertEqual(item["article_id"], "2011-english-i-text-4")
        self.assertEqual(item["sentence_count"], 3)
        self.assertEqual(item["capture_count"], 5)
        self.assertEqual(item["candidate_count"], 5)
        self.assertEqual(item["candidate_item_count"], 3)
        self.assertEqual(item["package_sha256"], "a" * 64)
        self.assertTrue(item["event_written"])
        self.assertTrue(item["dispatcher_accepted"])
        self.assertTrue(item["package_visible"])
        self.assertTrue(item["quick_intake_complete"])
        self.assertEqual(item["selected_by"], "signed_latest_authoritative")
        self.assertEqual(item["selector_sha256"], "b" * 64)
        self.assertEqual(item["proposal_sha256"], "c" * 64)
        self.assertEqual(item["authoritative_package_sha256"], "a" * 64)
        self.assertEqual(item["receipt_count"], 1)
        self.assertNotIn("observed_model", item)
        self.assertNotIn("observed_reasoning_effort", item)
        serialized = json.dumps(rows, ensure_ascii=False)
        self.assertNotIn("must-not-leak", serialized)
        self.assertNotIn("/Users/", serialized)

    def test_english_queued_display_status_tracks_worker_freshness_only(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["english"]["items"].append(
            {
                "capture_id": "EN-QUEUED-001",
                "unit_sha256": "e3" * 32,
                "generation": 0,
                "frozen_payload_sha256": "e4" * 32,
                "release_id": "1" * 64,
                "input_fingerprint": "sha256:english-queued-001",
                "rule_version": "study-intake-concurrent-dispatch-contract-v1",
                "server_queue_status": "unknown",
                "subject": "english",
                "study_date": "2026-08-04",
                "captured_at": "2026-08-04T20:00:00+08:00",
                "updated_at": "2026-08-04T20:00:00+08:00",
                "target_label": "source-1 · S01",
                "article_id": "source-1",
                "sentence_id": "S01",
                "sentence_count": 1,
                "capture_count": 1,
                "candidate_count": 1,
                "english_status": "queued",
                "luna_status": "queued",
                "queue_state": "queued",
                "current_stage": "queued",
                "pipeline_status": "queued",
                "event_written": True,
                "dispatcher_accepted": False,
                "package_visible": False,
                "quick_intake_complete": False,
                "foreground_completion_stage": "event_written",
                "selected_by": "none",
                "selector_type": "signed_latest_authoritative",
                "selector_sha256": None,
                "proposal_sha256": None,
                "authoritative_package_sha256": None,
                "processing_outcome": None,
                "selection_authority_status": "missing",
                "requested_model": "gpt-5.6-luna",
                "requested_reasoning_effort": "max",
            }
        )
        payload["subjects"]["english"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["english"]["items"]
        )
        payload["dispatchers"]["english"]["last_heartbeat"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dashboard.HEARTBEAT_READY_MAX_AGE_SECONDS + 5)
        ).isoformat()
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        self.assertEqual(rows["items"][0]["english_status"], "queued")
        self.assertEqual(
            rows["items"][0]["display_status"], "queued_worker_offline"
        )

        payload["dispatchers"]["english"]["last_heartbeat"] = datetime.now(
            timezone.utc
        ).isoformat()
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        self.assertEqual(rows["items"][0]["english_status"], "queued")
        self.assertNotIn("display_status", rows["items"][0])

    def test_english_signed_latest_failure_is_publicly_unselected(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        failed_item = {
            "capture_id": "EN-LATEST-FAILED-001",
            "unit_sha256": "e5" * 32,
            "generation": 0,
            "frozen_payload_sha256": "e6" * 32,
            "release_id": "1" * 64,
            "input_fingerprint": "sha256:english-latest-failed-001",
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "server_queue_status": "unknown",
            "subject": "english",
            "study_date": "2026-08-04",
            "updated_at": "2026-08-04T20:05:00+08:00",
            "target_label": "latest failure, historical success retained privately",
            "queue_state": "failed",
            "current_stage": "failed",
            "luna_status": "failed",
            "event_written": True,
            "dispatcher_accepted": True,
            "package_visible": False,
            "quick_intake_complete": False,
            "foreground_completion_stage": "dispatcher_accepted",
            "selected_by": "none",
            "selector_type": "signed_latest_authoritative",
            "selector_sha256": "d" * 64,
            "proposal_sha256": None,
            "authoritative_package_sha256": None,
            "processing_outcome": "failed",
            "selection_authority_status": "hmac_verified",
            "historical_package_sha256": "f" * 64,
            "candidate_path": "/Users/example/private-historical-package.json",
        }
        payload["subjects"]["english"]["items"].append(failed_item)
        payload["subjects"]["english"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["english"]["items"]
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, rows = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 200)
        item = rows["items"][0]
        self.assertEqual(item["selected_by"], "none")
        self.assertFalse(item["package_visible"])
        self.assertFalse(item["quick_intake_complete"])
        self.assertEqual(item["selector_sha256"], "d" * 64)
        self.assertIsNone(item["proposal_sha256"])
        self.assertIsNone(item["authoritative_package_sha256"])
        self.assertNotIn("package_sha256", item)
        serialized = json.dumps(rows, ensure_ascii=False)
        self.assertNotIn("historical_package_sha256", serialized)
        self.assertNotIn("/Users/", serialized)

        payload["subjects"]["english"]["items"][0]["package_sha256"] = "f" * 64
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=english"
        )
        self.assertEqual(status, 503)
        self.assertEqual(
            response["error"],
            "projection_v3_english_authority_closure_invalid",
        )

    def test_cs408_v2_list_and_detail_never_expose_private_answer_material(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["cs408"]["items"][0]
        item.update({
            "luna_status": "two_pass_ready",
            "quick_capture_status": "awaiting_daily_curation",
            "luna_report_status": "two_pass_ready",
            "sol_review_status": "not_started",
            "formal_curation_status": "not_started",
            "runtime_identity_status": "confirmed",
            "runtime_model": "gpt-5.6-luna",
            "runtime_reasoning_effort": "max",
            "processor_attribution": "confirmed_luna_max",
            "processor_display_name": "Luna Max（运行时已确认）",
        })
        for key in ("first_break", "suggestion_summary", "evidence_refs"):
            item.pop(key, None)
        private_answer = "PRIVATE-ANSWER-SENTINEL-DO-NOT-EXPOSE"
        report_sha = "a" * 64
        markdown_sha = "b" * 64
        item.update({
            "report_json_ref": f"study-intake-report://sha256/{report_sha}",
            "report_json_sha256": report_sha,
            "report_markdown_ref": (
                f"study-intake-report-markdown://sha256/{markdown_sha}"
            ),
            "report_markdown_sha256": markdown_sha,
            "contains_answer_material": True,
            "private_current_answer": private_answer,
            "report": {
                "schema_version": "study-intake-luna-analysis-v2",
                "private_current_answer": private_answer,
            },
            "rendered_markdown": f"# 私有报告\n\n{private_answer}\n",
            "answer_material_warning": f"私有答案警告：{private_answer}",
        })
        self.projection_path.write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        _, _, listing = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=cs408"
        )
        serialized_listing = json.dumps(listing, ensure_ascii=False)
        self.assertNotIn(private_answer, serialized_listing)
        self.assertNotIn("private_current_answer", serialized_listing)
        self.assertNotIn("report_json_ref", listing["items"][0])
        self.assertEqual(
            listing["items"][0]["runtime_identity_status"],
            "requested_unverified",
        )
        self.assertEqual(
            listing["items"][0]["processor_display_name"],
            "模型预处理（运行身份未验证）",
        )

        status, headers, detail = self.json_request(
            "GET",
            "/api/v1/items/CAP-20260804-408001?date=2026-08-04&subject=cs408",
        )
        self.assertEqual(status, 200)
        serialized_detail = json.dumps(detail, ensure_ascii=False)
        self.assertNotIn(private_answer, serialized_detail)
        self.assertNotIn("private_detail", detail)
        self.assertNotIn("private_current_answer", serialized_detail)
        self.assertNotIn("rendered_markdown", serialized_detail)
        self.assertNotIn("answer_material_warning", serialized_detail)
        self.assertEqual(headers["Cache-Control"], "no-store, max-age=0")
        self.assertNotIn("Access-Control-Allow-Origin", headers)

    def test_twenty_task_list_and_ten_independent_details_are_isolated(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        payload["subjects"]["math"]["counts"] = {
            "total": 20,
            "queued": 0,
            "running": 20,
            "ready": 0,
            "failed": 0,
            "stale": 0,
            "evidence_pending": 0,
        }
        expected: dict[str, str] = {}
        for ordinal in range(1, 21):
            capture_id = f"TASK-{ordinal:02d}"
            item = self.add_dispatch_detail(
                payload,
                subject="math",
                capture_id=capture_id,
                ordinal=ordinal,
            )
            expected[capture_id] = item["unit_sha256"]
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, listing = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["items"]), 20)
        self.assertEqual(
            listing["dashboard_request_counter_scope"],
            "dashboard_read_only_request",
        )
        self.assertEqual(listing["dashboard_request_model_call_count"], 0)
        serialized_list = json.dumps(listing, ensure_ascii=False)
        self.assertNotIn("task_detail_path", serialized_list)
        self.assertNotIn("latest_event_path", serialized_list)

        for ordinal in range(1, 11):
            capture_id = f"TASK-{ordinal:02d}"
            status, _, response = self.json_request(
                "GET",
                f"/api/v1/items/{capture_id}?date=2026-08-04&subject=math",
            )
            self.assertEqual(status, 200)
            detail = response["item"]["task_detail"]
            self.assertEqual(detail["task_identity"]["unit_sha256"], expected[capture_id])
            self.assertEqual(detail["task_identity"]["generation"], 1)
            self.assertEqual(detail["server_queue"]["confirmation"], "unconfirmed")
            self.assertEqual(detail["runtime"]["requested_model"], "gpt-5.6-luna")
            self.assertEqual(detail["runtime"]["requested_reasoning_effort"], "max")
            self.assertEqual(response["dashboard_request_model_call_count"], 0)
            self.assertEqual(response["dashboard_request_formal_write_count"], 0)
            self.assertNotIn("model_call_count", response)

    def test_list_never_opens_large_detail_and_blocked_detail_is_isolated(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        payload["subjects"]["math"]["counts"] = {
            "total": 2,
            "queued": 0,
            "running": 2,
            "ready": 0,
            "failed": 0,
            "stale": 0,
            "evidence_pending": 0,
        }
        large = self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-LARGE",
            ordinal=31,
        )
        self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-GOOD",
            ordinal=32,
        )
        Path(large["task_detail_path"]).write_bytes(
            b"{" + b"x" * (dashboard.MAX_TASK_DETAIL_BYTES + 1)
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        status, _, listing = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["items"]), 2)
        status, _, blocked = self.json_request(
            "GET", "/api/v1/items/TASK-LARGE?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 422)
        self.assertEqual(blocked["error"], "task_detail_dependency_too_large")
        status, _, good = self.json_request(
            "GET", "/api/v1/items/TASK-GOOD?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 200)
        self.assertEqual(good["item"]["capture_id"], "TASK-GOOD")

    def test_detail_default_privacy_and_explicit_raw_refresh_never_call_model(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        payload["subjects"]["math"]["counts"] = {
            "total": 1,
            "queued": 0,
            "running": 1,
            "ready": 0,
            "failed": 0,
            "stale": 0,
            "evidence_pending": 0,
        }
        self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-PRIVACY",
            ordinal=41,
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        path = "/api/v1/items/TASK-PRIVACY?date=2026-08-04&subject=math"
        for suffix in ("", "&raw=1", "", "&raw=1"):
            status, _, response = self.json_request("GET", path + suffix)
            self.assertEqual(status, 200)
            self.assertEqual(response["dashboard_request_model_call_count"], 0)
            self.assertEqual(response["dashboard_request_formal_write_count"], 0)
            self.assertNotIn("model_call_count", response)
            detail = response["item"]["task_detail"]
            if suffix:
                self.assertFalse(detail["raw_available"])
                self.assertNotIn("raw_sections", detail)
            else:
                self.assertNotIn("raw_sections", detail)
            serialized = json.dumps(response, ensure_ascii=False)
            for forbidden in ("raw_prompt", "final_answer", "private_reasoning", "answer_images"):
                self.assertNotIn(forbidden, serialized)

    def test_projection_identity_and_detail_generation_preconditions_close(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        item = self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-PROJECTION-FENCE",
            ordinal=92,
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        _, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        _, _, listing = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=math"
        )
        self.assertEqual(summary["projection_identity"], listing["projection_identity"])
        identity = summary["projection_identity"]
        self.assertEqual(identity["status"], "verified")
        self.assertEqual(identity["release_id"], "1" * 64)
        self.assertEqual(
            identity["sha256"],
            dashboard.sha256_bytes(self.projection_path.read_bytes()),
        )
        query = {
            "date": "2026-08-04",
            "subject": "math",
            "projection_sha256": identity["sha256"],
            "projection_generation": identity["generation"],
            "projection_release_id": identity["release_id"],
            "task_generation": str(item["generation"]),
            "task_fence": str(item["fence"]),
        }
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/TASK-PROJECTION-FENCE?" + urlencode(query),
        )
        self.assertEqual(status, 200)
        self.assertEqual(response["projection_identity"], identity)

        query["projection_sha256"] = "f" * 64
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/TASK-PROJECTION-FENCE?" + urlencode(query),
        )
        self.assertEqual(status, 409)
        self.assertEqual(response["error"], "projection_generation_conflict")

        query["projection_sha256"] = identity["sha256"]
        query["task_generation"] = str(item["generation"] + 1)
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/TASK-PROJECTION-FENCE?" + urlencode(query),
        )
        self.assertEqual(status, 409)
        self.assertEqual(response["error"], "task_generation_conflict")

        detail_path = Path(item["task_detail_path"])
        detail = json.loads(detail_path.read_text(encoding="utf-8"))
        detail["fence"] += 1
        detail_path.write_text(json.dumps(detail), encoding="utf-8")
        query["task_generation"] = str(item["generation"])
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/TASK-PROJECTION-FENCE?" + urlencode(query),
        )
        self.assertEqual(status, 409)
        self.assertEqual(response["error"], "task_detail_generation_conflict")

    def test_private_detail_is_rejected_and_generation_conflict_cannot_overwrite(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"] = []
        payload["subjects"]["math"]["counts"] = {
            "total": 2,
            "queued": 0,
            "running": 2,
            "ready": 0,
            "failed": 0,
            "stale": 0,
            "evidence_pending": 0,
        }
        self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-PRIVATE",
            ordinal=51,
            forbidden=True,
        )
        current = self.add_dispatch_detail(
            payload,
            subject="math",
            capture_id="TASK-FENCE",
            ordinal=52,
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, rejected = self.json_request(
            "GET", "/api/v1/items/TASK-PRIVATE?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 422)
        self.assertEqual(rejected["error"], "task_detail_private_or_write_conflict")
        self.assertNotIn("PRIVATE-ANSWER-SENTINEL", json.dumps(rejected))

        old_completion = {
            "unit_sha256": current["unit_sha256"],
            "lease_fence": 0,
            "subject": "math",
            "capture_id": "TASK-FENCE",
            "release_id": "1" * 64,
            "outcome": "completed",
            "package_sha256": "c" * 64,
            "receipt_sha256": "d" * 64,
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
        }
        with mock.patch.object(
            dashboard,
            "_verify_completion_authority",
            return_value=old_completion,
        ):
            status, _, conflict = self.json_request(
                "GET", "/api/v1/items/TASK-FENCE?date=2026-08-04&subject=math"
            )
        self.assertEqual(status, 409)
        self.assertEqual(conflict["error"], "task_detail_generation_conflict")

    def test_degraded_is_unconsumable_and_408_visual_gaps_are_explicit(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["cs408"]["items"] = []
        payload["subjects"]["cs408"]["counts"] = {
            "total": 1,
            "queued": 0,
            "running": 1,
            "ready": 0,
            "failed": 0,
            "stale": 0,
            "evidence_pending": 0,
        }
        item = self.add_dispatch_detail(
            payload,
            subject="cs408",
            capture_id="TASK-408-DEGRADED",
            ordinal=61,
            phase="published",
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        detail_payload = json.loads(Path(item["task_detail_path"]).read_text(encoding="utf-8"))
        completion = {
            "unit_sha256": item["unit_sha256"],
            "lease_fence": detail_payload["fence"],
            "subject": "cs408",
            "capture_id": "TASK-408-DEGRADED",
            "release_id": "1" * 64,
            "outcome": "single_pass_degraded",
            "package_sha256": "c" * 64,
            "receipt_sha256": "d" * 64,
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
        }
        with mock.patch.object(
            dashboard,
            "_verify_completion_authority",
            return_value=completion,
        ):
            status, _, response = self.json_request(
                "GET",
                "/api/v1/items/TASK-408-DEGRADED?date=2026-08-04&subject=cs408",
            )
        self.assertEqual(status, 200)
        detail = response["item"]["task_detail"]
        self.assertEqual(detail["result"]["status"], "degraded")
        self.assertFalse(detail["result"]["consumable"])
        self.assertEqual(detail["cs408_visual"]["status"], "unconfirmed")
        self.assertEqual(detail["deterministic_diff"]["status"], "unconfirmed")

        completion["outcome"] = "future_terminal_state"
        with mock.patch.object(
            dashboard,
            "_verify_completion_authority",
            return_value=completion,
        ):
            status, _, response = self.json_request(
                "GET",
                "/api/v1/items/TASK-408-DEGRADED?date=2026-08-04&subject=cs408",
            )
        self.assertEqual(status, 200)
        self.assertEqual(response["item"]["task_detail"]["result"]["status"], "pending")
        self.assertFalse(response["item"]["task_detail"]["result"]["consumable"])

    def test_subject_source_error_is_public_but_safely_bounded(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["cs408"]["status"] = "source_error"
        payload["subjects"]["cs408"]["error_code"] = "canonical_status_failed"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, summary = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=cs408",
        )
        self.assertEqual(status, 200)
        section = summary["subjects"]["cs408"]
        self.assertEqual(section["status"], "source_error")
        self.assertEqual(section["error_code"], "canonical_status_failed")

        payload["subjects"]["cs408"]["error_code"] = "/Users/private/failure"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, summary = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=cs408",
        )
        self.assertNotIn("error_code", summary["subjects"]["cs408"])

    def test_metrics_require_explicit_non_path_sources(self) -> None:
        status, _, payload = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        metrics = payload["subjects"]["math"]["metrics"]
        self.assertEqual(metrics["review_time_delta_pct"], {
            "value": -27.5,
            "source": "eval-receipt:math-trial-001",
        })
        self.assertEqual(metrics["critical_errors"]["value"], 0)
        self.assertNotIn("unverified_claim", metrics)

    def test_reference_allowlist_rejects_artifact_local_paths(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][0]
        item["evidence_refs"] = [
            "capture:SAFE-REFERENCE",
            "artifact:/tmp/dashboard-private.json",
            "artifact:/var/folders/private.json",
        ]
        item["adoption_receipt_id"] = "artifact:/tmp/adoption.json"
        item["stage_receipts"]["analysis"]["artifact_ref"] = (
            "artifact:/tmp/analysis.json"
        )
        payload["subjects"]["math"]["metric_sources"][
            "critical_errors"
        ] = "artifact:/tmp/metrics.json"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")

        _, _, listing = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=math"
        )
        _, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        serialized = json.dumps(
            {"listing": listing, "summary": summary}, ensure_ascii=False
        )
        self.assertNotIn("artifact:/", serialized)
        self.assertNotIn("/tmp/", serialized)
        self.assertNotIn("/var/folders/", serialized)
        public_item = listing["items"][0]
        self.assertNotIn("evidence_refs", public_item)
        self.assertNotIn("adoption_receipt_id", public_item)
        self.assertNotIn("stage_receipts", public_item)
        self.assertNotIn("critical_errors", summary["subjects"]["math"]["metrics"])

    def test_item_api_allowlists_fields_and_drops_local_paths(self) -> None:
        status, _, payload = self.json_request(
            "GET",
            "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        item = payload["item"]
        self.assertEqual(item["delivery_status"], "consumed")
        self.assertEqual(item["adoption_status"], "direct_adopted")
        self.assertEqual(item["adoption_bucket"], "direct")
        self.assertEqual(item["adoption_receipt_id"], "sol-receipt:001")
        self.assertNotIn("evidence_refs", item)
        for protected in (
            "safe_summary",
            "result_summary",
            "evidence_summary",
            "first_break",
            "suggestion_summary",
            "risks",
            "unresolved",
        ):
            self.assertNotIn(protected, item)
        self.assertEqual(item["pipeline_status"], "two_pass_ready")
        self.assertEqual(
            item["runtime_identity_status"], "requested_unverified"
        )
        self.assertEqual(item["evidence_coverage_pct"], 100)
        self.assertEqual(item["visual_coverage_pct"], 100)
        self.assertEqual(
            set(item["stage_receipts"]), {"analysis", "critical_review"}
        )
        self.assertEqual(
            item["stage_receipts"]["critical_review"]["runtime_model"],
            "gpt-5.6-luna",
        )
        self.assertNotIn("private_path", item["stage_receipts"]["analysis"])
        self.assertNotIn("raw_prompt", item)
        self.assertNotIn("final_answer", item)
        self.assertNotIn("source_path", item)
        serialized = json.dumps(payload, ensure_ascii=False)
        self.assertNotIn("must-not-leak", serialized)
        self.assertNotIn("/Users/", serialized)

    def test_item_api_exposes_only_safe_failure_and_resume_status(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["cs408"]["items"][0]
        item.update({
            "last_error_code": "cs408_analysis_usage_limit",
            "retry_at_hint": "2026-08-08T11:37:00+08:00",
            "critical_resume_status": "full_rerun_required",
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/CAP-20260804-408001?date=2026-08-04&subject=cs408",
        )
        self.assertEqual(status, 200)
        public = response["item"]
        self.assertEqual(public["last_error_code"], "cs408_analysis_usage_limit")
        self.assertEqual(public["retry_at_hint"], "2026-08-08T11:37:00+08:00")
        self.assertEqual(public["critical_resume_status"], "full_rerun_required")

        item.update({
            "last_error_code": "/Users/private/error",
            "retry_at_hint": "retry at /etc/passwd",
            "critical_resume_status": "../../private",
        })
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, response = self.json_request(
            "GET",
            "/api/v1/items/CAP-20260804-408001?date=2026-08-04&subject=cs408",
        )
        public = response["item"]
        self.assertNotIn("last_error_code", public)
        self.assertNotIn("retry_at_hint", public)
        self.assertNotIn("critical_resume_status", public)

    def test_consumed_without_adoption_receipt_is_not_counted_as_reviewed(self) -> None:
        status, _, items_payload = self.json_request(
            "GET",
            "/api/v1/items?date=2026-08-04&subject=math&status=ready",
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(items_payload["items"]), 2)
        second = next(item for item in items_payload["items"] if item["capture_id"].endswith("002"))
        self.assertEqual(second["delivery_status"], "consumed")
        self.assertEqual(second["adoption_status"], "unknown")
        self.assertNotIn("adoption_receipt_id", second)

        status, _, summary = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["reviewed"], 1)

    def test_invalid_adoption_status_is_forced_to_unknown(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["items"][0]["adoption_status"] = "adopted_without_contract"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        self.assertEqual(response["item"]["adoption_status"], "unknown")
        status, _, summary = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["reviewed"], 0)

    def test_adoption_status_without_receipt_is_forced_to_unknown(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][1]
        item["adoption_status"] = "modified_adopted"
        item.pop("adoption_receipt_id", None)
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET",
            "/api/v1/items/MFI-20260804-002?date=2026-08-04&subject=math",
        )
        self.assertEqual(status, 200)
        self.assertEqual(response["item"]["adoption_status"], "unknown")
        self.assertNotIn("adoption_receipt_id", response["item"])

    def test_granular_and_legacy_adoption_buckets_are_mutually_distinct(self) -> None:
        expected = {
            "direct_adopted": "direct",
            "minor_edit_adopted": "minor",
            "major_edit_adopted": "major",
            "modified_adopted": "legacy_modified",
            "rejected": "rejected",
            "fallback": "fallback",
        }
        for status_name, bucket in expected.items():
            with self.subTest(status=status_name):
                payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
                for dispatcher in payload["dispatchers"].values():
                    dispatcher["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
                item = payload["subjects"]["math"]["items"][0]
                item["adoption_status"] = status_name
                item["adoption_receipt_id"] = f"sol-receipt:{bucket}"
                self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
                _, _, response = self.json_request(
                    "GET",
                    "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
                )
                self.assertEqual(response["item"]["adoption_status"], status_name)
                self.assertEqual(response["item"]["adoption_bucket"], bucket)

    def test_runtime_claim_without_actual_identity_is_downgraded(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][0]
        item["runtime_identity_status"] = "confirmed"
        item.pop("runtime_model")
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, response = self.json_request(
            "GET",
            "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
        )
        self.assertEqual(
            response["item"]["runtime_identity_status"],
            "requested_unverified",
        )

    def test_invalid_or_inconsistent_coverage_is_not_published(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][0]
        item["evidence_claim_count"] = 2
        item["evidence_claims_with_refs"] = 3
        item["evidence_coverage_pct"] = 100
        item["visual_claim_count"] = 4.5
        item["visual_coverage_pct"] = 101
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, response = self.json_request(
            "GET",
            "/api/v1/items/MFI-20260804-001?date=2026-08-04&subject=math",
        )
        public = response["item"]
        self.assertNotIn("evidence_claim_count", public)
        self.assertNotIn("evidence_claims_with_refs", public)
        self.assertNotIn("evidence_coverage_pct", public)
        self.assertNotIn("visual_claim_count", public)
        self.assertNotIn("visual_coverage_pct", public)

    def test_failures_processed_and_remaining_are_reconciled_from_items(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        item = payload["subjects"]["math"]["items"][2]
        item["luna_status"] = "failed"
        item["queue_state"] = "failed"
        item["current_stage"] = "failed"
        item["last_error_code"] = "critical_review_failed"
        payload["subjects"]["math"]["counts"] = dashboard._v3_counts(
            payload["subjects"]["math"]["items"]
        )
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        _, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(summary["counts"]["processed"], 4)
        self.assertEqual(summary["counts"]["remaining"], 1)
        self.assertEqual(summary["counts"]["failures"], 1)
        self.assertEqual(summary["observability"]["failures"], {
            "total": 1,
            "by_code": {"critical_review_failed": 1},
        })

    def test_missing_projection_reports_unavailable_without_fake_counts(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.json"
        self.httpd.store = dashboard.ProjectionStore(missing)
        status, _, payload = self.json_request(
            "GET",
            "/api/v1/summary?date=2026-08-04&subject=all",
        )
        self.assertEqual(status, 200)
        self.assertFalse(payload["available"])
        self.assertEqual(payload["error"], "projection_missing")
        self.assertNotIn("counts", payload)

    def test_requested_date_loads_matching_archived_projection(self) -> None:
        archived = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        current = json.loads(self.projection_path.read_text(encoding="utf-8"))
        current["study_date"] = "2026-08-05"
        for section in current["subjects"].values():
            for item in section["items"]:
                item["study_date"] = "2026-08-05"
        self.projection_path.write_text(json.dumps(current), encoding="utf-8")
        archive_dir = self.projection_path.parent / "dashboard-projections"
        archive_dir.mkdir()
        (archive_dir / "2026-08-04.json").write_text(
            json.dumps(archived), encoding="utf-8"
        )

        status, _, summary = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 200)
        self.assertTrue(summary["available"])
        self.assertEqual(summary["study_date"], "2026-08-04")
        self.assertEqual(summary["counts"]["selected"], 5)

    def test_requested_date_without_archive_is_explicitly_unavailable(self) -> None:
        current = json.loads(self.projection_path.read_text(encoding="utf-8"))
        current["study_date"] = "2026-08-05"
        self.projection_path.write_text(json.dumps(current), encoding="utf-8")

        status, _, payload = self.json_request(
            "GET", "/api/v1/items?date=2026-08-04&subject=all"
        )
        self.assertEqual(status, 404)
        self.assertFalse(payload["available"])
        self.assertEqual(payload["error"], "projection_date_unavailable")
        self.assertNotIn("items", payload)

    def test_invalid_projection_reports_503(self) -> None:
        self.projection_path.write_text("{broken", encoding="utf-8")
        status, _, payload = self.json_request("GET", "/healthz")
        self.assertEqual(status, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["error"], "projection_invalid_json")

    def test_schema_mismatch_reports_503(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["schema_version"] = "unexpected-v2"
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request("GET", "/api/v1/items")
        self.assertEqual(status, 503)
        self.assertFalse(response["available"])
        self.assertEqual(response["error"], "projection_schema_mismatch")

    def test_projection_v3_declared_counts_must_close_exactly(self) -> None:
        payload = json.loads(self.projection_path.read_text(encoding="utf-8"))
        payload["subjects"]["math"]["counts"]["selected"] += 1
        self.projection_path.write_text(json.dumps(payload), encoding="utf-8")
        status, _, response = self.json_request(
            "GET", "/api/v1/summary?date=2026-08-04&subject=math"
        )
        self.assertEqual(status, 503)
        self.assertEqual(response["error"], "projection_v3_counts_not_closed")

    def test_projection_v3_unknown_fields_fail_closed_at_every_public_layer(self) -> None:
        baseline = json.loads(self.projection_path.read_text(encoding="utf-8"))
        mutations = {
            "root": lambda payload: payload.__setitem__("unexpected", True),
            "dispatcher": lambda payload: payload["dispatchers"]["math"].__setitem__(
                "unexpected", True
            ),
            "subject": lambda payload: payload["subjects"]["math"].__setitem__(
                "unexpected", True
            ),
            "item": lambda payload: payload["subjects"]["math"]["items"][0].__setitem__(
                "unexpected", True
            ),
        }
        for layer, mutate in mutations.items():
            with self.subTest(layer=layer):
                payload = json.loads(json.dumps(baseline))
                mutate(payload)
                self.projection_path.write_text(
                    json.dumps(payload), encoding="utf-8"
                )
                status, _, body = self.json_request("GET", "/api/v1/summary")
                self.assertEqual(status, 503)
                self.assertFalse(body["available"])

    def test_projection_v3_missing_concurrency_or_canary_fields_fail_closed(self) -> None:
        baseline = json.loads(self.projection_path.read_text(encoding="utf-8"))
        mutations = {
            "root_concurrency": lambda payload: payload.pop("concurrency"),
            "subject_concurrency": lambda payload: payload["subjects"]["math"].pop(
                "concurrency_status"
            ),
            "subject_canary_gate": lambda payload: payload["subjects"]["math"].pop(
                "canary_gate"
            ),
            "dispatcher_canary_gate": lambda payload: payload["dispatchers"][
                "math"
            ].pop("canary_gate"),
        }
        for layer, mutate in mutations.items():
            with self.subTest(layer=layer):
                payload = json.loads(json.dumps(baseline))
                mutate(payload)
                self.projection_path.write_text(
                    json.dumps(payload), encoding="utf-8"
                )
                status, _, body = self.json_request("GET", "/api/v1/summary")
                self.assertEqual(status, 503)
                self.assertFalse(body["available"])
                self.assertIn(
                    body["error"],
                    {
                        "projection_v3_root_shape_invalid",
                        "projection_v3_subject_shape_invalid",
                        "projection_v3_dispatcher_shape_invalid",
                        "projection_v3_concurrency_subject_shape_invalid",
                    },
                )

    def test_projection_and_task_detail_schemas_publish_closed_privacy_contracts(self) -> None:
        projection_schema = json.loads(
            (DASHBOARD_DIR.parent / "schemas/dashboard-projection-v3.json").read_text(
                encoding="utf-8"
            )
        )
        detail_schema = json.loads(
            (DASHBOARD_DIR.parent / "schemas/dashboard-task-detail-v1.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertFalse(projection_schema["additionalProperties"])
        self.assertEqual(
            projection_schema["properties"]["schema_version"]["const"],
            "study-intake-dashboard-projection-v3",
        )
        self.assertEqual(
            set(projection_schema["properties"]["en_p0_006_status"]["enum"]),
            {
                "unknown",
                "needs_user_decision",
                "review_required_not_signable",
                "no_data",
                "authorization_ready",
                "luna_running",
                "luna_complete_with_failures",
                "quality_ready",
                "queued_for_sol",
                "formal_applying",
                "recovering",
                "safe_paused",
                "complete_with_failures",
                "verified_complete",
                "invalid",
            },
        )
        self.assertIn("en_p0_006", projection_schema["required"])
        self.assertIn("concurrency", projection_schema["required"])
        self.assertFalse(projection_schema["$defs"]["enP0006"]["additionalProperties"])
        self.assertFalse(projection_schema["$defs"]["enP0006Sol"]["additionalProperties"])
        plugin_projection_schema = json.loads(
            (
                DASHBOARD_DIR.parent
                / "plugin/kaoyan-study-intake/schemas/dashboard-projection-v3.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(plugin_projection_schema, projection_schema)
        item_properties = projection_schema["$defs"]["item"]["properties"]
        self.assertIn("unit_sha256", item_properties)
        for field in (
            "event_written",
            "dispatcher_accepted",
            "package_visible",
            "quick_intake_complete",
            "selected_by",
            "selector_sha256",
            "proposal_sha256",
            "authoritative_package_sha256",
        ):
            self.assertIn(field, item_properties)
        self.assertIn("global_sol", projection_schema["properties"])
        self.assertIn("concurrency", projection_schema["properties"])
        concurrency_schema = projection_schema["$defs"]["concurrency"]
        self.assertFalse(concurrency_schema["additionalProperties"])
        for field in (
            "global_active_task_count",
            "global_peak_active",
            "verified_runner_global_active_task_count",
            "scheduler_claim_global_peak_active",
            "runner_evidenced_task_count_global",
            "runner_interval_missing_count_global",
            "terminal_task_count_global",
            "terminal_by_outcome_global",
            "terminal_failure_binding_count_global",
            "effective_concurrency_limit",
            "available_concurrency_slots",
            "backpressure_reason",
            "source_telemetry",
        ):
            self.assertIn(field, concurrency_schema["required"])
        subject_properties = projection_schema["$defs"]["subject"]["properties"]
        for field in (
            "subject_active_task_count",
            "subject_pending_task_count",
            "subject_peak_active",
            "subject_verified_runner_active_task_count",
            "subject_runner_evidenced_task_count",
            "subject_runner_interval_missing_count",
            "scheduler_claim_subject_peak_active",
            "terminal_index_sha256",
            "terminal_task_count",
            "terminal_by_outcome",
            "terminal_failure_bindings",
            "effective_concurrency_limit",
            "available_concurrency_slots",
            "backpressure_reason",
        ):
            self.assertIn(field, subject_properties)
            self.assertIn(
                field, projection_schema["$defs"]["subject"]["required"]
            )
        self.assertIn(
            "canary_gate", projection_schema["$defs"]["subject"]["required"]
        )
        self.assertIn(
            "canary_gate", projection_schema["$defs"]["dispatcher"]["required"]
        )
        canary_schema = projection_schema["$defs"]["productionCanaryStateV2"]
        self.assertFalse(canary_schema["additionalProperties"])
        self.assertFalse(
            projection_schema["$defs"]["canaryStateAuthority"][
                "additionalProperties"
            ]
        )
        self.assertEqual(
            canary_schema["properties"]["schema_version"]["const"],
            "study-intake-production-canary-state-v2",
        )
        self.assertFalse(
            canary_schema["properties"]["fast_mode_requested"]["const"]
        )
        self.assertEqual(
            canary_schema["properties"]["fast_mode_effective"]["const"],
            "not_requested",
        )
        self.assertEqual(
            canary_schema["properties"]["requested_service_tier"]["type"],
            "null",
        )
        self.assertEqual(
            canary_schema["properties"]["initial_canary_inflight_limit"][
                "const"
            ],
            1,
        )
        for field in (
            "terminal_index_sha256",
            "terminal_task_count",
            "terminal_by_outcome",
        ):
            self.assertIn(field, canary_schema["required"])
        telemetry_schema = projection_schema["$defs"]["concurrencyTelemetry"]
        self.assertFalse(telemetry_schema["additionalProperties"])
        self.assertEqual(
            telemetry_schema["properties"]["counter_scope"]["const"],
            "same_release_current_activation_task_process_lifecycle",
        )
        self.assertEqual(
            telemetry_schema["properties"]["peak_source"]["const"],
            "hmac_task_process_identity_and_exit_half_open_intervals",
        )
        self.assertEqual(
            telemetry_schema["properties"]["requested_service_tier"]["type"],
            "null",
        )
        self.assertFalse(detail_schema["additionalProperties"])
        self.assertEqual(
            detail_schema["properties"]["schema_version"]["const"],
            "study-intake-dashboard-task-detail-v1",
        )
        self.assertEqual(
            detail_schema["properties"]["model_call_count"]["maximum"], 2
        )
        self.assertEqual(
            detail_schema["properties"][
                "consumed_terminal_duplicate_read_count"
            ]["const"],
            0,
        )
        self.assertIn("raw_available", detail_schema["required"])
        self.assertIn("stage_statuses", detail_schema["required"])
        stage_event_properties = detail_schema["$defs"]["stage_event"]["properties"]
        for field in ("event", "sequence", "event_sha256"):
            self.assertIn(field, stage_event_properties)
            self.assertIn(field, detail_schema["$defs"]["stage_event"]["required"])
        serialized = json.dumps(detail_schema)
        for forbidden in ("raw_prompt", "final_answer", "private_reasoning", "answer_images"):
            self.assertNotIn(forbidden, serialized)

    def test_host_method_query_and_path_guards(self) -> None:
        status, _, payload = self.json_request("GET", "/healthz", host="example.com")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_host")

        status, headers, payload = self.json_request("POST", "/api/v1/items")
        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "GET")
        self.assertEqual(payload["error"], "method_not_allowed")
        self.assertNotIn("Access-Control-Allow-Origin", headers)

        status, _, payload = self.json_request("GET", "/api/v1/items?subject=history")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_subject")

        status, _, payload = self.json_request("GET", "/api/v1/items?date=2026-99-99")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_date")

        status, _, payload = self.json_request("GET", "/%2e%2e/server.py")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_path")

    def test_static_assets_are_same_origin_and_use_safe_dom_apis(self) -> None:
        status, headers, html = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        html_text = html.decode("utf-8")
        self.assertIn("今天的快速入库", html_text)
        self.assertIn('src="app.js"', html_text)
        for element_id in (
            "main-content",
            "board-view",
            "lane-waiting",
            "lane-processing",
            "lane-completed",
            "errors-view",
            "task-dialog",
            "refresh-button",
        ):
            self.assertIn(f'id="{element_id}"', html_text)
        self.assertNotIn("https://", html_text)

        status, _, js = self.request("GET", "/app.js")
        self.assertEqual(status, 200)
        js_text = js.decode("utf-8")
        self.assertNotIn("innerHTML", js_text)
        self.assertIn('const ITEMS_API_URL = "/api/v1/items"', js_text)
        self.assertIn("dashboard_request_counter_scope", js_text)
        self.assertIn("dashboard_request_model_call_count", js_text)
        self.assertIn("dashboard_request_provider_request_count", js_text)
        self.assertIn("dashboard_request_mcp_tool_call_count", js_text)
        self.assertIn("dashboard_request_formal_write_count", js_text)
        self.assertIn("projection_sha256", js_text)
        self.assertIn("projection_generation", js_text)
        self.assertIn("projection_release_id", js_text)
        self.assertIn("task_generation", js_text)
        self.assertIn("task_fence", js_text)
        self.assertIn("detail v2 未公开独立 Provider 结果", js_text)
        self.assertIn("detail v2 未公开独立 MCP 查询字段", js_text)
        self.assertIn("前端不复制 Analysis 证据", js_text)
        for method in ('method: "POST"', 'method: "PATCH"', 'method: "PUT"', 'method: "DELETE"'):
            self.assertNotIn(method, js_text)
        self.assertNotIn("localStorage", js_text)
        self.assertNotIn("sessionStorage", js_text)

        status, _, css = self.request("GET", "/styles.css")
        self.assertEqual(status, 200)
        self.assertIn("prefers-reduced-motion", css.decode("utf-8"))


    def test_deterministic_diff_does_not_drop_late_changed_fields(self) -> None:
        before = {f"field_{index:04d}": index for index in range(1205)}
        after = dict(before)
        after["field_1204"] = "changed-at-the-end"
        diff = dashboard._deterministic_stage_diff(before, after)
        self.assertEqual(diff["status"], "confirmed")
        self.assertEqual(diff["compared_count"], 1205)
        self.assertEqual(diff["changed_count"], 1)
        self.assertEqual(diff["unchanged_count"], 1204)
        self.assertEqual(len(diff["fields"]), 1)
        self.assertEqual(diff["fields"][0]["field"], "root.field_1204")
        self.assertTrue(diff["fields"][0]["changed"])

    def test_stage_lifecycle_attributes_timeout_to_active_stage(self) -> None:
        lifecycle = dashboard._stage_lifecycle([
            {
                "event": "analysis_submitted",
                "changed_at": "2026-08-05T00:00:00Z",
            },
            {
                "event": "analysis_completed",
                "changed_at": "2026-08-05T00:00:01Z",
            },
            {
                "event": "critical_started",
                "changed_at": "2026-08-05T00:00:02Z",
            },
            {
                "event": "timeout",
                "changed_at": "2026-08-05T00:00:03Z",
                "error_code": "critical_review_timeout",
            },
        ])
        self.assertEqual(lifecycle["analysis"]["status"], "completed")
        self.assertEqual(
            lifecycle["critical_review"]["status"], "timed_out"
        )
        self.assertEqual(
            lifecycle["critical_review"]["error_code"],
            "critical_review_timeout",
        )


if __name__ == "__main__":
    unittest.main()
