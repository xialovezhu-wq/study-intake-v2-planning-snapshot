from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "dashboard"))

import dashboard_projection as projection  # noqa: E402
import server as dashboard  # noqa: E402


FIXTURE = ROOT / "dashboard/tests/fixtures/dashboard_projection.json"
H = "a" * 64


def v4_projection() -> dict:
    value = json.loads(FIXTURE.read_text(encoding="utf-8"))
    value["schema_version"] = dashboard.SCHEMA_VERSION
    value["configured_global_continuous_concurrency_limit"] = 60
    for section in value["subjects"].values():
        for item in section["items"]:
            projection._ensure_task_axes(item)
            if item.get("server_queue_status") == "rate_limited":
                item["server_queue_status"] = "confirmed_rate_limited"
        section["batch_partition"] = {
            "current_batch_task_count": 0,
            "current_batch_terminal_task_count": 0,
            "current_batch_failed_task_count": 0,
            "current_batch_sol_candidate_task_count": 0,
            "current_batch_diagnostic_task_count": 0,
            "outside_batch_pending_task_count": sum(
                item["local_dispatch_status"]
                in {"pending", "pending_consumer_paused", "retrying"}
                for item in section["items"]
            ),
            "outside_batch_failed_preserved_task_count": 0,
        }
        section["blocking_batch"] = None
        section["capacity"] = {
            "capacity_mode": "disabled",
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "effective_concurrency_limit": 0,
        }
    return value


class DashboardProjectionV4ContractTests(unittest.TestCase):
    def test_complete_v4_projection_is_accepted(self) -> None:
        self.assertIsNone(dashboard._projection_contract_error(v4_projection()))

    def test_missing_required_task_axis_fails_closed(self) -> None:
        value = v4_projection()
        value["subjects"]["math"]["items"][0].pop(
            "authority_snapshot_sha256"
        )
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_task_shape_invalid",
        )

    def test_unknown_server_queue_cannot_claim_confirmation(self) -> None:
        value = v4_projection()
        item = value["subjects"]["math"]["items"][0]
        item["server_queue_status"] = "unknown"
        item["server_queue_confirmation"] = "confirmed_event"
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_server_queue_confirmation_invalid",
        )

    def test_initial_canary_capacity_cannot_silently_be_three(self) -> None:
        value = v4_projection()
        capacity = value["subjects"]["math"]["capacity"]
        capacity["capacity_mode"] = "initial_canary"
        capacity["effective_concurrency_limit"] = 3
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_capacity_invalid",
        )

    def test_current_batch_partition_must_close_monotonically(self) -> None:
        value = v4_projection()
        partition = value["subjects"]["math"]["batch_partition"]
        partition.update(
            current_batch_task_count=1,
            current_batch_terminal_task_count=1,
            current_batch_sol_candidate_task_count=1,
            current_batch_diagnostic_task_count=1,
        )
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_batch_partition_invalid",
        )

    def test_preserved_preclaim_failure_requires_exact_primary_attempt(self) -> None:
        value = v4_projection()
        item = value["subjects"]["math"]["items"][0]
        item.update(
            {
                "queue_state": "failed",
                "local_dispatch_status": "terminal",
                "model_stage": "not_started",
                "terminal_status": "failed",
                "local_model_submitted": False,
                "exact_error_code": "subject_luna_batch_already_current",
                "terminal_receipt_sha256": "1" * 64,
                "preclaim_failure_stage": "pre_claim",
                "preclaim_failed_at": "2026-08-12T10:00:00+00:00",
                "primary_preclaim_failure_receipt_sha256": "1" * 64,
                "queue_preserved_for_recovery": True,
                "recovery_status": "waiting_explicit_resume",
                "preclaim_attempt_count": 1,
                "latest_preclaim_attempt_at": "2026-08-12T10:00:00+00:00",
                "latest_preclaim_attempt_error_code": "subject_luna_batch_already_current",
                "preclaim_attempt_history": [
                    {
                        "receipt_sha256": "1" * 64,
                        "unit_sha256": item["unit_sha256"],
                        "frozen_payload_sha256": item["frozen_payload_sha256"],
                        "failure_stage": "pre_claim",
                        "error_code": "subject_luna_batch_already_current",
                        "failed_at": "2026-08-12T10:00:00+00:00",
                        "queue_entry_preserved": True,
                        "primary": True,
                    }
                ],
            }
        )
        value["subjects"]["math"]["counts"] = dashboard._v3_counts(
            value["subjects"]["math"]["items"]
        )
        self.assertIsNone(dashboard._projection_contract_error(value))
        public = dashboard._public_item(item, "math")
        self.assertIsNotNone(public)
        self.assertTrue(public["queue_preserved_for_recovery"])
        self.assertEqual(
            public["primary_preclaim_failure_receipt_sha256"], "1" * 64
        )
        self.assertTrue(public["preclaim_attempt_history"][0]["primary"])

        item["preclaim_attempt_history"][0]["primary"] = False
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_preclaim_attempt_history_invalid",
        )

    def test_cross_date_blocker_must_be_bound_to_subject_blockers(self) -> None:
        value = v4_projection()
        section = value["subjects"]["english"]
        section["blocking_batch"] = {
            "batch_id": "OLD-BATCH-1",
            "study_date": "2026-08-11",
            "status": "frozen",
            "all_terminal": True,
            "sol_ready": False,
            "writer_bound": True,
            "writer_handoff_status": "awaiting_luna",
            "blocker_code": "cross_date_batch_writer_bound",
        }
        section["blockers"].append("cross_date_batch_writer_bound")
        self.assertIsNone(dashboard._projection_contract_error(value))
        summary = dashboard._summary(
            value,
            study_date=value["study_date"],
            subject="all",
        )
        self.assertTrue(
            summary["subjects"]["english"]["blocking_batch"]["writer_bound"]
        )

        section["blockers"].clear()
        self.assertEqual(
            dashboard._projection_contract_error(value),
            "projection_v4_blocking_batch_invalid",
        )

    def test_dashboard_copy_separates_execution_quality_from_technical_failure(self) -> None:
        html = (ROOT / "dashboard/static/index.html").read_text(encoding="utf-8")
        source = (ROOT / "dashboard/static/app.js").read_text(encoding="utf-8")
        self.assertIn("执行成功与质量状态保持分离", html)
        self.assertIn("质量问题不会进入此处", html)
        self.assertIn('if (item.execution_status === "succeeded")', source)
        self.assertIn('if (item.execution_status === "failed")', source)
        self.assertIn('return "technical_failure"', source)
        self.assertNotIn("等待显式恢复", source)

    def test_v4_selection_is_release_configured_not_subject_order_dependent(self) -> None:
        configured = {
            "dashboard": {
                "projection_schema_version": projection.MAIN_SCHEMA,
            }
        }
        legacy = {"dashboard": {}}
        self.assertEqual(
            projection._configured_dashboard_schema(configured),
            projection.MAIN_SCHEMA,
        )
        self.assertEqual(
            projection._configured_dashboard_schema(legacy),
            projection.LEGACY_MAIN_SCHEMA,
        )


class DashboardTaskDetailV2ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.detail = {
            "schema_version": "study-intake-dispatch-task-detail-v2",
            "unit_sha256": "1" * 64,
            "owner_id": "dispatcher-101-task",
            "fence": 1,
            "attempt": 1,
            "subject": "math",
            "capture_id": "CAP-V2-1",
            "release_id": "2" * 64,
            "frozen_payload_sha256": "3" * 64,
            "rule_version": "study-intake-concurrent-dispatch-contract-v2",
            "evidence_integrity": "frozen_and_hmac_bound",
            "phase": "provider_process_started",
            "stage_name": "analysis",
            "latest_event_sha256": "4" * 64,
            "server_queue_status": "unknown",
            "artifacts": {},
            "updated_at": "2026-08-12T10:00:00+00:00",
            "formal_write_count": 0,
            "authority": {
                "algorithm": "HMAC-SHA256",
                "purpose": "dispatch-task-detail",
                "hmac_sha256": "5" * 64,
            },
        }
        self.item = {
            "capture_id": "CAP-V2-1",
            "subject": "math",
            "unit_sha256": "1" * 64,
            "release_id": "2" * 64,
            "frozen_payload_sha256": "3" * 64,
            "input_fingerprint": "6" * 64,
            "rule_version": "study-intake-concurrent-dispatch-contract-v2",
            "generation": 1,
            "attempt": 1,
            "fence": 1,
            "local_dispatch_status": "running",
            "evidence_access_status": "ready",
            "analysis_execution_status": "running",
            "analysis_report_status": "not_started",
            "review_execution_status": "not_started",
            "review_report_status": "not_started",
            "sol_review_status": "not_eligible",
            "formal_write_status": "not_authorized",
            "server_queue_status": "unknown",
            "server_queue_confirmation": "unconfirmed",
            "warning_codes": [],
            "elapsed_runtime_seconds": 20,
            "last_meaningful_progress_at": "2026-08-12T10:00:00+00:00",
            "soft_timeout_warning": False,
            "stall_probe_status": "healthy",
            "exact_error_code": None,
            "authority_snapshot_sha256": "7" * 64,
            "analysis_execution_receipt_sha256": None,
            "analysis_raw_output_sha256": None,
            "analysis_normalization_receipt_sha256": None,
            "analysis_report_sha256": None,
            "critical_review_execution_receipt_sha256": None,
            "critical_review_raw_output_sha256": None,
            "critical_review_normalization_receipt_sha256": None,
            "critical_review_report_sha256": None,
            "sol_handoff_envelope_sha256": None,
            "terminal_receipt_sha256": None,
            "last_state_change_at": "2026-08-12T10:00:00+00:00",
        }

    def public_detail(self) -> dict:
        with (
            mock.patch.object(dashboard, "_load_dispatch_events", return_value=([], [])),
            mock.patch.object(dashboard, "_verify_completion_authority", return_value=None),
            mock.patch.object(dashboard, "_verify_content_member_authority", return_value=None),
        ):
            return dashboard._public_dispatch_task_detail(
                self.detail,
                item=self.item,
                runtime_root=ROOT,
                include_raw=False,
                detail_authority_verified=True,
                output_schema_version=dashboard.TASK_DETAIL_SCHEMA_VERSION,
            )

    def test_internal_v2_maps_to_exact_public_v2_nine_axis_shape(self) -> None:
        value = self.public_detail()
        schema = json.loads(
            (ROOT / "schemas/dashboard-task-detail-v2.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(set(value), set(schema["required"]))
        self.assertEqual(value["schema_version"], dashboard.TASK_DETAIL_SCHEMA_VERSION)
        self.assertEqual(value["server_queue"], {
            "status": "unknown",
            "confirmation": "unconfirmed",
            "error_code": None,
        })
        self.assertEqual(value["evidence"]["authority_snapshot_sha256"], "7" * 64)
        self.assertEqual(value["progress"]["stall_probe_status"], "healthy")
        self.assertEqual(value["formal_write"]["formal_write_count"], 0)

    def test_public_v2_is_not_accepted_as_internal_v2(self) -> None:
        self.detail["schema_version"] = dashboard.TASK_DETAIL_SCHEMA_VERSION
        with self.assertRaisesRegex(
            dashboard.TaskDetailError, "task_detail_schema_mismatch"
        ):
            self.public_detail()

    def test_non_hash_input_fingerprint_fails_public_v2(self) -> None:
        self.item["input_fingerprint"] = "sha256:" + "6" * 64
        with self.assertRaisesRegex(
            dashboard.TaskDetailError, "task_detail_hash_invalid"
        ):
            self.public_detail()

    def test_server_queue_unknown_confirmed_and_clear_branches(self) -> None:
        unknown = self.public_detail()["server_queue"]
        self.assertEqual(
            unknown,
            {"status": "unknown", "confirmation": "unconfirmed", "error_code": None},
        )

        self.item.update(
            server_queue_status="confirmed_rate_limited",
            server_queue_confirmation="confirmed_event",
            exact_error_code="upstream_http_429",
        )
        limited = self.public_detail()["server_queue"]
        self.assertEqual(limited["status"], "confirmed_rate_limited")
        self.assertEqual(limited["confirmation"], "confirmed_event")
        self.assertEqual(limited["error_code"], "upstream_http_429")

        self.item.update(
            server_queue_status="clear",
            server_queue_confirmation="provider_receipt_verified",
            exact_error_code=None,
        )
        clear = self.public_detail()["server_queue"]
        self.assertEqual(
            clear,
            {
                "status": "clear",
                "confirmation": "provider_receipt_verified",
                "error_code": None,
            },
        )


class BatchAuthorityPrecedenceTests(unittest.TestCase):
    def test_terminal_receipt_outweighs_stale_running_event(self) -> None:
        item = {
            "queue_state": "running",
            "current_stage": "critical_review",
            "luna_status": "processing",
            "local_dispatch_status": "running",
            "model_stage": "critical_review",
            "terminal_status": None,
        }
        task = {
            "status": "execution_failed",
            "warning_codes": [],
            "terminal_receipt_sha256": H,
            "error_code": "critical_review_failed",
        }
        projection._apply_batch_task_state(item, task)
        self.assertEqual(item["local_dispatch_status"], "terminal")
        self.assertEqual(item["terminal_status"], "failed")
        self.assertEqual(item["terminal_receipt_sha256"], H)

    def test_newer_analysis_event_outweighs_stale_selected_batch(self) -> None:
        item = {
            "queue_state": "running",
            "current_stage": "analysis",
            "luna_status": "processing",
            "local_dispatch_status": "running",
            "model_stage": "analysis",
            "terminal_status": None,
        }
        projection._apply_batch_task_state(
            item,
            {"status": "selected", "quality_outcome": None},
        )
        self.assertEqual(item["queue_state"], "running")
        self.assertEqual(item["current_stage"], "analysis")


if __name__ == "__main__":
    unittest.main()
