from __future__ import annotations

import copy
import http.client
import json
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


DASHBOARD_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DASHBOARD_DIR))

import concurrency_campaign as campaign  # noqa: E402
import server as dashboard  # noqa: E402


def _sha(index: int) -> str:
    return f"{index:064x}"


def _v2_pending_subject(subject: str, *, index: int) -> dict:
    return {
        "subject": subject,
        "status": "awaiting_first_capture",
        "runtime_state": "armed",
        "capture_id": None,
        "unit_sha256": None,
        "terminal_outcome": None,
        "terminal_kind": None,
        "error_code": None,
        "stage": "not_started",
        "stage_timeout_seconds": campaign.SUBJECT_TIMEOUTS[subject],
        "mcp_namespace": campaign.SUBJECT_MCP[subject],
        "mcp_canonical_call_count": 0,
        "analysis_status": "not_started",
        "critical_review_status": "not_started",
        "report_status": "not_started",
        "report_json_ref": None,
        "report_json_sha256": None,
        "report_markdown_ref": None,
        "report_markdown_sha256": None,
        "report_reopen_status": "not_available",
        "package_status": "not_started",
        "package_ref": None,
        "package_sha256": None,
        "completion_sha256": None,
        "processing_receipt_sha256": None,
        "model_call_count": 0,
        "provider_request_count": 0,
        "provider_execution_contract_status": "not_verified",
        "provider_stage_identity_sha256s": {
            "analysis": None,
            "critical_review": None,
        },
        "provider_stage_exit_sha256s": {
            "analysis": None,
            "critical_review": None,
        },
        "provider_executable_sha256": None,
        "task_runner_executable_sha256": None,
        "runtime_observed_model_call_count": 0,
        "runtime_observed_provider_request_count": 0,
        "runtime_observed_mcp_tool_call_count": 0,
        "terminal_receipt_sha256": None,
        "terminal_index_sha256": _sha(30 + index),
        "runner_process_identity_sha256": None,
        "runner_process_exit_sha256": None,
        "runner_started_at": None,
        "runner_finished_at": None,
        "read_session_id": None,
        "evidence_generation": None,
        "evidence_authority_fingerprint": None,
        "evidence_refs": [],
        "late_result_fenced": False,
        "started_at": None,
        "completed_at": None,
    }


def v2_campaign(*, scope: str = "real_production_hmac_v2") -> dict:
    now = datetime.now(timezone.utc).isoformat()
    subjects = {
        subject: _v2_pending_subject(subject, index=index)
        for index, subject in enumerate(campaign.SUBJECTS, start=1)
    }
    real_scope = scope == "real_production_hmac_v2"
    if not real_scope:
        for row in subjects.values():
            row["provider_execution_contract_status"] = (
                "zero_model_fixture_not_applicable"
            )
    return {
        "schema_version": campaign.SCHEMA_VERSION,
        "evidence_scope": scope,
        "generated_at": now,
        "campaign_id": _sha(1),
        "release_id": "a" * 64,
        "current_release_usable": real_scope,
        "status": (
            "production_canary_active" if real_scope else "test_evidence_only"
        ),
        "result_label": (
            "production_canary_pending"
            if real_scope
            else "zero_model_fixture_evidence_only"
        ),
        "production_accepted": False,
        "model_request_contract": {
            "schema_version": "study-intake-model-request-contract-v2",
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "service_tier_policy": "absent",
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
        },
        "safety": {
            "production_evidence": real_scope,
            "provider_execution": False,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "runtime_observed_model_call_count": 0,
            "runtime_observed_provider_request_count": 0,
            "runtime_observed_mcp_tool_call_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        "canary": {
            "global_activation_id": _sha(2),
            "activated_at": now,
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "asynchronous_subject_canary": True,
            "slots": {
                subject: {
                    "subject": subject,
                    "activation_id": _sha(10 + index),
                    "producer_high_watermark_sha256": _sha(20 + index),
                    "runtime_state": "armed",
                    "verification_status": "awaiting_first_capture",
                    "capture_id": None,
                    "terminal_receipt_sha256": None,
                }
                for index, subject in enumerate(campaign.SUBJECTS, start=1)
            },
        },
        "concurrency": {
            "calculation_source": "hmac_terminal_index_task_supervisor_lifecycle",
            "interval_policy": (
                "half_open_end_before_start_zero_duration_nonoverlap"
            ),
            "terminal_task_count": 0,
            "active_task_count": 0,
            "lifecycle_task_count": 0,
            "runner_evidenced_task_count": 0,
            "runner_interval_missing_count": 0,
            "global_peak_active": 0,
            "subject_peak_active": {
                subject: 0 for subject in campaign.SUBJECTS
            },
            "overlap_observed": False,
            "overlap_subjects": [],
            "terminal_index_sha256_by_subject": {
                subject: _sha(30 + index)
                for index, subject in enumerate(campaign.SUBJECTS, start=1)
            },
            "failure_bindings_by_subject": {
                subject: [] for subject in campaign.SUBJECTS
            },
            "telemetry_cross_check": "not_available",
        },
        "subjects": subjects,
    }


def mark_verified(
    payload: dict,
    subject: str,
    *,
    index: int,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    terminal_sha = _sha(100 + index)
    slot = payload["canary"]["slots"][subject]
    slot.update(
        {
            "runtime_state": "continuous_concurrent_unlocked",
            "verification_status": "verified",
            "capture_id": f"capture-{subject}-001",
            "terminal_receipt_sha256": terminal_sha,
        }
    )
    row = payload["subjects"][subject]
    row.update(
        {
            "status": "verified",
            "runtime_state": "continuous_concurrent_unlocked",
            "capture_id": f"capture-{subject}-001",
            "unit_sha256": _sha(110 + index),
            "terminal_outcome": "succeeded",
            "terminal_kind": "dispatch-success-v2",
            "stage": "report_verified",
            "mcp_canonical_call_count": 2,
            "analysis_status": "completed",
            "critical_review_status": "completed",
            "report_status": "reopen_verified",
            "report_json_ref": (
                f"study-intake-report://sha256/{_sha(120 + index)}"
            ),
            "report_json_sha256": _sha(120 + index),
            "report_markdown_ref": (
                "study-intake-report-markdown://sha256/"
                f"{_sha(121 + index)}"
            ),
            "report_markdown_sha256": _sha(121 + index),
            "report_reopen_status": "json_markdown_package_verified",
            "package_status": "reopen_verified",
            "package_ref": (
                "study-intake-dispatch-package://sha256/"
                f"{_sha(122 + index)}"
            ),
            "package_sha256": _sha(122 + index),
            "completion_sha256": _sha(130 + index),
            "processing_receipt_sha256": _sha(140 + index),
            "model_call_count": 2,
            "provider_request_count": 4,
            "provider_execution_contract_status": (
                "verified_no_fast_mode_argv_environment"
            ),
            "provider_stage_identity_sha256s": {
                "analysis": _sha(180 + index),
                "critical_review": _sha(181 + index),
            },
            "provider_stage_exit_sha256s": {
                "analysis": _sha(190 + index),
                "critical_review": _sha(191 + index),
            },
            "provider_executable_sha256": _sha(192 + index),
            "task_runner_executable_sha256": _sha(193 + index),
            "runtime_observed_model_call_count": 2,
            "runtime_observed_provider_request_count": 4,
            "runtime_observed_mcp_tool_call_count": 2,
            "terminal_receipt_sha256": terminal_sha,
            "runner_process_identity_sha256": _sha(150 + index),
            "runner_process_exit_sha256": _sha(160 + index),
            "runner_started_at": now,
            "runner_finished_at": now,
            "read_session_id": f"session-{subject}-001",
            "evidence_generation": f"generation-{subject}-001",
            "evidence_authority_fingerprint": _sha(170 + index),
            "evidence_refs": [
                f"artifact:{subject}:001",
                f"context:{subject}:001",
            ],
            "started_at": now,
            "completed_at": now,
        }
    )
    safety = payload["safety"]
    safety["provider_execution"] = True
    safety["model_call_count"] += 2
    safety["provider_request_count"] += 4
    safety["mcp_tool_call_count"] += 2
    safety["runtime_observed_model_call_count"] += 2
    safety["runtime_observed_provider_request_count"] += 4
    safety["runtime_observed_mcp_tool_call_count"] += 2
    concurrency = payload["concurrency"]
    concurrency["terminal_task_count"] += 1
    concurrency["lifecycle_task_count"] += 1
    concurrency["runner_evidenced_task_count"] += 1
    concurrency["global_peak_active"] = max(
        concurrency["global_peak_active"], 1
    )
    concurrency["subject_peak_active"][subject] = 1
    payload["status"] = "production_canary_active"
    payload["result_label"] = "production_canary_partially_verified"


def mark_failed_paused(payload: dict, subject: str, *, index: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    terminal_sha = _sha(200 + index)
    unit_sha = _sha(210 + index)
    slot = payload["canary"]["slots"][subject]
    slot.update(
        {
            "runtime_state": "failed_drained",
            "verification_status": "failed_paused",
            "capture_id": f"capture-{subject}-failed-001",
            "terminal_receipt_sha256": terminal_sha,
        }
    )
    row = payload["subjects"][subject]
    row.update(
        {
            "status": "failed_paused",
            "runtime_state": "failed_drained",
            "capture_id": f"capture-{subject}-failed-001",
            "unit_sha256": unit_sha,
            "terminal_outcome": "failed",
            "terminal_kind": "dispatch-failure-v2",
            "error_code": "fixture-failure",
            "stage": "failed",
            "analysis_status": "failed",
            "critical_review_status": "failed",
            "report_status": "failed",
            "package_status": "failed",
            "terminal_receipt_sha256": terminal_sha,
            "late_result_fenced": True,
            "completed_at": now,
        }
    )
    concurrency = payload["concurrency"]
    concurrency["terminal_task_count"] += 1
    concurrency["lifecycle_task_count"] += 1
    concurrency["runner_interval_missing_count"] += 1
    concurrency["failure_bindings_by_subject"][subject].append(
        {
            "unit_sha256": unit_sha,
            "outcome": "failed",
            "error_code": "fixture-failure",
            "terminal_kind": "dispatch-failure-v2",
            "terminal_receipt_sha256": terminal_sha,
            "late_result_fenced": True,
        }
    )
    payload["status"] = "production_canary_active_with_subject_failure"
    payload["result_label"] = "production_canary_subject_failure"


def legacy_v1_campaign() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    subjects = {}
    for index, subject in enumerate(campaign.SUBJECTS, start=1):
        subjects[subject] = {
            "subject": subject,
            "capture_id": f"FRESH-{subject.upper()}-001",
            "stage": "report_verified",
            "stage_timeout_seconds": campaign.SUBJECT_TIMEOUTS[subject],
            "mcp_namespace": campaign.SUBJECT_MCP[subject],
            "mcp_canonical_call_count": index,
            "analysis_status": "completed",
            "critical_review_status": "completed",
            "report_status": "verified",
            "report_ref": f"report:{subject}:001",
            "report_sha256": _sha(index),
            "package_ref": f"package:{subject}:001",
            "package_sha256": _sha(index + 10),
            "model_call_count": 2,
            "provider_request_count": 2,
            "error_code": None,
            "started_at": now,
            "completed_at": now,
        }
    return {
        "schema_version": campaign.LEGACY_SCHEMA_VERSION,
        "generated_at": now,
        "campaign_id": "THREE-SUBJECT-CONCURRENCY-001",
        "release_id": "a" * 64,
        "status": "passed",
        "result_label": "three_subject_concurrency_runtime_accepted",
        "production_accepted": False,
        "barrier": {
            "expected": 3,
            "arrived": 3,
            "global_peak_active": 3,
            "per_subject_peak_active": {
                "math": 1,
                "cs408": 1,
                "english": 1,
            },
            "submitted_at_spread_ms": 1500,
            "released_at": now,
            "completed_at": now,
        },
        "fast_mode": {
            "requested": True,
            "service_tier": "priority",
            "effective_status": "requested_unverified",
        },
        "safety": {
            "provider_execution": True,
            "model_call_count": 6,
            "provider_request_count": 6,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        "canary": {
            "activation_id": "PRODUCTION-CANARY-001",
            "activated_at": now,
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "per_subject_limit": 1,
            "slots": {
                subject: {
                    "subject": subject,
                    "producer_high_watermark_sha256": _sha(index + 20),
                    "state": "succeeded",
                    "capture_id": f"FRESH-{subject.upper()}-001",
                    "completion_receipt_sha256": _sha(index + 30),
                }
                for index, subject in enumerate(campaign.SUBJECTS, start=1)
            },
        },
        "subjects": subjects,
    }


class CampaignContractTest(unittest.TestCase):
    def test_v2_schema_closes_provider_public_surface_to_status_and_hashes(self) -> None:
        schema = json.loads(
            (
                DASHBOARD_DIR.parent
                / "schemas/three-subject-concurrency-campaign-v2.json"
            ).read_text(encoding="utf-8")
        )
        self.assertFalse(schema["additionalProperties"])
        subject = schema["$defs"]["subject"]
        self.assertFalse(subject["additionalProperties"])
        for field in (
            "provider_execution_contract_status",
            "provider_stage_identity_sha256s",
            "provider_stage_exit_sha256s",
            "provider_executable_sha256",
            "task_runner_executable_sha256",
        ):
            self.assertIn(field, subject["required"])
        self.assertEqual(
            set(
                subject["properties"][
                    "provider_execution_contract_status"
                ]["enum"]
            ),
            {
                "verified_no_fast_mode_argv_environment",
                "zero_model_fixture_not_applicable",
                "not_verified",
            },
        )
        stage_hashes = schema["$defs"]["providerStageHashes"]
        self.assertFalse(stage_hashes["additionalProperties"])
        self.assertEqual(
            set(stage_hashes["required"]), {"analysis", "critical_review"}
        )
        for forbidden in (
            "argv",
            "environment_key_names",
            "provider_process_identity_path",
            "provider_process_exit_path",
            "executable_path",
            "cwd",
        ):
            self.assertNotIn(forbidden, subject["properties"])
        for definition in (
            "providerStageHashesClosed",
            "providerStageHashesEmpty",
        ):
            self.assertFalse(
                schema["$defs"][definition]["additionalProperties"]
            )
            self.assertEqual(
                set(schema["$defs"][definition]["required"]),
                {"analysis", "critical_review"},
            )
        fixture_then = schema["allOf"][0]["then"]["properties"]["subjects"]
        real_else = schema["allOf"][0]["else"]["properties"]["subjects"]
        for subject_name in campaign.SUBJECTS:
            self.assertEqual(
                fixture_then["properties"][subject_name]["$ref"],
                "#/$defs/fixtureProviderSubject",
            )
            self.assertEqual(
                real_else["properties"][subject_name]["$ref"],
                "#/$defs/realProviderSubject",
            )

    def test_v2_current_pending_campaign_is_valid(self) -> None:
        self.assertIsNone(campaign.campaign_contract_error(v2_campaign()))

    def test_v2_supports_asynchronous_subject_verification_without_barrier(self) -> None:
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        self.assertIsNone(campaign.campaign_contract_error(payload))
        self.assertEqual(
            payload["subjects"]["math"]["report_status"], "reopen_verified"
        )
        self.assertEqual(
            payload["subjects"]["math"]["package_status"], "reopen_verified"
        )
        self.assertEqual(payload["concurrency"]["global_peak_active"], 1)
        self.assertFalse(payload["concurrency"]["overlap_observed"])
        self.assertEqual(
            payload["subjects"]["math"][
                "provider_execution_contract_status"
            ],
            "verified_no_fast_mode_argv_environment",
        )

    def test_v2_success_requires_reopened_runtime_and_grounding_closures(self) -> None:
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        payload["subjects"]["math"]["completion_sha256"] = None
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_subject_success_invalid",
        )
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        payload["subjects"]["math"]["model_call_count"] = 1
        payload["safety"]["model_call_count"] = 1
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_subject_counter_scope_invalid",
        )

    def test_v2_failure_requires_exact_terminal_index_failure_binding(self) -> None:
        payload = v2_campaign()
        mark_failed_paused(payload, "english", index=3)
        self.assertIsNone(campaign.campaign_contract_error(payload))
        payload["concurrency"]["failure_bindings_by_subject"]["english"] = []
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_subject_failure_binding_invalid",
        )

    def test_v2_zero_model_fixture_cannot_masquerade_as_current(self) -> None:
        payload = v2_campaign(scope="zero_model_fixture_v2")
        self.assertIsNone(campaign.campaign_contract_error(payload))
        self.assertEqual(
            campaign.campaign_runtime_error(
                payload,
                expected_release_id="a" * 64,
            ),
            "campaign_evidence_scope_not_live",
        )
        payload["current_release_usable"] = True
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_current_release_scope_invalid",
        )
        payload = v2_campaign(scope="zero_model_fixture_v2")
        payload["subjects"]["math"][
            "provider_execution_contract_status"
        ] = "verified_no_fast_mode_argv_environment"
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_fixture_provider_contract_invalid",
        )

    def test_v2_provider_execution_contract_is_hash_only_and_required(self) -> None:
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        payload["subjects"]["math"].pop(
            "provider_stage_exit_sha256s"
        )
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_subject_invalid",
        )
        payload = v2_campaign()
        payload["subjects"]["math"]["provider_argv"] = ["codex", "exec"]
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_subject_invalid",
        )

    def test_v2_requires_no_tier_model_contract_and_fixed_limits(self) -> None:
        payload = v2_campaign()
        payload["model_request_contract"]["requested_service_tier"] = "priority"
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_model_request_contract_invalid",
        )
        payload = v2_campaign()
        payload["canary"]["continuous_concurrency_limit"] = 21
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_canary_invalid",
        )

    def test_v2_rejects_missing_current_fields_and_legacy_fast_barrier_aliases(self) -> None:
        for field in ("model_request_contract", "concurrency"):
            with self.subTest(missing=field):
                payload = v2_campaign()
                payload.pop(field)
                self.assertEqual(
                    campaign.campaign_contract_error(payload),
                    "campaign_v2_topology_invalid",
                )
        payload = v2_campaign()
        payload["barrier"] = legacy_v1_campaign()["barrier"]
        payload["fast_mode"] = legacy_v1_campaign()["fast_mode"]
        self.assertEqual(
            campaign.campaign_contract_error(payload),
            "campaign_v2_topology_invalid",
        )

    def test_v2_runtime_gate_rejects_cross_release_and_stale_campaigns(self) -> None:
        payload = v2_campaign()
        self.assertEqual(
            campaign.campaign_runtime_error(
                payload,
                expected_release_id="b" * 64,
            ),
            "campaign_release_mismatch",
        )
        observed_at = datetime.now(timezone.utc)
        payload["generated_at"] = (
            observed_at - timedelta(seconds=campaign.MAX_AGE_SECONDS + 1)
        ).isoformat()
        self.assertEqual(
            campaign.campaign_runtime_error(
                payload,
                expected_release_id="a" * 64,
                now=observed_at,
            ),
            "campaign_stale",
        )

    def test_legacy_v1_is_valid_historical_evidence_only(self) -> None:
        payload = legacy_v1_campaign()
        self.assertIsNone(campaign.campaign_contract_error(payload))
        self.assertEqual(
            campaign.campaign_runtime_error(
                payload,
                expected_release_id="a" * 64,
            ),
            "campaign_historical_legacy_only",
        )

    def test_public_subject_filter_does_not_mutate_projection(self) -> None:
        payload = v2_campaign()
        before = copy.deepcopy(payload)
        public = campaign.public_campaign(payload, subject="english")
        self.assertEqual(set(public["subjects"]), {"english"})
        public["subjects"]["english"]["provider_stage_identity_sha256s"][
            "analysis"
        ] = _sha(999)
        public["concurrency"]["subject_peak_active"]["english"] = 9
        self.assertEqual(payload, before)


class CampaignEndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="campaign-dashboard-")
        root = Path(self.temp.name)
        self.campaign_path = root / "campaign.json"
        self.campaign_path.write_text(
            json.dumps(v2_campaign()), encoding="utf-8"
        )
        self.httpd = dashboard.DashboardHTTPServer(
            ("127.0.0.1", 0),
            dashboard.DashboardHandler,
            store=dashboard.ProjectionStore(root / "projection.json"),
            campaign_store=campaign.CampaignStore(self.campaign_path),
            expected_release_id="a" * 64,
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def request(self, path: str) -> tuple[int, dict]:
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.httpd.server_address[1], timeout=3
        )
        connection.request("GET", path, headers={"Host": "127.0.0.1:8767"})
        response = connection.getresponse()
        body = json.loads(response.read().decode("utf-8"))
        connection.close()
        return response.status, body

    def test_v2_subject_filter_returns_only_requested_subject(self) -> None:
        status, body = self.request(
            "/api/v1/concurrency-campaign?subject=english"
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["available"])
        self.assertEqual(set(body["subjects"]), {"english"})
        self.assertFalse(body["production_accepted"])
        self.assertEqual(body["safety"]["formal_write_count"], 0)
        self.assertFalse(body["safety"]["sol_enabled"])
        self.assertFalse(body["model_request_contract"]["fast_mode_requested"])

    def test_v2_api_exposes_only_provider_status_and_stage_hashes(self) -> None:
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
        status, body = self.request(
            "/api/v1/concurrency-campaign?subject=math"
        )
        self.assertEqual(status, 200)
        row = body["subjects"]["math"]
        self.assertEqual(
            row["provider_execution_contract_status"],
            "verified_no_fast_mode_argv_environment",
        )
        self.assertEqual(
            set(row["provider_stage_identity_sha256s"]),
            {"analysis", "critical_review"},
        )
        self.assertEqual(
            set(row["provider_stage_exit_sha256s"]),
            {"analysis", "critical_review"},
        )
        serialized = json.dumps(body, sort_keys=True)
        for forbidden in (
            '"argv":',
            '"environment_key_names":',
            '"provider_process_identity_path":',
            '"provider_process_exit_path":',
            '"executable_path":',
            '"cwd":',
        ):
            self.assertNotIn(forbidden, serialized)

    def test_v2_verified_api_requires_report_package_and_two_provider_stages(self) -> None:
        payload = v2_campaign()
        mark_verified(payload, "math", index=1)
        self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
        status, body = self.request(
            "/api/v1/concurrency-campaign?subject=math"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["subjects"]["math"]["status"], "verified")

        payload["subjects"]["math"]["provider_stage_exit_sha256s"][
            "critical_review"
        ] = None
        self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
        status, body = self.request(
            "/api/v1/concurrency-campaign?subject=math"
        )
        self.assertEqual(status, 503)
        self.assertFalse(body["available"])
        serialized = json.dumps(body, sort_keys=True)
        self.assertNotIn("provider_argv", serialized)
        self.assertNotIn("environment_key_names", serialized)
        self.assertNotIn("executable_path", serialized)

    def test_invalid_projection_does_not_emit_zero_safety_proof(self) -> None:
        payload = v2_campaign()
        payload["production_accepted"] = True
        self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
        status, body = self.request("/api/v1/concurrency-campaign")
        self.assertEqual(status, 503)
        self.assertFalse(body["available"])
        self.assertIsNone(body["formal_write_count"])
        self.assertIsNone(body["sol_enabled"])

    def test_v2_zero_model_fixture_is_visible_but_never_current_release_usable(self) -> None:
        payload = v2_campaign(scope="zero_model_fixture_v2")
        self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
        status, body = self.request("/api/v1/concurrency-campaign")
        self.assertEqual(status, 200)
        self.assertTrue(body["available"])
        self.assertEqual(body["status"], "test_evidence_only")
        self.assertFalse(body["current_release_usable"])
        self.assertFalse(body["production_accepted"])
        self.assertEqual(
            {
                row["provider_execution_contract_status"]
                for row in body["subjects"].values()
            },
            {"zero_model_fixture_not_applicable"},
        )

    def test_legacy_v1_projection_is_historical_not_live(self) -> None:
        for release_id in ("a" * 64, "b" * 64):
            with self.subTest(release_id=release_id):
                payload = legacy_v1_campaign()
                payload["release_id"] = release_id
                self.campaign_path.write_text(json.dumps(payload), encoding="utf-8")
                status, body = self.request("/api/v1/concurrency-campaign")
                self.assertEqual(status, 200)
                self.assertTrue(body["available"])
                self.assertEqual(body["status"], "historical_legacy")
                self.assertEqual(body["evidence_scope"], "historical_legacy_v1")
                self.assertFalse(body["current_release_usable"])
                self.assertFalse(body["production_accepted"])


if __name__ == "__main__":
    unittest.main()
