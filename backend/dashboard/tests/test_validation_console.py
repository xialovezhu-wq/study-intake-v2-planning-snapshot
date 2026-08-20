from __future__ import annotations

import http.client
import json
import shutil
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path


DASHBOARD_DIR = Path(__file__).resolve().parents[1]
FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "dashboard_projection.json"
sys.path.insert(0, str(DASHBOARD_DIR))

import server as dashboard  # noqa: E402
from validation_console import (  # noqa: E402
    ValidationConsoleError,
    ValidationConsoleStore,
)


def sha(seed: str) -> str:
    return __import__("hashlib").sha256(seed.encode("utf-8")).hexdigest()


def payload(subject: str = "math", capture_id: str = "CAP-FIXTURE-1") -> dict:
    return {
        "subject": subject,
        "capture_id": capture_id,
        "capture_content_sha256": sha(f"{subject}:{capture_id}"),
        "activation_id": sha(f"activation:{subject}"),
        "user_authorization_receipt_sha256": sha(
            f"user-authorization:{subject}:{capture_id}"
        ),
        "maximum_tasks": 1,
        "formal_write_allowed": False,
    }


class ValidationConsoleStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="validation-console-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.release_id = "a" * 64
        self.store = ValidationConsoleStore(
            self.root / "fixture",
            fixture_mode=True,
            central_release_id=self.release_id,
        )

    def write_headers(self, nonce: str) -> dict:
        state = self.store.public_state()
        return {
            "csrf_token": state["csrf_token"],
            "nonce": nonce,
            "expected_revision": state["revision"],
        }

    def test_fixture_stage_authorization_is_one_shot_and_terminal_relocks(self) -> None:
        issued = self.store.issue_authorization(
            "terra",
            payload(),
            **self.write_headers("nonce-terra-fixture-0001"),
        )
        self.assertEqual(issued["status"], "fixture_authorization_issued")
        self.assertRegex(issued["live_authorization_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(issued["issuer_receipt_sha256"], r"^[0-9a-f]{64}$")
        active = self.store.public_state()
        self.assertEqual(active["authorization"], "PRESENT")
        terminal = self.store.terminal_relock(
            "terra",
            "succeeded",
            **self.write_headers("nonce-terra-terminal-0001"),
        )
        self.assertEqual(terminal["remaining_tasks"], 0)
        self.assertEqual(self.store.public_state()["authorization"], "ABSENT")
        with self.assertRaises(ValidationConsoleError) as caught:
            self.store.terminal_relock(
                "terra",
                "succeeded",
                **self.write_headers("nonce-terra-terminal-0002"),
            )
        self.assertEqual(caught.exception.code, "authorization_missing")

    def test_nonce_replay_and_stale_revision_are_rejected(self) -> None:
        headers = self.write_headers("nonce-replay-fixture-0001")
        self.store.emergency_lock(**headers)
        with self.assertRaises(ValidationConsoleError) as caught:
            self.store.emergency_lock(**headers)
        self.assertIn(
            caught.exception.code,
            {"state_precondition_failed", "nonce_replayed"},
        )

    def test_luna_requires_verified_terra_plan(self) -> None:
        with self.assertRaises(ValidationConsoleError) as caught:
            self.store.issue_authorization(
                "luna",
                payload(),
                **self.write_headers("nonce-luna-no-plan-0001"),
            )
        self.assertEqual(caught.exception.code, "terra_plan_required")
        value = {
            **payload(),
            "verified_terra_plan_sha256": sha("verified-plan"),
        }
        issued = self.store.issue_authorization(
            "luna",
            value,
            **self.write_headers("nonce-luna-plan-0001"),
        )
        self.assertEqual(issued["stage"], "luna")

    def test_three_subject_and_same_subject_campaigns_have_independent_authorizations(self) -> None:
        three_subject = self.store.create_campaign(
            {
                "captures": [
                    payload("math", "CAP-MATH-1"),
                    payload("cs408", "CAP-CS408-1"),
                    payload("english", "CAP-ENGLISH-1"),
                ]
            },
            **self.write_headers("nonce-campaign-three-0001"),
        )
        self.assertEqual(three_subject["authorization_count"], 3)
        self.assertEqual(three_subject["backlog_scan_count"], 0)
        self.assertEqual(three_subject["model_call_count"], 0)
        same_subject = self.store.create_campaign(
            {
                "captures": [
                    payload("math", "CAP-MATH-A"),
                    payload("math", "CAP-MATH-B"),
                    payload("math", "CAP-MATH-C"),
                ]
            },
            **self.write_headers("nonce-campaign-math-0001"),
        )
        self.assertEqual(same_subject["authorization_count"], 3)
        self.assertNotEqual(
            same_subject["campaign_id"], three_subject["campaign_id"]
        )

    def test_handoff_outcomes_map_to_normal_risk_and_diagnostic_without_sol(self) -> None:
        expectations = {
            "accepted": "normal",
            "corrected": "normal",
            "issues_found": "risk",
            "technical_quarantine": "diagnostic",
        }
        for index, (outcome, package_kind) in enumerate(expectations.items()):
            result = self.store.export_handoff(
                {"quality_outcome": outcome},
                **self.write_headers(f"nonce-handoff-{index:04d}"),
            )
            self.assertEqual(result["package_kind"], package_kind)
            self.assertFalse(result["calls_sol_model"])
            self.assertEqual(result["formal_write_count"], 0)

    def test_fixture_promotion_never_changes_production_state(self) -> None:
        receipts = [sha("stage-1"), sha("stage-2"), sha("stage-3")]
        preview = self.store.promotion_preview(
            {"live_stage_receipts": receipts}
        )
        self.assertTrue(preview["eligible"])
        applied = self.store.promotion_apply(
            {"live_stage_receipts": receipts},
            **self.write_headers("nonce-promotion-fixture-0001"),
        )
        self.assertFalse(applied["production_state_changed"])
        self.assertFalse(applied["production_accepted"])
        self.assertFalse(self.store.public_state()["production_accepted"])

    def test_production_store_rejects_authorization_campaign_handoff_and_promotion(self) -> None:
        production = ValidationConsoleStore(
            self.root / "production",
            fixture_mode=False,
            central_release_id=self.release_id,
        )
        state = production.public_state()
        headers = {
            "csrf_token": state["csrf_token"],
            "nonce": "nonce-production-locked-0001",
            "expected_revision": state["revision"],
        }
        operations = [
            lambda: production.issue_authorization(
                "terra", payload(), **headers
            ),
            lambda: production.create_campaign(
                {"captures": [payload()]}, **headers
            ),
            lambda: production.export_handoff(
                {"quality_outcome": "accepted"}, **headers
            ),
            lambda: production.promotion_apply(
                {"live_stage_receipts": [sha("1"), sha("2"), sha("3")]},
                **headers,
            ),
        ]
        for operation in operations:
            with self.assertRaises(ValidationConsoleError):
                operation()
        reopened = ValidationConsoleStore(
            self.root / "production",
            fixture_mode=False,
            central_release_id=self.release_id,
        )
        public = reopened.public_state()
        self.assertEqual(public["authorization"], "ABSENT")
        self.assertEqual(public["live_gate"], "LOCKED")
        self.assertFalse(public["production_accepted"])
        raw = json.loads(reopened.state_path.read_text(encoding="utf-8"))
        self.assertEqual(raw["production_promotion_attempt_count"], 0)

    def test_technical_status_reopens_and_drives_only_live_remaining_flag(self) -> None:
        status = {
            "schema_version": "study-intake-validation-console-technical-status-v1",
            "central_release_id": self.release_id,
            "execution_mode": "offline",
            "live_gate_locked": True,
            "manual_authorization_present": False,
            "production_accepted": False,
            "formal_write_count": 0,
            "skills": {subject: "passed" for subject in ("math", "cs408", "english")},
            "mcp_preflight": {subject: "passed" for subject in ("math", "cs408", "english")},
            "engineering": {
                gate: "passed"
                for gate in (
                    "validation_console_frontend",
                    "validation_console_backend",
                    "production_issuer",
                    "campaign_control",
                    "promotion_gate",
                    "immutable_build",
                    "transactional_deploy",
                    "data_integrity",
                )
            },
            "reports": {},
            "audit_package": {},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.store.publish_technical_status(status)
        self.assertTrue(
            self.store.public_state()[
                "only_real_model_capture_validation_remains"
            ]
        )


class ValidationConsoleHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory(prefix="validation-console-http-")
        root = Path(cls.temp.name)
        projection = root / "state/dashboard_projection.json"
        projection.parent.mkdir(parents=True)
        shutil.copy2(FIXTURE_PATH, projection)
        cls.validation = ValidationConsoleStore(
            root / "state/validation-console",
            fixture_mode=False,
            central_release_id="b" * 64,
        )
        cls.httpd = dashboard.DashboardHTTPServer(
            ("127.0.0.1", 0),
            dashboard.DashboardHandler,
            store=dashboard.ProjectionStore(projection),
            expected_release_id="b" * 64,
            validation_store=cls.validation,
        )
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.httpd.server_address[1]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)
        cls.temp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        request_headers = {"Host": f"127.0.0.1:{self.port}", **(headers or {})}
        raw = None
        if body is not None:
            raw = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
            request_headers["Content-Length"] = str(len(raw))
        connection.request(method, path, body=raw, headers=request_headers)
        response = connection.getresponse()
        payload = response.read()
        response_headers = {key: value for key, value in response.getheaders()}
        connection.close()
        return response.status, payload, response_headers

    def test_static_console_and_state_endpoint_are_available(self) -> None:
        status, body, _headers = self.request("GET", "/validation-console/")
        self.assertEqual(status, 200)
        self.assertIn("上线前验收控制台".encode("utf-8"), body)
        status, body, _headers = self.request(
            "GET", "/api/v1/validation-console/state"
        )
        self.assertEqual(status, 200)
        value = json.loads(body)
        self.assertEqual(value["execution_mode"], "OFFLINE")
        self.assertEqual(value["live_gate"], "LOCKED")
        self.assertEqual(value["authorization"], "ABSENT")
        self.assertFalse(value["production_accepted"])

    def test_production_authorize_and_preflight_posts_are_locked(self) -> None:
        state = self.validation.public_state()
        headers = {
            "X-Study-CSRF": state["csrf_token"],
            "X-Study-Nonce": "nonce-http-locked-fixture-0001",
            "If-Match": str(state["revision"]),
        }
        status, body, _headers = self.request(
            "POST",
            "/api/v1/validation-console/authorize/terra",
            body=payload(),
            headers=headers,
        )
        self.assertEqual(status, 423)
        self.assertEqual(json.loads(body)["error"], "offline_locked")
        headers["X-Study-Nonce"] = "nonce-http-preflight-fixture-0001"
        status, body, _headers = self.request(
            "POST",
            "/api/v1/validation-console/preflight",
            body={},
            headers=headers,
        )
        self.assertEqual(status, 423)
        self.assertEqual(json.loads(body)["error"], "operator_preflight_only")

    def test_original_dashboard_post_contract_remains_get_only(self) -> None:
        status, body, headers = self.request("POST", "/api/v1/items", body={})
        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "GET")
        self.assertEqual(json.loads(body)["error"], "method_not_allowed")


if __name__ == "__main__":
    unittest.main()
