from __future__ import annotations

import copy
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from scripts import pre_model_p0_gate as gate


AUDIT_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-interface-audit-20260809"
)
ARTIFACT_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-model-lane-staging-20260809/artifacts/pre-model-gate"
)
MATRIX = ARTIFACT_ROOT / "p0-matrix.json"
GOLDEN = ARTIFACT_ROOT / "zero-model-golden-inventory.json"


class PreModelP0GateTests(unittest.TestCase):
    @staticmethod
    def needs_user_matrix() -> dict:
        value = copy.deepcopy(json.loads(MATRIX.read_text(encoding="utf-8")))
        issue = next(
            row for row in value["issues"] if row["issue_id"] == "EN-P0-006"
        )
        issue["gate_status"] = "needs_user_decision"
        issue["implementation_state"] = "historical_open_fixture"
        issue["remaining_action"] = "fixture still needs complete evidence"
        issue["closure_checks"] = [
            {
                "check_id": "historical-open-fixture",
                "status": "pending",
                "evidence": "test fixture only",
            }
        ]
        issue["requires_user_disposition"] = True
        issue["legacy_disposition"].update(
            {
                "inventory_status": "evidence_incomplete",
                "identified_target_count": 97,
                "unidentified_target_count": 1,
                "disposition_receipt_count": 0,
            }
        )
        return value

    @staticmethod
    def closed_v3_matrix() -> dict:
        value = copy.deepcopy(json.loads(MATRIX.read_text(encoding="utf-8")))
        issue = next(
            row for row in value["issues"] if row["issue_id"] == "EN-P0-006"
        )
        issue["gate_status"] = "closed"
        issue["implementation_state"] = "execution_closure_verified"
        issue["requires_user_disposition"] = True
        issue["remaining_action"] = "none"
        issue["closure_checks"] = [
            {
                "check_id": "verified-v3-execution-closure",
                "status": "passed",
                "evidence": "test fixture binds the reopened execution closure",
            }
        ]
        issue["legacy_disposition"].update(
            {
                "inventory_status": "complete",
                "identified_target_count": 98,
                "unidentified_target_count": 0,
                "disposition_receipt_count": 98,
            }
        )
        return value

    @staticmethod
    def verified_execution() -> dict:
        return {
            "schema_version": "verified_english_legacy_execution_closure_v1",
            "batch_id": "EN-P0-006-BATCH-TEST",
            "batch_sha256": "a" * 64,
            "closure_sha256": "b" * 64,
            "closure": {
                "status": "verified_complete",
                "target_count": 98,
                "committed_count": 7,
                "already_current_count": 91,
                "failed_count": 0,
                "omitted_count": 0,
                "unknown_count": 0,
                "duplicate_count": 0,
                "recovery_pending_count": 0,
                "formal_write_count": 7,
            },
            "checkpoint_count": 98,
            "item_result_receipt_count": 98,
            "review_receipt_count": 98,
            "recovery_receipt_count": 0,
            "status": "verified_complete",
            "formal_write_count": 7,
        }

    @staticmethod
    def closed_existing_inventory_matrix() -> dict:
        value = PreModelP0GateTests.closed_v3_matrix()
        issue = next(
            row for row in value["issues"] if row["issue_id"] == "EN-P0-006"
        )
        issue["implementation_state"] = (
            "verified_existing_inventory_no_recuration_required"
        )
        issue["requires_user_disposition"] = False
        issue["closure_checks"] = [
            {
                "check_id": "english-existing-inventory-zero-model-closure",
                "status": "passed",
                "evidence": "98 current objects and per-target HMAC receipts reopened",
            }
        ]
        return value

    @staticmethod
    def verified_existing_inventory() -> dict:
        return {
            "schema_version": (
                "english_legacy_authorization_expansion_verification_v1"
            ),
            "status": "verified_complete",
            "outcome": (
                "verified_existing_inventory_no_recuration_required"
            ),
            "target_count": 98,
            "current_object_identity_and_hash_verified_count": 98,
            "per_target_hmac_receipt_count": 98,
            "sp_022_verified": True,
            "mutation_write_set_verified": True,
            "luna_recuration_allowed": False,
            "sol_parent_batch_allowed": False,
            "unidentified_target_count": 0,
            "omitted_target_count": 0,
            "duplicate_target_count": 0,
            "unknown_target_count": 0,
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def test_real_closed_matrix_requires_reopenable_zero_model_closure(self) -> None:
        with self.assertRaisesRegex(
            gate.P0GateError, "en_p0_006_requires_verified_closure"
        ):
            gate.evaluate_gate(MATRIX, AUDIT_ROOT, GOLDEN)

    def test_en_p0_006_cannot_be_closed_by_editing_one_status(self) -> None:
        value = self.needs_user_matrix()
        issue = next(
            row for row in value["issues"] if row["issue_id"] == "EN-P0-006"
        )
        issue["gate_status"] = "closed"
        issue["closure_checks"] = [
            {
                "check_id": "forged",
                "status": "passed",
                "evidence": "a status edit is not a signed per-target disposition",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "en_p0_006_must_need_user_decision"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

    def test_only_signed_future_contract_can_supersede_complete_disposition(self) -> None:
        value = self.needs_user_matrix()
        issue = next(
            row for row in value["issues"] if row["issue_id"] == "EN-P0-006"
        )
        legacy = issue["legacy_disposition"]
        legacy["inventory_status"] = "complete"
        legacy["unidentified_target_count"] = 0
        legacy["disposition_receipt_count"] = legacy["identified_target_count"]
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "en_p0_006_requires_signed_v2_closure"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

    def test_closing_every_other_issue_still_blocks_on_en_p0_006(self) -> None:
        value = self.needs_user_matrix()
        for issue in value["issues"]:
            if issue["issue_id"] == "EN-P0-006":
                continue
            issue["gate_status"] = "closed"
            issue["closure_checks"] = [
                {
                    "check_id": f"synthetic-pass-{issue['issue_id']}",
                    "status": "passed",
                    "evidence": "test fixture only",
                }
            ]
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            result = gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["blocking_issue_ids"], ["EN-P0-006"])
        self.assertEqual(result["model_call_count"], 0)

    def test_audit_issue_set_is_exact_and_cannot_be_omitted(self) -> None:
        value = self.needs_user_matrix()
        value["issues"] = value["issues"][:-1]
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "p0_matrix_issue_set_mismatch"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

    def test_closed_issue_requires_only_passed_checks(self) -> None:
        value = json.loads(MATRIX.read_text(encoding="utf-8"))
        value = copy.deepcopy(value)
        issue = next(
            row
            for row in value["issues"]
            if row["issue_id"] == "MATH-AUDIT-P0-001"
        )
        issue["closure_checks"][0]["status"] = "pending"
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "p0_closed_without_passed_checks"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

    def test_audit_and_golden_hash_bindings_fail_closed(self) -> None:
        value = json.loads(MATRIX.read_text(encoding="utf-8"))
        value = copy.deepcopy(value)
        value["audit_register_sha256"]["math"] = "0" * 64
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "p0_audit_binding_invalid"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

        value = json.loads(MATRIX.read_text(encoding="utf-8"))
        value["golden_inventory_sha256"] = "0" * 64
        with tempfile.TemporaryDirectory(prefix="p0-gate-") as raw:
            path = Path(raw) / "matrix.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                gate.P0GateError, "golden_inventory_binding_invalid"
            ):
                gate.evaluate_gate(path, AUDIT_ROOT, GOLDEN)

    def test_open_gate_raises_before_callers_can_enter_model_lane(self) -> None:
        with tempfile.TemporaryDirectory(prefix="p0-open-") as raw:
            matrix_path = Path(raw) / "matrix.json"
            matrix_path.write_text(
                json.dumps(self.needs_user_matrix()), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                gate.P0GateError, "pre_model_p0_gate_blocked"
            ):
                gate.require_model_lane_ready(matrix_path, AUDIT_ROOT, GOLDEN)

    def test_remediation_gate_requires_complete_signed_v3_arguments(self) -> None:
        with self.assertRaisesRegex(
            gate.P0GateError, "en_p0_006_remediation_gate_args_incomplete"
        ):
            gate.evaluate_gate(
                MATRIX,
                AUDIT_ROOT,
                GOLDEN,
                english_legacy_authorization_expansion_receipt_root=Path(
                    "/tmp/not-consumed"
                ),
            )

    def test_closed_matrix_without_closure_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            gate.P0GateError, "en_p0_006_requires_verified_closure"
        ):
            gate.evaluate_gate(MATRIX, AUDIT_ROOT, GOLDEN)

    def test_verified_v3_execution_closure_closes_en_p0_006(self) -> None:
        with tempfile.TemporaryDirectory(prefix="p0-v3-closed-") as raw:
            matrix_path = Path(raw) / "matrix.json"
            matrix_path.write_text(
                json.dumps(self.closed_v3_matrix()), encoding="utf-8"
            )
            store = mock.Mock()
            store.reopen_verified_english_legacy_execution_closure.return_value = (
                self.verified_execution()
            )
            with mock.patch.object(
                gate, "SubjectSolRuntimeStore", return_value=store
            ):
                result = gate.evaluate_gate(
                    matrix_path,
                    AUDIT_ROOT,
                    GOLDEN,
                    english_legacy_execution_runtime_root=Path(raw),
                    english_legacy_execution_batch_id="EN-P0-006-BATCH-TEST",
                    english_legacy_execution_closure_sha256="b" * 64,
                )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["blocking_issue_ids"], [])
        self.assertEqual(
            result["english_legacy_execution_closure"]["status"],
            "verified_complete",
        )

    def test_zero_model_existing_inventory_closure_closes_en_p0_006(self) -> None:
        with tempfile.TemporaryDirectory(prefix="p0-existing-closed-") as raw:
            matrix_path = Path(raw) / "matrix.json"
            matrix_path.write_text(
                json.dumps(self.closed_existing_inventory_matrix()),
                encoding="utf-8",
            )
            store = mock.Mock()
            store.verify_existing_inventory_no_recuration_required.return_value = (
                self.verified_existing_inventory()
            )
            with mock.patch.object(
                gate, "EnglishLegacyDispositionV3Store", return_value=store
            ):
                result = gate.require_model_lane_ready(
                    matrix_path,
                    AUDIT_ROOT,
                    GOLDEN,
                    english_legacy_authorization_expansion_receipt_root=Path(raw),
                    english_legacy_authorization_expansion_closure_sha256=(
                        "c" * 64
                    ),
                    english_legacy_authorization_expansion_authority_key_path=(
                        Path(raw) / "authority.key"
                    ),
                )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["blocking_issue_ids"], [])
        self.assertEqual(
            result["english_legacy_existing_inventory_closure"]["outcome"],
            "verified_existing_inventory_no_recuration_required",
        )

    def test_complete_with_failures_cannot_close_en_p0_006(self) -> None:
        with tempfile.TemporaryDirectory(prefix="p0-v3-failed-") as raw:
            matrix_path = Path(raw) / "matrix.json"
            matrix_path.write_text(
                json.dumps(self.closed_v3_matrix()), encoding="utf-8"
            )
            incomplete = self.verified_execution()
            incomplete["status"] = "complete_with_failures"
            incomplete["closure"]["status"] = "complete_with_failures"
            store = mock.Mock()
            store.reopen_verified_english_legacy_execution_closure.return_value = (
                incomplete
            )
            with mock.patch.object(
                gate, "SubjectSolRuntimeStore", return_value=store
            ):
                with self.assertRaisesRegex(
                    gate.P0GateError,
                    "en_p0_006_execution_closure_not_complete",
                ):
                    gate.evaluate_gate(
                        matrix_path,
                        AUDIT_ROOT,
                        GOLDEN,
                        english_legacy_execution_runtime_root=Path(raw),
                        english_legacy_execution_batch_id=(
                            "EN-P0-006-BATCH-TEST"
                        ),
                        english_legacy_execution_closure_sha256="b" * 64,
                    )


if __name__ == "__main__":
    unittest.main()
