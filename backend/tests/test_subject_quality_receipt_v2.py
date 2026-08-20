from __future__ import annotations

import copy
import hashlib
import sys
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import subject_sol_contract as contract  # noqa: E402


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class SubjectQualityReceiptV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.store = contract.SubjectSolRuntimeStore(self.runtime)
        self.frozen = {
            "subject": "math",
            "capture_id": "MATH-CAP-V2",
            "study_date": "2026-08-12",
            "input_fingerprint": digest("input"),
        }
        self.task = {
            "capture_id": self.frozen["capture_id"],
            "unit_sha256": digest("unit"),
            "input_fingerprint": self.frozen["input_fingerprint"],
            "study_date": self.frozen["study_date"],
            "frozen_payload_sha256": contract._value_sha256(self.frozen),
        }
        self.store.prepare_and_freeze_subject_batch(
            subject="math",
            batch_id="MATH-BATCH-V2",
            study_date="2026-08-12",
            capture_high_watermark="MATH-CAP-V2",
            authority_generation="math-generation-v2",
            authority_fingerprint=digest("authority"),
            tasks=[self.task],
            scan_snapshot_sha256=digest("scan"),
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _stage(
        self, stage_name: str, *, missing_normalization: bool = False
    ) -> dict[str, object]:
        return {
            "stage_execution_receipt_sha256": digest(
                f"{stage_name}:execution"
            ),
            "raw_output_object_sha256": digest(f"{stage_name}:raw"),
            "stage_normalization_receipt_sha256": (
                None
                if missing_normalization
                else digest(f"{stage_name}:normalization")
            ),
            "normalization_status": (
                "normalized_with_warnings"
                if missing_normalization
                else "normalized"
            ),
            "normalization_warnings": [],
            "mcp_transcript_sha256": digest(f"{stage_name}:transcript"),
            "capture_freeze_receipt_sha256": digest("capture-freeze"),
            "mcp_read_session_receipt_sha256": digest("read-session"),
            "read_session_manifest_sha256": digest("authority-snapshot"),
            "read_session_id": "READ-MATH-V2",
            "evidence_generation": "math-generation-v2",
            "evidence_authority_fingerprint": digest("authority"),
        }

    def _verified(
        self,
        outcome: str,
        *,
        warning: bool = False,
    ) -> dict[str, object]:
        analysis = self._stage(
            "analysis", missing_normalization=warning
        )
        review = self._stage("critical_review")
        if outcome == "failed":
            analysis = {}
            review = {}
        elif outcome == "needs_rework":
            review = {}
        package = None
        package_sha = None
        if outcome == "succeeded":
            package = {
                "task": {"frozen_payload": copy.deepcopy(self.frozen)},
                "analysis": {"stage": "analysis", "report": "ok"},
                "critical_review": {
                    "stage": "critical_review",
                    "report": "ok",
                },
            }
            package_sha = digest("dispatch-package")
        return {
            "latest": {
                "subject": "math",
                "unit_sha256": self.task["unit_sha256"],
                "completion_sha256": digest("completion"),
            },
            "completion": {
                "subject": "math",
                "capture_id": self.task["capture_id"],
                "unit_sha256": self.task["unit_sha256"],
                "outcome": outcome,
                "error_code": (
                    None if outcome == "succeeded" else f"{outcome}_error"
                ),
                "receipt_sha256": digest("dispatch-receipt"),
                "package_sha256": package_sha,
                "finished_at": "2026-08-12T12:00:00Z",
            },
            "receipt": {
                "observed_stage_runtime": {
                    "analysis": analysis,
                    "critical_review": review,
                }
            },
            "package": package,
        }

    def test_success_and_warning_publish_strict_reopenable_v2_receipts(self) -> None:
        for warning in (False, True):
            with self.subTest(warning=warning):
                if warning:
                    self.tearDown()
                    self.setUp()
                result = self.store.record_verified_luna_completion(
                    "math", self._verified("succeeded", warning=warning)
                )
                receipt_sha = result["subject_quality_receipt_sha256"]
                self.assertIsInstance(receipt_sha, str)
                task = result["subject_luna_batch"]["tasks"][0]
                reopened = self.store.read_verified_quality_receipt_v2(
                    receipt_sha, subject="math", task=task
                )
                expected_status = (
                    "workflow_complete_with_warnings"
                    if warning
                    else "workflow_complete"
                )
                self.assertEqual(reopened["execution_status"], expected_status)
                self.assertEqual(reopened["model_call_count"], 2)
                self.assertEqual(reopened["formal_write_count"], 0)
                self.assertEqual(
                    reopened["capture_freeze_receipt_sha256"],
                    digest("capture-freeze"),
                )
                self.assertEqual(
                    reopened["mcp_read_session_receipt_sha256"],
                    digest("read-session"),
                )
                contract.validate_subject_quality_receipt_v2(reopened)
                if not warning:
                    from tests.test_successor_schema_roundtrip import (  # noqa: PLC0415
                        assert_positive_and_negatives,
                    )

                    assert_positive_and_negatives(
                        self,
                        schema_name="subject-quality-receipt-v2.json",
                        value=reopened,
                        missing_key="analysis",
                        wrong_type_key="model_call_count",
                        pseudo_sha_path=("critical_review", "mcp_transcript_sha256"),
                        authority_purpose_path=("seal", "purpose"),
                    )

    def test_partial_and_failed_are_terminal_diagnostics_without_quality(self) -> None:
        cases = (
            ("needs_rework", "workflow_partial"),
            ("failed", "execution_failed"),
        )
        for index, (outcome, expected_status) in enumerate(cases):
            with self.subTest(outcome=outcome):
                if index:
                    self.tearDown()
                    self.setUp()
                result = self.store.record_verified_luna_completion(
                    "math", self._verified(outcome)
                )
                self.assertIsNone(result["subject_quality_receipt_sha256"])
                task = result["subject_luna_batch"]["tasks"][0]
                self.assertEqual(task["status"], expected_status)
                self.assertIsNone(task["sol_handoff_envelope_sha256"])
                self.assertEqual(result["formal_write_count"], 0)

    def test_wrong_transcript_and_wrong_warning_union_fail_closed(self) -> None:
        core = {
            "batch_id": "MATH-BATCH-V2",
            "subject": "math",
            "capture_id": self.task["capture_id"],
            "unit_sha256": self.task["unit_sha256"],
            "frozen_payload_sha256": self.task["frozen_payload_sha256"],
            "completion_sha256": digest("completion"),
            "dispatch_receipt_sha256": digest("dispatch-receipt"),
            "dispatch_package_sha256": digest("dispatch-package"),
            "capture_freeze_receipt_sha256": digest("capture-freeze"),
            "mcp_read_session_receipt_sha256": digest("read-session"),
            "authority_snapshot_sha256": digest("snapshot"),
            "analysis": {
                "raw_output_sha256": digest("analysis-raw"),
                "execution_receipt_sha256": digest("analysis-execution"),
                "normalization_receipt_sha256": digest("analysis-normalization"),
                "report_sha256": digest("analysis-report"),
                "mcp_transcript_sha256": digest("same-transcript"),
                "warning_codes": [],
            },
            "critical_review": {
                "raw_output_sha256": digest("review-raw"),
                "execution_receipt_sha256": digest("review-execution"),
                "normalization_receipt_sha256": digest("review-normalization"),
                "report_sha256": digest("review-report"),
                "mcp_transcript_sha256": digest("same-transcript"),
                "warning_codes": [],
            },
            "execution_status": "workflow_complete",
            "warning_codes": [],
            "package_sha256": digest("package"),
            "sol_handoff_envelope_sha256": digest("handoff"),
            "model_call_count": 2,
            "formal_write_count": 0,
            "issued_at": "2026-08-12T12:00:00Z",
        }
        with self.assertRaisesRegex(
            contract.SubjectSolContractError,
            "quality_stage_artifacts_not_distinct",
        ):
            self.store._issue_quality_receipt_v2(core)
        core["critical_review"]["mcp_transcript_sha256"] = digest(
            "review-transcript"
        )
        core["execution_status"] = "workflow_complete_with_warnings"
        core["warning_codes"] = ["unbound-warning"]
        with self.assertRaisesRegex(
            contract.SubjectSolContractError,
            "subject_quality_warning_binding_invalid",
        ):
            self.store._issue_quality_receipt_v2(core)


if __name__ == "__main__":
    unittest.main()
