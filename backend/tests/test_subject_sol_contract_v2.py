from __future__ import annotations

import copy
import hashlib
import sys
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from subject_sol_contract import (  # noqa: E402
    _document_sha256,
    SubjectSolContractError,
    recompute_subject_luna_batch_v2,
    transition_subject_luna_task_v2,
    validate_daily_sol_batch_v3,
    validate_sol_task_handoff_envelope_v1,
    validate_subject_luna_batch_v2,
    SubjectSolRuntimeStore,
)


H = "a" * 64


def task(capture_id: str, *, status: str = "selected") -> dict[str, object]:
    return {
        "capture_id": capture_id,
        "unit_sha256": ("1" if capture_id.endswith("1") else "2") * 64,
        "input_fingerprint": ("3" if capture_id.endswith("1") else "4") * 64,
        "study_date": "2026-08-12",
        "frozen_payload_sha256": ("5" if capture_id.endswith("1") else "6") * 64,
        "status": status,
        "analysis_execution_receipt_sha256": None,
        "analysis_raw_output_sha256": None,
        "analysis_normalization_receipt_sha256": None,
        "analysis_report_sha256": None,
        "critical_review_execution_receipt_sha256": None,
        "critical_review_raw_output_sha256": None,
        "critical_review_normalization_receipt_sha256": None,
        "critical_review_report_sha256": None,
        "package_sha256": None,
        "sol_handoff_envelope_sha256": None,
        "warning_codes": [],
        "terminal_receipt_sha256": None,
        "error_code": None,
    }


def batch(tasks: list[dict[str, object]]) -> dict[str, object]:
    return recompute_subject_luna_batch_v2(
        {
            "schema_version": "subject_luna_batch_v2",
            "batch_id": "B-20260812",
            "subject": "math",
            "study_date": "2026-08-12",
            "status": "frozen",
            "capture_high_watermark": "MFI-CAP-HWM",
            "scan_snapshot_sha256": "7" * 64,
            "authority_generation": "math-generation-1",
            "authority_fingerprint": "8" * 64,
            "tasks": tasks,
            "exclusion_receipt_sha256s": [],
            "all_terminal": False,
            "sol_ready": False,
            "blocking_task_ids": [],
            "sol_candidate_task_ids": [],
            "diagnostic_task_ids": [],
            "formal_write_count": 0,
            "revision": 0,
            "updated_at": None,
        }
    )


def stage_updates(*, warnings: bool = False) -> dict[str, object]:
    values = {
        "analysis_execution_receipt_sha256": "9" * 64,
        "analysis_raw_output_sha256": "a" * 64,
        "analysis_normalization_receipt_sha256": "b" * 64,
        "analysis_report_sha256": "c" * 64,
        "critical_review_execution_receipt_sha256": "d" * 64,
        "critical_review_raw_output_sha256": "e" * 64,
        "critical_review_normalization_receipt_sha256": "f" * 64,
        "critical_review_report_sha256": "0" * 64,
        "package_sha256": "1" * 64,
        "sol_handoff_envelope_sha256": "2" * 64,
        "terminal_receipt_sha256": "3" * 64,
    }
    if warnings:
        values["warning_codes"] = ["duplicate_finding_removed"]
    return values


class SubjectSolV2Tests(unittest.TestCase):
    def test_archived_batch_successor_rebinds_authority_without_migration(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = SubjectSolRuntimeStore(Path(temporary) / "runtime")
            old = store.prepare_and_freeze_subject_batch(
                subject="math",
                batch_id="B-OLD-MATH",
                study_date="2026-08-14",
                capture_high_watermark="CAP-OLD",
                scan_snapshot_sha256="7" * 64,
                authority_generation="math-old",
                authority_fingerprint="8" * 64,
                tasks=[{
                    "capture_id": "CAP-OLD",
                    "unit_sha256": "1" * 64,
                    "input_fingerprint": "2" * 64,
                    "study_date": "2026-08-14",
                    "frozen_payload_sha256": "3" * 64,
                }],
            )
            store.record_terminal_failure(
                subject="math",
                batch_id=old["batch_id"],
                capture_id="CAP-OLD",
                unit_sha256="1" * 64,
                status="failed",
                error_code="quality_review_pending",
            )
            store.rollover_background_luna_batch(
                "math",
                mode="explicit_failure_resume",
                resume_acceptance_sha256="4" * 64,
            )
            archived = store.read_subject_batch("math")
            rows = [
                {
                    "capture_id": f"CAP-{index}",
                    "unit_sha256": str(index) * 64,
                    "input_fingerprint": str(index + 4) * 64,
                    "study_date": "2026-08-14",
                    "frozen_payload_sha256": chr(96 + index) * 64,
                }
                for index in range(1, 5)
            ]
            arguments = {
                "subject": "math",
                "batch_id": "B-NEW-MATH",
                "study_date": "2026-08-14",
                "capture_high_watermark": "CAP-NEW",
                "scan_snapshot_sha256": "9" * 64,
                "authority_generation": "math-new",
                "authority_fingerprint": "a" * 64,
                "tasks": rows,
            }
            archived_sha256 = _document_sha256(archived)
            prepared = store.prepare_and_freeze_subject_batch(**arguments)
            self.assertEqual(prepared["authority_generation"], "math-new")
            self.assertEqual(_document_sha256(archived), archived_sha256)
            writer = store._read_writer_locked("math")
            self.assertEqual(
                writer["generation_fence"],
                {
                    "blocked": False,
                    "source_generation": "math-old",
                    "next_generation": "math-new",
                },
            )

    def test_runtime_projection_ignores_duplicate_and_older_events(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = SubjectSolRuntimeStore(Path(temporary) / "runtime")
            prepared = store.prepare_and_freeze_subject_batch(
                subject="math",
                batch_id="B-MONOTONIC-V2",
                study_date="2026-08-12",
                capture_high_watermark="CAP-1",
                scan_snapshot_sha256=hashlib.sha256(b"scan").hexdigest(),
                authority_generation="math-generation-1",
                authority_fingerprint=hashlib.sha256(
                    b"authority"
                ).hexdigest(),
                tasks=[
                    {
                        "capture_id": "CAP-1",
                        "unit_sha256": "1" * 64,
                        "input_fingerprint": "3" * 64,
                        "study_date": "2026-08-12",
                        "frozen_payload_sha256": "5" * 64,
                    }
                ],
            )
            common = {
                "subject": "math",
                "batch_id": prepared["batch_id"],
                "capture_id": "CAP-1",
                "unit_sha256": "1" * 64,
            }
            store.record_task_progress(**common, status="claimed")
            latest = store.record_task_progress(
                **common, status="critical_review_running"
            )
            revision = latest["revision"]
            replayed = store.record_task_progress(
                **common, status="analysis_running"
            )
            self.assertEqual(
                replayed["tasks"][0]["status"], "critical_review_running"
            )
            self.assertEqual(replayed["revision"], revision)

    def test_failed_v2_rollover_keeps_v1_audit_counts_and_resume_binding(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = SubjectSolRuntimeStore(Path(temporary) / "runtime")
            prepared = store.prepare_and_freeze_subject_batch(
                subject="english",
                batch_id="B-FAILED-V2",
                study_date="2026-08-12",
                capture_high_watermark="CAP-1",
                scan_snapshot_sha256=hashlib.sha256(b"scan").hexdigest(),
                authority_generation="english-generation-1",
                authority_fingerprint=hashlib.sha256(
                    b"authority"
                ).hexdigest(),
                tasks=[
                    {
                        "capture_id": "CAP-1",
                        "unit_sha256": "1" * 64,
                        "input_fingerprint": "3" * 64,
                        "study_date": "2026-08-12",
                        "frozen_payload_sha256": "5" * 64,
                    }
                ],
            )
            store.record_terminal_failure(
                subject="english",
                batch_id=prepared["batch_id"],
                capture_id="CAP-1",
                unit_sha256="1" * 64,
                status="failed",
                error_code="provider_process_failed",
            )
            with self.assertRaisesRegex(
                SubjectSolContractError, "resume_acceptance_sha256_invalid"
            ):
                store.rollover_background_luna_batch(
                    "english", mode="explicit_failure_resume"
                )
            result = store.rollover_background_luna_batch(
                "english",
                mode="explicit_failure_resume",
                resume_acceptance_sha256=hashlib.sha256(
                    b"explicit-resume"
                ).hexdigest(),
            )
            self.assertEqual(
                result["receipt"]["terminal_status_counts"],
                {
                    "quality_passed": 0,
                    "needs_rework": 0,
                    "failed": 1,
                    "evidence_pending": 0,
                },
            )

    def test_failed_math_rollover_is_ready_for_same_authority_canary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = SubjectSolRuntimeStore(Path(temporary) / "runtime")
            fingerprint = hashlib.sha256(b"math-authority").hexdigest()
            prepared = store.prepare_and_freeze_subject_batch(
                subject="math",
                batch_id="B-FAILED-MATH",
                study_date="2026-08-15",
                capture_high_watermark="CAP-1",
                scan_snapshot_sha256=hashlib.sha256(b"scan").hexdigest(),
                authority_generation="math-generation-1",
                authority_fingerprint=fingerprint,
                tasks=[
                    {
                        "capture_id": "CAP-1",
                        "unit_sha256": "1" * 64,
                        "input_fingerprint": "3" * 64,
                        "study_date": "2026-08-15",
                        "frozen_payload_sha256": "5" * 64,
                    }
                ],
            )
            store.record_terminal_failure(
                subject="math",
                batch_id=prepared["batch_id"],
                capture_id="CAP-1",
                unit_sha256="1" * 64,
                status="failed",
                error_code="math_migration_claim_evidence_invalid",
            )
            store.rollover_background_luna_batch(
                "math",
                mode="explicit_failure_resume",
                resume_acceptance_sha256=hashlib.sha256(
                    b"exact-migration-descriptor"
                ).hexdigest(),
            )

            readiness = store.canary_readiness(
                "math",
                next_generation="math-generation-1",
                next_authority_fingerprint=fingerprint,
            )

            self.assertEqual(readiness["readiness"], "ready")
            self.assertEqual(
                readiness["reason"], "recovered_terminal_batch_archived"
            )
            self.assertEqual(readiness["formal_write_count"], 0)

            successor_fingerprint = hashlib.sha256(
                b"math-successor-authority"
            ).hexdigest()
            successor = store.canary_readiness(
                "math",
                next_generation="math-generation-2",
                next_authority_fingerprint=successor_fingerprint,
            )
            self.assertEqual(successor["readiness"], "ready")
            replaced = store.prepare_and_freeze_subject_batch(
                subject="math",
                batch_id="B-SUCCESSOR-MATH",
                study_date="2026-08-15",
                capture_high_watermark="CAP-2",
                scan_snapshot_sha256=hashlib.sha256(b"scan-2").hexdigest(),
                authority_generation="math-generation-2",
                authority_fingerprint=successor_fingerprint,
                tasks=[
                    {
                        "capture_id": "CAP-2",
                        "unit_sha256": "2" * 64,
                        "input_fingerprint": "4" * 64,
                        "study_date": "2026-08-15",
                        "frozen_payload_sha256": "6" * 64,
                    }
                ],
            )
            self.assertEqual(replaced["batch_id"], "B-SUCCESSOR-MATH")
            self.assertEqual(
                replaced["authority_generation"], "math-generation-2"
            )

    def test_good_task_is_candidate_while_failed_sibling_is_diagnostic(self) -> None:
        good = task("CAP-1", status="workflow_complete_with_warnings")
        good.update(stage_updates(warnings=True))
        failed = task("CAP-2", status="execution_failed")
        failed.update(
            terminal_receipt_sha256="4" * 64,
            error_code="provider_process_failed",
        )
        value = validate_subject_luna_batch_v2(batch([failed, good]))
        self.assertTrue(value["all_terminal"])
        self.assertTrue(value["sol_ready"])
        self.assertEqual(value["sol_candidate_task_ids"], ["CAP-1"])
        self.assertEqual(value["diagnostic_task_ids"], ["CAP-2"])
        self.assertEqual(value["blocking_task_ids"], [])

    def test_external_pending_is_not_a_batch_member_or_terminal_blocker(self) -> None:
        complete = task("CAP-1", status="workflow_complete")
        complete.update(stage_updates())
        value = validate_subject_luna_batch_v2(batch([complete]))
        self.assertTrue(value["all_terminal"])
        self.assertTrue(value["sol_ready"])
        self.assertNotIn("outside_batch_pending_task_count", value)

    def test_forward_transition_and_terminal_immutability(self) -> None:
        value = validate_subject_luna_batch_v2(batch([task("CAP-1")]))
        value = transition_subject_luna_task_v2(
            value,
            capture_id="CAP-1",
            unit_sha256="1" * 64,
            status="claimed",
        )
        value = transition_subject_luna_task_v2(
            value,
            capture_id="CAP-1",
            unit_sha256="1" * 64,
            status="analysis_running",
        )
        self.assertEqual(value["tasks"][0]["status"], "analysis_running")
        with self.assertRaisesRegex(
            SubjectSolContractError, "subject_luna_task_v2_state_regression"
        ):
            transition_subject_luna_task_v2(
                value,
                capture_id="CAP-1",
                unit_sha256="1" * 64,
                status="claimed",
            )

        failed = transition_subject_luna_task_v2(
            value,
            capture_id="CAP-1",
            unit_sha256="1" * 64,
            status="execution_failed",
            updates={
                "terminal_receipt_sha256": "4" * 64,
                "error_code": "analysis_execution_failed",
            },
        )
        with self.assertRaisesRegex(
            SubjectSolContractError, "subject_luna_task_v2_terminal_regression"
        ):
            transition_subject_luna_task_v2(
                failed,
                capture_id="CAP-1",
                unit_sha256="1" * 64,
                status="analysis_running",
            )

    def test_warning_completion_requires_warning_and_handoff(self) -> None:
        invalid = task("CAP-1", status="workflow_complete_with_warnings")
        invalid.update(stage_updates())
        value = batch([task("CAP-1")])
        value["tasks"] = [invalid]
        with self.assertRaisesRegex(
            SubjectSolContractError,
            "subject_luna_task_v2_warning_complete_binding_invalid",
        ):
            validate_subject_luna_batch_v2(value)

    def test_task_set_is_sorted_and_duplicate_identity_fails(self) -> None:
        value = batch([task("CAP-2"), task("CAP-1")])
        self.assertEqual(
            [row["capture_id"] for row in value["tasks"]],
            ["CAP-1", "CAP-2"],
        )
        duplicate = copy.deepcopy(value)
        duplicate["tasks"][1]["capture_id"] = "CAP-1"
        with self.assertRaises(SubjectSolContractError):
            validate_subject_luna_batch_v2(duplicate)

    def test_daily_sol_v3_binds_only_candidate_handoff(self) -> None:
        candidate = task("CAP-1", status="workflow_complete_with_warnings")
        candidate.update(stage_updates(warnings=True))
        diagnostic = task("CAP-2", status="execution_failed")
        diagnostic.update(
            terminal_receipt_sha256="4" * 64,
            error_code="provider_process_failed",
        )
        luna = validate_subject_luna_batch_v2(batch([candidate, diagnostic]))
        handoff = {
            "schema_version": "sol_task_handoff_envelope_v1",
            "batch_id": luna["batch_id"],
            "subject": "math",
            "capture_id": "CAP-1",
            "unit_sha256": "1" * 64,
            "input_fingerprint": "3" * 64,
            "study_date": "2026-08-12",
            "frozen_payload_sha256": "5" * 64,
            "authority_snapshot_sha256": "6" * 64,
            "analysis": {
                "raw_output_sha256": "a" * 64,
                "execution_receipt_sha256": "9" * 64,
                "normalization_receipt_sha256": "b" * 64,
                "report_sha256": "c" * 64,
                "warning_codes": [],
            },
            "critical_review": {
                "raw_output_sha256": "e" * 64,
                "execution_receipt_sha256": "d" * 64,
                "normalization_receipt_sha256": "f" * 64,
                "report_sha256": "0" * 64,
                "warning_codes": ["duplicate_finding_removed"],
            },
            "package_sha256": "1" * 64,
            "terminal_receipt_sha256": "3" * 64,
            "warning_codes": ["duplicate_finding_removed"],
            "execution_status": "workflow_complete_with_warnings",
            "formal_write_count": 0,
        }
        handoff = validate_sol_task_handoff_envelope_v1(handoff)
        from subject_sol_contract import _document_sha256  # noqa: PLC0415

        handoff_sha = _document_sha256(handoff)
        event = {
            "event_id": "AUTH-1",
            "event_type": "explicit_user_sol_authorization",
            "subject": "math",
            "study_date": "2026-08-12",
            "subject_luna_batch_id": luna["batch_id"],
            "authorized_at": "2026-08-12T12:00:00Z",
        }
        authorization_core = {
            "schema_version": "user_sol_authorization_receipt_v2",
            "sol_batch_id": "SOL-1",
            "subject": "math",
            "study_date": "2026-08-12",
            "subject_luna_batch_id": luna["batch_id"],
            "subject_luna_batch_sha256": _document_sha256(luna),
            "sol_candidate_task_ids": ["CAP-1"],
            "sol_handoff_envelope_sha256s": [handoff_sha],
            "diagnostic_task_ids": ["CAP-2"],
            "writer_adapter": "math_nightly_writer_v1",
            "idempotency_key": "SOL-IDEMPOTENCY-1",
            "event": event,
            "formal_write_count": 0,
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": "user-sol-authorization-receipt-v2",
                "hmac_sha256": "7" * 64,
            },
        }
        sol_batch = {
            "schema_version": "daily_sol_batch_v3",
            "batch_id": "SOL-1",
            "subject": "math",
            "study_date": "2026-08-12",
            "subject_luna_batch": luna,
            "subject_luna_batch_sha256": _document_sha256(luna),
            "sol_candidate_task_ids": ["CAP-1"],
            "sol_handoff_envelope_sha256s": [handoff_sha],
            "diagnostic_task_ids": ["CAP-2"],
            "authorization_receipt": authorization_core,
            "authorization_receipt_sha256": _document_sha256(authorization_core),
            "writer_adapter": "math_nightly_writer_v1",
            "idempotency_key": "SOL-IDEMPOTENCY-1",
            "status": "authorized",
            "formal_write_count": 0,
        }
        checked = validate_daily_sol_batch_v3(sol_batch)
        self.assertEqual(checked["sol_candidate_task_ids"], ["CAP-1"])
        self.assertEqual(checked["diagnostic_task_ids"], ["CAP-2"])


if __name__ == "__main__":
    unittest.main()
