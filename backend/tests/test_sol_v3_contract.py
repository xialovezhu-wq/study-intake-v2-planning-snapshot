from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import subject_sol_contract as contract  # noqa: E402
from isolated_authority_publishers import (  # noqa: E402
    IndependentUserIntentPublisher,
    IsolatedWriterAdapterPublisher,
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def seal(purpose: str) -> dict:
    return {
        "algorithm": "HMAC-SHA256",
        "purpose": purpose,
        "hmac_sha256": digest(purpose),
    }


class SolV3ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.store = contract.SubjectSolRuntimeStore(self.runtime)
        self.envelope = {
            "schema_version": contract.SOL_TASK_HANDOFF_SCHEMA,
            "batch_id": "MATH-BATCH-V2",
            "subject": "math",
            "capture_id": "MATH-CAP-1",
            "unit_sha256": digest("unit"),
            "input_fingerprint": digest("input"),
            "study_date": "2026-08-12",
            "frozen_payload_sha256": digest("frozen"),
            "authority_snapshot_sha256": digest("snapshot"),
            "analysis": {
                "raw_output_sha256": digest("analysis-raw"),
                "execution_receipt_sha256": digest("analysis-execution"),
                "normalization_receipt_sha256": digest("analysis-normalization"),
                "report_sha256": digest("analysis-report"),
                "warning_codes": [],
            },
            "critical_review": {
                "raw_output_sha256": digest("review-raw"),
                "execution_receipt_sha256": digest("review-execution"),
                "normalization_receipt_sha256": digest("review-normalization"),
                "report_sha256": digest("review-report"),
                "warning_codes": [],
            },
            "package_sha256": digest("package"),
            "terminal_receipt_sha256": digest("terminal"),
            "warning_codes": [],
            "execution_status": "workflow_complete",
            "formal_write_count": 0,
        }
        self.handoff_sha, _ = self.store._publish_immutable(
            self.store.sol_task_handoff_root,
            contract.validate_sol_task_handoff_envelope_v1(self.envelope),
        )
        task = {
            "capture_id": self.envelope["capture_id"],
            "unit_sha256": self.envelope["unit_sha256"],
            "input_fingerprint": self.envelope["input_fingerprint"],
            "study_date": self.envelope["study_date"],
            "frozen_payload_sha256": self.envelope["frozen_payload_sha256"],
            "status": "workflow_complete",
            "analysis_execution_receipt_sha256": self.envelope["analysis"]["execution_receipt_sha256"],
            "analysis_raw_output_sha256": self.envelope["analysis"]["raw_output_sha256"],
            "analysis_normalization_receipt_sha256": self.envelope["analysis"]["normalization_receipt_sha256"],
            "analysis_report_sha256": self.envelope["analysis"]["report_sha256"],
            "critical_review_execution_receipt_sha256": self.envelope["critical_review"]["execution_receipt_sha256"],
            "critical_review_raw_output_sha256": self.envelope["critical_review"]["raw_output_sha256"],
            "critical_review_normalization_receipt_sha256": self.envelope["critical_review"]["normalization_receipt_sha256"],
            "critical_review_report_sha256": self.envelope["critical_review"]["report_sha256"],
            "package_sha256": self.envelope["package_sha256"],
            "sol_handoff_envelope_sha256": self.handoff_sha,
            "terminal_receipt_sha256": self.envelope["terminal_receipt_sha256"],
            "warning_codes": [],
            "error_code": None,
        }
        raw_luna = {
            "schema_version": contract.SUBJECT_BATCH_V2_SCHEMA,
            "batch_id": self.envelope["batch_id"],
            "subject": "math",
            "study_date": "2026-08-12",
            "status": "frozen",
            "capture_high_watermark": "MATH-CAP-1",
            "scan_snapshot_sha256": digest("scan"),
            "authority_generation": "math-generation-v2",
            "authority_fingerprint": digest("authority"),
            "tasks": [task],
            "exclusion_receipt_sha256s": [],
            "all_terminal": True,
            "sol_ready": True,
            "blocking_task_ids": [],
            "sol_candidate_task_ids": ["MATH-CAP-1"],
            "diagnostic_task_ids": [],
            "formal_write_count": 0,
            "revision": 2,
            "updated_at": "2026-08-12T10:00:00Z",
        }
        self.luna = contract.validate_subject_luna_batch_v2(raw_luna)
        self.authorization = {
            "schema_version": contract.USER_SOL_AUTHORIZATION_V2_SCHEMA,
            "sol_batch_id": "SOL-MATH-V3",
            "subject": "math",
            "study_date": "2026-08-12",
            "subject_luna_batch_id": self.luna["batch_id"],
            "subject_luna_batch_sha256": contract._document_sha256(self.luna),
            "sol_candidate_task_ids": ["MATH-CAP-1"],
            "sol_handoff_envelope_sha256s": [self.handoff_sha],
            "diagnostic_task_ids": [],
            "writer_adapter": "math_nightly_writer_v1",
            "idempotency_key": "sol-math-v3",
            "event": {
                "event_id": "AUTH-MATH-V3",
                "event_type": "explicit_user_sol_authorization",
                "subject": "math",
                "study_date": "2026-08-12",
                "subject_luna_batch_id": self.luna["batch_id"],
                "authorized_at": "2026-08-12T10:05:00Z",
            },
            "formal_write_count": 0,
            "seal": seal("user-sol-authorization-receipt-v2"),
        }
        self.batch = {
            "schema_version": contract.DAILY_SOL_BATCH_V3_SCHEMA,
            "batch_id": "SOL-MATH-V3",
            "subject": "math",
            "study_date": "2026-08-12",
            "subject_luna_batch": self.luna,
            "subject_luna_batch_sha256": contract._document_sha256(self.luna),
            "sol_candidate_task_ids": ["MATH-CAP-1"],
            "sol_handoff_envelope_sha256s": [self.handoff_sha],
            "diagnostic_task_ids": [],
            "authorization_receipt": self.authorization,
            "authorization_receipt_sha256": contract._document_sha256(self.authorization),
            "writer_adapter": "math_nightly_writer_v1",
            "idempotency_key": "sol-math-v3",
            "status": "authorized",
            "formal_write_count": 0,
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def dispatcher_effect(self) -> dict:
        return {
            "scope": "subject",
            "subject": "math",
            "paused_subjects": [],
            "drained_subjects": [],
            "other_subjects_unchanged": True,
        }

    def review(self) -> dict:
        return {
            "schema_version": contract.SOL_REVIEW_RECEIPT_V2_SCHEMA,
            "batch_id": self.batch["batch_id"],
            "daily_sol_batch_sha256": contract._document_sha256(self.batch),
            "subject": "math",
            "fencing_token": 7,
            "writer": "sol",
            "writer_adapter": "math_nightly_writer_v1",
            "canonical_evidence_sha256": digest("canonical-evidence"),
            "decisions": [{
                "capture_id": "MATH-CAP-1",
                "unit_sha256": self.envelope["unit_sha256"],
                "sol_handoff_envelope_sha256": self.handoff_sha,
                "decision": "adopted",
                "replacement_proposal_sha256": None,
                "replacement_package_sha256": None,
                "reason": "independently reviewed",
            }],
            "status": "approved",
            "dispatcher_effect": self.dispatcher_effect(),
            "formal_write_count": 0,
            "completed_at": "2026-08-12T10:10:00Z",
            "seal": seal("writer-adapter-sol-review-receipt-v2"),
        }

    def commit(self) -> dict:
        return {
            "schema_version": contract.SOL_COMMIT_RECEIPT_V2_SCHEMA,
            "batch_id": self.batch["batch_id"],
            "daily_sol_batch_sha256": contract._document_sha256(self.batch),
            "subject": "math",
            "fencing_token": 7,
            "writer": "sol",
            "writer_adapter": "math_nightly_writer_v1",
            "sol_candidate_task_ids": ["MATH-CAP-1"],
            "sol_handoff_envelope_sha256s": [self.handoff_sha],
            "execution_result_sha256": digest("execution-result"),
            "sol_review_receipt_sha256": digest("review-receipt"),
            "pre_state_sha256": digest("pre-state"),
            "post_state_sha256": digest("post-state"),
            "operations": [{"operation_id": "op-1"}],
            "writer_process": {
                "adapter_run_id": "run-1",
                "pid": 101,
                "terminal_state": "stopped",
                "exit_code": 0,
                "stopped_at": "2026-08-12T10:11:00Z",
                "evidence_sha256": digest("process"),
            },
            "transaction_end": {
                "transaction_id": "tx-1",
                "state": "committed",
                "ended_at": "2026-08-12T10:11:00Z",
                "evidence_sha256": digest("transaction"),
            },
            "recovery_position_sha256": None,
            "rollback_receipt_sha256": None,
            "status": "committed",
            "dispatcher_effect": self.dispatcher_effect(),
            "formal_write_count": 1,
            "completed_at": "2026-08-12T10:12:00Z",
            "seal": seal("deterministic-writer-apply-receipt-v2"),
        }

    def activate_batch_for_review(self) -> dict:
        authorization_sha, _ = self.store._publish_immutable(
            self.store.user_sol_authorization_root,
            self.authorization,
        )
        self.assertEqual(
            authorization_sha,
            self.batch["authorization_receipt_sha256"],
        )
        with contract._FileLock(self.store._subject_lock_path("math")):
            self.store._write_batch_locked(self.luna, preadvanced=True)
            writer = self.store._read_writer_locked("math")
            writer.update(
                {
                    "handoff_status": "ready_for_authorization",
                    "batch_id": self.luna["batch_id"],
                }
            )
            self.store._write_writer_locked(writer)
        with mock.patch.object(self.store, "_verify_external_seal"):
            self.store.authorize_batch("math", self.batch)
            return self.store.begin_sol_review(
                "math", self.batch["batch_id"], owner_id="sol-v3-test"
            )

    def activate_real_published_batch_for_review(self) -> tuple[dict, dict]:
        with contract._FileLock(self.store._subject_lock_path("math")):
            self.store._write_batch_locked(self.luna, preadvanced=True)
            writer = self.store._read_writer_locked("math")
            writer.update(
                {
                    "handoff_status": "ready_for_authorization",
                    "batch_id": self.luna["batch_id"],
                }
            )
            self.store._write_writer_locked(writer)
        authorization_sha, _ = IndependentUserIntentPublisher(
            self.runtime
        ).publish_sol_authorization(
            luna_batch=self.luna,
            sol_batch_id="SOL-MATH-V3-PUBLISHED",
            event_id="AUTH-MATH-V3-PUBLISHED",
            authorized_at="2026-08-12T10:05:00Z",
            idempotency_key="sol-math-v3-published",
        )
        batch = self.store.build_authorized_batch(
            "math",
            sol_batch_id="SOL-MATH-V3-PUBLISHED",
            authorization_receipt_sha256=authorization_sha,
        )
        self.store.authorize_batch("math", batch)
        claim = self.store.begin_sol_review(
            "math", batch["batch_id"], owner_id="sol-v3-published-test"
        )
        return batch, claim

    def publish_real_review(self, batch: dict, claim: dict) -> tuple[str, dict]:
        core = self.review()
        core.pop("schema_version")
        core.pop("seal")
        core.update(
            {
                "batch_id": batch["batch_id"],
                "daily_sol_batch_sha256": contract._document_sha256(batch),
                "fencing_token": claim["fencing_token"],
            }
        )
        root = Path(self.temp.name) / "v2-review-result"
        root.mkdir()
        (root / "review.json").write_text(
            json.dumps(core, sort_keys=True) + "\n", encoding="utf-8"
        )
        review_sha, review_path = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_review_from_result_directory(root, batch=batch)
        receipt = json.loads(review_path.read_text(encoding="utf-8"))
        return review_sha, receipt

    def publish_real_zero_write_commit(
        self, batch: dict, claim: dict, review_sha: str
    ) -> tuple[str, dict]:
        root = Path(self.temp.name) / "v2-apply-result"
        root.mkdir()
        effect = self.dispatcher_effect()
        rows = {
            "writer-process.json": {
                "batch_id": batch["batch_id"],
                "subject": "math",
                "fencing_token": claim["fencing_token"],
                "adapter_run_id": "v2-zero-write-run",
                "pid": 101,
                "terminal_state": "stopped",
                "exit_code": 0,
                "stopped_at": "2026-08-12T10:11:01Z",
                "formal_write_count": 0,
            },
            "transaction-end.json": {
                "batch_id": batch["batch_id"],
                "subject": "math",
                "fencing_token": claim["fencing_token"],
                "transaction_id": "v2-zero-write-transaction",
                "state": "already_current",
                "ended_at": "2026-08-12T10:11:00Z",
                "formal_write_count": 0,
            },
            "operations.json": {
                "batch_id": batch["batch_id"],
                "subject": "math",
                "fencing_token": claim["fencing_token"],
                "pre_state_sha256": digest("zero-write-state"),
                "post_state_sha256": digest("zero-write-state"),
                "operations": [],
                "formal_write_count": 0,
            },
            "result.json": {
                "batch_id": batch["batch_id"],
                "subject": "math",
                "fencing_token": claim["fencing_token"],
                "writer": "sol",
                "writer_adapter": "math_nightly_writer_v1",
                "sol_review_receipt_sha256": review_sha,
                "status": "already_current",
                "dispatcher_effect": effect,
                "completed_at": "2026-08-12T10:11:02Z",
            },
        }
        for name, value in rows.items():
            (root / name).write_text(
                json.dumps(value, sort_keys=True) + "\n", encoding="utf-8"
            )
        commit_sha, commit_path = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_apply_from_execution_result_directory(root, batch=batch)
        receipt = json.loads(commit_path.read_text(encoding="utf-8"))
        return commit_sha, receipt

    def test_daily_v3_reopens_exact_content_addressed_handoff(self) -> None:
        with mock.patch.object(self.store, "_verify_external_seal"):
            reopened = self.store.read_verified_daily_sol_batch_v3(self.batch)
        self.assertEqual(reopened, self.batch)

    def test_daily_v3_fails_closed_when_handoff_is_missing(self) -> None:
        path = (
            self.store.sol_task_handoff_root
            / "sha256"
            / self.handoff_sha[:2]
            / f"{self.handoff_sha}.json"
        )
        path.unlink()
        with mock.patch.object(self.store, "_verify_external_seal"):
            with self.assertRaisesRegex(
                contract.SubjectSolContractError,
                "sol_task_handoff_envelope_missing",
            ):
                self.store.read_verified_daily_sol_batch_v3(self.batch)

    def test_review_v2_binds_exact_candidate_identity_and_handoff(self) -> None:
        checked = contract.validate_sol_review_receipt_v2(
            self.review(), batch=self.batch
        )
        self.assertEqual(checked["status"], "approved")
        stale = self.review()
        stale["decisions"][0]["unit_sha256"] = digest("other-unit")
        with self.assertRaisesRegex(
            contract.SubjectSolContractError, "sol_review_batch_binding_invalid"
        ):
            contract.validate_sol_review_receipt_v2(stale, batch=self.batch)

    def test_review_v2_modified_decision_requires_both_replacements(self) -> None:
        value = self.review()
        value["decisions"][0]["decision"] = "modified"
        value["decisions"][0]["replacement_proposal_sha256"] = digest("replacement")
        with self.assertRaisesRegex(
            contract.SubjectSolContractError,
            "sol_review_replacement_binding_invalid",
        ):
            contract.validate_sol_review_receipt_v2(value, batch=self.batch)

    def test_commit_v2_binds_daily_batch_candidates_and_handoffs(self) -> None:
        checked = contract.validate_sol_commit_receipt_v2(
            self.commit(), batch=self.batch
        )
        self.assertEqual(checked["formal_write_count"], 1)
        stale = self.commit()
        stale["sol_handoff_envelope_sha256s"] = [digest("other-handoff")]
        with self.assertRaisesRegex(
            contract.SubjectSolContractError, "sol_commit_batch_binding_invalid"
        ):
            contract.validate_sol_commit_receipt_v2(stale, batch=self.batch)

    def test_commit_v2_noncommit_cannot_claim_formal_write(self) -> None:
        value = self.commit()
        value["status"] = "already_current"
        value["transaction_end"]["state"] = "already_current"
        with self.assertRaisesRegex(
            contract.SubjectSolContractError,
            "noncommit_formal_write_count_nonzero",
        ):
            contract.validate_sol_commit_receipt_v2(value, batch=self.batch)

    def test_review_v2_is_reopened_by_real_control_plane_path(self) -> None:
        claim = self.activate_batch_for_review()
        self.assertEqual(claim["global"]["active_writer"]["remaining_count"], 1)
        review = self.review()
        review["fencing_token"] = claim["fencing_token"]
        review_sha, _ = self.store._publish_immutable(
            self.store.writer_review_receipt_root,
            review,
        )
        with mock.patch.object(self.store, "_verify_external_seal"):
            result = self.store.record_sol_review("math", review_sha)
        self.assertEqual(result["global"]["active_writer"]["status"], "applying")
        self.assertEqual(result["review_receipt_sha256"], review_sha)

    def test_commit_v2_reopens_execution_result_and_fails_closed_if_missing(self) -> None:
        claim = self.activate_batch_for_review()
        review = self.review()
        review["fencing_token"] = claim["fencing_token"]
        review_sha, _ = self.store._publish_immutable(
            self.store.writer_review_receipt_root,
            review,
        )
        with mock.patch.object(self.store, "_verify_external_seal"):
            self.store.record_sol_review("math", review_sha)
        commit = self.commit()
        commit["fencing_token"] = claim["fencing_token"]
        commit["sol_review_receipt_sha256"] = review_sha
        commit_sha, _ = self.store._publish_immutable(
            self.store.writer_apply_receipt_root,
            commit,
        )
        with mock.patch.object(self.store, "_verify_external_seal"):
            with self.assertRaisesRegex(
                contract.SubjectSolContractError,
                "isolated_writer_execution_result_missing",
            ):
                self.store.finish_subject_commit("math", commit_sha)
        self.assertEqual(
            self.store.read_global()["active_writer"]["status"],
            "safe_paused",
        )

    def test_real_v2_intent_publisher_binds_exact_candidate_handoff_set(self) -> None:
        batch, claim = self.activate_real_published_batch_for_review()
        authorization = batch["authorization_receipt"]
        checked = contract.validate_user_sol_authorization_receipt_v2(
            authorization
        )
        self.assertEqual(checked["sol_candidate_task_ids"], ["MATH-CAP-1"])
        self.assertEqual(
            checked["sol_handoff_envelope_sha256s"], [self.handoff_sha]
        )
        self.assertEqual(checked["diagnostic_task_ids"], [])
        self.assertEqual(claim["formal_write_count"], 0)
        from tests.test_successor_schema_roundtrip import (  # noqa: PLC0415
            assert_positive_and_negatives,
        )

        assert_positive_and_negatives(
            self,
            schema_name="user-sol-authorization-receipt-v2.json",
            value=authorization,
            missing_key="sol_candidate_task_ids",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("sol_handoff_envelope_sha256s", "0"),
            authority_purpose_path=("seal", "purpose"),
        )

    def test_real_v2_review_and_zero_write_commit_publishers_reopen(self) -> None:
        batch, claim = self.activate_real_published_batch_for_review()
        review_sha, review = self.publish_real_review(batch, claim)
        contract.validate_sol_review_receipt_v2(review, batch=batch)
        review_state = self.store.record_sol_review("math", review_sha)
        self.assertEqual(
            review_state["global"]["active_writer"]["status"], "applying"
        )
        commit_sha, commit = self.publish_real_zero_write_commit(
            batch, claim, review_sha
        )
        contract.validate_sol_commit_receipt_v2(commit, batch=batch)
        from tests.test_successor_schema_roundtrip import (  # noqa: PLC0415
            assert_positive_and_negatives,
        )

        assert_positive_and_negatives(
            self,
            schema_name="sol-review-receipt-v2.json",
            value=review,
            missing_key="decisions",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("decisions", "0", "sol_handoff_envelope_sha256"),
            authority_purpose_path=("seal", "purpose"),
        )
        assert_positive_and_negatives(
            self,
            schema_name="sol-commit-receipt-v2.json",
            value=commit,
            missing_key="sol_candidate_task_ids",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("sol_handoff_envelope_sha256s", "0"),
            authority_purpose_path=("seal", "purpose"),
        )
        result = self.store.finish_subject_commit("math", commit_sha)
        self.assertEqual(result["commit_receipt_sha256"], commit_sha)
        self.assertEqual(result["subject"]["handoff_status"], "complete")
        self.assertEqual(result["formal_write_count"], 0)

    def test_v2_publishers_fail_closed_on_candidate_or_handoff_drift(self) -> None:
        stale_luna = copy.deepcopy(self.luna)
        stale_luna["sol_candidate_task_ids"] = []
        with self.assertRaises(contract.SubjectSolContractError):
            IndependentUserIntentPublisher(
                self.runtime
            ).publish_sol_authorization(
                luna_batch=stale_luna,
                sol_batch_id="SOL-STALE",
                event_id="AUTH-STALE",
                authorized_at="2026-08-12T10:05:00Z",
                idempotency_key="sol-stale",
            )

        batch, claim = self.activate_real_published_batch_for_review()
        core = self.review()
        core.pop("schema_version")
        core.pop("seal")
        core.update(
            {
                "batch_id": batch["batch_id"],
                "daily_sol_batch_sha256": contract._document_sha256(batch),
                "fencing_token": claim["fencing_token"],
            }
        )
        core["decisions"][0]["sol_handoff_envelope_sha256"] = digest(
            "stale-handoff"
        )
        root = Path(self.temp.name) / "v2-stale-review"
        root.mkdir()
        (root / "review.json").write_text(
            json.dumps(core, sort_keys=True) + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(
            contract.SubjectSolContractError,
            "sol_review_batch_binding_invalid",
        ):
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_review_from_result_directory(root, batch=batch)

    def test_new_schemas_are_strict_and_parseable(self) -> None:
        for name in (
            "sol-review-receipt-v2.json",
            "sol-commit-receipt-v2.json",
        ):
            schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
            self.assertFalse(schema["additionalProperties"])
            self.assertTrue(schema["required"])


if __name__ == "__main__":
    unittest.main()
