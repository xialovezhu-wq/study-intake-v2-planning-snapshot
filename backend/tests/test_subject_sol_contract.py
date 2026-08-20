from __future__ import annotations

import copy
import hashlib
import inspect
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import subject_sol_contract as control  # noqa: E402
import preprocessor_core as core  # noqa: E402
from isolated_authority_publishers import (  # noqa: E402
    IndependentUserIntentPublisher,
    SubjectAuthorityObserverPublisher,
    IsolatedWriterAdapterPublisher,
)
from subject_sol_contract import (  # noqa: E402
    SUBJECT_WRITER_ADAPTERS,
    SubjectSolContractError,
    SubjectSolRuntimeStore,
    validate_daily_sol_batch_v2,
    validate_sol_commit_receipt_v1,
    validate_sol_review_receipt_v1,
    validate_subject_exclusion_receipt_v1,
    validate_subject_luna_batch_v1,
    validate_subject_quality_receipt_v1,
)
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
from dashboard_projection import _subject_batch_state  # noqa: E402


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")


def validate_runtime_schema(name: str, value: object) -> list[dict[str, object]]:
    schema = json.loads(
        (ROOT / "schemas" / f"{name}.json").read_text(encoding="utf-8")
    )
    request = json.dumps(
        {"schema": schema, "instance": value},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    script = r"""
import json, sys
from jsonschema import Draft202012Validator
request = json.load(sys.stdin)
Draft202012Validator.check_schema(request['schema'])
errors = sorted(
    Draft202012Validator(request['schema']).iter_errors(request['instance']),
    key=lambda error: tuple(str(part) for part in error.absolute_path),
)
json.dump([
    {'path': list(error.absolute_path), 'validator': error.validator, 'message': error.message}
    for error in errors
], sys.stdout, separators=(',', ':'))
"""
    result = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=request,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr or "jsonschema validator failed")
    return json.loads(result.stdout)


class ControlPlaneTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.store = SubjectSolRuntimeStore(self.runtime)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def create_batch(
        self,
        subject: str,
        *,
        batch_id: str | None = None,
        count: int = 2,
        generation: str | None = None,
    ) -> dict:
        batch_id = batch_id or f"LUNA-{subject}"
        generation = generation or f"{subject}-generation-1"
        # This module is the retained v1 history/reader suite.  Production
        # prepare_and_freeze_subject_batch() intentionally emits v2, so build
        # the historical bytes explicitly instead of accidentally feeding a
        # v2 task to the private v1 quality-closure helper.
        rows = [
            {
                "capture_id": f"{subject.upper()}-{index}",
                "unit_sha256": digest(f"unit:{subject}:{index}"),
                "input_fingerprint": digest(f"input:{subject}:{index}"),
                "study_date": "2026-08-09" if index == 0 else "2026-08-08",
                "frozen_payload_sha256": digest(f"frozen:{subject}:{index}"),
                "status": "selected",
                "proposal_sha256": None,
                "package_sha256": None,
                "quality_receipt_sha256": None,
                "terminal_receipt_sha256": None,
                "error_code": None,
            }
            for index in range(count)
        ]
        value = {
            "schema_version": "subject_luna_batch_v1",
            "batch_id": batch_id,
            "subject": subject,
            "study_date": "2026-08-09",
            "status": "frozen",
            "capture_high_watermark": f"hwm-{subject}-10",
            "scan_snapshot_sha256": digest(f"scan:{subject}:{batch_id}"),
            "authority_generation": generation,
            "authority_fingerprint": digest(
                f"authority:{subject}:{generation}"
            ),
            "tasks": rows,
            "exclusion_receipt_sha256s": [],
            "all_terminal": False,
            "sol_ready": False,
            "blocking_task_ids": [],
            "formal_write_count": 0,
            "revision": -1,
            "updated_at": None,
        }
        written = self.store._write_batch_locked(value)
        writer = self.store._read_writer_locked(subject)
        writer.update(
            {
                "handoff_status": "awaiting_luna",
                "batch_id": written["batch_id"],
                "authorization_receipt_sha256": None,
                "daily_sol_batch_sha256": None,
                "review_receipt_sha256": None,
                "commit_receipt_sha256": None,
            }
        )
        self.store._write_writer_locked(writer)
        return written

    def artifact(self, kind: str, unit_sha256: str, *, transcript: bool) -> str:
        payload = (
            json.dumps(
                {"kind": kind, "unit_sha256": unit_sha256},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        artifact_sha = hashlib.sha256(payload).hexdigest()
        if transcript:
            path = (
                self.runtime
                / "private/reports/mcp-stage-transcripts/sha256"
                / artifact_sha[:2]
                / f"{artifact_sha}.json"
            )
        else:
            path = (
                self.runtime
                / "private/reports/model-stage-outputs/objects"
                / f"{artifact_sha}.json"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(payload)
        return artifact_sha

    def quality(
        self,
        batch: dict,
        index: int,
        *,
        outcome: str = "accepted",
    ) -> dict:
        task = batch["tasks"][index]
        analysis_output, _ = self.store._publish_immutable_value(
            self.store.receipt_root / "subject-stage-closures",
            {"kind": "analysis-output", "unit_sha256": task["unit_sha256"]},
        )
        analysis_mcp = self.artifact(
            "analysis-mcp", task["unit_sha256"], transcript=True
        )
        critical_output, _ = self.store._publish_immutable_value(
            self.store.receipt_root / "subject-stage-closures",
            {"kind": "critical-output", "unit_sha256": task["unit_sha256"]},
        )
        critical_mcp = self.artifact(
            "critical-mcp", task["unit_sha256"], transcript=True
        )
        proposal = {
            "schema_version": "test_luna_proposal_v1",
            "subject": batch["subject"],
            "capture_id": task["capture_id"],
        }
        proposal_sha, _ = self.store._publish_immutable_value(
            self.store.receipt_root / "subject-proposals", proposal
        )
        capture_sha, _ = self.store._publish_immutable_value(
            self.store.dispatch_root / "capture-freeze-receipts",
            {"capture_id": task["capture_id"], "frozen": task["frozen_payload_sha256"]},
        )
        session_sha, _ = self.store._publish_immutable_value(
            self.store.dispatch_root / "mcp-read-session-receipts",
            {"capture_id": task["capture_id"], "complete": True},
        )
        package = {
            "subject": batch["subject"],
            "capture_id": task["capture_id"],
            "evidence_generation": batch["authority_generation"],
            "evidence_authority_fingerprint": batch["authority_fingerprint"],
            "capture_freeze_receipt_sha256": capture_sha,
            "mcp_read_session_receipt_sha256": session_sha,
            "luna_proposal": proposal,
            "luna_proposal_sha256": proposal_sha,
        }
        package_payload = control._json_file_bytes(package)
        package_sha = hashlib.sha256(package_payload).hexdigest()
        package_path = core.canonical_subject_package_path(
            self.runtime,
            subject=batch["subject"],
            study_date=task["study_date"],
            capture_id=task["capture_id"],
            input_fingerprint=task["input_fingerprint"],
            package_sha256=package_sha,
        )
        package_path.parent.mkdir(parents=True, exist_ok=True)
        package_path.write_bytes(package_payload)
        return self.store._issue_quality_receipt(
            {
                "batch_id": batch["batch_id"],
                "subject": batch["subject"],
                "capture_id": task["capture_id"],
                "unit_sha256": task["unit_sha256"],
                "frozen_payload_sha256": task["frozen_payload_sha256"],
                "completion_sha256": digest(f"completion:{task['unit_sha256']}"),
                "dispatch_receipt_sha256": digest(f"dispatch:{task['unit_sha256']}"),
                "dispatch_package_sha256": digest(f"dispatch-package:{task['unit_sha256']}"),
                "capture_freeze_receipt_sha256": capture_sha,
                "mcp_read_session_receipt_sha256": session_sha,
                "analysis_output_sha256": analysis_output,
                "analysis_mcp_transcript_sha256": analysis_mcp,
                "critical_review_output_sha256": critical_output,
                "critical_review_mcp_transcript_sha256": critical_mcp,
                "draft_sha256": digest(f"draft:{task['unit_sha256']}"),
                "review_outcome": outcome,
                "proposal_sha256": proposal_sha,
                "package_sha256": package_sha,
                "authority": {
                    "generation": batch["authority_generation"],
                    "authority_fingerprint": batch["authority_fingerprint"],
                },
                "model_call_count": 2,
                "formal_write_count": 0,
                "issued_at": "2026-08-09T01:00:00Z",
            }
        )

    def ready_batch(self, subject: str, *, batch_id: str | None = None) -> dict:
        batch = self.create_batch(subject, batch_id=batch_id)
        batch = self.store.freeze_subject_batch(subject, batch["batch_id"])
        for index in range(len(batch["tasks"])):
            batch = self.store._record_quality_receipt(
                self.quality(batch, index)
            )
        self.assertTrue(batch["all_terminal"])
        self.assertTrue(batch["sol_ready"])
        return batch

    def authorize(
        self,
        subject: str,
        *,
        sol_batch_id: str | None = None,
        authorized_at: str = "2026-08-09T02:00:00Z",
    ) -> tuple[dict, dict]:
        sol_batch_id = sol_batch_id or f"SOL-{subject}"
        luna = self.store.read_subject_batch(subject)
        assert luna is not None
        authorization_sha, _ = IndependentUserIntentPublisher(
            self.runtime
        ).publish_sol_authorization(
            luna_batch=luna,
            sol_batch_id=sol_batch_id,
            event_id=f"AUTH-{subject}-{sol_batch_id}",
            authorized_at=authorized_at,
            idempotency_key=f"idem-{subject}-{sol_batch_id}",
        )
        batch = self.store.build_authorized_batch(
            subject,
            sol_batch_id=sol_batch_id,
            authorization_receipt_sha256=authorization_sha,
        )
        state = self.store.authorize_batch(subject, batch)
        return batch, state

    def approve_review(
        self, batch: dict, claim: dict, *, status: str = "approved"
    ) -> tuple[dict, dict]:
        decisions = [
            {
                "proposal_sha256": proposal,
                "decision": "adopt" if status == "approved" else "reject",
                "modified_proposal_sha256": None,
                "reason": "canonical evidence independently reopened",
            }
            for proposal in batch["proposal_sha256s"]
        ]
        core = {
                "batch_id": batch["batch_id"],
                "subject": batch["subject"],
                "fencing_token": claim["fencing_token"],
                "writer": "sol",
                "writer_adapter": SUBJECT_WRITER_ADAPTERS[batch["subject"]],
                "canonical_evidence_sha256": digest(
                    f"canonical:{batch['batch_id']}"
                ),
                "decisions": decisions,
                "status": status,
                "dispatcher_effect": {
                    "scope": "subject",
                    "subject": batch["subject"],
                    "paused_subjects": [],
                    "drained_subjects": [],
                    "other_subjects_unchanged": True,
                },
                "formal_write_count": 0,
                "completed_at": "2026-08-09T02:05:00Z",
            }
        root = Path(self.temp.name) / f"review-result-{len(list(Path(self.temp.name).glob('review-result-*')))}"
        root.mkdir(parents=True)
        (root / "review.json").write_text(
            json.dumps(core, sort_keys=True) + "\n", encoding="utf-8"
        )
        receipt_sha, receipt_path = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_review_from_result_directory(root, batch=batch)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        state = self.store.record_sol_review(batch["subject"], receipt_sha)
        return receipt, state

    def complete_without_write(
        self,
        batch: dict,
        claim: dict,
        review_receipt: dict,
        *,
        fencing_token: int | None = None,
    ) -> dict:
        review_sha = control._document_sha256(review_receipt)
        receipt_sha = self.publish_isolated_apply(
            batch,
            {
                "batch_id": batch["batch_id"],
                "subject": batch["subject"],
                "fencing_token": fencing_token or claim["fencing_token"],
                "writer": "sol",
                "writer_adapter": SUBJECT_WRITER_ADAPTERS[batch["subject"]],
                "sol_review_receipt_sha256": review_sha,
                "pre_state_sha256": digest(f"pre:{batch['batch_id']}"),
                "post_state_sha256": digest(f"pre:{batch['batch_id']}"),
                "operations": [],
                "writer_process": {
                    "adapter_run_id": f"sim-{batch['batch_id']}",
                    "pid": 12345,
                    "terminal_state": "stopped",
                    "exit_code": 0,
                    "stopped_at": "2026-08-09T02:09:59Z",
                    "evidence_sha256": digest(f"process:{batch['batch_id']}"),
                },
                "transaction_end": {
                    "transaction_id": f"txn-{batch['batch_id']}",
                    "state": "already_current",
                    "ended_at": "2026-08-09T02:09:58Z",
                    "evidence_sha256": digest(f"txn:{batch['batch_id']}"),
                },
                "recovery_position_sha256": None,
                "rollback_receipt_sha256": None,
                "status": "already_current",
                "dispatcher_effect": {
                    "scope": "subject",
                    "subject": batch["subject"],
                    "paused_subjects": [],
                    "drained_subjects": [],
                    "other_subjects_unchanged": True,
                },
                "formal_write_count": 0,
                "completed_at": "2026-08-09T02:10:00Z",
            },
        )
        return self.store.finish_subject_commit(batch["subject"], receipt_sha)

    def publish_isolated_apply(self, batch: dict, core: dict) -> str:
        root = Path(self.temp.name) / f"writer-result-{len(list(Path(self.temp.name).glob('writer-result-*')))}"
        root.mkdir(parents=True)
        process = dict(core["writer_process"])
        process.pop("evidence_sha256", None)
        process.update(
            {
                "batch_id": core["batch_id"],
                "subject": core["subject"],
                "fencing_token": core["fencing_token"],
                "formal_write_count": 0,
            }
        )
        transaction = dict(core["transaction_end"])
        transaction.pop("evidence_sha256", None)
        transaction.update(
            {
                "batch_id": core["batch_id"],
                "subject": core["subject"],
                "fencing_token": core["fencing_token"],
                "formal_write_count": 0,
            }
        )
        operations = {
            "batch_id": core["batch_id"],
            "subject": core["subject"],
            "fencing_token": core["fencing_token"],
            "pre_state_sha256": core["pre_state_sha256"],
            "post_state_sha256": core["post_state_sha256"],
            "operations": core["operations"],
            "formal_write_count": core["formal_write_count"],
        }
        result = {
            key: core[key]
            for key in (
                "batch_id", "subject", "fencing_token", "writer",
                "writer_adapter", "sol_review_receipt_sha256", "status",
                "dispatcher_effect", "completed_at",
            )
        }
        for name, value in (
            ("writer-process.json", process),
            ("transaction-end.json", transaction),
            ("operations.json", operations),
            ("result.json", result),
        ):
            (root / name).write_text(
                json.dumps(value, sort_keys=True) + "\n", encoding="utf-8"
            )
        if core["status"] == "failed" and core.get("recovery_position_sha256"):
            (root / "recovery-position.json").write_text(
                json.dumps({"state_sha256": core["recovery_position_sha256"]}) + "\n",
                encoding="utf-8",
            )
        if core["status"] == "rolled_back" and core.get("rollback_receipt_sha256"):
            (root / "rollback.json").write_text(
                json.dumps({"state_sha256": core["rollback_receipt_sha256"]}) + "\n",
                encoding="utf-8",
            )
        receipt_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_apply_from_execution_result_directory(root, batch=batch)
        return receipt_sha


class SubjectBatchQualityTests(ControlPlaneTestCase):
    def test_successful_background_batch_rolls_over_without_sol_or_formal_write(
        self,
    ) -> None:
        first = self.create_batch("math", batch_id="LUNA-MATH-FIRST", count=1)
        first = self.store.freeze_subject_batch("math", first["batch_id"])
        first = self.store._record_quality_receipt(self.quality(first, 0))
        self.assertTrue(first["all_terminal"])
        self.assertTrue(first["sol_ready"])

        rolled = self.store.rollover_background_luna_batch(
            "math", mode="auto_success"
        )
        receipt = rolled["receipt"]
        self.assertEqual(receipt["old_batch_id"], first["batch_id"])
        self.assertEqual(receipt["next_generation"], first["authority_generation"])
        self.assertFalse(receipt["sol_called"])
        self.assertFalse(receipt["sol_enabled"])
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["provider_request_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertTrue(Path(rolled["receipt_path"]).is_file())
        archive = json.loads(
            Path(receipt["archive_path"]).read_text(encoding="utf-8")
        )
        pointer = json.loads(
            self.store._background_rollover_pointer_path("math").read_text(
                encoding="utf-8"
            )
        )
        strict_samples = {
            "subject-background-luna-batch-archive-v1": archive,
            "subject-background-luna-rollover-pointer-v1": pointer,
            "subject-background-luna-rollover-receipt-v1": receipt,
        }
        for name, sample in strict_samples.items():
            with self.subTest(schema=name):
                self.assertEqual(validate_runtime_schema(name, sample), [])
                missing = copy.deepcopy(sample)
                missing.pop("seal")
                self.assertTrue(validate_runtime_schema(name, missing))
                wrong_purpose = copy.deepcopy(sample)
                wrong_purpose["seal"]["purpose"] = "wrong-purpose"
                self.assertTrue(validate_runtime_schema(name, wrong_purpose))
        bad_archive_sha = copy.deepcopy(archive)
        bad_archive_sha["batch_snapshot_sha256"] = "not-a-sha256"
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-batch-archive-v1",
                bad_archive_sha,
            )
        )
        bad_archive_type = copy.deepcopy(archive)
        bad_archive_type["batch"]["revision"] = "1"
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-batch-archive-v1",
                bad_archive_type,
            )
        )
        bad_pointer_path = copy.deepcopy(pointer)
        bad_pointer_path["rollover_receipt_path"] = "relative/receipt.json"
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-rollover-pointer-v1",
                bad_pointer_path,
            )
        )
        bad_receipt_mode = copy.deepcopy(receipt)
        bad_receipt_mode["resume_acceptance_sha256"] = "f" * 64
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-rollover-receipt-v1",
                bad_receipt_mode,
            )
        )
        bad_receipt_count = copy.deepcopy(receipt)
        bad_receipt_count["terminal_status_counts"]["failed"] = "0"
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-rollover-receipt-v1",
                bad_receipt_count,
            )
        )
        writer = self.store.read_subject("math")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        self.assertFalse(writer["generation_fence"]["blocked"])
        self.assertEqual(
            writer["generation_fence"]["source_generation"],
            first["authority_generation"],
        )
        self.assertEqual(
            writer["generation_fence"]["next_generation"],
            first["authority_generation"],
        )

        second = self.create_batch(
            "math",
            batch_id="LUNA-MATH-SECOND",
            count=20,
            generation=first["authority_generation"],
        )
        self.assertEqual(second["batch_id"], "LUNA-MATH-SECOND")
        self.assertEqual(len(second["tasks"]), 20)
        self.assertEqual(second["formal_write_count"], 0)
        again = self.store.rollover_background_luna_batch(
            "math", mode="auto_success"
        ) if second.get("all_terminal") else None
        self.assertIsNone(again)

    def test_failed_background_batch_requires_explicit_resume_binding(
        self,
    ) -> None:
        batch = self.create_batch("english", batch_id="LUNA-ENGLISH-FAIL", count=1)
        batch = self.store.record_terminal_failure(
            subject="english",
            batch_id=batch["batch_id"],
            capture_id=batch["tasks"][0]["capture_id"],
            unit_sha256=batch["tasks"][0]["unit_sha256"],
            status="failed",
            error_code="synthetic_terminal_failure",
        )
        batch = self.store.freeze_subject_batch("english", batch["batch_id"])
        self.assertTrue(batch["all_terminal"])
        self.assertFalse(batch["sol_ready"])
        with self.assertRaises(SubjectSolContractError) as missing:
            self.store.rollover_background_luna_batch(
                "english", mode="explicit_failure_resume"
            )
        self.assertEqual(
            missing.exception.code, "resume_acceptance_sha256_invalid"
        )
        acceptance_sha256 = digest("explicit-english-subject-resume")
        rolled = self.store.rollover_background_luna_batch(
            "english",
            mode="explicit_failure_resume",
            resume_acceptance_sha256=acceptance_sha256,
        )
        self.assertEqual(
            rolled["receipt"]["resume_acceptance_sha256"],
            acceptance_sha256,
        )
        self.assertFalse(rolled["receipt"]["sol_called"])
        self.assertEqual(rolled["receipt"]["formal_write_count"], 0)
        self.assertEqual(
            validate_runtime_schema(
                "subject-background-luna-rollover-receipt-v1",
                rolled["receipt"],
            ),
            [],
        )
        missing_resume = copy.deepcopy(rolled["receipt"])
        missing_resume["resume_acceptance_sha256"] = None
        self.assertTrue(
            validate_runtime_schema(
                "subject-background-luna-rollover-receipt-v1",
                missing_resume,
            )
        )
        writer = self.store.read_subject("english")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])

    def test_all_terminal_and_sol_ready_are_independent(self) -> None:
        batch = self.create_batch("math")
        batch = self.store.freeze_subject_batch("math", batch["batch_id"])
        self.assertFalse(batch["all_terminal"])
        self.assertFalse(batch["sol_ready"])

        batch = self.store._record_quality_receipt(self.quality(batch, 0))
        batch = self.store._record_quality_receipt(
            self.quality(batch, 1, outcome="rejected")
        )
        self.assertTrue(batch["all_terminal"])
        self.assertFalse(batch["sol_ready"])
        self.assertEqual(batch["blocking_task_ids"], ["MATH-1"])
        self.assertEqual(batch["tasks"][1]["status"], "needs_rework")

    def test_each_successful_task_binds_two_distinct_mcp_transcripts(self) -> None:
        batch = self.create_batch("english", count=1)
        batch = self.store.freeze_subject_batch("english", batch["batch_id"])
        receipt = self.quality(batch, 0, outcome="corrected")
        checked = validate_subject_quality_receipt_v1(receipt)
        self.assertEqual(checked["model_call_count"], 2)
        self.assertNotEqual(
            checked["analysis_mcp_transcript_sha256"],
            checked["critical_review_mcp_transcript_sha256"],
        )
        batch = self.store._record_quality_receipt(receipt)
        self.assertTrue(batch["sol_ready"])
        self.assertEqual(batch["formal_write_count"], 0)

    def test_tampered_quality_receipt_fails_hmac(self) -> None:
        batch = self.create_batch("cs408", count=1)
        receipt = self.quality(batch, 0)
        tampered = copy.deepcopy(receipt)
        tampered["review_outcome"] = "corrected"
        with self.assertRaisesRegex(
            SubjectSolContractError, "control_receipt_hmac_invalid"
        ):
            self.store._record_quality_receipt(tampered)

    def test_quality_receipt_cannot_bind_missing_transcript(self) -> None:
        batch = self.create_batch("cs408", count=1)
        receipt = self.quality(batch, 0)
        transcript_sha = receipt["critical_review_mcp_transcript_sha256"]
        transcript_path = (
            self.runtime
            / "private/reports/mcp-stage-transcripts/sha256"
            / transcript_sha[:2]
            / f"{transcript_sha}.json"
        )
        transcript_path.unlink()
        with self.assertRaises(SubjectSolContractError) as missing:
            self.store._record_quality_receipt(receipt)
        self.assertEqual(missing.exception.code, "quality_artifact_missing")
        self.assertEqual(
            missing.exception.diagnostic["artifact"],
            "critical_review_mcp_transcript",
        )

    def test_failure_terminal_is_not_quality_success(self) -> None:
        batch = self.create_batch("math", count=1)
        batch = self.store.record_terminal_failure(
            subject="math",
            batch_id=batch["batch_id"],
            capture_id="MATH-0",
            unit_sha256=batch["tasks"][0]["unit_sha256"],
            status="evidence_pending",
            error_code="solution_image_missing",
        )
        batch = self.store.freeze_subject_batch("math", batch["batch_id"])
        self.assertTrue(batch["all_terminal"])
        self.assertFalse(batch["sol_ready"])
        self.assertEqual(batch["formal_write_count"], 0)


class SubjectExclusionReceiptTests(ControlPlaneTestCase):
    def closed_batch_with_rejection(self, subject: str = "math") -> dict:
        batch = self.create_batch(subject)
        batch = self.store.freeze_subject_batch(subject, batch["batch_id"])
        batch = self.store._record_quality_receipt(self.quality(batch, 0))
        batch = self.store._record_quality_receipt(
            self.quality(batch, 1, outcome="rejected")
        )
        self.assertTrue(batch["all_terminal"])
        self.assertFalse(batch["sol_ready"])
        return batch

    def issue_exclusion(self, batch: dict, index: int = 1) -> dict:
        task = batch["tasks"][index]
        request = self.store.prepare_subject_exclusion_authority(
            batch["subject"],
            original_batch_id=batch["batch_id"],
            capture_id=task["capture_id"],
            unit_sha256=task["unit_sha256"],
        )
        authorization_sha, _ = IndependentUserIntentPublisher(
            self.runtime, exclusion=True
        ).publish_subject_exclusion_authorization(
            request=request,
            event_id=f"EXCLUDE-{task['capture_id']}",
            authorized_at="2026-08-09T03:00:00Z",
            reason="user explicitly excluded this failed unit from the rebuilt batch",
        )
        return self.store.issue_subject_exclusion_receipt(
            batch["subject"],
            authorization_receipt_sha256=authorization_sha,
            issued_at="2026-08-09T03:00:01Z",
        )

    def test_signed_exclusion_rebuilds_exact_survivor_set(self) -> None:
        original = self.closed_batch_with_rejection()
        receipt = self.issue_exclusion(original)
        checked = validate_subject_exclusion_receipt_v1(receipt)
        receipt_sha = control._document_sha256(checked)
        receipt_path = (
            self.runtime
            / "dispatch/control-receipts/subject-exclusion/sha256"
            / receipt_sha[:2]
            / f"{receipt_sha}.json"
        )
        snapshot_sha = receipt["original_batch"]["batch_sha256"]
        snapshot_path = (
            self.runtime
            / "dispatch/subject-luna-batch-snapshots/sha256"
            / snapshot_sha[:2]
            / f"{snapshot_sha}.json"
        )
        self.assertTrue(receipt_path.is_file())
        self.assertTrue(snapshot_path.is_file())
        self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(snapshot_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(
            receipt["user_authorization"]["event_type"],
            "explicit_user_subject_exclusion",
        )

        rebuilt = self.store.create_subject_batch(
            batch_id="LUNA-math-rebuilt",
            subject="math",
            study_date=original["study_date"],
            capture_high_watermark=original["capture_high_watermark"],
            authority_generation=original["authority_generation"],
            authority_fingerprint=original["authority_fingerprint"],
            tasks=[original["tasks"][0]],
            exclusion_receipt_sha256s=[receipt_sha],
        )
        self.assertEqual(
            [row["capture_id"] for row in rebuilt["tasks"]],
            [original["tasks"][0]["capture_id"]],
        )
        self.assertTrue(rebuilt["sol_ready"])
        rebuilt = self.store.freeze_subject_batch("math", rebuilt["batch_id"])
        self.assertTrue(rebuilt["all_terminal"])
        self.assertTrue(rebuilt["sol_ready"])
        self.assertEqual(rebuilt["formal_write_count"], 0)

    def test_arbitrary_receipt_hash_cannot_replace_blocking_batch(self) -> None:
        original = self.closed_batch_with_rejection("cs408")
        with self.assertRaises(SubjectSolContractError) as rejected:
            self.store.create_subject_batch(
                batch_id="LUNA-cs408-rebuilt",
                subject="cs408",
                study_date=original["study_date"],
                capture_high_watermark=original["capture_high_watermark"],
                authority_generation=original["authority_generation"],
                authority_fingerprint=original["authority_fingerprint"],
                tasks=[original["tasks"][0]],
                exclusion_receipt_sha256s=["a" * 64],
            )
        self.assertEqual(
            rejected.exception.code,
            "subject_exclusion_receipt_missing",
        )
        self.assertEqual(
            self.store.read_subject_batch("cs408")["batch_id"],
            original["batch_id"],
        )

    def test_tampered_exclusion_hmac_cannot_unblock(self) -> None:
        original = self.closed_batch_with_rejection("english")
        receipt = self.issue_exclusion(original)
        tampered = copy.deepcopy(receipt)
        tampered["reason"] = "tampered after signature"
        payload = control._json_file_bytes(tampered)
        tampered_sha = hashlib.sha256(payload).hexdigest()
        path = (
            self.runtime
            / "dispatch/control-receipts/subject-exclusion/sha256"
            / tampered_sha[:2]
            / f"{tampered_sha}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        with self.assertRaises(SubjectSolContractError) as rejected:
            self.store.create_subject_batch(
                batch_id="LUNA-english-rebuilt",
                subject="english",
                study_date=original["study_date"],
                capture_high_watermark=original["capture_high_watermark"],
                authority_generation=original["authority_generation"],
                authority_fingerprint=original["authority_fingerprint"],
                tasks=[original["tasks"][0]],
                exclusion_receipt_sha256s=[tampered_sha],
            )
        self.assertIn(
            rejected.exception.code,
            {"exclusion_reason_binding_invalid", "control_receipt_hmac_invalid"},
        )

    def test_exclusion_must_preserve_every_nonexcluded_task_exactly(self) -> None:
        original = self.closed_batch_with_rejection()
        receipt_sha = control._document_sha256(self.issue_exclusion(original))
        with self.assertRaises(SubjectSolContractError) as rejected:
            self.store.create_subject_batch(
                batch_id="LUNA-math-lossy-rebuild",
                subject="math",
                study_date=original["study_date"],
                capture_high_watermark=original["capture_high_watermark"],
                authority_generation=original["authority_generation"],
                authority_fingerprint=original["authority_fingerprint"],
                tasks=[],
                exclusion_receipt_sha256s=[receipt_sha],
            )
        self.assertEqual(
            rejected.exception.code,
            "subject_exclusion_survivor_set_mismatch",
        )

    def test_terminal_failure_receipt_is_reopened_and_stale_exclusion_fails(self) -> None:
        original = self.create_batch("cs408")
        original = self.store._record_quality_receipt(self.quality(original, 0))
        failed = original["tasks"][1]
        original = self.store.record_terminal_failure(
            subject="cs408",
            batch_id=original["batch_id"],
            capture_id=failed["capture_id"],
            unit_sha256=failed["unit_sha256"],
            status="failed",
            error_code="provider_schema_rejected",
        )
        original = self.store.freeze_subject_batch("cs408", original["batch_id"])
        receipt = self.issue_exclusion(original)
        self.assertEqual(receipt["excluded_task"]["status"], "failed")
        self.assertEqual(
            receipt["excluded_task"]["terminal_receipt_sha256"],
            original["tasks"][1]["terminal_receipt_sha256"],
        )

        with self.assertRaises(SubjectSolContractError) as rejected:
            self.store.record_terminal_failure(
                subject="cs408",
                batch_id=original["batch_id"],
                capture_id=failed["capture_id"],
                unit_sha256=failed["unit_sha256"],
                status="failed",
                error_code="different_failure_after_authorization",
            )
        self.assertEqual(rejected.exception.code, "terminal_task_already_closed")

    def test_each_removed_blocking_unit_requires_its_own_receipt(self) -> None:
        original = self.create_batch("math", count=3)
        original = self.store.freeze_subject_batch("math", original["batch_id"])
        original = self.store._record_quality_receipt(self.quality(original, 0))
        original = self.store._record_quality_receipt(
            self.quality(original, 1, outcome="rejected")
        )
        third = original["tasks"][2]
        original = self.store.record_terminal_failure(
            subject="math",
            batch_id=original["batch_id"],
            capture_id=third["capture_id"],
            unit_sha256=third["unit_sha256"],
            status="evidence_pending",
            error_code="solution_image_missing",
        )
        first_receipt = self.issue_exclusion(original, 1)
        second_receipt = self.issue_exclusion(original, 2)
        receipt_hashes = sorted(
            [
                control._document_sha256(first_receipt),
                control._document_sha256(second_receipt),
            ]
        )
        rebuilt = self.store.create_subject_batch(
            batch_id="LUNA-math-two-exclusions",
            subject="math",
            study_date=original["study_date"],
            capture_high_watermark=original["capture_high_watermark"],
            authority_generation=original["authority_generation"],
            authority_fingerprint=original["authority_fingerprint"],
            tasks=[original["tasks"][0]],
            exclusion_receipt_sha256s=receipt_hashes,
        )
        self.assertEqual(rebuilt["exclusion_receipt_sha256s"], receipt_hashes)
        rebuilt = self.store.freeze_subject_batch("math", rebuilt["batch_id"])
        self.assertTrue(rebuilt["sol_ready"])

    def test_quality_passed_task_cannot_receive_exclusion_receipt(self) -> None:
        batch = self.ready_batch("math")
        task = batch["tasks"][0]
        with self.assertRaises(SubjectSolContractError) as rejected:
            self.store.prepare_subject_exclusion_authority(
                "math",
                original_batch_id=batch["batch_id"],
                capture_id=task["capture_id"],
                unit_sha256=task["unit_sha256"],
            )
        self.assertEqual(
            rejected.exception.code,
            "subject_exclusion_task_not_blocking",
        )


class GlobalSolWriterTests(ControlPlaneTestCase):
    def test_daily_v2_binds_complete_subject_batch_and_explicit_authorization(self) -> None:
        self.ready_batch("math")
        batch, state = self.authorize("math")
        self.assertEqual(validate_daily_sol_batch_v2(batch), batch)
        self.assertEqual(batch["authorization_receipt"]["event"]["event_type"], "explicit_user_sol_authorization")
        self.assertEqual(len(batch["package_sha256s"]), 2)
        self.assertEqual(state["global"]["active_writer_count"], 0)
        self.assertEqual(state["subject"]["handoff_status"], "queued_for_sol")

    def test_fifo_and_second_claim_fail_closed_with_receipts(self) -> None:
        self.ready_batch("math")
        math_batch, _ = self.authorize(
            "math", authorized_at="2026-08-09T02:00:00Z"
        )
        self.ready_batch("cs408")
        cs_batch, _ = self.authorize(
            "cs408", authorized_at="2026-08-09T02:00:01Z"
        )
        with self.assertRaises(SubjectSolContractError) as non_head:
            self.store.begin_sol_review(
                "cs408", cs_batch["batch_id"], owner_id="sol-cs"
            )
        self.assertEqual(non_head.exception.code, "global_sol_fifo_head_mismatch")
        first_receipt = non_head.exception.diagnostic["claim_receipt_sha256"]

        claim = self.store.begin_sol_review(
            "math", math_batch["batch_id"], owner_id="sol-math"
        )
        self.assertEqual(claim["global"]["active_writer_count"], 1)
        with self.assertRaises(SubjectSolContractError) as busy:
            self.store.begin_sol_review(
                "cs408", cs_batch["batch_id"], owner_id="sol-cs"
            )
        self.assertEqual(busy.exception.code, "global_sol_writer_busy")
        second_receipt = busy.exception.diagnostic["claim_receipt_sha256"]
        self.assertNotEqual(first_receipt, second_receipt)
        for receipt_sha in (first_receipt, second_receipt):
            path = (
                self.runtime
                / "dispatch/control-receipts/sol-claims/sha256"
                / receipt_sha[:2]
                / f"{receipt_sha}.json"
            )
            self.assertTrue(path.is_file())

    def test_concurrent_claims_never_create_two_active_writers(self) -> None:
        self.ready_batch("math")
        math_batch, _ = self.authorize("math", authorized_at="2026-08-09T02:00:00Z")
        self.ready_batch("english")
        english_batch, _ = self.authorize("english", authorized_at="2026-08-09T02:00:01Z")
        barrier = threading.Barrier(3)
        results: list[str] = []

        def claim(subject: str, batch_id: str) -> None:
            barrier.wait()
            try:
                self.store.begin_sol_review(subject, batch_id, owner_id=f"sol-{subject}")
            except SubjectSolContractError as exc:
                results.append(exc.code)
            else:
                results.append("acquired")

        threads = [
            threading.Thread(target=claim, args=("math", math_batch["batch_id"])),
            threading.Thread(target=claim, args=("english", english_batch["batch_id"])),
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(2)
        self.assertEqual(results.count("acquired"), 1)
        self.assertEqual(self.store.read_global()["active_writer_count"], 1)

    def test_luna_claim_and_sol_fence_share_one_subject_critical_section(self) -> None:
        self.ready_batch("math")
        batch, _ = self.authorize("math")
        entered = threading.Event()
        release = threading.Event()
        lease_root = self.runtime / "dispatch/state/leases"
        lease_root.mkdir(parents=True)
        outcomes: list[str] = []

        def submitter(_task: object) -> str:
            entered.set()
            self.assertTrue(release.wait(2))
            (lease_root / f"{digest('atomic-luna')}.json").write_text(
                json.dumps({"subject": "math", "status": "claimed"}) + "\n",
                encoding="utf-8",
            )
            return "claimed"

        luna = threading.Thread(
            target=lambda: outcomes.append(
                self.store.submit_luna_under_generation_fence(
                    "math", submitter, object()
                )
            )
        )
        luna.start()
        self.assertTrue(entered.wait(2))

        def claim_sol() -> None:
            try:
                self.store.begin_sol_review(
                    "math", batch["batch_id"], owner_id="atomic-sol"
                )
            except SubjectSolContractError as exc:
                outcomes.append(exc.code)

        sol = threading.Thread(target=claim_sol)
        sol.start()
        release.set()
        luna.join(2)
        sol.join(2)
        self.assertEqual(sorted(outcomes), ["claimed", "subject_luna_not_terminal"])
        self.assertEqual(self.store.read_global()["active_writer_count"], 0)

    def test_runtime_rejects_reordered_pending_fifo_projection(self) -> None:
        self.ready_batch("math")
        self.authorize("math", authorized_at="2026-08-09T02:00:00Z")
        self.ready_batch("english")
        self.authorize("english", authorized_at="2026-08-09T02:00:01Z")
        path = self.store.global_state_path
        state = json.loads(path.read_text(encoding="utf-8"))
        state["queue"] = list(reversed(state["queue"]))
        path.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
        with self.assertRaises(SubjectSolContractError) as caught:
            self.store.read_global()
        self.assertEqual(caught.exception.code, "global_sol_fifo_order_invalid")

    def test_subject_write_fence_does_not_pause_other_subject_luna(self) -> None:
        self.ready_batch("math")
        batch, _ = self.authorize("math")
        claim = self.store.begin_sol_review("math", batch["batch_id"], owner_id="sol")
        self.assertFalse(self.store.luna_admission("math")["read_session_allowed"])
        self.assertEqual(self.store.luna_admission("math")["route"], "next_batch")
        for other in ("cs408", "english"):
            admission = self.store.luna_admission(other)
            self.assertTrue(admission["read_session_allowed"])
            self.assertTrue(admission["other_subjects_unchanged"])

        review, reviewed = self.approve_review(batch, claim)
        self.assertEqual(reviewed["subject"]["handoff_status"], "sol_reviewed")
        finished = self.complete_without_write(batch, claim, review)
        self.assertEqual(finished["global"]["active_writer_count"], 0)
        self.assertEqual(finished["formal_write_count"], 0)
        self.assertFalse(self.store.luna_admission("math")["read_session_allowed"])
        next_fingerprint = digest("math-generation-2-authority")
        observer_sha, _ = SubjectAuthorityObserverPublisher(
            self.runtime,
            SimpleNamespace(
                subject_authority_snapshot=lambda _subject: {
                    "subject": "math",
                    "generation": "math-generation-2",
                    "authority_fingerprint": next_fingerprint,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            ),
        ).observe("math", observed_at="2026-08-09T04:00:00Z")
        closure = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_post_commit_authority_closure(
            subject="math",
            batch_id=batch["batch_id"],
            subject_luna_batch_id=batch["subject_luna_batch"]["batch_id"],
            source_generation="math-generation-1",
            authority_observation_sha256=observer_sha,
            commit_receipt_sha256=finished["commit_receipt_sha256"],
            acknowledged_at="2026-08-09T04:00:01Z",
        )
        acknowledged = self.store.acknowledge_subject_generation(
            "math", authority_ack_sha256=closure["authority_ack_sha256"]
        )
        self.assertFalse(acknowledged["generation_fence"]["blocked"])
        self.assertTrue(self.store.luna_admission("math")["read_session_allowed"])

    def test_dispatcher_routes_same_subject_to_next_batch_without_model_submit(self) -> None:
        self.ready_batch("math")
        batch, _ = self.authorize("math")
        self.store.begin_sol_review("math", batch["batch_id"], owner_id="sol")
        config_path = Path(self.temp.name) / "config.json"
        config_path.write_text("{}\n", encoding="utf-8")
        runtime = ProductionDispatchRuntime(
            {
                "runtime_root": str(self.runtime),
                "worker": {"model_timeout_seconds": 1},
                "math_deep_v2": {
                    "soft_runtime_warning_seconds": 3600,
                    "stall_timeout_seconds": 1800,
                    "stall_probe_interval_seconds": 60,
                    "stall_probe_required_consecutive_failures": 2,
                },
            },
            "math",
            config_path,
        )
        task = SimpleNamespace(unit_sha256=digest("next-math-unit"))
        decisions = [
            {
                "capture_id": "MATH-NEXT",
                "unit_sha256": task.unit_sha256,
                "eligible": True,
                "model_enqueue_allowed": True,
            }
        ]
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([SimpleNamespace(task=task)], decisions),
        ), mock.patch(
            "preprocess_dispatcher._write_subject_projections"
        ), mock.patch.object(runtime.dispatcher, "submit") as submit:
            handles, projected = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        submit.assert_not_called()
        self.assertEqual(projected[0]["phase"], "next_batch")
        self.assertEqual(
            projected[0]["error_code"], "subject_generation_write_fence"
        )

    def test_sol_review_and_commit_are_distinct_receipts(self) -> None:
        self.ready_batch("english")
        batch, _ = self.authorize("english")
        claim = self.store.begin_sol_review("english", batch["batch_id"], owner_id="sol")
        review, review_state = self.approve_review(batch, claim)
        validate_sol_review_receipt_v1(review, batch=batch)
        self.assertEqual(review["formal_write_count"], 0)
        self.assertIsNone(review_state["subject"]["commit_receipt_sha256"])
        finished = self.complete_without_write(batch, claim, review)
        self.assertNotEqual(
            finished["subject"]["review_receipt_sha256"],
            finished["subject"]["commit_receipt_sha256"],
        )
        self.assertEqual(finished["global"]["formal_write_count"], 0)

    def test_subject_with_active_luna_claim_cannot_start_sol(self) -> None:
        self.ready_batch("cs408")
        batch, _ = self.authorize("cs408")
        lease_root = self.runtime / "dispatch/state/leases"
        lease_root.mkdir(parents=True)
        (lease_root / f"{digest('active-luna')}.json").write_text(
            json.dumps({"subject": "cs408", "status": "claimed"}) + "\n",
            encoding="utf-8",
        )
        with self.assertRaises(SubjectSolContractError) as blocked:
            self.store.begin_sol_review("cs408", batch["batch_id"], owner_id="sol")
        self.assertEqual(blocked.exception.code, "subject_luna_not_terminal")
        self.assertEqual(blocked.exception.diagnostic["active_luna_count"], 1)

    def test_review_rejection_releases_global_lease_and_safe_pauses_only_subject(self) -> None:
        self.ready_batch("math")
        batch, _ = self.authorize("math")
        claim = self.store.begin_sol_review("math", batch["batch_id"], owner_id="sol")
        _, state = self.approve_review(batch, claim, status="rejected")
        self.assertEqual(state["global"]["active_writer_count"], 0)
        self.assertEqual(state["subject"]["handoff_status"], "safe_paused")
        self.assertTrue(self.store.luna_admission("english")["read_session_allowed"])


class ArchitectureBoundaryTests(ControlPlaneTestCase):
    def test_public_authority_signers_are_disabled(self) -> None:
        for method, code in (
            (self.store.issue_sol_review_receipt, "public_sol_review_signer_disabled"),
            (
                lambda _core: self.store.issue_sol_commit_receipt(
                    _core, batch={}
                ),
                "public_sol_commit_signer_disabled",
            ),
        ):
            with self.subTest(code=code):
                with self.assertRaises(SubjectSolContractError) as caught:
                    method({})
                self.assertEqual(caught.exception.code, code)
        with self.assertRaisesRegex(
            ValueError, "direct_writer_apply_signer_disabled"
        ):
            IsolatedWriterAdapterPublisher(self.runtime).publish_apply({}, batch={})
        with self.assertRaisesRegex(ValueError, "direct_sol_review_signer_disabled"):
            IsolatedWriterAdapterPublisher(self.runtime).publish_review({})

    def test_sol_authorization_requires_reopenable_independent_receipt(self) -> None:
        self.ready_batch("math")
        before = self.store.read_subject_batch("math")
        with self.assertRaises(SubjectSolContractError) as missing:
            self.store.build_authorized_batch(
                "math",
                sol_batch_id="SOL-missing",
                authorization_receipt_sha256="a" * 64,
            )
        self.assertEqual(missing.exception.code, "user_sol_authorization_receipt_missing")
        self.assertEqual(self.store.read_subject_batch("math"), before)

        authorization_sha, authorization_path = IndependentUserIntentPublisher(
            self.runtime
        ).publish_sol_authorization(
            luna_batch=before,
            sol_batch_id="SOL-tampered",
            event_id="AUTH-tamper",
            authorized_at="2026-08-09T02:00:00Z",
            idempotency_key="idem-tamper",
        )
        tampered = json.loads(authorization_path.read_text(encoding="utf-8"))
        tampered["event"]["authorized_at"] = "2026-08-09T02:00:01Z"
        payload = control._json_file_bytes(tampered)
        tampered_sha = hashlib.sha256(payload).hexdigest()
        tampered_path = (
            self.store.user_sol_authorization_root / "sha256"
            / tampered_sha[:2] / f"{tampered_sha}.json"
        )
        tampered_path.parent.mkdir(parents=True, exist_ok=True)
        tampered_path.write_bytes(payload)
        with self.assertRaises(SubjectSolContractError) as invalid:
            self.store.build_authorized_batch(
                "math",
                sol_batch_id="SOL-tampered",
                authorization_receipt_sha256=tampered_sha,
            )
        self.assertEqual(invalid.exception.code, "user_sol_authorization_hmac_invalid")
        self.assertNotEqual(authorization_sha, tampered_sha)
        stale_batch = copy.deepcopy(before)
        stale_batch["batch_id"] = "LUNA-math-stale-authority"
        stale_sha, _ = IndependentUserIntentPublisher(
            self.runtime
        ).publish_sol_authorization(
            luna_batch=stale_batch,
            sol_batch_id="SOL-stale",
            event_id="AUTH-stale",
            authorized_at="2026-08-09T02:00:02Z",
            idempotency_key="idem-stale",
        )
        with self.assertRaises(SubjectSolContractError) as stale:
            self.store.build_authorized_batch(
                "math",
                sol_batch_id="SOL-stale",
                authorization_receipt_sha256=stale_sha,
            )
        self.assertEqual(stale.exception.code, "user_sol_authorization_stale")

    def test_exclusion_authorization_unknown_tampered_and_replayed_fail_closed(self) -> None:
        original = SubjectExclusionReceiptTests.closed_batch_with_rejection(self)
        task = original["tasks"][1]
        before_blockers = list(original["blocking_task_ids"])
        with self.assertRaises(SubjectSolContractError) as missing:
            self.store.issue_subject_exclusion_receipt(
                "math", authorization_receipt_sha256="b" * 64
            )
        self.assertEqual(
            missing.exception.code,
            "user_subject_exclusion_authorization_receipt_missing",
        )
        request = self.store.prepare_subject_exclusion_authority(
            "math",
            original_batch_id=original["batch_id"],
            capture_id=task["capture_id"],
            unit_sha256=task["unit_sha256"],
        )
        auth_sha, auth_path = IndependentUserIntentPublisher(
            self.runtime, exclusion=True
        ).publish_subject_exclusion_authorization(
            request=request,
            event_id="EXCLUDE-authorized",
            authorized_at="2026-08-09T03:00:00Z",
            reason="explicit exclusion",
        )
        tampered = json.loads(auth_path.read_text(encoding="utf-8"))
        tampered["reason"] = "tampered"
        payload = control._json_file_bytes(tampered)
        tampered_sha = hashlib.sha256(payload).hexdigest()
        tampered_path = (
            self.store.user_exclusion_authorization_root / "sha256"
            / tampered_sha[:2] / f"{tampered_sha}.json"
        )
        tampered_path.parent.mkdir(parents=True, exist_ok=True)
        tampered_path.write_bytes(payload)
        with self.assertRaises(SubjectSolContractError) as invalid:
            self.store.issue_subject_exclusion_receipt(
                "math", authorization_receipt_sha256=tampered_sha
            )
        self.assertEqual(
            invalid.exception.code,
            "user_subject_exclusion_authorization_hmac_invalid",
        )
        stale_request = dict(request)
        stale_request["original_batch_sha256"] = "c" * 64
        stale_sha, _ = IndependentUserIntentPublisher(
            self.runtime, exclusion=True
        ).publish_subject_exclusion_authorization(
            request=stale_request,
            event_id="EXCLUDE-stale",
            authorized_at="2026-08-09T03:00:00Z",
            reason="stale exclusion",
        )
        with self.assertRaises(SubjectSolContractError) as stale:
            self.store.issue_subject_exclusion_receipt(
                "math", authorization_receipt_sha256=stale_sha
            )
        self.assertEqual(
            stale.exception.code, "subject_exclusion_authorization_stale"
        )
        receipt = self.store.issue_subject_exclusion_receipt(
            "math", authorization_receipt_sha256=auth_sha
        )
        self.assertEqual(receipt["formal_write_count"], 0)
        with self.assertRaises(SubjectSolContractError) as replayed:
            self.store.issue_subject_exclusion_receipt(
                "math", authorization_receipt_sha256=auth_sha
            )
        self.assertEqual(
            replayed.exception.code, "subject_exclusion_authorization_replayed"
        )
        current = self.store.read_subject_batch("math")
        self.assertEqual(current["blocking_task_ids"], before_blockers)
        self.assertFalse(current["sol_ready"])

    def test_invalid_failed_apply_keeps_global_lease_safe_paused(self) -> None:
        self.ready_batch("math")
        batch, _ = self.authorize("math")
        claim = self.store.begin_sol_review(
            "math", batch["batch_id"], owner_id="sim-sol"
        )
        review, _ = self.approve_review(batch, claim)
        core = {
            "batch_id": batch["batch_id"],
            "subject": "math",
            "fencing_token": claim["fencing_token"],
            "writer": "sol",
            "writer_adapter": SUBJECT_WRITER_ADAPTERS["math"],
            "sol_review_receipt_sha256": control._document_sha256(review),
            "pre_state_sha256": digest("pre-failed"),
            "post_state_sha256": digest("post-failed"),
            "operations": [],
            "writer_process": {
                "adapter_run_id": "sim-failed",
                "pid": 2222,
                "terminal_state": "stopped",
                "exit_code": 1,
                "stopped_at": "2026-08-09T02:10:00Z",
                "evidence_sha256": digest("process-stopped"),
            },
            "transaction_end": {
                "transaction_id": "txn-failed",
                "state": "failed",
                "ended_at": "2026-08-09T02:09:59Z",
                "evidence_sha256": digest("txn-ended"),
            },
            "recovery_position_sha256": None,
            "rollback_receipt_sha256": None,
            "status": "failed",
            "dispatcher_effect": {
                "scope": "subject", "subject": "math",
                "paused_subjects": [], "drained_subjects": [],
                "other_subjects_unchanged": True,
            },
            "formal_write_count": 0,
            "completed_at": "2026-08-09T02:10:01Z",
        }
        core["recovery_position_sha256"] = digest("recovery-position")
        apply_sha = self.publish_isolated_apply(batch, core)
        apply_receipt = self.store._read_content_addressed(
            self.store.writer_apply_receipt_root,
            apply_sha,
            "test_apply_receipt",
        )
        execution = self.store._read_content_addressed(
            self.store.writer_execution_result_root,
            apply_receipt["execution_result_sha256"],
            "test_execution_result",
        )
        recovery_sha = execution["recovery_position_artifact_sha256"]
        recovery_path = (
            self.store.writer_artifact_root / "recovery-position" / "sha256"
            / recovery_sha[:2] / f"{recovery_sha}.json"
        )
        recovery_path.unlink()
        with self.assertRaises(SubjectSolContractError) as invalid:
            self.store.finish_subject_commit("math", apply_sha)
        self.assertEqual(
            invalid.exception.code, "isolated_writer_recovery-position_artifact_missing"
        )
        global_state = self.store.read_global()
        self.assertEqual(global_state["active_writer_count"], 1)
        self.assertEqual(global_state["active_writer"]["status"], "safe_paused")
        self.assertEqual(
            self.store.read_subject("math")["writer_state"]["handoff_status"],
            "safe_paused",
        )
        valid_sha = self.publish_isolated_apply(batch, core)
        released = self.store.finish_subject_commit("math", valid_sha)
        self.assertEqual(released["global"]["active_writer_count"], 0)
        self.assertEqual(released["subject"]["handoff_status"], "safe_paused")
        self.assertEqual(released["formal_write_count"], 0)

    def test_stale_signed_apply_fence_keeps_lease_safe_paused(self) -> None:
        self.ready_batch("english")
        batch, _ = self.authorize("english")
        claim = self.store.begin_sol_review(
            "english", batch["batch_id"], owner_id="sim-sol"
        )
        review, _ = self.approve_review(batch, claim)
        with self.assertRaises(SubjectSolContractError) as caught:
            self.complete_without_write(
                batch,
                claim,
                review,
                fencing_token=claim["fencing_token"] + 1,
            )
        self.assertEqual(caught.exception.code, "stale_global_sol_fence")
        state = self.store.read_global()
        self.assertEqual(state["active_writer_count"], 1)
        self.assertEqual(state["active_writer"]["status"], "safe_paused")
        self.assertEqual(
            self.store.read_subject("english")["writer_state"]["handoff_status"],
            "safe_paused",
        )

    def test_dashboard_derives_corrected_from_verified_quality_receipt(self) -> None:
        batch = self.create_batch("math", count=1)
        batch = self.store.freeze_subject_batch("math", batch["batch_id"])
        batch = self.store._record_quality_receipt(
            self.quality(batch, 0, outcome="corrected")
        )
        projected = _subject_batch_state(
            self.runtime, "math", batch["study_date"]
        )
        assert projected is not None
        self.assertEqual(projected["tasks"][0]["quality_outcome"], "corrected")
        self.assertTrue(projected["tasks"][0]["quality_receipt_verified"])

    def test_control_module_contains_no_formal_writer_import_or_invocation(self) -> None:
        source = inspect.getsource(control)
        self.assertNotIn("import math_nightly_writer", source)
        self.assertNotIn("import cs408_daily_intake_writer", source)
        self.assertNotIn("import english_daily_intake_writer", source)
        self.assertNotIn("subprocess", source)

    def test_v1_single_package_batch_is_not_accepted(self) -> None:
        with self.assertRaisesRegex(SubjectSolContractError, "daily_sol_batch_shape_invalid"):
            validate_daily_sol_batch_v2(
                {
                    "schema_version": "daily_sol_batch_v1",
                    "batch_id": "legacy",
                    "subject": "math",
                }
            )


if __name__ == "__main__":
    unittest.main()
