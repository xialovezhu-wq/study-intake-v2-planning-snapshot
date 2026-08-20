from __future__ import annotations

import hashlib
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from preprocess_dispatcher import build_parser  # noqa: E402
import preprocessor_core as core  # noqa: E402
import subject_sol_contract as control  # noqa: E402
from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    FrozenTask,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


class SuccessfulDispatcherRunner:
    """Two-stage zero-model runner used only to publish real dispatch authority."""

    @staticmethod
    def _result(stage: str, task: FrozenTask) -> StageResult:
        return StageResult(
            payload={"stage": stage, "unit_sha256": task.unit_sha256},
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            duration_ms=1,
        )

    def run_analysis(self, task: FrozenTask, _context: object) -> StageResult:
        return self._result("analysis", task)

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: dict,
        _context: object,
    ) -> StageResult:
        if draft_analysis.get("stage") != "analysis":
            raise AssertionError("critical review did not receive analysis draft")
        return self._result("critical_review", task)

    def cancel(self, _context: object) -> None:
        return None


class HardenedSubjectControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.store = SubjectSolRuntimeStore(self.runtime)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def prepare_v1_batch(
        self,
        *,
        tasks: list[dict[str, object]],
        study_date: str = "2026-08-09",
    ) -> dict:
        """Publish an explicit historical v1 fixture for the v1 hardening suite."""

        rows = [
            {
                **task,
                "status": "selected",
                "proposal_sha256": None,
                "package_sha256": None,
                "quality_receipt_sha256": None,
                "terminal_receipt_sha256": None,
                "error_code": None,
            }
            for task in tasks
        ]
        value = {
            "schema_version": "subject_luna_batch_v1",
            "batch_id": "MATH-FROZEN-1",
            "subject": "math",
            "study_date": study_date,
            "status": "frozen",
            "capture_high_watermark": "scan-hwm-9",
            "scan_snapshot_sha256": digest("scan-snapshot"),
            "authority_generation": "math-generation-1",
            "authority_fingerprint": digest("math-authority-1"),
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
        writer = self.store._read_writer_locked("math")
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

    def test_generation_ack_cli_accepts_only_signed_authority_digest(self) -> None:
        parser = build_parser()
        value = digest("authority-ack")
        args = parser.parse_args(
            [
                "--subject", "math", "sol-ack-generation",
                "--authority-ack-sha256", value,
            ]
        )
        self.assertEqual(args.authority_ack_sha256, value)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    [
                        "--subject", "math", "sol-ack-generation",
                        "--generation", "math-generation-2",
                    ]
                )
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    [
                        "--subject", "math", "sol-ack-generation",
                        "--authority-ack-sha", value,
                    ]
                )

    def test_sol_authority_cli_accepts_only_content_addressed_receipts(self) -> None:
        parser = build_parser()
        digest_value = digest("authority")
        authorize = parser.parse_args(
            [
                "--subject", "math", "sol-authorize",
                "--sol-batch-id", "SOL-MATH-1",
                "--authorization-receipt-sha256", digest_value,
            ]
        )
        self.assertEqual(authorize.authorization_receipt_sha256, digest_value)
        review = parser.parse_args(
            ["--subject", "math", "sol-review", "--receipt-sha256", digest_value]
        )
        self.assertEqual(review.receipt_sha256, digest_value)
        finish = parser.parse_args(
            [
                "--subject", "math", "sol-finish",
                "--writer-apply-receipt-sha256", digest_value,
            ]
        )
        self.assertEqual(finish.writer_apply_receipt_sha256, digest_value)
        for legacy in (
            ["--subject", "math", "sol-authorize", "--batch", "x.json"],
            ["--subject", "math", "sol-review", "--receipt", "x.json"],
            ["--subject", "math", "sol-finish", "--receipt", "x.json"],
            [
                "--subject", "math", "sol-authorize", "--sol-bat", "SOL-1",
                "--authorization-receipt-sha256", digest_value,
            ],
            [
                "--subject", "math", "sol-exclude",
                "--authorization-receipt-sha", digest_value,
            ],
            ["--subject", "math", "sol-begin", "--bat", "SOL-1"],
        ):
            with self.subTest(legacy=legacy):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        parser.parse_args(legacy)

    def prepare(self, *, dates: tuple[str, ...] = ("2026-08-09",)) -> dict:
        tasks = []
        for index, study_date in enumerate(dates):
            frozen = {
                "subject": "math",
                "capture_id": f"MATH-{index}",
                "study_date": study_date,
                "input_fingerprint": digest(f"input-{index}"),
            }
            tasks.append(
                {
                    "capture_id": frozen["capture_id"],
                    "unit_sha256": digest(f"unit-{index}"),
                    "input_fingerprint": frozen["input_fingerprint"],
                    "study_date": study_date,
                    "frozen_payload_sha256": control._value_sha256(frozen),
                }
            )
        return self.prepare_v1_batch(tasks=tasks)

    def test_task_progress_is_monotonic_and_terminal_cannot_reopen(self) -> None:
        batch = self.prepare()
        task = batch["tasks"][0]
        common = {
            "subject": "math",
            "batch_id": batch["batch_id"],
            "capture_id": task["capture_id"],
            "unit_sha256": task["unit_sha256"],
        }
        for status in (
            "claimed",
            "analysis_running",
            "critical_review_running",
        ):
            projected = self.store.record_task_progress(
                **common, status=status
            )
            self.assertEqual(projected["tasks"][0]["status"], status)
        replayed = self.store.record_task_progress(
            **common, status="analysis_running"
        )
        self.assertEqual(
            replayed["tasks"][0]["status"], "critical_review_running"
        )
        terminal = self.store.record_task_terminal_failure(
            **common,
            status="failed",
            error_code="GENERATION_MISMATCH",
        )
        self.assertEqual(terminal["tasks"][0]["status"], "failed")
        after_terminal = self.store.record_task_progress(
            **common, status="critical_review_running"
        )
        self.assertEqual(after_terminal["tasks"][0]["status"], "failed")
        self.assertTrue(after_terminal["all_terminal"])

    def successful_authority(
        self,
        *,
        outcome: str,
        group_member_capture_id: str | None = None,
    ) -> tuple[dict, dict]:
        owner_payload = {
            "subject": "math",
            "capture_id": "MATH-0",
            "study_date": "2026-08-09",
            "input_fingerprint": digest("input-0"),
            "input_binding": {"fixture": "subject-sol-quality-hardening"},
            "model_input": {"question": "source-backed test fixture"},
            "allowed_evidence_refs": [],
            "image_paths": [],
        }
        frozen = {
            **owner_payload,
            "dispatch_contract": {
                "schema_version": "study-intake-dispatch-release-binding-v1",
                "release_id": core.LOADED_CORE_SHA256,
                "loaded_core_sha256": core.LOADED_CORE_SHA256,
            },
        }
        if group_member_capture_id is not None:
            member_payload = {
                **owner_payload,
                "capture_id": group_member_capture_id,
                "input_fingerprint": digest(
                    f"input:{group_member_capture_id}"
                ),
            }
            content_processing_id = digest("two-member-content-processing")
            rule_version = "subject-sol-hardening-group-v1"
            rule_version_sha256 = digest(rule_version)
            frozen.update(
                {
                    "content_processing_id": content_processing_id,
                    "content_group_members": [owner_payload, member_payload],
                    "content_group_capture_ids": [
                        owner_payload["capture_id"],
                        member_payload["capture_id"],
                    ],
                    "math_group_members": [owner_payload, member_payload],
                }
            )
            frozen["dispatch_contract"].update(
                {
                    "rule_version": rule_version,
                    "rule_version_sha256": rule_version_sha256,
                    "subject_processing_contract_sha256": digest(
                        "math-subject-processing-contract"
                    ),
                    "content_processing_id": content_processing_id,
                    "math_group_processing_key": digest(
                        "two-member-math-group"
                    ),
                    "math_group_capture_ids": [
                        owner_payload["capture_id"],
                        member_payload["capture_id"],
                    ],
                }
            )
        dispatch_task = FrozenTask(frozen)
        batch = self.prepare_v1_batch(
            tasks=[
                {
                    "capture_id": frozen["capture_id"],
                    "unit_sha256": dispatch_task.unit_sha256,
                    "input_fingerprint": frozen["input_fingerprint"],
                    "study_date": frozen["study_date"],
                    "frozen_payload_sha256": dispatch_task.frozen_payload_sha256,
                }
            ]
        )
        task = batch["tasks"][0]
        capture_sha, _ = self.store._publish_immutable_value(
            self.store.dispatch_root / "capture-freeze-receipts",
            {"capture_id": task["capture_id"], "frozen": True},
        )
        session_sha, _ = self.store._publish_immutable_value(
            self.store.dispatch_root / "mcp-read-session-receipts",
            {"capture_id": task["capture_id"], "complete": True},
        )
        transcript_shas = []
        for stage in ("analysis", "critical_review"):
            payload = control._json_file_bytes({"stage": stage, "calls": ["mcp"]})
            transcript_sha = hashlib.sha256(payload).hexdigest()
            path = (
                self.runtime / "private/reports/mcp-stage-transcripts/sha256"
                / transcript_sha[:2] / f"{transcript_sha}.json"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            transcript_shas.append(transcript_sha)
        proposal = {
            "schema_version": "luna_proposal_v2",
            "subject": "math",
            "capture_id": task["capture_id"],
            "critical_review_outcome": outcome,
            "review_status": "proposal_ready",
        }
        proposal_sha = control._value_sha256(proposal)
        report_payload = control._json_file_bytes(
            {
                "schema_version": "math_processing_report_v2",
                "subject": "math",
                "capture_id": task["capture_id"],
                "analysis": {
                    "evidence_assessment": {"completeness": "complete"},
                    "sol_verification_plan": {
                        "recommended_disposition": "ready_for_independent_review"
                    },
                },
            }
        )
        report_sha = hashlib.sha256(report_payload).hexdigest()
        report_path = (
            self.runtime / "private/reports/objects" / f"{report_sha}.json"
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_bytes(report_payload)
        subject_package = {
            "subject": "math",
            "capture_id": task["capture_id"],
            "study_date": task["study_date"],
            "input_fingerprint": task["input_fingerprint"],
            "evidence_generation": batch["authority_generation"],
            "evidence_authority_fingerprint": batch["authority_fingerprint"],
            "capture_freeze_receipt_sha256": capture_sha,
            "mcp_read_session_receipt_sha256": session_sha,
            "stage_receipts": {
                "analysis": {"mcp_transcript_sha256": transcript_shas[0]},
                "critical_review": {"mcp_transcript_sha256": transcript_shas[1]},
            },
            "luna_proposal": proposal,
            "luna_proposal_sha256": proposal_sha,
            "report_json_sha256": report_sha,
            "formal_write_count": 0,
        }
        subject_payload = control._json_file_bytes(subject_package)
        subject_sha = hashlib.sha256(subject_payload).hexdigest()
        subject_path = self.runtime / "packages/objects" / f"{subject_sha}.json"
        subject_path.parent.mkdir(parents=True, exist_ok=True)
        subject_path.write_bytes(subject_payload)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: SuccessfulDispatcherRunner(),
            stage_timeout_seconds=2,
        )
        result = dispatcher.submit(dispatch_task).wait(5)
        self.assertEqual(result.outcome, "succeeded")
        verified = dispatcher.lease_store.verify_authoritative_completion(
            "math",
            task["capture_id"],
            expected_release_id=core.LOADED_CORE_SHA256,
            expected_unit_sha256=task["unit_sha256"],
            expected_input_fingerprint=task["input_fingerprint"],
        )
        self.assertEqual(
            verified["completion"]["authority"]["schema_version"],
            "study-intake-dispatch-authority-v1",
        )
        self.assertNotIn("seal", verified["completion"])
        publication = {
            "subject_package_sha256": subject_sha,
            "luna_proposal_sha256": proposal_sha,
            "critical_review_outcome": outcome,
            "capture_freeze_receipt_sha256": capture_sha,
            "mcp_read_session_receipt_sha256": session_sha,
            "evidence_generation": batch["authority_generation"],
            "evidence_authority_fingerprint": batch["authority_fingerprint"],
        }
        return verified, publication

    def close(self, *, outcome: str) -> dict:
        verified, publication = self.successful_authority(outcome=outcome)
        self.store.record_verified_luna_completion("math", verified)
        config = {"runtime_root": str(self.runtime)}
        with mock.patch(
            "preprocessor_core.reopen_verified_subject_publication",
            return_value=publication,
        ):
            closure = self.store.build_verified_completion_quality_closure(
                "math", verified, config=config
            )
            return self.store.close_verified_completion(
                "math", closure=closure, config=config
            )

    def test_accepted_and_corrected_success_close(self) -> None:
        for outcome in ("accepted", "corrected"):
            with self.subTest(outcome=outcome):
                if outcome == "corrected":
                    self.tearDown()
                    self.setUp()
                batch = self.close(outcome=outcome)
                self.assertTrue(batch["sol_ready"])
                self.assertEqual(batch["tasks"][0]["status"], "quality_passed")
                self.assertEqual(batch["formal_write_count"], 0)

    def test_two_member_group_alias_reopens_owner_but_quality_closes_once(
        self,
    ) -> None:
        member_capture_id = "MATH-1"
        owner_verified, publication = self.successful_authority(
            outcome="accepted",
            group_member_capture_id=member_capture_id,
        )
        member_verified = LeaseStore(
            self.runtime
        ).verify_authoritative_completion(
            "math",
            member_capture_id,
            expected_release_id=core.LOADED_CORE_SHA256,
            expected_unit_sha256=owner_verified["completion"]["unit_sha256"],
            expected_input_fingerprint=digest(
                f"input:{member_capture_id}"
            ),
        )
        self.assertEqual(
            member_verified["latest"]["capture_id"], member_capture_id
        )
        self.assertEqual(
            member_verified["latest"]["group_owner_capture_id"], "MATH-0"
        )
        self.assertEqual(
            member_verified["completion"]["capture_id"], "MATH-0"
        )
        self.assertEqual(
            member_verified["latest"]["completion_sha256"],
            owner_verified["latest"]["completion_sha256"],
        )
        self.assertEqual(
            member_verified["completion"]["package_sha256"],
            owner_verified["completion"]["package_sha256"],
        )

        projected = self.store.record_verified_luna_completion(
            "math", owner_verified
        )
        self.assertEqual(len(projected["subject_luna_batch"]["tasks"]), 1)
        self.assertEqual(
            projected["subject_luna_batch"]["tasks"][0]["capture_id"],
            "MATH-0",
        )
        config = {"runtime_root": str(self.runtime)}
        with mock.patch(
            "preprocessor_core.reopen_verified_subject_publication",
            return_value=publication,
        ):
            closure = self.store.build_verified_completion_quality_closure(
                "math", owner_verified, config=config
            )
            closed = self.store.close_verified_completion(
                "math", closure=closure, config=config
            )
            with self.assertRaises(SubjectSolContractError) as repeat_error:
                self.store.close_verified_completion(
                    "math", closure=closure, config=config
                )
        self.assertEqual(repeat_error.exception.code, "quality_task_not_pending")
        self.assertTrue(closed["sol_ready"])
        self.assertEqual(closed["tasks"][0]["status"], "quality_passed")

        replayed = self.store.record_verified_luna_completion(
            "math", member_verified
        )
        self.assertEqual(len(replayed["subject_luna_batch"]["tasks"]), 1)
        self.assertEqual(
            replayed["subject_luna_batch"]["tasks"][0]["capture_id"],
            "MATH-0",
        )
        with self.assertRaises(SubjectSolContractError) as member_error:
            self.store.build_verified_completion_quality_closure(
                "math", member_verified, config=config
            )
        self.assertEqual(member_error.exception.code, "quality_task_not_pending")
        quality_receipts = list(
            (self.store.receipt_root / "subject-quality").rglob("*.json")
        )
        self.assertEqual(len(quality_receipts), 1)

    def test_critical_review_reject_becomes_needs_rework_without_package(self) -> None:
        batch = self.prepare()
        task = batch["tasks"][0]
        verified = {
            "latest": {"subject": "math"},
            "completion": {
                "subject": "math", "capture_id": task["capture_id"],
                "unit_sha256": task["unit_sha256"], "outcome": "failed",
                "error_code": "math_critical_review_rejected",
            },
            "package": None,
        }
        state = self.store.record_verified_luna_completion("math", verified)
        closed = state["subject_luna_batch"]["tasks"][0]
        self.assertEqual(closed["status"], "needs_rework")
        self.assertIsNone(closed["package_sha256"])
        self.assertIsNotNone(closed["terminal_receipt_sha256"])

    def test_mixed_date_batch_preserves_exact_task_dates(self) -> None:
        batch = self.prepare(dates=("2026-08-07", "2026-08-08", "2026-08-09"))
        self.assertEqual(batch["study_date"], "2026-08-09")
        self.assertEqual(
            [row["study_date"] for row in batch["tasks"]],
            ["2026-08-07", "2026-08-08", "2026-08-09"],
        )

    def test_projection_tamper_is_rejected_by_hmac_pointer(self) -> None:
        self.prepare()
        path = self.runtime / "dispatch/state/subject-luna-batches/math.json"
        value = json.loads(path.read_text())
        value["capture_high_watermark"] = "tampered"
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        with self.assertRaises(SubjectSolContractError) as caught:
            self.store.read_subject_batch("math")
        self.assertEqual(caught.exception.code, "subject_luna_batch_pointer_mismatch")

    def test_public_quality_signer_is_disabled(self) -> None:
        with self.assertRaises(SubjectSolContractError) as caught:
            self.store.issue_quality_receipt({})
        self.assertEqual(caught.exception.code, "public_quality_signer_disabled")

    def test_arbitrary_generation_string_is_not_an_ack(self) -> None:
        with self.assertRaises(TypeError):
            self.store.acknowledge_subject_generation(
                "math", next_generation="arbitrary"  # type: ignore[call-arg]
            )

    def test_outer_and_inner_hash_tamper_fail_closed(self) -> None:
        expected_codes = {
            "outer": "quality_dispatch_authority_binding_mismatch",
            "inner": "quality_package_hash_mismatch",
        }
        for target in ("outer", "inner"):
            with self.subTest(target=target):
                if target == "inner":
                    self.tearDown()
                    self.setUp()
                verified, publication = self.successful_authority(outcome="accepted")
                self.store.record_verified_luna_completion("math", verified)
                config = {"runtime_root": str(self.runtime)}
                with mock.patch(
                    "preprocessor_core.reopen_verified_subject_publication",
                    return_value=publication,
                ):
                    closure = self.store.build_verified_completion_quality_closure(
                        "math", verified, config=config
                    )
                    if target == "outer":
                        closure["dispatch_package_sha256"] = digest("tampered-outer")
                    else:
                        package_path = (
                            self.runtime / "packages/objects"
                            / f"{closure['package_sha256']}.json"
                        )
                        package_path.write_bytes(package_path.read_bytes() + b" ")
                    with self.assertRaises(SubjectSolContractError) as caught:
                        self.store.close_verified_completion(
                            "math", closure=closure, config=config
                        )
                    self.assertEqual(caught.exception.code, expected_codes[target])

    def test_each_dispatch_closure_digest_tamper_stays_quality_pending(
        self,
    ) -> None:
        fields = (
            "completion_sha256",
            "dispatch_receipt_sha256",
            "dispatch_package_sha256",
        )
        for index, field in enumerate(fields):
            with self.subTest(field=field):
                if index:
                    self.tearDown()
                    self.setUp()
                verified, publication = self.successful_authority(
                    outcome="accepted"
                )
                projected = self.store.record_verified_luna_completion(
                    "math", verified
                )
                self.assertEqual(
                    projected["subject_luna_batch"]["tasks"][0]["status"],
                    "quality_pending",
                )
                config = {"runtime_root": str(self.runtime)}
                with mock.patch(
                    "preprocessor_core.reopen_verified_subject_publication",
                    return_value=publication,
                ):
                    closure = self.store.build_verified_completion_quality_closure(
                        "math", verified, config=config
                    )
                    closure[field] = digest(f"tampered:{field}")
                    with self.assertRaises(SubjectSolContractError) as caught:
                        self.store.close_verified_completion(
                            "math", closure=closure, config=config
                        )
                self.assertEqual(
                    caught.exception.code,
                    "quality_dispatch_authority_binding_mismatch",
                )
                pending = self.store.read_subject_batch("math")
                self.assertIsNotNone(pending)
                self.assertEqual(pending["tasks"][0]["status"], "quality_pending")
                self.assertIsNone(
                    pending["tasks"][0]["quality_receipt_sha256"]
                )

    def test_concurrent_dispatch_import_failure_maps_to_stable_quality_error(
        self,
    ) -> None:
        verified, publication = self.successful_authority(outcome="accepted")
        self.store.record_verified_luna_completion("math", verified)
        config = {"runtime_root": str(self.runtime)}
        real_import = __import__

        def import_with_dispatch_failure(
            name: str,
            globals_value: object = None,
            locals_value: object = None,
            fromlist: object = (),
            level: int = 0,
        ) -> object:
            if name == "concurrent_dispatch":
                raise ImportError("sealed concurrent_dispatch import failure")
            return real_import(
                name,
                globals_value,
                locals_value,
                fromlist,
                level,
            )

        with mock.patch(
            "preprocessor_core.reopen_verified_subject_publication",
            return_value=publication,
        ):
            closure = self.store.build_verified_completion_quality_closure(
                "math", verified, config=config
            )
            with mock.patch(
                "builtins.__import__",
                side_effect=import_with_dispatch_failure,
            ):
                with self.assertRaises(SubjectSolContractError) as caught:
                    self.store.close_verified_completion(
                        "math", closure=closure, config=config
                    )
        self.assertEqual(
            caught.exception.code,
            "quality_dispatch_authority_invalid",
        )
        self.assertEqual(caught.exception.diagnostic.get("reason"), "ImportError")
        pending = self.store.read_subject_batch("math")
        self.assertIsNotNone(pending)
        self.assertEqual(pending["tasks"][0]["status"], "quality_pending")
        self.assertIsNone(pending["tasks"][0]["quality_receipt_sha256"])

    def test_subject_sol_private_sealed_completion_is_rejected(
        self,
    ) -> None:
        verified, publication = self.successful_authority(outcome="accepted")
        self.store.record_verified_luna_completion("math", verified)
        config = {"runtime_root": str(self.runtime)}
        with mock.patch(
            "preprocessor_core.reopen_verified_subject_publication",
            return_value=publication,
        ):
            closure = self.store.build_verified_completion_quality_closure(
                "math", verified, config=config
            )
            completion = dict(verified["completion"])
            completion.pop("authority", None)
            fake_completion = self.store._seal(
                completion,
                purpose="dispatch-completion",
            )
            completion_path = (
                self.runtime / "dispatch/state/completions"
                / f"{closure['unit_sha256']}.json"
            )
            self.store._atomic_json(completion_path, fake_completion)

            # Keep the outer pointer under genuine Dispatcher authority so
            # rejection reaches the SubjectSol-shaped completion itself.
            dispatcher_store = LeaseStore(self.runtime)
            latest = dict(verified["latest"])
            latest.pop("authority", None)
            latest["completion_sha256"] = hashlib.sha256(
                completion_path.read_bytes()
            ).hexdigest()
            genuine_latest = dispatcher_store._seal(
                latest,
                purpose="dispatch-latest",
            )
            latest_path = dispatcher_store._latest_path("math", "MATH-0")
            self.store._atomic_json(latest_path, genuine_latest)
            with self.assertRaises(SubjectSolContractError) as caught:
                self.store.close_verified_completion(
                    "math", closure=closure, config=config
                )
        self.assertEqual(
            caught.exception.code,
            "quality_dispatch_authority_invalid",
        )
        self.assertEqual(
            caught.exception.diagnostic.get("reason"),
            "authority_proof_missing",
        )

    def test_quality_pending_crash_resume_closes_same_frozen_task(self) -> None:
        verified, publication = self.successful_authority(outcome="accepted")
        first = self.store.record_verified_luna_completion("math", verified)
        self.assertEqual(
            first["subject_luna_batch"]["tasks"][0]["status"], "quality_pending"
        )
        resumed = SubjectSolRuntimeStore(self.runtime)
        config = {"runtime_root": str(self.runtime)}
        with mock.patch(
            "preprocessor_core.reopen_verified_subject_publication",
            return_value=publication,
        ):
            closure = resumed.build_verified_completion_quality_closure(
                "math", verified, config=config
            )
            closed = resumed.close_verified_completion(
                "math", closure=closure, config=config
            )
        self.assertTrue(closed["sol_ready"])
        self.assertEqual(closed["tasks"][0]["status"], "quality_passed")


if __name__ == "__main__":
    unittest.main()
