from __future__ import annotations

import copy
import hashlib
import hmac
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from mixed_luna_stress import (  # noqa: E402
    MixedLunaStressError,
    RUN_COUNTS,
    SELECTION_SCHEMA,
    STAGE_ORDER,
    _validate_summary_core,
    run_mixed_stress,
    verify_summary,
)


class MixedLunaStressTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="mixed-luna-stress-")
        self.runtime = Path(self.temporary.name) / "runtime"
        self.runtime.mkdir()
        self.key_path = Path(self.temporary.name) / "authority.key"
        self.key = b"m" * 32
        self.key_path.write_bytes(self.key)
        os.chmod(self.key_path, 0o600)
        self.scopes = {
            "english": "1" * 64,
            "math": "2" * 64,
            "cs408": "3" * 64,
        }
        self.runner_calls = 0
        self.authority_calls: list[str] = []
        self.sol_state = {
            "schema_version": "global_sol_writer_v1",
            "revision": 0,
            "next_fencing_token": 1,
            "queue": [],
            "active_writer": None,
            "active_writer_count": 0,
            "formal_write_count": 0,
            "updated_at": None,
        }

        class FakeProcessingHost:
            def __init__(host_self, owner: "MixedLunaStressTests") -> None:
                host_self.owner = owner

            def subject_authority_snapshot(
                host_self, subject: str
            ) -> dict[str, str]:
                host_self.owner.authority_calls.append(subject)
                return host_self.owner.authority(subject)

        self.processing_host = FakeProcessingHost(self)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def selection(self, mode: str, attempt: int) -> dict:
        tasks = []
        ordinal = 1
        for subject in ("english", "math", "cs408"):
            for index in range(1, RUN_COUNTS[mode][subject] + 1):
                task_id = f"{mode}-{attempt}-{subject}-{index:03d}"
                tasks.append(
                    {
                        "ordinal": ordinal,
                        "task_id": task_id,
                        "unit_sha256": hashlib.sha256(task_id.encode()).hexdigest(),
                        "subject": subject,
                        "subject_scope_sha256": self.scopes[subject],
                        "subject_batch_id": f"{subject}-batch-attempt-{attempt}",
                        "subject_batch_sha256": hashlib.sha256(
                            f"{subject}-batch-{attempt}".encode()
                        ).hexdigest(),
                        "dispatcher_id": f"dispatcher-{subject}",
                        "mcp_namespace": f"kaoyan_{subject}_read",
                        "generation": f"candidate-generation-{subject}",
                        "authority_fingerprint": hashlib.sha256(
                            f"authority-{subject}".encode()
                        ).hexdigest(),
                        "read_session_binding_sha256": hashlib.sha256(
                            f"read-session-binding:{task_id}".encode()
                        ).hexdigest(),
                        "sol_authorized": False,
                        "formal_write_count": 0,
                    }
                )
                ordinal += 1
        return {
            "schema_version": SELECTION_SCHEMA,
            "campaign_id": "THREE-SUBJECT-LUNA-STRESS-001",
            "candidate_release_id": "a" * 64,
            "run_mode": mode,
            "execution_attempt": attempt,
            "execution_runtime_sha256": hashlib.sha256(
                f"execution-runtime:{attempt}".encode()
            ).hexdigest(),
            "max_workers": sum(RUN_COUNTS[mode].values()),
            "subject_scopes": dict(self.scopes),
            "tasks": tasks,
            "sol_enabled": False,
            "formal_write_count": 0,
        }

    def authority(self, subject: str) -> dict[str, str]:
        return {
            "mcp_namespace": f"kaoyan_{subject}_read",
            "generation": f"candidate-generation-{subject}",
            "authority_fingerprint": hashlib.sha256(
                f"authority-{subject}".encode()
            ).hexdigest(),
        }

    def _digest(self, task_id: str, label: str) -> str:
        return hashlib.sha256(f"{task_id}:{label}".encode()).hexdigest()

    def runner(self, task: dict, observe: object) -> dict:
        self.runner_calls += 1
        for event in STAGE_ORDER:
            observe(event)  # type: ignore[operator]
        task_id = task["task_id"]
        return {
            "status": "quality_passed",
            "quality_outcome": "accepted",
            "proposal_action": (
                "proposal_ready"
                if task["subject"] == "english"
                else "subject_proposal_ready"
            ),
            "subject": task["subject"],
            "task_id": task_id,
            "unit_sha256": task["unit_sha256"],
            "subject_batch_id": task["subject_batch_id"],
            "dispatcher_id": task["dispatcher_id"],
            "mcp_namespace": task["mcp_namespace"],
            "generation": task["generation"],
            "read_session_binding_sha256": task[
                "read_session_binding_sha256"
            ],
            "read_session_id": f"read-session-{task_id}",
            "read_session_manifest_sha256": self._digest(task_id, "manifest"),
            "analysis_transcript_sha256": self._digest(task_id, "analysis-transcript"),
            "analysis_stage_receipt_sha256": self._digest(task_id, "analysis-receipt"),
            "analysis_stage_receipt_hmac_sha256": hmac.new(
                self.key, f"{task_id}:analysis".encode(), hashlib.sha256
            ).hexdigest(),
            "critical_review_transcript_sha256": self._digest(
                task_id, "critical-transcript"
            ),
            "critical_review_stage_receipt_sha256": self._digest(
                task_id, "critical-receipt"
            ),
            "critical_review_stage_receipt_hmac_sha256": hmac.new(
                self.key, f"{task_id}:critical_review".encode(), hashlib.sha256
            ).hexdigest(),
            "final_read_session_receipt_sha256": self._digest(task_id, "final-session"),
            "package_sha256": self._digest(task_id, "package"),
            "quality_receipt_sha256": self._digest(task_id, "quality"),
            "model_call_count": 2,
            "formal_write_count": 0,
        }

    def verifier(self, task: dict, result: dict) -> None:
        task_id = task["task_id"]
        expected_analysis = hmac.new(
            self.key, f"{task_id}:analysis".encode(), hashlib.sha256
        ).hexdigest()
        expected_critical = hmac.new(
            self.key, f"{task_id}:critical_review".encode(), hashlib.sha256
        ).hexdigest()
        if result["analysis_stage_receipt_hmac_sha256"] != expected_analysis:
            raise MixedLunaStressError("analysis_stage_hmac_invalid")
        if result["critical_review_stage_receipt_hmac_sha256"] != expected_critical:
            raise MixedLunaStressError("critical_stage_hmac_invalid")

    def _assert_subject_failure_is_local(
        self,
        *,
        failing_subject: str,
        error_code: str,
        failure_kind: str,
    ) -> None:
        runtime = self.runtime / f"local-failure-{failing_subject}-{failure_kind}"
        runtime.mkdir()
        all_entered = threading.Event()
        failure_seen = threading.Event()
        release_siblings = threading.Event()
        lock = threading.Lock()
        entered: set[str] = set()
        runner_terminals: set[str] = set()
        outcome: list[tuple[str, Path, dict]] = []
        campaign_errors: list[BaseException] = []

        def local_failure_runner(task: dict, observe: object) -> dict:
            subject = task["subject"]
            with lock:
                entered.add(subject)
                if entered == {"english", "math", "cs408"}:
                    all_entered.set()
            try:
                if subject == failing_subject:
                    if failure_kind in {"provider", "mcp"}:
                        observe("analysis_submitted")  # type: ignore[operator]
                        failure_seen.set()
                        raise MixedLunaStressError(error_code)
                    result = self.runner(task, observe)
                    result["quality_outcome"] = "rejected"
                    result["proposal_action"] = "conflict"
                    failure_seen.set()
                    return result
                if not release_siblings.wait(5):
                    raise AssertionError("subject-local sibling release timed out")
                return self.runner(task, observe)
            finally:
                with lock:
                    runner_terminals.add(subject)

        def campaign() -> None:
            try:
                outcome.append(
                    run_mixed_stress(
                        self.selection("smoke_3", 1),
                        runtime_root=runtime,
                        authority_key_path=self.key_path,
                        processing_host=self.processing_host,
                        runner=local_failure_runner,
                        artifact_verifier=self.verifier,
                        sol_state_reader=lambda: dict(self.sol_state),
                        barrier_timeout_seconds=5,
                    )
                )
            except BaseException as exc:  # pragma: no cover - assertion aid
                campaign_errors.append(exc)

        thread = threading.Thread(target=campaign, name="mixed-local-failure-test")
        thread.start()
        try:
            self.assertTrue(all_entered.wait(5))
            self.assertTrue(failure_seen.wait(5))
            summary_root = runtime / "dispatch" / "mixed-luna-stress" / "summaries"
            self.assertFalse(summary_root.exists())
        finally:
            release_siblings.set()
        thread.join(10)
        self.assertFalse(thread.is_alive())
        if campaign_errors:
            raise campaign_errors[0]
        self.assertEqual({"english", "math", "cs408"}, runner_terminals)
        self.assertEqual(1, len(outcome))
        digest, path, summary = outcome[0]
        self.assertTrue(path.is_file())
        self.assertEqual("failed_closed", summary["status"])
        self.assertEqual(3, summary["barrier_arrived"])
        self.assertEqual(3, summary["global_peak_active"])
        self.assertEqual(
            {"english": 1, "math": 1, "cs408": 1},
            summary["per_subject_peak_active"],
        )
        results = {row["subject"]: row for row in summary["results"]}
        self.assertEqual({"english", "math", "cs408"}, set(results))
        self.assertEqual("failed", results[failing_subject]["status"])
        self.assertEqual(error_code, results[failing_subject]["error_code"])
        self.assertIsNone(results[failing_subject]["closure"])
        for subject in {"english", "math", "cs408"} - {failing_subject}:
            self.assertEqual("succeeded", results[subject]["status"])
            self.assertIsNone(results[subject]["error_code"])
            self.assertIsInstance(results[subject]["closure"], dict)
        self.assertEqual(0, summary["formal_write_count"])
        self.assertEqual(summary, verify_summary(runtime, self.key_path, digest))

    def test_smoke_3_uses_one_common_barrier_and_signed_sol_snapshot(self) -> None:
        digest, _path, summary = run_mixed_stress(
            self.selection("smoke_3", 1),
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=self.runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
            barrier_timeout_seconds=5,
        )
        self.assertEqual("passed", summary["status"])
        self.assertEqual(3, summary["barrier_expected"])
        self.assertEqual(3, summary["barrier_arrived"])
        self.assertIsNotNone(summary["barrier_released_at"])
        self.assertEqual(3, summary["global_peak_active"])
        self.assertEqual(
            {"english": 1, "math": 1, "cs408": 1},
            summary["per_subject_peak_active"],
        )
        self.assertEqual(6, summary["stage_submission_count"])
        self.assertEqual(6, summary["distinct_transcript_count"])
        self.assertEqual(3, summary["distinct_final_session_count"])
        self.assertEqual(3, summary["distinct_package_count"])
        self.assertEqual(3, summary["distinct_quality_receipt_count"])
        evidence = summary["sol_control"]["global_state_evidence"]
        self.assertEqual(evidence["before_sha256"], evidence["after_sha256"])
        self.assertTrue(evidence["unchanged"])
        self.assertEqual(0, evidence["formal_write_count_delta"])
        self.assertEqual(
            summary,
            verify_summary(self.runtime, self.key_path, digest),
        )

    def test_math_provider_failure_does_not_cancel_subject_siblings(self) -> None:
        self._assert_subject_failure_is_local(
            failing_subject="math",
            error_code="math_analysis_invalid_json_schema",
            failure_kind="provider",
        )

    def test_english_mcp_failure_does_not_cancel_subject_siblings(self) -> None:
        self._assert_subject_failure_is_local(
            failing_subject="english",
            error_code="english_analysis_mcp_duplicate_read",
            failure_kind="mcp",
        )

    def test_cs408_quality_failure_does_not_cancel_subject_siblings(self) -> None:
        self._assert_subject_failure_is_local(
            failing_subject="cs408",
            error_code="mixed_stress_quality_result_invalid",
            failure_kind="quality",
        )

    def test_multiple_subject_failures_preserve_each_terminal_signature(
        self,
    ) -> None:
        error_codes = {
            "english": "english_analysis_mcp_duplicate_read",
            "math": "math_analysis_invalid_json_schema",
            "cs408": "critical_review_refs_invalid",
        }

        def failing_runner(task: dict, observe: object) -> dict:
            observe("analysis_submitted")  # type: ignore[operator]
            raise MixedLunaStressError(error_codes[task["subject"]])

        digest, _path, summary = run_mixed_stress(
            self.selection("smoke_3", 1),
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=failing_runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
        )
        self.assertEqual("failed_closed", summary["status"])
        self.assertEqual(3, summary["global_peak_active"])
        self.assertEqual(
            error_codes,
            {
                row["subject"]: row["error_code"]
                for row in summary["results"]
            },
        )
        self.assertTrue(
            all(row["status"] == "failed" for row in summary["results"])
        )
        self.assertEqual(0, summary["formal_write_count"])
        self.assertEqual(summary, verify_summary(self.runtime, self.key_path, digest))

    def test_summary_rejects_nonterminal_or_duplicate_result_rows(self) -> None:
        _digest, _path, summary = run_mixed_stress(
            self.selection("smoke_3", 1),
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=self.runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
        )
        core = {
            key: copy.deepcopy(value)
            for key, value in summary.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        processing = copy.deepcopy(core)
        processing["results"][0].update(
            {
                "status": "processing",
                "error_code": "still_processing",
                "closure": None,
            }
        )
        processing["status"] = "failed_closed"
        processing["failure_code"] = "mixed_stress_quality_closure_failed"
        processing["distinct_transcript_count"] = 4
        processing["distinct_hmac_stage_receipt_count"] = 4
        processing["distinct_stage_receipt_hmac_count"] = 4
        processing["distinct_final_session_count"] = 2
        processing["distinct_package_count"] = 2
        processing["distinct_quality_receipt_count"] = 2
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_summary_invalid"
        ):
            _validate_summary_core(processing)

        duplicate = copy.deepcopy(core)
        duplicate["results"][1]["ordinal"] = duplicate["results"][0]["ordinal"]
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_summary_invalid"
        ):
            _validate_summary_core(duplicate)

    def test_revoked_98_and_108_modes_are_not_selectable(self) -> None:
        self.assertEqual(
            {"smoke_3": {"english": 1, "math": 1, "cs408": 1}},
            RUN_COUNTS,
        )
        for revoked in ("smoke_20", "full_108"):
            with self.assertRaises(KeyError):
                self.selection(revoked, 2)

    def test_candidate_authority_drift_fails_before_any_runner_call(self) -> None:
        def drift(subject: str) -> dict[str, str]:
            value = self.authority(subject)
            if subject == "english":
                value["generation"] = "drifted-generation"
            return value

        class DriftHost:
            def subject_authority_snapshot(_self, subject: str) -> dict[str, str]:
                return drift(subject)

        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_candidate_authority_drift"
        ):
            run_mixed_stress(
                self.selection("smoke_3", 1),
                runtime_root=self.runtime,
                authority_key_path=self.key_path,
                processing_host=DriftHost(),
                runner=self.runner,
                artifact_verifier=self.verifier,
                sol_state_reader=lambda: dict(self.sol_state),
            )
        self.assertEqual(0, self.runner_calls)

    def test_smoke_rejects_any_staircase_prerequisite(self) -> None:
        smoke_selection = self.selection("smoke_3", 1)
        smoke_sha, _path, smoke = run_mixed_stress(
            smoke_selection,
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=self.runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
        )
        self.assertEqual("passed", smoke["status"])
        calls_before = self.runner_calls
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_prerequisite_invalid"
        ):
            run_mixed_stress(
                self.selection("smoke_3", 2),
                runtime_root=self.runtime,
                authority_key_path=self.key_path,
                processing_host=self.processing_host,
                runner=self.runner,
                artifact_verifier=self.verifier,
                sol_state_reader=lambda: dict(self.sol_state),
                smoke_3_summary_sha256=smoke_sha,
            )
        self.assertEqual(calls_before, self.runner_calls)

    def test_cross_subject_namespace_reuse_fails_preflight(self) -> None:
        selection = self.selection("smoke_3", 1)
        for task in selection["tasks"]:
            task["mcp_namespace"] = "shared_namespace"
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_cross_subject_isolation_invalid"
        ):
            run_mixed_stress(
                selection,
                runtime_root=self.runtime,
                authority_key_path=self.key_path,
                processing_host=self.processing_host,
                runner=self.runner,
                artifact_verifier=self.verifier,
                sol_state_reader=lambda: dict(self.sol_state),
            )
        self.assertEqual(0, self.runner_calls)

    def test_cross_subject_authority_fingerprint_reuse_fails_preflight(self) -> None:
        selection = self.selection("smoke_3", 1)
        for task in selection["tasks"]:
            task["authority_fingerprint"] = "f" * 64
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_stress_cross_subject_isolation_invalid"
        ):
            run_mixed_stress(
                selection,
                runtime_root=self.runtime,
                authority_key_path=self.key_path,
                processing_host=self.processing_host,
                runner=self.runner,
                artifact_verifier=self.verifier,
                sol_state_reader=lambda: dict(self.sol_state),
            )
        self.assertEqual(0, self.runner_calls)

    def test_rejected_critic_is_not_success_or_handoff(self) -> None:
        def rejected_runner(task: dict, observe: object) -> dict:
            result = self.runner(task, observe)
            if task["subject"] == "english":
                result["quality_outcome"] = "rejected"
                result["proposal_action"] = "conflict"
            return result

        _digest, _path, summary = run_mixed_stress(
            self.selection("smoke_3", 1),
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=rejected_runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
        )
        self.assertEqual("failed_closed", summary["status"])
        self.assertFalse(
            summary["sol_control"]["subjects"]["english"][
                "parent_sol_batch_handoff_eligible"
            ]
        )
        self.assertEqual(0, summary["formal_write_count"])

    def test_global_sol_state_change_fails_closed_with_signed_evidence(self) -> None:
        changed = False

        def mutating_runner(task: dict, observe: object) -> dict:
            nonlocal changed
            result = self.runner(task, observe)
            if not changed:
                changed = True
                self.sol_state["revision"] = 1
                self.sol_state["formal_write_count"] = 1
            return result

        digest, _path, summary = run_mixed_stress(
            self.selection("smoke_3", 1),
            runtime_root=self.runtime,
            authority_key_path=self.key_path,
            processing_host=self.processing_host,
            runner=mutating_runner,
            artifact_verifier=self.verifier,
            sol_state_reader=lambda: dict(self.sol_state),
        )
        self.assertEqual("failed_closed", summary["status"])
        self.assertEqual("mixed_stress_sol_state_changed", summary["failure_code"])
        evidence = summary["sol_control"]["global_state_evidence"]
        self.assertFalse(evidence["unchanged"])
        self.assertEqual(1, evidence["formal_write_count_delta"])
        self.assertEqual(summary, verify_summary(self.runtime, self.key_path, digest))


if __name__ == "__main__":
    unittest.main()
