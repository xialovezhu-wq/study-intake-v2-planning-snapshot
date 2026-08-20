from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "dashboard"))

from concurrent_dispatch import ConcurrentDispatcher, FrozenTask, LeaseStore  # noqa: E402
from core_dispatch_bridge import CoreCandidateSubprocessRunner  # noqa: E402
import dashboard_projection as projection  # noqa: E402
import server as dashboard  # noqa: E402
from subject_sol_contract import SubjectSolRuntimeStore  # noqa: E402
from test_production_canary_admission import authority, canary_task  # noqa: E402
from test_successor_schema_roundtrip import schema_results  # noqa: E402


FIXTURE = ROOT / "tests/fixtures/review_candidate_task_runner.py"
READ_CLI = ROOT / "bin/read_sol_review_candidate.py"


def runtime_tree(root: Path) -> dict[str, tuple[int, str]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_mode,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


class ReviewCandidateTerminalTests(unittest.TestCase):
    def _run_terminal(
        self,
        disposition: str,
        *,
        subject: str = "math",
        exact_math_first: bool = False,
        restricted_reopen_error: bool = False,
    ) -> tuple[Path, FrozenTask, object]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        runtime = Path(temporary.name) / "runtime"
        release_id = "a" * 64
        store = LeaseStore(runtime)
        store.begin_subject_drain(subject)
        store.activate_production_canary(
            subject,
            release_id=release_id,
            producer_authority=authority(subject, release_id),
            activated_at="2026-08-11T00:00:00Z",
        )
        base = canary_task(901, subject=subject, release_id=release_id)
        payload = dict(base.frozen_payload)
        payload["test_report_disposition"] = disposition
        if exact_math_first:
            payload["math_exact_smoke_binding"] = {
                "formal_id": "GS-269",
                "capture_event_id": "MFI-CAP-5d1e87959549b634bdda36b2",
            }
        task = FrozenTask(payload)
        binding = payload.get("math_exact_smoke_binding")
        with mock.patch(
            "concurrent_dispatch.verify_exact_task_binding",
            return_value=binding,
        ):
            store.materialize_production_canary_task(task)
        config_path = Path(temporary.name) / "config.json"
        config_path.write_text("{}\n", encoding="utf-8")
        dispatcher = ConcurrentDispatcher(
            runtime,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            ),
            stage_timeout_seconds=5,
            production_canary=True,
        )
        reopen = mock.patch.object(
            SubjectSolRuntimeStore,
            "read_restricted_sol_review_candidate",
            side_effect=(
                RuntimeError("synthetic restricted reopen failure")
                if restricted_reopen_error
                else None
            ),
        )
        if restricted_reopen_error:
            with reopen:
                result = dispatcher.submit(task).wait(10)
        else:
            result = dispatcher.submit(task).wait(10)
        return runtime, task, result

    @staticmethod
    def _dashboard_decision(task: FrozenTask) -> dict[str, object]:
        frozen = task.frozen_payload
        return {
            "subject": "math",
            "capture_id": frozen["capture_id"],
            "study_date": frozen["study_date"],
            "target_label": frozen["target_label"],
            "input_fingerprint": frozen["input_fingerprint"],
            "eligible": True,
            "reason": "eligible",
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "release_id": "a" * 64,
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "model_enqueue_allowed": True,
            "updated_at": "2026-08-11T00:00:01Z",
        }

    def test_task_runner_to_finish_preserves_needs_sol_review_without_retry(self) -> None:
        runtime, task, result = self._run_terminal("needs_sol_review")
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "succeeded")
        self.assertIsNotNone(result.completion)
        store = LeaseStore(runtime)
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "continuous_concurrent_unlocked")
        self.assertTrue(state["luna_consumer_enabled"])
        self.assertTrue(state["unlocked_once"])
        self.assertNotEqual(state["state"], "failed_drained")
        self.assertEqual(state["last_report_status"], "reopen_verified")
        self.assertEqual(state["queue_depth"], 0)
        self.assertEqual(store.subject_status("math")["retry_wait_count"], 0)

        contract = task.frozen_payload["dispatch_contract"][
            "producer_input_contract"
        ]
        queue_path = store._production_canary_queue_path(
            "math", contract["producer_input_contract_sha256"]
        )
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        self.assertEqual(
            queue["schema_version"],
            "study-intake-production-canary-queue-entry-v3",
        )
        self.assertEqual(queue["queue_status"], "succeeded")
        self.assertEqual(queue["terminal_outcome"], "succeeded")
        self.assertEqual(queue["execution_status"], "succeeded")
        self.assertEqual(queue["quality_status"], "issues_found")
        self.assertEqual(queue["report_disposition"], "needs_sol_review")
        self.assertEqual(queue["sol_review_status"], "pending")

        terminal = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            terminal["schema_version"],
            "study-intake-production-canary-review-terminal-receipt-v1",
        )
        self.assertEqual(terminal["outcome"], "succeeded")
        self.assertEqual(terminal["execution_status"], "succeeded")
        self.assertEqual(terminal["quality_status"], "issues_found")
        self.assertEqual(
            terminal["report_disposition"], "needs_sol_review"
        )
        self.assertEqual(terminal["sol_review_status"], "pending")
        self.assertGreater(terminal["observed_model_call_count"], 0)
        self.assertGreater(terminal["observed_provider_request_count"], 0)
        self.assertGreater(terminal["observed_mcp_tool_call_count"], 0)
        self.assertEqual(terminal["formal_write_count"], 0)

        completion = result.completion
        assert completion is not None
        package = json.loads(
            Path(completion["package_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(
            package["schema_version"],
            "study-intake-review-candidate-package-v1",
        )
        self.assertEqual(package["pipeline"], ["analysis"])
        self.assertIsNone(package["critical_review"])
        self.assertEqual(package["execution_status"], "succeeded")
        self.assertEqual(package["quality_status"], "issues_found")
        self.assertTrue(package["report_available"])
        self.assertEqual(package["sol_review_status"], "pending")
        self.assertFalse(package["formal_write_eligible"])
        self.assertTrue(package["warnings"])

    def _assert_subject_quality_success(self, subject: str) -> None:
        runtime, task, result = self._run_terminal(
            "needs_sol_review", subject=subject
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "succeeded")
        self.assertIsNone(result.error_code)
        self.assertIsNotNone(result.completion)
        completion = result.completion
        assert completion is not None
        self.assertEqual(completion["outcome"], "succeeded")
        receipt = json.loads(
            Path(completion["receipt_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["outcome"], "succeeded")
        self.assertIsNone(receipt["error_code"])
        package = json.loads(
            Path(completion["package_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(package["execution_status"], "succeeded")
        self.assertEqual(package["quality_status"], "issues_found")
        self.assertTrue(package["report_available"])
        self.assertEqual(package["report_disposition"], "needs_sol_review")
        self.assertEqual(package["sol_review_status"], "pending")
        self.assertFalse(package["formal_write_eligible"])
        self.assertFalse(package["production_accepted"])
        self.assertTrue(package["warnings"])
        self.assertIsInstance(package["review_result"]["error_code"], str)
        self.assertTrue(package["review_result"]["error_code"])
        reopened = SubjectSolRuntimeStore(
            runtime
        ).read_restricted_sol_review_candidate(
            str(completion["report_json_sha256"])
        )
        self.assertTrue(reopened["report_available"])
        self.assertIn("analysis", reopened["raw_outputs"])
        self.assertIn("analysis", reopened["mcp_transcripts"])
        self.assertTrue(reopened["mcp_transcripts"]["analysis"]["calls"])
        if subject == "cs408":
            self.assertEqual(
                package["pipeline"], ["analysis", "critical_review"]
            )
            self.assertIsNotNone(package["critical_review"])
            self.assertIsNotNone(package["stage_runtime"]["critical_review"])
            self.assertNotEqual(
                package["stage_runtime"]["analysis"][
                    "raw_output_object_sha256"
                ],
                package["stage_runtime"]["critical_review"][
                    "raw_output_object_sha256"
                ],
            )
            self.assertIn("critical_review", reopened["raw_outputs"])
            self.assertIn("critical_review", reopened["mcp_transcripts"])
            self.assertTrue(
                reopened["mcp_transcripts"]["critical_review"]["calls"]
            )
        else:
            self.assertEqual(package["pipeline"], ["analysis"])
            self.assertIsNone(package["critical_review"])
            self.assertIsNone(package["stage_runtime"]["critical_review"])
        store = LeaseStore(runtime)
        state = store.production_canary_status(subject)
        self.assertEqual(state["terminal_by_outcome"]["succeeded"], 1)
        self.assertEqual(store.subject_status(subject)["retry_wait_count"], 0)
        contract = task.frozen_payload["dispatch_contract"][
            "producer_input_contract"
        ]
        queue = json.loads(
            store._production_canary_queue_path(
                subject,
                contract["producer_input_contract_sha256"],
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(queue["queue_status"], "succeeded")
        self.assertEqual(queue["terminal_outcome"], "succeeded")
        self.assertIsNone(queue.get("terminal_error_code"))
        self.assertNotEqual(queue["queue_status"], "retrying")
        self.assertEqual(queue["formal_write_count"], 0)
        terminal = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertIsNone(terminal.get("terminal_error_code"))
        self.assertIsNone(terminal.get("error_code"))
        self.assertEqual(store.pending_production_canary_tasks(subject), [])
        self.assertEqual(
            task.frozen_payload.get("processing_attempt_number", 1), 1
        )

    def test_math_quality_findings_keep_execution_succeeded(self) -> None:
        self._assert_subject_quality_success("math")

    def test_english_quality_findings_keep_execution_succeeded(self) -> None:
        self._assert_subject_quality_success("english")

    def test_cs408_quality_findings_keep_execution_succeeded(self) -> None:
        self._assert_subject_quality_success("cs408")

    def test_exact_math_first_review_candidate_unlocks_concurrent_gate(self) -> None:
        runtime, _task, result = self._run_terminal(
            "needs_sol_review", exact_math_first=True
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "succeeded")
        store = LeaseStore(runtime)
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "continuous_concurrent_unlocked")
        self.assertTrue(state["unlocked_once"])
        self.assertEqual(
            state["next_action"], "consume_pending_post_activation_queue"
        )
        terminal = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(terminal["outcome"], "succeeded")
        self.assertEqual(terminal["state_after"], "continuous_concurrent_unlocked")
        self.assertFalse(terminal["production_accepted"])
        self.assertEqual(terminal["formal_write_count"], 0)
        self.assertFalse(terminal["sol_enabled"])
        completion = result.completion
        assert completion is not None
        package = json.loads(
            Path(completion["package_path"]).read_text(encoding="utf-8")
        )
        self.assertTrue(package["report_available"])
        self.assertEqual(package["report_disposition"], "needs_sol_review")
        self.assertEqual(package["sol_review_status"], "pending")
        self.assertFalse(package["formal_write_eligible"])
        results = schema_results(
            {
                "state": ("production-canary-state-v3.json", state),
                "terminal": (
                    "production-canary-review-terminal-receipt-v1.json",
                    terminal,
                ),
            }
        )
        self.assertEqual(results, {key: [] for key in results}, results)

    def test_exact_math_first_review_requires_restricted_reopen(self) -> None:
        runtime, _task, result = self._run_terminal(
            "needs_sol_review",
            exact_math_first=True,
            restricted_reopen_error=True,
        )
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.error_code,
            "production_canary_review_evidence_incomplete",
        )
        state = LeaseStore(runtime).production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["unlocked_once"])

    def test_exact_math_quarantine_does_not_unlock_concurrent_gate(self) -> None:
        runtime, _task, result = self._run_terminal(
            "quarantined", exact_math_first=True
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "failed")
        state = LeaseStore(runtime).production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["unlocked_once"])
        self.assertFalse(state["luna_consumer_enabled"])

    def test_quarantined_terminal_is_retained_and_gate_remains_usable(self) -> None:
        runtime, _task, result = self._run_terminal("quarantined")
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "failed")
        store = LeaseStore(runtime)
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(store.subject_status("math")["retry_wait_count"], 0)
        terminal_path = Path(state["last_terminal_receipt_path"])
        self.assertTrue(terminal_path.is_file())
        terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
        self.assertEqual(terminal["execution_status"], "failed")
        self.assertEqual(terminal["quality_status"], "unchecked")
        self.assertEqual(terminal["report_disposition"], "quarantined")
        self.assertEqual(terminal["sol_review_status"], "not_eligible")
        self.assertFalse(terminal["formal_write_eligible"])

    def test_restricted_sol_cli_reopens_exact_raw_and_mcp_without_writes(self) -> None:
        runtime, _task, result = self._run_terminal("needs_sol_review")
        completion = result.completion
        assert completion is not None
        before = runtime_tree(runtime)
        completed = subprocess.run(
            [
                sys.executable,
                str(READ_CLI),
                "--runtime-root",
                str(runtime),
                "--report-sha256",
                str(completion["report_json_sha256"]),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        view = json.loads(completed.stdout)
        after = runtime_tree(runtime)
        self.assertEqual(after, before)
        self.assertTrue(view["report_available"])
        self.assertEqual(view["sol_review_status"], "pending")
        self.assertFalse(view["formal_write_eligible"])
        self.assertFalse(view["automatic_adoption"])
        self.assertFalse(view["automatic_formal_write"])
        self.assertEqual(view["formal_write_count"], 0)
        self.assertIn("analysis", view["raw_outputs"])
        self.assertIn("analysis", view["mcp_transcripts"])
        self.assertTrue(view["mcp_transcripts"]["analysis"]["calls"])
        self.assertTrue(view["review_result"])
        self.assertTrue(view["warnings"])

    def test_restricted_sol_exact_hash_fails_closed(self) -> None:
        runtime, _task, result = self._run_terminal("needs_sol_review")
        completion = result.completion
        assert completion is not None
        before = runtime_tree(runtime)
        with self.assertRaisesRegex(
            Exception, "review_candidate_report_missing"
        ):
            SubjectSolRuntimeStore(runtime).read_restricted_sol_review_candidate(
                "f" * 64
            )
        self.assertEqual(runtime_tree(runtime), before)

    def test_new_review_terminal_projects_nonfailed_dashboard_and_public_api(
        self,
    ) -> None:
        for disposition in ("needs_sol_review", "quarantined"):
            with self.subTest(disposition=disposition):
                runtime, task, _result = self._run_terminal(disposition)
                item = projection._item_from_decision(
                    runtime,
                    self._dashboard_decision(task),
                    subject="math",
                    generated_at="2026-08-11T00:01:00Z",
                )
                self.assertIsNotNone(item)
                assert item is not None
                execution_succeeded = disposition == "needs_sol_review"
                expected_luna_status = (
                    "succeeded" if execution_succeeded else "failed"
                )
                self.assertEqual(item["luna_status"], expected_luna_status)
                self.assertEqual(item["local_dispatch_status"], "terminal")
                self.assertEqual(
                    item["analysis_execution_status"],
                    "completed" if execution_succeeded else "failed",
                )
                self.assertEqual(
                    item["analysis_report_status"],
                    "available_with_warnings"
                    if execution_succeeded
                    else "not_started",
                )
                self.assertIs(
                    item["report_available"], execution_succeeded
                )
                self.assertEqual(
                    item["sol_review_status"],
                    "pending" if execution_succeeded else "not_eligible",
                )
                self.assertFalse(item["formal_write_eligible"])
                if execution_succeeded:
                    self.assertRegex(
                        item["analysis_raw_output_sha256"],
                        r"^[0-9a-f]{64}$",
                    )
                    self.assertRegex(
                        item["terminal_receipt_sha256"], r"^[0-9a-f]{64}$"
                    )
                else:
                    self.assertNotIn("analysis_raw_output_sha256", item)
                    self.assertNotIn("terminal_receipt_sha256", item)
                public = dashboard._public_item(item, "math")
                self.assertIsNotNone(public)
                assert public is not None
                self.assertEqual(public["luna_status"], expected_luna_status)
                self.assertIs(
                    public["report_available"], execution_succeeded
                )
                self.assertEqual(
                    public["sol_review_status"],
                    "pending" if execution_succeeded else "not_eligible",
                )
                self.assertFalse(public["formal_write_eligible"])

    def test_unreopenable_needs_rework_keeps_legacy_failed_projection(self) -> None:
        runtime, task, _result = self._run_terminal("needs_sol_review")
        state = LeaseStore(runtime).production_canary_status("math")
        assert state is not None
        Path(state["last_terminal_receipt_path"]).rename(
            Path(state["last_terminal_receipt_path"] + ".unavailable")
        )
        item = projection._item_from_decision(
            runtime,
            self._dashboard_decision(task),
            subject="math",
            generated_at="2026-08-11T00:01:00Z",
        )
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["luna_status"], "failed")
        self.assertFalse(item["report_available"])
        self.assertEqual(item["sol_review_status"], "not_eligible")

    def test_emitted_review_contracts_and_historical_state_v3_validate(self) -> None:
        for disposition in ("needs_sol_review", "quarantined"):
            with self.subTest(disposition=disposition):
                runtime, task, result = self._run_terminal(disposition)
                store = LeaseStore(runtime)
                state = store.production_canary_status("math")
                assert state is not None and result.completion is not None
                contract = task.frozen_payload["dispatch_contract"][
                    "producer_input_contract"
                ]
                queue = json.loads(
                    store._production_canary_queue_path(
                        "math", contract["producer_input_contract_sha256"]
                    ).read_text(encoding="utf-8")
                )
                terminal = json.loads(
                    Path(state["last_terminal_receipt_path"]).read_text(
                        encoding="utf-8"
                    )
                )
                package = json.loads(
                    Path(result.completion["package_path"]).read_text(
                        encoding="utf-8"
                    )
                )
                results = schema_results(
                    {
                        "state": ("production-canary-state-v3.json", state),
                        "queue": ("production-canary-queue-entry-v3.json", queue),
                        "terminal": (
                            "production-canary-review-terminal-receipt-v1.json",
                            terminal,
                        ),
                        "package": ("review-candidate-package-v1.json", package),
                    }
                )
                self.assertEqual(results, {key: [] for key in results}, results)
                self.assertEqual(state["last_report_status"], "reopen_verified")
                self.assertEqual(
                    state["last_critical_review_status"], "not_started"
                )
                self.assertEqual(
                    state["next_action"],
                    "consume_pending_post_activation_queue"
                    if disposition == "needs_sol_review"
                    else "explicit_subject_resume_required",
                )


if __name__ == "__main__":
    unittest.main()
