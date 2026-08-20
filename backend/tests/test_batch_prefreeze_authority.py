from __future__ import annotations

import json
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import DispatchError, FrozenTask  # noqa: E402
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
from processing_plugin import ProcessingPluginHost  # noqa: E402
from subject_sol_contract import SubjectSolContractError  # noqa: E402


class BatchPrefreezeAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="batch-prefreeze-")
        self.runtime_root = Path(self.temp.name) / "runtime"
        self.runtime_root.mkdir(parents=True)
        key_path = self.runtime_root / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True)
        key_path.write_bytes(b"p" * 32)
        key_path.chmod(0o600)
        self.config_path = Path(self.temp.name) / "config.json"
        self.config_path.write_text("{}\n", encoding="utf-8")
        self.runtime = ProductionDispatchRuntime(
            {
                "runtime_root": str(self.runtime_root),
                "timezone": "UTC",
                "worker": {"model_timeout_seconds": 1},
                "math_deep_v2": {
                    "soft_runtime_warning_seconds": 3600,
                    "stall_timeout_seconds": 1800,
                    "stall_probe_interval_seconds": 60,
                    "stall_probe_required_consecutive_failures": 2,
                },
            },
            "math",
            self.config_path,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _unit(capture_id: str, recorded_at: str) -> SimpleNamespace:
        task = FrozenTask(
            {
                "subject": "math",
                "capture_id": capture_id,
                "study_date": "2026-08-09",
                "recorded_at": recorded_at,
                "input_fingerprint": (capture_id.encode().hex() + "0" * 64)[:64],
                "model_input": {"capture_id": capture_id},
                "dispatch_contract": {"dispatch_reason": "eligible"},
            }
        )
        return SimpleNamespace(task=task)

    def test_scan_snapshot_is_deterministic_and_not_completion_derived(self) -> None:
        first = self._unit("CAP-001", "2026-08-09T01:00:00Z")
        second = self._unit("CAP-002", "2026-08-09T02:00:00Z")
        one = self.runtime._scan_snapshot(
            "math", [first, second], "2026-08-09"
        )
        two = self.runtime._scan_snapshot(
            "math", [second, first], "2026-08-09"
        )
        self.assertEqual(one, two)
        snapshot, snapshot_sha, high_watermark, tasks = one
        self.assertEqual(snapshot["tasks"], tasks)
        self.assertEqual(len(snapshot_sha), 64)
        self.assertEqual(len(high_watermark), 64)
        self.assertNotIn("completion", snapshot)

    def test_batch_is_frozen_before_first_submit_and_runner_gets_same_authority(self) -> None:
        unit = self._unit("CAP-003", "2026-08-09T03:00:00Z")
        decisions = [
            {
                "capture_id": "CAP-003",
                "unit_sha256": unit.task.unit_sha256,
                "eligible": True,
                "model_enqueue_allowed": True,
                "study_date": "2026-08-09",
            }
        ]
        events: list[str] = []
        self.runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda _subject: {
                "generation": "generation-1",
                "authority_fingerprint": "b" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )

        def prepare(**kwargs):
            events.append("freeze")
            self.assertEqual(kwargs["tasks"][0]["capture_id"], "CAP-003")
            self.assertEqual(kwargs["authority_generation"], "generation-1")
            return {"status": "frozen", "batch_id": kwargs["batch_id"]}

        self.runtime.subject_sol.prepare_and_freeze_subject_batch = prepare

        def submit(task):
            events.append("submit")
            runner = self.runtime._runner_factory(task, None)
            self.assertEqual(
                runner.expected_batch_authority["generation"], "generation-1"
            )
            self.assertEqual(
                runner.expected_batch_authority["authority_fingerprint"],
                "b" * 64,
            )
            return SimpleNamespace(done=False)

        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([unit], decisions),
        ), mock.patch.object(self.runtime.dispatcher, "submit", side_effect=submit):
            handles, _ = self.runtime.scan_and_submit()
        self.assertEqual(len(handles), 1)
        self.assertEqual(events, ["freeze", "submit"])

    def test_real_store_freezes_exact_tasks_and_scan_authority_idempotently(self) -> None:
        units = [
            self._unit("CAP-101", "2026-08-09T01:00:00Z"),
            self._unit("CAP-102", "2026-08-09T02:00:00Z"),
        ]
        self.runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda _subject: {
                "generation": "generation-prefreeze-1",
                "authority_fingerprint": "c" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        self.runtime._prepare_batch_before_submit(units)
        first = self.runtime.subject_sol.read_subject_batch("math")
        self.runtime._prepare_batch_before_submit(list(reversed(units)))
        second = self.runtime.subject_sol.read_subject_batch("math")
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "frozen")
        self.assertEqual(first["authority_generation"], "generation-prefreeze-1")
        self.assertEqual(first["authority_fingerprint"], "c" * 64)
        self.assertEqual(
            [row["capture_id"] for row in first["tasks"]],
            ["CAP-101", "CAP-102"],
        )
        self.assertEqual(first["formal_write_count"], 0)

    def test_migrated_exact_four_pass_reopened_commit_to_batch_store(self) -> None:
        descriptor_sha256 = "d" * 64
        commit = {
            "schema_version": "study-intake-math-pending-queue-migration-commit-v1",
            "migration_descriptor_sha256": descriptor_sha256,
            "queue_mappings": [{"capture_id": f"CAP-MIG-{index}"} for index in range(4)],
        }
        commit_path = self.runtime_root / "migration-commit.json"
        commit_raw = (
            json.dumps(commit, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        commit_path.write_bytes(commit_raw)
        reopened = {
            "migration_commit_path": str(commit_path),
            "migration_commit_sha256": hashlib.sha256(commit_raw).hexdigest(),
        }
        units = []
        for index in range(4):
            unit = self._unit(
                f"CAP-MIG-{index}", f"2026-08-09T0{index + 1}:00:00Z"
            )
            payload = dict(unit.task.frozen_payload)
            payload["math_exact_smoke_binding"] = {
                "migration_descriptor_sha256": descriptor_sha256
            }
            units.append(SimpleNamespace(task=FrozenTask(payload)))
        self.runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda _subject: {
                "generation": "generation-migrated",
                "authority_fingerprint": "e" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        captured = {}

        def prepare(**kwargs):
            captured.update(kwargs)
            return {"status": "frozen", "batch_id": kwargs["batch_id"]}

        self.runtime.subject_sol.prepare_and_freeze_subject_batch = prepare
        with mock.patch(
            "preprocess_dispatcher.verify_exact_batch_authority"
        ), mock.patch(
            "preprocess_dispatcher.reopen_exact_math_pending_queue_migration_commit",
            return_value=reopened,
        ):
            self.runtime._prepare_batch_before_submit(units)
        self.assertEqual(captured["exact_math_migration_commit"], commit)

    def test_migrated_exact_four_missing_commit_fails_before_batch_write(self) -> None:
        descriptor_sha256 = "d" * 64
        units = []
        for index in range(4):
            unit = self._unit(
                f"CAP-MISSING-{index}", f"2026-08-09T0{index + 1}:00:00Z"
            )
            payload = dict(unit.task.frozen_payload)
            payload["math_exact_smoke_binding"] = {
                "migration_descriptor_sha256": descriptor_sha256
            }
            units.append(SimpleNamespace(task=FrozenTask(payload)))
        self.runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda _subject: {
                "generation": "generation-migrated",
                "authority_fingerprint": "e" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        with mock.patch(
            "preprocess_dispatcher.verify_exact_batch_authority"
        ), mock.patch(
            "preprocess_dispatcher.reopen_exact_math_pending_queue_migration_commit",
            side_effect=KeyError("migration_commit"),
        ), mock.patch.object(
            self.runtime.subject_sol, "prepare_and_freeze_subject_batch"
        ) as prepare, self.assertRaisesRegex(
            DispatchError, "math_migration_batch_authority_invalid"
        ):
            self.runtime._prepare_batch_before_submit(units)
        prepare.assert_not_called()

    def test_valid_production_config_without_host_fails_before_submit(self) -> None:
        unit = self._unit("CAP-004", "2026-08-09T04:00:00Z")
        self.runtime.config["model"] = {
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
        }
        with self.assertRaisesRegex(
            DispatchError, "processing_batch_authority_host_required"
        ), mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([unit], []),
        ), mock.patch.object(self.runtime.dispatcher, "submit") as submit:
            self.runtime.scan_and_submit()
        submit.assert_not_called()

    def test_controlled_exact_tasks_use_real_host_prefreeze_and_generation_fence(self) -> None:
        release_id = "a" * 64
        unit = self._unit("CAP-EXACT-001", "2026-08-09T04:30:00Z")
        payload = dict(unit.task.frozen_payload)
        payload["dispatch_contract"] = {
            "release_id": release_id,
            "dispatch_reason": "controlled_replay",
        }
        task = FrozenTask(payload)
        mcp_release_id = (
            "21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425"
        )
        mcp_source_root = Path(
            "/Users/xiazhibin/Documents/Codex/local-study-read-mcp"
        )
        host = ProcessingPluginHost(
            {
                "enabled": True,
                "root": str(ROOT / "plugin/kaoyan-study-intake"),
                "component_lock_path": str(
                    ROOT / "plugin/kaoyan-study-intake/component-lock.json"
                ),
                "mcp_client_python": str(mcp_source_root / ".venv/bin/python"),
                "mcp_project_root": str(
                    Path("/Users/xiazhibin/.codex/local-study-read-mcp/releases")
                    / mcp_release_id
                ),
                "authority_key_path": str(
                    self.runtime_root / "dispatch/state/authority.key"
                ),
                "profile": "background",
                "timeout_seconds": 5,
            },
            runtime_root=self.runtime_root,
            candidate_release_id=release_id,
        )
        lock = json.loads(
            (ROOT / "plugin/kaoyan-study-intake/component-lock.json").read_text(
                encoding="utf-8"
            )
        )
        server_release = lock["mcp_server_release"]

        def authority_response(subject, _tool, arguments):
            return {
                "ok": True,
                "schema_version": "study-read-mcp.v3",
                "profile": "background",
                "read_route": arguments["route"],
                "generation": "generation-exact-1",
                "authority_fingerprint": "f" * 64,
                "server_release": server_release,
                "preprocessor_release": "c" * 64,
                "formal_write_count": 0,
                "model_call_count": 0,
                "items": [{
                    "subject": subject,
                    "available": True,
                    "generation": "generation-exact-1",
                    "authority_fingerprint": "f" * 64,
                    "adapter_release": server_release,
                }],
            }

        self.runtime.processing_host = host
        with mock.patch.object(host, "_call", side_effect=authority_response):
            prepared = self.runtime.prepare_controlled_replay_tasks(
                [task], release_id=release_id
            )
        self.assertEqual(prepared["task_count"], 1)
        self.assertEqual(prepared["model_call_count"], 0)
        self.assertEqual(prepared["formal_write_count"], 0)
        self.assertEqual(
            prepared["batch_authority"]["generation"], "generation-exact-1"
        )
        batch = self.runtime.subject_sol.read_subject_batch("math")
        self.assertEqual(batch["tasks"][0]["unit_sha256"], task.unit_sha256)
        token = object()
        with mock.patch.object(
            self.runtime.dispatcher, "submit", return_value=token
        ) as submit:
            observed = self.runtime.submit_prepared_controlled_replay_task(task)
        self.assertIs(observed, token)
        submit.assert_called_once_with(task)

    def test_successful_completion_runs_deterministic_quality_closure(self) -> None:
        unit = self._unit("CAP-201", "2026-08-09T05:00:00Z")
        completion = {
            "subject": "math",
            "capture_id": "CAP-201",
            "release_id": "a" * 64,
            "outcome": "succeeded",
        }
        result = SimpleNamespace(
            unit_sha256=unit.task.unit_sha256,
            completion=completion,
            outcome="succeeded",
            error_code=None,
        )
        handle = SimpleNamespace(done=True, wait=lambda _timeout: result)
        verified = {"completion": completion}
        pending_runtime = {
            "subject_luna_batch": {
                "tasks": [{
                    "capture_id": "CAP-201",
                    "unit_sha256": unit.task.unit_sha256,
                    "status": "quality_pending",
                }]
            }
        }
        final_runtime = {"subject_luna_batch": {"sol_ready": True}}
        with mock.patch.object(
            self.runtime.dispatcher.lease_store,
            "verify_authoritative_completion",
            return_value=verified,
        ), mock.patch.object(
            self.runtime.subject_sol,
            "record_verified_luna_completion",
            return_value=pending_runtime,
        ) as record, mock.patch.object(
            self.runtime.subject_sol,
            "build_verified_completion_quality_closure",
            return_value={"schema_version": "verified_completion_quality_closure_v1"},
        ) as build, mock.patch.object(
            self.runtime.subject_sol,
            "close_verified_completion",
        ) as close, mock.patch.object(
            self.runtime.subject_sol,
            "read_subject",
            return_value=final_runtime,
        ):
            projected = self.runtime.persist_finished_luna([handle])
        self.assertEqual(projected, [final_runtime])
        record.assert_called_once_with("math", verified)
        build.assert_called_once_with(
            "math", verified, config=self.runtime.config
        )
        close.assert_called_once_with(
            "math",
            closure={"schema_version": "verified_completion_quality_closure_v1"},
            config=self.runtime.config,
        )

    def test_critical_reject_completion_never_runs_success_closure(self) -> None:
        unit = self._unit("CAP-202", "2026-08-09T06:00:00Z")
        completion = {
            "subject": "math",
            "capture_id": "CAP-202",
            "release_id": "a" * 64,
            "outcome": "failed",
            "error_code": "math_critical_review_rejected",
        }
        result = SimpleNamespace(
            unit_sha256=unit.task.unit_sha256,
            completion=completion,
            outcome="failed",
            error_code="math_critical_review_rejected",
        )
        handle = SimpleNamespace(done=True, wait=lambda _timeout: result)
        verified = {"completion": completion}
        rejected_runtime = {
            "subject_luna_batch": {
                "tasks": [{
                    "capture_id": "CAP-202",
                    "unit_sha256": unit.task.unit_sha256,
                    "status": "needs_rework",
                    "package_sha256": None,
                }]
            }
        }
        with mock.patch.object(
            self.runtime.dispatcher.lease_store,
            "verify_authoritative_completion",
            return_value=verified,
        ), mock.patch.object(
            self.runtime.subject_sol,
            "record_verified_luna_completion",
            return_value=rejected_runtime,
        ), mock.patch.object(
            self.runtime.subject_sol,
            "build_verified_completion_quality_closure",
        ) as build:
            projected = self.runtime.persist_finished_luna([handle])
        self.assertEqual(projected, [rejected_runtime])
        build.assert_not_called()

    def test_evidence_incomplete_success_is_closed_as_failed_not_sol_ready(self) -> None:
        unit = self._unit("CAP-203", "2026-08-09T06:30:00Z")
        completion = {
            "subject": "math",
            "capture_id": "CAP-203",
            "release_id": "a" * 64,
            "outcome": "succeeded",
        }
        result = SimpleNamespace(
            unit_sha256=unit.task.unit_sha256,
            completion=completion,
            outcome="succeeded",
            error_code=None,
        )
        handle = SimpleNamespace(done=True, wait=lambda _timeout: result)
        verified = {"completion": completion}
        pending_runtime = {
            "subject_luna_batch": {
                "batch_id": "LUNA-MATH-TEST",
                "tasks": [{
                    "capture_id": "CAP-203",
                    "unit_sha256": unit.task.unit_sha256,
                    "status": "quality_pending",
                }],
            }
        }
        failed_runtime = {"subject_luna_batch": {"sol_ready": False}}
        with mock.patch.object(
            self.runtime.dispatcher.lease_store,
            "verify_authoritative_completion",
            return_value=verified,
        ), mock.patch.object(
            self.runtime.subject_sol,
            "record_verified_luna_completion",
            return_value=pending_runtime,
        ), mock.patch.object(
            self.runtime.subject_sol,
            "build_verified_completion_quality_closure",
            side_effect=SubjectSolContractError(
                "quality_subject_proposal_not_sol_ready"
            ),
        ), mock.patch.object(
            self.runtime.subject_sol,
            "record_task_terminal_failure",
        ) as fail, mock.patch.object(
            self.runtime.subject_sol,
            "read_subject",
            return_value=failed_runtime,
        ):
            projected = self.runtime.persist_finished_luna([handle])
        self.assertEqual(projected, [failed_runtime])
        fail.assert_called_once_with(
            subject="math",
            batch_id="LUNA-MATH-TEST",
            capture_id="CAP-203",
            unit_sha256=unit.task.unit_sha256,
            status="failed",
            error_code="quality_subject_proposal_not_sol_ready",
        )

    def test_submit_rejection_closes_the_exact_frozen_task_as_failed(self) -> None:
        unit = self._unit("CAP-301", "2026-08-09T07:00:00Z")
        decisions = [{
            "capture_id": "CAP-301",
            "unit_sha256": unit.task.unit_sha256,
            "eligible": True,
            "model_enqueue_allowed": True,
            "study_date": "2026-08-09",
        }]
        self.runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda _subject: {
                "generation": "generation-prefreeze-submit-failure",
                "authority_fingerprint": "e" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([unit], decisions),
        ), mock.patch.object(
            self.runtime.dispatcher,
            "submit",
            side_effect=RuntimeError("synthetic submit failure"),
        ):
            handles, projected = self.runtime.scan_and_submit()
        self.assertEqual(handles, [])
        self.assertEqual(projected[0]["phase"], "failed")
        batch = self.runtime.subject_sol.read_subject_batch("math")
        self.assertEqual(batch["tasks"][0]["status"], "execution_failed")
        self.assertEqual(
            batch["tasks"][0]["error_code"], "submit_runtime_failed"
        )
        self.assertIsNotNone(batch["tasks"][0]["terminal_receipt_sha256"])


if __name__ == "__main__":
    unittest.main()
