from __future__ import annotations

import base64
import hashlib
import json
import os
import signal
import sys
import tempfile
import threading
import time
import unittest
import subprocess
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import (  # noqa: E402
    COMPLETION_SCHEMA,
    PACKAGE_SCHEMA,
    TASK_EVENT_SCHEMA,
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    StageResult,
    dispatch_rule_binding,
)
from preprocessor_core import CodexRunner, PreprocessorError  # noqa: E402
from process_identity import kernel_process_start_token  # noqa: E402


def task(index: int, *, subject: str = "math") -> FrozenTask:
    return FrozenTask(
        {
            "subject": subject,
            "capture_id": f"BACKEND-{index:04d}",
            "study_date": "2026-08-12",
            "recorded_at": "2026-08-12T00:00:00Z",
            "input_fingerprint": f"fingerprint-{index}",
            "input_binding": {"index": index},
            "model_input": {"test_index": index},
            "allowed_evidence_refs": [f"capture:{index}"],
            "image_paths": [],
            "dispatch_contract": {
                "schema_version": "study-intake-dispatch-release-binding-v1",
                **dispatch_rule_binding(
                    release_id="a" * 64,
                    subject=subject,
                    subject_processing_contract_sha256=None,
                ),
            },
        }
    )


def result(stage: str, frozen: FrozenTask) -> StageResult:
    return StageResult(
        payload={"stage": stage, "unit_sha256": frozen.unit_sha256},
        runtime_model="gpt-5.6-luna",
        runtime_reasoning_effort="max",
        runtime_metadata_provenance="codex_json_attestation_v1",
        runtime_identity_status="confirmed",
        duration_ms=1,
    )


class BackendSuccessorContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.supervisors: list[subprocess.Popen[bytes]] = []

    def tearDown(self) -> None:
        for process in self.supervisors:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
        self.temp.cleanup()

    def _events(self, frozen: FrozenTask) -> list[dict]:
        root = (
            self.runtime
            / "dispatch/state/task-events"
            / frozen.unit_sha256
            / "fence-1"
        )
        rows: list[dict] = []
        for path in sorted(root.glob("*.json")):
            index = json.loads(path.read_text(encoding="utf-8"))
            rows.append(
                json.loads(
                    Path(index["event_path"]).read_text(encoding="utf-8")
                )
            )
        return rows

    def _bound_codex_runner(self) -> tuple[CodexRunner, LeaseStore, FrozenTask]:
        frozen = task(1)
        store = LeaseStore(self.runtime)
        owner_id = f"dispatcher-{os.getpid()}-{'1' * 32}"
        decision = store.claim(
            frozen.unit_sha256,
            owner_id,
            subject="math",
            task=frozen,
        )
        assert decision.lease is not None
        supervisor = subprocess.Popen(
            [sys.executable, "-c", "import time;time.sleep(60)"],
            start_new_session=True,
        )
        self.supervisors.append(supervisor)
        store.publish_task_process_identity(
            frozen,
            decision.lease,
            child_pid=supervisor.pid,
            child_pgid=supervisor.pid,
            process_start_token=kernel_process_start_token(supervisor.pid),
            launch_nonce="1" * 32,
            launched_at="2026-08-12T00:00:00Z",
            argv=[sys.executable, "-c", "import time;time.sleep(60)"],
            executable_path=Path(sys.executable),
            start_new_session=True,
        )
        runner = CodexRunner(
            {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            self.runtime,
        )
        runner.bind_dispatch_process_lifecycle(
            task=frozen,
            lease=decision.lease,
            lease_store=store,
        )
        return runner, store, frozen

    def test_provider_raw_is_published_before_exit_receipt(self) -> None:
        runner, store, frozen = self._bound_codex_runner()
        context_root = (
            self.runtime / "dispatch/contexts" / frozen.unit_sha256 / "fence-1"
        )
        context_root.mkdir(parents=True, exist_ok=True)
        output_path = self.runtime / "provider-last-message.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        exact_raw = b'{"partial":"exact-provider-bytes"}\n'
        output_path.write_bytes(exact_raw)
        order: list[str] = []
        original_raw = store.publish_model_stage_raw_output

        def publish_raw(*args: object, **kwargs: object) -> dict:
            order.append("raw")
            return original_raw(*args, **kwargs)

        def publish_exit(*args: object, **kwargs: object) -> dict:
            del args, kwargs
            order.append("exit")
            return {
                "provider_process_exit_sha256": "e" * 64,
                "provider_process_exit_path": str(
                    self.runtime / "synthetic-provider-exit.json"
                ),
            }

        with (
            mock.patch.object(
                store, "publish_model_stage_raw_output", side_effect=publish_raw
            ),
            mock.patch.object(
                store, "publish_provider_process_exit", side_effect=publish_exit
            ),
        ):
            completed = runner._invoke_subprocess(
                [
                    sys.executable,
                    "-c",
                    "import sys;sys.stdout.buffer.write(b'wire')",
                ],
                input=b"",
                timeout=0,
                cwd=context_root,
                stage_name="math_analysis",
                raw_output_path=output_path,
                provider_schema_sha256="b" * 64,
            )
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(order, ["raw", "exit"])
        raw_paths = list(store.model_stage_raw_output_root.rglob("*.json"))
        self.assertEqual(len(raw_paths), 1)
        raw = json.loads(raw_paths[0].read_text(encoding="utf-8"))
        self.assertEqual(base64.b64decode(raw["raw_output_base64"]), exact_raw)
        manifest_paths = list(
            store.model_stage_raw_chain_manifest_root.rglob("*.json")
        )
        self.assertEqual(len(manifest_paths), 1)
        manifest_bytes = manifest_paths[0].read_bytes()
        self.assertEqual(
            hashlib.sha256(manifest_bytes).hexdigest(),
            raw["raw_chain_manifest_sha256"],
        )
        manifest = json.loads(manifest_bytes)
        store._verify_seal(
            manifest,
            purpose="study-intake-model-stage-raw-chain-manifest",
        )
        self.assertEqual(
            manifest["chains"]["output_last_message_checkpoint"][
                "reconstructed_sha256"
            ],
            hashlib.sha256(exact_raw).hexdigest(),
        )
        self.assertEqual(raw["provider_stage_name"], "math_analysis")
        self.assertEqual(raw["stage_name"], "analysis")
        with self.assertRaisesRegex(
            DispatchError, "model_stage_raw_identity_mismatch"
        ):
            store.publish_model_stage_raw_output(
                frozen,
                runner._dispatch_process_lifecycle["lease"],
                stage_name="math_analysis",
                raw_output=exact_raw,
                provider_stdout=b"wire",
                provider_stderr=b"",
                provider_returncode=0,
                provider_schema_sha256="b" * 64,
                provider_process_identity_sha256="f" * 64,
                raw_chain_manifest_sha256=raw[
                    "raw_chain_manifest_sha256"
                ],
                raw_chain_manifest_ref=raw["raw_chain_manifest_ref"],
                raw_chain_total_chunk_count=raw[
                    "raw_chain_total_chunk_count"
                ],
                raw_chain_reconstruction_sha256=raw[
                    "raw_chain_reconstruction_sha256"
                ],
            )

    def test_raw_survives_exit_publication_crash_window(self) -> None:
        runner, store, frozen = self._bound_codex_runner()
        context_root = (
            self.runtime / "dispatch/contexts" / frozen.unit_sha256 / "fence-1"
        )
        context_root.mkdir(parents=True, exist_ok=True)
        output_path = self.runtime / "provider-crash-last-message.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        exact_raw = b'{"survives":true}\n'
        output_path.write_bytes(exact_raw)
        with (
            mock.patch.object(
                store,
                "publish_provider_process_exit",
                side_effect=OSError("synthetic-exit-publication-crash"),
            ),
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "provider_process_exit_publish_failed"
            ):
                runner._invoke_subprocess(
                    [sys.executable, "-c", "pass"],
                    input=b"",
                    timeout=0,
                    cwd=context_root,
                    stage_name="math_analysis",
                    raw_output_path=output_path,
                    provider_schema_sha256="c" * 64,
                )
        raw_paths = list(store.model_stage_raw_output_root.rglob("*.json"))
        self.assertEqual(len(raw_paths), 1)
        raw = json.loads(raw_paths[0].read_text(encoding="utf-8"))
        self.assertEqual(base64.b64decode(raw["raw_output_base64"]), exact_raw)
        self.assertEqual(
            list(store.provider_process_exit_root.rglob("*.json")), []
        )

    def test_first_chunk_survives_real_host_sigkill_without_false_final(self) -> None:
        _runner, store, frozen = self._bound_codex_runner()
        context_root = (
            self.runtime / "dispatch/contexts" / frozen.unit_sha256 / "fence-1"
        )
        context_root.mkdir(parents=True, exist_ok=True)
        payload_path = self.runtime / "frozen-payload.json"
        payload_path.write_text(
            json.dumps(frozen.frozen_payload, sort_keys=True),
            encoding="utf-8",
        )
        lease = store._read_object(store._lease_path(frozen.unit_sha256))
        assert isinstance(lease, dict)
        host_script = r"""
import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from concurrent_dispatch import FrozenTask, Lease, LeaseStore
from preprocessor_core import CodexRunner
runtime = Path(sys.argv[2])
task = FrozenTask(json.loads(Path(sys.argv[3]).read_text(encoding="utf-8")))
lease = Lease(task.unit_sha256, sys.argv[4], int(sys.argv[5]))
runner = CodexRunner({"model":"gpt-5.6-luna","reasoning_effort":"max"}, runtime)
runner.bind_dispatch_process_lifecycle(task=task, lease=lease, lease_store=LeaseStore(runtime))
runner._invoke_subprocess(
    [sys.executable, "-c", "import sys,time;sys.stdout.buffer.write(b'first-provider-chunk');sys.stdout.flush();time.sleep(60)"],
    input=b"", timeout=None,
    cwd=runtime / "dispatch" / "contexts" / task.unit_sha256 / f"fence-{lease.fence}",
    stage_name="math_analysis",
    raw_output_path=runtime / "provider-host-killed-last-message.json",
    provider_schema_sha256="9" * 64,
)
"""
        host = subprocess.Popen(
            [
                sys.executable,
                "-c",
                host_script,
                str(ROOT / "lib"),
                str(self.runtime),
                str(payload_path),
                str(lease["owner_id"]),
                str(lease["fence"]),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        provider_pid: int | None = None
        try:
            deadline = time.monotonic() + 10
            chunk_paths: list[Path] = []
            while time.monotonic() < deadline:
                chunk_paths = list(store.model_stage_raw_chunk_root.rglob("*.json"))
                if chunk_paths:
                    break
                if host.poll() is not None:
                    stdout, stderr = host.communicate()
                    self.fail(
                        f"host exited before first chunk: {stdout!r} {stderr!r}"
                    )
                time.sleep(0.02)
            self.assertEqual(len(chunk_paths), 1)
            chunk_path = chunk_paths[0]
            chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
            store._verify_seal(
                chunk, purpose="study-intake-model-stage-raw-chunk"
            )
            self.assertEqual(
                base64.b64decode(chunk["chunk_base64"]),
                b"first-provider-chunk",
            )
            identity_paths = list(
                store.provider_process_identity_root.rglob("*.json")
            )
            self.assertEqual(len(identity_paths), 1)
            provider_identity = json.loads(
                identity_paths[0].read_text(encoding="utf-8")
            )
            provider_pid = int(provider_identity["provider_pid"])
            os.kill(host.pid, signal.SIGKILL)
            host.wait(timeout=3)
            host.communicate()
            self.assertEqual(
                list(store.model_stage_raw_chain_manifest_root.rglob("*.json")),
                [],
            )
            self.assertEqual(
                list(store.model_stage_raw_output_root.rglob("*.json")), []
            )
            self.assertEqual(
                list(store.provider_process_exit_root.rglob("*.json")), []
            )
        finally:
            if host.poll() is None:
                os.kill(host.pid, signal.SIGKILL)
                host.wait(timeout=3)
                host.communicate()
            if provider_pid is not None:
                try:
                    os.killpg(provider_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_old_stage_timeout_alias_is_soft_warning_only(self) -> None:
        frozen = task(2)

        class LongButHealthy:
            def run_analysis(self, item, _context):
                time.sleep(0.20)
                return result("analysis", item)

            def run_critical_review(self, item, _draft, _context):
                return result("critical_review", item)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: LongButHealthy(),
            stage_timeout_seconds=0.05,
            stall_timeout_seconds=1.0,
            stall_probe_interval_seconds=0.02,
        )
        terminal = dispatcher.submit(frozen).wait(2)
        self.assertEqual(terminal.outcome, "succeeded")
        events = self._events(frozen)
        self.assertIn("soft_timeout_warning", [row["event"] for row in events])
        self.assertNotIn("timeout", [row["event"] for row in events])

    def test_execution_receipt_rejects_false_mcp_call_accounting(self) -> None:
        _runner, store, frozen = self._bound_codex_runner()
        lease = _runner._dispatch_process_lifecycle["lease"]
        common = {
            "stage_name": "math_analysis",
            "execution_status": "failed",
            "raw_output_object_sha256": None,
            "raw_output_object_ref": None,
            "provider_process_identity_sha256": "1" * 64,
            "provider_process_exit_sha256": "2" * 64,
            "authority_snapshot_manifest_sha256": None,
            "mcp_grounding_manifest_sha256": None,
            "mcp_transport_sha256": None,
            "mcp_transcript_sha256": None,
            "successful_mcp_tool_call_count": 0,
            "grounding_mcp_tool_call_count": 0,
            "failed_mcp_tool_call_count": 0,
            "last_mcp_error_code": None,
            "provider_returncode": 1,
            "duration_ms": 1,
        }
        for attempted in (True, -1, 1):
            with self.subTest(attempted=attempted), self.assertRaisesRegex(
                DispatchError, "model_stage_execution_count_invalid"
            ):
                store.publish_model_stage_execution_receipt(
                    frozen,
                    lease,
                    attempted_mcp_tool_call_count=attempted,
                    **common,
                )

    def test_live_provider_pid_alone_cannot_pass_data_plane_probe(self) -> None:
        runner = CodexRunner(
            {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            self.runtime,
        )
        process = subprocess.Popen(
            [sys.executable, "-c", "import time;time.sleep(10)"],
            start_new_session=True,
        )
        try:
            token = kernel_process_start_token(process.pid)
            with runner._process_lock:
                runner._active_processes.add(process)
                runner._active_process_stage[process] = "math_analysis"
                runner._active_process_start_token[process] = token
                runner._active_process_identity_refs[process] = {
                    "provider_process_identity_sha256": "a" * 64,
                    "provider_process_identity_path": "/tmp/unused-identity.json",
                }
                runner._provider_progress["math_analysis"] = {
                    "progress_receipt_sha256": "f" * 64,
                    "progress_kind": "provider_output",
                }
            probe = runner.respond_liveness_probe(
                stage_name="math_analysis",
                nonce="0" * 32,
                baseline_progress_receipt_sha256="f" * 64,
                provider_process_identity_sha256="a" * 64,
                provider_pid=process.pid,
                provider_pgid=process.pid,
                process_start_token=token,
                baseline_kernel_snapshot_sha256="b" * 64,
            )
            self.assertFalse(probe["provider_data_plane_ok"])
            self.assertIsNone(probe["progress_receipt_sha256"])
        finally:
            process.terminate()
            process.wait(timeout=2)

    def test_unverified_synthetic_probe_cannot_cancel_task_or_sibling(self) -> None:
        stalled_task = task(3)
        sibling = task(4)

        class Stalled:
            def __init__(self) -> None:
                self.stop = threading.Event()

            def run_analysis(self, item, _context):
                self.stop.wait(0.20)
                return result("analysis", item)

            def run_critical_review(self, item, _draft, _context):
                return result("critical_review", item)

            def probe_stage(self, _task, _context, *, stage, nonce):
                del stage
                return {
                    "probe_supported": True,
                    "probe_nonce": nonce,
                    "control_channel_ok": False,
                    "provider_data_plane_ok": False,
                }

            def cancel(self, _context):
                self.stop.set()

        class Healthy:
            def run_analysis(self, item, _context):
                return result("analysis", item)

            def run_critical_review(self, item, _draft, _context):
                return result("critical_review", item)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda item, _context: (
                Stalled() if item.unit_sha256 == stalled_task.unit_sha256 else Healthy()
            ),
            soft_runtime_warning_seconds=0.03,
            stall_timeout_seconds=0.05,
            stall_probe_interval_seconds=0.03,
            stall_probe_required_consecutive_failures=2,
        )
        terminals = dispatcher.dispatch([stalled_task, sibling])
        by_unit = {row.unit_sha256: row for row in terminals}
        self.assertEqual(by_unit[stalled_task.unit_sha256].outcome, "succeeded")
        self.assertEqual(by_unit[sibling.unit_sha256].outcome, "succeeded")
        failed_probes = [
            row
            for row in self._events(stalled_task)
            if row["event"] == "stall_probe_failed"
        ]
        self.assertEqual(failed_probes, [])

    def test_long_stage_without_verified_probe_is_not_wallclock_cancelled_and_issues_v2_v3(self) -> None:
        frozen = task(5)

        class Progressing:
            def run_analysis(self, item, _context):
                time.sleep(0.20)
                return result("analysis", item)

            def run_critical_review(self, item, _draft, _context):
                return result("critical_review", item)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _item, _context: Progressing(),
            soft_runtime_warning_seconds=0.05,
            stall_timeout_seconds=0.08,
            stall_probe_interval_seconds=0.02,
        )
        terminal = dispatcher.submit(frozen).wait(3)
        self.assertEqual(terminal.outcome, "succeeded")
        completion = dict(terminal.completion or {})
        self.assertEqual(completion["schema_version"], COMPLETION_SCHEMA)
        package = json.loads(
            Path(completion["package_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(package["schema_version"], PACKAGE_SCHEMA)
        report = json.loads(
            (
                self.runtime
                / "dispatch/reports/json/sha256"
                / completion["report_json_sha256"][:2]
                / f"{completion['report_json_sha256']}.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(report["schema_version"], "study-intake-dispatch-report-v2")
        self.assertTrue(
            all(row["schema_version"] == TASK_EVENT_SCHEMA for row in self._events(frozen))
        )
        self.assertNotIn("stage_stalled", [row["event"] for row in self._events(frozen)])


if __name__ == "__main__":
    unittest.main()
