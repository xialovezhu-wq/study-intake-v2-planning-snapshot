from __future__ import annotations

import copy
import json
import os
import signal
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import ConcurrentDispatcher  # noqa: E402
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateSubprocessRunner,
    scan_eligible_candidates,
)
from preprocessor_core import atomic_write_json  # noqa: E402
from tests import test_english_adapter as english_support  # noqa: E402


FAKE_CODEX = ROOT / "tests/fixtures/blocking_zero_model_codex.py"
TASK_RUNNER = ROOT / "tests/fixtures/provider_lifecycle_task_runner.py"


def _process_absent(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _process_group_absent(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


class ProviderProcessHardCancelTests(unittest.TestCase):
    """Exercise the real supervisor→Provider cancellation tree, zero-model."""

    def setUp(self) -> None:
        self.fixture = english_support.EnglishAdapterTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        for index in range(1, 6):
            self.fixture.write_sentence(index)
        self.config = copy.deepcopy(self.fixture.config)
        self.config["model"]["codex_path"] = str(FAKE_CODEX)
        self.config_path = self.fixture.base / "hard-cancel-config.json"
        atomic_write_json(self.config_path, self.config)
        self.marker = self.fixture.base / "provider-grandchild.json"
        self.old_marker = os.environ.get("ZERO_MODEL_CODEX_MARKER")
        self.old_activity_mode = os.environ.get(
            "ZERO_MODEL_PROVIDER_ACTIVITY_MODE"
        )
        os.environ["ZERO_MODEL_CODEX_MARKER"] = str(self.marker)
        os.environ["ZERO_MODEL_PROVIDER_ACTIVITY_MODE"] = "frozen"
        self.addCleanup(self._restore_marker)

    def _restore_marker(self) -> None:
        if self.old_marker is None:
            os.environ.pop("ZERO_MODEL_CODEX_MARKER", None)
        else:
            os.environ["ZERO_MODEL_CODEX_MARKER"] = self.old_marker
        if self.old_activity_mode is None:
            os.environ.pop("ZERO_MODEL_PROVIDER_ACTIVITY_MODE", None)
        else:
            os.environ["ZERO_MODEL_PROVIDER_ACTIVITY_MODE"] = (
                self.old_activity_mode
            )

    def _wait_marker(self, timeout: float = 10.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.marker.is_file():
                return json.loads(self.marker.read_text(encoding="utf-8"))
            time.sleep(0.02)
        self.fail("zero-model Provider grandchild did not start")

    def _start(
        self,
        *,
        timeout: float,
        stall_timeout: float = 3600.0,
        stall_probe_interval: float = 0.10,
    ):
        frozen, decisions = scan_eligible_candidates(self.config, "english")
        self.assertEqual(len(frozen), 1, decisions)
        selected = frozen[0]
        dispatcher = None
        dispatcher = ConcurrentDispatcher(
            self.fixture.runtime,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                self.config_path,
                selected.reason,
                command=[sys.executable, str(TASK_RUNNER)],
                lease_store=dispatcher.lease_store,
            ),
            stage_timeout_seconds=timeout,
            stall_timeout_seconds=stall_timeout,
            stall_probe_interval_seconds=stall_probe_interval,
            stall_probe_required_consecutive_failures=2,
        )
        handle = dispatcher.submit(selected.task)
        marker = self._wait_marker()
        identity_index_path = (
            dispatcher.lease_store.provider_process_identity_latest_root
            / selected.task.unit_sha256
            / "fence-1"
            / "english_analysis.json"
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not identity_index_path.is_file():
            time.sleep(0.02)
        self.assertTrue(identity_index_path.is_file())
        identity_index = json.loads(
            identity_index_path.read_text(encoding="utf-8")
        )
        identity = json.loads(
            Path(identity_index["provider_process_identity_path"]).read_text(
                encoding="utf-8"
            )
        )
        return dispatcher, selected, handle, marker, identity

    def _events(self, dispatcher, selected) -> list[dict[str, object]]:
        root = (
            dispatcher.lease_store.task_event_index_root
            / selected.task.unit_sha256
            / "fence-1"
        )
        rows: list[dict[str, object]] = []
        for path in sorted(root.glob("*.json")):
            index = json.loads(path.read_text(encoding="utf-8"))
            rows.append(
                json.loads(
                    Path(index["event_path"]).read_text(encoding="utf-8")
                )
            )
        return rows

    def _assert_closed(
        self,
        dispatcher,
        selected,
        marker: dict[str, object],
        identity: dict[str, object],
        *,
        provider_reason: str,
        supervisor_reason: str,
    ) -> None:
        provider_pid = int(marker["pid"])
        provider_pgid = int(marker["pgid"])
        supervisor_pid = int(identity["supervisor_pid"])
        supervisor_pgid = int(identity["supervisor_pgid"])
        provider_exit_index = json.loads(
            (
                dispatcher.lease_store.provider_process_exit_latest_root
                / selected.task.unit_sha256
                / "fence-1"
                / "english_analysis.json"
            ).read_text(encoding="utf-8")
        )
        provider_exit = json.loads(
            Path(provider_exit_index["provider_process_exit_path"]).read_text(
                encoding="utf-8"
            )
        )
        supervisor_exit_index = json.loads(
            (
                dispatcher.lease_store.task_process_exit_latest_root
                / selected.task.unit_sha256
                / "fence-1.json"
            ).read_text(encoding="utf-8")
        )
        supervisor_exit = json.loads(
            Path(supervisor_exit_index["task_process_exit_path"]).read_text(
                encoding="utf-8"
            )
        )
        for receipt in (provider_exit, supervisor_exit):
            self.assertTrue(receipt["reaped"])
            self.assertTrue(receipt["process_absent"])
            self.assertTrue(receipt["pgid_absent"])
            self.assertFalse(receipt["late_result_publish_allowed"])
        self.assertEqual(provider_exit["termination_reason"], provider_reason)
        self.assertEqual(supervisor_exit["termination_reason"], supervisor_reason)
        self.assertEqual(
            provider_exit["process_start_token"], identity["process_start_token"]
        )
        self.assertTrue(_process_absent(provider_pid))
        self.assertTrue(_process_group_absent(provider_pgid))
        self.assertTrue(_process_absent(supervisor_pid))
        self.assertTrue(_process_group_absent(supervisor_pgid))
        self.assertIsNone(identity["requested_service_tier"])
        self.assertFalse(identity["fast_mode_requested"])
        self.assertNotIn("service_tier", " ".join(identity["argv"]))
        self.assertEqual(identity["forbidden_environment_key_matches"], [])
        self.assertEqual(
            identity["forbidden_environment_value_key_matches"], []
        )

    def test_sigterm_reaps_provider_grandchild_and_supervisor(self) -> None:
        dispatcher, selected, handle, marker, identity = self._start(timeout=30)
        self.assertNotEqual(int(identity["supervisor_pid"]), int(marker["pid"]))
        self.assertTrue(handle.cancel("daemon_shutdown"))
        result = handle.wait(15)
        self.assertEqual(result.outcome, "cancelled", result)
        self.assertEqual(result.error_code, "daemon_shutdown")
        self._assert_closed(
            dispatcher,
            selected,
            marker,
            identity,
            provider_reason="cancelled",
            supervisor_reason="cancelled",
        )

    def test_real_busy_provider_kernel_activity_prevents_stall(self) -> None:
        os.environ["ZERO_MODEL_PROVIDER_ACTIVITY_MODE"] = "busy"
        dispatcher, selected, handle, marker, identity = self._start(
            timeout=0.05,
            stall_timeout=0.20,
            stall_probe_interval=0.08,
        )
        deadline = time.monotonic() + 10
        healthy_probe = None
        while time.monotonic() < deadline and healthy_probe is None:
            for row in self._events(dispatcher, selected):
                artifacts = row.get("artifacts")
                if (
                    row.get("event") == "stall_probe"
                    and isinstance(artifacts, dict)
                    and artifacts.get(
                        "stall_probe_provider_kernel_activity_ok"
                    )
                    is True
                ):
                    healthy_probe = row
                    break
            time.sleep(0.02)
        self.assertIsNotNone(healthy_probe)
        self.assertTrue(
            healthy_probe["artifacts"][
                "stall_probe_automatic_cancellation_eligible"
            ]
        )
        self.assertFalse(
            healthy_probe["artifacts"][
                "stall_probe_provider_data_plane_ok"
            ]
        )
        time.sleep(0.35)
        self.assertFalse(handle.done)
        self.assertTrue(handle.cancel("test_cleanup"))
        result = handle.wait(15)
        self.assertEqual(result.outcome, "cancelled", result)
        self.assertNotEqual(result.error_code, "analysis_stalled")
        receipt_path = Path(
            healthy_probe["artifacts"][
                "provider_kernel_probe_receipt_path"
            ]
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertTrue(receipt["kernel_activity_detected"])
        self.assertEqual(receipt["provider_pid"], int(marker["pid"]))
        self.assertEqual(
            receipt["process_start_token"], identity["process_start_token"]
        )

    def test_real_frozen_provider_two_failed_rounds_stall_and_fence(self) -> None:
        dispatcher, selected, handle, marker, identity = self._start(
            timeout=0.05,
            stall_timeout=0.20,
            stall_probe_interval=0.08,
        )
        result = handle.wait(15)
        self.assertEqual(result.outcome, "stalled", result)
        self.assertEqual(result.error_code, "analysis_stalled")
        events = self._events(dispatcher, selected)
        failed = [
            row for row in events if row.get("event") == "stall_probe_failed"
        ]
        self.assertEqual(len(failed), 2)
        for row in failed:
            artifacts = row["artifacts"]
            self.assertTrue(artifacts["stall_probe_control_channel_ok"])
            self.assertFalse(
                artifacts["stall_probe_provider_kernel_activity_ok"]
            )
            self.assertFalse(
                artifacts["stall_probe_provider_data_plane_ok"]
            )
            self.assertTrue(
                artifacts["stall_probe_automatic_cancellation_eligible"]
            )
        self._assert_closed(
            dispatcher,
            selected,
            marker,
            identity,
            provider_reason="cancelled",
            supervisor_reason="cancelled",
        )
        completion = result.completion or {}
        self.assertIsNone(completion.get("package_path"))
        self.assertIsNone(completion.get("package_sha256"))

    def test_activity_between_probe_windows_renews_chained_baseline(self) -> None:
        os.environ["ZERO_MODEL_PROVIDER_ACTIVITY_MODE"] = (
            "burst_then_frozen"
        )
        os.environ["ZERO_MODEL_PROVIDER_BURST_SECONDS"] = "0.38"
        self.addCleanup(
            os.environ.pop, "ZERO_MODEL_PROVIDER_BURST_SECONDS", None
        )
        dispatcher, selected, handle, _marker, _identity = self._start(
            timeout=0.05,
            stall_timeout=0.15,
            stall_probe_interval=0.18,
        )
        result = handle.wait(15)
        self.assertEqual(result.outcome, "stalled", result)
        probes = [
            row
            for row in self._events(dispatcher, selected)
            if row.get("event") in {"stall_probe", "stall_probe_failed"}
        ]
        self.assertGreaterEqual(len(probes), 3)
        self.assertEqual(probes[0]["event"], "stall_probe")
        self.assertTrue(
            probes[0]["artifacts"][
                "stall_probe_provider_kernel_activity_ok"
            ]
        )
        healthy_after_first = [
            row
            for row in probes[1:]
            if row["artifacts"][
                "stall_probe_provider_kernel_activity_ok"
            ]
        ]
        self.assertTrue(
            healthy_after_first,
            "activity between probe sample windows was not carried by the "
            "previous-final to current-final receipt chain",
        )
        failed = [row for row in probes if row["event"] == "stall_probe_failed"]
        self.assertEqual(len(failed), 2)
        first_failed_index = probes.index(failed[0])
        self.assertGreater(first_failed_index, 1)
        receipt = json.loads(
            Path(
                healthy_after_first[0]["artifacts"][
                    "provider_kernel_probe_receipt_path"
                ]
            ).read_text(encoding="utf-8")
        )
        self.assertIsNotNone(receipt["previous_probe_receipt_sha256"])

    def test_old_wall_timeout_is_soft_only_until_explicit_cancel(self) -> None:
        dispatcher, selected, handle, marker, identity = self._start(timeout=0.05)
        time.sleep(0.20)
        self.assertFalse(handle.done)
        self.assertTrue(handle.cancel("user_explicit_cancel"))
        result = handle.wait(15)
        self.assertEqual(result.outcome, "cancelled", result)
        self.assertEqual(result.error_code, "user_explicit_cancel")
        self._assert_closed(
            dispatcher,
            selected,
            marker,
            identity,
            provider_reason="cancelled",
            supervisor_reason="cancelled",
        )

    def test_direct_supervisor_sigterm_reaps_provider_grandchild(self) -> None:
        dispatcher, selected, handle, marker, identity = self._start(timeout=30)
        os.kill(int(identity["supervisor_pid"]), signal.SIGTERM)
        self.assertTrue(handle.cancel("daemon_shutdown"))
        result = handle.wait(15)
        self.assertEqual(result.outcome, "cancelled", result)
        self.assertEqual(result.error_code, "daemon_shutdown")
        self._assert_closed(
            dispatcher,
            selected,
            marker,
            identity,
            provider_reason="cancelled",
            supervisor_reason="cancelled",
        )


if __name__ == "__main__":
    unittest.main()
