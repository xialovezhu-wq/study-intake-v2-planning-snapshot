from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import LeaseStore  # noqa: E402
from fixtures.process_lifecycle import (  # noqa: E402
    ProcessRegistration,
    process_absent,
    register_process,
    stop_process,
    wait_for_progress,
)
from tests import test_production_canary_scanner_concurrency as canonical_support  # noqa: E402


DISPATCHER_FIXTURE = (
    ROOT / "tests" / "fixtures" / "production_dispatcher_process.py"
)
TASK_FIXTURE = ROOT / "tests" / "fixtures" / "blocking_production_runner.py"
SUBJECTS = ("math", "cs408", "english")


class ThreeDispatcherProcessTopologyTests(unittest.TestCase):
    """Prove the real scanner runs in three isolated dispatcher processes."""

    def setUp(self) -> None:
        self.helper = canonical_support.CanonicalProductionScannerIntegrationTests(
            methodName="runTest"
        )
        self.helper.setUp()
        self.addCleanup(self._cleanup_helper)
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self.registrations: dict[str, ProcessRegistration] = {}
        self.addCleanup(self._cleanup_processes)

    def _cleanup_helper(self) -> None:
        try:
            self.helper.tearDown()
        finally:
            self.helper.doCleanups()

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _stop_process(self, process: subprocess.Popen[str]) -> None:
        registration = next(
            (
                value
                for value in self.registrations.values()
                if value.process is process
            ),
            None,
        )
        if registration is None:
            if process.poll() is not None:
                process.wait(timeout=10)
            else:
                registration = register_process(
                    process, require_private_group=True
                )
        if registration is not None and not process_absent(registration):
            stop_process(
                registration,
                term_timeout=35,
                kill_timeout=10,
            )
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()

    def _cleanup_processes(self) -> None:
        for process in self.processes.values():
            self._stop_process(process)

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise AssertionError(f"not an object: {path}")
        return value

    def _wait_dispatcher_markers(
        self,
        marker_root: Path,
        *,
        statuses: set[str],
        idle_timeout: float = 90.0,
    ) -> dict[str, dict[str, object]]:
        latest: dict[str, dict[str, object]] = {}

        def observe() -> dict[str, dict[str, object]] | None:
            nonlocal latest
            latest = {}
            for subject in SUBJECTS:
                path = marker_root / "dispatchers" / f"{subject}.json"
                if not path.is_file():
                    continue
                latest[subject] = self._read_json(path)
            if len(latest) == 3 and all(
                row.get("status") in statuses for row in latest.values()
            ):
                return latest
            failed = {
                subject: row
                for subject, row in latest.items()
                if row.get("status") == "failed"
            }
            if failed:
                self.fail(f"dispatcher fixture failed: {failed}")
            return None

        try:
            return wait_for_progress(
                observe,
                progress_snapshot=lambda: json.dumps(latest, sort_keys=True),
                processes=self.processes.values(),
                idle_timeout=idle_timeout,
            )
        except TimeoutError as exc:
            self.fail(f"dispatcher marker stalled: {exc}; markers={latest}")

    def _wait_task_markers(
        self,
        marker_root: Path,
        expected_units: set[str],
        *,
        idle_timeout: float = 90.0,
    ) -> dict[str, dict[str, object]]:
        latest: dict[str, dict[str, object]] = {}

        def observe() -> dict[str, dict[str, object]] | None:
            nonlocal latest
            latest = {}
            for path in marker_root.glob("*.json"):
                row = self._read_json(path)
                unit = row.get("unit_sha256")
                if isinstance(unit, str) and unit in expected_units:
                    latest[unit] = row
            if set(latest) == expected_units:
                return latest
            return None

        try:
            return wait_for_progress(
                observe,
                progress_snapshot=lambda: (
                    len(latest),
                    tuple(sorted(latest)),
                ),
                processes=self.processes.values(),
                idle_timeout=idle_timeout,
            )
        except TimeoutError as exc:
            self.fail(
                "task marker stalled: "
                f"{exc}; observed={len(latest)} expected={len(expected_units)}"
            )

    @staticmethod
    def _line_count(path: Path) -> int:
        if not path.is_file():
            return 0
        return sum(
            1
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    @staticmethod
    def _jsonl_rows(path: Path) -> list[dict[str, object]]:
        if not path.is_file():
            return []
        rows: list[dict[str, object]] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise AssertionError(f"not a JSONL object: {path}")
            rows.append(value)
        return rows

    @staticmethod
    def _provider_identity_inventory(runtime_root: Path) -> set[str]:
        roots = (
            runtime_root / "dispatch" / "provider-process-identities",
            runtime_root / "dispatch" / "provider-process-exits",
        )
        return {
            str(path.relative_to(runtime_root))
            for root in roots
            if root.exists()
            for path in root.rglob("*.json")
        }

    def _launch_dispatchers(
        self,
        scenario: dict[str, object],
    ) -> None:
        marker_root = Path(scenario["marker_root"])
        fixtures = scenario["fixtures"]
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["BLOCKING_MARKER_ROOT"] = str(marker_root)
        environment["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        environment["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(
            scenario["payload_root"]
        )
        for subject in SUBJECTS:
            config_path = Path(fixtures[subject].base) / (
                f"canonical-shared-{subject}-config.json"
            )
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(DISPATCHER_FIXTURE),
                    "--subject",
                    subject,
                    "--config",
                    str(config_path),
                    "--marker-root",
                    str(marker_root),
                    "--runner",
                    str(TASK_FIXTURE),
                ],
                cwd=str(ROOT),
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            self.registrations[subject] = register_process(
                process, require_private_group=True
            )
            self.processes[subject] = process

    def _wait_for_heartbeat_advance(
        self,
        marker_root: Path,
        before: dict[str, int],
        subjects: tuple[str, ...],
        *,
        timeout: float = 5.0,
    ) -> dict[str, dict[str, object]]:
        deadline = time.monotonic() + timeout
        current: dict[str, dict[str, object]] = {}
        while time.monotonic() < deadline:
            current = {
                subject: self._read_json(
                    marker_root / "dispatchers" / f"{subject}.json"
                )
                for subject in subjects
            }
            if all(
                int(current[subject]["heartbeat_sequence"])
                > before[subject]
                for subject in subjects
            ):
                return current
            time.sleep(0.02)
        self.fail(f"dispatcher heartbeat did not advance: {current}")

    def test_three_os_dispatchers_scan_twenty_each_and_isolate_math_stop(
        self,
    ) -> None:
        scenario = self.helper._prepare_shared_canonical_runtimes()
        runtime_root = Path(scenario["shared_runtime"])
        marker_root = Path(scenario["marker_root"])

        # Canonical first captures run to a verified zero-model publication so
        # each durable subject gate is unlocked before the OS process proof.
        self.helper._run_shared_canonical_phase(
            scenario,
            count_by_subject={subject: 1 for subject in SUBJECTS},
            start_by_subject={
                "math": 4100,
                "cs408": 4200,
                "english": 4300,
            },
        )
        for subject in SUBJECTS:
            gate = scenario["runtimes"][
                subject
            ].dispatcher.lease_store.production_canary_status_read_only(
                subject
            )
            self.assertEqual(
                gate["state"], "continuous_concurrent_unlocked", gate
            )
            self.assertTrue(gate["luna_consumer_enabled"], gate)
            self.assertEqual(gate["continuous_concurrency_limit"], 20)
            self.assertEqual(gate["formal_write_count"], 0)

        candidates_by_subject: dict[str, list[object]] = {}
        for offset, subject in enumerate(SUBJECTS):
            candidates_by_subject[subject] = (
                self.helper._write_shared_subject_captures(
                    scenario,
                    subject=subject,
                    count=20,
                    start=5000 + offset * 100,
                )
            )
            self.assertEqual(len(candidates_by_subject[subject]), 20)

        component_lock = self._read_json(
            Path(scenario["host_config"]["component_lock_path"])
        )
        sealed_runtime = component_lock["mcp_sealed_runtime"]
        self.assertEqual(
            Path(scenario["host_config"]["mcp_client_python"]).resolve(),
            Path(sealed_runtime["python_executable"]).resolve(),
        )
        self.assertEqual(
            Path(scenario["host_config"]["mcp_project_root"]).resolve(),
            Path(sealed_runtime["release_root"]).resolve(),
        )
        provider_inventory_before = self._provider_identity_inventory(
            runtime_root
        )

        self._launch_dispatchers(scenario)
        waiting = self._wait_dispatcher_markers(
            marker_root,
            statuses={"waiting_for_dispatcher_barrier"},
        )
        self.assertEqual(
            len({int(row["pid"]) for row in waiting.values()}), 3
        )
        self.assertEqual(
            len({int(row["pgid"]) for row in waiting.values()}), 3
        )
        for subject, row in waiting.items():
            self.assertEqual(row["subject"], subject)
            self.assertEqual(row["pid"], self.processes[subject].pid)
            self.assertEqual(row["pid"], row["pgid"])
            self.assertIsNone(row["scan_worker_factory"])
            self.assertTrue(row["default_worker_scanner"])

        (marker_root / "dispatcher-start").write_text(
            "release\n", encoding="utf-8"
        )
        running = self._wait_dispatcher_markers(
            marker_root, statuses={"running"}
        )
        self.assertEqual(
            {str(row["release_id"]) for row in running.values()},
            {next(iter(running.values()))["release_id"]},
        )
        self.assertEqual(
            len({str(row["activation_id"]) for row in running.values()}), 3
        )
        self.assertEqual(
            len(
                {
                    str(row["producer_authority_fingerprint"])
                    for row in running.values()
                }
            ),
            3,
        )
        self.assertEqual(
            len({str(row["dispatcher_owner_id"]) for row in running.values()}),
            3,
        )
        self.assertEqual(
            {
                subject: row["submitted_task_count"]
                for subject, row in running.items()
            },
            {subject: 20 for subject in SUBJECTS},
            running,
        )
        self.assertTrue(
            all(row["effective_concurrency_limit"] == 20 for row in running.values())
        )
        for row in running.values():
            self.assertEqual(
                row["local_authority_snapshot_subprocess_count"], 1
            )
            self.assertEqual(row["real_mcp_request_count"], 0)
            self.assertEqual(row["unexpected_mcp_call_count"], 0)
            authority = row["gate_after_scan"]["authority"]
            self.assertEqual(authority["algorithm"], "HMAC-SHA256")
            self.assertEqual(
                authority["purpose"], "dispatch-production-canary-state"
            )

        unit_subject: dict[str, str] = {}
        for subject, row in running.items():
            units = row["submitted_units"]
            self.assertIsInstance(units, list)
            self.assertEqual(len(units), 20)
            for unit in units:
                self.assertIsInstance(unit, str)
                self.assertNotIn(unit, unit_subject)
                unit_subject[unit] = subject
        self.assertEqual(len(unit_subject), 60)

        task_markers = self._wait_task_markers(
            marker_root, set(unit_subject), idle_timeout=90
        )
        task_pids = {int(row["pid"]) for row in task_markers.values()}
        task_pgids = {int(row["pgid"]) for row in task_markers.values()}
        self.assertEqual(len(task_pids), 60)
        self.assertEqual(len(task_pgids), 60)
        self.assertTrue(
            all(int(row["pid"]) == int(row["pgid"]) for row in task_markers.values())
        )
        self.assertTrue(all(self._pid_alive(pid) for pid in task_pids))
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(
                len({str(row[field]) for row in task_markers.values()}), 60
            )

        store = LeaseStore(runtime_root)
        owner_ids: dict[str, set[str]] = {subject: set() for subject in SUBJECTS}
        heartbeat_values: dict[str, set[str]] = {
            subject: set() for subject in SUBJECTS
        }
        for unit, subject in unit_subject.items():
            lease = self._read_json(store.lease_root / f"{unit}.json")
            self.assertEqual(lease["subject"], subject)
            self.assertEqual(lease["status"], "claimed")
            self.assertEqual(lease["fence"], 1)
            owner = str(lease["owner_id"])
            self.assertTrue(
                owner.startswith(f"dispatcher-{self.processes[subject].pid}-")
            )
            owner_ids[subject].add(owner)
            heartbeat_values[subject].add(str(lease["heartbeat_at"]))
        self.assertTrue(all(len(values) == 1 for values in owner_ids.values()))
        self.assertEqual(
            len({next(iter(values)) for values in owner_ids.values()}), 3
        )
        self.assertTrue(all(values for values in heartbeat_values.values()))

        telemetry = store.production_canary_concurrency_telemetry(
            release_id=str(next(iter(running.values()))["release_id"])
        )
        for subject in SUBJECTS:
            self.assertGreaterEqual(
                telemetry["subject_peak_active"][subject], 20, telemetry
            )
        self.assertGreaterEqual(telemetry["global_peak_active"], 60, telemetry)
        self.assertEqual(telemetry["runner_interval_missing_count_global"], 0)

        heartbeat_before = {
            subject: int(row["heartbeat_sequence"])
            for subject, row in running.items()
        }
        self._wait_for_heartbeat_advance(
            marker_root, heartbeat_before, SUBJECTS
        )

        math_task_pids = {
            int(task_markers[unit]["pid"])
            for unit, subject in unit_subject.items()
            if subject == "math"
        }
        sibling_task_pids = {
            int(task_markers[unit]["pid"])
            for unit, subject in unit_subject.items()
            if subject in {"cs408", "english"}
        }
        sibling_heartbeat_before = {
            subject: int(
                self._read_json(
                    marker_root / "dispatchers" / f"{subject}.json"
                )["heartbeat_sequence"]
            )
            for subject in ("cs408", "english")
        }
        math_process = self.processes["math"]
        self._stop_process(math_process)
        self.assertEqual(math_process.returncode, 0)
        math_final = self._read_json(
            marker_root / "dispatchers" / "math.json"
        )
        self.assertEqual(math_final["status"], "drained", math_final)
        self.assertEqual(math_final["active_task_count"], 0, math_final)
        self.assertEqual(
            math_final["shutdown"]["late_result_fence_status"],
            "sealed",
            math_final,
        )

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(
            self._pid_alive(pid) for pid in math_task_pids
        ):
            time.sleep(0.02)
        self.assertFalse(any(self._pid_alive(pid) for pid in math_task_pids))
        self.assertTrue(all(self._pid_alive(pid) for pid in sibling_task_pids))
        for subject in ("cs408", "english"):
            self.assertIsNone(self.processes[subject].poll())
        sibling_rows = self._wait_for_heartbeat_advance(
            marker_root,
            sibling_heartbeat_before,
            ("cs408", "english"),
        )
        self.assertTrue(
            all(row["status"] == "running" for row in sibling_rows.values())
        )
        for subject in ("cs408", "english"):
            gate = store.production_canary_status_read_only(subject)
            self.assertEqual(
                gate["state"], "continuous_concurrent_unlocked", gate
            )
            self.assertTrue(gate["luna_consumer_enabled"], gate)
            self.assertEqual(gate["active_task_count"], 20, gate)
            self.assertEqual(gate["formal_write_count"], 0)
        math_gate = store.production_canary_status_read_only("math")
        self.assertFalse(math_gate["luna_consumer_enabled"], math_gate)
        self.assertEqual(math_gate["active_task_count"], 0, math_gate)
        self.assertEqual(math_gate["formal_write_count"], 0)

        for subject in ("cs408", "english"):
            self._stop_process(self.processes[subject])
            self.assertEqual(self.processes[subject].returncode, 0)
            final = self._read_json(
                marker_root / "dispatchers" / f"{subject}.json"
            )
            self.assertEqual(final["status"], "drained", final)
            self.assertEqual(final["active_task_count"], 0, final)
            for field in (
                "model_call_count",
                "provider_request_count",
                "mcp_call_count",
                "real_mcp_request_count",
                "unexpected_mcp_call_count",
                "sol_call_count",
                "formal_write_count",
            ):
                self.assertEqual(final[field], 0, final)
            self.assertEqual(
                final["local_authority_snapshot_subprocess_count"], 1, final
            )
        for field in (
            "model_call_count",
            "provider_request_count",
            "mcp_call_count",
            "real_mcp_request_count",
            "unexpected_mcp_call_count",
            "sol_call_count",
            "formal_write_count",
        ):
            self.assertEqual(math_final[field], 0, math_final)
        self.assertEqual(
            math_final["local_authority_snapshot_subprocess_count"], 1, math_final
        )

        self.assertEqual(
            self._provider_identity_inventory(runtime_root),
            provider_inventory_before,
        )


if __name__ == "__main__":
    unittest.main()
