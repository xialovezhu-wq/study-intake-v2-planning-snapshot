from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "scripts"))

import mixed_luna_production as mixed_production  # noqa: E402
from mixed_luna_production import (  # noqa: E402
    build_selection,
    load_math_new_business_task,
    prepare_adapters,
)
from mixed_luna_stress import MixedLunaStressError, RUN_COUNTS, validate_selection  # noqa: E402
import run_mixed_luna_stress as mixed_cli  # noqa: E402
from run_mixed_luna_stress import _bind_candidate_release, _workers  # noqa: E402


class _FixtureAdapter:
    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.prepared = False

    def selection_rows(self, run_mode: str):
        rows = []
        for index in range(1, RUN_COUNTS[run_mode][self.subject] + 1):
            task_id = f"{run_mode}:{self.subject}:{index}"
            row = {
                "task_id": task_id,
                "unit_sha256": hashlib.sha256(task_id.encode()).hexdigest(),
                "subject": self.subject,
                "subject_scope_sha256": hashlib.sha256(
                    f"scope:{self.subject}".encode()
                ).hexdigest(),
                "subject_batch_id": f"batch:{run_mode}:{self.subject}",
                "subject_batch_sha256": hashlib.sha256(
                    f"batch:{run_mode}:{self.subject}".encode()
                ).hexdigest(),
                "dispatcher_id": f"dispatcher:{self.subject}",
                "mcp_namespace": f"kaoyan_{self.subject}_read",
                "generation": f"generation:{self.subject}",
                "authority_fingerprint": hashlib.sha256(
                    f"authority:{self.subject}".encode()
                ).hexdigest(),
                "read_session_binding_sha256": hashlib.sha256(
                    f"session:{task_id}".encode()
                ).hexdigest(),
                "sol_authorized": False,
                "formal_write_count": 0,
            }
            rows.append(row)
        return rows

    def prepare(self) -> None:
        self.prepared = True

    def run(self, task, observe):  # pragma: no cover - interface fixture
        raise AssertionError("model path must not run in selection tests")

    def verify(self, task, result):  # pragma: no cover - interface fixture
        raise AssertionError("artifact path must not run in selection tests")

    def close(self) -> None:
        return None


class MixedLunaProductionTests(unittest.TestCase):
    @staticmethod
    def _controlled_english_locator_probe(
        root: Path,
        *,
        capture_id: str,
        study_date: str,
        input_fingerprint: str,
    ):
        unit_sha256 = "5" * 64
        task_id = f"english-controlled:{unit_sha256}"
        frozen = SimpleNamespace(
            frozen_payload={
                "capture_id": capture_id,
                "study_date": study_date,
                "input_fingerprint": input_fingerprint,
            },
            unit_sha256=unit_sha256,
        )
        adapter = mixed_production.ControlledReplaySubjectMixedAdapter.__new__(
            mixed_production.ControlledReplaySubjectMixedAdapter
        )
        adapter.subject = "english"
        adapter.config = {"runtime_root": str(root)}
        adapter._tasks_by_id = {task_id: frozen}
        return adapter, {"task_id": task_id}

    @staticmethod
    def _write_minimal_english_reopen_authority(
        root: Path,
        *,
        capture_id: str,
        package_sha256: str,
    ) -> None:
        for kind, value in (
            ("jobs", {}),
            (
                "latest",
                {
                    "schema_version": "study-intake-preprocess-latest-v1",
                    "package_sha256": package_sha256,
                },
            ),
        ):
            path = root / "state" / kind / "english" / f"{capture_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(mixed_production.json_file_bytes(value))

    def test_controlled_english_reopen_uses_canonical_v1_package_path(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(
            prefix="mixed-controlled-english-v1-"
        ) as raw:
            root = Path(raw).resolve()
            capture_id = "EN@CAP+1"
            study_date = "2026-08-10"
            input_fingerprint = "4" * 64
            payload = b"{}\n"
            package_sha256 = hashlib.sha256(payload).hexdigest()
            expected = mixed_production.canonical_subject_package_path(
                root,
                subject="english",
                study_date=study_date,
                capture_id=capture_id,
                input_fingerprint=input_fingerprint,
                package_sha256=package_sha256,
            )
            expected.parent.mkdir(parents=True, exist_ok=True)
            expected.write_bytes(payload)
            adapter, task = self._controlled_english_locator_probe(
                root,
                capture_id=capture_id,
                study_date=study_date,
                input_fingerprint=input_fingerprint,
            )
            seen = []

            def stop_after_locator(path, code):
                seen.append((path, code))
                raise MixedLunaStressError("locator_probe_stop")

            with (
                mock.patch.object(
                    mixed_production,
                    "reopen_verified_subject_publication",
                    return_value={"subject_package_sha256": package_sha256},
                ),
                mock.patch.object(
                    mixed_production,
                    "_load_object",
                    side_effect=stop_after_locator,
                ),
                self.assertRaises(MixedLunaStressError) as caught,
            ):
                adapter._reopen_closure(task)
            self.assertEqual(caught.exception.code, "locator_probe_stop")
            self.assertEqual(
                seen,
                [(expected, "mixed_controlled_package_invalid")],
            )
            self.assertFalse(
                (root / "packages" / "objects" / f"{package_sha256}.json").exists()
            )

    def test_controlled_english_reopen_does_not_fallback_to_objects_alias(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(
            prefix="mixed-controlled-english-no-alias-"
        ) as raw:
            root = Path(raw).resolve()
            capture_id = "EN-ALIAS-1"
            study_date = "2026-08-10"
            input_fingerprint = "6" * 64
            payload = b"{}\n"
            package_sha256 = hashlib.sha256(payload).hexdigest()
            alias = root / "packages" / "objects" / f"{package_sha256}.json"
            alias.parent.mkdir(parents=True, exist_ok=True)
            alias.write_bytes(payload)
            canonical = mixed_production.canonical_subject_package_path(
                root,
                subject="english",
                study_date=study_date,
                capture_id=capture_id,
                input_fingerprint=input_fingerprint,
                package_sha256=package_sha256,
            )
            self.assertFalse(canonical.exists())
            self._write_minimal_english_reopen_authority(
                root,
                capture_id=capture_id,
                package_sha256=package_sha256,
            )
            adapter, task = self._controlled_english_locator_probe(
                root,
                capture_id=capture_id,
                study_date=study_date,
                input_fingerprint=input_fingerprint,
            )
            with (
                mock.patch.object(mixed_production, "_load_object") as loader,
                self.assertRaises(MixedLunaStressError) as caught,
            ):
                adapter._reopen_closure(task)
            self.assertEqual(
                caught.exception.code,
                "subject_publication_reopen_invalid",
            )
            loader.assert_not_called()
            self.assertTrue(alias.is_file())
            self.assertFalse(canonical.exists())

    def test_controlled_english_reopen_rejects_package_symlinks(self) -> None:
        for mode in ("leaf", "ancestor"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(
                prefix=f"mixed-controlled-english-{mode}-symlink-"
            ) as raw:
                root = Path(raw).resolve()
                capture_id = "EN-SYMLINK-1"
                study_date = "2026-08-10"
                input_fingerprint = "7" * 64
                payload = b"{}\n"
                package_sha256 = hashlib.sha256(payload).hexdigest()
                canonical = mixed_production.canonical_subject_package_path(
                    root,
                    subject="english",
                    study_date=study_date,
                    capture_id=capture_id,
                    input_fingerprint=input_fingerprint,
                    package_sha256=package_sha256,
                )
                canonical.parent.mkdir(parents=True, exist_ok=True)
                canonical.write_bytes(payload)
                if mode == "leaf":
                    outside = root / "outside-package.json"
                    canonical.rename(outside)
                    canonical.symlink_to(outside)
                else:
                    outside = root / "outside-capture-directory"
                    canonical.parent.rename(outside)
                    canonical.parent.symlink_to(
                        outside,
                        target_is_directory=True,
                    )
                self._write_minimal_english_reopen_authority(
                    root,
                    capture_id=capture_id,
                    package_sha256=package_sha256,
                )
                adapter, task = self._controlled_english_locator_probe(
                    root,
                    capture_id=capture_id,
                    study_date=study_date,
                    input_fingerprint=input_fingerprint,
                )
                with (
                    mock.patch.object(mixed_production, "_load_object") as loader,
                    self.assertRaises(MixedLunaStressError) as caught,
                ):
                    adapter._reopen_closure(task)
                self.assertEqual(
                    caught.exception.code,
                    "subject_publication_reopen_invalid",
                )
                loader.assert_not_called()

    def test_math_smoke_selects_only_null_formal_id_new_source(self) -> None:
        def unit(capture_id: str, formal_id: str | None, route: str):
            candidate = SimpleNamespace(
                capture_id=capture_id,
                input_binding={"source_route": route},
            )
            task = SimpleNamespace(unit_sha256=hashlib.sha256(capture_id.encode()).hexdigest())
            return SimpleNamespace(
                formal_id=formal_id,
                candidate=candidate,
                task=task,
            )

        execution = SimpleNamespace(
            units=(
                unit("existing-1", "GS-109", "old_existing"),
                unit("existing-2", "GS-507", "old_existing"),
                unit("new-1", None, "new_intake"),
            ),
            scope_sha256="1" * 64,
            authority_manifest_sha256="2" * 64,
        )

        class Runtime:
            def __init__(self) -> None:
                self.registered = []

            def register_controlled_replay_candidate(self, task, candidate, *, reason):
                self.registered.append((task, candidate, reason))

        runtime = Runtime()
        with mock.patch.object(
            mixed_production,
            "build_live_math_direct_mcp_execution",
            return_value=execution,
        ):
            _scope, tasks, candidates = load_math_new_business_task(
                config={},
                release_id="a" * 64,
                live_manifest_path=Path("unused.json"),
                runtime=runtime,
                execution_attempt=1,
                execution_runtime_id="runtime-1",
            )
        self.assertEqual(["new-1"], [row.capture_id for row in candidates])
        self.assertEqual(1, len(tasks))
        self.assertEqual(1, len(runtime.registered))

    def test_direct_runtime_config_gets_exact_candidate_release_binding(self) -> None:
        release_id = "a" * 64
        original = {"schema_version": "study-intake-preprocessor-config-v1"}
        bound = _bind_candidate_release(original, release_id)
        self.assertNotIn("authority_release_id", original)
        self.assertEqual(bound["authority_release_id"], release_id)
        with self.assertRaisesRegex(
            MixedLunaStressError, "mixed_candidate_release_mismatch"
        ):
            _bind_candidate_release(
                {"authority_release_id": "b" * 64}, release_id
            )

    def test_selection_is_exact_three_new_business_units(self) -> None:
        adapters = tuple(
            _FixtureAdapter(subject) for subject in ("english", "math", "cs408")
        )
        selection, indexed = build_selection(
            campaign_id="mixed-production-fixture",
            candidate_release_id="a" * 64,
            execution_runtime_sha256="d" * 64,
            run_mode="smoke_3",
            execution_attempt=1,
            max_workers=3,
            adapters=adapters,
        )
        checked = validate_selection(selection)
        self.assertEqual(len(checked["tasks"]), 3)
        self.assertEqual(checked["max_workers"], 3)
        self.assertEqual(
            {
                subject: sum(row["subject"] == subject for row in checked["tasks"])
                for subject in ("english", "math", "cs408")
            },
            {"english": 1, "math": 1, "cs408": 1},
        )
        self.assertEqual(
            len({row["read_session_binding_sha256"] for row in checked["tasks"]}),
            3,
        )
        prepare_adapters(indexed)
        self.assertTrue(all(adapter.prepared for adapter in adapters))
        self.assertEqual(3, _workers("smoke_3", None))

    def test_subject_timeout_cancels_only_its_handle_and_waits_terminal(
        self,
    ) -> None:
        task = _FixtureAdapter("math").selection_rows("smoke_3")[0]
        frozen = object()

        class TimeoutHandle:
            def __init__(handle_self, runtime: object) -> None:
                handle_self.runtime = runtime
                handle_self.cancel_calls = 0
                handle_self.wait_timeouts = []

            def cancel(handle_self) -> bool:
                handle_self.cancel_calls += 1
                return True

            def wait(handle_self, timeout=None):
                handle_self.wait_timeouts.append(timeout)
                if len(handle_self.wait_timeouts) == 1:
                    raise TimeoutError("dispatch_handle_wait_timeout")
                handle_self.runtime.active_count = 0
                return SimpleNamespace(
                    outcome="cancelled",
                    error_code="dispatch_cancelled",
                )

        class Runtime:
            def __init__(runtime_self) -> None:
                runtime_self.active_count = 1
                runtime_self.dispatcher = SimpleNamespace(cancel=mock.Mock())
                runtime_self.handle = TimeoutHandle(runtime_self)

            def submit_prepared_controlled_replay_task(
                runtime_self, observed_frozen: object
            ):
                self.assertIs(frozen, observed_frozen)
                return runtime_self.handle

        runtime = Runtime()
        adapter = mixed_production.ControlledReplaySubjectMixedAdapter.__new__(
            mixed_production.ControlledReplaySubjectMixedAdapter
        )
        adapter.subject = "math"
        adapter.runtime = runtime
        adapter.task_timeout_seconds = 0.01
        adapter._prepared = True
        adapter._rows = {task["task_id"]: task}
        adapter._tasks_by_id = {task["task_id"]: frozen}

        with self.assertRaises(MixedLunaStressError) as caught:
            adapter.run(task, lambda _event, _observed_at=None: None)
        self.assertEqual(
            "mixed_controlled_dispatch_failed", caught.exception.code
        )
        self.assertEqual(1, runtime.handle.cancel_calls)
        self.assertEqual([0.01, None], runtime.handle.wait_timeouts)
        self.assertEqual(0, runtime.active_count)
        runtime.dispatcher.cancel.assert_not_called()

    def test_revoked_legacy_counts_are_absent(self) -> None:
        self.assertNotIn("smoke_20", RUN_COUNTS)
        self.assertNotIn("full_108", RUN_COUNTS)
        with self.assertRaises(KeyError):
            _workers("full_108", 108)

    def test_mixed_gate_allows_only_new_business_capture_lanes(self) -> None:
        args = SimpleNamespace(
            p0_matrix=Path("matrix.json"),
            p0_audit_root=Path("audit"),
            golden_inventory=Path("golden.json"),
            english_existing_inventory_receipt_root=Path("receipts"),
            english_existing_inventory_closure_sha256="a" * 64,
            english_existing_inventory_authority_key=Path("key"),
        )
        evaluated = {
            "blocking_issue_ids": [],
            "matrix_sha256": "b" * 64,
            "golden_inventory_sha256": "c" * 64,
        }
        with mock.patch.object(
            mixed_cli,
            "require_model_lane_ready",
            return_value=evaluated,
        ):
            gate = mixed_cli._require_gate(args)
        self.assertEqual(
            gate["decision"], "allow_three_subject_new_business_smoke_only"
        )
        self.assertEqual(gate["math_lane"], "candidate_bound_new_business_capture")
        self.assertEqual(gate["cs408_lane"], "candidate_bound_controlled_replay")
        self.assertTrue(gate["ordinary_english_allowed"])
        self.assertFalse(gate["ordinary_golden_allowed"])
        self.assertTrue(gate["global_model_lane_ready"])

    def test_mixed_gate_rejects_any_remaining_p0_blocker(self) -> None:
        args = SimpleNamespace(
            p0_matrix=Path("matrix.json"),
            p0_audit_root=Path("audit"),
            golden_inventory=Path("golden.json"),
            english_existing_inventory_receipt_root=Path("receipts"),
            english_existing_inventory_closure_sha256="a" * 64,
            english_existing_inventory_authority_key=Path("key"),
        )
        with mock.patch.object(
            mixed_cli,
            "require_model_lane_ready",
            side_effect=mixed_cli.P0GateError("pre_model_p0_gate_blocked"),
        ):
            with self.assertRaisesRegex(
                MixedLunaStressError,
                "pre_model_p0_gate_blocked",
            ):
                mixed_cli._require_gate(args)


if __name__ == "__main__":
    unittest.main()
