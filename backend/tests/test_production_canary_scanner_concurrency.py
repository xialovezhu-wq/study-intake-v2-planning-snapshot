from __future__ import annotations

import copy
import concurrent.futures
import hashlib
import json
import os
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    Lease,
    LeaseStore,
)
from core_dispatch_bridge import CoreCandidateSubprocessRunner  # noqa: E402
from processing_plugin import ProcessingPluginHost  # noqa: E402
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
import preprocessor_core as core  # noqa: E402
from test_concurrent_dispatch import core_candidate  # noqa: E402
from test_production_canary_admission import authority, canary_task  # noqa: E402
from tests.test_subject_sol_contract import ControlPlaneTestCase  # noqa: E402
from tests.test_three_subject_production_quality_closure import (  # noqa: E402
    ThreeSubjectProductionQualityClosureTests,
)
from tests import test_english_adapter as english_support  # noqa: E402
from tests import test_preprocessor as cs408_support  # noqa: E402
from tests import test_math_formalization_contract as math_formalization_support  # noqa: E402
from tests import test_math_v2_core as math_support  # noqa: E402
from tests import test_three_subject_sealed_chain_replay as sealed_support  # noqa: E402


FIXTURE = ROOT / "tests" / "fixtures" / "blocking_production_runner.py"
RELEASE_ID = "a" * 64
ACTIVATED_AT = "2026-08-11T00:00:00Z"


def _candidate_value(candidate: core.Candidate, reason: str) -> dict[str, object]:
    return {
        "candidate": {
            "subject": candidate.subject,
            "capture_id": candidate.capture_id,
            "study_date": candidate.study_date,
            "recorded_at": candidate.recorded_at,
            "input_fingerprint": candidate.input_fingerprint,
            "input_binding": candidate.input_binding,
            "model_input": candidate.model_input,
            "allowed_evidence_refs": list(candidate.allowed_evidence_refs),
            "image_paths": [str(path) for path in candidate.image_paths],
            "target_label": candidate.target_label,
            "canonical_state": candidate.canonical_state,
            "sol_state": candidate.sol_state,
            "private_context": candidate.private_context,
        },
        "reason": reason,
    }


class _AppendOnlyProducerWorker:
    """Temp-only adapter over an append-only producer JSONL authority."""

    def __init__(self, config, ledger_path: Path):
        self.release_id = core.LOADED_CORE_SHA256
        self.runtime_root = Path(str(config["runtime_root"]))
        self.ledger_path = ledger_path
        self.adapters = {}

    def scan_statuses(self, _study_date, *, only_subject=None):
        pending = []
        if self.ledger_path.exists():
            for raw in self.ledger_path.read_text(encoding="utf-8").splitlines():
                if not raw.strip():
                    continue
                candidate = json.loads(raw)["candidate"]
                if only_subject is not None and candidate["subject"] != only_subject:
                    continue
                pending.append(
                    {
                        "event_id": candidate["capture_id"],
                        "recorded_at": candidate["recorded_at"],
                    }
                )
        return {str(only_subject or "math"): {"pending": pending}}

    def eligible_candidates(self, subject, _study_date, **kwargs):
        rows = []
        capture_allowlist = kwargs.get("capture_allowlist")
        if not self.ledger_path.exists():
            return rows
        for raw in self.ledger_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            value = json.loads(raw)
            candidate_value = value["candidate"]
            candidate = core.Candidate(
                **{
                    **candidate_value,
                    "allowed_evidence_refs": tuple(
                        candidate_value["allowed_evidence_refs"]
                    ),
                    "image_paths": tuple(
                        Path(path) for path in candidate_value["image_paths"]
                    ),
                }
            )
            if candidate.subject != subject:
                continue
            if (
                capture_allowlist is not None
                and candidate.capture_id not in capture_allowlist
            ):
                continue
            # A real producer ledger is append-only; eligibility is a consumer
            # projection.  Reopen the dispatch latest index instead of deleting
            # or rewriting the producer event after completion.
            latest = (
                self.runtime_root
                / "dispatch/state/latest"
                / subject
                / f"{candidate.capture_id}.json"
            )
            if latest.exists():
                continue
            rows.append((candidate, str(value["reason"])))
        return rows


class _PassThroughSubjectSol:
    def luna_admission(self, _subject: str) -> dict[str, object]:
        return {
            "read_session_allowed": True,
            "reason": "admitted",
        }

    def submit_luna_under_generation_fence(
        self, _subject: str, submit, task: FrozenTask
    ):
        return submit(task)


def _with_behavior(task: FrozenTask, behavior: str) -> FrozenTask:
    payload = dict(task.frozen_payload)
    payload["test_behavior"] = behavior
    return FrozenTask(payload)


def _decision(task: FrozenTask) -> dict[str, object]:
    payload = task.frozen_payload
    return {
        "subject": payload["subject"],
        "capture_id": payload["capture_id"],
        "study_date": payload["study_date"],
        "input_fingerprint": payload["input_fingerprint"],
        "unit_sha256": task.unit_sha256,
        "frozen_payload_sha256": task.frozen_payload_sha256,
        "eligible": True,
        "reason": "test_zero_model_scanner_fixture",
        "phase": "eligible",
        "model_enqueue_allowed": True,
        "formal_write_count": 0,
    }


class ProductionCanaryScannerConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temp.name) / "runtime"
        self.config_path = Path(self.temp.name) / "config.json"
        self.config_path.write_text("{}\n", encoding="utf-8")
        self.marker_root = Path(self.temp.name) / "markers"
        self._fixture_env = {
            name: os.environ.get(name)
            for name in (
                "BLOCKING_MARKER_ROOT",
                "BLOCKING_DEFAULT_BEHAVIOR",
                "BLOCKING_FAIL_CAPTURE_ID",
                "ZERO_MODEL_STAGE_PAYLOAD_ROOT",
            )
        }
        os.environ["BLOCKING_MARKER_ROOT"] = str(self.marker_root)
        for name in (
            "BLOCKING_DEFAULT_BEHAVIOR",
            "BLOCKING_FAIL_CAPTURE_ID",
            "ZERO_MODEL_STAGE_PAYLOAD_ROOT",
        ):
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._fixture_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temp.cleanup()

    def _arm(self, subject: str) -> LeaseStore:
        store = LeaseStore(self.runtime_root)
        store.begin_subject_drain(subject)
        store.activate_production_canary(
            subject,
            release_id=RELEASE_ID,
            producer_authority=authority(subject, RELEASE_ID),
            activated_at=ACTIVATED_AT,
            continuous_concurrency_limit=20,
        )
        return store

    def _runtime(self, subject: str) -> ProductionDispatchRuntime:
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.config = {
            "runtime_root": str(self.runtime_root),
            "timezone": "UTC",
        }
        runtime.subject = subject
        runtime.config_path = self.config_path
        runtime.runtime_root = self.runtime_root
        runtime.production_canary = True
        runtime.continuous_concurrency_limit = 20
        runtime.processing_host = None
        runtime.subject_sol = _PassThroughSubjectSol()
        runtime._frozen_batch_authority = None
        runtime._direct_controlled_candidates = {}
        runtime._projection_fail_closed = False
        store = LeaseStore(self.runtime_root)
        runtime.dispatcher = ConcurrentDispatcher(
            self.runtime_root,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                self.config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            ),
            stage_timeout_seconds=10,
            production_canary=True,
        )
        return runtime

    def _scan(
        self, runtime: ProductionDispatchRuntime, tasks: list[FrozenTask]
    ):
        decisions = [_decision(task) for task in tasks]
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(
                [SimpleNamespace(task=task) for task in tasks],
                decisions,
            ),
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            return runtime.scan_and_submit()

    def _unlock(self, runtime: ProductionDispatchRuntime, subject: str) -> None:
        first = _with_behavior(
            canary_task(
                1,
                subject=subject,
                recorded_at="2026-08-11T00:00:01Z",
            ),
            "healthy",
        )
        handles, _ = self._scan(runtime, [first])
        self.assertEqual(len(handles), 1)
        result = handles[0].wait(10)
        self.assertEqual(result.outcome, "succeeded", result)
        self.assertEqual(
            runtime.dispatcher.lease_store.production_canary_status(subject)[
                "state"
            ],
            "continuous_concurrent_unlocked",
        )

    def _wait_for_markers(
        self, expected_units: set[str], *, timeout: float = 10
    ) -> dict[str, dict[str, object]]:
        deadline = time.monotonic() + timeout
        values: dict[str, dict[str, object]] = {}
        while time.monotonic() < deadline:
            values = {}
            for path in self.marker_root.glob("*.json"):
                value = json.loads(path.read_text(encoding="utf-8"))
                if value.get("unit_sha256") in expected_units:
                    values[str(value["unit_sha256"])] = value
            if set(values) == expected_units:
                return values
            time.sleep(0.01)
        self.fail(f"only {len(values)}/{len(expected_units)} scanner children started")

    def test_production_infrastructure_exit_is_terminal_without_retry(
        self,
    ) -> None:
        subject = "english"
        store = self._arm(subject)
        runtime = self._runtime(subject)
        task = _with_behavior(
            canary_task(
                90,
                subject=subject,
                recorded_at="2026-08-11T00:00:02Z",
            ),
            "healthy",
        )
        missing_payload_root = Path(self.temp.name) / "missing-stage-payloads"
        with mock.patch.dict(
            os.environ,
            {"ZERO_MODEL_STAGE_PAYLOAD_ROOT": str(missing_payload_root)},
            clear=False,
        ):
            handles, decisions = self._scan(runtime, [task])
            self.assertEqual(len(handles), 1, decisions)
            result = handles[0].wait(10)

        self.assertEqual(result.status, "completed", result)
        self.assertEqual(result.outcome, "failed", result)
        self.assertEqual(result.error_code, "task_process_exit_1", result)
        self.assertIsNotNone(result.completion)
        self.assertEqual(result.completion["lease_fence"], 1)
        identity_fence_one = (
            store.task_process_identity_latest_root
            / task.unit_sha256
            / "fence-1.json"
        )
        exit_fence_one = (
            store.task_process_exit_latest_root
            / task.unit_sha256
            / "fence-1.json"
        )
        self.assertTrue(identity_fence_one.is_file())
        self.assertTrue(exit_fence_one.is_file())
        self.assertFalse(
            (
                store.task_process_identity_latest_root
                / task.unit_sha256
                / "fence-2.json"
            ).exists()
        )
        self.assertFalse(
            (
                store.task_process_exit_latest_root
                / task.unit_sha256
                / "fence-2.json"
            ).exists()
        )
        lease = json.loads(
            store._lease_path(task.unit_sha256).read_text(encoding="utf-8")
        )
        self.assertEqual(lease["fence"], 1)
        self.assertEqual(lease["infrastructure_recovery_count"], 0)
        state = store.production_canary_status(subject)
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["terminal_by_outcome"]["failed"], 1)
        self.assertEqual(state["active_task_count"], 0)

    def test_canary_binding_fault_precedes_identity_publication(self) -> None:
        subject = "math"
        store = self._arm(subject)
        task = canary_task(
            91,
            subject=subject,
            recorded_at="2026-08-11T00:00:03Z",
        )
        store.materialize_production_canary_task(task)
        owner_id = f"dispatcher-{os.getpid()}-{'1' * 32}"
        decision = store.claim(
            task.unit_sha256,
            owner_id,
            subject=subject,
            task=task,
            production_canary=True,
        )
        self.assertEqual(decision.status, "claimed")
        self.assertIsNotNone(decision.lease)
        recovered = store.recover_after_infrastructure_crash(
            decision.lease,
            error_code="task_process_exit_1",
        )
        self.assertEqual(recovered.fence, 2)

        with mock.patch(
            "concurrent_dispatch.require_kernel_process_start_token"
        ), mock.patch(
            "concurrent_dispatch._publish_content_addressed"
        ) as publish_object, mock.patch(
            "concurrent_dispatch._publish_named_immutable"
        ) as publish_index, self.assertRaisesRegex(
            DispatchError,
            "production_canary_process_identity_binding_invalid",
        ):
            store.publish_task_process_identity(
                task,
                recovered,
                child_pid=999_991,
                child_pgid=999_991,
                process_start_token="pid=999991;start=1",
                launch_nonce="2" * 32,
                launched_at="2026-08-11T00:00:04Z",
                argv=[sys.executable, str(FIXTURE)],
                executable_path=Path(sys.executable),
                start_new_session=True,
            )
        publish_object.assert_not_called()
        publish_index.assert_not_called()
        self.assertFalse(
            (
                store.task_process_identity_latest_root
                / task.unit_sha256
                / "fence-2.json"
            ).exists()
        )

    def test_unlocked_real_scanner_submits_twenty_isolated_task_processes(
        self,
    ) -> None:
        subject = "math"
        store = self._arm(subject)
        runtime = self._runtime(subject)
        self._unlock(runtime, subject)
        tasks = [
            _with_behavior(
                canary_task(
                    100 + index,
                    subject=subject,
                    recorded_at=f"2026-08-11T00:01:{index:02d}Z",
                ),
                "block_all",
            )
            for index in range(20)
        ]
        handles, decisions = self._scan(runtime, tasks)
        self.assertEqual(len(handles), 20)
        self.assertEqual(sum(row["model_enqueue_allowed"] is True for row in decisions), 20)
        expected_units = {task.unit_sha256 for task in tasks}
        markers = self._wait_for_markers(expected_units)
        self.assertEqual(len({row["pid"] for row in markers.values()}), 20)
        self.assertTrue(all(row["pid"] == row["pgid"] for row in markers.values()))
        for key in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(len({row[key] for row in markers.values()}), 20)
        (self.marker_root / "release-all").touch()
        results = [handle.wait(15) for handle in handles]
        self.assertEqual([result.outcome for result in results], ["succeeded"] * 20)
        for task in tasks:
            lease = json.loads(
                (
                    self.runtime_root
                    / "dispatch/state/leases"
                    / f"{task.unit_sha256}.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(lease["status"], "completed")
            identity_index = json.loads(
                (
                    store.task_process_identity_latest_root
                    / task.unit_sha256
                    / "fence-1.json"
                ).read_text(encoding="utf-8")
            )
            exit_index = json.loads(
                (
                    store.task_process_exit_latest_root
                    / task.unit_sha256
                    / "fence-1.json"
                ).read_text(encoding="utf-8")
            )
            self.assertNotEqual(
                identity_index["process_identity_sha256"],
                exit_index["task_process_exit_sha256"],
            )
        telemetry = store.production_canary_concurrency_telemetry(
            release_id=RELEASE_ID
        )
        self.assertGreaterEqual(
            telemetry["subject_peak_active"][subject], 20, telemetry
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 20, telemetry)
        self.assertEqual(
            telemetry["runner_interval_missing_count_global"], 0
        )

    def test_one_task_failure_drains_subject_but_nineteen_siblings_finish(
        self,
    ) -> None:
        subject = "english"
        store = self._arm(subject)
        runtime = self._runtime(subject)
        self._unlock(runtime, subject)
        tasks = [
            _with_behavior(
                canary_task(
                    200 + index,
                    subject=subject,
                    recorded_at=f"2026-08-11T00:02:{index:02d}Z",
                ),
                "fail_after_release" if index == 0 else "block_all",
            )
            for index in range(20)
        ]
        handles, _ = self._scan(runtime, tasks)
        self.assertEqual(len(handles), 20)
        expected_units = {task.unit_sha256 for task in tasks}
        markers = self._wait_for_markers(expected_units)
        self.assertEqual(len(markers), 20)
        (self.marker_root / "release-all").touch()
        results = [handle.wait(15) for handle in handles]
        self.assertEqual(
            sum(result.outcome == "failed" for result in results),
            1,
            results,
        )
        self.assertEqual(sum(result.outcome == "succeeded" for result in results), 19)
        self.assertFalse(any(result.outcome == "cancelled" for result in results))
        state = store.production_canary_status(subject)
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["active_task_count"], 0)
        self.assertEqual(
            state["terminal_by_outcome"],
            {
                "succeeded": 20,
                "failed": 1,
                "cancelled": 0,
                "timed_out": 0,
                "stalled": 0,
                "needs_rework": 0,
            },
        )
        public_status = store.subject_status(subject)
        failed_units = {
            row["unit_sha256"]
            for row in public_status["terminal_failure_bindings"]
        }
        self.assertEqual(failed_units, {tasks[0].unit_sha256})
        self.assertEqual(state["queue_depth"], 0)


class InjectedAppendOnlyScannerUnitTests(unittest.TestCase):
    """Narrow fixture-only scanner and SubjectSol control-path coverage.

    This class intentionally injects an append-only worker and manually closes
    the quality receipt.  It is not the canonical producer-to-scanner proof and
    must not be counted as such.  It never invokes a model, Provider, MCP or
    Sol.
    """

    def setUp(self) -> None:
        self.support = ThreeSubjectProductionQualityClosureTests(
            methodName="runTest"
        )
        self.support.setUp()
        self.addCleanup(self.support.doCleanups)
        self.marker_roots: list[Path] = []
        self._fixture_env = {
            name: os.environ.get(name)
            for name in (
                "BLOCKING_MARKER_ROOT",
                "BLOCKING_DEFAULT_BEHAVIOR",
                "BLOCKING_FAIL_CAPTURE_ID",
                "ZERO_MODEL_STAGE_PAYLOAD_ROOT",
            )
        }
        for name in (
            "BLOCKING_DEFAULT_BEHAVIOR",
            "BLOCKING_FAIL_CAPTURE_ID",
            "ZERO_MODEL_STAGE_PAYLOAD_ROOT",
        ):
            os.environ.pop(name, None)
        self.addCleanup(self._restore_fixture_env)

    def _restore_fixture_env(self) -> None:
        for name, value in self._fixture_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    @staticmethod
    def _canary_config(config: dict) -> dict:
        value = json.loads(json.dumps(config))
        value["model"].pop("service_tier", None)
        value["authority_release_id"] = core.LOADED_CORE_SHA256
        value["dispatch"]["production_canary"] = {
            "enabled": True,
            "status": "production_canary_active",
            "admission": "first_post_activation_producer_capture",
            "keep_backlog_drained": True,
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
        }
        return value

    @staticmethod
    def _processing_contract_sha256(config: dict, subject: str) -> str:
        builder = {
            "math": core.math_processing_contract,
            "cs408": core.cs408_processing_contract,
            "english": core.english_processing_contract,
        }[subject]
        return str(builder(config)["processing_contract_sha256"])

    def _candidate(
        self,
        *,
        subject: str,
        index: int,
        processing_contract_sha256: str,
        behavior: str = "healthy",
    ) -> core.Candidate:
        candidate = core_candidate(40_000 + index, subject=subject)
        input_binding = dict(candidate.input_binding)
        input_binding["processing_contract_sha256"] = (
            processing_contract_sha256
        )
        input_binding["test_behavior"] = behavior
        evidence_ref = core.model_mcp_item_ref(
            subject=subject,
            generation=f"{subject}-zero-model-fixture-generation",
            collection="producer-ledger-test",
            stable_id=f"capture-{index}",
            source_hash=__import__("hashlib").sha256(
                f"{subject}:{index}".encode()
            ).hexdigest(),
        )
        model_input = dict(candidate.model_input)
        model_input["producer_ledger_sequence"] = index
        model_input["test_behavior"] = behavior
        return replace(
            candidate,
            capture_id=f"REAL-SCANNER-{subject.upper()}-{index:04d}",
            recorded_at=f"2026-08-11T00:{1 + index // 60:02d}:{index % 60:02d}Z",
            input_fingerprint=__import__("hashlib").sha256(
                f"real-scanner:{subject}:{index}".encode()
            ).hexdigest(),
            input_binding=input_binding,
            model_input=model_input,
            allowed_evidence_refs=(evidence_ref,),
            image_paths=(),
            private_context={"test_behavior": behavior},
        )

    @staticmethod
    def _append(ledger: Path, candidates: list[core.Candidate]) -> None:
        ledger.parent.mkdir(parents=True, exist_ok=True)
        with ledger.open("a", encoding="utf-8") as handle:
            for candidate in candidates:
                handle.write(
                    json.dumps(
                        _candidate_value(candidate, "append_only_producer_ready"),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

    def _runtime(self, subject: str):
        prepared = self.support._capture_prepared_subject(subject)
        config = self._canary_config(prepared["config"])
        runtime_root = Path(str(config["runtime_root"]))
        ledger = runtime_root / "producer-authority" / f"{subject}.jsonl"
        config_path = runtime_root / "real-scanner-config.json"
        config_path.write_text(
            json.dumps(config, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        runtime = ProductionDispatchRuntime(
            config,
            subject,
            config_path,
            scan_worker_factory=lambda cfg: _AppendOnlyProducerWorker(
                cfg, ledger
            ),
        )
        generation = f"{subject}-zero-model-fixture-generation"
        authority_fingerprint = __import__("hashlib").sha256(
            f"authority:{subject}:zero-model-fixture".encode()
        ).hexdigest()
        runtime.processing_host = SimpleNamespace(
            subject_authority_snapshot=lambda requested: {
                "subject": requested,
                "generation": generation,
                "authority_fingerprint": authority_fingerprint,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
            }
        )
        marker_root = runtime_root / "zero-model-runner-markers" / subject
        marker_root.mkdir(parents=True, exist_ok=True)
        payload_root = runtime_root / "zero-model-stage-payloads" / subject
        payload_root.mkdir(parents=True, exist_ok=True)
        self.marker_roots.append(marker_root)
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(payload_root)
        runtime.dispatcher.runner_factory = (
            lambda _task, _context: CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=runtime.dispatcher.lease_store,
            )
        )
        runtime.dispatcher.lease_store.begin_subject_drain(subject)
        runtime.activate_production_canary(
            activated_at=ACTIVATED_AT,
            expected_producer_authority_fingerprint=None,
        )
        return runtime, ledger, marker_root, payload_root, config

    @staticmethod
    def _close_test_quality_and_rollover(
        runtime: ProductionDispatchRuntime, handle
    ) -> dict:
        result = handle.wait(15)
        if result.outcome != "succeeded":
            raise AssertionError(result)
        projected = runtime.persist_finished_luna([handle])
        if len(projected) != 1:
            raise AssertionError(projected)
        batch = runtime.subject_sol.read_subject_batch(runtime.subject)
        if batch.get("all_terminal") is not True or batch.get("sol_ready") is not True:
            raise AssertionError(batch)
        return runtime.subject_sol.rollover_background_luna_batch(
            runtime.subject, mode="auto_success"
        )

    @staticmethod
    def _injected_stage_runtime(
        *, subject: str, unit_sha256: str, evidence_ref: str
    ) -> dict[str, dict[str, object]]:
        read_session_id = f"ZERO-MODEL-FIXTURE-{subject}-{unit_sha256}"
        shared = {
            "read_session_id": read_session_id,
            "read_session_manifest_sha256": hashlib.sha256(
                f"read-session:{read_session_id}".encode()
            ).hexdigest(),
            "evidence_generation": f"{subject}-zero-model-fixture-generation",
            "evidence_authority_fingerprint": hashlib.sha256(
                f"authority:{subject}:zero-model-fixture".encode()
            ).hexdigest(),
            "evidence_subject": subject,
            "evidence_release_id": core.LOADED_CORE_SHA256,
            "mcp_consumed_evidence_refs": [evidence_ref],
            "mcp_cited_evidence_refs": [evidence_ref],
            "mcp_stage_grounded_evidence_refs": [evidence_ref],
            "provider_request_count": 2,
            "mcp_tool_call_count": 1,
            "model_call_count": 1,
            "normalization_status": "normalized",
            "normalization_warning_count": 0,
            "normalization_warnings": [],
        }
        rows: dict[str, dict[str, object]] = {}
        for stage_name in ("analysis", "critical_review"):
            row = dict(shared)
            row["mcp_grounding_manifest_sha256"] = hashlib.sha256(
                f"{stage_name}:{unit_sha256}:{evidence_ref}".encode()
            ).hexdigest()
            for field, namespace, ref_prefix in (
                (
                    "raw_output_object_sha256",
                    "raw-output",
                    "study-intake-model-stage-raw-output://sha256/",
                ),
                (
                    "stage_execution_receipt_sha256",
                    "execution-receipt",
                    "study-intake-model-stage-execution://sha256/",
                ),
                (
                    "stage_normalization_receipt_sha256",
                    "normalization-receipt",
                    "study-intake-model-stage-normalization://sha256/",
                ),
            ):
                digest = hashlib.sha256(
                    f"{namespace}:{unit_sha256}:{stage_name}".encode()
                ).hexdigest()
                row[field] = digest
                row[field.removesuffix("_sha256") + "_ref"] = ref_prefix + digest
            rows[stage_name] = row
        return rows

    @classmethod
    def _publish_injected_stage_payload(
        cls,
        *,
        payload_root: Path,
        subject: str,
        handle,
    ) -> None:
        evidence_ref = str(handle.task.frozen_payload["allowed_evidence_refs"][0])
        unit_sha256 = handle.unit_sha256
        payload = {
            "analysis": {
                "stage": "analysis",
                "unit_sha256": unit_sha256,
                "fixture_scope": "temp_zero_model_only",
                "evidence_refs": [evidence_ref],
            },
            "critical_review": {
                "stage": "critical_review",
                "unit_sha256": unit_sha256,
                "fixture_scope": "temp_zero_model_only",
                "evidence_refs": [evidence_ref],
            },
            "stage_runtime": cls._injected_stage_runtime(
                subject=subject,
                unit_sha256=unit_sha256,
                evidence_ref=evidence_ref,
            ),
        }
        core.atomic_write_json(payload_root / f"{unit_sha256}.json", payload)

    def _wait_for_markers(
        self, marker_root: Path, units: set[str], timeout: float = 15
    ) -> dict[str, dict[str, object]]:
        deadline = time.monotonic() + timeout
        observed: dict[str, dict[str, object]] = {}
        while time.monotonic() < deadline:
            observed = {
                str(value["unit_sha256"]): value
                for path in marker_root.glob("*.json")
                for value in [json.loads(path.read_text(encoding="utf-8"))]
                if value.get("unit_sha256") in units
            }
            if set(observed) == units:
                return observed
            time.sleep(0.01)
        self.fail(f"only {len(observed)}/{len(units)} children started")

    @staticmethod
    def _scan(runtime: ProductionDispatchRuntime):
        # Dashboard projection has its own v2 migration suite.  Keep this
        # scanner/process test scoped to the producer, dispatcher and SubjectSol
        # authorities while still executing the real scan_and_submit method.
        with mock.patch("preprocess_dispatcher._write_subject_projections"):
            return runtime.scan_and_submit()

    def test_injected_append_only_scanner_first_success_then_twenty_run(
        self,
    ) -> None:
        subject = "math"
        runtime, ledger, marker_root, payload_root, config = self._runtime(subject)
        contract_sha = self._processing_contract_sha256(config, subject)
        first = self._candidate(
            subject=subject,
            index=1,
            processing_contract_sha256=contract_sha,
            behavior="block_all",
        )
        self._append(ledger, [first])
        first_handles, first_decisions = self._scan(runtime)
        self.assertEqual(len(first_handles), 1, first_decisions)
        first = first_handles[0]
        self._wait_for_markers(marker_root, {first.unit_sha256})
        self._publish_injected_stage_payload(
            payload_root=payload_root,
            subject=subject,
            handle=first,
        )
        (marker_root / "release-all").touch()
        rollover = self._close_test_quality_and_rollover(
            runtime, first
        )
        self.assertEqual(rollover["receipt"]["mode"], "auto_success")
        self.assertEqual(rollover["receipt"]["formal_write_count"], 0)
        self.assertFalse(rollover["receipt"]["sol_called"])
        (marker_root / "release-all").unlink()
        state = runtime.dispatcher.lease_store.production_canary_status(subject)
        self.assertEqual(state["state"], "continuous_concurrent_unlocked")

        candidates = [
            self._candidate(
                subject=subject,
                index=100 + index,
                processing_contract_sha256=contract_sha,
                behavior="block_all",
            )
            for index in range(20)
        ]
        self._append(ledger, candidates)
        handles, decisions = self._scan(runtime)
        self.assertEqual(len(handles), 20, decisions)
        units = {handle.unit_sha256 for handle in handles}
        markers = self._wait_for_markers(marker_root, units)
        self.assertEqual(
            {row["behavior"] for row in markers.values()},
            {"block_all"},
            markers,
        )
        self.assertFalse(any(handle.done for handle in handles))
        self.assertEqual(len({row["pid"] for row in markers.values()}), 20)
        self.assertTrue(
            all(row["pid"] == row["pgid"] for row in markers.values())
        )
        for handle in handles:
            self._publish_injected_stage_payload(
                payload_root=payload_root,
                subject=subject,
                handle=handle,
            )
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(len({row[field] for row in markers.values()}), 20)
        (marker_root / "release-all").touch()
        results = [handle.wait(20) for handle in handles]
        self.assertEqual([row.outcome for row in results], ["succeeded"] * 20)
        telemetry = (
            runtime.dispatcher.lease_store.production_canary_concurrency_telemetry(
                release_id=core.LOADED_CORE_SHA256
            )
        )
        self.assertGreaterEqual(
            telemetry["subject_peak_active"][subject], 20, telemetry
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 20, telemetry)
        self.assertEqual(telemetry["runner_interval_missing_count_global"], 0)

class CanonicalProductionScannerIntegrationTests(unittest.TestCase):
    """Canonical producer/adapter/SubjectSol scanner proofs with zero models."""

    def setUp(self) -> None:
        self.marker_env = {
            name: os.environ.get(name)
            for name in (
                "BLOCKING_MARKER_ROOT",
                "BLOCKING_DEFAULT_BEHAVIOR",
                "BLOCKING_FAIL_CAPTURE_ID",
                "ZERO_MODEL_STAGE_PAYLOAD_ROOT",
            )
        }
        self.support = ThreeSubjectProductionQualityClosureTests(
            methodName="runTest"
        )

    def tearDown(self) -> None:
        for name, value in self.marker_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.support.doCleanups()

    @staticmethod
    def _enable_canary(config: dict, host_config: dict) -> dict:
        configured = copy.deepcopy(config)
        configured["model"]["codex_path"] = "/usr/bin/false"
        release_manifest = (
            Path(str(configured["runtime_root"]))
            / "canonical-scanner-release.json"
        )
        core.atomic_write_json(
            release_manifest,
            {
                "schema_version": core.RELEASE_SCHEMA,
                "release_id": core.LOADED_CORE_SHA256,
            },
        )
        configured["release"] = {"manifest_path": str(release_manifest)}
        configured_host = copy.deepcopy(host_config)
        subject_repo_roots = configured_host.pop(
            "_canonical_subject_repo_roots", None
        )
        configured["processing_plugin"] = configured_host
        if isinstance(subject_repo_roots, dict):
            configured["subject_repo_roots"] = subject_repo_roots
        configured.setdefault("dispatch", {})["production_canary"] = {
            "enabled": True,
            "status": "production_canary_active",
            "admission": "first_post_activation_producer_capture",
            "keep_backlog_drained": True,
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
        }
        return configured

    @staticmethod
    def _install_authority_stub(
        *,
        runtime_root: Path,
        fixture,
        host_config: dict,
    ) -> tuple[ProcessingPluginHost, dict]:
        """Build a host from the current sealed component lock unchanged."""
        configured = copy.deepcopy(host_config)
        subject_repo_roots = configured.pop(
            "_canonical_subject_repo_roots", None
        )
        host = ProcessingPluginHost(
            configured,
            runtime_root=runtime_root,
            candidate_release_id=core.LOADED_CORE_SHA256,
        )
        fixture.runtime = runtime_root
        fixture.config = configured
        fixture.host = host
        if isinstance(subject_repo_roots, dict):
            configured["_canonical_subject_repo_roots"] = subject_repo_roots
        return host, configured

    @staticmethod
    def _stage_runtime(
        publication: dict, receipts: dict, *, subject: str
    ) -> dict[str, dict[str, object]]:
        rows: dict[str, dict[str, object]] = {}
        for stage_name in ("analysis", "critical_review"):
            receipt = receipts[stage_name]
            manifest = receipt["mcp_grounding_manifest"]
            refs = [row["evidence_ref"] for row in manifest["items"]]
            rows[stage_name] = {
                "read_session_id": publication["read_session_id"],
                "read_session_manifest_sha256": publication[
                    "read_session_manifest_sha256"
                ],
                "evidence_generation": publication["evidence_generation"],
                "evidence_authority_fingerprint": publication[
                    "evidence_authority_fingerprint"
                ],
                "evidence_subject": subject,
                "evidence_release_id": core.LOADED_CORE_SHA256,
                "mcp_grounding_manifest_sha256": manifest[
                    "manifest_sha256"
                ],
                "mcp_consumed_evidence_refs": refs,
                "mcp_cited_evidence_refs": refs,
                "mcp_stage_grounded_evidence_refs": refs,
                "provider_request_count": int(
                    receipt["provider_request_count"]
                ),
                "mcp_tool_call_count": int(receipt["mcp_tool_call_count"]),
                "model_call_count": 1,
            }
            # The zero-model task runner receives this sealed local stage
            # payload and copies it into its signed dispatch completion.  Keep
            # its v2 closure shape complete without launching a Provider.
            for field, namespace, ref_prefix in (
                (
                    "raw_output_object_sha256",
                    "raw-output",
                    "study-intake-model-stage-raw-output://sha256/",
                ),
                (
                    "stage_execution_receipt_sha256",
                    "execution-receipt",
                    "study-intake-model-stage-execution://sha256/",
                ),
                (
                    "stage_normalization_receipt_sha256",
                    "normalization-receipt",
                    "study-intake-model-stage-normalization://sha256/",
                ),
            ):
                digest = hashlib.sha256(
                    (
                        f"{namespace}:{subject}:{stage_name}:"
                        f"{publication['read_session_id']}"
                    ).encode()
                ).hexdigest()
                rows[stage_name][field] = digest
                rows[stage_name][field.removesuffix("_sha256") + "_ref"] = (
                    ref_prefix + digest
                )
            rows[stage_name]["normalization_status"] = "normalized"
            rows[stage_name]["normalization_warning_count"] = 0
            rows[stage_name]["normalization_warnings"] = []
        return rows

    @staticmethod
    def _wait_for_unit_markers(
        marker_root: Path,
        units: set[str],
        *,
        timeout_seconds: float = 30,
    ) -> dict[str, dict[str, object]]:
        deadline = time.monotonic() + timeout_seconds
        markers: dict[str, dict[str, object]] = {}
        while time.monotonic() < deadline:
            markers = {}
            for path in marker_root.glob("*.json"):
                value = core.load_json(path)
                unit = str(value.get("unit_sha256") or "")
                if unit in units:
                    markers[unit] = value
            if set(markers) == units:
                return markers
            time.sleep(0.01)
        return markers

    @staticmethod
    def _configure_zero_model_runner(
        runtime: ProductionDispatchRuntime,
        config_path: Path,
    ) -> None:
        store = runtime.dispatcher.lease_store
        runtime.dispatcher.runner_factory = lambda _task, _context: (
            CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            )
        )

    @staticmethod
    def _canonical_candidates(
        *,
        subject: str,
        config: dict,
        fixture,
    ) -> list[core.Candidate]:
        worker = core.Worker(config)
        if subject == "cs408":
            status = worker.adapters["cs408"].status("2026-08-11")
            candidates = worker.adapters["cs408"].candidates(status)
            if worker.adapters["cs408"].candidate_errors:
                raise AssertionError(worker.adapters["cs408"].candidate_errors)
            return candidates
        study_date = (
            fixture.study_date if subject == "english" else "2026-08-11"
        )
        return [
            candidate
            for candidate, _reason in worker.eligible_candidates(
                subject, study_date
            )
        ]

    def _publish_canonical_candidate(
        self,
        *,
        subject: str,
        config: dict,
        runtime_root: Path,
        candidate: core.Candidate,
        prepared_host,
    ) -> dict[str, object]:
        if subject == "math":
            return self._publish_math_candidate(
                config=config,
                runtime_root=runtime_root,
                candidate=candidate,
                prepared_host=prepared_host,
            )
        if subject == "cs408":
            return self._publish_cs408_candidate(
                config=config,
                runtime_root=runtime_root,
                candidate=candidate,
                prepared_host=prepared_host,
            )
        return self._publish_english_candidate(
            config=config,
            runtime_root=runtime_root,
            candidate=candidate,
            prepared_host=prepared_host,
        )

    def _publish_english_candidate(
        self,
        *,
        config: dict,
        runtime_root: Path,
        candidate: core.Candidate,
        prepared_host,
    ) -> dict[str, object]:
        fixture, _host, _host_config = prepared_host
        probe = core.Worker(config)
        context = probe.runner._background_context(candidate)
        host = probe.runner._processing_host
        self.assertIsInstance(host, ProcessingPluginHost)
        publication, receipts = self.support._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="english",
        )
        analysis_manifest = receipts["analysis"]["mcp_grounding_manifest"]
        critical_manifest = receipts["critical_review"][
            "mcp_grounding_manifest"
        ]

        def required_refs(manifest: dict) -> list[str]:
            artifact_ref = next(
                row["evidence_ref"]
                for row in manifest["items"]
                if row["collection"] == "task_artifact"
            )
            library_ref = next(
                row["evidence_ref"]
                for row in manifest["items"]
                if row["collection"]
                not in {"task_context", "task_artifact"}
            )
            return [artifact_ref, library_ref]

        seeded = english_support.FakeEnglishRunner(one_item=True).run(candidate)
        draft = copy.deepcopy(seeded.analysis)
        sealed = sealed_support.ThreeSubjectSealedChainReplayTests(
            methodName="runTest"
        )
        grounding = sealed._english_analysis_fixture(
            required_refs(analysis_manifest)
        )["items"][0]["grounding"]
        grounding["user_evidence_ref"] = draft["items"][0][
            "source_event_id"
        ]
        draft["items"][0]["grounding"] = grounding
        draft["candidate_id"] = core.english_candidate_content_id(
            str(candidate.input_binding["candidate_document_id"]), draft
        )
        core.validate_english_candidate_answer_safety(draft)
        core.validate_english_mcp_grounding(
            draft["items"], grounding_manifest=analysis_manifest
        )
        semantic_draft = core.english_review_semantic_draft(draft)
        revised_items = copy.deepcopy(draft["items"])
        revised_items[0]["grounding"]["mcp_evidence_refs"] = required_refs(
            critical_manifest
        )
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "confirmed",
            "draft_analysis_sha256": core.sha256_value(semantic_draft),
            "findings": [],
            "correction_resolutions": [],
            "revised_items": revised_items,
        }
        core.validate_english_candidate_answer_safety(review)
        core.validate_english_critical_review(review, semantic_draft)
        core.validate_english_applied_corrections(review)
        core.validate_english_mcp_grounding(
            review["revised_items"], grounding_manifest=critical_manifest
        )
        final = copy.deepcopy(draft)
        final["items"] = copy.deepcopy(revised_items)
        final["producer"]["prompt_version"] = config[
            "english_two_pass_v1"
        ]["critical_review_prompt_version"]
        final["candidate_id"] = core.english_candidate_content_id(
            str(candidate.input_binding["candidate_document_id"]), final
        )
        profile = config["english_two_pass_v1"]
        self.support._stamp_stage_receipts(
            receipts,
            schemas={
                "analysis": core._schema_sha256(
                    Path(profile["analysis_output_schema"])
                ),
                "critical_review": core._schema_sha256(
                    Path(profile["critical_review_output_schema"])
                ),
            },
            payloads={"analysis": draft, "critical_review": review},
        )
        checkpoint = core.CodexRunner(
            probe.model_config, runtime_root
        )._write_analysis_checkpoint(
            candidate,
            draft=draft,
            analysis_receipt=receipts["analysis"],
        )
        result = core.ModelResult(
            analysis=final,
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
            analysis_checkpoint_sha256=checkpoint["checkpoint_sha256"],
            analysis_checkpoint_ref=checkpoint["checkpoint_ref"],
            analysis_checkpoint_binding_key=checkpoint["binding_key"],
            analysis_checkpoint_binding_sha256=checkpoint[
                "binding_sha256"
            ],
        )

        class Runner:
            def run(self, _candidate):
                return result

        job = core.Worker(config, model_runner=Runner()).process_claimed_candidate(
            candidate,
            "production_canary_pending_queue",
            write_dashboard=False,
        )
        self.assertEqual(job["status"], "ready", job)
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="english",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "accepted")
        return {
            "analysis": draft,
            "critical_review": review,
            "stage_runtime": self._stage_runtime(
                publication, receipts, subject="english"
            ),
        }

    @staticmethod
    def _write_math_captures(
        fixture: math_support.MathV2CoreTests,
        *,
        count: int,
        start: int,
    ) -> list[dict]:
        captures: list[dict] = []
        for offset in range(count):
            index = start + offset
            capture = fixture._capture(index)
            capture.update(
                {
                    "event_id": f"MFI-CAP-{index:024d}",
                    "study_date": "2026-08-11",
                    "recorded_at": (
                        f"2026-08-11T01:{offset // 60:02d}:"
                        f"{offset % 60:02d}Z"
                    ),
                    "original_content_hash": hashlib.sha256(
                        f"original:{index}".encode()
                    ).hexdigest(),
                    "effective_evidence_hash": hashlib.sha256(
                        f"evidence:{index}".encode()
                    ).hexdigest(),
                    "effective_target_hash": hashlib.sha256(
                        f"target:{index}".encode()
                    ).hexdigest(),
                }
            )
            captures.append(capture)
        core.atomic_write_json(
            fixture.status_path,
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-11",
                "pending_count": len(captures),
                "closed_count": 0,
                "pending": captures,
                "closed": [],
                "active_freezes": [],
                "ledger_hash": hashlib.sha256(
                    json.dumps(
                        captures,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            },
        )
        return captures

    def _publish_math_candidate(
        self,
        *,
        config: dict,
        runtime_root: Path,
        candidate: core.Candidate,
        prepared_host,
    ) -> dict[str, object]:
        fixture, _host, _host_config = prepared_host
        probe = core.Worker(config)
        context = probe.runner._background_context(candidate)
        host = probe.runner._processing_host
        self.assertIsInstance(host, ProcessingPluginHost)
        publication, receipts = self.support._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="math",
        )
        evidence_ref = publication["mcp_stage_transcripts"]["analysis"][
            "grounding_refs"
        ][0]
        sealed = math_formalization_support.MathFormalizationContractTests(
            methodName="runTest"
        )
        sealed.setUpClass()
        draft = sealed.corrected_capture_overlay()
        self.support._remap_evidence_refs(draft, evidence_ref)
        draft["evidence_assessment"]["completeness"] = "complete"
        draft["evidence_assessment"]["gaps"] = []
        draft["unresolved"] = []
        draft["sol_verification_plan"][
            "recommended_disposition"
        ] = "new_candidate"
        draft["correct_reasoning_reconstruction"] = [
            copy.deepcopy(draft["question_structure"]["objects"][0])
        ]
        image_refs = list(
            candidate.input_binding.get("image_evidence_refs") or []
        )
        if image_refs:
            image_claim = draft["question_structure"]["objects"][0]
            image_claim["provenance"] = "mixed"
            image_claim["evidence_refs"] = [evidence_ref, *image_refs]
        runtime_candidate = core.candidate_with_grounding_refs(
            candidate,
            core.processing_grounding_refs(receipts, subject="math"),
        )
        draft = core.validate_math_semantic_gates(draft, runtime_candidate)
        review = {
            "schema_version": "study-intake-luna-math-critical-review-v2",
            "verdict": "pass",
            "summary": "zero-model canonical production scanner fixture",
            "revised_analysis": copy.deepcopy(draft),
            "relationship_decisions": [],
            "unsupported_claims": [],
            "evidence_misreads": [],
            "mathematical_errors": [],
            "visual_findings": [],
            "provenance_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        relationship_context = core.build_math_mcp_relationship_context(
            (), draft_analysis=draft
        )
        schemas = core._expected_math_dynamic_schema_sha256s(
            config["math_deep_v2"],
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            image_evidence_refs=image_refs,
            draft_analysis=draft,
            relationship_context=relationship_context,
            allow_empty_predeclared_evidence_refs=(
                core._math_direct_mcp_schema_mode(candidate.input_binding)
            ),
            math_source_bundle_formalization_mode=(
                core._math_verified_source_bundle_formalization_mode(
                    candidate.input_binding,
                    candidate.model_input,
                )
            ),
        )
        self.support._stamp_stage_receipts(
            receipts,
            schemas=schemas,
            payloads={"analysis": draft, "critical_review": review},
            ordered_image_sha256s=list(
                candidate.input_binding.get("ordered_image_sha256s") or []
            ),
        )
        result = core.ModelResult(
            analysis=copy.deepcopy(draft),
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
            relationship_context=relationship_context,
        )

        class Runner:
            def run(self, _candidate):
                return result

        job = core.Worker(config, model_runner=Runner()).process_claimed_candidate(
            candidate,
            "production_canary_pending_queue",
            write_dashboard=False,
        )
        self.assertEqual(job["status"], "two_pass_ready", job)
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="math",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "accepted")
        return {
            "analysis": draft,
            "critical_review": review,
            "stage_runtime": self._stage_runtime(
                publication, receipts, subject="math"
            ),
        }

    @staticmethod
    def _write_cs408_captures(
        fixture: cs408_support.PreprocessorTests,
        config: dict,
        *,
        count: int,
        start: int,
    ) -> list[dict]:
        evidence_root = Path(
            config["private_evidence"]["current_question_root"]
        )
        template = copy.deepcopy(
            fixture.cs_status["captures"][fixture.cs_capture]
        )
        rows: list[dict] = []
        for offset in range(count):
            index = start + offset
            capture_id = f"CAP-408-CANON-{index:04d}"
            context_id = f"DETAIL-408-CANON-{index:04d}"
            request_id = f"REQ-408-CANON-{index:04d}"
            session_id = f"SESSION-408-CANON-{index:04d}"
            item_id = f"ITEM-408-CANON-{index:04d}"
            source_id = f"SRC-408-CANON-{index:04d}"
            grader_id = f"GRADER-408-CANON-{index:04d}"
            source_binding_sha256 = hashlib.sha256(
                f"source-binding:{index}".encode()
            ).hexdigest()
            event_time = (
                f"2026-08-11T02:{offset // 60:02d}:"
                f"{offset % 60:02d}+00:00"
            )
            feedback = f"当前题快速反馈已经冻结 {index}。"
            bundle = {
                "schema_version": "current-question-evidence-bundle-v1",
                "context_id": context_id,
                "request_id": request_id,
                "session_id": session_id,
                "item_id": item_id,
                "source_id": source_id,
                "source_kind": "morning_review",
                "study_date": "2026-08-11",
                "event_time": event_time,
                "timezone": "Asia/Shanghai",
                "source_binding_sha256": source_binding_sha256,
                "current_question": {
                    "public_text": (
                        f"当前题 {index} 只讨论一个状态转换机制。"
                    ),
                    "options": [],
                    "response_instruction": "说明第一步判断。",
                    "public_surface_sha256": hashlib.sha256(
                        f"public:{index}".encode()
                    ).hexdigest(),
                    "attachment_sha256s": [],
                },
                "learner_evidence": {
                    "answer_text": "C",
                    "choice": "C",
                    "confidence": "low",
                    "first_action": "先判断状态",
                    "reasoning": "推理在边界条件处中断。",
                    "prompt_level": "L0",
                    "observed_at": event_time,
                },
                "evaluation_evidence": {
                    "grader_capsule_id": grader_id,
                    "correct_answer": "私有答案证据",
                    "correct_option": "C",
                    "standard_explanation": "私有详细解析",
                    "grader_result": "fragile_correct",
                    "grader_basis": "当前题绑定评分",
                    "provided_by": "current_question_grader",
                    "missing_fields": [],
                },
                "assessment": {
                    "first_result": "fragile_correct",
                    "choice_result": "correct",
                    "reasoning_result": "break",
                    "confidence": "low",
                    "prompt_level": "L0",
                    "first_break": "边界条件识别中断",
                    "first_break_provenance": "observed",
                    "first_action": "先判断状态",
                    "first_action_provenance": "observed",
                },
                "frozen_assistant_feedback": {
                    "text": feedback,
                    "sha256": hashlib.sha256(feedback.encode()).hexdigest(),
                },
                "provenance": {
                    "source_locator": f"current-question-capsule:{index}",
                    "source_sha256": hashlib.sha256(
                        f"source:{index}".encode()
                    ).hexdigest(),
                    "context_sha256": hashlib.sha256(
                        f"context:{index}".encode()
                    ).hexdigest(),
                    "attachment_provenance": [],
                    "created_at": event_time,
                    "missing_items": [],
                },
                "missing_fields": [],
                "attachment_objects": [],
                "formal_write_count": 0,
            }
            bundle_bytes = cs408_support.json_file_bytes(bundle)
            bundle_sha = cs408_support.sha(bundle_bytes)
            core.atomic_write_json(
                evidence_root / "objects" / f"{bundle_sha}.json", bundle
            )
            manifest = {
                "schema_version": "current-question-evidence-manifest-v1",
                "bundle_schema_version": (
                    "current-question-evidence-bundle-v1"
                ),
                "context_id": context_id,
                "request_id": request_id,
                "session_id": session_id,
                "item_id": item_id,
                "source_id": source_id,
                "source_kind": "morning_review",
                "study_date": "2026-08-11",
                "source_binding_sha256": source_binding_sha256,
                "object_sha256": bundle_sha,
                "object_size_bytes": len(bundle_bytes),
                "attachment_objects": [],
                "created_at": event_time,
                "answer_safe_manifest": True,
                "formal_write_count": 0,
            }
            manifest_sha = cs408_support.sha(
                cs408_support.json_file_bytes(manifest)
            )
            core.atomic_write_json(
                evidence_root / "manifests" / f"{manifest_sha}.json",
                manifest,
            )
            attestation_sha = hashlib.sha256(
                f"resolution-attestation:{index}".encode()
            ).hexdigest()
            trace = cs408_support.publish_trace_supplement(
                private_root=evidence_root,
                capture_id=capture_id,
                context_id=context_id,
                item_id=item_id,
                evidence_manifest_sha256=manifest_sha,
                created_at=event_time,
                supplement_kind="resolved_trace",
                resolution_receipt_sha256=attestation_sha,
                events=[
                    {
                        "role": "learner",
                        "kind": "answer",
                        "text": "C",
                        "observed_at": None,
                    }
                ],
            )
            resolution_core = {
                "schema": "current-question-turn-receipt-v1",
                "status": "teaching_resolved",
                "context_id": context_id,
                "session_id": session_id,
                "item_id": item_id,
                "capture_id": capture_id,
                "capture_receipt_sha256": hashlib.sha256(
                    f"capture-receipt:{index}".encode()
                ).hexdigest(),
                "evidence_manifest_sha256": manifest_sha,
                "resolution_attestation_locator": (
                    "current-question-turn://sha256/" + attestation_sha
                ),
                "resolution_attestation_sha256": attestation_sha,
                "trace_supplement": {
                    "schema_version": (
                        "current-question-trace-supplement-binding-v1"
                    ),
                    "capture_id": capture_id,
                    "locator": trace["locator"],
                    "object_sha": trace["object_sha"],
                    "binding_sha256": trace["binding_sha256"],
                    "interaction_trace_sha256": trace[
                        "interaction_trace_sha256"
                    ],
                    "formal_write_count": 0,
                },
                "session_resolution_receipt_sha256": hashlib.sha256(
                    f"session-resolution:{index}".encode()
                ).hexdigest(),
                "mastery_effect": "none",
                "retention_effect": "none",
                "independent_repair": False,
                "feedback_sha256": hashlib.sha256(
                    feedback.encode()
                ).hexdigest(),
                "advance_allowed": True,
                "formal_write_count": 0,
            }
            resolution_sha = cs408_support.sha(
                cs408_support.json_file_bytes(resolution_core)
            )
            core.atomic_write_json(
                evidence_root / "turns" / f"{resolution_sha}.json",
                resolution_core,
            )
            capture_receipt_sha = resolution_core[
                "capture_receipt_sha256"
            ]
            handoff = {
                "schema_version": "current-question-background-handoff-v1",
                "capture_id": capture_id,
                "context_id": context_id,
                "item_id": item_id,
                "evidence_manifest_sha256": manifest_sha,
                "capture_receipt_sha256": capture_receipt_sha,
                "status": "ready",
                "completion_kind": "teaching_resolved",
                "created_at": event_time,
                "updated_at": event_time,
                "trace_supplement_locator": trace["locator"],
                "trace_supplement_object_sha256": trace["object_sha"],
                "interaction_trace_sha256": trace[
                    "interaction_trace_sha256"
                ],
                "resolution_receipt_sha256": resolution_sha,
                "formal_write_count": 0,
            }
            handoff_sha = cs408_support.sha(
                cs408_support.json_file_bytes(handoff)
            )
            core.atomic_write_json(
                evidence_root
                / "background-handoffs/objects"
                / f"{handoff_sha}.json",
                handoff,
            )
            core.atomic_write_json(
                evidence_root
                / "background-handoffs/bindings"
                / f"{hashlib.sha256(capture_id.encode()).hexdigest()}.json",
                {
                    "schema_version": (
                        "current-question-background-handoff-binding-v1"
                    ),
                    "capture_id": capture_id,
                    "status": "ready",
                    "object_sha256": handoff_sha,
                    "locator": (
                        "current-question-background-handoff://sha256/"
                        f"{handoff_sha}"
                    ),
                    "updated_at": event_time,
                    "formal_write_count": 0,
                },
            )
            row = copy.deepcopy(template)
            row.update(
                {
                    "capture_id": capture_id,
                    "study_date": "2026-08-11",
                    "created_at": event_time,
                    "recorded_at": event_time,
                    "payload_sha256": hashlib.sha256(
                        f"payload:{index}".encode()
                    ).hexdigest(),
                }
            )
            row["capture"].update(
                {
                    "study_date": "2026-08-11",
                    "idempotency_key": f"canonical-cs408-{index}",
                }
            )
            row["capture"]["source_facts"].update(
                {
                    "source_id": source_id,
                    "details_id": context_id,
                    "locator": f"current-question-capsule:{index}",
                }
            )
            row["capture"]["stable_evidence_refs"] = [
                {
                    "kind": "current_question_evidence_bundle_v1",
                    "locator": (
                        "current-question-evidence://sha256/"
                        f"{manifest_sha}"
                    ),
                    "sha256": manifest_sha,
                }
            ]
            row["capture"]["identity_hint"][
                "idempotency_key"
            ] = f"canonical-cs408-identity-{index}"
            rows.append(row)
        for private_dir in (
            evidence_root,
            evidence_root / "manifests",
            evidence_root / "objects",
            evidence_root / "attachments",
            evidence_root / "trace-supplements",
            evidence_root / "trace-supplements/objects",
            evidence_root / "trace-supplements/bindings",
            evidence_root / "background-handoffs",
            evidence_root / "background-handoffs/objects",
            evidence_root / "background-handoffs/bindings",
            evidence_root / "turns",
        ):
            private_dir.mkdir(parents=True, exist_ok=True)
            private_dir.chmod(0o700)
        status = {
            "schema": "intake_fact_capture_state_v2",
            "study_date": "2026-08-11",
            "capture_count": len(rows),
            "pending_capture_ids": [row["capture_id"] for row in rows],
            "captures": {row["capture_id"]: row for row in rows},
            "neutral_saves": {},
            "active_batch": None,
            "latest_date_batch": None,
        }
        core.atomic_write_json(fixture.cs_status_path, status)
        return rows

    def _publish_cs408_candidate(
        self,
        *,
        config: dict,
        runtime_root: Path,
        candidate: core.Candidate,
        prepared_host,
    ) -> dict[str, object]:
        fixture, _host, _host_config = prepared_host
        probe = core.Worker(config)
        context = probe.runner._background_context(candidate)
        host = probe.runner._processing_host
        self.assertIsInstance(host, ProcessingPluginHost)
        publication, receipts = self.support._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="cs408",
        )
        evidence_ref = publication["mcp_stage_transcripts"]["analysis"][
            "grounding_refs"
        ][0]
        draft = sealed_support._payload(
            sealed_support.SEALED["cs408"]["analysis_output"]
        )
        self.support._remap_evidence_refs(draft, evidence_ref)
        draft["evidence_assessment"]["completeness"] = "complete"
        draft["evidence_assessment"]["gaps"] = []
        draft["unresolved"] = []
        draft["sol_verification_plan"][
            "recommended_disposition"
        ] = "new_candidate"
        runtime_candidate = core.candidate_with_grounding_refs(
            candidate,
            core.processing_grounding_refs(receipts, subject="cs408"),
        )
        draft = core.validate_analysis_v2(
            draft,
            runtime_candidate.allowed_evidence_refs,
            require_network_context=True,
        )
        review = sealed_support._payload(
            sealed_support.SEALED["cs408"]["critical_output"]
        )
        review = (
            sealed_support.ThreeSubjectSealedChainReplayTests._fix_cs408_review(
                review
            )
        )
        self.support._remap_evidence_refs(review, evidence_ref)
        revised = review["revised_analysis"]
        revised["evidence_assessment"]["completeness"] = "complete"
        revised["evidence_assessment"]["gaps"] = []
        revised["unresolved"] = []
        revised["sol_verification_plan"][
            "recommended_disposition"
        ] = "new_candidate"
        for resolution in review["correction_resolutions"]:
            if resolution.get("resolution") != "applied":
                continue
            material_paths = [
                path
                for path in resolution["affected_json_paths"]
                if core._resolve_correction_json_path(draft, path)
                != core._resolve_correction_json_path(revised, path)
            ]
            self.assertTrue(material_paths)
            resolution["affected_json_paths"] = material_paths
            for section in (
                "unsupported_claims",
                "evidence_misreads",
                "answer_safety_findings",
                "missing_analysis",
                "required_corrections",
                "sol_priority_checks",
            ):
                for finding in review[section]:
                    if finding.get("finding_id") == resolution.get(
                        "finding_id"
                    ):
                        finding["affected_json_paths"] = copy.deepcopy(
                            material_paths
                        )
        review = core.validate_critical_review_v2(
            review,
            allowed_evidence_refs=runtime_candidate.allowed_evidence_refs,
            draft_analysis=copy.deepcopy(draft),
            require_network_context=True,
        )
        profile = config["cs408_deep_v2"]
        _, analysis_schema_sha256 = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["analysis_output_schema"]),
            stage_name="cs408_analysis",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
        )
        _, review_schema_sha256 = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["critical_review_output_schema"]),
            stage_name="cs408_critical_review",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            allowed_analysis_refs=core._analysis_review_refs(draft),
            allowed_correction_paths=core._cs408_writable_correction_paths(
                draft
            ),
        )
        self.support._stamp_stage_receipts(
            receipts,
            schemas={
                "analysis": analysis_schema_sha256,
                "critical_review": review_schema_sha256,
            },
            payloads={"analysis": draft, "critical_review": review},
            ordered_image_sha256s=list(
                candidate.input_binding.get("ordered_image_sha256s") or []
            ),
        )
        result = core.ModelResult(
            analysis=copy.deepcopy(review["revised_analysis"]),
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
        )

        class Runner:
            def run(self, _candidate):
                return result

        job = core.Worker(config, model_runner=Runner()).process_claimed_candidate(
            candidate,
            "production_canary_pending_queue",
            write_dashboard=False,
        )
        self.assertEqual(job["status"], "two_pass_ready", job)
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="cs408",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "corrected")
        return {
            "analysis": draft,
            "critical_review": review,
            "stage_runtime": self._stage_runtime(
                publication, receipts, subject="cs408"
            ),
        }

    def _prepare_shared_canonical_runtimes(self) -> dict[str, object]:
        english = english_support.EnglishAdapterTests(methodName="runTest")
        english.setUp()
        self.addCleanup(english.tearDown)
        math = math_support.MathV2CoreTests(methodName="runTest")
        math.setUp()
        self.addCleanup(math.tearDown)
        cs408 = cs408_support.PreprocessorTests(methodName="runTest")
        cs408.setUp()
        self.addCleanup(cs408.tearDown)

        core.atomic_write_json(
            math.status_path,
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-11",
                "pending_count": 0,
                "closed_count": 0,
                "pending": [],
                "closed": [],
                "active_freezes": [],
                "ledger_hash": hashlib.sha256(b"empty-math").hexdigest(),
            },
        )
        core.atomic_write_json(
            cs408.cs_status_path,
            {
                "schema": "intake_fact_capture_state_v2",
                "study_date": "2026-08-11",
                "capture_count": 0,
                "pending_capture_ids": [],
                "captures": {},
                "neutral_saves": {},
                "active_batch": None,
                "latest_date_batch": None,
            },
        )

        shared_runtime = english.base / "shared-three-subject-runtime"
        shared_runtime.mkdir(parents=True)
        fixture, _host, host_config = self.support._new_processing_host(
            shared_runtime
        )
        # This integration fixture launches three real local MCP clients while
        # sixty task processes are active.  Five seconds is a startup race on a
        # loaded host, not an execution failure; production limits are not
        # changed by this test-only allowance.
        host_config["timeout_seconds"] = 60
        host, host_config = self._install_authority_stub(
            runtime_root=shared_runtime,
            fixture=fixture,
            host_config=host_config,
        )
        configs = {
            "math": self._enable_canary(math.config, host_config),
            "cs408": self._enable_canary(cs408.make_v2_config(), host_config),
            "english": self._enable_canary(english.config, host_config),
        }
        configs["math"]["math_deep_v2"]["mode"] = "production"
        fixtures = {"math": math, "cs408": cs408, "english": english}
        runtimes: dict[str, ProductionDispatchRuntime] = {}
        config_paths: dict[str, Path] = {}
        for subject in ("math", "cs408", "english"):
            config = configs[subject]
            config["runtime_root"] = str(shared_runtime)
            config_path = fixtures[subject].base / (
                f"canonical-shared-{subject}-config.json"
            )
            core.atomic_write_json(config_path, config)
            config_paths[subject] = config_path
            runtime = ProductionDispatchRuntime(config, subject, config_path)
            runtime.dispatcher.lease_store.begin_subject_drain(subject)
            runtime.activate_production_canary(
                activated_at="2026-08-10T00:00:00Z"
            )
            runtimes[subject] = runtime

        marker_root = shared_runtime / "markers"
        payload_root = shared_runtime / "stage-payloads"
        payload_root.mkdir(parents=True)
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        os.environ["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(payload_root)
        for subject, runtime in runtimes.items():
            self._configure_zero_model_runner(
                runtime, config_paths[subject]
            )
        return {
            "shared_runtime": shared_runtime,
            "marker_root": marker_root,
            "payload_root": payload_root,
            "fixture": fixture,
            "host": host,
            "host_config": host_config,
            "configs": configs,
            "fixtures": fixtures,
            "runtimes": runtimes,
        }

    def _write_shared_subject_captures(
        self,
        scenario: dict[str, object],
        *,
        subject: str,
        count: int,
        start: int,
    ) -> list[core.Candidate]:
        fixtures = scenario["fixtures"]
        configs = scenario["configs"]
        fixture = fixtures[subject]
        config = configs[subject]
        if subject == "math":
            self._write_math_captures(fixture, count=count, start=start)
        elif subject == "cs408":
            self._write_cs408_captures(
                fixture, config, count=count, start=start
            )
        else:
            for offset in range(count):
                index = start + offset
                sentence = fixture.write_sentence(
                    index,
                    article_id=f"CANONICAL-SHARED-EN-{index}",
                    evidence_origin="live_user",
                )
                fixture.write_completion([sentence], index=100000 + index)
        candidates = self._canonical_candidates(
            subject=subject,
            config=config,
            fixture=fixture,
        )
        self.assertEqual(len(candidates), count)
        return candidates

    def _run_shared_canonical_phase(
        self,
        scenario: dict[str, object],
        *,
        count_by_subject: dict[str, int],
        start_by_subject: dict[str, int],
    ) -> dict[str, list]:
        subjects = tuple(count_by_subject)
        candidates_by_subject: dict[str, dict[str, core.Candidate]] = {}
        for subject in subjects:
            candidates = self._write_shared_subject_captures(
                scenario,
                subject=subject,
                count=count_by_subject[subject],
                start=start_by_subject[subject],
            )
            candidates_by_subject[subject] = {
                candidate.capture_id: candidate for candidate in candidates
            }

        runtimes = scenario["runtimes"]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(subjects)
        ) as executor:
            futures = {
                subject: executor.submit(runtimes[subject].scan_and_submit)
                for subject in subjects
            }
            scan_results = {
                subject: future.result(timeout=30)
                for subject, future in futures.items()
            }
        handles_by_subject: dict[str, list] = {}
        all_handles = []
        for subject in subjects:
            handles, decisions = scan_results[subject]
            self.assertEqual(
                len(handles), count_by_subject[subject], decisions
            )
            handles_by_subject[subject] = handles
            all_handles.extend(handles)

        expected_units = {handle.unit_sha256 for handle in all_handles}
        markers = self._wait_for_unit_markers(
            scenario["marker_root"], expected_units, timeout_seconds=45
        )
        self.assertEqual(set(markers), expected_units)
        self.assertEqual(
            len({int(row["pid"]) for row in markers.values()}),
            len(all_handles),
        )
        self.assertTrue(
            all(int(row["pid"]) == int(row["pgid"]) for row in markers.values())
        )
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(
                len({str(row[field]) for row in markers.values()}),
                len(all_handles),
            )

        prepared_host = (
            scenario["fixture"],
            scenario["host"],
            scenario["host_config"],
        )
        for subject in subjects:
            for handle in handles_by_subject[subject]:
                capture_id = str(handle.task.frozen_payload["capture_id"])
                candidate = candidates_by_subject[subject][capture_id]
                stage_payload = self._publish_canonical_candidate(
                    subject=subject,
                    config=scenario["configs"][subject],
                    runtime_root=scenario["shared_runtime"],
                    candidate=candidate,
                    prepared_host=prepared_host,
                )
                core.atomic_write_json(
                    scenario["payload_root"] / f"{handle.unit_sha256}.json",
                    stage_payload,
                )
        release_path = scenario["marker_root"] / "release-all"
        release_path.write_text("release\n", encoding="utf-8")
        results_by_subject: dict[str, list] = {}
        for subject in subjects:
            results = [
                handle.wait(45) for handle in handles_by_subject[subject]
            ]
            self.assertEqual(
                [result.outcome for result in results],
                ["succeeded"] * count_by_subject[subject],
            )
            results_by_subject[subject] = results
        for subject in subjects:
            projected = runtimes[subject].persist_finished_luna(
                handles_by_subject[subject]
            )
            self.assertEqual(len(projected), count_by_subject[subject])
            writer = runtimes[subject].subject_sol.read_subject(subject)[
                "writer_state"
            ]
            self.assertEqual(writer["handoff_status"], "awaiting_luna")
            self.assertIsNone(writer["batch_id"])
        release_path.unlink()
        return handles_by_subject

    def _assert_canonical_task_closures(
        self,
        scenario: dict[str, object],
        handles_by_subject: dict[str, list],
    ) -> None:
        runtime_root = Path(scenario["shared_runtime"])
        marker_root = Path(scenario["marker_root"])
        all_handles = [
            handle
            for handles in handles_by_subject.values()
            for handle in handles
        ]
        expected_units = {handle.unit_sha256 for handle in all_handles}
        markers = self._wait_for_unit_markers(
            marker_root, expected_units, timeout_seconds=5
        )
        self.assertEqual(set(markers), expected_units)
        owner_fence_units: set[tuple[str, int, str]] = set()
        owner_ids_by_subject: dict[str, set[str]] = {
            subject: set() for subject in handles_by_subject
        }
        lease_file_shas: set[str] = set()
        terminal_receipt_shas: set[str] = set()
        supervisor_pids: set[int] = set()
        supervisor_pgids: set[int] = set()
        process_identity_shas: set[str] = set()
        process_exit_shas: set[str] = set()
        context_roots: set[str] = set()
        mcp_session_roots: set[str] = set()
        report_roots: set[str] = set()
        read_sessions: set[str] = set()
        package_shas: set[str] = set()
        report_json_shas: set[str] = set()
        report_markdown_shas: set[str] = set()

        for subject, handles in handles_by_subject.items():
            store = scenario["runtimes"][subject].dispatcher.lease_store
            state = store.production_canary_status_read_only(subject)
            self.assertIsNotNone(state)
            terminal_index_path = Path(str(state["terminal_index_path"]))
            terminal_index_bytes = terminal_index_path.read_bytes()
            self.assertEqual(
                hashlib.sha256(terminal_index_bytes).hexdigest(),
                state["terminal_index_sha256"],
            )
            terminal_index = core.load_json(terminal_index_path)
            store._verify_seal(
                terminal_index,
                purpose="dispatch-production-canary-terminal-index",
            )
            self.assertEqual(terminal_index["subject"], subject)
            units = terminal_index["units"]
            for handle in handles:
                task = handle.task
                self.assertIsNotNone(task)
                unit = handle.unit_sha256
                lease_path = store._lease_path(unit)
                lease_bytes = lease_path.read_bytes()
                lease_value = json.loads(lease_bytes.decode("utf-8"))
                lease_file_shas.add(hashlib.sha256(lease_bytes).hexdigest())
                self.assertEqual(lease_value["status"], "completed")
                self.assertEqual(lease_value["unit_sha256"], unit)
                self.assertEqual(lease_value["subject"], subject)
                owner_id = str(lease_value["owner_id"])
                owner_ids_by_subject[subject].add(owner_id)
                fence = int(lease_value["fence"])
                owner_fence_units.add((owner_id, fence, unit))
                lease = Lease(unit, owner_id, fence)
                closure = store._task_process_closure_locked(task, lease)
                for path_key, sha_key, purpose in (
                    (
                        "supervisor_process_identity_path",
                        "supervisor_process_identity_sha256",
                        "dispatch-task-process-identity",
                    ),
                    (
                        "supervisor_process_exit_path",
                        "supervisor_process_exit_sha256",
                        "dispatch-task-process-exit",
                    ),
                ):
                    artifact_path = Path(str(closure[path_key]))
                    artifact_bytes = artifact_path.read_bytes()
                    self.assertEqual(
                        hashlib.sha256(artifact_bytes).hexdigest(),
                        closure[sha_key],
                    )
                    artifact = json.loads(artifact_bytes.decode("utf-8"))
                    store._verify_seal(artifact, purpose=purpose)
                marker = markers[unit]
                self.assertEqual(
                    closure["supervisor_pid"], int(marker["pid"])
                )
                self.assertEqual(
                    closure["supervisor_pgid"], int(marker["pgid"])
                )
                self.assertEqual(closure["supervisor_pid"], closure["supervisor_pgid"])
                self.assertTrue(closure["reaped"])
                self.assertTrue(closure["process_absent"])
                self.assertTrue(closure["pgid_absent"])
                self.assertEqual(closure["termination_reason"], "completed")
                self.assertEqual(closure["returncode"], 0)
                process_identity_shas.add(
                    str(closure["supervisor_process_identity_sha256"])
                )
                process_exit_shas.add(
                    str(closure["supervisor_process_exit_sha256"])
                )
                supervisor_pids.add(int(closure["supervisor_pid"]))
                supervisor_pgids.add(int(closure["supervisor_pgid"]))

                completion_path = store._completion_path(unit)
                completion = core.load_json(completion_path)
                store._verify_seal(completion, purpose="dispatch-completion")
                self.assertEqual(completion["outcome"], "succeeded")
                self.assertEqual(completion["unit_sha256"], unit)
                self.assertEqual(completion["subject"], subject)
                self.assertEqual(completion["lease_fence"], fence)
                process_execution = completion["process_execution"]
                self.assertFalse(process_execution["canonical_task_runner"])
                self.assertFalse(
                    process_execution["provider_process_closure_required"]
                )
                self.assertEqual(process_execution["provider_stages"], {})
                self.assertEqual(
                    process_execution["supervisor_process_identity_sha256"],
                    closure["supervisor_process_identity_sha256"],
                )
                self.assertEqual(
                    process_execution["supervisor_process_exit_sha256"],
                    closure["supervisor_process_exit_sha256"],
                )

                package_sha = str(completion["package_sha256"])
                package_path = Path(str(completion["package_path"]))
                package_bytes = package_path.read_bytes()
                self.assertEqual(
                    hashlib.sha256(package_bytes).hexdigest(), package_sha
                )
                package = json.loads(package_bytes.decode("utf-8"))
                self.assertEqual(package["unit_sha256"], unit)
                self.assertEqual(package["subject"], subject)
                self.assertEqual(
                    completion["package_ref"],
                    f"study-intake-dispatch-package://sha256/{package_sha}",
                )

                report_json_sha = str(completion["report_json_sha256"])
                report_json_path = (
                    runtime_root
                    / "dispatch/reports/json/sha256"
                    / report_json_sha[:2]
                    / f"{report_json_sha}.json"
                )
                report_json_bytes = report_json_path.read_bytes()
                self.assertEqual(
                    hashlib.sha256(report_json_bytes).hexdigest(),
                    report_json_sha,
                )
                report_json = json.loads(report_json_bytes.decode("utf-8"))
                self.assertEqual(report_json["unit_sha256"], unit)
                self.assertEqual(report_json["package_sha256"], package_sha)
                self.assertEqual(
                    completion["report_json_ref"],
                    f"study-intake-report://sha256/{report_json_sha}",
                )

                report_markdown_sha = str(
                    completion["report_markdown_sha256"]
                )
                report_markdown_path = (
                    runtime_root
                    / "dispatch/reports/markdown/sha256"
                    / report_markdown_sha[:2]
                    / f"{report_markdown_sha}.md"
                )
                report_markdown_bytes = report_markdown_path.read_bytes()
                self.assertEqual(
                    hashlib.sha256(report_markdown_bytes).hexdigest(),
                    report_markdown_sha,
                )
                self.assertTrue(report_markdown_bytes.strip())
                self.assertEqual(
                    completion["report_markdown_ref"],
                    "study-intake-report-markdown://sha256/"
                    + report_markdown_sha,
                )

                terminal_row = units[unit]
                self.assertEqual(terminal_row["outcome"], "succeeded")
                self.assertEqual(
                    terminal_row["report_reopen_status"],
                    "json_markdown_package_verified",
                )
                terminal_path = Path(
                    str(terminal_row["terminal_receipt_path"])
                )
                terminal_bytes = terminal_path.read_bytes()
                self.assertEqual(
                    hashlib.sha256(terminal_bytes).hexdigest(),
                    terminal_row["terminal_receipt_sha256"],
                )
                terminal_receipt_shas.add(
                    str(terminal_row["terminal_receipt_sha256"])
                )
                terminal = json.loads(terminal_bytes.decode("utf-8"))
                store._verify_seal(
                    terminal, purpose="dispatch-production-canary-terminal"
                )
                self.assertEqual(terminal["selected"]["unit_sha256"], unit)
                self.assertEqual(terminal["selected"]["lease_fence"], fence)
                self.assertEqual(
                    terminal["selected"]["lease_owner_id"], owner_id
                )
                self.assertEqual(
                    terminal["selected"]["context_root"],
                    marker["context_root"],
                )
                self.assertEqual(
                    terminal["selected"]["mcp_session_root"],
                    marker["mcp_session_root"],
                )
                self.assertEqual(
                    terminal["selected"]["report_root"],
                    marker["report_root"],
                )
                self.assertEqual(terminal["package_sha256"], package_sha)
                self.assertEqual(
                    terminal["report_json_sha256"], report_json_sha
                )
                self.assertEqual(
                    terminal["report_markdown_sha256"],
                    report_markdown_sha,
                )
                self.assertFalse(terminal["fast_mode_requested"])
                self.assertEqual(
                    terminal["fast_mode_effective"], "not_requested"
                )
                self.assertIn("requested_service_tier", terminal)
                self.assertIsNone(terminal["requested_service_tier"])
                self.assertGreater(terminal["observed_model_call_count"], 0)
                self.assertGreater(
                    terminal["observed_provider_request_count"], 0
                )
                self.assertGreater(
                    terminal["observed_mcp_tool_call_count"], 0
                )
                self.assertEqual(
                    terminal["counter_scope"],
                    "current_control_plane_operation",
                )
                self.assertEqual(terminal["model_call_count"], 0)
                self.assertEqual(terminal["provider_request_count"], 0)
                self.assertEqual(terminal["mcp_tool_call_count"], 0)
                self.assertEqual(terminal["formal_write_count"], 0)
                self.assertFalse(terminal["sol_enabled"])

                contract = task.frozen_payload["dispatch_contract"]
                self.assertIn("requested_service_tier", contract)
                self.assertIsNone(contract["requested_service_tier"])
                self.assertFalse(contract["fast_mode_requested"])
                self.assertEqual(
                    contract["fast_mode_effective"], "not_requested"
                )
                context_roots.add(str(marker["context_root"]))
                mcp_session_roots.add(str(marker["mcp_session_root"]))
                report_roots.add(str(marker["report_root"]))
                read_sessions.add(str(terminal["read_session_id"]))
                package_shas.add(package_sha)
                report_json_shas.add(report_json_sha)
                report_markdown_shas.add(report_markdown_sha)

        expected_count = len(all_handles)
        self.assertEqual(len(lease_file_shas), expected_count)
        self.assertEqual(len(terminal_receipt_shas), expected_count)
        self.assertTrue(
            all(len(owner_ids) == 1 for owner_ids in owner_ids_by_subject.values())
        )
        self.assertEqual(
            len(
                {
                    owner_id
                    for owner_ids in owner_ids_by_subject.values()
                    for owner_id in owner_ids
                }
            ),
            len(owner_ids_by_subject),
        )
        for values in (
            owner_fence_units,
            supervisor_pids,
            supervisor_pgids,
            process_identity_shas,
            process_exit_shas,
            context_roots,
            mcp_session_roots,
            report_roots,
            read_sessions,
            package_shas,
            report_json_shas,
            report_markdown_shas,
        ):
            self.assertEqual(len(values), expected_count)

    def test_canonical_three_subject_twenty_each_share_one_release_barrier(
        self,
    ) -> None:
        scenario = self._prepare_shared_canonical_runtimes()
        self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 1, "cs408": 1, "english": 1},
            start_by_subject={"math": 1000, "cs408": 1000, "english": 1},
        )
        handles = self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 20, "cs408": 20, "english": 20},
            start_by_subject={"math": 2000, "cs408": 2000, "english": 100},
        )
        self.assertEqual(sum(map(len, handles.values())), 60)
        self._assert_canonical_task_closures(scenario, handles)
        for subject in ("math", "cs408", "english"):
            self.assertEqual(
                scenario["configs"][subject]["model"]["codex_path"],
                "/usr/bin/false",
            )
        external_audit = {
            "schema_version": "canonical-zero-model-invocation-audit-v1",
            "fixture_scope": "temp_zero_model_only",
            "synthetic_stage_evidence_task_count": 60,
            "local_authority_fixture_subprocess_count": 0,
            "real_model_call_count": 0,
            "real_provider_request_count": 0,
            "real_mcp_request_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
        }
        core.atomic_write_json(
            scenario["shared_runtime"] / "canonical-external-audit.json",
            external_audit,
        )
        self.assertEqual(external_audit["real_model_call_count"], 0)
        self.assertEqual(external_audit["real_provider_request_count"], 0)
        self.assertEqual(external_audit["real_mcp_request_count"], 0)
        self.assertEqual(external_audit["sol_call_count"], 0)
        self.assertEqual(external_audit["formal_write_count"], 0)
        telemetry = scenario["runtimes"][
            "math"
        ].dispatcher.lease_store.production_canary_concurrency_telemetry(
            release_id=core.LOADED_CORE_SHA256
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 60, telemetry)
        for subject in ("math", "cs408", "english"):
            self.assertGreaterEqual(
                telemetry["subject_peak_active"][subject], 20, telemetry
            )
        self.assertEqual(telemetry["runner_interval_missing_count_global"], 0)

    def test_canonical_math_one_failure_keeps_nineteen_siblings_running(
        self,
    ) -> None:
        scenario = self._prepare_shared_canonical_runtimes()
        self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 1},
            start_by_subject={"math": 1000},
        )
        candidates = self._write_shared_subject_captures(
            scenario, subject="math", count=20, start=3000
        )
        candidates_by_capture = {
            candidate.capture_id: candidate for candidate in candidates
        }
        failed_capture_id = candidates[0].capture_id
        os.environ["BLOCKING_FAIL_CAPTURE_ID"] = failed_capture_id
        runtime = scenario["runtimes"]["math"]
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 20, decisions)
        expected_units = {handle.unit_sha256 for handle in handles}
        markers = self._wait_for_unit_markers(
            scenario["marker_root"], expected_units, timeout_seconds=30
        )
        self.assertEqual(set(markers), expected_units)
        failed_handle = next(
            handle
            for handle in handles
            if handle.task.frozen_payload["capture_id"] == failed_capture_id
        )
        self.assertEqual(
            markers[failed_handle.unit_sha256]["behavior"],
            "fail_after_release",
        )
        prepared_host = (
            scenario["fixture"],
            scenario["host"],
            scenario["host_config"],
        )
        for handle in handles:
            if handle is failed_handle:
                continue
            capture_id = str(handle.task.frozen_payload["capture_id"])
            stage_payload = self._publish_math_candidate(
                config=scenario["configs"]["math"],
                runtime_root=scenario["shared_runtime"],
                candidate=candidates_by_capture[capture_id],
                prepared_host=prepared_host,
            )
            core.atomic_write_json(
                scenario["payload_root"] / f"{handle.unit_sha256}.json",
                stage_payload,
            )
        release_path = scenario["marker_root"] / "release-all"
        release_path.write_text("release\n", encoding="utf-8")
        results = [handle.wait(45) for handle in handles]
        self.assertEqual(
            sum(result.outcome == "succeeded" for result in results), 19
        )
        self.assertEqual(
            sum(result.outcome == "failed" for result in results), 1
        )
        failed_result = failed_handle.wait(0)
        self.assertEqual(
            failed_result.error_code,
            "synthetic_zero_model_fixture_failure",
        )
        persistence = runtime.persist_finished_luna_with_status(handles)
        # The failed terminal is also projected into SubjectSol as one durable
        # failed task row; nineteen siblings independently close quality.
        self.assertEqual(len(persistence["projected"]), 20)
        self.assertEqual(persistence["failures"], [], persistence)
        batch = runtime.subject_sol.read_subject_batch("math")
        self.assertEqual(
            sum(
                row["status"] == "workflow_complete"
                for row in batch["tasks"]
            ),
            19,
        )
        self.assertEqual(
            sum(
                row["status"] == "execution_failed"
                for row in batch["tasks"]
            ),
            1,
        )
        succeeded_handles = [
            handle for handle in handles if handle is not failed_handle
        ]
        self._assert_canonical_task_closures(
            scenario, {"math": succeeded_handles}
        )
        store = runtime.dispatcher.lease_store
        state = store.production_canary_status_read_only("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["active_task_count"], 0)
        self.assertEqual(
            state["blocking_reason"],
            "synthetic_zero_model_fixture_failure",
        )
        self.assertEqual(
            state["next_action"], "explicit_subject_resume_required"
        )
        terminal_index = core.load_json(Path(state["terminal_index_path"]))
        failed_terminal = terminal_index["units"][failed_handle.unit_sha256]
        self.assertEqual(failed_terminal["outcome"], "failed")
        self.assertEqual(
            failed_terminal["error_code"],
            "synthetic_zero_model_fixture_failure",
        )
        self.assertTrue(
            core.load_json(
                Path(failed_terminal["terminal_receipt_path"])
            )["late_result_fenced"]
        )
        failed_completion = core.load_json(
            store._completion_path(failed_handle.unit_sha256)
        )
        store._verify_seal(
            failed_completion, purpose="dispatch-completion"
        )
        self.assertEqual(failed_completion["outcome"], "failed")
        self.assertEqual(
            failed_completion["unit_sha256"], failed_handle.unit_sha256
        )
        self.assertEqual(failed_completion["subject"], "math")
        self.assertEqual(
            failed_completion["lease_fence"],
            core.load_json(store._lease_path(failed_handle.unit_sha256))[
                "fence"
            ],
        )
        self.assertIsNone(failed_completion["package_sha256"])
        self.assertIsNone(failed_completion["package_ref"])
        self.assertIsNone(failed_completion["report_json_ref"])
        self.assertIsNone(failed_completion["report_markdown_ref"])
        failed_processing_receipt = core.load_json(
            Path(failed_completion["receipt_path"])
        )
        store._verify_seal(
            failed_processing_receipt, purpose="dispatch-receipt"
        )
        self.assertEqual(failed_processing_receipt["outcome"], "failed")
        self.assertEqual(
            failed_processing_receipt["error_code"],
            "synthetic_zero_model_fixture_failure",
        )

        os.environ.pop("BLOCKING_FAIL_CAPTURE_ID", None)
        release_path.unlink()
        self._write_math_captures(
            scenario["fixtures"]["math"], count=2, start=4000
        )
        queued_handles, queued_decisions = runtime.scan_and_submit()
        self.assertEqual(queued_handles, [])
        self.assertEqual(
            sum(
                decision.get("phase") == "producer_queue_pending"
                for decision in queued_decisions
            ),
            2,
        )
        queued_state = store.production_canary_status_read_only("math")
        self.assertEqual(queued_state["state"], "failed_drained")
        self.assertFalse(queued_state["luna_consumer_enabled"])
        self.assertGreaterEqual(queued_state["queue_depth"], 2)
        self.assertEqual(runtime.dispatcher.active_count, 0)

    def test_canonical_math_continues_two_full_twenty_task_batches(
        self,
    ) -> None:
        scenario = self._prepare_shared_canonical_runtimes()
        self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 1},
            start_by_subject={"math": 1000},
        )
        first = self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 20},
            start_by_subject={"math": 5000},
        )
        second = self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 20},
            start_by_subject={"math": 6000},
        )
        self.assertEqual(len(first["math"]), 20)
        self.assertEqual(len(second["math"]), 20)
        self.assertTrue(
            {handle.unit_sha256 for handle in first["math"]}.isdisjoint(
                {handle.unit_sha256 for handle in second["math"]}
            )
        )
        self._assert_canonical_task_closures(scenario, second)
        runtime = scenario["runtimes"]["math"]
        state = runtime.dispatcher.lease_store.production_canary_status_read_only(
            "math"
        )
        self.assertEqual(state["state"], "continuous_concurrent_unlocked")
        terminal_index = core.load_json(Path(state["terminal_index_path"]))
        self.assertEqual(terminal_index["terminal_task_count"], 41)
        self.assertEqual(terminal_index["terminal_by_outcome"]["succeeded"], 41)
        archives = sorted(
            runtime.subject_sol.subject_batch_archive_root.glob(
                "background/sha256/*/*.json"
            )
        )
        self.assertEqual(len(archives), 3)
        for path in archives:
            archive = core.load_json(path)
            runtime.subject_sol._verify_seal(
                archive, purpose="subject-background-luna-batch-archive"
            )
            self.assertFalse(archive["sol_called"])
            self.assertFalse(archive["sol_enabled"])
            self.assertEqual(archive["formal_write_count"], 0)
        writer = runtime.subject_sol.read_subject("math")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])

    def test_canonical_math_failure_does_not_stop_cs408_or_english(
        self,
    ) -> None:
        scenario = self._prepare_shared_canonical_runtimes()
        self._run_shared_canonical_phase(
            scenario,
            count_by_subject={"math": 1, "cs408": 1, "english": 1},
            start_by_subject={"math": 1000, "cs408": 1000, "english": 1},
        )
        candidates_by_subject: dict[str, dict[str, core.Candidate]] = {}
        starts = {"math": 7000, "cs408": 7000, "english": 700}
        for subject in ("math", "cs408", "english"):
            candidates = self._write_shared_subject_captures(
                scenario,
                subject=subject,
                count=20,
                start=starts[subject],
            )
            candidates_by_subject[subject] = {
                candidate.capture_id: candidate for candidate in candidates
            }
        failed_capture_id = next(iter(candidates_by_subject["math"]))
        os.environ["BLOCKING_FAIL_CAPTURE_ID"] = failed_capture_id
        runtimes = scenario["runtimes"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                subject: executor.submit(runtimes[subject].scan_and_submit)
                for subject in ("math", "cs408", "english")
            }
            scanned = {
                subject: future.result(timeout=30)
                for subject, future in futures.items()
            }
        handles_by_subject = {
            subject: list(scanned[subject][0])
            for subject in ("math", "cs408", "english")
        }
        for subject, handles in handles_by_subject.items():
            self.assertEqual(len(handles), 20, scanned[subject][1])
        all_handles = [
            handle
            for handles in handles_by_subject.values()
            for handle in handles
        ]
        expected_units = {handle.unit_sha256 for handle in all_handles}
        markers = self._wait_for_unit_markers(
            scenario["marker_root"], expected_units, timeout_seconds=45
        )
        self.assertEqual(set(markers), expected_units)
        prepared_host = (
            scenario["fixture"],
            scenario["host"],
            scenario["host_config"],
        )
        failed_handle = next(
            handle
            for handle in handles_by_subject["math"]
            if handle.task.frozen_payload["capture_id"] == failed_capture_id
        )
        for subject, handles in handles_by_subject.items():
            for handle in handles:
                if handle is failed_handle:
                    continue
                capture_id = str(handle.task.frozen_payload["capture_id"])
                stage_payload = self._publish_canonical_candidate(
                    subject=subject,
                    config=scenario["configs"][subject],
                    runtime_root=scenario["shared_runtime"],
                    candidate=candidates_by_subject[subject][capture_id],
                    prepared_host=prepared_host,
                )
                core.atomic_write_json(
                    scenario["payload_root"] / f"{handle.unit_sha256}.json",
                    stage_payload,
                )
        release_path = scenario["marker_root"] / "release-all"
        release_path.write_text("release\n", encoding="utf-8")
        results_by_subject = {
            subject: [handle.wait(45) for handle in handles]
            for subject, handles in handles_by_subject.items()
        }
        self.assertEqual(
            sum(row.outcome == "failed" for row in results_by_subject["math"]),
            1,
        )
        self.assertEqual(
            sum(
                row.outcome == "succeeded"
                for row in results_by_subject["math"]
            ),
            19,
        )
        for subject in ("cs408", "english"):
            self.assertEqual(
                [row.outcome for row in results_by_subject[subject]],
                ["succeeded"] * 20,
            )
        math_persistence = runtimes["math"].persist_finished_luna_with_status(
            handles_by_subject["math"]
        )
        self.assertEqual(math_persistence["failures"], [])
        self.assertEqual(len(math_persistence["projected"]), 20)
        for subject in ("cs408", "english"):
            self.assertEqual(
                len(
                    runtimes[subject].persist_finished_luna(
                        handles_by_subject[subject]
                    )
                ),
                20,
            )
        success_handles = {
            "math": [
                handle
                for handle in handles_by_subject["math"]
                if handle is not failed_handle
            ],
            "cs408": handles_by_subject["cs408"],
            "english": handles_by_subject["english"],
        }
        self._assert_canonical_task_closures(scenario, success_handles)
        math_state = runtimes[
            "math"
        ].dispatcher.lease_store.production_canary_status_read_only("math")
        self.assertEqual(math_state["state"], "failed_drained")
        self.assertFalse(math_state["luna_consumer_enabled"])
        for subject in ("cs408", "english"):
            state = runtimes[
                subject
            ].dispatcher.lease_store.production_canary_status_read_only(subject)
            self.assertEqual(state["state"], "continuous_concurrent_unlocked")
            self.assertTrue(state["luna_consumer_enabled"])
            writer = runtimes[subject].subject_sol.read_subject(subject)[
                "writer_state"
            ]
            self.assertEqual(writer["handoff_status"], "awaiting_luna")
            self.assertIsNone(writer["batch_id"])
        telemetry = runtimes[
            "english"
        ].dispatcher.lease_store.production_canary_concurrency_telemetry(
            release_id=core.LOADED_CORE_SHA256
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 60)
        release_path.unlink()
        os.environ.pop("BLOCKING_FAIL_CAPTURE_ID", None)

    def test_canonical_english_first_success_then_twenty_rolls_over_real_batches(
        self,
    ) -> None:
        english = english_support.EnglishAdapterTests(methodName="runTest")
        english.setUp()
        self.addCleanup(english.tearDown)
        fixture, _host, host_config = self.support._new_processing_host(
            english.runtime
        )
        host, host_config = self._install_authority_stub(
            runtime_root=english.runtime,
            fixture=fixture,
            host_config=host_config,
        )
        config = self._enable_canary(english.config, host_config)
        config_path = english.base / "canonical-config.json"
        core.atomic_write_json(config_path, config)
        runtime = ProductionDispatchRuntime(config, "english", config_path)
        runtime.dispatcher.lease_store.begin_subject_drain("english")
        runtime.activate_production_canary(
            activated_at="2026-08-10T00:00:00Z"
        )
        sentence = english.write_sentence(
            1,
            article_id="CANONICAL-EN-1",
            evidence_origin="live_user",
        )
        english.write_completion([sentence], index=1001)
        candidates = core.Worker(config).eligible_candidates(
            "english", english.study_date
        )
        self.assertEqual(len(candidates), 1)
        candidate, _reason = candidates[0]
        marker_root = english.base / "markers"
        payload_root = english.base / "stage-payloads"
        payload_root.mkdir()
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        os.environ["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(payload_root)
        store = runtime.dispatcher.lease_store
        runtime.dispatcher.runner_factory = lambda _task, _context: (
            CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            )
        )
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 1, decisions)
        handle = handles[0]
        deadline = time.monotonic() + 10
        marker_path = marker_root / f"{handle.unit_sha256}.json"
        while not marker_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker_path.is_file())
        stage_payload = self._publish_english_candidate(
            config=config,
            runtime_root=english.runtime,
            candidate=candidate,
            prepared_host=(fixture, host, host_config),
        )
        core.atomic_write_json(
            payload_root / f"{handle.unit_sha256}.json", stage_payload
        )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        result = handle.wait(20)
        self.assertEqual(result.outcome, "succeeded", result)
        projected = runtime.persist_finished_luna([handle])
        self.assertEqual(len(projected), 1)
        subject_state = runtime.subject_sol.read_subject("english")
        self.assertEqual(
            subject_state["writer_state"]["handoff_status"],
            "awaiting_luna",
        )
        self.assertIsNone(subject_state["writer_state"]["batch_id"])
        pointer = core.load_json(
            runtime.subject_sol._background_rollover_pointer_path("english")
        )
        self.assertEqual(pointer["formal_write_count"], 0)
        gate = store.production_canary_status("english")
        self.assertEqual(
            gate["state"], "continuous_concurrent_unlocked"
        )
        self.assertEqual(gate["formal_write_count"], 0)

        (marker_root / "release-all").unlink()
        for index in range(20):
            event_index = 100 + index
            sentence = english.write_sentence(
                event_index,
                article_id=f"CANONICAL-EN-{event_index}",
                evidence_origin="live_user",
            )
            english.write_completion(
                [sentence], index=2000 + event_index
            )
        candidate_rows = core.Worker(config).eligible_candidates(
            "english", english.study_date
        )
        self.assertEqual(len(candidate_rows), 20)
        candidates_by_capture = {
            candidate.capture_id: candidate
            for candidate, _reason in candidate_rows
        }
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 20, decisions)
        expected_units = {handle.unit_sha256 for handle in handles}
        deadline = time.monotonic() + 20
        markers: dict[str, dict] = {}
        while time.monotonic() < deadline:
            markers = {}
            for path in marker_root.glob("*.json"):
                value = core.load_json(path)
                if value.get("unit_sha256") in expected_units:
                    markers[str(value["unit_sha256"])] = value
            if set(markers) == expected_units:
                break
            time.sleep(0.01)
        self.assertEqual(set(markers), expected_units)
        self.assertEqual(len({row["pid"] for row in markers.values()}), 20)
        self.assertTrue(
            all(row["pid"] == row["pgid"] for row in markers.values())
        )
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(
                len({row[field] for row in markers.values()}), 20
            )
        for handle in handles:
            capture_id = str(handle.task.frozen_payload["capture_id"])
            candidate = candidates_by_capture[capture_id]
            stage_payload = self._publish_english_candidate(
                config=config,
                runtime_root=english.runtime,
                candidate=candidate,
                prepared_host=(fixture, host, host_config),
            )
            core.atomic_write_json(
                payload_root / f"{handle.unit_sha256}.json",
                stage_payload,
            )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        results = [handle.wait(30) for handle in handles]
        self.assertEqual([result.outcome for result in results], [
            "succeeded"
        ] * 20)
        projected = runtime.persist_finished_luna(handles)
        self.assertEqual(len(projected), 20)
        writer = runtime.subject_sol.read_subject("english")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        telemetry = store.production_canary_concurrency_telemetry(
            release_id=core.LOADED_CORE_SHA256
        )
        self.assertGreaterEqual(
            telemetry["subject_peak_active"]["english"], 20
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 20)
        self.assertEqual(
            telemetry["runner_interval_missing_count_global"], 0
        )
        projection = core.load_json(
            english.runtime
            / "dispatch/state/subject-projections/english.json"
        )
        self.assertEqual(projection["formal_write_count"], 0)

    def test_canonical_math_first_success_then_twenty_rolls_over_real_batches(
        self,
    ) -> None:
        math = math_support.MathV2CoreTests(methodName="runTest")
        math.setUp()
        self.addCleanup(math.tearDown)
        core.atomic_write_json(
            math.status_path,
            {
                "schema_version": "math-fast-intake-status-v1",
                "study_date": "2026-08-11",
                "pending_count": 0,
                "closed_count": 0,
                "pending": [],
                "closed": [],
                "active_freezes": [],
                "ledger_hash": hashlib.sha256(b"empty-math").hexdigest(),
            },
        )
        fixture, _host, host_config = self.support._new_processing_host(
            math.runtime
        )
        host, host_config = self._install_authority_stub(
            runtime_root=math.runtime,
            fixture=fixture,
            host_config=host_config,
        )
        config = self._enable_canary(math.config, host_config)
        config["math_deep_v2"]["mode"] = "production"
        config_path = math.base / "canonical-config.json"
        core.atomic_write_json(config_path, config)
        runtime = ProductionDispatchRuntime(config, "math", config_path)
        runtime.dispatcher.lease_store.begin_subject_drain("math")
        runtime.activate_production_canary(
            activated_at="2026-08-10T00:00:00Z"
        )
        self._write_math_captures(math, count=1, start=1000)
        candidate_rows = core.Worker(config).eligible_candidates(
            "math", "2026-08-11"
        )
        self.assertEqual(len(candidate_rows), 1)
        candidate, _reason = candidate_rows[0]
        marker_root = math.base / "markers"
        payload_root = math.base / "stage-payloads"
        payload_root.mkdir()
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        os.environ["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(payload_root)
        store = runtime.dispatcher.lease_store
        runtime.dispatcher.runner_factory = lambda _task, _context: (
            CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            )
        )
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 1, decisions)
        handle = handles[0]
        deadline = time.monotonic() + 10
        marker_path = marker_root / f"{handle.unit_sha256}.json"
        while not marker_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker_path.is_file())
        stage_payload = self._publish_math_candidate(
            config=config,
            runtime_root=math.runtime,
            candidate=candidate,
            prepared_host=(fixture, host, host_config),
        )
        core.atomic_write_json(
            payload_root / f"{handle.unit_sha256}.json", stage_payload
        )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        result = handle.wait(20)
        self.assertEqual(result.outcome, "succeeded", result)
        projected = runtime.persist_finished_luna([handle])
        self.assertEqual(len(projected), 1)
        writer = runtime.subject_sol.read_subject("math")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        pointer = core.load_json(
            runtime.subject_sol._background_rollover_pointer_path("math")
        )
        self.assertEqual(pointer["formal_write_count"], 0)

        (marker_root / "release-all").unlink()
        self._write_math_captures(math, count=20, start=2000)
        candidate_rows = core.Worker(config).eligible_candidates(
            "math", "2026-08-11"
        )
        self.assertEqual(len(candidate_rows), 20)
        candidates_by_capture = {
            candidate.capture_id: candidate
            for candidate, _reason in candidate_rows
        }
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 20, decisions)
        expected_units = {handle.unit_sha256 for handle in handles}
        deadline = time.monotonic() + 20
        markers: dict[str, dict] = {}
        while time.monotonic() < deadline:
            markers = {}
            for path in marker_root.glob("*.json"):
                value = core.load_json(path)
                if value.get("unit_sha256") in expected_units:
                    markers[str(value["unit_sha256"])] = value
            if set(markers) == expected_units:
                break
            time.sleep(0.01)
        self.assertEqual(set(markers), expected_units)
        self.assertEqual(len({row["pid"] for row in markers.values()}), 20)
        self.assertTrue(
            all(row["pid"] == row["pgid"] for row in markers.values())
        )
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(
                len({row[field] for row in markers.values()}), 20
            )
        for handle in handles:
            capture_id = str(handle.task.frozen_payload["capture_id"])
            stage_payload = self._publish_math_candidate(
                config=config,
                runtime_root=math.runtime,
                candidate=candidates_by_capture[capture_id],
                prepared_host=(fixture, host, host_config),
            )
            core.atomic_write_json(
                payload_root / f"{handle.unit_sha256}.json",
                stage_payload,
            )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        results = [handle.wait(30) for handle in handles]
        self.assertEqual(
            [result.outcome for result in results], ["succeeded"] * 20
        )
        projected = runtime.persist_finished_luna(handles)
        self.assertEqual(len(projected), 20)
        writer = runtime.subject_sol.read_subject("math")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        telemetry = store.production_canary_concurrency_telemetry(
            release_id=core.LOADED_CORE_SHA256
        )
        self.assertGreaterEqual(
            telemetry["subject_peak_active"]["math"], 20
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 20)
        self.assertEqual(
            telemetry["runner_interval_missing_count_global"], 0
        )

    def test_canonical_cs408_first_success_then_twenty_rolls_over_real_batches(
        self,
    ) -> None:
        cs408 = cs408_support.PreprocessorTests(methodName="runTest")
        cs408.setUp()
        self.addCleanup(cs408.tearDown)
        config = cs408.make_v2_config()
        core.atomic_write_json(
            cs408.cs_status_path,
            {
                "schema": "intake_fact_capture_state_v2",
                "study_date": "2026-08-11",
                "capture_count": 0,
                "pending_capture_ids": [],
                "captures": {},
                "neutral_saves": {},
                "active_batch": None,
                "latest_date_batch": None,
            },
        )
        fixture, _host, host_config = self.support._new_processing_host(
            cs408.runtime
        )
        host, host_config = self._install_authority_stub(
            runtime_root=cs408.runtime,
            fixture=fixture,
            host_config=host_config,
        )
        config = self._enable_canary(config, host_config)
        config_path = cs408.base / "canonical-config.json"
        core.atomic_write_json(config_path, config)
        runtime = ProductionDispatchRuntime(config, "cs408", config_path)
        runtime.dispatcher.lease_store.begin_subject_drain("cs408")
        runtime.activate_production_canary(
            activated_at="2026-08-10T00:00:00Z"
        )
        self._write_cs408_captures(cs408, config, count=1, start=1000)
        worker = core.Worker(config)
        status = worker.adapters["cs408"].status("2026-08-11")
        candidates = worker.adapters["cs408"].candidates(status)
        self.assertEqual(
            len(candidates), 1, worker.adapters["cs408"].candidate_errors
        )
        candidate = candidates[0]
        marker_root = cs408.base / "markers"
        payload_root = cs408.base / "stage-payloads"
        payload_root.mkdir()
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        os.environ["ZERO_MODEL_STAGE_PAYLOAD_ROOT"] = str(payload_root)
        store = runtime.dispatcher.lease_store
        runtime.dispatcher.runner_factory = lambda _task, _context: (
            CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(FIXTURE)],
                lease_store=store,
            )
        )
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 1, decisions)
        handle = handles[0]
        deadline = time.monotonic() + 10
        marker_path = marker_root / f"{handle.unit_sha256}.json"
        while not marker_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker_path.is_file())
        stage_payload = self._publish_cs408_candidate(
            config=config,
            runtime_root=cs408.runtime,
            candidate=candidate,
            prepared_host=(fixture, host, host_config),
        )
        core.atomic_write_json(
            payload_root / f"{handle.unit_sha256}.json", stage_payload
        )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        result = handle.wait(20)
        self.assertEqual(result.outcome, "succeeded", result)
        projected = runtime.persist_finished_luna([handle])
        self.assertEqual(len(projected), 1)
        writer = runtime.subject_sol.read_subject("cs408")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        pointer = core.load_json(
            runtime.subject_sol._background_rollover_pointer_path("cs408")
        )
        self.assertEqual(pointer["formal_write_count"], 0)

        (marker_root / "release-all").unlink()
        self._write_cs408_captures(
            cs408, config, count=20, start=2000
        )
        worker = core.Worker(config)
        status = worker.adapters["cs408"].status("2026-08-11")
        candidates = worker.adapters["cs408"].candidates(status)
        self.assertEqual(
            len(candidates), 20, worker.adapters["cs408"].candidate_errors
        )
        candidates_by_capture = {
            candidate.capture_id: candidate for candidate in candidates
        }
        handles, decisions = runtime.scan_and_submit()
        self.assertEqual(len(handles), 20, decisions)
        expected_units = {handle.unit_sha256 for handle in handles}
        deadline = time.monotonic() + 20
        markers: dict[str, dict] = {}
        while time.monotonic() < deadline:
            markers = {}
            for path in marker_root.glob("*.json"):
                value = core.load_json(path)
                if value.get("unit_sha256") in expected_units:
                    markers[str(value["unit_sha256"])] = value
            if set(markers) == expected_units:
                break
            time.sleep(0.01)
        self.assertEqual(set(markers), expected_units)
        self.assertEqual(len({row["pid"] for row in markers.values()}), 20)
        self.assertTrue(
            all(row["pid"] == row["pgid"] for row in markers.values())
        )
        for field in (
            "context_root",
            "mcp_session_root",
            "report_root",
            "read_session_id",
        ):
            self.assertEqual(
                len({row[field] for row in markers.values()}), 20
            )
        for handle in handles:
            capture_id = str(handle.task.frozen_payload["capture_id"])
            stage_payload = self._publish_cs408_candidate(
                config=config,
                runtime_root=cs408.runtime,
                candidate=candidates_by_capture[capture_id],
                prepared_host=(fixture, host, host_config),
            )
            core.atomic_write_json(
                payload_root / f"{handle.unit_sha256}.json",
                stage_payload,
            )
        (marker_root / "release-all").write_text(
            "release\n", encoding="utf-8"
        )
        results = [handle.wait(30) for handle in handles]
        self.assertEqual(
            [result.outcome for result in results], ["succeeded"] * 20
        )
        projected = runtime.persist_finished_luna(handles)
        self.assertEqual(len(projected), 20)
        writer = runtime.subject_sol.read_subject("cs408")["writer_state"]
        self.assertEqual(writer["handoff_status"], "awaiting_luna")
        self.assertIsNone(writer["batch_id"])
        telemetry = store.production_canary_concurrency_telemetry(
            release_id=core.LOADED_CORE_SHA256
        )
        self.assertGreaterEqual(
            telemetry["subject_peak_active"]["cs408"], 20
        )
        self.assertGreaterEqual(telemetry["global_peak_active"], 20)
        self.assertEqual(
            telemetry["runner_interval_missing_count_global"], 0
        )


if __name__ == "__main__":
    unittest.main()
