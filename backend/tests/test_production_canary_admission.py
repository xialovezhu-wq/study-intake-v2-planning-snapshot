from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
)
from preprocess_dispatcher import (  # noqa: E402
    ProductionDispatchRuntime,
    _audit,
    _run_daemon,
)
from subject_sol_contract import SubjectSolContractError  # noqa: E402
from process_identity import kernel_process_start_token  # noqa: E402
from fixtures.process_lifecycle import (  # noqa: E402
    communicate_with_cleanup,
    process_absent,
    register_process,
    stop_process,
)


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def authority(subject: str, release_id: str) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "study-intake-producer-authority-v1",
        "subject": subject,
        "release_id": release_id,
        "loaded_core_sha256": "1" * 64,
        "processing_contract_sha256": "2" * 64,
        "model": REQUIRED_MODEL,
        "reasoning_effort": REQUIRED_REASONING_EFFORT,
        "formal_write_count": 0,
    }
    return {**core, "authority_fingerprint": sha(core)}


def canary_task(
    index: int,
    *,
    subject: str = "math",
    release_id: str = "a" * 64,
    recorded_at: str = "2026-08-11T00:00:01Z",
    capture_id: str | None = None,
    fingerprint_suffix: str = "",
) -> FrozenTask:
    capture = capture_id or f"POST-{subject}-{index:04d}"
    input_fingerprint = hashlib.sha256(
        f"{subject}:{capture}:{index}:{fingerprint_suffix}".encode()
    ).hexdigest()
    source_event = {
        "event_id": capture,
        "recorded_at": recorded_at,
        "source_sha256": hashlib.sha256(
            f"source:{subject}:{capture}:{index}:{fingerprint_suffix}".encode()
        ).hexdigest(),
    }
    capture_type = "fact_observation" if subject == "math" else None
    evidence_contract = (
        {
            "schema_version": (
                "study-intake-math-capture-evidence-contract-v1"
            ),
            "capture_type": capture_type,
            "stable_formal_id": None,
            "source_fingerprint": source_event["source_sha256"],
            "content_fingerprint": None,
            "verbatim_learning_record_sha256": hashlib.sha256(
                f"record:{capture}".encode()
            ).hexdigest(),
            "full_dialogue_sha256": hashlib.sha256(
                f"dialogue:{capture}".encode()
            ).hexdigest(),
            "question_image_sha256s": [],
            "solution_image_sha256s": [],
            "canonical_solution_text_sha256": None,
            "evidence_status": "ready",
            "missing_roles": [],
            "formal_write_count": 0,
        }
        if subject == "math"
        else None
    )
    producer_core = {
        "schema_version": "study-intake-producer-dispatch-input-v2",
        "subject": subject,
        "release_id": release_id,
        "authority_fingerprint": authority(subject, release_id)[
            "authority_fingerprint"
        ],
        "producer_unit_id": capture,
        "producer_recorded_at": recorded_at,
        "input_fingerprint": input_fingerprint,
        "source_events": [source_event],
        "earliest_source_recorded_at": recorded_at,
        "latest_source_recorded_at": recorded_at,
        "source_event_set_sha256": sha([source_event]),
        "capture_type": capture_type,
        "evidence_contract": evidence_contract,
        "evidence_contract_sha256": (
            sha(evidence_contract) if evidence_contract is not None else None
        ),
        "formal_write_count": 0,
    }
    producer_contract = {
        **producer_core,
        "producer_input_contract_sha256": sha(producer_core),
    }
    return FrozenTask(
        {
            "subject": subject,
            "capture_id": capture,
            "study_date": "2026-08-11",
            "recorded_at": recorded_at,
            "input_fingerprint": input_fingerprint,
            "input_binding": {"source": "append-only-producer"},
            "model_input": {"question": f"question-{index}"},
            "allowed_evidence_refs": [f"capture:{capture}"],
            "image_paths": [],
            "target_label": capture,
            "canonical_state": "pending",
            "sol_state": "disabled",
            "dispatch_contract": {
                "schema_version": "study-intake-dispatch-release-binding-v1",
                "release_id": release_id,
                "dispatch_reason": "production_canary_pending_queue",
                "requested_service_tier": None,
                "fast_mode_requested": False,
                "fast_mode_effective": "not_requested",
                "producer_input_contract": producer_contract,
            },
        }
    )


def english_canary_task(
    index: int,
    *,
    evidence_origin: str,
    release_id: str = "a" * 64,
    recorded_at: str = "2026-08-11T00:00:01Z",
) -> FrozenTask:
    task = canary_task(
        index,
        subject="english",
        release_id=release_id,
        recorded_at=recorded_at,
    )
    payload = copy.deepcopy(dict(task.frozen_payload))
    contract = payload["dispatch_contract"]["producer_input_contract"]
    event = {
        "event_id": contract["source_events"][0]["event_id"],
        "event_type": "sentence_captured",
        "occurred_at": recorded_at,
        "learning": {"evidence_origin": evidence_origin},
    }
    payload["model_input"] = {"batch_events": [event]}
    source_event = {
        "event_id": event["event_id"],
        "recorded_at": recorded_at,
        "source_sha256": sha(event),
    }
    contract["source_events"] = [source_event]
    contract["source_event_set_sha256"] = sha([source_event])
    contract_core = dict(contract)
    contract_core.pop("producer_input_contract_sha256", None)
    contract["producer_input_contract_sha256"] = sha(contract_core)
    return FrozenTask(payload)


def grounded_stage(stage: str, task: FrozenTask) -> StageResult:
    evidence_ref = str(task.frozen_payload["allowed_evidence_refs"][0])
    subject = str(task.frozen_payload["subject"])
    generation = f"{subject}-test-generation"
    return StageResult(
        payload={
            "stage": stage,
            "unit_sha256": task.unit_sha256,
            "evidence_refs": [evidence_ref],
        },
        runtime_model=REQUIRED_MODEL,
        runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
        runtime_metadata_provenance="codex_json_attestation_v1",
        runtime_identity_status="confirmed",
        duration_ms=1,
        read_session_id="READ-CANARY-SHARED",
        read_session_manifest_sha256=sha(
            {"session": "READ-CANARY-SHARED"}
        ),
        authority_snapshot_manifest_sha256=sha(
            {"snapshot": "READ-CANARY-SHARED"}
        ),
        capture_freeze_receipt_sha256=sha(
            {"capture": task.frozen_payload["capture_id"]}
        ),
        mcp_read_session_receipt_sha256=sha(
            {"read_session": "READ-CANARY-SHARED"}
        ),
        evidence_generation=generation,
        evidence_authority_fingerprint=sha(
            {"authority": generation}
        ),
        evidence_subject=subject,
        evidence_release_id=str(
            task.frozen_payload["dispatch_contract"]["release_id"]
        ),
        mcp_grounding_manifest_sha256=sha(
            {"stage": stage, "ref": evidence_ref}
        ),
        mcp_transcript_sha256=sha(
            {"transcript": stage, "ref": evidence_ref}
        ),
        mcp_consumed_evidence_refs=(evidence_ref,),
        mcp_cited_evidence_refs=(evidence_ref,),
        mcp_stage_grounded_evidence_refs=(evidence_ref,),
        provider_request_count=2,
        mcp_tool_call_count=1,
        model_call_count=1,
    )


class GroundedRunner:
    def run_analysis(self, task, _context):
        return grounded_stage("analysis", task)

    def run_critical_review(self, task, _draft, _context):
        return grounded_stage("critical_review", task)

    def cancel(self, _context):
        return None


class FailedRunner(GroundedRunner):
    def run_analysis(self, _task, _context):
        raise RuntimeError("synthetic-canary-failure")


class BlockingGroundedRunner(GroundedRunner):
    def __init__(self, entered: threading.Event, release: threading.Event) -> None:
        self.entered = entered
        self.release = release

    def run_analysis(self, task, context):
        self.entered.set()
        self.release.wait(5)
        return super().run_analysis(task, context)


class CrossSessionRunner(GroundedRunner):
    def run_critical_review(self, task, _draft, _context):
        value = grounded_stage("critical_review", task)
        return StageResult(
            **{
                **value.__dict__,
                "read_session_id": "READ-CROSS-SUBJECT-SESSION",
            }
        )


class MutatedCriticalBindingRunner(GroundedRunner):
    def __init__(self, key: str, value: str) -> None:
        self.key = key
        self.value = value

    def run_critical_review(self, task, _draft, _context):
        stage = grounded_stage("critical_review", task)
        return StageResult(**{**stage.__dict__, self.key: self.value})


class MissingGroundingRunner(GroundedRunner):
    def run_critical_review(self, task, _draft, _context):
        return StageResult(
            payload={"stage": "critical_review", "unit": task.unit_sha256},
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            provider_request_count=2,
            mcp_tool_call_count=1,
            model_call_count=1,
        )


class IdentityPublishingFixtureRunner:
    """Zero-model fixture with a real isolated supervisor process identity."""

    def __init__(self, inner, store: LeaseStore) -> None:
        self.inner = inner
        self.store = store
        self.executes_full_two_pass_in_analysis = bool(
            getattr(inner, "executes_full_two_pass_in_analysis", False)
        )

    def _publish_identity(self, task, context) -> None:
        process = subprocess.Popen(
            ["/bin/sh", "-c", "read fixture_line"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        registration = register_process(process, require_private_group=True)
        refs = None
        stdout = b""
        stderr = b""
        try:
            pgid = os.getpgid(process.pid)
            token = kernel_process_start_token(process.pid)
            launched_at = (
                dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            refs = self.store.publish_task_process_identity(
                task,
                context.lease,
                child_pid=process.pid,
                child_pgid=pgid,
                process_start_token=token,
                launch_nonce=uuid.uuid4().hex,
                launched_at=launched_at,
                argv=["/bin/sh", "-c", "read fixture_line"],
                executable_path=Path("/bin/sh"),
                start_new_session=True,
            )
            self.store.record_task_event(
                task,
                context.lease,
                "child_process_started",
                artifact_refs={
                    "process_identity_sha256": refs[
                        "process_identity_sha256"
                    ],
                    "process_identity_path": refs["process_identity_path"],
                },
            )
        finally:
            stdout, stderr = communicate_with_cleanup(
                registration,
                b"fixture\n",
                timeout=2,
            )
        if refs is not None:
            finished_at = (
                dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            exit_refs = self.store.publish_task_process_exit(
                task,
                context.lease,
                process_identity_sha256=refs["process_identity_sha256"],
                process_identity_path=refs["process_identity_path"],
                returncode=int(process.returncode or 0),
                termination_reason="completed",
                reaped=True,
                process_absent=True,
                pgid_absent=True,
                stdout_sha256=hashlib.sha256(stdout).hexdigest(),
                stdout_size=len(stdout),
                stderr_sha256=hashlib.sha256(stderr).hexdigest(),
                stderr_size=len(stderr),
                finished_at=finished_at,
            )
            self.store.record_task_event(
                task,
                context.lease,
                "child_process_exited",
                artifact_refs={
                    "process_identity_sha256": refs[
                        "process_identity_sha256"
                    ],
                    "process_identity_path": refs["process_identity_path"],
                    "process_exit_sha256": exit_refs[
                        "task_process_exit_sha256"
                    ],
                    "process_exit_path": exit_refs[
                        "task_process_exit_path"
                    ],
                },
            )

    def run_analysis(self, task, context):
        self._publish_identity(task, context)
        return self.inner.run_analysis(task, context)

    def run_critical_review(self, task, draft, context):
        return self.inner.run_critical_review(task, draft, context)

    def cancel(self, context):
        return self.inner.cancel(context)


class ProductionCanaryAdmissionTests(unittest.TestCase):
    def test_runtime_projects_verified_task_events_monotonically_into_batch(self) -> None:
        unit = "a" * 64
        batch = {
            "status": "frozen",
            "batch_id": "LUNA-CS408-TEST",
            "tasks": [
                {
                    "capture_id": "OBS-CS408-TEST",
                    "unit_sha256": unit,
                    "status": "selected",
                }
            ],
        }
        latest = {
            "event": "analysis_submitted",
            "stage_name": "analysis",
        }

        class FakeSubjectSol:
            def read_subject_batch(self, _subject):
                return copy.deepcopy(batch)

            def record_task_progress(self, **kwargs):
                order = {
                    "selected": 0,
                    "claimed": 1,
                    "analysis_running": 2,
                    "critical_review_running": 3,
                }
                current = batch["tasks"][0]["status"]
                desired = kwargs["status"]
                if order[desired] > order[current]:
                    batch["tasks"][0]["status"] = desired
                return copy.deepcopy(batch)

        class FakeLeaseStore:
            def verify_task_event_history(self, _unit, *, expected_release_id):
                self.expected_release_id = expected_release_id
                return {"events": [{"event": copy.deepcopy(latest)}]}

        runtime = object.__new__(ProductionDispatchRuntime)
        runtime.subject = "cs408"
        runtime.config = {}
        runtime.subject_sol = FakeSubjectSol()
        lease_store = FakeLeaseStore()
        runtime.dispatcher = SimpleNamespace(lease_store=lease_store)

        projected = runtime.sync_subject_batch_task_progress()
        self.assertEqual(projected["tasks"][0]["status"], "analysis_running")
        latest.update(
            event="provider_process_started",
            stage_name="critical_review",
        )
        projected = runtime.sync_subject_batch_task_progress()
        self.assertEqual(
            projected["tasks"][0]["status"], "critical_review_running"
        )
        latest.update(event="analysis_submitted", stage_name="analysis")
        projected = runtime.sync_subject_batch_task_progress()
        self.assertEqual(
            projected["tasks"][0]["status"], "critical_review_running"
        )
        latest.update(event="failed", stage_name="terminal")
        projected = runtime.sync_subject_batch_task_progress()
        self.assertEqual(
            projected["tasks"][0]["status"], "critical_review_running"
        )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.release_id = "a" * 64
        self.activated_at = "2026-08-11T00:00:00Z"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_fixture_process_timeout_cleanup_reaps_registered_pid_and_pgid(
        self,
    ) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import signal, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "time.sleep(30)",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        registration = register_process(process, require_private_group=True)
        try:
            communicate_with_cleanup(
                registration,
                timeout=0.01,
                term_timeout=0.05,
                kill_timeout=0.5,
            )
        finally:
            if not process_absent(registration):
                stop_process(registration, term_timeout=0.05, kill_timeout=0.5)
        self.assertTrue(process_absent(registration))
        self.assertEqual(registration.pid, process.pid)
        self.assertEqual(registration.pgid, registration.pid)

    def arm(self, subject: str = "math") -> LeaseStore:
        store = LeaseStore(self.runtime)
        store.begin_subject_drain(subject)
        state = store.activate_production_canary(
            subject,
            release_id=self.release_id,
            producer_authority=authority(subject, self.release_id),
            activated_at=self.activated_at,
        )
        self.assertEqual(state["status"], "production_canary_active")
        self.assertEqual(state["state"], "armed")
        self.assertTrue(store.subject_status(subject)["draining"])
        return store

    def dispatcher(self, runner_factory=None) -> ConcurrentDispatcher:
        base_factory = runner_factory or (
            lambda _task, _context: GroundedRunner()
        )
        store = LeaseStore(self.runtime)
        return ConcurrentDispatcher(
            self.runtime,
            lambda task, context: IdentityPublishingFixtureRunner(
                base_factory(task, context), store
            ),
            soft_runtime_warning_seconds=2,
            stall_timeout_seconds=30,
            stall_probe_interval_seconds=1,
            stall_probe_required_consecutive_failures=2,
            production_canary=True,
        )

    def scan_runtime(self, subject_sol: object) -> ProductionDispatchRuntime:
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.config = {}
        runtime.subject = "math"
        runtime.production_canary = True
        runtime.dispatcher = self.dispatcher()
        runtime.subject_sol = subject_sol
        runtime.processing_host = None
        runtime._frozen_batch_authority = None
        return runtime

    def test_activation_requires_drain_and_is_idempotent(self) -> None:
        store = LeaseStore(self.runtime)
        with self.assertRaisesRegex(
            DispatchError, "production_canary_subject_not_drained"
        ):
            store.activate_production_canary(
                "math",
                release_id=self.release_id,
                producer_authority=authority("math", self.release_id),
                activated_at=self.activated_at,
            )
        store.begin_subject_drain("math")
        first = store.activate_production_canary(
            "math",
            release_id=self.release_id,
            producer_authority=authority("math", self.release_id),
            activated_at=self.activated_at,
        )
        second = store.activate_production_canary(
            "math",
            release_id=self.release_id,
            producer_authority=authority("math", self.release_id),
            activated_at="2026-08-11T00:01:00Z",
        )
        self.assertEqual(first["activation_id"], second["activation_id"])

    def test_production_runtime_derives_plugin_release_from_manifest(self) -> None:
        release_manifest = Path(self.temp.name) / "release.json"
        release_manifest.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-preprocessor-release-v2",
                    "release_id": self.release_id,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        config_path = Path(self.temp.name) / "config.json"
        config = {
            "runtime_root": str(self.runtime),
            "release": {"manifest_path": str(release_manifest)},
            "processing_plugin": {"enabled": True},
            "worker": {"model_timeout_seconds": 1},
            "math_deep_v2": {
                "soft_runtime_warning_seconds": 1,
                "stall_timeout_seconds": 60,
                "stall_probe_interval_seconds": 1,
                "stall_probe_required_consecutive_failures": 2,
            },
        }
        config_path.write_text(json.dumps(config) + "\n", encoding="utf-8")

        with mock.patch("preprocess_dispatcher.ProcessingPluginHost") as host:
            runtime = ProductionDispatchRuntime(config, "math", config_path)

        host.assert_called_once_with(
            config["processing_plugin"],
            runtime_root=self.runtime,
            candidate_release_id=self.release_id,
            subject_roots={},
            require_authority_snapshot=True,
        )
        self.assertIs(runtime.processing_host, host.return_value)

    def test_old_capture_and_resigned_old_capture_are_rejected(self) -> None:
        store = self.arm()
        for suffix in ("", "resigned"):
            old = canary_task(
                1,
                recorded_at=self.activated_at,
                capture_id="OLD-CAPTURE",
                fingerprint_suffix=suffix,
            )
            with self.assertRaisesRegex(
                DispatchError, "production_canary_pre_activation_capture"
            ):
                store.materialize_production_canary_task(old)
            store.record_production_canary_pre_activation_exclusion(old)
        self.assertEqual(store.production_canary_status("math")["queue_depth"], 0)
        state = store.production_canary_status("math")
        self.assertEqual(state["excluded_by_high_watermark_count"], 1)
        self.assertEqual(
            state["queue_classification"]["pre_activation_frozen"], 1
        )

    def test_canary_rejects_any_fast_mode_request_or_missing_normal_binding(self) -> None:
        store = self.arm()
        valid = canary_task(9)
        for mutation in (
            {"fast_mode_requested": True},
            {"requested_service_tier": "priority"},
            {"fast_mode_effective": "requested_unverified"},
        ):
            payload = copy.deepcopy(dict(valid.frozen_payload))
            payload["dispatch_contract"].update(mutation)
            with self.assertRaisesRegex(
                DispatchError, "production_canary_fast_mode_binding_invalid"
            ):
                store.materialize_production_canary_task(FrozenTask(payload))

    def test_english_canary_rejects_synthetic_before_materialization(self) -> None:
        store = self.arm("english")
        synthetic = english_canary_task(
            90,
            evidence_origin="synthetic_fixture",
            release_id=self.release_id,
        )
        with self.assertRaisesRegex(
            DispatchError,
            "production_canary_synthetic_fixture_forbidden",
        ):
            store.materialize_production_canary_task(synthetic)
        state = store.production_canary_status_read_only("english")
        self.assertEqual(state["queue_depth"], 0)
        self.assertEqual(state["canary_queue_count"], 0)
        self.assertEqual(state["state"], "armed")
        self.assertFalse(
            store._production_canary_queue_subject_root("english").exists()
        )

        live = english_canary_task(
            91,
            evidence_origin="live_user",
            release_id=self.release_id,
            recorded_at="2026-08-11T00:00:02Z",
        )
        entry = store.materialize_production_canary_task(live)
        self.assertEqual(entry["queue_status"], "pending")
        self.assertEqual(
            [task.unit_sha256 for task in store.pending_production_canary_tasks("english")],
            [live.unit_sha256],
        )

    def test_english_canary_revalidates_persisted_pending_task(self) -> None:
        store = self.arm("english")
        synthetic = english_canary_task(
            92,
            evidence_origin="synthetic_fixture",
            release_id=self.release_id,
        )
        state = store.production_canary_status_read_only("english")
        contract = store._producer_contract_from_task(synthetic)
        store._materialize_canary_queue_postimage_locked(
            task=synthetic,
            state=state,
            contract=contract,
        )
        with self.assertRaisesRegex(
            DispatchError,
            "production_canary_synthetic_fixture_forbidden",
        ):
            store.pending_production_canary_tasks("english")

    def test_audit_classifies_old_without_submit_or_provider(self) -> None:
        store = self.arm()
        old = canary_task(
            1,
            recorded_at=self.activated_at,
            capture_id="AUDIT-OLD",
        )
        decisions = [
            {
                "subject": "math",
                "capture_id": "AUDIT-OLD",
                "study_date": "2026-08-11",
                "unit_sha256": old.unit_sha256,
                "eligible": True,
                "model_enqueue_allowed": True,
            }
        ]
        config = {
            "runtime_root": str(self.runtime),
            "dispatch": {
                "production_canary": {
                    "enabled": True,
                    "status": "production_canary_active",
                    "admission": "first_post_activation_producer_capture",
                    "keep_backlog_drained": True,
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                }
            },
        }
        before = {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([SimpleNamespace(task=old)], decisions),
        ):
            result = _audit(config, "math")
        after = {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }
        self.assertEqual(after, before)
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["provider_request_count"], 0)
        self.assertEqual(result["excluded_by_high_watermark_count"], 1)
        self.assertEqual(result["canary_queue_count"], 0)
        self.assertTrue(result["read_only"])
        self.assertIn("lease_status", result)
        self.assertTrue(result["lease_status"]["read_only"])
        self.assertEqual(result["draining"], result["lease_status"]["draining"])
        self.assertEqual(
            result["active_count"], result["lease_status"]["active_count"]
        )
        self.assertEqual(
            result["claimed_total"], result["lease_status"]["claimed_total"]
        )
        self.assertEqual(
            result["decisions"][0]["reason"], "pre_activation_frozen"
        )
        self.assertFalse(result["decisions"][0]["model_enqueue_allowed"])

    def test_producer_contract_failure_is_durable_but_old_exclusion_is_normal(
        self,
    ) -> None:
        store = self.arm()
        old = canary_task(
            80,
            recorded_at=self.activated_at,
            capture_id="PRECLAIM-OLD",
        )
        invalid_payload = copy.deepcopy(dict(canary_task(81).frozen_payload))
        invalid_payload["dispatch_contract"]["fast_mode_requested"] = True
        invalid = FrozenTask(invalid_payload)
        valid = canary_task(82, recorded_at="2026-08-11T00:01:02Z")

        class ConsumerMustRemainOff:
            def luna_admission(self, _subject):
                raise AssertionError("failed consumer must not be admitted")

        runtime = self.scan_runtime(ConsumerMustRemainOff())

        def scan_rows(include_new: bool = False):
            tasks = [old, invalid, valid]
            if include_new:
                tasks.append(
                    canary_task(83, recorded_at="2026-08-11T00:01:03Z")
                )
            decisions = [
                {
                    "subject": "math",
                    "capture_id": task.frozen_payload["capture_id"],
                    "study_date": "2026-08-11",
                    "unit_sha256": task.unit_sha256,
                    "eligible": True,
                    "model_enqueue_allowed": True,
                }
                for task in tasks
            ]
            return [SimpleNamespace(task=task) for task in tasks], decisions

        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=scan_rows(),
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            handles, decisions = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["queue_depth"], 1)
        self.assertEqual(state["excluded_by_high_watermark_count"], 1)
        self.assertEqual(
            state["last_preclaim_failure_stage"], "producer_contract"
        )
        receipt_path = Path(state["last_terminal_receipt_path"])
        receipt_sha256 = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        self.assertEqual(
            receipt_sha256, state["last_preclaim_failure_receipt_sha256"]
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        store._verify_seal(
            receipt,
            purpose="dispatch-production-canary-preclaim-failure",
        )
        self.assertEqual(receipt["failure_stage"], "producer_contract")
        self.assertFalse(receipt["queue_entry_preserved"])
        old_decision = next(
            row for row in decisions if row.get("unit_sha256") == old.unit_sha256
        )
        self.assertEqual(old_decision["reason"], "pre_activation_frozen")
        self.assertNotIn("preclaim_failure_receipt_sha256", old_decision)

        first_receipt_sha256 = state["last_preclaim_failure_receipt_sha256"]
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=scan_rows(include_new=True),
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            handles, decisions = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        state = store.production_canary_status("math")
        self.assertEqual(state["queue_depth"], 2)
        self.assertEqual(
            state["last_preclaim_failure_receipt_sha256"],
            first_receipt_sha256,
        )
        invalid_decision = next(
            row
            for row in decisions
            if row.get("unit_sha256") == invalid.unit_sha256
        )
        self.assertEqual(
            invalid_decision["phase"],
            "production_canary_preclaim_failed",
        )
        self.assertEqual(
            invalid_decision["preclaim_failure_receipt_sha256"],
            first_receipt_sha256,
        )

    def test_admission_and_preclaim_failures_require_explicit_resume(self) -> None:
        for failure_stage in ("consumer_admission", "pre_claim"):
            with self.subTest(failure_stage=failure_stage):
                if failure_stage == "pre_claim":
                    self.temp.cleanup()
                    self.temp = tempfile.TemporaryDirectory()
                    self.runtime = Path(self.temp.name) / "runtime"
                store = self.arm()
                task = canary_task(84)

                class Admission:
                    calls = 0

                    def luna_admission(self, _subject):
                        self.calls += 1
                        return {
                            "read_session_allowed": (
                                failure_stage == "pre_claim"
                            ),
                            "reason": (
                                "admitted"
                                if failure_stage == "pre_claim"
                                else "subject_generation_write_fence"
                            ),
                        }

                    def submit_luna_under_generation_fence(
                        self, _subject, submitter, submitted_task
                    ):
                        return submitter(submitted_task)

                admission = Admission()
                runtime = self.scan_runtime(admission)
                decisions = [
                    {
                        "subject": "math",
                        "capture_id": task.frozen_payload["capture_id"],
                        "study_date": "2026-08-11",
                        "unit_sha256": task.unit_sha256,
                        "eligible": True,
                        "model_enqueue_allowed": True,
                    }
                ]
                prepare_patch = (
                    mock.patch.object(
                        runtime,
                        "_prepare_batch_before_submit",
                        side_effect=DispatchError(
                            "synthetic_pre_claim_failure"
                        ),
                    )
                    if failure_stage == "pre_claim"
                    else mock.patch.object(
                        runtime,
                        "_prepare_batch_before_submit",
                        wraps=runtime._prepare_batch_before_submit,
                    )
                )
                with mock.patch(
                    "preprocess_dispatcher.scan_eligible_candidates",
                    return_value=([SimpleNamespace(task=task)], decisions),
                ), mock.patch(
                    "preprocess_dispatcher._write_subject_projections"
                ), prepare_patch as prepare:
                    handles, _ = runtime.scan_and_submit()
                    self.assertEqual(handles, [])
                    first_calls = admission.calls
                    handles, _ = runtime.scan_and_submit()
                    self.assertEqual(handles, [])
                    self.assertEqual(admission.calls, first_calls)
                    if failure_stage == "pre_claim":
                        self.assertEqual(prepare.call_count, 1)
                state = store.production_canary_status("math")
                self.assertEqual(state["state"], "failed_drained")
                self.assertEqual(state["queue_depth"], 1)
                self.assertEqual(
                    state["last_preclaim_failure_stage"], failure_stage
                )
                pending = store.pending_production_canary_tasks("math")
                self.assertEqual([row.unit_sha256 for row in pending], [task.unit_sha256])
                resumed = store.resume_production_canary("math")
                self.assertEqual(resumed["state"], "armed")
                self.assertTrue(resumed["luna_consumer_enabled"])

    def test_daemon_generation_fence_failure_does_not_retry_next_poll(self) -> None:
        store = self.arm()
        task = canary_task(85)

        class GenerationFenceFailure:
            submit_calls = 0

            def luna_admission(self, _subject):
                return {"read_session_allowed": True, "reason": "admitted"}

            def submit_luna_under_generation_fence(self, *_args, **_kwargs):
                self.submit_calls += 1
                raise SubjectSolContractError(
                    "synthetic_generation_fence_failure"
                )

        subject_sol = GenerationFenceFailure()
        runtime = self.scan_runtime(subject_sol)
        runtime.sync_subject_batch_task_progress = lambda: None
        installed_handlers: dict[int, object] = {}
        failed_projection_count = 0
        scan_count = 0

        def signal_install(signum, handler):
            previous = installed_handlers.get(signum, 0)
            if callable(handler):
                installed_handlers[signum] = handler
            return previous

        def scan_rows(_config, _subject, **kwargs):
            nonlocal scan_count
            self.assertIsInstance(kwargs.get("producer_recorded_after"), str)
            scan_count += 1
            return [SimpleNamespace(task=task)], [
                {
                    "subject": "math",
                    "capture_id": task.frozen_payload["capture_id"],
                    "study_date": "2026-08-11",
                    "unit_sha256": task.unit_sha256,
                    "eligible": True,
                    "model_enqueue_allowed": True,
                }
            ]

        def projection(*_args, **_kwargs):
            nonlocal failed_projection_count
            state = json.loads(
                store._production_canary_state_path("math").read_text(
                    encoding="utf-8"
                )
            )
            if state.get("state") == "failed_drained":
                failed_projection_count += 1
                if failed_projection_count == 2:
                    installed_handlers[signal.SIGTERM](signal.SIGTERM, None)

        with mock.patch(
            "preprocess_dispatcher.ProductionDispatchRuntime",
            return_value=runtime,
        ), mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            side_effect=scan_rows,
        ), mock.patch(
            "preprocess_dispatcher.signal.signal",
            side_effect=signal_install,
        ), mock.patch(
            "preprocess_dispatcher._write_subject_projections",
            side_effect=projection,
        ), mock.patch("preprocess_dispatcher._poll_interval", return_value=0.01):
            self.assertEqual(_run_daemon({}, "math", Path("unused.json")), 0)
        self.assertGreaterEqual(scan_count, 2)
        self.assertEqual(subject_sol.submit_calls, 1)
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["queue_depth"], 1)
        self.assertEqual(store.subject_status("math")["claimed_total"], 0)
        self.assertEqual(
            state["last_preclaim_failure_stage"],
            "submit_generation_fence",
        )
        self.assertEqual(store.resume_production_canary("math")["state"], "armed")

    def test_preclaim_receipt_crash_windows_reconcile_before_claim(self) -> None:
        for window in (
            "after_index_before_content_addressed",
            "after_content_addressed_before_state",
            "after_state_before_return",
        ):
            with self.subTest(window=window):
                if window != "after_index_before_content_addressed":
                    self.temp.cleanup()
                    self.temp = tempfile.TemporaryDirectory()
                    self.runtime = Path(self.temp.name) / "runtime"
                store = self.arm()
                task = canary_task(86)
                store.materialize_production_canary_task(task)
                if window == "after_index_before_content_addressed":
                    patcher = mock.patch.object(
                        store,
                        "_publish_preclaim_receipt_locked",
                        side_effect=OSError("synthetic receipt publish crash"),
                    )
                elif window == "after_content_addressed_before_state":
                    patcher = mock.patch.object(
                        store,
                        "_commit_production_canary_preclaim_failure_locked",
                        side_effect=RuntimeError(
                            "synthetic pre-state commit crash"
                        ),
                    )
                else:
                    original_commit = (
                        store._commit_production_canary_preclaim_failure_locked
                    )

                    def commit_then_crash(*args, **kwargs):
                        original_commit(*args, **kwargs)
                        raise RuntimeError("synthetic post-state crash")

                    patcher = mock.patch.object(
                        store,
                        "_commit_production_canary_preclaim_failure_locked",
                        side_effect=commit_then_crash,
                    )
                with patcher, self.assertRaises((OSError, RuntimeError)):
                    store.fail_production_canary_preclaim(
                        "math",
                        task,
                        failure_stage="submit_generation_fence",
                        error_code="synthetic_crash_window",
                    )
                restarted = LeaseStore(self.runtime)
                reconciled = restarted.production_canary_status("math")
                self.assertEqual(reconciled["state"], "failed_drained")
                self.assertFalse(reconciled["luna_consumer_enabled"])
                self.assertEqual(reconciled["queue_depth"], 1)
                receipt_path = Path(reconciled["last_terminal_receipt_path"])
                self.assertTrue(receipt_path.is_file())
                self.assertEqual(
                    hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
                    reconciled["last_preclaim_failure_receipt_sha256"],
                )
                pending = restarted.pending_production_canary_tasks("math")
                self.assertEqual(len(pending), 1)
                self.assertEqual(pending[0].unit_sha256, task.unit_sha256)

    def test_first_success_unlocks_continuous_concurrent_consumption(self) -> None:
        store = self.arm()
        first = canary_task(1)
        second = canary_task(2, recorded_at="2026-08-11T00:00:02Z")
        store.materialize_production_canary_task(first)
        store.materialize_production_canary_task(second)
        self.assertEqual(store.production_canary_status("math")["queue_depth"], 2)
        dispatcher = self.dispatcher()
        result = dispatcher.submit(first).wait(5)
        self.assertEqual(result.outcome, "succeeded")
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "continuous_concurrent_unlocked")
        self.assertTrue(state["unlocked_once"])
        self.assertEqual(state["queue_depth"], 1)
        self.assertGreater(state["observed_model_call_count"], 0)
        self.assertGreater(state["observed_provider_request_count"], 0)
        self.assertGreater(state["observed_mcp_tool_call_count"], 0)
        self.assertEqual(state["last_report_status"], "reopen_verified")
        self.assertFalse(state["fast_mode_requested"])
        self.assertEqual(state["fast_mode_effective"], "not_requested")
        self.assertTrue(state["last_analysis_evidence_refs"])
        self.assertTrue(state["last_critical_review_evidence_refs"])
        second_result = dispatcher.submit(second).wait(5)
        self.assertEqual(second_result.outcome, "succeeded")
        self.assertEqual(store.production_canary_status("math")["queue_depth"], 0)
        self.assertTrue(store.subject_status("math")["draining"])

    def test_per_subject_limit_keeps_additional_rows_pending(self) -> None:
        store = self.arm()
        first = canary_task(1)
        second = canary_task(2, recorded_at="2026-08-11T00:00:02Z")
        store.materialize_production_canary_task(first)
        store.materialize_production_canary_task(second)
        entered = threading.Event()
        release = threading.Event()
        dispatcher = self.dispatcher(
            lambda _task, _context: BlockingGroundedRunner(entered, release)
        )
        handle = dispatcher.submit(first)
        self.assertTrue(entered.wait(2))
        with self.assertRaisesRegex(
            DispatchError, "production_canary_first_task_in_flight"
        ):
            dispatcher.submit(second)
        self.assertEqual(store.production_canary_status("math")["queue_depth"], 1)
        release.set()
        self.assertEqual(handle.wait(5).outcome, "succeeded")

    def test_failure_pauses_only_subject_while_producer_queue_grows(self) -> None:
        store = self.arm()
        failed = canary_task(1)
        store.materialize_production_canary_task(failed)
        result = self.dispatcher(
            lambda _task, _context: FailedRunner()
        ).submit(failed).wait(5)
        self.assertEqual(result.outcome, "failed")
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["active_task_count"], 0)
        for index in (2, 3):
            store.materialize_production_canary_task(
                canary_task(
                    index,
                    recorded_at=f"2026-08-11T00:00:0{index}Z",
                )
            )
        state = store.production_canary_status("math")
        self.assertEqual(state["queue_depth"], 2)
        self.assertEqual(store.subject_status("math")["claimed_total"], 0)
        resumed = store.resume_production_canary("math")
        self.assertEqual(resumed["state"], "armed")
        next_task = store.pending_production_canary_tasks("math")[0]
        self.assertEqual(self.dispatcher().submit(next_task).wait(5).outcome, "succeeded")

    def test_failed_daemon_scan_materializes_without_consumer_or_model(self) -> None:
        store = self.arm()
        failed = canary_task(1)
        store.materialize_production_canary_task(failed)
        self.dispatcher(
            lambda _task, _context: FailedRunner()
        ).submit(failed).wait(5)
        new_tasks = [
            canary_task(2, recorded_at="2026-08-11T00:00:02Z"),
            canary_task(3, recorded_at="2026-08-11T00:00:03Z"),
        ]

        class ConsumerMustRemainOff:
            def luna_admission(self, _subject):
                raise AssertionError("consumer admission must not run")

        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.config = {}
        runtime.subject = "math"
        runtime.production_canary = True
        runtime.dispatcher = self.dispatcher()
        runtime.subject_sol = ConsumerMustRemainOff()
        decisions = [
            {
                "subject": "math",
                "capture_id": task.frozen_payload["capture_id"],
                "study_date": "2026-08-11",
                "unit_sha256": task.unit_sha256,
                "eligible": True,
                "model_enqueue_allowed": True,
            }
            for task in new_tasks
        ]
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(
                [SimpleNamespace(task=task) for task in new_tasks],
                decisions,
            ),
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            handles, projected = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        self.assertTrue(
            all(row["phase"] == "producer_queue_pending" for row in projected)
        )
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertEqual(state["queue_depth"], 2)
        self.assertEqual(state["active_task_count"], 0)

    def test_user_pause_keeps_hwm_and_queue_live_until_resume(self) -> None:
        store = self.arm()
        before = store.production_canary_status("math")
        paused = store.pause_production_canary("math")
        self.assertEqual(paused["state"], "paused_drained")
        self.assertFalse(paused["luna_consumer_enabled"])
        self.assertEqual(paused["blocking_reason"], "paused_by_user")
        for index in (1, 2):
            store.materialize_production_canary_task(
                canary_task(
                    index,
                    recorded_at=f"2026-08-11T00:00:0{index}Z",
                )
            )
        paused = store.production_canary_status("math")
        self.assertEqual(paused["queue_depth"], 2)
        self.assertEqual(paused["active_task_count"], 0)
        self.assertEqual(paused["activation_id"], before["activation_id"])
        self.assertEqual(
            paused["producer_high_watermark_sha256"],
            before["producer_high_watermark_sha256"],
        )
        resumed = store.resume_production_canary("math")
        self.assertEqual(resumed["state"], "armed")
        task = store.pending_production_canary_tasks("math")[0]
        self.assertEqual(self.dispatcher().submit(task).wait(5).outcome, "succeeded")

    def test_paused_daemon_materializes_only_and_never_submits_luna(self) -> None:
        store = self.arm()
        before = store.pause_production_canary("math")
        tasks = [
            canary_task(61, recorded_at="2026-08-11T00:01:01Z"),
            canary_task(62, recorded_at="2026-08-11T00:01:02Z"),
        ]

        class ConsumerMustRemainOff:
            def luna_admission(self, _subject):
                raise AssertionError("paused consumer must not admit Luna")

            def submit_luna_under_generation_fence(self, *_args, **_kwargs):
                raise AssertionError("paused consumer must not submit Luna")

        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.config = {}
        runtime.subject = "math"
        runtime.production_canary = True
        runtime.dispatcher = self.dispatcher()
        runtime.subject_sol = ConsumerMustRemainOff()
        decisions = [
            {
                "subject": "math",
                "capture_id": task.frozen_payload["capture_id"],
                "study_date": "2026-08-11",
                "unit_sha256": task.unit_sha256,
                "eligible": True,
                "model_enqueue_allowed": True,
            }
            for task in tasks
        ]
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(
                [SimpleNamespace(task=task) for task in tasks],
                decisions,
            ),
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            handles, projected = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        self.assertTrue(
            all(row["phase"] == "producer_queue_pending" for row in projected)
        )
        paused = store.production_canary_status("math")
        self.assertEqual(paused["state"], "paused_drained")
        self.assertEqual(paused["queue_depth"], 2)
        self.assertEqual(paused["active_task_count"], 0)
        self.assertEqual(store.subject_status("math")["claimed_total"], 0)
        self.assertEqual(paused["activation_id"], before["activation_id"])
        self.assertEqual(
            paused["producer_high_watermark_sha256"],
            before["producer_high_watermark_sha256"],
        )
        resumed = store.resume_production_canary("math")
        self.assertEqual(resumed["state"], "armed")
        first = store.pending_production_canary_tasks("math")[0]
        self.assertEqual(self.dispatcher().submit(first).wait(5).outcome, "succeeded")

    def test_replay_and_resigned_duplicate_are_rejected(self) -> None:
        store = self.arm()
        task = canary_task(1, capture_id="REPLAY-CAPTURE")
        store.materialize_production_canary_task(task)
        self.assertEqual(self.dispatcher().submit(task).wait(5).outcome, "succeeded")
        with self.assertRaisesRegex(
            DispatchError, "production_canary_task_replay"
        ):
            self.dispatcher().submit(task)
        resigned = canary_task(
            1,
            capture_id="REPLAY-CAPTURE",
            fingerprint_suffix="new-signature",
        )
        with self.assertRaisesRegex(
            DispatchError, "production_canary_producer_replay"
        ):
            store.materialize_production_canary_task(resigned)

    def test_terminal_receipt_binds_gate_and_processing_closure(self) -> None:
        store = self.arm()
        task = canary_task(1)
        store.materialize_production_canary_task(task)
        self.assertEqual(self.dispatcher().submit(task).wait(5).outcome, "succeeded")
        state = store.production_canary_status("math")
        path = Path(state["last_terminal_receipt_path"])
        receipt = json.loads(path.read_text(encoding="utf-8"))
        store._verify_seal(
            receipt, purpose="dispatch-production-canary-terminal"
        )
        self.assertEqual(receipt["activation_id"], state["activation_id"])
        self.assertEqual(
            receipt["producer_high_watermark_sha256"],
            state["producer_high_watermark_sha256"],
        )
        self.assertEqual(receipt["selected"]["unit_sha256"], task.unit_sha256)
        self.assertRegex(receipt["canary_gate_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            receipt["completion_receipt_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertRegex(receipt["package_sha256"], r"^[0-9a-f]{64}$")
        self.assertFalse(receipt["fast_mode_requested"])
        self.assertEqual(receipt["fast_mode_effective"], "not_requested")
        self.assertEqual(
            set(receipt["mcp_stage_grounding"]),
            {"analysis", "critical_review"},
        )
        self.assertEqual(
            receipt["mcp_stage_grounding"]["analysis"]["read_session_id"],
            receipt["mcp_stage_grounding"]["critical_review"][
                "read_session_id"
            ],
        )
        process_execution = receipt["process_execution"]
        self.assertRegex(
            process_execution["supervisor_process_identity_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertRegex(
            process_execution["supervisor_process_exit_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(
            process_execution["supervisor_pid"],
            process_execution["supervisor_pgid"],
        )
        self.assertTrue(process_execution["reaped"])
        self.assertTrue(process_execution["process_absent"])
        self.assertTrue(process_execution["pgid_absent"])

    def test_missing_or_cross_session_grounding_fails_closed(self) -> None:
        for index, runner_factory, expected in (
            (
                41,
                lambda _task, _context: MissingGroundingRunner(),
                "production_canary_mcp_grounding_missing",
            ),
            (
                42,
                lambda _task, _context: CrossSessionRunner(),
                "production_canary_mcp_session_cross_binding",
            ),
            (
                43,
                lambda _task, _context: MutatedCriticalBindingRunner(
                    "evidence_generation", "foreign-generation"
                ),
                "production_canary_mcp_session_cross_binding",
            ),
            (
                44,
                lambda _task, _context: MutatedCriticalBindingRunner(
                    "evidence_subject", "english"
                ),
                "production_canary_mcp_grounding_invalid",
            ),
            (
                45,
                lambda _task, _context: MutatedCriticalBindingRunner(
                    "evidence_release_id", "b" * 64
                ),
                "production_canary_mcp_grounding_invalid",
            ),
        ):
            with self.subTest(expected=expected):
                if index != 41:
                    self.temp.cleanup()
                    self.temp = tempfile.TemporaryDirectory()
                    self.runtime = Path(self.temp.name) / "runtime"
                store = self.arm()
                task = canary_task(index)
                store.materialize_production_canary_task(task)
                result = self.dispatcher(runner_factory).submit(task).wait(5)
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.error_code, expected)
                state = store.production_canary_status("math")
                self.assertEqual(state["state"], "failed_drained")
                self.assertEqual(state["active_task_count"], 0)

    def test_emergency_cancel_seals_terminal_and_late_result_fence(self) -> None:
        store = self.arm()
        task = canary_task(51)
        store.materialize_production_canary_task(task)
        entered = threading.Event()
        release = threading.Event()
        dispatcher = self.dispatcher(
            lambda _task, _context: BlockingGroundedRunner(entered, release)
        )
        handle = dispatcher.submit(task)
        self.assertTrue(entered.wait(2))
        shutdown = dispatcher.emergency_cancel(
            timeout=1.0, error_code="daemon_shutdown"
        )
        self.assertEqual(shutdown["active_count"], 0)
        self.assertEqual(shutdown["late_result_fence_status"], "sealed")
        result = handle.wait(2)
        self.assertEqual(result.outcome, "cancelled")
        self.assertEqual(result.error_code, "daemon_shutdown")
        self.assertIsNotNone(result.completion)
        self.assertIsNone(result.completion["package_sha256"])
        state = store.production_canary_status("math")
        terminal_sha256 = state["last_terminal_receipt_sha256"]
        self.assertEqual(state["active_task_count"], 0)
        self.assertEqual(store.subject_status("math")["claimed_total"], 0)
        self.assertEqual(state["late_result_fence_status"], "sealed")
        self.assertEqual(
            state["last_emergency_cancel_receipt_sha256"], terminal_sha256
        )
        terminal = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(terminal["terminal_kind"], "emergency_hard_cancel")
        self.assertTrue(terminal["late_result_fenced"])
        release.set()
        time.sleep(0.1)
        self.assertEqual(
            store.production_canary_status("math")[
                "last_terminal_receipt_sha256"
            ],
            terminal_sha256,
        )
        paused = store.pause_production_canary("math")
        self.assertEqual(paused["state"], "paused_drained")
        self.assertFalse(paused["luna_consumer_enabled"])

    def test_daemon_signal_path_uses_bounded_hard_cancel(self) -> None:
        store = self.arm()
        task = canary_task(52)
        store.materialize_production_canary_task(task)
        entered = threading.Event()
        release = threading.Event()
        dispatcher = self.dispatcher(
            lambda _task, _context: BlockingGroundedRunner(entered, release)
        )
        handle = dispatcher.submit(task)
        self.assertTrue(entered.wait(2))
        runtime = SimpleNamespace(
            dispatcher=dispatcher,
            production_canary=True,
        )
        requested_stop = False

        def signal_install(_signum, handler):
            nonlocal requested_stop
            if callable(handler) and not requested_stop:
                requested_stop = True
                handler(_signum, None)
            return 0

        with mock.patch(
            "preprocess_dispatcher.ProductionDispatchRuntime",
            return_value=runtime,
        ), mock.patch(
            "preprocess_dispatcher.signal.signal",
            side_effect=signal_install,
        ), mock.patch("preprocess_dispatcher._write_subject_projections"):
            self.assertEqual(_run_daemon({}, "math", Path("unused.json")), 0)
        result = handle.wait(2)
        self.assertEqual(result.outcome, "cancelled")
        self.assertEqual(result.error_code, "daemon_shutdown")
        state = store.production_canary_status("math")
        self.assertEqual(state["late_result_fence_status"], "sealed")
        self.assertEqual(state["active_task_count"], 0)
        self.assertEqual(store.subject_status("math")["claimed_total"], 0)
        release.set()

    def test_daemon_refreshes_running_heartbeat_during_blocked_first_scan(
        self,
    ) -> None:
        store = self.arm()
        runtime = self.scan_runtime(object())
        runtime.sync_subject_batch_task_progress = lambda: None
        entered = threading.Event()
        release_scan = threading.Event()
        installed_handlers: dict[int, object] = {}
        projections: list[dict[str, object]] = []

        def signal_install(signum, handler):
            previous = installed_handlers.get(signum, 0)
            if callable(handler):
                installed_handlers[signum] = handler
            return previous

        def blocked_scan():
            entered.set()
            self.assertTrue(release_scan.wait(2))
            return [], []

        def projection(*_args, **kwargs):
            projections.append(dict(kwargs))

        original_status = store.subject_status

        def short_heartbeat_status(subject):
            value = original_status(subject)
            value["heartbeat_interval_seconds"] = 0.02
            return value

        runtime.dispatcher.lease_store.subject_status = short_heartbeat_status
        runtime.scan_and_submit = blocked_scan
        result: list[int] = []
        failure: list[BaseException] = []

        def run() -> None:
            try:
                result.append(_run_daemon({}, "math", Path("unused.json")))
            except BaseException as exc:
                failure.append(exc)

        with mock.patch(
            "preprocess_dispatcher.ProductionDispatchRuntime",
            return_value=runtime,
        ), mock.patch(
            "preprocess_dispatcher.signal.signal",
            side_effect=signal_install,
        ), mock.patch(
            "preprocess_dispatcher._write_subject_projections",
            side_effect=projection,
        ), mock.patch("preprocess_dispatcher._poll_interval", return_value=0.01):
            thread = threading.Thread(target=run)
            thread.start()
            self.assertTrue(entered.wait(1))
            deadline = time.monotonic() + 1
            while (
                sum(
                    row.get("daemon_status") == "running"
                    for row in projections
                )
                < 3
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            self.assertGreaterEqual(
                sum(
                    row.get("daemon_status") == "running"
                    for row in projections
                ),
                3,
            )
            installed_handlers[signal.SIGTERM](signal.SIGTERM, None)
            release_scan.set()
            thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(failure, [])
        self.assertEqual(result, [0])
        state = store.production_canary_status("math")
        self.assertEqual(state["model_call_count"], 0)
        self.assertEqual(state["provider_request_count"], 0)
        self.assertEqual(state["formal_write_count"], 0)
        self.assertFalse(state["sol_enabled"])

    def test_post_terminal_projection_failure_repauses_subject(self) -> None:
        store = self.arm()
        task = canary_task(1)
        store.materialize_production_canary_task(task)
        dispatcher = self.dispatcher()
        handle = dispatcher.submit(task)
        self.assertEqual(handle.wait(5).outcome, "succeeded")

        class ProjectionFailure:
            def record_verified_luna_completion(self, _subject, _verified):
                raise SubjectSolContractError("synthetic_projection_failure")

        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "math"
        runtime.dispatcher = dispatcher
        runtime.subject_sol = ProjectionFailure()
        runtime._frozen_batch_authority = None
        projection = runtime.persist_finished_luna_with_status([handle])
        self.assertEqual(projection["handled_units"], [task.unit_sha256])
        self.assertEqual(
            projection["failures"][0]["error_code"],
            "synthetic_projection_failure",
        )
        self.assertTrue(
            projection["failures"][0]["durable_failure_receipt"]
        )
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        receipt = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(
            receipt["terminal_kind"], "post_terminal_projection_failure"
        )
        self.assertRegex(
            receipt["prior_terminal_receipt_sha256"], r"^[0-9a-f]{64}$"
        )

    def test_unexpected_projection_failure_does_not_abandon_19_siblings(
        self,
    ) -> None:
        store = self.arm()
        first = canary_task(200)
        store.materialize_production_canary_task(first)
        first_handle = self.dispatcher().submit(first)
        self.assertEqual(first_handle.wait(5).outcome, "succeeded")
        tasks = [
            canary_task(
                201 + index,
                recorded_at=f"2026-08-11T00:01:{index:02d}Z",
            )
            for index in range(20)
        ]
        for task in tasks:
            store.materialize_production_canary_task(task)
        dispatcher = self.dispatcher()
        handles = [dispatcher.submit(task) for task in tasks]
        self.assertTrue(all(handle.wait(5).outcome == "succeeded" for handle in handles))

        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "math"
        runtime.dispatcher = dispatcher
        runtime._projection_fail_closed = False
        call_count = 0

        def project_one(rows):
            nonlocal call_count
            call_count += 1
            handle = list(rows)[0]
            if call_count == 1:
                raise ValueError("synthetic unexpected projection failure")
            return [{"unit_sha256": handle.unit_sha256, "projected": True}]

        with mock.patch.object(
            runtime, "_persist_finished_luna_unchecked", side_effect=project_one
        ):
            projection = runtime.persist_finished_luna_with_status(handles)
        self.assertEqual(call_count, 20)
        self.assertEqual(len(projection["handled_units"]), 20)
        self.assertEqual(len(projection["projected"]), 19)
        self.assertEqual(len(projection["failures"]), 1)
        self.assertEqual(
            projection["failures"][0]["error_code"],
            "production_canary_projection_unexpected",
        )
        self.assertTrue(
            projection["failures"][0]["durable_failure_receipt"]
        )
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertFalse(state["luna_consumer_enabled"])
        self.assertEqual(state["active_task_count"], 0)
        self.assertEqual(state["terminal_task_count"], 21)
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
        terminal_index = json.loads(
            Path(state["terminal_index_path"]).read_text(encoding="utf-8")
        )
        failed_entry = terminal_index["units"][tasks[0].unit_sha256]
        self.assertEqual(failed_entry["outcome"], "failed")
        self.assertEqual(len(failed_entry["history"]), 2)
        telemetry = store.production_canary_concurrency_telemetry(
            release_id=self.release_id
        )
        self.assertEqual(telemetry["terminal_task_count_global"], 21)
        self.assertEqual(
            telemetry["terminal_by_outcome_global"],
            state["terminal_by_outcome"],
        )
        public_status = store.subject_status("math")
        failure_bindings = public_status["terminal_failure_bindings"]
        self.assertEqual(len(failure_bindings), 1)
        self.assertEqual(failure_bindings[0]["unit_sha256"], tasks[0].unit_sha256)
        self.assertEqual(failure_bindings[0]["outcome"], "failed")
        self.assertRegex(
            failure_bindings[0]["terminal_receipt_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertNotIn("terminal_receipt_path", failure_bindings[0])

    def test_runner_peak_uses_half_open_intervals_without_tie_inflation(
        self,
    ) -> None:
        t0 = dt.datetime(2026, 8, 11, tzinfo=dt.timezone.utc)
        t1 = t0 + dt.timedelta(seconds=1)
        t2 = t1 + dt.timedelta(seconds=1)
        peak, zero_count = LeaseStore._half_open_interval_peak(
            [
                (t0, t1, "a" * 64),
                (t1, t2, "b" * 64),
                (t1, t1, "c" * 64),
            ]
        )
        self.assertEqual(peak, 1)
        self.assertEqual(zero_count, 1)

    def test_terminal_verifier_failure_forces_failed_drained(self) -> None:
        store = self.arm()
        task = canary_task(1)
        store.materialize_production_canary_task(task)
        dispatcher = self.dispatcher()
        with mock.patch.object(
            dispatcher.lease_store,
            "finish_production_canary_task",
            side_effect=DispatchError("synthetic_terminal_verifier_failure"),
        ):
            result = dispatcher.submit(task).wait(5)
        self.assertEqual(result.outcome, "failed")
        state = store.production_canary_status("math")
        self.assertEqual(state["state"], "failed_drained")
        self.assertEqual(state["active_task_count"], 0)
        receipt = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["terminal_kind"], "fail_closed_recovery")

    def test_subjects_activate_and_fail_independently(self) -> None:
        stores = {subject: self.arm(subject) for subject in ("math", "cs408", "english")}
        tasks = {
            subject: canary_task(index, subject=subject)
            for index, subject in enumerate(("math", "cs408", "english"), start=1)
        }
        for subject, task in tasks.items():
            stores[subject].materialize_production_canary_task(task)
        failed = self.dispatcher(
            lambda task, _context: (
                FailedRunner()
                if task.frozen_payload["subject"] == "math"
                else GroundedRunner()
            )
        ).submit(tasks["math"]).wait(5)
        self.assertEqual(failed.outcome, "failed")
        for subject in ("cs408", "english"):
            result = self.dispatcher().submit(tasks[subject]).wait(5)
            self.assertEqual(result.outcome, "succeeded")
        self.assertEqual(stores["math"].production_canary_status("math")["state"], "failed_drained")
        for subject in ("cs408", "english"):
            self.assertEqual(
                stores[subject].production_canary_status(subject)["state"],
                "continuous_concurrent_unlocked",
            )

    def test_activation_rollback_preserves_queue_and_receipts(self) -> None:
        store = self.arm()
        task = canary_task(1)
        entry = store.materialize_production_canary_task(task)
        task_path = Path(entry["task_object_path"])
        rolled_back = store.deactivate_production_canary(
            "math", expected_release_id=self.release_id
        )
        self.assertEqual(rolled_back["status"], "production_canary_inactive")
        self.assertEqual(rolled_back["state"], "inactive_rolled_back")
        self.assertTrue(task_path.is_file())
        self.assertEqual(store.production_canary_status("math")["queue_depth"], 1)


if __name__ == "__main__":
    unittest.main()
