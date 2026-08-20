#!/usr/bin/env python3

from __future__ import annotations

import copy
import datetime as dt
import errno
import hashlib
import inspect
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    HEARTBEAT_INTERVAL_SECONDS,
    InfrastructureCrash,
    LEASE_TTL_SECONDS,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
    TaskExecutionContext,
    dispatch_rule_binding,
)
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateRunner,
    CoreCandidateSubprocessRunner,
    _producer_authority_from_processing_contract,
    scan_eligible_candidates,
)
from preprocessor_core import (  # noqa: E402
    Candidate,
    ModelResult,
    PreprocessorError,
    RELEASE_SCHEMA,
    atomic_restore_json_if_current,
    atomic_write_json,
    canonical_bytes,
    Worker,
)
from processing_plugin import ProcessingPluginHost  # noqa: E402
from preprocess_dispatcher import (  # noqa: E402
    MAX_DISCOVERY_LATENCY_SECONDS,
    ProductionDispatchRuntime,
    _audit,
    _poll_interval,
)
from fixtures.scanner_worker_fake import (  # noqa: E402
    ScannerWorkerFake,
    make_scanner_worker_factory,
)
from fixtures.process_lifecycle import (  # noqa: E402
    process_absent,
    register_process,
    stop_process,
)


def frozen_task(index: int, *, subject: str = "math") -> FrozenTask:
    release_id = "a" * 64
    return FrozenTask(
        {
            "subject": subject,
            "capture_id": f"CAP-{index:04d}",
            "study_date": "2026-08-05",
            "input_fingerprint": f"fingerprint-{index:04d}",
            "input_binding": {"index": index},
            "model_input": {"question": f"question-{index}"},
            "allowed_evidence_refs": [f"capture:{index}"],
            "image_paths": [],
            "dispatch_contract": {
                "schema_version": "study-intake-dispatch-release-binding-v1",
                **dispatch_rule_binding(
                    release_id=release_id,
                    subject=subject,
                    subject_processing_contract_sha256=None,
                ),
            },
        }
    )


def stage_runtime_profile() -> dict[str, int]:
    return {
        "soft_runtime_warning_seconds": 60,
        "stall_timeout_seconds": 60,
        "stall_probe_interval_seconds": 1,
        "stall_probe_required_consecutive_failures": 2,
    }


def stage_result(stage: str, task: FrozenTask) -> StageResult:
    return StageResult(
        payload={"stage": stage, "unit_sha256": task.unit_sha256},
        runtime_model=REQUIRED_MODEL,
        runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
        runtime_metadata_provenance="codex_json_attestation_v1",
        runtime_identity_status="confirmed",
        duration_ms=1,
    )


def core_candidate(index: int, *, subject: str = "cs408") -> Candidate:
    event_time = "2026-08-05T00:00:00Z"
    trace_events = [
        {
            "ordinal": 1,
            "role": "learner",
            "kind": "answer",
            "text": f"独立作答 {index}",
        }
    ]
    evidence = {
        "schema_version": "current-question-evidence-bundle-v3",
        "question_mode": "dialogue_only",
        "context_id": f"CTX-{index}",
        "request_id": f"REQ-{index}",
        "session_id": f"SESSION-{index}",
        "item_id": f"ITEM-{index}",
        "source_id": f"SOURCE-{index}",
        "source_kind": "current_question",
        "study_date": "2026-08-05",
        "event_time": event_time,
        "timezone": "Asia/Shanghai",
        "source_binding_sha256": "1" * 64,
        "current_question": {
            "public_text": f"408 question {index}",
            "options": [],
            "response_instruction": "说明判断。",
            "public_surface_sha256": "2" * 64,
            "attachment_sha256s": [],
        },
        "learner_evidence": {
            "answer_text": "独立作答",
            "choice": None,
            "confidence": "high",
            "first_action": "检查条件",
            "reasoning": "根据定义判断",
            "prompt_level": "L0",
            "observed_at": event_time,
        },
        "evaluation_evidence": {
            "grader_capsule_id": f"GRADE-{index}",
            "correct_answer": "受保护答案",
            "correct_option": None,
            "standard_explanation": "受保护解析",
            "grader_result": "independent_correct",
            "grader_basis": "当前题证据",
            "provided_by": "current_question_grader",
            "missing_fields": [],
        },
        "assessment": {
            "first_result": "independent_correct",
            "choice_result": None,
            "reasoning_result": "correct",
            "confidence": "high",
            "prompt_level": "L0",
            "first_break": None,
            "first_break_provenance": "not_applicable",
            "first_action": "检查条件",
            "first_action_provenance": "observed",
        },
        "frozen_assistant_feedback": {
            "text": "只读反馈",
            "sha256": hashlib.sha256("只读反馈".encode()).hexdigest(),
        },
        "provenance": {
            "source_locator": f"current-question-capsule:{index}",
            "source_sha256": "3" * 64,
            "context_sha256": "4" * 64,
            "attachment_provenance": [],
            "created_at": event_time,
            "missing_items": [],
        },
        "missing_fields": [],
        "attachment_objects": [],
        "interaction_trace": {
            "schema": "current-question-interaction-trace-v2",
            "events": trace_events,
            "original_event_count": 1,
            "included_event_count": 1,
            "omitted_event_count": 0,
            "omitted_ranges": [],
            "truncation_reason": None,
            "full_trace_sha256": hashlib.sha256(
                canonical_bytes(trace_events)
            ).hexdigest(),
        },
        "formal_write_count": 0,
    }
    input_binding = {
        "index": index,
        "processing_contract_sha256": "9" * 64,
        "evidence_status": "ready",
    }
    if subject == "math":
        input_binding["evidence_bundle_sha256"] = "8" * 64
        math_contract = {
            "schema_version": "study-intake-math-capture-evidence-contract-v1",
            "capture_type": "fact_observation",
            "stable_formal_id": None,
            "source_fingerprint": hashlib.sha256(
                f"source-{index}".encode()
            ).hexdigest(),
            "content_fingerprint": hashlib.sha256(
                f"content-{index}".encode()
            ).hexdigest(),
            "verbatim_learning_record_sha256": hashlib.sha256(
                f"learning-{index}".encode()
            ).hexdigest(),
            "full_dialogue_sha256": hashlib.sha256(
                f"dialogue-{index}".encode()
            ).hexdigest(),
            "question_image_sha256s": [],
            "solution_image_sha256s": [],
            "canonical_solution_text_sha256": None,
            "evidence_status": "ready",
            "missing_roles": [],
            "formal_write_count": 0,
        }
        input_binding.update(
            {
                "capture_type": "fact_observation",
                "math_evidence_contract": math_contract,
                "math_evidence_contract_sha256": hashlib.sha256(
                    json.dumps(
                        math_contract,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            }
        )
    model_input = {
        "input": index,
        "current_question_evidence": evidence,
    }
    if subject == "math":
        # Math's image evidence boundary requires an explicit artifact list
        # even when the candidate has no images.  Empty image_paths therefore
        # binds to an empty provided-to-model artifact sequence.
        model_input["source_bundle"] = {"artifacts": []}
    return Candidate(
        subject=subject,
        capture_id=f"CORE-{index:04d}",
        study_date="2026-08-05",
        recorded_at="2026-08-05T00:00:00Z",
        input_fingerprint=hashlib.sha256(
            f"core-fingerprint-{subject}-{index}".encode()
        ).hexdigest(),
        input_binding=input_binding,
        model_input=model_input,
        allowed_evidence_refs=(f"capture:{index}",),
        image_paths=(),
        target_label=f"target-{index}",
        canonical_state="awaiting_daily_curation",
        sol_state="pending_review",
    )


class BlockingCoordinator:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.release = threading.Event()
        self.condition = threading.Condition()
        self.analysis_entered: list[str] = []
        self.critical_entered: list[str] = []
        self.calls: dict[str, dict[str, int]] = {}
        self.context_roots: set[Path] = set()

    def enter_analysis(self, task: FrozenTask, root: Path) -> None:
        with self.condition:
            unit = task.unit_sha256
            self.analysis_entered.append(unit)
            self.context_roots.add(root)
            self.calls.setdefault(unit, {"analysis": 0, "critical_review": 0})[
                "analysis"
            ] += 1
            self.condition.notify_all()

    def enter_critical(self, task: FrozenTask) -> None:
        with self.condition:
            unit = task.unit_sha256
            self.critical_entered.append(unit)
            self.calls.setdefault(unit, {"analysis": 0, "critical_review": 0})[
                "critical_review"
            ] += 1
            self.condition.notify_all()

    def wait_all_entered(self, timeout: float = 5) -> bool:
        deadline = time.monotonic() + timeout
        with self.condition:
            while len(self.analysis_entered) < self.expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
            return True


class BlockingFakeRunner:
    """Shared gate proves every task starts before any task is released."""

    def __init__(self, coordinator: BlockingCoordinator) -> None:
        self.coordinator = coordinator

    def run_analysis(self, task, context):
        self.coordinator.enter_analysis(task, context.root)
        self.coordinator.release.wait(10)
        return stage_result("analysis", task)

    def run_critical_review(self, task, draft_analysis, context):
        if draft_analysis.get("stage") != "analysis":
            raise AssertionError("critical review did not receive analysis draft")
        self.coordinator.enter_critical(task)
        return stage_result("critical_review", task)


class BehaviorRunner:
    def __init__(self, behavior: str, gate: threading.Event | None = None) -> None:
        self.behavior = behavior
        self.gate = gate
        self.cancel_called = threading.Event()

    def run_analysis(self, task, context):
        if self.behavior == "crash":
            raise RuntimeError("synthetic crash")
        if self.behavior == "block":
            assert self.gate is not None
            self.gate.wait(10)
        return stage_result("analysis", task)

    def run_critical_review(self, task, draft_analysis, context):
        return stage_result("critical_review", task)

    def cancel(self, _context):
        self.cancel_called.set()


class ConcurrentDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _assert_unbounded_batch(self, count: int, subject: str) -> None:
        coordinator = BlockingCoordinator(count)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BlockingFakeRunner(coordinator),
            stage_timeout_seconds=5,
        )
        handles = dispatcher.dispatch(
            [frozen_task(index, subject=subject) for index in range(count)],
            wait=False,
        )
        self.assertTrue(coordinator.wait_all_entered())
        self.assertEqual(dispatcher.active_count, count)
        self.assertEqual(len(coordinator.context_roots), count)
        coordinator.release.set()
        results = [handle.wait(5) for handle in handles]
        self.assertEqual([result.outcome for result in results], ["succeeded"] * count)
        self.assertTrue(dispatcher.drain(5))
        self.assertEqual(len(coordinator.critical_entered), count)

    def test_ten_unique_tasks_all_enter_before_release(self) -> None:
        self._assert_unbounded_batch(10, "mixed")

    def test_twenty_unique_tasks_all_enter_before_release(self) -> None:
        self._assert_unbounded_batch(20, "mixed")

    def test_twenty_same_subject_tasks_have_no_subject_cap(self) -> None:
        self._assert_unbounded_batch(20, "cs408")

    def _scan_distinct_tasks(
        self,
        *,
        subject: str,
        count: int,
        shared_semantic_input: bool,
    ) -> list[FrozenTask]:
        candidates = [
            core_candidate(10_000 + index, subject=subject)
            for index in range(count)
        ]
        if shared_semantic_input:
            shared = candidates[0]
            candidates = [
                Candidate(
                    **{
                        **candidate.__dict__,
                        "model_input": copy.deepcopy(shared.model_input),
                        "allowed_evidence_refs": shared.allowed_evidence_refs,
                    }
                )
                for candidate in candidates
            ]

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                pass

            def eligible_candidates(self, requested_subject, _date, **_kwargs):
                if requested_subject != subject:
                    raise AssertionError("scanner crossed subject boundary")
                return [(candidate, "eligible") for candidate in candidates]

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        frozen, decisions = scan_eligible_candidates(
            config, subject, worker_factory=ScanWorker
        )
        self.assertEqual(len(frozen), count, decisions)
        self.assertEqual(len(decisions), count)
        self.assertEqual(len({row.task.unit_sha256 for row in frozen}), count)
        self.assertEqual(
            len({row.task.frozen_payload_sha256 for row in frozen}), count
        )
        self.assertTrue(
            all(row["model_enqueue_allowed"] for row in decisions)
        )
        self.assertTrue(
            all(not row["reuses_group_owner"] for row in decisions)
        )
        self.assertTrue(
            all(
                row["group_owner_capture_id"] == row["capture_id"]
                for row in decisions
            )
        )
        return [row.task for row in frozen]

    def _assert_distinct_units_start_distinct_processes(
        self, tasks: list[FrozenTask]
    ) -> None:
        expected = len(tasks)
        condition = threading.Condition()
        release = threading.Event()
        processes: list[subprocess.Popen[bytes]] = []
        session_contexts: dict[str, Mapping[str, Any]] = {}
        session_paths: dict[str, Path] = {}
        model_mcp_entries: list[tuple[str, str]] = []
        mcp_active = 0
        mcp_peak = 0
        runner_errors: list[str] = []
        key_path = self.runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(b"k" * 32)
        key_path.chmod(0o600)
        component_lock = json.loads(
            (
                ROOT / "plugin/kaoyan-study-intake/component-lock.json"
            ).read_text(encoding="utf-8")
        )
        mcp_release_id = component_lock["mcp_release_id"]
        server_release = component_lock["mcp_server_release"]
        processing_config = {
            "enabled": True,
            "root": str(ROOT / "plugin/kaoyan-study-intake"),
            "component_lock_path": str(
                ROOT / "plugin/kaoyan-study-intake/component-lock.json"
            ),
            "mcp_client_python": str(
                Path("/Users/xiazhibin/Documents/Codex/local-study-read-mcp")
                / ".venv/bin/python"
            ),
            "mcp_project_root": str(
                Path("/Users/xiazhibin/.codex/local-study-read-mcp/releases")
                / mcp_release_id
            ),
            "authority_key_path": str(key_path),
            "profile": "background",
            "timeout_seconds": 5,
        }
        math_question_raw = bytes.fromhex(
            "89504e470d0a1a0a0000000d494844520000000100000001"
            "08060000001f15c4890000000d4944415408d763f8cfc0f0"
            "1f00050001ff89993d1d0000000049454e44ae426082"
        )
        math_question_path = self.runtime / "math-concurrency-question.png"
        math_solution_path = self.runtime / "math-concurrency-solution.md"
        math_dialogue_path = self.runtime / "math-concurrency-dialogue.json"
        math_learning_record_path = (
            self.runtime / "math-concurrency-learning-record.json"
        )
        math_question_path.write_bytes(math_question_raw)
        math_solution_path.write_text(
            "# 解析\n\n并发会话使用的文字解析证据。\n", encoding="utf-8"
        )
        math_dialogue_path.write_text(
            json.dumps(
                {
                    "schema_version": "test-math-dialogue.v1",
                    "turns": [
                        {"speaker": "user", "text": "并发任务真实作答。"},
                        {"speaker": "assistant", "text": "并发任务反馈。"},
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        math_learning_record_path.write_text(
            json.dumps(
                {
                    "schema_version": "test-math-learning-record.v1",
                    "task_kind": "formal_problem",
                    "result": "captured_for_parallel_preprocessing",
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        math_capture_artifacts = (
            {
                "artifact_id": "dialogue",
                "artifact_kind": "dialogue",
                "path": str(math_dialogue_path),
                "sha256": hashlib.sha256(
                    math_dialogue_path.read_bytes()
                ).hexdigest(),
            },
            {
                "artifact_id": "learning-record",
                "artifact_kind": "learning_record",
                "path": str(math_learning_record_path),
                "sha256": hashlib.sha256(
                    math_learning_record_path.read_bytes()
                ).hexdigest(),
            },
            {
                "artifact_id": "question-image",
                "artifact_kind": "question_image",
                "path": str(math_question_path),
                "sha256": hashlib.sha256(math_question_raw).hexdigest(),
            },
            {
                "artifact_id": "solution-text",
                "artifact_kind": "solution_text",
                "path": str(math_solution_path),
                "sha256": hashlib.sha256(
                    math_solution_path.read_bytes()
                ).hexdigest(),
            },
        )

        def fake_host_call(call_subject, tool, arguments):
            nonlocal mcp_active, mcp_peak
            route = dict(arguments["route"])
            if tool == "authority_bundle":
                return {
                    "ok": True,
                    "schema_version": "study-read-mcp.v3",
                    "profile": "background",
                    "read_route": route,
                    "generation": "generation-concurrency-test",
                    "authority_fingerprint": "b" * 64,
                    "server_release": server_release,
                    "preprocessor_release": "c" * 64,
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "items": [{
                        "subject": call_subject,
                        "available": True,
                        "generation": "generation-concurrency-test",
                        "authority_fingerprint": "b" * 64,
                        "adapter_release": server_release,
                    }],
                }
            if str(route.get("route_request_id") or "").startswith(
                "model-mcp-"
            ):
                read_session = dict(arguments["read_session"])
                with condition:
                    mcp_active += 1
                    mcp_peak = max(mcp_peak, mcp_active)
                    model_mcp_entries.append(
                        (call_subject, str(read_session["read_session_id"]))
                    )
                    condition.notify_all()
                try:
                    if not release.wait(10):
                        raise DispatchError("mcp_tool_barrier_timeout")
                finally:
                    with condition:
                        mcp_active -= 1
                        condition.notify_all()
                return {
                    "ok": True,
                    "schema_version": "study-read-mcp.v3",
                    "profile": "luna",
                    "subject": call_subject,
                    "read_route": route,
                    "generation": "generation-concurrency-test",
                    "authority_fingerprint": "b" * 64,
                    "adapter_release": server_release,
                    "server_release": server_release,
                    "read_session": read_session,
                    "total_count": 0,
                    "returned_count": 0,
                    "offset": 0,
                    "next_cursor": None,
                    "truncated": False,
                    "complete": True,
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "mcp_tool_call_count": 1,
                    "items": [],
                }
            return {
                "ok": True,
                "schema_version": "study-read-mcp.v3",
                "profile": "background",
                "subject": call_subject,
                "read_route": route,
                "generation": "generation-concurrency-test",
                "authority_fingerprint": "b" * 64,
                "adapter_release": server_release,
                "server_release": server_release,
                "formal_write_count": 0,
                "model_call_count": 0,
                "items": [{
                    "operation": "curation_inventory",
                    "data_role": "projection",
                    "projection_event_binding": {"event_count": 1},
                    "items": [],
                }],
            }

        class ProcessBackedRunner:
            def __init__(self):
                self.host = ProcessingPluginHost(
                    processing_config,
                    runtime_root=self_runtime,
                    candidate_release_id="a" * 64,
                )

            def run_analysis(self, task, _context):
                payload = task.frozen_payload
                try:
                    with mock.patch.object(
                        self.host, "_call", side_effect=fake_host_call
                    ):
                        session_context = self.host.open_read_session(
                            subject=str(payload["subject"]),
                            capture_id=str(payload["capture_id"]),
                            study_date=str(payload["study_date"]),
                            input_fingerprint=str(payload["input_fingerprint"]),
                            input_binding=dict(payload["input_binding"]),
                            capture_facts_sha256=hashlib.sha256(
                                (
                                    json.dumps(
                                        payload["model_input"],
                                        ensure_ascii=False,
                                        sort_keys=True,
                                        separators=(",", ":"),
                                    )
                                    + "\n"
                                ).encode("utf-8")
                            ).hexdigest(),
                            capture_facts=dict(payload["model_input"]),
                            capture_scene=(
                                "intensive_reading"
                                if payload["subject"] == "english"
                                else "morning_review"
                                if payload["subject"] == "cs408"
                                else "formal_problem"
                            ),
                            capture_identity={
                                "content_fingerprint": str(
                                    payload["input_fingerprint"]
                                )
                            },
                            capture_artifacts=(
                                math_capture_artifacts
                                if payload["subject"] == "math"
                                else ()
                            ),
                            captured_at=str(payload["recorded_at"]),
                            provider_schema_sha256="2" * 64,
                            canonical_schema_sha256="3" * 64,
                            validator_sha256="4" * 64,
                        )
                        session = session_context["mcp_read_session"]
                        process = subprocess.Popen(
                            [sys.executable, "-c", "import time; time.sleep(30)"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                        with condition:
                            processes.append(process)
                            session_contexts[task.unit_sha256] = session_context
                            session_paths[task.unit_sha256] = (
                                self.host.read_session_manifest_path(
                                    session_context
                                )
                            )
                            condition.notify_all()
                        model_route = {
                            "caller_skill_id": session["skill_id"],
                            "caller_skill_version": session["skill_version"],
                            "plugin_version": session["plugin_version"],
                            "route_request_id": (
                                "model-mcp-" + session["read_session_id"]
                            ),
                            "evidence_scope_hash": task.unit_sha256,
                            "read_route": "mcp",
                            "chunk_index": 1,
                            "chunk_count": 1,
                            "consumed_duplicate_read_count": 0,
                        }
                        model_tool = {
                            "math": "math_investigate_library",
                            "cs408": "cs408_investigate_library",
                            "english": "english_investigate_library",
                        }[str(payload["subject"])]
                        model_result = self.host._call(
                            str(payload["subject"]),
                            model_tool,
                            {
                                "collection": "catalog",
                                "cursor": None,
                                "page_size": 1,
                                "route": model_route,
                                "read_session": {
                                    "read_session_id": session["read_session_id"],
                                    "manifest_sha256": session["manifest_sha256"],
                                },
                            },
                        )
                        if (
                            model_result.get("profile") != "luna"
                            or model_result.get("subject")
                            != str(payload["subject"])
                            or model_result.get("generation")
                            != session["generation"]
                            or model_result.get("authority_fingerprint")
                            != session["authority_fingerprint"]
                            or model_result.get("read_session")
                            != {
                                "read_session_id": session["read_session_id"],
                                "manifest_sha256": session["manifest_sha256"],
                            }
                            or model_result.get("formal_write_count") != 0
                            or model_result.get("mcp_tool_call_count") != 1
                        ):
                            raise DispatchError(
                                "model_mcp_barrier_envelope_invalid"
                            )
                except Exception as exc:
                    with condition:
                        runner_errors.append(f"{type(exc).__name__}: {exc}")
                        condition.notify_all()
                    raise
                process.terminate()
                process.wait(timeout=5)
                read_session_id = session_context["mcp_read_session"][
                    "read_session_id"
                ]
                return StageResult(
                    payload={"stage": "analysis", "unit_sha256": task.unit_sha256},
                    runtime_model=None,
                    runtime_reasoning_effort=None,
                    runtime_metadata_provenance="unavailable",
                    runtime_identity_status="requested_unverified",
                    duration_ms=1,
                    read_session_id=read_session_id,
                    semantic_stage_count=1,
                    provider_request_count=2,
                    mcp_tool_call_count=1,
                    model_call_count=1,
                    consumed_terminal_duplicate_read_count=0,
                )

            def run_critical_review(self, task, _context, _draft):
                read_session_id = session_contexts[task.unit_sha256][
                    "mcp_read_session"
                ]["read_session_id"]
                return StageResult(
                    payload={
                        "stage": "critical_review",
                        "unit_sha256": task.unit_sha256,
                    },
                    runtime_model=None,
                    runtime_reasoning_effort=None,
                    runtime_metadata_provenance="unavailable",
                    runtime_identity_status="requested_unverified",
                    duration_ms=1,
                    read_session_id=read_session_id,
                    semantic_stage_count=1,
                    provider_request_count=2,
                    mcp_tool_call_count=1,
                    model_call_count=1,
                    consumed_terminal_duplicate_read_count=0,
                )

            def cancel(self, _context):
                return None

        self_runtime = self.runtime
        tasks_by_subject: dict[str, list[FrozenTask]] = {}
        for task in tasks:
            tasks_by_subject.setdefault(
                str(task.frozen_payload["subject"]), []
            ).append(task)
        dispatchers = {
            subject: ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: ProcessBackedRunner(),
                stage_timeout_seconds=15,
            )
            for subject in tasks_by_subject
        }
        handles = [
            handle
            for subject, subject_tasks in tasks_by_subject.items()
            for handle in dispatchers[subject].dispatch(
                subject_tasks, wait=False
            )
        ]
        try:
            with condition:
                reached = condition.wait_for(
                    lambda: len(model_mcp_entries) == expected, timeout=10
                )
            if not reached:
                diagnostics = [
                    {
                        "status": result.status,
                        "outcome": result.outcome,
                        "error_code": result.error_code,
                    }
                    for handle in handles
                    for result in [handle.wait(1)]
                ]
                self.fail(
                    "model MCP/session barrier not reached: "
                    f"errors={runner_errors} results={diagnostics}"
                )
            self.assertEqual(expected, mcp_active)
            self.assertEqual(expected, mcp_peak)
            self.assertEqual(expected, len(set(model_mcp_entries)))
            self.assertEqual(
                expected,
                sum(dispatcher.active_count for dispatcher in dispatchers.values()),
            )
            self.assertEqual(
                {
                    subject: len(subject_tasks)
                    for subject, subject_tasks in tasks_by_subject.items()
                },
                {
                    subject: dispatchers[subject].active_count
                    for subject in tasks_by_subject
                },
            )
            lease_store = LeaseStore(self.runtime)
            for subject, subject_tasks in tasks_by_subject.items():
                status = lease_store.subject_status(subject)
                self.assertEqual(status["active_count"], len(subject_tasks))
                self.assertFalse(status["draining"])
            self.assertEqual(expected, len({process.pid for process in processes}))
            self.assertTrue(all(process.poll() is None for process in processes))
            self.assertEqual(len(session_contexts), expected)
            self.assertEqual(
                len({
                    context["mcp_read_session"]["read_session_id"]
                    for context in session_contexts.values()
                }),
                expected,
            )
            self.assertEqual(
                len({
                    context["mcp_read_session"]["manifest_sha256"]
                    for context in session_contexts.values()
                }),
                expected,
            )
            receipt_hashes = {
                context["mcp_read_session_receipt_sha256"]
                for context in session_contexts.values()
            }
            self.assertEqual(len(receipt_hashes), expected)
            self.assertTrue(
                all(
                    (
                        self.runtime
                        / "dispatch/mcp-read-session-receipts/sha256"
                        / digest[:2]
                        / f"{digest}.json"
                    ).is_file()
                    for digest in receipt_hashes
                )
            )
            self.assertTrue(
                all(
                    context["processing_binding"]["host_semantic_prefetch"]
                    is False
                    and context["formal_write_count"] == 0
                    for context in session_contexts.values()
                )
            )
            self.assertEqual(
                {
                    subject: len(subject_tasks)
                    for subject, subject_tasks in tasks_by_subject.items()
                },
                {
                    subject: sum(
                        context["mcp_read_session"]["subject"] == subject
                        for context in session_contexts.values()
                    )
                    for subject in tasks_by_subject
                },
            )
            self.assertTrue(all(path.is_file() for path in session_paths.values()))
            release.set()
            results = [handle.wait(15) for handle in handles]
            self.assertEqual(["succeeded"] * expected, [r.outcome for r in results])
            self.assertTrue(
                all(dispatcher.drain(5) for dispatcher in dispatchers.values())
            )
        finally:
            release.set()
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)

    def test_scanner_starts_ten_distinct_math_units_and_read_sessions(self) -> None:
        self._assert_distinct_units_start_distinct_processes(
            self._scan_distinct_tasks(
                subject="math", count=10, shared_semantic_input=True
            )
        )

    def test_scanner_starts_thirty_three_subject_units_and_read_sessions(
        self,
    ) -> None:
        tasks = []
        for subject in ("math", "cs408", "english"):
            tasks.extend(
                self._scan_distinct_tasks(
                    subject=subject,
                    count=10,
                    shared_semantic_input=True,
                )
            )
        self._assert_distinct_units_start_distinct_processes(tasks)

    def test_identical_unit_is_executed_once(self) -> None:
        coordinator = BlockingCoordinator(1)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BlockingFakeRunner(coordinator),
            stage_timeout_seconds=5,
        )
        task = frozen_task(1)
        first = dispatcher.submit(task)
        second = dispatcher.submit(FrozenTask(dict(task.frozen_payload)))
        self.assertIs(first, second)
        self.assertTrue(coordinator.wait_all_entered())
        coordinator.release.set()
        self.assertEqual(first.wait(5).outcome, "succeeded")
        self.assertEqual(coordinator.calls[task.unit_sha256]["analysis"], 1)
        third = dispatcher.submit(task)
        self.assertEqual(third.wait(1).status, "deduplicated")
        self.assertEqual(coordinator.calls[task.unit_sha256]["critical_review"], 1)

    def test_exact_duplicate_scanned_unit_is_executed_once(self) -> None:
        task = self._scan_distinct_tasks(
            subject="math", count=1, shared_semantic_input=True
        )[0]
        duplicate = FrozenTask(dict(task.frozen_payload))
        self.assertEqual(duplicate.unit_sha256, task.unit_sha256)
        self.assertEqual(
            duplicate.frozen_payload_sha256, task.frozen_payload_sha256
        )
        coordinator = BlockingCoordinator(1)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BlockingFakeRunner(coordinator),
            stage_timeout_seconds=5,
        )
        first = dispatcher.submit(task)
        second = dispatcher.submit(duplicate)
        self.assertIs(first, second)
        self.assertTrue(coordinator.wait_all_entered())
        coordinator.release.set()
        self.assertEqual(first.wait(5).outcome, "succeeded")
        self.assertEqual(
            coordinator.calls[task.unit_sha256],
            {"analysis": 1, "critical_review": 1},
        )
        self.assertEqual(dispatcher.submit(task).wait(1).status, "deduplicated")

    def test_explicit_cancel_isolated_from_successful_sibling(self) -> None:
        blocked = frozen_task(1)
        healthy = frozen_task(2)
        gate = threading.Event()
        runners: dict[str, BehaviorRunner] = {}

        def factory(task, _context):
            behavior = "block" if task.unit_sha256 == blocked.unit_sha256 else "ok"
            runner = BehaviorRunner(behavior, gate)
            runners[task.unit_sha256] = runner
            return runner

        dispatcher = ConcurrentDispatcher(self.runtime, factory)
        handles = dispatcher.dispatch([blocked, healthy], wait=False)
        by_unit = {handle.unit_sha256: handle for handle in handles}
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and blocked.unit_sha256 not in runners:
            time.sleep(0.01)
        self.assertTrue(by_unit[blocked.unit_sha256].cancel("user_cancelled"))
        self.assertEqual(
            by_unit[blocked.unit_sha256].wait(2).outcome, "cancelled"
        )
        self.assertEqual(
            by_unit[healthy.unit_sha256].wait(2).outcome, "succeeded"
        )
        self.assertTrue(runners[blocked.unit_sha256].cancel_called.is_set())
        self.assertFalse(runners[healthy.unit_sha256].cancel_called.is_set())
        gate.set()

    def test_crash_isolated_from_successful_sibling(self) -> None:
        crashed = frozen_task(3)
        healthy = frozen_task(4)

        def factory(task, _context):
            return BehaviorRunner(
                "crash" if task.unit_sha256 == crashed.unit_sha256 else "ok"
            )

        dispatcher = ConcurrentDispatcher(
            self.runtime, factory, stage_timeout_seconds=1
        )
        results = dispatcher.dispatch([crashed, healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertEqual(by_unit[crashed.unit_sha256].outcome, "failed")
        self.assertEqual(
            by_unit[crashed.unit_sha256].error_code,
            "runner_crash:RuntimeError",
        )
        self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")

    def test_os_process_resource_exhaustion_waits_without_pausing_sibling(
        self,
    ) -> None:
        constrained = frozen_task(41)
        healthy = frozen_task(42)

        class ResourceRunner(BehaviorRunner):
            def run_analysis(self, task, context):
                if task.unit_sha256 == constrained.unit_sha256:
                    raise OSError(errno.EAGAIN, "synthetic process limit")
                return super().run_analysis(task, context)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: ResourceRunner("ok"),
            stage_timeout_seconds=1,
        )
        results = dispatcher.dispatch([constrained, healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertEqual(by_unit[constrained.unit_sha256].status, "retry_wait")
        self.assertEqual(
            by_unit[constrained.unit_sha256].error_code,
            "process_resource_eagain",
        )
        self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")
        self.assertFalse(
            dispatcher.lease_store._completion_path(
                constrained.unit_sha256
            ).exists()
        )
        lease = json.loads(
            dispatcher.lease_store._lease_path(
                constrained.unit_sha256
            ).read_text()
        )
        self.assertEqual(lease["status"], "retry_wait")
        self.assertEqual(lease["retry_kind"], "process_resource")

    def test_task_thread_start_resource_failure_does_not_stop_later_tasks(
        self,
    ) -> None:
        constrained = frozen_task(411)
        healthy = [frozen_task(412), frozen_task(413)]
        original_start = threading.Thread.start
        failed = False

        def injected_start(thread):
            nonlocal failed
            if (
                not failed
                and thread.name
                == f"preprocess-{constrained.unit_sha256[:12]}"
            ):
                failed = True
                raise OSError(errno.EAGAIN, "synthetic thread limit")
            return original_start(thread)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        with mock.patch.object(
            threading.Thread, "start", new=injected_start
        ):
            results = dispatcher.dispatch([constrained, *healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertTrue(failed)
        self.assertEqual(
            by_unit[constrained.unit_sha256].status, "retry_wait"
        )
        self.assertEqual(
            by_unit[constrained.unit_sha256].error_code,
            "process_resource_eagain",
        )
        self.assertEqual(
            [by_unit[task.unit_sha256].outcome for task in healthy],
            ["succeeded", "succeeded"],
        )
        self.assertEqual(dispatcher.active_count, 0)
        lease = json.loads(
            dispatcher.lease_store._lease_path(
                constrained.unit_sha256
            ).read_text()
        )
        self.assertEqual(lease["status"], "retry_wait")
        self.assertEqual(lease["retry_kind"], "process_resource")

    def test_heartbeat_thread_start_resource_failure_is_task_local(self) -> None:
        constrained = frozen_task(414)
        healthy = frozen_task(415)
        original_start = threading.Thread.start
        failed = False

        def injected_start(thread):
            nonlocal failed
            if (
                not failed
                and thread.name.startswith(
                    f"heartbeat-{constrained.unit_sha256[:12]}-"
                )
            ):
                failed = True
                raise OSError(errno.ENOMEM, "synthetic thread memory limit")
            return original_start(thread)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        with mock.patch.object(
            threading.Thread, "start", new=injected_start
        ):
            results = dispatcher.dispatch([constrained, healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertTrue(failed)
        self.assertEqual(
            by_unit[constrained.unit_sha256].error_code,
            "process_resource_enomem",
        )
        self.assertEqual(
            by_unit[constrained.unit_sha256].outcome, "waiting_retry"
        )
        self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")
        self.assertEqual(dispatcher.active_count, 0)

    def test_stage_thread_start_resource_failure_is_task_local(self) -> None:
        constrained = frozen_task(416)
        healthy = frozen_task(417)
        original_start = threading.Thread.start
        failed = False

        def injected_start(thread):
            nonlocal failed
            if (
                not failed
                and thread.name
                == f"analysis-{constrained.unit_sha256[:12]}"
            ):
                failed = True
                raise OSError(errno.EMFILE, "synthetic descriptor limit")
            return original_start(thread)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        with mock.patch.object(
            threading.Thread, "start", new=injected_start
        ):
            results = dispatcher.dispatch([constrained, healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertTrue(failed)
        self.assertEqual(
            by_unit[constrained.unit_sha256].error_code,
            "process_resource_emfile",
        )
        self.assertEqual(
            by_unit[constrained.unit_sha256].outcome, "waiting_retry"
        )
        self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")
        self.assertEqual(dispatcher.active_count, 0)

    def test_luna_rate_limit_waits_only_that_task_and_can_resume(self) -> None:
        limited = frozen_task(43)
        healthy = frozen_task(44)
        first_limited = True

        class LimitedRunner(BehaviorRunner):
            def run_analysis(self, task, context):
                nonlocal first_limited
                if task.unit_sha256 == limited.unit_sha256 and first_limited:
                    first_limited = False
                    raise DispatchError("luna_rate_limited")
                return super().run_analysis(task, context)

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: LimitedRunner("ok"),
            stage_timeout_seconds=1,
        )
        results = dispatcher.dispatch([limited, healthy])
        by_unit = {result.unit_sha256: result for result in results}
        self.assertEqual(by_unit[limited.unit_sha256].status, "retry_wait")
        self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")
        detail = json.loads(
            (
                dispatcher.lease_store.task_detail_root
                / f"{limited.unit_sha256}.json"
            ).read_text()
        )
        self.assertEqual(detail["phase"], "retry_wait")
        self.assertEqual(
            detail["server_queue_status"], "confirmed_rate_limited"
        )
        resumed = dispatcher.submit(limited).wait(2)
        self.assertEqual(resumed.outcome, "succeeded")
        self.assertEqual(resumed.completion["lease_fence"], 2)

    def test_usage_limit_is_terminal_and_never_loops_as_retry_wait(self) -> None:
        calls = 0

        class UsageLimitRunner(BehaviorRunner):
            def run_analysis(self, task, context):
                nonlocal calls
                calls += 1
                raise DispatchError("english_analysis_usage_limit")

        task = frozen_task(45, subject="english")
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: UsageLimitRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(2)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.error_code, "english_analysis_usage_limit")
        self.assertEqual(result.completion["lease_fence"], 1)
        self.assertEqual(calls, 1)

    def test_infrastructure_recovery_uses_new_fence_and_analysis_checkpoint(
        self,
    ) -> None:
        task = frozen_task(46, subject="cs408")
        calls = {"factory": 0, "analysis": 0, "critical_review": 0}
        leases = []

        class RecoveringRunner(BehaviorRunner):
            def run_analysis(self, current_task, context):
                calls["analysis"] += 1
                return stage_result("analysis", current_task)

            def run_critical_review(
                self, current_task, draft_analysis, context
            ):
                calls["critical_review"] += 1
                if context.lease.fence == 1:
                    raise InfrastructureCrash("runner_infrastructure_crash")
                return stage_result("critical_review", current_task)

        def factory(_task, context):
            calls["factory"] += 1
            leases.append(context.lease)
            return RecoveringRunner("ok")

        dispatcher = ConcurrentDispatcher(
            self.runtime, factory, stage_timeout_seconds=1
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "succeeded")
        self.assertEqual(result.completion["lease_fence"], 2)
        self.assertEqual(calls, {"factory": 2, "analysis": 1, "critical_review": 2})
        checkpoint_latest = json.loads(
            dispatcher.lease_store._analysis_checkpoint_latest_path(
                task.unit_sha256
            ).read_text()
        )
        self.assertEqual(checkpoint_latest["source_fence"], 1)
        fence_two_root = (
            dispatcher.lease_store.task_event_index_root
            / task.unit_sha256
            / "fence-2"
        )
        fence_two_events = [
            json.loads(path.read_text())["event"]
            for path in sorted(fence_two_root.glob("*.json"))
        ]
        self.assertIn("analysis_checkpoint_reused", fence_two_events)
        with self.assertRaisesRegex(DispatchError, "stale_lease_fence"):
            dispatcher.lease_store.publish_terminal(
                leases[0],
                task=task,
                outcome="failed",
                error_code="late_old_generation",
                analysis=None,
                critical_review=None,
                started_at="2026-08-05T00:00:00Z",
                finished_at="2026-08-05T00:00:01Z",
            )
        completion = json.loads(
            dispatcher.lease_store._completion_path(task.unit_sha256).read_text()
        )
        self.assertEqual(completion["lease_fence"], 2)
        self.assertEqual(completion["outcome"], "succeeded")

    def test_stale_production_child_cannot_overwrite_core_or_math_members(
        self,
    ) -> None:
        task = frozen_task(460, subject="math")
        store = LeaseStore(self.runtime)
        claimed = store.claim(
            task.unit_sha256, "production-owner", subject="math"
        )
        self.assertEqual(claimed.status, "claimed")
        assert claimed.lease is not None
        gate = self.runtime / "release-stale-child"
        ready = self.runtime / "stale-child-ready"
        env = os.environ.copy()
        for name in (
            "STUDY_PREPROCESS_RUNTIME_ROOT",
            "STUDY_PREPROCESS_UNIT_SHA256",
            "STUDY_PREPROCESS_LEASE_FENCE",
            "STUDY_PREPROCESS_LEASE_OWNER_ID",
        ):
            env.pop(name, None)
        env.update(
            {
                "STUDY_PREPROCESS_RUNTIME_ROOT": str(
                    self.runtime.resolve()
                ),
                "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
                "STUDY_PREPROCESS_LEASE_FENCE": "1",
                "STUDY_PREPROCESS_LEASE_OWNER_ID": "production-owner",
            }
        )
        process = subprocess.Popen(
            [
                sys.executable,
                str(
                    ROOT
                    / "tests"
                    / "fixtures"
                    / "late_fenced_core_writer.py"
                ),
                str(self.runtime),
                str(gate),
                str(ready),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            ready_deadline = time.monotonic() + 30
            while not ready.exists():
                if process.poll() is not None:
                    stdout, stderr = process.communicate(timeout=1)
                    self.fail(
                        "stale child exited before readiness: "
                        f"stdout={stdout!r} stderr={stderr!r}"
                    )
                if time.monotonic() >= ready_deadline:
                    self.fail("stale child readiness timeout")
                time.sleep(0.01)

            recovered = store.recover_after_infrastructure_crash(
                claimed.lease,
                error_code="synthetic_lost_heartbeat",
            )
            self.assertEqual(recovered.fence, 2)
            targets = [
                self.runtime / "state/jobs/math/GROUP-OWNER.json",
                self.runtime / "state/jobs/math/GROUP-MEMBER.json",
                self.runtime / "state/latest/math/GROUP-OWNER.json",
                self.runtime / "state/latest/math/GROUP-MEMBER.json",
                self.runtime / "packages/objects" / ("f" * 64 + ".json"),
            ]
            expected = json.dumps(
                {"writer": "current-fence-two"}, sort_keys=True
            )
            for path in targets:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(expected, encoding="utf-8")
            gate.touch()
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(
                json.loads(stdout), ["stale_lease_fence"] * len(targets)
            )
            for path in targets:
                self.assertEqual(path.read_text(encoding="utf-8"), expected)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=2)

    def test_late_old_generation_cannot_unlink_new_generation_pointer(
        self,
    ) -> None:
        task = frozen_task(461, subject="cs408")
        store = LeaseStore(self.runtime)
        claimed = store.claim(
            task.unit_sha256, "resume-owner", subject="cs408"
        )
        assert claimed.lease is not None
        pointer = self.runtime / "state/latest/cs408/CAP-0461.json"
        value = {"publication": "same-bytes-across-generations"}

        def lease_environment(fence: int) -> dict[str, str]:
            return {
                "STUDY_PREPROCESS_RUNTIME_ROOT": str(self.runtime.resolve()),
                "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
                "STUDY_PREPROCESS_LEASE_FENCE": str(fence),
                "STUDY_PREPROCESS_LEASE_OWNER_ID": "resume-owner",
            }

        with mock.patch.dict(
            os.environ, lease_environment(1), clear=False
        ):
            atomic_write_json(pointer, value)
        recovered = store.recover_after_infrastructure_crash(
            claimed.lease,
            error_code="synthetic_lost_heartbeat",
        )
        self.assertEqual(recovered.fence, 2)
        with mock.patch.dict(
            os.environ, lease_environment(2), clear=False
        ):
            # Identical bytes make this a fence test rather than only a
            # compare-and-swap test.
            atomic_write_json(pointer, value)
        with mock.patch.dict(
            os.environ, lease_environment(1), clear=False
        ), self.assertRaisesRegex(PreprocessorError, "stale_lease_fence"):
            atomic_restore_json_if_current(
                pointer,
                expected_current=value,
                replacement=None,
            )
        self.assertTrue(pointer.is_file())
        self.assertEqual(json.loads(pointer.read_text()), value)

    def test_infrastructure_crash_is_automatically_recovered_at_most_once(
        self,
    ) -> None:
        task = frozen_task(47)
        fences = []

        class AlwaysInfrastructureCrash(BehaviorRunner):
            def run_analysis(self, current_task, context):
                fences.append(context.lease.fence)
                raise InfrastructureCrash("runner_infrastructure_crash")

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: AlwaysInfrastructureCrash("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(fences, [1, 2])
        self.assertEqual(result.completion["lease_fence"], 2)
        self.assertEqual(
            result.error_code,
            "infrastructure_recovery_exhausted:runner_infrastructure_crash",
        )

    def test_evidence_or_format_error_is_not_retried(self) -> None:
        task = frozen_task(48, subject="cs408")
        calls = 0

        class EvidenceErrorRunner(BehaviorRunner):
            def run_analysis(self, current_task, context):
                nonlocal calls
                calls += 1
                raise DispatchError("cs408_evidence_format_invalid")

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: EvidenceErrorRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(2)
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.completion["lease_fence"], 1)
        self.assertEqual(calls, 1)

    def test_cancel_isolated_from_successful_sibling(self) -> None:
        blocked = frozen_task(5)
        healthy = frozen_task(6)
        gate = threading.Event()
        entered = threading.Event()

        class CancelRunner(BehaviorRunner):
            def run_analysis(self, task, context):
                if self.behavior == "block":
                    entered.set()
                return super().run_analysis(task, context)

        def factory(task, _context):
            return CancelRunner(
                "block" if task.unit_sha256 == blocked.unit_sha256 else "ok",
                gate,
            )

        dispatcher = ConcurrentDispatcher(
            self.runtime, factory, stage_timeout_seconds=2
        )
        blocked_handle = dispatcher.submit(blocked)
        healthy_handle = dispatcher.submit(healthy)
        self.assertTrue(entered.wait(2))
        self.assertTrue(blocked_handle.cancel())
        self.assertEqual(blocked_handle.wait(2).outcome, "cancelled")
        self.assertEqual(healthy_handle.wait(2).outcome, "succeeded")
        gate.set()

    def test_stale_fence_cannot_publish(self) -> None:
        store = LeaseStore(self.runtime)
        task = frozen_task(7)
        base = dt.datetime(2026, 8, 5, tzinfo=dt.timezone.utc)
        first = store.claim(task.unit_sha256, "owner-1", now=base.isoformat())
        self.assertEqual(first.lease.fence, 1)
        takeover_time = base + dt.timedelta(seconds=LEASE_TTL_SECONDS + 1)
        second = store.claim(
            task.unit_sha256, "owner-2", now=takeover_time.isoformat()
        )
        self.assertEqual(second.lease.fence, 2)
        with self.assertRaisesRegex(DispatchError, "stale_lease_fence"):
            store.publish_terminal(
                first.lease,
                task=task,
                outcome="failed",
                error_code="late_result",
                analysis=None,
                critical_review=None,
                started_at=base.isoformat(),
                finished_at=takeover_time.isoformat(),
            )
        completion_path = (
            self.runtime
            / "dispatch"
            / "state"
            / "completions"
            / f"{task.unit_sha256}.json"
        )
        self.assertFalse(completion_path.exists())

    def test_drain_stops_new_claims_and_waits_for_active_zero(self) -> None:
        coordinator = BlockingCoordinator(1)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BlockingFakeRunner(coordinator),
            stage_timeout_seconds=5,
        )
        handle = dispatcher.submit(frozen_task(8))
        self.assertTrue(coordinator.wait_all_entered())
        drained: list[bool] = []
        thread = threading.Thread(target=lambda: drained.append(dispatcher.drain(5)))
        thread.start()
        deadline = time.monotonic() + 2
        while not dispatcher.draining and time.monotonic() < deadline:
            time.sleep(0.01)
        with self.assertRaisesRegex(DispatchError, "dispatcher_draining"):
            dispatcher.submit(frozen_task(9))
        self.assertTrue(thread.is_alive())
        coordinator.release.set()
        thread.join(5)
        self.assertEqual(drained, [True])
        self.assertEqual(handle.wait(1).outcome, "succeeded")
        self.assertEqual(dispatcher.active_count, 0)

    def test_subject_drain_does_not_treat_stale_claim_as_empty(self) -> None:
        store = LeaseStore(self.runtime)
        task = frozen_task(801, subject="cs408")
        stale_at = (
            dt.datetime.now(dt.timezone.utc)
            - dt.timedelta(seconds=LEASE_TTL_SECONDS + 5)
        ).isoformat()
        decision = store.claim(
            task.unit_sha256,
            "stale-owner",
            subject="cs408",
            now=stale_at,
        )
        assert decision.lease is not None
        store.begin_subject_drain("cs408")
        status = store.subject_status("cs408")
        self.assertEqual(status["active_count"], 0)
        self.assertEqual(status["stale_count"], 1)
        self.assertEqual(status["claimed_total"], 1)

        drained: list[dict[str, Any]] = []
        waiter = threading.Thread(
            target=lambda: drained.append(
                store.wait_subject_drained("cs408", poll_seconds=0.01)
            )
        )
        waiter.start()
        time.sleep(0.05)
        self.assertTrue(waiter.is_alive())
        store.mark_retry_wait(
            decision.lease,
            error_code="process_resource_eagain",
            retry_kind="process_resource",
        )
        waiter.join(1)
        self.assertFalse(waiter.is_alive())
        self.assertEqual(drained[0]["claimed_total"], 0)

    def test_model_contract_mismatch_is_rejected(self) -> None:
        with self.assertRaisesRegex(DispatchError, "requested_model_mismatch"):
            FrozenTask({"capture_id": "x"}, requested_model="other")

        class WrongRuntimeRunner(BehaviorRunner):
            def run_analysis(self, task, context):
                return StageResult(
                    payload={"draft": True},
                    runtime_model="other",
                    runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
                    runtime_metadata_provenance="codex_json_attestation_v1",
                    runtime_identity_status="confirmed",
                )

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: WrongRuntimeRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(frozen_task(10)).wait(2)
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.error_code, "runtime_identity_confirmation_invalid"
        )

    def test_receipt_and_package_are_immutable_content_addressed(self) -> None:
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(frozen_task(11)).wait(2)
        completion = result.completion
        self.assertIsNotNone(completion)
        for key in ("receipt_path", "package_path"):
            path = Path(completion[key])
            self.assertTrue(path.is_file())
            self.assertEqual(oct(path.stat().st_mode & 0o777), "0o400")
            digest = path.stem
            self.assertEqual(
                __import__("hashlib").sha256(path.read_bytes()).hexdigest(), digest
            )
        receipt = json.loads(Path(completion["receipt_path"]).read_text())
        ledger_path = Path(completion["ledger_entry_path"])
        self.assertTrue(ledger_path.is_file())
        self.assertEqual(oct(ledger_path.stat().st_mode & 0o777), "0o400")
        self.assertEqual(
            __import__("hashlib").sha256(ledger_path.read_bytes()).hexdigest(),
            completion["ledger_entry_sha256"],
        )
        self.assertEqual(receipt["model_contract"]["model"], REQUIRED_MODEL)
        self.assertEqual(
            receipt["model_contract"]["reasoning_effort"],
            REQUIRED_REASONING_EFFORT,
        )
        self.assertEqual(HEARTBEAT_INTERVAL_SECONDS, 15)
        self.assertEqual(LEASE_TTL_SECONDS, 120)

    def test_evidence_pending_readiness_is_hmac_bound_and_never_enqueued(self) -> None:
        contract = Path(self.temp.name) / "controlled-contract.json"
        contract.write_text("{}\n", encoding="utf-8")
        receipt = {
            "schema_version": "current-question-evidence-readiness-receipt-v1",
            "status": "evidence_pending",
            "capture_id": "CAP-EVIDENCE-PENDING",
            "evidence_manifest_sha256": "1" * 64,
            "evidence_bundle_sha256": "2" * 64,
            "question_mode": "image_question",
            "attachment_rows_sha256": "3" * 64,
            "required_roles": ["question_image", "solution_image"],
            "present_roles": ["question_image"],
            "missing_roles": ["solution_image"],
            "trace_full_sha256": "4" * 64,
            "controlled_contract_path": str(contract.resolve()),
            "controlled_contract_sha256": "5" * 64,
            "producer_build_sha256": "6" * 64,
            "created_at": "2026-08-05T00:00:00Z",
            "model_enqueue_allowed": False,
            "formal_write_count": 0,
        }
        store = LeaseStore(self.runtime)
        published = store.publish_evidence_readiness(
            receipt, expected_release_id="a" * 64
        )
        self.assertEqual(published["status"], "evidence_pending")
        self.assertFalse(published["model_enqueue_allowed"])
        verified = store.verify_evidence_readiness(
            published["authority_receipt_sha256"],
            expected_release_id="a" * 64,
            expected_capture_id="CAP-EVIDENCE-PENDING",
        )
        self.assertEqual(
            verified["authority_receipt"]["receipt"]["status"],
            "evidence_pending",
        )
        self.assertTrue(Path(published["ledger_entry_path"]).is_file())

        contradictory = dict(receipt)
        contradictory["model_enqueue_allowed"] = True
        with self.assertRaisesRegex(
            DispatchError, "evidence_readiness_identity_invalid"
        ):
            store.publish_evidence_readiness(
                contradictory, expected_release_id="a" * 64
            )

    def test_subject_drain_marker_rejects_cross_dispatcher_claims(self) -> None:
        store = LeaseStore(self.runtime)
        store.begin_subject_drain("math")
        with self.assertRaisesRegex(DispatchError, "subject_draining"):
            store.claim(frozen_task(12).unit_sha256, "other", subject="math")
        store.clear_subject_drain("math")
        decision = store.claim(
            frozen_task(12).unit_sha256, "other", subject="math"
        )
        self.assertEqual(decision.status, "claimed")

    def test_hmac_allowlisted_one_shot_runs_without_clearing_math_drain(self) -> None:
        release_id = "a" * 64
        task = frozen_task(120)
        payload = dict(task.frozen_payload)
        payload["dispatch_contract"] = {
            **dispatch_rule_binding(
                release_id=release_id,
                subject="math",
                subject_processing_contract_sha256="9" * 64,
            ),
            "schema_version": "study-intake-dispatch-release-binding-v1",
            "dispatch_reason": "controlled_replay",
        }
        task = FrozenTask(payload)
        store = LeaseStore(self.runtime)
        store.begin_subject_drain("math")
        prepared = store.prepare_controlled_replay_allowlist(
            release_id=release_id,
            tasks=[task],
        )
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
            controlled_replay_authority=prepared["allowlist"],
        )
        result = dispatcher.submit(task).wait(5)
        self.assertEqual(result.outcome, "succeeded")
        self.assertTrue(store.subject_status("math")["draining"])
        self.assertEqual(store.subject_status("math")["active_count"], 0)
        self.assertTrue(Path(prepared["allowlist_path"]).is_file())

        replay = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
            controlled_replay_authority=prepared["allowlist"],
        )
        repeated = replay.dispatch([task])[0]
        self.assertEqual(repeated.outcome, "failed")
        self.assertEqual(
            repeated.error_code, "controlled_replay_already_consumed"
        )

    def test_frozen_task_payload_cannot_be_mutated_after_hashing(self) -> None:
        source = {"subject": "math", "nested": {"value": 1}}
        task = FrozenTask(source)
        unit = task.unit_sha256
        source["nested"]["value"] = 2
        exposed = task.frozen_payload
        exposed["nested"]["value"] = 3
        self.assertEqual(task.frozen_payload["nested"]["value"], 1)
        self.assertEqual(task.unit_sha256, unit)

    def test_rule_checksum_changes_task_identity_with_subject_contract(self) -> None:
        first_rule = dispatch_rule_binding(
            release_id="a" * 64,
            subject="math",
            subject_processing_contract_sha256="1" * 64,
        )
        second_rule = dispatch_rule_binding(
            release_id="a" * 64,
            subject="math",
            subject_processing_contract_sha256="2" * 64,
        )
        self.assertNotEqual(
            first_rule["rule_version_sha256"],
            second_rule["rule_version_sha256"],
        )
        first = FrozenTask(
            {
                "subject": "math",
                "dispatch_contract": first_rule,
            }
        )
        second = FrozenTask(
            {
                "subject": "math",
                "dispatch_contract": second_rule,
            }
        )
        self.assertNotEqual(first.unit_sha256, second.unit_sha256)
        self.assertEqual(first_rule["requested_model"], "gpt-5.6-luna")
        self.assertEqual(first_rule["requested_reasoning_effort"], "max")

    def test_core_scan_bridge_uses_public_read_only_unbounded_selector(self) -> None:
        candidates = [core_candidate(index) for index in range(20)]
        calls: list[tuple[str, str, bool, bool]] = []

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                pass

            def eligible_candidates(
                self,
                subject,
                study_date,
                *,
                mutate_recovery_state,
                scan_all_study_dates,
            ):
                calls.append(
                    (
                        subject,
                        study_date,
                        mutate_recovery_state,
                        scan_all_study_dates,
                    )
                )
                return [(candidate, "eligible") for candidate in candidates]

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        frozen, decisions = scan_eligible_candidates(
            config, "cs408", worker_factory=ScanWorker
        )
        self.assertEqual(len(frozen), 20)
        self.assertEqual(len(decisions), 20)
        self.assertEqual(calls[0][0], "cs408")
        self.assertFalse(calls[0][2])
        self.assertTrue(calls[0][3])
        self.assertEqual(len({row.task.unit_sha256 for row in frozen}), 20)

    def test_core_scan_bridge_discovers_delayed_intake_for_all_subjects(self) -> None:
        calls: list[tuple[str, str, bool, bool]] = []

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                pass

            def eligible_candidates(
                self,
                subject,
                study_date,
                *,
                mutate_recovery_state,
                scan_all_study_dates,
            ):
                calls.append(
                    (
                        subject,
                        study_date,
                        mutate_recovery_state,
                        scan_all_study_dates,
                    )
                )
                return []

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        for subject in ("math", "cs408", "english"):
            frozen, decisions = scan_eligible_candidates(
                config, subject, worker_factory=ScanWorker
            )
            self.assertEqual(frozen, [])
            self.assertEqual(decisions, [])
        self.assertEqual([row[0] for row in calls], ["math", "cs408", "english"])
        self.assertTrue(all(row[2] is False for row in calls))
        self.assertTrue(all(row[3] is True for row in calls))

    def test_scanner_worker_fake_contract_records_selector_and_status_calls(
        self,
    ) -> None:
        candidate = core_candidate(19, subject="math")
        factory = make_scanner_worker_factory(
            release_id="b" * 64,
            eligible_candidates=[(candidate, "eligible")],
            scan_statuses={
                "math": {
                    "pending": [
                        {
                            "event_id": candidate.capture_id,
                            "recorded_at": candidate.recorded_at,
                        }
                    ]
                }
            },
        )
        worker = factory(
            {
                "timezone": "Asia/Shanghai",
                "model": {
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                },
            }
        )

        self.assertEqual(worker.release_id, "b" * 64)
        self.assertEqual(
            worker.scan_statuses(
                None,
                only_subject="math",
            )["math"]["pending"][0]["event_id"],
            candidate.capture_id,
        )
        self.assertEqual(
            worker.eligible_candidates(
                "math",
                "2026-08-05",
                mutate_recovery_state=False,
            ),
            [(candidate, "eligible")],
        )
        self.assertEqual(
            [row["method"] for row in factory.calls],
            ["scan_statuses", "eligible_candidates"],
        )
        self.assertFalse(factory.calls[1]["kwargs"]["mutate_recovery_state"])

    def test_scanner_worker_fake_matches_production_scan_protocol(self) -> None:
        for method_name in ("scan_statuses", "eligible_candidates"):
            with self.subTest(method=method_name):
                production = inspect.signature(getattr(Worker, method_name))
                fake = inspect.signature(getattr(ScannerWorkerFake, method_name))
                self.assertEqual(
                    [
                        (name, parameter.kind, parameter.default)
                        for name, parameter in production.parameters.items()
                    ],
                    [
                        (name, parameter.kind, parameter.default)
                        for name, parameter in fake.parameters.items()
                    ],
                )

    def test_math_foreign_release_audit_filters_before_candidate_build(self) -> None:
        scan_factory = make_scanner_worker_factory(
            scan_statuses={
                "math": {
                    "pending": [
                        {
                            "event_id": "OLD",
                            "recorded_at": "2026-08-15T12:26:18Z",
                        },
                        {
                            "event_id": "NEW",
                            "recorded_at": "2026-08-15T12:26:19Z",
                        },
                    ]
                }
            }
        )

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        frozen, decisions = scan_eligible_candidates(
            config,
            "math",
            worker_factory=scan_factory,
            producer_recorded_after="2026-08-15T12:26:18Z",
        )
        self.assertEqual(frozen, [])
        self.assertEqual(decisions, [])
        self.assertEqual(
            [
                row["kwargs"]["capture_allowlist"]
                for row in scan_factory.calls
                if row["method"] == "eligible_candidates"
            ],
            [frozenset({"NEW"})],
        )

    def test_math_same_release_audit_keeps_producer_watermark_filter(self) -> None:
        calls: list[dict[str, Any]] = []

        class CanaryStore:
            def __init__(self, _runtime_root):
                pass

            def production_canary_identity_read_only(
                self, _subject, *, configured_release_id
            ):
                return {
                    "foreign_release": False,
                    "release_id": configured_release_id,
                }

            def production_canary_status_read_only(
                self, _subject, *, expected_release_id=None
            ):
                return {
                    "producer_high_watermark": {
                        "recorded_at": "2026-08-15T12:26:18Z"
                    }
                }

            def subject_status_read_only(self, _subject):
                return {
                    "subject": "math",
                    "active_count": 0,
                    "claimed_total": 0,
                    "draining": True,
                    "read_only": True,
                }

        config = {
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
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

        def fake_scan(*args, **kwargs):
            calls.append({"args": args, "kwargs": kwargs})
            return [], []

        with (
            mock.patch("preprocess_dispatcher.LeaseStore", CanaryStore),
            mock.patch(
                "preprocess_dispatcher.release_identity",
                return_value=("a" * 64, "b" * 64),
            ),
            mock.patch(
                "preprocess_dispatcher._production_canary_enabled",
                return_value=True,
            ),
            mock.patch(
                "preprocess_dispatcher.scan_eligible_candidates",
                side_effect=fake_scan,
            ),
        ):
            result = _audit(config, "math")

        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0]["kwargs"]["producer_recorded_after"],
            "2026-08-15T12:26:18Z",
        )
        self.assertEqual(result["eligible_count"], 0)

    def test_core_scan_bridge_exact_allowlist_excludes_every_other_capture(
        self,
    ) -> None:
        candidates = [core_candidate(index) for index in range(8)]

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                pass

            def eligible_candidates(
                self,
                subject,
                study_date,
                *,
                mutate_recovery_state,
                scan_all_study_dates,
            ):
                del subject, study_date, mutate_recovery_state
                self.scan_all_study_dates = scan_all_study_dates
                return [(candidate, "eligible") for candidate in candidates]

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        allowed = frozenset(
            {candidates[2].capture_id, candidates[6].capture_id}
        )
        frozen, decisions = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            capture_allowlist=allowed,
        )
        self.assertEqual(
            {row.candidate.capture_id for row in frozen}, allowed
        )
        self.assertEqual(
            {row["capture_id"] for row in decisions}, allowed
        )
        self.assertTrue(all(row["eligible"] for row in decisions))

    def test_core_scan_bridge_external_override_is_exact_and_release_bound(
        self,
    ) -> None:
        candidate = core_candidate(77, subject="math")
        candidate = Candidate(
            **{
                **candidate.__dict__,
                "input_binding": {
                    **candidate.input_binding,
                    "evidence_bundle_sha256": "7" * 64,
                },
            }
        )

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                pass

        config = {
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        frozen, decisions = scan_eligible_candidates(
            config,
            "math",
            worker_factory=ScanWorker,
            capture_allowlist=frozenset({candidate.capture_id}),
            candidate_overrides=[candidate],
        )
        self.assertEqual(len(frozen), 1)
        self.assertEqual(decisions[0]["capture_id"], candidate.capture_id)
        self.assertEqual(
            decisions[0]["reason"], "controlled_replay_external_input"
        )
        exact_candidate = Candidate(
            **{
                **candidate.__dict__,
                "capture_id": "MFI-CAP-5d1e87959549b634bdda36b2",
            }
        )
        exact_frozen, exact_decisions = scan_eligible_candidates(
            config,
            "math",
            worker_factory=ScanWorker,
            capture_allowlist=frozenset({exact_candidate.capture_id}),
            candidate_overrides=[exact_candidate],
        )
        self.assertEqual(exact_frozen, [])
        self.assertEqual(len(exact_decisions), 1)
        self.assertEqual(
            exact_decisions[0]["error_code"],
            "math_smoke_runtime_root_invalid",
        )
        self.assertFalse(exact_decisions[0]["model_enqueue_allowed"])
        with self.assertRaisesRegex(DispatchError, "candidate_overrides_invalid"):
            scan_eligible_candidates(
                config,
                "cs408",
                worker_factory=ScanWorker,
                capture_allowlist=frozenset({candidate.capture_id}),
                candidate_overrides=[candidate],
            )

    def test_concurrent_cs408_rejects_unresolved_v1_and_v2_before_model(
        self,
    ) -> None:
        legacy_rows = []
        for index, schema in enumerate(
            (
                "current-question-evidence-bundle-v1",
                "current-question-evidence-bundle-v2",
            ),
            start=1,
        ):
            candidate = core_candidate(100 + index)
            model_input = dict(candidate.model_input)
            bundle = dict(model_input["current_question_evidence"])
            bundle["schema_version"] = schema
            bundle["interaction_trace"] = {
                "schema": "current-question-interaction-trace-v1",
                "events": [],
                "event_count": 0,
                "truncated": False,
            }
            model_input["current_question_evidence"] = bundle
            legacy_rows.append(
                (
                    Candidate(
                        **{
                            **candidate.__dict__,
                            "model_input": model_input,
                        }
                    ),
                    "eligible",
                )
            )

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                self.adapters = {
                    "cs408": SimpleNamespace(
                        candidate_diagnostics={}, candidate_errors={}
                    )
                }

            def eligible_candidates(
                self, _subject, _study_date, *, mutate_recovery_state
            ):
                self.mutate_recovery_state = mutate_recovery_state
                return legacy_rows

        config = {
            "timezone": "Asia/Shanghai",
            "runtime_root": str(self.runtime),
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        frozen, decisions = scan_eligible_candidates(
            config, "cs408", worker_factory=ScanWorker
        )
        self.assertEqual(frozen, [])
        self.assertEqual(len(decisions), 2)
        self.assertTrue(
            all(row["model_enqueue_allowed"] is False for row in decisions)
        )
        self.assertEqual(
            {row["reason"] for row in decisions},
            {
                "concurrent_cs408_legacy_resolution_required",
                "concurrent_cs408_evidence_v3_required",
            },
        )

    def test_production_runtime_runner_uses_only_frozen_task_reason(self) -> None:
        config_path = self.runtime / "config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        runtime = ProductionDispatchRuntime(
            {
                "runtime_root": str(self.runtime),
                "worker": {"model_timeout_seconds": 1},
                "cs408_deep_v2": stage_runtime_profile(),
            },
            "cs408",
            config_path,
        )
        self.assertFalse(hasattr(runtime, "registry"))
        task = FrozenTask(
            {
                **dict(frozen_task(700, subject="cs408").frozen_payload),
                "dispatch_contract": {
                    "dispatch_reason": "eligible",
                },
            }
        )
        runner = runtime._runner_factory(task, None)
        self.assertIsInstance(runner, CoreCandidateSubprocessRunner)
        self.assertIsNone(runner.reason)

    def test_scan_and_submit_continues_after_task_thread_resource_failure(
        self,
    ) -> None:
        config_path = self.runtime / "scan-resource-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        runtime = ProductionDispatchRuntime(
            {
                "runtime_root": str(self.runtime),
                "worker": {"model_timeout_seconds": 1},
                "cs408_deep_v2": stage_runtime_profile(),
            },
            "cs408",
            config_path,
        )
        constrained = frozen_task(701, subject="cs408")
        healthy = [
            frozen_task(702, subject="cs408"),
            frozen_task(703, subject="cs408"),
        ]
        units = [
            SimpleNamespace(task=task) for task in [constrained, *healthy]
        ]
        runtime.dispatcher.runner_factory = (
            lambda _task, _context: BehaviorRunner("ok")
        )
        original_start = threading.Thread.start
        failed = False

        def injected_start(thread):
            nonlocal failed
            if (
                not failed
                and thread.name
                == f"preprocess-{constrained.unit_sha256[:12]}"
            ):
                failed = True
                raise OSError(errno.ENFILE, "synthetic system file limit")
            return original_start(thread)

        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(units, [{"eligible": True}] * len(units)),
        ), mock.patch.object(
            threading.Thread, "start", new=injected_start
        ):
            handles, decisions = runtime.scan_and_submit()
            results = [handle.wait(2) for handle in handles]
        self.assertTrue(failed)
        self.assertEqual(len(handles), 3)
        self.assertEqual(len(decisions), 3)
        by_unit = {result.unit_sha256: result for result in results}
        self.assertEqual(
            by_unit[constrained.unit_sha256].error_code,
            "process_resource_enfile",
        )
        self.assertEqual(
            [by_unit[task.unit_sha256].outcome for task in healthy],
            ["succeeded", "succeeded"],
        )
        self.assertEqual(runtime.dispatcher.active_count, 0)

    def test_backlog_audit_never_submits_or_clears_drain(self) -> None:
        task = frozen_task(704, subject="math")
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(
                [SimpleNamespace(task=task)],
                [{"capture_id": "CAP-0704", "eligible": True}],
            ),
        ) as scan:
            value = _audit({"runtime_root": str(self.runtime)}, "math")
        scan.assert_called_once_with(
            {"runtime_root": str(self.runtime)},
            "math",
            publish_evidence_readiness=False,
            producer_recorded_after=None,
        )
        self.assertEqual(value["eligible_count"], 1)
        self.assertEqual(value["eligible_units"][0]["capture_id"], "CAP-0704")
        self.assertEqual(value["model_call_count"], 0)
        self.assertEqual(value["formal_write_count"], 0)

    def test_math_activation_reuses_prior_signed_high_watermark(self) -> None:
        recorded_at = "2026-08-15T12:26:18.469151+00:00"
        refreshed = {"status": "production_canary_active"}

        class Store:
            def production_canary_status_read_only(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)
                return {
                    "producer_high_watermark": {
                        "recorded_at": recorded_at,
                    }
                }

            def activate_production_canary(self, *args, **kwargs):
                return {"activation_id": "a" * 64}

            def production_canary_status(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)
                return refreshed

        runtime = object.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "math"
        runtime.config = {"runtime_root": str(self.runtime)}
        runtime.continuous_concurrency_limit = 20
        runtime.dispatcher = SimpleNamespace(lease_store=Store())
        authority = {"authority_fingerprint": "b" * 64}
        with mock.patch(
            "preprocess_dispatcher.Worker",
            return_value=SimpleNamespace(release_id="c" * 64),
        ), mock.patch(
            "preprocess_dispatcher.producer_authority_binding",
            return_value=authority,
        ), mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([], []),
        ) as scan:
            self.assertIs(
                runtime.activate_production_canary(
                    activated_at="2026-08-16T00:00:00+00:00"
                ),
                refreshed,
            )
        scan.assert_called_once_with(
            runtime.config,
            "math",
            publish_evidence_readiness=False,
            producer_recorded_after=recorded_at,
        )

    def test_math_daemon_scan_reuses_signed_high_watermark(self) -> None:
        recorded_at = "2026-08-15T12:26:18.469151+00:00"

        class Store:
            def reconcile_production_canary_preclaim_failures(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)

            def production_canary_global_preclaim_failure(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)
                return None

            def production_canary_status_read_only(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)
                return {
                    "producer_high_watermark": {
                        "recorded_at": recorded_at,
                    }
                }

            def production_canary_status(self, subject):
                self_subject = subject
                if self_subject != "math":
                    raise AssertionError(self_subject)
                return {
                    "state": "armed",
                    "active_task_count": 0,
                    "luna_consumer_enabled": False,
                    "continuous_concurrency_limit": 20,
                }

            def set_production_canary_backpressure(self, subject, reason):
                if subject != "math" or reason is not None:
                    raise AssertionError((subject, reason))
                return self.production_canary_status(subject)

            def subject_status(self, subject):
                if subject != "math":
                    raise AssertionError(subject)
                return {"subject": subject}

        runtime = object.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "math"
        runtime.config = {"runtime_root": str(self.runtime)}
        runtime.dispatcher = SimpleNamespace(lease_store=Store())
        runtime._projection_fail_closed = False
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=([], []),
        ) as scan, mock.patch(
            "preprocess_dispatcher._write_subject_projections"
        ):
            handles, decisions = runtime.scan_and_submit()
        self.assertEqual(handles, [])
        self.assertEqual(decisions, [])
        scan.assert_called_once_with(
            runtime.config,
            "math",
            producer_recorded_after=recorded_at,
        )

    def test_activation_persists_four_historical_cs408_exclusions(self) -> None:
        release_id = "a" * 64
        activated_at = "2026-08-10T00:00:00Z"
        candidates = [core_candidate(800 + index) for index in range(4)]
        readiness_contract = self.runtime / "cs408-readiness-contract.json"
        readiness_contract.parent.mkdir(parents=True, exist_ok=True)
        readiness_contract.write_text("{}\n", encoding="utf-8")
        readiness_contract_sha256 = hashlib.sha256(
            readiness_contract.read_bytes()
        ).hexdigest()

        def readiness_for(candidate):
            def digest(label):
                return hashlib.sha256(
                    f"{candidate.capture_id}:{label}".encode()
                ).hexdigest()

            return {
                "schema_version": (
                    "current-question-evidence-readiness-receipt-v1"
                ),
                "status": "ready",
                "capture_id": candidate.capture_id,
                "evidence_manifest_sha256": digest("manifest"),
                "evidence_bundle_sha256": digest("bundle"),
                "question_mode": "dialogue_only",
                "attachment_rows_sha256": digest("attachments"),
                "required_roles": [],
                "present_roles": [],
                "missing_roles": [],
                "trace_full_sha256": digest("trace"),
                "controlled_contract_path": str(
                    readiness_contract.resolve()
                ),
                "controlled_contract_sha256": readiness_contract_sha256,
                "producer_build_sha256": digest("producer"),
                "created_at": candidate.recorded_at,
                "model_enqueue_allowed": True,
                "formal_write_count": 0,
            }

        diagnostics = {
            candidate.capture_id: readiness_for(candidate)
            for candidate in candidates
        }

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                self.adapters = {
                    "cs408": SimpleNamespace(
                        candidate_diagnostics=diagnostics,
                        candidate_errors={},
                        candidate_error_context={},
                    )
                }

            def eligible_candidates(
                self, requested_subject, _study_date, **_kwargs
            ):
                if requested_subject != "cs408":
                    raise AssertionError("scanner crossed subject boundary")
                return [(candidate, "eligible") for candidate in candidates]

        config = {
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
            "worker": {"model_timeout_seconds": 1},
            "cs408_deep_v2": stage_runtime_profile(),
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
        source_release_manifest = self.runtime / "source-release.json"
        atomic_write_json(
            source_release_manifest,
            {
                "schema_version": RELEASE_SCHEMA,
                "release_id": release_id,
            },
        )
        config["release"] = {"manifest_path": str(source_release_manifest)}
        config_path = self.runtime / "activation-classification-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        runtime = ProductionDispatchRuntime(
            config,
            "cs408",
            config_path,
            scan_worker_factory=ScanWorker,
        )
        store = runtime.dispatcher.lease_store
        store.begin_subject_drain("cs408")
        authority = _producer_authority_from_processing_contract(
            "cs408", release_id, "9" * 64
        )
        source_event_set_sha256 = hashlib.sha256(
            canonical_bytes([])
        ).hexdigest()
        high_watermark = {
            "schema_version": "study-intake-producer-high-watermark-v1",
            "subject": "cs408",
            "release_id": release_id,
            "recorded_at": activated_at,
            "producer_authority_fingerprint": authority[
                "authority_fingerprint"
            ],
            "source_event_ids": [],
            "source_event_set_sha256": source_event_set_sha256,
            "formal_write_count": 0,
        }
        high_watermark_sha256 = hashlib.sha256(
            canonical_bytes(high_watermark)
        ).hexdigest()
        expected_activation_id = hashlib.sha256(
            canonical_bytes(
                {
                    "schema_version": (
                        "study-intake-production-canary-activation-receipt-v2"
                    ),
                    "subject": "cs408",
                    "release_id": release_id,
                    "activated_at": activated_at,
                    "producer_authority_fingerprint": authority[
                        "authority_fingerprint"
                    ],
                    "producer_high_watermark_sha256": high_watermark_sha256,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                }
            )
        ).hexdigest()
        with mock.patch(
            "preprocess_dispatcher.Worker",
            return_value=SimpleNamespace(release_id=release_id),
        ), mock.patch(
            "preprocess_dispatcher.producer_authority_binding",
            return_value=authority,
        ), mock.patch.object(
            runtime.dispatcher,
            "submit",
            side_effect=AssertionError("activation submitted Luna work"),
        ) as submit:
            state = runtime.activate_production_canary(
                activated_at=activated_at,
                expected_activation_id=expected_activation_id,
                expected_producer_authority_fingerprint=authority[
                    "authority_fingerprint"
                ],
            )
        submit.assert_not_called()
        self.assertEqual(state["historical_eligible_count"], 4)
        self.assertEqual(state["excluded_by_high_watermark_count"], 4)
        self.assertEqual(state["canary_queue_count"], 0)
        self.assertEqual(state["queue_depth"], 0)
        self.assertEqual(state["queue_classification"]["pre_activation_frozen"], 4)
        for key in (
            "model_call_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "observed_model_call_count",
            "observed_provider_request_count",
            "observed_mcp_tool_call_count",
            "formal_write_count",
        ):
            self.assertEqual(state[key], 0, key)
        self.assertFalse(state["sol_enabled"])
        self.assertEqual(
            list(
                (
                    store.production_canary_queue_root
                    / "cs408"
                    / expected_activation_id
                ).glob("*.json")
            ),
            [],
        )
        exclusion_root = (
            store.production_canary_excluded_root
            / "cs408"
            / expected_activation_id
        )
        exclusions = sorted(exclusion_root.glob("*.json"))
        self.assertEqual(len(exclusions), 4)
        for path in exclusions:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(receipt["activation_id"], expected_activation_id)
            self.assertEqual(receipt["classification"], "pre_activation_frozen")
            self.assertEqual(
                receipt["authority"]["purpose"],
                "dispatch-production-canary-exclusion",
            )
            self.assertEqual(receipt["formal_write_count"], 0)

        exclusions_by_unit = {
            receipt["producer_unit_id"]: receipt
            for receipt in (
                json.loads(path.read_text(encoding="utf-8"))
                for path in exclusions
            )
        }
        canary_state_path = store.production_canary_state_root / "cs408.json"
        stable_state_bytes = canary_state_path.read_bytes()
        stable_state = json.loads(stable_state_bytes.decode("utf-8"))
        from dashboard_projection import _canonical_bytes, _public_canary_gate

        for _scan_round in range(2):
            with mock.patch.object(
                runtime.dispatcher,
                "submit",
                side_effect=AssertionError(
                    "historical scan submitted Luna work"
                ),
            ) as submit, mock.patch(
                "preprocess_dispatcher._write_subject_projections"
            ) as project:
                handles, daemon_decisions = runtime.scan_and_submit()
            submit.assert_not_called()
            project.assert_called_once()
            self.assertEqual(handles, [])
            historical_decisions = [
                decision
                for decision in daemon_decisions
                if decision.get("capture_id") in diagnostics
            ]
            self.assertEqual(len(historical_decisions), 4)
            for decision in historical_decisions:
                receipt = exclusions_by_unit[decision["capture_id"]]
                self.assertEqual(
                    decision["unit_sha256"], receipt["unit_sha256"]
                )
                self.assertEqual(
                    decision["frozen_payload_sha256"],
                    receipt["frozen_payload_sha256"],
                )
                self.assertEqual(decision["reason"], "pre_activation_frozen")
                self.assertEqual(
                    decision["phase"], "producer_queue_excluded"
                )
                self.assertRegex(
                    decision["evidence_readiness_receipt_sha256"],
                    r"^[0-9a-f]{64}$",
                )
                self.assertFalse(decision["model_enqueue_allowed"])
            self.assertEqual(canary_state_path.read_bytes(), stable_state_bytes)
            heartbeat_gate = project.call_args.kwargs["lease_status"][
                "canary_gate"
            ]
            raw_status_gate = store.production_canary_status_read_only(
                "cs408"
            )
            self.assertEqual(heartbeat_gate, raw_status_gate)
            heartbeat_public_gate = _public_canary_gate(
                heartbeat_gate,
                subject="cs408",
                release_id=release_id,
            )
            raw_public_gate = _public_canary_gate(
                raw_status_gate,
                subject="cs408",
                release_id=release_id,
            )
            self.assertEqual(heartbeat_public_gate, raw_public_gate)
            self.assertEqual(
                heartbeat_public_gate["state_authority_sha256"],
                hashlib.sha256(
                    _canonical_bytes(stable_state["authority"])
                ).hexdigest(),
            )
            self.assertEqual(
                heartbeat_gate["authority"], stable_state["authority"]
            )
            self.assertEqual(
                heartbeat_gate["updated_at"], stable_state["updated_at"]
            )
        after_daemon_scan = store.production_canary_status_read_only("cs408")
        self.assertIsNotNone(after_daemon_scan)
        self.assertEqual(after_daemon_scan["state"], "armed")
        self.assertEqual(after_daemon_scan["historical_eligible_count"], 4)
        self.assertEqual(after_daemon_scan["excluded_by_high_watermark_count"], 4)
        self.assertEqual(after_daemon_scan["canary_queue_count"], 0)
        self.assertEqual(after_daemon_scan["queue_depth"], 0)
        for key in (
            "model_call_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "observed_model_call_count",
            "observed_provider_request_count",
            "observed_mcp_tool_call_count",
            "formal_write_count",
        ):
            self.assertEqual(after_daemon_scan[key], 0, key)
        self.assertFalse(after_daemon_scan["sol_enabled"])
        self.assertIsNone(
            after_daemon_scan["last_preclaim_failure_receipt_sha256"]
        )
        preclaim_root = (
            store.production_canary_preclaim_failure_root
            / "cs408"
            / expected_activation_id
        )
        self.assertEqual(list(preclaim_root.glob("*.json")), [])

        historical_candidates = list(candidates)
        conflicting_candidate = Candidate(
            **{
                **historical_candidates[0].__dict__,
                "input_fingerprint": "e" * 64,
            }
        )
        candidates[:] = [conflicting_candidate]
        conflicting_units, _ = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            publish_evidence_readiness=False,
        )
        self.assertEqual(len(conflicting_units), 1)
        resigned_inspection = store.inspect_production_canary_task_read_only(
            "cs408", conflicting_units[0].task
        )
        self.assertEqual(
            resigned_inspection["classification"], "pre_activation_frozen"
        )
        self.assertTrue(resigned_inspection["persisted"])
        original_receipt = next(
            json.loads(path.read_text(encoding="utf-8"))
            for path in exclusions
            if json.loads(path.read_text(encoding="utf-8"))[
                "producer_unit_id"
            ]
            == resigned_inspection["producer_unit_id"]
        )
        resigned_receipt = (
            store.record_production_canary_pre_activation_exclusion(
                conflicting_units[0].task
            )
        )
        self.assertEqual(resigned_receipt, original_receipt)
        self.assertEqual(len(list(exclusion_root.glob("*.json"))), 4)
        resigned_state = store.production_canary_status_read_only("cs408")
        self.assertIsNotNone(resigned_state)
        self.assertEqual(resigned_state["historical_eligible_count"], 4)
        candidates[:] = historical_candidates

        def read_only_scan(config_value, subject_value, **kwargs):
            return scan_eligible_candidates(
                config_value,
                subject_value,
                worker_factory=ScanWorker,
                **kwargs,
            )

        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            side_effect=read_only_scan,
        ):
            audit = _audit(config, "cs408")
        self.assertTrue(audit["read_only"])
        self.assertEqual(audit["historical_eligible_count"], 4)
        self.assertEqual(audit["excluded_by_high_watermark_count"], 4)
        self.assertEqual(audit["canary_queue_count"], 0)
        self.assertEqual(
            audit["historical_eligible_count"],
            audit["canary_gate"]["historical_eligible_count"],
        )
        self.assertEqual(
            audit["excluded_by_high_watermark_count"],
            audit["canary_gate"]["excluded_by_high_watermark_count"],
        )

        target_manifest = self.runtime / "target-release.json"
        target_release_id = "d" * 64
        atomic_write_json(
            target_manifest,
            {
                "schema_version": RELEASE_SCHEMA,
                "release_id": target_release_id,
            },
        )
        target_config = copy.deepcopy(config)
        target_config["release"] = {"manifest_path": str(target_manifest)}
        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            side_effect=read_only_scan,
        ):
            target_audit = _audit(target_config, "cs408")
        self.assertTrue(target_audit["read_only"])
        self.assertEqual(target_audit["eligible_count"], 4)
        self.assertEqual(target_audit["historical_eligible_count"], 4)
        self.assertEqual(target_audit["excluded_by_high_watermark_count"], 4)
        self.assertEqual(target_audit["canary_queue_count"], 0)
        self.assertIsNone(target_audit["canary_gate"])
        self.assertEqual(
            {row["phase"] for row in target_audit["decisions"]},
            {"target_release_preflight_read_only"},
        )
        self.assertEqual(audit["model_call_count"], 0)
        self.assertEqual(audit["provider_request_count"], 0)
        self.assertEqual(audit["formal_write_count"], 0)

        candidates[:] = [core_candidate(899)]
        crash_units, _ = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            publish_evidence_readiness=False,
        )
        self.assertEqual(len(crash_units), 1)
        crash_task = crash_units[0].task
        with mock.patch.object(
            store,
            "_write_canary_state_locked",
            side_effect=OSError("synthetic receipt-first crash window"),
        ):
            with self.assertRaisesRegex(
                OSError, "synthetic receipt-first crash window"
            ):
                store.record_production_canary_pre_activation_exclusion(
                    crash_task
                )
        stale_projection = store.production_canary_status_read_only("cs408")
        self.assertIsNotNone(stale_projection)
        self.assertEqual(stale_projection["historical_eligible_count"], 4)
        exclusion_root = (
            store.production_canary_excluded_root
            / "cs408"
            / expected_activation_id
        )
        self.assertEqual(len(list(exclusion_root.glob("*.json"))), 5)
        with mock.patch.object(
            store,
            "_write_canary_state_locked",
            wraps=store._write_canary_state_locked,
        ) as reconcile_write:
            store.record_production_canary_pre_activation_exclusion(crash_task)
        reconcile_write.assert_called_once()
        reconciled = store.production_canary_status_read_only("cs408")
        self.assertIsNotNone(reconciled)
        self.assertEqual(reconciled["historical_eligible_count"], 5)
        self.assertEqual(reconciled["excluded_by_high_watermark_count"], 5)

        post_activation = core_candidate(900)
        candidates[:] = [
            Candidate(
                **{
                    **post_activation.__dict__,
                    "recorded_at": "2026-08-10T00:00:01Z",
                }
            )
        ]
        with mock.patch(
            "preprocess_dispatcher.Worker",
            return_value=SimpleNamespace(release_id=release_id),
        ), mock.patch(
            "preprocess_dispatcher.producer_authority_binding",
            return_value=authority,
        ), mock.patch.object(
            runtime.dispatcher,
            "submit",
            side_effect=AssertionError("activation submitted Luna work"),
        ) as submit:
            queued_state = runtime.activate_production_canary(
                activated_at=activated_at,
                expected_activation_id=expected_activation_id,
                expected_producer_authority_fingerprint=authority[
                    "authority_fingerprint"
                ],
            )
        submit.assert_not_called()
        self.assertEqual(queued_state["historical_eligible_count"], 5)
        self.assertEqual(queued_state["excluded_by_high_watermark_count"], 5)
        self.assertEqual(queued_state["canary_queue_count"], 0)
        self.assertEqual(queued_state["queue_depth"], 0)
        queue_entries = list(
            (
                store.production_canary_queue_root
                / "cs408"
                / expected_activation_id
            ).glob("*.json")
        )
        self.assertEqual(queue_entries, [])
        task_root = (
            self.runtime
            / "dispatch"
            / "production-canary"
            / "tasks"
            / "cs408"
        )
        self.assertEqual(list(task_root.rglob("*.json")), [])
        self.assertEqual(queued_state["model_call_count"], 0)
        self.assertEqual(queued_state["provider_request_count"], 0)
        self.assertEqual(queued_state["mcp_tool_call_count"], 0)
        self.assertEqual(queued_state["formal_write_count"], 0)

        post_units, _ = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            publish_evidence_readiness=False,
        )
        self.assertEqual(len(post_units), 1)
        old_queue_entry = store.materialize_production_canary_task(
            post_units[0].task
        )
        self.assertEqual(old_queue_entry["activation_id"], expected_activation_id)
        old_queue_root = (
            store.production_canary_queue_root
            / "cs408"
            / expected_activation_id
        )
        self.assertEqual(len(list(old_queue_root.glob("*.json"))), 1)

        candidates[:] = [core_candidate(901)]
        inactive_units, _ = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            publish_evidence_readiness=False,
        )
        self.assertEqual(len(inactive_units), 1)
        store.deactivate_production_canary(
            "cs408", expected_release_id=release_id
        )
        with self.assertRaises(DispatchError) as inactive_error:
            store.record_production_canary_pre_activation_exclusion(
                inactive_units[0].task
            )
        self.assertEqual(inactive_error.exception.code, "production_canary_inactive")
        self.assertEqual(len(list(exclusion_root.glob("*.json"))), 5)

        candidates[:] = []
        successor_activated_at = "2026-08-10T00:00:02Z"
        successor_high_watermark = {
            **high_watermark,
            "recorded_at": successor_activated_at,
        }
        successor_high_watermark_sha256 = hashlib.sha256(
            canonical_bytes(successor_high_watermark)
        ).hexdigest()
        successor_activation_id = hashlib.sha256(
            canonical_bytes(
                {
                    "schema_version": (
                        "study-intake-production-canary-activation-receipt-v2"
                    ),
                    "subject": "cs408",
                    "release_id": release_id,
                    "activated_at": successor_activated_at,
                    "producer_authority_fingerprint": authority[
                        "authority_fingerprint"
                    ],
                    "producer_high_watermark_sha256": (
                        successor_high_watermark_sha256
                    ),
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                }
            )
        ).hexdigest()
        with mock.patch(
            "preprocess_dispatcher.Worker",
            return_value=SimpleNamespace(release_id=release_id),
        ), mock.patch(
            "preprocess_dispatcher.producer_authority_binding",
            return_value=authority,
        ):
            successor_state = runtime.activate_production_canary(
                activated_at=successor_activated_at,
                expected_activation_id=successor_activation_id,
                expected_producer_authority_fingerprint=authority[
                    "authority_fingerprint"
                ],
            )
        self.assertEqual(successor_state["historical_eligible_count"], 0)
        self.assertEqual(successor_state["excluded_by_high_watermark_count"], 0)
        self.assertEqual(successor_state["canary_queue_count"], 0)
        self.assertEqual(successor_state["queue_depth"], 0)
        self.assertEqual(store.pending_production_canary_tasks("cs408"), [])
        self.assertEqual(len(list(old_queue_root.glob("*.json"))), 1)
        successor_queue_root = (
            store.production_canary_queue_root
            / "cs408"
            / successor_activation_id
        )
        self.assertEqual(list(successor_queue_root.glob("*.json")), [])
        self.assertTrue(exclusion_root.exists())
        self.assertEqual(len(list(exclusion_root.glob("*.json"))), 5)
        successor_exclusion_root = (
            store.production_canary_excluded_root
            / "cs408"
            / successor_activation_id
        )
        self.assertEqual(list(successor_exclusion_root.glob("*.json")), [])

    def test_all_subjects_rescan_within_one_second_without_concurrency_cap(
        self,
    ) -> None:
        config = {
            "worker": {
                "poll_interval_seconds": 60,
                "english_poll_interval_seconds": 5,
            }
        }
        self.assertEqual(MAX_DISCOVERY_LATENCY_SECONDS, 1.0)
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                self.assertLessEqual(_poll_interval(config, subject), 1.0)
        self.assertEqual(_poll_interval({"worker": {}}, "math"), 1.0)
        with self.assertRaisesRegex(
            DispatchError, "discovery_poll_interval_invalid"
        ):
            _poll_interval({"worker": {"poll_interval_seconds": 0}}, "math")

    def test_core_scan_exposes_evidence_pending_without_submitting_model(self) -> None:
        contract = Path(self.temp.name) / "controlled-contract.json"
        contract.write_text("{}\n", encoding="utf-8")
        readiness = {
            "schema_version": "current-question-evidence-readiness-receipt-v1",
            "status": "evidence_pending",
            "capture_id": "CAP-PENDING",
            "evidence_manifest_sha256": "1" * 64,
            "evidence_bundle_sha256": "2" * 64,
            "question_mode": "image_question",
            "attachment_rows_sha256": "3" * 64,
            "required_roles": ["question_image", "solution_image"],
            "present_roles": ["question_image"],
            "missing_roles": ["solution_image"],
            "trace_full_sha256": "4" * 64,
            "controlled_contract_path": str(contract.resolve()),
            "controlled_contract_sha256": "5" * 64,
            "producer_build_sha256": "6" * 64,
            "created_at": "2026-08-05T00:00:00Z",
            "model_enqueue_allowed": False,
            "formal_write_count": 0,
        }

        class ScanWorker:
            release_id = "a" * 64

            def __init__(self, _config):
                self.adapters = {
                    "cs408": SimpleNamespace(
                        candidate_diagnostics={"CAP-PENDING": readiness},
                        candidate_errors={"CAP-PENDING": "evidence_pending"},
                    )
                }

            def eligible_candidates(
                self, _subject, _study_date, *, mutate_recovery_state
            ):
                self.mutate_recovery_state = mutate_recovery_state
                return []

        config = {
            "timezone": "Asia/Shanghai",
            "runtime_root": str(self.runtime),
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        before = {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }
        read_only_frozen, read_only_decisions = scan_eligible_candidates(
            config,
            "cs408",
            worker_factory=ScanWorker,
            publish_evidence_readiness=False,
        )
        after = {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }
        self.assertEqual(after, before)
        self.assertEqual(read_only_frozen, [])
        self.assertIsNone(
            read_only_decisions[0]["evidence_readiness_receipt_sha256"]
        )
        frozen, decisions = scan_eligible_candidates(
            config, "cs408", worker_factory=ScanWorker
        )
        self.assertEqual(frozen, [])
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0]["eligible"])
        self.assertEqual(decisions[0]["reason"], "evidence_pending")
        self.assertFalse(decisions[0]["model_enqueue_allowed"])
        self.assertRegex(decisions[0]["unit_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            decisions[0]["frozen_payload_sha256"], r"^[0-9a-f]{64}$"
        )
        receipt_sha256 = decisions[0][
            "evidence_readiness_receipt_sha256"
        ]
        verified = LeaseStore(self.runtime).verify_evidence_readiness(
            receipt_sha256,
            expected_release_id="a" * 64,
            expected_capture_id="CAP-PENDING",
        )
        self.assertFalse(
            verified["authority_receipt"]["receipt"][
                "model_enqueue_allowed"
            ]
        )

    def test_core_runner_uses_isolated_workers_and_public_claim_bridge(self) -> None:
        candidate = core_candidate(21)
        task = FrozenTask.from_candidate(candidate)
        task_payload = dict(task.frozen_payload)
        task_payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            **dispatch_rule_binding(
                release_id="b" * 64,
                subject="cs408",
                subject_processing_contract_sha256="9" * 64,
            ),
            "dispatch_reason": "eligible",
        }
        task = FrozenTask(task_payload)
        refs = {
            "analysis": "mcp-item:cs408:artifact:analysis-fixture",
            "critical_review": "mcp-item:cs408:artifact:critic-fixture",
        }

        def stage_receipt(stage_name: str) -> dict[str, Any]:
            grounding_core = {
                "schema_version": "model_mcp_grounding_manifest_v1",
                "items": [
                    {
                        "evidence_ref": refs[stage_name],
                        "subject": "cs408",
                        "generation": "cs408-fixture-generation",
                    }
                ],
                "item_count": 1,
                "host_semantic_prefetch": False,
                "formal_write_count": 0,
            }
            grounding_sha256 = hashlib.sha256(
                json.dumps(
                    grounding_core,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            return {
                "status": "ready",
                "requested_model": REQUIRED_MODEL,
                "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
                "runtime_model": REQUIRED_MODEL,
                "runtime_reasoning_effort": REQUIRED_REASONING_EFFORT,
                "runtime_metadata_provenance": "codex_json_attestation_v1",
                "runtime_identity_status": "confirmed",
                "duration_ms": 2,
                "processing_binding": {"candidate_release_id": "b" * 64},
                "read_session_id": "READ-CS408-FIXTURE",
                "read_session_manifest_sha256": "3" * 64,
                "authority_snapshot_manifest_sha256": "5" * 64,
                "capture_freeze_receipt_sha256": "6" * 64,
                "mcp_read_session_receipt_sha256": "7" * 64,
                "mcp_transcript_sha256": (
                    "8" * 64 if stage_name == "analysis" else "9" * 64
                ),
                "evidence_generation": "cs408-fixture-generation",
                "evidence_authority_fingerprint": "4" * 64,
                "mcp_grounding_manifest": {
                    **grounding_core,
                    "manifest_sha256": grounding_sha256,
                },
                "mcp_grounding_manifest_sha256": grounding_sha256,
                "semantic_stage_count": 1,
                "provider_request_count": 2,
                "mcp_tool_call_count": 1,
                "consumed_terminal_duplicate_read_count": 0,
            }

        result = ModelResult(
            analysis={"revised": True},
            duration_ms=5,
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            pipeline_status="two_pass_ready",
            draft_analysis={"draft": True, "evidence_refs": [refs["analysis"]]},
            critical_review={
                "review": True,
                "evidence_refs": [refs["critical_review"]],
            },
            stage_receipts={
                stage_name: stage_receipt(stage_name)
                for stage_name in ("analysis", "critical_review")
            },
        )
        workers: list[Any] = []
        process_calls: list[tuple[str, bool]] = []

        class FakeModelRunner:
            def run(self, _candidate):
                return result

            def cancel_active(self):
                return None

        class FakeWorker:
            release_id = "b" * 64

            def __init__(self, _config, *, model_runner=None):
                self.runner = model_runner or FakeModelRunner()
                self._math_v2_group_result_cache = {}
                workers.append(self)

            def _run_candidate_model(self, row):
                return self.runner.run(row)

            def process_claimed_candidate(
                self, row, reason, *, write_dashboard
            ):
                process_calls.append((reason, write_dashboard))
                self.runner.run(row)
                return {"status": "two_pass_ready"}

        store = LeaseStore(self.runtime)
        decision = store.claim(task.unit_sha256, "bridge-owner", subject="cs408")
        context = TaskExecutionContext(
            task,
            decision.lease,
            self.runtime / "context",
            1,
        )
        runner = CoreCandidateRunner(
            {
                "model": {
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                }
            },
            candidate,
            "eligible",
            store,
            worker_factory=FakeWorker,
        )
        analysis = runner.run_analysis(task, context)
        critical = runner.run_critical_review(
            task, analysis.payload, context
        )
        self.assertEqual(analysis.payload["draft"], True)
        self.assertEqual(critical.payload["review"], True)
        self.assertEqual(len(workers), 1)
        self.assertEqual(process_calls, [("eligible", False)])

    def test_core_runner_rejects_worker_result_after_cancel_race(self) -> None:
        candidate = core_candidate(22)
        base = FrozenTask.from_candidate(candidate)
        payload = dict(base.frozen_payload)
        payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            **dispatch_rule_binding(
                release_id="b" * 64,
                subject="cs408",
                subject_processing_contract_sha256="9" * 64,
            ),
            "dispatch_reason": "eligible",
        }
        task = FrozenTask(payload)
        store = LeaseStore(self.runtime)
        decision = store.claim(task.unit_sha256, "cancel-race", subject="cs408")
        context = TaskExecutionContext(
            task, decision.lease, self.runtime / "cancel-race-context", 1
        )

        class FakeModelRunner:
            def cancel_active(self):
                return None

        class CancelAtReturnWorker:
            release_id = "b" * 64

            def __init__(self, _config, *, model_runner=None):
                self.runner = model_runner or FakeModelRunner()

            def process_claimed_candidate(
                self, _row, _reason, *, write_dashboard
            ):
                self.assert_false = write_dashboard
                context.cancel("analysis_timeout")
                return {"status": "two_pass_ready", "package_path": "late"}

        runner = CoreCandidateRunner(
            {
                "model": {
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                }
            },
            candidate,
            "eligible",
            store,
            worker_factory=CancelAtReturnWorker,
        )
        with self.assertRaisesRegex(DispatchError, "dispatch_cancelled"):
            runner.run_analysis(task, context)
        self.assertIsNone(runner._published)
        self.assertFalse(store._completion_path(task.unit_sha256).exists())

    def test_authoritative_completion_hmac_and_forged_receipt_rejected(self) -> None:
        base = frozen_task(30, subject="math")
        payload = dict(base.frozen_payload)
        release_id = "c" * 64
        payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            "release_id": release_id,
        }
        task = FrozenTask(payload)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "succeeded")
        verified = dispatcher.lease_store.verify_authoritative_completion(
            "math", "CAP-0030", expected_release_id=release_id
        )
        self.assertEqual(
            verified["receipt"]["model_contract"]["model"], REQUIRED_MODEL
        )
        self.assertEqual(verified["completion"]["lease_fence"], 1)
        key_path = self.runtime / "dispatch" / "state" / "authority.key"
        self.assertEqual(oct(key_path.stat().st_mode & 0o777), "0o600")

        receipt_path = Path(verified["completion"]["receipt_path"])
        forged = dict(verified["receipt"])
        forged["outcome"] = "forged"
        os.chmod(receipt_path, 0o600)
        receipt_path.write_text(
            json.dumps(
                forged,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            DispatchError, "receipt_content_hash_mismatch"
        ):
            dispatcher.lease_store.verify_authoritative_completion(
                "math", "CAP-0030", expected_release_id=release_id
            )

    def test_task_events_are_state_changes_only_and_detail_is_lightweight(self) -> None:
        base = frozen_task(31, subject="english")
        task_payload = dict(base.frozen_payload)
        release_id = "d" * 64
        task_payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            **dispatch_rule_binding(
                release_id=release_id,
                subject="english",
                subject_processing_contract_sha256="e" * 64,
            ),
        }
        task = FrozenTask(task_payload)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: BehaviorRunner("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "succeeded")
        event_root = (
            self.runtime
            / "dispatch"
            / "state"
            / "task-events"
            / task.unit_sha256
            / "fence-1"
        )
        indexes = [json.loads(path.read_text()) for path in sorted(event_root.glob("*.json"))]
        self.assertEqual(
            [row["event"] for row in indexes],
            [
                "claim",
                "process_started",
                "analysis_submitted",
                "analysis_completed",
                "critical_started",
                "critical_completed",
                "published",
            ],
        )
        self.assertNotIn("heartbeat", [row["event"] for row in indexes])
        detail_path = (
            self.runtime
            / "dispatch"
            / "state"
            / "task-details"
            / f"{task.unit_sha256}.json"
        )
        detail = json.loads(detail_path.read_text())
        self.assertEqual(detail["phase"], "published")
        self.assertEqual(detail["server_queue_status"], "unknown")
        self.assertEqual(
            detail["rule_version_sha256"],
            task_payload["dispatch_contract"]["rule_version_sha256"],
        )
        self.assertEqual(detail["model"], "gpt-5.6-luna")
        self.assertEqual(detail["reasoning_effort"], "max")
        history = dispatcher.lease_store.verify_task_event_history(
            task.unit_sha256,
            expected_release_id=release_id,
        )
        self.assertEqual(history["model_call_count"], 2)
        self.assertEqual(
            [row["stage"] for row in history["model_submissions"]],
            ["analysis", "critical_review"],
        )
        self.assertEqual(history["formal_write_count"], 0)
        serialized = json.dumps(detail)
        self.assertNotIn("prompt", serialized)
        self.assertNotIn("private_trace", serialized)
        verified = dispatcher.lease_store.verify_authoritative_task_detail(
            task.unit_sha256, expected_release_id=release_id
        )
        self.assertEqual(verified["latest_event"]["event"], "published")

    def test_failed_analysis_has_one_verified_model_submission(self) -> None:
        base = frozen_task(311, subject="english")
        payload = dict(base.frozen_payload)
        release_id = "9" * 64
        payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            **dispatch_rule_binding(
                release_id=release_id,
                subject="english",
                subject_processing_contract_sha256="8" * 64,
            ),
        }
        task = FrozenTask(payload)

        class FailingAnalysis(BehaviorRunner):
            def run_analysis(self, current_task, context):
                del current_task, context
                raise DispatchError("synthetic_analysis_failure")

        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: FailingAnalysis("ok"),
            stage_timeout_seconds=1,
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "failed")
        history = dispatcher.lease_store.verify_task_event_history(
            task.unit_sha256,
            expected_release_id=release_id,
        )
        self.assertEqual(history["model_call_count"], 1)
        self.assertEqual(
            [row["stage"] for row in history["model_submissions"]],
            ["analysis"],
        )
        verified = dispatcher.lease_store.verify_authoritative_task_detail(
            task.unit_sha256, expected_release_id=release_id
        )
        self.assertEqual(verified["latest_event"]["event"], "failed")

    def _production_process_task(
        self,
        index: int,
        behavior: str,
        *,
        error_code: str | None = None,
    ) -> FrozenTask:
        base = frozen_task(index, subject="cs408")
        payload = dict(base.frozen_payload)
        payload["test_behavior"] = behavior
        if error_code is not None:
            payload["test_error_code"] = error_code
        return FrozenTask(payload)

    def _assert_production_process_batch(self, count: int) -> None:
        marker_root = self.runtime / f"markers-{count}"
        config_path = self.runtime / f"fake-config-{count}.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=5,
            )
            tasks = [
                self._production_process_task(index + 100, "block_all")
                for index in range(count)
            ]
            handles = dispatcher.dispatch(tasks, wait=False)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                markers = list(marker_root.glob("*.json"))
                if len(markers) == count:
                    break
                time.sleep(0.01)
            self.assertEqual(len(markers), count)
            pids = {json.loads(path.read_text())["pid"] for path in markers}
            self.assertEqual(len(pids), count)
            context_roots = {
                json.loads(path.read_text())["context_root"]
                for path in markers
            }
            self.assertEqual(len(context_roots), count)
            for task, context_root in zip(tasks, sorted(context_roots)):
                self.assertIsNotNone(context_root)
            self.assertTrue(
                all(
                    str(self.runtime / "dispatch" / "contexts")
                    in str(context_root)
                    for context_root in context_roots
                )
            )
            (marker_root / "release-all").touch()
            results = [handle.wait(5) for handle in handles]
            self.assertEqual(
                [result.outcome for result in results], ["succeeded"] * count
            )
            first_event_root = (
                self.runtime
                / "dispatch"
                / "state"
                / "task-events"
                / tasks[0].unit_sha256
                / "fence-1"
            )
            production_events = [
                json.loads(path.read_text())["event"]
                for path in sorted(first_event_root.glob("*.json"))
            ]
            self.assertEqual(
                production_events,
                [
                    "claim",
                    "process_started",
                    "published",
                ],
            )
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker

    def test_ten_production_tasks_have_ten_unique_runner_processes(self) -> None:
        self._assert_production_process_batch(10)

    def test_twenty_production_tasks_have_twenty_unique_runner_processes(self) -> None:
        self._assert_production_process_batch(20)

    def test_cancel_set_before_runner_launch_still_seals_identity_and_exit(
        self,
    ) -> None:
        marker_root = self.runtime / "cancel-before-launch-markers"
        config_path = self.runtime / "cancel-before-launch-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        base = frozen_task(199, subject="math")
        payload = dict(base.frozen_payload)
        payload["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            **dispatch_rule_binding(
                release_id="a" * 64,
                subject="math",
                subject_processing_contract_sha256="b" * 64,
            ),
            "dispatch_reason": "eligible",
        }
        task = FrozenTask(payload)
        store = LeaseStore(self.runtime)
        owner_id = f"dispatcher-{os.getpid()}-{'0' * 32}"
        decision = store.claim(
            task.unit_sha256,
            owner_id,
            subject="math",
            task=task,
        )
        self.assertIsNotNone(decision.lease)
        lease = decision.lease
        assert lease is not None
        context_root = (
            self.runtime
            / "dispatch"
            / "contexts"
            / task.unit_sha256
            / f"fence-{lease.fence}"
        )
        context_root.mkdir(parents=True, exist_ok=True)
        context = TaskExecutionContext(task, lease, context_root, 2)
        context.cancel("analysis_timeout")
        runner = CoreCandidateSubprocessRunner(
            config_path,
            "eligible",
            command=[
                sys.executable,
                str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
            ],
            lease_store=store,
        )
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            with self.assertRaisesRegex(DispatchError, "dispatch_cancelled"):
                runner.run_analysis(task, context)
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker
        identity_index = json.loads(
            (
                store.task_process_identity_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}.json"
            ).read_text(encoding="utf-8")
        )
        exit_index = json.loads(
            (
                store.task_process_exit_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}.json"
            ).read_text(encoding="utf-8")
        )
        exit_receipt = json.loads(
            Path(exit_index["task_process_exit_path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            exit_index["task_process_identity_sha256"],
            identity_index["process_identity_sha256"],
        )
        self.assertEqual(exit_receipt["termination_reason"], "timed_out")
        self.assertTrue(exit_receipt["reaped"])
        self.assertTrue(exit_receipt["process_absent"])
        self.assertTrue(exit_receipt["pgid_absent"])
        self.assertFalse(exit_receipt["late_result_publish_allowed"])
        self.assertFalse(marker_root.exists())

    def test_killing_one_production_process_does_not_kill_sibling(self) -> None:
        marker_root = self.runtime / "kill-markers"
        config_path = self.runtime / "fake-kill-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=5,
            )
            first_task = self._production_process_task(201, "block_unit")
            second_task = self._production_process_task(202, "block_unit")
            first = dispatcher.submit(first_task)
            second = dispatcher.submit(second_task)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and len(list(marker_root.glob("*.json"))) < 2:
                time.sleep(0.01)
            markers = {
                json.loads(path.read_text())["unit_sha256"]: json.loads(path.read_text())["pid"]
                for path in marker_root.glob("*.json")
            }
            self.assertEqual(len(markers), 2)
            self.assertTrue(first.cancel())
            self.assertEqual(first.wait(3).outcome, "cancelled")
            os.kill(markers[second_task.unit_sha256], 0)
            (marker_root / f"release-{second_task.unit_sha256}").touch()
            self.assertEqual(second.wait(3).outcome, "succeeded")
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker

    def test_blocking_fixture_exits_after_parent_pid_changes(self) -> None:
        marker_root = self.runtime / "parent-change-markers"
        config_path = self.runtime / "parent-change-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        unit = "f" * 64
        request = {
            "task": {
                "frozen_payload": {
                    "subject": "math",
                    "capture_id": "PARENT-CHANGE",
                    "allowed_evidence_refs": [],
                    "dispatch_contract": {},
                }
            },
            "unit_sha256": unit,
            "lease_fence": 1,
        }
        parent_code = """
import os
import subprocess
import sys
import time

fixture = subprocess.Popen(
    [sys.executable, sys.argv[1], "--config", sys.argv[2]],
    stdin=subprocess.PIPE,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
fixture.stdin.write(sys.argv[3].encode("utf-8"))
fixture.stdin.close()
while not os.path.exists(sys.argv[4]):
    time.sleep(0.01)
os._exit(0)
"""
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        old_behavior = os.environ.get("BLOCKING_DEFAULT_BEHAVIOR")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = "block_all"
        parent_process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                parent_code,
                str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
                str(config_path),
                json.dumps(request, sort_keys=True),
                str(marker_root / f"{unit}.json"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        registration = register_process(
            parent_process, require_private_group=True
        )
        child_pid = None
        child_pgid = None
        try:
            marker_path = marker_root / f"{unit}.json"
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not marker_path.exists():
                time.sleep(0.01)
            self.assertTrue(marker_path.exists())
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
            child_pid = int(marker["pid"])
            child_pgid = int(marker["pgid"])
            self.assertEqual(int(marker["parent_pid"]), registration.pid)
            parent_process.wait(timeout=2)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.01)
            else:
                self.fail(
                    f"blocking fixture survived parent change: "
                    f"pid={child_pid} pgid={child_pgid}"
                )
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                pass
            else:
                self.fail(f"blocking fixture still alive: pid={child_pid}")
        finally:
            if not process_absent(registration):
                stop_process(registration, term_timeout=0.5, kill_timeout=1)
            if child_pid is not None:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if child_pgid is not None and child_pgid == child_pid:
                try:
                    os.killpg(child_pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker
            if old_behavior is None:
                os.environ.pop("BLOCKING_DEFAULT_BEHAVIOR", None)
            else:
                os.environ["BLOCKING_DEFAULT_BEHAVIOR"] = old_behavior

    def test_production_process_explicit_cancel_isolated_from_healthy_sibling(self) -> None:
        marker_root = self.runtime / "timeout-markers"
        config_path = self.runtime / "fake-timeout-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=0.2,
            )
            blocked = self._production_process_task(203, "block_unit")
            healthy = self._production_process_task(204, "healthy")
            handles = dispatcher.dispatch([blocked, healthy], wait=False)
            by_unit = {handle.unit_sha256: handle for handle in handles}
            deadline = time.monotonic() + 5
            marker_path = marker_root / f"{blocked.unit_sha256}.json"
            while time.monotonic() < deadline and not marker_path.exists():
                time.sleep(0.01)
            self.assertTrue(marker_path.exists())
            self.assertTrue(by_unit[blocked.unit_sha256].cancel("user_cancelled"))
            self.assertEqual(
                by_unit[blocked.unit_sha256].wait(3).outcome, "cancelled"
            )
            self.assertEqual(
                by_unit[healthy.unit_sha256].wait(3).outcome, "succeeded"
            )
        finally:
            for path in marker_root.glob("*.json"):
                try:
                    pid = int(json.loads(path.read_text())["pid"])
                    os.killpg(pid, signal.SIGTERM)
                except (OSError, KeyError, TypeError, ValueError):
                    pass
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker

    def test_production_reported_timeout_is_terminal_timeout_event(self) -> None:
        marker_root = self.runtime / "reported-timeout-markers"
        config_path = self.runtime / "fake-reported-timeout-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=2,
            )
            task = self._production_process_task(
                205, "reported_timeout"
            )
            result = dispatcher.submit(task).wait(5)
            self.assertEqual(result.outcome, "timed_out")
            self.assertEqual(
                result.error_code, "cs408_critical_review_timeout"
            )
            event_root = (
                self.runtime
                / "dispatch/state/task-events"
                / task.unit_sha256
                / "fence-1"
            )
            events = [
                json.loads(path.read_text())["event"]
                for path in sorted(event_root.glob("*.json"))
            ]
            self.assertEqual(events[-1], "timeout")
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker

    def test_production_reported_rate_limit_waits_only_that_task(self) -> None:
        marker_root = self.runtime / "reported-limit-markers"
        config_path = self.runtime / "fake-reported-limit-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            limited = self._production_process_task(
                206, "reported_rate_limited"
            )
            healthy = self._production_process_task(207, "healthy")
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=2,
            )
            results = dispatcher.dispatch([limited, healthy])
            by_unit = {result.unit_sha256: result for result in results}
            self.assertEqual(
                by_unit[limited.unit_sha256].status, "retry_wait"
            )
            self.assertEqual(
                by_unit[limited.unit_sha256].outcome, "waiting_retry"
            )
            self.assertEqual(
                by_unit[healthy.unit_sha256].outcome, "succeeded"
            )
            lease = json.loads(
                (
                    self.runtime
                    / "dispatch/state/leases"
                    / f"{limited.unit_sha256}.json"
                ).read_text()
            )
            self.assertEqual(lease["status"], "retry_wait")
            self.assertEqual(lease["retry_kind"], "service_limit")
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker

    def test_prefixed_provider_wait_codes_are_retryable_but_task_errors_are_not(
        self,
    ) -> None:
        marker_root = self.runtime / "provider-code-markers"
        config_path = self.runtime / "provider-code-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text("{}\n", encoding="utf-8")
        command = [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "blocking_production_runner.py"),
        ]
        retry_codes = (
            "cs408_analysis_rate_limit_exceeded",
            "cs408_critical_review_upstream_http_429",
            "cs408_analysis_service_unavailable",
        )
        terminal_codes = (
            "cs408_analysis_usage_limit",
            "cs408_analysis_quota_exceeded",
            "cs408_analysis_evidence_invalid",
            "cs408_analysis_format_error",
            "cs408_analysis_timeout",
        )
        retry_tasks = [
            self._production_process_task(
                220 + index,
                "reported_error",
                error_code=code,
            )
            for index, code in enumerate(retry_codes)
        ]
        terminal_tasks = [
            self._production_process_task(
                230 + index,
                "reported_error",
                error_code=code,
            )
            for index, code in enumerate(terminal_codes)
        ]
        healthy = self._production_process_task(240, "healthy")
        old_marker = os.environ.get("BLOCKING_MARKER_ROOT")
        os.environ["BLOCKING_MARKER_ROOT"] = str(marker_root)
        try:
            dispatcher = ConcurrentDispatcher(
                self.runtime,
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path, "eligible", command=command
                ),
                stage_timeout_seconds=2,
            )
            results = dispatcher.dispatch(
                [*retry_tasks, *terminal_tasks, healthy]
            )
            by_unit = {result.unit_sha256: result for result in results}
            for task, code in zip(retry_tasks, retry_codes):
                result = by_unit[task.unit_sha256]
                self.assertEqual(result.status, "retry_wait", code)
                self.assertEqual(result.outcome, "waiting_retry", code)
                lease = json.loads(
                    dispatcher.lease_store._lease_path(
                        task.unit_sha256
                    ).read_text()
                )
                self.assertEqual(lease["retry_kind"], "service_limit")
            for task, code in zip(terminal_tasks, terminal_codes):
                result = by_unit[task.unit_sha256]
                self.assertEqual(result.status, "completed", code)
                self.assertNotEqual(result.outcome, "waiting_retry", code)
                lease = json.loads(
                    dispatcher.lease_store._lease_path(
                        task.unit_sha256
                    ).read_text()
                )
                self.assertEqual(lease["status"], "completed")
            self.assertEqual(by_unit[healthy.unit_sha256].outcome, "succeeded")
        finally:
            if old_marker is None:
                os.environ.pop("BLOCKING_MARKER_ROOT", None)
            else:
                os.environ["BLOCKING_MARKER_ROOT"] = old_marker


if __name__ == "__main__":
    unittest.main()
