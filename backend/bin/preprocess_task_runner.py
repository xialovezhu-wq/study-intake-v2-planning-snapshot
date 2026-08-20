#!/usr/bin/env python3
"""Isolated child process for one production frozen preprocessing task."""

from __future__ import annotations

import argparse
import copy
import dataclasses
import hashlib
import json
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import (  # noqa: E402
    DispatchError,
    FrozenTask,
    Lease,
    LeaseStore,
    validate_dispatch_rule_binding,
)
from execution_quality_contract import decide_execution_quality  # noqa: E402
from core_dispatch_bridge import (  # noqa: E402
    _CapturingRunner,
    _stage_result_from_model,
    content_processing_identity,
    validate_concurrent_cs408_candidate,
    validate_fixed_model_contract,
)
from preprocessor_core import (  # noqa: E402
    Candidate,
    ModelResult,
    PreprocessorError,
    Worker,
    load_config,
    math_group_processing_key,
)


class _CancellationCoordinator:
    """Signal-safe bridge from the supervisor PGID to the real Provider child."""

    def __init__(self) -> None:
        self.requested = threading.Event()
        self._lock = threading.Lock()
        self._runner: object | None = None
        self.result: Mapping[str, Any] | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="task-runner-provider-cancel",
            daemon=True,
        )
        self._thread.start()

    def request(self) -> None:
        self.requested.set()

    def bind(self, runner: object) -> None:
        with self._lock:
            if self._runner is not None:
                raise DispatchError("task_runner_cancel_runner_already_bound")
            self._runner = runner

    def _run(self) -> None:
        self.requested.wait()
        while True:
            with self._lock:
                runner = self._runner
            if runner is not None:
                break
            self.requested.wait(0.01)
        cancel = getattr(runner, "cancel_active", None)
        if not callable(cancel):
            self.result = {"requested": 0, "reaped": 0, "unconfirmed": 1}
            return
        reason = "daemon_signal"
        intent_path = os.environ.get("STUDY_PREPROCESS_CANCEL_INTENT_PATH")
        if intent_path:
            try:
                intent = json.loads(Path(intent_path).read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                intent = None
            if (
                isinstance(intent, Mapping)
                and intent.get("schema_version")
                == "study-intake-task-cancel-intent-v1"
                and intent.get("unit_sha256")
                == os.environ.get("STUDY_PREPROCESS_UNIT_SHA256")
                and str(intent.get("lease_fence"))
                == os.environ.get("STUDY_PREPROCESS_LEASE_FENCE")
                and isinstance(intent.get("reason"), str)
                and intent["reason"]
            ):
                reason = str(intent["reason"])
        try:
            value = cancel(timeout=6.0, reason=reason)
        except TypeError:
            # Compatibility for narrow unit doubles.  Production CodexRunner
            # accepts the explicit reason and seals it into the exit kind.
            value = cancel()
        self.result = value if isinstance(value, Mapping) else {
            "requested": 0,
            "reaped": 0,
            "unconfirmed": 1,
        }

    def join(self, timeout: float = 7.0) -> None:
        if self.requested.is_set():
            self._thread.join(timeout)


class _LivenessProbeCoordinator:
    """Answer supervisor nonce challenges from the live Provider data plane."""

    def __init__(self) -> None:
        self.requested = threading.Event()
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self._runner: object | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="task-runner-liveness-probe",
            daemon=True,
        )
        self._thread.start()

    def request(self) -> None:
        self.requested.set()

    def bind(self, runner: object) -> None:
        with self._lock:
            self._runner = runner

    @staticmethod
    def _paths() -> tuple[Path, Path] | None:
        raw_root = os.environ.get("STUDY_PREPROCESS_CONTEXT_ROOT")
        if not raw_root:
            return None
        root = Path(raw_root).resolve() / "stall-probes"
        return root / "request.json", root / "response.json"

    def _run(self) -> None:
        while not self.stop.is_set():
            if not self.requested.wait(0.1):
                continue
            self.requested.clear()
            paths = self._paths()
            with self._lock:
                runner = self._runner
            if paths is None or runner is None:
                continue
            request_path, response_path = paths
            try:
                request = json.loads(request_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if not isinstance(request, Mapping):
                continue
            nonce = request.get("nonce") if isinstance(request, Mapping) else None
            stage_name = (
                request.get("provider_stage_name")
                if isinstance(request, Mapping)
                else None
            )
            baseline_progress_receipt_sha256 = request.get(
                "baseline_progress_receipt_sha256"
            )
            provider_process_identity_sha256 = request.get(
                "provider_process_identity_sha256"
            )
            provider_pid = request.get("provider_pid")
            provider_pgid = request.get("provider_pgid")
            process_start_token = request.get("process_start_token")
            baseline_kernel_snapshot_sha256 = request.get(
                "baseline_kernel_snapshot_sha256"
            )
            if (
                request.get("schema_version")
                != "study-intake-task-liveness-probe-v1"
                or request.get("unit_sha256")
                != os.environ.get("STUDY_PREPROCESS_UNIT_SHA256")
                or str(request.get("lease_fence"))
                != os.environ.get("STUDY_PREPROCESS_LEASE_FENCE")
                or not isinstance(nonce, str)
                or len(nonce) != 32
                or any(char not in "0123456789abcdef" for char in nonce)
                or not isinstance(stage_name, str)
                or not isinstance(
                    provider_process_identity_sha256, str
                )
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    provider_process_identity_sha256,
                )
                is None
                or isinstance(provider_pid, bool)
                or not isinstance(provider_pid, int)
                or provider_pid <= 1
                or isinstance(provider_pgid, bool)
                or not isinstance(provider_pgid, int)
                or provider_pgid != provider_pid
                or not isinstance(process_start_token, str)
                or not process_start_token
                or not isinstance(
                    baseline_kernel_snapshot_sha256, str
                )
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    baseline_kernel_snapshot_sha256,
                )
                is None
                or (
                    baseline_progress_receipt_sha256 is not None
                    and (
                        not isinstance(
                            baseline_progress_receipt_sha256, str
                        )
                        or re.fullmatch(
                            r"[0-9a-f]{64}",
                            baseline_progress_receipt_sha256,
                        )
                        is None
                    )
                )
            ):
                continue
            callback = getattr(runner, "respond_liveness_probe", None)
            try:
                provider = (
                    callback(
                        stage_name=stage_name,
                        nonce=nonce,
                        baseline_progress_receipt_sha256=(
                            baseline_progress_receipt_sha256
                        ),
                        provider_process_identity_sha256=(
                            provider_process_identity_sha256
                        ),
                        provider_pid=provider_pid,
                        provider_pgid=provider_pgid,
                        process_start_token=process_start_token,
                        baseline_kernel_snapshot_sha256=(
                            baseline_kernel_snapshot_sha256
                        ),
                    )
                    if callable(callback)
                    else {"provider_data_plane_ok": False}
                )
            except BaseException:
                provider = {"provider_data_plane_ok": False}
            response = {
                "schema_version": "study-intake-task-liveness-probe-response-v1",
                "unit_sha256": request["unit_sha256"],
                "lease_fence": request["lease_fence"],
                "nonce": nonce,
                "nonce_sha256": hashlib.sha256(
                    nonce.encode("ascii")
                ).hexdigest(),
                "control_channel_ok": True,
                "provider_data_plane_ok": bool(
                    isinstance(provider, Mapping)
                    and provider.get("provider_data_plane_ok") is True
                ),
                "progress_receipt_sha256": (
                    provider.get("progress_receipt_sha256")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "provider_process_identity_sha256": (
                    provider.get("provider_process_identity_sha256")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "provider_pid": (
                    provider.get("provider_pid")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "provider_pgid": (
                    provider.get("provider_pgid")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "process_start_token": (
                    provider.get("process_start_token")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "baseline_kernel_snapshot_sha256": (
                    provider.get("baseline_kernel_snapshot_sha256")
                    if isinstance(provider, Mapping)
                    else None
                ),
                "responded_at_unix_ns": time.time_ns(),
                "formal_write_count": 0,
            }
            response_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temp = response_path.with_name(f".{response_path.name}.{nonce}.tmp")
            temp.write_text(
                json.dumps(response, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temp, response_path)

    def close(self) -> None:
        self.stop.set()
        self.requested.set()
        self._thread.join(timeout=1)


def _candidate_from_payload(value: Mapping[str, Any]) -> Candidate:
    try:
        return Candidate(
            subject=str(value["subject"]),
            capture_id=str(value["capture_id"]),
            study_date=str(value["study_date"]),
            recorded_at=(
                str(value["recorded_at"])
                if value.get("recorded_at") is not None
                else None
            ),
            input_fingerprint=str(value["input_fingerprint"]),
            input_binding=copy.deepcopy(dict(value["input_binding"])),
            model_input=copy.deepcopy(dict(value["model_input"])),
            allowed_evidence_refs=tuple(value["allowed_evidence_refs"]),
            image_paths=tuple(Path(path) for path in value["image_paths"]),
            target_label=str(value["target_label"]),
            canonical_state=str(value["canonical_state"]),
            sol_state=str(value["sol_state"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DispatchError("frozen_candidate_invalid") from exc


def _candidate_from_task(task: FrozenTask) -> Candidate:
    return _candidate_from_payload(task.frozen_payload)


class _ContentReuseRunner:
    """Reuse the owner's completed two-pass result without another model call."""

    def __init__(self, inner: _CapturingRunner) -> None:
        self.inner = inner
        self.reusable_result: ModelResult | None = None

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)

    def run(self, candidate: Candidate) -> ModelResult:
        if self.reusable_result is not None:
            return self.reusable_result
        return self.inner.run(candidate)

    def run_math_v2(self, candidate: Candidate) -> ModelResult:
        if self.reusable_result is not None:
            return self.reusable_result
        return self.inner.run_math_v2(candidate)

    def resume_critical(
        self,
        candidate: Candidate,
        *,
        draft_analysis: Mapping[str, Any],
        analysis_receipt: Mapping[str, Any],
    ) -> ModelResult:
        if self.reusable_result is not None:
            return self.reusable_result
        return self.inner.resume_critical(
            candidate,
            draft_analysis=draft_analysis,
            analysis_receipt=analysis_receipt,
        )


def _stage_value(
    payload: Mapping[str, Any],
    result: ModelResult,
    stage_name: str,
    *,
    expected_subject: str,
    expected_release_id: str,
) -> dict[str, Any]:
    validated = _stage_result_from_model(
        result,
        payload,
        stage_name,
        expected_subject=expected_subject,
        expected_release_id=expected_release_id,
    )
    return dataclasses.asdict(validated)


class _StageEventRecorder:
    """Record only real model-call boundaries in the authoritative task log."""

    _EVENTS = {
        "english_analysis": ("analysis_submitted", None),
        "english_critical_review": ("critical_started", "critical_completed"),
        "math_analysis": ("analysis_submitted", None),
        "math_critical_review": ("critical_started", "critical_completed"),
        "cs408_analysis": ("analysis_submitted", None),
        "cs408_critical_review": ("critical_started", "critical_completed"),
    }

    def __init__(
        self,
        runner: object,
        *,
        task: FrozenTask,
        lease: Lease,
        store: LeaseStore,
    ) -> None:
        execute = getattr(runner, "_execute_prompt", None)
        if not callable(execute):
            raise DispatchError("task_process_stage_event_runner_invalid")
        self._execute = execute
        self.task = task
        self.lease = lease
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        stage_name = kwargs.get("stage_name")
        events = self._EVENTS.get(stage_name)
        if events is None:
            raise DispatchError("task_process_stage_name_invalid")
        self.store.record_task_event(self.task, self.lease, events[0])
        result = self._execute(*args, **kwargs)
        if events[1] is not None:
            self.store.record_task_event(self.task, self.lease, events[1])
        return result


def _checkpoint_artifact_refs(meta: Mapping[str, Any]) -> dict[str, str]:
    checkpoint_sha256 = meta.get("checkpoint_sha256")
    checkpoint_ref = meta.get("checkpoint_ref")
    binding_key = meta.get("binding_key")
    binding_sha256 = meta.get("binding_sha256")
    if (
        not isinstance(checkpoint_sha256, str)
        or len(checkpoint_sha256) != 64
        or checkpoint_ref
        != (
            "study-intake-analysis-checkpoint://sha256/"
            + checkpoint_sha256
        )
        or not isinstance(binding_key, str)
        or len(binding_key) != 64
        or not isinstance(binding_sha256, str)
        or len(binding_sha256) != 64
    ):
        raise DispatchError("analysis_checkpoint_event_metadata_invalid")
    for value in (checkpoint_sha256, binding_key, binding_sha256):
        if any(char not in "0123456789abcdef" for char in value):
            raise DispatchError(
                "analysis_checkpoint_event_metadata_invalid"
            )
    return {
        "checkpoint_sha256": checkpoint_sha256,
        "checkpoint_ref": checkpoint_ref,
        "checkpoint_binding_key": binding_key,
        "checkpoint_binding_sha256": binding_sha256,
    }


class _AnalysisCheckpointEventRecorder:
    """Emit analysis_completed only after validation and durable checkpoint."""

    def __init__(
        self,
        runner: object,
        *,
        task: FrozenTask,
        lease: Lease,
        store: LeaseStore,
    ) -> None:
        write_checkpoint = getattr(runner, "_write_analysis_checkpoint", None)
        if not callable(write_checkpoint):
            raise DispatchError("task_process_checkpoint_runner_invalid")
        self._write_checkpoint = write_checkpoint
        self.task = task
        self.lease = lease
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # The core atomic writer now holds the same dispatch lock while it
        # verifies this exact fence.  Wrapping it in run_if_current would take
        # the same file lock twice; the write guard is the single CAS boundary.
        if not self.store.is_current(self.lease):
            raise DispatchError("stale_lease_fence")
        meta = self._write_checkpoint(*args, **kwargs)
        if not isinstance(meta, Mapping):
            raise DispatchError("analysis_checkpoint_metadata_invalid")
        self.store.record_task_event(
            self.task,
            self.lease,
            "analysis_completed",
            artifact_refs=_checkpoint_artifact_refs(meta),
        )
        return meta


class _AnalysisCheckpointLoadRecorder:
    """Record reuse only after the core has verified the persisted checkpoint."""

    def __init__(
        self,
        runner: object,
        *,
        task: FrozenTask,
        lease: Lease,
        store: LeaseStore,
    ) -> None:
        load_checkpoint = getattr(runner, "load_analysis_checkpoint", None)
        if not callable(load_checkpoint):
            raise DispatchError("task_process_checkpoint_loader_invalid")
        self._load_checkpoint = load_checkpoint
        self.task = task
        self.lease = lease
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        result = self._load_checkpoint(*args, **kwargs)
        if (
            not isinstance(result, tuple)
            or len(result) != 3
            or not isinstance(result[2], Mapping)
        ):
            raise DispatchError("analysis_checkpoint_load_result_invalid")
        self.store.record_task_event(
            self.task,
            self.lease,
            "analysis_checkpoint_reused",
            artifact_refs=_checkpoint_artifact_refs(result[2]),
        )
        return result


def run_request(
    config_path: Path,
    request: Mapping[str, Any],
    *,
    cancellation: _CancellationCoordinator | None = None,
    liveness_probes: _LivenessProbeCoordinator | None = None,
) -> dict[str, Any]:
    config = load_config(config_path)
    if (
        config.get("execution_mode") == "offline"
        and os.environ.get("STUDY_INTAKE_FIXTURE_EXECUTION") != "1"
    ):
        raise DispatchError("offline_task_runner_forbidden")
    validate_fixed_model_contract(config)
    raw_task = request.get("task")
    if not isinstance(raw_task, Mapping):
        raise DispatchError("task_process_request_invalid")
    task = FrozenTask.from_mapping(raw_task)
    fence = request.get("lease_fence")
    owner_id = request.get("lease_owner_id")
    execution_mode = request.get("execution_mode", "full_two_pass")
    runtime_root = Path(str(config["runtime_root"])).resolve()
    expected_context_root = (
        runtime_root
        / "dispatch"
        / "contexts"
        / task.unit_sha256
        / f"fence-{fence}"
    ).resolve()
    raw_context_root = os.environ.get("STUDY_PREPROCESS_CONTEXT_ROOT")
    context_root = (
        Path(raw_context_root).resolve()
        if isinstance(raw_context_root, str) and raw_context_root
        else None
    )
    if (
        request.get("schema_version") != "study-intake-production-task-request-v1"
        or request.get("unit_sha256") != task.unit_sha256
        or not isinstance(fence, int)
        or not isinstance(owner_id, str)
        or not owner_id
        or os.environ.get("STUDY_PREPROCESS_UNIT_SHA256") != task.unit_sha256
        or os.environ.get("STUDY_PREPROCESS_LEASE_FENCE") != str(fence)
        or os.environ.get("STUDY_PREPROCESS_LEASE_OWNER_ID") != owner_id
        or os.environ.get("STUDY_PREPROCESS_RUNTIME_ROOT")
        != str(runtime_root)
        or context_root != expected_context_root
        or not expected_context_root.is_dir()
        or execution_mode not in {"full_two_pass", "critical_resume"}
    ):
        raise DispatchError("task_process_request_binding_mismatch")
    model_config = config.get("model")
    if not isinstance(model_config, Mapping):
        raise DispatchError("task_process_model_config_invalid")
    config = dict(config)
    config["model"] = {
        **dict(model_config),
        "task_context_root": str(expected_context_root),
        "task_model_temp_root": str(
            expected_context_root / "model-output"
        ),
    }
    candidate = _candidate_from_task(task)
    validate_concurrent_cs408_candidate(candidate)
    worker = Worker(config)
    contract = task.frozen_payload.get("dispatch_contract")
    if (
        not isinstance(contract, Mapping)
        or contract.get("release_id") != worker.release_id
    ):
        raise DispatchError("task_process_release_mismatch")
    validate_dispatch_rule_binding(
        contract,
        subject=candidate.subject,
        require_processing_contract=True,
    )
    if contract.get("subject_processing_contract_sha256") != (
        candidate.input_binding.get("processing_contract_sha256")
    ):
        raise DispatchError(
            "task_process_processing_contract_binding_mismatch"
        )
    frozen_reason = contract.get("dispatch_reason")
    if (
        not isinstance(frozen_reason, str)
        or not frozen_reason
        or request.get("reason") != frozen_reason
    ):
        raise DispatchError("task_process_reason_binding_mismatch")
    lease = Lease(task.unit_sha256, owner_id, fence)
    lease_store = LeaseStore(Path(str(config["runtime_root"])))
    supervisor_launch_nonce = os.environ.get(
        "STUDY_PREPROCESS_PROCESS_LAUNCH_NONCE"
    )
    if supervisor_launch_nonce is not None:
        supervisor_sha256 = request.get(
            "supervisor_process_identity_sha256"
        )
        supervisor_path = request.get("supervisor_process_identity_path")
        if (
            not isinstance(supervisor_sha256, str)
            or not isinstance(supervisor_path, str)
            or request.get("supervisor_launch_nonce")
            != supervisor_launch_nonce
        ):
            raise DispatchError("task_process_identity_request_missing")
        lease_store.verify_task_process_identity(
            task,
            lease,
            expected_sha256=supervisor_sha256,
            expected_path=supervisor_path,
            expected_launch_nonce=supervisor_launch_nonce,
        )
    lifecycle_binder = getattr(
        worker.runner, "bind_dispatch_process_lifecycle", None
    )
    if not callable(lifecycle_binder):
        if supervisor_launch_nonce is not None:
            raise DispatchError("task_process_lifecycle_runner_invalid")
    else:
        lifecycle_binder(
            task=task,
            lease=lease,
            lease_store=lease_store,
        )
    if cancellation is not None:
        cancellation.bind(worker.runner)
    if liveness_probes is not None:
        liveness_probes.bind(worker.runner)
    if execution_mode == "critical_resume":
        if (
            fence < 2
            or not lease_store.resume_checkpoint_required(lease)
        ):
            raise DispatchError("task_process_resume_fence_invalid")
    worker.runner._execute_prompt = _StageEventRecorder(
        worker.runner,
        task=task,
        lease=lease,
        store=lease_store,
    )
    worker.runner._write_analysis_checkpoint = (
        _AnalysisCheckpointEventRecorder(
            worker.runner,
            task=task,
            lease=lease,
            store=lease_store,
        )
    )
    if execution_mode == "critical_resume":
        worker.runner.load_analysis_checkpoint = (
            _AnalysisCheckpointLoadRecorder(
                worker.runner,
                task=task,
                lease=lease,
                store=lease_store,
            )
        )
    capturing = _CapturingRunner(worker.runner)
    reuse_runner = _ContentReuseRunner(capturing)
    worker.runner = reuse_runner
    published = worker.process_claimed_candidate(
        candidate,
        (
            "infrastructure_resume"
            if execution_mode == "critical_resume"
            else frozen_reason
        ),
        write_dashboard=False,
    )
    expected_statuses = {
        "math": {
            "ready", "shadow_two_pass_ready", "two_pass_ready",
            "succeeded",
        },
        "cs408": {"two_pass_ready", "succeeded"},
        "english": {"ready", "succeeded"},
    }
    technical_review_failure = bool(
        published.get("status") == "failed"
        and published.get("report_disposition") == "quarantined"
        and isinstance(published.get("review_candidate_stage"), Mapping)
    )
    if (
        published.get("status") not in expected_statuses[candidate.subject]
        and not technical_review_failure
    ):
        error = published.get("last_error_code") or published.get("error_code")
        raise DispatchError(str(error or "core_publication_failed"))
    results = [row for row in capturing.results if isinstance(row, ModelResult)]
    if not results and published.get("status") in {"succeeded", "failed"}:
        review_stage = published.get("review_candidate_stage")
        if not isinstance(review_stage, Mapping):
            raise DispatchError("core_review_candidate_stage_missing")
        disposition = str(published.get("report_disposition") or "")
        review_result = review_stage.get("review_result")
        review_error_code = (
            review_result.get("error_code")
            if isinstance(review_result, Mapping)
            else None
        )
        quarantined = disposition == "quarantined"
        technical_error = (
            published.get("last_error_code")
            or published.get("error_code")
            or review_error_code
            or "review_candidate_identity_invalid"
            if quarantined
            else None
        )
        decision = decide_execution_quality(
            model_completed=bool(
                int(review_stage.get("model_call_count") or 0) >= 1
            ),
            provider_completed=bool(
                int(review_stage.get("provider_request_count") or 0) >= 2
            ),
            mcp_database_query_completed=bool(
                int(review_stage.get("mcp_tool_call_count") or 0) >= 1
            ),
            raw_output_reopenable=bool(
                review_stage.get("raw_output_object_sha256")
                and review_stage.get("raw_output_object_ref")
            ),
            report_reopenable=review_stage.get("report_available") is True,
            identity_verified=bool(
                review_stage.get("runtime_identity_status") == "confirmed"
                and not quarantined
            ),
            quality_findings_present=not quarantined,
            technical_error_code=(
                str(technical_error) if quarantined else None
            ),
            quarantined=quarantined,
        )
        if (
            published.get("status") != decision.job_status
            or disposition != decision.report_disposition
            or review_stage.get("execution_status")
            != decision.execution_status
            or review_stage.get("quality_status") != decision.quality_status
            or review_stage.get("report_available") is not True
            or review_stage.get("sol_review_status")
            != decision.sol_review_status
            or review_stage.get("formal_write_eligible") is not False
            or review_stage.get("production_accepted") is not False
            or (
                not quarantined
                and (
                    not isinstance(review_error_code, str)
                    or not review_error_code
                )
            )
        ):
            raise DispatchError("core_review_candidate_binding_invalid")
        return {
            "schema_version": "study-intake-production-task-result-v1",
            "unit_sha256": task.unit_sha256,
            "lease_fence": fence,
            "analysis": copy.deepcopy(dict(review_stage)),
            "critical_review": None,
            "terminal_outcome": decision.terminal_outcome,
            "terminal_error_code": decision.error_code,
            "terminal_quality_error_code": (
                str(review_error_code) if not quarantined else None
            ),
            "terminal_report_disposition": disposition,
            "terminal_review_stage_count": 1,
            **decision.publication_fields(),
            "member_publications": [copy.deepcopy(dict(published))],
            "formal_write_count": 0,
        }
    if not results:
        raise DispatchError("core_model_result_invalid")
    two_pass = [row for row in results if row.pipeline_status == "two_pass_ready"]
    quality_review = [
        row
        for row in results
        if row.pipeline_status == "quality_review_ready"
    ]
    result = (
        two_pass[-1]
        if two_pass
        else quality_review[-1]
        if quality_review
        else results[-1]
    )
    if not two_pass and not quality_review:
        raise DispatchError("core_two_pass_not_ready")
    reuse_runner.reusable_result = result
    member_publications: list[dict[str, Any]] = [
        copy.deepcopy(dict(published))
    ]
    raw_content_members = task.frozen_payload.get("content_group_members")
    if raw_content_members is not None:
        expected_content_id = task.frozen_payload.get(
            "content_processing_id"
        )
        expected_capture_ids = task.frozen_payload.get(
            "content_group_capture_ids"
        )
        if (
            not isinstance(raw_content_members, list)
            or not raw_content_members
            or not isinstance(expected_content_id, str)
            or not isinstance(expected_capture_ids, list)
        ):
            raise DispatchError("content_group_task_binding_invalid")
        members = [
            _candidate_from_payload(raw)
            for raw in raw_content_members
            if isinstance(raw, Mapping)
        ]
        if (
            len(members) != len(raw_content_members)
            or [member.capture_id for member in members]
            != expected_capture_ids
            or len(expected_capture_ids) != len(set(expected_capture_ids))
            or candidate.capture_id not in expected_capture_ids
        ):
            raise DispatchError("content_group_task_binding_invalid")
        for member in members:
            validate_concurrent_cs408_candidate(member)
            identity = content_processing_identity(member, contract)
            if (
                member.subject != candidate.subject
                or identity.get("content_processing_id")
                != expected_content_id
                or member.input_binding.get("processing_contract_sha256")
                != contract.get("subject_processing_contract_sha256")
            ):
                raise DispatchError("content_group_task_binding_invalid")
        if candidate.subject == "math":
            raw_math_members = task.frozen_payload.get("math_group_members")
            expected_group_key = contract.get("math_group_processing_key")
            expected_math_capture_ids = contract.get(
                "math_group_capture_ids"
            )
            if (
                raw_math_members != raw_content_members
                or expected_math_capture_ids != expected_capture_ids
                or not isinstance(expected_group_key, str)
                or any(
                    math_group_processing_key(member) != expected_group_key
                    for member in members
                )
            ):
                raise DispatchError("math_group_task_binding_invalid")
        for member in members:
            if member.capture_id == candidate.capture_id:
                continue
            member_published = worker.process_claimed_candidate(
                member,
                frozen_reason,
                write_dashboard=False,
            )
            if member_published.get("status") not in expected_statuses[
                candidate.subject
            ]:
                error = member_published.get(
                    "last_error_code"
                ) or member_published.get("error_code")
                raise DispatchError(
                    str(error or "content_group_member_publication_failed")
                )
            member_publications.append(
                copy.deepcopy(dict(member_published))
            )
    elif candidate.subject == "math":
        raw_members = task.frozen_payload.get("math_group_members")
        expected_group_key = contract.get("math_group_processing_key")
        expected_capture_ids = contract.get("math_group_capture_ids")
        if (
            not isinstance(raw_members, list)
            or not raw_members
            or not isinstance(expected_group_key, str)
            or not isinstance(expected_capture_ids, list)
        ):
            raise DispatchError("math_group_task_binding_invalid")
        members = [
            _candidate_from_payload(raw)
            for raw in raw_members
            if isinstance(raw, Mapping)
        ]
        if (
            len(members) != len(raw_members)
            or [member.capture_id for member in members]
            != expected_capture_ids
            or any(
                member.subject != "math"
                or math_group_processing_key(member) != expected_group_key
                or member.input_binding.get("processing_contract_sha256")
                != contract.get("subject_processing_contract_sha256")
                for member in members
            )
        ):
            raise DispatchError("math_group_task_binding_invalid")
        for member in members:
            if member.capture_id == candidate.capture_id:
                continue
            member_published = worker.process_claimed_candidate(
                member,
                frozen_reason,
                write_dashboard=False,
            )
            if member_published.get("status") not in expected_statuses["math"]:
                error = member_published.get(
                    "last_error_code"
                ) or member_published.get("error_code")
                raise DispatchError(
                    str(error or "math_group_member_publication_failed")
                )
            member_publications.append(
                copy.deepcopy(dict(member_published))
            )
    draft = (
        result.draft_analysis
        if isinstance(result.draft_analysis, Mapping)
        else result.analysis
    )
    critical = (
        result.critical_review
        if isinstance(result.critical_review, Mapping)
        else None
    )
    task_result = {
        "schema_version": "study-intake-production-task-result-v1",
        "unit_sha256": task.unit_sha256,
        "lease_fence": fence,
        "analysis": _stage_value(
            draft,
            result,
            "analysis",
            expected_subject=candidate.subject,
            expected_release_id=str(
                task.frozen_payload["dispatch_contract"]["release_id"]
            ),
        ),
        "critical_review": (
            _stage_value(
                critical,
                result,
                "critical_review",
                expected_subject=candidate.subject,
                expected_release_id=str(
                    task.frozen_payload["dispatch_contract"]["release_id"]
                ),
            )
            if critical is not None
            else None
        ),
        "member_publications": member_publications,
        "formal_write_count": 0,
    }
    if quality_review and not two_pass:
        if (
            candidate.subject != "cs408"
            or published.get("status") != "succeeded"
            or not isinstance(result.critical_review, Mapping)
            or result.critical_review.get("verdict") != "reject"
            or published.get("report_available") is not True
            or published.get("execution_status") != "succeeded"
            or published.get("quality_status") != "issues_found"
            or published.get("report_disposition") != "needs_sol_review"
            or published.get("sol_review_status") != "pending"
            or published.get("formal_write_eligible") is not False
            or published.get("production_accepted") is not False
        ):
            raise DispatchError("core_quality_review_publication_invalid")
        stage_receipts = result.stage_receipts
        decision = decide_execution_quality(
            model_completed=result.semantic_stage_count == 2,
            provider_completed=bool(
                isinstance(stage_receipts, Mapping)
                and all(
                    isinstance(stage_receipts.get(stage_name), Mapping)
                    and int(
                        stage_receipts[stage_name].get(
                            "provider_request_count"
                        )
                        or 0
                    )
                    >= 2
                    for stage_name in ("analysis", "critical_review")
                )
            ),
            mcp_database_query_completed=bool(
                isinstance(stage_receipts, Mapping)
                and all(
                    isinstance(stage_receipts.get(stage_name), Mapping)
                    and int(
                        stage_receipts[stage_name].get(
                            "mcp_tool_call_count"
                        )
                        or 0
                    )
                    >= 1
                    for stage_name in ("analysis", "critical_review")
                )
            ),
            raw_output_reopenable=bool(
                isinstance(stage_receipts, Mapping)
                and all(
                    isinstance(stage_receipts.get(stage_name), Mapping)
                    and bool(
                        stage_receipts[stage_name].get(
                            "raw_output_object_sha256"
                        )
                        and stage_receipts[stage_name].get(
                            "raw_output_object_ref"
                        )
                    )
                    for stage_name in ("analysis", "critical_review")
                )
            ),
            report_reopenable=True,
            identity_verified=bool(
                isinstance(stage_receipts, Mapping)
                and all(
                    isinstance(stage_receipts.get(stage_name), Mapping)
                    and stage_receipts[stage_name].get(
                        "runtime_identity_status"
                    )
                    == "confirmed"
                    for stage_name in ("analysis", "critical_review")
                )
            ),
            quality_findings_present=True,
        )
        task_result.update(
            {
                "terminal_outcome": decision.terminal_outcome,
                "terminal_error_code": None,
                "terminal_quality_error_code": (
                    "cs408_critical_review_rejected"
                ),
                "terminal_report_disposition": "needs_sol_review",
                "terminal_review_stage_count": 2,
                **decision.publication_fields(),
            }
        )
    return task_result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    cancellation = _CancellationCoordinator()
    liveness_probes = _LivenessProbeCoordinator()

    def request_cancel(_signum: int, _frame: object) -> None:
        cancellation.request()

    previous_handlers = {
        signum: signal.signal(signum, request_cancel)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    previous_handlers[signal.SIGUSR1] = signal.signal(
        signal.SIGUSR1,
        lambda _signum, _frame: liveness_probes.request(),
    )
    try:
        request = json.loads(sys.stdin.buffer.read())
        if not isinstance(request, Mapping):
            raise DispatchError("task_process_request_invalid")
        result = run_request(
            args.config,
            request,
            cancellation=cancellation,
            liveness_probes=liveness_probes,
        )
        if cancellation.requested.is_set():
            raise DispatchError("task_process_cancelled")
        sys.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (DispatchError, PreprocessorError) as exc:
        sys.stderr.write(json.dumps({"error_code": exc.code}, sort_keys=True))
        return 2
    except (UnicodeError, json.JSONDecodeError):
        sys.stderr.write(json.dumps({"error_code": "task_process_request_invalid"}))
        return 2
    finally:
        cancellation.join()
        liveness_probes.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
