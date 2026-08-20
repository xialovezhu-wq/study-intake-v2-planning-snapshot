"""Fail-closed mixed-subject Luna stress coordinator.

The coordinator owns only the common start barrier and verification summary.
Each subject keeps its own immutable batch, dispatcher, MCP namespace,
candidate authority and read sessions.  No Sol or formal writer is imported.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import copy
import datetime as dt
import hashlib
import hmac
import json
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

if __package__:
    from .preprocessor_core import (
        atomic_publish_json_no_clobber,
        canonical_bytes,
        json_file_bytes,
        sha256_file,
    )
else:
    from preprocessor_core import (  # type: ignore[no-redef]
        atomic_publish_json_no_clobber,
        canonical_bytes,
        json_file_bytes,
        sha256_file,
    )


SELECTION_SCHEMA = "mixed_luna_stress_selection_v1"
SUMMARY_SCHEMA = "mixed_luna_stress_summary_v1"
SUBJECTS = ("english", "math", "cs408")
RUN_COUNTS = {
    # EN-P0-006 is an existing-object inventory audit, not a Luna workload.
    # Higher stress stages remain deliberately undefined until a new frozen
    # inventory of distinct, not-yet-ingested business captures exists.
    "smoke_3": {"english": 1, "math": 1, "cs408": 1},
}
STAGE_ORDER = (
    "analysis_submitted",
    "analysis_completed",
    "critical_review_submitted",
    "critical_review_completed",
)
ENGLISH_SOL_READY_ACTIONS = {
    "proposal_ready",
}


class MixedLunaStressError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def processing_host_authority_refresher(
    processing_host: object,
) -> Callable[[str], Mapping[str, Any]]:
    """Bind preflight to ProcessingPluginHost.subject_authority_snapshot."""

    snapshot = getattr(processing_host, "subject_authority_snapshot", None)
    if not callable(snapshot):
        raise MixedLunaStressError("mixed_stress_processing_host_invalid")

    def refresh(subject: str) -> Mapping[str, Any]:
        value = snapshot(subject)
        if not isinstance(value, Mapping):
            raise MixedLunaStressError("mixed_stress_candidate_authority_invalid")
        return copy.deepcopy(dict(value))

    return refresh


def _sha(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _authority_key(path: Path) -> bytes:
    target = path.expanduser().resolve()
    try:
        node = target.lstat()
        key = target.read_bytes()
    except OSError as exc:
        raise MixedLunaStressError("mixed_stress_authority_key_missing") from exc
    if target.is_symlink() or not target.is_file() or node.st_size != 32 or len(key) != 32:
        raise MixedLunaStressError("mixed_stress_authority_key_invalid")
    return key


def validate_selection(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "campaign_id",
        "candidate_release_id",
        "run_mode",
        "execution_attempt",
        "execution_runtime_sha256",
        "max_workers",
        "subject_scopes",
        "tasks",
        "sol_enabled",
        "formal_write_count",
    }
    mode = value.get("run_mode")
    expected_counts = RUN_COUNTS.get(str(mode))
    tasks = value.get("tasks")
    scopes = value.get("subject_scopes")
    if (
        set(value) != expected_keys
        or value.get("schema_version") != SELECTION_SCHEMA
        or not isinstance(value.get("campaign_id"), str)
        or not str(value.get("campaign_id") or "")
        or not _sha(value.get("candidate_release_id"))
        or expected_counts is None
        or not isinstance(value.get("execution_attempt"), int)
        or isinstance(value.get("execution_attempt"), bool)
        or int(value.get("execution_attempt") or 0) < 1
        or not _sha(value.get("execution_runtime_sha256"))
        or value.get("max_workers") != sum(expected_counts.values())
        or not isinstance(scopes, Mapping)
        or set(scopes) != set(SUBJECTS)
        or any(not _sha(scopes.get(subject)) for subject in SUBJECTS)
        or not isinstance(tasks, list)
        or len(tasks) != sum(expected_counts.values())
        or value.get("sol_enabled") is not False
        or value.get("formal_write_count") != 0
    ):
        raise MixedLunaStressError("mixed_stress_selection_invalid")
    task_keys = {
        "ordinal",
        "task_id",
        "unit_sha256",
        "subject",
        "subject_scope_sha256",
        "subject_batch_id",
        "subject_batch_sha256",
        "dispatcher_id",
        "mcp_namespace",
        "generation",
        "authority_fingerprint",
        "read_session_binding_sha256",
        "sol_authorized",
        "formal_write_count",
    }
    checked: list[dict[str, Any]] = []
    for ordinal, raw in enumerate(tasks, start=1):
        if not isinstance(raw, Mapping) or set(raw) != task_keys:
            raise MixedLunaStressError("mixed_stress_task_invalid")
        subject = raw.get("subject")
        if (
            raw.get("ordinal") != ordinal
            or subject not in SUBJECTS
            or not isinstance(raw.get("task_id"), str)
            or not str(raw.get("task_id") or "")
            or not _sha(raw.get("unit_sha256"))
            or raw.get("subject_scope_sha256") != scopes.get(subject)
            or not isinstance(raw.get("subject_batch_id"), str)
            or not str(raw.get("subject_batch_id") or "")
            or not _sha(raw.get("subject_batch_sha256"))
            or not isinstance(raw.get("dispatcher_id"), str)
            or not str(raw.get("dispatcher_id") or "")
            or not isinstance(raw.get("mcp_namespace"), str)
            or not str(raw.get("mcp_namespace") or "")
            or not isinstance(raw.get("generation"), str)
            or not str(raw.get("generation") or "")
            or not _sha(raw.get("authority_fingerprint"))
            or not _sha(raw.get("read_session_binding_sha256"))
            or raw.get("sol_authorized") is not False
            or raw.get("formal_write_count") != 0
        ):
            raise MixedLunaStressError("mixed_stress_task_invalid")
        checked.append(copy.deepcopy(dict(raw)))
    if (
        len({row["task_id"] for row in checked}) != len(checked)
        or len({row["unit_sha256"] for row in checked}) != len(checked)
        or len({row["read_session_binding_sha256"] for row in checked})
        != len(checked)
        or {
            subject: sum(row["subject"] == subject for row in checked)
            for subject in SUBJECTS
        }
        != expected_counts
    ):
        raise MixedLunaStressError("mixed_stress_distinct_task_set_invalid")
    subject_bindings: dict[str, dict[str, str]] = {}
    for subject in SUBJECTS:
        rows = [row for row in checked if row["subject"] == subject]
        binding_fields = (
            "subject_batch_id",
            "subject_batch_sha256",
            "dispatcher_id",
            "mcp_namespace",
            "generation",
            "authority_fingerprint",
        )
        if any(len({str(row[field]) for row in rows}) != 1 for field in binding_fields):
            raise MixedLunaStressError("mixed_stress_subject_binding_invalid")
        subject_bindings[subject] = {
            field: str(rows[0][field]) for field in binding_fields
        }
    for field in (
        "subject_batch_id",
        "subject_batch_sha256",
        "dispatcher_id",
        "mcp_namespace",
        "generation",
        "authority_fingerprint",
    ):
        if len({subject_bindings[subject][field] for subject in SUBJECTS}) != 3:
            raise MixedLunaStressError("mixed_stress_cross_subject_isolation_invalid")
    return {**copy.deepcopy(dict(value)), "tasks": checked}


class _MixedBarrierState:
    def __init__(self, tasks: Sequence[Mapping[str, Any]], timeout_seconds: float):
        self.tasks = tuple(tasks)
        self.timeout_seconds = float(timeout_seconds)
        self.lock = threading.Lock()
        self.arrived = 0
        self.active = 0
        self.peak = 0
        self.subject_active = {subject: 0 for subject in SUBJECTS}
        self.subject_peak = {subject: 0 for subject in SUBJECTS}
        self.arrived_at: dict[int, str] = {}
        self.active_ordinals: set[int] = set()
        self.events: dict[int, list[dict[str, str]]] = {
            int(task["ordinal"]): [] for task in tasks
        }
        self.released_at: str | None = None
        self.failure_code: str | None = None
        self.barrier = threading.Barrier(
            len(tasks), action=self._release, timeout=self.timeout_seconds
        )
        # The arrival barrier proves that all selected workers reached the
        # common release point.  A separate activation barrier makes peak
        # accounting start only after that release, so queued/waiting threads
        # cannot be counted as active Luna execution units.
        self.activation_barrier = threading.Barrier(
            len(tasks), timeout=self.timeout_seconds
        )

    def _release(self) -> None:
        with self.lock:
            self.released_at = _now()

    def enter(self, task: Mapping[str, Any]) -> None:
        ordinal = int(task["ordinal"])
        subject = str(task["subject"])
        with self.lock:
            if ordinal in self.arrived_at:
                raise MixedLunaStressError("mixed_stress_duplicate_arrival")
            self.arrived_at[ordinal] = _now()
            self.arrived += 1
        try:
            self.barrier.wait()
        except threading.BrokenBarrierError as exc:
            with self.lock:
                self.failure_code = self.failure_code or "mixed_stress_barrier_failed"
            raise MixedLunaStressError("mixed_stress_barrier_failed") from exc
        with self.lock:
            self.active_ordinals.add(ordinal)
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.subject_active[subject] += 1
            self.subject_peak[subject] = max(
                self.subject_peak[subject], self.subject_active[subject]
            )
        try:
            self.activation_barrier.wait()
        except threading.BrokenBarrierError as exc:
            with self.lock:
                self.failure_code = (
                    self.failure_code or "mixed_stress_activation_barrier_failed"
                )
            raise MixedLunaStressError(
                "mixed_stress_activation_barrier_failed"
            ) from exc

    def leave(self, task: Mapping[str, Any]) -> None:
        ordinal = int(task["ordinal"])
        with self.lock:
            if ordinal in self.active_ordinals:
                self.active_ordinals.remove(ordinal)
                self.active -= 1
                self.subject_active[str(task["subject"])] -= 1

    def observe(
        self,
        task: Mapping[str, Any],
        event: str,
        observed_at: str | None = None,
    ) -> None:
        ordinal = int(task["ordinal"])
        timestamp = observed_at or _now()
        try:
            parsed = _parse(timestamp)
        except (TypeError, ValueError) as exc:
            raise MixedLunaStressError("mixed_stress_stage_timestamp_invalid") from exc
        if parsed.tzinfo is None:
            raise MixedLunaStressError("mixed_stress_stage_timestamp_invalid")
        with self.lock:
            events = self.events[ordinal]
            expected = STAGE_ORDER[len(events)] if len(events) < 4 else None
            if event != expected:
                self.failure_code = "mixed_stress_stage_order_invalid"
                raise MixedLunaStressError(self.failure_code)
            if events and _parse(events[-1]["observed_at"]) > parsed:
                self.failure_code = "mixed_stress_stage_timestamp_invalid"
                raise MixedLunaStressError(self.failure_code)
            events.append({"name": event, "observed_at": timestamp})

    def model_calls(self, task: Mapping[str, Any]) -> int:
        with self.lock:
            return sum(
                event["name"].endswith("_submitted")
                for event in self.events[int(task["ordinal"])]
            )

    def metrics(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        row_by_ordinal = {int(row["ordinal"]): row for row in rows}
        with self.lock:
            traces = []
            submitted: list[dt.datetime] = []
            for task in self.tasks:
                ordinal = int(task["ordinal"])
                events = copy.deepcopy(self.events[ordinal])
                submitted_at = (
                    events[0]["observed_at"]
                    if events and events[0]["name"] == "analysis_submitted"
                    else None
                )
                if submitted_at is not None:
                    submitted.append(_parse(submitted_at))
                traces.append(
                    {
                        "ordinal": ordinal,
                        "task_id": task["task_id"],
                        "unit_sha256": task["unit_sha256"],
                        "subject": task["subject"],
                        "barrier_arrived_at": self.arrived_at.get(ordinal),
                        "submitted_at": submitted_at,
                        "stage_events": events,
                        "strict_stage_order_verified": tuple(
                            event["name"] for event in events
                        )
                        == STAGE_ORDER,
                    }
                )
            first = min(submitted).isoformat() if submitted else None
            last = max(submitted).isoformat() if submitted else None
            spread = (
                int(round((max(submitted) - min(submitted)).total_seconds() * 1000))
                if submitted
                else None
            )
            success = all(
                row_by_ordinal.get(int(task["ordinal"]), {}).get("status")
                == "succeeded"
                for task in self.tasks
            )
            failure_code = self.failure_code
            if failure_code is None and not success:
                failure_code = "mixed_stress_quality_closure_failed"
            return {
                "barrier_expected": len(self.tasks),
                "barrier_arrived": self.arrived,
                "barrier_released_at": self.released_at,
                "global_peak_active": self.peak,
                "per_subject_peak_active": dict(self.subject_peak),
                "submitted_at_first": first,
                "submitted_at_last": last,
                "submitted_at_spread_ms": spread,
                "item_traces": traces,
                "failure_code": failure_code,
            }


def _check_artifacts(
    task: Mapping[str, Any],
    result: Mapping[str, Any],
    verifier: Callable[[Mapping[str, Any], Mapping[str, Any]], None],
) -> dict[str, Any]:
    expected_keys = {
        "status",
        "quality_outcome",
        "proposal_action",
        "subject",
        "task_id",
        "unit_sha256",
        "subject_batch_id",
        "dispatcher_id",
        "mcp_namespace",
        "generation",
        "read_session_binding_sha256",
        "read_session_id",
        "read_session_manifest_sha256",
        "analysis_transcript_sha256",
        "analysis_stage_receipt_sha256",
        "analysis_stage_receipt_hmac_sha256",
        "critical_review_transcript_sha256",
        "critical_review_stage_receipt_sha256",
        "critical_review_stage_receipt_hmac_sha256",
        "final_read_session_receipt_sha256",
        "package_sha256",
        "quality_receipt_sha256",
        "model_call_count",
        "formal_write_count",
    }
    if set(result) != expected_keys:
        raise MixedLunaStressError("mixed_stress_result_shape_invalid")
    subject = str(task["subject"])
    proposal_action = result.get("proposal_action")
    if (
        result.get("status") != "quality_passed"
        or result.get("quality_outcome") not in {"accepted", "corrected"}
        or result.get("subject") != subject
        or result.get("task_id") != task["task_id"]
        or result.get("unit_sha256") != task["unit_sha256"]
        or result.get("subject_batch_id") != task["subject_batch_id"]
        or result.get("dispatcher_id") != task["dispatcher_id"]
        or result.get("mcp_namespace") != task["mcp_namespace"]
        or result.get("generation") != task["generation"]
        or result.get("read_session_binding_sha256")
        != task["read_session_binding_sha256"]
        or not isinstance(result.get("read_session_id"), str)
        or not str(result.get("read_session_id") or "")
        or any(
            not _sha(result.get(field))
            for field in expected_keys
            if field.endswith("_sha256")
        )
        or result.get("model_call_count") != 2
        or result.get("formal_write_count") != 0
        or proposal_action in {"conflict", "evidence_incomplete"}
        or (
            subject == "english"
            and proposal_action not in ENGLISH_SOL_READY_ACTIONS
        )
    ):
        raise MixedLunaStressError("mixed_stress_quality_result_invalid")
    verifier(task, result)
    return copy.deepcopy(dict(result))


def _summary_path(runtime_root: Path, digest: str) -> Path:
    return (
        runtime_root
        / "dispatch"
        / "mixed-luna-stress"
        / "summaries"
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )


def _validate_summary_core(value: Mapping[str, Any]) -> None:
    expected_keys = {
        "schema_version",
        "campaign_id",
        "candidate_release_id",
        "run_mode",
        "execution_attempt",
        "execution_runtime_sha256",
        "max_workers",
        "subject_scopes",
        "subject_bindings",
        "candidate_authority_refresh",
        "selected_task_count",
        "distinct_task_count",
        "distinct_unit_count",
        "subject_counts",
        "barrier_expected",
        "barrier_arrived",
        "barrier_released_at",
        "global_peak_active",
        "per_subject_peak_active",
        "submitted_at_first",
        "submitted_at_last",
        "submitted_at_spread_ms",
        "stage_submission_count",
        "stage_submission_limit",
        "distinct_transcript_count",
        "distinct_hmac_stage_receipt_count",
        "distinct_stage_receipt_hmac_count",
        "distinct_final_session_count",
        "distinct_package_count",
        "distinct_quality_receipt_count",
        "item_traces",
        "results",
        "prerequisites",
        "status",
        "failure_code",
        "sol_control",
        "model_call_count",
        "formal_write_count",
        "completed_at",
    }
    mode = value.get("run_mode")
    counts = RUN_COUNTS.get(str(mode))
    expected_count = sum(counts.values()) if counts is not None else None
    results = value.get("results")
    traces = value.get("item_traces")
    if (
        set(value) != expected_keys
        or value.get("schema_version") != SUMMARY_SCHEMA
        or not _sha(value.get("candidate_release_id"))
        or not _sha(value.get("execution_runtime_sha256"))
        or expected_count is None
        or value.get("max_workers") != expected_count
        or value.get("selected_task_count") != expected_count
        or value.get("subject_counts") != counts
        or value.get("barrier_expected") != expected_count
        or not isinstance(results, list)
        or len(results) != expected_count
        or not isinstance(traces, list)
        or len(traces) != expected_count
        or value.get("stage_submission_limit") != expected_count * 2
        or not isinstance(value.get("stage_submission_count"), int)
        or not 0 <= int(value.get("stage_submission_count") or 0) <= expected_count * 2
        or value.get("model_call_count") != value.get("stage_submission_count")
        or value.get("formal_write_count") != 0
        or value.get("status") not in {"passed", "failed_closed"}
        or not isinstance(value.get("completed_at"), str)
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    result_keys = {
        "ordinal",
        "task_id",
        "unit_sha256",
        "subject",
        "status",
        "error_code",
        "model_call_count",
        "formal_write_count",
        "closure",
    }
    result_by_ordinal: dict[int, Mapping[str, Any]] = {}
    for row in results:
        if not isinstance(row, Mapping) or set(row) != result_keys:
            raise MixedLunaStressError("mixed_stress_summary_invalid")
        ordinal = row.get("ordinal")
        status = row.get("status")
        model_call_count = row.get("model_call_count")
        if (
            not isinstance(ordinal, int)
            or isinstance(ordinal, bool)
            or ordinal in result_by_ordinal
            or not isinstance(row.get("task_id"), str)
            or not str(row.get("task_id") or "")
            or not _sha(row.get("unit_sha256"))
            or row.get("subject") not in SUBJECTS
            or status not in {"succeeded", "failed"}
            or row.get("formal_write_count") != 0
            or not isinstance(model_call_count, int)
            or isinstance(model_call_count, bool)
            or not 0 <= model_call_count <= 2
            or (
                status == "succeeded"
                and (
                    row.get("error_code") is not None
                    or model_call_count != 2
                    or not isinstance(row.get("closure"), Mapping)
                )
            )
            or (
                status == "failed"
                and (
                    not isinstance(row.get("error_code"), str)
                    or not str(row.get("error_code") or "")
                    or row.get("closure") is not None
                )
            )
        ):
            raise MixedLunaStressError("mixed_stress_summary_invalid")
        result_by_ordinal[ordinal] = row
    if set(result_by_ordinal) != set(range(1, expected_count + 1)):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    successful = [row for row in results if row["status"] == "succeeded"]
    if value.get("stage_submission_count") != sum(
        int(row["model_call_count"]) for row in results
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    trace_by_ordinal = {
        int(trace.get("ordinal") or 0): trace
        for trace in traces
        if isinstance(trace, Mapping)
    }
    if len(trace_by_ordinal) != expected_count:
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    strict_order = True
    submitted: list[dt.datetime] = []
    for ordinal in range(1, expected_count + 1):
        trace = trace_by_ordinal.get(ordinal)
        events = trace.get("stage_events") if isinstance(trace, Mapping) else None
        row = result_by_ordinal[ordinal]
        if (
            not isinstance(events, list)
            or len(events) > 4
            or trace.get("task_id") != row.get("task_id")
            or trace.get("unit_sha256") != row.get("unit_sha256")
            or trace.get("subject") != row.get("subject")
        ):
            raise MixedLunaStressError("mixed_stress_summary_invalid")
        names = tuple(
            event.get("name") if isinstance(event, Mapping) else None
            for event in events
        )
        if names != STAGE_ORDER[: len(names)]:
            raise MixedLunaStressError("mixed_stress_summary_invalid")
        exact = names == STAGE_ORDER
        if trace.get("strict_stage_order_verified") is not exact:
            raise MixedLunaStressError("mixed_stress_summary_invalid")
        strict_order = strict_order and exact
        submitted_at = trace.get("submitted_at")
        if submitted_at is not None:
            try:
                submitted.append(_parse(str(submitted_at)))
            except ValueError as exc:
                raise MixedLunaStressError("mixed_stress_summary_invalid") from exc
    first = value.get("submitted_at_first")
    last = value.get("submitted_at_last")
    spread = value.get("submitted_at_spread_ms")
    if submitted:
        expected_first = min(submitted)
        expected_last = max(submitted)
        if (
            first is None
            or last is None
            or _parse(str(first)) != expected_first
            or _parse(str(last)) != expected_last
            or spread
            != int(round((expected_last - expected_first).total_seconds() * 1000))
        ):
            raise MixedLunaStressError("mixed_stress_summary_invalid")
    elif first is not None or last is not None or spread is not None:
        raise MixedLunaStressError("mixed_stress_summary_invalid")

    closures = [row.get("closure") for row in successful]
    if any(not isinstance(closure, Mapping) for closure in closures):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    transcript_hashes = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_transcript_sha256",
            "critical_review_transcript_sha256",
        )
    ]
    receipt_hashes = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_stage_receipt_sha256",
            "critical_review_stage_receipt_sha256",
        )
    ]
    stage_hmacs = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_stage_receipt_hmac_sha256",
            "critical_review_stage_receipt_hmac_sha256",
        )
    ]
    finals = [closure["final_read_session_receipt_sha256"] for closure in closures]
    packages = [closure["package_sha256"] for closure in closures]
    qualities = [closure["quality_receipt_sha256"] for closure in closures]
    sessions = [closure["read_session_id"] for closure in closures]
    if (
        value.get("distinct_transcript_count") != len(set(transcript_hashes))
        or value.get("distinct_hmac_stage_receipt_count") != len(set(receipt_hashes))
        or value.get("distinct_stage_receipt_hmac_count") != len(set(stage_hmacs))
        or value.get("distinct_final_session_count") != len(set(finals))
        or value.get("distinct_package_count") != len(set(packages))
        or value.get("distinct_quality_receipt_count") != len(set(qualities))
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    sol = value.get("sol_control")
    sol_subjects = sol.get("subjects") if isinstance(sol, Mapping) else None
    sol_evidence = (
        sol.get("global_state_evidence") if isinstance(sol, Mapping) else None
    )
    if (
        not isinstance(sol, Mapping)
        or sol.get("sol_enabled") is not False
        or not isinstance(sol_evidence, Mapping)
        or sol_evidence.get("schema_version")
        != "mixed_luna_global_sol_state_evidence_v1"
        or not _sha(sol_evidence.get("before_sha256"))
        or not _sha(sol_evidence.get("after_sha256"))
        or sol_evidence.get("before_active_writer_count") != 0
        or sol_evidence.get("after_active_writer_count") != 0
        or sol_evidence.get("before_active_writer") is not None
        or sol_evidence.get("after_active_writer") is not None
        or not isinstance(sol_evidence.get("before_formal_write_count"), int)
        or not isinstance(sol_evidence.get("after_formal_write_count"), int)
        or sol_evidence.get("formal_write_count_delta")
        != (
            int(sol_evidence.get("after_formal_write_count") or 0)
            - int(sol_evidence.get("before_formal_write_count") or 0)
        )
        or sol_evidence.get("unchanged")
        is not (
            sol_evidence.get("before_sha256")
            == sol_evidence.get("after_sha256")
        )
        or not isinstance(sol_subjects, Mapping)
        or set(sol_subjects) != set(SUBJECTS)
        or any(
            not isinstance(sol_subjects[subject], Mapping)
            or sol_subjects[subject].get("sol_authorized") is not False
            for subject in SUBJECTS
        )
        or sol_subjects["math"].get("parent_sol_batch_handoff_eligible") is not False
        or sol_subjects["cs408"].get("parent_sol_batch_handoff_eligible") is not False
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    should_pass = (
        len(successful) == expected_count
        and value.get("distinct_task_count") == expected_count
        and value.get("distinct_unit_count") == expected_count
        and value.get("barrier_arrived") == expected_count
        and value.get("barrier_released_at") is not None
        and value.get("global_peak_active") == expected_count
        and value.get("per_subject_peak_active") == counts
        and value.get("stage_submission_count") == expected_count * 2
        and len(set(transcript_hashes)) == expected_count * 2
        and len(set(receipt_hashes)) == expected_count * 2
        and len(set(stage_hmacs)) == expected_count * 2
        and len(set(finals)) == expected_count
        and len(set(packages)) == expected_count
        and len(set(qualities)) == expected_count
        and len(set(sessions)) == expected_count
        and strict_order
        and sol_evidence.get("unchanged") is True
        and sol_evidence.get("formal_write_count_delta") == 0
    )
    if (
        value.get("status") != ("passed" if should_pass else "failed_closed")
        or (value.get("failure_code") is None) is not should_pass
        or sol_subjects["english"].get("parent_sol_batch_handoff_eligible")
        is not (should_pass and mode == "full_108")
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")


def _publish_summary(
    runtime_root: Path,
    key: bytes,
    core: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    _validate_summary_core(core)
    summary = {
        **copy.deepcopy(dict(core)),
        "hmac_key_id": hashlib.sha256(key).hexdigest(),
        "hmac_sha256": hmac.new(
            key,
            canonical_bytes({"purpose": SUMMARY_SCHEMA, "payload": core}) + b"\n",
            hashlib.sha256,
        ).hexdigest(),
    }
    digest = hashlib.sha256(json_file_bytes(summary)).hexdigest()
    path = _summary_path(runtime_root, digest)
    atomic_publish_json_no_clobber(path, summary)
    if sha256_file(path) != digest:
        raise MixedLunaStressError("mixed_stress_summary_write_mismatch")
    return digest, path, summary


def verify_summary(
    runtime_root: Path,
    authority_key_path: Path,
    digest: str,
) -> dict[str, Any]:
    if not _sha(digest):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    path = _summary_path(runtime_root.resolve(), digest)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise MixedLunaStressError("mixed_stress_summary_invalid") from exc
    if not isinstance(value, Mapping) or path.is_symlink() or sha256_file(path) != digest:
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    key = _authority_key(authority_key_path)
    core = {
        name: copy.deepcopy(nested)
        for name, nested in value.items()
        if name not in {"hmac_key_id", "hmac_sha256"}
    }
    expected = hmac.new(
        key,
        canonical_bytes({"purpose": SUMMARY_SCHEMA, "payload": core}) + b"\n",
        hashlib.sha256,
    ).hexdigest()
    if (
        value.get("schema_version") != SUMMARY_SCHEMA
        or value.get("hmac_key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(str(value.get("hmac_sha256") or ""), expected)
        or value.get("formal_write_count") != 0
        or value.get("sol_control", {}).get("sol_enabled") is not False
    ):
        raise MixedLunaStressError("mixed_stress_summary_invalid")
    _validate_summary_core(core)
    return copy.deepcopy(dict(value))


def _verify_prerequisite(
    runtime_root: Path,
    authority_key_path: Path,
    digest: str,
    *,
    expected_mode: str,
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    summary = verify_summary(runtime_root, authority_key_path, digest)
    if (
        summary.get("status") != "passed"
        or summary.get("run_mode") != expected_mode
        or summary.get("campaign_id") != selection.get("campaign_id")
        or summary.get("candidate_release_id")
        != selection.get("candidate_release_id")
        or summary.get("subject_scopes") != selection.get("subject_scopes")
        or summary.get("execution_attempt") == selection.get("execution_attempt")
        or summary.get("execution_runtime_sha256")
        == selection.get("execution_runtime_sha256")
    ):
        raise MixedLunaStressError("mixed_stress_prerequisite_invalid")
    return summary


def run_mixed_stress(
    selection_value: Mapping[str, Any],
    *,
    runtime_root: Path,
    authority_key_path: Path,
    processing_host: object,
    runner: Callable[
        [Mapping[str, Any], Callable[..., None]], Mapping[str, Any]
    ],
    artifact_verifier: Callable[[Mapping[str, Any], Mapping[str, Any]], None],
    sol_state_reader: Callable[[], Mapping[str, Any]],
    barrier_timeout_seconds: float = 30,
    smoke_3_summary_sha256: str | None = None,
    smoke_20_summary_sha256: str | None = None,
) -> tuple[str, Path, dict[str, Any]]:
    selection = validate_selection(selection_value)
    if barrier_timeout_seconds <= 0:
        raise MixedLunaStressError("mixed_stress_barrier_timeout_invalid")
    runtime = runtime_root.expanduser().resolve()
    key = _authority_key(authority_key_path)
    try:
        sol_before = copy.deepcopy(dict(sol_state_reader()))
    except Exception as exc:
        raise MixedLunaStressError("mixed_stress_sol_state_reopen_failed") from exc
    if (
        sol_before.get("active_writer_count") != 0
        or sol_before.get("active_writer") is not None
        or not isinstance(sol_before.get("formal_write_count"), int)
    ):
        raise MixedLunaStressError("mixed_stress_sol_not_disabled")
    sol_before_sha = hashlib.sha256(
        canonical_bytes(sol_before) + b"\n"
    ).hexdigest()
    mode = str(selection["run_mode"])
    prerequisites = {
        "smoke_3_summary_sha256": smoke_3_summary_sha256,
        "smoke_20_summary_sha256": smoke_20_summary_sha256,
    }
    prerequisite_summaries: list[Mapping[str, Any]] = []
    if mode == "smoke_3":
        if any(value is not None for value in prerequisites.values()):
            raise MixedLunaStressError("mixed_stress_prerequisite_invalid")
    else:  # pragma: no cover - validate_selection rejects undefined modes.
        raise MixedLunaStressError("mixed_stress_run_mode_invalid")

    authority_refresher = processing_host_authority_refresher(processing_host)
    refreshed: dict[str, dict[str, str]] = {}
    for subject in SUBJECTS:
        authority = dict(authority_refresher(subject))
        expected = next(
            task for task in selection["tasks"] if task["subject"] == subject
        )
        if (
            authority.get("generation") != expected["generation"]
            or authority.get("authority_fingerprint")
            != expected["authority_fingerprint"]
            or (
                authority.get("subject") is not None
                and authority.get("subject") != subject
            )
            or (
                authority.get("mcp_namespace") is not None
                and authority.get("mcp_namespace") != expected["mcp_namespace"]
            )
            or authority.get("model_call_count", 0) != 0
            or authority.get("formal_write_count", 0) != 0
        ):
            raise MixedLunaStressError("mixed_stress_candidate_authority_drift")
        refreshed[subject] = {
            "mcp_namespace": str(expected["mcp_namespace"]),
            "generation": str(authority["generation"]),
            "authority_fingerprint": str(authority["authority_fingerprint"]),
            "refreshed_at": _now(),
        }

    tasks = selection["tasks"]
    state = _MixedBarrierState(tasks, barrier_timeout_seconds)
    rows: list[dict[str, Any]] = []

    def execute(task: Mapping[str, Any]) -> dict[str, Any]:
        try:
            state.enter(task)
            raw = runner(
                task,
                lambda event, observed_at=None: state.observe(
                    task, event, observed_at
                ),
            )
            checked = _check_artifacts(task, raw, artifact_verifier)
            return {
                "ordinal": task["ordinal"],
                "task_id": task["task_id"],
                "unit_sha256": task["unit_sha256"],
                "subject": task["subject"],
                "status": "succeeded",
                "error_code": None,
                "model_call_count": 2,
                "formal_write_count": 0,
                "closure": checked,
            }
        except MixedLunaStressError as exc:
            return {
                "ordinal": task["ordinal"],
                "task_id": task["task_id"],
                "unit_sha256": task["unit_sha256"],
                "subject": task["subject"],
                "status": "failed",
                "error_code": exc.code,
                "model_call_count": state.model_calls(task),
                "formal_write_count": 0,
                "closure": None,
            }
        except Exception:
            return {
                "ordinal": task["ordinal"],
                "task_id": task["task_id"],
                "unit_sha256": task["unit_sha256"],
                "subject": task["subject"],
                "status": "failed",
                "error_code": "mixed_stress_unexpected_task_failure",
                "model_call_count": state.model_calls(task),
                "formal_write_count": 0,
                "closure": None,
            }
        finally:
            state.leave(task)

    with ThreadPoolExecutor(max_workers=int(selection["max_workers"])) as executor:
        futures = [executor.submit(execute, task) for task in tasks]
        for future in as_completed(futures):
            rows.append(future.result())
    rows.sort(key=lambda row: int(row["ordinal"]))
    try:
        sol_after = copy.deepcopy(dict(sol_state_reader()))
    except Exception as exc:
        raise MixedLunaStressError("mixed_stress_sol_state_reopen_failed") from exc
    sol_after_sha = hashlib.sha256(
        canonical_bytes(sol_after) + b"\n"
    ).hexdigest()
    sol_unchanged = sol_before_sha == sol_after_sha
    sol_delta = (
        int(sol_after.get("formal_write_count") or 0)
        - int(sol_before.get("formal_write_count") or 0)
    )
    metrics = state.metrics(rows)
    successful = [row for row in rows if row["status"] == "succeeded"]
    closures = [row["closure"] for row in successful]
    transcript_hashes = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_transcript_sha256",
            "critical_review_transcript_sha256",
        )
    ]
    stage_receipt_hashes = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_stage_receipt_sha256",
            "critical_review_stage_receipt_sha256",
        )
    ]
    stage_hmacs = [
        closure[field]
        for closure in closures
        for field in (
            "analysis_stage_receipt_hmac_sha256",
            "critical_review_stage_receipt_hmac_sha256",
        )
    ]
    final_sessions = [closure["final_read_session_receipt_sha256"] for closure in closures]
    packages = [closure["package_sha256"] for closure in closures]
    qualities = [closure["quality_receipt_sha256"] for closure in closures]
    read_session_ids = [closure["read_session_id"] for closure in closures]
    expected_count = len(tasks)
    stage_submission_count = sum(row["model_call_count"] for row in rows)
    exact_artifacts = (
        len(successful) == expected_count
        and stage_submission_count == expected_count * 2
        and len(transcript_hashes) == len(set(transcript_hashes)) == expected_count * 2
        and len(stage_receipt_hashes)
        == len(set(stage_receipt_hashes))
        == expected_count * 2
        and len(stage_hmacs) == len(set(stage_hmacs)) == expected_count * 2
        and len(final_sessions) == len(set(final_sessions)) == expected_count
        and len(packages) == len(set(packages)) == expected_count
        and len(qualities) == len(set(qualities)) == expected_count
        and len(read_session_ids) == len(set(read_session_ids)) == expected_count
    )
    exact_peaks = (
        metrics["barrier_arrived"] == expected_count
        and metrics["barrier_released_at"] is not None
        and metrics["global_peak_active"] == expected_count
        and metrics["per_subject_peak_active"] == RUN_COUNTS[mode]
    )
    exact_order = all(
        trace["strict_stage_order_verified"] for trace in metrics["item_traces"]
    )
    sol_safe = (
        sol_after.get("active_writer_count") == 0
        and sol_after.get("active_writer") is None
        and sol_unchanged
        and sol_delta == 0
    )
    passed = (
        exact_artifacts
        and exact_peaks
        and exact_order
        and metrics["failure_code"] is None
        and sol_safe
    )
    english_closed = False
    core = {
        "schema_version": SUMMARY_SCHEMA,
        "campaign_id": selection["campaign_id"],
        "candidate_release_id": selection["candidate_release_id"],
        "run_mode": mode,
        "execution_attempt": selection["execution_attempt"],
        "execution_runtime_sha256": selection["execution_runtime_sha256"],
        "max_workers": selection["max_workers"],
        "subject_scopes": copy.deepcopy(selection["subject_scopes"]),
        "subject_bindings": {
            subject: {
                key_name: next(
                    task[key_name]
                    for task in tasks
                    if task["subject"] == subject
                )
                for key_name in (
                    "subject_batch_id",
                    "subject_batch_sha256",
                    "dispatcher_id",
                    "mcp_namespace",
                    "generation",
                    "authority_fingerprint",
                )
            }
            for subject in SUBJECTS
        },
        "candidate_authority_refresh": refreshed,
        "selected_task_count": expected_count,
        "distinct_task_count": len({task["task_id"] for task in tasks}),
        "distinct_unit_count": len({task["unit_sha256"] for task in tasks}),
        "subject_counts": copy.deepcopy(RUN_COUNTS[mode]),
        "barrier_expected": metrics["barrier_expected"],
        "barrier_arrived": metrics["barrier_arrived"],
        "barrier_released_at": metrics["barrier_released_at"],
        "global_peak_active": metrics["global_peak_active"],
        "per_subject_peak_active": metrics["per_subject_peak_active"],
        "submitted_at_first": metrics["submitted_at_first"],
        "submitted_at_last": metrics["submitted_at_last"],
        "submitted_at_spread_ms": metrics["submitted_at_spread_ms"],
        "stage_submission_count": stage_submission_count,
        "stage_submission_limit": expected_count * 2,
        "distinct_transcript_count": len(set(transcript_hashes)),
        "distinct_hmac_stage_receipt_count": len(set(stage_receipt_hashes)),
        "distinct_stage_receipt_hmac_count": len(set(stage_hmacs)),
        "distinct_final_session_count": len(set(final_sessions)),
        "distinct_package_count": len(set(packages)),
        "distinct_quality_receipt_count": len(set(qualities)),
        "item_traces": metrics["item_traces"],
        "results": rows,
        "prerequisites": prerequisites,
        "status": "passed" if passed else "failed_closed",
        "failure_code": (
            None
            if passed
            else metrics["failure_code"]
            or (
                "mixed_stress_sol_state_changed"
                if not sol_safe
                else "mixed_stress_acceptance_failed"
            )
        ),
        "sol_control": {
            "sol_enabled": False,
            "global_state_evidence": {
                "schema_version": "mixed_luna_global_sol_state_evidence_v1",
                "before_sha256": sol_before_sha,
                "after_sha256": sol_after_sha,
                "unchanged": sol_unchanged,
                "before_active_writer_count": sol_before.get(
                    "active_writer_count"
                ),
                "after_active_writer_count": sol_after.get(
                    "active_writer_count"
                ),
                "before_active_writer": copy.deepcopy(
                    sol_before.get("active_writer")
                ),
                "after_active_writer": copy.deepcopy(
                    sol_after.get("active_writer")
                ),
                "before_formal_write_count": sol_before.get(
                    "formal_write_count"
                ),
                "after_formal_write_count": sol_after.get(
                    "formal_write_count"
                ),
                "formal_write_count_delta": sol_delta,
            },
            "subjects": {
                "english": {
                    "sol_authorized": False,
                    "parent_sol_batch_handoff_eligible": english_closed,
                },
                "math": {
                    "sol_authorized": False,
                    "parent_sol_batch_handoff_eligible": False,
                },
                "cs408": {
                    "sol_authorized": False,
                    "parent_sol_batch_handoff_eligible": False,
                },
            },
        },
        "model_call_count": stage_submission_count,
        "formal_write_count": 0,
        "completed_at": _now(),
    }
    return _publish_summary(runtime, key, core)


__all__ = [
    "MixedLunaStressError",
    "RUN_COUNTS",
    "SELECTION_SCHEMA",
    "STAGE_ORDER",
    "processing_host_authority_refresher",
    "SUMMARY_SCHEMA",
    "run_mixed_stress",
    "validate_selection",
    "verify_summary",
]
