#!/usr/bin/env python3
"""Deterministic projection merge for the read-only dashboard.

Only dispatcher decisions and immutable task-event indexes are read. Package
payloads, prompts, task-detail bodies, model output and private traces are never
opened while producing the list projection.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

if __package__:
    from .concurrent_dispatch import DispatchError, FrozenTask, LeaseStore
    from .subject_sol_contract import (
        SubjectSolContractError,
        SubjectSolRuntimeStore,
        validate_english_legacy_recuration_sol_batch_v1,
        validate_subject_luna_batch_v1,
        validate_subject_luna_batch_v2,
    )
else:
    from concurrent_dispatch import (  # type: ignore[no-redef]
        DispatchError,
        FrozenTask,
        LeaseStore,
    )
    from subject_sol_contract import (  # type: ignore[no-redef]
        SubjectSolContractError,
        SubjectSolRuntimeStore,
        validate_english_legacy_recuration_sol_batch_v1,
        validate_subject_luna_batch_v1,
        validate_subject_luna_batch_v2,
    )


SUBJECTS = ("math", "cs408", "english")
QUEUE_STATES = (
    "queued",
    "running",
    "ready",
    "needs_rework",
    "failed",
    "stale",
    "evidence_pending",
)
REQUIRED_MODEL = "gpt-5.6-luna"
REQUIRED_REASONING_EFFORT = "max"
SUBJECT_SCHEMA = "study-intake-dispatch-subject-projection-v1"
MAIN_SCHEMA = "study-intake-dashboard-projection-v5"
PREVIOUS_MAIN_SCHEMA = "study-intake-dashboard-projection-v4"
LEGACY_MAIN_SCHEMA = "study-intake-dashboard-projection-v3"
MODERN_MAIN_SCHEMAS = frozenset({PREVIOUS_MAIN_SCHEMA, MAIN_SCHEMA})
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
SAFE_CODE = re.compile(r"^[A-Za-z0-9._:-]{1,160}$")
SAFE_MCP_EVIDENCE_REF = re.compile(
    r"^mcp-item:(math|cs408|english):[0-9a-f]{64}$"
)
MAX_SUBJECT_BYTES = 2 * 1024 * 1024
MAX_INDEX_BYTES = 32 * 1024
MAX_EVENT_INDEXES_PER_TASK = 256
MAX_CONTROL_STATE_BYTES = 2 * 1024 * 1024
MAX_CANARY_QUEUE_ENTRIES = 4096
MAX_PRECLAIM_ATTEMPTS = 256
MAX_EN_P0_BATCH_BYTES = 4 * 1024 * 1024
MAX_EN_P0_SAFE_RECEIPT_BYTES = 512 * 1024
MAX_EN_P0_RUN_SUMMARIES = 1024
SUBJECT_BATCH_SCHEMA = "subject_luna_batch_v1"
SUBJECT_BATCH_V2_SCHEMA = "subject_luna_batch_v2"
GLOBAL_SOL_SCHEMA = "global_sol_writer_lease_v1"
SUBJECT_SOL_WRITER_SCHEMA = "subject_sol_writer_state_v2"
SUBJECT_WRITER_ADAPTERS = {
    "math": "math_nightly_writer_v1",
    "cs408": "cs408_daily_intake_writer_v1",
    "english": "english_daily_intake_writer_v1",
}
GLOBAL_SOL_QUEUE_STATES = {
    "queued",
    "active",
    "reviewed",
    "committed",
    "failed",
    "safe_paused",
}
EN_P0_WORK_ITEM_BATCH_SCHEMA = "english_legacy_recuration_work_item_batch_v1"
EN_P0_COMPLETION_BINDING_SCHEMA = "english_legacy_recuration_completion_binding_v1"
EN_P0_ATTEMPT_CLAIM_SCHEMA = "english_legacy_recuration_attempt_claim_v1"
SUBJECT_BATCH_TASK_STATES = {
    "selected",
    "queued",
    "retrying",
    "analysis_running",
    "critical_review_running",
    "quality_pending",
    "quality_passed",
    "needs_rework",
    "failed",
    "evidence_pending",
}
SUBJECT_BATCH_V2_TASK_STATES = {
    "selected",
    "claimed",
    "analysis_running",
    "critical_review_running",
    "packaging",
    "workflow_complete",
    "workflow_complete_with_warnings",
    "workflow_partial",
    "execution_failed",
    "cancelled",
    "stalled",
}
SUBJECT_BATCH_V2_TERMINAL_STATES = {
    "workflow_complete",
    "workflow_complete_with_warnings",
    "workflow_partial",
    "execution_failed",
    "cancelled",
    "stalled",
}
SUBJECT_BATCH_V2_CANDIDATE_STATES = {
    "workflow_complete",
    "workflow_complete_with_warnings",
}
DASHBOARD_V4_ITEM_FIELDS = {
    "evidence_access_status",
    "analysis_execution_status",
    "analysis_report_status",
    "review_execution_status",
    "review_report_status",
    "formal_write_status",
    "warning_codes",
    "elapsed_runtime_seconds",
    "last_meaningful_progress_at",
    "soft_timeout_warning",
    "stall_probe_status",
    "exact_error_code",
    "authority_snapshot_sha256",
    "analysis_execution_receipt_sha256",
    "analysis_raw_output_sha256",
    "analysis_normalization_receipt_sha256",
    "analysis_report_sha256",
    "critical_review_execution_receipt_sha256",
    "critical_review_raw_output_sha256",
    "critical_review_normalization_receipt_sha256",
    "critical_review_report_sha256",
    "sol_handoff_envelope_sha256",
    "terminal_receipt_sha256",
    "preclaim_failure_stage",
    "preclaim_failed_at",
    "primary_preclaim_failure_receipt_sha256",
    "queue_preserved_for_recovery",
    "recovery_status",
    "preclaim_attempt_count",
    "latest_preclaim_attempt_at",
    "latest_preclaim_attempt_error_code",
    "preclaim_attempt_history",
}
DASHBOARD_V5_ITEM_FIELDS = DASHBOARD_V4_ITEM_FIELDS | {
    "report_available",
    "formal_write_eligible",
}
PROCESSING_SKILLS = {
    "math": "background-math-processing",
    "cs408": "background-cs408-processing",
    "english": "background-english-processing",
}
PRODUCTION_CANARY_STATES = {
    "armed",
    "canary_in_flight",
    "continuous_concurrent_unlocked",
    "failed_drained",
    "paused_drained",
    "inactive_rolled_back",
}
CONCURRENCY_OBSERVATION_SCHEMA = (
    "study-intake-dashboard-concurrency-observation-v1"
)
CONCURRENCY_SOURCE = "lease_store_subject_status_v1"
TERMINAL_OUTCOMES = (
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "stalled",
    "needs_rework",
)
LEGACY_TERMINAL_OUTCOMES = tuple(
    outcome for outcome in TERMINAL_OUTCOMES if outcome != "stalled"
)
TERMINAL_FAILURE_OUTCOMES = set(TERMINAL_OUTCOMES) - {"succeeded"}
CONCURRENCY_COUNTER_SCOPE = (
    "same_release_current_activation_task_process_lifecycle"
)
CONCURRENCY_PEAK_SOURCE = (
    "hmac_task_process_identity_and_exit_half_open_intervals"
)
CONCURRENCY_RUNNER_INTERVAL_POLICY = (
    "half_open_end_before_start_zero_duration_nonoverlap"
)
CONCURRENCY_STATES = {
    "first_canary_armed",
    "first_canary_single_in_flight",
    "continuous_concurrent_unlocked",
    "failed_drained",
    "paused_drained",
    "inactive_rolled_back",
    "canary_not_configured",
}
SAFE_DECISION_KEYS = {
    "subject",
    "capture_id",
    "study_date",
    "target_label",
    "input_fingerprint",
    "eligible",
    "reason",
    "unit_sha256",
    "frozen_payload_sha256",
    "release_id",
    "rule_version",
    "rule_version_sha256",
    "subject_processing_contract_sha256",
    "semantic_contract_sha256",
    "processing_contract_sha256",
    "generation",
    "phase",
    "model",
    "reasoning_effort",
    "model_enqueue_allowed",
    "evidence_status",
    "evidence_manifest_sha256",
    "evidence_bundle_sha256",
    "evidence_readiness_receipt_sha256",
    "legacy_compatibility_receipt_sha256",
    "semantic_reuse_receipt_sha256",
    "semantic_reuse_source_release_id",
    "package_sha256",
    "superseded_input_fingerprint",
    "projection_origin",
    "error_code",
    "updated_at",
}
EVENT_TO_STATE = {
    "claim": ("running", "frozen_evidence", "processing"),
    "claimed": ("running", "frozen_evidence", "processing"),
    "process_started": ("running", "analysis", "processing"),
    "child_process_started": ("running", "analysis", "processing"),
    "child_process_exited": ("running", "critical_review", "processing"),
    "provider_process_started": ("running", "analysis", "processing"),
    "provider_process_exited": ("running", "analysis", "processing"),
    "provider_progress": ("running", "analysis", "processing"),
    "provider_heartbeat": ("running", "analysis", "processing"),
    "mcp_call_started": ("running", "analysis", "processing"),
    "mcp_call_completed": ("running", "analysis", "processing"),
    "mcp_receipt_published": ("running", "analysis", "processing"),
    "raw_output_persisted": ("running", "analysis", "processing"),
    "normalization_started": ("running", "analysis", "processing"),
    "normalization_completed": ("running", "analysis", "processing"),
    "normalization_warning": ("running", "analysis", "processing"),
    "stage_progress": ("running", "analysis", "processing"),
    "soft_timeout_warning": ("running", "analysis", "processing"),
    "stall_probe_started": ("running", "analysis", "processing"),
    "stall_probe_succeeded": ("running", "analysis", "processing"),
    "stall_probe_failed": ("running", "analysis", "processing"),
    "stall_suspected": ("running", "analysis", "processing"),
    "model_submitted": ("running", "analysis", "processing"),
    "analysis_submitted": ("running", "analysis", "processing"),
    "analysis_completed": ("running", "critical_review", "processing"),
    "analysis_checkpoint_reused": ("running", "critical_review", "processing"),
    "critical_started": ("running", "critical_review", "processing"),
    "critical_completed": ("running", "critical_review", "processing"),
    "recovery_scheduled": ("queued", "analysis", "retrying"),
    "retry_wait": ("queued", "analysis", "retrying"),
    "published": ("ready", "quality_ready", "two_pass_ready"),
    "workflow_complete": ("ready", "quality_ready", "two_pass_ready"),
    "workflow_complete_with_warnings": (
        "ready",
        "quality_ready",
        "two_pass_ready",
    ),
    "workflow_partial": ("failed", "failed", "failed"),
    "execution_failed": ("failed", "failed", "failed"),
    "stalled": ("failed", "failed", "failed"),
    "cancelled": ("stale", "stale", "stale"),
    "needs_rework": ("needs_rework", "needs_rework", "failed"),
    "failed": ("failed", "failed", "failed"),
    "timeout": ("failed", "failed", "failed"),
    "cancel": ("stale", "stale", "stale"),
}
SERVICE_RETRY_CODES = {
    "luna_rate_limited",
    "luna_service_rate_limited",
    "luna_service_unavailable",
    "model_rate_limited",
    "service_rate_limited",
    "upstream_rate_limited",
    "upstream_http_429",
}


class DashboardProjectionError(RuntimeError):
    pass


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _processing_component_identity(
    config: Mapping[str, Any], subject: str
) -> dict[str, Any]:
    """Return only hash-bound public component identity from the plugin lock."""

    processing = config.get("processing_plugin")
    if not isinstance(processing, Mapping) or processing.get("enabled") is not True:
        return {}
    raw_path = processing.get("component_lock_path")
    if not isinstance(raw_path, str):
        return {}
    lock_path = Path(raw_path)
    lock = _bounded_object(lock_path, 512 * 1024)
    if not isinstance(lock, Mapping):
        return {}
    skill_id = PROCESSING_SKILLS[subject]
    skills = lock.get("skills")
    skill = skills.get(skill_id) if isinstance(skills, Mapping) else None
    server_names = lock.get("mcp_server_names")
    server_id = (
        server_names.get(subject)
        if isinstance(server_names, Mapping)
        else None
    )
    expected_server_id = {
        "math": "kaoyan_math_read",
        "cs408": "kaoyan_cs408_read",
        "english": "kaoyan_english_read",
    }[subject]
    if (
        lock.get("schema_version")
        != "kaoyan-study-intake-component-lock.v1"
        or lock.get("plugin_name") != "kaoyan-study-intake"
        or not isinstance(lock.get("plugin_version"), str)
        or not isinstance(lock.get("minimum_mcp_server_release"), str)
        or not isinstance(lock.get("mcp_server_release"), str)
        or not str(lock.get("mcp_server_release")).startswith(
            f"{lock.get('minimum_mcp_server_release')}+sha256."
        )
        or SHA256.fullmatch(
            str(lock.get("mcp_server_release")).rsplit("+sha256.", 1)[-1]
        )
        is None
        or not isinstance(skill, Mapping)
        or server_id != expected_server_id
        or not isinstance(skill.get("version"), str)
        or not isinstance(skill.get("sha256"), str)
        or SHA256.fullmatch(str(skill.get("sha256"))) is None
    ):
        return {}
    try:
        lock_sha256 = hashlib.sha256(lock_path.read_bytes()).hexdigest()
    except OSError:
        return {}
    return {
        "plugin_id": "kaoyan-study-intake",
        "plugin_version": str(lock["plugin_version"]),
        "plugin_component_lock_sha256": lock_sha256,
        "processing_skill_id": skill_id,
        "processing_skill_version": str(skill["version"]),
        "processing_skill_sha256": str(skill["sha256"]),
        "mcp_server_id": str(server_id),
        "mcp_server_release": str(lock.get("mcp_server_release") or ""),
        "minimum_mcp_server_release": str(
            lock["minimum_mcp_server_release"]
        ),
    }


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_temp)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _atomic_bytes(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_temp)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _subject_projection_path(
    runtime_root: Path, subject: str, study_date: str
) -> Path:
    return (
        runtime_root
        / "dispatch"
        / "state"
        / "subject-projections"
        / "by-date"
        / study_date
        / f"{subject}.json"
    )


def _archive_projection_path(runtime_root: Path, study_date: str) -> Path:
    return runtime_root / "state" / "dashboard-projections" / f"{study_date}.json"


@contextlib.contextmanager
def _short_lock(path: Path, timeout_seconds: float = 3.0) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise DashboardProjectionError("dashboard_projection_lock_busy")
                time.sleep(0.02)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _bounded_object(path: Path, max_bytes: int) -> dict[str, Any] | None:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or node.st_size > max_bytes:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _verified_dispatch_object(
    runtime_root: Path,
    path: Path,
    *,
    purpose: str,
    max_bytes: int,
    expected_sha256: str | None = None,
    allowed_root: Path | None = None,
) -> tuple[dict[str, Any], str] | None:
    """Reopen one HMAC-sealed dispatcher object without creating state."""

    try:
        resolved = path.resolve(strict=True)
        if allowed_root is not None:
            resolved.relative_to(allowed_root.resolve(strict=True))
        before = resolved.lstat()
        if resolved.is_symlink() or not resolved.is_file() or before.st_size > max_bytes:
            return None
        raw = resolved.read_bytes()
        after = resolved.lstat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            return None
        digest = hashlib.sha256(raw).hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            return None
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            return None
        LeaseStore(runtime_root)._verify_seal_read_only(value, purpose=purpose)
    except (
        DispatchError,
        FileNotFoundError,
        OSError,
        UnicodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        return None
    return value, digest


def _content_addressed_task(
    runtime_root: Path,
    queue_entry: Mapping[str, Any],
    *,
    subject: str,
) -> dict[str, Any] | None:
    digest = _safe_hash(queue_entry.get("task_object_sha256"))
    raw_path = queue_entry.get("task_object_path")
    if digest is None or not isinstance(raw_path, str):
        return None
    root = (
        runtime_root / "dispatch" / "production-canary" / "tasks" / subject
    )
    expected = root / "sha256" / digest[:2] / f"{digest}.json"
    if Path(raw_path).resolve(strict=False) != expected.resolve(strict=False):
        return None
    try:
        resolved = expected.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        node = resolved.lstat()
        if resolved.is_symlink() or not resolved.is_file() or node.st_size > MAX_CONTROL_STATE_BYTES:
            return None
        raw = resolved.read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            return None
        value = json.loads(raw.decode("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, ValueError, json.JSONDecodeError):
        return None
    try:
        task = FrozenTask.from_mapping(value)
    except (DispatchError, KeyError, TypeError, ValueError):
        return None
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "study-intake-frozen-task-v1"
        or task.unit_sha256 != queue_entry.get("unit_sha256")
        or task.frozen_payload_sha256 != queue_entry.get("frozen_payload_sha256")
    ):
        return None
    frozen_payload = dict(task.frozen_payload)
    if frozen_payload.get("subject") != subject:
        return None
    return task.as_dict()


def _preclaim_attempt_row(
    receipt: Mapping[str, Any], receipt_sha256: str, *, primary_sha256: str
) -> dict[str, Any] | None:
    evidence = receipt.get("failure_evidence")
    failed_at = _safe_text(receipt.get("failed_at"), 64)
    stage = _safe_text(receipt.get("failure_stage"), 64)
    error_code = _safe_text(receipt.get("error_code"), 160)
    unit_sha256 = _safe_hash(evidence.get("unit_sha256")) if isinstance(evidence, Mapping) else None
    frozen_sha256 = (
        _safe_hash(evidence.get("frozen_payload_sha256"))
        if isinstance(evidence, Mapping)
        else None
    )
    if (
        failed_at is None
        or stage is None
        or error_code is None
        or SAFE_CODE.fullmatch(stage) is None
        or SAFE_CODE.fullmatch(error_code) is None
        or unit_sha256 is None
        or frozen_sha256 is None
        or receipt.get("model_submission_started") is not False
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("mcp_tool_call_count") != 0
        or receipt.get("formal_write_count") != 0
        or receipt.get("sol_enabled") is not False
        or receipt.get("state_after") != "failed_drained"
    ):
        return None
    return {
        "receipt_sha256": receipt_sha256,
        "unit_sha256": unit_sha256,
        "frozen_payload_sha256": frozen_sha256,
        "failure_stage": stage,
        "error_code": error_code,
        "failed_at": failed_at,
        "queue_entry_preserved": receipt.get("queue_entry_preserved") is True,
        "primary": receipt_sha256 == primary_sha256,
    }


def _preserved_preclaim_failures(
    runtime_root: Path,
    *,
    subject: str,
    release_id: str,
    canary_gate: Mapping[str, Any] | None,
    study_date: str,
    generated_at: str,
) -> list[dict[str, Any]]:
    """Project preserved terminal pre-claim failures by stable producer identity."""

    activation_id = (
        _safe_hash(canary_gate.get("activation_id"))
        if isinstance(canary_gate, Mapping)
        else None
    )
    if activation_id is None:
        return []
    queue_root = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-queue"
        / subject
        / activation_id
    )
    if not queue_root.is_dir() or queue_root.is_symlink():
        return []
    queue_paths = sorted(queue_root.glob("*.json"))
    if len(queue_paths) > MAX_CANARY_QUEUE_ENTRIES:
        return []

    attempt_root = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-preclaim-failures"
        / subject
        / activation_id
    )
    verified_attempts: list[tuple[dict[str, Any], str]] = []
    if attempt_root.is_dir() and not attempt_root.is_symlink():
        attempt_paths = sorted(attempt_root.glob("*.json"))
        if len(attempt_paths) <= MAX_PRECLAIM_ATTEMPTS:
            for attempt_path in attempt_paths:
                reopened = _verified_dispatch_object(
                    runtime_root,
                    attempt_path,
                    purpose="dispatch-production-canary-preclaim-failure",
                    max_bytes=MAX_CONTROL_STATE_BYTES,
                    allowed_root=attempt_root,
                )
                if reopened is not None:
                    verified_attempts.append(reopened)

    projected: list[dict[str, Any]] = []
    for queue_path in queue_paths:
        reopened_queue = _verified_dispatch_object(
            runtime_root,
            queue_path,
            purpose="dispatch-production-canary-queue",
            max_bytes=MAX_CONTROL_STATE_BYTES,
            allowed_root=queue_root,
        )
        if reopened_queue is None:
            continue
        queue_entry, _queue_sha256 = reopened_queue
        queue_contract_sha256 = _safe_hash(
            queue_entry.get("producer_input_contract_sha256")
        )
        primary_sha256 = _safe_hash(queue_entry.get("terminal_receipt_sha256"))
        primary_path = queue_entry.get("terminal_receipt_path")
        if (
            queue_entry.get("schema_version")
            != "study-intake-production-canary-queue-entry-v2"
            or queue_entry.get("subject") != subject
            or queue_entry.get("release_id") != release_id
            or queue_entry.get("activation_id") != activation_id
            or queue_entry.get("queue_status") != "pending"
            or queue_entry.get("terminal_outcome") != "failed"
            or queue_entry.get("formal_write_count") != 0
            or queue_contract_sha256 is None
            or queue_path.name != f"{queue_contract_sha256}.json"
            or primary_sha256 is None
            or not isinstance(primary_path, str)
        ):
            continue
        receipt_root = (
            runtime_root
            / "dispatch"
            / "production-canary"
            / "receipts"
            / subject
            / activation_id
        )
        expected_receipt_path = (
            receipt_root
            / "sha256"
            / primary_sha256[:2]
            / f"{primary_sha256}.json"
        )
        if Path(primary_path).resolve(strict=False) != expected_receipt_path.resolve(strict=False):
            continue
        reopened_primary = _verified_dispatch_object(
            runtime_root,
            expected_receipt_path,
            purpose="dispatch-production-canary-preclaim-failure",
            max_bytes=MAX_CONTROL_STATE_BYTES,
            expected_sha256=primary_sha256,
            allowed_root=receipt_root,
        )
        task_object = _content_addressed_task(runtime_root, queue_entry, subject=subject)
        if reopened_primary is None or task_object is None:
            continue
        primary = reopened_primary[0]
        evidence = primary.get("failure_evidence")
        frozen_payload = task_object["frozen_payload"]
        producer_unit_id = _safe_control_id(queue_entry.get("producer_unit_id"))
        capture_id = _safe_control_id(frozen_payload.get("capture_id"))
        source_event_set = _safe_hash(queue_entry.get("source_event_set_sha256"))
        input_fingerprint = _safe_text(frozen_payload.get("input_fingerprint"), 320)
        if (
            primary.get("schema_version")
            != "study-intake-production-canary-preclaim-failure-receipt-v2"
            or primary.get("subject") != subject
            or primary.get("release_id") != release_id
            or primary.get("activation_id") != activation_id
            or primary.get("queue_entry_preserved") is not True
            or primary.get("queue_status_after") != "pending"
            or primary.get("model_submission_started") is not False
            or primary.get("model_call_count") != 0
            or primary.get("provider_request_count") != 0
            or primary.get("mcp_tool_call_count") != 0
            or primary.get("formal_write_count") != 0
            or primary.get("sol_enabled") is not False
            or primary.get("state_after") != "failed_drained"
            or not isinstance(evidence, Mapping)
            or evidence.get("unit_sha256") != queue_entry.get("unit_sha256")
            or evidence.get("frozen_payload_sha256")
            != queue_entry.get("frozen_payload_sha256")
            or evidence.get("producer_input_contract_sha256")
            != queue_entry.get("producer_input_contract_sha256")
            or evidence.get("producer_unit_id") != queue_entry.get("producer_unit_id")
            or evidence.get("source_event_set_sha256") != source_event_set
            or evidence.get("capture_id") != capture_id
            or queue_entry.get("terminal_error_code") != primary.get("error_code")
            or frozen_payload.get("study_date") != study_date
            or producer_unit_id is None
            or capture_id is None
            or source_event_set is None
            or input_fingerprint is None
        ):
            continue
        attempts: list[dict[str, Any]] = []
        for attempt, attempt_sha256 in verified_attempts:
            attempt_evidence = attempt.get("failure_evidence")
            if (
                attempt.get("schema_version")
                != "study-intake-production-canary-preclaim-failure-receipt-v2"
                or attempt.get("subject") != subject
                or attempt.get("release_id") != release_id
                or attempt.get("activation_id") != activation_id
                or not isinstance(attempt_evidence, Mapping)
                or attempt_evidence.get("producer_unit_id") != producer_unit_id
                or attempt_evidence.get("source_event_set_sha256") != source_event_set
            ):
                continue
            row = _preclaim_attempt_row(
                attempt,
                attempt_sha256,
                primary_sha256=primary_sha256,
            )
            if row is not None:
                attempts.append(row)
        if not any(row["primary"] for row in attempts):
            primary_row = _preclaim_attempt_row(
                primary,
                primary_sha256,
                primary_sha256=primary_sha256,
            )
            if primary_row is None:
                continue
            attempts.append(primary_row)
        attempts.sort(key=lambda row: (row["failed_at"], row["receipt_sha256"]))
        primary_row = next(row for row in attempts if row["primary"])
        latest_row = attempts[-1]
        dispatch_contract = frozen_payload.get("dispatch_contract")
        decision: dict[str, Any] = {
            "subject": subject,
            "capture_id": capture_id,
            "study_date": frozen_payload.get("study_date"),
            "target_label": frozen_payload.get("target_label") or capture_id,
            "input_fingerprint": input_fingerprint,
            "eligible": False,
            "reason": primary_row["error_code"],
            "error_code": primary_row["error_code"],
            "unit_sha256": queue_entry.get("unit_sha256"),
            "frozen_payload_sha256": queue_entry.get("frozen_payload_sha256"),
            "release_id": release_id,
            "rule_version": "study-intake-concurrent-dispatch-contract-v1",
            "model_enqueue_allowed": False,
            "updated_at": primary_row["failed_at"],
        }
        if isinstance(dispatch_contract, Mapping):
            for key in ("rule_version_sha256", "subject_processing_contract_sha256"):
                digest = _safe_hash(dispatch_contract.get(key))
                if digest is not None:
                    decision[key] = digest
        item = _item_from_decision(
            runtime_root,
            decision,
            subject=subject,
            generated_at=generated_at,
        )
        if item is None:
            continue
        binding = frozen_payload.get("input_binding")
        batch_trigger = (
            _safe_text(binding.get("batch_trigger"), 160)
            if isinstance(binding, Mapping)
            else None
        )
        item.update(
            {
                "queue_state": "failed",
                "current_stage": "failed",
                "luna_status": "failed",
                "local_dispatch_status": "terminal",
                "model_stage": "not_started",
                "terminal_status": "failed",
                "analysis_execution_status": "not_started",
                "analysis_report_status": "not_started",
                "review_execution_status": "not_started",
                "review_report_status": "not_started",
                "sol_review_status": "not_eligible",
                "local_model_submitted": False,
                "last_error_code": primary_row["error_code"],
                "processing_error_code": primary_row["error_code"],
                "exact_error_code": primary_row["error_code"],
                "last_state_change_at": primary_row["failed_at"],
                "terminal_receipt_sha256": primary_sha256,
                "preclaim_failure_stage": primary_row["failure_stage"],
                "preclaim_failed_at": primary_row["failed_at"],
                "primary_preclaim_failure_receipt_sha256": primary_sha256,
                "queue_preserved_for_recovery": True,
                "recovery_status": "waiting_explicit_resume",
                "preclaim_attempt_count": len(attempts),
                "latest_preclaim_attempt_at": latest_row["failed_at"],
                "latest_preclaim_attempt_error_code": latest_row["error_code"],
                "preclaim_attempt_history": attempts,
            }
        )
        if batch_trigger is not None:
            item["batch_trigger"] = batch_trigger
        projected.append(item)
    return projected


def _safe_text(value: Any, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or len(stripped) > limit or any(ord(char) < 32 for char in stripped):
        return None
    return stripped


def _configured_release_id(
    config: Mapping[str, Any],
    decisions: Iterable[Mapping[str, Any]],
) -> str:
    release = config.get("release")
    if isinstance(release, Mapping):
        manifest_path = release.get("manifest_path")
        if isinstance(manifest_path, str):
            manifest = _bounded_object(Path(manifest_path), 1024 * 1024)
            value = manifest.get("release_id") if manifest is not None else None
            if isinstance(value, str) and SHA256.fullmatch(value):
                return value
    for decision in decisions:
        value = decision.get("release_id")
        if isinstance(value, str) and SHA256.fullmatch(value):
            return value
    return "0" * 64


def _configured_dashboard_schema(config: Mapping[str, Any]) -> str:
    dashboard = config.get("dashboard")
    configured = (
        dashboard.get("projection_schema_version")
        if isinstance(dashboard, Mapping)
        else None
    )
    return (
        str(configured)
        if configured in {*MODERN_MAIN_SCHEMAS, LEGACY_MAIN_SCHEMA}
        else LEGACY_MAIN_SCHEMA
    )


def _configured_continuous_concurrency_limit(config: Mapping[str, Any]) -> int:
    dispatch = config.get("dispatch")
    canary = dispatch.get("production_canary") if isinstance(dispatch, Mapping) else None
    value = canary.get("continuous_concurrency_limit") if isinstance(canary, Mapping) else None
    return int(value) if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 64 else 20


def _capacity_projection(
    canary_gate: Mapping[str, Any] | None,
    *,
    configured_continuous_limit: int,
) -> dict[str, Any]:
    if (
        canary_gate is None
        or canary_gate.get("luna_consumer_enabled") is not True
        or canary_gate.get("state")
        in {"failed_drained", "paused_drained", "inactive_rolled_back"}
    ):
        mode = "disabled"
        effective = 0
    elif canary_gate.get("state") == "continuous_concurrent_unlocked":
        mode = "continuous_concurrent"
        effective = int(canary_gate.get("continuous_concurrency_limit") or configured_continuous_limit)
    else:
        mode = "initial_canary"
        effective = 1
    return {
        "capacity_mode": mode,
        "initial_canary_inflight_limit": 1,
        "continuous_concurrency_limit": configured_continuous_limit,
        "effective_concurrency_limit": effective,
    }


def _safe_decision(
    raw: Mapping[str, Any],
    *,
    subject: str,
    study_date: str,
    updated_at: str,
) -> dict[str, Any] | None:
    capture_id = _safe_text(raw.get("capture_id"), 160)
    if capture_id is None or SAFE_ID.fullmatch(capture_id) is None:
        return None
    row = {key: raw.get(key) for key in SAFE_DECISION_KEYS if key in raw}
    row["subject"] = subject
    row["capture_id"] = capture_id
    row["study_date"] = _safe_text(raw.get("study_date"), 10) or study_date
    row["updated_at"] = _safe_text(raw.get("updated_at"), 64) or updated_at
    input_fingerprint = _safe_text(raw.get("input_fingerprint"), 320)
    if input_fingerprint is None:
        row.pop("input_fingerprint", None)
    else:
        row["input_fingerprint"] = input_fingerprint
    target_label = _safe_text(raw.get("target_label"), 160)
    if target_label is None:
        row.pop("target_label", None)
    else:
        row["target_label"] = target_label
    for key in (
        "unit_sha256",
        "frozen_payload_sha256",
        "release_id",
        "rule_version_sha256",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "evidence_readiness_receipt_sha256",
        "legacy_compatibility_receipt_sha256",
        "semantic_reuse_receipt_sha256",
        "semantic_reuse_source_release_id",
        "package_sha256",
        "superseded_input_fingerprint",
        "subject_processing_contract_sha256",
        "semantic_contract_sha256",
        "processing_contract_sha256",
    ):
        value = row.get(key)
        if value is not None and (not isinstance(value, str) or SHA256.fullmatch(value) is None):
            row.pop(key, None)
    for key in (
        "reason",
        "error_code",
        "rule_version",
        "phase",
        "model",
        "reasoning_effort",
        "evidence_status",
        "projection_origin",
    ):
        value = _safe_text(row.get(key), 160)
        if value is None:
            row.pop(key, None)
        else:
            row[key] = value
    row["eligible"] = raw.get("eligible") is True
    row["model_enqueue_allowed"] = raw.get("model_enqueue_allowed") is not False
    generation = raw.get("generation")
    if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0:
        row["generation"] = generation
    else:
        row.pop("generation", None)
    return row


def _merge_decisions(
    previous: Sequence[Mapping[str, Any]],
    current: Sequence[Mapping[str, Any]],
    *,
    subject: str,
    study_date: str,
    updated_at: str,
) -> list[dict[str, Any]]:
    by_capture: dict[str, dict[str, Any]] = {}
    for source in (previous, current):
        for raw in source:
            if not isinstance(raw, Mapping):
                continue
            safe = _safe_decision(
                raw,
                subject=subject,
                study_date=study_date,
                updated_at=updated_at,
            )
            if safe is not None and safe.get("study_date") == study_date:
                by_capture[safe["capture_id"]] = safe
    ordered = sorted(
        by_capture.values(),
        key=lambda row: (str(row.get("updated_at", "")), row["capture_id"]),
        reverse=True,
    )
    return ordered


def _verified_event_for_index(
    runtime_root: Path,
    unit_sha256: str,
    index: Mapping[str, Any],
) -> dict[str, Any] | None:
    event_sha = index.get("event_sha256")
    if not isinstance(event_sha, str) or SHA256.fullmatch(event_sha) is None:
        return None
    expected = (
        runtime_root
        / "dispatch"
        / "events"
        / "sha256"
        / event_sha[:2]
        / f"{event_sha}.json"
    ).resolve(strict=False)
    event_path = index.get("event_path")
    if not isinstance(event_path, str) or Path(event_path).resolve(strict=False) != expected:
        return None
    try:
        if expected.is_symlink() or not expected.is_file() or expected.stat().st_size > 64 * 1024:
            return None
        raw = expected.read_bytes()
        if hashlib.sha256(raw).hexdigest() != event_sha:
            return None
        event = json.loads(raw.decode("utf-8"))
        key_path = runtime_root / "dispatch" / "state" / "authority.key"
        if key_path.is_symlink() or not key_path.is_file() or key_path.stat().st_size > 1024:
            return None
        key = key_path.read_bytes()
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(event, dict):
        return None
    authority = event.get("authority")
    core = dict(event)
    core.pop("authority", None)
    expected_hmac = hmac.new(
        key,
        _canonical_bytes(
            {"purpose": "dispatch-task-event", "payload": core}
        ).rstrip(b"\n"),
        hashlib.sha256,
    ).hexdigest()
    if (
        not isinstance(authority, Mapping)
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("purpose") != "dispatch-task-event"
        or authority.get("key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(str(authority.get("hmac_sha256") or ""), expected_hmac)
        or event.get("schema_version") not in {
            "study-intake-dispatch-task-event-v1",
            "study-intake-dispatch-task-event-v2",
        }
        or event.get("unit_sha256") != unit_sha256
        or event.get("fence") != index.get("fence")
        or event.get("attempt") != index.get("attempt")
        or event.get("event") != index.get("event")
        or event.get("formal_write_count") != 0
    ):
        return None
    return event


def _event_timeline(runtime_root: Path, unit_sha256: str) -> list[dict[str, Any]]:
    root = runtime_root / "dispatch" / "state" / "task-events" / unit_sha256
    if not root.is_dir() or root.is_symlink():
        return []
    paths = sorted(root.glob("fence-*/*.json"))
    if len(paths) > MAX_EVENT_INDEXES_PER_TASK:
        paths = paths[-MAX_EVENT_INDEXES_PER_TASK:]
    valid: list[tuple[int, int, dict[str, Any]]] = []
    for path in paths:
        value = _bounded_object(path, MAX_INDEX_BYTES)
        if value is None:
            continue
        fence = value.get("fence")
        sequence = value.get("sequence")
        event = value.get("event")
        event_sha = value.get("event_sha256")
        if (
            value.get("schema_version")
            not in {
                "study-intake-dispatch-task-event-index-v1",
                "study-intake-dispatch-task-event-index-v2",
            }
            or value.get("unit_sha256") != unit_sha256
            or not isinstance(fence, int)
            or fence < 1
            or not isinstance(sequence, int)
            or sequence < 1
            or event not in EVENT_TO_STATE
            or not isinstance(event_sha, str)
            or SHA256.fullmatch(event_sha) is None
            or value.get("formal_write_count") != 0
        ):
            continue
        event_value = _verified_event_for_index(runtime_root, unit_sha256, value)
        if event_value is None:
            continue
        valid.append((fence, sequence, event_value))
    valid.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in valid]


def _verified_review_candidate_projection(
    runtime_root: Path,
    decision: Mapping[str, Any],
    terminal_event: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Reopen one quality report without conflating quality and execution.

    The authenticated task event is only the entry point.  Dashboard review
    availability is projected after the completion, processing receipt,
    content-addressed package/report, v3 queue entry and review terminal
    receipt all bind to the same unit and release.  Any missing link leaves the
    historical needs_rework projection unchanged and fail-closed.
    """

    if terminal_event is None or terminal_event.get("event") not in {
        "published",
        "needs_rework",
    }:
        return None
    execution_succeeded = terminal_event.get("event") == "published"
    expected_outcome = "succeeded" if execution_succeeded else "needs_rework"
    unit_sha256 = _safe_hash(decision.get("unit_sha256"))
    frozen_sha256 = _safe_hash(decision.get("frozen_payload_sha256"))
    release_id = _safe_hash(decision.get("release_id"))
    subject = str(decision.get("subject") or "")
    capture_id = str(decision.get("capture_id") or "")
    artifacts = terminal_event.get("artifacts")
    if (
        unit_sha256 is None
        or frozen_sha256 is None
        or release_id is None
        or subject not in SUBJECTS
        or not isinstance(artifacts, Mapping)
    ):
        return None

    completion_sha256 = _safe_hash(artifacts.get("completion_sha256"))
    completion_path = artifacts.get("completion_path")
    completion_root = runtime_root / "dispatch" / "state" / "completions"
    expected_completion = completion_root / f"{unit_sha256}.json"
    if (
        completion_sha256 is None
        or not isinstance(completion_path, str)
        or Path(completion_path).resolve(strict=False)
        != expected_completion.resolve(strict=False)
    ):
        return None
    reopened_completion = _verified_dispatch_object(
        runtime_root,
        expected_completion,
        purpose="dispatch-completion",
        max_bytes=MAX_CONTROL_STATE_BYTES,
        expected_sha256=completion_sha256,
        allowed_root=completion_root,
    )
    if reopened_completion is None:
        return None
    completion = reopened_completion[0]

    receipt_sha256 = _safe_hash(artifacts.get("receipt_sha256"))
    receipt_path = artifacts.get("receipt_path")
    receipt_root = runtime_root / "dispatch" / "receipts"
    expected_receipt = (
        receipt_root
        / "sha256"
        / str(receipt_sha256 or "")[:2]
        / f"{receipt_sha256}.json"
    )
    if (
        receipt_sha256 is None
        or not isinstance(receipt_path, str)
        or Path(receipt_path).resolve(strict=False)
        != expected_receipt.resolve(strict=False)
    ):
        return None
    reopened_receipt = _verified_dispatch_object(
        runtime_root,
        expected_receipt,
        purpose="dispatch-receipt",
        max_bytes=MAX_CONTROL_STATE_BYTES,
        expected_sha256=receipt_sha256,
        allowed_root=receipt_root,
    )
    if reopened_receipt is None:
        return None
    receipt = reopened_receipt[0]

    package_sha256 = _safe_hash(completion.get("package_sha256"))
    report_sha256 = _safe_hash(completion.get("report_json_sha256"))
    report_markdown_sha256 = _safe_hash(
        completion.get("report_markdown_sha256")
    )
    if (
        completion.get("schema_version")
        != "study-intake-concurrent-completion-v2"
        or completion.get("unit_sha256") != unit_sha256
        or completion.get("subject") != subject
        or completion.get("capture_id") != capture_id
        or completion.get("release_id") != release_id
        or completion.get("outcome") != expected_outcome
        or completion.get("receipt_sha256") != receipt_sha256
        or Path(str(completion.get("receipt_path") or "")).resolve(
            strict=False
        )
        != expected_receipt.resolve(strict=False)
        or package_sha256 is None
        or report_sha256 is None
        or report_markdown_sha256 is None
        or completion.get("package_ref")
        != "study-intake-dispatch-package://sha256/" + package_sha256
        or completion.get("report_json_ref")
        != "study-intake-report://sha256/" + report_sha256
        or completion.get("report_markdown_ref")
        != "study-intake-report-markdown://sha256/"
        + report_markdown_sha256
        or receipt.get("schema_version")
        != "study-intake-concurrent-receipt-v1"
        or receipt.get("unit_sha256") != unit_sha256
        or receipt.get("subject") != subject
        or receipt.get("capture_id") != capture_id
        or receipt.get("release_id") != release_id
        or receipt.get("outcome") != expected_outcome
        or receipt.get("package_sha256") != package_sha256
        or receipt.get("report_json_sha256") != report_sha256
        or receipt.get("report_markdown_sha256")
        != report_markdown_sha256
        or receipt.get("formal_write_count") != 0
    ):
        return None

    try:
        review_view = SubjectSolRuntimeStore(
            runtime_root
        ).read_restricted_sol_review_candidate(report_sha256)
    except SubjectSolContractError:
        return None
    package = review_view.get("package")
    report = review_view.get("report")
    disposition = review_view.get("report_disposition")
    if (
        disposition not in {"needs_sol_review", "quarantined"}
        or execution_succeeded and disposition != "needs_sol_review"
        or not isinstance(package, Mapping)
        or not isinstance(report, Mapping)
        or package.get("unit_sha256") != unit_sha256
        or package.get("subject") != subject
        or package.get("capture_id") != capture_id
        or package.get("release_id") != release_id
        or package.get("report_disposition") != disposition
        or report.get("package_sha256") != package_sha256
    ):
        return None

    markdown_root = runtime_root / "dispatch" / "reports" / "markdown"
    markdown_path = (
        markdown_root
        / "sha256"
        / report_markdown_sha256[:2]
        / f"{report_markdown_sha256}.md"
    )
    try:
        markdown_bytes = markdown_path.read_bytes()
    except OSError:
        return None
    if (
        not markdown_bytes.strip()
        or hashlib.sha256(markdown_bytes).hexdigest()
        != report_markdown_sha256
    ):
        return None

    queue_root = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-queue"
        / subject
    )
    if not queue_root.is_dir() or queue_root.is_symlink():
        return None
    queue_paths = sorted(queue_root.glob("*/*.json"))
    if len(queue_paths) > MAX_CANARY_QUEUE_ENTRIES:
        return None
    queue_matches: list[tuple[dict[str, Any], str]] = []
    for queue_path in queue_paths:
        reopened_queue = _verified_dispatch_object(
            runtime_root,
            queue_path,
            purpose="dispatch-production-canary-queue",
            max_bytes=MAX_CONTROL_STATE_BYTES,
            allowed_root=queue_root,
        )
        if reopened_queue is None:
            continue
        queue = reopened_queue[0]
        if (
            queue.get("schema_version")
            == "study-intake-production-canary-queue-entry-v3"
            and queue.get("unit_sha256") == unit_sha256
            and queue.get("frozen_payload_sha256") == frozen_sha256
            and queue.get("release_id") == release_id
            and queue.get("subject") == subject
            and queue.get("queue_status")
            == ("succeeded" if execution_succeeded else disposition)
            and queue.get("terminal_outcome") == expected_outcome
            and queue.get("formal_write_count") == 0
            and (
                not execution_succeeded
                or queue.get("execution_status") == "succeeded"
                and queue.get("quality_status") == "issues_found"
                and queue.get("report_available") is True
                and queue.get("report_disposition") == "needs_sol_review"
                and queue.get("sol_review_status") == "pending"
                and queue.get("formal_write_eligible") is False
                and queue.get("production_accepted") is False
            )
        ):
            queue_matches.append((queue, reopened_queue[1]))
    if len(queue_matches) != 1:
        return None
    queue = queue_matches[0][0]
    activation_id = _safe_hash(queue.get("activation_id"))
    terminal_sha256 = _safe_hash(queue.get("terminal_receipt_sha256"))
    terminal_path = queue.get("terminal_receipt_path")
    terminal_root = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / str(activation_id or "")
    )
    expected_terminal = (
        terminal_root
        / "sha256"
        / str(terminal_sha256 or "")[:2]
        / f"{terminal_sha256}.json"
    )
    if (
        activation_id is None
        or terminal_sha256 is None
        or not isinstance(terminal_path, str)
        or Path(terminal_path).resolve(strict=False)
        != expected_terminal.resolve(strict=False)
    ):
        return None
    reopened_terminal = _verified_dispatch_object(
        runtime_root,
        expected_terminal,
        purpose="dispatch-production-canary-terminal",
        max_bytes=MAX_CONTROL_STATE_BYTES,
        expected_sha256=terminal_sha256,
        allowed_root=terminal_root,
    )
    if reopened_terminal is None:
        return None
    terminal = reopened_terminal[0]
    selected = terminal.get("selected")
    if (
        terminal.get("schema_version")
        != "study-intake-production-canary-review-terminal-receipt-v1"
        or terminal.get("activation_id") != activation_id
        or terminal.get("subject") != subject
        or terminal.get("release_id") != release_id
        or terminal.get("outcome") != expected_outcome
        or terminal.get("completion_sha256") != completion_sha256
        or terminal.get("completion_receipt_sha256") != receipt_sha256
        or terminal.get("package_sha256") != package_sha256
        or terminal.get("report_json_sha256") != report_sha256
        or terminal.get("report_markdown_sha256")
        != report_markdown_sha256
        or terminal.get("report_reopen_status")
        != "json_markdown_package_verified"
        or terminal.get("report_available") is not True
        or terminal.get("report_disposition") != disposition
        or terminal.get("sol_review_status")
        != ("pending" if disposition == "needs_sol_review" else "not_eligible")
        or terminal.get("formal_write_eligible") is not False
        or execution_succeeded
        and (
            terminal.get("execution_status") != "succeeded"
            or terminal.get("quality_status") != "issues_found"
            or terminal.get("production_accepted") is not False
        )
        or not execution_succeeded
        and (
            terminal.get("execution_status") != "failed"
            or terminal.get("quality_status") != "unchecked"
            or terminal.get("production_accepted") is not False
            or not isinstance(terminal.get("error_code"), str)
            or not str(terminal.get("error_code")).strip()
        )
        or terminal.get("formal_write_count") != 0
        or int(terminal.get("observed_model_call_count") or 0) < 1
        or int(terminal.get("observed_provider_request_count") or 0) < 2
        or int(terminal.get("observed_mcp_tool_call_count") or 0) < 1
        or not isinstance(selected, Mapping)
        or selected.get("unit_sha256") != unit_sha256
        or selected.get("frozen_payload_sha256") != frozen_sha256
    ):
        return None

    stage_runtime = package.get("stage_runtime")
    critical_runtime = (
        stage_runtime.get("critical_review")
        if isinstance(stage_runtime, Mapping)
        else None
    )
    warnings = (
        report.get("findings")
        if isinstance(report.get("findings"), list)
        else report.get("warnings")
    )
    warning_codes = sorted(
        {
            str(row.get("code"))
            for row in warnings
            if isinstance(row, Mapping)
            and isinstance(row.get("code"), str)
            and SAFE_CODE.fullmatch(str(row.get("code"))) is not None
        }
    ) if isinstance(warnings, list) else []
    runtime_updates: dict[str, Any] = {
        "queue_state": "ready" if execution_succeeded else "failed",
        "current_stage": "quality_ready" if execution_succeeded else "failed",
        "luna_status": "succeeded" if execution_succeeded else "failed",
        "local_dispatch_status": "terminal",
        "model_stage": "quality_closed",
        "terminal_status": "succeeded" if execution_succeeded else "failed",
        "execution_status": "succeeded" if execution_succeeded else "failed",
        "quality_status": (
            "issues_found" if disposition == "needs_sol_review" else "unchecked"
        ),
        "quality_outcome": disposition,
        "report_disposition": disposition,
        "terminal_error_code": (
            None if execution_succeeded else terminal.get("error_code")
        ),
        "production_accepted": False,
        "analysis_execution_status": "completed",
        "analysis_report_status": (
            "quarantined"
            if disposition == "quarantined"
            else "available_with_warnings"
        ),
        "review_execution_status": (
            "completed" if critical_runtime is not None else "not_started"
        ),
        "review_report_status": (
            "quarantined"
            if disposition == "quarantined" and critical_runtime is not None
            else "available_with_warnings"
            if critical_runtime is not None
            else "not_started"
        ),
        "sol_review_status": (
            "pending" if disposition == "needs_sol_review" else "not_eligible"
        ),
        "formal_write_status": "not_authorized",
        "report_available": True,
        "formal_write_eligible": False,
        "warning_codes": warning_codes,
        "package_sha256": package_sha256,
        "analysis_report_sha256": report_sha256,
        "terminal_receipt_sha256": terminal_sha256,
    }
    analysis_runtime = (
        stage_runtime.get("analysis")
        if isinstance(stage_runtime, Mapping)
        else None
    )
    if isinstance(analysis_runtime, Mapping):
        runtime_updates["analysis_raw_output_sha256"] = _safe_hash(
            analysis_runtime.get("raw_output_object_sha256")
        )
        runtime_updates["analysis_execution_receipt_sha256"] = _safe_hash(
            analysis_runtime.get("stage_execution_receipt_sha256")
        )
        runtime_updates["analysis_normalization_receipt_sha256"] = _safe_hash(
            analysis_runtime.get("stage_normalization_receipt_sha256")
        )
    if isinstance(critical_runtime, Mapping):
        runtime_updates["critical_review_raw_output_sha256"] = _safe_hash(
            critical_runtime.get("raw_output_object_sha256")
        )
        runtime_updates[
            "critical_review_execution_receipt_sha256"
        ] = _safe_hash(critical_runtime.get("stage_execution_receipt_sha256"))
        runtime_updates[
            "critical_review_normalization_receipt_sha256"
        ] = _safe_hash(
            critical_runtime.get("stage_normalization_receipt_sha256")
        )
        runtime_updates["critical_review_report_sha256"] = report_sha256
    return runtime_updates


def _published_event_declares_review_candidate(
    runtime_root: Path,
    terminal_event: Mapping[str, Any] | None,
) -> bool:
    """Detect a review package whose full report chain failed to reopen."""

    if terminal_event is None or terminal_event.get("event") != "published":
        return False
    artifacts = terminal_event.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return False
    package_sha256 = _safe_hash(artifacts.get("package_sha256"))
    package_path = artifacts.get("package_path")
    package_root = runtime_root / "dispatch" / "packages"
    expected_path = (
        package_root
        / "sha256"
        / str(package_sha256 or "")[:2]
        / f"{package_sha256}.json"
    )
    if (
        package_sha256 is None
        or not isinstance(package_path, str)
        or Path(package_path).resolve(strict=False)
        != expected_path.resolve(strict=False)
    ):
        return False
    try:
        payload = expected_path.read_bytes()
        package = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return bool(
        hashlib.sha256(payload).hexdigest() == package_sha256
        and isinstance(package, Mapping)
        and package.get("schema_version")
        == "study-intake-review-candidate-package-v1"
    )


def _content_value_sha256(value: object) -> str:
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DashboardProjectionError(
            "english_authoritative_proposal_invalid"
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def _verified_proposal_sha256(package: Mapping[str, Any]) -> str | None:
    """Return only a proposal object whose hash is inside the HMAC-bound package."""

    candidates: list[Mapping[str, Any]] = [package]
    critical_review = package.get("critical_review")
    if isinstance(critical_review, Mapping):
        candidates.append(critical_review)
        host_publication = critical_review.get("host_publication")
        if isinstance(host_publication, Mapping):
            candidates.append(host_publication)
    found: set[str] = set()
    for candidate in candidates:
        digest = candidate.get("luna_proposal_sha256")
        proposal = candidate.get("luna_proposal")
        if digest is None and proposal is None:
            continue
        if (
            not isinstance(digest, str)
            or SHA256.fullmatch(digest) is None
            or not isinstance(proposal, Mapping)
            or _content_value_sha256(proposal) != digest
        ):
            raise DashboardProjectionError(
                "english_authoritative_proposal_invalid"
            )
        found.add(digest)
    if len(found) > 1:
        raise DashboardProjectionError(
            "english_authoritative_proposal_ambiguous"
        )
    return next(iter(found), None)


def _english_capture_authority(
    runtime_root: Path,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve English completion only through the signed latest pointer.

    Historical packages and candidate directories are deliberately not
    searched.  A present but invalid latest chain therefore closes selection
    instead of falling back to an older successful object.
    """

    capture_id = str(item["capture_id"])
    unit_sha256 = str(item["unit_sha256"])
    release_id = str(item["release_id"])
    input_fingerprint = str(item["input_fingerprint"])
    timeline = _event_timeline(runtime_root, unit_sha256)
    event_written = True
    dispatcher_accepted = bool(timeline)
    state: dict[str, Any] = {
        "event_written": event_written,
        "dispatcher_accepted": dispatcher_accepted,
        "package_visible": False,
        "quick_intake_complete": False,
        "foreground_completion_stage": (
            "dispatcher_accepted" if dispatcher_accepted else "event_written"
        ),
        "selected_by": "none",
        "selector_type": "signed_latest_authoritative",
        "selector_sha256": None,
        "proposal_sha256": None,
        "authoritative_package_sha256": None,
        "processing_outcome": None,
        "selection_authority_status": "missing",
    }
    latest_path = (
        runtime_root
        / "dispatch"
        / "state"
        / "latest-authoritative"
        / "english"
        / f"{capture_id}.json"
    )
    try:
        pointer_exists = latest_path.is_file() and not latest_path.is_symlink()
    except OSError:
        pointer_exists = False
    if not pointer_exists:
        return state
    try:
        verified = LeaseStore(runtime_root).verify_authoritative_completion(
            "english",
            capture_id,
            expected_release_id=release_id,
            expected_unit_sha256=unit_sha256,
            expected_input_fingerprint=input_fingerprint,
        )
        latest_raw = latest_path.read_bytes()
        latest_value = json.loads(latest_raw.decode("utf-8"))
        if (
            not isinstance(latest_value, Mapping)
            or dict(latest_value) != dict(verified["latest"])
        ):
            raise DashboardProjectionError(
                "english_latest_pointer_changed_during_read"
            )
        selector_sha256 = hashlib.sha256(latest_raw).hexdigest()
        completion = verified["completion"]
        package = verified.get("package")
        outcome = completion.get("outcome")
        if outcome not in {"succeeded", "failed", "timed_out", "cancelled"}:
            raise DashboardProjectionError(
                "english_authoritative_outcome_invalid"
            )
        state.update(
            {
                "dispatcher_accepted": True,
                "selector_sha256": selector_sha256,
                "processing_outcome": outcome,
                "selection_authority_status": "hmac_verified",
            }
        )
        if (
            outcome != "succeeded"
            or not isinstance(package, Mapping)
            or not isinstance(completion.get("package_sha256"), str)
            or SHA256.fullmatch(str(completion.get("package_sha256"))) is None
            or completion.get("package_sha256")
            != verified["latest"].get("package_sha256")
        ):
            state["foreground_completion_stage"] = "dispatcher_accepted"
            return state
        proposal_sha256 = _verified_proposal_sha256(package)
        if proposal_sha256 is None:
            raise DashboardProjectionError(
                "english_authoritative_proposal_missing"
            )
        state.update(
            {
                "package_visible": True,
                "quick_intake_complete": True,
                "foreground_completion_stage": "package_visible",
                "selected_by": "signed_latest_authoritative",
                "proposal_sha256": proposal_sha256,
                "authoritative_package_sha256": completion[
                    "package_sha256"
                ],
            }
        )
        return state
    except (DashboardProjectionError, DispatchError, KeyError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        code = (
            exc.code
            if isinstance(exc, DispatchError)
            else str(exc)
            if isinstance(exc, DashboardProjectionError)
            else "english_authoritative_selection_invalid"
        )
        state.update(
            {
                "package_visible": False,
                "quick_intake_complete": False,
                "foreground_completion_stage": (
                    "dispatcher_accepted"
                    if dispatcher_accepted
                    else "event_written"
                ),
                "selected_by": "none",
                "selector_sha256": None,
                "proposal_sha256": None,
                "authoritative_package_sha256": None,
                "processing_outcome": None,
                "selection_authority_status": "invalid",
                "selection_error_code": code,
            }
        )
        return state


def _apply_english_capture_authority(
    runtime_root: Path, item: dict[str, Any]
) -> None:
    state = _english_capture_authority(runtime_root, item)
    # Never retain a package hash sourced only from a decision, subject batch,
    # directory order, or mtime.  The signed current pointer is the sole gate.
    item.pop("package_sha256", None)
    item.pop("proposal_sha256", None)
    item.pop("selection_error_code", None)
    item.update(state)
    if state["package_visible"] is True:
        item["package_sha256"] = state["authoritative_package_sha256"]


def _queue_and_stage(
    decision: Mapping[str, Any],
    event: Mapping[str, Any] | None,
) -> tuple[str, str, str]:
    if event is not None:
        event_name = str(event["event"])
        if (
            event_name
            in {
                "child_process_started",
                "child_process_exited",
                "provider_process_started",
                "provider_process_exited",
                "provider_progress",
                "provider_heartbeat",
                "mcp_call_started",
                "mcp_call_completed",
                "mcp_receipt_published",
                "raw_output_persisted",
                "normalization_started",
                "normalization_completed",
                "normalization_warning",
                "stage_progress",
                "soft_timeout_warning",
                "stall_probe_started",
                "stall_probe_succeeded",
                "stall_probe_failed",
                "stall_suspected",
            }
            and event.get("stage_name") == "critical_review"
        ):
            return "running", "critical_review", "processing"
        return EVENT_TO_STATE[event_name]
    if decision.get("reason") == "semantic_package_reused":
        if all(
            isinstance(decision.get(key), str)
            and SHA256.fullmatch(str(decision.get(key))) is not None
            for key in (
                "semantic_reuse_receipt_sha256",
                "semantic_reuse_source_release_id",
                "package_sha256",
            )
        ):
            return "ready", "quality_ready", "two_pass_ready"
        return "failed", "failed", "failed"
    if decision.get("reason") == "production_canary_queued_not_selected":
        return "queued", "ready_for_selection", "paused"
    if decision.get("model_enqueue_allowed") is False or decision.get("eligible") is False:
        reason = str(decision.get("error_code") or decision.get("reason") or "")
        if (
            reason.startswith("awaiting_")
            or "evidence" in reason
            or "permission" in reason
            or reason.endswith("_missing")
        ):
            return "evidence_pending", "frozen_evidence", "skipped"
        if decision.get("error_code"):
            return "failed", "failed", "failed"
        return "evidence_pending", "frozen_evidence", "skipped"
    return "queued", "frozen_evidence", "queued"


def _status_axes(
    decision: Mapping[str, Any],
    event: Mapping[str, Any] | None,
    queue_state: str,
    current_stage: str,
) -> tuple[str, str, str | None]:
    terminal_status = None
    if queue_state in {"ready", "needs_rework", "failed", "stale"}:
        terminal_status = queue_state
    if event is not None:
        event_name = str(event.get("event") or "")
        if event_name in {"claim", "claimed"}:
            local_status = "claimed"
        elif event_name in {"recovery_scheduled", "retry_wait"}:
            local_status = "retrying"
        elif event_name in {"cancel", "cancelled"}:
            local_status = "cancelled"
        elif event_name == "stalled":
            local_status = "stalled"
        elif terminal_status is not None:
            local_status = "terminal"
        else:
            local_status = "running"
    elif decision.get("reason") == "production_canary_queued_not_selected":
        local_status = "pending_consumer_paused"
    elif terminal_status is not None:
        local_status = "terminal"
    elif queue_state == "evidence_pending":
        local_status = "pending"
    else:
        local_status = "pending"
    model_stage = {
        "analysis": "analysis",
        "critical_review": "critical_review",
        "quality_ready": "quality_closed",
        "needs_rework": "quality_closed",
        "failed": "failed",
        "stale": "cancelled",
        "ready_for_selection": "not_started",
        "frozen_evidence": "not_started",
    }.get(current_stage, "not_started")
    return local_status, model_stage, terminal_status


def _task_runtime_axes(
    decision: Mapping[str, Any],
    timeline: Sequence[Mapping[str, Any]],
    *,
    queue_state: str,
    local_dispatch_status: str,
    generated_at: str,
) -> dict[str, Any]:
    """Project independent execution, report, queue and stall axes."""

    names = [str(row.get("event") or "") for row in timeline]
    latest = timeline[-1] if timeline else None
    analysis_execution = "not_started"
    analysis_report = "not_started"
    review_execution = "not_started"
    review_report = "not_started"
    if any(
        name
        in {
            "process_started",
            "child_process_started",
            "model_submitted",
            "analysis_submitted",
            "provider_process_started",
            "provider_progress",
            "provider_heartbeat",
            "mcp_call_started",
            "mcp_call_completed",
            "mcp_receipt_published",
            "raw_output_persisted",
            "normalization_started",
            "normalization_completed",
            "normalization_warning",
            "stage_progress",
        }
        and row.get("stage_name") != "critical_review"
        for name, row in zip(names, timeline)
    ):
        analysis_execution = "running"
    if any(
        name
        in {
            "analysis_completed",
            "analysis_checkpoint_reused",
            "critical_started",
            "critical_completed",
            "published",
            "workflow_complete",
            "workflow_complete_with_warnings",
        }
        or row.get("stage_name") == "critical_review"
        for name, row in zip(names, timeline)
    ):
        analysis_execution = "completed"
        analysis_report = "available"
    if any(
        name in {"critical_started", "critical_completed"}
        or row.get("stage_name") == "critical_review"
        for name, row in zip(names, timeline)
    ):
        review_execution = "running"
    if any(
        name
        in {
            "critical_completed",
            "published",
            "workflow_complete",
            "workflow_complete_with_warnings",
        }
        for name in names
    ):
        review_execution = "completed"
        review_report = "available"
    warning_codes = sorted(
        {
            str(row.get("warning_code"))
            for row in timeline
            if isinstance(row.get("warning_code"), str)
            and SAFE_CODE.fullmatch(str(row.get("warning_code"))) is not None
        }
    )
    if "normalization_warning" in names or warning_codes:
        warning_stage = next(
            (
                str(row.get("stage_name") or "")
                for row in reversed(timeline)
                if row.get("event") == "normalization_warning"
                or row.get("warning_code") is not None
            ),
            "",
        )
        if warning_stage == "critical_review":
            review_report = "available_with_warnings"
        else:
            analysis_report = "available_with_warnings"
    if "workflow_complete_with_warnings" in names:
        review_report = "available_with_warnings"

    latest_name = str(latest.get("event") or "") if latest is not None else ""
    if latest_name in {"cancel", "cancelled"}:
        target = (
            "review" if latest and latest.get("stage_name") == "critical_review" else "analysis"
        )
        if target == "review":
            review_execution = "cancelled"
        else:
            analysis_execution = "cancelled"
    elif latest_name == "stalled":
        target = (
            "review" if latest and latest.get("stage_name") == "critical_review" else "analysis"
        )
        if target == "review":
            review_execution = "stalled"
        else:
            analysis_execution = "stalled"
    elif latest_name in {"failed", "execution_failed", "workflow_partial", "timeout"}:
        if latest and latest.get("stage_name") == "critical_review":
            review_execution = "failed"
        elif analysis_execution != "completed":
            analysis_execution = "failed"
        else:
            review_execution = "failed"

    reason = str(decision.get("error_code") or decision.get("reason") or "")
    if queue_state == "evidence_pending":
        evidence_access = "missing"
    elif any(token in reason for token in ("quarantin", "tamper", "cross_subject")):
        evidence_access = "quarantined"
    elif timeline or decision.get("eligible") is True:
        evidence_access = "ready"
    else:
        evidence_access = "unverified"

    non_progress_events = {
        "soft_timeout_warning",
        "stall_probe_started",
        "stall_probe_failed",
        "stall_suspected",
    }
    last_progress = next(
        (
            _safe_text(row.get("occurred_at"), 64)
            for row in reversed(timeline)
            if row.get("event") not in non_progress_events
        ),
        None,
    )
    started_at = next(
        (
            _safe_text(row.get("occurred_at"), 64)
            for row in timeline
            if row.get("event") in {"claim", "claimed", "process_started"}
        ),
        None,
    )
    elapsed_runtime: int | None = None
    if started_at is not None:
        try:
            start = dt.datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            end_text = (
                _safe_text(latest.get("occurred_at"), 64)
                if latest is not None
                and local_dispatch_status in {"terminal", "cancelled", "stalled"}
                else generated_at
            )
            end = dt.datetime.fromisoformat(str(end_text).replace("Z", "+00:00"))
            elapsed_runtime = max(0, int((end - start).total_seconds()))
        except (TypeError, ValueError):
            pass
    if latest_name == "stalled":
        probe_status = "stalled"
    elif latest_name in {"cancel", "cancelled"}:
        probe_status = "cancelled"
    elif latest_name in {"stall_probe_started", "stall_probe_failed"}:
        probe_status = "probing"
    elif latest_name == "stall_suspected":
        probe_status = "stall_suspected"
    elif timeline:
        probe_status = "healthy"
    else:
        probe_status = "not_applicable"
    axes: dict[str, Any] = {
        "evidence_access_status": evidence_access,
        "analysis_execution_status": analysis_execution,
        "analysis_report_status": analysis_report,
        "review_execution_status": review_execution,
        "review_report_status": review_report,
        "sol_review_status": (
            "pending" if queue_state == "ready" else "not_eligible"
        ),
        "formal_write_status": "not_authorized",
        "report_available": analysis_report in {
            "available", "available_with_warnings", "quarantined"
        } or review_report in {
            "available", "available_with_warnings", "quarantined"
        },
        "formal_write_eligible": False,
        "warning_codes": warning_codes,
        "elapsed_runtime_seconds": elapsed_runtime,
        "last_meaningful_progress_at": last_progress,
        "soft_timeout_warning": "soft_timeout_warning" in names,
        "stall_probe_status": probe_status,
    }
    for field in (
        "authority_snapshot_sha256",
        "analysis_execution_receipt_sha256",
        "analysis_raw_output_sha256",
        "analysis_normalization_receipt_sha256",
        "analysis_report_sha256",
        "critical_review_execution_receipt_sha256",
        "critical_review_raw_output_sha256",
        "critical_review_normalization_receipt_sha256",
        "critical_review_report_sha256",
        "package_sha256",
        "sol_handoff_envelope_sha256",
        "terminal_receipt_sha256",
    ):
        digest = next(
            (
                str(row[field])
                for row in reversed(timeline)
                if isinstance(row.get(field), str)
                and SHA256.fullmatch(str(row.get(field))) is not None
            ),
            None,
        )
        if digest is not None:
            axes[field] = digest
    return axes


def _ensure_task_axes(item: dict[str, Any]) -> None:
    queue_state = str(item.get("queue_state") or "queued")
    stage = str(item.get("current_stage") or "frozen_evidence")
    local = item.get("local_dispatch_status")
    if local not in {
        "pending",
        "pending_consumer_paused",
        "claimed",
        "running",
        "retrying",
        "terminal",
        "cancelled",
        "stalled",
    }:
        item["local_dispatch_status"] = (
            "terminal"
            if queue_state in {"ready", "needs_rework", "failed", "stale"}
            else "running"
            if queue_state == "running"
            else "pending"
        )
    item.setdefault(
        "evidence_access_status",
        "missing" if queue_state == "evidence_pending" else "unverified",
    )
    item.setdefault(
        "analysis_execution_status",
        "completed"
        if stage in {"critical_review", "quality_ready"}
        else "running"
        if stage == "analysis"
        else "not_started",
    )
    item.setdefault(
        "analysis_report_status",
        "available" if stage in {"critical_review", "quality_ready"} else "not_started",
    )
    item.setdefault(
        "review_execution_status",
        "completed"
        if stage == "quality_ready"
        else "running"
        if stage == "critical_review"
        else "not_started",
    )
    item.setdefault(
        "review_report_status",
        "available" if stage == "quality_ready" else "not_started",
    )
    item.setdefault(
        "sol_review_status",
        "pending" if queue_state == "ready" else "not_eligible",
    )
    item.setdefault("formal_write_status", "not_authorized")
    item.setdefault(
        "report_available",
        item.get("analysis_report_status")
        in {"available", "available_with_warnings", "quarantined"}
        or item.get("review_report_status")
        in {"available", "available_with_warnings", "quarantined"},
    )
    item.setdefault("formal_write_eligible", False)
    item.setdefault("warning_codes", [])
    item.setdefault("elapsed_runtime_seconds", item.get("elapsed_seconds"))
    item.setdefault("last_meaningful_progress_at", item.get("last_state_change_at"))
    item.setdefault("soft_timeout_warning", False)
    item.setdefault("stall_probe_status", "not_applicable")
    item.setdefault("server_queue_status", "unknown")
    item.setdefault("server_queue_confirmation", "unconfirmed")
    item.setdefault("exact_error_code", None)
    for field in (
        "authority_snapshot_sha256",
        "analysis_execution_receipt_sha256",
        "analysis_raw_output_sha256",
        "analysis_normalization_receipt_sha256",
        "analysis_report_sha256",
        "critical_review_execution_receipt_sha256",
        "critical_review_raw_output_sha256",
        "critical_review_normalization_receipt_sha256",
        "critical_review_report_sha256",
        "sol_handoff_envelope_sha256",
        "terminal_receipt_sha256",
    ):
        item.setdefault(field, None)


def _item_from_decision(
    runtime_root: Path,
    decision: Mapping[str, Any],
    *,
    subject: str,
    generated_at: str,
) -> dict[str, Any] | None:
    unit = decision.get("unit_sha256")
    frozen = decision.get("frozen_payload_sha256")
    release_id = decision.get("release_id")
    if not all(isinstance(value, str) and SHA256.fullmatch(value) for value in (unit, frozen, release_id)):
        return None
    timeline = _event_timeline(runtime_root, unit)
    latest_event = timeline[-1] if timeline else None
    queue_state, current_stage, luna_status = _queue_and_stage(decision, latest_event)
    local_dispatch_status, model_stage, terminal_status = _status_axes(
        decision, latest_event, queue_state, current_stage
    )
    input_fingerprint = _safe_text(decision.get("input_fingerprint"), 320)
    if input_fingerprint is None:
        input_fingerprint = f"sha256:{frozen}"
    semantic_contract_sha256 = next(
        (
            str(decision[key])
            for key in (
                "semantic_contract_sha256",
                "subject_processing_contract_sha256",
                "processing_contract_sha256",
                "rule_version_sha256",
            )
            if isinstance(decision.get(key), str)
            and SHA256.fullmatch(str(decision.get(key))) is not None
        ),
        "0" * 64,
    )
    generation = (
        latest_event.get("attempt")
        if isinstance(latest_event, Mapping)
        and isinstance(latest_event.get("attempt"), int)
        else decision.get("generation", 0)
    )
    authority_core = {
        "schema_version": "study-intake-authority-key-v1",
        "subject": subject,
        "capture_id": decision["capture_id"],
        "input_fingerprint": input_fingerprint,
        "semantic_contract_sha256": semantic_contract_sha256,
        "release_id": release_id,
        "generation": generation,
    }
    item: dict[str, Any] = {
        "capture_id": decision["capture_id"],
        "subject": subject,
        "study_date": decision["study_date"],
        "updated_at": decision["updated_at"],
        "target_label": decision.get("target_label") or decision["capture_id"],
        "queue_state": queue_state,
        "current_stage": current_stage,
        "luna_status": luna_status,
        "local_dispatch_status": local_dispatch_status,
        "model_stage": model_stage,
        "terminal_status": terminal_status,
        "unit_sha256": unit,
        "input_fingerprint": input_fingerprint,
        "frozen_payload_sha256": frozen,
        "release_id": release_id,
        "rule_version": decision.get("rule_version")
        or "study-intake-concurrent-dispatch-contract-v1",
        "server_queue_status": "unknown",
        "server_queue_confirmation": "unconfirmed",
        "requested_model": REQUIRED_MODEL,
        "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
        "runtime_identity_status": "requested_unverified",
        "semantic_contract_sha256": semantic_contract_sha256,
        "generation": generation,
        "authority_key_sha256": hashlib.sha256(
            _canonical_bytes(authority_core)
        ).hexdigest(),
    }
    item.update(
        _task_runtime_axes(
            decision,
            timeline,
            queue_state=queue_state,
            local_dispatch_status=local_dispatch_status,
            generated_at=generated_at,
        )
    )
    review_projection = _verified_review_candidate_projection(
        runtime_root, decision, latest_event
    )
    if review_projection is not None:
        item.update(review_projection)
    elif _published_event_declares_review_candidate(
        runtime_root, latest_event
    ):
        item.update(
            {
                "queue_state": "failed",
                "current_stage": "failed",
                "luna_status": "failed",
                "local_dispatch_status": "terminal",
                "model_stage": "failed",
                "terminal_status": "failed",
                "execution_status": "failed",
                "quality_status": "unchecked",
                "quality_outcome": "pending",
                "report_disposition": None,
                "terminal_error_code": "review_report_reopen_failed",
                "report_available": False,
                "sol_review_status": "not_eligible",
                "formal_write_status": "not_authorized",
                "formal_write_eligible": False,
                "production_accepted": False,
                "exact_error_code": "review_report_reopen_failed",
                "last_error_code": "review_report_reopen_failed",
                "processing_error_code": "review_report_reopen_failed",
            }
        )
    explicit_processing_contract_sha256 = next(
        (
            str(decision[key])
            for key in (
                "processing_contract_sha256",
                "subject_processing_contract_sha256",
            )
            if isinstance(decision.get(key), str)
            and SHA256.fullmatch(str(decision.get(key))) is not None
        ),
        None,
    )
    if explicit_processing_contract_sha256 is not None:
        item["processing_contract_sha256"] = (
            explicit_processing_contract_sha256
        )
    rule_version_sha = decision.get("rule_version_sha256")
    if isinstance(rule_version_sha, str) and SHA256.fullmatch(rule_version_sha):
        item["rule_version_sha256"] = rule_version_sha
    detail_path = (
        runtime_root
        / "dispatch"
        / "state"
        / "task-details"
        / f"{unit}.json"
    )
    try:
        if detail_path.is_file() and not detail_path.is_symlink():
            item["task_detail_path"] = str(detail_path)
    except OSError:
        pass
    if latest_event is not None:
        item["attempt"] = latest_event.get("attempt")
        item["fence"] = latest_event.get("fence")
        occurred_at = _safe_text(latest_event.get("occurred_at"), 64)
        if occurred_at is not None:
            item["last_state_change_at"] = occurred_at
        first_started = next(
            (
                _safe_text(event.get("occurred_at"), 64)
                for event in timeline
                if event.get("event") in {"claim", "process_started"}
            ),
            None,
        )
        if first_started is not None:
            item["started_at"] = first_started
            try:
                started_value = dt.datetime.fromisoformat(first_started.replace("Z", "+00:00"))
                generated_value = dt.datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
                elapsed_end = generated_value
                if queue_state != "running" and occurred_at is not None:
                    elapsed_end = dt.datetime.fromisoformat(
                        occurred_at.replace("Z", "+00:00")
                    )
                item["elapsed_seconds"] = max(
                    0, int((elapsed_end - started_value).total_seconds())
                )
            except (TypeError, ValueError):
                pass
    item["local_model_submitted"] = any(
        event.get("event")
        in {"model_submitted", "analysis_submitted", "provider_process_started"}
        for event in timeline
    )
    if latest_event is not None and latest_event.get("event") == "retry_wait":
        retry_code = _safe_text(latest_event.get("error_code"), 160)
        normalized = retry_code.lower() if retry_code else ""
        if normalized in SERVICE_RETRY_CODES or normalized.endswith("_rate_limited"):
            item["server_queue_status"] = "rate_limited"
            item["server_queue_confirmation"] = "confirmed_event"
    reason = _safe_text(
        decision.get("error_code") or decision.get("reason"), 160
    )
    event_error = (
        _safe_text(latest_event.get("error_code"), 160)
        if latest_event is not None
        else None
    )
    authoritative_error = event_error or reason
    if (
        queue_state in {"failed", "needs_rework", "evidence_pending"}
        and authoritative_error
        and SAFE_CODE.fullmatch(authoritative_error)
    ):
        item["last_error_code"] = authoritative_error
        item["processing_error_code"] = authoritative_error
        item["exact_error_code"] = authoritative_error
    if (
        decision.get("reason") == "semantic_package_reused"
        and queue_state == "failed"
    ):
        item["last_error_code"] = "semantic_reuse_binding_invalid"
        item["processing_error_code"] = "semantic_reuse_binding_invalid"
        item["exact_error_code"] = "semantic_reuse_binding_invalid"
    for key in (
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "evidence_readiness_receipt_sha256",
        "legacy_compatibility_receipt_sha256",
        "semantic_reuse_receipt_sha256",
        "semantic_reuse_source_release_id",
        "package_sha256",
        "superseded_input_fingerprint",
        "evidence_status",
        "projection_origin",
    ):
        value = decision.get(key)
        if value is not None:
            item[key] = value
    return item


def _current_stage(items: Sequence[Mapping[str, Any]]) -> str:
    active_order = ("critical_review", "analysis", "frozen_evidence")
    stages = {str(item.get("current_stage")) for item in items}
    for stage in active_order:
        if stage in stages:
            return stage
    for stage in ("needs_rework", "failed", "stale", "quality_ready"):
        if stage in stages:
            return stage
    return "idle"


def _counts(items: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    result = {
        "selected": 0,
        "queued": 0,
        "analysis_running": 0,
        "critical_review_running": 0,
        "terminal": 0,
        "quality_passed": 0,
        "needs_rework": 0,
        "failed": 0,
        "evidence_pending": 0,
    }
    for item in items:
        result["selected"] += 1
        queue_state = str(item.get("queue_state") or "")
        stage = str(item.get("current_stage") or "")
        if queue_state == "evidence_pending":
            result["evidence_pending"] += 1
        elif queue_state == "queued":
            result["queued"] += 1
        elif queue_state == "running" and stage == "critical_review":
            result["critical_review_running"] += 1
        elif queue_state == "running":
            result["analysis_running"] += 1
        elif queue_state == "ready":
            result["quality_passed"] += 1
        elif queue_state == "needs_rework":
            result["needs_rework"] += 1
        elif queue_state in {"failed", "stale"}:
            result["failed"] += 1
    result["terminal"] = (
        result["quality_passed"]
        + result["needs_rework"]
        + result["failed"]
    )
    return result


def _safe_control_id(value: Any) -> str | None:
    candidate = _safe_text(value, 160)
    if candidate is None or SAFE_ID.fullmatch(candidate) is None:
        return None
    return candidate


def _safe_hash(value: Any) -> str | None:
    candidate = _safe_text(value, 64)
    if candidate is None or SHA256.fullmatch(candidate) is None:
        return None
    return candidate


def _public_terminal_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) not in {
        frozenset(LEGACY_TERMINAL_OUTCOMES),
        frozenset(TERMINAL_OUTCOMES),
    }:
        raise DashboardProjectionError("production_canary_terminal_counts_invalid")
    counts: dict[str, int] = {}
    for outcome in TERMINAL_OUTCOMES:
        raw = value.get(outcome, 0)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise DashboardProjectionError(
                "production_canary_terminal_counts_invalid"
            )
        counts[outcome] = raw
    return counts


def _public_terminal_failure_bindings(
    value: Any,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 4096:
        raise DashboardProjectionError(
            "production_canary_terminal_failure_bindings_invalid"
        )
    result: list[dict[str, Any]] = []
    seen_units: set[str] = set()
    required = {
        "unit_sha256",
        "outcome",
        "error_code",
        "terminal_kind",
        "terminal_receipt_sha256",
    }
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != required:
            raise DashboardProjectionError(
                "production_canary_terminal_failure_bindings_invalid"
            )
        unit_sha256 = _safe_hash(raw.get("unit_sha256"))
        receipt_sha256 = _safe_hash(raw.get("terminal_receipt_sha256"))
        outcome = raw.get("outcome")
        terminal_kind = _safe_text(raw.get("terminal_kind"), 160)
        error_code = raw.get("error_code")
        if (
            unit_sha256 is None
            or unit_sha256 in seen_units
            or receipt_sha256 is None
            or outcome not in TERMINAL_FAILURE_OUTCOMES
            or terminal_kind is None
            or SAFE_CODE.fullmatch(terminal_kind) is None
            or (
                error_code is not None
                and (
                    not isinstance(error_code, str)
                    or SAFE_CODE.fullmatch(error_code) is None
                )
            )
        ):
            raise DashboardProjectionError(
                "production_canary_terminal_failure_bindings_invalid"
            )
        seen_units.add(unit_sha256)
        result.append(
            {
                "unit_sha256": unit_sha256,
                "outcome": str(outcome),
                "error_code": error_code,
                "terminal_kind": terminal_kind,
                "terminal_receipt_sha256": receipt_sha256,
            }
        )
    return result


def _public_canary_selection(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise DashboardProjectionError("production_canary_selection_invalid")
    result: dict[str, Any] = {}
    for key in (
        "producer_input_contract_sha256",
        "source_event_set_sha256",
        "unit_sha256",
        "frozen_payload_sha256",
        "canary_gate_sha256",
        "canary_gate_authority_sha256",
    ):
        raw = value.get(key)
        if raw is not None:
            checked = _safe_hash(raw)
            if checked is None:
                raise DashboardProjectionError(
                    "production_canary_selection_invalid"
                )
            result[key] = checked
    for key in ("producer_unit_id", "producer_recorded_at"):
        raw = value.get(key)
        if raw is not None:
            checked = _safe_text(raw, 160 if key == "producer_unit_id" else 64)
            if checked is None:
                raise DashboardProjectionError(
                    "production_canary_selection_invalid"
                )
            result[key] = checked
    required = {
        "producer_unit_id",
        "producer_recorded_at",
        "producer_input_contract_sha256",
        "source_event_set_sha256",
        "unit_sha256",
        "frozen_payload_sha256",
    }
    if not required.issubset(result):
        raise DashboardProjectionError("production_canary_selection_invalid")
    return result


def _public_canary_gate(
    value: Any,
    *,
    subject: str,
    release_id: str,
) -> dict[str, Any] | None:
    """Return a path-free, scope-explicit production canary projection.

    ``LeaseStore.subject_status`` has already verified the state HMAC.  This
    second boundary deliberately copies only fields safe for the Dashboard and
    rejects malformed or cross-release state rather than silently presenting a
    partial production status.
    """

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise DashboardProjectionError("production_canary_state_invalid")
    state = value.get("state")
    expected_status = (
        "production_canary_inactive"
        if state == "inactive_rolled_back"
        else "production_canary_active"
    )
    if (
        value.get("schema_version")
        not in {
            "study-intake-production-canary-state-v2",
            "study-intake-production-canary-state-v3",
        }
        or value.get("status") != expected_status
        or state not in PRODUCTION_CANARY_STATES
        or value.get("subject") != subject
        or value.get("release_id") != release_id
        or value.get("producer_capture_enabled") is not True
        or value.get("sol_formal_curation_enabled") is not False
        or value.get("post_activation_only") is not True
        or value.get("initial_canary_inflight_limit") != 1
        or isinstance(value.get("continuous_concurrency_limit"), bool)
        or not isinstance(value.get("continuous_concurrency_limit"), int)
        or not 1 <= value.get("continuous_concurrency_limit") <= 64
        or value.get("keep_backlog_drained") is not True
        or value.get("production_accepted") is not False
        or value.get("fast_mode_requested") is not False
        or value.get("fast_mode_effective") != "not_requested"
        or value.get("model_call_count") != 0
        or value.get("provider_request_count") != 0
        or value.get("observability_counter_scope")
        != "current_activation_cumulative"
        or "requested_service_tier" not in value
        or value.get("requested_service_tier") is not None
        or value.get("mcp_tool_call_count") != 0
        or value.get("formal_write_count") != 0
        or value.get("sol_enabled") is not False
    ):
        raise DashboardProjectionError("production_canary_state_invalid")
    luna_enabled = value.get("luna_consumer_enabled")
    unlocked_once = value.get("unlocked_once")
    if not isinstance(luna_enabled, bool) or not isinstance(unlocked_once, bool):
        raise DashboardProjectionError("production_canary_state_invalid")
    if state in {"failed_drained", "paused_drained", "inactive_rolled_back"} and luna_enabled:
        raise DashboardProjectionError("production_canary_consumer_state_invalid")
    if state in {
        "armed",
        "canary_in_flight",
        "continuous_concurrent_unlocked",
    } and not luna_enabled:
        raise DashboardProjectionError("production_canary_consumer_state_invalid")
    if state == "continuous_concurrent_unlocked" and not unlocked_once:
        raise DashboardProjectionError("production_canary_unlock_state_invalid")
    queue_depth = value.get("queue_depth")
    active_count = value.get("active_task_count")
    oldest_age = value.get("oldest_pending_age_seconds")
    if (
        isinstance(queue_depth, bool)
        or not isinstance(queue_depth, int)
        or queue_depth < 0
        or isinstance(active_count, bool)
        or not isinstance(active_count, int)
        or active_count < 0
        or (
            oldest_age is not None
            and (
                isinstance(oldest_age, bool)
                or not isinstance(oldest_age, int)
                or oldest_age < 0
            )
        )
        or (queue_depth == 0) is not (oldest_age is None)
    ):
        raise DashboardProjectionError("production_canary_queue_state_invalid")
    if state == "canary_in_flight" and active_count != 1:
        raise DashboardProjectionError("production_canary_active_count_invalid")
    if state in {"armed", "paused_drained", "inactive_rolled_back"} and active_count:
        raise DashboardProjectionError("production_canary_active_count_invalid")
    continuous_limit = int(value["continuous_concurrency_limit"])
    if state in {"continuous_concurrent_unlocked", "failed_drained"} and active_count > continuous_limit:
        raise DashboardProjectionError("production_canary_active_count_invalid")

    hashes: dict[str, str] = {}
    for key in (
        "activation_id",
        "producer_authority_fingerprint",
        "producer_high_watermark_sha256",
        "activation_receipt_sha256",
        "activation_gate_authority_sha256",
        "terminal_index_sha256",
    ):
        checked = _safe_hash(value.get(key))
        if checked is None:
            raise DashboardProjectionError("production_canary_binding_invalid")
        hashes[key] = checked
    for key in (
        "last_terminal_receipt_sha256",
        "last_report_sha256",
        "last_package_sha256",
        "last_emergency_cancel_receipt_sha256",
        "last_preclaim_failure_receipt_sha256",
        "last_preclaim_failure_evidence_sha256",
        "preclaim_failure_resume_ack_sha256",
        "last_evidence_authority_fingerprint",
        "last_analysis_grounding_manifest_sha256",
        "last_critical_review_grounding_manifest_sha256",
    ):
        raw = value.get(key)
        if raw is not None:
            checked = _safe_hash(raw)
            if checked is None:
                raise DashboardProjectionError("production_canary_binding_invalid")
            hashes[key] = checked

    timestamps: dict[str, str | None] = {}
    for key in (
        "activated_at",
        "last_success_at",
        "last_failure_at",
        "last_preclaim_failure_at",
        "last_emergency_cancel_at",
        "updated_at",
    ):
        raw = value.get(key)
        if raw is None and key in {
            "last_success_at",
            "last_failure_at",
            "last_preclaim_failure_at",
            "last_emergency_cancel_at",
        }:
            timestamps[key] = None
            continue
        checked = _safe_text(raw, 64)
        if checked is None:
            raise DashboardProjectionError("production_canary_timestamp_invalid")
        timestamps[key] = checked

    texts: dict[str, str | None] = {}
    for key in (
        "blocking_reason",
        "backpressure_reason",
        "next_action",
        "last_analysis_status",
        "last_critical_review_status",
        "last_report_status",
        "last_read_session_id",
        "last_evidence_generation",
        "last_preclaim_failure_stage",
        "last_preclaim_failure_error_code",
    ):
        raw = value.get(key)
        if raw is None and key in {
            "blocking_reason",
            "backpressure_reason",
            "last_read_session_id",
            "last_evidence_generation",
            "last_preclaim_failure_stage",
            "last_preclaim_failure_error_code",
        }:
            texts[key] = None
            continue
        checked = _safe_text(raw, 160)
        if checked is None or SAFE_CODE.fullmatch(checked) is None:
            raise DashboardProjectionError("production_canary_status_text_invalid")
        texts[key] = checked

    backpressure_reason = texts["backpressure_reason"]
    if backpressure_reason not in {
        None,
        "initial_canary_inflight_limit_reached",
        "continuous_concurrency_limit_reached",
        "luna_consumer_disabled",
    }:
        raise DashboardProjectionError(
            "production_canary_backpressure_invalid"
        )
    if backpressure_reason == "initial_canary_inflight_limit_reached" and (
        state != "canary_in_flight" or active_count != 1
    ):
        raise DashboardProjectionError(
            "production_canary_backpressure_invalid"
        )
    if backpressure_reason == "continuous_concurrency_limit_reached" and (
        state != "continuous_concurrent_unlocked"
        or active_count < continuous_limit
    ):
        raise DashboardProjectionError(
            "production_canary_backpressure_invalid"
        )
    if backpressure_reason == "luna_consumer_disabled" and (
        state not in {"failed_drained", "paused_drained", "inactive_rolled_back"}
        or luna_enabled
    ):
        raise DashboardProjectionError(
            "production_canary_backpressure_invalid"
        )

    preclaim_stage = texts["last_preclaim_failure_stage"]
    if preclaim_stage not in {
        None,
        "producer_contract",
        "materialize",
        "consumer_admission",
        "pre_claim",
        "submit_generation_fence",
    }:
        raise DashboardProjectionError("production_canary_preclaim_invalid")
    preclaim_presence = (
        timestamps["last_preclaim_failure_at"] is not None,
        preclaim_stage is not None,
        texts["last_preclaim_failure_error_code"] is not None,
        "last_preclaim_failure_receipt_sha256" in hashes,
        "last_preclaim_failure_evidence_sha256" in hashes,
    )
    if any(preclaim_presence) and not all(preclaim_presence):
        raise DashboardProjectionError("production_canary_preclaim_invalid")
    preclaim_ack = hashes.get("preclaim_failure_resume_ack_sha256")
    preclaim_receipt = hashes.get("last_preclaim_failure_receipt_sha256")
    if preclaim_ack is not None and preclaim_receipt is None:
        raise DashboardProjectionError("production_canary_preclaim_invalid")
    if (
        state
        in {"armed", "canary_in_flight", "continuous_concurrent_unlocked"}
        and preclaim_receipt is not None
        and preclaim_ack != preclaim_receipt
    ):
        raise DashboardProjectionError("production_canary_preclaim_invalid")

    late_result_fence_status = value.get("late_result_fence_status")
    if late_result_fence_status not in {"not_required", "sealed"}:
        raise DashboardProjectionError("production_canary_late_fence_invalid")
    if late_result_fence_status == "sealed" and (
        timestamps["last_emergency_cancel_at"] is None
        or "last_emergency_cancel_receipt_sha256" not in hashes
    ):
        raise DashboardProjectionError("production_canary_late_fence_invalid")
    if late_result_fence_status == "not_required" and (
        timestamps["last_emergency_cancel_at"] is not None
        or "last_emergency_cancel_receipt_sha256" in hashes
    ):
        raise DashboardProjectionError("production_canary_late_fence_invalid")

    evidence_refs: dict[str, list[str]] = {}
    for key in (
        "last_analysis_evidence_refs",
        "last_critical_review_evidence_refs",
    ):
        raw = value.get(key)
        if (
            not isinstance(raw, list)
            or len(raw) > 256
            or any(
                not isinstance(ref, str)
                or (
                    SAFE_MCP_EVIDENCE_REF.fullmatch(ref) is None
                    and SHA256.fullmatch(ref) is None
                )
                or (
                    SAFE_MCP_EVIDENCE_REF.fullmatch(ref) is not None
                    and not ref.startswith(f"mcp-item:{subject}:")
                )
                for ref in raw
            )
        ):
            raise DashboardProjectionError(
                "production_canary_evidence_refs_invalid"
            )
        evidence_refs[key] = [
            ref
            if ref.startswith("mcp-item:")
            else f"mcp-item:{subject}:{ref}"
            for ref in raw
        ]
    exact_math_migration_pending = (
        subject == "math"
        and state == "continuous_concurrent_unlocked"
        and value.get("unlocked_once") is True
        and queue_depth + active_count == 4
        and value.get("terminal_task_count") == 0
        and value.get("last_analysis_status") == "not_started"
        and value.get("last_critical_review_status") == "not_started"
        and value.get("last_report_status") == "not_started"
        and evidence_refs["last_analysis_evidence_refs"] == []
        and evidence_refs["last_critical_review_evidence_refs"] == []
        and texts["last_read_session_id"] is None
        and texts["last_evidence_generation"] is None
        and "last_evidence_authority_fingerprint" not in hashes
        and "last_analysis_grounding_manifest_sha256" not in hashes
        and "last_critical_review_grounding_manifest_sha256" not in hashes
    )
    if (
        state == "continuous_concurrent_unlocked"
        and not exact_math_migration_pending
        and (
            not evidence_refs["last_analysis_evidence_refs"]
            or texts["last_read_session_id"] is None
            or texts["last_evidence_generation"] is None
            or "last_evidence_authority_fingerprint" not in hashes
            or "last_analysis_grounding_manifest_sha256" not in hashes
        )
    ):
        raise DashboardProjectionError(
            "production_canary_success_grounding_invalid"
        )

    counters: dict[str, int] = {}
    for key in (
        "observed_model_call_count",
        "observed_provider_request_count",
        "observed_mcp_tool_call_count",
    ):
        raw = value.get(key)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise DashboardProjectionError("production_canary_counter_invalid")
        counters[key] = raw

    terminal_task_count = value.get("terminal_task_count")
    terminal_by_outcome = _public_terminal_counts(
        value.get("terminal_by_outcome")
    )
    if (
        isinstance(terminal_task_count, bool)
        or not isinstance(terminal_task_count, int)
        or terminal_task_count < 0
        or sum(terminal_by_outcome.values()) != terminal_task_count
    ):
        raise DashboardProjectionError(
            "production_canary_terminal_counts_invalid"
        )

    selected = _public_canary_selection(value.get("selected"))
    last_selected = _public_canary_selection(value.get("last_selected"))
    active_selections_raw = value.get("active_selections")
    if (
        not isinstance(active_selections_raw, Mapping)
        or len(active_selections_raw) != active_count
        or len(active_selections_raw) > 64
    ):
        raise DashboardProjectionError("production_canary_selection_invalid")
    active_selections: dict[str, dict[str, Any]] = {}
    for unit_sha256, raw_selection in active_selections_raw.items():
        checked_unit = _safe_hash(unit_sha256)
        public_selection = _public_canary_selection(raw_selection)
        if (
            checked_unit is None
            or public_selection is None
            or public_selection.get("unit_sha256") != checked_unit
        ):
            raise DashboardProjectionError("production_canary_selection_invalid")
        active_selections[checked_unit] = public_selection
    if state == "canary_in_flight" and (
        selected is None
        or selected.get("unit_sha256") not in active_selections
    ):
        raise DashboardProjectionError("production_canary_selection_invalid")
    if state != "canary_in_flight" and selected is not None:
        raise DashboardProjectionError("production_canary_selection_invalid")
    authority = value.get("authority")
    if (
        not isinstance(authority, Mapping)
        or authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or _safe_hash(authority.get("key_id")) is None
        or authority.get("purpose") != "dispatch-production-canary-state"
        or _safe_hash(authority.get("hmac_sha256")) is None
    ):
        raise DashboardProjectionError("production_canary_authority_missing")
    public_authority = {
        "schema_version": "study-intake-dispatch-authority-v1",
        "algorithm": "HMAC-SHA256",
        "key_id": str(authority["key_id"]),
        "purpose": "dispatch-production-canary-state",
        "hmac_sha256": str(authority["hmac_sha256"]),
    }
    authority_sha256 = hashlib.sha256(
        _canonical_bytes(public_authority)
    ).hexdigest()
    return {
        "schema_version": str(value["schema_version"]),
        "status": expected_status,
        "state": str(state),
        "subject": subject,
        "release_id": release_id,
        **hashes,
        **timestamps,
        "producer_capture_enabled": True,
        "luna_consumer_enabled": luna_enabled,
        "sol_formal_curation_enabled": False,
        "queue_depth": queue_depth,
        "oldest_pending_age_seconds": oldest_age,
        "active_task_count": active_count,
        **texts,
        "post_activation_only": True,
        "initial_canary_inflight_limit": 1,
        "continuous_concurrency_limit": continuous_limit,
        "keep_backlog_drained": True,
        "selected": selected,
        "last_selected": last_selected,
        "active_selections": active_selections,
        "terminal_task_count": terminal_task_count,
        "terminal_by_outcome": terminal_by_outcome,
        "unlocked_once": unlocked_once,
        "production_accepted": False,
        "fast_mode_requested": False,
        "fast_mode_effective": str(value["fast_mode_effective"]),
        "requested_service_tier": None,
        "late_result_fence_status": str(late_result_fence_status),
        "model_call_count": 0,
        "provider_request_count": 0,
        "mcp_tool_call_count": 0,
        "control_plane_model_call_count": 0,
        "control_plane_provider_request_count": 0,
        "control_plane_mcp_tool_call_count": 0,
        "observability_counter_scope": "current_activation_cumulative",
        **counters,
        **evidence_refs,
        "formal_write_count": 0,
        "sol_enabled": False,
        "state_authority_sha256": authority_sha256,
        "authority": public_authority,
    }


def _valid_timestamp(value: Any) -> str | None:
    checked = _safe_text(value, 64)
    if checked is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(checked.replace("Z", "+00:00"))
    except ValueError:
        return None
    return checked if parsed.tzinfo is not None else None


def _unavailable_concurrency_observation(
    subject: str,
    release_id: str,
    error_code: str,
) -> dict[str, Any]:
    return {
        "schema_version": CONCURRENCY_OBSERVATION_SCHEMA,
        "status": "unavailable",
        "subject": subject,
        "release_id": release_id,
        "source_schema_version": None,
        "observed_at": None,
        "active_count": None,
        "stale_count": None,
        "claimed_total": None,
        "completed_count": None,
        "retry_wait_count": None,
        "max_fence": None,
        "draining": None,
        "heartbeat_interval_seconds": None,
        "lease_ttl_seconds": None,
        "effective_concurrency_limit": None,
        "available_concurrency_slots": None,
        "subject_peak_active": None,
        "verified_runner_active_task_count": None,
        "runner_evidenced_task_count": None,
        "runner_interval_missing_count": None,
        "scheduler_claim_subject_peak_active": None,
        "terminal_index_sha256": None,
        "terminal_task_count": None,
        "terminal_by_outcome": None,
        "terminal_failure_bindings": None,
        "global_active_task_count": None,
        "global_peak_active": None,
        "backpressure_reason": None,
        "canary_concurrency_telemetry": None,
        "error_code": error_code,
    }


def _public_subject_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != set(SUBJECTS):
        raise DashboardProjectionError("concurrency_telemetry_topology_invalid")
    result: dict[str, int] = {}
    for subject in SUBJECTS:
        raw = value.get(subject)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise DashboardProjectionError("concurrency_telemetry_counter_invalid")
        result[subject] = raw
    return result


def _public_concurrency_telemetry(value: Any, *, release_id: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DashboardProjectionError("concurrency_telemetry_missing")
    if (
        value.get("schema_version")
        != "study-intake-production-canary-concurrency-telemetry-v1"
        or value.get("release_id") != release_id
        or value.get("counter_scope") != CONCURRENCY_COUNTER_SCOPE
        or value.get("peak_source") != CONCURRENCY_PEAK_SOURCE
        or value.get("runner_interval_policy")
        != CONCURRENCY_RUNNER_INTERVAL_POLICY
        or "requested_service_tier" not in value
        or value.get("requested_service_tier") is not None
        or value.get("fast_mode_requested") is not False
        or value.get("fast_mode_effective") != "not_requested"
        or value.get("model_call_count") != 0
        or value.get("provider_request_count") != 0
        or value.get("formal_write_count") != 0
        or value.get("sol_enabled") is not False
        or _valid_timestamp(value.get("updated_at")) is None
    ):
        raise DashboardProjectionError("concurrency_telemetry_invalid")

    activation_ids = value.get("activation_ids")
    terminal_indexes = value.get("terminal_index_sha256_by_subject")
    if (
        not isinstance(activation_ids, Mapping)
        or set(activation_ids) != set(SUBJECTS)
        or not isinstance(terminal_indexes, Mapping)
        or set(terminal_indexes) != set(SUBJECTS)
    ):
        raise DashboardProjectionError("concurrency_telemetry_topology_invalid")
    public_activation_ids: dict[str, str | None] = {}
    public_terminal_indexes: dict[str, str | None] = {}
    for subject in SUBJECTS:
        activation_id = activation_ids.get(subject)
        terminal_index = terminal_indexes.get(subject)
        if (
            activation_id is not None
            and _safe_hash(activation_id) is None
        ) or (
            terminal_index is not None
            and _safe_hash(terminal_index) is None
        ) or ((activation_id is None) is not (terminal_index is None)):
            raise DashboardProjectionError("concurrency_telemetry_binding_invalid")
        public_activation_ids[subject] = activation_id
        public_terminal_indexes[subject] = terminal_index

    public_active = _public_subject_count_map(value.get("active_by_subject"))
    public_peaks = _public_subject_count_map(value.get("subject_peak_active"))
    verified_runner_active = _public_subject_count_map(
        value.get("verified_runner_active_by_subject")
    )
    scheduler_peaks = _public_subject_count_map(
        value.get("scheduler_claim_subject_peak_active")
    )
    runner_evidenced = _public_subject_count_map(
        value.get("runner_evidenced_task_count_by_subject")
    )
    runner_missing = _public_subject_count_map(
        value.get("runner_interval_missing_count_by_subject")
    )
    zero_duration = _public_subject_count_map(
        value.get("zero_duration_runner_interval_count_by_subject")
    )
    terminal_counts = _public_subject_count_map(
        value.get("terminal_task_count_by_subject")
    )

    raw_outcomes = value.get("terminal_by_outcome_by_subject")
    raw_failures = value.get("terminal_failure_bindings_by_subject")
    if (
        not isinstance(raw_outcomes, Mapping)
        or set(raw_outcomes) != set(SUBJECTS)
        or not isinstance(raw_failures, Mapping)
        or set(raw_failures) != set(SUBJECTS)
    ):
        raise DashboardProjectionError("concurrency_telemetry_topology_invalid")
    outcomes_by_subject: dict[str, dict[str, int]] = {}
    failures_by_subject: dict[str, list[dict[str, Any]]] = {}
    for subject in SUBJECTS:
        outcomes = _public_terminal_counts(raw_outcomes.get(subject))
        failures = _public_terminal_failure_bindings(raw_failures.get(subject))
        if (
            sum(outcomes.values()) != terminal_counts[subject]
            or len(failures)
            != sum(outcomes[outcome] for outcome in TERMINAL_FAILURE_OUTCOMES)
            or runner_evidenced[subject] + runner_missing[subject]
            != terminal_counts[subject] + public_active[subject]
            or verified_runner_active[subject] > public_active[subject]
            or public_peaks[subject] < verified_runner_active[subject]
            or scheduler_peaks[subject] < public_active[subject]
            or zero_duration[subject] > runner_evidenced[subject]
        ):
            raise DashboardProjectionError("concurrency_telemetry_counter_invalid")
        if public_activation_ids[subject] is None and (
            public_active[subject] != 0
            or terminal_counts[subject] != 0
            or failures
        ):
            raise DashboardProjectionError("concurrency_telemetry_binding_invalid")
        outcomes_by_subject[subject] = outcomes
        failures_by_subject[subject] = failures

    terminal_outcomes_global = _public_terminal_counts(
        value.get("terminal_by_outcome_global")
    )
    global_active = value.get("global_active_task_count")
    global_peak = value.get("global_peak_active")
    global_verified_active = value.get(
        "verified_runner_global_active_task_count"
    )
    scheduler_global_peak = value.get("scheduler_claim_global_peak_active")
    terminal_global = value.get("terminal_task_count_global")
    runner_evidenced_global = value.get("runner_evidenced_task_count_global")
    runner_missing_global = value.get("runner_interval_missing_count_global")
    zero_duration_global = value.get(
        "zero_duration_runner_interval_count_global"
    )
    global_values = (
        global_active,
        global_peak,
        global_verified_active,
        scheduler_global_peak,
        terminal_global,
        runner_evidenced_global,
        runner_missing_global,
        zero_duration_global,
    )
    if any(
        isinstance(raw, bool) or not isinstance(raw, int) or raw < 0
        for raw in global_values
    ) or (
        global_active != sum(public_active.values())
        or global_verified_active != sum(verified_runner_active.values())
        or global_peak < global_verified_active
        or scheduler_global_peak < global_active
        or terminal_global != sum(terminal_counts.values())
        or runner_evidenced_global != sum(runner_evidenced.values())
        or runner_missing_global != sum(runner_missing.values())
        or zero_duration_global != sum(zero_duration.values())
        or terminal_outcomes_global
        != {
            outcome: sum(
                outcomes_by_subject[subject][outcome]
                for subject in SUBJECTS
            )
            for outcome in TERMINAL_OUTCOMES
        }
    ):
        raise DashboardProjectionError("concurrency_telemetry_counter_invalid")

    authority = value.get("authority")
    if (
        not isinstance(authority, Mapping)
        or authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("purpose")
        != "dispatch-production-canary-concurrency-telemetry"
        or _safe_hash(authority.get("key_id")) is None
        or _safe_hash(authority.get("hmac_sha256")) is None
    ):
        raise DashboardProjectionError("concurrency_telemetry_authority_invalid")
    public_authority = {
        "schema_version": "study-intake-dispatch-authority-v1",
        "algorithm": "HMAC-SHA256",
        "key_id": str(authority["key_id"]),
        "purpose": "dispatch-production-canary-concurrency-telemetry",
        "hmac_sha256": str(authority["hmac_sha256"]),
    }
    return {
        "schema_version": str(value["schema_version"]),
        "release_id": release_id,
        "activation_ids": public_activation_ids,
        "active_by_subject": public_active,
        "subject_peak_active": public_peaks,
        "global_active_task_count": global_active,
        "global_peak_active": global_peak,
        "verified_runner_active_by_subject": verified_runner_active,
        "verified_runner_global_active_task_count": global_verified_active,
        "scheduler_claim_subject_peak_active": scheduler_peaks,
        "scheduler_claim_global_peak_active": scheduler_global_peak,
        "runner_evidenced_task_count_by_subject": runner_evidenced,
        "runner_evidenced_task_count_global": runner_evidenced_global,
        "runner_interval_missing_count_by_subject": runner_missing,
        "runner_interval_missing_count_global": runner_missing_global,
        "zero_duration_runner_interval_count_by_subject": zero_duration,
        "zero_duration_runner_interval_count_global": zero_duration_global,
        "terminal_index_sha256_by_subject": public_terminal_indexes,
        "terminal_task_count_by_subject": terminal_counts,
        "terminal_task_count_global": terminal_global,
        "terminal_by_outcome_by_subject": outcomes_by_subject,
        "terminal_by_outcome_global": terminal_outcomes_global,
        "terminal_failure_bindings_by_subject": failures_by_subject,
        "counter_scope": CONCURRENCY_COUNTER_SCOPE,
        "peak_source": CONCURRENCY_PEAK_SOURCE,
        "runner_interval_policy": CONCURRENCY_RUNNER_INTERVAL_POLICY,
        "requested_service_tier": None,
        "fast_mode_requested": False,
        "fast_mode_effective": "not_requested",
        "updated_at": str(value["updated_at"]),
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
        "authority_sha256": hashlib.sha256(
            _canonical_bytes(public_authority)
        ).hexdigest(),
        "authority": public_authority,
    }


def _lease_concurrency_observation(
    value: Any,
    *,
    subject: str,
    release_id: str,
) -> dict[str, Any]:
    """Normalize the trusted LeaseStore status without inventing zeroes."""

    unavailable = lambda code: _unavailable_concurrency_observation(
        subject, release_id, code
    )
    if not isinstance(value, Mapping):
        return unavailable("lease_status_unavailable")
    if (
        value.get("schema_version")
        != "study-intake-dispatch-subject-status-v1"
    ):
        return unavailable("lease_status_schema_invalid")
    if value.get("subject") != subject:
        return unavailable("lease_status_subject_mismatch")
    observed_at = _valid_timestamp(value.get("observed_at"))
    if observed_at is None:
        return unavailable("lease_status_observed_at_invalid")
    counts: dict[str, int] = {}
    for key in (
        "active_count",
        "stale_count",
        "claimed_total",
        "completed_count",
        "retry_wait_count",
        "max_fence",
        "heartbeat_interval_seconds",
        "lease_ttl_seconds",
    ):
        raw = value.get(key)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            return unavailable("lease_status_counter_invalid")
        counts[key] = raw
    if counts["claimed_total"] < (
        counts["active_count"] + counts["stale_count"]
    ):
        return unavailable("lease_status_claimed_count_invalid")
    if counts["heartbeat_interval_seconds"] < 1 or counts["lease_ttl_seconds"] < 1:
        return unavailable("lease_status_timing_invalid")
    draining = value.get("draining")
    if not isinstance(draining, bool):
        return unavailable("lease_status_draining_invalid")
    canary_gate = value.get("canary_gate")
    telemetry: dict[str, Any] | None = None
    effective_limit: int | None = None
    subject_peak: int | None = None
    verified_runner_active: int | None = None
    runner_evidenced: int | None = None
    runner_missing: int | None = None
    scheduler_claim_peak: int | None = None
    terminal_index_sha256: str | None = None
    terminal_task_count: int | None = None
    terminal_by_outcome: dict[str, int] | None = None
    terminal_failure_bindings: list[dict[str, Any]] | None = None
    global_active: int | None = None
    global_peak: int | None = None
    backpressure_reason: str | None = None
    if canary_gate is not None:
        try:
            telemetry = _public_concurrency_telemetry(
                value.get("canary_concurrency_telemetry"),
                release_id=release_id,
            )
        except DashboardProjectionError as exc:
            return unavailable(str(exc))
        for key in (
            "effective_concurrency_limit",
            "available_concurrency_slots",
            "subject_peak_active",
            "verified_runner_active_task_count",
            "runner_evidenced_task_count",
            "runner_interval_missing_count",
            "scheduler_claim_subject_peak_active",
            "terminal_task_count",
            "global_active_task_count",
            "global_peak_active",
        ):
            raw = value.get(key)
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                return unavailable("lease_status_concurrency_counter_invalid")
        effective_limit = int(value["effective_concurrency_limit"])
        available_slots = int(value["available_concurrency_slots"])
        subject_peak = int(value["subject_peak_active"])
        verified_runner_active = int(
            value["verified_runner_active_task_count"]
        )
        runner_evidenced = int(value["runner_evidenced_task_count"])
        runner_missing = int(value["runner_interval_missing_count"])
        scheduler_claim_peak = int(
            value["scheduler_claim_subject_peak_active"]
        )
        terminal_task_count = int(value["terminal_task_count"])
        terminal_index_sha256 = _safe_hash(
            value.get("terminal_index_sha256")
        )
        try:
            terminal_by_outcome = _public_terminal_counts(
                value.get("terminal_by_outcome")
            )
            terminal_failure_bindings = _public_terminal_failure_bindings(
                value.get("terminal_failure_bindings")
            )
        except DashboardProjectionError as exc:
            return unavailable(str(exc))
        global_active = int(value["global_active_task_count"])
        global_peak = int(value["global_peak_active"])
        if (
            subject_peak != telemetry["subject_peak_active"][subject]
            or verified_runner_active
            != telemetry["verified_runner_active_by_subject"][subject]
            or runner_evidenced
            != telemetry["runner_evidenced_task_count_by_subject"][subject]
            or runner_missing
            != telemetry["runner_interval_missing_count_by_subject"][subject]
            or scheduler_claim_peak
            != telemetry["scheduler_claim_subject_peak_active"][subject]
            or terminal_index_sha256 is None
            or terminal_index_sha256
            != telemetry["terminal_index_sha256_by_subject"][subject]
            or terminal_index_sha256 != canary_gate.get("terminal_index_sha256")
            or terminal_task_count
            != telemetry["terminal_task_count_by_subject"][subject]
            or terminal_task_count != canary_gate.get("terminal_task_count")
            or terminal_by_outcome
            != telemetry["terminal_by_outcome_by_subject"][subject]
            or terminal_by_outcome != canary_gate.get("terminal_by_outcome")
            or terminal_failure_bindings
            != telemetry["terminal_failure_bindings_by_subject"][subject]
            or global_active != telemetry["global_active_task_count"]
            or global_peak != telemetry["global_peak_active"]
            or available_slots
            != max(
                0,
                effective_limit
                - int(canary_gate.get("active_task_count") or 0),
            )
        ):
            return unavailable("lease_status_concurrency_binding_invalid")
        raw_backpressure = value.get("backpressure_reason")
        if raw_backpressure is not None:
            backpressure_reason = _safe_text(raw_backpressure, 160)
            if (
                backpressure_reason is None
                or SAFE_CODE.fullmatch(backpressure_reason) is None
            ):
                return unavailable("lease_status_backpressure_invalid")
        if backpressure_reason not in {
            None,
            "initial_canary_inflight_limit_reached",
            "continuous_concurrency_limit_reached",
            "luna_consumer_disabled",
        } or backpressure_reason != canary_gate.get("backpressure_reason"):
            return unavailable("lease_status_backpressure_binding_invalid")
    elif value.get("canary_concurrency_telemetry") is not None:
        return unavailable("lease_status_concurrency_without_canary")
    return {
        "schema_version": CONCURRENCY_OBSERVATION_SCHEMA,
        "status": "verified",
        "subject": subject,
        "release_id": release_id,
        "source_schema_version": "study-intake-dispatch-subject-status-v1",
        "observed_at": observed_at,
        **counts,
        "draining": draining,
        "effective_concurrency_limit": effective_limit,
        "available_concurrency_slots": (
            available_slots if canary_gate is not None else None
        ),
        "subject_peak_active": subject_peak,
        "verified_runner_active_task_count": verified_runner_active,
        "runner_evidenced_task_count": runner_evidenced,
        "runner_interval_missing_count": runner_missing,
        "scheduler_claim_subject_peak_active": scheduler_claim_peak,
        "terminal_index_sha256": terminal_index_sha256,
        "terminal_task_count": terminal_task_count,
        "terminal_by_outcome": terminal_by_outcome,
        "terminal_failure_bindings": terminal_failure_bindings,
        "global_active_task_count": global_active,
        "global_peak_active": global_peak,
        "backpressure_reason": backpressure_reason,
        "canary_concurrency_telemetry": telemetry,
        "error_code": None,
    }


def _latest_concurrency_telemetry(
    heartbeat_values: Mapping[str, Mapping[str, Any] | None],
    *,
    release_id: str,
) -> dict[str, Any] | None:
    candidates: list[tuple[dt.datetime, str, dict[str, Any]]] = []
    for subject in SUBJECTS:
        heartbeat = heartbeat_values.get(subject)
        observation = (
            heartbeat.get("concurrency_observation")
            if isinstance(heartbeat, Mapping)
            else None
        )
        telemetry = (
            observation.get("canary_concurrency_telemetry")
            if isinstance(observation, Mapping)
            else None
        )
        if (
            not isinstance(telemetry, Mapping)
            or telemetry.get("release_id") != release_id
        ):
            continue
        timestamp = _valid_timestamp(telemetry.get("updated_at"))
        if timestamp is None:
            continue
        parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        canonical_sha256 = hashlib.sha256(
            _canonical_bytes(dict(telemetry))
        ).hexdigest()
        candidates.append((parsed, canonical_sha256, dict(telemetry)))
    if not candidates:
        return None
    candidates.sort(key=lambda row: (row[0], row[1]))
    latest_time = candidates[-1][0]
    latest = [row for row in candidates if row[0] == latest_time]
    if len({row[1] for row in latest}) != 1:
        raise DashboardProjectionError("concurrency_telemetry_latest_conflict")
    return latest[-1][2]


def _subject_concurrency_projection(
    observation: Any,
    canary_gate: Mapping[str, Any] | None,
    shared_telemetry: Mapping[str, Any] | None,
    *,
    subject: str,
    release_id: str,
) -> dict[str, Any]:
    if (
        not isinstance(observation, Mapping)
        or observation.get("schema_version") != CONCURRENCY_OBSERVATION_SCHEMA
        or observation.get("subject") != subject
        or observation.get("release_id") != release_id
    ):
        observation = _unavailable_concurrency_observation(
            subject, release_id, "lease_status_unavailable"
        )

    state = str(canary_gate.get("state")) if canary_gate is not None else None
    state_projection = {
        "armed": ("first_canary_armed", 1, "bounded"),
        "canary_in_flight": (
            "first_canary_single_in_flight",
            1,
            "bounded",
        ),
        "continuous_concurrent_unlocked": (
            "continuous_concurrent_unlocked",
            (
                int(canary_gate["continuous_concurrency_limit"])
                if canary_gate is not None
                else None
            ),
            "bounded",
        ),
        "failed_drained": ("failed_drained", 0, "bounded"),
        "paused_drained": ("paused_drained", 0, "bounded"),
        "inactive_rolled_back": ("inactive_rolled_back", 0, "bounded"),
    }.get(state, ("canary_not_configured", None, "unavailable"))
    concurrency_state, effective_limit, limit_mode = state_projection

    source_verified = observation.get("status") == "verified"
    active = observation.get("active_count") if source_verified else None
    stale = observation.get("stale_count") if source_verified else None
    retry_wait = observation.get("retry_wait_count") if source_verified else None
    error_code = (
        None
        if source_verified
        else _safe_text(observation.get("error_code"), 160)
        or "lease_status_unavailable"
    )

    inconsistent = False
    telemetry_active: int | None = None
    telemetry_peak: int | None = None
    verified_runner_active: int | None = None
    runner_evidenced: int | None = None
    runner_missing: int | None = None
    scheduler_claim_peak: int | None = None
    terminal_index_sha256: str | None = None
    terminal_task_count: int | None = None
    terminal_by_outcome: dict[str, int] | None = None
    terminal_failure_bindings: list[dict[str, Any]] | None = None
    if canary_gate is not None:
        if (
            not isinstance(shared_telemetry, Mapping)
            or shared_telemetry.get("release_id") != release_id
            or not isinstance(shared_telemetry.get("activation_ids"), Mapping)
            or shared_telemetry["activation_ids"].get(subject)
            != canary_gate.get("activation_id")
            or not isinstance(shared_telemetry.get("active_by_subject"), Mapping)
            or not isinstance(
                shared_telemetry.get("subject_peak_active"), Mapping
            )
        ):
            inconsistent = True
        else:
            telemetry_active = shared_telemetry["active_by_subject"].get(subject)
            telemetry_peak = shared_telemetry["subject_peak_active"].get(subject)
            verified_runner_active = observation.get(
                "verified_runner_active_task_count"
            )
            runner_evidenced = observation.get("runner_evidenced_task_count")
            runner_missing = observation.get("runner_interval_missing_count")
            scheduler_claim_peak = observation.get(
                "scheduler_claim_subject_peak_active"
            )
            terminal_index_sha256 = observation.get("terminal_index_sha256")
            terminal_task_count = observation.get("terminal_task_count")
            terminal_by_outcome = observation.get("terminal_by_outcome")
            terminal_failure_bindings = observation.get(
                "terminal_failure_bindings"
            )
            if (
                isinstance(telemetry_active, bool)
                or not isinstance(telemetry_active, int)
                or isinstance(telemetry_peak, bool)
                or not isinstance(telemetry_peak, int)
                or telemetry_active != canary_gate.get("active_task_count")
                or telemetry_active != active
                or any(
                    isinstance(raw, bool)
                    or not isinstance(raw, int)
                    or raw < 0
                    for raw in (
                        verified_runner_active,
                        runner_evidenced,
                        runner_missing,
                        scheduler_claim_peak,
                        terminal_task_count,
                    )
                )
                or _safe_hash(terminal_index_sha256) is None
                or not isinstance(terminal_by_outcome, Mapping)
                or not isinstance(terminal_failure_bindings, list)
            ):
                inconsistent = True
    if isinstance(active, int) and not isinstance(active, bool):
        if state == "canary_in_flight" and active != 1:
            inconsistent = True
        elif state in {
            "armed",
            "paused_drained",
            "inactive_rolled_back",
        } and active != 0:
            inconsistent = True
    if inconsistent:
        source_verified = False
        active = None
        stale = None
        retry_wait = None
        verified_runner_active = None
        runner_evidenced = None
        runner_missing = None
        scheduler_claim_peak = None
        terminal_index_sha256 = None
        terminal_task_count = None
        terminal_by_outcome = None
        terminal_failure_bindings = None
        error_code = "canary_lease_snapshot_inconsistent"

    pending: int | None = None
    if (
        source_verified
        and canary_gate is not None
        and isinstance(retry_wait, int)
    ):
        pending = int(canary_gate["queue_depth"]) + retry_wait

    runner_evidence_complete = (
        source_verified
        and canary_gate is not None
        and runner_missing == 0
    )
    peak = telemetry_peak if runner_evidence_complete else None
    effective_observed = observation.get("effective_concurrency_limit")
    if canary_gate is not None and (
        isinstance(effective_observed, bool)
        or not isinstance(effective_observed, int)
        or effective_observed != effective_limit
    ):
        source_verified = False
        active = None
        pending = None
        peak = None
        stale = None
        retry_wait = None
        verified_runner_active = None
        runner_evidenced = None
        runner_missing = None
        scheduler_claim_peak = None
        terminal_index_sha256 = None
        terminal_task_count = None
        terminal_by_outcome = None
        terminal_failure_bindings = None
        error_code = "effective_concurrency_limit_mismatch"

    if not source_verified:
        status = "unavailable"
    elif canary_gate is None:
        status = "partial"
    elif not runner_evidence_complete:
        status = "partial"
        error_code = "runner_interval_evidence_incomplete"
    else:
        status = "verified"
    projected_effective_limit = (
        effective_limit
        if source_verified and canary_gate is not None
        else None
    )
    projected_limit_mode = (
        limit_mode
        if source_verified and canary_gate is not None
        else "unavailable"
    )
    available_slots = (
        max(0, projected_effective_limit - active)
        if isinstance(projected_effective_limit, int)
        and isinstance(active, int)
        else None
    )

    backpressure: str | None = None
    if not source_verified:
        backpressure = error_code
    elif canary_gate is None:
        backpressure = "canary_state_unavailable"
    elif not runner_evidence_complete:
        backpressure = "runner_interval_evidence_incomplete"
    elif isinstance(stale, int) and stale > 0:
        backpressure = "stale_lease_fence_detected"
    elif canary_gate.get("blocking_reason") is not None:
        backpressure = str(canary_gate["blocking_reason"])
    elif canary_gate.get("backpressure_reason") is not None:
        backpressure = str(canary_gate["backpressure_reason"])
    elif isinstance(retry_wait, int) and retry_wait > 0:
        backpressure = "lease_retry_wait"
    elif state == "canary_in_flight" and isinstance(pending, int) and pending > 0:
        backpressure = "first_canary_single_in_flight"
    elif state == "armed" and isinstance(pending, int) and pending > 0:
        backpressure = "awaiting_first_canary_admission"

    return {
        "concurrency_status": status,
        "concurrency_state": concurrency_state,
        "concurrency_source": CONCURRENCY_SOURCE if source_verified else None,
        "concurrency_observed_at": (
            observation.get("observed_at") if source_verified else None
        ),
        "concurrency_error_code": error_code,
        "subject_active_task_count": active,
        "subject_pending_task_count": pending,
        "subject_peak_active": peak,
        "subject_verified_runner_active_task_count": verified_runner_active,
        "subject_runner_evidenced_task_count": runner_evidenced,
        "subject_runner_interval_missing_count": runner_missing,
        "scheduler_claim_subject_peak_active": scheduler_claim_peak,
        "terminal_index_sha256": terminal_index_sha256,
        "terminal_task_count": terminal_task_count,
        "terminal_by_outcome": (
            dict(terminal_by_outcome)
            if isinstance(terminal_by_outcome, Mapping)
            else None
        ),
        "terminal_failure_bindings": (
            list(terminal_failure_bindings)
            if isinstance(terminal_failure_bindings, list)
            else None
        ),
        "lease_retry_wait_task_count": retry_wait,
        "lease_stale_task_count": stale,
        "effective_concurrency_limit": projected_effective_limit,
        "effective_concurrency_limit_mode": projected_limit_mode,
        "available_concurrency_slots": available_slots,
        "backpressure_reason": backpressure,
    }


def _global_concurrency_projection(
    subjects: Mapping[str, Mapping[str, Any]],
    shared_telemetry: Mapping[str, Any] | None,
    *,
    release_id: str,
    generated_at: str,
) -> dict[str, Any]:
    if (
        isinstance(shared_telemetry, Mapping)
        and shared_telemetry.get("release_id") == release_id
    ):
        global_active = shared_telemetry.get("global_active_task_count")
        global_peak = shared_telemetry.get("global_peak_active")
        telemetry_authority_sha256 = shared_telemetry.get("authority_sha256")
        global_verified_runner_active = shared_telemetry.get(
            "verified_runner_global_active_task_count"
        )
        scheduler_claim_global_peak = shared_telemetry.get(
            "scheduler_claim_global_peak_active"
        )
        runner_evidenced_global = shared_telemetry.get(
            "runner_evidenced_task_count_global"
        )
        runner_missing_global = shared_telemetry.get(
            "runner_interval_missing_count_global"
        )
        zero_duration_global = shared_telemetry.get(
            "zero_duration_runner_interval_count_global"
        )
        terminal_task_count_global = shared_telemetry.get(
            "terminal_task_count_global"
        )
        terminal_by_outcome_global = shared_telemetry.get(
            "terminal_by_outcome_global"
        )
        terminal_failure_binding_count_global = sum(
            len(shared_telemetry["terminal_failure_bindings_by_subject"][subject])
            for subject in SUBJECTS
        )
    else:
        global_active = None
        global_peak = None
        telemetry_authority_sha256 = None
        global_verified_runner_active = None
        scheduler_claim_global_peak = None
        runner_evidenced_global = None
        runner_missing_global = None
        zero_duration_global = None
        terminal_task_count_global = None
        terminal_by_outcome_global = None
        terminal_failure_binding_count_global = None

    limit_modes = [
        subjects[subject].get("effective_concurrency_limit_mode")
        for subject in SUBJECTS
    ]
    if any(mode == "unavailable" for mode in limit_modes):
        global_limit = None
        global_limit_mode = "unavailable"
    elif any(mode == "unbounded" for mode in limit_modes):
        global_limit = None
        global_limit_mode = "unbounded"
    elif all(mode == "bounded" for mode in limit_modes):
        limits = [
            subjects[subject].get("effective_concurrency_limit")
            for subject in SUBJECTS
        ]
        if all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in limits
        ):
            global_limit = sum(limits)
            global_limit_mode = "bounded"
        else:
            global_limit = None
            global_limit_mode = "unavailable"
    else:
        global_limit = None
        global_limit_mode = "unavailable"

    statuses = [subjects[subject].get("concurrency_status") for subject in SUBJECTS]
    status = (
        "verified"
        if all(value == "verified" for value in statuses)
        else "partial"
        if any(value in {"verified", "partial"} for value in statuses)
        else "unavailable"
    )
    if status == "unavailable" or any(
        value == "unavailable" for value in statuses
    ):
        global_active = None
    if status != "verified":
        global_peak = None
    available_slots = (
        max(0, global_limit - global_active)
        if isinstance(global_limit, int) and isinstance(global_active, int)
        else None
    )
    reasons = [
        (subject, subjects[subject].get("backpressure_reason"))
        for subject in SUBJECTS
        if isinstance(subjects[subject].get("backpressure_reason"), str)
    ]
    if not reasons:
        backpressure_reason = None
    elif len(reasons) == 1:
        backpressure_reason = f"{reasons[0][0]}:{reasons[0][1]}"
    else:
        backpressure_reason = "multiple_subject_backpressure"

    return {
        "schema_version": "study-intake-dashboard-concurrency-v1",
        "status": status,
        "release_id": release_id,
        "generated_at": generated_at,
        "source": (
            CONCURRENCY_SOURCE
            if isinstance(shared_telemetry, Mapping)
            else None
        ),
        "source_telemetry": (
            dict(shared_telemetry)
            if isinstance(shared_telemetry, Mapping)
            else None
        ),
        "telemetry_authority_sha256": telemetry_authority_sha256,
        "peak_semantics": "hmac_task_process_half_open_intervals",
        "global_active_task_count": global_active,
        "global_peak_active": global_peak,
        "verified_runner_global_active_task_count": (
            global_verified_runner_active
        ),
        "scheduler_claim_global_peak_active": scheduler_claim_global_peak,
        "runner_evidenced_task_count_global": runner_evidenced_global,
        "runner_interval_missing_count_global": runner_missing_global,
        "zero_duration_runner_interval_count_global": zero_duration_global,
        "terminal_task_count_global": terminal_task_count_global,
        "terminal_by_outcome_global": (
            dict(terminal_by_outcome_global)
            if isinstance(terminal_by_outcome_global, Mapping)
            else None
        ),
        "terminal_failure_binding_count_global": (
            terminal_failure_binding_count_global
        ),
        "effective_concurrency_limit": global_limit,
        "effective_concurrency_limit_mode": global_limit_mode,
        "available_concurrency_slots": available_slots,
        "backpressure_reason": backpressure_reason,
        "subject_observed_at": {
            subject: subjects[subject].get("concurrency_observed_at")
            for subject in SUBJECTS
        },
    }


def _capture_high_watermark(
    raw: Any,
    authority_generation: str,
) -> dict[str, Any] | None:
    watermark = _safe_control_id(raw)
    generation = _safe_control_id(authority_generation)
    if watermark is None or generation is None:
        return None
    return {
        "value": watermark,
        "authority_generation": generation,
    }


def _subject_batch_state(
    runtime_root: Path,
    subject: str,
    study_date: str | None,
) -> dict[str, Any] | None:
    value = _bounded_object(
        runtime_root
        / "dispatch"
        / "state"
        / "subject-luna-batches"
        / f"{subject}.json",
        MAX_CONTROL_STATE_BYTES,
    )
    if value is None:
        return None
    schema_version = value.get("schema_version")
    try:
        value = (
            validate_subject_luna_batch_v2(value)
            if schema_version == SUBJECT_BATCH_V2_SCHEMA
            else validate_subject_luna_batch_v1(value)
        )
    except (SubjectSolContractError, TypeError, ValueError):
        return None
    if value.get("subject") != subject or (
        study_date is not None and value.get("study_date") != study_date
    ):
        return None
    batch_id = _safe_control_id(value.get("batch_id"))
    generation = _safe_control_id(value.get("authority_generation"))
    high_watermark = _capture_high_watermark(
        value.get("capture_high_watermark"),
        generation or "",
    )
    scan_snapshot_sha256 = _safe_hash(value.get("scan_snapshot_sha256"))
    if (
        batch_id is None or generation is None or high_watermark is None
        or scan_snapshot_sha256 is None
    ):
        return None
    tasks: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_task in value["tasks"]:
        if not isinstance(raw_task, Mapping):
            return None
        capture_id = _safe_control_id(raw_task.get("capture_id"))
        status = _safe_text(raw_task.get("status"), 40)
        unit_sha256 = _safe_hash(raw_task.get("unit_sha256"))
        input_fingerprint = _safe_hash(raw_task.get("input_fingerprint"))
        task_study_date = _safe_text(raw_task.get("study_date"), 10)
        frozen_sha256 = _safe_hash(raw_task.get("frozen_payload_sha256"))
        if (
            capture_id is None
            or capture_id in seen
            or status
            not in (
                SUBJECT_BATCH_V2_TASK_STATES
                if schema_version == SUBJECT_BATCH_V2_SCHEMA
                else SUBJECT_BATCH_TASK_STATES
            )
            or unit_sha256 is None
            or input_fingerprint is None
            or task_study_date is None
            or frozen_sha256 is None
        ):
            return None
        seen.add(capture_id)
        task = {
            "capture_id": capture_id,
            "status": status,
            "unit_sha256": unit_sha256,
            "input_fingerprint": input_fingerprint,
            "study_date": task_study_date,
            "frozen_payload_sha256": frozen_sha256,
        }
        for key in (
            "proposal_sha256",
            "package_sha256",
            "quality_receipt_sha256",
            "analysis_execution_receipt_sha256",
            "analysis_raw_output_sha256",
            "analysis_normalization_receipt_sha256",
            "analysis_report_sha256",
            "critical_review_execution_receipt_sha256",
            "critical_review_raw_output_sha256",
            "critical_review_normalization_receipt_sha256",
            "critical_review_report_sha256",
            "sol_handoff_envelope_sha256",
            "terminal_receipt_sha256",
        ):
            digest = _safe_hash(raw_task.get(key))
            if digest is not None:
                task[key] = digest
        if schema_version == SUBJECT_BATCH_V2_SCHEMA:
            task["warning_codes"] = list(raw_task.get("warning_codes") or [])
            error_code = _safe_text(raw_task.get("error_code"), 160)
            if error_code is not None and SAFE_CODE.fullmatch(error_code):
                task["error_code"] = error_code
            task["quality_outcome"] = (
                "accepted"
                if status == "workflow_complete"
                else "needs_sol_review"
                if status == "workflow_complete_with_warnings"
                else "failed"
                if status in SUBJECT_BATCH_V2_TERMINAL_STATES
                else "pending"
            )
        elif status == "quality_passed":
            quality_sha = _safe_hash(raw_task.get("quality_receipt_sha256"))
            receipt = None
            if quality_sha is not None:
                try:
                    receipt = SubjectSolRuntimeStore(
                        runtime_root
                    ).read_verified_quality_receipt(
                        quality_sha, subject=subject, task=raw_task
                    )
                except (SubjectSolContractError, OSError, ValueError):
                    receipt = None
            task["quality_receipt_verified"] = receipt is not None
            task["quality_outcome"] = (
                receipt["review_outcome"] if receipt is not None else "pending"
            )
        else:
            task["quality_outcome"] = {
                "needs_rework": "rejected",
                "failed": "failed",
            }.get(status, "pending")
        tasks.append(task)
    list_fields: dict[str, list[str]] = {}
    for field in (
        "blocking_task_ids",
        "sol_candidate_task_ids",
        "diagnostic_task_ids",
    ):
        raw_values = value.get(field, [])
        rows: list[str] = []
        if not isinstance(raw_values, list):
            return None
        for item in raw_values:
            safe = _safe_control_id(item)
            if safe is None:
                return None
            rows.append(safe)
        list_fields[field] = rows
    return {
        "schema_version": schema_version,
        "batch_id": batch_id,
        "study_date": str(value["study_date"]),
        "status": value["status"],
        "capture_high_watermark": high_watermark,
        "scan_snapshot_sha256": scan_snapshot_sha256,
        "authority_generation": generation,
        "tasks": tasks,
        "declared_all_terminal": value["all_terminal"],
        "declared_sol_ready": value["sol_ready"],
        **list_fields,
    }


def _apply_batch_task_state(item: dict[str, Any], task: Mapping[str, Any]) -> None:
    state = str(task["status"])
    if state in SUBJECT_BATCH_V2_TASK_STATES:
        mapped_v2 = {
            "selected": ("queued", "frozen_evidence", "queued", "pending"),
            "claimed": ("running", "frozen_evidence", "processing", "pending"),
            "analysis_running": ("running", "analysis", "processing", "pending"),
            "critical_review_running": (
                "running",
                "critical_review",
                "processing",
                "pending",
            ),
            "packaging": ("running", "quality_ready", "processing", "pending"),
            "workflow_complete": (
                "ready",
                "quality_ready",
                "succeeded",
                "accepted",
            ),
            "workflow_complete_with_warnings": (
                "ready",
                "quality_ready",
                "succeeded",
                "needs_sol_review",
            ),
            "workflow_partial": ("failed", "failed", "failed", "failed"),
            "execution_failed": ("failed", "failed", "failed", "failed"),
            "cancelled": ("stale", "stale", "stale", "failed"),
            "stalled": ("failed", "failed", "failed", "failed"),
        }
        batch_queue, batch_stage, batch_luna, quality_outcome = mapped_v2[state]
        terminal = state in SUBJECT_BATCH_V2_TERMINAL_STATES
        current_rank = {
            "evidence_pending": 0,
            "queued": 0,
            "running:frozen_evidence": 1,
            "running:analysis": 2,
            "running:critical_review": 3,
            "running:quality_ready": 4,
            "ready": 5,
            "needs_rework": 5,
            "failed": 5,
            "stale": 5,
        }.get(
            (
                f"{item.get('queue_state')}:{item.get('current_stage')}"
                if item.get("queue_state") == "running"
                else str(item.get("queue_state"))
            ),
            0,
        )
        batch_rank = {
            "selected": 0,
            "claimed": 1,
            "analysis_running": 2,
            "critical_review_running": 3,
            "packaging": 4,
            "workflow_complete": 5,
            "workflow_complete_with_warnings": 5,
            "workflow_partial": 5,
            "execution_failed": 5,
            "cancelled": 5,
            "stalled": 5,
        }[state]
        # A v2 terminal task always carries a terminal receipt.  It therefore
        # outranks stale current-fence events; nonterminal batch state may only
        # fill gaps or move the item forward.
        if terminal or batch_rank >= current_rank:
            item.update(
                {
                    "queue_state": batch_queue,
                    "current_stage": batch_stage,
                    "luna_status": batch_luna,
                    "local_dispatch_status": (
                        "cancelled"
                        if state == "cancelled"
                        else "stalled"
                        if state == "stalled"
                        else "terminal"
                        if terminal
                        else "claimed"
                        if state == "claimed"
                        else "running"
                        if batch_rank > 0
                        else "pending"
                    ),
                    "model_stage": (
                        "analysis"
                        if state == "analysis_running"
                        else "critical_review"
                        if state == "critical_review_running"
                        else "quality_closed"
                        if state
                        in {
                            "packaging",
                            "workflow_complete",
                            "workflow_complete_with_warnings",
                        }
                        else "cancelled"
                        if state == "cancelled"
                        else "failed"
                        if terminal
                        else "not_started"
                    ),
                    "terminal_status": (
                        "succeeded"
                        if state
                        in {
                            "workflow_complete",
                            "workflow_complete_with_warnings",
                        }
                        else "failed"
                        if terminal
                        else None
                    ),
                    "quality_outcome": quality_outcome,
                    "execution_status": (
                        "succeeded"
                        if state
                        in {
                            "workflow_complete",
                            "workflow_complete_with_warnings",
                        }
                        else "failed"
                        if terminal
                        else "running"
                    ),
                    "quality_status": (
                        "passed"
                        if state == "workflow_complete"
                        else "issues_found"
                        if state == "workflow_complete_with_warnings"
                        else "unchecked"
                    ),
                    "report_disposition": (
                        "accepted"
                        if state == "workflow_complete"
                        else "needs_sol_review"
                        if state == "workflow_complete_with_warnings"
                        else None
                    ),
                    "production_accepted": False,
                }
            )
        analysis_execution_ready = all(
            _safe_hash(task.get(field)) is not None
            for field in (
                "analysis_execution_receipt_sha256",
                "analysis_raw_output_sha256",
            )
        )
        review_execution_ready = all(
            _safe_hash(task.get(field)) is not None
            for field in (
                "critical_review_execution_receipt_sha256",
                "critical_review_raw_output_sha256",
            )
        )
        analysis_report_ready = _safe_hash(task.get("analysis_report_sha256")) is not None
        review_report_ready = _safe_hash(task.get("critical_review_report_sha256")) is not None
        item["analysis_execution_status"] = (
            "completed"
            if analysis_execution_ready
            else "running"
            if state == "analysis_running"
            else "failed"
            if state in {"workflow_partial", "execution_failed", "cancelled", "stalled"}
            else "not_started"
        )
        item["review_execution_status"] = (
            "completed"
            if review_execution_ready
            else "running"
            if state == "critical_review_running"
            else "stalled"
            if state == "stalled" and analysis_execution_ready
            else "cancelled"
            if state == "cancelled" and analysis_execution_ready
            else "failed"
            if state in {"workflow_partial", "execution_failed"} and analysis_execution_ready
            else "not_started"
        )
        warnings = [
            code
            for code in task.get("warning_codes", [])
            if isinstance(code, str) and SAFE_CODE.fullmatch(code)
        ]
        item["warning_codes"] = sorted(set(warnings))
        item["analysis_report_status"] = (
            "available_with_warnings"
            if analysis_report_ready
            and state == "workflow_complete_with_warnings"
            else "available"
            if analysis_report_ready
            else "normalization_failed"
            if analysis_execution_ready and terminal and warnings
            else "not_started"
        )
        item["review_report_status"] = (
            "available_with_warnings"
            if review_report_ready
            and state == "workflow_complete_with_warnings"
            else "available"
            if review_report_ready
            else "normalization_failed"
            if review_execution_ready and terminal and warnings
            else "not_started"
        )
        item["sol_review_status"] = (
            "pending"
            if state in SUBJECT_BATCH_V2_CANDIDATE_STATES
            or state == "workflow_partial"
            and (
                analysis_report_ready or review_report_ready
            )
            else "not_eligible"
        )
        item["formal_write_status"] = "not_authorized"
        item["report_available"] = bool(
            analysis_report_ready or review_report_ready
        )
        item["formal_write_eligible"] = False
        error_code = _safe_text(task.get("error_code"), 160)
        if error_code is not None and SAFE_CODE.fullmatch(error_code):
            item["exact_error_code"] = error_code
            item["last_error_code"] = error_code
            item["processing_error_code"] = error_code
            if item.get("execution_status") == "failed":
                item["terminal_error_code"] = error_code
        elif item.get("execution_status") == "succeeded":
            item["terminal_error_code"] = None
        for key in (
            "authority_snapshot_sha256",
            "analysis_execution_receipt_sha256",
            "analysis_raw_output_sha256",
            "analysis_normalization_receipt_sha256",
            "analysis_report_sha256",
            "critical_review_execution_receipt_sha256",
            "critical_review_raw_output_sha256",
            "critical_review_normalization_receipt_sha256",
            "critical_review_report_sha256",
            "package_sha256",
            "sol_handoff_envelope_sha256",
            "terminal_receipt_sha256",
        ):
            digest = _safe_hash(task.get(key))
            if digest is not None:
                item[key] = digest
        return
    mapped = {
        "selected": ("queued", "frozen_evidence", "queued", "pending"),
        "queued": ("queued", "frozen_evidence", "queued", "pending"),
        "claimed": ("running", "frozen_evidence", "processing", "pending"),
        "retrying": ("queued", "analysis", "retrying", "pending"),
        "analysis_running": ("running", "analysis", "processing", "pending"),
        "critical_review_running": ("running", "critical_review", "processing", "pending"),
        "quality_pending": ("running", "critical_review", "processing", "pending"),
        "quality_passed": ("ready", "quality_ready", "two_pass_ready", "accepted"),
        "needs_rework": ("needs_rework", "needs_rework", "failed", "rejected"),
        "failed": ("failed", "failed", "failed", "failed"),
        "evidence_pending": ("evidence_pending", "frozen_evidence", "skipped", "pending"),
    }[state]
    current_rank = {
        "evidence_pending": 0,
        "queued": 0,
        "running:frozen_evidence": 1,
        "running:analysis": 2,
        "running:critical_review": 3,
        "ready": 4,
        "needs_rework": 4,
        "failed": 4,
        "stale": 4,
    }.get(
        (
            f"{item.get('queue_state')}:{item.get('current_stage')}"
            if item.get("queue_state") == "running"
            else str(item.get("queue_state"))
        ),
        0,
    )
    batch_queue, batch_stage, batch_luna, default_outcome = mapped
    batch_rank = {
        "selected": 0,
        "queued": 0,
        "claimed": 1,
        "retrying": 2,
        "analysis_running": 2,
        "critical_review_running": 3,
        "quality_pending": 3,
        "quality_passed": 4,
        "needs_rework": 4,
        "failed": 4,
        "evidence_pending": 4,
    }[state]
    event_terminal_is_authoritative = (
        current_rank == 4
        and batch_rank == 4
        and isinstance(item.get("last_state_change_at"), str)
    )
    if (batch_rank >= current_rank and not event_terminal_is_authoritative) or (
        state == "quality_pending" and item.get("queue_state") == "ready"
    ):
        item["queue_state"] = batch_queue
        item["current_stage"] = batch_stage
        item["luna_status"] = batch_luna
        terminal_status = (
            batch_queue
            if state in {
                "quality_passed",
                "needs_rework",
                "failed",
                "evidence_pending",
            }
            else None
        )
        item["terminal_status"] = terminal_status
        item["local_dispatch_status"] = (
            "terminal"
            if terminal_status is not None
            else "claimed"
            if state == "claimed"
            else "retrying"
            if state == "retrying"
            else "running"
            if batch_rank > 0
            else "pending"
        )
        item["model_stage"] = {
            "analysis_running": "analysis",
            "critical_review_running": "critical_review",
            "quality_pending": "critical_review",
            "quality_passed": "quality_closed",
            "needs_rework": "quality_closed",
            "failed": "failed",
            "evidence_pending": "not_started",
        }.get(state, "not_started")
        item["quality_outcome"] = task.get("quality_outcome") or default_outcome
        for key in ("package_sha256", "quality_receipt_sha256"):
            if key in task:
                item[key] = task[key]


def _subject_sol_state(
    runtime_root: Path,
    subject: str,
    batch_id: str | None,
    task_count: int,
) -> dict[str, Any] | None:
    value = _bounded_object(
        runtime_root / "dispatch" / "state" / "subject-sol" / f"{subject}.json",
        MAX_CONTROL_STATE_BYTES,
    )
    if (
        value is None
        or value.get("schema_version") != SUBJECT_SOL_WRITER_SCHEMA
        or value.get("subject") != subject
        or value.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
    ):
        return None
    expected_keys = {
        "schema_version",
        "subject",
        "revision",
        "handoff_status",
        "batch_id",
        "writer_adapter",
        "fencing_token",
        "authorization_receipt_sha256",
        "daily_sol_batch_sha256",
        "review_receipt_sha256",
        "commit_receipt_sha256",
        "generation_fence",
        "formal_write_count",
        "updated_at",
    }
    if set(value) != expected_keys:
        return None
    handoff = value.get("handoff_status")
    if handoff not in {
        "awaiting_luna",
        "ready_for_authorization",
        "queued_for_sol",
        "sol_reviewing",
        "sol_reviewed",
        "complete",
        "safe_paused",
        "failed",
    }:
        return None
    state_batch = value.get("batch_id")
    if state_batch is not None and _safe_control_id(state_batch) is None:
        return None
    if batch_id is not None and state_batch not in {None, batch_id}:
        return None
    revision = value.get("revision")
    fencing = value.get("fencing_token")
    formal_writes = value.get("formal_write_count")
    if any(
        isinstance(number, bool) or not isinstance(number, int) or number < 0
        for number in (revision, fencing, formal_writes)
    ):
        return None
    hashes: dict[str, str | None] = {}
    for key in (
        "authorization_receipt_sha256",
        "daily_sol_batch_sha256",
        "review_receipt_sha256",
        "commit_receipt_sha256",
    ):
        raw = value.get(key)
        if raw is not None and _safe_hash(raw) is None:
            return None
        hashes[key] = raw
    fence = value.get("generation_fence")
    if not isinstance(fence, Mapping) or set(fence) != {
        "blocked",
        "source_generation",
        "next_generation",
    } or not isinstance(fence.get("blocked"), bool):
        return None
    for key in ("source_generation", "next_generation"):
        if fence.get(key) is not None and _safe_control_id(fence.get(key)) is None:
            return None
    if value.get("updated_at") is not None and _safe_text(value.get("updated_at"), 64) is None:
        return None
    reviewed = task_count if hashes["review_receipt_sha256"] is not None else 0
    committed = (
        task_count
        if handoff == "complete" and hashes["commit_receipt_sha256"] is not None
        else 0
    )
    return {
        "batch_id": state_batch,
        "sol_reviewed_count": reviewed,
        "sol_committed_count": committed,
        "handoff_status": handoff,
        "formal_write_count": formal_writes,
        "generation_fence": dict(fence),
    }


def _empty_global_sol() -> dict[str, Any]:
    return {
        "state_available": False,
        "status": "no_data",
        "active_subject": None,
        "active_batch_id": None,
        "current_item": None,
        "authorized_queue": [],
        "fencing_token": None,
        "active_writer_count": None,
        "committed_count": None,
        "remaining_count": None,
        "formal_write_count": None,
        "updated_at": None,
    }


def _global_sol_state(runtime_root: Path) -> dict[str, Any]:
    value = _bounded_object(
        runtime_root / "dispatch" / "state" / "global-sol-writer.json",
        MAX_CONTROL_STATE_BYTES,
    )
    if value is None or value.get("schema_version") != GLOBAL_SOL_SCHEMA:
        return _empty_global_sol()
    expected_keys = {
        "schema_version",
        "revision",
        "next_fencing_token",
        "queue",
        "active_writer",
        "active_writer_count",
        "formal_write_count",
        "updated_at",
    }
    if set(value) != expected_keys:
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_state_invalid"}
    active_count = value.get("active_writer_count")
    formal_writes = value.get("formal_write_count")
    revision = value.get("revision")
    next_fencing_token = value.get("next_fencing_token")
    if (
        isinstance(active_count, bool)
        or not isinstance(active_count, int)
        or active_count not in {0, 1}
        or isinstance(formal_writes, bool)
        or not isinstance(formal_writes, int)
        or formal_writes < 0
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 0
        or isinstance(next_fencing_token, bool)
        or not isinstance(next_fencing_token, int)
        or next_fencing_token < 1
        or not isinstance(value.get("queue"), list)
    ):
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_state_invalid"}
    queue: list[dict[str, Any]] = []
    seen_batches: set[str] = set()
    for row in value["queue"]:
        if not isinstance(row, Mapping) or set(row) != {
            "batch_id",
            "subject",
            "authorized_at",
            "authorization_receipt_sha256",
            "daily_sol_batch_sha256",
            "status",
        }:
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_queue_invalid"}
        subject = row.get("subject")
        batch_id = _safe_control_id(row.get("batch_id"))
        authorized_at = _safe_text(row.get("authorized_at"), 64)
        receipt = _safe_hash(row.get("authorization_receipt_sha256"))
        daily_sha = _safe_hash(row.get("daily_sol_batch_sha256"))
        status = _safe_text(row.get("status"), 40)
        if (
            subject not in SUBJECTS
            or batch_id is None
            or batch_id in seen_batches
            or authorized_at is None
            or receipt is None
            or daily_sha is None
            or status not in GLOBAL_SOL_QUEUE_STATES
        ):
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_queue_invalid"}
        seen_batches.add(batch_id)
        public_row = {
            "subject": subject,
            "batch_id": batch_id,
            "authorized_at": authorized_at,
            "authorization_receipt_sha256": receipt,
            "daily_sol_batch_sha256": daily_sha,
            "status": status,
        }
        queue.append(public_row)
    if queue != sorted(
        queue,
        key=lambda row: (
            row["authorized_at"],
            row["authorization_receipt_sha256"],
            row["batch_id"],
        ),
    ):
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_fifo_invalid"}
    active = value.get("active_writer")
    active_subject = active_batch = current_item = None
    fencing_token = None
    pending_queue = [row for row in queue if row["status"] == "queued"]
    status = "queued" if pending_queue else "idle"
    committed_count = remaining_count = None
    if active is not None:
        if not isinstance(active, Mapping) or active_count != 1 or set(active) != {
            "batch_id",
            "subject",
            "fencing_token",
            "owner_id",
            "claimed_at",
            "authorization_receipt_sha256",
            "daily_sol_batch_sha256",
            "review_receipt_sha256",
            "status",
            "current_item",
            "committed_count",
            "remaining_count",
        }:
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_invalid"}
        active_subject = active.get("subject")
        active_batch = _safe_control_id(active.get("batch_id"))
        current_item = _safe_control_id(active.get("current_item")) if active.get("current_item") is not None else None
        fencing_token = active.get("fencing_token")
        status = _safe_text(active.get("status"), 32)
        committed_count = active.get("committed_count")
        remaining_count = active.get("remaining_count")
        owner_id = _safe_control_id(active.get("owner_id"))
        claimed_at = _safe_text(active.get("claimed_at"), 64)
        active_authorization = _safe_hash(active.get("authorization_receipt_sha256"))
        active_daily_batch = _safe_hash(active.get("daily_sol_batch_sha256"))
        active_review = active.get("review_receipt_sha256")
        if active_review is not None and _safe_hash(active_review) is None:
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_invalid"}
        if (
            active_subject not in SUBJECTS
            or active_batch is None
            or owner_id is None
            or claimed_at is None
            or active_authorization is None
            or active_daily_batch is None
            or isinstance(fencing_token, bool)
            or not isinstance(fencing_token, int)
            or fencing_token < 1
            or status not in {
                "reviewing", "applying", "recovering", "safe_paused", "failed",
            }
            or any(
                isinstance(number, bool) or not isinstance(number, int) or number < 0
                for number in (committed_count, remaining_count)
            )
        ):
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_invalid"}
        matching = next(
            (row for row in queue if row["batch_id"] == active_batch),
            None,
        )
        if (
            matching is None
            or matching["subject"] != active_subject
            or matching["authorization_receipt_sha256"] != active_authorization
            or matching["daily_sol_batch_sha256"] != active_daily_batch
            or matching["status"] not in {"active", "reviewed"}
            or (status == "reviewing" and matching["status"] != "active")
            or (status == "applying" and matching["status"] != "reviewed")
        ):
            return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_queue_binding_invalid"}
    elif active_count != 0:
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_invalid"}
    elif any(row["status"] in {"active", "reviewed"} for row in queue):
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_active_queue_binding_invalid"}
    elif queue and queue[-1]["status"] in {"failed", "safe_paused"}:
        status = queue[-1]["status"]
    updated_at = _safe_text(value.get("updated_at"), 64)
    if updated_at is None:
        return {**_empty_global_sol(), "status": "failed", "error_code": "global_sol_updated_at_missing"}
    return {
        "state_available": True,
        "status": status,
        "active_subject": active_subject,
        "active_batch_id": active_batch,
        "current_item": current_item,
        "authorized_queue": queue,
        "fencing_token": fencing_token,
        "active_writer_count": active_count,
        "committed_count": committed_count,
        "remaining_count": remaining_count,
        "formal_write_count": formal_writes,
        "updated_at": updated_at,
    }


def _empty_en_p0_006() -> dict[str, Any]:
    return {
        "state_available": False,
        "status": "no_data",
        "blocker_codes": [],
        "inventory": {
            "state_available": False,
            "target_count": None,
            "inventory_sha256": None,
            "target_set_sha256": None,
            "batch_authorization_sha256": None,
            "source": "no_data",
        },
        "luna": {
            "state_available": False,
            "status": "no_data",
            "selected_count": None,
            "running_count": None,
            "terminal_count": None,
            "quality_passed_count": None,
            "failed_count": None,
            "remaining_count": None,
            "all_terminal": None,
            "quality_ready": None,
        },
        "sol": {
            "state_available": False,
            "status": "no_data",
            "batch_id": None,
            "current_ordinal": None,
            "current_target_id": None,
            "fencing_token": None,
            "attempt": None,
            "committed_count": None,
            "already_current_count": None,
            "failed_count": None,
            "failure_attempt_count": None,
            "recovery_count": None,
            "recovery_pending_count": None,
            "formal_write_count": None,
            "final_closure_status": None,
            "execution_closure_sha256": None,
            "updated_at": None,
        },
    }


def _read_pinned_json(
    path: Path,
    expected_sha256: str,
    *,
    max_bytes: int,
    error_code: str,
) -> dict[str, Any]:
    try:
        node = path.lstat()
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DashboardProjectionError(error_code) from exc
    if (
        path.is_symlink()
        or not path.is_file()
        or node.st_size <= 0
        or node.st_size > max_bytes
        or hashlib.sha256(raw).hexdigest() != expected_sha256
        or not isinstance(value, dict)
    ):
        raise DashboardProjectionError(error_code)
    return value


def _configured_en_p0_work_item_batch(
    config: Mapping[str, Any],
) -> dict[str, Any] | None:
    dashboard = config.get("dashboard")
    section = (
        dashboard.get("en_p0_006")
        if isinstance(dashboard, Mapping)
        else None
    )
    if section is None:
        return None
    if not isinstance(section, Mapping):
        raise DashboardProjectionError("en_p0_006_dashboard_config_invalid")
    raw_path = section.get("work_item_batch_path")
    digest = _safe_hash(section.get("work_item_batch_sha256"))
    if not isinstance(raw_path, str) or not raw_path or digest is None:
        raise DashboardProjectionError("en_p0_006_dashboard_config_invalid")
    path = Path(raw_path).expanduser().absolute()
    value = _read_pinned_json(
        path,
        digest,
        max_bytes=MAX_EN_P0_BATCH_BYTES,
        error_code="en_p0_006_work_item_batch_invalid",
    )
    required = {
        "schema_version",
        "remediation_gate",
        "authorization_expansion_closure_sha256",
        "batch_authorization_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "authority",
        "target_count",
        "work_items",
        "model_call_count",
        "formal_write_count",
        "created_at",
    }
    gate = value.get("remediation_gate")
    authority = value.get("authority")
    rows = value.get("work_items")
    target_count = value.get("target_count")
    if (
        set(value) != required
        or value.get("schema_version") != EN_P0_WORK_ITEM_BATCH_SCHEMA
        or not isinstance(target_count, int)
        or isinstance(target_count, bool)
        or not 1 <= target_count <= 10000
        or not isinstance(rows, list)
        or len(rows) != target_count
        or value.get("model_call_count") != 0
        or value.get("formal_write_count") != 0
        or not isinstance(gate, Mapping)
        or gate.get("schema_version") != "en_p0_006_remediation_gate_v1"
        or gate.get("issue_id") != "EN-P0-006"
        or gate.get("subject") != "english"
        or gate.get("authorization_gate") != "passed"
        or gate.get("execution_gate") != "pending"
        or gate.get("decision") != "allow_en_p0_006_luna_recuration_only"
        or gate.get("allowed_lanes") != ["en_p0_006_luna_recuration"]
        or gate.get("target_count") != target_count
        or gate.get("inventory_sha256") != value.get("inventory_sha256")
        or gate.get("target_set_sha256") != value.get("target_set_sha256")
        or gate.get("batch_authorization_sha256")
        != value.get("batch_authorization_sha256")
        or gate.get("authorization_expansion_closure_sha256")
        != value.get("authorization_expansion_closure_sha256")
        or gate.get("model_call_count") != 0
        or gate.get("formal_write_count") != 0
        or not isinstance(authority, Mapping)
        or set(authority) != {"generation", "authority_fingerprint"}
        or _safe_control_id(authority.get("generation")) is None
        or _safe_hash(authority.get("authority_fingerprint")) is None
        or any(
            _safe_hash(value.get(field)) is None
            for field in (
                "authorization_expansion_closure_sha256",
                "batch_authorization_sha256",
                "inventory_sha256",
                "target_set_sha256",
            )
        )
        or _safe_text(value.get("created_at"), 64) is None
    ):
        raise DashboardProjectionError("en_p0_006_work_item_batch_invalid")
    work_items: list[dict[str, Any]] = []
    target_ids: set[str] = set()
    work_hashes: set[str] = set()
    for expected_ordinal, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping) or set(raw) != {
            "ordinal", "target_id", "work_item_sha256", "work_item_path",
        }:
            raise DashboardProjectionError("en_p0_006_work_item_batch_invalid")
        target_id = _safe_text(raw.get("target_id"), 240)
        work_sha = _safe_hash(raw.get("work_item_sha256"))
        if (
            raw.get("ordinal") != expected_ordinal
            or target_id is None
            or target_id in target_ids
            or work_sha is None
            or work_sha in work_hashes
            or not isinstance(raw.get("work_item_path"), str)
            or not str(raw.get("work_item_path") or "")
        ):
            raise DashboardProjectionError("en_p0_006_work_item_batch_invalid")
        target_ids.add(target_id)
        work_hashes.add(work_sha)
        work_items.append(
            {
                "ordinal": expected_ordinal,
                "target_id": target_id,
                "work_item_sha256": work_sha,
            }
        )
    return {
        "source": "candidate_bound_work_item_batch",
        "work_item_batch_sha256": digest,
        "authorization_expansion_closure_sha256": str(
            value["authorization_expansion_closure_sha256"]
        ),
        "inventory_sha256": str(value["inventory_sha256"]),
        "target_set_sha256": str(value["target_set_sha256"]),
        "batch_authorization_sha256": str(value["batch_authorization_sha256"]),
        "target_count": target_count,
        "work_items": work_items,
    }


def _verified_en_p0_run_summary(
    config: Mapping[str, Any],
    runtime_root: Path,
    digest: str,
) -> dict[str, Any]:
    """Use the Luna lane's canonical verifier without importing private payloads."""

    try:
        if __package__:
            from .english_legacy_recuration import (
                EnglishLegacyRecurationError,
                verify_run_summary,
            )
        else:
            from english_legacy_recuration import (  # type: ignore[no-redef]
                EnglishLegacyRecurationError,
                verify_run_summary,
            )
    except ImportError as exc:
        raise DashboardProjectionError("en_p0_006_run_summary_verifier_unavailable") from exc
    try:
        value = verify_run_summary(config, runtime_root, digest)
    except (EnglishLegacyRecurationError, OSError, ValueError) as exc:
        raise DashboardProjectionError("en_p0_006_run_summary_invalid") from exc
    if not isinstance(value, dict):
        raise DashboardProjectionError("en_p0_006_run_summary_invalid")
    return value


def _en_p0_luna_state(
    config: Mapping[str, Any],
    runtime_root: Path,
    batch: Mapping[str, Any],
) -> dict[str, Any]:
    base = runtime_root / "dispatch" / "english-legacy-recuration"
    expected = {
        int(row["ordinal"]): {
            "target_id": str(row["target_id"]),
            "work_item_sha256": str(row["work_item_sha256"]),
        }
        for row in batch["work_items"]
    }
    selected: set[int] = set()
    quality: set[int] = set()
    failed: set[int] = set()
    completion_artifacts: dict[int, tuple[str, str]] = {}
    successful_summary_artifacts: dict[int, tuple[str, str]] = {}
    authority_observed = False

    for ordinal, row in expected.items():
        work_sha = row["work_item_sha256"]
        claim_path = base / "attempt-claims" / f"{work_sha}.json"
        binding_path = base / "completion-bindings" / f"{work_sha}.json"
        if claim_path.exists():
            claim = _bounded_object(claim_path, MAX_EN_P0_SAFE_RECEIPT_BYTES)
            if (
                claim is None
                or set(claim) != {
                    "schema_version", "work_item_sha256", "target_id", "attempt",
                    "claimed_at", "model_call_count", "formal_write_count",
                }
                or claim.get("schema_version") != EN_P0_ATTEMPT_CLAIM_SCHEMA
                or claim.get("work_item_sha256") != work_sha
                or claim.get("target_id") != row["target_id"]
                or not isinstance(claim.get("attempt"), int)
                or isinstance(claim.get("attempt"), bool)
                or int(claim.get("attempt") or 0) < 1
                or _safe_text(claim.get("claimed_at"), 64) is None
                or claim.get("model_call_count") != 0
                or claim.get("formal_write_count") != 0
            ):
                raise DashboardProjectionError("en_p0_006_attempt_claim_invalid")
            authority_observed = True
            selected.add(ordinal)
        if not binding_path.exists():
            continue
        authority_observed = True
        binding = _bounded_object(binding_path, MAX_EN_P0_SAFE_RECEIPT_BYTES)
        if (
            binding is None
            or set(binding) != {
                "schema_version", "work_item_sha256", "package_sha256",
                "quality_receipt_sha256", "formal_write_count",
            }
            or binding.get("schema_version") != EN_P0_COMPLETION_BINDING_SCHEMA
            or binding.get("work_item_sha256") != work_sha
            or _safe_hash(binding.get("package_sha256")) is None
            or _safe_hash(binding.get("quality_receipt_sha256")) is None
            or binding.get("formal_write_count") != 0
        ):
            raise DashboardProjectionError("en_p0_006_completion_binding_invalid")
        selected.add(ordinal)
        package_sha = str(binding["package_sha256"])
        package_path = (
            base / "packages" / "sha256" / package_sha[:2] / f"{package_sha}.json"
        )
        try:
            package_node = package_path.lstat()
            package_hash = hashlib.sha256(package_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise DashboardProjectionError(
                "en_p0_006_completion_artifact_invalid"
            ) from exc
        if (
            package_path.is_symlink()
            or not package_path.is_file()
            or package_node.st_size <= 0
            or package_node.st_size > MAX_EN_P0_BATCH_BYTES
            or package_hash != package_sha
        ):
            raise DashboardProjectionError("en_p0_006_completion_artifact_invalid")
        receipt_sha = str(binding["quality_receipt_sha256"])
        receipt_path = (
            base
            / "quality-receipts"
            / "sha256"
            / receipt_sha[:2]
            / f"{receipt_sha}.json"
        )
        try:
            receipt_node = receipt_path.lstat()
            receipt_hash = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise DashboardProjectionError(
                "en_p0_006_quality_receipt_invalid"
            ) from exc
        if (
            receipt_path.is_symlink()
            or not receipt_path.is_file()
            or receipt_node.st_size <= 0
            or receipt_node.st_size > MAX_EN_P0_SAFE_RECEIPT_BYTES
            or receipt_hash != receipt_sha
        ):
            raise DashboardProjectionError("en_p0_006_quality_receipt_invalid")
        completion_artifacts[ordinal] = (package_sha, receipt_sha)

    summary_root = base / "run-summaries" / "sha256"
    summary_paths = (
        sorted(summary_root.glob("*/*.json"))
        if summary_root.is_dir() and not summary_root.is_symlink()
        else []
    )
    if len(summary_paths) > MAX_EN_P0_RUN_SUMMARIES:
        raise DashboardProjectionError("en_p0_006_run_summary_limit_exceeded")
    for path in summary_paths:
        digest = path.stem
        if _safe_hash(digest) is None:
            continue
        candidate = _read_pinned_json(
            path,
            digest,
            max_bytes=MAX_EN_P0_BATCH_BYTES,
            error_code="en_p0_006_run_summary_invalid",
        )
        if candidate.get("work_item_batch_sha256") != batch["work_item_batch_sha256"]:
            continue
        summary = _verified_en_p0_run_summary(config, runtime_root, digest)
        authority_observed = True
        ordinals = summary.get("selected_ordinals")
        results = summary.get("results")
        if (
            summary.get("authorization_expansion_closure_sha256")
            != batch["authorization_expansion_closure_sha256"]
            or summary.get("batch_authorization_sha256")
            != batch["batch_authorization_sha256"]
            or summary.get("inventory_sha256") != batch["inventory_sha256"]
            or summary.get("target_set_sha256") != batch["target_set_sha256"]
            or summary.get("batch_target_count") != batch["target_count"]
            or not isinstance(ordinals, list)
            or not isinstance(results, list)
            or ordinals != sorted(set(ordinals))
            or len(ordinals) != len(results)
            or summary.get("selected_count") != len(ordinals)
            or [row.get("ordinal") for row in results if isinstance(row, Mapping)]
            != ordinals
            or any(ordinal not in expected for ordinal in ordinals)
            or summary.get("formal_write_count") != 0
        ):
            raise DashboardProjectionError("en_p0_006_run_summary_invalid")
        result_counts = {name: 0 for name in ("succeeded", "deduplicated", "failed")}
        for result in results:
            if not isinstance(result, Mapping):
                raise DashboardProjectionError("en_p0_006_run_summary_invalid")
            ordinal = result.get("ordinal")
            if (
                not isinstance(ordinal, int)
                or isinstance(ordinal, bool)
                or result.get("target_id") != expected[ordinal]["target_id"]
                or result.get("status") not in result_counts
                or result.get("formal_write_count") != 0
            ):
                raise DashboardProjectionError("en_p0_006_run_summary_invalid")
            selected.add(ordinal)
            result_counts[str(result["status"])] += 1
            if result["status"] == "failed":
                failed.add(ordinal)
            else:
                receipt_sha = _safe_hash(result.get("quality_receipt_sha256"))
                package_sha = _safe_hash(result.get("package_sha256"))
                artifacts = (
                    (str(package_sha), str(receipt_sha))
                    if package_sha is not None and receipt_sha is not None
                    else None
                )
                if artifacts is None or completion_artifacts.get(ordinal) != artifacts:
                    raise DashboardProjectionError("en_p0_006_run_summary_invalid")
                prior = successful_summary_artifacts.setdefault(ordinal, artifacts)
                if prior != artifacts:
                    raise DashboardProjectionError("en_p0_006_run_summary_conflict")
                quality.add(ordinal)
        if (
            summary.get("succeeded_count") != result_counts["succeeded"]
            or summary.get("deduplicated_count") != result_counts["deduplicated"]
            or summary.get("failed_count") != result_counts["failed"]
        ):
            raise DashboardProjectionError("en_p0_006_run_summary_invalid")
    if quality & failed:
        raise DashboardProjectionError("en_p0_006_luna_terminal_conflict")
    if not authority_observed:
        return dict(_empty_en_p0_006()["luna"])
    terminal = quality | failed
    running = selected - terminal
    target_count = int(batch["target_count"])
    return {
        "state_available": True,
        "status": (
            "quality_ready"
            if len(quality) == target_count and not failed
            else "complete_with_failures"
            if len(terminal) == target_count and failed
            else "running"
            if running
            else "not_started"
        ),
        "selected_count": len(selected),
        "running_count": len(running),
        "terminal_count": len(terminal),
        "quality_passed_count": len(quality),
        "failed_count": len(failed),
        "remaining_count": target_count - len(terminal),
        "all_terminal": len(terminal) == target_count,
        "quality_ready": len(quality) == target_count and not failed,
    }


def _discover_en_p0_sol_state(runtime_root: Path) -> dict[str, Any] | None:
    public_global = _global_sol_state(runtime_root)
    if public_global.get("state_available") is not True:
        if public_global.get("status") == "failed":
            raise DashboardProjectionError("en_p0_006_global_sol_state_invalid")
        return None
    store = SubjectSolRuntimeStore(runtime_root)
    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for entry in public_global.get("authorized_queue", []):
        if not isinstance(entry, Mapping) or entry.get("subject") != "english":
            continue
        digest = _safe_hash(entry.get("daily_sol_batch_sha256"))
        if digest is None:
            continue
        path = (
            store.english_legacy_sol_batch_root
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        value = _bounded_object(path, MAX_EN_P0_BATCH_BYTES)
        if value is None or value.get("schema_version") != "english_legacy_recuration_sol_batch_v1":
            continue
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise DashboardProjectionError("en_p0_006_sol_batch_invalid") from exc
        if hashlib.sha256(raw).hexdigest() != digest or _canonical_bytes(value) != raw:
            raise DashboardProjectionError("en_p0_006_sol_batch_invalid")
        try:
            batch = validate_english_legacy_recuration_sol_batch_v1(value)
            snapshot = store.read_english_legacy_recuration_state(str(batch["batch_id"]))
        except (SubjectSolContractError, OSError, ValueError) as exc:
            raise DashboardProjectionError("en_p0_006_sol_state_invalid") from exc
        candidates.append((batch, snapshot))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise DashboardProjectionError("en_p0_006_sol_batch_ambiguous")
    batch, snapshot = candidates[0]
    state = snapshot["state"]
    rows = state["items"]
    failure_attempt_count = sum(
        attempt.get("failure_receipt_sha256") is not None
        for row in rows
        for attempt in row["attempts"]
    )
    recovery_count = sum(
        attempt.get("recovery_receipt_sha256") is not None
        for row in rows
        for attempt in row["attempts"]
    )
    current_failures = sum(row["status"] == "failed" for row in rows)
    recovery_pending = (
        1 if state["status"] in {"safe_paused", "recovering"} else 0
    )
    closure = None
    closure_sha = _safe_hash(state.get("execution_closure_sha256"))
    if closure_sha is not None:
        try:
            closure = store.reopen_verified_english_legacy_execution_closure(
                str(batch["batch_id"]), closure_sha256=closure_sha
            )
        except (SubjectSolContractError, OSError, ValueError) as exc:
            raise DashboardProjectionError(
                "en_p0_006_execution_closure_invalid"
            ) from exc
        current_failures = int(closure["failed_count"])
        recovery_pending = int(closure["recovery_pending_count"])
    return {
        "batch": {
            "source": "verified_sol_batch",
            "work_item_batch_sha256": None,
            "inventory_sha256": batch["inventory_sha256"],
            "target_set_sha256": batch["target_set_sha256"],
            "batch_authorization_sha256": batch["batch_authorization_sha256"],
            "target_count": batch["target_count"],
            "work_items": [
                {
                    "ordinal": row["ordinal"],
                    "target_id": row["target_id"],
                    "work_item_sha256": row["work_item_sha256"],
                }
                for row in batch["work_items"]
            ],
        },
        "luna": {
            "state_available": True,
            "status": "quality_ready",
            "selected_count": batch["target_count"],
            "running_count": 0,
            "terminal_count": batch["target_count"],
            "quality_passed_count": batch["target_count"],
            "failed_count": 0,
            "remaining_count": 0,
            "all_terminal": True,
            "quality_ready": True,
        },
        "sol": {
            "state_available": True,
            "status": state["status"],
            "batch_id": batch["batch_id"],
            "current_ordinal": state["current_ordinal"],
            "current_target_id": state["current_target_id"],
            "fencing_token": state["fencing_token"] or None,
            "attempt": state["attempt"] or None,
            "committed_count": sum(row["status"] == "committed" for row in rows),
            "already_current_count": sum(
                row["status"] == "already_current" for row in rows
            ),
            "failed_count": current_failures,
            "failure_attempt_count": failure_attempt_count,
            "recovery_count": recovery_count,
            "recovery_pending_count": recovery_pending,
            "formal_write_count": state["formal_write_count"],
            "final_closure_status": closure.get("status") if closure else None,
            "execution_closure_sha256": closure_sha,
            "updated_at": state["updated_at"],
        },
    }


def _en_p0_006_state(
    config: Mapping[str, Any], runtime_root: Path,
) -> dict[str, Any]:
    empty = _empty_en_p0_006()
    try:
        configured_batch = _configured_en_p0_work_item_batch(config)
        sol_snapshot = _discover_en_p0_sol_state(runtime_root)
        sol_batch = sol_snapshot["batch"] if sol_snapshot is not None else None
        if configured_batch is not None and sol_batch is not None:
            for field in (
                "inventory_sha256", "target_set_sha256",
                "batch_authorization_sha256", "target_count",
            ):
                if configured_batch[field] != sol_batch[field]:
                    raise DashboardProjectionError("en_p0_006_sol_batch_binding_invalid")
            if [
                (row["ordinal"], row["target_id"], row["work_item_sha256"])
                for row in configured_batch["work_items"]
            ] != [
                (row["ordinal"], row["target_id"], row["work_item_sha256"])
                for row in sol_batch["work_items"]
            ]:
                raise DashboardProjectionError("en_p0_006_sol_batch_binding_invalid")
        batch = configured_batch or sol_batch
        if batch is None:
            return empty
        luna = (
            _en_p0_luna_state(config, runtime_root, batch)
            if configured_batch is not None
            else dict(sol_snapshot["luna"])
        )
        sol = (
            dict(sol_snapshot["sol"])
            if sol_snapshot is not None
            else dict(empty["sol"])
        )
        inventory = {
            "state_available": True,
            "target_count": batch["target_count"],
            "inventory_sha256": batch["inventory_sha256"],
            "target_set_sha256": batch["target_set_sha256"],
            "batch_authorization_sha256": batch["batch_authorization_sha256"],
            "source": batch["source"],
        }
        closure_status = sol.get("final_closure_status")
        status = (
            "verified_complete"
            if closure_status == "verified_complete"
            else "complete_with_failures"
            if closure_status == "complete_with_failures"
            else "safe_paused"
            if sol.get("status") == "safe_paused"
            else "recovering"
            if sol.get("status") == "recovering"
            else "formal_applying"
            if sol.get("status") == "active"
            else "queued_for_sol"
            if sol.get("status") == "queued"
            else "quality_ready"
            if luna.get("quality_ready") is True
            else "luna_complete_with_failures"
            if luna.get("all_terminal") is True
            and int(luna.get("failed_count") or 0) > 0
            else "luna_running"
            if int(luna.get("selected_count") or 0) > 0
            else "authorization_ready"
        )
        return {
            "state_available": True,
            "status": status,
            "blocker_codes": [],
            "inventory": inventory,
            "luna": luna,
            "sol": sol,
        }
    except DashboardProjectionError as exc:
        return {
            **empty,
            "status": "invalid",
            "blocker_codes": [str(exc)],
        }


def _dispatcher_status(subject_projection: Mapping[str, Any] | None) -> str:
    if subject_projection is None:
        return "starting"
    daemon_status = str(subject_projection.get("daemon_status") or "")
    if daemon_status == "drained":
        return "drained"
    if daemon_status in {"disabled", "paused", "stopped"}:
        return "paused"
    if subject_projection.get("processing_error_code") or subject_projection.get("error_code"):
        return "failed"
    return "running"


def _legacy_projection_item(
    runtime_root: Path,
    raw: Mapping[str, Any],
    *,
    subject: str,
    study_date: str,
    release_id: str,
    source_projection_sha256: str,
) -> dict[str, Any] | None:
    """Convert one v1 row only after verifying its persisted publication."""

    capture_id = _safe_text(raw.get("capture_id"), 160)
    if (
        capture_id is None
        or SAFE_ID.fullmatch(capture_id) is None
        or raw.get("subject") != subject
        or raw.get("study_date") != study_date
    ):
        return None
    fingerprint = _safe_text(raw.get("input_fingerprint"), 64)
    package_sha256 = _safe_text(raw.get("package_sha256"), 64)
    valid_hashes = all(
        isinstance(value, str) and SHA256.fullmatch(value)
        for value in (fingerprint, package_sha256)
    )
    job = _bounded_object(
        runtime_root / "state" / "jobs" / subject / f"{capture_id}.json",
        2 * 1024 * 1024,
    )
    latest = _bounded_object(
        runtime_root / "state" / "latest" / subject / f"{capture_id}.json",
        2 * 1024 * 1024,
    )
    package_path: Path | None = None
    if isinstance(job, Mapping) and isinstance(job.get("package_path"), str):
        package_path = Path(str(job["package_path"])).resolve(strict=False)
    publication_verified = bool(
        valid_hashes
        and isinstance(job, Mapping)
        and isinstance(latest, Mapping)
        and job.get("subject") == subject
        and latest.get("subject") == subject
        and job.get("capture_id") == capture_id
        and latest.get("capture_id") == capture_id
        and job.get("study_date") == study_date
        and latest.get("study_date") == study_date
        and job.get("input_fingerprint") == fingerprint
        and latest.get("input_fingerprint") == fingerprint
        and job.get("package_sha256") == package_sha256
        and latest.get("package_sha256") == package_sha256
        and package_path is not None
        and isinstance(latest.get("package_path"), str)
        and Path(str(latest.get("package_path"))).resolve(strict=False)
        == package_path
    )
    if publication_verified:
        try:
            package_path.relative_to(runtime_root)
            publication_verified = bool(
                package_path.is_file()
                and not package_path.is_symlink()
                and hashlib.sha256(package_path.read_bytes()).hexdigest()
                == package_sha256
            )
        except (ValueError, OSError):
            publication_verified = False

    identity = {
        "schema_version": "study-intake-dashboard-legacy-row-identity-v1",
        "subject": subject,
        "capture_id": capture_id,
        "study_date": study_date,
        "input_fingerprint": fingerprint,
        "package_sha256": package_sha256,
        "source_projection_sha256": source_projection_sha256,
    }
    unit_sha256 = hashlib.sha256(_canonical_bytes(identity)).hexdigest()
    frozen_sha256 = hashlib.sha256(
        _canonical_bytes({**identity, "kind": "legacy_projection_migration"})
    ).hexdigest()
    legacy_status = str(
        raw.get("task_status") or raw.get("luna_status") or ""
    )
    if publication_verified and legacy_status in {"ready", "two_pass_ready"}:
        queue_state, current_stage, luna_status = (
            "ready",
            "quality_ready",
            legacy_status,
        )
    elif publication_verified and legacy_status in {"processing", "running"}:
        queue_state, current_stage, luna_status = (
            "running",
            str(raw.get("stage") or "analysis"),
            "processing",
        )
    elif publication_verified and legacy_status in {"queued", "retrying"}:
        queue_state, current_stage, luna_status = (
            "queued",
            "frozen_evidence",
            legacy_status,
        )
    elif publication_verified and legacy_status == "failed":
        queue_state, current_stage, luna_status = "failed", "failed", "failed"
    else:
        queue_state, current_stage, luna_status = "stale", "stale", "stale"
    runtime_status = "requested_unverified"
    item: dict[str, Any] = {
        "capture_id": capture_id,
        "subject": subject,
        "study_date": study_date,
        "updated_at": _safe_text(raw.get("updated_at"), 64)
        or "1970-01-01T00:00:00+00:00",
        "target_label": _safe_text(raw.get("target_label"), 160)
        or capture_id,
        "queue_state": queue_state,
        "current_stage": current_stage,
        "luna_status": luna_status,
        "unit_sha256": unit_sha256,
        "input_fingerprint": fingerprint or f"sha256:{frozen_sha256}",
        "frozen_payload_sha256": frozen_sha256,
        "release_id": release_id,
        "rule_version": "study-intake-dashboard-legacy-v1-migration-v1",
        "server_queue_status": "unknown",
        "server_queue_confirmation": "unconfirmed",
        "requested_model": REQUIRED_MODEL,
        "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
        "runtime_identity_status": runtime_status,
        "local_model_submitted": legacy_status not in {"", "queued"},
        "projection_origin": "legacy_v1_migration",
    }
    for key in (
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "quality_receipt_sha256",
    ):
        value = raw.get(key)
        if isinstance(value, str) and SHA256.fullmatch(value):
            item[key] = value
    if not publication_verified:
        item["last_error_code"] = "legacy_projection_binding_mismatch"
    elif queue_state == "failed":
        error = _safe_text(raw.get("last_error_code"), 160)
        item["last_error_code"] = (
            error
            if error is not None and SAFE_CODE.fullmatch(error)
            else "legacy_processing_failed"
        )
    return item


def _migrate_legacy_archive(
    config: Mapping[str, Any],
    runtime_root: Path,
    archive_path: Path,
    *,
    study_date: str,
    timestamp: str,
) -> Path | None:
    try:
        raw_bytes = archive_path.read_bytes()
        legacy = json.loads(raw_bytes.decode("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(legacy, Mapping)
        or legacy.get("schema_version")
        != "study-intake-dashboard-projection-v1"
        or legacy.get("study_date") != study_date
    ):
        return None
    source_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    migration_root = runtime_root / "dispatch" / "state" / "dashboard-migrations"
    backup_path = (
        migration_root
        / "backups"
        / "sha256"
        / source_sha256[:2]
        / f"{source_sha256}.json"
    )
    if backup_path.exists():
        if backup_path.read_bytes() != raw_bytes:
            raise DashboardProjectionError("dashboard_migration_backup_conflict")
    else:
        _atomic_bytes(backup_path, raw_bytes)
    release_id = _configured_release_id(config, ())
    legacy_subjects = legacy.get("subjects")
    if not isinstance(legacy_subjects, Mapping):
        legacy_subjects = {}
    for subject in SUBJECTS:
        raw_subject = legacy_subjects.get(subject)
        raw_items = (
            raw_subject.get("items", [])
            if isinstance(raw_subject, Mapping)
            and isinstance(raw_subject.get("items"), list)
            else []
        )
        migrated_items = [
            migrated
            for raw in raw_items
            if isinstance(raw, Mapping)
            for migrated in [
                _legacy_projection_item(
                    runtime_root,
                    raw,
                    subject=subject,
                    study_date=study_date,
                    release_id=release_id,
                    source_projection_sha256=source_sha256,
                )
            ]
            if migrated is not None
        ]
        date_path = _subject_projection_path(runtime_root, subject, study_date)
        previous = _bounded_object(date_path, MAX_SUBJECT_BYTES)
        if previous is None:
            previous = {
                "schema_version": SUBJECT_SCHEMA,
                "subject": subject,
                "study_date": study_date,
                "daemon_status": "historical",
                "release_id": release_id,
                "decisions": [],
                "updated_at": timestamp,
                "formal_write_count": 0,
            }
        projection = dict(previous)
        projection["legacy_items"] = migrated_items
        _atomic_json(date_path, projection)
    receipt_path = migration_root / "receipts" / f"{study_date}.json"
    receipt = {
        "schema_version": "study-intake-dashboard-projection-migration-receipt-v1",
        "study_date": study_date,
        "source_schema_version": "study-intake-dashboard-projection-v1",
        "source_projection_path": str(archive_path),
        "source_projection_sha256": source_sha256,
        "backup_path": str(backup_path),
        "target_schema_version": _configured_dashboard_schema(config),
        "release_id": release_id,
        "migrated_at": timestamp,
        "formal_write_count": 0,
    }
    _atomic_json(receipt_path, receipt)
    return receipt_path


def restore_dashboard_projection_migration(
    runtime_root: Path, study_date: str
) -> dict[str, Any]:
    """Restore the exact pre-migration archive from its content backup."""

    root = runtime_root.expanduser().resolve()
    migration_root = root / "dispatch" / "state" / "dashboard-migrations"
    receipt_path = migration_root / "receipts" / f"{study_date}.json"
    receipt = _bounded_object(receipt_path, MAX_INDEX_BYTES)
    if (
        receipt is None
        or receipt.get("schema_version")
        != "study-intake-dashboard-projection-migration-receipt-v1"
        or receipt.get("study_date") != study_date
        or receipt.get("formal_write_count") != 0
    ):
        raise DashboardProjectionError("dashboard_migration_receipt_invalid")
    target_path = Path(str(receipt.get("source_projection_path") or "")).resolve(
        strict=False
    )
    expected_target = _archive_projection_path(root, study_date).resolve(
        strict=False
    )
    backup_path = Path(str(receipt.get("backup_path") or "")).resolve(
        strict=False
    )
    source_sha256 = receipt.get("source_projection_sha256")
    expected_backup = (
        migration_root
        / "backups"
        / "sha256"
        / str(source_sha256)[:2]
        / f"{source_sha256}.json"
    ).resolve(strict=False)
    if (
        target_path != expected_target
        or backup_path != expected_backup
        or not isinstance(source_sha256, str)
        or SHA256.fullmatch(source_sha256) is None
    ):
        raise DashboardProjectionError("dashboard_migration_binding_invalid")
    try:
        backup_raw = backup_path.read_bytes()
    except OSError as exc:
        raise DashboardProjectionError("dashboard_migration_backup_missing") from exc
    if hashlib.sha256(backup_raw).hexdigest() != source_sha256:
        raise DashboardProjectionError("dashboard_migration_backup_hash_mismatch")
    previous_target_sha256 = (
        hashlib.sha256(target_path.read_bytes()).hexdigest()
        if target_path.is_file()
        else None
    )
    _atomic_bytes(target_path, backup_raw)
    rollback = {
        "schema_version": "study-intake-dashboard-projection-migration-rollback-v1",
        "study_date": study_date,
        "migration_receipt_path": str(receipt_path),
        "migration_receipt_sha256": hashlib.sha256(
            receipt_path.read_bytes()
        ).hexdigest(),
        "previous_target_sha256": previous_target_sha256,
        "restored_projection_sha256": source_sha256,
        "restored_at": _utc_now(),
        "formal_write_count": 0,
    }
    rollback_path = migration_root / "rollbacks" / f"{study_date}.json"
    _atomic_json(rollback_path, rollback)
    return rollback


def _main_projection(
    config: Mapping[str, Any],
    runtime_root: Path,
    *,
    study_date: str,
    generated_at: str,
) -> dict[str, Any]:
    subject_values: dict[str, Mapping[str, Any] | None] = {}
    heartbeat_values: dict[str, Mapping[str, Any] | None] = {}
    all_decisions: list[Mapping[str, Any]] = []
    for subject in SUBJECTS:
        global_path = (
            runtime_root
            / "dispatch"
            / "state"
            / "subject-projections"
            / f"{subject}.json"
        )
        heartbeat = _bounded_object(global_path, MAX_SUBJECT_BYTES)
        if heartbeat is not None and (
            heartbeat.get("schema_version") != SUBJECT_SCHEMA
            or heartbeat.get("subject") != subject
        ):
            heartbeat = None
        date_value = _bounded_object(
            _subject_projection_path(runtime_root, subject, study_date),
            MAX_SUBJECT_BYTES,
        )
        if date_value is not None and (
            date_value.get("schema_version") != SUBJECT_SCHEMA
            or date_value.get("subject") != subject
            or date_value.get("study_date") != study_date
        ):
            date_value = None
        if (
            date_value is None
            and heartbeat is not None
            and heartbeat.get("study_date") == study_date
        ):
            date_value = heartbeat
        heartbeat_values[subject] = heartbeat
        subject_values[subject] = date_value
        if date_value is not None and isinstance(date_value.get("decisions"), list):
            all_decisions.extend(
                row
                for row in date_value["decisions"]
                if isinstance(row, Mapping)
            )
    configured_release = _configured_release_id(config, all_decisions)
    shared_concurrency_telemetry = _latest_concurrency_telemetry(
        heartbeat_values,
        release_id=configured_release,
    )
    global_sol = _global_sol_state(runtime_root)
    en_p0_006 = _en_p0_006_state(config, runtime_root)
    dispatchers: dict[str, dict[str, Any]] = {}
    subjects: dict[str, dict[str, Any]] = {}
    projection_schema = _configured_dashboard_schema(config)
    configured_continuous_limit = _configured_continuous_concurrency_limit(config)
    for subject in SUBJECTS:
        value = subject_values[subject]
        heartbeat_value = heartbeat_values[subject]
        heartbeat_release = (
            heartbeat_value.get("release_id")
            if heartbeat_value is not None
            else None
        )
        if (
            not isinstance(heartbeat_release, str)
            or SHA256.fullmatch(heartbeat_release) is None
        ):
            heartbeat_release = configured_release
        heartbeat_generation_status = (
            "current_release"
            if heartbeat_release == configured_release
            else "previous_release_heartbeat"
        )
        heartbeat_is_current_release = (
            heartbeat_generation_status == "current_release"
        )
        canary_gate = _public_canary_gate(
            heartbeat_value.get("canary_gate")
            if heartbeat_value is not None and heartbeat_is_current_release
            else None,
            subject=subject,
            release_id=configured_release,
        )
        decisions = (
            [row for row in value.get("decisions", []) if isinstance(row, Mapping)]
            if value is not None
            else []
        )
        decision_items = [
            item
            for decision in decisions
            if decision.get("study_date") == study_date
            for item in [
                _item_from_decision(
                    runtime_root,
                    decision,
                    subject=subject,
                    generated_at=generated_at,
                )
            ]
            if item is not None
        ]
        legacy_items = (
            [
                dict(row)
                for row in value.get("legacy_items", [])
                if isinstance(row, Mapping)
                and row.get("subject") == subject
                and row.get("study_date") == study_date
                and isinstance(row.get("capture_id"), str)
            ]
            if value is not None and isinstance(value.get("legacy_items"), list)
            else []
        )
        by_capture = {
            str(item["capture_id"]): item
            for item in legacy_items
            if isinstance(item.get("capture_id"), str)
        }
        for item in decision_items:
            by_capture[str(item["capture_id"])] = item
        if projection_schema in MODERN_MAIN_SCHEMAS:
            for item in _preserved_preclaim_failures(
                runtime_root,
                subject=subject,
                release_id=configured_release,
                canary_gate=canary_gate,
                study_date=study_date,
                generated_at=generated_at,
            ):
                # The content-addressed queue task and its preserved terminal
                # receipt outrank later producer replays for the same capture.
                by_capture[str(item["capture_id"])] = item
        items = list(by_capture.values())
        for item in items:
            _ensure_task_axes(item)
        batch = _subject_batch_state(runtime_root, subject, study_date)
        latest_batch = _subject_batch_state(runtime_root, subject, None)
        blocking_batch: dict[str, Any] | None = None
        batch_blockers: list[str] = []
        if (
            batch is None
            and latest_batch is not None
            and latest_batch.get("study_date") != study_date
        ):
            historical_sol = _subject_sol_state(
                runtime_root,
                subject,
                str(latest_batch["batch_id"]),
                len(latest_batch["tasks"]),
            )
            writer_bound = bool(
                historical_sol is not None
                and historical_sol.get("batch_id") == latest_batch.get("batch_id")
            )
            blocker_code = (
                "cross_date_batch_writer_bound"
                if writer_bound
                else "cross_date_batch_still_current"
            )
            batch_blockers.append(blocker_code)
            blocking_batch = {
                "batch_id": str(latest_batch["batch_id"]),
                "study_date": str(latest_batch["study_date"]),
                "status": str(latest_batch["status"]),
                "all_terminal": latest_batch["declared_all_terminal"] is True,
                "sol_ready": latest_batch["declared_sol_ready"] is True,
                "writer_bound": writer_bound,
                "writer_handoff_status": (
                    str(historical_sol["handoff_status"])
                    if historical_sol is not None
                    else "unknown"
                ),
                "blocker_code": blocker_code,
            }
        batch_all_terminal: bool | None = None
        batch_sol_ready: bool | None = None
        batch_partition = {
            "current_batch_task_count": 0,
            "current_batch_terminal_task_count": 0,
            "current_batch_failed_task_count": 0,
            "current_batch_sol_candidate_task_count": 0,
            "current_batch_diagnostic_task_count": 0,
            "outside_batch_pending_task_count": 0,
            "outside_batch_failed_preserved_task_count": 0,
        }
        if batch is not None:
            batch_tasks = {
                str(task["capture_id"]): task for task in batch["tasks"]
            }
            item_ids = {str(item.get("capture_id")) for item in items}
            if not set(batch_tasks).issubset(item_ids):
                batch_blockers.append("batch_task_missing_from_projection")
            for item in items:
                task = batch_tasks.get(str(item.get("capture_id")))
                if task is None:
                    continue
                if (
                    item.get("unit_sha256") != task.get("unit_sha256")
                    or item.get("input_fingerprint")
                    != task.get("input_fingerprint")
                    or item.get("study_date") != task.get("study_date")
                    or item.get("frozen_payload_sha256")
                    != task.get("frozen_payload_sha256")
                ):
                    batch_blockers.append("batch_task_identity_mismatch")
                    continue
                _apply_batch_task_state(item, task)
            is_v2_batch = batch.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA
            terminal_states = (
                SUBJECT_BATCH_V2_TERMINAL_STATES
                if is_v2_batch
                else {
                    "quality_passed",
                    "needs_rework",
                    "failed",
                    "evidence_pending",
                }
            )
            computed_all_terminal = bool(batch_tasks) and all(
                task["status"] in terminal_states
                for task in batch_tasks.values()
            )
            if is_v2_batch:
                sol_candidate_ids = sorted(
                    task["capture_id"]
                    for task in batch_tasks.values()
                    if task["status"] in SUBJECT_BATCH_V2_CANDIDATE_STATES
                    and _safe_hash(task.get("package_sha256")) is not None
                    and _safe_hash(task.get("sol_handoff_envelope_sha256"))
                    is not None
                )
                diagnostic_ids = sorted(
                    task["capture_id"]
                    for task in batch_tasks.values()
                    if task["status"]
                    in (
                        SUBJECT_BATCH_V2_TERMINAL_STATES
                        - SUBJECT_BATCH_V2_CANDIDATE_STATES
                    )
                    or task["status"] in SUBJECT_BATCH_V2_CANDIDATE_STATES
                    and _safe_hash(task.get("sol_handoff_envelope_sha256"))
                    is None
                )
                computed_sol_ready = bool(
                    computed_all_terminal and sol_candidate_ids
                )
                if sol_candidate_ids != batch.get("sol_candidate_task_ids"):
                    batch_blockers.append("batch_sol_candidate_set_mismatch")
                if diagnostic_ids != batch.get("diagnostic_task_ids"):
                    batch_blockers.append("batch_diagnostic_set_mismatch")
            else:
                sol_candidate_ids = sorted(
                    task["capture_id"]
                    for task in batch_tasks.values()
                    if task["status"] == "quality_passed"
                    and task.get("quality_receipt_verified") is True
                    and _safe_hash(task.get("package_sha256")) is not None
                    and _safe_hash(task.get("quality_receipt_sha256")) is not None
                )
                diagnostic_ids = sorted(
                    task["capture_id"]
                    for task in batch_tasks.values()
                    if task["status"] in {"needs_rework", "failed", "evidence_pending"}
                )
                computed_sol_ready = bool(batch_tasks) and len(sol_candidate_ids) == len(
                    batch_tasks
                )
            if batch["declared_all_terminal"] != computed_all_terminal:
                batch_blockers.append("batch_all_terminal_mismatch")
            if not is_v2_batch and any(
                task["status"] == "quality_passed"
                and task.get("quality_receipt_verified") is not True
                for task in batch_tasks.values()
            ):
                batch_blockers.append("quality_receipt_authority_invalid")
            if batch["declared_sol_ready"] != (
                computed_sol_ready and batch["status"] == "frozen"
            ):
                batch_blockers.append("batch_sol_ready_mismatch")
            batch_blockers.extend(
                f"blocking_task:{capture_id}"
                for capture_id in batch["blocking_task_ids"]
            )
            batch_all_terminal = (
                computed_all_terminal
                and batch["declared_all_terminal"] is True
                and "batch_task_missing_from_projection" not in batch_blockers
            )
            batch_sol_ready = bool(
                batch_all_terminal
                and computed_sol_ready
                and batch["declared_sol_ready"] is True
                and batch["status"] == "frozen"
                and not batch_blockers
            )
            outside_pending = sum(
                1
                for item in items
                if str(item.get("capture_id")) not in batch_tasks
                and item.get("local_dispatch_status")
                in {"pending", "pending_consumer_paused", "retrying"}
                and item.get("terminal_status") is None
                and item.get("queue_preserved_for_recovery") is not True
            )
            batch_partition = {
                "current_batch_task_count": len(batch_tasks),
                "current_batch_terminal_task_count": sum(
                    task["status"] in terminal_states
                    for task in batch_tasks.values()
                ),
                "current_batch_failed_task_count": len(diagnostic_ids),
                "current_batch_sol_candidate_task_count": len(sol_candidate_ids),
                "current_batch_diagnostic_task_count": len(diagnostic_ids),
                "outside_batch_pending_task_count": outside_pending,
                "outside_batch_failed_preserved_task_count": sum(
                    1
                    for item in items
                    if str(item.get("capture_id")) not in batch_tasks
                    and item.get("queue_preserved_for_recovery") is True
                    and item.get("terminal_status") == "failed"
                ),
            }
        else:
            batch_partition["outside_batch_pending_task_count"] = sum(
                item.get("local_dispatch_status")
                in {"pending", "pending_consumer_paused", "retrying"}
                for item in items
                if item.get("terminal_status") is None
                and item.get("queue_preserved_for_recovery") is not True
            )
            batch_partition["outside_batch_failed_preserved_task_count"] = sum(
                item.get("queue_preserved_for_recovery") is True
                and item.get("terminal_status") == "failed"
                for item in items
            )
        if subject == "english":
            for item in items:
                _apply_english_capture_authority(runtime_root, item)
        if projection_schema in MODERN_MAIN_SCHEMAS:
            for item in items:
                if item.get("server_queue_status") == "rate_limited":
                    item["server_queue_status"] = "confirmed_rate_limited"
        if projection_schema == PREVIOUS_MAIN_SCHEMA:
            for item in items:
                item.pop("report_available", None)
                item.pop("formal_write_eligible", None)
        elif projection_schema == LEGACY_MAIN_SCHEMA:
            for item in items:
                for field in DASHBOARD_V5_ITEM_FIELDS:
                    item.pop(field, None)
        items.sort(
            key=lambda item: (str(item.get("updated_at", "")), item["capture_id"]),
            reverse=True,
        )
        heartbeat = (
            _safe_text(heartbeat_value.get("updated_at"), 64)
            if heartbeat_value is not None
            else None
        ) or "1970-01-01T00:00:00+00:00"
        stage = _current_stage(items)
        daemon_status = str(
            heartbeat_value.get("daemon_status")
            if heartbeat_value is not None
            else ""
        )
        concurrency_projection = _subject_concurrency_projection(
            (
                heartbeat_value.get("concurrency_observation")
                if heartbeat_value is not None and heartbeat_is_current_release
                else None
            ),
            canary_gate,
            shared_concurrency_telemetry,
            subject=subject,
            release_id=configured_release,
        )
        if canary_gate is not None:
            dispatcher_enabled = canary_gate["luna_consumer_enabled"] is True
            control_state = (
                "drained"
                if canary_gate["state"] == "inactive_rolled_back"
                else "paused"
                if canary_gate["state"] in {"failed_drained", "paused_drained"}
                else "running"
            )
        else:
            dispatcher_enabled = daemon_status not in {
                "disabled",
                "paused",
                "drained",
                "stopped",
                "historical",
            }
            control_state = (
                "drained"
                if daemon_status == "drained"
                else "paused"
                if not dispatcher_enabled
                else "draining"
                if heartbeat_value is not None
                and heartbeat_value.get("draining") is True
                else "running"
            )
        dispatchers[subject] = {
            "status": (
                "failed"
                if canary_gate is not None
                and canary_gate["state"] == "failed_drained"
                else "drained"
                if canary_gate is not None
                and canary_gate["state"] == "inactive_rolled_back"
                else "paused"
                if canary_gate is not None
                and canary_gate["state"] == "paused_drained"
                else "running"
                if canary_gate is not None
                else _dispatcher_status(heartbeat_value)
            ),
            "last_heartbeat": heartbeat,
            "release_id": configured_release,
            "active_release_id": configured_release,
            "heartbeat_release_id": heartbeat_release,
            "heartbeat_generation_status": heartbeat_generation_status,
            "previous_release_heartbeat": (
                heartbeat_generation_status == "previous_release_heartbeat"
            ),
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "current_stage": stage,
            "enabled": dispatcher_enabled,
            "paused": control_state in {"paused", "drained", "draining"},
            "control_state": control_state,
            "control_reason": (
                _safe_text(heartbeat_value.get("control_reason"), 160)
                if heartbeat_value is not None
                else None
            )
            or daemon_status
            or "projection_missing",
            "canary_gate": canary_gate,
            **_processing_component_identity(config, subject),
        }
        processing_error_code = (
            heartbeat_value.get("processing_error_code")
            if heartbeat_value is not None
            else None
        ) or (
            heartbeat_value.get("error_code")
            if heartbeat_value is not None
            else None
        )
        subjects[subject] = {
            "enabled": dispatcher_enabled,
            "status": (
                "source_error"
                if processing_error_code
                else "active" if dispatcher_enabled else "disabled"
            ),
            "control_state": control_state,
            "control_reason": dispatchers[subject]["control_reason"],
            "current_stage": stage,
            "counts": _counts(items),
            "items": items,
            "metrics": {},
            "metric_sources": {},
            "batch_state_available": batch is not None,
            "batch_id": batch.get("batch_id") if batch is not None else None,
            "batch_status": batch.get("status") if batch is not None else "no_data",
            **(
                {"blocking_batch": blocking_batch}
                if projection_schema in MODERN_MAIN_SCHEMAS
                else {}
            ),
            "capture_high_watermark": (
                batch.get("capture_high_watermark") if batch is not None else None
            ),
            "scan_snapshot_sha256": (
                batch.get("scan_snapshot_sha256") if batch is not None else None
            ),
            "authority_generation": (
                batch.get("authority_generation") if batch is not None else None
            ),
            "all_terminal": batch_all_terminal,
            "sol_ready": batch_sol_ready,
            **(
                {"batch_partition": batch_partition}
                if projection_schema in MODERN_MAIN_SCHEMAS
                else {}
            ),
            **(
                {
                    "capacity": _capacity_projection(
                        canary_gate,
                        configured_continuous_limit=configured_continuous_limit,
                    )
                }
                if projection_schema in MODERN_MAIN_SCHEMAS
                else {}
            ),
            "blockers": sorted(set(batch_blockers)),
            "sol_handoff_status": "unknown",
            "sol_reviewed_count": None,
            "sol_committed_count": None,
            "canary_gate": canary_gate,
            **concurrency_projection,
        }
        batch_id = batch.get("batch_id") if batch is not None else None
        subject_sol = _subject_sol_state(
            runtime_root,
            subject,
            batch_id,
            len(batch["tasks"]) if batch is not None else 0,
        )
        if subject_sol is not None:
            subjects[subject]["sol_reviewed_count"] = subject_sol[
                "sol_reviewed_count"
            ]
            subjects[subject]["sol_committed_count"] = subject_sol[
                "sol_committed_count"
            ]
        if global_sol.get("state_available") is True:
            active_for_subject = (
                global_sol.get("active_subject") == subject
                and global_sol.get("active_batch_id") == batch_id
            )
            queued_for_subject = next(
                (
                    row
                    for row in global_sol.get("authorized_queue", [])
                    if row.get("subject") == subject
                    and row.get("batch_id") == batch_id
                ),
                None,
            )
            if active_for_subject:
                subjects[subject]["sol_handoff_status"] = {
                    "reviewing": "sol_reviewing",
                    "applying": "formal_applying",
                    "safe_paused": "safe_paused",
                    "failed": "safe_paused",
                }.get(str(global_sol.get("status")), "queued_for_sol")
                if batch is not None:
                    if global_sol.get("status") == "applying":
                        subjects[subject]["sol_reviewed_count"] = len(batch["tasks"])
                    committed = global_sol.get("committed_count")
                    if isinstance(committed, int) and not isinstance(committed, bool):
                        subjects[subject]["sol_committed_count"] = min(
                            committed,
                            len(batch["tasks"]),
                        )
            elif queued_for_subject is not None:
                queue_status = queued_for_subject.get("status")
                subjects[subject]["sol_handoff_status"] = {
                    "queued": "queued_for_sol",
                    "committed": "committed",
                    "failed": "safe_paused",
                    "safe_paused": "safe_paused",
                }.get(str(queue_status), "queued_for_sol")
            elif (
                subject_sol is not None
                and batch is not None
                and subject_sol["sol_committed_count"] == len(batch["tasks"])
                and len(batch["tasks"]) > 0
            ):
                subjects[subject]["sol_handoff_status"] = "committed"
            elif batch_sol_ready is True:
                subjects[subject]["sol_handoff_status"] = "awaiting_authorization"
            elif batch is not None:
                subjects[subject]["sol_handoff_status"] = "not_ready"
        if subject_sol is not None and global_sol.get("active_subject") != subject:
            subjects[subject]["sol_handoff_status"] = {
                "awaiting_luna": "not_ready",
                "ready_for_authorization": "awaiting_authorization",
                "queued_for_sol": "queued_for_sol",
                "sol_reviewing": "sol_reviewing",
                "sol_reviewed": "formal_applying",
                "complete": "committed",
                "safe_paused": "safe_paused",
                "failed": "safe_paused",
            }.get(
                str(subject_sol.get("handoff_status")),
                subjects[subject]["sol_handoff_status"],
            )
        if (
            isinstance(processing_error_code, str)
            and SAFE_CODE.fullmatch(processing_error_code)
        ):
            subjects[subject]["processing_error_code"] = processing_error_code
            subjects[subject]["error_code"] = processing_error_code
    concurrency = _global_concurrency_projection(
        subjects,
        shared_concurrency_telemetry,
        release_id=configured_release,
        generated_at=generated_at,
    )
    return {
        "schema_version": projection_schema,
        **(
            {
                "configured_global_continuous_concurrency_limit": (
                    configured_continuous_limit * len(SUBJECTS)
                )
            }
            if projection_schema in MODERN_MAIN_SCHEMAS
            else {}
        ),
        "generated_at": generated_at,
        "study_date": study_date,
        "en_p0_006_status": en_p0_006["status"],
        "en_p0_006": en_p0_006,
        "dispatchers": dispatchers,
        "subjects": subjects,
        "concurrency": concurrency,
        "global_sol": global_sol,
    }


def update_subject_and_main_projection(
    config: Mapping[str, Any],
    subject: str,
    *,
    study_date: str,
    daemon_status: str,
    eligible_count: int,
    submitted_count: int,
    decisions: Sequence[Mapping[str, Any]],
    lease_status: Mapping[str, Any],
    error_code: str | None = None,
    control_reason: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    if subject not in SUBJECTS:
        raise DashboardProjectionError("dashboard_projection_subject_invalid")
    runtime_root = Path(str(config["runtime_root"])).expanduser().resolve()
    timestamp = updated_at or _utc_now()
    dashboard = config.get("dashboard")
    if isinstance(dashboard, Mapping):
        raw_main_path = dashboard.get("projection_path")
    else:
        raw_main_path = None
    main_path = (
        Path(str(raw_main_path)).expanduser().absolute()
        if isinstance(raw_main_path, str) and raw_main_path
        else runtime_root / "state" / "dashboard_projection.json"
    )
    heartbeat_path = (
        runtime_root
        / "dispatch"
        / "state"
        / "subject-projections"
        / f"{subject}.json"
    )
    subject_path = _subject_projection_path(
        runtime_root, subject, study_date
    )
    archive_path = _archive_projection_path(runtime_root, study_date)
    lock_path = runtime_root / "dispatch" / "state" / "dashboard-projection.lock"
    with _short_lock(lock_path):
        migration_receipt_path = _migrate_legacy_archive(
            config,
            runtime_root,
            archive_path,
            study_date=study_date,
            timestamp=timestamp,
        )
        previous = _bounded_object(subject_path, MAX_SUBJECT_BYTES)
        previous_decisions = (
            previous.get("decisions", [])
            if previous is not None
            and previous.get("schema_version") == SUBJECT_SCHEMA
            and previous.get("subject") == subject
            and isinstance(previous.get("decisions"), list)
            else []
        )
        merged = _merge_decisions(
            previous_decisions,
            decisions,
            subject=subject,
            study_date=study_date,
            updated_at=timestamp,
        )
        release_id = _configured_release_id(config, merged)
        concurrency_observation = _lease_concurrency_observation(
            lease_status,
            subject=subject,
            release_id=release_id,
        )
        subject_projection = {
            "schema_version": SUBJECT_SCHEMA,
            "subject": subject,
            "study_date": study_date,
            "daemon_status": daemon_status,
            "eligible_count": eligible_count,
            "submitted_count": submitted_count,
            "active_count": concurrency_observation.get("active_count"),
            "stale_count": concurrency_observation.get("stale_count"),
            "claimed_total": concurrency_observation.get("claimed_total"),
            "completed_count": concurrency_observation.get("completed_count"),
            "retry_wait_count": concurrency_observation.get("retry_wait_count"),
            "max_fence": concurrency_observation.get("max_fence"),
            "draining": lease_status.get("draining", False),
            "heartbeat_interval_seconds": lease_status.get("heartbeat_interval_seconds", 15),
            "lease_ttl_seconds": lease_status.get("lease_ttl_seconds", 120),
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "release_id": release_id,
            "decisions": merged,
            "error_code": error_code,
            "processing_error_code": error_code,
            "control_state": (
                "drained"
                if daemon_status == "drained"
                else "paused"
                if daemon_status in {"disabled", "paused", "stopped"}
                else "draining"
                if lease_status.get("draining") is True
                else "running"
            ),
            "control_reason": control_reason or daemon_status,
            "updated_at": timestamp,
            "formal_write_count": 0,
            "concurrency_observation": concurrency_observation,
        }
        canary_gate = _public_canary_gate(
            lease_status.get("canary_gate"),
            subject=subject,
            release_id=release_id,
        )
        subject_projection["canary_gate"] = canary_gate
        if previous is not None and isinstance(
            previous.get("legacy_items"), list
        ):
            subject_projection["legacy_items"] = previous["legacy_items"]
        _atomic_json(subject_path, subject_projection)
        _atomic_json(heartbeat_path, subject_projection)
        main = _main_projection(
            config,
            runtime_root,
            study_date=study_date,
            generated_at=timestamp,
        )
        _atomic_json(archive_path, main)
        existing_main = _bounded_object(main_path, 8 * 1024 * 1024)
        timezone = str(config.get("timezone") or "UTC")
        try:
            today = dt.datetime.now(ZoneInfo(timezone)).date().isoformat()
        except (ValueError, KeyError):
            today = dt.datetime.now(dt.timezone.utc).date().isoformat()
        if (
            existing_main is None
            or existing_main.get("study_date") == study_date
            or study_date == today
        ):
            _atomic_json(main_path, main)
        if migration_receipt_path is not None:
            receipt = _bounded_object(migration_receipt_path, MAX_INDEX_BYTES)
            if receipt is None:
                raise DashboardProjectionError(
                    "dashboard_migration_receipt_missing"
                )
            receipt["target_projection_path"] = str(archive_path)
            receipt["target_projection_sha256"] = hashlib.sha256(
                archive_path.read_bytes()
            ).hexdigest()
            _atomic_json(migration_receipt_path, receipt)
    return main
