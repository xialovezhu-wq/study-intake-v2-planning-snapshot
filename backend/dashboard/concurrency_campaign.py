"""Validated public projection for the three-subject Luna concurrency canary.

The projection is deliberately separate from the normal queue projection.  A
canary receipt can therefore be absent or fail closed without weakening the
Dashboard's three-dispatcher readiness checks.
"""

from __future__ import annotations

import copy
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


LEGACY_SCHEMA_VERSION = "study-intake-three-subject-concurrency-campaign-v1"
SCHEMA_VERSION = "study-intake-three-subject-concurrency-campaign-v2"
MAX_BYTES = 512 * 1024
MAX_AGE_SECONDS = 7 * 24 * 60 * 60
FUTURE_SKEW_SECONDS = 300
SUBJECTS = ("math", "cs408", "english")
SUBJECT_TIMEOUTS = {"math": 3600, "cs408": 1800, "english": 1800}
SUBJECT_MCP = {
    "math": "kaoyan_math_read",
    "cs408": "kaoyan_cs408_read",
    "english": "kaoyan_english_read",
}
CAMPAIGN_STATUSES = {
    "preflight",
    "single_subject_acceptance",
    "concurrent_running",
    "production_canary_active",
    "passed",
    "failed_closed",
    "activation_ready",
    "activated",
}
STAGES = {
    "not_started",
    "analysis_submitted",
    "analysis_completed",
    "critical_review_submitted",
    "critical_review_completed",
    "report_verified",
    "failed",
}
STAGE_STATUSES = {"not_started", "running", "completed", "failed"}
FAST_MODE_STATUSES = {"requested_unverified", "confirmed"}
CANARY_SLOT_STATES = {
    "armed",
    "claimed",
    "running",
    "succeeded",
    "failed_paused",
    "unlocked",
}
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,199}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
REPORT_JSON_REF = re.compile(
    r"^study-intake-report://sha256/[0-9a-f]{64}$"
)
REPORT_MARKDOWN_REF = re.compile(
    r"^study-intake-report-markdown://sha256/[0-9a-f]{64}$"
)
PACKAGE_REF = re.compile(
    r"^study-intake-dispatch-package://sha256/[0-9a-f]{64}$"
)
PRIVATE_LOCATION = re.compile(
    r"(?:file://|(?<![A-Za-z0-9:/])/(?!/)[^\s\"'()<>{}\[\],;，。；]+|"
    r"(?<![A-Za-z0-9])(?:~|\.\.)[/\\]|[A-Za-z]:[/\\]|\\\\[^\s\\/]+[/\\])",
    re.IGNORECASE,
)

TOP_LEVEL_KEYS = {
    "schema_version",
    "generated_at",
    "campaign_id",
    "release_id",
    "status",
    "result_label",
    "production_accepted",
    "barrier",
    "fast_mode",
    "safety",
    "canary",
    "subjects",
}
BARRIER_KEYS = {
    "expected",
    "arrived",
    "global_peak_active",
    "per_subject_peak_active",
    "submitted_at_spread_ms",
    "released_at",
    "completed_at",
}
FAST_MODE_KEYS = {"requested", "service_tier", "effective_status"}
SAFETY_KEYS = {
    "provider_execution",
    "model_call_count",
    "provider_request_count",
    "formal_write_count",
    "sol_enabled",
}
CANARY_KEYS = {
    "activation_id",
    "activated_at",
    "post_activation_only",
    "historical_backlog_drained",
    "per_subject_limit",
    "slots",
}
CANARY_SLOT_KEYS = {
    "subject",
    "producer_high_watermark_sha256",
    "state",
    "capture_id",
    "completion_receipt_sha256",
}
SUBJECT_KEYS = {
    "subject",
    "capture_id",
    "stage",
    "stage_timeout_seconds",
    "mcp_namespace",
    "mcp_canonical_call_count",
    "analysis_status",
    "critical_review_status",
    "report_status",
    "report_ref",
    "report_sha256",
    "package_ref",
    "package_sha256",
    "model_call_count",
    "provider_request_count",
    "error_code",
    "started_at",
    "completed_at",
}

V2_EVIDENCE_SCOPES = {
    "real_production_hmac_v2",
    "zero_model_fixture_v2",
}
V2_STATUSES = {
    "production_canary_active",
    "production_canary_active_with_subject_failure",
    "production_verified",
    "test_evidence_only",
}
V2_RESULT_LABELS = {
    "production_canary_pending",
    "production_canary_partially_verified",
    "production_canary_subject_failure",
    "production_runtime_verified",
    "zero_model_fixture_evidence_only",
}
V2_SUBJECT_STATUSES = {
    "awaiting_first_capture",
    "running",
    "verified",
    "failed_paused",
    "paused",
    "inactive",
}
V2_RUNTIME_STATES = {
    "armed",
    "canary_in_flight",
    "continuous_concurrent_unlocked",
    "failed_drained",
    "paused_drained",
    "inactive_rolled_back",
}
V2_PROVIDER_EXECUTION_STATUSES = {
    "verified_no_fast_mode_argv_environment",
    "zero_model_fixture_not_applicable",
    "not_verified",
}
V2_PROVIDER_STAGE_KEYS = {"analysis", "critical_review"}
V2_TOP_LEVEL_KEYS = {
    "schema_version",
    "evidence_scope",
    "generated_at",
    "campaign_id",
    "release_id",
    "current_release_usable",
    "status",
    "result_label",
    "production_accepted",
    "model_request_contract",
    "safety",
    "canary",
    "concurrency",
    "subjects",
}
V2_MODEL_REQUEST_KEYS = {
    "schema_version",
    "model",
    "reasoning_effort",
    "service_tier_policy",
    "requested_service_tier",
    "fast_mode_requested",
    "fast_mode_effective",
}
V2_SAFETY_KEYS = {
    "production_evidence",
    "provider_execution",
    "model_call_count",
    "provider_request_count",
    "mcp_tool_call_count",
    "runtime_observed_model_call_count",
    "runtime_observed_provider_request_count",
    "runtime_observed_mcp_tool_call_count",
    "formal_write_count",
    "sol_enabled",
}
V2_CANARY_KEYS = {
    "global_activation_id",
    "activated_at",
    "post_activation_only",
    "historical_backlog_drained",
    "initial_canary_inflight_limit",
    "continuous_concurrency_limit",
    "asynchronous_subject_canary",
    "slots",
}
V2_SLOT_KEYS = {
    "subject",
    "activation_id",
    "producer_high_watermark_sha256",
    "runtime_state",
    "verification_status",
    "capture_id",
    "terminal_receipt_sha256",
}
V2_CONCURRENCY_KEYS = {
    "calculation_source",
    "interval_policy",
    "terminal_task_count",
    "active_task_count",
    "lifecycle_task_count",
    "runner_evidenced_task_count",
    "runner_interval_missing_count",
    "global_peak_active",
    "subject_peak_active",
    "overlap_observed",
    "overlap_subjects",
    "terminal_index_sha256_by_subject",
    "failure_bindings_by_subject",
    "telemetry_cross_check",
}
V2_FAILURE_BINDING_KEYS = {
    "unit_sha256",
    "outcome",
    "error_code",
    "terminal_kind",
    "terminal_receipt_sha256",
    "late_result_fenced",
}
V2_SUBJECT_KEYS = {
    "subject",
    "status",
    "runtime_state",
    "capture_id",
    "unit_sha256",
    "terminal_outcome",
    "terminal_kind",
    "error_code",
    "stage",
    "stage_timeout_seconds",
    "mcp_namespace",
    "mcp_canonical_call_count",
    "analysis_status",
    "critical_review_status",
    "report_status",
    "report_json_ref",
    "report_json_sha256",
    "report_markdown_ref",
    "report_markdown_sha256",
    "report_reopen_status",
    "package_status",
    "package_ref",
    "package_sha256",
    "completion_sha256",
    "processing_receipt_sha256",
    "model_call_count",
    "provider_request_count",
    "provider_execution_contract_status",
    "provider_stage_identity_sha256s",
    "provider_stage_exit_sha256s",
    "provider_executable_sha256",
    "task_runner_executable_sha256",
    "runtime_observed_model_call_count",
    "runtime_observed_provider_request_count",
    "runtime_observed_mcp_tool_call_count",
    "terminal_receipt_sha256",
    "terminal_index_sha256",
    "runner_process_identity_sha256",
    "runner_process_exit_sha256",
    "runner_started_at",
    "runner_finished_at",
    "read_session_id",
    "evidence_generation",
    "evidence_authority_fingerprint",
    "evidence_refs",
    "late_result_fenced",
    "started_at",
    "completed_at",
}


@dataclass(frozen=True)
class CampaignResult:
    available: bool
    projection: dict[str, Any] | None = None
    error: str | None = None
    error_code: str | None = None
    mtime_ns: int | None = None


def _timestamp(value: object, *, nullable: bool = False) -> bool:
    if value is None:
        return nullable
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _parsed_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _safe_id(value: object, *, nullable: bool = False) -> bool:
    if value is None:
        return nullable
    return isinstance(value, str) and SAFE_ID.fullmatch(value) is not None


def _sha(value: object, *, nullable: bool = False) -> bool:
    if value is None:
        return nullable
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def _content_ref(
    value: object, pattern: re.Pattern[str], *, nullable: bool = False
) -> bool:
    if value is None:
        return nullable
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _contains_private_location(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_private_location(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_private_location(child) for child in value)
    if isinstance(value, str) and any(
        pattern.fullmatch(value) is not None
        for pattern in (REPORT_JSON_REF, REPORT_MARKDOWN_REF, PACKAGE_REF)
    ):
        return False
    return isinstance(value, str) and PRIVATE_LOCATION.search(value) is not None


def _campaign_v1_contract_error(payload: Mapping[str, Any]) -> str | None:
    if set(payload) != TOP_LEVEL_KEYS:
        return "campaign_topology_invalid"
    if payload.get("schema_version") != LEGACY_SCHEMA_VERSION:
        return "campaign_schema_mismatch"
    if not _timestamp(payload.get("generated_at")):
        return "campaign_generated_at_invalid"
    if not _safe_id(payload.get("campaign_id")) or not _sha(payload.get("release_id")):
        return "campaign_identity_invalid"
    status = payload.get("status")
    if status not in CAMPAIGN_STATUSES:
        return "campaign_status_invalid"
    if payload.get("production_accepted") is not False:
        return "campaign_production_acceptance_forbidden"
    result_label = payload.get("result_label")
    if result_label not in {
        "three_subject_concurrency_not_yet_accepted",
        "three_subject_concurrency_runtime_accepted",
        "three_subject_concurrency_failed_closed",
    }:
        return "campaign_result_label_invalid"
    if status in {"passed", "activation_ready", "activated"}:
        if result_label != "three_subject_concurrency_runtime_accepted":
            return "campaign_result_label_invalid"
    elif status == "failed_closed":
        if result_label != "three_subject_concurrency_failed_closed":
            return "campaign_result_label_invalid"
    elif result_label != "three_subject_concurrency_not_yet_accepted":
        return "campaign_result_label_invalid"

    barrier = payload.get("barrier")
    if not isinstance(barrier, Mapping) or set(barrier) != BARRIER_KEYS:
        return "campaign_barrier_invalid"
    peaks = barrier.get("per_subject_peak_active")
    if (
        barrier.get("expected") != 3
        or not _non_negative_int(barrier.get("arrived"))
        or int(barrier.get("arrived") or 0) > 3
        or not _non_negative_int(barrier.get("global_peak_active"))
        or not isinstance(peaks, Mapping)
        or set(peaks) != set(SUBJECTS)
        or any(not _non_negative_int(peaks.get(subject)) for subject in SUBJECTS)
        or (
            barrier.get("submitted_at_spread_ms") is not None
            and not _non_negative_int(barrier.get("submitted_at_spread_ms"))
        )
        or not _timestamp(barrier.get("released_at"), nullable=True)
        or not _timestamp(barrier.get("completed_at"), nullable=True)
    ):
        return "campaign_barrier_invalid"

    fast_mode = payload.get("fast_mode")
    if (
        not isinstance(fast_mode, Mapping)
        or set(fast_mode) != FAST_MODE_KEYS
        or fast_mode.get("requested") is not True
        or fast_mode.get("service_tier") != "priority"
        or fast_mode.get("effective_status") not in FAST_MODE_STATUSES
    ):
        return "campaign_fast_mode_invalid"

    safety = payload.get("safety")
    if not isinstance(safety, Mapping) or set(safety) != SAFETY_KEYS:
        return "campaign_safety_invalid"

    canary = payload.get("canary")
    if (
        not isinstance(canary, Mapping)
        or set(canary) != CANARY_KEYS
        or not _safe_id(canary.get("activation_id"))
        or not _timestamp(canary.get("activated_at"))
        or canary.get("post_activation_only") is not True
        or canary.get("historical_backlog_drained") is not True
        or canary.get("per_subject_limit") != 1
        or not isinstance(canary.get("slots"), Mapping)
        or set(canary["slots"]) != set(SUBJECTS)
    ):
        return "campaign_canary_invalid"
    slots = canary["slots"]
    for subject in SUBJECTS:
        slot = slots.get(subject)
        if (
            not isinstance(slot, Mapping)
            or set(slot) != CANARY_SLOT_KEYS
            or slot.get("subject") != subject
            or not _sha(slot.get("producer_high_watermark_sha256"))
            or slot.get("state") not in CANARY_SLOT_STATES
            or not _safe_id(slot.get("capture_id"), nullable=True)
            or not _sha(slot.get("completion_receipt_sha256"), nullable=True)
        ):
            return "campaign_canary_slot_invalid"
        state = slot["state"]
        if state == "armed" and (
            slot.get("capture_id") is not None
            or slot.get("completion_receipt_sha256") is not None
        ):
            return "campaign_canary_slot_invalid"
        if state in {"claimed", "running"} and (
            slot.get("capture_id") is None
            or slot.get("completion_receipt_sha256") is not None
        ):
            return "campaign_canary_slot_invalid"
        if state in {"succeeded", "failed_paused", "unlocked"} and (
            slot.get("capture_id") is None
            or slot.get("completion_receipt_sha256") is None
        ):
            return "campaign_canary_slot_invalid"
    if (
        safety.get("formal_write_count") != 0
        or safety.get("sol_enabled") is not False
        or not _non_negative_int(safety.get("model_call_count"))
        or not _non_negative_int(safety.get("provider_request_count"))
        or not isinstance(safety.get("provider_execution"), bool)
        or safety.get("provider_execution")
        is not (int(safety.get("provider_request_count") or 0) > 0)
    ):
        return "campaign_safety_invalid"

    subjects = payload.get("subjects")
    if not isinstance(subjects, Mapping) or set(subjects) != set(SUBJECTS):
        return "campaign_subject_topology_invalid"
    total_models = 0
    total_providers = 0
    for subject in SUBJECTS:
        row = subjects.get(subject)
        if not isinstance(row, Mapping) or set(row) != SUBJECT_KEYS:
            return "campaign_subject_invalid"
        if (
            row.get("subject") != subject
            or not _safe_id(row.get("capture_id"), nullable=True)
            or row.get("stage") not in STAGES
            or row.get("stage_timeout_seconds") != SUBJECT_TIMEOUTS[subject]
            or row.get("mcp_namespace") != SUBJECT_MCP[subject]
            or not _non_negative_int(row.get("mcp_canonical_call_count"))
            or row.get("analysis_status") not in STAGE_STATUSES
            or row.get("critical_review_status") not in STAGE_STATUSES
            or row.get("report_status") not in {
                "not_started",
                "pending",
                "verified",
                "failed",
            }
            or not _safe_id(row.get("report_ref"), nullable=True)
            or not _sha(row.get("report_sha256"), nullable=True)
            or not _safe_id(row.get("package_ref"), nullable=True)
            or not _sha(row.get("package_sha256"), nullable=True)
            or not _non_negative_int(row.get("model_call_count"))
            or not _non_negative_int(row.get("provider_request_count"))
            or not _safe_id(row.get("error_code"), nullable=True)
            or not _timestamp(row.get("started_at"), nullable=True)
            or not _timestamp(row.get("completed_at"), nullable=True)
        ):
            return "campaign_subject_invalid"
        if (row.get("report_ref") is None) is not (row.get("report_sha256") is None):
            return "campaign_subject_report_binding_invalid"
        if (row.get("package_ref") is None) is not (row.get("package_sha256") is None):
            return "campaign_subject_package_binding_invalid"
        slot = slots[subject]
        if slot.get("capture_id") != row.get("capture_id"):
            return "campaign_canary_capture_binding_invalid"
        slot_state = slot.get("state")
        if slot_state == "armed" and (
            row.get("stage") != "not_started"
            or row.get("analysis_status") != "not_started"
            or row.get("critical_review_status") != "not_started"
            or row.get("report_status") != "not_started"
            or row.get("mcp_canonical_call_count") != 0
            or row.get("model_call_count") != 0
            or row.get("provider_request_count") != 0
        ):
            return "campaign_canary_armed_subject_invalid"
        if slot_state in {"succeeded", "unlocked"} and (
            row.get("stage") != "report_verified"
            or row.get("mcp_canonical_call_count", 0) <= 0
            or row.get("analysis_status") != "completed"
            or row.get("critical_review_status") != "completed"
            or row.get("report_status") != "verified"
            or row.get("report_ref") is None
            or row.get("package_ref") is None
            or row.get("model_call_count", 0) < 2
            or row.get("provider_request_count", 0) <= 0
            or row.get("error_code") is not None
        ):
            return "campaign_canary_completed_subject_invalid"
        if slot_state == "failed_paused" and (
            row.get("stage") != "failed" or row.get("error_code") is None
        ):
            return "campaign_canary_failed_subject_invalid"
        total_models += int(row["model_call_count"])
        total_providers += int(row["provider_request_count"])

    if (
        total_models != safety.get("model_call_count")
        or total_providers != safety.get("provider_request_count")
    ):
        return "campaign_counter_mismatch"

    passed = status in {"passed", "activation_ready", "activated"}
    if passed:
        if (
            barrier.get("arrived") != 3
            or int(barrier.get("global_peak_active") or 0) < 3
            or any(int(peaks.get(subject) or 0) < 1 for subject in SUBJECTS)
            or barrier.get("submitted_at_spread_ms") is None
            or int(barrier.get("submitted_at_spread_ms") or 0) > 2000
            or barrier.get("released_at") is None
            or barrier.get("completed_at") is None
            or safety.get("model_call_count", 0) < 6
            or safety.get("provider_request_count", 0) < 6
        ):
            return "campaign_acceptance_metrics_invalid"
        if any(
            slots[subject].get("state") not in {"succeeded", "unlocked"}
            for subject in SUBJECTS
        ):
            return "campaign_canary_acceptance_invalid"
        for subject in SUBJECTS:
            row = subjects[subject]
            if (
                row.get("capture_id") is None
                or row.get("stage") != "report_verified"
                or row.get("mcp_canonical_call_count", 0) <= 0
                or row.get("analysis_status") != "completed"
                or row.get("critical_review_status") != "completed"
                or row.get("report_status") != "verified"
                or row.get("report_ref") is None
                or row.get("package_ref") is None
                or row.get("model_call_count", 0) < 2
                or row.get("provider_request_count", 0) <= 0
                or row.get("error_code") is not None
                or row.get("started_at") is None
                or row.get("completed_at") is None
            ):
                return "campaign_subject_acceptance_invalid"
    if _contains_private_location(payload):
        return "campaign_private_location_forbidden"
    return None


def _campaign_v2_contract_error(payload: Mapping[str, Any]) -> str | None:
    if set(payload) != V2_TOP_LEVEL_KEYS:
        return "campaign_v2_topology_invalid"
    if payload.get("schema_version") != SCHEMA_VERSION:
        return "campaign_schema_mismatch"
    scope = payload.get("evidence_scope")
    if scope not in V2_EVIDENCE_SCOPES:
        return "campaign_evidence_scope_invalid"
    if not _timestamp(payload.get("generated_at")):
        return "campaign_generated_at_invalid"
    if not _sha(payload.get("campaign_id")) or not _sha(payload.get("release_id")):
        return "campaign_identity_invalid"
    if payload.get("production_accepted") is not False:
        return "campaign_production_acceptance_forbidden"

    real_scope = scope == "real_production_hmac_v2"
    if payload.get("current_release_usable") is not real_scope:
        return "campaign_current_release_scope_invalid"
    status = payload.get("status")
    result_label = payload.get("result_label")
    if status not in V2_STATUSES or result_label not in V2_RESULT_LABELS:
        return "campaign_status_invalid"
    if not real_scope:
        if (
            status != "test_evidence_only"
            or result_label != "zero_model_fixture_evidence_only"
        ):
            return "campaign_fixture_scope_masquerade"

    request = payload.get("model_request_contract")
    if (
        not isinstance(request, Mapping)
        or set(request) != V2_MODEL_REQUEST_KEYS
        or request.get("schema_version")
        != "study-intake-model-request-contract-v2"
        or request.get("model") != "gpt-5.6-luna"
        or request.get("reasoning_effort") != "max"
        or request.get("service_tier_policy") != "absent"
        or "requested_service_tier" not in request
        or request.get("requested_service_tier") is not None
        or request.get("fast_mode_requested") is not False
        or request.get("fast_mode_effective") != "not_requested"
    ):
        return "campaign_model_request_contract_invalid"

    safety = payload.get("safety")
    if not isinstance(safety, Mapping) or set(safety) != V2_SAFETY_KEYS:
        return "campaign_safety_invalid"
    counter_keys = (
        "model_call_count",
        "provider_request_count",
        "mcp_tool_call_count",
        "runtime_observed_model_call_count",
        "runtime_observed_provider_request_count",
        "runtime_observed_mcp_tool_call_count",
    )
    if (
        safety.get("production_evidence") is not real_scope
        or any(not _non_negative_int(safety.get(key)) for key in counter_keys)
        or safety.get("formal_write_count") != 0
        or safety.get("sol_enabled") is not False
        or not isinstance(safety.get("provider_execution"), bool)
        or safety.get("provider_execution")
        is not (int(safety.get("provider_request_count") or 0) > 0)
    ):
        return "campaign_safety_invalid"
    if not real_scope and any(
        safety.get(key) != 0
        for key in (
            "model_call_count",
            "provider_request_count",
            "mcp_tool_call_count",
        )
    ):
        return "campaign_fixture_actual_call_forbidden"

    canary = payload.get("canary")
    if (
        not isinstance(canary, Mapping)
        or set(canary) != V2_CANARY_KEYS
        or not _sha(canary.get("global_activation_id"))
        or not _timestamp(canary.get("activated_at"))
        or canary.get("post_activation_only") is not True
        or canary.get("historical_backlog_drained") is not True
        or canary.get("initial_canary_inflight_limit") != 1
        or canary.get("continuous_concurrency_limit") != 20
        or canary.get("asynchronous_subject_canary") is not True
        or not isinstance(canary.get("slots"), Mapping)
        or set(canary["slots"]) != set(SUBJECTS)
    ):
        return "campaign_canary_invalid"

    subjects = payload.get("subjects")
    if not isinstance(subjects, Mapping) or set(subjects) != set(SUBJECTS):
        return "campaign_subject_topology_invalid"

    actual_models = 0
    actual_providers = 0
    actual_mcp = 0
    observed_models = 0
    observed_providers = 0
    observed_mcp = 0
    subject_statuses: list[str] = []
    for subject in SUBJECTS:
        slot = canary["slots"].get(subject)
        row = subjects.get(subject)
        if (
            not isinstance(slot, Mapping)
            or set(slot) != V2_SLOT_KEYS
            or slot.get("subject") != subject
            or not _sha(slot.get("activation_id"))
            or not _sha(slot.get("producer_high_watermark_sha256"))
            or slot.get("runtime_state") not in V2_RUNTIME_STATES
            or slot.get("verification_status") not in V2_SUBJECT_STATUSES
            or not _safe_id(slot.get("capture_id"), nullable=True)
            or not _sha(slot.get("terminal_receipt_sha256"), nullable=True)
        ):
            return "campaign_canary_slot_invalid"
        if not isinstance(row, Mapping) or set(row) != V2_SUBJECT_KEYS:
            return "campaign_subject_invalid"
        nullable_sha_fields = (
            "unit_sha256",
            "report_json_sha256",
            "report_markdown_sha256",
            "package_sha256",
            "completion_sha256",
            "processing_receipt_sha256",
            "terminal_receipt_sha256",
            "terminal_index_sha256",
            "runner_process_identity_sha256",
            "runner_process_exit_sha256",
            "evidence_authority_fingerprint",
            "provider_executable_sha256",
            "task_runner_executable_sha256",
        )
        nullable_id_fields = (
            "capture_id",
            "terminal_outcome",
            "terminal_kind",
            "error_code",
            "read_session_id",
            "evidence_generation",
        )
        nullable_time_fields = (
            "runner_started_at",
            "runner_finished_at",
            "started_at",
            "completed_at",
        )
        row_counter_keys = (
            "mcp_canonical_call_count",
            "model_call_count",
            "provider_request_count",
            "runtime_observed_model_call_count",
            "runtime_observed_provider_request_count",
            "runtime_observed_mcp_tool_call_count",
        )
        refs = row.get("evidence_refs")
        provider_identity_sha256s = row.get(
            "provider_stage_identity_sha256s"
        )
        provider_exit_sha256s = row.get("provider_stage_exit_sha256s")
        if (
            row.get("subject") != subject
            or row.get("status") not in V2_SUBJECT_STATUSES
            or row.get("runtime_state") != slot.get("runtime_state")
            or row.get("status") != slot.get("verification_status")
            or any(not _sha(row.get(key), nullable=True) for key in nullable_sha_fields)
            or any(not _safe_id(row.get(key), nullable=True) for key in nullable_id_fields)
            or any(not _timestamp(row.get(key), nullable=True) for key in nullable_time_fields)
            or row.get("stage_timeout_seconds") != SUBJECT_TIMEOUTS[subject]
            or row.get("mcp_namespace") != SUBJECT_MCP[subject]
            or any(not _non_negative_int(row.get(key)) for key in row_counter_keys)
            or row.get("analysis_status")
            not in {"not_started", "running", "completed", "failed"}
            or row.get("critical_review_status")
            not in {"not_started", "running", "completed", "failed"}
            or row.get("report_status")
            not in {"not_started", "reopen_verified", "failed"}
            or row.get("report_reopen_status")
            not in {"not_available", "json_markdown_package_verified"}
            or row.get("package_status")
            not in {"not_started", "reopen_verified", "failed"}
            or not _content_ref(
                row.get("report_json_ref"), REPORT_JSON_REF, nullable=True
            )
            or not _content_ref(
                row.get("report_markdown_ref"),
                REPORT_MARKDOWN_REF,
                nullable=True,
            )
            or not _content_ref(
                row.get("package_ref"), PACKAGE_REF, nullable=True
            )
            or row.get("provider_execution_contract_status")
            not in V2_PROVIDER_EXECUTION_STATUSES
            or not isinstance(provider_identity_sha256s, Mapping)
            or set(provider_identity_sha256s) != V2_PROVIDER_STAGE_KEYS
            or any(
                not _sha(provider_identity_sha256s.get(stage), nullable=True)
                for stage in V2_PROVIDER_STAGE_KEYS
            )
            or not isinstance(provider_exit_sha256s, Mapping)
            or set(provider_exit_sha256s) != V2_PROVIDER_STAGE_KEYS
            or any(
                not _sha(provider_exit_sha256s.get(stage), nullable=True)
                for stage in V2_PROVIDER_STAGE_KEYS
            )
            or not isinstance(row.get("late_result_fenced"), bool)
            or not isinstance(refs, list)
            or refs != sorted(set(refs))
            or any(not _safe_id(ref) for ref in refs)
            or row.get("capture_id") != slot.get("capture_id")
            or row.get("terminal_receipt_sha256")
            != slot.get("terminal_receipt_sha256")
        ):
            return "campaign_subject_invalid"
        if (row.get("report_json_ref") is None) is not (
            row.get("report_json_sha256") is None
        ):
            return "campaign_subject_report_binding_invalid"
        if (row.get("report_markdown_ref") is None) is not (
            row.get("report_markdown_sha256") is None
        ):
            return "campaign_subject_report_binding_invalid"
        if (row.get("package_ref") is None) is not (row.get("package_sha256") is None):
            return "campaign_subject_package_binding_invalid"
        for ref_name, sha_name, prefix in (
            (
                "report_json_ref",
                "report_json_sha256",
                "study-intake-report://sha256/",
            ),
            (
                "report_markdown_ref",
                "report_markdown_sha256",
                "study-intake-report-markdown://sha256/",
            ),
            (
                "package_ref",
                "package_sha256",
                "study-intake-dispatch-package://sha256/",
            ),
        ):
            digest = row.get(sha_name)
            if digest is not None and row.get(ref_name) != prefix + str(digest):
                return "campaign_subject_content_address_binding_invalid"
        if (row.get("runner_process_identity_sha256") is None) is not (
            row.get("runner_process_exit_sha256") is None
        ):
            return "campaign_runner_closure_invalid"
        if (row.get("runner_started_at") is None) is not (
            row.get("runner_finished_at") is None
        ):
            return "campaign_runner_closure_invalid"
        if not real_scope and any(
            row.get(key) != 0
            for key in (
                "mcp_canonical_call_count",
                "model_call_count",
                "provider_request_count",
            )
        ):
            return "campaign_fixture_actual_call_forbidden"
        if not real_scope and (
            row.get("provider_execution_contract_status")
            != "zero_model_fixture_not_applicable"
            or any(
                provider_identity_sha256s.get(stage) is not None
                or provider_exit_sha256s.get(stage) is not None
                for stage in V2_PROVIDER_STAGE_KEYS
            )
            or row.get("provider_executable_sha256") is not None
            or row.get("task_runner_executable_sha256") is not None
        ):
            return "campaign_fixture_provider_contract_invalid"
        if real_scope and any(
            row.get(actual) != row.get(observed)
            for actual, observed in (
                (
                    "mcp_canonical_call_count",
                    "runtime_observed_mcp_tool_call_count",
                ),
                ("model_call_count", "runtime_observed_model_call_count"),
                (
                    "provider_request_count",
                    "runtime_observed_provider_request_count",
                ),
            )
        ):
            return "campaign_subject_counter_scope_invalid"

        subject_status = str(row["status"])
        subject_statuses.append(subject_status)
        runtime_state = row["runtime_state"]
        if subject_status == "awaiting_first_capture":
            if (
                runtime_state != "armed"
                or any(
                    row.get(key) is not None
                    for key in (
                        "capture_id",
                        "unit_sha256",
                        "terminal_outcome",
                        "terminal_receipt_sha256",
                    )
                )
                or row.get("stage") != "not_started"
                or row.get("package_status") != "not_started"
                or row.get("report_status") != "not_started"
                or row.get("report_reopen_status") != "not_available"
            ):
                return "campaign_subject_pending_invalid"
        elif subject_status == "running":
            if (
                runtime_state
                not in {"canary_in_flight", "continuous_concurrent_unlocked"}
                or row.get("capture_id") is None
                or row.get("unit_sha256") is None
                or row.get("terminal_receipt_sha256") is not None
                or row.get("stage") != "running"
            ):
                return "campaign_subject_running_invalid"
        elif subject_status == "verified":
            runner_started = _parsed_timestamp(row.get("runner_started_at"))
            runner_finished = _parsed_timestamp(row.get("runner_finished_at"))
            if (
                runtime_state != "continuous_concurrent_unlocked"
                or row.get("capture_id") is None
                or row.get("unit_sha256") is None
                or row.get("terminal_outcome") != "succeeded"
                or row.get("terminal_kind") is None
                or row.get("error_code") is not None
                or row.get("terminal_receipt_sha256") is None
                or row.get("terminal_index_sha256") is None
                or row.get("completion_sha256") is None
                or row.get("processing_receipt_sha256") is None
                or row.get("package_status") != "reopen_verified"
                or row.get("package_ref") is None
                or row.get("report_status") != "reopen_verified"
                or row.get("report_reopen_status")
                != "json_markdown_package_verified"
                or row.get("report_json_ref") is None
                or row.get("report_json_sha256") is None
                or row.get("report_markdown_ref") is None
                or row.get("report_markdown_sha256") is None
                or row.get("analysis_status") != "completed"
                or row.get("critical_review_status") != "completed"
                or int(row.get("runtime_observed_model_call_count") or 0) < 2
                or int(row.get("runtime_observed_provider_request_count") or 0)
                < 2
                or int(row.get("runtime_observed_mcp_tool_call_count") or 0) < 2
                or row.get("runner_process_identity_sha256") is None
                or row.get("runner_process_exit_sha256") is None
                or (
                    real_scope
                    and (
                        row.get("provider_execution_contract_status")
                        != "verified_no_fast_mode_argv_environment"
                        or any(
                            provider_identity_sha256s.get(stage) is None
                            or provider_exit_sha256s.get(stage) is None
                            for stage in V2_PROVIDER_STAGE_KEYS
                        )
                        or row.get("provider_executable_sha256") is None
                        or row.get("task_runner_executable_sha256") is None
                    )
                )
                or runner_started is None
                or runner_finished is None
                or runner_finished < runner_started
                or row.get("read_session_id") is None
                or row.get("evidence_generation") is None
                or row.get("evidence_authority_fingerprint") is None
                or not refs
                or row.get("late_result_fenced") is not False
                or row.get("started_at") is None
                or row.get("completed_at") is None
            ):
                return "campaign_subject_success_invalid"
        elif subject_status == "failed_paused":
            if (
                runtime_state != "failed_drained"
                or row.get("capture_id") is None
                or row.get("unit_sha256") is None
                or row.get("terminal_outcome") in {None, "succeeded"}
                or row.get("terminal_kind") is None
                or row.get("error_code") is None
                or row.get("terminal_receipt_sha256") is None
                or row.get("terminal_index_sha256") is None
                or row.get("stage") != "failed"
                or row.get("late_result_fenced") is not True
                or row.get("completed_at") is None
            ):
                return "campaign_subject_failure_invalid"
        elif subject_status == "paused" and runtime_state != "paused_drained":
            return "campaign_subject_pause_invalid"
        elif subject_status == "inactive" and runtime_state != "inactive_rolled_back":
            return "campaign_subject_inactive_invalid"

        if real_scope and subject_status != "verified":
            if (
                row.get("provider_execution_contract_status")
                != "not_verified"
                or any(
                    provider_identity_sha256s.get(stage) is not None
                    or provider_exit_sha256s.get(stage) is not None
                    for stage in V2_PROVIDER_STAGE_KEYS
                )
                or row.get("provider_executable_sha256") is not None
            ):
                return "campaign_provider_contract_status_invalid"
            if (
                row.get("runner_process_identity_sha256") is not None
                and row.get("task_runner_executable_sha256") is None
            ):
                return "campaign_task_runner_contract_status_invalid"

        actual_models += int(row["model_call_count"])
        actual_providers += int(row["provider_request_count"])
        actual_mcp += int(row["mcp_canonical_call_count"])
        observed_models += int(row["runtime_observed_model_call_count"])
        observed_providers += int(row["runtime_observed_provider_request_count"])
        observed_mcp += int(row["runtime_observed_mcp_tool_call_count"])

    if (
        actual_models != safety.get("model_call_count")
        or actual_providers != safety.get("provider_request_count")
        or actual_mcp != safety.get("mcp_tool_call_count")
        or observed_models != safety.get("runtime_observed_model_call_count")
        or observed_providers
        != safety.get("runtime_observed_provider_request_count")
        or observed_mcp != safety.get("runtime_observed_mcp_tool_call_count")
    ):
        return "campaign_counter_mismatch"

    concurrency = payload.get("concurrency")
    if not isinstance(concurrency, Mapping) or set(concurrency) != V2_CONCURRENCY_KEYS:
        return "campaign_concurrency_invalid"
    subject_peaks = concurrency.get("subject_peak_active")
    terminal_indexes = concurrency.get("terminal_index_sha256_by_subject")
    failures = concurrency.get("failure_bindings_by_subject")
    overlap_subjects = concurrency.get("overlap_subjects")
    terminal_count = concurrency.get("terminal_task_count")
    active_count = concurrency.get("active_task_count")
    lifecycle_count = concurrency.get("lifecycle_task_count")
    evidenced_count = concurrency.get("runner_evidenced_task_count")
    missing_count = concurrency.get("runner_interval_missing_count")
    if (
        concurrency.get("calculation_source")
        != "hmac_terminal_index_task_supervisor_lifecycle"
        or concurrency.get("interval_policy")
        != "half_open_end_before_start_zero_duration_nonoverlap"
        or any(
            not _non_negative_int(value)
            for value in (
                terminal_count,
                active_count,
                lifecycle_count,
                evidenced_count,
                missing_count,
                concurrency.get("global_peak_active"),
            )
        )
        or int(terminal_count or 0) + int(active_count or 0)
        != int(lifecycle_count or 0)
        or int(evidenced_count or 0) + int(missing_count or 0)
        != int(lifecycle_count or 0)
        or not isinstance(subject_peaks, Mapping)
        or set(subject_peaks) != set(SUBJECTS)
        or any(not _non_negative_int(subject_peaks.get(subject)) for subject in SUBJECTS)
        or not isinstance(terminal_indexes, Mapping)
        or set(terminal_indexes) != set(SUBJECTS)
        or any(not _sha(terminal_indexes.get(subject)) for subject in SUBJECTS)
        or not isinstance(failures, Mapping)
        or set(failures) != set(SUBJECTS)
        or not isinstance(overlap_subjects, list)
        or overlap_subjects != sorted(set(overlap_subjects))
        or any(subject not in SUBJECTS for subject in overlap_subjects)
        or not isinstance(concurrency.get("overlap_observed"), bool)
        or concurrency.get("overlap_observed")
        is not (int(concurrency.get("global_peak_active") or 0) >= 2)
        or concurrency.get("telemetry_cross_check")
        not in {"matched_non_authoritative", "not_available"}
    ):
        return "campaign_concurrency_invalid"
    for subject in SUBJECTS:
        rows = failures.get(subject)
        if not isinstance(rows, list):
            return "campaign_failure_binding_invalid"
        for binding in rows:
            if (
                not isinstance(binding, Mapping)
                or set(binding) != V2_FAILURE_BINDING_KEYS
                or not _sha(binding.get("unit_sha256"))
                or binding.get("outcome")
                not in {"failed", "cancelled", "timed_out", "needs_rework"}
                or not _safe_id(binding.get("error_code"), nullable=True)
                or not _safe_id(binding.get("terminal_kind"))
                or not _sha(binding.get("terminal_receipt_sha256"))
                or not isinstance(binding.get("late_result_fenced"), bool)
            ):
                return "campaign_failure_binding_invalid"
        row = subjects[subject]
        if row.get("terminal_index_sha256") != terminal_indexes.get(subject):
            return "campaign_terminal_index_binding_invalid"
        if row.get("status") == "failed_paused" and not any(
            binding.get("unit_sha256") == row.get("unit_sha256")
            and binding.get("outcome") == row.get("terminal_outcome")
            and binding.get("error_code") == row.get("error_code")
            and binding.get("terminal_kind") == row.get("terminal_kind")
            and binding.get("terminal_receipt_sha256")
            == row.get("terminal_receipt_sha256")
            and binding.get("late_result_fenced")
            is row.get("late_result_fenced")
            for binding in rows
        ):
            return "campaign_subject_failure_binding_invalid"

    if real_scope:
        failed = "failed_paused" in subject_statuses
        verified_count = subject_statuses.count("verified")
        progressed = "verified" in subject_statuses
        expected = (
            (
                "production_canary_active_with_subject_failure",
                "production_canary_subject_failure",
            )
            if failed
            else (
                "production_verified",
                "production_runtime_verified",
            )
            if verified_count == len(SUBJECTS)
            else (
                "production_canary_active",
                "production_canary_partially_verified",
            )
            if progressed
            else (
                "production_canary_active",
                "production_canary_pending",
            )
        )
        if (status, result_label) != expected:
            return "campaign_status_binding_invalid"

    if _contains_private_location(payload):
        return "campaign_private_location_forbidden"
    return None


def campaign_contract_error(payload: Mapping[str, Any]) -> str | None:
    schema = payload.get("schema_version")
    if schema == LEGACY_SCHEMA_VERSION:
        return _campaign_v1_contract_error(payload)
    if schema == SCHEMA_VERSION:
        return _campaign_v2_contract_error(payload)
    return "campaign_schema_mismatch"


def campaign_runtime_error(
    payload: Mapping[str, Any],
    *,
    expected_release_id: str | None,
    now: datetime | None = None,
) -> str | None:
    """Bind a valid projection to this server release and a bounded time window."""

    if payload.get("schema_version") == LEGACY_SCHEMA_VERSION:
        return "campaign_historical_legacy_only"
    if payload.get("current_release_usable") is not True:
        return "campaign_evidence_scope_not_live"
    if expected_release_id is None or SHA256.fullmatch(expected_release_id) is None:
        return "campaign_expected_release_unavailable"
    if payload.get("release_id") != expected_release_id:
        return "campaign_release_mismatch"
    generated_at = _parsed_timestamp(payload.get("generated_at"))
    if generated_at is None:
        return "campaign_generated_at_invalid"
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age_seconds = (observed_at - generated_at).total_seconds()
    if age_seconds < -FUTURE_SKEW_SECONDS:
        return "campaign_generated_in_future"
    if age_seconds > MAX_AGE_SECONDS:
        return "campaign_stale"
    return None


class CampaignStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> CampaignResult:
        descriptor: int | None = None
        try:
            flags = os.O_RDONLY | os.O_NONBLOCK
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self.path, flags)
        except FileNotFoundError:
            return CampaignResult(
                available=False,
                error="三科并发验收尚未开始。",
                error_code="campaign_missing",
            )
        except OSError:
            return CampaignResult(
                available=False,
                error="无法安全读取三科并发验收投影。",
                error_code="campaign_unreadable",
            )
        try:
            node = os.fstat(descriptor)
            if not stat.S_ISREG(node.st_mode):
                return CampaignResult(
                    available=False,
                    error="三科并发验收投影不是安全的普通文件。",
                    error_code="campaign_unsafe_file",
                    mtime_ns=node.st_mtime_ns,
                )
            if node.st_size > MAX_BYTES:
                return CampaignResult(
                    available=False,
                    error="三科并发验收投影超过安全大小限制。",
                    error_code="campaign_too_large",
                    mtime_ns=node.st_mtime_ns,
                )
            chunks: list[bytes] = []
            total = 0
            while total <= MAX_BYTES:
                chunk = os.read(descriptor, min(65536, MAX_BYTES + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            raw = b"".join(chunks)
            if len(raw) != node.st_size:
                return CampaignResult(
                    available=False,
                    error="三科并发验收投影读取不完整。",
                    error_code="campaign_read_incomplete",
                    mtime_ns=node.st_mtime_ns,
                )
        finally:
            if descriptor is not None:
                os.close(descriptor)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return CampaignResult(
                available=False,
                error="三科并发验收投影无法解析。",
                error_code="campaign_invalid_json",
                mtime_ns=node.st_mtime_ns,
            )
        if not isinstance(payload, Mapping):
            error = "campaign_topology_invalid"
        else:
            error = campaign_contract_error(payload)
        if error is not None:
            return CampaignResult(
                available=False,
                error="三科并发验收投影未通过安全合同。",
                error_code=error,
                mtime_ns=node.st_mtime_ns,
            )
        return CampaignResult(
            available=True,
            projection=dict(payload),
            mtime_ns=node.st_mtime_ns,
        )


def public_campaign(payload: Mapping[str, Any], *, subject: str = "all") -> dict[str, Any]:
    if subject not in {"all", *SUBJECTS}:
        raise ValueError("invalid_subject")
    if payload.get("schema_version") == SCHEMA_VERSION:
        contract_error = campaign_contract_error(payload)
        if contract_error is not None:
            raise ValueError(contract_error)
    result = copy.deepcopy(dict(payload))
    subjects = payload["subjects"]
    result["subjects"] = (
        {name: copy.deepcopy(dict(subjects[name])) for name in SUBJECTS}
        if subject == "all"
        else {subject: copy.deepcopy(dict(subjects[subject]))}
    )
    result["available"] = True
    result["counter_scope"] = "acceptance_campaign"
    return result
