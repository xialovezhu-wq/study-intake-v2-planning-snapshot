#!/usr/bin/env python3
"""Local study-intake Dashboard plus isolated validation-console namespace.

The existing Dashboard routes remain GET-only and read one atomically replaced
projection.  Protected POST is available only under /api/v1/validation-console;
the production console remains offline/locked and cannot issue live work.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import signal
import stat
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, unquote, urlsplit

_DASHBOARD_MODULE_DIR = Path(__file__).resolve().parent
if str(_DASHBOARD_MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(_DASHBOARD_MODULE_DIR))

from concurrency_campaign import CampaignStore, campaign_runtime_error, public_campaign
from validation_console import ValidationConsoleError, ValidationConsoleStore


HOST = "127.0.0.1"
PORT = 8767
SCHEMA_VERSION = "study-intake-dashboard-projection-v5"
PREVIOUS_SCHEMA_VERSION = "study-intake-dashboard-projection-v4"
LEGACY_SCHEMA_VERSION = "study-intake-dashboard-projection-v3"
MODERN_SCHEMA_VERSIONS = {PREVIOUS_SCHEMA_VERSION, SCHEMA_VERSION}
ACCEPTED_SCHEMA_VERSIONS = {
    LEGACY_SCHEMA_VERSION,
    *MODERN_SCHEMA_VERSIONS,
}
MAX_PROJECTION_BYTES = 8 * 1024 * 1024
MAX_TASK_DETAIL_BYTES = 1024 * 1024
MAX_RELEASE_MANIFEST_BYTES = 1024 * 1024
HEARTBEAT_READY_MAX_AGE_SECONDS = 90
PROJECTION_READY_MAX_AGE_SECONDS = 90
HEARTBEAT_FUTURE_SKEW_SECONDS = 300
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
VALIDATION_STATIC_DIR = Path(__file__).resolve().parent / "validation_static"
DEFAULT_PROJECTION_PATH = BASE_DIR / "state" / "dashboard_projection.json"
DEFAULT_CAMPAIGN_PATH = (
    Path.home()
    / ".codex"
    / "study-intake-preprocessor"
    / "state"
    / "three-subject-concurrency-campaign.json"
)
DEFAULT_RELEASE_MANIFEST_PATH = BASE_DIR / "release.json"
PROJECTION_PATH = Path(
    os.environ.get(
        "STUDY_PREPROCESSOR_DASHBOARD_PROJECTION",
        str(DEFAULT_PROJECTION_PATH),
    )
).expanduser()
CAMPAIGN_PATH = Path(
    os.environ.get(
        "STUDY_PREPROCESSOR_CONCURRENCY_CAMPAIGN",
        str(DEFAULT_CAMPAIGN_PATH),
    )
).expanduser()
ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}"}
ALLOWED_SUBJECTS = {"all", "math", "cs408", "english"}
SUBJECT_NAMES = ("math", "cs408", "english")
QUEUE_STATES = (
    "queued",
    "running",
    "ready",
    "needs_rework",
    "failed",
    "stale",
    "evidence_pending",
)
V3_COUNT_KEYS = (
    "selected",
    "queued",
    "analysis_running",
    "critical_review_running",
    "terminal",
    "quality_passed",
    "needs_rework",
    "failed",
    "evidence_pending",
)
CURRENT_STAGES = {
    "idle",
    "frozen_evidence",
    "ready_for_selection",
    "queued",
    "dispatching",
    "analysis",
    "critical_review",
    "quality_ready",
    "needs_rework",
    "failed",
    "stale",
}
REQUIRED_MODEL = "gpt-5.6-luna"
REQUIRED_REASONING_EFFORT = "max"
SUBJECT_SOFT_RUNTIME_WARNING_SECONDS = {
    "math": 3600,
    "cs408": 1800,
    "english": 1800,
}
PRODUCTION_CANARY_STATES = {
    "armed",
    "canary_in_flight",
    "continuous_concurrent_unlocked",
    "failed_drained",
    "paused_drained",
    "inactive_rolled_back",
}
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
CODEX_RUNTIME_ATTESTATION_SUPPORTED = False
ENGLISH_CAPTURE_AUTHORITY_BOOLEAN_FIELDS = (
    "event_written",
    "dispatcher_accepted",
    "package_visible",
    "quick_intake_complete",
)
ENGLISH_CAPTURE_AUTHORITY_HASH_FIELDS = (
    "selector_sha256",
    "proposal_sha256",
    "authoritative_package_sha256",
)
ENGLISH_PROCESSING_OUTCOMES = {"succeeded", "failed", "timed_out", "cancelled"}
EN_P0_006_STATUSES = {
    "unknown",
    "needs_user_decision",
    "review_required_not_signable",
    "no_data",
    "authorization_ready",
    "luna_running",
    "luna_complete_with_failures",
    "quality_ready",
    "queued_for_sol",
    "formal_applying",
    "recovering",
    "safe_paused",
    "complete_with_failures",
    "verified_complete",
    "invalid",
}
EN_P0_006_LUNA_STATUSES = {
    "no_data", "not_started", "running", "quality_ready",
    "complete_with_failures",
}
EN_P0_006_SOL_STATUSES = {
    "no_data", "queued", "active", "recovering", "safe_paused", "complete",
}
EN_P0_006_INVENTORY_SOURCES = {
    "no_data", "candidate_bound_work_item_batch", "verified_sol_batch",
}
ALLOWED_LUNA_STATUSES = {
    "queued",
    "processing",
    "retrying",
    "ready",
    "two_pass_ready",
    "single_pass_degraded",
    "awaiting_teaching_resolution",
    "needs_sol_review",
    "quarantined",
    "failed",
    "stale",
    "skipped",
}
ALLOWED_DELIVERY_STATUSES = {"offered", "consumed"}
ALLOWED_ADOPTION_STATUSES = {
    "direct_adopted",
    "minor_edit_adopted",
    "major_edit_adopted",
    "modified_adopted",
    "rejected",
    "fallback",
    "unknown",
}
RECEIPTED_ADOPTION_STATUSES = {
    "direct_adopted",
    "minor_edit_adopted",
    "major_edit_adopted",
    "modified_adopted",
    "rejected",
    "fallback",
}
ADOPTION_BUCKETS = {
    "direct_adopted": "direct",
    "minor_edit_adopted": "minor",
    "major_edit_adopted": "major",
    "modified_adopted": "legacy_modified",
    "rejected": "rejected",
    "fallback": "fallback",
    "unknown": "unknown",
}
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
SAFE_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,319}$")
SAFE_CONTENT_REF = re.compile(
    r"^study-intake-(?:stage|report)://sha256/"
    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$"
)
RELEASE_ID = re.compile(r"^[0-9a-f]{64}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SAFE_MCP_EVIDENCE_REF = re.compile(
    r"^mcp-item:(math|cs408|english):[0-9a-f]{64}$"
)
TASK_DETAIL_SCHEMA_VERSION = "study-intake-dashboard-task-detail-v2"
LEGACY_TASK_DETAIL_SCHEMA_VERSION = "study-intake-dashboard-task-detail-v1"
TASK_DETAIL_STAGES = {
    "queued",
    "dispatched",
    "analysis",
    "critical_review",
    "evidence_validation",
    "completed",
    "failed",
    "stale",
}
MATH_SOURCE_TYPE_ALIASES = {
    "old_existing": "old_existing",
    "new_intake": "new_intake",
    "existing_formal": "old_existing",
    "new_source": "new_intake",
}


def _canonical_math_source_type(value: object) -> str:
    return MATH_SOURCE_TYPE_ALIASES.get(str(value), "unknown")
FORBIDDEN_DETAIL_KEYS = {
    "prompt",
    "raw_prompt",
    "full_prompt",
    "answer",
    "final_answer",
    "private_reasoning",
    "chain_of_thought",
    "answer_images",
    "image_bytes",
}
FORBIDDEN_DETAIL_KEY_FRAGMENTS = {
    "prompt",
    "answer",
    "chain_of_thought",
    "private_reasoning",
    "raw_image",
    "image_bytes",
}

STATIC_ROUTES = {
    "/": (STATIC_DIR / "index.html", "text/html; charset=utf-8"),
    "/index.html": (STATIC_DIR / "index.html", "text/html; charset=utf-8"),
    "/styles.css": (STATIC_DIR / "styles.css", "text/css; charset=utf-8"),
    "/app.js": (STATIC_DIR / "app.js", "text/javascript; charset=utf-8"),
    "/validation-console": (
        VALIDATION_STATIC_DIR / "index.html",
        "text/html; charset=utf-8",
    ),
    "/validation-console/": (
        VALIDATION_STATIC_DIR / "index.html",
        "text/html; charset=utf-8",
    ),
    "/validation-console/index.html": (
        VALIDATION_STATIC_DIR / "index.html",
        "text/html; charset=utf-8",
    ),
    "/validation-console/styles.css": (
        VALIDATION_STATIC_DIR / "styles.css",
        "text/css; charset=utf-8",
    ),
    "/validation-console/app.js": (
        VALIDATION_STATIC_DIR / "app.js",
        "text/javascript; charset=utf-8",
    ),
}


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _content_value_sha256(value: object) -> str:
    """Match the preprocessing core's content-value hash (no file newline)."""

    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ProjectionResult:
    available: bool
    projection: dict[str, Any] | None = None
    projection_sha256: str | None = None
    error: str | None = None
    error_code: str | None = None
    mtime_ns: int | None = None


def _release_id_from_manifest(path: Path = DEFAULT_RELEASE_MANIFEST_PATH) -> str | None:
    """Read the immutable code release identity without exposing its path."""

    try:
        with path.open("rb") as manifest:
            payload = manifest.read(MAX_RELEASE_MANIFEST_BYTES + 1)
        if len(payload) > MAX_RELEASE_MANIFEST_BYTES:
            return None
        value = json.loads(payload)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    release_id = value.get("release_id") if isinstance(value, Mapping) else None
    return release_id if isinstance(release_id, str) and RELEASE_ID.fullmatch(release_id) else None


def _nullable_non_negative_integer(value: Any) -> bool:
    return value is None or (
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
    )


def _nullable_positive_integer(value: Any) -> bool:
    return value is None or (
        isinstance(value, int) and not isinstance(value, bool) and value >= 1
    )


def _nullable_sha256(value: Any) -> bool:
    return value is None or (
        isinstance(value, str) and SHA256.fullmatch(value) is not None
    )


def _en_p0_006_contract_error(raw: Any) -> str | None:
    if not isinstance(raw, Mapping) or set(raw) != {
        "state_available", "status", "blocker_codes", "inventory", "luna", "sol",
    }:
        return "projection_v3_en_p0_006_shape_invalid"
    state_available = raw.get("state_available")
    status = raw.get("status")
    blockers = raw.get("blocker_codes")
    inventory = raw.get("inventory")
    luna = raw.get("luna")
    sol = raw.get("sol")
    if (
        not isinstance(state_available, bool)
        or status not in EN_P0_006_STATUSES - {
            "unknown", "needs_user_decision", "review_required_not_signable",
        }
        or not isinstance(blockers, list)
        or len(blockers) > 32
        or len(blockers) != len(set(blockers))
        or any(
            not isinstance(code, str) or SAFE_ID.fullmatch(code) is None
            for code in blockers
        )
        or not isinstance(inventory, Mapping)
        or not isinstance(luna, Mapping)
        or not isinstance(sol, Mapping)
    ):
        return "projection_v3_en_p0_006_shape_invalid"
    if set(inventory) != {
        "state_available", "target_count", "inventory_sha256",
        "target_set_sha256", "batch_authorization_sha256", "source",
    } or set(luna) != {
        "state_available", "status", "selected_count", "running_count",
        "terminal_count", "quality_passed_count", "failed_count",
        "remaining_count", "all_terminal", "quality_ready",
    } or set(sol) != {
        "state_available", "status", "batch_id", "current_ordinal",
        "current_target_id", "fencing_token", "attempt", "committed_count",
        "already_current_count", "failed_count", "failure_attempt_count",
        "recovery_count", "recovery_pending_count", "formal_write_count",
        "final_closure_status", "execution_closure_sha256", "updated_at",
    }:
        return "projection_v3_en_p0_006_shape_invalid"
    if (
        not isinstance(inventory.get("state_available"), bool)
        or inventory.get("source") not in EN_P0_006_INVENTORY_SOURCES
        or not _nullable_positive_integer(inventory.get("target_count"))
        or any(
            not _nullable_sha256(inventory.get(field))
            for field in (
                "inventory_sha256", "target_set_sha256",
                "batch_authorization_sha256",
            )
        )
    ):
        return "projection_v3_en_p0_006_inventory_invalid"
    inventory_available = inventory["state_available"]
    target_count = inventory.get("target_count")
    inventory_hashes = (
        inventory.get("inventory_sha256"), inventory.get("target_set_sha256"),
        inventory.get("batch_authorization_sha256"),
    )
    if inventory_available:
        if (
            target_count is None
            or any(value is None for value in inventory_hashes)
            or inventory.get("source") == "no_data"
        ):
            return "projection_v3_en_p0_006_inventory_invalid"
    elif (
        target_count is not None
        or any(value is not None for value in inventory_hashes)
        or inventory.get("source") != "no_data"
    ):
        return "projection_v3_en_p0_006_inventory_invalid"

    luna_count_fields = (
        "selected_count", "running_count", "terminal_count",
        "quality_passed_count", "failed_count", "remaining_count",
    )
    if (
        not isinstance(luna.get("state_available"), bool)
        or luna.get("status") not in EN_P0_006_LUNA_STATUSES
        or any(
            not _nullable_non_negative_integer(luna.get(field))
            for field in luna_count_fields
        )
        or luna.get("all_terminal") not in {True, False, None}
        or luna.get("quality_ready") not in {True, False, None}
    ):
        return "projection_v3_en_p0_006_luna_invalid"
    if luna["state_available"]:
        if not inventory_available or any(luna.get(field) is None for field in luna_count_fields):
            return "projection_v3_en_p0_006_luna_invalid"
        selected = int(luna["selected_count"])
        running = int(luna["running_count"])
        terminal = int(luna["terminal_count"])
        quality = int(luna["quality_passed_count"])
        failed = int(luna["failed_count"])
        remaining = int(luna["remaining_count"])
        if (
            selected != running + terminal
            or terminal != quality + failed
            or selected > int(target_count)
            or remaining != int(target_count) - terminal
            or luna.get("all_terminal") is not (terminal == target_count)
            or luna.get("quality_ready")
            is not (quality == target_count and failed == 0)
            or (luna.get("status") == "not_started" and selected != 0)
            or (luna.get("status") == "running" and running == 0)
            or (
                luna.get("status") == "quality_ready"
                and luna.get("quality_ready") is not True
            )
            or (
                luna.get("status") == "complete_with_failures"
                and not (luna.get("all_terminal") is True and failed > 0)
            )
        ):
            return "projection_v3_en_p0_006_luna_counts_invalid"
    elif (
        luna.get("status") != "no_data"
        or any(luna.get(field) is not None for field in luna_count_fields)
        or luna.get("all_terminal") is not None
        or luna.get("quality_ready") is not None
    ):
        return "projection_v3_en_p0_006_luna_no_data_invalid"

    sol_count_fields = (
        "committed_count", "already_current_count", "failed_count",
        "failure_attempt_count", "recovery_count", "recovery_pending_count",
        "formal_write_count",
    )
    if (
        not isinstance(sol.get("state_available"), bool)
        or sol.get("status") not in EN_P0_006_SOL_STATUSES
        or not _nullable_positive_integer(sol.get("current_ordinal"))
        or not _nullable_positive_integer(sol.get("fencing_token"))
        or not _nullable_positive_integer(sol.get("attempt"))
        or any(
            not _nullable_non_negative_integer(sol.get(field))
            for field in sol_count_fields
        )
        or sol.get("final_closure_status")
        not in {None, "verified_complete", "complete_with_failures"}
        or not _nullable_sha256(sol.get("execution_closure_sha256"))
    ):
        return "projection_v3_en_p0_006_sol_invalid"
    if sol["state_available"]:
        batch_id = sol.get("batch_id")
        current_target = sol.get("current_target_id")
        if (
            not inventory_available
            or not isinstance(batch_id, str)
            or SAFE_ID.fullmatch(batch_id) is None
            or (
                current_target is not None
                and _text(current_target, limit=240) is None
            )
            or any(sol.get(field) is None for field in sol_count_fields)
            or int(sol["committed_count"])
            + int(sol["already_current_count"])
            + int(sol["failed_count"])
            > int(target_count)
            or int(sol["recovery_count"]) > int(sol["failure_attempt_count"])
            or int(sol["recovery_pending_count"]) > int(sol["failure_attempt_count"])
            or (
                sol.get("updated_at") is not None
                and _text(sol.get("updated_at"), limit=64) is None
            )
        ):
            return "projection_v3_en_p0_006_sol_invalid"
        closure_status = sol.get("final_closure_status")
        closure_sha = sol.get("execution_closure_sha256")
        if sol.get("status") == "complete":
            if (
                sol.get("current_ordinal") is not None
                or current_target is not None
                or closure_status is None
                or closure_sha is None
            ):
                return "projection_v3_en_p0_006_sol_closure_invalid"
        elif closure_status is not None or closure_sha is not None:
            return "projection_v3_en_p0_006_sol_closure_invalid"
    elif (
        sol.get("status") != "no_data"
        or sol.get("batch_id") is not None
        or sol.get("current_ordinal") is not None
        or sol.get("current_target_id") is not None
        or sol.get("fencing_token") is not None
        or sol.get("attempt") is not None
        or any(sol.get(field) is not None for field in sol_count_fields)
        or sol.get("final_closure_status") is not None
        or sol.get("execution_closure_sha256") is not None
        or sol.get("updated_at") is not None
    ):
        return "projection_v3_en_p0_006_sol_no_data_invalid"

    if status == "no_data" and (
        state_available or inventory_available or luna["state_available"]
        or sol["state_available"] or blockers
    ):
        return "projection_v3_en_p0_006_no_data_invalid"
    if status == "invalid" and (state_available or not blockers):
        return "projection_v3_en_p0_006_invalid_state_invalid"
    if status not in {"no_data", "invalid"} and (
        not state_available or not inventory_available or blockers
    ):
        return "projection_v3_en_p0_006_state_invalid"
    if status == "verified_complete" and (
        sol.get("final_closure_status") != "verified_complete"
        or int(sol.get("failed_count") or 0) != 0
        or int(sol.get("recovery_pending_count") or 0) != 0
    ):
        return "projection_v3_en_p0_006_verified_complete_invalid"
    if status == "complete_with_failures" and (
        sol.get("final_closure_status") != "complete_with_failures"
    ):
        return "projection_v3_en_p0_006_complete_with_failures_invalid"
    return None


def _valid_timestamp_text(value: Any, *, optional: bool = False) -> bool:
    if value is None:
        return optional
    if not isinstance(value, str) or not value or len(value) > 64:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _terminal_counts_valid(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and set(value) in {
            frozenset(LEGACY_TERMINAL_OUTCOMES),
            frozenset(TERMINAL_OUTCOMES),
        }
        and all(
            isinstance(value.get(outcome, 0), int)
            and not isinstance(value.get(outcome, 0), bool)
            and value.get(outcome, 0) >= 0
            for outcome in TERMINAL_OUTCOMES
        )
    )


def _terminal_failure_bindings_valid(value: Any) -> bool:
    if not isinstance(value, list) or len(value) > 4096:
        return False
    required = {
        "unit_sha256",
        "outcome",
        "error_code",
        "terminal_kind",
        "terminal_receipt_sha256",
    }
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != required:
            return False
        unit = raw.get("unit_sha256")
        receipt = raw.get("terminal_receipt_sha256")
        error_code = raw.get("error_code")
        terminal_kind = raw.get("terminal_kind")
        if (
            not isinstance(unit, str)
            or SHA256.fullmatch(unit) is None
            or unit in seen
            or not isinstance(receipt, str)
            or SHA256.fullmatch(receipt) is None
            or raw.get("outcome") not in TERMINAL_FAILURE_OUTCOMES
            or not isinstance(terminal_kind, str)
            or SAFE_ID.fullmatch(terminal_kind) is None
            or (
                error_code is not None
                and (
                    not isinstance(error_code, str)
                    or SAFE_ID.fullmatch(error_code) is None
                )
            )
        ):
            return False
        seen.add(unit)
    return True


def _production_canary_contract_error(
    value: Any,
    *,
    subject: str,
    release_id: str,
) -> str | None:
    if not isinstance(value, Mapping):
        return "projection_v3_canary_shape_invalid"
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
    ):
        return "projection_v3_canary_identity_invalid"
    required_hashes = (
        "activation_id",
        "producer_authority_fingerprint",
        "producer_high_watermark_sha256",
        "activation_receipt_sha256",
        "activation_gate_authority_sha256",
        "terminal_index_sha256",
        "state_authority_sha256",
    )
    if any(
        not isinstance(value.get(key), str)
        or SHA256.fullmatch(str(value.get(key))) is None
        for key in required_hashes
    ):
        return "projection_v3_canary_binding_invalid"
    for key in (
        "last_terminal_receipt_sha256",
        "last_report_sha256",
        "last_package_sha256",
        "last_emergency_cancel_receipt_sha256",
        "last_preclaim_failure_receipt_sha256",
        "last_preclaim_failure_evidence_sha256",
        "preclaim_failure_resume_ack_sha256",
    ):
        raw = value.get(key)
        if raw is not None and (
            not isinstance(raw, str) or SHA256.fullmatch(raw) is None
        ):
            return "projection_v3_canary_binding_invalid"
    if not _valid_timestamp_text(value.get("activated_at")) or not _valid_timestamp_text(
        value.get("updated_at")
    ):
        return "projection_v3_canary_timestamp_invalid"
    if not _valid_timestamp_text(value.get("last_success_at"), optional=True) or not (
        _valid_timestamp_text(value.get("last_failure_at"), optional=True)
    ):
        return "projection_v3_canary_timestamp_invalid"
    if not _valid_timestamp_text(
        value.get("last_emergency_cancel_at"), optional=True
    ):
        return "projection_v3_canary_timestamp_invalid"
    if not _valid_timestamp_text(
        value.get("last_preclaim_failure_at"), optional=True
    ):
        return "projection_v3_canary_timestamp_invalid"
    preclaim_stage = value.get("last_preclaim_failure_stage")
    if preclaim_stage not in {
        None,
        "producer_contract",
        "materialize",
        "consumer_admission",
        "pre_claim",
        "submit_generation_fence",
    }:
        return "projection_v3_canary_preclaim_invalid"
    preclaim_error = value.get("last_preclaim_failure_error_code")
    preclaim_presence = (
        value.get("last_preclaim_failure_at") is not None,
        preclaim_stage is not None,
        preclaim_error is not None,
        value.get("last_preclaim_failure_receipt_sha256") is not None,
        value.get("last_preclaim_failure_evidence_sha256") is not None,
    )
    if any(preclaim_presence) and not all(preclaim_presence):
        return "projection_v3_canary_preclaim_invalid"
    if preclaim_error is not None and (
        not isinstance(preclaim_error, str)
        or SAFE_ID.fullmatch(preclaim_error) is None
    ):
        return "projection_v3_canary_preclaim_invalid"
    preclaim_ack = value.get("preclaim_failure_resume_ack_sha256")
    preclaim_receipt = value.get("last_preclaim_failure_receipt_sha256")
    if preclaim_ack is not None and preclaim_receipt is None:
        return "projection_v3_canary_preclaim_invalid"
    if (
        state
        in {"armed", "canary_in_flight", "continuous_concurrent_unlocked"}
        and preclaim_receipt is not None
        and preclaim_ack != preclaim_receipt
    ):
        return "projection_v3_canary_preclaim_invalid"
    late_result_fence_status = value.get("late_result_fence_status")
    if late_result_fence_status not in {"not_required", "sealed"}:
        return "projection_v3_canary_late_fence_invalid"
    if late_result_fence_status == "sealed" and (
        value.get("last_emergency_cancel_at") is None
        or value.get("last_emergency_cancel_receipt_sha256") is None
    ):
        return "projection_v3_canary_late_fence_invalid"
    if late_result_fence_status == "not_required" and (
        value.get("last_emergency_cancel_at") is not None
        or value.get("last_emergency_cancel_receipt_sha256") is not None
    ):
        return "projection_v3_canary_late_fence_invalid"
    required_booleans = {
        "producer_capture_enabled": True,
        "sol_formal_curation_enabled": False,
        "post_activation_only": True,
        "keep_backlog_drained": True,
        "production_accepted": False,
        "fast_mode_requested": False,
        "sol_enabled": False,
    }
    if any(value.get(key) is not expected for key, expected in required_booleans.items()):
        return "projection_v3_canary_safety_boundary_invalid"
    if value.get("fast_mode_effective") != "not_requested":
        return "projection_v3_canary_fast_mode_invalid"
    if (
        "requested_service_tier" not in value
        or value.get("requested_service_tier") is not None
        or value.get("observability_counter_scope")
        != "current_activation_cumulative"
    ):
        return "projection_v3_canary_runtime_scope_invalid"
    if not isinstance(value.get("luna_consumer_enabled"), bool) or not isinstance(
        value.get("unlocked_once"), bool
    ):
        return "projection_v3_canary_state_invalid"
    if (
        value.get("initial_canary_inflight_limit") != 1
        or isinstance(value.get("continuous_concurrency_limit"), bool)
        or not isinstance(value.get("continuous_concurrency_limit"), int)
        or not 1 <= value["continuous_concurrency_limit"] <= 64
    ):
        return "projection_v3_canary_limit_invalid"
    for key in (
        "queue_depth",
        "active_task_count",
        "model_call_count",
        "provider_request_count",
        "mcp_tool_call_count",
        "control_plane_model_call_count",
        "control_plane_provider_request_count",
        "control_plane_mcp_tool_call_count",
        "observed_model_call_count",
        "observed_provider_request_count",
        "observed_mcp_tool_call_count",
        "formal_write_count",
        "terminal_task_count",
    ):
        raw = value.get(key)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            return "projection_v3_canary_counter_invalid"
    if (
        value.get("model_call_count") != 0
        or value.get("provider_request_count") != 0
        or value.get("mcp_tool_call_count") != 0
        or value.get("control_plane_model_call_count") != 0
        or value.get("control_plane_provider_request_count") != 0
        or value.get("control_plane_mcp_tool_call_count") != 0
        or value.get("formal_write_count") != 0
    ):
        return "projection_v3_canary_safety_boundary_invalid"
    terminal_by_outcome = value.get("terminal_by_outcome")
    if (
        not _terminal_counts_valid(terminal_by_outcome)
        or sum(int(terminal_by_outcome.get(outcome, 0)) for outcome in TERMINAL_OUTCOMES)
        != value.get("terminal_task_count")
    ):
        return "projection_v3_canary_terminal_counts_invalid"
    queue_depth = int(value["queue_depth"])
    oldest_age = value.get("oldest_pending_age_seconds")
    if oldest_age is not None and (
        isinstance(oldest_age, bool)
        or not isinstance(oldest_age, int)
        or oldest_age < 0
    ):
        return "projection_v3_canary_queue_invalid"
    if (queue_depth == 0) is not (oldest_age is None):
        return "projection_v3_canary_queue_invalid"
    active_count = int(value["active_task_count"])
    luna_enabled = value["luna_consumer_enabled"]
    if state in {"failed_drained", "paused_drained", "inactive_rolled_back"}:
        if luna_enabled or (
            state != "failed_drained" and active_count != 0
        ):
            return "projection_v3_canary_state_invalid"
    elif not luna_enabled:
        return "projection_v3_canary_state_invalid"
    if state == "armed" and active_count != 0:
        return "projection_v3_canary_state_invalid"
    if state == "canary_in_flight" and active_count != 1:
        return "projection_v3_canary_state_invalid"
    if state == "continuous_concurrent_unlocked" and value.get("unlocked_once") is not True:
        return "projection_v3_canary_state_invalid"
    if (
        state in {"continuous_concurrent_unlocked", "failed_drained"}
        and active_count > value["continuous_concurrency_limit"]
    ):
        return "projection_v3_canary_state_invalid"
    if state == "failed_drained" and (
        value.get("last_failure_at") is None
        or not isinstance(value.get("blocking_reason"), str)
        or not value.get("blocking_reason")
    ):
        return "projection_v3_canary_failure_closure_invalid"
    if state == "continuous_concurrent_unlocked" and value.get("last_success_at") is None:
        return "projection_v3_canary_success_closure_invalid"
    next_action = value.get("next_action")
    blocking_reason = value.get("blocking_reason")
    backpressure_reason = value.get("backpressure_reason")
    if backpressure_reason not in {
        None,
        "initial_canary_inflight_limit_reached",
        "continuous_concurrency_limit_reached",
        "luna_consumer_disabled",
    }:
        return "projection_v3_canary_backpressure_invalid"
    if backpressure_reason == "initial_canary_inflight_limit_reached" and (
        state != "canary_in_flight" or active_count != 1
    ):
        return "projection_v3_canary_backpressure_invalid"
    if backpressure_reason == "continuous_concurrency_limit_reached" and (
        state != "continuous_concurrent_unlocked"
        or active_count < value["continuous_concurrency_limit"]
    ):
        return "projection_v3_canary_backpressure_invalid"
    if backpressure_reason == "luna_consumer_disabled" and (
        state not in {"failed_drained", "paused_drained", "inactive_rolled_back"}
        or luna_enabled
    ):
        return "projection_v3_canary_backpressure_invalid"
    stage_statuses = (
        value.get("last_analysis_status"),
        value.get("last_critical_review_status"),
        value.get("last_report_status"),
    )
    if (
        not isinstance(next_action, str)
        or SAFE_ID.fullmatch(next_action) is None
        or any(
            not isinstance(status, str) or SAFE_ID.fullmatch(status) is None
            for status in stage_statuses
        )
        or (
            blocking_reason is not None
            and (
                not isinstance(blocking_reason, str)
                or SAFE_ID.fullmatch(blocking_reason) is None
            )
        )
        or (
            backpressure_reason is not None
            and (
                not isinstance(backpressure_reason, str)
                or SAFE_ID.fullmatch(backpressure_reason) is None
            )
        )
    ):
        return "projection_v3_canary_status_text_invalid"
    for key in ("last_read_session_id", "last_evidence_generation"):
        raw = value.get(key)
        if raw is not None and (
            not isinstance(raw, str) or SAFE_ID.fullmatch(raw) is None
        ):
            return "projection_v3_canary_grounding_binding_invalid"
    for key in (
        "last_evidence_authority_fingerprint",
        "last_analysis_grounding_manifest_sha256",
        "last_critical_review_grounding_manifest_sha256",
    ):
        raw = value.get(key)
        if raw is not None and (
            not isinstance(raw, str) or SHA256.fullmatch(raw) is None
        ):
            return "projection_v3_canary_grounding_binding_invalid"
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
                or SAFE_MCP_EVIDENCE_REF.fullmatch(ref) is None
                or not ref.startswith(f"mcp-item:{subject}:")
                for ref in raw
            )
        ):
            return "projection_v3_canary_evidence_refs_invalid"
        evidence_refs[key] = raw
    if state == "continuous_concurrent_unlocked" and (
        not evidence_refs["last_analysis_evidence_refs"]
        or not evidence_refs["last_critical_review_evidence_refs"]
        or value.get("last_read_session_id") is None
        or value.get("last_evidence_generation") is None
        or value.get("last_evidence_authority_fingerprint") is None
        or value.get("last_analysis_grounding_manifest_sha256") is None
        or value.get("last_critical_review_grounding_manifest_sha256") is None
    ):
        return "projection_v3_canary_success_grounding_invalid"
    selected = value.get("selected")
    last_selected = value.get("last_selected")
    for selection in (selected, last_selected):
        if selection is None:
            continue
        if not isinstance(selection, Mapping):
            return "projection_v3_canary_selection_invalid"
        for key in (
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "unit_sha256",
            "frozen_payload_sha256",
            "canary_gate_sha256",
            "canary_gate_authority_sha256",
        ):
            raw = selection.get(key)
            if raw is not None and (
                not isinstance(raw, str) or SHA256.fullmatch(raw) is None
            ):
                return "projection_v3_canary_selection_invalid"
        if SAFE_ID.fullmatch(str(selection.get("producer_unit_id") or "")) is None:
            return "projection_v3_canary_selection_invalid"
        if not _valid_timestamp_text(selection.get("producer_recorded_at")):
            return "projection_v3_canary_selection_invalid"
    if state == "canary_in_flight" and selected is None:
        return "projection_v3_canary_selection_invalid"
    active_selections = value.get("active_selections")
    if (
        not isinstance(active_selections, Mapping)
        or len(active_selections) != active_count
        or len(active_selections) > 64
    ):
        return "projection_v3_canary_selection_invalid"
    for unit_sha256, active_selected in active_selections.items():
        if (
            not isinstance(unit_sha256, str)
            or SHA256.fullmatch(unit_sha256) is None
            or not isinstance(active_selected, Mapping)
            or active_selected.get("unit_sha256") != unit_sha256
        ):
            return "projection_v3_canary_selection_invalid"
    if state == "canary_in_flight" and (
        not isinstance(selected, Mapping)
        or selected.get("unit_sha256") not in active_selections
    ):
        return "projection_v3_canary_selection_invalid"
    if state != "canary_in_flight" and selected is not None:
        return "projection_v3_canary_selection_invalid"
    authority = value.get("authority")
    if (
        not isinstance(authority, Mapping)
        or authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("purpose") != "dispatch-production-canary-state"
        or any(
            not isinstance(authority.get(key), str)
            or SHA256.fullmatch(str(authority.get(key))) is None
            for key in ("key_id", "hmac_sha256")
        )
        or sha256_bytes(canonical_bytes(dict(authority)))
        != value.get("state_authority_sha256")
    ):
        return "projection_v3_canary_authority_invalid"
    return None


def _production_status_from_canary(value: Mapping[str, Any] | None) -> str:
    if value is None:
        return "paused"
    return {
        "armed": "production_canary_active",
        "canary_in_flight": "production_canary_active",
        "continuous_concurrent_unlocked": "runtime_canary_verified",
        "failed_drained": "failed",
        "paused_drained": "paused",
        "inactive_rolled_back": "paused",
    }.get(str(value.get("state")), "failed")


def _public_production_canary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    result: dict[str, Any] = {}
    for key in (
        "schema_version",
        "status",
        "state",
        "subject",
        "release_id",
        "activation_id",
        "activated_at",
        "producer_authority_fingerprint",
        "producer_high_watermark_sha256",
        "activation_receipt_sha256",
        "activation_gate_authority_sha256",
        "terminal_index_sha256",
        "last_terminal_receipt_sha256",
        "last_report_sha256",
        "last_package_sha256",
        "last_emergency_cancel_at",
        "last_emergency_cancel_receipt_sha256",
        "late_result_fence_status",
        "last_preclaim_failure_at",
        "last_preclaim_failure_stage",
        "last_preclaim_failure_error_code",
        "last_preclaim_failure_receipt_sha256",
        "last_preclaim_failure_evidence_sha256",
        "preclaim_failure_resume_ack_sha256",
        "last_success_at",
        "last_failure_at",
        "blocking_reason",
        "backpressure_reason",
        "next_action",
        "last_analysis_status",
        "last_critical_review_status",
        "last_report_status",
        "last_read_session_id",
        "last_evidence_generation",
        "last_evidence_authority_fingerprint",
        "last_analysis_grounding_manifest_sha256",
        "last_critical_review_grounding_manifest_sha256",
        "fast_mode_effective",
        "observability_counter_scope",
        "updated_at",
        "state_authority_sha256",
    ):
        raw = value.get(key)
        if raw is None:
            result[key] = None
            continue
        safe = _safe_public_text(raw, limit=160)
        if safe is not None:
            result[key] = safe
    for key in (
        "producer_capture_enabled",
        "luna_consumer_enabled",
        "sol_formal_curation_enabled",
        "post_activation_only",
        "keep_backlog_drained",
        "unlocked_once",
        "production_accepted",
        "fast_mode_requested",
        "sol_enabled",
    ):
        checked = _boolean(value.get(key))
        if checked is not None:
            result[key] = checked
    terminal_by_outcome = value.get("terminal_by_outcome")
    result["terminal_by_outcome"] = (
        {
            outcome: int(terminal_by_outcome.get(outcome, 0))
            for outcome in TERMINAL_OUTCOMES
        }
        if _terminal_counts_valid(terminal_by_outcome)
        else None
    )
    for key in (
        "queue_depth",
        "oldest_pending_age_seconds",
        "active_task_count",
        "initial_canary_inflight_limit",
        "continuous_concurrency_limit",
        "model_call_count",
        "provider_request_count",
        "mcp_tool_call_count",
        "control_plane_model_call_count",
        "control_plane_provider_request_count",
        "control_plane_mcp_tool_call_count",
        "observed_model_call_count",
        "observed_provider_request_count",
        "observed_mcp_tool_call_count",
        "formal_write_count",
        "terminal_task_count",
    ):
        raw = value.get(key)
        if raw is None and key == "oldest_pending_age_seconds":
            result[key] = None
            continue
        checked = _safe_non_negative_integer(raw)
        if checked is not None:
            result[key] = checked
    result["requested_service_tier"] = (
        None if value.get("requested_service_tier") is None else "unavailable"
    )
    selected = value.get("selected")
    if isinstance(selected, Mapping):
        public_selected: dict[str, Any] = {}
        for key in (
            "producer_unit_id",
            "producer_recorded_at",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "unit_sha256",
            "frozen_payload_sha256",
            "canary_gate_sha256",
            "canary_gate_authority_sha256",
        ):
            safe = _safe_public_text(selected.get(key), limit=160)
            if safe is not None:
                public_selected[key] = safe
        result["selected"] = public_selected
    else:
        result["selected"] = None
    last_selected = value.get("last_selected")
    if isinstance(last_selected, Mapping):
        public_last_selected: dict[str, Any] = {}
        for key in (
            "producer_unit_id",
            "producer_recorded_at",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "unit_sha256",
            "frozen_payload_sha256",
            "canary_gate_sha256",
            "canary_gate_authority_sha256",
        ):
            safe = _safe_public_text(last_selected.get(key), limit=160)
            if safe is not None:
                public_last_selected[key] = safe
        result["last_selected"] = public_last_selected
    else:
        result["last_selected"] = None
    active_selections = value.get("active_selections")
    result["active_selections"] = {}
    if isinstance(active_selections, Mapping):
        for unit_sha256, active_selected in active_selections.items():
            if not isinstance(active_selected, Mapping):
                continue
            public_active: dict[str, Any] = {}
            for key in (
                "producer_unit_id",
                "producer_recorded_at",
                "producer_input_contract_sha256",
                "source_event_set_sha256",
                "unit_sha256",
                "frozen_payload_sha256",
                "canary_gate_sha256",
                "canary_gate_authority_sha256",
            ):
                safe = _safe_public_text(active_selected.get(key), limit=160)
                if safe is not None:
                    public_active[key] = safe
            if SHA256.fullmatch(str(unit_sha256)) and public_active:
                result["active_selections"][str(unit_sha256)] = public_active
    for key in (
        "last_analysis_evidence_refs",
        "last_critical_review_evidence_refs",
    ):
        raw = value.get(key)
        result[key] = list(raw) if isinstance(raw, list) else []
    result["production_status"] = _production_status_from_canary(value)
    return result


CONCURRENCY_SUBJECT_FIELDS = {
    "concurrency_status",
    "concurrency_state",
    "concurrency_source",
    "concurrency_observed_at",
    "concurrency_error_code",
    "subject_active_task_count",
    "subject_pending_task_count",
    "subject_peak_active",
    "subject_verified_runner_active_task_count",
    "subject_runner_evidenced_task_count",
    "subject_runner_interval_missing_count",
    "scheduler_claim_subject_peak_active",
    "terminal_index_sha256",
    "terminal_task_count",
    "terminal_by_outcome",
    "terminal_failure_bindings",
    "lease_retry_wait_task_count",
    "lease_stale_task_count",
    "effective_concurrency_limit",
    "effective_concurrency_limit_mode",
    "available_concurrency_slots",
    "backpressure_reason",
}

PROJECTION_V3_ROOT_FIELDS = {
    "schema_version",
    "generated_at",
    "study_date",
    "en_p0_006_status",
    "en_p0_006",
    "dispatchers",
    "subjects",
    "concurrency",
    "global_sol",
}

DISPATCHER_LIVE_REQUIRED_FIELDS = {
    "status",
    "last_heartbeat",
    "release_id",
    "active_release_id",
    "heartbeat_release_id",
    "heartbeat_generation_status",
    "requested_model",
    "requested_reasoning_effort",
    "current_stage",
    "enabled",
    "paused",
    "control_state",
    "previous_release_heartbeat",
    "canary_gate",
}

DISPATCHER_V3_FIELDS = frozenset(
    {
        "active_release_id",
        "canary_gate",
        "control_reason",
        "control_state",
        "current_stage",
        "enabled",
        "heartbeat_generation_status",
        "heartbeat_release_id",
        "last_heartbeat",
        "mcp_server_id",
        "mcp_server_release",
        "minimum_mcp_server_release",
        "paused",
        "plugin_component_lock_sha256",
        "plugin_id",
        "plugin_version",
        "previous_release_heartbeat",
        "processing_skill_id",
        "processing_skill_sha256",
        "processing_skill_version",
        "release_id",
        "requested_model",
        "requested_reasoning_effort",
        "runtime_identity_status",
        "status",
    }
)

SUBJECT_V3_FIELDS = frozenset(
    {
        "all_terminal",
        "authority_generation",
        "available_concurrency_slots",
        "backpressure_reason",
        "batch_id",
        "batch_partition",
        "batch_state_available",
        "batch_status",
        "blocking_batch",
        "blockers",
        "canary_gate",
        "capacity",
        "capture_high_watermark",
        "concurrency_error_code",
        "concurrency_observed_at",
        "concurrency_source",
        "concurrency_state",
        "concurrency_status",
        "control_reason",
        "control_state",
        "counts",
        "current_stage",
        "effective_concurrency_limit",
        "effective_concurrency_limit_mode",
        "enabled",
        "error_code",
        "items",
        "lease_retry_wait_task_count",
        "lease_stale_task_count",
        "metric_sources",
        "metrics",
        "pipeline",
        "processing_error_code",
        "scan_snapshot_sha256",
        "sol_committed_count",
        "sol_handoff_status",
        "sol_ready",
        "sol_reviewed_count",
        "status",
        "subject_active_task_count",
        "subject_peak_active",
        "subject_pending_task_count",
        "subject_verified_runner_active_task_count",
        "subject_runner_evidenced_task_count",
        "subject_runner_interval_missing_count",
        "scheduler_claim_subject_peak_active",
        "terminal_index_sha256",
        "terminal_task_count",
        "terminal_by_outcome",
        "terminal_failure_bindings",
    }
)

ITEM_V3_FIELDS = frozenset(
    {
        "adoption_reason_code", "adoption_receipt_id", "adoption_recorded_at",
        "adoption_status", "attempt", "authoritative_package_sha256",
        "authority_key_sha256", "article_id", "answer_material_warning",
        "batch_trigger", "blocker_count", "candidate_count",
        "candidate_item_count", "candidate_path", "candidate_validation_status",
        "capture_count", "capture_id", "captured_at", "contains_answer_material",
        "critical_resume_status", "current_stage",
        "delivery_status", "dispatcher_accepted", "duration_seconds",
        "elapsed_seconds", "event_written", "evidence_bundle_sha256",
        "evidence_claim_count", "evidence_claims_with_refs",
        "evidence_coverage_pct", "evidence_hash_status",
        "evidence_manifest_sha256", "evidence_readiness_receipt_sha256",
        "evidence_refs", "evidence_status", "fence", "final_answer",
        "first_break", "formal_curation_status", "foreground_completion_stage",
        "frozen_payload_sha256",
        "generation", "input_fingerprint", "last_error_code",
        "last_state_change_at", "legacy_compatibility_receipt_sha256",
        "historical_package_sha256", "local_model_submitted", "luna_report_status",
        "luna_status", "obsidian_url", "observed_identity_provenance",
        "observed_model", "observed_reasoning_effort",
        "package_fingerprint", "package_sha256", "package_version",
        "package_visible", "pipeline_status", "processing_contract_sha256",
        "private_current_answer", "processing_error_code", "processing_outcome",
        "processor_attribution", "processor_display_name", "projection_origin",
        "proposal_sha256", "quality_gate_status", "quality_outcome",
        "quality_receipt_sha256", "queue_state", "quick_capture_status",
        "local_dispatch_status", "model_stage", "terminal_status",
        "quick_intake_complete", "raw_prompt", "receipt_count", "receipt_status",
        "release_id", "rendered_markdown", "report", "report_json_ref",
        "report_json_sha256", "report_markdown_ref", "report_markdown_sha256",
        "requested_model",
        "requested_reasoning_effort", "retry_count", "risks", "rule_version",
        "rule_version_sha256", "runtime_identity_status", "runtime_model",
        "runtime_reasoning_effort", "safe_summary", "selected_by",
        "selection_authority_status", "selection_error_code", "selector_sha256",
        "selector_type", "semantic_contract_sha256",
        "semantic_reuse_receipt_sha256", "semantic_reuse_source_release_id",
        "sentence_count", "sentence_id", "server_queue_confirmation",
        "server_queue_status", "sol_committed", "sol_decision_sha256",
        "sol_review_status", "sol_reviewed", "source_path", "source_sentence",
        "stage_receipts",
        "started_at", "study_date", "subject", "suggestion_summary",
        "superseded_input_fingerprint", "target_id", "target_label",
        "task_detail_path", "two_stage_status", "unit_sha256", "user_verbatim",
        "unknown_evidence_ref_count", "updated_at", "visual_claim_count",
        "visual_claims_with_refs", "visual_coverage_pct",
        "visual_evidence_required",
        "english_status", "retry_at_hint",
        "evidence_access_status", "analysis_execution_status",
        "analysis_report_status", "review_execution_status",
        "review_report_status", "formal_write_status", "warning_codes",
        "report_available", "formal_write_eligible",
        "execution_status", "quality_status", "report_disposition",
        "terminal_error_code", "production_accepted",
        "elapsed_runtime_seconds", "last_meaningful_progress_at",
        "soft_timeout_warning", "stall_probe_status", "exact_error_code",
        "authority_snapshot_sha256",
        "analysis_execution_receipt_sha256", "analysis_raw_output_sha256",
        "analysis_normalization_receipt_sha256", "analysis_report_sha256",
        "critical_review_execution_receipt_sha256",
        "critical_review_raw_output_sha256",
        "critical_review_normalization_receipt_sha256",
        "critical_review_report_sha256", "sol_handoff_envelope_sha256",
        "terminal_receipt_sha256",
        "preclaim_failure_stage", "preclaim_failed_at",
        "primary_preclaim_failure_receipt_sha256",
        "queue_preserved_for_recovery", "recovery_status",
        "preclaim_attempt_count", "latest_preclaim_attempt_at",
        "latest_preclaim_attempt_error_code", "preclaim_attempt_history",
    }
)

V4_REQUIRED_ITEM_FIELDS = frozenset(
    {
        "local_dispatch_status",
        "server_queue_confirmation",
        "evidence_access_status",
        "analysis_execution_status",
        "analysis_report_status",
        "review_execution_status",
        "review_report_status",
        "sol_review_status",
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
    }
)

V5_REQUIRED_ITEM_FIELDS = V4_REQUIRED_ITEM_FIELDS | frozenset(
    {"report_available", "formal_write_eligible"}
)

V4_NULLABLE_SHA256_ITEM_FIELDS = frozenset(
    {
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
    }
)

CONCURRENCY_GLOBAL_FIELDS = frozenset(
    {
        "schema_version", "status", "release_id", "generated_at", "source",
        "source_telemetry", "telemetry_authority_sha256", "peak_semantics",
        "global_active_task_count", "global_peak_active",
        "verified_runner_global_active_task_count",
        "scheduler_claim_global_peak_active",
        "runner_evidenced_task_count_global",
        "runner_interval_missing_count_global",
        "zero_duration_runner_interval_count_global",
        "terminal_task_count_global", "terminal_by_outcome_global",
        "terminal_failure_binding_count_global",
        "effective_concurrency_limit", "effective_concurrency_limit_mode",
        "available_concurrency_slots", "backpressure_reason",
        "subject_observed_at",
    }
)

CONCURRENCY_TELEMETRY_FIELDS = frozenset(
    {
        "schema_version", "release_id", "activation_ids", "active_by_subject",
        "subject_peak_active", "global_active_task_count", "global_peak_active",
        "verified_runner_active_by_subject",
        "verified_runner_global_active_task_count",
        "scheduler_claim_subject_peak_active",
        "scheduler_claim_global_peak_active",
        "runner_evidenced_task_count_by_subject",
        "runner_evidenced_task_count_global",
        "runner_interval_missing_count_by_subject",
        "runner_interval_missing_count_global",
        "zero_duration_runner_interval_count_by_subject",
        "zero_duration_runner_interval_count_global",
        "terminal_index_sha256_by_subject", "terminal_task_count_by_subject",
        "terminal_task_count_global", "terminal_by_outcome_by_subject",
        "terminal_by_outcome_global", "terminal_failure_bindings_by_subject",
        "counter_scope", "peak_source", "runner_interval_policy",
        "requested_service_tier", "fast_mode_requested", "fast_mode_effective",
        "updated_at", "model_call_count", "provider_request_count",
        "formal_write_count", "sol_enabled", "authority_sha256", "authority",
    }
)


def _non_negative_int_or_none(value: Any) -> bool:
    return value is None or (
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
    )


def _subject_count_map_valid(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and set(value) == set(SUBJECT_NAMES)
        and all(
            isinstance(value.get(subject), int)
            and not isinstance(value.get(subject), bool)
            and value[subject] >= 0
            for subject in SUBJECT_NAMES
        )
    )


def _concurrency_telemetry_contract_error(
    telemetry: Any,
    *,
    release_id: str,
    telemetry_authority_sha256: Any,
) -> str | None:
    if (
        not isinstance(telemetry, Mapping)
        or set(telemetry) != CONCURRENCY_TELEMETRY_FIELDS
        or telemetry.get("schema_version")
        != "study-intake-production-canary-concurrency-telemetry-v1"
        or telemetry.get("release_id") != release_id
        or telemetry.get("counter_scope") != CONCURRENCY_COUNTER_SCOPE
        or telemetry.get("peak_source") != CONCURRENCY_PEAK_SOURCE
        or telemetry.get("runner_interval_policy")
        != CONCURRENCY_RUNNER_INTERVAL_POLICY
        or "requested_service_tier" not in telemetry
        or telemetry.get("requested_service_tier") is not None
        or telemetry.get("fast_mode_requested") is not False
        or telemetry.get("fast_mode_effective") != "not_requested"
        or telemetry.get("model_call_count") != 0
        or telemetry.get("provider_request_count") != 0
        or telemetry.get("formal_write_count") != 0
        or telemetry.get("sol_enabled") is not False
        or not _valid_timestamp_text(telemetry.get("updated_at"))
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    activation_ids = telemetry.get("activation_ids")
    terminal_indexes = telemetry.get("terminal_index_sha256_by_subject")
    count_map_keys = (
        "active_by_subject",
        "subject_peak_active",
        "verified_runner_active_by_subject",
        "scheduler_claim_subject_peak_active",
        "runner_evidenced_task_count_by_subject",
        "runner_interval_missing_count_by_subject",
        "zero_duration_runner_interval_count_by_subject",
        "terminal_task_count_by_subject",
    )
    if (
        not isinstance(activation_ids, Mapping)
        or set(activation_ids) != set(SUBJECT_NAMES)
        or not isinstance(terminal_indexes, Mapping)
        or set(terminal_indexes) != set(SUBJECT_NAMES)
        or any(
            not _subject_count_map_valid(telemetry.get(key))
            for key in count_map_keys
        )
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    active = telemetry["active_by_subject"]
    peaks = telemetry["subject_peak_active"]
    verified_active = telemetry["verified_runner_active_by_subject"]
    scheduler_peaks = telemetry["scheduler_claim_subject_peak_active"]
    evidenced = telemetry["runner_evidenced_task_count_by_subject"]
    missing = telemetry["runner_interval_missing_count_by_subject"]
    zero_duration = telemetry["zero_duration_runner_interval_count_by_subject"]
    terminal_counts = telemetry["terminal_task_count_by_subject"]
    outcomes_by_subject = telemetry.get("terminal_by_outcome_by_subject")
    failures_by_subject = telemetry.get("terminal_failure_bindings_by_subject")
    if (
        not isinstance(outcomes_by_subject, Mapping)
        or set(outcomes_by_subject) != set(SUBJECT_NAMES)
        or not isinstance(failures_by_subject, Mapping)
        or set(failures_by_subject) != set(SUBJECT_NAMES)
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    for subject in SUBJECT_NAMES:
        activation_id = activation_ids.get(subject)
        index_sha = terminal_indexes.get(subject)
        outcomes = outcomes_by_subject.get(subject)
        failures = failures_by_subject.get(subject)
        if (
            (activation_id is not None and (
                not isinstance(activation_id, str)
                or SHA256.fullmatch(activation_id) is None
            ))
            or (index_sha is not None and (
                not isinstance(index_sha, str)
                or SHA256.fullmatch(index_sha) is None
            ))
            or ((activation_id is None) is not (index_sha is None))
            or not _terminal_counts_valid(outcomes)
            or not _terminal_failure_bindings_valid(failures)
            or sum(int(outcomes.get(outcome, 0)) for outcome in TERMINAL_OUTCOMES)
            != terminal_counts[subject]
            or len(failures)
            != sum(int(outcomes.get(outcome, 0)) for outcome in TERMINAL_FAILURE_OUTCOMES)
            or evidenced[subject] + missing[subject]
            != terminal_counts[subject] + active[subject]
            or verified_active[subject] > active[subject]
            or peaks[subject] < verified_active[subject]
            or scheduler_peaks[subject] < active[subject]
            or zero_duration[subject] > evidenced[subject]
            or (activation_id is None and (
                active[subject] != 0 or terminal_counts[subject] != 0 or failures
            ))
        ):
            return "projection_v3_concurrency_telemetry_invalid"
    global_keys = (
        "global_active_task_count",
        "global_peak_active",
        "verified_runner_global_active_task_count",
        "scheduler_claim_global_peak_active",
        "runner_evidenced_task_count_global",
        "runner_interval_missing_count_global",
        "zero_duration_runner_interval_count_global",
        "terminal_task_count_global",
    )
    if any(
        isinstance(telemetry.get(key), bool)
        or not isinstance(telemetry.get(key), int)
        or telemetry[key] < 0
        for key in global_keys
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    global_outcomes = telemetry.get("terminal_by_outcome_global")
    if (
        not _terminal_counts_valid(global_outcomes)
        or telemetry["global_active_task_count"] != sum(active.values())
        or telemetry["verified_runner_global_active_task_count"]
        != sum(verified_active.values())
        or telemetry["global_peak_active"]
        < telemetry["verified_runner_global_active_task_count"]
        or telemetry["scheduler_claim_global_peak_active"]
        < telemetry["global_active_task_count"]
        or telemetry["runner_evidenced_task_count_global"]
        != sum(evidenced.values())
        or telemetry["runner_interval_missing_count_global"] != sum(missing.values())
        or telemetry["zero_duration_runner_interval_count_global"]
        != sum(zero_duration.values())
        or telemetry["terminal_task_count_global"] != sum(terminal_counts.values())
        or any(
            int(global_outcomes.get(outcome, 0))
            != sum(
                int(outcomes_by_subject[subject].get(outcome, 0))
                for subject in SUBJECT_NAMES
            )
            for outcome in TERMINAL_OUTCOMES
        )
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    authority = telemetry.get("authority")
    authority_sha256 = (
        sha256_bytes(canonical_bytes(dict(authority)))
        if isinstance(authority, Mapping)
        else None
    )
    if (
        not isinstance(authority, Mapping)
        or set(authority)
        != {"schema_version", "algorithm", "key_id", "purpose", "hmac_sha256"}
        or authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("purpose")
        != "dispatch-production-canary-concurrency-telemetry"
        or any(
            not isinstance(authority.get(key), str)
            or SHA256.fullmatch(str(authority.get(key))) is None
            for key in ("key_id", "hmac_sha256")
        )
        or telemetry.get("authority_sha256") != authority_sha256
        or telemetry_authority_sha256 != authority_sha256
    ):
        return "projection_v3_concurrency_telemetry_invalid"
    return None


def _concurrency_contract_error(payload: Mapping[str, Any]) -> str | None:
    subjects = payload.get("subjects")
    raw = payload.get("concurrency")
    if not isinstance(subjects, Mapping):
        return "projection_v3_concurrency_topology_invalid"
    subject_has_fields = [
        isinstance(subjects.get(subject), Mapping)
        and bool(CONCURRENCY_SUBJECT_FIELDS & set(subjects[subject]))
        for subject in SUBJECT_NAMES
    ]
    if raw is None:
        return "projection_v3_concurrency_required"
    if not isinstance(raw, Mapping) or not all(subject_has_fields):
        return "projection_v3_concurrency_topology_partial"
    if set(raw) != CONCURRENCY_GLOBAL_FIELDS:
        return "projection_v3_concurrency_shape_invalid"
    dispatchers = payload.get("dispatchers")
    releases = {
        dispatcher.get("release_id")
        for dispatcher in dispatchers.values()
        if isinstance(dispatchers, Mapping) and isinstance(dispatcher, Mapping)
    } if isinstance(dispatchers, Mapping) else set()
    if (
        raw.get("schema_version") != "study-intake-dashboard-concurrency-v1"
        or len(releases) != 1
        or raw.get("release_id") not in releases
        or raw.get("generated_at") != payload.get("generated_at")
        or raw.get("peak_semantics")
        != "hmac_task_process_half_open_intervals"
        or raw.get("status") not in {"verified", "partial", "unavailable"}
        or raw.get("effective_concurrency_limit_mode")
        not in {"bounded", "unbounded", "unavailable"}
        or not _non_negative_int_or_none(raw.get("global_active_task_count"))
        or not _non_negative_int_or_none(raw.get("global_peak_active"))
        or not _non_negative_int_or_none(
            raw.get("verified_runner_global_active_task_count")
        )
        or not _non_negative_int_or_none(
            raw.get("scheduler_claim_global_peak_active")
        )
        or not _non_negative_int_or_none(
            raw.get("runner_evidenced_task_count_global")
        )
        or not _non_negative_int_or_none(
            raw.get("runner_interval_missing_count_global")
        )
        or not _non_negative_int_or_none(
            raw.get("zero_duration_runner_interval_count_global")
        )
        or not _non_negative_int_or_none(raw.get("terminal_task_count_global"))
        or not _non_negative_int_or_none(
            raw.get("terminal_failure_binding_count_global")
        )
        or (
            raw.get("terminal_by_outcome_global") is not None
            and not _terminal_counts_valid(raw.get("terminal_by_outcome_global"))
        )
        or not _non_negative_int_or_none(raw.get("effective_concurrency_limit"))
        or not _non_negative_int_or_none(raw.get("available_concurrency_slots"))
    ):
        return "projection_v3_concurrency_shape_invalid"
    global_reason = raw.get("backpressure_reason")
    if global_reason is not None and (
        not isinstance(global_reason, str) or SAFE_ID.fullmatch(global_reason) is None
    ):
        return "projection_v3_concurrency_backpressure_invalid"

    telemetry = raw.get("source_telemetry")
    public_telemetry: Mapping[str, Any] | None = None
    if telemetry is not None:
        telemetry_error = _concurrency_telemetry_contract_error(
            telemetry,
            release_id=str(raw.get("release_id")),
            telemetry_authority_sha256=raw.get(
                "telemetry_authority_sha256"
            ),
        )
        if (
            telemetry_error is not None
            or raw.get("source") != "lease_store_subject_status_v1"
        ):
            return telemetry_error or "projection_v3_concurrency_telemetry_invalid"
        public_telemetry = telemetry
    elif (
        raw.get("telemetry_authority_sha256") is not None
        or raw.get("source") is not None
    ):
        return "projection_v3_concurrency_telemetry_invalid"

    statuses: list[str] = []
    observed_map = raw.get("subject_observed_at")
    if not isinstance(observed_map, Mapping) or set(observed_map) != set(SUBJECT_NAMES):
        return "projection_v3_concurrency_observed_at_invalid"
    for subject in SUBJECT_NAMES:
        section = subjects[subject]
        if not CONCURRENCY_SUBJECT_FIELDS.issubset(section):
            return "projection_v3_concurrency_subject_shape_invalid"
        status = section.get("concurrency_status")
        state = section.get("concurrency_state")
        mode = section.get("effective_concurrency_limit_mode")
        if (
            status not in {"verified", "partial", "unavailable"}
            or state not in {
                "first_canary_armed",
                "first_canary_single_in_flight",
                "continuous_concurrent_unlocked",
                "failed_drained",
                "paused_drained",
                "inactive_rolled_back",
                "canary_not_configured",
            }
            or mode not in {"bounded", "unbounded", "unavailable"}
            or any(
                not _non_negative_int_or_none(section.get(key))
                for key in (
                    "subject_active_task_count",
                    "subject_pending_task_count",
                    "subject_peak_active",
                    "subject_verified_runner_active_task_count",
                    "subject_runner_evidenced_task_count",
                    "subject_runner_interval_missing_count",
                    "scheduler_claim_subject_peak_active",
                    "terminal_task_count",
                    "lease_retry_wait_task_count",
                    "lease_stale_task_count",
                    "effective_concurrency_limit",
                    "available_concurrency_slots",
                )
            )
        ):
            return "projection_v3_concurrency_subject_shape_invalid"
        statuses.append(str(status))
        observed_at = section.get("concurrency_observed_at")
        if observed_map.get(subject) != observed_at or (
            observed_at is not None and not _valid_timestamp_text(observed_at)
        ):
            return "projection_v3_concurrency_observed_at_invalid"
        for key in ("concurrency_error_code", "backpressure_reason"):
            value = section.get(key)
            if value is not None and (
                not isinstance(value, str) or SAFE_ID.fullmatch(value) is None
            ):
                return "projection_v3_concurrency_subject_shape_invalid"

        terminal_index_sha256 = section.get("terminal_index_sha256")
        terminal_by_outcome = section.get("terminal_by_outcome")
        terminal_failures = section.get("terminal_failure_bindings")
        if (
            terminal_index_sha256 is not None
            and (
                not isinstance(terminal_index_sha256, str)
                or SHA256.fullmatch(terminal_index_sha256) is None
            )
        ) or (
            terminal_by_outcome is not None
            and not _terminal_counts_valid(terminal_by_outcome)
        ) or (
            terminal_failures is not None
            and not _terminal_failure_bindings_valid(terminal_failures)
        ):
            return "projection_v3_concurrency_subject_shape_invalid"

        gate = section.get("canary_gate")
        if status in {"verified", "partial"}:
            if (
                section.get("concurrency_source")
                != "lease_store_subject_status_v1"
                or observed_at is None
            ):
                return "projection_v3_concurrency_source_invalid"
            if isinstance(gate, Mapping):
                if not isinstance(public_telemetry, Mapping):
                    return "projection_v3_concurrency_source_invalid"
                state_expected = {
                    "armed": "first_canary_armed",
                    "canary_in_flight": "first_canary_single_in_flight",
                    "continuous_concurrent_unlocked": (
                        "continuous_concurrent_unlocked"
                    ),
                    "failed_drained": "failed_drained",
                    "paused_drained": "paused_drained",
                    "inactive_rolled_back": "inactive_rolled_back",
                }.get(gate.get("state"))
                effective_expected = {
                    "armed": 1,
                    "canary_in_flight": 1,
                    "continuous_concurrent_unlocked": gate.get(
                        "continuous_concurrency_limit"
                    ),
                    "failed_drained": 0,
                    "paused_drained": 0,
                    "inactive_rolled_back": 0,
                }.get(gate.get("state"))
                active = section.get("subject_active_task_count")
                pending = section.get("subject_pending_task_count")
                peak = section.get("subject_peak_active")
                retry_wait = section.get("lease_retry_wait_task_count")
                stale = section.get("lease_stale_task_count")
                runner_missing = section.get(
                    "subject_runner_interval_missing_count"
                )
                telemetry_active = public_telemetry["active_by_subject"][subject]
                telemetry_peak = public_telemetry["subject_peak_active"][subject]
                expected_failures = public_telemetry[
                    "terminal_failure_bindings_by_subject"
                ][subject]
                if (
                    state_expected is None
                    or state != state_expected
                    or public_telemetry["activation_ids"][subject]
                    != gate.get("activation_id")
                    or active != gate.get("active_task_count")
                    or active != telemetry_active
                    or pending != gate.get("queue_depth") + retry_wait
                    or section.get("effective_concurrency_limit")
                    != effective_expected
                    or mode != "bounded"
                    or section.get("available_concurrency_slots")
                    != max(0, effective_expected - active)
                    or not isinstance(stale, int)
                    or section.get("subject_verified_runner_active_task_count")
                    != public_telemetry[
                        "verified_runner_active_by_subject"
                    ][subject]
                    or section.get("subject_runner_evidenced_task_count")
                    != public_telemetry[
                        "runner_evidenced_task_count_by_subject"
                    ][subject]
                    or runner_missing
                    != public_telemetry[
                        "runner_interval_missing_count_by_subject"
                    ][subject]
                    or section.get("scheduler_claim_subject_peak_active")
                    != public_telemetry[
                        "scheduler_claim_subject_peak_active"
                    ][subject]
                    or terminal_index_sha256
                    != public_telemetry[
                        "terminal_index_sha256_by_subject"
                    ][subject]
                    or terminal_index_sha256 != gate.get("terminal_index_sha256")
                    or section.get("terminal_task_count")
                    != public_telemetry["terminal_task_count_by_subject"][subject]
                    or section.get("terminal_task_count")
                    != gate.get("terminal_task_count")
                    or terminal_by_outcome
                    != public_telemetry["terminal_by_outcome_by_subject"][subject]
                    or terminal_by_outcome != gate.get("terminal_by_outcome")
                    or terminal_failures != expected_failures
                ):
                    return "projection_v3_concurrency_binding_invalid"
                if status == "verified" and (
                    runner_missing != 0
                    or peak != telemetry_peak
                    or section.get("concurrency_error_code") is not None
                ):
                    return "projection_v3_concurrency_binding_invalid"
                if status == "partial" and (
                    not isinstance(runner_missing, int)
                    or runner_missing < 1
                    or peak is not None
                    or section.get("concurrency_error_code")
                    != "runner_interval_evidence_incomplete"
                ):
                    return "projection_v3_concurrency_partial_invalid"
            elif (
                status != "partial"
                or state != "canary_not_configured"
                or not isinstance(section.get("subject_active_task_count"), int)
                or section.get("subject_pending_task_count") is not None
                or section.get("subject_peak_active") is not None
                or section.get("effective_concurrency_limit") is not None
                or section.get("available_concurrency_slots") is not None
                or mode != "unavailable"
                or any(
                    section.get(key) is not None
                    for key in (
                        "subject_verified_runner_active_task_count",
                        "subject_runner_evidenced_task_count",
                        "subject_runner_interval_missing_count",
                        "scheduler_claim_subject_peak_active",
                        "terminal_index_sha256",
                        "terminal_task_count",
                        "terminal_by_outcome",
                        "terminal_failure_bindings",
                    )
                )
            ):
                return "projection_v3_concurrency_partial_invalid"
        else:
            if any(
                section.get(key) is not None
                for key in (
                    "concurrency_source",
                    "concurrency_observed_at",
                    "subject_active_task_count",
                    "subject_pending_task_count",
                    "subject_peak_active",
                    "subject_verified_runner_active_task_count",
                    "subject_runner_evidenced_task_count",
                    "subject_runner_interval_missing_count",
                    "scheduler_claim_subject_peak_active",
                    "terminal_index_sha256",
                    "terminal_task_count",
                    "terminal_by_outcome",
                    "terminal_failure_bindings",
                    "lease_retry_wait_task_count",
                    "lease_stale_task_count",
                    "effective_concurrency_limit",
                    "available_concurrency_slots",
                )
            ) or mode != "unavailable" or not isinstance(
                section.get("concurrency_error_code"), str
            ):
                return "projection_v3_concurrency_unavailable_invalid"

    expected_status = (
        "verified"
        if all(status == "verified" for status in statuses)
        else "partial"
        if any(status in {"verified", "partial"} for status in statuses)
        else "unavailable"
    )
    if raw.get("status") != expected_status:
        return "projection_v3_concurrency_status_invalid"
    global_active = raw.get("global_active_task_count")
    global_peak = raw.get("global_peak_active")
    if isinstance(public_telemetry, Mapping):
        expected_failure_count = sum(
            len(public_telemetry["terminal_failure_bindings_by_subject"][subject])
            for subject in SUBJECT_NAMES
        )
        if (
            raw.get("verified_runner_global_active_task_count")
            != public_telemetry.get(
                "verified_runner_global_active_task_count"
            )
            or raw.get("scheduler_claim_global_peak_active")
            != public_telemetry.get("scheduler_claim_global_peak_active")
            or raw.get("runner_evidenced_task_count_global")
            != public_telemetry.get("runner_evidenced_task_count_global")
            or raw.get("runner_interval_missing_count_global")
            != public_telemetry.get("runner_interval_missing_count_global")
            or raw.get("zero_duration_runner_interval_count_global")
            != public_telemetry.get(
                "zero_duration_runner_interval_count_global"
            )
            or raw.get("terminal_task_count_global")
            != public_telemetry.get("terminal_task_count_global")
            or raw.get("terminal_by_outcome_global")
            != public_telemetry.get("terminal_by_outcome_global")
            or raw.get("terminal_failure_binding_count_global")
            != expected_failure_count
        ):
            return "projection_v3_concurrency_global_binding_invalid"
    elif any(
        raw.get(key) is not None
        for key in (
            "verified_runner_global_active_task_count",
            "scheduler_claim_global_peak_active",
            "runner_evidenced_task_count_global",
            "runner_interval_missing_count_global",
            "zero_duration_runner_interval_count_global",
            "terminal_task_count_global",
            "terminal_by_outcome_global",
            "terminal_failure_binding_count_global",
        )
    ):
        return "projection_v3_concurrency_global_binding_invalid"

    all_subject_sources_available = all(
        status in {"verified", "partial"} for status in statuses
    ) and all(
        subjects[subject].get("concurrency_source")
        == "lease_store_subject_status_v1"
        for subject in SUBJECT_NAMES
    )
    expected_global_active = (
        public_telemetry.get("global_active_task_count")
        if isinstance(public_telemetry, Mapping)
        and all_subject_sources_available
        else None
    )
    expected_global_peak = (
        public_telemetry.get("global_peak_active")
        if isinstance(public_telemetry, Mapping)
        and expected_status == "verified"
        else None
    )
    if (
        global_active != expected_global_active
        or global_peak != expected_global_peak
        or (
            isinstance(global_active, int)
            and global_active
            != sum(
                int(subjects[subject]["subject_active_task_count"])
                for subject in SUBJECT_NAMES
            )
        )
    ):
        return "projection_v3_concurrency_global_binding_invalid"

    modes = [
        subjects[subject]["effective_concurrency_limit_mode"]
        for subject in SUBJECT_NAMES
    ]
    if any(mode == "unavailable" for mode in modes):
        expected_limit = None
        expected_mode = "unavailable"
        expected_slots = None
    elif any(mode == "unbounded" for mode in modes):
        expected_limit = None
        expected_mode = "unbounded"
        expected_slots = None
    else:
        expected_limit = sum(
            int(subjects[subject]["effective_concurrency_limit"])
            for subject in SUBJECT_NAMES
        )
        expected_mode = "bounded"
        expected_slots = (
            max(0, expected_limit - global_active)
            if isinstance(global_active, int)
            else None
        )
    if (
        raw.get("effective_concurrency_limit") != expected_limit
        or raw.get("effective_concurrency_limit_mode") != expected_mode
        or raw.get("available_concurrency_slots") != expected_slots
    ):
        return "projection_v3_concurrency_global_limit_invalid"
    return None


def _projection_contract_error(payload: Mapping[str, Any]) -> str | None:
    """Return the first v3 closure violation without trusting producer aggregates."""

    schema_version = payload.get("schema_version")
    expected_root_fields = set(PROJECTION_V3_ROOT_FIELDS)
    if schema_version in MODERN_SCHEMA_VERSIONS:
        expected_root_fields.add("configured_global_continuous_concurrency_limit")
    if (
        schema_version not in ACCEPTED_SCHEMA_VERSIONS
        or set(payload) != expected_root_fields
    ):
        return "projection_v3_root_shape_invalid"
    dispatchers = payload.get("dispatchers")
    subjects = payload.get("subjects")
    global_sol = payload.get("global_sol")
    if (
        not isinstance(dispatchers, Mapping)
        or not isinstance(subjects, Mapping)
        or not isinstance(global_sol, Mapping)
    ):
        return "projection_v3_topology_invalid"
    if set(dispatchers) != set(SUBJECT_NAMES) or set(subjects) != set(SUBJECT_NAMES):
        return "projection_v3_topology_invalid"
    dispatcher_release_ids = {
        dispatcher.get("release_id")
        for dispatcher in dispatchers.values()
        if isinstance(dispatcher, Mapping)
    }
    if (
        len(dispatcher_release_ids) != 1
        or any(
            not isinstance(release_id, str)
            or RELEASE_ID.fullmatch(release_id) is None
            for release_id in dispatcher_release_ids
        )
    ):
        return "projection_v3_release_topology_invalid"
    en_p0_006_status = payload.get("en_p0_006_status")
    if (
        not isinstance(en_p0_006_status, str)
        or en_p0_006_status not in EN_P0_006_STATUSES
    ):
        return "projection_v3_en_p0_006_status_invalid"
    en_p0_error = _en_p0_006_contract_error(payload.get("en_p0_006"))
    if en_p0_error is not None:
        return en_p0_error
    if payload["en_p0_006"].get("status") != en_p0_006_status:
        return "projection_v3_en_p0_006_status_mismatch"
    canary_presence: list[bool] = []
    for subject in SUBJECT_NAMES:
        dispatcher = dispatchers.get(subject)
        section = subjects.get(subject)
        if not isinstance(dispatcher, Mapping) or not isinstance(section, Mapping):
            return "projection_v3_topology_invalid"
        if set(dispatcher) - DISPATCHER_V3_FIELDS:
            return "projection_v3_dispatcher_unknown_field"
        if set(section) - SUBJECT_V3_FIELDS:
            return "projection_v3_subject_unknown_field"
        if schema_version in MODERN_SCHEMA_VERSIONS:
            capacity = section.get("capacity")
            if (
                "blocking_batch" not in section
                or
                not isinstance(capacity, Mapping)
                or set(capacity) != {
                    "capacity_mode",
                    "initial_canary_inflight_limit",
                    "continuous_concurrency_limit",
                    "effective_concurrency_limit",
                }
                or capacity.get("capacity_mode")
                not in {"disabled", "initial_canary", "continuous_concurrent"}
                or capacity.get("initial_canary_inflight_limit") != 1
                or capacity.get("continuous_concurrency_limit") != 20
                or capacity.get("effective_concurrency_limit") not in {0, 1, 20}
                or (
                    capacity.get("capacity_mode") == "disabled"
                    and capacity.get("effective_concurrency_limit") != 0
                )
                or (
                    capacity.get("capacity_mode") == "initial_canary"
                    and capacity.get("effective_concurrency_limit") != 1
                )
                or (
                    capacity.get("capacity_mode") == "continuous_concurrent"
                    and capacity.get("effective_concurrency_limit") != 20
                )
            ):
                return "projection_v4_capacity_invalid"
        if not DISPATCHER_LIVE_REQUIRED_FIELDS.issubset(dispatcher):
            return "projection_v3_dispatcher_shape_invalid"
        if "canary_gate" not in section:
            return "projection_v3_subject_shape_invalid"
        if (
            dispatcher.get("requested_model") != REQUIRED_MODEL
            or dispatcher.get("requested_reasoning_effort")
            != REQUIRED_REASONING_EFFORT
        ):
            return "projection_v3_runtime_contract_invalid"
        release_id = dispatcher.get("release_id")
        if not isinstance(release_id, str) or RELEASE_ID.fullmatch(release_id) is None:
            return "projection_v3_dispatcher_release_invalid"
        if (
            dispatcher.get("active_release_id") != release_id
            or not isinstance(dispatcher.get("enabled"), bool)
            or dispatcher.get("control_state")
            not in {"running", "draining", "paused", "drained"}
        ):
            return "projection_v3_dispatcher_shape_invalid"
        if not isinstance(dispatcher.get("previous_release_heartbeat"), bool):
            return "projection_v3_heartbeat_generation_invalid"
        heartbeat_release = dispatcher.get("heartbeat_release_id")
        expected_heartbeat_status = (
            "current_release"
            if heartbeat_release == release_id
            else "previous_release_heartbeat"
        )
        if (
            not isinstance(heartbeat_release, str)
            or RELEASE_ID.fullmatch(heartbeat_release) is None
            or dispatcher.get("previous_release_heartbeat")
            is (heartbeat_release == release_id)
            or dispatcher.get("heartbeat_generation_status")
            != expected_heartbeat_status
        ):
            return "projection_v3_heartbeat_generation_invalid"
        if section.get("current_stage") not in CURRENT_STAGES:
            return "projection_v3_current_stage_invalid"
        dispatcher_canary = dispatcher.get("canary_gate")
        subject_canary = section.get("canary_gate")
        if (dispatcher_canary is None) is not (subject_canary is None):
            return "projection_v3_canary_projection_mismatch"
        canary_presence.append(subject_canary is not None)
        if subject_canary is not None:
            if dispatcher_canary != subject_canary:
                return "projection_v3_canary_projection_mismatch"
            canary_error = _production_canary_contract_error(
                subject_canary,
                subject=subject,
                release_id=release_id,
            )
            if canary_error is not None:
                return canary_error
        counts = section.get("counts")
        items = section.get("items")
        if not isinstance(counts, Mapping) or not isinstance(items, list):
            return "projection_v3_subject_shape_invalid"
        values: dict[str, int] = {}
        for key in V3_COUNT_KEYS:
            value = counts.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return "projection_v3_counts_invalid"
            values[key] = value
        if values["terminal"] != (
            values["quality_passed"]
            + values["needs_rework"]
            + values["failed"]
        ):
            return "projection_v3_terminal_counts_not_closed"
        if values["selected"] != (
            values["queued"]
            + values["analysis_running"]
            + values["critical_review_running"]
            + values["terminal"]
            + values["evidence_pending"]
        ):
            return "projection_v3_counts_not_closed"
        recomputed = _v3_counts(items)
        if any(recomputed[key] != values[key] for key in V3_COUNT_KEYS):
            return "projection_v3_counts_not_reproducible"
        if payload.get("schema_version") in MODERN_SCHEMA_VERSIONS:
            partition = section.get("batch_partition")
            partition_fields = {
                "current_batch_task_count",
                "current_batch_terminal_task_count",
                "current_batch_failed_task_count",
                "current_batch_sol_candidate_task_count",
                "current_batch_diagnostic_task_count",
                "outside_batch_pending_task_count",
                "outside_batch_failed_preserved_task_count",
            }
            if (
                not isinstance(partition, Mapping)
                or set(partition) != partition_fields
                or any(
                    isinstance(partition.get(field), bool)
                    or not isinstance(partition.get(field), int)
                    or int(partition.get(field)) < 0
                    for field in partition_fields
                )
                or partition["current_batch_terminal_task_count"]
                > partition["current_batch_task_count"]
                or partition["current_batch_sol_candidate_task_count"]
                + partition["current_batch_diagnostic_task_count"]
                != partition["current_batch_terminal_task_count"]
                or partition["current_batch_failed_task_count"]
                != partition["current_batch_diagnostic_task_count"]
            ):
                return "projection_v4_batch_partition_invalid"
            blocking_batch = section.get("blocking_batch")
            if blocking_batch is not None:
                blocking_fields = {
                    "batch_id",
                    "study_date",
                    "status",
                    "all_terminal",
                    "sol_ready",
                    "writer_bound",
                    "writer_handoff_status",
                    "blocker_code",
                }
                if (
                    not isinstance(blocking_batch, Mapping)
                    or set(blocking_batch) != blocking_fields
                    or not _text(blocking_batch.get("batch_id"), limit=160)
                    or not re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}",
                        str(blocking_batch.get("study_date") or ""),
                    )
                    or blocking_batch.get("study_date") == payload.get("study_date")
                    or not _text(blocking_batch.get("status"), limit=40)
                    or any(
                        not isinstance(blocking_batch.get(key), bool)
                        for key in ("all_terminal", "sol_ready", "writer_bound")
                    )
                    or blocking_batch.get("blocker_code")
                    not in {"cross_date_batch_writer_bound", "cross_date_batch_still_current"}
                    or blocking_batch.get("blocker_code")
                    not in set(section.get("blockers") or [])
                    or not _text(blocking_batch.get("writer_handoff_status"), limit=40)
                ):
                    return "projection_v4_blocking_batch_invalid"
        for item in items:
            if (
                not isinstance(item, Mapping)
                or item.get("queue_state") not in QUEUE_STATES
                or (
                    "local_dispatch_status" in item
                    and item.get("local_dispatch_status") not in {
                    "pending",
                    "pending_evidence",
                    "pending_consumer_paused",
                    "claimed",
                    "running",
                    "retrying",
                    "terminal",
                    "cancelled",
                    "stalled",
                    }
                )
                or (
                    "model_stage" in item
                    and item.get("model_stage") not in {
                    "not_started",
                    "analysis",
                    "critical_review",
                    "quality_closed",
                    "failed",
                    "cancelled",
                    }
                )
                or (
                    "terminal_status" in item
                    and item.get("terminal_status") not in {
                    None,
                    "ready",
                    "succeeded",
                    "needs_rework",
                    "failed",
                    "stale",
                    "evidence_pending",
                    "workflow_complete",
                    "workflow_complete_with_warnings",
                    "workflow_partial",
                    "execution_failed",
                    "cancelled",
                    "stalled",
                    }
                )
            ):
                return "projection_v3_queue_state_invalid"
            if set(item) - ITEM_V3_FIELDS:
                return "projection_v3_task_unknown_field"
            if any(
                not isinstance(item.get(key), str)
                or SHA256.fullmatch(str(item.get(key))) is None
                for key in ("unit_sha256", "frozen_payload_sha256", "release_id")
            ):
                return "projection_v3_task_identity_invalid"
            if item.get("release_id") != release_id:
                return "projection_v3_task_release_mismatch"
            generation = item.get("generation")
            attempt = item.get("attempt")
            fence = item.get("fence")
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation < 0
                or ((attempt is None) is not (fence is None))
                or (
                    attempt is not None
                    and (
                        isinstance(attempt, bool)
                        or not isinstance(attempt, int)
                        or attempt < 1
                        or isinstance(fence, bool)
                        or not isinstance(fence, int)
                        or fence < 1
                        or generation != attempt
                    )
                )
            ):
                return "projection_v3_task_generation_invalid"
            if (
                not _text(item.get("input_fingerprint"), limit=320)
                or not _text(item.get("rule_version"), limit=160)
                or (
                    item.get("server_queue_status")
                    in {"rate_limited", "confirmed_rate_limited"}
                    and item.get("server_queue_confirmation")
                    != "confirmed_event"
                )
                or item.get("server_queue_status")
                not in {"unknown", "rate_limited", "confirmed_rate_limited", "clear"}
            ):
                return "projection_v3_task_identity_invalid"
            if payload.get("schema_version") in MODERN_SCHEMA_VERSIONS:
                if not V4_REQUIRED_ITEM_FIELDS.issubset(item):
                    return "projection_v4_task_shape_invalid"
                if payload.get("schema_version") == SCHEMA_VERSION and (
                    not V5_REQUIRED_ITEM_FIELDS.issubset(item)
                    or not isinstance(item.get("report_available"), bool)
                    or item.get("formal_write_eligible") is not False
                ):
                    return "projection_v5_review_report_shape_invalid"
                if payload.get("schema_version") == PREVIOUS_SCHEMA_VERSION and (
                    "report_available" in item
                    or "formal_write_eligible" in item
                ):
                    return "projection_v4_task_unknown_field"
                axis_contract = {
                    "evidence_access_status": {
                        "unverified", "ready", "missing", "quarantined",
                    },
                    "analysis_execution_status": {
                        "not_started", "running", "completed", "failed",
                        "cancelled", "stalled",
                    },
                    "review_execution_status": {
                        "not_started", "running", "completed", "failed",
                        "cancelled", "stalled",
                    },
                    "analysis_report_status": {
                        "not_started", "available", "available_with_warnings",
                        "normalization_failed", "quarantined",
                    },
                    "review_report_status": {
                        "not_started", "available", "available_with_warnings",
                        "normalization_failed", "quarantined",
                    },
                    "sol_review_status": {
                        "not_eligible", "pending", "reviewing", "adopted",
                        "modified", "rejected",
                    },
                    "formal_write_status": {
                        "not_authorized", "pending", "committed", "failed",
                    },
                    "stall_probe_status": {
                        "not_applicable", "healthy", "stall_suspected",
                        "probing", "stalled", "cancelled",
                    },
                }
                if any(item.get(key) not in allowed for key, allowed in axis_contract.items()):
                    return "projection_v4_task_axes_invalid"
                warning_codes = item.get("warning_codes")
                if (
                    not isinstance(warning_codes, list)
                    or warning_codes != sorted(set(warning_codes))
                    or any(
                        not isinstance(code, str) or SAFE_ID.fullmatch(code) is None
                        for code in warning_codes
                    )
                    or not isinstance(item.get("soft_timeout_warning"), bool)
                    or not _nullable_non_negative_integer(
                        item.get("elapsed_runtime_seconds")
                    )
                    or (
                        item.get("last_meaningful_progress_at") is not None
                        and _text(item.get("last_meaningful_progress_at"), limit=64)
                        is None
                    )
                ):
                    return "projection_v4_task_progress_invalid"
                if any(
                    not _nullable_sha256(item.get(field))
                    for field in V4_NULLABLE_SHA256_ITEM_FIELDS
                ):
                    return "projection_v4_task_artifact_binding_invalid"
                exact_error = item.get("exact_error_code")
                if exact_error is not None and (
                    not isinstance(exact_error, str)
                    or SAFE_ID.fullmatch(exact_error) is None
                ):
                    return "projection_v4_task_error_code_invalid"
                queue_status = item.get("server_queue_status")
                queue_confirmation = item.get("server_queue_confirmation")
                if (
                    queue_status == "unknown"
                    and queue_confirmation != "unconfirmed"
                    or queue_status == "confirmed_rate_limited"
                    and queue_confirmation
                    not in {"confirmed_event", "provider_receipt_verified"}
                    or queue_status == "clear"
                    and queue_confirmation != "provider_receipt_verified"
                ):
                    return "projection_v4_server_queue_confirmation_invalid"
                preserved = item.get("queue_preserved_for_recovery")
                if preserved is not None:
                    history = item.get("preclaim_attempt_history")
                    primary_sha = item.get("primary_preclaim_failure_receipt_sha256")
                    if (
                        preserved is not True
                        or item.get("recovery_status") != "waiting_explicit_resume"
                        or item.get("local_dispatch_status") != "terminal"
                        or item.get("terminal_status") != "failed"
                        or item.get("queue_state") != "failed"
                        or item.get("model_stage") != "not_started"
                        or item.get("local_model_submitted") is not False
                        or not _nullable_sha256(primary_sha)
                        or primary_sha is None
                        or item.get("terminal_receipt_sha256") != primary_sha
                        or not _text(item.get("preclaim_failure_stage"), limit=64)
                        or not _valid_timestamp_text(item.get("preclaim_failed_at"))
                        or not _valid_timestamp_text(item.get("latest_preclaim_attempt_at"))
                        or not _text(item.get("latest_preclaim_attempt_error_code"), limit=160)
                        or isinstance(item.get("preclaim_attempt_count"), bool)
                        or not isinstance(item.get("preclaim_attempt_count"), int)
                        or item.get("preclaim_attempt_count") < 1
                        or not isinstance(history, list)
                        or len(history) != item.get("preclaim_attempt_count")
                        or len(history) > 256
                    ):
                        return "projection_v4_preclaim_failure_invalid"
                    primary_count = 0
                    for attempt_row in history:
                        if (
                            not isinstance(attempt_row, Mapping)
                            or set(attempt_row) != {
                                "receipt_sha256",
                                "unit_sha256",
                                "frozen_payload_sha256",
                                "failure_stage",
                                "error_code",
                                "failed_at",
                                "queue_entry_preserved",
                                "primary",
                            }
                            or any(
                                not isinstance(attempt_row.get(key), str)
                                or SHA256.fullmatch(attempt_row[key]) is None
                                for key in (
                                    "receipt_sha256",
                                    "unit_sha256",
                                    "frozen_payload_sha256",
                                )
                            )
                            or not _text(attempt_row.get("failure_stage"), limit=64)
                            or not _text(attempt_row.get("error_code"), limit=160)
                            or not _valid_timestamp_text(attempt_row.get("failed_at"))
                            or not isinstance(attempt_row.get("queue_entry_preserved"), bool)
                            or not isinstance(attempt_row.get("primary"), bool)
                        ):
                            return "projection_v4_preclaim_attempt_history_invalid"
                        if attempt_row.get("primary") is True:
                            primary_count += 1
                            if (
                                attempt_row.get("receipt_sha256") != primary_sha
                                or attempt_row.get("queue_entry_preserved") is not True
                                or attempt_row.get("failure_stage")
                                != item.get("preclaim_failure_stage")
                                or attempt_row.get("error_code")
                                != item.get("exact_error_code")
                            ):
                                return "projection_v4_preclaim_attempt_history_invalid"
                    if primary_count != 1:
                        return "projection_v4_preclaim_attempt_history_invalid"
            if item.get("quality_outcome") not in {
                None,
                "pending",
                "accepted",
                "corrected",
                "needs_sol_review",
                "quarantined",
                "rejected",
                "failed",
            } or any(
                key in item and not isinstance(item.get(key), bool)
                for key in ("sol_reviewed", "sol_committed")
            ):
                return "projection_v3_task_quality_invalid"
            shared_axes = {
                "execution_status",
                "quality_status",
                "report_disposition",
                "production_accepted",
            }
            if shared_axes.intersection(item):
                if not shared_axes.issubset(item):
                    return "projection_v5_shared_status_invalid"
                execution_status = item.get("execution_status")
                quality_status = item.get("quality_status")
                disposition = item.get("report_disposition")
                terminal_error = item.get("terminal_error_code")
                if item.get("production_accepted") is not False:
                    return "projection_v5_shared_status_invalid"
                if execution_status == "succeeded":
                    if (
                        terminal_error is not None
                        or item.get("report_available") is not True
                        or quality_status == "passed"
                        and (
                            disposition != "accepted"
                            or item.get("sol_review_status") != "not_required"
                        )
                        or quality_status == "issues_found"
                        and (
                            disposition != "needs_sol_review"
                            or item.get("sol_review_status") != "pending"
                        )
                        or quality_status not in {"passed", "issues_found"}
                    ):
                        return "projection_v5_shared_status_invalid"
                elif execution_status == "failed":
                    if (
                        quality_status != "unchecked"
                        or disposition not in {
                            "technical_failure",
                            "quarantined",
                        }
                        or not isinstance(terminal_error, str)
                        or SAFE_ID.fullmatch(terminal_error) is None
                        or item.get("sol_review_status") != "not_eligible"
                    ):
                        return "projection_v5_shared_status_invalid"
                elif execution_status != "running" or quality_status != "unchecked":
                    return "projection_v5_shared_status_invalid"
            if subject == "english":
                authority_required = {
                    *ENGLISH_CAPTURE_AUTHORITY_BOOLEAN_FIELDS,
                    *ENGLISH_CAPTURE_AUTHORITY_HASH_FIELDS,
                    "foreground_completion_stage",
                    "selected_by",
                    "selector_type",
                    "processing_outcome",
                    "selection_authority_status",
                }
                if not authority_required.issubset(item):
                    return "projection_v3_english_authority_shape_invalid"
                if any(
                    not isinstance(item.get(key), bool)
                    for key in ENGLISH_CAPTURE_AUTHORITY_BOOLEAN_FIELDS
                ) or item.get("event_written") is not True:
                    return "projection_v3_english_authority_shape_invalid"
                accepted = item["dispatcher_accepted"]
                visible = item["package_visible"]
                if (
                    item["quick_intake_complete"] is not visible
                    or (accepted and not item["event_written"])
                    or (visible and not accepted)
                ):
                    return "projection_v3_english_authority_closure_invalid"
                expected_stage = (
                    "package_visible"
                    if visible
                    else "dispatcher_accepted"
                    if accepted
                    else "event_written"
                )
                if (
                    item.get("foreground_completion_stage") != expected_stage
                    or item.get("selector_type")
                    != "signed_latest_authoritative"
                ):
                    return "projection_v3_english_authority_closure_invalid"
                selected_by = item.get("selected_by")
                authority_status = item.get("selection_authority_status")
                processing_outcome = item.get("processing_outcome")
                hashes = {
                    key: item.get(key)
                    for key in ENGLISH_CAPTURE_AUTHORITY_HASH_FIELDS
                }
                if any(
                    value is not None
                    and (
                        not isinstance(value, str)
                        or SHA256.fullmatch(value) is None
                    )
                    for value in hashes.values()
                ):
                    return "projection_v3_english_authority_hash_invalid"
                if authority_status not in {"missing", "invalid", "hmac_verified"}:
                    return "projection_v3_english_authority_shape_invalid"
                error_code = item.get("selection_error_code")
                if error_code is not None and (
                    not isinstance(error_code, str)
                    or SAFE_ID.fullmatch(error_code) is None
                ):
                    return "projection_v3_english_authority_shape_invalid"
                if visible:
                    if (
                        selected_by != "signed_latest_authoritative"
                        or authority_status != "hmac_verified"
                        or processing_outcome != "succeeded"
                        or error_code is not None
                        or any(value is None for value in hashes.values())
                        or item.get("package_sha256")
                        != hashes["authoritative_package_sha256"]
                    ):
                        return "projection_v3_english_authority_closure_invalid"
                else:
                    if (
                        selected_by != "none"
                        or hashes["proposal_sha256"] is not None
                        or hashes["authoritative_package_sha256"] is not None
                        or item.get("package_sha256") is not None
                    ):
                        return "projection_v3_english_authority_closure_invalid"
                    if authority_status == "hmac_verified":
                        if (
                            hashes["selector_sha256"] is None
                            or processing_outcome
                            not in ENGLISH_PROCESSING_OUTCOMES - {"succeeded"}
                            or error_code is not None
                        ):
                            return "projection_v3_english_authority_closure_invalid"
                    elif (
                        hashes["selector_sha256"] is not None
                        or processing_outcome is not None
                        or (
                            authority_status == "missing"
                            and error_code is not None
                        )
                        or (
                            authority_status == "invalid"
                            and error_code is None
                        )
                    ):
                        return "projection_v3_english_authority_closure_invalid"
        batch_available = section.get("batch_state_available")
        if not isinstance(batch_available, bool):
            return "projection_v3_batch_state_invalid"
        blockers = section.get("blockers")
        if not isinstance(blockers, list) or any(
            not isinstance(code, str) or SAFE_ID.fullmatch(code) is None
            for code in blockers
        ):
            return "projection_v3_batch_blockers_invalid"
        if batch_available:
            if (
                SAFE_ID.fullmatch(str(section.get("batch_id") or "")) is None
                or section.get("batch_status") not in {"open", "frozen"}
                or SAFE_ID.fullmatch(
                    str(section.get("authority_generation") or "")
                ) is None
                or not isinstance(section.get("all_terminal"), bool)
                or not isinstance(section.get("sol_ready"), bool)
            ):
                return "projection_v3_batch_state_invalid"
            if section.get("all_terminal") is True and not (
                values["selected"] > 0
                and values["terminal"] + values["evidence_pending"]
                == values["selected"]
            ):
                return "projection_v3_all_terminal_invalid"
            if section.get("sol_ready") is True and not (
                section.get("all_terminal") is True
                and section.get("batch_status") == "frozen"
                and values["selected"] > 0
                and values["quality_passed"] == values["selected"]
                and not blockers
            ):
                return "projection_v3_sol_ready_invalid"
        elif any(
            section.get(key) is not None
            for key in (
                "batch_id",
                "capture_high_watermark",
                "authority_generation",
                "all_terminal",
                "sol_ready",
            )
        ) or section.get("batch_status") != "no_data":
            return "projection_v3_batch_no_data_invalid"

    if any(canary_presence) and not all(canary_presence):
        return "projection_v3_canary_topology_partial"
    if schema_version in MODERN_SCHEMA_VERSIONS and (
        payload.get("configured_global_continuous_concurrency_limit") != 60
        or sum(
            int(subjects[subject]["capacity"]["continuous_concurrency_limit"])
            for subject in SUBJECT_NAMES
        )
        != 60
    ):
        return "projection_v4_global_capacity_invalid"

    concurrency_error = _concurrency_contract_error(payload)
    if concurrency_error is not None:
        return concurrency_error

    required_global = {
        "state_available",
        "status",
        "active_subject",
        "active_batch_id",
        "current_item",
        "authorized_queue",
        "fencing_token",
        "active_writer_count",
        "committed_count",
        "remaining_count",
        "formal_write_count",
        "updated_at",
    }
    if not required_global.issubset(global_sol):
        return "projection_v3_global_sol_shape_invalid"
    state_available = global_sol.get("state_available")
    if not isinstance(state_available, bool) or not isinstance(
        global_sol.get("authorized_queue"), list
    ):
        return "projection_v3_global_sol_shape_invalid"
    if not state_available:
        if global_sol.get("status") not in {"no_data", "failed"}:
            return "projection_v3_global_sol_no_data_invalid"
        if any(
            global_sol.get(key) is not None
            for key in (
                "active_subject",
                "active_batch_id",
                "current_item",
                "fencing_token",
                "active_writer_count",
                "committed_count",
                "remaining_count",
                "formal_write_count",
                "updated_at",
            )
        ) or global_sol.get("authorized_queue"):
            return "projection_v3_global_sol_no_data_invalid"
    else:
        active_count = global_sol.get("active_writer_count")
        if (
            isinstance(active_count, bool)
            or not isinstance(active_count, int)
            or active_count not in {0, 1}
            or isinstance(global_sol.get("formal_write_count"), bool)
            or not isinstance(global_sol.get("formal_write_count"), int)
            or global_sol.get("formal_write_count") < 0
        ):
            return "projection_v3_global_sol_counts_invalid"
        if active_count == 1 and (
            global_sol.get("active_subject") not in SUBJECT_NAMES
            or SAFE_ID.fullmatch(str(global_sol.get("active_batch_id") or "")) is None
            or isinstance(global_sol.get("fencing_token"), bool)
            or not isinstance(global_sol.get("fencing_token"), int)
            or global_sol.get("fencing_token") < 1
            or any(
                isinstance(global_sol.get(key), bool)
                or not isinstance(global_sol.get(key), int)
                or global_sol.get(key) < 0
                for key in ("committed_count", "remaining_count")
            )
        ):
            return "projection_v3_global_sol_active_invalid"
        if active_count == 0 and any(
            global_sol.get(key) is not None
            for key in (
                "active_subject",
                "active_batch_id",
                "current_item",
                "fencing_token",
                "committed_count",
                "remaining_count",
            )
        ):
            return "projection_v3_global_sol_active_invalid"
        if global_sol.get("status") not in {
            "idle",
            "queued",
            "reviewing",
            "applying",
            "recovering",
            "safe_paused",
            "failed",
        } or not _text(global_sol.get("updated_at"), limit=64):
            return "projection_v3_global_sol_state_invalid"
        queue_order: list[tuple[str, str, str]] = []
        if len(global_sol["authorized_queue"]) > 256:
            return "projection_v3_global_sol_queue_invalid"
        for row in global_sol["authorized_queue"]:
            if (
                not isinstance(row, Mapping)
                or row.get("subject") not in SUBJECT_NAMES
                or SAFE_ID.fullmatch(str(row.get("batch_id") or "")) is None
                or not _text(row.get("authorized_at"), limit=64)
                or not isinstance(row.get("authorization_receipt_sha256"), str)
                or SHA256.fullmatch(row["authorization_receipt_sha256"]) is None
                or row.get("status") not in {
                    "queued",
                    "active",
                    "reviewed",
                    "committed",
                    "failed",
                    "safe_paused",
                }
                or not isinstance(row.get("daily_sol_batch_sha256"), str)
                or SHA256.fullmatch(row["daily_sol_batch_sha256"]) is None
            ):
                return "projection_v3_global_sol_queue_invalid"
            queue_order.append(
                (
                    row["authorized_at"],
                    row["authorization_receipt_sha256"],
                    row["batch_id"],
                )
            )
        if queue_order != sorted(queue_order):
            return "projection_v3_global_sol_fifo_invalid"
    return None


class ProjectionStore:
    """Loads a complete atomic projection without retaining stale fallback data."""

    def __init__(self, path: Path = PROJECTION_PATH) -> None:
        self.path = path

    def _load_path(
        self,
        path: Path,
        *,
        missing_code: str = "projection_missing",
        missing_message: str = "预处理服务尚未生成 Dashboard 投影。",
    ) -> ProjectionResult:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return ProjectionResult(
                available=False,
                error=missing_message,
                error_code=missing_code,
            )
        except OSError:
            return ProjectionResult(
                available=False,
                error="无法读取 Dashboard 投影状态。",
                error_code="projection_unreadable",
            )

        if stat.st_size > MAX_PROJECTION_BYTES:
            return ProjectionResult(
                available=False,
                error="Dashboard 投影超过安全大小限制。",
                error_code="projection_too_large",
                mtime_ns=stat.st_mtime_ns,
            )

        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return ProjectionResult(
                available=False,
                error="Dashboard 投影损坏或尚未完成原子更新。",
                error_code="projection_invalid_json",
                mtime_ns=stat.st_mtime_ns,
            )

        if not isinstance(payload, dict):
            return ProjectionResult(
                available=False,
                error="Dashboard 投影格式无效。",
                error_code="projection_invalid_shape",
                mtime_ns=stat.st_mtime_ns,
            )
        if payload.get("schema_version") not in ACCEPTED_SCHEMA_VERSIONS:
            return ProjectionResult(
                available=False,
                error="Dashboard 投影版本不兼容。",
                error_code="projection_schema_mismatch",
                mtime_ns=stat.st_mtime_ns,
            )
        contract_error = _projection_contract_error(payload)
        if contract_error is not None:
            return ProjectionResult(
                available=False,
                error="Dashboard v3 投影未通过拓扑、批次或计数闭合校验。",
                error_code=contract_error,
                mtime_ns=stat.st_mtime_ns,
            )

        return ProjectionResult(
            available=True,
            projection=payload,
            projection_sha256=sha256_bytes(raw),
            mtime_ns=stat.st_mtime_ns,
        )

    def load(self, study_date: str | None = None) -> ProjectionResult:
        current = self._load_path(self.path)
        if study_date is None:
            return current
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", study_date):
            return ProjectionResult(
                available=False,
                error="请求日期没有可用的 Dashboard 投影。",
                error_code="projection_date_unavailable",
            )

        current_date = (
            current.projection.get("study_date")
            if current.available and isinstance(current.projection, dict)
            else None
        )
        if current_date == study_date:
            return current

        archive_path = self.path.parent / "dashboard-projections" / f"{study_date}.json"
        archived = self._load_path(
            archive_path,
            missing_code="projection_date_unavailable",
            missing_message="请求日期没有可用的 Dashboard 投影。",
        )
        if archived.available and archived.projection is not None:
            if archived.projection.get("study_date") != study_date:
                return ProjectionResult(
                    available=False,
                    error="请求日期没有可用的 Dashboard 投影。",
                    error_code="projection_date_unavailable",
                    mtime_ns=archived.mtime_ns,
                )
            return archived

        # A missing current projection retains its established no-data contract.
        # Once a valid current projection exists for another date, however, an
        # absent archive must never be represented as a truthful empty result.
        if current.error_code == "projection_missing" and archived.error_code == (
            "projection_date_unavailable"
        ):
            return current
        if not current.available and archived.error_code == "projection_date_unavailable":
            return current
        return archived


def _text(value: Any, *, limit: int = 4000) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    return value[:limit]


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _safe_ref(value: Any) -> str | None:
    ref = _text(value, limit=320)
    if ref is None:
        return None
    if SAFE_REF.fullmatch(ref) or SAFE_CONTENT_REF.fullmatch(ref):
        return ref
    return None


def _projection_identity(
    projection: Mapping[str, Any], projection_sha256: str | None
) -> dict[str, Any]:
    generation = _safe_public_text(projection.get("generated_at"), limit=64)
    if generation is not None and not _valid_timestamp_text(generation):
        generation = None
    dispatchers = projection.get("dispatchers")
    releases = {
        value.get("release_id")
        for value in dispatchers.values()
        if isinstance(dispatchers, Mapping)
        and isinstance(value, Mapping)
        and isinstance(value.get("release_id"), str)
        and RELEASE_ID.fullmatch(str(value.get("release_id"))) is not None
    } if isinstance(dispatchers, Mapping) else set()
    release_id = next(iter(releases)) if len(releases) == 1 else None
    digest = (
        projection_sha256
        if isinstance(projection_sha256, str)
        and SHA256.fullmatch(projection_sha256) is not None
        else None
    )
    verified = generation is not None and digest is not None and release_id is not None
    return {
        "status": "verified" if verified else "unavailable",
        "generation": generation,
        "sha256": digest,
        "release_id": release_id,
    }


def _dashboard_request_counters() -> dict[str, Any]:
    return {
        "dashboard_request_counter_scope": "dashboard_read_only_request",
        "dashboard_request_model_call_count": 0,
        "dashboard_request_provider_request_count": 0,
        "dashboard_request_mcp_tool_call_count": 0,
        "dashboard_request_formal_write_count": 0,
    }


def _safe_link(value: Any) -> str | None:
    link = _text(value, limit=1000)
    if link is None:
        return None
    if link.startswith("obsidian://"):
        return link
    if link.startswith("http://127.0.0.1:8765/"):
        return link
    return None


def _public_capture_high_watermark(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    watermark = _safe_ref(value.get("value"))
    generation = _safe_ref(value.get("authority_generation"))
    if watermark is None or generation is None:
        return None
    return {"value": watermark, "authority_generation": generation}


def _valid_date(value: str | None) -> bool:
    if value is None:
        return True
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))


def _subject_sections(projection: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    subjects = projection.get("subjects")
    if not isinstance(subjects, Mapping):
        return {}
    result: dict[str, Mapping[str, Any]] = {}
    for subject in SUBJECT_NAMES:
        section = subjects.get(subject)
        if isinstance(section, Mapping):
            result[subject] = section
    return result


def _public_dispatchers(projection: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw_dispatchers = projection.get("dispatchers")
    if not isinstance(raw_dispatchers, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for subject in SUBJECT_NAMES:
        raw = raw_dispatchers.get(subject)
        if not isinstance(raw, Mapping):
            continue
        public: dict[str, Any] = {}
        for key in (
            "status",
            "last_heartbeat",
            "requested_model",
            "requested_reasoning_effort",
            "runtime_identity_status",
            "runtime_model",
            "runtime_reasoning_effort",
            "release_id",
            "active_release_id",
            "heartbeat_release_id",
            "heartbeat_generation_status",
            "plugin_id",
            "plugin_version",
            "plugin_component_lock_sha256",
            "processing_skill_id",
            "processing_skill_version",
            "processing_skill_sha256",
            "mcp_server_id",
            "mcp_server_release",
            "minimum_mcp_server_release",
            "current_stage",
            "last_error_code",
            "control_state",
            "control_reason",
        ):
            value = _safe_public_text(raw.get(key), limit=160)
            if value is not None:
                public[key] = value
        paused = _boolean(raw.get("paused"))
        if paused is not None:
            public["paused"] = paused
        enabled = _boolean(raw.get("enabled"))
        if enabled is not None:
            public["enabled"] = enabled
        previous_release_heartbeat = _boolean(
            raw.get("previous_release_heartbeat")
        )
        if previous_release_heartbeat is not None:
            public["previous_release_heartbeat"] = (
                previous_release_heartbeat
            )
        canary_gate = _public_production_canary(raw.get("canary_gate"))
        if canary_gate is not None:
            public["canary_gate"] = canary_gate
            public["production_status"] = canary_gate["production_status"]
        else:
            public["production_status"] = (
                "paused"
                if public.get("paused") is True or public.get("enabled") is False
                else "not_configured"
            )
        public["stage_timeout_seconds"] = SUBJECT_SOFT_RUNTIME_WARNING_SECONDS[subject]
        release_id = public.get("release_id")
        if not isinstance(release_id, str) or RELEASE_ID.fullmatch(release_id) is None:
            public.pop("release_id", None)
        result[subject] = public
    return result


def _dispatcher_readiness(
    dispatcher: Mapping[str, Any],
    *,
    expected_release_id: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if dispatcher.get("previous_release_heartbeat") is True:
        return {
            "ready": False,
            "error": "previous_release_heartbeat",
            "heartbeat_generation_status": "previous_release_heartbeat",
        }
    if dispatcher.get("enabled") is False or dispatcher.get("control_state") in {
        "paused",
        "drained",
        "draining",
    }:
        return {
            "ready": False,
            "error": "dispatcher_paused",
            "control_state": dispatcher.get("control_state") or "paused",
        }
    heartbeat_text = dispatcher.get("last_heartbeat")
    if not isinstance(heartbeat_text, str):
        return {"ready": False, "error": "dispatcher_heartbeat_missing"}
    try:
        heartbeat = datetime.fromisoformat(heartbeat_text.replace("Z", "+00:00"))
    except ValueError:
        return {"ready": False, "error": "dispatcher_heartbeat_invalid"}
    if heartbeat.tzinfo is None:
        return {"ready": False, "error": "dispatcher_heartbeat_invalid"}
    checked_at = now or datetime.now(timezone.utc)
    age_seconds = (
        checked_at.astimezone(timezone.utc) - heartbeat.astimezone(timezone.utc)
    ).total_seconds()
    public_age = max(0, int(age_seconds))
    if age_seconds < -HEARTBEAT_FUTURE_SKEW_SECONDS:
        return {
            "ready": False,
            "error": "dispatcher_heartbeat_in_future",
            "heartbeat_age_seconds": 0,
        }
    if age_seconds > HEARTBEAT_READY_MAX_AGE_SECONDS:
        return {
            "ready": False,
            "error": "dispatcher_heartbeat_stale",
            "heartbeat_age_seconds": public_age,
        }
    if dispatcher.get("paused") is True:
        return {
            "ready": False,
            "error": "dispatcher_paused",
            "heartbeat_age_seconds": public_age,
        }
    if dispatcher.get("status") != "running":
        return {
            "ready": False,
            "error": "dispatcher_not_running",
            "heartbeat_age_seconds": public_age,
        }
    if expected_release_id is None:
        return {
            "ready": False,
            "error": "dashboard_release_unversioned",
            "heartbeat_age_seconds": public_age,
        }
    heartbeat_release_id = (
        dispatcher.get("heartbeat_release_id")
        or dispatcher.get("release_id")
    )
    if heartbeat_release_id != expected_release_id:
        return {
            "ready": False,
            "error": "dispatcher_release_mismatch",
            "heartbeat_age_seconds": public_age,
            "expected_release_id": expected_release_id,
            "dispatcher_release_id": heartbeat_release_id,
            "heartbeat_generation_status": dispatcher.get(
                "heartbeat_generation_status"
            ),
        }
    if (
        dispatcher.get("requested_model") != REQUIRED_MODEL
        or dispatcher.get("requested_reasoning_effort")
        != REQUIRED_REASONING_EFFORT
    ):
        return {
            "ready": False,
            "error": "dispatcher_runtime_contract_mismatch",
            "heartbeat_age_seconds": public_age,
        }
    return {
        "ready": True,
        "heartbeat_age_seconds": public_age,
        "release_match": True,
    }


def _service_health(
    projection: Mapping[str, Any],
    *,
    expected_release_id: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    checked_at = now or datetime.now(timezone.utc)

    def runtime_timestamp_fresh(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        try:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if observed.tzinfo is None:
            return False
        age = (
            checked_at.astimezone(timezone.utc)
            - observed.astimezone(timezone.utc)
        ).total_seconds()
        return -HEARTBEAT_FUTURE_SKEW_SECONDS <= age <= PROJECTION_READY_MAX_AGE_SECONDS

    def runtime_timestamp_not_too_far_in_future(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        try:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if observed.tzinfo is None:
            return False
        age = (
            checked_at.astimezone(timezone.utc)
            - observed.astimezone(timezone.utc)
        ).total_seconds()
        return age >= -HEARTBEAT_FUTURE_SKEW_SECONDS
    generated_at = _text(projection.get("generated_at"), limit=64)
    projection_age_seconds: int | None = None
    projection_fresh = False
    if generated_at is not None:
        try:
            generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            age = (
                checked_at.astimezone(timezone.utc)
                - generated.astimezone(timezone.utc)
            ).total_seconds()
            projection_age_seconds = max(0, int(age))
            projection_fresh = (
                generated.tzinfo is not None
                and -HEARTBEAT_FUTURE_SKEW_SECONDS
                <= age
                <= PROJECTION_READY_MAX_AGE_SECONDS
            )
        except (ValueError, AttributeError):
            projection_fresh = False
    dispatchers = _public_dispatchers(projection)
    subjects = _subject_sections(projection)
    canary_topology = all(
        isinstance(subjects.get(subject, {}).get("canary_gate"), Mapping)
        for subject in SUBJECT_NAMES
    )
    concurrency = projection.get("concurrency")
    target_release_bound = bool(
        expected_release_id
        and all(
            dispatchers.get(subject, {}).get("release_id")
            == expected_release_id
            for subject in SUBJECT_NAMES
        )
    )
    source_telemetry = (
        concurrency.get("source_telemetry")
        if isinstance(concurrency, Mapping)
        else None
    )
    source_telemetry_bound = bool(
        expected_release_id
        and isinstance(concurrency, Mapping)
        and isinstance(source_telemetry, Mapping)
        and _concurrency_telemetry_contract_error(
            source_telemetry,
            release_id=expected_release_id,
            telemetry_authority_sha256=concurrency.get(
                "telemetry_authority_sha256"
            ),
        )
        is None
        and source_telemetry.get("activation_ids")
        == {
            subject: subjects.get(subject, {}).get("canary_gate", {}).get(
                "activation_id"
            )
            for subject in SUBJECT_NAMES
        }
    )
    canary_runtime_ready = bool(
        target_release_bound
        and canary_topology
        and isinstance(concurrency, Mapping)
        and concurrency.get("status") == "verified"
        and concurrency.get("release_id") == expected_release_id
        and concurrency.get("source") == "lease_store_subject_status_v1"
        and concurrency.get("runner_interval_missing_count_global") == 0
        and isinstance(concurrency.get("source_telemetry"), Mapping)
        and concurrency["source_telemetry"].get("counter_scope")
        == CONCURRENCY_COUNTER_SCOPE
        and concurrency["source_telemetry"].get("peak_source")
        == CONCURRENCY_PEAK_SOURCE
        and concurrency["source_telemetry"].get("runner_interval_policy")
        == CONCURRENCY_RUNNER_INTERVAL_POLICY
        and source_telemetry_bound
        and runtime_timestamp_not_too_far_in_future(
            source_telemetry.get("updated_at")
        )
        and all(
            subjects.get(subject, {}).get("concurrency_status") == "verified"
            and subjects.get(subject, {}).get("concurrency_source")
            == "lease_store_subject_status_v1"
            and subjects.get(subject, {}).get("canary_gate", {}).get(
                "release_id"
            )
            == expected_release_id
            and runtime_timestamp_fresh(
                subjects.get(subject, {}).get("concurrency_observed_at")
            )
            for subject in SUBJECT_NAMES
        )
    )
    services: dict[str, Any] = {}
    required = 0
    ready = 0
    for subject in SUBJECT_NAMES:
        enabled = bool(subjects.get(subject, {}).get("enabled", False))
        gate = subjects.get(subject, {}).get("canary_gate")
        operational_enabled = (
            gate.get("luna_consumer_enabled") is True
            if canary_topology and isinstance(gate, Mapping)
            else enabled
        )
        readiness = _dispatcher_readiness(
            dispatchers.get(subject, {}),
            expected_release_id=expected_release_id,
            now=checked_at,
        )
        services[subject] = {"enabled": operational_enabled, **readiness}
        if operational_enabled or canary_topology:
            required += 1
            if operational_enabled and readiness.get("ready") is True:
                ready += 1
    global_sol = _public_global_sol(projection)
    gate_sol_safety = bool(
        canary_topology
        and all(
            subjects[subject]["canary_gate"].get("sol_enabled") is False
            and subjects[subject]["canary_gate"].get("formal_write_count") == 0
            and subjects[subject]["canary_gate"].get(
                "sol_formal_curation_enabled"
            )
            is False
            for subject in SUBJECT_NAMES
        )
    )
    global_sol_inactive = bool(
        global_sol.get("state_available") is not True
        or (
            global_sol.get("status") in {"idle", "safe_paused", "failed"}
            and global_sol.get("active_writer_count") == 0
            and global_sol.get("formal_write_count") == 0
        )
    )
    sol_safety_ready = gate_sol_safety and global_sol_inactive
    sol_writer_ready = sol_safety_ready
    luna_processing_ready = required > 0 and required == ready
    return {
        "ready": (
            projection_fresh
            and luna_processing_ready
            and sol_safety_ready
            and canary_runtime_ready
        ),
        "http_ready": True,
        "projection_fresh": projection_fresh,
        "projection_age_seconds": projection_age_seconds,
        "luna_processing_ready": luna_processing_ready,
        "sol_writer_ready": sol_writer_ready,
        "sol_safety_ready": sol_safety_ready,
        "production_canary_runtime_ready": canary_runtime_ready,
        "required_dispatchers": required,
        "ready_dispatchers": ready,
        "services": services,
    }


def _public_global_sol(projection: Mapping[str, Any]) -> dict[str, Any]:
    raw = projection.get("global_sol")
    if not isinstance(raw, Mapping):
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
    result: dict[str, Any] = {
        "state_available": raw.get("state_available") is True,
        "status": _safe_public_text(raw.get("status"), limit=40) or "no_data",
        "active_subject": (
            raw.get("active_subject")
            if raw.get("active_subject") in SUBJECT_NAMES
            else None
        ),
        "active_batch_id": _safe_ref(raw.get("active_batch_id")),
        "current_item": _safe_ref(raw.get("current_item")),
        "authorized_queue": [],
        "updated_at": _safe_public_text(raw.get("updated_at"), limit=64),
    }
    for key in (
        "fencing_token",
        "active_writer_count",
        "committed_count",
        "remaining_count",
        "formal_write_count",
    ):
        result[key] = _safe_non_negative_integer(raw.get(key))
    rows = raw.get("authorized_queue")
    if isinstance(rows, list):
        for row in rows[:256]:
            if not isinstance(row, Mapping) or row.get("subject") not in SUBJECT_NAMES:
                continue
            batch_id = _safe_ref(row.get("batch_id"))
            authorized_at = _safe_public_text(row.get("authorized_at"), limit=64)
            receipt = _safe_ref(row.get("authorization_receipt_sha256"))
            status = _safe_public_text(row.get("status"), limit=40)
            if (
                batch_id is None
                or authorized_at is None
                or receipt is None
                or status not in {
                    "queued",
                    "active",
                    "reviewed",
                    "committed",
                    "failed",
                    "safe_paused",
                }
            ):
                continue
            public_row = {
                "subject": row["subject"],
                "batch_id": batch_id,
                "authorized_at": authorized_at,
                "authorization_receipt_sha256": receipt,
                "status": status,
            }
            daily = _safe_ref(row.get("daily_sol_batch_sha256"))
            if daily is None:
                continue
            public_row["daily_sol_batch_sha256"] = daily
            result["authorized_queue"].append(public_row)
    error_code = _safe_public_text(raw.get("error_code"), limit=160)
    if error_code is not None and SAFE_ID.fullmatch(error_code):
        result["error_code"] = error_code
    return result


def _empty_public_en_p0_006() -> dict[str, Any]:
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


def _public_en_p0_006(projection: Mapping[str, Any]) -> dict[str, Any]:
    raw = projection.get("en_p0_006")
    if _en_p0_006_contract_error(raw) is not None:
        return _empty_public_en_p0_006()
    assert isinstance(raw, Mapping)
    inventory = raw["inventory"]
    luna = raw["luna"]
    sol = raw["sol"]
    assert isinstance(inventory, Mapping)
    assert isinstance(luna, Mapping)
    assert isinstance(sol, Mapping)
    return {
        "state_available": raw.get("state_available") is True,
        "status": str(raw["status"]),
        "blocker_codes": [str(code) for code in raw["blocker_codes"]],
        "inventory": {
            "state_available": inventory.get("state_available") is True,
            "target_count": _safe_non_negative_integer(inventory.get("target_count")),
            "inventory_sha256": _safe_ref(inventory.get("inventory_sha256")),
            "target_set_sha256": _safe_ref(inventory.get("target_set_sha256")),
            "batch_authorization_sha256": _safe_ref(
                inventory.get("batch_authorization_sha256")
            ),
            "source": str(inventory["source"]),
        },
        "luna": {
            "state_available": luna.get("state_available") is True,
            "status": str(luna["status"]),
            **{
                field: _safe_non_negative_integer(luna.get(field))
                for field in (
                    "selected_count", "running_count", "terminal_count",
                    "quality_passed_count", "failed_count", "remaining_count",
                )
            },
            "all_terminal": _boolean(luna.get("all_terminal")),
            "quality_ready": _boolean(luna.get("quality_ready")),
        },
        "sol": {
            "state_available": sol.get("state_available") is True,
            "status": str(sol["status"]),
            "batch_id": _safe_ref(sol.get("batch_id")),
            "current_ordinal": _safe_non_negative_integer(
                sol.get("current_ordinal")
            ),
            "current_target_id": _text(sol.get("current_target_id"), limit=240),
            "fencing_token": _safe_non_negative_integer(sol.get("fencing_token")),
            "attempt": _safe_non_negative_integer(sol.get("attempt")),
            **{
                field: _safe_non_negative_integer(sol.get(field))
                for field in (
                    "committed_count", "already_current_count", "failed_count",
                    "failure_attempt_count", "recovery_count",
                    "recovery_pending_count", "formal_write_count",
                )
            },
            "final_closure_status": (
                sol.get("final_closure_status")
                if sol.get("final_closure_status")
                in {"verified_complete", "complete_with_failures"}
                else None
            ),
            "execution_closure_sha256": _safe_ref(
                sol.get("execution_closure_sha256")
            ),
            "updated_at": _safe_public_text(sol.get("updated_at"), limit=64),
        },
    }


def _other_subjects_luna_continuing(
    global_sol: Mapping[str, Any],
    service_health: Mapping[str, Any],
) -> dict[str, Any]:
    active_subject = global_sol.get("active_subject")
    if (
        global_sol.get("state_available") is not True
        or global_sol.get("status") not in {"reviewing", "applying", "recovering"}
        or active_subject not in SUBJECT_NAMES
    ):
        return {
            "confirmed": False,
            "subjects": [],
            "reason": "no_active_sol_writer",
        }
    others = [subject for subject in SUBJECT_NAMES if subject != active_subject]
    services = service_health.get("services")
    if not isinstance(services, Mapping):
        return {
            "confirmed": False,
            "subjects": [],
            "reason": "dispatcher_health_missing",
        }
    fresh = [
        subject
        for subject in others
        if isinstance(services.get(subject), Mapping)
        and services[subject].get("enabled") is True
        and services[subject].get("ready") is True
    ]
    return {
        "confirmed": len(fresh) == len(others),
        "subjects": fresh,
        "reason": (
            "fresh_heartbeats_confirmed"
            if len(fresh) == len(others)
            else "other_subject_heartbeat_not_fresh"
        ),
    }


PUBLIC_ITEM_TEXT_FIELDS = (
    "capture_id",
    "candidate_kind",
    "source_kind",
    "subject",
    "study_date",
    "captured_at",
    "updated_at",
    "target_id",
    "target_label",
    "article_id",
    "sentence_id",
    "batch_trigger",
    "english_status",
    "display_status",
    "candidate_validation_status",
    "receipt_status",
    "luna_status",
    "quick_capture_status",
    "luna_report_status",
    "pipeline_status",
    "two_stage_status",
    "quality_gate_status",
    "quality_outcome",
    "execution_status",
    "quality_status",
    "report_disposition",
    "terminal_error_code",
    "sol_review_status",
    "formal_curation_status",
    "runtime_identity_status",
    "runtime_model",
    "runtime_reasoning_effort",
    "observed_model",
    "observed_reasoning_effort",
    "observed_identity_provenance",
    "requested_model",
    "requested_reasoning_effort",
    "processor_attribution",
    "processor_display_name",
    "stage",
    "queue_state",
    "local_dispatch_status",
    "model_stage",
    "terminal_status",
    "current_stage",
    "task_id",
    "input_sha256",
    "rule_version",
    "rule_version_sha256",
    "evidence_integrity_status",
    "processing_status",
    "unit_sha256",
    "frozen_payload_sha256",
    "release_id",
    "server_queue_status",
    "server_queue_confirmation",
    "started_at",
    "last_state_change_at",
    "package_version",
    "package_fingerprint",
    "package_sha256",
    "input_fingerprint",
    "processing_contract_sha256",
    "semantic_contract_sha256",
    "authority_key_sha256",
    "processing_error_code",
    "foreground_completion_stage",
    "selected_by",
    "selector_type",
    "selector_sha256",
    "proposal_sha256",
    "authoritative_package_sha256",
    "processing_outcome",
    "selection_authority_status",
    "selection_error_code",
    "quality_receipt_sha256",
    "evidence_manifest_sha256",
    "evidence_bundle_sha256",
    "evidence_readiness_receipt_sha256",
    "legacy_compatibility_receipt_sha256",
    "superseded_input_fingerprint",
    "projection_origin",
    "evidence_hash_status",
    "knowledge_snapshot_status",
    "delivery_status",
    "adoption_status",
    "adoption_receipt_id",
    "adoption_recorded_at",
    "adoption_note",
    "adoption_reason_code",
    "sol_decision_sha256",
    "disposition",
    "evidence_access_status",
    "analysis_execution_status",
    "analysis_report_status",
    "review_execution_status",
    "review_report_status",
    "formal_write_status",
    "stall_probe_status",
    "exact_error_code",
    "last_meaningful_progress_at",
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
    "authority_snapshot_sha256",
    "preclaim_failure_stage",
    "preclaim_failed_at",
    "primary_preclaim_failure_receipt_sha256",
    "recovery_status",
    "latest_preclaim_attempt_at",
    "latest_preclaim_attempt_error_code",
)
PUBLIC_ITEM_NUMBER_FIELDS = (
    "generation",
    "fence",
    "attempt",
    "duration_seconds",
    "retry_count",
    "evidence_claim_count",
    "evidence_claims_with_refs",
    "unknown_evidence_ref_count",
    "evidence_coverage_pct",
    "visual_claim_count",
    "visual_claims_with_refs",
    "visual_coverage_pct",
    "sentence_count",
    "capture_count",
    "candidate_count",
    "candidate_item_count",
    "receipt_count",
    "blocker_count",
    "elapsed_seconds",
    "elapsed_runtime_seconds",
    "preclaim_attempt_count",
)
PUBLIC_ITEM_BOOLEAN_FIELDS = (
    "visual_evidence_required",
    "visual_required",
    "two_pass_complete",
    "local_model_submitted",
    "sol_reviewed",
    "sol_committed",
    "event_written",
    "dispatcher_accepted",
    "package_visible",
    "quick_intake_complete",
    "soft_timeout_warning",
    "queue_preserved_for_recovery",
    "report_available",
    "formal_write_eligible",
    "production_accepted",
)
PUBLIC_ITEM_LIST_FIELDS: tuple[str, ...] = ("warning_codes",)

STAGE_RECEIPT_TEXT_FIELDS = (
    "status",
    "requested_model",
    "requested_reasoning_effort",
    "runtime_identity_status",
    "runtime_model",
    "runtime_reasoning_effort",
    "observed_model",
    "observed_reasoning_effort",
    "observed_identity_provenance",
    "artifact_ref",
    "artifact_sha256",
    "output_sha256",
    "schema_sha256",
    "prompt_sha256",
    "error_code",
    "retry_at_hint",
)
STAGE_RECEIPT_NUMBER_FIELDS = ("duration_ms", "duration_seconds")


def _safe_public_text(value: Any, *, limit: int = 4000) -> str | None:
    value = _text(value, limit=limit)
    if value is None or _contains_local_path(value):
        return None
    if any(ord(character) < 32 for character in value):
        return None
    return value


def _safe_non_negative_number(value: Any) -> int | float | None:
    value = _number(value)
    if value is None or value < 0:
        return None
    return value


def _safe_non_negative_integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _safe_percentage(value: Any) -> int | float | None:
    value = _safe_non_negative_number(value)
    if value is None or value > 100:
        return None
    return value


def _public_stage_receipts(item: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = item.get("stage_receipts")
    if not isinstance(raw, Mapping):
        raw = item.get("runtime_receipts")
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for stage in ("analysis", "critical_review"):
        receipt = raw.get(stage)
        if not isinstance(receipt, Mapping):
            continue
        public: dict[str, Any] = {}
        for key in STAGE_RECEIPT_TEXT_FIELDS:
            value = (
                _safe_ref(receipt.get(key))
                if key.endswith(("_ref", "_sha256"))
                else _safe_public_text(receipt.get(key), limit=320)
            )
            if value is not None:
                public[key] = value
        for key in STAGE_RECEIPT_NUMBER_FIELDS:
            value = (
                _safe_non_negative_integer(receipt.get(key))
                if key == "duration_ms"
                else _safe_non_negative_number(receipt.get(key))
            )
            if value is not None:
                public[key] = value
        if "runtime_identity_status" in public:
            public["runtime_identity_status"] = "requested_unverified"
        if public:
            result[stage] = public
    return result


def _normalize_item_coverage(result: dict[str, Any], prefix: str) -> None:
    claim_key = f"{prefix}_claim_count"
    covered_key = f"{prefix}_claims_with_refs"
    percentage_key = f"{prefix}_coverage_pct"
    claims = result.get(claim_key)
    covered = result.get(covered_key)
    if not isinstance(claims, int) or not isinstance(covered, int):
        return
    if covered > claims:
        result.pop(claim_key, None)
        result.pop(covered_key, None)
        result.pop(percentage_key, None)
        return
    if claims == 0:
        result.pop(percentage_key, None)
        return
    result[percentage_key] = round((covered / claims) * 100, 1)


def _public_item(item: Mapping[str, Any], subject_hint: str) -> dict[str, Any] | None:
    capture_id = _text(item.get("capture_id"), limit=160)
    if capture_id is None or SAFE_ID.fullmatch(capture_id) is None:
        return None

    subject = _text(item.get("subject"), limit=16) or subject_hint
    if subject not in {"math", "cs408", "english"}:
        return None

    result: dict[str, Any] = {"capture_id": capture_id, "subject": subject}
    for key in PUBLIC_ITEM_TEXT_FIELDS:
        if key in {"capture_id", "subject"}:
            continue
        value = _safe_public_text(item.get(key), limit=4000)
        if value is not None:
            result[key] = value

    unit_sha = _text(item.get("unit_sha256"), limit=64)
    task_detail_path = _text(item.get("task_detail_path"), limit=2000)
    result["detail_available"] = bool(
        unit_sha
        and SHA256.fullmatch(unit_sha)
        and task_detail_path
        and task_detail_path.endswith(
            f"/dispatch/state/task-details/{unit_sha}.json"
        )
    )
    public_queue_status = result.get("server_queue_status")
    public_queue_confirmation = result.get("server_queue_confirmation")
    if (
        public_queue_status in {"rate_limited", "confirmed_rate_limited"}
        and public_queue_confirmation
        in {"confirmed_event", "provider_receipt_verified"}
    ):
        pass
    elif (
        public_queue_status == "clear"
        and public_queue_confirmation == "provider_receipt_verified"
    ):
        pass
    else:
        result["server_queue_status"] = "unknown"
        result["server_queue_confirmation"] = "unconfirmed"

    if "observed_model" not in result and "runtime_model" in result:
        result["observed_model"] = result["runtime_model"]
    if "observed_reasoning_effort" not in result and "runtime_reasoning_effort" in result:
        result["observed_reasoning_effort"] = result["runtime_reasoning_effort"]

    for key in (
        "package_sha256",
        "quality_receipt_sha256",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "evidence_readiness_receipt_sha256",
        "legacy_compatibility_receipt_sha256",
        "superseded_input_fingerprint",
        "input_sha256",
        "unit_sha256",
        "frozen_payload_sha256",
        "processing_contract_sha256",
        "semantic_contract_sha256",
        "authority_key_sha256",
        "authority_snapshot_sha256",
        "selector_sha256",
        "proposal_sha256",
        "authoritative_package_sha256",
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
        "primary_preclaim_failure_receipt_sha256",
    ):
        value = result.get(key)
        if not isinstance(value, str) or SHA256.fullmatch(value) is None:
            result.pop(key, None)
    if subject == "english":
        for key in ENGLISH_CAPTURE_AUTHORITY_HASH_FIELDS:
            if item.get(key) is None:
                result[key] = None
        if item.get("processing_outcome") is None:
            result["processing_outcome"] = None
    release_id = result.get("release_id")
    if not isinstance(release_id, str) or RELEASE_ID.fullmatch(release_id) is None:
        result.pop("release_id", None)

    for key in ("pipeline_status", "two_stage_status"):
        if key in result and result[key] not in ALLOWED_LUNA_STATUSES:
            result.pop(key, None)
    if "runtime_identity_status" in result:
        result["runtime_identity_status"] = "requested_unverified"
        for key in (
            "runtime_model",
            "runtime_reasoning_effort",
            "observed_model",
            "observed_reasoning_effort",
        ):
            result.pop(key, None)
        if result.get("processor_attribution") not in {"not_run", "legacy_unverified"}:
            result["processor_attribution"] = "requested_unverified"
            result["processor_display_name"] = "模型预处理（运行身份未验证）"

    delivery = result.get("delivery_status")
    if delivery not in ALLOWED_DELIVERY_STATUSES:
        result.pop("delivery_status", None)
    adoption = result.get("adoption_status")
    if adoption not in ALLOWED_ADOPTION_STATUSES:
        result["adoption_status"] = "unknown"
    receipt_id = _safe_ref(result.get("adoption_receipt_id"))
    if receipt_id is None or result.get("adoption_status") == "unknown":
        result.pop("adoption_receipt_id", None)
        result.pop("adoption_recorded_at", None)
        if result.get("adoption_status") in RECEIPTED_ADOPTION_STATUSES:
            result["adoption_status"] = "unknown"
    else:
        result["adoption_receipt_id"] = receipt_id
    result["adoption_bucket"] = ADOPTION_BUCKETS[result.get("adoption_status", "unknown")]
    for key in PUBLIC_ITEM_NUMBER_FIELDS:
        value = (
            _safe_percentage(item.get(key))
            if key.endswith("_pct")
            else (
                _safe_non_negative_number(item.get(key))
                if key == "duration_seconds"
                else _safe_non_negative_integer(item.get(key))
            )
        )
        if value is not None:
            result[key] = value
    for key in PUBLIC_ITEM_BOOLEAN_FIELDS:
        value = _boolean(item.get(key))
        if value is not None:
            result[key] = value
    _normalize_item_coverage(result, "evidence")
    _normalize_item_coverage(result, "visual")
    for key in PUBLIC_ITEM_LIST_FIELDS:
        raw_values = item.get(key)
        if not isinstance(raw_values, list):
            continue
        clean_values = []
        for raw_value in raw_values[:50]:
            clean = (
                _safe_ref(raw_value)
                if key == "evidence_refs"
                else _text(raw_value, limit=1000)
            )
            if clean is not None:
                clean_values.append(clean)
        if clean_values:
            result[key] = clean_values
    for key in ("last_error_code", "critical_resume_status"):
        value = _text(item.get(key), limit=160)
        if value is not None and re.fullmatch(r"[A-Za-z0-9._:-]+", value):
            result[key] = value
    retry_at_hint = _text(item.get("retry_at_hint"), limit=160)
    if (
        retry_at_hint is not None
        and not any(ord(character) < 32 for character in retry_at_hint)
        and not _contains_local_path(retry_at_hint)
        and "/" not in retry_at_hint
        and "\\" not in retry_at_hint
        and "~" not in retry_at_hint
    ):
        result["retry_at_hint"] = retry_at_hint
    link = _safe_link(item.get("obsidian_url"))
    if link is not None:
        result["obsidian_url"] = link
    stage_receipts = _public_stage_receipts(item)
    if stage_receipts:
        result["stage_receipts"] = stage_receipts
    raw_attempts = item.get("preclaim_attempt_history")
    if isinstance(raw_attempts, list):
        public_attempts: list[dict[str, Any]] = []
        for raw_attempt in raw_attempts[:256]:
            if not isinstance(raw_attempt, Mapping):
                continue
            receipt_sha256 = _safe_ref(raw_attempt.get("receipt_sha256"))
            unit_sha256 = _safe_ref(raw_attempt.get("unit_sha256"))
            frozen_sha256 = _safe_ref(raw_attempt.get("frozen_payload_sha256"))
            failure_stage = _safe_public_text(
                raw_attempt.get("failure_stage"), limit=64
            )
            error_code = _safe_public_text(raw_attempt.get("error_code"), limit=160)
            failed_at = _safe_public_text(raw_attempt.get("failed_at"), limit=64)
            if None in {
                receipt_sha256,
                unit_sha256,
                frozen_sha256,
                failure_stage,
                error_code,
                failed_at,
            }:
                continue
            public_attempts.append(
                {
                    "receipt_sha256": receipt_sha256,
                    "unit_sha256": unit_sha256,
                    "frozen_payload_sha256": frozen_sha256,
                    "failure_stage": failure_stage,
                    "error_code": error_code,
                    "failed_at": failed_at,
                    "queue_entry_preserved": raw_attempt.get("queue_entry_preserved")
                    is True,
                    "primary": raw_attempt.get("primary") is True,
                }
            )
        if public_attempts:
            result["preclaim_attempt_history"] = public_attempts
    return result


def _all_public_items(projection: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for subject, section in _subject_sections(projection).items():
        raw_items = section.get("items")
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            item = _public_item(raw_item, subject)
            if item is not None:
                items.append(item)
    return items


def _raw_item(
    projection: Mapping[str, Any], *, capture_id: str, study_date: str | None,
    subject: str,
) -> tuple[Mapping[str, Any], str] | None:
    for subject_name, section in _subject_sections(projection).items():
        if subject != "all" and subject != subject_name:
            continue
        raw_items = section.get("items")
        if not isinstance(raw_items, list):
            continue
        for item in raw_items:
            if not isinstance(item, Mapping):
                continue
            if item.get("capture_id") != capture_id:
                continue
            if study_date is not None and item.get("study_date") != study_date:
                continue
            return item, subject_name
    return None


class TaskDetailError(RuntimeError):
    pass


def _contains_forbidden_detail_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized_key = str(key).strip().lower().replace("-", "_")
            if normalized_key in FORBIDDEN_DETAIL_KEYS or any(
                fragment in normalized_key
                for fragment in FORBIDDEN_DETAIL_KEY_FRAGMENTS
            ):
                return True
            if _contains_forbidden_detail_key(nested):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_detail_key(item) for item in value)
    return False


def _detail_sha(value: Any, *, required: bool = False) -> str | None:
    candidate = _text(value, limit=64)
    if candidate is not None and SHA256.fullmatch(candidate):
        return candidate
    if required:
        raise TaskDetailError("task_detail_hash_invalid")
    return None


def _detail_integer(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TaskDetailError(f"task_detail_{label}_invalid")
    return value


def _read_bounded_json(path: Path, *, max_bytes: int = 256 * 1024) -> Mapping[str, Any]:
    try:
        node_stat = path.lstat()
    except OSError as exc:
        raise TaskDetailError("task_detail_dependency_missing") from exc
    if path.is_symlink() or not stat.S_ISREG(node_stat.st_mode):
        raise TaskDetailError("task_detail_dependency_type_invalid")
    if node_stat.st_size > max_bytes:
        raise TaskDetailError("task_detail_dependency_too_large")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskDetailError("task_detail_dependency_invalid_json") from exc
    if not isinstance(value, Mapping):
        raise TaskDetailError("task_detail_dependency_invalid_shape")
    return value


def _expected_runtime_path(runtime_root: Path, absolute_value: Any, relative: Path) -> Path:
    value = _text(absolute_value, limit=2000)
    if value is None:
        raise TaskDetailError("task_detail_runtime_path_missing")
    path = Path(value).expanduser().resolve(strict=False)
    expected = (runtime_root / relative).resolve(strict=False)
    if path != expected:
        raise TaskDetailError("task_detail_runtime_path_mismatch")
    current = runtime_root.absolute()
    for part in relative.parts:
        current = current / part
        try:
            if current.is_symlink():
                raise TaskDetailError("task_detail_runtime_symlink_rejected")
        except OSError as exc:
            raise TaskDetailError("task_detail_runtime_path_unreadable") from exc
    return path


def _load_dispatch_events(
    runtime_root: Path,
    detail: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[Mapping[str, Any]]]:
    unit_sha = _detail_sha(detail.get("unit_sha256"), required=True)
    fence = _detail_integer(detail.get("fence"), label="fence")
    attempt = _detail_integer(detail.get("attempt"), label="attempt")
    index_root = _expected_runtime_path(
        runtime_root,
        detail.get("event_index_root"),
        Path("dispatch/state/task-events") / unit_sha / f"fence-{fence}",
    )
    if not index_root.is_dir() or index_root.is_symlink():
        raise TaskDetailError("task_detail_event_index_invalid")
    index_paths = sorted(index_root.glob("*.json"))
    if len(index_paths) > 100:
        raise TaskDetailError("task_detail_stage_events_invalid")
    events: list[dict[str, Any]] = []
    raw_events: list[Mapping[str, Any]] = []
    prior_sequence = -1
    for index_path in index_paths:
        index = _read_bounded_json(index_path, max_bytes=32 * 1024)
        if index.get("schema_version") not in {
            "study-intake-dispatch-task-event-index-v1",
            "study-intake-dispatch-task-event-index-v2",
        }:
            raise TaskDetailError("task_detail_event_index_invalid")
        sequence = _detail_integer(index.get("sequence"), label="event_sequence")
        event_sha = _detail_sha(index.get("event_sha256"), required=True)
        if (
            sequence <= prior_sequence
            or index.get("unit_sha256") != unit_sha
            or index.get("fence") != fence
            or index.get("attempt") != attempt
            or index.get("formal_write_count") != 0
        ):
            raise TaskDetailError("task_detail_event_index_invalid")
        prior_sequence = sequence
        event_path = _expected_runtime_path(
            runtime_root,
            index.get("event_path"),
            Path("dispatch/events/sha256") / event_sha[:2] / f"{event_sha}.json",
        )
        event = _read_bounded_json(event_path, max_bytes=64 * 1024)
        if sha256_bytes(event_path.read_bytes()) != event_sha:
            raise TaskDetailError("task_detail_event_hash_mismatch")
        if (
            event.get("schema_version") not in {
                "study-intake-dispatch-task-event-v1",
                "study-intake-dispatch-task-event-v2",
            }
            or event.get("unit_sha256") != unit_sha
            or event.get("fence") != fence
            or event.get("attempt") != attempt
            or event.get("formal_write_count") != 0
            or index.get("event") != event.get("event")
        ):
            raise TaskDetailError("task_detail_event_identity_mismatch")
        event_name = _safe_public_text(event.get("event"), limit=40)
        occurred_at = _safe_public_text(event.get("occurred_at"), limit=64)
        if event_name not in {
            "claim",
            "claimed",
            "process_started",
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
            "model_submitted",
            "analysis_submitted",
            "analysis_completed",
            "analysis_checkpoint_reused",
            "critical_started",
            "critical_completed",
            "recovery_scheduled",
            "retry_wait",
            "published",
            "workflow_complete",
            "workflow_complete_with_warnings",
            "workflow_partial",
            "execution_failed",
            "stalled",
            "cancelled",
            "failed",
            "timeout",
            "cancel",
        } or occurred_at is None:
            raise TaskDetailError("task_detail_event_invalid")
        stage_map = {
            "claim": ("queued", "queued"),
            "claimed": ("queued", "claimed"),
            "process_started": ("dispatched", "running"),
            "child_process_started": ("analysis", "running"),
            "child_process_exited": ("analysis", "running"),
            "provider_process_started": ("analysis", "running"),
            "provider_process_exited": ("analysis", "running"),
            "provider_progress": ("analysis", "running"),
            "provider_heartbeat": ("analysis", "running"),
            "mcp_call_started": ("analysis", "running"),
            "mcp_call_completed": ("analysis", "running"),
            "mcp_receipt_published": ("analysis", "running"),
            "raw_output_persisted": ("analysis", "running"),
            "normalization_started": ("analysis", "running"),
            "normalization_completed": ("analysis", "running"),
            "normalization_warning": ("analysis", "running"),
            "stage_progress": ("analysis", "running"),
            "soft_timeout_warning": ("analysis", "running"),
            "stall_probe_started": ("analysis", "probing"),
            "stall_probe_succeeded": ("analysis", "running"),
            "stall_probe_failed": ("analysis", "probing"),
            "stall_suspected": ("analysis", "stall_suspected"),
            "model_submitted": ("analysis", "running"),
            "analysis_submitted": ("analysis", "running"),
            "analysis_completed": ("analysis", "ready"),
            "analysis_checkpoint_reused": ("critical_review", "running"),
            "critical_started": ("critical_review", "running"),
            "critical_completed": ("critical_review", "ready"),
            "recovery_scheduled": ("analysis", "retrying"),
            "retry_wait": ("analysis", "retrying"),
            "published": ("completed", "ready"),
            "workflow_complete": ("completed", "ready"),
            "workflow_complete_with_warnings": ("completed", "ready"),
            "workflow_partial": ("failed", "failed"),
            "execution_failed": ("failed", "failed"),
            "stalled": ("failed", "stalled"),
            "cancelled": ("stale", "cancelled"),
            "failed": ("failed", "failed"),
            "timeout": ("failed", "failed"),
            "cancel": ("stale", "stale"),
        }
        stage, status = stage_map[event_name]
        if event.get("stage_name") == "critical_review" and stage in {
            "analysis",
            "dispatched",
        }:
            stage = "critical_review"
        row: dict[str, Any] = {
            "event_id": f"dispatch-event:{event_sha}",
            "event_type": "stage_transition",
            "event": event_name,
            "sequence": sequence,
            "stage": stage,
            "status": status,
            "changed_at": occurred_at,
            "generation": attempt,
            "fence": fence,
            "event_sha256": event_sha,
        }
        error_code = _safe_public_text(event.get("error_code"), limit=120)
        if error_code and re.fullmatch(r"[A-Za-z0-9._:-]+", error_code):
            row["error_code"] = error_code
        event_artifacts = event.get("artifacts")
        if isinstance(event_artifacts, Mapping):
            for artifact_key in ("checkpoint_sha256", "receipt_sha256"):
                artifact_sha = _detail_sha(event_artifacts.get(artifact_key))
                if artifact_sha is not None:
                    row[artifact_key] = artifact_sha
        events.append(row)
        raw_events.append(event)
    return events, raw_events


def _verify_completion_authority(
    runtime_root: Path,
    subject: str,
    capture_id: str,
    expected_release_id: str | None,
    *,
    expected_unit_sha256: str | None = None,
    expected_generation: int | None = None,
    expected_input_fingerprint: str | None = None,
) -> Mapping[str, Any] | None:
    """Call the standalone dispatcher verifier when the merged runtime provides it."""

    try:
        import importlib.util

        module_path = BASE_DIR / "lib" / "concurrent_dispatch.py"
        spec = importlib.util.spec_from_file_location(
            "study_intake_concurrent_dispatch_dashboard_verify", module_path
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_authoritative_completion", None)
        if not callable(verifier):
            return None
        value = verifier(
            runtime_root,
            subject,
            capture_id,
            expected_release_id=expected_release_id,
            expected_unit_sha256=expected_unit_sha256,
            expected_generation=expected_generation,
            expected_input_fingerprint=expected_input_fingerprint,
        )
        return value if isinstance(value, Mapping) else None
    except Exception:
        return None


def _stage_lifecycle(events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    lifecycle: dict[str, dict[str, Any]] = {
        "analysis": {"status": "not_started"},
        "critical_review": {"status": "not_started"},
    }
    active_stage = "analysis"
    for event in events:
        name = event.get("event")
        changed_at = event.get("changed_at")
        error_code = event.get("error_code")
        if name in {"model_submitted", "analysis_submitted"}:
            active_stage = "analysis"
            lifecycle["analysis"].update(
                {"status": "running", "started_at": changed_at}
            )
        elif name in {"analysis_completed", "analysis_checkpoint_reused"}:
            lifecycle["analysis"].update(
                {"status": "completed", "finished_at": changed_at}
            )
            if name == "analysis_checkpoint_reused":
                lifecycle["analysis"]["checkpoint_reused"] = True
            active_stage = "critical_review"
        elif name == "critical_started":
            active_stage = "critical_review"
            lifecycle["critical_review"].update(
                {"status": "running", "started_at": changed_at}
            )
        elif name == "critical_completed":
            lifecycle["critical_review"].update(
                {"status": "completed", "finished_at": changed_at}
            )
        elif name == "retry_wait":
            lifecycle[active_stage]["status"] = "waiting_retry"
            lifecycle[active_stage]["finished_at"] = changed_at
            if error_code:
                lifecycle[active_stage]["error_code"] = error_code
        elif name in {"timeout", "failed", "cancel"}:
            lifecycle[active_stage]["status"] = {
                "timeout": "timed_out",
                "failed": "failed",
                "cancel": "cancelled",
            }[str(name)]
            lifecycle[active_stage]["finished_at"] = changed_at
            if error_code:
                lifecycle[active_stage]["error_code"] = error_code
    return lifecycle


def _verify_task_detail_authority(
    runtime_root: Path,
    unit_sha256: str,
    expected_release_id: str,
) -> Mapping[str, Any] | None:
    try:
        import importlib.util

        module_path = BASE_DIR / "lib" / "concurrent_dispatch.py"
        spec = importlib.util.spec_from_file_location(
            "study_intake_concurrent_dispatch_dashboard_detail_verify",
            module_path,
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_authoritative_task_detail", None)
        if not callable(verifier):
            return None
        value = verifier(
            runtime_root,
            unit_sha256,
            expected_release_id=expected_release_id,
        )
        if not isinstance(value, Mapping) or not isinstance(value.get("detail"), Mapping):
            return None
        return value
    except Exception:
        return None


def _verify_content_member_authority(
    runtime_root: Path,
    unit_sha256: str,
    capture_id: str,
    *,
    expected_release_id: str,
    expected_input_fingerprint: str,
) -> Mapping[str, Any] | None:
    """Verify a lightweight alias; never load a prompt or model result."""

    try:
        import importlib.util

        module_path = BASE_DIR / "lib" / "concurrent_dispatch.py"
        spec = importlib.util.spec_from_file_location(
            "study_intake_concurrent_dispatch_dashboard_member_verify",
            module_path,
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(
            module, "verify_content_member_registration", None
        )
        if not callable(verifier):
            return None
        value = verifier(
            runtime_root,
            unit_sha256,
            capture_id,
            expected_release_id=expected_release_id,
            expected_input_fingerprint=expected_input_fingerprint,
        )
        return value if isinstance(value, Mapping) else None
    except Exception:
        return None


def _verify_evidence_readiness_authority(
    runtime_root: Path,
    receipt_sha256: str,
    *,
    expected_release_id: str,
    expected_capture_id: str,
) -> Mapping[str, Any] | None:
    try:
        import importlib.util

        module_path = BASE_DIR / "lib" / "concurrent_dispatch.py"
        spec = importlib.util.spec_from_file_location(
            "study_intake_concurrent_dispatch_dashboard_readiness_verify",
            module_path,
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_evidence_readiness", None)
        if not callable(verifier):
            return None
        value = verifier(
            runtime_root,
            receipt_sha256,
            expected_release_id=expected_release_id,
            expected_capture_id=expected_capture_id,
        )
        return value if isinstance(value, Mapping) else None
    except Exception:
        return None


def _verify_analysis_checkpoint_authority(
    runtime_root: Path,
    checkpoint_sha256: str,
    *,
    expected_unit_sha256: str,
    expected_frozen_payload_sha256: str,
    expected_release_id: str,
) -> Mapping[str, Any] | None:
    try:
        import importlib.util

        module_path = BASE_DIR / "lib" / "concurrent_dispatch.py"
        spec = importlib.util.spec_from_file_location(
            "study_intake_concurrent_dispatch_dashboard_checkpoint_verify",
            module_path,
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_analysis_checkpoint", None)
        if not callable(verifier):
            return None
        value = verifier(
            runtime_root,
            checkpoint_sha256,
            expected_unit_sha256=expected_unit_sha256,
            expected_frozen_payload_sha256=expected_frozen_payload_sha256,
            expected_release_id=expected_release_id,
        )
        return value if isinstance(value, Mapping) else None
    except Exception:
        return None


def _protected_field_label(path: str) -> str:
    normalized = path.lower().replace("-", "_")
    if any(fragment in normalized for fragment in FORBIDDEN_DETAIL_KEY_FRAGMENTS):
        return f"protected_field_sha256:{sha256_bytes(path.encode('utf-8'))}"
    bounded = path[:160]
    return bounded if re.fullmatch(r"[A-Za-z0-9_.:\[\]-]+", bounded) else (
        f"field_sha256:{sha256_bytes(path.encode('utf-8'))}"
    )


def _leaf_fingerprints(
    value: Any,
    *,
    prefix: str = "root",
    result: dict[str, str] | None = None,
) -> dict[str, str]:
    output = result if result is not None else {}
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            _leaf_fingerprints(
                value[key],
                prefix=f"{prefix}.{key}",
                result=output,
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _leaf_fingerprints(
                item,
                prefix=f"{prefix}[{index}]",
                result=output,
            )
    else:
        output[prefix] = sha256_bytes(canonical_bytes(value))
    return output


def _structured_stage_summary(
    value: Any,
    *,
    source: str = "authoritative_package",
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    leaves = _leaf_fingerprints(value)
    safe_fields = sorted({_protected_field_label(str(key)) for key in value})[:80]
    return {
        "status": "completed",
        "payload_sha256": sha256_bytes(canonical_bytes(value)),
        "top_level_fields": safe_fields,
        "leaf_count": len(leaves),
        "content_policy": "protected_structure_only",
        "source": source,
    }


def _comparison_payloads(
    subject: str,
    analysis: Any,
    critical: Any,
) -> tuple[Any, Any, Mapping[str, Any] | None]:
    if not isinstance(analysis, Mapping) or not isinstance(critical, Mapping):
        return None, None, None
    if subject in {"math", "cs408"}:
        revised = critical.get("revised_analysis")
        return (
            analysis,
            revised,
            dict(revised) if isinstance(revised, Mapping) else None,
        )
    if subject == "english":
        original_items = analysis.get("items")
        revised_items = critical.get("revised_items")
        if not isinstance(original_items, list) or not isinstance(revised_items, list):
            return None, None, None
        return original_items, revised_items, {"items": revised_items}
    return None, None, None


def _deterministic_stage_diff(before_value: Any, after_value: Any) -> dict[str, Any]:
    if not isinstance(before_value, (Mapping, list)) or not isinstance(
        after_value, (Mapping, list)
    ):
        return {
            "status": "unconfirmed",
            "fields": [],
            "reason": "authoritative_two_stage_package_unavailable",
        }
    before = _leaf_fingerprints(before_value)
    after = _leaf_fingerprints(after_value)
    missing = sha256_bytes(canonical_bytes({"status": "missing"}))
    fields = []
    compared_count = 0
    unchanged_count = 0
    for path in sorted(set(before) | set(after)):
        compared_count += 1
        before_sha = before.get(path, missing)
        after_sha = after.get(path, missing)
        changed = before_sha != after_sha
        if not changed:
            unchanged_count += 1
            continue
        fields.append(
            {
                "field": _protected_field_label(path),
                "before_sha256": before_sha,
                "after_sha256": after_sha,
                "changed": True,
            }
        )
    return {
        "status": "confirmed",
        "fields": fields,
        "compared_count": compared_count,
        "changed_count": len(fields),
        "unchanged_count": unchanged_count,
        "reason": "local_recursive_sha256_diff",
    }


def _cs408_visual_summary(
    package: Any,
    authoritative_result: Mapping[str, Any] | None,
    *,
    runtime_root: Path,
    capture_id: str,
    release_id: str | None,
) -> dict[str, Any]:
    if not isinstance(package, Mapping):
        return {
            "status": "unconfirmed",
            "role_counts": {},
            "role_order": [],
            "validation_status": "unconfirmed",
            "reason": "authoritative_package_unavailable",
        }
    task = package.get("task")
    frozen = task.get("frozen_payload") if isinstance(task, Mapping) else None
    if not isinstance(frozen, Mapping):
        frozen = {}
    model_input = frozen.get("model_input")
    current_evidence = (
        model_input.get("current_question_evidence")
        if isinstance(model_input, Mapping)
        else None
    )
    attachment_rows = (
        current_evidence.get("attachment_objects")
        if isinstance(current_evidence, Mapping)
        else None
    )
    role_order: list[str] = []
    if isinstance(attachment_rows, list):
        for row in attachment_rows[:32]:
            role = row.get("role") if isinstance(row, Mapping) else None
            if role in {"question_image", "solution_image"}:
                role_order.append(str(role))
    contract = frozen.get("dispatch_contract")
    readiness_sha = (
        contract.get("evidence_readiness_receipt_sha256")
        if isinstance(contract, Mapping)
        else None
    )
    readiness = None
    if (
        isinstance(readiness_sha, str)
        and SHA256.fullmatch(readiness_sha)
        and isinstance(release_id, str)
    ):
        readiness = _verify_evidence_readiness_authority(
            runtime_root,
            readiness_sha,
            expected_release_id=release_id,
            expected_capture_id=capture_id,
        )
    authority_receipt = (
        readiness.get("authority_receipt")
        if isinstance(readiness, Mapping)
        else None
    )
    receipt = (
        authority_receipt.get("receipt")
        if isinstance(authority_receipt, Mapping)
        else None
    )
    if not role_order and isinstance(receipt, Mapping):
        present = receipt.get("present_roles")
        if isinstance(present, list):
            role_order = [
                str(role)
                for role in present
                if role in {"question_image", "solution_image"}
            ]
    role_counts = {
        role: role_order.count(role)
        for role in ("question_image", "solution_image")
    }
    return {
        "status": "confirmed",
        "role_counts": role_counts,
        "role_order": role_order,
        "validation_status": (
            "hmac_verified" if readiness is not None else "package_verified"
        ),
        "question_mode": (
            receipt.get("question_mode")
            if isinstance(receipt, Mapping)
            else (
                current_evidence.get("question_mode")
                if isinstance(current_evidence, Mapping)
                else "unknown"
            )
        ),
        "required_roles": (
            receipt.get("required_roles") if isinstance(receipt, Mapping) else []
        ),
        "missing_roles": (
            receipt.get("missing_roles") if isinstance(receipt, Mapping) else []
        ),
        "reason": "authoritative_package_role_projection",
    }


def _dispatch_stage_receipts(
    receipt: Mapping[str, Any] | None,
) -> tuple[dict[str, dict[str, Any]], str | None, str | None, bool]:
    observed = receipt.get("observed_stage_runtime") if isinstance(receipt, Mapping) else None
    model_contract = receipt.get("model_contract") if isinstance(receipt, Mapping) else None
    if not isinstance(observed, Mapping):
        return {}, None, None, False
    requested_model = (
        model_contract.get("model")
        if isinstance(model_contract, Mapping)
        else REQUIRED_MODEL
    )
    requested_effort = (
        model_contract.get("reasoning_effort")
        if isinstance(model_contract, Mapping)
        else REQUIRED_REASONING_EFFORT
    )
    projected: dict[str, dict[str, Any]] = {}
    for stage in ("analysis", "critical_review"):
        runtime = observed.get(stage)
        if not isinstance(runtime, Mapping):
            continue
        row: dict[str, Any] = {
            "status": "completed",
            "requested_model": requested_model,
            "requested_reasoning_effort": requested_effort,
            "runtime_identity_status": "requested_unverified",
        }
        duration = _safe_non_negative_integer(runtime.get("duration_ms"))
        if duration is not None:
            row["duration_ms"] = duration
        for key in (
            "semantic_stage_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "model_call_count",
            "consumed_terminal_duplicate_read_count",
        ):
            value = _safe_non_negative_integer(runtime.get(key))
            if value is not None:
                row[key] = value
        read_session_id = _safe_public_text(
            runtime.get("read_session_id"), limit=160
        )
        if read_session_id is not None:
            row["read_session_id"] = read_session_id
        provenance = _safe_public_text(
            runtime.get("runtime_metadata_provenance"), limit=160
        )
        if provenance is not None:
            row["observed_identity_provenance"] = provenance
        projected[stage] = row
    return projected, None, None, False


MATH_PIPELINE_STAGE_KEYS = (
    "evidence_assembly",
    "model_mcp_investigation",
    "knowledge_error_extraction",
    "relationship_retrieval",
    "independent_review",
    "sol_candidate",
)


def _math_pipeline_pending_summary(
    package: Mapping[str, Any] | None,
    *,
    phase: str,
    error_code: str | None = None,
    stage_statuses: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    task = package.get("task") if isinstance(package, Mapping) else None
    frozen = task.get("frozen_payload") if isinstance(task, Mapping) else None
    model_input = frozen.get("model_input") if isinstance(frozen, Mapping) else None
    model_input = model_input if isinstance(model_input, Mapping) else {}
    source_bundle = model_input.get("source_bundle")
    legacy_snapshot = model_input.get("knowledge_distribution_snapshot")
    investigation_stage = (
        "knowledge_snapshot"
        if isinstance(legacy_snapshot, Mapping)
        else "model_mcp_investigation"
    )
    stage_keys = tuple(
        investigation_stage if key == "model_mcp_investigation" else key
        for key in MATH_PIPELINE_STAGE_KEYS
    )
    evidence_ready = isinstance(source_bundle, Mapping)
    stage_status = {
        "evidence_assembly": "completed" if evidence_ready else "pending",
        investigation_stage: (
            "completed" if isinstance(legacy_snapshot, Mapping) else "pending"
        ),
        "knowledge_error_extraction": (
            "running" if phase in {"analysis", "critical_review", "publishing"} else "pending"
        ),
        "relationship_retrieval": "pending",
        "independent_review": (
            "running" if phase in {"critical_review", "publishing"} else "pending"
        ),
        "sol_candidate": "pending",
    }
    lifecycle = stage_statuses if isinstance(stage_statuses, Mapping) else {}
    analysis = lifecycle.get("analysis")
    critical = lifecycle.get("critical_review")
    analysis_status = (
        analysis.get("status") if isinstance(analysis, Mapping) else None
    )
    critical_status = (
        critical.get("status") if isinstance(critical, Mapping) else None
    )
    if analysis_status in {"running", "completed", "failed"}:
        stage_status["evidence_assembly"] = "completed"
        stage_status[investigation_stage] = analysis_status
        stage_status["knowledge_error_extraction"] = analysis_status
    if analysis_status == "completed" and critical_status in {
        "running", "completed", "failed",
    }:
        stage_status["relationship_retrieval"] = "completed"
        stage_status["independent_review"] = critical_status
    if error_code is not None:
        if "failed" not in stage_status.values():
            first_pending = next(
                (
                    key
                    for key in stage_keys
                    if stage_status[key] != "completed"
                ),
                "sol_candidate",
            )
            stage_status[first_pending] = "failed"
    stages = [
        {
            "stage": key,
            "status": stage_status[key],
            "item_count": 0,
        }
        for key in stage_keys
    ]
    result: dict[str, Any] = {
        "status": "failed" if error_code else "pending",
        "source_type": _canonical_math_source_type(
            source_bundle.get("source_kind")
            if isinstance(source_bundle, Mapping)
            else None
        ),
        "relationship_mode": "SHADOW",
        "candidate_file_validation": "unavailable",
        "runtime_identity": "requested_unverified",
        "stages": stages,
        "formal_write_count": 0,
    }
    if error_code is not None:
        result["error_code"] = error_code
    return result


def _legacy_math_pipeline_v3_summary(
    core_package: Mapping[str, Any],
    report: Mapping[str, Any],
    quality: Mapping[str, Any],
    *,
    capture_id: str,
    report_sha: str,
) -> dict[str, Any]:
    """Render an already published v3 package without treating it as v4 evidence."""

    snapshot = report.get("knowledge_distribution_snapshot")
    context = report.get("relationship_context")
    source_binding = report.get("source_binding")
    inventory = report.get("evidence_inventory")
    extraction = report.get("knowledge_and_error_extraction")
    decisions = report.get("relationship_decisions")
    if (
        report.get("schema_version") != "study-intake-luna-math-candidate-v3"
        or report.get("capture_id") != capture_id
        or report.get("formal_write_count") != 0
        or report.get("relationship_mode") != "SHADOW"
        or not isinstance(snapshot, Mapping)
        or not isinstance(context, Mapping)
        or not isinstance(source_binding, Mapping)
        or not isinstance(inventory, Mapping)
        or not isinstance(extraction, Mapping)
        or not isinstance(decisions, list)
    ):
        raise TaskDetailError("math_pipeline_candidate_contract_invalid")
    snapshot_core = {
        key: value for key, value in snapshot.items() if key != "snapshot_sha256"
    }
    context_core = {
        key: value
        for key, value in context.items()
        if key != "relationship_context_sha256"
    }
    if (
        snapshot.get("snapshot_sha256") != _content_value_sha256(snapshot_core)
        or context.get("relationship_context_sha256")
        != _content_value_sha256(context_core)
        or source_binding.get("knowledge_snapshot_sha256")
        != snapshot.get("snapshot_sha256")
        or source_binding.get("knowledge_source_set_sha256")
        != snapshot.get("source_set_sha256")
        or source_binding.get("relationship_context_sha256")
        != context.get("relationship_context_sha256")
        or core_package.get("report_json_sha256") != report_sha
        or quality.get("knowledge_snapshot_sha256")
        != snapshot.get("snapshot_sha256")
        or quality.get("knowledge_source_set_sha256")
        != snapshot.get("source_set_sha256")
        or quality.get("relationship_context_sha256")
        != context.get("relationship_context_sha256")
    ):
        raise TaskDetailError("math_pipeline_candidate_binding_invalid")
    candidates = context.get("candidates")
    if not isinstance(candidates, list) or len(candidates) > 5:
        raise TaskDetailError("math_pipeline_relationship_context_invalid")
    artifacts = inventory.get("artifacts")
    text_sources = inventory.get("text_sources")
    artifacts = artifacts if isinstance(artifacts, list) else []
    text_sources = text_sources if isinstance(text_sources, list) else []
    extraction_count = sum(
        len(value) if isinstance(value, list) else int(bool(value))
        for value in extraction.values()
    )
    corpus = snapshot.get("corpus_stats")
    card_count = (
        int(corpus.get("card_count") or 0) if isinstance(corpus, Mapping) else 0
    )
    return {
        "status": "completed",
        "source_type": _canonical_math_source_type(inventory.get("source_kind")),
        "relationship_mode": "SHADOW",
        "candidate_file_validation": "content_hash_verified",
        "candidate_file_sha256": report_sha,
        "knowledge_source_set_sha256": snapshot.get("source_set_sha256"),
        "relationship_context_sha256": context.get("relationship_context_sha256"),
        "runtime_identity": "requested_unverified",
        "stages": [
            {"stage": "evidence_assembly", "status": "completed", "item_count": len(artifacts) + len(text_sources) + 1},
            {"stage": "knowledge_snapshot", "status": "completed", "item_count": card_count},
            {"stage": "knowledge_error_extraction", "status": "completed", "item_count": extraction_count},
            {"stage": "relationship_retrieval", "status": "completed", "item_count": len(candidates)},
            {"stage": "independent_review", "status": "completed", "item_count": len(decisions)},
            {"stage": "sol_candidate", "status": "completed", "item_count": 1},
        ],
        "formal_write_count": 0,
    }


def _math_pipeline_summary(
    runtime_root: Path,
    capture_id: str,
    package: Mapping[str, Any] | None,
    *,
    phase: str,
    stage_statuses: Mapping[str, Any] | None = None,
    failure_error_code: str | None = None,
    expected_input_fingerprint: str | None = None,
    expected_processing_contract_sha256: str | None = None,
    expected_release_id: str | None = None,
) -> dict[str, Any]:
    """Project the six math stages without making a task failure global."""

    if SAFE_ID.fullmatch(capture_id) is None:
        return _math_pipeline_pending_summary(
            package,
            phase=phase,
            error_code="math_pipeline_capture_id_invalid",
            stage_statuses=stage_statuses,
        )
    pointer_paths = (
        runtime_root / "shadow" / "state" / "latest" / "math" / f"{capture_id}.json",
        runtime_root / "state" / "latest" / "math" / f"{capture_id}.json",
    )
    try:
        pointer = next(
            (
                _read_bounded_json(path, max_bytes=128 * 1024)
                for path in pointer_paths
                if path.is_file()
            ),
            None,
        )
        if not isinstance(pointer, Mapping):
            return _math_pipeline_pending_summary(
                package,
                phase=phase,
                error_code=failure_error_code,
                stage_statuses=stage_statuses,
            )
        if (
            expected_input_fingerprint is not None
            and pointer.get("input_fingerprint") != expected_input_fingerprint
        ):
            raise TaskDetailError("math_pipeline_pointer_generation_mismatch")
        if (
            expected_processing_contract_sha256 is not None
            and pointer.get("processing_contract_sha256")
            != expected_processing_contract_sha256
        ):
            raise TaskDetailError("math_pipeline_pointer_generation_mismatch")
        if (
            expected_release_id is not None
            and pointer.get("authority_release_id") != expected_release_id
        ):
            raise TaskDetailError("math_pipeline_pointer_generation_mismatch")
        package_sha = _detail_sha(pointer.get("package_sha256"), required=True)
        pointer_schema = pointer.get("schema_version")
        if pointer_schema == "study-intake-math-shadow-latest-v1":
            core_package_path = (
                runtime_root
                / "shadow"
                / "packages"
                / "objects"
                / f"{package_sha}.json"
            )
        elif pointer_schema == "study-intake-preprocess-latest-v2":
            core_package_path = (
                runtime_root / "packages" / "objects" / f"{package_sha}.json"
            )
        else:
            raise TaskDetailError("math_pipeline_pointer_schema_invalid")
        core_package = _read_bounded_json(
            core_package_path, max_bytes=MAX_TASK_DETAIL_BYTES
        )
        raw_package = core_package_path.read_bytes()
        if sha256_bytes(raw_package) != package_sha:
            raise TaskDetailError("math_pipeline_package_hash_invalid")
        report_sha = _detail_sha(
            core_package.get("report_json_sha256"), required=True
        )
        quality = core_package.get("quality_receipt")
        if (
            core_package.get("schema_version")
            != "study-intake-preprocess-package-v2"
            or core_package.get("subject") != "math"
            or core_package.get("capture_id") != capture_id
            or (
                expected_input_fingerprint is not None
                and core_package.get("input_fingerprint")
                != expected_input_fingerprint
            )
            or (
                expected_processing_contract_sha256 is not None
                and core_package.get("processing_contract_sha256")
                != expected_processing_contract_sha256
            )
            or core_package.get("formal_write_count") != 0
            or pointer.get("report_json_sha256") not in {None, report_sha}
            or not isinstance(quality, Mapping)
            or quality.get("formal_write_count") != 0
            or core_package.get("quality_receipt_sha256")
            != _content_value_sha256(quality)
            or quality.get("candidate_schema_version")
            not in {
                "study-intake-luna-math-candidate-v3",
                "study-intake-luna-math-candidate-v4",
            }
        ):
            raise TaskDetailError("math_pipeline_package_binding_invalid")
        report_path = (
            runtime_root
            / "private"
            / "reports"
            / "objects"
            / f"{report_sha}.json"
        )
        report = _read_bounded_json(report_path, max_bytes=MAX_TASK_DETAIL_BYTES)
        raw_report = report_path.read_bytes()
        if sha256_bytes(raw_report) != report_sha:
            raise TaskDetailError("math_pipeline_candidate_hash_invalid")
        if report.get("schema_version") == "study-intake-luna-math-candidate-v3":
            return _legacy_math_pipeline_v3_summary(
                core_package,
                report,
                quality,
                capture_id=capture_id,
                report_sha=report_sha,
            )
        context = report.get("relationship_context")
        mcp_evidence = report.get("model_selected_mcp_evidence")
        source_binding = report.get("source_binding")
        inventory = report.get("evidence_inventory")
        extraction = report.get("knowledge_and_error_extraction")
        decisions = report.get("relationship_decisions")
        receipts = report.get("stage_receipts")
        if (
            report.get("schema_version")
            != "study-intake-luna-math-candidate-v4"
            or report.get("capture_id") != capture_id
            or report.get("formal_write_count") != 0
            or report.get("relationship_mode") != "SHADOW"
            or not isinstance(context, Mapping)
            or not isinstance(mcp_evidence, Mapping)
            or not isinstance(source_binding, Mapping)
            or not isinstance(inventory, Mapping)
            or not isinstance(extraction, Mapping)
            or not isinstance(decisions, list)
            or not isinstance(receipts, Mapping)
        ):
            raise TaskDetailError("math_pipeline_candidate_contract_invalid")
        context_core = {
            key: value
            for key, value in context.items()
            if key != "relationship_context_sha256"
        }
        if (
            context.get("relationship_context_sha256")
            != _content_value_sha256(context_core)
            or source_binding.get("relationship_context_sha256")
            != context.get("relationship_context_sha256")
            or source_binding.get("host_semantic_prefetch") is not False
            or mcp_evidence.get("host_semantic_prefetch") is not False
            or core_package.get("report_json_sha256") != report_sha
            or quality.get("relationship_context_sha256")
            != context.get("relationship_context_sha256")
            or quality.get("host_semantic_prefetch") is not False
            or core_package.get("host_semantic_prefetch") is not False
            or core_package.get("consumed_terminal_duplicate_read_count") != 0
        ):
            raise TaskDetailError("math_pipeline_candidate_binding_invalid")
        candidates = context.get("candidates")
        if not isinstance(candidates, list) or len(candidates) > 240:
            raise TaskDetailError("math_pipeline_relationship_context_invalid")
        artifacts = inventory.get("artifacts")
        text_sources = inventory.get("text_sources")
        artifacts = artifacts if isinstance(artifacts, list) else []
        text_sources = text_sources if isinstance(text_sources, list) else []
        extraction_count = sum(
            len(value) if isinstance(value, list) else int(bool(value))
            for value in extraction.values()
        )
        transcript_count = len(core_package.get("mcp_stage_transcripts") or {})
        return {
            "status": "completed",
            "source_type": _canonical_math_source_type(
                inventory.get("source_kind")
            ),
            "relationship_mode": "SHADOW",
            "candidate_file_validation": "content_hash_verified",
            "candidate_file_sha256": report_sha,
            "relationship_context_sha256": context.get(
                "relationship_context_sha256"
            ),
            "runtime_identity": "requested_unverified",
            "read_session_id": core_package.get("read_session_id"),
            "model_call_count": core_package.get("model_call_count"),
            "mcp_tool_call_count": core_package.get("mcp_tool_call_count"),
            "mcp_stage_transcript_count": transcript_count,
            "consumed_terminal_duplicate_read_count": 0,
            "stages": [
                {
                    "stage": "evidence_assembly",
                    "status": "completed",
                    "item_count": len(artifacts) + len(text_sources) + 1,
                },
                {
                    "stage": "model_mcp_investigation",
                    "status": "completed",
                    "item_count": int(core_package.get("mcp_tool_call_count") or 0),
                },
                {
                    "stage": "knowledge_error_extraction",
                    "status": "completed",
                    "item_count": extraction_count,
                },
                {
                    "stage": "relationship_retrieval",
                    "status": "completed",
                    "item_count": len(candidates),
                },
                {
                    "stage": "independent_review",
                    "status": "completed",
                    "item_count": len(decisions),
                },
                {
                    "stage": "sol_candidate",
                    "status": "completed",
                    "item_count": 1,
                },
            ],
            "formal_write_count": 0,
        }
    except (OSError, UnicodeError, json.JSONDecodeError, TaskDetailError) as exc:
        code = str(exc) if isinstance(exc, TaskDetailError) else "math_pipeline_dependency_unreadable"
        return _math_pipeline_pending_summary(
            package,
            phase=phase,
            error_code=code,
            stage_statuses=stage_statuses,
        )


def _public_dispatch_task_detail(
    detail: Mapping[str, Any],
    *,
    item: Mapping[str, Any],
    runtime_root: Path,
    include_raw: bool,
    detail_authority_verified: bool,
    verified_latest_event: Mapping[str, Any] | None = None,
    output_schema_version: str = LEGACY_TASK_DETAIL_SCHEMA_VERSION,
) -> dict[str, Any]:
    if detail.get("schema_version") not in {
        "study-intake-dispatch-task-detail-v1",
        "study-intake-dispatch-task-detail-v2",
    }:
        raise TaskDetailError("task_detail_schema_mismatch")
    if _contains_forbidden_detail_key(detail) or detail.get("formal_write_count") != 0:
        raise TaskDetailError("task_detail_private_or_write_conflict")
    subject = str(item.get("subject"))
    capture_id = str(item.get("capture_id"))
    unit_sha = _detail_sha(detail.get("unit_sha256"), required=True)
    frozen_sha = _detail_sha(detail.get("frozen_payload_sha256"), required=True)
    release_id = _text(detail.get("release_id"), limit=64)
    if release_id is not None and RELEASE_ID.fullmatch(release_id) is None:
        raise TaskDetailError("task_detail_release_invalid")
    if item.get("release_id") != release_id:
        raise TaskDetailError("task_detail_generation_conflict")
    item_input_fingerprint = _text(
        item.get("input_fingerprint"), limit=320
    )
    member_authority = (
        _verify_content_member_authority(
            runtime_root,
            unit_sha,
            capture_id,
            expected_release_id=release_id,
            expected_input_fingerprint=item_input_fingerprint,
        )
        if isinstance(release_id, str)
        and isinstance(item_input_fingerprint, str)
        else None
    )
    member_registration = (
        member_authority.get("registration")
        if isinstance(member_authority, Mapping)
        and isinstance(member_authority.get("registration"), Mapping)
        else None
    )
    member_alias_verified = bool(
        isinstance(member_registration, Mapping)
        and member_registration.get("unit_sha256") == unit_sha
        and member_registration.get("subject") == subject
        and member_registration.get("capture_id") == capture_id
        and member_registration.get("release_id") == release_id
        and member_registration.get("input_fingerprint")
        == item_input_fingerprint
    )
    if (
        detail.get("subject") != subject
        or (
            detail.get("capture_id") != capture_id
            and not member_alias_verified
        )
        or item.get("unit_sha256") != unit_sha
        or (
            item.get("frozen_payload_sha256") != frozen_sha
            and not member_alias_verified
        )
        or item.get("rule_version") != detail.get("rule_version")
        or (
            item.get("rule_version_sha256") is not None
            and detail.get("rule_version_sha256") is not None
            and item.get("rule_version_sha256")
            != detail.get("rule_version_sha256")
        )
    ):
        raise TaskDetailError("task_detail_identity_mismatch")
    fence = _detail_integer(detail.get("fence"), label="fence")
    attempt = _detail_integer(detail.get("attempt"), label="attempt")
    if (
        item.get("generation") != attempt
        or item.get("attempt") != attempt
        or item.get("fence") != fence
    ):
        raise TaskDetailError("task_detail_generation_conflict")
    events, raw_events = _load_dispatch_events(runtime_root, detail)
    authoritative_result = _verify_completion_authority(
        runtime_root,
        subject,
        capture_id,
        release_id,
        expected_unit_sha256=unit_sha,
        expected_generation=attempt,
        expected_input_fingerprint=item_input_fingerprint,
    )
    completion = (
        authoritative_result.get("completion")
        if isinstance(authoritative_result, Mapping)
        and isinstance(authoritative_result.get("completion"), Mapping)
        else authoritative_result
    )
    completion_verified = isinstance(completion, Mapping)
    package = (
        authoritative_result.get("package")
        if isinstance(authoritative_result, Mapping)
        and isinstance(authoritative_result.get("package"), Mapping)
        else None
    )
    receipt = (
        authoritative_result.get("receipt")
        if isinstance(authoritative_result, Mapping)
        and isinstance(authoritative_result.get("receipt"), Mapping)
        else None
    )
    verified_stage_receipts, observed_model, observed_effort, runtime_attested = (
        _dispatch_stage_receipts(receipt)
    )
    if completion_verified and (
        completion.get("unit_sha256") != unit_sha
        or completion.get("lease_fence") != fence
        or completion.get("subject") != subject
        or completion.get("capture_id")
        != (
            detail.get("capture_id")
            if member_alias_verified
            else capture_id
        )
        or completion.get("release_id") != release_id
    ):
        raise TaskDetailError("task_detail_generation_conflict")
    artifacts = detail.get("artifacts")
    if not isinstance(artifacts, Mapping):
        artifacts = {}
    checkpoints: list[dict[str, Any]] = []
    seen_checkpoint_shas: set[str] = set()
    for event in events:
        event_checkpoint = _detail_sha(event.get("checkpoint_sha256"))
        if event_checkpoint is None or event_checkpoint in seen_checkpoint_shas:
            continue
        checkpoint_row = {
            "stage": "analysis",
            "status": "ready",
            "checkpoint_sha256": event_checkpoint,
        }
        event_receipt = _detail_sha(event.get("receipt_sha256"))
        if event_receipt is not None:
            checkpoint_row["receipt_sha256"] = event_receipt
        checkpoints.append(checkpoint_row)
        seen_checkpoint_shas.add(event_checkpoint)
    checkpoint_sha = _detail_sha(artifacts.get("checkpoint_sha256"))
    if checkpoint_sha is not None and checkpoint_sha not in seen_checkpoint_shas:
        checkpoint_row: dict[str, Any] = {
            "stage": "analysis",
            "status": "ready",
            "checkpoint_sha256": checkpoint_sha,
        }
        checkpoint_receipt = _detail_sha(artifacts.get("receipt_sha256"))
        if checkpoint_receipt is not None:
            checkpoint_row["receipt_sha256"] = checkpoint_receipt
        checkpoints.append(checkpoint_row)
    checkpoint_analysis_value = None
    if checkpoints and isinstance(release_id, str):
        checkpoint_verification = _verify_analysis_checkpoint_authority(
            runtime_root,
            str(checkpoints[-1]["checkpoint_sha256"]),
            expected_unit_sha256=unit_sha,
            expected_frozen_payload_sha256=frozen_sha,
            expected_release_id=release_id,
        )
        checkpoint_value = (
            checkpoint_verification.get("checkpoint")
            if isinstance(checkpoint_verification, Mapping)
            else None
        )
        if isinstance(checkpoint_value, Mapping) and isinstance(
            checkpoint_value.get("analysis"), Mapping
        ):
            checkpoint_analysis_value = checkpoint_value["analysis"]
            checkpoints[-1]["authority_status"] = "hmac_verified"
        else:
            checkpoints[-1]["authority_status"] = "unconfirmed"
    declared_integrity = _safe_public_text(detail.get("evidence_integrity"), limit=80)
    authority = detail.get("authority")
    authority_shape_valid = bool(
        isinstance(authority, Mapping)
        and authority.get("algorithm") == "HMAC-SHA256"
        and authority.get("purpose") == "dispatch-task-detail"
        and _detail_sha(authority.get("hmac_sha256"))
    )
    queue_status = _safe_public_text(detail.get("server_queue_status"), limit=40)
    public_queue = {
        "confirmation": "unconfirmed",
        "declared_status": "unknown",
    }
    if (
        isinstance(verified_latest_event, Mapping)
        and verified_latest_event.get("event") == "retry_wait"
        and queue_status in {"rate_limited", "confirmed_rate_limited"}
    ):
        retry_code = _safe_public_text(
            verified_latest_event.get("error_code"), limit=120
        )
        normalized_retry = retry_code.lower() if retry_code else ""
        if normalized_retry.endswith("_rate_limited") or normalized_retry in {
            "luna_service_unavailable",
            "upstream_http_429",
        }:
            public_queue = {
                "confirmation": "confirmed_event",
                "declared_status": "rate_limited",
            }
    phase = _safe_public_text(detail.get("phase"), limit=40) or "unknown"
    outcome = _safe_public_text(
        completion.get("outcome") if completion_verified else None,
        limit=80,
    )
    if completion_verified:
        normalized_outcome = outcome.lower() if outcome is not None else ""
        if "degraded" in normalized_outcome:
            result_status = "degraded"
        elif normalized_outcome in {
            "failed",
            "timeout",
            "timed_out",
            "cancel",
            "cancelled",
            "canceled",
            "rejected",
        }:
            result_status = "failed"
        elif normalized_outcome in {
            "completed",
            "ready",
            "published",
            "success",
            "succeeded",
        }:
            result_status = "ready"
        else:
            # A valid HMAC proves origin, not that an unknown outcome is consumable.
            result_status = "pending"
    elif phase in {"failed", "timeout", "cancel"}:
        result_status = "failed"
    else:
        result_status = "pending"
    consumable = bool(
        completion_verified
        and result_status == "ready"
        and completion.get("package_sha256")
        and completion.get("receipt_sha256")
        and set(verified_stage_receipts) == {"analysis", "critical_review"}
    )
    public_result: dict[str, Any] = {
        "status": result_status,
        "consumable": consumable,
        "consumability_confirmation": (
            "authoritative" if completion_verified else "unconfirmed"
        ),
        "generation": attempt,
        "fence": fence,
        "outcome": outcome or "unknown",
    }
    for target_key, source_key in (
        ("final_sha256", "package_sha256"),
        ("receipt_sha256", "receipt_sha256"),
    ):
        value = _detail_sha(
            completion.get(source_key) if completion_verified else artifacts.get(source_key)
        )
        if value is not None:
            public_result[target_key] = value
    completion_error = _safe_public_text(
        completion.get("error_code") if completion_verified else None,
        limit=120,
    )
    receipt_error = _safe_public_text(
        receipt.get("error_code") if isinstance(receipt, Mapping) else None,
        limit=120,
    )
    error_code = completion_error or receipt_error
    if error_code and re.fullmatch(r"[A-Za-z0-9._:-]+", error_code):
        public_result["error_code"] = error_code
    analysis_value = package.get("analysis") if isinstance(package, Mapping) else None
    critical_value = (
        package.get("critical_review") if isinstance(package, Mapping) else None
    )
    diff_before, diff_after, final_structured_value = _comparison_payloads(
        subject, analysis_value, critical_value
    )
    structured_stages: dict[str, Any] = {}
    for stage_name, stage_value, stage_source in (
        (
            "analysis",
            analysis_value if isinstance(analysis_value, Mapping) else checkpoint_analysis_value,
            (
                "authoritative_package"
                if isinstance(analysis_value, Mapping)
                else "hmac_checkpoint"
            ),
        ),
        ("critical_review", critical_value, "authoritative_package"),
        ("final_result", final_structured_value, "authoritative_package"),
    ):
        summary = _structured_stage_summary(stage_value, source=stage_source)
        if summary is not None:
            structured_stages[stage_name] = summary
    stage_statuses = _stage_lifecycle(events)
    public: dict[str, Any] = {
        "schema_version": LEGACY_TASK_DETAIL_SCHEMA_VERSION,
        "source_schema_version": detail.get("schema_version"),
        "capture_id": capture_id,
        "subject": subject,
        "task_identity": {
            "task_id": unit_sha,
            "unit_sha256": unit_sha,
            "generation": attempt,
            "fence": fence,
            "input_sha256": (
                item.get("frozen_payload_sha256")
                if member_alias_verified
                else frozen_sha
            ),
            "rule_version": detail.get("rule_version"),
        },
        "runtime": {
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "observed_model": observed_model,
            "observed_reasoning_effort": observed_effort,
            "identity_confirmation": (
                "runtime_attested" if runtime_attested else "requested_unverified"
            ),
        },
        "server_queue": public_queue,
        "evidence_integrity": {
            "declared_status": declared_integrity or "unknown",
            "authority_status": (
                "hmac_verified"
                if detail_authority_verified
                else "sealed_unverified" if authority_shape_valid else "unconfirmed"
            ),
        },
        "phase": phase,
        "stage_events": events,
        "stage_statuses": stage_statuses,
        "checkpoints": checkpoints,
        "deterministic_diff": _deterministic_stage_diff(
            diff_before, diff_after
        ),
        "result": public_result,
        "raw_available": bool(
            completion_verified
            and isinstance(analysis_value, Mapping)
            and isinstance(critical_value, Mapping)
            and isinstance(final_structured_value, Mapping)
            and not _contains_local_path(
                [analysis_value, critical_value, final_structured_value]
            )
        ),
        "model_call_count": sum(
            int(row.get("model_call_count") or 0)
            for row in verified_stage_receipts.values()
        ),
        "mcp_tool_call_count": sum(
            int(row.get("mcp_tool_call_count") or 0)
            for row in verified_stage_receipts.values()
        ),
        "provider_request_count": sum(
            int(row.get("provider_request_count") or 0)
            for row in verified_stage_receipts.values()
        ),
        "consumed_terminal_duplicate_read_count": sum(
            int(row.get("consumed_terminal_duplicate_read_count") or 0)
            for row in verified_stage_receipts.values()
        ),
        "formal_write_count": 0,
    }
    warning_codes = [
        code
        for code in item.get("warning_codes", [])
        if isinstance(code, str) and SAFE_ID.fullmatch(code)
    ] if isinstance(item.get("warning_codes"), list) else []
    exact_error = _safe_public_text(
        item.get("exact_error_code") or public_result.get("error_code"),
        limit=160,
    )
    def stage_axis(prefix: str) -> dict[str, Any]:
        execution_status = _safe_public_text(
            item.get(f"{prefix}_execution_status"), limit=40
        ) or "not_started"
        report_status = _safe_public_text(
            item.get(f"{prefix}_report_status"), limit=40
        ) or "not_started"
        stage_warning_codes = (
            warning_codes if report_status == "available_with_warnings" else []
        )
        return {
            "execution_status": execution_status,
            "report_status": report_status,
            "raw_output_sha256": _detail_sha(
                item.get(f"{prefix}_raw_output_sha256")
            ),
            "execution_receipt_sha256": _detail_sha(
                item.get(f"{prefix}_execution_receipt_sha256")
            ),
            "normalization_receipt_sha256": _detail_sha(
                item.get(f"{prefix}_normalization_receipt_sha256")
            ),
            "report_sha256": _detail_sha(item.get(f"{prefix}_report_sha256")),
            "warning_codes": stage_warning_codes,
            "error_code": exact_error if execution_status in {"failed", "cancelled", "stalled"} else None,
        }
    public.update(
        {
            "identity": {
                "subject": subject,
                "capture_id": capture_id,
                "unit_sha256": unit_sha,
                "release_id": release_id,
                "attempt": attempt,
                "fence": fence,
            },
            "dispatch": {
                "status": item.get("local_dispatch_status") or "pending",
                "phase": phase,
            },
            "evidence": {
                "access_status": item.get("evidence_access_status") or "unverified",
                "integrity_status": declared_integrity or "unknown",
            },
            "analysis": stage_axis("analysis"),
            "critical_review": stage_axis("critical_review"),
            "sol_review": {
                "status": item.get("sol_review_status") or "not_eligible",
            },
            "formal_write": {
                "status": item.get("formal_write_status") or "not_authorized",
                "count": 0,
            },
            "progress": {
                "elapsed_runtime_seconds": _safe_non_negative_integer(
                    item.get("elapsed_runtime_seconds")
                ),
                "last_meaningful_progress_at": _safe_public_text(
                    item.get("last_meaningful_progress_at"), limit=64
                ),
                "soft_timeout_warning": item.get("soft_timeout_warning") is True,
                "stall_probe_status": item.get("stall_probe_status") or "not_applicable",
            },
            "terminal": {
                "status": item.get("terminal_status"),
                "error_code": exact_error,
                "receipt_sha256": _detail_sha(item.get("terminal_receipt_sha256")),
                "issued_at": _safe_public_text(
                    item.get("last_state_change_at"), limit=64
                ),
            },
            "artifacts": {
                "package_sha256": _detail_sha(
                    item.get("package_sha256") or public_result.get("final_sha256")
                ),
                "sol_handoff_envelope_sha256": _detail_sha(
                    item.get("sol_handoff_envelope_sha256")
                ),
            },
            "generated_at": _safe_public_text(
                item.get("updated_at") or item.get("last_state_change_at"), limit=64
            ),
        }
    )
    if release_id is not None:
        public["task_identity"]["release_id"] = release_id
    if member_alias_verified:
        content_processing_id = _detail_sha(
            member_registration.get("content_processing_id")
        )
        member_reference_sha256 = _detail_sha(
            member_authority.get("registration_sha256")
        )
        if content_processing_id is not None:
            public["task_identity"][
                "content_processing_id"
            ] = content_processing_id
        if member_reference_sha256 is not None:
            public["task_identity"][
                "member_reference_sha256"
            ] = member_reference_sha256
        public["task_identity"]["shared_execution"] = (
            detail.get("capture_id") != capture_id
        )
    rule_version_sha = _detail_sha(
        detail.get("rule_version_sha256") or item.get("rule_version_sha256")
    )
    if rule_version_sha is not None:
        public["task_identity"]["rule_version_sha256"] = rule_version_sha
    if structured_stages:
        public["structured_stages"] = structured_stages
    if verified_stage_receipts:
        public["stage_receipts"] = verified_stage_receipts
    if subject == "math":
        failure_error_code = (
            public_result.get("error_code")
            if public_result.get("status") == "failed"
            and isinstance(public_result.get("error_code"), str)
            else None
        )
        public["math_pipeline"] = _math_pipeline_summary(
            runtime_root,
            capture_id,
            package,
            phase=phase,
            stage_statuses=stage_statuses,
            failure_error_code=failure_error_code,
            expected_input_fingerprint=item_input_fingerprint,
            expected_processing_contract_sha256=(
                item.get("processing_contract_sha256")
                if isinstance(item.get("processing_contract_sha256"), str)
                else None
            ),
            expected_release_id=release_id,
        )
    if subject == "cs408":
        public["cs408_visual"] = _cs408_visual_summary(
            package,
            authoritative_result,
            runtime_root=runtime_root,
            capture_id=capture_id,
            release_id=release_id,
        )
    if include_raw and public["raw_available"]:
        public["raw_sections"] = [
            {
                "kind": "deterministic_output",
                "label": label,
                "verification": "hmac_verified_package",
                "content_type": "structured_output_not_hidden_reasoning",
                "content": json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                ),
            }
            for label, value in (
                ("Analysis 结构化输出", analysis_value),
                ("Critical review 结构化输出", critical_value),
                ("最终结构化结果", final_structured_value),
            )
        ]
    if output_schema_version != TASK_DETAIL_SCHEMA_VERSION:
        return public
    release = _detail_sha(release_id, required=True)
    input_fingerprint = _detail_sha(item.get("input_fingerprint"), required=True)
    owner_id = _safe_public_text(detail.get("owner_id"), limit=160)
    generated_at = _safe_public_text(detail.get("updated_at"), limit=64)
    if owner_id is None or generated_at is None:
        raise TaskDetailError("task_detail_v2_identity_missing")
    event_sha = _detail_sha(detail.get("latest_event_sha256"))
    raw_stage_name = _safe_public_text(detail.get("stage_name"), limit=40)
    dispatch_stage = (
        raw_stage_name
        if raw_stage_name in {"analysis", "critical_review"}
        else "terminal"
        if item.get("local_dispatch_status") in {"terminal", "cancelled", "stalled"}
        else "packaging"
        if phase in {"packaging", "published", "workflow_complete", "workflow_complete_with_warnings"}
        else "dispatch"
    )
    declared_queue = str(item.get("server_queue_status") or "unknown")
    queue_status_v2 = (
        "confirmed_rate_limited"
        if declared_queue in {"rate_limited", "confirmed_rate_limited"}
        else "clear"
        if declared_queue == "clear"
        else "unknown"
    )
    declared_confirmation = item.get("server_queue_confirmation")
    queue_confirmation_v2 = (
        declared_confirmation
        if queue_status_v2 == "confirmed_rate_limited"
        and declared_confirmation in {"confirmed_event", "provider_receipt_verified"}
        or queue_status_v2 == "clear"
        and declared_confirmation == "provider_receipt_verified"
        else "unconfirmed"
    )
    terminal_status = item.get("terminal_status")
    if terminal_status not in {
        "workflow_complete",
        "workflow_complete_with_warnings",
        "workflow_partial",
        "execution_failed",
        "cancelled",
        "stalled",
    }:
        terminal_status = "not_terminal"
    return {
        "schema_version": TASK_DETAIL_SCHEMA_VERSION,
        "subject": subject,
        "capture_id": capture_id,
        "unit_sha256": unit_sha,
        "release_id": release,
        "attempt": attempt,
        "fence": fence,
        "identity": {
            "owner_id": owner_id,
            "frozen_payload_sha256": frozen_sha,
            "input_fingerprint": input_fingerprint,
        },
        "dispatch": {
            "local_dispatch_status": item.get("local_dispatch_status") or "pending",
            "current_stage": dispatch_stage,
            "last_event_sha256": event_sha,
        },
        "evidence": {
            "evidence_access_status": item.get("evidence_access_status") or "unverified",
            "authority_snapshot_sha256": _detail_sha(
                item.get("authority_snapshot_sha256")
            ),
            "exact_error_code": (
                exact_error
                if item.get("evidence_access_status") in {"missing", "quarantined"}
                else None
            ),
        },
        "analysis": public["analysis"],
        "critical_review": public["critical_review"],
        "server_queue": {
            "status": queue_status_v2,
            "confirmation": queue_confirmation_v2,
            "error_code": exact_error if queue_status_v2 == "confirmed_rate_limited" else None,
        },
        "sol_review": {
            "status": item.get("sol_review_status") or "not_eligible",
            "receipt_sha256": _detail_sha(item.get("sol_decision_sha256")),
        },
        "formal_write": {
            "status": item.get("formal_write_status") or "not_authorized",
            "formal_write_count": 0,
            "receipt_sha256": None,
        },
        "progress": public["progress"],
        "terminal": {
            "status": terminal_status,
            "error_code": exact_error if terminal_status != "not_terminal" else None,
            "receipt_sha256": _detail_sha(item.get("terminal_receipt_sha256")),
            "issued_at": (
                _safe_public_text(item.get("last_state_change_at"), limit=64)
                if terminal_status != "not_terminal"
                else None
            ),
        },
        "artifacts": public["artifacts"],
        "generated_at": generated_at,
    }


def _load_dispatch_task_detail(
    projection_path: Path,
    raw_item: Mapping[str, Any],
    public_item: Mapping[str, Any],
    *,
    include_raw: bool,
    output_schema_version: str = LEGACY_TASK_DETAIL_SCHEMA_VERSION,
) -> dict[str, Any] | None:
    unit_sha = _detail_sha(raw_item.get("unit_sha256"))
    raw_path = _text(raw_item.get("task_detail_path"), limit=2000)
    if unit_sha is None or raw_path is None:
        return None
    runtime_root = projection_path.expanduser().resolve().parent.parent
    path = _expected_runtime_path(
        runtime_root,
        raw_path,
        Path("dispatch/state/task-details") / f"{unit_sha}.json",
    )
    expected_release_id = _text(raw_item.get("release_id"), limit=64)
    verified = (
        _verify_task_detail_authority(runtime_root, unit_sha, expected_release_id)
        if expected_release_id and RELEASE_ID.fullmatch(expected_release_id)
        else None
    )
    detail = (
        verified["detail"]
        if verified is not None
        else _read_bounded_json(path, max_bytes=MAX_TASK_DETAIL_BYTES)
    )
    verified_latest_event = (
        verified.get("latest_event")
        if isinstance(verified, Mapping)
        and isinstance(verified.get("latest_event"), Mapping)
        else None
    )
    return _public_dispatch_task_detail(
        detail,
        item=public_item,
        runtime_root=runtime_root,
        include_raw=include_raw,
        detail_authority_verified=verified is not None,
        verified_latest_event=verified_latest_event,
        output_schema_version=output_schema_version,
    )


def _contains_local_path(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_local_path(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_local_path(item) for item in value)
    if not isinstance(value, str):
        return False
    lowered = value.lower()
    return any(
        marker in lowered
        for marker in (
            "/users/", "file://", "/private/", "/tmp/", "/var/folders/", "~/",
        )
    )


def _item_sort_key(item: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(item.get("updated_at") or item.get("captured_at") or ""),
        str(item.get("capture_id") or ""),
    )


def _filtered_items(
    projection: Mapping[str, Any],
    *,
    study_date: str | None,
    subject: str,
    status: str | None = None,
) -> list[dict[str, Any]]:
    dispatcher = _public_dispatchers(projection).get("english", {})
    worker_online = False
    heartbeat_text = dispatcher.get("last_heartbeat")
    if (
        dispatcher.get("status") == "running"
        and dispatcher.get("paused") is not True
        and isinstance(heartbeat_text, str)
    ):
        try:
            heartbeat = datetime.fromisoformat(
                heartbeat_text.replace("Z", "+00:00")
            )
            age = (
                datetime.now(timezone.utc)
                - heartbeat.astimezone(timezone.utc)
            ).total_seconds()
            worker_online = (
                heartbeat.tzinfo is not None
                and -HEARTBEAT_FUTURE_SKEW_SECONDS
                <= age
                <= HEARTBEAT_READY_MAX_AGE_SECONDS
            )
        except (ValueError, AttributeError):
            worker_online = False
    result = []
    for raw_item in _all_public_items(projection):
        item = dict(raw_item)
        if (
            item.get("subject") == "english"
            and item.get("english_status") == "queued"
        ):
            if worker_online:
                item.pop("display_status", None)
            else:
                item["display_status"] = "queued_worker_offline"
        if study_date is not None and item.get("study_date") != study_date:
            continue
        if subject != "all" and item.get("subject") != subject:
            continue
        if status is not None and item.get("queue_state") != status:
            continue
        result.append(item)
    return sorted(result, key=_item_sort_key, reverse=True)


def _v3_counts(items: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    result = {key: 0 for key in V3_COUNT_KEYS}
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


def _counts(items: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    materialized = list(items)
    result = _v3_counts(materialized)
    legacy_counts = {status: 0 for status in sorted(ALLOWED_LUNA_STATUSES)}
    reviewed = 0
    committed = 0
    for item in materialized:
        status = item.get("luna_status")
        if status in legacy_counts:
            legacy_counts[str(status)] += 1
        if (
            item.get("adoption_status") in RECEIPTED_ADOPTION_STATUSES
            and item.get("adoption_receipt_id")
        ):
            reviewed += 1
        if item.get("sol_committed") is True:
            committed += 1
    counts = {
        **legacy_counts,
        **result,
        "waiting": (
            result["queued"]
            + result["analysis_running"]
            + result["critical_review_running"]
            + result["evidence_pending"]
        ),
        "attention": result["needs_rework"] + result["failed"],
        "failures": result["failed"],
        "reviewed": reviewed,
        "committed": committed,
        "package_ready": result["quality_passed"],
        "processed": result["terminal"],
    }
    counts["remaining"] = (
        result["selected"] - result["terminal"]
    )
    return counts


def _pipeline(
    items: Iterable[Mapping[str, Any]], counts: Mapping[str, int]
) -> dict[str, int]:
    analysis = 0
    critical_review = 0
    for item in items:
        submitted = item.get("local_model_submitted")
        queue_state = str(item.get("queue_state") or "")
        receipts = item.get("stage_receipts")
        analysis_receipt = (
            receipts.get("analysis") if isinstance(receipts, Mapping) else None
        )
        critical_receipt = (
            receipts.get("critical_review")
            if isinstance(receipts, Mapping)
            else None
        )
        if queue_state in {"queued", "evidence_pending"}:
            continue
        if (
            isinstance(analysis_receipt, Mapping)
            or submitted is True
            or submitted is None
        ):
            analysis += 1
        if (
            isinstance(critical_receipt, Mapping)
            or (
                submitted is None
                and queue_state in {"ready", "needs_rework", "failed", "stale"}
            )
        ):
            critical_review += 1
    return {
        "capture": counts.get("selected", 0),
        "analysis": analysis,
        "critical_review": critical_review,
        "quality_ready": counts.get("quality_passed", 0),
        "sol_reviewed": counts.get("reviewed", 0),
        "formal_apply": counts.get("committed", 0),
    }


def _coverage_summary(
    items: Iterable[Mapping[str, Any]],
    *,
    claim_key: str,
    covered_key: str,
    percentage_key: str,
) -> dict[str, Any]:
    item_count = 0
    claim_count = 0.0
    covered_count = 0.0
    percentages: list[float] = []
    for item in items:
        claims = _safe_non_negative_number(item.get(claim_key))
        covered = _safe_non_negative_number(item.get(covered_key))
        percentage = _safe_percentage(item.get(percentage_key))
        if claims is None and covered is None and percentage is None:
            continue
        item_count += 1
        if claims is not None and covered is not None and covered <= claims:
            claim_count += float(claims)
            covered_count += float(covered)
        elif percentage is not None:
            percentages.append(float(percentage))
    result: dict[str, Any] = {"items_reported": item_count}
    if claim_count > 0:
        result.update({
            "claims": int(claim_count),
            "claims_with_refs": int(covered_count),
            "coverage_pct": round((covered_count / claim_count) * 100, 1),
        })
    elif percentages:
        result["coverage_pct"] = round(sum(percentages) / len(percentages), 1)
    return result


def _observability(
    items: list[Mapping[str, Any]], worker: Mapping[str, Any]
) -> dict[str, Any]:
    adoption = {
        "direct": 0,
        "minor": 0,
        "major": 0,
        "legacy_modified": 0,
        "rejected": 0,
        "fallback": 0,
        "unknown": 0,
    }
    receipted = 0
    stage_receipted = 0
    quality_receipted = 0
    identity_counts: dict[str, int] = {}
    confirmed_models: dict[tuple[str, str], int] = {}
    failure_codes: dict[str, int] = {}
    two_pass_ready = 0
    single_pass_degraded = 0
    processed = 0
    visual_required = 0

    terminal_statuses = {
        "ready", "two_pass_ready", "single_pass_degraded", "failed", "stale", "skipped",
    }
    for item in items:
        status = str(item.get("luna_status") or "")
        pipeline_status = str(
            item.get("pipeline_status") or item.get("two_stage_status") or status
        )
        if status in terminal_statuses:
            processed += 1
            identity = str(item.get("runtime_identity_status") or "unavailable")
            identity_counts[identity] = identity_counts.get(identity, 0) + 1
            if identity == "confirmed":
                model = str(item.get("runtime_model") or "")
                effort = str(item.get("runtime_reasoning_effort") or "")
                if model and effort:
                    key = (model, effort)
                    confirmed_models[key] = confirmed_models.get(key, 0) + 1
        if pipeline_status == "two_pass_ready":
            two_pass_ready += 1
        elif pipeline_status == "single_pass_degraded":
            single_pass_degraded += 1

        bucket = str(item.get("adoption_bucket") or "unknown")
        if bucket not in adoption:
            bucket = "unknown"
        adoption[bucket] += 1
        if bucket != "unknown" and item.get("adoption_receipt_id"):
            receipted += 1
        if item.get("stage_receipts"):
            stage_receipted += 1
        if item.get("quality_receipt_sha256"):
            quality_receipted += 1
        if item.get("visual_evidence_required") is True or item.get("visual_required") is True:
            visual_required += 1
        error_code = item.get("last_error_code")
        if status == "failed" and isinstance(error_code, str):
            failure_codes[error_code] = failure_codes.get(error_code, 0) + 1

    evidence = _coverage_summary(
        items,
        claim_key="evidence_claim_count",
        covered_key="evidence_claims_with_refs",
        percentage_key="evidence_coverage_pct",
    )
    visual = _coverage_summary(
        items,
        claim_key="visual_claim_count",
        covered_key="visual_claims_with_refs",
        percentage_key="visual_coverage_pct",
    )
    visual["required_items"] = visual_required
    runtime = {
        "requested_model": worker.get("requested_model"),
        "requested_reasoning_effort": worker.get("requested_reasoning_effort"),
        "identity_counts": identity_counts,
        "confirmed_models": [
            {"model": model, "reasoning_effort": effort, "items": count}
            for (model, effort), count in sorted(confirmed_models.items())
        ],
    }
    return {
        "processed": processed,
        "remaining": max(0, len(items) - processed),
        "two_pass": {
            "ready": two_pass_ready,
            "single_pass_degraded": single_pass_degraded,
        },
        "runtime": runtime,
        "evidence": evidence,
        "visual": visual,
        "adoption": {**adoption, "receipted": receipted},
        "receipts": {
            "adoption": receipted,
            "stage": stage_receipted,
            "quality": quality_receipted,
        },
        "failures": {
            "total": sum(failure_codes.values()),
            "by_code": failure_codes,
        },
    }


PUBLIC_METRIC_FIELDS = (
    "trial_days",
    "targets",
    "visible_targets",
    "control_targets",
    "direct_adopted",
    "modified_adopted",
    "minor_edit_adopted",
    "major_edit_adopted",
    "rejected",
    "fallback",
    "critical_errors",
    "evidence_reference_rate",
    "evidence_claim_precision",
    "visual_evidence_coverage_pct",
    "first_break_accuracy_pct",
    "omission_rate",
    "median_sol_review_seconds",
    "baseline_median_sol_review_seconds",
    "review_time_delta_pct",
    "source_read_delta_pct",
    "ready_within_10m_pct",
)


def _public_metrics(section: Mapping[str, Any]) -> dict[str, dict[str, int | float | str]]:
    raw = section.get("metrics")
    sources = section.get("metric_sources")
    if not isinstance(raw, Mapping) or not isinstance(sources, Mapping):
        return {}
    result: dict[str, dict[str, int | float | str]] = {}
    for key in PUBLIC_METRIC_FIELDS:
        value = _number(raw.get(key))
        source = _safe_ref(sources.get(key))
        if value is not None and source is not None:
            result[key] = {"value": value, "source": source}
    return result


def _public_subject_concurrency(section: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "concurrency_status": "unavailable",
        "concurrency_state": "canary_not_configured",
        "concurrency_source": None,
        "concurrency_observed_at": None,
        "concurrency_error_code": "concurrency_projection_unavailable",
        "subject_active_task_count": None,
        "subject_pending_task_count": None,
        "subject_peak_active": None,
        "subject_verified_runner_active_task_count": None,
        "subject_runner_evidenced_task_count": None,
        "subject_runner_interval_missing_count": None,
        "scheduler_claim_subject_peak_active": None,
        "terminal_index_sha256": None,
        "terminal_task_count": None,
        "terminal_by_outcome": None,
        "terminal_failure_bindings": None,
        "effective_concurrency_limit": None,
        "available_concurrency_slots": None,
        "effective_concurrency_limit_mode": "unavailable",
        "backpressure_reason": "concurrency_projection_unavailable",
    }
    status = _safe_public_text(section.get("concurrency_status"), limit=32)
    if status in {"verified", "partial", "unavailable"}:
        result["concurrency_status"] = status
    state = _safe_public_text(section.get("concurrency_state"), limit=64)
    if state in {
        "first_canary_armed",
        "first_canary_single_in_flight",
        "continuous_concurrent_unlocked",
        "failed_drained",
        "paused_drained",
        "inactive_rolled_back",
        "canary_not_configured",
    }:
        result["concurrency_state"] = state
    source = _safe_public_text(section.get("concurrency_source"), limit=80)
    if source == "lease_store_subject_status_v1":
        result["concurrency_source"] = source
    observed_at = _safe_public_text(
        section.get("concurrency_observed_at"), limit=64
    )
    if observed_at is not None and _valid_timestamp_text(observed_at):
        result["concurrency_observed_at"] = observed_at
    for key in (
        "concurrency_error_code",
        "backpressure_reason",
    ):
        raw = _safe_public_text(section.get(key), limit=160)
        result[key] = raw if raw is None or SAFE_ID.fullmatch(raw) else None
    for key in (
        "subject_active_task_count",
        "subject_pending_task_count",
        "subject_peak_active",
        "subject_verified_runner_active_task_count",
        "subject_runner_evidenced_task_count",
        "subject_runner_interval_missing_count",
        "scheduler_claim_subject_peak_active",
        "terminal_task_count",
        "effective_concurrency_limit",
        "available_concurrency_slots",
    ):
        raw = section.get(key)
        result[key] = (
            raw
            if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0
            else None
        )
    terminal_index = section.get("terminal_index_sha256")
    if isinstance(terminal_index, str) and SHA256.fullmatch(terminal_index):
        result["terminal_index_sha256"] = terminal_index
    terminal_outcomes = section.get("terminal_by_outcome")
    if _terminal_counts_valid(terminal_outcomes):
        result["terminal_by_outcome"] = {
            outcome: int(terminal_outcomes.get(outcome, 0))
            for outcome in TERMINAL_OUTCOMES
        }
    terminal_failures = section.get("terminal_failure_bindings")
    if _terminal_failure_bindings_valid(terminal_failures):
        result["terminal_failure_bindings"] = [
            {
                "unit_sha256": str(binding["unit_sha256"]),
                "outcome": str(binding["outcome"]),
                "error_code": (
                    str(binding["error_code"])
                    if binding.get("error_code") is not None
                    else None
                ),
                "terminal_kind": str(binding["terminal_kind"]),
                "terminal_receipt_sha256": str(
                    binding["terminal_receipt_sha256"]
                ),
            }
            for binding in terminal_failures
        ]
    mode = _safe_public_text(
        section.get("effective_concurrency_limit_mode"), limit=24
    )
    result["effective_concurrency_limit_mode"] = (
        mode if mode in {"bounded", "unbounded", "unavailable"} else "unavailable"
    )
    return result


def _public_global_concurrency(projection: Mapping[str, Any]) -> dict[str, Any]:
    unavailable = {
        "schema_version": "study-intake-dashboard-concurrency-v1",
        "status": "unavailable",
        "release_id": None,
        "generated_at": None,
        "source": None,
        "telemetry_authority_sha256": None,
        "peak_semantics": None,
        "global_active_task_count": None,
        "global_peak_active": None,
        "verified_runner_global_active_task_count": None,
        "scheduler_claim_global_peak_active": None,
        "runner_evidenced_task_count_global": None,
        "runner_interval_missing_count_global": None,
        "zero_duration_runner_interval_count_global": None,
        "terminal_task_count_global": None,
        "terminal_by_outcome_global": None,
        "terminal_failure_binding_count_global": None,
        "effective_concurrency_limit": None,
        "available_concurrency_slots": None,
        "effective_concurrency_limit_mode": "unavailable",
        "backpressure_reason": "concurrency_projection_unavailable",
        "subject_observed_at": {
            subject: None for subject in SUBJECT_NAMES
        },
    }
    raw = projection.get("concurrency")
    if not isinstance(raw, Mapping):
        return unavailable
    result = dict(unavailable)
    if raw.get("schema_version") == "study-intake-dashboard-concurrency-v1":
        result["schema_version"] = str(raw["schema_version"])
    status = _safe_public_text(raw.get("status"), limit=32)
    if status in {"verified", "partial", "unavailable"}:
        result["status"] = status
    release_id = _safe_public_text(raw.get("release_id"), limit=64)
    if release_id is not None and RELEASE_ID.fullmatch(release_id):
        result["release_id"] = release_id
    generated_at = _safe_public_text(raw.get("generated_at"), limit=64)
    if generated_at is not None and _valid_timestamp_text(generated_at):
        result["generated_at"] = generated_at
    if raw.get("source") == "lease_store_subject_status_v1":
        result["source"] = str(raw["source"])
    if raw.get("peak_semantics") == "hmac_task_process_half_open_intervals":
        result["peak_semantics"] = str(raw["peak_semantics"])
    telemetry_authority_sha256 = _safe_public_text(
        raw.get("telemetry_authority_sha256"), limit=64
    )
    if (
        telemetry_authority_sha256 is not None
        and SHA256.fullmatch(telemetry_authority_sha256)
    ):
        result["telemetry_authority_sha256"] = telemetry_authority_sha256
    for key in (
        "global_active_task_count",
        "global_peak_active",
        "verified_runner_global_active_task_count",
        "scheduler_claim_global_peak_active",
        "runner_evidenced_task_count_global",
        "runner_interval_missing_count_global",
        "zero_duration_runner_interval_count_global",
        "terminal_task_count_global",
        "terminal_failure_binding_count_global",
        "effective_concurrency_limit",
        "available_concurrency_slots",
    ):
        value = raw.get(key)
        result[key] = (
            value
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0
            else None
        )
    terminal_outcomes = raw.get("terminal_by_outcome_global")
    if _terminal_counts_valid(terminal_outcomes):
        result["terminal_by_outcome_global"] = {
            outcome: int(terminal_outcomes.get(outcome, 0))
            for outcome in TERMINAL_OUTCOMES
        }
    mode = raw.get("effective_concurrency_limit_mode")
    result["effective_concurrency_limit_mode"] = (
        str(mode)
        if mode in {"bounded", "unbounded", "unavailable"}
        else "unavailable"
    )
    reason = _safe_public_text(raw.get("backpressure_reason"), limit=160)
    result["backpressure_reason"] = (
        reason if reason is None or SAFE_ID.fullmatch(reason) else None
    )
    observed = raw.get("subject_observed_at")
    if isinstance(observed, Mapping):
        result["subject_observed_at"] = {
            subject: (
                value
                if isinstance((value := observed.get(subject)), str)
                and _valid_timestamp_text(value)
                else None
            )
            for subject in SUBJECT_NAMES
        }
    return result


def _unavailable_task_counters(error_code: str) -> dict[str, Any]:
    return {
        "task_counter_scope": "unavailable",
        "task_counter_selection_scope": "none",
        "task_counter_error_code": error_code,
        "task_counter_task_count": None,
        "task_counter_stage_receipt_count": None,
        "model_call_count": None,
        "provider_request_count": None,
        "mcp_tool_call_count": None,
    }


def _subject_task_counter_projection(
    *,
    projection_path: Path | None,
    section: Mapping[str, Any],
    canary_gate: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Project task counters only from reopened authoritative stage receipts."""

    if canary_gate is None:
        return _unavailable_task_counters("canary_task_not_selected")
    active = canary_gate.get("active_selections")
    selections: list[Mapping[str, Any]] = []
    selection_scope = "none"
    if isinstance(active, Mapping) and active:
        if not all(isinstance(value, Mapping) for value in active.values()):
            return _unavailable_task_counters("canary_active_selection_invalid")
        selections = [value for value in active.values() if isinstance(value, Mapping)]
        selection_scope = "active_tasks"
    elif isinstance(canary_gate.get("last_selected"), Mapping):
        selections = [canary_gate["last_selected"]]
        selection_scope = "last_terminal_task"
    if not selections:
        return _unavailable_task_counters("canary_task_receipt_not_available")
    if projection_path is None:
        return _unavailable_task_counters("task_receipt_projection_path_unavailable")

    raw_items = section.get("items")
    if not isinstance(raw_items, list):
        return _unavailable_task_counters("task_receipt_item_index_unavailable")
    counts = {
        "model_call_count": 0,
        "provider_request_count": 0,
        "mcp_tool_call_count": 0,
    }
    stage_receipt_count = 0
    seen_units: set[str] = set()
    for selection in selections:
        unit_sha256 = selection.get("unit_sha256")
        capture_id = selection.get("producer_unit_id")
        if (
            not isinstance(unit_sha256, str)
            or SHA256.fullmatch(unit_sha256) is None
            or not isinstance(capture_id, str)
            or SAFE_ID.fullmatch(capture_id) is None
            or unit_sha256 in seen_units
        ):
            return _unavailable_task_counters("task_receipt_selection_invalid")
        seen_units.add(unit_sha256)
        candidates = [
            item
            for item in raw_items
            if isinstance(item, Mapping)
            and item.get("unit_sha256") == unit_sha256
        ]
        exact_capture = [
            item for item in candidates if item.get("capture_id") == capture_id
        ]
        if len(exact_capture) == 1:
            raw_item = exact_capture[0]
        elif len(candidates) == 1:
            raw_item = candidates[0]
        else:
            return _unavailable_task_counters("task_receipt_item_binding_invalid")
        public_item = _public_item(raw_item, str(canary_gate.get("subject")))
        if public_item is None:
            return _unavailable_task_counters("task_receipt_item_invalid")
        try:
            detail = _load_dispatch_task_detail(
                projection_path,
                raw_item,
                public_item,
                include_raw=False,
            )
        except (TaskDetailError, OSError, ValueError):
            return _unavailable_task_counters("task_stage_receipt_reopen_failed")
        if (
            not isinstance(detail, Mapping)
            or not isinstance(detail.get("task_identity"), Mapping)
            or detail["task_identity"].get("unit_sha256") != unit_sha256
        ):
            return _unavailable_task_counters("task_stage_receipt_binding_invalid")
        receipts = detail.get("stage_receipts")
        if not isinstance(receipts, Mapping) or not receipts:
            return _unavailable_task_counters("task_stage_receipt_not_available")
        for receipt in receipts.values():
            if not isinstance(receipt, Mapping):
                return _unavailable_task_counters("task_stage_receipt_invalid")
            for key in counts:
                value = receipt.get(key)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                ):
                    return _unavailable_task_counters(
                        "task_stage_receipt_counter_missing"
                    )
                counts[key] += value
            stage_receipt_count += 1
    return {
        "task_counter_scope": "verified_stage_receipts",
        "task_counter_selection_scope": selection_scope,
        "task_counter_error_code": None,
        "task_counter_task_count": len(selections),
        "task_counter_stage_receipt_count": stage_receipt_count,
        **counts,
    }


def _summary(
    projection: Mapping[str, Any],
    *,
    study_date: str | None,
    subject: str,
    expected_release_id: str | None = None,
    projection_path: Path | None = None,
    projection_sha256: str | None = None,
) -> dict[str, Any]:
    resolved_date = study_date or _text(projection.get("study_date"), limit=10)
    per_subject: dict[str, Any] = {}
    for subject_name, section in _subject_sections(projection).items():
        if subject != "all" and subject != subject_name:
            continue
        items = _filtered_items(
            projection,
            study_date=resolved_date,
            subject=subject_name,
        )
        counts = _counts(items)
        public_section = {
            "enabled": bool(section.get("enabled", False)),
            "counts": counts,
            "pipeline": _pipeline(items, counts),
            "metrics": _public_metrics(section),
            "batch_state_available": section.get("batch_state_available") is True,
            "batch_id": _safe_ref(section.get("batch_id")),
            "batch_status": _safe_public_text(section.get("batch_status"), limit=32)
            or "no_data",
            "capture_high_watermark": _public_capture_high_watermark(
                section.get("capture_high_watermark")
            ),
            "authority_generation": _safe_ref(section.get("authority_generation")),
            "all_terminal": _boolean(section.get("all_terminal")),
            "sol_ready": _boolean(section.get("sol_ready")),
            "batch_partition": {
                key: _safe_non_negative_integer(
                    section.get("batch_partition", {}).get(key)
                )
                for key in (
                    "current_batch_task_count",
                    "current_batch_terminal_task_count",
                    "current_batch_failed_task_count",
                    "current_batch_sol_candidate_task_count",
                    "current_batch_diagnostic_task_count",
                    "outside_batch_pending_task_count",
                    "outside_batch_failed_preserved_task_count",
                )
            }
            if isinstance(section.get("batch_partition"), Mapping)
            else None,
            "blocking_batch": (
                {
                    "batch_id": _safe_ref(section["blocking_batch"].get("batch_id")),
                    "study_date": _safe_public_text(
                        section["blocking_batch"].get("study_date"), limit=10
                    ),
                    "status": _safe_public_text(
                        section["blocking_batch"].get("status"), limit=40
                    ),
                    "all_terminal": _boolean(
                        section["blocking_batch"].get("all_terminal")
                    ),
                    "sol_ready": _boolean(
                        section["blocking_batch"].get("sol_ready")
                    ),
                    "writer_bound": _boolean(
                        section["blocking_batch"].get("writer_bound")
                    ),
                    "writer_handoff_status": _safe_public_text(
                        section["blocking_batch"].get("writer_handoff_status"),
                        limit=40,
                    ),
                    "blocker_code": _safe_public_text(
                        section["blocking_batch"].get("blocker_code"), limit=80
                    ),
                }
                if isinstance(section.get("blocking_batch"), Mapping)
                else None
            ),
            "blockers": [
                code
                for code in section.get("blockers", [])[:256]
                if isinstance(code, str) and SAFE_ID.fullmatch(code)
            ]
            if isinstance(section.get("blockers"), list)
            else [],
            "sol_handoff_status": _safe_public_text(
                section.get("sol_handoff_status"), limit=40
            )
            or "unknown",
            "sol_reviewed_count": _safe_non_negative_integer(
                section.get("sol_reviewed_count")
            ),
            "sol_committed_count": _safe_non_negative_integer(
                section.get("sol_committed_count")
            ),
        }
        current_stage = _text(section.get("current_stage"), limit=40)
        if current_stage in CURRENT_STAGES:
            public_section["current_stage"] = current_stage
        section_status = _text(section.get("status"), limit=32)
        if section_status in {"active", "disabled", "source_error"}:
            public_section["status"] = section_status
        error_code = _text(section.get("error_code"), limit=80)
        if error_code and re.fullmatch(r"[A-Za-z0-9._:-]+", error_code):
            public_section["error_code"] = error_code
        processing_error_code = _text(
            section.get("processing_error_code"), limit=80
        )
        if processing_error_code and re.fullmatch(
            r"[A-Za-z0-9._:-]+", processing_error_code
        ):
            public_section["processing_error_code"] = processing_error_code
        for key in ("control_state", "control_reason"):
            value = _safe_public_text(section.get(key), limit=160)
            if value is not None:
                public_section[key] = value
        canary_gate = _public_production_canary(section.get("canary_gate"))
        public_section["canary_gate"] = canary_gate
        public_section["production_status"] = (
            canary_gate["production_status"]
            if canary_gate is not None
            else (
                "paused"
                if public_section.get("enabled") is False
                else "not_configured"
            )
        )
        public_section["producer_capture_enabled"] = (
            canary_gate.get("producer_capture_enabled")
            if canary_gate is not None
            else None
        )
        public_section["luna_consumer_enabled"] = (
            canary_gate.get("luna_consumer_enabled")
            if canary_gate is not None
            else None
        )
        public_section["sol_formal_curation_enabled"] = (
            canary_gate.get("sol_formal_curation_enabled")
            if canary_gate is not None
            else False
        )
        public_section["queue_depth"] = (
            canary_gate.get("queue_depth") if canary_gate is not None else None
        )
        public_section["oldest_pending_age_seconds"] = (
            canary_gate.get("oldest_pending_age_seconds")
            if canary_gate is not None
            else None
        )
        public_section["active_task_count"] = (
            canary_gate.get("active_task_count")
            if canary_gate is not None
            else None
        )
        public_section["last_success_at"] = (
            canary_gate.get("last_success_at")
            if canary_gate is not None
            else None
        )
        public_section["last_failure_at"] = (
            canary_gate.get("last_failure_at")
            if canary_gate is not None
            else None
        )
        public_section["blocking_reason"] = (
            canary_gate.get("blocking_reason")
            if canary_gate is not None
            else public_section.get("control_reason")
        )
        public_section["next_action"] = (
            canary_gate.get("next_action") if canary_gate is not None else None
        )
        public_section["control_plane_counter_scope"] = (
            "canary_gate_control_plane" if canary_gate is not None else None
        )
        public_section["control_plane_model_call_count"] = (
            canary_gate.get("control_plane_model_call_count")
            if canary_gate is not None
            else None
        )
        public_section["control_plane_provider_request_count"] = (
            canary_gate.get("control_plane_provider_request_count")
            if canary_gate is not None
            else None
        )
        public_section["control_plane_mcp_tool_call_count"] = (
            canary_gate.get("control_plane_mcp_tool_call_count")
            if canary_gate is not None
            else None
        )
        public_section["activation_observed_counter_scope"] = (
            canary_gate.get("observability_counter_scope")
            if canary_gate is not None
            else None
        )
        for target, source in (
            ("activation_observed_model_call_count", "observed_model_call_count"),
            (
                "activation_observed_provider_request_count",
                "observed_provider_request_count",
            ),
            (
                "activation_observed_mcp_tool_call_count",
                "observed_mcp_tool_call_count",
            ),
        ):
            public_section[target] = (
                canary_gate.get(source) if canary_gate is not None else None
            )
        public_section.update(
            _subject_task_counter_projection(
                projection_path=projection_path,
                section=section,
                canary_gate=canary_gate,
            )
        )
        public_section["formal_write_count"] = (
            canary_gate.get("formal_write_count")
            if canary_gate is not None
            else None
        )
        public_section["sol_enabled"] = (
            canary_gate.get("sol_enabled") if canary_gate is not None else False
        )
        if projection.get("schema_version") in MODERN_SCHEMA_VERSIONS:
            public_section["soft_runtime_warning_seconds"] = (
                SUBJECT_SOFT_RUNTIME_WARNING_SECONDS[subject_name]
            )
        else:
            public_section["stage_timeout_seconds"] = (
                SUBJECT_SOFT_RUNTIME_WARNING_SECONDS[subject_name]
            )
        public_section.update(_public_subject_concurrency(section))
        if projection.get("schema_version") in MODERN_SCHEMA_VERSIONS and isinstance(
            section.get("capacity"), Mapping
        ):
            public_section["capacity"] = dict(section["capacity"])
        public_section["initial_canary_inflight_limit"] = (
            canary_gate.get("initial_canary_inflight_limit")
            if canary_gate is not None
            else None
        )
        public_section["continuous_concurrency_limit"] = (
            canary_gate.get("continuous_concurrency_limit")
            if canary_gate is not None
            else None
        )
        public_section["concurrency_capacity_phase"] = (
            "continuous"
            if canary_gate is not None
            and canary_gate.get("state") == "continuous_concurrent_unlocked"
            else "initial_canary"
            if canary_gate is not None
            and canary_gate.get("state") in {"armed", "canary_in_flight"}
            else "paused"
            if canary_gate is not None
            else "unavailable"
        )
        per_subject[subject_name] = public_section

    all_items = _filtered_items(
        projection,
        study_date=resolved_date,
        subject=subject,
    )
    total_counts = _counts(all_items)
    dispatchers = _public_dispatchers(projection)
    runtime_request = next(iter(dispatchers.values()), {})
    service_health = _service_health(
        projection,
        expected_release_id=expected_release_id,
    )
    global_sol = _public_global_sol(projection)
    en_p0_006 = _public_en_p0_006(projection)
    concurrency = _public_global_concurrency(projection)
    continuous_limits = [
        section.get("continuous_concurrency_limit")
        for section in per_subject.values()
        if isinstance(section.get("continuous_concurrency_limit"), int)
    ]
    concurrency["initial_canary_global_limit"] = sum(
        int(section.get("initial_canary_inflight_limit") or 0)
        for section in per_subject.values()
    )
    concurrency["configured_continuous_global_limit"] = (
        int(projection.get("configured_global_continuous_concurrency_limit"))
        if projection.get("schema_version") in MODERN_SCHEMA_VERSIONS
        and isinstance(
            projection.get("configured_global_continuous_concurrency_limit"), int
        )
        else sum(int(value) for value in continuous_limits)
        if len(continuous_limits) == len(per_subject) and per_subject
        else None
    )
    concurrency["capacity_phase"] = (
        "continuous"
        if per_subject
        and all(
            section.get("concurrency_capacity_phase") == "continuous"
            for section in per_subject.values()
        )
        else "initial_canary"
        if any(
            section.get("concurrency_capacity_phase") == "initial_canary"
            for section in per_subject.values()
        )
        else "mixed_or_paused"
    )
    return {
        "available": True,
        "schema_version": str(projection.get("schema_version") or SCHEMA_VERSION),
        "projection_identity": _projection_identity(
            projection, projection_sha256
        ),
        **_dashboard_request_counters(),
        "generated_at": _text(projection.get("generated_at"), limit=64),
        "study_date": resolved_date,
        "en_p0_006_status": en_p0_006["status"],
        "en_p0_006": en_p0_006,
        "subject": subject,
        "worker": runtime_request,
        "dispatchers": dispatchers,
        "service_health": service_health,
        "global_sol": global_sol,
        "concurrency": concurrency,
        "global_active_task_count": concurrency["global_active_task_count"],
        "global_peak_active": concurrency["global_peak_active"],
        "effective_concurrency_limit": concurrency[
            "effective_concurrency_limit"
        ],
        "available_concurrency_slots": concurrency[
            "available_concurrency_slots"
        ],
        "backpressure_reason": concurrency["backpressure_reason"],
        "other_subjects_luna_continuing": _other_subjects_luna_continuing(
            global_sol,
            service_health,
        ),
        "counts": total_counts,
        "pipeline": _pipeline(all_items, total_counts),
        "observability": _observability(all_items, runtime_request),
        "subjects": per_subject,
    }


def _multi_agent_v2_summary(projection: Mapping[str, Any]) -> dict[str, Any]:
    """Return a secret-free, GET-only Multi-Agent V2 control projection."""

    del projection
    config_path = BASE_DIR / "config.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        config = {}
    execution_mode = config.get("execution_mode")
    if execution_mode not in {"fixture", "offline", "live_authorized"}:
        execution_mode = "offline"
    live_gate = config.get("live_execution_gate")
    state_path = (
        Path(str(live_gate.get("authorization_state_path")))
        if isinstance(live_gate, Mapping)
        and isinstance(live_gate.get("authorization_state_path"), str)
        else None
    )
    authorization_present = False
    if state_path is not None:
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            authorization_present = bool(
                isinstance(state, Mapping)
                and state.get("status") == "armed_once"
                and state.get("remaining_tasks") == 1
            )
        except (OSError, UnicodeError, json.JSONDecodeError):
            authorization_present = False
    return {
        "schema_version": "study-intake-dashboard-multi-agent-v1",
        "execution_mode": execution_mode,
        "live_gate_locked": not authorization_present,
        "manual_authorization_present": authorization_present,
        "task_level": {
            "configured_subjects": list(SUBJECT_NAMES),
            "active_task_count": 0,
            "pending_task_count": 0,
        },
        "branch_level": {
            "logical_branch_count": 0,
            "pending_branch_count": 0,
            "active_branch_count": 0,
            "terminal_branch_count": 0,
            "physical_concurrency": 0,
            "waves_completed": 0,
            "drop_policy": "never",
        },
        "quality": {
            "accepted_count": 0,
            "corrected_count": 0,
            "issues_found_count": 0,
            "technical_quarantine_count": 0,
            "sol_review_ready_count": 0,
            "quality_clean_count": 0,
        },
        "safety_counts": {
            "real_terra_call_count": 0,
            "real_luna_call_count": 0,
            "real_provider_model_request_count": 0,
            "production_mcp_task_count": 0,
            "live_capture_created_count": 0,
            "live_capture_consumed_count": 0,
            "formal_write_count": 0,
        },
        "production_accepted": False,
        "formal_write_count": 0,
    }


class DashboardHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        store: ProjectionStore | None = None,
        campaign_store: CampaignStore | None = None,
        expected_release_id: str | None = None,
        validation_store: ValidationConsoleStore | None = None,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.store = store or ProjectionStore()
        self.campaign_store = campaign_store or CampaignStore(CAMPAIGN_PATH)
        self.expected_release_id = (
            expected_release_id
            if expected_release_id is not None
            else _release_id_from_manifest()
        )
        self.validation_store = validation_store or ValidationConsoleStore(
            self.store.path.parent
            / "validation-console"
            / "releases"
            / (self.expected_release_id or "0" * 64),
            fixture_mode=False,
            central_release_id=self.expected_release_id or "0" * 64,
        )


class DashboardHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "StudyDashboard/1.0"
    sys_version = ""

    @property
    def dashboard_server(self) -> DashboardHTTPServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, format: str, *args: Any) -> None:
        # Never place query strings, item identifiers or local paths in logs.
        return

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Security-Policy", (
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; font-src 'self'; "
            "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; "
            "object-src 'none'; media-src 'none'; manifest-src 'none'"
        ))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")

    def _send_bytes(
        self,
        status: HTTPStatus,
        body: bytes,
        content_type: str,
        *,
        allow: str | None = None,
    ) -> None:
        self.send_response(status.value)
        self._security_headers()
        if allow is not None:
            self.send_header("Allow", allow)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(
        self, status: HTTPStatus, payload: Mapping[str, Any], *, allow: str | None = None
    ) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._send_bytes(status, body, "application/json; charset=utf-8", allow=allow)

    def _reject_method(self) -> None:
        self._send_json(
            HTTPStatus.METHOD_NOT_ALLOWED,
            {"error": "method_not_allowed", "message": "该服务只允许 GET 请求。"},
            allow="GET",
        )

    def do_HEAD(self) -> None:  # noqa: N802
        self._reject_method()

    def do_POST(self) -> None:  # noqa: N802
        if not self._host_allowed():
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_host", "message": "只允许通过本机 Dashboard 地址访问。"},
            )
            return
        split = urlsplit(self.path)
        path = unquote(split.path)
        if not path.startswith("/api/v1/validation-console/"):
            self._reject_method()
            return
        self._serve_validation_post(path)

    def do_PUT(self) -> None:  # noqa: N802
        self._reject_method()

    def do_PATCH(self) -> None:  # noqa: N802
        self._reject_method()

    def do_DELETE(self) -> None:  # noqa: N802
        self._reject_method()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._reject_method()

    def _host_allowed(self) -> bool:
        dynamic = {
            f"127.0.0.1:{self.dashboard_server.server_address[1]}",
            f"localhost:{self.dashboard_server.server_address[1]}",
        }
        return self.headers.get("Host", "").lower() in ALLOWED_HOSTS | dynamic

    def _projection_unavailable(self, result: ProjectionResult) -> None:
        if result.error_code == "projection_missing":
            status = HTTPStatus.OK
        elif result.error_code == "projection_date_unavailable":
            status = HTTPStatus.NOT_FOUND
        else:
            status = HTTPStatus.SERVICE_UNAVAILABLE
        self._send_json(
            status,
            {
                "available": False,
                "error": result.error_code,
                "message": result.error,
            },
        )

    def do_GET(self) -> None:  # noqa: N802
        if not self._host_allowed():
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_host", "message": "只允许通过本机 Dashboard 地址访问。"},
            )
            return

        split = urlsplit(self.path)
        path = unquote(split.path)
        if ".." in path or "\\" in path or "\x00" in path:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_path", "message": "请求路径无效。"},
            )
            return

        if path in STATIC_ROUTES:
            self._serve_static(path)
            return
        if path == "/healthz":
            self._serve_health()
            return
        if path == "/api/v1/summary":
            self._serve_summary(parse_qs(split.query, keep_blank_values=True))
            return
        if path == "/api/v1/multi-agent-v2":
            self._serve_multi_agent_v2(
                parse_qs(split.query, keep_blank_values=True)
            )
            return
        if path == "/api/v1/concurrency-campaign":
            self._serve_concurrency_campaign(
                parse_qs(split.query, keep_blank_values=True)
            )
            return
        if path == "/api/v1/validation-console/state":
            self._send_json(
                HTTPStatus.OK,
                self.dashboard_server.validation_store.public_state(),
            )
            return
        if path == "/api/v1/validation-console/skills":
            state = self.dashboard_server.validation_store.public_state()
            self._send_json(
                HTTPStatus.OK,
                {
                    "schema_version": "study-intake-validation-skills-public-v1",
                    "skills": state["skills"],
                    "formal_write_count": 0,
                },
            )
            return
        if path == "/api/v1/validation-console/mcp-preflight":
            state = self.dashboard_server.validation_store.public_state()
            self._send_json(
                HTTPStatus.OK,
                {
                    "schema_version": "study-intake-validation-mcp-public-v1",
                    "mcp_preflight": state["mcp_preflight"],
                    "write_call_count": 0,
                    "formal_write_count": 0,
                },
            )
            return
        if path == "/api/v1/validation-console/campaigns":
            self._send_json(
                HTTPStatus.OK,
                self.dashboard_server.validation_store.campaigns_public(),
            )
            return
        if path.startswith("/api/v1/validation-console/reports/"):
            report_id = path.removeprefix(
                "/api/v1/validation-console/reports/"
            )
            try:
                body, content_type = self.dashboard_server.validation_store.report(
                    report_id
                )
            except ValidationConsoleError as exc:
                self._send_json(
                    HTTPStatus(exc.status),
                    {"error": exc.code, "message": exc.message},
                )
                return
            self._send_bytes(HTTPStatus.OK, body, content_type)
            return
        if path == "/api/v1/validation-console/audit-package":
            try:
                body = self.dashboard_server.validation_store.audit_package()
            except ValidationConsoleError as exc:
                self._send_json(
                    HTTPStatus(exc.status),
                    {"error": exc.code, "message": exc.message},
                )
                return
            self._send_bytes(HTTPStatus.OK, body, "application/zip")
            return
        if path == "/api/v1/items":
            self._serve_items(parse_qs(split.query, keep_blank_values=True))
            return
        if path.startswith("/api/v1/items/"):
            capture_id = path.removeprefix("/api/v1/items/")
            self._serve_item(capture_id, parse_qs(split.query, keep_blank_values=True))
            return

        self._send_json(
            HTTPStatus.NOT_FOUND,
            {"error": "not_found", "message": "没有这个只读资源。"},
        )

    def _validation_payload(self) -> dict[str, Any] | None:
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
            self._send_json(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {"error": "content_type_invalid", "message": "必须提交 JSON。"},
            )
            return None
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if not 0 <= length <= 64 * 1024:
            self._send_json(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"error": "request_too_large", "message": "请求超过安全限制。"},
            )
            return None
        try:
            value = json.loads(self.rfile.read(length) or b"{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_json", "message": "JSON 无效。"},
            )
            return None
        if not isinstance(value, dict):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_json", "message": "JSON 必须是对象。"},
            )
            return None
        return value

    def _validation_headers(self) -> tuple[str | None, str | None, int | None]:
        revision: int | None
        try:
            revision = int(self.headers.get("If-Match", ""))
        except ValueError:
            revision = None
        return (
            self.headers.get("X-Study-CSRF"),
            self.headers.get("X-Study-Nonce"),
            revision,
        )

    def _serve_validation_post(self, path: str) -> None:
        payload = self._validation_payload()
        if payload is None:
            return
        csrf_token, nonce, revision = self._validation_headers()
        store = self.dashboard_server.validation_store
        try:
            if path == "/api/v1/validation-console/preflight":
                if not store.fixture_mode:
                    raise ValidationConsoleError(
                        "operator_preflight_only",
                        "真实 MCP 预检仅由当前 Sol 受控执行。",
                        423,
                    )
                result = {
                    "status": "fixture_preflight_no_external_call",
                    "model_call_count": 0,
                    "mcp_call_count": 0,
                    "formal_write_count": 0,
                }
            elif path == "/api/v1/validation-console/authorize/terra":
                result = store.issue_authorization(
                    "terra",
                    payload,
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/authorize/luna":
                result = store.issue_authorization(
                    "luna",
                    payload,
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/fixture/terminal":
                result = store.terminal_relock(
                    str(payload.get("stage") or ""),
                    str(payload.get("outcome") or ""),
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/campaigns":
                result = store.create_campaign(
                    payload,
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/emergency-lock":
                result = store.emergency_lock(
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/handoff/export":
                result = store.export_handoff(
                    payload,
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            elif path == "/api/v1/validation-console/promotion/preview":
                result = store.promotion_preview(payload)
            elif path == "/api/v1/validation-console/promotion/apply":
                result = store.promotion_apply(
                    payload,
                    csrf_token=csrf_token,
                    nonce=nonce,
                    expected_revision=revision,
                )
            else:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": "not_found", "message": "没有这个控制台操作。"},
                )
                return
        except ValidationConsoleError as exc:
            self._send_json(
                HTTPStatus(exc.status),
                {"error": exc.code, "message": exc.message},
            )
            return
        self._send_json(HTTPStatus.OK, result)

    def _serve_static(self, path: str) -> None:
        file_path, content_type = STATIC_ROUTES[path]
        try:
            body = file_path.read_bytes()
        except OSError:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "static_unavailable", "message": "Dashboard 静态资源不可用。"},
            )
            return
        self._send_bytes(HTTPStatus.OK, body, content_type)

    def _serve_health(self) -> None:
        result = self.dashboard_server.store.load()
        if result.available and result.projection is not None:
            multi_agent = _multi_agent_v2_summary(result.projection)
            readiness = _service_health(
                result.projection,
                expected_release_id=self.dashboard_server.expected_release_id,
            )
            if not readiness["ready"]:
                readiness_error = (
                    "production_canary_runtime_unverified"
                    if readiness["production_canary_runtime_ready"] is not True
                    else "dispatcher_topology_degraded"
                )
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "status": "degraded",
                        "http_alive": True,
                        "http_ready": True,
                        "worker_ready": False,
                        "dispatchers_ready": False,
                        "projection_available": True,
                        "projection_fresh": readiness["projection_fresh"],
                        "projection_age_seconds": readiness[
                            "projection_age_seconds"
                        ],
                        "luna_processing_ready": readiness[
                            "luna_processing_ready"
                        ],
                        "sol_writer_ready": readiness["sol_writer_ready"],
                        "sol_safety_ready": readiness["sol_safety_ready"],
                        "production_canary_runtime_ready": readiness[
                            "production_canary_runtime_ready"
                        ],
                        "execution_mode": multi_agent["execution_mode"],
                        "live_gate_locked": multi_agent["live_gate_locked"],
                        "manual_authorization_present": multi_agent[
                            "manual_authorization_present"
                        ],
                        "live_model_execution": False,
                        "multi_agent_safety_counts": multi_agent[
                            "safety_counts"
                        ],
                        "schema_version": SCHEMA_VERSION,
                        "error": readiness_error,
                        "required_dispatchers": readiness["required_dispatchers"],
                        "ready_dispatchers": readiness["ready_dispatchers"],
                        "services": readiness["services"],
                        "dispatchers": _public_dispatchers(result.projection),
                    },
                )
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "http_alive": True,
                    "http_ready": True,
                    "worker_ready": True,
                    "dispatchers_ready": True,
                    "projection_available": True,
                    "projection_fresh": readiness["projection_fresh"],
                    "projection_age_seconds": readiness[
                        "projection_age_seconds"
                    ],
                    "luna_processing_ready": readiness[
                        "luna_processing_ready"
                    ],
                    "sol_writer_ready": readiness["sol_writer_ready"],
                    "sol_safety_ready": readiness["sol_safety_ready"],
                    "production_canary_runtime_ready": readiness[
                        "production_canary_runtime_ready"
                    ],
                    "execution_mode": multi_agent["execution_mode"],
                    "live_gate_locked": multi_agent["live_gate_locked"],
                    "manual_authorization_present": multi_agent[
                        "manual_authorization_present"
                    ],
                    "live_model_execution": False,
                    "multi_agent_safety_counts": multi_agent[
                        "safety_counts"
                    ],
                    "schema_version": SCHEMA_VERSION,
                    "required_dispatchers": readiness["required_dispatchers"],
                    "ready_dispatchers": readiness["ready_dispatchers"],
                    "services": readiness["services"],
                    "dispatchers": _public_dispatchers(result.projection),
                },
            )
            return
        status = (
            HTTPStatus.OK
            if result.error_code == "projection_missing"
            else HTTPStatus.SERVICE_UNAVAILABLE
        )
        self._send_json(
            status,
            {
                "status": "no_data" if result.error_code == "projection_missing" else "degraded",
                "http_alive": True,
                "http_ready": True,
                "worker_ready": False,
                "dispatchers_ready": False,
                "projection_available": False,
                "projection_fresh": False,
                "luna_processing_ready": False,
                "sol_writer_ready": False,
                "sol_safety_ready": False,
                "production_canary_runtime_ready": False,
                "error": result.error_code,
            },
        )

    @staticmethod
    def _one(query: Mapping[str, list[str]], key: str) -> str | None:
        values = query.get(key)
        if not values:
            return None
        return values[0]

    def _validated_filters(
        self, query: Mapping[str, list[str]], *, allow_status: bool
    ) -> tuple[str | None, str, str | None] | None:
        study_date = self._one(query, "date")
        subject = self._one(query, "subject") or "all"
        status = self._one(query, "status") if allow_status else None
        if not _valid_date(study_date):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_date", "message": "date 必须是 YYYY-MM-DD。"},
            )
            return None
        if subject not in ALLOWED_SUBJECTS:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_subject", "message": "subject 只允许 all、math、cs408 或 english。"},
            )
            return None
        if status is not None and status not in QUEUE_STATES:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_status", "message": "status 不是已知的队列状态。"},
            )
            return None
        return study_date, subject, status

    def _serve_summary(self, query: Mapping[str, list[str]]) -> None:
        filters = self._validated_filters(query, allow_status=False)
        if filters is None:
            return
        study_date, subject, _ = filters
        result = self.dashboard_server.store.load(study_date)
        if not result.available or result.projection is None:
            self._projection_unavailable(result)
            return
        self._send_json(
            HTTPStatus.OK,
            _summary(
                result.projection,
                study_date=study_date,
                subject=subject,
                expected_release_id=self.dashboard_server.expected_release_id,
                projection_path=self.dashboard_server.store.path,
                projection_sha256=result.projection_sha256,
            ),
        )

    def _serve_multi_agent_v2(
        self, query: Mapping[str, list[str]]
    ) -> None:
        if query:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "invalid_multi_agent_query",
                    "message": (
                        "Multi-Agent V2 control projection does not accept "
                        "query parameters."
                    ),
                },
            )
            return
        result = self.dashboard_server.store.load()
        if not result.available or result.projection is None:
            self._projection_unavailable(result)
            return
        self._send_json(
            HTTPStatus.OK,
            _multi_agent_v2_summary(result.projection),
        )

    def _serve_concurrency_campaign(
        self, query: Mapping[str, list[str]]
    ) -> None:
        values = query.get("subject")
        if set(query) - {"subject"} or (
            values is not None and (len(values) != 1 or not values[0])
        ):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "invalid_concurrency_campaign_query",
                    "message": "只允许一个非空 subject 参数。",
                },
            )
            return
        subject = values[0] if values is not None else "all"
        if subject not in ALLOWED_SUBJECTS:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "invalid_subject",
                    "message": "subject 只允许 all、math、cs408 或 english。",
                },
            )
            return
        result = self.dashboard_server.campaign_store.load()
        if not result.available or result.projection is None:
            status = (
                HTTPStatus.OK
                if result.error_code == "campaign_missing"
                else HTTPStatus.SERVICE_UNAVAILABLE
            )
            self._send_json(
                status,
                {
                    "available": False,
                    "error": result.error_code,
                    "message": result.error,
                    "production_accepted": False,
                    "formal_write_count": None,
                    "sol_enabled": None,
                },
            )
            return
        legacy_fast_mode = result.projection.get("fast_mode")
        if (
            result.projection.get("schema_version")
            == "study-intake-three-subject-concurrency-campaign-v1"
            and isinstance(legacy_fast_mode, Mapping)
            and legacy_fast_mode.get("requested") is True
            and legacy_fast_mode.get("service_tier") == "priority"
        ):
            historical = public_campaign(result.projection, subject=subject)
            historical["historical_status"] = historical.get("status")
            historical["status"] = "historical_legacy"
            historical["result_label"] = (
                "historical_concurrency_evidence_only"
            )
            historical["evidence_scope"] = "historical_legacy_v1"
            historical["current_release_usable"] = False
            historical["production_accepted"] = False
            self._send_json(HTTPStatus.OK, historical)
            return
        if (
            result.projection.get("schema_version")
            == "study-intake-three-subject-concurrency-campaign-v2"
            and result.projection.get("evidence_scope")
            == "zero_model_fixture_v2"
        ):
            fixture = public_campaign(result.projection, subject=subject)
            fixture["status"] = "test_evidence_only"
            fixture["result_label"] = "zero_model_fixture_evidence_only"
            fixture["current_release_usable"] = False
            fixture["production_accepted"] = False
            self._send_json(HTTPStatus.OK, fixture)
            return
        runtime_error = campaign_runtime_error(
            result.projection,
            expected_release_id=self.dashboard_server.expected_release_id,
        )
        if runtime_error is not None:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "available": False,
                    "error": runtime_error,
                    "message": "三科并发验收投影与当前 release 或有效时间窗不匹配。",
                    "production_accepted": False,
                    "formal_write_count": None,
                    "sol_enabled": None,
                },
            )
            return
        self._send_json(
            HTTPStatus.OK,
            public_campaign(result.projection, subject=subject),
        )

    def _serve_items(self, query: Mapping[str, list[str]]) -> None:
        filters = self._validated_filters(query, allow_status=True)
        if filters is None:
            return
        study_date, subject, status = filters
        result = self.dashboard_server.store.load(study_date)
        if not result.available or result.projection is None:
            self._projection_unavailable(result)
            return
        resolved_date = study_date or _text(result.projection.get("study_date"), limit=10)
        items = _filtered_items(
            result.projection,
            study_date=resolved_date,
            subject=subject,
            status=status,
        )
        identity = _projection_identity(
            result.projection, result.projection_sha256
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "available": True,
                "projection_identity": identity,
                "study_date": resolved_date,
                "subject": subject,
                "status": status,
                "items": items,
                **_dashboard_request_counters(),
            },
        )

    def _serve_item(self, capture_id: str, query: Mapping[str, list[str]]) -> None:
        if SAFE_ID.fullmatch(capture_id) is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_capture_id", "message": "capture ID 格式无效。"},
            )
            return
        raw_value = self._one(query, "raw")
        if raw_value not in {None, "0", "1"}:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_raw_mode", "message": "raw 只允许 0 或 1。"},
            )
            return
        precondition_keys = {
            "projection_sha256",
            "projection_generation",
            "projection_release_id",
            "task_generation",
            "task_fence",
        }
        if any(
            key in query
            and (
                len(query[key]) != 1
                or not isinstance(query[key][0], str)
                or not query[key][0]
            )
            for key in precondition_keys
        ):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "invalid_projection_precondition",
                    "message": "投影与任务前置条件必须各自是一个非空值。",
                },
            )
            return
        projection_precondition_names = {
            "projection_sha256",
            "projection_generation",
            "projection_release_id",
        }
        supplied_projection_preconditions = projection_precondition_names & set(
            query
        )
        if supplied_projection_preconditions and supplied_projection_preconditions != (
            projection_precondition_names
        ):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "incomplete_projection_precondition",
                    "message": "projection SHA、generation、release 必须成组提供。",
                },
            )
            return
        task_precondition_names = {"task_generation", "task_fence"}
        supplied_task_preconditions = task_precondition_names & set(query)
        if supplied_task_preconditions and supplied_task_preconditions != (
            task_precondition_names
        ):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": "incomplete_task_precondition",
                    "message": "task generation 与 fence 必须成组提供。",
                },
            )
            return
        include_raw = raw_value == "1"
        filters = self._validated_filters(query, allow_status=False)
        if filters is None:
            return
        study_date, subject, _ = filters
        result = self.dashboard_server.store.load(study_date)
        if not result.available or result.projection is None:
            self._projection_unavailable(result)
            return
        identity = _projection_identity(
            result.projection, result.projection_sha256
        )
        if supplied_projection_preconditions and (
            self._one(query, "projection_sha256") != identity.get("sha256")
            or self._one(query, "projection_generation")
            != identity.get("generation")
            or self._one(query, "projection_release_id")
            != identity.get("release_id")
        ):
            self._send_json(
                HTTPStatus.CONFLICT,
                {
                    "available": False,
                    "error": "projection_generation_conflict",
                    "capture_id": capture_id,
                    "projection_identity": identity,
                    **_dashboard_request_counters(),
                },
            )
            return
        resolved_date = study_date or _text(result.projection.get("study_date"), limit=10)
        found = _raw_item(
            result.projection,
            capture_id=capture_id,
            study_date=resolved_date,
            subject=subject,
        )
        if found is not None:
            raw_item, subject_hint = found
            item = _public_item(raw_item, subject_hint)
            if item is not None:
                if supplied_task_preconditions and (
                    self._one(query, "task_generation")
                    != str(item.get("generation"))
                    or self._one(query, "task_fence")
                    != str(item.get("fence"))
                ):
                    self._send_json(
                        HTTPStatus.CONFLICT,
                        {
                            "available": False,
                            "error": "task_generation_conflict",
                            "capture_id": capture_id,
                            "projection_identity": identity,
                            **_dashboard_request_counters(),
                        },
                    )
                    return
                try:
                    detail = _load_dispatch_task_detail(
                        self.dashboard_server.store.path,
                        raw_item,
                        item,
                        include_raw=include_raw,
                        output_schema_version=(
                            TASK_DETAIL_SCHEMA_VERSION
                            if result.projection.get("schema_version")
                            in MODERN_SCHEMA_VERSIONS
                            else LEGACY_TASK_DETAIL_SCHEMA_VERSION
                        ),
                    )
                except TaskDetailError as exc:
                    error_code = str(exc)
                    status = (
                        HTTPStatus.CONFLICT
                        if error_code == "task_detail_generation_conflict"
                        else HTTPStatus.UNPROCESSABLE_ENTITY
                    )
                    self._send_json(
                        status,
                        {
                            "available": False,
                            "error": error_code,
                            "capture_id": capture_id,
                            "projection_identity": identity,
                            **_dashboard_request_counters(),
                        },
                    )
                    return
                if detail is not None:
                    item["task_detail"] = detail
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "available": True,
                        "projection_identity": identity,
                        "item": item,
                        **_dashboard_request_counters(),
                    },
                )
                return
        self._send_json(
            HTTPStatus.NOT_FOUND,
            {
                "error": "item_not_found",
                "message": "当前投影中没有这条记录。",
                "projection_identity": identity,
                **_dashboard_request_counters(),
            },
        )


def main() -> None:
    server = DashboardHTTPServer((HOST, PORT), DashboardHandler)
    stop_once = threading.Event()

    def stop(_signum: int, _frame: Any) -> None:
        if stop_once.is_set():
            return
        stop_once.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
