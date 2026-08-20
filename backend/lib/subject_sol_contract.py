"""Luna batch quality and globally serialized Sol hand-off contracts.

This module is a control plane only.  It never imports or invokes a formal
writer.  Luna artifacts remain proposal-only and every Luna-side object has a
zero formal-write count.  A global logical lease, serialized by one OS flock,
allows at most one subject to enter Sol review/apply while the other subjects'
Luna dispatchers remain independent.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import base64
import binascii
import copy
import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
from typing import Any, Callable, TypeVar


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SUBJECTS = ("math", "cs408", "english")
SUBJECT_WRITER_ADAPTERS = {
    "math": "math_nightly_writer_v1",
    "cs408": "cs408_daily_intake_writer_v1",
    "english": "english_daily_intake_writer_v1",
}
SUBJECT_BATCH_SCHEMA = "subject_luna_batch_v1"
SUBJECT_BATCH_V2_SCHEMA = "subject_luna_batch_v2"
SUBJECT_QUALITY_RECEIPT_SCHEMA = "subject_quality_receipt_v1"
SUBJECT_QUALITY_RECEIPT_V2_SCHEMA = "subject_quality_receipt_v2"
SUBJECT_EXCLUSION_RECEIPT_SCHEMA = "subject_exclusion_receipt_v1"
DAILY_SOL_BATCH_SCHEMA = "daily_sol_batch_v2"
DAILY_SOL_BATCH_V3_SCHEMA = "daily_sol_batch_v3"
SOL_TASK_HANDOFF_SCHEMA = "sol_task_handoff_envelope_v1"
GLOBAL_SOL_WRITER_SCHEMA = "global_sol_writer_lease_v1"
SUBJECT_WRITER_STATE_SCHEMA = "subject_sol_writer_state_v2"
SOL_REVIEW_RECEIPT_SCHEMA = "sol_review_receipt_v1"
SOL_COMMIT_RECEIPT_SCHEMA = "sol_commit_receipt_v1"
SOL_REVIEW_RECEIPT_V2_SCHEMA = "sol_review_receipt_v2"
SOL_COMMIT_RECEIPT_V2_SCHEMA = "sol_commit_receipt_v2"
USER_SOL_AUTHORIZATION_SCHEMA = "user_sol_authorization_receipt_v1"
USER_SOL_AUTHORIZATION_V2_SCHEMA = "user_sol_authorization_receipt_v2"
USER_SUBJECT_EXCLUSION_AUTHORIZATION_SCHEMA = (
    "user_subject_exclusion_authorization_receipt_v1"
)
SUBJECT_AUTHORITY_OBSERVATION_SCHEMA = "subject_authority_observation_v1"
SUBJECT_GENERATION_AUTHORITY_ACK_SCHEMA = "subject_generation_authority_ack_v2"
SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_SCHEMA = (
    "subject_background_luna_rollover_receipt_v1"
)
SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA = (
    "subject_background_luna_rollover_receipt_v2"
)
CS408_TERMINAL_BATCH_RETIREMENT_RECEIPT_SCHEMA = (
    "cs408_terminal_batch_writer_retirement_receipt_v1"
)
CS408_TERMINAL_BATCH_RETIREMENT_POINTER_SCHEMA = (
    "cs408_terminal_batch_writer_retirement_pointer_v1"
)
CS408_TERMINAL_BATCH_RETIREMENT_ROLLBACK_RECEIPT_SCHEMA = (
    "cs408_terminal_batch_writer_retirement_rollback_receipt_v1"
)

# This is deliberately a single historical transaction descriptor, not a
# reusable recovery policy.  Any different 408 batch needs a new immutable
# successor and separate user authorization.
CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION = {
    "subject": "cs408",
    "mode": "archive_only_zero_replay",
    "batch_id": "LUNA-CS408-2026-08-12-6FC5AFB45F313F8A0E4D",
    "batch_sha256": (
        "7b22c081aebb0efa912f981e1d71cbdf8b4501da8634e959aec5108c1125ec39"
    ),
    "terminal_receipt_sha256": (
        "83c000a307f2af2037afae903ec1b791e85c00da75ebb9f5178088c76f11a5ff"
    ),
    "writer_preimage_sha256": (
        "f4dc94f9be21578d37e663e187948af581a4fc0dba633cb2e4fe7f6e12f8a29d"
    ),
    "batch_pointer_sha256": (
        "804f5f213d0a56351bb21179110309943577b453aabe2b5ef71e66640cb91e92"
    ),
    "snapshot_sha256": (
        "96400ebe760cdda21b4fe62f8a3987ef118f217a01289ea0ef8b6f1d8ff541dc"
    ),
    "source_generation": "cs408-26b5390a8f9a2c425e58",
    "source_authority_fingerprint": (
        "26b5390a8f9a2c425e58fbc6bec5b0a450dff85c4e1bd1817d99f133259f35f2"
    ),
    "target_generation": "cs408-e51335bd173df83c1736",
    "target_authority_fingerprint": (
        "e51335bd173df83c173666f4b880bdc91a008055818bd7425650959a8d59b08b"
    ),
}
ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION = {
    "subject": "english",
    "mode": "archive_reviewable_batch_zero_replay",
    "batch_id": "LUNA-ENGLISH-2026-08-13-2F171BD0B92CE1926349",
    "batch_sha256": (
        "d6b7ba66c92c69040a575b28c35a0cf24b646bb4a6ec690501fb27603f611ea1"
    ),
    "batch_pointer_sha256": (
        "ba4b4faeb0f00bfc17e8c327ef93581a6009c9ae31be8344efc9a4ce38f43ac9"
    ),
    "batch_snapshot_sha256": (
        "c5dedbf1eae264a0ff0b7d019c62adb49e164ba1f78a4d356a02df6fea0bdd57"
    ),
    "writer_preimage_sha256": (
        "ffc3e30309eec819fc16692f069434953ea457a5ee9025ae1786e96227e86a8b"
    ),
    "unit_sha256": (
        "3408c5a0dc067a17df0c3dd8c0b1ea171b5d473923df885ee5c7f4e3d85b2cc6"
    ),
    "source_generation": "english-317672cabdaba8030c2b",
    "source_authority_fingerprint": (
        "317672cabdaba8030c2b2b36accaf5da35ee2d96efbdc0db48137d83d7b44f13"
    ),
}
ENGLISH_LEGACY_BATCH_AUTHORIZATION_SCHEMA = (
    "english_legacy_batch_authorization_v1"
)
ENGLISH_LEGACY_SOL_WORK_ITEM_SCHEMA = "english_legacy_sol_work_item_v1"
ENGLISH_LEGACY_SOL_BATCH_SCHEMA = "english_legacy_recuration_sol_batch_v1"
ENGLISH_LEGACY_ITEM_REVIEW_SCHEMA = (
    "english_legacy_sol_item_review_receipt_v1"
)
ENGLISH_LEGACY_ITEM_APPLY_SCHEMA = (
    "english_legacy_sol_item_apply_receipt_v1"
)
ENGLISH_LEGACY_ITEM_FAILURE_SCHEMA = (
    "english_legacy_sol_item_failure_receipt_v1"
)
ENGLISH_LEGACY_ITEM_RECOVERY_SCHEMA = (
    "english_legacy_sol_item_recovery_receipt_v1"
)
ENGLISH_LEGACY_ROLLING_CHECKPOINT_SCHEMA = (
    "english_legacy_rolling_authority_checkpoint_v1"
)
ENGLISH_LEGACY_EXECUTION_CLOSURE_SCHEMA = (
    "english_legacy_recuration_execution_closure_v1"
)
ENGLISH_LEGACY_SOL_STATE_SCHEMA = "english_legacy_sol_batch_state_v1"

ENGLISH_LEGACY_TARGET_KINDS = {
    "master_bank_row",
    "sentence_pattern_card",
    "article_learning_page",
}
ENGLISH_LEGACY_SOL_ACTIONS = {
    "update_existing",
    "already_current",
}
ENGLISH_LEGACY_PROPOSAL_ACTIONS = {
    "update_existing_proposal",
    "already_current_proposal",
}

_T = TypeVar("_T")

TASK_RUNNING_STATUSES = {
    "selected",
    "queued",
    "claimed",
    "analysis_running",
    "critical_review_running",
    "retrying",
    "quality_pending",
}
TASK_TERMINAL_STATUSES = {
    "quality_passed",
    "needs_rework",
    "failed",
    "evidence_pending",
}
TASK_STATUSES = TASK_RUNNING_STATUSES | TASK_TERMINAL_STATUSES
TASK_V2_NONTERMINAL_STATUSES = {
    "selected",
    "claimed",
    "analysis_running",
    "critical_review_running",
    "packaging",
}
TASK_V2_TERMINAL_STATUSES = {
    "workflow_complete",
    "workflow_complete_with_warnings",
    "workflow_partial",
    "execution_failed",
    "cancelled",
    "stalled",
}
TASK_V2_SOL_CANDIDATE_STATUSES = {
    "workflow_complete",
    "workflow_complete_with_warnings",
}
TASK_V2_DIAGNOSTIC_STATUSES = TASK_V2_TERMINAL_STATUSES - (
    TASK_V2_SOL_CANDIDATE_STATUSES
)
TASK_V2_STATUSES = TASK_V2_NONTERMINAL_STATUSES | TASK_V2_TERMINAL_STATUSES
TASK_V2_STATUS_RANK = {
    "selected": 0,
    "claimed": 1,
    "analysis_running": 2,
    "critical_review_running": 3,
    "packaging": 4,
}
TASK_V2_HASH_FIELDS = (
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
)
TASK_V2_FIELDS = {
    "capture_id",
    "unit_sha256",
    "input_fingerprint",
    "study_date",
    "frozen_payload_sha256",
    "status",
    *TASK_V2_HASH_FIELDS,
    "warning_codes",
    "error_code",
}
QUALITY_OUTCOMES = {"accepted", "corrected", "rejected"}
CRITICAL_REVIEW_REJECTION_CODES = {
    "critical_review_rejected",
    "math_critical_review_rejected",
    "cs408_critical_review_rejected",
    "english_critical_review_rejected",
}
QUEUE_STATUSES = {
    "queued",
    "active",
    "reviewed",
    "committed",
    "failed",
    "safe_paused",
}


class SubjectSolContractError(ValueError):
    """A stable fail-closed control-plane error."""

    def __init__(self, code: str, **diagnostic: Any) -> None:
        super().__init__(code)
        self.code = code
        self.diagnostic = dict(diagnostic)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SubjectSolContractError(f"{label}_invalid")
    return value


def _sequence(value: Any, label: str, *, nonempty: bool = False) -> list[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise SubjectSolContractError(f"{label}_invalid")
    result = list(value)
    if nonempty and not result:
        raise SubjectSolContractError(f"{label}_invalid")
    return result


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SubjectSolContractError(f"{label}_invalid")
    return value


def _optional_nonempty(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, label)


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise SubjectSolContractError(f"{label}_invalid")
    return value


def _optional_sha256(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, label)


def _subject(value: Any) -> str:
    if value not in SUBJECT_WRITER_ADAPTERS:
        raise SubjectSolContractError("subject_invalid")
    return str(value)


def _safe_component(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", value):
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise SubjectSolContractError(f"{label}_invalid")
    return value


def _timestamp(value: Any, label: str) -> str:
    text = _nonempty(value, label)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SubjectSolContractError(f"{label}_invalid") from exc
    if parsed.tzinfo is None:
        raise SubjectSolContractError(f"{label}_invalid")
    return text


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SubjectSolContractError("control_value_not_canonical") from exc


def _json_file_bytes(value: Any) -> bytes:
    return _canonical_bytes(value) + b"\n"


def _value_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _document_sha256(value: Any) -> str:
    return hashlib.sha256(_json_file_bytes(value)).hexdigest()


def _unique_hashes(value: Any, label: str, *, nonempty: bool = False) -> list[str]:
    rows = _sequence(value, label, nonempty=nonempty)
    result = [_sha256(row, label.removesuffix("s")) for row in rows]
    if len(result) != len(set(result)) or result != sorted(result):
        raise SubjectSolContractError(f"{label}_invalid")
    return result


def _seal_shape(value: Any) -> dict[str, Any]:
    seal = dict(_mapping(value, "seal"))
    if set(seal) != {"algorithm", "purpose", "hmac_sha256"}:
        raise SubjectSolContractError("seal_shape_invalid")
    if seal.get("algorithm") != "HMAC-SHA256":
        raise SubjectSolContractError("seal_algorithm_invalid")
    _nonempty(seal.get("purpose"), "seal_purpose")
    _sha256(seal.get("hmac_sha256"), "seal_hmac_sha256")
    return seal


def _dispatcher_effect(value: Any, subject: str) -> dict[str, Any]:
    effect = dict(_mapping(value, "dispatcher_effect"))
    if set(effect) != {
        "scope",
        "subject",
        "paused_subjects",
        "drained_subjects",
        "other_subjects_unchanged",
    }:
        raise SubjectSolContractError("dispatcher_effect_shape_invalid")
    if (
        effect.get("scope") != "subject"
        or effect.get("subject") != subject
        or effect.get("paused_subjects") != []
        or effect.get("drained_subjects") != []
        or effect.get("other_subjects_unchanged") is not True
    ):
        raise SubjectSolContractError("dispatcher_effect_not_isolated")
    return effect


def _validate_batch_task(value: Any) -> dict[str, Any]:
    task = dict(_mapping(value, "subject_luna_task"))
    required = {
        "capture_id",
        "unit_sha256",
        "input_fingerprint",
        "study_date",
        "frozen_payload_sha256",
        "status",
        "proposal_sha256",
        "package_sha256",
        "quality_receipt_sha256",
        "terminal_receipt_sha256",
        "error_code",
    }
    if set(task) != required:
        raise SubjectSolContractError("subject_luna_task_shape_invalid")
    _nonempty(task.get("capture_id"), "capture_id")
    _sha256(task.get("unit_sha256"), "unit_sha256")
    _sha256(task.get("input_fingerprint"), "input_fingerprint")
    _nonempty(task.get("study_date"), "task_study_date")
    _sha256(task.get("frozen_payload_sha256"), "frozen_payload_sha256")
    status = task.get("status")
    if status not in TASK_STATUSES:
        raise SubjectSolContractError("subject_luna_task_status_invalid")
    proposal = _optional_sha256(task.get("proposal_sha256"), "proposal_sha256")
    package = _optional_sha256(task.get("package_sha256"), "package_sha256")
    quality = _optional_sha256(
        task.get("quality_receipt_sha256"), "quality_receipt_sha256"
    )
    terminal = _optional_sha256(
        task.get("terminal_receipt_sha256"), "terminal_receipt_sha256"
    )
    error = _optional_nonempty(task.get("error_code"), "task_error_code")
    if status == "quality_passed" and (
        proposal is None
        or package is None
        or quality is None
        or terminal is not None
        or error is not None
    ):
        raise SubjectSolContractError("quality_passed_task_binding_invalid")
    if status == "needs_rework":
        quality_rejection = (
            proposal is not None and package is None and quality is not None
            and terminal is None
        )
        terminal_rejection = (
            proposal is None and package is None and quality is None
            and terminal is not None
        )
        if error != "critical_review_rejected" or not (
            quality_rejection or terminal_rejection
        ):
            raise SubjectSolContractError("needs_rework_task_binding_invalid")
    if status in {"failed", "evidence_pending"} and (
        terminal is None or error is None or quality is not None
    ):
        raise SubjectSolContractError("failed_task_binding_invalid")
    return task


def validate_subject_luna_batch_v1(value: Mapping[str, Any]) -> dict[str, Any]:
    batch = dict(_mapping(value, "subject_luna_batch"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "study_date",
        "status",
        "capture_high_watermark",
        "scan_snapshot_sha256",
        "authority_generation",
        "authority_fingerprint",
        "tasks",
        "exclusion_receipt_sha256s",
        "all_terminal",
        "sol_ready",
        "blocking_task_ids",
        "formal_write_count",
        "revision",
        "updated_at",
    }
    if set(batch) != required or batch.get("schema_version") != SUBJECT_BATCH_SCHEMA:
        raise SubjectSolContractError("subject_luna_batch_shape_invalid")
    _nonempty(batch.get("batch_id"), "batch_id")
    _subject(batch.get("subject"))
    _nonempty(batch.get("study_date"), "study_date")
    if batch.get("status") not in {"open", "frozen"}:
        raise SubjectSolContractError("subject_luna_batch_status_invalid")
    _nonempty(batch.get("capture_high_watermark"), "capture_high_watermark")
    _sha256(batch.get("scan_snapshot_sha256"), "scan_snapshot_sha256")
    _nonempty(batch.get("authority_generation"), "authority_generation")
    _sha256(batch.get("authority_fingerprint"), "authority_fingerprint")
    tasks = [_validate_batch_task(row) for row in _sequence(batch.get("tasks"), "tasks")]
    identities = [(row["capture_id"], row["unit_sha256"]) for row in tasks]
    if (
        len(identities) != len(set(identities))
        or len({row["capture_id"] for row in tasks}) != len(tasks)
        or len({row["unit_sha256"] for row in tasks}) != len(tasks)
    ):
        raise SubjectSolContractError("subject_luna_task_duplicate")
    if tasks != sorted(tasks, key=lambda row: (row["capture_id"], row["unit_sha256"])):
        raise SubjectSolContractError("subject_luna_tasks_not_sorted")
    _unique_hashes(batch.get("exclusion_receipt_sha256s"), "exclusion_receipt_sha256s")
    if not isinstance(batch.get("all_terminal"), bool) or not isinstance(
        batch.get("sol_ready"), bool
    ):
        raise SubjectSolContractError("subject_luna_readiness_invalid")
    blocking = _sequence(batch.get("blocking_task_ids"), "blocking_task_ids")
    if any(not isinstance(item, str) or not item for item in blocking):
        raise SubjectSolContractError("blocking_task_ids_invalid")
    expected_blocking = sorted(
        row["capture_id"] for row in tasks if row["status"] != "quality_passed"
    )
    expected_all_terminal = bool(tasks) and all(
        row["status"] in TASK_TERMINAL_STATUSES for row in tasks
    )
    expected_ready = bool(
        batch["status"] == "frozen"
        and expected_all_terminal
        and not expected_blocking
    )
    if (
        blocking != expected_blocking
        or batch["all_terminal"] is not expected_all_terminal
        or batch["sol_ready"] is not expected_ready
    ):
        raise SubjectSolContractError("subject_luna_readiness_mismatch")
    if batch.get("formal_write_count") != 0:
        raise SubjectSolContractError("luna_formal_write_count_nonzero")
    _integer(batch.get("revision"), "subject_batch_revision")
    if batch.get("updated_at") is not None:
        _timestamp(batch["updated_at"], "subject_batch_updated_at")
    return batch


def _warning_codes(value: Any) -> list[str]:
    rows = _sequence(value, "warning_codes")
    result: list[str] = []
    for row in rows:
        code = _nonempty(row, "warning_code")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", code) is None:
            raise SubjectSolContractError("warning_code_invalid")
        result.append(code)
    if result != sorted(set(result)):
        raise SubjectSolContractError("warning_codes_invalid")
    return result


def _validate_batch_task_v2(value: Any) -> dict[str, Any]:
    """Validate one task without treating report quality as execution truth."""

    task = dict(_mapping(value, "subject_luna_task_v2"))
    if set(task) != TASK_V2_FIELDS:
        raise SubjectSolContractError("subject_luna_task_v2_shape_invalid")
    _nonempty(task.get("capture_id"), "capture_id")
    _sha256(task.get("unit_sha256"), "unit_sha256")
    _sha256(task.get("input_fingerprint"), "input_fingerprint")
    _nonempty(task.get("study_date"), "task_study_date")
    _sha256(task.get("frozen_payload_sha256"), "frozen_payload_sha256")
    status = task.get("status")
    if status not in TASK_V2_STATUSES:
        raise SubjectSolContractError("subject_luna_task_v2_status_invalid")
    hashes = {
        field: _optional_sha256(task.get(field), field)
        for field in TASK_V2_HASH_FIELDS
    }
    warnings = _warning_codes(task.get("warning_codes"))
    error = _optional_nonempty(task.get("error_code"), "task_error_code")

    analysis_execution_ready = all(
        hashes[field] is not None
        for field in (
            "analysis_execution_receipt_sha256",
            "analysis_raw_output_sha256",
        )
    )
    review_execution_ready = all(
        hashes[field] is not None
        for field in (
            "critical_review_execution_receipt_sha256",
            "critical_review_raw_output_sha256",
        )
    )
    review_execution_present = any(
        hashes[field] is not None
        for field in (
            "critical_review_execution_receipt_sha256",
            "critical_review_raw_output_sha256",
        )
    )
    analysis_report_ready = all(
        hashes[field] is not None
        for field in (
            "analysis_normalization_receipt_sha256",
            "analysis_report_sha256",
        )
    )
    review_report_ready = all(
        hashes[field] is not None
        for field in (
            "critical_review_normalization_receipt_sha256",
            "critical_review_report_sha256",
        )
    )
    review_report_present = any(
        hashes[field] is not None
        for field in (
            "critical_review_normalization_receipt_sha256",
            "critical_review_report_sha256",
        )
    )
    terminal_ready = hashes["terminal_receipt_sha256"] is not None
    package_ready = hashes["package_sha256"] is not None
    handoff_ready = hashes["sol_handoff_envelope_sha256"] is not None

    if status in TASK_V2_NONTERMINAL_STATUSES and (
        terminal_ready or error is not None
    ):
        raise SubjectSolContractError("subject_luna_task_v2_nonterminal_binding_invalid")
    if status == "packaging" and not (
        analysis_execution_ready and review_execution_ready
    ):
        raise SubjectSolContractError("subject_luna_task_v2_stage_binding_missing")
    if status == "workflow_complete" and not (
        analysis_execution_ready
        and review_execution_ready
        and analysis_report_ready
        and review_report_ready
        and package_ready
        and handoff_ready
        and terminal_ready
        and not warnings
        and error is None
    ):
        raise SubjectSolContractError("subject_luna_task_v2_complete_binding_invalid")
    complete_with_quality_findings = bool(
        analysis_execution_ready
        and analysis_report_ready
        and (
            not review_execution_present
            and not review_report_present
            or review_execution_ready
            and review_report_ready
        )
        and package_ready
        and not handoff_ready
        and terminal_ready
        and warnings
        and error is None
    )
    if status == "workflow_complete_with_warnings" and not (
        complete_with_quality_findings
        or (
            analysis_execution_ready
            and review_execution_ready
            and package_ready
            and handoff_ready
            and terminal_ready
            and warnings
            and error is None
        )
    ):
        raise SubjectSolContractError(
            "subject_luna_task_v2_warning_complete_binding_invalid"
        )
    if status == "workflow_partial" and not (
        analysis_execution_ready
        and terminal_ready
        and error is not None
        and not handoff_ready
    ):
        raise SubjectSolContractError("subject_luna_task_v2_partial_binding_invalid")
    if status in {"execution_failed", "cancelled", "stalled"} and not (
        terminal_ready and error is not None and not handoff_ready
    ):
        raise SubjectSolContractError("subject_luna_task_v2_terminal_binding_invalid")
    return task


def recompute_subject_luna_batch_v2(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a canonical v2 batch derived only from its frozen task set."""

    batch = copy.deepcopy(dict(_mapping(value, "subject_luna_batch_v2")))
    tasks = [_validate_batch_task_v2(row) for row in batch.get("tasks", [])]
    tasks.sort(key=lambda row: (row["capture_id"], row["unit_sha256"]))
    batch["tasks"] = tasks
    batch["blocking_task_ids"] = sorted(
        row["capture_id"]
        for row in tasks
        if row["status"] not in TASK_V2_TERMINAL_STATUSES
    )
    batch["sol_candidate_task_ids"] = sorted(
        row["capture_id"]
        for row in tasks
        if row["status"] in TASK_V2_SOL_CANDIDATE_STATUSES
        and row.get("sol_handoff_envelope_sha256") is not None
    )
    batch["diagnostic_task_ids"] = sorted(
        row["capture_id"]
        for row in tasks
        if row["status"] in TASK_V2_DIAGNOSTIC_STATUSES
        or (
            row["status"] in TASK_V2_SOL_CANDIDATE_STATUSES
            and row.get("sol_handoff_envelope_sha256") is None
        )
    )
    batch["all_terminal"] = bool(tasks) and not batch["blocking_task_ids"]
    batch["sol_ready"] = bool(
        batch.get("status") == "frozen"
        and batch["all_terminal"]
        and batch["sol_candidate_task_ids"]
    )
    return batch


def validate_subject_luna_batch_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate per-task Sol eligibility while retaining failed siblings."""

    batch = dict(_mapping(value, "subject_luna_batch_v2"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "study_date",
        "status",
        "capture_high_watermark",
        "scan_snapshot_sha256",
        "authority_generation",
        "authority_fingerprint",
        "tasks",
        "exclusion_receipt_sha256s",
        "all_terminal",
        "sol_ready",
        "blocking_task_ids",
        "sol_candidate_task_ids",
        "diagnostic_task_ids",
        "formal_write_count",
        "revision",
        "updated_at",
    }
    if set(batch) != required or batch.get("schema_version") != SUBJECT_BATCH_V2_SCHEMA:
        raise SubjectSolContractError("subject_luna_batch_v2_shape_invalid")
    _nonempty(batch.get("batch_id"), "batch_id")
    _subject(batch.get("subject"))
    _nonempty(batch.get("study_date"), "study_date")
    if batch.get("status") not in {"open", "frozen"}:
        raise SubjectSolContractError("subject_luna_batch_v2_status_invalid")
    _nonempty(batch.get("capture_high_watermark"), "capture_high_watermark")
    _sha256(batch.get("scan_snapshot_sha256"), "scan_snapshot_sha256")
    _nonempty(batch.get("authority_generation"), "authority_generation")
    _sha256(batch.get("authority_fingerprint"), "authority_fingerprint")
    tasks = [
        _validate_batch_task_v2(row)
        for row in _sequence(batch.get("tasks"), "tasks")
    ]
    identities = [(row["capture_id"], row["unit_sha256"]) for row in tasks]
    if (
        len(identities) != len(set(identities))
        or len({row["capture_id"] for row in tasks}) != len(tasks)
        or len({row["unit_sha256"] for row in tasks}) != len(tasks)
        or tasks
        != sorted(tasks, key=lambda row: (row["capture_id"], row["unit_sha256"]))
    ):
        raise SubjectSolContractError("subject_luna_task_v2_identity_invalid")
    _unique_hashes(batch.get("exclusion_receipt_sha256s"), "exclusion_receipt_sha256s")
    for field in (
        "blocking_task_ids",
        "sol_candidate_task_ids",
        "diagnostic_task_ids",
    ):
        rows = _sequence(batch.get(field), field)
        if (
            any(not isinstance(row, str) or not row for row in rows)
            or rows != sorted(set(rows))
        ):
            raise SubjectSolContractError(f"{field}_invalid")
    if not isinstance(batch.get("all_terminal"), bool) or not isinstance(
        batch.get("sol_ready"), bool
    ):
        raise SubjectSolContractError("subject_luna_batch_v2_readiness_invalid")
    recomputed = recompute_subject_luna_batch_v2(batch)
    for field in (
        "tasks",
        "blocking_task_ids",
        "sol_candidate_task_ids",
        "diagnostic_task_ids",
        "all_terminal",
        "sol_ready",
    ):
        if batch[field] != recomputed[field]:
            raise SubjectSolContractError("subject_luna_batch_v2_readiness_mismatch")
    if batch.get("formal_write_count") != 0:
        raise SubjectSolContractError("luna_formal_write_count_nonzero")
    _integer(batch.get("revision"), "subject_batch_revision")
    if batch.get("updated_at") is not None:
        _timestamp(batch["updated_at"], "subject_batch_updated_at")
    return batch


def transition_subject_luna_task_v2(
    batch_value: Mapping[str, Any],
    *,
    capture_id: str,
    unit_sha256: str,
    status: str,
    updates: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply an idempotent, forward-only transition to one frozen batch task."""

    batch = validate_subject_luna_batch_v2(batch_value)
    checked_capture = _nonempty(capture_id, "capture_id")
    checked_unit = _sha256(unit_sha256, "unit_sha256")
    if status not in TASK_V2_STATUSES:
        raise SubjectSolContractError("subject_luna_task_v2_status_invalid")
    target_index = next(
        (
            index
            for index, row in enumerate(batch["tasks"])
            if row["capture_id"] == checked_capture
            and row["unit_sha256"] == checked_unit
        ),
        None,
    )
    if target_index is None:
        raise SubjectSolContractError("subject_luna_task_v2_not_found")
    target = copy.deepcopy(batch["tasks"][target_index])
    previous = str(target["status"])
    if previous in TASK_V2_TERMINAL_STATUSES and status != previous:
        raise SubjectSolContractError("subject_luna_task_v2_terminal_regression")
    if (
        previous in TASK_V2_NONTERMINAL_STATUSES
        and status in TASK_V2_NONTERMINAL_STATUSES
        and TASK_V2_STATUS_RANK[status] < TASK_V2_STATUS_RANK[previous]
    ):
        raise SubjectSolContractError("subject_luna_task_v2_state_regression")
    changes = dict(updates or {})
    if set(changes) - (TASK_V2_FIELDS - {
        "capture_id",
        "unit_sha256",
        "input_fingerprint",
        "study_date",
        "frozen_payload_sha256",
        "status",
    }):
        raise SubjectSolContractError("subject_luna_task_v2_update_invalid")
    target.update(changes)
    target["status"] = status
    _validate_batch_task_v2(target)
    candidate = copy.deepcopy(batch)
    candidate["tasks"][target_index] = target
    candidate["revision"] = int(batch["revision"]) + 1
    candidate["updated_at"] = _utc_now()
    candidate = recompute_subject_luna_batch_v2(candidate)
    return validate_subject_luna_batch_v2(candidate)


def validate_subject_exclusion_receipt_v1(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the portable shape of one explicit user exclusion receipt.

    Trust is deliberately not established here.  The runtime verifier must
    reopen the content-addressed receipt and original batch snapshot, verify
    their hashes, and verify this receipt's HMAC and authority key id.
    """

    receipt = dict(_mapping(value, "subject_exclusion_receipt"))
    required = {
        "schema_version",
        "subject",
        "original_batch",
        "excluded_task",
        "user_authorization",
        "user_authorization_receipt_sha256",
        "reason",
        "authority_key_id",
        "formal_write_count",
        "issued_at",
        "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != SUBJECT_EXCLUSION_RECEIPT_SCHEMA
    ):
        raise SubjectSolContractError("subject_exclusion_receipt_shape_invalid")

    subject = _subject(receipt.get("subject"))
    original = dict(_mapping(receipt.get("original_batch"), "original_batch"))
    if set(original) != {
        "batch_id",
        "batch_sha256",
        "study_date",
        "authority_generation",
        "authority_fingerprint",
    }:
        raise SubjectSolContractError("exclusion_original_batch_shape_invalid")
    _nonempty(original.get("batch_id"), "original_batch_id")
    _sha256(original.get("batch_sha256"), "original_batch_sha256")
    _nonempty(original.get("study_date"), "original_study_date")
    _nonempty(original.get("authority_generation"), "original_authority_generation")
    _sha256(
        original.get("authority_fingerprint"),
        "original_authority_fingerprint",
    )

    task = _validate_batch_task(receipt.get("excluded_task"))
    if task["status"] not in {"needs_rework", "failed", "evidence_pending"}:
        raise SubjectSolContractError("subject_exclusion_task_not_blocking")

    authorization = dict(
        _mapping(receipt.get("user_authorization"), "exclusion_user_authorization")
    )
    if set(authorization) != {
        "schema_version",
        "event_id",
        "event_type",
        "subject",
        "original_batch_id",
        "original_batch_sha256",
        "capture_id",
        "unit_sha256",
        "reason",
        "authorized_at",
        "formal_write_count",
        "seal",
    }:
        raise SubjectSolContractError("exclusion_authorization_shape_invalid")
    if (
        authorization.get("schema_version")
        != USER_SUBJECT_EXCLUSION_AUTHORIZATION_SCHEMA
        or authorization.get("event_type") != "explicit_user_subject_exclusion"
        or authorization.get("subject") != subject
        or authorization.get("original_batch_id") != original["batch_id"]
        or authorization.get("original_batch_sha256") != original["batch_sha256"]
        or authorization.get("capture_id") != task["capture_id"]
        or authorization.get("unit_sha256") != task["unit_sha256"]
    ):
        raise SubjectSolContractError("exclusion_authorization_binding_invalid")
    _nonempty(authorization.get("event_id"), "exclusion_authorization_event_id")
    _sha256(
        authorization.get("original_batch_sha256"),
        "exclusion_authorization_batch_sha256",
    )
    _nonempty(authorization.get("reason"), "exclusion_authorization_reason")
    if authorization.get("formal_write_count") != 0:
        raise SubjectSolContractError("exclusion_authorization_formal_write_count_nonzero")
    _seal_shape(authorization.get("seal"))
    _sha256(
        receipt.get("user_authorization_receipt_sha256"),
        "user_authorization_receipt_sha256",
    )
    authorized_at = _timestamp(
        authorization.get("authorized_at"), "exclusion_authorized_at"
    )
    issued_at = _timestamp(receipt.get("issued_at"), "exclusion_issued_at")
    authorized_dt = dt.datetime.fromisoformat(
        authorized_at[:-1] + "+00:00" if authorized_at.endswith("Z") else authorized_at
    )
    issued_dt = dt.datetime.fromisoformat(
        issued_at[:-1] + "+00:00" if issued_at.endswith("Z") else issued_at
    )
    if authorized_dt > issued_dt:
        raise SubjectSolContractError("exclusion_authorization_after_issue")
    if receipt.get("reason") != authorization.get("reason"):
        raise SubjectSolContractError("exclusion_reason_binding_invalid")
    _sha256(receipt.get("authority_key_id"), "authority_key_id")
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("exclusion_formal_write_count_nonzero")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_subject_quality_receipt_v1(value: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(_mapping(value, "subject_quality_receipt"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "capture_id",
        "unit_sha256",
        "frozen_payload_sha256",
        "completion_sha256",
        "dispatch_receipt_sha256",
        "dispatch_package_sha256",
        "capture_freeze_receipt_sha256",
        "mcp_read_session_receipt_sha256",
        "analysis_output_sha256",
        "analysis_mcp_transcript_sha256",
        "critical_review_output_sha256",
        "critical_review_mcp_transcript_sha256",
        "draft_sha256",
        "review_outcome",
        "proposal_sha256",
        "package_sha256",
        "authority",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }
    if set(receipt) != required or receipt.get("schema_version") != SUBJECT_QUALITY_RECEIPT_SCHEMA:
        raise SubjectSolContractError("subject_quality_receipt_shape_invalid")
    _nonempty(receipt.get("batch_id"), "batch_id")
    _subject(receipt.get("subject"))
    _nonempty(receipt.get("capture_id"), "capture_id")
    for field in (
        "unit_sha256",
        "frozen_payload_sha256",
        "completion_sha256",
        "dispatch_receipt_sha256",
        "dispatch_package_sha256",
        "capture_freeze_receipt_sha256",
        "mcp_read_session_receipt_sha256",
        "analysis_output_sha256",
        "analysis_mcp_transcript_sha256",
        "critical_review_output_sha256",
        "critical_review_mcp_transcript_sha256",
        "draft_sha256",
        "proposal_sha256",
    ):
        _sha256(receipt.get(field), field)
    outcome = receipt.get("review_outcome")
    if outcome not in QUALITY_OUTCOMES:
        raise SubjectSolContractError("quality_review_outcome_invalid")
    _sha256(receipt.get("package_sha256"), "package_sha256")
    if (
        receipt["analysis_output_sha256"]
        == receipt["critical_review_output_sha256"]
        or receipt["analysis_mcp_transcript_sha256"]
        == receipt["critical_review_mcp_transcript_sha256"]
    ):
        raise SubjectSolContractError("quality_stage_artifacts_not_distinct")
    authority = dict(_mapping(receipt.get("authority"), "quality_authority"))
    if set(authority) != {"generation", "authority_fingerprint"}:
        raise SubjectSolContractError("quality_authority_shape_invalid")
    _nonempty(authority.get("generation"), "quality_generation")
    _sha256(authority.get("authority_fingerprint"), "quality_authority_fingerprint")
    if receipt.get("model_call_count") != 2:
        raise SubjectSolContractError("quality_model_call_count_invalid")
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("quality_formal_write_count_nonzero")
    _timestamp(receipt.get("issued_at"), "quality_issued_at")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_subject_quality_receipt_v2(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a two-stage v2 execution authenticity closure.

    The receipt is intentionally narrower than the Sol handoff.  It proves
    that both model stages, their MCP transcripts, and the immutable task
    authority all belong to one completed v2 task.  Warning completion is
    allowed to omit only normalization/report artifacts; raw output,
    execution receipt, and MCP transcript remain mandatory.
    """

    receipt = dict(_mapping(value, "subject_quality_receipt_v2"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "capture_id",
        "unit_sha256",
        "frozen_payload_sha256",
        "completion_sha256",
        "dispatch_receipt_sha256",
        "dispatch_package_sha256",
        "capture_freeze_receipt_sha256",
        "mcp_read_session_receipt_sha256",
        "authority_snapshot_sha256",
        "analysis",
        "critical_review",
        "execution_status",
        "warning_codes",
        "package_sha256",
        "sol_handoff_envelope_sha256",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != SUBJECT_QUALITY_RECEIPT_V2_SCHEMA
    ):
        raise SubjectSolContractError("subject_quality_receipt_v2_shape_invalid")
    _nonempty(receipt.get("batch_id"), "batch_id")
    _subject(receipt.get("subject"))
    _nonempty(receipt.get("capture_id"), "capture_id")
    for field in (
        "unit_sha256",
        "frozen_payload_sha256",
        "completion_sha256",
        "dispatch_receipt_sha256",
        "dispatch_package_sha256",
        "capture_freeze_receipt_sha256",
        "mcp_read_session_receipt_sha256",
        "authority_snapshot_sha256",
        "package_sha256",
        "sol_handoff_envelope_sha256",
    ):
        _sha256(receipt.get(field), field)

    def stage(value: Any, label: str) -> dict[str, Any]:
        row = dict(_mapping(value, label))
        if set(row) != {
            "raw_output_sha256",
            "execution_receipt_sha256",
            "normalization_receipt_sha256",
            "report_sha256",
            "mcp_transcript_sha256",
            "warning_codes",
        }:
            raise SubjectSolContractError(
                "subject_quality_receipt_v2_stage_shape_invalid"
            )
        for field in (
            "raw_output_sha256",
            "execution_receipt_sha256",
            "mcp_transcript_sha256",
        ):
            _sha256(row.get(field), f"{label}_{field}")
        for field in ("normalization_receipt_sha256", "report_sha256"):
            _optional_sha256(row.get(field), f"{label}_{field}")
        row["warning_codes"] = _warning_codes(row.get("warning_codes"))
        return row

    analysis = stage(receipt.get("analysis"), "analysis")
    critical_review = stage(receipt.get("critical_review"), "critical_review")
    if (
        analysis["raw_output_sha256"] == critical_review["raw_output_sha256"]
        or analysis["execution_receipt_sha256"]
        == critical_review["execution_receipt_sha256"]
        or analysis["mcp_transcript_sha256"]
        == critical_review["mcp_transcript_sha256"]
    ):
        raise SubjectSolContractError("quality_stage_artifacts_not_distinct")
    warnings = _warning_codes(receipt.get("warning_codes"))
    if warnings != sorted(
        set(analysis["warning_codes"] + critical_review["warning_codes"])
    ):
        raise SubjectSolContractError("subject_quality_warning_binding_invalid")
    status = receipt.get("execution_status")
    if status == "workflow_complete":
        if warnings or any(
            row[field] is None
            for row in (analysis, critical_review)
            for field in ("normalization_receipt_sha256", "report_sha256")
        ):
            raise SubjectSolContractError("subject_quality_status_invalid")
    elif status == "workflow_complete_with_warnings":
        if not warnings:
            raise SubjectSolContractError("subject_quality_status_invalid")
    else:
        raise SubjectSolContractError("subject_quality_status_invalid")
    if receipt.get("model_call_count") != 2:
        raise SubjectSolContractError("quality_model_call_count_invalid")
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("quality_formal_write_count_nonzero")
    _timestamp(receipt.get("issued_at"), "quality_issued_at")
    seal = _seal_shape(receipt.get("seal"))
    if seal.get("purpose") != "subject-quality-receipt-v2":
        raise SubjectSolContractError("subject_quality_receipt_v2_seal_invalid")
    return receipt


def _authorization_receipt(value: Any) -> dict[str, Any]:
    receipt = dict(_mapping(value, "sol_authorization_receipt"))
    required = {
        "schema_version",
        "sol_batch_id",
        "subject",
        "study_date",
        "subject_luna_batch_id",
        "subject_luna_batch_sha256",
        "proposal_sha256s",
        "package_sha256s",
        "quality_receipt_sha256s",
        "writer_adapter",
        "idempotency_key",
        "event",
        "formal_write_count",
        "seal",
    }
    if set(receipt) != required or receipt.get("schema_version") != USER_SOL_AUTHORIZATION_SCHEMA:
        raise SubjectSolContractError("sol_authorization_receipt_shape_invalid")
    subject = _subject(receipt.get("subject"))
    _nonempty(receipt.get("sol_batch_id"), "sol_batch_id")
    _nonempty(receipt.get("study_date"), "study_date")
    _nonempty(receipt.get("subject_luna_batch_id"), "subject_luna_batch_id")
    _sha256(receipt.get("subject_luna_batch_sha256"), "subject_luna_batch_sha256")
    for field in ("proposal_sha256s", "package_sha256s", "quality_receipt_sha256s"):
        _unique_hashes(receipt.get(field), field, nonempty=True)
    if receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]:
        raise SubjectSolContractError("cross_subject_writer_adapter")
    _nonempty(receipt.get("idempotency_key"), "idempotency_key")
    event = dict(_mapping(receipt.get("event"), "authorization_event"))
    if set(event) != {
        "event_id",
        "event_type",
        "subject",
        "study_date",
        "subject_luna_batch_id",
        "authorized_at",
    }:
        raise SubjectSolContractError("authorization_event_shape_invalid")
    if (
        event.get("event_type") != "explicit_user_sol_authorization"
        or event.get("subject") != subject
        or event.get("study_date") != receipt.get("study_date")
        or event.get("subject_luna_batch_id") != receipt.get("subject_luna_batch_id")
    ):
        raise SubjectSolContractError("authorization_event_binding_invalid")
    _nonempty(event.get("event_id"), "authorization_event_id")
    _timestamp(event.get("authorized_at"), "authorized_at")
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("authorization_formal_write_count_nonzero")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_daily_sol_batch_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    batch = dict(_mapping(value, "daily_sol_batch"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "study_date",
        "subject_luna_batch",
        "subject_luna_batch_sha256",
        "proposal_sha256s",
        "package_sha256s",
        "quality_receipt_sha256s",
        "authorization_receipt",
        "authorization_receipt_sha256",
        "writer_adapter",
        "idempotency_key",
        "status",
        "formal_write_count",
    }
    if set(batch) != required or batch.get("schema_version") != DAILY_SOL_BATCH_SCHEMA:
        raise SubjectSolContractError("daily_sol_batch_shape_invalid")
    _nonempty(batch.get("batch_id"), "sol_batch_id")
    subject = _subject(batch.get("subject"))
    _nonempty(batch.get("study_date"), "study_date")
    luna_batch = validate_subject_luna_batch_v1(
        _mapping(batch.get("subject_luna_batch"), "subject_luna_batch")
    )
    if (
        luna_batch["subject"] != subject
        or luna_batch["study_date"] != batch["study_date"]
        or luna_batch["sol_ready"] is not True
    ):
        raise SubjectSolContractError("subject_luna_batch_not_sol_ready")
    if batch.get("subject_luna_batch_sha256") != _document_sha256(luna_batch):
        raise SubjectSolContractError("subject_luna_batch_hash_mismatch")
    proposals = _unique_hashes(batch.get("proposal_sha256s"), "proposal_sha256s", nonempty=True)
    packages = _unique_hashes(batch.get("package_sha256s"), "package_sha256s", nonempty=True)
    qualities = _unique_hashes(
        batch.get("quality_receipt_sha256s"), "quality_receipt_sha256s", nonempty=True
    )
    if proposals != sorted(row["proposal_sha256"] for row in luna_batch["tasks"]):
        raise SubjectSolContractError("sol_proposal_set_mismatch")
    if packages != sorted(row["package_sha256"] for row in luna_batch["tasks"]):
        raise SubjectSolContractError("sol_package_set_mismatch")
    if qualities != sorted(row["quality_receipt_sha256"] for row in luna_batch["tasks"]):
        raise SubjectSolContractError("sol_quality_receipt_set_mismatch")
    authorization = _authorization_receipt(batch.get("authorization_receipt"))
    if batch.get("authorization_receipt_sha256") != _document_sha256(authorization):
        raise SubjectSolContractError("authorization_receipt_hash_mismatch")
    for field in (
        "subject",
        "study_date",
        "proposal_sha256s",
        "package_sha256s",
        "quality_receipt_sha256s",
        "writer_adapter",
        "idempotency_key",
    ):
        if authorization.get(field) != batch.get(field):
            raise SubjectSolContractError("authorization_receipt_binding_mismatch")
    if (
        authorization.get("sol_batch_id") != batch.get("batch_id")
        or
        authorization.get("subject_luna_batch_id") != luna_batch["batch_id"]
        or authorization.get("subject_luna_batch_sha256")
        != batch["subject_luna_batch_sha256"]
        or batch.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
        or batch.get("status") != "authorized"
        or batch.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("daily_sol_batch_binding_invalid")
    _nonempty(batch.get("idempotency_key"), "idempotency_key")
    return batch


def _validate_sol_handoff_stage(value: Any, *, label: str) -> dict[str, Any]:
    stage = dict(_mapping(value, label))
    required = {
        "raw_output_sha256",
        "execution_receipt_sha256",
        "normalization_receipt_sha256",
        "report_sha256",
        "warning_codes",
    }
    if set(stage) != required:
        raise SubjectSolContractError("sol_task_handoff_stage_shape_invalid")
    _sha256(stage.get("raw_output_sha256"), f"{label}_raw_output_sha256")
    _sha256(
        stage.get("execution_receipt_sha256"),
        f"{label}_execution_receipt_sha256",
    )
    _optional_sha256(
        stage.get("normalization_receipt_sha256"),
        f"{label}_normalization_receipt_sha256",
    )
    _optional_sha256(stage.get("report_sha256"), f"{label}_report_sha256")
    _warning_codes(stage.get("warning_codes"))
    return stage


def validate_sol_task_handoff_envelope_v1(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    envelope = dict(_mapping(value, "sol_task_handoff_envelope"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "capture_id",
        "unit_sha256",
        "input_fingerprint",
        "study_date",
        "frozen_payload_sha256",
        "authority_snapshot_sha256",
        "analysis",
        "critical_review",
        "package_sha256",
        "terminal_receipt_sha256",
        "warning_codes",
        "execution_status",
        "formal_write_count",
    }
    if set(envelope) != required or envelope.get("schema_version") != SOL_TASK_HANDOFF_SCHEMA:
        raise SubjectSolContractError("sol_task_handoff_envelope_shape_invalid")
    _nonempty(envelope.get("batch_id"), "batch_id")
    _subject(envelope.get("subject"))
    _nonempty(envelope.get("capture_id"), "capture_id")
    for field in (
        "unit_sha256",
        "input_fingerprint",
        "frozen_payload_sha256",
        "authority_snapshot_sha256",
        "package_sha256",
        "terminal_receipt_sha256",
    ):
        _sha256(envelope.get(field), field)
    _nonempty(envelope.get("study_date"), "study_date")
    analysis = _validate_sol_handoff_stage(envelope.get("analysis"), label="analysis")
    review = _validate_sol_handoff_stage(
        envelope.get("critical_review"), label="critical_review"
    )
    warnings = _warning_codes(envelope.get("warning_codes"))
    if envelope.get("execution_status") not in TASK_V2_SOL_CANDIDATE_STATUSES:
        raise SubjectSolContractError("sol_task_handoff_execution_status_invalid")
    if envelope.get("execution_status") == "workflow_complete" and (
        warnings or analysis["warning_codes"] or review["warning_codes"]
    ):
        raise SubjectSolContractError("sol_task_handoff_warning_status_invalid")
    if envelope.get("execution_status") == "workflow_complete_with_warnings" and not (
        warnings or analysis["warning_codes"] or review["warning_codes"]
    ):
        raise SubjectSolContractError("sol_task_handoff_warning_status_invalid")
    if envelope.get("formal_write_count") != 0:
        raise SubjectSolContractError("luna_formal_write_count_nonzero")
    return envelope


def _authorization_receipt_v2(value: Any) -> dict[str, Any]:
    receipt = dict(_mapping(value, "sol_authorization_receipt_v2"))
    required = {
        "schema_version",
        "sol_batch_id",
        "subject",
        "study_date",
        "subject_luna_batch_id",
        "subject_luna_batch_sha256",
        "sol_candidate_task_ids",
        "sol_handoff_envelope_sha256s",
        "diagnostic_task_ids",
        "writer_adapter",
        "idempotency_key",
        "event",
        "formal_write_count",
        "seal",
    }
    if set(receipt) != required or receipt.get("schema_version") != USER_SOL_AUTHORIZATION_V2_SCHEMA:
        raise SubjectSolContractError("sol_authorization_receipt_v2_shape_invalid")
    subject = _subject(receipt.get("subject"))
    for field in ("sol_batch_id", "study_date", "subject_luna_batch_id", "idempotency_key"):
        _nonempty(receipt.get(field), field)
    _sha256(receipt.get("subject_luna_batch_sha256"), "subject_luna_batch_sha256")
    candidate_ids = _sequence(
        receipt.get("sol_candidate_task_ids"),
        "sol_candidate_task_ids",
        nonempty=True,
    )
    diagnostics = _sequence(receipt.get("diagnostic_task_ids"), "diagnostic_task_ids")
    if candidate_ids != sorted(set(candidate_ids)) or diagnostics != sorted(set(diagnostics)):
        raise SubjectSolContractError("sol_authorization_task_ids_invalid")
    _unique_hashes(
        receipt.get("sol_handoff_envelope_sha256s"),
        "sol_handoff_envelope_sha256s",
        nonempty=True,
    )
    if receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]:
        raise SubjectSolContractError("cross_subject_writer_adapter")
    event = dict(_mapping(receipt.get("event"), "authorization_event"))
    if set(event) != {
        "event_id", "event_type", "subject", "study_date",
        "subject_luna_batch_id", "authorized_at",
    } or (
        event.get("event_type") != "explicit_user_sol_authorization"
        or event.get("subject") != subject
        or event.get("study_date") != receipt.get("study_date")
        or event.get("subject_luna_batch_id") != receipt.get("subject_luna_batch_id")
    ):
        raise SubjectSolContractError("authorization_event_binding_invalid")
    _nonempty(event.get("event_id"), "authorization_event_id")
    _timestamp(event.get("authorized_at"), "authorized_at")
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("authorization_formal_write_count_nonzero")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_user_sol_authorization_receipt_v2(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Public strict validator for the isolated v2 intent publisher."""

    receipt = _authorization_receipt_v2(value)
    if receipt["seal"]["purpose"] != "user-sol-authorization-receipt-v2":
        raise SubjectSolContractError("sol_authorization_receipt_v2_seal_invalid")
    return receipt


def validate_daily_sol_batch_v3(value: Mapping[str, Any]) -> dict[str, Any]:
    batch = dict(_mapping(value, "daily_sol_batch_v3"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "study_date",
        "subject_luna_batch",
        "subject_luna_batch_sha256",
        "sol_candidate_task_ids",
        "sol_handoff_envelope_sha256s",
        "diagnostic_task_ids",
        "authorization_receipt",
        "authorization_receipt_sha256",
        "writer_adapter",
        "idempotency_key",
        "status",
        "formal_write_count",
    }
    if set(batch) != required or batch.get("schema_version") != DAILY_SOL_BATCH_V3_SCHEMA:
        raise SubjectSolContractError("daily_sol_batch_v3_shape_invalid")
    _nonempty(batch.get("batch_id"), "sol_batch_id")
    subject = _subject(batch.get("subject"))
    _nonempty(batch.get("study_date"), "study_date")
    luna = validate_subject_luna_batch_v2(
        _mapping(batch.get("subject_luna_batch"), "subject_luna_batch")
    )
    if (
        luna["subject"] != subject
        or luna["study_date"] != batch["study_date"]
        or luna["sol_ready"] is not True
        or batch.get("subject_luna_batch_sha256") != _document_sha256(luna)
    ):
        raise SubjectSolContractError("subject_luna_batch_v2_not_sol_ready")
    candidates = _sequence(
        batch.get("sol_candidate_task_ids"),
        "sol_candidate_task_ids",
        nonempty=True,
    )
    diagnostics = _sequence(batch.get("diagnostic_task_ids"), "diagnostic_task_ids")
    if candidates != luna["sol_candidate_task_ids"] or diagnostics != luna["diagnostic_task_ids"]:
        raise SubjectSolContractError("daily_sol_batch_v3_task_set_mismatch")
    envelope_hashes = _unique_hashes(
        batch.get("sol_handoff_envelope_sha256s"),
        "sol_handoff_envelope_sha256s",
        nonempty=True,
    )
    if len(envelope_hashes) != len(candidates):
        raise SubjectSolContractError("sol_handoff_candidate_count_mismatch")
    authorization = _authorization_receipt_v2(batch.get("authorization_receipt"))
    if batch.get("authorization_receipt_sha256") != _document_sha256(authorization):
        raise SubjectSolContractError("authorization_receipt_hash_mismatch")
    expected_authorization = {
        "sol_batch_id": batch["batch_id"],
        "subject": subject,
        "study_date": batch["study_date"],
        "subject_luna_batch_id": luna["batch_id"],
        "subject_luna_batch_sha256": batch["subject_luna_batch_sha256"],
        "sol_candidate_task_ids": candidates,
        "sol_handoff_envelope_sha256s": envelope_hashes,
        "diagnostic_task_ids": diagnostics,
        "writer_adapter": batch.get("writer_adapter"),
        "idempotency_key": batch.get("idempotency_key"),
    }
    if any(authorization.get(key) != value for key, value in expected_authorization.items()):
        raise SubjectSolContractError("authorization_receipt_binding_mismatch")
    if (
        batch.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
        or batch.get("status") != "authorized"
        or batch.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("daily_sol_batch_v3_binding_invalid")
    return batch


def validate_sol_review_receipt_v2(
    value: Mapping[str, Any], *, batch: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Validate one Sol review over the exact v3 task-handoff set.

    Luna report quality is deliberately not treated as the writer decision.
    Every decision is keyed by the frozen task identity and immutable handoff
    object that Sol actually reviewed.
    """

    receipt = dict(_mapping(value, "sol_review_receipt_v2"))
    required = {
        "schema_version",
        "batch_id",
        "daily_sol_batch_sha256",
        "subject",
        "fencing_token",
        "writer",
        "writer_adapter",
        "canonical_evidence_sha256",
        "decisions",
        "status",
        "dispatcher_effect",
        "formal_write_count",
        "completed_at",
        "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != SOL_REVIEW_RECEIPT_V2_SCHEMA
    ):
        raise SubjectSolContractError("sol_review_receipt_v2_shape_invalid")
    subject = _subject(receipt.get("subject"))
    _nonempty(receipt.get("batch_id"), "batch_id")
    _sha256(receipt.get("daily_sol_batch_sha256"), "daily_sol_batch_sha256")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    if (
        receipt.get("writer") != "sol"
        or receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
    ):
        raise SubjectSolContractError("sol_review_writer_invalid")
    _sha256(receipt.get("canonical_evidence_sha256"), "canonical_evidence_sha256")

    decisions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in _sequence(
        receipt.get("decisions"), "sol_review_decisions", nonempty=True
    ):
        row = dict(_mapping(raw, "sol_review_decision_v2"))
        if set(row) != {
            "capture_id",
            "unit_sha256",
            "sol_handoff_envelope_sha256",
            "decision",
            "replacement_proposal_sha256",
            "replacement_package_sha256",
            "reason",
        }:
            raise SubjectSolContractError("sol_review_decision_v2_shape_invalid")
        capture_id = _nonempty(row.get("capture_id"), "capture_id")
        if capture_id in seen:
            raise SubjectSolContractError("sol_review_decision_v2_duplicate")
        seen.add(capture_id)
        _sha256(row.get("unit_sha256"), "unit_sha256")
        _sha256(
            row.get("sol_handoff_envelope_sha256"),
            "sol_handoff_envelope_sha256",
        )
        decision = row.get("decision")
        if decision not in {"adopted", "modified", "rejected"}:
            raise SubjectSolContractError("sol_review_decision_v2_invalid")
        replacement_proposal = _optional_sha256(
            row.get("replacement_proposal_sha256"),
            "replacement_proposal_sha256",
        )
        replacement_package = _optional_sha256(
            row.get("replacement_package_sha256"),
            "replacement_package_sha256",
        )
        if (decision == "modified") != (
            replacement_proposal is not None and replacement_package is not None
        ):
            raise SubjectSolContractError("sol_review_replacement_binding_invalid")
        if decision != "modified" and (
            replacement_proposal is not None or replacement_package is not None
        ):
            raise SubjectSolContractError("sol_review_replacement_binding_invalid")
        _nonempty(row.get("reason"), "sol_review_reason")
        decisions.append(row)

    if receipt.get("status") not in {
        "approved",
        "needs_user",
        "rejected",
        "failed",
    }:
        raise SubjectSolContractError("sol_review_status_invalid")
    if receipt["status"] == "approved" and any(
        row["decision"] == "rejected" for row in decisions
    ):
        raise SubjectSolContractError("sol_review_status_inconsistent")
    _dispatcher_effect(receipt.get("dispatcher_effect"), subject)
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("sol_review_formal_write_count_nonzero")
    _timestamp(receipt.get("completed_at"), "sol_review_completed_at")
    _seal_shape(receipt.get("seal"))

    if batch is not None:
        authorized = validate_daily_sol_batch_v3(batch)
        task_by_capture = {
            row["capture_id"]: row
            for row in authorized["subject_luna_batch"]["tasks"]
        }
        expected = {
            capture_id: (
                task_by_capture[capture_id]["unit_sha256"],
                task_by_capture[capture_id]["sol_handoff_envelope_sha256"],
            )
            for capture_id in authorized["sol_candidate_task_ids"]
        }
        observed = {
            row["capture_id"]: (
                row["unit_sha256"],
                row["sol_handoff_envelope_sha256"],
            )
            for row in decisions
        }
        if (
            receipt["batch_id"] != authorized["batch_id"]
            or subject != authorized["subject"]
            or receipt["daily_sol_batch_sha256"]
            != _document_sha256(authorized)
            or observed != expected
        ):
            raise SubjectSolContractError("sol_review_batch_binding_invalid")
    return receipt


def validate_sol_commit_receipt_v2(
    value: Mapping[str, Any], *, batch: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate a writer receipt bound to the exact authorized v3 candidates."""

    authorized = validate_daily_sol_batch_v3(batch)
    receipt = dict(_mapping(value, "sol_commit_receipt_v2"))
    required = {
        "schema_version",
        "batch_id",
        "daily_sol_batch_sha256",
        "subject",
        "fencing_token",
        "writer",
        "writer_adapter",
        "sol_candidate_task_ids",
        "sol_handoff_envelope_sha256s",
        "execution_result_sha256",
        "sol_review_receipt_sha256",
        "pre_state_sha256",
        "post_state_sha256",
        "operations",
        "writer_process",
        "transaction_end",
        "recovery_position_sha256",
        "rollback_receipt_sha256",
        "status",
        "dispatcher_effect",
        "formal_write_count",
        "completed_at",
        "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != SOL_COMMIT_RECEIPT_V2_SCHEMA
    ):
        raise SubjectSolContractError("sol_commit_receipt_v2_shape_invalid")
    subject = _subject(receipt.get("subject"))
    if (
        receipt.get("batch_id") != authorized["batch_id"]
        or subject != authorized["subject"]
        or receipt.get("daily_sol_batch_sha256") != _document_sha256(authorized)
        or receipt.get("sol_candidate_task_ids")
        != authorized["sol_candidate_task_ids"]
        or receipt.get("sol_handoff_envelope_sha256s")
        != authorized["sol_handoff_envelope_sha256s"]
    ):
        raise SubjectSolContractError("sol_commit_batch_binding_invalid")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    if (
        receipt.get("writer") != "sol"
        or receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
    ):
        raise SubjectSolContractError("sol_commit_writer_invalid")
    for field in (
        "execution_result_sha256",
        "sol_review_receipt_sha256",
        "pre_state_sha256",
        "post_state_sha256",
    ):
        _sha256(receipt.get(field), field)
    if not isinstance(receipt.get("operations"), list):
        raise SubjectSolContractError("sol_commit_operations_invalid")
    writer_process = dict(_mapping(receipt.get("writer_process"), "writer_process"))
    if set(writer_process) != {
        "adapter_run_id",
        "pid",
        "terminal_state",
        "exit_code",
        "stopped_at",
        "evidence_sha256",
    }:
        raise SubjectSolContractError("writer_process_shape_invalid")
    _nonempty(writer_process.get("adapter_run_id"), "adapter_run_id")
    _integer(writer_process.get("pid"), "writer_pid", minimum=1)
    if writer_process.get("terminal_state") != "stopped":
        raise SubjectSolContractError("writer_process_not_stopped")
    _integer(writer_process.get("exit_code"), "writer_exit_code")
    _timestamp(writer_process.get("stopped_at"), "writer_stopped_at")
    _sha256(writer_process.get("evidence_sha256"), "writer_process_evidence_sha256")
    transaction = dict(_mapping(receipt.get("transaction_end"), "transaction_end"))
    if set(transaction) != {
        "transaction_id",
        "state",
        "ended_at",
        "evidence_sha256",
    }:
        raise SubjectSolContractError("transaction_end_shape_invalid")
    _nonempty(transaction.get("transaction_id"), "transaction_id")
    if transaction.get("state") not in {
        "committed",
        "already_current",
        "failed",
        "rolled_back",
    }:
        raise SubjectSolContractError("transaction_end_state_invalid")
    _timestamp(transaction.get("ended_at"), "transaction_ended_at")
    _sha256(transaction.get("evidence_sha256"), "transaction_end_evidence_sha256")
    if receipt.get("status") not in {
        "committed",
        "already_current",
        "failed",
        "rolled_back",
    } or transaction["state"] != receipt["status"]:
        raise SubjectSolContractError("transaction_end_status_mismatch")
    recovery = _optional_sha256(
        receipt.get("recovery_position_sha256"), "recovery_position_sha256"
    )
    rollback = _optional_sha256(
        receipt.get("rollback_receipt_sha256"), "rollback_receipt_sha256"
    )
    if receipt["status"] == "failed" and recovery is None:
        raise SubjectSolContractError("failed_apply_recovery_position_missing")
    if receipt["status"] == "rolled_back" and rollback is None:
        raise SubjectSolContractError("rolled_back_apply_receipt_missing")
    if receipt["status"] in {"committed", "already_current"} and (
        recovery is not None or rollback is not None
    ):
        raise SubjectSolContractError("successful_apply_recovery_evidence_invalid")
    count = _integer(receipt.get("formal_write_count"), "formal_write_count")
    if receipt["status"] != "committed" and count != 0:
        raise SubjectSolContractError("noncommit_formal_write_count_nonzero")
    _dispatcher_effect(receipt.get("dispatcher_effect"), subject)
    _timestamp(receipt.get("completed_at"), "sol_commit_completed_at")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_sol_review_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    receipt = dict(_mapping(value, "sol_review_receipt"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "fencing_token",
        "writer",
        "writer_adapter",
        "canonical_evidence_sha256",
        "decisions",
        "status",
        "dispatcher_effect",
        "formal_write_count",
        "completed_at",
        "seal",
    }
    if set(receipt) != required or receipt.get("schema_version") != SOL_REVIEW_RECEIPT_SCHEMA:
        raise SubjectSolContractError("sol_review_receipt_shape_invalid")
    subject = _subject(receipt.get("subject"))
    _nonempty(receipt.get("batch_id"), "batch_id")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    if receipt.get("writer") != "sol" or receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]:
        raise SubjectSolContractError("sol_review_writer_invalid")
    _sha256(receipt.get("canonical_evidence_sha256"), "canonical_evidence_sha256")
    decisions = []
    seen: set[str] = set()
    for raw in _sequence(receipt.get("decisions"), "sol_review_decisions", nonempty=True):
        row = dict(_mapping(raw, "sol_review_decision"))
        if set(row) != {"proposal_sha256", "decision", "modified_proposal_sha256", "reason"}:
            raise SubjectSolContractError("sol_review_decision_shape_invalid")
        proposal = _sha256(row.get("proposal_sha256"), "proposal_sha256")
        if proposal in seen or row.get("decision") not in {"adopt", "modify", "reject"}:
            raise SubjectSolContractError("sol_review_decision_invalid")
        seen.add(proposal)
        modified = _optional_sha256(
            row.get("modified_proposal_sha256"), "modified_proposal_sha256"
        )
        if (row["decision"] == "modify") is (modified is None):
            raise SubjectSolContractError("sol_review_modified_proposal_invalid")
        _nonempty(row.get("reason"), "sol_review_reason")
        decisions.append(row)
    if receipt.get("status") not in {"approved", "needs_user", "rejected", "failed"}:
        raise SubjectSolContractError("sol_review_status_invalid")
    if receipt["status"] == "approved" and any(
        row["decision"] == "reject" for row in decisions
    ):
        raise SubjectSolContractError("sol_review_status_inconsistent")
    _dispatcher_effect(receipt.get("dispatcher_effect"), subject)
    if receipt.get("formal_write_count") != 0:
        raise SubjectSolContractError("sol_review_formal_write_count_nonzero")
    _timestamp(receipt.get("completed_at"), "sol_review_completed_at")
    _seal_shape(receipt.get("seal"))
    if batch is not None:
        authorized = validate_daily_sol_batch_v2(batch)
        if (
            receipt["batch_id"] != authorized["batch_id"]
            or subject != authorized["subject"]
            or sorted(seen) != authorized["proposal_sha256s"]
        ):
            raise SubjectSolContractError("sol_review_batch_binding_invalid")
    return receipt


def validate_sol_commit_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any]
) -> dict[str, Any]:
    authorized = validate_daily_sol_batch_v2(batch)
    receipt = dict(_mapping(value, "sol_commit_receipt"))
    required = {
        "schema_version",
        "batch_id",
        "subject",
        "fencing_token",
        "writer",
        "writer_adapter",
        "execution_result_sha256",
        "sol_review_receipt_sha256",
        "pre_state_sha256",
        "post_state_sha256",
        "operations",
        "writer_process",
        "transaction_end",
        "recovery_position_sha256",
        "rollback_receipt_sha256",
        "status",
        "dispatcher_effect",
        "formal_write_count",
        "completed_at",
        "seal",
    }
    if set(receipt) != required or receipt.get("schema_version") != SOL_COMMIT_RECEIPT_SCHEMA:
        raise SubjectSolContractError("sol_commit_receipt_shape_invalid")
    subject = _subject(receipt.get("subject"))
    if receipt.get("batch_id") != authorized["batch_id"] or subject != authorized["subject"]:
        raise SubjectSolContractError("sol_commit_batch_binding_invalid")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    if receipt.get("writer") != "sol" or receipt.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]:
        raise SubjectSolContractError("sol_commit_writer_invalid")
    for field in (
        "execution_result_sha256",
        "sol_review_receipt_sha256",
        "pre_state_sha256",
        "post_state_sha256",
    ):
        _sha256(receipt.get(field), field)
    if not isinstance(receipt.get("operations"), list):
        raise SubjectSolContractError("sol_commit_operations_invalid")
    writer_process = dict(
        _mapping(receipt.get("writer_process"), "writer_process")
    )
    if set(writer_process) != {
        "adapter_run_id", "pid", "terminal_state", "exit_code",
        "stopped_at", "evidence_sha256",
    }:
        raise SubjectSolContractError("writer_process_shape_invalid")
    _nonempty(writer_process.get("adapter_run_id"), "adapter_run_id")
    _integer(writer_process.get("pid"), "writer_pid", minimum=1)
    if writer_process.get("terminal_state") != "stopped":
        raise SubjectSolContractError("writer_process_not_stopped")
    _integer(writer_process.get("exit_code"), "writer_exit_code")
    _timestamp(writer_process.get("stopped_at"), "writer_stopped_at")
    _sha256(writer_process.get("evidence_sha256"), "writer_process_evidence_sha256")
    transaction = dict(
        _mapping(receipt.get("transaction_end"), "transaction_end")
    )
    if set(transaction) != {
        "transaction_id", "state", "ended_at", "evidence_sha256",
    }:
        raise SubjectSolContractError("transaction_end_shape_invalid")
    _nonempty(transaction.get("transaction_id"), "transaction_id")
    if transaction.get("state") not in {
        "committed", "already_current", "failed", "rolled_back"
    }:
        raise SubjectSolContractError("transaction_end_state_invalid")
    _timestamp(transaction.get("ended_at"), "transaction_ended_at")
    _sha256(transaction.get("evidence_sha256"), "transaction_end_evidence_sha256")
    if receipt.get("status") not in {"committed", "already_current", "failed", "rolled_back"}:
        raise SubjectSolContractError("sol_commit_status_invalid")
    if transaction["state"] != receipt["status"]:
        raise SubjectSolContractError("transaction_end_status_mismatch")
    recovery = _optional_sha256(
        receipt.get("recovery_position_sha256"), "recovery_position_sha256"
    )
    rollback = _optional_sha256(
        receipt.get("rollback_receipt_sha256"), "rollback_receipt_sha256"
    )
    if receipt["status"] == "failed" and recovery is None:
        raise SubjectSolContractError("failed_apply_recovery_position_missing")
    if receipt["status"] == "rolled_back" and rollback is None:
        raise SubjectSolContractError("rolled_back_apply_receipt_missing")
    if receipt["status"] in {"committed", "already_current"} and (
        recovery is not None or rollback is not None
    ):
        raise SubjectSolContractError("successful_apply_recovery_evidence_invalid")
    count = _integer(receipt.get("formal_write_count"), "formal_write_count")
    if receipt["status"] != "committed" and count != 0:
        raise SubjectSolContractError("noncommit_formal_write_count_nonzero")
    _dispatcher_effect(receipt.get("dispatcher_effect"), subject)
    _timestamp(receipt.get("completed_at"), "sol_commit_completed_at")
    _seal_shape(receipt.get("seal"))
    return receipt


def validate_english_legacy_batch_authorization_v1(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    authorization = dict(_mapping(value, "english_legacy_batch_authorization"))
    required = {
        "schema_version", "issue_id", "subject", "batch_authorization_id",
        "intent_sha256", "inventory_sha256", "target_set_sha256",
        "target_count", "authority_generation", "authority_fingerprint",
        "disposition", "authorized_operations", "exact_inventory_only",
        "user_message_sha256", "authorized_at", "materialized_at",
        "model_call_count", "formal_write_count", "authority_key_id", "seal",
    }
    if (
        set(authorization) != required
        or authorization.get("schema_version")
        != ENGLISH_LEGACY_BATCH_AUTHORIZATION_SCHEMA
        or authorization.get("issue_id") != "EN-P0-006"
        or authorization.get("subject") != "english"
        or authorization.get("disposition") != "deterministic_recuration"
        or authorization.get("authorized_operations")
        != ["luna_recuration", "sol_review", "sol_apply"]
        or authorization.get("exact_inventory_only") is not True
        or authorization.get("model_call_count") != 0
        or authorization.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_batch_authorization_invalid")
    _nonempty(authorization.get("batch_authorization_id"), "batch_authorization_id")
    for field in (
        "intent_sha256", "inventory_sha256", "target_set_sha256",
        "authority_fingerprint", "user_message_sha256", "authority_key_id",
    ):
        _sha256(authorization.get(field), field)
    _integer(authorization.get("target_count"), "target_count", minimum=1)
    _nonempty(authorization.get("authority_generation"), "authority_generation")
    _timestamp(authorization.get("authorized_at"), "authorized_at")
    _timestamp(authorization.get("materialized_at"), "materialized_at")
    seal = _seal_shape(authorization.get("seal"))
    if seal["purpose"] != "english-legacy-batch-authorization-v1":
        raise SubjectSolContractError("english_legacy_batch_authorization_seal_invalid")
    return authorization


def _validate_english_legacy_work_item(value: Any) -> dict[str, Any]:
    item = dict(_mapping(value, "english_legacy_recuration_work_item"))
    required = {
        "schema_version", "parent_batch_authorization_sha256",
        "inventory_sha256", "authorization_event_sha256",
        "target_authorization_receipt_sha256", "target_id", "target_kind",
        "ordinal", "current_object_sha256", "work_item_sha256",
        "authority_generation", "authority_fingerprint", "proposal_sha256",
        "package_sha256", "quality_receipt_sha256", "status",
        "luna_terminal", "quality_passed", "quality_outcome",
        "proposed_action", "model_call_count", "formal_write_count",
        "idempotency_key",
    }
    if set(item) != required or item.get("schema_version") != ENGLISH_LEGACY_SOL_WORK_ITEM_SCHEMA:
        raise SubjectSolContractError("english_legacy_work_item_shape_invalid")
    for field in (
        "parent_batch_authorization_sha256", "inventory_sha256",
        "authorization_event_sha256", "target_authorization_receipt_sha256",
        "current_object_sha256", "work_item_sha256", "authority_fingerprint",
        "proposal_sha256", "package_sha256", "quality_receipt_sha256",
    ):
        _sha256(item.get(field), field)
    _nonempty(item.get("target_id"), "target_id")
    if item.get("target_kind") not in ENGLISH_LEGACY_TARGET_KINDS:
        raise SubjectSolContractError("english_legacy_target_kind_invalid")
    _integer(item.get("ordinal"), "ordinal", minimum=1)
    _nonempty(item.get("authority_generation"), "authority_generation")
    _nonempty(item.get("idempotency_key"), "idempotency_key")
    if (
        item.get("status") != "quality_passed"
        or item.get("luna_terminal") is not True
        or item.get("quality_passed") is not True
        or item.get("quality_outcome") not in {"accepted", "corrected"}
        or item.get("proposed_action") not in ENGLISH_LEGACY_PROPOSAL_ACTIONS
        or item.get("model_call_count") != 2
        or item.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_work_item_not_sol_ready")
    return item


def validate_english_legacy_recuration_sol_batch_v1(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    batch = dict(_mapping(value, "english_legacy_recuration_sol_batch"))
    required = {
        "schema_version", "issue_id", "batch_id", "subject",
        "batch_authorization_sha256", "authorization_expansion_closure_sha256",
        "inventory_sha256",
        "target_set_sha256", "target_count", "authority_generation",
        "authority_fingerprint", "authorized_at", "work_items", "status",
        "formal_write_count",
    }
    if (
        set(batch) != required
        or batch.get("schema_version") != ENGLISH_LEGACY_SOL_BATCH_SCHEMA
        or batch.get("issue_id") != "EN-P0-006"
        or batch.get("subject") != "english"
        or batch.get("status") != "authorized"
        or batch.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_sol_batch_shape_invalid")
    _nonempty(batch.get("batch_id"), "batch_id")
    for field in (
        "batch_authorization_sha256", "authorization_expansion_closure_sha256",
        "inventory_sha256", "target_set_sha256", "authority_fingerprint",
    ):
        _sha256(batch.get(field), field)
    target_count = _integer(batch.get("target_count"), "target_count", minimum=1)
    _nonempty(batch.get("authority_generation"), "authority_generation")
    _timestamp(batch.get("authorized_at"), "authorized_at")
    items = [
        _validate_english_legacy_work_item(row)
        for row in _sequence(batch.get("work_items"), "work_items", nonempty=True)
    ]
    if len(items) != target_count:
        raise SubjectSolContractError("english_legacy_target_count_mismatch")
    target_ids = [str(row["target_id"]) for row in items]
    if target_ids != sorted(target_ids) or len(target_ids) != len(set(target_ids)):
        raise SubjectSolContractError("english_legacy_target_order_invalid")
    if [row["ordinal"] for row in items] != list(range(1, target_count + 1)):
        raise SubjectSolContractError("english_legacy_ordinal_sequence_invalid")
    if any(
        row["parent_batch_authorization_sha256"]
        != batch["batch_authorization_sha256"]
        or row["inventory_sha256"] != batch["inventory_sha256"]
        or row["authority_generation"] != batch["authority_generation"]
        or row["authority_fingerprint"] != batch["authority_fingerprint"]
        for row in items
    ):
        raise SubjectSolContractError("english_legacy_work_item_parent_mismatch")
    for field in (
        "authorization_event_sha256", "target_authorization_receipt_sha256",
        "work_item_sha256", "proposal_sha256", "package_sha256",
        "quality_receipt_sha256", "idempotency_key",
    ):
        values = [row[field] for row in items]
        if len(values) != len(set(values)):
            raise SubjectSolContractError(f"english_legacy_{field}_duplicate")
    batch["work_items"] = items
    return batch


def _english_legacy_item(
    batch: Mapping[str, Any], ordinal: int,
) -> dict[str, Any]:
    checked = validate_english_legacy_recuration_sol_batch_v1(batch)
    checked_ordinal = _integer(ordinal, "ordinal", minimum=1)
    if checked_ordinal > checked["target_count"]:
        raise SubjectSolContractError("english_legacy_item_not_found")
    return dict(checked["work_items"][checked_ordinal - 1])


def _validate_writer_stopped(value: Any) -> dict[str, Any]:
    process = dict(_mapping(value, "writer_process"))
    if set(process) != {
        "adapter_run_id", "pid", "terminal_state", "exit_code", "stopped_at",
        "evidence_sha256",
    }:
        raise SubjectSolContractError("english_legacy_writer_process_shape_invalid")
    _nonempty(process.get("adapter_run_id"), "adapter_run_id")
    _integer(process.get("pid"), "writer_pid", minimum=1)
    if process.get("terminal_state") != "stopped":
        raise SubjectSolContractError("english_legacy_writer_process_not_stopped")
    _integer(process.get("exit_code"), "writer_exit_code")
    _timestamp(process.get("stopped_at"), "writer_stopped_at")
    _sha256(process.get("evidence_sha256"), "writer_process_evidence_sha256")
    return process


def _validate_transaction_end(value: Any, *, expected: set[str]) -> dict[str, Any]:
    transaction = dict(_mapping(value, "transaction_end"))
    if set(transaction) != {"transaction_id", "state", "ended_at", "evidence_sha256"}:
        raise SubjectSolContractError("english_legacy_transaction_end_shape_invalid")
    _nonempty(transaction.get("transaction_id"), "transaction_id")
    if transaction.get("state") not in expected:
        raise SubjectSolContractError("english_legacy_transaction_end_state_invalid")
    _timestamp(transaction.get("ended_at"), "transaction_ended_at")
    _sha256(transaction.get("evidence_sha256"), "transaction_end_evidence_sha256")
    return transaction


def validate_english_legacy_sol_item_review_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    receipt = dict(_mapping(value, "english_legacy_sol_item_review_receipt"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "target_id", "target_kind", "ordinal", "fencing_token", "attempt",
        "previous_checkpoint_sha256", "canonical_evidence_sha256",
        "authority_checkpoint_sha256", "current_object_sha256", "decision",
        "desired_object_sha256",
        "reason", "status", "formal_write_count", "completed_at", "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != ENGLISH_LEGACY_ITEM_REVIEW_SCHEMA
        or receipt.get("issue_id") != "EN-P0-006"
        or receipt.get("subject") != "english"
        or receipt.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_item_review_shape_invalid")
    _nonempty(receipt.get("batch_id"), "batch_id")
    _nonempty(receipt.get("target_id"), "target_id")
    if receipt.get("target_kind") not in ENGLISH_LEGACY_TARGET_KINDS:
        raise SubjectSolContractError("english_legacy_target_kind_invalid")
    for field in (
        "batch_sha256", "canonical_evidence_sha256",
        "authority_checkpoint_sha256", "current_object_sha256",
    ):
        _sha256(receipt.get(field), field)
    _optional_sha256(receipt.get("previous_checkpoint_sha256"), "previous_checkpoint_sha256")
    desired = _optional_sha256(receipt.get("desired_object_sha256"), "desired_object_sha256")
    _integer(receipt.get("ordinal"), "ordinal", minimum=1)
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    _integer(receipt.get("attempt"), "attempt", minimum=1)
    decision = receipt.get("decision")
    status = receipt.get("status")
    if decision not in ENGLISH_LEGACY_SOL_ACTIONS | {"reject"}:
        raise SubjectSolContractError("english_legacy_item_review_decision_invalid")
    if status not in {"approved", "rejected", "failed"}:
        raise SubjectSolContractError("english_legacy_item_review_status_invalid")
    if (status == "approved") != (decision in ENGLISH_LEGACY_SOL_ACTIONS):
        raise SubjectSolContractError("english_legacy_item_review_status_inconsistent")
    if (decision == "update_existing") != (desired is not None):
        raise SubjectSolContractError("english_legacy_item_review_desired_hash_invalid")
    _nonempty(receipt.get("reason"), "review_reason")
    _timestamp(receipt.get("completed_at"), "completed_at")
    if _seal_shape(receipt.get("seal"))["purpose"] != "english-legacy-sol-item-review-receipt-v1":
        raise SubjectSolContractError("english_legacy_item_review_seal_invalid")
    if batch is not None:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        item = _english_legacy_item(checked_batch, int(receipt["ordinal"]))
        if (
            receipt["batch_id"] != checked_batch["batch_id"]
            or receipt["batch_sha256"] != _document_sha256(checked_batch)
            or receipt["target_id"] != item["target_id"]
            or receipt["target_kind"] != item["target_kind"]
            or receipt["current_object_sha256"] != item["current_object_sha256"]
            or (
                receipt["decision"] in ENGLISH_LEGACY_SOL_ACTIONS
                and receipt["decision"]
                != item["proposed_action"].removesuffix("_proposal")
            )
        ):
            raise SubjectSolContractError("english_legacy_item_review_batch_mismatch")
    return receipt


def validate_english_legacy_sol_item_apply_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any],
) -> dict[str, Any]:
    checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
    receipt = dict(_mapping(value, "english_legacy_sol_item_apply_receipt"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "target_id", "target_kind", "ordinal", "fencing_token", "attempt",
        "previous_checkpoint_sha256", "review_receipt_sha256",
        "pre_authority_sha256", "post_authority_sha256",
        "pre_object_sha256", "post_object_sha256", "operations",
        "writer_process", "transaction_end", "status", "formal_write_count",
        "completed_at", "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != ENGLISH_LEGACY_ITEM_APPLY_SCHEMA
        or receipt.get("issue_id") != "EN-P0-006"
        or receipt.get("subject") != "english"
        or receipt.get("status") not in {"committed", "already_current"}
    ):
        raise SubjectSolContractError("english_legacy_item_apply_shape_invalid")
    item = _english_legacy_item(checked_batch, _integer(receipt.get("ordinal"), "ordinal", minimum=1))
    if (
        receipt.get("batch_id") != checked_batch["batch_id"]
        or receipt.get("batch_sha256") != _document_sha256(checked_batch)
        or receipt.get("target_id") != item["target_id"]
        or receipt.get("target_kind") != item["target_kind"]
    ):
        raise SubjectSolContractError("english_legacy_item_apply_batch_mismatch")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    _integer(receipt.get("attempt"), "attempt", minimum=1)
    _optional_sha256(receipt.get("previous_checkpoint_sha256"), "previous_checkpoint_sha256")
    _sha256(receipt.get("review_receipt_sha256"), "review_receipt_sha256")
    pre_authority = _sha256(
        receipt.get("pre_authority_sha256"), "pre_authority_sha256"
    )
    post_authority = _sha256(
        receipt.get("post_authority_sha256"), "post_authority_sha256"
    )
    pre_hash = _sha256(receipt.get("pre_object_sha256"), "pre_object_sha256")
    post_hash = _sha256(receipt.get("post_object_sha256"), "post_object_sha256")
    if pre_hash != item["current_object_sha256"]:
        raise SubjectSolContractError("english_legacy_apply_object_cas_mismatch")
    operations = _sequence(receipt.get("operations"), "operations")
    for raw in operations:
        operation = dict(_mapping(raw, "english_legacy_update_operation"))
        if set(operation) != {
            "operation", "target_id", "target_kind", "before_object_sha256",
            "after_object_sha256",
        } or operation.get("operation") != "update_existing":
            raise SubjectSolContractError("english_legacy_create_or_invalid_operation")
        if operation.get("target_id") != item["target_id"] or operation.get("target_kind") != item["target_kind"]:
            raise SubjectSolContractError("english_legacy_operation_target_mismatch")
        if (
            _sha256(operation.get("before_object_sha256"), "before_object_sha256") != pre_hash
            or _sha256(operation.get("after_object_sha256"), "after_object_sha256") != post_hash
        ):
            raise SubjectSolContractError("english_legacy_operation_hash_mismatch")
    count = _integer(receipt.get("formal_write_count"), "formal_write_count")
    if receipt["status"] == "already_current":
        if (
            operations
            or count != 0
            or pre_hash != post_hash
            or pre_authority != post_authority
        ):
            raise SubjectSolContractError("english_legacy_already_current_not_zero_write")
    elif (
        not operations
        or count != len(operations)
        or pre_hash == post_hash
        or pre_authority == post_authority
    ):
        raise SubjectSolContractError("english_legacy_committed_operation_invalid")
    _validate_writer_stopped(receipt.get("writer_process"))
    transaction = _validate_transaction_end(
        receipt.get("transaction_end"), expected={"committed", "already_current"}
    )
    if transaction["state"] != receipt["status"]:
        raise SubjectSolContractError("english_legacy_apply_transaction_mismatch")
    _timestamp(receipt.get("completed_at"), "completed_at")
    if _seal_shape(receipt.get("seal"))["purpose"] != "english-legacy-sol-item-apply-receipt-v1":
        raise SubjectSolContractError("english_legacy_item_apply_seal_invalid")
    return receipt


def validate_english_legacy_sol_item_failure_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any],
) -> dict[str, Any]:
    checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
    receipt = dict(_mapping(value, "english_legacy_sol_item_failure_receipt"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "target_id", "target_kind", "ordinal", "fencing_token", "attempt",
        "previous_checkpoint_sha256", "review_receipt_sha256", "failure_stage",
        "authority_checkpoint_sha256", "reason", "recovery_position_sha256", "writer_process",
        "transaction_end", "retryable", "formal_write_count", "failed_at", "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != ENGLISH_LEGACY_ITEM_FAILURE_SCHEMA
        or receipt.get("issue_id") != "EN-P0-006"
        or receipt.get("subject") != "english"
        or receipt.get("failure_stage") not in {"sol_review", "formal_apply"}
        or receipt.get("formal_write_count") != 0
        or not isinstance(receipt.get("retryable"), bool)
    ):
        raise SubjectSolContractError("english_legacy_item_failure_shape_invalid")
    item = _english_legacy_item(checked_batch, _integer(receipt.get("ordinal"), "ordinal", minimum=1))
    if (
        receipt.get("batch_id") != checked_batch["batch_id"]
        or receipt.get("batch_sha256") != _document_sha256(checked_batch)
        or receipt.get("target_id") != item["target_id"]
        or receipt.get("target_kind") != item["target_kind"]
    ):
        raise SubjectSolContractError("english_legacy_item_failure_batch_mismatch")
    _integer(receipt.get("fencing_token"), "fencing_token", minimum=1)
    _integer(receipt.get("attempt"), "attempt", minimum=1)
    _optional_sha256(receipt.get("previous_checkpoint_sha256"), "previous_checkpoint_sha256")
    _optional_sha256(receipt.get("review_receipt_sha256"), "review_receipt_sha256")
    _sha256(
        receipt.get("authority_checkpoint_sha256"),
        "authority_checkpoint_sha256",
    )
    _nonempty(receipt.get("reason"), "failure_reason")
    _sha256(receipt.get("recovery_position_sha256"), "recovery_position_sha256")
    _validate_writer_stopped(receipt.get("writer_process"))
    _validate_transaction_end(receipt.get("transaction_end"), expected={"failed", "rolled_back"})
    _timestamp(receipt.get("failed_at"), "failed_at")
    if _seal_shape(receipt.get("seal"))["purpose"] != "english-legacy-sol-item-failure-receipt-v1":
        raise SubjectSolContractError("english_legacy_item_failure_seal_invalid")
    return receipt


def validate_english_legacy_sol_item_recovery_receipt_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any],
) -> dict[str, Any]:
    checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
    receipt = dict(_mapping(value, "english_legacy_sol_item_recovery_receipt"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "target_id", "target_kind", "ordinal", "prior_fencing_token",
        "new_fencing_token", "attempt", "previous_checkpoint_sha256",
        "failure_receipt_sha256", "recovery_position_sha256",
        "authority_checkpoint_sha256", "status",
        "formal_write_count", "recovered_at", "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != ENGLISH_LEGACY_ITEM_RECOVERY_SCHEMA
        or receipt.get("issue_id") != "EN-P0-006"
        or receipt.get("subject") != "english"
        or receipt.get("status") != "resumed"
        or receipt.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_item_recovery_shape_invalid")
    item = _english_legacy_item(checked_batch, _integer(receipt.get("ordinal"), "ordinal", minimum=1))
    if (
        receipt.get("batch_id") != checked_batch["batch_id"]
        or receipt.get("batch_sha256") != _document_sha256(checked_batch)
        or receipt.get("target_id") != item["target_id"]
        or receipt.get("target_kind") != item["target_kind"]
    ):
        raise SubjectSolContractError("english_legacy_item_recovery_batch_mismatch")
    prior = _integer(receipt.get("prior_fencing_token"), "prior_fencing_token", minimum=1)
    new = _integer(receipt.get("new_fencing_token"), "new_fencing_token", minimum=1)
    if new <= prior:
        raise SubjectSolContractError("english_legacy_recovery_fence_not_advanced")
    _integer(receipt.get("attempt"), "attempt", minimum=2)
    for field in (
        "previous_checkpoint_sha256", "failure_receipt_sha256",
        "recovery_position_sha256", "authority_checkpoint_sha256",
    ):
        _sha256(receipt.get(field), field)
    _timestamp(receipt.get("recovered_at"), "recovered_at")
    if _seal_shape(receipt.get("seal"))["purpose"] != "english-legacy-sol-item-recovery-receipt-v1":
        raise SubjectSolContractError("english_legacy_item_recovery_seal_invalid")
    return receipt


def validate_english_legacy_rolling_authority_checkpoint_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any],
) -> dict[str, Any]:
    checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
    checkpoint = dict(_mapping(value, "english_legacy_rolling_checkpoint"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "fencing_token", "attempt", "ordinal", "target_id", "outcome",
        "previous_checkpoint_sha256", "item_result_receipt_sha256",
        "pre_authority_sha256", "post_authority_sha256", "completed_target_count",
        "committed_count", "already_current_count", "failed_count", "next_ordinal",
        "formal_write_count", "issued_at", "seal",
    }
    if (
        set(checkpoint) != required
        or checkpoint.get("schema_version") != ENGLISH_LEGACY_ROLLING_CHECKPOINT_SCHEMA
        or checkpoint.get("issue_id") != "EN-P0-006"
        or checkpoint.get("subject") != "english"
        or checkpoint.get("outcome") not in {"committed", "already_current", "failed"}
    ):
        raise SubjectSolContractError("english_legacy_checkpoint_shape_invalid")
    item = _english_legacy_item(
        checked_batch, _integer(checkpoint.get("ordinal"), "ordinal", minimum=1)
    )
    if (
        checkpoint.get("batch_id") != checked_batch["batch_id"]
        or checkpoint.get("batch_sha256") != _document_sha256(checked_batch)
        or checkpoint.get("target_id") != item["target_id"]
    ):
        raise SubjectSolContractError("english_legacy_checkpoint_batch_mismatch")
    _integer(checkpoint.get("fencing_token"), "fencing_token", minimum=1)
    _integer(checkpoint.get("attempt"), "attempt", minimum=1)
    _optional_sha256(checkpoint.get("previous_checkpoint_sha256"), "previous_checkpoint_sha256")
    for field in (
        "item_result_receipt_sha256", "pre_authority_sha256",
        "post_authority_sha256",
    ):
        _sha256(checkpoint.get(field), field)
    completed = _integer(checkpoint.get("completed_target_count"), "completed_target_count")
    committed = _integer(checkpoint.get("committed_count"), "committed_count")
    already = _integer(checkpoint.get("already_current_count"), "already_current_count")
    failed = _integer(checkpoint.get("failed_count"), "failed_count")
    _integer(checkpoint.get("formal_write_count"), "formal_write_count")
    if completed != committed + already + failed or completed > checked_batch["target_count"]:
        raise SubjectSolContractError("english_legacy_checkpoint_counts_invalid")
    next_ordinal = checkpoint.get("next_ordinal")
    if next_ordinal is not None:
        _integer(next_ordinal, "next_ordinal", minimum=1)
        if next_ordinal > checked_batch["target_count"]:
            raise SubjectSolContractError("english_legacy_checkpoint_next_invalid")
    _timestamp(checkpoint.get("issued_at"), "issued_at")
    if _seal_shape(checkpoint.get("seal"))["purpose"] != "english-legacy-rolling-authority-checkpoint-v1":
        raise SubjectSolContractError("english_legacy_checkpoint_seal_invalid")
    return checkpoint


def validate_english_legacy_recuration_execution_closure_v1(
    value: Mapping[str, Any], *, batch: Mapping[str, Any],
) -> dict[str, Any]:
    checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
    closure = dict(_mapping(value, "english_legacy_execution_closure"))
    required = {
        "schema_version", "issue_id", "batch_id", "batch_sha256", "subject",
        "inventory_sha256", "target_set_sha256", "target_count",
        "final_checkpoint_sha256", "item_outcomes", "committed_count",
        "already_current_count", "failed_count", "omitted_count", "unknown_count",
        "duplicate_count", "recovery_pending_count", "formal_write_count",
        "status", "completed_at", "seal",
    }
    if (
        set(closure) != required
        or closure.get("schema_version") != ENGLISH_LEGACY_EXECUTION_CLOSURE_SCHEMA
        or closure.get("issue_id") != "EN-P0-006"
        or closure.get("subject") != "english"
        or closure.get("batch_id") != checked_batch["batch_id"]
        or closure.get("batch_sha256") != _document_sha256(checked_batch)
        or closure.get("inventory_sha256") != checked_batch["inventory_sha256"]
        or closure.get("target_set_sha256") != checked_batch["target_set_sha256"]
        or closure.get("target_count") != checked_batch["target_count"]
        or closure.get("status") not in {"verified_complete", "complete_with_failures"}
    ):
        raise SubjectSolContractError("english_legacy_execution_closure_shape_invalid")
    _sha256(closure.get("final_checkpoint_sha256"), "final_checkpoint_sha256")
    rows = _sequence(closure.get("item_outcomes"), "item_outcomes", nonempty=True)
    if len(rows) != checked_batch["target_count"]:
        raise SubjectSolContractError("english_legacy_execution_closure_target_mismatch")
    seen: set[str] = set()
    seen_attempt_receipts: set[str] = set()
    total_writes = 0
    statuses: list[str] = []
    for ordinal, raw in enumerate(rows, start=1):
        row = dict(_mapping(raw, "english_legacy_item_outcome"))
        if set(row) != {
            "ordinal", "target_id", "status", "attempt_count",
            "attempts", "final_receipt_sha256", "checkpoint_sha256",
            "formal_write_count",
        }:
            raise SubjectSolContractError("english_legacy_item_outcome_shape_invalid")
        item = _english_legacy_item(checked_batch, ordinal)
        if (
            row.get("ordinal") != ordinal
            or row.get("target_id") != item["target_id"]
            or row.get("target_id") in seen
            or row.get("status") not in {"committed", "already_current", "failed"}
        ):
            raise SubjectSolContractError("english_legacy_item_outcome_invalid")
        seen.add(str(row["target_id"]))
        statuses.append(str(row["status"]))
        attempt_count = _integer(
            row.get("attempt_count"), "attempt_count", minimum=1
        )
        attempts = _sequence(row.get("attempts"), "attempts", nonempty=True)
        if len(attempts) != attempt_count:
            raise SubjectSolContractError(
                "english_legacy_item_outcome_attempt_count_invalid"
            )
        previous_fence = 0
        final_attempt_result: str | None = None
        for expected_attempt, raw_attempt in enumerate(attempts, start=1):
            attempt = dict(_mapping(raw_attempt, "execution_attempt"))
            if set(attempt) != {
                "attempt", "fencing_token", "review_receipt_sha256",
                "apply_receipt_sha256", "failure_receipt_sha256",
                "recovery_receipt_sha256",
            }:
                raise SubjectSolContractError(
                    "english_legacy_execution_attempt_shape_invalid"
                )
            fence = _integer(
                attempt.get("fencing_token"),
                "attempt_fencing_token",
                minimum=1,
            )
            if attempt.get("attempt") != expected_attempt or fence <= previous_fence:
                raise SubjectSolContractError(
                    "english_legacy_execution_attempt_sequence_invalid"
                )
            previous_fence = fence
            hashes = {
                field: _optional_sha256(attempt.get(field), field)
                for field in (
                    "review_receipt_sha256", "apply_receipt_sha256",
                    "failure_receipt_sha256", "recovery_receipt_sha256",
                )
            }
            result_count = sum(
                hashes[field] is not None
                for field in ("apply_receipt_sha256", "failure_receipt_sha256")
            )
            if (
                result_count != 1
                or (
                    hashes["apply_receipt_sha256"] is not None
                    and hashes["review_receipt_sha256"] is None
                )
                or (expected_attempt == 1)
                != (hashes["recovery_receipt_sha256"] is None)
                or (
                    expected_attempt < attempt_count
                    and hashes["failure_receipt_sha256"] is None
                )
            ):
                raise SubjectSolContractError(
                    "english_legacy_execution_attempt_evidence_invalid"
                )
            for receipt_sha in hashes.values():
                if receipt_sha is None:
                    continue
                if receipt_sha in seen_attempt_receipts:
                    raise SubjectSolContractError(
                        "english_legacy_execution_attempt_receipt_replayed"
                    )
                seen_attempt_receipts.add(receipt_sha)
            final_attempt_result = (
                hashes["apply_receipt_sha256"]
                or hashes["failure_receipt_sha256"]
            )
        final_receipt = _sha256(
            row.get("final_receipt_sha256"), "final_receipt_sha256"
        )
        if (
            final_receipt != final_attempt_result
            or (row["status"] == "failed")
            != (attempts[-1].get("failure_receipt_sha256") is not None)
        ):
            raise SubjectSolContractError(
                "english_legacy_item_outcome_final_receipt_invalid"
            )
        _sha256(row.get("checkpoint_sha256"), "checkpoint_sha256")
        total_writes += _integer(row.get("formal_write_count"), "item_formal_write_count")
    committed = _integer(closure.get("committed_count"), "committed_count")
    already = _integer(closure.get("already_current_count"), "already_current_count")
    failed = _integer(closure.get("failed_count"), "failed_count")
    for field in ("omitted_count", "unknown_count", "duplicate_count", "recovery_pending_count"):
        _integer(closure.get(field), field)
    if (
        committed != statuses.count("committed")
        or already != statuses.count("already_current")
        or failed != statuses.count("failed")
        or closure.get("formal_write_count") != total_writes
    ):
        raise SubjectSolContractError("english_legacy_execution_closure_counts_invalid")
    if closure["status"] == "verified_complete" and (
        failed
        or closure["omitted_count"]
        or closure["unknown_count"]
        or closure["duplicate_count"]
        or closure["recovery_pending_count"]
        or committed + already != checked_batch["target_count"]
    ):
        raise SubjectSolContractError("english_legacy_verified_complete_invalid")
    if closure["status"] == "complete_with_failures" and failed == 0:
        raise SubjectSolContractError("english_legacy_failure_closure_without_failure")
    _timestamp(closure.get("completed_at"), "completed_at")
    if _seal_shape(closure.get("seal"))["purpose"] != "english-legacy-recuration-execution-closure-v1":
        raise SubjectSolContractError("english_legacy_execution_closure_seal_invalid")
    return closure


def build_english_legacy_sol_work_item_v1(
    *,
    batch_authorization_sha256: str,
    authorization: Mapping[str, Any],
    authorization_mapping: Mapping[str, Any],
    work_item: Mapping[str, Any],
    quality_verification: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert one verified Luna work item into one strict Sol queue item."""

    authorization_sha = _sha256(
        batch_authorization_sha256, "batch_authorization_sha256"
    )
    checked_authorization = validate_english_legacy_batch_authorization_v1(
        authorization
    )
    mapping = dict(_mapping(authorization_mapping, "target_authorization_mapping"))
    try:
        from english_legacy_recuration import validate_work_item

        source = validate_work_item(
            _mapping(work_item, "english_legacy_luna_work_item")
        )
    except Exception as exc:
        code = getattr(exc, "code", "english_legacy_luna_work_item_invalid")
        raise SubjectSolContractError(str(code)) from exc
    quality = dict(_mapping(quality_verification, "english_legacy_quality_verification"))
    required_mapping = {
        "ordinal", "target_id", "authorization_event_sha256",
        "disposition_receipt_sha256",
    }
    if set(mapping) != required_mapping:
        raise SubjectSolContractError("english_legacy_target_authorization_mapping_invalid")
    ordinal = _integer(mapping.get("ordinal"), "ordinal", minimum=1)
    target_id = _nonempty(mapping.get("target_id"), "target_id")
    target_kind = source.get("target_kind")
    if target_kind not in ENGLISH_LEGACY_TARGET_KINDS:
        raise SubjectSolContractError("english_legacy_target_kind_invalid")
    if (
        source.get("schema_version") != "english_legacy_recuration_work_item_v1"
        or source.get("batch_authorization_sha256") != authorization_sha
        or source.get("inventory_sha256") != checked_authorization["inventory_sha256"]
        or source.get("target_authorization_receipt_sha256")
        != mapping["disposition_receipt_sha256"]
        or source.get("target_id") != target_id
        or source.get("ordinal") != ordinal
        or quality.get("schema_version")
        != "verified_english_legacy_recuration_quality_v1"
        or quality.get("target_id") != target_id
        or quality.get("target_kind") != target_kind
        or quality.get("ordinal") != ordinal
        or quality.get("inventory_sha256") != checked_authorization["inventory_sha256"]
        or quality.get("batch_authorization_sha256") != authorization_sha
        or quality.get("target_authorization_receipt_sha256")
        != mapping["disposition_receipt_sha256"]
        or quality.get("quality_outcome") not in {"accepted", "corrected"}
        or quality.get("proposal_action") not in ENGLISH_LEGACY_PROPOSAL_ACTIONS
        or quality.get("model_call_count") != 2
        or quality.get("formal_write_count") != 0
    ):
        raise SubjectSolContractError("english_legacy_quality_not_sol_ready")
    current_object_sha = _sha256(
        source.get("current_object_sha256"), "current_object_sha256"
    )
    work_item_sha = _sha256(quality.get("work_item_sha256"), "work_item_sha256")
    package_sha = _sha256(quality.get("package_sha256"), "package_sha256")
    evidence = dict(_mapping(source.get("target_evidence"), "target_evidence"))
    evidence_payload = dict(
        _mapping(evidence.get("payload"), "target_evidence_payload")
    )
    evidence_authorization = dict(
        _mapping(
            evidence_payload.get("authorization"),
            "target_evidence_authorization",
        )
    )
    if (
        _document_sha256(source) != work_item_sha
        or evidence_authorization.get("batch_authorization_sha256")
        != authorization_sha
        or evidence_authorization.get("target_authorization_event_sha256")
        != mapping["authorization_event_sha256"]
        or evidence_authorization.get("target_authorization_receipt_sha256")
        != mapping["disposition_receipt_sha256"]
        or evidence_authorization.get("existing_object_only") is not True
    ):
        raise SubjectSolContractError(
            "english_legacy_work_item_authorization_binding_mismatch"
        )
    value = {
        "schema_version": ENGLISH_LEGACY_SOL_WORK_ITEM_SCHEMA,
        "parent_batch_authorization_sha256": authorization_sha,
        "inventory_sha256": checked_authorization["inventory_sha256"],
        "authorization_event_sha256": _sha256(
            mapping.get("authorization_event_sha256"),
            "authorization_event_sha256",
        ),
        "target_authorization_receipt_sha256": _sha256(
            mapping.get("disposition_receipt_sha256"),
            "target_authorization_receipt_sha256",
        ),
        "target_id": target_id,
        "target_kind": target_kind,
        "ordinal": ordinal,
        "current_object_sha256": current_object_sha,
        "work_item_sha256": work_item_sha,
        "authority_generation": checked_authorization["authority_generation"],
        "authority_fingerprint": checked_authorization["authority_fingerprint"],
        "proposal_sha256": _sha256(
            quality.get("proposal_sha256"), "proposal_sha256"
        ),
        "package_sha256": package_sha,
        "quality_receipt_sha256": _sha256(
            quality.get("receipt_sha256"), "quality_receipt_sha256"
        ),
        "status": "quality_passed",
        "luna_terminal": True,
        "quality_passed": True,
        "quality_outcome": quality["quality_outcome"],
        "proposed_action": quality["proposal_action"],
        "model_call_count": 2,
        "formal_write_count": 0,
        "idempotency_key": "sha256:" + _value_sha256(
            {
                "batch_authorization_sha256": authorization_sha,
                "target_id": target_id,
                "package_sha256": package_sha,
            }
        ),
    }
    return _validate_english_legacy_work_item(value)


class _FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Any = None

    def __enter__(self) -> "_FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.handle = self.path.open("a+b")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *_args: object) -> None:
        assert self.handle is not None
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


class _ExistingFileLock(_FileLock):
    """Lock an existing file without creating any readiness surface."""

    def __enter__(self) -> "_ExistingFileLock":
        try:
            self.handle = self.path.open("rb")
        except OSError as exc:
            raise SubjectSolContractError(
                "canary_readiness_lock_unavailable"
            ) from exc
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_SH)
        return self


class SubjectSolRuntimeStore:
    """Durable Luna batch and globally serialized Sol control state."""

    def __init__(self, runtime_root: Path) -> None:
        self.runtime_root = runtime_root.resolve()
        self.dispatch_root = self.runtime_root / "dispatch"
        self.state_root = self.dispatch_root / "state"
        self.subject_batch_root = self.state_root / "subject-luna-batches"
        self.subject_writer_root = self.state_root / "subject-sol"
        self.global_state_path = self.state_root / "global-sol-writer.json"
        self.global_lock_path = self.state_root / "global-sol-writer.lock"
        self.subject_lock_root = self.state_root / "subject-sol-locks"
        self.authority_key_path = self.state_root / "authority.key"
        self.user_authority_root = self.state_root / "external-authorities"
        self.user_sol_authority_key_path = (
            self.user_authority_root / "user-sol-authorization.key"
        )
        self.user_exclusion_authority_key_path = (
            self.user_authority_root / "user-subject-exclusion-authorization.key"
        )
        self.writer_adapter_authority_key_path = (
            self.user_authority_root / "sol-writer-adapter.key"
        )
        self.subject_authority_observer_key_path = (
            self.user_authority_root / "subject-mcp-authority-observer.key"
        )
        self.receipt_root = self.dispatch_root / "control-receipts"
        self.subject_quality_v2_root = (
            self.receipt_root / "subject-quality-v2"
        )
        self.subject_batch_snapshot_root = (
            self.dispatch_root / "subject-luna-batch-snapshots"
        )
        self.subject_batch_pointer_root = self.state_root / "subject-luna-batch-pointers"
        self.subject_batch_archive_root = self.dispatch_root / "subject-luna-batch-archives"
        self.background_rollover_receipt_root = (
            self.receipt_root / "subject-background-rollovers"
        )
        self.background_rollover_intent_root = (
            self.state_root / "subject-background-rollover-intents"
        )
        self.background_rollover_pointer_root = (
            self.state_root / "subject-background-rollovers"
        )
        self.background_rollover_rollback_receipt_root = (
            self.receipt_root / "subject-background-rollbacks"
        )
        self.background_rollover_rollback_intent_root = (
            self.state_root / "subject-background-rollback-intents"
        )
        self.cs408_terminal_retirement_receipt_root = (
            self.receipt_root / "cs408-terminal-batch-retirements"
        )
        self.cs408_terminal_retirement_pointer_path = (
            self.state_root
            / "cs408-terminal-batch-retirements"
            / "current.json"
        )
        self.cs408_terminal_retirement_intent_path = (
            self.state_root
            / "cs408-terminal-batch-retirement-intents"
            / "authorized-2026-08-12.json"
        )
        self.cs408_terminal_retirement_rollback_receipt_root = (
            self.receipt_root / "cs408-terminal-batch-retirement-rollbacks"
        )
        self.cs408_terminal_retirement_rollback_intent_root = (
            self.state_root / "cs408-terminal-batch-retirement-rollback-intents"
        )
        self.cs408_terminal_retirement_archive_root = (
            self.dispatch_root / "cs408-terminal-batch-retirement-archives"
        )
        self.english_preserved_review_batch_archive_root = (
            self.dispatch_root
            / "english-preserved-review-batch-retirement-archives"
        )
        self.english_preserved_review_batch_pointer_path = (
            self.state_root
            / "english-preserved-review-batch-retirements"
            / "authorized-33548.json"
        )
        self.english_preserved_review_batch_rollback_root = (
            self.receipt_root
            / "english-preserved-review-batch-retirement-rollbacks"
        )
        self.sol_batch_root = self.dispatch_root / "sol-batches"
        self.sol_task_handoff_root = self.dispatch_root / "sol-task-handoffs"
        self.user_sol_authorization_root = (
            self.dispatch_root / "user-sol-authorizations"
        )
        self.user_exclusion_authorization_root = (
            self.dispatch_root / "user-subject-exclusion-authorizations"
        )
        self.consumed_user_exclusion_root = (
            self.state_root / "consumed-user-subject-exclusion-authorizations"
        )
        self.writer_review_receipt_root = (
            self.dispatch_root / "writer-adapter-receipts" / "sol-review"
        )
        self.writer_apply_receipt_root = (
            self.dispatch_root / "writer-adapter-receipts" / "writer-apply"
        )
        self.writer_execution_result_root = (
            self.dispatch_root / "writer-adapter-receipts" / "execution-result"
        )
        self.writer_artifact_root = (
            self.dispatch_root / "writer-adapter-receipts" / "artifacts"
        )
        self.subject_authority_observation_root = (
            self.dispatch_root / "subject-authority-observations"
        )
        self.english_legacy_root = self.dispatch_root / "english-legacy"
        self.english_legacy_batch_authorization_root = (
            self.english_legacy_root / "batch-authorizations"
        )
        self.english_legacy_target_authorization_event_root = (
            self.english_legacy_root / "authorization-events"
        )
        self.english_legacy_disposition_receipt_root = (
            self.english_legacy_root / "dispositions"
        )
        self.english_legacy_authorization_closure_root = (
            self.english_legacy_root / "authorization-closures"
        )
        self.english_legacy_batch_authority_key_path = (
            self.user_authority_root / "english-legacy-batch-authorization.key"
        )
        self.english_legacy_sol_batch_root = (
            self.english_legacy_root / "sol-batches"
        )
        self.english_legacy_sol_state_root = (
            self.state_root / "english-legacy-sol-batches"
        )
        self.english_legacy_item_review_root = (
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-review"
        )
        self.english_legacy_item_apply_root = (
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-apply"
        )
        self.english_legacy_item_failure_root = (
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-failure"
        )
        self.english_legacy_item_recovery_root = (
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-recovery"
        )
        self.english_legacy_checkpoint_root = (
            self.english_legacy_root / "rolling-checkpoints"
        )
        self.english_legacy_execution_closure_root = (
            self.english_legacy_root / "execution-closures"
        )

    def _subject_lock_path(self, subject: str) -> Path:
        return self.subject_lock_root / f"{_subject(subject)}.lock"

    def _quality_subject_package_path(
        self,
        *,
        subject: str,
        study_date: str,
        capture_id: str,
        input_fingerprint: str,
        package_sha256: str,
        error_prefix: str,
    ) -> Path:
        """Locate the already-authoritative Worker package representation."""

        checked = _subject(subject)
        _nonempty(study_date, "study_date")
        _nonempty(capture_id, "capture_id")
        _sha256(input_fingerprint, "input_fingerprint")
        digest = _sha256(package_sha256, "package_sha256")
        try:
            from preprocessor_core import (  # type: ignore
                PreprocessorError as PackageLocatorError,
                canonical_subject_package_path,
            )
        except ImportError as exc:
            raise SubjectSolContractError(
                f"{error_prefix}_path_invalid"
            ) from exc
        try:
            package_path = canonical_subject_package_path(
                self.runtime_root,
                subject=checked,
                study_date=study_date,
                capture_id=capture_id,
                input_fingerprint=input_fingerprint,
                package_sha256=digest,
            )
        except (OSError, ValueError, PackageLocatorError) as exc:
            raise SubjectSolContractError(
                f"{error_prefix}_path_invalid"
            ) from exc
        try:
            relative = package_path.relative_to(self.runtime_root)
        except ValueError as exc:
            raise SubjectSolContractError(
                f"{error_prefix}_path_invalid"
            ) from exc
        cursor = self.runtime_root
        for part in relative.parts:
            cursor = cursor / part
            if cursor.is_symlink():
                raise SubjectSolContractError(
                    f"{error_prefix}_path_invalid"
                )
        if not package_path.is_file():
            raise SubjectSolContractError(f"{error_prefix}_missing")
        try:
            resolved = package_path.resolve(strict=True)
        except OSError as exc:
            raise SubjectSolContractError(
                f"{error_prefix}_missing"
            ) from exc
        if resolved != package_path:
            raise SubjectSolContractError(
                f"{error_prefix}_path_invalid"
            )
        return package_path

    def _batch_path(self, subject: str) -> Path:
        return self.subject_batch_root / f"{_subject(subject)}.json"

    def _batch_pointer_path(self, subject: str) -> Path:
        return self.subject_batch_pointer_root / f"{_subject(subject)}.json"

    def _writer_path(self, subject: str) -> Path:
        return self.subject_writer_root / f"{_subject(subject)}.json"

    def _background_rollover_intent_path(
        self, subject: str, batch_id: str
    ) -> Path:
        return (
            self.background_rollover_intent_root
            / _subject(subject)
            / f"{_safe_component(batch_id)}.json"
        )

    def _background_rollover_v2_intent_path(
        self, subject: str, batch_id: str
    ) -> Path:
        return (
            self.background_rollover_intent_root
            / _subject(subject)
            / f"{_safe_component(batch_id)}.v2.json"
        )

    def _background_rollover_pointer_path(self, subject: str) -> Path:
        return self.background_rollover_pointer_root / f"{_subject(subject)}.json"

    def _background_rollover_rollback_intent_path(
        self, receipt_sha256: str
    ) -> Path:
        digest = _sha256(receipt_sha256, "recovery_receipt_sha256")
        return self.background_rollover_rollback_intent_root / f"{digest}.json"

    @staticmethod
    def _retire_mutable_recovery_intent(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _retire_background_rollover_v2_intents(
        self,
        *,
        subject: str,
        batch_id: str,
        recovery_receipt_sha256: str,
    ) -> None:
        # Delete the old-batch keyed intent first.  If a crash interrupts the
        # cleanup, the receipt-keyed rollback intent remains sufficient to
        # finish and prove the same compensation on restart.
        self._retire_mutable_recovery_intent(
            self._background_rollover_v2_intent_path(subject, batch_id)
        )
        self._retire_mutable_recovery_intent(
            self._background_rollover_rollback_intent_path(
                recovery_receipt_sha256
            )
        )

    def _immutable_background_rollback_receipt(
        self,
        *,
        recovery_receipt_sha256: str,
        rollback_token: str,
    ) -> tuple[dict[str, Any], str, Path] | None:
        matches: list[tuple[dict[str, Any], str, Path]] = []
        root = self.background_rollover_rollback_receipt_root
        for candidate in sorted(root.glob("sha256/*/*.json")):
            try:
                payload = candidate.read_bytes()
                value = json.loads(payload.decode("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SubjectSolContractError(
                    "subject_background_rollback_receipt_unreadable"
                ) from exc
            digest = hashlib.sha256(payload).hexdigest()
            if (
                candidate.stem != digest
                or candidate.parent.name != digest[:2]
                or not isinstance(value, Mapping)
                or _json_file_bytes(value) != payload
            ):
                raise SubjectSolContractError(
                    "subject_background_rollback_receipt_hash_mismatch"
                )
            if (
                value.get("recovery_receipt_sha256")
                != recovery_receipt_sha256
            ):
                continue
            self._verify_seal(
                value, purpose="subject-background-luna-rollback-v1"
            )
            if (
                value.get("schema_version")
                != "subject_background_luna_recovery_rollback_receipt_v1"
                or value.get("rollback_token") != rollback_token
                or value.get("model_call_count") != 0
                or value.get("provider_request_count") != 0
                or value.get("mcp_tool_call_count") != 0
                or value.get("formal_write_count") != 0
                or value.get("sol_enabled") is not False
            ):
                raise SubjectSolContractError(
                    "subject_background_rollback_receipt_invalid"
                )
            matches.append((dict(value), digest, candidate))
        if len(matches) > 1:
            raise SubjectSolContractError(
                "subject_background_rollback_receipt_ambiguous"
            )
        return matches[0] if matches else None

    def _english_legacy_state_path(self, batch_id: str) -> Path:
        return self.english_legacy_sol_state_root / f"{_safe_component(batch_id)}.json"

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(_json_file_bytes(value))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _read_json(path: Path, label: str) -> dict[str, Any] | None:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SubjectSolContractError(f"{label}_unreadable") from exc
        if not isinstance(value, Mapping):
            raise SubjectSolContractError(f"{label}_invalid")
        return dict(value)

    def _authority_key(self, *, create: bool) -> bytes:
        path = self.authority_key_path
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if create and not path.exists():
            try:
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(secrets.token_bytes(32))
                    handle.flush()
                    os.fsync(handle.fileno())
        try:
            mode = path.stat().st_mode & 0o777
            key = path.read_bytes()
        except OSError as exc:
            raise SubjectSolContractError("control_authority_key_unavailable") from exc
        if mode & 0o077 or len(key) < 32:
            raise SubjectSolContractError("control_authority_key_invalid")
        return key

    def _seal(self, core: Mapping[str, Any], *, purpose: str) -> dict[str, Any]:
        value = copy.deepcopy(dict(core))
        if "seal" in value:
            raise SubjectSolContractError("control_receipt_already_sealed")
        mac = hmac.new(
            self._authority_key(create=True),
            _canonical_bytes({"purpose": purpose, "payload": value}),
            hashlib.sha256,
        ).hexdigest()
        return {
            **value,
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": purpose,
                "hmac_sha256": mac,
            },
        }

    def _verify_seal(self, value: Mapping[str, Any], *, purpose: str) -> None:
        document = dict(value)
        seal = _seal_shape(document.pop("seal", None))
        expected = hmac.new(
            self._authority_key(create=False),
            _canonical_bytes({"purpose": purpose, "payload": document}),
            hashlib.sha256,
        ).hexdigest()
        if seal["purpose"] != purpose or not hmac.compare_digest(
            seal["hmac_sha256"], expected
        ):
            raise SubjectSolContractError("control_receipt_hmac_invalid")

    @staticmethod
    def _external_authority_key(path: Path, label: str) -> bytes:
        try:
            mode = path.stat().st_mode & 0o777
            key = path.read_bytes()
        except OSError as exc:
            raise SubjectSolContractError(f"{label}_key_unavailable") from exc
        if mode & 0o077 or len(key) < 32:
            raise SubjectSolContractError(f"{label}_key_invalid")
        return key

    def _verify_external_seal(
        self,
        value: Mapping[str, Any],
        *,
        purpose: str,
        key_path: Path,
        label: str,
    ) -> None:
        document = dict(value)
        seal = _seal_shape(document.pop("seal", None))
        expected = hmac.new(
            self._external_authority_key(key_path, label),
            _canonical_bytes({"purpose": purpose, "payload": document}),
            hashlib.sha256,
        ).hexdigest()
        if seal["purpose"] != purpose or not hmac.compare_digest(
            seal["hmac_sha256"], expected
        ):
            raise SubjectSolContractError(f"{label}_hmac_invalid")

    def _authority_key_id(self) -> str:
        return hashlib.sha256(self._authority_key(create=True)).hexdigest()

    def _read_content_addressed(
        self, root: Path, digest: str, label: str
    ) -> dict[str, Any]:
        checked = _sha256(digest, f"{label}_sha256")
        path = root / "sha256" / checked[:2] / f"{checked}.json"
        try:
            payload = path.read_bytes()
            value = json.loads(payload.decode("utf-8"))
        except FileNotFoundError as exc:
            raise SubjectSolContractError(f"{label}_missing") from exc
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SubjectSolContractError(f"{label}_unreadable") from exc
        if (
            hashlib.sha256(payload).hexdigest() != checked
            or not isinstance(value, Mapping)
            or _json_file_bytes(value) != payload
        ):
            raise SubjectSolContractError(f"{label}_hash_mismatch")
        return dict(value)

    def read_verified_sol_task_handoff(
        self,
        digest: str,
        *,
        batch: Mapping[str, Any],
        task: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reopen one immutable handoff and bind it to its v2 task."""

        checked_batch = validate_subject_luna_batch_v2(batch)
        checked_task = _validate_batch_task_v2(task)
        if checked_task not in checked_batch["tasks"]:
            raise SubjectSolContractError("sol_handoff_task_not_in_batch")
        envelope = validate_sol_task_handoff_envelope_v1(
            self._read_content_addressed(
                self.sol_task_handoff_root,
                digest,
                "sol_task_handoff_envelope",
            )
        )
        if (
            envelope["batch_id"] != checked_batch["batch_id"]
            or envelope["subject"] != checked_batch["subject"]
            or envelope["execution_status"] != checked_task["status"]
            or any(
                envelope[field] != checked_task[field]
                for field in (
                    "capture_id",
                    "unit_sha256",
                    "input_fingerprint",
                    "study_date",
                    "frozen_payload_sha256",
                    "package_sha256",
                    "terminal_receipt_sha256",
                )
            )
            or digest != checked_task["sol_handoff_envelope_sha256"]
            or envelope["analysis"]["raw_output_sha256"]
            != checked_task["analysis_raw_output_sha256"]
            or envelope["analysis"]["execution_receipt_sha256"]
            != checked_task["analysis_execution_receipt_sha256"]
            or envelope["critical_review"]["raw_output_sha256"]
            != checked_task["critical_review_raw_output_sha256"]
            or envelope["critical_review"]["execution_receipt_sha256"]
            != checked_task["critical_review_execution_receipt_sha256"]
        ):
            raise SubjectSolContractError("sol_handoff_task_binding_mismatch")
        return envelope

    def read_restricted_sol_review_candidate(
        self, report_sha256: str
    ) -> dict[str, Any]:
        """Reopen one content-addressed non-formal report for Sol review.

        This read-only path never creates a Sol batch, authorization, adoption
        token, or writer receipt.  It accepts only a pending review/quarantine
        terminal that explicitly forbids formal writing and reopens every raw
        Provider output by the exact hashes embedded in its stage runtime.
        """

        digest = _sha256(report_sha256, "review_candidate_report_sha256")
        report = self._read_content_addressed(
            self.dispatch_root / "reports" / "json",
            digest,
            "review_candidate_report",
        )
        disposition = report.get("report_disposition")
        expected_sol_review_status = (
            "pending" if disposition == "needs_sol_review" else "not_eligible"
        )
        if (
            report.get("schema_version")
            != "study-intake-review-candidate-terminal-v1"
            or report.get("report_available") is not True
            or report.get("report_disposition")
            not in {"needs_sol_review", "quarantined"}
            or report.get("terminal_status")
            != report.get("report_disposition")
            or report.get("sol_review_status") != expected_sol_review_status
            or report.get("formal_write_eligible") is not False
            or report.get("formal_write_count") != 0
            or not isinstance(report.get("analysis"), Mapping)
            or not isinstance(report.get("review_result"), Mapping)
            or not isinstance(report.get("warnings"), list)
            or not report.get("warnings")
            or report.get("findings") is not None
            and not isinstance(report.get("findings"), list)
        ):
            raise SubjectSolContractError(
                "restricted_sol_review_candidate_invalid"
            )
        package_digest = _sha256(
            report.get("package_sha256"),
            "review_candidate_package_sha256",
        )
        if report.get("package_ref") != (
            "study-intake-dispatch-package://sha256/" + package_digest
        ):
            raise SubjectSolContractError(
                "restricted_sol_review_package_binding_invalid"
            )
        package = self._read_content_addressed(
            self.dispatch_root / "packages",
            package_digest,
            "review_candidate_package",
        )
        quality_axes = {
            "execution_status": (
                "succeeded" if disposition == "needs_sol_review" else "failed"
            ),
            "quality_status": (
                "issues_found"
                if disposition == "needs_sol_review"
                else "unchecked"
            ),
            "production_accepted": False,
        }
        if (
            package.get("schema_version")
            != "study-intake-review-candidate-package-v1"
            or any(
                package.get(field) != report.get(field)
                for field in (
                    "unit_sha256",
                    "subject",
                    "capture_id",
                    "release_id",
                    "analysis",
                    "critical_review",
                    "stage_runtime",
                    "terminal_status",
                    "report_available",
                    "report_disposition",
                    "sol_review_status",
                    "formal_write_eligible",
                    "review_result",
                    "findings",
                    "warnings",
                    "formal_write_count",
                )
            )
            or any(
                report.get(field) is not None
                and report.get(field) != expected
                or package.get(field) is not None
                and package.get(field) != expected
                or report.get(field) != package.get(field)
                for field, expected in quality_axes.items()
            )
        ):
            raise SubjectSolContractError(
                "restricted_sol_review_package_binding_invalid"
            )
        runtime = report.get("stage_runtime")
        if not isinstance(runtime, Mapping):
            raise SubjectSolContractError(
                "restricted_sol_review_runtime_invalid"
            )
        raw_outputs: dict[str, dict[str, Any]] = {}
        mcp_transcripts: dict[str, dict[str, Any]] = {}
        raw_root = self.dispatch_root / "model-stage-raw-outputs"
        transcript_root = (
            self.runtime_root
            / "private"
            / "reports"
            / "mcp-stage-transcripts"
        )
        for stage_name in ("analysis", "critical_review"):
            raw_stage = runtime.get(stage_name)
            if raw_stage is None:
                continue
            if not isinstance(raw_stage, Mapping):
                raise SubjectSolContractError(
                    "restricted_sol_review_runtime_invalid"
                )
            raw_digest = _sha256(
                raw_stage.get("raw_output_object_sha256"),
                f"{stage_name}_raw_output_object_sha256",
            )
            transcript_digest = _sha256(
                raw_stage.get("review_mcp_transcript_sha256"),
                f"{stage_name}_review_mcp_transcript_sha256",
            )
            if raw_stage.get("raw_output_object_ref") != (
                "study-intake-model-stage-raw-output://sha256/"
                + raw_digest
            ) or raw_stage.get("review_mcp_transcript_ref") != (
                "study-intake-mcp-stage-transcript://sha256/"
                + transcript_digest
            ):
                raise SubjectSolContractError(
                    "restricted_sol_review_raw_ref_invalid"
                )
            transcript = self._read_content_addressed(
                transcript_root,
                transcript_digest,
                f"{stage_name}_review_mcp_transcript",
            )
            calls = transcript.get("calls")
            if (
                transcript.get("schema_version")
                != "model-driven-mcp-stage-transcript-v1"
                or transcript.get("stage_name")
                != f"{report.get('subject')}_{stage_name}"
                or transcript.get("subject") != report.get("subject")
                or not isinstance(calls, list)
                or not calls
                or transcript.get("mcp_tool_call_count") != len(calls)
                or int(transcript.get("mcp_tool_call_count") or 0) < 1
                or transcript.get("model_call_count") != 1
                or int(transcript.get("provider_request_count") or 0)
                != len(calls) + 1
                or transcript.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "restricted_sol_review_mcp_transcript_invalid"
                )
            mcp_transcripts[stage_name] = transcript
            matches = [
                path
                for path in raw_root.rglob(f"{raw_digest}.json")
                if path.is_file() and not path.is_symlink()
            ]
            if len(matches) != 1:
                raise SubjectSolContractError(
                    "restricted_sol_review_raw_object_missing"
                )
            try:
                raw_bytes = matches[0].read_bytes()
                raw_value = json.loads(raw_bytes.decode("utf-8"))
                raw_payload = base64.b64decode(
                    str(raw_value.get("raw_output_base64") or ""),
                    validate=True,
                )
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SubjectSolContractError(
                    "restricted_sol_review_raw_object_invalid"
                ) from exc
            except (ValueError, binascii.Error) as exc:
                raise SubjectSolContractError(
                    "restricted_sol_review_raw_object_invalid"
                ) from exc
            if (
                hashlib.sha256(raw_bytes).hexdigest() != raw_digest
                or not isinstance(raw_value, Mapping)
                or raw_value.get("schema_version")
                != "study-intake-model-stage-raw-output-v1"
                or raw_value.get("raw_output_encoding") != "base64"
                or raw_value.get("raw_output_size") != len(raw_payload)
                or raw_value.get("raw_output_sha256")
                != hashlib.sha256(raw_payload).hexdigest()
                or raw_value.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "restricted_sol_review_raw_object_invalid"
                )
            raw_outputs[stage_name] = dict(raw_value)
        if "analysis" not in raw_outputs:
            raise SubjectSolContractError(
                "restricted_sol_review_raw_object_missing"
            )
        return {
            "schema_version": "restricted_sol_review_candidate_view_v1",
            "report_sha256": digest,
            "report_available": True,
            "report_disposition": report["report_disposition"],
            "sol_review_status": expected_sol_review_status,
            "formal_write_eligible": False,
            "report": copy.deepcopy(report),
            "package": copy.deepcopy(package),
            "raw_outputs": raw_outputs,
            "mcp_transcripts": mcp_transcripts,
            "review_result": copy.deepcopy(report["review_result"]),
            "findings": copy.deepcopy(report.get("findings") or []),
            "warnings": copy.deepcopy(report["warnings"]),
            "automatic_adoption": False,
            "automatic_formal_write": False,
            "formal_write_count": 0,
        }

    def read_verified_daily_sol_batch_v3(
        self, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Validate authorization and reopen every per-task handoff."""

        batch = validate_daily_sol_batch_v3(value)
        authorization = batch["authorization_receipt"]
        self._verify_external_seal(
            authorization,
            purpose="user-sol-authorization-receipt-v2",
            key_path=self.user_sol_authority_key_path,
            label="user_sol_authorization",
        )
        luna = batch["subject_luna_batch"]
        task_by_capture = {row["capture_id"]: row for row in luna["tasks"]}
        expected_digests = [
            _sha256(
                task_by_capture[capture_id]["sol_handoff_envelope_sha256"],
                "sol_handoff_envelope_sha256",
            )
            for capture_id in batch["sol_candidate_task_ids"]
        ]
        if sorted(expected_digests) != batch["sol_handoff_envelope_sha256s"]:
            raise SubjectSolContractError("sol_handoff_hash_set_mismatch")
        reopened = [
            self.read_verified_sol_task_handoff(
                digest,
                batch=luna,
                task=task_by_capture[capture_id],
            )
            for capture_id, digest in zip(
                batch["sol_candidate_task_ids"], expected_digests
            )
        ]
        if [row["capture_id"] for row in reopened] != batch["sol_candidate_task_ids"]:
            raise SubjectSolContractError("sol_handoff_candidate_order_mismatch")
        return copy.deepcopy(batch)

    def _read_writer_artifact(
        self,
        kind: str,
        digest: str,
        *,
        purpose: str,
    ) -> dict[str, Any]:
        value = self._read_content_addressed(
            self.writer_artifact_root / kind,
            digest,
            f"isolated_writer_{kind}_artifact",
        )
        self._verify_external_seal(
            value,
            purpose=purpose,
            key_path=self.writer_adapter_authority_key_path,
            label=f"isolated_writer_{kind}_artifact",
        )
        return value

    def _verify_writer_execution_result(
        self,
        digest: str,
        *,
        batch: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Reopen the isolated writer result and every authority artifact."""

        authorized = (
            validate_daily_sol_batch_v3(batch)
            if batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
            else validate_daily_sol_batch_v2(batch)
        )
        result = self._read_content_addressed(
            self.writer_execution_result_root,
            digest,
            "isolated_writer_execution_result",
        )
        self._verify_external_seal(
            result,
            purpose="isolated-writer-execution-result",
            key_path=self.writer_adapter_authority_key_path,
            label="isolated_writer_execution_result",
        )
        required = {
            "schema_version",
            "batch_id",
            "subject",
            "fencing_token",
            "writer",
            "writer_adapter",
            "sol_review_receipt_sha256",
            "writer_process_artifact_sha256",
            "transaction_artifact_sha256",
            "operations_artifact_sha256",
            "recovery_position_artifact_sha256",
            "rollback_artifact_sha256",
            "status",
            "dispatcher_effect",
            "completed_at",
            "seal",
        }
        subject = _subject(result.get("subject"))
        if (
            set(result) != required
            or result.get("schema_version") != "isolated_writer_execution_result_v1"
            or result.get("batch_id") != authorized["batch_id"]
            or subject != authorized["subject"]
            or result.get("writer") != "sol"
            or result.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
        ):
            raise SubjectSolContractError("isolated_writer_execution_result_invalid")
        fence = _integer(result.get("fencing_token"), "fencing_token", minimum=1)
        review_sha = _sha256(
            result.get("sol_review_receipt_sha256"), "sol_review_receipt_sha256"
        )
        completed_at = _timestamp(result.get("completed_at"), "completed_at")
        status = result.get("status")
        if status not in {"committed", "already_current", "failed", "rolled_back"}:
            raise SubjectSolContractError("isolated_writer_execution_status_invalid")
        _dispatcher_effect(result.get("dispatcher_effect"), subject)

        process_sha = _sha256(
            result.get("writer_process_artifact_sha256"),
            "writer_process_artifact_sha256",
        )
        process = self._read_writer_artifact(
            "writer-process",
            process_sha,
            purpose="isolated-writer-process-artifact",
        )
        if (
            set(process)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "adapter_run_id", "pid", "terminal_state", "exit_code",
                "stopped_at", "formal_write_count", "seal",
            }
            or process.get("schema_version") != "isolated_writer_process_artifact_v1"
            or process.get("batch_id") != result["batch_id"]
            or process.get("subject") != subject
            or process.get("fencing_token") != fence
            or process.get("terminal_state") != "stopped"
            or process.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError("isolated_writer_process_artifact_invalid")
        _nonempty(process.get("adapter_run_id"), "adapter_run_id")
        _integer(process.get("pid"), "writer_pid", minimum=1)
        exit_code = _integer(process.get("exit_code"), "writer_exit_code")
        stopped_at = _timestamp(process.get("stopped_at"), "writer_stopped_at")

        transaction_sha = _sha256(
            result.get("transaction_artifact_sha256"),
            "transaction_artifact_sha256",
        )
        transaction = self._read_writer_artifact(
            "transaction-end",
            transaction_sha,
            purpose="isolated-writer-transaction-artifact",
        )
        if (
            set(transaction)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "transaction_id", "state", "ended_at", "formal_write_count",
                "seal",
            }
            or transaction.get("schema_version")
            != "isolated_writer_transaction_artifact_v1"
            or transaction.get("batch_id") != result["batch_id"]
            or transaction.get("subject") != subject
            or transaction.get("fencing_token") != fence
            or transaction.get("state") != status
            or transaction.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError("isolated_writer_transaction_artifact_invalid")
        _nonempty(transaction.get("transaction_id"), "transaction_id")
        ended_at = _timestamp(transaction.get("ended_at"), "transaction_ended_at")

        operations_sha = _sha256(
            result.get("operations_artifact_sha256"),
            "operations_artifact_sha256",
        )
        operations = self._read_writer_artifact(
            "operations",
            operations_sha,
            purpose="isolated-writer-operations-artifact",
        )
        if (
            set(operations)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "pre_state_sha256", "post_state_sha256", "operations",
                "formal_write_count", "seal",
            }
            or operations.get("schema_version")
            != "isolated_writer_operations_artifact_v1"
            or operations.get("batch_id") != result["batch_id"]
            or operations.get("subject") != subject
            or operations.get("fencing_token") != fence
            or not isinstance(operations.get("operations"), list)
        ):
            raise SubjectSolContractError("isolated_writer_operations_artifact_invalid")
        pre_state = _sha256(operations.get("pre_state_sha256"), "pre_state_sha256")
        post_state = _sha256(operations.get("post_state_sha256"), "post_state_sha256")
        formal_count = _integer(
            operations.get("formal_write_count"), "formal_write_count"
        )
        if status != "committed" and formal_count != 0:
            raise SubjectSolContractError("noncommit_formal_write_count_nonzero")
        if status in {"committed", "already_current"} and exit_code != 0:
            raise SubjectSolContractError("successful_writer_exit_code_invalid")

        recovery_sha = _optional_sha256(
            result.get("recovery_position_artifact_sha256"),
            "recovery_position_artifact_sha256",
        )
        rollback_sha = _optional_sha256(
            result.get("rollback_artifact_sha256"), "rollback_artifact_sha256"
        )
        if (status == "failed") is (recovery_sha is None):
            raise SubjectSolContractError("failed_apply_recovery_position_invalid")
        if (status == "rolled_back") is (rollback_sha is None):
            raise SubjectSolContractError("rolled_back_apply_receipt_invalid")
        if status in {"committed", "already_current"} and (
            recovery_sha is not None or rollback_sha is not None
        ):
            raise SubjectSolContractError("successful_apply_recovery_evidence_invalid")

        for kind, artifact_sha, schema_version, purpose in (
            (
                "recovery-position", recovery_sha,
                "isolated_writer_recovery_position_artifact_v1",
                "isolated-writer-recovery-position-artifact",
            ),
            (
                "rollback", rollback_sha,
                "isolated_writer_rollback_artifact_v1",
                "isolated-writer-rollback-artifact",
            ),
        ):
            if artifact_sha is None:
                continue
            artifact = self._read_writer_artifact(kind, artifact_sha, purpose=purpose)
            expected_keys = {
                "schema_version", "batch_id", "subject", "fencing_token",
                "writer_process_artifact_sha256", "transaction_artifact_sha256",
                "state_sha256", "formal_write_count", "seal",
            }
            if (
                set(artifact) != expected_keys
                or artifact.get("schema_version") != schema_version
                or artifact.get("batch_id") != result["batch_id"]
                or artifact.get("subject") != subject
                or artifact.get("fencing_token") != fence
                or artifact.get("writer_process_artifact_sha256") != process_sha
                or artifact.get("transaction_artifact_sha256") != transaction_sha
                or artifact.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    f"isolated_writer_{kind.replace('-', '_')}_artifact_invalid"
                )
            _sha256(artifact.get("state_sha256"), f"{kind}_state_sha256")

        def parsed_timestamp(value: str) -> dt.datetime:
            return dt.datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )

        if not (
            parsed_timestamp(ended_at)
            <= parsed_timestamp(stopped_at)
            <= parsed_timestamp(completed_at)
        ):
            raise SubjectSolContractError("isolated_writer_timeline_invalid")

        core = {
            "batch_id": result["batch_id"],
            "subject": subject,
            "fencing_token": fence,
            "writer": "sol",
            "writer_adapter": result["writer_adapter"],
            "execution_result_sha256": digest,
            "sol_review_receipt_sha256": review_sha,
            "pre_state_sha256": pre_state,
            "post_state_sha256": post_state,
            "operations": copy.deepcopy(operations["operations"]),
            "writer_process": {
                "adapter_run_id": process["adapter_run_id"],
                "pid": process["pid"],
                "terminal_state": "stopped",
                "exit_code": process["exit_code"],
                "stopped_at": process["stopped_at"],
                "evidence_sha256": process_sha,
            },
            "transaction_end": {
                "transaction_id": transaction["transaction_id"],
                "state": transaction["state"],
                "ended_at": transaction["ended_at"],
                "evidence_sha256": transaction_sha,
            },
            "recovery_position_sha256": recovery_sha,
            "rollback_receipt_sha256": rollback_sha,
            "status": status,
            "dispatcher_effect": copy.deepcopy(result["dispatcher_effect"]),
            "formal_write_count": formal_count,
            "completed_at": completed_at,
        }
        if authorized["schema_version"] == DAILY_SOL_BATCH_V3_SCHEMA:
            core.update(
                {
                    "daily_sol_batch_sha256": _document_sha256(authorized),
                    "sol_candidate_task_ids": copy.deepcopy(
                        authorized["sol_candidate_task_ids"]
                    ),
                    "sol_handoff_envelope_sha256s": copy.deepcopy(
                        authorized["sol_handoff_envelope_sha256s"]
                    ),
                }
            )
        return result, core

    def _verify_excluded_task_evidence(
        self, task: Mapping[str, Any], *, original_batch: Mapping[str, Any]
    ) -> None:
        status = task["status"]
        if status == "needs_rework" and task.get("quality_receipt_sha256") is not None:
            digest = _sha256(
                task.get("quality_receipt_sha256"),
                "quality_receipt_sha256",
            )
            quality = validate_subject_quality_receipt_v1(
                self._read_content_addressed(
                    self.receipt_root / "subject-quality",
                    digest,
                    "subject_exclusion_quality_receipt",
                )
            )
            self._verify_seal(quality, purpose="subject-quality-receipt")
            if (
                quality["batch_id"] != original_batch["batch_id"]
                or quality["subject"] != original_batch["subject"]
                or quality["capture_id"] != task["capture_id"]
                or quality["unit_sha256"] != task["unit_sha256"]
                or quality["frozen_payload_sha256"]
                != task["frozen_payload_sha256"]
                or quality["review_outcome"] != "rejected"
                or quality["proposal_sha256"] != task["proposal_sha256"]
                or task["package_sha256"] is not None
            ):
                raise SubjectSolContractError(
                    "subject_exclusion_quality_binding_mismatch"
                )
            return

        digest = _sha256(
            task.get("terminal_receipt_sha256"),
            "terminal_receipt_sha256",
        )
        terminal = self._read_content_addressed(
            self.receipt_root / "subject-terminal",
            digest,
            "subject_exclusion_terminal_receipt",
        )
        required = {
            "schema_version",
            "batch_id",
            "subject",
            "capture_id",
            "unit_sha256",
            "status",
            "error_code",
            "formal_write_count",
            "issued_at",
            "seal",
        }
        allowed_terminal_statuses = {"failed", "evidence_pending", "needs_rework"}
        if (
            set(terminal) != required
            or terminal.get("schema_version")
            != "subject_luna_terminal_receipt_v1"
            or terminal.get("batch_id") != original_batch["batch_id"]
            or terminal.get("subject") != original_batch["subject"]
            or terminal.get("capture_id") != task["capture_id"]
            or terminal.get("unit_sha256") != task["unit_sha256"]
            or terminal.get("status") not in allowed_terminal_statuses
            or terminal.get("status") != status
            or terminal.get("error_code") != task["error_code"]
            or terminal.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "subject_exclusion_terminal_binding_mismatch"
            )
        _timestamp(terminal.get("issued_at"), "terminal_receipt_issued_at")
        self._verify_seal(terminal, purpose="subject-luna-terminal-receipt")

    def _verify_exclusion_receipt_by_digest(
        self, digest: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        receipt = validate_subject_exclusion_receipt_v1(
            self._read_content_addressed(
                self.receipt_root / "subject-exclusion",
                digest,
                "subject_exclusion_receipt",
            )
        )
        self._verify_seal(receipt, purpose="subject-exclusion-receipt")
        if receipt["authority_key_id"] != self._authority_key_id():
            raise SubjectSolContractError("subject_exclusion_key_id_mismatch")

        original_sha = receipt["original_batch"]["batch_sha256"]
        original = validate_subject_luna_batch_v1(
            self._read_content_addressed(
                self.subject_batch_snapshot_root,
                original_sha,
                "subject_exclusion_original_batch",
            )
        )
        metadata = receipt["original_batch"]
        if (
            original["status"] != "frozen"
            or original["all_terminal"] is not True
            or original["exclusion_receipt_sha256s"]
            or original["subject"] != receipt["subject"]
            or original["batch_id"] != metadata["batch_id"]
            or original["study_date"] != metadata["study_date"]
            or original["authority_generation"]
            != metadata["authority_generation"]
            or original["authority_fingerprint"]
            != metadata["authority_fingerprint"]
        ):
            raise SubjectSolContractError(
                "subject_exclusion_original_batch_binding_mismatch"
            )
        target = next(
            (
                row
                for row in original["tasks"]
                if row["capture_id"] == receipt["excluded_task"]["capture_id"]
                and row["unit_sha256"] == receipt["excluded_task"]["unit_sha256"]
            ),
            None,
        )
        if target != receipt["excluded_task"]:
            raise SubjectSolContractError("subject_exclusion_task_binding_mismatch")
        self._verify_excluded_task_evidence(target, original_batch=original)
        return receipt, original

    def _verify_batch_exclusions_locked(self, batch: Mapping[str, Any]) -> None:
        digests = batch["exclusion_receipt_sha256s"]
        if not digests:
            return
        receipts: list[dict[str, Any]] = []
        originals: list[dict[str, Any]] = []
        for digest in digests:
            receipt, original = self._verify_exclusion_receipt_by_digest(digest)
            receipts.append(receipt)
            originals.append(original)
        original = originals[0]
        if any(row != original for row in originals[1:]):
            raise SubjectSolContractError("subject_exclusion_mixed_original_batches")
        if batch["batch_id"] == original["batch_id"]:
            raise SubjectSolContractError("subject_exclusion_batch_not_rebuilt")
        for field in (
            "subject",
            "study_date",
            "capture_high_watermark",
            "authority_generation",
            "authority_fingerprint",
        ):
            if batch[field] != original[field]:
                raise SubjectSolContractError(
                    "subject_exclusion_rebuild_metadata_mismatch"
                )
        excluded = [receipt["excluded_task"] for receipt in receipts]
        identities = [
            (row["capture_id"], row["unit_sha256"]) for row in excluded
        ]
        if len(identities) != len(set(identities)):
            raise SubjectSolContractError("subject_exclusion_duplicate_task")
        expected_survivors = [row for row in original["tasks"] if row not in excluded]
        if batch["tasks"] != expected_survivors:
            raise SubjectSolContractError("subject_exclusion_survivor_set_mismatch")

    def _verify_batch_quality_closures_locked(
        self, batch: Mapping[str, Any]
    ) -> None:
        allowed_batch_ids = {batch["batch_id"]}
        for digest in batch["exclusion_receipt_sha256s"]:
            receipt, _ = self._verify_exclusion_receipt_by_digest(digest)
            allowed_batch_ids.add(receipt["original_batch"]["batch_id"])
        for task in batch["tasks"]:
            if task["status"] != "quality_passed":
                continue
            receipt_sha = _sha256(
                task.get("quality_receipt_sha256"), "quality_receipt_sha256"
            )
            receipt = validate_subject_quality_receipt_v1(
                self._read_content_addressed(
                    self.receipt_root / "subject-quality",
                    receipt_sha,
                    "subject_quality_receipt",
                )
            )
            self._verify_seal(receipt, purpose="subject-quality-receipt")
            expected = {
                "batch_id": batch["batch_id"],
                "subject": batch["subject"],
                "capture_id": task["capture_id"],
                "unit_sha256": task["unit_sha256"],
                "frozen_payload_sha256": task["frozen_payload_sha256"],
                "proposal_sha256": task["proposal_sha256"],
                "package_sha256": task["package_sha256"],
            }
            if (
                receipt.get("batch_id") not in allowed_batch_ids
                or any(
                    receipt.get(key) != value
                    for key, value in expected.items()
                    if key != "batch_id"
                )
                or receipt["authority"]
                != {
                    "generation": batch["authority_generation"],
                    "authority_fingerprint": batch["authority_fingerprint"],
                }
                or receipt["review_outcome"] not in {"accepted", "corrected"}
            ):
                raise SubjectSolContractError("subject_quality_receipt_binding_mismatch")
            package_path = self._quality_subject_package_path(
                subject=batch["subject"],
                study_date=task["study_date"],
                capture_id=task["capture_id"],
                input_fingerprint=task["input_fingerprint"],
                package_sha256=receipt["package_sha256"],
                error_prefix="subject_quality_package",
            )
            try:
                payload = package_path.read_bytes()
                package = json.loads(payload.decode("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SubjectSolContractError("subject_quality_package_missing") from exc
            if (
                hashlib.sha256(payload).hexdigest() != receipt["package_sha256"]
                or not isinstance(package, Mapping)
                or package.get("luna_proposal_sha256")
                != receipt["proposal_sha256"]
                or package.get("capture_freeze_receipt_sha256")
                != receipt["capture_freeze_receipt_sha256"]
                or package.get("mcp_read_session_receipt_sha256")
                != receipt["mcp_read_session_receipt_sha256"]
            ):
                raise SubjectSolContractError("subject_quality_package_binding_mismatch")
            proposal = self._read_immutable_value(
                self.receipt_root / "subject-proposals",
                receipt["proposal_sha256"],
                "subject_quality_proposal",
            )
            if package.get("luna_proposal") != proposal:
                raise SubjectSolContractError("subject_quality_proposal_binding_mismatch")
            self._verify_quality_artifacts(receipt)

    def _publish_immutable(self, root: Path, value: Mapping[str, Any]) -> tuple[str, Path]:
        payload = _json_file_bytes(value)
        digest = hashlib.sha256(payload).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() != payload:
                raise SubjectSolContractError("content_addressed_object_conflict")
            return digest, path
        descriptor, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise SubjectSolContractError("content_addressed_object_conflict")
            os.chmod(path, 0o400)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return digest, path

    def _publish_immutable_value(
        self, root: Path, value: Mapping[str, Any]
    ) -> tuple[str, Path]:
        payload = _canonical_bytes(value)
        digest = hashlib.sha256(payload).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() != payload:
                raise SubjectSolContractError("content_addressed_object_conflict")
            return digest, path
        descriptor, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise SubjectSolContractError("content_addressed_object_conflict")
            os.chmod(path, 0o400)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return digest, path

    def _read_immutable_value(
        self, root: Path, digest: str, label: str
    ) -> dict[str, Any]:
        checked = _sha256(digest, f"{label}_sha256")
        path = root / "sha256" / checked[:2] / f"{checked}.json"
        try:
            payload = path.read_bytes()
            value = json.loads(payload.decode("utf-8"))
        except FileNotFoundError as exc:
            raise SubjectSolContractError(f"{label}_missing") from exc
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SubjectSolContractError(f"{label}_unreadable") from exc
        if (
            hashlib.sha256(payload).hexdigest() != checked
            or not isinstance(value, Mapping)
            or _canonical_bytes(value) != payload
        ):
            raise SubjectSolContractError(f"{label}_hash_mismatch")
        return dict(value)

    @staticmethod
    def _recompute_batch(batch: dict[str, Any]) -> None:
        batch["tasks"] = sorted(
            batch["tasks"], key=lambda row: (row["capture_id"], row["unit_sha256"])
        )
        batch["blocking_task_ids"] = sorted(
            row["capture_id"] for row in batch["tasks"] if row["status"] != "quality_passed"
        )
        batch["all_terminal"] = bool(batch["tasks"]) and all(
            row["status"] in TASK_TERMINAL_STATUSES for row in batch["tasks"]
        )
        batch["sol_ready"] = bool(
            batch["status"] == "frozen"
            and batch["all_terminal"]
            and not batch["blocking_task_ids"]
        )

    @staticmethod
    def _validate_subject_batch(value: Mapping[str, Any]) -> dict[str, Any]:
        """Validate current v2 bytes while preserving the historical v1 reader."""

        if value.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA:
            return validate_subject_luna_batch_v2(value)
        return validate_subject_luna_batch_v1(value)

    @staticmethod
    def _recompute_subject_batch(batch: dict[str, Any]) -> None:
        if batch.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA:
            recomputed = recompute_subject_luna_batch_v2(batch)
            batch.clear()
            batch.update(recomputed)
            return
        SubjectSolRuntimeStore._recompute_batch(batch)

    def _read_batch_locked(self, subject: str) -> dict[str, Any] | None:
        value = self._read_json(self._batch_path(subject), "subject_luna_batch")
        if value is None:
            if self._batch_pointer_path(subject).exists():
                raise SubjectSolContractError("subject_luna_batch_pointer_orphaned")
            return None
        batch = self._validate_subject_batch(value)
        pointer = self._read_json(
            self._batch_pointer_path(subject), "subject_luna_batch_pointer"
        )
        if pointer is None:
            raise SubjectSolContractError("subject_luna_batch_pointer_missing")
        required = {
            "schema_version", "subject", "batch_id", "revision",
            "batch_sha256", "snapshot_sha256", "seal",
        }
        if (
            set(pointer) != required
            or pointer.get("schema_version") != "subject_luna_batch_pointer_v1"
            or pointer.get("subject") != batch["subject"]
            or pointer.get("batch_id") != batch["batch_id"]
            or pointer.get("revision") != batch["revision"]
            or pointer.get("batch_sha256") != _document_sha256(batch)
        ):
            raise SubjectSolContractError("subject_luna_batch_pointer_mismatch")
        self._verify_seal(pointer, purpose="subject-luna-batch-pointer")
        envelope = self._read_content_addressed(
            self.subject_batch_snapshot_root,
            _sha256(pointer.get("snapshot_sha256"), "batch_snapshot_sha256"),
            "subject_luna_batch_snapshot",
        )
        if (
            envelope.get("schema_version") != "subject_luna_batch_snapshot_v1"
            or envelope.get("subject") != batch["subject"]
            or envelope.get("batch_id") != batch["batch_id"]
            or envelope.get("revision") != batch["revision"]
            or envelope.get("batch_sha256") != pointer["batch_sha256"]
            or envelope.get("batch") != batch
        ):
            raise SubjectSolContractError("subject_luna_batch_snapshot_mismatch")
        self._verify_seal(envelope, purpose="subject-luna-batch-snapshot")
        if batch["schema_version"] == SUBJECT_BATCH_SCHEMA:
            self._verify_batch_exclusions_locked(batch)
            self._verify_batch_quality_closures_locked(batch)
        return batch

    def _write_batch_locked(
        self, value: Mapping[str, Any], *, preadvanced: bool = False
    ) -> dict[str, Any]:
        batch = copy.deepcopy(dict(value))
        self._recompute_subject_batch(batch)
        if not preadvanced:
            batch["revision"] = int(batch.get("revision", -1)) + 1
            batch["updated_at"] = _utc_now()
        checked = self._validate_subject_batch(batch)
        batch_sha = _document_sha256(checked)
        snapshot = self._seal(
            {
                "schema_version": "subject_luna_batch_snapshot_v1",
                "subject": checked["subject"],
                "batch_id": checked["batch_id"],
                "revision": checked["revision"],
                "batch_sha256": batch_sha,
                "batch": checked,
            },
            purpose="subject-luna-batch-snapshot",
        )
        snapshot_sha, _ = self._publish_immutable(
            self.subject_batch_snapshot_root, snapshot
        )
        self._atomic_json(self._batch_path(checked["subject"]), checked)
        pointer = self._seal(
            {
                "schema_version": "subject_luna_batch_pointer_v1",
                "subject": checked["subject"],
                "batch_id": checked["batch_id"],
                "revision": checked["revision"],
                "batch_sha256": batch_sha,
                "snapshot_sha256": snapshot_sha,
            },
            purpose="subject-luna-batch-pointer",
        )
        self._atomic_json(self._batch_pointer_path(checked["subject"]), pointer)
        return copy.deepcopy(checked)

    @staticmethod
    def _default_writer(subject: str) -> dict[str, Any]:
        checked = _subject(subject)
        return {
            "schema_version": SUBJECT_WRITER_STATE_SCHEMA,
            "subject": checked,
            "revision": 0,
            "handoff_status": "awaiting_luna",
            "batch_id": None,
            "writer_adapter": SUBJECT_WRITER_ADAPTERS[checked],
            "fencing_token": 0,
            "authorization_receipt_sha256": None,
            "daily_sol_batch_sha256": None,
            "review_receipt_sha256": None,
            "commit_receipt_sha256": None,
            "generation_fence": {
                "blocked": False,
                "source_generation": None,
                "next_generation": None,
            },
            "formal_write_count": 0,
            "updated_at": None,
        }

    def _read_writer_locked(self, subject: str) -> dict[str, Any]:
        value = self._read_json(self._writer_path(subject), "subject_writer_state")
        state = self._default_writer(subject) if value is None else value
        if (
            state.get("schema_version") != SUBJECT_WRITER_STATE_SCHEMA
            or state.get("subject") != _subject(subject)
            or state.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]
            or state.get("handoff_status")
            not in {
                "awaiting_luna",
                "ready_for_authorization",
                "queued_for_sol",
                "sol_reviewing",
                "sol_reviewed",
                "complete",
                "safe_paused",
                "failed",
            }
            or not isinstance(state.get("generation_fence"), Mapping)
        ):
            raise SubjectSolContractError("subject_writer_state_invalid")
        _integer(state.get("revision"), "subject_writer_revision")
        _integer(state.get("fencing_token"), "subject_writer_fencing_token")
        _integer(state.get("formal_write_count"), "subject_writer_formal_write_count")
        return copy.deepcopy(state)

    def _write_writer_locked(self, state: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(state))
        value["revision"] = int(value.get("revision", -1)) + 1
        value["updated_at"] = _utc_now()
        checked = self._read_writer_projection(value)
        self._atomic_json(self._writer_path(checked["subject"]), checked)
        return copy.deepcopy(checked)

    @staticmethod
    def _read_writer_projection(value: Mapping[str, Any]) -> dict[str, Any]:
        subject = _subject(value.get("subject"))
        required = set(SubjectSolRuntimeStore._default_writer(subject))
        state = dict(value)
        if set(state) != required or state.get("schema_version") != SUBJECT_WRITER_STATE_SCHEMA:
            raise SubjectSolContractError("subject_writer_state_invalid")
        if state.get("writer_adapter") != SUBJECT_WRITER_ADAPTERS[subject]:
            raise SubjectSolContractError("subject_writer_state_invalid")
        if state.get("handoff_status") not in {
            "awaiting_luna",
            "ready_for_authorization",
            "queued_for_sol",
            "sol_reviewing",
            "sol_reviewed",
            "complete",
            "safe_paused",
            "failed",
        }:
            raise SubjectSolContractError("subject_writer_state_invalid")
        _integer(state.get("revision"), "subject_writer_revision")
        _integer(state.get("fencing_token"), "subject_writer_fencing_token")
        _integer(state.get("formal_write_count"), "subject_writer_formal_write_count")
        fence = dict(_mapping(state.get("generation_fence"), "generation_fence"))
        if set(fence) != {"blocked", "source_generation", "next_generation"} or not isinstance(
            fence.get("blocked"), bool
        ):
            raise SubjectSolContractError("generation_fence_invalid")
        _optional_nonempty(fence.get("source_generation"), "source_generation")
        _optional_nonempty(fence.get("next_generation"), "next_generation")
        if state.get("updated_at") is not None:
            _timestamp(state["updated_at"], "subject_writer_updated_at")
        return state

    @staticmethod
    def _default_global() -> dict[str, Any]:
        return {
            "schema_version": GLOBAL_SOL_WRITER_SCHEMA,
            "revision": 0,
            "next_fencing_token": 1,
            "queue": [],
            "active_writer": None,
            "active_writer_count": 0,
            "formal_write_count": 0,
            "updated_at": None,
        }

    @staticmethod
    def _validate_global(value: Mapping[str, Any]) -> dict[str, Any]:
        state = dict(value)
        if set(state) != set(SubjectSolRuntimeStore._default_global()) or state.get(
            "schema_version"
        ) != GLOBAL_SOL_WRITER_SCHEMA:
            raise SubjectSolContractError("global_sol_writer_state_invalid")
        _integer(state.get("revision"), "global_revision")
        _integer(state.get("next_fencing_token"), "next_fencing_token", minimum=1)
        _integer(state.get("formal_write_count"), "global_formal_write_count")
        queue = _sequence(state.get("queue"), "global_sol_queue")
        seen: set[str] = set()
        for raw in queue:
            row = dict(_mapping(raw, "global_sol_queue_entry"))
            if set(row) != {
                "batch_id",
                "subject",
                "authorized_at",
                "authorization_receipt_sha256",
                "daily_sol_batch_sha256",
                "status",
            }:
                raise SubjectSolContractError("global_sol_queue_entry_invalid")
            batch_id = _nonempty(row.get("batch_id"), "queue_batch_id")
            if batch_id in seen or row.get("status") not in QUEUE_STATUSES:
                raise SubjectSolContractError("global_sol_queue_entry_invalid")
            seen.add(batch_id)
            _subject(row.get("subject"))
            _timestamp(row.get("authorized_at"), "queue_authorized_at")
            _sha256(row.get("authorization_receipt_sha256"), "authorization_receipt_sha256")
            _sha256(row.get("daily_sol_batch_sha256"), "daily_sol_batch_sha256")
        queued = [row for row in queue if row["status"] == "queued"]
        expected_queued = sorted(
            queued,
            key=lambda row: (
                row["authorized_at"],
                row["authorization_receipt_sha256"],
                row["batch_id"],
            ),
        )
        if queued != expected_queued:
            raise SubjectSolContractError("global_sol_fifo_order_invalid")
        active = state.get("active_writer")
        if active is not None:
            active = dict(_mapping(active, "active_writer"))
            if set(active) != {
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
                raise SubjectSolContractError("active_writer_invalid")
            _nonempty(active.get("batch_id"), "active_batch_id")
            _subject(active.get("subject"))
            _integer(active.get("fencing_token"), "fencing_token", minimum=1)
            _nonempty(active.get("owner_id"), "writer_owner_id")
            _timestamp(active.get("claimed_at"), "writer_claimed_at")
            _sha256(active.get("authorization_receipt_sha256"), "authorization_receipt_sha256")
            _sha256(active.get("daily_sol_batch_sha256"), "daily_sol_batch_sha256")
            _optional_sha256(active.get("review_receipt_sha256"), "review_receipt_sha256")
            if active.get("status") not in {
                "reviewing",
                "applying",
                "recovering",
                "safe_paused",
                "failed",
            }:
                raise SubjectSolContractError("active_writer_invalid")
            _optional_nonempty(active.get("current_item"), "active_writer_current_item")
            _integer(active.get("committed_count"), "active_writer_committed_count")
            _integer(active.get("remaining_count"), "active_writer_remaining_count")
        expected_count = 1 if active is not None else 0
        if state.get("active_writer_count") != expected_count:
            raise SubjectSolContractError("active_writer_count_invalid")
        if expected_count:
            allowed_queue_states = (
                {"safe_paused"}
                if active["status"] == "safe_paused"
                else {"active", "reviewed"}
            )
            if not any(
                row["batch_id"] == active["batch_id"]
                and row["status"] in allowed_queue_states
                for row in queue
            ):
                raise SubjectSolContractError("active_writer_queue_binding_invalid")
        if state.get("updated_at") is not None:
            _timestamp(state["updated_at"], "global_updated_at")
        return state

    def _read_global_locked(self) -> dict[str, Any]:
        value = self._read_json(self.global_state_path, "global_sol_writer_state")
        return self._validate_global(self._default_global() if value is None else value)

    def _write_global_locked(self, state: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(state))
        value["revision"] = int(value.get("revision", -1)) + 1
        value["active_writer_count"] = 1 if value.get("active_writer") is not None else 0
        value["updated_at"] = _utc_now()
        checked = self._validate_global(value)
        self._atomic_json(self.global_state_path, checked)
        return copy.deepcopy(checked)

    @staticmethod
    def _default_english_legacy_state(
        batch: Mapping[str, Any], *, batch_sha256: str,
    ) -> dict[str, Any]:
        checked = validate_english_legacy_recuration_sol_batch_v1(batch)
        return {
            "schema_version": ENGLISH_LEGACY_SOL_STATE_SCHEMA,
            "batch_id": checked["batch_id"],
            "batch_sha256": _sha256(batch_sha256, "batch_sha256"),
            "status": "queued",
            "attempt": 0,
            "fencing_token": 0,
            "current_ordinal": 1,
            "current_target_id": checked["work_items"][0]["target_id"],
            "current_review_receipt_sha256": None,
            "current_checkpoint_sha256": None,
            "items": [
                {
                    "ordinal": row["ordinal"],
                    "target_id": row["target_id"],
                    "status": "pending",
                    "attempts": [],
                    "checkpoint_sha256": None,
                    "formal_write_count": 0,
                }
                for row in checked["work_items"]
            ],
            "formal_write_count": 0,
            "execution_closure_sha256": None,
            "revision": 0,
            "updated_at": None,
        }

    @staticmethod
    def _validate_english_legacy_state(
        value: Mapping[str, Any], *, batch: Mapping[str, Any], batch_sha256: str,
    ) -> dict[str, Any]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        state = copy.deepcopy(dict(value))
        required = set(
            SubjectSolRuntimeStore._default_english_legacy_state(
                checked_batch, batch_sha256=batch_sha256
            )
        )
        if (
            set(state) != required
            or state.get("schema_version") != ENGLISH_LEGACY_SOL_STATE_SCHEMA
            or state.get("batch_id") != checked_batch["batch_id"]
            or state.get("batch_sha256") != batch_sha256
            or state.get("status")
            not in {"queued", "active", "recovering", "safe_paused", "complete"}
        ):
            raise SubjectSolContractError("english_legacy_sol_state_invalid")
        attempt = _integer(state.get("attempt"), "attempt")
        fence = _integer(state.get("fencing_token"), "fencing_token")
        current_ordinal = state.get("current_ordinal")
        current_target = state.get("current_target_id")
        if state["status"] == "complete":
            if current_ordinal is not None or current_target is not None:
                raise SubjectSolContractError("english_legacy_completed_state_has_current_item")
        else:
            ordinal = _integer(current_ordinal, "current_ordinal", minimum=1)
            item = _english_legacy_item(checked_batch, ordinal)
            if current_target != item["target_id"]:
                raise SubjectSolContractError("english_legacy_state_current_item_mismatch")
        _optional_sha256(
            state.get("current_review_receipt_sha256"),
            "current_review_receipt_sha256",
        )
        _optional_sha256(
            state.get("current_checkpoint_sha256"),
            "current_checkpoint_sha256",
        )
        rows = _sequence(state.get("items"), "state_items", nonempty=True)
        if len(rows) != checked_batch["target_count"]:
            raise SubjectSolContractError("english_legacy_state_item_count_invalid")
        total_writes = 0
        for ordinal, raw in enumerate(rows, start=1):
            row = dict(_mapping(raw, "english_legacy_state_item"))
            if set(row) != {
                "ordinal", "target_id", "status", "attempts",
                "checkpoint_sha256", "formal_write_count",
            }:
                raise SubjectSolContractError("english_legacy_state_item_shape_invalid")
            batch_item = _english_legacy_item(checked_batch, ordinal)
            if (
                row.get("ordinal") != ordinal
                or row.get("target_id") != batch_item["target_id"]
                or row.get("status")
                not in {"pending", "reviewed", "committed", "already_current", "failed"}
            ):
                raise SubjectSolContractError("english_legacy_state_item_invalid")
            item_writes = _integer(
                row.get("formal_write_count"), "item_formal_write_count"
            )
            total_writes += item_writes
            _optional_sha256(row.get("checkpoint_sha256"), "checkpoint_sha256")
            attempts = _sequence(row.get("attempts"), "item_attempts")
            for expected_attempt, raw_attempt in enumerate(attempts, start=1):
                attempt_row = dict(_mapping(raw_attempt, "item_attempt"))
                if set(attempt_row) != {
                    "attempt", "fencing_token", "review_receipt_sha256",
                    "apply_receipt_sha256", "failure_receipt_sha256",
                    "recovery_receipt_sha256",
                }:
                    raise SubjectSolContractError("english_legacy_attempt_shape_invalid")
                if attempt_row.get("attempt") != expected_attempt:
                    raise SubjectSolContractError("english_legacy_attempt_sequence_invalid")
                _integer(attempt_row.get("fencing_token"), "attempt_fencing_token", minimum=1)
                for field in (
                    "review_receipt_sha256", "apply_receipt_sha256",
                    "failure_receipt_sha256", "recovery_receipt_sha256",
                ):
                    _optional_sha256(attempt_row.get(field), field)
        if state.get("formal_write_count") != total_writes:
            raise SubjectSolContractError("english_legacy_state_write_count_invalid")
        _optional_sha256(
            state.get("execution_closure_sha256"), "execution_closure_sha256"
        )
        _integer(state.get("revision"), "revision")
        if state.get("updated_at") is not None:
            _timestamp(state["updated_at"], "updated_at")
        if state["status"] in {"active", "recovering"} and (attempt < 1 or fence < 1):
            raise SubjectSolContractError("english_legacy_active_state_fence_invalid")
        return state

    def _english_legacy_batch_by_digest(self, digest: str) -> dict[str, Any]:
        checked = _sha256(digest, "english_legacy_sol_batch_sha256")
        value = self._read_content_addressed(
            self.english_legacy_sol_batch_root,
            checked,
            "english_legacy_recuration_sol_batch",
        )
        return validate_english_legacy_recuration_sol_batch_v1(value)

    def _read_english_legacy_state_locked(
        self, batch: Mapping[str, Any], *, batch_sha256: str,
    ) -> dict[str, Any]:
        path = self._english_legacy_state_path(str(batch["batch_id"]))
        value = self._read_json(path, "english_legacy_sol_state")
        if value is None:
            value = self._default_english_legacy_state(
                batch, batch_sha256=batch_sha256
            )
        return self._validate_english_legacy_state(
            value, batch=batch, batch_sha256=batch_sha256
        )

    def _write_english_legacy_state_locked(
        self, state: Mapping[str, Any], *, batch: Mapping[str, Any], batch_sha256: str,
    ) -> dict[str, Any]:
        value = copy.deepcopy(dict(state))
        value["revision"] = int(value.get("revision", -1)) + 1
        value["updated_at"] = _utc_now()
        checked = self._validate_english_legacy_state(
            value, batch=batch, batch_sha256=batch_sha256
        )
        self._atomic_json(self._english_legacy_state_path(checked["batch_id"]), checked)
        return copy.deepcopy(checked)

    def read_english_legacy_recuration_state(self, batch_id: str) -> dict[str, Any]:
        checked_id = _nonempty(batch_id, "batch_id")
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            entry = next(
                (row for row in global_state["queue"] if row["batch_id"] == checked_id),
                None,
            )
            if entry is None or entry["subject"] != "english":
                raise SubjectSolContractError("english_legacy_sol_batch_not_found")
            batch_sha = str(entry["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            state = self._read_english_legacy_state_locked(
                batch, batch_sha256=batch_sha
            )
        return {"global": global_state, "batch": batch, "state": state}

    def reopen_verified_english_legacy_execution_closure(
        self, batch_id: str, *, closure_sha256: str | None = None
    ) -> dict[str, Any]:
        """Reopen the complete checkpoint chain for the EN-P0-006 gate."""

        snapshot = self.read_english_legacy_recuration_state(batch_id)
        batch = snapshot["batch"]
        state = snapshot["state"]
        digest = closure_sha256 or state.get("execution_closure_sha256")
        checked_digest = _sha256(digest, "execution_closure_sha256")
        if state.get("execution_closure_sha256") != checked_digest:
            raise SubjectSolContractError(
                "english_legacy_execution_closure_pointer_mismatch"
            )
        closure = self._read_content_addressed(
            self.english_legacy_execution_closure_root,
            checked_digest,
            "english_legacy_recuration_execution_closure",
        )
        self._verify_seal(
            closure, purpose="english-legacy-recuration-execution-closure-v1"
        )
        closure = validate_english_legacy_recuration_execution_closure_v1(
            closure, batch=batch
        )
        checkpoint_sha: str | None = closure["final_checkpoint_sha256"]
        seen_checkpoints: set[str] = set()
        seen_results: set[str] = set()
        checkpoints_by_result: dict[str, tuple[str, dict[str, Any]]] = {}
        child_pre_authority: str | None = None
        while checkpoint_sha is not None:
            if checkpoint_sha in seen_checkpoints:
                raise SubjectSolContractError(
                    "english_legacy_checkpoint_cycle_detected"
                )
            checkpoint = self._read_english_legacy_checkpoint(
                checkpoint_sha, batch=batch
            )
            if (
                child_pre_authority is not None
                and child_pre_authority != checkpoint["post_authority_sha256"]
            ):
                raise SubjectSolContractError(
                    "english_legacy_checkpoint_authority_chain_broken"
                )
            seen_checkpoints.add(checkpoint_sha)
            result_sha = checkpoint["item_result_receipt_sha256"]
            if result_sha in seen_results:
                raise SubjectSolContractError(
                    "english_legacy_checkpoint_result_replayed"
                )
            seen_results.add(result_sha)
            if checkpoint["outcome"] == "failed":
                result = validate_english_legacy_sol_item_failure_receipt_v1(
                    self._read_english_legacy_writer_receipt(
                        self.english_legacy_item_failure_root,
                        result_sha,
                        label="english_legacy_item_failure_receipt",
                        purpose="english-legacy-sol-item-failure-receipt-v1",
                    ),
                    batch=batch,
                )
            else:
                result = validate_english_legacy_sol_item_apply_receipt_v1(
                    self._read_english_legacy_writer_receipt(
                        self.english_legacy_item_apply_root,
                        result_sha,
                        label="english_legacy_item_apply_receipt",
                        purpose="english-legacy-sol-item-apply-receipt-v1",
                    ),
                    batch=batch,
                )
            if (
                result["ordinal"] != checkpoint["ordinal"]
                or result["target_id"] != checkpoint["target_id"]
                or result["fencing_token"] != checkpoint["fencing_token"]
                or result["attempt"] != checkpoint["attempt"]
                or result["previous_checkpoint_sha256"]
                != checkpoint["previous_checkpoint_sha256"]
            ):
                raise SubjectSolContractError(
                    "english_legacy_checkpoint_result_binding_mismatch"
                )
            checkpoints_by_result[result_sha] = (checkpoint_sha, checkpoint)
            child_pre_authority = checkpoint["pre_authority_sha256"]
            checkpoint_sha = checkpoint["previous_checkpoint_sha256"]
        if child_pre_authority != batch["authority_fingerprint"]:
            raise SubjectSolContractError(
                "english_legacy_checkpoint_genesis_mismatch"
            )
        expected_results: set[str] = set()
        expected_reviews: set[str] = set()
        expected_recoveries: set[str] = set()
        expected_final_checkpoints: set[str] = set()
        for outcome, row in zip(closure["item_outcomes"], state["items"]):
            if (
                outcome["ordinal"] != row["ordinal"]
                or outcome["target_id"] != row["target_id"]
                or outcome["status"] != row["status"]
                or outcome["attempts"] != row["attempts"]
                or outcome["checkpoint_sha256"] != row["checkpoint_sha256"]
                or outcome["formal_write_count"] != row["formal_write_count"]
            ):
                raise SubjectSolContractError(
                    "english_legacy_execution_closure_state_mismatch"
                )
            expected_final_checkpoints.add(outcome["checkpoint_sha256"])
            for attempt_index, attempt in enumerate(
                outcome["attempts"], start=1
            ):
                result_sha = (
                    attempt["apply_receipt_sha256"]
                    or attempt["failure_receipt_sha256"]
                )
                if result_sha is None or result_sha not in checkpoints_by_result:
                    raise SubjectSolContractError(
                        "english_legacy_execution_closure_result_missing"
                    )
                expected_results.add(result_sha)
                checkpoint_digest, checkpoint = checkpoints_by_result[result_sha]
                review_sha = attempt["review_receipt_sha256"]
                if review_sha is not None:
                    review = validate_english_legacy_sol_item_review_receipt_v1(
                        self._read_english_legacy_writer_receipt(
                            self.english_legacy_item_review_root,
                            review_sha,
                            label="english_legacy_item_review_receipt",
                            purpose=(
                                "english-legacy-sol-item-review-receipt-v1"
                            ),
                        ),
                        batch=batch,
                    )
                    if (
                        review["ordinal"] != outcome["ordinal"]
                        or review["target_id"] != outcome["target_id"]
                        or review["attempt"] != attempt_index
                        or review["fencing_token"] != attempt["fencing_token"]
                        or review["previous_checkpoint_sha256"]
                        != checkpoint["previous_checkpoint_sha256"]
                        or review["authority_checkpoint_sha256"]
                        != checkpoint["pre_authority_sha256"]
                    ):
                        raise SubjectSolContractError(
                            "english_legacy_execution_review_binding_mismatch"
                        )
                    expected_reviews.add(review_sha)
                result = (
                    validate_english_legacy_sol_item_apply_receipt_v1(
                        self._read_english_legacy_writer_receipt(
                            self.english_legacy_item_apply_root,
                            result_sha,
                            label="english_legacy_item_apply_receipt",
                            purpose=(
                                "english-legacy-sol-item-apply-receipt-v1"
                            ),
                        ),
                        batch=batch,
                    )
                    if attempt["apply_receipt_sha256"] is not None
                    else validate_english_legacy_sol_item_failure_receipt_v1(
                        self._read_english_legacy_writer_receipt(
                            self.english_legacy_item_failure_root,
                            result_sha,
                            label="english_legacy_item_failure_receipt",
                            purpose=(
                                "english-legacy-sol-item-failure-receipt-v1"
                            ),
                        ),
                        batch=batch,
                    )
                )
                self._verify_english_legacy_execution_evidence(
                    result,
                    failure=attempt["failure_receipt_sha256"] is not None,
                )
                if (
                    result["review_receipt_sha256"] != review_sha
                    or (
                        attempt["apply_receipt_sha256"] is not None
                        and (
                            result["pre_authority_sha256"]
                            != checkpoint["pre_authority_sha256"]
                            or result["post_authority_sha256"]
                            != checkpoint["post_authority_sha256"]
                        )
                    )
                    or (
                        attempt["failure_receipt_sha256"] is not None
                        and result["authority_checkpoint_sha256"]
                        != checkpoint["post_authority_sha256"]
                    )
                ):
                    raise SubjectSolContractError(
                        "english_legacy_execution_result_authority_mismatch"
                    )
                recovery_sha = attempt["recovery_receipt_sha256"]
                if recovery_sha is not None:
                    previous_attempt = outcome["attempts"][attempt_index - 2]
                    failure_sha = previous_attempt["failure_receipt_sha256"]
                    if failure_sha is None or failure_sha not in checkpoints_by_result:
                        raise SubjectSolContractError(
                            "english_legacy_execution_recovery_failure_missing"
                        )
                    failure_checkpoint_sha, failure_checkpoint = (
                        checkpoints_by_result[failure_sha]
                    )
                    recovery = validate_english_legacy_sol_item_recovery_receipt_v1(
                        self._read_english_legacy_writer_receipt(
                            self.english_legacy_item_recovery_root,
                            recovery_sha,
                            label="english_legacy_item_recovery_receipt",
                            purpose=(
                                "english-legacy-sol-item-recovery-receipt-v1"
                            ),
                        ),
                        batch=batch,
                    )
                    if (
                        recovery["ordinal"] != outcome["ordinal"]
                        or recovery["target_id"] != outcome["target_id"]
                        or recovery["attempt"] != attempt_index
                        or recovery["prior_fencing_token"]
                        != previous_attempt["fencing_token"]
                        or recovery["new_fencing_token"]
                        != attempt["fencing_token"]
                        or recovery["failure_receipt_sha256"] != failure_sha
                        or recovery["previous_checkpoint_sha256"]
                        != failure_checkpoint_sha
                        or recovery["authority_checkpoint_sha256"]
                        != failure_checkpoint["post_authority_sha256"]
                    ):
                        raise SubjectSolContractError(
                            "english_legacy_execution_recovery_binding_mismatch"
                        )
                    expected_recoveries.add(recovery_sha)
        if (
            expected_results != seen_results
            or not expected_final_checkpoints.issubset(seen_checkpoints)
        ):
            raise SubjectSolContractError(
                "english_legacy_execution_closure_evidence_incomplete"
            )
        return {
            "schema_version": "verified_english_legacy_execution_closure_v1",
            "batch_id": batch["batch_id"],
            "batch_sha256": _document_sha256(batch),
            "closure_sha256": checked_digest,
            "closure": closure,
            "checkpoint_count": len(seen_checkpoints),
            "item_result_receipt_count": len(seen_results),
            "review_receipt_count": len(expected_reviews),
            "recovery_receipt_count": len(expected_recoveries),
            "status": closure["status"],
            "formal_write_count": closure["formal_write_count"],
        }

    def _verify_english_legacy_item_authority(
        self, item: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> None:
        event_sha = str(item["authorization_event_sha256"])
        event = self._read_content_addressed(
            self.english_legacy_target_authorization_event_root,
            event_sha,
            "english_legacy_target_authorization_event",
        )
        self._verify_english_legacy_authority_seal(
            event, purpose="english-legacy-target-authorization-event-v3"
        )
        if (
            event.get("schema_version") != "english_legacy_target_authorization_event_v3"
            or event.get("parent_batch_authorization_sha256")
            != batch["batch_authorization_sha256"]
            or event.get("inventory_sha256") != batch["inventory_sha256"]
            or event.get("target_set_sha256") != batch["target_set_sha256"]
            or event.get("ordinal") != item["ordinal"]
            or event.get("target_id") != item["target_id"]
            or event.get("target_kind") != item["target_kind"]
            or event.get("current_object_sha256") != item["current_object_sha256"]
            or event.get("disposition") != "deterministic_recuration"
            or event.get("authorized_operations")
            != ["luna_recuration", "sol_review", "sol_apply"]
        ):
            raise SubjectSolContractError("english_legacy_target_authorization_invalid")
        receipt_sha = str(item["target_authorization_receipt_sha256"])
        receipt = self._read_content_addressed(
            self.english_legacy_disposition_receipt_root,
            receipt_sha,
            "english_legacy_disposition_receipt",
        )
        self._verify_english_legacy_authority_seal(
            receipt, purpose="english-legacy-disposition-receipt-v3"
        )
        receipt_target = receipt.get("target")
        if not isinstance(receipt_target, Mapping):
            receipt_target = receipt
        if (
            receipt.get("schema_version") != "english_legacy_disposition_receipt_v3"
            or receipt.get("batch_authorization_sha256")
            != batch["batch_authorization_sha256"]
            or receipt.get("inventory_sha256") != batch["inventory_sha256"]
            or receipt.get("authorization_event_sha256") != event_sha
            or receipt_target.get("target_id") != item["target_id"]
            or receipt_target.get("target_kind") != item["target_kind"]
            or receipt_target.get("ordinal") != item["ordinal"]
        ):
            raise SubjectSolContractError("english_legacy_disposition_receipt_invalid")

    def _verify_english_legacy_authority_seal(
        self, value: Mapping[str, Any], *, purpose: str
    ) -> None:
        document = dict(value)
        seal = _seal_shape(document.pop("seal", None))
        if seal["purpose"] != purpose:
            raise SubjectSolContractError("english_legacy_authority_seal_invalid")
        try:
            path = self.english_legacy_batch_authority_key_path
            if path.is_symlink() or not path.is_file():
                raise OSError("unsafe key")
            mode = path.stat().st_mode & 0o777
            key = path.read_bytes()
        except OSError as exc:
            raise SubjectSolContractError(
                "english_legacy_authority_key_unavailable"
            ) from exc
        if mode & 0o077 or len(key) < 32:
            raise SubjectSolContractError("english_legacy_authority_key_invalid")
        expected = hmac.new(key, _canonical_bytes(document), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(seal["hmac_sha256"], expected):
            raise SubjectSolContractError("english_legacy_authority_hmac_invalid")
        if document.get("authority_key_id") != hashlib.sha256(key).hexdigest():
            raise SubjectSolContractError("english_legacy_authority_key_id_mismatch")

    def _verify_english_legacy_luna_quality(
        self,
        batch: Mapping[str, Any],
        *,
        config: Mapping[str, Any] | None,
        quality_verifier: Callable[
            [Mapping[str, Any], Path, str], Mapping[str, Any]
        ] | None,
    ) -> list[dict[str, Any]]:
        checked = validate_english_legacy_recuration_sol_batch_v1(batch)
        if quality_verifier is None:
            if config is None:
                raise SubjectSolContractError(
                    "english_legacy_quality_verifier_required"
                )
            try:
                from english_legacy_recuration import verify_quality_receipt
            except ImportError as exc:
                raise SubjectSolContractError(
                    "english_legacy_quality_verifier_unavailable"
                ) from exc
            quality_verifier = verify_quality_receipt
        verified_rows = []
        verifier_config = {} if config is None else config
        for item in checked["work_items"]:
            try:
                verified = dict(
                    quality_verifier(
                        verifier_config,
                        self.runtime_root,
                        item["quality_receipt_sha256"],
                    )
                )
            except Exception as exc:
                code = getattr(exc, "code", "english_legacy_quality_reopen_failed")
                raise SubjectSolContractError(str(code)) from exc
            if (
                verified.get("schema_version")
                != "verified_english_legacy_recuration_quality_v1"
                or verified.get("receipt_sha256") != item["quality_receipt_sha256"]
                or verified.get("package_sha256") != item["package_sha256"]
                or verified.get("work_item_sha256") != item["work_item_sha256"]
                or verified.get("target_id") != item["target_id"]
                or verified.get("target_kind") != item["target_kind"]
                or verified.get("ordinal") != item["ordinal"]
                or verified.get("inventory_sha256") != item["inventory_sha256"]
                or verified.get("batch_authorization_sha256")
                != item["parent_batch_authorization_sha256"]
                or verified.get("target_authorization_receipt_sha256")
                != item["target_authorization_receipt_sha256"]
                or verified.get("authority")
                != {
                    "generation": item["authority_generation"],
                    "authority_fingerprint": item["authority_fingerprint"],
                }
                or verified.get("proposal_action") != item["proposed_action"]
                or verified.get("proposal_sha256") != item["proposal_sha256"]
                or verified.get("quality_outcome") != item["quality_outcome"]
                or verified.get("model_call_count") != 2
                or verified.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "english_legacy_quality_receipt_binding_mismatch"
                )
            verified_rows.append(verified)
        return verified_rows

    def build_english_legacy_recuration_sol_batch(
        self,
        *,
        batch_id: str,
        batch_authorization_sha256: str,
        authorization_expansion_closure_sha256: str,
        quality_receipt_sha256s: Sequence[str],
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Build the exact parent batch from reopened Luna quality receipts."""

        authorization_sha = _sha256(
            batch_authorization_sha256, "batch_authorization_sha256"
        )
        authorization = validate_english_legacy_batch_authorization_v1(
            self._read_content_addressed(
                self.english_legacy_batch_authorization_root,
                authorization_sha,
                "english_legacy_batch_authorization",
            )
        )
        self._verify_english_legacy_authority_seal(
            authorization, purpose="english-legacy-batch-authorization-v1"
        )
        closure_sha = _sha256(
            authorization_expansion_closure_sha256,
            "authorization_expansion_closure_sha256",
        )
        try:
            from english_legacy_disposition import EnglishLegacyDispositionV3Store
        except ImportError as exc:
            raise SubjectSolContractError(
                "english_legacy_authorization_verifier_unavailable"
            ) from exc
        legacy_store = EnglishLegacyDispositionV3Store(
            self.english_legacy_root,
            self.english_legacy_batch_authority_key_path,
        )
        try:
            authorization = validate_english_legacy_batch_authorization_v1(
                legacy_store.verify_batch_authorization(
                    authorization_sha, require_live_inventory=False
                )
            )
            closure = dict(
                legacy_store.reopen_verified_authorization_expansion_closure(
                    closure_sha, require_live_inventory=False
                )
            )
        except Exception as exc:
            code = getattr(
                exc, "code", "english_legacy_authorization_verification_failed"
            )
            raise SubjectSolContractError(str(code)) from exc
        if (
            closure.get("batch_authorization_sha256") != authorization_sha
            or closure.get("inventory_sha256") != authorization["inventory_sha256"]
            or closure.get("target_set_sha256") != authorization["target_set_sha256"]
            or closure.get("target_count") != authorization["target_count"]
        ):
            raise SubjectSolContractError(
                "english_legacy_authorization_expansion_incomplete"
            )
        raw_receipt_shas = _sequence(
            quality_receipt_sha256s,
            "quality_receipt_sha256s",
            nonempty=True,
        )
        receipt_shas = [
            _sha256(row, "quality_receipt_sha256")
            for row in raw_receipt_shas
        ]
        if len(receipt_shas) != len(set(receipt_shas)):
            raise SubjectSolContractError(
                "english_legacy_quality_receipt_set_mismatch"
            )
        if len(receipt_shas) != authorization["target_count"]:
            raise SubjectSolContractError("english_legacy_quality_receipt_set_mismatch")
        try:
            from english_legacy_recuration import (
                validate_work_item,
                verify_quality_receipt,
            )
        except ImportError as exc:
            raise SubjectSolContractError(
                "english_legacy_quality_verifier_unavailable"
            ) from exc
        verified = []
        for receipt_sha in receipt_shas:
            try:
                verified.append(
                    dict(
                        verify_quality_receipt(
                            config, self.runtime_root, receipt_sha
                        )
                    )
                )
            except Exception as exc:
                code = getattr(
                    exc, "code", "english_legacy_quality_reopen_failed"
                )
                raise SubjectSolContractError(str(code)) from exc
        verified.sort(key=lambda row: int(row.get("ordinal", 0)))
        mappings = _sequence(
            closure.get("target_authorizations"),
            "target_authorizations",
            nonempty=True,
        )
        if len(verified) != len(mappings):
            raise SubjectSolContractError("english_legacy_quality_receipt_set_mismatch")
        items = []
        for expected_ordinal, (quality, raw_mapping) in enumerate(
            zip(verified, mappings), start=1
        ):
            mapping = dict(_mapping(raw_mapping, "target_authorization_mapping"))
            if quality.get("ordinal") != expected_ordinal:
                raise SubjectSolContractError("english_legacy_quality_receipt_order_invalid")
            work_sha = _sha256(quality.get("work_item_sha256"), "work_item_sha256")
            work_path = (
                self.dispatch_root / "english-legacy-recuration" / "work-items"
                / "sha256" / work_sha[:2] / f"{work_sha}.json"
            )
            value = self._read_json(work_path, "english_legacy_recuration_work_item")
            if (
                value is None
                or hashlib.sha256(work_path.read_bytes()).hexdigest() != work_sha
            ):
                raise SubjectSolContractError("english_legacy_work_item_missing")
            work_item = validate_work_item(value)
            if (
                mapping.get("ordinal") != expected_ordinal
                or mapping.get("target_id") != quality.get("target_id")
                or work_item["target_id"] != quality.get("target_id")
                or work_item["target_kind"] != quality.get("target_kind")
                or quality.get("quality_outcome") not in {"accepted", "corrected"}
                or quality.get("proposal_action")
                not in ENGLISH_LEGACY_PROPOSAL_ACTIONS
            ):
                raise SubjectSolContractError("english_legacy_quality_not_sol_ready")
            items.append(
                build_english_legacy_sol_work_item_v1(
                    batch_authorization_sha256=authorization_sha,
                    authorization=authorization,
                    authorization_mapping=mapping,
                    work_item=work_item,
                    quality_verification=quality,
                )
            )
        return validate_english_legacy_recuration_sol_batch_v1(
            {
                "schema_version": ENGLISH_LEGACY_SOL_BATCH_SCHEMA,
                "issue_id": "EN-P0-006",
                "batch_id": _nonempty(batch_id, "batch_id"),
                "subject": "english",
                "batch_authorization_sha256": authorization_sha,
                "authorization_expansion_closure_sha256": closure_sha,
                "inventory_sha256": authorization["inventory_sha256"],
                "target_set_sha256": authorization["target_set_sha256"],
                "target_count": authorization["target_count"],
                "authority_generation": authorization["authority_generation"],
                "authority_fingerprint": authorization["authority_fingerprint"],
                "authorized_at": authorization["authorized_at"],
                "work_items": items,
                "status": "authorized",
                "formal_write_count": 0,
            }
        )

    def stage_english_legacy_recuration_batch(
        self,
        batch: Mapping[str, Any],
        *,
        config: Mapping[str, Any] | None = None,
        quality_verifier: Callable[
            [Mapping[str, Any], Path, str], Mapping[str, Any]
        ] | None = None,
        authorization_verifier: Callable[[str], Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        checked = validate_english_legacy_recuration_sol_batch_v1(batch)
        authorization_sha = checked["batch_authorization_sha256"]
        authorization = validate_english_legacy_batch_authorization_v1(
            self._read_content_addressed(
                self.english_legacy_batch_authorization_root,
                authorization_sha,
                "english_legacy_batch_authorization",
            )
        )
        self._verify_english_legacy_authority_seal(
            authorization, purpose="english-legacy-batch-authorization-v1"
        )
        if (
            _document_sha256(authorization) != authorization_sha
            or authorization["inventory_sha256"] != checked["inventory_sha256"]
            or authorization["target_set_sha256"] != checked["target_set_sha256"]
            or authorization["target_count"] != checked["target_count"]
            or authorization["authority_generation"] != checked["authority_generation"]
            or authorization["authority_fingerprint"] != checked["authority_fingerprint"]
            or authorization["authorized_at"] != checked["authorized_at"]
        ):
            raise SubjectSolContractError("english_legacy_batch_authorization_stale")
        closure_sha = checked["authorization_expansion_closure_sha256"]
        if authorization_verifier is None:
            try:
                from english_legacy_disposition import (
                    EnglishLegacyDispositionV3Store,
                )
            except ImportError as exc:
                raise SubjectSolContractError(
                    "english_legacy_authorization_verifier_unavailable"
                ) from exc
            legacy_store = EnglishLegacyDispositionV3Store(
                self.english_legacy_root,
                self.english_legacy_batch_authority_key_path,
            )

            def authorization_verifier(digest: str) -> Mapping[str, Any]:
                return legacy_store.reopen_verified_authorization_expansion_closure(
                    digest, require_live_inventory=False
                )

        try:
            authorization_closure = dict(authorization_verifier(closure_sha))
        except Exception as exc:
            code = getattr(
                exc, "code", "english_legacy_authorization_verification_failed"
            )
            raise SubjectSolContractError(str(code)) from exc
        if _document_sha256(authorization_closure) != closure_sha:
            raise SubjectSolContractError(
                "english_legacy_authorization_closure_digest_mismatch"
            )
        self._verify_english_legacy_authority_seal(
            authorization_closure,
            purpose="english-legacy-authorization-expansion-closure-v1",
        )
        expected_authorizations = [
            {
                "ordinal": row["ordinal"],
                "target_id": row["target_id"],
                "authorization_event_sha256": row["authorization_event_sha256"],
                "disposition_receipt_sha256": row[
                    "target_authorization_receipt_sha256"
                ],
            }
            for row in checked["work_items"]
        ]
        if (
            authorization_closure.get("schema_version")
            != "english_legacy_authorization_expansion_closure_v1"
            or authorization_closure.get("batch_authorization_sha256")
            != checked["batch_authorization_sha256"]
            or authorization_closure.get("inventory_sha256")
            != checked["inventory_sha256"]
            or authorization_closure.get("target_set_sha256")
            != checked["target_set_sha256"]
            or authorization_closure.get("target_count") != checked["target_count"]
            or authorization_closure.get("materialized_target_count")
            != checked["target_count"]
            or authorization_closure.get("target_authorizations")
            != expected_authorizations
            or any(
                authorization_closure.get(field) != 0
                for field in (
                    "unidentified_target_count", "omitted_target_count",
                    "duplicate_target_count", "unknown_target_count",
                )
            )
        ):
            raise SubjectSolContractError(
                "english_legacy_authorization_expansion_incomplete"
            )
        for item in checked["work_items"]:
            self._verify_english_legacy_item_authority(item, batch=checked)
        self._verify_english_legacy_luna_quality(
            checked, config=config, quality_verifier=quality_verifier
        )
        batch_sha, _ = self._publish_immutable(
            self.english_legacy_sol_batch_root, checked
        )
        entry = {
            "batch_id": checked["batch_id"],
            "subject": "english",
            "authorized_at": checked["authorized_at"],
            "authorization_receipt_sha256": authorization_sha,
            # Kept for global lease v1 compatibility; this digest names the
            # parent batch artifact, not a daily_sol_batch_v2 document.
            "daily_sol_batch_sha256": batch_sha,
            "status": "queued",
        }
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            existing = next(
                (row for row in global_state["queue"] if row["batch_id"] == checked["batch_id"]),
                None,
            )
            if existing is not None:
                if existing != entry:
                    raise SubjectSolContractError("sol_batch_id_conflict")
                state = self._read_english_legacy_state_locked(
                    checked, batch_sha256=batch_sha
                )
                return {"global": global_state, "batch": checked, "state": state}
            with _FileLock(self._subject_lock_path("english")):
                writer = self._read_writer_locked("english")
                if writer["handoff_status"] not in {
                    "awaiting_luna", "ready_for_authorization"
                }:
                    raise SubjectSolContractError("english_sol_writer_not_idle")
                global_state["queue"].append(entry)
                global_state["queue"].sort(
                    key=lambda row: (
                        row["authorized_at"], row["authorization_receipt_sha256"],
                        row["batch_id"],
                    )
                )
                global_written = self._write_global_locked(global_state)
                state = self._write_english_legacy_state_locked(
                    self._default_english_legacy_state(
                        checked, batch_sha256=batch_sha
                    ),
                    batch=checked,
                    batch_sha256=batch_sha,
                )
                writer.update(
                    {
                        "handoff_status": "queued_for_sol",
                        "batch_id": checked["batch_id"],
                        "authorization_receipt_sha256": authorization_sha,
                        "daily_sol_batch_sha256": batch_sha,
                    }
                )
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "batch": checked,
            "state": state,
        }

    @staticmethod
    def _start_english_legacy_item_attempt(
        state: dict[str, Any], *, fencing_token: int,
    ) -> int:
        ordinal = int(state["current_ordinal"])
        row = state["items"][ordinal - 1]
        attempt = len(row["attempts"]) + 1
        row["attempts"].append(
            {
                "attempt": attempt,
                "fencing_token": fencing_token,
                "review_receipt_sha256": None,
                "apply_receipt_sha256": None,
                "failure_receipt_sha256": None,
                "recovery_receipt_sha256": None,
            }
        )
        state["attempt"] = attempt
        state["fencing_token"] = fencing_token
        state["current_review_receipt_sha256"] = None
        return attempt

    def _read_english_legacy_writer_receipt(
        self, root: Path, digest: str, *, label: str, purpose: str,
    ) -> dict[str, Any]:
        value = self._read_content_addressed(root, digest, label)
        self._verify_external_seal(
            value,
            purpose=purpose,
            key_path=self.writer_adapter_authority_key_path,
            label=label,
        )
        return value

    def _verify_english_legacy_execution_evidence(
        self,
        receipt: Mapping[str, Any],
        *,
        failure: bool,
    ) -> None:
        """Reopen every persisted English writer execution authority object."""

        process_binding = _mapping(
            receipt.get("writer_process"), "writer_process"
        )
        transaction_binding = _mapping(
            receipt.get("transaction_end"), "transaction_end"
        )
        process_sha = _sha256(
            process_binding.get("evidence_sha256"),
            "writer_process_evidence_sha256",
        )
        transaction_sha = _sha256(
            transaction_binding.get("evidence_sha256"),
            "transaction_end_evidence_sha256",
        )
        process = self._read_writer_artifact(
            "writer-process",
            process_sha,
            purpose="isolated-writer-process-artifact",
        )
        transaction = self._read_writer_artifact(
            "transaction-end",
            transaction_sha,
            purpose="isolated-writer-transaction-artifact",
        )
        fence = receipt.get("fencing_token")
        if (
            set(process)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "adapter_run_id", "pid", "terminal_state", "exit_code",
                "stopped_at", "formal_write_count", "seal",
            }
            or process.get("schema_version")
            != "isolated_writer_process_artifact_v1"
            or process.get("batch_id") != receipt.get("batch_id")
            or process.get("subject") != "english"
            or process.get("fencing_token") != fence
            or process.get("adapter_run_id")
            != process_binding.get("adapter_run_id")
            or process.get("pid") != process_binding.get("pid")
            or process.get("terminal_state")
            != process_binding.get("terminal_state")
            or process.get("exit_code") != process_binding.get("exit_code")
            or process.get("stopped_at") != process_binding.get("stopped_at")
            or process.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "english_legacy_writer_process_artifact_invalid"
            )
        if (
            set(transaction)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "transaction_id", "state", "ended_at", "formal_write_count",
                "seal",
            }
            or transaction.get("schema_version")
            != "isolated_writer_transaction_artifact_v1"
            or transaction.get("batch_id") != receipt.get("batch_id")
            or transaction.get("subject") != "english"
            or transaction.get("fencing_token") != fence
            or transaction.get("transaction_id")
            != transaction_binding.get("transaction_id")
            or transaction.get("state") != transaction_binding.get("state")
            or transaction.get("ended_at") != transaction_binding.get("ended_at")
            or transaction.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "english_legacy_transaction_artifact_invalid"
            )
        if not failure:
            return
        recovery_sha = _sha256(
            receipt.get("recovery_position_sha256"),
            "recovery_position_sha256",
        )
        recovery = self._read_writer_artifact(
            "recovery-position",
            recovery_sha,
            purpose="isolated-writer-recovery-position-artifact",
        )
        if (
            set(recovery)
            != {
                "schema_version", "batch_id", "subject", "fencing_token",
                "writer_process_artifact_sha256",
                "transaction_artifact_sha256", "state_sha256",
                "formal_write_count", "seal",
            }
            or recovery.get("schema_version")
            != "isolated_writer_recovery_position_artifact_v1"
            or recovery.get("batch_id") != receipt.get("batch_id")
            or recovery.get("subject") != "english"
            or recovery.get("fencing_token") != fence
            or recovery.get("writer_process_artifact_sha256") != process_sha
            or recovery.get("transaction_artifact_sha256") != transaction_sha
            or recovery.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "english_legacy_recovery_position_artifact_invalid"
            )
        _sha256(recovery.get("state_sha256"), "recovery_state_sha256")

    def _read_english_legacy_checkpoint(
        self, digest: str, *, batch: Mapping[str, Any]
    ) -> dict[str, Any]:
        value = self._read_content_addressed(
            self.english_legacy_checkpoint_root,
            digest,
            "english_legacy_rolling_checkpoint",
        )
        self._verify_seal(
            value, purpose="english-legacy-rolling-authority-checkpoint-v1"
        )
        return validate_english_legacy_rolling_authority_checkpoint_v1(
            value, batch=batch
        )

    def _english_legacy_expected_authority(
        self, *, batch: Mapping[str, Any], state: Mapping[str, Any]
    ) -> str:
        checkpoint_sha = state.get("current_checkpoint_sha256")
        if checkpoint_sha is None:
            return str(batch["authority_fingerprint"])
        checkpoint = self._read_english_legacy_checkpoint(
            str(checkpoint_sha), batch=batch
        )
        return str(checkpoint["post_authority_sha256"])

    def _publish_english_legacy_checkpoint(
        self,
        *,
        batch: Mapping[str, Any],
        state: Mapping[str, Any],
        item_result_receipt_sha256: str,
        outcome: str,
        post_authority_sha256: str,
    ) -> tuple[dict[str, Any], str]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        ordinal = int(state["current_ordinal"])
        current_item = state["items"][ordinal - 1]
        previous_sha = state.get("current_checkpoint_sha256")
        if previous_sha is None:
            pre_authority = checked_batch["authority_fingerprint"]
        else:
            previous = self._read_english_legacy_checkpoint(
                str(previous_sha), batch=checked_batch
            )
            pre_authority = previous["post_authority_sha256"]
        statuses = [row["status"] for row in state["items"]]
        completed = sum(
            status in {"committed", "already_current", "failed"}
            for status in statuses
        )
        next_ordinal = (
            ordinal
            if outcome == "failed"
            else (ordinal + 1 if ordinal < checked_batch["target_count"] else None)
        )
        value = self._seal(
            {
                "schema_version": ENGLISH_LEGACY_ROLLING_CHECKPOINT_SCHEMA,
                "issue_id": "EN-P0-006",
                "batch_id": checked_batch["batch_id"],
                "batch_sha256": _document_sha256(checked_batch),
                "subject": "english",
                "fencing_token": state["fencing_token"],
                "attempt": state["attempt"],
                "ordinal": ordinal,
                "target_id": current_item["target_id"],
                "outcome": outcome,
                "previous_checkpoint_sha256": previous_sha,
                "item_result_receipt_sha256": _sha256(
                    item_result_receipt_sha256, "item_result_receipt_sha256"
                ),
                "pre_authority_sha256": pre_authority,
                "post_authority_sha256": _sha256(
                    post_authority_sha256, "post_authority_sha256"
                ),
                "completed_target_count": completed,
                "committed_count": statuses.count("committed"),
                "already_current_count": statuses.count("already_current"),
                "failed_count": statuses.count("failed"),
                "next_ordinal": next_ordinal,
                "formal_write_count": state["formal_write_count"],
                "issued_at": _utc_now(),
            },
            purpose="english-legacy-rolling-authority-checkpoint-v1",
        )
        validate_english_legacy_rolling_authority_checkpoint_v1(
            value, batch=checked_batch
        )
        digest, _ = self._publish_immutable(
            self.english_legacy_checkpoint_root, value
        )
        return value, digest

    def _publish_english_legacy_execution_closure(
        self, *, batch: Mapping[str, Any], state: Mapping[str, Any]
    ) -> tuple[dict[str, Any], str] | None:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        rows = list(state["items"])
        if any(
            row["status"] not in {"committed", "already_current", "failed"}
            for row in rows
        ):
            return None
        outcomes = []
        for row in rows:
            latest = row["attempts"][-1]
            final_receipt = (
                latest["apply_receipt_sha256"]
                or latest["failure_receipt_sha256"]
            )
            if final_receipt is None or row["checkpoint_sha256"] is None:
                raise SubjectSolContractError(
                    "english_legacy_terminal_item_receipt_missing"
                )
            outcomes.append(
                {
                    "ordinal": row["ordinal"],
                    "target_id": row["target_id"],
                    "status": row["status"],
                    "attempt_count": len(row["attempts"]),
                    "attempts": copy.deepcopy(row["attempts"]),
                    "final_receipt_sha256": final_receipt,
                    "checkpoint_sha256": row["checkpoint_sha256"],
                    "formal_write_count": row["formal_write_count"],
                }
            )
        failed = sum(row["status"] == "failed" for row in rows)
        value = self._seal(
            {
                "schema_version": ENGLISH_LEGACY_EXECUTION_CLOSURE_SCHEMA,
                "issue_id": "EN-P0-006",
                "batch_id": checked_batch["batch_id"],
                "batch_sha256": _document_sha256(checked_batch),
                "subject": "english",
                "inventory_sha256": checked_batch["inventory_sha256"],
                "target_set_sha256": checked_batch["target_set_sha256"],
                "target_count": checked_batch["target_count"],
                "final_checkpoint_sha256": state["current_checkpoint_sha256"],
                "item_outcomes": outcomes,
                "committed_count": sum(row["status"] == "committed" for row in rows),
                "already_current_count": sum(
                    row["status"] == "already_current" for row in rows
                ),
                "failed_count": failed,
                "omitted_count": 0,
                "unknown_count": 0,
                "duplicate_count": 0,
                "recovery_pending_count": failed,
                "formal_write_count": state["formal_write_count"],
                "status": (
                    "complete_with_failures" if failed else "verified_complete"
                ),
                "completed_at": _utc_now(),
            },
            purpose="english-legacy-recuration-execution-closure-v1",
        )
        validate_english_legacy_recuration_execution_closure_v1(
            value, batch=checked_batch
        )
        digest, _ = self._publish_immutable(
            self.english_legacy_execution_closure_root, value
        )
        return value, digest

    def begin_english_legacy_recuration(
        self, batch_id: str, *, owner_id: str
    ) -> dict[str, Any]:
        requested_batch = _nonempty(batch_id, "batch_id")
        owner = _nonempty(owner_id, "writer_owner_id")
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            if global_state["active_writer"] is not None:
                _, receipt_sha = self._claim_receipt(
                    batch_id=requested_batch, subject="english", owner_id=owner,
                    outcome="rejected", reason="global_sol_writer_busy",
                    active_writer=global_state["active_writer"], fencing_token=None,
                )
                raise SubjectSolContractError(
                    "global_sol_writer_busy", claim_receipt_sha256=receipt_sha
                )
            pending = [row for row in global_state["queue"] if row["status"] == "queued"]
            if not pending or pending[0]["batch_id"] != requested_batch:
                _, receipt_sha = self._claim_receipt(
                    batch_id=requested_batch, subject="english", owner_id=owner,
                    outcome="rejected", reason="global_sol_fifo_head_mismatch",
                    active_writer=None, fencing_token=None,
                )
                raise SubjectSolContractError(
                    "global_sol_fifo_head_mismatch", claim_receipt_sha256=receipt_sha
                )
            entry = pending[0]
            batch_sha = str(entry["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            if self._active_luna_count("english"):
                raise SubjectSolContractError("subject_luna_not_terminal")
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                writer = self._read_writer_locked("english")
                if state["status"] != "queued" or writer["handoff_status"] != "queued_for_sol":
                    raise SubjectSolContractError("english_legacy_sol_queue_state_mismatch")
                token = global_state["next_fencing_token"]
                _, claim_sha = self._claim_receipt(
                    batch_id=requested_batch, subject="english", owner_id=owner,
                    outcome="acquired", reason="english_legacy_parent_batch_acquired",
                    active_writer=None, fencing_token=token,
                )
                self._start_english_legacy_item_attempt(
                    state, fencing_token=token
                )
                state["status"] = "active"
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                active = {
                    "batch_id": requested_batch,
                    "subject": "english",
                    "fencing_token": token,
                    "owner_id": owner,
                    "claimed_at": _utc_now(),
                    "authorization_receipt_sha256": entry[
                        "authorization_receipt_sha256"
                    ],
                    "daily_sol_batch_sha256": batch_sha,
                    "review_receipt_sha256": None,
                    "status": "reviewing",
                    "current_item": state["current_target_id"],
                    "committed_count": 0,
                    "remaining_count": batch["target_count"],
                }
                entry["status"] = "active"
                global_state["active_writer"] = active
                global_state["next_fencing_token"] = token + 1
                global_written = self._write_global_locked(global_state)
                writer.update(
                    {
                        "handoff_status": "sol_reviewing",
                        "fencing_token": token,
                        "generation_fence": {
                            "blocked": True,
                            "source_generation": batch["authority_generation"],
                            "next_generation": None,
                        },
                    }
                )
                writer_written = self._write_writer_locked(writer)
        return {
            "schema_version": "english_legacy_parent_claim_result_v1",
            "claim_receipt_sha256": claim_sha,
            "fencing_token": token,
            "current_ordinal": state_written["current_ordinal"],
            "current_target_id": state_written["current_target_id"],
            "global": global_written,
            "subject": writer_written,
            "state": state_written,
            "formal_write_count": 0,
        }

    def record_english_legacy_item_review(
        self, review_receipt_sha256: str
    ) -> dict[str, Any]:
        review_sha = _sha256(review_receipt_sha256, "review_receipt_sha256")
        raw = self._read_english_legacy_writer_receipt(
            self.english_legacy_item_review_root,
            review_sha,
            label="english_legacy_item_review_receipt",
            purpose="english-legacy-sol-item-review-receipt-v1",
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if not isinstance(active, Mapping) or active.get("subject") != "english":
                raise SubjectSolContractError("global_sol_writer_not_active")
            batch_sha = str(active["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            receipt = validate_english_legacy_sol_item_review_receipt_v1(
                raw, batch=batch
            )
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                if (
                    state["status"] != "active"
                    or receipt["fencing_token"] != state["fencing_token"]
                    or receipt["attempt"] != state["attempt"]
                    or receipt["ordinal"] != state["current_ordinal"]
                    or receipt["target_id"] != state["current_target_id"]
                    or receipt["previous_checkpoint_sha256"]
                    != state["current_checkpoint_sha256"]
                    or receipt["authority_checkpoint_sha256"]
                    != self._english_legacy_expected_authority(
                        batch=batch, state=state
                    )
                    or active.get("status") != "reviewing"
                ):
                    raise SubjectSolContractError("stale_english_legacy_item_review")
                item_state = state["items"][receipt["ordinal"] - 1]
                attempt = item_state["attempts"][-1]
                if attempt["review_receipt_sha256"] is not None:
                    raise SubjectSolContractError("english_legacy_item_review_replayed")
                attempt["review_receipt_sha256"] = review_sha
                state["current_review_receipt_sha256"] = review_sha
                active = dict(active)
                active["review_receipt_sha256"] = review_sha
                writer = self._read_writer_locked("english")
                if receipt["status"] == "approved":
                    item_state["status"] = "reviewed"
                    active["status"] = "applying"
                    writer["handoff_status"] = "sol_reviewed"
                else:
                    active["status"] = "failed"
                    writer["handoff_status"] = "failed"
                global_state["active_writer"] = active
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                global_written = self._write_global_locked(global_state)
                writer["review_receipt_sha256"] = review_sha
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "state": state_written,
            "review_receipt_sha256": review_sha,
            "formal_write_count": 0,
        }

    def finish_english_legacy_item(
        self, apply_receipt_sha256: str
    ) -> dict[str, Any]:
        apply_sha = _sha256(apply_receipt_sha256, "apply_receipt_sha256")
        raw = self._read_english_legacy_writer_receipt(
            self.english_legacy_item_apply_root,
            apply_sha,
            label="english_legacy_item_apply_receipt",
            purpose="english-legacy-sol-item-apply-receipt-v1",
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if (
                not isinstance(active, Mapping)
                or active.get("subject") != "english"
                or active.get("status") != "applying"
            ):
                raise SubjectSolContractError("global_sol_writer_not_applying")
            batch_sha = str(active["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            receipt = validate_english_legacy_sol_item_apply_receipt_v1(
                raw, batch=batch
            )
            self._verify_english_legacy_execution_evidence(
                receipt, failure=False
            )
            review_sha = str(receipt["review_receipt_sha256"])
            review = validate_english_legacy_sol_item_review_receipt_v1(
                self._read_english_legacy_writer_receipt(
                    self.english_legacy_item_review_root,
                    review_sha,
                    label="english_legacy_item_review_receipt",
                    purpose="english-legacy-sol-item-review-receipt-v1",
                ),
                batch=batch,
            )
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                if (
                    state["status"] != "active"
                    or receipt["fencing_token"] != state["fencing_token"]
                    or receipt["attempt"] != state["attempt"]
                    or receipt["ordinal"] != state["current_ordinal"]
                    or receipt["target_id"] != state["current_target_id"]
                    or receipt["previous_checkpoint_sha256"]
                    != state["current_checkpoint_sha256"]
                    or review_sha != state["current_review_receipt_sha256"]
                    or review["fencing_token"] != receipt["fencing_token"]
                    or review["attempt"] != receipt["attempt"]
                    or review["authority_checkpoint_sha256"]
                    != receipt["pre_authority_sha256"]
                    or receipt["pre_authority_sha256"]
                    != self._english_legacy_expected_authority(
                        batch=batch, state=state
                    )
                    or receipt["pre_object_sha256"]
                    != review["current_object_sha256"]
                    or (
                        receipt["status"] == "committed"
                        and (
                            review["decision"] != "update_existing"
                            or receipt["post_object_sha256"]
                            != review["desired_object_sha256"]
                        )
                    )
                    or (
                        receipt["status"] == "already_current"
                        and review["decision"] != "already_current"
                    )
                ):
                    raise SubjectSolContractError("stale_english_legacy_item_apply")
                item_state = state["items"][receipt["ordinal"] - 1]
                attempt = item_state["attempts"][-1]
                if attempt["apply_receipt_sha256"] is not None:
                    raise SubjectSolContractError("english_legacy_item_apply_replayed")
                attempt["apply_receipt_sha256"] = apply_sha
                item_state["status"] = receipt["status"]
                item_state["formal_write_count"] = receipt["formal_write_count"]
                state["formal_write_count"] = sum(
                    row["formal_write_count"] for row in state["items"]
                )
                _, checkpoint_sha = self._publish_english_legacy_checkpoint(
                    batch=batch,
                    state=state,
                    item_result_receipt_sha256=apply_sha,
                    outcome=receipt["status"],
                    post_authority_sha256=receipt["post_authority_sha256"],
                )
                item_state["checkpoint_sha256"] = checkpoint_sha
                state["current_checkpoint_sha256"] = checkpoint_sha
                ordinal = int(receipt["ordinal"])
                completed = sum(
                    row["status"] in {"committed", "already_current"}
                    for row in state["items"]
                )
                active = dict(active)
                writer = self._read_writer_locked("english")
                queue_entry = next(
                    row for row in global_state["queue"]
                    if row["batch_id"] == batch["batch_id"]
                )
                closure_value = None
                closure_sha = None
                if ordinal < batch["target_count"]:
                    next_item = state["items"][ordinal]
                    if next_item["status"] != "pending" or next_item["attempts"]:
                        raise SubjectSolContractError(
                            "english_legacy_completed_item_replay_detected"
                        )
                    state["current_ordinal"] = ordinal + 1
                    state["current_target_id"] = next_item["target_id"]
                    self._start_english_legacy_item_attempt(
                        state, fencing_token=int(active["fencing_token"])
                    )
                    state["status"] = "active"
                    active["status"] = "reviewing"
                    active["current_item"] = next_item["target_id"]
                    active["review_receipt_sha256"] = None
                    active["committed_count"] = completed
                    active["remaining_count"] = batch["target_count"] - completed
                    global_state["active_writer"] = active
                    queue_entry["status"] = "active"
                    writer["handoff_status"] = "sol_reviewing"
                    writer["review_receipt_sha256"] = None
                else:
                    state["current_ordinal"] = None
                    state["current_target_id"] = None
                    state["current_review_receipt_sha256"] = None
                    state["status"] = "complete"
                    closure = self._publish_english_legacy_execution_closure(
                        batch=batch, state=state
                    )
                    if closure is None:
                        raise SubjectSolContractError(
                            "english_legacy_execution_closure_not_terminal"
                        )
                    closure_value, closure_sha = closure
                    state["execution_closure_sha256"] = closure_sha
                    global_state["active_writer"] = None
                    queue_entry["status"] = "committed"
                    writer["handoff_status"] = "complete"
                    writer["commit_receipt_sha256"] = apply_sha
                global_state["formal_write_count"] += receipt["formal_write_count"]
                writer["formal_write_count"] += receipt["formal_write_count"]
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                global_written = self._write_global_locked(global_state)
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "state": state_written,
            "apply_receipt_sha256": apply_sha,
            "checkpoint_sha256": checkpoint_sha,
            "execution_closure": closure_value,
            "execution_closure_sha256": closure_sha,
            "formal_write_count": receipt["formal_write_count"],
        }

    def record_english_legacy_item_failure(
        self, failure_receipt_sha256: str
    ) -> dict[str, Any]:
        failure_sha = _sha256(failure_receipt_sha256, "failure_receipt_sha256")
        raw = self._read_english_legacy_writer_receipt(
            self.english_legacy_item_failure_root,
            failure_sha,
            label="english_legacy_item_failure_receipt",
            purpose="english-legacy-sol-item-failure-receipt-v1",
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if not isinstance(active, Mapping) or active.get("subject") != "english":
                raise SubjectSolContractError("global_sol_writer_not_active")
            batch_sha = str(active["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            receipt = validate_english_legacy_sol_item_failure_receipt_v1(
                raw, batch=batch
            )
            self._verify_english_legacy_execution_evidence(
                receipt, failure=True
            )
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                if (
                    state["status"] != "active"
                    or receipt["fencing_token"] != state["fencing_token"]
                    or receipt["attempt"] != state["attempt"]
                    or receipt["ordinal"] != state["current_ordinal"]
                    or receipt["target_id"] != state["current_target_id"]
                    or receipt["previous_checkpoint_sha256"]
                    != state["current_checkpoint_sha256"]
                    or receipt["review_receipt_sha256"]
                    != state["current_review_receipt_sha256"]
                    or receipt["authority_checkpoint_sha256"]
                    != self._english_legacy_expected_authority(
                        batch=batch, state=state
                    )
                ):
                    raise SubjectSolContractError("stale_english_legacy_item_failure")
                item_state = state["items"][receipt["ordinal"] - 1]
                attempt = item_state["attempts"][-1]
                if attempt["failure_receipt_sha256"] is not None:
                    raise SubjectSolContractError("english_legacy_item_failure_replayed")
                attempt["failure_receipt_sha256"] = failure_sha
                item_state["status"] = "failed"
                _, checkpoint_sha = self._publish_english_legacy_checkpoint(
                    batch=batch,
                    state=state,
                    item_result_receipt_sha256=failure_sha,
                    outcome="failed",
                    post_authority_sha256=receipt[
                        "authority_checkpoint_sha256"
                    ],
                )
                item_state["checkpoint_sha256"] = checkpoint_sha
                state["current_checkpoint_sha256"] = checkpoint_sha
                state["status"] = "safe_paused"
                queue_entry = next(
                    row for row in global_state["queue"]
                    if row["batch_id"] == batch["batch_id"]
                )
                queue_entry["status"] = "safe_paused"
                global_state["active_writer"] = None
                closure_value = None
                closure_sha = None
                closure = self._publish_english_legacy_execution_closure(
                    batch=batch, state=state
                )
                if closure is not None:
                    closure_value, closure_sha = closure
                    state["execution_closure_sha256"] = closure_sha
                writer = self._read_writer_locked("english")
                writer["handoff_status"] = "safe_paused"
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                global_written = self._write_global_locked(global_state)
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "state": state_written,
            "failure_receipt_sha256": failure_sha,
            "checkpoint_sha256": checkpoint_sha,
            "execution_closure": closure_value,
            "execution_closure_sha256": closure_sha,
            "formal_write_count": 0,
        }

    def begin_english_legacy_recovery(
        self, batch_id: str, *, owner_id: str
    ) -> dict[str, Any]:
        requested_batch = _nonempty(batch_id, "batch_id")
        owner = _nonempty(owner_id, "writer_owner_id")
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            if global_state["active_writer"] is not None:
                raise SubjectSolContractError("global_sol_writer_busy")
            entry = next(
                (row for row in global_state["queue"] if row["batch_id"] == requested_batch),
                None,
            )
            if entry is None or entry["status"] != "safe_paused":
                raise SubjectSolContractError("english_legacy_batch_not_recoverable")
            candidates = [
                row for row in global_state["queue"]
                if row["status"] == "queued" or row["batch_id"] == requested_batch
            ]
            candidates.sort(
                key=lambda row: (
                    row["authorized_at"], row["authorization_receipt_sha256"],
                    row["batch_id"],
                )
            )
            if not candidates or candidates[0]["batch_id"] != requested_batch:
                raise SubjectSolContractError("global_sol_fifo_head_mismatch")
            batch_sha = str(entry["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                if state["status"] != "safe_paused":
                    raise SubjectSolContractError("english_legacy_batch_not_recoverable")
                item_state = state["items"][int(state["current_ordinal"]) - 1]
                if item_state["status"] != "failed" or not item_state["attempts"]:
                    raise SubjectSolContractError("english_legacy_failed_item_missing")
                previous_attempt = item_state["attempts"][-1]
                failure_sha = previous_attempt["failure_receipt_sha256"]
                if failure_sha is None:
                    raise SubjectSolContractError("english_legacy_failure_receipt_missing")
                failure = validate_english_legacy_sol_item_failure_receipt_v1(
                    self._read_english_legacy_writer_receipt(
                        self.english_legacy_item_failure_root,
                        failure_sha,
                        label="english_legacy_item_failure_receipt",
                        purpose="english-legacy-sol-item-failure-receipt-v1",
                    ),
                    batch=batch,
                )
                self._verify_english_legacy_execution_evidence(
                    failure, failure=True
                )
                if failure["retryable"] is not True:
                    raise SubjectSolContractError("english_legacy_failure_not_retryable")
                token = global_state["next_fencing_token"]
                _, claim_sha = self._claim_receipt(
                    batch_id=requested_batch, subject="english", owner_id=owner,
                    outcome="acquired", reason="english_legacy_recovery_acquired",
                    active_writer=None, fencing_token=token,
                )
                self._start_english_legacy_item_attempt(
                    state, fencing_token=token
                )
                state["status"] = "recovering"
                state["execution_closure_sha256"] = None
                active = {
                    "batch_id": requested_batch,
                    "subject": "english",
                    "fencing_token": token,
                    "owner_id": owner,
                    "claimed_at": _utc_now(),
                    "authorization_receipt_sha256": entry[
                        "authorization_receipt_sha256"
                    ],
                    "daily_sol_batch_sha256": batch_sha,
                    "review_receipt_sha256": None,
                    "status": "recovering",
                    "current_item": state["current_target_id"],
                    "committed_count": sum(
                        row["status"] in {"committed", "already_current"}
                        for row in state["items"]
                    ),
                    "remaining_count": sum(
                        row["status"] not in {"committed", "already_current"}
                        for row in state["items"]
                    ),
                }
                entry["status"] = "active"
                global_state["active_writer"] = active
                global_state["next_fencing_token"] = token + 1
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                global_written = self._write_global_locked(global_state)
                writer = self._read_writer_locked("english")
                writer["handoff_status"] = "sol_reviewing"
                writer["fencing_token"] = token
                writer_written = self._write_writer_locked(writer)
        return {
            "schema_version": "english_legacy_recovery_claim_result_v1",
            "claim_receipt_sha256": claim_sha,
            "prior_fencing_token": previous_attempt["fencing_token"],
            "new_fencing_token": token,
            "attempt": state_written["attempt"],
            "failure_receipt_sha256": failure_sha,
            "recovery_position_sha256": failure["recovery_position_sha256"],
            "checkpoint_sha256": state_written["current_checkpoint_sha256"],
            "global": global_written,
            "state": state_written,
            "formal_write_count": 0,
        }

    def record_english_legacy_item_recovery(
        self, recovery_receipt_sha256: str
    ) -> dict[str, Any]:
        recovery_sha = _sha256(recovery_receipt_sha256, "recovery_receipt_sha256")
        raw = self._read_english_legacy_writer_receipt(
            self.english_legacy_item_recovery_root,
            recovery_sha,
            label="english_legacy_item_recovery_receipt",
            purpose="english-legacy-sol-item-recovery-receipt-v1",
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if (
                not isinstance(active, Mapping)
                or active.get("subject") != "english"
                or active.get("status") != "recovering"
            ):
                raise SubjectSolContractError("english_legacy_recovery_not_active")
            batch_sha = str(active["daily_sol_batch_sha256"])
            batch = self._english_legacy_batch_by_digest(batch_sha)
            receipt = validate_english_legacy_sol_item_recovery_receipt_v1(
                raw, batch=batch
            )
            with _FileLock(self._subject_lock_path("english")):
                state = self._read_english_legacy_state_locked(
                    batch, batch_sha256=batch_sha
                )
                item_state = state["items"][int(state["current_ordinal"]) - 1]
                previous_attempt = item_state["attempts"][-2]
                current_attempt = item_state["attempts"][-1]
                failure_sha = previous_attempt["failure_receipt_sha256"]
                failure = validate_english_legacy_sol_item_failure_receipt_v1(
                    self._read_english_legacy_writer_receipt(
                        self.english_legacy_item_failure_root,
                        str(failure_sha),
                        label="english_legacy_item_failure_receipt",
                        purpose="english-legacy-sol-item-failure-receipt-v1",
                    ),
                    batch=batch,
                )
                self._verify_english_legacy_execution_evidence(
                    failure, failure=True
                )
                if (
                    state["status"] != "recovering"
                    or receipt["new_fencing_token"] != state["fencing_token"]
                    or receipt["prior_fencing_token"]
                    != previous_attempt["fencing_token"]
                    or receipt["attempt"] != state["attempt"]
                    or receipt["ordinal"] != state["current_ordinal"]
                    or receipt["target_id"] != state["current_target_id"]
                    or receipt["previous_checkpoint_sha256"]
                    != state["current_checkpoint_sha256"]
                    or receipt["failure_receipt_sha256"] != failure_sha
                    or receipt["recovery_position_sha256"]
                    != failure["recovery_position_sha256"]
                    or receipt["authority_checkpoint_sha256"]
                    != self._english_legacy_expected_authority(
                        batch=batch, state=state
                    )
                    or current_attempt["recovery_receipt_sha256"] is not None
                ):
                    raise SubjectSolContractError("stale_english_legacy_recovery")
                current_attempt["recovery_receipt_sha256"] = recovery_sha
                item_state["status"] = "pending"
                state["status"] = "active"
                active = dict(active)
                active["status"] = "reviewing"
                global_state["active_writer"] = active
                state_written = self._write_english_legacy_state_locked(
                    state, batch=batch, batch_sha256=batch_sha
                )
                global_written = self._write_global_locked(global_state)
        return {
            "global": global_written,
            "state": state_written,
            "recovery_receipt_sha256": recovery_sha,
            "formal_write_count": 0,
        }

    def prepare_subject_exclusion_authority(
        self,
        subject: str,
        *,
        original_batch_id: str,
        capture_id: str,
        unit_sha256: str,
    ) -> dict[str, Any]:
        checked_subject = _subject(subject)
        checked_batch_id = _nonempty(original_batch_id, "original_batch_id")
        checked_capture_id = _nonempty(capture_id, "capture_id")
        checked_unit = _sha256(unit_sha256, "unit_sha256")
        with _FileLock(self._subject_lock_path(checked_subject)):
            batch = self._read_batch_locked(checked_subject)
            if batch is None or batch["batch_id"] != checked_batch_id:
                raise SubjectSolContractError("subject_exclusion_batch_not_found")
            if (
                batch["status"] != "frozen"
                or batch["all_terminal"] is not True
                or batch["exclusion_receipt_sha256s"]
            ):
                raise SubjectSolContractError(
                    "subject_exclusion_original_batch_not_closed"
                )
            target = next(
                (
                    row
                    for row in batch["tasks"]
                    if row["capture_id"] == checked_capture_id
                    and row["unit_sha256"] == checked_unit
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("subject_exclusion_task_not_found")
            if target["status"] not in {
                "needs_rework",
                "failed",
                "evidence_pending",
            }:
                raise SubjectSolContractError("subject_exclusion_task_not_blocking")
            writer = self._read_writer_locked(checked_subject)
            if (
                writer["handoff_status"] != "awaiting_luna"
                or writer["batch_id"] != batch["batch_id"]
            ):
                raise SubjectSolContractError(
                    "subject_exclusion_writer_state_not_safe"
                )
            self._verify_excluded_task_evidence(target, original_batch=batch)
            batch_sha, _ = self._publish_immutable(
                self.subject_batch_snapshot_root,
                batch,
            )
            return {
                "schema_version": "subject_exclusion_authority_request_v1",
                "subject": checked_subject,
                "original_batch_id": batch["batch_id"],
                "original_batch_sha256": batch_sha,
                "capture_id": target["capture_id"],
                "unit_sha256": target["unit_sha256"],
                "formal_write_count": 0,
            }

    def issue_subject_exclusion_receipt(
        self,
        subject: str,
        *,
        authorization_receipt_sha256: str,
        issued_at: str | None = None,
    ) -> dict[str, Any]:
        """Consume one independent user authorization; never sign user intent."""

        checked_subject = _subject(subject)
        authorization_sha = _sha256(
            authorization_receipt_sha256,
            "user_exclusion_authorization_receipt_sha256",
        )
        authorization = self._read_content_addressed(
            self.user_exclusion_authorization_root,
            authorization_sha,
            "user_subject_exclusion_authorization_receipt",
        )
        self._verify_external_seal(
            authorization,
            purpose="user-subject-exclusion-authorization-receipt",
            key_path=self.user_exclusion_authority_key_path,
            label="user_subject_exclusion_authorization",
        )
        if (
            authorization.get("schema_version")
            != USER_SUBJECT_EXCLUSION_AUTHORIZATION_SCHEMA
            or authorization.get("subject") != checked_subject
            or authorization.get("event_type")
            != "explicit_user_subject_exclusion"
            or authorization.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError("user_subject_exclusion_authorization_invalid")
        checked_batch_id = _nonempty(
            authorization.get("original_batch_id"), "original_batch_id"
        )
        checked_capture_id = _nonempty(
            authorization.get("capture_id"), "capture_id"
        )
        checked_unit = _sha256(authorization.get("unit_sha256"), "unit_sha256")
        original_batch_sha = _sha256(
            authorization.get("original_batch_sha256"), "original_batch_sha256"
        )
        _nonempty(authorization.get("event_id"), "exclusion_authorization_event_id")
        _timestamp(authorization.get("authorized_at"), "exclusion_authorized_at")
        reason = _nonempty(authorization.get("reason"), "exclusion_reason")
        with _FileLock(self._subject_lock_path(checked_subject)):
            consumed_path = (
                self.consumed_user_exclusion_root / f"{authorization_sha}.json"
            )
            if consumed_path.exists():
                raise SubjectSolContractError(
                    "subject_exclusion_authorization_replayed"
                )
            batch = self._read_batch_locked(checked_subject)
            if batch is None or batch["batch_id"] != checked_batch_id:
                raise SubjectSolContractError("subject_exclusion_batch_not_found")
            if batch["exclusion_receipt_sha256s"]:
                raise SubjectSolContractError("subject_exclusion_authorization_replayed")
            current_sha = _document_sha256(batch)
            if current_sha != original_batch_sha:
                raise SubjectSolContractError("subject_exclusion_authorization_stale")
            target = next(
                (
                    row for row in batch["tasks"]
                    if row["capture_id"] == checked_capture_id
                    and row["unit_sha256"] == checked_unit
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("subject_exclusion_task_not_found")
            if target["status"] not in {"needs_rework", "failed", "evidence_pending"}:
                raise SubjectSolContractError("subject_exclusion_task_not_blocking")
            writer = self._read_writer_locked(checked_subject)
            if (
                writer["handoff_status"] != "awaiting_luna"
                or writer["batch_id"] != batch["batch_id"]
            ):
                raise SubjectSolContractError("subject_exclusion_writer_state_not_safe")
            self._verify_excluded_task_evidence(target, original_batch=batch)
            reopened = self._read_content_addressed(
                self.subject_batch_snapshot_root,
                original_batch_sha,
                "subject_exclusion_original_batch",
            )
            if reopened != batch:
                raise SubjectSolContractError("subject_exclusion_authorization_stale")
            issue_time = issued_at or _utc_now()
            value = {
                "schema_version": SUBJECT_EXCLUSION_RECEIPT_SCHEMA,
                "subject": checked_subject,
                "original_batch": {
                    "batch_id": batch["batch_id"],
                    "batch_sha256": original_batch_sha,
                    "study_date": batch["study_date"],
                    "authority_generation": batch["authority_generation"],
                    "authority_fingerprint": batch["authority_fingerprint"],
                },
                "excluded_task": copy.deepcopy(target),
                "user_authorization": copy.deepcopy(authorization),
                "user_authorization_receipt_sha256": authorization_sha,
                "reason": reason,
                "authority_key_id": self._authority_key_id(),
                "formal_write_count": 0,
                "issued_at": _timestamp(issue_time, "exclusion_issued_at"),
            }
            receipt = validate_subject_exclusion_receipt_v1(
                self._seal(value, purpose="subject-exclusion-receipt")
            )
            self._verify_seal(receipt, purpose="subject-exclusion-receipt")
            receipt_sha, _ = self._publish_immutable(
                self.receipt_root / "subject-exclusion",
                receipt,
            )
            self._atomic_json(
                consumed_path,
                {
                    "schema_version": "consumed_user_subject_exclusion_authorization_v1",
                    "authorization_receipt_sha256": authorization_sha,
                    "exclusion_receipt_sha256": receipt_sha,
                },
            )
            return copy.deepcopy(receipt)

    def create_subject_batch(
        self,
        *,
        batch_id: str,
        subject: str,
        study_date: str,
        capture_high_watermark: str,
        authority_generation: str,
        authority_fingerprint: str,
        tasks: Sequence[Mapping[str, Any]],
        exclusion_receipt_sha256s: Sequence[str] = (),
        scan_snapshot_sha256: str | None = None,
    ) -> dict[str, Any]:
        if not exclusion_receipt_sha256s:
            if scan_snapshot_sha256 is None:
                raise SubjectSolContractError("scan_snapshot_sha256_required")
            return self.prepare_and_freeze_subject_batch(
                batch_id=batch_id,
                subject=subject,
                study_date=study_date,
                capture_high_watermark=capture_high_watermark,
                scan_snapshot_sha256=scan_snapshot_sha256,
                authority_generation=authority_generation,
                authority_fingerprint=authority_fingerprint,
                tasks=tasks,
            )
        checked_subject = _subject(subject)
        rows = []
        for raw in tasks:
            row = dict(raw)
            rows.append(
                {
                    "capture_id": _nonempty(row.get("capture_id"), "capture_id"),
                    "unit_sha256": _sha256(row.get("unit_sha256"), "unit_sha256"),
                    "input_fingerprint": _sha256(
                        row.get("input_fingerprint"), "input_fingerprint"
                    ),
                    "study_date": _nonempty(row.get("study_date"), "task_study_date"),
                    "frozen_payload_sha256": _sha256(
                        row.get("frozen_payload_sha256"), "frozen_payload_sha256"
                    ),
                    "status": row.get("status", "selected"),
                    "proposal_sha256": row.get("proposal_sha256"),
                    "package_sha256": row.get("package_sha256"),
                    "quality_receipt_sha256": row.get("quality_receipt_sha256"),
                    "terminal_receipt_sha256": row.get("terminal_receipt_sha256"),
                    "error_code": row.get("error_code"),
                }
            )
        value = {
            "schema_version": SUBJECT_BATCH_SCHEMA,
            "batch_id": _nonempty(batch_id, "batch_id"),
            "subject": checked_subject,
            "study_date": _nonempty(study_date, "study_date"),
            "status": "frozen",
            "capture_high_watermark": _nonempty(
                capture_high_watermark, "capture_high_watermark"
            ),
            "scan_snapshot_sha256": "0" * 64,
            "authority_generation": _nonempty(
                authority_generation, "authority_generation"
            ),
            "authority_fingerprint": _sha256(
                authority_fingerprint, "authority_fingerprint"
            ),
            "tasks": rows,
            "exclusion_receipt_sha256s": sorted(exclusion_receipt_sha256s),
            "all_terminal": False,
            "sol_ready": False,
            "blocking_task_ids": [],
            "formal_write_count": 0,
            "revision": -1,
            "updated_at": None,
        }
        with _FileLock(self._subject_lock_path(checked_subject)):
            existing = self._read_batch_locked(checked_subject)
            if existing is not None:
                if existing["batch_id"] == batch_id:
                    candidate = copy.deepcopy(value)
                    self._recompute_batch(candidate)
                    candidate["revision"] = existing["revision"]
                    candidate["updated_at"] = existing["updated_at"]
                    if candidate == existing:
                        return existing
                if not value["exclusion_receipt_sha256s"]:
                    raise SubjectSolContractError("subject_luna_batch_already_open")
                if (
                    existing["status"] != "frozen"
                    or existing["all_terminal"] is not True
                    or existing["exclusion_receipt_sha256s"]
                ):
                    raise SubjectSolContractError(
                        "subject_exclusion_original_batch_not_closed"
                    )
                current_path = self._batch_path(checked_subject)
                try:
                    current_sha = hashlib.sha256(current_path.read_bytes()).hexdigest()
                except OSError as exc:
                    raise SubjectSolContractError(
                        "subject_exclusion_original_batch_unreadable"
                    ) from exc
                for digest in value["exclusion_receipt_sha256s"]:
                    receipt, original = self._verify_exclusion_receipt_by_digest(
                        digest
                    )
                    if (
                        receipt["original_batch"]["batch_sha256"] != current_sha
                        or original != existing
                    ):
                        raise SubjectSolContractError(
                            "subject_exclusion_receipt_stale"
                        )
                candidate = copy.deepcopy(value)
                candidate["scan_snapshot_sha256"] = existing["scan_snapshot_sha256"]
                self._recompute_batch(candidate)
                self._verify_batch_exclusions_locked(candidate)
                writer = self._read_writer_locked(checked_subject)
                if (
                    writer["handoff_status"] != "awaiting_luna"
                    or writer["batch_id"] != existing["batch_id"]
                ):
                    raise SubjectSolContractError(
                        "subject_exclusion_writer_state_not_safe"
                    )
                written = self._write_batch_locked(candidate)
                writer["batch_id"] = written["batch_id"]
                writer["handoff_status"] = "awaiting_luna"
                self._write_writer_locked(writer)
                return written
            if value["exclusion_receipt_sha256s"]:
                raise SubjectSolContractError(
                    "subject_exclusion_original_batch_missing"
                )
            return self._write_batch_locked(value)

    def _read_background_rollover_pointer_locked(
        self, subject: str
    ) -> dict[str, Any] | None:
        value = self._read_json(
            self._background_rollover_pointer_path(subject),
            "subject_background_rollover_pointer",
        )
        if value is None:
            return None
        required = {
            "schema_version",
            "subject",
            "old_batch_id",
            "old_batch_sha256",
            "rollover_id",
            "rollover_receipt_sha256",
            "rollover_receipt_path",
            "writer_revision_after",
            "applied_at",
            "formal_write_count",
            "seal",
        }
        if (
            set(value) != required
            or value.get("schema_version")
            != "subject_background_luna_rollover_pointer_v1"
            or value.get("subject") != _subject(subject)
            or value.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_pointer_invalid"
            )
        for key in (
            "old_batch_sha256",
            "rollover_id",
            "rollover_receipt_sha256",
        ):
            _sha256(value.get(key), key)
        _integer(value.get("writer_revision_after"), "writer_revision_after")
        _timestamp(value.get("applied_at"), "rollover_applied_at")
        self._verify_seal(
            value, purpose="subject-background-luna-rollover-pointer"
        )
        return value

    def _read_background_rollover_receipt(
        self, receipt_sha256: str
    ) -> dict[str, Any]:
        receipt = self._read_content_addressed(
            self.background_rollover_receipt_root,
            receipt_sha256,
            "subject_background_rollover_receipt",
        )
        if (
            receipt.get("schema_version")
            == SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
        ):
            return self._validate_background_rollover_receipt_v2(receipt)
        required = {
            "schema_version",
            "rollover_id",
            "mode",
            "subject",
            "old_batch_id",
            "old_batch_sha256",
            "old_batch_snapshot_sha256",
            "old_batch_snapshot_path",
            "old_batch_revision",
            "archive_sha256",
            "archive_path",
            "task_set_sha256",
            "task_count",
            "terminal_status_counts",
            "quality_receipt_sha256s",
            "terminal_receipt_sha256s",
            "evidence_reference_set_sha256",
            "authority_generation",
            "authority_fingerprint",
            "next_generation",
            "writer_revision_before",
            "writer_revision_after",
            "resume_acceptance_sha256",
            "sol_called",
            "sol_enabled",
            "model_call_count",
            "provider_request_count",
            "formal_write_count",
            "created_at",
            "seal",
        }
        if (
            set(receipt) != required
            or receipt.get("schema_version")
            != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_SCHEMA
            or receipt.get("mode") not in {"auto_success", "explicit_failure_resume"}
            or receipt.get("next_generation")
            != receipt.get("authority_generation")
            or receipt.get("sol_called") is not False
            or receipt.get("sol_enabled") is not False
            or receipt.get("model_call_count") != 0
            or receipt.get("provider_request_count") != 0
            or receipt.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_receipt_invalid"
            )
        _subject(receipt.get("subject"))
        for key in (
            "rollover_id",
            "old_batch_sha256",
            "old_batch_snapshot_sha256",
            "archive_sha256",
            "task_set_sha256",
            "evidence_reference_set_sha256",
            "authority_fingerprint",
        ):
            _sha256(receipt.get(key), key)
        _integer(receipt.get("old_batch_revision"), "old_batch_revision")
        _integer(receipt.get("task_count"), "task_count", minimum=1)
        _integer(receipt.get("writer_revision_before"), "writer_revision_before")
        _integer(receipt.get("writer_revision_after"), "writer_revision_after")
        if receipt["writer_revision_after"] != receipt["writer_revision_before"] + 1:
            raise SubjectSolContractError(
                "subject_background_rollover_writer_revision_invalid"
            )
        if receipt["mode"] == "explicit_failure_resume":
            _sha256(receipt.get("resume_acceptance_sha256"), "resume_acceptance_sha256")
        elif receipt.get("resume_acceptance_sha256") is not None:
            raise SubjectSolContractError(
                "subject_background_rollover_resume_binding_invalid"
            )
        _timestamp(receipt.get("created_at"), "rollover_created_at")
        self._verify_seal(
            receipt, purpose="subject-background-luna-rollover"
        )
        return receipt

    def _validate_background_rollover_receipt_v2(
        self, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt = copy.deepcopy(dict(value))
        required = {
            "schema_version",
            "rollover_id",
            "mode",
            "subject",
            "old_batch_id",
            "old_batch_sha256",
            "old_batch_snapshot_sha256",
            "old_batch_snapshot_path",
            "old_batch_revision",
            "archive_sha256",
            "archive_path",
            "task_set_sha256",
            "task_count",
            "terminal_status_counts",
            "quality_receipt_sha256s",
            "terminal_receipt_sha256s",
            "evidence_reference_set_sha256",
            "source_generation",
            "source_authority_fingerprint",
            "next_generation",
            "next_authority_fingerprint",
            "original_preclaim_failure_receipt_sha256",
            "original_preclaim_failure_receipt_path",
            "preserved_queue_entry_sha256",
            "preserved_queue_identity",
            "canary_state_before",
            "canary_state_before_sha256",
            "subsequent_attempt_receipt_sha256s",
            "preserved_task",
            "writer_revision_before",
            "writer_revision_after",
            "writer_state_before",
            "writer_state_before_sha256",
            "writer_state_after",
            "writer_state_after_sha256",
            "batch_pointer_before",
            "batch_pointer_before_sha256",
            "background_rollover_pointer_before",
            "background_rollover_pointer_before_sha256",
            "rollback_token",
            "sol_called",
            "sol_enabled",
            "model_call_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "formal_write_count",
            "created_at",
            "seal",
        }
        if (
            set(receipt) != required
            or receipt.get("schema_version")
            != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
            or receipt.get("mode") != "explicit_failure_resume"
            or receipt.get("subject") != "english"
            or receipt.get("sol_called") is not False
            or receipt.get("sol_enabled") is not False
            or receipt.get("model_call_count") != 0
            or receipt.get("provider_request_count") != 0
            or receipt.get("mcp_tool_call_count") != 0
            or receipt.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_receipt_v2_invalid"
            )
        _subject(receipt.get("subject"))
        for key in (
            "rollover_id",
            "old_batch_sha256",
            "old_batch_snapshot_sha256",
            "archive_sha256",
            "task_set_sha256",
            "evidence_reference_set_sha256",
            "source_authority_fingerprint",
            "next_authority_fingerprint",
            "original_preclaim_failure_receipt_sha256",
            "preserved_queue_entry_sha256",
            "canary_state_before_sha256",
            "writer_state_before_sha256",
            "writer_state_after_sha256",
            "batch_pointer_before_sha256",
            "rollback_token",
        ):
            _sha256(receipt.get(key), key)
        for key in (
            "old_batch_snapshot_path",
            "archive_path",
            "original_preclaim_failure_receipt_path",
        ):
            path = _nonempty(receipt.get(key), key)
            if not Path(path).is_absolute():
                raise SubjectSolContractError(f"{key}_invalid")
        _nonempty(receipt.get("old_batch_id"), "old_batch_id")
        _nonempty(receipt.get("source_generation"), "source_generation")
        _nonempty(receipt.get("next_generation"), "next_generation")
        _integer(receipt.get("old_batch_revision"), "old_batch_revision")
        _integer(receipt.get("task_count"), "task_count", minimum=1)
        before = _integer(
            receipt.get("writer_revision_before"), "writer_revision_before"
        )
        after = _integer(
            receipt.get("writer_revision_after"), "writer_revision_after"
        )
        if after != before + 1:
            raise SubjectSolContractError(
                "subject_background_rollover_writer_revision_invalid"
            )
        status_counts = dict(
            _mapping(receipt.get("terminal_status_counts"), "terminal_status_counts")
        )
        if set(status_counts) != {
            "quality_passed",
            "needs_rework",
            "failed",
            "evidence_pending",
        }:
            raise SubjectSolContractError(
                "subject_background_rollover_terminal_counts_invalid"
            )
        for status, count in status_counts.items():
            _integer(count, f"terminal_status_count_{status}")
        _unique_hashes(
            receipt.get("quality_receipt_sha256s"),
            "quality_receipt_sha256s",
        )
        _unique_hashes(
            receipt.get("terminal_receipt_sha256s"),
            "terminal_receipt_sha256s",
        )
        attempts = _unique_hashes(
            receipt.get("subsequent_attempt_receipt_sha256s"),
            "subsequent_attempt_receipt_sha256s",
        )
        if receipt["original_preclaim_failure_receipt_sha256"] in attempts:
            raise SubjectSolContractError(
                "subject_background_rollover_attempt_binding_invalid"
            )

        queue_identity = dict(
            _mapping(receipt.get("preserved_queue_identity"), "preserved_queue_identity")
        )
        if set(queue_identity) != {
            "activation_id",
            "release_id",
            "producer_unit_id",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "source_event_ids",
            "unit_sha256",
            "frozen_payload_sha256",
            "task_object_sha256",
        }:
            raise SubjectSolContractError(
                "subject_background_rollover_queue_binding_invalid"
            )

        canary_before = dict(
            _mapping(receipt.get("canary_state_before"), "canary_state_before")
        )
        if (
            _value_sha256(canary_before)
            != receipt.get("canary_state_before_sha256")
            or canary_before.get("subject") != "english"
            or canary_before.get("activation_id")
            != queue_identity["activation_id"]
            or canary_before.get("luna_consumer_enabled") is not False
            or canary_before.get("active_task_count") != 0
            or canary_before.get("formal_write_count") != 0
            or canary_before.get("sol_enabled") is not False
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_canary_preimage_invalid"
            )
        for key in (
            "activation_id",
            "release_id",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "unit_sha256",
            "frozen_payload_sha256",
            "task_object_sha256",
        ):
            _sha256(queue_identity.get(key), key)
        _nonempty(queue_identity.get("producer_unit_id"), "producer_unit_id")
        source_event_ids = _sequence(
            queue_identity.get("source_event_ids"),
            "source_event_ids",
            nonempty=True,
        )
        if (
            any(not isinstance(row, str) or not row for row in source_event_ids)
            or len(source_event_ids) != len(set(source_event_ids))
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_queue_binding_invalid"
            )

        preserved_task = dict(
            _mapping(receipt.get("preserved_task"), "preserved_task")
        )
        if set(preserved_task) != {
            "unit_sha256",
            "frozen_payload_sha256",
            "task_object_sha256",
            "task_object_path",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
        }:
            raise SubjectSolContractError(
                "subject_background_rollover_preserved_task_invalid"
            )
        for key in (
            "unit_sha256",
            "frozen_payload_sha256",
            "task_object_sha256",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
        ):
            _sha256(preserved_task.get(key), key)
        preserved_task_path = _nonempty(
            preserved_task.get("task_object_path"), "task_object_path"
        )
        if not Path(preserved_task_path).is_absolute():
            raise SubjectSolContractError(
                "subject_background_rollover_preserved_task_invalid"
            )
        if any(
            preserved_task.get(key) != queue_identity.get(key)
            for key in (
                "unit_sha256",
                "frozen_payload_sha256",
                "task_object_sha256",
                "producer_input_contract_sha256",
                "source_event_set_sha256",
            )
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_preserved_task_invalid"
            )

        writer_after = dict(
            _mapping(receipt.get("writer_state_after"), "writer_state_after")
        )
        writer_before = dict(
            _mapping(receipt.get("writer_state_before"), "writer_state_before")
        )
        self._read_writer_projection(writer_before)
        self._read_writer_projection(writer_after)
        expected_writer_keys = set(self._default_writer("english"))
        if (
            set(writer_after) != expected_writer_keys
            or writer_after.get("subject") != "english"
            or writer_after.get("revision") != after
            or writer_after.get("handoff_status") != "awaiting_luna"
            or writer_after.get("batch_id") is not None
            or writer_after.get("formal_write_count") != 0
            or writer_after.get("generation_fence")
            != {
                "blocked": False,
                "source_generation": receipt["source_generation"],
                "next_generation": receipt["next_generation"],
            }
            or _value_sha256(writer_after)
            != receipt.get("writer_state_after_sha256")
            or _value_sha256(writer_before)
            != receipt.get("writer_state_before_sha256")
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_writer_after_invalid"
            )
        batch_pointer = dict(
            _mapping(receipt.get("batch_pointer_before"), "batch_pointer_before")
        )
        if _value_sha256(batch_pointer) != receipt.get(
            "batch_pointer_before_sha256"
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_batch_pointer_invalid"
            )
        prior_rollover = receipt.get("background_rollover_pointer_before")
        prior_rollover_sha256 = receipt.get(
            "background_rollover_pointer_before_sha256"
        )
        if prior_rollover is None:
            if prior_rollover_sha256 is not None:
                raise SubjectSolContractError(
                    "subject_background_rollover_pointer_preimage_invalid"
                )
        else:
            prior_rollover = dict(
                _mapping(prior_rollover, "background_rollover_pointer_before")
            )
            if (
                _value_sha256(prior_rollover) != prior_rollover_sha256
                or prior_rollover.get("subject") != "english"
            ):
                raise SubjectSolContractError(
                    "subject_background_rollover_pointer_preimage_invalid"
                )
        rollback_core = {
            "rollover_id": receipt["rollover_id"],
            "old_batch_sha256": receipt["old_batch_sha256"],
            "writer_state_before_sha256": receipt[
                "writer_state_before_sha256"
            ],
            "writer_state_after_sha256": receipt[
                "writer_state_after_sha256"
            ],
            "batch_pointer_before_sha256": receipt[
                "batch_pointer_before_sha256"
            ],
            "background_rollover_pointer_before_sha256": (
                prior_rollover_sha256
            ),
            "preserved_queue_entry_sha256": receipt[
                "preserved_queue_entry_sha256"
            ],
            "canary_state_before_sha256": receipt[
                "canary_state_before_sha256"
            ],
        }
        if _value_sha256(rollback_core) != receipt.get("rollback_token"):
            raise SubjectSolContractError(
                "subject_background_rollover_rollback_token_invalid"
            )
        _timestamp(receipt.get("created_at"), "rollover_created_at")
        self._verify_seal(
            receipt, purpose="subject-background-luna-rollover-v2"
        )
        return receipt

    def _validate_cs408_terminal_retirement_receipt(
        self, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt = dict(value)
        required = {
            "schema_version",
            "authorization_descriptor_sha256",
            "retirement_id",
            "mode",
            "subject",
            "old_batch_id",
            "old_batch_sha256",
            "terminal_receipt_sha256",
            "terminal_receipt_path",
            "batch_snapshot_sha256",
            "batch_snapshot_path",
            "batch_pointer_before",
            "batch_pointer_before_sha256",
            "source_generation",
            "source_authority_fingerprint",
            "next_generation",
            "next_authority_fingerprint",
            "archive_sha256",
            "archive_path",
            "writer_revision_before",
            "writer_revision_after",
            "writer_state_before",
            "writer_state_before_sha256",
            "writer_state_after",
            "writer_state_after_sha256",
            "global_sol_state_before",
            "global_sol_state_before_sha256",
            "global_sol_state_after_sha256",
            "rollback_token",
            "queue_entry_created",
            "replacement_task_created",
            "capture_replayed",
            "sol_called",
            "sol_enabled",
            "model_call_count",
            "provider_request_count",
            "authority_snapshot_count",
            "authority_snapshot_mcp_tool_call_count",
            "authority_snapshot_sha256s",
            "model_mcp_tool_call_count",
            "mcp_tool_call_count",
            "formal_write_count",
            "created_at",
            "seal",
        }
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        if (
            set(receipt) != required
            or receipt.get("schema_version")
            != CS408_TERMINAL_BATCH_RETIREMENT_RECEIPT_SCHEMA
            or receipt.get("authorization_descriptor_sha256")
            != _value_sha256(authorization)
            or receipt.get("mode") != authorization["mode"]
            or receipt.get("subject") != "cs408"
            or receipt.get("old_batch_id") != authorization["batch_id"]
            or receipt.get("old_batch_sha256")
            != authorization["batch_sha256"]
            or receipt.get("terminal_receipt_sha256")
            != authorization["terminal_receipt_sha256"]
            or receipt.get("batch_snapshot_sha256")
            != authorization["snapshot_sha256"]
            or receipt.get("batch_pointer_before_sha256")
            != authorization["batch_pointer_sha256"]
            or receipt.get("writer_state_before_sha256")
            != authorization["writer_preimage_sha256"]
            or receipt.get("source_generation")
            != authorization["source_generation"]
            or receipt.get("source_authority_fingerprint")
            != authorization["source_authority_fingerprint"]
            or receipt.get("next_generation")
            != authorization["target_generation"]
            or receipt.get("next_authority_fingerprint")
            != authorization["target_authority_fingerprint"]
            or any(
                receipt.get(key) is not False
                for key in (
                    "queue_entry_created",
                    "replacement_task_created",
                    "capture_replayed",
                    "sol_called",
                    "sol_enabled",
                )
            )
            or any(
                receipt.get(key) != 0
                for key in (
                    "model_call_count",
                    "provider_request_count",
                    "model_mcp_tool_call_count",
                    "formal_write_count",
                )
            )
            or receipt.get("authority_snapshot_count") != 2
            or receipt.get("authority_snapshot_mcp_tool_call_count") != 2
            or receipt.get("mcp_tool_call_count") != 2
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_receipt_invalid"
            )
        for key in (
            "retirement_id",
            "authorization_descriptor_sha256",
            "old_batch_sha256",
            "terminal_receipt_sha256",
            "batch_snapshot_sha256",
            "batch_pointer_before_sha256",
            "source_authority_fingerprint",
            "next_authority_fingerprint",
            "archive_sha256",
            "writer_state_before_sha256",
            "writer_state_after_sha256",
            "global_sol_state_before_sha256",
            "global_sol_state_after_sha256",
            "rollback_token",
        ):
            _sha256(receipt.get(key), key)
        authority_snapshot_sha256s = list(
            _sequence(
                receipt.get("authority_snapshot_sha256s"),
                "authority_snapshot_sha256s",
                nonempty=True,
            )
        )
        if (
            len(authority_snapshot_sha256s) != 2
            or any(
                not isinstance(row, str) or SHA256_RE.fullmatch(row) is None
                for row in authority_snapshot_sha256s
            )
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_authority_observations_invalid"
            )
        for key in (
            "terminal_receipt_path",
            "batch_snapshot_path",
            "archive_path",
        ):
            if not Path(_nonempty(receipt.get(key), key)).is_absolute():
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_path_invalid"
                )
        before = dict(
            _mapping(receipt.get("writer_state_before"), "writer_state_before")
        )
        after = dict(
            _mapping(receipt.get("writer_state_after"), "writer_state_after")
        )
        pointer = dict(
            _mapping(receipt.get("batch_pointer_before"), "batch_pointer_before")
        )
        self._read_writer_projection(before)
        self._read_writer_projection(after)
        global_before = dict(
            _mapping(
                receipt.get("global_sol_state_before"),
                "global_sol_state_before",
            )
        )
        self._validate_global(global_before)
        before_revision = _integer(
            receipt.get("writer_revision_before"), "writer_revision_before"
        )
        after_revision = _integer(
            receipt.get("writer_revision_after"), "writer_revision_after"
        )
        expected_after = copy.deepcopy(before)
        expected_after.update(
            {
                "revision": after_revision,
                "batch_id": None,
                "generation_fence": {
                    "blocked": False,
                    "source_generation": authorization["source_generation"],
                    "next_generation": authorization["target_generation"],
                },
                "updated_at": after.get("updated_at"),
            }
        )
        if (
            after_revision != before_revision + 1
            or before.get("revision") != before_revision
            or after.get("revision") != after_revision
            or _document_sha256(before)
            != receipt["writer_state_before_sha256"]
            or _document_sha256(after) != receipt["writer_state_after_sha256"]
            or before.get("batch_id") != authorization["batch_id"]
            or after.get("batch_id") is not None
            or after.get("handoff_status") != "awaiting_luna"
            or after.get("formal_write_count") != 0
            or after != expected_after
            or after.get("generation_fence")
            != {
                "blocked": False,
                "source_generation": authorization["source_generation"],
                "next_generation": authorization["target_generation"],
            }
            or any(
                after.get(key) is not None
                for key in (
                    "authorization_receipt_sha256",
                    "daily_sol_batch_sha256",
                    "review_receipt_sha256",
                    "commit_receipt_sha256",
                )
            )
            or _document_sha256(pointer)
            != receipt["batch_pointer_before_sha256"]
            or pointer.get("batch_id") != authorization["batch_id"]
            or pointer.get("batch_sha256") != authorization["batch_sha256"]
            or pointer.get("snapshot_sha256")
            != authorization["snapshot_sha256"]
            or _document_sha256(global_before)
            != receipt["global_sol_state_before_sha256"]
            or receipt["global_sol_state_after_sha256"]
            != receipt["global_sol_state_before_sha256"]
            or global_before.get("active_writer") is not None
            or bool(global_before.get("queue"))
            or global_before.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_preimage_invalid"
            )
        rollback_core = {
            "retirement_id": receipt["retirement_id"],
            "old_batch_sha256": receipt["old_batch_sha256"],
            "terminal_receipt_sha256": receipt["terminal_receipt_sha256"],
            "writer_state_before_sha256": receipt[
                "writer_state_before_sha256"
            ],
            "writer_state_after_sha256": receipt["writer_state_after_sha256"],
            "batch_pointer_before_sha256": receipt[
                "batch_pointer_before_sha256"
            ],
            "batch_snapshot_sha256": receipt["batch_snapshot_sha256"],
            "global_sol_state_before_sha256": receipt[
                "global_sol_state_before_sha256"
            ],
        }
        if _value_sha256(rollback_core) != receipt["rollback_token"]:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_token_invalid"
            )
        _timestamp(receipt.get("created_at"), "retirement_created_at")
        self._verify_seal(
            receipt, purpose="cs408-terminal-batch-writer-retirement-v1"
        )
        return receipt

    def _verify_cs408_authorized_historical_evidence_locked(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Reopen every immutable and canonical byte in the one authorization."""

        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        batch = self._read_batch_locked("cs408")
        pointer = self._read_json(
            self._batch_pointer_path("cs408"),
            "subject_luna_batch_pointer",
        )
        if (
            batch is None
            or pointer is None
            or _document_sha256(batch) != authorization["batch_sha256"]
            or _document_sha256(pointer)
            != authorization["batch_pointer_sha256"]
            or pointer.get("snapshot_sha256")
            != authorization["snapshot_sha256"]
            or batch.get("authority_generation")
            != authorization["source_generation"]
            or batch.get("authority_fingerprint")
            != authorization["source_authority_fingerprint"]
            or len(batch.get("tasks") or []) != 1
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_historical_evidence_drift"
            )
        task = batch["tasks"][0]
        if task.get("terminal_receipt_sha256") != authorization[
            "terminal_receipt_sha256"
        ]:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_historical_evidence_drift"
            )
        self._verify_excluded_task_evidence(task, original_batch=batch)
        snapshot = self._read_content_addressed(
            self.subject_batch_snapshot_root,
            authorization["snapshot_sha256"],
            "cs408_terminal_retirement_snapshot",
        )
        self._verify_seal(snapshot, purpose="subject-luna-batch-snapshot")
        if (
            snapshot.get("batch") != batch
            or snapshot.get("batch_sha256") != authorization["batch_sha256"]
            or snapshot.get("batch_id") != authorization["batch_id"]
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_historical_evidence_drift"
            )
        return batch, pointer

    def _read_cs408_terminal_retirement_receipt(
        self, receipt_sha256: str
    ) -> dict[str, Any]:
        receipt = self._validate_cs408_terminal_retirement_receipt(
            self._read_content_addressed(
                self.cs408_terminal_retirement_receipt_root,
                receipt_sha256,
                "cs408_terminal_retirement_receipt",
            )
        )
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        terminal_path = (
            self.receipt_root
            / "subject-terminal"
            / "sha256"
            / authorization["terminal_receipt_sha256"][:2]
            / f"{authorization['terminal_receipt_sha256']}.json"
        )
        snapshot_path = (
            self.subject_batch_snapshot_root
            / "sha256"
            / authorization["snapshot_sha256"][:2]
            / f"{authorization['snapshot_sha256']}.json"
        )
        archive_path = (
            self.cs408_terminal_retirement_archive_root
            / "sha256"
            / str(receipt["archive_sha256"])[:2]
            / f"{receipt['archive_sha256']}.json"
        )
        if (
            receipt["terminal_receipt_path"] != str(terminal_path)
            or receipt["batch_snapshot_path"] != str(snapshot_path)
            or receipt["archive_path"] != str(archive_path)
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_evidence_path_invalid"
            )
        snapshot = self._read_content_addressed(
            self.subject_batch_snapshot_root,
            authorization["snapshot_sha256"],
            "cs408_terminal_retirement_snapshot",
        )
        self._verify_seal(snapshot, purpose="subject-luna-batch-snapshot")
        batch = dict(_mapping(snapshot.get("batch"), "retirement_snapshot_batch"))
        self._validate_subject_batch(batch)
        pointer = dict(receipt["batch_pointer_before"])
        if (
            snapshot.get("schema_version") != "subject_luna_batch_snapshot_v1"
            or snapshot.get("subject") != "cs408"
            or snapshot.get("batch_id") != authorization["batch_id"]
            or snapshot.get("batch_sha256") != authorization["batch_sha256"]
            or batch.get("batch_id") != authorization["batch_id"]
            or _document_sha256(batch) != authorization["batch_sha256"]
            or pointer.get("snapshot_sha256")
            != authorization["snapshot_sha256"]
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_snapshot_binding_invalid"
            )
        if len(batch.get("tasks") or []) != 1:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_task_set_invalid"
            )
        task = batch["tasks"][0]
        if (
            task.get("terminal_receipt_sha256")
            != authorization["terminal_receipt_sha256"]
            or task.get("status") != "failed"
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_terminal_binding_invalid"
            )
        self._verify_excluded_task_evidence(task, original_batch=batch)
        archive = self._read_content_addressed(
            self.cs408_terminal_retirement_archive_root,
            str(receipt["archive_sha256"]),
            "cs408_terminal_retirement_archive",
        )
        self._verify_seal(
            archive, purpose="cs408-terminal-batch-retirement-archive-v1"
        )
        archive_required = {
            "schema_version",
            "authorization_descriptor_sha256",
            "subject",
            "mode",
            "batch",
            "batch_sha256",
            "batch_pointer",
            "batch_pointer_sha256",
            "batch_snapshot_sha256",
            "terminal_receipt_sha256",
            "source_generation",
            "source_authority_fingerprint",
            "archived_at",
            "queue_entry_created",
            "replacement_task_created",
            "capture_replayed",
            "sol_enabled",
            "formal_write_count",
            "seal",
        }
        if (
            set(archive) != archive_required
            or archive.get("schema_version")
            != "cs408_terminal_batch_archive_v1"
            or archive.get("authorization_descriptor_sha256")
            != _value_sha256(authorization)
            or archive.get("subject") != "cs408"
            or archive.get("mode") != authorization["mode"]
            or archive.get("batch") != batch
            or archive.get("batch_sha256") != authorization["batch_sha256"]
            or archive.get("batch_pointer") != pointer
            or archive.get("batch_pointer_sha256")
            != authorization["batch_pointer_sha256"]
            or archive.get("batch_snapshot_sha256")
            != authorization["snapshot_sha256"]
            or archive.get("terminal_receipt_sha256")
            != authorization["terminal_receipt_sha256"]
            or archive.get("source_generation")
            != authorization["source_generation"]
            or archive.get("source_authority_fingerprint")
            != authorization["source_authority_fingerprint"]
            or archive.get("queue_entry_created") is not False
            or archive.get("replacement_task_created") is not False
            or archive.get("capture_replayed") is not False
            or archive.get("sol_enabled") is not False
            or archive.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_archive_binding_invalid"
            )
        _timestamp(archive.get("archived_at"), "retirement_archived_at")
        return receipt

    def _read_cs408_terminal_retirement_pointer_locked(
        self,
    ) -> dict[str, Any] | None:
        pointer = self._read_json(
            self.cs408_terminal_retirement_pointer_path,
            "cs408_terminal_retirement_pointer",
        )
        if pointer is None:
            return None
        required = {
            "schema_version",
            "subject",
            "retirement_id",
            "old_batch_id",
            "old_batch_sha256",
            "retirement_receipt_sha256",
            "retirement_receipt_path",
            "writer_revision_after",
            "writer_state_after_sha256",
            "next_generation",
            "next_authority_fingerprint",
            "applied_at",
            "formal_write_count",
            "seal",
        }
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        if (
            set(pointer) != required
            or pointer.get("schema_version")
            != CS408_TERMINAL_BATCH_RETIREMENT_POINTER_SCHEMA
            or pointer.get("subject") != "cs408"
            or pointer.get("old_batch_id") != authorization["batch_id"]
            or pointer.get("old_batch_sha256")
            != authorization["batch_sha256"]
            or pointer.get("next_generation")
            != authorization["target_generation"]
            or pointer.get("next_authority_fingerprint")
            != authorization["target_authority_fingerprint"]
            or pointer.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_pointer_invalid"
            )
        for key in (
            "retirement_id",
            "old_batch_sha256",
            "retirement_receipt_sha256",
            "writer_state_after_sha256",
            "next_authority_fingerprint",
        ):
            _sha256(pointer.get(key), key)
        _integer(pointer.get("writer_revision_after"), "writer_revision_after")
        _timestamp(pointer.get("applied_at"), "retirement_applied_at")
        if not Path(
            _nonempty(
                pointer.get("retirement_receipt_path"),
                "retirement_receipt_path",
            )
        ).is_absolute():
            raise SubjectSolContractError(
                "cs408_terminal_retirement_pointer_invalid"
            )
        expected_receipt_path = (
            self.cs408_terminal_retirement_receipt_root
            / "sha256"
            / str(pointer["retirement_receipt_sha256"])[:2]
            / f"{pointer['retirement_receipt_sha256']}.json"
        )
        if pointer["retirement_receipt_path"] != str(expected_receipt_path):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_pointer_invalid"
            )
        self._verify_seal(
            pointer, purpose="cs408-terminal-batch-writer-retirement-pointer-v1"
        )
        receipt = self._read_cs408_terminal_retirement_receipt(
            str(pointer["retirement_receipt_sha256"])
        )
        if (
            receipt.get("retirement_id") != pointer["retirement_id"]
            or receipt.get("old_batch_sha256") != pointer["old_batch_sha256"]
            or receipt.get("writer_revision_after")
            != pointer["writer_revision_after"]
            or receipt.get("writer_state_after_sha256")
            != pointer["writer_state_after_sha256"]
            or receipt.get("next_generation") != pointer["next_generation"]
            or receipt.get("next_authority_fingerprint")
            != pointer["next_authority_fingerprint"]
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_pointer_binding_invalid"
            )
        return pointer

    @staticmethod
    def _cs408_retirement_postimage_verification_sha256(
        *, pointer: Mapping[str, Any], receipt_sha256: str
    ) -> str:
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        return _value_sha256(
            {
                "authorization_descriptor_sha256": _value_sha256(
                    authorization
                ),
                "retirement_receipt_sha256": receipt_sha256,
                "writer_postimage_sha256": pointer[
                    "writer_state_after_sha256"
                ],
                "old_batch_sha256": authorization["batch_sha256"],
                "batch_pointer_sha256": authorization[
                    "batch_pointer_sha256"
                ],
                "snapshot_sha256": authorization["snapshot_sha256"],
                "terminal_receipt_sha256": authorization[
                    "terminal_receipt_sha256"
                ],
            }
        )

    def _cs408_retirement_result_locked(
        self,
        *,
        pointer: Mapping[str, Any],
        receipt: Mapping[str, Any],
        idempotent: bool,
    ) -> dict[str, Any]:
        receipt_sha256 = str(pointer["retirement_receipt_sha256"])
        writer = self._read_writer_locked("cs408")
        if (
            _document_sha256(writer) != receipt["writer_state_after_sha256"]
            or writer != receipt["writer_state_after"]
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_writer_postimage_invalid"
            )
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        postimage_sha256 = (
            self._cs408_retirement_postimage_verification_sha256(
                pointer=pointer, receipt_sha256=receipt_sha256
            )
        )
        return {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-result-v1"
            ),
            "subject": "cs408",
            "status": "retired",
            "mode": authorization["mode"],
            "authorization_descriptor_sha256": _value_sha256(authorization),
            "batch_id": authorization["batch_id"],
            "batch_sha256": authorization["batch_sha256"],
            "terminal_receipt_sha256": authorization[
                "terminal_receipt_sha256"
            ],
            "writer_preimage_sha256": authorization[
                "writer_preimage_sha256"
            ],
            "batch_pointer_sha256": authorization["batch_pointer_sha256"],
            "snapshot_sha256": authorization["snapshot_sha256"],
            "source_generation": authorization["source_generation"],
            "source_authority_fingerprint": authorization[
                "source_authority_fingerprint"
            ],
            "next_generation": authorization["target_generation"],
            "next_authority_fingerprint": authorization["target_authority_fingerprint"],
            "retirement_receipt_sha256": receipt_sha256,
            "retirement_receipt_path": pointer["retirement_receipt_path"],
            "rollback_token": receipt["rollback_token"],
            "writer_postimage_sha256": receipt[
                "writer_state_after_sha256"
            ],
            "postimage_verification_sha256": postimage_sha256,
            "authority_snapshot_sha256s": list(
                receipt["authority_snapshot_sha256s"]
            ),
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "queue_entry_created_count": 0,
            "replacement_task_created_count": 0,
            "capture_replay_count": 0,
            "idempotent": idempotent,
        }

    def _publish_cs408_retirement_rollback_locked(
        self,
        *,
        retirement_receipt: Mapping[str, Any],
        retirement_receipt_sha256: str,
        retirement_receipt_path: str,
        idempotent: bool,
    ) -> dict[str, Any]:
        before = dict(retirement_receipt["writer_state_before"])
        current_writer = self._read_writer_locked("cs408")
        current_sha256 = _document_sha256(current_writer)
        if current_sha256 not in {
            retirement_receipt["writer_state_before_sha256"],
            retirement_receipt["writer_state_after_sha256"],
        }:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_writer_drift"
            )
        pointer = self._read_json(
            self.cs408_terminal_retirement_pointer_path,
            "cs408_terminal_retirement_pointer",
        )
        if pointer is not None:
            checked_pointer = (
                self._read_cs408_terminal_retirement_pointer_locked()
            )
            if checked_pointer is None or checked_pointer.get(
                "retirement_receipt_sha256"
            ) != retirement_receipt_sha256:
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_rollback_pointer_drift"
                )
        if current_sha256 == retirement_receipt["writer_state_after_sha256"]:
            self._atomic_json(self._writer_path("cs408"), before)
        try:
            self.cs408_terminal_retirement_pointer_path.unlink()
        except FileNotFoundError:
            pass
        try:
            self.cs408_terminal_retirement_intent_path.unlink()
        except FileNotFoundError:
            pass
        if (
            self.cs408_terminal_retirement_pointer_path.exists()
            or self.cs408_terminal_retirement_pointer_path.is_symlink()
            or self.cs408_terminal_retirement_intent_path.exists()
            or self.cs408_terminal_retirement_intent_path.is_symlink()
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_withdrawal_failed"
            )
        restored = self._read_writer_locked("cs408")
        if _document_sha256(restored) != retirement_receipt[
            "writer_state_before_sha256"
        ]:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_restore_failed"
            )
        batch = self._read_batch_locked("cs408")
        old_pointer = self._read_json(
            self._batch_pointer_path("cs408"),
            "subject_luna_batch_pointer",
        )
        if (
            batch is None
            or _document_sha256(batch)
            != retirement_receipt["old_batch_sha256"]
            or old_pointer is None
            or _document_sha256(old_pointer)
            != retirement_receipt["batch_pointer_before_sha256"]
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_evidence_drift"
            )
        rollback_postimage_sha256 = _value_sha256(
            {
                "retirement_receipt_sha256": retirement_receipt_sha256,
                "writer_state_restored_sha256": retirement_receipt[
                    "writer_state_before_sha256"
                ],
                "old_batch_sha256": retirement_receipt["old_batch_sha256"],
                "batch_pointer_unchanged_sha256": retirement_receipt[
                    "batch_pointer_before_sha256"
                ],
                "batch_snapshot_unchanged_sha256": retirement_receipt[
                    "batch_snapshot_sha256"
                ],
                "terminal_receipt_unchanged_sha256": retirement_receipt[
                    "terminal_receipt_sha256"
                ],
                "retirement_pointer_withdrawn": True,
                "retirement_intent_withdrawn": True,
            }
        )
        rollback = self._seal(
            {
                "schema_version": (
                    CS408_TERMINAL_BATCH_RETIREMENT_ROLLBACK_RECEIPT_SCHEMA
                ),
                "subject": "cs408",
                "retirement_receipt_sha256": retirement_receipt_sha256,
                "retirement_receipt_path": retirement_receipt_path,
                "authorization_descriptor_sha256": retirement_receipt[
                    "authorization_descriptor_sha256"
                ],
                "rollback_token": retirement_receipt["rollback_token"],
                "writer_state_after_sha256": retirement_receipt[
                    "writer_state_after_sha256"
                ],
                "writer_state_restored_sha256": retirement_receipt[
                    "writer_state_before_sha256"
                ],
                "batch_pointer_unchanged_sha256": retirement_receipt[
                    "batch_pointer_before_sha256"
                ],
                "batch_snapshot_unchanged_sha256": retirement_receipt[
                    "batch_snapshot_sha256"
                ],
                "terminal_receipt_unchanged_sha256": retirement_receipt[
                    "terminal_receipt_sha256"
                ],
                "retirement_pointer_withdrawn": True,
                "retirement_intent_withdrawn": True,
                "rollback_postimage_verification_sha256": (
                    rollback_postimage_sha256
                ),
                "queue_entry_created_count": 0,
                "replacement_task_created_count": 0,
                "capture_replay_count": 0,
                "authority_snapshot_count": 0,
                "authority_snapshot_mcp_tool_call_count": 0,
                "model_mcp_tool_call_count": 0,
                "mcp_tool_call_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "rolled_back_at": _utc_now(),
            },
            purpose="cs408-terminal-batch-writer-retirement-rollback-v1",
        )
        rollback_sha256, rollback_path = self._publish_immutable(
            self.cs408_terminal_retirement_rollback_receipt_root, rollback
        )
        reopened = self._read_content_addressed(
            self.cs408_terminal_retirement_rollback_receipt_root,
            rollback_sha256,
            "cs408_terminal_retirement_rollback_receipt",
        )
        self._verify_seal(
            reopened,
            purpose="cs408-terminal-batch-writer-retirement-rollback-v1",
        )
        if reopened != rollback:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_receipt_invalid"
            )
        return {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-rollback-result-v1"
            ),
            "subject": "cs408",
            "status": "rolled_back",
            "retirement_receipt_sha256": retirement_receipt_sha256,
            "rollback_receipt_sha256": rollback_sha256,
            "rollback_receipt_path": str(rollback_path),
            "rollback_token": retirement_receipt["rollback_token"],
            "writer_state_restored_sha256": retirement_receipt[
                "writer_state_before_sha256"
            ],
            "old_batch_sha256": retirement_receipt["old_batch_sha256"],
            "batch_pointer_unchanged_sha256": retirement_receipt[
                "batch_pointer_before_sha256"
            ],
            "batch_snapshot_unchanged_sha256": retirement_receipt[
                "batch_snapshot_sha256"
            ],
            "terminal_receipt_unchanged_sha256": retirement_receipt[
                "terminal_receipt_sha256"
            ],
            "retirement_pointer_withdrawn": True,
            "retirement_intent_withdrawn": True,
            "rollback_postimage_verification_sha256": (
                rollback_postimage_sha256
            ),
            "rollback_receipt_reopened": True,
            "idempotent": idempotent,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def retire_authorized_cs408_terminal_batch(
        self,
        *,
        expected_batch_sha256: str,
        expected_terminal_receipt_sha256: str,
        expected_writer_preimage_sha256: str,
        expected_batch_pointer_sha256: str,
        expected_snapshot_sha256: str,
        expected_source_generation: str,
        expected_source_authority_fingerprint: str,
        expected_next_generation: str,
        expected_next_authority_fingerprint: str,
        authority_snapshots: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Retire the one authorized 408 terminal writer, with zero replay."""

        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        provided = {
            "batch_sha256": expected_batch_sha256,
            "terminal_receipt_sha256": expected_terminal_receipt_sha256,
            "writer_preimage_sha256": expected_writer_preimage_sha256,
            "batch_pointer_sha256": expected_batch_pointer_sha256,
            "snapshot_sha256": expected_snapshot_sha256,
            "source_generation": expected_source_generation,
            "source_authority_fingerprint": (
                expected_source_authority_fingerprint
            ),
            "target_generation": expected_next_generation,
            "target_authority_fingerprint": (
                expected_next_authority_fingerprint
            ),
        }
        if any(provided[key] != authorization[key] for key in provided):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_authorization_mismatch"
            )
        observations = [copy.deepcopy(dict(row)) for row in authority_snapshots]
        if len(observations) != 2:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_authority_observations_invalid"
            )
        observation_sha256s: list[str] = []
        for observation in observations:
            if (
                observation.get("schema_version")
                != "subject_authority_snapshot_v1"
                or observation.get("subject") != "cs408"
                or observation.get("generation")
                != authorization["target_generation"]
                or observation.get("authority_fingerprint")
                != authorization["target_authority_fingerprint"]
                or observation.get("model_call_count") != 0
                or observation.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_authority_mismatch"
                )
            observation_sha256s.append(_value_sha256(observation))

        receipt_sha256: str | None = None
        receipt_path: str | None = None
        with _FileLock(self.global_lock_path):
            global_before = self._read_global_locked()
            if (
                global_before.get("active_writer") is not None
                or bool(global_before.get("queue"))
                or global_before.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_sol_state_present"
                )
            with _FileLock(self._subject_lock_path("cs408")):
                batch = self._read_batch_locked("cs408")
                pointer = self._read_json(
                    self._batch_pointer_path("cs408"),
                    "subject_luna_batch_pointer",
                )
                writer = self._read_writer_locked("cs408")
                self._verify_cs408_authorized_historical_evidence_locked()
                existing_pointer = (
                    self._read_cs408_terminal_retirement_pointer_locked()
                )
                if existing_pointer is not None:
                    existing_receipt = (
                        self._read_cs408_terminal_retirement_receipt(
                            str(
                                existing_pointer[
                                    "retirement_receipt_sha256"
                                ]
                            )
                        )
                    )
                    if existing_receipt.get(
                        "authority_snapshot_sha256s"
                    ) != observation_sha256s:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_idempotency_conflict"
                        )
                    return self._cs408_retirement_result_locked(
                        pointer=existing_pointer,
                        receipt=existing_receipt,
                        idempotent=True,
                    )
                if batch is None or pointer is None:
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_preimage_missing"
                    )
                batch_sha256 = _document_sha256(batch)
                pointer_sha256 = _document_sha256(pointer)
                writer_sha256 = _document_sha256(writer)
                if (
                    batch_sha256 != authorization["batch_sha256"]
                    or pointer_sha256
                    != authorization["batch_pointer_sha256"]
                    or writer_sha256
                    != authorization["writer_preimage_sha256"]
                    or batch.get("batch_id") != authorization["batch_id"]
                    or batch.get("authority_generation")
                    != authorization["source_generation"]
                    or batch.get("authority_fingerprint")
                    != authorization["source_authority_fingerprint"]
                    or batch.get("status") != "frozen"
                    or batch.get("all_terminal") is not True
                    or batch.get("sol_ready") is not False
                    or len(batch.get("tasks") or []) != 1
                    or writer.get("batch_id") != authorization["batch_id"]
                    or writer.get("handoff_status") != "awaiting_luna"
                    or writer.get("formal_write_count") != 0
                ):
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_preimage_mismatch"
                    )
                task = batch["tasks"][0]
                if (
                    task.get("terminal_receipt_sha256")
                    != authorization["terminal_receipt_sha256"]
                    or task.get("status") != "failed"
                ):
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_terminal_binding_invalid"
                    )
                self._verify_excluded_task_evidence(
                    task, original_batch=batch
                )
                snapshot_sha256 = _sha256(
                    pointer.get("snapshot_sha256"), "snapshot_sha256"
                )
                if snapshot_sha256 != authorization["snapshot_sha256"]:
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_snapshot_mismatch"
                    )
                snapshot = self._read_content_addressed(
                    self.subject_batch_snapshot_root,
                    snapshot_sha256,
                    "cs408_terminal_retirement_snapshot",
                )
                if snapshot.get("batch") != batch:
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_snapshot_binding_invalid"
                    )
                self._verify_seal(
                    snapshot, purpose="subject-luna-batch-snapshot"
                )
                descriptor_sha256 = _value_sha256(authorization)
                archive = self._seal(
                    {
                        "schema_version": "cs408_terminal_batch_archive_v1",
                        "authorization_descriptor_sha256": descriptor_sha256,
                        "subject": "cs408",
                        "mode": authorization["mode"],
                        "batch": batch,
                        "batch_sha256": batch_sha256,
                        "batch_pointer": pointer,
                        "batch_pointer_sha256": pointer_sha256,
                        "batch_snapshot_sha256": snapshot_sha256,
                        "terminal_receipt_sha256": authorization[
                            "terminal_receipt_sha256"
                        ],
                        "source_generation": authorization[
                            "source_generation"
                        ],
                        "source_authority_fingerprint": authorization[
                            "source_authority_fingerprint"
                        ],
                        "archived_at": _utc_now(),
                        "queue_entry_created": False,
                        "replacement_task_created": False,
                        "capture_replayed": False,
                        "sol_enabled": False,
                        "formal_write_count": 0,
                    },
                    purpose="cs408-terminal-batch-retirement-archive-v1",
                )
                archive_sha256, archive_path = self._publish_immutable(
                    self.cs408_terminal_retirement_archive_root, archive
                )
                transition_at = _utc_now()
                writer_after = copy.deepcopy(writer)
                writer_after.update(
                    {
                        "revision": writer["revision"] + 1,
                        "batch_id": None,
                        "generation_fence": {
                            "blocked": False,
                            "source_generation": authorization[
                                "source_generation"
                            ],
                            "next_generation": authorization["target_generation"],
                        },
                        "updated_at": transition_at,
                    }
                )
                writer_after = self._read_writer_projection(writer_after)
                terminal_path = (
                    self.receipt_root
                    / "subject-terminal"
                    / "sha256"
                    / authorization["terminal_receipt_sha256"][:2]
                    / f"{authorization['terminal_receipt_sha256']}.json"
                )
                snapshot_path = (
                    self.subject_batch_snapshot_root
                    / "sha256"
                    / snapshot_sha256[:2]
                    / f"{snapshot_sha256}.json"
                )
                retirement_identity = {
                    "authorization_descriptor_sha256": descriptor_sha256,
                    "authority_snapshot_sha256s": observation_sha256s,
                    "writer_state_after_sha256": _document_sha256(
                        writer_after
                    ),
                }
                retirement_id = _value_sha256(retirement_identity)
                rollback_core = {
                    "retirement_id": retirement_id,
                    "old_batch_sha256": batch_sha256,
                    "terminal_receipt_sha256": authorization[
                        "terminal_receipt_sha256"
                    ],
                    "writer_state_before_sha256": writer_sha256,
                    "writer_state_after_sha256": _document_sha256(
                        writer_after
                    ),
                    "batch_pointer_before_sha256": pointer_sha256,
                    "batch_snapshot_sha256": snapshot_sha256,
                    "global_sol_state_before_sha256": _document_sha256(
                        global_before
                    ),
                }
                intent = self._read_json(
                    self.cs408_terminal_retirement_intent_path,
                    "cs408_terminal_retirement_intent",
                )
                if intent is None:
                    intent = self._seal(
                        {
                            "schema_version": (
                                CS408_TERMINAL_BATCH_RETIREMENT_RECEIPT_SCHEMA
                            ),
                            "authorization_descriptor_sha256": descriptor_sha256,
                            "retirement_id": retirement_id,
                            "mode": authorization["mode"],
                            "subject": "cs408",
                            "old_batch_id": authorization["batch_id"],
                            "old_batch_sha256": batch_sha256,
                            "terminal_receipt_sha256": authorization[
                                "terminal_receipt_sha256"
                            ],
                            "terminal_receipt_path": str(terminal_path),
                            "batch_snapshot_sha256": snapshot_sha256,
                            "batch_snapshot_path": str(snapshot_path),
                            "batch_pointer_before": pointer,
                            "batch_pointer_before_sha256": pointer_sha256,
                            "source_generation": authorization[
                                "source_generation"
                            ],
                            "source_authority_fingerprint": authorization[
                                "source_authority_fingerprint"
                            ],
                            "next_generation": authorization["target_generation"],
                            "next_authority_fingerprint": authorization["target_authority_fingerprint"],
                            "archive_sha256": archive_sha256,
                            "archive_path": str(archive_path),
                            "writer_revision_before": writer["revision"],
                            "writer_revision_after": writer_after["revision"],
                            "writer_state_before": writer,
                            "writer_state_before_sha256": writer_sha256,
                            "writer_state_after": writer_after,
                            "writer_state_after_sha256": _document_sha256(
                                writer_after
                            ),
                            "global_sol_state_before": global_before,
                            "global_sol_state_before_sha256": _document_sha256(
                                global_before
                            ),
                            "global_sol_state_after_sha256": _document_sha256(
                                global_before
                            ),
                            "rollback_token": _value_sha256(rollback_core),
                            "queue_entry_created": False,
                            "replacement_task_created": False,
                            "capture_replayed": False,
                            "sol_called": False,
                            "sol_enabled": False,
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "authority_snapshot_count": 2,
                            "authority_snapshot_mcp_tool_call_count": 2,
                            "authority_snapshot_sha256s": observation_sha256s,
                            "model_mcp_tool_call_count": 0,
                            "mcp_tool_call_count": 2,
                            "formal_write_count": 0,
                            "created_at": transition_at,
                        },
                        purpose="cs408-terminal-batch-writer-retirement-v1",
                    )
                    self._validate_cs408_terminal_retirement_receipt(intent)
                    self._atomic_json(
                        self.cs408_terminal_retirement_intent_path, intent
                    )
                else:
                    intent = self._validate_cs408_terminal_retirement_receipt(
                        intent
                    )
                    if intent.get("retirement_id") != retirement_id:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_intent_conflict"
                        )
                receipt_sha256, published_path = self._publish_immutable(
                    self.cs408_terminal_retirement_receipt_root, intent
                )
                receipt_path = str(published_path)
                try:
                    self._atomic_json(
                        self._writer_path("cs408"),
                        intent["writer_state_after"],
                    )
                    if _document_sha256(
                        self._read_writer_locked("cs408")
                    ) != intent["writer_state_after_sha256"]:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_writer_postimage_invalid"
                        )
                    pointer_after = self._seal(
                        {
                            "schema_version": (
                                CS408_TERMINAL_BATCH_RETIREMENT_POINTER_SCHEMA
                            ),
                            "subject": "cs408",
                            "retirement_id": retirement_id,
                            "old_batch_id": authorization["batch_id"],
                            "old_batch_sha256": batch_sha256,
                            "retirement_receipt_sha256": receipt_sha256,
                            "retirement_receipt_path": receipt_path,
                            "writer_revision_after": writer_after["revision"],
                            "writer_state_after_sha256": intent[
                                "writer_state_after_sha256"
                            ],
                            "next_generation": authorization["target_generation"],
                            "next_authority_fingerprint": authorization["target_authority_fingerprint"],
                            "applied_at": _utc_now(),
                            "formal_write_count": 0,
                        },
                        purpose=(
                            "cs408-terminal-batch-writer-retirement-pointer-v1"
                        ),
                    )
                    self._atomic_json(
                        self.cs408_terminal_retirement_pointer_path,
                        pointer_after,
                    )
                    checked_pointer = (
                        self._read_cs408_terminal_retirement_pointer_locked()
                    )
                    if checked_pointer is None:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_pointer_missing"
                        )
                    global_after = self._read_global_locked()
                    if _document_sha256(global_after) != intent[
                        "global_sol_state_after_sha256"
                    ]:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_global_drift"
                        )
                    return self._cs408_retirement_result_locked(
                        pointer=checked_pointer,
                        receipt=intent,
                        idempotent=False,
                    )
                except Exception:
                    if receipt_sha256 is None or receipt_path is None:
                        raise
                    self._publish_cs408_retirement_rollback_locked(
                        retirement_receipt=intent,
                        retirement_receipt_sha256=receipt_sha256,
                        retirement_receipt_path=receipt_path,
                        idempotent=False,
                    )
                    raise

    def rollback_authorized_cs408_terminal_batch_retirement(
        self, *, retirement_receipt_path: Path
    ) -> dict[str, Any]:
        path = retirement_receipt_path.resolve()
        try:
            path.relative_to(
                self.cs408_terminal_retirement_receipt_root.resolve()
            )
            payload = path.read_bytes()
        except (OSError, ValueError) as exc:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_receipt_path_invalid"
            ) from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        receipt = self._read_cs408_terminal_retirement_receipt(
            receipt_sha256
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            if (
                global_state.get("active_writer") is not None
                or bool(global_state.get("queue"))
                or global_state.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_rollback_sol_state_present"
                )
            with _FileLock(self._subject_lock_path("cs408")):
                self._verify_cs408_authorized_historical_evidence_locked()
                return self._publish_cs408_retirement_rollback_locked(
                    retirement_receipt=receipt,
                    retirement_receipt_sha256=receipt_sha256,
                    retirement_receipt_path=str(path),
                    idempotent=(
                        _document_sha256(self._read_writer_locked("cs408"))
                        == receipt["writer_state_before_sha256"]
                    ),
                )

    def reopen_authorized_cs408_terminal_batch_retirement_rollback(
        self,
        *,
        retirement_receipt_path: Path,
        rollback_receipt_path: Path,
    ) -> dict[str, Any]:
        """Read-only verification of the complete rollback postimage."""

        retirement_path = retirement_receipt_path.resolve()
        rollback_path = rollback_receipt_path.resolve()
        try:
            retirement_path.relative_to(
                self.cs408_terminal_retirement_receipt_root.resolve()
            )
            rollback_path.relative_to(
                self.cs408_terminal_retirement_rollback_receipt_root.resolve()
            )
            retirement_payload = retirement_path.read_bytes()
            rollback_payload = rollback_path.read_bytes()
        except (OSError, ValueError) as exc:
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_receipt_path_invalid"
            ) from exc
        retirement_sha256 = hashlib.sha256(retirement_payload).hexdigest()
        rollback_sha256 = hashlib.sha256(rollback_payload).hexdigest()
        expected_retirement_path = (
            self.cs408_terminal_retirement_receipt_root
            / "sha256"
            / retirement_sha256[:2]
            / f"{retirement_sha256}.json"
        )
        expected_rollback_path = (
            self.cs408_terminal_retirement_rollback_receipt_root
            / "sha256"
            / rollback_sha256[:2]
            / f"{rollback_sha256}.json"
        )
        if (
            retirement_path != expected_retirement_path
            or rollback_path != expected_rollback_path
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_receipt_path_invalid"
            )
        retirement = self._read_cs408_terminal_retirement_receipt(
            retirement_sha256
        )
        rollback = self._read_content_addressed(
            self.cs408_terminal_retirement_rollback_receipt_root,
            rollback_sha256,
            "cs408_terminal_retirement_rollback_receipt",
        )
        required = {
            "schema_version",
            "subject",
            "retirement_receipt_sha256",
            "retirement_receipt_path",
            "authorization_descriptor_sha256",
            "rollback_token",
            "writer_state_after_sha256",
            "writer_state_restored_sha256",
            "batch_pointer_unchanged_sha256",
            "batch_snapshot_unchanged_sha256",
            "terminal_receipt_unchanged_sha256",
            "retirement_pointer_withdrawn",
            "retirement_intent_withdrawn",
            "rollback_postimage_verification_sha256",
            "queue_entry_created_count",
            "replacement_task_created_count",
            "capture_replay_count",
            "authority_snapshot_count",
            "authority_snapshot_mcp_tool_call_count",
            "model_mcp_tool_call_count",
            "mcp_tool_call_count",
            "model_call_count",
            "provider_request_count",
            "formal_write_count",
            "sol_enabled",
            "rolled_back_at",
            "seal",
        }
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        expected_postimage_sha256 = _value_sha256(
            {
                "retirement_receipt_sha256": retirement_sha256,
                "writer_state_restored_sha256": authorization[
                    "writer_preimage_sha256"
                ],
                "old_batch_sha256": authorization["batch_sha256"],
                "batch_pointer_unchanged_sha256": authorization[
                    "batch_pointer_sha256"
                ],
                "batch_snapshot_unchanged_sha256": authorization[
                    "snapshot_sha256"
                ],
                "terminal_receipt_unchanged_sha256": authorization[
                    "terminal_receipt_sha256"
                ],
                "retirement_pointer_withdrawn": True,
                "retirement_intent_withdrawn": True,
            }
        )
        if (
            set(rollback) != required
            or rollback.get("schema_version")
            != CS408_TERMINAL_BATCH_RETIREMENT_ROLLBACK_RECEIPT_SCHEMA
            or rollback.get("subject") != "cs408"
            or rollback.get("retirement_receipt_sha256")
            != retirement_sha256
            or rollback.get("retirement_receipt_path")
            != str(retirement_path)
            or rollback.get("authorization_descriptor_sha256")
            != _value_sha256(authorization)
            or rollback.get("rollback_token") != retirement["rollback_token"]
            or rollback.get("writer_state_after_sha256")
            != retirement["writer_state_after_sha256"]
            or rollback.get("writer_state_restored_sha256")
            != authorization["writer_preimage_sha256"]
            or rollback.get("batch_pointer_unchanged_sha256")
            != authorization["batch_pointer_sha256"]
            or rollback.get("batch_snapshot_unchanged_sha256")
            != authorization["snapshot_sha256"]
            or rollback.get("terminal_receipt_unchanged_sha256")
            != authorization["terminal_receipt_sha256"]
            or rollback.get("retirement_pointer_withdrawn") is not True
            or rollback.get("retirement_intent_withdrawn") is not True
            or rollback.get("rollback_postimage_verification_sha256")
            != expected_postimage_sha256
            or any(
                rollback.get(key) != 0
                for key in (
                    "queue_entry_created_count",
                    "replacement_task_created_count",
                    "capture_replay_count",
                    "authority_snapshot_count",
                    "authority_snapshot_mcp_tool_call_count",
                    "model_mcp_tool_call_count",
                    "mcp_tool_call_count",
                    "model_call_count",
                    "provider_request_count",
                    "formal_write_count",
                )
            )
            or rollback.get("sol_enabled") is not False
        ):
            raise SubjectSolContractError(
                "cs408_terminal_retirement_rollback_receipt_invalid"
            )
        _timestamp(rollback.get("rolled_back_at"), "retirement_rolled_back_at")
        self._verify_seal(
            rollback,
            purpose="cs408-terminal-batch-writer-retirement-rollback-v1",
        )
        with _ExistingFileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            with _ExistingFileLock(self._subject_lock_path("cs408")):
                self._verify_cs408_authorized_historical_evidence_locked()
                writer = self._read_writer_locked("cs408")
                if (
                    _document_sha256(writer)
                    != authorization["writer_preimage_sha256"]
                    or global_state.get("active_writer") is not None
                    or bool(global_state.get("queue"))
                    or global_state.get("formal_write_count") != 0
                    or self.cs408_terminal_retirement_pointer_path.exists()
                    or self.cs408_terminal_retirement_pointer_path.is_symlink()
                    or self.cs408_terminal_retirement_intent_path.exists()
                    or self.cs408_terminal_retirement_intent_path.is_symlink()
                ):
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_rollback_postimage_invalid"
                    )
        return {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-rollback-reopen-result-v1"
            ),
            "subject": "cs408",
            "status": "reopened",
            "authorization_descriptor_sha256": _value_sha256(authorization),
            "retirement_receipt_sha256": retirement_sha256,
            "retirement_receipt_path": str(retirement_path),
            "rollback_receipt_sha256": rollback_sha256,
            "rollback_receipt_path": str(rollback_path),
            "rollback_token": retirement["rollback_token"],
            "writer_state_restored_sha256": authorization[
                "writer_preimage_sha256"
            ],
            "old_batch_sha256": authorization["batch_sha256"],
            "batch_pointer_unchanged_sha256": authorization[
                "batch_pointer_sha256"
            ],
            "batch_snapshot_unchanged_sha256": authorization[
                "snapshot_sha256"
            ],
            "terminal_receipt_unchanged_sha256": authorization[
                "terminal_receipt_sha256"
            ],
            "retirement_pointer_withdrawn": True,
            "retirement_intent_withdrawn": True,
            "rollback_postimage_verification_sha256": (
                expected_postimage_sha256
            ),
            "rollback_receipt_reopened": True,
            "rollback_reopen_read_only": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def reopen_authorized_cs408_terminal_batch_retirement(
        self, *, retirement_receipt_path: Path | None = None
    ) -> dict[str, Any]:
        """Read and verify the retirement postimage without any MCP call."""

        with _ExistingFileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            with _ExistingFileLock(self._subject_lock_path("cs408")):
                pointer = self._read_cs408_terminal_retirement_pointer_locked()
                if pointer is None:
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_pointer_missing"
                    )
                receipt_sha256 = str(pointer["retirement_receipt_sha256"])
                if retirement_receipt_path is not None:
                    supplied = retirement_receipt_path.resolve()
                    expected = Path(
                        str(pointer["retirement_receipt_path"])
                    ).resolve()
                    try:
                        payload = supplied.read_bytes()
                    except OSError as exc:
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_receipt_path_invalid"
                        ) from exc
                    if (
                        supplied != expected
                        or hashlib.sha256(payload).hexdigest()
                        != receipt_sha256
                    ):
                        raise SubjectSolContractError(
                            "cs408_terminal_retirement_receipt_path_invalid"
                        )
                receipt = self._read_cs408_terminal_retirement_receipt(
                    receipt_sha256
                )
                writer = self._read_writer_locked("cs408")
                if (
                    _document_sha256(writer)
                    != receipt["writer_state_after_sha256"]
                    or _document_sha256(global_state)
                    != receipt["global_sol_state_after_sha256"]
                ):
                    raise SubjectSolContractError(
                        "cs408_terminal_retirement_postimage_invalid"
                    )
                base = self._cs408_retirement_result_locked(
                    pointer=pointer, receipt=receipt, idempotent=True
                )
                return {
                    **base,
                    "schema_version": (
                        "study-intake-cs408-terminal-batch-retirement-reopen-result-v1"
                    ),
                    "status": "reopened",
                    "authority_snapshot_count": 0,
                    "authority_snapshot_mcp_tool_call_count": 0,
                    "model_mcp_tool_call_count": 0,
                    "mcp_tool_call_count": 0,
                }

    def archive_authorized_english_preserved_review_batch(
        self,
        *,
        target_generation: str,
        target_authority_fingerprint: str,
    ) -> dict[str, Any]:
        """Archive and release the writer for the exact 33548 review batch."""

        authorization = ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION
        target_generation = _nonempty(
            target_generation, "english_preserved_review_target_generation"
        )
        target_authority_fingerprint = _sha256(
            target_authority_fingerprint,
            "english_preserved_review_target_authority_fingerprint",
        )
        descriptor_sha256 = _value_sha256(authorization)
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if (
                isinstance(active, Mapping)
                and active.get("subject") == "english"
            ) or any(
                row.get("subject") == "english"
                and row.get("status") in {"queued", "active", "reviewed"}
                for row in global_state["queue"]
            ):
                raise SubjectSolContractError(
                    "english_preserved_review_sol_state_present"
                )
            with _FileLock(self._subject_lock_path("english")):
                existing_pointer = self._read_json(
                    self.english_preserved_review_batch_pointer_path,
                    "english_preserved_review_batch_pointer",
                )
                if existing_pointer is not None:
                    self._verify_seal(
                        existing_pointer,
                        purpose=(
                            "english-preserved-review-batch-retirement-pointer-v1"
                        ),
                    )
                    archive = self._read_content_addressed(
                        self.english_preserved_review_batch_archive_root,
                        _sha256(
                            existing_pointer.get("archive_sha256"),
                            "english_preserved_review_archive_sha256",
                        ),
                        "english_preserved_review_batch_archive",
                    )
                    self._verify_seal(
                        archive,
                        purpose=(
                            "english-preserved-review-batch-retirement-archive-v1"
                        ),
                    )
                    writer = self._read_writer_locked("english")
                    if (
                        existing_pointer.get("authorization_descriptor_sha256")
                        != descriptor_sha256
                        or existing_pointer.get("batch_sha256")
                        != authorization["batch_sha256"]
                        or _document_sha256(writer)
                        != existing_pointer.get("writer_postimage_sha256")
                        or existing_pointer.get("target_generation")
                        != target_generation
                        or existing_pointer.get(
                            "target_authority_fingerprint"
                        )
                        != target_authority_fingerprint
                    ):
                        raise SubjectSolContractError(
                            "english_preserved_review_retirement_postimage_invalid"
                        )
                    return {
                        "status": "reopened",
                        "archive": archive,
                        "archive_sha256": existing_pointer["archive_sha256"],
                        "archive_path": existing_pointer["archive_path"],
                        "pointer": existing_pointer,
                        "pointer_sha256": _document_sha256(existing_pointer),
                        "writer": writer,
                        "writer_postimage_sha256": _document_sha256(writer),
                        "target_generation": target_generation,
                        "target_authority_fingerprint": (
                            target_authority_fingerprint
                        ),
                        "batch_postimage_sha256": authorization["batch_sha256"],
                        "batch_pointer_postimage_sha256": authorization[
                            "batch_pointer_sha256"
                        ],
                        "idempotent": True,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "mcp_tool_call_count": 0,
                        "sol_call_count": 0,
                        "formal_write_count": 0,
                    }
                batch_path = self._batch_path("english")
                pointer_path = self._batch_pointer_path("english")
                writer_path = self._writer_path("english")
                try:
                    batch_bytes = batch_path.read_bytes()
                    pointer_bytes = pointer_path.read_bytes()
                    writer_bytes = writer_path.read_bytes()
                except OSError as exc:
                    raise SubjectSolContractError(
                        "english_preserved_review_preimage_unreadable"
                    ) from exc
                if (
                    hashlib.sha256(batch_bytes).hexdigest()
                    != authorization["batch_sha256"]
                    or hashlib.sha256(pointer_bytes).hexdigest()
                    != authorization["batch_pointer_sha256"]
                    or hashlib.sha256(writer_bytes).hexdigest()
                    != authorization["writer_preimage_sha256"]
                ):
                    raise SubjectSolContractError(
                        "english_preserved_review_preimage_drift"
                    )
                batch = self._read_batch_locked("english")
                writer = self._read_writer_locked("english")
                pointer = self._read_json(
                    pointer_path, "subject_luna_batch_pointer"
                )
                assert batch is not None and pointer is not None
                if (
                    batch.get("batch_id") != authorization["batch_id"]
                    or batch.get("authority_generation")
                    != authorization["source_generation"]
                    or batch.get("authority_fingerprint")
                    != authorization["source_authority_fingerprint"]
                    or batch.get("all_terminal") is not True
                    or batch.get("sol_ready") is not False
                    or len(batch.get("tasks") or []) != 1
                    or batch["tasks"][0].get("unit_sha256")
                    != authorization["unit_sha256"]
                    or batch["tasks"][0].get("status") != "execution_failed"
                    or pointer.get("snapshot_sha256")
                    != authorization["batch_snapshot_sha256"]
                    or writer.get("batch_id") != authorization["batch_id"]
                    or writer.get("handoff_status") != "awaiting_luna"
                    or writer.get("formal_write_count") != 0
                ):
                    raise SubjectSolContractError(
                        "english_preserved_review_retirement_binding_invalid"
                    )
                archive = self._seal(
                    {
                        "schema_version": (
                            "english_preserved_review_batch_archive_v1"
                        ),
                        "authorization_descriptor_sha256": descriptor_sha256,
                        "subject": "english",
                        "mode": authorization["mode"],
                        "batch_id": authorization["batch_id"],
                        "batch": batch,
                        "batch_sha256": authorization["batch_sha256"],
                        "batch_pointer": pointer,
                        "batch_pointer_sha256": authorization[
                            "batch_pointer_sha256"
                        ],
                        "batch_snapshot_sha256": authorization[
                            "batch_snapshot_sha256"
                        ],
                        "writer_preimage": writer,
                        "writer_preimage_sha256": authorization[
                            "writer_preimage_sha256"
                        ],
                        "target_generation": target_generation,
                        "target_authority_fingerprint": (
                            target_authority_fingerprint
                        ),
                        "capture_replayed": False,
                        "replacement_task_created": False,
                        "queue_entry_created": False,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "mcp_tool_call_count": 0,
                        "sol_call_count": 0,
                        "formal_write_count": 0,
                        "archived_at": _utc_now(),
                    },
                    purpose=(
                        "english-preserved-review-batch-retirement-archive-v1"
                    ),
                )
                archive_sha256, archive_path = self._publish_immutable(
                    self.english_preserved_review_batch_archive_root, archive
                )
                writer_after = copy.deepcopy(writer)
                writer_after.update(
                    {
                        "handoff_status": "awaiting_luna",
                        "batch_id": None,
                        "authorization_receipt_sha256": None,
                        "daily_sol_batch_sha256": None,
                        "review_receipt_sha256": None,
                        "commit_receipt_sha256": None,
                        "generation_fence": {
                            "blocked": False,
                            "source_generation": authorization[
                                "source_generation"
                            ],
                            "next_generation": authorization[
                                "source_generation"
                            ],
                        },
                    }
                )
                writer_after["generation_fence"]["next_generation"] = (
                    target_generation
                )
                pointer_value: dict[str, Any] | None = None
                try:
                    writer_after = self._write_writer_locked(writer_after)
                    if os.environ.get(
                        "STUDY_INTAKE_ENGLISH_REVIEW_REPAIR_FAILPOINT"
                    ) == "after_writer_retirement":
                        raise SubjectSolContractError(
                            "english_preserved_review_failpoint_after_writer_retirement"
                        )
                    pointer_value = self._seal(
                    {
                        "schema_version": (
                            "english_preserved_review_batch_retirement_pointer_v1"
                        ),
                        "authorization_descriptor_sha256": descriptor_sha256,
                        "subject": "english",
                        "batch_id": authorization["batch_id"],
                        "batch_sha256": authorization["batch_sha256"],
                        "batch_pointer_sha256": authorization[
                            "batch_pointer_sha256"
                        ],
                        "archive_sha256": archive_sha256,
                        "archive_path": str(archive_path),
                        "writer_preimage_sha256": authorization[
                            "writer_preimage_sha256"
                        ],
                        "writer_postimage_sha256": _document_sha256(
                            writer_after
                        ),
                        "target_generation": target_generation,
                        "target_authority_fingerprint": (
                            target_authority_fingerprint
                        ),
                        "applied_at": _utc_now(),
                        "formal_write_count": 0,
                    },
                    purpose=(
                        "english-preserved-review-batch-retirement-pointer-v1"
                    ),
                )
                    self._atomic_json(
                        self.english_preserved_review_batch_pointer_path,
                        pointer_value,
                    )
                except BaseException as exc:
                    try:
                        self._atomic_json(
                            self._writer_path("english"),
                            self._read_writer_projection(writer),
                        )
                        try:
                            self.english_preserved_review_batch_pointer_path.unlink()
                        except FileNotFoundError:
                            pass
                        if (
                            hashlib.sha256(
                                self._writer_path("english").read_bytes()
                            ).hexdigest()
                            != authorization["writer_preimage_sha256"]
                            or self.english_preserved_review_batch_pointer_path.exists()
                        ):
                            raise SubjectSolContractError(
                                "english_preserved_review_retirement_rollback_unverified"
                            )
                    except BaseException as rollback_exc:
                        raise SubjectSolContractError(
                            "english_preserved_review_retirement_fail_fenced"
                        ) from rollback_exc
                    raise SubjectSolContractError(
                        "english_preserved_review_retirement_apply_rolled_back"
                    ) from exc
                assert pointer_value is not None
                return {
                    "status": "applied",
                    "archive": archive,
                    "archive_sha256": archive_sha256,
                    "archive_path": str(archive_path),
                    "pointer": pointer_value,
                    "pointer_sha256": _document_sha256(pointer_value),
                    "writer": writer_after,
                    "writer_postimage_sha256": _document_sha256(writer_after),
                    "target_generation": target_generation,
                    "target_authority_fingerprint": (
                        target_authority_fingerprint
                    ),
                    "batch_postimage_sha256": authorization["batch_sha256"],
                    "batch_pointer_postimage_sha256": authorization[
                        "batch_pointer_sha256"
                    ],
                    "idempotent": False,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "mcp_tool_call_count": 0,
                    "sol_call_count": 0,
                    "formal_write_count": 0,
                }

    def reopen_authorized_english_preserved_review_batch(
        self,
        *,
        target_generation: str,
        target_authority_fingerprint: str,
    ) -> dict[str, Any]:
        return self.archive_authorized_english_preserved_review_batch(
            target_generation=target_generation,
            target_authority_fingerprint=target_authority_fingerprint,
        )

    def rollback_authorized_english_preserved_review_batch(
        self, *, archive_sha256: str
    ) -> dict[str, Any]:
        authorization = ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION
        checked_archive = _sha256(
            archive_sha256, "english_preserved_review_archive_sha256"
        )
        with _FileLock(self.global_lock_path):
            with _FileLock(self._subject_lock_path("english")):
                pointer = self._read_json(
                    self.english_preserved_review_batch_pointer_path,
                    "english_preserved_review_batch_pointer",
                )
                if pointer is None or pointer.get("archive_sha256") != checked_archive:
                    raise SubjectSolContractError(
                        "english_preserved_review_retirement_pointer_missing"
                    )
                self._verify_seal(
                    pointer,
                    purpose=(
                        "english-preserved-review-batch-retirement-pointer-v1"
                    ),
                )
                archive = self._read_content_addressed(
                    self.english_preserved_review_batch_archive_root,
                    checked_archive,
                    "english_preserved_review_batch_archive",
                )
                self._verify_seal(
                    archive,
                    purpose=(
                        "english-preserved-review-batch-retirement-archive-v1"
                    ),
                )
                writer = self._read_writer_locked("english")
                if _document_sha256(writer) != pointer.get(
                    "writer_postimage_sha256"
                ):
                    raise SubjectSolContractError(
                        "english_preserved_review_retirement_postimage_invalid"
                    )
                restored = self._read_writer_projection(
                    dict(archive["writer_preimage"])
                )
                self._atomic_json(self._writer_path("english"), restored)
                try:
                    self.english_preserved_review_batch_pointer_path.unlink()
                except FileNotFoundError as exc:
                    raise SubjectSolContractError(
                        "english_preserved_review_retirement_pointer_missing"
                    ) from exc
                rollback = self._seal(
                    {
                        "schema_version": (
                            "english_preserved_review_batch_retirement_rollback_receipt_v1"
                        ),
                        "authorization_descriptor_sha256": _value_sha256(
                            authorization
                        ),
                        "subject": "english",
                        "archive_sha256": checked_archive,
                        "batch_restored_sha256": authorization[
                            "batch_sha256"
                        ],
                        "batch_pointer_restored_sha256": authorization[
                            "batch_pointer_sha256"
                        ],
                        "writer_restored_sha256": _document_sha256(restored),
                        "status": "rolled_back",
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "mcp_tool_call_count": 0,
                        "sol_call_count": 0,
                        "formal_write_count": 0,
                        "rolled_back_at": _utc_now(),
                    },
                    purpose=(
                        "english-preserved-review-batch-retirement-rollback-v1"
                    ),
                )
                rollback_sha256, rollback_path = self._publish_immutable(
                    self.english_preserved_review_batch_rollback_root,
                    rollback,
                )
                return {
                    "rollback_receipt": rollback,
                    "rollback_receipt_sha256": rollback_sha256,
                    "rollback_receipt_path": str(rollback_path),
                    "batch_restored_sha256": authorization["batch_sha256"],
                    "batch_pointer_restored_sha256": authorization[
                        "batch_pointer_sha256"
                    ],
                    "writer_restored_sha256": _document_sha256(restored),
                    "formal_write_count": 0,
                }

    def reopen_authorized_english_preserved_review_batch_rollback(
        self, *, rollback_receipt_sha256: str
    ) -> dict[str, Any]:
        digest = _sha256(
            rollback_receipt_sha256,
            "english_preserved_review_batch_rollback_sha256",
        )
        authorization = ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION
        with _FileLock(self.global_lock_path):
            with _FileLock(self._subject_lock_path("english")):
                rollback = self._read_content_addressed(
                    self.english_preserved_review_batch_rollback_root,
                    digest,
                    "english_preserved_review_batch_rollback",
                )
                self._verify_seal(
                    rollback,
                    purpose=(
                        "english-preserved-review-batch-retirement-rollback-v1"
                    ),
                )
                writer = self._read_writer_locked("english")
                batch = self._read_batch_locked("english")
                pointer = self._read_json(
                    self._batch_pointer_path("english"),
                    "subject_luna_batch_pointer",
                )
                if (
                    batch is None
                    or pointer is None
                    or _document_sha256(batch)
                    != authorization["batch_sha256"]
                    or _document_sha256(pointer)
                    != authorization["batch_pointer_sha256"]
                    or _document_sha256(writer)
                    != authorization["writer_preimage_sha256"]
                    or self.english_preserved_review_batch_pointer_path.exists()
                    or rollback.get("status") != "rolled_back"
                    or rollback.get("formal_write_count") != 0
                ):
                    raise SubjectSolContractError(
                        "english_preserved_review_batch_rollback_postimage_invalid"
                    )
                return {
                    "schema_version": (
                        "english_preserved_review_batch_retirement_rollback_"
                        "reopen_v1"
                    ),
                    "status": "reopened",
                    "rollback_receipt": rollback,
                    "rollback_receipt_sha256": digest,
                    "batch_restored_sha256": authorization["batch_sha256"],
                    "batch_pointer_restored_sha256": authorization[
                        "batch_pointer_sha256"
                    ],
                    "writer_restored_sha256": authorization[
                        "writer_preimage_sha256"
                    ],
                    "formal_write_count": 0,
                }

    def _require_background_rollover_for_replacement_locked(
        self, subject: str, existing: Mapping[str, Any], writer: Mapping[str, Any]
    ) -> None:
        authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        english_authorization = (
            ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION
        )
        if (
            subject == "english"
            and existing.get("batch_id") == english_authorization["batch_id"]
            and _document_sha256(existing)
            == english_authorization["batch_sha256"]
        ):
            pointer = self._read_json(
                self.english_preserved_review_batch_pointer_path,
                "english_preserved_review_batch_pointer",
            )
            if pointer is None:
                raise SubjectSolContractError(
                    "english_preserved_review_batch_retirement_required"
                )
            self._verify_seal(
                pointer,
                purpose=(
                    "english-preserved-review-batch-retirement-pointer-v1"
                ),
            )
            if (
                pointer.get("batch_sha256")
                != english_authorization["batch_sha256"]
                or pointer.get("writer_postimage_sha256")
                != _document_sha256(writer)
            ):
                raise SubjectSolContractError(
                    "english_preserved_review_retirement_postimage_invalid"
                )
            return
        if (
            subject == "cs408"
            and existing.get("batch_id") == authorization["batch_id"]
            and _document_sha256(existing) == authorization["batch_sha256"]
        ):
            special = self._read_cs408_terminal_retirement_pointer_locked()
            if special is None:
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_required"
                )
            receipt = self._read_cs408_terminal_retirement_receipt(
                str(special["retirement_receipt_sha256"])
            )
            if (
                writer != receipt["writer_state_after"]
                or _document_sha256(writer)
                != receipt["writer_state_after_sha256"]
            ):
                raise SubjectSolContractError(
                    "cs408_terminal_retirement_postimage_invalid"
                )
            self._verify_cs408_authorized_historical_evidence_locked()
            return
        pointer = self._read_background_rollover_pointer_locked(subject)
        if pointer is None:
            raise SubjectSolContractError(
                "subject_background_rollover_required"
            )
        if (
            pointer.get("old_batch_id") != existing.get("batch_id")
            or pointer.get("old_batch_sha256") != _document_sha256(existing)
            or pointer.get("writer_revision_after") != writer.get("revision")
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_pointer_stale"
            )
        receipt = self._read_background_rollover_receipt(
            str(pointer["rollover_receipt_sha256"])
        )
        if (
            receipt.get("rollover_id") != pointer.get("rollover_id")
            or receipt.get("old_batch_id") != existing.get("batch_id")
            or receipt.get("old_batch_sha256") != _document_sha256(existing)
            or receipt.get("writer_revision_after") != writer.get("revision")
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_receipt_stale"
            )

    def canary_readiness(
        self,
        subject: str,
        *,
        next_generation: str | None = None,
        next_authority_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Read-only tri-state readiness for an initial canary consumer.

        A failed terminal batch is recoverable only when it is still owned by
        the subject writer and the caller has frozen the next authority.  The
        method never creates keys, receipts, directories, or projections.
        """

        checked = _subject(subject)
        if next_generation is not None:
            next_generation = _nonempty(next_generation, "next_generation")
        if next_authority_fingerprint is not None:
            next_authority_fingerprint = _sha256(
                next_authority_fingerprint, "next_authority_fingerprint"
            )
        subject_lock_path = self._subject_lock_path(checked)
        if self.global_lock_path.is_file() and subject_lock_path.is_file():
            with _ExistingFileLock(self.global_lock_path):
                global_state = self._read_global_locked()
                with _ExistingFileLock(subject_lock_path):
                    batch = self._read_batch_locked(checked)
                    writer = self._read_writer_locked(checked)
                    rollover_pointer = (
                        self._read_background_rollover_pointer_locked(checked)
                    )
                    cs408_retirement_pointer = (
                        self._read_cs408_terminal_retirement_pointer_locked()
                        if checked == "cs408"
                        else None
                    )
                    english_review_retirement_pointer = (
                        self._read_json(
                            self.english_preserved_review_batch_pointer_path,
                            "english_preserved_review_batch_pointer",
                        )
                        if checked == "english"
                        else None
                    )
        else:
            first = (
                self._read_global_locked(),
                self._read_batch_locked(checked),
                self._read_writer_locked(checked),
                self._read_background_rollover_pointer_locked(checked),
                (
                    self._read_cs408_terminal_retirement_pointer_locked()
                    if checked == "cs408"
                    else None
                ),
                (
                    self._read_json(
                        self.english_preserved_review_batch_pointer_path,
                        "english_preserved_review_batch_pointer",
                    )
                    if checked == "english"
                    else None
                ),
            )
            second = (
                self._read_global_locked(),
                self._read_batch_locked(checked),
                self._read_writer_locked(checked),
                self._read_background_rollover_pointer_locked(checked),
                (
                    self._read_cs408_terminal_retirement_pointer_locked()
                    if checked == "cs408"
                    else None
                ),
                (
                    self._read_json(
                        self.english_preserved_review_batch_pointer_path,
                        "english_preserved_review_batch_pointer",
                    )
                    if checked == "english"
                    else None
                ),
            )
            if first != second:
                raise SubjectSolContractError(
                    "canary_readiness_concurrent_drift"
                )
            (
                global_state,
                batch,
                writer,
                rollover_pointer,
                cs408_retirement_pointer,
                english_review_retirement_pointer,
            ) = first
        subject_sol_present = (
            isinstance(global_state.get("active_writer"), Mapping)
            and global_state["active_writer"].get("subject") == checked
        ) or any(
            row.get("subject") == checked
            and row.get("status") in {"queued", "active", "reviewed"}
            for row in global_state["queue"]
        )
        common = {
            "schema_version": "study-intake-canary-readiness-v1",
            "subject": checked,
            "batch_id": batch.get("batch_id") if batch else None,
            "batch_sha256": _document_sha256(batch) if batch else None,
            "writer_revision": writer["revision"],
            "writer_batch_id": writer["batch_id"],
            "source_generation": (
                batch.get("authority_generation") if batch else None
            ),
            "next_generation": next_generation,
            "next_authority_fingerprint": next_authority_fingerprint,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        if checked == "cs408":
            authorization = CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
            batch_pointer = self._read_json(
                self._batch_pointer_path(checked),
                "subject_luna_batch_pointer",
            )
            task = (
                batch["tasks"][0]
                if batch is not None and len(batch.get("tasks") or []) == 1
                else None
            )
            common.update(
                {
                    "mode": authorization["mode"],
                    "authorization_descriptor_sha256": _value_sha256(
                        authorization
                    ),
                    "terminal_receipt_sha256": (
                        task.get("terminal_receipt_sha256")
                        if task is not None
                        else None
                    ),
                    "writer_preimage_sha256": (
                        authorization["writer_preimage_sha256"]
                        if cs408_retirement_pointer is not None
                        else _document_sha256(writer)
                    ),
                    "batch_pointer_sha256": (
                        _document_sha256(batch_pointer)
                        if batch_pointer is not None
                        else None
                    ),
                    "snapshot_sha256": (
                        batch_pointer.get("snapshot_sha256")
                        if batch_pointer is not None
                        else None
                    ),
                    "source_authority_fingerprint": (
                        batch.get("authority_fingerprint")
                        if batch is not None
                        else None
                    ),
                    "retirement_receipt_sha256": None,
                    "retirement_receipt_path": None,
                    "retirement_rollback_token": None,
                    "writer_postimage_sha256": None,
                    "postimage_verification_sha256": None,
                }
            )
        if subject_sol_present:
            return {
                **common,
                "readiness": "invalid",
                "reason": "subject_background_rollover_sol_state_present",
            }
        authorized_old_current = bool(
            checked == "cs408"
            and batch is not None
            and _document_sha256(batch)
            == CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION["batch_sha256"]
        )
        if authorized_old_current and cs408_retirement_pointer is not None:
            self._verify_cs408_authorized_historical_evidence_locked()
            receipt_sha256 = str(
                cs408_retirement_pointer["retirement_receipt_sha256"]
            )
            receipt = self._read_cs408_terminal_retirement_receipt(
                receipt_sha256
            )
            valid_retirement = (
                batch is not None
                and _document_sha256(batch)
                == CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION[
                    "batch_sha256"
                ]
                and _document_sha256(writer)
                == receipt["writer_state_after_sha256"]
                and writer == receipt["writer_state_after"]
                and next_generation == receipt["next_generation"]
                and next_authority_fingerprint
                == receipt["next_authority_fingerprint"]
                and _document_sha256(global_state)
                == receipt["global_sol_state_after_sha256"]
            )
            postimage_sha256 = (
                self._cs408_retirement_postimage_verification_sha256(
                    pointer=cs408_retirement_pointer,
                    receipt_sha256=receipt_sha256,
                )
                if valid_retirement
                else None
            )
            return {
                **common,
                "readiness": "ready" if valid_retirement else "invalid",
                "reason": (
                    "cs408_terminal_batch_retired"
                    if valid_retirement
                    else "cs408_terminal_retirement_postimage_invalid"
                ),
                "terminal_receipt_sha256": receipt[
                    "terminal_receipt_sha256"
                ],
                "writer_preimage_sha256": receipt[
                    "writer_state_before_sha256"
                ],
                "batch_pointer_sha256": receipt[
                    "batch_pointer_before_sha256"
                ],
                "snapshot_sha256": receipt["batch_snapshot_sha256"],
                "source_generation": receipt["source_generation"],
                "source_authority_fingerprint": receipt[
                    "source_authority_fingerprint"
                ],
                "retirement_receipt_sha256": receipt_sha256,
                "retirement_receipt_path": cs408_retirement_pointer[
                    "retirement_receipt_path"
                ],
                "retirement_rollback_token": receipt["rollback_token"],
                "writer_postimage_sha256": receipt[
                    "writer_state_after_sha256"
                ],
                "postimage_verification_sha256": postimage_sha256,
            }
        authorized_english_review_current = bool(
            checked == "english"
            and batch is not None
            and _document_sha256(batch)
            == ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION[
                "batch_sha256"
            ]
        )
        if (
            authorized_english_review_current
            and english_review_retirement_pointer is not None
        ):
            authorization = ENGLISH_PRESERVED_REVIEW_BATCH_RETIREMENT_AUTHORIZATION
            pointer = english_review_retirement_pointer
            self._verify_seal(
                pointer,
                purpose="english-preserved-review-batch-retirement-pointer-v1",
            )
            archive = self._read_content_addressed(
                self.english_preserved_review_batch_archive_root,
                _sha256(
                    pointer.get("archive_sha256"),
                    "english_preserved_review_archive_sha256",
                ),
                "english_preserved_review_batch_archive",
            )
            self._verify_seal(
                archive,
                purpose="english-preserved-review-batch-retirement-archive-v1",
            )
            valid_retirement = (
                pointer.get("authorization_descriptor_sha256")
                == _value_sha256(authorization)
                and pointer.get("batch_id") == authorization["batch_id"]
                and pointer.get("batch_sha256") == authorization["batch_sha256"]
                and pointer.get("batch_pointer_sha256")
                == authorization["batch_pointer_sha256"]
                and archive.get("batch_sha256") == authorization["batch_sha256"]
                and archive.get("batch_pointer_sha256")
                == authorization["batch_pointer_sha256"]
                and pointer.get("writer_postimage_sha256")
                == _document_sha256(writer)
                and writer.get("handoff_status") == "awaiting_luna"
                and writer.get("batch_id") is None
                and writer.get("formal_write_count") == 0
                and pointer.get("target_generation") == next_generation
                and pointer.get("target_authority_fingerprint")
                == next_authority_fingerprint
                and archive.get("target_generation") == next_generation
                and archive.get("target_authority_fingerprint")
                == next_authority_fingerprint
                and pointer.get("formal_write_count") == 0
                and archive.get("formal_write_count") == 0
            )
            return {
                **common,
                "readiness": "ready" if valid_retirement else "invalid",
                "reason": (
                    "english_preserved_review_batch_retired"
                    if valid_retirement
                    else "english_preserved_review_retirement_postimage_invalid"
                ),
            }
        if batch is None:
            fence = writer["generation_fence"]
            valid = (
                writer["handoff_status"] == "awaiting_luna"
                and writer["batch_id"] is None
                and fence.get("blocked") is False
                and (
                    next_generation is None
                    or fence.get("next_generation") in {None, next_generation}
                )
            )
            return {
                **common,
                "readiness": "ready" if valid else "invalid",
                "reason": "ready" if valid else "subject_writer_state_invalid",
            }
        fence = writer.get("generation_fence") or {}
        if (
            batch.get("status") == "frozen"
            and batch.get("all_terminal") is True
            and batch.get("sol_ready") is False
            and writer.get("handoff_status") == "awaiting_luna"
            and writer.get("batch_id") is None
            and writer.get("formal_write_count") == 0
            and fence.get("blocked") is False
            and fence.get("source_generation")
            == batch.get("authority_generation")
            and next_generation is not None
            and fence.get("next_generation")
            in {batch.get("authority_generation"), next_generation}
            and rollover_pointer is not None
            and rollover_pointer.get("old_batch_id") == batch.get("batch_id")
            and rollover_pointer.get("old_batch_sha256")
            == _document_sha256(batch)
            and rollover_pointer.get("writer_revision_after")
            == writer.get("revision")
        ):
            receipt = self._read_background_rollover_receipt(
                str(rollover_pointer["rollover_receipt_sha256"])
            )
            valid_v2_recovery = (
                receipt.get("schema_version")
                == SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
                and receipt.get("next_generation") == next_generation
                and receipt.get("next_authority_fingerprint")
                == next_authority_fingerprint
            )
            valid_v1_archived_rollover = (
                receipt.get("schema_version")
                == SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_SCHEMA
                and receipt.get("subject") == checked
                and receipt.get("old_batch_id") == batch.get("batch_id")
                and receipt.get("old_batch_sha256")
                == _document_sha256(batch)
                and receipt.get("next_generation")
                == batch.get("authority_generation")
                and receipt.get("authority_generation")
                == batch.get("authority_generation")
                and receipt.get("authority_fingerprint")
                == batch.get("authority_fingerprint")
                and receipt.get("formal_write_count") == 0
                and receipt.get("sol_enabled") is False
            )
            valid_recovery = bool(
                valid_v2_recovery or valid_v1_archived_rollover
            )
            return {
                **common,
                "readiness": "ready" if valid_recovery else "invalid",
                "reason": (
                    "recovered_terminal_batch_archived"
                    if valid_recovery
                    else "subject_background_rollover_authority_mismatch"
                ),
            }
        if (
            batch.get("status") == "frozen"
            and batch.get("all_terminal") is True
            and batch.get("sol_ready") is False
            and writer.get("handoff_status") == "awaiting_luna"
            and writer.get("batch_id") == batch.get("batch_id")
            and writer.get("formal_write_count") == 0
        ):
            if next_generation is None or next_authority_fingerprint is None:
                return {
                    **common,
                    "readiness": "invalid",
                    "reason": "next_subject_authority_required",
                }
            return {
                **common,
                "readiness": "recoverable_terminal_batch",
                "reason": "explicit_subject_resume_required",
            }
        return {
            **common,
            "readiness": "invalid",
            "reason": "subject_luna_batch_already_current",
        }

    def read_background_rollover_recovery(
        self, subject: str
    ) -> dict[str, Any] | None:
        """Reopen the current v2 recovery transaction without mutation."""

        checked = _subject(subject)
        with _ExistingFileLock(self._subject_lock_path(checked)):
            pointer = self._read_background_rollover_pointer_locked(checked)
            if pointer is None:
                return None
            receipt = self._read_background_rollover_receipt(
                str(pointer["rollover_receipt_sha256"])
            )
            if (
                receipt.get("schema_version")
                != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
                or receipt.get("subject") != checked
                or receipt.get("rollover_id") != pointer.get("rollover_id")
                or receipt.get("old_batch_id")
                != pointer.get("old_batch_id")
                or receipt.get("old_batch_sha256")
                != pointer.get("old_batch_sha256")
                or receipt.get("writer_revision_after")
                != pointer.get("writer_revision_after")
            ):
                raise SubjectSolContractError(
                    "subject_background_rollover_recovery_binding_invalid"
                )
            return {
                "pointer": pointer,
                "receipt": receipt,
                "recovery_receipt_sha256": pointer[
                    "rollover_receipt_sha256"
                ],
                "recovery_receipt_path": pointer[
                    "rollover_receipt_path"
                ],
            }

    def rollover_background_luna_batch_v2(
        self,
        subject: str,
        *,
        next_generation: str,
        next_authority_fingerprint: str,
        original_preclaim_failure_receipt_sha256: str,
        original_preclaim_failure_receipt_path: str,
        preserved_queue_entry: Mapping[str, Any],
        canary_state_before: Mapping[str, Any],
        subsequent_attempt_receipt_sha256s: Sequence[str],
        preserved_task: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Generation-aware, zero-write recovery for one preserved English queue."""

        checked = _subject(subject)
        if checked != "english":
            raise SubjectSolContractError(
                "subject_background_rollover_v2_subject_invalid"
            )
        next_generation = _nonempty(next_generation, "next_generation")
        next_authority_fingerprint = _sha256(
            next_authority_fingerprint, "next_authority_fingerprint"
        )
        original_sha256 = _sha256(
            original_preclaim_failure_receipt_sha256,
            "original_preclaim_failure_receipt_sha256",
        )
        original_path = Path(
            _nonempty(
                original_preclaim_failure_receipt_path,
                "original_preclaim_failure_receipt_path",
            )
        )
        if not original_path.is_absolute():
            raise SubjectSolContractError(
                "original_preclaim_failure_receipt_path_invalid"
            )
        attempts = sorted(
            {
                _sha256(row, "subsequent_attempt_receipt_sha256")
                for row in subsequent_attempt_receipt_sha256s
            }
        )
        if original_sha256 in attempts:
            raise SubjectSolContractError(
                "subject_background_rollover_attempt_binding_invalid"
            )
        queue_entry = copy.deepcopy(dict(preserved_queue_entry))
        canary_before = copy.deepcopy(dict(canary_state_before))
        preserved = copy.deepcopy(dict(preserved_task))
        try:
            queue_entry_sha256 = _document_sha256(queue_entry)
            producer_unit_id = _nonempty(
                queue_entry.get("producer_unit_id"), "producer_unit_id"
            )
            source_event_ids = list(
                _sequence(
                    queue_entry.get("source_event_ids"),
                    "source_event_ids",
                    nonempty=True,
                )
            )
            queue_identity = {
                "activation_id": _sha256(
                    queue_entry.get("activation_id"), "activation_id"
                ),
                "release_id": _sha256(
                    queue_entry.get("release_id"), "release_id"
                ),
                "producer_unit_id": producer_unit_id,
                "producer_input_contract_sha256": _sha256(
                    queue_entry.get("producer_input_contract_sha256"),
                    "producer_input_contract_sha256",
                ),
                "source_event_set_sha256": _sha256(
                    queue_entry.get("source_event_set_sha256"),
                    "source_event_set_sha256",
                ),
                "source_event_ids": source_event_ids,
                "unit_sha256": _sha256(
                    queue_entry.get("unit_sha256"), "unit_sha256"
                ),
                "frozen_payload_sha256": _sha256(
                    queue_entry.get("frozen_payload_sha256"),
                    "frozen_payload_sha256",
                ),
                "task_object_sha256": _sha256(
                    queue_entry.get("task_object_sha256"), "task_object_sha256"
                ),
            }
        except (TypeError, ValueError) as exc:
            raise SubjectSolContractError(
                "subject_background_rollover_queue_binding_invalid"
            ) from exc
        if (
            queue_entry.get("subject") != "english"
            or queue_entry.get("queue_status") != "pending"
            or queue_entry.get("formal_write_count") != 0
            or queue_entry.get("terminal_receipt_sha256") != original_sha256
            or queue_entry.get("terminal_error_code")
            != "subject_luna_batch_already_current"
            or len(source_event_ids) != len(set(source_event_ids))
            or any(not isinstance(row, str) or not row for row in source_event_ids)
            or canary_before.get("subject") != "english"
            or canary_before.get("activation_id")
            != queue_identity["activation_id"]
            or canary_before.get("luna_consumer_enabled") is not False
            or canary_before.get("active_task_count") != 0
            or canary_before.get("formal_write_count") != 0
            or canary_before.get("sol_enabled") is not False
        ):
            raise SubjectSolContractError(
                "subject_background_rollover_queue_binding_invalid"
            )
        preserved_expected = {
            "unit_sha256": queue_identity["unit_sha256"],
            "frozen_payload_sha256": queue_identity["frozen_payload_sha256"],
            "task_object_sha256": queue_identity["task_object_sha256"],
            "task_object_path": queue_entry.get("task_object_path"),
            "producer_input_contract_sha256": queue_identity[
                "producer_input_contract_sha256"
            ],
            "source_event_set_sha256": queue_identity[
                "source_event_set_sha256"
            ],
        }
        if preserved != preserved_expected or not Path(
            str(preserved.get("task_object_path") or "")
        ).is_absolute():
            raise SubjectSolContractError(
                "subject_background_rollover_preserved_task_invalid"
            )
        try:
            original_bytes = original_path.read_bytes()
            original_receipt = json.loads(original_bytes.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SubjectSolContractError(
                "original_preclaim_failure_receipt_unreadable"
            ) from exc
        if (
            hashlib.sha256(original_bytes).hexdigest() != original_sha256
            or not isinstance(original_receipt, Mapping)
            or original_receipt.get("schema_version")
            != "study-intake-production-canary-preclaim-failure-receipt-v2"
            or original_receipt.get("subject") != "english"
            or original_receipt.get("activation_id")
            != queue_identity["activation_id"]
            or original_receipt.get("failure_stage") != "pre_claim"
            or original_receipt.get("error_code")
            != "subject_luna_batch_already_current"
            or original_receipt.get("queue_entry_preserved") is not True
            or original_receipt.get("model_submission_started") is not False
            or not isinstance(original_receipt.get("failure_evidence"), Mapping)
            or any(
                original_receipt["failure_evidence"].get(key)
                != queue_identity[key]
                for key in (
                    "producer_unit_id",
                    "producer_input_contract_sha256",
                    "source_event_set_sha256",
                    "unit_sha256",
                    "frozen_payload_sha256",
                )
            )
        ):
            raise SubjectSolContractError(
                "original_preclaim_failure_receipt_binding_invalid"
            )

        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active_writer = global_state.get("active_writer")
            if (
                isinstance(active_writer, Mapping)
                and active_writer.get("subject") == checked
            ) or any(
                row.get("subject") == checked
                and row.get("status") in {"queued", "active", "reviewed"}
                for row in global_state["queue"]
            ):
                raise SubjectSolContractError(
                    "subject_background_rollover_sol_state_present"
                )
            with _FileLock(self._subject_lock_path(checked)):
                batch = self._read_batch_locked(checked)
                writer = self._read_writer_locked(checked)
                if batch is None:
                    raise SubjectSolContractError("subject_luna_batch_not_found")
                batch_sha256 = _document_sha256(batch)
                existing_pointer = self._read_background_rollover_pointer_locked(
                    checked
                )
                if (
                    existing_pointer is not None
                    and existing_pointer.get("old_batch_id") == batch["batch_id"]
                    and existing_pointer.get("old_batch_sha256") == batch_sha256
                    and writer.get("handoff_status") == "awaiting_luna"
                    and writer.get("batch_id") is None
                ):
                    receipt = self._read_background_rollover_receipt(
                        str(existing_pointer["rollover_receipt_sha256"])
                    )
                    if (
                        receipt.get("schema_version")
                        != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
                        or receipt.get("next_generation") != next_generation
                        or receipt.get(
                            "original_preclaim_failure_receipt_sha256"
                        )
                        != original_sha256
                        or receipt.get("preserved_queue_entry_sha256")
                        != queue_entry_sha256
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_v2_idempotency_conflict"
                        )
                    return {
                        "receipt": receipt,
                        "receipt_sha256": existing_pointer[
                            "rollover_receipt_sha256"
                        ],
                        "receipt_path": existing_pointer[
                            "rollover_receipt_path"
                        ],
                        "writer_state": writer,
                        "idempotent": True,
                    }
                recovery_intent_path = (
                    self._background_rollover_v2_intent_path(
                        checked, batch["batch_id"]
                    )
                )
                recovery_intent = self._read_json(
                    recovery_intent_path,
                    "subject_background_rollover_v2_intent",
                )
                if recovery_intent is not None:
                    intent = self._validate_background_rollover_receipt_v2(
                        recovery_intent
                    )
                    if (
                        intent.get("old_batch_sha256") != batch_sha256
                        or intent.get("next_generation") != next_generation
                        or intent.get("next_authority_fingerprint")
                        != next_authority_fingerprint
                        or intent.get(
                            "original_preclaim_failure_receipt_sha256"
                        )
                        != original_sha256
                        or intent.get("preserved_queue_entry_sha256")
                        != queue_entry_sha256
                        or intent.get("canary_state_before_sha256")
                        != _value_sha256(canary_before)
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_intent_conflict"
                        )
                    writer_sha256 = _value_sha256(writer)
                    if writer_sha256 not in {
                        intent["writer_state_before_sha256"],
                        intent["writer_state_after_sha256"],
                    }:
                        raise SubjectSolContractError(
                            "subject_background_rollover_crash_window_conflict"
                        )
                    prior_pointer = intent[
                        "background_rollover_pointer_before"
                    ]
                    if existing_pointer is not None and (
                        prior_pointer is None
                        or _value_sha256(existing_pointer)
                        != _value_sha256(prior_pointer)
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_crash_window_conflict"
                        )
                    receipt_sha256, receipt_path = self._publish_immutable(
                        self.background_rollover_receipt_root, intent
                    )
                    if (
                        writer_sha256
                        == intent["writer_state_before_sha256"]
                    ):
                        self._atomic_json(
                            self._writer_path(checked),
                            intent["writer_state_after"],
                        )
                    recovered_writer = self._read_writer_locked(checked)
                    if (
                        _value_sha256(recovered_writer)
                        != intent["writer_state_after_sha256"]
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_crash_window_conflict"
                        )
                    rollover_pointer = self._seal(
                        {
                            "schema_version": "subject_background_luna_rollover_pointer_v1",
                            "subject": checked,
                            "old_batch_id": batch["batch_id"],
                            "old_batch_sha256": batch_sha256,
                            "rollover_id": intent["rollover_id"],
                            "rollover_receipt_sha256": receipt_sha256,
                            "rollover_receipt_path": str(receipt_path),
                            "writer_revision_after": recovered_writer[
                                "revision"
                            ],
                            "applied_at": _utc_now(),
                            "formal_write_count": 0,
                        },
                        purpose="subject-background-luna-rollover-pointer",
                    )
                    self._atomic_json(
                        self._background_rollover_pointer_path(checked),
                        rollover_pointer,
                    )
                    return {
                        "receipt": intent,
                        "receipt_sha256": receipt_sha256,
                        "receipt_path": str(receipt_path),
                        "writer_state": recovered_writer,
                        "idempotent": True,
                    }
                if (
                    batch.get("status") != "frozen"
                    or batch.get("all_terminal") is not True
                    or batch.get("sol_ready") is not False
                    or writer.get("handoff_status") != "awaiting_luna"
                    or writer.get("batch_id") != batch.get("batch_id")
                    or writer.get("formal_write_count") != 0
                ):
                    raise SubjectSolContractError(
                        "subject_background_rollover_failure_not_closed"
                    )
                pointer = self._read_json(
                    self._batch_pointer_path(checked),
                    "subject_luna_batch_pointer",
                )
                if pointer is None:
                    raise SubjectSolContractError(
                        "subject_luna_batch_pointer_missing"
                    )
                snapshot_sha256 = _sha256(
                    pointer.get("snapshot_sha256"), "batch_snapshot_sha256"
                )
                snapshot_path = (
                    self.subject_batch_snapshot_root
                    / "sha256"
                    / snapshot_sha256[:2]
                    / f"{snapshot_sha256}.json"
                )
                archive = self._seal(
                    {
                        "schema_version": "subject_background_luna_batch_archive_v1",
                        "subject": checked,
                        "batch_id": batch["batch_id"],
                        "batch_sha256": batch_sha256,
                        "batch_snapshot_sha256": snapshot_sha256,
                        "batch": batch,
                        "authority_generation": batch["authority_generation"],
                        "authority_fingerprint": batch["authority_fingerprint"],
                        "archived_at": _utc_now(),
                        "sol_called": False,
                        "sol_enabled": False,
                        "formal_write_count": 0,
                    },
                    purpose="subject-background-luna-batch-archive",
                )
                archive_sha256, archive_path = self._publish_immutable(
                    self.subject_batch_archive_root / "background", archive
                )
                references = [
                    {
                        key: row.get(key)
                        for key in (
                            "capture_id",
                            "unit_sha256",
                            "frozen_payload_sha256",
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
                            "warning_codes",
                            "error_code",
                        )
                    }
                    for row in batch["tasks"]
                ]
                statuses = [str(row["status"]) for row in batch["tasks"]]
                if batch.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA:
                    status_counts = {
                        "quality_passed": sum(
                            status in TASK_V2_SOL_CANDIDATE_STATUSES
                            for status in statuses
                        ),
                        "needs_rework": statuses.count("workflow_partial"),
                        "failed": sum(
                            status in {"execution_failed", "cancelled", "stalled"}
                            for status in statuses
                        ),
                        "evidence_pending": 0,
                    }
                else:
                    status_counts = {
                        status: statuses.count(status)
                        for status in sorted(TASK_TERMINAL_STATUSES)
                    }
                writer_before = copy.deepcopy(writer)
                transition_at = _utc_now()
                writer_after = copy.deepcopy(writer)
                writer_after.update(
                    {
                        "revision": writer["revision"] + 1,
                        "handoff_status": "awaiting_luna",
                        "batch_id": None,
                        "authorization_receipt_sha256": None,
                        "daily_sol_batch_sha256": None,
                        "review_receipt_sha256": None,
                        "commit_receipt_sha256": None,
                        "generation_fence": {
                            "blocked": False,
                            "source_generation": batch["authority_generation"],
                            "next_generation": next_generation,
                        },
                        "updated_at": transition_at,
                    }
                )
                prior_rollover_pointer = copy.deepcopy(existing_pointer)
                prior_rollover_pointer_sha256 = (
                    _value_sha256(prior_rollover_pointer)
                    if prior_rollover_pointer is not None
                    else None
                )
                rollover_identity = {
                    "mode": "explicit_failure_resume",
                    "subject": checked,
                    "old_batch_id": batch["batch_id"],
                    "old_batch_sha256": batch_sha256,
                    "source_generation": batch["authority_generation"],
                    "next_generation": next_generation,
                    "original_preclaim_failure_receipt_sha256": original_sha256,
                    "preserved_queue_entry_sha256": queue_entry_sha256,
                    "preserved_task": preserved,
                }
                rollover_id = _value_sha256(rollover_identity)
                intent_path = self._background_rollover_v2_intent_path(
                    checked, batch["batch_id"]
                )
                intent = self._read_json(
                    intent_path, "subject_background_rollover_v2_intent"
                )
                if intent is None:
                    rollback_core = {
                        "rollover_id": rollover_id,
                        "old_batch_sha256": batch_sha256,
                        "writer_state_before_sha256": _value_sha256(
                            writer_before
                        ),
                        "writer_state_after_sha256": _value_sha256(
                            writer_after
                        ),
                        "batch_pointer_before_sha256": _value_sha256(pointer),
                        "background_rollover_pointer_before_sha256": (
                            prior_rollover_pointer_sha256
                        ),
                        "preserved_queue_entry_sha256": queue_entry_sha256,
                        "canary_state_before_sha256": _value_sha256(
                            canary_before
                        ),
                    }
                    receipt_core = {
                        "schema_version": (
                            SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
                        ),
                        "rollover_id": rollover_id,
                        "mode": "explicit_failure_resume",
                        "subject": checked,
                        "old_batch_id": batch["batch_id"],
                        "old_batch_sha256": batch_sha256,
                        "old_batch_snapshot_sha256": snapshot_sha256,
                        "old_batch_snapshot_path": str(snapshot_path),
                        "old_batch_revision": batch["revision"],
                        "archive_sha256": archive_sha256,
                        "archive_path": str(archive_path),
                        "task_set_sha256": _value_sha256(batch["tasks"]),
                        "task_count": len(batch["tasks"]),
                        "terminal_status_counts": status_counts,
                        "quality_receipt_sha256s": sorted(
                            row.get("quality_receipt_sha256")
                            for row in batch["tasks"]
                            if row.get("quality_receipt_sha256") is not None
                        ),
                        "terminal_receipt_sha256s": sorted(
                            row["terminal_receipt_sha256"]
                            for row in batch["tasks"]
                            if row["terminal_receipt_sha256"] is not None
                        ),
                        "evidence_reference_set_sha256": _value_sha256(references),
                        "source_generation": batch["authority_generation"],
                        "source_authority_fingerprint": batch[
                            "authority_fingerprint"
                        ],
                        "next_generation": next_generation,
                        "next_authority_fingerprint": (
                            next_authority_fingerprint
                        ),
                        "original_preclaim_failure_receipt_sha256": original_sha256,
                        "original_preclaim_failure_receipt_path": str(original_path),
                        "preserved_queue_entry_sha256": queue_entry_sha256,
                        "preserved_queue_identity": queue_identity,
                        "canary_state_before": canary_before,
                        "canary_state_before_sha256": _value_sha256(
                            canary_before
                        ),
                        "subsequent_attempt_receipt_sha256s": attempts,
                        "preserved_task": preserved,
                        "writer_revision_before": writer["revision"],
                        "writer_revision_after": writer["revision"] + 1,
                        "writer_state_before": writer_before,
                        "writer_state_before_sha256": _value_sha256(
                            writer_before
                        ),
                        "writer_state_after": writer_after,
                        "writer_state_after_sha256": _value_sha256(writer_after),
                        "batch_pointer_before": pointer,
                        "batch_pointer_before_sha256": _value_sha256(pointer),
                        "background_rollover_pointer_before": (
                            prior_rollover_pointer
                        ),
                        "background_rollover_pointer_before_sha256": (
                            prior_rollover_pointer_sha256
                        ),
                        "rollback_token": _value_sha256(rollback_core),
                        "sol_called": False,
                        "sol_enabled": False,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "mcp_tool_call_count": 0,
                        "formal_write_count": 0,
                        "created_at": _utc_now(),
                    }
                    intent = self._seal(
                        receipt_core,
                        purpose="subject-background-luna-rollover-v2",
                    )
                    self._atomic_json(intent_path, intent)
                else:
                    checked_intent = self._validate_background_rollover_receipt_v2(
                        intent
                    )
                    if checked_intent.get("rollover_id") != rollover_id:
                        raise SubjectSolContractError(
                            "subject_background_rollover_intent_conflict"
                        )
                receipt_sha256, receipt_path = self._publish_immutable(
                    self.background_rollover_receipt_root, intent
                )
                writer = self._read_writer_projection(
                    dict(intent["writer_state_after"])
                )
                self._atomic_json(self._writer_path(checked), writer)
                if writer["revision"] != intent["writer_revision_after"]:
                    raise SubjectSolContractError(
                        "subject_background_rollover_writer_revision_mismatch"
                    )
                rollover_pointer = self._seal(
                    {
                        "schema_version": "subject_background_luna_rollover_pointer_v1",
                        "subject": checked,
                        "old_batch_id": batch["batch_id"],
                        "old_batch_sha256": batch_sha256,
                        "rollover_id": rollover_id,
                        "rollover_receipt_sha256": receipt_sha256,
                        "rollover_receipt_path": str(receipt_path),
                        "writer_revision_after": writer["revision"],
                        "applied_at": _utc_now(),
                        "formal_write_count": 0,
                    },
                    purpose="subject-background-luna-rollover-pointer",
                )
                self._atomic_json(
                    self._background_rollover_pointer_path(checked),
                    rollover_pointer,
                )
                return {
                    "receipt": intent,
                    "receipt_sha256": receipt_sha256,
                    "receipt_path": str(receipt_path),
                    "writer_state": writer,
                    "idempotent": False,
                }

    def reopen_background_luna_batch_v2_rollback(
        self,
        *,
        recovery_receipt_path: Path,
        rollback_receipt_path: Path,
    ) -> dict[str, Any]:
        """Read-only reopen of the one English recovery rollback postimage."""

        recovery_path = recovery_receipt_path.resolve()
        rollback_path = rollback_receipt_path.resolve()
        try:
            recovery_path.relative_to(
                self.background_rollover_receipt_root.resolve()
            )
            rollback_path.relative_to(
                self.background_rollover_rollback_receipt_root.resolve()
            )
            recovery_payload = recovery_path.read_bytes()
            rollback_payload = rollback_path.read_bytes()
        except (OSError, ValueError) as exc:
            raise SubjectSolContractError(
                "subject_background_rollback_receipt_path_invalid"
            ) from exc
        recovery_sha256 = hashlib.sha256(recovery_payload).hexdigest()
        rollback_sha256 = hashlib.sha256(rollback_payload).hexdigest()
        expected_recovery_path = (
            self.background_rollover_receipt_root
            / "sha256"
            / recovery_sha256[:2]
            / f"{recovery_sha256}.json"
        )
        expected_rollback_path = (
            self.background_rollover_rollback_receipt_root
            / "sha256"
            / rollback_sha256[:2]
            / f"{rollback_sha256}.json"
        )
        if (
            recovery_path != expected_recovery_path
            or rollback_path != expected_rollback_path
        ):
            raise SubjectSolContractError(
                "subject_background_rollback_receipt_path_invalid"
            )
        recovery = self._read_background_rollover_receipt(recovery_sha256)
        if (
            recovery.get("schema_version")
            != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
            or recovery.get("subject") != "english"
        ):
            raise SubjectSolContractError(
                "subject_background_recovery_receipt_not_v2"
            )
        rollback = self._read_content_addressed(
            self.background_rollover_rollback_receipt_root,
            rollback_sha256,
            "subject_background_rollback_receipt",
        )
        required = {
            "schema_version",
            "subject",
            "recovery_receipt_sha256",
            "recovery_receipt_path",
            "rollback_token",
            "writer_state_after_sha256",
            "writer_state_restored_sha256",
            "batch_pointer_restored_sha256",
            "background_rollover_pointer_restored_sha256",
            "preserved_queue_entry_sha256",
            "model_call_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "formal_write_count",
            "sol_enabled",
            "rolled_back_at",
            "seal",
        }
        if (
            set(rollback) != required
            or rollback.get("schema_version")
            != "subject_background_luna_recovery_rollback_receipt_v1"
            or rollback.get("subject") != "english"
            or rollback.get("recovery_receipt_sha256") != recovery_sha256
            or rollback.get("recovery_receipt_path") != str(recovery_path)
            or rollback.get("rollback_token") != recovery["rollback_token"]
            or rollback.get("writer_state_after_sha256")
            != recovery["writer_state_after_sha256"]
            or rollback.get("writer_state_restored_sha256")
            != recovery["writer_state_before_sha256"]
            or rollback.get("batch_pointer_restored_sha256")
            != recovery["batch_pointer_before_sha256"]
            or rollback.get("background_rollover_pointer_restored_sha256")
            != recovery["background_rollover_pointer_before_sha256"]
            or rollback.get("preserved_queue_entry_sha256")
            != recovery["preserved_queue_entry_sha256"]
            or any(
                rollback.get(key) != 0
                for key in (
                    "model_call_count",
                    "provider_request_count",
                    "mcp_tool_call_count",
                    "formal_write_count",
                )
            )
            or rollback.get("sol_enabled") is not False
        ):
            raise SubjectSolContractError(
                "subject_background_rollback_receipt_invalid"
            )
        _timestamp(rollback.get("rolled_back_at"), "rollback_rolled_back_at")
        self._verify_seal(
            rollback, purpose="subject-background-luna-rollback-v1"
        )
        with _ExistingFileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active_writer = global_state.get("active_writer")
            if (
                (
                    isinstance(active_writer, Mapping)
                    and active_writer.get("subject") == "english"
                )
                or any(
                    row.get("subject") == "english"
                    and row.get("status") in {"queued", "active", "reviewed"}
                    for row in global_state["queue"]
                )
                or global_state.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError(
                    "subject_background_rollback_postimage_mismatch"
                )
            with _ExistingFileLock(self._subject_lock_path("english")):
                batch = self._read_batch_locked("english")
                writer = self._read_writer_locked("english")
                pointer = self._read_json(
                    self._batch_pointer_path("english"),
                    "subject_luna_batch_pointer",
                )
                rollover_pointer = (
                    self._read_background_rollover_pointer_locked("english")
                )
                prior_rollover = recovery[
                    "background_rollover_pointer_before"
                ]
                rollover_matches = bool(
                    (prior_rollover is None and rollover_pointer is None)
                    or (
                        prior_rollover is not None
                        and rollover_pointer is not None
                        and _value_sha256(rollover_pointer)
                        == recovery[
                            "background_rollover_pointer_before_sha256"
                        ]
                    )
                )
                if (
                    batch is None
                    or _document_sha256(batch)
                    != recovery["old_batch_sha256"]
                    or _value_sha256(writer)
                    != recovery["writer_state_before_sha256"]
                    or pointer is None
                    or _value_sha256(pointer)
                    != recovery["batch_pointer_before_sha256"]
                    or not rollover_matches
                    or self._background_rollover_v2_intent_path(
                        "english", str(recovery["old_batch_id"])
                    ).exists()
                    or self._background_rollover_v2_intent_path(
                        "english", str(recovery["old_batch_id"])
                    ).is_symlink()
                    or self._background_rollover_rollback_intent_path(
                        recovery_sha256
                    ).exists()
                    or self._background_rollover_rollback_intent_path(
                        recovery_sha256
                    ).is_symlink()
                ):
                    raise SubjectSolContractError(
                        "subject_background_rollback_postimage_mismatch"
                    )
        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-sol-rollback-reopen-result-v1"
            ),
            "subject": "english",
            "status": "reopened",
            "recovery_receipt_sha256": recovery_sha256,
            "recovery_receipt_path": str(recovery_path),
            "rollback_receipt_sha256": rollback_sha256,
            "rollback_receipt_path": str(rollback_path),
            "rollback_token": recovery["rollback_token"],
            "writer_state_restored_sha256": recovery[
                "writer_state_before_sha256"
            ],
            "batch_pointer_restored_sha256": recovery[
                "batch_pointer_before_sha256"
            ],
            "preserved_queue_entry_sha256": recovery[
                "preserved_queue_entry_sha256"
            ],
            "mutable_subject_sol_intents_withdrawn": True,
            "rollback_receipt_reopened": True,
            "rollback_reopen_read_only": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def rollback_background_luna_batch_v2(
        self, *, recovery_receipt_path: Path
    ) -> dict[str, Any]:
        """Compensate a v2 recovery when every mutable postimage still matches."""

        path = recovery_receipt_path.resolve()
        try:
            path.relative_to(self.background_rollover_receipt_root.resolve())
            payload = path.read_bytes()
        except (OSError, ValueError) as exc:
            raise SubjectSolContractError(
                "subject_background_recovery_receipt_path_invalid"
            ) from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        receipt = self._read_background_rollover_receipt(receipt_sha256)
        if (
            receipt.get("schema_version")
            != SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_V2_SCHEMA
        ):
            raise SubjectSolContractError(
                "subject_background_recovery_receipt_not_v2"
            )
        subject = str(receipt["subject"])
        rollback_intent_path = self._background_rollover_rollback_intent_path(
            receipt_sha256
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active_writer = global_state.get("active_writer")
            if (
                isinstance(active_writer, Mapping)
                and active_writer.get("subject") == subject
            ) or any(
                row.get("subject") == subject
                and row.get("status") in {"queued", "active", "reviewed"}
                for row in global_state["queue"]
            ):
                raise SubjectSolContractError(
                    "subject_background_rollback_sol_state_present"
                )
            with _FileLock(self._subject_lock_path(subject)):
                batch = self._read_batch_locked(subject)
                pointer = self._read_json(
                    self._batch_pointer_path(subject),
                    "subject_luna_batch_pointer",
                )
                writer = self._read_writer_locked(subject)
                writer_sha256 = _value_sha256(writer)
                pointer_sha256 = (
                    _value_sha256(pointer) if pointer is not None else None
                )
                before_writer_sha256 = receipt["writer_state_before_sha256"]
                after_writer_sha256 = receipt["writer_state_after_sha256"]
                current_rollover_pointer = (
                    self._read_background_rollover_pointer_locked(subject)
                )
                prior_rollover_pointer = receipt[
                    "background_rollover_pointer_before"
                ]
                prior_rollover_sha256 = receipt[
                    "background_rollover_pointer_before_sha256"
                ]
                already_rolled_back = bool(
                    batch is not None
                    and _document_sha256(batch) == receipt["old_batch_sha256"]
                    and writer_sha256 == before_writer_sha256
                    and pointer_sha256 == receipt["batch_pointer_before_sha256"]
                    and (
                        (
                            prior_rollover_pointer is None
                            and current_rollover_pointer is None
                        )
                        or (
                            prior_rollover_pointer is not None
                            and current_rollover_pointer is not None
                            and _value_sha256(current_rollover_pointer)
                            == prior_rollover_sha256
                        )
                    )
                )

                def verified_restored_writer() -> dict[str, Any]:
                    restored_batch = self._read_batch_locked(subject)
                    restored_pointer = self._read_json(
                        self._batch_pointer_path(subject),
                        "subject_luna_batch_pointer",
                    )
                    restored_writer = self._read_writer_locked(subject)
                    restored_rollover = (
                        self._read_background_rollover_pointer_locked(
                            subject
                        )
                    )
                    restored_rollover_matches = bool(
                        (
                            prior_rollover_pointer is None
                            and restored_rollover is None
                        )
                        or (
                            prior_rollover_pointer is not None
                            and restored_rollover is not None
                            and _value_sha256(restored_rollover)
                            == prior_rollover_sha256
                        )
                    )
                    if (
                        restored_batch is None
                        or _document_sha256(restored_batch)
                        != receipt["old_batch_sha256"]
                        or _value_sha256(restored_writer)
                        != before_writer_sha256
                        or restored_pointer is None
                        or _value_sha256(restored_pointer)
                        != receipt["batch_pointer_before_sha256"]
                        or not restored_rollover_matches
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollback_restore_failed"
                        )
                    return restored_writer

                def retire_mutable_intents() -> None:
                    self._retire_background_rollover_v2_intents(
                        subject=subject,
                        batch_id=str(receipt["old_batch_id"]),
                        recovery_receipt_sha256=receipt_sha256,
                    )

                existing_rollback = self._read_json(
                    rollback_intent_path,
                    "subject_background_rollback_intent",
                )
                if existing_rollback is not None:
                    self._verify_seal(
                        existing_rollback,
                        purpose="subject-background-luna-rollback-v1",
                    )
                    if (
                        existing_rollback.get("schema_version")
                        != "subject_background_luna_recovery_rollback_receipt_v1"
                        or existing_rollback.get("subject") != subject
                        or existing_rollback.get("recovery_receipt_sha256")
                        != receipt_sha256
                        or existing_rollback.get("rollback_token")
                        != receipt["rollback_token"]
                        or batch is None
                        or _document_sha256(batch)
                        != receipt["old_batch_sha256"]
                        or writer_sha256
                        not in {before_writer_sha256, after_writer_sha256}
                        or pointer_sha256
                        != receipt["batch_pointer_before_sha256"]
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollback_intent_conflict"
                        )
                    current_rollover_is_after = bool(
                        current_rollover_pointer is not None
                        and current_rollover_pointer.get("rollover_id")
                        == receipt["rollover_id"]
                        and current_rollover_pointer.get(
                            "rollover_receipt_sha256"
                        )
                        == receipt_sha256
                    )
                    current_rollover_is_before = bool(
                        (
                            prior_rollover_pointer is None
                            and current_rollover_pointer is None
                        )
                        or (
                            prior_rollover_pointer is not None
                            and current_rollover_pointer is not None
                            and _value_sha256(current_rollover_pointer)
                            == prior_rollover_sha256
                        )
                    )
                    if not (
                        current_rollover_is_after
                        or current_rollover_is_before
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollback_intent_conflict"
                        )
                    self._atomic_json(
                        self._writer_path(subject),
                        receipt["writer_state_before"],
                    )
                    self._atomic_json(
                        self._batch_pointer_path(subject),
                        receipt["batch_pointer_before"],
                    )
                    rollover_pointer_path = (
                        self._background_rollover_pointer_path(subject)
                    )
                    if prior_rollover_pointer is None:
                        try:
                            rollover_pointer_path.unlink()
                        except FileNotFoundError:
                            pass
                    else:
                        self._atomic_json(
                            rollover_pointer_path, prior_rollover_pointer
                        )
                    restored = verified_restored_writer()
                    rollback_sha256, rollback_path = self._publish_immutable(
                        self.background_rollover_rollback_receipt_root,
                        existing_rollback,
                    )
                    retire_mutable_intents()
                    return {
                        "rollback_receipt": existing_rollback,
                        "rollback_receipt_sha256": rollback_sha256,
                        "rollback_receipt_path": str(rollback_path),
                        "recovery_receipt_sha256": receipt_sha256,
                        "writer_state": restored,
                        "idempotent": True,
                        "formal_write_count": 0,
                    }
                if already_rolled_back:
                    immutable = self._immutable_background_rollback_receipt(
                        recovery_receipt_sha256=receipt_sha256,
                        rollback_token=str(receipt["rollback_token"]),
                    )
                    if immutable is None:
                        raise SubjectSolContractError(
                            "subject_background_rollback_proof_missing"
                        )
                    immutable_receipt, rollback_sha256, rollback_path = (
                        immutable
                    )
                    restored = verified_restored_writer()
                    retire_mutable_intents()
                    return {
                        "rollback_receipt": immutable_receipt,
                        "rollback_receipt_sha256": rollback_sha256,
                        "rollback_receipt_path": str(rollback_path),
                        "recovery_receipt_sha256": receipt_sha256,
                        "writer_state": restored,
                        "idempotent": True,
                        "formal_write_count": 0,
                    }
                if (
                    batch is None
                    or _document_sha256(batch) != receipt["old_batch_sha256"]
                    or writer_sha256 != after_writer_sha256
                    or pointer_sha256 != receipt["batch_pointer_before_sha256"]
                    or current_rollover_pointer is None
                    or current_rollover_pointer.get("rollover_id")
                    != receipt["rollover_id"]
                    or current_rollover_pointer.get(
                        "rollover_receipt_sha256"
                    )
                    != receipt_sha256
                    or current_rollover_pointer.get("writer_revision_after")
                    != receipt["writer_revision_after"]
                ):
                    raise SubjectSolContractError(
                        "subject_background_rollback_postimage_mismatch"
                    )
                rollback_core = {
                    "schema_version": (
                        "subject_background_luna_recovery_rollback_receipt_v1"
                    ),
                    "subject": subject,
                    "recovery_receipt_sha256": receipt_sha256,
                    "recovery_receipt_path": str(path),
                    "rollback_token": receipt["rollback_token"],
                    "writer_state_after_sha256": after_writer_sha256,
                    "writer_state_restored_sha256": before_writer_sha256,
                    "batch_pointer_restored_sha256": receipt[
                        "batch_pointer_before_sha256"
                    ],
                    "background_rollover_pointer_restored_sha256": (
                        prior_rollover_sha256
                    ),
                    "preserved_queue_entry_sha256": receipt[
                        "preserved_queue_entry_sha256"
                    ],
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "mcp_tool_call_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                    "rolled_back_at": _utc_now(),
                }
                rollback_receipt = self._seal(
                    rollback_core,
                    purpose="subject-background-luna-rollback-v1",
                )
                self._atomic_json(rollback_intent_path, rollback_receipt)
                self._atomic_json(
                    self._writer_path(subject), receipt["writer_state_before"]
                )
                self._atomic_json(
                    self._batch_pointer_path(subject),
                    receipt["batch_pointer_before"],
                )
                rollover_pointer_path = self._background_rollover_pointer_path(
                    subject
                )
                if prior_rollover_pointer is None:
                    try:
                        rollover_pointer_path.unlink()
                    except FileNotFoundError:
                        pass
                else:
                    self._atomic_json(
                        rollover_pointer_path, prior_rollover_pointer
                    )
                restored = verified_restored_writer()
                rollback_sha256, rollback_path = self._publish_immutable(
                    self.background_rollover_rollback_receipt_root,
                    rollback_receipt,
                )
                retire_mutable_intents()
                return {
                    "rollback_receipt": rollback_receipt,
                    "rollback_receipt_sha256": rollback_sha256,
                    "rollback_receipt_path": str(rollback_path),
                    "recovery_receipt_sha256": receipt_sha256,
                    "writer_state": restored,
                    "idempotent": False,
                    "formal_write_count": 0,
                }

    def rollover_background_luna_batch(
        self,
        subject: str,
        *,
        mode: str,
        resume_acceptance_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Archive a terminal background batch without invoking Sol or writing formal data."""

        checked = _subject(subject)
        if mode not in {"auto_success", "explicit_failure_resume"}:
            raise SubjectSolContractError("subject_background_rollover_mode_invalid")
        if mode == "explicit_failure_resume":
            resume_acceptance_sha256 = _sha256(
                resume_acceptance_sha256, "resume_acceptance_sha256"
            )
        elif resume_acceptance_sha256 is not None:
            raise SubjectSolContractError(
                "subject_background_rollover_resume_binding_invalid"
            )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active_writer = global_state.get("active_writer")
            if (
                isinstance(active_writer, Mapping)
                and active_writer.get("subject") == checked
            ) or any(
                row.get("subject") == checked
                and row.get("status") in {"queued", "active", "reviewed"}
                for row in global_state["queue"]
            ):
                raise SubjectSolContractError(
                    "subject_background_rollover_sol_state_present"
                )
            with _FileLock(self._subject_lock_path(checked)):
                batch = self._read_batch_locked(checked)
                writer = self._read_writer_locked(checked)
                if batch is None:
                    raise SubjectSolContractError("subject_luna_batch_not_found")
                batch_sha256 = _document_sha256(batch)
                existing_pointer = self._read_background_rollover_pointer_locked(
                    checked
                )
                if (
                    existing_pointer is not None
                    and existing_pointer.get("old_batch_id") == batch["batch_id"]
                    and existing_pointer.get("old_batch_sha256") == batch_sha256
                    and writer.get("handoff_status") == "awaiting_luna"
                    and writer.get("batch_id") is None
                ):
                    receipt = self._read_background_rollover_receipt(
                        str(existing_pointer["rollover_receipt_sha256"])
                    )
                    return {
                        "receipt": receipt,
                        "receipt_sha256": existing_pointer[
                            "rollover_receipt_sha256"
                        ],
                        "receipt_path": existing_pointer[
                            "rollover_receipt_path"
                        ],
                        "writer_state": writer,
                        "idempotent": True,
                    }
                existing_intent = self._read_json(
                    self._background_rollover_intent_path(
                        checked, batch["batch_id"]
                    ),
                    "subject_background_rollover_intent",
                )
                if (
                    existing_intent is not None
                    and writer.get("handoff_status") == "awaiting_luna"
                    and writer.get("batch_id") is None
                ):
                    self._verify_seal(
                        existing_intent,
                        purpose="subject-background-luna-rollover",
                    )
                    if (
                        existing_intent.get("old_batch_id")
                        != batch["batch_id"]
                        or existing_intent.get("old_batch_sha256")
                        != batch_sha256
                        or existing_intent.get("mode") != mode
                        or existing_intent.get("resume_acceptance_sha256")
                        != resume_acceptance_sha256
                        or existing_intent.get("writer_revision_after")
                        != writer.get("revision")
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_intent_conflict"
                        )
                    receipt_sha256, receipt_path = self._publish_immutable(
                        self.background_rollover_receipt_root,
                        existing_intent,
                    )
                    applied_at = _utc_now()
                    rollover_pointer = self._seal(
                        {
                            "schema_version": "subject_background_luna_rollover_pointer_v1",
                            "subject": checked,
                            "old_batch_id": batch["batch_id"],
                            "old_batch_sha256": batch_sha256,
                            "rollover_id": existing_intent["rollover_id"],
                            "rollover_receipt_sha256": receipt_sha256,
                            "rollover_receipt_path": str(receipt_path),
                            "writer_revision_after": writer["revision"],
                            "applied_at": applied_at,
                            "formal_write_count": 0,
                        },
                        purpose="subject-background-luna-rollover-pointer",
                    )
                    self._atomic_json(
                        self._background_rollover_pointer_path(checked),
                        rollover_pointer,
                    )
                    return {
                        "receipt": existing_intent,
                        "receipt_sha256": receipt_sha256,
                        "receipt_path": str(receipt_path),
                        "writer_state": writer,
                        "idempotent": True,
                    }
                if batch.get("status") != "frozen" or batch.get("all_terminal") is not True:
                    raise SubjectSolContractError(
                        "subject_background_rollover_batch_not_terminal"
                    )
                statuses = [str(row["status"]) for row in batch["tasks"]]
                is_v2_batch = (
                    batch.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA
                )
                if mode == "auto_success":
                    if (
                        batch.get("sol_ready") is not True
                        or (
                            not is_v2_batch
                            and set(statuses) != {"quality_passed"}
                        )
                        or (
                            is_v2_batch
                            and not any(
                                status in TASK_V2_SOL_CANDIDATE_STATUSES
                                for status in statuses
                            )
                        )
                        or writer.get("handoff_status") != "ready_for_authorization"
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_success_not_closed"
                        )
                elif (
                    batch.get("sol_ready") is not False
                    or (
                        not is_v2_batch
                        and not any(
                            status != "quality_passed" for status in statuses
                        )
                    )
                    or (
                        is_v2_batch
                        and any(
                            row.get("status")
                            in TASK_V2_SOL_CANDIDATE_STATUSES
                            and row.get("sol_handoff_envelope_sha256")
                            is not None
                            for row in batch["tasks"]
                        )
                    )
                    or writer.get("handoff_status") != "awaiting_luna"
                ):
                    raise SubjectSolContractError(
                        "subject_background_rollover_failure_not_closed"
                    )
                if (
                    writer.get("batch_id") != batch["batch_id"]
                    or writer.get("formal_write_count") != 0
                    or any(
                        writer.get(key) is not None
                        for key in (
                            "authorization_receipt_sha256",
                            "daily_sol_batch_sha256",
                            "review_receipt_sha256",
                            "commit_receipt_sha256",
                        )
                    )
                ):
                    raise SubjectSolContractError(
                        "subject_background_rollover_writer_invalid"
                    )
                pointer = self._read_json(
                    self._batch_pointer_path(checked),
                    "subject_luna_batch_pointer",
                )
                if pointer is None:
                    raise SubjectSolContractError(
                        "subject_luna_batch_pointer_missing"
                    )
                snapshot_sha256 = _sha256(
                    pointer.get("snapshot_sha256"), "batch_snapshot_sha256"
                )
                snapshot_path = (
                    self.subject_batch_snapshot_root
                    / "sha256"
                    / snapshot_sha256[:2]
                    / f"{snapshot_sha256}.json"
                )
                archive = self._seal(
                    {
                        "schema_version": "subject_background_luna_batch_archive_v1",
                        "subject": checked,
                        "batch_id": batch["batch_id"],
                        "batch_sha256": batch_sha256,
                        "batch_snapshot_sha256": snapshot_sha256,
                        "batch": batch,
                        "authority_generation": batch["authority_generation"],
                        "authority_fingerprint": batch["authority_fingerprint"],
                        "archived_at": _utc_now(),
                        "sol_called": False,
                        "sol_enabled": False,
                        "formal_write_count": 0,
                    },
                    purpose="subject-background-luna-batch-archive",
                )
                archive_sha256, archive_path = self._publish_immutable(
                    self.subject_batch_archive_root / "background", archive
                )
                references = [
                    {
                        key: row.get(key)
                        for key in (
                            "capture_id",
                            "unit_sha256",
                            "frozen_payload_sha256",
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
                            "warning_codes",
                            "error_code",
                        )
                    }
                    for row in batch["tasks"]
                ]
                if is_v2_batch:
                    # The rollover receipt is a retained v1 audit envelope.
                    # Keep its historical four count buckets stable while
                    # projecting successor execution outcomes into them.
                    status_counts = {
                        "quality_passed": sum(
                            status in TASK_V2_SOL_CANDIDATE_STATUSES
                            for status in statuses
                        ),
                        "needs_rework": statuses.count("workflow_partial"),
                        "failed": sum(
                            status in {"execution_failed", "cancelled", "stalled"}
                            for status in statuses
                        ),
                        "evidence_pending": 0,
                    }
                else:
                    status_counts = {
                        status: statuses.count(status)
                        for status in sorted(TASK_TERMINAL_STATUSES)
                    }
                rollover_identity = {
                    "mode": mode,
                    "subject": checked,
                    "old_batch_id": batch["batch_id"],
                    "old_batch_sha256": batch_sha256,
                    "resume_acceptance_sha256": resume_acceptance_sha256,
                }
                rollover_id = _value_sha256(rollover_identity)
                intent_path = self._background_rollover_intent_path(
                    checked, batch["batch_id"]
                )
                intent = self._read_json(
                    intent_path, "subject_background_rollover_intent"
                )
                if intent is None:
                    receipt_core = {
                        "schema_version": SUBJECT_BACKGROUND_ROLLOVER_RECEIPT_SCHEMA,
                        "rollover_id": rollover_id,
                        "mode": mode,
                        "subject": checked,
                        "old_batch_id": batch["batch_id"],
                        "old_batch_sha256": batch_sha256,
                        "old_batch_snapshot_sha256": snapshot_sha256,
                        "old_batch_snapshot_path": str(snapshot_path),
                        "old_batch_revision": batch["revision"],
                        "archive_sha256": archive_sha256,
                        "archive_path": str(archive_path),
                        "task_set_sha256": _value_sha256(batch["tasks"]),
                        "task_count": len(batch["tasks"]),
                        "terminal_status_counts": status_counts,
                        "quality_receipt_sha256s": sorted(
                            row.get("quality_receipt_sha256")
                            for row in batch["tasks"]
                            if row.get("quality_receipt_sha256") is not None
                        ),
                        "terminal_receipt_sha256s": sorted(
                            row["terminal_receipt_sha256"]
                            for row in batch["tasks"]
                            if row["terminal_receipt_sha256"] is not None
                        ),
                        "evidence_reference_set_sha256": _value_sha256(references),
                        "authority_generation": batch["authority_generation"],
                        "authority_fingerprint": batch["authority_fingerprint"],
                        "next_generation": batch["authority_generation"],
                        "writer_revision_before": writer["revision"],
                        "writer_revision_after": writer["revision"] + 1,
                        "resume_acceptance_sha256": resume_acceptance_sha256,
                        "sol_called": False,
                        "sol_enabled": False,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "formal_write_count": 0,
                        "created_at": _utc_now(),
                    }
                    intent = self._seal(
                        receipt_core, purpose="subject-background-luna-rollover"
                    )
                    self._atomic_json(intent_path, intent)
                else:
                    self._verify_seal(
                        intent, purpose="subject-background-luna-rollover"
                    )
                    if (
                        intent.get("rollover_id") != rollover_id
                        or intent.get("old_batch_sha256") != batch_sha256
                    ):
                        raise SubjectSolContractError(
                            "subject_background_rollover_intent_conflict"
                        )
                receipt_sha256, receipt_path = self._publish_immutable(
                    self.background_rollover_receipt_root, intent
                )
                if not (
                    writer.get("handoff_status") == "awaiting_luna"
                    and writer.get("batch_id") is None
                    and writer.get("revision") == intent["writer_revision_after"]
                ):
                    writer.update(
                        {
                            "handoff_status": "awaiting_luna",
                            "batch_id": None,
                            "authorization_receipt_sha256": None,
                            "daily_sol_batch_sha256": None,
                            "review_receipt_sha256": None,
                            "commit_receipt_sha256": None,
                            "generation_fence": {
                                "blocked": False,
                                "source_generation": batch[
                                    "authority_generation"
                                ],
                                "next_generation": batch[
                                    "authority_generation"
                                ],
                            },
                        }
                    )
                    writer = self._write_writer_locked(writer)
                if writer["revision"] != intent["writer_revision_after"]:
                    raise SubjectSolContractError(
                        "subject_background_rollover_writer_revision_mismatch"
                    )
                applied_at = _utc_now()
                rollover_pointer = self._seal(
                    {
                        "schema_version": "subject_background_luna_rollover_pointer_v1",
                        "subject": checked,
                        "old_batch_id": batch["batch_id"],
                        "old_batch_sha256": batch_sha256,
                        "rollover_id": rollover_id,
                        "rollover_receipt_sha256": receipt_sha256,
                        "rollover_receipt_path": str(receipt_path),
                        "writer_revision_after": writer["revision"],
                        "applied_at": applied_at,
                        "formal_write_count": 0,
                    },
                    purpose="subject-background-luna-rollover-pointer",
                )
                self._atomic_json(
                    self._background_rollover_pointer_path(checked),
                    rollover_pointer,
                )
                return {
                    "receipt": intent,
                    "receipt_sha256": receipt_sha256,
                    "receipt_path": str(receipt_path),
                    "writer_state": writer,
                    "idempotent": False,
                }

    def prepare_and_freeze_subject_batch(
        self,
        *,
        subject: str,
        batch_id: str,
        study_date: str,
        capture_high_watermark: str,
        authority_generation: str,
        authority_fingerprint: str,
        tasks: Sequence[Mapping[str, Any]],
        scan_snapshot_sha256: str,
        exact_math_migration_commit: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically publish the exact expected task set before model work."""

        checked_subject = _subject(subject)
        checked_date = _nonempty(study_date, "study_date")
        rows: list[dict[str, Any]] = []
        for raw in tasks:
            row = dict(_mapping(raw, "prepared_subject_luna_task"))
            if set(row) != {
                "capture_id", "unit_sha256", "input_fingerprint", "study_date",
                "frozen_payload_sha256",
            }:
                raise SubjectSolContractError("prepared_subject_luna_task_shape_invalid")
            task_date = _nonempty(row.get("study_date"), "task_study_date")
            rows.append(
                {
                    "capture_id": _nonempty(row.get("capture_id"), "capture_id"),
                    "unit_sha256": _sha256(row.get("unit_sha256"), "unit_sha256"),
                    "input_fingerprint": _sha256(
                        row.get("input_fingerprint"), "input_fingerprint"
                    ),
                    "study_date": task_date,
                    "frozen_payload_sha256": _sha256(
                        row.get("frozen_payload_sha256"), "frozen_payload_sha256"
                    ),
                    "status": "selected",
                    "analysis_execution_receipt_sha256": None,
                    "analysis_raw_output_sha256": None,
                    "analysis_normalization_receipt_sha256": None,
                    "analysis_report_sha256": None,
                    "critical_review_execution_receipt_sha256": None,
                    "critical_review_raw_output_sha256": None,
                    "critical_review_normalization_receipt_sha256": None,
                    "critical_review_report_sha256": None,
                    "package_sha256": None,
                    "sol_handoff_envelope_sha256": None,
                    "warning_codes": [],
                    "terminal_receipt_sha256": None,
                    "error_code": None,
                }
            )
        if not rows:
            raise SubjectSolContractError("prepared_subject_luna_tasks_empty")
        value = {
            "schema_version": SUBJECT_BATCH_V2_SCHEMA,
            "batch_id": _nonempty(batch_id, "batch_id"),
            "subject": checked_subject,
            "study_date": checked_date,
            "status": "frozen",
            "capture_high_watermark": _nonempty(
                capture_high_watermark, "capture_high_watermark"
            ),
            "scan_snapshot_sha256": _sha256(
                scan_snapshot_sha256, "scan_snapshot_sha256"
            ),
            "authority_generation": _nonempty(
                authority_generation, "authority_generation"
            ),
            "authority_fingerprint": _sha256(
                authority_fingerprint, "authority_fingerprint"
            ),
            "tasks": rows,
            "exclusion_receipt_sha256s": [],
            "all_terminal": False,
            "sol_ready": False,
            "blocking_task_ids": [],
            "sol_candidate_task_ids": [],
            "diagnostic_task_ids": [],
            "formal_write_count": 0,
            "revision": -1,
            "updated_at": None,
        }
        migration_commit = (
            dict(exact_math_migration_commit)
            if exact_math_migration_commit is not None
            else None
        )
        migration_mappings: dict[str, Mapping[str, Any]] = {}
        if migration_commit is not None:
            raw_mappings = migration_commit.get("queue_mappings")
            if (
                checked_subject != "math"
                or migration_commit.get("schema_version")
                != "study-intake-math-pending-queue-migration-commit-v1"
                or migration_commit.get("task_count") != 4
                or migration_commit.get("target_authority_generation")
                != value["authority_generation"]
                or migration_commit.get(
                    "target_subject_authority_fingerprint"
                )
                != value["authority_fingerprint"]
                or migration_commit.get("formal_write_count") != 0
                or migration_commit.get("model_call_count") != 0
                or migration_commit.get("provider_request_count") != 0
                or migration_commit.get("sol_enabled") is not False
                or not isinstance(raw_mappings, list)
                or len(raw_mappings) != 4
            ):
                raise SubjectSolContractError(
                    "math_migration_batch_authority_invalid"
                )
            for raw_mapping in raw_mappings:
                mapping = _mapping(raw_mapping, "math_migration_queue_mapping")
                capture_id = _nonempty(
                    mapping.get("capture_event_id"), "capture_event_id"
                )
                if capture_id in migration_mappings:
                    raise SubjectSolContractError(
                        "math_migration_batch_authority_invalid"
                    )
                migration_mappings[capture_id] = mapping
            if set(migration_mappings) != {
                str(row["capture_id"]) for row in rows
            } or any(
                migration_mappings[str(row["capture_id"])].get(
                    "target_unit_sha256"
                )
                != row["unit_sha256"]
                or migration_mappings[str(row["capture_id"])].get(
                    "target_frozen_payload_sha256"
                )
                != row["frozen_payload_sha256"]
                for row in rows
            ):
                raise SubjectSolContractError(
                    "math_migration_batch_authority_invalid"
                )
        with _FileLock(self._subject_lock_path(checked_subject)):
            existing = self._read_batch_locked(checked_subject)
            migration_rebind = False
            archived_successor_replacement = False
            if existing is not None:
                candidate = copy.deepcopy(value)
                self._recompute_subject_batch(candidate)
                candidate["revision"] = existing["revision"]
                candidate["updated_at"] = existing["updated_at"]
                if candidate == existing:
                    return existing
                writer = self._read_writer_locked(checked_subject)
                fence = writer["generation_fence"]
                normal_replacement = (
                    writer["handoff_status"] == "awaiting_luna"
                    and writer["batch_id"] is None
                    and fence.get("blocked") is False
                    and fence.get("next_generation") == value["authority_generation"]
                )
                archived_successor_replacement = bool(
                    existing.get("status") == "frozen"
                    and existing.get("all_terminal") is True
                    and writer["handoff_status"] == "awaiting_luna"
                    and writer["batch_id"] is None
                    and writer.get("formal_write_count") == 0
                    and fence.get("blocked") is False
                    and fence.get("source_generation")
                    == existing.get("authority_generation")
                    and fence.get("next_generation")
                    == existing.get("authority_generation")
                    and value["authority_generation"]
                    != existing.get("authority_generation")
                )
                archived = (
                    migration_commit.get("gs269_archived_evidence")
                    if migration_commit is not None
                    else None
                )
                migration_rebind = bool(
                    isinstance(archived, Mapping)
                    and archived.get("batch_id") == existing.get("batch_id")
                    and archived.get("batch_sha256")
                    == _document_sha256(existing)
                    and archived.get("archived_authority_generation")
                    == existing.get("authority_generation")
                    and archived.get("archived_authority_fingerprint")
                    == existing.get("authority_fingerprint")
                    and archived.get("formal_write_count") == 0
                    and existing.get("status") == "frozen"
                    and existing.get("all_terminal") is True
                    and existing.get("sol_ready") is False
                    and writer["handoff_status"] == "awaiting_luna"
                    and writer["batch_id"] is None
                    and writer.get("formal_write_count") == 0
                    and fence.get("blocked") is False
                    and fence.get("source_generation")
                    == existing.get("authority_generation")
                    and fence.get("next_generation")
                    == existing.get("authority_generation")
                    and value["authority_generation"]
                    != existing.get("authority_generation")
                )
                if (
                    not normal_replacement
                    and not archived_successor_replacement
                    and not migration_rebind
                ):
                    raise SubjectSolContractError("subject_luna_batch_already_current")
                # A same-generation replacement is legal only after the
                # background Luna batch has been archived by the zero-write
                # rollover transaction.  Formal Sol acknowledgement advances
                # the generation and keeps its existing replacement contract;
                # it must not be made dependent on a background receipt.
                requires_exact_cs408_retirement = bool(
                    checked_subject == "cs408"
                    and existing.get("batch_id")
                    == CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION[
                        "batch_id"
                    ]
                    and _document_sha256(existing)
                    == CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION[
                        "batch_sha256"
                    ]
                )
                if not migration_rebind and (requires_exact_cs408_retirement or (
                    fence.get("next_generation")
                    == existing.get("authority_generation")
                )):
                    self._require_background_rollover_for_replacement_locked(
                        checked_subject, existing, writer
                    )
            written = self._write_batch_locked(value)
            writer = self._read_writer_locked(checked_subject)
            if migration_rebind or archived_successor_replacement:
                writer["generation_fence"] = {
                    "blocked": False,
                    "source_generation": existing["authority_generation"],
                    "next_generation": written["authority_generation"],
                }
            writer.update(
                {
                    "handoff_status": "awaiting_luna",
                    "batch_id": written["batch_id"],
                    "authorization_receipt_sha256": None,
                    "daily_sol_batch_sha256": None,
                    "review_receipt_sha256": None,
                    "commit_receipt_sha256": None,
                }
            )
            self._write_writer_locked(writer)
            return written

    def freeze_subject_batch(self, subject: str, batch_id: str) -> dict[str, Any]:
        checked = _subject(subject)
        with _FileLock(self._subject_lock_path(checked)):
            batch = self._read_batch_locked(checked)
            if batch is None or batch["batch_id"] != _nonempty(batch_id, "batch_id"):
                raise SubjectSolContractError("subject_luna_batch_not_found")
            if batch["status"] == "frozen":
                return batch
            batch["status"] = "frozen"
            written = self._write_batch_locked(batch)
            writer = self._read_writer_locked(checked)
            writer["handoff_status"] = (
                "ready_for_authorization" if written["sol_ready"] else "awaiting_luna"
            )
            writer["batch_id"] = written["batch_id"]
            self._write_writer_locked(writer)
            return written

    def _issue_quality_receipt(self, core: Mapping[str, Any]) -> dict[str, Any]:
        value = {"schema_version": SUBJECT_QUALITY_RECEIPT_SCHEMA, **dict(core)}
        sealed = self._seal(value, purpose="subject-quality-receipt")
        return validate_subject_quality_receipt_v1(sealed)

    def _issue_quality_receipt_v2(
        self, core: Mapping[str, Any]
    ) -> dict[str, Any]:
        value = {
            "schema_version": SUBJECT_QUALITY_RECEIPT_V2_SCHEMA,
            **dict(core),
        }
        sealed = self._seal(value, purpose="subject-quality-receipt-v2")
        return validate_subject_quality_receipt_v2(sealed)

    def issue_quality_receipt(self, _core: Mapping[str, Any]) -> dict[str, Any]:
        raise SubjectSolContractError("public_quality_signer_disabled")

    def _verify_quality_artifacts(self, receipt: Mapping[str, Any]) -> None:
        for label, digest in (
            ("analysis_output", receipt["analysis_output_sha256"]),
            ("critical_review_output", receipt["critical_review_output_sha256"]),
        ):
            self._read_immutable_value(
                self.receipt_root / "subject-stage-closures", digest, label
            )
        artifacts = {
            "analysis_mcp_transcript": (
                self.runtime_root
                / "private/reports/mcp-stage-transcripts/sha256"
                / str(receipt["analysis_mcp_transcript_sha256"])[:2]
                / f"{receipt['analysis_mcp_transcript_sha256']}.json",
                receipt["analysis_mcp_transcript_sha256"],
            ),
            "critical_review_mcp_transcript": (
                self.runtime_root
                / "private/reports/mcp-stage-transcripts/sha256"
                / str(receipt["critical_review_mcp_transcript_sha256"])[:2]
                / f"{receipt['critical_review_mcp_transcript_sha256']}.json",
                receipt["critical_review_mcp_transcript_sha256"],
            ),
        }
        for label, (path, expected) in artifacts.items():
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise SubjectSolContractError(
                    "quality_artifact_missing", artifact=label
                ) from exc
            if hashlib.sha256(payload).hexdigest() != expected:
                raise SubjectSolContractError(
                    "quality_artifact_hash_mismatch", artifact=label
                )

    def build_verified_completion_quality_closure(
        self,
        subject: str,
        verified: Mapping[str, Any],
        *,
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Derive, without signing, the only closure shape accepted by close."""

        checked = _subject(subject)
        authority = dict(_mapping(verified, "verified_luna_authority"))
        latest = dict(_mapping(authority.get("latest"), "verified_latest"))
        completion = dict(_mapping(authority.get("completion"), "verified_completion"))
        dispatch_package = dict(
            _mapping(authority.get("package"), "verified_dispatch_package")
        )
        capture_id = _nonempty(completion.get("capture_id"), "capture_id")
        unit_sha = _sha256(completion.get("unit_sha256"), "unit_sha256")
        if (
            latest.get("subject") != checked
            or completion.get("subject") != checked
            or completion.get("outcome") != "succeeded"
            or latest.get("unit_sha256") != unit_sha
        ):
            raise SubjectSolContractError("quality_verified_completion_invalid")
        batch = self.read_subject_batch(checked)
        if batch is None or batch["status"] != "frozen":
            raise SubjectSolContractError("pre_frozen_subject_batch_required")
        task = next(
            (
                row for row in batch["tasks"]
                if row["capture_id"] == capture_id and row["unit_sha256"] == unit_sha
            ),
            None,
        )
        if task is None or task["status"] != "quality_pending":
            raise SubjectSolContractError("quality_task_not_pending")
        try:
            from preprocessor_core import (  # type: ignore
                PreprocessorError,
                reopen_verified_subject_publication,
            )
            publication = reopen_verified_subject_publication(
                config,
                subject=checked,
                capture_id=capture_id,
                study_date=task["study_date"],
                input_fingerprint=task["input_fingerprint"],
            )
        except (ImportError, PreprocessorError) as exc:
            raise SubjectSolContractError("quality_subject_publication_invalid") from exc
        package_sha = _sha256(
            publication.get("subject_package_sha256"), "package_sha256"
        )
        if (
            publication.get("evidence_generation") != batch["authority_generation"]
            or publication.get("evidence_authority_fingerprint")
            != batch["authority_fingerprint"]
        ):
            raise SubjectSolContractError("quality_subject_authority_mismatch")
        package_path = self._quality_subject_package_path(
            subject=checked,
            study_date=task["study_date"],
            capture_id=capture_id,
            input_fingerprint=task["input_fingerprint"],
            package_sha256=package_sha,
            error_prefix="quality_package",
        )
        try:
            payload = package_path.read_bytes()
            package = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SubjectSolContractError("quality_package_missing") from exc
        if hashlib.sha256(payload).hexdigest() != package_sha or not isinstance(
            package, Mapping
        ):
            raise SubjectSolContractError("quality_package_hash_mismatch")
        if checked in {"math", "cs408"}:
            report_sha = _sha256(
                package.get("report_json_sha256"), "report_json_sha256"
            )
            report_path = (
                self.runtime_root
                / "private/reports/objects"
                / f"{report_sha}.json"
            )
            try:
                report_payload = report_path.read_bytes()
                report = json.loads(report_payload.decode("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SubjectSolContractError(
                    "quality_subject_report_missing"
                ) from exc
            # Math publishes a candidate wrapper whose final analysis is
            # nested under ``analysis``.  CS408 intentionally publishes the
            # final analysis itself as the sole report object.  Keep those
            # production representations distinct instead of forcing CS408
            # through the math wrapper shape.
            analysis = (
                report.get("analysis")
                if checked == "math" and isinstance(report, Mapping)
                else report if checked == "cs408" else None
            )
            evidence = (
                analysis.get("evidence_assessment")
                if isinstance(analysis, Mapping)
                else None
            )
            verification = (
                analysis.get("sol_verification_plan")
                if isinstance(analysis, Mapping)
                else None
            )
            if (
                hashlib.sha256(report_payload).hexdigest() != report_sha
                or not isinstance(evidence, Mapping)
                or evidence.get("completeness") != "complete"
                or not isinstance(verification, Mapping)
                or verification.get("recommended_disposition")
                in {"needs_user", "insufficient_evidence"}
            ):
                raise SubjectSolContractError(
                    "quality_subject_proposal_not_sol_ready"
                )
        proposal = dict(_mapping(package.get("luna_proposal"), "luna_proposal"))
        outcome = publication.get("critical_review_outcome")
        if outcome not in {"accepted", "corrected"}:
            raise SubjectSolContractError("quality_success_outcome_invalid")
        stage_receipts = dict(_mapping(package.get("stage_receipts"), "stage_receipts"))
        analysis_receipt = dict(
            _mapping(stage_receipts.get("analysis"), "analysis_stage_receipt")
        )
        critical_receipt = dict(
            _mapping(stage_receipts.get("critical_review"), "critical_stage_receipt")
        )
        analysis_payload = dict(
            _mapping(dispatch_package.get("analysis"), "dispatch_analysis")
        )
        critical_payload = dict(
            _mapping(dispatch_package.get("critical_review"), "dispatch_critical_review")
        )
        analysis_sha, _ = self._publish_immutable_value(
            self.receipt_root / "subject-stage-closures", analysis_payload
        )
        critical_sha, _ = self._publish_immutable_value(
            self.receipt_root / "subject-stage-closures", critical_payload
        )
        dispatch_package_sha = _sha256(
            completion.get("package_sha256"), "dispatch_package_sha256"
        )
        completion_sha = _sha256(
            latest.get("completion_sha256"), "completion_sha256"
        )
        dispatch_receipt_sha = _sha256(
            completion.get("receipt_sha256"), "dispatch_receipt_sha256"
        )
        return {
            "schema_version": "verified_completion_quality_closure_v1",
            "batch_id": batch["batch_id"],
            "subject": checked,
            "capture_id": capture_id,
            "unit_sha256": unit_sha,
            "input_fingerprint": task["input_fingerprint"],
            "study_date": task["study_date"],
            "frozen_payload_sha256": task["frozen_payload_sha256"],
            "completion_sha256": completion_sha,
            "dispatch_receipt_sha256": dispatch_receipt_sha,
            "dispatch_package_sha256": dispatch_package_sha,
            "capture_freeze_receipt_sha256": _sha256(
                package.get("capture_freeze_receipt_sha256"),
                "capture_freeze_receipt_sha256",
            ),
            "mcp_read_session_receipt_sha256": _sha256(
                package.get("mcp_read_session_receipt_sha256"),
                "mcp_read_session_receipt_sha256",
            ),
            "analysis_output_sha256": analysis_sha,
            "analysis_mcp_transcript_sha256": _sha256(
                analysis_receipt.get("mcp_transcript_sha256"),
                "analysis_mcp_transcript_sha256",
            ),
            "critical_review_output_sha256": critical_sha,
            "critical_review_mcp_transcript_sha256": _sha256(
                critical_receipt.get("mcp_transcript_sha256"),
                "critical_review_mcp_transcript_sha256",
            ),
            "draft_sha256": analysis_sha,
            "review_outcome": outcome,
            "proposal_sha256": _sha256(
                package.get("luna_proposal_sha256"), "proposal_sha256"
            ),
            "package_sha256": package_sha,
            "authority": {
                "generation": batch["authority_generation"],
                "authority_fingerprint": batch["authority_fingerprint"],
            },
            "model_call_count": 2,
            "formal_write_count": 0,
            "issued_at": _utc_now(),
        }

    def _record_quality_receipt(self, receipt: Mapping[str, Any]) -> dict[str, Any]:
        checked_receipt = validate_subject_quality_receipt_v1(receipt)
        self._verify_seal(checked_receipt, purpose="subject-quality-receipt")
        self._verify_quality_artifacts(checked_receipt)
        subject = checked_receipt["subject"]
        with _FileLock(self._subject_lock_path(subject)):
            batch = self._read_batch_locked(subject)
            if batch is None or batch["batch_id"] != checked_receipt["batch_id"]:
                raise SubjectSolContractError("quality_batch_not_found")
            if (
                batch["authority_generation"] != checked_receipt["authority"]["generation"]
                or batch["authority_fingerprint"]
                != checked_receipt["authority"]["authority_fingerprint"]
            ):
                raise SubjectSolContractError("quality_generation_mismatch")
            target = next(
                (
                    row
                    for row in batch["tasks"]
                    if row["capture_id"] == checked_receipt["capture_id"]
                    and row["unit_sha256"] == checked_receipt["unit_sha256"]
                ),
                None,
            )
            if target is None or target["frozen_payload_sha256"] != checked_receipt[
                "frozen_payload_sha256"
            ]:
                raise SubjectSolContractError("quality_task_binding_mismatch")
            digest, _ = self._publish_immutable(
                self.receipt_root / "subject-quality", checked_receipt
            )
            expected_status = (
                "needs_rework"
                if checked_receipt["review_outcome"] == "rejected"
                else "quality_passed"
            )
            replacement = {
                **target,
                "status": expected_status,
                "proposal_sha256": checked_receipt["proposal_sha256"],
                "package_sha256": (
                    None if expected_status == "needs_rework"
                    else checked_receipt["package_sha256"]
                ),
                "quality_receipt_sha256": digest,
                "terminal_receipt_sha256": None,
                "error_code": (
                    "critical_review_rejected" if expected_status == "needs_rework" else None
                ),
            }
            if target["quality_receipt_sha256"] is not None:
                if target == replacement:
                    return batch
                raise SubjectSolContractError("quality_receipt_conflict")
            batch["tasks"] = [replacement if row is target else row for row in batch["tasks"]]
            written = self._write_batch_locked(batch)
            writer = self._read_writer_locked(subject)
            writer["handoff_status"] = (
                "ready_for_authorization" if written["sol_ready"] else "awaiting_luna"
            )
            writer["batch_id"] = written["batch_id"]
            self._write_writer_locked(writer)
            return written

    def record_quality_receipt(self, _receipt: Mapping[str, Any]) -> dict[str, Any]:
        raise SubjectSolContractError("external_quality_receipt_rejected")

    def close_verified_completion(
        self,
        subject: str,
        *,
        closure: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Close quality only from a complete, reopened execution authority chain."""

        checked_subject = _subject(subject)
        value = dict(_mapping(closure, "verified_completion_quality_closure"))
        required = {
            "schema_version", "batch_id", "subject", "capture_id",
            "unit_sha256", "input_fingerprint", "study_date",
            "frozen_payload_sha256", "completion_sha256",
            "dispatch_receipt_sha256", "dispatch_package_sha256",
            "capture_freeze_receipt_sha256",
            "mcp_read_session_receipt_sha256", "analysis_output_sha256",
            "analysis_mcp_transcript_sha256", "critical_review_output_sha256",
            "critical_review_mcp_transcript_sha256", "draft_sha256",
            "review_outcome", "proposal_sha256", "package_sha256",
            "authority", "model_call_count", "formal_write_count", "issued_at",
        }
        if (
            set(value) != required
            or value.get("schema_version")
            != "verified_completion_quality_closure_v1"
            or value.get("subject") != checked_subject
        ):
            raise SubjectSolContractError("quality_closure_shape_invalid")
        for field in (
            "unit_sha256", "input_fingerprint", "frozen_payload_sha256",
            "completion_sha256", "dispatch_receipt_sha256",
            "dispatch_package_sha256",
            "capture_freeze_receipt_sha256", "mcp_read_session_receipt_sha256",
            "analysis_output_sha256", "analysis_mcp_transcript_sha256",
            "critical_review_output_sha256",
            "critical_review_mcp_transcript_sha256", "draft_sha256",
            "proposal_sha256", "package_sha256",
        ):
            _sha256(value.get(field), field)
        _nonempty(value.get("batch_id"), "batch_id")
        _nonempty(value.get("capture_id"), "capture_id")
        _nonempty(value.get("study_date"), "study_date")
        if value.get("review_outcome") not in {"accepted", "corrected"}:
            raise SubjectSolContractError("quality_review_outcome_invalid")
        if value.get("model_call_count") != 2 or value.get("formal_write_count") != 0:
            raise SubjectSolContractError("quality_closure_counts_invalid")
        _timestamp(value.get("issued_at"), "quality_issued_at")
        authority = dict(_mapping(value.get("authority"), "quality_authority"))
        if set(authority) != {"generation", "authority_fingerprint"}:
            raise SubjectSolContractError("quality_authority_shape_invalid")
        _nonempty(authority.get("generation"), "quality_generation")
        _sha256(authority.get("authority_fingerprint"), "quality_authority_fingerprint")

        # Dispatcher completions are owned and signed by LeaseStore.  Reopen
        # that authority chain before taking the SubjectSol lock so the lock
        # order remains dispatch -> subject and this control plane never
        # re-signs or attempts to reinterpret dispatcher receipts.
        try:
            from concurrent_dispatch import (  # type: ignore
                DispatchError,
                verify_authoritative_completion,
            )
            from preprocessor_core import (  # type: ignore
                PreprocessorError,
                release_identity,
                reopen_verified_subject_publication,
            )
        except ImportError as exc:
            raise SubjectSolContractError(
                "quality_dispatch_authority_invalid",
                reason="ImportError",
            ) from exc
        try:
            expected_release_id, _ = release_identity(config)
            dispatch_authority = verify_authoritative_completion(
                self.runtime_root,
                checked_subject,
                value["capture_id"],
                expected_release_id=expected_release_id,
                expected_unit_sha256=value["unit_sha256"],
                expected_input_fingerprint=value["input_fingerprint"],
            )
        except (OSError, PreprocessorError, DispatchError) as exc:
            raise SubjectSolContractError(
                "quality_dispatch_authority_invalid",
                reason=getattr(exc, "code", type(exc).__name__),
            ) from exc

        latest = dict(_mapping(dispatch_authority.get("latest"), "dispatch_latest"))
        completion = dict(
            _mapping(dispatch_authority.get("completion"), "dispatch_completion")
        )
        dispatch_receipt = dict(
            _mapping(dispatch_authority.get("receipt"), "dispatch_receipt")
        )
        dispatch_package = dict(
            _mapping(dispatch_authority.get("package"), "dispatch_package")
        )

        with _FileLock(self._subject_lock_path(checked_subject)):
            batch = self._read_batch_locked(checked_subject)
            if (
                batch is None
                or batch["batch_id"] != value["batch_id"]
                or batch["status"] != "frozen"
                or batch["authority_generation"] != authority["generation"]
                or batch["authority_fingerprint"]
                != authority["authority_fingerprint"]
            ):
                raise SubjectSolContractError("quality_batch_authority_mismatch")
            target = next(
                (
                    row for row in batch["tasks"]
                    if row["capture_id"] == value["capture_id"]
                    and row["unit_sha256"] == value["unit_sha256"]
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("quality_task_binding_mismatch")
            if target["status"] != "quality_pending":
                raise SubjectSolContractError("quality_task_not_pending")
            for field in (
                "input_fingerprint", "study_date", "frozen_payload_sha256"
            ):
                if target[field] != value[field]:
                    raise SubjectSolContractError("quality_task_binding_mismatch")

            if (
                latest.get("completion_sha256") != value["completion_sha256"]
                or completion.get("receipt_sha256")
                != value["dispatch_receipt_sha256"]
                or completion.get("package_sha256")
                != value["dispatch_package_sha256"]
            ):
                raise SubjectSolContractError(
                    "quality_dispatch_authority_binding_mismatch"
                )
            if any(
                completion.get(key) != expected
                for key, expected in {
                    "subject": checked_subject,
                    "capture_id": value["capture_id"],
                    "unit_sha256": value["unit_sha256"],
                    "outcome": "succeeded",
                    "package_sha256": value["dispatch_package_sha256"],
                }.items()
            ) or any(
                dispatch_receipt.get(key) != expected
                for key, expected in {
                    "subject": checked_subject,
                    "capture_id": value["capture_id"],
                    "unit_sha256": value["unit_sha256"],
                    "package_sha256": value["dispatch_package_sha256"],
                }.items()
            ) or any(
                dispatch_package.get(key) != expected
                for key, expected in {
                    "subject": checked_subject,
                    "capture_id": value["capture_id"],
                    "unit_sha256": value["unit_sha256"],
                }.items()
            ):
                raise SubjectSolContractError("quality_completion_binding_mismatch")

            dispatch_analysis = dispatch_package.get("analysis")
            dispatch_critical = dispatch_package.get("critical_review")
            if (
                not isinstance(dispatch_analysis, Mapping)
                or not isinstance(dispatch_critical, Mapping)
                or _value_sha256(dispatch_analysis)
                != value["analysis_output_sha256"]
                or _value_sha256(dispatch_analysis) != value["draft_sha256"]
                or _value_sha256(dispatch_critical)
                != value["critical_review_output_sha256"]
            ):
                raise SubjectSolContractError(
                    "quality_dispatch_stage_payload_binding_mismatch"
                )

            package_path = self._quality_subject_package_path(
                subject=checked_subject,
                study_date=value["study_date"],
                capture_id=value["capture_id"],
                input_fingerprint=value["input_fingerprint"],
                package_sha256=value["package_sha256"],
                error_prefix="quality_package",
            )
            try:
                package_payload = package_path.read_bytes()
                package = json.loads(package_payload.decode("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SubjectSolContractError("quality_package_missing") from exc
            if (
                hashlib.sha256(package_payload).hexdigest() != value["package_sha256"]
                or not isinstance(package, Mapping)
            ):
                raise SubjectSolContractError("quality_package_hash_mismatch")
            try:
                publication = reopen_verified_subject_publication(
                    config,
                    subject=checked_subject,
                    capture_id=value["capture_id"],
                    study_date=value["study_date"],
                    input_fingerprint=value["input_fingerprint"],
                )
            except (ImportError, PreprocessorError) as exc:
                raise SubjectSolContractError(
                    "quality_subject_publication_invalid"
                ) from exc
            if (
                publication.get("subject_package_sha256") != value["package_sha256"]
                or publication.get("luna_proposal_sha256")
                != value["proposal_sha256"]
                or publication.get("critical_review_outcome")
                != value["review_outcome"]
                or publication.get("capture_freeze_receipt_sha256")
                != value["capture_freeze_receipt_sha256"]
                or publication.get("mcp_read_session_receipt_sha256")
                != value["mcp_read_session_receipt_sha256"]
                or publication.get("evidence_generation") != authority["generation"]
                or publication.get("evidence_authority_fingerprint")
                != authority["authority_fingerprint"]
            ):
                raise SubjectSolContractError("quality_subject_publication_mismatch")
            proposal = package.get("luna_proposal")
            if not isinstance(proposal, Mapping):
                raise SubjectSolContractError("quality_proposal_missing")
            if (
                _value_sha256(proposal) != value["proposal_sha256"]
                or package.get("luna_proposal_sha256") != value["proposal_sha256"]
                or package.get("subject") != checked_subject
                or package.get("capture_id") != value["capture_id"]
                or package.get("evidence_generation") != authority["generation"]
                or package.get("evidence_authority_fingerprint")
                != authority["authority_fingerprint"]
                or package.get("capture_freeze_receipt_sha256")
                != value["capture_freeze_receipt_sha256"]
                or package.get("mcp_read_session_receipt_sha256")
                != value["mcp_read_session_receipt_sha256"]
            ):
                raise SubjectSolContractError("quality_package_binding_mismatch")
            proposal_sha, _ = self._publish_immutable_value(
                self.receipt_root / "subject-proposals", proposal
            )
            if proposal_sha != value["proposal_sha256"]:
                raise SubjectSolContractError("quality_proposal_hash_mismatch")

            # These two receipts are ProcessingPluginHost-owned HMAC objects
            # whose content-addressed bytes include a trailing LF.  The
            # authoritative publication reopen above has already reopened and
            # validated both objects and bound their digests to the package.
            # SubjectSol must not reinterpret them with its own no-LF
            # immutable-value representation.

            receipt = self._issue_quality_receipt(
                {
                    key: copy.deepcopy(value[key])
                    for key in required
                    if key not in {"schema_version", "input_fingerprint", "study_date"}
                }
            )
            self._verify_quality_artifacts(receipt)
            receipt_sha, _ = self._publish_immutable(
                self.receipt_root / "subject-quality", receipt
            )
            replacement = {
                **target,
                "status": "quality_passed",
                "proposal_sha256": receipt["proposal_sha256"],
                "package_sha256": receipt["package_sha256"],
                "quality_receipt_sha256": receipt_sha,
                "terminal_receipt_sha256": None,
                "error_code": None,
            }
            batch["tasks"] = [
                replacement if row is target else row for row in batch["tasks"]
            ]
            written = self._write_batch_locked(batch)
            writer = self._read_writer_locked(checked_subject)
            writer["handoff_status"] = (
                "ready_for_authorization" if written["sol_ready"] else "awaiting_luna"
            )
            writer["batch_id"] = written["batch_id"]
            self._write_writer_locked(writer)
            return written

    def record_terminal_failure(
        self,
        *,
        subject: str,
        batch_id: str,
        capture_id: str,
        unit_sha256: str,
        status: str,
        error_code: str,
    ) -> dict[str, Any]:
        checked = _subject(subject)
        if status not in {
            "failed",
            "evidence_pending",
            "execution_failed",
            "cancelled",
            "stalled",
        }:
            raise SubjectSolContractError("terminal_failure_status_invalid")
        with _FileLock(self._subject_lock_path(checked)):
            batch = self._read_batch_locked(checked)
            if (
                batch is None
                or batch["batch_id"] != batch_id
                or batch["status"] != "frozen"
            ):
                raise SubjectSolContractError("subject_luna_batch_not_found")
            target = next(
                (
                    row
                    for row in batch["tasks"]
                    if row["capture_id"] == capture_id and row["unit_sha256"] == unit_sha256
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("terminal_task_not_found")
            if batch["schema_version"] == SUBJECT_BATCH_V2_SCHEMA:
                terminal_status = {
                    "failed": "execution_failed",
                    "evidence_pending": "execution_failed",
                }.get(status, status)
                if str(error_code).endswith("_stalled"):
                    terminal_status = "stalled"
                elif status == "failed" and str(error_code) in {
                    "cancelled",
                    "user_cancelled",
                    "global_emergency_stop",
                }:
                    terminal_status = "cancelled"
                if target["status"] in TASK_V2_TERMINAL_STATUSES:
                    if (
                        target["status"] == terminal_status
                        and target["error_code"] == error_code
                    ):
                        return copy.deepcopy(batch)
                    raise SubjectSolContractError("terminal_task_already_closed")
                receipt = self._seal(
                    {
                        "schema_version": "subject_luna_terminal_receipt_v2",
                        "batch_id": batch_id,
                        "subject": checked,
                        "capture_id": capture_id,
                        "unit_sha256": _sha256(unit_sha256, "unit_sha256"),
                        "status": terminal_status,
                        "task_artifacts": {
                            field: target.get(field)
                            for field in TASK_V2_HASH_FIELDS
                            if field
                            not in {
                                "sol_handoff_envelope_sha256",
                                "terminal_receipt_sha256",
                            }
                        },
                        "warning_codes": copy.deepcopy(target["warning_codes"]),
                        "error_code": _nonempty(error_code, "error_code"),
                        "formal_write_count": 0,
                        "issued_at": _utc_now(),
                    },
                    purpose="subject-luna-terminal-receipt-v2",
                )
                digest, _ = self._publish_immutable(
                    self.receipt_root / "subject-terminal-v2", receipt
                )
                candidate = transition_subject_luna_task_v2(
                    batch,
                    capture_id=capture_id,
                    unit_sha256=unit_sha256,
                    status=terminal_status,
                    updates={
                        "terminal_receipt_sha256": digest,
                        "error_code": error_code,
                    },
                )
                written = self._write_batch_locked(candidate, preadvanced=True)
                writer = self._read_writer_locked(checked)
                writer["handoff_status"] = (
                    "ready_for_authorization"
                    if written["sol_ready"]
                    else "awaiting_luna"
                )
                writer["batch_id"] = written["batch_id"]
                self._write_writer_locked(writer)
                return written
            if target["status"] in TASK_TERMINAL_STATUSES:
                if target["status"] == status and target["error_code"] == error_code:
                    return batch
                raise SubjectSolContractError("terminal_task_already_closed")
            receipt = self._seal(
                {
                    "schema_version": "subject_luna_terminal_receipt_v1",
                    "batch_id": batch_id,
                    "subject": checked,
                    "capture_id": capture_id,
                    "unit_sha256": unit_sha256,
                    "status": status,
                    "error_code": _nonempty(error_code, "error_code"),
                    "formal_write_count": 0,
                    "issued_at": _utc_now(),
                },
                purpose="subject-luna-terminal-receipt",
            )
            digest, _ = self._publish_immutable(
                self.receipt_root / "subject-terminal", receipt
            )
            replacement = {
                **target,
                "status": status,
                "proposal_sha256": None,
                "package_sha256": None,
                "quality_receipt_sha256": None,
                "terminal_receipt_sha256": digest,
                "error_code": error_code,
            }
            batch["tasks"] = [replacement if row is target else row for row in batch["tasks"]]
            return self._write_batch_locked(batch)

    def record_task_terminal_failure(self, **kwargs: Any) -> dict[str, Any]:
        return self.record_terminal_failure(**kwargs)

    def record_task_progress(
        self,
        *,
        subject: str,
        batch_id: str,
        capture_id: str,
        unit_sha256: str,
        status: str,
    ) -> dict[str, Any]:
        """Monotonically project one durable task event into its frozen batch."""

        checked = _subject(subject)
        ordered = {
            "selected": 0,
            "queued": 0,
            "claimed": 1,
            "analysis_running": 2,
            "critical_review_running": 3,
            "quality_pending": 4,
        }
        if status not in {"claimed", "analysis_running", "critical_review_running"}:
            raise SubjectSolContractError("subject_luna_task_progress_status_invalid")
        with _FileLock(self._subject_lock_path(checked)):
            batch = self._read_batch_locked(checked)
            if (
                batch is None
                or batch["batch_id"] != batch_id
                or batch["status"] != "frozen"
            ):
                raise SubjectSolContractError("subject_luna_batch_not_found")
            target = next(
                (
                    row
                    for row in batch["tasks"]
                    if row["capture_id"] == capture_id
                    and row["unit_sha256"] == unit_sha256
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("terminal_task_not_found")
            if batch["schema_version"] == SUBJECT_BATCH_V2_SCHEMA:
                current = str(target["status"])
                if current in TASK_V2_TERMINAL_STATUSES:
                    return copy.deepcopy(batch)
                if (
                    current in TASK_V2_NONTERMINAL_STATUSES
                    and status in TASK_V2_NONTERMINAL_STATUSES
                    and TASK_V2_STATUS_RANK[status]
                    <= TASK_V2_STATUS_RANK[current]
                ):
                    # Task events are replayable projections.  An older or
                    # duplicate event cannot regress the current batch state.
                    return copy.deepcopy(batch)
                candidate = transition_subject_luna_task_v2(
                    batch,
                    capture_id=capture_id,
                    unit_sha256=unit_sha256,
                    status=status,
                )
                return self._write_batch_locked(candidate, preadvanced=True)
            current = str(target["status"])
            if current in TASK_TERMINAL_STATUSES or current == "quality_pending":
                return copy.deepcopy(batch)
            current_rank = ordered.get(current)
            desired_rank = ordered[status]
            if current_rank is None:
                raise SubjectSolContractError("subject_luna_task_progress_state_invalid")
            if desired_rank <= current_rank:
                return copy.deepcopy(batch)
            replacement = {**target, "status": status}
            batch["tasks"] = [
                replacement if row is target else row for row in batch["tasks"]
            ]
            return self._write_batch_locked(batch)

    def read_subject_batch(self, subject: str) -> dict[str, Any] | None:
        checked = _subject(subject)
        with _FileLock(self._subject_lock_path(checked)):
            value = self._read_batch_locked(checked)
            return copy.deepcopy(value)

    def read_subject(self, subject: str) -> dict[str, Any]:
        checked = _subject(subject)
        with _FileLock(self._subject_lock_path(checked)):
            batch = self._read_batch_locked(checked)
            writer = self._read_writer_locked(checked)
        eligible = bool(
            batch
            and batch["sol_ready"]
            and writer["handoff_status"] == "ready_for_authorization"
        )
        return {
            "schema_version": "subject_sol_runtime_v2",
            "subject": checked,
            "subject_luna_batch": batch,
            "writer_state": writer,
            "sol_eligibility": {
                "eligible": eligible,
                "reason": (
                    "eligible"
                    if eligible
                    else writer["handoff_status"]
                    if batch and batch["sol_ready"]
                    else "subject_quality_not_closed"
                    if batch and batch["all_terminal"]
                    else "subject_luna_incomplete"
                ),
                "batch_id": batch["batch_id"] if batch else None,
            },
            "formal_write_count": writer["formal_write_count"],
        }

    @staticmethod
    def _normalization_warning_codes(
        stage_name: str, runtime: Mapping[str, Any]
    ) -> list[str]:
        codes: list[str] = []
        raw = runtime.get("normalization_warnings")
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            for index, warning in enumerate(raw, start=1):
                code = warning.get("code") if isinstance(warning, Mapping) else None
                if not isinstance(code, str) or not code:
                    code = f"warning_{index}"
                normalized = re.sub(r"[^A-Za-z0-9._:-]", "_", code)[:120]
                codes.append(f"{stage_name}:{normalized}")
        if (
            runtime.get("normalization_status") == "normalized_with_warnings"
            and not codes
        ):
            codes.append(f"{stage_name}:normalization_warning")
        return codes

    def _record_verified_luna_completion_v2_locked(
        self,
        *,
        subject: str,
        batch: Mapping[str, Any],
        target: Mapping[str, Any],
        latest: Mapping[str, Any],
        completion: Mapping[str, Any],
        receipt: Mapping[str, Any],
        package: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Close one v2 task from already verified immutable dispatch refs."""

        capture_id = str(target["capture_id"])
        unit_sha = str(target["unit_sha256"])
        if target["status"] in TASK_V2_TERMINAL_STATUSES:
            writer = self._read_writer_locked(subject)
            return {
                "schema_version": "subject_sol_runtime_v2",
                "subject": subject,
                "subject_luna_batch": copy.deepcopy(dict(batch)),
                "writer_state": writer,
                "sol_eligibility": {
                    "eligible": bool(batch["sol_ready"]),
                    "reason": (
                        "eligible" if batch["sol_ready"] else "subject_luna_incomplete"
                    ),
                    "batch_id": batch["batch_id"],
                },
                "formal_write_count": writer["formal_write_count"],
            }

        observed = receipt.get("observed_stage_runtime")
        stage_runtime = observed if isinstance(observed, Mapping) else {}
        updates: dict[str, Any] = {
            "analysis_execution_receipt_sha256": None,
            "analysis_raw_output_sha256": None,
            "analysis_normalization_receipt_sha256": None,
            "analysis_report_sha256": None,
            "critical_review_execution_receipt_sha256": None,
            "critical_review_raw_output_sha256": None,
            "critical_review_normalization_receipt_sha256": None,
            "critical_review_report_sha256": None,
            "package_sha256": None,
            "sol_handoff_envelope_sha256": None,
            "warning_codes": [],
            "terminal_receipt_sha256": None,
            "error_code": None,
        }
        stage_rows: dict[str, dict[str, Any]] = {}
        quality_stage_rows: dict[str, dict[str, Any]] = {}
        warning_codes: list[str] = []
        snapshot_hashes: list[str] = []
        capture_freeze_hashes: list[str] = []
        read_session_receipt_hashes: list[str] = []
        quality_review_success = bool(
            completion.get("outcome") == "succeeded"
            and isinstance(package, Mapping)
            and package.get("schema_version")
            == "study-intake-review-candidate-package-v1"
            and package.get("report_available") is True
            and package.get("report_disposition") == "needs_sol_review"
            and package.get("sol_review_status") == "pending"
            and package.get("formal_write_eligible") is False
            and isinstance(package.get("warnings"), list)
            and package.get("warnings")
        )
        quality_stage_names: tuple[str, ...] = ()
        if quality_review_success:
            restricted = self.read_restricted_sol_review_candidate(
                _sha256(
                    completion.get("report_json_sha256"),
                    "report_json_sha256",
                )
            )
            transcripts = restricted.get("mcp_transcripts")
            raw_outputs = restricted.get("raw_outputs")
            quality_stage_names = tuple(
                stage_name
                for stage_name in ("analysis", "critical_review")
                if isinstance(package.get(stage_name), Mapping)
            )
            if (
                restricted.get("report_available") is not True
                or restricted.get("sol_review_status") != "pending"
                or restricted.get("formal_write_eligible") is not False
                or restricted.get("formal_write_count") != 0
                or not quality_stage_names
                or quality_stage_names[0] != "analysis"
                or not isinstance(transcripts, Mapping)
                or not isinstance(raw_outputs, Mapping)
                or set(transcripts) != set(quality_stage_names)
                or set(raw_outputs) != set(quality_stage_names)
                or any(
                    not isinstance(transcripts.get(stage_name), Mapping)
                    or not transcripts[stage_name].get("calls")
                    or not isinstance(raw_outputs.get(stage_name), Mapping)
                    for stage_name in quality_stage_names
                )
            ):
                raise SubjectSolContractError(
                    "completion_quality_report_reopen_failed"
                )
        for stage_name in ("analysis", "critical_review"):
            raw_runtime = stage_runtime.get(stage_name)
            runtime = dict(raw_runtime) if isinstance(raw_runtime, Mapping) else {}
            prefix = "analysis" if stage_name == "analysis" else "critical_review"
            execution = runtime.get("stage_execution_receipt_sha256")
            raw_output = runtime.get("raw_output_object_sha256")
            normalization = runtime.get("stage_normalization_receipt_sha256")
            updates[f"{prefix}_execution_receipt_sha256"] = (
                _sha256(execution, f"{prefix}_execution_receipt_sha256")
                if execution is not None
                else None
            )
            updates[f"{prefix}_raw_output_sha256"] = (
                _sha256(raw_output, f"{prefix}_raw_output_sha256")
                if raw_output is not None
                else None
            )
            updates[f"{prefix}_normalization_receipt_sha256"] = (
                _sha256(normalization, f"{prefix}_normalization_receipt_sha256")
                if normalization is not None
                else None
            )
            stage_report = (
                package.get(stage_name) if isinstance(package, Mapping) else None
            )
            if isinstance(stage_report, Mapping):
                stage_report_sha, _ = self._publish_immutable_value(
                    self.receipt_root / "subject-stage-reports" / stage_name,
                    stage_report,
                )
                updates[f"{prefix}_report_sha256"] = stage_report_sha
            else:
                updates[f"{prefix}_report_sha256"] = None
            stage_warnings = self._normalization_warning_codes(stage_name, runtime)
            warning_codes.extend(stage_warnings)
            stage_rows[stage_name] = {
                "raw_output_sha256": updates[f"{prefix}_raw_output_sha256"],
                "execution_receipt_sha256": updates[
                    f"{prefix}_execution_receipt_sha256"
                ],
                "normalization_receipt_sha256": updates[
                    f"{prefix}_normalization_receipt_sha256"
                ],
                "report_sha256": updates[f"{prefix}_report_sha256"],
                "warning_codes": sorted(stage_warnings),
            }
            quality_stage_rows[stage_name] = {
                **copy.deepcopy(stage_rows[stage_name]),
                "mcp_transcript_sha256": (
                    _sha256(
                        runtime.get("mcp_transcript_sha256")
                        or runtime.get("review_mcp_transcript_sha256"),
                        f"{prefix}_mcp_transcript_sha256",
                    )
                    if runtime.get("mcp_transcript_sha256") is not None
                    or (
                        quality_review_success
                        and stage_name in quality_stage_names
                        and runtime.get("review_mcp_transcript_sha256")
                        is not None
                    )
                    else None
                ),
            }
            capture_freeze = runtime.get("capture_freeze_receipt_sha256")
            if capture_freeze is not None:
                capture_freeze_hashes.append(
                    _sha256(
                        capture_freeze,
                        f"{prefix}_capture_freeze_receipt_sha256",
                    )
                )
            read_session_receipt = runtime.get(
                "mcp_read_session_receipt_sha256"
            )
            if read_session_receipt is not None:
                read_session_receipt_hashes.append(
                    _sha256(
                        read_session_receipt,
                        f"{prefix}_mcp_read_session_receipt_sha256",
                    )
                )
            snapshot = runtime.get("authority_snapshot_sha256")
            if snapshot is None:
                snapshot = runtime.get("read_session_manifest_sha256")
            if snapshot is not None:
                snapshot_hashes.append(
                    _sha256(snapshot, f"{prefix}_authority_snapshot_sha256")
                )

        outcome = str(completion.get("outcome") or "")
        error_code = str(completion.get("error_code") or "luna_task_failed")
        candidate_status: str
        if outcome == "succeeded":
            if not isinstance(package, Mapping):
                raise SubjectSolContractError("successful_completion_package_missing")
            required_stage_names = (
                quality_stage_names
                if quality_review_success
                else ("analysis", "critical_review")
            )
            package_task = package.get("task")
            frozen = (
                package_task.get("frozen_payload")
                if isinstance(package_task, Mapping)
                else None
            )
            if (
                not isinstance(frozen, Mapping)
                or _value_sha256(frozen) != target["frozen_payload_sha256"]
                or frozen.get("input_fingerprint") != target["input_fingerprint"]
                or frozen.get("study_date") != target["study_date"]
            ):
                raise SubjectSolContractError("completion_frozen_task_binding_mismatch")
            updates["package_sha256"] = _sha256(
                completion.get("package_sha256"), "package_sha256"
            )
            if quality_review_success:
                quality_codes = sorted(
                    {
                        str(row.get("code"))
                        for row in (
                            package.get("findings")
                            if isinstance(package.get("findings"), list)
                            else package.get("warnings", [])
                        )
                        if isinstance(row, Mapping)
                        and isinstance(row.get("code"), str)
                        and row.get("code")
                    }
                )
                warning_codes.extend(quality_codes)
            for prefix in required_stage_names:
                stage_name = (
                    "analysis" if prefix == "analysis" else "critical_review"
                )
                if updates[f"{prefix}_execution_receipt_sha256"] is None:
                    raise SubjectSolContractError("completion_stage_execution_receipt_missing")
                if updates[f"{prefix}_raw_output_sha256"] is None:
                    raise SubjectSolContractError("completion_stage_raw_output_missing")
                if quality_stage_rows[stage_name]["mcp_transcript_sha256"] is None:
                    raise SubjectSolContractError(
                        "completion_stage_mcp_transcript_missing"
                    )
                if updates[f"{prefix}_normalization_receipt_sha256"] is None:
                    code = f"{prefix}:normalization_receipt_missing"
                    warning_codes.append(code)
                    quality_stage_rows[stage_name]["warning_codes"].append(code)
                    stage_rows[stage_name]["warning_codes"].append(code)
                if updates[f"{prefix}_report_sha256"] is None:
                    code = f"{prefix}:report_missing"
                    warning_codes.append(code)
                    quality_stage_rows[stage_name]["warning_codes"].append(code)
                    stage_rows[stage_name]["warning_codes"].append(code)
                quality_stage_rows[stage_name]["warning_codes"] = sorted(
                    set(quality_stage_rows[stage_name]["warning_codes"])
                )
                stage_rows[stage_name]["warning_codes"] = sorted(
                    set(stage_rows[stage_name]["warning_codes"])
                )
            if not quality_review_success and (
                len(capture_freeze_hashes) != 2
                or len(set(capture_freeze_hashes)) != 1
            ):
                raise SubjectSolContractError(
                    "completion_capture_freeze_receipt_mismatch"
                )
            if not quality_review_success and (
                len(read_session_receipt_hashes) != 2
                or len(set(read_session_receipt_hashes)) != 1
            ):
                raise SubjectSolContractError(
                    "completion_mcp_read_session_receipt_mismatch"
                )
            if (
                not quality_review_success
                and len(snapshot_hashes) == 2
                and len(set(snapshot_hashes)) != 1
            ):
                raise SubjectSolContractError("completion_authority_snapshot_mismatch")
            if not quality_review_success and not snapshot_hashes:
                # Historical verified completions lack the explicit snapshot
                # field.  Their immutable read-session id and generation are
                # folded into one deterministic authority object identity;
                # successor completions carry authority_snapshot_sha256
                # directly and take the branch above.
                session_binding = {
                    stage: {
                        "read_session_id": (
                            stage_runtime.get(stage, {}).get("read_session_id")
                            if isinstance(stage_runtime.get(stage), Mapping)
                            else None
                        ),
                        "evidence_generation": (
                            stage_runtime.get(stage, {}).get("evidence_generation")
                            if isinstance(stage_runtime.get(stage), Mapping)
                            else None
                        ),
                        "evidence_authority_fingerprint": (
                            stage_runtime.get(stage, {}).get(
                                "evidence_authority_fingerprint"
                            )
                            if isinstance(stage_runtime.get(stage), Mapping)
                            else None
                        ),
                    }
                    for stage in required_stage_names
                }
                if (
                    not all(
                        all(isinstance(value, str) and value for value in row.values())
                        for row in session_binding.values()
                    )
                    or len(
                        {
                            tuple(row.values())
                            for row in session_binding.values()
                        }
                    )
                    != 1
                ):
                    raise SubjectSolContractError(
                        "completion_authority_snapshot_missing"
                    )
                snapshot_hashes = [_value_sha256(session_binding["analysis"])]
            elif not quality_review_success and len(set(snapshot_hashes)) != 1:
                raise SubjectSolContractError("completion_authority_snapshot_mismatch")
            candidate_status = (
                "workflow_complete_with_warnings"
                if warning_codes
                else "workflow_complete"
            )
            updates["error_code"] = None
        elif outcome == "needs_rework" and all(
            updates[field] is not None
            for field in (
                "analysis_execution_receipt_sha256",
                "analysis_raw_output_sha256",
            )
        ):
            candidate_status = "workflow_partial"
            updates["error_code"] = error_code
        elif outcome == "cancelled":
            candidate_status = "cancelled"
            updates["error_code"] = error_code
        elif error_code.endswith("_stalled"):
            candidate_status = "stalled"
            updates["error_code"] = error_code
        else:
            candidate_status = "execution_failed"
            updates["error_code"] = error_code

        warning_codes = sorted(set(warning_codes))
        updates["warning_codes"] = warning_codes
        terminal_receipt = self._seal(
            {
                "schema_version": "subject_luna_terminal_receipt_v2",
                "batch_id": batch["batch_id"],
                "subject": subject,
                "capture_id": capture_id,
                "unit_sha256": unit_sha,
                "status": candidate_status,
                "task_artifacts": {
                    key: updates[key]
                    for key in TASK_V2_HASH_FIELDS
                    if key
                    not in {
                        "sol_handoff_envelope_sha256",
                        "terminal_receipt_sha256",
                    }
                },
                "warning_codes": warning_codes,
                "error_code": updates["error_code"],
                "dispatch_completion_sha256": _sha256(
                    latest.get("completion_sha256"), "completion_sha256"
                ),
                "dispatch_receipt_sha256": _sha256(
                    completion.get("receipt_sha256"), "dispatch_receipt_sha256"
                ),
                "formal_write_count": 0,
                "issued_at": _timestamp(
                    completion.get("finished_at"), "completion_finished_at"
                ),
            },
            purpose="subject-luna-terminal-receipt-v2",
        )
        terminal_sha, _ = self._publish_immutable(
            self.receipt_root / "subject-terminal-v2", terminal_receipt
        )
        updates["terminal_receipt_sha256"] = terminal_sha

        if (
            candidate_status in TASK_V2_SOL_CANDIDATE_STATUSES
            and not quality_review_success
        ):
            envelope = validate_sol_task_handoff_envelope_v1(
                {
                    "schema_version": SOL_TASK_HANDOFF_SCHEMA,
                    "batch_id": batch["batch_id"],
                    "subject": subject,
                    "capture_id": capture_id,
                    "unit_sha256": unit_sha,
                    "input_fingerprint": target["input_fingerprint"],
                    "study_date": target["study_date"],
                    "frozen_payload_sha256": target["frozen_payload_sha256"],
                    "authority_snapshot_sha256": snapshot_hashes[0],
                    "analysis": stage_rows["analysis"],
                    "critical_review": stage_rows["critical_review"],
                    "package_sha256": updates["package_sha256"],
                    "terminal_receipt_sha256": terminal_sha,
                    "warning_codes": warning_codes,
                    "execution_status": candidate_status,
                    "formal_write_count": 0,
                }
            )
            handoff_sha, _ = self._publish_immutable(
                self.sol_task_handoff_root, envelope
            )
            updates["sol_handoff_envelope_sha256"] = handoff_sha

            quality_receipt = self._issue_quality_receipt_v2(
                {
                    "batch_id": batch["batch_id"],
                    "subject": subject,
                    "capture_id": capture_id,
                    "unit_sha256": unit_sha,
                    "frozen_payload_sha256": target[
                        "frozen_payload_sha256"
                    ],
                    "completion_sha256": _sha256(
                        latest.get("completion_sha256"), "completion_sha256"
                    ),
                    "dispatch_receipt_sha256": _sha256(
                        completion.get("receipt_sha256"),
                        "dispatch_receipt_sha256",
                    ),
                    "dispatch_package_sha256": updates["package_sha256"],
                    "capture_freeze_receipt_sha256": capture_freeze_hashes[0],
                    "mcp_read_session_receipt_sha256": (
                        read_session_receipt_hashes[0]
                    ),
                    "authority_snapshot_sha256": snapshot_hashes[0],
                    "analysis": quality_stage_rows["analysis"],
                    "critical_review": quality_stage_rows[
                        "critical_review"
                    ],
                    "execution_status": candidate_status,
                    "warning_codes": warning_codes,
                    "package_sha256": updates["package_sha256"],
                    "sol_handoff_envelope_sha256": handoff_sha,
                    "model_call_count": 2,
                    "formal_write_count": 0,
                    "issued_at": _timestamp(
                        completion.get("finished_at"),
                        "completion_finished_at",
                    ),
                }
            )
            quality_receipt_sha, _ = self._publish_immutable(
                self.subject_quality_v2_root, quality_receipt
            )
        else:
            quality_receipt_sha = None

        candidate = transition_subject_luna_task_v2(
            batch,
            capture_id=capture_id,
            unit_sha256=unit_sha,
            status=candidate_status,
            updates=updates,
        )
        written = self._write_batch_locked(candidate, preadvanced=True)
        writer = self._read_writer_locked(subject)
        writer["handoff_status"] = (
            "ready_for_authorization" if written["sol_ready"] else "awaiting_luna"
        )
        writer["batch_id"] = written["batch_id"]
        writer = self._write_writer_locked(writer)
        return {
            "schema_version": "subject_sol_runtime_v2",
            "subject": subject,
            "subject_luna_batch": written,
            "writer_state": writer,
            "sol_eligibility": {
                "eligible": bool(written["sol_ready"]),
                "reason": (
                    "eligible"
                    if written["sol_ready"]
                    else "subject_luna_incomplete"
                    if not written["all_terminal"]
                    else "subject_has_diagnostics_only"
                ),
                "batch_id": written["batch_id"],
            },
            "formal_write_count": writer["formal_write_count"],
            "subject_quality_receipt_sha256": quality_receipt_sha,
        }

    def record_verified_luna_completion(
        self, subject: str, verified: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Project terminality only onto an already frozen expected task."""
        checked = _subject(subject)
        authority = dict(_mapping(verified, "verified_luna_authority"))
        latest = dict(_mapping(authority.get("latest"), "verified_latest"))
        completion = dict(_mapping(authority.get("completion"), "verified_completion"))
        package = authority.get("package")
        capture_id = _nonempty(completion.get("capture_id"), "capture_id")
        unit_sha = _sha256(completion.get("unit_sha256"), "unit_sha256")
        if latest.get("subject") != checked or completion.get("subject") != checked:
            raise SubjectSolContractError("cross_subject_luna_completion")
        with _FileLock(self._subject_lock_path(checked)):
            batch = self._read_batch_locked(checked)
            if batch is None or batch["status"] != "frozen":
                raise SubjectSolContractError("pre_frozen_subject_batch_required")
            target = next(
                (
                    task for task in batch["tasks"]
                    if task["capture_id"] == capture_id
                    and task["unit_sha256"] == unit_sha
                ),
                None,
            )
            if target is None:
                raise SubjectSolContractError("completion_not_in_frozen_task_set")
            if batch["schema_version"] == SUBJECT_BATCH_V2_SCHEMA:
                raw_receipt = authority.get("receipt")
                return self._record_verified_luna_completion_v2_locked(
                    subject=checked,
                    batch=batch,
                    target=target,
                    latest=latest,
                    completion=completion,
                    receipt=(
                        dict(raw_receipt)
                        if isinstance(raw_receipt, Mapping)
                        else {}
                    ),
                    package=(dict(package) if isinstance(package, Mapping) else None),
                )
            if target["status"] in TASK_TERMINAL_STATUSES or target["status"] == "quality_pending":
                writer = self._read_writer_locked(checked)
                return {
                    "schema_version": "subject_sol_runtime_v2",
                    "subject": checked,
                    "subject_luna_batch": copy.deepcopy(batch),
                    "writer_state": writer,
                    "sol_eligibility": {
                        "eligible": False,
                        "reason": "subject_quality_not_closed",
                        "batch_id": batch["batch_id"],
                    },
                    "formal_write_count": writer["formal_write_count"],
                }
            if completion.get("outcome") == "succeeded":
                if not isinstance(package, Mapping):
                    raise SubjectSolContractError("successful_completion_package_missing")
                task = package.get("task")
                frozen = task.get("frozen_payload") if isinstance(task, Mapping) else None
                if (
                    not isinstance(frozen, Mapping)
                    or _value_sha256(frozen) != target["frozen_payload_sha256"]
                    or frozen.get("input_fingerprint") != target["input_fingerprint"]
                    or frozen.get("study_date") != target["study_date"]
                ):
                    raise SubjectSolContractError("completion_frozen_task_binding_mismatch")
                replacement = {**target, "status": "quality_pending"}
            else:
                error_code = _nonempty(
                    completion.get("error_code") or "luna_task_failed",
                    "completion_error_code",
                )
                rejected = error_code in CRITICAL_REVIEW_REJECTION_CODES
                terminal_status = "needs_rework" if rejected else "failed"
                receipt = self._seal(
                    {
                        "schema_version": "subject_luna_terminal_receipt_v1",
                        "batch_id": batch["batch_id"],
                        "subject": checked,
                        "capture_id": capture_id,
                        "unit_sha256": unit_sha,
                        "status": terminal_status,
                        "error_code": (
                            "critical_review_rejected" if rejected else error_code
                        ),
                        "formal_write_count": 0,
                        "issued_at": _utc_now(),
                    },
                    purpose="subject-luna-terminal-receipt",
                )
                digest, _ = self._publish_immutable(
                    self.receipt_root / "subject-terminal", receipt
                )
                replacement = {
                    **target,
                    "status": terminal_status,
                    "proposal_sha256": None,
                    "package_sha256": None,
                    "quality_receipt_sha256": None,
                    "terminal_receipt_sha256": digest,
                    "error_code": (
                        "critical_review_rejected" if rejected else error_code
                    ),
                }
            batch["tasks"] = [
                replacement if row is target else row for row in batch["tasks"]
            ]
            self._write_batch_locked(batch)
        return self.read_subject(checked)

    def _read_archived_subject_batch_v2(
        self,
        subject: str,
        *,
        batch_id: str,
        batch_sha256: str,
    ) -> dict[str, Any]:
        """Reopen one exact zero-write rollover archive for nightly Sol."""

        checked_subject = _subject(subject)
        checked_batch_id = _nonempty(batch_id, "subject_luna_batch_id")
        checked_batch_sha = _sha256(batch_sha256, "subject_luna_batch_sha256")
        root = self.subject_batch_archive_root / "background" / "sha256"
        paths = sorted(root.glob("*/*.json"))
        if len(paths) > 4096:
            raise SubjectSolContractError("subject_luna_archive_inventory_too_large")
        matches: list[dict[str, Any]] = []
        for path in paths:
            digest = path.stem
            if SHA256_RE.fullmatch(digest) is None:
                raise SubjectSolContractError("subject_luna_archive_name_invalid")
            raw = self._read_content_addressed(
                self.subject_batch_archive_root / "background",
                digest,
                "subject_background_luna_batch_archive",
            )
            if (
                raw.get("subject") != checked_subject
                or raw.get("batch_id") != checked_batch_id
                or raw.get("batch_sha256") != checked_batch_sha
            ):
                continue
            required = {
                "schema_version",
                "subject",
                "batch_id",
                "batch_sha256",
                "batch_snapshot_sha256",
                "batch",
                "authority_generation",
                "authority_fingerprint",
                "archived_at",
                "sol_called",
                "sol_enabled",
                "formal_write_count",
                "seal",
            }
            if (
                set(raw) != required
                or raw.get("schema_version")
                != "subject_background_luna_batch_archive_v1"
                or raw.get("sol_called") is not False
                or raw.get("sol_enabled") is not False
                or raw.get("formal_write_count") != 0
            ):
                raise SubjectSolContractError("subject_luna_archive_invalid")
            self._verify_seal(
                raw, purpose="subject-background-luna-batch-archive"
            )
            batch = validate_subject_luna_batch_v2(
                _mapping(raw.get("batch"), "archived_subject_luna_batch")
            )
            if (
                batch["subject"] != checked_subject
                or batch["batch_id"] != checked_batch_id
                or _document_sha256(batch) != checked_batch_sha
                or raw.get("authority_generation")
                != batch["authority_generation"]
                or raw.get("authority_fingerprint")
                != batch["authority_fingerprint"]
            ):
                raise SubjectSolContractError("subject_luna_archive_binding_invalid")
            matches.append(batch)
        if len(matches) != 1:
            raise SubjectSolContractError(
                "subject_luna_archive_missing"
                if not matches
                else "subject_luna_archive_ambiguous"
            )
        return matches[0]

    def build_authorized_batch(
        self,
        subject: str,
        *,
        sol_batch_id: str,
        authorization_receipt_sha256: str,
    ) -> dict[str, Any]:
        checked_subject = _subject(subject)
        raw_authorization = self._read_content_addressed(
            self.user_sol_authorization_root,
            authorization_receipt_sha256,
            "user_sol_authorization_receipt",
        )
        if raw_authorization.get("schema_version") == USER_SOL_AUTHORIZATION_V2_SCHEMA:
            authorization = _authorization_receipt_v2(raw_authorization)
            authorization_purpose = "user-sol-authorization-receipt-v2"
        else:
            authorization = _authorization_receipt(raw_authorization)
            authorization_purpose = "user-sol-authorization-receipt"
        self._verify_external_seal(
            authorization,
            purpose=authorization_purpose,
            key_path=self.user_sol_authority_key_path,
            label="user_sol_authorization",
        )
        core = authorization
        checked_batch_id = _nonempty(sol_batch_id, "sol_batch_id")
        luna = self.read_subject_batch(checked_subject)
        if authorization["schema_version"] == USER_SOL_AUTHORIZATION_V2_SCHEMA:
            expected_luna_id = _nonempty(
                core.get("subject_luna_batch_id"), "subject_luna_batch_id"
            )
            expected_luna_sha = _sha256(
                core.get("subject_luna_batch_sha256"),
                "subject_luna_batch_sha256",
            )
            if not (
                isinstance(luna, Mapping)
                and luna.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA
                and luna.get("batch_id") == expected_luna_id
                and _document_sha256(luna) == expected_luna_sha
            ):
                luna = self._read_archived_subject_batch_v2(
                    checked_subject,
                    batch_id=expected_luna_id,
                    batch_sha256=expected_luna_sha,
                )
        elif luna is None:
            raise SubjectSolContractError("subject_luna_batch_not_found")
        if luna["schema_version"] == SUBJECT_BATCH_V2_SCHEMA:
            task_by_capture = {row["capture_id"]: row for row in luna["tasks"]}
            expected_handoffs = sorted(
                _sha256(
                    task_by_capture[capture_id]["sol_handoff_envelope_sha256"],
                    "sol_handoff_envelope_sha256",
                )
                for capture_id in luna["sol_candidate_task_ids"]
            )
            if (
                core["sol_batch_id"] != checked_batch_id
                or core["subject"] != luna["subject"]
                or core["study_date"] != luna["study_date"]
                or core["subject_luna_batch_id"] != luna["batch_id"]
                or core["subject_luna_batch_sha256"] != _document_sha256(luna)
                or core["sol_candidate_task_ids"]
                != luna["sol_candidate_task_ids"]
                or core["sol_handoff_envelope_sha256s"] != expected_handoffs
                or core["diagnostic_task_ids"] != luna["diagnostic_task_ids"]
            ):
                raise SubjectSolContractError("user_sol_authorization_stale")
            value = {
                "schema_version": DAILY_SOL_BATCH_V3_SCHEMA,
                "batch_id": checked_batch_id,
                "subject": luna["subject"],
                "study_date": luna["study_date"],
                "subject_luna_batch": luna,
                "subject_luna_batch_sha256": core[
                    "subject_luna_batch_sha256"
                ],
                "sol_candidate_task_ids": core["sol_candidate_task_ids"],
                "sol_handoff_envelope_sha256s": expected_handoffs,
                "diagnostic_task_ids": core["diagnostic_task_ids"],
                "authorization_receipt": authorization,
                "authorization_receipt_sha256": _document_sha256(authorization),
                "writer_adapter": core["writer_adapter"],
                "idempotency_key": core["idempotency_key"],
                "status": "authorized",
                "formal_write_count": 0,
            }
            return self.read_verified_daily_sol_batch_v3(value)
        if (
            core["sol_batch_id"] != checked_batch_id
            or
            core["subject"] != luna["subject"]
            or core["study_date"] != luna["study_date"]
            or core["subject_luna_batch_id"] != luna["batch_id"]
            or core["subject_luna_batch_sha256"] != _document_sha256(luna)
            or core["proposal_sha256s"]
            != sorted(row["proposal_sha256"] for row in luna["tasks"])
            or core["package_sha256s"]
            != sorted(row["package_sha256"] for row in luna["tasks"])
            or core["quality_receipt_sha256s"]
            != sorted(row["quality_receipt_sha256"] for row in luna["tasks"])
        ):
            raise SubjectSolContractError("user_sol_authorization_stale")
        value = {
            "schema_version": DAILY_SOL_BATCH_SCHEMA,
            "batch_id": checked_batch_id,
            "subject": luna["subject"],
            "study_date": luna["study_date"],
            "subject_luna_batch": luna,
            "subject_luna_batch_sha256": core["subject_luna_batch_sha256"],
            "proposal_sha256s": core["proposal_sha256s"],
            "package_sha256s": core["package_sha256s"],
            "quality_receipt_sha256s": core["quality_receipt_sha256s"],
            "authorization_receipt": authorization,
            "authorization_receipt_sha256": _document_sha256(authorization),
            "writer_adapter": core["writer_adapter"],
            "idempotency_key": core["idempotency_key"],
            "status": "authorized",
            "formal_write_count": 0,
        }
        return validate_daily_sol_batch_v2(value)

    def _verify_authorized_batch(self, batch: Mapping[str, Any]) -> dict[str, Any]:
        if batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA:
            checked = self.read_verified_daily_sol_batch_v3(batch)
            authorization_sha = checked["authorization_receipt_sha256"]
            reopened = _authorization_receipt_v2(
                self._read_content_addressed(
                    self.user_sol_authorization_root,
                    authorization_sha,
                    "user_sol_authorization_receipt",
                )
            )
            if reopened != checked["authorization_receipt"]:
                raise SubjectSolContractError(
                    "user_sol_authorization_reopen_mismatch"
                )
            self._verify_external_seal(
                reopened,
                purpose="user-sol-authorization-receipt-v2",
                key_path=self.user_sol_authority_key_path,
                label="user_sol_authorization",
            )
            return checked
        checked = validate_daily_sol_batch_v2(batch)
        authorization_sha = checked["authorization_receipt_sha256"]
        reopened = _authorization_receipt(
            self._read_content_addressed(
                self.user_sol_authorization_root,
                authorization_sha,
                "user_sol_authorization_receipt",
            )
        )
        if reopened != checked["authorization_receipt"]:
            raise SubjectSolContractError("user_sol_authorization_reopen_mismatch")
        self._verify_external_seal(
            reopened,
            purpose="user-sol-authorization-receipt",
            key_path=self.user_sol_authority_key_path,
            label="user_sol_authorization",
        )
        return checked

    def authorize_batch(self, subject: str, batch: Mapping[str, Any]) -> dict[str, Any]:
        checked_subject = _subject(subject)
        authorized = self._verify_authorized_batch(batch)
        if authorized["subject"] != checked_subject:
            raise SubjectSolContractError("cross_subject_sol_authorization")
        digest, _ = self._publish_immutable(self.sol_batch_root, authorized)
        authorized_at = authorized["authorization_receipt"]["event"]["authorized_at"]
        entry = {
            "batch_id": authorized["batch_id"],
            "subject": checked_subject,
            "authorized_at": authorized_at,
            "authorization_receipt_sha256": authorized["authorization_receipt_sha256"],
            "daily_sol_batch_sha256": digest,
            "status": "queued",
        }
        with _FileLock(self.global_lock_path):
            state = self._read_global_locked()
            existing = next(
                (row for row in state["queue"] if row["batch_id"] == entry["batch_id"]),
                None,
            )
            if existing is not None:
                if existing == entry:
                    return {"global": state, "subject": self.read_subject(checked_subject)}
                raise SubjectSolContractError("sol_batch_id_conflict")
            with _FileLock(self._subject_lock_path(checked_subject)):
                current = self._read_batch_locked(checked_subject)
                writer = self._read_writer_locked(checked_subject)
                archived_authorization = current != authorized["subject_luna_batch"]
                if archived_authorization:
                    archived = self._read_archived_subject_batch_v2(
                        checked_subject,
                        batch_id=authorized["subject_luna_batch"]["batch_id"],
                        batch_sha256=authorized["subject_luna_batch_sha256"],
                    )
                    if archived != authorized["subject_luna_batch"]:
                        raise SubjectSolContractError("subject_luna_batch_changed")
                if (
                    writer["handoff_status"]
                    not in {"ready_for_authorization", "awaiting_luna"}
                    or authorized["subject_luna_batch"]["sol_ready"] is not True
                    or (
                        archived_authorization
                        and (
                            writer["handoff_status"] != "awaiting_luna"
                            or writer.get("batch_id") is not None
                        )
                    )
                ):
                    raise SubjectSolContractError("subject_not_ready_for_authorization")
                state["queue"].append(entry)
                state["queue"].sort(
                    key=lambda row: (
                        row["authorized_at"],
                        row["authorization_receipt_sha256"],
                        row["batch_id"],
                    )
                )
                global_written = self._write_global_locked(state)
                writer.update(
                    {
                        "handoff_status": "queued_for_sol",
                        "batch_id": authorized["batch_id"],
                        "authorization_receipt_sha256": authorized[
                            "authorization_receipt_sha256"
                        ],
                        "daily_sol_batch_sha256": digest,
                    }
                )
                writer_written = self._write_writer_locked(writer)
        return {"global": global_written, "subject": writer_written}

    def _sol_batch_by_digest(self, digest: str) -> dict[str, Any]:
        checked = _sha256(digest, "daily_sol_batch_sha256")
        path = self.sol_batch_root / "sha256" / checked[:2] / f"{checked}.json"
        value = self._read_json(path, "daily_sol_batch")
        if value is None or hashlib.sha256(path.read_bytes()).hexdigest() != checked:
            raise SubjectSolContractError("daily_sol_batch_missing")
        return self._verify_authorized_batch(value)

    def _active_luna_count(self, subject: str) -> int:
        count = 0
        root = self.state_root / "leases"
        for path in sorted(root.glob("*.json")):
            value = self._read_json(path, "luna_lease")
            if value is None:
                continue
            if value.get("subject") == subject and value.get("status") in {
                "claimed",
                "waiting_retry",
            }:
                count += 1
        return count

    def _claim_receipt(
        self,
        *,
        batch_id: str,
        subject: str,
        owner_id: str,
        outcome: str,
        reason: str,
        active_writer: Mapping[str, Any] | None,
        fencing_token: int | None,
    ) -> tuple[dict[str, Any], str]:
        receipt = self._seal(
            {
                "schema_version": "global_sol_writer_claim_receipt_v1",
                "batch_id": batch_id,
                "subject": subject,
                "owner_id": owner_id,
                "outcome": outcome,
                "reason": reason,
                "observed_active_batch_id": (
                    active_writer.get("batch_id") if isinstance(active_writer, Mapping) else None
                ),
                "observed_active_subject": (
                    active_writer.get("subject") if isinstance(active_writer, Mapping) else None
                ),
                "fencing_token": fencing_token,
                "formal_write_count": 0,
                "issued_at": _utc_now(),
            },
            purpose="global-sol-writer-claim-receipt",
        )
        digest, _ = self._publish_immutable(self.receipt_root / "sol-claims", receipt)
        return receipt, digest

    def begin_sol_review(
        self, subject: str, batch_id: str, *, owner_id: str
    ) -> dict[str, Any]:
        checked = _subject(subject)
        requested_batch = _nonempty(batch_id, "batch_id")
        owner = _nonempty(owner_id, "writer_owner_id")
        with _FileLock(self.global_lock_path):
            state = self._read_global_locked()
            active = state.get("active_writer")
            if active is not None:
                _, receipt_sha = self._claim_receipt(
                    batch_id=requested_batch,
                    subject=checked,
                    owner_id=owner,
                    outcome="rejected",
                    reason="global_sol_writer_busy",
                    active_writer=active,
                    fencing_token=None,
                )
                raise SubjectSolContractError(
                    "global_sol_writer_busy", claim_receipt_sha256=receipt_sha
                )
            pending = [row for row in state["queue"] if row["status"] == "queued"]
            if not pending or pending[0]["batch_id"] != requested_batch:
                _, receipt_sha = self._claim_receipt(
                    batch_id=requested_batch,
                    subject=checked,
                    owner_id=owner,
                    outcome="rejected",
                    reason="global_sol_fifo_head_mismatch",
                    active_writer=None,
                    fencing_token=None,
                )
                raise SubjectSolContractError(
                    "global_sol_fifo_head_mismatch", claim_receipt_sha256=receipt_sha
                )
            entry = pending[0]
            if entry["subject"] != checked:
                raise SubjectSolContractError("cross_subject_sol_claim")
            specialized_path = (
                self.english_legacy_sol_batch_root / "sha256"
                / str(entry["daily_sol_batch_sha256"])[:2]
                / f"{entry['daily_sol_batch_sha256']}.json"
            )
            if specialized_path.is_file():
                raise SubjectSolContractError(
                    "english_legacy_recuration_command_required"
                )
            batch = self._sol_batch_by_digest(entry["daily_sol_batch_sha256"])
            with _FileLock(self._subject_lock_path(checked)):
                writer = self._read_writer_locked(checked)
                if (
                    writer["handoff_status"] != "queued_for_sol"
                    or writer["batch_id"] != requested_batch
                ):
                    raise SubjectSolContractError("subject_sol_queue_state_mismatch")
                active_luna = self._active_luna_count(checked)
                if active_luna:
                    _, receipt_sha = self._claim_receipt(
                        batch_id=requested_batch,
                        subject=checked,
                        owner_id=owner,
                        outcome="rejected",
                        reason="subject_luna_not_terminal",
                        active_writer=None,
                        fencing_token=None,
                    )
                    raise SubjectSolContractError(
                        "subject_luna_not_terminal",
                        claim_receipt_sha256=receipt_sha,
                        active_luna_count=active_luna,
                    )
                token = state["next_fencing_token"]
                _, claim_sha = self._claim_receipt(
                    batch_id=requested_batch,
                    subject=checked,
                    owner_id=owner,
                    outcome="acquired",
                    reason="fifo_head_acquired",
                    active_writer=None,
                    fencing_token=token,
                )
                active_writer = {
                    "batch_id": requested_batch,
                    "subject": checked,
                    "fencing_token": token,
                    "owner_id": owner,
                    "claimed_at": _utc_now(),
                    "authorization_receipt_sha256": entry[
                        "authorization_receipt_sha256"
                    ],
                    "daily_sol_batch_sha256": entry["daily_sol_batch_sha256"],
                    "review_receipt_sha256": None,
                    "status": "reviewing",
                    "current_item": None,
                    "committed_count": 0,
                    "remaining_count": len(
                        batch["sol_candidate_task_ids"]
                        if batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
                        else batch["proposal_sha256s"]
                    ),
                }
                entry["status"] = "active"
                state["active_writer"] = active_writer
                state["next_fencing_token"] = token + 1
                global_written = self._write_global_locked(state)
                writer.update(
                    {
                        "handoff_status": "sol_reviewing",
                        "fencing_token": token,
                        "generation_fence": {
                            "blocked": True,
                            "source_generation": batch["subject_luna_batch"][
                                "authority_generation"
                            ],
                            "next_generation": None,
                        },
                    }
                )
                writer_written = self._write_writer_locked(writer)
        return {
            "schema_version": "global_sol_writer_claim_result_v1",
            "claim_receipt_sha256": claim_sha,
            "fencing_token": token,
            "global": global_written,
            "subject": writer_written,
            "formal_write_count": 0,
        }

    def begin_subject_commit(self, subject: str, batch_id: str) -> dict[str, Any]:
        return self.begin_sol_review(
            subject, batch_id, owner_id=f"control-{os.getpid()}"
        )

    def issue_sol_review_receipt(self, core: Mapping[str, Any]) -> dict[str, Any]:
        raise SubjectSolContractError("public_sol_review_signer_disabled")

    def record_sol_review(
        self, subject: str, review_receipt_sha256: str
    ) -> dict[str, Any]:
        checked = _subject(subject)
        review_sha = _sha256(review_receipt_sha256, "sol_review_receipt_sha256")
        raw_review = self._read_content_addressed(
            self.writer_review_receipt_root,
            review_sha,
            "writer_adapter_sol_review_receipt",
        )
        is_v2 = raw_review.get("schema_version") == SOL_REVIEW_RECEIPT_V2_SCHEMA
        validated = (
            validate_sol_review_receipt_v2(raw_review)
            if is_v2
            else validate_sol_review_receipt_v1(raw_review)
        )
        self._verify_external_seal(
            validated,
            purpose=(
                "writer-adapter-sol-review-receipt-v2"
                if is_v2
                else "writer-adapter-sol-review-receipt"
            ),
            key_path=self.writer_adapter_authority_key_path,
            label="writer_adapter_sol_review",
        )
        with _FileLock(self.global_lock_path):
            state = self._read_global_locked()
            active = state.get("active_writer")
            if (
                not isinstance(active, Mapping)
                or active.get("subject") != checked
                or active.get("batch_id") != validated["batch_id"]
                or active.get("fencing_token") != validated["fencing_token"]
            ):
                raise SubjectSolContractError("stale_global_sol_fence")
            batch = self._sol_batch_by_digest(str(active["daily_sol_batch_sha256"]))
            if is_v2 != (
                batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
            ):
                raise SubjectSolContractError("sol_review_schema_generation_mismatch")
            validated = (
                validate_sol_review_receipt_v2(validated, batch=batch)
                if is_v2
                else validate_sol_review_receipt_v1(validated, batch=batch)
            )
            digest = review_sha
            with _FileLock(self._subject_lock_path(checked)):
                writer = self._read_writer_locked(checked)
                queue_entry = next(
                    row for row in state["queue"] if row["batch_id"] == validated["batch_id"]
                )
                if validated["status"] == "approved":
                    active = dict(active)
                    active["review_receipt_sha256"] = digest
                    active["status"] = "applying"
                    state["active_writer"] = active
                    queue_entry["status"] = "reviewed"
                    writer["handoff_status"] = "sol_reviewed"
                    writer["review_receipt_sha256"] = digest
                else:
                    queue_entry["status"] = "safe_paused"
                    state["active_writer"] = None
                    writer["handoff_status"] = "safe_paused"
                    writer["review_receipt_sha256"] = digest
                global_written = self._write_global_locked(state)
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "review_receipt_sha256": digest,
            "formal_write_count": 0,
        }

    def issue_sol_commit_receipt(
        self, core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> dict[str, Any]:
        raise SubjectSolContractError("public_sol_commit_signer_disabled")

    def finish_subject_commit(
        self, subject: str, writer_apply_receipt_sha256: str
    ) -> dict[str, Any]:
        checked = _subject(subject)
        apply_sha = _sha256(
            writer_apply_receipt_sha256, "writer_apply_receipt_sha256"
        )
        with _FileLock(self.global_lock_path):
            state = self._read_global_locked()
            active = state.get("active_writer")
            if not isinstance(active, Mapping) or active.get("subject") != checked:
                raise SubjectSolContractError("global_sol_writer_not_active")
            batch = self._sol_batch_by_digest(str(active["daily_sol_batch_sha256"]))
            try:
                raw_apply = self._read_content_addressed(
                    self.writer_apply_receipt_root,
                    apply_sha,
                    "deterministic_writer_apply_receipt",
                )
                is_v2 = (
                    raw_apply.get("schema_version") == SOL_COMMIT_RECEIPT_V2_SCHEMA
                )
                if is_v2 != (
                    batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
                ):
                    raise SubjectSolContractError(
                        "sol_commit_schema_generation_mismatch"
                    )
                validated = (
                    validate_sol_commit_receipt_v2(raw_apply, batch=batch)
                    if is_v2
                    else validate_sol_commit_receipt_v1(raw_apply, batch=batch)
                )
                self._verify_external_seal(
                    validated,
                    purpose=(
                        "deterministic-writer-apply-receipt-v2"
                        if is_v2
                        else "deterministic-writer-apply-receipt"
                    ),
                    key_path=self.writer_adapter_authority_key_path,
                    label="deterministic_writer_apply",
                )
                _, derived_core = self._verify_writer_execution_result(
                    validated["execution_result_sha256"],
                    batch=batch,
                )
                if any(
                    validated.get(key) != expected
                    for key, expected in derived_core.items()
                ):
                    raise SubjectSolContractError(
                        "writer_apply_execution_result_binding_mismatch"
                    )
                if (
                    validated["batch_id"] != active["batch_id"]
                    or validated["fencing_token"] != active["fencing_token"]
                    or validated["sol_review_receipt_sha256"]
                    != active.get("review_receipt_sha256")
                ):
                    raise SubjectSolContractError("stale_global_sol_fence")
            except SubjectSolContractError:
                active = dict(active)
                active["status"] = "safe_paused"
                state["active_writer"] = active
                queue_entry = next(
                    row for row in state["queue"]
                    if row["batch_id"] == active["batch_id"]
                )
                queue_entry["status"] = "safe_paused"
                with _FileLock(self._subject_lock_path(checked)):
                    writer = self._read_writer_locked(checked)
                    writer["handoff_status"] = "safe_paused"
                    self._write_writer_locked(writer)
                    self._write_global_locked(state)
                raise
            digest = apply_sha
            with _FileLock(self._subject_lock_path(checked)):
                writer = self._read_writer_locked(checked)
                queue_entry = next(
                    row for row in state["queue"] if row["batch_id"] == active["batch_id"]
                )
                success = validated["status"] in {"committed", "already_current"}
                queue_entry["status"] = "committed" if success else "safe_paused"
                state["active_writer"] = None
                state["formal_write_count"] += validated["formal_write_count"]
                writer["handoff_status"] = "complete" if success else "safe_paused"
                writer["commit_receipt_sha256"] = digest
                writer["formal_write_count"] += validated["formal_write_count"]
                global_written = self._write_global_locked(state)
                writer_written = self._write_writer_locked(writer)
        return {
            "global": global_written,
            "subject": writer_written,
            "commit_receipt_sha256": digest,
            "formal_write_count": validated["formal_write_count"],
        }

    def acknowledge_subject_generation(
        self, subject: str, *, authority_ack_sha256: str
    ) -> dict[str, Any]:
        checked = _subject(subject)
        ack = self._read_content_addressed(
            self.dispatch_root / "subject-generation-acks",
            authority_ack_sha256,
            "subject_generation_authority_ack",
        )
        required = {
            "schema_version", "subject", "batch_id", "subject_luna_batch_id",
            "source_generation",
            "next_generation", "next_authority_fingerprint",
            "authority_observation_sha256", "commit_receipt_sha256",
            "acknowledged_at", "formal_write_count", "seal",
        }
        if (
            set(ack) != required
            or ack.get("schema_version") != SUBJECT_GENERATION_AUTHORITY_ACK_SCHEMA
            or ack.get("subject") != checked
            or ack.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError("subject_generation_authority_ack_invalid")
        self._verify_external_seal(
            ack,
            purpose="writer-adapter-subject-generation-authority-ack",
            key_path=self.writer_adapter_authority_key_path,
            label="writer_adapter_generation_ack",
        )
        source_generation = _nonempty(
            ack.get("source_generation"), "source_generation"
        )
        generation = _nonempty(ack.get("next_generation"), "next_generation")
        fingerprint = _sha256(
            ack.get("next_authority_fingerprint"), "next_authority_fingerprint"
        )
        commit_sha = _sha256(
            ack.get("commit_receipt_sha256"), "commit_receipt_sha256"
        )
        _timestamp(ack.get("acknowledged_at"), "generation_acknowledged_at")
        observation = self._read_content_addressed(
            self.subject_authority_observation_root,
            _sha256(
                ack.get("authority_observation_sha256"),
                "authority_observation_sha256",
            ),
            "subject_authority_observation",
        )
        if (
            set(observation)
            != {
                "schema_version", "subject", "generation",
                "authority_fingerprint", "observed_at", "model_call_count",
                "formal_write_count", "seal",
            }
            or observation.get("schema_version")
            != SUBJECT_AUTHORITY_OBSERVATION_SCHEMA
            or observation.get("subject") != checked
            or observation.get("generation") != generation
            or observation.get("authority_fingerprint") != fingerprint
            or observation.get("model_call_count") != 0
            or observation.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError("subject_authority_observation_invalid")
        observed_at = _timestamp(
            observation.get("observed_at"), "authority_observed_at"
        )
        self._verify_external_seal(
            observation,
            purpose="subject-mcp-authority-observation",
            key_path=self.subject_authority_observer_key_path,
            label="subject_authority_observer",
        )
        with _FileLock(self.global_lock_path):
            global_state = self._read_global_locked()
            active = global_state.get("active_writer")
            if isinstance(active, Mapping) and active.get("subject") == checked:
                raise SubjectSolContractError("subject_writer_generation_fence_held")
            with _FileLock(self._subject_lock_path(checked)):
                writer = self._read_writer_locked(checked)
                fence = dict(writer["generation_fence"])
                batch = self._read_batch_locked(checked)
                queue_entry = next(
                    (
                        row for row in global_state["queue"]
                        if row["batch_id"] == ack["batch_id"]
                    ),
                    None,
                )
                if (
                    batch is None
                    or batch["batch_id"] != ack["subject_luna_batch_id"]
                    or batch["authority_generation"] != source_generation
                    or writer["handoff_status"] != "complete"
                    or writer["batch_id"] != ack["batch_id"]
                    or writer["commit_receipt_sha256"] != commit_sha
                    or fence.get("blocked") is not True
                    or fence.get("source_generation") != source_generation
                    or not isinstance(queue_entry, Mapping)
                    or queue_entry.get("status") != "committed"
                ):
                    raise SubjectSolContractError("subject_generation_ack_not_committed")
                authorized_batch = self._sol_batch_by_digest(
                    str(queue_entry["daily_sol_batch_sha256"])
                )
                commit = validate_sol_commit_receipt_v1(
                    self._read_content_addressed(
                        self.writer_apply_receipt_root, commit_sha,
                        "subject_generation_commit_receipt",
                    ),
                    batch=authorized_batch,
                )
                self._verify_external_seal(
                    commit,
                    purpose="deterministic-writer-apply-receipt",
                    key_path=self.writer_adapter_authority_key_path,
                    label="deterministic_writer_apply",
                )
                _, derived_commit = self._verify_writer_execution_result(
                    commit["execution_result_sha256"],
                    batch=authorized_batch,
                )
                if (
                    commit.get("batch_id") != ack["batch_id"]
                    or commit.get("status") not in {"committed", "already_current"}
                    or any(
                        commit.get(key) != value
                        for key, value in derived_commit.items()
                    )
                ):
                    raise SubjectSolContractError("subject_generation_commit_invalid")
                commit_completed = _timestamp(
                    commit.get("completed_at"), "commit_completed_at"
                )
                parse_time = lambda value: dt.datetime.fromisoformat(
                    value[:-1] + "+00:00" if value.endswith("Z") else value
                )
                if parse_time(observed_at) < parse_time(commit_completed):
                    raise SubjectSolContractError(
                        "subject_authority_observation_before_commit"
                    )
                if generation == source_generation:
                    raise SubjectSolContractError("subject_generation_not_advanced")
                archive = self._seal(
                    {
                        "schema_version": "subject_luna_batch_archive_v1",
                        "subject": checked,
                        "batch_id": batch["batch_id"],
                        "batch_sha256": _document_sha256(batch),
                        "commit_receipt_sha256": commit_sha,
                        "authority_ack_sha256": authority_ack_sha256,
                        "source_generation": source_generation,
                        "next_generation": generation,
                        "archived_at": _utc_now(),
                        "formal_write_count": 0,
                    },
                    purpose="subject-luna-batch-archive",
                )
                self._publish_immutable(self.subject_batch_archive_root, archive)
                writer["generation_fence"] = {
                    "blocked": False,
                    "source_generation": source_generation,
                    "next_generation": generation,
                }
                writer["handoff_status"] = "awaiting_luna"
                writer["batch_id"] = None
                writer["authorization_receipt_sha256"] = None
                writer["daily_sol_batch_sha256"] = None
                writer["review_receipt_sha256"] = None
                writer["commit_receipt_sha256"] = None
                return self._write_writer_locked(writer)

    def luna_admission(self, subject: str) -> dict[str, Any]:
        checked = _subject(subject)
        with _FileLock(self._subject_lock_path(checked)):
            writer = self._read_writer_locked(checked)
        blocked = writer["generation_fence"]["blocked"] is True
        return {
            "schema_version": "subject_luna_admission_v1",
            "subject": checked,
            "route": "next_batch" if blocked else "current_batch",
            "read_session_allowed": not blocked,
            "reason": (
                "subject_generation_write_fence" if blocked else "admitted"
            ),
            "fencing_token": writer["fencing_token"] if blocked else None,
            "other_subjects_unchanged": True,
            "formal_write_count": 0,
        }

    def submit_luna_under_generation_fence(
        self,
        subject: str,
        submitter: Callable[[Any], _T],
        task: Any,
    ) -> _T:
        """Atomically recheck the subject fence and claim through submitter."""

        checked = _subject(subject)
        with _FileLock(self._subject_lock_path(checked)):
            writer = self._read_writer_locked(checked)
            if writer["generation_fence"]["blocked"] is True:
                raise SubjectSolContractError("subject_generation_write_fence")
            return submitter(task)

    def read_global(self) -> dict[str, Any]:
        with _FileLock(self.global_lock_path):
            return copy.deepcopy(self._read_global_locked())

    def read_verified_quality_receipt(
        self, digest: str, *, subject: str, task: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Read-only Dashboard authority path for accepted/corrected outcome."""

        checked_subject = _subject(subject)
        receipt = validate_subject_quality_receipt_v1(
            self._read_content_addressed(
                self.receipt_root / "subject-quality",
                digest,
                "dashboard_subject_quality_receipt",
            )
        )
        self._verify_seal(receipt, purpose="subject-quality-receipt")
        if (
            receipt["subject"] != checked_subject
            or receipt["capture_id"] != task.get("capture_id")
            or receipt["unit_sha256"] != task.get("unit_sha256")
            or receipt["frozen_payload_sha256"]
            != task.get("frozen_payload_sha256")
            or receipt["proposal_sha256"] != task.get("proposal_sha256")
            or receipt["package_sha256"] != task.get("package_sha256")
            or receipt["review_outcome"] not in {"accepted", "corrected"}
        ):
            raise SubjectSolContractError("dashboard_quality_receipt_binding_invalid")
        return copy.deepcopy(receipt)

    def read_verified_quality_receipt_v2(
        self, digest: str, *, subject: str, task: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Reopen the immutable v2 authenticity receipt for one exact task."""

        checked_subject = _subject(subject)
        receipt = validate_subject_quality_receipt_v2(
            self._read_content_addressed(
                self.subject_quality_v2_root,
                digest,
                "subject_quality_receipt_v2",
            )
        )
        self._verify_seal(receipt, purpose="subject-quality-receipt-v2")
        if (
            receipt["subject"] != checked_subject
            or receipt["capture_id"] != task.get("capture_id")
            or receipt["unit_sha256"] != task.get("unit_sha256")
            or receipt["frozen_payload_sha256"]
            != task.get("frozen_payload_sha256")
            or receipt["package_sha256"] != task.get("package_sha256")
            or receipt["sol_handoff_envelope_sha256"]
            != task.get("sol_handoff_envelope_sha256")
            or receipt["execution_status"] != task.get("status")
        ):
            raise SubjectSolContractError(
                "subject_quality_receipt_v2_binding_invalid"
            )
        return copy.deepcopy(receipt)


# Deliberately no daily_sol_batch_v1 compatibility: v1 bound one package and
# cannot prove a complete subject-quality closure.


def partition_multi_agent_sol_handoffs(
    handoffs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Partition V2 items without letting quality issues hide clean work."""

    if not handoffs:
        raise SubjectSolContractError("multi_agent_handoff_batch_empty")
    candidate_ids: list[str] = []
    clean_ids: list[str] = []
    issue_ids: list[str] = []
    diagnostic_ids: list[str] = []
    blocking_ids: list[str] = []
    seen: set[str] = set()
    for handoff in handoffs:
        capture_id = handoff.get("capture_id")
        outcome = handoff.get("quality_status")
        if not isinstance(capture_id, str) or not capture_id or capture_id in seen:
            raise SubjectSolContractError("multi_agent_handoff_identity_invalid")
        seen.add(capture_id)
        if handoff.get("formal_write_count") != 0:
            raise SubjectSolContractError("multi_agent_handoff_formal_write_nonzero")
        if outcome in {"accepted", "corrected"}:
            if handoff.get("sol_review_ready") is not True or handoff.get("quality_clean") is not True:
                blocking_ids.append(capture_id)
                continue
            candidate_ids.append(capture_id)
            clean_ids.append(capture_id)
        elif outcome == "issues_found":
            if (
                handoff.get("execution_status") != "succeeded"
                or handoff.get("sol_review_ready") is not True
                or handoff.get("quality_clean") is not False
                or handoff.get("candidate_preserved") is not True
                or handoff.get("risk_report_preserved") is not True
                or handoff.get("automatic_retry") is not False
            ):
                blocking_ids.append(capture_id)
                continue
            candidate_ids.append(capture_id)
            issue_ids.append(capture_id)
        elif outcome == "technical_quarantine":
            if handoff.get("diagnostic_review_ready") is True:
                diagnostic_ids.append(capture_id)
            else:
                blocking_ids.append(capture_id)
        else:
            blocking_ids.append(capture_id)
    return {
        "schema_version": "subject_multi_agent_handoff_partition_v1",
        "all_terminal": not blocking_ids,
        "sol_review_ready": bool(candidate_ids) and not blocking_ids,
        "sol_candidate_task_ids": candidate_ids,
        "quality_clean_task_ids": clean_ids,
        "issues_found_task_ids": issue_ids,
        "diagnostic_task_ids": diagnostic_ids,
        "blocking_task_ids": blocking_ids,
        "formal_write_count": 0,
    }


__all__ = [
    "DAILY_SOL_BATCH_SCHEMA",
    "DAILY_SOL_BATCH_V3_SCHEMA",
    "ENGLISH_LEGACY_BATCH_AUTHORIZATION_SCHEMA",
    "ENGLISH_LEGACY_EXECUTION_CLOSURE_SCHEMA",
    "ENGLISH_LEGACY_ITEM_APPLY_SCHEMA",
    "ENGLISH_LEGACY_ITEM_FAILURE_SCHEMA",
    "ENGLISH_LEGACY_ITEM_RECOVERY_SCHEMA",
    "ENGLISH_LEGACY_ITEM_REVIEW_SCHEMA",
    "ENGLISH_LEGACY_ROLLING_CHECKPOINT_SCHEMA",
    "ENGLISH_LEGACY_SOL_BATCH_SCHEMA",
    "ENGLISH_LEGACY_SOL_WORK_ITEM_SCHEMA",
    "GLOBAL_SOL_WRITER_SCHEMA",
    "SOL_COMMIT_RECEIPT_SCHEMA",
    "SOL_COMMIT_RECEIPT_V2_SCHEMA",
    "SOL_REVIEW_RECEIPT_SCHEMA",
    "SOL_REVIEW_RECEIPT_V2_SCHEMA",
    "SOL_TASK_HANDOFF_SCHEMA",
    "SUBJECT_AUTHORITY_OBSERVATION_SCHEMA",
    "SUBJECT_BATCH_SCHEMA",
    "SUBJECT_BATCH_V2_SCHEMA",
    "SUBJECT_EXCLUSION_RECEIPT_SCHEMA",
    "SUBJECT_QUALITY_RECEIPT_SCHEMA",
    "SUBJECT_QUALITY_RECEIPT_V2_SCHEMA",
    "SUBJECT_GENERATION_AUTHORITY_ACK_SCHEMA",
    "SUBJECT_WRITER_ADAPTERS",
    "SubjectSolContractError",
    "SubjectSolRuntimeStore",
    "build_english_legacy_sol_work_item_v1",
    "partition_multi_agent_sol_handoffs",
    "validate_daily_sol_batch_v2",
    "validate_daily_sol_batch_v3",
    "validate_english_legacy_batch_authorization_v1",
    "validate_english_legacy_recuration_execution_closure_v1",
    "validate_english_legacy_recuration_sol_batch_v1",
    "validate_english_legacy_rolling_authority_checkpoint_v1",
    "validate_english_legacy_sol_item_apply_receipt_v1",
    "validate_english_legacy_sol_item_failure_receipt_v1",
    "validate_english_legacy_sol_item_recovery_receipt_v1",
    "validate_english_legacy_sol_item_review_receipt_v1",
    "validate_sol_commit_receipt_v1",
    "validate_sol_commit_receipt_v2",
    "validate_sol_review_receipt_v1",
    "validate_sol_review_receipt_v2",
    "validate_sol_task_handoff_envelope_v1",
    "validate_subject_exclusion_receipt_v1",
    "validate_subject_luna_batch_v1",
    "validate_subject_luna_batch_v2",
    "validate_subject_quality_receipt_v1",
    "validate_subject_quality_receipt_v2",
    "validate_user_sol_authorization_receipt_v2",
    "recompute_subject_luna_batch_v2",
    "transition_subject_luna_task_v2",
]
