#!/usr/bin/env python3
"""Private, content-addressed evidence for one current 408 question.

The public capture ledger stores only the ``current-question-evidence://``
locator and the manifest digest.  Question text, options, answers, learner
reply, and feedback never enter the repository through this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import datetime as dt
import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from image_integrity_408 import ImageIntegrityError, validate_image_bytes


BUNDLE_SCHEMA = "current-question-evidence-bundle-v1"
BUNDLE_SCHEMA_V2 = "current-question-evidence-bundle-v2"
BUNDLE_SCHEMA_V3 = "current-question-evidence-bundle-v3"
MANIFEST_SCHEMA = "current-question-evidence-manifest-v1"
EVALUATION_CAPSULE_SCHEMA = "current-question-evaluation-capsule-v1"
STUDY_OBSERVATION_SCHEMA = "current-question-study-observation-v1"
ANSWER_OPERATION_SCHEMA = "answer-current-and-next-operation-v1"
ANSWER_DISPLAY_LIFECYCLE_SCHEMA = "answer-current-display-lifecycle-v1"
TRACE_SUPPLEMENT_SCHEMA = "current-question-trace-supplement-v1"
TRACE_SUPPLEMENT_BINDING_SCHEMA = "current-question-trace-supplement-binding-v1"
BACKGROUND_HANDOFF_SCHEMA = "current-question-background-handoff-v1"
BACKGROUND_HANDOFF_BINDING_SCHEMA = (
    "current-question-background-handoff-binding-v1"
)
RECOVERY_SCHEMA = "current-question-capture-recovery-v1"
TURN_RECEIPT_SCHEMA = "current-question-turn-receipt-v1"
DEFAULT_PRIVATE_ROOT = (
    Path.home()
    / ".codex"
    / "study-intake-preprocessor"
    / "private"
    / "current-question-evidence"
)
LOCATOR_PREFIX = "current-question-evidence://sha256/"
ATTACHMENT_LOCATOR_PREFIX = "current-question-attachment://sha256/"
RECOVERY_LOCATOR_PREFIX = "current-question-recovery://sha256/"
TURN_LOCATOR_PREFIX = "current-question-turn://sha256/"
EVALUATION_LOCATOR_PREFIX = "current-question-evaluation://sha256/"
STUDY_OBSERVATION_LOCATOR_PREFIX = "current-question-study-observation://sha256/"
TRACE_SUPPLEMENT_LOCATOR_PREFIX = "current-question-trace-supplement://sha256/"
BACKGROUND_HANDOFF_LOCATOR_PREFIX = (
    "current-question-background-handoff://sha256/"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_BUNDLE_BYTES = 256 * 1024
MAX_METADATA_BYTES = 512 * 1024
MAX_ATTACHMENT_BYTES = 16 * 1024 * 1024
MAX_ATTACHMENTS = 8
MAX_OPERATION_BYTES = 768 * 1024
EVALUATION_CAPSULE_KEYS = {
    "schema_version",
    "capsule_id",
    "session_id",
    "item_id",
    "source_id",
    "source_stable",
    "source_binding_sha256",
    "context_id",
    "evaluation_evidence",
    "formal_write_count",
}
BUNDLE_KEYS = {
    "schema_version",
    "context_id",
    "request_id",
    "session_id",
    "item_id",
    "source_id",
    "source_kind",
    "study_date",
    "event_time",
    "timezone",
    "source_binding_sha256",
    "current_question",
    "learner_evidence",
    "evaluation_evidence",
    "assessment",
    "frozen_assistant_feedback",
    "provenance",
    "missing_fields",
    "attachment_objects",
    "formal_write_count",
}
BUNDLE_V2_KEYS = BUNDLE_KEYS | {"interaction_trace"}
BUNDLE_V3_KEYS = BUNDLE_KEYS | {"question_mode", "interaction_trace"}
INTERACTION_TRACE_KEYS = {"schema", "events", "event_count", "truncated"}
INTERACTION_TRACE_V2_KEYS = {
    "schema",
    "events",
    "original_event_count",
    "included_event_count",
    "omitted_event_count",
    "omitted_ranges",
    "truncation_reason",
    "full_trace_sha256",
}
INTERACTION_TRACE_EVENT_KEYS = {"ordinal", "role", "kind", "text"}
STUDY_OBSERVATION_KEYS = {
    "schema_version",
    "observation_id",
    "context_id",
    "request_id",
    "session_id",
    "item_id",
    "source_id",
    "study_date",
    "event_time",
    "first_result",
    "evidence_locator",
    "evidence_manifest_sha256",
    "first_answer_receipt_sha256",
    "processing_status",
    "formal_write_count",
}
ANSWER_OPERATION_KEYS = {
    "schema",
    "operation_id",
    "input_sha256",
    "status",
    "event_time",
    "prepared",
    "result",
    "formal_write_count",
}
ANSWER_DISPLAY_LIFECYCLE_KEYS = {
    "schema",
    "display_receipt_sha256",
    "status",
    "first_operation_id",
    "first_result",
    "first_turn",
    "interaction_trace",
    "resolution",
    "formal_write_count",
}
TRACE_SUPPLEMENT_KEYS = {
    "schema_version",
    "capture_id",
    "context_id",
    "item_id",
    "evidence_manifest_sha256",
    "created_at",
    "supplement_kind",
    "resolution_receipt_sha256",
    "events",
    "interaction_trace_sha256",
    "formal_write_count",
}
TRACE_SUPPLEMENT_EVENT_KEYS = {"role", "kind", "text", "observed_at"}
TRACE_SUPPLEMENT_BINDING_KEYS = {
    "schema_version",
    "capture_id",
    "context_id",
    "item_id",
    "evidence_manifest_sha256",
    "locator",
    "object_sha",
    "created_at",
    "supplement_kind",
    "resolution_receipt_sha256",
    "formal_write_count",
}
BACKGROUND_HANDOFF_KEYS = {
    "schema_version",
    "capture_id",
    "context_id",
    "item_id",
    "evidence_manifest_sha256",
    "capture_receipt_sha256",
    "status",
    "completion_kind",
    "created_at",
    "updated_at",
    "trace_supplement_locator",
    "trace_supplement_object_sha256",
    "interaction_trace_sha256",
    "resolution_receipt_sha256",
    "formal_write_count",
}
BACKGROUND_HANDOFF_BINDING_KEYS = {
    "schema_version",
    "capture_id",
    "status",
    "object_sha256",
    "locator",
    "updated_at",
    "formal_write_count",
}
CURRENT_QUESTION_KEYS = {
    "public_text",
    "options",
    "response_instruction",
    "public_surface_sha256",
    "attachment_sha256s",
}
LEARNER_EVIDENCE_KEYS = {
    "answer_text",
    "choice",
    "confidence",
    "first_action",
    "reasoning",
    "prompt_level",
    "observed_at",
}
EVALUATION_EVIDENCE_KEYS = {
    "grader_capsule_id",
    "correct_answer",
    "correct_option",
    "standard_explanation",
    "grader_result",
    "grader_basis",
    "provided_by",
    "missing_fields",
}
ASSESSMENT_KEYS = {
    "first_result",
    "choice_result",
    "reasoning_result",
    "confidence",
    "prompt_level",
    "first_break",
    "first_break_provenance",
    "first_action",
    "first_action_provenance",
}
FEEDBACK_KEYS = {"text", "sha256"}
PROVENANCE_KEYS = {
    "source_locator",
    "source_sha256",
    "context_sha256",
    "attachment_provenance",
    "created_at",
    "missing_items",
}
MANIFEST_KEYS = {
    "schema_version",
    "bundle_schema_version",
    "context_id",
    "request_id",
    "session_id",
    "item_id",
    "source_id",
    "source_kind",
    "study_date",
    "source_binding_sha256",
    "object_sha256",
    "object_size_bytes",
    "attachment_objects",
    "created_at",
    "answer_safe_manifest",
    "formal_write_count",
}


class CurrentQuestionEvidenceError(RuntimeError):
    """A private evidence object was unsafe, invalid, or drifted."""


def _json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    options: dict[str, Any] = {"ensure_ascii": False, "sort_keys": True}
    if pretty:
        options["indent"] = 2
    else:
        options["separators"] = (",", ":")
    return (json.dumps(value, **options) + "\n").encode("utf-8")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _private_root(root: str | Path | None) -> Path:
    raw_path = (
        Path(root).expanduser()
        if root is not None
        else DEFAULT_PRIVATE_ROOT
    )
    if raw_path.exists() and stat.S_ISLNK(raw_path.lstat().st_mode):
        raise CurrentQuestionEvidenceError("private evidence root cannot be a symlink")
    path = raw_path.resolve()
    if path == Path(path.anchor):
        raise CurrentQuestionEvidenceError("private evidence root is too broad")
    return path


def _ensure_private_directory(path: Path, *, boundary: Path) -> None:
    try:
        path.relative_to(boundary)
    except ValueError as exc:
        raise CurrentQuestionEvidenceError("private evidence path escaped root") from exc
    try:
        boundary.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        raise CurrentQuestionEvidenceError(
            "current_question_evidence_permissions_unsafe"
        ) from exc
    relative_parts = path.relative_to(boundary).parts
    private_directories = [boundary]
    private_directories.extend(
        boundary.joinpath(*relative_parts[:index])
        for index in range(1, len(relative_parts) + 1)
    )
    for cursor in private_directories:
        try:
            if not cursor.exists():
                cursor.mkdir(mode=0o700)
            info = cursor.lstat()
        except OSError as exc:
            raise CurrentQuestionEvidenceError(
                "current_question_evidence_permissions_unsafe"
            ) from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
        ):
            raise CurrentQuestionEvidenceError(
                "current_question_evidence_permissions_unsafe"
            )
        try:
            os.chmod(cursor, 0o700)
        except OSError as exc:
            raise CurrentQuestionEvidenceError(
                "current_question_evidence_permissions_unsafe"
            ) from exc


def _atomic_private_write(path: Path, raw: bytes, *, root: Path) -> None:
    _ensure_private_directory(path.parent, boundary=root)
    if path.exists():
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise CurrentQuestionEvidenceError("private evidence object is unsafe")
        if path.read_bytes() != raw:
            raise CurrentQuestionEvidenceError("content-addressed evidence conflict")
        os.chmod(path, 0o600)
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if tmp.exists():
            tmp.unlink()


def _atomic_private_replace(path: Path, raw: bytes, *, root: Path) -> None:
    """Atomically replace one bounded private operation state."""

    _ensure_private_directory(path.parent, boundary=root)
    if path.exists():
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise CurrentQuestionEvidenceError("private operation state is unsafe")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if tmp.exists():
            tmp.unlink()


def _read_object(path: Path, digest: str, *, root: Path, maximum: int) -> dict[str, Any]:
    if not SHA256_RE.fullmatch(digest):
        raise CurrentQuestionEvidenceError("private evidence digest is invalid")
    try:
        path.relative_to(root)
        info = path.lstat()
    except (OSError, ValueError) as exc:
        raise CurrentQuestionEvidenceError("private evidence object is missing") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > maximum
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise CurrentQuestionEvidenceError("private evidence permissions are unsafe")
    raw = path.read_bytes()
    if len(raw) != info.st_size or _sha256(raw) != digest:
        raise CurrentQuestionEvidenceError("private evidence content drifted")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CurrentQuestionEvidenceError("private evidence JSON is invalid") from exc
    if not isinstance(value, dict):
        raise CurrentQuestionEvidenceError("private evidence object must be a mapping")
    return value


def _exact_mapping(value: object, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise CurrentQuestionEvidenceError(f"{label} fields are incomplete or unknown")
    return value


def _bounded_string(
    value: object,
    maximum: int,
    label: str,
    *,
    nullable: bool = True,
    allow_empty: bool = True,
) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise CurrentQuestionEvidenceError(f"{label} must be text")
    if len(value.encode("utf-8")) > maximum or (not allow_empty and not value):
        raise CurrentQuestionEvidenceError(f"{label} exceeds its canonical boundary")
    return value


def _text_list(value: object, maximum_items: int, item_maximum: int, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or len(value) > maximum_items
        or any(
            not isinstance(item, str)
            or not item
            or len(item.encode("utf-8")) > item_maximum
            for item in value
        )
    ):
        raise CurrentQuestionEvidenceError(f"{label} is invalid")
    return value


def _parse_time(value: object, label: str) -> None:
    text = _bounded_string(value, 80, label, nullable=False, allow_empty=False)
    try:
        dt.datetime.fromisoformat(str(text))
    except ValueError as exc:
        raise CurrentQuestionEvidenceError(f"{label} is invalid") from exc


def _validate_attachment_rows(value: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > MAX_ATTACHMENTS:
        raise CurrentQuestionEvidenceError(f"{label} is invalid")
    for row in value:
        if (
            not isinstance(row, dict)
            or set(row) != {"sha256", "mime_type", "size_bytes", "label"}
            or not SHA256_RE.fullmatch(str(row.get("sha256") or ""))
            or not isinstance(row.get("size_bytes"), int)
            or isinstance(row.get("size_bytes"), bool)
            or not 1 <= row["size_bytes"] <= MAX_ATTACHMENT_BYTES
        ):
            raise CurrentQuestionEvidenceError(f"{label} row is invalid")
        _bounded_string(
            row.get("mime_type"), 80, f"{label}.mime_type", nullable=False, allow_empty=False
        )
        _bounded_string(
            row.get("label"), 160, f"{label}.label", nullable=False, allow_empty=False
        )
    return value


def _validate_attachment_rows_v3(
    value: object, label: str
) -> list[dict[str, Any]]:
    keys = {
        "ordinal",
        "role",
        "original_bytes_ref",
        "sha256",
        "declared_mime_type",
        "detected_mime_type",
        "detected_format",
        "byte_count",
        "label",
    }
    if not isinstance(value, list) or len(value) > MAX_ATTACHMENTS:
        raise CurrentQuestionEvidenceError(f"{label} is invalid")
    for ordinal, row in enumerate(value, start=1):
        digest = str(row.get("sha256") or "") if isinstance(row, dict) else ""
        if (
            not isinstance(row, dict)
            or set(row) != keys
            or row.get("ordinal") != ordinal
            or row.get("role") not in {"question_image", "solution_image"}
            or not SHA256_RE.fullmatch(digest)
            or row.get("original_bytes_ref") != ATTACHMENT_LOCATOR_PREFIX + digest
            or row.get("declared_mime_type")
            not in {"image/png", "image/jpeg", "image/webp"}
            or row.get("detected_mime_type") != row.get("declared_mime_type")
            or row.get("detected_format") not in {"png", "jpeg", "webp"}
            or not isinstance(row.get("byte_count"), int)
            or isinstance(row.get("byte_count"), bool)
            or not 1 <= row["byte_count"] <= MAX_ATTACHMENT_BYTES
        ):
            raise CurrentQuestionEvidenceError(f"{label} row is invalid")
        expected_format = {
            "image/png": "png",
            "image/jpeg": "jpeg",
            "image/webp": "webp",
        }[row["detected_mime_type"]]
        if row["detected_format"] != expected_format:
            raise CurrentQuestionEvidenceError(f"{label} row is invalid")
        _bounded_string(
            row.get("label"), 160, f"{label}.label", nullable=False, allow_empty=False
        )
    return value


def _verified_image_format(data: bytes) -> tuple[str, str]:
    """Map the shared full-byte validator into the evidence error contract."""

    try:
        return validate_image_bytes(data)
    except ImageIntegrityError as exc:
        if exc.code == "signature_unsupported":
            message = "private attachment image magic is invalid"
        elif exc.code == "decoder_unavailable":
            message = "private attachment image decoder is unavailable"
        else:
            message = "private attachment image integrity is invalid"
        raise CurrentQuestionEvidenceError(message) from exc


def _validate_bundle(bundle: object) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        raise CurrentQuestionEvidenceError("current-question bundle must be a mapping")
    schema = bundle.get("schema_version")
    expected_keys = (
        BUNDLE_V3_KEYS
        if schema == BUNDLE_SCHEMA_V3
        else BUNDLE_V2_KEYS
        if schema == BUNDLE_SCHEMA_V2
        else BUNDLE_KEYS
    )
    value = _exact_mapping(bundle, expected_keys, "current-question bundle")
    if schema not in {BUNDLE_SCHEMA, BUNDLE_SCHEMA_V2, BUNDLE_SCHEMA_V3}:
        raise CurrentQuestionEvidenceError("current-question bundle schema is invalid")
    if value.get("formal_write_count") != 0:
        raise CurrentQuestionEvidenceError("private bundle cannot claim formal writes")
    for key in ("context_id", "request_id", "source_id"):
        _bounded_string(value.get(key), 256, key, nullable=False, allow_empty=False)
    _bounded_string(value.get("source_kind"), 80, "source_kind", nullable=False, allow_empty=False)
    for key in ("session_id", "item_id"):
        _bounded_string(value.get(key), 256, key)
    _parse_time(value.get("event_time"), "event_time")
    current = _exact_mapping(
        value.get("current_question"), CURRENT_QUESTION_KEYS, "current_question"
    )
    learner = _exact_mapping(
        value.get("learner_evidence"), LEARNER_EVIDENCE_KEYS, "learner_evidence"
    )
    evaluation = _exact_mapping(
        value.get("evaluation_evidence"),
        EVALUATION_EVIDENCE_KEYS,
        "evaluation_evidence",
    )
    assessment = _exact_mapping(value.get("assessment"), ASSESSMENT_KEYS, "assessment")
    feedback = _exact_mapping(
        value.get("frozen_assistant_feedback"),
        FEEDBACK_KEYS,
        "frozen_assistant_feedback",
    )
    provenance = _exact_mapping(value.get("provenance"), PROVENANCE_KEYS, "provenance")
    if schema == BUNDLE_SCHEMA_V2:
        trace = _exact_mapping(
            value.get("interaction_trace"),
            INTERACTION_TRACE_KEYS,
            "interaction_trace",
        )
        if (
            trace.get("schema") != "current-question-interaction-trace-v1"
            or not isinstance(trace.get("truncated"), bool)
            or not isinstance(trace.get("event_count"), int)
            or isinstance(trace.get("event_count"), bool)
        ):
            raise CurrentQuestionEvidenceError("interaction_trace metadata is invalid")
        events = trace.get("events")
        if (
            not isinstance(events, list)
            or len(events) > 24
            or trace["event_count"] != len(events)
        ):
            raise CurrentQuestionEvidenceError("interaction_trace events are invalid")
        for ordinal, event in enumerate(events, start=1):
            row = _exact_mapping(
                event, INTERACTION_TRACE_EVENT_KEYS, "interaction_trace event"
            )
            if (
                row.get("ordinal") != ordinal
                or row.get("role") not in {"learner", "assistant"}
                or row.get("kind") not in {
                    "utterance",
                    "first_action",
                    "reasoning",
                    "hint",
                    "correction",
                    "restatement",
                    "answer",
                }
            ):
                raise CurrentQuestionEvidenceError("interaction_trace event is invalid")
            _bounded_string(
                row.get("text"),
                2048,
                "interaction_trace.text",
                nullable=False,
                allow_empty=False,
            )
        if len(_json_bytes(trace)) > 32 * 1024:
            raise CurrentQuestionEvidenceError("interaction_trace exceeds size limit")
    elif schema == BUNDLE_SCHEMA_V3:
        if value.get("question_mode") not in {"dialogue_only", "image_question"}:
            raise CurrentQuestionEvidenceError("question_mode is invalid")
        trace = _exact_mapping(
            value.get("interaction_trace"),
            INTERACTION_TRACE_V2_KEYS,
            "interaction_trace",
        )
        original_count = trace.get("original_event_count")
        included_count = trace.get("included_event_count")
        omitted_count = trace.get("omitted_event_count")
        events = trace.get("events")
        ranges = trace.get("omitted_ranges")
        if (
            trace.get("schema") != "current-question-interaction-trace-v2"
            or not isinstance(original_count, int)
            or isinstance(original_count, bool)
            or not isinstance(included_count, int)
            or isinstance(included_count, bool)
            or not isinstance(omitted_count, int)
            or isinstance(omitted_count, bool)
            or original_count < 0
            or not isinstance(events, list)
            or included_count != len(events)
            or included_count > 24
            or omitted_count != original_count - included_count
            or not isinstance(ranges, list)
            or not SHA256_RE.fullmatch(str(trace.get("full_trace_sha256") or ""))
            or (omitted_count == 0 and (ranges or trace.get("truncation_reason") is not None))
            or (
                omitted_count > 0
                and (
                    not ranges
                    or not isinstance(trace.get("truncation_reason"), str)
                    or not trace["truncation_reason"]
                )
            )
        ):
            raise CurrentQuestionEvidenceError("interaction_trace metadata is invalid")
        last_ordinal = 0
        for event in events:
            row = _exact_mapping(
                event, INTERACTION_TRACE_EVENT_KEYS, "interaction_trace event"
            )
            if (
                not isinstance(row.get("ordinal"), int)
                or isinstance(row.get("ordinal"), bool)
                or not last_ordinal < row["ordinal"] <= original_count
                or row.get("role") not in {"learner", "assistant"}
                or row.get("kind")
                not in {
                    "utterance",
                    "first_action",
                    "reasoning",
                    "hint",
                    "correction",
                    "restatement",
                    "answer",
                }
            ):
                raise CurrentQuestionEvidenceError("interaction_trace event is invalid")
            last_ordinal = row["ordinal"]
            _bounded_string(
                row.get("text"), 2048, "interaction_trace.text", nullable=False, allow_empty=False
            )
        omitted_total = 0
        omitted_ordinals: set[int] = set()
        previous_end = 0
        for item in ranges:
            if (
                not isinstance(item, dict)
                or set(item) != {"start_ordinal", "end_ordinal"}
                or not isinstance(item.get("start_ordinal"), int)
                or isinstance(item.get("start_ordinal"), bool)
                or not isinstance(item.get("end_ordinal"), int)
                or isinstance(item.get("end_ordinal"), bool)
                or not previous_end < item["start_ordinal"] <= item["end_ordinal"] <= original_count
            ):
                raise CurrentQuestionEvidenceError("interaction_trace omitted ranges are invalid")
            previous_end = item["end_ordinal"]
            omitted_total += item["end_ordinal"] - item["start_ordinal"] + 1
            omitted_ordinals.update(
                range(item["start_ordinal"], item["end_ordinal"] + 1)
            )
        included_ordinals = {event["ordinal"] for event in events}
        if (
            omitted_total != omitted_count
            or included_ordinals & omitted_ordinals
            or included_ordinals | omitted_ordinals
            != set(range(1, original_count + 1))
            or len(_json_bytes(trace)) > 32 * 1024
        ):
            raise CurrentQuestionEvidenceError("interaction_trace omitted ranges are invalid")
        if omitted_count == 0 and trace.get("full_trace_sha256") != _sha256(
            _json_bytes(events).rstrip(b"\n")
        ):
            raise CurrentQuestionEvidenceError("interaction_trace full digest is invalid")
    _bounded_string(current.get("public_text"), 20000, "current_question.public_text", nullable=False)
    _bounded_string(
        current.get("response_instruction"),
        4000,
        "current_question.response_instruction",
        nullable=False,
    )
    options = current.get("options")
    if not isinstance(options, list) or len(options) > 12:
        raise CurrentQuestionEvidenceError("current_question.options is invalid")
    for option in options:
        if isinstance(option, str):
            _bounded_string(option, 4000, "current_question.options", nullable=False)
        elif isinstance(option, dict) and set(option) == {"label", "text"}:
            _bounded_string(option["label"], 4000, "current_question.option.label", nullable=False)
            _bounded_string(option["text"], 4000, "current_question.option.text", nullable=False)
        else:
            raise CurrentQuestionEvidenceError("current_question option is invalid")
    attachment_sha256s = current.get("attachment_sha256s")
    if (
        not isinstance(attachment_sha256s, list)
        or len(attachment_sha256s) > MAX_ATTACHMENTS
        or any(not SHA256_RE.fullmatch(str(item or "")) for item in attachment_sha256s)
    ):
        raise CurrentQuestionEvidenceError("current_question attachment hashes are invalid")
    public_surface = current.get("public_surface_sha256")
    if public_surface is not None and not SHA256_RE.fullmatch(str(public_surface)):
        raise CurrentQuestionEvidenceError("current_question public surface hash is invalid")

    for key, maximum in {
        "answer_text": 12000,
        "choice": 80,
        "confidence": 80,
        "first_action": 4000,
        "reasoning": 16000,
        "prompt_level": 80,
        "observed_at": 80,
    }.items():
        _bounded_string(learner.get(key), maximum, f"learner_evidence.{key}")
    for key, maximum in {
        "grader_capsule_id": 256,
        "correct_answer": 8000,
        "correct_option": 80,
        "standard_explanation": 24000,
        "grader_result": 80,
        "grader_basis": 8000,
        "provided_by": 160,
    }.items():
        _bounded_string(evaluation.get(key), maximum, f"evaluation_evidence.{key}")
    _text_list(evaluation.get("missing_fields"), 64, 4000, "evaluation_evidence.missing_fields")
    for key, maximum in {
        "first_result": 80,
        "choice_result": 80,
        "reasoning_result": 80,
        "confidence": 80,
        "prompt_level": 80,
        "first_break": 4000,
        "first_break_provenance": 80,
        "first_action": 4000,
        "first_action_provenance": 80,
    }.items():
        _bounded_string(assessment.get(key), maximum, f"assessment.{key}")
    feedback_text = feedback.get("text")
    if (
        not isinstance(feedback_text, str)
        or len(feedback_text.encode("utf-8")) > 16000
        or feedback.get("sha256") != _sha256(feedback_text.encode("utf-8"))
    ):
        raise CurrentQuestionEvidenceError("frozen feedback binding is invalid")
    for key in ("source_binding_sha256",):
        if not SHA256_RE.fullmatch(str(value.get(key) or "")):
            raise CurrentQuestionEvidenceError(f"private bundle {key} is invalid")
    if value.get("timezone") != "Asia/Shanghai":
        raise CurrentQuestionEvidenceError("private bundle timezone is invalid")
    if value.get("attachment_objects") != []:
        raise CurrentQuestionEvidenceError("attachments must be bound by the writer")
    if value["provenance"].get("attachment_provenance") != []:
        raise CurrentQuestionEvidenceError("attachment provenance must be bound by the writer")
    _text_list(value.get("missing_fields"), 64, 4000, "missing_fields")
    source_locator = provenance.get("source_locator")
    if source_locator is not None:
        locator = _bounded_string(source_locator, 1000, "provenance.source_locator") or ""
        lowered = locator.lower()
        if (
            locator.startswith(("/", "~"))
            or lowered.startswith(("file:", "http:", "https:"))
            or ".." in locator
            or any(marker in lowered for marker in ("history", "related", "variant", "formal_library"))
        ):
            raise CurrentQuestionEvidenceError("provenance.source_locator is unsafe")
    for key in ("source_sha256", "context_sha256"):
        if not SHA256_RE.fullmatch(str(provenance.get(key) or "")):
            raise CurrentQuestionEvidenceError(f"provenance.{key} is invalid")
    _parse_time(provenance.get("created_at"), "provenance.created_at")
    _text_list(provenance.get("missing_items"), 64, 4000, "provenance.missing_items")
    if provenance.get("attachment_provenance") != []:
        raise CurrentQuestionEvidenceError("attachment provenance must be bound by the writer")
    return value


def stage_attachments(
    attachments: list[dict[str, Any]] | None,
    *,
    private_root: str | Path | None = None,
    question_mode: str | None = None,
) -> list[dict[str, Any]]:
    root = _private_root(private_root)
    attachment_rows: list[dict[str, Any]] = []
    pending_writes: list[tuple[str, bytes]] = []
    if len(attachments or []) > MAX_ATTACHMENTS:
        raise CurrentQuestionEvidenceError("too many private attachments")
    for attachment in attachments or []:
        if question_mode is not None:
            if not isinstance(attachment, dict) or set(attachment) != {
                "data",
                "mime_type",
                "role",
                "label",
            }:
                raise CurrentQuestionEvidenceError("private attachment shape is invalid")
            data = attachment["data"]
            declared_mime = str(attachment["mime_type"] or "").strip().lower()
            role = str(attachment["role"] or "").strip()
            label = str(attachment["label"] or "").strip()
            if not isinstance(data, bytes) or not 0 < len(data) <= MAX_ATTACHMENT_BYTES:
                raise CurrentQuestionEvidenceError("private attachment is invalid")
            detected_format, detected_mime = _verified_image_format(data)
            if (
                question_mode != "image_question"
                or role not in {"question_image", "solution_image"}
                or declared_mime != detected_mime
                or not label
                or len(label.encode("utf-8")) > 160
            ):
                raise CurrentQuestionEvidenceError("private attachment declaration is invalid")
            digest = _sha256(data)
            pending_writes.append((digest, data))
            attachment_rows.append(
                {
                    "ordinal": len(attachment_rows) + 1,
                    "role": role,
                    "original_bytes_ref": ATTACHMENT_LOCATOR_PREFIX + digest,
                    "sha256": digest,
                    "declared_mime_type": declared_mime,
                    "detected_mime_type": detected_mime,
                    "detected_format": detected_format,
                    "byte_count": len(data),
                    "label": label,
                }
            )
            continue
        if not isinstance(attachment, dict) or set(attachment) != {
            "data",
            "mime_type",
            "label",
        }:
            raise CurrentQuestionEvidenceError("private attachment shape is invalid")
        data = attachment["data"]
        mime_type = str(attachment["mime_type"] or "").strip()
        label = str(attachment["label"] or "").strip()
        if (
            not isinstance(data, bytes)
            or not 0 < len(data) <= MAX_ATTACHMENT_BYTES
            or not mime_type
            or len(mime_type.encode("utf-8")) > 80
            or not label
            or len(label.encode("utf-8")) > 160
        ):
            raise CurrentQuestionEvidenceError("private attachment is invalid")
        digest = _sha256(data)
        pending_writes.append((digest, data))
        attachment_rows.append(
            {
                "sha256": digest,
                "size_bytes": len(data),
                "mime_type": mime_type,
                "label": label,
            }
        )
    if question_mode is None:
        _validate_attachment_rows(attachment_rows, "attachment_objects")
    else:
        _validate_attachment_rows_v3(attachment_rows, "attachment_objects")
        roles = {str(row.get("role") or "") for row in attachment_rows}
        if question_mode == "dialogue_only" and attachment_rows:
            raise CurrentQuestionEvidenceError(
                "dialogue_only evidence cannot bind image attachments"
            )
        if question_mode == "image_question" and not {
            "question_image",
            "solution_image",
        } <= roles:
            raise CurrentQuestionEvidenceError(
                "image_question requires question_image and solution_image"
            )
        if question_mode not in {"dialogue_only", "image_question"}:
            raise CurrentQuestionEvidenceError("question_mode is invalid")
    for digest, data in pending_writes:
        _atomic_private_write(
            root / "attachments" / f"{digest}.bin", data, root=root
        )
    return attachment_rows


def _require_v3_bundle_complete(bundle: dict[str, Any]) -> None:
    question = bundle.get("current_question") or {}
    learner = bundle.get("learner_evidence") or {}
    evaluation = bundle.get("evaluation_evidence") or {}
    trace = bundle.get("interaction_trace") or {}
    if not str(question.get("public_text") or "").strip():
        raise CurrentQuestionEvidenceError("capture question text is required")
    options = question.get("options")
    if not isinstance(options, list) or len(options) != 4:
        raise CurrentQuestionEvidenceError("capture options A-D are required")
    labels: list[str] = []
    texts: list[str] = []
    for option in options:
        if not isinstance(option, dict) or set(option) != {"label", "text"}:
            raise CurrentQuestionEvidenceError("capture option shape is invalid")
        labels.append(str(option.get("label") or "").strip().upper())
        texts.append(str(option.get("text") or "").strip())
    if labels != ["A", "B", "C", "D"]:
        raise CurrentQuestionEvidenceError("capture option labels are invalid")
    if any(not text for text in texts) or len(set(texts)) != 4:
        raise CurrentQuestionEvidenceError("capture option texts are invalid")
    answer = str(learner.get("answer_text") or "").strip().upper()
    choice = str(learner.get("choice") or "").strip().upper()
    if answer not in {"A", "B", "C", "D"} or choice != answer:
        raise CurrentQuestionEvidenceError("capture learner answer is invalid")
    learner_answers = [
        str(event.get("text") or "").strip().upper()
        for event in trace.get("events") or []
        if isinstance(event, dict)
        and event.get("role") == "learner"
        and event.get("kind") == "answer"
    ]
    if choice not in learner_answers:
        raise CurrentQuestionEvidenceError("capture learner answer event is required")
    if not (
        str(evaluation.get("standard_explanation") or "").strip()
        or str(evaluation.get("grader_basis") or "").strip()
    ):
        raise CurrentQuestionEvidenceError("capture explanation is required")


def _verify_staged_attachments(
    rows: list[dict[str, Any]], *, private_root: str | Path | None, schema: str | None = None
) -> None:
    root = _private_root(private_root)
    if schema == BUNDLE_SCHEMA_V3:
        _validate_attachment_rows_v3(rows, "staged_attachment_objects")
    else:
        _validate_attachment_rows(rows, "staged_attachment_objects")
    for row in rows:
        path = root / "attachments" / f"{row['sha256']}.bin"
        try:
            info = path.lstat()
            data = path.read_bytes()
        except OSError as exc:
            raise CurrentQuestionEvidenceError("staged attachment is missing") from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != row.get("size_bytes", row.get("byte_count"))
            or stat.S_IMODE(info.st_mode) & 0o077
            or len(data) != info.st_size
            or _sha256(data) != row["sha256"]
        ):
            raise CurrentQuestionEvidenceError("staged attachment drifted")
        if schema == BUNDLE_SCHEMA_V3:
            detected_format, detected_mime = _verified_image_format(data)
            if (
                row.get("detected_format") != detected_format
                or row.get("detected_mime_type") != detected_mime
                or row.get("declared_mime_type") != detected_mime
            ):
                raise CurrentQuestionEvidenceError(
                    "staged attachment image declaration drifted"
                )


def bundle_evidence_status(bundle: dict[str, Any]) -> str:
    """Return the fail-closed semantic-producer eligibility of one bundle."""

    if bundle.get("schema_version") != BUNDLE_SCHEMA_V3:
        return "ready"
    roles = [
        str(row.get("role") or "")
        for row in bundle.get("attachment_objects") or []
        if isinstance(row, dict)
    ]
    if bundle.get("question_mode") == "image_question" and not {
        "question_image",
        "solution_image",
    } <= set(roles):
        return "evidence_pending"
    return "ready"


def publish_bundle(
    bundle: dict[str, Any],
    *,
    private_root: str | Path | None = None,
    attachments: list[dict[str, Any]] | None = None,
    staged_attachment_objects: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Persist one private bundle and return only its opaque public locator."""

    bundle = _validate_bundle(bundle)
    if bundle["schema_version"] == BUNDLE_SCHEMA_V3:
        _require_v3_bundle_complete(bundle)
    if attachments is not None and staged_attachment_objects is not None:
        raise CurrentQuestionEvidenceError("attachment inputs are mutually exclusive")
    attachment_rows = (
        stage_attachments(
            attachments,
            private_root=private_root,
            question_mode=(
                str(bundle.get("question_mode"))
                if bundle["schema_version"] == BUNDLE_SCHEMA_V3
                else None
            ),
        )
        if staged_attachment_objects is None
        else [dict(row) for row in staged_attachment_objects]
    )
    _verify_staged_attachments(
        attachment_rows,
        private_root=private_root,
        schema=str(bundle["schema_version"]),
    )
    root = _private_root(private_root)
    attachment_sha256s = [row["sha256"] for row in attachment_rows]
    declared_attachment_sha256s = bundle["current_question"].get(
        "attachment_sha256s"
    )
    question_mode = bundle.get("question_mode")
    evidence_status = "ready"
    if bundle["schema_version"] == BUNDLE_SCHEMA_V3:
        roles = [str(row["role"]) for row in attachment_rows]
        if question_mode == "dialogue_only" and attachment_rows:
            raise CurrentQuestionEvidenceError(
                "dialogue_only evidence cannot bind image attachments"
            )
        missing_roles = (
            {"question_image", "solution_image"} - set(roles)
            if question_mode == "image_question"
            else set()
        )
        if missing_roles:
            raise CurrentQuestionEvidenceError(
                "image_question requires question_image and solution_image"
            )
        if attachment_rows and declared_attachment_sha256s != attachment_sha256s:
            raise CurrentQuestionEvidenceError(
                "declared attachment hashes do not match staged attachment objects"
            )
        missing_fields = set(bundle.get("missing_fields") or [])
        missing_items = set(bundle["provenance"].get("missing_items") or [])
    elif declared_attachment_sha256s != attachment_sha256s:
        raise CurrentQuestionEvidenceError(
            "declared attachment hashes do not match staged attachment objects"
        )
    stored_bundle = {
        **bundle,
        "current_question": {
            **bundle["current_question"],
            "attachment_sha256s": (
                declared_attachment_sha256s
                if bundle["schema_version"] == BUNDLE_SCHEMA_V3
                else attachment_sha256s
            ),
        },
        "provenance": {
            **bundle["provenance"],
            "attachment_provenance": attachment_rows,
            **(
                {"missing_items": sorted(missing_items)}
                if bundle["schema_version"] == BUNDLE_SCHEMA_V3
                else {}
            ),
        },
        "attachment_objects": attachment_rows,
        **(
            {"missing_fields": sorted(missing_fields)}
            if bundle["schema_version"] == BUNDLE_SCHEMA_V3
            else {}
        ),
    }
    payload_raw = _json_bytes(stored_bundle, pretty=True)
    if not 0 < len(payload_raw) <= MAX_BUNDLE_BYTES:
        raise CurrentQuestionEvidenceError("current-question bundle exceeds size limit")
    object_sha = _sha256(payload_raw)
    object_path = root / "objects" / f"{object_sha}.json"
    _atomic_private_write(object_path, payload_raw, root=root)
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "bundle_schema_version": bundle["schema_version"],
        "context_id": str(bundle.get("context_id") or ""),
        "request_id": str(bundle.get("request_id") or ""),
        "source_id": str(bundle.get("source_id") or ""),
        "source_kind": str(bundle.get("source_kind") or ""),
        "session_id": str(bundle.get("session_id") or ""),
        "item_id": str(bundle.get("item_id") or ""),
        "study_date": str(bundle.get("study_date") or ""),
        "source_binding_sha256": str(bundle.get("source_binding_sha256") or ""),
        "object_sha256": object_sha,
        "object_size_bytes": len(payload_raw),
        "attachment_objects": attachment_rows,
        "created_at": str(bundle["provenance"].get("created_at") or ""),
        "answer_safe_manifest": True,
        "formal_write_count": 0,
    }
    manifest_raw = _json_bytes(manifest, pretty=True)
    manifest_sha = _sha256(manifest_raw)
    _atomic_private_write(
        root / "manifests" / f"{manifest_sha}.json", manifest_raw, root=root
    )
    return {
        "schema": MANIFEST_SCHEMA,
        "locator": LOCATOR_PREFIX + manifest_sha,
        "manifest_sha256": manifest_sha,
        "object_sha256": object_sha,
        "evidence_status": evidence_status,
        "formal_write_count": 0,
    }


def read_bundle(
    locator: str, *, private_root: str | Path | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = _private_root(private_root)
    if not isinstance(locator, str) or not locator.startswith(LOCATOR_PREFIX):
        raise CurrentQuestionEvidenceError("current-question locator is invalid")
    manifest_sha = locator[len(LOCATOR_PREFIX) :]
    manifest = _read_object(
        root / "manifests" / f"{manifest_sha}.json",
        manifest_sha,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    if (
        set(manifest) != MANIFEST_KEYS
        or manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("bundle_schema_version")
        not in {BUNDLE_SCHEMA, BUNDLE_SCHEMA_V2, BUNDLE_SCHEMA_V3}
        or manifest.get("answer_safe_manifest") is not True
        or manifest.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("current-question manifest is invalid")
    object_sha = str(manifest.get("object_sha256") or "")
    bundle = _read_object(
        root / "objects" / f"{object_sha}.json",
        object_sha,
        root=root,
        maximum=MAX_BUNDLE_BYTES,
    )
    if (
        bundle.get("schema_version") != manifest.get("bundle_schema_version")
        or bundle.get("context_id") != manifest.get("context_id")
        or bundle.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("current-question bundle binding drifted")
    for key in (
        "request_id",
        "session_id",
        "item_id",
        "source_id",
        "source_kind",
        "study_date",
        "source_binding_sha256",
    ):
        if bundle.get(key) != manifest.get(key):
            raise CurrentQuestionEvidenceError(
                f"current-question bundle {key} binding drifted"
            )
    if bundle.get("provenance", {}).get("created_at") != manifest.get("created_at"):
        raise CurrentQuestionEvidenceError("current-question created_at binding drifted")
    attachments = manifest.get("attachment_objects")
    if not isinstance(attachments, list) or attachments != bundle.get(
        "attachment_objects"
    ):
        raise CurrentQuestionEvidenceError("private attachment manifest drifted")
    actual_attachment_sha256s = [
        row.get("sha256") for row in attachments if isinstance(row, dict)
    ]
    declared_attachment_sha256s = bundle.get("current_question", {}).get(
        "attachment_sha256s"
    )
    if manifest.get("bundle_schema_version") == BUNDLE_SCHEMA_V3:
        _validate_bundle(
            {
                **bundle,
                "attachment_objects": [],
                "provenance": {
                    **bundle["provenance"],
                    "attachment_provenance": [],
                },
            }
        )
        _validate_attachment_rows_v3(attachments, "private attachment manifest")
        roles = [str(row["role"]) for row in attachments]
        if bundle.get("question_mode") == "dialogue_only" and attachments:
            raise CurrentQuestionEvidenceError("dialogue-only attachment binding drifted")
        if actual_attachment_sha256s and declared_attachment_sha256s != actual_attachment_sha256s:
            raise CurrentQuestionEvidenceError("current-question attachment binding drifted")
        if bundle.get("question_mode") == "image_question":
            missing_roles = {"question_image", "solution_image"} - set(roles)
            missing_markers = {
                str(item) for item in bundle.get("missing_fields", [])
            }
            if any(
                f"attachment_role.{role}" not in missing_markers
                for role in missing_roles
            ):
                raise CurrentQuestionEvidenceError(
                    "image-question readiness binding drifted"
                )
    elif declared_attachment_sha256s != actual_attachment_sha256s:
        raise CurrentQuestionEvidenceError("current-question attachment binding drifted")
    if bundle.get("provenance", {}).get("attachment_provenance") != attachments:
        raise CurrentQuestionEvidenceError("attachment provenance binding drifted")
    for row in attachments:
        if manifest.get("bundle_schema_version") != BUNDLE_SCHEMA_V3 and (
            not isinstance(row, dict)
            or set(row) != {"sha256", "mime_type", "size_bytes", "label"}
            or not SHA256_RE.fullmatch(str(row.get("sha256") or ""))
            or not isinstance(row.get("size_bytes"), int)
            or row["size_bytes"] <= 0
        ):
            raise CurrentQuestionEvidenceError("private attachment metadata is invalid")
        digest = str(row["sha256"])
        path = root / "attachments" / f"{digest}.bin"
        try:
            info = path.lstat()
            data = path.read_bytes()
        except OSError as exc:
            raise CurrentQuestionEvidenceError("private attachment is missing") from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != row.get("size_bytes", row.get("byte_count"))
            or stat.S_IMODE(info.st_mode) & 0o077
            or len(data) != info.st_size
            or _sha256(data) != digest
        ):
            raise CurrentQuestionEvidenceError("private attachment drifted")
        if manifest.get("bundle_schema_version") == BUNDLE_SCHEMA_V3:
            detected_format, detected_mime = _verified_image_format(data)
            if (
                row.get("detected_format") != detected_format
                or row.get("detected_mime_type") != detected_mime
                or row.get("declared_mime_type") != detected_mime
            ):
                raise CurrentQuestionEvidenceError(
                    "private attachment image declaration drifted"
                )
    return manifest, bundle


def publish_study_observation(
    value: dict[str, Any], *, private_root: str | Path | None = None
) -> dict[str, str]:
    """Publish a private non-wrong observation for independent correct evidence."""

    row = _exact_mapping(value, STUDY_OBSERVATION_KEYS, "study observation")
    if (
        row.get("schema_version") != STUDY_OBSERVATION_SCHEMA
        or row.get("formal_write_count") != 0
        or row.get("processing_status") != "awaiting_background_analysis"
        or row.get("first_result") != "independent_correct"
    ):
        raise CurrentQuestionEvidenceError("study observation metadata is invalid")
    for key in (
        "observation_id",
        "context_id",
        "request_id",
        "session_id",
        "item_id",
        "source_id",
        "study_date",
    ):
        _bounded_string(row.get(key), 256, key, nullable=False, allow_empty=False)
    _parse_time(row.get("event_time"), "event_time")
    if (
        not str(row.get("evidence_locator") or "").startswith(LOCATOR_PREFIX)
        or not SHA256_RE.fullmatch(str(row.get("evidence_manifest_sha256") or ""))
        or not SHA256_RE.fullmatch(str(row.get("first_answer_receipt_sha256") or ""))
    ):
        raise CurrentQuestionEvidenceError("study observation receipt binding is invalid")
    raw = _json_bytes(row, pretty=True)
    if not 0 < len(raw) <= MAX_METADATA_BYTES:
        raise CurrentQuestionEvidenceError("study observation exceeds size limit")
    root = _private_root(private_root)
    digest = _sha256(raw)
    _atomic_private_write(root / "observations" / f"{digest}.json", raw, root=root)
    return {
        "locator": STUDY_OBSERVATION_LOCATOR_PREFIX + digest,
        "sha256": digest,
    }


def read_study_observation(
    locator: str, *, private_root: str | Path | None = None
) -> dict[str, Any]:
    if not isinstance(locator, str) or not locator.startswith(
        STUDY_OBSERVATION_LOCATOR_PREFIX
    ):
        raise CurrentQuestionEvidenceError("study observation locator is invalid")
    digest = locator[len(STUDY_OBSERVATION_LOCATOR_PREFIX) :]
    root = _private_root(private_root)
    value = _read_object(
        root / "observations" / f"{digest}.json",
        digest,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    _exact_mapping(value, STUDY_OBSERVATION_KEYS, "study observation")
    if value.get("schema_version") != STUDY_OBSERVATION_SCHEMA:
        raise CurrentQuestionEvidenceError("study observation schema drifted")
    return value


def _validate_operation_id(operation_id: str) -> str:
    if not re.fullmatch(r"AON-[0-9A-F]{64}", str(operation_id or "")):
        raise CurrentQuestionEvidenceError("answer operation id is invalid")
    return operation_id


@contextmanager
def answer_operation_lock(
    operation_id: str, *, private_root: str | Path | None = None
) -> Iterator[None]:
    operation_id = _validate_operation_id(operation_id)
    root = _private_root(private_root)
    directory = root / "answer-operations"
    _ensure_private_directory(directory, boundary=root)
    lock_path = directory / f"{operation_id}.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def read_answer_operation(
    operation_id: str, *, private_root: str | Path | None = None
) -> dict[str, Any] | None:
    operation_id = _validate_operation_id(operation_id)
    root = _private_root(private_root)
    path = root / "answer-operations" / f"{operation_id}.json"
    if not path.exists():
        return None
    try:
        info = path.lstat()
    except OSError as exc:
        raise CurrentQuestionEvidenceError("answer operation is unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > MAX_OPERATION_BYTES
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise CurrentQuestionEvidenceError("answer operation permissions are unsafe")
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CurrentQuestionEvidenceError("answer operation JSON is invalid") from exc
    return _validate_answer_operation(value, operation_id)


def _validate_answer_operation(value: object, operation_id: str) -> dict[str, Any]:
    row = _exact_mapping(value, ANSWER_OPERATION_KEYS, "answer operation")
    if (
        row.get("schema") != ANSWER_OPERATION_SCHEMA
        or row.get("operation_id") != operation_id
        or row.get("status") not in {"prepared", "complete"}
        or row.get("formal_write_count") != 0
        or not SHA256_RE.fullmatch(str(row.get("input_sha256") or ""))
        or not isinstance(row.get("prepared"), dict)
    ):
        raise CurrentQuestionEvidenceError("answer operation metadata is invalid")
    _parse_time(row.get("event_time"), "answer operation event_time")
    if row["status"] == "complete" and not isinstance(row.get("result"), dict):
        raise CurrentQuestionEvidenceError("answer operation result is missing")
    if row["status"] == "prepared" and row.get("result") is not None:
        raise CurrentQuestionEvidenceError("prepared answer operation has a result")
    return row


def write_answer_operation(
    value: dict[str, Any], *, private_root: str | Path | None = None
) -> None:
    operation_id = _validate_operation_id(str(value.get("operation_id") or ""))
    row = _validate_answer_operation(value, operation_id)
    raw = _json_bytes(row, pretty=True)
    if not 0 < len(raw) <= MAX_OPERATION_BYTES:
        raise CurrentQuestionEvidenceError("answer operation exceeds size limit")
    root = _private_root(private_root)
    _atomic_private_replace(
        root / "answer-operations" / f"{operation_id}.json", raw, root=root
    )


@contextmanager
def answer_display_lock(
    display_receipt_sha256: str, *, private_root: str | Path | None = None
) -> Iterator[None]:
    if not SHA256_RE.fullmatch(str(display_receipt_sha256 or "")):
        raise CurrentQuestionEvidenceError("display receipt digest is invalid")
    root = _private_root(private_root)
    directory = root / "answer-display-lifecycles"
    _ensure_private_directory(directory, boundary=root)
    lock_path = directory / f"{display_receipt_sha256}.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _validate_display_lifecycle(
    value: object, display_receipt_sha256: str
) -> dict[str, Any]:
    row = _exact_mapping(
        value, ANSWER_DISPLAY_LIFECYCLE_KEYS, "answer display lifecycle"
    )
    if (
        row.get("schema") != ANSWER_DISPLAY_LIFECYCLE_SCHEMA
        or row.get("display_receipt_sha256") != display_receipt_sha256
        or row.get("status") not in {"first_recorded", "resolved"}
        or row.get("formal_write_count") != 0
        or not re.fullmatch(r"AON-[0-9A-F]{64}", str(row.get("first_operation_id") or ""))
        or row.get("first_result") not in {
            "wrong",
            "partial",
            "uncertain",
        }
        or not isinstance(row.get("first_turn"), dict)
    ):
        raise CurrentQuestionEvidenceError("answer display lifecycle is invalid")
    if row["status"] == "resolved" and not isinstance(row.get("resolution"), dict):
        raise CurrentQuestionEvidenceError("resolved display lifecycle lacks resolution")
    if row["status"] == "first_recorded" and row.get("resolution") is not None:
        raise CurrentQuestionEvidenceError("unresolved display lifecycle has resolution")
    raw_trace = row.get("interaction_trace")
    if not isinstance(raw_trace, dict):
        raise CurrentQuestionEvidenceError("answer display lifecycle trace is invalid")
    if raw_trace.get("schema") == "current-question-interaction-trace-v2":
        trace = _exact_mapping(
            raw_trace,
            INTERACTION_TRACE_V2_KEYS,
            "answer display lifecycle trace",
        )
        events = trace.get("events")
        original_count = trace.get("original_event_count")
        if (
            not isinstance(events, list)
            or not isinstance(original_count, int)
            or isinstance(original_count, bool)
            or trace.get("included_event_count") != len(events)
            or trace.get("omitted_event_count") != original_count - len(events)
            or len(events) > 24
            or not SHA256_RE.fullmatch(str(trace.get("full_trace_sha256") or ""))
        ):
            raise CurrentQuestionEvidenceError("answer display lifecycle trace is invalid")
    else:
        trace = _exact_mapping(
            raw_trace,
            INTERACTION_TRACE_KEYS,
            "answer display lifecycle trace",
        )
        events = trace.get("events")
        original_count = len(events) if isinstance(events, list) else -1
        if (
            trace.get("schema") != "current-question-interaction-trace-v1"
            or not isinstance(trace.get("truncated"), bool)
            or trace.get("event_count") != original_count
            or original_count > 24
        ):
            raise CurrentQuestionEvidenceError("answer display lifecycle trace is invalid")
    last_ordinal = 0
    for event in events:
        value = _exact_mapping(
            event, INTERACTION_TRACE_EVENT_KEYS, "answer display lifecycle event"
        )
        if (
            not isinstance(value.get("ordinal"), int)
            or isinstance(value.get("ordinal"), bool)
            or not last_ordinal < value["ordinal"] <= original_count
            or value.get("role") not in {"learner", "assistant"}
            or value.get("kind") not in {
                "utterance",
                "first_action",
                "reasoning",
                "hint",
                "correction",
                "restatement",
                "answer",
            }
        ):
            raise CurrentQuestionEvidenceError(
                "answer display lifecycle event is invalid"
            )
        last_ordinal = value["ordinal"]
        _bounded_string(
            value.get("text"),
            2048,
            "answer display lifecycle event text",
            nullable=False,
            allow_empty=False,
        )
    return row


def read_answer_display_lifecycle(
    display_receipt_sha256: str, *, private_root: str | Path | None = None
) -> dict[str, Any] | None:
    if not SHA256_RE.fullmatch(str(display_receipt_sha256 or "")):
        raise CurrentQuestionEvidenceError("display receipt digest is invalid")
    root = _private_root(private_root)
    path = root / "answer-display-lifecycles" / f"{display_receipt_sha256}.json"
    if not path.exists():
        return None
    try:
        info = path.lstat()
    except OSError as exc:
        raise CurrentQuestionEvidenceError("answer display lifecycle unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > MAX_OPERATION_BYTES
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise CurrentQuestionEvidenceError("answer display lifecycle is unsafe")
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CurrentQuestionEvidenceError(
            "answer display lifecycle JSON is invalid"
        ) from exc
    return _validate_display_lifecycle(value, display_receipt_sha256)


def write_answer_display_lifecycle(
    value: dict[str, Any], *, private_root: str | Path | None = None
) -> None:
    digest = str(value.get("display_receipt_sha256") or "")
    row = _validate_display_lifecycle(value, digest)
    raw = _json_bytes(row, pretty=True)
    if not 0 < len(raw) <= MAX_OPERATION_BYTES:
        raise CurrentQuestionEvidenceError("answer display lifecycle exceeds size limit")
    root = _private_root(private_root)
    _atomic_private_replace(
        root / "answer-display-lifecycles" / f"{digest}.json", raw, root=root
    )


def _validate_trace_supplement_events(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not 1 <= len(value) <= 24:
        raise CurrentQuestionEvidenceError("trace supplement events are invalid")
    rows: list[dict[str, Any]] = []
    for event in value:
        row = _exact_mapping(
            event, TRACE_SUPPLEMENT_EVENT_KEYS, "trace supplement event"
        )
        if (
            row.get("role") not in {"learner", "assistant"}
            or row.get("kind") not in {
                "utterance",
                "first_action",
                "reasoning",
                "hint",
                "correction",
                "restatement",
                "answer",
            }
            or row.get("observed_at") is not None
        ):
            raise CurrentQuestionEvidenceError("trace supplement event is invalid")
        _bounded_string(
            row.get("text"),
            2048,
            "trace supplement event text",
            nullable=False,
            allow_empty=False,
        )
        rows.append(dict(row))
    if len(_json_bytes(rows)) > 32 * 1024:
        raise CurrentQuestionEvidenceError("trace supplement events exceed size limit")
    return rows


def publish_trace_supplement(
    *,
    private_root: str | Path | None,
    capture_id: str,
    context_id: str,
    item_id: str,
    evidence_manifest_sha256: str,
    created_at: str,
    supplement_kind: str,
    resolution_receipt_sha256: str,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bind one resolved private trace to a capture without rewriting it."""

    if supplement_kind != "resolved_trace":
        raise CurrentQuestionEvidenceError("trace supplement kind is invalid")
    for label, value in {
        "capture_id": capture_id,
        "context_id": context_id,
        "item_id": item_id,
    }.items():
        _bounded_string(value, 256, label, nullable=False, allow_empty=False)
    for label, value in {
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "resolution_receipt_sha256": resolution_receipt_sha256,
    }.items():
        if not SHA256_RE.fullmatch(str(value or "")):
            raise CurrentQuestionEvidenceError(f"trace supplement {label} is invalid")
    _parse_time(created_at, "trace supplement created_at")
    normalized_events = _validate_trace_supplement_events(events)
    interaction_trace_sha256 = _sha256(_json_bytes(normalized_events))
    supplement = {
        "schema_version": TRACE_SUPPLEMENT_SCHEMA,
        "capture_id": capture_id,
        "context_id": context_id,
        "item_id": item_id,
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "created_at": created_at,
        "supplement_kind": supplement_kind,
        "resolution_receipt_sha256": resolution_receipt_sha256,
        "events": normalized_events,
        "interaction_trace_sha256": interaction_trace_sha256,
        "formal_write_count": 0,
    }
    raw = _json_bytes(supplement, pretty=True)
    if not 0 < len(raw) <= MAX_METADATA_BYTES:
        raise CurrentQuestionEvidenceError("trace supplement exceeds size limit")
    root = _private_root(private_root)
    object_sha = _sha256(raw)
    _atomic_private_write(
        root / "trace-supplements" / "objects" / f"{object_sha}.json",
        raw,
        root=root,
    )
    locator = TRACE_SUPPLEMENT_LOCATOR_PREFIX + object_sha
    binding = {
        "schema_version": TRACE_SUPPLEMENT_BINDING_SCHEMA,
        "capture_id": capture_id,
        "context_id": context_id,
        "item_id": item_id,
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "locator": locator,
        "object_sha": object_sha,
        "created_at": created_at,
        "supplement_kind": supplement_kind,
        "resolution_receipt_sha256": resolution_receipt_sha256,
        "formal_write_count": 0,
    }
    binding_raw = _json_bytes(binding, pretty=True)
    binding_key = _sha256(capture_id.encode("utf-8"))
    binding_path = (
        root / "trace-supplements" / "bindings" / f"{binding_key}.json"
    )
    if binding_path.exists():
        existing = binding_path.read_bytes()
        if existing != binding_raw:
            existing_binding, _ = read_trace_supplement_for_capture(
                capture_id, private_root=private_root
            )
            legacy_upgrade = (
                existing_binding.get("supplement_kind") == "legacy_backfill"
                and existing_binding.get("resolution_receipt_sha256") is None
                and existing_binding.get("capture_id") == capture_id
                and existing_binding.get("context_id") == context_id
                and existing_binding.get("item_id") == item_id
                and existing_binding.get("evidence_manifest_sha256")
                == evidence_manifest_sha256
            )
            if not legacy_upgrade:
                raise CurrentQuestionEvidenceError(
                    "capture trace supplement binding already resolved differently"
                )
            _atomic_private_replace(binding_path, binding_raw, root=root)
    else:
        _atomic_private_write(binding_path, binding_raw, root=root)
    return {
        "schema_version": TRACE_SUPPLEMENT_BINDING_SCHEMA,
        "capture_id": capture_id,
        "locator": locator,
        "object_sha": object_sha,
        "binding_sha256": _sha256(binding_raw),
        "interaction_trace_sha256": interaction_trace_sha256,
        "formal_write_count": 0,
    }


def read_trace_supplement_for_capture(
    capture_id: str, *, private_root: str | Path | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    _bounded_string(
        capture_id, 256, "capture_id", nullable=False, allow_empty=False
    )
    root = _private_root(private_root)
    binding_key = _sha256(capture_id.encode("utf-8"))
    binding_path = (
        root / "trace-supplements" / "bindings" / f"{binding_key}.json"
    )
    try:
        info = binding_path.lstat()
        raw = binding_path.read_bytes()
        binding = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CurrentQuestionEvidenceError(
            "capture trace supplement binding is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > MAX_METADATA_BYTES
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise CurrentQuestionEvidenceError(
            "capture trace supplement binding is unsafe"
        )
    row = _exact_mapping(
        binding, TRACE_SUPPLEMENT_BINDING_KEYS, "trace supplement binding"
    )
    if (
        row.get("schema_version") != TRACE_SUPPLEMENT_BINDING_SCHEMA
        or row.get("capture_id") != capture_id
        or row.get("formal_write_count") != 0
        or not SHA256_RE.fullmatch(str(row.get("object_sha") or ""))
        or row.get("locator")
        != TRACE_SUPPLEMENT_LOCATOR_PREFIX + str(row.get("object_sha") or "")
    ):
        raise CurrentQuestionEvidenceError("trace supplement binding is invalid")
    object_sha = str(row["object_sha"])
    supplement = _read_object(
        root / "trace-supplements" / "objects" / f"{object_sha}.json",
        object_sha,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    obj = _exact_mapping(
        supplement, TRACE_SUPPLEMENT_KEYS, "trace supplement"
    )
    if (
        obj.get("schema_version") != TRACE_SUPPLEMENT_SCHEMA
        or obj.get("capture_id") != capture_id
        or obj.get("context_id") != row.get("context_id")
        or obj.get("item_id") != row.get("item_id")
        or obj.get("evidence_manifest_sha256")
        != row.get("evidence_manifest_sha256")
        or obj.get("resolution_receipt_sha256")
        != row.get("resolution_receipt_sha256")
        or obj.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("trace supplement object drifted")
    _validate_trace_supplement_events(obj.get("events"))
    if obj.get("interaction_trace_sha256") != _sha256(
        _json_bytes(obj["events"])
    ):
        raise CurrentQuestionEvidenceError("trace supplement trace hash drifted")
    return row, obj


def _validate_background_handoff_object(value: object) -> dict[str, Any]:
    row = _exact_mapping(
        value, BACKGROUND_HANDOFF_KEYS, "current-question background handoff"
    )
    if (
        row.get("schema_version") != BACKGROUND_HANDOFF_SCHEMA
        or row.get("status") not in {"teaching_pending", "evidence_pending", "ready"}
        or row.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("background handoff metadata is invalid")
    for label in ("capture_id", "context_id", "item_id"):
        _bounded_string(
            row.get(label), 256, label, nullable=False, allow_empty=False
        )
    if not SHA256_RE.fullmatch(
        str(row.get("evidence_manifest_sha256") or "")
    ):
        raise CurrentQuestionEvidenceError(
            "background handoff evidence manifest hash is invalid"
        )
    _parse_time(row.get("created_at"), "background handoff created_at")
    _parse_time(row.get("updated_at"), "background handoff updated_at")
    status = row["status"]
    completion_kind = row.get("completion_kind")
    capture_receipt = row.get("capture_receipt_sha256")
    supplement_locator = row.get("trace_supplement_locator")
    supplement_sha = row.get("trace_supplement_object_sha256")
    interaction_sha = row.get("interaction_trace_sha256")
    resolution_sha = row.get("resolution_receipt_sha256")
    if status in {"teaching_pending", "evidence_pending"}:
        if any(
            value is not None
            for value in (
                completion_kind,
                capture_receipt,
                supplement_locator,
                supplement_sha,
                interaction_sha,
                resolution_sha,
            )
        ):
            raise CurrentQuestionEvidenceError(
                "pending background handoff contains ready-only fields"
            )
        return row
    if (
        completion_kind not in {"first_turn_complete", "teaching_resolved"}
        or not SHA256_RE.fullmatch(str(capture_receipt or ""))
        or not SHA256_RE.fullmatch(str(resolution_sha or ""))
    ):
        raise CurrentQuestionEvidenceError("ready background handoff is incomplete")
    if interaction_sha is not None and not SHA256_RE.fullmatch(
        str(interaction_sha)
    ):
        raise CurrentQuestionEvidenceError(
            "ready background handoff interaction trace hash is invalid"
        )
    if completion_kind == "first_turn_complete":
        if supplement_locator is not None or supplement_sha is not None:
            raise CurrentQuestionEvidenceError(
                "first-turn background handoff cannot bind a trace supplement"
            )
    else:
        if (
            not SHA256_RE.fullmatch(str(supplement_sha or ""))
            or supplement_locator
            != TRACE_SUPPLEMENT_LOCATOR_PREFIX + str(supplement_sha or "")
            or not SHA256_RE.fullmatch(str(interaction_sha or ""))
        ):
            raise CurrentQuestionEvidenceError(
                "teaching-resolved background handoff supplement is invalid"
            )
    return row


def _validate_background_handoff_binding(
    value: object, capture_id: str
) -> dict[str, Any]:
    row = _exact_mapping(
        value,
        BACKGROUND_HANDOFF_BINDING_KEYS,
        "current-question background handoff binding",
    )
    if (
        row.get("schema_version") != BACKGROUND_HANDOFF_BINDING_SCHEMA
        or row.get("capture_id") != capture_id
        or row.get("status") not in {"teaching_pending", "evidence_pending", "ready"}
        or row.get("formal_write_count") != 0
        or not SHA256_RE.fullmatch(str(row.get("object_sha256") or ""))
        or row.get("locator")
        != BACKGROUND_HANDOFF_LOCATOR_PREFIX
        + str(row.get("object_sha256") or "")
    ):
        raise CurrentQuestionEvidenceError("background handoff binding is invalid")
    _parse_time(row.get("updated_at"), "background handoff binding updated_at")
    return row


@contextmanager
def _background_handoff_lock(
    capture_id: str, *, private_root: str | Path | None = None
) -> Iterator[None]:
    _bounded_string(
        capture_id, 256, "capture_id", nullable=False, allow_empty=False
    )
    root = _private_root(private_root)
    directory = root / "background-handoffs" / "locks"
    _ensure_private_directory(directory, boundary=root)
    key = _sha256(capture_id.encode("utf-8"))
    descriptor = os.open(directory / f"{key}.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _write_background_handoff(
    handoff: dict[str, Any], *, private_root: str | Path | None
) -> dict[str, Any]:
    row = _validate_background_handoff_object(handoff)
    root = _private_root(private_root)
    raw = _json_bytes(row, pretty=True)
    if not 0 < len(raw) <= MAX_METADATA_BYTES:
        raise CurrentQuestionEvidenceError("background handoff exceeds size limit")
    object_sha = _sha256(raw)
    _atomic_private_write(
        root / "background-handoffs" / "objects" / f"{object_sha}.json",
        raw,
        root=root,
    )
    locator = BACKGROUND_HANDOFF_LOCATOR_PREFIX + object_sha
    binding = {
        "schema_version": BACKGROUND_HANDOFF_BINDING_SCHEMA,
        "capture_id": row["capture_id"],
        "status": row["status"],
        "object_sha256": object_sha,
        "locator": locator,
        "updated_at": row["updated_at"],
        "formal_write_count": 0,
    }
    binding = _validate_background_handoff_binding(
        binding, str(row["capture_id"])
    )
    binding_raw = _json_bytes(binding, pretty=True)
    binding_key = _sha256(str(row["capture_id"]).encode("utf-8"))
    _atomic_private_replace(
        root / "background-handoffs" / "bindings" / f"{binding_key}.json",
        binding_raw,
        root=root,
    )
    return {
        **binding,
        "binding_sha256": _sha256(binding_raw),
        "completion_kind": row["completion_kind"],
        "trace_supplement_locator": row["trace_supplement_locator"],
        "trace_supplement_object_sha256": row[
            "trace_supplement_object_sha256"
        ],
        "interaction_trace_sha256": row["interaction_trace_sha256"],
        "resolution_receipt_sha256": row["resolution_receipt_sha256"],
    }


def read_background_handoff_for_capture(
    capture_id: str, *, private_root: str | Path | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve the current immutable background-handoff object for a capture."""

    _bounded_string(
        capture_id, 256, "capture_id", nullable=False, allow_empty=False
    )
    root = _private_root(private_root)
    binding_key = _sha256(capture_id.encode("utf-8"))
    binding_path = (
        root / "background-handoffs" / "bindings" / f"{binding_key}.json"
    )
    try:
        info = binding_path.lstat()
        raw = binding_path.read_bytes()
        binding_value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CurrentQuestionEvidenceError(
            "capture background handoff binding is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_size <= 0
        or info.st_size > MAX_METADATA_BYTES
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise CurrentQuestionEvidenceError(
            "capture background handoff binding is unsafe"
        )
    binding = _validate_background_handoff_binding(binding_value, capture_id)
    object_sha = str(binding["object_sha256"])
    value = _read_object(
        root / "background-handoffs" / "objects" / f"{object_sha}.json",
        object_sha,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    handoff = _validate_background_handoff_object(value)
    if (
        handoff.get("capture_id") != capture_id
        or handoff.get("status") != binding.get("status")
        or handoff.get("updated_at") != binding.get("updated_at")
    ):
        raise CurrentQuestionEvidenceError("background handoff object drifted")
    return binding, handoff


def publish_background_handoff_pending(
    *,
    private_root: str | Path | None,
    capture_id: str,
    context_id: str,
    item_id: str,
    evidence_manifest_sha256: str,
    created_at: str,
    status: str = "teaching_pending",
) -> dict[str, Any]:
    """Fail closed before capture commit by publishing a teaching gate."""

    pending = {
        "schema_version": BACKGROUND_HANDOFF_SCHEMA,
        "capture_id": capture_id,
        "context_id": context_id,
        "item_id": item_id,
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "capture_receipt_sha256": None,
        "status": status,
        "completion_kind": None,
        "created_at": created_at,
        "updated_at": created_at,
        "trace_supplement_locator": None,
        "trace_supplement_object_sha256": None,
        "interaction_trace_sha256": None,
        "resolution_receipt_sha256": None,
        "formal_write_count": 0,
    }
    _validate_background_handoff_object(pending)
    with _background_handoff_lock(capture_id, private_root=private_root):
        try:
            _, existing = read_background_handoff_for_capture(
                capture_id, private_root=private_root
            )
        except CurrentQuestionEvidenceError as exc:
            if "unavailable" not in str(exc):
                raise
            existing = None
        if existing is not None:
            for field in (
                "capture_id",
                "context_id",
                "item_id",
                "evidence_manifest_sha256",
                "created_at",
            ):
                if existing.get(field) != pending.get(field):
                    raise CurrentQuestionEvidenceError(
                        "background handoff identity drifted"
                    )
            if existing.get("status") == "ready":
                return _write_background_handoff(
                    existing, private_root=private_root
                )
        return _write_background_handoff(pending, private_root=private_root)


def publish_background_handoff_ready(
    *,
    private_root: str | Path | None,
    capture_id: str,
    context_id: str,
    item_id: str,
    evidence_manifest_sha256: str,
    capture_receipt_sha256: str,
    updated_at: str,
    completion_kind: str,
    resolution_receipt_sha256: str,
    interaction_trace_sha256: str | None,
    trace_supplement_locator: str | None = None,
    trace_supplement_object_sha256: str | None = None,
) -> dict[str, Any]:
    """Atomically point the gate at one immutable, fully bound ready object."""

    with _background_handoff_lock(capture_id, private_root=private_root):
        _, existing = read_background_handoff_for_capture(
            capture_id, private_root=private_root
        )
        for field, expected in {
            "capture_id": capture_id,
            "context_id": context_id,
            "item_id": item_id,
            "evidence_manifest_sha256": evidence_manifest_sha256,
        }.items():
            if existing.get(field) != expected:
                raise CurrentQuestionEvidenceError(
                    "background handoff ready identity drifted"
                )
        if existing.get("status") == "evidence_pending":
            raise CurrentQuestionEvidenceError(
                "background handoff evidence is pending"
            )
        ready = {
            "schema_version": BACKGROUND_HANDOFF_SCHEMA,
            "capture_id": capture_id,
            "context_id": context_id,
            "item_id": item_id,
            "evidence_manifest_sha256": evidence_manifest_sha256,
            "capture_receipt_sha256": capture_receipt_sha256,
            "status": "ready",
            "completion_kind": completion_kind,
            "created_at": existing["created_at"],
            "updated_at": updated_at,
            "trace_supplement_locator": trace_supplement_locator,
            "trace_supplement_object_sha256": (
                trace_supplement_object_sha256
            ),
            "interaction_trace_sha256": interaction_trace_sha256,
            "resolution_receipt_sha256": resolution_receipt_sha256,
            "formal_write_count": 0,
        }
        _validate_background_handoff_object(ready)
        if existing.get("status") == "ready" and existing != ready:
            legacy_teaching_upgrade = (
                existing.get("completion_kind") == "first_turn_complete"
                and completion_kind == "teaching_resolved"
                and existing.get("capture_receipt_sha256")
                == capture_receipt_sha256
                and existing.get("trace_supplement_locator") is None
                and existing.get("trace_supplement_object_sha256") is None
                and trace_supplement_locator is not None
                and trace_supplement_object_sha256 is not None
                and interaction_trace_sha256 is not None
            )
            if not legacy_teaching_upgrade:
                raise CurrentQuestionEvidenceError(
                    "background handoff already became ready differently"
                )
        return _write_background_handoff(ready, private_root=private_root)


def publish_metadata_object(
    value: dict[str, Any],
    *,
    kind: str,
    private_root: str | Path | None = None,
) -> dict[str, str]:
    if kind not in {"recoveries", "turns"}:
        raise CurrentQuestionEvidenceError("private metadata kind is invalid")
    schema = RECOVERY_SCHEMA if kind == "recoveries" else TURN_RECEIPT_SCHEMA
    if not isinstance(value, dict) or value.get("schema") != schema:
        raise CurrentQuestionEvidenceError("private metadata schema is invalid")
    raw = _json_bytes(value, pretty=True)
    if not 0 < len(raw) <= MAX_METADATA_BYTES:
        raise CurrentQuestionEvidenceError("private metadata exceeds size limit")
    root = _private_root(private_root)
    digest = _sha256(raw)
    _atomic_private_write(root / kind / f"{digest}.json", raw, root=root)
    prefix = RECOVERY_LOCATOR_PREFIX if kind == "recoveries" else TURN_LOCATOR_PREFIX
    return {"locator": prefix + digest, "sha256": digest}


def publish_evaluation_capsule(
    value: dict[str, Any], *, private_root: str | Path | None = None
) -> dict[str, str]:
    """Persist a grader capsule separately from every public turn request."""

    if not isinstance(value, dict) or set(value) != EVALUATION_CAPSULE_KEYS:
        raise CurrentQuestionEvidenceError("evaluation capsule fields are invalid")
    if (
        value.get("schema_version") != EVALUATION_CAPSULE_SCHEMA
        or value.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("evaluation capsule schema is invalid")
    evaluation = _exact_mapping(
        value.get("evaluation_evidence"),
        EVALUATION_EVIDENCE_KEYS,
        "evaluation capsule evidence",
    )
    if evaluation.get("grader_capsule_id") != value.get("capsule_id"):
        raise CurrentQuestionEvidenceError("evaluation capsule identity drifted")
    source_stable = value.get("source_stable")
    if not isinstance(source_stable, bool):
        raise CurrentQuestionEvidenceError("evaluation capsule source stability is invalid")
    if source_stable:
        if not SHA256_RE.fullmatch(str(value.get("source_binding_sha256") or "")):
            raise CurrentQuestionEvidenceError("evaluation capsule source binding is invalid")
    elif value.get("source_binding_sha256") is not None:
        raise CurrentQuestionEvidenceError("unstable evaluation capsule has source binding")
    raw = _json_bytes(value, pretty=True)
    if not 0 < len(raw) <= MAX_METADATA_BYTES:
        raise CurrentQuestionEvidenceError("evaluation capsule exceeds size limit")
    root = _private_root(private_root)
    digest = _sha256(raw)
    _atomic_private_write(
        root / "evaluation-capsules" / f"{digest}.json", raw, root=root
    )
    return {"locator": EVALUATION_LOCATOR_PREFIX + digest, "sha256": digest}


def read_evaluation_capsule(
    locator: str,
    *,
    expected_sha256: str,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    if not isinstance(locator, str) or not locator.startswith(EVALUATION_LOCATOR_PREFIX):
        raise CurrentQuestionEvidenceError("evaluation capsule locator is invalid")
    digest = locator[len(EVALUATION_LOCATOR_PREFIX) :]
    if digest != expected_sha256:
        raise CurrentQuestionEvidenceError("evaluation capsule locator/hash drifted")
    root = _private_root(private_root)
    value = _read_object(
        root / "evaluation-capsules" / f"{digest}.json",
        digest,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    if (
        set(value) != EVALUATION_CAPSULE_KEYS
        or value.get("schema_version") != EVALUATION_CAPSULE_SCHEMA
        or value.get("formal_write_count") != 0
    ):
        raise CurrentQuestionEvidenceError("evaluation capsule content is invalid")
    evaluation = _exact_mapping(
        value.get("evaluation_evidence"),
        EVALUATION_EVIDENCE_KEYS,
        "evaluation capsule evidence",
    )
    if evaluation.get("grader_capsule_id") != value.get("capsule_id"):
        raise CurrentQuestionEvidenceError("evaluation capsule identity drifted")
    source_stable = value.get("source_stable")
    if not isinstance(source_stable, bool):
        raise CurrentQuestionEvidenceError("evaluation capsule source stability is invalid")
    if source_stable:
        if not SHA256_RE.fullmatch(str(value.get("source_binding_sha256") or "")):
            raise CurrentQuestionEvidenceError("evaluation capsule source binding is invalid")
    elif value.get("source_binding_sha256") is not None:
        raise CurrentQuestionEvidenceError("unstable evaluation capsule has source binding")
    return value


def read_metadata_object(
    locator: str,
    *,
    kind: str,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    if kind not in {"recoveries", "turns"}:
        raise CurrentQuestionEvidenceError("private metadata kind is invalid")
    prefix = RECOVERY_LOCATOR_PREFIX if kind == "recoveries" else TURN_LOCATOR_PREFIX
    if not isinstance(locator, str) or not locator.startswith(prefix):
        raise CurrentQuestionEvidenceError("private metadata locator is invalid")
    digest = locator[len(prefix) :]
    root = _private_root(private_root)
    value = _read_object(
        root / kind / f"{digest}.json",
        digest,
        root=root,
        maximum=MAX_METADATA_BYTES,
    )
    expected = RECOVERY_SCHEMA if kind == "recoveries" else TURN_RECEIPT_SCHEMA
    if value.get("schema") != expected:
        raise CurrentQuestionEvidenceError("private metadata object schema drifted")
    return value


__all__ = [
    "BUNDLE_SCHEMA",
    "BUNDLE_SCHEMA_V2",
    "BUNDLE_SCHEMA_V3",
    "ATTACHMENT_LOCATOR_PREFIX",
    "DEFAULT_PRIVATE_ROOT",
    "EVALUATION_CAPSULE_SCHEMA",
    "EVALUATION_LOCATOR_PREFIX",
    "STUDY_OBSERVATION_SCHEMA",
    "STUDY_OBSERVATION_LOCATOR_PREFIX",
    "ANSWER_OPERATION_SCHEMA",
    "ANSWER_DISPLAY_LIFECYCLE_SCHEMA",
    "TRACE_SUPPLEMENT_SCHEMA",
    "TRACE_SUPPLEMENT_BINDING_SCHEMA",
    "BACKGROUND_HANDOFF_SCHEMA",
    "BACKGROUND_HANDOFF_BINDING_SCHEMA",
    "BACKGROUND_HANDOFF_LOCATOR_PREFIX",
    "LOCATOR_PREFIX",
    "MANIFEST_SCHEMA",
    "RECOVERY_SCHEMA",
    "TURN_RECEIPT_SCHEMA",
    "CurrentQuestionEvidenceError",
    "publish_bundle",
    "publish_evaluation_capsule",
    "publish_metadata_object",
    "publish_study_observation",
    "read_bundle",
    "read_evaluation_capsule",
    "read_metadata_object",
    "read_study_observation",
    "answer_operation_lock",
    "read_answer_operation",
    "write_answer_operation",
    "answer_display_lock",
    "read_answer_display_lifecycle",
    "write_answer_display_lifecycle",
    "publish_trace_supplement",
    "read_trace_supplement_for_capture",
    "publish_background_handoff_pending",
    "publish_background_handoff_ready",
    "read_background_handoff_for_capture",
    "stage_attachments",
    "bundle_evidence_status",
    "validate_image_bytes",
    "ImageIntegrityError",
]
