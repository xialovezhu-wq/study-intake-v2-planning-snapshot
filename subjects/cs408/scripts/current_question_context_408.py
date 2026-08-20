#!/usr/bin/env python3
"""Validate one self-contained 408 current-question turn.

This module deliberately has no repository, personalization, history, Luna, or
formal-graph dependency.  Raw question and learner material remains an
in-memory/private-evidence concern; only its normalized answer-safe projection
is handed to the learning and capture writers.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import re
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA = "current-question-context-v1"
TIMEZONE = "Asia/Shanghai"
SOURCES = {
    "morning_review",
    "daily_practice",
    "evening_d0",
    "ordinary_question",
}
CONFIDENCE = {"low", "medium", "high"}
CHOICE_RESULTS = {
    "correct",
    "incorrect",
    "partial",
    "blank",
    "uncertain",
    "not_applicable",
}
REASONING_RESULTS = {"sound", "partial", "diverged", "not_observed"}
PROVENANCE = {
    "user_report",
    "visible_evidence",
    "derived_classification",
    "not_observed",
}
PROMPT_LEVELS = {"none", "L1", "L2", "L3", "L4", "L5"}
DIRECT_PROVENANCE = {"user_report", "visible_evidence"}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+-]{1,255}$")
FORMAL_ID_RE = re.compile(r"^(?:DS|CO|OS|CN)_(?:\d{4}|UNK)_\d{3}$")
SUBJECTS = {"DS", "CO", "OS", "CN", "未确认"}
MAX_TEXT_BYTES = 256 * 1024
INTERACTION_TRACE_SCHEMA = "current-question-interaction-trace-v1"
INTERACTION_TRACE_SCHEMA_V2 = "current-question-interaction-trace-v2"
MAX_TRACE_EVENTS = 24
MAX_TRACE_EVENT_BYTES = 2 * 1024
MAX_TRACE_BYTES = 32 * 1024
TRACE_ROLES = {"learner", "assistant"}
TRACE_KINDS = {
    "utterance",
    "first_action",
    "reasoning",
    "hint",
    "correction",
    "restatement",
    "answer",
}
QUESTION_EVIDENCE_KEYS = {
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
ALLOWED_KEYS = {
    "schema",
    "source",
    "request_id",
    "session_id",
    "item_id",
    "source_id",
    "source_stable",
    "source_binding_sha256",
    "grader_capsule_id",
    "event_time",
    "study_date",
    "timezone",
    "idempotency_key",
    "display_receipt_sha256",
    "question_valid",
    "question_evidence",
    "learner_evidence",
    "assessment",
    "formal_write_count",
}
PRIVATE_CONTEXT_KEYS = {
    "answer",
    "correct_answer",
    "correct_option",
    "standard_answer",
    "standard_solution",
    "solution",
    "grader",
    "grading",
    "grading_capsule",
    "private_evaluator",
    "evaluator",
    "decisive_reason",
    "feedback_by_result",
    "feedback_by_choice",
    "answer_key",
}


class CurrentQuestionContextError(ValueError):
    """The current-question input is incomplete, unsafe, or inconsistent."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _required_id(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not SAFE_ID_RE.fullmatch(text):
        raise CurrentQuestionContextError(f"{label}_invalid")
    return text


def _bounded_text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise CurrentQuestionContextError(f"{label}_must_be_text")
    text = value.strip()
    if not allow_empty and not text:
        raise CurrentQuestionContextError(f"{label}_required")
    if len(value.encode("utf-8")) > MAX_TEXT_BYTES:
        raise CurrentQuestionContextError(f"{label}_too_large")
    return text


def _event_date(event_time: str) -> str:
    try:
        parsed = dt.datetime.fromisoformat(event_time)
    except ValueError as exc:
        raise CurrentQuestionContextError("event_time_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CurrentQuestionContextError("event_time_must_be_timezone_aware")
    return parsed.astimezone(ZoneInfo(TIMEZONE)).date().isoformat()


def _normalize_evidence(raw: object, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise CurrentQuestionContextError(f"{label}_must_be_object")
    encoded = _canonical(raw)
    if not raw or len(encoded) > MAX_TEXT_BYTES:
        raise CurrentQuestionContextError(f"{label}_invalid_size")
    def contains_private(value: Any) -> bool:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = str(key).strip().lower().replace("-", "_")
                if normalized_key in PRIVATE_CONTEXT_KEYS or contains_private(child):
                    return True
        elif isinstance(value, (list, tuple)):
            return any(contains_private(child) for child in value)
        return False

    if contains_private(raw):
        raise CurrentQuestionContextError(f"{label}_contains_private_grading")
    return copy.deepcopy(raw)


def _limited_optional_text(value: object, maximum: int, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value.encode("utf-8")) > maximum:
        raise CurrentQuestionContextError(f"{label}_invalid")
    return value


def _normalize_question_evidence(raw: object) -> dict[str, Any]:
    value = _normalize_evidence(raw, "question_evidence")
    if set(value) != QUESTION_EVIDENCE_KEYS:
        raise CurrentQuestionContextError("question_evidence_fields_invalid")
    _limited_optional_text(value["public_text"], 20000, "public_text")
    _limited_optional_text(
        value["response_instruction"], 4000, "response_instruction"
    )
    options = value["options"]
    if not isinstance(options, list) or len(options) > 12:
        raise CurrentQuestionContextError("question_options_invalid")
    for option in options:
        if isinstance(option, str):
            _limited_optional_text(option, 4000, "question_option")
        elif isinstance(option, dict) and set(option) == {"label", "text"}:
            _limited_optional_text(option["label"], 4000, "question_option_label")
            _limited_optional_text(option["text"], 4000, "question_option_text")
        else:
            raise CurrentQuestionContextError("question_option_invalid")
    surface_sha = value["public_surface_sha256"]
    if surface_sha is not None and not SHA256_RE.fullmatch(str(surface_sha)):
        raise CurrentQuestionContextError("public_surface_sha256_invalid")
    attachment_hashes = value["attachment_sha256s"]
    if (
        not isinstance(attachment_hashes, list)
        or len(attachment_hashes) > 8
        or any(not SHA256_RE.fullmatch(str(item or "")) for item in attachment_hashes)
    ):
        raise CurrentQuestionContextError("question_attachment_hashes_invalid")
    return value


def _normalize_learner_evidence(raw: object) -> dict[str, Any]:
    value = _normalize_evidence(raw, "learner_evidence")
    if set(value) != LEARNER_EVIDENCE_KEYS:
        raise CurrentQuestionContextError("learner_evidence_fields_invalid")
    for key, maximum in {
        "answer_text": 12000,
        "choice": 80,
        "confidence": 80,
        "first_action": 4000,
        "reasoning": 16000,
        "prompt_level": 80,
        "observed_at": 80,
    }.items():
        _limited_optional_text(value[key], maximum, f"learner_{key}")
    return value


def normalize_interaction_trace(raw: object | None) -> dict[str, Any]:
    """Validate one bounded, private-only trace of the current question."""

    if raw is None:
        events_raw: object = []
        legacy_truncated = False
    elif isinstance(raw, list):
        events_raw = raw
        legacy_truncated = False
    elif isinstance(raw, dict) and set(raw) == {"events", "truncated"}:
        events_raw = raw["events"]
        legacy_truncated = raw["truncated"]
    elif isinstance(raw, dict) and set(raw) == {
        "schema",
        "events",
        "event_count",
        "truncated",
    }:
        if (
            raw.get("schema") != INTERACTION_TRACE_SCHEMA
            or raw.get("event_count") != len(raw.get("events") or [])
        ):
            raise CurrentQuestionContextError("interaction_trace_metadata_invalid")
        normalized_events: list[dict[str, Any]] = []
        for ordinal, event in enumerate(raw.get("events") or [], start=1):
            if (
                not isinstance(event, dict)
                or set(event) != {"ordinal", "role", "kind", "text"}
                or event.get("ordinal") != ordinal
            ):
                raise CurrentQuestionContextError(
                    "interaction_trace_event_shape_invalid"
                )
            normalized_events.append(
                {
                    "role": event["role"],
                    "kind": event["kind"],
                    "text": event["text"],
                }
            )
        events_raw = normalized_events
        legacy_truncated = raw["truncated"]
    elif isinstance(raw, dict) and set(raw) == {
        "schema",
        "events",
        "original_event_count",
        "included_event_count",
        "omitted_event_count",
        "omitted_ranges",
        "truncation_reason",
        "full_trace_sha256",
    }:
        if raw.get("schema") != INTERACTION_TRACE_SCHEMA_V2:
            raise CurrentQuestionContextError("interaction_trace_metadata_invalid")
        events = raw.get("events")
        original_count = raw.get("original_event_count")
        included_count = raw.get("included_event_count")
        omitted_count = raw.get("omitted_event_count")
        omitted_ranges = raw.get("omitted_ranges")
        reason = raw.get("truncation_reason")
        full_digest = raw.get("full_trace_sha256")
        if (
            not isinstance(events, list)
            or not isinstance(original_count, int)
            or isinstance(original_count, bool)
            or not isinstance(included_count, int)
            or isinstance(included_count, bool)
            or not isinstance(omitted_count, int)
            or isinstance(omitted_count, bool)
            or original_count < 0
            or included_count != len(events)
            or included_count > MAX_TRACE_EVENTS
            or omitted_count != original_count - included_count
            or not SHA256_RE.fullmatch(str(full_digest or ""))
            or not isinstance(omitted_ranges, list)
            or (omitted_count == 0 and (omitted_ranges or reason is not None))
            or (omitted_count > 0 and (not omitted_ranges or not isinstance(reason, str) or not reason))
        ):
            raise CurrentQuestionContextError("interaction_trace_metadata_invalid")
        normalized_events: list[dict[str, Any]] = []
        last_ordinal = 0
        for event in events:
            if (
                not isinstance(event, dict)
                or set(event) != {"ordinal", "role", "kind", "text"}
                or not isinstance(event.get("ordinal"), int)
                or isinstance(event.get("ordinal"), bool)
                or not last_ordinal < event["ordinal"] <= original_count
            ):
                raise CurrentQuestionContextError("interaction_trace_event_shape_invalid")
            last_ordinal = event["ordinal"]
            role = str(event.get("role") or "")
            kind = str(event.get("kind") or "")
            text = _bounded_text(event.get("text"), "interaction_trace_event_text")
            if role not in TRACE_ROLES or kind not in TRACE_KINDS:
                raise CurrentQuestionContextError("interaction_trace_event_shape_invalid")
            if len(text.encode("utf-8")) > MAX_TRACE_EVENT_BYTES:
                raise CurrentQuestionContextError("interaction_trace_event_too_large")
            normalized_events.append(
                {"ordinal": event["ordinal"], "role": role, "kind": kind, "text": text}
            )
        omitted_total = 0
        omitted_ordinals: set[int] = set()
        previous_end = 0
        for item in omitted_ranges:
            if (
                not isinstance(item, dict)
                or set(item) != {"start_ordinal", "end_ordinal"}
                or not isinstance(item.get("start_ordinal"), int)
                or isinstance(item.get("start_ordinal"), bool)
                or not isinstance(item.get("end_ordinal"), int)
                or isinstance(item.get("end_ordinal"), bool)
                or not previous_end < item["start_ordinal"] <= item["end_ordinal"] <= original_count
            ):
                raise CurrentQuestionContextError("interaction_trace_omitted_ranges_invalid")
            previous_end = item["end_ordinal"]
            omitted_total += item["end_ordinal"] - item["start_ordinal"] + 1
            omitted_ordinals.update(
                range(item["start_ordinal"], item["end_ordinal"] + 1)
            )
        included_ordinals = {event["ordinal"] for event in normalized_events}
        if (
            omitted_total != omitted_count
            or included_ordinals & omitted_ordinals
            or included_ordinals | omitted_ordinals
            != set(range(1, original_count + 1))
        ):
            raise CurrentQuestionContextError("interaction_trace_omitted_ranges_invalid")
        if omitted_count == 0 and str(full_digest) != hashlib.sha256(
            _canonical(normalized_events)
        ).hexdigest():
            raise CurrentQuestionContextError("interaction_trace_full_digest_invalid")
        normalized = {
            "schema": INTERACTION_TRACE_SCHEMA_V2,
            "events": normalized_events,
            "original_event_count": original_count,
            "included_event_count": included_count,
            "omitted_event_count": omitted_count,
            "omitted_ranges": copy.deepcopy(omitted_ranges),
            "truncation_reason": reason,
            "full_trace_sha256": str(full_digest),
        }
        if len(_canonical(normalized)) > MAX_TRACE_BYTES:
            raise CurrentQuestionContextError("interaction_trace_too_large")
        return normalized
    else:
        raise CurrentQuestionContextError("interaction_trace_shape_invalid")
    if not isinstance(legacy_truncated, bool):
        raise CurrentQuestionContextError("interaction_trace_truncated_invalid")
    if not isinstance(events_raw, list):
        raise CurrentQuestionContextError("interaction_trace_event_count_invalid")
    full_events: list[dict[str, Any]] = []
    for ordinal, event in enumerate(events_raw, start=1):
        if not isinstance(event, dict) or set(event) != {"role", "kind", "text"}:
            raise CurrentQuestionContextError("interaction_trace_event_shape_invalid")
        role = str(event.get("role") or "")
        kind = str(event.get("kind") or "")
        text = _bounded_text(event.get("text"), "interaction_trace_event_text")
        if role not in TRACE_ROLES:
            raise CurrentQuestionContextError("interaction_trace_role_invalid")
        if kind not in TRACE_KINDS:
            raise CurrentQuestionContextError("interaction_trace_kind_invalid")
        if len(text.encode("utf-8")) > MAX_TRACE_EVENT_BYTES:
            raise CurrentQuestionContextError("interaction_trace_event_too_large")
        full_events.append(
            {"ordinal": ordinal, "role": role, "kind": kind, "text": text}
        )
    full_trace_sha256 = hashlib.sha256(_canonical(full_events)).hexdigest()
    original_count = len(full_events)
    if original_count > MAX_TRACE_EVENTS:
        half = MAX_TRACE_EVENTS // 2
        events = full_events[:half] + full_events[-half:]
        omitted_ranges = [
            {
                "start_ordinal": half + 1,
                "end_ordinal": original_count - half,
            }
        ]
        truncation_reason: str | None = "max_event_count_first_last"
    else:
        events = full_events
        omitted_ranges = []
        truncation_reason = None
    omitted_count = original_count - len(events)
    if legacy_truncated and omitted_count == 0:
        raise CurrentQuestionContextError(
            "legacy_truncated_trace_lacks_explicit_omission_metadata"
        )
    normalized = {
        "schema": INTERACTION_TRACE_SCHEMA_V2,
        "events": events,
        "original_event_count": original_count,
        "included_event_count": len(events),
        "omitted_event_count": omitted_count,
        "omitted_ranges": omitted_ranges,
        "truncation_reason": truncation_reason,
        "full_trace_sha256": full_trace_sha256,
    }
    if len(_canonical(normalized)) > MAX_TRACE_BYTES:
        raise CurrentQuestionContextError("interaction_trace_too_large")
    return normalized


def _normalize_assessment(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise CurrentQuestionContextError("assessment_must_be_object")
    allowed = {
        "choice_result",
        "reasoning_result",
        "confidence",
        "prompt_level",
        "first_break",
        "first_break_provenance",
        "first_action",
        "first_action_provenance",
    }
    if set(raw) - allowed:
        raise CurrentQuestionContextError("assessment_unknown_field")
    choice = str(raw.get("choice_result") or "not_applicable")
    reasoning = str(raw.get("reasoning_result") or "not_observed")
    confidence = str(raw.get("confidence") or "low")
    prompt = str(raw.get("prompt_level") or "none")
    provenance = str(raw.get("first_break_provenance") or "not_observed")
    if choice not in CHOICE_RESULTS:
        raise CurrentQuestionContextError("choice_result_invalid")
    if reasoning not in REASONING_RESULTS:
        raise CurrentQuestionContextError("reasoning_result_invalid")
    if confidence not in CONFIDENCE:
        raise CurrentQuestionContextError("confidence_invalid")
    if prompt not in PROMPT_LEVELS:
        raise CurrentQuestionContextError("prompt_level_invalid")
    if provenance not in PROVENANCE:
        raise CurrentQuestionContextError("first_break_provenance_invalid")
    first_break = _bounded_text(
        raw.get("first_break") or "未观察到",
        "first_break",
    )
    if provenance == "not_observed" and first_break not in {
        "未观察到",
        "not_observed",
    }:
        raise CurrentQuestionContextError("unobserved_first_break_has_content")
    first_action = raw.get("first_action")
    first_action_provenance = raw.get("first_action_provenance")
    if first_action is not None:
        first_action = _bounded_text(first_action, "first_action")
        if first_action_provenance not in DIRECT_PROVENANCE:
            raise CurrentQuestionContextError("first_action_requires_direct_provenance")
    elif first_action_provenance is not None:
        raise CurrentQuestionContextError("first_action_provenance_without_action")
    return {
        "choice_result": choice,
        "reasoning_result": reasoning,
        "confidence": confidence,
        "prompt_level": prompt,
        "first_break": first_break,
        "first_break_provenance": provenance,
        "first_action": first_action,
        "first_action_provenance": first_action_provenance,
    }


def _require_capture_evidence_complete(
    current_question: dict[str, Any],
    learner_evidence: dict[str, Any],
    evaluation: dict[str, Any],
    trace: dict[str, Any],
) -> None:
    if not str(current_question.get("public_text") or "").strip():
        raise CurrentQuestionContextError("capture_question_text_required")
    options = current_question.get("options")
    if not isinstance(options, list) or len(options) != 4:
        raise CurrentQuestionContextError("capture_options_A_to_D_required")
    labels: list[str] = []
    texts: list[str] = []
    for option in options:
        if not isinstance(option, dict) or set(option) != {"label", "text"}:
            raise CurrentQuestionContextError("capture_option_shape_invalid")
        label = str(option.get("label") or "").strip().upper()
        text = str(option.get("text") or "").strip()
        labels.append(label)
        texts.append(text)
    if labels != ["A", "B", "C", "D"]:
        raise CurrentQuestionContextError("capture_option_labels_invalid")
    if any(not text for text in texts) or len(set(texts)) != 4:
        raise CurrentQuestionContextError("capture_option_texts_invalid")
    answer = str(learner_evidence.get("answer_text") or "").strip().upper()
    choice = str(learner_evidence.get("choice") or "").strip().upper()
    if answer not in {"A", "B", "C", "D"} or choice != answer:
        raise CurrentQuestionContextError("capture_learner_answer_choice_invalid")
    learner_answers = [
        str(event.get("text") or "").strip().upper()
        for event in trace.get("events") or []
        if event.get("role") == "learner" and event.get("kind") == "answer"
    ]
    if choice not in learner_answers:
        raise CurrentQuestionContextError("capture_learner_answer_event_required")
    if not (
        str(evaluation.get("standard_explanation") or "").strip()
        or str(evaluation.get("grader_basis") or "").strip()
    ):
        raise CurrentQuestionContextError("capture_explanation_required")


def classify(assessment: dict[str, Any]) -> dict[str, Any]:
    """Return teaching result, canonical result, and capture eligibility."""

    choice = assessment["choice_result"]
    reasoning = assessment["reasoning_result"]
    confidence = assessment["confidence"]
    prompt = assessment["prompt_level"]
    has_break = (
        assessment["first_break_provenance"] != "not_observed"
        and assessment["first_break"] not in {"未观察到", "not_observed"}
    )
    if choice in {"blank", "uncertain"}:
        teaching = "uncertain"
    elif choice == "incorrect":
        teaching = "wrong"
    elif choice == "partial":
        teaching = "partial"
    elif reasoning in {"partial", "diverged"}:
        teaching = "partial"
    elif choice == "correct" and (
        confidence != "high" or prompt != "none" or has_break
    ):
        teaching = "fragile_correct"
    elif choice == "correct" and reasoning in {"sound", "not_observed"}:
        teaching = "independent_correct"
    elif reasoning == "sound" and confidence == "high" and prompt == "none" and not has_break:
        teaching = "independent_correct"
    else:
        teaching = "uncertain"
    canonical = (
        "independent_correct" if teaching == "fragile_correct" else teaching
    )
    return {
        "teaching_result": teaching,
        "canonical_first_result": canonical,
        "fragile_override": teaching == "fragile_correct",
        "capture_required": teaching != "independent_correct",
        "capture_reason": (
            "not_eligible"
            if teaching == "independent_correct"
            else "current_question_exposed_nonindependent_evidence"
        ),
    }


def validate_context(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) - ALLOWED_KEYS:
        raise CurrentQuestionContextError("current_question_context_fields_invalid")
    if raw.get("schema") != SCHEMA:
        raise CurrentQuestionContextError("current_question_context_schema_invalid")
    if raw.get("formal_write_count") != 0:
        raise CurrentQuestionContextError("current_question_context_formal_write_invalid")
    source = str(raw.get("source") or "")
    if source not in SOURCES:
        raise CurrentQuestionContextError("current_question_source_invalid")
    event_time = _bounded_text(raw.get("event_time"), "event_time")
    actual_date = _event_date(event_time)
    study_date = _bounded_text(raw.get("study_date"), "study_date")
    if study_date != actual_date:
        raise CurrentQuestionContextError("study_date_event_time_mismatch")
    if raw.get("timezone") != TIMEZONE:
        raise CurrentQuestionContextError("current_question_timezone_invalid")
    display_sha = raw.get("display_receipt_sha256")
    if display_sha is not None and not SHA256_RE.fullmatch(str(display_sha)):
        raise CurrentQuestionContextError("display_receipt_sha256_invalid")
    if source == "morning_review" and display_sha is None:
        raise CurrentQuestionContextError("morning_display_receipt_required")
    source_stable = raw.get("source_stable")
    if not isinstance(source_stable, bool):
        raise CurrentQuestionContextError("source_stable_must_be_boolean")
    source_binding_sha = raw.get("source_binding_sha256")
    if source_stable:
        if not SHA256_RE.fullmatch(str(source_binding_sha or "")):
            raise CurrentQuestionContextError("stable_source_binding_required")
    elif source_binding_sha is not None:
        raise CurrentQuestionContextError("unstable_source_has_binding")
    assessment = _normalize_assessment(raw.get("assessment"))
    classification = classify(assessment)
    question_evidence = _normalize_question_evidence(raw.get("question_evidence"))
    learner_evidence = _normalize_learner_evidence(raw.get("learner_evidence"))
    if learner_evidence["confidence"] != assessment["confidence"]:
        raise CurrentQuestionContextError("learner_confidence_assessment_drifted")
    if learner_evidence["prompt_level"] != assessment["prompt_level"]:
        raise CurrentQuestionContextError("learner_prompt_assessment_drifted")
    if learner_evidence["observed_at"] != event_time:
        raise CurrentQuestionContextError("learner_observed_at_event_time_drifted")
    if learner_evidence["first_action"] != assessment.get("first_action"):
        raise CurrentQuestionContextError("learner_first_action_assessment_drifted")
    if source == "morning_review" and question_evidence["public_surface_sha256"] is None:
        raise CurrentQuestionContextError("morning_public_surface_sha256_required")
    normalized = {
        "schema": SCHEMA,
        "source": source,
        "request_id": _required_id(raw.get("request_id"), "request_id"),
        "session_id": _required_id(raw.get("session_id"), "session_id"),
        "item_id": _required_id(raw.get("item_id"), "item_id"),
        "source_id": _required_id(raw.get("source_id"), "source_id"),
        "source_stable": source_stable,
        "source_binding_sha256": (
            str(source_binding_sha) if source_binding_sha is not None else None
        ),
        "grader_capsule_id": _required_id(
            raw.get("grader_capsule_id"), "grader_capsule_id"
        ),
        "event_time": event_time,
        "study_date": study_date,
        "timezone": TIMEZONE,
        "idempotency_key": _required_id(
            raw.get("idempotency_key"), "idempotency_key"
        ),
        "display_receipt_sha256": display_sha,
        "question_valid": raw.get("question_valid") is True,
        "question_evidence": question_evidence,
        "learner_evidence": learner_evidence,
        "assessment": assessment,
        "classification": classification,
        "formal_write_count": 0,
    }
    if not normalized["question_valid"]:
        raise CurrentQuestionContextError("current_question_is_not_valid")
    context_material = copy.deepcopy(normalized)
    context_material.pop("classification", None)
    normalized["context_id"] = (
        "CQC-" + hashlib.sha256(_canonical(context_material)).hexdigest()[:24].upper()
    )
    return normalized


def private_bundle(
    context: dict[str, Any],
    *,
    feedback_text: str,
    private_evaluation: dict[str, Any],
    interaction_trace: object | None = None,
    question_mode: str | None = None,
) -> dict[str, Any]:
    frozen_feedback = _bounded_text(feedback_text, "feedback_text")
    if not isinstance(private_evaluation, dict) or not private_evaluation:
        raise CurrentQuestionContextError("private_evaluation_required")
    if len(_canonical(private_evaluation)) > MAX_TEXT_BYTES:
        raise CurrentQuestionContextError("private_evaluation_too_large")
    feedback_sha256 = hashlib.sha256(
        frozen_feedback.encode("utf-8")
    ).hexdigest()
    question = context["question_evidence"]
    learner = context["learner_evidence"]
    assessment = context["assessment"]
    evaluation_missing = private_evaluation.get("missing_fields") or []
    if not isinstance(evaluation_missing, list) or not all(
        isinstance(item, str) for item in evaluation_missing
    ):
        raise CurrentQuestionContextError("private_evaluation_missing_fields_invalid")
    context_material = {
        key: value
        for key, value in context.items()
        if key not in {"classification", "context_id"}
    }
    context_sha256 = hashlib.sha256(_canonical(context_material)).hexdigest()
    current_question = {
        "public_text": str(
            question.get("public_text")
            or question.get("visible_stem")
            or question.get("stem")
            or ""
        ).strip(),
        "options": copy.deepcopy(
            question.get("options") or question.get("visible_options") or []
        ),
        "response_instruction": str(
            question.get("response_instruction") or ""
        ).strip(),
        "public_surface_sha256": (
            str(question.get("public_surface_sha256") or "").strip()
            or str(question.get("surface_sha256") or "").strip()
            or context["source_binding_sha256"]
        ),
        "attachment_sha256s": copy.deepcopy(
            question.get("attachment_sha256s") or []
        ),
    }
    evaluation = {
        "grader_capsule_id": context["grader_capsule_id"],
        "correct_answer": private_evaluation.get("correct_answer"),
        "correct_option": private_evaluation.get("correct_option"),
        "standard_explanation": private_evaluation.get("standard_explanation"),
        "grader_result": (
            private_evaluation.get("grader_result")
            or context["classification"]["teaching_result"]
        ),
        "grader_basis": (
            private_evaluation.get("grader_basis")
            or private_evaluation.get("basis")
            or private_evaluation.get("decisive_reason")
        ),
        "provided_by": private_evaluation.get("provided_by") or "private_grader_capsule",
        "missing_fields": sorted({item.strip() for item in evaluation_missing if item.strip()}),
    }
    learner_evidence = {
        "answer_text": learner.get("answer_text") or learner.get("reply_text"),
        "choice": learner.get("choice"),
        "confidence": assessment["confidence"],
        "first_action": assessment.get("first_action"),
        "reasoning": learner.get("reasoning"),
        "prompt_level": assessment["prompt_level"],
        "observed_at": context["event_time"],
    }
    canonical_assessment = {
        "first_result": context["classification"]["teaching_result"],
        "choice_result": assessment["choice_result"],
        "reasoning_result": assessment["reasoning_result"],
        "confidence": assessment["confidence"],
        "prompt_level": assessment["prompt_level"],
        "first_break": assessment["first_break"],
        "first_break_provenance": assessment["first_break_provenance"],
        "first_action": assessment.get("first_action"),
        "first_action_provenance": assessment.get("first_action_provenance"),
    }
    trace = normalize_interaction_trace(interaction_trace)
    if context["classification"]["capture_required"]:
        _require_capture_evidence_complete(
            current_question, learner_evidence, evaluation, trace
        )
    missing_fields = set(evaluation["missing_fields"])
    if not current_question["public_text"]:
        missing_fields.add("current_question.public_text")
    if not evaluation["grader_basis"]:
        missing_fields.add("evaluation_evidence.grader_basis")
    normalized_question_mode = str(question_mode or "").strip() or (
        "image_question"
        if current_question["attachment_sha256s"]
        else "dialogue_only"
    )
    if normalized_question_mode not in {"dialogue_only", "image_question"}:
        raise CurrentQuestionContextError("question_mode_invalid")
    bundle = {
        "schema_version": "current-question-evidence-bundle-v3",
        "question_mode": normalized_question_mode,
        "context_id": context["context_id"],
        "request_id": context["request_id"],
        "session_id": context["session_id"],
        "item_id": context["item_id"],
        "source_id": context["source_id"],
        "source_kind": context["source"],
        "study_date": context["study_date"],
        "event_time": context["event_time"],
        "timezone": TIMEZONE,
        "source_binding_sha256": context["source_binding_sha256"],
        "current_question": current_question,
        "learner_evidence": learner_evidence,
        "evaluation_evidence": evaluation,
        "assessment": canonical_assessment,
        "frozen_assistant_feedback": {
            "text": frozen_feedback,
            "sha256": feedback_sha256,
        },
        "provenance": {
            "source_locator": context["source_id"],
            "source_sha256": context["source_binding_sha256"],
            "context_sha256": context_sha256,
            "attachment_provenance": [],
            "created_at": context["event_time"],
            "missing_items": sorted(missing_fields),
        },
        "missing_fields": sorted(missing_fields),
        "attachment_objects": [],
        "formal_write_count": 0,
        "interaction_trace": trace,
    }
    return bundle


__all__ = [
    "SCHEMA",
    "TIMEZONE",
    "CurrentQuestionContextError",
    "classify",
    "normalize_interaction_trace",
    "INTERACTION_TRACE_SCHEMA_V2",
    "private_bundle",
    "validate_context",
]
