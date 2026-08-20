#!/usr/bin/env python3
"""Bounded current-question feedback, capture, and receipt-gated navigation.

The internal current-turn primitive records only the current first answer,
commits one private evidence bundle, and conditionally creates one answer-safe
capture.  The external ``answer-current-and-next`` composition may then invoke
the separate next-item action only after the current item is resolved.  No path
reads personalization, history, related questions, variants, Luna state, or a
formal table.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import capture_hot_writer_408 as capture_hot
import current_question_context_408 as context_model
import current_question_evidence_408 as private_evidence
import intake_fact_capture_408 as capture_model
import review_outcome_hot_408 as review_outcome


TURN_SCHEMA = "managed-408-current-question-turn-v1"
RECOVERY_RESULT_SCHEMA = "managed-408-current-question-recovery-result-v1"
NEXT_ITEM_SCHEMA = "managed-408-next-item-v1"
CONTINUE_CURRENT_SCHEMA = "managed-408-continue-current-v1"
ANSWER_AND_NEXT_SCHEMA = "managed-408-answer-current-and-next-v1"
MAX_FEEDBACK_BYTES = 16 * 1024
MAX_ATTACHMENTS_JSON_BYTES = 64 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CAPTURE_HOT_DIRNAME = ".capture-hot-writer-v1"
CANONICAL_PROMPT_LEVEL_MAP = {
    "none": "none",
    "L1": "minimal",
    "L2": "progressive",
    "L3": "progressive",
    "L4": "progressive",
    "L5": "full",
}


class ManagedCurrentTurnError(RuntimeError):
    """One bounded current-question transaction was invalid."""


FaultInjector = Callable[[str], None]


def _json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    options: dict[str, Any] = {"ensure_ascii": False, "sort_keys": True}
    if pretty:
        options["indent"] = 2
    else:
        options["separators"] = (",", ":")
    return (json.dumps(value, **options) + "\n").encode("utf-8")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _frozen_feedback(value: object) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        raise ManagedCurrentTurnError("feedback_text_required")
    body = value.strip()
    raw = body.encode("utf-8")
    if len(raw) > MAX_FEEDBACK_BYTES:
        raise ManagedCurrentTurnError("feedback_text_too_large")
    return body, _sha256(raw)


def _fault(injector: FaultInjector | None, phase: str) -> None:
    if injector is not None:
        injector(phase)


def _validate_controlled_assessment(
    context: dict[str, Any], private_evaluation: dict[str, Any]
) -> None:
    """Bind the public controlled verdict to the private grader capsule."""

    if not isinstance(private_evaluation, dict) or not private_evaluation:
        raise ManagedCurrentTurnError("private_grader_evidence_required")
    learner_choice = str(
        context["learner_evidence"].get("choice") or ""
    ).strip().upper()
    declared = str(context["assessment"].get("choice_result") or "")
    correct_option = str(
        private_evaluation.get("correct_option") or ""
    ).strip().upper()
    if learner_choice in {"BLANK", "UNCERTAIN", "不确定", "空白", ""}:
        if declared not in {"blank", "uncertain", "not_applicable"}:
            raise ManagedCurrentTurnError(
                "controlled_assessment_blank_choice_drifted"
            )
    elif learner_choice in {"A", "B", "C", "D"} and correct_option in {
        "A",
        "B",
        "C",
        "D",
    }:
        derived = "correct" if learner_choice == correct_option else "incorrect"
        if declared != derived:
            raise ManagedCurrentTurnError(
                "controlled_assessment_private_grader_drifted"
            )

    grader_result = str(
        private_evaluation.get("grader_result") or ""
    ).strip().lower()
    if grader_result:
        expected = {
            "correct": {"independent_correct", "fragile_correct"},
            "independent_correct": {"independent_correct"},
            "fragile_correct": {"fragile_correct"},
            "partial": {"partial"},
            "wrong": {"wrong"},
            "incorrect": {"wrong"},
            "uncertain": {"uncertain"},
            "blank": {"uncertain"},
        }.get(grader_result)
        if expected is not None and context["classification"]["teaching_result"] not in expected:
            raise ManagedCurrentTurnError(
                "controlled_assessment_grader_result_drifted"
            )


def _capture_paths(repo: Path) -> tuple[Path, Path, Path]:
    root = capture_model.capture_root(repo)
    return root / "events.jsonl", root / "state.json", root / CAPTURE_HOT_DIRNAME


def _canonical_prompt_level(prompt_level: str) -> str:
    mapped = CANONICAL_PROMPT_LEVEL_MAP.get(prompt_level)
    if mapped is None:
        raise ManagedCurrentTurnError("prompt_level_mapping_missing")
    return mapped


def _direct_user_facts(context: dict[str, Any]) -> dict[str, str]:
    assessment = context["assessment"]
    provenance = assessment["first_break_provenance"]
    first_break = assessment["first_break"]
    if provenance in {"user_report", "visible_evidence"}:
        result = {
            "user_error_entry": first_break,
            "user_error_provenance": provenance,
            "observed_at": context["event_time"],
        }
    else:
        result = {
            "user_error_entry": "未观察到",
            "user_error_provenance": "not_observed",
            "observed_at": context["event_time"],
        }
    if assessment.get("first_action") is not None:
        result["first_action"] = str(assessment["first_action"])
        result["first_action_provenance"] = str(
            assessment["first_action_provenance"]
        )
    return result


def _capture_payload(
    context: dict[str, Any], evidence_receipt: dict[str, Any]
) -> dict[str, Any]:
    if evidence_receipt.get("evidence_status") != "ready":
        raise ManagedCurrentTurnError("capture_evidence_receipt_not_ready")
    identity = {
        "status": "unknown",
        "formal_id": "",
        "mode": "unknown",
        "basis": "正式身份留待日终 Sol 重新核验",
    }
    source_facts = {
        "subject": "未确认",
        "question_type": "当前单题首答",
        "source_id": context["source_id"],
        "details_id": context["context_id"],
    }
    missing: list[str] = ["formal_identity", "main_knowledge"]
    if _direct_user_facts(context)["user_error_provenance"] == "not_observed":
        missing.append("user_error_entry")
    anchor = context["source_id"]
    raw = {
        "schema": capture_model.SCHEMA,
        "study_date": context["study_date"],
        "timezone": context_model.TIMEZONE,
        "idempotency_key": (
            f"current-question:{context['context_id']}:fast-capture:v1"
        ),
        "stable_evidence_refs": [
            {
                "kind": capture_model.CURRENT_QUESTION_EVIDENCE_REF_KIND,
                "locator": evidence_receipt["locator"],
                "sha256": evidence_receipt["manifest_sha256"],
            }
        ],
        "source_facts": source_facts,
        "user_facts": _direct_user_facts(context),
        "identity_hint": identity,
        "answer_safe_context_anchor": (
            f"当前题 {context['source_id']} 的 {anchor} 非独立首答证据"
        ),
        "missing_fields": sorted(set(missing)),
        "formalization_authorized": True,
        "authorization_policy": (
            capture_model.CURRENT_QUESTION_FAILURE_STANDING_POLICY
        ),
    }
    return capture_model._validate_capture_payload(raw)


def _record_ordinary_first(
    repo: Path,
    context: dict[str, Any],
    *,
    evidence_manifest_sha256: str | None,
) -> dict[str, Any]:
    assessment = context["assessment"]
    classification = context["classification"]
    canonical_source = (
        context["source"]
        if context["source"] in {"daily_practice", "evening_d0"}
        else "daily_practice"
    )
    choice_result = assessment["choice_result"]
    if choice_result == "blank":
        choice_result = "uncertain"
    prompt_level = _canonical_prompt_level(assessment["prompt_level"])
    first_result = classification["canonical_first_result"]
    fragile_override = classification["fragile_override"]
    if (
        classification["teaching_result"] == "fragile_correct"
        and prompt_level != "none"
    ):
        first_result = "partial"
        fragile_override = False
    try:
        return review_outcome.record_outcome_hot(
            repo,
            source=canonical_source,
            session_id=context["session_id"],
            item_id=context["item_id"],
            source_id=context["source_id"],
            mechanism_key=context["source_id"],
            knowledge_point=context["source_id"],
            first_result=first_result,
            choice_result=choice_result,
            reasoning_result=assessment["reasoning_result"],
            confidence=assessment["confidence"],
            prompt_level=prompt_level,
            first_break=assessment["first_break"],
            first_break_provenance=assessment["first_break_provenance"],
            event_time=context["event_time"],
            observed_date=context["study_date"],
            idempotency_key=context["idempotency_key"],
            formal_node_id=None,
            review_unit_id=None,
            fragile_override=fragile_override,
            managed_user_reply_binding_sha256=evidence_manifest_sha256,
            presentation_kind="current_question",
            current_question_minimal=True,
        )
    except review_outcome.HotOutcomeError as exc:
        raise ManagedCurrentTurnError(f"first_answer_commit_failed:{exc}") from exc


def _morning_args(context: dict[str, Any]) -> argparse.Namespace:
    classification = context["classification"]
    assessment = context["assessment"]
    learner_choice = str(context["learner_evidence"].get("choice") or "BLANK").upper()
    if learner_choice in {"UNCERTAIN", "不确定", "空白"}:
        learner_choice = "BLANK"
    surface_sha = str(
        context["question_evidence"].get("public_surface_sha256") or ""
    )
    if not SHA256_RE.fullmatch(surface_sha):
        raise ManagedCurrentTurnError("morning_surface_sha256_required")
    teaching_result = classification["teaching_result"]
    session_result = "blank" if teaching_result == "uncertain" else teaching_result
    choice_result = assessment["choice_result"]
    if choice_result == "blank":
        choice_result = "uncertain"
    prompt_level = _canonical_prompt_level(assessment["prompt_level"])
    return argparse.Namespace(
        repo=str(context["_repo"]),
        session=context["session_id"],
        item=context["item_id"],
        display_surface_sha256=surface_sha,
        learner_choice=learner_choice,
        result=session_result,
        choice_result=choice_result,
        reasoning_result=assessment["reasoning_result"],
        confidence=assessment["confidence"],
        response_seconds=None,
        prompt_level=prompt_level,
        first_break=assessment["first_break"],
        first_break_provenance=assessment["first_break_provenance"],
        feedback_type="current_question_feedback",
        current_only=True,
        _current_question_managed_v1=True,
        _current_question_capsule_verified=True,
        _current_question_controlled_choice_result=choice_result,
        _current_question_receipt_only=True,
        _current_question_feedback_sha256=context.get("_feedback_sha256"),
        managed_user_reply_binding_sha256=context.get(
            "_evidence_manifest_sha256"
        ),
    )


def _record_morning_first(
    repo: Path,
    context: dict[str, Any],
    *,
    evidence_manifest_sha256: str | None,
) -> dict[str, Any]:
    try:
        import morning_review_session as morning_session
    except ImportError as exc:
        raise ManagedCurrentTurnError("morning_session_module_unavailable") from exc
    internal = {
        **context,
        "_repo": repo,
        "_evidence_manifest_sha256": evidence_manifest_sha256,
    }
    try:
        return morning_session.command_record_first(_morning_args(internal))
    except morning_session.SessionError as exc:
        raise ManagedCurrentTurnError(f"first_answer_commit_failed:{exc}") from exc


def _record_first_answer(
    repo: Path,
    context: dict[str, Any],
    *,
    evidence_manifest_sha256: str | None,
) -> dict[str, Any]:
    if context["source"] == "morning_review":
        return _record_morning_first(
            repo, context, evidence_manifest_sha256=evidence_manifest_sha256
        )
    return _record_ordinary_first(
        repo, context, evidence_manifest_sha256=evidence_manifest_sha256
    )


def _validate_first_answer_receipts(
    context: dict[str, Any], result: dict[str, Any]
) -> None:
    if not isinstance(result, dict):
        raise ManagedCurrentTurnError("first_answer_commit_receipt_missing")
    if context["source"] == "morning_review":
        required = (
            "review_commit_receipt_sha256",
            "session_binding_receipt_sha256",
        )
    else:
        required = ("commit_receipt_sha256",)
    for field in required:
        if not SHA256_RE.fullmatch(str(result.get(field) or "")):
            raise ManagedCurrentTurnError(
                f"first_answer_commit_receipt_invalid:{field}"
            )
    if context["source"] == "morning_review":
        _validate_event_specific_session_commit(
            result.get("session_first_commit"), "session_first_commit"
        )
        _validate_event_specific_session_commit(
            result.get("session_binding_commit"), "session_binding_commit"
        )
        if (
            result["session_binding_commit"].get("receipt_sha256")
            != result.get("session_binding_receipt_sha256")
        ):
            raise ManagedCurrentTurnError(
                "session_binding_receipt_hash_drifted"
            )
        receipt_verification = result.get("review_receipt_verification")
        if (
            not isinstance(receipt_verification, dict)
            or receipt_verification.get("status") != "verified"
            or receipt_verification.get("receipt_sha256")
            != result.get("review_commit_receipt_sha256")
        ):
            raise ManagedCurrentTurnError(
                "review_receipt_verification_binding_failed"
            )
    verification = (
        result.get("review_verification")
        if context["source"] == "morning_review"
        else result.get("verification")
    )
    if not isinstance(verification, dict) or verification.get("status") not in {
        "verified",
        "pass",
    }:
        raise ManagedCurrentTurnError("first_answer_commit_verification_failed")
    if (
        context["source"] == "morning_review"
        and verification.get("receipt_sha256")
        != result.get("review_commit_receipt_sha256")
    ):
        raise ManagedCurrentTurnError("review_commit_verification_binding_failed")


def _validate_event_specific_session_commit(
    commit: object, label: str
) -> None:
    if not isinstance(commit, dict):
        raise ManagedCurrentTurnError(f"{label}_missing")
    receipt_sha = str(commit.get("receipt_sha256") or "")
    verification = commit.get("verification")
    if (
        commit.get("status") not in {"pass", "committed", "already_committed"}
        or not SHA256_RE.fullmatch(receipt_sha)
        or not isinstance(verification, dict)
        or verification.get("status") != "pass"
        or verification.get("receipt_sha256") != receipt_sha
    ):
        raise ManagedCurrentTurnError(f"{label}_verification_failed")


def _validate_session_feedback_receipt(result: dict[str, Any] | None) -> None:
    if not isinstance(result, dict):
        raise ManagedCurrentTurnError("session_feedback_receipt_missing")
    commit = result.get("session_commit")
    _validate_event_specific_session_commit(commit, "session_feedback_commit")


def _mark_morning_feedback(
    repo: Path,
    context: dict[str, Any],
    first_answer: dict[str, Any],
    *,
    feedback_sha256: str,
) -> dict[str, Any] | None:
    if context["source"] != "morning_review":
        return None
    try:
        import morning_review_session as morning_session
    except ImportError as exc:
        raise ManagedCurrentTurnError(
            "session_feedback_receipt_failed:module_unavailable"
        ) from exc
    try:
        internal = {
            **context,
            "_repo": repo,
            "_evidence_manifest_sha256": None,
            "_feedback_sha256": feedback_sha256,
        }
        return morning_session.command_reveal_feedback(
            _morning_args(internal),
            review_receipt_sha256=first_answer.get(
                "review_commit_receipt_sha256"
            ),
            session_binding_receipt_sha256=first_answer.get(
                "session_binding_receipt_sha256"
            ),
        )
    except morning_session.SessionError as exc:
        raise ManagedCurrentTurnError(
            f"session_feedback_receipt_failed:{exc}"
        ) from exc


def _commit_capture(repo: Path, payload: dict[str, Any]) -> dict[str, Any]:
    ledger, state, hot_root = _capture_paths(repo)
    if not ledger.is_file() or not state.is_file():
        raise ManagedCurrentTurnError("capture_truth_not_prepared")
    try:
        with capture_hot.capture_ledger_lock(ledger) as lock_token:
            result = capture_hot.capture_fact(
                ledger,
                state,
                hot_root,
                lock_token=lock_token,
                validated_payload=payload,
            )
    except (capture_hot.CaptureHotWriterError, OSError) as exc:
        raise ManagedCurrentTurnError(f"capture_commit_failed:{exc}") from exc
    joint = result.get("joint_verification") or {}
    if (
        result.get("status") not in {"CAPTURED", "ALREADY_COMMITTED"}
        or result.get("quality_status") != "awaiting_daily_curation"
        or joint.get("status") != "PASS"
        or result.get("formal_write_count") != 0
    ):
        raise ManagedCurrentTurnError("capture_commit_not_verified")
    return result


def _ensure_background_handoff_pending(
    *,
    capture_payload: dict[str, Any],
    context: dict[str, Any],
    evidence_manifest_sha256: str,
    evidence_status: str,
    private_root: str | Path | None,
) -> tuple[str, dict[str, Any]]:
    canonical = capture_hot.canonical_fact_event_from_payload(
        capture_payload, created_at=context["event_time"]
    )
    capture_id = str(canonical["capture_id"])
    handoff = private_evidence.publish_background_handoff_pending(
        private_root=private_root,
        capture_id=capture_id,
        context_id=context["context_id"],
        item_id=context["item_id"],
        evidence_manifest_sha256=evidence_manifest_sha256,
        created_at=context["event_time"],
        status=(
            "evidence_pending"
            if evidence_status == "evidence_pending"
            else "teaching_pending"
        ),
    )
    return capture_id, handoff


def _interaction_trace_events_sha256(
    evidence_bundle: dict[str, Any] | None,
) -> str | None:
    trace = (
        evidence_bundle.get("interaction_trace")
        if isinstance(evidence_bundle, dict)
        else None
    )
    if not isinstance(trace, dict) or not isinstance(trace.get("events"), list):
        return None
    events = [
        {
            "role": row["role"],
            "kind": row["kind"],
            "text": row["text"],
            "observed_at": None,
        }
        for row in trace["events"]
    ]
    return _sha256(_json_bytes(events)) if events else None


def _publish_first_turn_ready_handoff(
    *,
    context: dict[str, Any],
    capture: dict[str, Any],
    evidence_manifest_sha256: str,
    evidence_bundle: dict[str, Any] | None,
    private_root: str | Path | None,
) -> dict[str, Any]:
    trace_sha = _interaction_trace_events_sha256(evidence_bundle)
    ready_attestation = private_evidence.publish_metadata_object(
        {
            "schema": private_evidence.TURN_RECEIPT_SCHEMA,
            "status": "background_handoff_attested",
            "completion_kind": "first_turn_complete",
            "context_id": context["context_id"],
            "session_id": context["session_id"],
            "item_id": context["item_id"],
            "capture_id": capture["capture_id"],
            "capture_receipt_sha256": capture["receipt_sha256"],
            "evidence_manifest_sha256": evidence_manifest_sha256,
            "interaction_trace_sha256": trace_sha,
            "event_time": context["event_time"],
            "advance_allowed": False,
            "formal_write_count": 0,
        },
        kind="turns",
        private_root=private_root,
    )
    return private_evidence.publish_background_handoff_ready(
        private_root=private_root,
        capture_id=str(capture["capture_id"]),
        context_id=context["context_id"],
        item_id=context["item_id"],
        evidence_manifest_sha256=evidence_manifest_sha256,
        capture_receipt_sha256=str(capture["receipt_sha256"]),
        updated_at=context["event_time"],
        completion_kind="first_turn_complete",
        resolution_receipt_sha256=ready_attestation["sha256"],
        interaction_trace_sha256=trace_sha,
    )


def _publish_recovery(
    *,
    context: dict[str, Any],
    feedback_sha256: str,
    evidence_receipt: dict[str, Any] | None,
    capture_payload: dict[str, Any] | None,
    failed_stage: str,
    reason_code: str,
    feedback_text: str,
    first_answer: dict[str, Any] | None,
    evidence_bundle: dict[str, Any] | None,
    staged_attachment_objects: list[dict[str, Any]] | None,
    private_root: str | Path | None,
) -> dict[str, str]:
    recovery_kind = {
        "first_answer": "first_answer_recovery",
        "session_feedback": "session_feedback_recovery",
        "capture": "capture_recovery",
        "private_evidence": "capture_evidence_recovery",
    }.get(failed_stage, "turn_recovery")
    recovery = {
        "schema": private_evidence.RECOVERY_SCHEMA,
        "recovery_kind": recovery_kind,
        "context": context,
        "feedback_text": feedback_text,
        "feedback_sha256": feedback_sha256,
        "evidence_locator": (
            evidence_receipt.get("locator") if evidence_receipt else None
        ),
        "evidence_manifest_sha256": (
            evidence_receipt.get("manifest_sha256") if evidence_receipt else None
        ),
        "capture_payload": capture_payload,
        "evidence_bundle": evidence_bundle,
        "staged_attachment_objects": staged_attachment_objects or [],
        "first_answer": first_answer,
        "failed_stage": failed_stage,
        "reason_code": reason_code[:320],
        "formal_write_count": 0,
    }
    return private_evidence.publish_metadata_object(
        recovery, kind="recoveries", private_root=private_root
    )


def _publish_study_observation(
    *,
    context: dict[str, Any],
    evidence_receipt: dict[str, Any],
    first_answer: dict[str, Any],
    private_root: str | Path | None,
) -> dict[str, str]:
    first_receipt_sha = str(
        first_answer.get("commit_receipt_sha256")
        or first_answer.get("review_commit_receipt_sha256")
        or ""
    )
    evidence_manifest_sha = str(evidence_receipt.get("manifest_sha256") or "")
    if not SHA256_RE.fullmatch(first_receipt_sha):
        raise ManagedCurrentTurnError("observation_first_answer_receipt_missing")
    if not SHA256_RE.fullmatch(evidence_manifest_sha):
        raise ManagedCurrentTurnError("observation_evidence_binding_missing")
    observation_id = (
        "OBS-"
        + _sha256(
            (
                f"{context['context_id']}\0"
                f"{evidence_manifest_sha}\0"
                f"{first_receipt_sha}"
            ).encode("utf-8")
        )[:24].upper()
    )
    published = private_evidence.publish_study_observation(
        {
            "schema_version": private_evidence.STUDY_OBSERVATION_SCHEMA,
            "observation_id": observation_id,
            "context_id": context["context_id"],
            "request_id": context["request_id"],
            "session_id": context["session_id"],
            "item_id": context["item_id"],
            "source_id": context["source_id"],
            "study_date": context["study_date"],
            "event_time": context["event_time"],
            "first_result": "independent_correct",
            "evidence_locator": evidence_receipt["locator"],
            "evidence_manifest_sha256": evidence_manifest_sha,
            "first_answer_receipt_sha256": first_receipt_sha,
            "processing_status": "awaiting_background_analysis",
            "formal_write_count": 0,
        },
        private_root=private_root,
    )
    return {
        "observation_id": observation_id,
        "locator": published["locator"],
        "sha256": published["sha256"],
    }


def _turn_advance_allowed(
    *,
    context: dict[str, Any],
    capture_status: str,
    observation_status: str,
    feedback_authorized: bool,
) -> bool:
    if not feedback_authorized:
        return False
    first_result = context["classification"]["teaching_result"]
    if first_result == "independent_correct":
        return (
            capture_status == "not_eligible"
            and observation_status == "awaiting_background_analysis"
        )
    if first_result in {"wrong", "partial", "uncertain"}:
        return False
    return capture_status == "awaiting_daily_curation"


def _publish_turn_receipt(
    *,
    context: dict[str, Any],
    feedback_sha256: str,
    first_answer: dict[str, Any] | None,
    capture: dict[str, Any] | None,
    capture_status: str,
    recovery: dict[str, str] | None,
    feedback_authorized: bool,
    recovery_kind: str | None,
    private_root: str | Path | None,
    observation: dict[str, Any] | None = None,
    observation_status: str = "not_applicable",
) -> dict[str, str]:
    receipt = {
        "schema": private_evidence.TURN_RECEIPT_SCHEMA,
        "context_id": context["context_id"],
        "request_id": context["request_id"],
        "session_id": context["session_id"],
        "item_id": context["item_id"],
        "feedback_sha256": feedback_sha256,
        "first_result": context["classification"]["teaching_result"],
        "first_answer_receipt_sha256": (
            first_answer.get("commit_receipt_sha256")
            or first_answer.get("review_commit_receipt_sha256")
            if first_answer
            else None
        ),
        "capture_id": capture.get("capture_id") if capture else None,
        "capture_receipt_sha256": capture.get("receipt_sha256") if capture else None,
        "capture_status": capture_status,
        "observation_id": (
            observation.get("observation_id") if observation else None
        ),
        "observation_locator": (
            observation.get("locator") if observation else None
        ),
        "observation_status": observation_status,
        "recovery_locator": recovery.get("locator") if recovery else None,
        "recovery_kind": recovery_kind,
        "feedback_authorized": feedback_authorized,
        "advance_allowed": _turn_advance_allowed(
            context=context,
            capture_status=capture_status,
            observation_status=observation_status,
            feedback_authorized=feedback_authorized,
        ),
        "next_display_receipt_sha256": None,
        "formal_write_count": 0,
    }
    return private_evidence.publish_metadata_object(
        receipt, kind="turns", private_root=private_root
    )


def run_current_question_turn(
    repo: str | Path,
    raw_context: dict[str, Any],
    *,
    feedback_text: str,
    private_evaluation: dict[str, Any],
    interaction_trace: object | None = None,
    attachments: list[dict[str, Any]] | None = None,
    question_mode: str | None = None,
    private_root: str | Path | None = None,
    fault_injector: FaultInjector | None = None,
) -> dict[str, Any]:
    """Freeze feedback, then perform one bounded current-question transaction."""

    root = Path(repo).expanduser().resolve()
    if not root.is_dir():
        raise ManagedCurrentTurnError("repository_missing")
    context = context_model.validate_context(raw_context)
    _validate_controlled_assessment(context, private_evaluation)
    context_binding_sha = _sha256(
        _json_bytes(
            {
                key: value
                for key, value in context.items()
                if key not in {"classification", "context_id"}
            }
        )
    )
    feedback, feedback_sha = _frozen_feedback(feedback_text)
    capture_required = context["classification"]["capture_required"]
    effective_trace = interaction_trace
    learner_choice = str(context["learner_evidence"].get("choice") or "").strip().upper()
    if capture_required and learner_choice in {"A", "B", "C", "D"}:
        effective_trace = _trace_with_current_answer(
            context_model.normalize_interaction_trace(interaction_trace),
            learner_choice,
        )
    evidence_receipt: dict[str, Any] | None = None
    evidence_bundle: dict[str, Any] | None = None
    staged_attachment_objects: list[dict[str, Any]] = []
    capture_payload: dict[str, Any] | None = None
    first_answer: dict[str, Any] | None = None
    capture: dict[str, Any] | None = None
    recovery: dict[str, str] | None = None
    recovery_kind: str | None = None
    capture_status = "not_eligible"
    observation: dict[str, Any] | None = None
    observation_status = "not_applicable"
    session_feedback: dict[str, Any] | None = None
    background_handoff: dict[str, Any] | None = None
    private_evidence_failure_reason: str | None = None

    if capture_required and not context["source_stable"]:
        capture_status = "capture_unavailable_unstable_evidence"
    elif context["source_stable"]:
        try:
            evidence_bundle = context_model.private_bundle(
                context,
                feedback_text=feedback,
                private_evaluation=private_evaluation,
                interaction_trace=effective_trace,
                question_mode=question_mode,
            )
            staged_attachment_objects = private_evidence.stage_attachments(
                attachments,
                private_root=private_root,
                question_mode=str(evidence_bundle["question_mode"]),
            )
            if evidence_bundle.get("schema_version") == private_evidence.BUNDLE_SCHEMA_V3:
                evidence_bundle["current_question"]["attachment_sha256s"] = [
                    row["sha256"] for row in staged_attachment_objects
                ]
            evidence_receipt = private_evidence.publish_bundle(
                evidence_bundle,
                private_root=private_root,
                staged_attachment_objects=staged_attachment_objects,
            )
            if capture_required and evidence_receipt.get("evidence_status") == "ready":
                capture_payload = _capture_payload(context, evidence_receipt)
        except (context_model.CurrentQuestionContextError, private_evidence.CurrentQuestionEvidenceError) as exc:
            private_evidence_failure_reason = str(exc)
            if capture_required:
                capture_status = "capture_pending_recovery"
            else:
                observation_status = "observation_pending_recovery"
            try:
                if attachments and len(staged_attachment_objects) != len(attachments):
                    raise private_evidence.CurrentQuestionEvidenceError(
                        "attachment staging incomplete; retry original input"
                    )
                recovery = _publish_recovery(
                    context=context,
                    feedback_text=feedback,
                    feedback_sha256=feedback_sha,
                    evidence_receipt=None,
                    capture_payload=None,
                    failed_stage="private_evidence",
                    reason_code=str(exc),
                    first_answer=None,
                    evidence_bundle=evidence_bundle,
                    staged_attachment_objects=staged_attachment_objects,
                    private_root=private_root,
                )
            except (private_evidence.CurrentQuestionEvidenceError, OSError):
                recovery = None
            recovery_kind = (
                "capture_evidence_recovery"
                if capture_required
                else "observation_evidence_recovery"
            )

    try:
        _fault(fault_injector, "before_first_answer_commit")
        first_answer = _record_first_answer(
            root,
            context,
            evidence_manifest_sha256=(
                evidence_receipt.get("manifest_sha256")
                if evidence_receipt
                else None
            ),
        )
        _validate_first_answer_receipts(context, first_answer)
        _fault(fault_injector, "after_first_answer_commit")
    except Exception as exc:
        recovery = _publish_recovery(
            context=context,
            feedback_text=feedback,
            feedback_sha256=feedback_sha,
            evidence_receipt=evidence_receipt,
            capture_payload=capture_payload,
            failed_stage="first_answer",
            reason_code=str(exc),
            first_answer=first_answer,
            evidence_bundle=evidence_bundle,
            staged_attachment_objects=staged_attachment_objects,
            private_root=private_root,
        )
        capture_status = "first_answer_pending_recovery"
        recovery_kind = "first_answer_recovery"
        turn_receipt = _publish_turn_receipt(
            context=context,
            feedback_sha256=feedback_sha,
            first_answer=first_answer,
            capture=None,
            capture_status=capture_status,
            recovery=recovery,
            feedback_authorized=False,
            recovery_kind=recovery_kind,
            private_root=private_root,
        )
        return {
            "schema": TURN_SCHEMA,
            "status": "recovery_required",
            "reason_code": "first_answer_commit_or_receipt_failed",
            "context_id": context["context_id"],
            "session_id": context["session_id"],
            "item_id": context["item_id"],
            "feedback_authorized": False,
            "first_answer_committed": False,
            "capture_status": capture_status,
            "recovery_kind": recovery_kind,
            "recovery_locator": recovery["locator"],
            "turn_receipt_locator": turn_receipt["locator"],
            "turn_receipt_sha256": turn_receipt["sha256"],
            "next_item_published": False,
            "formal_write_count": 0,
        }

    if context["source"] == "morning_review":
        try:
            _fault(fault_injector, "before_session_feedback_receipt")
            session_feedback = _mark_morning_feedback(
                root,
                context,
                first_answer,
                feedback_sha256=feedback_sha,
            )
            _validate_session_feedback_receipt(session_feedback)
            _fault(fault_injector, "after_session_feedback_receipt")
        except Exception as exc:
            recovery = _publish_recovery(
                context=context,
                feedback_text=feedback,
                feedback_sha256=feedback_sha,
                evidence_receipt=evidence_receipt,
                capture_payload=capture_payload,
                failed_stage="session_feedback",
                reason_code=str(exc),
                first_answer=first_answer,
                evidence_bundle=evidence_bundle,
                staged_attachment_objects=staged_attachment_objects,
                private_root=private_root,
            )
            capture_status = "session_feedback_pending_recovery"
            recovery_kind = "session_feedback_recovery"
            turn_receipt = _publish_turn_receipt(
                context=context,
                feedback_sha256=feedback_sha,
                first_answer=first_answer,
                capture=None,
                capture_status=capture_status,
                recovery=recovery,
                feedback_authorized=False,
                recovery_kind=recovery_kind,
                private_root=private_root,
            )
            return {
                "schema": TURN_SCHEMA,
                "status": "recovery_required",
                "reason_code": "session_feedback_receipt_failed",
                "context_id": context["context_id"],
                "session_id": context["session_id"],
                "item_id": context["item_id"],
                "feedback_authorized": False,
                "first_answer_committed": True,
                "capture_status": capture_status,
                "recovery_kind": recovery_kind,
                "recovery_locator": recovery["locator"],
                "turn_receipt_locator": turn_receipt["locator"],
                "turn_receipt_sha256": turn_receipt["sha256"],
                "next_item_published": False,
                "formal_write_count": 0,
            }

    if recovery_kind in {
        "capture_evidence_recovery",
        "observation_evidence_recovery",
    }:
        try:
            recovery = _publish_recovery(
                context=context,
                feedback_text=feedback,
                feedback_sha256=feedback_sha,
                evidence_receipt=None,
                capture_payload=None,
                failed_stage="private_evidence",
                reason_code=(
                    private_evidence_failure_reason
                    or "private_evidence_publish_failed"
                ),
                first_answer=first_answer,
                evidence_bundle=evidence_bundle,
                staged_attachment_objects=staged_attachment_objects,
                private_root=private_root,
            )
        except (private_evidence.CurrentQuestionEvidenceError, OSError):
            recovery = None

    if (
        first_answer is not None
        and capture_required
        and context["source_stable"]
        and capture_payload is not None
        and capture_status != "capture_unavailable_unstable_evidence"
    ):
        try:
            if evidence_receipt is None or evidence_receipt.get("evidence_status") != "ready":
                raise ManagedCurrentTurnError("capture_evidence_receipt_not_ready")
            expected_capture_id, background_handoff = (
                _ensure_background_handoff_pending(
                    capture_payload=capture_payload,
                    context=context,
                    evidence_manifest_sha256=str(
                        evidence_receipt["manifest_sha256"]
                    ),
                    evidence_status=str(evidence_receipt["evidence_status"]),
                    private_root=private_root,
                )
            )
            _fault(fault_injector, "before_capture_commit")
            capture = _commit_capture(root, capture_payload)
            _fault(fault_injector, "after_capture_commit")
            if capture.get("capture_id") != expected_capture_id:
                raise ManagedCurrentTurnError("capture_background_handoff_id_drifted")
            if context["classification"]["teaching_result"] not in {
                "wrong",
                "partial",
                "uncertain",
            } and evidence_receipt.get("evidence_status") == "ready":
                background_handoff = (
                    _publish_first_turn_ready_handoff(
                        context=context,
                        capture=capture,
                        evidence_manifest_sha256=str(
                            evidence_receipt["manifest_sha256"]
                        ),
                        evidence_bundle=evidence_bundle,
                        private_root=private_root,
                    )
                )
            capture_status = "awaiting_daily_curation"
        except Exception as exc:
            try:
                recovery = _publish_recovery(
                    context=context,
                    feedback_text=feedback,
                    feedback_sha256=feedback_sha,
                    evidence_receipt=evidence_receipt,
                    capture_payload=capture_payload,
                    failed_stage="capture",
                    reason_code=str(exc),
                    first_answer=first_answer,
                    evidence_bundle=evidence_bundle,
                    staged_attachment_objects=staged_attachment_objects,
                    private_root=private_root,
                )
            except (private_evidence.CurrentQuestionEvidenceError, OSError):
                recovery = None
            capture_status = "capture_pending_recovery"
            recovery_kind = "capture_recovery"

    if (
        first_answer is not None
        and not capture_required
        and context["source_stable"]
        and evidence_receipt is not None
    ):
        try:
            observation = _publish_study_observation(
                context=context,
                evidence_receipt=evidence_receipt,
                first_answer=first_answer,
                private_root=private_root,
            )
            observation_status = "awaiting_background_analysis"
        except (
            ManagedCurrentTurnError,
            private_evidence.CurrentQuestionEvidenceError,
            OSError,
        ):
            observation_status = "observation_pending_recovery"

    try:
        turn_receipt = _publish_turn_receipt(
            context=context,
            feedback_sha256=feedback_sha,
            first_answer=first_answer,
            capture=capture,
            capture_status=capture_status,
            recovery=recovery,
            feedback_authorized=True,
            recovery_kind=recovery_kind,
            private_root=private_root,
            observation=observation,
            observation_status=observation_status,
        )
    except (private_evidence.CurrentQuestionEvidenceError, OSError):
        if capture_status != "capture_pending_recovery":
            raise
        turn_receipt = None
    return {
        "schema": TURN_SCHEMA,
        "status": "feedback_ready",
        "context_id": context["context_id"],
        "session_id": context["session_id"],
        "item_id": context["item_id"],
        "first_result": context["classification"]["teaching_result"],
        "feedback_text": feedback,
        "feedback_sha256": feedback_sha,
        "feedback_authorized": True,
        "first_answer_committed": first_answer is not None,
        "session_feedback_receipt_sha256": (
            ((session_feedback or {}).get("session_commit") or {}).get(
                "receipt_sha256"
            )
            if session_feedback is not None
            else None
        ),
        "capture_status": capture_status,
        "capture_id": capture.get("capture_id") if capture else None,
        "capture_receipt_sha256": capture.get("receipt_sha256") if capture else None,
        "background_handoff_status": (
            background_handoff.get("status") if background_handoff else None
        ),
        "background_handoff_locator": (
            background_handoff.get("locator") if background_handoff else None
        ),
        "background_handoff": background_handoff,
        "observation_status": observation_status,
        "observation_id": (
            observation.get("observation_id") if observation else None
        ),
        "observation_locator": (
            observation.get("locator") if observation else None
        ),
        "observation_sha256": (
            observation.get("sha256") if observation else None
        ),
        "evidence_locator": evidence_receipt.get("locator") if evidence_receipt else None,
        "evidence_manifest_sha256": (
            evidence_receipt.get("manifest_sha256") if evidence_receipt else None
        ),
        "evidence_status": (
            evidence_receipt.get("evidence_status") if evidence_receipt else None
        ),
        "recovery_locator": recovery.get("locator") if recovery else None,
        "recovery_kind": recovery_kind,
        "turn_receipt_locator": (
            turn_receipt.get("locator") if turn_receipt else None
        ),
        "turn_receipt_sha256": (
            turn_receipt.get("sha256") if turn_receipt else None
        ),
        "advance_allowed": _turn_advance_allowed(
            context=context,
            capture_status=capture_status,
            observation_status=observation_status,
            feedback_authorized=True,
        ),
        "retry_idempotency_key": (
            context["idempotency_key"]
            if capture_status == "capture_pending_recovery"
            else None
        ),
        "context_binding_sha256": context_binding_sha,
        "next_display_receipt_sha256": None,
        "next_item_published": False,
        "luna_call_count": 0,
        "personalization_read_count": 0,
        "formal_write_count": 0,
    }


def recover_current_receipts(
    repo: str | Path,
    recovery_locator: str,
    *,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(repo).expanduser().resolve()
    recovery = private_evidence.read_metadata_object(
        recovery_locator, kind="recoveries", private_root=private_root
    )
    raw_context = dict(recovery["context"])
    raw_context.pop("classification", None)
    raw_context.pop("context_id", None)
    context = context_model.validate_context(raw_context)
    feedback, feedback_sha = _frozen_feedback(recovery.get("feedback_text"))
    if feedback_sha != recovery.get("feedback_sha256"):
        raise ManagedCurrentTurnError("recovery_feedback_binding_drifted")
    failed_stage = str(recovery.get("failed_stage") or "")
    if failed_stage not in {"first_answer", "session_feedback"}:
        raise ManagedCurrentTurnError("receipt_recovery_stage_invalid")
    first_answer = recovery.get("first_answer")
    if failed_stage == "first_answer":
        first_answer = _record_first_answer(
            root,
            context,
            evidence_manifest_sha256=recovery.get("evidence_manifest_sha256"),
        )
        _validate_first_answer_receipts(context, first_answer)
    else:
        _validate_first_answer_receipts(context, first_answer)

    session_feedback: dict[str, Any] | None = None
    if failed_stage in {"first_answer", "session_feedback"} and context["source"] == "morning_review":
        session_feedback = _mark_morning_feedback(
            root,
            context,
            first_answer,
            feedback_sha256=feedback_sha,
        )
        _validate_session_feedback_receipt(session_feedback)

    payload = recovery.get("capture_payload")
    capture_recovery: dict[str, str] | None = None
    observation: dict[str, Any] | None = None
    observation_status = "not_applicable"
    if not context["classification"]["capture_required"]:
        capture_status = "not_eligible"
        evidence_locator = str(recovery.get("evidence_locator") or "")
        evidence_manifest_sha = str(
            recovery.get("evidence_manifest_sha256") or ""
        )
        if (
            not evidence_locator.startswith(private_evidence.LOCATOR_PREFIX)
            or not SHA256_RE.fullmatch(evidence_manifest_sha)
        ):
            raise ManagedCurrentTurnError(
                "receipt_recovery_observation_evidence_missing"
            )
        observation = _publish_study_observation(
            context=context,
            evidence_receipt={
                "locator": evidence_locator,
                "manifest_sha256": evidence_manifest_sha,
            },
            first_answer=first_answer,
            private_root=private_root,
        )
        observation_status = "awaiting_background_analysis"
    elif not context["source_stable"]:
        capture_status = "capture_unavailable_unstable_evidence"
    elif isinstance(payload, dict):
        capture_status = "capture_pending_recovery"
        capture_recovery = _publish_recovery(
            context=context,
            feedback_text=feedback,
            feedback_sha256=feedback_sha,
            evidence_receipt=(
                {
                    "locator": recovery.get("evidence_locator"),
                    "manifest_sha256": recovery.get("evidence_manifest_sha256"),
                }
                if recovery.get("evidence_locator")
                and recovery.get("evidence_manifest_sha256")
                else None
            ),
            capture_payload=payload,
            failed_stage="capture",
            reason_code="receipt_recovery_completed_capture_still_pending",
            first_answer=first_answer,
            evidence_bundle=recovery.get("evidence_bundle"),
            staged_attachment_objects=recovery.get("staged_attachment_objects"),
            private_root=private_root,
        )
    else:
        raise ManagedCurrentTurnError("receipt_recovery_lacks_capture_payload")
    turn_receipt = _publish_turn_receipt(
        context=context,
        feedback_sha256=feedback_sha,
        first_answer=first_answer,
        capture=None,
        capture_status=capture_status,
        recovery=capture_recovery,
        feedback_authorized=True,
        recovery_kind=("capture_recovery" if capture_recovery else None),
        private_root=private_root,
        observation=observation,
        observation_status=observation_status,
    )
    operation_result: dict[str, Any] | None = None
    if observation is not None:
        operation_result = _complete_recovered_answer_operation(
            root,
            context=context,
            recovery_locator=recovery_locator,
            feedback_text=feedback,
            capture_status=capture_status,
            capture_id=None,
            observation_status=observation_status,
            observation=observation,
            turn_receipt=turn_receipt,
            background_handoff=None,
            private_root=private_root,
        )
    following = (
        operation_result.get("next_item")
        if isinstance(operation_result, dict)
        else None
    )
    return {
        "schema": RECOVERY_RESULT_SCHEMA,
        "status": "feedback_ready",
        "context_id": context["context_id"],
        "capture_status": capture_status,
        "capture_id": None,
        "capture_receipt_sha256": None,
        "observation_status": observation_status,
        "observation_id": (
            observation.get("observation_id") if observation else None
        ),
        "observation_locator": (
            observation.get("locator") if observation else None
        ),
        "feedback_text": feedback,
        "feedback_authorized": True,
        "session_feedback_receipt_sha256": (
            ((session_feedback or {}).get("session_commit") or {}).get(
                "receipt_sha256"
            )
            if session_feedback is not None
            else None
        ),
        "position_changed": (
            isinstance(following, dict) and following.get("status") == "published"
        ),
        "next_item": following,
        "answer_operation_result": operation_result,
        "recovery_locator": (
            capture_recovery.get("locator") if capture_recovery else None
        ),
        "turn_receipt_locator": turn_receipt["locator"],
        "turn_receipt_sha256": turn_receipt["sha256"],
        "formal_write_count": 0,
    }


def _repair_answer_display_lifecycle_after_capture(
    *,
    context: dict[str, Any],
    recovery: dict[str, Any],
    capture: dict[str, Any],
    turn_receipt: dict[str, str],
    background_handoff: dict[str, Any],
    evidence_bundle: dict[str, Any] | None,
    private_root: str | Path | None,
) -> bool:
    first_answer = recovery.get("first_answer")
    canonical_event = (
        first_answer.get("canonical_event")
        if isinstance(first_answer, dict)
        else None
    )
    canonical_first_result = (
        first_answer.get("normalized_result")
        if isinstance(first_answer, dict)
        else None
    ) or (
        canonical_event.get("first_result")
        if isinstance(canonical_event, dict)
        else None
    )
    lifecycle_first_result = (
        canonical_first_result
        if canonical_first_result in {"wrong", "partial", "uncertain"}
        else context["classification"]["teaching_result"]
    )
    if lifecycle_first_result not in {
        "wrong",
        "partial",
        "uncertain",
    }:
        return False
    display_digest = str(context.get("display_receipt_sha256") or "")
    if not SHA256_RE.fullmatch(display_digest):
        return False
    recovered_trace: dict[str, Any] | None = None
    try:
        legacy_binding, legacy_supplement = (
            private_evidence.read_trace_supplement_for_capture(
                str(capture["capture_id"]), private_root=private_root
            )
        )
    except private_evidence.CurrentQuestionEvidenceError as exc:
        if "unavailable" not in str(exc):
            raise
    else:
        if (
            legacy_binding.get("supplement_kind") == "legacy_backfill"
            and legacy_binding.get("resolution_receipt_sha256") is None
        ):
            recovered_trace = context_model.normalize_interaction_trace(
                [
                    {
                        "role": row["role"],
                        "kind": row["kind"],
                        "text": row["text"],
                    }
                    for row in legacy_supplement["events"]
                ]
            )
    bundle_trace = (
        evidence_bundle.get("interaction_trace")
        if isinstance(evidence_bundle, dict)
        else None
    )
    if recovered_trace is None:
        recovered_trace = (
            bundle_trace
            if isinstance(bundle_trace, dict)
            else context_model.normalize_interaction_trace(None)
        )
    request_operation_id = str(context.get("request_id") or "")
    if not re.fullmatch(r"AON-[0-9A-F]{64}", request_operation_id):
        request_operation_id = (
            "AON-"
            + _sha256(
                (
                    f"recovered\0{display_digest}\0{context['context_id']}"
                ).encode("utf-8")
            ).upper()
        )
    with private_evidence.answer_display_lock(
        display_digest, private_root=private_root
    ):
        lifecycle = private_evidence.read_answer_display_lifecycle(
            display_digest, private_root=private_root
        )
        if lifecycle is not None:
            first_turn = lifecycle["first_turn"]
            if (
                lifecycle.get("first_result")
                != lifecycle_first_result
                or first_turn.get("context_id") != context["context_id"]
                or first_turn.get("session_id") != context["session_id"]
                or first_turn.get("item_id") != context["item_id"]
            ):
                raise ManagedCurrentTurnError(
                    "capture_recovery_display_lifecycle_drifted"
                )
            recovered_trace = _merge_interaction_traces(
                recovered_trace, lifecycle["interaction_trace"]
            )
            first_operation_id = lifecycle["first_operation_id"]
        else:
            first_turn = {
                "schema": TURN_SCHEMA,
                "status": "feedback_ready",
                "context_id": context["context_id"],
                "session_id": context["session_id"],
                "item_id": context["item_id"],
                "first_result": lifecycle_first_result,
                "feedback_text": recovery.get("feedback_text"),
                "feedback_sha256": recovery.get("feedback_sha256"),
                "feedback_authorized": True,
                "first_answer_committed": True,
                "session_feedback_receipt_sha256": None,
                "observation_status": "not_applicable",
                "observation_id": None,
                "observation_locator": None,
                "observation_sha256": None,
                "evidence_locator": recovery.get("evidence_locator"),
                "evidence_manifest_sha256": recovery.get(
                    "evidence_manifest_sha256"
                ),
                "next_display_receipt_sha256": None,
                "next_item_published": False,
                "luna_call_count": 0,
                "personalization_read_count": 0,
                "formal_write_count": 0,
            }
            first_operation_id = request_operation_id
        repaired_first_turn = {
            **first_turn,
            "capture_status": "awaiting_daily_curation",
            "capture_id": capture["capture_id"],
            "capture_receipt_sha256": capture["receipt_sha256"],
            "background_handoff_status": background_handoff["status"],
            "background_handoff_locator": background_handoff["locator"],
            "background_handoff": background_handoff,
            "recovery_locator": None,
            "recovery_kind": None,
            "turn_receipt_locator": turn_receipt["locator"],
            "turn_receipt_sha256": turn_receipt["sha256"],
            "advance_allowed": False,
            "retry_idempotency_key": None,
        }
        private_evidence.write_answer_display_lifecycle(
            {
                "schema": private_evidence.ANSWER_DISPLAY_LIFECYCLE_SCHEMA,
                "display_receipt_sha256": display_digest,
                "status": "first_recorded",
                "first_operation_id": first_operation_id,
                "first_result": lifecycle_first_result,
                "first_turn": repaired_first_turn,
                "interaction_trace": recovered_trace,
                "resolution": None,
                "formal_write_count": 0,
            },
            private_root=private_root,
        )
    return True


def _complete_recovered_answer_operation(
    repo: Path,
    *,
    context: dict[str, Any],
    recovery_locator: str,
    feedback_text: str,
    capture_status: str,
    capture_id: str | None,
    observation_status: str,
    observation: dict[str, Any] | None,
    turn_receipt: dict[str, str],
    background_handoff: dict[str, Any] | None,
    private_root: str | Path | None,
) -> dict[str, Any] | None:
    operation_id = str(context.get("request_id") or "")
    display_digest = str(context.get("display_receipt_sha256") or "")
    if not re.fullmatch(r"AON-[0-9A-F]{64}", operation_id):
        return None
    if not SHA256_RE.fullmatch(display_digest):
        return None
    with private_evidence.answer_display_lock(
        display_digest, private_root=private_root
    ), private_evidence.answer_operation_lock(
        operation_id, private_root=private_root
    ):
        operation = private_evidence.read_answer_operation(
            operation_id, private_root=private_root
        )
        if operation is None:
            return None
        prepared_context = (operation.get("prepared") or {}).get("context")
        if not isinstance(prepared_context, dict):
            raise ManagedCurrentTurnError(
                "recovered_answer_operation_context_missing"
            )
        prepared_context_id = context_model.validate_context(
            prepared_context
        )["context_id"]
        if (
            prepared_context_id != context["context_id"]
            or prepared_context.get("session_id") != context["session_id"]
            or prepared_context.get("item_id") != context["item_id"]
        ):
            raise ManagedCurrentTurnError(
                "recovered_answer_operation_context_drifted"
            )
        existing_result = operation.get("result")
        if (
            operation.get("status") == "complete"
            and isinstance(existing_result, dict)
            and existing_result.get("recovery_locator") is None
            and existing_result.get("capture_status") == capture_status
            and existing_result.get("capture_id") == capture_id
            and existing_result.get("observation_status") == observation_status
        ):
            replay = dict(existing_result)
            replay["idempotent_replay"] = True
            replay["learner_evidence_write_count"] = 0
            replay["formal_write_count"] = 0
            return replay
        if (
            isinstance(existing_result, dict)
            and existing_result.get("recovery_locator") not in {
                None,
                recovery_locator,
            }
            and existing_result.get("status")
            not in {"recovery_required", "feedback_ready_recovery_required"}
        ):
            raise ManagedCurrentTurnError(
                "recovered_answer_operation_state_drifted"
            )
        first_result = context["classification"]["teaching_result"]
        advance_allowed = _turn_advance_allowed(
            context=context,
            capture_status=capture_status,
            observation_status=observation_status,
            feedback_authorized=True,
        )
        following: dict[str, Any] | None = None
        if advance_allowed:
            following = next_item(
                repo,
                prior_turn_receipt_locator=turn_receipt["locator"],
                private_root=private_root,
            )
        if first_result in {"wrong", "partial", "uncertain"}:
            status = "feedback_ready_continue_current"
        elif following is None:
            status = "feedback_ready_recovery_required"
        elif following.get("status") == "complete":
            status = "feedback_ready_session_complete"
        else:
            status = "feedback_and_next_ready"
        result = _answer_result(
            operation_id=operation_id,
            status=status,
            feedback_text=feedback_text,
            first_result=first_result,
            capture_status=capture_status,
            capture_id=capture_id,
            observation_status=observation_status,
            observation_id=(
                observation.get("observation_id") if observation else None
            ),
            observation_locator=(
                observation.get("locator") if observation else None
            ),
            turn_receipt_locator=turn_receipt["locator"],
            following=following,
            recovery_locator=None,
            learner_evidence_write_count=0,
            background_handoff=background_handoff,
        )
        return _complete_answer_operation(
            operation, result, private_root=private_root
        )


def recover_current_capture(
    repo: str | Path,
    recovery_locator: str,
    *,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    """Retry only private evidence plus the required capture or observation."""

    root = Path(repo).expanduser().resolve()
    recovery = private_evidence.read_metadata_object(
        recovery_locator, kind="recoveries", private_root=private_root
    )
    failed_stage = recovery.get("failed_stage")
    if failed_stage not in {"capture", "private_evidence"}:
        raise ManagedCurrentTurnError("capture_recovery_stage_invalid")
    raw_context = dict(recovery["context"])
    raw_context.pop("classification", None)
    raw_context.pop("context_id", None)
    context = context_model.validate_context(raw_context)
    payload = recovery.get("capture_payload")
    evidence_bundle = recovery.get("evidence_bundle")
    first_answer = recovery.get("first_answer")
    if not isinstance(first_answer, dict):
        raise ManagedCurrentTurnError("capture_recovery_first_answer_receipt_missing")
    _validate_first_answer_receipts(context, first_answer)
    evidence_manifest_sha = str(
        recovery.get("evidence_manifest_sha256") or ""
    )
    evidence_receipt: dict[str, Any] | None = None
    if failed_stage == "private_evidence":
        bundle = evidence_bundle
        staged = recovery.get("staged_attachment_objects")
        if not isinstance(bundle, dict) or not isinstance(staged, list):
            raise ManagedCurrentTurnError("capture_evidence_recovery_material_missing")
        evidence_receipt = private_evidence.publish_bundle(
            bundle,
            private_root=private_root,
            staged_attachment_objects=staged,
        )
        evidence_manifest_sha = str(evidence_receipt["manifest_sha256"])
        if context["classification"]["capture_required"]:
            payload = _capture_payload(context, evidence_receipt)
    elif recovery.get("evidence_locator") and evidence_manifest_sha:
        _, reopened_bundle = private_evidence.read_bundle(
            str(recovery["evidence_locator"]), private_root=private_root
        )
        evidence_receipt = {
            "locator": recovery["evidence_locator"],
            "manifest_sha256": evidence_manifest_sha,
            "evidence_status": private_evidence.bundle_evidence_status(
                reopened_bundle
            ),
        }
    if not context["classification"]["capture_required"]:
        if failed_stage != "private_evidence" or evidence_receipt is None:
            raise ManagedCurrentTurnError("observation_recovery_stage_invalid")
        observation = _publish_study_observation(
            context=context,
            evidence_receipt=evidence_receipt,
            first_answer=first_answer,
            private_root=private_root,
        )
        turn_receipt = _publish_turn_receipt(
            context=context,
            feedback_sha256=str(recovery["feedback_sha256"]),
            first_answer=first_answer,
            capture=None,
            capture_status="not_eligible",
            recovery=None,
            feedback_authorized=True,
            recovery_kind=None,
            private_root=private_root,
            observation=observation,
            observation_status="awaiting_background_analysis",
        )
        operation_result = _complete_recovered_answer_operation(
            root,
            context=context,
            recovery_locator=recovery_locator,
            feedback_text=str(recovery["feedback_text"]),
            capture_status="not_eligible",
            capture_id=None,
            observation_status="awaiting_background_analysis",
            observation=observation,
            turn_receipt=turn_receipt,
            background_handoff=None,
            private_root=private_root,
        )
        following = (
            operation_result.get("next_item")
            if isinstance(operation_result, dict)
            else None
        )
        return {
            "schema": RECOVERY_RESULT_SCHEMA,
            "status": "observation_ready",
            "context_id": context["context_id"],
            "capture_status": "not_eligible",
            "capture_id": None,
            "capture_receipt_sha256": None,
            "observation_status": "awaiting_background_analysis",
            "observation_id": observation["observation_id"],
            "observation_locator": observation["locator"],
            "observation_sha256": observation["sha256"],
            "feedback_authorized": True,
            "position_changed": (
                isinstance(following, dict)
                and following.get("status") == "published"
            ),
            "next_item": following,
            "answer_operation_result": operation_result,
            "outcome_write_count": 0,
            "session_write_count": 0,
            "turn_receipt_locator": turn_receipt["locator"],
            "turn_receipt_sha256": turn_receipt["sha256"],
            "formal_write_count": 0,
        }
    if not isinstance(payload, dict):
        raise ManagedCurrentTurnError("capture_recovery_payload_missing")
    if not SHA256_RE.fullmatch(evidence_manifest_sha):
        raise ManagedCurrentTurnError("capture_recovery_evidence_binding_missing")
    expected_capture_id, background_handoff = (
        _ensure_background_handoff_pending(
            capture_payload=payload,
            context=context,
            evidence_manifest_sha256=evidence_manifest_sha,
            evidence_status=str(
                (evidence_receipt or {}).get("evidence_status") or "ready"
            ),
            private_root=private_root,
        )
    )
    capture = _commit_capture(
        root, capture_model._validate_capture_payload(payload)
    )
    if capture.get("capture_id") != expected_capture_id:
        raise ManagedCurrentTurnError("capture_recovery_background_handoff_id_drifted")
    if context["classification"]["teaching_result"] not in {
        "wrong",
        "partial",
        "uncertain",
    } and (evidence_receipt or {}).get("evidence_status") == "ready":
        background_handoff = _publish_first_turn_ready_handoff(
            context=context,
            capture=capture,
            evidence_manifest_sha256=evidence_manifest_sha,
            evidence_bundle=(
                evidence_bundle if isinstance(evidence_bundle, dict) else None
            ),
            private_root=private_root,
        )
    turn_receipt = _publish_turn_receipt(
        context=context,
        feedback_sha256=str(recovery["feedback_sha256"]),
        first_answer=first_answer,
        capture=capture,
        capture_status="awaiting_daily_curation",
        recovery=None,
        feedback_authorized=True,
        recovery_kind=None,
        private_root=private_root,
    )
    display_lifecycle_repaired = _repair_answer_display_lifecycle_after_capture(
        context=context,
        recovery=recovery,
        capture=capture,
        turn_receipt=turn_receipt,
        background_handoff=background_handoff,
        evidence_bundle=(
            evidence_bundle if isinstance(evidence_bundle, dict) else None
        ),
        private_root=private_root,
    )
    operation_result = _complete_recovered_answer_operation(
        root,
        context=context,
        recovery_locator=recovery_locator,
        feedback_text=str(recovery["feedback_text"]),
        capture_status="awaiting_daily_curation",
        capture_id=str(capture["capture_id"]),
        observation_status="not_applicable",
        observation=None,
        turn_receipt=turn_receipt,
        background_handoff=background_handoff,
        private_root=private_root,
    )
    following = (
        operation_result.get("next_item")
        if isinstance(operation_result, dict)
        else None
    )
    return {
        "schema": RECOVERY_RESULT_SCHEMA,
        "status": "awaiting_daily_curation",
        "context_id": context["context_id"],
        "capture_id": capture["capture_id"],
        "capture_receipt_sha256": capture["receipt_sha256"],
        "background_handoff_status": background_handoff["status"],
        "background_handoff_locator": background_handoff["locator"],
        "background_handoff": background_handoff,
        "display_lifecycle_repaired": display_lifecycle_repaired,
        "feedback_authorized": True,
        "position_changed": (
            isinstance(following, dict) and following.get("status") == "published"
        ),
        "next_item": following,
        "answer_operation_result": operation_result,
        "outcome_write_count": 0,
        "session_write_count": 0,
        "turn_receipt_locator": turn_receipt["locator"],
        "turn_receipt_sha256": turn_receipt["sha256"],
        "formal_write_count": 0,
    }


def _publish_prepared_display(
    repo: Path,
    *,
    session_id: str,
    item_id: str,
    private_root: str | Path | None,
) -> dict[str, Any]:
    import morning_review_prepared_pack_408 as prepared_pack

    shown = prepared_pack.show_item(repo, session_id, item_id)
    surface = dict(shown["surface"])
    lines = [str(surface["stem"]).strip(), ""]
    lines.extend(
        f"{key}. {str(surface['options'][key]).strip()}"
        for key in ("A", "B", "C", "D")
    )
    lines.extend(["", str(surface["response_instruction"]).strip()])
    display_text = "\n".join(lines).strip() + "\n"
    scheduling_binding = prepared_pack.scheduling_binding_for_item(
        repo, session_id, item_id
    )
    display = {
        "schema": private_evidence.TURN_RECEIPT_SCHEMA,
        "status": "display_published",
        "context_id": None,
        "request_id": None,
        "session_id": session_id,
        "item_id": item_id,
        "feedback_sha256": None,
        "first_result": None,
        "first_answer_receipt_sha256": None,
        "capture_id": None,
        "capture_receipt_sha256": None,
        "capture_status": "not_started",
        "recovery_locator": None,
        "advance_allowed": False,
        "surface_sha256": shown["surface_sha256"],
        "display_sha256": _sha256(display_text.encode("utf-8")),
        "next_display_receipt_sha256": None,
        "scheduling_binding": scheduling_binding,
        "formal_write_count": 0,
    }
    receipt = private_evidence.publish_metadata_object(
        display, kind="turns", private_root=private_root
    )
    return {
        "schema": NEXT_ITEM_SCHEMA,
        "status": "published",
        "session_id": session_id,
        "item_id": item_id,
        "surface_sha256": shown["surface_sha256"],
        "display_text": display_text,
        "display_receipt_locator": receipt["locator"],
        "display_receipt_sha256": receipt["sha256"],
        "scheduling_binding": scheduling_binding,
        "formal_write_count": 0,
    }


def next_item(
    repo: str | Path,
    *,
    prior_turn_receipt_locator: str,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    """Publish the next unanswered prepared surface as a separate action."""

    prior = private_evidence.read_metadata_object(
        prior_turn_receipt_locator, kind="turns", private_root=private_root
    )
    if prior.get("advance_allowed") is not True:
        raise ManagedCurrentTurnError("current_capture_recovery_required_before_next_item")
    root = Path(repo).expanduser().resolve()
    try:
        import morning_review_prepared_pack_408 as prepared_pack
        import morning_review_session as morning_session
    except ImportError as exc:
        raise ManagedCurrentTurnError("morning_next_item_module_unavailable") from exc
    session_id = str(prior.get("session_id") or "")
    prior_item_id = str(prior.get("item_id") or "")
    session_dir = morning_session._session_dir(root, session_id)
    with morning_session.session_lock(session_dir):
        state = morning_session._load_state(session_dir, session_id)
        morning_session._ensure_active(state)
        active_order = [
            item_id
            for item_id in state["item_order"]
            if state["items"][item_id].get("resolution")
            not in morning_session.NAVIGATION_EXCLUDED_RESOLUTIONS
        ]
        if (
            prior_item_id not in active_order
            or state["items"][prior_item_id].get("first") is None
        ):
            raise ManagedCurrentTurnError(
                "prior_turn_receipt_is_not_navigation_frontier"
            )
        next_id = next(
            (
                item_id
                for item_id in active_order
                if state["items"][item_id].get("first") is None
            ),
            None,
        )
        prior_index = active_order.index(prior_item_id)
        if next_id is None:
            frontier_matches = prior_index == len(active_order) - 1
            if not frontier_matches and prior.get("status") == "teaching_resolved":
                item = state["items"][prior_item_id]
                resolution = item.get("teaching_resolution")
                supplement = prior.get("trace_supplement")
                receipt_sha = str(
                    prior.get("session_resolution_receipt_sha256") or ""
                )
                resolution_matches = bool(
                    item.get("resolution") == "relearn_required"
                    and isinstance(resolution, dict)
                    and isinstance(supplement, dict)
                    and resolution.get("capture_id") == prior.get("capture_id")
                    and resolution.get("capture_receipt_sha256")
                    == prior.get("capture_receipt_sha256")
                    and resolution.get("resolution_attestation_sha256")
                    == prior.get("resolution_attestation_sha256")
                    and resolution.get("trace_supplement_sha256")
                    == supplement.get("object_sha")
                    and resolution.get("interaction_trace_sha256")
                    == supplement.get("interaction_trace_sha256")
                    and resolution.get("mastery_effect") == "none"
                    and resolution.get("retention_effect") == "none"
                    and resolution.get("independent_repair") is False
                    and SHA256_RE.fullmatch(receipt_sha)
                )
                if resolution_matches:
                    try:
                        morning_session._verify_session_hot_event_receipt(
                            session_dir,
                            receipt_sha,
                            event_type="teaching_resolved",
                            item_id=prior_item_id,
                        )
                    except morning_session.SessionError:
                        resolution_matches = False
                frontier_matches = resolution_matches
        else:
            next_index = active_order.index(next_id)
            frontier_matches = next_index > 0 and prior_index == next_index - 1
        if not frontier_matches:
            raise ManagedCurrentTurnError(
                "prior_turn_receipt_is_not_navigation_frontier"
            )
    if next_id is None:
        return {
            "schema": NEXT_ITEM_SCHEMA,
            "status": "complete",
            "session_id": session_id,
            "item_id": None,
            "formal_write_count": 0,
        }
    return _publish_prepared_display(
        root,
        session_id=session_id,
        item_id=next_id,
        private_root=private_root,
    )


def _complete_answer_operation(
    operation: dict[str, Any],
    result: dict[str, Any],
    *,
    private_root: str | Path | None,
) -> dict[str, Any]:
    private_evidence.write_answer_operation(
        {**operation, "status": "complete", "result": result},
        private_root=private_root,
    )
    return result


def _trace_with_current_answer(
    trace: dict[str, Any], choice: str
) -> dict[str, Any]:
    events = [
        {"role": row["role"], "kind": row["kind"], "text": row["text"]}
        for row in trace["events"]
    ]
    canonical_choice_present = False
    for row in events:
        if row["role"] != "learner" or row["kind"] != "answer":
            continue
        trace_choice = str(row["text"]).strip().upper()
        if trace_choice in {"A", "B", "C", "D"}:
            if trace_choice != choice:
                raise ManagedCurrentTurnError("interaction_trace_choice_drifted")
            canonical_choice_present = True
    if not canonical_choice_present:
        events.append({"role": "learner", "kind": "answer", "text": choice})
    if trace.get("omitted_event_count", 0) and not canonical_choice_present:
        raise ManagedCurrentTurnError(
            "truncated_interaction_trace_must_include_canonical_choice"
        )
    return context_model.normalize_interaction_trace(events)


def _merge_interaction_traces(
    existing: dict[str, Any], current: dict[str, Any]
) -> dict[str, Any]:
    if existing.get("omitted_event_count", 0) or current.get("omitted_event_count", 0):
        raise ManagedCurrentTurnError(
            "interaction_trace_merge_requires_complete_source_traces"
        )
    merged = [
        {
            "role": str(row["role"]),
            "kind": str(row["kind"]),
            "text": str(row["text"]),
        }
        for trace in (existing, current)
        for row in trace["events"]
    ]
    return context_model.normalize_interaction_trace(merged)


def _normalize_attachments_json(
    raw: object | None,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    if raw is None:
        raw = '{"question_mode":"dialogue_only","attachments":[]}'
    if isinstance(raw, str):
        encoded = raw.encode("utf-8")
        if len(encoded) > MAX_ATTACHMENTS_JSON_BYTES:
            raise ManagedCurrentTurnError("attachments_json_too_large")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ManagedCurrentTurnError("attachments_json_invalid") from exc
    else:
        encoded = json.dumps(
            raw, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > MAX_ATTACHMENTS_JSON_BYTES:
            raise ManagedCurrentTurnError("attachments_json_too_large")
        value = raw
    if not isinstance(value, dict) or set(value) != {"question_mode", "attachments"}:
        raise ManagedCurrentTurnError("attachments_json_fields_invalid")
    question_mode = str(value.get("question_mode") or "").strip()
    descriptors = value.get("attachments")
    if question_mode not in {"dialogue_only", "image_question"}:
        raise ManagedCurrentTurnError("attachments_question_mode_invalid")
    if not isinstance(descriptors, list) or len(descriptors) > private_evidence.MAX_ATTACHMENTS:
        raise ManagedCurrentTurnError("attachments_count_invalid")
    if question_mode == "dialogue_only" and descriptors:
        raise ManagedCurrentTurnError("dialogue_only_attachments_forbidden")
    writer_rows: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    for descriptor in descriptors:
        if not isinstance(descriptor, dict) or set(descriptor) != {
            "path",
            "sha256",
            "mime_type",
            "role",
            "label",
        }:
            raise ManagedCurrentTurnError("attachment_descriptor_fields_invalid")
        path_text = descriptor.get("path")
        if not isinstance(path_text, str) or not path_text or "\x00" in path_text:
            raise ManagedCurrentTurnError("attachment_path_invalid")
        path = Path(path_text).expanduser()
        try:
            before = path.lstat()
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise ManagedCurrentTurnError("attachment_must_be_regular_non_symlink")
            if not 0 < before.st_size <= private_evidence.MAX_ATTACHMENT_BYTES:
                raise ManagedCurrentTurnError("attachment_size_invalid")
            with path.open("rb") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode):
                    raise ManagedCurrentTurnError("attachment_must_be_regular_non_symlink")
                data = handle.read(private_evidence.MAX_ATTACHMENT_BYTES + 1)
            after = path.lstat()
        except OSError as exc:
            raise ManagedCurrentTurnError("attachment_read_failed") from exc
        if (
            stat.S_ISLNK(after.st_mode)
            or (before.st_dev, before.st_ino, before.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
            or len(data) != before.st_size
        ):
            raise ManagedCurrentTurnError("attachment_changed_while_reading")
        digest = _sha256(data)
        declared_digest = str(descriptor.get("sha256") or "").strip().lower()
        if not SHA256_RE.fullmatch(declared_digest) or digest != declared_digest:
            raise ManagedCurrentTurnError("attachment_sha256_mismatch")
        mime_type = str(descriptor.get("mime_type") or "").strip().lower()
        role = str(descriptor.get("role") or "").strip()
        label = str(descriptor.get("label") or "").strip()
        try:
            _image_format, detected_mime = private_evidence._verified_image_format(data)
        except private_evidence.CurrentQuestionEvidenceError as exc:
            raise ManagedCurrentTurnError(str(exc)) from exc
        if mime_type != detected_mime:
            raise ManagedCurrentTurnError("attachment_mime_mismatch")
        if role not in {"question_image", "solution_image"}:
            raise ManagedCurrentTurnError("attachment_role_invalid")
        if not label or len(label.encode("utf-8")) > 160:
            raise ManagedCurrentTurnError("attachment_label_invalid")
        writer_rows.append(
            {"data": data, "mime_type": mime_type, "role": role, "label": label}
        )
        identity_rows.append(
            {
                "role": role,
                "label": label,
                "mime_type": mime_type,
                "sha256": digest,
                "byte_count": len(data),
            }
        )
    roles = {row["role"] for row in identity_rows}
    if question_mode == "image_question" and not {
        "question_image",
        "solution_image",
    } <= roles:
        raise ManagedCurrentTurnError(
            "image_question_requires_question_and_solution_images"
        )
    return question_mode, writer_rows, identity_rows


def _answer_result(
    *,
    operation_id: str,
    status: str,
    feedback_text: str | None,
    first_result: str | None,
    capture_status: str | None,
    capture_id: str | None,
    observation_status: str | None,
    observation_id: str | None,
    observation_locator: str | None,
    turn_receipt_locator: str | None,
    following: dict[str, Any] | None,
    recovery_locator: str | None,
    learner_evidence_write_count: int,
    trace_supplement: dict[str, Any] | None = None,
    background_handoff: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema": ANSWER_AND_NEXT_SCHEMA,
        "status": status,
        "operation_id": operation_id,
        "idempotent_replay": False,
        "feedback_authorized": feedback_text is not None,
        "feedback_text": feedback_text,
        "first_result": first_result,
        "capture_status": capture_status,
        "capture_id": capture_id,
        "observation_status": observation_status,
        "observation_id": observation_id,
        "observation_locator": observation_locator,
        "turn_receipt_locator": turn_receipt_locator,
        "next_item_published": (
            isinstance(following, dict) and following.get("status") == "published"
        ),
        "next_item": following,
        "recovery_locator": recovery_locator,
        "trace_supplement": trace_supplement,
        "background_handoff": background_handoff,
        "learner_evidence_write_count": learner_evidence_write_count,
        "luna_call_count": 0,
        "formal_write_count": 0,
    }


def answer_current_and_next(
    repo: str | Path,
    *,
    display_receipt_locator: str,
    choice: str,
    confidence: str,
    prompt_level: str,
    interaction_trace: object | None = None,
    attachments_json: object | None = None,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    """Commit one first answer; advance only after the current item is resolved."""

    normalized_choice = str(choice or "").strip().upper()
    normalized_confidence = str(confidence or "").strip().lower()
    normalized_prompt = str(prompt_level or "").strip()
    if normalized_choice not in {"A", "B", "C", "D"}:
        raise ManagedCurrentTurnError("choice_must_be_A_to_D")
    if normalized_confidence not in {"high", "medium", "low"}:
        raise ManagedCurrentTurnError("confidence_invalid")
    if normalized_prompt not in context_model.PROMPT_LEVELS:
        raise ManagedCurrentTurnError("prompt_level_invalid")
    normalized_trace = _trace_with_current_answer(
        context_model.normalize_interaction_trace(interaction_trace),
        normalized_choice,
    )
    question_mode, attachment_rows, attachment_identity = _normalize_attachments_json(
        attachments_json
    )
    if not isinstance(display_receipt_locator, str) or not display_receipt_locator.startswith(
        private_evidence.TURN_LOCATOR_PREFIX
    ):
        raise ManagedCurrentTurnError("display_receipt_locator_invalid")
    display_digest = display_receipt_locator.removeprefix(
        private_evidence.TURN_LOCATOR_PREFIX
    )
    if not SHA256_RE.fullmatch(display_digest):
        raise ManagedCurrentTurnError("display_receipt_locator_invalid")
    operation_material = {
        "display_receipt_locator": display_receipt_locator,
        "choice": normalized_choice,
        "confidence": normalized_confidence,
        "prompt_level": normalized_prompt,
        "interaction_trace": normalized_trace,
        "question_mode": question_mode,
        "attachments": attachment_identity,
    }
    input_sha = _sha256(_json_bytes(operation_material))
    operation_id = "AON-" + input_sha.upper()
    root = Path(repo).expanduser().resolve()
    if not root.is_dir():
        raise ManagedCurrentTurnError("repository_missing")

    with private_evidence.answer_display_lock(
        display_digest, private_root=private_root
    ), private_evidence.answer_operation_lock(
        operation_id, private_root=private_root
    ):
        operation = private_evidence.read_answer_operation(
            operation_id, private_root=private_root
        )
        if operation is not None and operation.get("input_sha256") != input_sha:
            raise ManagedCurrentTurnError("answer_operation_input_drifted")
        if operation is not None and operation.get("status") == "complete":
            replay = dict(operation["result"])
            replay["idempotent_replay"] = True
            replay["learner_evidence_write_count"] = 0
            replay["formal_write_count"] = 0
            return replay
        lifecycle = private_evidence.read_answer_display_lifecycle(
            display_digest, private_root=private_root
        )
        if lifecycle is not None and lifecycle.get("status") == "resolved":
            replay = dict(lifecycle["resolution"]["result"])
            replay["idempotent_replay"] = True
            replay["learner_evidence_write_count"] = 0
            if operation is None:
                now = dt.datetime.now(ZoneInfo(context_model.TIMEZONE)).isoformat(
                    timespec="microseconds"
                )
                operation = {
                    "schema": private_evidence.ANSWER_OPERATION_SCHEMA,
                    "operation_id": operation_id,
                    "input_sha256": input_sha,
                    "status": "prepared",
                    "event_time": now,
                    "prepared": {"mode": "resolved_replay"},
                    "result": None,
                    "formal_write_count": 0,
                }
            return _complete_answer_operation(
                operation, replay, private_root=private_root
            )

        try:
            import morning_review_prepared_pack_408 as prepared_pack
        except ImportError as exc:
            raise ManagedCurrentTurnError("prepared_pack_module_unavailable") from exc

        if lifecycle is not None:
            if attachment_rows:
                raise ManagedCurrentTurnError("followup_attachments_forbidden")
            if operation is None:
                event_time = dt.datetime.now(
                    ZoneInfo(context_model.TIMEZONE)
                ).isoformat(timespec="microseconds")
                followup = prepared_pack.prepare_followup_attempt_from_display(
                    root,
                    display_receipt_locator=display_receipt_locator,
                    choice=normalized_choice,
                    confidence=normalized_confidence,
                    prompt_level=normalized_prompt,
                    private_root=private_root,
                )
                operation = {
                    "schema": private_evidence.ANSWER_OPERATION_SCHEMA,
                    "operation_id": operation_id,
                    "input_sha256": input_sha,
                    "status": "prepared",
                    "event_time": event_time,
                    "prepared": {
                        "mode": "teaching_followup",
                        "followup": followup,
                        "interaction_trace": normalized_trace,
                    },
                    "result": None,
                    "formal_write_count": 0,
                }
                private_evidence.write_answer_operation(
                    operation, private_root=private_root
                )
            prepared = operation["prepared"]
            followup = prepared["followup"]
            first_turn = lifecycle["first_turn"]
            if (
                first_turn.get("capture_status") != "awaiting_daily_curation"
                or not first_turn.get("capture_id")
                or not SHA256_RE.fullmatch(
                    str(first_turn.get("capture_receipt_sha256") or "")
                )
            ):
                raise ManagedCurrentTurnError(
                    "current_capture_recovery_required_before_correction"
                )
            cumulative_trace = _merge_interaction_traces(
                lifecycle["interaction_trace"], prepared["interaction_trace"]
            )
            if cumulative_trace != lifecycle["interaction_trace"]:
                lifecycle = {**lifecycle, "interaction_trace": cumulative_trace}
                private_evidence.write_answer_display_lifecycle(
                    lifecycle, private_root=private_root
                )
            if followup.get("choice_result") != "correct":
                result = _answer_result(
                    operation_id=operation_id,
                    status="feedback_ready_continue_current",
                    feedback_text=followup["feedback_text"],
                    first_result=lifecycle["first_result"],
                    capture_status=first_turn.get("capture_status"),
                    capture_id=first_turn.get("capture_id"),
                    observation_status="not_applicable",
                    observation_id=None,
                    observation_locator=None,
                    turn_receipt_locator=first_turn.get("turn_receipt_locator"),
                    following=None,
                    recovery_locator=None,
                    learner_evidence_write_count=0,
                    background_handoff=first_turn.get("background_handoff"),
                )
                return _complete_answer_operation(
                    operation, result, private_root=private_root
                )

            trace = cumulative_trace
            trace_sha = _sha256(_json_bytes(trace))
            evidence_manifest_sha = str(
                first_turn.get("evidence_manifest_sha256")
                or followup.get("evidence_manifest_sha256")
                or ""
            )
            capture_id = str(first_turn.get("capture_id") or "")
            capture_receipt_sha = str(
                first_turn.get("capture_receipt_sha256") or ""
            )
            if (
                not SHA256_RE.fullmatch(evidence_manifest_sha)
                or not capture_id
                or not SHA256_RE.fullmatch(capture_receipt_sha)
            ):
                raise ManagedCurrentTurnError(
                    "resolved_trace_original_capture_binding_missing"
                )
            attestation = private_evidence.publish_metadata_object(
                {
                    "schema": private_evidence.TURN_RECEIPT_SCHEMA,
                    "status": "teaching_resolution_attested",
                    "attestation_schema": "teaching-resolution-attestation-v1",
                    "context_id": first_turn["context_id"],
                    "session_id": followup["session_id"],
                    "item_id": followup["item_id"],
                    "capture_id": capture_id,
                    "capture_receipt_sha256": capture_receipt_sha,
                    "evidence_manifest_sha256": evidence_manifest_sha,
                    "interaction_trace_sha256": trace_sha,
                    "resolution_choice_result": "correct",
                    "event_time": operation["event_time"],
                    "advance_allowed": False,
                    "formal_write_count": 0,
                },
                kind="turns",
                private_root=private_root,
            )
            events = [
                {
                    "role": row["role"],
                    "kind": row["kind"],
                    "text": row["text"],
                    "observed_at": None,
                }
                for row in trace["events"]
            ]
            supplement = private_evidence.publish_trace_supplement(
                private_root=private_root,
                capture_id=capture_id,
                context_id=first_turn["context_id"],
                item_id=followup["item_id"],
                evidence_manifest_sha256=evidence_manifest_sha,
                created_at=operation["event_time"],
                supplement_kind="resolved_trace",
                resolution_receipt_sha256=attestation["sha256"],
                events=events,
            )
            if not isinstance(supplement, dict):
                raise ManagedCurrentTurnError("trace_supplement_receipt_invalid")
            supplement_sha = str(
                supplement.get("object_sha")
                or supplement.get("object_sha256")
                or supplement.get("sha256")
                or ""
            )
            if not SHA256_RE.fullmatch(supplement_sha):
                raise ManagedCurrentTurnError("trace_supplement_hash_invalid")
            supplement_trace_sha = str(
                supplement.get("interaction_trace_sha256") or ""
            )
            if not SHA256_RE.fullmatch(supplement_trace_sha):
                raise ManagedCurrentTurnError(
                    "trace_supplement_interaction_hash_invalid"
                )
            try:
                import morning_review_session as morning_session
            except ImportError as exc:
                raise ManagedCurrentTurnError(
                    "morning_session_module_unavailable"
                ) from exc
            try:
                teaching_resolution = morning_session.mark_teaching_resolved(
                    root,
                    followup["session_id"],
                    followup["item_id"],
                    capture_id=capture_id,
                    capture_receipt_sha256=capture_receipt_sha,
                    resolution_attestation_sha256=attestation["sha256"],
                    trace_supplement_sha256=supplement_sha,
                    interaction_trace_sha256=supplement_trace_sha,
                )
            except morning_session.SessionError as exc:
                raise ManagedCurrentTurnError(
                    f"teaching_resolution_session_commit_failed:{exc}"
                ) from exc
            session_resolution_receipt = str(
                (teaching_resolution.get("session_commit") or {}).get(
                    "receipt_sha256"
                )
                or ""
            )
            if not SHA256_RE.fullmatch(session_resolution_receipt):
                raise ManagedCurrentTurnError(
                    "teaching_resolution_session_receipt_invalid"
                )
            final_turn = private_evidence.publish_metadata_object(
                {
                    "schema": private_evidence.TURN_RECEIPT_SCHEMA,
                    "status": "teaching_resolved",
                    "context_id": first_turn["context_id"],
                    "session_id": followup["session_id"],
                    "item_id": followup["item_id"],
                    "capture_id": capture_id,
                    "capture_receipt_sha256": capture_receipt_sha,
                    "evidence_manifest_sha256": evidence_manifest_sha,
                    "resolution_attestation_locator": attestation["locator"],
                    "resolution_attestation_sha256": attestation["sha256"],
                    "trace_supplement": supplement,
                    "session_resolution_receipt_sha256": (
                        session_resolution_receipt
                    ),
                    "mastery_effect": "none",
                    "retention_effect": "none",
                    "independent_repair": False,
                    "feedback_sha256": _sha256(
                        followup["feedback_text"].encode("utf-8")
                    ),
                    "advance_allowed": True,
                    "formal_write_count": 0,
                },
                kind="turns",
                private_root=private_root,
            )
            if (
                (first_turn.get("background_handoff") or {}).get("status")
                == "evidence_pending"
            ):
                background_handoff = first_turn.get("background_handoff")
            else:
                background_handoff = private_evidence.publish_background_handoff_ready(
                    private_root=private_root,
                    capture_id=capture_id,
                    context_id=first_turn["context_id"],
                    item_id=followup["item_id"],
                    evidence_manifest_sha256=evidence_manifest_sha,
                    capture_receipt_sha256=capture_receipt_sha,
                    updated_at=operation["event_time"],
                    completion_kind="teaching_resolved",
                    resolution_receipt_sha256=final_turn["sha256"],
                    interaction_trace_sha256=supplement_trace_sha,
                    trace_supplement_locator=supplement["locator"],
                    trace_supplement_object_sha256=supplement_sha,
                )
            following = next_item(
                root,
                prior_turn_receipt_locator=final_turn["locator"],
                private_root=private_root,
            )
            result = _answer_result(
                operation_id=operation_id,
                status=(
                    "feedback_ready_session_complete"
                    if following.get("status") == "complete"
                    else "feedback_and_next_ready"
                ),
                feedback_text=followup["feedback_text"],
                first_result=lifecycle["first_result"],
                capture_status=first_turn.get("capture_status"),
                capture_id=capture_id,
                observation_status="not_applicable",
                observation_id=None,
                observation_locator=None,
                turn_receipt_locator=final_turn["locator"],
                following=following,
                recovery_locator=None,
                learner_evidence_write_count=0,
                trace_supplement=supplement,
                background_handoff=background_handoff,
            )
            private_evidence.write_answer_display_lifecycle(
                {
                    **lifecycle,
                    "status": "resolved",
                    "resolution": {
                        "operation_id": operation_id,
                        "attestation": attestation,
                        "supplement": supplement,
                        "turn_receipt": final_turn,
                        "result": result,
                    },
                },
                private_root=private_root,
            )
            return _complete_answer_operation(
                operation, result, private_root=private_root
            )

        if operation is None:
            display_receipt = private_evidence.read_metadata_object(
                display_receipt_locator,
                kind="turns",
                private_root=private_root,
            )
            if display_receipt.get("status") != "display_published":
                raise ManagedCurrentTurnError("display_receipt_is_not_current_surface")
            session_id = str(display_receipt.get("session_id") or "")
            item_id = str(display_receipt.get("item_id") or "")
            surface_sha = str(display_receipt.get("surface_sha256") or "")
            if not session_id or not item_id or not SHA256_RE.fullmatch(surface_sha):
                raise ManagedCurrentTurnError("display_receipt_binding_incomplete")
            event_time = dt.datetime.now(ZoneInfo(context_model.TIMEZONE)).isoformat(
                timespec="microseconds"
            )
            request_id = operation_id
            preparation = prepared_pack.prepare_current_turn(
                root,
                session_id=session_id,
                item_id=item_id,
                choice=normalized_choice,
                confidence=normalized_confidence,
                prompt_level=normalized_prompt,
                request_id=request_id,
                event_time=event_time,
                display_surface_sha256=surface_sha,
                display_receipt_locator=display_receipt_locator,
                interaction_trace=normalized_trace,
                include_private_feedback=True,
                private_root=private_root,
            )
            prepared = {
                "mode": "first_answer",
                "context": preparation["context"],
                "grader_capsule_locator": preparation["grader_capsule_locator"],
                "grader_capsule_sha256": preparation["grader_capsule_sha256"],
                "feedback_text": preparation["_private_frozen_feedback_text"],
                "interaction_trace": preparation["_private_interaction_trace"],
                "scheduling_binding": preparation.get("scheduling_binding"),
                "scheduling_binding_status": preparation.get(
                    "scheduling_binding_status"
                ),
            }
            operation = {
                "schema": private_evidence.ANSWER_OPERATION_SCHEMA,
                "operation_id": operation_id,
                "input_sha256": input_sha,
                "status": "prepared",
                "event_time": event_time,
                "prepared": prepared,
                "result": None,
                "formal_write_count": 0,
            }
            private_evidence.write_answer_operation(
                operation, private_root=private_root
            )
        else:
            prepared = operation["prepared"]
        capsule = private_evidence.read_evaluation_capsule(
            prepared["grader_capsule_locator"],
            expected_sha256=prepared["grader_capsule_sha256"],
            private_root=private_root,
        )
        current = run_current_question_turn(
            root,
            prepared["context"],
            feedback_text=prepared["feedback_text"],
            private_evaluation=capsule["evaluation_evidence"],
            interaction_trace=prepared["interaction_trace"],
            attachments=attachment_rows,
            question_mode=question_mode,
            private_root=private_root,
        )
        needs_teaching_lifecycle = (
            current.get("status") == "feedback_ready"
            and current.get("first_result") in {"wrong", "partial", "uncertain"}
        )
        hold_for_teaching = (
            needs_teaching_lifecycle
            and current.get("capture_status") == "awaiting_daily_curation"
        )
        following: dict[str, Any] | None = None
        if current.get("advance_allowed") is True and not hold_for_teaching:
            following = next_item(
                root,
                prior_turn_receipt_locator=str(
                    current.get("turn_receipt_locator") or ""
                ),
                private_root=private_root,
            )
        if current.get("status") != "feedback_ready":
            status = "recovery_required"
        elif hold_for_teaching:
            status = "feedback_ready_continue_current"
        elif following is None:
            status = "feedback_ready_recovery_required"
        elif following.get("status") == "complete":
            status = "feedback_ready_session_complete"
        else:
            status = "feedback_and_next_ready"
        result = _answer_result(
            operation_id=operation_id,
            status=status,
            feedback_text=(
                current.get("feedback_text")
                if current.get("feedback_authorized") is True
                else None
            ),
            first_result=current.get("first_result"),
            capture_status=current.get("capture_status"),
            capture_id=current.get("capture_id"),
            observation_status=current.get("observation_status"),
            observation_id=current.get("observation_id"),
            observation_locator=current.get("observation_locator"),
            turn_receipt_locator=current.get("turn_receipt_locator"),
            following=following,
            recovery_locator=current.get("recovery_locator"),
            learner_evidence_write_count=1,
            background_handoff=current.get("background_handoff"),
        )
        if needs_teaching_lifecycle:
            private_evidence.write_answer_display_lifecycle(
                {
                    "schema": private_evidence.ANSWER_DISPLAY_LIFECYCLE_SCHEMA,
                    "display_receipt_sha256": display_digest,
                    "status": "first_recorded",
                    "first_operation_id": operation_id,
                    "first_result": current["first_result"],
                    "first_turn": current,
                    "interaction_trace": prepared["interaction_trace"],
                    "resolution": None,
                    "formal_write_count": 0,
                },
                private_root=private_root,
            )
        return _complete_answer_operation(
            operation, result, private_root=private_root
        )


def continue_current(
    *,
    prior_turn_receipt_locator: str,
    context_id: str,
    session_id: str,
    item_id: str,
    private_root: str | Path | None = None,
) -> dict[str, Any]:
    """Verify and retain the current item without looking up a successor."""

    prior = private_evidence.read_metadata_object(
        prior_turn_receipt_locator, kind="turns", private_root=private_root
    )
    expected = {
        "context_id": context_id,
        "session_id": session_id,
        "item_id": item_id,
    }
    for field, value in expected.items():
        if prior.get(field) != value:
            raise ManagedCurrentTurnError(
                f"continue_current_{field}_binding_drifted"
            )
    return {
        "schema": CONTINUE_CURRENT_SCHEMA,
        "status": "current_item_retained",
        **expected,
        "capture_status": prior.get("capture_status"),
        "feedback_authorized": prior.get("feedback_authorized") is True,
        "next_item_lookup_count": 0,
        "next_item_published": False,
        "formal_write_count": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--private-root", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    recover = sub.add_parser("recover-current-capture")
    recover.add_argument("--recovery-locator", required=True)
    recover_receipts = sub.add_parser("recover-current-receipts")
    recover_receipts.add_argument("--recovery-locator", required=True)
    answer_next = sub.add_parser("answer-current-and-next")
    answer_next.add_argument("--display-receipt-locator", required=True)
    answer_next.add_argument("--choice", choices=("A", "B", "C", "D"), required=True)
    answer_next.add_argument(
        "--confidence", choices=("high", "medium", "low"), required=True
    )
    answer_next.add_argument(
        "--prompt-level", choices=sorted(context_model.PROMPT_LEVELS), default="none"
    )
    answer_next.add_argument(
        "--trace-json",
        default="[]",
        help="Bounded private current-question trace; never enters the public capture",
    )
    answer_next.add_argument(
        "--attachments-json",
        default='{"question_mode":"dialogue_only","attachments":[]}',
        help="Bounded private attachment descriptors; paths never enter stored evidence",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "recover-current-capture":
            result = recover_current_capture(
                args.repo,
                args.recovery_locator,
                private_root=args.private_root,
            )
        elif args.command == "recover-current-receipts":
            result = recover_current_receipts(
                args.repo,
                args.recovery_locator,
                private_root=args.private_root,
            )
        else:
            result = answer_current_and_next(
                args.repo,
                display_receipt_locator=args.display_receipt_locator,
                choice=args.choice,
                confidence=args.confidence,
                prompt_level=args.prompt_level,
                interaction_trace=json.loads(args.trace_json),
                attachments_json=args.attachments_json,
                private_root=args.private_root,
            )
    except (KeyError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
        result = {
            "schema": TURN_SCHEMA,
            "status": "blocked",
            "reason_code": str(exc),
            "feedback_authorized": False,
            "formal_write_count": 0,
        }
    sys.stdout.buffer.write(_json_bytes(result))
    return 0 if result.get("status") not in {"blocked", "recovery_required"} else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ManagedCurrentTurnError",
    "answer_current_and_next",
    "recover_current_capture",
    "recover_current_receipts",
]
