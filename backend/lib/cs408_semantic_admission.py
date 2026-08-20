"""408-only scene admission and typed Luna proposal contracts.

This module is deliberately subject-local.  It does not read a formal study
repository, start a Provider or MCP server, write through Sol, or normalize a
model-owned identifier/reference.  It is an integration proposal for an R2
successor release, not an active deployment.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Mapping, Sequence


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MCP_REF_RE = re.compile(r"^mcp-item:cs408:[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SCENES = {"morning_review", "formal_problem"}
TYPED_OPERATIONS = {
    "mark_existing_item",
    "propose_new_item",
    "mark_existing_knowledge",
    "propose_new_knowledge",
    "propose_relation",
    "needs_review",
}


class Cs408SemanticAdmissionError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _exact_mapping(
    value: Any, required: set[str], code: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != required:
        raise Cs408SemanticAdmissionError(code)
    return dict(value)


def _hash(value: Any, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise Cs408SemanticAdmissionError(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if not isinstance(value, str) or SAFE_ID_RE.fullmatch(value) is None:
        raise Cs408SemanticAdmissionError(code)
    return value


def _ref(value: Any, prefix: str, code: str) -> str:
    digest = _hash(str(value)[len(prefix) :] if isinstance(value, str) else None, code)
    if value != prefix + digest:
        raise Cs408SemanticAdmissionError(code)
    return str(value)


def _nested_scene_keys(value: Any, *, depth: int = 0) -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if depth > 0 and key in {"scene", "source_kind"}:
                found.append(str(key))
            found.extend(_nested_scene_keys(nested, depth=depth + 1))
    elif isinstance(value, list):
        for nested in value:
            found.extend(_nested_scene_keys(nested, depth=depth + 1))
    return found


def validate_scene_envelope(value: Mapping[str, Any]) -> dict[str, Any]:
    envelope = _exact_mapping(
        value,
        {
            "schema_version",
            "subject",
            "capture_id",
            "scene",
            "source_kind",
            "evidence",
        },
        "cs408_scene_envelope_shape_invalid",
    )
    if envelope["schema_version"] != "cs408-scene-admission-input-v1":
        raise Cs408SemanticAdmissionError("cs408_scene_schema_invalid")
    if envelope["subject"] != "cs408":
        raise Cs408SemanticAdmissionError("cs408_scene_subject_invalid")
    _safe_id(envelope["capture_id"], "cs408_scene_capture_id_invalid")
    scene = envelope["scene"]
    source_kind = envelope["source_kind"]
    if scene not in SCENES or source_kind not in SCENES:
        raise Cs408SemanticAdmissionError("cs408_scene_value_invalid")
    if scene != source_kind:
        raise Cs408SemanticAdmissionError("cs408_scene_conflict")
    if not isinstance(envelope["evidence"], Mapping):
        raise Cs408SemanticAdmissionError("cs408_scene_evidence_invalid")
    if _nested_scene_keys(envelope["evidence"], depth=1):
        raise Cs408SemanticAdmissionError("cs408_scene_nested_forgery")
    return deepcopy(envelope)


def _pending_receipt(
    envelope: Mapping[str, Any], missing: Sequence[str]
) -> dict[str, Any]:
    receipt = {
        "schema_version": "cs408-scene-admission-receipt-v1",
        "subject": "cs408",
        "capture_id": envelope["capture_id"],
        "scene": envelope["scene"],
        "status": "evidence_pending",
        "missing_fields": sorted(set(missing)),
        "admission_kind": None,
        "model_enqueue_allowed": False,
        "item_formalization_allowed": False,
        "private_artifact_bindings": [],
        "formal_write_count": 0,
    }
    return {**receipt, "receipt_sha256": sha256_value(receipt)}


def _morning_receipt(envelope: Mapping[str, Any]) -> dict[str, Any]:
    evidence = dict(envelope["evidence"])
    required = {
        "review_identity",
        "display_receipt_sha256",
        "display_receipt_ref",
        "first_answer",
        "confidence",
        "exposed_problem",
        "correction",
    }
    missing = [name for name in required if name not in evidence]
    if missing:
        return _pending_receipt(envelope, missing)
    if set(evidence) != required:
        raise Cs408SemanticAdmissionError("cs408_morning_evidence_shape_invalid")
    _safe_id(evidence["review_identity"], "cs408_review_identity_invalid")
    display_sha = _hash(
        evidence["display_receipt_sha256"],
        "cs408_display_receipt_hash_invalid",
    )
    _ref(
        evidence["display_receipt_ref"],
        "study-intake-display-receipt://sha256/",
        "cs408_display_receipt_ref_invalid",
    )
    if evidence["display_receipt_ref"] != (
        "study-intake-display-receipt://sha256/" + display_sha
    ):
        raise Cs408SemanticAdmissionError("cs408_display_receipt_ref_invalid")
    first = _exact_mapping(
        evidence["first_answer"],
        {"answer_sha256", "observed_at", "outcome", "no_prompt", "reasoning_break"},
        "cs408_first_answer_shape_invalid",
    )
    _hash(first["answer_sha256"], "cs408_first_answer_hash_invalid")
    if (
        not isinstance(first["observed_at"], str)
        or not first["observed_at"]
        or first["outcome"]
        not in {"independent_correct", "fragile_correct", "wrong", "partial", "blank", "uncertain"}
        or not isinstance(first["no_prompt"], bool)
        or not isinstance(first["reasoning_break"], bool)
    ):
        raise Cs408SemanticAdmissionError("cs408_first_answer_contract_invalid")
    confidence = _exact_mapping(
        evidence["confidence"],
        {"level", "provenance"},
        "cs408_confidence_shape_invalid",
    )
    if confidence["level"] not in {"high", "medium", "low"} or not isinstance(
        confidence["provenance"], str
    ) or not confidence["provenance"]:
        raise Cs408SemanticAdmissionError("cs408_confidence_contract_invalid")
    if not isinstance(evidence["exposed_problem"], str) or not evidence[
        "exposed_problem"
    ]:
        raise Cs408SemanticAdmissionError("cs408_exposed_problem_invalid")
    correction = _exact_mapping(
        evidence["correction"],
        {"occurred", "trace_artifact_sha256", "trace_artifact_ref"},
        "cs408_correction_trace_shape_invalid",
    )
    if not isinstance(correction["occurred"], bool):
        raise Cs408SemanticAdmissionError("cs408_correction_trace_invalid")
    bindings = [display_sha]
    if correction["occurred"]:
        trace_sha = _hash(
            correction["trace_artifact_sha256"],
            "cs408_correction_trace_hash_invalid",
        )
        if correction["trace_artifact_ref"] != (
            "study-intake-correction-trace://sha256/" + trace_sha
        ):
            raise Cs408SemanticAdmissionError("cs408_correction_trace_ref_invalid")
        bindings.append(trace_sha)
    elif correction["trace_artifact_sha256"] is not None or correction[
        "trace_artifact_ref"
    ] is not None:
        raise Cs408SemanticAdmissionError("cs408_correction_trace_fabricated")
    independent = (
        first["outcome"] == "independent_correct"
        and confidence["level"] == "high"
        and first["no_prompt"] is True
        and first["reasoning_break"] is False
    )
    receipt = {
        "schema_version": "cs408-scene-admission-receipt-v1",
        "subject": "cs408",
        "capture_id": envelope["capture_id"],
        "scene": "morning_review",
        "status": "ready",
        "missing_fields": [],
        "admission_kind": "observation_only" if independent else "failure_capture",
        "model_enqueue_allowed": True,
        "item_formalization_allowed": not independent,
        "private_artifact_bindings": bindings,
        "formal_write_count": 0,
    }
    return {**receipt, "receipt_sha256": sha256_value(receipt)}


def _formal_receipt(envelope: Mapping[str, Any]) -> dict[str, Any]:
    evidence = dict(envelope["evidence"])
    required = {
        "stable_question_identity",
        "question_surface_sha256",
        "question_image",
        "solution_image",
        "actual_answer",
        "full_dialogue",
    }
    missing = [name for name in required if name not in evidence]
    if missing:
        return _pending_receipt(envelope, missing)
    if set(evidence) != required:
        raise Cs408SemanticAdmissionError("cs408_formal_evidence_shape_invalid")
    _safe_id(
        evidence["stable_question_identity"],
        "cs408_formal_identity_invalid",
    )
    surface_sha = _hash(
        evidence["question_surface_sha256"],
        "cs408_question_surface_hash_invalid",
    )
    bindings = [surface_sha]
    for role in ("question_image", "solution_image"):
        image = _exact_mapping(
            evidence[role],
            {"capture_id", "sha256", "ref", "detected_mime_type"},
            "cs408_formal_image_shape_invalid",
        )
        if image["capture_id"] != envelope["capture_id"]:
            raise Cs408SemanticAdmissionError("cs408_formal_cross_capture")
        digest = _hash(image["sha256"], "cs408_formal_image_hash_invalid")
        if image["ref"] != "study-intake-private-image://sha256/" + digest:
            raise Cs408SemanticAdmissionError("cs408_formal_image_ref_invalid")
        if image["detected_mime_type"] not in {"image/png", "image/jpeg", "image/webp"}:
            raise Cs408SemanticAdmissionError("cs408_formal_image_mime_invalid")
        bindings.append(digest)
    answer = _exact_mapping(
        evidence["actual_answer"],
        {"answer_sha256", "observed_at"},
        "cs408_actual_answer_shape_invalid",
    )
    bindings.append(
        _hash(answer["answer_sha256"], "cs408_actual_answer_hash_invalid")
    )
    if not isinstance(answer["observed_at"], str) or not answer["observed_at"]:
        raise Cs408SemanticAdmissionError("cs408_actual_answer_time_invalid")
    dialogue = _exact_mapping(
        evidence["full_dialogue"],
        {"artifact_sha256", "artifact_ref", "chunk_manifest_sha256", "chunks"},
        "cs408_dialogue_shape_invalid",
    )
    artifact_sha = _hash(
        dialogue["artifact_sha256"], "cs408_dialogue_artifact_hash_invalid"
    )
    if dialogue["artifact_ref"] != (
        "study-intake-private-dialogue://sha256/" + artifact_sha
    ):
        raise Cs408SemanticAdmissionError("cs408_dialogue_artifact_ref_invalid")
    chunks = dialogue["chunks"]
    if not isinstance(chunks, list) or not chunks:
        return _pending_receipt(envelope, ["full_dialogue.chunks"])
    expected_ordinals = list(range(len(chunks)))
    actual_ordinals: list[int] = []
    manifest_rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for chunk in chunks:
        row = _exact_mapping(
            chunk,
            {"ordinal", "capture_id", "sha256", "ref", "byte_count"},
            "cs408_dialogue_chunk_shape_invalid",
        )
        if row["capture_id"] != envelope["capture_id"]:
            raise Cs408SemanticAdmissionError("cs408_formal_cross_capture")
        if isinstance(row["ordinal"], bool) or not isinstance(row["ordinal"], int):
            raise Cs408SemanticAdmissionError("cs408_dialogue_chunk_order_invalid")
        digest = _hash(row["sha256"], "cs408_dialogue_chunk_hash_invalid")
        if digest in seen_hashes:
            raise Cs408SemanticAdmissionError("cs408_dialogue_chunk_duplicate")
        seen_hashes.add(digest)
        if row["ref"] != "study-intake-dialogue-chunk://sha256/" + digest:
            raise Cs408SemanticAdmissionError("cs408_dialogue_chunk_ref_invalid")
        if isinstance(row["byte_count"], bool) or not isinstance(
            row["byte_count"], int
        ) or row["byte_count"] < 1:
            raise Cs408SemanticAdmissionError("cs408_dialogue_chunk_size_invalid")
        actual_ordinals.append(row["ordinal"])
        manifest_rows.append(
            {
                "ordinal": row["ordinal"],
                "sha256": digest,
                "byte_count": row["byte_count"],
            }
        )
        bindings.append(digest)
    if actual_ordinals != expected_ordinals:
        raise Cs408SemanticAdmissionError("cs408_dialogue_chunk_order_invalid")
    expected_manifest = sha256_value(manifest_rows)
    if dialogue["chunk_manifest_sha256"] != expected_manifest:
        raise Cs408SemanticAdmissionError("cs408_dialogue_digest_drift")
    bindings.append(artifact_sha)
    receipt = {
        "schema_version": "cs408-scene-admission-receipt-v1",
        "subject": "cs408",
        "capture_id": envelope["capture_id"],
        "scene": "formal_problem",
        "status": "ready",
        "missing_fields": [],
        "admission_kind": "failure_capture",
        "model_enqueue_allowed": True,
        "item_formalization_allowed": True,
        "private_artifact_bindings": bindings,
        "formal_write_count": 0,
    }
    return {**receipt, "receipt_sha256": sha256_value(receipt)}


def build_scene_admission_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    envelope = validate_scene_envelope(value)
    return (
        _morning_receipt(envelope)
        if envelope["scene"] == "morning_review"
        else _formal_receipt(envelope)
    )


def _reject_formal_write_surface(value: Any) -> None:
    forbidden = {
        "formal_write",
        "formal_write_authorized",
        "apply",
        "write",
        "delete",
        "create_node",
        "assign_formal_id",
    }
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key in forbidden:
                raise Cs408SemanticAdmissionError("cs408_typed_formal_write_forbidden")
            _reject_formal_write_surface(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_formal_write_surface(nested)


def _payload_for_operation(
    operation: str,
    payload: Any,
    *,
    capture_id: str,
) -> dict[str, Any]:
    specs = {
        "mark_existing_item": {"formal_id", "identity_match"},
        "propose_new_item": {"proposal_local_id", "identity_status"},
        "mark_existing_knowledge": {"formal_knowledge_id", "identity_match"},
        "propose_new_knowledge": {
            "proposal_local_id",
            "label",
            "parent_status",
        },
        "propose_relation": {
            "current_capture_endpoint",
            "formal_node_endpoint",
            "relation_type",
            "strength",
            "reason",
            "existing_edge",
        },
        "needs_review": {"reason_code"},
    }
    row = _exact_mapping(
        payload, specs[operation], "cs408_typed_operation_payload_invalid"
    )
    if operation in {"mark_existing_item", "mark_existing_knowledge"}:
        id_key = "formal_id" if operation == "mark_existing_item" else "formal_knowledge_id"
        _safe_id(row[id_key], "cs408_typed_formal_id_invalid")
        if row["identity_match"] != "exact":
            raise Cs408SemanticAdmissionError("cs408_typed_identity_not_exact")
    elif operation == "propose_new_item":
        _safe_id(row["proposal_local_id"], "cs408_typed_local_id_invalid")
        if row["identity_status"] != "no_reliable_match":
            raise Cs408SemanticAdmissionError("cs408_typed_new_item_identity_invalid")
    elif operation == "propose_new_knowledge":
        _safe_id(row["proposal_local_id"], "cs408_typed_local_id_invalid")
        if not isinstance(row["label"], str) or not row["label"]:
            raise Cs408SemanticAdmissionError("cs408_typed_knowledge_label_invalid")
        if row["parent_status"] not in {"exact", "ambiguous", "missing"}:
            raise Cs408SemanticAdmissionError("cs408_typed_parent_status_invalid")
    elif operation == "propose_relation":
        if row["current_capture_endpoint"] != "capture:" + capture_id:
            raise Cs408SemanticAdmissionError("cs408_typed_relation_endpoint_invalid")
        if not isinstance(row["formal_node_endpoint"], str) or not row[
            "formal_node_endpoint"
        ].startswith("formal:"):
            raise Cs408SemanticAdmissionError("cs408_typed_relation_endpoint_invalid")
        _safe_id(
            row["formal_node_endpoint"][len("formal:") :],
            "cs408_typed_relation_endpoint_invalid",
        )
        if (
            row["relation_type"] not in {"R01", "R02", "R03", "R04", "R05", "R06", "R07", "R08", "R09"}
            or row["strength"] not in {"strong", "medium", "weak"}
            or not isinstance(row["reason"], str)
            or not row["reason"]
            or row["existing_edge"] is not False
        ):
            raise Cs408SemanticAdmissionError("cs408_typed_relation_payload_invalid")
    elif operation == "needs_review":
        if row["reason_code"] not in {
            "ambiguous_identity",
            "identity_conflict",
            "weak_similarity",
            "knowledge_alias_conflict",
            "parent_ambiguity",
            "child_ambiguity",
            "relation_endpoint_insufficient",
        }:
            raise Cs408SemanticAdmissionError("cs408_typed_needs_review_reason_invalid")
    return row


def validate_typed_operations(value: Mapping[str, Any]) -> dict[str, Any]:
    document = _exact_mapping(
        value,
        {
            "schema_version",
            "subject",
            "capture_id",
            "verdict",
            "analysis_output_sha256",
            "critical_review_output_sha256",
            "stage_allowed_evidence_refs",
            "operations",
            "formal_write_count",
        },
        "cs408_typed_operations_shape_invalid",
    )
    if document["schema_version"] != "cs408-typed-operations-v1":
        raise Cs408SemanticAdmissionError("cs408_typed_operations_schema_invalid")
    if document["subject"] != "cs408":
        raise Cs408SemanticAdmissionError("cs408_typed_operations_subject_invalid")
    capture_id = _safe_id(
        document["capture_id"], "cs408_typed_operations_capture_invalid"
    )
    if document["verdict"] not in {"pass", "pass_with_warnings", "reject"}:
        raise Cs408SemanticAdmissionError("cs408_typed_operations_verdict_invalid")
    _hash(document["analysis_output_sha256"], "cs408_typed_analysis_hash_invalid")
    _hash(
        document["critical_review_output_sha256"],
        "cs408_typed_review_hash_invalid",
    )
    allowed = _exact_mapping(
        document["stage_allowed_evidence_refs"],
        {"analysis", "critical_review"},
        "cs408_typed_stage_refs_shape_invalid",
    )
    for stage in ("analysis", "critical_review"):
        refs = allowed[stage]
        if (
            not isinstance(refs, list)
            or not refs
            or len(refs) != len(set(refs))
            or any(not isinstance(ref, str) or MCP_REF_RE.fullmatch(ref) is None for ref in refs)
        ):
            raise Cs408SemanticAdmissionError("cs408_typed_stage_refs_invalid")
    operations = document["operations"]
    if not isinstance(operations, list):
        raise Cs408SemanticAdmissionError("cs408_typed_operations_invalid")
    if document["verdict"] == "reject" and operations:
        raise Cs408SemanticAdmissionError("cs408_reject_typed_operations_forbidden")
    if document["verdict"] != "reject" and not operations:
        raise Cs408SemanticAdmissionError("cs408_typed_operations_missing")
    if document["formal_write_count"] != 0 or isinstance(
        document["formal_write_count"], bool
    ):
        raise Cs408SemanticAdmissionError("cs408_typed_formal_write_forbidden")
    _reject_formal_write_surface(document)
    operation_ids: set[str] = set()
    operation_hashes: set[str] = set()
    for raw in operations:
        row = _exact_mapping(
            raw,
            {
                "operation_id",
                "operation",
                "stage",
                "target",
                "payload",
                "evidence_refs",
                "counterevidence",
                "sol_verification_action",
                "formal_write_count",
            },
            "cs408_typed_operation_shape_invalid",
        )
        operation_id = _safe_id(
            row["operation_id"], "cs408_typed_operation_id_invalid"
        )
        if operation_id in operation_ids:
            raise Cs408SemanticAdmissionError("cs408_typed_operation_duplicate")
        operation_ids.add(operation_id)
        operation = row["operation"]
        if operation not in TYPED_OPERATIONS:
            raise Cs408SemanticAdmissionError("cs408_typed_operation_unknown")
        if row["stage"] not in {"analysis", "critical_review"}:
            raise Cs408SemanticAdmissionError("cs408_typed_operation_stage_invalid")
        refs = row["evidence_refs"]
        if (
            not isinstance(refs, list)
            or not refs
            or len(refs) != len(set(refs))
            or any(ref not in allowed[row["stage"]] for ref in refs)
        ):
            raise Cs408SemanticAdmissionError("cs408_typed_operation_refs_invalid")
        if (
            not isinstance(row["target"], str)
            or not row["target"]
            or not isinstance(row["counterevidence"], list)
            or not isinstance(row["sol_verification_action"], str)
            or not row["sol_verification_action"]
            or row["formal_write_count"] != 0
            or isinstance(row["formal_write_count"], bool)
        ):
            raise Cs408SemanticAdmissionError("cs408_typed_operation_contract_invalid")
        payload = _payload_for_operation(
            operation, row["payload"], capture_id=capture_id
        )
        if operation in {"mark_existing_item", "propose_new_item", "needs_review"}:
            if row["target"] != "capture:" + capture_id:
                raise Cs408SemanticAdmissionError("cs408_typed_operation_target_invalid")
        elif operation in {"mark_existing_knowledge", "propose_new_knowledge"}:
            if not row["target"].startswith("knowledge:"):
                raise Cs408SemanticAdmissionError("cs408_typed_operation_target_invalid")
        elif operation == "propose_relation" and not row["target"].startswith(
            "relation:"
        ):
            raise Cs408SemanticAdmissionError("cs408_typed_operation_target_invalid")
        validated_row = {**row, "payload": payload}
        operation_hash = sha256_value(validated_row)
        if operation_hash in operation_hashes:
            raise Cs408SemanticAdmissionError("cs408_typed_operation_duplicate")
        operation_hashes.add(operation_hash)
    return deepcopy(document)


def build_cs408_luna_proposal_v3(
    *,
    typed_operations: Mapping[str, Any],
    scene_admission_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    operations = validate_typed_operations(typed_operations)
    receipt = _exact_mapping(
        scene_admission_receipt,
        {
            "schema_version",
            "subject",
            "capture_id",
            "scene",
            "status",
            "missing_fields",
            "admission_kind",
            "model_enqueue_allowed",
            "item_formalization_allowed",
            "private_artifact_bindings",
            "formal_write_count",
            "receipt_sha256",
        },
        "cs408_scene_receipt_shape_invalid",
    )
    unsigned = dict(receipt)
    receipt_sha = unsigned.pop("receipt_sha256")
    if receipt_sha != sha256_value(unsigned) or receipt["status"] != "ready":
        raise Cs408SemanticAdmissionError("cs408_scene_receipt_binding_invalid")
    if receipt["capture_id"] != operations["capture_id"]:
        raise Cs408SemanticAdmissionError("cs408_proposal_capture_mismatch")
    if operations["verdict"] == "reject":
        raise Cs408SemanticAdmissionError("cs408_reject_package_forbidden")
    proposal = {
        "schema_version": "cs408-luna-proposal-v3",
        "subject": "cs408",
        "capture_id": operations["capture_id"],
        "scene_admission_receipt_sha256": receipt_sha,
        "typed_operations_sha256": sha256_value(operations),
        "typed_operations": deepcopy(operations),
        "review_status": "proposal_ready",
        "consumer_contract_versions": ["cs408-sol-readonly-consumer-v2"],
        "sol_enabled": False,
        "formal_write_count": 0,
    }
    return {**proposal, "proposal_sha256": sha256_value(proposal)}


def reopen_cs408_sol_readonly_envelope(
    value: Mapping[str, Any], *, expected_capture_id: str
) -> dict[str, Any]:
    envelope = _exact_mapping(
        value,
        {
            "schema_version",
            "subject",
            "capture_id",
            "scene_admission_receipt_sha256",
            "typed_operations_sha256",
            "typed_operations",
            "review_status",
            "consumer_contract_versions",
            "sol_enabled",
            "formal_write_count",
            "proposal_sha256",
        },
        "cs408_consumer_envelope_shape_invalid",
    )
    unsigned = dict(envelope)
    proposal_sha = unsigned.pop("proposal_sha256")
    if proposal_sha != sha256_value(unsigned):
        raise Cs408SemanticAdmissionError("cs408_consumer_envelope_hash_invalid")
    if (
        envelope["schema_version"] != "cs408-luna-proposal-v3"
        or envelope["subject"] != "cs408"
        or envelope["capture_id"] != expected_capture_id
        or envelope["review_status"] != "proposal_ready"
        or envelope["consumer_contract_versions"]
        != ["cs408-sol-readonly-consumer-v2"]
        or envelope["sol_enabled"] is not False
        or envelope["formal_write_count"] != 0
    ):
        raise Cs408SemanticAdmissionError("cs408_consumer_envelope_contract_invalid")
    typed = validate_typed_operations(envelope["typed_operations"])
    if envelope["typed_operations_sha256"] != sha256_value(typed):
        raise Cs408SemanticAdmissionError("cs408_consumer_typed_hash_invalid")
    decisions = [
        {
            "operation_id": row["operation_id"],
            "allowed_decisions": ["adopt", "modify", "reject", "needs_user"],
            "writer_invoked": False,
        }
        for row in typed["operations"]
    ]
    return {
        "schema_version": "cs408-sol-readonly-consumer-result-v2",
        "subject": "cs408",
        "capture_id": expected_capture_id,
        "status": "ready_for_independent_decision",
        "operation_decisions": decisions,
        "offer_token_created": False,
        "adoption_token_created": False,
        "sol_enabled": False,
        "formal_write_count": 0,
    }
