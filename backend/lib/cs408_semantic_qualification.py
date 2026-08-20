"""408-only semantic qualification receipt for Luna two-stage processing.

This proposal separates semantic analysis quality from delivery state.  It is
strictly read-only: it does not start a Provider or MCP server, mutate formal
sources, invoke Sol, normalize references, or infer formal writes.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Iterable, Mapping, Sequence


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MCP_REF_RE = re.compile(r"^mcp-item:cs408:[0-9a-f]{64}$")
ALLOWED_TOOLS = {
    "get_task_context",
    "read_task_artifact",
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
}
DELIVERY_STATUSES = {
    "accepted",
    "corrected",
    "rejected",
    "validator_failed",
    "blocked",
}
FORMAT_WARNING_CODES = {
    "latex_render",
    "markdown_render",
    "escape_render",
    "whitespace_render",
}


class Cs408SemanticQualificationError(ValueError):
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


def content_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _mapping(value: Any, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise Cs408SemanticQualificationError(code)
    return dict(value)


def _exact_mapping(
    value: Any,
    required: set[str],
    code: str,
) -> dict[str, Any]:
    result = _mapping(value, code)
    if set(result) != required:
        raise Cs408SemanticQualificationError(code)
    return result


def _sha(value: Any, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise Cs408SemanticQualificationError(code)
    return value


def _nonempty_string(value: Any, code: str) -> str:
    if not isinstance(value, str) or not value:
        raise Cs408SemanticQualificationError(code)
    return value


def _walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, Mapping):
        for nested in value.values():
            yield from _walk(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _walk(nested)


def _payload_mcp_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    for nested in _walk(value):
        if isinstance(nested, str) and nested.startswith("mcp-item:"):
            if MCP_REF_RE.fullmatch(nested) is None:
                raise Cs408SemanticQualificationError(
                    "cs408_semantic_mcp_ref_invalid"
                )
            refs.add(nested)
    return refs


def _transcript_evidence_refs(transcript: Mapping[str, Any]) -> set[str]:
    refs: set[str] = set()
    for call in transcript.get("calls", []):
        if not isinstance(call, Mapping):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_transcript_call_invalid"
            )
        result = call.get("result")
        if not isinstance(result, Mapping):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_transcript_result_invalid"
            )
        for nested in _walk(result.get("items", [])):
            if not isinstance(nested, Mapping) or "evidence_ref" not in nested:
                continue
            ref = nested["evidence_ref"]
            if not isinstance(ref, str) or MCP_REF_RE.fullmatch(ref) is None:
                raise Cs408SemanticQualificationError(
                    "cs408_semantic_transcript_ref_invalid"
                )
            refs.add(ref)
    return refs


def _analysis_paths(value: Any, prefix: str = "analysis") -> set[str]:
    paths = {prefix}
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str) or not key:
                continue
            paths.update(_analysis_paths(nested, f"{prefix}.{key}"))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            paths.update(_analysis_paths(nested, f"{prefix}[{index}]"))
    return paths


def _critical_analysis_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key == "analysis_refs":
                if not isinstance(nested, list) or not nested:
                    raise Cs408SemanticQualificationError(
                        "cs408_semantic_analysis_refs_invalid"
                    )
                for ref in nested:
                    if not isinstance(ref, str) or not ref.startswith("analysis."):
                        raise Cs408SemanticQualificationError(
                            "cs408_semantic_analysis_refs_invalid"
                        )
                    refs.add(ref)
            else:
                refs.update(_critical_analysis_refs(nested))
    elif isinstance(value, list):
        for nested in value:
            refs.update(_critical_analysis_refs(nested))
    return refs


def _stage_output(value: Any, stage_name: str) -> dict[str, Any]:
    stage = _mapping(value, "cs408_semantic_stage_output_invalid")
    if (
        stage.get("schema_version") != "study-intake-model-stage-output-v1"
        or stage.get("stage_name") != stage_name
        or stage.get("requested_model") != "gpt-5.6-luna"
        or stage.get("requested_reasoning_effort") != "max"
        or stage.get("formal_write_count") != 0
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_stage_binding_invalid"
        )
    payload = _mapping(
        stage.get("payload"),
        "cs408_semantic_stage_payload_invalid",
    )
    expected_payload_schema = {
        "cs408_analysis": "study-intake-luna-analysis-v2",
        "cs408_critical_review": "study-intake-luna-critical-review-v2",
    }[stage_name]
    if payload.get("schema_version") != expected_payload_schema:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_stage_payload_invalid"
        )
    return stage


def _validate_transcript(
    value: Any,
    stage_name: str,
) -> tuple[dict[str, Any], dict[str, bool]]:
    transcript = _mapping(value, "cs408_semantic_transcript_invalid")
    if (
        transcript.get("schema_version")
        != "model-driven-mcp-stage-transcript-v1"
        or transcript.get("subject") != "cs408"
        or transcript.get("stage_name") != stage_name
        or transcript.get("formal_write_count") != 0
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_transcript_binding_invalid"
        )
    calls = transcript.get("calls")
    if not isinstance(calls, list) or not calls:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_transcript_calls_invalid"
        )
    expected_sequence = list(range(1, len(calls) + 1))
    actual_sequence: list[int] = []
    context_artifacts: set[str] = set()
    read_artifacts: set[str] = set()
    task_context_seen = False
    task_artifact_content_complete = True
    knowledge_lookup = False
    item_identity_lookup = False
    relation_lookup = False
    capture_ids: set[str] = set()
    for call in calls:
        row = _mapping(call, "cs408_semantic_transcript_call_invalid")
        actual_sequence.append(row.get("sequence"))
        tool = row.get("tool")
        if tool not in ALLOWED_TOOLS:
            raise Cs408SemanticQualificationError(
                "cs408_semantic_tool_not_allowed"
            )
        if row.get("server") != "kaoyan_cs408_read":
            raise Cs408SemanticQualificationError(
                "cs408_semantic_namespace_invalid"
            )
        arguments = _mapping(
            row.get("arguments"),
            "cs408_semantic_transcript_arguments_invalid",
        )
        result = _mapping(
            row.get("result"),
            "cs408_semantic_transcript_result_invalid",
        )
        if (
            row.get("arguments_sha256") != content_sha256(arguments)
            or row.get("result_sha256") != content_sha256(result)
        ):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_transcript_content_hash_invalid"
            )
        if (
            result.get("subject") != "cs408"
            or result.get("formal_write_count") != 0
            or result.get("ok") is not True
        ):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_mcp_result_invalid"
            )
        items = result.get("items")
        if not isinstance(items, list):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_mcp_items_invalid"
            )
        if tool == "get_task_context":
            task_context_seen = True
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                capture_id = item.get("capture_id")
                if isinstance(capture_id, str):
                    capture_ids.add(capture_id)
                artifacts = item.get("artifacts", [])
                if isinstance(artifacts, list):
                    for artifact in artifacts:
                        if isinstance(artifact, Mapping) and isinstance(
                            artifact.get("artifact_id"), str
                        ):
                            context_artifacts.add(artifact["artifact_id"])
        if tool == "read_task_artifact":
            artifact_id = arguments.get("artifact_id")
            if isinstance(artifact_id, str):
                read_artifacts.add(artifact_id)
            for item in items:
                if not isinstance(item, Mapping):
                    task_artifact_content_complete = False
                    continue
                if item.get("content_complete") is not True:
                    task_artifact_content_complete = False
                capture_id = item.get("capture_id")
                if isinstance(capture_id, str):
                    capture_ids.add(capture_id)
        for nested in _walk(items):
            if not isinstance(nested, Mapping):
                continue
            role = nested.get("data_role")
            collection = nested.get("collection")
            if role == "formal_knowledge" or collection == "knowledge_nodes":
                knowledge_lookup = True
            if role == "formal_wrong_item" or collection == "formal_nodes":
                item_identity_lookup = True
            if (
                collection == "relations"
                or (isinstance(role, str) and "relation" in role)
            ):
                relation_lookup = True
    if actual_sequence != expected_sequence or calls[0].get("tool") != "get_task_context":
        raise Cs408SemanticQualificationError(
            "cs408_semantic_transcript_sequence_invalid"
        )
    coverage = _mapping(
        transcript.get("coverage"),
        "cs408_semantic_transcript_coverage_invalid",
    )
    pagination_complete = (
        coverage.get("all_returned_pages_consumed") is True
        and coverage.get("unresolved_next_cursors") == []
        and coverage.get("duplicate_argument_count") == 0
        and coverage.get("call_count") == len(calls)
    )
    return transcript, {
        "task_context_seen": task_context_seen,
        "task_artifacts_complete": bool(context_artifacts)
        and context_artifacts == read_artifacts
        and task_artifact_content_complete,
        "pagination_complete": pagination_complete,
        "knowledge_lookup_complete": knowledge_lookup,
        "item_identity_lookup_complete": item_identity_lookup,
        "relation_lookup_complete": relation_lookup,
        "single_capture_identity": len(capture_ids) == 1,
    }


def _fresh_context_proof(value: Any) -> dict[str, Any]:
    proof = _exact_mapping(
        value,
        {
            "schema_version",
            "each_stage_uses_ephemeral_exec",
            "checkpoint_resume_starts_execute_prompt",
            "provider_context_resume_token_absent",
            "proof_artifact_sha256",
        },
        "cs408_semantic_fresh_context_proof_invalid",
    )
    if proof["schema_version"] != "cs408-fresh-critical-context-proof-v1":
        raise Cs408SemanticQualificationError(
            "cs408_semantic_fresh_context_proof_invalid"
        )
    _sha(
        proof["proof_artifact_sha256"],
        "cs408_semantic_fresh_context_proof_invalid",
    )
    for field in (
        "each_stage_uses_ephemeral_exec",
        "checkpoint_resume_starts_execute_prompt",
        "provider_context_resume_token_absent",
    ):
        if not isinstance(proof[field], bool):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_fresh_context_proof_invalid"
            )
    return proof


def _format_warnings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_format_warnings_invalid"
        )
    result: list[dict[str, Any]] = []
    for warning in value:
        row = _exact_mapping(
            warning,
            {
                "json_path",
                "warning_code",
                "message",
                "raw_fragment_sha256",
                "semantic_sha256_before",
                "semantic_sha256_after",
                "machine_parseable",
                "structural_semantics_unchanged",
            },
            "cs408_semantic_format_warning_shape_invalid",
        )
        if (
            not isinstance(row["json_path"], str)
            or not row["json_path"].startswith("$.")
            or row["warning_code"] not in FORMAT_WARNING_CODES
            or not isinstance(row["message"], str)
            or not row["message"]
            or row["machine_parseable"] is not True
            or row["structural_semantics_unchanged"] is not True
        ):
            raise Cs408SemanticQualificationError(
                "cs408_semantic_format_warning_not_harmless"
            )
        _sha(
            row["raw_fragment_sha256"],
            "cs408_semantic_format_warning_hash_invalid",
        )
        before = _sha(
            row["semantic_sha256_before"],
            "cs408_semantic_format_warning_hash_invalid",
        )
        after = _sha(
            row["semantic_sha256_after"],
            "cs408_semantic_format_warning_hash_invalid",
        )
        if before != after:
            raise Cs408SemanticQualificationError(
                "cs408_semantic_format_warning_changes_semantics"
            )
        result.append(deepcopy(row))
    return result


def _semantic_sections_complete(payload: Mapping[str, Any]) -> bool:
    atomic = payload.get("atomic_signals")
    formalization = payload.get("formalization_candidates")
    network = payload.get("knowledge_network_context")
    diagnosis = payload.get("reasoning_diagnosis")
    assessment = payload.get("evidence_assessment")
    return (
        isinstance(atomic, list)
        and bool(atomic)
        and isinstance(formalization, Mapping)
        and bool(formalization)
        and isinstance(network, Mapping)
        and bool(network)
        and isinstance(diagnosis, Mapping)
        and bool(diagnosis)
        and isinstance(assessment, Mapping)
        and bool(assessment)
    )


def _identity_conflict_present(
    analysis_payload: Mapping[str, Any],
    critical_payload: Mapping[str, Any],
) -> bool:
    for nested in _walk((analysis_payload, critical_payload)):
        if not isinstance(nested, Mapping):
            continue
        if (
            nested.get("signal_id") == "sig-identity-conflict"
            or nested.get("canonical_term") == "当前题目身份绑定冲突"
            or str(nested.get("finding_id", "")).startswith("CR-IDENTITY-")
        ):
            return True
    return False


def build_cs408_semantic_qualification_receipt(
    *,
    capture_id: str,
    analysis_stage_output: Mapping[str, Any],
    critical_review_stage_output: Mapping[str, Any],
    analysis_transcript: Mapping[str, Any],
    critical_review_transcript: Mapping[str, Any],
    source_physical_sha256s: Mapping[str, str],
    fresh_context_proof: Mapping[str, Any],
    delivery_status: str,
    delivery_detail: str,
    format_warnings: Sequence[Mapping[str, Any]],
    blocking_delivery_reasons: Sequence[str],
    legacy_failure_code: str | None,
) -> dict[str, Any]:
    """Build a deterministic, content-bound 408-only qualification receipt."""

    _nonempty_string(capture_id, "cs408_semantic_capture_id_invalid")
    if delivery_status not in DELIVERY_STATUSES:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_delivery_status_invalid"
        )
    _nonempty_string(delivery_detail, "cs408_semantic_delivery_detail_invalid")
    if not isinstance(blocking_delivery_reasons, (list, tuple)) or any(
        not isinstance(reason, str) or not reason
        for reason in blocking_delivery_reasons
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_blocking_reasons_invalid"
        )
    if legacy_failure_code is not None and (
        not isinstance(legacy_failure_code, str) or not legacy_failure_code
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_legacy_failure_code_invalid"
        )
    analysis_stage = _stage_output(analysis_stage_output, "cs408_analysis")
    critical_stage = _stage_output(
        critical_review_stage_output,
        "cs408_critical_review",
    )
    analysis_payload = analysis_stage["payload"]
    critical_payload = critical_stage["payload"]
    analysis_log, analysis_checks = _validate_transcript(
        analysis_transcript,
        "cs408_analysis",
    )
    critical_log, critical_checks = _validate_transcript(
        critical_review_transcript,
        "cs408_critical_review",
    )
    proof = _fresh_context_proof(fresh_context_proof)
    warnings = _format_warnings(list(format_warnings))
    source_hashes = _exact_mapping(
        source_physical_sha256s,
        {
            "analysis_output",
            "critical_review_output",
            "analysis_transcript",
            "critical_review_transcript",
        },
        "cs408_semantic_source_hashes_invalid",
    )
    for source_hash in source_hashes.values():
        _sha(source_hash, "cs408_semantic_source_hashes_invalid")

    if (
        analysis_log.get("read_session_id")
        != critical_log.get("read_session_id")
        or analysis_log.get("generation") != critical_log.get("generation")
        or analysis_log.get("authority_fingerprint")
        != critical_log.get("authority_fingerprint")
        or analysis_log.get("read_session_manifest_sha256")
        != critical_log.get("read_session_manifest_sha256")
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_cross_stage_binding_invalid"
        )
    analysis_refs = _payload_mcp_refs(analysis_payload)
    critical_refs = _payload_mcp_refs(critical_payload)
    analysis_allowed_refs = _transcript_evidence_refs(analysis_log)
    critical_allowed_refs = _transcript_evidence_refs(critical_log)
    analysis_refs_exact = analysis_refs <= analysis_allowed_refs
    critical_refs_exact = critical_refs <= critical_allowed_refs
    if not analysis_refs_exact or not critical_refs_exact:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_stage_ref_membership_invalid"
        )
    allowed_analysis_paths = _analysis_paths(analysis_payload)
    critical_analysis_refs = _critical_analysis_refs(critical_payload)
    if not critical_analysis_refs <= allowed_analysis_paths:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_analysis_ref_membership_invalid"
        )
    capture_ids: set[str] = set()
    for transcript in (analysis_log, critical_log):
        for call in transcript["calls"]:
            if call["tool"] != "get_task_context":
                continue
            for item in call["result"]["items"]:
                if isinstance(item, Mapping) and isinstance(
                    item.get("capture_id"), str
                ):
                    capture_ids.add(item["capture_id"])
    if capture_ids != {capture_id}:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_capture_binding_invalid"
        )

    semantic_coverage = {
        "analysis_task_artifacts_complete": analysis_checks[
            "task_artifacts_complete"
        ],
        "critical_review_task_artifacts_complete": critical_checks[
            "task_artifacts_complete"
        ],
        "analysis_pagination_complete": analysis_checks[
            "pagination_complete"
        ],
        "critical_review_pagination_complete": critical_checks[
            "pagination_complete"
        ],
        "analysis_knowledge_lookup_complete": analysis_checks[
            "knowledge_lookup_complete"
        ],
        "critical_review_knowledge_lookup_complete": critical_checks[
            "knowledge_lookup_complete"
        ],
        "analysis_item_identity_lookup_complete": analysis_checks[
            "item_identity_lookup_complete"
        ],
        "critical_review_item_identity_lookup_complete": critical_checks[
            "item_identity_lookup_complete"
        ],
        "analysis_relation_lookup_complete": analysis_checks[
            "relation_lookup_complete"
        ],
        "critical_review_relation_lookup_complete": critical_checks[
            "relation_lookup_complete"
        ],
        "analysis_stage_refs_exact": analysis_refs_exact,
        "critical_review_stage_refs_exact": critical_refs_exact,
        "critical_review_analysis_refs_exact": True,
        "semantic_sections_complete": _semantic_sections_complete(
            analysis_payload
        ),
        "fresh_critical_context_verified": all(
            proof[field]
            for field in (
                "each_stage_uses_ephemeral_exec",
                "checkpoint_resume_starts_execute_prompt",
                "provider_context_resume_token_absent",
            )
        ),
    }
    qualified = all(semantic_coverage.values())
    semantic_analysis_status = "qualified" if qualified else "unqualified"
    verdict = critical_payload.get("verdict")
    if verdict not in {"pass", "pass_with_warnings", "reject"}:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_critical_verdict_invalid"
        )
    identity_conflict = _identity_conflict_present(
        analysis_payload,
        critical_payload,
    )
    if identity_conflict and verdict != "reject":
        raise Cs408SemanticQualificationError(
            "cs408_semantic_identity_conflict_not_rejected"
        )
    if verdict == "reject" and delivery_status in {"accepted", "corrected"}:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_reject_delivery_invalid"
        )
    if not qualified and delivery_status in {"accepted", "corrected"}:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_unqualified_delivery_invalid"
        )
    if delivery_status == "rejected" and verdict != "reject":
        raise Cs408SemanticQualificationError(
            "cs408_semantic_rejected_without_verdict"
        )
    executable_delivery_allowed = (
        qualified
        and delivery_status in {"accepted", "corrected"}
        and verdict in {"pass", "pass_with_warnings"}
    )
    intended_delivery_outcome = (
        "rejected"
        if verdict == "reject"
        else "corrected"
        if delivery_status == "corrected"
        else "accepted"
        if delivery_status == "accepted"
        else "blocked"
    )
    qualification_reasons = [
        "task_artifacts_reopened",
        "knowledge_and_item_identity_queried",
        "existing_relations_queried",
        "current_and_historical_error_boundaries_recorded",
        "stage_local_references_exact",
        "fresh_critical_review_context_verified",
    ] if qualified else [
        "semantic_coverage_incomplete",
    ]
    receipt = {
        "schema_version": "cs408-luna-semantic-qualification-v1",
        "subject": "cs408",
        "capture_id": capture_id,
        "analysis_output_sha256": source_hashes["analysis_output"],
        "critical_review_output_sha256": source_hashes[
            "critical_review_output"
        ],
        "analysis_transcript_sha256": source_hashes["analysis_transcript"],
        "critical_review_transcript_sha256": source_hashes[
            "critical_review_transcript"
        ],
        "analysis_output_canonical_sha256": sha256_value(analysis_stage),
        "critical_review_output_canonical_sha256": sha256_value(
            critical_stage
        ),
        "analysis_transcript_canonical_sha256": sha256_value(analysis_log),
        "critical_review_transcript_canonical_sha256": sha256_value(
            critical_log
        ),
        "read_session_id": analysis_log["read_session_id"],
        "read_session_manifest_sha256": analysis_log[
            "read_session_manifest_sha256"
        ],
        "authority_generation": analysis_log["generation"],
        "authority_fingerprint": analysis_log["authority_fingerprint"],
        "fresh_context_proof_sha256": proof["proof_artifact_sha256"],
        "semantic_analysis_status": semantic_analysis_status,
        "delivery_status": delivery_status,
        "delivery_detail": delivery_detail,
        "critical_review_verdict": verdict,
        "intended_delivery_outcome": intended_delivery_outcome,
        "identity_conflict_present": identity_conflict,
        "executable_delivery_allowed": executable_delivery_allowed,
        "semantic_coverage": semantic_coverage,
        "format_warnings": warnings,
        "qualification_reasons": qualification_reasons,
        "blocking_delivery_reasons": list(blocking_delivery_reasons),
        "legacy_failure_code": legacy_failure_code,
        "sol_disabled": True,
        "formal_write_count": 0,
    }
    return {**receipt, "receipt_sha256": sha256_value(receipt)}


def reopen_cs408_semantic_qualification_receipt(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = _mapping(value, "cs408_semantic_receipt_invalid")
    required = {
        "schema_version",
        "subject",
        "capture_id",
        "analysis_output_sha256",
        "critical_review_output_sha256",
        "analysis_transcript_sha256",
        "critical_review_transcript_sha256",
        "analysis_output_canonical_sha256",
        "critical_review_output_canonical_sha256",
        "analysis_transcript_canonical_sha256",
        "critical_review_transcript_canonical_sha256",
        "read_session_id",
        "read_session_manifest_sha256",
        "authority_generation",
        "authority_fingerprint",
        "fresh_context_proof_sha256",
        "semantic_analysis_status",
        "delivery_status",
        "delivery_detail",
        "critical_review_verdict",
        "intended_delivery_outcome",
        "identity_conflict_present",
        "executable_delivery_allowed",
        "semantic_coverage",
        "format_warnings",
        "qualification_reasons",
        "blocking_delivery_reasons",
        "legacy_failure_code",
        "sol_disabled",
        "formal_write_count",
        "receipt_sha256",
    }
    if set(receipt) != required:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_receipt_shape_invalid"
        )
    provided = _sha(
        receipt.pop("receipt_sha256"),
        "cs408_semantic_receipt_hash_invalid",
    )
    if (
        receipt.get("schema_version")
        != "cs408-luna-semantic-qualification-v1"
        or receipt.get("subject") != "cs408"
        or receipt.get("sol_disabled") is not True
        or receipt.get("formal_write_count") != 0
        or sha256_value(receipt) != provided
    ):
        raise Cs408SemanticQualificationError(
            "cs408_semantic_receipt_hash_invalid"
        )
    for field in (
        "analysis_output_sha256",
        "critical_review_output_sha256",
        "analysis_transcript_sha256",
        "critical_review_transcript_sha256",
        "analysis_output_canonical_sha256",
        "critical_review_output_canonical_sha256",
        "analysis_transcript_canonical_sha256",
        "critical_review_transcript_canonical_sha256",
        "read_session_manifest_sha256",
        "authority_fingerprint",
        "fresh_context_proof_sha256",
    ):
        _sha(receipt.get(field), "cs408_semantic_receipt_binding_invalid")
    if receipt.get("semantic_analysis_status") not in {
        "qualified",
        "unqualified",
    } or receipt.get("delivery_status") not in DELIVERY_STATUSES:
        raise Cs408SemanticQualificationError(
            "cs408_semantic_receipt_state_invalid"
        )
    _format_warnings(receipt.get("format_warnings"))
    return deepcopy({**receipt, "receipt_sha256": provided})
