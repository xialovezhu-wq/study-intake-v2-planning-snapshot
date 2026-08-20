#!/usr/bin/env python3
"""Fail-closed, candidate-bound semantic postconditions for daily Golden replay.

The validator never calls a model and never treats the replay prompt or a
model-authored ``passed`` flag as evidence.  It reopens the immutable package,
proposal, external subject-quality receipt, both stage outputs, both MCP
transcripts, and the subject report before evaluating business postconditions.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest as validate_math_business_manifest,
)
from math_live_golden_assertions import (  # noqa: E402
    MathLiveGoldenAssertionError,
    validate_result as validate_live_math_result,
)
from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    validate_luna_proposal_v2,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    validate_subject_quality_receipt_v1,
)


EVIDENCE_SCHEMA = "candidate-golden-semantic-evidence-v1"
REPORT_SCHEMA = "candidate-golden-semantic-validation-v1"
CONTROLLED_SPEC_SCHEMA = "study-intake-controlled-replay-spec-v1"
PURPOSE = REPORT_SCHEMA
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FORMAL_ID_RE = re.compile(r"^(?:GS|LA|PR)-[A-Za-z0-9._:-]+$")
LIVE_MATH_ROLES = {
    "LUNA-MATH-001": "LUNA-MATH-20260809-001",
    "LUNA-MATH-002": "LUNA-MATH-20260809-002",
    "LUNA-MATH-003": "LUNA-MATH-20260809-003",
}
MATH_ROLES = {
    "GS-111",
    "GS-240",
    "complete-new-intake",
    *LIVE_MATH_ROLES,
}
CS408_ROLES = {
    "DS_2023_002",
    "FILE_PROTECTION",
    "OS_2009_003",
    "FREE_SPACE",
}
ENGLISH_SOURCE_ROLES = {"english-q37-a", "english-q37-c", "english-q37-d"}
TASK_ROLES = MATH_ROLES | CS408_ROLES | {"english-daily-microbatch"}
ALLOWED_OPERATIONS = {
    "mark_existing_item",
    "mark_existing_knowledge",
    "mark_status",
    "propose_item_update",
    "propose_new_item",
    "propose_new_knowledge",
    "propose_relation",
    "needs_review",
    "existing_knowledge_marker",
    "wrong_item_update_proposal",
    "new_wrong_item_proposal",
    "new_knowledge_proposal",
    "relation_proposal",
    "review_exclusion_proposal",
    "reactivation_proposal",
}


class GoldenSemanticError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _fail(code: str) -> None:
    raise GoldenSemanticError(code)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise GoldenSemanticError("golden_semantic_json_invalid") from exc


def _value_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path, code: str, *, max_bytes: int = 32 * 1024 * 1024) -> dict[str, Any]:
    try:
        path = path.expanduser().resolve(strict=True)
        stat = path.lstat()
        if path.is_symlink() or not path.is_file() or not 0 < stat.st_size <= max_bytes:
            raise OSError("unsafe artifact")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GoldenSemanticError(code) from exc
    if not isinstance(value, dict):
        _fail(code)
    return value


def _artifact_ref(value: Any, code: str) -> tuple[Path, str, dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256"}:
        _fail(code)
    raw_path = value.get("path")
    digest = value.get("sha256")
    if (
        not isinstance(raw_path, str)
        or not Path(raw_path).expanduser().is_absolute()
        or not isinstance(digest, str)
        or SHA256_RE.fullmatch(digest) is None
    ):
        _fail(code)
    path = Path(raw_path).expanduser().resolve()
    document = _load_json(path, code)
    if _file_sha256(path) != digest:
        _fail(code)
    return path, digest, document


def _all_values(value: Any, key: str) -> list[Any]:
    rows: list[Any] = []
    if isinstance(value, Mapping):
        for name, nested in value.items():
            if name == key:
                rows.append(nested)
            rows.extend(_all_values(nested, key))
    elif isinstance(value, list):
        for nested in value:
            rows.extend(_all_values(nested, key))
    return rows


def _assert_zero_writes(value: Any) -> None:
    if any(item != 0 for item in _all_values(value, "formal_write_count")):
        _fail("golden_semantic_formal_write_detected")


def _assert_no_legacy_english(value: Any) -> None:
    serialized = _canonical(value).decode("utf-8").casefold()
    if "en-p0-006" in serialized or "english-legacy" in serialized:
        _fail("golden_semantic_english_legacy_workload_forbidden")


def _seal(key: bytes, purpose: str, payload: Mapping[str, Any]) -> str:
    material = {"purpose": purpose, "payload": copy.deepcopy(dict(payload))}
    return hmac.new(key, _canonical(material), hashlib.sha256).hexdigest()


def _verify_quality_hmac(receipt: Mapping[str, Any], key: bytes) -> None:
    try:
        checked = validate_subject_quality_receipt_v1(receipt)
    except SubjectSolContractError as exc:
        raise GoldenSemanticError("golden_semantic_quality_receipt_invalid") from exc
    seal = checked.get("seal")
    if not isinstance(seal, Mapping):
        _fail("golden_semantic_quality_receipt_invalid")
    core = dict(checked)
    core.pop("seal", None)
    expected = _seal(key, "subject-quality-receipt", core)
    if not hmac.compare_digest(str(seal.get("hmac_sha256") or ""), expected):
        _fail("golden_semantic_quality_hmac_invalid")


def _load_authority_key(path: Path, expected_sha256: object) -> bytes:
    try:
        resolved = path.expanduser().resolve(strict=True)
        stat = resolved.lstat()
        raw = resolved.read_bytes()
    except OSError as exc:
        raise GoldenSemanticError("golden_semantic_authority_key_invalid") from exc
    if (
        resolved.is_symlink()
        or not resolved.is_file()
        or stat.st_mode & 0o077
        or len(raw) < 32
        or not isinstance(expected_sha256, str)
        or hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        _fail("golden_semantic_authority_key_invalid")
    return raw


def _spec_rows(ref: Any, subject: str, release_id: str) -> tuple[list[dict[str, Any]], str]:
    _path, digest, spec = _artifact_ref(ref, f"golden_semantic_{subject}_spec_invalid")
    entries = spec.get("entries")
    if (
        spec.get("schema_version") != CONTROLLED_SPEC_SCHEMA
        or spec.get("release_id") != release_id
        or spec.get("formal_write_count") != 0
        or not isinstance(entries, list)
        or not entries
        or any(not isinstance(row, Mapping) or row.get("subject") != subject for row in entries)
    ):
        _fail(f"golden_semantic_{subject}_spec_invalid")
    rows = [copy.deepcopy(dict(row)) for row in entries]
    if any(
        not isinstance(row.get("capture_id"), str)
        or not isinstance(row.get("sample_role"), str)
        or not isinstance(row.get("expected_input_fingerprint"), str)
        or SHA256_RE.fullmatch(str(row["expected_input_fingerprint"])) is None
        for row in rows
    ):
        _fail(f"golden_semantic_{subject}_spec_unbound")
    return rows, digest


def _operations(proposal: Mapping[str, Any]) -> set[str]:
    try:
        validate_luna_proposal_v2(proposal)
    except PreprocessorError as exc:
        raise GoldenSemanticError("golden_semantic_proposal_contract_invalid") from exc
    raw = proposal.get("operations")
    if not isinstance(raw, list) or not raw or any(not isinstance(row, Mapping) for row in raw):
        _fail("golden_semantic_proposal_operations_invalid")
    operations = {str(row.get("operation") or row.get("proposal_type") or "") for row in raw}
    if not operations or not operations <= ALLOWED_OPERATIONS:
        _fail("golden_semantic_proposal_operation_forbidden")
    for row in raw:
        operation = str(row.get("operation") or row.get("proposal_type") or "")
        if "relation" in operation and row.get("formal_write_count", 0) != 0:
            _fail("golden_semantic_relation_not_proposal_only")
    return operations


def _processing_binding(
    package: Mapping[str, Any], *, subject: str, release_id: str
) -> Mapping[str, Any]:
    binding = package.get("processing_binding")
    required = {
        "schema_version", "base_release_id", "candidate_release_id", "plugin",
        "skill", "mcp", "schemas", "validator_sha256",
        "host_semantic_prefetch", "binding_sha256", "formal_write_count",
    }
    if not isinstance(binding, Mapping) or set(binding) != required:
        _fail("golden_semantic_processing_binding_invalid")
    core = {key: copy.deepcopy(value) for key, value in binding.items() if key != "binding_sha256"}
    expected_skill = {
        "math": "background-math-processing",
        "cs408": "background-cs408-processing",
        "english": "background-english-processing",
    }[subject]
    expected_mcp = {
        "math": "kaoyan_math_read",
        "cs408": "kaoyan_cs408_read",
        "english": "kaoyan_english_read",
    }[subject]
    plugin = binding.get("plugin")
    skill = binding.get("skill")
    mcp = binding.get("mcp")
    schemas = binding.get("schemas")
    components = (plugin, skill)
    if (
        binding.get("schema_version") != "processing_binding_v2"
        or binding.get("candidate_release_id") != release_id
        or not isinstance(binding.get("base_release_id"), str)
        or SHA256_RE.fullmatch(str(binding.get("base_release_id"))) is None
        or binding.get("host_semantic_prefetch") is not False
        or binding.get("formal_write_count") != 0
        or binding.get("binding_sha256") != _value_sha256(core)
        or package.get("processing_binding_sha256") != binding.get("binding_sha256")
        or any(
            not isinstance(component, Mapping)
            or set(component) != {"id", "version", "sha256"}
            or not isinstance(component.get("version"), str)
            or not component["version"]
            or not isinstance(component.get("sha256"), str)
            or SHA256_RE.fullmatch(str(component["sha256"])) is None
            for component in components
        )
        or plugin.get("id") != "kaoyan-study-intake"
        or skill.get("id") != expected_skill
        or not isinstance(mcp, Mapping)
        or set(mcp) != {"id", "version", "sha256", "profile", "subject"}
        or mcp.get("id") != expected_mcp
        or mcp.get("subject") != subject
        or mcp.get("profile") != "luna"
        or not isinstance(mcp.get("version"), str)
        or not isinstance(mcp.get("sha256"), str)
        or SHA256_RE.fullmatch(str(mcp["sha256"])) is None
        or not str(mcp["version"]).endswith(str(mcp["sha256"]))
        or not isinstance(schemas, Mapping)
        or set(schemas) != {"provider_sha256", "canonical_sha256"}
        or any(
            not isinstance(value, str) or SHA256_RE.fullmatch(value) is None
            for value in schemas.values()
        )
        or not isinstance(binding.get("validator_sha256"), str)
        or SHA256_RE.fullmatch(str(binding["validator_sha256"])) is None
    ):
        _fail("golden_semantic_processing_binding_invalid")
    return binding


def _transcript(
    value: Mapping[str, Any], *, subject: str, stage: str, release_id: str,
    generation: str, authority: str,
) -> None:
    expected_stage_name = f"{subject}_{stage}"
    if (
        value.get("schema_version") != "model-driven-mcp-stage-transcript-v1"
        or value.get("subject") != subject
        or value.get("stage_name") != expected_stage_name
        or value.get("generation") != generation
        or value.get("authority_fingerprint") != authority
        or value.get("model_call_count") != 1
        or value.get("formal_write_count") != 0
    ):
        _fail("golden_semantic_mcp_transcript_invalid")
    _assert_zero_writes(value)
    duplicates = [
        *_all_values(value, "consumed_terminal_duplicate_read_count"),
        *_all_values(value, "consumed_duplicate_read_count"),
    ]
    if not duplicates or any(item != 0 for item in duplicates):
        _fail("golden_semantic_terminal_duplicate_read")
    release_values = _all_values(value, "candidate_release_id")
    if not release_values or any(item != release_id for item in release_values):
        _fail("golden_semantic_transcript_release_mismatch")
    if any(item != generation for item in _all_values(value, "generation")):
        _fail("golden_semantic_transcript_generation_mismatch")
    if any(item != authority for item in _all_values(value, "authority_fingerprint")):
        _fail("golden_semantic_transcript_authority_mismatch")
    calls = value.get("calls")
    if (
        not isinstance(calls, list)
        or not calls
        or value.get("mcp_tool_call_count") != len(calls)
        or value.get("provider_request_count") != 1
    ):
        _fail("golden_semantic_mcp_call_closure_invalid")
    tools: set[str] = set()
    for index, call in enumerate(calls, start=1):
        if not isinstance(call, Mapping):
            _fail("golden_semantic_mcp_call_closure_invalid")
        tool = call.get("tool")
        arguments = call.get("arguments")
        result = call.get("result")
        if (
            call.get("sequence") != index
            or not isinstance(tool, str)
            or not tool
            or not isinstance(arguments, Mapping)
            or not isinstance(result, Mapping)
            or call.get("arguments_sha256") != _value_sha256(arguments)
            or call.get("result_sha256") != _value_sha256(result)
            or result.get("formal_write_count") != 0
            or result.get("model_call_count") not in {0, None}
            or result.get("complete") is False
            or result.get("truncated") is True
        ):
            _fail("golden_semantic_mcp_call_closure_invalid")
        tools.add(tool)
        routes = _all_values(result, "read_route")
        if any(route != "mcp_model_driven" for route in routes if isinstance(route, str)):
            _fail("golden_semantic_background_mcp_route_invalid")
    if (
        "get_task_context" not in tools
        or "read_task_artifact" not in tools
        or not tools.intersection(
            {"list_records", "get_records", "search_records", "query_relations"}
        )
    ):
        _fail("golden_semantic_stage_evidence_reread_incomplete")


def _analysis_payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = value.get("payload")
    return payload if isinstance(payload, Mapping) else value


def _critical_payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = _analysis_payload(value)
    review = payload.get("critical_review")
    return review if isinstance(review, Mapping) else payload


def _final_analysis(report: Mapping[str, Any], critical: Mapping[str, Any]) -> Mapping[str, Any]:
    revised = critical.get("revised_analysis")
    if isinstance(revised, Mapping):
        return revised
    analysis = report.get("analysis")
    return analysis if isinstance(analysis, Mapping) else report


def _target_identity(report: Mapping[str, Any], analysis: Mapping[str, Any]) -> Mapping[str, Any]:
    value = report.get("target_identity")
    if not isinstance(value, Mapping):
        value = analysis.get("target_identity")
    if not isinstance(value, Mapping):
        _fail("golden_math_target_identity_missing")
    return value


def _require_reasoning(analysis: Mapping[str, Any], *, later: bool = True) -> Mapping[str, Any]:
    reasoning = analysis.get("reasoning_diagnosis")
    if not isinstance(reasoning, Mapping) or not isinstance(reasoning.get("first_break"), Mapping):
        _fail("golden_math_first_break_missing")
    for key in ("independent_correct_steps", "hint_dependencies", "self_corrections"):
        if not isinstance(reasoning.get(key), list) or not reasoning[key]:
            _fail("golden_math_learning_boundary_missing")
    if later and (not isinstance(reasoning.get("later_breaks"), list) or not reasoning["later_breaks"]):
        _fail("golden_math_later_breaks_missing")
    formal = analysis.get("formalization_candidates")
    if isinstance(formal, Mapping) and formal.get("mastery_evidence") not in (None, []):
        _fail("golden_math_unauthorized_mastery_upgrade")
    return reasoning


def _assert_signal_closure(analysis: Mapping[str, Any]) -> None:
    signals = analysis.get("atomic_signals")
    matrix = analysis.get("truth_match_matrix")
    novel = analysis.get("novel_knowledge_candidates")
    unmatched = analysis.get("unmatched_signals")
    if not isinstance(signals, list) or not signals:
        _fail("golden_math_signal_closure_missing")
    signal_ids = {str(row.get("signal_id")) for row in signals if isinstance(row, Mapping)}
    resolved = {
        str(row.get("signal_id"))
        for rows in (matrix, novel)
        if isinstance(rows, list)
        for row in rows
        if isinstance(row, Mapping)
    }
    if not signal_ids <= resolved or unmatched not in ([], None):
        _fail("golden_math_signal_closure_invalid")


def _validate_math_replay(
    role: str, report: Mapping[str, Any], critical: Mapping[str, Any], operations: set[str]
) -> list[str]:
    analysis = _final_analysis(report, critical)
    identity = _target_identity(report, analysis)
    plan = analysis.get("sol_verification_plan")
    disposition = plan.get("recommended_disposition") if isinstance(plan, Mapping) else None
    _assert_signal_closure(analysis)
    if role in {"GS-111", "GS-240"}:
        if (
            identity.get("formal_card_id") != role
            or identity.get("delivered_card_id") != role
            or disposition != "update_existing_candidate"
            or "propose_new_item" in operations
            or "new_wrong_item_proposal" in operations
            or not operations.intersection({"propose_item_update", "wrong_item_update_proposal"})
        ):
            _fail("golden_math_existing_identity_boundary_invalid")
        _require_reasoning(analysis)
        if role == "GS-111":
            # The fixed-y error used to exist only as prose.  A deterministic
            # postcondition needs a machine-owned semantic code, not substring
            # matching or a model-authored pass flag.
            codes = {
                str(row.get("business_semantic_code") or "")
                for row in analysis.get("reasoning_diagnosis", {}).get("later_breaks", [])
                if isinstance(row, Mapping)
            }
            if "VARIABLE_ROLE_Y_FIXED_IN_SECOND_PARTIAL" not in codes:
                _fail("golden_math_gs111_fixed_y_machine_field_missing")
        return [
            "existing_identity_preserved",
            "first_and_later_breaks_structurally_preserved",
            "independent_hint_and_self_correction_boundaries_preserved",
            "no_mastery_upgrade",
            "proposal_only",
        ]
    if role == "complete-new-intake":
        group = identity.get("target_group_key")
        if (
            identity.get("formal_card_id") is not None
            or identity.get("delivered_card_id") is not None
            or not isinstance(group, str)
            or not group.startswith("source:")
            or disposition != "new_candidate"
            or not operations.intersection({"propose_new_item", "new_wrong_item_proposal"})
            or operations.intersection({"propose_item_update", "wrong_item_update_proposal"})
        ):
            _fail("golden_math_new_source_identity_boundary_invalid")
        _require_reasoning(analysis)
        for operation in report.get("operations", []):
            target = operation.get("target") if isinstance(operation, Mapping) else None
            if isinstance(target, str) and FORMAL_ID_RE.fullmatch(target):
                _fail("golden_math_new_source_formal_id_invented")
        return [
            "new_source_formal_id_null",
            "new_item_proposal_only",
            "first_and_later_breaks_structurally_preserved",
            "no_mastery_upgrade",
        ]
    _fail("golden_math_role_invalid")


def _blocking_corrections(review: Mapping[str, Any]) -> list[str]:
    findings: dict[str, Mapping[str, Any]] = {}
    for field in (
        "unsupported_claims", "evidence_misreads", "answer_safety_findings",
        "missing_analysis", "required_corrections", "findings",
    ):
        rows = review.get(field)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            identifier = row.get("finding_id") or row.get("correction_id")
            if isinstance(identifier, str) and row.get("severity") in {"blocking", "error"}:
                findings[identifier] = row
    resolutions = review.get("correction_resolutions")
    if not isinstance(resolutions, list):
        _fail("golden_cs408_correction_resolutions_missing")
    resolution_ids = [
        str(row.get("finding_id"))
        for row in resolutions
        if isinstance(row, Mapping) and isinstance(row.get("finding_id"), str)
    ]
    if len(resolution_ids) != len(set(resolution_ids)):
        _fail("golden_cs408_blocking_correction_unresolved")
    by_id = {
        str(row.get("finding_id")): row
        for row in resolutions
        if isinstance(row, Mapping) and isinstance(row.get("finding_id"), str)
    }
    for identifier, finding in findings.items():
        resolution = by_id.get(identifier)
        if (
            not isinstance(resolution, Mapping)
            or resolution.get("resolution") != "applied"
            or resolution.get("affected_json_paths") != finding.get("affected_json_paths")
            or resolution.get("before") == resolution.get("after")
        ):
            _fail("golden_cs408_blocking_correction_unresolved")
    return sorted(findings)


def _knowledge_ids(value: Any) -> set[str]:
    ids: set[str] = set()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key in {"knowledge_id", "matched_node_id", "formal_id", "start_id", "end_id"} and isinstance(nested, str):
                ids.add(nested)
            ids.update(_knowledge_ids(nested))
    elif isinstance(value, list):
        for nested in value:
            ids.update(_knowledge_ids(nested))
    return ids


def _validate_cs408(
    role: str, report: Mapping[str, Any], critical: Mapping[str, Any], operations: set[str]
) -> list[str]:
    analysis = _final_analysis(report, critical)
    if (
        analysis.get("candidate_schema_version") != "study-intake-candidate-v3"
        or not isinstance(analysis.get("coverage_manifest"), Mapping)
        or analysis["coverage_manifest"].get("complete") is not True
        or analysis.get("unmatched_signals") not in ([], None)
    ):
        _fail("golden_cs408_candidate_closure_invalid")
    identifiers = _knowledge_ids(analysis)
    expected = {
        "DS_2023_002": {
            "DS_2023_002", "DS02-02", "DS07-04", "DS01-11", "DS02-03"
        },
        "FILE_PROTECTION": {"OS04-08", "OS_UNK_085"},
        "OS_2009_003": {"OS04-15", "OS_2009_003"},
        "FREE_SPACE": {"OS04-25", "OS04-28"},
    }[role]
    if not expected <= identifiers:
        _fail("golden_cs408_expected_identity_missing")
    edges = analysis.get("existing_formal_edges")
    if not isinstance(edges, list):
        _fail("golden_cs408_edge_projection_missing")
    for edge in edges:
        if (
            not isinstance(edge, Mapping)
            or edge.get("source_edge_status") != "existing_formal_edge"
            or edge.get("current_capture_action_status") != "proposal_only"
        ):
            _fail("golden_cs408_existing_edge_not_projection")
    expected_edges = {
        "DS_2023_002": {("R06", "DS_2024_002", "DS_2023_002")},
        "FILE_PROTECTION": {
            ("R06", "OS_2017_003", "OS_UNK_085"),
            ("R06", "OS_UNK_084", "OS_UNK_085"),
        },
        "OS_2009_003": {
            ("R06", "OS_2009_003", "OS_UNK_082"),
            ("R01", "OS_2020_004", "OS_2009_003"),
        },
        "FREE_SPACE": {
            ("R05", "DS_2017_003", "OS_UNK_074"),
            ("R06", "OS_2014_004", "OS_UNK_074"),
            ("R06", "OS_2015_003", "OS_UNK_074"),
            ("R05", "OS_2015_003", "OS_UNK_075"),
            ("R06", "OS_UNK_074", "OS_UNK_075"),
            ("R04", "OS_UNK_074", "OS_UNK_082"),
        },
    }[role]
    actual_edges = {
        (
            str(row.get("relationship_type") or "").split(" ", 1)[0],
            str(row.get("start_id") or ""),
            str(row.get("end_id") or ""),
        )
        for row in edges
        if isinstance(row, Mapping)
    }
    if actual_edges != expected_edges:
        _fail("golden_cs408_exact_edge_set_invalid")
    proposals = analysis.get("new_relation_proposals")
    if not isinstance(proposals, list) or proposals:
        _fail("golden_cs408_relation_proposals_missing")
    if any(
        not isinstance(row, Mapping)
        or row.get("formal_write_count", 0) != 0
        or row.get("status", "proposal_only") not in {"proposal_only", "needs_review"}
        for row in proposals
    ):
        _fail("golden_cs408_relation_not_proposal_only")
    if analysis.get("current_error_points") not in (None, []):
        _fail("golden_cs408_current_error_not_supported")
    formal = analysis.get("formalization_candidates")
    if isinstance(formal, Mapping) and formal.get("mastery_evidence") not in (None, []):
        _fail("golden_cs408_unauthorized_mastery_upgrade")
    if any("relation" in operation and not operation.startswith("propose") and not operation.endswith("proposal") for operation in operations):
        _fail("golden_cs408_relation_not_proposal_only")
    corrections = _blocking_corrections(critical)
    return [
        "candidate_v3_complete",
        "expected_knowledge_identity_present",
        "blocking_corrections_closed:" + str(len(corrections)),
        "existing_edges_are_projection",
        "new_relations_are_proposal_only",
    ]


def _validate_english(
    analysis: Mapping[str, Any], source_capture_ids: Sequence[str], operations: set[str]
) -> list[str]:
    capture_ids = analysis.get("capture_event_ids")
    sentences = analysis.get("sentence_records")
    signals = analysis.get("observed_signals")
    outcomes = analysis.get("signal_outcomes")
    coverage = analysis.get("capture_coverage")
    if (
        not isinstance(capture_ids, list)
        or capture_ids != list(source_capture_ids)
        or not isinstance(sentences, list)
        or len(sentences) != 3
        or {row.get("source_event_id") for row in sentences if isinstance(row, Mapping)} != set(source_capture_ids)
    ):
        _fail("golden_english_three_event_binding_invalid")
    for row in sentences:
        if not isinstance(row, Mapping) or any(
            not isinstance(row.get(field), str) or not str(row[field]).strip()
            for field in (
                "source_sentence", "user_first_translation", "corrected_meaning",
                "user_evidence_verbatim",
            )
        ):
            _fail("golden_english_sentence_evidence_incomplete")
    if (
        not isinstance(signals, list)
        or len(signals) != 6
        or not isinstance(outcomes, list)
        or len(outcomes) != 6
        or not isinstance(coverage, Mapping)
        or coverage.get("declared_signal_count") != 6
        or coverage.get("covered_signal_count") != 6
        or coverage.get("unmatched_signal_ids") != []
    ):
        _fail("golden_english_signal_coverage_invalid")
    signal_ids = {row.get("signal_id") for row in signals if isinstance(row, Mapping)}
    outcome_ids = {row.get("signal_id") for row in outcomes if isinstance(row, Mapping)}
    if signal_ids != outcome_ids or None in signal_ids:
        _fail("golden_english_signal_closure_invalid")
    expected_signals = {
        ("pregnant", "EVT-20260806-507660196D4F941D"),
        ("entertaining", "EVT-20260806-507660196D4F941D"),
        ("permanent", "EVT-20260806-8F4D09BF50F3F1B3"),
        ("gossip", "EVT-20260806-8F4D09BF50F3F1B3"),
        ("V-ing phrase as subject", "EVT-20260806-4DF6447806282008"),
        ("be highly valued by", "EVT-20260806-4DF6447806282008"),
    }
    actual_signals = {
        (str(row.get("canonical_term") or ""), str(row.get("source_event_id") or ""))
        for row in signals
        if isinstance(row, Mapping)
    }
    if actual_signals != expected_signals:
        _fail("golden_english_exact_signal_set_invalid")
    protected = {"unknown", "mistranslated", "structure_unresolved", "needs_hint"}
    if any(
        row.get("knowledge_state") in protected
        and next((out for out in outcomes if out.get("signal_id") == row.get("signal_id")), {}).get("terminal_status")
        not in {"candidate", "protected_candidate", "needs_review"}
        for row in signals
        if isinstance(row, Mapping)
    ):
        _fail("golden_english_same_day_protection_missing")
    if analysis.get("reactivation_proposals") not in ([], None) or "reactivation_proposal" in operations:
        _fail("golden_english_unauthorized_reactivation")
    exclusions = analysis.get("review_exclusion_proposals")
    if not isinstance(exclusions, list) or len(exclusions) != 1:
        _fail("golden_english_exclusion_projection_invalid")
    for row in exclusions:
        scan = row.get("day_conflict_check") if isinstance(row, Mapping) else None
        if (
            not isinstance(row, Mapping)
            or row.get("proposal_type") != "review_exclusion_proposal"
            or row.get("item") != "celebrities"
            or row.get("bank_id") != "20260424-204"
            or not isinstance(scan, Mapping)
            or scan.get("event_count_scanned") != 3
            or scan.get("explicit_unknown_seen_anywhere_in_day") is not False
        ):
            _fail("golden_english_exclusion_projection_invalid")
    if analysis.get("needs_user_decision") not in (None, []):
        _fail("golden_english_unexpected_user_decision")
    items = analysis.get("items")
    if not isinstance(items, list) or len(items) != 6:
        _fail("golden_english_item_candidates_invalid")
    source_sentences = {
        " ".join(str(row["source_sentence"]).casefold().split()) for row in sentences
    }
    for item in items:
        if (
            not isinstance(item, Mapping)
            or item.get("candidate_status") != "familiarity_candidate"
            or item.get("mastery_proposal") is not None
        ):
            _fail("golden_english_item_candidates_invalid")
        expected_bank = (
            "new_candidate"
            if item.get("item") == "V-ing phrase as subject"
            else "existing_bank"
        )
        if item.get("bank_status") != expected_bank:
            _fail("golden_english_item_candidates_invalid")
        card = item.get("card")
        old_example = card.get("old_word_example") if isinstance(card, Mapping) else None
        if (
            not isinstance(old_example, str)
            or " ".join(old_example.casefold().split()) in source_sentences
        ):
            _fail("golden_english_old_word_example_not_distinct")
    forbidden = {"formal_writeback", "mastered_items_write", "wordbook_mutation"}
    if any(
        value not in (None, False, 0, [], {}, "none", "proposal_only")
        for key in forbidden
        for value in _all_values(analysis, key)
    ):
        _fail("golden_english_formal_state_mutation")
    return [
        "three_sentence_facts_preserved",
        "six_of_six_signals_closed",
        "same_day_unknown_protection_preserved",
        "no_unauthorized_reactivation",
        "review_exclusion_is_proposal_only",
        "formal_write_count_zero",
    ]


def _common_task(
    row: Mapping[str, Any], *, release_id: str, key: bytes
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], set[str], dict[str, str]]:
    role = str(row.get("role") or "")
    subject = str(row.get("subject") or "")
    capture_id = str(row.get("capture_id") or "")
    package_path, package_sha, package = _artifact_ref(row.get("package"), "golden_semantic_package_invalid")
    proposal_path, _proposal_file_sha, proposal = _artifact_ref(row.get("proposal"), "golden_semantic_proposal_invalid")
    proposal_sha = _value_sha256(proposal)
    _quality_path, quality_sha, quality = _artifact_ref(row.get("quality_receipt"), "golden_semantic_quality_receipt_invalid")
    _analysis_path, analysis_sha, analysis_output = _artifact_ref(row.get("analysis_output"), "golden_semantic_analysis_output_invalid")
    _critical_path, critical_sha, critical_output = _artifact_ref(row.get("critical_review_output"), "golden_semantic_critical_output_invalid")
    transcript_refs = row.get("transcripts")
    if not isinstance(transcript_refs, Mapping) or set(transcript_refs) != {
        "analysis", "critical_review"
    }:
        _fail("golden_semantic_transcript_refs_invalid")
    _ap, analysis_transcript_sha, analysis_transcript = _artifact_ref(transcript_refs.get("analysis"), "golden_semantic_analysis_transcript_invalid")
    _cp, critical_transcript_sha, critical_transcript = _artifact_ref(transcript_refs.get("critical_review"), "golden_semantic_critical_transcript_invalid")
    report_path, report_sha, report = _artifact_ref(row.get("report"), "golden_semantic_report_invalid")
    _assert_zero_writes(row)
    for value in (package, proposal, quality, analysis_output, critical_output, analysis_transcript, critical_transcript, report):
        _assert_zero_writes(value)
    _verify_quality_hmac(quality, key)
    binding = package.get("processing_binding")
    stage_receipts = package.get("stage_receipts")
    if (
        package.get("subject") != subject
        or package.get("capture_id") != capture_id
        or package.get("model_call_count") != 2
        or package.get("semantic_stage_count") != 2
        or package.get("formal_write_count") != 0
        or not isinstance(binding, Mapping)
        or binding.get("candidate_release_id") != release_id
        or not isinstance(stage_receipts, Mapping)
        or package.get("luna_proposal_sha256") != proposal_sha
        or package.get("report_json_sha256") != report_sha
    ):
        _fail("golden_semantic_package_closure_invalid")
    _processing_binding(package, subject=subject, release_id=release_id)
    embedded = package.get("luna_proposal")
    if not isinstance(embedded, Mapping) or _value_sha256(embedded) != proposal_sha or dict(embedded) != proposal:
        _fail("golden_semantic_proposal_closure_invalid")
    generation = quality.get("authority", {}).get("generation")
    authority = quality.get("authority", {}).get("authority_fingerprint")
    if (
        quality.get("subject") != subject
        or quality.get("capture_id") != capture_id
        or quality.get("package_sha256") != package_sha
        or quality.get("proposal_sha256") != proposal_sha
        or quality.get("analysis_output_sha256") != analysis_sha
        or quality.get("critical_review_output_sha256") != critical_sha
        or quality.get("analysis_mcp_transcript_sha256") != analysis_transcript_sha
        or quality.get("critical_review_mcp_transcript_sha256") != critical_transcript_sha
        or quality.get("review_outcome") not in {"accepted", "corrected"}
        or package.get("evidence_generation") != generation
        or package.get("evidence_authority_fingerprint") != authority
        or proposal.get("subject") != subject
        or proposal.get("capture_id") != capture_id
        or proposal.get("review_status") != "proposal_ready"
        or proposal.get("critical_review_outcome") != quality.get("review_outcome")
        or quality.get("capture_freeze_receipt_sha256")
        != package.get("capture_freeze_receipt_sha256")
        or quality.get("mcp_read_session_receipt_sha256")
        != package.get("mcp_read_session_receipt_sha256")
        or proposal.get("processing_binding_sha256")
        != package.get("processing_binding_sha256")
        or proposal.get("capture_freeze_receipt_sha256")
        != package.get("capture_freeze_receipt_sha256")
        or proposal.get("read_session_id") != package.get("read_session_id")
        or proposal.get("read_session_manifest_sha256")
        != package.get("read_session_manifest_sha256")
        or proposal.get("mcp_read_session_receipt_sha256")
        != package.get("mcp_read_session_receipt_sha256")
        or proposal.get("mcp_stage_transcript_sha256s")
        != [analysis_transcript_sha, critical_transcript_sha]
    ):
        _fail("golden_semantic_quality_closure_invalid")
    for operation in proposal.get("operations", []):
        operation_proposal = operation.get("proposal") if isinstance(operation, Mapping) else None
        if (
            not isinstance(operation_proposal, Mapping)
            or operation_proposal.get("subject_payload_sha256") != report_sha
            or operation_proposal.get("subject_payload_role")
            != "critical_review_revised"
            or operation_proposal.get("host_semantic_projection") != "none"
        ):
            _fail("golden_semantic_proposal_payload_closure_invalid")
    for stage, transcript_sha in (("analysis", analysis_transcript_sha), ("critical_review", critical_transcript_sha)):
        receipt = stage_receipts.get(stage)
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("mcp_transcript_sha256") != transcript_sha
            or receipt.get("model_call_count") != 1
            or receipt.get("processing_binding_sha256")
            != package.get("processing_binding_sha256")
            or receipt.get("read_session_id") != package.get("read_session_id")
            or receipt.get("evidence_generation") != generation
            or receipt.get("evidence_authority_fingerprint") != authority
            or receipt.get("consumed_terminal_duplicate_read_count") != 0
            or receipt.get("formal_write_count") != 0
        ):
            _fail("golden_semantic_stage_receipt_closure_invalid")
    transcript_publication = package.get("mcp_stage_transcripts")
    if not isinstance(transcript_publication, Mapping):
        _fail("golden_semantic_stage_receipt_closure_invalid")
    grounding_sha256s: list[str] = []
    for stage, expected_sha in (
        ("analysis", analysis_transcript_sha),
        ("critical_review", critical_transcript_sha),
    ):
        publication = transcript_publication.get(stage)
        if (
            not isinstance(publication, Mapping)
            or publication.get("transcript_sha256") != expected_sha
            or not isinstance(publication.get("grounding_manifest_sha256"), str)
            or SHA256_RE.fullmatch(
                str(publication.get("grounding_manifest_sha256"))
            ) is None
        ):
            _fail("golden_semantic_stage_receipt_closure_invalid")
        grounding_sha256s.append(str(publication["grounding_manifest_sha256"]))
    if proposal.get("mcp_grounding_manifest_sha256s") != grounding_sha256s:
        _fail("golden_semantic_proposal_grounding_closure_invalid")
    _transcript(analysis_transcript, subject=subject, stage="analysis", release_id=release_id, generation=str(generation), authority=str(authority))
    _transcript(critical_transcript, subject=subject, stage="critical_review", release_id=release_id, generation=str(generation), authority=str(authority))
    operations = _operations(proposal)
    return (
        package,
        proposal,
        _analysis_payload(analysis_output),
        _critical_payload(critical_output),
        report,
        operations,
        {"package_path": str(package_path), "report_path": str(report_path), "quality_sha256": quality_sha},
    )


def validate(evidence_path: Path, authority_key_path: Path, output_root: Path) -> dict[str, Any]:
    evidence = _load_json(evidence_path, "golden_semantic_evidence_invalid")
    if (
        set(evidence) != {
            "schema_version", "candidate_release_id", "authority_key_sha256",
            "controlled_replay_specs", "math_live_business_manifest", "tasks",
            "formal_write_count",
        }
        or evidence.get("schema_version") != EVIDENCE_SCHEMA
        or evidence.get("formal_write_count") != 0
    ):
        _fail("golden_semantic_evidence_invalid")
    _assert_no_legacy_english(evidence)
    release_id = evidence.get("candidate_release_id")
    if not isinstance(release_id, str) or SHA256_RE.fullmatch(release_id) is None:
        _fail("golden_semantic_release_invalid")
    key = _load_authority_key(authority_key_path, evidence.get("authority_key_sha256"))
    specs = evidence.get("controlled_replay_specs")
    if not isinstance(specs, Mapping) or set(specs) != {"math", "cs408", "english"}:
        _fail("golden_semantic_controlled_specs_invalid")
    math_rows, math_spec_sha = _spec_rows(specs["math"], "math", release_id)
    cs_rows, cs_spec_sha = _spec_rows(specs["cs408"], "cs408", release_id)
    en_rows, en_spec_sha = _spec_rows(specs["english"], "english", release_id)
    if {row["sample_role"] for row in math_rows} != MATH_ROLES or len(math_rows) != 6:
        _fail("golden_semantic_math6_spec_not_executable")
    if any(
        row["sample_role"] in LIVE_MATH_ROLES
        and (
            row.get("fixture_kind") != "math_live_business_task"
            or not isinstance(row.get("business_manifest_sha256"), str)
        )
        for row in math_rows
    ):
        _fail("golden_semantic_math_live_frozen_task_binding_missing")
    if {row["sample_role"] for row in cs_rows} != CS408_ROLES or len(cs_rows) != 4:
        _fail("golden_semantic_cs4084_spec_invalid")
    if {row["sample_role"] for row in en_rows} != ENGLISH_SOURCE_ROLES or len(en_rows) != 3:
        _fail("golden_semantic_english_daily_spec_invalid")
    manifest_path, manifest_sha, _manifest_document = _artifact_ref(
        evidence.get("math_live_business_manifest"),
        "golden_semantic_math_business_manifest_invalid",
    )
    try:
        business = validate_math_business_manifest(manifest_path)
    except MathLiveBusinessFixtureError as exc:
        raise GoldenSemanticError("golden_semantic_math_business_manifest_invalid") from exc
    business_tasks = {
        row["capture_id"]: row
        for row in business.get("tasks", [])
        if isinstance(row, Mapping) and isinstance(row.get("capture_id"), str)
    }
    raw_tasks = evidence.get("tasks")
    if not isinstance(raw_tasks, list) or len(raw_tasks) != 11:
        _fail("golden_semantic_task_set_invalid")
    roles = {row.get("role") for row in raw_tasks if isinstance(row, Mapping)}
    if roles != TASK_ROLES or len(roles) != len(raw_tasks):
        _fail("golden_semantic_task_set_invalid")
    spec_capture_by_role = {
        row["sample_role"]: row["capture_id"] for row in [*math_rows, *cs_rows]
    }
    english_capture_ids = [row["capture_id"] for row in en_rows]
    results: list[dict[str, Any]] = []
    for raw in sorted(raw_tasks, key=lambda row: str(row.get("role"))):
        if not isinstance(raw, Mapping):
            _fail("golden_semantic_task_set_invalid")
        role = str(raw["role"])
        allowed_task_fields = {
            "role", "subject", "capture_id", "package", "proposal",
            "quality_receipt", "analysis_output", "critical_review_output",
            "transcripts", "report", "formal_write_count",
        }
        if role == "english-daily-microbatch":
            allowed_task_fields.add("source_capture_ids")
        if set(raw) != allowed_task_fields or raw.get("formal_write_count") != 0:
            _fail("golden_semantic_task_shape_invalid")
        subject = str(raw.get("subject") or "")
        capture_id = str(raw.get("capture_id") or "")
        expected_subject = "math" if role in MATH_ROLES else "cs408" if role in CS408_ROLES else "english"
        if subject != expected_subject:
            _fail("golden_semantic_task_subject_mismatch")
        if role != "english-daily-microbatch" and spec_capture_by_role.get(role) != capture_id:
            _fail("golden_semantic_task_spec_binding_mismatch")
        package, _proposal, analysis, critical, report, operations, paths = _common_task(
            raw, release_id=release_id, key=key
        )
        if role in LIVE_MATH_ROLES:
            business_capture = LIVE_MATH_ROLES[role]
            task = business_tasks.get(business_capture)
            if not isinstance(task, Mapping) or capture_id != business_capture:
                _fail("golden_semantic_math_business_task_binding_invalid")
            try:
                live = validate_live_math_result(
                    task, Path(paths["package_path"]), Path(paths["report_path"])
                )
            except MathLiveGoldenAssertionError as exc:
                raise GoldenSemanticError(exc.code) from exc
            assertions = list(live["assertions"])
        elif role in {"GS-111", "GS-240", "complete-new-intake"}:
            assertions = _validate_math_replay(role, report, critical, operations)
        elif role in CS408_ROLES:
            assertions = _validate_cs408(role, report, critical, operations)
        else:
            source_ids = raw.get("source_capture_ids")
            if (
                not isinstance(source_ids, list)
                or len(source_ids) != 3
                or set(source_ids) != set(english_capture_ids)
            ):
                _fail("golden_english_three_event_binding_invalid")
            effective = package.get("analysis")
            if not isinstance(effective, Mapping):
                effective = _final_analysis(report, critical)
            assertions = _validate_english(effective, english_capture_ids, operations)
        results.append(
            {
                "role": role,
                "subject": subject,
                "capture_id": capture_id,
                "package_sha256": raw["package"]["sha256"],
                "quality_receipt_sha256": paths["quality_sha256"],
                "assertions": assertions,
                "status": "passed",
                "model_call_count": 2,
                "formal_write_count": 0,
            }
        )
    evidence_sha = _file_sha256(evidence_path.resolve())
    core = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "candidate_release_id": release_id,
        "evidence_manifest_sha256": evidence_sha,
        "controlled_replay_spec_sha256s": {
            "math": math_spec_sha,
            "cs408": cs_spec_sha,
            "english": en_spec_sha,
        },
        "math_live_business_manifest_sha256": manifest_sha,
        "task_count": 11,
        "results": results,
        "model_call_count": 22,
        "formal_write_count": 0,
    }
    report = {
        **core,
        "seal": {
            "algorithm": "HMAC-SHA256",
            "purpose": PURPOSE,
            "hmac_sha256": _seal(key, PURPOSE, core),
        },
    }
    payload = _canonical(report) + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    path = output_root.expanduser().resolve() / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, raw_name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
    temporary = Path(raw_name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                _fail("golden_semantic_report_collision")
        os.chmod(path, 0o400)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {
        "status": "passed",
        "candidate_release_id": release_id,
        "validation_report_path": str(path),
        "validation_report_sha256": digest,
        "task_count": 11,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(allow_abbrev=False)
    root.add_argument("--evidence", type=Path, required=True)
    root.add_argument("--authority-key", type=Path, required=True)
    root.add_argument("--output-root", type=Path, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        value = validate(args.evidence, args.authority_key, args.output_root)
    except GoldenSemanticError as exc:
        print(
            json.dumps(
                {
                    "schema_version": "candidate-golden-semantic-validation-error-v1",
                    "status": "failed",
                    "error_code": exc.code,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
