"""Deterministic business assertions for the three live math tasks.

This validator runs only after the normal package, receipt, MCP transcript, and
Schema validators.  It does not reinterpret mathematics.  It fails closed when
the model-owned result crosses the task-specific semantic boundary that the
user explicitly supplied for GS-109, GS-507, or the new-source episode.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping


RESULT_SCHEMA = "study-intake-math-live-business-golden-assertion-result-v1"
PACKAGE_SCHEMA = "study-intake-preprocess-package-v2"
REPORT_SCHEMA = "study-intake-luna-math-candidate-v4"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FORMAL_ID_RE = re.compile(r"^(?:GS|LA|PR)-[A-Za-z0-9._:-]+$")

GS109_KIND = "independent_correct_no_false_wrong_card"
GS507_KIND = "wrong_then_corrected_weakness_proposal"
NEW_SOURCE_KIND = "wrong_then_corrected_multistage_method_proposal"
SUPPORTED_TASKS = {
    ("GS-109", GS109_KIND),
    ("GS-507", GS507_KIND),
    (None, NEW_SOURCE_KIND),
}
GS109_ALLOWED_OPERATIONS = {
    "mark_existing_item",
    "mark_existing_knowledge",
    "mark_status",
}
GS507_ALLOWED_OPERATIONS = {
    "mark_existing_item",
    "propose_item_update",
    "mark_existing_knowledge",
    "propose_relation",
    "mark_status",
}
NEW_SOURCE_ALLOWED_OPERATIONS = {
    "propose_new_item",
    "mark_existing_knowledge",
    "propose_new_knowledge",
    "propose_relation",
    "mark_status",
    "needs_review",
}
WEAKNESS_FORMAL_FIELDS = {
    "wrong_point",
    "error_causes",
    "method_gap",
    "wrong_history",
    "mastery_evidence",
    "relationship_proposals",
}


class MathLiveGoldenAssertionError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _fail(code: str) -> None:
    raise MathLiveGoldenAssertionError(code)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _value_sha256(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_content_addressed_json(
    path: Path, *, code: str, max_bytes: int = 16 * 1024 * 1024
) -> tuple[dict[str, Any], str]:
    try:
        path = path.expanduser().resolve(strict=True)
        node = path.lstat()
        if (
            path.is_symlink()
            or not path.is_file()
            or node.st_size <= 0
            or node.st_size > max_bytes
        ):
            raise OSError("unsafe file")
        digest = _file_sha256(path)
        if path.name != f"{digest}.json":
            raise OSError("not content addressed")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MathLiveGoldenAssertionError(code) from exc
    if not isinstance(value, dict):
        _fail(code)
    return value, digest


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(code)
    return value


def _list(value: Any, code: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(code)
    return value


def _assert_zero_formal_writes(value: Any, code: str) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key == "formal_write_count" and nested != 0:
                _fail(code)
            _assert_zero_formal_writes(nested, code)
    elif isinstance(value, list):
        for nested in value:
            _assert_zero_formal_writes(nested, code)


def _validate_common(
    task: Mapping[str, Any],
    package: Mapping[str, Any],
    package_sha256: str,
    report: Mapping[str, Any],
    report_sha256: str,
) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    formal_id = task.get("formal_id")
    task_kind = task.get("task_kind")
    capture_id = task.get("capture_id")
    if (
        (formal_id, task_kind) not in SUPPORTED_TASKS
        or not isinstance(capture_id, str)
        or not capture_id
        or task.get("luna_eligible") is not True
        or task.get("proposal_only") is not True
    ):
        _fail("math_live_golden_task_contract_invalid")
    if (
        package.get("schema_version") != PACKAGE_SCHEMA
        or package.get("subject") != "math"
        or package.get("capture_id") != capture_id
        or package.get("pipeline_status") != "two_pass_ready"
        or package.get("model") != "gpt-5.6-luna"
        or package.get("reasoning_effort") != "max"
        or package.get("model_call_count") != 2
        or package.get("semantic_stage_count") != 2
        or package.get("host_semantic_prefetch") is not False
        or package.get("formal_write_count") != 0
        or package.get("report_json_sha256") != report_sha256
        or package.get("report_json_ref")
        != f"study-intake-report://sha256/{report_sha256}"
    ):
        _fail("math_live_golden_package_contract_invalid")
    _assert_zero_formal_writes(package, "math_live_golden_formal_write_detected")
    _assert_zero_formal_writes(report, "math_live_golden_formal_write_detected")
    proposal = _mapping(
        package.get("luna_proposal"), "math_live_golden_proposal_invalid"
    )
    if (
        package.get("luna_proposal_sha256") != _value_sha256(proposal)
        or proposal.get("schema_version") != "luna_proposal_v2"
        or proposal.get("subject") != "math"
        or proposal.get("capture_id") != capture_id
        or proposal.get("review_status") != "proposal_ready"
        or proposal.get("host_semantic_prefetch") is not False
        or proposal.get("formal_write_count") != 0
    ):
        _fail("math_live_golden_proposal_invalid")
    operations = _list(
        proposal.get("operations"), "math_live_golden_proposal_invalid"
    )
    if not operations or any(not isinstance(row, Mapping) for row in operations):
        _fail("math_live_golden_proposal_invalid")
    quality = _mapping(
        package.get("quality_receipt"), "math_live_golden_quality_invalid"
    )
    if (
        quality.get("pipeline_status") != "two_pass_ready"
        or quality.get("two_stage_status") != "two_pass_ready"
        or quality.get("quality_gate_status") != "pass"
        or quality.get("formal_write_count") != 0
    ):
        _fail("math_live_golden_quality_invalid")
    stage_receipts = _mapping(
        package.get("stage_receipts"), "math_live_golden_stage_receipts_invalid"
    )
    for stage in ("analysis", "critical_review"):
        receipt = _mapping(
            stage_receipts.get(stage), "math_live_golden_stage_receipts_invalid"
        )
        if (
            receipt.get("status") != "ready"
            or receipt.get("requested_model") != "gpt-5.6-luna"
            or receipt.get("requested_reasoning_effort") != "max"
            or receipt.get("runtime_identity_status")
            not in {"requested_unverified", "confirmed"}
            or receipt.get("model_call_count") != 1
            or receipt.get("formal_write_count") != 0
        ):
            _fail("math_live_golden_stage_receipts_invalid")
    if (
        report.get("schema_version") != REPORT_SCHEMA
        or report.get("capture_id") != capture_id
        or report.get("formal_write_count") != 0
        or report.get("relationship_mode") != "SHADOW"
    ):
        _fail("math_live_golden_report_contract_invalid")
    sol_actions = _mapping(
        report.get("sol_action_items"), "math_live_golden_report_contract_invalid"
    )
    if sol_actions.get("formal_writer_authority") != "nightly_sol_only":
        _fail("math_live_golden_formal_authority_invalid")
    analysis = _mapping(
        report.get("analysis"), "math_live_golden_analysis_invalid"
    )
    if analysis.get("schema_version") != "study-intake-luna-math-analysis-v2":
        _fail("math_live_golden_analysis_invalid")
    return analysis, [dict(row) for row in operations]


def _validate_gs109(
    analysis: Mapping[str, Any],
    report: Mapping[str, Any],
    operations: list[Mapping[str, Any]],
) -> list[str]:
    operation_types = {str(row.get("operation") or "") for row in operations}
    if (
        not operation_types
        or not operation_types.issubset(GS109_ALLOWED_OPERATIONS)
        or not operation_types.intersection({"mark_existing_item", "mark_status"})
    ):
        _fail("math_gs109_wrong_or_weakness_proposal")
    plan = _mapping(
        analysis.get("sol_verification_plan"), "math_gs109_analysis_invalid"
    )
    if plan.get("recommended_disposition") != "mastery_observation_candidate":
        _fail("math_gs109_wrong_or_weakness_proposal")
    evidence = _mapping(
        analysis.get("evidence_assessment"), "math_gs109_analysis_invalid"
    )
    if (
        evidence.get("completeness") != "limited_by_evidence"
        or not _list(evidence.get("gaps"), "math_gs109_analysis_invalid")
        or not _list(analysis.get("unresolved"), "math_gs109_analysis_invalid")
    ):
        _fail("math_gs109_unknown_reasoning_boundary_missing")
    reasoning = _mapping(
        analysis.get("reasoning_diagnosis"), "math_gs109_analysis_invalid"
    )
    for key in (
        "independent_correct_steps",
        "later_breaks",
        "hint_dependencies",
        "self_corrections",
        "history_merge",
    ):
        if _list(reasoning.get(key), "math_gs109_analysis_invalid"):
            _fail("math_gs109_reasoning_inference_invalid")
    if reasoning.get("first_break") is not None:
        _fail("math_gs109_reasoning_inference_invalid")
    for claim in _list(
        analysis.get("correct_reasoning_reconstruction"),
        "math_gs109_analysis_invalid",
    ):
        if (
            not isinstance(claim, Mapping)
            or claim.get("claim_type") != "standard_math_candidate"
            or claim.get("provenance")
            not in {"formal_card", "source_bundle", "image"}
        ):
            _fail("math_gs109_reasoning_inference_invalid")
    signatures = _mapping(
        analysis.get("knowledge_error_signatures"),
        "math_gs109_analysis_invalid",
    )
    if signatures.get("first_error") is not None:
        _fail("math_gs109_wrong_or_weakness_proposal")
    for key in ("later_errors", "historical_errors", "current_errors"):
        if _list(signatures.get(key), "math_gs109_analysis_invalid"):
            _fail("math_gs109_wrong_or_weakness_proposal")
    formal = _mapping(
        analysis.get("formalization_candidates"), "math_gs109_analysis_invalid"
    )
    if any(_list(formal.get(key), "math_gs109_analysis_invalid") for key in WEAKNESS_FORMAL_FIELDS):
        _fail("math_gs109_wrong_or_weakness_proposal")
    independent = _list(
        formal.get("independent_performance"), "math_gs109_analysis_invalid"
    )
    if any(
        not isinstance(claim, Mapping)
        or claim.get("claim_type") != "observed_fact"
        or claim.get("provenance") != "capture"
        or not str(claim.get("counterevidence_or_boundary") or "").strip()
        for claim in independent
    ):
        _fail("math_gs109_reasoning_inference_invalid")
    if _list(
        report.get("novel_knowledge_candidates"),
        "math_gs109_report_invalid",
    ) or _list(report.get("novel_error_candidates"), "math_gs109_report_invalid"):
        _fail("math_gs109_wrong_or_weakness_proposal")
    if _list(
        report.get("relationship_decisions"), "math_gs109_report_invalid"
    ):
        _fail("math_gs109_wrong_or_weakness_proposal")
    signals = _list(analysis.get("atomic_signals"), "math_gs109_analysis_invalid")
    if any(
        not isinstance(signal, Mapping)
        or signal.get("error_role") != "none"
        or signal.get("signal_type") in {"error", "trap"}
        for signal in signals
    ):
        _fail("math_gs109_wrong_or_weakness_proposal")
    return [
        "independent_correct_observation_preserved",
        "unobserved_reasoning_remains_unknown",
        "no_false_wrong_item_or_weakness_proposal",
        "no_new_knowledge_or_relation_proposal",
        "formal_write_count_zero",
    ]


def _validate_gs507(
    analysis: Mapping[str, Any],
    report: Mapping[str, Any],
    operations: list[Mapping[str, Any]],
) -> list[str]:
    operation_types = {str(row.get("operation") or "") for row in operations}
    if (
        not operation_types
        or not operation_types.issubset(GS507_ALLOWED_OPERATIONS)
        or "propose_item_update" not in operation_types
    ):
        _fail("math_gs507_operation_boundary_invalid")
    plan = _mapping(
        analysis.get("sol_verification_plan"), "math_gs507_analysis_invalid"
    )
    if plan.get("recommended_disposition") != "update_existing_candidate":
        _fail("math_gs507_identity_boundary_invalid")
    reasoning = _mapping(
        analysis.get("reasoning_diagnosis"), "math_gs507_analysis_invalid"
    )
    if not isinstance(reasoning.get("first_break"), Mapping):
        _fail("math_gs507_first_break_missing")
    for key, code in (
        ("later_breaks", "math_gs507_later_breaks_missing"),
        ("independent_correct_steps", "math_gs507_independent_steps_missing"),
        ("hint_dependencies", "math_gs507_post_explanation_boundary_missing"),
        ("self_corrections", "math_gs507_post_explanation_boundary_missing"),
    ):
        if not _list(reasoning.get(key), "math_gs507_analysis_invalid"):
            _fail(code)
    signatures = _mapping(
        analysis.get("knowledge_error_signatures"),
        "math_gs507_analysis_invalid",
    )
    if not isinstance(signatures.get("first_error"), Mapping):
        _fail("math_gs507_first_break_missing")
    if not _list(signatures.get("later_errors"), "math_gs507_analysis_invalid"):
        _fail("math_gs507_later_breaks_missing")
    formal = _mapping(
        analysis.get("formalization_candidates"), "math_gs507_analysis_invalid"
    )
    for key in ("wrong_point", "error_causes", "method_gap"):
        if not _list(formal.get(key), "math_gs507_analysis_invalid"):
            _fail("math_gs507_weakness_proposal_missing")
    if _list(formal.get("mastery_evidence"), "math_gs507_analysis_invalid"):
        _fail("math_gs507_independent_mastery_upgrade")
    if _list(
        report.get("novel_knowledge_candidates"),
        "math_gs507_report_invalid",
    ):
        _fail("math_gs507_operation_boundary_invalid")
    relationship_decisions = _list(
        report.get("relationship_decisions"), "math_gs507_report_invalid"
    )
    if "propose_relation" in operation_types and not any(
        isinstance(row, Mapping) and row.get("verdict") == "keep_proposal"
        for row in relationship_decisions
    ):
        _fail("math_gs507_relation_without_kept_decision")
    signals = _list(analysis.get("atomic_signals"), "math_gs507_analysis_invalid")
    error_roles = {
        str(row.get("error_role") or "")
        for row in signals
        if isinstance(row, Mapping)
    }
    if not {"first_error", "later_error"}.issubset(error_roles):
        _fail("math_gs507_break_signal_closure_invalid")
    return [
        "existing_item_weakness_proposal_only",
        "first_and_later_breaks_preserved",
        "independent_correct_steps_preserved",
        "post_explanation_understanding_labeled_separately",
        "no_independent_mastery_upgrade",
        "formal_write_count_zero",
    ]


def _validate_new_source_identity(
    task: Mapping[str, Any],
    report: Mapping[str, Any],
    operations: list[Mapping[str, Any]],
) -> None:
    if (
        task.get("formal_id") is not None
        or task.get("source_route") != "new_source_learning_episode"
        or task.get("source_locator") != "question-bank-id:170710"
    ):
        _fail("math_new_source_identity_contract_invalid")
    target_identity = _mapping(
        report.get("target_identity"), "math_new_source_identity_contract_invalid"
    )
    if (
        target_identity.get("formal_card_id") is not None
        or target_identity.get("delivered_card_id") is not None
        or target_identity.get("target_group_key")
        not in {
            "source:question-bank-id:170710",
            f"source:{task['capture_id']}",
        }
    ):
        _fail("math_new_source_formal_id_invented")

    for operation in operations:
        operation_type = str(operation.get("operation") or "")
        target = operation.get("target")
        if (
            operation_type in {"propose_new_item", "propose_new_knowledge"}
            and (
                not isinstance(target, str)
                or not target
                or FORMAL_ID_RE.fullmatch(target) is not None
            )
        ):
            _fail("math_new_source_formal_id_invented")

    for key in ("novel_knowledge_candidates", "novel_error_candidates"):
        for candidate in _list(
            report.get(key), "math_new_source_report_invalid"
        ):
            if not isinstance(candidate, Mapping):
                _fail("math_new_source_report_invalid")
            candidate_id = candidate.get("candidate_id")
            if (
                isinstance(candidate_id, str)
                and FORMAL_ID_RE.fullmatch(candidate_id) is not None
            ):
                _fail("math_new_source_formal_id_invented")


def _validate_new_source(
    task: Mapping[str, Any],
    analysis: Mapping[str, Any],
    report: Mapping[str, Any],
    operations: list[Mapping[str, Any]],
) -> list[str]:
    _validate_new_source_identity(task, report, operations)
    operation_types = {str(row.get("operation") or "") for row in operations}
    if (
        not operation_types
        or not operation_types.issubset(NEW_SOURCE_ALLOWED_OPERATIONS)
        or "propose_new_item" not in operation_types
    ):
        _fail("math_new_source_operation_boundary_invalid")
    plan = _mapping(
        analysis.get("sol_verification_plan"),
        "math_new_source_analysis_invalid",
    )
    if plan.get("recommended_disposition") != "new_candidate":
        _fail("math_new_source_identity_contract_invalid")
    reasoning = _mapping(
        analysis.get("reasoning_diagnosis"),
        "math_new_source_analysis_invalid",
    )
    if not isinstance(reasoning.get("first_break"), Mapping):
        _fail("math_new_source_first_break_missing")
    for key, code in (
        ("later_breaks", "math_new_source_later_breaks_missing"),
        ("independent_correct_steps", "math_new_source_independent_steps_missing"),
        ("hint_dependencies", "math_new_source_explanation_boundary_missing"),
    ):
        if not _list(reasoning.get(key), "math_new_source_analysis_invalid"):
            _fail(code)
    signatures = _mapping(
        analysis.get("knowledge_error_signatures"),
        "math_new_source_analysis_invalid",
    )
    if not isinstance(signatures.get("first_error"), Mapping):
        _fail("math_new_source_first_break_missing")
    if not _list(signatures.get("later_errors"), "math_new_source_analysis_invalid"):
        _fail("math_new_source_later_breaks_missing")
    formal = _mapping(
        analysis.get("formalization_candidates"),
        "math_new_source_analysis_invalid",
    )
    for key in ("wrong_point", "error_causes", "method_gap"):
        if not _list(formal.get(key), "math_new_source_analysis_invalid"):
            _fail("math_new_source_weakness_proposal_missing")
    if _list(formal.get("mastery_evidence"), "math_new_source_analysis_invalid"):
        _fail("math_new_source_independent_mastery_upgrade")
    relationship_decisions = _list(
        report.get("relationship_decisions"), "math_new_source_report_invalid"
    )
    if "propose_relation" in operation_types and not any(
        isinstance(row, Mapping) and row.get("verdict") == "keep_proposal"
        for row in relationship_decisions
    ):
        _fail("math_new_source_relation_without_kept_decision")
    signals = _list(
        analysis.get("atomic_signals"), "math_new_source_analysis_invalid"
    )
    error_roles = {
        str(row.get("error_role") or "")
        for row in signals
        if isinstance(row, Mapping)
    }
    if not {"first_error", "later_error"}.issubset(error_roles):
        _fail("math_new_source_break_signal_closure_invalid")
    return [
        "new_source_identity_remains_formal_id_null",
        "no_gs_la_or_pr_identifier_invented",
        "new_wrong_item_and_knowledge_candidates_are_proposal_only",
        "first_and_later_multistage_breaks_preserved",
        "independent_correct_steps_preserved",
        "post_explanation_understanding_labeled_separately",
        "no_independent_mastery_upgrade",
        "formal_write_count_zero",
    ]


def validate_result(
    task: Mapping[str, Any], package_path: Path, report_path: Path
) -> dict[str, Any]:
    package, package_sha256 = _load_content_addressed_json(
        package_path, code="math_live_golden_package_unreadable"
    )
    report, report_sha256 = _load_content_addressed_json(
        report_path, code="math_live_golden_report_unreadable"
    )
    analysis, operations = _validate_common(
        task, package, package_sha256, report, report_sha256
    )
    task_kind = str(task["task_kind"])
    if task_kind == GS109_KIND:
        assertions = _validate_gs109(analysis, report, operations)
    elif task_kind == GS507_KIND:
        assertions = _validate_gs507(analysis, report, operations)
    else:
        assertions = _validate_new_source(task, analysis, report, operations)
    return {
        "schema_version": RESULT_SCHEMA,
        "status": "passed",
        "capture_id": task["capture_id"],
        "business_task_id": task["business_task_id"],
        "formal_id": task["formal_id"],
        "task_kind": task_kind,
        "package_sha256": package_sha256,
        "report_sha256": report_sha256,
        "operation_types": sorted(
            {str(row.get("operation") or "") for row in operations}
        ),
        "assertions": assertions,
        "semantic_assertion_status": "passed",
        "model_call_count": 2,
        "formal_write_count": 0,
    }
