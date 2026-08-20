"""Content-addressed fan-in, review disposition, and Sol handoff."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping, Sequence


QUALITY = frozenset({"accepted", "corrected", "issues_found", "technical_quarantine"})
SOL_ACTIONS = frozenset({"adopt", "modify", "reject", "request_reread", "defer"})


class ReadBundleError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def build_read_bundle(plan: Mapping[str, Any], fanout: Mapping[str, Any]) -> dict[str, Any]:
    results = fanout.get("results")
    if not isinstance(results, list) or len(results) != len(plan.get("branches") or []):
        raise ReadBundleError("read_bundle_branch_membership_invalid")
    evidence_index: dict[str, dict[str, Any]] = {}
    membership: dict[str, list[str]] = {}
    conflicts: list[dict[str, Any]] = []
    technical_errors: list[str] = []
    successful: list[str] = []
    failed: list[str] = []
    missing: list[str] = []
    branch_hashes: list[dict[str, str]] = []
    required_by_id = {row["branch_id"]: row["required"] for row in plan["branches"]}
    for result in results:
        branch_id = result.get("branch_id")
        if branch_id not in required_by_id or result.get("plan_sha256") != plan.get("plan_sha256"):
            raise ReadBundleError("read_bundle_branch_identity_invalid")
        digest = result.get("result_sha256")
        core = {key: copy.deepcopy(value) for key, value in result.items() if key != "result_sha256"}
        if digest != sha256_value(core) or result.get("formal_write_count") != 0:
            technical_errors.append(f"branch_receipt_invalid:{branch_id}")
        status = result.get("status")
        if status == "succeeded":
            successful.append(branch_id)
        elif status in {"failed", "cancelled", "timed_out"}:
            failed.append(branch_id)
        else:
            missing.append(branch_id)
        branch_hashes.append({"branch_id": branch_id, "result_sha256": str(digest)})
        for evidence in result.get("evidence") or []:
            if not isinstance(evidence, Mapping):
                technical_errors.append(f"evidence_shape_invalid:{branch_id}")
                continue
            ref = evidence.get("evidence_ref")
            source_sha = evidence.get("source_sha256")
            if not isinstance(ref, str) or not ref or not isinstance(source_sha, str) or len(source_sha) != 64:
                technical_errors.append(f"evidence_identity_invalid:{branch_id}")
                continue
            previous = evidence_index.get(ref)
            if previous is not None and previous.get("source_sha256") != source_sha:
                conflicts.append({"evidence_ref": ref, "first_source_sha256": previous.get("source_sha256"), "conflicting_source_sha256": source_sha, "branch_id": branch_id})
            else:
                evidence_index.setdefault(ref, copy.deepcopy(dict(evidence)))
            membership.setdefault(ref, []).append(branch_id)
    required_failures = sorted(branch_id for branch_id in failed + missing if required_by_id.get(branch_id) is True)
    core = {
        "schema_version": "read_bundle_v1",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "subject": plan["subject"],
        "capture_id": plan["capture_id"],
        "frozen_task_sha256": plan["frozen_task_sha256"],
        "release_id": plan["release_id"],
        "activation_id": plan["activation_id"],
        "authority_snapshot_sha256": plan["authority_snapshot_sha256"],
        "generation": plan["generation"],
        "ordered_branch_results": branch_hashes,
        "successful_branch_ids": successful,
        "failed_branch_ids": failed,
        "missing_branch_ids": missing,
        "required_failure_branch_ids": required_failures,
        "evidence_index": dict(sorted(evidence_index.items())),
        "evidence_membership": {key: sorted(set(value)) for key, value in sorted(membership.items())},
        "deduplicated_evidence_count": len(evidence_index),
        "conflict_index": conflicts,
        "technical_integrity_errors": sorted(set(technical_errors)),
        "coverage_complete": not required_failures,
        "fanout_telemetry": {
            key: fanout[key]
            for key in ("logical_branch_count", "physical_slot_count", "maximum_active_branch_count", "wave_count", "duration_ms")
        },
        "formal_write_count": 0,
    }
    return {**core, "read_bundle_sha256": sha256_value(core)}


def build_analysis_candidate(bundle: Mapping[str, Any]) -> dict[str, Any]:
    core = {
        "schema_version": "multi_agent_candidate_v1",
        "subject": bundle["subject"],
        "capture_id": bundle["capture_id"],
        "read_bundle_sha256": bundle["read_bundle_sha256"],
        "evidence_refs": sorted(bundle["evidence_index"]),
        "claims": [
            {"evidence_ref": ref, "summary": value.get("summary")}
            for ref, value in sorted(bundle["evidence_index"].items())
        ],
        "conflicts": copy.deepcopy(bundle["conflict_index"]),
        "formal_write_count": 0,
    }
    return {**core, "candidate_sha256": sha256_value(core)}


def review_candidate(
    bundle: Mapping[str, Any], candidate: Mapping[str, Any], *,
    outcome: str, corrected_candidate: Mapping[str, Any] | None = None,
    issue_codes: Sequence[str] = (), recommended_follow_up_reads: Sequence[str] = (),
) -> dict[str, Any]:
    if outcome not in QUALITY:
        raise ReadBundleError("critical_review_outcome_invalid")
    technical = bool(bundle.get("technical_integrity_errors"))
    if technical and outcome != "technical_quarantine":
        raise ReadBundleError("technical_error_requires_quarantine")
    if outcome == "corrected" and not isinstance(corrected_candidate, Mapping):
        raise ReadBundleError("corrected_candidate_missing")
    if outcome != "corrected" and corrected_candidate is not None:
        raise ReadBundleError("unexpected_corrected_candidate")
    quality_clean = outcome in {"accepted", "corrected"} and not issue_codes
    candidate_trusted = outcome != "technical_quarantine"
    risk_core = {
        "schema_version": "risk_report_v1",
        "subject": bundle["subject"],
        "capture_id": bundle["capture_id"],
        "read_bundle_sha256": bundle["read_bundle_sha256"],
        "outcome": outcome,
        "issue_codes": sorted(set(issue_codes)),
        "technical_integrity_errors": copy.deepcopy(list(bundle.get("technical_integrity_errors") or [])),
        "required_failure_branch_ids": copy.deepcopy(list(bundle.get("required_failure_branch_ids") or [])),
        "conflicts": copy.deepcopy(list(bundle.get("conflict_index") or [])),
        "recommended_follow_up_reads": list(recommended_follow_up_reads),
        "automatic_retry": False,
        "formal_write_count": 0,
    }
    risk = {**risk_core, "risk_report_sha256": sha256_value(risk_core)}
    core = {
        "schema_version": "multi_agent_critical_review_v1",
        "subject": bundle["subject"],
        "capture_id": bundle["capture_id"],
        "read_bundle_sha256": bundle["read_bundle_sha256"],
        "candidate_sha256": candidate["candidate_sha256"],
        "outcome": outcome,
        "execution_status": "succeeded",
        "sol_review_ready": outcome != "technical_quarantine",
        "diagnostic_review_ready": True,
        "quality_clean": quality_clean,
        "candidate_preserved": True,
        "candidate_trusted": candidate_trusted,
        "risk_report_preserved": True,
        "report_disposition": "ready" if quality_clean else "needs_sol_review",
        "automatic_retry": False,
        "corrected_candidate": copy.deepcopy(dict(corrected_candidate)) if corrected_candidate is not None else None,
        "risk_report": risk,
        "formal_write_count": 0,
    }
    return {**core, "review_sha256": sha256_value(core)}


def build_sol_handoff(
    bundle: Mapping[str, Any], candidate: Mapping[str, Any], review: Mapping[str, Any]
) -> dict[str, Any]:
    trusted = review.get("candidate_trusted") is True
    core = {
        "schema_version": "sol_handoff_envelope_v1",
        "subject": bundle["subject"],
        "capture_id": bundle["capture_id"],
        "read_bundle_sha256": bundle["read_bundle_sha256"],
        "candidate_sha256": candidate["candidate_sha256"] if trusted else None,
        "review_sha256": review["review_sha256"],
        "risk_report_sha256": review["risk_report"]["risk_report_sha256"],
        "sol_review_ready": review["sol_review_ready"],
        "diagnostic_review_ready": review["diagnostic_review_ready"],
        "quality_clean": review["quality_clean"],
        "allowed_sol_actions": sorted(SOL_ACTIONS),
        "formal_apply_authorized": False,
        "formal_write_count": 0,
    }
    return {**core, "handoff_sha256": sha256_value(core)}
