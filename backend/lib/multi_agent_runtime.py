"""Host-owned Multi-Agent V2 task lifecycle with injectable drivers."""

from __future__ import annotations

import copy
from typing import Any, Mapping, Sequence

from orchestration_plan import validate_read_plan
from read_bundle import (
    build_analysis_candidate,
    build_read_bundle,
    build_sol_handoff,
    review_candidate,
    sha256_value,
)
from read_fanout import ReadFanoutScheduler


class MultiAgentRuntimeError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class MultiAgentTaskRuntime:
    """Execute a validated plan; model-specific drivers stay outside Host logic."""

    def __init__(self, *, physical_slots: int) -> None:
        self.scheduler = ReadFanoutScheduler(physical_slots=physical_slots)

    def execute(
        self,
        *,
        plan: Mapping[str, Any],
        branch_worker: Any,
        reviewer_outcome: str,
        issue_codes: Sequence[str] = (),
        recommended_follow_up_reads: Sequence[str] = (),
        corrected_candidate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        validate_read_plan(plan)
        validated = copy.deepcopy(dict(plan))
        fanout = self.scheduler.run(validated, branch_worker)
        bundle = build_read_bundle(validated, fanout)
        candidate = build_analysis_candidate(bundle)
        review = review_candidate(
            bundle,
            candidate,
            outcome=reviewer_outcome,
            corrected_candidate=corrected_candidate,
            issue_codes=issue_codes,
            recommended_follow_up_reads=recommended_follow_up_reads,
        )
        handoff = build_sol_handoff(bundle, candidate, review)
        task_core = {
            "schema_version": "multi_agent_task_receipt_v1",
            "subject": validated["subject"],
            "capture_id": validated["capture_id"],
            "plan_sha256": validated["plan_sha256"],
            "stage_receipt_sha256s": [],
            "read_bundle_sha256": bundle["read_bundle_sha256"],
            "candidate_sha256": (
                candidate["candidate_sha256"]
                if review["candidate_trusted"] is True
                else None
            ),
            "review_sha256": review["review_sha256"],
            "risk_report_sha256": review["risk_report"][
                "risk_report_sha256"
            ],
            "handoff_sha256": handoff["handoff_sha256"],
            "execution_status": (
                "technical_quarantine"
                if reviewer_outcome == "technical_quarantine"
                else "succeeded"
            ),
            "sol_review_ready": review["sol_review_ready"],
            "quality_clean": review["quality_clean"],
            "formal_write_count": 0,
        }
        task_receipt = {
            **task_core,
            "receipt_sha256": sha256_value(task_core),
        }
        return {
            "plan": copy.deepcopy(dict(validated)),
            "fanout": fanout,
            "read_bundle": bundle,
            "candidate": candidate,
            "critical_review": review,
            "sol_handoff": handoff,
            "task_receipt": task_receipt,
            "formal_write_count": 0,
        }
