"""Pure shared execution-versus-quality terminal-state contract.

The contract deliberately owns no persistence, retry, or subject-specific
behavior.  Callers must prove the model, Provider, MCP database query, raw
output, report, and immutable identity before asking for a successful terminal
decision.  Quality findings never turn a technically complete execution into a
failure.
"""

from __future__ import annotations

from dataclasses import dataclass


EXECUTION_STATUSES = frozenset({"running", "succeeded", "failed"})
QUALITY_STATUSES = frozenset(
    {"unchecked", "passed", "issues_found", "needs_sol_review"}
)
MULTI_AGENT_QUALITY_STATUSES = frozenset(
    {"accepted", "corrected", "issues_found", "technical_quarantine"}
)


@dataclass(frozen=True)
class ExecutionQualityDecision:
    """One terminal projection shared by Math, English, and CS408."""

    execution_status: str
    quality_status: str
    report_available: bool
    report_disposition: str
    sol_review_status: str
    formal_write_eligible: bool
    production_accepted: bool
    error_code: str | None
    retry_allowed: bool

    @property
    def job_status(self) -> str:
        return self.execution_status

    @property
    def terminal_outcome(self) -> str:
        return self.execution_status

    def publication_fields(self) -> dict[str, object]:
        """Return the common persisted fields without subject-specific data."""

        return {
            "execution_status": self.execution_status,
            "quality_status": self.quality_status,
            "report_available": self.report_available,
            "report_disposition": self.report_disposition,
            "sol_review_status": self.sol_review_status,
            "formal_write_eligible": self.formal_write_eligible,
            "production_accepted": self.production_accepted,
        }


def decide_execution_quality(
    *,
    model_completed: bool,
    provider_completed: bool,
    mcp_database_query_completed: bool,
    raw_output_reopenable: bool,
    report_reopenable: bool,
    identity_verified: bool,
    quality_findings_present: bool,
    technical_error_code: str | None = None,
    quarantined: bool = False,
    technical_retry_allowed: bool = False,
) -> ExecutionQualityDecision:
    """Return the sole shared terminal decision for all three subjects.

    A quarantine is a technical identity/authority/evidence failure.  It may
    retain a reopenable report, but it is never a quality-only success.
    """

    proof_complete = all(
        (
            model_completed,
            provider_completed,
            mcp_database_query_completed,
            raw_output_reopenable,
            report_reopenable,
            identity_verified,
        )
    )
    if quarantined or technical_error_code is not None or not proof_complete:
        error_code = technical_error_code or "technical_execution_incomplete"
        return ExecutionQualityDecision(
            execution_status="failed",
            quality_status="unchecked",
            report_available=bool(report_reopenable),
            report_disposition=(
                "quarantined" if quarantined else "technical_failure"
            ),
            sol_review_status="not_eligible",
            formal_write_eligible=False,
            production_accepted=False,
            error_code=error_code,
            retry_allowed=bool(technical_retry_allowed and not quarantined),
        )
    if quality_findings_present:
        return ExecutionQualityDecision(
            execution_status="succeeded",
            quality_status="issues_found",
            report_available=True,
            report_disposition="needs_sol_review",
            sol_review_status="pending",
            formal_write_eligible=False,
            production_accepted=False,
            error_code=None,
            retry_allowed=False,
        )
    return ExecutionQualityDecision(
        execution_status="succeeded",
        quality_status="passed",
        report_available=True,
        report_disposition="accepted",
        sol_review_status="not_required",
        formal_write_eligible=False,
        production_accepted=False,
        error_code=None,
        retry_allowed=False,
    )


@dataclass(frozen=True)
class MultiAgentQualityDecision:
    execution_status: str
    quality_status: str
    sol_review_ready: bool
    diagnostic_review_ready: bool
    quality_clean: bool
    candidate_preserved: bool
    candidate_trusted: bool
    risk_report_preserved: bool
    automatic_retry: bool
    report_disposition: str
    formal_write_count: int = 0

    def publication_fields(self) -> dict[str, object]:
        return {
            "execution_status": self.execution_status,
            "quality_status": self.quality_status,
            "sol_review_ready": self.sol_review_ready,
            "diagnostic_review_ready": self.diagnostic_review_ready,
            "quality_clean": self.quality_clean,
            "candidate_preserved": self.candidate_preserved,
            "candidate_trusted": self.candidate_trusted,
            "risk_report_preserved": self.risk_report_preserved,
            "automatic_retry": self.automatic_retry,
            "report_disposition": self.report_disposition,
            "formal_write_count": self.formal_write_count,
        }


def decide_multi_agent_execution_quality(
    *,
    quality_status: str,
    technical_integrity_complete: bool,
) -> MultiAgentQualityDecision:
    """Return the V2 item-level decision without hiding issue packages."""

    if quality_status not in MULTI_AGENT_QUALITY_STATUSES:
        raise ValueError("multi_agent_quality_status_invalid")
    if not technical_integrity_complete and quality_status != "technical_quarantine":
        raise ValueError("technical_integrity_requires_quarantine")
    if quality_status == "technical_quarantine":
        return MultiAgentQualityDecision(
            execution_status="failed",
            quality_status=quality_status,
            sol_review_ready=False,
            diagnostic_review_ready=True,
            quality_clean=False,
            candidate_preserved=False,
            candidate_trusted=False,
            risk_report_preserved=True,
            automatic_retry=False,
            report_disposition="quarantined_diagnostic",
        )
    if quality_status == "issues_found":
        return MultiAgentQualityDecision(
            execution_status="succeeded",
            quality_status=quality_status,
            sol_review_ready=True,
            diagnostic_review_ready=True,
            quality_clean=False,
            candidate_preserved=True,
            candidate_trusted=True,
            risk_report_preserved=True,
            automatic_retry=False,
            report_disposition="needs_sol_review",
        )
    return MultiAgentQualityDecision(
        execution_status="succeeded",
        quality_status=quality_status,
        sol_review_ready=True,
        diagnostic_review_ready=True,
        quality_clean=True,
        candidate_preserved=True,
        candidate_trusted=True,
        risk_report_preserved=True,
        automatic_retry=False,
        report_disposition="accepted",
    )
