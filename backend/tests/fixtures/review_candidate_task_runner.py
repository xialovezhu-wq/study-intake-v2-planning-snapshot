#!/usr/bin/env python3
"""Zero-external-call runner for the review-candidate terminal path."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from execution_quality_contract import decide_execution_quality  # noqa: E402


def canonical_file(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def publish(root: Path, value: dict[str, object]) -> tuple[str, Path]:
    payload = canonical_file(value)
    digest = hashlib.sha256(payload).hexdigest()
    path = root / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return digest, path


parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.parse_args()
request = json.loads(input())
task = request["task"]
unit = request["unit_sha256"]
fence = request["lease_fence"]
frozen = task["frozen_payload"]
subject = str(frozen["subject"])
disposition = str(frozen.get("test_report_disposition") or "needs_sol_review")
if disposition not in {"needs_sol_review", "quarantined"}:
    raise SystemExit("invalid fixture disposition")
runtime_root = Path(os.environ["STUDY_PREPROCESS_RUNTIME_ROOT"])


def build_stage(stage_role: str, *, has_finding: bool) -> dict[str, object]:
    provider_stage = f"{subject}_{stage_role}"
    stage_error = (
        f"{provider_stage}_runtime_identity_mismatch"
        if disposition == "quarantined"
        else f"{provider_stage}_semantic_validation_rejected"
    )
    raw_payload = json.dumps(
        {
            "candidate": "raw returned output",
            "stage_name": stage_role,
            "unit_sha256": unit,
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    raw_object = {
        "schema_version": "study-intake-model-stage-raw-output-v1",
        "stage_name": stage_role,
        "provider_stage_name": provider_stage,
        "raw_output_encoding": "base64",
        "raw_output_base64": base64.b64encode(raw_payload).decode("ascii"),
        "raw_output_sha256": hashlib.sha256(raw_payload).hexdigest(),
        "raw_output_size": len(raw_payload),
        "formal_write_count": 0,
    }
    raw_sha256, _raw_path = publish(
        runtime_root
        / "dispatch"
        / "model-stage-raw-outputs"
        / subject
        / unit,
        raw_object,
    )
    calls = [
        {
            "tool": "search_records",
            "status": "returned",
            "content_sha256": hashlib.sha256(
                f"mcp-content:{unit}:{stage_role}".encode()
            ).hexdigest(),
        }
    ]
    transcript = {
        "schema_version": "model-driven-mcp-stage-transcript-v1",
        "stage_name": provider_stage,
        "subject": subject,
        "read_session_id": f"REVIEW-{unit}",
        "read_session_manifest_sha256": hashlib.sha256(
            f"read-session:{unit}".encode()
        ).hexdigest(),
        "generation": f"{subject}-review-fixture",
        "authority_fingerprint": hashlib.sha256(
            f"authority:{unit}".encode()
        ).hexdigest(),
        "calls": calls,
        "coverage": {
            "call_count": 1,
            "all_returned_pages_consumed": True,
            "unresolved_next_cursors": [],
            "duplicate_argument_count": 0,
            "host_semantic_prefetch": False,
        },
        "semantic_stage_count": 1,
        "provider_request_count": 2,
        "mcp_tool_call_count": 1,
        "model_call_count": 1,
        "formal_write_count": 0,
    }
    transcript_sha256, _transcript_path = publish(
        runtime_root / "private" / "reports" / "mcp-stage-transcripts",
        transcript,
    )
    warnings = (
        [
            {
                "code": stage_error,
                "stage": provider_stage,
                "kind": (
                    "identity_anomaly"
                    if disposition == "quarantined"
                    else "semantic_validation_rejected"
                ),
            }
        ]
        if has_finding
        else []
    )
    execution_sha256 = hashlib.sha256(
        f"execution:{unit}:{stage_role}".encode()
    ).hexdigest()
    normalization_sha256 = hashlib.sha256(
        f"normalization:{unit}:{stage_role}".encode()
    ).hexdigest()
    return {
        "payload": {
            "candidate": "normalized returned output",
            "stage_name": stage_role,
            "unit_sha256": unit,
        },
        "runtime_model": (
            "unexpected-provider-model"
            if disposition == "quarantined"
            else "gpt-5.6-luna"
        ),
        "runtime_reasoning_effort": (
            "high" if disposition == "quarantined" else "max"
        ),
        "runtime_metadata_provenance": "codex_json_attestation_v1",
        "runtime_identity_status": (
            "quarantined" if disposition == "quarantined" else "confirmed"
        ),
        "duration_ms": 1,
        "semantic_stage_count": 1,
        "provider_request_count": 2,
        "mcp_tool_call_count": 1,
        "model_call_count": 1,
        "raw_output_object_sha256": raw_sha256,
        "raw_output_object_ref": (
            "study-intake-model-stage-raw-output://sha256/" + raw_sha256
        ),
        "stage_execution_receipt_sha256": execution_sha256,
        "stage_execution_receipt_ref": (
            "study-intake-model-stage-execution://sha256/" + execution_sha256
        ),
        "stage_normalization_receipt_sha256": normalization_sha256,
        "stage_normalization_receipt_ref": (
            "study-intake-model-stage-normalization://sha256/"
            + normalization_sha256
        ),
        "normalization_status": (
            "normalized_with_warnings" if warnings else "normalized"
        ),
        "normalization_warning_count": len(warnings),
        "normalization_warnings": warnings,
        "review_mcp_transcript_sha256": transcript_sha256,
        "review_mcp_transcript_ref": (
            "study-intake-mcp-stage-transcript://sha256/"
            + transcript_sha256
        ),
    }


quarantined = disposition == "quarantined"
finding_stage = "critical_review" if subject == "cs408" else "analysis"
error_code = (
    f"{subject}_analysis_runtime_identity_mismatch"
    if quarantined
    else f"{subject}_{finding_stage}_semantic_validation_rejected"
)
analysis_stage = build_stage(
    "analysis", has_finding=quarantined or finding_stage == "analysis"
)
critical_stage = (
    build_stage("critical_review", has_finding=True)
    if subject == "cs408" and not quarantined
    else None
)
decision = decide_execution_quality(
    model_completed=True,
    provider_completed=True,
    mcp_database_query_completed=True,
    raw_output_reopenable=True,
    report_reopenable=True,
    identity_verified=not quarantined,
    quality_findings_present=not quarantined,
    technical_error_code=error_code if quarantined else None,
    quarantined=quarantined,
)
print(
    json.dumps(
        {
            "schema_version": "study-intake-production-task-result-v1",
            "unit_sha256": unit,
            "lease_fence": fence,
            "analysis": analysis_stage,
            "critical_review": critical_stage,
            "terminal_outcome": decision.terminal_outcome,
            "terminal_error_code": decision.error_code,
            "terminal_quality_error_code": (
                error_code if not quarantined else None
            ),
            "terminal_report_disposition": disposition,
            "terminal_review_stage_count": 2 if critical_stage else 1,
            **decision.publication_fields(),
            "formal_write_count": 0,
        }
    )
)
