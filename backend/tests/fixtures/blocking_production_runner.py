#!/usr/bin/env python3
"""Protocol fake used to prove one production runner process per task."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.parse_args()
request = json.loads(input())
task = request["task"]
unit = request["unit_sha256"]
fence = request["lease_fence"]
initial_parent_pid = os.getppid()
payload = task["frozen_payload"]
subject = str(payload.get("subject") or "")
release_id = str(
    (payload.get("dispatch_contract") or {}).get("release_id") or ""
)
allowed_refs = payload.get("allowed_evidence_refs") or []
evidence_ref = str(allowed_refs[0]) if allowed_refs else None
read_session_id = f"ZERO-MODEL-FIXTURE-{subject}-{unit}"
context_root = os.environ.get("STUDY_PREPROCESS_CONTEXT_ROOT")
context_path = Path(context_root) if context_root else None
canary_fixture = isinstance(
    (payload.get("dispatch_contract") or {}).get("producer_input_contract"),
    dict,
)
event_store = None
event_task = None
event_lease = None
if canary_fixture:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
    from concurrent_dispatch import FrozenTask, Lease, LeaseStore

    event_store = LeaseStore(Path(os.environ["STUDY_PREPROCESS_RUNTIME_ROOT"]))
    event_task = FrozenTask.from_mapping(task)
    event_lease = Lease(
        unit,
        str(request["lease_owner_id"]),
        int(fence),
    )
private_context = payload.get("private_context")
behavior = payload.get("test_behavior") or (
    private_context.get("test_behavior")
    if isinstance(private_context, dict)
    else None
) or (
    (payload.get("model_input") or {}).get("test_behavior")
    if isinstance(payload.get("model_input"), dict)
    else None
) or (
    (payload.get("input_binding") or {}).get("test_behavior")
    if isinstance(payload.get("input_binding"), dict)
    else None
) or os.environ.get("BLOCKING_DEFAULT_BEHAVIOR") or "healthy"
if (
    os.environ.get("BLOCKING_FAIL_CAPTURE_ID")
    == str(payload.get("capture_id") or "")
):
    behavior = "fail_after_release"
marker_root = Path(os.environ["BLOCKING_MARKER_ROOT"])
marker_root.mkdir(parents=True, exist_ok=True)
(marker_root / f"{unit}.json").write_text(
    json.dumps(
        {
            "pid": os.getpid(),
            "pgid": os.getpgid(0),
            "parent_pid": initial_parent_pid,
            "unit_sha256": unit,
            "context_root": context_root,
            "mcp_session_root": (
                str(context_path / "mcp-session")
                if context_path is not None
                else None
            ),
            "report_root": (
                str(context_path / "reports")
                if context_path is not None
                else None
            ),
            "read_session_id": read_session_id,
            "behavior": behavior,
            "model_input_test_behavior": (
                (payload.get("model_input") or {}).get("test_behavior")
                if isinstance(payload.get("model_input"), dict)
                else None
            ),
            "input_binding_test_behavior": (
                (payload.get("input_binding") or {}).get("test_behavior")
                if isinstance(payload.get("input_binding"), dict)
                else None
            ),
            "fixture_scope": "temp_zero_model_only",
        }
    ),
    encoding="utf-8",
)
if event_store is not None:
    event_store.record_task_event(
        event_task, event_lease, "analysis_submitted"
    )
if behavior == "reported_timeout":
    print(
        json.dumps({"error_code": "cs408_critical_review_timeout"}),
        file=__import__("sys").stderr,
    )
    raise SystemExit(2)
if behavior == "reported_rate_limited":
    print(
        json.dumps({"error_code": "cs408_analysis_rate_limited"}),
        file=__import__("sys").stderr,
    )
    raise SystemExit(2)
if behavior == "reported_error":
    print(
        json.dumps({"error_code": str(payload["test_error_code"])}),
        file=__import__("sys").stderr,
    )
    raise SystemExit(2)
if behavior in {"block_all", "block_unit", "fail_after_release"}:
    release = (
        marker_root / "release-all"
        if behavior in {"block_all", "fail_after_release"}
        else marker_root / f"release-{unit}"
    )
    while not release.exists():
        # A supervisor can disappear while this fixture is waiting at the
        # test barrier.  Do not become an orphan that survives the test: the
        # parent PID is captured at launch and the fixture exits as soon as
        # re-parenting is observed.
        if os.getppid() != initial_parent_pid:
            raise SystemExit(0)
        time.sleep(0.01)
if behavior == "fail_after_release":
    print(
        json.dumps({"error_code": "synthetic_zero_model_fixture_failure"}),
        file=__import__("sys").stderr,
    )
    raise SystemExit(2)
if event_store is not None:
    event_store.record_task_event(
        event_task, event_lease, "analysis_completed"
    )
    event_store.record_task_event(
        event_task, event_lease, "critical_started"
    )
    event_store.record_task_event(
        event_task, event_lease, "critical_completed"
    )
stage = {
    "payload": {
        "stage": "analysis",
        "unit_sha256": unit,
        "fixture_scope": "temp_zero_model_only",
        "evidence_refs": [evidence_ref] if evidence_ref else [],
    },
    "runtime_model": "gpt-5.6-luna",
    "runtime_reasoning_effort": "max",
    "runtime_metadata_provenance": "codex_json_attestation_v1",
    "runtime_identity_status": "confirmed",
    "duration_ms": 1,
    "read_session_id": read_session_id if evidence_ref else None,
    "read_session_manifest_sha256": (
        hashlib.sha256(f"read-session:{read_session_id}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "authority_snapshot_manifest_sha256": (
        hashlib.sha256(f"authority-snapshot:{unit}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "capture_freeze_receipt_sha256": (
        hashlib.sha256(f"capture-freeze:{unit}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "mcp_read_session_receipt_sha256": (
        hashlib.sha256(f"read-session-receipt:{unit}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "evidence_generation": (
        f"{subject}-zero-model-fixture-generation" if evidence_ref else None
    ),
    "evidence_authority_fingerprint": (
        hashlib.sha256(f"authority:{subject}:zero-model-fixture".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "evidence_subject": subject if evidence_ref else None,
    "evidence_release_id": release_id if evidence_ref else None,
    "mcp_grounding_manifest_sha256": (
        hashlib.sha256(f"analysis:{unit}:{evidence_ref}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "mcp_transcript_sha256": (
        hashlib.sha256(f"analysis-transcript:{unit}".encode()).hexdigest()
        if evidence_ref
        else None
    ),
    "mcp_consumed_evidence_refs": [evidence_ref] if evidence_ref else [],
    "mcp_cited_evidence_refs": [evidence_ref] if evidence_ref else [],
    "mcp_stage_grounded_evidence_refs": [evidence_ref] if evidence_ref else [],
    "semantic_stage_count": 1,
    "provider_request_count": 2 if evidence_ref else 1,
    "mcp_tool_call_count": 1 if evidence_ref else 0,
    "model_call_count": 1,
}
critical = dict(stage)
critical["payload"] = {
    "stage": "critical_review",
    "unit_sha256": unit,
    "fixture_scope": "temp_zero_model_only",
    "evidence_refs": [evidence_ref] if evidence_ref else [],
}
payload_root = os.environ.get("ZERO_MODEL_STAGE_PAYLOAD_ROOT")
if payload_root:
    payload_path = Path(payload_root) / f"{unit}.json"
    exact_payloads = json.loads(payload_path.read_text(encoding="utf-8"))
    if (
        not isinstance(exact_payloads, dict)
        or not {"analysis", "critical_review"}.issubset(exact_payloads)
        or not set(exact_payloads).issubset(
            {"analysis", "critical_review", "stage_runtime"}
        )
        or not isinstance(exact_payloads["analysis"], dict)
        or not isinstance(exact_payloads["critical_review"], dict)
    ):
        raise SystemExit("zero_model_stage_payload_invalid")
    # These are exact copies of an already sealed local test publication.  Do
    # not add fixture markers to them: SubjectSol verifies their canonical
    # hashes byte-for-byte against the publication receipts.
    stage["payload"] = exact_payloads["analysis"]
    critical["payload"] = exact_payloads["critical_review"]
    stage_runtime = exact_payloads.get("stage_runtime")
    if stage_runtime is not None:
        if (
            not isinstance(stage_runtime, dict)
            or set(stage_runtime) != {"analysis", "critical_review"}
            or not all(
                isinstance(value, dict)
                for value in stage_runtime.values()
            )
        ):
            raise SystemExit("zero_model_stage_runtime_invalid")
        for target, name in (
            (stage, "analysis"),
            (critical, "critical_review"),
        ):
            runtime_row = stage_runtime[name]
            required = {
                "read_session_id",
                "read_session_manifest_sha256",
                "evidence_generation",
                "evidence_authority_fingerprint",
                "evidence_subject",
                "evidence_release_id",
                "mcp_grounding_manifest_sha256",
                "mcp_consumed_evidence_refs",
                "mcp_cited_evidence_refs",
                "mcp_stage_grounded_evidence_refs",
                "provider_request_count",
                "mcp_tool_call_count",
                "model_call_count",
            }
            if not required.issubset(runtime_row):
                raise SystemExit("zero_model_stage_runtime_incomplete")
            target.update(runtime_row)
critical["mcp_grounding_manifest_sha256"] = (
    hashlib.sha256(f"critical_review:{unit}:{evidence_ref}".encode()).hexdigest()
    if evidence_ref
    else None
)
critical["mcp_transcript_sha256"] = (
    hashlib.sha256(f"critical-review-transcript:{unit}".encode()).hexdigest()
    if evidence_ref
    else None
)
print(
    json.dumps(
        {
            "schema_version": "study-intake-production-task-result-v1",
            "unit_sha256": unit,
            "lease_fence": fence,
            "analysis": stage,
            "critical_review": critical,
            "formal_write_count": 0,
        }
    )
)
