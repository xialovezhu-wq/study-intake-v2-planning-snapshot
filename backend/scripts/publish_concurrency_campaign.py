#!/usr/bin/env python3
"""Publish a fail-closed asynchronous production-Canary Dashboard projection.

The current v2 publisher is read-only with respect to processing authority. It
derives subject state and concurrency from HMAC state, terminal indexes,
terminal receipts, task/provider process closures, MCP grounding, reports, and
packages. Mutable telemetry is only a cross-check. The legacy v1 helpers remain
private compatibility code for reopening historical evidence; the CLI cannot
publish v1 as current release evidence. Only caller-selected evidence output
and Dashboard state paths are written.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import tempfile
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "scripts"))

from dashboard.concurrency_campaign import (  # noqa: E402
    LEGACY_SCHEMA_VERSION as LEGACY_DASHBOARD_SCHEMA_VERSION,
    SCHEMA_VERSION as DASHBOARD_SCHEMA_VERSION,
    SUBJECT_MCP,
    SUBJECT_TIMEOUTS,
    campaign_contract_error,
)
from concurrent_dispatch import DispatchError, FrozenTask  # noqa: E402
from mixed_luna_stress import (  # noqa: E402
    MixedLunaStressError,
    STAGE_ORDER,
    verify_summary,
)
from preprocessor_core import (  # noqa: E402
    canonical_subject_package_path,
    safe_component,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    validate_subject_quality_receipt_v1,
)
import release_manager as release_manager  # noqa: E402


SUBJECTS = ("math", "cs408", "english")
TARGET_MODEL_REQUEST_CONTRACT = {
    "schema_version": "study-intake-model-request-contract-v2",
    "model": "gpt-5.6-luna",
    "reasoning_effort": "max",
    "service_tier_policy": "absent",
    "requested_service_tier": None,
    "fast_mode_requested": False,
    "fast_mode_effective": "not_requested",
}
PROVIDER_ARGV_POLICY_VERSION = (
    "study-intake-provider-argv-no-fast-mode-v1"
)
PROVIDER_ENVIRONMENT_POLICY_VERSION = (
    "study-intake-provider-environment-no-fast-mode-v1"
)
PROVIDER_EXECUTION_VERIFIED = "verified_no_fast_mode_argv_environment"
PROVIDER_EXECUTION_FIXTURE = "zero_model_fixture_not_applicable"
PROVIDER_EXECUTION_NOT_VERIFIED = "not_verified"
FROZEN_RUNTIME_SCHEMA_SHA256S = {
    "provider-process-identity-v1.json": (
        "181d727cae82a656d0db6bf9a3612c8b95012af22a7612fc4f26af67465c1556"
    ),
    "provider-process-exit-v1.json": (
        "6111eab50475c9d3459fc07b5605255c7aecb97fd9ac02e41e69ab9da20c5e8f"
    ),
    "task-process-identity-v1.json": (
        "7c98988d06ccd1f28d8397f0b1fb52b83cae26729942db68807c8e14aeae615b"
    ),
    "task-process-exit-v1.json": (
        "8d980a41669f6f3218aab911ba88c66a47fd60bb61c1cdb332babd70b9d2bb0e"
    ),
    "production-canary-terminal-receipt-v2.json": (
        "e5424bdca75f49a5cdd19ec52494f61757d7fdceae8ae666d844fb1969659019"
    ),
    "production-canary-terminal-index-v2.json": (
        "0064d485e7f38739aac69dec03fb497e0f7223abe9498807ed57b65ca441e1aa"
    ),
    "dispatch-report-v1.json": (
        "daf1b8ae9d6cf767a26147a8f8817e366c3adc275ba7888fff2b3f316508c812"
    ),
}
TASK_PROCESS_IDENTITY_KEYS = {
    "argv_sha256",
    "authority",
    "capture_id",
    "child_pgid",
    "child_pid",
    "context_root",
    "dispatcher_pid",
    "executable_path",
    "executable_sha256",
    "expected_mcp_session_root",
    "expected_report_root",
    "formal_write_count",
    "frozen_payload_sha256",
    "launch_nonce",
    "launched_at",
    "lease_fence",
    "model_call_count",
    "owner_id",
    "process_start_token",
    "provider_request_count",
    "release_id",
    "schema_version",
    "sol_enabled",
    "start_new_session",
    "subject",
    "unit_sha256",
}
TASK_PROCESS_EXIT_KEYS = {
    "authority",
    "capture_id",
    "child_pgid",
    "child_pid",
    "finished_at",
    "formal_write_count",
    "frozen_payload_sha256",
    "late_result_publish_allowed",
    "launch_nonce",
    "launched_at",
    "lease_fence",
    "model_call_count",
    "owner_id",
    "pgid_absent",
    "process_absent",
    "process_start_token",
    "provider_request_count",
    "reaped",
    "release_id",
    "returncode",
    "schema_version",
    "sol_enabled",
    "stderr_sha256",
    "stderr_size",
    "stdout_sha256",
    "stdout_size",
    "subject",
    "task_process_identity_path",
    "task_process_identity_sha256",
    "termination_reason",
    "unit_sha256",
}
PROVIDER_PROCESS_IDENTITY_KEYS = {
    "argv",
    "argv_policy_version",
    "argv_sha256",
    "authority",
    "capture_id",
    "context_root",
    "cwd",
    "environment_key_names",
    "environment_key_names_sha256",
    "environment_policy_version",
    "executable_path",
    "executable_sha256",
    "fast_mode_effective",
    "fast_mode_requested",
    "forbidden_environment_key_matches",
    "forbidden_environment_value_key_matches",
    "formal_write_count",
    "frozen_payload_sha256",
    "launch_nonce",
    "launched_at",
    "lease_fence",
    "owner_id",
    "process_start_token",
    "provider_pgid",
    "provider_pid",
    "provider_request_started",
    "release_id",
    "requested_service_tier",
    "role",
    "schema_version",
    "sol_enabled",
    "stage_name",
    "start_new_session",
    "subject",
    "supervisor_pgid",
    "supervisor_pid",
    "supervisor_process_identity_path",
    "supervisor_process_identity_sha256",
    "unit_sha256",
}
PROVIDER_PROCESS_EXIT_KEYS = {
    "authority",
    "capture_id",
    "finished_at",
    "formal_write_count",
    "frozen_payload_sha256",
    "late_result_publish_allowed",
    "lease_fence",
    "owner_id",
    "pgid_absent",
    "process_absent",
    "process_start_token",
    "provider_pgid",
    "provider_pid",
    "provider_process_identity_path",
    "provider_process_identity_sha256",
    "reaped",
    "release_id",
    "returncode",
    "schema_version",
    "sol_enabled",
    "stage_name",
    "subject",
    "termination_reason",
    "unit_sha256",
}
DISPATCH_REPORT_KEYS = {
    "analysis",
    "capture_id",
    "critical_review",
    "formal_write_count",
    "package_ref",
    "package_sha256",
    "release_id",
    "schema_version",
    "subject",
    "unit_sha256",
}
V2_EVIDENCE_SCOPES = {
    "real_production_hmac_v2",
    "zero_model_fixture_v2",
}
V2_CANARY_CONTROL_KEYS = {
    "producer_capture_enabled",
    "sol_formal_curation_enabled",
    "post_activation_only",
    "initial_canary_inflight_limit",
    "continuous_concurrency_limit",
    "keep_backlog_drained",
    "production_accepted",
    "requested_service_tier",
    "fast_mode_requested",
    "fast_mode_effective",
    "model_call_count",
    "provider_request_count",
    "mcp_tool_call_count",
    "formal_write_count",
    "sol_enabled",
}
V2_GLOBAL_ACTIVATION_KEYS = {
    "schema_version",
    "status",
    "activation_id",
    "release_id",
    "activated_at",
    "post_activation_only",
    "historical_backlog_drained",
    "initial_canary_inflight_limit",
    "continuous_concurrency_limit",
    "requested_service_tier",
    "fast_mode_requested",
    "fast_mode_effective",
    "producer_high_watermark_sha256s",
    "slots",
    "canary_manifest_sha256",
    "release_manifest_sha256",
    "pre_activation_verification_sha256",
    "post_activation_verification_sha256",
    "service_release_ids",
    "deployment_prepare_receipt_sha256",
    "model_call_count",
    "provider_request_count",
    "real_luna_runs",
    "formal_write_count",
    "sol_enabled",
    "production_accepted",
    "authority",
}
V2_TERMINAL_OUTCOMES = {
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "needs_rework",
}
READ_ONLY_MCP_TOOLS = {
    "get_task_context",
    "read_task_artifact",
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,199}$")
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_MARKDOWN_BYTES = 16 * 1024 * 1024
STAGE_CALL_KEYS = {
    "schema_version",
    "phase",
    "stage_name",
    "subject",
    "read_session_id",
    "read_session_manifest_sha256",
    "generation",
    "authority_fingerprint",
    "transcript_sha256",
    "calls",
    "pagination_coverage_complete",
    "duplicate_read_count",
    "consumed_terminal_duplicate_read_count",
    "host_semantic_prefetch",
    "failure_reason",
    "semantic_stage_count",
    "provider_request_count",
    "provider_request_count_status",
    "mcp_tool_call_count",
    "model_call_count",
    "formal_write_count",
    "created_at",
    "hmac_key_id",
    "hmac_sha256",
}
STAGE_CALL_ROW_KEYS = {
    "sequence",
    "tool",
    "arguments",
    "arguments_sha256",
    "result_sha256",
    "generation",
    "authority_fingerprint",
    "total_count",
    "returned_count",
    "offset",
    "next_cursor",
    "truncated",
    "complete",
}
FORMAL_GUARD_KEYS = {
    "schema_version",
    "release_id",
    "summary_sha256",
    "baseline_file_sha256",
    "config_file_sha256",
    "baseline_manifest_sha256",
    "before_manifest_sha256",
    "after_manifest_sha256",
    "captured_before_at",
    "captured_after_at",
    "formal_write_count",
    "hmac_key_id",
    "hmac_sha256",
}
DEPLOYMENT_AUTHORITY_KEYS = {
    "schema_version",
    "algorithm",
    "key_id",
    "purpose",
    "hmac_sha256",
}
DISPATCH_AUTHORITY_KEYS = {
    "schema_version",
    "algorithm",
    "key_id",
    "purpose",
    "hmac_sha256",
}
CANARY_ACTIVATION_KEYS = {
    "schema_version",
    "status",
    "activation_id",
    "release_id",
    "activated_at",
    "post_activation_only",
    "historical_backlog_drained",
    "per_subject_limit",
    "producer_high_watermark_sha256s",
    "slots",
    "canary_manifest_sha256",
    "release_manifest_sha256",
    "pre_activation_verification_sha256",
    "post_activation_verification_sha256",
    "service_release_ids",
    "deployment_prepare_receipt_sha256",
    "model_call_count",
    "provider_request_count",
    "real_luna_runs",
    "formal_write_count",
    "sol_enabled",
    "production_accepted",
    "authority",
}
CANARY_SLOT_KEYS = {
    "subject",
    "producer_high_watermark_sha256",
    "state",
    "capture_id",
    "completion_receipt_sha256",
}
CANARY_CONTROL_KEYS = {
    "producer_capture_enabled",
    "sol_formal_curation_enabled",
    "post_activation_only",
    "per_subject_limit",
    "keep_backlog_drained",
    "production_accepted",
    "fast_mode_requested",
    "fast_mode_effective",
    "model_call_count",
    "provider_request_count",
    "formal_write_count",
    "sol_enabled",
}
CANARY_SELECTED_KEYS = {
    "producer_unit_id",
    "producer_recorded_at",
    "producer_input_contract_sha256",
    "source_event_set_sha256",
    "unit_sha256",
    "frozen_payload_sha256",
}
SUBJECT_CANARY_ACTIVATION_KEYS = {
    "schema_version",
    "subject",
    "release_id",
    "activated_at",
    "producer_authority_fingerprint",
    "producer_high_watermark_sha256",
    "activation_id",
    "producer_authority",
    "producer_high_watermark",
    *CANARY_CONTROL_KEYS,
    "authority",
}
SUBJECT_CANARY_GATE_KEYS = {
    "schema_version",
    "activation_id",
    "subject",
    "release_id",
    "producer_authority_fingerprint",
    "producer_high_watermark_sha256",
    "activation_receipt_sha256",
    "activation_gate_authority_sha256",
    "state_before",
    "state_after",
    "selected",
    "admitted_at",
    *CANARY_CONTROL_KEYS,
    "authority",
}
SUBJECT_CANARY_TERMINAL_KEYS = {
    "schema_version",
    "activation_id",
    "subject",
    "release_id",
    "producer_authority_fingerprint",
    "producer_high_watermark_sha256",
    "activation_receipt_sha256",
    "activation_gate_authority_sha256",
    "canary_gate_sha256",
    "canary_gate_authority_sha256",
    "selected",
    "outcome",
    "error_code",
    "completion_path",
    "completion_sha256",
    "completion_receipt_path",
    "completion_receipt_sha256",
    "package_path",
    "package_sha256",
    "observed_model_call_count",
    "observed_provider_request_count",
    "observed_mcp_tool_call_count",
    "read_session_id",
    "evidence_generation",
    "evidence_authority_fingerprint",
    "task_declared_evidence_refs_sha256",
    "mcp_stage_grounding",
    "finished_at",
    "state_after",
    *CANARY_CONTROL_KEYS,
    "authority",
}
STAGE_GROUNDING_KEYS = {
    "read_session_id",
    "read_session_manifest_sha256",
    "evidence_generation",
    "evidence_authority_fingerprint",
    "mcp_grounding_manifest_sha256",
    "consumed_evidence_refs",
    "cited_evidence_refs",
    "grounded_evidence_refs",
    "effective_allowed_evidence_refs_sha256",
    "mcp_tool_call_count",
}
CANARY_QUEUE_SUCCESS_KEYS = {
    "schema_version",
    "activation_id",
    "subject",
    "release_id",
    "producer_high_watermark_sha256",
    "producer_authority_fingerprint",
    "producer_input_contract_sha256",
    "producer_unit_id",
    "producer_recorded_at",
    "source_event_set_sha256",
    "source_event_ids",
    "unit_sha256",
    "frozen_payload_sha256",
    "task_object_sha256",
    "task_object_path",
    "queue_status",
    "discovered_at",
    "claimed_at",
    "finished_at",
    "canary_gate_sha256",
    "canary_gate_path",
    "canary_gate_authority_sha256",
    "terminal_receipt_sha256",
    "terminal_receipt_path",
    "terminal_outcome",
    "terminal_error_code",
    "formal_write_count",
    "authority",
}


class CampaignPublishError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _fail(code: str) -> None:
    raise CampaignPublishError(code)


def _sha(value: object) -> bool:
    return isinstance(value, str) and SHA256_RE.fullmatch(value) is not None


def _aware_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value or len(value) > 100:
        return False
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _timestamp(value: object, code: str) -> dt.datetime:
    if not _aware_timestamp(value):
        _fail(code)
    assert isinstance(value, str)
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        dt.timezone.utc
    )


def _compact_bytes(value: object, *, newline: bool) -> bytes:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CampaignPublishError("campaign_value_not_canonical") from exc
    return raw + (b"\n" if newline else b"")


def _pretty_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError) as exc:
        raise CampaignPublishError("campaign_value_not_canonical") from exc


def _safe_root(path: Path, code: str) -> Path:
    if not path.is_absolute():
        _fail(code)
    try:
        resolved = path.resolve(strict=True)
        node = path.lstat()
    except OSError as exc:
        raise CampaignPublishError(code) from exc
    if resolved != path or path.is_symlink() or not stat.S_ISDIR(node.st_mode):
        _fail(code)
    return path


def _safe_regular_bytes(
    path: Path,
    *,
    root: Path,
    maximum: int,
    code: str,
) -> bytes:
    try:
        path.relative_to(root)
        if path.resolve(strict=True) != path:
            _fail(code)
    except (OSError, ValueError) as exc:
        raise CampaignPublishError(code) from exc
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum
        ):
            _fail(code)
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                _fail(code)
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(raw) != before.st_size
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            _fail(code)
        return raw
    except CampaignPublishError:
        raise
    except OSError as exc:
        raise CampaignPublishError(code) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _json_mapping(raw: bytes, code: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CampaignPublishError(code) from exc
    if not isinstance(value, Mapping):
        _fail(code)
    return copy.deepcopy(dict(value))


def _read_regular_json(path: Path, *, root: Path, code: str) -> dict[str, Any]:
    return _json_mapping(
        _safe_regular_bytes(path, root=root, maximum=MAX_JSON_BYTES, code=code),
        code,
    )


def _read_content_json(
    path: Path,
    *,
    root: Path,
    digest: str,
    encoding: str,
    code: str,
) -> dict[str, Any]:
    if not _sha(digest):
        _fail(code)
    raw = _safe_regular_bytes(path, root=root, maximum=MAX_JSON_BYTES, code=code)
    value = _json_mapping(raw, code)
    expected = {
        "pretty": _pretty_bytes(value),
        "compact_line": _compact_bytes(value, newline=True),
        "compact": _compact_bytes(value, newline=False),
    }.get(encoding)
    if expected is None or raw != expected or hashlib.sha256(raw).hexdigest() != digest:
        _fail(code)
    return value


def _read_named_canonical_receipt(
    path: Path, *, code: str
) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        _fail(code)
    root = _safe_root(path.parent, code)
    raw = _safe_regular_bytes(path, root=root, maximum=MAX_JSON_BYTES, code=code)
    value = _json_mapping(raw, code)
    digest = hashlib.sha256(raw).hexdigest()
    if raw != _compact_bytes(value, newline=False) or path.name != f"{digest}.json":
        _fail(code)
    return value, digest


def _read_key(path: Path, code: str) -> bytes:
    if not path.is_absolute():
        _fail(code)
    try:
        if path.resolve(strict=True) != path:
            _fail(code)
        node = path.lstat()
        key = path.read_bytes()
    except CampaignPublishError:
        raise
    except OSError as exc:
        raise CampaignPublishError(code) from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(node.st_mode)
        or stat.S_IMODE(node.st_mode) & 0o077
        or len(key) != 32
    ):
        _fail(code)
    return key


def _verify_candidate_release(root: Path, release_id: str) -> dict[str, Any]:
    if not _sha(release_id):
        _fail("campaign_candidate_release_invalid")
    manifest = _read_regular_json(
        root / "release.json", root=root, code="campaign_candidate_release_invalid"
    )
    contract = manifest.get("model_contract")
    test_results = manifest.get("test_results")
    formal = (
        test_results.get("formal_surface_gate")
        if isinstance(test_results, Mapping)
        else None
    )
    if (
        manifest.get("schema_version") != "study-intake-preprocessor-release-v2"
        or manifest.get("release_id") != release_id
        or not isinstance(contract, Mapping)
        or contract.get("model") != "gpt-5.6-luna"
        or contract.get("reasoning_effort") != "max"
        or contract.get("service_tier") != "priority"
        or not isinstance(formal, Mapping)
        or formal.get("status") != "passed"
        or formal.get("verification_status") != "unchanged"
        or any(
            not _sha(formal.get(field))
            for field in (
                "baseline_file_sha256",
                "config_file_sha256",
                "baseline_manifest_sha256",
                "current_manifest_sha256",
            )
        )
    ):
        _fail("campaign_candidate_release_invalid")
    return copy.deepcopy(dict(formal))


def _verify_canary_activation(
    path: Path,
    *,
    key: bytes,
    release_id: str,
    release_manifest_sha256: str,
) -> tuple[dict[str, Any], str]:
    receipt, digest = _read_named_canonical_receipt(
        path, code="campaign_canary_activation_invalid"
    )
    authority = receipt.get("authority")
    core = {
        name: copy.deepcopy(value)
        for name, value in receipt.items()
        if name != "authority"
    }
    expected_hmac = hmac.new(
        key, _compact_bytes(core, newline=False), hashlib.sha256
    ).hexdigest()
    watermarks = receipt.get("producer_high_watermark_sha256s")
    slots = receipt.get("slots")
    services = receipt.get("service_release_ids")
    if (
        set(receipt) != CANARY_ACTIVATION_KEYS
        or receipt.get("schema_version")
        != "study-intake-three-subject-canary-activation-receipt-v1"
        or receipt.get("status") != "production_canary_active"
        or not _sha(receipt.get("activation_id"))
        or receipt.get("release_id") != release_id
        or not _aware_timestamp(receipt.get("activated_at"))
        or receipt.get("post_activation_only") is not True
        or receipt.get("historical_backlog_drained") is not True
        or receipt.get("per_subject_limit") != 1
        or not isinstance(watermarks, Mapping)
        or set(watermarks) != set(SUBJECTS)
        or any(not _sha(watermarks.get(subject)) for subject in SUBJECTS)
        or not isinstance(slots, Mapping)
        or set(slots) != set(SUBJECTS)
        or not isinstance(services, Mapping)
        or set(services) != {"math", "cs408", "english", "dashboard"}
        or any(value != release_id for value in services.values())
        or any(
            not _sha(receipt.get(field))
            for field in (
                "canary_manifest_sha256",
                "release_manifest_sha256",
                "pre_activation_verification_sha256",
                "post_activation_verification_sha256",
                "deployment_prepare_receipt_sha256",
            )
        )
        or receipt.get("release_manifest_sha256") != release_manifest_sha256
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("real_luna_runs") != 0
        or receipt.get("formal_write_count") != 0
        or receipt.get("sol_enabled") is not False
        or receipt.get("production_accepted") is not False
        or not isinstance(authority, Mapping)
        or set(authority) != DEPLOYMENT_AUTHORITY_KEYS
        or authority.get("schema_version")
        != "study-intake-deployment-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("key_id") != hashlib.sha256(key).hexdigest()
        or authority.get("purpose")
        != "three-subject-production-canary-activation"
        or not hmac.compare_digest(
            str(authority.get("hmac_sha256") or ""), expected_hmac
        )
    ):
        _fail("campaign_canary_activation_invalid")
    for subject in SUBJECTS:
        slot = slots[subject]
        if (
            not isinstance(slot, Mapping)
            or set(slot) != CANARY_SLOT_KEYS
            or slot.get("subject") != subject
            or slot.get("producer_high_watermark_sha256")
            != watermarks[subject]
            or slot.get("state") != "armed"
            or slot.get("capture_id") is not None
            or slot.get("completion_receipt_sha256") is not None
        ):
            _fail("campaign_canary_activation_invalid")
    return receipt, digest


def _verify_dispatch_seal(
    value: Mapping[str, Any],
    *,
    key: bytes,
    purpose: str,
    code: str,
) -> None:
    authority = value.get("authority")
    core = {
        name: copy.deepcopy(child)
        for name, child in value.items()
        if name != "authority"
    }
    expected = hmac.new(
        key,
        _compact_bytes({"purpose": purpose, "payload": core}, newline=False),
        hashlib.sha256,
    ).hexdigest()
    if (
        not isinstance(authority, Mapping)
        or set(authority) != DISPATCH_AUTHORITY_KEYS
        or authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("key_id") != hashlib.sha256(key).hexdigest()
        or authority.get("purpose") != purpose
        or not hmac.compare_digest(
            str(authority.get("hmac_sha256") or ""), expected
        )
    ):
        _fail(code)


def _canary_controls_valid(value: Mapping[str, Any]) -> bool:
    return (
        value.get("producer_capture_enabled") is True
        and value.get("sol_formal_curation_enabled") is False
        and value.get("post_activation_only") is True
        and value.get("per_subject_limit") == 1
        and value.get("keep_backlog_drained") is True
        and value.get("production_accepted") is False
        and value.get("fast_mode_requested") is True
        and value.get("fast_mode_effective") == "requested_unverified"
        and value.get("model_call_count") == 0
        and value.get("provider_request_count") == 0
        and value.get("formal_write_count") == 0
        and value.get("sol_enabled") is False
    )


def _read_dispatch_content(
    path: Path,
    *,
    runtime_root: Path,
    digest: str,
    code: str,
    content_addressed: bool,
) -> dict[str, Any]:
    value = _read_content_json(
        path,
        root=runtime_root,
        digest=digest,
        encoding="compact_line",
        code=code,
    )
    if content_addressed and path.name != f"{digest}.json":
        _fail(code)
    return value


def _verify_subject_canary_activation(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    activation_id: str,
    digest: str,
    release_id: str,
    high_watermark_sha256: str,
) -> dict[str, Any]:
    path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / activation_id
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )
    receipt = _read_dispatch_content(
        path,
        runtime_root=runtime_root,
        digest=digest,
        code="campaign_subject_canary_activation_invalid",
        content_addressed=True,
    )
    _verify_dispatch_seal(
        receipt,
        key=key,
        purpose="dispatch-production-canary-activation",
        code="campaign_subject_canary_activation_invalid",
    )
    producer = receipt.get("producer_authority")
    watermark = receipt.get("producer_high_watermark")
    producer_core = (
        {
            name: copy.deepcopy(value)
            for name, value in producer.items()
            if name != "authority_fingerprint"
        }
        if isinstance(producer, Mapping)
        else None
    )
    if (
        set(receipt) != SUBJECT_CANARY_ACTIVATION_KEYS
        or receipt.get("schema_version")
        != "study-intake-production-canary-activation-receipt-v1"
        or receipt.get("subject") != subject
        or receipt.get("release_id") != release_id
        or receipt.get("activation_id") != activation_id
        or not _aware_timestamp(receipt.get("activated_at"))
        or receipt.get("producer_high_watermark_sha256")
        != high_watermark_sha256
        or not _sha(receipt.get("producer_authority_fingerprint"))
        or not isinstance(producer, Mapping)
        or producer.get("schema_version") != "study-intake-producer-authority-v1"
        or producer.get("subject") != subject
        or producer.get("release_id") != release_id
        or producer.get("model") != "gpt-5.6-luna"
        or producer.get("reasoning_effort") != "max"
        or producer.get("service_tier") != "priority"
        or producer.get("formal_write_count") != 0
        or producer.get("authority_fingerprint")
        != receipt.get("producer_authority_fingerprint")
        or producer.get("authority_fingerprint")
        != hashlib.sha256(_compact_bytes(producer_core, newline=False)).hexdigest()
        or not isinstance(watermark, Mapping)
        or watermark.get("schema_version")
        != "study-intake-producer-high-watermark-v1"
        or watermark.get("subject") != subject
        or watermark.get("release_id") != release_id
        or watermark.get("producer_authority_fingerprint")
        != receipt.get("producer_authority_fingerprint")
        or hashlib.sha256(_compact_bytes(watermark, newline=False)).hexdigest()
        != high_watermark_sha256
        or not _canary_controls_valid(receipt)
    ):
        _fail("campaign_subject_canary_activation_invalid")
    return receipt


def _verify_subject_canary_gate(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    digest = str(terminal.get("canary_gate_sha256") or "")
    activation_id = str(terminal.get("activation_id") or "")
    path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / activation_id
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )
    receipt = _read_dispatch_content(
        path,
        runtime_root=runtime_root,
        digest=digest,
        code="campaign_subject_canary_gate_invalid",
        content_addressed=True,
    )
    _verify_dispatch_seal(
        receipt,
        key=key,
        purpose="dispatch-production-canary-gate",
        code="campaign_subject_canary_gate_invalid",
    )
    selected = receipt.get("selected")
    authority = receipt.get("authority")
    state_before = receipt.get("state_before")
    expected_after = (
        "canary_in_flight"
        if state_before == "armed"
        else "continuous_consumption_unlocked"
    )
    if (
        set(receipt) != SUBJECT_CANARY_GATE_KEYS
        or receipt.get("schema_version")
        != "study-intake-production-canary-gate-receipt-v1"
        or receipt.get("activation_id") != activation_id
        or receipt.get("subject") != subject
        or receipt.get("release_id") != terminal.get("release_id")
        or receipt.get("producer_authority_fingerprint")
        != terminal.get("producer_authority_fingerprint")
        or receipt.get("producer_high_watermark_sha256")
        != terminal.get("producer_high_watermark_sha256")
        or receipt.get("activation_receipt_sha256")
        != terminal.get("activation_receipt_sha256")
        or receipt.get("activation_gate_authority_sha256")
        != terminal.get("activation_gate_authority_sha256")
        or state_before not in {"armed", "continuous_consumption_unlocked"}
        or receipt.get("state_after") != expected_after
        or not isinstance(selected, Mapping)
        or set(selected) != CANARY_SELECTED_KEYS
        or selected != terminal.get("selected")
        or not _aware_timestamp(receipt.get("admitted_at"))
        or not isinstance(authority, Mapping)
        or hashlib.sha256(_compact_bytes(authority, newline=False)).hexdigest()
        != terminal.get("canary_gate_authority_sha256")
        or not _canary_controls_valid(receipt)
    ):
        _fail("campaign_subject_canary_gate_invalid")
    return receipt


def _verify_canary_queue_task(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
    terminal_path: Path,
    terminal_sha256: str,
) -> dict[str, Any]:
    selected = terminal.get("selected")
    if not isinstance(selected, Mapping):
        _fail("campaign_subject_canary_task_invalid")
    contract_sha256 = str(
        selected.get("producer_input_contract_sha256") or ""
    )
    if not _sha(contract_sha256):
        _fail("campaign_subject_canary_task_invalid")
    queue_path = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-queue"
        / subject
        / f"{contract_sha256}.json"
    )
    queue_raw = _safe_regular_bytes(
        queue_path,
        root=runtime_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_subject_canary_queue_invalid",
    )
    queue = _json_mapping(queue_raw, "campaign_subject_canary_queue_invalid")
    _verify_dispatch_seal(
        queue,
        key=key,
        purpose="dispatch-production-canary-queue",
        code="campaign_subject_canary_queue_invalid",
    )
    gate_sha256 = str(terminal.get("canary_gate_sha256") or "")
    activation_id = str(terminal.get("activation_id") or "")
    gate_path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / activation_id
        / "sha256"
        / gate_sha256[:2]
        / f"{gate_sha256}.json"
    )
    if (
        queue_raw != _compact_bytes(queue, newline=True)
        or set(queue) != CANARY_QUEUE_SUCCESS_KEYS
        or queue.get("schema_version")
        != "study-intake-production-canary-queue-entry-v1"
        or queue.get("activation_id") != activation_id
        or queue.get("subject") != subject
        or queue.get("release_id") != terminal.get("release_id")
        or queue.get("producer_high_watermark_sha256")
        != terminal.get("producer_high_watermark_sha256")
        or queue.get("producer_authority_fingerprint")
        != terminal.get("producer_authority_fingerprint")
        or queue.get("producer_input_contract_sha256") != contract_sha256
        or queue.get("producer_unit_id")
        != selected.get("producer_unit_id")
        or queue.get("producer_recorded_at")
        != selected.get("producer_recorded_at")
        or queue.get("source_event_set_sha256")
        != selected.get("source_event_set_sha256")
        or queue.get("unit_sha256") != selected.get("unit_sha256")
        or queue.get("frozen_payload_sha256")
        != selected.get("frozen_payload_sha256")
        or queue.get("queue_status") != "succeeded"
        or not _aware_timestamp(queue.get("discovered_at"))
        or not _aware_timestamp(queue.get("claimed_at"))
        or queue.get("finished_at") != terminal.get("finished_at")
        or queue.get("canary_gate_sha256") != gate_sha256
        or queue.get("canary_gate_path") != str(gate_path)
        or queue.get("canary_gate_authority_sha256")
        != terminal.get("canary_gate_authority_sha256")
        or queue.get("terminal_receipt_sha256") != terminal_sha256
        or queue.get("terminal_receipt_path") != str(terminal_path)
        or queue.get("terminal_outcome") != "succeeded"
        or queue.get("terminal_error_code") is not None
        or queue.get("formal_write_count") != 0
    ):
        _fail("campaign_subject_canary_queue_invalid")

    task_sha256 = str(queue.get("task_object_sha256") or "")
    task_path = Path(str(queue.get("task_object_path") or ""))
    expected_task_path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "tasks"
        / subject
        / "sha256"
        / task_sha256[:2]
        / f"{task_sha256}.json"
    )
    task_object = _read_dispatch_content(
        task_path,
        runtime_root=runtime_root,
        digest=task_sha256,
        code="campaign_subject_canary_task_invalid",
        content_addressed=True,
    )
    try:
        task = FrozenTask.from_mapping(task_object)
    except DispatchError as exc:
        raise CampaignPublishError(
            "campaign_subject_canary_task_invalid"
        ) from exc
    payload = task.frozen_payload
    dispatch_contract = payload.get("dispatch_contract")
    producer_contract = (
        dispatch_contract.get("producer_input_contract")
        if isinstance(dispatch_contract, Mapping)
        else None
    )
    producer_core = (
        {
            name: copy.deepcopy(value)
            for name, value in producer_contract.items()
            if name != "producer_input_contract_sha256"
        }
        if isinstance(producer_contract, Mapping)
        else None
    )
    source_events = (
        producer_contract.get("source_events")
        if isinstance(producer_contract, Mapping)
        else None
    )
    allowed_refs = payload.get("allowed_evidence_refs")
    if (
        task_path != expected_task_path
        or set(task_object)
        != {
            "schema_version",
            "frozen_payload",
            "requested_model",
            "requested_reasoning_effort",
            "unit_sha256",
        }
        or task_object.get("schema_version") != "study-intake-frozen-task-v1"
        or task_object.get("requested_model") != "gpt-5.6-luna"
        or task_object.get("requested_reasoning_effort") != "max"
        or task_object != task.as_dict()
        or task.unit_sha256 != selected.get("unit_sha256")
        or task.frozen_payload_sha256
        != selected.get("frozen_payload_sha256")
        or payload.get("subject") != subject
        or payload.get("capture_id") != selected.get("producer_unit_id")
        or payload.get("recorded_at") != selected.get("producer_recorded_at")
        or not isinstance(dispatch_contract, Mapping)
        or dispatch_contract.get("release_id") != terminal.get("release_id")
        or dispatch_contract.get("requested_service_tier") != "priority"
        or dispatch_contract.get("fast_mode_requested") is not True
        or not isinstance(producer_contract, Mapping)
        or not isinstance(producer_core, Mapping)
        or producer_contract.get("producer_input_contract_sha256")
        != contract_sha256
        or hashlib.sha256(
            _compact_bytes(producer_core, newline=False)
        ).hexdigest()
        != contract_sha256
        or producer_contract.get("authority_fingerprint")
        != terminal.get("producer_authority_fingerprint")
        or producer_contract.get("source_event_set_sha256")
        != selected.get("source_event_set_sha256")
        or not isinstance(source_events, list)
        or not source_events
        or hashlib.sha256(
            _compact_bytes(source_events, newline=False)
        ).hexdigest()
        != selected.get("source_event_set_sha256")
        or not isinstance(allowed_refs, list)
        or not allowed_refs
        or not all(isinstance(ref, str) and ref for ref in allowed_refs)
        or len(set(allowed_refs)) != len(allowed_refs)
    ):
        _fail("campaign_subject_canary_task_invalid")
    declared_refs = sorted(set(allowed_refs))
    declared_refs_sha256 = hashlib.sha256(
        _compact_bytes(declared_refs, newline=False)
    ).hexdigest()
    if (
        terminal.get("task_declared_evidence_refs_sha256")
        != declared_refs_sha256
    ):
        _fail("campaign_subject_canary_task_invalid")
    return {
        "queue_state_sha256": hashlib.sha256(queue_raw).hexdigest(),
        "task_object_sha256": task_sha256,
        "allowed_evidence_refs": declared_refs,
        "task_declared_evidence_refs_sha256": declared_refs_sha256,
    }


def _verify_current_canary_state(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
) -> str:
    path = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary"
        / f"{subject}.json"
    )
    raw = _safe_regular_bytes(
        path,
        root=runtime_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_subject_canary_state_invalid",
    )
    state = _json_mapping(raw, "campaign_subject_canary_state_invalid")
    _verify_dispatch_seal(
        state,
        key=key,
        purpose="dispatch-production-canary-state",
        code="campaign_subject_canary_state_invalid",
    )
    if (
        raw != _compact_bytes(state, newline=True)
        or state.get("schema_version")
        != "study-intake-production-canary-state-v1"
        or state.get("status") != "production_canary_active"
        or state.get("state") != "continuous_consumption_unlocked"
        or state.get("subject") != subject
        or state.get("release_id") != terminal.get("release_id")
        or state.get("activation_id") != terminal.get("activation_id")
        or state.get("producer_authority_fingerprint")
        != terminal.get("producer_authority_fingerprint")
        or state.get("producer_high_watermark_sha256")
        != terminal.get("producer_high_watermark_sha256")
        or state.get("activation_receipt_sha256")
        != terminal.get("activation_receipt_sha256")
        or state.get("activation_gate_authority_sha256")
        != terminal.get("activation_gate_authority_sha256")
        or state.get("luna_consumer_enabled") is not True
        or state.get("active_task_count") != 0
        or state.get("unlocked_once") is not True
        or state.get("last_failure_at") is not None
        or state.get("last_emergency_cancel_at") is not None
        or state.get("last_emergency_cancel_receipt_sha256") is not None
        or state.get("late_result_fence_status") != "not_required"
        or state.get("last_preclaim_failure_at") is not None
        or state.get("last_preclaim_failure_stage") is not None
        or state.get("last_preclaim_failure_error_code") is not None
        or state.get("last_preclaim_failure_receipt_sha256") is not None
        or state.get("last_preclaim_failure_evidence_sha256") is not None
        or state.get("preclaim_failure_resume_ack_sha256") is not None
        or state.get("last_analysis_status") != "completed"
        or state.get("last_critical_review_status") != "completed"
        or state.get("last_report_status") != "reopen_verified"
        or int(state.get("observed_model_call_count") or 0)
        < int(terminal.get("observed_model_call_count") or 0)
        or int(state.get("observed_provider_request_count") or 0)
        < int(terminal.get("observed_provider_request_count") or 0)
        or int(state.get("observed_mcp_tool_call_count") or 0)
        < int(terminal.get("observed_mcp_tool_call_count") or 0)
        or not _canary_controls_valid(state)
    ):
        _fail("campaign_subject_canary_state_invalid")
    return hashlib.sha256(raw).hexdigest()


def _read_dispatch_bound_path(
    raw_path: object,
    *,
    runtime_root: Path,
    digest: object,
    code: str,
    content_addressed: bool,
) -> tuple[dict[str, Any], Path]:
    if not isinstance(raw_path, str) or not raw_path or not _sha(digest):
        _fail(code)
    path = Path(raw_path)
    value = _read_dispatch_content(
        path,
        runtime_root=runtime_root,
        digest=str(digest),
        code=code,
        content_addressed=content_addressed,
    )
    return value, path


def _verify_dispatch_completion_chain(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    completion, completion_path = _read_dispatch_bound_path(
        terminal.get("completion_path"),
        runtime_root=runtime_root,
        digest=terminal.get("completion_sha256"),
        code="campaign_dispatch_completion_invalid",
        content_addressed=False,
    )
    _verify_dispatch_seal(
        completion,
        key=key,
        purpose="dispatch-completion",
        code="campaign_dispatch_completion_invalid",
    )
    processing_receipt, processing_receipt_path = _read_dispatch_bound_path(
        terminal.get("completion_receipt_path"),
        runtime_root=runtime_root,
        digest=terminal.get("completion_receipt_sha256"),
        code="campaign_dispatch_completion_receipt_invalid",
        content_addressed=True,
    )
    _verify_dispatch_seal(
        processing_receipt,
        key=key,
        purpose="dispatch-receipt",
        code="campaign_dispatch_completion_receipt_invalid",
    )
    package, package_path = _read_dispatch_bound_path(
        terminal.get("package_path"),
        runtime_root=runtime_root,
        digest=terminal.get("package_sha256"),
        code="campaign_dispatch_package_invalid",
        content_addressed=True,
    )
    runtime = processing_receipt.get("observed_stage_runtime")
    if not isinstance(runtime, Mapping) or set(runtime) != {
        "analysis",
        "critical_review",
    }:
        _fail("campaign_dispatch_completion_receipt_invalid")
    totals = {
        "model": 0,
        "provider": 0,
        "mcp": 0,
    }
    for role in ("analysis", "critical_review"):
        row = runtime.get(role)
        if not isinstance(row, Mapping):
            _fail("campaign_dispatch_completion_receipt_invalid")
        for source, destination in (
            ("model_call_count", "model"),
            ("provider_request_count", "provider"),
            ("mcp_tool_call_count", "mcp"),
        ):
            value = row.get(source)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                _fail("campaign_dispatch_completion_receipt_invalid")
            totals[destination] += value
    unit_sha256 = terminal.get("selected", {}).get("unit_sha256")
    capture_id = terminal.get("selected", {}).get("producer_unit_id")
    common_invalid = (
        completion.get("unit_sha256") != unit_sha256
        or completion.get("subject") != subject
        or completion.get("capture_id") != capture_id
        or completion.get("release_id") != terminal.get("release_id")
        or completion.get("outcome") != "succeeded"
        or completion.get("receipt_sha256")
        != terminal.get("completion_receipt_sha256")
        or completion.get("receipt_path") != str(processing_receipt_path)
        or completion.get("package_sha256") != terminal.get("package_sha256")
        or completion.get("package_path") != str(package_path)
        or completion.get("finished_at") != terminal.get("finished_at")
        or processing_receipt.get("unit_sha256") != unit_sha256
        or processing_receipt.get("subject") != subject
        or processing_receipt.get("capture_id") != capture_id
        or processing_receipt.get("release_id") != terminal.get("release_id")
        or processing_receipt.get("outcome") != "succeeded"
        or processing_receipt.get("package_sha256")
        != terminal.get("package_sha256")
        or processing_receipt.get("package_path") != str(package_path)
        or processing_receipt.get("finished_at") != terminal.get("finished_at")
        or processing_receipt.get("formal_write_count") != 0
        or package.get("schema_version") != "study-intake-concurrent-package-v1"
        or package.get("unit_sha256") != unit_sha256
        or package.get("subject") != subject
        or package.get("capture_id") != capture_id
        or package.get("release_id") != terminal.get("release_id")
        or package.get("pipeline") != ["analysis", "critical_review"]
        or not isinstance(package.get("analysis"), Mapping)
        or not isinstance(package.get("critical_review"), Mapping)
        or package.get("formal_write_count") != 0
        or totals["model"] != terminal.get("observed_model_call_count")
        or totals["provider"]
        != terminal.get("observed_provider_request_count")
        or totals["mcp"] != terminal.get("observed_mcp_tool_call_count")
    )
    if common_invalid:
        _fail("campaign_dispatch_completion_invalid")
    return {
        "completion_path": str(completion_path),
        "completion_sha256": str(terminal["completion_sha256"]),
        "completion_receipt_path": str(processing_receipt_path),
        "completion_receipt_sha256": str(
            terminal["completion_receipt_sha256"]
        ),
        "dispatch_package_path": str(package_path),
        "dispatch_package_sha256": str(terminal["package_sha256"]),
        "observed_model_call_count": totals["model"],
        "observed_provider_request_count": totals["provider"],
        "observed_mcp_tool_call_count": totals["mcp"],
    }


def _verify_subject_canary_terminal(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    path: Path,
    release_id: str,
    global_activation: Mapping[str, Any],
) -> dict[str, Any]:
    if not path.is_absolute():
        _fail("campaign_subject_canary_terminal_invalid")
    raw = _safe_regular_bytes(
        path,
        root=runtime_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_subject_canary_terminal_invalid",
    )
    terminal = _json_mapping(raw, "campaign_subject_canary_terminal_invalid")
    digest = hashlib.sha256(raw).hexdigest()
    activation_id = terminal.get("activation_id")
    expected_path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / str(activation_id)
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )
    selected = terminal.get("selected")
    stage_grounding = terminal.get("mcp_stage_grounding")
    if (
        raw != _compact_bytes(terminal, newline=True)
        or path != expected_path
        or set(terminal) != SUBJECT_CANARY_TERMINAL_KEYS
        or terminal.get("schema_version")
        != "study-intake-production-canary-terminal-receipt-v1"
        or not _sha(activation_id)
        or terminal.get("subject") != subject
        or terminal.get("release_id") != release_id
        or terminal.get("producer_high_watermark_sha256")
        != global_activation["producer_high_watermark_sha256s"][subject]
        or not _sha(terminal.get("producer_authority_fingerprint"))
        or not _sha(terminal.get("activation_receipt_sha256"))
        or not _sha(terminal.get("activation_gate_authority_sha256"))
        or not _sha(terminal.get("canary_gate_sha256"))
        or not _sha(terminal.get("canary_gate_authority_sha256"))
        or not isinstance(selected, Mapping)
        or set(selected) != CANARY_SELECTED_KEYS
        or any(
            not _sha(selected.get(field))
            for field in (
                "producer_input_contract_sha256",
                "source_event_set_sha256",
                "unit_sha256",
                "frozen_payload_sha256",
            )
        )
        or not isinstance(selected.get("producer_unit_id"), str)
        or not SAFE_ID_RE.fullmatch(str(selected.get("producer_unit_id") or ""))
        or not _aware_timestamp(selected.get("producer_recorded_at"))
        or terminal.get("outcome") != "succeeded"
        or terminal.get("error_code") is not None
        or not _sha(terminal.get("completion_sha256"))
        or not _sha(terminal.get("completion_receipt_sha256"))
        or not _sha(terminal.get("package_sha256"))
        or not isinstance(terminal.get("read_session_id"), str)
        or not terminal.get("read_session_id")
        or not isinstance(terminal.get("evidence_generation"), str)
        or not terminal.get("evidence_generation")
        or not _sha(terminal.get("evidence_authority_fingerprint"))
        or not _sha(terminal.get("task_declared_evidence_refs_sha256"))
        or not isinstance(stage_grounding, Mapping)
        or set(stage_grounding) != {"analysis", "critical_review"}
        or not _aware_timestamp(terminal.get("finished_at"))
        or terminal.get("state_after") != "continuous_consumption_unlocked"
        or any(
            not isinstance(terminal.get(field), int)
            or isinstance(terminal.get(field), bool)
            or int(terminal[field]) < 1
            for field in (
                "observed_model_call_count",
                "observed_provider_request_count",
                "observed_mcp_tool_call_count",
            )
        )
        or not _canary_controls_valid(terminal)
    ):
        _fail("campaign_subject_canary_terminal_invalid")
    _verify_dispatch_seal(
        terminal,
        key=key,
        purpose="dispatch-production-canary-terminal",
        code="campaign_subject_canary_terminal_invalid",
    )
    subject_activation = _verify_subject_canary_activation(
        runtime_root,
        key,
        subject=subject,
        activation_id=str(activation_id),
        digest=str(terminal["activation_receipt_sha256"]),
        release_id=release_id,
        high_watermark_sha256=str(
            terminal["producer_high_watermark_sha256"]
        ),
    )
    activation_authority = subject_activation["authority"]
    if (
        terminal.get("activation_gate_authority_sha256")
        != hashlib.sha256(
            _compact_bytes(activation_authority, newline=False)
        ).hexdigest()
    ):
        _fail("campaign_subject_canary_activation_invalid")
    gate = _verify_subject_canary_gate(
        runtime_root, key, subject=subject, terminal=terminal
    )
    chain = _verify_dispatch_completion_chain(
        runtime_root, key, subject=subject, terminal=terminal
    )
    queue_task = _verify_canary_queue_task(
        runtime_root,
        key,
        subject=subject,
        terminal=terminal,
        terminal_path=path,
        terminal_sha256=digest,
    )
    state_sha256 = _verify_current_canary_state(
        runtime_root, key, subject=subject, terminal=terminal
    )
    global_activated = _timestamp(
        global_activation.get("activated_at"),
        "campaign_subject_canary_timeline_invalid",
    )
    subject_activated = _timestamp(
        subject_activation.get("activated_at"),
        "campaign_subject_canary_timeline_invalid",
    )
    produced = _timestamp(
        selected.get("producer_recorded_at"),
        "campaign_subject_canary_timeline_invalid",
    )
    admitted = _timestamp(
        gate.get("admitted_at"), "campaign_subject_canary_timeline_invalid"
    )
    finished = _timestamp(
        terminal.get("finished_at"),
        "campaign_subject_canary_timeline_invalid",
    )
    if not global_activated <= subject_activated < produced <= admitted <= finished:
        _fail("campaign_subject_canary_timeline_invalid")
    return {
        "terminal": terminal,
        "terminal_sha256": digest,
        "terminal_hmac_sha256": terminal["authority"]["hmac_sha256"],
        "subject_activation_sha256": terminal["activation_receipt_sha256"],
        "subject_activation_hmac_sha256": subject_activation["authority"][
            "hmac_sha256"
        ],
        "canary_gate_sha256": terminal["canary_gate_sha256"],
        "canary_gate_hmac_sha256": gate["authority"]["hmac_sha256"],
        "canary_gate_authority_sha256": terminal[
            "canary_gate_authority_sha256"
        ],
        "canary_state_sha256": state_sha256,
        **queue_task,
        **chain,
    }


def _verify_formal_surface_guard(
    path: Path,
    *,
    key: bytes,
    release_id: str,
    summary_sha256: str,
    release_formal_gate: Mapping[str, Any],
) -> dict[str, Any]:
    if not path.is_absolute():
        _fail("campaign_formal_surface_guard_invalid")
    root = _safe_root(path.parent, "campaign_formal_surface_guard_invalid")
    guard = _read_regular_json(
        path, root=root, code="campaign_formal_surface_guard_invalid"
    )
    core = {
        name: copy.deepcopy(value)
        for name, value in guard.items()
        if name not in {"hmac_key_id", "hmac_sha256"}
    }
    expected_hmac = hmac.new(
        key,
        _compact_bytes(
            {
                "purpose": "concurrency-formal-surface-guard-v1",
                "payload": core,
            },
            newline=True,
        ),
        hashlib.sha256,
    ).hexdigest()
    if (
        set(guard) != FORMAL_GUARD_KEYS
        or guard.get("schema_version")
        != "study-intake-concurrency-formal-surface-guard-v1"
        or guard.get("release_id") != release_id
        or guard.get("summary_sha256") != summary_sha256
        or guard.get("baseline_file_sha256")
        != release_formal_gate.get("baseline_file_sha256")
        or guard.get("config_file_sha256")
        != release_formal_gate.get("config_file_sha256")
        or guard.get("baseline_manifest_sha256")
        != release_formal_gate.get("baseline_manifest_sha256")
        or guard.get("before_manifest_sha256")
        != release_formal_gate.get("current_manifest_sha256")
        or guard.get("after_manifest_sha256")
        != guard.get("before_manifest_sha256")
        or any(
            not _sha(guard.get(field))
            for field in (
                "baseline_file_sha256",
                "config_file_sha256",
                "baseline_manifest_sha256",
                "before_manifest_sha256",
                "after_manifest_sha256",
            )
        )
        or not _aware_timestamp(guard.get("captured_before_at"))
        or not _aware_timestamp(guard.get("captured_after_at"))
        or guard.get("formal_write_count") != 0
        or guard.get("hmac_key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(str(guard.get("hmac_sha256") or ""), expected_hmac)
    ):
        _fail("campaign_formal_surface_guard_invalid")
    return guard


def _verify_summary_gate(summary: Mapping[str, Any], release_id: str) -> None:
    peaks = summary.get("per_subject_peak_active")
    sol = summary.get("sol_control")
    sol_evidence = sol.get("global_state_evidence") if isinstance(sol, Mapping) else None
    if (
        summary.get("status") != "passed"
        or summary.get("candidate_release_id") != release_id
        or summary.get("run_mode") != "smoke_3"
        or summary.get("selected_task_count") != 3
        or summary.get("distinct_task_count") != 3
        or summary.get("distinct_unit_count") != 3
        or summary.get("subject_counts")
        != {"english": 1, "math": 1, "cs408": 1}
        or summary.get("barrier_expected") != 3
        or summary.get("barrier_arrived") != 3
        or not isinstance(summary.get("global_peak_active"), int)
        or int(summary.get("global_peak_active") or 0) < 3
        or not isinstance(peaks, Mapping)
        or set(peaks) != set(SUBJECTS)
        or any(not isinstance(peaks.get(subject), int) or int(peaks[subject]) < 1 for subject in SUBJECTS)
        or not isinstance(summary.get("submitted_at_spread_ms"), int)
        or not 0 <= int(summary["submitted_at_spread_ms"]) <= 2000
        or summary.get("stage_submission_count") != 6
        or summary.get("model_call_count") != 6
        or summary.get("formal_write_count") != 0
        or not isinstance(sol, Mapping)
        or sol.get("sol_enabled") is not False
        or not isinstance(sol_evidence, Mapping)
        or sol_evidence.get("unchanged") is not True
        or sol_evidence.get("formal_write_count_delta") != 0
    ):
        _fail("campaign_summary_acceptance_invalid")


def _verify_quality_receipt(
    runtime_root: Path,
    key: bytes,
    digest: str,
    *,
    subject: str,
    result: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    path = (
        runtime_root
        / "dispatch"
        / "control-receipts"
        / "subject-quality"
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )
    receipt = _read_content_json(
        path,
        root=runtime_root,
        digest=digest,
        encoding="compact_line",
        code="campaign_quality_receipt_invalid",
    )
    try:
        checked = validate_subject_quality_receipt_v1(receipt)
    except SubjectSolContractError as exc:
        raise CampaignPublishError("campaign_quality_receipt_invalid") from exc
    core = dict(checked)
    seal = core.pop("seal", None)
    expected = hmac.new(
        key,
        _compact_bytes(
            {"purpose": "subject-quality-receipt", "payload": core},
            newline=False,
        ),
        hashlib.sha256,
    ).hexdigest()
    authority = checked.get("authority")
    if (
        not isinstance(seal, Mapping)
        or seal.get("algorithm") != "HMAC-SHA256"
        or seal.get("purpose") != "subject-quality-receipt"
        or not hmac.compare_digest(str(seal.get("hmac_sha256") or ""), expected)
        or checked.get("subject") != subject
        or checked.get("unit_sha256") != result.get("unit_sha256")
        or checked.get("package_sha256")
        != result.get("closure", {}).get("package_sha256")
        or checked.get("model_call_count") != 2
        or checked.get("formal_write_count") != 0
        or checked.get("review_outcome") not in {"accepted", "corrected"}
        or not isinstance(authority, Mapping)
        or authority.get("generation") != binding.get("generation")
        or authority.get("authority_fingerprint")
        != binding.get("authority_fingerprint")
    ):
        _fail("campaign_quality_receipt_invalid")
    return checked


def _verify_stage_closure(
    runtime_root: Path, digest: str, *, code: str
) -> dict[str, Any]:
    return _read_content_json(
        runtime_root
        / "dispatch"
        / "control-receipts"
        / "subject-stage-closures"
        / "sha256"
        / digest[:2]
        / f"{digest}.json",
        root=runtime_root,
        digest=digest,
        encoding="compact",
        code=code,
    )


def _load_bound_package(
    runtime_root: Path,
    *,
    release_id: str,
    subject: str,
    quality: Mapping[str, Any],
) -> dict[str, Any]:
    capture_id = quality.get("capture_id")
    if not isinstance(capture_id, str) or not SAFE_ID_RE.fullmatch(capture_id):
        _fail("campaign_capture_identity_invalid")
    component = safe_component(capture_id)
    job = _read_regular_json(
        runtime_root / "state" / "jobs" / subject / f"{component}.json",
        root=runtime_root,
        code="campaign_subject_job_invalid",
    )
    pointer = _read_regular_json(
        runtime_root / "state" / "latest" / subject / f"{component}.json",
        root=runtime_root,
        code="campaign_subject_pointer_invalid",
    )
    package_sha = str(quality.get("package_sha256") or "")
    study_date = job.get("study_date")
    input_fingerprint = job.get("input_fingerprint")
    if (
        job.get("schema_version") != "study-intake-preprocess-job-v1"
        or job.get("subject") != subject
        or job.get("capture_id") != capture_id
        or job.get("package_sha256") != package_sha
        or job.get("formal_write_count") != 0
        or job.get("status") not in {"ready", "two_pass_ready"}
        or pointer.get("subject") != subject
        or pointer.get("capture_id") != capture_id
        or pointer.get("package_sha256") != package_sha
        or pointer.get("authority_release_id") != release_id
        or pointer.get("formal_write_count") not in {None, 0}
        or not isinstance(study_date, str)
        or not _sha(input_fingerprint)
    ):
        _fail("campaign_subject_publication_invalid")
    try:
        package_path = canonical_subject_package_path(
            runtime_root,
            subject=subject,
            study_date=study_date,
            capture_id=capture_id,
            input_fingerprint=str(input_fingerprint),
            package_sha256=package_sha,
        )
    except Exception as exc:
        raise CampaignPublishError("campaign_subject_package_invalid") from exc
    if job.get("package_path") != str(package_path) or pointer.get("package_path") != str(
        package_path
    ):
        _fail("campaign_subject_package_invalid")
    package = _read_content_json(
        package_path,
        root=runtime_root,
        digest=package_sha,
        encoding="pretty",
        code="campaign_subject_package_invalid",
    )
    if (
        package.get("subject") != subject
        or package.get("capture_id") != capture_id
        or package.get("study_date") != study_date
        or package.get("input_fingerprint") != input_fingerprint
        or package.get("pipeline_status") != "two_pass_ready"
        or package.get("formal_write_count") != 0
        or package.get("model_call_count") != 2
    ):
        _fail("campaign_subject_package_invalid")
    return package


def _verify_stage_call_receipt(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    role: str,
    closure: Mapping[str, Any],
    binding: Mapping[str, Any],
    package_stage: Mapping[str, Any],
) -> tuple[int, int, str]:
    digest_field = f"{role}_stage_receipt_sha256"
    hmac_field = f"{role}_stage_receipt_hmac_sha256"
    transcript_field = f"{role}_transcript_sha256"
    digest = str(closure.get(digest_field) or "")
    receipt = _read_content_json(
        runtime_root
        / "dispatch"
        / "mcp-read-session-call-receipts"
        / "sha256"
        / digest[:2]
        / f"{digest}.json",
        root=runtime_root,
        digest=digest,
        encoding="compact_line",
        code="campaign_stage_call_receipt_invalid",
    )
    core = {
        name: copy.deepcopy(value)
        for name, value in receipt.items()
        if name not in {"hmac_key_id", "hmac_sha256"}
    }
    expected_hmac = hmac.new(
        key,
        _compact_bytes(
            {"purpose": "mcp-read-session-model-calls-v1", "payload": core},
            newline=True,
        ),
        hashlib.sha256,
    ).hexdigest()
    calls = receipt.get("calls")
    prompt_version = package_stage.get("prompt_version")
    if (
        set(receipt) != STAGE_CALL_KEYS
        or receipt.get("schema_version") != "mcp_stage_call_receipt_v1"
        or receipt.get("phase") != "model_stage_calls"
        or receipt.get("stage_name") != prompt_version
        or receipt.get("subject") != subject
        or receipt.get("read_session_id") != closure.get("read_session_id")
        or receipt.get("read_session_manifest_sha256")
        != closure.get("read_session_manifest_sha256")
        or receipt.get("generation") != binding.get("generation")
        or receipt.get("authority_fingerprint")
        != binding.get("authority_fingerprint")
        or receipt.get("transcript_sha256") != closure.get(transcript_field)
        or receipt.get("transcript_sha256")
        != package_stage.get("mcp_transcript_sha256")
        or package_stage.get("mcp_call_receipt_sha256") != digest
        or package_stage.get("mcp_call_receipt_ref")
        != f"study-intake-mcp-read-session-call://sha256/{digest}"
        or package_stage.get("status") != "ready"
        or package_stage.get("pagination_coverage_complete") is not True
        or package_stage.get("formal_write_count") != 0
        or not isinstance(calls, list)
        or not calls
        or receipt.get("mcp_tool_call_count") != len(calls)
        or package_stage.get("mcp_tool_call_count") != len(calls)
        or receipt.get("provider_request_count") != len(calls) + 1
        or package_stage.get("provider_request_count") != len(calls) + 1
        or receipt.get("provider_request_count_status")
        != "derived_from_codex_tool_loop"
        or receipt.get("pagination_coverage_complete") is not True
        or receipt.get("duplicate_read_count") != 0
        or receipt.get("consumed_terminal_duplicate_read_count") != 0
        or receipt.get("host_semantic_prefetch") is not False
        or receipt.get("failure_reason") is not None
        or receipt.get("semantic_stage_count") != 1
        or receipt.get("model_call_count") != 1
        or receipt.get("formal_write_count") != 0
        or receipt.get("hmac_key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(str(receipt.get("hmac_sha256") or ""), expected_hmac)
        or receipt.get("hmac_sha256") != closure.get(hmac_field)
    ):
        _fail("campaign_stage_call_receipt_invalid")
    seen_reads: set[tuple[str, str]] = set()
    for index, call in enumerate(calls, start=1):
        arguments = call.get("arguments") if isinstance(call, Mapping) else None
        tool = call.get("tool") if isinstance(call, Mapping) else None
        arguments_sha256 = (
            call.get("arguments_sha256")
            if isinstance(call, Mapping)
            else None
        )
        read_identity = (str(tool), str(arguments_sha256))
        if (
            not isinstance(call, Mapping)
            or set(call) != STAGE_CALL_ROW_KEYS
            or call.get("sequence") != index
            or tool not in READ_ONLY_MCP_TOOLS
            or not isinstance(arguments, Mapping)
            or not _sha(arguments_sha256)
            or hashlib.sha256(
                _compact_bytes(arguments, newline=False)
            ).hexdigest()
            != arguments_sha256
            or read_identity in seen_reads
            or not _sha(call.get("result_sha256"))
            or call.get("generation") != binding.get("generation")
            or call.get("authority_fingerprint")
            != binding.get("authority_fingerprint")
        ):
            _fail("campaign_stage_call_receipt_invalid")
        seen_reads.add(read_identity)
    return len(calls), len(calls) + 1, digest


def _verify_final_read_session(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    closure: Mapping[str, Any],
    binding: Mapping[str, Any],
    package: Mapping[str, Any],
    stage_rows: list[dict[str, Any]],
) -> None:
    stage_receipts = package.get("stage_receipts")
    embedded = (
        stage_receipts.get("read_session")
        if isinstance(stage_receipts, Mapping)
        else None
    )
    digest = str(closure.get("final_read_session_receipt_sha256") or "")
    receipt = _read_content_json(
        runtime_root
        / "dispatch"
        / "mcp-read-session-receipts"
        / "sha256"
        / digest[:2]
        / f"{digest}.json",
        root=runtime_root,
        digest=digest,
        encoding="compact_line",
        code="campaign_final_read_session_invalid",
    )
    core = {
        name: copy.deepcopy(value)
        for name, value in receipt.items()
        if name not in {"hmac_key_id", "hmac_sha256"}
    }
    expected_hmac = hmac.new(
        key,
        _compact_bytes(
            {"purpose": "mcp-read-session-receipt-v1-final", "payload": core},
            newline=True,
        ),
        hashlib.sha256,
    ).hexdigest()
    expected_stage_rows = [
        {
            "stage": row["stage"],
            "mcp_call_receipt_sha256": row["receipt_sha256"],
            "mcp_transcript_sha256": row["transcript_sha256"],
            "mcp_tool_call_count": row["mcp_tool_call_count"],
            "provider_request_count": row["provider_request_count"],
        }
        for row in stage_rows
    ]
    if (
        not isinstance(embedded, Mapping)
        or embedded.get("status") != "complete"
        or embedded.get("receipt_sha256") != digest
        or embedded.get("receipt_ref")
        != f"study-intake-mcp-read-session://sha256/{digest}"
        or embedded.get("receipt") != receipt
        or receipt.get("schema_version") != "mcp_read_session_receipt_v1"
        or receipt.get("phase") != "complete"
        or receipt.get("subject") != subject
        or receipt.get("read_session_id") != closure.get("read_session_id")
        or receipt.get("read_session_manifest_sha256")
        != closure.get("read_session_manifest_sha256")
        or receipt.get("generation") != binding.get("generation")
        or receipt.get("authority_fingerprint")
        != binding.get("authority_fingerprint")
        or receipt.get("stage_call_receipts") != expected_stage_rows
        or receipt.get("model_mcp_tool_call_count")
        != sum(row["mcp_tool_call_count"] for row in stage_rows)
        or receipt.get("provider_request_count")
        != sum(row["provider_request_count"] for row in stage_rows)
        or receipt.get("provider_request_count_status")
        != "derived_from_codex_tool_loop"
        or receipt.get("pagination_coverage_complete") is not True
        or receipt.get("formal_write_count") != 0
        or receipt.get("hmac_key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(str(receipt.get("hmac_sha256") or ""), expected_hmac)
    ):
        _fail("campaign_final_read_session_invalid")


def _verify_report(
    runtime_root: Path,
    *,
    package: Mapping[str, Any],
    quality: Mapping[str, Any],
) -> tuple[str, str, str | None]:
    critical_sha = str(quality.get("critical_review_output_sha256") or "")
    critical = _verify_stage_closure(
        runtime_root, critical_sha, code="campaign_report_invalid"
    )
    analysis_sha = str(quality.get("analysis_output_sha256") or "")
    _verify_stage_closure(runtime_root, analysis_sha, code="campaign_report_invalid")
    report_sha = package.get("report_json_sha256")
    if report_sha is not None:
        markdown_sha = package.get("report_markdown_sha256")
        embedded_quality = package.get("quality_receipt")
        if (
            not _sha(report_sha)
            or package.get("report_json_ref")
            != f"study-intake-report://sha256/{report_sha}"
            or not _sha(markdown_sha)
            or package.get("report_markdown_ref")
            != f"study-intake-report-markdown://sha256/{markdown_sha}"
            or not isinstance(embedded_quality, Mapping)
            or package.get("quality_receipt_sha256")
            != hashlib.sha256(
                _compact_bytes(embedded_quality, newline=False)
            ).hexdigest()
            or embedded_quality.get("formal_write_count") != 0
        ):
            _fail("campaign_report_invalid")
        _read_content_json(
            runtime_root / "private" / "reports" / "objects" / f"{report_sha}.json",
            root=runtime_root,
            digest=str(report_sha),
            encoding="pretty",
            code="campaign_report_invalid",
        )
        raw_markdown = _safe_regular_bytes(
            runtime_root
            / "private"
            / "reports"
            / "markdown"
            / f"{markdown_sha}.md",
            root=runtime_root,
            maximum=MAX_MARKDOWN_BYTES,
            code="campaign_report_invalid",
        )
        if hashlib.sha256(raw_markdown).hexdigest() != markdown_sha:
            _fail("campaign_report_invalid")
        selected_sha = str(report_sha)
        selected_markdown_sha: str | None = str(markdown_sha)
    else:
        critical_payload = package.get("critical_review")
        if (
            package.get("schema_version") != "study-intake-preprocess-package-v1"
            or not isinstance(critical_payload, Mapping)
            or _compact_bytes(critical_payload, newline=False)
            != _compact_bytes(critical, newline=False)
        ):
            _fail("campaign_report_invalid")
        selected_sha = critical_sha
        selected_markdown_sha = None
    return (
        f"report:sha256:{selected_sha}",
        selected_sha,
        selected_markdown_sha,
    )


def _mcp_refs(value: object, subject: str) -> list[str]:
    prefix = f"mcp-item:{subject}:"
    found: set[str] = set()
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, Mapping):
            pending.extend(current.values())
        elif isinstance(current, (list, tuple)):
            pending.extend(current)
        elif isinstance(current, str) and current.startswith(prefix):
            found.add(current)
    return sorted(found)


def _verify_terminal_grounding(
    *,
    subject: str,
    terminal: Mapping[str, Any],
    canary: Mapping[str, Any],
    closure: Mapping[str, Any],
    binding: Mapping[str, Any],
    package: Mapping[str, Any],
    package_stages: Mapping[str, Any],
    stage_rows: list[dict[str, Any]],
    stage_payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    grounding = terminal.get("mcp_stage_grounding")
    publications = package.get("mcp_stage_transcripts")
    allowed_refs = canary.get("allowed_evidence_refs")
    if (
        not isinstance(grounding, Mapping)
        or set(grounding) != {"analysis", "critical_review"}
        or not isinstance(publications, Mapping)
        or set(publications) != {"analysis", "critical_review"}
        or not isinstance(allowed_refs, list)
        or package.get("read_session_id") != terminal.get("read_session_id")
        or package.get("read_session_id") != closure.get("read_session_id")
        or package.get("read_session_manifest_sha256")
        != closure.get("read_session_manifest_sha256")
        or package.get("evidence_generation")
        != terminal.get("evidence_generation")
        or package.get("evidence_generation") != binding.get("generation")
        or package.get("evidence_authority_fingerprint")
        != terminal.get("evidence_authority_fingerprint")
        or package.get("evidence_authority_fingerprint")
        != binding.get("authority_fingerprint")
        or sorted(set(package.get("allowed_evidence_refs") or []))
        != sorted(set(allowed_refs))
    ):
        _fail("campaign_subject_mcp_grounding_invalid")
    prior_consumed: set[str] = set()
    source: dict[str, Any] = {}
    for index, role in enumerate(("analysis", "critical_review")):
        row = grounding.get(role)
        package_stage = package_stages.get(role)
        publication = publications.get(role)
        if not all(
            isinstance(value, Mapping)
            for value in (row, package_stage, publication)
        ):
            _fail("campaign_subject_mcp_grounding_invalid")
        assert isinstance(row, Mapping)
        assert isinstance(package_stage, Mapping)
        assert isinstance(publication, Mapping)
        consumed = row.get("consumed_evidence_refs")
        cited = row.get("cited_evidence_refs")
        grounded = row.get("grounded_evidence_refs")
        if not all(isinstance(value, list) for value in (consumed, cited, grounded)):
            _fail("campaign_subject_mcp_grounding_invalid")
        consumed_set = set(consumed)
        cited_set = set(cited)
        grounded_set = set(grounded)
        effective = set(allowed_refs) | prior_consumed | consumed_set
        expected_effective_sha = hashlib.sha256(
            _compact_bytes(sorted(effective), newline=False)
        ).hexdigest()
        expected_cited = _mcp_refs(stage_payloads[role], subject)
        if (
            set(row) != STAGE_GROUNDING_KEYS
            or row.get("read_session_id") != terminal.get("read_session_id")
            or row.get("read_session_manifest_sha256")
            != closure.get("read_session_manifest_sha256")
            or row.get("evidence_generation") != binding.get("generation")
            or row.get("evidence_authority_fingerprint")
            != binding.get("authority_fingerprint")
            or package_stage.get("read_session_id") != row.get("read_session_id")
            or package_stage.get("read_session_manifest_sha256")
            != row.get("read_session_manifest_sha256")
            or package_stage.get("evidence_generation")
            != row.get("evidence_generation")
            or package_stage.get("evidence_authority_fingerprint")
            != row.get("evidence_authority_fingerprint")
            or package_stage.get("mcp_grounding_manifest_sha256")
            != row.get("mcp_grounding_manifest_sha256")
            or publication.get("grounding_manifest_sha256")
            != row.get("mcp_grounding_manifest_sha256")
            or publication.get("grounding_refs") != sorted(consumed_set)
            or row.get("mcp_tool_call_count")
            != stage_rows[index]["mcp_tool_call_count"]
            or any(
                not isinstance(ref, str) or not ref
                for ref in consumed_set | cited_set | grounded_set
            )
            or list(consumed) != sorted(consumed_set)
            or list(cited) != sorted(cited_set)
            or list(grounded) != sorted(grounded_set)
            or sorted(cited_set) != expected_cited
            or not grounded_set
            or grounded_set != consumed_set.intersection(cited_set)
            or not cited_set.issubset(effective)
            or row.get("effective_allowed_evidence_refs_sha256")
            != expected_effective_sha
        ):
            _fail("campaign_subject_mcp_grounding_invalid")
        prior_consumed.update(consumed_set)
        source[role] = {
            "mcp_grounding_manifest_sha256": row[
                "mcp_grounding_manifest_sha256"
            ],
            "grounded_evidence_refs": sorted(grounded_set),
        }
    return source


def _subject_projection(
    runtime_root: Path,
    key: bytes,
    *,
    release_id: str,
    subject: str,
    result: Mapping[str, Any],
    trace: Mapping[str, Any],
    binding: Mapping[str, Any],
    canary: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    closure = result.get("closure")
    events = trace.get("stage_events")
    if (
        result.get("status") != "succeeded"
        or result.get("subject") != subject
        or result.get("model_call_count") != 2
        or result.get("formal_write_count") != 0
        or not isinstance(closure, Mapping)
        or not isinstance(events, list)
        or [event.get("name") for event in events if isinstance(event, Mapping)]
        != list(STAGE_ORDER)
        or trace.get("strict_stage_order_verified") is not True
        or any(
            not isinstance(event, Mapping)
            or not _aware_timestamp(event.get("observed_at"))
            for event in events
        )
    ):
        _fail("campaign_subject_stage_invalid")
    quality_sha = str(closure.get("quality_receipt_sha256") or "")
    quality = _verify_quality_receipt(
        runtime_root,
        key,
        quality_sha,
        subject=subject,
        result=result,
        binding=binding,
    )
    stage_payloads = {
        "analysis": _verify_stage_closure(
            runtime_root,
            str(quality["analysis_output_sha256"]),
            code="campaign_analysis_output_invalid",
        ),
        "critical_review": _verify_stage_closure(
            runtime_root,
            str(quality["critical_review_output_sha256"]),
            code="campaign_critical_review_output_invalid",
        ),
    }
    package = _load_bound_package(
        runtime_root,
        release_id=release_id,
        subject=subject,
        quality=quality,
    )
    package_stages = package.get("stage_receipts")
    if not isinstance(package_stages, Mapping):
        _fail("campaign_subject_package_invalid")
    stage_rows: list[dict[str, Any]] = []
    for role, package_name in (
        ("analysis", "analysis"),
        ("critical_review", "critical_review"),
    ):
        package_stage = package_stages.get(package_name)
        if not isinstance(package_stage, Mapping):
            _fail("campaign_subject_package_invalid")
        calls, providers, receipt_sha = _verify_stage_call_receipt(
            runtime_root,
            key,
            subject=subject,
            role=role,
            closure=closure,
            binding=binding,
            package_stage=package_stage,
        )
        stage_rows.append(
            {
                "stage": package_name,
                "receipt_sha256": receipt_sha,
                "transcript_sha256": closure[f"{role}_transcript_sha256"],
                "mcp_tool_call_count": calls,
                "provider_request_count": providers,
            }
        )
    _verify_final_read_session(
        runtime_root,
        key,
        subject=subject,
        closure=closure,
        binding=binding,
        package=package,
        stage_rows=stage_rows,
    )
    grounding_source = _verify_terminal_grounding(
        subject=subject,
        terminal=canary["terminal"],
        canary=canary,
        closure=closure,
        binding=binding,
        package=package,
        package_stages=package_stages,
        stage_rows=stage_rows,
        stage_payloads=stage_payloads,
    )
    mcp_calls = sum(row["mcp_tool_call_count"] for row in stage_rows)
    provider_requests = sum(row["provider_request_count"] for row in stage_rows)
    if (
        mcp_calls <= 0
        or provider_requests <= 0
        or package.get("mcp_tool_call_count") != mcp_calls
        or package.get("provider_request_count") != provider_requests
    ):
        _fail("campaign_subject_counter_invalid")
    report_ref, report_sha, report_markdown_sha = _verify_report(
        runtime_root, package=package, quality=quality
    )
    package_sha = str(closure.get("package_sha256") or "")
    capture_id = str(quality["capture_id"])
    terminal = canary.get("terminal")
    selected = terminal.get("selected") if isinstance(terminal, Mapping) else None
    if (
        not isinstance(selected, Mapping)
        or selected.get("unit_sha256") != result.get("unit_sha256")
        or selected.get("producer_unit_id") != capture_id
        or selected.get("frozen_payload_sha256")
        != quality.get("frozen_payload_sha256")
        or canary.get("observed_model_call_count") != 2
        or canary.get("observed_provider_request_count") != provider_requests
        or canary.get("observed_mcp_tool_call_count") != mcp_calls
        or _timestamp(
            events[-1].get("observed_at"),
            "campaign_subject_canary_timeline_invalid",
        )
        > _timestamp(
            terminal.get("finished_at"),
            "campaign_subject_canary_timeline_invalid",
        )
    ):
        _fail("campaign_subject_canary_completion_binding_invalid")
    projection = {
        "subject": subject,
        "capture_id": capture_id,
        "stage": "report_verified",
        "stage_timeout_seconds": SUBJECT_TIMEOUTS[subject],
        "mcp_namespace": SUBJECT_MCP[subject],
        "mcp_canonical_call_count": mcp_calls,
        "analysis_status": "completed",
        "critical_review_status": "completed",
        "report_status": "verified",
        "report_ref": report_ref,
        "report_sha256": report_sha,
        "package_ref": f"package:sha256:{package_sha}",
        "package_sha256": package_sha,
        "model_call_count": 2,
        "provider_request_count": provider_requests,
        "error_code": None,
        "started_at": events[0]["observed_at"],
        "completed_at": events[-1]["observed_at"],
    }
    source_binding = {
        "subject": subject,
        "unit_sha256": result["unit_sha256"],
        "mcp_authority_fingerprint": binding["authority_fingerprint"],
        "quality_receipt_sha256": quality_sha,
        "analysis_stage_call_receipt_sha256": stage_rows[0]["receipt_sha256"],
        "critical_review_stage_call_receipt_sha256": stage_rows[1][
            "receipt_sha256"
        ],
        "final_read_session_receipt_sha256": closure[
            "final_read_session_receipt_sha256"
        ],
        "report_sha256": report_sha,
        "report_markdown_sha256": report_markdown_sha,
        "package_sha256": package_sha,
        "physical_reopen_sha256s": {
            "quality_receipt": quality_sha,
            "report": report_sha,
            "report_markdown": report_markdown_sha,
            "package": package_sha,
            "dispatch_completion": canary["completion_sha256"],
            "dispatch_completion_receipt": canary[
                "completion_receipt_sha256"
            ],
            "dispatch_package": canary["dispatch_package_sha256"],
        },
        "production_canary": {
            "terminal_receipt_sha256": canary["terminal_sha256"],
            "terminal_receipt_hmac_sha256": canary[
                "terminal_hmac_sha256"
            ],
            "subject_activation_receipt_sha256": canary[
                "subject_activation_sha256"
            ],
            "subject_activation_receipt_hmac_sha256": canary[
                "subject_activation_hmac_sha256"
            ],
            "canary_gate_sha256": canary["canary_gate_sha256"],
            "canary_gate_hmac_sha256": canary[
                "canary_gate_hmac_sha256"
            ],
            "canary_gate_authority_sha256": canary[
                "canary_gate_authority_sha256"
            ],
            "dispatch_completion_sha256": canary["completion_sha256"],
            "dispatch_completion_receipt_sha256": canary[
                "completion_receipt_sha256"
            ],
            "dispatch_package_sha256": canary[
                "dispatch_package_sha256"
            ],
            "canary_state_sha256": canary["canary_state_sha256"],
            "queue_state_sha256": canary["queue_state_sha256"],
            "task_object_sha256": canary["task_object_sha256"],
            "task_declared_evidence_refs_sha256": canary[
                "task_declared_evidence_refs_sha256"
            ],
            "mcp_stage_grounding": grounding_source,
        },
        "model_call_count": 2,
        "mcp_canonical_call_count": mcp_calls,
        "provider_request_count": provider_requests,
        "formal_write_count": 0,
    }
    return projection, source_binding


def _build_legacy_projection_v1(
    *,
    campaign_runtime_root: Path,
    campaign_authority_key: Path,
    summary_sha256: str,
    processing_runtime_root: Path,
    processing_authority_key: Path,
    candidate_release_root: Path,
    candidate_release_id: str,
    formal_surface_guard: Path,
    deployment_authority_key: Path,
    canary_activation_receipt: Path,
    dispatch_authority_key: Path,
    canary_terminal_receipts: Mapping[str, Path],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    campaign_root = _safe_root(
        campaign_runtime_root, "campaign_summary_runtime_invalid"
    )
    processing_root = _safe_root(
        processing_runtime_root, "campaign_processing_runtime_invalid"
    )
    release_root = _safe_root(
        candidate_release_root, "campaign_candidate_release_invalid"
    )
    _read_key(campaign_authority_key, "campaign_summary_authority_key_invalid")
    processing_key = _read_key(
        processing_authority_key, "campaign_processing_authority_key_invalid"
    )
    deployment_key = _read_key(
        deployment_authority_key, "campaign_deployment_authority_key_invalid"
    )
    dispatch_key = _read_key(
        dispatch_authority_key, "campaign_dispatch_authority_key_invalid"
    )
    release_formal_gate = _verify_candidate_release(
        release_root, candidate_release_id
    )
    release_manifest_raw = _safe_regular_bytes(
        release_root / "release.json",
        root=release_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_candidate_release_invalid",
    )
    global_activation, global_activation_sha256 = _verify_canary_activation(
        canary_activation_receipt,
        key=deployment_key,
        release_id=candidate_release_id,
        release_manifest_sha256=hashlib.sha256(release_manifest_raw).hexdigest(),
    )
    try:
        summary = verify_summary(
            campaign_root, campaign_authority_key, summary_sha256
        )
    except MixedLunaStressError as exc:
        raise CampaignPublishError("campaign_summary_hmac_invalid") from exc
    _verify_summary_gate(summary, candidate_release_id)
    if (
        set(canary_terminal_receipts) != set(SUBJECTS)
        or _timestamp(
            global_activation.get("activated_at"),
            "campaign_canary_timeline_invalid",
        )
        > _timestamp(
            summary.get("submitted_at_first"),
            "campaign_canary_timeline_invalid",
        )
    ):
        _fail("campaign_canary_terminal_set_invalid")
    canary_results = {
        subject: _verify_subject_canary_terminal(
            processing_root,
            dispatch_key,
            subject=subject,
            path=canary_terminal_receipts[subject],
            release_id=candidate_release_id,
            global_activation=global_activation,
        )
        for subject in SUBJECTS
    }
    summary_completed = _timestamp(
        summary.get("completed_at"), "campaign_canary_timeline_invalid"
    )
    if any(
        _timestamp(
            canary_results[subject]["terminal"].get("finished_at"),
            "campaign_canary_timeline_invalid",
        )
        > summary_completed
        for subject in SUBJECTS
    ):
        _fail("campaign_canary_timeline_invalid")
    formal_guard = _verify_formal_surface_guard(
        formal_surface_guard,
        key=processing_key,
        release_id=candidate_release_id,
        summary_sha256=summary_sha256,
        release_formal_gate=release_formal_gate,
    )
    results = summary.get("results")
    traces = summary.get("item_traces")
    bindings = summary.get("subject_bindings")
    if (
        not isinstance(results, list)
        or not isinstance(traces, list)
        or not isinstance(bindings, Mapping)
    ):
        _fail("campaign_summary_subjects_invalid")
    result_by_subject = {
        row.get("subject"): row for row in results if isinstance(row, Mapping)
    }
    trace_by_subject = {
        row.get("subject"): row for row in traces if isinstance(row, Mapping)
    }
    if (
        set(result_by_subject) != set(SUBJECTS)
        or set(trace_by_subject) != set(SUBJECTS)
        or set(bindings) != set(SUBJECTS)
    ):
        _fail("campaign_summary_subjects_invalid")
    subject_results = {
        subject: _subject_projection(
            processing_root,
            processing_key,
            release_id=candidate_release_id,
            subject=subject,
            result=result_by_subject[subject],
            trace=trace_by_subject[subject],
            binding=bindings[subject],
            canary=canary_results[subject],
        )
        for subject in SUBJECTS
    }
    subjects = {
        subject: subject_results[subject][0] for subject in SUBJECTS
    }
    subject_sources = {
        subject: subject_results[subject][1] for subject in SUBJECTS
    }
    model_count = sum(row["model_call_count"] for row in subjects.values())
    provider_count = sum(
        row["provider_request_count"] for row in subjects.values()
    )
    projection = {
        "schema_version": LEGACY_DASHBOARD_SCHEMA_VERSION,
        "generated_at": summary["completed_at"],
        "campaign_id": summary["campaign_id"],
        "release_id": candidate_release_id,
        "status": "passed",
        "result_label": "three_subject_concurrency_runtime_accepted",
        "production_accepted": False,
        "barrier": {
            "expected": 3,
            "arrived": summary["barrier_arrived"],
            "global_peak_active": summary["global_peak_active"],
            "per_subject_peak_active": {
                subject: summary["per_subject_peak_active"][subject]
                for subject in SUBJECTS
            },
            "submitted_at_spread_ms": summary["submitted_at_spread_ms"],
            "released_at": summary["barrier_released_at"],
            "completed_at": summary["completed_at"],
        },
        "fast_mode": {
            "requested": True,
            "service_tier": "priority",
            # The release proves the request flag only.  No provider service-tier
            # attestation schema exists in this release, so confirmation is forbidden.
            "effective_status": "requested_unverified",
        },
        "safety": {
            "provider_execution": provider_count > 0,
            "model_call_count": model_count,
            "provider_request_count": provider_count,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        "canary": {
            "activation_id": global_activation["activation_id"],
            "activated_at": global_activation["activated_at"],
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "per_subject_limit": 1,
            "slots": {
                subject: {
                    "subject": subject,
                    "producer_high_watermark_sha256": global_activation[
                        "producer_high_watermark_sha256s"
                    ][subject],
                    "state": "unlocked",
                    "capture_id": subjects[subject]["capture_id"],
                    "completion_receipt_sha256": canary_results[subject][
                        "terminal_sha256"
                    ],
                }
                for subject in SUBJECTS
            },
        },
        "subjects": subjects,
    }
    contract_error = campaign_contract_error(projection)
    if contract_error is not None:
        raise CampaignPublishError(contract_error)
    summary_path = (
        campaign_root
        / "dispatch"
        / "mixed-luna-stress"
        / "summaries"
        / "sha256"
        / summary_sha256[:2]
        / f"{summary_sha256}.json"
    )
    source_binding = {
        "schema_version": "study-intake-concurrency-source-binding-v1",
        "status": "verified",
        "campaign_id": summary["campaign_id"],
        "release_id": candidate_release_id,
        "generated_at": summary["completed_at"],
        "mixed_summary": {
            "path": str(summary_path),
            "sha256": summary_sha256,
            "hmac_key_id": summary["hmac_key_id"],
            "hmac_sha256": summary["hmac_sha256"],
            "hmac_verified": True,
        },
        "production_canary": {
            "global_activation_receipt_sha256": global_activation_sha256,
            "global_activation_receipt_hmac_sha256": global_activation[
                "authority"
            ]["hmac_sha256"],
            "global_activation_receipt_key_id": global_activation[
                "authority"
            ]["key_id"],
            "activation_id": global_activation["activation_id"],
            "activated_at": global_activation["activated_at"],
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "per_subject_limit": 1,
            "producer_high_watermark_sha256s": copy.deepcopy(
                global_activation["producer_high_watermark_sha256s"]
            ),
            "terminal_receipt_sha256s": {
                subject: canary_results[subject]["terminal_sha256"]
                for subject in SUBJECTS
            },
        },
        "subjects": subject_sources,
        "formal_surface_guard": {
            "baseline_file_sha256": formal_guard["baseline_file_sha256"],
            "config_file_sha256": formal_guard["config_file_sha256"],
            "baseline_manifest_sha256": formal_guard[
                "baseline_manifest_sha256"
            ],
            "before_manifest_sha256": formal_guard[
                "before_manifest_sha256"
            ],
            "after_manifest_sha256": formal_guard["after_manifest_sha256"],
            "guard_hmac_sha256": formal_guard["hmac_sha256"],
        },
        "real_luna_run_count": 3,
        "formal_write_count": 0,
        "sol_enabled": False,
        "production_accepted": False,
    }
    return projection, source_binding, formal_guard


def _executable_binding_v2(path: Path, *, code: str) -> dict[str, str]:
    if not path.is_absolute():
        _fail(code)
    try:
        resolved = path.expanduser().resolve(strict=True)
        node = resolved.stat()
    except OSError as exc:
        raise CampaignPublishError(code) from exc
    if (
        not stat.S_ISREG(node.st_mode)
        or node.st_size <= 0
        or node.st_size > 1024 * 1024 * 1024
        or not os.access(resolved, os.X_OK)
    ):
        _fail(code)
    digest = hashlib.sha256()
    try:
        with resolved.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise CampaignPublishError(code) from exc
    return {"path": str(resolved), "sha256": digest.hexdigest()}


def _verify_candidate_release_v2(
    root: Path, release_id: str, *, evidence_scope: str
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    if not _sha(release_id):
        _fail("campaign_candidate_release_v2_invalid")
    manifest_path = root / "release.json"
    manifest_raw = _safe_regular_bytes(
        manifest_path,
        root=root,
        maximum=MAX_JSON_BYTES,
        code="campaign_candidate_release_v2_invalid",
    )
    manifest = _json_mapping(
        manifest_raw, "campaign_candidate_release_v2_invalid"
    )
    config = _read_regular_json(
        root / "config.json",
        root=root,
        code="campaign_candidate_release_v2_invalid",
    )
    model = config.get("model")
    dispatch = config.get("dispatch")
    canary = (
        dispatch.get("production_canary")
        if isinstance(dispatch, Mapping)
        else None
    )
    tests = manifest.get("test_results")
    formal = (
        tests.get("formal_surface_gate")
        if isinstance(tests, Mapping)
        else None
    )
    if (
        manifest.get("schema_version")
        != "study-intake-preprocessor-release-v2"
        or manifest.get("release_id") != release_id
        or manifest.get("model_contract") != TARGET_MODEL_REQUEST_CONTRACT
        or not isinstance(model, Mapping)
        or model.get("model") != "gpt-5.6-luna"
        or model.get("reasoning_effort") != "max"
        or "service_tier" in model
        or not isinstance(canary, Mapping)
        or canary.get("enabled") is not True
        or canary.get("status") != "production_canary_active"
        or canary.get("admission")
        != "first_post_activation_producer_capture"
        or canary.get("keep_backlog_drained") is not True
        or canary.get("post_activation_only") is not True
        or canary.get("initial_canary_inflight_limit") != 1
        or canary.get("continuous_concurrency_limit") != 20
        or not isinstance(formal, Mapping)
        or formal.get("status") != "passed"
        or formal.get("verification_status") != "unchanged"
        or formal.get("differences") != {}
        or formal.get("baseline_manifest_sha256")
        != formal.get("current_manifest_sha256")
        or any(
            not _sha(formal.get(name))
            for name in (
                "baseline_file_sha256",
                "config_file_sha256",
                "baseline_manifest_sha256",
                "current_manifest_sha256",
            )
        )
        or manifest.get("formal_write_count") != 0
    ):
        _fail("campaign_candidate_release_v2_invalid")
    execution_contract: dict[str, Any] = {
        "release_verified": False,
        "provider_executable": None,
        "task_runner_executable": None,
    }
    if evidence_scope == "real_production_hmac_v2":
        try:
            verified = release_manager.verify_release(root)
        except release_manager.ReleaseError as exc:
            raise CampaignPublishError(
                "campaign_candidate_release_immutable_verification_failed"
            ) from exc
        if (
            verified.get("release_id") != release_id
            or verified.get("model_contract")
            != TARGET_MODEL_REQUEST_CONTRACT
        ):
            _fail("campaign_candidate_release_immutable_verification_failed")
        for schema_name, expected_sha256 in FROZEN_RUNTIME_SCHEMA_SHA256S.items():
            schema_path = root / "schemas" / schema_name
            try:
                schema_raw = _safe_regular_bytes(
                    schema_path,
                    root=root,
                    maximum=MAX_JSON_BYTES,
                    code="campaign_runtime_schema_contract_mismatch",
                )
            except CampaignPublishError:
                raise
            if hashlib.sha256(schema_raw).hexdigest() != expected_sha256:
                _fail("campaign_runtime_schema_contract_mismatch")
        codex_path = model.get("codex_path")
        if not isinstance(codex_path, str) or not codex_path:
            _fail("campaign_provider_executable_binding_invalid")
        execution_contract = {
            "release_verified": True,
            "provider_executable": _executable_binding_v2(
                Path(codex_path),
                code="campaign_provider_executable_binding_invalid",
            ),
            "task_runner_executable": _executable_binding_v2(
                root / "bin" / "preprocess_task_runner.py",
                code="campaign_task_runner_executable_binding_invalid",
            ),
        }
    return (
        manifest,
        hashlib.sha256(manifest_raw).hexdigest(),
        execution_contract,
    )


def _verify_canary_activation_v2(
    path: Path,
    *,
    key: bytes,
    release_id: str,
    release_manifest_sha256: str,
) -> tuple[dict[str, Any], str]:
    receipt, digest = _read_named_canonical_receipt(
        path, code="campaign_canary_activation_v2_invalid"
    )
    authority = receipt.get("authority")
    core = {
        name: copy.deepcopy(value)
        for name, value in receipt.items()
        if name != "authority"
    }
    expected_hmac = hmac.new(
        key, _compact_bytes(core, newline=False), hashlib.sha256
    ).hexdigest()
    watermarks = receipt.get("producer_high_watermark_sha256s")
    slots = receipt.get("slots")
    services = receipt.get("service_release_ids")
    if (
        set(receipt) != V2_GLOBAL_ACTIVATION_KEYS
        or receipt.get("schema_version")
        != "study-intake-three-subject-canary-activation-receipt-v2"
        or receipt.get("status") != "production_canary_active"
        or not _sha(receipt.get("activation_id"))
        or receipt.get("release_id") != release_id
        or not _aware_timestamp(receipt.get("activated_at"))
        or receipt.get("post_activation_only") is not True
        or receipt.get("historical_backlog_drained") is not True
        or receipt.get("initial_canary_inflight_limit") != 1
        or receipt.get("continuous_concurrency_limit") != 20
        or "requested_service_tier" not in receipt
        or receipt.get("requested_service_tier") is not None
        or receipt.get("fast_mode_requested") is not False
        or receipt.get("fast_mode_effective") != "not_requested"
        or not isinstance(watermarks, Mapping)
        or set(watermarks) != set(SUBJECTS)
        or any(not _sha(watermarks.get(subject)) for subject in SUBJECTS)
        or not isinstance(slots, Mapping)
        or set(slots) != set(SUBJECTS)
        or not isinstance(services, Mapping)
        or set(services) != {"math", "cs408", "english", "dashboard"}
        or any(value != release_id for value in services.values())
        or receipt.get("release_manifest_sha256")
        != release_manifest_sha256
        or any(
            not _sha(receipt.get(name))
            for name in (
                "canary_manifest_sha256",
                "release_manifest_sha256",
                "pre_activation_verification_sha256",
                "post_activation_verification_sha256",
                "deployment_prepare_receipt_sha256",
            )
        )
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("real_luna_runs") != 0
        or receipt.get("formal_write_count") != 0
        or receipt.get("sol_enabled") is not False
        or receipt.get("production_accepted") is not False
        or not isinstance(authority, Mapping)
        or set(authority) != DEPLOYMENT_AUTHORITY_KEYS
        or authority.get("schema_version")
        != "study-intake-deployment-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("key_id") != hashlib.sha256(key).hexdigest()
        or authority.get("purpose")
        != "three-subject-production-canary-activation"
        or not hmac.compare_digest(
            str(authority.get("hmac_sha256") or ""), expected_hmac
        )
    ):
        _fail("campaign_canary_activation_v2_invalid")
    for subject in SUBJECTS:
        slot = slots[subject]
        if (
            not isinstance(slot, Mapping)
            or set(slot) != CANARY_SLOT_KEYS
            or slot.get("subject") != subject
            or slot.get("producer_high_watermark_sha256")
            != watermarks[subject]
            or slot.get("state") != "armed"
            or slot.get("capture_id") is not None
            or slot.get("completion_receipt_sha256") is not None
        ):
            _fail("campaign_canary_activation_v2_invalid")
    return receipt, digest


def _v2_canary_controls_valid(value: Mapping[str, Any]) -> bool:
    return (
        value.get("producer_capture_enabled") is True
        and value.get("sol_formal_curation_enabled") is False
        and value.get("post_activation_only") is True
        and value.get("initial_canary_inflight_limit") == 1
        and value.get("continuous_concurrency_limit") == 20
        and value.get("keep_backlog_drained") is True
        and value.get("production_accepted") is False
        and "requested_service_tier" in value
        and value.get("requested_service_tier") is None
        and value.get("fast_mode_requested") is False
        and value.get("fast_mode_effective") == "not_requested"
        and value.get("model_call_count") == 0
        and value.get("provider_request_count") == 0
        and value.get("mcp_tool_call_count") == 0
        and value.get("formal_write_count") == 0
        and value.get("sol_enabled") is False
    )


def _read_hmac_content(
    path_value: object,
    sha256_value: object,
    *,
    runtime_root: Path,
    expected_root: Path,
    key: bytes,
    purpose: str,
    code: str,
) -> tuple[dict[str, Any], str, Path]:
    if not isinstance(path_value, str) or not _sha(sha256_value):
        _fail(code)
    path = Path(path_value)
    expected = expected_root.resolve()
    try:
        path.resolve(strict=True).relative_to(expected)
    except (OSError, ValueError) as exc:
        raise CampaignPublishError(code) from exc
    digest = str(sha256_value)
    value = _read_dispatch_content(
        path,
        runtime_root=runtime_root,
        digest=digest,
        code=code,
        content_addressed=True,
    )
    _verify_dispatch_seal(value, key=key, purpose=purpose, code=code)
    return value, digest, path


def _verify_subject_activation_v2(
    runtime_root: Path,
    key: bytes,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    receipt, digest, _path = _read_hmac_content(
        state.get("activation_receipt_path"),
        state.get("activation_receipt_sha256"),
        runtime_root=runtime_root,
        expected_root=(
            runtime_root / "dispatch" / "production-canary" / "receipts"
        ),
        key=key,
        purpose="dispatch-production-canary-activation",
        code="campaign_subject_activation_v2_invalid",
    )
    producer = receipt.get("producer_authority")
    producer_core = (
        {
            name: copy.deepcopy(child)
            for name, child in producer.items()
            if name != "authority_fingerprint"
        }
        if isinstance(producer, Mapping)
        else None
    )
    watermark = receipt.get("producer_high_watermark")
    if (
        receipt.get("schema_version")
        != "study-intake-production-canary-activation-receipt-v2"
        or receipt.get("subject") != state.get("subject")
        or receipt.get("release_id") != state.get("release_id")
        or receipt.get("activation_id") != state.get("activation_id")
        or digest != state.get("activation_receipt_sha256")
        or receipt.get("producer_authority_fingerprint")
        != state.get("producer_authority_fingerprint")
        or receipt.get("producer_high_watermark_sha256")
        != state.get("producer_high_watermark_sha256")
        or not isinstance(producer, Mapping)
        or not isinstance(producer_core, Mapping)
        or producer.get("schema_version")
        != "study-intake-producer-authority-v1"
        or producer.get("subject") != state.get("subject")
        or producer.get("release_id") != state.get("release_id")
        or producer.get("model") != "gpt-5.6-luna"
        or producer.get("reasoning_effort") != "max"
        or "service_tier" in producer
        or producer.get("formal_write_count") != 0
        or producer.get("authority_fingerprint")
        != hashlib.sha256(
            _compact_bytes(producer_core, newline=False)
        ).hexdigest()
        or not isinstance(watermark, Mapping)
        or hashlib.sha256(
            _compact_bytes(watermark, newline=False)
        ).hexdigest()
        != state.get("producer_high_watermark_sha256")
        or not _v2_canary_controls_valid(receipt)
    ):
        _fail("campaign_subject_activation_v2_invalid")
    return receipt


def _read_subject_state_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    global_activation: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    path = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary"
        / f"{subject}.json"
    )
    raw = _safe_regular_bytes(
        path,
        root=runtime_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_subject_state_v2_invalid",
    )
    state = _json_mapping(raw, "campaign_subject_state_v2_invalid")
    _verify_dispatch_seal(
        state,
        key=key,
        purpose="dispatch-production-canary-state",
        code="campaign_subject_state_v2_invalid",
    )
    terminal_counts = state.get("terminal_by_outcome")
    active = state.get("active_selections")
    if (
        raw != _compact_bytes(state, newline=True)
        or state.get("schema_version")
        != "study-intake-production-canary-state-v2"
        or state.get("status") != "production_canary_active"
        or state.get("state")
        not in {
            "armed",
            "canary_in_flight",
            "continuous_concurrent_unlocked",
            "failed_drained",
            "paused_drained",
            "inactive_rolled_back",
        }
        or state.get("subject") != subject
        or state.get("release_id") != release_id
        or not _sha(state.get("activation_id"))
        or state.get("producer_high_watermark_sha256")
        != global_activation["producer_high_watermark_sha256s"][subject]
        or not _sha(state.get("producer_authority_fingerprint"))
        or not _sha(state.get("activation_receipt_sha256"))
        or not _sha(state.get("activation_gate_authority_sha256"))
        or not _sha(state.get("terminal_index_sha256"))
        or not isinstance(active, Mapping)
        or state.get("active_task_count") != len(active)
        or not isinstance(terminal_counts, Mapping)
        or set(terminal_counts) != V2_TERMINAL_OUTCOMES
        or any(
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            for count in terminal_counts.values()
        )
        or state.get("terminal_task_count") != sum(terminal_counts.values())
        or not _v2_canary_controls_valid(state)
    ):
        _fail("campaign_subject_state_v2_invalid")
    _verify_subject_activation_v2(runtime_root, key, state)
    return state, hashlib.sha256(raw).hexdigest()


def _verify_no_fast_provider_identity_v2(
    identity: Mapping[str, Any],
    *,
    expected_executable: Mapping[str, str],
) -> None:
    argv = identity.get("argv")
    environment_key_names = identity.get("environment_key_names")
    if (
        identity.get("argv_policy_version")
        != PROVIDER_ARGV_POLICY_VERSION
        or not isinstance(argv, list)
        or not argv
        or any(not isinstance(item, str) or not item for item in argv)
        or identity.get("argv_sha256")
        != hashlib.sha256(
            _compact_bytes(argv, newline=False)
        ).hexdigest()
        or identity.get("environment_policy_version")
        != PROVIDER_ENVIRONMENT_POLICY_VERSION
        or not isinstance(environment_key_names, list)
        or environment_key_names != sorted(set(environment_key_names))
        or any(
            not isinstance(item, str) or not item
            for item in environment_key_names
        )
        or identity.get("environment_key_names_sha256")
        != hashlib.sha256(
            _compact_bytes(environment_key_names, newline=False)
        ).hexdigest()
        or identity.get("forbidden_environment_key_matches") != []
        or identity.get("forbidden_environment_value_key_matches") != []
        or identity.get("executable_path") != expected_executable.get("path")
        or identity.get("executable_sha256")
        != expected_executable.get("sha256")
    ):
        _fail("campaign_provider_execution_contract_invalid")
    prohibited_markers = (
        "service_tier",
        "service-tier",
        "service.tier",
        "service tier",
        "priority",
    )
    normalized_argv = [
        unicodedata.normalize("NFKC", item).casefold() for item in argv
    ]
    if any(
        marker in item
        for item in normalized_argv
        for marker in prohibited_markers
    ):
        _fail("campaign_provider_fast_mode_argv_forbidden")
    normalized_environment_keys = [
        re.sub(
            r"[^a-z0-9]+",
            "_",
            unicodedata.normalize("NFKC", item).casefold(),
        ).strip("_")
        for item in environment_key_names
    ]
    if any("service_tier" in item for item in normalized_environment_keys):
        _fail("campaign_provider_fast_mode_environment_forbidden")
    try:
        argv_executable = Path(argv[0]).expanduser().resolve(strict=True)
    except OSError as exc:
        raise CampaignPublishError(
            "campaign_provider_executable_binding_invalid"
        ) from exc
    required_tokens = {
        "--strict-config",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--json",
    }
    model_pairs = zip(argv, argv[1:])
    if (
        str(argv_executable) != expected_executable.get("path")
        or len(argv) < 3
        or argv[1] != "exec"
        or not required_tokens.issubset(set(argv))
        or ("--model", "gpt-5.6-luna") not in model_pairs
        or not any(
            argv[index] == "--config"
            and argv[index + 1] == 'model_reasoning_effort="max"'
            for index in range(len(argv) - 1)
        )
        or not any(
            argv[index] == "--sandbox" and argv[index + 1] == "read-only"
            for index in range(len(argv) - 1)
        )
        or argv[-1] != "-"
    ):
        _fail("campaign_provider_argv_contract_invalid")


def _verify_task_process_closure_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    unit_sha256: str,
    frozen_payload_sha256: str,
    identity_sha256: str,
    identity_path: str,
    exit_sha256: str | None,
    exit_path: str | None,
    process_execution: Mapping[str, Any] | None,
    require_real_provider_closure: bool,
    require_release_task_runner: bool,
    execution_contract: Mapping[str, Any],
) -> dict[str, Any]:
    identity, identity_digest, identity_file = _read_hmac_content(
        identity_path,
        identity_sha256,
        runtime_root=runtime_root,
        expected_root=runtime_root / "dispatch" / "process-identities",
        key=key,
        purpose="dispatch-task-process-identity",
        code="campaign_task_process_identity_invalid",
    )
    if (
        set(identity) != TASK_PROCESS_IDENTITY_KEYS
        or identity.get("schema_version")
        != "study-intake-task-process-identity-v1"
        or identity.get("subject") != subject
        or identity.get("release_id") != release_id
        or identity.get("unit_sha256") != unit_sha256
        or identity.get("frozen_payload_sha256") != frozen_payload_sha256
        or identity.get("model_call_count") != 0
        or identity.get("provider_request_count") != 0
        or identity.get("formal_write_count") != 0
        or identity.get("sol_enabled") is not False
        or identity.get("start_new_session") is not True
        or not _aware_timestamp(identity.get("launched_at"))
        or not _sha(identity.get("argv_sha256"))
        or not _sha(identity.get("executable_sha256"))
        or not isinstance(identity.get("capture_id"), str)
        or not identity.get("capture_id")
        or not isinstance(identity.get("lease_fence"), int)
        or isinstance(identity.get("lease_fence"), bool)
        or int(identity["lease_fence"]) < 1
        or any(
            isinstance(identity.get(name), bool)
            or not isinstance(identity.get(name), int)
            or int(identity[name]) < 2
            for name in ("dispatcher_pid", "child_pid", "child_pgid")
        )
        or any(
            not isinstance(identity.get(name), str)
            or not Path(str(identity[name])).is_absolute()
            for name in (
                "executable_path",
                "context_root",
                "expected_mcp_session_root",
                "expected_report_root",
            )
        )
    ):
        _fail("campaign_task_process_identity_invalid")
    expected_task_runner = execution_contract.get("task_runner_executable")
    if require_release_task_runner and (
        execution_contract.get("release_verified") is not True
        or not isinstance(expected_task_runner, Mapping)
        or identity.get("executable_path")
        != expected_task_runner.get("path")
        or identity.get("executable_sha256")
        != expected_task_runner.get("sha256")
    ):
        _fail("campaign_task_runner_executable_binding_invalid")
    result: dict[str, Any] = {
        "identity_sha256": identity_digest,
        "identity_path": str(identity_file),
        "started_at": identity["launched_at"],
        "finished_at": None,
        "exit_sha256": None,
        "exit_path": None,
        "task_runner_executable_sha256": (
            identity.get("executable_sha256")
            if require_release_task_runner
            else None
        ),
        "provider_execution": None,
    }
    if exit_sha256 is None or exit_path is None:
        if exit_sha256 is not None or exit_path is not None or process_execution is not None:
            _fail("campaign_task_process_closure_invalid")
        return result
    exit_value, exit_digest, exit_file = _read_hmac_content(
        exit_path,
        exit_sha256,
        runtime_root=runtime_root,
        expected_root=runtime_root / "dispatch" / "process-exits",
        key=key,
        purpose="dispatch-task-process-exit",
        code="campaign_task_process_exit_invalid",
    )
    if (
        set(exit_value) != TASK_PROCESS_EXIT_KEYS
        or exit_value.get("schema_version")
        != "study-intake-task-process-exit-v1"
        or exit_value.get("subject") != subject
        or exit_value.get("release_id") != release_id
        or exit_value.get("unit_sha256") != unit_sha256
        or exit_value.get("frozen_payload_sha256") != frozen_payload_sha256
        or exit_value.get("capture_id") != identity.get("capture_id")
        or exit_value.get("task_process_identity_sha256") != identity_digest
        or exit_value.get("task_process_identity_path") != str(identity_file)
        or exit_value.get("owner_id") != identity.get("owner_id")
        or exit_value.get("lease_fence") != identity.get("lease_fence")
        or exit_value.get("child_pid") != identity.get("child_pid")
        or exit_value.get("child_pgid") != identity.get("child_pgid")
        or exit_value.get("process_start_token")
        != identity.get("process_start_token")
        or exit_value.get("launch_nonce") != identity.get("launch_nonce")
        or exit_value.get("launched_at") != identity.get("launched_at")
        or exit_value.get("reaped") is not True
        or exit_value.get("process_absent") is not True
        or exit_value.get("pgid_absent") is not True
        or exit_value.get("model_call_count") != 0
        or exit_value.get("provider_request_count") != 0
        or exit_value.get("formal_write_count") != 0
        or exit_value.get("sol_enabled") is not False
        or not _sha(exit_value.get("stdout_sha256"))
        or not _sha(exit_value.get("stderr_sha256"))
        or any(
            isinstance(exit_value.get(name), bool)
            or not isinstance(exit_value.get(name), int)
            or int(exit_value[name]) < 0
            for name in ("stdout_size", "stderr_size")
        )
        or not _aware_timestamp(exit_value.get("finished_at"))
        or _timestamp(
            exit_value.get("finished_at"),
            "campaign_task_process_interval_invalid",
        )
        < _timestamp(
            identity.get("launched_at"),
            "campaign_task_process_interval_invalid",
        )
    ):
        _fail("campaign_task_process_closure_invalid")
    if (
        exit_value.get("termination_reason") == "completed"
        and (
            exit_value.get("returncode") != 0
            or exit_value.get("late_result_publish_allowed") is not True
        )
    ) or (
        exit_value.get("termination_reason") != "completed"
        and exit_value.get("late_result_publish_allowed") is not False
    ):
        _fail("campaign_task_process_closure_invalid")
    if process_execution is not None:
        if (
            process_execution.get("supervisor_process_identity_sha256")
            != identity_digest
            or process_execution.get("supervisor_process_identity_path")
            != str(identity_file)
            or process_execution.get("supervisor_process_exit_sha256")
            != exit_digest
            or process_execution.get("supervisor_process_exit_path")
            != str(exit_file)
            or process_execution.get("supervisor_pid")
            != identity.get("child_pid")
            or process_execution.get("supervisor_pgid")
            != identity.get("child_pgid")
            or process_execution.get("process_start_token")
            != identity.get("process_start_token")
            or process_execution.get("launch_nonce")
            != identity.get("launch_nonce")
            or process_execution.get("launched_at")
            != identity.get("launched_at")
            or process_execution.get("finished_at")
            != exit_value.get("finished_at")
            or process_execution.get("returncode")
            != exit_value.get("returncode")
            or process_execution.get("termination_reason")
            != exit_value.get("termination_reason")
            or process_execution.get("reaped") is not True
            or process_execution.get("process_absent") is not True
            or process_execution.get("pgid_absent") is not True
        ):
            _fail("campaign_task_process_execution_binding_invalid")
        if require_real_provider_closure:
            result["provider_execution"] = _verify_real_provider_closures_v2(
                runtime_root,
                key,
                subject=subject,
                release_id=release_id,
                unit_sha256=unit_sha256,
                frozen_payload_sha256=frozen_payload_sha256,
                supervisor_identity_sha256=identity_digest,
                supervisor_identity=identity,
                execution_contract=execution_contract,
                process_execution=process_execution,
            )
    result.update(
        {
            "finished_at": exit_value["finished_at"],
            "exit_sha256": exit_digest,
            "exit_path": str(exit_file),
            "late_result_publish_allowed": exit_value.get(
                "late_result_publish_allowed"
            ),
        }
    )
    return result


def _verify_real_provider_closures_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    unit_sha256: str,
    frozen_payload_sha256: str,
    supervisor_identity_sha256: str,
    supervisor_identity: Mapping[str, Any],
    execution_contract: Mapping[str, Any],
    process_execution: Mapping[str, Any],
) -> dict[str, Any]:
    stages = process_execution.get("provider_stages")
    expected_stages = {f"{subject}_analysis", f"{subject}_critical_review"}
    if (
        process_execution.get("canonical_task_runner") is not True
        or process_execution.get("provider_process_closure_required") is not True
        or not isinstance(stages, Mapping)
        or set(stages) != expected_stages
    ):
        _fail("campaign_real_provider_closure_missing")
    expected_provider = execution_contract.get("provider_executable")
    if (
        execution_contract.get("release_verified") is not True
        or not isinstance(expected_provider, Mapping)
    ):
        _fail("campaign_provider_executable_binding_invalid")
    identity_sha256s: dict[str, str] = {}
    exit_sha256s: dict[str, str] = {}
    for stage_name in sorted(expected_stages):
        row = stages[stage_name]
        if not isinstance(row, Mapping):
            _fail("campaign_real_provider_closure_invalid")
        identity, identity_sha, identity_path = _read_hmac_content(
            row.get("provider_process_identity_path"),
            row.get("provider_process_identity_sha256"),
            runtime_root=runtime_root,
            expected_root=(
                runtime_root / "dispatch" / "provider-process-identities"
            ),
            key=key,
            purpose="dispatch-provider-process-identity",
            code="campaign_real_provider_identity_invalid",
        )
        exit_value, exit_sha, exit_path = _read_hmac_content(
            row.get("provider_process_exit_path"),
            row.get("provider_process_exit_sha256"),
            runtime_root=runtime_root,
            expected_root=runtime_root / "dispatch" / "provider-process-exits",
            key=key,
            purpose="dispatch-provider-process-exit",
            code="campaign_real_provider_exit_invalid",
        )
        if (
            set(identity) != PROVIDER_PROCESS_IDENTITY_KEYS
            or set(exit_value) != PROVIDER_PROCESS_EXIT_KEYS
            or identity.get("schema_version")
            != "study-intake-provider-process-identity-v1"
            or identity.get("role") != "codex_exec_provider_child"
            or identity.get("stage_name") != stage_name
            or identity.get("subject") != subject
            or identity.get("release_id") != release_id
            or identity.get("unit_sha256") != unit_sha256
            or identity.get("frozen_payload_sha256") != frozen_payload_sha256
            or not isinstance(identity.get("capture_id"), str)
            or not identity.get("capture_id")
            or identity.get("supervisor_process_identity_sha256")
            != supervisor_identity_sha256
            or identity.get("supervisor_process_identity_path")
            != process_execution.get("supervisor_process_identity_path")
            or identity.get("supervisor_pid")
            != supervisor_identity.get("child_pid")
            or identity.get("supervisor_pgid")
            != supervisor_identity.get("child_pgid")
            or identity.get("owner_id") != supervisor_identity.get("owner_id")
            or identity.get("lease_fence")
            != supervisor_identity.get("lease_fence")
            or identity.get("cwd") != supervisor_identity.get("context_root")
            or identity.get("context_root")
            != supervisor_identity.get("context_root")
            or identity.get("start_new_session") is not True
            or identity.get("provider_request_started") is not True
            or "requested_service_tier" not in identity
            or identity.get("requested_service_tier") is not None
            or identity.get("fast_mode_requested") is not False
            or identity.get("fast_mode_effective") != "not_requested"
            or identity.get("formal_write_count") != 0
            or identity.get("sol_enabled") is not False
            or exit_value.get("schema_version")
            != "study-intake-provider-process-exit-v1"
            or exit_value.get("stage_name") != stage_name
            or exit_value.get("subject") != subject
            or exit_value.get("release_id") != release_id
            or exit_value.get("unit_sha256") != unit_sha256
            or exit_value.get("frozen_payload_sha256")
            != frozen_payload_sha256
            or exit_value.get("capture_id") != identity.get("capture_id")
            or exit_value.get("owner_id") != identity.get("owner_id")
            or exit_value.get("lease_fence") != identity.get("lease_fence")
            or exit_value.get("provider_process_identity_sha256")
            != identity_sha
            or exit_value.get("provider_process_identity_path")
            != str(identity_path)
            or exit_value.get("provider_pid") != identity.get("provider_pid")
            or exit_value.get("provider_pgid") != identity.get("provider_pgid")
            or exit_value.get("process_start_token")
            != identity.get("process_start_token")
            or not _aware_timestamp(identity.get("launched_at"))
            or not _aware_timestamp(exit_value.get("finished_at"))
            or _timestamp(
                exit_value.get("finished_at"),
                "campaign_real_provider_interval_invalid",
            )
            < _timestamp(
                identity.get("launched_at"),
                "campaign_real_provider_interval_invalid",
            )
            or exit_value.get("returncode") != 0
            or exit_value.get("termination_reason") != "completed"
            or exit_value.get("reaped") is not True
            or exit_value.get("process_absent") is not True
            or exit_value.get("pgid_absent") is not True
            or exit_value.get("late_result_publish_allowed") is not True
            or exit_value.get("formal_write_count") != 0
            or exit_value.get("sol_enabled") is not False
            or row.get("provider_process_identity_sha256") != identity_sha
            or row.get("provider_process_exit_sha256") != exit_sha
            or row.get("provider_process_identity_path") != str(identity_path)
            or row.get("provider_process_exit_path") != str(exit_path)
        ):
            _fail("campaign_real_provider_closure_invalid")
        _verify_no_fast_provider_identity_v2(
            identity, expected_executable=expected_provider
        )
        public_stage = (
            "analysis" if stage_name.endswith("_analysis") else "critical_review"
        )
        identity_sha256s[public_stage] = identity_sha
        exit_sha256s[public_stage] = exit_sha
    return {
        "status": PROVIDER_EXECUTION_VERIFIED,
        "stage_identity_sha256s": identity_sha256s,
        "stage_exit_sha256s": exit_sha256s,
        "provider_executable_sha256": expected_provider["sha256"],
    }


def _verify_queue_task_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    activation_id: str,
    selected: Mapping[str, Any],
    terminal: Mapping[str, Any] | None,
) -> dict[str, Any]:
    contract_sha = str(selected.get("producer_input_contract_sha256") or "")
    if not _sha(activation_id) or not _sha(contract_sha):
        _fail("campaign_queue_task_v2_invalid")
    queue_path = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-queue"
        / subject
        / activation_id
        / f"{contract_sha}.json"
    )
    raw = _safe_regular_bytes(
        queue_path,
        root=runtime_root,
        maximum=MAX_JSON_BYTES,
        code="campaign_queue_task_v2_invalid",
    )
    queue = _json_mapping(raw, "campaign_queue_task_v2_invalid")
    _verify_dispatch_seal(
        queue,
        key=key,
        purpose="dispatch-production-canary-queue",
        code="campaign_queue_task_v2_invalid",
    )
    if (
        raw != _compact_bytes(queue, newline=True)
        or queue.get("schema_version")
        != "study-intake-production-canary-queue-entry-v2"
        or queue.get("subject") != subject
        or queue.get("release_id") != release_id
        or queue.get("activation_id") != activation_id
        or queue.get("producer_input_contract_sha256") != contract_sha
        or queue.get("producer_unit_id") != selected.get("producer_unit_id")
        or queue.get("producer_recorded_at")
        != selected.get("producer_recorded_at")
        or queue.get("source_event_set_sha256")
        != selected.get("source_event_set_sha256")
        or queue.get("unit_sha256") != selected.get("unit_sha256")
        or queue.get("frozen_payload_sha256")
        != selected.get("frozen_payload_sha256")
        or queue.get("formal_write_count") != 0
    ):
        _fail("campaign_queue_task_v2_invalid")
    if terminal is not None and (
        queue.get("finished_at") != terminal.get("finished_at")
        or queue.get("terminal_receipt_sha256")
        != terminal.get("_terminal_sha256")
        or queue.get("terminal_outcome") != terminal.get("outcome")
        or queue.get("terminal_error_code") != terminal.get("error_code")
        or queue.get("queue_status")
        != ("succeeded" if terminal.get("outcome") == "succeeded" else "failed")
    ):
        _fail("campaign_queue_terminal_binding_invalid")
    task_sha = str(queue.get("task_object_sha256") or "")
    task_path = Path(str(queue.get("task_object_path") or ""))
    task_value = _read_dispatch_content(
        task_path,
        runtime_root=runtime_root,
        digest=task_sha,
        code="campaign_frozen_task_v2_invalid",
        content_addressed=True,
    )
    try:
        task = FrozenTask.from_mapping(task_value)
    except DispatchError as exc:
        raise CampaignPublishError("campaign_frozen_task_v2_invalid") from exc
    payload = task.frozen_payload
    contract = payload.get("dispatch_contract")
    producer = (
        contract.get("producer_input_contract")
        if isinstance(contract, Mapping)
        else None
    )
    producer_core = (
        {
            name: copy.deepcopy(child)
            for name, child in producer.items()
            if name != "producer_input_contract_sha256"
        }
        if isinstance(producer, Mapping)
        else None
    )
    allowed_refs = payload.get("allowed_evidence_refs")
    if (
        task.unit_sha256 != selected.get("unit_sha256")
        or task.frozen_payload_sha256
        != selected.get("frozen_payload_sha256")
        or payload.get("subject") != subject
        or payload.get("capture_id") != selected.get("producer_unit_id")
        or payload.get("recorded_at") != selected.get("producer_recorded_at")
        or not isinstance(contract, Mapping)
        or contract.get("release_id") != release_id
        or "requested_service_tier" not in contract
        or contract.get("requested_service_tier") is not None
        or contract.get("fast_mode_requested") is not False
        or contract.get("fast_mode_effective") != "not_requested"
        or not isinstance(producer, Mapping)
        or not isinstance(producer_core, Mapping)
        or producer.get("producer_input_contract_sha256") != contract_sha
        or hashlib.sha256(
            _compact_bytes(producer_core, newline=False)
        ).hexdigest()
        != contract_sha
        or not isinstance(allowed_refs, list)
        or not allowed_refs
        or sorted(set(allowed_refs)) != sorted(allowed_refs)
        or any(not isinstance(ref, str) or not ref for ref in allowed_refs)
    ):
        _fail("campaign_frozen_task_v2_invalid")
    declared_sha = hashlib.sha256(
        _compact_bytes(sorted(allowed_refs), newline=False)
    ).hexdigest()
    if terminal is not None and terminal.get(
        "task_declared_evidence_refs_sha256"
    ) != declared_sha:
        _fail("campaign_frozen_task_evidence_binding_invalid")
    return {
        "task": task,
        "queue_sha256": hashlib.sha256(raw).hexdigest(),
        "task_sha256": task_sha,
        "allowed_evidence_refs": sorted(allowed_refs),
        "declared_evidence_refs_sha256": declared_sha,
    }


def _verify_gate_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    digest = str(terminal.get("canary_gate_sha256") or "")
    activation_id = str(terminal.get("activation_id") or "")
    path = (
        runtime_root
        / "dispatch"
        / "production-canary"
        / "receipts"
        / subject
        / activation_id
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    )
    gate, _digest, _path = _read_hmac_content(
        str(path),
        digest,
        runtime_root=runtime_root,
        expected_root=(
            runtime_root / "dispatch" / "production-canary" / "receipts"
        ),
        key=key,
        purpose="dispatch-production-canary-gate",
        code="campaign_subject_gate_v2_invalid",
    )
    selected = terminal.get("selected")
    gate_selected = gate.get("selected")
    stable_keys = {
        "producer_unit_id",
        "producer_recorded_at",
        "producer_input_contract_sha256",
        "source_event_set_sha256",
        "unit_sha256",
        "frozen_payload_sha256",
        "lease_owner_id",
        "lease_fence",
        "context_root",
        "mcp_session_root",
        "report_root",
    }
    expected_after = (
        "canary_in_flight"
        if gate.get("state_before") == "armed"
        else "continuous_concurrent_unlocked"
    )
    authority = gate.get("authority")
    if (
        gate.get("schema_version")
        != "study-intake-production-canary-gate-receipt-v2"
        or gate.get("activation_id") != activation_id
        or gate.get("subject") != subject
        or gate.get("release_id") != terminal.get("release_id")
        or gate.get("activation_receipt_sha256")
        != terminal.get("activation_receipt_sha256")
        or gate.get("activation_gate_authority_sha256")
        != terminal.get("activation_gate_authority_sha256")
        or gate.get("state_before")
        not in {"armed", "continuous_concurrent_unlocked"}
        or gate.get("state_after") != expected_after
        or not isinstance(selected, Mapping)
        or not isinstance(gate_selected, Mapping)
        or any(gate_selected.get(name) != selected.get(name) for name in stable_keys)
        or gate_selected.get("process_identity_sha256") is not None
        or gate_selected.get("process_identity_path") is not None
        or not isinstance(authority, Mapping)
        or hashlib.sha256(
            _compact_bytes(authority, newline=False)
        ).hexdigest()
        != terminal.get("canary_gate_authority_sha256")
        or not _aware_timestamp(gate.get("admitted_at"))
        or not _v2_canary_controls_valid(gate)
    ):
        _fail("campaign_subject_gate_v2_invalid")
    return gate


def _verify_grounding_v2(
    *,
    subject: str,
    terminal: Mapping[str, Any],
    allowed_evidence_refs: list[str],
) -> dict[str, Any]:
    grounding = terminal.get("mcp_stage_grounding")
    if not isinstance(grounding, Mapping) or set(grounding) != {
        "analysis",
        "critical_review",
    }:
        _fail("campaign_subject_grounding_v2_invalid")
    prior_consumed: set[str] = set()
    common_binding: tuple[str, str, str] | None = None
    grounded_all: set[str] = set()
    call_count = 0
    manifests: dict[str, str] = {}
    for stage_name in ("analysis", "critical_review"):
        row = grounding.get(stage_name)
        if not isinstance(row, Mapping) or set(row) != STAGE_GROUNDING_KEYS:
            _fail("campaign_subject_grounding_v2_invalid")
        consumed = row.get("consumed_evidence_refs")
        cited = row.get("cited_evidence_refs")
        grounded = row.get("grounded_evidence_refs")
        if not all(isinstance(value, list) and value for value in (consumed, cited, grounded)):
            _fail("campaign_subject_grounding_v2_invalid")
        consumed_set = set(consumed)
        cited_set = set(cited)
        grounded_set = set(grounded)
        effective = set(allowed_evidence_refs) | prior_consumed | consumed_set
        binding = (
            str(row.get("read_session_id") or ""),
            str(row.get("evidence_generation") or ""),
            str(row.get("evidence_authority_fingerprint") or ""),
        )
        stage_calls = row.get("mcp_tool_call_count")
        if (
            not all(binding)
            or not _sha(binding[2])
            or not _sha(row.get("read_session_manifest_sha256"))
            or not _sha(row.get("mcp_grounding_manifest_sha256"))
            or list(consumed) != sorted(consumed_set)
            or list(cited) != sorted(cited_set)
            or list(grounded) != sorted(grounded_set)
            or any(not isinstance(ref, str) or not ref for ref in effective | cited_set)
            or not grounded_set
            or grounded_set != consumed_set.intersection(cited_set)
            or not cited_set.issubset(effective)
            or row.get("effective_allowed_evidence_refs_sha256")
            != hashlib.sha256(
                _compact_bytes(sorted(effective), newline=False)
            ).hexdigest()
            or isinstance(stage_calls, bool)
            or not isinstance(stage_calls, int)
            or stage_calls < 1
        ):
            _fail("campaign_subject_grounding_v2_invalid")
        if common_binding is None:
            common_binding = binding
        elif common_binding != binding:
            _fail("campaign_subject_grounding_cross_binding")
        prior_consumed.update(consumed_set)
        grounded_all.update(grounded_set)
        call_count += stage_calls
        manifests[stage_name] = str(row["mcp_grounding_manifest_sha256"])
    assert common_binding is not None
    if (
        terminal.get("read_session_id") != common_binding[0]
        or terminal.get("evidence_generation") != common_binding[1]
        or terminal.get("evidence_authority_fingerprint") != common_binding[2]
        or terminal.get("observed_mcp_tool_call_count") != call_count
    ):
        _fail("campaign_subject_grounding_cross_binding")
    return {
        "read_session_id": common_binding[0],
        "evidence_generation": common_binding[1],
        "evidence_authority_fingerprint": common_binding[2],
        "evidence_refs": sorted(grounded_all),
        "mcp_tool_call_count": call_count,
        "grounding_manifest_sha256s": manifests,
    }


def _verify_success_chain_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    completion, completion_path = _read_dispatch_bound_path(
        terminal.get("completion_path"),
        runtime_root=runtime_root,
        digest=terminal.get("completion_sha256"),
        code="campaign_dispatch_completion_v2_invalid",
        content_addressed=False,
    )
    _verify_dispatch_seal(
        completion,
        key=key,
        purpose="dispatch-completion",
        code="campaign_dispatch_completion_v2_invalid",
    )
    package, package_path = _read_dispatch_bound_path(
        terminal.get("package_path"),
        runtime_root=runtime_root,
        digest=terminal.get("package_sha256"),
        code="campaign_dispatch_package_v2_invalid",
        content_addressed=True,
    )
    processing_receipt, _processing_path = _read_dispatch_bound_path(
        terminal.get("completion_receipt_path"),
        runtime_root=runtime_root,
        digest=terminal.get("completion_receipt_sha256"),
        code="campaign_processing_receipt_v2_invalid",
        content_addressed=True,
    )
    _verify_dispatch_seal(
        processing_receipt,
        key=key,
        purpose="dispatch-receipt",
        code="campaign_processing_receipt_v2_invalid",
    )
    selected = terminal.get("selected")
    if not isinstance(selected, Mapping):
        _fail("campaign_dispatch_report_v2_invalid")
    package_sha256 = str(terminal.get("package_sha256") or "")
    report_json_sha256 = str(terminal.get("report_json_sha256") or "")
    report_markdown_sha256 = str(
        terminal.get("report_markdown_sha256") or ""
    )
    package_ref = (
        "study-intake-dispatch-package://sha256/" + package_sha256
    )
    report_json_ref = "study-intake-report://sha256/" + report_json_sha256
    report_markdown_ref = (
        "study-intake-report-markdown://sha256/" + report_markdown_sha256
    )
    if (
        not all(
            _sha(value)
            for value in (
                package_sha256,
                report_json_sha256,
                report_markdown_sha256,
            )
        )
        or terminal.get("package_ref") != package_ref
        or terminal.get("report_json_ref") != report_json_ref
        or terminal.get("report_markdown_ref") != report_markdown_ref
        or terminal.get("report_reopen_status")
        != "json_markdown_package_verified"
    ):
        _fail("campaign_dispatch_report_v2_invalid")
    report_json_path = (
        runtime_root
        / "dispatch"
        / "reports"
        / "json"
        / "sha256"
        / report_json_sha256[:2]
        / f"{report_json_sha256}.json"
    )
    report_json = _read_content_json(
        report_json_path,
        root=runtime_root,
        digest=report_json_sha256,
        encoding="compact_line",
        code="campaign_dispatch_report_v2_invalid",
    )
    report_markdown_path = (
        runtime_root
        / "dispatch"
        / "reports"
        / "markdown"
        / "sha256"
        / report_markdown_sha256[:2]
        / f"{report_markdown_sha256}.md"
    )
    report_markdown = _safe_regular_bytes(
        report_markdown_path,
        root=runtime_root,
        maximum=MAX_MARKDOWN_BYTES,
        code="campaign_dispatch_report_v2_invalid",
    )
    expected_markdown = (
        f"# {subject} Luna report\n\n"
        f"- capture_id: {selected.get('producer_unit_id')}\n"
        f"- unit_sha256: {selected.get('unit_sha256')}\n"
        f"- report_json_ref: {report_json_ref}\n"
        f"- package_ref: {package_ref}\n"
        "- formal_write_count: 0\n"
    ).encode("utf-8")
    if (
        hashlib.sha256(report_markdown).hexdigest()
        != report_markdown_sha256
        or report_markdown != expected_markdown
        or set(report_json) != DISPATCH_REPORT_KEYS
        or report_json.get("schema_version")
        != "study-intake-dispatch-report-v1"
        or report_json.get("unit_sha256") != selected.get("unit_sha256")
        or report_json.get("subject") != subject
        or report_json.get("capture_id") != selected.get("producer_unit_id")
        or report_json.get("release_id") != terminal.get("release_id")
        or report_json.get("package_ref") != package_ref
        or report_json.get("package_sha256") != package_sha256
        or not isinstance(report_json.get("analysis"), Mapping)
        or not report_json.get("analysis")
        or not isinstance(report_json.get("critical_review"), Mapping)
        or not report_json.get("critical_review")
        or report_json.get("formal_write_count") != 0
    ):
        _fail("campaign_dispatch_report_v2_invalid")
    runtime = processing_receipt.get("observed_stage_runtime")
    if not isinstance(runtime, Mapping) or set(runtime) != {
        "analysis",
        "critical_review",
    }:
        _fail("campaign_processing_receipt_v2_invalid")
    totals = {"model": 0, "provider": 0, "mcp": 0}
    for stage_name in ("analysis", "critical_review"):
        stage = runtime.get(stage_name)
        if not isinstance(stage, Mapping):
            _fail("campaign_processing_receipt_v2_invalid")
        for source, destination in (
            ("model_call_count", "model"),
            ("provider_request_count", "provider"),
            ("mcp_tool_call_count", "mcp"),
        ):
            value = stage.get(source)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                _fail("campaign_processing_receipt_v2_invalid")
            totals[destination] += value
    if (
        not isinstance(selected, Mapping)
        or completion.get("unit_sha256") != selected.get("unit_sha256")
        or completion.get("subject") != subject
        or completion.get("capture_id") != selected.get("producer_unit_id")
        or completion.get("release_id") != terminal.get("release_id")
        or completion.get("outcome") != "succeeded"
        or completion.get("receipt_sha256")
        != terminal.get("completion_receipt_sha256")
        or completion.get("receipt_path")
        != terminal.get("completion_receipt_path")
        or completion.get("package_sha256") != terminal.get("package_sha256")
        or completion.get("package_path") != terminal.get("package_path")
        or completion.get("package_ref") != package_ref
        or completion.get("report_json_ref") != report_json_ref
        or completion.get("report_json_sha256") != report_json_sha256
        or completion.get("report_markdown_ref") != report_markdown_ref
        or completion.get("report_markdown_sha256")
        != report_markdown_sha256
        or processing_receipt.get("unit_sha256")
        != selected.get("unit_sha256")
        or processing_receipt.get("subject") != subject
        or processing_receipt.get("capture_id")
        != selected.get("producer_unit_id")
        or processing_receipt.get("release_id") != terminal.get("release_id")
        or processing_receipt.get("outcome") != "succeeded"
        or processing_receipt.get("package_sha256")
        != terminal.get("package_sha256")
        or processing_receipt.get("package_path")
        != terminal.get("package_path")
        or processing_receipt.get("package_ref") != package_ref
        or processing_receipt.get("report_json_ref") != report_json_ref
        or processing_receipt.get("report_json_sha256")
        != report_json_sha256
        or processing_receipt.get("report_markdown_ref")
        != report_markdown_ref
        or processing_receipt.get("report_markdown_sha256")
        != report_markdown_sha256
        or processing_receipt.get("formal_write_count") != 0
        or package.get("schema_version") != "study-intake-concurrent-package-v1"
        or package.get("unit_sha256") != selected.get("unit_sha256")
        or package.get("subject") != subject
        or package.get("capture_id") != selected.get("producer_unit_id")
        or package.get("release_id") != terminal.get("release_id")
        or package.get("pipeline") != ["analysis", "critical_review"]
        or not isinstance(package.get("analysis"), Mapping)
        or not package.get("analysis")
        or not isinstance(package.get("critical_review"), Mapping)
        or not package.get("critical_review")
        or package.get("formal_write_count") != 0
        or report_json.get("analysis") != package.get("analysis")
        or report_json.get("critical_review") != package.get("critical_review")
        or processing_receipt.get("process_execution")
        != terminal.get("process_execution")
        or totals["model"] != terminal.get("observed_model_call_count")
        or totals["provider"]
        != terminal.get("observed_provider_request_count")
        or totals["mcp"] != terminal.get("observed_mcp_tool_call_count")
        or not _aware_timestamp(completion.get("finished_at"))
        or completion.get("finished_at")
        != processing_receipt.get("finished_at")
        or _timestamp(
            completion.get("finished_at"),
            "campaign_completion_timeline_v2_invalid",
        )
        > _timestamp(
            terminal.get("finished_at"),
            "campaign_completion_timeline_v2_invalid",
        )
    ):
        _fail("campaign_dispatch_package_v2_invalid")
    return {
        "completion_path": str(completion_path),
        "completion_sha256": str(terminal["completion_sha256"]),
        "completion_receipt_path": str(terminal["completion_receipt_path"]),
        "completion_receipt_sha256": str(
            terminal["completion_receipt_sha256"]
        ),
        "observed_model_call_count": totals["model"],
        "observed_provider_request_count": totals["provider"],
        "observed_mcp_tool_call_count": totals["mcp"],
        "package_path": str(package_path),
        "package_ref": package_ref,
        "package_sha256": package_sha256,
        "package": package,
        "report_json_ref": report_json_ref,
        "report_json_sha256": report_json_sha256,
        "report_markdown_ref": report_markdown_ref,
        "report_markdown_sha256": report_markdown_sha256,
        "report_reopen_status": "json_markdown_package_verified",
    }


def _verify_terminal_receipt_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    state: Mapping[str, Any],
    entry: Mapping[str, Any],
    terminal_sha256: str,
    terminal_path: str,
    evidence_scope: str,
    execution_contract: Mapping[str, Any],
) -> dict[str, Any]:
    terminal, digest, path = _read_hmac_content(
        terminal_path,
        terminal_sha256,
        runtime_root=runtime_root,
        expected_root=(
            runtime_root / "dispatch" / "production-canary" / "receipts"
        ),
        key=key,
        purpose="dispatch-production-canary-terminal",
        code="campaign_subject_terminal_v2_invalid",
    )
    selected = terminal.get("selected")
    outcome = terminal.get("outcome")
    if (
        terminal.get("schema_version")
        != "study-intake-production-canary-terminal-receipt-v2"
        or terminal.get("activation_id") != state.get("activation_id")
        or terminal.get("subject") != subject
        or terminal.get("release_id") != release_id
        or terminal.get("producer_authority_fingerprint")
        != state.get("producer_authority_fingerprint")
        or terminal.get("producer_high_watermark_sha256")
        != state.get("producer_high_watermark_sha256")
        or terminal.get("activation_receipt_sha256")
        != state.get("activation_receipt_sha256")
        or terminal.get("activation_gate_authority_sha256")
        != state.get("activation_gate_authority_sha256")
        or not isinstance(selected, Mapping)
        or selected.get("unit_sha256") != entry.get("unit_sha256")
        or selected.get("frozen_payload_sha256")
        != entry.get("frozen_payload_sha256")
        or outcome not in V2_TERMINAL_OUTCOMES
        or outcome != entry.get("outcome")
        or terminal.get("error_code") != entry.get("error_code")
        or terminal.get("finished_at") != entry.get("finished_at")
        or any(
            terminal.get(name) != entry.get(name)
            for name in (
                "package_ref",
                "package_sha256",
                "report_json_ref",
                "report_json_sha256",
                "report_markdown_ref",
                "report_markdown_sha256",
                "report_reopen_status",
            )
        )
        or not _aware_timestamp(terminal.get("finished_at"))
        or not _v2_canary_controls_valid(terminal)
    ):
        _fail("campaign_subject_terminal_v2_invalid")
    gate = _verify_gate_v2(
        runtime_root, key, subject=subject, terminal=terminal
    )
    terminal_with_sha = dict(terminal)
    terminal_with_sha["_terminal_sha256"] = digest
    task_info = _verify_queue_task_v2(
        runtime_root,
        key,
        subject=subject,
        release_id=release_id,
        activation_id=str(terminal.get("activation_id") or ""),
        selected=selected,
        terminal=terminal_with_sha,
    )
    process_execution = terminal.get("process_execution")
    runner_interval = entry.get("runner_interval")
    runner: dict[str, Any] | None = None
    if isinstance(runner_interval, Mapping):
        execution = (
            process_execution
            if isinstance(process_execution, Mapping)
            else None
        )
        if execution is None:
            _fail("campaign_terminal_runner_binding_missing")
        runner = _verify_task_process_closure_v2(
            runtime_root,
            key,
            subject=subject,
            release_id=release_id,
            unit_sha256=str(entry["unit_sha256"]),
            frozen_payload_sha256=str(entry["frozen_payload_sha256"]),
            identity_sha256=str(
                runner_interval.get("task_process_identity_sha256") or ""
            ),
            identity_path=str(
                execution.get("supervisor_process_identity_path") or ""
            ),
            exit_sha256=str(
                runner_interval.get("task_process_exit_sha256") or ""
            ),
            exit_path=str(execution.get("supervisor_process_exit_path") or ""),
            process_execution=execution,
            require_real_provider_closure=(
                evidence_scope == "real_production_hmac_v2"
                and outcome == "succeeded"
            ),
            require_release_task_runner=(
                evidence_scope == "real_production_hmac_v2"
            ),
            execution_contract=execution_contract,
        )
        if (
            runner.get("started_at") != runner_interval.get("started_at")
            or runner.get("finished_at") != runner_interval.get("finished_at")
        ):
            _fail("campaign_terminal_runner_interval_mismatch")
    elif entry.get("runner_evidenced") is True:
        _fail("campaign_terminal_runner_binding_missing")

    success: dict[str, Any] | None = None
    grounding: dict[str, Any] | None = None
    if outcome == "succeeded":
        if (
            terminal.get("error_code") is not None
            or terminal.get("state_after")
            != "continuous_concurrent_unlocked"
            or not isinstance(process_execution, Mapping)
            or runner is None
            or any(
                isinstance(terminal.get(name), bool)
                or not isinstance(terminal.get(name), int)
                or int(terminal[name]) < 1
                for name in (
                    "observed_model_call_count",
                    "observed_provider_request_count",
                    "observed_mcp_tool_call_count",
                )
            )
        ):
            _fail("campaign_subject_success_terminal_invalid")
        grounding = _verify_grounding_v2(
            subject=subject,
            terminal=terminal,
            allowed_evidence_refs=task_info["allowed_evidence_refs"],
        )
        success = _verify_success_chain_v2(
            runtime_root, key, subject=subject, terminal=terminal
        )
    elif terminal.get("state_after") != "failed_drained":
        _fail("campaign_subject_failure_terminal_invalid")
    return {
        "terminal": terminal,
        "terminal_sha256": digest,
        "terminal_path": str(path),
        "terminal_hmac_sha256": terminal["authority"]["hmac_sha256"],
        "gate_sha256": terminal["canary_gate_sha256"],
        "gate_hmac_sha256": gate["authority"]["hmac_sha256"],
        "task_info": task_info,
        "runner": runner,
        "grounding": grounding,
        "success": success,
    }


def _verify_terminal_index_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    state: Mapping[str, Any],
    evidence_scope: str,
    execution_contract: Mapping[str, Any],
) -> dict[str, Any]:
    index, digest, path = _read_hmac_content(
        state.get("terminal_index_path"),
        state.get("terminal_index_sha256"),
        runtime_root=runtime_root,
        expected_root=(
            runtime_root
            / "dispatch"
            / "production-canary"
            / "terminal-indexes"
        ),
        key=key,
        purpose="dispatch-production-canary-terminal-index",
        code="campaign_terminal_index_v2_invalid",
    )
    units = index.get("units")
    counts = index.get("terminal_by_outcome")
    if (
        index.get("schema_version")
        != "study-intake-production-canary-terminal-index-v2"
        or index.get("subject") != subject
        or index.get("release_id") != release_id
        or index.get("activation_id") != state.get("activation_id")
        or index.get("counter_scope")
        != "current_activation_latest_terminal_per_unit"
        or index.get("model_call_count") != 0
        or index.get("provider_request_count") != 0
        or index.get("formal_write_count") != 0
        or index.get("sol_enabled") is not False
        or not isinstance(units, Mapping)
        or index.get("terminal_task_count") != len(units)
        or not isinstance(counts, Mapping)
        or set(counts) != V2_TERMINAL_OUTCOMES
        or dict(counts) != state.get("terminal_by_outcome")
        or index.get("terminal_task_count")
        != state.get("terminal_task_count")
    ):
        _fail("campaign_terminal_index_v2_invalid")
    terminal_results: dict[str, dict[str, Any]] = {}
    intervals: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for unit_sha256, raw_entry in units.items():
        if not isinstance(raw_entry, Mapping) or not _sha(unit_sha256):
            _fail("campaign_terminal_index_entry_invalid")
        entry = dict(raw_entry)
        history = entry.get("history")
        runner_interval = entry.get("runner_interval")
        if (
            entry.get("unit_sha256") != unit_sha256
            or not _sha(entry.get("frozen_payload_sha256"))
            or entry.get("outcome") not in V2_TERMINAL_OUTCOMES
            or not _aware_timestamp(entry.get("admitted_at"))
            or not _aware_timestamp(entry.get("finished_at"))
            or not isinstance(history, list)
            or not history
            or history[-1].get("terminal_receipt_sha256")
            != entry.get("terminal_receipt_sha256")
            or history[-1].get("terminal_receipt_path")
            != entry.get("terminal_receipt_path")
            or entry.get("runner_evidenced")
            is not isinstance(runner_interval, Mapping)
        ):
            _fail("campaign_terminal_index_entry_invalid")
        prior_sha: str | None = None
        history_terminals: list[dict[str, Any]] = []
        for history_row in history:
            if not isinstance(history_row, Mapping):
                _fail("campaign_terminal_history_invalid")
            history_terminal, history_digest, history_path = _read_hmac_content(
                history_row.get("terminal_receipt_path"),
                history_row.get("terminal_receipt_sha256"),
                runtime_root=runtime_root,
                expected_root=(
                    runtime_root
                    / "dispatch"
                    / "production-canary"
                    / "receipts"
                ),
                key=key,
                purpose="dispatch-production-canary-terminal",
                code="campaign_terminal_history_invalid",
            )
            selected = history_terminal.get("selected")
            if (
                history_terminal.get("schema_version")
                != "study-intake-production-canary-terminal-receipt-v2"
                or history_terminal.get("subject") != subject
                or history_terminal.get("release_id") != release_id
                or history_terminal.get("activation_id")
                != state.get("activation_id")
                or not isinstance(selected, Mapping)
                or selected.get("unit_sha256") != unit_sha256
                or history_terminal.get("outcome")
                != history_row.get("outcome")
                or history_terminal.get("error_code")
                != history_row.get("error_code")
                or history_terminal.get("finished_at")
                != history_row.get("finished_at")
                or history_row.get("prior_terminal_receipt_sha256")
                != prior_sha
            ):
                _fail("campaign_terminal_history_invalid")
            prior_sha = history_digest
            history_terminals.append(
                {
                    "terminal": history_terminal,
                    "sha256": history_digest,
                    "path": str(history_path),
                }
            )
        latest_has_execution = isinstance(
            history_terminals[-1]["terminal"].get("process_execution"),
            Mapping,
        )
        latest_entry = dict(entry)
        if runner_interval is not None and not latest_has_execution:
            latest_entry["runner_evidenced"] = False
            latest_entry["runner_interval"] = None
        result = _verify_terminal_receipt_v2(
            runtime_root,
            key,
            subject=subject,
            release_id=release_id,
            state=state,
            entry=latest_entry,
            terminal_sha256=str(entry["terminal_receipt_sha256"]),
            terminal_path=str(entry["terminal_receipt_path"]),
            evidence_scope=evidence_scope,
            execution_contract=execution_contract,
        )
        if runner_interval is not None and result["runner"] is None:
            runner_source = next(
                (
                    item
                    for item in reversed(history_terminals)
                    if isinstance(item["terminal"].get("process_execution"), Mapping)
                ),
                None,
            )
            if runner_source is None:
                _fail("campaign_terminal_runner_history_missing")
            execution = runner_source["terminal"]["process_execution"]
            assert isinstance(execution, Mapping)
            result["runner"] = _verify_task_process_closure_v2(
                runtime_root,
                key,
                subject=subject,
                release_id=release_id,
                unit_sha256=str(unit_sha256),
                frozen_payload_sha256=str(entry["frozen_payload_sha256"]),
                identity_sha256=str(
                    runner_interval.get("task_process_identity_sha256") or ""
                ),
                identity_path=str(
                    execution.get("supervisor_process_identity_path") or ""
                ),
                exit_sha256=str(
                    runner_interval.get("task_process_exit_sha256") or ""
                ),
                exit_path=str(
                    execution.get("supervisor_process_exit_path") or ""
                ),
                process_execution=execution,
                require_real_provider_closure=False,
                require_release_task_runner=(
                    evidence_scope == "real_production_hmac_v2"
                ),
                execution_contract=execution_contract,
            )
        runner = result.get("runner")
        if isinstance(runner, Mapping):
            if (
                runner.get("started_at") != runner_interval.get("started_at")
                or runner.get("finished_at")
                != runner_interval.get("finished_at")
            ):
                _fail("campaign_terminal_runner_interval_mismatch")
            intervals.append(
                {
                    "subject": subject,
                    "unit_sha256": unit_sha256,
                    "started_at": runner["started_at"],
                    "finished_at": runner["finished_at"],
                }
            )
        if entry.get("outcome") != "succeeded":
            terminal = result["terminal"]
            late_fenced = bool(terminal.get("late_result_fenced"))
            if isinstance(runner, Mapping):
                late_fenced = late_fenced or (
                    runner.get("late_result_publish_allowed") is False
                )
            failures.append(
                {
                    "unit_sha256": unit_sha256,
                    "outcome": str(entry["outcome"]),
                    "error_code": entry.get("error_code"),
                    "terminal_kind": str(entry.get("terminal_kind") or "normal"),
                    "terminal_receipt_sha256": str(
                        entry["terminal_receipt_sha256"]
                    ),
                    "late_result_fenced": late_fenced,
                }
            )
        terminal_results[unit_sha256] = result
    expected_counts = {outcome: 0 for outcome in V2_TERMINAL_OUTCOMES}
    for entry in units.values():
        expected_counts[str(entry["outcome"])] += 1
    if expected_counts != dict(counts):
        _fail("campaign_terminal_index_count_invalid")
    return {
        "index": index,
        "index_sha256": digest,
        "index_path": str(path),
        "terminals": terminal_results,
        "intervals": intervals,
        "failure_bindings": failures,
        "terminal_task_count": len(units),
        "runner_evidenced_task_count": len(intervals),
        "runner_interval_missing_count": len(units) - len(intervals),
    }


def _verify_active_selections_v2(
    runtime_root: Path,
    key: bytes,
    *,
    subject: str,
    release_id: str,
    state: Mapping[str, Any],
    evidence_scope: str,
    execution_contract: Mapping[str, Any],
) -> dict[str, Any]:
    active = state.get("active_selections")
    if not isinstance(active, Mapping):
        _fail("campaign_active_selection_invalid")
    verified: dict[str, dict[str, Any]] = {}
    intervals: list[dict[str, Any]] = []
    missing = 0
    for unit_sha256, selected in active.items():
        if not _sha(unit_sha256) or not isinstance(selected, Mapping):
            _fail("campaign_active_selection_invalid")
        task_info = _verify_queue_task_v2(
            runtime_root,
            key,
            subject=subject,
            release_id=release_id,
            activation_id=str(state.get("activation_id") or ""),
            selected=selected,
            terminal=None,
        )
        runner = None
        identity_sha = selected.get("process_identity_sha256")
        identity_path = selected.get("process_identity_path")
        if identity_sha is None:
            if identity_path is not None:
                _fail("campaign_active_runner_binding_invalid")
            missing += 1
        else:
            runner = _verify_task_process_closure_v2(
                runtime_root,
                key,
                subject=subject,
                release_id=release_id,
                unit_sha256=str(unit_sha256),
                frozen_payload_sha256=str(selected["frozen_payload_sha256"]),
                identity_sha256=str(identity_sha),
                identity_path=str(identity_path or ""),
                exit_sha256=None,
                exit_path=None,
                process_execution=None,
                require_real_provider_closure=False,
                require_release_task_runner=(
                    evidence_scope == "real_production_hmac_v2"
                ),
                execution_contract=execution_contract,
            )
            intervals.append(
                {
                    "subject": subject,
                    "unit_sha256": unit_sha256,
                    "started_at": runner["started_at"],
                    "finished_at": None,
                }
            )
        verified[unit_sha256] = {
            "selected": dict(selected),
            "task_info": task_info,
            "runner": runner,
        }
    return {
        "active": verified,
        "intervals": intervals,
        "runner_evidenced_task_count": len(intervals),
        "runner_interval_missing_count": missing,
    }


def _half_open_peak_v2(
    intervals: list[dict[str, Any]], *, observed_at: str
) -> tuple[int, list[str], dict[str, int]]:
    observed = _timestamp(observed_at, "campaign_interval_observed_at_invalid")
    events: list[
        tuple[dt.datetime, int, int, tuple[str, str]]
    ] = []
    subject_intervals: dict[str, list[dict[str, Any]]] = {
        subject: [] for subject in SUBJECTS
    }
    for row in intervals:
        subject = str(row.get("subject") or "")
        unit = str(row.get("unit_sha256") or "")
        if subject not in SUBJECTS or not _sha(unit):
            _fail("campaign_runner_interval_invalid")
        started = _timestamp(
            row.get("started_at"), "campaign_runner_interval_invalid"
        )
        finished = (
            _timestamp(
                row.get("finished_at"), "campaign_runner_interval_invalid"
            )
            if row.get("finished_at") is not None
            else observed
        )
        if finished < started:
            _fail("campaign_runner_interval_invalid")
        subject_intervals[subject].append(row)
        if finished == started:
            continue
        identity = (subject, unit)
        events.append((started, 1, 1, identity))
        events.append((finished, 0, -1, identity))
    active: set[tuple[str, str]] = set()
    peak = 0
    overlap_subjects: set[str] = set()
    for _at, _order, delta, identity in sorted(events):
        if delta < 0:
            if identity not in active:
                _fail("campaign_runner_interval_invalid")
            active.remove(identity)
        else:
            if identity in active:
                _fail("campaign_runner_interval_invalid")
            active.add(identity)
        count = len(active)
        if count > peak:
            peak = count
            overlap_subjects = {subject for subject, _unit in active}
        elif count == peak and count >= 2:
            overlap_subjects.update(subject for subject, _unit in active)

    def subject_peak(rows: list[dict[str, Any]]) -> int:
        local_events: list[tuple[dt.datetime, int, int]] = []
        for row in rows:
            start = _timestamp(
                row.get("started_at"), "campaign_runner_interval_invalid"
            )
            end = (
                _timestamp(
                    row.get("finished_at"), "campaign_runner_interval_invalid"
                )
                if row.get("finished_at") is not None
                else observed
            )
            if end == start:
                continue
            local_events.append((start, 1, 1))
            local_events.append((end, 0, -1))
        current = 0
        local_peak = 0
        for _at, _order, delta in sorted(local_events):
            current += delta
            if current < 0:
                _fail("campaign_runner_interval_invalid")
            local_peak = max(local_peak, current)
        return local_peak

    return (
        peak,
        sorted(overlap_subjects if peak >= 2 else set()),
        {
            subject: subject_peak(subject_intervals[subject])
            for subject in SUBJECTS
        },
    )


def _telemetry_cross_check_v2(
    runtime_root: Path,
    key: bytes,
    *,
    release_id: str,
    terminal_index_sha256s: Mapping[str, str],
    terminal_counts: Mapping[str, int],
    failure_bindings: Mapping[str, list[dict[str, Any]]],
    subject_peaks: Mapping[str, int],
    global_peak: int,
) -> str:
    path = (
        runtime_root
        / "dispatch"
        / "state"
        / "production-canary-concurrency-telemetry.json"
    )
    if not path.exists():
        return "not_available"
    value = _read_regular_json(
        path,
        root=runtime_root,
        code="campaign_concurrency_telemetry_invalid",
    )
    _verify_dispatch_seal(
        value,
        key=key,
        purpose="dispatch-production-canary-concurrency-telemetry",
        code="campaign_concurrency_telemetry_invalid",
    )
    expected_failure_rows = {
        subject: [
            {
                name: row[name]
                for name in (
                    "unit_sha256",
                    "outcome",
                    "error_code",
                    "terminal_kind",
                    "terminal_receipt_sha256",
                )
            }
            for row in failure_bindings[subject]
        ]
        for subject in SUBJECTS
    }
    if (
        value.get("schema_version")
        != "study-intake-production-canary-concurrency-telemetry-v1"
        or value.get("release_id") != release_id
        or value.get("counter_scope")
        != "same_release_current_activation_task_process_lifecycle"
        or value.get("peak_source")
        != "hmac_task_process_identity_and_exit_half_open_intervals"
        or value.get("runner_interval_policy")
        != "half_open_end_before_start_zero_duration_nonoverlap"
        or value.get("terminal_index_sha256_by_subject")
        != dict(terminal_index_sha256s)
        or value.get("terminal_task_count_by_subject")
        != dict(terminal_counts)
        or value.get("terminal_failure_bindings_by_subject")
        != expected_failure_rows
        or value.get("subject_peak_active") != dict(subject_peaks)
        or value.get("global_peak_active") != global_peak
        or "requested_service_tier" not in value
        or value.get("requested_service_tier") is not None
        or value.get("fast_mode_requested") is not False
        or value.get("fast_mode_effective") != "not_requested"
        or value.get("model_call_count") != 0
        or value.get("provider_request_count") != 0
        or value.get("formal_write_count") != 0
        or value.get("sol_enabled") is not False
    ):
        _fail("campaign_concurrency_telemetry_mismatch")
    return "matched_non_authoritative"


def _empty_subject_projection_v2(
    *,
    subject: str,
    state: Mapping[str, Any],
    status: str,
    evidence_scope: str,
) -> dict[str, Any]:
    return {
        "subject": subject,
        "status": status,
        "runtime_state": state["state"],
        "capture_id": None,
        "unit_sha256": None,
        "terminal_outcome": None,
        "terminal_kind": None,
        "error_code": None,
        "stage": "not_started",
        "stage_timeout_seconds": SUBJECT_TIMEOUTS[subject],
        "mcp_namespace": SUBJECT_MCP[subject],
        "mcp_canonical_call_count": 0,
        "analysis_status": "not_started",
        "critical_review_status": "not_started",
        "report_status": "not_started",
        "report_json_ref": None,
        "report_json_sha256": None,
        "report_markdown_ref": None,
        "report_markdown_sha256": None,
        "report_reopen_status": "not_available",
        "package_status": "not_started",
        "package_ref": None,
        "package_sha256": None,
        "completion_sha256": None,
        "processing_receipt_sha256": None,
        "model_call_count": 0,
        "provider_request_count": 0,
        "provider_execution_contract_status": (
            PROVIDER_EXECUTION_FIXTURE
            if evidence_scope == "zero_model_fixture_v2"
            else PROVIDER_EXECUTION_NOT_VERIFIED
        ),
        "provider_stage_identity_sha256s": {
            "analysis": None,
            "critical_review": None,
        },
        "provider_stage_exit_sha256s": {
            "analysis": None,
            "critical_review": None,
        },
        "provider_executable_sha256": None,
        "task_runner_executable_sha256": None,
        "runtime_observed_model_call_count": 0,
        "runtime_observed_provider_request_count": 0,
        "runtime_observed_mcp_tool_call_count": 0,
        "terminal_receipt_sha256": None,
        "terminal_index_sha256": state["terminal_index_sha256"],
        "runner_process_identity_sha256": None,
        "runner_process_exit_sha256": None,
        "runner_started_at": None,
        "runner_finished_at": None,
        "read_session_id": None,
        "evidence_generation": None,
        "evidence_authority_fingerprint": None,
        "evidence_refs": [],
        "late_result_fenced": False,
        "started_at": None,
        "completed_at": None,
    }


def _subject_projection_v2(
    *,
    subject: str,
    state: Mapping[str, Any],
    terminal_index: Mapping[str, Any],
    active: Mapping[str, Any],
    evidence_scope: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    active_rows = active.get("active")
    if not isinstance(active_rows, Mapping):
        _fail("campaign_subject_projection_invalid")
    real_scope = evidence_scope == "real_production_hmac_v2"
    if active_rows:
        selected_unit = (
            str(state.get("selected", {}).get("unit_sha256") or "")
            if isinstance(state.get("selected"), Mapping)
            else ""
        )
        if selected_unit not in active_rows:
            selected_unit = sorted(active_rows)[0]
        selected_row = active_rows[selected_unit]
        selected = selected_row["selected"]
        runner = selected_row.get("runner")
        projection = _empty_subject_projection_v2(
            subject=subject,
            state=state,
            status="running",
            evidence_scope=evidence_scope,
        )
        projection.update(
            {
                "capture_id": selected["producer_unit_id"],
                "unit_sha256": selected_unit,
                "stage": "running",
                "analysis_status": "running",
                "runner_process_identity_sha256": (
                    runner.get("identity_sha256")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "runner_started_at": (
                    runner.get("started_at")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "task_runner_executable_sha256": (
                    runner.get("task_runner_executable_sha256")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "started_at": (
                    runner.get("started_at")
                    if isinstance(runner, Mapping)
                    else None
                ),
            }
        )
        return projection, {
            "selected_unit_sha256": selected_unit,
            "terminal_receipt_sha256": None,
        }

    last_sha = state.get("last_terminal_receipt_sha256")
    terminal_results = terminal_index.get("terminals")
    if last_sha is None:
        state_name = str(state.get("state") or "")
        status = {
            "armed": "awaiting_first_capture",
            "paused_drained": "paused",
            "inactive_rolled_back": "inactive",
        }.get(state_name)
        if status is None:
            _fail("campaign_subject_terminal_projection_missing")
        return _empty_subject_projection_v2(
            subject=subject,
            state=state,
            status=status,
            evidence_scope=evidence_scope,
        ), {"selected_unit_sha256": None, "terminal_receipt_sha256": None}
    if not isinstance(terminal_results, Mapping):
        _fail("campaign_subject_terminal_projection_missing")
    terminal_result = next(
        (
            row
            for row in terminal_results.values()
            if isinstance(row, Mapping)
            and row.get("terminal_sha256") == last_sha
        ),
        None,
    )
    if terminal_result is None:
        _fail("campaign_subject_terminal_projection_missing")
    terminal = terminal_result["terminal"]
    selected = terminal["selected"]
    unit_sha256 = str(selected["unit_sha256"])
    index_entry = terminal_index["index"]["units"][unit_sha256]
    observed_model = int(terminal.get("observed_model_call_count") or 0)
    observed_provider = int(
        terminal.get("observed_provider_request_count") or 0
    )
    observed_mcp = int(terminal.get("observed_mcp_tool_call_count") or 0)
    actual_model = observed_model if real_scope else 0
    actual_provider = observed_provider if real_scope else 0
    actual_mcp = observed_mcp if real_scope else 0
    runner = terminal_result.get("runner")
    late_fenced = bool(terminal.get("late_result_fenced"))
    if isinstance(runner, Mapping):
        late_fenced = late_fenced or (
            terminal.get("outcome") != "succeeded"
            and runner.get("late_result_publish_allowed") is False
        )
    if terminal.get("outcome") == "succeeded":
        grounding = terminal_result.get("grounding")
        success = terminal_result.get("success")
        if not isinstance(grounding, Mapping) or not isinstance(success, Mapping):
            _fail("campaign_subject_success_projection_invalid")
        provider_execution = (
            runner.get("provider_execution")
            if isinstance(runner, Mapping)
            else None
        )
        if real_scope and (
            not isinstance(provider_execution, Mapping)
            or provider_execution.get("status")
            != PROVIDER_EXECUTION_VERIFIED
        ):
            _fail("campaign_provider_execution_contract_invalid")
        status = "verified"
        projection = {
            "subject": subject,
            "status": status,
            "runtime_state": state["state"],
            "capture_id": selected["producer_unit_id"],
            "unit_sha256": unit_sha256,
            "terminal_outcome": "succeeded",
            "terminal_kind": str(index_entry.get("terminal_kind") or "normal"),
            "error_code": None,
            "stage": "report_verified",
            "stage_timeout_seconds": SUBJECT_TIMEOUTS[subject],
            "mcp_namespace": SUBJECT_MCP[subject],
            "mcp_canonical_call_count": actual_mcp,
            "analysis_status": "completed",
            "critical_review_status": "completed",
            "report_status": "reopen_verified",
            "report_json_ref": success["report_json_ref"],
            "report_json_sha256": success["report_json_sha256"],
            "report_markdown_ref": success["report_markdown_ref"],
            "report_markdown_sha256": success[
                "report_markdown_sha256"
            ],
            "report_reopen_status": success["report_reopen_status"],
            "package_status": "reopen_verified",
            "package_ref": success["package_ref"],
            "package_sha256": success["package_sha256"],
            "completion_sha256": success["completion_sha256"],
            "processing_receipt_sha256": success[
                "completion_receipt_sha256"
            ],
            "model_call_count": actual_model,
            "provider_request_count": actual_provider,
            "provider_execution_contract_status": (
                PROVIDER_EXECUTION_VERIFIED
                if real_scope
                else PROVIDER_EXECUTION_FIXTURE
            ),
            "provider_stage_identity_sha256s": (
                copy.deepcopy(
                    provider_execution["stage_identity_sha256s"]
                )
                if real_scope
                else {"analysis": None, "critical_review": None}
            ),
            "provider_stage_exit_sha256s": (
                copy.deepcopy(provider_execution["stage_exit_sha256s"])
                if real_scope
                else {"analysis": None, "critical_review": None}
            ),
            "provider_executable_sha256": (
                provider_execution["provider_executable_sha256"]
                if real_scope
                else None
            ),
            "task_runner_executable_sha256": (
                runner.get("task_runner_executable_sha256")
                if real_scope and isinstance(runner, Mapping)
                else None
            ),
            "runtime_observed_model_call_count": observed_model,
            "runtime_observed_provider_request_count": observed_provider,
            "runtime_observed_mcp_tool_call_count": observed_mcp,
            "terminal_receipt_sha256": terminal_result["terminal_sha256"],
            "terminal_index_sha256": terminal_index["index_sha256"],
            "runner_process_identity_sha256": (
                runner.get("identity_sha256")
                if isinstance(runner, Mapping)
                else None
            ),
            "runner_process_exit_sha256": (
                runner.get("exit_sha256")
                if isinstance(runner, Mapping)
                else None
            ),
            "runner_started_at": (
                runner.get("started_at")
                if isinstance(runner, Mapping)
                else None
            ),
            "runner_finished_at": (
                runner.get("finished_at")
                if isinstance(runner, Mapping)
                else None
            ),
            "read_session_id": grounding["read_session_id"],
            "evidence_generation": grounding["evidence_generation"],
            "evidence_authority_fingerprint": grounding[
                "evidence_authority_fingerprint"
            ],
            "evidence_refs": grounding["evidence_refs"],
            "late_result_fenced": False,
            "started_at": (
                runner.get("started_at")
                if isinstance(runner, Mapping)
                else index_entry["admitted_at"]
            ),
            "completed_at": terminal["finished_at"],
        }
    else:
        projection = _empty_subject_projection_v2(
            subject=subject,
            state=state,
            status="failed_paused",
            evidence_scope=evidence_scope,
        )
        projection.update(
            {
                "capture_id": selected["producer_unit_id"],
                "unit_sha256": unit_sha256,
                "terminal_outcome": terminal["outcome"],
                "terminal_kind": str(
                    index_entry.get("terminal_kind") or "normal"
                ),
                "error_code": terminal.get("error_code"),
                "stage": "failed",
                "analysis_status": "failed",
                "critical_review_status": "failed",
                "report_status": "failed",
                "package_status": "failed",
                "model_call_count": actual_model,
                "provider_request_count": actual_provider,
                "mcp_canonical_call_count": actual_mcp,
                "runtime_observed_model_call_count": observed_model,
                "runtime_observed_provider_request_count": observed_provider,
                "runtime_observed_mcp_tool_call_count": observed_mcp,
                "terminal_receipt_sha256": terminal_result[
                    "terminal_sha256"
                ],
                "runner_process_identity_sha256": (
                    runner.get("identity_sha256")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "runner_process_exit_sha256": (
                    runner.get("exit_sha256")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "runner_started_at": (
                    runner.get("started_at")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "runner_finished_at": (
                    runner.get("finished_at")
                    if isinstance(runner, Mapping)
                    else None
                ),
                "task_runner_executable_sha256": (
                    runner.get("task_runner_executable_sha256")
                    if real_scope and isinstance(runner, Mapping)
                    else None
                ),
                "late_result_fenced": late_fenced,
                "started_at": (
                    runner.get("started_at")
                    if isinstance(runner, Mapping)
                    else index_entry["admitted_at"]
                ),
                "completed_at": terminal["finished_at"],
            }
        )
    return projection, {
        "selected_unit_sha256": unit_sha256,
        "terminal_receipt_sha256": terminal_result["terminal_sha256"],
    }


def build_projection(
    *,
    processing_runtime_root: Path,
    dispatch_authority_key: Path,
    candidate_release_root: Path,
    candidate_release_id: str,
    deployment_authority_key: Path,
    canary_activation_receipt: Path,
    evidence_scope: str,
    generated_at: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if evidence_scope not in V2_EVIDENCE_SCOPES:
        _fail("campaign_evidence_scope_invalid")
    runtime_root = _safe_root(
        processing_runtime_root, "campaign_processing_runtime_invalid"
    )
    release_root = _safe_root(
        candidate_release_root, "campaign_candidate_release_v2_invalid"
    )
    dispatch_key = _read_key(
        dispatch_authority_key, "campaign_dispatch_authority_key_invalid"
    )
    deployment_key = _read_key(
        deployment_authority_key,
        "campaign_deployment_authority_key_invalid",
    )
    (
        release_manifest,
        release_manifest_sha256,
        execution_contract,
    ) = (
        _verify_candidate_release_v2(
            release_root,
            candidate_release_id,
            evidence_scope=evidence_scope,
        )
    )
    global_activation, global_activation_sha256 = (
        _verify_canary_activation_v2(
            canary_activation_receipt,
            key=deployment_key,
            release_id=candidate_release_id,
            release_manifest_sha256=release_manifest_sha256,
        )
    )
    observed_at = generated_at or dt.datetime.now(dt.timezone.utc).isoformat()
    _timestamp(observed_at, "campaign_generated_at_invalid")
    states: dict[str, dict[str, Any]] = {}
    state_sha256s: dict[str, str] = {}
    indexes: dict[str, dict[str, Any]] = {}
    active: dict[str, dict[str, Any]] = {}
    all_intervals: list[dict[str, Any]] = []
    for subject in SUBJECTS:
        state, state_sha = _read_subject_state_v2(
            runtime_root,
            dispatch_key,
            subject=subject,
            release_id=candidate_release_id,
            global_activation=global_activation,
        )
        if (
            state.get("activated_at") != global_activation.get("activated_at")
            or _timestamp(
                state.get("activated_at"),
                "campaign_subject_activation_timeline_invalid",
            )
            > _timestamp(observed_at, "campaign_generated_at_invalid")
        ):
            _fail("campaign_subject_activation_timeline_invalid")
        index = _verify_terminal_index_v2(
            runtime_root,
            dispatch_key,
            subject=subject,
            release_id=candidate_release_id,
            state=state,
            evidence_scope=evidence_scope,
            execution_contract=execution_contract,
        )
        active_rows = _verify_active_selections_v2(
            runtime_root,
            dispatch_key,
            subject=subject,
            release_id=candidate_release_id,
            state=state,
            evidence_scope=evidence_scope,
            execution_contract=execution_contract,
        )
        states[subject] = state
        state_sha256s[subject] = state_sha
        indexes[subject] = index
        active[subject] = active_rows
        all_intervals.extend(index["intervals"])
        all_intervals.extend(active_rows["intervals"])

    global_peak, overlap_subjects, subject_peaks = _half_open_peak_v2(
        all_intervals, observed_at=observed_at
    )
    terminal_index_sha256s = {
        subject: str(indexes[subject]["index_sha256"])
        for subject in SUBJECTS
    }
    terminal_counts = {
        subject: int(indexes[subject]["terminal_task_count"])
        for subject in SUBJECTS
    }
    active_counts = {
        subject: len(active[subject]["active"]) for subject in SUBJECTS
    }
    failure_bindings = {
        subject: indexes[subject]["failure_bindings"]
        for subject in SUBJECTS
    }
    telemetry_status = _telemetry_cross_check_v2(
        runtime_root,
        dispatch_key,
        release_id=candidate_release_id,
        terminal_index_sha256s=terminal_index_sha256s,
        terminal_counts=terminal_counts,
        failure_bindings=failure_bindings,
        subject_peaks=subject_peaks,
        global_peak=global_peak,
    )
    subject_rows: dict[str, dict[str, Any]] = {}
    subject_bindings: dict[str, dict[str, Any]] = {}
    for subject in SUBJECTS:
        row, binding = _subject_projection_v2(
            subject=subject,
            state=states[subject],
            terminal_index=indexes[subject],
            active=active[subject],
            evidence_scope=evidence_scope,
        )
        subject_rows[subject] = row
        subject_bindings[subject] = binding

    real_scope = evidence_scope == "real_production_hmac_v2"
    observed_model_count = sum(
        row["runtime_observed_model_call_count"]
        for row in subject_rows.values()
    )
    observed_provider_count = sum(
        row["runtime_observed_provider_request_count"]
        for row in subject_rows.values()
    )
    observed_mcp_count = sum(
        row["runtime_observed_mcp_tool_call_count"]
        for row in subject_rows.values()
    )
    actual_model_count = sum(
        row["model_call_count"] for row in subject_rows.values()
    )
    actual_provider_count = sum(
        row["provider_request_count"] for row in subject_rows.values()
    )
    actual_mcp_count = sum(
        row["mcp_canonical_call_count"] for row in subject_rows.values()
    )
    statuses = [row["status"] for row in subject_rows.values()]
    if not real_scope:
        status = "test_evidence_only"
        result_label = "zero_model_fixture_evidence_only"
    elif "failed_paused" in statuses:
        status = "production_canary_active_with_subject_failure"
        result_label = "production_canary_subject_failure"
    elif statuses.count("verified") == len(SUBJECTS):
        status = "production_verified"
        result_label = "production_runtime_verified"
    elif "verified" in statuses:
        status = "production_canary_active"
        result_label = "production_canary_partially_verified"
    else:
        status = "production_canary_active"
        result_label = "production_canary_pending"
    campaign_id = hashlib.sha256(
        _compact_bytes(
            {
                "schema_version": DASHBOARD_SCHEMA_VERSION,
                "release_id": candidate_release_id,
                "global_activation_id": global_activation["activation_id"],
                "terminal_index_sha256_by_subject": terminal_index_sha256s,
            },
            newline=False,
        )
    ).hexdigest()
    terminal_task_count = sum(terminal_counts.values())
    active_task_count = sum(active_counts.values())
    runner_evidenced_count = sum(
        indexes[subject]["runner_evidenced_task_count"]
        + active[subject]["runner_evidenced_task_count"]
        for subject in SUBJECTS
    )
    runner_missing_count = sum(
        indexes[subject]["runner_interval_missing_count"]
        + active[subject]["runner_interval_missing_count"]
        for subject in SUBJECTS
    )
    projection = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "evidence_scope": evidence_scope,
        "generated_at": observed_at,
        "campaign_id": campaign_id,
        "release_id": candidate_release_id,
        "current_release_usable": real_scope,
        "status": status,
        "result_label": result_label,
        "production_accepted": False,
        "model_request_contract": copy.deepcopy(
            TARGET_MODEL_REQUEST_CONTRACT
        ),
        "safety": {
            "production_evidence": real_scope,
            "provider_execution": actual_provider_count > 0,
            "model_call_count": actual_model_count,
            "provider_request_count": actual_provider_count,
            "mcp_tool_call_count": actual_mcp_count,
            "runtime_observed_model_call_count": observed_model_count,
            "runtime_observed_provider_request_count": (
                observed_provider_count
            ),
            "runtime_observed_mcp_tool_call_count": observed_mcp_count,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        "canary": {
            "global_activation_id": global_activation["activation_id"],
            "activated_at": global_activation["activated_at"],
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "asynchronous_subject_canary": True,
            "slots": {
                subject: {
                    "subject": subject,
                    "activation_id": states[subject]["activation_id"],
                    "producer_high_watermark_sha256": states[subject][
                        "producer_high_watermark_sha256"
                    ],
                    "runtime_state": states[subject]["state"],
                    "verification_status": subject_rows[subject]["status"],
                    "capture_id": subject_rows[subject]["capture_id"],
                    "terminal_receipt_sha256": subject_rows[subject][
                        "terminal_receipt_sha256"
                    ],
                }
                for subject in SUBJECTS
            },
        },
        "concurrency": {
            "calculation_source": (
                "hmac_terminal_index_task_supervisor_lifecycle"
            ),
            "interval_policy": (
                "half_open_end_before_start_zero_duration_nonoverlap"
            ),
            "terminal_task_count": terminal_task_count,
            "active_task_count": active_task_count,
            "lifecycle_task_count": terminal_task_count + active_task_count,
            "runner_evidenced_task_count": runner_evidenced_count,
            "runner_interval_missing_count": runner_missing_count,
            "global_peak_active": global_peak,
            "subject_peak_active": subject_peaks,
            "overlap_observed": global_peak >= 2,
            "overlap_subjects": overlap_subjects,
            "terminal_index_sha256_by_subject": terminal_index_sha256s,
            "failure_bindings_by_subject": failure_bindings,
            "telemetry_cross_check": telemetry_status,
        },
        "subjects": subject_rows,
    }
    contract_error = campaign_contract_error(projection)
    if contract_error is not None:
        raise CampaignPublishError(contract_error)
    formal = copy.deepcopy(
        dict(release_manifest["test_results"]["formal_surface_gate"])
    )
    source_binding = {
        "schema_version": "study-intake-concurrency-source-binding-v2",
        "evidence_scope": evidence_scope,
        "campaign_id": campaign_id,
        "release_id": candidate_release_id,
        "current_release_usable": real_scope,
        "generated_at": observed_at,
        "global_activation": {
            "receipt_path": str(canary_activation_receipt),
            "receipt_sha256": global_activation_sha256,
            "hmac_sha256": global_activation["authority"]["hmac_sha256"],
        },
        "subjects": {
            subject: {
                "state_sha256": state_sha256s[subject],
                "terminal_index_path": indexes[subject]["index_path"],
                "terminal_index_sha256": indexes[subject]["index_sha256"],
                **subject_bindings[subject],
                "provider_execution_contract_status": subject_rows[subject][
                    "provider_execution_contract_status"
                ],
                "provider_stage_identity_sha256s": copy.deepcopy(
                    subject_rows[subject][
                        "provider_stage_identity_sha256s"
                    ]
                ),
                "provider_stage_exit_sha256s": copy.deepcopy(
                    subject_rows[subject]["provider_stage_exit_sha256s"]
                ),
                "provider_executable_sha256": subject_rows[subject][
                    "provider_executable_sha256"
                ],
                "task_runner_executable_sha256": subject_rows[subject][
                    "task_runner_executable_sha256"
                ],
                "report_json_ref": subject_rows[subject]["report_json_ref"],
                "report_json_sha256": subject_rows[subject][
                    "report_json_sha256"
                ],
                "report_markdown_ref": subject_rows[subject][
                    "report_markdown_ref"
                ],
                "report_markdown_sha256": subject_rows[subject][
                    "report_markdown_sha256"
                ],
                "report_reopen_status": subject_rows[subject][
                    "report_reopen_status"
                ],
                "package_ref": subject_rows[subject]["package_ref"],
                "package_sha256": subject_rows[subject]["package_sha256"],
            }
            for subject in SUBJECTS
        },
        "concurrency_calculation_source": (
            "hmac_terminal_index_task_supervisor_lifecycle"
        ),
        "telemetry_role": "cross_check_only_non_authoritative",
        "formal_surface_guard": {
            "baseline_file_sha256": formal["baseline_file_sha256"],
            "config_file_sha256": formal["config_file_sha256"],
            "baseline_manifest_sha256": formal[
                "baseline_manifest_sha256"
            ],
            "current_manifest_sha256": formal["current_manifest_sha256"],
        },
        "formal_write_count": 0,
        "sol_enabled": False,
        "production_accepted": False,
    }
    return projection, source_binding, formal


def _publish_atomic(path: Path, payload: Mapping[str, Any]) -> str:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        _fail("campaign_state_path_invalid")
    parent = path.parent
    try:
        if parent.resolve(strict=True) != parent or not parent.is_dir():
            _fail("campaign_state_path_invalid")
        if path.exists() or path.is_symlink():
            node = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(node.st_mode):
                _fail("campaign_state_path_invalid")
    except CampaignPublishError:
        raise
    except OSError as exc:
        raise CampaignPublishError("campaign_state_path_invalid") from exc
    raw = _pretty_bytes(payload)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o644)
        handle = os.fdopen(descriptor, "wb")
        descriptor = -1
        with handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    if path.is_symlink() or path.read_bytes() != raw:
        _fail("campaign_state_write_mismatch")
    return hashlib.sha256(raw).hexdigest()


def _safe_output_root(path: Path) -> Path:
    if not path.is_absolute() or path in {Path("/"), Path.home()}:
        _fail("campaign_evidence_root_invalid")
    if path.exists() or path.is_symlink():
        return _safe_root(path, "campaign_evidence_root_invalid")
    parent = _safe_root(path.parent, "campaign_evidence_root_invalid")
    try:
        path.mkdir(mode=0o700)
    except OSError as exc:
        raise CampaignPublishError("campaign_evidence_root_invalid") from exc
    if path.parent != parent:
        _fail("campaign_evidence_root_invalid")
    return _safe_root(path, "campaign_evidence_root_invalid")


def _publish_content_addressed(
    evidence_root: Path,
    collection: str,
    payload: Mapping[str, Any],
) -> tuple[str, Path]:
    if not SAFE_ID_RE.fullmatch(collection):
        _fail("campaign_evidence_collection_invalid")
    raw = _pretty_bytes(payload)
    digest = hashlib.sha256(raw).hexdigest()
    directory = evidence_root / collection / "sha256" / digest[:2]
    try:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if directory.resolve(strict=True) != directory or directory.is_symlink():
            _fail("campaign_evidence_path_invalid")
        path = directory / f"{digest}.json"
        if path.exists() or path.is_symlink():
            existing = _safe_regular_bytes(
                path,
                root=evidence_root,
                maximum=MAX_JSON_BYTES,
                code="campaign_evidence_path_invalid",
            )
            if existing != raw:
                _fail("campaign_evidence_conflict")
            return digest, path
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{digest}.", dir=directory
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o400)
            handle = os.fdopen(descriptor, "wb")
            descriptor = -1
            with handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                existing = _safe_regular_bytes(
                    path,
                    root=evidence_root,
                    maximum=MAX_JSON_BYTES,
                    code="campaign_evidence_path_invalid",
                )
                if existing != raw:
                    _fail("campaign_evidence_conflict")
            os.chmod(path, 0o400)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
    except CampaignPublishError:
        raise
    except OSError as exc:
        raise CampaignPublishError("campaign_evidence_publish_failed") from exc
    return digest, path


def _publish_legacy_release_evidence_v1(
    evidence_root: Path,
    *,
    projection: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    formal_guard: Mapping[str, Any],
) -> dict[str, Any]:
    root = _safe_output_root(evidence_root)
    source_sha, source_path = _publish_content_addressed(
        root, "source-bindings", source_binding
    )
    subject_sources = source_binding["subjects"]
    subjects = projection["subjects"]
    subject_refs: dict[str, dict[str, str]] = {}
    subject_hashes: dict[str, str] = {}
    fingerprints: dict[str, str] = {}
    for subject in SUBJECTS:
        row = subjects[subject]
        source = subject_sources[subject]
        receipt = {
            "schema_version": "study-intake-subject-resume-evidence-v1",
            "status": "passed",
            "subject": subject,
            "release_id": projection["release_id"],
            "mcp_authority_fingerprint": source[
                "mcp_authority_fingerprint"
            ],
            "mcp_read_completed": True,
            "mcp_read_call_count": row["mcp_canonical_call_count"],
            "analysis_completed": True,
            "report_generated": True,
            "report_sha256": row["report_sha256"],
            "requested_service_tier": "priority",
            "effective_service_tier": "requested_unverified",
            "model_call_count": row["model_call_count"],
            "provider_request_count": row["provider_request_count"],
            "real_luna_run_count": 1,
            "formal_surface_sha256": formal_guard[
                "after_manifest_sha256"
            ],
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
            "evidence_refs": [
                f"source-binding:sha256:{source_sha}",
                "mixed-summary:sha256:"
                + str(source_binding["mixed_summary"]["sha256"]),
                "quality-receipt:sha256:"
                + str(source["quality_receipt_sha256"]),
                "analysis-stage-call:sha256:"
                + str(source["analysis_stage_call_receipt_sha256"]),
                "critical-review-stage-call:sha256:"
                + str(source["critical_review_stage_call_receipt_sha256"]),
                "production-canary-activation:sha256:"
                + str(
                    source_binding["production_canary"][
                        "global_activation_receipt_sha256"
                    ]
                ),
                "production-canary-terminal:sha256:"
                + str(
                    source["production_canary"][
                        "terminal_receipt_sha256"
                    ]
                ),
                "production-canary-gate:sha256:"
                + str(
                    source["production_canary"]["canary_gate_sha256"]
                ),
                "report:sha256:" + str(source["report_sha256"]),
                "package:sha256:" + str(source["package_sha256"]),
            ],
        }
        digest, path = _publish_content_addressed(
            root, "subject-receipts", receipt
        )
        subject_refs[subject] = {"path": str(path), "sha256": digest}
        subject_hashes[subject] = digest
        fingerprints[subject] = source["mcp_authority_fingerprint"]
    concurrency = {
        "schema_version": "study-intake-three-subject-concurrency-evidence-v1",
        "status": "passed",
        "release_id": projection["release_id"],
        "subjects": list(SUBJECTS),
        "barrier_expected": projection["barrier"]["expected"],
        "barrier_arrived": projection["barrier"]["arrived"],
        "global_peak_active": projection["barrier"]["global_peak_active"],
        "per_subject_peak_active": copy.deepcopy(
            projection["barrier"]["per_subject_peak_active"]
        ),
        "mcp_read_subject_count": 3,
        "analysis_completed_subject_count": 3,
        "report_generated_count": 3,
        "real_luna_run_count": 3,
        "requested_service_tier": "priority",
        "effective_service_tier": "requested_unverified",
        "model_call_count": projection["safety"]["model_call_count"],
        "provider_request_count": projection["safety"][
            "provider_request_count"
        ],
        "mcp_authority_fingerprints": fingerprints,
        "subject_receipt_sha256s": subject_hashes,
        "formal_surface_sha256": formal_guard["after_manifest_sha256"],
        "formal_write_count": 0,
        "sol_enabled": False,
        "production_accepted": False,
    }
    concurrency_sha, concurrency_path = _publish_content_addressed(
        root, "concurrency-evidence", concurrency
    )
    return {
        "source_binding": {"path": str(source_path), "sha256": source_sha},
        "subject_receipts": subject_refs,
        "concurrency_receipt": {
            "path": str(concurrency_path),
            "sha256": concurrency_sha,
        },
        "mcp_authority_fingerprints": fingerprints,
        "formal_surface_guard": {
            "baseline_file_sha256": formal_guard["baseline_file_sha256"],
            "config_file_sha256": formal_guard["config_file_sha256"],
            "baseline_manifest_sha256": formal_guard[
                "baseline_manifest_sha256"
            ],
            "before_manifest_sha256": formal_guard[
                "before_manifest_sha256"
            ],
            "after_manifest_sha256": formal_guard["after_manifest_sha256"],
        },
    }


def _publish_release_evidence_v2(
    evidence_root: Path,
    *,
    projection: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    formal_guard: Mapping[str, Any],
) -> dict[str, Any]:
    root = _safe_output_root(evidence_root)
    source_sha, source_path = _publish_content_addressed(
        root, "source-bindings-v2", source_binding
    )
    subject_refs: dict[str, dict[str, str]] = {}
    subject_hashes: dict[str, str] = {}
    for subject in SUBJECTS:
        row = projection["subjects"][subject]
        source = source_binding["subjects"][subject]
        evidence_refs = [
            f"source-binding:sha256:{source_sha}",
            "terminal-index:sha256:"
            + str(source["terminal_index_sha256"]),
        ]
        if source.get("terminal_receipt_sha256") is not None:
            evidence_refs.append(
                "production-canary-terminal:sha256:"
                + str(source["terminal_receipt_sha256"])
            )
        if row.get("package_sha256") is not None:
            evidence_refs.append(
                "package:sha256:" + str(row["package_sha256"])
            )
        if row.get("report_json_sha256") is not None:
            evidence_refs.append(
                "report-json:sha256:" + str(row["report_json_sha256"])
            )
        if row.get("report_markdown_sha256") is not None:
            evidence_refs.append(
                "report-markdown:sha256:"
                + str(row["report_markdown_sha256"])
            )
        for stage in ("analysis", "critical_review"):
            identity_sha = row["provider_stage_identity_sha256s"][stage]
            exit_sha = row["provider_stage_exit_sha256s"][stage]
            if identity_sha is not None:
                evidence_refs.append(
                    f"provider-{stage}-identity:sha256:{identity_sha}"
                )
            if exit_sha is not None:
                evidence_refs.append(
                    f"provider-{stage}-exit:sha256:{exit_sha}"
                )
        receipt = {
            "schema_version": "study-intake-subject-canary-evidence-v2",
            "evidence_scope": projection["evidence_scope"],
            "status": row["status"],
            "subject": subject,
            "release_id": projection["release_id"],
            "current_release_usable": projection[
                "current_release_usable"
            ],
            "terminal_index_sha256": source["terminal_index_sha256"],
            "terminal_receipt_sha256": source.get(
                "terminal_receipt_sha256"
            ),
            "mcp_read_completed": row["analysis_status"] == "completed",
            "mcp_read_call_count": row["mcp_canonical_call_count"],
            "analysis_completed": row["analysis_status"] == "completed",
            "critical_review_completed": (
                row["critical_review_status"] == "completed"
            ),
            "report_status": row["report_status"],
            "report_json_ref": row["report_json_ref"],
            "report_json_sha256": row["report_json_sha256"],
            "report_markdown_ref": row["report_markdown_ref"],
            "report_markdown_sha256": row["report_markdown_sha256"],
            "report_reopen_status": row["report_reopen_status"],
            "package_status": row["package_status"],
            "package_ref": row["package_ref"],
            "package_sha256": row["package_sha256"],
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "model_call_count": row["model_call_count"],
            "provider_request_count": row["provider_request_count"],
            "provider_execution_contract_status": row[
                "provider_execution_contract_status"
            ],
            "provider_stage_identity_sha256s": copy.deepcopy(
                row["provider_stage_identity_sha256s"]
            ),
            "provider_stage_exit_sha256s": copy.deepcopy(
                row["provider_stage_exit_sha256s"]
            ),
            "provider_executable_sha256": row[
                "provider_executable_sha256"
            ],
            "task_runner_executable_sha256": row[
                "task_runner_executable_sha256"
            ],
            "runtime_observed_model_call_count": row[
                "runtime_observed_model_call_count"
            ],
            "runtime_observed_provider_request_count": row[
                "runtime_observed_provider_request_count"
            ],
            "runtime_observed_mcp_tool_call_count": row[
                "runtime_observed_mcp_tool_call_count"
            ],
            "formal_surface_sha256": formal_guard[
                "current_manifest_sha256"
            ],
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
            "evidence_refs": evidence_refs,
        }
        digest, path = _publish_content_addressed(
            root, "subject-canary-evidence-v2", receipt
        )
        subject_refs[subject] = {"path": str(path), "sha256": digest}
        subject_hashes[subject] = digest
    concurrency = {
        "schema_version": "study-intake-three-subject-concurrency-evidence-v2",
        "evidence_scope": projection["evidence_scope"],
        "status": projection["status"],
        "result_label": projection["result_label"],
        "release_id": projection["release_id"],
        "campaign_id": projection["campaign_id"],
        "current_release_usable": projection["current_release_usable"],
        "asynchronous_subject_canary": True,
        "calculation_source": projection["concurrency"][
            "calculation_source"
        ],
        "interval_policy": projection["concurrency"]["interval_policy"],
        "global_peak_active": projection["concurrency"][
            "global_peak_active"
        ],
        "subject_peak_active": copy.deepcopy(
            projection["concurrency"]["subject_peak_active"]
        ),
        "overlap_observed": projection["concurrency"][
            "overlap_observed"
        ],
        "overlap_subjects": copy.deepcopy(
            projection["concurrency"]["overlap_subjects"]
        ),
        "terminal_index_sha256_by_subject": copy.deepcopy(
            projection["concurrency"][
                "terminal_index_sha256_by_subject"
            ]
        ),
        "failure_bindings_by_subject": copy.deepcopy(
            projection["concurrency"]["failure_bindings_by_subject"]
        ),
        "requested_service_tier": None,
        "fast_mode_requested": False,
        "fast_mode_effective": "not_requested",
        "model_call_count": projection["safety"]["model_call_count"],
        "provider_request_count": projection["safety"][
            "provider_request_count"
        ],
        "provider_execution_contract_status_by_subject": {
            subject: projection["subjects"][subject][
                "provider_execution_contract_status"
            ]
            for subject in SUBJECTS
        },
        "runtime_observed_model_call_count": projection["safety"][
            "runtime_observed_model_call_count"
        ],
        "runtime_observed_provider_request_count": projection["safety"][
            "runtime_observed_provider_request_count"
        ],
        "runtime_observed_mcp_tool_call_count": projection["safety"][
            "runtime_observed_mcp_tool_call_count"
        ],
        "subject_receipt_sha256s": subject_hashes,
        "formal_surface_sha256": formal_guard[
            "current_manifest_sha256"
        ],
        "formal_write_count": 0,
        "sol_enabled": False,
        "production_accepted": False,
    }
    concurrency_sha, concurrency_path = _publish_content_addressed(
        root, "concurrency-evidence-v2", concurrency
    )
    return {
        "source_binding": {"path": str(source_path), "sha256": source_sha},
        "subject_receipts": subject_refs,
        "concurrency_receipt": {
            "path": str(concurrency_path),
            "sha256": concurrency_sha,
        },
        "formal_surface_guard": {
            "baseline_file_sha256": formal_guard["baseline_file_sha256"],
            "config_file_sha256": formal_guard["config_file_sha256"],
            "baseline_manifest_sha256": formal_guard[
                "baseline_manifest_sha256"
            ],
            "current_manifest_sha256": formal_guard[
                "current_manifest_sha256"
            ],
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish a verified three-subject concurrency Dashboard state",
        allow_abbrev=False,
    )
    parser.add_argument("--processing-runtime-root", required=True, type=Path)
    parser.add_argument("--dispatch-authority-key", required=True, type=Path)
    parser.add_argument("--candidate-release-root", required=True, type=Path)
    parser.add_argument("--candidate-release-id", required=True)
    parser.add_argument("--deployment-authority-key", required=True, type=Path)
    parser.add_argument(
        "--canary-activation-receipt", required=True, type=Path
    )
    parser.add_argument(
        "--evidence-scope",
        required=True,
        choices=sorted(V2_EVIDENCE_SCOPES),
    )
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--state-file", required=True, type=Path)
    return parser


def _parse_subject_paths(values: list[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for raw in values:
        subject, separator, path_text = raw.partition("=")
        if (
            separator != "="
            or subject not in SUBJECTS
            or subject in result
            or not path_text
        ):
            _fail("campaign_canary_terminal_arguments_invalid")
        path = Path(path_text)
        if not path.is_absolute():
            _fail("campaign_canary_terminal_arguments_invalid")
        result[subject] = path
    if set(result) != set(SUBJECTS):
        _fail("campaign_canary_terminal_arguments_invalid")
    return result


def main() -> int:
    args = build_parser().parse_args()
    try:
        projection, source_binding, formal_guard = build_projection(
            processing_runtime_root=args.processing_runtime_root,
            dispatch_authority_key=args.dispatch_authority_key,
            candidate_release_root=args.candidate_release_root,
            candidate_release_id=args.candidate_release_id,
            deployment_authority_key=args.deployment_authority_key,
            canary_activation_receipt=args.canary_activation_receipt,
            evidence_scope=args.evidence_scope,
        )
        evidence = _publish_release_evidence_v2(
            args.evidence_root,
            projection=projection,
            source_binding=source_binding,
            formal_guard=formal_guard,
        )
        state_sha = _publish_atomic(args.state_file, projection)
    except (CampaignPublishError, OSError, KeyError, TypeError, ValueError) as exc:
        code = getattr(exc, "code", "campaign_publish_io_error")
        print(
            json.dumps(
                {"status": "failed_closed", "error_code": code},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": "published",
                "campaign_status": projection["status"],
                "evidence_scope": projection["evidence_scope"],
                "current_release_usable": projection[
                    "current_release_usable"
                ],
                "campaign_id": projection["campaign_id"],
                "release_id": projection["release_id"],
                "state_sha256": state_sha,
                "source_binding_sha256": evidence["source_binding"]["sha256"],
                "concurrency_evidence_sha256": evidence[
                    "concurrency_receipt"
                ]["sha256"],
                "subject_receipt_sha256s": {
                    subject: evidence["subject_receipts"][subject]["sha256"]
                    for subject in SUBJECTS
                },
                "production_accepted": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
