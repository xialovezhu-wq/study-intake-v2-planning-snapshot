#!/usr/bin/env python3
"""Unbounded, fenced two-pass dispatch for frozen preprocessing units.

This module is deliberately independent from :mod:`preprocessor_core`.  It can
be placed in front of existing candidate adapters without changing their
single-worker implementation.  Every unique frozen unit gets its own runner,
execution directory, cancellation token, timeout, heartbeat, and lease fence.

"Unbounded" here means that this layer imposes no worker or per-subject cap: a
thread is started for every newly claimed unique unit.  Operating-system and
upstream service limits still apply and belong outside this application layer.
"""

from __future__ import annotations

import copy
import contextlib
import base64
import binascii
import datetime as dt
import errno
import fcntl
import hashlib
import hmac
import json
import os
import queue
import re
import signal
import subprocess
import tempfile
import threading
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from process_identity import (
    KERNEL_ACTIVITY_COUNTERS,
    KERNEL_ACTIVITY_SNAPSHOT_SCHEMA,
    ProcessIdentityError,
    kernel_activity_snapshot_sha256,
    kernel_process_activity_delta,
    parse_kernel_process_start_token,
    require_kernel_process_start_token,
)
from math_exact_smoke import (
    EXACT_ORDER,
    MathExactSmokeError,
    verify_committed_exact_math_migration_claim,
    verify_exact_task_binding,
)


FINALIZATION_FAILPOINT_ENV = "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT"
ENGLISH_REVIEW_REPAIR_FAILPOINT_ENV = (
    "STUDY_INTAKE_ENGLISH_REVIEW_REPAIR_FAILPOINT"
)


DISPATCH_CONTRACT_SCHEMA = "study-intake-concurrent-dispatch-contract-v1"
DISPATCH_RULE_VERSION = DISPATCH_CONTRACT_SCHEMA
FROZEN_TASK_SCHEMA = "study-intake-frozen-task-v1"
LEASE_SCHEMA = "study-intake-dispatch-lease-v1"
PACKAGE_SCHEMA = "study-intake-preprocess-package-v3"
REVIEW_PACKAGE_SCHEMA = "study-intake-review-candidate-package-v1"
RECEIPT_SCHEMA = "study-intake-concurrent-receipt-v1"
COMPLETION_SCHEMA = "study-intake-concurrent-completion-v2"
TASK_EVENT_SCHEMA = "study-intake-dispatch-task-event-v2"
TASK_DETAIL_SCHEMA = "study-intake-dispatch-task-detail-v2"
STAGE_PROGRESS_RECEIPT_SCHEMA = "study-intake-stage-progress-receipt-v1"
MODEL_STAGE_RAW_OUTPUT_SCHEMA = "study-intake-model-stage-raw-output-v1"
MODEL_STAGE_RAW_CHUNK_SCHEMA = "study-intake-model-stage-raw-chunk-v1"
MODEL_STAGE_RAW_CHAIN_MANIFEST_SCHEMA = (
    "study-intake-model-stage-raw-chain-manifest-v1"
)
MODEL_STAGE_EXECUTION_RECEIPT_SCHEMA = (
    "study-intake-model-stage-execution-receipt-v1"
)
MODEL_STAGE_NORMALIZATION_RECEIPT_SCHEMA = (
    "study-intake-model-stage-normalization-receipt-v1"
)
AUTHORITY_LEDGER_SCHEMA = "study-intake-dispatch-authority-ledger-entry-v1"
EVIDENCE_READINESS_AUTHORITY_SCHEMA = (
    "study-intake-evidence-readiness-authority-v1"
)
ANALYSIS_CHECKPOINT_SCHEMA = "study-intake-analysis-checkpoint-v1"
CONTENT_MEMBER_REGISTRATION_SCHEMA = (
    "study-intake-content-member-registration-v1"
)
CONTENT_MEMBER_PUBLICATION_SCHEMA = (
    "study-intake-content-member-publication-v1"
)
SEMANTIC_REUSE_RECEIPT_SCHEMA = (
    "study-intake-subject-semantic-package-reuse-receipt-v1"
)
CONTROLLED_REPLAY_ALLOWLIST_SCHEMA = (
    "study-intake-controlled-replay-allowlist-v1"
)
CONTROLLED_REPLAY_PURPOSE = "dispatch-controlled-replay-allowlist"
PRODUCER_DISPATCH_INPUT_SCHEMA = "study-intake-producer-dispatch-input-v2"
PRODUCTION_CANARY_ACTIVATION_SCHEMA = (
    "study-intake-production-canary-activation-receipt-v2"
)
PRODUCTION_CANARY_GATE_SCHEMA = (
    "study-intake-production-canary-gate-receipt-v2"
)
LEGACY_PRODUCTION_CANARY_STATE_SCHEMA = (
    "study-intake-production-canary-state-v2"
)
PRODUCTION_CANARY_STATE_SCHEMA = "study-intake-production-canary-state-v3"
KNOWN_PRODUCTION_CANARY_STATE_SCHEMAS = frozenset(
    {
        LEGACY_PRODUCTION_CANARY_STATE_SCHEMA,
        PRODUCTION_CANARY_STATE_SCHEMA,
    }
)
PRODUCTION_CANARY_QUEUE_SCHEMA = "study-intake-production-canary-queue-entry-v2"
PRODUCTION_CANARY_REVIEW_QUEUE_SCHEMA = (
    "study-intake-production-canary-queue-entry-v3"
)
KNOWN_PRODUCTION_CANARY_QUEUE_SCHEMAS = frozenset(
    {PRODUCTION_CANARY_QUEUE_SCHEMA, PRODUCTION_CANARY_REVIEW_QUEUE_SCHEMA}
)
PRODUCTION_CANARY_TERMINAL_SCHEMA = (
    "study-intake-production-canary-terminal-receipt-v3"
)
PRODUCTION_CANARY_REVIEW_TERMINAL_SCHEMA = (
    "study-intake-production-canary-review-terminal-receipt-v1"
)
ENGLISH_PRESERVED_REVIEW_REPAIR_RECEIPT_SCHEMA = (
    "study-intake-english-preserved-review-repair-receipt-v1"
)
ENGLISH_PRESERVED_REVIEW_REPAIR_ROLLBACK_RECEIPT_SCHEMA = (
    "study-intake-english-preserved-review-repair-rollback-receipt-v1"
)
PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA = (
    "study-intake-production-canary-preclaim-failure-receipt-v2"
)
PRODUCTION_CANARY_PRECLAIM_REPAIR_ACK_SCHEMA = (
    "study-intake-production-canary-preclaim-repair-ack-receipt-v2"
)
ENGLISH_QUICK_FLUSH_SUPERSEDE_TERMINAL_SCHEMA = (
    "study-intake-english-quick-flush-supersede-terminal-receipt-v1"
)
ENGLISH_QUICK_FLUSH_SUPERSEDE_STATUS_SCHEMA = (
    "study-intake-english-quick-flush-supersede-status-v1"
)
PRODUCTION_CANARY_CONCURRENCY_TELEMETRY_SCHEMA = (
    "study-intake-production-canary-concurrency-telemetry-v1"
)
PRODUCTION_CANARY_TERMINAL_INDEX_SCHEMA = (
    "study-intake-production-canary-terminal-index-v3"
)
TASK_PROCESS_IDENTITY_SCHEMA = "study-intake-task-process-identity-v1"
TASK_PROCESS_EXIT_SCHEMA = "study-intake-task-process-exit-v1"
PROVIDER_PROCESS_IDENTITY_SCHEMA = (
    "study-intake-provider-process-identity-v1"
)
PROVIDER_PROCESS_EXIT_SCHEMA = "study-intake-provider-process-exit-v1"
PROVIDER_KERNEL_PROBE_RECEIPT_SCHEMA = (
    "study-intake-provider-kernel-probe-receipt-v1"
)
STALE_CLAIM_QUARANTINE_RECEIPT_SCHEMA = (
    "study-intake-stale-claim-quarantine-receipt-v1"
)

# This is intentionally one immutable production incident descriptor.  It is
# not a general failed-task repair API: every identity and mutable preimage is
# pinned to the single preserved English task authorized for this successor.
ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION = {
    "subject": "english",
    "mode": "same_queue_review_reclassification_zero_external_calls",
    "original_preclaim_failure_receipt_sha256": (
        "33548b0f2d6a8a2c73fc24b1dc5b2828241aa6baa1ccc29cf758864eed07525a"
    ),
    "rollover_receipt_sha256": (
        "847883cae478764305ed4964b5e11324da3abf4fe55154816e78070f4d74c3d4"
    ),
    "recovery_supersede_receipt_sha256": (
        "6e9c67a44e69e7b008bece5b4e32d60233283c8611b193ba7c0b37f1a3b0c024"
    ),
    "source_release_id": (
        "b8cc051cd01b7172b48c92f7cea128cb3fcfadf4552da08fef5c9936e122bd00"
    ),
    "source_activation_id": (
        "f3ca26457aa902bfead0b965d89f1d54fc03c4318108bb7d3c78cbae3e71dc89"
    ),
    "capture_id": "EN-20260813-9F1540FD06EE48B0",
    "unit_sha256": (
        "3408c5a0dc067a17df0c3dd8c0b1ea171b5d473923df885ee5c7f4e3d85b2cc6"
    ),
    "frozen_payload_sha256": (
        "565bd68e6c9e76d1cd3a43d0025c373be02c3358ca76f50ea7e4a2e20c8de033"
    ),
    "task_object_sha256": (
        "806336b7688bc22ff68e06d1e2a47836dc338f7b98f012504509fb29bbbf9ce4"
    ),
    "producer_input_contract_sha256": (
        "c5e08ab3cc64a560d6ef11e1b7bd334bd1e04bf42c026a4c58a52ec332b3b7eb"
    ),
    "raw_output_object_sha256": (
        "16efef81d24d1291fafe3208ec4346d0692463162f52eeb518c4d6a267a0259b"
    ),
    "mcp_transport_sha256": (
        "a24fb89f1305296ef334ecfa5262b732d3fcf6f5aa296f42e2f60ecfae1cbe0b"
    ),
    "duplicate_result_projection_sha256": (
        "8ae40e1192c892751e35cb562a123e38eeee803b883a410087d1c43b764ee232"
    ),
    "stage_execution_receipt_sha256": (
        "467340779b727de15f20e6c13f695bfe44f9bf08f6a703721ef06bba1e94110e"
    ),
    "processing_receipt_sha256": (
        "362b35441e0a59e7a7801c75b2d578a62dfc1d6f186ba893983a40b10899c5fe"
    ),
    "mcp_failure_receipt_sha256": (
        "81b08eaf47042cade18f0d1dd11ff3c882133d862bfd4edfe073f144f8dec422"
    ),
    "canary_terminal_receipt_sha256": (
        "9562b475d6f4ff3851b817102653cdf71df72f3e62c5d3bd8382f128d7c9d77e"
    ),
    "subject_terminal_receipt_sha256": (
        "a824d0bfec2c79d1e6d10b8cc8fd479e7159fe96d9935aee9b39990cd8ccec42"
    ),
    "completion_preimage_sha256": (
        "10791a915bdf09b5be5e32f918f43319e5b4ffc4de7a6fc6911784a2539dbb07"
    ),
    "lease_preimage_sha256": (
        "32a95417022382a07c714844820c5a40e700b7ba9b2bf0b574c7f50862d79f55"
    ),
    "task_detail_preimage_sha256": (
        "75f606bc0581dfd125ccc8698024fcb9f2ebb989bee1095396344070c7607c33"
    ),
    "queue_preimage_sha256": (
        "94c1d4dfe43c8a172ab377188e94bc034761b4f8701594c5c7bed01c57f57785"
    ),
    "gate_preimage_sha256": (
        "df28fd72271e9f5008285919fada76905ccddf0d6e59ca8b4e330d3c2af69fbe"
    ),
    "terminal_index_preimage_sha256": (
        "76c2fab0a872b6d7a879b060add8c5fee03a1b4ee36449696202edf146337e9a"
    ),
    "subject_batch_preimage_sha256": (
        "d6b7ba66c92c69040a575b28c35a0cf24b646bb4a6ec690501fb27603f611ea1"
    ),
    "subject_batch_pointer_preimage_sha256": (
        "ba4b4faeb0f00bfc17e8c327ef93581a6009c9ae31be8344efc9a4ce38f43ac9"
    ),
    "subject_writer_preimage_sha256": (
        "ffc3e30309eec819fc16692f069434953ea457a5ee9025ae1786e96227e86a8b"
    ),
    "batch_id": "LUNA-ENGLISH-2026-08-13-2F171BD0B92CE1926349",
}

PRODUCTION_CANARY_PRECLAIM_FAILURE_STAGES = frozenset(
    {
        "producer_contract",
        "materialize",
        "consumer_admission",
        "pre_claim",
        "submit_generation_fence",
    }
)

REQUIRED_MODEL = "gpt-5.6-luna"
REQUIRED_REASONING_EFFORT = "max"
HEARTBEAT_INTERVAL_SECONDS = 15
LEASE_TTL_SECONDS = 120
INITIAL_CANARY_INFLIGHT_LIMIT = 1
DEFAULT_CONTINUOUS_CONCURRENCY_LIMIT = 20
MAX_CONTINUOUS_CONCURRENCY_LIMIT = 64
PROVIDER_ARGV_POLICY_VERSION = "study-intake-provider-argv-no-fast-mode-v1"
PROVIDER_ENVIRONMENT_POLICY_VERSION = (
    "study-intake-provider-environment-no-fast-mode-v1"
)

_DISPATCHER_OWNER_ID_RE = re.compile(
    r"^dispatcher-(?P<pid>[1-9][0-9]*)-(?P<instance>[0-9a-f]{32})$"
)


class DispatchError(RuntimeError):
    """Fail-closed dispatcher error with a stable machine code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


class DispatchCancelled(DispatchError):
    def __init__(self) -> None:
        super().__init__("dispatch_cancelled")


class StageTimeout(DispatchError):
    def __init__(self, stage: str) -> None:
        super().__init__(f"{stage}_timeout")
        self.stage = stage


class StageStalled(DispatchError):
    """A stage with no verified progress after two independent probes."""

    def __init__(self, stage: str) -> None:
        super().__init__(f"{stage}_stalled")
        self.stage = stage


class RetryableDispatchError(DispatchError):
    """A verified per-task upstream wait condition, never a global pause."""


class InfrastructureCrash(DispatchError):
    """A runner infrastructure failure eligible for one fenced recovery."""


def validate_no_fast_mode_provider_argv(
    argv: Sequence[str],
) -> dict[str, Any]:
    """Return the exact argv evidence or reject every fast-mode spelling.

    Provider argv is private task evidence, so retaining the canonical list in
    the HMAC/content-addressed process identity is preferable to a bare digest:
    a later verifier can recompute the digest and independently prove that no
    service-tier override was submitted.
    """

    if (
        not isinstance(argv, Sequence)
        or isinstance(argv, (str, bytes, bytearray))
        or not argv
        or any(not isinstance(item, str) or not item for item in argv)
    ):
        raise DispatchError("provider_argv_invalid")
    exact = list(argv)
    prohibited_markers = (
        "service_tier",
        "service-tier",
        "service.tier",
        "service tier",
        "priority",
    )
    for item in exact:
        normalized = unicodedata.normalize("NFKC", item).casefold()
        if any(marker in normalized for marker in prohibited_markers):
            raise DispatchError("provider_fast_mode_argv_forbidden")
    return {
        "argv_policy_version": PROVIDER_ARGV_POLICY_VERSION,
        "argv": exact,
        "argv_sha256": _sha256_bytes(_canonical_bytes(exact)),
        "requested_service_tier": None,
        "fast_mode_requested": False,
        "fast_mode_effective": "not_requested",
    }


def validate_no_fast_mode_provider_environment(
    environment: Mapping[str, str],
) -> dict[str, Any]:
    """Freeze the inherited key names while keeping every value private."""

    if not isinstance(environment, Mapping) or any(
        not isinstance(key, str)
        or not key
        or not isinstance(value, str)
        for key, value in environment.items()
    ):
        raise DispatchError("provider_environment_invalid")
    key_names = sorted(environment)
    forbidden: list[str] = []
    forbidden_value_keys: list[str] = []
    value_markers = (
        "service_tier",
        "service-tier",
        "service.tier",
        "service tier",
    )
    for key in key_names:
        normalized = unicodedata.normalize("NFKC", key).casefold()
        normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
        if "service_tier" in normalized:
            forbidden.append(key)
        normalized_value = unicodedata.normalize(
            "NFKC", environment[key]
        ).casefold()
        if any(marker in normalized_value for marker in value_markers):
            forbidden_value_keys.append(key)
    if forbidden or forbidden_value_keys:
        raise DispatchError("provider_fast_mode_environment_forbidden")
    return {
        "environment_policy_version": PROVIDER_ENVIRONMENT_POLICY_VERSION,
        "environment_key_names": key_names,
        "environment_key_names_sha256": _sha256_bytes(
            _canonical_bytes(key_names)
        ),
        "forbidden_environment_key_matches": [],
        "forbidden_environment_value_key_matches": [],
    }


_PROCESS_RESOURCE_ERRNOS = frozenset(
    {errno.EAGAIN, errno.ENOMEM, errno.EMFILE, errno.ENFILE}
)
_SERVICE_RETRY_CODES = frozenset(
    {
        "luna_rate_limited",
        "luna_service_rate_limited",
        "luna_service_unavailable",
        "model_rate_limited",
        "rate_limit_exceeded",
        "service_unavailable",
        "service_rate_limited",
        "upstream_rate_limited",
        "upstream_http_429",
    }
)


def _process_resource_error_code(exc: OSError) -> str | None:
    if exc.errno not in _PROCESS_RESOURCE_ERRNOS:
        return None
    name = errno.errorcode.get(exc.errno, f"ERRNO_{exc.errno}").lower()
    return f"process_resource_{name}"


def _thread_start_resource_error_code(exc: BaseException) -> str | None:
    """Return a retry code only for an actual local thread resource failure."""

    if isinstance(exc, OSError):
        return _process_resource_error_code(exc)
    if isinstance(exc, RuntimeError) and str(exc).strip().lower() == (
        "can't start new thread"
    ):
        # CPython converts the platform thread-creation failure into this
        # errno-less RuntimeError.  It is still a local resource wait, never a
        # reason to stop dispatching unrelated tasks.
        return "process_resource_thread_unavailable"
    return None


def _start_thread_or_raise_retry(thread: threading.Thread) -> None:
    try:
        thread.start()
    except (OSError, RuntimeError) as exc:
        code = _thread_start_resource_error_code(exc)
        if code is not None:
            raise RetryableDispatchError(code) from exc
        raise


def _is_process_resource_retry_code(code: str) -> bool:
    return code.strip().lower().startswith("process_resource_")


def _is_service_retry_code(code: str) -> bool:
    normalized = code.strip().lower()
    # Quota/usage exhaustion and deterministic task failures are terminal.
    # Check these exclusions before accepting a provider suffix so a malformed
    # producer cannot disguise them as a server-side queue condition.
    terminal_markers = (
        "usage_limit",
        "quota",
        "evidence",
        "format",
        "timeout",
    )
    if any(marker in normalized for marker in terminal_markers):
        return False
    return normalized in _SERVICE_RETRY_CODES or normalized.endswith(
        (
            "_rate_limited",
            "_rate_limit_exceeded",
            "_upstream_http_429",
            "_service_unavailable",
        )
    )


def _is_infrastructure_crash_code(code: str) -> bool:
    normalized = code.strip().lower()
    return (
        normalized in {
            "infrastructure_crash",
            "runner_infrastructure_crash",
            "task_process_infrastructure_crash",
        }
        or normalized.startswith("runner_stage_exit_")
        or normalized.startswith("task_process_exit_")
    )


def _is_timeout_code(code: str) -> bool:
    """Recognize a task-reported terminal timeout without retrying it."""

    return code.strip().lower().endswith("_timeout")


def dispatch_rule_binding(
    *,
    release_id: str,
    subject: str,
    subject_processing_contract_sha256: str | None,
) -> dict[str, Any]:
    """Bind one task identity to release, subject rules, Luna, and Max."""

    if not isinstance(release_id, str) or not release_id:
        raise DispatchError("dispatch_rule_release_id_invalid")
    if not isinstance(subject, str) or not subject:
        raise DispatchError("dispatch_rule_subject_invalid")
    if subject_processing_contract_sha256 is not None:
        _validate_unit_sha256(subject_processing_contract_sha256)
    rule = {
        "rule_version": DISPATCH_RULE_VERSION,
        "release_id": release_id,
        "subject": subject,
        "subject_processing_contract_sha256": (
            subject_processing_contract_sha256
        ),
        "requested_model": REQUIRED_MODEL,
        "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
    }
    return {
        **rule,
        "rule_version_sha256": _sha256_bytes(_canonical_bytes(rule)),
        # The request argv/config deliberately omit service_tier.  Frozen task
        # evidence keeps the decision explicit so absence cannot be confused
        # with an unverified priority request.
        "requested_service_tier": None,
        "fast_mode_requested": False,
        "fast_mode_effective": "not_requested",
    }


def validate_dispatch_rule_binding(
    contract: Mapping[str, Any],
    *,
    subject: str,
    require_processing_contract: bool,
) -> dict[str, Any]:
    """Fail closed when a frozen dispatch rule checksum has drifted."""

    processing_contract = contract.get(
        "subject_processing_contract_sha256"
    )
    if processing_contract is not None and not isinstance(
        processing_contract, str
    ):
        raise DispatchError("dispatch_rule_processing_contract_invalid")
    if require_processing_contract and processing_contract is None:
        raise DispatchError("dispatch_rule_processing_contract_missing")
    expected = dispatch_rule_binding(
        release_id=str(contract.get("release_id") or ""),
        subject=subject,
        subject_processing_contract_sha256=processing_contract,
    )
    for key, value in expected.items():
        if contract.get(key) != value:
            raise DispatchError("dispatch_rule_binding_mismatch")
    if (
        "requested_service_tier" not in contract
        or contract.get("requested_service_tier") is not None
        or contract.get("fast_mode_requested") is not False
        or contract.get("fast_mode_effective") != "not_requested"
    ):
        raise DispatchError("dispatch_rule_fast_mode_binding_mismatch")
    return expected


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DispatchError("non_json_dispatch_value") from exc


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: object) -> dt.datetime:
    if not isinstance(value, str):
        raise DispatchError("invalid_lease_timestamp")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise DispatchError("invalid_lease_timestamp") from exc
    if parsed.tzinfo is None:
        raise DispatchError("invalid_lease_timestamp")
    return parsed.astimezone(dt.timezone.utc)


def _safe_component(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-+")
    if value and len(value) <= 200 and all(char in allowed for char in value):
        return value
    return _sha256_bytes(value.encode("utf-8"))[:32]


def _validate_unit_sha256(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise DispatchError("unit_sha256_invalid")
    return value


def _atomic_replace_json(path: Path, value: Mapping[str, Any]) -> None:
    """Atomically replace mutable coordination state."""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = _canonical_bytes(value) + b"\n"
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _atomic_replace_bytes(path: Path, payload: bytes) -> None:
    """Atomically restore already-verified physical bytes."""

    if not payload:
        raise DispatchError("atomic_restore_payload_empty")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _publish_content_addressed(root: Path, value: Mapping[str, Any]) -> tuple[str, Path]:
    """Atomically publish immutable bytes and never replace an existing object."""

    payload = _canonical_bytes(value) + b"\n"
    digest = _sha256_bytes(payload)
    path = root / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
    temp_path = Path(name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise DispatchError("content_address_collision")
        os.chmod(path, 0o400)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
    return digest, path


def _publish_content_addressed_bytes(
    root: Path, payload: bytes, *, suffix: str
) -> tuple[str, Path]:
    """Publish immutable non-JSON report bytes by their physical digest."""

    if not payload or suffix not in {".md"}:
        raise DispatchError("content_addressed_bytes_invalid")
    digest = _sha256_bytes(payload)
    path = root / "sha256" / digest[:2] / f"{digest}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
    temp_path = Path(name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise DispatchError("content_address_collision")
        os.chmod(path, 0o400)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
    return digest, path


@dataclass(frozen=True, init=False)
class FrozenTask:
    """Canonical JSON snapshot of one adapter candidate.

    ``frozen_payload`` includes the adapter's complete bounded input.  The unit
    digest therefore deduplicates identical work across subjects, scans, and
    dispatcher processes without relying on a mutable queue identifier.
    """

    _canonical_payload: bytes = field(repr=False, compare=True)
    requested_model: str
    requested_reasoning_effort: str

    def __init__(
        self,
        frozen_payload: Mapping[str, Any],
        requested_model: str = REQUIRED_MODEL,
        requested_reasoning_effort: str = REQUIRED_REASONING_EFFORT,
    ) -> None:
        if requested_model != REQUIRED_MODEL:
            raise DispatchError("requested_model_mismatch")
        if requested_reasoning_effort != REQUIRED_REASONING_EFFORT:
            raise DispatchError("requested_reasoning_effort_mismatch")
        snapshot = copy.deepcopy(dict(frozen_payload))
        canonical = _canonical_bytes(snapshot)
        normalized = json.loads(canonical.decode("utf-8"))
        if not isinstance(normalized, dict):
            raise DispatchError("frozen_task_not_object")
        object.__setattr__(self, "_canonical_payload", canonical)
        object.__setattr__(self, "requested_model", requested_model)
        object.__setattr__(
            self, "requested_reasoning_effort", requested_reasoning_effort
        )

    @property
    def frozen_payload(self) -> Mapping[str, Any]:
        """Return a copy so callers cannot mutate the hashed task snapshot."""

        value = json.loads(self._canonical_payload.decode("utf-8"))
        assert isinstance(value, dict)
        return value

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FrozenTask":
        if value.get("schema_version") == FROZEN_TASK_SCHEMA:
            payload = value.get("frozen_payload")
        else:
            payload = value
        if not isinstance(payload, Mapping):
            raise DispatchError("frozen_task_not_object")
        return cls(
            frozen_payload=payload,
            requested_model=str(value.get("requested_model", REQUIRED_MODEL)),
            requested_reasoning_effort=str(
                value.get("requested_reasoning_effort", REQUIRED_REASONING_EFFORT)
            ),
        )

    @classmethod
    def from_candidate(cls, candidate: object) -> "FrozenTask":
        """Duck-typed bridge for existing ``Candidate`` adapter objects."""

        fields = (
            "subject",
            "capture_id",
            "study_date",
            "recorded_at",
            "input_fingerprint",
            "input_binding",
            "model_input",
            "allowed_evidence_refs",
            "image_paths",
            "target_label",
            "canonical_state",
            "sol_state",
        )
        payload: dict[str, Any] = {}
        for name in fields:
            if not hasattr(candidate, name):
                raise DispatchError(f"candidate_missing_{name}")
            item = getattr(candidate, name)
            if name == "image_paths":
                item = [str(path) for path in item]
            elif isinstance(item, tuple):
                item = list(item)
            payload[name] = copy.deepcopy(item)
        return cls(payload)

    @property
    def unit_sha256(self) -> str:
        payload = self.frozen_payload
        content_processing_id = payload.get("content_processing_id")
        if content_processing_id is not None:
            _validate_unit_sha256(str(content_processing_id))
            contract = payload.get("dispatch_contract")
            if not isinstance(contract, Mapping):
                raise DispatchError("content_processing_contract_missing")
            subject = payload.get("subject")
            release_id = contract.get("release_id")
            rule_version_sha256 = contract.get("rule_version_sha256")
            if (
                not isinstance(subject, str)
                or not subject
                or not isinstance(release_id, str)
                or not release_id
                or not isinstance(rule_version_sha256, str)
            ):
                raise DispatchError("content_processing_contract_invalid")
            _validate_unit_sha256(rule_version_sha256)
            content_contract = {
                "schema_version": "study-intake-content-work-unit-v1",
                "subject": subject,
                "release_id": release_id,
                "rule_version_sha256": rule_version_sha256,
                "content_processing_id": content_processing_id,
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            }
            return _sha256_bytes(_canonical_bytes(content_contract))
        contract = {
            "schema_version": DISPATCH_CONTRACT_SCHEMA,
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "frozen_payload_sha256": _sha256_bytes(self._canonical_payload),
        }
        return _sha256_bytes(_canonical_bytes(contract))

    @property
    def frozen_payload_sha256(self) -> str:
        return _sha256_bytes(self._canonical_payload)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": FROZEN_TASK_SCHEMA,
            "frozen_payload": json.loads(self._canonical_payload.decode("utf-8")),
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "unit_sha256": self.unit_sha256,
        }


@dataclass(frozen=True)
class StageResult:
    payload: Mapping[str, Any]
    runtime_model: str | None
    runtime_reasoning_effort: str | None
    runtime_metadata_provenance: str = "unavailable"
    runtime_identity_status: str | None = None
    duration_ms: int = 0
    read_session_id: str | None = None
    read_session_manifest_sha256: str | None = None
    authority_snapshot_manifest_sha256: str | None = None
    capture_freeze_receipt_sha256: str | None = None
    mcp_read_session_receipt_sha256: str | None = None
    evidence_generation: str | None = None
    evidence_authority_fingerprint: str | None = None
    evidence_subject: str | None = None
    evidence_release_id: str | None = None
    mcp_grounding_manifest_sha256: str | None = None
    mcp_transcript_sha256: str | None = None
    review_mcp_transcript_sha256: str | None = None
    review_mcp_transcript_ref: str | None = None
    mcp_consumed_evidence_refs: tuple[str, ...] = ()
    mcp_cited_evidence_refs: tuple[str, ...] = ()
    mcp_stage_grounded_evidence_refs: tuple[str, ...] = ()
    semantic_stage_count: int = 1
    provider_request_count: int = 1
    mcp_tool_call_count: int = 0
    model_call_count: int = 1
    consumed_terminal_duplicate_read_count: int = 0
    raw_output_object_sha256: str | None = None
    raw_output_object_ref: str | None = None
    stage_execution_receipt_sha256: str | None = None
    stage_execution_receipt_ref: str | None = None
    stage_normalization_receipt_sha256: str | None = None
    stage_normalization_receipt_ref: str | None = None
    normalization_status: str | None = None
    normalization_warning_count: int = 0
    normalization_warnings: tuple[Mapping[str, Any], ...] = ()

    @classmethod
    def coerce(cls, value: object) -> "StageResult":
        if isinstance(value, cls):
            result = value
        elif isinstance(value, Mapping):
            payload = value.get("payload")
            if not isinstance(payload, Mapping):
                raise DispatchError("stage_payload_invalid")
            raw_model = value.get("runtime_model")
            raw_effort = value.get("runtime_reasoning_effort")
            result = cls(
                payload=payload,
                runtime_model=(
                    str(raw_model) if isinstance(raw_model, str) else None
                ),
                runtime_reasoning_effort=(
                    str(raw_effort) if isinstance(raw_effort, str) else None
                ),
                runtime_metadata_provenance=str(
                    value.get("runtime_metadata_provenance", "unavailable")
                ),
                runtime_identity_status=(
                    str(value["runtime_identity_status"])
                    if isinstance(value.get("runtime_identity_status"), str)
                    else None
                ),
                duration_ms=int(value.get("duration_ms", 0)),
                read_session_id=(
                    str(value["read_session_id"])
                    if isinstance(value.get("read_session_id"), str)
                    else None
                ),
                read_session_manifest_sha256=(
                    str(value["read_session_manifest_sha256"])
                    if isinstance(
                        value.get("read_session_manifest_sha256"), str
                    )
                    else None
                ),
                authority_snapshot_manifest_sha256=(
                    str(value["authority_snapshot_manifest_sha256"])
                    if isinstance(
                        value.get("authority_snapshot_manifest_sha256"), str
                    )
                    else None
                ),
                capture_freeze_receipt_sha256=(
                    str(value["capture_freeze_receipt_sha256"])
                    if isinstance(
                        value.get("capture_freeze_receipt_sha256"), str
                    )
                    else None
                ),
                mcp_read_session_receipt_sha256=(
                    str(value["mcp_read_session_receipt_sha256"])
                    if isinstance(
                        value.get("mcp_read_session_receipt_sha256"), str
                    )
                    else None
                ),
                evidence_generation=(
                    str(value["evidence_generation"])
                    if isinstance(value.get("evidence_generation"), str)
                    else None
                ),
                evidence_authority_fingerprint=(
                    str(value["evidence_authority_fingerprint"])
                    if isinstance(
                        value.get("evidence_authority_fingerprint"), str
                    )
                    else None
                ),
                evidence_subject=(
                    str(value["evidence_subject"])
                    if isinstance(value.get("evidence_subject"), str)
                    else None
                ),
                evidence_release_id=(
                    str(value["evidence_release_id"])
                    if isinstance(value.get("evidence_release_id"), str)
                    else None
                ),
                mcp_grounding_manifest_sha256=(
                    str(value["mcp_grounding_manifest_sha256"])
                    if isinstance(
                        value.get("mcp_grounding_manifest_sha256"), str
                    )
                    else None
                ),
                mcp_transcript_sha256=(
                    str(value["mcp_transcript_sha256"])
                    if isinstance(value.get("mcp_transcript_sha256"), str)
                    else None
                ),
                review_mcp_transcript_sha256=(
                    str(value["review_mcp_transcript_sha256"])
                    if isinstance(
                        value.get("review_mcp_transcript_sha256"), str
                    )
                    else None
                ),
                review_mcp_transcript_ref=(
                    str(value["review_mcp_transcript_ref"])
                    if isinstance(
                        value.get("review_mcp_transcript_ref"), str
                    )
                    else None
                ),
                mcp_consumed_evidence_refs=tuple(
                    str(item)
                    for item in value.get("mcp_consumed_evidence_refs", ())
                    if isinstance(item, str)
                ),
                mcp_cited_evidence_refs=tuple(
                    str(item)
                    for item in value.get("mcp_cited_evidence_refs", ())
                    if isinstance(item, str)
                ),
                mcp_stage_grounded_evidence_refs=tuple(
                    str(item)
                    for item in value.get(
                        "mcp_stage_grounded_evidence_refs", ()
                    )
                    if isinstance(item, str)
                ),
                semantic_stage_count=int(value.get("semantic_stage_count", 1)),
                provider_request_count=int(value.get("provider_request_count", 1)),
                mcp_tool_call_count=int(value.get("mcp_tool_call_count", 0)),
                model_call_count=int(value.get("model_call_count", 1)),
                consumed_terminal_duplicate_read_count=int(
                    value.get("consumed_terminal_duplicate_read_count", 0)
                ),
                raw_output_object_sha256=(
                    str(value["raw_output_object_sha256"])
                    if isinstance(value.get("raw_output_object_sha256"), str)
                    else None
                ),
                raw_output_object_ref=(
                    str(value["raw_output_object_ref"])
                    if isinstance(value.get("raw_output_object_ref"), str)
                    else None
                ),
                stage_execution_receipt_sha256=(
                    str(value["stage_execution_receipt_sha256"])
                    if isinstance(
                        value.get("stage_execution_receipt_sha256"), str
                    )
                    else None
                ),
                stage_execution_receipt_ref=(
                    str(value["stage_execution_receipt_ref"])
                    if isinstance(value.get("stage_execution_receipt_ref"), str)
                    else None
                ),
                stage_normalization_receipt_sha256=(
                    str(value["stage_normalization_receipt_sha256"])
                    if isinstance(
                        value.get("stage_normalization_receipt_sha256"), str
                    )
                    else None
                ),
                stage_normalization_receipt_ref=(
                    str(value["stage_normalization_receipt_ref"])
                    if isinstance(
                        value.get("stage_normalization_receipt_ref"), str
                    )
                    else None
                ),
                normalization_status=(
                    str(value["normalization_status"])
                    if isinstance(value.get("normalization_status"), str)
                    else None
                ),
                normalization_warning_count=int(
                    value.get("normalization_warning_count", 0)
                ),
                normalization_warnings=tuple(
                    copy.deepcopy(dict(item))
                    for item in value.get("normalization_warnings", ())
                    if isinstance(item, Mapping)
                ),
            )
        else:
            raise DispatchError("stage_result_invalid")
        identity_status = result.runtime_identity_status
        if identity_status is None:
            identity_status = (
                "confirmed"
                if result.runtime_model == REQUIRED_MODEL
                and result.runtime_reasoning_effort
                == REQUIRED_REASONING_EFFORT
                and result.runtime_metadata_provenance
                == "codex_json_attestation_v1"
                else "requested_unverified"
                if result.runtime_model is None
                and result.runtime_reasoning_effort is None
                and result.runtime_metadata_provenance == "unavailable"
                else "invalid"
            )
            object.__setattr__(result, "runtime_identity_status", identity_status)
        if identity_status == "confirmed":
            if (
                result.runtime_model != REQUIRED_MODEL
                or result.runtime_reasoning_effort
                != REQUIRED_REASONING_EFFORT
                or result.runtime_metadata_provenance
                != "codex_json_attestation_v1"
            ):
                raise DispatchError("runtime_identity_confirmation_invalid")
        elif identity_status == "requested_unverified":
            if (
                result.runtime_model is not None
                or result.runtime_reasoning_effort is not None
                or result.runtime_metadata_provenance != "unavailable"
            ):
                raise DispatchError("runtime_identity_unverified_invalid")
        elif identity_status == "quarantined":
            if (
                not isinstance(result.runtime_model, str)
                and not isinstance(result.runtime_reasoning_effort, str)
            ) or (
                result.runtime_model == REQUIRED_MODEL
                and result.runtime_reasoning_effort
                == REQUIRED_REASONING_EFFORT
            ):
                raise DispatchError("runtime_identity_quarantine_invalid")
        else:
            raise DispatchError("runtime_identity_status_invalid")
        if not isinstance(result.payload, Mapping):
            raise DispatchError("stage_payload_invalid")
        if (
            result.semantic_stage_count != 1
            or result.provider_request_count < 1
            or result.mcp_tool_call_count < 0
            or result.provider_request_count != result.mcp_tool_call_count + 1
            or result.model_call_count != 1
            or result.consumed_terminal_duplicate_read_count != 0
        ):
            raise DispatchError("stage_observability_contract_invalid")
        receipt_hashes = (
            result.raw_output_object_sha256,
            result.stage_execution_receipt_sha256,
            result.stage_normalization_receipt_sha256,
        )
        if any(receipt_hashes):
            if (
                any(
                    not isinstance(value, str) or len(value) != 64
                    for value in receipt_hashes
                )
                or result.raw_output_object_ref
                != "study-intake-model-stage-raw-output://sha256/"
                + str(result.raw_output_object_sha256)
                or result.stage_execution_receipt_ref
                != "study-intake-model-stage-execution://sha256/"
                + str(result.stage_execution_receipt_sha256)
                or result.stage_normalization_receipt_ref
                != "study-intake-model-stage-normalization://sha256/"
                + str(result.stage_normalization_receipt_sha256)
                or result.normalization_status
                not in {"normalized", "normalized_with_warnings"}
                or result.normalization_warning_count
                != len(result.normalization_warnings)
            ):
                raise DispatchError("stage_receipt_closure_invalid")
        if (
            result.review_mcp_transcript_sha256 is None
        ) != (result.review_mcp_transcript_ref is None):
            raise DispatchError("stage_review_transcript_binding_invalid")
        if result.review_mcp_transcript_sha256 is not None:
            _validate_unit_sha256(result.review_mcp_transcript_sha256)
            if result.review_mcp_transcript_ref != (
                "study-intake-mcp-stage-transcript://sha256/"
                + result.review_mcp_transcript_sha256
            ):
                raise DispatchError("stage_review_transcript_binding_invalid")
        grounding_fields = (
            result.read_session_manifest_sha256,
            result.authority_snapshot_manifest_sha256,
            result.capture_freeze_receipt_sha256,
            result.mcp_read_session_receipt_sha256,
            result.evidence_generation,
            result.evidence_authority_fingerprint,
            result.evidence_subject,
            result.evidence_release_id,
            result.mcp_grounding_manifest_sha256,
            result.mcp_transcript_sha256,
        )
        has_grounding = any(value is not None for value in grounding_fields) or any(
            (
                result.mcp_consumed_evidence_refs,
                result.mcp_cited_evidence_refs,
                result.mcp_stage_grounded_evidence_refs,
            )
        )
        if has_grounding:
            consumed = set(result.mcp_consumed_evidence_refs)
            cited = set(result.mcp_cited_evidence_refs)
            grounded = set(result.mcp_stage_grounded_evidence_refs)
            if (
                result.read_session_id is None
                or any(
                    not isinstance(value, str) or not value
                    for value in grounding_fields
                )
                or len(str(result.read_session_manifest_sha256)) != 64
                or len(str(result.authority_snapshot_manifest_sha256)) != 64
                or len(str(result.capture_freeze_receipt_sha256)) != 64
                or len(str(result.mcp_read_session_receipt_sha256)) != 64
                or len(str(result.evidence_authority_fingerprint)) != 64
                or len(str(result.evidence_release_id)) != 64
                or len(str(result.mcp_grounding_manifest_sha256)) != 64
                or len(str(result.mcp_transcript_sha256)) != 64
                or not consumed
                or not cited
                or not grounded
                or not grounded.issubset(consumed.intersection(cited))
                or result.mcp_tool_call_count < 1
            ):
                raise DispatchError("stage_mcp_grounding_contract_invalid")
            for digest in (
                result.read_session_manifest_sha256,
                result.authority_snapshot_manifest_sha256,
                result.capture_freeze_receipt_sha256,
                result.mcp_read_session_receipt_sha256,
                result.evidence_authority_fingerprint,
                result.evidence_release_id,
                result.mcp_grounding_manifest_sha256,
                result.mcp_transcript_sha256,
            ):
                _validate_unit_sha256(str(digest))
        _canonical_bytes(result.payload)
        return result


@dataclass(frozen=True)
class Lease:
    unit_sha256: str
    owner_id: str
    fence: int


@dataclass(frozen=True)
class ClaimDecision:
    status: str
    lease: Lease | None = None
    completion: Mapping[str, Any] | None = None


class _ExclusiveFileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Any = None

    def __enter__(self) -> "_ExclusiveFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.handle = self.path.open("a+b")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *_args: object) -> None:
        assert self.handle is not None
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


class _ExistingSharedFileLock(_ExclusiveFileLock):
    """Share-lock an existing dispatch lock without creating filesystem state."""

    def __enter__(self) -> "_ExistingSharedFileLock":
        try:
            self.handle = self.path.open("rb")
        except OSError as exc:
            raise DispatchError(
                "production_canary_readiness_lock_unavailable"
            ) from exc
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_SH)
        return self


class LeaseStore:
    """Cross-process dedupe, heartbeat, TTL, and monotonic fencing."""

    def __init__(self, runtime_root: Path) -> None:
        self.runtime_root = runtime_root.resolve()
        self.state_root = self.runtime_root / "dispatch" / "state"
        self.lease_root = self.state_root / "leases"
        self.completion_root = self.state_root / "completions"
        self.drain_root = self.state_root / "drain"
        self.latest_root = self.state_root / "latest-authoritative"
        self.content_member_registration_root = (
            self.state_root / "content-members"
        )
        self.content_member_publication_root = (
            self.runtime_root / "dispatch" / "member-publications"
        )
        self.task_event_index_root = self.state_root / "task-events"
        self.task_detail_root = self.state_root / "task-details"
        self.analysis_checkpoint_latest_root = (
            self.state_root / "analysis-checkpoint-latest"
        )
        self.analysis_checkpoint_root = (
            self.runtime_root / "dispatch" / "analysis-checkpoints"
        )
        self.authority_ledger_root = (
            self.runtime_root / "dispatch" / "authority-ledger" / "entries"
        )
        self.evidence_readiness_root = (
            self.runtime_root / "dispatch" / "evidence-readiness"
        )
        self.evidence_readiness_latest_root = (
            self.state_root / "evidence-readiness-latest"
        )
        self.semantic_reuse_receipt_root = (
            self.runtime_root / "dispatch" / "semantic-reuse" / "receipts"
        )
        self.semantic_reuse_latest_root = (
            self.state_root / "semantic-reuse-latest"
        )
        self.controlled_replay_root = (
            self.runtime_root / "dispatch" / "controlled-replay" / "allowlists"
        )
        self.controlled_replay_consumed_root = (
            self.state_root / "controlled-replay-consumed"
        )
        self.production_canary_receipt_root = (
            self.runtime_root / "dispatch" / "production-canary" / "receipts"
        )
        self.production_canary_state_root = (
            self.state_root / "production-canary"
        )
        self.production_canary_queue_root = (
            self.state_root / "production-canary-queue"
        )
        self.production_canary_excluded_root = (
            self.state_root / "production-canary-excluded"
        )
        self.production_canary_preclaim_failure_root = (
            self.state_root / "production-canary-preclaim-failures"
        )
        self.production_canary_quick_flush_supersede_root = (
            self.state_root / "production-canary-quick-flush-supersedes"
        )
        self.production_canary_recovery_finalization_root = (
            self.state_root / "production-canary-recovery-finalizations"
        )
        self.production_canary_recovery_finalization_intent_root = (
            self.state_root
            / "production-canary-recovery-finalization-intents"
        )
        self.production_canary_recovery_staged_activation_root = (
            self.state_root
            / "production-canary-recovery-staged-activations"
        )
        self.production_canary_recovery_finalization_rollback_intent_root = (
            self.state_root
            / "production-canary-recovery-finalization-rollback-intents"
        )
        self.production_canary_concurrency_telemetry_path = (
            self.state_root / "production-canary-concurrency-telemetry.json"
        )
        self.production_canary_terminal_index_root = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "terminal-indexes"
        )
        self.task_process_identity_root = (
            self.runtime_root / "dispatch" / "process-identities"
        )
        self.task_process_identity_latest_root = (
            self.state_root / "process-identity-latest"
        )
        self.task_process_exit_root = (
            self.runtime_root / "dispatch" / "process-exits"
        )
        self.task_process_exit_latest_root = (
            self.state_root / "process-exit-latest"
        )
        self.provider_process_identity_root = (
            self.runtime_root / "dispatch" / "provider-process-identities"
        )
        self.provider_process_identity_latest_root = (
            self.state_root / "provider-process-identity-latest"
        )
        self.provider_process_exit_root = (
            self.runtime_root / "dispatch" / "provider-process-exits"
        )
        self.provider_process_exit_latest_root = (
            self.state_root / "provider-process-exit-latest"
        )
        self.provider_kernel_probe_receipt_root = (
            self.runtime_root / "dispatch" / "provider-kernel-probe-receipts"
        )
        self.provider_kernel_probe_latest_root = (
            self.state_root / "provider-kernel-probe-latest"
        )
        self.model_stage_raw_output_root = (
            self.runtime_root / "dispatch" / "model-stage-raw-outputs"
        )
        self.model_stage_raw_chunk_root = (
            self.runtime_root / "dispatch" / "model-stage-raw-chunks"
        )
        self.model_stage_raw_chunk_latest_root = (
            self.state_root / "model-stage-raw-chunk-latest"
        )
        self.model_stage_raw_chain_manifest_root = (
            self.runtime_root
            / "dispatch"
            / "model-stage-raw-chain-manifests"
        )
        self.model_stage_execution_receipt_root = (
            self.runtime_root / "dispatch" / "model-stage-execution-receipts"
        )
        self.model_stage_normalization_receipt_root = (
            self.runtime_root / "dispatch" / "model-stage-normalization-receipts"
        )
        self.stage_progress_receipt_root = (
            self.runtime_root / "dispatch" / "stage-progress-receipts"
        )
        self.stage_progress_latest_root = (
            self.state_root / "stage-progress-latest"
        )
        self.stale_claim_quarantine_receipt_root = (
            self.runtime_root
            / "dispatch"
            / "stale-claim-quarantine"
            / "receipts"
        )
        self.english_preserved_review_repair_receipt_root = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "english-preserved-review-repairs"
            / "receipts"
        )
        self.english_preserved_review_repair_pointer_path = (
            self.state_root
            / "english-preserved-review-repairs"
            / "authorized-33548.json"
        )
        self.english_preserved_review_repair_rollback_receipt_root = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "english-preserved-review-repairs"
            / "rollbacks"
        )
        self.english_preserved_review_repair_preimage_root = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "english-preserved-review-repairs"
            / "preimages"
        )
        self.english_preserved_review_repair_intent_path = (
            self.state_root
            / "english-preserved-review-repairs"
            / "authorized-33548-intent.json"
        )
        self.authority_key_path = self.state_root / "authority.key"
        self.lock_path = self.state_root / "dispatch.lock"

    def _lease_path(self, unit_sha256: str) -> Path:
        return self.lease_root / f"{_validate_unit_sha256(unit_sha256)}.json"

    def _completion_path(self, unit_sha256: str) -> Path:
        return self.completion_root / f"{_validate_unit_sha256(unit_sha256)}.json"

    def _latest_path(self, subject: str, capture_id: str) -> Path:
        return (
            self.latest_root
            / _safe_component(subject)
            / f"{_safe_component(capture_id)}.json"
        )

    def _content_members_root(self, unit_sha256: str) -> Path:
        return self.content_member_registration_root / _validate_unit_sha256(
            unit_sha256
        )

    def publish_semantic_package_reuse(
        self, material: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Record a verified cross-release package reuse without model work.

        The source package, receipt, authority ledger, evidence capsule, and
        both old and current subject-scoped semantic contracts are bound into
        one immutable HMAC receipt.  No historical object is rewritten.
        """

        required_hashes = (
            "current_release_id",
            "current_loaded_core_sha256",
            "current_processing_contract_sha256",
            "current_code_closure_sha256",
            "source_release_id",
            "source_loaded_core_sha256",
            "source_processing_contract_sha256",
            "source_code_closure_sha256",
            "source_unit_sha256",
            "source_content_processing_id",
            "source_completion_sha256",
            "source_receipt_sha256",
            "source_ledger_entry_sha256",
            "source_package_sha256",
            "semantic_evidence_capsule_sha256",
            "normalized_contract_sha256",
        )
        core = copy.deepcopy(dict(material))
        if (
            core.get("subject") not in {"math", "cs408", "english"}
            or not isinstance(core.get("capture_id"), str)
            or not core["capture_id"]
            or core.get("compatibility_reason")
            not in {
                "exact_subject_semantic_identity",
                "legacy_global_core_only_migration",
                "legacy_subject_bridge_closure_introduction",
            }
            or any(
                not isinstance(core.get(field), str)
                or len(str(core[field])) != 64
                for field in required_hashes
            )
            or core.get("formal_write_count") != 0
            or core.get("model_call_count") != 0
        ):
            raise DispatchError("semantic_package_reuse_material_invalid")
        for field in required_hashes:
            _validate_unit_sha256(str(core[field]))
        reuse_identity = {
            "schema_version": SEMANTIC_REUSE_RECEIPT_SCHEMA,
            **core,
        }
        reuse_id = _sha256_bytes(_canonical_bytes(reuse_identity))
        latest_path = (
            self.semantic_reuse_latest_root
            / _safe_component(str(core["current_release_id"]))
            / _safe_component(str(core["subject"]))
            / f"{_safe_component(str(core['capture_id']))}.json"
        )
        with _ExclusiveFileLock(self.lock_path):
            previous = self._read_object(latest_path)
            if previous is not None:
                self._verify_seal(
                    previous, purpose="dispatch-semantic-reuse-latest"
                )
                if previous.get("reuse_id") == reuse_id:
                    receipt_path = Path(
                        str(previous.get("reuse_receipt_path") or "")
                    )
                    receipt = self._read_object(receipt_path)
                    if receipt is None:
                        raise DispatchError(
                            "semantic_package_reuse_receipt_missing"
                        )
                    self._verify_seal(
                        receipt, purpose="dispatch-semantic-reuse-receipt"
                    )
                    return {
                        "receipt": receipt,
                        "reuse_id": reuse_id,
                        "reuse_receipt_sha256": previous[
                            "reuse_receipt_sha256"
                        ],
                        "reuse_receipt_path": str(receipt_path),
                    }
            receipt = self._seal(
                {
                    **reuse_identity,
                    "reuse_id": reuse_id,
                    "recorded_at": _utc_now(),
                },
                purpose="dispatch-semantic-reuse-receipt",
            )
            receipt_sha256, receipt_path = _publish_content_addressed(
                self.semantic_reuse_receipt_root, receipt
            )
            latest = self._seal(
                {
                    "schema_version": (
                        "study-intake-subject-semantic-package-reuse-latest-v1"
                    ),
                    "subject": core["subject"],
                    "capture_id": core["capture_id"],
                    "current_release_id": core["current_release_id"],
                    "source_release_id": core["source_release_id"],
                    "source_package_sha256": core["source_package_sha256"],
                    "reuse_id": reuse_id,
                    "reuse_receipt_sha256": receipt_sha256,
                    "reuse_receipt_path": str(receipt_path),
                    "formal_write_count": 0,
                },
                purpose="dispatch-semantic-reuse-latest",
            )
            _atomic_replace_json(latest_path, latest)
            return {
                "receipt": receipt,
                "reuse_id": reuse_id,
                "reuse_receipt_sha256": receipt_sha256,
                "reuse_receipt_path": str(receipt_path),
            }

    def register_content_members(self, task: FrozenTask) -> None:
        """Register every submitter before execution or attach it after reuse.

        The registration stores only immutable identity hashes and routing
        fields.  It never copies the model input or result.  Holding the same
        short dispatch lock as terminal publication closes the race where a
        new duplicate arrives while its content owner is finishing.
        """

        frozen = task.frozen_payload
        content_processing_id = frozen.get("content_processing_id")
        if content_processing_id is None:
            return
        _validate_unit_sha256(str(content_processing_id))
        contract = frozen.get("dispatch_contract")
        members = frozen.get("content_group_members")
        subject = frozen.get("subject")
        if (
            not isinstance(contract, Mapping)
            or not isinstance(members, list)
            or not members
            or not isinstance(subject, str)
            or not subject
        ):
            raise DispatchError("content_group_task_binding_invalid")
        release_id = contract.get("release_id")
        rule_version = contract.get("rule_version")
        rule_version_sha256 = contract.get("rule_version_sha256")
        if (
            not isinstance(release_id, str)
            or not release_id
            or not isinstance(rule_version, str)
            or not rule_version
            or not isinstance(rule_version_sha256, str)
        ):
            raise DispatchError("content_group_task_binding_invalid")
        _validate_unit_sha256(rule_version_sha256)
        seen: set[str] = set()
        with _ExclusiveFileLock(self.lock_path):
            for raw_member in members:
                if not isinstance(raw_member, Mapping):
                    raise DispatchError("content_group_members_invalid")
                member = copy.deepcopy(dict(raw_member))
                capture_id = member.get("capture_id")
                if (
                    member.get("subject") != subject
                    or not isinstance(capture_id, str)
                    or not capture_id
                    or capture_id in seen
                    or not isinstance(member.get("study_date"), str)
                    or not isinstance(member.get("input_fingerprint"), str)
                ):
                    raise DispatchError("content_group_members_invalid")
                seen.add(capture_id)
                member_sha256 = _sha256_bytes(_canonical_bytes(member))
                registration = self._seal(
                    {
                        "schema_version": CONTENT_MEMBER_REGISTRATION_SCHEMA,
                        "unit_sha256": task.unit_sha256,
                        "content_processing_id": content_processing_id,
                        "subject": subject,
                        "capture_id": capture_id,
                        "study_date": member["study_date"],
                        "input_fingerprint": member["input_fingerprint"],
                        "member_frozen_payload_sha256": member_sha256,
                        "release_id": release_id,
                        "rule_version": rule_version,
                        "rule_version_sha256": rule_version_sha256,
                        "group_processing_key": (
                            contract.get("math_group_processing_key")
                            if subject == "math"
                            else content_processing_id
                        ),
                        "model": REQUIRED_MODEL,
                        "reasoning_effort": REQUIRED_REASONING_EFFORT,
                        "formal_write_count": 0,
                    },
                    purpose="dispatch-content-member-registration",
                )
                _publish_content_addressed(
                    self._content_members_root(task.unit_sha256),
                    registration,
                )
            completion = self._read_object(
                self._completion_path(task.unit_sha256)
            )
            if completion is not None:
                self._attach_content_member_publications_locked(completion)

    def _registered_content_members_locked(
        self, unit_sha256: str
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        root = self._content_members_root(unit_sha256)
        for path in sorted(root.glob("sha256/*/*.json")):
            raw = path.read_bytes()
            if _sha256_bytes(raw) != path.stem:
                raise DispatchError("content_member_registration_hash_mismatch")
            value = self._read_object(path)
            if value is None:
                raise DispatchError("content_member_registration_missing")
            self._verify_seal(
                value, purpose="dispatch-content-member-registration"
            )
            if (
                value.get("schema_version")
                != CONTENT_MEMBER_REGISTRATION_SCHEMA
                or value.get("unit_sha256") != unit_sha256
                or value.get("model") != REQUIRED_MODEL
                or value.get("reasoning_effort")
                != REQUIRED_REASONING_EFFORT
                or value.get("formal_write_count") != 0
            ):
                raise DispatchError("content_member_registration_invalid")
            rows.append(value)
        return rows

    def verify_content_member_registration(
        self,
        unit_sha256: str,
        capture_id: str,
        *,
        expected_release_id: str,
        expected_input_fingerprint: str,
    ) -> dict[str, Any]:
        """Verify one lightweight member alias without opening model output."""

        _validate_unit_sha256(unit_sha256)
        if not capture_id or not expected_release_id or not expected_input_fingerprint:
            raise DispatchError("content_member_lookup_invalid")
        matches: list[tuple[dict[str, Any], str, Path]] = []
        with _ExclusiveFileLock(self.lock_path):
            root = self._content_members_root(unit_sha256)
            for path in sorted(root.glob("sha256/*/*.json")):
                raw = path.read_bytes()
                digest = _sha256_bytes(raw)
                if digest != path.stem:
                    raise DispatchError(
                        "content_member_registration_hash_mismatch"
                    )
                value = self._read_object(path)
                if value is None:
                    raise DispatchError("content_member_registration_missing")
                self._verify_seal(
                    value, purpose="dispatch-content-member-registration"
                )
                if (
                    value.get("schema_version")
                    != CONTENT_MEMBER_REGISTRATION_SCHEMA
                    or value.get("unit_sha256") != unit_sha256
                ):
                    raise DispatchError("content_member_registration_invalid")
                if (
                    value.get("capture_id") == capture_id
                    and value.get("release_id") == expected_release_id
                    and value.get("input_fingerprint")
                    == expected_input_fingerprint
                ):
                    matches.append((value, digest, path))
            if len(matches) != 1:
                raise DispatchError(
                    "content_member_registration_missing"
                    if not matches
                    else "content_member_registration_ambiguous"
                )
            value, digest, path = matches[0]
            return {
                "registration": value,
                "registration_sha256": digest,
                "registration_path": str(path),
            }

    def _attach_content_member_publications_locked(
        self, completion: Mapping[str, Any]
    ) -> None:
        unit_sha256 = str(completion.get("unit_sha256") or "")
        _validate_unit_sha256(unit_sha256)
        self._verify_seal(completion, purpose="dispatch-completion")
        registrations = self._registered_content_members_locked(unit_sha256)
        if not registrations:
            return
        subject = completion.get("subject")
        release_id = completion.get("release_id")
        rule_version = completion.get("rule_version")
        rule_version_sha256 = completion.get("rule_version_sha256")
        owner_capture_id = completion.get("capture_id")
        content_ids = {row.get("content_processing_id") for row in registrations}
        if (
            len(content_ids) != 1
            or not isinstance(subject, str)
            or not isinstance(owner_capture_id, str)
            or any(
                row.get("subject") != subject
                or row.get("release_id") != release_id
                or row.get("rule_version") != rule_version
                or row.get("rule_version_sha256") != rule_version_sha256
                for row in registrations
            )
        ):
            raise DispatchError("content_member_completion_binding_mismatch")
        content_processing_id = next(iter(content_ids))
        _validate_unit_sha256(str(content_processing_id))
        completion_path = self._completion_path(unit_sha256)
        completion_sha256 = _sha256_bytes(completion_path.read_bytes())
        capture_ids = sorted(
            {str(row["capture_id"]) for row in registrations}
        )
        for row in registrations:
            latest_path = self._latest_path(
                subject, str(row["capture_id"])
            )
            existing_latest = self._read_object(latest_path)
            if existing_latest is not None:
                self._verify_seal(
                    existing_latest, purpose="dispatch-latest"
                )
                if (
                    existing_latest.get("unit_sha256") == unit_sha256
                    and existing_latest.get("completion_sha256")
                    == completion_sha256
                    and existing_latest.get("content_processing_id")
                    == content_processing_id
                    and isinstance(
                        existing_latest.get("member_publication_sha256"),
                        str,
                    )
                ):
                    # A later duplicate may add its own alias, but must never
                    # rewrite an earlier member's already-published reference.
                    continue
            publication = self._seal(
                {
                    "schema_version": CONTENT_MEMBER_PUBLICATION_SCHEMA,
                    "unit_sha256": unit_sha256,
                    "lease_fence": completion.get("lease_fence"),
                    "content_processing_id": content_processing_id,
                    "subject": subject,
                    "capture_id": row["capture_id"],
                    "study_date": row["study_date"],
                    "input_fingerprint": row["input_fingerprint"],
                    "member_frozen_payload_sha256": row[
                        "member_frozen_payload_sha256"
                    ],
                    "group_owner_capture_id": owner_capture_id,
                    "group_capture_ids": capture_ids,
                    "release_id": release_id,
                    "rule_version": rule_version,
                    "rule_version_sha256": rule_version_sha256,
                    "completion_path": str(completion_path),
                    "completion_sha256": completion_sha256,
                    "receipt_sha256": completion.get("receipt_sha256"),
                    "ledger_entry_sha256": completion.get(
                        "ledger_entry_sha256"
                    ),
                    "package_sha256": completion.get("package_sha256"),
                    "outcome": completion.get("outcome"),
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "formal_write_count": 0,
                },
                purpose="dispatch-content-member-publication",
            )
            member_sha256, member_path = _publish_content_addressed(
                self.content_member_publication_root, publication
            )
            latest = self._seal(
                {
                    "schema_version": "study-intake-authoritative-latest-v1",
                    "subject": subject,
                    "capture_id": row["capture_id"],
                    "release_id": release_id,
                    "rule_version": rule_version,
                    "rule_version_sha256": rule_version_sha256,
                    "unit_sha256": unit_sha256,
                    "lease_fence": completion.get("lease_fence"),
                    "completion_path": str(completion_path),
                    "completion_sha256": completion_sha256,
                    "receipt_sha256": completion.get("receipt_sha256"),
                    "ledger_entry_sha256": completion.get(
                        "ledger_entry_sha256"
                    ),
                    "ledger_entry_path": completion.get(
                        "ledger_entry_path"
                    ),
                    "package_sha256": completion.get("package_sha256"),
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "group_owner_capture_id": owner_capture_id,
                    "group_processing_key": row.get(
                        "group_processing_key"
                    ),
                    "group_capture_ids": capture_ids,
                    "content_processing_id": content_processing_id,
                    "member_publication_path": str(member_path),
                    "member_publication_sha256": member_sha256,
                    "updated_at": completion.get("finished_at"),
                },
                purpose="dispatch-latest",
            )
            _atomic_replace_json(
                latest_path, latest
            )

    def _analysis_checkpoint_latest_path(self, unit_sha256: str) -> Path:
        return self.analysis_checkpoint_latest_root / (
            f"{_validate_unit_sha256(unit_sha256)}.json"
        )

    def _read_authority_key(self) -> bytes:
        path = self.authority_key_path
        try:
            info = path.lstat()
            if (
                not path.is_file()
                or path.is_symlink()
                or (info.st_mode & 0o777) != 0o600
            ):
                raise DispatchError("authority_key_permissions_invalid")
            key = path.read_bytes()
        except OSError as exc:
            raise DispatchError("authority_key_unreadable") from exc
        if len(key) != 32:
            raise DispatchError("authority_key_invalid")
        return key

    def _authority_key(self) -> bytes:
        """Load or create the installation-local receipt authority secret."""

        path = self.authority_key_path
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            fd = -1
        if fd >= 0:
            try:
                key = os.urandom(32)
                os.write(fd, key)
                os.fsync(fd)
            finally:
                os.close(fd)
        return self._read_authority_key()

    def _seal(self, value: Mapping[str, Any], *, purpose: str) -> dict[str, Any]:
        core = copy.deepcopy(dict(value))
        core.pop("authority", None)
        key = self._authority_key()
        mac = hmac.new(
            key,
            _canonical_bytes({"purpose": purpose, "payload": core}),
            hashlib.sha256,
        ).hexdigest()
        core["authority"] = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": _sha256_bytes(key),
            "purpose": purpose,
            "hmac_sha256": mac,
        }
        return core

    @staticmethod
    def _verify_seal_with_key(
        value: Mapping[str, Any], *, purpose: str, key: bytes
    ) -> None:
        authority = value.get("authority")
        if not isinstance(authority, Mapping):
            raise DispatchError("authority_proof_missing")
        core = copy.deepcopy(dict(value))
        core.pop("authority", None)
        expected = hmac.new(
            key,
            _canonical_bytes({"purpose": purpose, "payload": core}),
            hashlib.sha256,
        ).hexdigest()
        if (
            authority.get("schema_version")
            != "study-intake-dispatch-authority-v1"
            or authority.get("algorithm") != "HMAC-SHA256"
            or authority.get("key_id") != _sha256_bytes(key)
            or authority.get("purpose") != purpose
            or not hmac.compare_digest(
                str(authority.get("hmac_sha256") or ""), expected
            )
        ):
            raise DispatchError("authority_hmac_invalid")

    def _verify_seal(self, value: Mapping[str, Any], *, purpose: str) -> None:
        self._verify_seal_with_key(
            value,
            purpose=purpose,
            key=self._authority_key(),
        )

    def _verify_seal_read_only(
        self, value: Mapping[str, Any], *, purpose: str
    ) -> None:
        self._verify_seal_with_key(
            value,
            purpose=purpose,
            key=self._read_authority_key(),
        )

    def _production_canary_state_path(self, subject: str) -> Path:
        return self.production_canary_state_root / f"{_safe_component(subject)}.json"

    def _read_canary_identity_read_only(
        self, subject: str, *, configured_release_id: str
    ) -> dict[str, Any] | None:
        """Read only the signed identity needed to isolate a foreign release.

        This deliberately does not reopen the release-specific state or terminal
        index schema.  Atomic state replacement plus the HMAC is sufficient for
        this bounded identity read, and avoiding the dispatch lock keeps target
        release preview audits byte-for-byte read only.
        """

        _validate_unit_sha256(configured_release_id)
        value = self._read_object(self._production_canary_state_path(subject))
        if value is None:
            return None
        self._verify_seal_read_only(
            value, purpose="dispatch-production-canary-state"
        )
        schema_version = value.get("schema_version")
        release_id = value.get("release_id")
        activation_id = value.get("activation_id")
        try:
            if (
                schema_version not in KNOWN_PRODUCTION_CANARY_STATE_SCHEMAS
                or value.get("subject") != subject
                or not isinstance(release_id, str)
                or not isinstance(activation_id, str)
                or isinstance(value.get("formal_write_count"), bool)
                or value.get("formal_write_count") != 0
                or value.get("sol_enabled") is not False
            ):
                raise DispatchError("production_canary_state_invalid")
            _validate_unit_sha256(release_id)
            _validate_unit_sha256(activation_id)
        except DispatchError as exc:
            if exc.code == "production_canary_state_invalid":
                raise
            raise DispatchError("production_canary_state_invalid") from exc
        foreign_release = release_id != configured_release_id
        if (
            not foreign_release
            and schema_version != PRODUCTION_CANARY_STATE_SCHEMA
        ):
            raise DispatchError("production_canary_state_invalid")
        return {
            "schema_version": schema_version,
            "subject": subject,
            "release_id": release_id,
            "activation_id": activation_id,
            "foreign_release": foreign_release,
            "state": value.get("state"),
            "status": value.get("status"),
            "luna_consumer_enabled": value.get("luna_consumer_enabled"),
            "active_task_count": value.get("active_task_count"),
            "selected": copy.deepcopy(value.get("selected")),
            "active_selections": copy.deepcopy(value.get("active_selections")),
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def production_canary_identity_read_only(
        self, subject: str, *, configured_release_id: str
    ) -> dict[str, Any] | None:
        """Return a bounded signed identity without locks or projection writes."""

        return self._read_canary_identity_read_only(
            subject, configured_release_id=configured_release_id
        )

    def _production_canary_queue_subject_root(
        self, subject: str, activation_id: str | None = None
    ) -> Path:
        root = self.production_canary_queue_root / _safe_component(subject)
        if activation_id is None:
            state = self._read_object(self._production_canary_state_path(subject))
            if state is not None:
                activation_id = str(state.get("activation_id") or "")
        if activation_id is None:
            return root
        return root / _validate_unit_sha256(activation_id)

    def _production_canary_queue_path(
        self, subject: str, producer_input_contract_sha256: str
    ) -> Path:
        return self._production_canary_queue_subject_root(subject) / (
            f"{_validate_unit_sha256(producer_input_contract_sha256)}.json"
        )

    def _quick_flush_supersede_activation_root(
        self, state: Mapping[str, Any]
    ) -> Path:
        return (
            self.production_canary_quick_flush_supersede_root
            / "english"
            / _validate_unit_sha256(str(state.get("activation_id") or ""))
        )

    def _quick_flush_supersede_status_path(
        self,
        state: Mapping[str, Any],
        old_producer_input_contract_sha256: str,
    ) -> Path:
        return self._quick_flush_supersede_activation_root(state) / (
            _validate_unit_sha256(old_producer_input_contract_sha256) + ".json"
        )

    def _validate_quick_flush_supersession_evidence_locked(
        self,
        task: FrozenTask,
        evidence: object,
    ) -> dict[str, Any]:
        if not isinstance(evidence, Mapping) or set(evidence) != {
            "schema_version",
            "subject",
            "study_date",
            "source_id",
            "original_event_id",
            "original_event_sha256",
            "quick_flush_intent_id",
            "quick_flush_intent_sha256",
            "quick_flush_capture_receipt_sha256",
            "quick_flush_authority_sha256",
            "supersession_chain",
            "effective_event_id",
            "effective_event_sha256",
            "formal_write_count",
        }:
            raise DispatchError(
                "english_quick_flush_supersession_evidence_invalid"
            )
        payload = task.frozen_payload
        binding = payload.get("input_binding")
        event_ids = (
            binding.get("capture_event_ids")
            if isinstance(binding, Mapping)
            else None
        )
        event_hashes = (
            binding.get("capture_event_sha256")
            if isinstance(binding, Mapping)
            else None
        )
        original_event_id = evidence.get("original_event_id")
        chain = evidence.get("supersession_chain")
        if (
            evidence.get("schema_version")
            != (
                "study-intake-english-quick-flush-"
                "supersession-evidence-v1"
            )
            or evidence.get("subject") != "english"
            or payload.get("subject") != "english"
            or evidence.get("study_date") != payload.get("study_date")
            or not isinstance(binding, Mapping)
            or binding.get("batch_trigger") != "explicit_quick_intake"
            or evidence.get("source_id") != binding.get("source_id")
            or event_ids != [original_event_id]
            or not isinstance(event_hashes, Mapping)
            or set(event_hashes) != {original_event_id}
            or event_hashes.get(original_event_id)
            != evidence.get("original_event_sha256")
            or evidence.get("quick_flush_intent_id")
            != binding.get("quick_flush_intent_id")
            or evidence.get("quick_flush_intent_sha256")
            != binding.get("quick_flush_intent_sha256")
            or evidence.get("quick_flush_capture_receipt_sha256")
            != binding.get("quick_flush_capture_receipt_sha256")
            or evidence.get("quick_flush_authority_sha256")
            != binding.get("quick_flush_authority_sha256")
            or not isinstance(chain, list)
            or not chain
            or evidence.get("formal_write_count") != 0
        ):
            raise DispatchError(
                "english_quick_flush_supersession_evidence_invalid"
            )
        for key in (
            "original_event_sha256",
            "quick_flush_intent_id",
            "quick_flush_intent_sha256",
            "quick_flush_capture_receipt_sha256",
            "quick_flush_authority_sha256",
            "effective_event_sha256",
        ):
            _validate_unit_sha256(str(evidence.get(key) or ""))
        expected_target = str(original_event_id)
        for row in chain:
            if (
                not isinstance(row, Mapping)
                or set(row)
                != {
                    "event_id",
                    "event_sha256",
                    "supersedes_event_id",
                }
                or row.get("supersedes_event_id") != expected_target
                or not isinstance(row.get("event_id"), str)
                or not str(row.get("event_id") or "")
            ):
                raise DispatchError(
                    "english_quick_flush_supersession_evidence_invalid"
                )
            _validate_unit_sha256(str(row.get("event_sha256") or ""))
            expected_target = str(row["event_id"])
        if (
            evidence.get("effective_event_id") != expected_target
            or evidence.get("effective_event_sha256")
            != chain[-1].get("event_sha256")
        ):
            raise DispatchError(
                "english_quick_flush_supersession_evidence_invalid"
            )
        return copy.deepcopy(dict(evidence))

    def _quick_flush_supersede_statuses_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, dict[str, Any]]:
        if state.get("subject") != "english":
            return {}
        root = self._quick_flush_supersede_activation_root(state)
        statuses: dict[str, dict[str, Any]] = {}
        for path in sorted(root.glob("*.json")):
            status = self._read_object(path)
            if status is None:
                raise DispatchError(
                    "english_quick_flush_supersede_status_invalid"
                )
            self._verify_seal(
                status, purpose="dispatch-english-quick-flush-supersede-status"
            )
            contract_sha256 = _validate_unit_sha256(
                str(status.get("producer_input_contract_sha256") or "")
            )
            queue_path = self._production_canary_queue_path(
                "english", contract_sha256
            )
            receipt = self._read_verified_content_addressed_locked(
                status.get("terminal_receipt_path"),
                status.get("terminal_receipt_sha256"),
                root=(
                    self.production_canary_receipt_root
                    / "english"
                    / str(state["activation_id"])
                ),
                purpose=(
                    "dispatch-english-quick-flush-supersede-terminal"
                ),
                error_code=(
                    "english_quick_flush_supersede_terminal_invalid"
                ),
            )
            queue = self._read_object(queue_path)
            try:
                queue_bytes = queue_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "english_quick_flush_supersede_status_invalid"
                ) from exc
            if queue is None:
                raise DispatchError(
                    "english_quick_flush_supersede_status_invalid"
                )
            self._verify_seal(
                queue, purpose="dispatch-production-canary-queue"
            )
            task = self._task_from_canary_queue_entry_locked(queue)
            evidence = self._validate_quick_flush_supersession_evidence_locked(
                task, receipt.get("supersession_evidence")
            )
            if (
                path.name != f"{contract_sha256}.json"
                or status.get("schema_version")
                != ENGLISH_QUICK_FLUSH_SUPERSEDE_STATUS_SCHEMA
                or status.get("status") != "superseded"
                or status.get("subject") != "english"
                or status.get("release_id") != state.get("release_id")
                or status.get("activation_id") != state.get("activation_id")
                or status.get("unit_sha256") != queue.get("unit_sha256")
                or status.get("frozen_payload_sha256")
                != queue.get("frozen_payload_sha256")
                or status.get("queue_entry_path") != str(queue_path)
                or status.get("queue_entry_sha256")
                != _sha256_bytes(queue_bytes)
                or status.get("terminal_outcome") != "superseded"
                or status.get("model_enqueue_allowed") is not False
                or status.get("formal_write_count") != 0
                or receipt.get("schema_version")
                != ENGLISH_QUICK_FLUSH_SUPERSEDE_TERMINAL_SCHEMA
                or receipt.get("subject") != "english"
                or receipt.get("release_id") != state.get("release_id")
                or receipt.get("activation_id") != state.get("activation_id")
                or receipt.get("producer_input_contract_sha256")
                != contract_sha256
                or receipt.get("unit_sha256") != queue.get("unit_sha256")
                or receipt.get("frozen_payload_sha256")
                != queue.get("frozen_payload_sha256")
                or receipt.get("task_object_sha256")
                != queue.get("task_object_sha256")
                or receipt.get("queue_entry_path") != str(queue_path)
                or receipt.get("queue_entry_sha256")
                != _sha256_bytes(queue_bytes)
                or receipt.get("supersession_evidence") != evidence
                or receipt.get("terminal_outcome") != "superseded"
                or receipt.get("terminal_error_code")
                != "english_quick_flush_source_superseded"
                or receipt.get("model_call_count") != 0
                or receipt.get("provider_request_count") != 0
                or receipt.get("mcp_tool_call_count") != 0
                or receipt.get("formal_write_count") != 0
                or receipt.get("sol_enabled") is not False
                or contract_sha256 in statuses
            ):
                raise DispatchError(
                    "english_quick_flush_supersede_status_invalid"
                )
            _parse_utc(receipt.get("terminal_at"))
            _parse_utc(status.get("updated_at"))
            statuses[contract_sha256] = dict(status)
        return statuses

    def _production_canary_exclusion_activation_root(
        self, subject: str, activation_id: str
    ) -> Path:
        return (
            self.production_canary_excluded_root
            / _safe_component(subject)
            / _validate_unit_sha256(activation_id)
        )

    def _production_canary_exclusions_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, dict[str, Any]]:
        subject = str(state["subject"])
        activation_id = str(state["activation_id"])
        receipts: dict[str, dict[str, Any]] = {}
        root = self._production_canary_exclusion_activation_root(
            subject, activation_id
        )
        for path in sorted(root.glob("*.json")):
            value = self._read_object(path)
            if value is None:
                raise DispatchError("production_canary_exclusion_invalid")
            self._verify_seal(
                value, purpose="dispatch-production-canary-exclusion"
            )
            producer_unit_id = value.get("producer_unit_id")
            if (
                value.get("schema_version")
                != "study-intake-production-canary-exclusion-v1"
                or value.get("subject") != subject
                or value.get("activation_id") != activation_id
                or value.get("release_id") != state.get("release_id")
                or value.get("producer_high_watermark_sha256")
                != state.get("producer_high_watermark_sha256")
                or value.get("classification") != "pre_activation_frozen"
                or value.get("model_enqueue_allowed") is not False
                or value.get("formal_write_count") != 0
                or not isinstance(producer_unit_id, str)
                or not producer_unit_id
                or producer_unit_id in receipts
            ):
                raise DispatchError("production_canary_exclusion_invalid")
            _validate_unit_sha256(
                str(value.get("producer_input_contract_sha256") or "")
            )
            _validate_unit_sha256(str(value.get("unit_sha256") or ""))
            _validate_unit_sha256(
                str(value.get("frozen_payload_sha256") or "")
            )
            receipts[producer_unit_id] = dict(value)
        return receipts

    def _production_canary_preclaim_failure_path(
        self,
        subject: str,
        activation_id: str,
        *,
        unit_sha256: str | None,
        frozen_payload_sha256: str | None,
    ) -> Path:
        identity_sha256 = _sha256_bytes(
            _canonical_bytes(
                {
                    "subject": subject,
                    "activation_id": activation_id,
                    "unit_sha256": unit_sha256,
                    "frozen_payload_sha256": frozen_payload_sha256,
                }
            )
        )
        return (
            self.production_canary_preclaim_failure_root
            / _safe_component(subject)
            / _safe_component(activation_id)
            / f"{identity_sha256}.json"
        )

    @staticmethod
    def _canary_control_fields(
        continuous_concurrency_limit: int = (
            DEFAULT_CONTINUOUS_CONCURRENCY_LIMIT
        ),
    ) -> dict[str, Any]:
        if (
            isinstance(continuous_concurrency_limit, bool)
            or not isinstance(continuous_concurrency_limit, int)
            or not 1
            <= continuous_concurrency_limit
            <= MAX_CONTINUOUS_CONCURRENCY_LIMIT
        ):
            raise DispatchError(
                "production_canary_continuous_concurrency_limit_invalid"
            )
        return {
            "producer_capture_enabled": True,
            "sol_formal_curation_enabled": False,
            "post_activation_only": True,
            "initial_canary_inflight_limit": INITIAL_CANARY_INFLIGHT_LIMIT,
            "continuous_concurrency_limit": continuous_concurrency_limit,
            "keep_backlog_drained": True,
            "production_accepted": False,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def _read_canary_state_locked(
        self, subject: str, *, required: bool = True
    ) -> dict[str, Any] | None:
        value = self._read_object(self._production_canary_state_path(subject))
        if value is None:
            if required:
                raise DispatchError("production_canary_not_active")
            return None
        self._verify_seal(value, purpose="dispatch-production-canary-state")
        canary_state = value.get("state")
        canary_status = value.get("status")
        active_task_count = value.get("active_task_count")
        continuous_limit = value.get("continuous_concurrency_limit")
        active_selections = value.get("active_selections")
        if (
            value.get("schema_version") != PRODUCTION_CANARY_STATE_SCHEMA
            or value.get("subject") != subject
            or canary_state
            not in {
                "armed",
                "canary_in_flight",
                "continuous_concurrent_unlocked",
                "failed_drained",
                "paused_drained",
                "inactive_rolled_back",
            }
            or (
                canary_status
                != (
                    "production_canary_inactive"
                    if canary_state == "inactive_rolled_back"
                    else "production_canary_active"
                )
            )
            or value.get("producer_capture_enabled") is not True
            or value.get("sol_formal_curation_enabled") is not False
            or value.get("post_activation_only") is not True
            or value.get("initial_canary_inflight_limit")
            != INITIAL_CANARY_INFLIGHT_LIMIT
            or isinstance(continuous_limit, bool)
            or not isinstance(continuous_limit, int)
            or not 1
            <= int(continuous_limit or 0)
            <= MAX_CONTINUOUS_CONCURRENCY_LIMIT
            or value.get("keep_backlog_drained") is not True
            or value.get("production_accepted") is not False
            or "requested_service_tier" not in value
            or value.get("requested_service_tier") is not None
            or value.get("fast_mode_requested") is not False
            or value.get("fast_mode_effective") != "not_requested"
            or value.get("model_call_count") != 0
            or value.get("provider_request_count") != 0
            or value.get("mcp_tool_call_count") != 0
            or value.get("formal_write_count") != 0
            or value.get("sol_enabled") is not False
            or isinstance(active_task_count, bool)
            or not isinstance(active_task_count, int)
            or not 0 <= active_task_count <= int(continuous_limit or 0)
            or not isinstance(active_selections, Mapping)
            or len(active_selections) != active_task_count
            or not isinstance(value.get("terminal_by_outcome"), Mapping)
            or set(value["terminal_by_outcome"])
            != set(self._empty_canary_terminal_counts())
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                for count in value["terminal_by_outcome"].values()
            )
            or isinstance(value.get("terminal_task_count"), bool)
            or not isinstance(value.get("terminal_task_count"), int)
            or value.get("terminal_task_count") < 0
            or sum(value["terminal_by_outcome"].values())
            != value.get("terminal_task_count")
        ):
            raise DispatchError("production_canary_state_invalid")
        consumer_enabled = value.get("luna_consumer_enabled")
        selected = value.get("selected")
        expected_state_shape = {
            "armed": (True, 0),
            "canary_in_flight": (True, 1),
            "paused_drained": (False, 0),
            "inactive_rolled_back": (False, 0),
        }
        if canary_state in expected_state_shape:
            expected_consumer, expected_count = expected_state_shape[canary_state]
            if (
                consumer_enabled is not expected_consumer
                or active_task_count != expected_count
            ):
                raise DispatchError("production_canary_state_invalid")
        elif canary_state == "continuous_concurrent_unlocked":
            if consumer_enabled is not True:
                raise DispatchError("production_canary_state_invalid")
        elif canary_state == "failed_drained" and consumer_enabled is not False:
            raise DispatchError("production_canary_state_invalid")
        if (
            (canary_state == "canary_in_flight" and not isinstance(selected, Mapping))
            or (canary_state != "canary_in_flight" and selected is not None)
        ):
            raise DispatchError("production_canary_state_invalid")
        if canary_state == "canary_in_flight" and (
            selected != next(iter(active_selections.values()), None)
        ):
            raise DispatchError("production_canary_state_invalid")
        for unit_sha256, active_selected in active_selections.items():
            self._validate_canary_active_selection_locked(
                value, str(unit_sha256), active_selected
            )
        self._read_canary_terminal_index_locked(value)
        backpressure_reason = value.get("backpressure_reason")
        if backpressure_reason not in {
            None,
            "continuous_concurrency_limit_reached",
        }:
            raise DispatchError("production_canary_state_invalid")
        if backpressure_reason is not None and (
            canary_state != "continuous_concurrent_unlocked"
            or active_task_count < int(continuous_limit)
        ):
            raise DispatchError("production_canary_state_invalid")
        return dict(value)

    def _read_verified_content_addressed_locked(
        self,
        path_value: object,
        sha256_value: object,
        *,
        root: Path,
        purpose: str,
        error_code: str,
    ) -> dict[str, Any]:
        if not isinstance(path_value, str) or not isinstance(sha256_value, str):
            raise DispatchError(error_code)
        _validate_unit_sha256(sha256_value)
        path = Path(path_value)
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root.resolve())
            raw = resolved.read_bytes()
        except (OSError, ValueError) as exc:
            raise DispatchError(error_code) from exc
        if _sha256_bytes(raw) != sha256_value:
            raise DispatchError(error_code)
        value = self._read_object(resolved)
        if value is None:
            raise DispatchError(error_code)
        try:
            self._verify_seal(value, purpose=purpose)
        except DispatchError as exc:
            raise DispatchError(error_code) from exc
        return value

    def _validate_canary_active_selection_locked(
        self,
        state: Mapping[str, Any],
        unit_sha256: str,
        selected: object,
    ) -> None:
        required = {
            "producer_unit_id",
            "producer_recorded_at",
            "producer_input_contract_sha256",
            "source_event_set_sha256",
            "unit_sha256",
            "frozen_payload_sha256",
            "canary_gate_sha256",
            "canary_gate_path",
            "canary_gate_authority_sha256",
            "lease_owner_id",
            "lease_fence",
            "context_root",
            "mcp_session_root",
            "report_root",
            "process_identity_sha256",
            "process_identity_path",
        }
        try:
            _validate_unit_sha256(unit_sha256)
            if not isinstance(selected, Mapping) or set(selected) != required:
                raise DispatchError("production_canary_state_invalid")
            for key in (
                "producer_input_contract_sha256",
                "source_event_set_sha256",
                "unit_sha256",
                "frozen_payload_sha256",
                "canary_gate_sha256",
                "canary_gate_authority_sha256",
            ):
                _validate_unit_sha256(str(selected.get(key) or ""))
            if (
                selected.get("unit_sha256") != unit_sha256
                or not isinstance(selected.get("producer_unit_id"), str)
                or not selected.get("producer_unit_id")
                or not isinstance(selected.get("producer_recorded_at"), str)
                or not isinstance(selected.get("lease_owner_id"), str)
                or not selected.get("lease_owner_id")
                or isinstance(selected.get("lease_fence"), bool)
                or not isinstance(selected.get("lease_fence"), int)
                or int(selected["lease_fence"]) < 1
                or (
                    selected.get("process_identity_sha256") is None
                    and selected.get("process_identity_path") is not None
                )
                or (
                    selected.get("process_identity_sha256") is not None
                    and selected.get("process_identity_path") is None
                )
            ):
                raise DispatchError("production_canary_state_invalid")
            _parse_utc(selected["producer_recorded_at"])
            context_root = Path(str(selected.get("context_root") or ""))
            mcp_root = Path(str(selected.get("mcp_session_root") or ""))
            report_root = Path(str(selected.get("report_root") or ""))
            if (
                not context_root.is_absolute()
                or mcp_root != context_root / "mcp-session"
                or report_root != context_root / "reports"
            ):
                raise DispatchError("production_canary_state_invalid")
            queue_path = self._production_canary_queue_path(
                str(state["subject"]),
                str(selected["producer_input_contract_sha256"]),
            )
            queue_entry = self._read_object(queue_path)
            if queue_entry is None:
                raise DispatchError("production_canary_state_invalid")
            self._verify_seal(
                queue_entry, purpose="dispatch-production-canary-queue"
            )
            queue_binding_keys = required - {"canary_gate_path"}
            if (
                queue_entry.get("schema_version")
                != PRODUCTION_CANARY_QUEUE_SCHEMA
                or queue_entry.get("activation_id") != state.get("activation_id")
                or queue_entry.get("release_id") != state.get("release_id")
                or queue_entry.get("subject") != state.get("subject")
                or queue_entry.get("queue_status") != "claimed"
                or any(
                    queue_entry.get(key) != selected.get(key)
                    for key in queue_binding_keys
                )
                or queue_entry.get("canary_gate_path")
                != selected.get("canary_gate_path")
            ):
                raise DispatchError("production_canary_state_invalid")
            gate = self._read_verified_content_addressed_locked(
                selected.get("canary_gate_path"),
                selected.get("canary_gate_sha256"),
                root=self.production_canary_receipt_root,
                purpose="dispatch-production-canary-gate",
                error_code="production_canary_state_invalid",
            )
            gate_selected = gate.get("selected")
            gate_binding_keys = required - {
                "canary_gate_sha256",
                "canary_gate_path",
                "canary_gate_authority_sha256",
                "process_identity_sha256",
                "process_identity_path",
            }
            if (
                gate.get("schema_version") != PRODUCTION_CANARY_GATE_SCHEMA
                or gate.get("activation_id") != state.get("activation_id")
                or gate.get("release_id") != state.get("release_id")
                or gate.get("subject") != state.get("subject")
                or not isinstance(gate_selected, Mapping)
                or gate_selected.get("process_identity_sha256") is not None
                or gate_selected.get("process_identity_path") is not None
                or any(
                    gate_selected.get(key) != selected.get(key)
                    for key in gate_binding_keys
                )
                or _sha256_bytes(_canonical_bytes(gate.get("authority")))
                != selected.get("canary_gate_authority_sha256")
            ):
                raise DispatchError("production_canary_state_invalid")
            if selected.get("process_identity_sha256") is not None:
                identity = self._read_verified_content_addressed_locked(
                    selected.get("process_identity_path"),
                    selected.get("process_identity_sha256"),
                    root=self.task_process_identity_root,
                    purpose="dispatch-task-process-identity",
                    error_code="production_canary_state_invalid",
                )
                if (
                    identity.get("schema_version") != TASK_PROCESS_IDENTITY_SCHEMA
                    or identity.get("unit_sha256") != unit_sha256
                    or identity.get("frozen_payload_sha256")
                    != selected.get("frozen_payload_sha256")
                    or identity.get("subject") != state.get("subject")
                    or identity.get("release_id") != state.get("release_id")
                    or identity.get("owner_id") != selected.get("lease_owner_id")
                    or identity.get("lease_fence") != selected.get("lease_fence")
                    or identity.get("context_root") != selected.get("context_root")
                    or identity.get("expected_mcp_session_root")
                    != selected.get("mcp_session_root")
                    or identity.get("expected_report_root")
                    != selected.get("report_root")
                ):
                    raise DispatchError("production_canary_state_invalid")
        except (DispatchError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, DispatchError) and exc.code == "production_canary_state_invalid":
                raise
            raise DispatchError("production_canary_state_invalid") from exc

    def _write_canary_state_locked(
        self, state: Mapping[str, Any], *, updated_at: str | None = None
    ) -> dict[str, Any]:
        core = copy.deepcopy(dict(state))
        core.pop("authority", None)
        core["updated_at"] = updated_at or _utc_now()
        sealed = self._seal(core, purpose="dispatch-production-canary-state")
        _atomic_replace_json(
            self._production_canary_state_path(str(core["subject"])), sealed
        )
        return sealed

    @staticmethod
    def _half_open_interval_peak(
        intervals: Sequence[
            tuple[dt.datetime, dt.datetime | None, str]
        ],
    ) -> tuple[int, int]:
        """Return a conservative peak for [start,end) runner intervals.

        End events sort before starts at the same instant.  Zero-duration
        intervals are evidenced but contribute no concurrency, preventing
        timestamp ties from manufacturing a peak.
        """

        events: list[tuple[dt.datetime, int, int, str]] = []
        zero_duration_count = 0
        for started_at, finished_at, unit_sha256 in intervals:
            if finished_at is not None and finished_at < started_at:
                raise DispatchError(
                    "production_canary_runner_interval_invalid"
                )
            if finished_at is not None and finished_at == started_at:
                zero_duration_count += 1
                continue
            events.append((started_at, 1, 1, unit_sha256))
            if finished_at is not None:
                events.append((finished_at, 0, -1, unit_sha256))
        active_count = 0
        peak = 0
        for _at, _order, delta, _unit in sorted(events):
            active_count += delta
            if active_count < 0:
                raise DispatchError(
                    "production_canary_runner_interval_invalid"
                )
            peak = max(peak, active_count)
        return peak, zero_duration_count

    def _recompute_canary_concurrency_telemetry_locked(
        self, release_id: str, *, updated_at: str | None = None
    ) -> dict[str, Any]:
        _validate_unit_sha256(release_id)
        subjects = ("math", "cs408", "english")
        existing = self._read_object(
            self.production_canary_concurrency_telemetry_path
        )
        if existing is not None:
            self._verify_seal(
                existing,
                purpose="dispatch-production-canary-concurrency-telemetry",
            )
            if (
                existing.get("schema_version")
                != PRODUCTION_CANARY_CONCURRENCY_TELEMETRY_SCHEMA
            ):
                raise DispatchError(
                    "production_canary_concurrency_telemetry_invalid"
                )
            if (
                existing.get("release_id") != release_id
                and int(existing.get("global_active_task_count") or 0) != 0
            ):
                raise DispatchError(
                    "production_canary_concurrency_release_conflict"
                )

        activation_ids: dict[str, str | None] = {
            subject: None for subject in subjects
        }
        active_by_subject = {subject: 0 for subject in subjects}
        terminal_index_sha256_by_subject: dict[str, str | None] = {
            subject: None for subject in subjects
        }
        terminal_task_count_by_subject = {subject: 0 for subject in subjects}
        terminal_by_outcome_by_subject = {
            subject: self._empty_canary_terminal_counts()
            for subject in subjects
        }
        terminal_failure_bindings_by_subject: dict[
            str, list[dict[str, Any]]
        ] = {subject: [] for subject in subjects}
        scheduler_intervals: dict[
            str, list[tuple[dt.datetime, dt.datetime | None, str]]
        ] = {subject: [] for subject in subjects}
        runner_intervals: dict[
            str, list[tuple[dt.datetime, dt.datetime | None, str]]
        ] = {subject: [] for subject in subjects}
        verified_runner_active_by_subject = {
            subject: 0 for subject in subjects
        }
        runner_evidenced_task_count_by_subject = {
            subject: 0 for subject in subjects
        }
        runner_interval_missing_count_by_subject = {
            subject: 0 for subject in subjects
        }
        for subject in subjects:
            identity = self._read_canary_identity_read_only(
                subject, configured_release_id=release_id
            )
            if identity is None:
                continue
            if identity["foreign_release"] is True:
                foreign_active_count = identity.get("active_task_count")
                foreign_active_selections = identity.get(
                    "active_selections"
                )
                if (
                    isinstance(foreign_active_count, bool)
                    or not isinstance(foreign_active_count, int)
                    or not isinstance(foreign_active_selections, Mapping)
                ):
                    raise DispatchError("production_canary_state_invalid")
                foreign_inactive = (
                    identity.get("state") == "inactive_rolled_back"
                    and identity.get("status")
                    == "production_canary_inactive"
                    and identity.get("luna_consumer_enabled") is False
                    and foreign_active_count == 0
                    and identity.get("selected") is None
                    and not foreign_active_selections
                )
                if foreign_inactive:
                    continue
                raise DispatchError(
                    "production_canary_concurrency_release_conflict"
                )
            state = self._read_canary_state_locked(subject, required=False)
            if state is None:
                raise DispatchError("production_canary_state_invalid")
            active_count = int(state.get("active_task_count") or 0)
            activation_ids[subject] = str(state["activation_id"])
            active_by_subject[subject] = active_count
            terminal_index = self._read_canary_terminal_index_locked(state)
            terminal_index_sha256_by_subject[subject] = str(
                state["terminal_index_sha256"]
            )
            terminal_task_count_by_subject[subject] = int(
                terminal_index["terminal_task_count"]
            )
            terminal_by_outcome_by_subject[subject] = copy.deepcopy(
                dict(terminal_index["terminal_by_outcome"])
            )
            terminal_units = terminal_index["units"]
            for unit_sha256, entry in terminal_units.items():
                scheduler_intervals[subject].append(
                    (
                        _parse_utc(entry["admitted_at"]),
                        _parse_utc(entry["finished_at"]),
                        str(unit_sha256),
                    )
                )
                runner_interval = entry.get("runner_interval")
                if isinstance(runner_interval, Mapping):
                    runner_intervals[subject].append(
                        (
                            _parse_utc(runner_interval["started_at"]),
                            _parse_utc(runner_interval["finished_at"]),
                            str(unit_sha256),
                        )
                    )
                    runner_evidenced_task_count_by_subject[subject] += 1
                else:
                    runner_interval_missing_count_by_subject[subject] += 1
                if entry.get("outcome") != "succeeded":
                    terminal_failure_bindings_by_subject[subject].append(
                        {
                            "unit_sha256": str(unit_sha256),
                            "outcome": str(entry["outcome"]),
                            "error_code": entry.get("error_code"),
                            "terminal_kind": str(entry["terminal_kind"]),
                            "terminal_receipt_sha256": str(
                                entry["terminal_receipt_sha256"]
                            ),
                        }
                    )
            for unit_sha256, selected in dict(
                state.get("active_selections") or {}
            ).items():
                if unit_sha256 in terminal_units:
                    raise DispatchError(
                        "production_canary_lifecycle_interval_conflict"
                    )
                queue_entry = self._read_object(
                    self._production_canary_queue_path(
                        subject,
                        str(selected["producer_input_contract_sha256"]),
                    )
                )
                if (
                    not isinstance(queue_entry, Mapping)
                    or queue_entry.get("queue_status") != "claimed"
                ):
                    raise DispatchError(
                        "production_canary_lifecycle_interval_invalid"
                    )
                scheduler_intervals[subject].append(
                    (
                        _parse_utc(queue_entry.get("claimed_at")),
                        None,
                        str(unit_sha256),
                    )
                )
                process_identity_sha256 = selected.get(
                    "process_identity_sha256"
                )
                if process_identity_sha256 is None:
                    runner_interval_missing_count_by_subject[subject] += 1
                    continue
                lease = Lease(
                    str(unit_sha256),
                    str(selected["lease_owner_id"]),
                    int(selected["lease_fence"]),
                )
                active_task = self._task_from_canary_queue_entry_locked(
                    queue_entry
                )
                identity, identity_sha256, identity_path = (
                    self._task_process_identity_locked(
                        active_task, lease
                    )
                )
                if (
                    identity_sha256 != process_identity_sha256
                    or identity_path != selected.get("process_identity_path")
                ):
                    raise DispatchError(
                        "production_canary_runner_identity_binding_invalid"
                    )
                try:
                    closure = self._task_process_closure_locked(
                        active_task, lease
                    )
                except DispatchError as exc:
                    if exc.code != "task_process_exit_index_missing":
                        raise
                    runner_intervals[subject].append(
                        (
                            _parse_utc(identity["launched_at"]),
                            None,
                            str(unit_sha256),
                        )
                    )
                    verified_runner_active_by_subject[subject] += 1
                else:
                    runner_intervals[subject].append(
                        (
                            _parse_utc(closure["launched_at"]),
                            _parse_utc(closure["finished_at"]),
                            str(unit_sha256),
                        )
                    )
                runner_evidenced_task_count_by_subject[subject] += 1

        subject_peaks: dict[str, int] = {}
        scheduler_subject_peaks: dict[str, int] = {}
        zero_duration_runner_interval_count_by_subject: dict[str, int] = {}
        for subject in subjects:
            subject_peak, zero_count = self._half_open_interval_peak(
                runner_intervals[subject]
            )
            scheduler_peak, _scheduler_zero = self._half_open_interval_peak(
                scheduler_intervals[subject]
            )
            subject_peaks[subject] = subject_peak
            scheduler_subject_peaks[subject] = scheduler_peak
            zero_duration_runner_interval_count_by_subject[subject] = (
                zero_count
            )
        global_active = sum(active_by_subject.values())
        verified_runner_global_active = sum(
            verified_runner_active_by_subject.values()
        )
        global_runner_intervals = [
            interval
            for subject in subjects
            for interval in runner_intervals[subject]
        ]
        global_scheduler_intervals = [
            interval
            for subject in subjects
            for interval in scheduler_intervals[subject]
        ]
        global_runner_peak, global_zero_count = (
            self._half_open_interval_peak(global_runner_intervals)
        )
        global_scheduler_peak, _ = self._half_open_interval_peak(
            global_scheduler_intervals
        )
        terminal_by_outcome_global = self._empty_canary_terminal_counts()
        for subject in subjects:
            for outcome, count in terminal_by_outcome_by_subject[subject].items():
                terminal_by_outcome_global[outcome] += int(count)
        telemetry_core = {
                "schema_version": (
                    PRODUCTION_CANARY_CONCURRENCY_TELEMETRY_SCHEMA
                ),
                "release_id": release_id,
                "activation_ids": activation_ids,
                "active_by_subject": active_by_subject,
                "subject_peak_active": subject_peaks,
                "global_active_task_count": global_active,
                "global_peak_active": global_runner_peak,
                "verified_runner_active_by_subject": (
                    verified_runner_active_by_subject
                ),
                "verified_runner_global_active_task_count": (
                    verified_runner_global_active
                ),
                "scheduler_claim_subject_peak_active": (
                    scheduler_subject_peaks
                ),
                "scheduler_claim_global_peak_active": global_scheduler_peak,
                "runner_evidenced_task_count_by_subject": (
                    runner_evidenced_task_count_by_subject
                ),
                "runner_evidenced_task_count_global": sum(
                    runner_evidenced_task_count_by_subject.values()
                ),
                "runner_interval_missing_count_by_subject": (
                    runner_interval_missing_count_by_subject
                ),
                "runner_interval_missing_count_global": sum(
                    runner_interval_missing_count_by_subject.values()
                ),
                "zero_duration_runner_interval_count_by_subject": (
                    zero_duration_runner_interval_count_by_subject
                ),
                "zero_duration_runner_interval_count_global": (
                    global_zero_count
                ),
                "counter_scope": (
                    "same_release_current_activation_task_process_lifecycle"
                ),
                "peak_source": (
                    "hmac_task_process_identity_and_exit_half_open_intervals"
                ),
                "runner_interval_policy": (
                    "half_open_end_before_start_zero_duration_nonoverlap"
                ),
                "terminal_index_sha256_by_subject": (
                    terminal_index_sha256_by_subject
                ),
                "terminal_task_count_by_subject": (
                    terminal_task_count_by_subject
                ),
                "terminal_task_count_global": sum(
                    terminal_task_count_by_subject.values()
                ),
                "terminal_by_outcome_by_subject": (
                    terminal_by_outcome_by_subject
                ),
                "terminal_by_outcome_global": terminal_by_outcome_global,
                "terminal_failure_bindings_by_subject": (
                    terminal_failure_bindings_by_subject
                ),
                "requested_service_tier": None,
                "fast_mode_requested": False,
                "fast_mode_effective": "not_requested",
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
        }
        if existing is not None and existing.get("release_id") == release_id:
            existing_core = dict(existing)
            existing_core.pop("authority", None)
            existing_core.pop("updated_at", None)
            if existing_core == telemetry_core:
                return dict(existing)
        telemetry = self._seal(
            {
                **telemetry_core,
                "updated_at": updated_at or _utc_now(),
            },
            purpose="dispatch-production-canary-concurrency-telemetry",
        )
        _atomic_replace_json(
            self.production_canary_concurrency_telemetry_path, telemetry
        )
        return telemetry

    def production_canary_concurrency_telemetry(
        self, *, release_id: str
    ) -> dict[str, Any]:
        with _ExclusiveFileLock(self.lock_path):
            return self._recompute_canary_concurrency_telemetry_locked(
                release_id
            )

    def _read_canary_concurrency_telemetry_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        telemetry = self._read_object(
            self.production_canary_concurrency_telemetry_path
        )
        if telemetry is None:
            raise DispatchError(
                "production_canary_concurrency_telemetry_missing"
            )
        self._verify_seal(
            telemetry,
            purpose="dispatch-production-canary-concurrency-telemetry",
        )
        subject = str(state["subject"])
        activation_ids = telemetry.get("activation_ids")
        active_by_subject = telemetry.get("active_by_subject")
        subject_peak_active = telemetry.get("subject_peak_active")
        terminal_indexes = telemetry.get(
            "terminal_index_sha256_by_subject"
        )
        terminal_counts = telemetry.get("terminal_task_count_by_subject")
        terminal_by_outcome = telemetry.get(
            "terminal_by_outcome_by_subject"
        )
        terminal_by_outcome_global = telemetry.get(
            "terminal_by_outcome_global"
        )
        failure_bindings = telemetry.get(
            "terminal_failure_bindings_by_subject"
        )
        verified_runner_active = telemetry.get(
            "verified_runner_active_by_subject"
        )
        runner_evidenced = telemetry.get(
            "runner_evidenced_task_count_by_subject"
        )
        runner_missing = telemetry.get(
            "runner_interval_missing_count_by_subject"
        )
        zero_duration = telemetry.get(
            "zero_duration_runner_interval_count_by_subject"
        )
        scheduler_peaks = telemetry.get(
            "scheduler_claim_subject_peak_active"
        )
        expected_subjects = {"math", "cs408", "english"}
        if (
            telemetry.get("schema_version")
            != PRODUCTION_CANARY_CONCURRENCY_TELEMETRY_SCHEMA
            or telemetry.get("release_id") != state.get("release_id")
            or not isinstance(activation_ids, Mapping)
            or activation_ids.get(subject) != state.get("activation_id")
            or not isinstance(active_by_subject, Mapping)
            or active_by_subject.get(subject)
            != state.get("active_task_count")
            or not isinstance(subject_peak_active, Mapping)
            or set(subject_peak_active) != expected_subjects
            or any(
                isinstance(subject_peak_active.get(item), bool)
                or not isinstance(subject_peak_active.get(item), int)
                or int(subject_peak_active[item]) < 0
                for item in expected_subjects
            )
            or telemetry.get("global_active_task_count")
            != sum(int(active_by_subject.get(item) or 0) for item in (
                "math", "cs408", "english"
            ))
            or isinstance(telemetry.get("global_peak_active"), bool)
            or not isinstance(telemetry.get("global_peak_active"), int)
            or int(telemetry["global_peak_active"]) < 0
            or telemetry.get("counter_scope")
            != "same_release_current_activation_task_process_lifecycle"
            or telemetry.get("peak_source")
            != "hmac_task_process_identity_and_exit_half_open_intervals"
            or telemetry.get("runner_interval_policy")
            != "half_open_end_before_start_zero_duration_nonoverlap"
            or not isinstance(verified_runner_active, Mapping)
            or set(verified_runner_active) != expected_subjects
            or telemetry.get("verified_runner_global_active_task_count")
            != sum(
                int(verified_runner_active.get(item) or 0)
                for item in expected_subjects
            )
            or not isinstance(scheduler_peaks, Mapping)
            or set(scheduler_peaks) != expected_subjects
            or not isinstance(runner_evidenced, Mapping)
            or set(runner_evidenced) != expected_subjects
            or telemetry.get("runner_evidenced_task_count_global")
            != sum(
                int(runner_evidenced.get(item) or 0)
                for item in expected_subjects
            )
            or not isinstance(runner_missing, Mapping)
            or set(runner_missing) != expected_subjects
            or telemetry.get("runner_interval_missing_count_global")
            != sum(
                int(runner_missing.get(item) or 0)
                for item in expected_subjects
            )
            or not isinstance(zero_duration, Mapping)
            or set(zero_duration) != expected_subjects
            or telemetry.get("zero_duration_runner_interval_count_global")
            != sum(
                int(zero_duration.get(item) or 0)
                for item in expected_subjects
            )
            or not isinstance(terminal_indexes, Mapping)
            or terminal_indexes.get(subject)
            != state.get("terminal_index_sha256")
            or not isinstance(terminal_counts, Mapping)
            or terminal_counts.get(subject)
            != state.get("terminal_task_count")
            or not isinstance(terminal_by_outcome, Mapping)
            or terminal_by_outcome.get(subject)
            != state.get("terminal_by_outcome")
            or any(
                not isinstance(terminal_by_outcome.get(item), Mapping)
                for item in ("math", "cs408", "english")
            )
            or telemetry.get("terminal_task_count_global")
            != sum(int(terminal_counts.get(item) or 0) for item in (
                "math", "cs408", "english"
            ))
            or not isinstance(terminal_by_outcome_global, Mapping)
            or terminal_by_outcome_global
            != {
                outcome: sum(
                    int(
                        (
                            terminal_by_outcome.get(item) or {}
                        ).get(outcome)
                        or 0
                    )
                    for item in ("math", "cs408", "english")
                )
                for outcome in self._empty_canary_terminal_counts()
            }
            or not isinstance(failure_bindings, Mapping)
            or set(failure_bindings) != expected_subjects
            or any(
                not isinstance(failure_bindings.get(item), list)
                for item in expected_subjects
            )
            or any(
                not isinstance(row, Mapping)
                or set(row)
                != {
                    "unit_sha256",
                    "outcome",
                    "error_code",
                    "terminal_kind",
                    "terminal_receipt_sha256",
                }
                or row.get("outcome") == "succeeded"
                for item in expected_subjects
                for row in failure_bindings[item]
            )
            or "requested_service_tier" not in telemetry
            or telemetry.get("requested_service_tier") is not None
            or telemetry.get("fast_mode_requested") is not False
            or telemetry.get("fast_mode_effective") != "not_requested"
            or telemetry.get("model_call_count") != 0
            or telemetry.get("provider_request_count") != 0
            or telemetry.get("formal_write_count") != 0
            or telemetry.get("sol_enabled") is not False
        ):
            raise DispatchError(
                "production_canary_concurrency_telemetry_binding_invalid"
            )
        return telemetry

    def _publish_canary_receipt_locked(
        self,
        subject: str,
        activation_id: str,
        value: Mapping[str, Any],
        *,
        purpose: str,
    ) -> tuple[str, Path, dict[str, Any]]:
        sealed = self._seal(value, purpose=purpose)
        receipt_sha256, receipt_path = _publish_content_addressed(
            self.production_canary_receipt_root
            / _safe_component(subject)
            / _safe_component(activation_id),
            sealed,
        )
        return receipt_sha256, receipt_path, sealed

    @staticmethod
    def _empty_canary_terminal_counts() -> dict[str, int]:
        return {
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
            "timed_out": 0,
            "stalled": 0,
            "needs_rework": 0,
        }

    def _publish_canary_terminal_index_locked(
        self,
        *,
        subject: str,
        release_id: str,
        activation_id: str,
        units: Mapping[str, Any],
        updated_at: str,
    ) -> tuple[dict[str, Any], str, Path]:
        _parse_utc(updated_at)
        _validate_unit_sha256(release_id)
        _validate_unit_sha256(activation_id)
        normalized_units = copy.deepcopy(dict(units))
        counts = self._empty_canary_terminal_counts()
        for unit_sha256, value in normalized_units.items():
            _validate_unit_sha256(str(unit_sha256))
            if not isinstance(value, Mapping):
                raise DispatchError("production_canary_terminal_index_invalid")
            outcome = value.get("outcome")
            if outcome not in counts:
                raise DispatchError("production_canary_terminal_index_invalid")
            counts[str(outcome)] += 1
        index = self._seal(
            {
                "schema_version": PRODUCTION_CANARY_TERMINAL_INDEX_SCHEMA,
                "subject": subject,
                "release_id": release_id,
                "activation_id": activation_id,
                "terminal_task_count": len(normalized_units),
                "terminal_by_outcome": counts,
                "units": normalized_units,
                "counter_scope": "current_activation_latest_terminal_per_unit",
                "updated_at": updated_at,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            },
            purpose="dispatch-production-canary-terminal-index",
        )
        index_sha256, index_path = _publish_content_addressed(
            self.production_canary_terminal_index_root
            / _safe_component(subject)
            / activation_id,
            index,
        )
        return index, index_sha256, index_path

    def _read_canary_terminal_index_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        value = self._read_verified_content_addressed_locked(
            state.get("terminal_index_path"),
            state.get("terminal_index_sha256"),
            root=self.production_canary_terminal_index_root,
            purpose="dispatch-production-canary-terminal-index",
            error_code="production_canary_terminal_index_invalid",
        )
        counts = value.get("terminal_by_outcome")
        units = value.get("units")
        if (
            value.get("schema_version")
            != PRODUCTION_CANARY_TERMINAL_INDEX_SCHEMA
            or value.get("subject") != state.get("subject")
            or value.get("release_id") != state.get("release_id")
            or value.get("activation_id") != state.get("activation_id")
            or value.get("counter_scope")
            != "current_activation_latest_terminal_per_unit"
            or value.get("model_call_count") != 0
            or value.get("provider_request_count") != 0
            or value.get("formal_write_count") != 0
            or value.get("sol_enabled") is not False
            or not isinstance(units, Mapping)
            or value.get("terminal_task_count") != len(units)
            or not isinstance(counts, Mapping)
            or dict(counts) != state.get("terminal_by_outcome")
            or value.get("terminal_task_count")
            != state.get("terminal_task_count")
            or set(counts) != set(self._empty_canary_terminal_counts())
            or sum(int(counts[key]) for key in counts) != len(units)
        ):
            raise DispatchError("production_canary_terminal_index_invalid")
        for unit_sha256, raw in units.items():
            if not isinstance(raw, Mapping):
                raise DispatchError("production_canary_terminal_index_invalid")
            history = raw.get("history")
            runner_interval = raw.get("runner_interval")
            try:
                _validate_unit_sha256(str(unit_sha256))
                _validate_unit_sha256(str(raw.get("frozen_payload_sha256") or ""))
                _validate_unit_sha256(str(raw.get("terminal_receipt_sha256") or ""))
                _parse_utc(raw.get("admitted_at"))
                _parse_utc(raw.get("finished_at"))
                if runner_interval is not None:
                    if not isinstance(runner_interval, Mapping):
                        raise DispatchError(
                            "production_canary_terminal_index_invalid"
                        )
                    _validate_unit_sha256(
                        str(
                            runner_interval.get(
                                "task_process_identity_sha256"
                            )
                            or ""
                        )
                    )
                    _validate_unit_sha256(
                        str(
                            runner_interval.get("task_process_exit_sha256")
                            or ""
                        )
                    )
                    started = _parse_utc(runner_interval.get("started_at"))
                    ended = _parse_utc(runner_interval.get("finished_at"))
                    if ended < started:
                        raise DispatchError(
                            "production_canary_terminal_index_invalid"
                        )
                report_values = {
                    "package_ref": raw.get("package_ref"),
                    "package_sha256": raw.get("package_sha256"),
                    "report_json_ref": raw.get("report_json_ref"),
                    "report_json_sha256": raw.get("report_json_sha256"),
                    "report_markdown_ref": raw.get("report_markdown_ref"),
                    "report_markdown_sha256": raw.get(
                        "report_markdown_sha256"
                    ),
                }
                if raw.get("report_reopen_status") == "not_available":
                    if any(value is not None for value in report_values.values()):
                        raise DispatchError(
                            "production_canary_terminal_index_invalid"
                        )
                elif (
                    raw.get("report_reopen_status")
                    == "json_markdown_package_verified"
                ):
                    for key in (
                        "package_sha256",
                        "report_json_sha256",
                        "report_markdown_sha256",
                    ):
                        _validate_unit_sha256(str(report_values[key] or ""))
                    if (
                        report_values["package_ref"]
                        != "study-intake-dispatch-package://sha256/"
                        + str(report_values["package_sha256"])
                        or report_values["report_json_ref"]
                        != "study-intake-report://sha256/"
                        + str(report_values["report_json_sha256"])
                        or report_values["report_markdown_ref"]
                        != "study-intake-report-markdown://sha256/"
                        + str(report_values["report_markdown_sha256"])
                    ):
                        raise DispatchError(
                            "production_canary_terminal_index_invalid"
                        )
                else:
                    raise DispatchError(
                        "production_canary_terminal_index_invalid"
                    )
            except DispatchError as exc:
                raise DispatchError(
                    "production_canary_terminal_index_invalid"
                ) from exc
            if (
                raw.get("unit_sha256") != unit_sha256
                or raw.get("outcome") not in counts
                or not isinstance(history, list)
                or not history
                or history[-1].get("terminal_receipt_sha256")
                != raw.get("terminal_receipt_sha256")
                or raw.get("runner_evidenced")
                is not (runner_interval is not None)
                or (
                    raw.get("outcome") == "succeeded"
                    and (
                        raw.get("report_reopen_status")
                        != "json_markdown_package_verified"
                        or raw.get("runner_evidenced") is not True
                        or runner_interval is None
                    )
                )
            ):
                raise DispatchError("production_canary_terminal_index_invalid")
        return value

    def _append_canary_terminal_index_locked(
        self,
        state: Mapping[str, Any],
        task: FrozenTask,
        *,
        queue_entry: Mapping[str, Any],
        terminal_receipt_sha256: str,
        terminal_receipt_path: str,
        outcome: str,
        error_code: str | None,
        finished_at: str,
        terminal_kind: str,
        prior_terminal_receipt_sha256: str | None = None,
    ) -> dict[str, Any]:
        allowed = self._empty_canary_terminal_counts()
        if outcome not in allowed:
            raise DispatchError("production_canary_terminal_outcome_invalid")
        _validate_unit_sha256(terminal_receipt_sha256)
        terminal_path = Path(terminal_receipt_path)
        try:
            terminal_bytes = terminal_path.read_bytes()
            terminal_path.resolve(strict=True).relative_to(
                self.production_canary_receipt_root.resolve()
            )
        except (OSError, ValueError) as exc:
            raise DispatchError("production_canary_terminal_index_invalid") from exc
        if _sha256_bytes(terminal_bytes) != terminal_receipt_sha256:
            raise DispatchError("production_canary_terminal_index_invalid")
        terminal_receipt = self._read_object(terminal_path)
        if not isinstance(terminal_receipt, Mapping):
            raise DispatchError("production_canary_terminal_index_invalid")
        self._verify_seal(
            terminal_receipt,
            purpose="dispatch-production-canary-terminal",
        )
        report_disposition = terminal_receipt.get("report_disposition")
        expected_terminal_schema = (
            PRODUCTION_CANARY_REVIEW_TERMINAL_SCHEMA
            if report_disposition in {"needs_sol_review", "quarantined"}
            or outcome == "needs_rework"
            else PRODUCTION_CANARY_TERMINAL_SCHEMA
        )
        if (
            terminal_receipt.get("schema_version")
            != expected_terminal_schema
            or terminal_receipt.get("activation_id")
            != state.get("activation_id")
            or terminal_receipt.get("release_id") != state.get("release_id")
            or terminal_receipt.get("subject") != state.get("subject")
            or terminal_receipt.get("outcome") != outcome
            or terminal_receipt.get("finished_at") != finished_at
            or report_disposition == "needs_sol_review"
            and (
                outcome != "succeeded"
                or terminal_receipt.get("execution_status") != "succeeded"
                or terminal_receipt.get("quality_status") != "issues_found"
                or terminal_receipt.get("sol_review_status") != "pending"
                or terminal_receipt.get("production_accepted") is not False
            )
            or report_disposition == "quarantined"
            and (
                outcome != "failed"
                or terminal_receipt.get("execution_status") != "failed"
                or terminal_receipt.get("quality_status") != "unchecked"
                or terminal_receipt.get("sol_review_status") != "not_eligible"
                or terminal_receipt.get("production_accepted") is not False
            )
        ):
            raise DispatchError("production_canary_terminal_index_invalid")
        report_fields = {
            key: terminal_receipt.get(key)
            for key in (
                "package_ref",
                "package_sha256",
                "report_json_ref",
                "report_json_sha256",
                "report_markdown_ref",
                "report_markdown_sha256",
                "report_reopen_status",
            )
        }
        index = self._read_canary_terminal_index_locked(state)
        units = copy.deepcopy(dict(index["units"]))
        prior = units.get(task.unit_sha256)
        runner_interval = (
            copy.deepcopy(prior.get("runner_interval"))
            if isinstance(prior, Mapping)
            and isinstance(prior.get("runner_interval"), Mapping)
            else None
        )
        if runner_interval is None:
            lease_owner_id = queue_entry.get("lease_owner_id")
            lease_fence = queue_entry.get("lease_fence")
            if (
                isinstance(lease_owner_id, str)
                and isinstance(lease_fence, int)
            ):
                try:
                    closure = self._task_process_closure_locked(
                        task,
                        Lease(task.unit_sha256, lease_owner_id, lease_fence),
                    )
                except DispatchError as exc:
                    if exc.code not in {
                        "task_process_identity_index_missing",
                        "task_process_exit_index_missing",
                    }:
                        raise
                else:
                    runner_interval = {
                        "task_process_identity_sha256": closure[
                            "supervisor_process_identity_sha256"
                        ],
                        "task_process_exit_sha256": closure[
                            "supervisor_process_exit_sha256"
                        ],
                        "pid": closure["supervisor_pid"],
                        "pgid": closure["supervisor_pgid"],
                        "process_start_token": closure[
                            "process_start_token"
                        ],
                        "started_at": closure["launched_at"],
                        "finished_at": closure["finished_at"],
                        "reaped": True,
                    }
        history = (
            copy.deepcopy(list(prior.get("history") or []))
            if isinstance(prior, Mapping)
            else []
        )
        history.append(
            {
                "terminal_receipt_sha256": terminal_receipt_sha256,
                "terminal_receipt_path": str(terminal_path),
                "outcome": outcome,
                "error_code": error_code,
                "terminal_kind": terminal_kind,
                "finished_at": finished_at,
                "prior_terminal_receipt_sha256": (
                    prior_terminal_receipt_sha256
                ),
            }
        )
        units[task.unit_sha256] = {
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "producer_unit_id": queue_entry.get("producer_unit_id"),
            "admitted_at": queue_entry.get("claimed_at"),
            "finished_at": finished_at,
            "outcome": outcome,
            "error_code": error_code,
            "terminal_kind": terminal_kind,
            "terminal_receipt_sha256": terminal_receipt_sha256,
            "terminal_receipt_path": str(terminal_path),
            **report_fields,
            "runner_evidenced": runner_interval is not None,
            "runner_interval": runner_interval,
            "history": history,
        }
        published, index_sha256, index_path = (
            self._publish_canary_terminal_index_locked(
                subject=str(state["subject"]),
                release_id=str(state["release_id"]),
                activation_id=str(state["activation_id"]),
                units=units,
                updated_at=finished_at,
            )
        )
        return {
            "terminal_index_sha256": index_sha256,
            "terminal_index_path": str(index_path),
            "terminal_task_count": published["terminal_task_count"],
            "terminal_by_outcome": copy.deepcopy(
                published["terminal_by_outcome"]
            ),
        }

    @staticmethod
    def _validate_producer_authority(
        producer_authority: Mapping[str, Any], subject: str, release_id: str
    ) -> dict[str, Any]:
        value = copy.deepcopy(dict(producer_authority))
        fingerprint = value.get("authority_fingerprint")
        core = dict(value)
        core.pop("authority_fingerprint", None)
        if (
            value.get("schema_version") != "study-intake-producer-authority-v1"
            or value.get("subject") != subject
            or value.get("release_id") != release_id
            or value.get("formal_write_count") != 0
            or "service_tier" in value
            or not isinstance(fingerprint, str)
            or fingerprint != _sha256_bytes(_canonical_bytes(core))
        ):
            raise DispatchError("producer_authority_invalid")
        _validate_unit_sha256(fingerprint)
        _validate_unit_sha256(release_id)
        return value

    def activate_production_canary(
        self,
        subject: str,
        *,
        release_id: str,
        producer_authority: Mapping[str, Any],
        activated_at: str | None = None,
        continuous_concurrency_limit: int = (
            DEFAULT_CONTINUOUS_CONCURRENCY_LIMIT
        ),
        staged_recovery_receipt_sha256: str | None = None,
        staged_recovery_receipt_path: str | None = None,
    ) -> dict[str, Any]:
        if subject not in {"math", "cs408", "english"}:
            raise DispatchError("production_canary_subject_invalid")
        timestamp = activated_at or _utc_now()
        _parse_utc(timestamp)
        authority = self._validate_producer_authority(
            producer_authority, subject, release_id
        )
        control_fields = self._canary_control_fields(
            continuous_concurrency_limit
        )
        staged_recovery = staged_recovery_receipt_sha256 is not None
        if staged_recovery:
            if subject != "english" or staged_recovery_receipt_path is None:
                raise DispatchError(
                    "subject_batch_recovery_staged_activation_invalid"
                )
            staged_recovery_receipt_sha256 = _validate_unit_sha256(
                staged_recovery_receipt_sha256
            )
            staged_recovery_receipt_path = str(
                Path(staged_recovery_receipt_path).resolve()
            )
        elif staged_recovery_receipt_path is not None:
            raise DispatchError(
                "subject_batch_recovery_staged_activation_invalid"
            )
        with _ExclusiveFileLock(self.lock_path):
            if not self._drain_path(subject).exists():
                raise DispatchError("production_canary_subject_not_drained")
            for path in sorted(self.lease_root.glob("*.json")):
                lease = self._read_object(path)
                if (
                    lease is not None
                    and lease.get("subject") == subject
                    and lease.get("status") == "claimed"
                ):
                    raise DispatchError("production_canary_claims_not_zero")
            existing_identity = self._read_canary_identity_read_only(
                subject, configured_release_id=release_id
            )
            existing: dict[str, Any] | None = None
            if existing_identity is not None:
                if existing_identity["foreign_release"] is True:
                    active_selections = existing_identity.get(
                        "active_selections"
                    )
                    foreign_inactive = (
                        existing_identity.get("schema_version")
                        in {
                            LEGACY_PRODUCTION_CANARY_STATE_SCHEMA,
                            PRODUCTION_CANARY_STATE_SCHEMA,
                        }
                        and existing_identity.get("state")
                        == "inactive_rolled_back"
                        and existing_identity.get("status")
                        == "production_canary_inactive"
                        and existing_identity.get("luna_consumer_enabled") is False
                        and not isinstance(
                            existing_identity.get("active_task_count"), bool
                        )
                        and existing_identity.get("active_task_count") == 0
                        and existing_identity.get("selected") is None
                        and isinstance(active_selections, Mapping)
                        and not active_selections
                    )
                    if not foreign_inactive:
                        raise DispatchError(
                            "production_canary_activation_conflict"
                        )
                else:
                    existing = self._read_canary_state_locked(
                        subject, required=False
                    )
            if existing is not None and existing.get("state") != "inactive_rolled_back":
                if (
                    existing.get("release_id") == release_id
                    and existing.get("producer_authority_fingerprint")
                    == authority["authority_fingerprint"]
                    and existing.get("continuous_concurrency_limit")
                    == continuous_concurrency_limit
                ):
                    if staged_recovery:
                        staged_pointer_path = (
                            self.production_canary_recovery_staged_activation_root
                            / f"{staged_recovery_receipt_sha256}.json"
                        )
                        staged_pointer = self._read_object(
                            staged_pointer_path
                        )
                        if staged_pointer is None:
                            raise DispatchError(
                                "subject_batch_recovery_staged_activation_missing"
                            )
                        self._verify_seal(
                            staged_pointer,
                            purpose=(
                                "dispatch-subject-batch-recovery-staged-activation"
                            ),
                        )
                        if (
                            existing.get("state") != "paused_drained"
                            or existing.get("luna_consumer_enabled") is not False
                            or staged_pointer.get("status") != "staged"
                            or staged_pointer.get("target_activation_id")
                            != existing.get("activation_id")
                            or staged_pointer.get("target_release_id")
                            != release_id
                            or staged_pointer.get("recovery_receipt_path")
                            != staged_recovery_receipt_path
                        ):
                            raise DispatchError(
                                "subject_batch_recovery_staged_activation_conflict"
                            )
                    return existing
                raise DispatchError("production_canary_activation_conflict")
            high_watermark = {
                "schema_version": "study-intake-producer-high-watermark-v1",
                "subject": subject,
                "release_id": release_id,
                "recorded_at": timestamp,
                "producer_authority_fingerprint": authority[
                    "authority_fingerprint"
                ],
                "source_event_ids": [],
                "source_event_set_sha256": _sha256_bytes(_canonical_bytes([])),
                "formal_write_count": 0,
            }
            high_watermark_sha256 = _sha256_bytes(
                _canonical_bytes(high_watermark)
            )
            activation_identity = {
                "schema_version": PRODUCTION_CANARY_ACTIVATION_SCHEMA,
                "subject": subject,
                "release_id": release_id,
                "activated_at": timestamp,
                "producer_authority_fingerprint": authority[
                    "authority_fingerprint"
                ],
                "producer_high_watermark_sha256": high_watermark_sha256,
                "initial_canary_inflight_limit": (
                    INITIAL_CANARY_INFLIGHT_LIMIT
                ),
                "continuous_concurrency_limit": continuous_concurrency_limit,
            }
            activation_id = _sha256_bytes(_canonical_bytes(activation_identity))
            activation_core = {
                **activation_identity,
                "activation_id": activation_id,
                "producer_authority": authority,
                "producer_high_watermark": high_watermark,
                **control_fields,
            }
            activation_receipt_sha256, activation_receipt_path, receipt = (
                self._publish_canary_receipt_locked(
                    subject,
                    activation_id,
                    activation_core,
                    purpose="dispatch-production-canary-activation",
                )
            )
            activation_gate_authority_sha256 = _sha256_bytes(
                _canonical_bytes(receipt["authority"])
            )
            terminal_index, terminal_index_sha256, terminal_index_path = (
                self._publish_canary_terminal_index_locked(
                    subject=subject,
                    release_id=release_id,
                    activation_id=activation_id,
                    units={},
                    updated_at=timestamp,
                )
            )
            state = {
                "schema_version": PRODUCTION_CANARY_STATE_SCHEMA,
                "status": "production_canary_active",
                "state": "paused_drained" if staged_recovery else "armed",
                "subject": subject,
                "release_id": release_id,
                "activation_id": activation_id,
                "activated_at": timestamp,
                "producer_authority_fingerprint": authority[
                    "authority_fingerprint"
                ],
                "producer_high_watermark": high_watermark,
                "producer_high_watermark_sha256": high_watermark_sha256,
                "activation_receipt_sha256": activation_receipt_sha256,
                "activation_receipt_path": str(activation_receipt_path),
                "activation_gate_authority_sha256": (
                    activation_gate_authority_sha256
                ),
                "luna_consumer_enabled": not staged_recovery,
                "queue_depth": 0,
                "historical_eligible_count": 0,
                "excluded_by_high_watermark_count": 0,
                "canary_queue_count": 0,
                "queue_classification": {
                    "pre_activation_frozen": 0,
                    "pending": 0,
                    "claimed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "oldest_pending_age_seconds": None,
                "active_task_count": 0,
                "active_selections": {},
                "terminal_index_sha256": terminal_index_sha256,
                "terminal_index_path": str(terminal_index_path),
                "terminal_task_count": 0,
                "terminal_by_outcome": copy.deepcopy(
                    terminal_index["terminal_by_outcome"]
                ),
                "backpressure_reason": None,
                "last_success_at": None,
                "last_failure_at": None,
                "last_preclaim_failure_at": None,
                "last_preclaim_failure_stage": None,
                "last_preclaim_failure_error_code": None,
                "last_preclaim_failure_receipt_sha256": None,
                "last_preclaim_failure_evidence_sha256": None,
                "preclaim_failure_resume_ack_sha256": None,
                "last_emergency_cancel_at": None,
                "last_emergency_cancel_receipt_sha256": None,
                "late_result_fence_status": "not_required",
                "observability_counter_scope": "current_activation_cumulative",
                "observed_model_call_count": 0,
                "observed_provider_request_count": 0,
                "observed_mcp_tool_call_count": 0,
                "last_analysis_status": "not_started",
                "last_critical_review_status": "not_started",
                "last_report_status": "not_started",
                "last_package_sha256": None,
                "last_read_session_id": None,
                "last_evidence_generation": None,
                "last_evidence_authority_fingerprint": None,
                "last_analysis_grounding_manifest_sha256": None,
                "last_critical_review_grounding_manifest_sha256": None,
                "last_analysis_evidence_refs": [],
                "last_critical_review_evidence_refs": [],
                "blocking_reason": (
                    "subject_batch_recovery_finalization_pending"
                    if staged_recovery
                    else None
                ),
                "next_action": (
                    "finalize_subject_batch_recovery"
                    if staged_recovery
                    else "await_first_post_activation_capture"
                ),
                "selected": None,
                "last_selected": None,
                "unlocked_once": False,
                "last_terminal_receipt_sha256": None,
                "last_terminal_receipt_path": None,
                **control_fields,
            }
            if staged_recovery:
                staged_core = {
                    "schema_version": (
                        "study-intake-subject-batch-recovery-staged-activation-v1"
                    ),
                    "subject": subject,
                    "status": "staged",
                    "recovery_receipt_sha256": (
                        staged_recovery_receipt_sha256
                    ),
                    "recovery_receipt_path": staged_recovery_receipt_path,
                    "target_activation_id": activation_id,
                    "target_release_id": release_id,
                    "producer_authority_fingerprint": authority[
                        "authority_fingerprint"
                    ],
                    "created_at": timestamp,
                    "armed_at": None,
                    "formal_write_count": 0,
                }
                _staged_sha256, _staged_path, staged_receipt = (
                    self._publish_canary_receipt_locked(
                        subject,
                        activation_id,
                        staged_core,
                        purpose=(
                            "dispatch-subject-batch-recovery-staged-activation"
                        ),
                    )
                )
                staged_pointer_path = (
                    self.production_canary_recovery_staged_activation_root
                    / f"{staged_recovery_receipt_sha256}.json"
                )
                existing_staged = self._read_object(staged_pointer_path)
                if existing_staged is not None:
                    self._verify_seal(
                        existing_staged,
                        purpose=(
                            "dispatch-subject-batch-recovery-staged-activation"
                        ),
                    )
                    if existing_staged != staged_receipt:
                        raise DispatchError(
                            "subject_batch_recovery_staged_activation_conflict"
                        )
                else:
                    _atomic_replace_json(
                        staged_pointer_path, staged_receipt
                    )
            written = self._write_canary_state_locked(
                state, updated_at=timestamp
            )
            self._recompute_canary_concurrency_telemetry_locked(
                release_id, updated_at=timestamp
            )
            return written

    def deactivate_production_canary(
        self, subject: str, *, expected_release_id: str
    ) -> dict[str, Any]:
        """Rollback an activation without deleting its queue or receipts."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if state.get("release_id") != expected_release_id:
                raise DispatchError("production_canary_release_mismatch")
            if int(state.get("active_task_count") or 0) != 0:
                raise DispatchError("production_canary_active_tasks_present")
            state.update(
                {
                    "status": "production_canary_inactive",
                    "state": "inactive_rolled_back",
                    "luna_consumer_enabled": False,
                    "blocking_reason": "activation_transaction_rolled_back",
                    "next_action": "activate_successor_canary",
                    "selected": None,
                    "active_selections": {},
                    "backpressure_reason": None,
                }
            )
            written = self._write_canary_state_locked(state)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"])
            )
            return written

    def _publish_preclaim_repair_ack_locked(
        self, state: Mapping[str, Any]
    ) -> str | None:
        failure_sha256 = state.get("last_preclaim_failure_receipt_sha256")
        if not isinstance(failure_sha256, str):
            return None
        _validate_unit_sha256(failure_sha256)
        failure_path = (
            self.production_canary_receipt_root
            / _safe_component(str(state["subject"]))
            / _safe_component(str(state["activation_id"]))
            / "sha256"
            / failure_sha256[:2]
            / f"{failure_sha256}.json"
        )
        try:
            failure_bytes = failure_path.read_bytes()
        except OSError as exc:
            raise DispatchError(
                "production_canary_preclaim_failure_receipt_missing"
            ) from exc
        failure = self._read_object(failure_path)
        if (
            _sha256_bytes(failure_bytes) != failure_sha256
            or failure is None
            or failure.get("schema_version")
            != PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA
        ):
            raise DispatchError(
                "production_canary_preclaim_failure_receipt_invalid"
            )
        self._verify_seal(
            failure, purpose="dispatch-production-canary-preclaim-failure"
        )
        evidence = failure.get("failure_evidence")
        unit_sha256 = (
            evidence.get("unit_sha256")
            if isinstance(evidence, Mapping)
            else None
        )
        acknowledged_at = _utc_now()
        ack_identity = {
            "activation_id": state["activation_id"],
            "release_id": state["release_id"],
            "subject": state["subject"],
            "unit_sha256": unit_sha256,
            "preclaim_failure_receipt_sha256": failure_sha256,
            "acknowledged_at": acknowledged_at,
        }
        ack_core = {
            "schema_version": (
                PRODUCTION_CANARY_PRECLAIM_REPAIR_ACK_SCHEMA
            ),
            "repair_ack_id": _sha256_bytes(_canonical_bytes(ack_identity)),
            **ack_identity,
            "preclaim_failure_receipt_path": str(failure_path),
            "failure_stage": failure["failure_stage"],
            "error_code": failure["error_code"],
            "ack_action": "explicit_subject_resume_repair",
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        ack_sha256, _ack_path, _ack = self._publish_canary_receipt_locked(
            str(state["subject"]),
            str(state["activation_id"]),
            ack_core,
            purpose="dispatch-production-canary-preclaim-repair-ack",
        )
        return ack_sha256

    def resume_production_canary(self, subject: str) -> dict[str, Any]:
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if state.get("state") not in {"failed_drained", "paused_drained"}:
                raise DispatchError("production_canary_not_resumable")
            if int(state.get("active_task_count") or 0) != 0:
                raise DispatchError("production_canary_active_tasks_present")
            repair_ack_sha256 = (
                self._publish_preclaim_repair_ack_locked(state)
                if state.get("state") == "failed_drained"
                and isinstance(
                    state.get("last_preclaim_failure_receipt_sha256"), str
                )
                and state.get("last_terminal_receipt_sha256")
                == state.get("last_preclaim_failure_receipt_sha256")
                else None
            )
            state.update(
                {
                    "state": (
                        "continuous_concurrent_unlocked"
                        if state.get("unlocked_once") is True
                        else "armed"
                    ),
                    "luna_consumer_enabled": True,
                    "blocking_reason": None,
                    "next_action": (
                        "consume_pending_post_activation_queue"
                        if int(state.get("queue_depth") or 0) > 0
                        else "await_post_activation_capture"
                    ),
                    "selected": None,
                    "active_selections": {},
                    "backpressure_reason": None,
                    "preclaim_failure_resume_ack_sha256": (
                        repair_ack_sha256
                        if repair_ack_sha256 is not None
                        else state.get("preclaim_failure_resume_ack_sha256")
                    ),
                }
            )
            written = self._write_canary_state_locked(state)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"])
            )
            return written

    def pause_production_canary(self, subject: str) -> dict[str, Any]:
        """Pause only the Luna consumer; the producer queue remains live."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if state.get("state") == "paused_drained":
                return state
            if state.get("state") == "inactive_rolled_back":
                raise DispatchError("production_canary_not_pauseable")
            if int(state.get("active_task_count") or 0) != 0:
                raise DispatchError("production_canary_pause_active_tasks_present")
            for path in sorted(self.lease_root.glob("*.json")):
                lease = self._read_object(path)
                if (
                    lease is not None
                    and lease.get("subject") == subject
                    and lease.get("status") == "claimed"
                ):
                    raise DispatchError(
                        "production_canary_pause_active_tasks_present"
                    )
            state.update(
                {
                    "state": "paused_drained",
                    "luna_consumer_enabled": False,
                    "blocking_reason": "paused_by_user",
                    "next_action": "explicit_subject_resume_required",
                    "selected": None,
                    "active_selections": {},
                    "backpressure_reason": None,
                }
            )
            written = self._write_canary_state_locked(state)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"])
            )
            return written

    @staticmethod
    def _producer_contract_from_task(task: FrozenTask) -> dict[str, Any]:
        payload = task.frozen_payload
        dispatch_contract = payload.get("dispatch_contract")
        contract = (
            dispatch_contract.get("producer_input_contract")
            if isinstance(dispatch_contract, Mapping)
            else None
        )
        if not isinstance(contract, Mapping):
            raise DispatchError("producer_input_contract_missing")
        value = copy.deepcopy(dict(contract))
        contract_sha256 = value.get("producer_input_contract_sha256")
        core = dict(value)
        core.pop("producer_input_contract_sha256", None)
        source_events = value.get("source_events")
        capture_type = value.get("capture_type")
        evidence_contract = value.get("evidence_contract")
        evidence_contract_sha256 = value.get("evidence_contract_sha256")
        if (
            value.get("schema_version") != PRODUCER_DISPATCH_INPUT_SCHEMA
            or value.get("formal_write_count") != 0
            or value.get("subject") != payload.get("subject")
            or value.get("producer_unit_id") != payload.get("capture_id")
            or value.get("producer_recorded_at") != payload.get("recorded_at")
            or value.get("input_fingerprint") != payload.get("input_fingerprint")
            or not isinstance(source_events, list)
            or not source_events
            or not isinstance(contract_sha256, str)
            or contract_sha256 != _sha256_bytes(_canonical_bytes(core))
            or value.get("source_event_set_sha256")
            != _sha256_bytes(_canonical_bytes(source_events))
            or (
                payload.get("subject") == "math"
                and (
                    capture_type
                    not in {
                        "formal_problem",
                        "new_source_problem",
                        "existing_formal_card_observation",
                        "fact_observation",
                    }
                    or not isinstance(evidence_contract, Mapping)
                    or evidence_contract.get("capture_type") != capture_type
                    or evidence_contract.get("evidence_status") != "ready"
                    or evidence_contract.get("missing_roles") != []
                    or evidence_contract.get("formal_write_count") != 0
                    or evidence_contract_sha256
                    != _sha256_bytes(_canonical_bytes(evidence_contract))
                )
            )
            or (
                payload.get("subject") != "math"
                and (
                    capture_type is not None
                    or evidence_contract is not None
                    or evidence_contract_sha256 is not None
                )
            )
        ):
            raise DispatchError("producer_input_contract_invalid")
        _validate_unit_sha256(contract_sha256)
        _validate_unit_sha256(str(value.get("source_event_set_sha256") or ""))
        seen: set[str] = set()
        normalized_events: list[dict[str, str]] = []
        for row in source_events:
            if not isinstance(row, Mapping):
                raise DispatchError("producer_source_event_invalid")
            event_id = row.get("event_id")
            recorded_at = row.get("recorded_at")
            source_sha256 = row.get("source_sha256")
            if (
                not isinstance(event_id, str)
                or not event_id
                or event_id in seen
                or not isinstance(recorded_at, str)
                or not recorded_at
                or not isinstance(source_sha256, str)
            ):
                raise DispatchError("producer_source_event_invalid")
            _parse_utc(recorded_at)
            _validate_unit_sha256(source_sha256)
            seen.add(event_id)
            normalized_events.append(
                {
                    "event_id": event_id,
                    "recorded_at": recorded_at,
                    "source_sha256": source_sha256,
                }
            )
        if normalized_events != sorted(
            normalized_events,
            key=lambda row: (
                row["recorded_at"], row["event_id"], row["source_sha256"]
            ),
        ):
            raise DispatchError("producer_source_event_order_invalid")
        _parse_utc(value.get("producer_recorded_at"))
        return value

    @staticmethod
    def _reject_synthetic_production_canary_evidence(
        task: FrozenTask,
    ) -> None:
        """Keep synthetic English fixtures outside production canary admission.

        Synthetic evidence remains valid for adapter, shadow, and isolated
        tests.  Production canary tasks are different: they are release
        acceptance evidence, so a synthetic event must be rejected before
        materialization and again whenever a persisted task is reopened.
        """

        payload = task.frozen_payload
        if payload.get("subject") != "english":
            return

        def contains_synthetic_evidence(value: Any) -> bool:
            if isinstance(value, Mapping):
                if value.get("evidence_origin") == "synthetic_fixture":
                    return True
                return any(
                    contains_synthetic_evidence(item)
                    for item in value.values()
                )
            if isinstance(value, list):
                return any(contains_synthetic_evidence(item) for item in value)
            return False

        if contains_synthetic_evidence(payload):
            raise DispatchError(
                "production_canary_synthetic_fixture_forbidden"
            )

    def _is_verified_subject_batch_recovery_task_locked(
        self,
        task: FrozenTask,
        state: Mapping[str, Any],
        contract: Mapping[str, Any],
    ) -> bool:
        """Recognize only the exact English queue sealed by v2 recovery."""

        if (
            contract.get("subject") != "english"
            or state.get("subject") != "english"
            or state.get("state")
            not in {
                "armed",
                "canary_in_flight",
                "continuous_concurrent_unlocked",
            }
            or state.get("luna_consumer_enabled") is not True
        ):
            return False
        contract_sha256 = _validate_unit_sha256(
            str(contract.get("producer_input_contract_sha256") or "")
        )
        queue_path = self._production_canary_queue_path(
            "english", contract_sha256
        )
        queue = self._read_object(queue_path)
        if queue is None:
            return False
        self._verify_seal(
            queue, purpose="dispatch-production-canary-queue"
        )
        queue_task = self._task_from_canary_queue_entry_locked(queue)
        source_event_ids = [
            str(row["event_id"]) for row in contract["source_events"]
        ]
        if (
            queue.get("schema_version") != PRODUCTION_CANARY_QUEUE_SCHEMA
            or queue.get("activation_id") != state.get("activation_id")
            or queue.get("subject") != "english"
            or queue.get("release_id") != state.get("release_id")
            or queue.get("producer_high_watermark_sha256")
            != state.get("producer_high_watermark_sha256")
            or queue.get("producer_authority_fingerprint")
            != state.get("producer_authority_fingerprint")
            or queue.get("producer_input_contract_sha256")
            != contract_sha256
            or queue.get("producer_unit_id")
            != contract.get("producer_unit_id")
            or queue.get("producer_recorded_at")
            != contract.get("producer_recorded_at")
            or queue.get("source_event_set_sha256")
            != contract.get("source_event_set_sha256")
            or queue.get("source_event_ids") != source_event_ids
            or queue.get("unit_sha256") != task.unit_sha256
            or queue.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or queue_task.unit_sha256 != task.unit_sha256
            or queue_task.frozen_payload_sha256
            != task.frozen_payload_sha256
            or queue_task.as_dict() != task.as_dict()
            or queue.get("queue_status")
            not in {"pending", "claimed", "succeeded", "failed"}
            or queue.get("formal_write_count") != 0
        ):
            raise DispatchError(
                "production_canary_recovery_queue_evidence_invalid"
            )

        candidates: list[tuple[Path, dict[str, Any]]] = []
        for pointer_path in sorted(
            self.production_canary_recovery_finalization_root.glob("*.json")
        ):
            pointer = self._read_object(pointer_path)
            if pointer is None:
                continue
            self._verify_seal(
                pointer,
                purpose=(
                    "dispatch-subject-batch-recovery-finalization-pointer"
                ),
            )
            pointer_queue_path = Path(
                str(pointer.get("replacement_queue_entry_path") or "")
            )
            if (
                pointer.get("subject") == "english"
                and pointer.get("status") == "finalized"
                and pointer.get("target_activation_id")
                == state.get("activation_id")
                and pointer.get("target_release_id")
                == state.get("release_id")
                and pointer_queue_path == queue_path
            ):
                candidates.append((pointer_path, pointer))
        if not candidates:
            return False
        if len(candidates) != 1:
            raise DispatchError(
                "production_canary_recovery_queue_evidence_ambiguous"
            )
        pointer_path, pointer = candidates[0]
        recovery_sha256 = _validate_unit_sha256(
            str(pointer.get("recovery_receipt_sha256") or "")
        )
        replacement_queue_sha256 = _validate_unit_sha256(
            str(pointer.get("replacement_queue_entry_sha256") or "")
        )
        supersede_sha256 = _validate_unit_sha256(
            str(pointer.get("supersede_receipt_sha256") or "")
        )
        if (
            pointer.get("schema_version")
            != (
                "study-intake-subject-batch-recovery-"
                "finalization-pointer-v1"
            )
            or pointer_path.name != f"{recovery_sha256}.json"
            or pointer.get("rolled_back_at") is not None
            or pointer.get("formal_write_count") != 0
        ):
            raise DispatchError(
                "production_canary_recovery_queue_evidence_invalid"
            )

        staged_path = (
            self.production_canary_recovery_staged_activation_root
            / f"{recovery_sha256}.json"
        )
        staged = self._read_object(staged_path)
        supersede_path = Path(
            str(pointer.get("supersede_receipt_path") or "")
        )
        supersede = self._read_object(supersede_path)
        if staged is None or supersede is None:
            raise DispatchError(
                "production_canary_recovery_queue_evidence_missing"
            )
        self._verify_seal(
            staged,
            purpose=(
                "dispatch-subject-batch-recovery-staged-activation"
            ),
        )
        self._verify_seal(
            supersede,
            purpose="dispatch-subject-batch-recovery-supersede",
        )
        replacement_task = supersede.get("replacement_task")
        if not isinstance(replacement_task, Mapping):
            raise DispatchError(
                "production_canary_recovery_queue_evidence_invalid"
            )
        task_object_path = Path(str(queue.get("task_object_path") or ""))
        try:
            task_object_sha256 = _sha256_bytes(task_object_path.read_bytes())
            supersede_actual_sha256 = _sha256_bytes(
                supersede_path.read_bytes()
            )
            queue_actual_sha256 = _sha256_bytes(queue_path.read_bytes())
        except OSError as exc:
            raise DispatchError(
                "production_canary_recovery_queue_evidence_missing"
            ) from exc
        if (
            staged.get("schema_version")
            != "study-intake-subject-batch-recovery-staged-activation-v1"
            or staged.get("subject") != "english"
            or staged.get("status") != "armed"
            or staged.get("recovery_receipt_sha256") != recovery_sha256
            or staged.get("target_activation_id")
            != state.get("activation_id")
            or staged.get("target_release_id") != state.get("release_id")
            or staged.get("producer_authority_fingerprint")
            != state.get("producer_authority_fingerprint")
            or staged.get("formal_write_count") != 0
            or supersede_actual_sha256 != supersede_sha256
            or supersede.get("schema_version")
            != "study-intake-subject-batch-recovery-supersede-receipt-v1"
            or supersede.get("subject") != "english"
            or supersede.get("recovery_receipt_sha256") != recovery_sha256
            or supersede.get("target_activation_id")
            != state.get("activation_id")
            or supersede.get("target_release_id") != state.get("release_id")
            or supersede.get("replacement_queue_entry_sha256")
            != replacement_queue_sha256
            or Path(
                str(supersede.get("replacement_queue_entry_path") or "")
            )
            != queue_path
            or supersede.get("source_event_set_sha256")
            != contract.get("source_event_set_sha256")
            or supersede.get("source_event_ids") != source_event_ids
            or supersede.get("model_call_count") != 0
            or supersede.get("provider_request_count") != 0
            or supersede.get("formal_write_count") != 0
            or supersede.get("sol_enabled") is not False
            or replacement_task.get("unit_sha256") != task.unit_sha256
            or replacement_task.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or replacement_task.get("task_object_sha256")
            != queue.get("task_object_sha256")
            or Path(str(replacement_task.get("task_object_path") or ""))
            != task_object_path
            or replacement_task.get("producer_input_contract_sha256")
            != contract_sha256
            or replacement_task.get("source_event_set_sha256")
            != contract.get("source_event_set_sha256")
            or task_object_sha256 != queue.get("task_object_sha256")
            or (
                queue.get("queue_status") == "pending"
                and queue_actual_sha256 != replacement_queue_sha256
            )
        ):
            raise DispatchError(
                "production_canary_recovery_queue_evidence_invalid"
            )
        return True

    def _validate_canary_task_locked(
        self, task: FrozenTask, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        contract = self._producer_contract_from_task(task)
        self._reject_synthetic_production_canary_evidence(task)
        if (
            contract.get("subject") != state.get("subject")
            or contract.get("release_id") != state.get("release_id")
            or contract.get("authority_fingerprint")
            != state.get("producer_authority_fingerprint")
        ):
            raise DispatchError("production_canary_producer_binding_mismatch")
        dispatch_contract = task.frozen_payload.get("dispatch_contract")
        if (
            not isinstance(dispatch_contract, Mapping)
            or "requested_service_tier" not in dispatch_contract
            or dispatch_contract.get("requested_service_tier") is not None
            or dispatch_contract.get("fast_mode_requested") is not False
            or dispatch_contract.get("fast_mode_effective") != "not_requested"
        ):
            raise DispatchError("production_canary_fast_mode_binding_invalid")
        try:
            committed_math_migration = (
                verify_committed_exact_math_migration_claim(
                    self.runtime_root,
                    task.frozen_payload,
                    task.unit_sha256,
                    state,
                    contract,
                )
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc
        if (
            _parse_utc(contract.get("producer_recorded_at"))
            <= _parse_utc(state.get("activated_at"))
            and not self._is_verified_subject_batch_recovery_task_locked(
                task, state, contract
            )
            and not committed_math_migration
        ):
            raise DispatchError("production_canary_pre_activation_capture")
        if contract.get("subject") != "english" and any(
            _parse_utc(row.get("recorded_at"))
            <= _parse_utc(state.get("activated_at"))
            for row in contract["source_events"]
        ) and not committed_math_migration:
            # Math/408 content groups may alias several captures.  A newly
            # appended member must never drag a pre-activation alias through
            # the frozen backlog fence.  English is intentionally different:
            # one new producer microbatch can bind earlier sentence events.
            raise DispatchError("production_canary_pre_activation_capture")
        return contract

    @staticmethod
    def _preclaim_optional_sha256(value: Any) -> str | None:
        if (
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        ):
            return value
        return None

    def _preclaim_failure_evidence_locked(
        self,
        task: FrozenTask | None,
        queue_entry: Mapping[str, Any] | None,
        lease: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        payload = task.frozen_payload if task is not None else {}
        dispatch_contract = payload.get("dispatch_contract")
        producer_contract = (
            dispatch_contract.get("producer_input_contract")
            if isinstance(dispatch_contract, Mapping)
            else None
        )
        producer_contract = (
            producer_contract
            if isinstance(producer_contract, Mapping)
            else {}
        )
        producer_recorded_at = producer_contract.get("producer_recorded_at")
        try:
            _parse_utc(producer_recorded_at)
        except DispatchError:
            producer_recorded_at = None
        task_subject = payload.get("subject")
        capture_id = payload.get("capture_id")
        producer_unit_id = producer_contract.get("producer_unit_id")
        core = {
            "unit_sha256": task.unit_sha256 if task is not None else None,
            "frozen_payload_sha256": (
                task.frozen_payload_sha256 if task is not None else None
            ),
            "task_subject": (
                task_subject if isinstance(task_subject, str) else None
            ),
            "capture_id": capture_id if isinstance(capture_id, str) else None,
            "producer_unit_id": (
                producer_unit_id
                if isinstance(producer_unit_id, str) and producer_unit_id
                else None
            ),
            "producer_recorded_at": producer_recorded_at,
            "producer_input_contract_sha256": self._preclaim_optional_sha256(
                producer_contract.get("producer_input_contract_sha256")
            ),
            "source_event_set_sha256": self._preclaim_optional_sha256(
                producer_contract.get("source_event_set_sha256")
            ),
            "queue_entry_sha256": (
                _sha256_bytes(_canonical_bytes(queue_entry))
                if isinstance(queue_entry, Mapping)
                else None
            ),
            "queue_status_before": (
                str(queue_entry.get("queue_status"))
                if isinstance(queue_entry, Mapping)
                and queue_entry.get("queue_status")
                in {"pending", "claimed", "succeeded", "failed"}
                else None
            ),
            "canary_gate_sha256": (
                self._preclaim_optional_sha256(
                    queue_entry.get("canary_gate_sha256")
                )
                if isinstance(queue_entry, Mapping)
                else None
            ),
            "lease_fence": (
                int(lease["fence"])
                if isinstance(lease, Mapping)
                and isinstance(lease.get("fence"), int)
                and int(lease["fence"]) >= 1
                else None
            ),
        }
        return {
            **core,
            "evidence_sha256": _sha256_bytes(_canonical_bytes(core)),
        }

    def _preclaim_failure_receipts_locked(
        self, subject: str, activation_id: str
    ) -> list[dict[str, Any]]:
        root = (
            self.production_canary_preclaim_failure_root
            / _safe_component(subject)
            / _safe_component(activation_id)
        )
        receipts: list[dict[str, Any]] = []
        for path in sorted(root.glob("*.json")):
            value = self._read_object(path)
            if value is None:
                raise DispatchError(
                    "production_canary_preclaim_failure_index_invalid"
                )
            self._verify_seal(
                value, purpose="dispatch-production-canary-preclaim-failure"
            )
            if (
                value.get("schema_version")
                != PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA
                or value.get("activation_id") != activation_id
                or value.get("subject") != subject
            ):
                raise DispatchError(
                    "production_canary_preclaim_failure_index_invalid"
                )
            receipts.append(dict(value))
        receipts.sort(
            key=lambda value: (
                _parse_utc(value.get("failed_at")),
                str(value.get("failure_id") or ""),
            )
        )
        return receipts

    def _canonical_preserved_preclaim_failure_locked(
        self,
        state: Mapping[str, Any],
        receipts: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], str, dict[str, Any]] | None:
        """Bind recovery to the queue's original failure, never the latest scan."""

        subject = str(state["subject"])
        pending = [
            row
            for row in self._queue_entries_locked(subject)
            if row.get("queue_status") == "pending"
            and isinstance(row.get("terminal_receipt_sha256"), str)
        ]
        if not pending:
            return None
        matches: list[tuple[dict[str, Any], str, dict[str, Any]]] = []
        by_digest = {
            _sha256_bytes(_canonical_bytes(row) + b"\n"): dict(row)
            for row in receipts
        }
        for queue_entry in pending:
            digest = str(queue_entry["terminal_receipt_sha256"])
            receipt = by_digest.get(digest)
            if receipt is None:
                raise DispatchError(
                    "production_canary_preserved_failure_receipt_missing"
                )
            evidence = receipt.get("failure_evidence")
            if (
                receipt.get("queue_entry_preserved") is not True
                or receipt.get("failure_stage") != "pre_claim"
                or receipt.get("model_submission_started") is not False
            ):
                continue
            if (
                not isinstance(evidence, Mapping)
                or evidence.get("producer_unit_id")
                != queue_entry.get("producer_unit_id")
                or evidence.get("producer_input_contract_sha256")
                != queue_entry.get("producer_input_contract_sha256")
                or evidence.get("source_event_set_sha256")
                != queue_entry.get("source_event_set_sha256")
                or evidence.get("unit_sha256") != queue_entry.get("unit_sha256")
                or evidence.get("frozen_payload_sha256")
                != queue_entry.get("frozen_payload_sha256")
            ):
                raise DispatchError(
                    "production_canary_preserved_failure_binding_invalid"
                )
            matches.append((receipt, digest, dict(queue_entry)))
        if len(matches) != 1:
            raise DispatchError(
                "production_canary_preserved_queue_not_unique"
            )
        return matches[0]

    def _preclaim_queue_entry_locked(
        self, subject: str, evidence: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        contract_sha256 = self._preclaim_optional_sha256(
            evidence.get("producer_input_contract_sha256")
        )
        if contract_sha256 is not None:
            candidate = self._read_object(
                self._production_canary_queue_path(subject, contract_sha256)
            )
            if isinstance(candidate, Mapping):
                self._verify_seal(
                    candidate, purpose="dispatch-production-canary-queue"
                )
                if (
                    candidate.get("unit_sha256")
                    == evidence.get("unit_sha256")
                    and candidate.get("frozen_payload_sha256")
                    == evidence.get("frozen_payload_sha256")
                ):
                    return dict(candidate)
                return None
        try:
            candidates = self._queue_entries_locked(subject)
        except DispatchError:
            return None
        return next(
            (
                dict(candidate)
                for candidate in candidates
                if candidate.get("unit_sha256")
                == evidence.get("unit_sha256")
                and candidate.get("frozen_payload_sha256")
                == evidence.get("frozen_payload_sha256")
            ),
            None,
        )

    def _publish_preclaim_receipt_locked(
        self, subject: str, activation_id: str, receipt: Mapping[str, Any]
    ) -> tuple[str, Path]:
        return _publish_content_addressed(
            self.production_canary_receipt_root
            / _safe_component(subject)
            / _safe_component(activation_id),
            receipt,
        )

    def _commit_production_canary_preclaim_failure_locked(
        self,
        state: Mapping[str, Any],
        receipt: Mapping[str, Any],
        receipt_sha256: str,
        receipt_path: Path,
    ) -> dict[str, Any]:
        subject = str(state["subject"])
        evidence = receipt.get("failure_evidence")
        if not isinstance(evidence, Mapping):
            raise DispatchError(
                "production_canary_preclaim_failure_evidence_invalid"
            )
        timestamp = str(receipt["failed_at"])
        queue_entry = self._preclaim_queue_entry_locked(subject, evidence)
        unit_sha256 = self._preclaim_optional_sha256(
            evidence.get("unit_sha256")
        )
        lease: dict[str, Any] | None = None
        if unit_sha256 is not None:
            candidate_lease = self._read_object(self._lease_path(unit_sha256))
            if (
                isinstance(candidate_lease, Mapping)
                and candidate_lease.get("status") == "claimed"
                and candidate_lease.get("subject") == subject
            ):
                lease = dict(candidate_lease)
                terminal_lease = dict(lease)
                terminal_lease.update(
                    {
                        "status": "preclaim_failed",
                        "completed_at": timestamp,
                        "outcome": "failed",
                        "error_code": receipt["error_code"],
                    }
                )
                _atomic_replace_json(
                    self._lease_path(unit_sha256), terminal_lease
                )

        selected = state.get("selected")
        active_selections = copy.deepcopy(
            dict(state.get("active_selections") or {})
        )
        selected_is_failed_task = bool(
            unit_sha256 is not None and unit_sha256 in active_selections
        )
        if unit_sha256 is not None:
            active_selections.pop(unit_sha256, None)
        active_after = len(active_selections)
        if queue_entry is not None and queue_entry.get("queue_status") in {
            "pending",
            "claimed",
        }:
            queue_entry = self._update_canary_queue_entry_locked(
                queue_entry,
                queue_status="pending",
                claimed_at=None,
                finished_at=None,
                terminal_receipt_sha256=receipt_sha256,
                terminal_receipt_path=str(receipt_path),
                terminal_outcome="failed",
                terminal_error_code=receipt["error_code"],
            )

        failed_state = dict(state)
        failed_state.update(
            {
                "state": "failed_drained",
                "luna_consumer_enabled": False,
                "active_task_count": active_after,
                "active_selections": active_selections,
                "selected": None if selected_is_failed_task else selected,
                "last_failure_at": timestamp,
                "blocking_reason": receipt["error_code"],
                "next_action": "explicit_subject_resume_required",
                "last_terminal_receipt_sha256": receipt_sha256,
                "last_terminal_receipt_path": str(receipt_path),
                "last_preclaim_failure_at": timestamp,
                "last_preclaim_failure_stage": receipt["failure_stage"],
                "last_preclaim_failure_error_code": receipt["error_code"],
                "last_preclaim_failure_receipt_sha256": receipt_sha256,
                "last_preclaim_failure_evidence_sha256": evidence[
                    "evidence_sha256"
                ],
                "preclaim_failure_resume_ack_sha256": None,
                "backpressure_reason": None,
            }
        )
        try:
            failed_state = self._refresh_canary_queue_projection_locked(
                failed_state
            )
        except DispatchError:
            # The HMAC state remains fail-closed even if queue observability is
            # the failing subsystem.  No consumer reopens from stale counts.
            pass
        written = self._write_canary_state_locked(
            failed_state, updated_at=timestamp
        )
        self._recompute_canary_concurrency_telemetry_locked(
            str(state["release_id"]), updated_at=timestamp
        )
        return written

    def _preclaim_repair_ack_matches_locked(
        self, state: Mapping[str, Any], failure_sha256: str
    ) -> bool:
        ack_sha256 = state.get("preclaim_failure_resume_ack_sha256")
        if not isinstance(ack_sha256, str):
            return False
        _validate_unit_sha256(ack_sha256)
        path = (
            self.production_canary_receipt_root
            / _safe_component(str(state["subject"]))
            / _safe_component(str(state["activation_id"]))
            / "sha256"
            / ack_sha256[:2]
            / f"{ack_sha256}.json"
        )
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise DispatchError(
                "production_canary_preclaim_repair_ack_missing"
            ) from exc
        ack = self._read_object(path)
        if _sha256_bytes(payload) != ack_sha256 or ack is None:
            raise DispatchError(
                "production_canary_preclaim_repair_ack_invalid"
            )
        self._verify_seal(
            ack, purpose="dispatch-production-canary-preclaim-repair-ack"
        )
        if (
            ack.get("schema_version")
            != PRODUCTION_CANARY_PRECLAIM_REPAIR_ACK_SCHEMA
            or ack.get("activation_id") != state.get("activation_id")
            or ack.get("release_id") != state.get("release_id")
            or ack.get("subject") != state.get("subject")
            or ack.get("preclaim_failure_receipt_sha256") != failure_sha256
            or ack.get("ack_action") != "explicit_subject_resume_repair"
            or ack.get("model_call_count") != 0
            or ack.get("provider_request_count") != 0
            or ack.get("mcp_tool_call_count") != 0
            or ack.get("formal_write_count") != 0
            or ack.get("sol_enabled") is not False
        ):
            raise DispatchError(
                "production_canary_preclaim_repair_ack_invalid"
            )
        return True

    def _reconcile_production_canary_preclaim_failures_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        subject = str(state["subject"])
        receipts = self._preclaim_failure_receipts_locked(
            subject, str(state["activation_id"])
        )
        if not receipts:
            return dict(state)
        canonical = (
            self._canonical_preserved_preclaim_failure_locked(
                state, receipts
            )
            if subject == "english"
            else None
        )
        latest = canonical[0] if canonical is not None else receipts[-1]
        latest_sha256 = (
            canonical[1]
            if canonical is not None
            else _sha256_bytes(_canonical_bytes(latest) + b"\n")
        )
        if (
            state.get("state") == "failed_drained"
            and state.get("last_preclaim_failure_receipt_sha256")
            == latest_sha256
        ):
            return dict(state)
        if self._preclaim_repair_ack_matches_locked(state, latest_sha256):
            return dict(state)
        receipt_sha256, receipt_path = self._publish_preclaim_receipt_locked(
            subject, str(state["activation_id"]), latest
        )
        if receipt_sha256 != latest_sha256:
            raise DispatchError(
                "production_canary_preclaim_receipt_reopen_invalid"
            )
        return self._commit_production_canary_preclaim_failure_locked(
            state, latest, receipt_sha256, receipt_path
        )

    def reconcile_production_canary_preclaim_failures(
        self, subject: str
    ) -> dict[str, Any]:
        """Recover receipt-first crash windows before any new claim."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            return self._reconcile_production_canary_preclaim_failures_locked(
                state
            )

    def production_canary_global_preclaim_failure(
        self, subject: str
    ) -> dict[str, Any] | None:
        """Suppress a task-less producer scan failure until explicit resume."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            state = self._reconcile_production_canary_preclaim_failures_locked(
                state
            )
            if state.get("state") != "failed_drained":
                return None
            path = self._production_canary_preclaim_failure_path(
                subject,
                str(state["activation_id"]),
                unit_sha256=None,
                frozen_payload_sha256=None,
            )
            value = self._read_object(path)
            if value is None:
                return None
            self._verify_seal(
                value, purpose="dispatch-production-canary-preclaim-failure"
            )
            receipt_sha256 = _sha256_bytes(path.read_bytes())
            if (
                value.get("schema_version")
                != PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA
                or value.get("activation_id") != state.get("activation_id")
                or value.get("subject") != subject
                or not isinstance(value.get("failure_evidence"), Mapping)
                or value["failure_evidence"].get("unit_sha256") is not None
            ):
                raise DispatchError(
                    "production_canary_preclaim_failure_index_invalid"
                )
            return {
                "receipt": dict(value),
                "receipt_sha256": receipt_sha256,
                "receipt_path": str(path),
            }

    def production_canary_preclaim_failure_for_task(
        self, subject: str, task: FrozenTask
    ) -> dict[str, Any] | None:
        """Return the current activation's durable failure suppression row."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            state = self._reconcile_production_canary_preclaim_failures_locked(
                state
            )
            if state.get("state") != "failed_drained":
                return None
            path = self._production_canary_preclaim_failure_path(
                subject,
                str(state["activation_id"]),
                unit_sha256=task.unit_sha256,
                frozen_payload_sha256=task.frozen_payload_sha256,
            )
            value = self._read_object(path)
            if value is None:
                return None
            self._verify_seal(
                value, purpose="dispatch-production-canary-preclaim-failure"
            )
            evidence = value.get("failure_evidence")
            if (
                value.get("schema_version")
                != PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA
                or value.get("activation_id") != state.get("activation_id")
                or value.get("subject") != subject
                or not isinstance(evidence, Mapping)
                or evidence.get("unit_sha256") != task.unit_sha256
                or evidence.get("frozen_payload_sha256")
                != task.frozen_payload_sha256
            ):
                raise DispatchError(
                    "production_canary_preclaim_failure_index_invalid"
                )
            return {
                "receipt": dict(value),
                "receipt_sha256": _sha256_bytes(path.read_bytes()),
                "receipt_path": str(path),
            }

    def production_canary_recovery_evidence(
        self,
        subject: str,
        *,
        expected_original_preclaim_failure_receipt_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Reopen the one preserved queue and its original pre-claim receipt."""

        expected = (
            _validate_unit_sha256(
                expected_original_preclaim_failure_receipt_sha256
            )
            if expected_original_preclaim_failure_receipt_sha256 is not None
            else None
        )
        with _ExistingSharedFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if (
                state.get("state")
                not in {"failed_drained", "inactive_rolled_back"}
                or state.get("luna_consumer_enabled") is not False
                or int(state.get("active_task_count") or 0) != 0
            ):
                raise DispatchError("production_canary_recovery_state_invalid")
            receipts = self._preclaim_failure_receipts_locked(
                subject, str(state["activation_id"])
            )
            canonical = self._canonical_preserved_preclaim_failure_locked(
                state, receipts
            )
            if canonical is None:
                raise DispatchError(
                    "production_canary_preserved_failure_missing"
                )
            receipt, receipt_sha256, queue_entry = canonical
            if expected is not None and receipt_sha256 != expected:
                raise DispatchError(
                    "production_canary_recovery_original_receipt_mismatch"
                )
            receipt_path = (
                self.production_canary_receipt_root
                / _safe_component(subject)
                / _safe_component(str(state["activation_id"]))
                / "sha256"
                / receipt_sha256[:2]
                / f"{receipt_sha256}.json"
            )
            try:
                receipt_bytes = receipt_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "production_canary_preclaim_failure_receipt_missing"
                ) from exc
            if _sha256_bytes(receipt_bytes) != receipt_sha256:
                raise DispatchError(
                    "production_canary_preclaim_failure_receipt_invalid"
                )
            self._verify_seal(
                receipt, purpose="dispatch-production-canary-preclaim-failure"
            )
            producer_unit_id = queue_entry.get("producer_unit_id")
            source_event_set_sha256 = queue_entry.get("source_event_set_sha256")
            attempts = sorted(
                _sha256_bytes(_canonical_bytes(row) + b"\n")
                for row in receipts
                if _sha256_bytes(_canonical_bytes(row) + b"\n")
                != receipt_sha256
                and isinstance(row.get("failure_evidence"), Mapping)
                and row["failure_evidence"].get("producer_unit_id")
                == producer_unit_id
                and row["failure_evidence"].get("source_event_set_sha256")
                == source_event_set_sha256
            )
            task = self._task_from_canary_queue_entry_locked(queue_entry)
            contract = self._producer_contract_from_task(task)
            if (
                contract.get("producer_unit_id") != producer_unit_id
                or contract.get("producer_input_contract_sha256")
                != queue_entry.get("producer_input_contract_sha256")
                or contract.get("source_event_set_sha256")
                != source_event_set_sha256
            ):
                raise DispatchError(
                    "production_canary_recovery_task_binding_invalid"
                )
            preserved_task = {
                "unit_sha256": task.unit_sha256,
                "frozen_payload_sha256": task.frozen_payload_sha256,
                "task_object_sha256": queue_entry["task_object_sha256"],
                "task_object_path": queue_entry["task_object_path"],
                "producer_input_contract_sha256": queue_entry[
                    "producer_input_contract_sha256"
                ],
                "source_event_set_sha256": source_event_set_sha256,
            }
            return {
                "schema_version": "study-intake-production-canary-recovery-evidence-v1",
                "subject": subject,
                "activation_id": state["activation_id"],
                "release_id": state["release_id"],
                "canary_state_before": copy.deepcopy(state),
                "canary_state_before_sha256": _sha256_bytes(
                    _canonical_bytes(state)
                ),
                "original_preclaim_failure_receipt_sha256": receipt_sha256,
                "original_preclaim_failure_receipt_path": str(receipt_path),
                "preserved_queue_entry": copy.deepcopy(queue_entry),
                "preserved_queue_entry_sha256": _sha256_bytes(
                    _canonical_bytes(queue_entry) + b"\n"
                ),
                "subsequent_attempt_receipt_sha256s": attempts,
                "preserved_task": preserved_task,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }

    def verify_production_canary_preserved_queue_postimage(
        self,
        subject: str,
        *,
        activation_id: str,
        producer_input_contract_sha256: str,
        expected_queue_entry_sha256: str,
    ) -> dict[str, Any]:
        """Verify a preserved queue by explicit activation, independent of gate."""

        activation_id = _validate_unit_sha256(activation_id)
        contract_sha256 = _validate_unit_sha256(
            producer_input_contract_sha256
        )
        expected_sha256 = _validate_unit_sha256(
            expected_queue_entry_sha256
        )
        with _ExclusiveFileLock(self.lock_path):
            path = (
                self._production_canary_queue_subject_root(
                    subject, activation_id
                )
                / f"{contract_sha256}.json"
            )
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "production_canary_preserved_queue_missing"
                ) from exc
            value = self._read_object(path)
            if value is None:
                raise DispatchError(
                    "production_canary_preserved_queue_invalid"
                )
            self._verify_seal(
                value, purpose="dispatch-production-canary-queue"
            )
            if (
                _sha256_bytes(payload) != expected_sha256
                or value.get("schema_version")
                != PRODUCTION_CANARY_QUEUE_SCHEMA
                or value.get("subject") != subject
                or value.get("activation_id") != activation_id
                or value.get("producer_input_contract_sha256")
                != contract_sha256
                or value.get("queue_status") != "pending"
                or value.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "production_canary_preserved_queue_postimage_mismatch"
                )
            return {
                "queue_entry": dict(value),
                "queue_entry_sha256": expected_sha256,
                "queue_entry_path": str(path),
            }

    def finalize_production_canary_subject_batch_recovery(
        self,
        subject: str,
        *,
        recovery_receipt_sha256: str,
        recovery_receipt_path: str,
        expected_target_release_id: str,
        old_queue_identity: Mapping[str, Any],
        old_queue_entry_sha256: str,
        replacement_task: FrozenTask,
    ) -> dict[str, Any]:
        """Create exactly one successor queue and its cross-activation proof."""

        if subject != "english":
            raise DispatchError("subject_batch_recovery_english_only")
        recovery_sha256 = _validate_unit_sha256(
            recovery_receipt_sha256
        )
        target_release_id = _validate_unit_sha256(
            expected_target_release_id
        )
        old_queue_sha256 = _validate_unit_sha256(old_queue_entry_sha256)
        old_identity = copy.deepcopy(dict(old_queue_identity))
        old_activation_id = _validate_unit_sha256(
            str(old_identity.get("activation_id") or "")
        )
        old_contract_sha256 = _validate_unit_sha256(
            str(old_identity.get("producer_input_contract_sha256") or "")
        )
        old_source_set_sha256 = _validate_unit_sha256(
            str(old_identity.get("source_event_set_sha256") or "")
        )
        old_source_event_ids = list(old_identity.get("source_event_ids") or [])
        pointer_path = (
            self.production_canary_recovery_finalization_root
            / f"{recovery_sha256}.json"
        )
        intent_path = (
            self.production_canary_recovery_finalization_intent_root
            / f"{recovery_sha256}.json"
        )
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if (
                state.get("release_id") != target_release_id
                or state.get("state") != "paused_drained"
                or state.get("luna_consumer_enabled") is not False
                or int(state.get("active_task_count") or 0) != 0
                or state.get("selected") is not None
                or state.get("formal_write_count") != 0
                or state.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "subject_batch_recovery_target_state_invalid"
                )
            target_activation_id = _validate_unit_sha256(
                str(state.get("activation_id") or "")
            )
            staged_pointer_path = (
                self.production_canary_recovery_staged_activation_root
                / f"{recovery_sha256}.json"
            )
            staged_pointer = self._read_object(staged_pointer_path)
            if staged_pointer is None:
                raise DispatchError(
                    "subject_batch_recovery_staged_activation_missing"
                )
            self._verify_seal(
                staged_pointer,
                purpose=(
                    "dispatch-subject-batch-recovery-staged-activation"
                ),
            )
            if (
                staged_pointer.get("status") != "staged"
                or staged_pointer.get("recovery_receipt_sha256")
                != recovery_sha256
                or staged_pointer.get("target_activation_id")
                != target_activation_id
                or staged_pointer.get("target_release_id")
                != target_release_id
                or staged_pointer.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "subject_batch_recovery_staged_activation_conflict"
                )
            if target_activation_id == old_activation_id:
                raise DispatchError(
                    "subject_batch_recovery_target_activation_not_new"
                )
            old_path = (
                self._production_canary_queue_subject_root(
                    subject, old_activation_id
                )
                / f"{old_contract_sha256}.json"
            )
            old_queue = self._read_object(old_path)
            if old_queue is None:
                raise DispatchError(
                    "production_canary_preserved_queue_missing"
                )
            self._verify_seal(
                old_queue, purpose="dispatch-production-canary-queue"
            )
            if (
                _sha256_bytes(old_path.read_bytes()) != old_queue_sha256
                or old_queue.get("queue_status") != "pending"
                or old_queue.get("terminal_receipt_sha256") is None
                or old_queue.get("source_event_set_sha256")
                != old_source_set_sha256
            ):
                raise DispatchError(
                    "production_canary_preserved_queue_postimage_mismatch"
                )
            existing_pointer = self._read_object(pointer_path)
            if existing_pointer is not None:
                self._verify_seal(
                    existing_pointer,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-pointer"
                    ),
                )
                if (
                    existing_pointer.get("recovery_receipt_sha256")
                    != recovery_sha256
                    or existing_pointer.get("target_release_id")
                    != target_release_id
                    or existing_pointer.get("target_activation_id")
                    != target_activation_id
                    or existing_pointer.get("status") != "finalized"
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_conflict"
                    )
                queue_path = Path(
                    str(existing_pointer["replacement_queue_entry_path"])
                )
                try:
                    queue_bytes = queue_path.read_bytes()
                except OSError as exc:
                    raise DispatchError(
                        "subject_batch_recovery_finalization_queue_missing"
                    ) from exc
                if (
                    _sha256_bytes(queue_bytes)
                    != existing_pointer["replacement_queue_entry_sha256"]
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_queue_drift"
                    )
                supersede_path = Path(
                    str(existing_pointer["supersede_receipt_path"])
                )
                supersede = self._read_object(supersede_path)
                if supersede is None:
                    raise DispatchError(
                        "subject_batch_recovery_supersede_receipt_missing"
                    )
                self._verify_seal(
                    supersede,
                    purpose=(
                        "dispatch-subject-batch-recovery-supersede"
                    ),
                )
                return {
                    **dict(existing_pointer),
                    "replacement_task": supersede["replacement_task"],
                    "old_activation_id": supersede["old_activation_id"],
                    "old_queue_entry_sha256": supersede[
                        "old_queue_entry_sha256"
                    ],
                    "idempotent": True,
                }
            contract = self._producer_contract_from_task(replacement_task)
            contract_source_ids = [
                str(row["event_id"]) for row in contract["source_events"]
            ]
            if (
                contract.get("subject") != subject
                or contract.get("release_id") != target_release_id
                or contract.get("authority_fingerprint")
                != state.get("producer_authority_fingerprint")
                or contract.get("source_event_set_sha256")
                != old_source_set_sha256
                or contract_source_ids != old_source_event_ids
            ):
                raise DispatchError("deterministic_producer_drift")
            contract_sha256 = str(
                contract["producer_input_contract_sha256"]
            )
            queue_path = (
                self._production_canary_queue_subject_root(
                    subject, target_activation_id
                )
                / f"{contract_sha256}.json"
            )
            task_sha256, task_path = _publish_content_addressed(
                self.runtime_root
                / "dispatch"
                / "production-canary"
                / "tasks"
                / subject,
                replacement_task.as_dict(),
            )
            intent_core = {
                "schema_version": (
                    "study-intake-subject-batch-recovery-finalization-intent-v1"
                ),
                "subject": subject,
                "recovery_receipt_sha256": recovery_sha256,
                "recovery_receipt_path": recovery_receipt_path,
                "old_activation_id": old_activation_id,
                "old_queue_entry_sha256": old_queue_sha256,
                "source_event_set_sha256": old_source_set_sha256,
                "source_event_ids": old_source_event_ids,
                "target_activation_id": target_activation_id,
                "target_release_id": target_release_id,
                "replacement_unit_sha256": replacement_task.unit_sha256,
                "replacement_frozen_payload_sha256": (
                    replacement_task.frozen_payload_sha256
                ),
                "replacement_task_object_sha256": task_sha256,
                "replacement_task_object_path": str(task_path),
                "replacement_producer_input_contract_sha256": (
                    contract_sha256
                ),
                "replacement_queue_entry_path": str(queue_path),
                "created_at": _utc_now(),
                "formal_write_count": 0,
            }
            existing_intent = self._read_object(intent_path)
            if existing_intent is None:
                (
                    _intent_receipt_sha256,
                    _intent_receipt_path,
                    existing_intent,
                ) = self._publish_canary_receipt_locked(
                    subject,
                    target_activation_id,
                    intent_core,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-intent"
                    ),
                )
                _atomic_replace_json(intent_path, existing_intent)
            else:
                self._verify_seal(
                    existing_intent,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-intent"
                    ),
                )
                if any(
                    existing_intent.get(key) != value
                    for key, value in intent_core.items()
                    if key != "created_at"
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_intent_conflict"
                    )
                _publish_content_addressed(
                    self.production_canary_receipt_root
                    / _safe_component(subject)
                    / _safe_component(target_activation_id),
                    existing_intent,
                )
            existing_queue = self._read_object(queue_path)
            if existing_queue is None:
                for row in self._queue_entries_locked(subject):
                    row_ids = set(row.get("source_event_ids") or [])
                    if (
                        row.get("source_event_set_sha256")
                        == old_source_set_sha256
                        or set(contract_source_ids) & row_ids
                    ):
                        raise DispatchError("deterministic_producer_drift")
                queue_core = {
                    "schema_version": PRODUCTION_CANARY_QUEUE_SCHEMA,
                    "activation_id": target_activation_id,
                    "subject": subject,
                    "release_id": target_release_id,
                    "producer_high_watermark_sha256": state[
                        "producer_high_watermark_sha256"
                    ],
                    "producer_authority_fingerprint": state[
                        "producer_authority_fingerprint"
                    ],
                    "producer_input_contract_sha256": contract_sha256,
                    "producer_unit_id": contract["producer_unit_id"],
                    "producer_recorded_at": contract["producer_recorded_at"],
                    "source_event_set_sha256": old_source_set_sha256,
                    "source_event_ids": contract_source_ids,
                    "unit_sha256": replacement_task.unit_sha256,
                    "frozen_payload_sha256": (
                        replacement_task.frozen_payload_sha256
                    ),
                    "task_object_sha256": task_sha256,
                    "task_object_path": str(task_path),
                    "queue_status": "pending",
                    "discovered_at": _utc_now(),
                    "claimed_at": None,
                    "finished_at": None,
                    "canary_gate_sha256": None,
                    "canary_gate_path": None,
                    "canary_gate_authority_sha256": None,
                    "lease_owner_id": None,
                    "lease_fence": None,
                    "context_root": None,
                    "mcp_session_root": None,
                    "report_root": None,
                    "process_identity_sha256": None,
                    "process_identity_path": None,
                    "terminal_receipt_sha256": None,
                    "terminal_receipt_path": None,
                    "terminal_outcome": None,
                    "terminal_error_code": None,
                    "formal_write_count": 0,
                }
                existing_queue = self._seal(
                    queue_core, purpose="dispatch-production-canary-queue"
                )
                _atomic_replace_json(queue_path, existing_queue)
                if (
                    os.environ.get(FINALIZATION_FAILPOINT_ENV)
                    == "after_queue_before_supersede_pointer"
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_injected_crash"
                    )
            else:
                self._verify_seal(
                    existing_queue,
                    purpose="dispatch-production-canary-queue",
                )
                if (
                    existing_queue.get("activation_id")
                    != target_activation_id
                    or existing_queue.get("release_id") != target_release_id
                    or existing_queue.get("unit_sha256")
                    != replacement_task.unit_sha256
                    or existing_queue.get("frozen_payload_sha256")
                    != replacement_task.frozen_payload_sha256
                    or existing_queue.get("queue_status") != "pending"
                    or existing_queue.get("terminal_receipt_sha256") is not None
                ):
                    raise DispatchError("deterministic_producer_drift")
            queue_sha256 = _sha256_bytes(queue_path.read_bytes())
            supersede_core = {
                "schema_version": (
                    "study-intake-subject-batch-recovery-supersede-receipt-v1"
                ),
                "subject": subject,
                "recovery_receipt_sha256": recovery_sha256,
                "recovery_receipt_path": recovery_receipt_path,
                "old_activation_id": old_activation_id,
                "old_queue_entry_sha256": old_queue_sha256,
                "old_producer_input_contract_sha256": old_contract_sha256,
                "source_event_set_sha256": old_source_set_sha256,
                "source_event_ids": old_source_event_ids,
                "target_activation_id": target_activation_id,
                "target_release_id": target_release_id,
                "replacement_task": {
                    "unit_sha256": replacement_task.unit_sha256,
                    "frozen_payload_sha256": (
                        replacement_task.frozen_payload_sha256
                    ),
                    "task_object_sha256": existing_queue[
                        "task_object_sha256"
                    ],
                    "task_object_path": existing_queue["task_object_path"],
                    "producer_input_contract_sha256": contract_sha256,
                    "source_event_set_sha256": old_source_set_sha256,
                },
                "replacement_queue_entry_sha256": queue_sha256,
                "replacement_queue_entry_path": str(queue_path),
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "created_at": existing_intent["created_at"],
            }
            supersede_sha256, supersede_path, supersede = (
                self._publish_canary_receipt_locked(
                    subject,
                    target_activation_id,
                    supersede_core,
                    purpose=(
                        "dispatch-subject-batch-recovery-supersede"
                    ),
                )
            )
            if (
                os.environ.get(FINALIZATION_FAILPOINT_ENV)
                == "after_supersede_before_pointer"
            ):
                raise DispatchError(
                    "subject_batch_recovery_finalization_injected_crash"
                )
            pointer = self._seal(
                {
                    "schema_version": (
                        "study-intake-subject-batch-recovery-finalization-pointer-v1"
                    ),
                    "subject": subject,
                    "status": "finalized",
                    "recovery_receipt_sha256": recovery_sha256,
                    "supersede_receipt_sha256": supersede_sha256,
                    "supersede_receipt_path": str(supersede_path),
                    "target_activation_id": target_activation_id,
                    "target_release_id": target_release_id,
                    "replacement_queue_entry_sha256": queue_sha256,
                    "replacement_queue_entry_path": str(queue_path),
                    "created_at": supersede["created_at"],
                    "rolled_back_at": None,
                    "formal_write_count": 0,
                },
                purpose=(
                    "dispatch-subject-batch-recovery-finalization-pointer"
                ),
            )
            _atomic_replace_json(pointer_path, pointer)
            refreshed = self._refresh_canary_queue_projection_locked(state)
            if self._canary_projection_changed(state, refreshed):
                self._write_canary_state_locked(refreshed)
            return {
                **pointer,
                "replacement_task": supersede["replacement_task"],
                "old_activation_id": old_activation_id,
                "old_queue_entry_sha256": old_queue_sha256,
                "idempotent": False,
            }

    def rollback_production_canary_subject_batch_finalization(
        self,
        *,
        recovery_receipt_sha256: str,
        apply: bool,
    ) -> dict[str, Any] | None:
        """Withdraw an unused target queue while preserving CA evidence."""

        recovery_sha256 = _validate_unit_sha256(
            recovery_receipt_sha256
        )
        pointer_path = (
            self.production_canary_recovery_finalization_root
            / f"{recovery_sha256}.json"
        )
        intent_path = (
            self.production_canary_recovery_finalization_intent_root
            / f"{recovery_sha256}.json"
        )
        rollback_intent_path = (
            self.production_canary_recovery_finalization_rollback_intent_root
            / f"{recovery_sha256}.json"
        )
        with _ExclusiveFileLock(self.lock_path):
            pointer = self._read_object(pointer_path)
            intent = self._read_object(intent_path)
            rollback_intent = self._read_object(rollback_intent_path)
            if pointer is None and intent is None:
                return None
            if pointer is not None:
                self._verify_seal(
                    pointer,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-pointer"
                    ),
                )
            if intent is not None:
                self._verify_seal(
                    intent,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-intent"
                    ),
                )
                if (
                    intent.get("schema_version")
                    != "study-intake-subject-batch-recovery-finalization-intent-v1"
                    or intent.get("recovery_receipt_sha256")
                    != recovery_sha256
                    or intent.get("formal_write_count") != 0
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_intent_invalid"
                    )
                intent_payload = _canonical_bytes(intent) + b"\n"
                intent_sha256 = _sha256_bytes(intent_payload)
                intent_receipt_path = (
                    self.production_canary_receipt_root
                    / "english"
                    / str(intent["target_activation_id"])
                    / "sha256"
                    / intent_sha256[:2]
                    / f"{intent_sha256}.json"
                )
                try:
                    intent_receipt_payload = intent_receipt_path.read_bytes()
                except OSError as exc:
                    raise DispatchError(
                        "subject_batch_recovery_finalization_intent_receipt_missing"
                    ) from exc
                if intent_receipt_payload != intent_payload:
                    raise DispatchError(
                        "subject_batch_recovery_finalization_intent_receipt_invalid"
                    )
            if pointer is None:
                assert intent is not None
                pointer = self._seal(
                    {
                        "schema_version": (
                            "study-intake-subject-batch-recovery-finalization-pointer-v1"
                        ),
                        "subject": "english",
                        "status": "finalized",
                        "recovery_receipt_sha256": recovery_sha256,
                        "supersede_receipt_sha256": None,
                        "supersede_receipt_path": None,
                        "target_activation_id": intent[
                            "target_activation_id"
                        ],
                        "target_release_id": intent["target_release_id"],
                        "replacement_queue_entry_sha256": None,
                        "replacement_queue_entry_path": intent[
                            "replacement_queue_entry_path"
                        ],
                        "created_at": intent["created_at"],
                        "rolled_back_at": None,
                        "formal_write_count": 0,
                    },
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-pointer"
                    ),
                )
            if (
                pointer.get("recovery_receipt_sha256") != recovery_sha256
                or pointer.get("formal_write_count") != 0
                or pointer.get("status") not in {"finalized", "rolled_back"}
            ):
                raise DispatchError(
                    "subject_batch_recovery_finalization_pointer_invalid"
                )
            queue_path = Path(
                str(pointer["replacement_queue_entry_path"])
            )
            if pointer["status"] == "rolled_back":
                if queue_path.exists():
                    raise DispatchError(
                        "subject_batch_recovery_finalization_rollback_drift"
                    )
                return {**dict(pointer), "idempotent": True, "applied": False}
            current = self._read_canary_state_locked("english")
            assert current is not None
            if (
                current.get("activation_id")
                != pointer.get("target_activation_id")
                or current.get("release_id")
                != pointer.get("target_release_id")
                or int(current.get("active_task_count") or 0) != 0
                or current.get("selected") is not None
                or bool(current.get("active_selections"))
                or current.get("formal_write_count") != 0
                or current.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "subject_batch_recovery_finalization_gate_drift"
                )
            queue = self._read_object(queue_path)
            if queue is None:
                if rollback_intent is not None:
                    self._verify_seal(
                        rollback_intent,
                        purpose=(
                            "dispatch-subject-batch-recovery-finalization-rollback"
                        ),
                    )
                    if (
                        rollback_intent.get("recovery_receipt_sha256")
                        != recovery_sha256
                        or rollback_intent.get("target_activation_id")
                        != pointer.get("target_activation_id")
                        or rollback_intent.get("target_release_id")
                        != pointer.get("target_release_id")
                        or rollback_intent.get(
                            "replacement_queue_entry_path"
                        )
                        != str(queue_path)
                        or rollback_intent.get("formal_write_count") != 0
                    ):
                        raise DispatchError(
                            "subject_batch_recovery_finalization_rollback_intent_invalid"
                        )
                    if apply:
                        core = copy.deepcopy(dict(pointer))
                        core.pop("authority", None)
                        core.update(
                            {
                                "status": "rolled_back",
                                "rolled_back_at": rollback_intent[
                                    "created_at"
                                ],
                            }
                        )
                        pointer = self._seal(
                            core,
                            purpose=(
                                "dispatch-subject-batch-recovery-finalization-pointer"
                            ),
                        )
                        _atomic_replace_json(pointer_path, pointer)
                    return {
                        **dict(pointer),
                        "idempotent": True,
                        "applied": bool(apply),
                    }
                if intent is not None and pointer.get(
                    "supersede_receipt_sha256"
                ) is None:
                    return {
                        **dict(pointer),
                        "idempotent": True,
                        "applied": False,
                    }
                raise DispatchError(
                    "subject_batch_recovery_finalization_queue_missing"
                )
            self._verify_seal(
                queue, purpose="dispatch-production-canary-queue"
            )
            lease = self._read_object(
                self._lease_path(str(queue.get("unit_sha256") or ""))
            )
            expected_queue_sha256 = pointer.get(
                "replacement_queue_entry_sha256"
            )
            if expected_queue_sha256 is None and intent is not None:
                expected_queue_sha256 = _sha256_bytes(
                    queue_path.read_bytes()
                )
                if (
                    queue.get("unit_sha256")
                    != intent.get("replacement_unit_sha256")
                    or queue.get("frozen_payload_sha256")
                    != intent.get("replacement_frozen_payload_sha256")
                    or queue.get("producer_input_contract_sha256")
                    != intent.get(
                        "replacement_producer_input_contract_sha256"
                    )
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_queue_drift"
                    )
            if (
                _sha256_bytes(queue_path.read_bytes())
                != expected_queue_sha256
                or queue.get("queue_status") != "pending"
                or queue.get("terminal_receipt_sha256") is not None
                or queue.get("lease_owner_id") is not None
                or (
                    lease is not None
                    and lease.get("status") == "claimed"
                )
            ):
                raise DispatchError(
                    "subject_batch_recovery_finalization_queue_drift"
                )
            if apply:
                rollback_core = {
                    "schema_version": (
                        "study-intake-subject-batch-recovery-finalization-rollback-v1"
                    ),
                    "subject": "english",
                    "recovery_receipt_sha256": recovery_sha256,
                    "target_activation_id": pointer[
                        "target_activation_id"
                    ],
                    "target_release_id": pointer["target_release_id"],
                    "replacement_queue_entry_sha256": (
                        expected_queue_sha256
                    ),
                    "replacement_queue_entry_path": str(queue_path),
                    "created_at": _utc_now(),
                    "formal_write_count": 0,
                }
                if rollback_intent is None:
                    (
                        _rollback_intent_sha256,
                        _rollback_intent_path,
                        rollback_intent,
                    ) = self._publish_canary_receipt_locked(
                        "english",
                        str(pointer["target_activation_id"]),
                        rollback_core,
                        purpose=(
                            "dispatch-subject-batch-recovery-finalization-rollback"
                        ),
                    )
                    _atomic_replace_json(
                        rollback_intent_path, rollback_intent
                    )
                else:
                    self._verify_seal(
                        rollback_intent,
                        purpose=(
                            "dispatch-subject-batch-recovery-finalization-rollback"
                        ),
                    )
                queue_path.unlink()
                if (
                    os.environ.get(FINALIZATION_FAILPOINT_ENV)
                    == "after_queue_unlink_before_rollback_pointer"
                ):
                    raise DispatchError(
                        "subject_batch_recovery_finalization_injected_crash"
                    )
                core = copy.deepcopy(dict(pointer))
                core.pop("authority", None)
                core.update(
                    {
                        "status": "rolled_back",
                        "rolled_back_at": rollback_intent["created_at"],
                    }
                )
                pointer = self._seal(
                    core,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-pointer"
                    ),
                )
                _atomic_replace_json(pointer_path, pointer)
            return {
                **dict(pointer),
                "idempotent": False,
                "applied": bool(apply),
            }

    def arm_finalized_production_canary_subject_batch_recovery(
        self,
        subject: str,
        *,
        recovery_receipt_sha256: str,
        expected_target_release_id: str,
    ) -> dict[str, Any]:
        """Enable the consumer only after every recovery proof reopens."""

        if subject != "english":
            raise DispatchError("subject_batch_recovery_english_only")
        recovery_sha256 = _validate_unit_sha256(
            recovery_receipt_sha256
        )
        target_release_id = _validate_unit_sha256(
            expected_target_release_id
        )
        staged_path = (
            self.production_canary_recovery_staged_activation_root
            / f"{recovery_sha256}.json"
        )
        finalization_path = (
            self.production_canary_recovery_finalization_root
            / f"{recovery_sha256}.json"
        )
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            staged = self._read_object(staged_path)
            finalization = self._read_object(finalization_path)
            if staged is None or finalization is None:
                raise DispatchError(
                    "subject_batch_recovery_finalization_missing"
                )
            self._verify_seal(
                staged,
                purpose=(
                    "dispatch-subject-batch-recovery-staged-activation"
                ),
            )
            self._verify_seal(
                finalization,
                purpose=(
                    "dispatch-subject-batch-recovery-finalization-pointer"
                ),
            )
            if (
                state.get("release_id") != target_release_id
                or state.get("activation_id")
                != staged.get("target_activation_id")
                or state.get("activation_id")
                != finalization.get("target_activation_id")
                or state.get("state") not in {"paused_drained", "armed"}
                or int(state.get("active_task_count") or 0) != 0
                or state.get("selected") is not None
                or bool(state.get("active_selections"))
                or state.get("formal_write_count") != 0
                or state.get("sol_enabled") is not False
                or staged.get("recovery_receipt_sha256") != recovery_sha256
                or staged.get("target_release_id") != target_release_id
                or staged.get("status") not in {"staged", "arming", "armed"}
                or finalization.get("recovery_receipt_sha256")
                != recovery_sha256
                or finalization.get("target_release_id")
                != target_release_id
                or finalization.get("status") != "finalized"
            ):
                raise DispatchError(
                    "subject_batch_recovery_arm_binding_invalid"
                )
            queue_path = Path(
                str(finalization["replacement_queue_entry_path"])
            )
            supersede_path = Path(
                str(finalization["supersede_receipt_path"])
            )
            queue = self._read_object(queue_path)
            supersede = self._read_object(supersede_path)
            if queue is None or supersede is None:
                raise DispatchError(
                    "subject_batch_recovery_arm_evidence_missing"
                )
            self._verify_seal(
                queue, purpose="dispatch-production-canary-queue"
            )
            self._verify_seal(
                supersede,
                purpose="dispatch-subject-batch-recovery-supersede",
            )
            if (
                _sha256_bytes(queue_path.read_bytes())
                != finalization.get("replacement_queue_entry_sha256")
                or queue.get("queue_status") != "pending"
                or queue.get("terminal_receipt_sha256") is not None
                or queue.get("activation_id") != state.get("activation_id")
                or queue.get("release_id") != target_release_id
                or supersede.get("replacement_queue_entry_sha256")
                != finalization.get("replacement_queue_entry_sha256")
                or supersede.get("recovery_receipt_sha256")
                != recovery_sha256
            ):
                raise DispatchError(
                    "subject_batch_recovery_arm_evidence_invalid"
                )
            if state.get("state") == "armed":
                if (
                    state.get("luna_consumer_enabled") is not True
                    or staged.get("status") not in {"arming", "armed"}
                ):
                    raise DispatchError(
                        "subject_batch_recovery_arm_binding_invalid"
                    )
                if staged.get("status") == "armed":
                    return {
                        "state": state,
                        "staged_activation": staged,
                        "finalization": finalization,
                        "idempotent": True,
                    }
            else:
                if (
                    state.get("luna_consumer_enabled") is not False
                    or staged.get("status") not in {"staged", "arming"}
                ):
                    raise DispatchError(
                        "subject_batch_recovery_arm_binding_invalid"
                    )
                if staged.get("status") == "staged":
                    staged_core = copy.deepcopy(dict(staged))
                    staged_core.pop("authority", None)
                    staged_core["status"] = "arming"
                    staged = self._seal(
                        staged_core,
                        purpose=(
                            "dispatch-subject-batch-recovery-staged-activation"
                        ),
                    )
                    _atomic_replace_json(staged_path, staged)
                state.update(
                    {
                        "state": "armed",
                        "luna_consumer_enabled": True,
                        "blocking_reason": None,
                        "next_action": (
                            "consume_pending_post_activation_queue"
                        ),
                        "selected": None,
                        "active_selections": {},
                        "backpressure_reason": None,
                    }
                )
                state = self._write_canary_state_locked(state)
            staged_core = copy.deepcopy(dict(staged))
            staged_core.pop("authority", None)
            staged_core.update(
                {"status": "armed", "armed_at": _utc_now()}
            )
            staged = self._seal(
                staged_core,
                purpose=(
                    "dispatch-subject-batch-recovery-staged-activation"
                ),
            )
            _atomic_replace_json(staged_path, staged)
            return {
                "state": state,
                "staged_activation": staged,
                "finalization": finalization,
                "idempotent": False,
            }

    def complete_production_canary_subject_batch_recovery_rollback(
        self, *, recovery_receipt_sha256: str
    ) -> dict[str, Any]:
        """Retire per-attempt mutable pointers after the old gate is restored."""

        recovery_sha256 = _validate_unit_sha256(
            recovery_receipt_sha256
        )
        paths = [
            self.production_canary_recovery_staged_activation_root
            / f"{recovery_sha256}.json",
            self.production_canary_recovery_finalization_root
            / f"{recovery_sha256}.json",
            self.production_canary_recovery_finalization_intent_root
            / f"{recovery_sha256}.json",
            self.production_canary_recovery_finalization_rollback_intent_root
            / f"{recovery_sha256}.json",
        ]
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked("english")
            assert state is not None
            if (
                int(state.get("active_task_count") or 0) != 0
                or state.get("luna_consumer_enabled") is not False
                or state.get("formal_write_count") != 0
                or state.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "subject_batch_recovery_rollback_cleanup_gate_invalid"
                )
            staged = self._read_object(paths[0])
            finalization = self._read_object(paths[1])
            if staged is not None:
                self._verify_seal(
                    staged,
                    purpose=(
                        "dispatch-subject-batch-recovery-staged-activation"
                    ),
                )
                if staged.get("recovery_receipt_sha256") != recovery_sha256:
                    raise DispatchError(
                        "subject_batch_recovery_rollback_cleanup_invalid"
                    )
                if state.get("activation_id") == staged.get(
                    "target_activation_id"
                ):
                    raise DispatchError(
                        "subject_batch_recovery_rollback_cleanup_gate_invalid"
                    )
            if finalization is not None:
                self._verify_seal(
                    finalization,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-pointer"
                    ),
                )
                if (
                    finalization.get("status") != "rolled_back"
                    or Path(
                        str(
                            finalization[
                                "replacement_queue_entry_path"
                            ]
                        )
                    ).exists()
                ):
                    raise DispatchError(
                        "subject_batch_recovery_rollback_cleanup_invalid"
                    )
            removed = 0
            for path in paths:
                try:
                    path.unlink()
                except FileNotFoundError:
                    continue
                removed += 1
            return {
                "schema_version": (
                    "study-intake-subject-batch-recovery-rollback-cleanup-v1"
                ),
                "subject": "english",
                "recovery_receipt_sha256": recovery_sha256,
                "removed_mutable_pointer_count": removed,
                "target_queue_withdrawn": True,
                "formal_write_count": 0,
                "sol_enabled": False,
            }

    def restore_production_canary_recovery_preimage(
        self,
        *,
        canary_state_before: Mapping[str, Any],
        expected_preserved_queue_entry_sha256: str,
        apply: bool,
    ) -> dict[str, Any]:
        """Preflight or restore the exact signed gate state captured by v2."""

        before = copy.deepcopy(dict(canary_state_before))
        subject = str(before.get("subject") or "")
        expected_queue_sha256 = _validate_unit_sha256(
            expected_preserved_queue_entry_sha256
        )
        with _ExclusiveFileLock(self.lock_path):
            self._verify_seal(
                before, purpose="dispatch-production-canary-state"
            )
            if (
                subject != "english"
                or before.get("schema_version")
                != PRODUCTION_CANARY_STATE_SCHEMA
                or before.get("state")
                not in {"failed_drained", "inactive_rolled_back"}
                or before.get("luna_consumer_enabled") is not False
                or int(before.get("active_task_count") or 0) != 0
                or before.get("formal_write_count") != 0
                or before.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "production_canary_recovery_preimage_invalid"
                )
            activation_id = _validate_unit_sha256(
                str(before.get("activation_id") or "")
            )
            queue_rows = list(
                self._production_canary_queue_subject_root(
                    subject, activation_id
                ).glob("*.json")
            )
            preserved = []
            for path in sorted(queue_rows):
                row = self._read_object(path)
                if row is None:
                    continue
                self._verify_seal(
                    row, purpose="dispatch-production-canary-queue"
                )
                if (
                    row.get("queue_status") == "pending"
                    and _sha256_bytes(path.read_bytes())
                    == expected_queue_sha256
                ):
                    preserved.append(dict(row))
            if len(preserved) != 1:
                raise DispatchError(
                    "production_canary_preserved_queue_postimage_mismatch"
                )
            current = self._read_canary_state_locked(subject)
            assert current is not None
            if current == before:
                return {
                    "subject": subject,
                    "status": "already_restored",
                    "canary_state_sha256": _sha256_bytes(
                        _canonical_bytes(before)
                    ),
                    "preserved_queue_entry_sha256": expected_queue_sha256,
                    "applied": False,
                }
            if (
                int(current.get("active_task_count") or 0) != 0
                or current.get("selected") is not None
                or bool(current.get("active_selections"))
                or current.get("formal_write_count") != 0
                or current.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "production_canary_recovery_gate_postimage_mismatch"
                )
            if apply:
                _atomic_replace_json(
                    self._production_canary_state_path(subject), before
                )
                restored = self._read_canary_state_locked(subject)
                if restored != before:
                    raise DispatchError(
                        "production_canary_recovery_gate_restore_failed"
                    )
            return {
                "subject": subject,
                "status": "restored" if apply else "preflight_ready",
                "canary_state_sha256": _sha256_bytes(
                    _canonical_bytes(before)
                ),
                "preserved_queue_entry_sha256": expected_queue_sha256,
                "applied": bool(apply),
            }

    def fail_production_canary_preclaim(
        self,
        subject: str,
        task: FrozenTask | None,
        *,
        failure_stage: str,
        error_code: str,
    ) -> dict[str, Any]:
        """Publish receipt first, then atomically close the subject state."""

        if failure_stage not in PRODUCTION_CANARY_PRECLAIM_FAILURE_STAGES:
            raise DispatchError("production_canary_preclaim_stage_invalid")
        if not isinstance(error_code, str) or not error_code:
            raise DispatchError("production_canary_preclaim_error_invalid")
        timestamp = _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            state = self._reconcile_production_canary_preclaim_failures_locked(
                state
            )
            if state.get("state") == "inactive_rolled_back":
                raise DispatchError("production_canary_inactive")

            queue_entry: dict[str, Any] | None = None
            if task is not None:
                provisional_evidence = self._preclaim_failure_evidence_locked(
                    task, None, None
                )
                queue_entry = self._preclaim_queue_entry_locked(
                    subject, provisional_evidence
                )
            lease: dict[str, Any] | None = None
            if task is not None:
                candidate_lease = self._read_object(
                    self._lease_path(task.unit_sha256)
                )
                if (
                    isinstance(candidate_lease, Mapping)
                    and candidate_lease.get("status") == "claimed"
                    and candidate_lease.get("subject") == subject
                ):
                    lease = dict(candidate_lease)
            evidence = self._preclaim_failure_evidence_locked(
                task, queue_entry, lease
            )
            failure_id = _sha256_bytes(
                _canonical_bytes(
                    {
                        "activation_id": state["activation_id"],
                        "subject": subject,
                        "failure_stage": failure_stage,
                        "error_code": error_code,
                        "evidence_sha256": evidence["evidence_sha256"],
                    }
                )
            )
            queue_preserved = queue_entry is not None
            receipt_core = {
                "schema_version": PRODUCTION_CANARY_PRECLAIM_FAILURE_SCHEMA,
                "failure_id": failure_id,
                "activation_id": state["activation_id"],
                "subject": subject,
                "release_id": state["release_id"],
                "producer_authority_fingerprint": state[
                    "producer_authority_fingerprint"
                ],
                "producer_high_watermark_sha256": state[
                    "producer_high_watermark_sha256"
                ],
                "activation_receipt_sha256": state[
                    "activation_receipt_sha256"
                ],
                "activation_gate_authority_sha256": state[
                    "activation_gate_authority_sha256"
                ],
                "failure_stage": failure_stage,
                "error_code": error_code,
                "failure_evidence": evidence,
                "queue_entry_preserved": queue_preserved,
                "queue_status_after": "pending" if queue_preserved else None,
                "model_submission_started": False,
                "failed_at": timestamp,
                "state_after": "failed_drained",
                "producer_capture_enabled": True,
                "luna_consumer_enabled": False,
                **self._canary_control_fields(
                    int(state["continuous_concurrency_limit"])
                ),
            }
            receipt = self._seal(
                receipt_core,
                purpose="dispatch-production-canary-preclaim-failure",
            )
            failure_index_path = self._production_canary_preclaim_failure_path(
                subject,
                str(state["activation_id"]),
                unit_sha256=(task.unit_sha256 if task is not None else None),
                frozen_payload_sha256=(
                    task.frozen_payload_sha256 if task is not None else None
                ),
            )
            # Phase 1: durable HMAC intent/receipt at a deterministic path.
            # A restarted daemon reconciles this before scanning or claiming.
            _atomic_replace_json(failure_index_path, receipt)
            # Phase 2: publish immutable content-addressed bytes, then swap the
            # state.  State never references a receipt that does not exist.
            receipt_sha256, receipt_path = (
                self._publish_preclaim_receipt_locked(
                    subject, str(state["activation_id"]), receipt
                )
            )
            committed = self._commit_production_canary_preclaim_failure_locked(
                state, receipt, receipt_sha256, receipt_path
            )
            return {
                "failure_id": failure_id,
                "preclaim_failure_receipt_sha256": receipt_sha256,
                "preclaim_failure_receipt_path": str(receipt_path),
                "failure_stage": failure_stage,
                "error_code": error_code,
                "state": committed["state"],
                "queue_entry_preserved": queue_preserved,
                "queue_status_after": "pending" if queue_preserved else None,
            }

    def _queue_entries_locked(self, subject: str) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        state = self._read_object(self._production_canary_state_path(subject))
        if state is None:
            raise DispatchError("production_canary_not_active")
        activation_id = str(state.get("activation_id") or "")
        _validate_unit_sha256(activation_id)
        for path in sorted(
            self._production_canary_queue_subject_root(
                subject, activation_id
            ).glob("*.json")
        ):
            value = self._read_object(path)
            if value is None:
                continue
            self._verify_seal(value, purpose="dispatch-production-canary-queue")
            if (
                value.get("schema_version")
                not in KNOWN_PRODUCTION_CANARY_QUEUE_SCHEMAS
                or value.get("subject") != subject
                or value.get("activation_id") != activation_id
                or value.get("formal_write_count") != 0
            ):
                raise DispatchError("production_canary_queue_invalid")
            entries.append(dict(value))
        return entries

    def _task_from_canary_queue_entry_locked(
        self, queue_entry: Mapping[str, Any]
    ) -> FrozenTask:
        task_sha256 = str(queue_entry.get("task_object_sha256") or "")
        _validate_unit_sha256(task_sha256)
        task_path = Path(str(queue_entry.get("task_object_path") or ""))
        task_root = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "tasks"
        ).resolve()
        try:
            task_path.resolve(strict=True).relative_to(task_root)
            task_bytes = task_path.read_bytes()
            task_value = json.loads(task_bytes.decode("utf-8"))
        except (OSError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError("production_canary_task_object_invalid") from exc
        if (
            _sha256_bytes(task_bytes) != task_sha256
            or not isinstance(task_value, Mapping)
        ):
            raise DispatchError("production_canary_task_object_invalid")
        task = FrozenTask.from_mapping(task_value)
        if (
            task.unit_sha256 != queue_entry.get("unit_sha256")
            or task.frozen_payload_sha256
            != queue_entry.get("frozen_payload_sha256")
        ):
            raise DispatchError("production_canary_task_object_invalid")
        return task

    def _refresh_canary_queue_projection_locked(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        updated = dict(state)
        entries = self._queue_entries_locked(str(state["subject"]))
        dispatcher_entries = [
            row
            for row in entries
            if row.get("queue_status")
            in {"pending", "claimed", "succeeded", "failed"}
        ]
        superseded = self._quick_flush_supersede_statuses_locked(state)
        pending = [
            row
            for row in entries
            if row.get("queue_status") == "pending"
            and row.get("producer_input_contract_sha256") not in superseded
        ]
        excluded = self._production_canary_exclusions_locked(state)
        updated["queue_depth"] = len(pending)
        updated["historical_eligible_count"] = len(excluded)
        updated["excluded_by_high_watermark_count"] = len(excluded)
        updated["canary_queue_count"] = len(dispatcher_entries)
        updated["queue_classification"] = {
            "pre_activation_frozen": len(excluded),
            **{
                status: sum(
                    row.get("queue_status") == status
                    for row in dispatcher_entries
                    if not (
                        status == "pending"
                        and row.get("producer_input_contract_sha256")
                        in superseded
                    )
                )
                for status in ("pending", "claimed", "succeeded", "failed")
            },
        }
        if pending:
            oldest = min(
                _parse_utc(row.get("producer_recorded_at")) for row in pending
            )
            updated["oldest_pending_age_seconds"] = max(
                0,
                int((dt.datetime.now(dt.timezone.utc) - oldest).total_seconds()),
            )
        else:
            updated["oldest_pending_age_seconds"] = None
        return updated

    @staticmethod
    def _canary_projection_changed(
        state: Mapping[str, Any], refreshed: Mapping[str, Any]
    ) -> bool:
        return any(
            state.get(field) != refreshed.get(field)
            for field in (
                "queue_depth",
                "oldest_pending_age_seconds",
                "historical_eligible_count",
                "excluded_by_high_watermark_count",
                "canary_queue_count",
                "queue_classification",
            )
        )

    def _materialize_canary_queue_postimage_locked(
        self,
        *,
        task: FrozenTask,
        state: Mapping[str, Any],
        contract: Mapping[str, Any],
    ) -> tuple[dict[str, Any], Path, str, Path, str, bool]:
        """Write one already-admitted task and its ordinary pending queue row."""

        subject = str(state.get("subject") or "")
        contract_sha256 = _validate_unit_sha256(
            str(contract.get("producer_input_contract_sha256") or "")
        )
        queue_path = self._production_canary_queue_path(
            subject, contract_sha256
        )
        if queue_path.exists():
            raise DispatchError("production_canary_queue_binding_conflict")
        task_payload = _canonical_bytes(task.as_dict()) + b"\n"
        expected_task_sha256 = _sha256_bytes(task_payload)
        expected_task_path = (
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "tasks"
            / _safe_component(subject)
            / "sha256"
            / expected_task_sha256[:2]
            / f"{expected_task_sha256}.json"
        )
        task_preexisted = expected_task_path.exists()
        task_sha256, task_path = _publish_content_addressed(
            self.runtime_root
            / "dispatch"
            / "production-canary"
            / "tasks"
            / _safe_component(subject),
            task.as_dict(),
        )
        if (
            task_sha256 != expected_task_sha256
            or task_path != expected_task_path
        ):
            raise DispatchError("production_canary_task_object_invalid")
        queue_core = {
            "schema_version": PRODUCTION_CANARY_QUEUE_SCHEMA,
            "activation_id": state["activation_id"],
            "subject": subject,
            "release_id": state["release_id"],
            "producer_high_watermark_sha256": state[
                "producer_high_watermark_sha256"
            ],
            "producer_authority_fingerprint": state[
                "producer_authority_fingerprint"
            ],
            "producer_input_contract_sha256": contract_sha256,
            "producer_unit_id": contract["producer_unit_id"],
            "producer_recorded_at": contract["producer_recorded_at"],
            "source_event_set_sha256": contract["source_event_set_sha256"],
            "source_event_ids": [
                row["event_id"] for row in contract["source_events"]
            ],
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "task_object_sha256": task_sha256,
            "task_object_path": str(task_path),
            "queue_status": "pending",
            "discovered_at": _utc_now(),
            "claimed_at": None,
            "finished_at": None,
            "canary_gate_sha256": None,
            "canary_gate_path": None,
            "canary_gate_authority_sha256": None,
            "lease_owner_id": None,
            "lease_fence": None,
            "context_root": None,
            "mcp_session_root": None,
            "report_root": None,
            "process_identity_sha256": None,
            "process_identity_path": None,
            "terminal_receipt_sha256": None,
            "terminal_receipt_path": None,
            "terminal_outcome": None,
            "terminal_error_code": None,
            "formal_write_count": 0,
        }
        queue_entry = self._seal(
            queue_core, purpose="dispatch-production-canary-queue"
        )
        _atomic_replace_json(queue_path, queue_entry)
        queue_sha256 = _sha256_bytes(queue_path.read_bytes())
        return (
            queue_entry,
            queue_path,
            queue_sha256,
            task_path,
            task_sha256,
            task_preexisted,
        )

    def materialize_production_canary_task(
        self, task: FrozenTask
    ) -> dict[str, Any]:
        subject = task.frozen_payload.get("subject")
        if subject not in {"math", "cs408", "english"}:
            raise DispatchError("production_canary_subject_invalid")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(str(subject))
            assert state is not None
            if state.get("state") == "inactive_rolled_back":
                raise DispatchError("production_canary_inactive")
            contract = self._validate_canary_task_locked(task, state)
            contract_sha256 = str(contract["producer_input_contract_sha256"])
            path = self._production_canary_queue_path(str(subject), contract_sha256)
            try:
                exact_math_binding = verify_exact_task_binding(
                    runtime_root=self.runtime_root,
                    task_payload=task.frozen_payload,
                    unit_sha256=task.unit_sha256,
                    state=state,
                )
            except MathExactSmokeError as exc:
                raise DispatchError(exc.code) from exc
            if exact_math_binding is not None:
                capture_event_id = str(
                    exact_math_binding["capture_event_id"]
                )
                for historical_path in sorted(
                    self._production_canary_queue_subject_root("math")
                    .parent.glob("*/*.json")
                ):
                    historical = self._read_object(historical_path)
                    if historical is None:
                        continue
                    self._verify_seal(
                        historical,
                        purpose="dispatch-production-canary-queue",
                    )
                    if capture_event_id not in set(
                        historical.get("source_event_ids") or []
                    ):
                        continue
                    same_current_queue = (
                        historical_path == path
                        and historical.get("unit_sha256")
                        == task.unit_sha256
                        and historical.get("frozen_payload_sha256")
                        == task.frozen_payload_sha256
                    )
                    if same_current_queue:
                        continue
                    if historical.get("queue_status") in {
                        "pending",
                        "claimed",
                    }:
                        raise DispatchError(
                            "math_smoke_processing_attempt_already_active"
                        )
                    if (
                        historical.get("queue_status") == "succeeded"
                        or historical.get("terminal_outcome")
                        in {"succeeded", "needs_rework"}
                    ):
                        raise DispatchError(
                            "math_smoke_accepted_package_already_exists"
                        )
                    raise DispatchError(
                        "math_smoke_followup_attempt_not_authorized"
                    )
            existing = self._read_object(path)
            if existing is not None:
                self._verify_seal(
                    existing, purpose="dispatch-production-canary-queue"
                )
                if (
                    existing.get("activation_id") != state.get("activation_id")
                    or existing.get("release_id") != state.get("release_id")
                    or existing.get("subject") != subject
                ):
                    raise DispatchError("production_canary_queue_binding_conflict")
                if (
                    subject == "english"
                    and contract_sha256
                    in self._quick_flush_supersede_statuses_locked(state)
                ):
                    result = dict(existing)
                    result["materialization_status"] = (
                        "quick_flush_source_superseded"
                    )
                    return result
                same_task = (
                    existing.get("unit_sha256") == task.unit_sha256
                    and existing.get("frozen_payload_sha256")
                    == task.frozen_payload_sha256
                )
                if not same_task:
                    if (
                        subject == "english"
                        and existing.get("queue_status") == "pending"
                        and isinstance(
                            existing.get("terminal_receipt_sha256"), str
                        )
                    ):
                        receipts = self._preclaim_failure_receipts_locked(
                            str(subject), str(state["activation_id"])
                        )
                        canonical = (
                            self._canonical_preserved_preclaim_failure_locked(
                                state, receipts
                            )
                        )
                        if (
                            canonical is not None
                            and canonical[2].get(
                                "producer_input_contract_sha256"
                            )
                            == existing.get(
                                "producer_input_contract_sha256"
                            )
                        ):
                            result = dict(existing)
                            result["materialization_status"] = (
                                "already_represented_by_queue"
                            )
                            return result
                    raise DispatchError("deterministic_producer_drift")
                result = dict(existing)
                if (
                    subject == "english"
                    and existing.get("queue_status") == "pending"
                    and isinstance(
                        existing.get("terminal_receipt_sha256"), str
                    )
                ):
                    receipts = self._preclaim_failure_receipts_locked(
                        str(subject), str(state["activation_id"])
                    )
                    canonical = (
                        self._canonical_preserved_preclaim_failure_locked(
                            state, receipts
                        )
                    )
                    if (
                        canonical is not None
                        and canonical[2].get(
                            "producer_input_contract_sha256"
                        )
                        == existing.get("producer_input_contract_sha256")
                    ):
                        result["materialization_status"] = (
                            "already_represented_by_queue"
                        )
                return result
            event_ids = {
                str(row["event_id"]) for row in contract["source_events"]
            }
            canonical_preserved_contract_sha256: str | None = None
            if subject == "english":
                receipts = self._preclaim_failure_receipts_locked(
                    str(subject), str(state["activation_id"])
                )
                canonical = self._canonical_preserved_preclaim_failure_locked(
                    state, receipts
                )
                if canonical is not None:
                    canonical_preserved_contract_sha256 = str(
                        canonical[2]["producer_input_contract_sha256"]
                    )
            for row in self._queue_entries_locked(str(subject)):
                previous_ids = set(row.get("source_event_ids") or [])
                collision = (
                    row.get("producer_unit_id")
                    == contract.get("producer_unit_id")
                    or row.get("source_event_set_sha256")
                    == contract.get("source_event_set_sha256")
                    or bool(event_ids & previous_ids)
                )
                if not collision:
                    continue
                exact_source = (
                    row.get("producer_unit_id")
                    == contract.get("producer_unit_id")
                    and row.get("source_event_set_sha256")
                    == contract.get("source_event_set_sha256")
                    and event_ids == previous_ids
                )
                if (
                    subject == "english"
                    and exact_source
                    and row.get("queue_status") == "pending"
                    and row.get("producer_input_contract_sha256")
                    == canonical_preserved_contract_sha256
                ):
                    result = dict(row)
                    result["materialization_status"] = (
                        "already_represented_by_queue"
                    )
                    return result
                if subject == "english":
                    raise DispatchError("deterministic_producer_drift")
                raise DispatchError("production_canary_producer_replay")
            queue_entry, _queue_path, _queue_sha, _task_path, _task_sha, _ = (
                self._materialize_canary_queue_postimage_locked(
                    task=task,
                    state=state,
                    contract=contract,
                )
            )
            refreshed = self._refresh_canary_queue_projection_locked(state)
            if refreshed.get("state") == "failed_drained":
                refreshed["next_action"] = "explicit_subject_resume_required"
            elif refreshed.get("state") in {
                "armed", "continuous_concurrent_unlocked"
            }:
                refreshed["next_action"] = "consume_pending_post_activation_queue"
            self._write_canary_state_locked(refreshed)
            return queue_entry

    def materialize_exact_math_migration(
        self,
        *,
        tasks: Sequence[FrozenTask],
        migration_descriptor: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Atomically stage the fixed four-task Math authority migration."""

        if len(tasks) != 4 or any(
            not isinstance(task, FrozenTask) for task in tasks
        ):
            raise DispatchError("math_pending_queue_migration_task_set_invalid")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked("math")
            assert state is not None
            if (
                state.get("state") != "paused_drained"
                or state.get("luna_consumer_enabled") is not False
                or state.get("active_task_count") != 0
                or state.get("active_selections") != {}
                or state.get("selected") is not None
                or self._queue_entries_locked("math")
            ):
                raise DispatchError(
                    "math_pending_queue_migration_target_not_quiescent"
                )
            try:
                from math_exact_smoke import (
                    validate_exact_math_migration_materialization,
                )

                validated = validate_exact_math_migration_materialization(
                    self.runtime_root,
                    tasks,
                    state,
                    migration_descriptor,
                )
            except MathExactSmokeError as exc:
                raise DispatchError(exc.code) from exc
            if not isinstance(validated, Mapping):
                raise DispatchError(
                    "math_pending_queue_migration_descriptor_invalid"
                )
            descriptor_sha256 = _validate_unit_sha256(
                str(validated.get("migration_descriptor_sha256") or "")
            )
            gs269_archived_evidence_sha256 = _validate_unit_sha256(
                str(validated.get("gs269_archived_evidence_sha256") or "")
            )
            source_mappings = validated.get("source_queue_mappings")
            if (
                validated.get("target_release_id") != state.get("release_id")
                or validated.get("target_activation_id")
                != state.get("activation_id")
                or validated.get("target_producer_authority_fingerprint")
                != state.get("producer_authority_fingerprint")
                or not isinstance(source_mappings, list)
                or len(source_mappings) != 4
            ):
                raise DispatchError(
                    "math_pending_queue_migration_target_binding_invalid"
                )

            state_path = self._production_canary_state_path("math")
            try:
                state_preimage = state_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "math_pending_queue_migration_state_invalid"
                ) from exc
            created_queue_paths: list[Path] = []
            created_task_paths: list[Path] = []
            rows: list[dict[str, Any]] = []
            try:
                for task, source_mapping in zip(tasks, source_mappings):
                    if not isinstance(source_mapping, Mapping):
                        raise DispatchError(
                            "math_pending_queue_migration_mapping_invalid"
                        )
                    binding = task.frozen_payload.get(
                        "math_exact_smoke_binding"
                    )
                    if (
                        task.frozen_payload.get("subject") != "math"
                        or not isinstance(binding, Mapping)
                        or binding.get("migration_descriptor_sha256")
                        != descriptor_sha256
                        or binding.get("formal_id")
                        != source_mapping.get("formal_id")
                        or binding.get("capture_event_id")
                        != source_mapping.get("capture_event_id")
                        or binding.get("source_queue_sha256")
                        != source_mapping.get("source_queue_sha256")
                        or binding.get("source_task_object_sha256")
                        != source_mapping.get("source_task_object_sha256")
                        or binding.get("gs269_archived_evidence_sha256")
                        != gs269_archived_evidence_sha256
                        or binding.get("processing_attempt_number") != 1
                    ):
                        raise DispatchError(
                            "math_pending_queue_migration_task_binding_invalid"
                        )
                    contract = self._producer_contract_from_task(task)
                    if (
                        contract.get("subject") != "math"
                        or contract.get("release_id") != state.get("release_id")
                        or contract.get("authority_fingerprint")
                        != state.get("producer_authority_fingerprint")
                    ):
                        raise DispatchError(
                            "math_pending_queue_migration_producer_binding_invalid"
                        )
                    dispatch_contract = task.frozen_payload.get(
                        "dispatch_contract"
                    )
                    if (
                        not isinstance(dispatch_contract, Mapping)
                        or "requested_service_tier" not in dispatch_contract
                        or dispatch_contract.get("requested_service_tier")
                        is not None
                        or dispatch_contract.get("fast_mode_requested") is not False
                        or dispatch_contract.get("fast_mode_effective")
                        != "not_requested"
                    ):
                        raise DispatchError(
                            "production_canary_fast_mode_binding_invalid"
                        )
                    planned_queue_path = self._production_canary_queue_path(
                        "math",
                        str(contract["producer_input_contract_sha256"]),
                    )
                    task_payload = _canonical_bytes(task.as_dict()) + b"\n"
                    planned_task_sha256 = _sha256_bytes(task_payload)
                    planned_task_path = (
                        self.runtime_root
                        / "dispatch"
                        / "production-canary"
                        / "tasks"
                        / "math"
                        / "sha256"
                        / planned_task_sha256[:2]
                        / f"{planned_task_sha256}.json"
                    )
                    if planned_queue_path.exists():
                        raise DispatchError(
                            "production_canary_queue_binding_conflict"
                        )
                    created_queue_paths.append(planned_queue_path)
                    planned_task_preexisted = planned_task_path.exists()
                    if not planned_task_preexisted:
                        created_task_paths.append(planned_task_path)
                    (
                        queue_entry,
                        queue_path,
                        queue_sha256,
                        task_path,
                        task_sha256,
                        task_preexisted,
                    ) = self._materialize_canary_queue_postimage_locked(
                        task=task,
                        state=state,
                        contract=contract,
                    )
                    if (
                        queue_path != planned_queue_path
                        or task_path != planned_task_path
                        or task_sha256 != planned_task_sha256
                        or task_preexisted != planned_task_preexisted
                    ):
                        raise DispatchError(
                            "math_pending_queue_migration_postimage_invalid"
                        )
                    rows.append(
                        {
                            **copy.deepcopy(queue_entry),
                            "migration_descriptor_sha256": descriptor_sha256,
                            "queue_entry_path": str(queue_path),
                            "queue_entry_sha256": queue_sha256,
                            "task_object_path": str(task_path),
                            "task_object_sha256": task_sha256,
                            "task_object_preexisted": task_preexisted,
                        }
                    )
                refreshed = self._refresh_canary_queue_projection_locked(state)
                if refreshed.get("queue_depth") != 4:
                    raise DispatchError(
                        "math_pending_queue_migration_queue_count_invalid"
                    )
                # The immutable GS-269 quality-success archive is the exact
                # first-task proof that permits this paused target gate to
                # resume at its existing continuous concurrency limit.
                refreshed["unlocked_once"] = True
                refreshed["next_action"] = "await_exact_math_migration_commit"
                self._write_canary_state_locked(refreshed)
                return rows
            except BaseException:
                for queue_path in reversed(created_queue_paths):
                    try:
                        queue_path.unlink()
                    except FileNotFoundError:
                        pass
                for task_path in reversed(created_task_paths):
                    try:
                        task_path.unlink()
                    except FileNotFoundError:
                        pass
                _atomic_replace_bytes(state_path, state_preimage)
                raise

    def rollback_exact_math_migration_materialization(
        self,
        *,
        migration_descriptor_sha256: str,
        queue_rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Remove only an unclaimed four-row Math migration postimage."""

        descriptor_sha256 = _validate_unit_sha256(
            migration_descriptor_sha256
        )
        if len(queue_rows) != 4:
            raise DispatchError("math_pending_queue_migration_task_set_invalid")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked("math")
            assert state is not None
            if (
                state.get("state") != "paused_drained"
                or state.get("luna_consumer_enabled") is not False
                or state.get("active_task_count") != 0
                or state.get("active_selections") != {}
            ):
                raise DispatchError(
                    "math_pending_queue_migration_rollback_not_quiescent"
                )
            checked: list[tuple[Path, Path, bool]] = []
            for row in queue_rows:
                if (
                    not isinstance(row, Mapping)
                    or row.get("migration_descriptor_sha256")
                    != descriptor_sha256
                    or row.get("subject") != "math"
                    or row.get("release_id") != state.get("release_id")
                    or row.get("activation_id") != state.get("activation_id")
                    or row.get("queue_status") != "pending"
                    or row.get("claimed_at") is not None
                    or row.get("terminal_receipt_sha256") is not None
                    or row.get("terminal_outcome") is not None
                ):
                    raise DispatchError(
                        "math_pending_queue_migration_rollback_binding_invalid"
                    )
                queue_path = Path(str(row.get("queue_entry_path") or ""))
                task_path = Path(str(row.get("task_object_path") or ""))
                queue_sha256 = _validate_unit_sha256(
                    str(row.get("queue_entry_sha256") or "")
                )
                task_sha256 = _validate_unit_sha256(
                    str(row.get("task_object_sha256") or "")
                )
                try:
                    queue_path.resolve(strict=True).relative_to(
                        self.production_canary_queue_root.resolve()
                    )
                    task_path.resolve(strict=True).relative_to(
                        (
                            self.runtime_root
                            / "dispatch"
                            / "production-canary"
                            / "tasks"
                        ).resolve()
                    )
                    queue_bytes = queue_path.read_bytes()
                    task_bytes = task_path.read_bytes()
                except (OSError, ValueError) as exc:
                    raise DispatchError(
                        "math_pending_queue_migration_rollback_binding_invalid"
                    ) from exc
                stored_queue = self._read_object(queue_path)
                if (
                    _sha256_bytes(queue_bytes) != queue_sha256
                    or _sha256_bytes(task_bytes) != task_sha256
                    or not isinstance(stored_queue, Mapping)
                    or any(
                        stored_queue.get(key) != value
                        for key, value in row.items()
                        if key
                        not in {
                            "migration_descriptor_sha256",
                            "queue_entry_path",
                            "queue_entry_sha256",
                            "task_object_preexisted",
                        }
                    )
                ):
                    raise DispatchError(
                        "math_pending_queue_migration_rollback_binding_invalid"
                    )
                self._verify_seal(
                    stored_queue,
                    purpose="dispatch-production-canary-queue",
                )
                checked.append(
                    (
                        queue_path,
                        task_path,
                        bool(row.get("task_object_preexisted")),
                    )
                )
            if len({queue_path for queue_path, _, _ in checked}) != 4:
                raise DispatchError(
                    "math_pending_queue_migration_rollback_binding_invalid"
                )
            for queue_path, _task_path, _preexisted in checked:
                queue_path.unlink()
            removed_tasks = 0
            for _queue_path, task_path, preexisted in checked:
                if preexisted or not task_path.exists():
                    continue
                if any(
                    task_path == Path(str(queue.get("task_object_path") or ""))
                    for queue in self._queue_entries_locked("math")
                ):
                    continue
                task_path.unlink()
                removed_tasks += 1
            refreshed = self._refresh_canary_queue_projection_locked(state)
            if refreshed.get("queue_depth") != 0:
                raise DispatchError(
                    "math_pending_queue_migration_rollback_incomplete"
                )
            refreshed["next_action"] = "await_exact_math_migration_retry"
            self._write_canary_state_locked(refreshed)
            return {
                "status": "rolled_back",
                "migration_descriptor_sha256": descriptor_sha256,
                "removed_queue_count": 4,
                "removed_task_object_count": removed_tasks,
                "source_mutation_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }

    def supersede_stale_english_quick_flush_task(
        self,
        task: FrozenTask,
        *,
        supersession_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Seal a non-business terminal for one corrected queued intent."""

        timestamp = _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked("english")
            assert state is not None
            if (
                state.get("state")
                not in {"armed", "continuous_concurrent_unlocked"}
                or state.get("luna_consumer_enabled") is not True
                or state.get("formal_write_count") != 0
                or state.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "english_quick_flush_supersede_gate_invalid"
                )
            contract = self._validate_canary_task_locked(task, state)
            contract_sha256 = str(
                contract["producer_input_contract_sha256"]
            )
            evidence = self._validate_quick_flush_supersession_evidence_locked(
                task, supersession_evidence
            )
            status_path = self._quick_flush_supersede_status_path(
                state, contract_sha256
            )
            existing = self._read_object(status_path)
            if existing is not None:
                statuses = self._quick_flush_supersede_statuses_locked(state)
                if contract_sha256 not in statuses:
                    raise DispatchError(
                        "english_quick_flush_supersede_status_invalid"
                    )
                return {
                    "status": "superseded",
                    "producer_input_contract_sha256": contract_sha256,
                    "terminal_receipt_sha256": existing[
                        "terminal_receipt_sha256"
                    ],
                    "terminal_receipt_path": existing[
                        "terminal_receipt_path"
                    ],
                    "idempotent": True,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "mcp_tool_call_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
            queue_path = self._production_canary_queue_path(
                "english", contract_sha256
            )
            queue = self._read_object(queue_path)
            try:
                queue_bytes = queue_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "english_quick_flush_supersede_queue_invalid"
                ) from exc
            if queue is None:
                raise DispatchError(
                    "english_quick_flush_supersede_queue_invalid"
                )
            self._verify_seal(
                queue, purpose="dispatch-production-canary-queue"
            )
            queue_task = self._task_from_canary_queue_entry_locked(queue)
            if (
                queue.get("activation_id") != state.get("activation_id")
                or queue.get("release_id") != state.get("release_id")
                or queue.get("subject") != "english"
                or queue.get("queue_status") != "pending"
                or queue.get("producer_input_contract_sha256")
                != contract_sha256
                or queue.get("unit_sha256") != task.unit_sha256
                or queue.get("frozen_payload_sha256")
                != task.frozen_payload_sha256
                or queue.get("terminal_receipt_sha256") is not None
                or queue_task.as_dict() != task.as_dict()
            ):
                raise DispatchError(
                    "english_quick_flush_supersede_queue_invalid"
                )
            queue_sha256 = _sha256_bytes(queue_bytes)
            receipt_core = {
                "schema_version": (
                    ENGLISH_QUICK_FLUSH_SUPERSEDE_TERMINAL_SCHEMA
                ),
                "subject": "english",
                "release_id": state["release_id"],
                "activation_id": state["activation_id"],
                "producer_input_contract_sha256": contract_sha256,
                "unit_sha256": task.unit_sha256,
                "frozen_payload_sha256": task.frozen_payload_sha256,
                "task_object_sha256": queue["task_object_sha256"],
                "queue_entry_sha256": queue_sha256,
                "queue_entry_path": str(queue_path),
                "supersession_evidence": evidence,
                "terminal_outcome": "superseded",
                "terminal_error_code": (
                    "english_quick_flush_source_superseded"
                ),
                "terminal_at": timestamp,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            (
                receipt_sha256,
                receipt_path,
                _receipt,
            ) = self._publish_canary_receipt_locked(
                "english",
                str(state["activation_id"]),
                receipt_core,
                purpose=(
                    "dispatch-english-quick-flush-supersede-terminal"
                ),
            )
            status = self._seal(
                {
                    "schema_version": (
                        ENGLISH_QUICK_FLUSH_SUPERSEDE_STATUS_SCHEMA
                    ),
                    "status": "superseded",
                    "subject": "english",
                    "release_id": state["release_id"],
                    "activation_id": state["activation_id"],
                    "producer_input_contract_sha256": contract_sha256,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "queue_entry_sha256": queue_sha256,
                    "queue_entry_path": str(queue_path),
                    "terminal_receipt_sha256": receipt_sha256,
                    "terminal_receipt_path": str(receipt_path),
                    "terminal_outcome": "superseded",
                    "model_enqueue_allowed": False,
                    "updated_at": timestamp,
                    "formal_write_count": 0,
                },
                purpose="dispatch-english-quick-flush-supersede-status",
            )
            _atomic_replace_json(status_path, status)
            statuses = self._quick_flush_supersede_statuses_locked(state)
            if contract_sha256 not in statuses:
                raise DispatchError(
                    "english_quick_flush_supersede_status_invalid"
                )
            refreshed = self._refresh_canary_queue_projection_locked(state)
            if self._canary_projection_changed(state, refreshed):
                self._write_canary_state_locked(refreshed)
            return {
                "status": "superseded",
                "producer_input_contract_sha256": contract_sha256,
                "terminal_receipt_sha256": receipt_sha256,
                "terminal_receipt_path": str(receipt_path),
                "idempotent": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }

    def record_production_canary_pre_activation_exclusion(
        self, task: FrozenTask
    ) -> dict[str, Any]:
        subject = str(task.frozen_payload.get("subject") or "")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if state.get("state") == "inactive_rolled_back":
                raise DispatchError("production_canary_inactive")
            contract = self._producer_contract_from_task(task)
            producer_is_pre_activation = _parse_utc(
                contract.get("producer_recorded_at")
            ) <= _parse_utc(state.get("activated_at"))
            group_contains_pre_activation = (
                contract.get("subject") != "english"
                and any(
                    _parse_utc(row.get("recorded_at"))
                    <= _parse_utc(state.get("activated_at"))
                    for row in contract["source_events"]
                )
            )
            if (
                contract.get("subject") != subject
                or contract.get("release_id") != state.get("release_id")
                or contract.get("authority_fingerprint")
                != state.get("producer_authority_fingerprint")
                or not (
                    producer_is_pre_activation
                    or group_contains_pre_activation
                )
            ):
                raise DispatchError(
                    "production_canary_pre_activation_exclusion_invalid"
                )
            contract_sha256 = str(contract["producer_input_contract_sha256"])
            exclusion_root = self._production_canary_exclusion_activation_root(
                subject, str(state["activation_id"])
            )

            def reconcile_existing(
                receipt: Mapping[str, Any], receipt_path: Path
            ) -> dict[str, Any]:
                if (
                    receipt.get("activation_id") != state.get("activation_id")
                    or receipt.get("subject") != subject
                    or receipt.get("release_id") != state.get("release_id")
                    or receipt.get("classification")
                    != "pre_activation_frozen"
                    or receipt.get("producer_input_contract_sha256")
                    != contract_sha256
                    or receipt.get("frozen_payload_sha256")
                    != task.frozen_payload_sha256
                    or receipt_path.parent != exclusion_root
                ):
                    raise DispatchError(
                        "production_canary_exclusion_binding_conflict"
                    )
                refreshed = self._refresh_canary_queue_projection_locked(state)
                if self._canary_projection_changed(state, refreshed):
                    self._write_canary_state_locked(refreshed)
                return dict(receipt)

            path = exclusion_root / f"{contract_sha256}.json"
            existing = self._read_object(path)
            if existing is not None:
                self._verify_seal(
                    existing, purpose="dispatch-production-canary-exclusion"
                )
                return reconcile_existing(existing, path)
            prior = self._production_canary_exclusions_locked(state).get(
                str(contract["producer_unit_id"])
            )
            if prior is not None:
                refreshed = self._refresh_canary_queue_projection_locked(state)
                if self._canary_projection_changed(state, refreshed):
                    self._write_canary_state_locked(refreshed)
                return dict(prior)
            exclusion = self._seal(
                {
                    "schema_version": (
                        "study-intake-production-canary-exclusion-v1"
                    ),
                    "activation_id": state["activation_id"],
                    "subject": subject,
                    "release_id": state["release_id"],
                    "producer_high_watermark_sha256": state[
                        "producer_high_watermark_sha256"
                    ],
                    "producer_input_contract_sha256": contract_sha256,
                    "producer_unit_id": contract["producer_unit_id"],
                    "producer_recorded_at": contract[
                        "producer_recorded_at"
                    ],
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "classification": "pre_activation_frozen",
                    "model_enqueue_allowed": False,
                    "classified_at": _utc_now(),
                    "formal_write_count": 0,
                },
                purpose="dispatch-production-canary-exclusion",
            )
            _atomic_replace_json(path, exclusion)
            refreshed = self._refresh_canary_queue_projection_locked(state)
            self._write_canary_state_locked(refreshed)
            return exclusion

    def pending_production_canary_tasks(self, subject: str) -> list[FrozenTask]:
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            superseded = self._quick_flush_supersede_statuses_locked(state)
            entries = [
                row
                for row in self._queue_entries_locked(subject)
                if row.get("queue_status") == "pending"
                and row.get("producer_input_contract_sha256")
                not in superseded
            ]
            entries.sort(
                key=lambda row: (
                    str(row.get("producer_recorded_at") or ""),
                    str(row.get("producer_unit_id") or ""),
                    str(row.get("producer_input_contract_sha256") or ""),
                )
            )
            tasks: list[FrozenTask] = []
            for entry in entries:
                path = Path(str(entry.get("task_object_path") or ""))
                expected_sha256 = entry.get("task_object_sha256")
                try:
                    raw = path.read_bytes()
                    value = json.loads(raw.decode("utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise DispatchError("production_canary_task_object_invalid") from exc
                if (
                    _sha256_bytes(raw) != expected_sha256
                    or not isinstance(value, Mapping)
                ):
                    raise DispatchError("production_canary_task_object_invalid")
                task = FrozenTask.from_mapping(value)
                if (
                    task.unit_sha256 != entry.get("unit_sha256")
                    or task.frozen_payload_sha256
                    != entry.get("frozen_payload_sha256")
                ):
                    raise DispatchError("production_canary_task_object_invalid")
                self._validate_canary_task_locked(task, state)
                tasks.append(task)
            return tasks

    def production_canary_status(
        self, subject: str, *, expected_release_id: str | None = None
    ) -> dict[str, Any] | None:
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject, required=False)
            if state is None:
                return None
            if (
                expected_release_id is not None
                and state.get("release_id") != expected_release_id
            ):
                raise DispatchError("production_canary_release_mismatch")
            state = self._reconcile_production_canary_preclaim_failures_locked(
                state
            )
            refreshed = self._refresh_canary_queue_projection_locked(state)
            if self._canary_projection_changed(state, refreshed):
                state = self._write_canary_state_locked(refreshed)
            return state

    def production_canary_status_read_only(
        self, subject: str, *, expected_release_id: str | None = None
    ) -> dict[str, Any] | None:
        """Reopen signed state without reconciliation or projection writes."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject, required=False)
            if state is None:
                return None
            if (
                expected_release_id is not None
                and state.get("release_id") != expected_release_id
            ):
                raise DispatchError("production_canary_release_mismatch")
            return copy.deepcopy(state)

    def inspect_production_canary_task_read_only(
        self, subject: str, task: FrozenTask
    ) -> dict[str, Any]:
        """Classify one scan row without materializing queue or exclusion state."""

        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            contract = self._producer_contract_from_task(task)
            try:
                self._validate_canary_task_locked(task, state)
            except DispatchError as exc:
                if exc.code == "production_canary_pre_activation_capture":
                    classification = "pre_activation_frozen"
                else:
                    raise
            else:
                classification = "post_activation_unmaterialized"
            persisted = str(contract["producer_unit_id"]) in (
                self._production_canary_exclusions_locked(state)
            )
            return {
                "classification": classification,
                "persisted": persisted,
                "activation_id": state["activation_id"],
                "producer_unit_id": contract["producer_unit_id"],
                "producer_input_contract_sha256": contract[
                    "producer_input_contract_sha256"
                ],
                "unit_sha256": task.unit_sha256,
                "frozen_payload_sha256": task.frozen_payload_sha256,
            }

    def set_production_canary_backpressure(
        self, subject: str, reason: str | None
    ) -> dict[str, Any]:
        if reason not in {None, "continuous_concurrency_limit_reached"}:
            raise DispatchError("production_canary_backpressure_invalid")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            if state.get("backpressure_reason") == reason:
                return state
            updated = dict(state)
            updated["backpressure_reason"] = reason
            if reason is not None:
                updated["next_action"] = "await_continuous_concurrency_capacity"
            elif state.get("state") == "continuous_concurrent_unlocked":
                updated["next_action"] = (
                    "consume_pending_post_activation_queue"
                )
            return self._write_canary_state_locked(updated)

    def _update_canary_queue_entry_locked(
        self, entry: Mapping[str, Any], **changes: Any
    ) -> dict[str, Any]:
        updated = copy.deepcopy(dict(entry))
        updated.pop("authority", None)
        updated.update(changes)
        sealed = self._seal(
            updated, purpose="dispatch-production-canary-queue"
        )
        _atomic_replace_json(
            self._production_canary_queue_path(
                str(updated["subject"]),
                str(updated["producer_input_contract_sha256"]),
            ),
            sealed,
        )
        return sealed

    def _admit_production_canary_task_locked(
        self,
        task: FrozenTask,
        state: Mapping[str, Any],
        timestamp: str,
        *,
        lease_owner_id: str,
        lease_fence: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        contract = self._validate_canary_task_locked(task, state)
        gate_state = state.get("state")
        if gate_state == "inactive_rolled_back":
            raise DispatchError("production_canary_inactive")
        if gate_state == "failed_drained":
            raise DispatchError("production_canary_subject_failed_drained")
        if gate_state == "canary_in_flight":
            raise DispatchError("production_canary_first_task_in_flight")
        if gate_state not in {"armed", "continuous_concurrent_unlocked"}:
            raise DispatchError("production_canary_state_invalid")
        active_count = int(state.get("active_task_count") or 0)
        active_limit = (
            INITIAL_CANARY_INFLIGHT_LIMIT
            if gate_state == "armed"
            else int(state["continuous_concurrency_limit"])
        )
        if active_count >= active_limit:
            raise DispatchError("production_canary_subject_limit_reached")
        contract_sha256 = str(contract["producer_input_contract_sha256"])
        queue_path = self._production_canary_queue_path(
            str(state["subject"]), contract_sha256
        )
        queue_entry = self._read_object(queue_path)
        if queue_entry is None:
            raise DispatchError("production_canary_task_not_materialized")
        self._verify_seal(
            queue_entry, purpose="dispatch-production-canary-queue"
        )
        if (
            queue_entry.get("queue_status") != "pending"
            or queue_entry.get("unit_sha256") != task.unit_sha256
            or queue_entry.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or queue_entry.get("activation_id") != state.get("activation_id")
        ):
            raise DispatchError("production_canary_task_replay")
        selected = {
            "producer_unit_id": contract["producer_unit_id"],
            "producer_recorded_at": contract["producer_recorded_at"],
            "producer_input_contract_sha256": contract_sha256,
            "source_event_set_sha256": contract["source_event_set_sha256"],
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "lease_owner_id": lease_owner_id,
            "lease_fence": lease_fence,
            "context_root": str(
                self.runtime_root
                / "dispatch"
                / "contexts"
                / task.unit_sha256
                / f"fence-{lease_fence}"
            ),
            "mcp_session_root": str(
                self.runtime_root
                / "dispatch"
                / "contexts"
                / task.unit_sha256
                / f"fence-{lease_fence}"
                / "mcp-session"
            ),
            "report_root": str(
                self.runtime_root
                / "dispatch"
                / "contexts"
                / task.unit_sha256
                / f"fence-{lease_fence}"
                / "reports"
            ),
            "process_identity_sha256": None,
            "process_identity_path": None,
        }
        gate_core = {
            "schema_version": PRODUCTION_CANARY_GATE_SCHEMA,
            "activation_id": state["activation_id"],
            "subject": state["subject"],
            "release_id": state["release_id"],
            "producer_authority_fingerprint": state[
                "producer_authority_fingerprint"
            ],
            "producer_high_watermark_sha256": state[
                "producer_high_watermark_sha256"
            ],
            "activation_receipt_sha256": state[
                "activation_receipt_sha256"
            ],
            "activation_gate_authority_sha256": state[
                "activation_gate_authority_sha256"
            ],
            "state_before": gate_state,
            "state_after": (
                "canary_in_flight"
                if gate_state == "armed"
                else "continuous_concurrent_unlocked"
            ),
            "selected": selected,
            "admitted_at": timestamp,
            **self._canary_control_fields(
                int(state["continuous_concurrency_limit"])
            ),
        }
        gate_sha256, gate_path, gate_receipt = (
            self._publish_canary_receipt_locked(
                str(state["subject"]),
                str(state["activation_id"]),
                gate_core,
                purpose="dispatch-production-canary-gate",
            )
        )
        selected.update(
            {
                "canary_gate_sha256": gate_sha256,
                "canary_gate_path": str(gate_path),
                "canary_gate_authority_sha256": _sha256_bytes(
                    _canonical_bytes(gate_receipt["authority"])
                ),
            }
        )
        queue_entry = self._update_canary_queue_entry_locked(
            queue_entry,
            queue_status="claimed",
            claimed_at=timestamp,
            finished_at=None,
            terminal_receipt_sha256=None,
            terminal_receipt_path=None,
            terminal_outcome=None,
            terminal_error_code=None,
            canary_gate_sha256=gate_sha256,
            canary_gate_path=str(gate_path),
            canary_gate_authority_sha256=selected[
                "canary_gate_authority_sha256"
            ],
            lease_owner_id=lease_owner_id,
            lease_fence=lease_fence,
            context_root=selected["context_root"],
            mcp_session_root=selected["mcp_session_root"],
            report_root=selected["report_root"],
            process_identity_sha256=None,
            process_identity_path=None,
        )
        updated = dict(state)
        active_selections = copy.deepcopy(
            dict(state.get("active_selections") or {})
        )
        if task.unit_sha256 in active_selections:
            raise DispatchError("production_canary_task_replay")
        active_selections[task.unit_sha256] = selected
        next_active_count = active_count + 1
        updated.update(
            {
                "state": (
                    "canary_in_flight"
                    if gate_state == "armed"
                    else "continuous_concurrent_unlocked"
                ),
                "luna_consumer_enabled": True,
                "active_task_count": next_active_count,
                "active_selections": active_selections,
                "blocking_reason": None,
                "next_action": "await_active_task_terminal",
                "selected": selected if gate_state == "armed" else None,
                "last_selected": selected,
                "backpressure_reason": (
                    "continuous_concurrency_limit_reached"
                    if gate_state == "continuous_concurrent_unlocked"
                    and next_active_count >= active_limit
                    else None
                ),
            }
        )
        updated = self._refresh_canary_queue_projection_locked(updated)
        self._write_canary_state_locked(updated, updated_at=timestamp)
        self._recompute_canary_concurrency_telemetry_locked(
            str(state["release_id"]), updated_at=timestamp
        )
        return contract, queue_entry

    def finish_production_canary_task(
        self,
        task: FrozenTask,
        *,
        outcome: str | None,
        error_code: str | None,
        completion: Mapping[str, Any] | None,
        finished_at: str | None = None,
    ) -> dict[str, Any]:
        timestamp = finished_at or _utc_now()
        _parse_utc(timestamp)
        subject = str(task.frozen_payload.get("subject") or "")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            contract = self._validate_canary_task_locked(task, state)
            contract_sha256 = str(contract["producer_input_contract_sha256"])
            queue_entry = self._read_object(
                self._production_canary_queue_path(subject, contract_sha256)
            )
            if queue_entry is None:
                raise DispatchError("production_canary_task_not_materialized")
            self._verify_seal(
                queue_entry, purpose="dispatch-production-canary-queue"
            )
            if (
                queue_entry.get("queue_status") != "claimed"
                or not isinstance(queue_entry.get("canary_gate_sha256"), str)
            ):
                raise DispatchError("production_canary_terminal_binding_invalid")
            selected = {
                "producer_unit_id": queue_entry.get("producer_unit_id"),
                "producer_recorded_at": queue_entry.get("producer_recorded_at"),
                "producer_input_contract_sha256": queue_entry.get(
                    "producer_input_contract_sha256"
                ),
                "source_event_set_sha256": queue_entry.get(
                    "source_event_set_sha256"
                ),
                "unit_sha256": queue_entry.get("unit_sha256"),
                "frozen_payload_sha256": queue_entry.get(
                    "frozen_payload_sha256"
                ),
                "canary_gate_sha256": queue_entry.get("canary_gate_sha256"),
                "canary_gate_path": queue_entry.get("canary_gate_path"),
                "canary_gate_authority_sha256": queue_entry.get(
                    "canary_gate_authority_sha256"
                ),
                "lease_owner_id": queue_entry.get("lease_owner_id"),
                "lease_fence": queue_entry.get("lease_fence"),
                "context_root": queue_entry.get("context_root"),
                "mcp_session_root": queue_entry.get("mcp_session_root"),
                "report_root": queue_entry.get("report_root"),
                "process_identity_sha256": queue_entry.get(
                    "process_identity_sha256"
                ),
                "process_identity_path": queue_entry.get(
                    "process_identity_path"
                ),
            }
            active_selections = copy.deepcopy(
                dict(state.get("active_selections") or {})
            )
            active_selected = active_selections.get(task.unit_sha256)
            if (
                not isinstance(active_selected, Mapping)
                or selected.get("unit_sha256") != task.unit_sha256
                or selected.get("producer_input_contract_sha256")
                != contract_sha256
                or not isinstance(selected.get("canary_gate_sha256"), str)
                or not isinstance(
                    selected.get("canary_gate_authority_sha256"), str
                )
                or any(
                    active_selected.get(key) != value
                    for key, value in selected.items()
                )
            ):
                raise DispatchError("production_canary_terminal_binding_invalid")
            succeeded = outcome == "succeeded" and isinstance(completion, Mapping)
            reviewable = bool(
                outcome == "failed"
                and isinstance(completion, Mapping)
                and isinstance(error_code, str)
                and error_code
                and isinstance(completion.get("package_path"), str)
                and completion.get("package_path")
                and isinstance(completion.get("package_sha256"), str)
                and completion.get("package_sha256")
            )
            report_generated = succeeded or reviewable
            completed_without_execution_failure = succeeded
            completion_path: str | None = None
            completion_sha256: str | None = None
            processing_receipt_path: str | None = None
            processing_receipt_sha256: str | None = None
            package_path: str | None = None
            package_sha256: str | None = None
            package_ref: str | None = None
            report_json_ref: str | None = None
            report_json_sha256: str | None = None
            report_markdown_ref: str | None = None
            report_markdown_sha256: str | None = None
            report_reopen_status = "not_available"
            report_terminal_status: str | None = None
            observed_model_calls = 0
            observed_provider_requests = 0
            observed_mcp_calls = 0
            mcp_stage_grounding: dict[str, Any] | None = None
            read_session_id: str | None = None
            evidence_generation: str | None = None
            evidence_authority_fingerprint: str | None = None
            task_declared_evidence_refs_sha256: str | None = None
            process_execution: dict[str, Any] | None = None
            ordinary_succeeded = False
            quality_reviewable = reviewable
            if isinstance(completion, Mapping):
                if (
                    completion.get("unit_sha256") != task.unit_sha256
                    or completion.get("subject") != subject
                    or completion.get("release_id") != state.get("release_id")
                    or completion.get("outcome") != outcome
                ):
                    raise DispatchError(
                        "production_canary_completion_binding_invalid"
                    )
                self._verify_seal(completion, purpose="dispatch-completion")
                completion_path = str(self._completion_path(task.unit_sha256))
                completion_bytes = Path(completion_path).read_bytes()
                completion_sha256 = _sha256_bytes(completion_bytes)
                processing_receipt_path = str(
                    completion.get("receipt_path") or ""
                )
                processing_receipt_sha256 = str(
                    completion.get("receipt_sha256") or ""
                )
                package_path = (
                    str(completion.get("package_path"))
                    if completion.get("package_path") is not None
                    else None
                )
                package_sha256 = (
                    str(completion.get("package_sha256"))
                    if completion.get("package_sha256") is not None
                    else None
                )
                package_ref = (
                    str(completion.get("package_ref"))
                    if completion.get("package_ref") is not None
                    else None
                )
                report_json_ref = (
                    str(completion.get("report_json_ref"))
                    if completion.get("report_json_ref") is not None
                    else None
                )
                report_json_sha256 = (
                    str(completion.get("report_json_sha256"))
                    if completion.get("report_json_sha256") is not None
                    else None
                )
                report_markdown_ref = (
                    str(completion.get("report_markdown_ref"))
                    if completion.get("report_markdown_ref") is not None
                    else None
                )
                report_markdown_sha256 = (
                    str(completion.get("report_markdown_sha256"))
                    if completion.get("report_markdown_sha256") is not None
                    else None
                )
                receipt = self._read_object(Path(processing_receipt_path))
                if receipt is None:
                    raise DispatchError(
                        "production_canary_processing_receipt_missing"
                    )
                self._verify_seal(receipt, purpose="dispatch-receipt")
                if _sha256_bytes(Path(processing_receipt_path).read_bytes()) != (
                    processing_receipt_sha256
                ):
                    raise DispatchError(
                        "production_canary_processing_receipt_invalid"
                    )
                raw_process_execution = receipt.get("process_execution")
                process_execution = (
                    copy.deepcopy(dict(raw_process_execution))
                    if isinstance(raw_process_execution, Mapping)
                    else None
                )
                runtime = receipt.get("observed_stage_runtime")
                if isinstance(runtime, Mapping):
                    for row in runtime.values():
                        if not isinstance(row, Mapping):
                            continue
                        observed_model_calls += int(row.get("model_call_count") or 0)
                        observed_provider_requests += int(
                            row.get("provider_request_count") or 0
                        )
                        observed_mcp_calls += int(
                            row.get("mcp_tool_call_count") or 0
                        )
                quality_success = False
                if (
                    succeeded
                    and package_path is not None
                    and package_sha256 is not None
                ):
                    quality_package_bytes = Path(package_path).read_bytes()
                    quality_package = json.loads(
                        quality_package_bytes.decode("utf-8")
                    )
                    quality_success = bool(
                        _sha256_bytes(quality_package_bytes) == package_sha256
                        and isinstance(quality_package, Mapping)
                        and quality_package.get("schema_version")
                        == REVIEW_PACKAGE_SCHEMA
                        and quality_package.get("report_available") is True
                        and quality_package.get("report_disposition")
                        == "needs_sol_review"
                        and quality_package.get("sol_review_status")
                        == "pending"
                        and quality_package.get("formal_write_eligible")
                        is False
                    )
                ordinary_succeeded = succeeded and not quality_success
                quality_reviewable = reviewable or quality_success
                if ordinary_succeeded:
                    if not isinstance(runtime, Mapping):
                        raise DispatchError(
                            "production_canary_mcp_grounding_missing"
                        )
                    raw_task_refs = task.frozen_payload.get(
                        "allowed_evidence_refs"
                    )
                    if not isinstance(raw_task_refs, list) or not all(
                        isinstance(ref, str) and ref for ref in raw_task_refs
                    ):
                        raise DispatchError(
                            "production_canary_task_evidence_refs_invalid"
                        )
                    declared_refs = set(raw_task_refs)
                    task_declared_evidence_refs_sha256 = _sha256_bytes(
                        _canonical_bytes(sorted(declared_refs))
                    )
                    mcp_stage_grounding = {}
                    prior_consumed: set[str] = set()
                    session_binding: tuple[str, str, str] | None = None
                    stage_names = [
                        stage_name
                        for stage_name in ("analysis", "critical_review")
                        if isinstance(runtime.get(stage_name), Mapping)
                    ]
                    if (
                        "analysis" not in stage_names
                        or ordinary_succeeded
                        and stage_names != ["analysis", "critical_review"]
                    ):
                        raise DispatchError(
                            "production_canary_mcp_grounding_missing"
                        )
                    for stage_name in stage_names:
                        row = runtime.get(stage_name)
                        if not isinstance(row, Mapping):
                            raise DispatchError(
                                "production_canary_mcp_grounding_missing"
                            )
                        consumed = row.get("mcp_consumed_evidence_refs")
                        cited = row.get("mcp_cited_evidence_refs")
                        grounded = row.get(
                            "mcp_stage_grounded_evidence_refs"
                        )
                        if not all(
                            isinstance(value, list)
                            and value
                            and all(
                                isinstance(ref, str) and ref
                                for ref in value
                            )
                            for value in (consumed, cited, grounded)
                        ):
                            raise DispatchError(
                                "production_canary_mcp_grounding_missing"
                            )
                        consumed_set = set(consumed)
                        cited_set = set(cited)
                        grounded_set = set(grounded)
                        effective_allowed = (
                            declared_refs | prior_consumed | consumed_set
                        )
                        stage_session = row.get("read_session_id")
                        stage_generation = row.get("evidence_generation")
                        stage_authority = row.get(
                            "evidence_authority_fingerprint"
                        )
                        manifest_sha256 = row.get(
                            "mcp_grounding_manifest_sha256"
                        )
                        read_manifest_sha256 = row.get(
                            "read_session_manifest_sha256"
                        )
                        if (
                            row.get("evidence_subject") != subject
                            or row.get("evidence_release_id")
                            != state.get("release_id")
                            or not isinstance(stage_session, str)
                            or not stage_session
                            or not isinstance(stage_generation, str)
                            or not stage_generation
                            or not isinstance(stage_authority, str)
                            or not isinstance(manifest_sha256, str)
                            or not isinstance(read_manifest_sha256, str)
                            or int(row.get("mcp_tool_call_count") or 0) < 1
                            or not grounded_set.issubset(
                                consumed_set.intersection(cited_set)
                            )
                            or not cited_set.issubset(effective_allowed)
                        ):
                            raise DispatchError(
                                "production_canary_mcp_grounding_invalid"
                            )
                        for digest in (
                            stage_authority,
                            manifest_sha256,
                            read_manifest_sha256,
                        ):
                            _validate_unit_sha256(digest)
                        binding = (
                            stage_session,
                            stage_generation,
                            stage_authority,
                        )
                        if session_binding is None:
                            session_binding = binding
                        elif session_binding != binding:
                            raise DispatchError(
                                "production_canary_mcp_session_cross_binding"
                            )
                        mcp_stage_grounding[stage_name] = {
                            "read_session_id": stage_session,
                            "read_session_manifest_sha256": (
                                read_manifest_sha256
                            ),
                            "evidence_generation": stage_generation,
                            "evidence_authority_fingerprint": stage_authority,
                            "mcp_grounding_manifest_sha256": manifest_sha256,
                            "consumed_evidence_refs": sorted(consumed_set),
                            "cited_evidence_refs": sorted(cited_set),
                            "grounded_evidence_refs": sorted(grounded_set),
                            "effective_allowed_evidence_refs_sha256": (
                                _sha256_bytes(
                                    _canonical_bytes(
                                        sorted(effective_allowed)
                                    )
                                )
                            ),
                            "mcp_tool_call_count": int(
                                row["mcp_tool_call_count"]
                            ),
                        }
                        prior_consumed.update(consumed_set)
                    assert session_binding is not None
                    (
                        read_session_id,
                        evidence_generation,
                        evidence_authority_fingerprint,
                    ) = session_binding
                elif quality_reviewable:
                    if not isinstance(runtime, Mapping):
                        raise DispatchError(
                            "production_canary_review_runtime_missing"
                        )
                    review_runtime_rows = [
                        row
                        for stage_name in ("analysis", "critical_review")
                        for row in (runtime.get(stage_name),)
                        if isinstance(row, Mapping)
                    ]
                    if not review_runtime_rows:
                        raise DispatchError(
                            "production_canary_review_runtime_missing"
                        )
                    for row in review_runtime_rows:
                        for key in (
                            "raw_output_object_sha256",
                            "stage_execution_receipt_sha256",
                            "stage_normalization_receipt_sha256",
                            "review_mcp_transcript_sha256",
                        ):
                            _validate_unit_sha256(str(row.get(key) or ""))
                        if (
                            row.get("review_mcp_transcript_ref")
                            != "study-intake-mcp-stage-transcript://sha256/"
                            + str(row["review_mcp_transcript_sha256"])
                            or int(row.get("model_call_count") or 0) != 1
                            or int(row.get("provider_request_count") or 0) < 2
                            or int(row.get("mcp_tool_call_count") or 0) < 1
                            or row.get("normalization_status")
                            not in {
                                "normalized",
                                "normalized_with_warnings",
                            }
                        ):
                            raise DispatchError(
                                "production_canary_review_runtime_invalid"
                            )
                event_root = self.task_event_index_root / task.unit_sha256
                event_names: list[str] = []
                for attempt_root in sorted(
                    (
                        path
                        for path in event_root.glob("fence-*")
                        if path.is_dir()
                        and path.name.removeprefix("fence-").isdigit()
                    ),
                    key=lambda path: int(path.name.removeprefix("fence-")),
                ):
                    for index_path in sorted(attempt_root.glob("*.json")):
                        index = self._read_object(index_path)
                        if index is None:
                            raise DispatchError(
                                "production_canary_event_history_invalid"
                            )
                        event_path = Path(str(index.get("event_path") or ""))
                        event = self._read_object(event_path)
                        if event is None:
                            raise DispatchError(
                                "production_canary_event_history_invalid"
                            )
                        self._verify_seal(event, purpose="dispatch-task-event")
                        if (
                            event.get("unit_sha256") != task.unit_sha256
                            or event.get("release_id") != state.get("release_id")
                            or event.get("event") != index.get("event")
                        ):
                            raise DispatchError(
                                "production_canary_event_history_invalid"
                            )
                        event_names.append(str(event["event"]))
                required_order = [
                    "analysis_submitted",
                    "analysis_completed",
                    "critical_started",
                    "critical_completed",
                    "published",
                ]
                cursor = 0
                for event_name in event_names:
                    if (
                        cursor < len(required_order)
                        and event_name == required_order[cursor]
                    ):
                        cursor += 1
                if ordinary_succeeded and cursor != len(required_order):
                    raise DispatchError(
                        "production_canary_stage_sequence_incomplete"
                    )
                if package_path is not None and package_sha256 is not None:
                    try:
                        package_bytes = Path(package_path).read_bytes()
                        package = json.loads(package_bytes.decode("utf-8"))
                    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                        raise DispatchError(
                            "production_canary_report_reopen_failed"
                        ) from exc
                    if (
                        _sha256_bytes(package_bytes) != package_sha256
                        or not isinstance(package, Mapping)
                        or package.get("unit_sha256") != task.unit_sha256
                        or (
                            ordinary_succeeded
                            and package.get("schema_version") != PACKAGE_SCHEMA
                        )
                        or (
                            quality_reviewable
                            and package.get("schema_version")
                            != REVIEW_PACKAGE_SCHEMA
                        )
                        or not isinstance(package.get("analysis"), Mapping)
                        or not package.get("analysis")
                        or (
                            ordinary_succeeded
                            and (
                                not isinstance(
                                    package.get("critical_review"), Mapping
                                )
                                or not package.get("critical_review")
                            )
                        )
                        or (
                            quality_reviewable
                            and (
                                package.get("report_available") is not True
                                or package.get("sol_review_status")
                                != (
                                    "pending"
                                    if package.get("report_disposition")
                                    == "needs_sol_review"
                                    else "not_eligible"
                                )
                                or package.get("formal_write_eligible")
                                is not False
                                or package.get("report_disposition")
                                not in {"needs_sol_review", "quarantined"}
                                or not isinstance(
                                    package.get("review_result"), Mapping
                                )
                                or not isinstance(
                                    package.get("warnings"), list
                                )
                                or not package.get("warnings")
                            )
                        )
                    ):
                        raise DispatchError(
                            "production_canary_report_reopen_failed"
                        )
                    if quality_reviewable:
                        report_terminal_status = str(
                            package["report_disposition"]
                        )
                if report_generated:
                    for digest in (
                        report_json_sha256,
                        report_markdown_sha256,
                        package_sha256,
                    ):
                        _validate_unit_sha256(str(digest or ""))
                    if (
                        package_ref
                        != "study-intake-dispatch-package://sha256/"
                        + str(package_sha256)
                        or report_json_ref
                        != "study-intake-report://sha256/"
                        + str(report_json_sha256)
                        or report_markdown_ref
                        != "study-intake-report-markdown://sha256/"
                        + str(report_markdown_sha256)
                    ):
                        raise DispatchError(
                            "production_canary_report_binding_invalid"
                        )
                    report_json_path = (
                        self.runtime_root
                        / "dispatch"
                        / "reports"
                        / "json"
                        / "sha256"
                        / str(report_json_sha256)[:2]
                        / f"{report_json_sha256}.json"
                    )
                    report_markdown_path = (
                        self.runtime_root
                        / "dispatch"
                        / "reports"
                        / "markdown"
                        / "sha256"
                        / str(report_markdown_sha256)[:2]
                        / f"{report_markdown_sha256}.md"
                    )
                    try:
                        report_json_bytes = report_json_path.read_bytes()
                        report_value = json.loads(
                            report_json_bytes.decode("utf-8")
                        )
                        markdown_bytes = report_markdown_path.read_bytes()
                    except (
                        OSError,
                        UnicodeError,
                        json.JSONDecodeError,
                    ) as exc:
                        raise DispatchError(
                            "production_canary_report_reopen_failed"
                        ) from exc
                    if (
                        _sha256_bytes(report_json_bytes)
                        != report_json_sha256
                        or _sha256_bytes(markdown_bytes)
                        != report_markdown_sha256
                        or not isinstance(report_value, Mapping)
                        or report_value.get("unit_sha256")
                        != task.unit_sha256
                        or report_value.get("package_sha256")
                        != package_sha256
                        or not markdown_bytes.strip()
                        or (
                            quality_reviewable
                            and (
                                report_value.get("report_available") is not True
                                or report_value.get("sol_review_status")
                                != (
                                    "pending"
                                    if report_value.get("report_disposition")
                                    == "needs_sol_review"
                                    else "not_eligible"
                                )
                                or report_value.get("formal_write_eligible")
                                is not False
                                or report_value.get("report_disposition")
                                != report_terminal_status
                                or not isinstance(
                                    report_value.get("review_result"), Mapping
                                )
                                or not isinstance(
                                    report_value.get("warnings"), list
                                )
                                or not report_value.get("warnings")
                            )
                        )
                    ):
                        raise DispatchError(
                            "production_canary_report_reopen_failed"
                        )
                    report_reopen_status = "json_markdown_package_verified"
                if quality_reviewable:
                    try:
                        from subject_sol_contract import SubjectSolRuntimeStore

                        restricted_quality = SubjectSolRuntimeStore(
                            self.runtime_root
                        ).read_restricted_sol_review_candidate(
                            str(report_json_sha256)
                        )
                    except Exception as exc:
                        raise DispatchError(
                            "production_canary_review_evidence_incomplete"
                        ) from exc
                    restricted_raw = restricted_quality.get("raw_outputs")
                    restricted_transcripts = restricted_quality.get(
                        "mcp_transcripts"
                    )
                    actual_stage_names = tuple(
                        stage_name
                        for stage_name in ("analysis", "critical_review")
                        if isinstance(package.get(stage_name), Mapping)
                    )
                    if (
                        not actual_stage_names
                        or actual_stage_names[0] != "analysis"
                        or not isinstance(restricted_raw, Mapping)
                        or not isinstance(restricted_transcripts, Mapping)
                        or set(restricted_raw) != set(actual_stage_names)
                        or set(restricted_transcripts) != set(actual_stage_names)
                    ):
                        raise DispatchError(
                            "production_canary_review_evidence_incomplete"
                        )
                    session_binding: tuple[str, str, str] | None = None
                    mcp_stage_grounding = {}
                    for stage_name in actual_stage_names:
                        transcript = restricted_transcripts.get(stage_name)
                        raw_output = restricted_raw.get(stage_name)
                        if (
                            not isinstance(transcript, Mapping)
                            or not isinstance(raw_output, Mapping)
                        ):
                            raise DispatchError(
                                "production_canary_review_evidence_incomplete"
                            )
                        calls = transcript.get("calls")
                        if not isinstance(calls, list) or not calls:
                            raise DispatchError(
                                "production_canary_review_evidence_incomplete"
                            )
                        database_result_refs = sorted(
                            {
                                str(
                                    call.get("result_sha256")
                                    or call.get("content_sha256")
                                )
                                for call in calls
                                if isinstance(call, Mapping)
                                and call.get("tool")
                                in {
                                    "list_records",
                                    "get_records",
                                    "search_records",
                                    "query_relations",
                                }
                                and isinstance(
                                    call.get("result_sha256")
                                    or call.get("content_sha256"),
                                    str,
                                )
                            }
                        )
                        stage_session = str(
                            transcript.get("read_session_id") or ""
                        )
                        stage_generation = str(
                            transcript.get("generation") or ""
                        )
                        stage_authority = str(
                            transcript.get("authority_fingerprint") or ""
                        )
                        stage_manifest = str(
                            transcript.get("read_session_manifest_sha256")
                            or ""
                        )
                        transcript_sha256 = str(
                            runtime[stage_name].get(
                                "review_mcp_transcript_sha256"
                            )
                            if isinstance(
                                runtime.get(stage_name), Mapping
                            )
                            else ""
                        )
                        if (
                            not database_result_refs
                            or not stage_session
                            or not stage_generation
                        ):
                            raise DispatchError(
                                "production_canary_review_evidence_incomplete"
                            )
                        for digest in (
                            stage_authority,
                            stage_manifest,
                            transcript_sha256,
                        ):
                            _validate_unit_sha256(digest)
                        binding = (
                            stage_session,
                            stage_generation,
                            stage_authority,
                        )
                        if session_binding is None:
                            session_binding = binding
                        elif session_binding != binding:
                            raise DispatchError(
                                "production_canary_mcp_session_cross_binding"
                            )
                        mcp_stage_grounding[stage_name] = {
                            "read_session_id": stage_session,
                            "read_session_manifest_sha256": stage_manifest,
                            "evidence_generation": stage_generation,
                            "evidence_authority_fingerprint": stage_authority,
                            "mcp_grounding_manifest_sha256": transcript_sha256,
                            "grounded_evidence_refs": database_result_refs,
                            "mcp_tool_call_count": int(
                                transcript.get("mcp_tool_call_count") or 0
                            ),
                        }
                    assert session_binding is not None
                    (
                        read_session_id,
                        evidence_generation,
                        evidence_authority_fingerprint,
                    ) = session_binding
                    raw_task_refs = task.frozen_payload.get(
                        "allowed_evidence_refs"
                    )
                    if not isinstance(raw_task_refs, list) or not all(
                        isinstance(ref, str) and ref for ref in raw_task_refs
                    ):
                        raise DispatchError(
                            "production_canary_review_evidence_incomplete"
                        )
                    task_declared_evidence_refs_sha256 = _sha256_bytes(
                        _canonical_bytes(sorted(set(raw_task_refs)))
                    )
            if ordinary_succeeded and (
                package_path is None
                or package_sha256 is None
                or package_ref is None
                or report_json_ref is None
                or report_json_sha256 is None
                or report_markdown_ref is None
                or report_markdown_sha256 is None
                or report_reopen_status
                != "json_markdown_package_verified"
                or observed_model_calls <= 0
                or observed_provider_requests <= 0
                or observed_mcp_calls <= 0
                or mcp_stage_grounding is None
                or read_session_id is None
                or evidence_generation is None
                or evidence_authority_fingerprint is None
                or task_declared_evidence_refs_sha256 is None
                or selected.get("process_identity_sha256") is None
                or selected.get("process_identity_path") is None
            ):
                raise DispatchError("production_canary_success_evidence_incomplete")
            if quality_reviewable and (
                package_path is None
                or package_sha256 is None
                or package_ref is None
                or report_json_ref is None
                or report_json_sha256 is None
                or report_markdown_ref is None
                or report_markdown_sha256 is None
                or report_reopen_status
                != "json_markdown_package_verified"
                or report_terminal_status
                not in {"needs_sol_review", "quarantined"}
                or observed_model_calls <= 0
                or observed_provider_requests <= 0
                or observed_mcp_calls <= 0
                or mcp_stage_grounding is None
                or read_session_id is None
                or evidence_generation is None
                or evidence_authority_fingerprint is None
                or task_declared_evidence_refs_sha256 is None
                or selected.get("process_identity_sha256") is None
                or selected.get("process_identity_path") is None
            ):
                raise DispatchError(
                    "production_canary_review_evidence_incomplete"
                )
            terminal_state_after = (
                "failed_drained"
                if state.get("state") == "failed_drained"
                or not completed_without_execution_failure
                else "continuous_concurrent_unlocked"
                if succeeded
                or state.get("unlocked_once") is True
                else "armed"
            )
            terminal_core = {
                "schema_version": (
                    PRODUCTION_CANARY_REVIEW_TERMINAL_SCHEMA
                    if quality_reviewable
                    else PRODUCTION_CANARY_TERMINAL_SCHEMA
                ),
                "counter_scope": "current_control_plane_operation",
                "terminal_kind": "normal",
                # Reviewable/quarantined output is fenced from the ordinary
                # success/formal path even though the subject gate remains
                # armed for subsequent work.
                "late_result_fenced": not succeeded,
                "activation_id": state["activation_id"],
                "subject": subject,
                "release_id": state["release_id"],
                "producer_authority_fingerprint": state[
                    "producer_authority_fingerprint"
                ],
                "producer_high_watermark_sha256": state[
                    "producer_high_watermark_sha256"
                ],
                "activation_receipt_sha256": state[
                    "activation_receipt_sha256"
                ],
                "activation_gate_authority_sha256": state[
                    "activation_gate_authority_sha256"
                ],
                "canary_gate_sha256": queue_entry["canary_gate_sha256"],
                "canary_gate_authority_sha256": selected[
                    "canary_gate_authority_sha256"
                ],
                "selected": {
                    key: selected[key]
                    for key in (
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
                        "process_identity_sha256",
                        "process_identity_path",
                    )
                },
                "outcome": outcome or "failed",
                "error_code": error_code,
                "completion_path": completion_path,
                "completion_sha256": completion_sha256,
                "completion_receipt_path": processing_receipt_path,
                "completion_receipt_sha256": processing_receipt_sha256,
                "package_path": package_path,
                "package_sha256": package_sha256,
                "package_ref": package_ref,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha256,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha256,
                "report_reopen_status": report_reopen_status,
                "observed_model_call_count": observed_model_calls,
                "observed_provider_request_count": observed_provider_requests,
                "observed_mcp_tool_call_count": observed_mcp_calls,
                "read_session_id": read_session_id,
                "evidence_generation": evidence_generation,
                "evidence_authority_fingerprint": (
                    evidence_authority_fingerprint
                ),
                "task_declared_evidence_refs_sha256": (
                    task_declared_evidence_refs_sha256
                ),
                "mcp_stage_grounding": mcp_stage_grounding,
                "process_execution": process_execution,
                "finished_at": timestamp,
                "state_after": terminal_state_after,
                **self._canary_control_fields(
                    int(state["continuous_concurrency_limit"])
                ),
            }
            if quality_reviewable:
                terminal_core.update(
                    {
                        "execution_status": (
                            "succeeded" if succeeded else "failed"
                        ),
                        "quality_status": (
                            "issues_found"
                            if report_terminal_status == "needs_sol_review"
                            else "unchecked"
                        ),
                        "report_available": True,
                        "report_disposition": report_terminal_status,
                        "sol_review_status": (
                            "pending"
                            if report_terminal_status == "needs_sol_review"
                            else "not_eligible"
                        ),
                        "formal_write_eligible": False,
                        "production_accepted": False,
                    }
                )
            emergency_cancelled = (
                outcome == "cancelled" and error_code == "daemon_shutdown"
            )
            if emergency_cancelled:
                terminal_core.update(
                    {
                        "terminal_kind": "emergency_hard_cancel",
                        "late_result_fenced": True,
                    }
                )
            terminal_sha256, terminal_path, _terminal = (
                self._publish_canary_receipt_locked(
                    subject,
                    str(state["activation_id"]),
                    terminal_core,
                    purpose="dispatch-production-canary-terminal",
                )
            )
            terminal_index_fields = self._append_canary_terminal_index_locked(
                state,
                task,
                queue_entry=queue_entry,
                terminal_receipt_sha256=terminal_sha256,
                terminal_receipt_path=str(terminal_path),
                outcome=str(outcome or "failed"),
                error_code=error_code,
                finished_at=timestamp,
                terminal_kind=(
                    "emergency_hard_cancel"
                    if emergency_cancelled
                    else "normal"
                ),
            )
            quality_queue_fields = (
                {
                    "execution_status": (
                        "succeeded" if succeeded else "failed"
                    ),
                    "quality_status": (
                        "issues_found"
                        if report_terminal_status == "needs_sol_review"
                        else "unchecked"
                    ),
                    "report_available": bool(report_generated),
                    "report_disposition": report_terminal_status,
                    "sol_review_status": (
                        "pending"
                        if report_terminal_status == "needs_sol_review"
                        else "not_eligible"
                    ),
                    "formal_write_eligible": False,
                    "production_accepted": False,
                }
                if quality_reviewable
                else {}
            )
            self._update_canary_queue_entry_locked(
                queue_entry,
                schema_version=(
                    PRODUCTION_CANARY_REVIEW_QUEUE_SCHEMA
                    if quality_reviewable
                    else PRODUCTION_CANARY_QUEUE_SCHEMA
                ),
                queue_status=(
                    "succeeded" if succeeded else "failed"
                ),
                finished_at=timestamp,
                terminal_receipt_sha256=terminal_sha256,
                terminal_receipt_path=str(terminal_path),
                terminal_outcome=outcome or "failed",
                terminal_error_code=error_code,
                **quality_queue_fields,
            )
            updated = dict(state)
            updated.update(terminal_index_fields)
            active_selections.pop(task.unit_sha256, None)
            remaining_active = len(active_selections)
            updated.update(
                {
                    "active_task_count": remaining_active,
                    "active_selections": active_selections,
                    "selected": None,
                    "last_terminal_receipt_sha256": terminal_sha256,
                    "last_terminal_receipt_path": str(terminal_path),
                }
            )
            if completed_without_execution_failure:
                if succeeded:
                    updated["last_success_at"] = timestamp
                updated["observed_model_call_count"] = int(
                    state.get("observed_model_call_count") or 0
                ) + observed_model_calls
                updated["observed_provider_request_count"] = int(
                    state.get("observed_provider_request_count") or 0
                ) + observed_provider_requests
                updated["observed_mcp_tool_call_count"] = int(
                    state.get("observed_mcp_tool_call_count") or 0
                ) + observed_mcp_calls
                updated["last_analysis_status"] = "completed"
                updated["last_critical_review_status"] = (
                    "completed"
                    if isinstance(mcp_stage_grounding, Mapping)
                    and "critical_review" in mcp_stage_grounding
                    else "not_started"
                )
                # production-canary-state-v3 is historical and immutable.
                # The exact review disposition lives in the new review
                # terminal/queue/report contracts; the v3 aggregate records
                # only that the report was reopened successfully.
                updated["last_report_status"] = "reopen_verified"
                updated["last_package_sha256"] = package_sha256
                updated["last_read_session_id"] = read_session_id
                updated["last_evidence_generation"] = evidence_generation
                updated["last_evidence_authority_fingerprint"] = (
                    evidence_authority_fingerprint
                )
                if isinstance(mcp_stage_grounding, Mapping):
                    if isinstance(
                        mcp_stage_grounding.get("analysis"), Mapping
                    ):
                        updated[
                            "last_analysis_grounding_manifest_sha256"
                        ] = mcp_stage_grounding["analysis"][
                            "mcp_grounding_manifest_sha256"
                        ]
                        updated["last_analysis_evidence_refs"] = list(
                            mcp_stage_grounding["analysis"][
                                "grounded_evidence_refs"
                            ]
                        )
                    if isinstance(
                        mcp_stage_grounding.get("critical_review"), Mapping
                    ):
                        updated[
                            "last_critical_review_grounding_manifest_sha256"
                        ] = mcp_stage_grounding["critical_review"][
                            "mcp_grounding_manifest_sha256"
                        ]
                        updated[
                            "last_critical_review_evidence_refs"
                        ] = list(
                            mcp_stage_grounding["critical_review"][
                                "grounded_evidence_refs"
                            ]
                        )
                if succeeded and state.get("state") != "failed_drained":
                    updated.update(
                        {
                            "state": "continuous_concurrent_unlocked",
                            "unlocked_once": True,
                            "luna_consumer_enabled": True,
                            "blocking_reason": None,
                            "next_action": "consume_pending_post_activation_queue",
                            "backpressure_reason": (
                                "continuous_concurrency_limit_reached"
                                if remaining_active
                                >= int(state["continuous_concurrency_limit"])
                                else None
                            ),
                        }
                    )
            else:
                updated.update(
                    {
                        "state": "failed_drained",
                        "luna_consumer_enabled": False,
                        "last_failure_at": timestamp,
                        "blocking_reason": str(error_code or "canary_task_failed"),
                        "next_action": "explicit_subject_resume_required",
                        "last_analysis_status": (
                            "completed"
                            if report_generated
                            else "failed_or_incomplete"
                        ),
                        "last_critical_review_status": (
                            "completed"
                            if isinstance(mcp_stage_grounding, Mapping)
                            and "critical_review" in mcp_stage_grounding
                            else "not_started"
                            if report_generated
                            else "failed_or_incomplete"
                        ),
                        "last_report_status": (
                            "reopen_verified"
                            if report_generated
                            else "not_verified"
                        ),
                        "backpressure_reason": None,
                    }
                )
                if report_generated:
                    updated["observed_model_call_count"] = int(
                        state.get("observed_model_call_count") or 0
                    ) + observed_model_calls
                    updated["observed_provider_request_count"] = int(
                        state.get("observed_provider_request_count") or 0
                    ) + observed_provider_requests
                    updated["observed_mcp_tool_call_count"] = int(
                        state.get("observed_mcp_tool_call_count") or 0
                    ) + observed_mcp_calls
                    updated["last_package_sha256"] = package_sha256
                    updated["last_read_session_id"] = read_session_id
                    updated["last_evidence_generation"] = evidence_generation
                    updated["last_evidence_authority_fingerprint"] = (
                        evidence_authority_fingerprint
                    )
                    if isinstance(mcp_stage_grounding, Mapping):
                        analysis_grounding = mcp_stage_grounding.get("analysis")
                        if isinstance(analysis_grounding, Mapping):
                            updated[
                                "last_analysis_grounding_manifest_sha256"
                            ] = analysis_grounding[
                                "mcp_grounding_manifest_sha256"
                            ]
                            updated["last_analysis_evidence_refs"] = list(
                                analysis_grounding["grounded_evidence_refs"]
                            )
                        critical_grounding = mcp_stage_grounding.get(
                            "critical_review"
                        )
                        if isinstance(critical_grounding, Mapping):
                            updated[
                                "last_critical_review_grounding_manifest_sha256"
                            ] = critical_grounding[
                                "mcp_grounding_manifest_sha256"
                            ]
                            updated[
                                "last_critical_review_evidence_refs"
                            ] = list(
                                critical_grounding["grounded_evidence_refs"]
                            )
                if emergency_cancelled:
                    updated.update(
                        {
                            "last_emergency_cancel_at": timestamp,
                            "last_emergency_cancel_receipt_sha256": (
                                terminal_sha256
                            ),
                            "late_result_fence_status": "sealed",
                        }
                    )
            updated = self._refresh_canary_queue_projection_locked(updated)
            self._write_canary_state_locked(updated, updated_at=timestamp)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"]), updated_at=timestamp
            )
            return {
                "terminal_receipt_sha256": terminal_sha256,
                "terminal_receipt_path": str(terminal_path),
                "state": updated["state"],
                "queue_depth": updated["queue_depth"],
            }

    def fail_production_canary_closed(
        self, task: FrozenTask, *, error_code: str
    ) -> dict[str, Any]:
        """Force a claimed canary slot into a durable subject-local pause.

        This recovery path is intentionally smaller than normal terminal
        verification.  It is used only when that verifier itself fails, so a
        malformed late result can never strand the gate in-flight or reopen
        the consumer.
        """

        subject = str(task.frozen_payload.get("subject") or "")
        timestamp = _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            contract = self._producer_contract_from_task(task)
            contract_sha256 = str(
                contract["producer_input_contract_sha256"]
            )
            active_selections = copy.deepcopy(
                dict(state.get("active_selections") or {})
            )
            active_selected = active_selections.get(task.unit_sha256)
            if not isinstance(active_selected, Mapping):
                raise DispatchError(
                    "production_canary_fail_closed_active_binding_missing"
                )
            queue_entry = (
                self._read_object(
                    self._production_canary_queue_path(
                        subject, contract_sha256
                    )
                )
                if len(contract_sha256) == 64
                else None
            )
            if not isinstance(queue_entry, Mapping):
                raise DispatchError(
                    "production_canary_fail_closed_queue_missing"
                )
            self._verify_seal(
                queue_entry, purpose="dispatch-production-canary-queue"
            )
            selected = {
                "producer_unit_id": queue_entry.get("producer_unit_id"),
                "producer_recorded_at": queue_entry.get("producer_recorded_at"),
                "producer_input_contract_sha256": queue_entry.get(
                    "producer_input_contract_sha256"
                ),
                "source_event_set_sha256": queue_entry.get(
                    "source_event_set_sha256"
                ),
                "unit_sha256": queue_entry.get("unit_sha256"),
                "frozen_payload_sha256": queue_entry.get(
                    "frozen_payload_sha256"
                ),
                "canary_gate_sha256": queue_entry.get("canary_gate_sha256"),
                "canary_gate_path": queue_entry.get("canary_gate_path"),
                "canary_gate_authority_sha256": queue_entry.get(
                    "canary_gate_authority_sha256"
                ),
                "lease_owner_id": queue_entry.get("lease_owner_id"),
                "lease_fence": queue_entry.get("lease_fence"),
                "context_root": queue_entry.get("context_root"),
                "mcp_session_root": queue_entry.get("mcp_session_root"),
                "report_root": queue_entry.get("report_root"),
                "process_identity_sha256": queue_entry.get(
                    "process_identity_sha256"
                ),
                "process_identity_path": queue_entry.get(
                    "process_identity_path"
                ),
            }
            if any(
                active_selected.get(key) != value
                for key, value in selected.items()
            ):
                raise DispatchError(
                    "production_canary_fail_closed_active_binding_invalid"
                )
            recovery_core = {
                "schema_version": PRODUCTION_CANARY_TERMINAL_SCHEMA,
                "counter_scope": "current_control_plane_operation",
                "terminal_kind": "fail_closed_recovery",
                "late_result_fenced": True,
                "activation_id": state["activation_id"],
                "subject": subject,
                "release_id": state["release_id"],
                "producer_authority_fingerprint": state[
                    "producer_authority_fingerprint"
                ],
                "producer_high_watermark_sha256": state[
                    "producer_high_watermark_sha256"
                ],
                "activation_receipt_sha256": state[
                    "activation_receipt_sha256"
                ],
                "activation_gate_authority_sha256": state[
                    "activation_gate_authority_sha256"
                ],
                "canary_gate_sha256": selected["canary_gate_sha256"],
                "canary_gate_authority_sha256": queue_entry.get(
                    "canary_gate_authority_sha256"
                ),
                "selected": {
                    key: selected[key]
                    for key in (
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
                        "process_identity_sha256",
                        "process_identity_path",
                    )
                },
                "outcome": "failed",
                "error_code": error_code,
                "completion_path": None,
                "completion_sha256": None,
                "completion_receipt_path": None,
                "completion_receipt_sha256": None,
                "package_path": None,
                "package_sha256": None,
                "package_ref": None,
                "report_json_ref": None,
                "report_json_sha256": None,
                "report_markdown_ref": None,
                "report_markdown_sha256": None,
                "report_reopen_status": "not_available",
                "observed_model_call_count": 0,
                "observed_provider_request_count": 0,
                "observed_mcp_tool_call_count": 0,
                "read_session_id": None,
                "evidence_generation": None,
                "evidence_authority_fingerprint": None,
                "task_declared_evidence_refs_sha256": None,
                "mcp_stage_grounding": None,
                "process_execution": None,
                "finished_at": timestamp,
                "state_after": "failed_drained",
                **self._canary_control_fields(
                    int(state["continuous_concurrency_limit"])
                ),
            }
            terminal_sha256, terminal_path, _ = (
                self._publish_canary_receipt_locked(
                    subject,
                    str(state["activation_id"]),
                    recovery_core,
                    purpose="dispatch-production-canary-terminal",
                )
            )
            terminal_index_fields = self._append_canary_terminal_index_locked(
                state,
                task,
                queue_entry=queue_entry,
                terminal_receipt_sha256=terminal_sha256,
                terminal_receipt_path=str(terminal_path),
                outcome="failed",
                error_code=error_code,
                finished_at=timestamp,
                terminal_kind="fail_closed_recovery",
            )
            self._update_canary_queue_entry_locked(
                queue_entry,
                queue_status="failed",
                finished_at=timestamp,
                terminal_receipt_sha256=terminal_sha256,
                terminal_receipt_path=str(terminal_path),
                terminal_outcome="failed",
                terminal_error_code=error_code,
            )
            updated = dict(state)
            updated.update(terminal_index_fields)
            active_selections.pop(task.unit_sha256, None)
            updated.update(
                {
                    "state": "failed_drained",
                    "luna_consumer_enabled": False,
                    "active_task_count": len(active_selections),
                    "active_selections": active_selections,
                    "selected": None,
                    "last_failure_at": timestamp,
                    "blocking_reason": error_code,
                    "next_action": "explicit_subject_resume_required",
                    "last_terminal_receipt_sha256": terminal_sha256,
                    "last_terminal_receipt_path": str(terminal_path),
                    "last_analysis_status": "failed_or_incomplete",
                    "last_critical_review_status": "failed_or_incomplete",
                    "last_report_status": "not_verified",
                    "backpressure_reason": None,
                }
            )
            updated = self._refresh_canary_queue_projection_locked(updated)
            self._write_canary_state_locked(updated, updated_at=timestamp)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"]), updated_at=timestamp
            )
            return {
                "terminal_receipt_sha256": terminal_sha256,
                "terminal_receipt_path": str(terminal_path),
                "state": "failed_drained",
            }

    def fail_production_canary_post_terminal(
        self, task: FrozenTask, *, error_code: str
    ) -> dict[str, Any]:
        """Pause one unlocked consumer if downstream projection fails."""

        timestamp = _utc_now()
        subject = str(task.frozen_payload.get("subject") or "")
        with _ExclusiveFileLock(self.lock_path):
            state = self._read_canary_state_locked(subject)
            assert state is not None
            contract = self._producer_contract_from_task(task)
            contract_sha256 = str(
                contract["producer_input_contract_sha256"]
            )
            queue_entry = self._read_object(
                self._production_canary_queue_path(subject, contract_sha256)
            )
            if not isinstance(queue_entry, Mapping):
                raise DispatchError(
                    "production_canary_post_terminal_binding_missing"
                )
            self._verify_seal(
                queue_entry, purpose="dispatch-production-canary-queue"
            )
            selected = {
                key: queue_entry.get(key)
                for key in (
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
                    "process_identity_sha256",
                    "process_identity_path",
                    "canary_gate_sha256",
                    "canary_gate_path",
                    "canary_gate_authority_sha256",
                )
            }
            prior_terminal_receipt_sha256 = queue_entry.get(
                "terminal_receipt_sha256"
            )
            if (
                selected.get("unit_sha256") != task.unit_sha256
                or selected.get("producer_input_contract_sha256")
                != contract_sha256
                or not isinstance(prior_terminal_receipt_sha256, str)
            ):
                raise DispatchError(
                    "production_canary_post_terminal_binding_invalid"
                )
            prior_terminal_path = Path(
                str(queue_entry.get("terminal_receipt_path") or "")
            )
            try:
                prior_terminal_bytes = prior_terminal_path.read_bytes()
                prior_terminal_path.resolve(strict=True).relative_to(
                    self.production_canary_receipt_root.resolve()
                )
            except (OSError, ValueError) as exc:
                raise DispatchError(
                    "production_canary_post_terminal_binding_invalid"
                ) from exc
            prior_terminal = self._read_object(prior_terminal_path)
            if (
                _sha256_bytes(prior_terminal_bytes)
                != prior_terminal_receipt_sha256
                or not isinstance(prior_terminal, Mapping)
            ):
                raise DispatchError(
                    "production_canary_post_terminal_binding_invalid"
                )
            self._verify_seal(
                prior_terminal,
                purpose="dispatch-production-canary-terminal",
            )
            if (
                prior_terminal.get("activation_id") != state.get("activation_id")
                or prior_terminal.get("release_id") != state.get("release_id")
                or prior_terminal.get("subject") != subject
                or (prior_terminal.get("selected") or {}).get("unit_sha256")
                != task.unit_sha256
            ):
                raise DispatchError(
                    "production_canary_post_terminal_binding_invalid"
                )
            prior_report_fields = {
                key: prior_terminal.get(key)
                for key in (
                    "package_path",
                    "package_sha256",
                    "package_ref",
                    "report_json_ref",
                    "report_json_sha256",
                    "report_markdown_ref",
                    "report_markdown_sha256",
                    "report_reopen_status",
                )
            }
            if prior_report_fields["report_reopen_status"] not in {
                "not_available",
                "json_markdown_package_verified",
            }:
                raise DispatchError(
                    "production_canary_post_terminal_binding_invalid"
                )
            receipt_core = {
                "schema_version": PRODUCTION_CANARY_TERMINAL_SCHEMA,
                "counter_scope": "current_control_plane_operation",
                "terminal_kind": "post_terminal_projection_failure",
                "late_result_fenced": True,
                "activation_id": state["activation_id"],
                "subject": subject,
                "release_id": state["release_id"],
                "producer_authority_fingerprint": state[
                    "producer_authority_fingerprint"
                ],
                "producer_high_watermark_sha256": state[
                    "producer_high_watermark_sha256"
                ],
                "activation_receipt_sha256": state[
                    "activation_receipt_sha256"
                ],
                "activation_gate_authority_sha256": state[
                    "activation_gate_authority_sha256"
                ],
                "canary_gate_sha256": selected["canary_gate_sha256"],
                "canary_gate_authority_sha256": selected[
                    "canary_gate_authority_sha256"
                ],
                "selected": {
                    key: selected[key]
                    for key in (
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
                        "process_identity_sha256",
                        "process_identity_path",
                    )
                },
                "prior_terminal_receipt_sha256": (
                    prior_terminal_receipt_sha256
                ),
                "outcome": "failed",
                "error_code": error_code,
                "completion_path": None,
                "completion_sha256": None,
                "completion_receipt_path": None,
                "completion_receipt_sha256": None,
                **prior_report_fields,
                "observed_model_call_count": 0,
                "observed_provider_request_count": 0,
                "observed_mcp_tool_call_count": 0,
                "read_session_id": None,
                "evidence_generation": None,
                "evidence_authority_fingerprint": None,
                "task_declared_evidence_refs_sha256": None,
                "mcp_stage_grounding": None,
                "process_execution": None,
                "finished_at": timestamp,
                "state_after": "failed_drained",
                **self._canary_control_fields(
                    int(state["continuous_concurrency_limit"])
                ),
            }
            receipt_sha256, receipt_path, _ = self._publish_canary_receipt_locked(
                subject,
                str(state["activation_id"]),
                receipt_core,
                purpose="dispatch-production-canary-terminal",
            )
            terminal_index_fields = self._append_canary_terminal_index_locked(
                state,
                task,
                queue_entry=queue_entry,
                terminal_receipt_sha256=receipt_sha256,
                terminal_receipt_path=str(receipt_path),
                outcome="failed",
                error_code=error_code,
                finished_at=timestamp,
                terminal_kind="post_terminal_projection_failure",
                prior_terminal_receipt_sha256=(
                    prior_terminal_receipt_sha256
                ),
            )
            self._update_canary_queue_entry_locked(
                queue_entry,
                terminal_receipt_sha256=receipt_sha256,
                terminal_receipt_path=str(receipt_path),
                terminal_outcome="failed",
                terminal_error_code=error_code,
            )
            updated = dict(state)
            updated.update(terminal_index_fields)
            updated.update(
                {
                    "state": "failed_drained",
                    "luna_consumer_enabled": False,
                    "selected": None,
                    "last_failure_at": timestamp,
                    "blocking_reason": error_code,
                    "next_action": "explicit_subject_resume_required",
                    "last_terminal_receipt_sha256": receipt_sha256,
                    "last_terminal_receipt_path": str(receipt_path),
                    "backpressure_reason": None,
                }
            )
            updated = self._refresh_canary_queue_projection_locked(updated)
            self._write_canary_state_locked(updated, updated_at=timestamp)
            self._recompute_canary_concurrency_telemetry_locked(
                str(state["release_id"]), updated_at=timestamp
            )
            return {
                "terminal_receipt_sha256": receipt_sha256,
                "terminal_receipt_path": str(receipt_path),
                "state": "failed_drained",
            }

    def prepare_controlled_replay_allowlist(
        self,
        *,
        release_id: str,
        tasks: Sequence[FrozenTask],
        max_age_seconds: int = 21600,
    ) -> dict[str, Any]:
        """Seal an exact, bounded one-shot replay list without opening claims."""

        _validate_unit_sha256(release_id)
        if (
            not tasks
            or len(tasks) > 16
            or isinstance(max_age_seconds, bool)
            or not isinstance(max_age_seconds, int)
            or not 60 <= max_age_seconds <= 21600
        ):
            raise DispatchError("controlled_replay_allowlist_invalid")
        rows: list[dict[str, Any]] = []
        seen_units: set[str] = set()
        for task in tasks:
            payload = task.frozen_payload
            contract = payload.get("dispatch_contract")
            subject = payload.get("subject")
            capture_ids = payload.get("content_group_capture_ids")
            if capture_ids is None:
                capture_ids = [payload.get("capture_id")]
            if (
                task.unit_sha256 in seen_units
                or subject not in {"math", "cs408", "english"}
                or not isinstance(contract, Mapping)
                or contract.get("release_id") != release_id
                or not isinstance(capture_ids, list)
                or not capture_ids
                or any(not isinstance(item, str) or not item for item in capture_ids)
                or len(capture_ids) != len(set(capture_ids))
            ):
                raise DispatchError("controlled_replay_task_invalid")
            seen_units.add(task.unit_sha256)
            rows.append(
                {
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "subject": subject,
                    "capture_ids": list(capture_ids),
                    "input_fingerprint": payload.get("input_fingerprint"),
                    "content_processing_id": payload.get("content_processing_id"),
                }
            )
        created = dt.datetime.now(dt.timezone.utc)
        core = {
            "schema_version": CONTROLLED_REPLAY_ALLOWLIST_SCHEMA,
            "status": "prepared",
            "release_id": release_id,
            "created_at": created.isoformat().replace("+00:00", "Z"),
            "expires_at": (
                created + dt.timedelta(seconds=max_age_seconds)
            ).isoformat().replace("+00:00", "Z"),
            "tasks": rows,
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "general_claims_remain_unchanged": True,
            "formal_write_count": 0,
        }
        sealed = self._seal(core, purpose=CONTROLLED_REPLAY_PURPOSE)
        digest, path = _publish_content_addressed(
            self.controlled_replay_root, sealed
        )
        return {
            "allowlist": sealed,
            "allowlist_sha256": digest,
            "allowlist_path": str(path),
        }

    def reopen_authorized_english_preserved_review_repair(
        self,
        *,
        repair_receipt_sha256: str | None = None,
        target_release_id: str | None = None,
        target_activation_id: str | None = None,
        target_generation: str | None = None,
        target_subject_authority_fingerprint: str | None = None,
        target_producer_authority_fingerprint: str | None = None,
        _lock_held: bool = False,
    ) -> dict[str, Any]:
        """Read-only reopen, including an uncertain post-apply lookup."""

        lock_context = (
            contextlib.nullcontext()
            if _lock_held
            else _ExistingSharedFileLock(self.lock_path)
        )
        with lock_context:
            pointer = self._read_object(
                self.english_preserved_review_repair_pointer_path
            )
            intent = self._read_object(
                self.english_preserved_review_repair_intent_path
            )
            if pointer is None:
                if intent is None:
                    auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
                    source_queue_path = (
                        self.production_canary_queue_root
                        / "english"
                        / auth["source_activation_id"]
                        / f"{auth['producer_input_contract_sha256']}.json"
                    )
                    batch_path = (
                        self.state_root / "subject-luna-batches" / "english.json"
                    )
                    batch_pointer_path = (
                        self.state_root
                        / "subject-luna-batch-pointers"
                        / "english.json"
                    )
                    writer_path = self.state_root / "subject-sol" / "english.json"
                    retirement_pointer_path = (
                        self.state_root
                        / "english-preserved-review-batch-retirements"
                        / "authorized-33548.json"
                    )
                    staged_state = self._read_object(
                        self._production_canary_state_path("english")
                    )
                    target_queue_path = (
                        self.production_canary_queue_root
                        / "english"
                        / str(
                            staged_state.get("activation_id")
                            if isinstance(staged_state, Mapping)
                            else "invalid"
                        )
                        / f"{auth['producer_input_contract_sha256']}.json"
                    )
                    no_mutation = (
                        isinstance(staged_state, Mapping)
                        and staged_state.get("state") == "paused_drained"
                        and staged_state.get("luna_consumer_enabled") is False
                        and source_queue_path.is_file()
                        and not target_queue_path.exists()
                        and _sha256_bytes(source_queue_path.read_bytes())
                        == auth["queue_preimage_sha256"]
                        and _sha256_bytes(batch_path.read_bytes())
                        == auth["subject_batch_preimage_sha256"]
                        and _sha256_bytes(batch_pointer_path.read_bytes())
                        == auth["subject_batch_pointer_preimage_sha256"]
                        and _sha256_bytes(writer_path.read_bytes())
                        == auth["subject_writer_preimage_sha256"]
                        and not retirement_pointer_path.exists()
                    )
                    if not no_mutation:
                        raise DispatchError(
                            "english_preserved_review_repair_fail_fenced"
                        )
                    return {
                        "schema_version": (
                            "study-intake-english-preserved-review-repair-"
                            "uncertain-reopen-v1"
                        ),
                        "status": "not_prepared_no_mutation",
                        "repair_receipt_sha256": None,
                        "repair_receipt_path": None,
                        "mutation_observed": False,
                        "rollback_complete": True,
                        "formal_write_count": 0,
                    }
                self._verify_seal(
                    intent,
                    purpose=(
                        "dispatch-english-preserved-review-repair-intent-v1"
                    ),
                )
                status = str(intent.get("status") or "")
                if status not in {
                    "prepared",
                    "rolled_back_same_process",
                    "rolled_back",
                }:
                    raise DispatchError(
                        "english_preserved_review_repair_intent_invalid"
                    )
                rollback_complete = False
                if status == "rolled_back_same_process":
                    capsule = (
                        self._read_english_preserved_review_preimage_capsule(
                            digest=str(
                                intent.get("preimage_capsule_sha256") or ""
                            ),
                            path_value=str(
                                intent.get("preimage_capsule_path") or ""
                            ),
                        )
                    )
                    auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
                    source_queue_path = Path(str(capsule["source_queue_path"]))
                    target_queue_path = Path(str(capsule["target_queue_path"]))
                    staged_gate_path = Path(str(capsule["staged_gate_path"]))
                    batch_path = (
                        self.state_root / "subject-luna-batches" / "english.json"
                    )
                    batch_pointer_path = (
                        self.state_root
                        / "subject-luna-batch-pointers"
                        / "english.json"
                    )
                    writer_path = (
                        self.state_root / "subject-sol" / "english.json"
                    )
                    retirement_pointer_path = (
                        self.state_root
                        / "english-preserved-review-batch-retirements"
                        / "authorized-33548.json"
                    )
                    rollback_complete = (
                        source_queue_path.is_file()
                        and not target_queue_path.exists()
                        and _sha256_bytes(source_queue_path.read_bytes())
                        == auth["queue_preimage_sha256"]
                        and _sha256_bytes(staged_gate_path.read_bytes())
                        == capsule["staged_gate_preimage_sha256"]
                        and _sha256_bytes(batch_path.read_bytes())
                        == auth["subject_batch_preimage_sha256"]
                        and _sha256_bytes(batch_pointer_path.read_bytes())
                        == auth["subject_batch_pointer_preimage_sha256"]
                        and _sha256_bytes(writer_path.read_bytes())
                        == auth["subject_writer_preimage_sha256"]
                        and not retirement_pointer_path.exists()
                        and not self.english_preserved_review_repair_pointer_path.exists()
                    )
                    if not rollback_complete:
                        raise DispatchError(
                            "english_preserved_review_repair_fail_fenced"
                        )
                return {
                    "schema_version": (
                        "study-intake-english-preserved-review-repair-"
                        "uncertain-reopen-v1"
                    ),
                    "status": status,
                    "repair_receipt_sha256": intent.get(
                        "repair_receipt_sha256"
                    ),
                    "repair_receipt_path": intent.get(
                        "repair_receipt_path"
                    ),
                    "preimage_capsule_sha256": intent.get(
                        "preimage_capsule_sha256"
                    ),
                    "preimage_capsule_path": intent.get(
                        "preimage_capsule_path"
                    ),
                    "mutation_observed": (
                        status not in {
                            "rolled_back_same_process",
                            "rolled_back",
                        }
                    ),
                    "rollback_complete": rollback_complete,
                    "formal_write_count": 0,
                }
            self._verify_seal(
                pointer,
                purpose="dispatch-english-preserved-review-repair-pointer-v1",
            )
            if not isinstance(intent, Mapping):
                raise DispatchError(
                    "english_preserved_review_repair_intent_invalid"
                )
            self._verify_seal(
                intent,
                purpose=(
                    "dispatch-english-preserved-review-repair-intent-v1"
                ),
            )
            digest = _validate_unit_sha256(
                str(pointer.get("repair_receipt_sha256") or "")
            )
            if (
                intent.get("status") != "applied"
                or intent.get("repair_receipt_sha256") != digest
                or intent.get("repair_receipt_path")
                != pointer.get("repair_receipt_path")
                or any(
                    intent.get(key) != pointer.get(key)
                    for key in (
                        "target_release_id",
                        "target_activation_id",
                        "target_generation",
                        "target_subject_authority_fingerprint",
                        "target_producer_authority_fingerprint",
                    )
                )
            ):
                raise DispatchError(
                    "english_preserved_review_repair_intent_invalid"
                )
            if repair_receipt_sha256 is not None and digest != (
                _validate_unit_sha256(repair_receipt_sha256)
            ):
                raise DispatchError(
                    "english_preserved_review_repair_receipt_mismatch"
                )
            receipt_path = Path(str(pointer.get("repair_receipt_path") or ""))
            expected_path = (
                self.english_preserved_review_repair_receipt_root
                / "sha256"
                / digest[:2]
                / f"{digest}.json"
            )
            if receipt_path.resolve() != expected_path.resolve():
                raise DispatchError(
                    "english_preserved_review_repair_receipt_path_invalid"
                )
            _payload, receipt = self._exact_preserved_repair_file(
                receipt_path, digest, "repair_receipt"
            )
            self._verify_seal(
                receipt,
                purpose="dispatch-english-preserved-review-repair-v1",
            )
            auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
            evidence_bindings = receipt.get("evidence_bindings")
            mutable_preimages = receipt.get("mutable_preimages")
            review_artifacts = receipt.get("review_artifacts")
            if (
                receipt.get("authorization_descriptor_sha256")
                != _sha256_bytes(_canonical_bytes(auth))
                or receipt.get("subject") != "english"
                or receipt.get("source_release_id")
                != auth["source_release_id"]
                or receipt.get("source_activation_id")
                != auth["source_activation_id"]
                or receipt.get("capture_id") != auth["capture_id"]
                or receipt.get("unit_sha256") != auth["unit_sha256"]
                or receipt.get("frozen_payload_sha256")
                != auth["frozen_payload_sha256"]
                or receipt.get("producer_input_contract_sha256")
                != auth["producer_input_contract_sha256"]
                or not isinstance(evidence_bindings, Mapping)
                or any(
                    evidence_bindings.get(key) != auth[key]
                    for key in (
                        "original_preclaim_failure_receipt_sha256",
                        "rollover_receipt_sha256",
                        "recovery_supersede_receipt_sha256",
                        "task_object_sha256",
                        "raw_output_object_sha256",
                        "mcp_transport_sha256",
                        "duplicate_result_projection_sha256",
                        "stage_execution_receipt_sha256",
                        "processing_receipt_sha256",
                        "mcp_failure_receipt_sha256",
                        "canary_terminal_receipt_sha256",
                        "subject_terminal_receipt_sha256",
                    )
                )
                or not isinstance(mutable_preimages, Mapping)
                or any(
                    mutable_preimages.get(key) != auth[key]
                    for key in (
                        "queue_preimage_sha256",
                        "gate_preimage_sha256",
                        "terminal_index_preimage_sha256",
                        "subject_batch_preimage_sha256",
                        "subject_batch_pointer_preimage_sha256",
                        "subject_writer_preimage_sha256",
                    )
                )
                or not isinstance(review_artifacts, Mapping)
            ):
                raise DispatchError(
                    "english_preserved_review_repair_receipt_binding_invalid"
                )
            capsule = self._read_english_preserved_review_preimage_capsule(
                digest=str(receipt.get("preimage_capsule_sha256") or ""),
                path_value=str(receipt.get("preimage_capsule_path") or ""),
            )
            if (
                intent.get("preimage_capsule_sha256")
                != receipt.get("preimage_capsule_sha256")
                or intent.get("preimage_capsule_path")
                != receipt.get("preimage_capsule_path")
                or capsule.get("target_release_id")
                != receipt.get("target_release_id")
                or capsule.get("target_activation_id")
                != receipt.get("target_activation_id")
            ):
                raise DispatchError(
                    "english_preserved_review_repair_capsule_binding_invalid"
                )
            batch_archive_path = Path(
                str(receipt.get("batch_archive_path") or "")
            )
            _batch_archive_bytes, batch_archive = (
                self._exact_preserved_repair_file(
                    batch_archive_path,
                    str(receipt.get("batch_archive_sha256") or ""),
                    "batch_archive",
                )
            )
            self._verify_exact_preserved_legacy_seal(
                batch_archive,
                purpose=(
                    "english-preserved-review-batch-retirement-archive-v1"
                ),
            )
            retirement_pointer_path = (
                self.state_root
                / "english-preserved-review-batch-retirements"
                / "authorized-33548.json"
            )
            _retirement_pointer_bytes, retirement_pointer = (
                self._exact_preserved_repair_file(
                    retirement_pointer_path,
                    str(receipt.get("batch_retirement_pointer_sha256") or ""),
                    "batch_retirement_pointer",
                )
            )
            self._verify_exact_preserved_legacy_seal(
                retirement_pointer,
                purpose=(
                    "english-preserved-review-batch-retirement-pointer-v1"
                ),
            )
            writer_path = self.state_root / "subject-sol" / "english.json"
            if (
                batch_archive.get("batch_sha256")
                != receipt.get("batch_postimage_sha256")
                or batch_archive.get("batch_pointer_sha256")
                != receipt.get("batch_pointer_postimage_sha256")
                or retirement_pointer.get("archive_sha256")
                != receipt.get("batch_archive_sha256")
                or retirement_pointer.get("writer_postimage_sha256")
                != receipt.get("writer_postimage_sha256")
                or _sha256_bytes(writer_path.read_bytes())
                != receipt.get("writer_postimage_sha256")
            ):
                raise DispatchError(
                    "english_preserved_review_repair_batch_postimage_invalid"
                )
            for sha_key, path_key, suffix in (
                ("package_sha256", "package_path", ".json"),
                ("report_json_sha256", "report_json_path", ".json"),
                ("report_markdown_sha256", "report_markdown_path", ".md"),
                ("mcp_transcript_sha256", "mcp_transcript_path", ".json"),
                (
                    "normalization_receipt_sha256",
                    "normalization_receipt_path",
                    ".json",
                ),
            ):
                artifact_path = Path(str(review_artifacts.get(path_key) or ""))
                artifact_sha = str(review_artifacts.get(sha_key) or "")
                _validate_unit_sha256(artifact_sha)
                if (
                    not artifact_path.is_file()
                    or artifact_path.suffix != suffix
                    or _sha256_bytes(artifact_path.read_bytes()) != artifact_sha
                ):
                    raise DispatchError(
                        "english_preserved_review_repair_artifact_invalid"
                    )
            normalization = self._read_object(
                Path(str(review_artifacts["normalization_receipt_path"]))
            )
            transcript = self._read_object(
                Path(str(review_artifacts["mcp_transcript_path"]))
            )
            package = self._read_object(
                Path(str(review_artifacts["package_path"]))
            )
            report = self._read_object(
                Path(str(review_artifacts["report_json_path"]))
            )
            markdown_path = Path(
                str(review_artifacts["report_markdown_path"])
            )
            review_terminal_path = Path(
                str(receipt.get("review_terminal_path") or "")
            )
            _review_terminal_bytes, review_terminal = (
                self._exact_preserved_repair_file(
                    review_terminal_path,
                    str(receipt.get("review_terminal_sha256") or ""),
                    "review_terminal",
                )
            )
            self._verify_seal(
                review_terminal,
                purpose="dispatch-production-canary-terminal",
            )
            terminal_index_path = Path(
                str(receipt.get("terminal_index_postimage_path") or "")
            )
            _terminal_index_bytes, terminal_index = (
                self._exact_preserved_repair_file(
                    terminal_index_path,
                    str(receipt.get("terminal_index_postimage_sha256") or ""),
                    "terminal_index_postimage",
                )
            )
            self._verify_seal(
                terminal_index,
                purpose="dispatch-production-canary-terminal-index",
            )
            terminal_unit = (
                terminal_index.get("units", {}).get(auth["unit_sha256"])
                if isinstance(terminal_index.get("units"), Mapping)
                else None
            )
            terminal_history = (
                terminal_unit.get("history")
                if isinstance(terminal_unit, Mapping)
                else None
            )
            if (
                normalization is None
                or transcript is None
                or package is None
                or report is None
                or not markdown_path.read_bytes()
                or normalization.get("normalization_status")
                != "normalized_with_warnings"
                or normalization.get("execution_receipt_sha256")
                != auth["stage_execution_receipt_sha256"]
                or normalization.get("raw_output_object_sha256")
                != auth["raw_output_object_sha256"]
                or transcript.get("schema_version")
                != "model-driven-mcp-stage-transcript-v1"
                or transcript.get("mcp_tool_call_count") != 40
                or transcript.get("coverage", {}).get(
                    "duplicate_argument_count"
                ) != 1
                or package.get("schema_version") != REVIEW_PACKAGE_SCHEMA
                or report.get("schema_version")
                != "study-intake-review-candidate-terminal-v1"
                or report.get("package_sha256")
                != review_artifacts["package_sha256"]
                or any(
                    report.get(key) != expected
                    for key, expected in (
                        ("report_available", True),
                        ("report_disposition", "needs_sol_review"),
                        ("sol_review_status", "pending"),
                        ("formal_write_eligible", False),
                        ("formal_write_count", 0),
                    )
                )
                or review_terminal.get("schema_version")
                != PRODUCTION_CANARY_REVIEW_TERMINAL_SCHEMA
                or review_terminal.get("release_id")
                != receipt.get("target_release_id")
                or review_terminal.get("activation_id")
                != receipt.get("target_activation_id")
                or review_terminal.get("outcome") != "needs_rework"
                or review_terminal.get("producer_authority_fingerprint")
                != receipt.get("target_producer_authority_fingerprint")
                or review_terminal.get("completion_receipt_sha256")
                != auth["processing_receipt_sha256"]
                or review_terminal.get("package_sha256")
                != review_artifacts["package_sha256"]
                or review_terminal.get("report_json_sha256")
                != review_artifacts["report_json_sha256"]
                or review_terminal.get("report_markdown_sha256")
                != review_artifacts["report_markdown_sha256"]
                or not isinstance(terminal_unit, Mapping)
                or not isinstance(terminal_history, list)
                or len(terminal_history) < 2
                or terminal_unit.get("terminal_receipt_sha256")
                != receipt.get("review_terminal_sha256")
                or terminal_unit.get("terminal_receipt_path")
                != receipt.get("review_terminal_path")
                or terminal_history[-1].get("terminal_receipt_sha256")
                != receipt.get("review_terminal_sha256")
                or terminal_history[-1].get("prior_terminal_receipt_sha256")
                != auth["canary_terminal_receipt_sha256"]
                or not any(
                    item.get("terminal_receipt_sha256")
                    == auth["canary_terminal_receipt_sha256"]
                    for item in terminal_history[:-1]
                    if isinstance(item, Mapping)
                )
            ):
                raise DispatchError(
                    "english_preserved_review_repair_artifact_invalid"
                )
            self._verify_seal(
                normalization,
                purpose="study-intake-model-stage-normalization",
            )
            expected_values = (
                ("target_release_id", target_release_id),
                ("target_activation_id", target_activation_id),
                ("target_generation", target_generation),
                (
                    "target_subject_authority_fingerprint",
                    target_subject_authority_fingerprint,
                ),
                (
                    "target_producer_authority_fingerprint",
                    target_producer_authority_fingerprint,
                ),
            )
            if any(
                expected is not None and receipt.get(key) != expected
                for key, expected in expected_values
            ):
                raise DispatchError(
                    "english_preserved_review_repair_target_binding_mismatch"
                )
            source_queue_path = Path(str(receipt["source_queue_path"]))
            target_queue_path = Path(str(receipt["target_queue_path"]))
            state_path = self._production_canary_state_path("english")
            target_queue = self._read_object(target_queue_path)
            state = self._read_object(state_path)
            if (
                source_queue_path.exists()
                or target_queue is None
                or state is None
                or _sha256_bytes(target_queue_path.read_bytes())
                != receipt["queue_postimage_sha256"]
                or _sha256_bytes(state_path.read_bytes())
                != receipt["gate_postimage_sha256"]
                or target_queue.get("schema_version")
                != PRODUCTION_CANARY_REVIEW_QUEUE_SCHEMA
                or target_queue.get("queue_status") != "needs_sol_review"
                or target_queue.get("unit_sha256") != auth["unit_sha256"]
                or target_queue.get("producer_input_contract_sha256")
                != auth["producer_input_contract_sha256"]
                or target_queue.get("terminal_receipt_sha256")
                != receipt.get("review_terminal_sha256")
                or target_queue.get("terminal_receipt_path")
                != receipt.get("review_terminal_path")
                or state.get("state") != "armed"
                or state.get("luna_consumer_enabled") is not True
                or state.get("terminal_index_sha256")
                != receipt["terminal_index_postimage_sha256"]
                or state.get("terminal_index_path")
                != receipt["terminal_index_postimage_path"]
                or receipt.get("source_queue_absent") is not True
                or receipt.get("queue_identity_preserved") is not True
                or any(
                    receipt.get(key) != 0
                    for key in (
                        "new_task_count",
                        "new_queue_count",
                        "capture_replay_count",
                        "model_call_count",
                        "provider_request_count",
                        "mcp_tool_call_count",
                        "sol_call_count",
                        "formal_write_count",
                    )
                )
            ):
                raise DispatchError(
                    "english_preserved_review_repair_postimage_invalid"
                )
            self._verify_seal(
                target_queue, purpose="dispatch-production-canary-queue"
            )
            self._verify_seal(
                state, purpose="dispatch-production-canary-state"
            )
            try:
                from subject_sol_contract import SubjectSolRuntimeStore

                restricted = SubjectSolRuntimeStore(
                    self.runtime_root
                ).read_restricted_sol_review_candidate(
                    str(review_artifacts["report_json_sha256"])
                )
            except Exception as exc:
                raise DispatchError(
                    "english_preserved_review_repair_restricted_sol_unreadable"
                ) from exc
            if (
                restricted.get("report_available") is not True
                or restricted.get("sol_review_status") != "pending"
                or restricted.get("formal_write_eligible") is not False
                or restricted.get("automatic_formal_write") is not False
                or restricted.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "english_preserved_review_repair_restricted_sol_unreadable"
                )
            return {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-reopen-v1"
                ),
                "status": "reopened",
                "repair_receipt": receipt,
                "repair_receipt_sha256": digest,
                "repair_receipt_path": str(receipt_path),
                "source_queue_absent": True,
                "target_queue_path": str(target_queue_path),
                "target_queue_sha256": receipt["queue_postimage_sha256"],
                "queue_identity_preserved": True,
                "report_sha256": receipt["review_artifacts"][
                    "report_json_sha256"
                ],
                "report_available": True,
                "sol_review_status": "pending",
                "formal_write_eligible": False,
                "restricted_sol_readable": True,
                "new_task_count": 0,
                "new_queue_count": 0,
                "capture_replay_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "sol_call_count": 0,
                "formal_write_count": 0,
            }
    def _consume_controlled_replay_allowlist(
        self,
        task: FrozenTask,
        authority: Mapping[str, Any],
    ) -> None:
        self._verify_seal(authority, purpose=CONTROLLED_REPLAY_PURPOSE)
        try:
            created = _parse_utc(authority.get("created_at"))
            expires = _parse_utc(authority.get("expires_at"))
        except DispatchError as exc:
            raise DispatchError("controlled_replay_allowlist_invalid") from exc
        now = dt.datetime.now(dt.timezone.utc)
        payload = task.frozen_payload
        contract = payload.get("dispatch_contract")
        tasks = authority.get("tasks")
        matching = [
            row
            for row in tasks
            if isinstance(row, Mapping)
            and row.get("unit_sha256") == task.unit_sha256
        ] if isinstance(tasks, list) else []
        if (
            authority.get("schema_version") != CONTROLLED_REPLAY_ALLOWLIST_SCHEMA
            or authority.get("status") != "prepared"
            or authority.get("model") != REQUIRED_MODEL
            or authority.get("reasoning_effort") != REQUIRED_REASONING_EFFORT
            or authority.get("general_claims_remain_unchanged") is not True
            or authority.get("formal_write_count") != 0
            or not isinstance(contract, Mapping)
            or authority.get("release_id") != contract.get("release_id")
            or created > now + dt.timedelta(seconds=60)
            or expires <= now
            or expires - created > dt.timedelta(hours=6)
            or len(matching) != 1
            or matching[0].get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or matching[0].get("subject") != payload.get("subject")
        ):
            raise DispatchError("controlled_replay_allowlist_invalid")
        authority_sha256 = _sha256_bytes(_canonical_bytes(authority) + b"\n")
        marker = (
            self.controlled_replay_consumed_root
            / authority_sha256
            / f"{task.unit_sha256}.json"
        )
        if marker.exists():
            raise DispatchError("controlled_replay_already_consumed")
        _atomic_replace_json(
            marker,
            {
                "schema_version": "study-intake-controlled-replay-consumption-v1",
                "allowlist_sha256": authority_sha256,
                "unit_sha256": task.unit_sha256,
                "release_id": contract.get("release_id"),
                "subject": payload.get("subject"),
                "consumed_at": _utc_now(),
                "formal_write_count": 0,
            },
        )

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any] | None:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError("dispatch_state_invalid") from exc
        if not isinstance(value, dict):
            raise DispatchError("dispatch_state_invalid")
        return value

    def claim(
        self,
        unit_sha256: str,
        owner_id: str,
        *,
        subject: str | None = None,
        task: FrozenTask | None = None,
        controlled_replay_authority: Mapping[str, Any] | None = None,
        production_canary: bool = False,
        now: str | None = None,
    ) -> ClaimDecision:
        timestamp = now or _utc_now()
        current_time = _parse_utc(timestamp)
        with _ExclusiveFileLock(self.lock_path):
            if production_canary and controlled_replay_authority is not None:
                raise DispatchError("production_canary_controlled_replay_forbidden")
            if controlled_replay_authority is not None:
                if task is None or task.unit_sha256 != unit_sha256:
                    raise DispatchError("controlled_replay_task_invalid")
                self._consume_controlled_replay_allowlist(
                    task, controlled_replay_authority
                )
            elif production_canary:
                if (
                    task is None
                    or task.unit_sha256 != unit_sha256
                    or subject not in {"math", "cs408", "english"}
                    or not self._drain_path(str(subject)).exists()
                ):
                    raise DispatchError("production_canary_task_invalid")
                # A completed/leased unit is a replay, not a successful
                # idempotent submit, while the post-activation gate is active.
                if self._completion_path(unit_sha256).exists() or self._lease_path(
                    unit_sha256
                ).exists():
                    raise DispatchError("production_canary_task_replay")
                state = self._read_canary_state_locked(str(subject))
                assert state is not None
                self._admit_production_canary_task_locked(
                    task,
                    state,
                    timestamp,
                    lease_owner_id=owner_id,
                    lease_fence=1,
                )
            elif subject is not None and self._drain_path(subject).exists():
                raise DispatchError("subject_draining")
            completion = self._read_object(self._completion_path(unit_sha256))
            if completion is not None:
                return ClaimDecision("completed", completion=completion)
            path = self._lease_path(unit_sha256)
            previous = self._read_object(path)
            previous_fence = 0
            infrastructure_recovery_count = 0
            retry_wait_count = 0
            resume_from_analysis_checkpoint = False
            if previous is not None:
                previous_fence = previous.get("fence")
                if not isinstance(previous_fence, int) or previous_fence < 1:
                    raise DispatchError("lease_fence_invalid")
                infrastructure_recovery_count = previous.get(
                    "infrastructure_recovery_count", 0
                )
                retry_wait_count = previous.get("retry_wait_count", 0)
                resume_from_analysis_checkpoint = (
                    previous.get("resume_from_analysis_checkpoint") is True
                )
                if (
                    not isinstance(infrastructure_recovery_count, int)
                    or infrastructure_recovery_count < 0
                    or not isinstance(retry_wait_count, int)
                    or retry_wait_count < 0
                ):
                    raise DispatchError("lease_retry_state_invalid")
                if previous.get("status") == "claimed":
                    heartbeat = _parse_utc(previous.get("heartbeat_at"))
                    age = (current_time - heartbeat).total_seconds()
                    if age < LEASE_TTL_SECONDS:
                        return ClaimDecision("active")
            fence = previous_fence + 1
            lease_value = {
                "schema_version": LEASE_SCHEMA,
                "unit_sha256": unit_sha256,
                "owner_id": owner_id,
                "subject": subject,
                "fence": fence,
                "status": "claimed",
                "claimed_at": timestamp,
                "heartbeat_at": timestamp,
                "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
                "ttl_seconds": LEASE_TTL_SECONDS,
                "infrastructure_recovery_count": infrastructure_recovery_count,
                "retry_wait_count": retry_wait_count,
                "resume_from_analysis_checkpoint": (
                    resume_from_analysis_checkpoint
                ),
            }
            _atomic_replace_json(path, lease_value)
            return ClaimDecision(
                "claimed", Lease(unit_sha256, owner_id, fence)
            )

    def mark_retry_wait(
        self,
        lease: Lease,
        *,
        error_code: str,
        retry_kind: str,
        resume_from_analysis_checkpoint: bool = False,
        now: str | None = None,
    ) -> Mapping[str, Any]:
        """Release one lease into an explicit, independently retryable wait."""

        if retry_kind not in {"process_resource", "service_limit"}:
            raise DispatchError("retry_kind_invalid")
        timestamp = now or _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            path = self._lease_path(lease.unit_sha256)
            current = self._read_object(path)
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            assert current is not None
            retry_wait_count = current.get("retry_wait_count", 0)
            if not isinstance(retry_wait_count, int) or retry_wait_count < 0:
                raise DispatchError("lease_retry_state_invalid")
            waiting = dict(current)
            waiting.update(
                {
                    "status": "retry_wait",
                    "retry_kind": retry_kind,
                    "retry_error_code": error_code,
                    "retry_wait_at": timestamp,
                    "retry_wait_count": retry_wait_count + 1,
                    "resume_from_analysis_checkpoint": (
                        resume_from_analysis_checkpoint
                    ),
                }
            )
            _atomic_replace_json(path, waiting)
            return waiting

    def recover_after_infrastructure_crash(
        self,
        lease: Lease,
        *,
        error_code: str,
        resume_from_analysis_checkpoint: bool = False,
        now: str | None = None,
    ) -> Lease:
        """Atomically create the sole permitted automatic recovery generation."""

        timestamp = now or _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            path = self._lease_path(lease.unit_sha256)
            current = self._read_object(path)
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            assert current is not None
            recovery_count = current.get("infrastructure_recovery_count", 0)
            if not isinstance(recovery_count, int) or recovery_count < 0:
                raise DispatchError("lease_retry_state_invalid")
            if recovery_count >= 1:
                raise DispatchError("infrastructure_recovery_exhausted")
            next_fence = lease.fence + 1
            recovered = {
                "schema_version": LEASE_SCHEMA,
                "unit_sha256": lease.unit_sha256,
                "owner_id": lease.owner_id,
                "subject": current.get("subject"),
                "fence": next_fence,
                "status": "claimed",
                "claimed_at": timestamp,
                "heartbeat_at": timestamp,
                "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
                "ttl_seconds": LEASE_TTL_SECONDS,
                "infrastructure_recovery_count": recovery_count + 1,
                "retry_wait_count": current.get("retry_wait_count", 0),
                "recovered_from_fence": lease.fence,
                "recovery_error_code": error_code,
                "resume_from_analysis_checkpoint": (
                    resume_from_analysis_checkpoint
                ),
            }
            _atomic_replace_json(path, recovered)
            return Lease(lease.unit_sha256, lease.owner_id, next_fence)

    def infrastructure_recovery_count(self, lease: Lease) -> int:
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            assert current is not None
            value = current.get("infrastructure_recovery_count", 0)
            if not isinstance(value, int) or value < 0:
                raise DispatchError("lease_retry_state_invalid")
            return value

    def resume_checkpoint_required(self, lease: Lease) -> bool:
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            assert current is not None
            return current.get("resume_from_analysis_checkpoint") is True

    def _verified_task_event_locked(
        self, lease: Lease, event: str
    ) -> Mapping[str, Any] | None:
        index_root = (
            self.task_event_index_root
            / lease.unit_sha256
            / f"fence-{lease.fence}"
        )
        for index_path in sorted(index_root.glob("*.json")):
            index = self._read_object(index_path)
            if (
                not isinstance(index, Mapping)
                or index.get("unit_sha256") != lease.unit_sha256
                or index.get("owner_id") != lease.owner_id
                or index.get("fence") != lease.fence
                or index.get("event") != event
            ):
                continue
            event_sha256 = str(index.get("event_sha256") or "")
            _validate_unit_sha256(event_sha256)
            event_path = Path(str(index.get("event_path") or ""))
            try:
                event_bytes = event_path.read_bytes()
            except OSError as exc:
                raise DispatchError("task_event_missing") from exc
            if _sha256_bytes(event_bytes) != event_sha256:
                raise DispatchError("task_event_content_hash_mismatch")
            value = self._read_object(event_path)
            if value is None:
                raise DispatchError("task_event_missing")
            self._verify_seal(value, purpose="dispatch-task-event")
            if (
                value.get("unit_sha256") != lease.unit_sha256
                or value.get("owner_id") != lease.owner_id
                or value.get("fence") != lease.fence
                or value.get("event") != event
            ):
                raise DispatchError("task_event_binding_mismatch")
            return value
        return None

    def lease_has_task_event(self, lease: Lease, event: str) -> bool:
        """Verify whether this generation recorded one authenticated phase."""

        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            return self._verified_task_event_locked(lease, event) is not None

    def lease_has_recoverable_analysis_checkpoint(
        self, lease: Lease
    ) -> bool:
        """Require an authenticated event that names a persisted checkpoint."""

        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            value = self._verified_task_event_locked(
                lease, "analysis_completed"
            )
            if value is None:
                return False
            artifacts = value.get("artifacts")
            if not isinstance(artifacts, Mapping):
                return False
            checkpoint_sha256 = artifacts.get("checkpoint_sha256")
            if not isinstance(checkpoint_sha256, str):
                return False
            _validate_unit_sha256(checkpoint_sha256)
            checkpoint_ref = artifacts.get("checkpoint_ref")
            checkpoint_path = artifacts.get("checkpoint_path")
            if checkpoint_ref is not None:
                if checkpoint_ref != (
                    "study-intake-analysis-checkpoint://sha256/"
                    + checkpoint_sha256
                ):
                    raise DispatchError(
                        "analysis_checkpoint_event_ref_mismatch"
                    )
                binding_key = artifacts.get("checkpoint_binding_key")
                binding_sha256 = artifacts.get(
                    "checkpoint_binding_sha256"
                )
                if not isinstance(binding_key, str) or not isinstance(
                    binding_sha256, str
                ):
                    raise DispatchError(
                        "analysis_checkpoint_event_binding_missing"
                    )
                _validate_unit_sha256(binding_key)
                _validate_unit_sha256(binding_sha256)
                return True
            if not isinstance(checkpoint_path, str) or not checkpoint_path:
                return False
            try:
                raw = Path(checkpoint_path).read_bytes()
            except OSError as exc:
                raise DispatchError("analysis_checkpoint_missing") from exc
            if _sha256_bytes(raw) != checkpoint_sha256:
                raise DispatchError("analysis_checkpoint_hash_mismatch")
            return True

    def publish_analysis_checkpoint(
        self,
        task: FrozenTask,
        lease: Lease,
        analysis: StageResult,
    ) -> Mapping[str, str]:
        """Seal a completed analysis so a new fence can skip that model stage."""

        frozen = task.frozen_payload
        contract = frozen.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id"))
            if isinstance(contract, Mapping) and contract.get("release_id")
            else None
        )
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            checkpoint = self._seal(
                {
                    "schema_version": ANALYSIS_CHECKPOINT_SCHEMA,
                    "unit_sha256": lease.unit_sha256,
                    "source_fence": lease.fence,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "release_id": release_id,
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "analysis": copy.deepcopy(dict(analysis.payload)),
                    "stage_runtime": _stage_runtime(analysis),
                    "created_at": _utc_now(),
                    "formal_write_count": 0,
                },
                purpose="dispatch-analysis-checkpoint",
            )
            checkpoint_sha256, checkpoint_path = _publish_content_addressed(
                self.analysis_checkpoint_root, checkpoint
            )
            latest = self._seal(
                {
                    "schema_version": (
                        "study-intake-analysis-checkpoint-latest-v1"
                    ),
                    "unit_sha256": lease.unit_sha256,
                    "source_fence": lease.fence,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "release_id": release_id,
                    "checkpoint_sha256": checkpoint_sha256,
                    "checkpoint_path": str(checkpoint_path),
                    "updated_at": checkpoint["created_at"],
                    "formal_write_count": 0,
                },
                purpose="dispatch-analysis-checkpoint-latest",
            )
            _atomic_replace_json(
                self._analysis_checkpoint_latest_path(lease.unit_sha256), latest
            )
            return {
                "checkpoint_sha256": checkpoint_sha256,
                "checkpoint_path": str(checkpoint_path),
            }

    def load_analysis_checkpoint(
        self,
        task: FrozenTask,
        lease: Lease,
    ) -> tuple[StageResult, Mapping[str, str]] | None:
        """Verify and load only an earlier generation's bound checkpoint."""

        frozen = task.frozen_payload
        contract = frozen.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id"))
            if isinstance(contract, Mapping) and contract.get("release_id")
            else None
        )
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            latest = self._read_object(
                self._analysis_checkpoint_latest_path(lease.unit_sha256)
            )
            if latest is None:
                return None
            self._verify_seal(
                latest, purpose="dispatch-analysis-checkpoint-latest"
            )
            checkpoint_sha256 = str(latest.get("checkpoint_sha256") or "")
            _validate_unit_sha256(checkpoint_sha256)
            expected_path = (
                self.analysis_checkpoint_root
                / "sha256"
                / checkpoint_sha256[:2]
                / f"{checkpoint_sha256}.json"
            )
            checkpoint_path = Path(str(latest.get("checkpoint_path") or ""))
            if checkpoint_path.resolve() != expected_path.resolve():
                raise DispatchError("analysis_checkpoint_path_mismatch")
            try:
                checkpoint_bytes = checkpoint_path.read_bytes()
            except OSError as exc:
                raise DispatchError("analysis_checkpoint_missing") from exc
            if _sha256_bytes(checkpoint_bytes) != checkpoint_sha256:
                raise DispatchError("analysis_checkpoint_hash_mismatch")
            checkpoint = self._read_object(checkpoint_path)
            if checkpoint is None:
                raise DispatchError("analysis_checkpoint_missing")
            self._verify_seal(
                checkpoint, purpose="dispatch-analysis-checkpoint"
            )
            source_fence = checkpoint.get("source_fence")
            common_valid = (
                isinstance(source_fence, int)
                and source_fence >= 1
                and source_fence < lease.fence
                and latest.get("source_fence") == source_fence
                and latest.get("unit_sha256") == lease.unit_sha256
                and checkpoint.get("unit_sha256") == lease.unit_sha256
                and latest.get("frozen_payload_sha256")
                == task.frozen_payload_sha256
                and checkpoint.get("frozen_payload_sha256")
                == task.frozen_payload_sha256
                and latest.get("release_id") == release_id
                and checkpoint.get("release_id") == release_id
                and checkpoint.get("model") == REQUIRED_MODEL
                and checkpoint.get("reasoning_effort")
                == REQUIRED_REASONING_EFFORT
            )
            if not common_valid:
                raise DispatchError("analysis_checkpoint_binding_mismatch")
            runtime = checkpoint.get("stage_runtime")
            analysis_payload = checkpoint.get("analysis")
            if not isinstance(runtime, Mapping) or not isinstance(
                analysis_payload, Mapping
            ):
                raise DispatchError("analysis_checkpoint_shape_invalid")
            result = StageResult.coerce(
                {
                    "payload": analysis_payload,
                    **dict(runtime),
                }
            )
            return result, {
                "checkpoint_sha256": checkpoint_sha256,
                "checkpoint_path": str(checkpoint_path),
            }

    @staticmethod
    def _read_quarantine_object(
        path: Path,
        *,
        missing_code: str,
        invalid_code: str,
    ) -> tuple[dict[str, Any], bytes]:
        """Read one exact regular JSON file for quarantine binding."""

        try:
            if path.is_symlink() or not path.is_file():
                raise DispatchError(missing_code)
            raw = path.read_bytes()
            value = json.loads(raw.decode("utf-8"))
        except DispatchError:
            raise
        except FileNotFoundError as exc:
            raise DispatchError(missing_code) from exc
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError(invalid_code) from exc
        if not isinstance(value, dict):
            raise DispatchError(invalid_code)
        return value, raw

    @staticmethod
    def _dispatcher_owner_pid(owner_id: object) -> int:
        if not isinstance(owner_id, str):
            raise DispatchError("stale_claim_owner_id_unparseable")
        match = _DISPATCHER_OWNER_ID_RE.fullmatch(owner_id)
        if match is None:
            raise DispatchError("stale_claim_owner_id_unparseable")
        pid = int(match.group("pid"))
        if pid <= 1:
            raise DispatchError("stale_claim_owner_id_unparseable")
        return pid

    @staticmethod
    def _owner_process_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            if exc.errno == errno.EPERM:
                return True
            raise DispatchError("stale_claim_owner_probe_failed") from exc
        return True

    def _quarantine_receipt_path(self, receipt_sha256: str) -> Path:
        _validate_unit_sha256(receipt_sha256)
        return (
            self.stale_claim_quarantine_receipt_root
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _verified_existing_stale_claim_quarantine(
        self,
        subject: str,
        unit_sha256: str,
        lease_value: Mapping[str, Any],
        *,
        apply: bool,
    ) -> dict[str, Any]:
        receipt_sha256 = str(
            lease_value.get("quarantine_receipt_sha256") or ""
        )
        _validate_unit_sha256(receipt_sha256)
        receipt_path = self._quarantine_receipt_path(receipt_sha256)
        receipt, raw = self._read_quarantine_object(
            receipt_path,
            missing_code="stale_claim_quarantine_receipt_missing",
            invalid_code="stale_claim_quarantine_receipt_invalid",
        )
        if _sha256_bytes(raw) != receipt_sha256:
            raise DispatchError("stale_claim_quarantine_receipt_hash_mismatch")
        self._verify_seal(
            receipt, purpose="dispatch-stale-claim-quarantine"
        )
        if (
            receipt.get("schema_version")
            != STALE_CLAIM_QUARANTINE_RECEIPT_SCHEMA
            or receipt.get("subject") != subject
            or receipt.get("unit_sha256") != unit_sha256
            or receipt.get("owner_id") != lease_value.get("owner_id")
            or receipt.get("lease_fence_after") != lease_value.get("fence")
            or receipt.get("lease_fence_before")
            != lease_value.get("quarantined_from_fence")
            or receipt.get("status_after") != "quarantined"
            or receipt.get("model_call_count") != 0
            or receipt.get("provider_request_count") != 0
            or receipt.get("formal_write_count") != 0
            or receipt.get("sol_enabled") is not False
        ):
            raise DispatchError("stale_claim_quarantine_receipt_binding_mismatch")
        return {
            "schema_version": "study-intake-stale-claim-quarantine-control-v1",
            "mode": "apply" if apply else "preview",
            "status": "already_quarantined",
            "eligible": False,
            "applied": False,
            "subject": subject,
            "unit_sha256": unit_sha256,
            "owner_id": lease_value.get("owner_id"),
            "owner_pid": receipt.get("owner_pid"),
            "lease_fence_before": receipt.get("lease_fence_before"),
            "lease_fence_after": receipt.get("lease_fence_after"),
            "lease_expired_at": receipt.get("lease_expired_at"),
            "quarantine_effective_at": receipt.get(
                "quarantine_effective_at"
            ),
            "production_canary_state_present": False,
            "receipt_sha256": receipt_sha256,
            "receipt_path": str(receipt_path),
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def _stale_claim_quarantine_candidate(
        self,
        subject: str,
        unit_sha256: str,
        *,
        current_time: dt.datetime,
    ) -> dict[str, Any]:
        if subject not in {"math", "cs408", "english"}:
            raise DispatchError("stale_claim_quarantine_subject_invalid")
        _validate_unit_sha256(unit_sha256)
        if self._read_canary_state_locked(subject, required=False) is not None:
            # Canary claims require a multi-object gate/queue closure.  This
            # bounded repair command is intentionally restricted to historical
            # pre-activation claims and must never perform a lease-only canary
            # transition.
            raise DispatchError(
                "stale_claim_quarantine_production_canary_state_present"
            )
        drain, drain_raw = self._read_quarantine_object(
            self._drain_path(subject),
            missing_code="stale_claim_quarantine_requires_subject_drain",
            invalid_code="stale_claim_quarantine_drain_invalid",
        )
        if (
            drain.get("schema_version") != "study-intake-subject-drain-v1"
            or drain.get("subject") != subject
            or drain.get("draining") is not True
        ):
            raise DispatchError("stale_claim_quarantine_drain_invalid")
        drain_started = _parse_utc(drain.get("started_at"))

        lease_path = self._lease_path(unit_sha256)
        lease, lease_raw = self._read_quarantine_object(
            lease_path,
            missing_code="stale_claim_quarantine_lease_missing",
            invalid_code="stale_claim_quarantine_lease_invalid",
        )
        if lease.get("status") == "quarantined":
            return {"existing": lease}
        if (
            lease.get("schema_version") != LEASE_SCHEMA
            or lease.get("unit_sha256") != unit_sha256
            or lease.get("subject") != subject
            or lease.get("status") != "claimed"
            or lease.get("heartbeat_interval_seconds")
            != HEARTBEAT_INTERVAL_SECONDS
            or lease.get("ttl_seconds") != LEASE_TTL_SECONDS
            or not isinstance(lease.get("fence"), int)
            or lease.get("fence") < 1
        ):
            raise DispatchError("stale_claim_quarantine_lease_not_claimed")
        heartbeat = _parse_utc(lease.get("heartbeat_at"))
        lease_expired = heartbeat + dt.timedelta(seconds=LEASE_TTL_SECONDS)
        if current_time <= lease_expired:
            raise DispatchError("stale_claim_quarantine_lease_not_expired")

        owner_id = lease.get("owner_id")
        owner_pid = self._dispatcher_owner_pid(owner_id)
        if self._owner_process_exists(owner_pid):
            raise DispatchError("stale_claim_quarantine_owner_alive")

        task_detail_path = self.task_detail_root / f"{unit_sha256}.json"
        task_detail, task_detail_raw = self._read_quarantine_object(
            task_detail_path,
            missing_code="stale_claim_quarantine_task_detail_missing",
            invalid_code="stale_claim_quarantine_task_detail_invalid",
        )
        self._verify_seal(task_detail, purpose="dispatch-task-detail")
        if (
            task_detail.get("schema_version") != TASK_DETAIL_SCHEMA
            or task_detail.get("unit_sha256") != unit_sha256
            or task_detail.get("subject") != subject
            or task_detail.get("owner_id") != owner_id
            or task_detail.get("fence") != lease.get("fence")
            or task_detail.get("formal_write_count") != 0
        ):
            raise DispatchError("stale_claim_quarantine_task_detail_binding_mismatch")

        effective = max(lease_expired, drain_started)
        utc_text = lambda value: value.isoformat().replace("+00:00", "Z")
        return {
            "existing": None,
            "lease": lease,
            "lease_path": lease_path,
            "lease_before_sha256": _sha256_bytes(lease_raw),
            "task_detail_sha256": _sha256_bytes(task_detail_raw),
            "drain_sha256": _sha256_bytes(drain_raw),
            "drain_started_at": utc_text(drain_started),
            "heartbeat_at": utc_text(heartbeat),
            "lease_expired_at": utc_text(lease_expired),
            "quarantine_effective_at": utc_text(effective),
            "owner_id": owner_id,
            "owner_pid": owner_pid,
            "lease_fence_before": lease.get("fence"),
            "lease_fence_after": int(lease.get("fence")) + 1,
            "production_canary_state_present": False,
        }

    def quarantine_stale_claim(
        self,
        subject: str,
        unit_sha256: str,
        *,
        apply: bool = False,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Preview or fence one dead-owner stale claim while drained.

        Preview performs no coordination writes and apply revalidates every
        gate while holding the dispatcher lock.  The immutable receipt is
        published before the mutable lease swap, making a crash in between
        safely retryable with the same deterministic receipt bytes.
        """

        timestamp = now or _utc_now()
        current_time = _parse_utc(timestamp)

        def evaluate() -> dict[str, Any]:
            candidate = self._stale_claim_quarantine_candidate(
                subject, unit_sha256, current_time=current_time
            )
            existing = candidate.get("existing")
            if isinstance(existing, Mapping):
                return self._verified_existing_stale_claim_quarantine(
                    subject,
                    unit_sha256,
                    existing,
                    apply=apply,
                )
            if not apply:
                return {
                    "schema_version": (
                        "study-intake-stale-claim-quarantine-control-v1"
                    ),
                    "mode": "preview",
                    "status": "eligible",
                    "eligible": True,
                    "applied": False,
                    "subject": subject,
                    "unit_sha256": unit_sha256,
                    "owner_id": candidate["owner_id"],
                    "owner_pid": candidate["owner_pid"],
                    "lease_fence_before": candidate["lease_fence_before"],
                    "lease_fence_after": candidate["lease_fence_after"],
                    "lease_before_sha256": candidate[
                        "lease_before_sha256"
                    ],
                    "task_detail_sha256": candidate[
                        "task_detail_sha256"
                    ],
                    "production_canary_state_present": False,
                    "lease_expired_at": candidate["lease_expired_at"],
                    "quarantine_effective_at": candidate[
                        "quarantine_effective_at"
                    ],
                    "receipt_sha256": None,
                    "receipt_path": None,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }

            binding = {
                "subject": subject,
                "unit_sha256": unit_sha256,
                "owner_id": candidate["owner_id"],
                "owner_pid": candidate["owner_pid"],
                "lease_fence_before": candidate["lease_fence_before"],
                "lease_fence_after": candidate["lease_fence_after"],
                "lease_before_sha256": candidate["lease_before_sha256"],
                "task_detail_sha256": candidate["task_detail_sha256"],
                "drain_sha256": candidate["drain_sha256"],
                "drain_started_at": candidate["drain_started_at"],
                "heartbeat_at": candidate["heartbeat_at"],
                "ttl_seconds": LEASE_TTL_SECONDS,
                "lease_expired_at": candidate["lease_expired_at"],
                "quarantine_effective_at": candidate[
                    "quarantine_effective_at"
                ],
                "production_canary_state_present": False,
            }
            quarantine_id = _sha256_bytes(_canonical_bytes(binding))
            receipt = self._seal(
                {
                    "schema_version": STALE_CLAIM_QUARANTINE_RECEIPT_SCHEMA,
                    "quarantine_id": quarantine_id,
                    **binding,
                    "status_before": "claimed",
                    "status_after": "quarantined",
                    "owner_process_status": "absent",
                    "late_result_publish_allowed": False,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                },
                purpose="dispatch-stale-claim-quarantine",
            )
            receipt_sha256, receipt_path = _publish_content_addressed(
                self.stale_claim_quarantine_receipt_root, receipt
            )
            # Fail closed if the owner PID appeared (including PID reuse)
            # between eligibility observation and the mutable fence swap.
            if self._owner_process_exists(int(candidate["owner_pid"])):
                raise DispatchError("stale_claim_quarantine_owner_reappeared")
            lease = dict(candidate["lease"])
            lease.update(
                {
                    "status": "quarantined",
                    "fence": candidate["lease_fence_after"],
                    "quarantined_from_fence": candidate[
                        "lease_fence_before"
                    ],
                    "quarantine_id": quarantine_id,
                    "quarantine_receipt_sha256": receipt_sha256,
                    "quarantine_effective_at": candidate[
                        "quarantine_effective_at"
                    ],
                    "quarantine_lease_before_sha256": candidate[
                        "lease_before_sha256"
                    ],
                    "quarantine_task_detail_sha256": candidate[
                        "task_detail_sha256"
                    ],
                    "production_canary_state_present": False,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
            )
            _atomic_replace_json(candidate["lease_path"], lease)
            return {
                "schema_version": (
                    "study-intake-stale-claim-quarantine-control-v1"
                ),
                "mode": "apply",
                "status": "quarantined",
                "eligible": True,
                "applied": True,
                "subject": subject,
                "unit_sha256": unit_sha256,
                "owner_id": candidate["owner_id"],
                "owner_pid": candidate["owner_pid"],
                "lease_fence_before": candidate["lease_fence_before"],
                "lease_fence_after": candidate["lease_fence_after"],
                "lease_expired_at": candidate["lease_expired_at"],
                "quarantine_effective_at": candidate[
                    "quarantine_effective_at"
                ],
                "production_canary_state_present": False,
                "receipt_sha256": receipt_sha256,
                "receipt_path": str(receipt_path),
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }

        if not apply:
            # Preview deliberately avoids even creating the coordination lock.
            # Apply performs the same checks again under the lock.
            return evaluate()
        with _ExclusiveFileLock(self.lock_path):
            return evaluate()

    def _drain_path(self, subject: str) -> Path:
        return self.drain_root / f"{_safe_component(subject)}.json"

    def begin_subject_drain(self, subject: str) -> None:
        with _ExclusiveFileLock(self.lock_path):
            _atomic_replace_json(
                self._drain_path(subject),
                {
                    "schema_version": "study-intake-subject-drain-v1",
                    "subject": subject,
                    "draining": True,
                    "started_at": _utc_now(),
                },
            )

    def clear_subject_drain(self, subject: str) -> None:
        with _ExclusiveFileLock(self.lock_path):
            try:
                self._drain_path(subject).unlink()
            except FileNotFoundError:
                return

    def subject_status_read_only(self, subject: str) -> dict[str, Any]:
        """Inspect lease/drain state without refreshing any projection."""

        now = dt.datetime.now(dt.timezone.utc)
        active = 0
        stale = 0
        claimed_total = 0
        with _ExclusiveFileLock(self.lock_path):
            for path in sorted(self.lease_root.glob("*.json")):
                value = self._read_object(path)
                if value is None or value.get("subject") != subject:
                    continue
                if value.get("status") != "claimed":
                    continue
                claimed_total += 1
                age = (
                    now - _parse_utc(value.get("heartbeat_at"))
                ).total_seconds()
                if age < LEASE_TTL_SECONDS:
                    active += 1
                else:
                    stale += 1
            draining = self._drain_path(subject).exists()
        return {
            "schema_version": "study-intake-dispatch-subject-status-read-only-v1",
            "subject": subject,
            "draining": draining,
            "active_count": active,
            "stale_count": stale,
            "claimed_total": claimed_total,
            "read_only": True,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "observed_at": _utc_now(),
        }

    def subject_status(self, subject: str) -> dict[str, Any]:
        """Return a bounded coordination projection without reading packages."""

        now = dt.datetime.now(dt.timezone.utc)
        active = 0
        stale = 0
        claimed_total = 0
        completed = 0
        retry_wait = 0
        fences: list[int] = []
        concurrency_telemetry: dict[str, Any] | None = None
        effective_concurrency_limit = 0
        available_concurrency_slots = 0
        subject_peak_active = 0
        global_active_task_count = 0
        global_peak_active = 0
        backpressure_reason: str | None = None
        terminal_task_count = 0
        terminal_by_outcome = self._empty_canary_terminal_counts()
        terminal_index_sha256: str | None = None
        terminal_failure_bindings: list[dict[str, Any]] = []
        verified_runner_active_task_count = 0
        runner_evidenced_task_count = 0
        runner_interval_missing_count = 0
        scheduler_claim_subject_peak_active = 0
        with _ExclusiveFileLock(self.lock_path):
            for path in sorted(self.lease_root.glob("*.json")):
                value = self._read_object(path)
                if value is None or value.get("subject") != subject:
                    continue
                fence = value.get("fence")
                if isinstance(fence, int):
                    fences.append(fence)
                if value.get("status") == "completed":
                    completed += 1
                elif value.get("status") == "retry_wait":
                    retry_wait += 1
                elif value.get("status") == "claimed":
                    claimed_total += 1
                    age = (now - _parse_utc(value.get("heartbeat_at"))).total_seconds()
                    if age < LEASE_TTL_SECONDS:
                        active += 1
                    else:
                        stale += 1
            draining = self._drain_path(subject).exists()
            canary_gate = self._read_canary_state_locked(
                subject, required=False
            )
            if canary_gate is not None:
                refreshed = self._refresh_canary_queue_projection_locked(
                    canary_gate
                )
                if self._canary_projection_changed(canary_gate, refreshed):
                    canary_gate = self._write_canary_state_locked(refreshed)
                concurrency_telemetry = (
                    self._read_canary_concurrency_telemetry_locked(canary_gate)
                )
                gate_state = canary_gate.get("state")
                if canary_gate.get("luna_consumer_enabled") is True:
                    effective_concurrency_limit = (
                        INITIAL_CANARY_INFLIGHT_LIMIT
                        if gate_state in {"armed", "canary_in_flight"}
                        else int(canary_gate["continuous_concurrency_limit"])
                        if gate_state == "continuous_concurrent_unlocked"
                        else 0
                    )
                available_concurrency_slots = max(
                    0,
                    effective_concurrency_limit
                    - int(canary_gate.get("active_task_count") or 0),
                )
                subject_peak_active = int(
                    concurrency_telemetry["subject_peak_active"].get(subject)
                    or 0
                )
                global_active_task_count = int(
                    concurrency_telemetry["global_active_task_count"]
                )
                global_peak_active = int(
                    concurrency_telemetry["global_peak_active"]
                )
                raw_backpressure = canary_gate.get("backpressure_reason")
                backpressure_reason = (
                    str(raw_backpressure)
                    if isinstance(raw_backpressure, str)
                    else None
                )
                terminal_task_count = int(
                    canary_gate.get("terminal_task_count") or 0
                )
                terminal_by_outcome = copy.deepcopy(
                    dict(canary_gate.get("terminal_by_outcome") or {})
                )
                terminal_index_sha256 = str(
                    canary_gate.get("terminal_index_sha256") or ""
                )
                terminal_failure_bindings = copy.deepcopy(
                    list(
                        concurrency_telemetry[
                            "terminal_failure_bindings_by_subject"
                        ].get(subject)
                        or []
                    )
                )
                verified_runner_active_task_count = int(
                    concurrency_telemetry[
                        "verified_runner_active_by_subject"
                    ].get(subject)
                    or 0
                )
                runner_evidenced_task_count = int(
                    concurrency_telemetry[
                        "runner_evidenced_task_count_by_subject"
                    ].get(subject)
                    or 0
                )
                runner_interval_missing_count = int(
                    concurrency_telemetry[
                        "runner_interval_missing_count_by_subject"
                    ].get(subject)
                    or 0
                )
                scheduler_claim_subject_peak_active = int(
                    concurrency_telemetry[
                        "scheduler_claim_subject_peak_active"
                    ].get(subject)
                    or 0
                )
        return {
            "schema_version": "study-intake-dispatch-subject-status-v1",
            "subject": subject,
            "draining": draining,
            "active_count": active,
            "stale_count": stale,
            "claimed_total": claimed_total,
            "completed_count": completed,
            "retry_wait_count": retry_wait,
            "max_fence": max(fences, default=0),
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "lease_ttl_seconds": LEASE_TTL_SECONDS,
            "canary_gate": canary_gate,
            "canary_concurrency_telemetry": concurrency_telemetry,
            "effective_concurrency_limit": effective_concurrency_limit,
            "available_concurrency_slots": available_concurrency_slots,
            "subject_peak_active": subject_peak_active,
            "global_active_task_count": global_active_task_count,
            "global_peak_active": global_peak_active,
            "backpressure_reason": backpressure_reason,
            "terminal_task_count": terminal_task_count,
            "terminal_by_outcome": terminal_by_outcome,
            "terminal_index_sha256": terminal_index_sha256,
            "terminal_failure_bindings": terminal_failure_bindings,
            "verified_runner_active_task_count": (
                verified_runner_active_task_count
            ),
            "runner_evidenced_task_count": runner_evidenced_task_count,
            "runner_interval_missing_count": runner_interval_missing_count,
            "scheduler_claim_subject_peak_active": (
                scheduler_claim_subject_peak_active
            ),
            "observed_at": _utc_now(),
        }

    def wait_subject_drained(
        self, subject: str, *, poll_seconds: float = 0.1
    ) -> dict[str, Any]:
        while True:
            status = self.subject_status(subject)
            # A stale heartbeat is not proof that the process or its descendants
            # have exited.  During release drain every claimed fence remains a
            # blocker until it is explicitly moved to a terminal/retry state.
            if status["claimed_total"] == 0:
                return status
            time.sleep(poll_seconds)

    def publish_task_process_identity(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        child_pid: int,
        child_pgid: int,
        process_start_token: str,
        launch_nonce: str,
        launched_at: str,
        argv: Sequence[str],
        executable_path: Path,
        start_new_session: bool,
    ) -> dict[str, Any]:
        """Seal the exact OS child identity before sending it task input."""

        if (
            isinstance(child_pid, bool)
            or not isinstance(child_pid, int)
            or child_pid <= 1
            or isinstance(child_pgid, bool)
            or not isinstance(child_pgid, int)
            or child_pgid != child_pid
            or start_new_session is not True
            or not isinstance(process_start_token, str)
            or not process_start_token.strip()
            or not isinstance(launch_nonce, str)
            or not re.fullmatch(r"[0-9a-f]{32}", launch_nonce)
            or not argv
            or any(not isinstance(item, str) or not item for item in argv)
        ):
            raise DispatchError("task_process_identity_invalid")
        try:
            require_kernel_process_start_token(
                child_pid, process_start_token.strip()
            )
        except ProcessIdentityError as exc:
            raise DispatchError("task_process_start_token_mismatch") from exc
        _parse_utc(launched_at)
        owner_match = _DISPATCHER_OWNER_ID_RE.fullmatch(lease.owner_id)
        if owner_match is None or int(owner_match.group("pid")) != os.getpid():
            raise DispatchError("task_process_dispatcher_identity_invalid")
        executable = executable_path.resolve()
        try:
            executable_bytes = executable.read_bytes()
        except OSError as exc:
            raise DispatchError("task_process_executable_unreadable") from exc
        frozen = task.frozen_payload
        subject = str(frozen.get("subject") or "")
        capture_id = str(frozen.get("capture_id") or "")
        contract = frozen.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id") or "")
            if isinstance(contract, Mapping)
            else ""
        )
        _validate_unit_sha256(release_id)
        context_root = (
            self.runtime_root
            / "dispatch"
            / "contexts"
            / task.unit_sha256
            / f"fence-{lease.fence}"
        ).resolve()
        core = {
            "schema_version": TASK_PROCESS_IDENTITY_SCHEMA,
            "unit_sha256": task.unit_sha256,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "subject": subject,
            "capture_id": capture_id,
            "release_id": release_id,
            "owner_id": lease.owner_id,
            "lease_fence": lease.fence,
            "dispatcher_pid": os.getpid(),
            "child_pid": child_pid,
            "child_pgid": child_pgid,
            "process_start_token": process_start_token.strip(),
            "launch_nonce": launch_nonce,
            "launched_at": launched_at,
            "argv_sha256": _sha256_bytes(_canonical_bytes(list(argv))),
            "executable_path": str(executable),
            "executable_sha256": _sha256_bytes(executable_bytes),
            "context_root": str(context_root),
            "expected_mcp_session_root": str(context_root / "mcp-session"),
            "expected_report_root": str(context_root / "reports"),
            "start_new_session": True,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        with _ExclusiveFileLock(self.lock_path):
            lease_path = self._lease_path(lease.unit_sha256)
            current = self._read_object(lease_path)
            if not (
                current
                and current.get("status") == "claimed"
                and current.get("owner_id") == lease.owner_id
                and current.get("fence") == lease.fence
                and current.get("unit_sha256") == task.unit_sha256
            ):
                raise DispatchError("stale_lease_fence")
            state = self._read_canary_state_locked(subject, required=False)
            active_selections: dict[str, Any] | None = None
            selected: Mapping[str, Any] | None = None
            queue_entry: dict[str, Any] | None = None
            if state is not None:
                active_selections = copy.deepcopy(
                    dict(state.get("active_selections") or {})
                )
                selected = active_selections.get(task.unit_sha256)
                if not isinstance(selected, Mapping):
                    raise DispatchError(
                        "production_canary_process_identity_binding_missing"
                    )
                if (
                    selected.get("lease_owner_id") != lease.owner_id
                    or selected.get("lease_fence") != lease.fence
                    or selected.get("process_identity_sha256") is not None
                    or selected.get("process_identity_path") is not None
                ):
                    raise DispatchError(
                        "production_canary_process_identity_binding_invalid"
                    )
                queue_path = self._production_canary_queue_path(
                    subject,
                    str(selected["producer_input_contract_sha256"]),
                )
                queue_entry = self._read_object(queue_path)
                if queue_entry is None:
                    raise DispatchError(
                        "production_canary_process_identity_queue_missing"
                    )
                self._verify_seal(
                    queue_entry, purpose="dispatch-production-canary-queue"
                )
            identity = self._seal(
                core, purpose="dispatch-task-process-identity"
            )
            identity_sha256, identity_path = _publish_content_addressed(
                self.task_process_identity_root
                / _safe_component(subject)
                / task.unit_sha256
                / f"fence-{lease.fence}",
                identity,
            )
            index_path = (
                self.task_process_identity_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}.json"
            )
            _publish_named_immutable(
                index_path,
                {
                    "schema_version": "study-intake-task-process-identity-index-v1",
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "process_identity_sha256": identity_sha256,
                    "process_identity_path": str(identity_path),
                    "formal_write_count": 0,
                },
            )
            if state is not None:
                assert active_selections is not None
                assert selected is not None
                assert queue_entry is not None
                self._update_canary_queue_entry_locked(
                    queue_entry,
                    process_identity_sha256=identity_sha256,
                    process_identity_path=str(identity_path),
                )
                selected = dict(selected)
                selected.update(
                    {
                        "process_identity_sha256": identity_sha256,
                        "process_identity_path": str(identity_path),
                    }
                )
                active_selections[task.unit_sha256] = selected
                updated_state = dict(state)
                updated_state["active_selections"] = active_selections
                if state.get("state") == "canary_in_flight":
                    updated_state["selected"] = selected
                self._write_canary_state_locked(
                    updated_state, updated_at=launched_at
                )
            return {
                "process_identity_sha256": identity_sha256,
                "process_identity_path": str(identity_path),
                "process_identity": identity,
            }

    @staticmethod
    def _provider_stage_name(subject: str, stage_name: str) -> str:
        expected = {
            f"{subject}_analysis",
            f"{subject}_critical_review",
        }
        if subject not in {"math", "cs408", "english"} or stage_name not in expected:
            raise DispatchError("provider_process_stage_invalid")
        return stage_name

    @classmethod
    def _semantic_stage_name(cls, subject: str, stage_name: str) -> str:
        checked = cls._provider_stage_name(subject, stage_name)
        semantic = checked.removeprefix(f"{subject}_")
        if semantic not in {"analysis", "critical_review"}:
            raise DispatchError("provider_process_stage_invalid")
        return semantic

    def _task_process_identity_locked(
        self, task: FrozenTask, lease: Lease
    ) -> tuple[dict[str, Any], str, str]:
        index_path = (
            self.task_process_identity_latest_root
            / task.unit_sha256
            / f"fence-{lease.fence}.json"
        )
        index = self._read_object(index_path)
        if not isinstance(index, Mapping):
            raise DispatchError("task_process_identity_index_missing")
        identity_sha256 = str(index.get("process_identity_sha256") or "")
        identity_path = Path(str(index.get("process_identity_path") or ""))
        _validate_unit_sha256(identity_sha256)
        try:
            payload = identity_path.read_bytes()
        except OSError as exc:
            raise DispatchError("task_process_identity_missing") from exc
        if _sha256_bytes(payload) != identity_sha256:
            raise DispatchError("task_process_identity_hash_mismatch")
        identity = self._read_object(identity_path)
        if identity is None:
            raise DispatchError("task_process_identity_missing")
        self._verify_seal(identity, purpose="dispatch-task-process-identity")
        if (
            identity.get("schema_version") != TASK_PROCESS_IDENTITY_SCHEMA
            or identity.get("unit_sha256") != task.unit_sha256
            or identity.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or identity.get("owner_id") != lease.owner_id
            or identity.get("lease_fence") != lease.fence
        ):
            raise DispatchError("task_process_identity_binding_mismatch")
        try:
            parse_kernel_process_start_token(
                str(identity.get("process_start_token") or ""),
                expected_pid=int(identity.get("child_pid") or 0),
            )
        except (ProcessIdentityError, TypeError, ValueError) as exc:
            raise DispatchError("task_process_start_token_invalid") from exc
        return identity, identity_sha256, str(identity_path)

    def verify_task_process_identity(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        expected_sha256: str,
        expected_path: str,
        expected_launch_nonce: str,
    ) -> dict[str, Any]:
        """Reopen the supervisor identity supplied by the parent request."""

        with _ExclusiveFileLock(self.lock_path):
            identity, identity_sha256, identity_path = (
                self._task_process_identity_locked(task, lease)
            )
            if (
                identity_sha256 != expected_sha256
                or identity_path != str(Path(expected_path))
                or identity.get("launch_nonce") != expected_launch_nonce
                or identity.get("child_pid") != os.getpid()
                or identity.get("child_pgid") != os.getpgrp()
            ):
                raise DispatchError("task_process_identity_request_mismatch")
            try:
                require_kernel_process_start_token(
                    os.getpid(), str(identity["process_start_token"])
                )
            except ProcessIdentityError as exc:
                raise DispatchError(
                    "task_process_identity_pid_reuse_detected"
                ) from exc
            return identity

    def publish_task_process_exit(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        process_identity_sha256: str,
        process_identity_path: str,
        returncode: int,
        termination_reason: str,
        reaped: bool,
        process_absent: bool,
        pgid_absent: bool,
        stdout_sha256: str,
        stdout_size: int,
        stderr_sha256: str,
        stderr_size: int,
        finished_at: str,
    ) -> dict[str, Any]:
        """Seal proof that the exact task supervisor and its PGID are gone."""

        if (
            isinstance(returncode, bool)
            or not isinstance(returncode, int)
            or termination_reason
            not in {
                "completed",
                "nonzero",
                "cancelled",
                "timed_out",
                "launch_failed",
            }
            or reaped is not True
            or process_absent is not True
            or pgid_absent is not True
            or isinstance(stdout_size, bool)
            or not isinstance(stdout_size, int)
            or stdout_size < 0
            or isinstance(stderr_size, bool)
            or not isinstance(stderr_size, int)
            or stderr_size < 0
        ):
            raise DispatchError("task_process_exit_invalid")
        for digest in (
            process_identity_sha256,
            stdout_sha256,
            stderr_sha256,
        ):
            _validate_unit_sha256(digest)
        _parse_utc(finished_at)
        supplied_identity_path = Path(process_identity_path)
        subject = str(task.frozen_payload.get("subject") or "")
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            identity, identity_sha256, identity_path = (
                self._task_process_identity_locked(task, lease)
            )
            if (
                identity_sha256 != process_identity_sha256
                or Path(identity_path) != supplied_identity_path
            ):
                raise DispatchError("task_process_exit_identity_mismatch")
            try:
                parse_kernel_process_start_token(
                    str(identity.get("process_start_token") or ""),
                    expected_pid=int(identity.get("child_pid") or 0),
                )
            except (ProcessIdentityError, TypeError, ValueError) as exc:
                raise DispatchError("task_process_start_token_invalid") from exc
            exit_value = self._seal(
                {
                    "schema_version": TASK_PROCESS_EXIT_SCHEMA,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "subject": subject,
                    "capture_id": str(
                        task.frozen_payload.get("capture_id") or ""
                    ),
                    "release_id": identity["release_id"],
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "task_process_identity_sha256": identity_sha256,
                    "task_process_identity_path": identity_path,
                    "child_pid": identity["child_pid"],
                    "child_pgid": identity["child_pgid"],
                    "process_start_token": identity["process_start_token"],
                    "launch_nonce": identity["launch_nonce"],
                    "launched_at": identity["launched_at"],
                    "returncode": returncode,
                    "termination_reason": termination_reason,
                    "reaped": True,
                    "process_absent": True,
                    "pgid_absent": True,
                    "late_result_publish_allowed": (
                        termination_reason == "completed" and returncode == 0
                    ),
                    "stdout_sha256": stdout_sha256,
                    "stdout_size": stdout_size,
                    "stderr_sha256": stderr_sha256,
                    "stderr_size": stderr_size,
                    "finished_at": finished_at,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                },
                purpose="dispatch-task-process-exit",
            )
            exit_sha256, exit_path = _publish_content_addressed(
                self.task_process_exit_root
                / _safe_component(subject)
                / task.unit_sha256
                / f"fence-{lease.fence}",
                exit_value,
            )
            index_path = (
                self.task_process_exit_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}.json"
            )
            _publish_named_immutable(
                index_path,
                {
                    "schema_version": "study-intake-task-process-exit-index-v1",
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "task_process_identity_sha256": identity_sha256,
                    "task_process_exit_sha256": exit_sha256,
                    "task_process_exit_path": str(exit_path),
                    "formal_write_count": 0,
                },
            )
            return {
                "task_process_exit_sha256": exit_sha256,
                "task_process_exit_path": str(exit_path),
                "task_process_exit": exit_value,
            }

    def _task_process_closure_locked(
        self, task: FrozenTask, lease: Lease
    ) -> dict[str, Any]:
        identity, identity_sha256, identity_path = (
            self._task_process_identity_locked(task, lease)
        )
        index_path = (
            self.task_process_exit_latest_root
            / task.unit_sha256
            / f"fence-{lease.fence}.json"
        )
        index = self._read_object(index_path)
        if not isinstance(index, Mapping):
            raise DispatchError("task_process_exit_index_missing")
        exit_sha256 = str(index.get("task_process_exit_sha256") or "")
        exit_path = Path(str(index.get("task_process_exit_path") or ""))
        _validate_unit_sha256(exit_sha256)
        try:
            exit_bytes = exit_path.read_bytes()
        except OSError as exc:
            raise DispatchError("task_process_exit_missing") from exc
        if _sha256_bytes(exit_bytes) != exit_sha256:
            raise DispatchError("task_process_exit_hash_mismatch")
        exit_value = self._read_object(exit_path)
        if exit_value is None:
            raise DispatchError("task_process_exit_missing")
        self._verify_seal(exit_value, purpose="dispatch-task-process-exit")
        if (
            index.get("schema_version")
            != "study-intake-task-process-exit-index-v1"
            or index.get("unit_sha256") != task.unit_sha256
            or index.get("owner_id") != lease.owner_id
            or index.get("lease_fence") != lease.fence
            or index.get("task_process_identity_sha256") != identity_sha256
            or exit_value.get("schema_version") != TASK_PROCESS_EXIT_SCHEMA
            or exit_value.get("unit_sha256") != task.unit_sha256
            or exit_value.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or exit_value.get("owner_id") != lease.owner_id
            or exit_value.get("lease_fence") != lease.fence
            or exit_value.get("task_process_identity_sha256")
            != identity_sha256
            or exit_value.get("task_process_identity_path") != identity_path
            or exit_value.get("child_pid") != identity.get("child_pid")
            or exit_value.get("child_pgid") != identity.get("child_pgid")
            or exit_value.get("process_start_token")
            != identity.get("process_start_token")
            or exit_value.get("launch_nonce") != identity.get("launch_nonce")
            or exit_value.get("reaped") is not True
            or exit_value.get("process_absent") is not True
            or exit_value.get("pgid_absent") is not True
        ):
            raise DispatchError("task_process_closure_binding_invalid")
        if _parse_utc(exit_value.get("finished_at")) < _parse_utc(
            identity.get("launched_at")
        ):
            raise DispatchError("task_process_closure_interval_invalid")
        return {
            "supervisor_process_identity_sha256": identity_sha256,
            "supervisor_process_identity_path": identity_path,
            "supervisor_process_exit_sha256": exit_sha256,
            "supervisor_process_exit_path": str(exit_path),
            "supervisor_pid": identity["child_pid"],
            "supervisor_pgid": identity["child_pgid"],
            "process_start_token": identity["process_start_token"],
            "launch_nonce": identity["launch_nonce"],
            "launched_at": identity["launched_at"],
            "finished_at": exit_value["finished_at"],
            "returncode": exit_value["returncode"],
            "termination_reason": exit_value["termination_reason"],
            "reaped": True,
            "process_absent": True,
            "pgid_absent": True,
        }

    def publish_provider_process_identity(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        provider_pid: int,
        provider_pgid: int,
        process_start_token: str,
        launch_nonce: str,
        launched_at: str,
        argv: Sequence[str],
        environment: Mapping[str, str],
        executable_path: Path,
        cwd: Path,
        start_new_session: bool,
    ) -> dict[str, Any]:
        """Seal the real Codex child before its prompt is sent on stdin."""

        argv_evidence = validate_no_fast_mode_provider_argv(argv)
        environment_evidence = validate_no_fast_mode_provider_environment(
            environment
        )
        subject = str(task.frozen_payload.get("subject") or "")
        checked_stage = self._provider_stage_name(subject, stage_name)
        if (
            isinstance(provider_pid, bool)
            or not isinstance(provider_pid, int)
            or provider_pid <= 1
            or isinstance(provider_pgid, bool)
            or not isinstance(provider_pgid, int)
            or provider_pgid != provider_pid
            or start_new_session is not True
            or not isinstance(process_start_token, str)
            or not process_start_token.strip()
            or not isinstance(launch_nonce, str)
            or not re.fullmatch(r"[0-9a-f]{32}", launch_nonce)
        ):
            raise DispatchError("provider_process_identity_invalid")
        try:
            require_kernel_process_start_token(
                provider_pid, process_start_token.strip()
            )
        except ProcessIdentityError as exc:
            raise DispatchError("provider_process_start_token_mismatch") from exc
        _parse_utc(launched_at)
        executable = executable_path.resolve()
        execution_root = cwd.resolve()
        try:
            executable_bytes = executable.read_bytes()
        except OSError as exc:
            raise DispatchError("provider_process_executable_unreadable") from exc
        contract = task.frozen_payload.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id") or "")
            if isinstance(contract, Mapping)
            else ""
        )
        _validate_unit_sha256(release_id)
        with _ExclusiveFileLock(self.lock_path):
            lease_path = self._lease_path(lease.unit_sha256)
            current = self._read_object(lease_path)
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            (
                supervisor,
                supervisor_sha256,
                supervisor_path,
            ) = self._task_process_identity_locked(task, lease)
            context_root = Path(str(supervisor["context_root"])).resolve()
            if execution_root != context_root:
                raise DispatchError("provider_process_context_root_mismatch")
            core = {
                "schema_version": PROVIDER_PROCESS_IDENTITY_SCHEMA,
                "role": "codex_exec_provider_child",
                "stage_name": checked_stage,
                "unit_sha256": task.unit_sha256,
                "frozen_payload_sha256": task.frozen_payload_sha256,
                "subject": subject,
                "capture_id": str(task.frozen_payload.get("capture_id") or ""),
                "release_id": release_id,
                "owner_id": lease.owner_id,
                "lease_fence": lease.fence,
                "supervisor_process_identity_sha256": supervisor_sha256,
                "supervisor_process_identity_path": supervisor_path,
                "supervisor_pid": supervisor["child_pid"],
                "supervisor_pgid": supervisor["child_pgid"],
                "provider_pid": provider_pid,
                "provider_pgid": provider_pgid,
                "process_start_token": process_start_token.strip(),
                "launch_nonce": launch_nonce,
                "launched_at": launched_at,
                "argv_policy_version": argv_evidence[
                    "argv_policy_version"
                ],
                "argv": argv_evidence["argv"],
                "argv_sha256": argv_evidence["argv_sha256"],
                "environment_policy_version": environment_evidence[
                    "environment_policy_version"
                ],
                "environment_key_names": environment_evidence[
                    "environment_key_names"
                ],
                "environment_key_names_sha256": environment_evidence[
                    "environment_key_names_sha256"
                ],
                "forbidden_environment_key_matches": [],
                "forbidden_environment_value_key_matches": [],
                "executable_path": str(executable),
                "executable_sha256": _sha256_bytes(executable_bytes),
                "cwd": str(execution_root),
                "context_root": str(context_root),
                "start_new_session": True,
                "provider_request_started": True,
                "requested_service_tier": argv_evidence[
                    "requested_service_tier"
                ],
                "fast_mode_requested": argv_evidence[
                    "fast_mode_requested"
                ],
                "fast_mode_effective": argv_evidence[
                    "fast_mode_effective"
                ],
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            identity = self._seal(
                core, purpose="dispatch-provider-process-identity"
            )
            identity_sha256, identity_path = _publish_content_addressed(
                self.provider_process_identity_root
                / _safe_component(subject)
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(checked_stage),
                identity,
            )
            index_path = (
                self.provider_process_identity_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(checked_stage)}.json"
            )
            _publish_named_immutable(
                index_path,
                {
                    "schema_version": (
                        "study-intake-provider-process-identity-index-v1"
                    ),
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": checked_stage,
                    "provider_process_identity_sha256": identity_sha256,
                    "provider_process_identity_path": str(identity_path),
                    "formal_write_count": 0,
                },
            )
            return {
                "provider_process_identity_sha256": identity_sha256,
                "provider_process_identity_path": str(identity_path),
                "provider_process_identity": identity,
            }

    def _provider_process_identity_locked(
        self, task: FrozenTask, lease: Lease, stage_name: str
    ) -> tuple[dict[str, Any], str, str]:
        subject = str(task.frozen_payload.get("subject") or "")
        checked_stage = self._provider_stage_name(subject, stage_name)
        current = self._read_object(self._lease_path(lease.unit_sha256))
        if not self._matches(current, lease, status="claimed"):
            raise DispatchError("stale_lease_fence")
        index_path = (
            self.provider_process_identity_latest_root
            / task.unit_sha256
            / f"fence-{lease.fence}"
            / f"{_safe_component(checked_stage)}.json"
        )
        index = self._read_object(index_path)
        if not isinstance(index, Mapping):
            raise DispatchError("provider_process_identity_index_missing")
        identity_sha256 = str(
            index.get("provider_process_identity_sha256") or ""
        )
        identity_path = Path(
            str(index.get("provider_process_identity_path") or "")
        )
        _validate_unit_sha256(identity_sha256)
        expected_path = (
            self.provider_process_identity_root
            / _safe_component(subject)
            / task.unit_sha256
            / f"fence-{lease.fence}"
            / _safe_component(checked_stage)
            / "sha256"
            / identity_sha256[:2]
            / f"{identity_sha256}.json"
        )
        if identity_path.resolve() != expected_path.resolve():
            raise DispatchError("provider_process_identity_path_mismatch")
        try:
            identity_bytes = identity_path.read_bytes()
        except OSError as exc:
            raise DispatchError("provider_process_identity_missing") from exc
        identity = self._read_object(identity_path)
        if (
            identity is None
            or _sha256_bytes(identity_bytes) != identity_sha256
        ):
            raise DispatchError("provider_process_identity_hash_mismatch")
        self._verify_seal(
            identity, purpose="dispatch-provider-process-identity"
        )
        if (
            identity.get("schema_version")
            != PROVIDER_PROCESS_IDENTITY_SCHEMA
            or identity.get("unit_sha256") != task.unit_sha256
            or identity.get("frozen_payload_sha256")
            != task.frozen_payload_sha256
            or identity.get("subject") != subject
            or identity.get("owner_id") != lease.owner_id
            or identity.get("lease_fence") != lease.fence
            or identity.get("stage_name") != checked_stage
            or index.get("unit_sha256") != task.unit_sha256
            or index.get("owner_id") != lease.owner_id
            or index.get("lease_fence") != lease.fence
            or index.get("stage_name") != checked_stage
        ):
            raise DispatchError("provider_process_identity_binding_mismatch")
        return dict(identity), identity_sha256, str(identity_path)

    def verified_live_provider_process_identity(
        self, task: FrozenTask, lease: Lease, *, stage_name: str
    ) -> dict[str, Any]:
        """Reopen the sealed identity and prove that exact Provider is live."""

        with _ExclusiveFileLock(self.lock_path):
            identity, identity_sha256, identity_path = (
                self._provider_process_identity_locked(
                    task, lease, stage_name
                )
            )
        try:
            provider_pid = int(identity["provider_pid"])
            provider_pgid = int(identity["provider_pgid"])
            if provider_pid <= 1 or provider_pgid != provider_pid:
                raise ValueError("provider process group invalid")
            require_kernel_process_start_token(
                provider_pid, str(identity["process_start_token"])
            )
            if os.getpgid(provider_pid) != provider_pgid:
                raise ValueError("provider process group mismatch")
        except (KeyError, TypeError, ValueError, OSError, ProcessIdentityError) as exc:
            raise DispatchError("provider_process_not_live") from exc
        return {
            "provider_process_identity_sha256": identity_sha256,
            "provider_process_identity_path": identity_path,
            "provider_pid": provider_pid,
            "provider_pgid": provider_pgid,
            "process_start_token": str(identity["process_start_token"]),
            "stage_name": str(identity["stage_name"]),
        }

    def publish_provider_kernel_probe_receipt(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        provider_process_identity_sha256: str,
        provider_process_identity_path: str,
        probe_nonce_sha256: str,
        previous_probe_receipt_sha256: str | None,
        baseline_progress_receipt_sha256: str | None,
        observed_progress_receipt_sha256: str | None,
        control_channel_ok: bool,
        response_identity_verified: bool,
        signed_progress_delta_ok: bool,
        baseline_kernel_snapshot: Mapping[str, Any],
        final_kernel_snapshot: Mapping[str, Any],
        kernel_activity_delta: Mapping[str, Any],
        probe_started_at: str,
        probe_finished_at: str,
    ) -> dict[str, Any]:
        """Seal one exact-Provider two-channel stall probe."""

        for value in (
            provider_process_identity_sha256,
            probe_nonce_sha256,
        ):
            _validate_unit_sha256(value)
        for value in (
            previous_probe_receipt_sha256,
            baseline_progress_receipt_sha256,
            observed_progress_receipt_sha256,
        ):
            if value is not None:
                _validate_unit_sha256(value)
        if any(
            not isinstance(value, bool)
            for value in (
                control_channel_ok,
                response_identity_verified,
                signed_progress_delta_ok,
            )
        ):
            raise DispatchError("provider_kernel_probe_boolean_invalid")
        _parse_utc(probe_started_at)
        _parse_utc(probe_finished_at)
        with _ExclusiveFileLock(self.lock_path):
            identity, identity_sha256, identity_path = (
                self._provider_process_identity_locked(
                    task, lease, stage_name
                )
            )
            if (
                identity_sha256 != provider_process_identity_sha256
                or identity_path != provider_process_identity_path
            ):
                raise DispatchError("provider_kernel_probe_identity_mismatch")
            provider_pid = int(identity["provider_pid"])
            provider_pgid = int(identity["provider_pgid"])
            process_start_token = str(identity["process_start_token"])
            for snapshot in (
                baseline_kernel_snapshot,
                final_kernel_snapshot,
            ):
                counters = snapshot.get("counters")
                if (
                    snapshot.get("schema_version")
                    != KERNEL_ACTIVITY_SNAPSHOT_SCHEMA
                    or snapshot.get("signal_source")
                    != "darwin_libproc_taskinfo_rusage_v2"
                    or snapshot.get("provider_pid") != provider_pid
                    or snapshot.get("provider_pgid") != provider_pgid
                    or snapshot.get("process_start_token")
                    != process_start_token
                    or not isinstance(counters, Mapping)
                    or set(counters) != set(KERNEL_ACTIVITY_COUNTERS)
                    or any(
                        isinstance(raw, bool)
                        or not isinstance(raw, int)
                        or raw < 0
                        for raw in counters.values()
                    )
                ):
                    raise DispatchError(
                        "provider_kernel_probe_snapshot_invalid"
                    )
            delta_counters = kernel_activity_delta.get("counters")
            kernel_activity_detected = kernel_activity_delta.get(
                "activity_detected"
            )
            if (
                kernel_activity_delta.get("signal_source")
                != "darwin_libproc_taskinfo_rusage_v2"
                or kernel_activity_delta.get("provider_pid") != provider_pid
                or kernel_activity_delta.get("provider_pgid") != provider_pgid
                or kernel_activity_delta.get("process_start_token")
                != process_start_token
                or not isinstance(delta_counters, Mapping)
                or set(delta_counters) != set(KERNEL_ACTIVITY_COUNTERS)
                or any(
                    isinstance(raw, bool)
                    or not isinstance(raw, int)
                    or raw < 0
                    for raw in delta_counters.values()
                )
                or not isinstance(kernel_activity_detected, bool)
                or kernel_activity_detected
                is not any(raw > 0 for raw in delta_counters.values())
            ):
                raise DispatchError("provider_kernel_probe_delta_invalid")
            baseline_sha256 = kernel_activity_snapshot_sha256(
                baseline_kernel_snapshot
            )
            final_sha256 = kernel_activity_snapshot_sha256(
                final_kernel_snapshot
            )
            latest_path = (
                self.provider_kernel_probe_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(stage_name)}.json"
            )
            previous = self._read_object(latest_path)
            indexed_previous_sha256 = (
                str(previous.get("provider_kernel_probe_receipt_sha256"))
                if isinstance(previous, Mapping)
                else None
            )
            if indexed_previous_sha256 != previous_probe_receipt_sha256:
                raise DispatchError(
                    "provider_kernel_probe_previous_receipt_mismatch"
                )
            sequence = (
                int(previous.get("sequence") or 0) + 1
                if isinstance(previous, Mapping)
                else 1
            )
            receipt = self._seal(
                {
                    "schema_version": PROVIDER_KERNEL_PROBE_RECEIPT_SCHEMA,
                    "sequence": sequence,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "subject": identity["subject"],
                    "capture_id": identity["capture_id"],
                    "release_id": identity["release_id"],
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": identity["stage_name"],
                    "provider_process_identity_sha256": identity_sha256,
                    "provider_process_identity_path": identity_path,
                    "provider_pid": provider_pid,
                    "provider_pgid": provider_pgid,
                    "process_start_token": process_start_token,
                    "probe_nonce_sha256": probe_nonce_sha256,
                    "previous_probe_receipt_sha256": (
                        previous_probe_receipt_sha256
                    ),
                    "baseline_progress_receipt_sha256": (
                        baseline_progress_receipt_sha256
                    ),
                    "observed_progress_receipt_sha256": (
                        observed_progress_receipt_sha256
                    ),
                    "control_channel_ok": control_channel_ok,
                    "response_identity_verified": response_identity_verified,
                    "signed_progress_delta_ok": signed_progress_delta_ok,
                    "kernel_activity_signal_available": True,
                    "baseline_kernel_snapshot_sha256": baseline_sha256,
                    "baseline_kernel_snapshot": copy.deepcopy(
                        dict(baseline_kernel_snapshot)
                    ),
                    "final_kernel_snapshot_sha256": final_sha256,
                    "final_kernel_snapshot": copy.deepcopy(
                        dict(final_kernel_snapshot)
                    ),
                    "kernel_activity_delta": copy.deepcopy(
                        dict(kernel_activity_delta)
                    ),
                    "kernel_activity_detected": kernel_activity_detected,
                    "automatic_stall_cancellation_eligible": True,
                    "probe_started_at": probe_started_at,
                    "probe_finished_at": probe_finished_at,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                },
                purpose="dispatch-provider-kernel-probe",
            )
            digest, path = _publish_content_addressed(
                self.provider_kernel_probe_receipt_root
                / _safe_component(str(identity["subject"]))
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(str(identity["stage_name"])),
                receipt,
            )
            _atomic_replace_json(
                latest_path,
                {
                    "schema_version": (
                        "study-intake-provider-kernel-probe-index-v1"
                    ),
                    "sequence": sequence,
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": identity["stage_name"],
                    "provider_kernel_probe_receipt_sha256": digest,
                    "provider_kernel_probe_receipt_path": str(path),
                    "probe_finished_at": probe_finished_at,
                    "formal_write_count": 0,
                },
            )
            return {
                "provider_kernel_probe_receipt_sha256": digest,
                "provider_kernel_probe_receipt_path": str(path),
                "kernel_activity_detected": kernel_activity_detected,
                "signed_progress_delta_ok": signed_progress_delta_ok,
                "control_channel_ok": control_channel_ok,
                "response_identity_verified": response_identity_verified,
                "provider_process_identity_sha256": identity_sha256,
                "provider_pid": provider_pid,
                "provider_pgid": provider_pgid,
                "process_start_token": process_start_token,
            }

    def verify_provider_kernel_probe_receipt(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        probe_nonce_sha256: str,
        provider_kernel_probe_receipt_sha256: str,
        provider_kernel_probe_receipt_path: str,
    ) -> dict[str, Any]:
        """Reopen one sealed probe before it can authorize cancellation.

        A runner-returned boolean is never stall evidence.  This verifier
        reopens the content-addressed HMAC receipt, its exact Provider
        identity, both Darwin counter snapshots, and any claimed signed
        progress delta under the current lease fence.
        """

        for value in (
            probe_nonce_sha256,
            provider_kernel_probe_receipt_sha256,
        ):
            _validate_unit_sha256(value)
        subject = str(task.frozen_payload.get("subject") or "")
        capture_id = str(task.frozen_payload.get("capture_id") or "")
        contract = task.frozen_payload.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id") or "")
            if isinstance(contract, Mapping)
            else ""
        )
        checked_stage = self._provider_stage_name(subject, stage_name)
        receipt_path = Path(provider_kernel_probe_receipt_path)
        expected_path = (
            self.provider_kernel_probe_receipt_root
            / _safe_component(subject)
            / task.unit_sha256
            / f"fence-{lease.fence}"
            / _safe_component(checked_stage)
            / "sha256"
            / provider_kernel_probe_receipt_sha256[:2]
            / f"{provider_kernel_probe_receipt_sha256}.json"
        )
        if receipt_path.resolve() != expected_path.resolve():
            raise DispatchError("provider_kernel_probe_receipt_path_mismatch")
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            try:
                raw = receipt_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "provider_kernel_probe_receipt_missing"
                ) from exc
            if _sha256_bytes(raw) != provider_kernel_probe_receipt_sha256:
                raise DispatchError(
                    "provider_kernel_probe_receipt_hash_mismatch"
                )
            receipt = self._read_object(receipt_path)
            if not isinstance(receipt, Mapping):
                raise DispatchError("provider_kernel_probe_receipt_missing")
            self._verify_seal(
                receipt, purpose="dispatch-provider-kernel-probe"
            )
            identity, identity_sha256, identity_path = (
                self._provider_process_identity_locked(
                    task, lease, checked_stage
                )
            )
            if (
                receipt.get("schema_version")
                != PROVIDER_KERNEL_PROBE_RECEIPT_SCHEMA
                or isinstance(receipt.get("sequence"), bool)
                or not isinstance(receipt.get("sequence"), int)
                or int(receipt["sequence"]) < 1
                or receipt.get("unit_sha256") != task.unit_sha256
                or receipt.get("frozen_payload_sha256")
                != task.frozen_payload_sha256
                or receipt.get("subject") != subject
                or receipt.get("capture_id") != capture_id
                or receipt.get("release_id") != release_id
                or receipt.get("owner_id") != lease.owner_id
                or receipt.get("lease_fence") != lease.fence
                or receipt.get("stage_name") != checked_stage
                or receipt.get("provider_process_identity_sha256")
                != identity_sha256
                or receipt.get("provider_process_identity_path")
                != identity_path
                or receipt.get("provider_pid")
                != identity.get("provider_pid")
                or receipt.get("provider_pgid")
                != identity.get("provider_pgid")
                or receipt.get("process_start_token")
                != identity.get("process_start_token")
                or receipt.get("probe_nonce_sha256")
                != probe_nonce_sha256
                or receipt.get("kernel_activity_signal_available") is not True
                or receipt.get("automatic_stall_cancellation_eligible")
                is not True
                or receipt.get("formal_write_count") != 0
                or receipt.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "provider_kernel_probe_receipt_binding_mismatch"
                )
            previous_probe_receipt_sha256 = receipt.get(
                "previous_probe_receipt_sha256"
            )
            if previous_probe_receipt_sha256 is not None:
                _validate_unit_sha256(str(previous_probe_receipt_sha256))
            sequence = int(receipt["sequence"])
            if (sequence == 1) is not (
                previous_probe_receipt_sha256 is None
            ):
                raise DispatchError(
                    "provider_kernel_probe_receipt_chain_invalid"
                )
            if sequence > 1:
                previous_path = (
                    self.provider_kernel_probe_receipt_root
                    / _safe_component(subject)
                    / task.unit_sha256
                    / f"fence-{lease.fence}"
                    / _safe_component(checked_stage)
                    / "sha256"
                    / str(previous_probe_receipt_sha256)[:2]
                    / f"{previous_probe_receipt_sha256}.json"
                )
                try:
                    previous_raw = previous_path.read_bytes()
                except OSError as exc:
                    raise DispatchError(
                        "provider_kernel_probe_previous_receipt_missing"
                    ) from exc
                previous_receipt = self._read_object(previous_path)
                if (
                    not isinstance(previous_receipt, Mapping)
                    or _sha256_bytes(previous_raw)
                    != previous_probe_receipt_sha256
                ):
                    raise DispatchError(
                        "provider_kernel_probe_previous_receipt_missing"
                    )
                self._verify_seal(
                    previous_receipt,
                    purpose="dispatch-provider-kernel-probe",
                )
                if (
                    previous_receipt.get("sequence") != sequence - 1
                    or previous_receipt.get("unit_sha256")
                    != task.unit_sha256
                    or previous_receipt.get("owner_id") != lease.owner_id
                    or previous_receipt.get("lease_fence") != lease.fence
                    or previous_receipt.get("stage_name") != checked_stage
                    or previous_receipt.get(
                        "provider_process_identity_sha256"
                    )
                    != identity_sha256
                    or previous_receipt.get("final_kernel_snapshot_sha256")
                    != receipt.get("baseline_kernel_snapshot_sha256")
                    or previous_receipt.get("final_kernel_snapshot")
                    != receipt.get("baseline_kernel_snapshot")
                ):
                    raise DispatchError(
                        "provider_kernel_probe_receipt_chain_invalid"
                    )
            for key in (
                "control_channel_ok",
                "response_identity_verified",
                "signed_progress_delta_ok",
                "kernel_activity_detected",
            ):
                if not isinstance(receipt.get(key), bool):
                    raise DispatchError(
                        "provider_kernel_probe_receipt_boolean_invalid"
                    )
            if (
                receipt.get("response_identity_verified") is True
                and receipt.get("control_channel_ok") is not True
            ):
                raise DispatchError(
                    "provider_kernel_probe_response_identity_invalid"
                )
            baseline = receipt.get("baseline_kernel_snapshot")
            final = receipt.get("final_kernel_snapshot")
            delta = receipt.get("kernel_activity_delta")
            if not all(
                isinstance(value, Mapping)
                for value in (baseline, final, delta)
            ):
                raise DispatchError(
                    "provider_kernel_probe_snapshot_invalid"
                )
            try:
                baseline_sha256 = kernel_activity_snapshot_sha256(baseline)
                final_sha256 = kernel_activity_snapshot_sha256(final)
                computed_delta = kernel_process_activity_delta(
                    baseline, final
                )
            except ProcessIdentityError as exc:
                raise DispatchError(
                    "provider_kernel_probe_snapshot_invalid"
                ) from exc
            if (
                receipt.get("baseline_kernel_snapshot_sha256")
                != baseline_sha256
                or receipt.get("final_kernel_snapshot_sha256")
                != final_sha256
                or dict(delta) != computed_delta
                or receipt.get("kernel_activity_detected")
                is not bool(computed_delta["activity_detected"])
            ):
                raise DispatchError("provider_kernel_probe_delta_invalid")
            baseline_progress_sha256 = receipt.get(
                "baseline_progress_receipt_sha256"
            )
            observed_progress_sha256 = receipt.get(
                "observed_progress_receipt_sha256"
            )
            for value in (
                baseline_progress_sha256,
                observed_progress_sha256,
            ):
                if value is not None:
                    _validate_unit_sha256(str(value))
            if receipt.get("signed_progress_delta_ok") is True:
                if (
                    not isinstance(observed_progress_sha256, str)
                    or observed_progress_sha256
                    == baseline_progress_sha256
                ):
                    raise DispatchError(
                        "provider_kernel_probe_progress_delta_invalid"
                    )
                progress_path = (
                    self.stage_progress_receipt_root
                    / _safe_component(subject)
                    / task.unit_sha256
                    / f"fence-{lease.fence}"
                    / _safe_component(checked_stage)
                    / "sha256"
                    / observed_progress_sha256[:2]
                    / f"{observed_progress_sha256}.json"
                )
                try:
                    progress_raw = progress_path.read_bytes()
                except OSError as exc:
                    raise DispatchError(
                        "provider_kernel_probe_progress_receipt_missing"
                    ) from exc
                progress = self._read_object(progress_path)
                if (
                    not isinstance(progress, Mapping)
                    or _sha256_bytes(progress_raw)
                    != observed_progress_sha256
                ):
                    raise DispatchError(
                        "provider_kernel_probe_progress_receipt_missing"
                    )
                self._verify_seal(
                    progress, purpose="dispatch-stage-progress"
                )
                if (
                    progress.get("unit_sha256") != task.unit_sha256
                    or progress.get("owner_id") != lease.owner_id
                    or progress.get("lease_fence") != lease.fence
                    or progress.get("stage_name") != checked_stage
                    or progress.get("progress_kind")
                    not in {"provider_output", "mcp_call"}
                    or progress.get("provider_pid")
                    != identity.get("provider_pid")
                    or progress.get("provider_pgid")
                    != identity.get("provider_pgid")
                    or progress.get("process_start_token")
                    != identity.get("process_start_token")
                ):
                    raise DispatchError(
                        "provider_kernel_probe_progress_binding_mismatch"
                    )
            _parse_utc(str(receipt.get("probe_started_at") or ""))
            _parse_utc(str(receipt.get("probe_finished_at") or ""))
            return {
                "provider_kernel_activity_ok": receipt[
                    "kernel_activity_detected"
                ],
                "provider_data_plane_ok": receipt[
                    "signed_progress_delta_ok"
                ],
                "control_channel_ok": receipt["control_channel_ok"],
                "response_identity_verified": receipt[
                    "response_identity_verified"
                ],
                "automatic_stall_cancellation_eligible": True,
                "provider_kernel_probe_receipt_sha256": (
                    provider_kernel_probe_receipt_sha256
                ),
                "provider_kernel_probe_receipt_path": str(receipt_path),
                "provider_process_identity_sha256": identity_sha256,
                "provider_process_identity_path": identity_path,
            }

    def publish_provider_process_exit(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        provider_process_identity_sha256: str,
        provider_process_identity_path: str,
        returncode: int,
        termination_reason: str,
        reaped: bool,
        process_absent: bool,
        pgid_absent: bool,
        finished_at: str,
    ) -> dict[str, Any]:
        """Seal proof that the exact Provider process group was reaped."""

        subject = str(task.frozen_payload.get("subject") or "")
        checked_stage = self._provider_stage_name(subject, stage_name)
        if (
            isinstance(returncode, bool)
            or not isinstance(returncode, int)
            or termination_reason
            not in {"completed", "cancelled", "timed_out", "launch_failed"}
            or reaped is not True
            or process_absent is not True
            or pgid_absent is not True
        ):
            raise DispatchError("provider_process_exit_invalid")
        _parse_utc(finished_at)
        _validate_unit_sha256(provider_process_identity_sha256)
        identity_path = Path(provider_process_identity_path)
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            try:
                identity_bytes = identity_path.read_bytes()
            except OSError as exc:
                raise DispatchError("provider_process_identity_missing") from exc
            identity = self._read_object(identity_path)
            if (
                _sha256_bytes(identity_bytes)
                != provider_process_identity_sha256
                or identity is None
            ):
                raise DispatchError("provider_process_identity_hash_mismatch")
            self._verify_seal(
                identity, purpose="dispatch-provider-process-identity"
            )
            if (
                identity.get("schema_version")
                != PROVIDER_PROCESS_IDENTITY_SCHEMA
                or identity.get("unit_sha256") != task.unit_sha256
                or identity.get("owner_id") != lease.owner_id
                or identity.get("lease_fence") != lease.fence
                or identity.get("stage_name") != checked_stage
            ):
                raise DispatchError("provider_process_identity_binding_mismatch")
            try:
                parse_kernel_process_start_token(
                    str(identity.get("process_start_token") or ""),
                    expected_pid=int(identity.get("provider_pid") or 0),
                )
            except (ProcessIdentityError, TypeError, ValueError) as exc:
                raise DispatchError(
                    "provider_process_start_token_invalid"
                ) from exc
            exit_value = self._seal(
                {
                    "schema_version": PROVIDER_PROCESS_EXIT_SCHEMA,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "subject": subject,
                    "capture_id": str(
                        task.frozen_payload.get("capture_id") or ""
                    ),
                    "release_id": identity["release_id"],
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": checked_stage,
                    "provider_process_identity_sha256": (
                        provider_process_identity_sha256
                    ),
                    "provider_process_identity_path": str(identity_path),
                    "provider_pid": identity["provider_pid"],
                    "provider_pgid": identity["provider_pgid"],
                    "process_start_token": identity["process_start_token"],
                    "returncode": returncode,
                    "termination_reason": termination_reason,
                    "reaped": True,
                    "process_absent": True,
                    "pgid_absent": True,
                    "late_result_publish_allowed": (
                        termination_reason == "completed"
                    ),
                    "finished_at": finished_at,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                },
                purpose="dispatch-provider-process-exit",
            )
            exit_sha256, exit_path = _publish_content_addressed(
                self.provider_process_exit_root
                / _safe_component(subject)
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(checked_stage),
                exit_value,
            )
            index_path = (
                self.provider_process_exit_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(checked_stage)}.json"
            )
            _publish_named_immutable(
                index_path,
                {
                    "schema_version": (
                        "study-intake-provider-process-exit-index-v1"
                    ),
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": checked_stage,
                    "provider_process_exit_sha256": exit_sha256,
                    "provider_process_exit_path": str(exit_path),
                    "provider_process_identity_sha256": (
                        provider_process_identity_sha256
                    ),
                    "formal_write_count": 0,
                },
            )
            return {
                "provider_process_exit_sha256": exit_sha256,
                "provider_process_exit_path": str(exit_path),
                "provider_process_exit": exit_value,
            }

    def _provider_process_closures_locked(
        self, task: FrozenTask, lease: Lease
    ) -> dict[str, Any]:
        subject = str(task.frozen_payload.get("subject") or "")
        closures: dict[str, Any] = {}
        for stage_name in (
            f"{subject}_analysis",
            f"{subject}_critical_review",
        ):
            identity_index_path = (
                self.provider_process_identity_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(stage_name)}.json"
            )
            identity_index = self._read_object(identity_index_path)
            if identity_index is None:
                continue
            identity_sha256 = str(
                identity_index.get("provider_process_identity_sha256") or ""
            )
            identity_path = Path(
                str(identity_index.get("provider_process_identity_path") or "")
            )
            _validate_unit_sha256(identity_sha256)
            try:
                identity_bytes = identity_path.read_bytes()
            except OSError as exc:
                raise DispatchError("provider_process_identity_missing") from exc
            identity = self._read_object(identity_path)
            if (
                _sha256_bytes(identity_bytes) != identity_sha256
                or identity is None
            ):
                raise DispatchError("provider_process_identity_hash_mismatch")
            self._verify_seal(
                identity, purpose="dispatch-provider-process-identity"
            )
            exit_index_path = (
                self.provider_process_exit_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(stage_name)}.json"
            )
            exit_index = self._read_object(exit_index_path)
            if exit_index is None:
                raise DispatchError("provider_process_exit_missing")
            exit_sha256 = str(
                exit_index.get("provider_process_exit_sha256") or ""
            )
            exit_path = Path(
                str(exit_index.get("provider_process_exit_path") or "")
            )
            _validate_unit_sha256(exit_sha256)
            try:
                exit_bytes = exit_path.read_bytes()
            except OSError as exc:
                raise DispatchError("provider_process_exit_missing") from exc
            exit_value = self._read_object(exit_path)
            if _sha256_bytes(exit_bytes) != exit_sha256 or exit_value is None:
                raise DispatchError("provider_process_exit_hash_mismatch")
            self._verify_seal(
                exit_value, purpose="dispatch-provider-process-exit"
            )
            if (
                identity.get("schema_version")
                != PROVIDER_PROCESS_IDENTITY_SCHEMA
                or exit_value.get("schema_version")
                != PROVIDER_PROCESS_EXIT_SCHEMA
                or identity.get("unit_sha256") != task.unit_sha256
                or exit_value.get("unit_sha256") != task.unit_sha256
                or identity.get("owner_id") != lease.owner_id
                or exit_value.get("owner_id") != lease.owner_id
                or identity.get("lease_fence") != lease.fence
                or exit_value.get("lease_fence") != lease.fence
                or identity.get("stage_name") != stage_name
                or exit_value.get("stage_name") != stage_name
                or exit_value.get("provider_process_identity_sha256")
                != identity_sha256
                or exit_value.get("process_start_token")
                != identity.get("process_start_token")
                or exit_value.get("reaped") is not True
                or exit_value.get("process_absent") is not True
                or exit_value.get("pgid_absent") is not True
            ):
                raise DispatchError("provider_process_closure_binding_invalid")
            closures[stage_name] = {
                "provider_process_identity_sha256": identity_sha256,
                "provider_process_identity_path": str(identity_path),
                "provider_process_exit_sha256": exit_sha256,
                "provider_process_exit_path": str(exit_path),
                "provider_pid": identity["provider_pid"],
                "provider_pgid": identity["provider_pgid"],
                "process_start_token": identity["process_start_token"],
                "returncode": exit_value["returncode"],
                "termination_reason": exit_value["termination_reason"],
                "reaped": True,
                "process_absent": True,
                "pgid_absent": True,
            }
        return closures

    def _stage_execution_binding_locked(
        self, task: FrozenTask, lease: Lease, stage_name: str
    ) -> dict[str, Any]:
        current = self._read_object(self._lease_path(lease.unit_sha256))
        if not self._matches(current, lease, status="claimed"):
            raise DispatchError("stale_lease_fence")
        subject = str(task.frozen_payload.get("subject") or "")
        checked_stage = self._provider_stage_name(subject, stage_name)
        semantic_stage = self._semantic_stage_name(subject, checked_stage)
        contract = task.frozen_payload.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id") or "")
            if isinstance(contract, Mapping)
            else ""
        )
        _validate_unit_sha256(release_id)
        return {
            "unit_sha256": task.unit_sha256,
            "owner_id": lease.owner_id,
            "lease_fence": lease.fence,
            "attempt": lease.fence,
            "subject": subject,
            "capture_id": str(task.frozen_payload.get("capture_id") or ""),
            "release_id": release_id,
            "frozen_payload_sha256": task.frozen_payload_sha256,
            "stage_name": semantic_stage,
            "provider_stage_name": checked_stage,
        }

    def publish_model_stage_raw_output(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        raw_output: bytes,
        provider_stdout: bytes,
        provider_stderr: bytes,
        provider_returncode: int,
        provider_schema_sha256: str,
        provider_process_identity_sha256: str,
        raw_chain_manifest_sha256: str,
        raw_chain_manifest_ref: str,
        raw_chain_total_chunk_count: int,
        raw_chain_reconstruction_sha256: str,
        captured_at: str | None = None,
    ) -> dict[str, Any]:
        """Publish exact Provider result bytes before parsing or validation."""

        if not isinstance(raw_output, bytes):
            raise DispatchError("model_stage_raw_output_invalid")
        for digest in (
            provider_schema_sha256,
            provider_process_identity_sha256,
            raw_chain_manifest_sha256,
            raw_chain_reconstruction_sha256,
        ):
            _validate_unit_sha256(digest)
        if raw_chain_manifest_ref != (
            "study-intake-model-stage-raw-chain-manifest://sha256/"
            + raw_chain_manifest_sha256
        ):
            raise DispatchError("model_stage_raw_chain_binding_invalid")
        timestamp = captured_at or _utc_now()
        _parse_utc(timestamp)
        with _ExclusiveFileLock(self.lock_path):
            binding = self._stage_execution_binding_locked(
                task, lease, stage_name
            )
            provider_stage = binding["provider_stage_name"]
            _identity, identity_sha256, _identity_path = (
                self._provider_process_identity_locked(
                    task, lease, provider_stage
                )
            )
            if identity_sha256 != provider_process_identity_sha256:
                raise DispatchError("model_stage_raw_identity_mismatch")
            manifest_path = (
                self.model_stage_raw_chain_manifest_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage)
                / "sha256"
                / raw_chain_manifest_sha256[:2]
                / f"{raw_chain_manifest_sha256}.json"
            )
            manifest = self._read_object(manifest_path)
            if (
                manifest is None
                or _sha256_bytes(manifest_path.read_bytes())
                != raw_chain_manifest_sha256
                or manifest.get("execution_binding")
                != {
                    **binding,
                    "provider_process_identity_sha256": identity_sha256,
                }
                or manifest.get("provider_process_identity_sha256")
                != identity_sha256
                or manifest.get("provider_schema_sha256")
                != provider_schema_sha256
                or manifest.get("total_chunk_count")
                != raw_chain_total_chunk_count
                or manifest.get("reconstruction_sha256")
                != raw_chain_reconstruction_sha256
                or manifest.get("output_last_message_final_sha256")
                != _sha256_bytes(raw_output)
                or manifest.get("chains", {}).get("stdout", {}).get(
                    "reconstructed_sha256"
                )
                != _sha256_bytes(provider_stdout)
                or manifest.get("chains", {}).get("stderr", {}).get(
                    "reconstructed_sha256"
                )
                != _sha256_bytes(provider_stderr)
            ):
                raise DispatchError("model_stage_raw_chain_binding_invalid")
            self._verify_seal(
                manifest,
                purpose="study-intake-model-stage-raw-chain-manifest",
            )
            raw_binding = {
                **binding,
                "provider_process_identity_sha256": identity_sha256,
            }
            value = {
                "schema_version": MODEL_STAGE_RAW_OUTPUT_SCHEMA,
                "stage_name": binding["stage_name"],
                "provider_stage_name": provider_stage,
                "execution_binding": raw_binding,
                "captured_at": timestamp,
                "provider_process_identity_sha256": identity_sha256,
                "raw_chain_manifest_sha256": raw_chain_manifest_sha256,
                "raw_chain_manifest_ref": raw_chain_manifest_ref,
                "raw_chain_total_chunk_count": raw_chain_total_chunk_count,
                "raw_chain_reconstruction_sha256": (
                    raw_chain_reconstruction_sha256
                ),
                "raw_output_sha256": _sha256_bytes(raw_output),
                "raw_output_size": len(raw_output),
                "raw_output_encoding": "base64",
                "raw_output_base64": base64.b64encode(raw_output).decode("ascii"),
                "provider_stdout_sha256": _sha256_bytes(provider_stdout),
                "provider_stderr_sha256": _sha256_bytes(provider_stderr),
                "provider_returncode": int(provider_returncode),
                "provider_schema_sha256": provider_schema_sha256,
                "formal_write_count": 0,
            }
            digest, path = _publish_content_addressed(
                self.model_stage_raw_output_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage),
                value,
            )
            return {
                "raw_output_object_sha256": digest,
                "raw_output_object_path": str(path),
                "raw_output_object_ref": (
                    "study-intake-model-stage-raw-output://sha256/" + digest
                ),
            }

    def _raw_chunk_path_locked(
        self,
        binding: Mapping[str, Any],
        lease: Lease,
        stream: str,
        digest: str,
    ) -> Path:
        _validate_unit_sha256(digest)
        return (
            self.model_stage_raw_chunk_root
            / _safe_component(str(binding["subject"]))
            / str(binding["unit_sha256"])
            / f"fence-{lease.fence}"
            / _safe_component(str(binding["provider_stage_name"]))
            / _safe_component(stream)
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )

    def _load_raw_chunk_chain_locked(
        self,
        binding: Mapping[str, Any],
        lease: Lease,
        *,
        stream: str,
        head_sha256: str | None,
    ) -> tuple[list[dict[str, Any]], bytes]:
        expected_semantics = (
            "snapshot"
            if stream == "output_last_message_checkpoint"
            else "append"
        )
        reverse: list[dict[str, Any]] = []
        seen: set[str] = set()
        current = head_sha256
        while current is not None:
            if current in seen:
                raise DispatchError("model_stage_raw_chunk_cycle")
            seen.add(current)
            path = self._raw_chunk_path_locked(binding, lease, stream, current)
            value = self._read_object(path)
            if (
                value is None
                or _sha256_bytes(path.read_bytes()) != current
                or value.get("execution_binding") != binding
                or value.get("stream") != stream
                or value.get("chunk_semantics") != expected_semantics
                or value.get("provider_process_identity_sha256")
                != binding.get("provider_process_identity_sha256")
            ):
                raise DispatchError("model_stage_raw_chunk_binding_invalid")
            self._verify_seal(
                value, purpose="study-intake-model-stage-raw-chunk"
            )
            try:
                chunk = base64.b64decode(
                    str(value["chunk_base64"]), validate=True
                )
            except (KeyError, ValueError, binascii.Error) as exc:
                raise DispatchError("model_stage_raw_chunk_invalid") from exc
            if (
                _sha256_bytes(chunk) != value.get("chunk_sha256")
                or len(chunk) != value.get("chunk_size")
            ):
                raise DispatchError("model_stage_raw_chunk_hash_mismatch")
            reverse.append({**dict(value), "_bytes": chunk, "_sha256": current})
            current = value.get("previous_chunk_sha256")
        rows = list(reversed(reverse))
        if any(row.get("sequence") != index for index, row in enumerate(rows, 1)):
            raise DispatchError("model_stage_raw_chunk_sequence_invalid")
        if expected_semantics == "append":
            reconstructed = bytearray()
            for row in rows:
                if (
                    row.get("offset_start") != len(reconstructed)
                    or row.get("offset_end")
                    != len(reconstructed) + len(row["_bytes"])
                    or row.get("cumulative_size") != row.get("offset_end")
                ):
                    raise DispatchError("model_stage_raw_chunk_offset_invalid")
                reconstructed.extend(row["_bytes"])
            return rows, bytes(reconstructed)
        reconstructed = rows[-1]["_bytes"] if rows else b""
        for row in rows:
            if (
                row.get("offset_start") != 0
                or row.get("offset_end") != len(row["_bytes"])
                or row.get("cumulative_size") != len(row["_bytes"])
            ):
                raise DispatchError("model_stage_raw_chunk_offset_invalid")
        return rows, reconstructed

    def publish_model_stage_raw_chunk(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        provider_process_identity_sha256: str,
        stream: str,
        chunk: bytes,
        written_at: str | None = None,
    ) -> dict[str, Any]:
        """Content-address one Provider chunk before it may renew progress."""

        if stream not in {"stdout", "stderr", "output_last_message_checkpoint"}:
            raise DispatchError("model_stage_raw_chunk_stream_invalid")
        if not isinstance(chunk, bytes) or len(chunk) > 64 * 1024:
            raise DispatchError("model_stage_raw_chunk_invalid")
        if not chunk and stream != "output_last_message_checkpoint":
            raise DispatchError("model_stage_raw_chunk_invalid")
        _validate_unit_sha256(provider_process_identity_sha256)
        timestamp = written_at or _utc_now()
        _parse_utc(timestamp)
        with _ExclusiveFileLock(self.lock_path):
            binding = self._stage_execution_binding_locked(task, lease, stage_name)
            provider_stage = binding["provider_stage_name"]
            _identity, identity_sha256, _identity_path = (
                self._provider_process_identity_locked(
                    task, lease, provider_stage
                )
            )
            if identity_sha256 != provider_process_identity_sha256:
                raise DispatchError("model_stage_raw_identity_mismatch")
            chunk_binding = {
                **binding,
                "provider_process_identity_sha256": identity_sha256,
            }
            latest_path = (
                self.model_stage_raw_chunk_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage)
                / f"{_safe_component(stream)}.json"
            )
            latest = self._read_object(latest_path)
            previous_sha256 = (
                str(latest.get("head_chunk_sha256"))
                if isinstance(latest, Mapping)
                else None
            )
            rows, reconstructed = self._load_raw_chunk_chain_locked(
                chunk_binding,
                lease,
                stream=stream,
                head_sha256=previous_sha256,
            )
            sequence = len(rows) + 1
            append = stream != "output_last_message_checkpoint"
            offset_start = len(reconstructed) if append else 0
            offset_end = offset_start + len(chunk)
            value = self._seal(
                {
                    "schema_version": MODEL_STAGE_RAW_CHUNK_SCHEMA,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "execution_binding": chunk_binding,
                    "provider_process_identity_sha256": identity_sha256,
                    "stream": stream,
                    "chunk_semantics": "append" if append else "snapshot",
                    "sequence": sequence,
                    "previous_chunk_sha256": previous_sha256,
                    "offset_start": offset_start,
                    "offset_end": offset_end,
                    "chunk_size": len(chunk),
                    "cumulative_size": offset_end if append else len(chunk),
                    "chunk_sha256": _sha256_bytes(chunk),
                    "chunk_base64": base64.b64encode(chunk).decode("ascii"),
                    "written_at": timestamp,
                    "formal_write_count": 0,
                },
                purpose="study-intake-model-stage-raw-chunk",
            )
            digest, path = _publish_content_addressed(
                self.model_stage_raw_chunk_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage)
                / _safe_component(stream),
                value,
            )
            _atomic_replace_json(
                latest_path,
                {
                    "schema_version": "study-intake-model-stage-raw-chunk-index-v1",
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "stream": stream,
                    "sequence": sequence,
                    "head_chunk_sha256": digest,
                    "head_chunk_path": str(path),
                    "written_at": timestamp,
                    "formal_write_count": 0,
                },
            )
            return {
                "raw_chunk_sha256": digest,
                "raw_chunk_path": str(path),
                "sequence": sequence,
                "stream": stream,
                "cumulative_size": offset_end if append else len(chunk),
            }

    def publish_model_stage_raw_chain_manifest(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        provider_process_identity_sha256: str,
        provider_returncode: int,
        provider_schema_sha256: str,
        completed_at: str | None = None,
    ) -> dict[str, Any]:
        for digest in (
            provider_process_identity_sha256,
            provider_schema_sha256,
        ):
            _validate_unit_sha256(digest)
        timestamp = completed_at or _utc_now()
        _parse_utc(timestamp)
        with _ExclusiveFileLock(self.lock_path):
            binding = self._stage_execution_binding_locked(task, lease, stage_name)
            provider_stage = binding["provider_stage_name"]
            _identity, identity_sha256, _identity_path = (
                self._provider_process_identity_locked(
                    task, lease, provider_stage
                )
            )
            if identity_sha256 != provider_process_identity_sha256:
                raise DispatchError("model_stage_raw_identity_mismatch")
            chain_binding = {
                **binding,
                "provider_process_identity_sha256": identity_sha256,
            }
            summaries: dict[str, Any] = {}
            reconstructed: dict[str, bytes] = {}
            total_chunks = 0
            for stream in ("stdout", "stderr", "output_last_message_checkpoint"):
                latest_path = (
                    self.model_stage_raw_chunk_latest_root
                    / task.unit_sha256
                    / f"fence-{lease.fence}"
                    / _safe_component(provider_stage)
                    / f"{_safe_component(stream)}.json"
                )
                latest = self._read_object(latest_path)
                head = (
                    str(latest.get("head_chunk_sha256"))
                    if isinstance(latest, Mapping)
                    else None
                )
                rows, content = self._load_raw_chunk_chain_locked(
                    chain_binding, lease, stream=stream, head_sha256=head
                )
                total_chunks += len(rows)
                reconstructed[stream] = content
                summaries[stream] = {
                    "chunk_count": len(rows),
                    "chunk_semantics": (
                        "snapshot"
                        if stream == "output_last_message_checkpoint"
                        else "append"
                    ),
                    "head_chunk_sha256": head,
                    "reconstructed_sha256": _sha256_bytes(content),
                    "total_size": len(content),
                }
            reconstruction_sha256 = _sha256_bytes(
                _canonical_bytes(
                    {
                        stream: {
                            "sha256": _sha256_bytes(content),
                            "size": len(content),
                        }
                        for stream, content in sorted(reconstructed.items())
                    }
                )
            )
            value = self._seal(
                {
                    "schema_version": MODEL_STAGE_RAW_CHAIN_MANIFEST_SCHEMA,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "execution_binding": chain_binding,
                    "provider_process_identity_sha256": identity_sha256,
                    "provider_returncode": int(provider_returncode),
                    "provider_schema_sha256": provider_schema_sha256,
                    "chains": summaries,
                    "total_chunk_count": total_chunks,
                    "output_last_message_final_sha256": _sha256_bytes(
                        reconstructed["output_last_message_checkpoint"]
                    ),
                    "output_last_message_final_size": len(
                        reconstructed["output_last_message_checkpoint"]
                    ),
                    "reconstruction_sha256": reconstruction_sha256,
                    "completed_at": timestamp,
                    "formal_write_count": 0,
                },
                purpose="study-intake-model-stage-raw-chain-manifest",
            )
            digest, path = _publish_content_addressed(
                self.model_stage_raw_chain_manifest_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage),
                value,
            )
            return {
                "raw_chain_manifest_sha256": digest,
                "raw_chain_manifest_path": str(path),
                "raw_chain_manifest_ref": (
                    "study-intake-model-stage-raw-chain-manifest://sha256/"
                    + digest
                ),
                "raw_chain_total_chunk_count": total_chunks,
                "raw_chain_reconstruction_sha256": reconstruction_sha256,
                "provider_stdout": reconstructed["stdout"],
                "provider_stderr": reconstructed["stderr"],
                "raw_output": reconstructed["output_last_message_checkpoint"],
            }

    def publish_model_stage_execution_receipt(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        execution_status: str,
        raw_output_object_sha256: str | None,
        raw_output_object_ref: str | None,
        provider_process_identity_sha256: str | None,
        provider_process_exit_sha256: str | None,
        authority_snapshot_manifest_sha256: str | None,
        mcp_grounding_manifest_sha256: str | None,
        mcp_transport_sha256: str | None,
        mcp_transcript_sha256: str | None,
        attempted_mcp_tool_call_count: int,
        successful_mcp_tool_call_count: int,
        grounding_mcp_tool_call_count: int,
        failed_mcp_tool_call_count: int,
        last_mcp_error_code: str | None,
        provider_returncode: int | None,
        duration_ms: int,
    ) -> dict[str, Any]:
        if execution_status not in {"completed", "failed", "cancelled", "stalled"}:
            raise DispatchError("model_stage_execution_status_invalid")
        counts = (
            attempted_mcp_tool_call_count,
            successful_mcp_tool_call_count,
            grounding_mcp_tool_call_count,
            failed_mcp_tool_call_count,
        )
        if (
            any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in counts
            )
            or attempted_mcp_tool_call_count
            != successful_mcp_tool_call_count + failed_mcp_tool_call_count
            or grounding_mcp_tool_call_count
            > successful_mcp_tool_call_count
            or isinstance(duration_ms, bool)
            or not isinstance(duration_ms, int)
            or duration_ms < 0
            or (failed_mcp_tool_call_count == 0)
            != (last_mcp_error_code is None)
            or (
                last_mcp_error_code is not None
                and (
                    not isinstance(last_mcp_error_code, str)
                    or not last_mcp_error_code
                )
            )
        ):
            raise DispatchError("model_stage_execution_count_invalid")
        for value in (
            raw_output_object_sha256,
            provider_process_identity_sha256,
            provider_process_exit_sha256,
            authority_snapshot_manifest_sha256,
            mcp_grounding_manifest_sha256,
            mcp_transport_sha256,
            mcp_transcript_sha256,
        ):
            if value is not None:
                _validate_unit_sha256(value)
        if (raw_output_object_sha256 is None) != (raw_output_object_ref is None):
            raise DispatchError("model_stage_execution_raw_binding_invalid")
        if (
            provider_process_identity_sha256 is None
            or provider_process_exit_sha256 is None
        ):
            raise DispatchError("model_stage_execution_process_closure_missing")
        if execution_status == "completed" and any(
            value is None
            for value in (
                raw_output_object_sha256,
                authority_snapshot_manifest_sha256,
                mcp_grounding_manifest_sha256,
                mcp_transport_sha256,
                mcp_transcript_sha256,
            )
        ):
            raise DispatchError("model_stage_execution_mcp_closure_missing")
        if raw_output_object_sha256 is not None and raw_output_object_ref != (
            "study-intake-model-stage-raw-output://sha256/"
            + raw_output_object_sha256
        ):
            raise DispatchError("model_stage_execution_raw_binding_invalid")
        with _ExclusiveFileLock(self.lock_path):
            base_binding = self._stage_execution_binding_locked(
                task, lease, stage_name
            )
            provider_stage = base_binding["provider_stage_name"]
            if provider_process_identity_sha256 is None:
                raise DispatchError("model_stage_execution_identity_missing")
            _identity, identity_sha256, _identity_path = (
                self._provider_process_identity_locked(
                    task, lease, provider_stage
                )
            )
            if identity_sha256 != provider_process_identity_sha256:
                raise DispatchError("model_stage_execution_identity_mismatch")
            process_closure = self._provider_process_closures_locked(
                task, lease
            ).get(provider_stage)
            if (
                not isinstance(process_closure, Mapping)
                or process_closure.get("provider_process_identity_sha256")
                != identity_sha256
                or process_closure.get("provider_process_exit_sha256")
                != provider_process_exit_sha256
            ):
                raise DispatchError("model_stage_execution_exit_mismatch")
            binding = {
                **base_binding,
                "provider_process_identity_sha256": identity_sha256,
            }
            if raw_output_object_sha256 is not None:
                raw_path = (
                    self.model_stage_raw_output_root
                    / _safe_component(binding["subject"])
                    / task.unit_sha256
                    / f"fence-{lease.fence}"
                    / _safe_component(provider_stage)
                    / "sha256"
                    / raw_output_object_sha256[:2]
                    / f"{raw_output_object_sha256}.json"
                )
                raw_object = self._read_object(raw_path)
                if (
                    raw_object is None
                    or _sha256_bytes(raw_path.read_bytes())
                    != raw_output_object_sha256
                    or raw_object.get("execution_binding") != binding
                ):
                    raise DispatchError(
                        "model_stage_execution_raw_binding_invalid"
                    )
            receipt = self._seal(
                {
                    "schema_version": MODEL_STAGE_EXECUTION_RECEIPT_SCHEMA,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "execution_status": execution_status,
                    "execution_binding": binding,
                    "raw_output_object_sha256": raw_output_object_sha256,
                    "raw_output_object_ref": raw_output_object_ref,
                    "provider_process_identity_sha256": provider_process_identity_sha256,
                    "provider_process_exit_sha256": provider_process_exit_sha256,
                    "authority_snapshot_manifest_sha256": (
                        authority_snapshot_manifest_sha256
                    ),
                    "mcp_grounding_manifest_sha256": (
                        mcp_grounding_manifest_sha256
                    ),
                    "mcp_transport_sha256": mcp_transport_sha256,
                    "mcp_transcript_sha256": mcp_transcript_sha256,
                    "attempted_mcp_tool_call_count": int(
                        attempted_mcp_tool_call_count
                    ),
                    "successful_mcp_tool_call_count": int(
                        successful_mcp_tool_call_count
                    ),
                    "grounding_mcp_tool_call_count": int(
                        grounding_mcp_tool_call_count
                    ),
                    "failed_mcp_tool_call_count": int(
                        failed_mcp_tool_call_count
                    ),
                    "last_mcp_error_code": last_mcp_error_code,
                    "provider_returncode": provider_returncode,
                    "duration_ms": int(duration_ms),
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "formal_write_count": 0,
                },
                purpose="study-intake-model-stage-execution",
            )
            digest, path = _publish_content_addressed(
                self.model_stage_execution_receipt_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage),
                receipt,
            )
            return {
                "stage_execution_receipt_sha256": digest,
                "stage_execution_receipt_path": str(path),
                "stage_execution_receipt_ref": (
                    "study-intake-model-stage-execution://sha256/" + digest
                ),
            }

    def publish_model_stage_normalization_receipt(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        execution_receipt_sha256: str,
        execution_receipt_ref: str,
        raw_output_object_sha256: str,
        raw_output_object_ref: str,
        normalization_status: str,
        normalized_payload_sha256: str | None,
        warnings: Sequence[Mapping[str, Any]],
        error_code: str | None,
    ) -> dict[str, Any]:
        if normalization_status not in {
            "normalized",
            "normalized_with_warnings",
            "failed",
        }:
            raise DispatchError("model_stage_normalization_status_invalid")
        for value in (execution_receipt_sha256, raw_output_object_sha256):
            _validate_unit_sha256(value)
        if normalized_payload_sha256 is not None:
            _validate_unit_sha256(normalized_payload_sha256)
        normalized_warnings = [copy.deepcopy(dict(row)) for row in warnings]
        with _ExclusiveFileLock(self.lock_path):
            base_binding = self._stage_execution_binding_locked(
                task, lease, stage_name
            )
            provider_stage = base_binding["provider_stage_name"]
            execution_path = (
                self.model_stage_execution_receipt_root
                / _safe_component(base_binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage)
                / "sha256"
                / execution_receipt_sha256[:2]
                / f"{execution_receipt_sha256}.json"
            )
            raw_path = (
                self.model_stage_raw_output_root
                / _safe_component(base_binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage)
                / "sha256"
                / raw_output_object_sha256[:2]
                / f"{raw_output_object_sha256}.json"
            )
            execution_value = self._read_object(execution_path)
            raw_value = self._read_object(raw_path)
            binding = (
                dict(execution_value.get("execution_binding") or {})
                if isinstance(execution_value, Mapping)
                else {}
            )
            if (
                execution_receipt_ref
                != "study-intake-model-stage-execution://sha256/"
                + execution_receipt_sha256
                or raw_output_object_ref
                != "study-intake-model-stage-raw-output://sha256/"
                + raw_output_object_sha256
                or execution_value is None
                or raw_value is None
                or any(
                    binding.get(key) != value
                    for key, value in base_binding.items()
                )
                or _sha256_bytes(execution_path.read_bytes())
                != execution_receipt_sha256
                or _sha256_bytes(raw_path.read_bytes())
                != raw_output_object_sha256
                or execution_value.get("execution_binding") != binding
                or raw_value.get("execution_binding") != binding
                or execution_value.get("raw_output_object_sha256")
                != raw_output_object_sha256
            ):
                raise DispatchError(
                    "model_stage_normalization_binding_invalid"
                )
            self._verify_seal(
                execution_value,
                purpose="study-intake-model-stage-execution",
            )
            receipt = self._seal(
                {
                    "schema_version": MODEL_STAGE_NORMALIZATION_RECEIPT_SCHEMA,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "execution_binding": binding,
                    "execution_receipt_sha256": execution_receipt_sha256,
                    "execution_receipt_ref": execution_receipt_ref,
                    "raw_output_object_sha256": raw_output_object_sha256,
                    "raw_output_object_ref": raw_output_object_ref,
                    "normalization_status": normalization_status,
                    "normalized_payload_sha256": normalized_payload_sha256,
                    "warning_count": len(normalized_warnings),
                    "warnings": normalized_warnings,
                    "error_code": error_code,
                    "normalizer_version": "deterministic-v1",
                    "formal_write_count": 0,
                },
                purpose="study-intake-model-stage-normalization",
            )
            digest, path = _publish_content_addressed(
                self.model_stage_normalization_receipt_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage),
                receipt,
            )
            return {
                "stage_normalization_receipt_sha256": digest,
                "stage_normalization_receipt_path": str(path),
                "stage_normalization_receipt_ref": (
                    "study-intake-model-stage-normalization://sha256/" + digest
                ),
            }

    def publish_stage_progress(
        self,
        task: FrozenTask,
        lease: Lease,
        *,
        stage_name: str,
        progress_kind: str,
        provider_pid: int | None = None,
        provider_pgid: int | None = None,
        process_start_token: str | None = None,
        provider_process_identity_sha256: str | None = None,
        stdout_bytes: int = 0,
        stderr_bytes: int = 0,
        mcp_tool_call_count: int = 0,
        artifact_sha256: str | None = None,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        allowed = {
            "stage_transition",
            "provider_output",
            "provider_event",
            "provider_liveness",
            "mcp_call",
            "raw_output_persisted",
            "normalization_completed",
            "provider_exit",
        }
        if progress_kind not in allowed:
            raise DispatchError("stage_progress_kind_invalid")
        timestamp = occurred_at or _utc_now()
        _parse_utc(timestamp)
        if artifact_sha256 is not None:
            _validate_unit_sha256(artifact_sha256)
        if provider_process_identity_sha256 is not None:
            _validate_unit_sha256(provider_process_identity_sha256)
        counters = (stdout_bytes, stderr_bytes, mcp_tool_call_count)
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counters
        ):
            raise DispatchError("stage_progress_counter_invalid")
        with _ExclusiveFileLock(self.lock_path):
            binding = self._stage_execution_binding_locked(
                task, lease, stage_name
            )
            provider_stage = binding["provider_stage_name"]
            provider_bound_kinds = {
                "provider_output", "provider_event", "provider_liveness",
                "mcp_call", "raw_output_persisted",
                "normalization_completed", "provider_exit",
            }
            if progress_kind in provider_bound_kinds:
                if provider_process_identity_sha256 is None:
                    raise DispatchError("stage_progress_provider_identity_missing")
                identity, identity_sha256, _identity_path = (
                    self._provider_process_identity_locked(
                        task, lease, provider_stage
                    )
                )
                if identity_sha256 != provider_process_identity_sha256:
                    raise DispatchError("stage_progress_provider_identity_mismatch")
                if any(
                    value is not None
                    for value in (provider_pid, provider_pgid, process_start_token)
                ) and (
                    provider_pid != identity.get("provider_pid")
                    or provider_pgid != identity.get("provider_pgid")
                    or process_start_token != identity.get("process_start_token")
                ):
                    raise DispatchError("stage_progress_provider_process_mismatch")
            latest_path = (
                self.stage_progress_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(provider_stage)}.json"
            )
            previous = self._read_object(latest_path)
            previous_receipt: Mapping[str, Any] | None = None
            if isinstance(previous, Mapping):
                previous_path = Path(
                    str(previous.get("progress_receipt_path") or "")
                )
                previous_receipt = self._read_object(previous_path)
                if not isinstance(previous_receipt, Mapping):
                    raise DispatchError("stage_progress_previous_missing")
                if any(
                    int(current) < int(previous_receipt.get(key) or 0)
                    for key, current in (
                        ("stdout_bytes", stdout_bytes),
                        ("stderr_bytes", stderr_bytes),
                        ("mcp_tool_call_count", mcp_tool_call_count),
                    )
                ):
                    raise DispatchError("stage_progress_counter_regression")
            previous_sha256 = (
                str(previous.get("progress_receipt_sha256"))
                if isinstance(previous, Mapping)
                else None
            )
            sequence = (
                int(previous.get("sequence") or 0) + 1
                if isinstance(previous, Mapping)
                else 1
            )
            receipt = self._seal(
                {
                    "schema_version": STAGE_PROGRESS_RECEIPT_SCHEMA,
                    "sequence": sequence,
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": binding["frozen_payload_sha256"],
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "attempt": binding["attempt"],
                    "subject": binding["subject"],
                    "capture_id": binding["capture_id"],
                    "release_id": binding["release_id"],
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "progress_kind": progress_kind,
                    "occurred_at": timestamp,
                    "provider_pid": provider_pid,
                    "provider_pgid": provider_pgid,
                    "process_start_token": process_start_token,
                    "provider_process_identity_sha256": (
                        provider_process_identity_sha256
                    ),
                    "stdout_bytes": int(stdout_bytes),
                    "stderr_bytes": int(stderr_bytes),
                    "mcp_tool_call_count": int(mcp_tool_call_count),
                    "artifact_sha256": artifact_sha256,
                    "previous_progress_receipt_sha256": previous_sha256,
                    "formal_write_count": 0,
                },
                purpose="dispatch-stage-progress",
            )
            digest, path = _publish_content_addressed(
                self.stage_progress_receipt_root
                / _safe_component(binding["subject"])
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / _safe_component(provider_stage),
                receipt,
            )
            _atomic_replace_json(
                latest_path,
                {
                    "schema_version": "study-intake-stage-progress-index-v1",
                    "sequence": sequence,
                    "unit_sha256": task.unit_sha256,
                    "owner_id": lease.owner_id,
                    "lease_fence": lease.fence,
                    "stage_name": binding["stage_name"],
                    "provider_stage_name": provider_stage,
                    "progress_receipt_sha256": digest,
                    "progress_receipt_path": str(path),
                    "occurred_at": timestamp,
                    "formal_write_count": 0,
                },
            )
            return {
                "progress_receipt_sha256": digest,
                "progress_receipt_path": str(path),
                "sequence": sequence,
                "occurred_at": timestamp,
            }

    def latest_stage_progress(
        self, task: FrozenTask, lease: Lease, *, stage_name: str
    ) -> dict[str, Any] | None:
        with _ExclusiveFileLock(self.lock_path):
            binding = self._stage_execution_binding_locked(
                task, lease, stage_name
            )
            latest_path = (
                self.stage_progress_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}"
                / f"{_safe_component(binding['provider_stage_name'])}.json"
            )
            latest = self._read_object(latest_path)
            if not isinstance(latest, Mapping):
                return None
            digest = str(latest.get("progress_receipt_sha256") or "")
            path = Path(str(latest.get("progress_receipt_path") or ""))
            _validate_unit_sha256(digest)
            receipt = self._read_object(path)
            if receipt is None or _sha256_bytes(path.read_bytes()) != digest:
                raise DispatchError("stage_progress_receipt_missing")
            self._verify_seal(receipt, purpose="dispatch-stage-progress")
            if (
                receipt.get("unit_sha256") != task.unit_sha256
                or receipt.get("owner_id") != lease.owner_id
                or receipt.get("lease_fence") != lease.fence
                or receipt.get("stage_name") != binding["stage_name"]
                or receipt.get("provider_stage_name")
                != binding["provider_stage_name"]
            ):
                raise DispatchError("stage_progress_binding_mismatch")
            return {
                "progress_receipt_sha256": digest,
                "sequence": receipt["sequence"],
                "occurred_at": receipt["occurred_at"],
                "progress_kind": receipt["progress_kind"],
                "stdout_bytes": receipt["stdout_bytes"],
                "stderr_bytes": receipt["stderr_bytes"],
                "mcp_tool_call_count": receipt["mcp_tool_call_count"],
                "artifact_sha256": receipt["artifact_sha256"],
                "provider_pid": receipt["provider_pid"],
                "provider_pgid": receipt["provider_pgid"],
                "process_start_token": receipt["process_start_token"],
            }

    def record_task_event(
        self,
        task: FrozenTask,
        lease: Lease,
        event: str,
        *,
        stage_name: str | None = None,
        error_code: str | None = None,
        artifact_refs: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        allowed = {
            "claim",
            "process_started",
            "child_process_started",
            "child_process_exited",
            "provider_process_started",
            "provider_process_exited",
            "provider_output_progress",
            "model_submitted",
            "analysis_submitted",
            "analysis_completed",
            "analysis_checkpoint_reused",
            "critical_started",
            "critical_completed",
            "recovery_scheduled",
            "retry_wait",
            "soft_timeout_warning",
            "stall_probe",
            "stall_probe_failed",
            "stage_stalled",
            "published",
            "needs_rework",
            "failed",
            "timeout",
            "cancel",
        }
        if event not in allowed:
            raise DispatchError("task_event_invalid")
        inferred_stages = {
            "claim": "dispatch",
            "process_started": "analysis",
            "child_process_started": "analysis",
            "child_process_exited": "terminal",
            "provider_process_started": "analysis",
            "provider_process_exited": "analysis",
            "provider_output_progress": "analysis",
            "model_submitted": "analysis",
            "analysis_submitted": "analysis",
            "analysis_completed": "analysis",
            "analysis_checkpoint_reused": "analysis",
            "critical_started": "critical_review",
            "critical_completed": "critical_review",
            "recovery_scheduled": "analysis",
            "retry_wait": "analysis",
            "soft_timeout_warning": "analysis",
            "stall_probe": "analysis",
            "stall_probe_failed": "analysis",
            "stage_stalled": "terminal",
            "published": "terminal",
            "needs_rework": "terminal",
            "failed": "terminal",
            "timeout": "terminal",
            "cancel": "terminal",
        }
        resolved_stage = stage_name or inferred_stages.get(event)
        if isinstance(resolved_stage, str) and resolved_stage not in {
            "dispatch",
            "analysis",
            "critical_review",
            "terminal",
        }:
            if "critical_review" in resolved_stage:
                resolved_stage = "critical_review"
            elif resolved_stage.endswith("_analysis"):
                resolved_stage = "analysis"
        if resolved_stage not in {
            "dispatch",
            "analysis",
            "critical_review",
            "terminal",
        }:
            raise DispatchError("task_event_stage_name_invalid")
        frozen = task.frozen_payload
        subject = str(frozen.get("subject") or "")
        capture_id = str(frozen.get("capture_id") or "")
        contract = frozen.get("dispatch_contract")
        release_id = (
            str(contract.get("release_id"))
            if isinstance(contract, Mapping) and contract.get("release_id")
            else None
        )
        rule_version = (
            contract.get("rule_version")
            if isinstance(contract, Mapping)
            else None
        )
        rule_version_sha256 = (
            contract.get("rule_version_sha256")
            if isinstance(contract, Mapping)
            else None
        )
        subject_processing_contract_sha256 = (
            contract.get("subject_processing_contract_sha256")
            if isinstance(contract, Mapping)
            else None
        )
        loaded_core_sha256 = (
            contract.get("loaded_core_sha256")
            if isinstance(contract, Mapping)
            else None
        )
        if loaded_core_sha256 is not None:
            _validate_unit_sha256(str(loaded_core_sha256))
        with _ExclusiveFileLock(self.lock_path):
            lease_path = self._lease_path(lease.unit_sha256)
            current = self._read_object(lease_path)
            if not (
                current
                and current.get("owner_id") == lease.owner_id
                and current.get("fence") == lease.fence
            ):
                raise DispatchError("stale_lease_fence")
            occurred_at = _utc_now()
            refreshed = dict(current)
            refreshed["heartbeat_at"] = occurred_at
            _atomic_replace_json(lease_path, refreshed)
            index_root = (
                self.task_event_index_root
                / lease.unit_sha256
                / f"fence-{lease.fence}"
            )
            sequence = len(list(index_root.glob("*.json"))) + 1
            safe_artifacts = {
                key: value
                for key, value in dict(artifact_refs or {}).items()
                if value is not None
                and key
                in {
                    "completion_path",
                    "completion_sha256",
                    "receipt_path",
                    "receipt_sha256",
                    "package_path",
                    "package_sha256",
                    "checkpoint_path",
                    "checkpoint_sha256",
                    "checkpoint_ref",
                    "checkpoint_binding_key",
                    "checkpoint_binding_sha256",
                    "process_identity_path",
                    "process_identity_sha256",
                    "process_exit_path",
                    "process_exit_sha256",
                    "provider_process_identity_path",
                    "provider_process_identity_sha256",
                    "provider_process_exit_path",
                    "provider_process_exit_sha256",
                    "raw_output_object_path",
                    "raw_output_object_sha256",
                    "provider_output_bytes_total",
                    "provider_stdout_bytes",
                    "provider_stderr_bytes",
                    "stall_probe_number",
                    "stall_probe_status",
                    "stall_probe_nonce_sha256",
                    "stall_probe_control_channel_ok",
                    "stall_probe_provider_data_plane_ok",
                    "stall_probe_provider_kernel_activity_ok",
                    "stall_probe_automatic_cancellation_eligible",
                    "provider_kernel_probe_receipt_sha256",
                    "provider_kernel_probe_receipt_path",
                    "stall_timeout_seconds",
                    "soft_timeout_seconds",
                }
            }
            previous_detail = self._read_object(
                self.task_detail_root / f"{lease.unit_sha256}.json"
            )
            meaningful_events = {
                "claim",
                "process_started",
                "child_process_started",
                "child_process_exited",
                "provider_process_started",
                "provider_process_exited",
                "provider_output_progress",
                "model_submitted",
                "analysis_submitted",
                "analysis_completed",
                "analysis_checkpoint_reused",
                "critical_started",
                "critical_completed",
                "recovery_scheduled",
                "retry_wait",
                "published",
                "needs_rework",
                "failed",
                "timeout",
                "cancel",
            }
            provider_probe_progress = event == "stall_probe" and (
                safe_artifacts.get(
                    "stall_probe_provider_kernel_activity_ok"
                )
                is True
                or safe_artifacts.get(
                    "stall_probe_provider_data_plane_ok"
                )
                is True
            )
            if event in meaningful_events or provider_probe_progress:
                last_progress_at = occurred_at
                last_progress_event = event
            else:
                last_progress_at = (
                    previous_detail.get("last_meaningful_progress_at")
                    if isinstance(previous_detail, Mapping)
                    else None
                )
                last_progress_event = (
                    previous_detail.get("last_meaningful_progress_event")
                    if isinstance(previous_detail, Mapping)
                    else None
                )
            event_value = self._seal(
                {
                    "schema_version": TASK_EVENT_SCHEMA,
                    "event": event,
                    "stage_name": resolved_stage,
                    "unit_sha256": lease.unit_sha256,
                    "owner_id": lease.owner_id,
                    "fence": lease.fence,
                    "attempt": lease.fence,
                    "subject": subject,
                    "capture_id": capture_id,
                    "release_id": release_id,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "rule_version": (
                        rule_version or DISPATCH_CONTRACT_SCHEMA
                    ),
                    "rule_version_sha256": rule_version_sha256,
                    "subject_processing_contract_sha256": (
                        subject_processing_contract_sha256
                    ),
                    "loaded_core_sha256": loaded_core_sha256,
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": (
                        REQUIRED_REASONING_EFFORT
                    ),
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                    "error_code": error_code,
                    "artifacts": safe_artifacts,
                    "occurred_at": occurred_at,
                    "formal_write_count": 0,
                },
                purpose="dispatch-task-event",
            )
            event_sha256, event_path = _publish_content_addressed(
                self.runtime_root / "dispatch" / "events", event_value
            )
            last_progress_sha256 = (
                event_sha256
                if event in meaningful_events
                else previous_detail.get("last_meaningful_progress_sha256")
                if isinstance(previous_detail, Mapping)
                else None
            )
            index_value = {
                "schema_version": "study-intake-dispatch-task-event-index-v1",
                "sequence": sequence,
                "event": event,
                "stage_name": resolved_stage,
                "occurred_at": event_value["occurred_at"],
                "event_sha256": event_sha256,
                "event_path": str(event_path),
                "unit_sha256": lease.unit_sha256,
                "owner_id": lease.owner_id,
                "fence": lease.fence,
                "attempt": lease.fence,
                "server_queue_status": (
                    "confirmed_rate_limited"
                    if event == "retry_wait"
                    and error_code is not None
                    and _is_service_retry_code(error_code)
                    else "unknown"
                ),
                "formal_write_count": 0,
            }
            index_path = index_root / f"{sequence:04d}-{event_sha256}.json"
            _publish_named_immutable(index_path, index_value)
            detail = self._seal(
                {
                    "schema_version": TASK_DETAIL_SCHEMA,
                    "unit_sha256": lease.unit_sha256,
                    "owner_id": lease.owner_id,
                    "fence": lease.fence,
                    "attempt": lease.fence,
                    "subject": subject,
                    "capture_id": capture_id,
                    "release_id": release_id,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "rule_version": (
                        rule_version or DISPATCH_CONTRACT_SCHEMA
                    ),
                    "rule_version_sha256": rule_version_sha256,
                    "subject_processing_contract_sha256": (
                        subject_processing_contract_sha256
                    ),
                    "loaded_core_sha256": loaded_core_sha256,
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": (
                        REQUIRED_REASONING_EFFORT
                    ),
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "evidence_integrity": "frozen_and_hmac_bound",
                    "phase": event,
                    "stage_name": resolved_stage,
                    "latest_event_path": str(event_path),
                    "latest_event_sha256": event_sha256,
                    "event_index_root": str(index_root),
                    "server_queue_status": (
                        "confirmed_rate_limited"
                        if event == "retry_wait"
                        and error_code is not None
                        and _is_service_retry_code(error_code)
                        else "unknown"
                    ),
                    "last_meaningful_progress_at": last_progress_at,
                    "last_meaningful_progress_event": last_progress_event,
                    "last_meaningful_progress_sha256": last_progress_sha256,
                    "soft_timeout_warning": (
                        event == "soft_timeout_warning"
                        or bool(
                            previous_detail.get("soft_timeout_warning")
                            if isinstance(previous_detail, Mapping)
                            else False
                        )
                    ),
                    "stall_probe_status": (
                        "cancelled"
                        if event == "cancel"
                        else "stalled"
                        if event == "stage_stalled"
                        else str(safe_artifacts.get("stall_probe_status"))
                        if event in {"stall_probe", "stall_probe_failed"}
                        and safe_artifacts.get("stall_probe_status")
                        in {"healthy", "stall_suspected", "probing"}
                        else "healthy"
                        if event in meaningful_events
                        else "not_applicable"
                    ),
                    "final_cancel_reason": (
                        error_code
                        if event in {"stage_stalled", "timeout", "cancel"}
                        else None
                    ),
                    "artifacts": safe_artifacts,
                    "updated_at": event_value["occurred_at"],
                    "formal_write_count": 0,
                },
                purpose="dispatch-task-detail",
            )
            _atomic_replace_json(
                self.task_detail_root / f"{lease.unit_sha256}.json", detail
            )
            return detail

    def task_progress_snapshot(
        self, task: FrozenTask, lease: Lease
    ) -> dict[str, Any]:
        """Reopen signed progress; lease heartbeats are deliberately excluded."""

        with _ExclusiveFileLock(self.lock_path):
            detail = self._read_object(
                self.task_detail_root / f"{lease.unit_sha256}.json"
            )
            if detail is None:
                raise DispatchError("task_detail_missing")
            self._verify_seal(detail, purpose="dispatch-task-detail")
            if (
                detail.get("unit_sha256") != task.unit_sha256
                or detail.get("owner_id") != lease.owner_id
                or detail.get("fence") != lease.fence
                or detail.get("frozen_payload_sha256")
                != task.frozen_payload_sha256
            ):
                raise DispatchError("task_progress_binding_mismatch")
            progress_at = detail.get("last_meaningful_progress_at")
            progress_event = detail.get("last_meaningful_progress_event")
            progress_sha256 = detail.get("last_meaningful_progress_sha256")
            if (
                not isinstance(progress_at, str)
                or not isinstance(progress_event, str)
                or not isinstance(progress_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", progress_sha256) is None
            ):
                raise DispatchError("task_progress_missing")
            _parse_utc(progress_at)
            return {
                "last_meaningful_progress_at": progress_at,
                "last_meaningful_progress_event": progress_event,
                "last_meaningful_progress_sha256": progress_sha256,
                "stage_name": detail.get("stage_name"),
                "soft_timeout_warning": bool(
                    detail.get("soft_timeout_warning")
                ),
                "stall_probe_status": detail.get("stall_probe_status"),
            }

    def heartbeat(self, lease: Lease, *, now: str | None = None) -> bool:
        timestamp = now or _utc_now()
        with _ExclusiveFileLock(self.lock_path):
            path = self._lease_path(lease.unit_sha256)
            current = self._read_object(path)
            if not self._matches(current, lease, status="claimed"):
                return False
            assert current is not None
            updated = dict(current)
            updated["heartbeat_at"] = timestamp
            _atomic_replace_json(path, updated)
            return True

    def is_current(self, lease: Lease) -> bool:
        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            return self._matches(current, lease, status="claimed")

    def run_if_current(
        self, lease: Lease, callback: Callable[[], Any]
    ) -> Any:
        """Run one short local publication while lease takeover is excluded."""

        with _ExclusiveFileLock(self.lock_path):
            current = self._read_object(self._lease_path(lease.unit_sha256))
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            return callback()

    @staticmethod
    def _matches(
        value: Mapping[str, Any] | None,
        lease: Lease,
        *,
        status: str,
    ) -> bool:
        return bool(
            value
            and value.get("schema_version") == LEASE_SCHEMA
            and value.get("unit_sha256") == lease.unit_sha256
            and value.get("owner_id") == lease.owner_id
            and value.get("fence") == lease.fence
            and value.get("status") == status
        )

    def publish_terminal(
        self,
        lease: Lease,
        *,
        task: FrozenTask,
        outcome: str,
        error_code: str | None,
        analysis: StageResult | None,
        critical_review: StageResult | None,
        report_disposition: str | None = None,
        quality_error_code: str | None = None,
        started_at: str,
        finished_at: str,
    ) -> Mapping[str, Any]:
        """Fence-check and atomically bind immutable package/receipt objects."""

        with _ExclusiveFileLock(self.lock_path):
            lease_path = self._lease_path(lease.unit_sha256)
            current = self._read_object(lease_path)
            if not self._matches(current, lease, status="claimed"):
                raise DispatchError("stale_lease_fence")
            completion_path = self._completion_path(lease.unit_sha256)
            if completion_path.exists():
                raise DispatchError("completion_already_published")

            package_sha256: str | None = None
            package_path: Path | None = None
            package_ref: str | None = None
            report_json_sha256: str | None = None
            report_json_path: Path | None = None
            report_json_ref: str | None = None
            report_markdown_sha256: str | None = None
            report_markdown_path: Path | None = None
            report_markdown_ref: str | None = None
            frozen = task.frozen_payload
            subject = str(frozen.get("subject") or "")
            capture_id = str(frozen.get("capture_id") or "")
            content_processing_id = frozen.get("content_processing_id")
            if content_processing_id is not None:
                _validate_unit_sha256(str(content_processing_id))
            dispatch_contract = frozen.get("dispatch_contract")
            release_id = (
                str(dispatch_contract.get("release_id"))
                if isinstance(dispatch_contract, Mapping)
                and dispatch_contract.get("release_id")
                else None
            )
            loaded_core_sha256 = (
                dispatch_contract.get("loaded_core_sha256")
                if isinstance(dispatch_contract, Mapping)
                else None
            )
            if loaded_core_sha256 is not None:
                _validate_unit_sha256(str(loaded_core_sha256))
            process_execution: dict[str, Any] | None = None
            supervisor_index_path = (
                self.task_process_identity_latest_root
                / task.unit_sha256
                / f"fence-{lease.fence}.json"
            )
            if supervisor_index_path.exists():
                supervisor_closure = self._task_process_closure_locked(
                    task, lease
                )
                supervisor_identity = self._read_verified_content_addressed_locked(
                    supervisor_closure["supervisor_process_identity_path"],
                    supervisor_closure["supervisor_process_identity_sha256"],
                    root=self.task_process_identity_root,
                    purpose="dispatch-task-process-identity",
                    error_code="task_process_identity_binding_mismatch",
                )
                provider_stages = self._provider_process_closures_locked(
                    task, lease
                )
                canonical_runner = (
                    Path(str(supervisor_identity["executable_path"])).resolve()
                    == (
                        Path(__file__).resolve().parents[1]
                        / "bin"
                        / "preprocess_task_runner.py"
                    ).resolve()
                )
                expected_provider_stages = {
                    f"{subject}_{stage_name}"
                    for stage_name, stage in (
                        ("analysis", analysis),
                        ("critical_review", critical_review),
                    )
                    if stage is not None
                }
                if outcome == "succeeded" and canonical_runner:
                    if set(provider_stages) != expected_provider_stages or any(
                        row.get("returncode") != 0
                        or row.get("termination_reason") != "completed"
                        or row.get("reaped") is not True
                        for row in provider_stages.values()
                    ):
                        raise DispatchError(
                            "successful_provider_process_closure_incomplete"
                        )
                process_execution = {
                    **supervisor_closure,
                    "canonical_task_runner": canonical_runner,
                    "provider_process_closure_required": canonical_runner,
                    "provider_stages": provider_stages,
                }
            rule_version = (
                dispatch_contract.get("rule_version")
                if isinstance(dispatch_contract, Mapping)
                else None
            )
            rule_version_sha256 = (
                dispatch_contract.get("rule_version_sha256")
                if isinstance(dispatch_contract, Mapping)
                else None
            )
            reviewable = bool(
                analysis is not None
                and (
                    outcome == "succeeded"
                    and report_disposition == "needs_sol_review"
                    and isinstance(quality_error_code, str)
                    and quality_error_code
                    or outcome == "failed"
                    and report_disposition == "quarantined"
                    and isinstance(error_code, str)
                    and error_code
                )
            )
            review_error_code = (
                quality_error_code
                if report_disposition == "needs_sol_review"
                else error_code
            )
            quality_review_success = bool(
                reviewable
                and report_disposition == "needs_sol_review"
            )
            published_outcome = (
                "succeeded" if quality_review_success else outcome
            )
            published_error_code = (
                None if quality_review_success else error_code
            )
            if outcome == "succeeded" or reviewable:
                if analysis is None or critical_review is None:
                    if not reviewable or analysis is None:
                        raise DispatchError("successful_package_incomplete")
                _validate_unit_sha256(str(release_id or ""))
                package = {
                    "schema_version": (
                        REVIEW_PACKAGE_SCHEMA if reviewable else PACKAGE_SCHEMA
                    ),
                    "unit_sha256": lease.unit_sha256,
                    "lease_fence": lease.fence,
                    "subject": subject,
                    "capture_id": capture_id,
                    "content_processing_id": content_processing_id,
                    "release_id": release_id,
                    "loaded_core_sha256": loaded_core_sha256,
                    "task": task.as_dict(),
                    "model_contract": {
                        "model": REQUIRED_MODEL,
                        "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    },
                    "pipeline": (
                        ["analysis", "critical_review"]
                        if critical_review is not None
                        else ["analysis"]
                    ),
                    "analysis": copy.deepcopy(dict(analysis.payload)),
                    "critical_review": (
                        copy.deepcopy(dict(critical_review.payload))
                        if critical_review is not None
                        else None
                    ),
                    "stage_runtime": {
                        "analysis": _stage_runtime(analysis),
                        "critical_review": (
                            _stage_runtime(critical_review)
                            if critical_review is not None
                            else None
                        ),
                    },
                    "formal_write_count": 0,
                }
                if reviewable:
                    warning_rows = [
                        copy.deepcopy(dict(row))
                        for stage in (analysis, critical_review)
                        if stage is not None
                        for row in stage.normalization_warnings
                        if isinstance(row, Mapping)
                    ]
                    if not warning_rows:
                        warning_rows = [
                            {
                                "code": review_error_code,
                                "kind": (
                                    "identity_anomaly"
                                    if report_disposition == "quarantined"
                                    else "semantic_validation_rejected"
                                ),
                            }
                        ]
                    package.update(
                        {
                            "execution_status": published_outcome,
                            "quality_status": (
                                "issues_found"
                                if report_disposition == "needs_sol_review"
                                else "unchecked"
                            ),
                            "terminal_status": report_disposition,
                            "report_available": True,
                            "report_disposition": report_disposition,
                            "sol_review_status": (
                                "pending"
                                if report_disposition == "needs_sol_review"
                                else "not_eligible"
                            ),
                            "formal_write_eligible": False,
                            "production_accepted": False,
                            "review_result": {
                                "status": report_disposition,
                                "error_code": review_error_code,
                            },
                            "findings": (
                                copy.deepcopy(warning_rows)
                                if report_disposition == "needs_sol_review"
                                else []
                            ),
                            "warnings": warning_rows,
                        }
                    )
                package_sha256, package_path = _publish_content_addressed(
                    self.runtime_root / "dispatch" / "packages", package
                )
                package_ref = (
                    "study-intake-dispatch-package://sha256/" + package_sha256
                )
                report_json = {
                    "schema_version": (
                        "study-intake-review-candidate-terminal-v1"
                        if reviewable
                        else "study-intake-dispatch-report-v2"
                    ),
                    "unit_sha256": lease.unit_sha256,
                    "subject": subject,
                    "capture_id": capture_id,
                    "release_id": release_id,
                    "package_ref": package_ref,
                    "package_sha256": package_sha256,
                    "analysis": copy.deepcopy(dict(analysis.payload)),
                    "critical_review": (
                        copy.deepcopy(dict(critical_review.payload))
                        if critical_review is not None
                        else None
                    ),
                    "formal_write_count": 0,
                }
                if critical_review is None:
                    report_json["critical_review"] = None
                if reviewable:
                    report_json.update(
                        {
                            "execution_status": published_outcome,
                            "quality_status": (
                                "issues_found"
                                if report_disposition == "needs_sol_review"
                                else "unchecked"
                            ),
                            "terminal_status": report_disposition,
                            "report_available": True,
                            "report_disposition": report_disposition,
                            "sol_review_status": (
                                "pending"
                                if report_disposition == "needs_sol_review"
                                else "not_eligible"
                            ),
                            "formal_write_eligible": False,
                            "production_accepted": False,
                            "review_result": copy.deepcopy(
                                package["review_result"]
                            ),
                            "findings": copy.deepcopy(package["findings"]),
                            "warnings": copy.deepcopy(package["warnings"]),
                            "stage_runtime": copy.deepcopy(
                                package["stage_runtime"]
                            ),
                        }
                    )
                report_json_sha256, report_json_path = (
                    _publish_content_addressed(
                        self.runtime_root / "dispatch" / "reports" / "json",
                        report_json,
                    )
                )
                report_json_ref = (
                    "study-intake-report://sha256/" + report_json_sha256
                )
                markdown = (
                    f"# {subject} Luna report\n\n"
                    f"- capture_id: {capture_id}\n"
                    f"- unit_sha256: {lease.unit_sha256}\n"
                    f"- report_json_ref: {report_json_ref}\n"
                    f"- package_ref: {package_ref}\n"
                    + (
                        f"- terminal_status: {report_disposition}\n"
                        "- report_available: true\n"
                        "- sol_review_status: pending\n"
                        "- formal_write_eligible: false\n"
                        if reviewable
                        else ""
                    )
                    + "- formal_write_count: 0\n"
                ).encode("utf-8")
                report_markdown_sha256, report_markdown_path = (
                    _publish_content_addressed_bytes(
                        self.runtime_root
                        / "dispatch"
                        / "reports"
                        / "markdown",
                        markdown,
                        suffix=".md",
                    )
                )
                report_markdown_ref = (
                    "study-intake-report-markdown://sha256/"
                    + report_markdown_sha256
                )

            observed = {
                "analysis": _stage_runtime(analysis) if analysis else None,
                "critical_review": (
                    _stage_runtime(critical_review) if critical_review else None
                ),
            }
            receipt_core = {
                "schema_version": RECEIPT_SCHEMA,
                "unit_sha256": lease.unit_sha256,
                "lease_fence": lease.fence,
                "subject": subject,
                "capture_id": capture_id,
                "content_processing_id": content_processing_id,
                "release_id": release_id,
                "loaded_core_sha256": loaded_core_sha256,
                "rule_version": rule_version,
                "rule_version_sha256": rule_version_sha256,
                "owner_id": lease.owner_id,
                "outcome": published_outcome,
                "error_code": published_error_code,
                "started_at": started_at,
                "finished_at": finished_at,
                "model_contract": {
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                },
                "observed_stage_runtime": observed,
                "process_execution": process_execution,
                "package_sha256": package_sha256,
                "package_path": str(package_path) if package_path else None,
                "package_ref": package_ref,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha256,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha256,
                "formal_write_count": 0,
            }
            receipt = self._seal(receipt_core, purpose="dispatch-receipt")
            receipt_sha256, receipt_path = _publish_content_addressed(
                self.runtime_root / "dispatch" / "receipts", receipt
            )
            ledger_entry = self._seal(
                {
                    "schema_version": AUTHORITY_LEDGER_SCHEMA,
                    "unit_sha256": lease.unit_sha256,
                    "lease_fence": lease.fence,
                    "subject": subject,
                    "capture_id": capture_id,
                    "release_id": release_id,
                    "loaded_core_sha256": loaded_core_sha256,
                    "rule_version": rule_version,
                    "rule_version_sha256": rule_version_sha256,
                    "outcome": published_outcome,
                    "receipt_sha256": receipt_sha256,
                    "receipt_path": str(receipt_path),
                    "package_sha256": package_sha256,
                    "model": REQUIRED_MODEL,
                    "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "recorded_at": finished_at,
                    "formal_write_count": 0,
                },
                purpose="dispatch-authority-ledger",
            )
            ledger_entry_sha256, ledger_entry_path = _publish_content_addressed(
                self.authority_ledger_root, ledger_entry
            )
            completion_core = {
                "schema_version": COMPLETION_SCHEMA,
                "unit_sha256": lease.unit_sha256,
                "lease_fence": lease.fence,
                "subject": subject,
                "capture_id": capture_id,
                "content_processing_id": content_processing_id,
                "release_id": release_id,
                "loaded_core_sha256": loaded_core_sha256,
                "rule_version": rule_version,
                "rule_version_sha256": rule_version_sha256,
                "outcome": published_outcome,
                "receipt_sha256": receipt_sha256,
                "receipt_path": str(receipt_path),
                "ledger_entry_sha256": ledger_entry_sha256,
                "ledger_entry_path": str(ledger_entry_path),
                "package_sha256": package_sha256,
                "package_path": str(package_path) if package_path else None,
                "package_ref": package_ref,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha256,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha256,
                "process_execution": process_execution,
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "finished_at": finished_at,
            }
            completion = self._seal(
                completion_core, purpose="dispatch-completion"
            )
            _publish_named_immutable(completion_path, completion)
            completion_sha256 = _sha256_bytes(completion_path.read_bytes())
            if subject and capture_id:
                group_processing_key = (
                    dispatch_contract.get("math_group_processing_key")
                    if subject == "math"
                    and isinstance(dispatch_contract, Mapping)
                    else None
                )
                raw_group_members = (
                    frozen.get("math_group_members")
                    if subject == "math"
                    else None
                )
                group_capture_ids = [capture_id]
                if raw_group_members is not None:
                    if not isinstance(raw_group_members, list):
                        raise DispatchError("math_group_members_invalid")
                    group_capture_ids = []
                    for member in raw_group_members:
                        member_capture_id = (
                            member.get("capture_id")
                            if isinstance(member, Mapping)
                            else None
                        )
                        if (
                            not isinstance(member_capture_id, str)
                            or not member_capture_id
                            or member_capture_id in group_capture_ids
                        ):
                            raise DispatchError("math_group_members_invalid")
                        group_capture_ids.append(member_capture_id)
                    if capture_id not in group_capture_ids:
                        raise DispatchError("math_group_owner_missing")
                latest_core = {
                        "schema_version": (
                            "study-intake-authoritative-latest-v1"
                        ),
                        "subject": subject,
                        "release_id": release_id,
                        "loaded_core_sha256": loaded_core_sha256,
                        "rule_version": rule_version,
                        "rule_version_sha256": rule_version_sha256,
                        "unit_sha256": lease.unit_sha256,
                        "lease_fence": lease.fence,
                        "completion_path": str(completion_path),
                        "completion_sha256": completion_sha256,
                        "receipt_sha256": receipt_sha256,
                        "ledger_entry_sha256": ledger_entry_sha256,
                        "ledger_entry_path": str(ledger_entry_path),
                        "package_sha256": package_sha256,
                        "model": REQUIRED_MODEL,
                        "reasoning_effort": REQUIRED_REASONING_EFFORT,
                        "updated_at": finished_at,
                }
                for member_capture_id in group_capture_ids:
                    latest = self._seal(
                        {
                            **latest_core,
                            "capture_id": member_capture_id,
                            "group_owner_capture_id": capture_id,
                            "group_processing_key": group_processing_key,
                            "group_capture_ids": group_capture_ids,
                        },
                        purpose="dispatch-latest",
                    )
                    _atomic_replace_json(
                        self._latest_path(subject, member_capture_id), latest
                    )
            if content_processing_id is not None:
                self._attach_content_member_publications_locked(completion)
            assert current is not None
            terminal_lease = dict(current)
            terminal_lease.update(
                {
                    "status": "completed",
                    "completed_at": finished_at,
                    "outcome": published_outcome,
                    "receipt_sha256": receipt_sha256,
                }
            )
            _atomic_replace_json(lease_path, terminal_lease)
            return completion

    def verify_authoritative_completion(
        self,
        subject: str,
        capture_id: str,
        *,
        expected_release_id: str,
        expected_unit_sha256: str | None = None,
        expected_generation: int | None = None,
        expected_input_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Verify the full current-release authority chain for one candidate."""

        if not expected_release_id:
            raise DispatchError("expected_release_id_required")
        with _ExclusiveFileLock(self.lock_path):
            latest_path = self._latest_path(subject, capture_id)
            latest = self._read_object(latest_path)
            if latest is None:
                raise DispatchError("authoritative_latest_missing")
            self._verify_seal(latest, purpose="dispatch-latest")
            if (
                expected_unit_sha256 is not None
                and latest.get("unit_sha256") != expected_unit_sha256
            ):
                raise DispatchError("authoritative_unit_generation_mismatch")
            if (
                expected_generation is not None
                and latest.get("lease_fence") != expected_generation
            ):
                raise DispatchError("authoritative_unit_generation_mismatch")
            completion_path = Path(str(latest.get("completion_path") or ""))
            expected_completion_path = self._completion_path(
                str(latest.get("unit_sha256") or "")
            )
            if completion_path.resolve() != expected_completion_path.resolve():
                raise DispatchError("completion_path_mismatch")
            completion_bytes = completion_path.read_bytes()
            if _sha256_bytes(completion_bytes) != latest.get("completion_sha256"):
                raise DispatchError("completion_content_hash_mismatch")
            completion = self._read_object(completion_path)
            if completion is None:
                raise DispatchError("completion_missing")
            self._verify_seal(completion, purpose="dispatch-completion")

            receipt_path = Path(str(completion.get("receipt_path") or ""))
            receipt_bytes = receipt_path.read_bytes()
            if _sha256_bytes(receipt_bytes) != completion.get("receipt_sha256"):
                raise DispatchError("receipt_content_hash_mismatch")
            receipt = self._read_object(receipt_path)
            if receipt is None:
                raise DispatchError("receipt_missing")
            self._verify_seal(receipt, purpose="dispatch-receipt")

            ledger_entry_sha256 = str(
                completion.get("ledger_entry_sha256") or ""
            )
            _validate_unit_sha256(ledger_entry_sha256)
            ledger_entry_path = Path(
                str(completion.get("ledger_entry_path") or "")
            )
            expected_ledger_entry_path = (
                self.authority_ledger_root
                / "sha256"
                / ledger_entry_sha256[:2]
                / f"{ledger_entry_sha256}.json"
            )
            if (
                ledger_entry_path.resolve()
                != expected_ledger_entry_path.resolve()
            ):
                raise DispatchError("authority_ledger_path_mismatch")
            ledger_entry_bytes = ledger_entry_path.read_bytes()
            if _sha256_bytes(ledger_entry_bytes) != ledger_entry_sha256:
                raise DispatchError("authority_ledger_hash_mismatch")
            ledger_entry = self._read_object(ledger_entry_path)
            if ledger_entry is None:
                raise DispatchError("authority_ledger_entry_missing")
            self._verify_seal(
                ledger_entry, purpose="dispatch-authority-ledger"
            )

            owner_capture_id = latest.get("group_owner_capture_id")
            if owner_capture_id is None:
                owner_capture_id = capture_id
            if not isinstance(owner_capture_id, str) or not owner_capture_id:
                raise DispatchError("latest_group_owner_invalid")
            latest_common = {
                "subject": subject,
                "capture_id": capture_id,
                "release_id": expected_release_id,
                "unit_sha256": latest.get("unit_sha256"),
                "lease_fence": latest.get("lease_fence"),
            }
            if any(
                latest.get(key) != expected
                for key, expected in latest_common.items()
            ):
                raise DispatchError("latest_binding_mismatch")
            owner_common = {
                **latest_common,
                "capture_id": owner_capture_id,
            }
            for value, label in (
                (completion, "completion"),
                (receipt, "receipt"),
                (ledger_entry, "authority_ledger"),
            ):
                if any(
                    value.get(key) != expected
                    for key, expected in owner_common.items()
                ):
                    raise DispatchError(f"{label}_binding_mismatch")
            group_capture_ids = latest.get("group_capture_ids")
            if (
                group_capture_ids is not None
                and (
                    not isinstance(group_capture_ids, list)
                    or capture_id not in group_capture_ids
                    or owner_capture_id not in group_capture_ids
                    or len(group_capture_ids) != len(set(group_capture_ids))
                )
            ):
                raise DispatchError("latest_group_members_invalid")
            rule_version_sha256 = latest.get("rule_version_sha256")
            if rule_version_sha256 is not None:
                _validate_unit_sha256(str(rule_version_sha256))
            for value, label in (
                (completion, "completion"),
                (receipt, "receipt"),
                (ledger_entry, "authority_ledger"),
            ):
                if (
                    value.get("rule_version")
                    != latest.get("rule_version")
                    or value.get("rule_version_sha256")
                    != rule_version_sha256
                ):
                    raise DispatchError(f"{label}_rule_binding_mismatch")
            if any(
                value.get("model") != REQUIRED_MODEL
                or value.get("reasoning_effort") != REQUIRED_REASONING_EFFORT
                for value in (latest, completion)
            ) or receipt.get("model_contract") != {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            } or (
                ledger_entry.get("model") != REQUIRED_MODEL
                or ledger_entry.get("reasoning_effort")
                != REQUIRED_REASONING_EFFORT
            ):
                raise DispatchError("authority_model_contract_mismatch")
            if (
                latest.get("receipt_sha256") != completion.get("receipt_sha256")
                or completion.get("package_sha256") != receipt.get("package_sha256")
                or latest.get("package_sha256") != completion.get("package_sha256")
                or latest.get("ledger_entry_sha256")
                != completion.get("ledger_entry_sha256")
                or latest.get("ledger_entry_path")
                != completion.get("ledger_entry_path")
                or ledger_entry.get("receipt_sha256")
                != completion.get("receipt_sha256")
                or ledger_entry.get("receipt_path")
                != completion.get("receipt_path")
                or ledger_entry.get("package_sha256")
                != completion.get("package_sha256")
            ):
                raise DispatchError("authority_artifact_binding_mismatch")

            package: dict[str, Any] | None = None
            package_sha256 = completion.get("package_sha256")
            if package_sha256 is not None:
                package_path = Path(str(completion.get("package_path") or ""))
                package_bytes = package_path.read_bytes()
                if _sha256_bytes(package_bytes) != package_sha256:
                    raise DispatchError("package_content_hash_mismatch")
                package = self._read_object(package_path)
                if package is None:
                    raise DispatchError("package_missing")
                package_task = package.get("task")
                package_frozen = (
                    package_task.get("frozen_payload")
                    if isinstance(package_task, Mapping)
                    else None
                )
                package_contract = (
                    package_frozen.get("dispatch_contract")
                    if isinstance(package_frozen, Mapping)
                    else None
                )
                package_members = (
                    package_frozen.get("content_group_members")
                    if isinstance(package_frozen, Mapping)
                    else None
                )
                package_member = next(
                    (
                        row
                        for row in package_members
                        if isinstance(row, Mapping)
                        and row.get("capture_id") == capture_id
                    ),
                    package_frozen
                    if isinstance(package_frozen, Mapping)
                    and package_frozen.get("capture_id") == capture_id
                    else None,
                ) if isinstance(package_members, list) else (
                    package_frozen
                    if isinstance(package_frozen, Mapping)
                    and package_frozen.get("capture_id") == capture_id
                    else None
                )
                if (
                    package.get("unit_sha256") != owner_common["unit_sha256"]
                    or package.get("lease_fence") != owner_common["lease_fence"]
                    or package.get("subject") != subject
                    or package.get("capture_id") != owner_capture_id
                    or package.get("release_id") != expected_release_id
                    or (
                        rule_version_sha256 is not None
                        and (
                            not isinstance(package_contract, Mapping)
                            or
                            package_contract.get("rule_version")
                            != latest.get("rule_version")
                            or package_contract.get("rule_version_sha256")
                            != rule_version_sha256
                        )
                    )
                    or package.get("model_contract")
                    != {
                        "model": REQUIRED_MODEL,
                        "reasoning_effort": REQUIRED_REASONING_EFFORT,
                    }
                ):
                    raise DispatchError("package_binding_mismatch")
                if (
                    expected_input_fingerprint is not None
                    and (
                        not isinstance(package_member, Mapping)
                        or package_member.get("input_fingerprint")
                        != expected_input_fingerprint
                    )
                ):
                    raise DispatchError("package_input_generation_mismatch")
                package_runtime = package.get("stage_runtime")
                observed_runtime = receipt.get("observed_stage_runtime")
                quality_review_success = bool(
                    completion.get("outcome") == "succeeded"
                    and package.get("schema_version")
                    == REVIEW_PACKAGE_SCHEMA
                    and package.get("report_available") is True
                    and package.get("report_disposition")
                    == "needs_sol_review"
                )
                if (
                    not isinstance(package_runtime, Mapping)
                    or not isinstance(observed_runtime, Mapping)
                    or set(package_runtime)
                    != {"analysis", "critical_review"}
                    or observed_runtime != package_runtime
                ):
                    raise DispatchError("authority_stage_runtime_binding_mismatch")
                for stage in ("analysis", "critical_review"):
                    runtime = package_runtime.get(stage)
                    if (
                        stage == "critical_review"
                        and runtime is None
                        and quality_review_success
                    ):
                        continue
                    if not isinstance(runtime, Mapping):
                        raise DispatchError(
                            "authority_stage_runtime_binding_mismatch"
                        )
                    StageResult.coerce({"payload": {}, **dict(runtime)})
            elif completion.get("outcome") == "succeeded":
                raise DispatchError("successful_completion_package_missing")
            member_publication: dict[str, Any] | None = None
            member_publication_sha256 = latest.get(
                "member_publication_sha256"
            )
            if member_publication_sha256 is not None:
                _validate_unit_sha256(str(member_publication_sha256))
                member_path = Path(
                    str(latest.get("member_publication_path") or "")
                )
                expected_member_path = (
                    self.content_member_publication_root
                    / "sha256"
                    / str(member_publication_sha256)[:2]
                    / f"{member_publication_sha256}.json"
                )
                if member_path.resolve() != expected_member_path.resolve():
                    raise DispatchError("content_member_publication_path_mismatch")
                member_bytes = member_path.read_bytes()
                if _sha256_bytes(member_bytes) != member_publication_sha256:
                    raise DispatchError("content_member_publication_hash_mismatch")
                member_publication = self._read_object(member_path)
                if member_publication is None:
                    raise DispatchError("content_member_publication_missing")
                self._verify_seal(
                    member_publication,
                    purpose="dispatch-content-member-publication",
                )
                if (
                    member_publication.get("schema_version")
                    != CONTENT_MEMBER_PUBLICATION_SCHEMA
                    or member_publication.get("unit_sha256")
                    != latest.get("unit_sha256")
                    or member_publication.get("lease_fence")
                    != latest.get("lease_fence")
                    or member_publication.get("subject") != subject
                    or member_publication.get("capture_id") != capture_id
                    or member_publication.get("release_id")
                    != expected_release_id
                    or member_publication.get("completion_sha256")
                    != latest.get("completion_sha256")
                    or member_publication.get("package_sha256")
                    != latest.get("package_sha256")
                    or member_publication.get("content_processing_id")
                    != latest.get("content_processing_id")
                    or member_publication.get("formal_write_count") != 0
                ):
                    raise DispatchError(
                        "content_member_publication_binding_mismatch"
                    )
            return {
                "latest": latest,
                "completion": completion,
                "receipt": receipt,
                "ledger_entry": ledger_entry,
                "package": package,
                "member_publication": member_publication,
            }

    def verify_authoritative_task_detail(
        self,
        unit_sha256: str,
        *,
        expected_release_id: str,
    ) -> dict[str, Any]:
        """Verify a lightweight detail envelope and its latest event object."""

        _validate_unit_sha256(unit_sha256)
        if not expected_release_id:
            raise DispatchError("expected_release_id_required")
        with _ExclusiveFileLock(self.lock_path):
            detail = self._read_object(
                self.task_detail_root / f"{unit_sha256}.json"
            )
            if detail is None:
                raise DispatchError("task_detail_missing")
            self._verify_seal(detail, purpose="dispatch-task-detail")
            event_path = Path(str(detail.get("latest_event_path") or ""))
            try:
                event_bytes = event_path.read_bytes()
            except OSError as exc:
                raise DispatchError("task_event_missing") from exc
            if _sha256_bytes(event_bytes) != detail.get("latest_event_sha256"):
                raise DispatchError("task_event_content_hash_mismatch")
            event = self._read_object(event_path)
            if event is None:
                raise DispatchError("task_event_missing")
            self._verify_seal(event, purpose="dispatch-task-event")
            for value, label in ((detail, "task_detail"), (event, "task_event")):
                if (
                    value.get("unit_sha256") != unit_sha256
                    or value.get("release_id") != expected_release_id
                    or value.get("fence") != detail.get("fence")
                    or value.get("attempt") != detail.get("attempt")
                ):
                    raise DispatchError(f"{label}_binding_mismatch")
            if event.get("event") != detail.get("phase"):
                raise DispatchError("task_detail_phase_mismatch")
            event_stage = event.get("stage_name")
            detail_stage = detail.get("stage_name")
            if (
                event_stage != detail_stage
                or (
                    event_stage is not None
                    and event_stage not in {
                        "dispatch",
                        "analysis",
                        "critical_review",
                        "terminal",
                    }
                )
            ):
                raise DispatchError("task_detail_stage_name_mismatch")
            if detail.get("rule_version_sha256") is not None:
                expected_rule = dispatch_rule_binding(
                    release_id=expected_release_id,
                    subject=str(detail.get("subject") or ""),
                    subject_processing_contract_sha256=detail.get(
                        "subject_processing_contract_sha256"
                    ),
                )
                for value, label in (
                    (detail, "task_detail"),
                    (event, "task_event"),
                ):
                    if any(
                        value.get(key) != expected
                        for key, expected in expected_rule.items()
                    ):
                        raise DispatchError(
                            f"{label}_rule_binding_mismatch"
                        )
                if (
                    detail.get("model") != REQUIRED_MODEL
                    or detail.get("reasoning_effort")
                    != REQUIRED_REASONING_EFFORT
                ):
                    raise DispatchError("task_detail_model_binding_mismatch")
            return {"detail": detail, "latest_event": event}

    def verify_task_event_history(
        self,
        unit_sha256: str,
        *,
        expected_release_id: str,
    ) -> dict[str, Any]:
        """Verify the complete immutable event history for one task attempt."""

        verified = self.verify_authoritative_task_detail(
            unit_sha256,
            expected_release_id=expected_release_id,
        )
        detail = verified["detail"]
        fence = detail.get("fence")
        expected_root = (
            self.task_event_index_root
            / unit_sha256
            / f"fence-{fence}"
        ).resolve()
        if Path(str(detail.get("event_index_root") or "")).resolve() != expected_root:
            raise DispatchError("task_event_index_root_mismatch")
        rows: list[dict[str, Any]] = []
        with _ExclusiveFileLock(self.lock_path):
            unit_root = self.task_event_index_root / unit_sha256
            attempt_roots = sorted(
                (
                    path
                    for path in unit_root.glob("fence-*")
                    if path.is_dir()
                    and path.name.removeprefix("fence-").isdigit()
                ),
                key=lambda path: int(path.name.removeprefix("fence-")),
            )
            if not attempt_roots or attempt_roots[-1].resolve() != expected_root:
                raise DispatchError("task_event_history_missing")
            for attempt_root in attempt_roots:
                attempt = int(attempt_root.name.removeprefix("fence-"))
                index_paths = sorted(attempt_root.glob("*.json"))
                if not index_paths:
                    raise DispatchError("task_event_history_missing")
                for sequence, index_path in enumerate(index_paths, start=1):
                    index = self._read_object(index_path)
                    if index is None:
                        raise DispatchError("task_event_index_missing")
                    event_sha256 = str(index.get("event_sha256") or "")
                    _validate_unit_sha256(event_sha256)
                    event_path = Path(str(index.get("event_path") or ""))
                    expected_event_path = (
                        self.runtime_root
                        / "dispatch"
                        / "events"
                        / "sha256"
                        / event_sha256[:2]
                        / f"{event_sha256}.json"
                    ).resolve()
                    if (
                        index.get("schema_version")
                        != "study-intake-dispatch-task-event-index-v1"
                        or index.get("sequence") != sequence
                        or (
                            index.get("stage_name") is not None
                            and index.get("stage_name") not in {
                                "dispatch",
                                "analysis",
                                "critical_review",
                                "terminal",
                            }
                        )
                        or index.get("unit_sha256") != unit_sha256
                        or not isinstance(index.get("owner_id"), str)
                        or not index.get("owner_id")
                        or index.get("fence") != attempt
                        or index.get("attempt") != attempt
                        or index.get("formal_write_count") != 0
                        or event_path.resolve() != expected_event_path
                    ):
                        raise DispatchError("task_event_index_binding_mismatch")
                    try:
                        raw = expected_event_path.read_bytes()
                    except OSError as exc:
                        raise DispatchError("task_event_missing") from exc
                    if _sha256_bytes(raw) != event_sha256:
                        raise DispatchError("task_event_content_hash_mismatch")
                    event = self._read_object(expected_event_path)
                    if event is None:
                        raise DispatchError("task_event_missing")
                    self._verify_seal(event, purpose="dispatch-task-event")
                    if (
                        event.get("schema_version") != TASK_EVENT_SCHEMA
                        or event.get("event") != index.get("event")
                        or event.get("stage_name") != index.get("stage_name")
                        or event.get("occurred_at") != index.get("occurred_at")
                        or event.get("unit_sha256") != unit_sha256
                        or event.get("owner_id") != index.get("owner_id")
                        or event.get("fence") != attempt
                        or event.get("attempt") != attempt
                        or event.get("release_id") != expected_release_id
                        or event.get("requested_model") != REQUIRED_MODEL
                        or event.get("requested_reasoning_effort")
                        != REQUIRED_REASONING_EFFORT
                        or event.get("formal_write_count") != 0
                    ):
                        raise DispatchError("task_event_binding_mismatch")
                    rows.append(
                        {
                            "attempt": attempt,
                            "sequence": sequence,
                            "event_sha256": event_sha256,
                            "event_path": str(expected_event_path),
                            "event": event,
                        }
                    )
        last = rows[-1]
        if (
            last["event_sha256"] != detail.get("latest_event_sha256")
            or last["event_path"]
            != str(Path(str(detail.get("latest_event_path") or "")).resolve())
            or last["event"].get("event") != detail.get("phase")
        ):
            raise DispatchError("task_event_history_latest_mismatch")
        submission_events = {
            "analysis_submitted": "analysis",
            "critical_started": "critical_review",
        }
        submissions = [
            {
                "stage": submission_events[str(row["event"]["event"])],
                "sequence": row["sequence"],
                "event_sha256": row["event_sha256"],
                "event_path": row["event_path"],
                "requested_model": row["event"]["requested_model"],
                "requested_reasoning_effort": row["event"][
                    "requested_reasoning_effort"
                ],
                "occurred_at": row["event"]["occurred_at"],
                "formal_write_count": 0,
            }
            for row in rows
            if row["event"].get("event") in submission_events
        ]
        return {
            "detail": detail,
            "events": rows,
            "model_submissions": submissions,
            "model_call_count": len(submissions),
            "formal_write_count": 0,
        }

    def verify_analysis_checkpoint(
        self,
        checkpoint_sha256: str,
        *,
        expected_unit_sha256: str,
        expected_frozen_payload_sha256: str,
        expected_release_id: str,
    ) -> dict[str, Any]:
        """Verify one content-addressed checkpoint referenced by an event."""

        _validate_unit_sha256(checkpoint_sha256)
        _validate_unit_sha256(expected_unit_sha256)
        _validate_unit_sha256(expected_frozen_payload_sha256)
        expected_path = (
            self.analysis_checkpoint_root
            / "sha256"
            / checkpoint_sha256[:2]
            / f"{checkpoint_sha256}.json"
        )
        with _ExclusiveFileLock(self.lock_path):
            try:
                raw = expected_path.read_bytes()
            except OSError as exc:
                raise DispatchError("analysis_checkpoint_missing") from exc
            if _sha256_bytes(raw) != checkpoint_sha256:
                raise DispatchError("analysis_checkpoint_hash_mismatch")
            checkpoint = self._read_object(expected_path)
            if checkpoint is None:
                raise DispatchError("analysis_checkpoint_missing")
            self._verify_seal(
                checkpoint, purpose="dispatch-analysis-checkpoint"
            )
            if (
                checkpoint.get("schema_version") != ANALYSIS_CHECKPOINT_SCHEMA
                or checkpoint.get("unit_sha256") != expected_unit_sha256
                or checkpoint.get("frozen_payload_sha256")
                != expected_frozen_payload_sha256
                or checkpoint.get("release_id") != expected_release_id
                or checkpoint.get("model") != REQUIRED_MODEL
                or checkpoint.get("reasoning_effort")
                != REQUIRED_REASONING_EFFORT
                or checkpoint.get("formal_write_count") != 0
                or not isinstance(checkpoint.get("analysis"), Mapping)
            ):
                raise DispatchError("analysis_checkpoint_binding_mismatch")
            return {"checkpoint": checkpoint, "checkpoint_sha256": checkpoint_sha256}

    def publish_evidence_readiness(
        self,
        receipt_value: Mapping[str, Any],
        *,
        expected_release_id: str,
    ) -> dict[str, Any]:
        """Publish a deterministic, authenticated zero-model readiness fact."""

        _validate_unit_sha256(expected_release_id)
        receipt = copy.deepcopy(dict(receipt_value))
        required_keys = {
            "schema_version",
            "status",
            "capture_id",
            "evidence_manifest_sha256",
            "evidence_bundle_sha256",
            "question_mode",
            "attachment_rows_sha256",
            "required_roles",
            "present_roles",
            "missing_roles",
            "trace_full_sha256",
            "controlled_contract_path",
            "controlled_contract_sha256",
            "producer_build_sha256",
            "created_at",
            "model_enqueue_allowed",
            "formal_write_count",
        }
        if set(receipt) != required_keys:
            raise DispatchError("evidence_readiness_shape_invalid")
        status = receipt.get("status")
        mode = receipt.get("question_mode")
        capture_id = receipt.get("capture_id")
        if (
            receipt.get("schema_version")
            != "current-question-evidence-readiness-receipt-v1"
            or status not in {"ready", "evidence_pending"}
            or mode not in {"dialogue_only", "image_question"}
            or not isinstance(capture_id, str)
            or not capture_id
            or len(capture_id) > 256
            or receipt.get("formal_write_count") != 0
            or receipt.get("model_enqueue_allowed")
            is not (status == "ready")
        ):
            raise DispatchError("evidence_readiness_identity_invalid")
        for key in (
            "evidence_manifest_sha256",
            "evidence_bundle_sha256",
            "attachment_rows_sha256",
            "trace_full_sha256",
            "controlled_contract_sha256",
            "producer_build_sha256",
        ):
            try:
                _validate_unit_sha256(str(receipt.get(key) or ""))
            except DispatchError as exc:
                raise DispatchError(
                    f"evidence_readiness_{key}_invalid"
                ) from exc
        required_roles = receipt.get("required_roles")
        present_roles = receipt.get("present_roles")
        missing_roles = receipt.get("missing_roles")
        role_set = {"question_image", "solution_image"}
        if any(
            not isinstance(value, list)
            or len(value) != len(set(value))
            or any(role not in role_set for role in value)
            for value in (required_roles, present_roles, missing_roles)
        ):
            raise DispatchError("evidence_readiness_roles_invalid")
        expected_required = (
            ["question_image", "solution_image"]
            if mode == "image_question"
            else []
        )
        assert isinstance(required_roles, list)
        assert isinstance(present_roles, list)
        assert isinstance(missing_roles, list)
        if (
            required_roles != expected_required
            or missing_roles
            != [role for role in required_roles if role not in present_roles]
            or (status == "ready" and missing_roles)
            or (mode == "dialogue_only" and present_roles)
        ):
            raise DispatchError("evidence_readiness_role_coverage_invalid")
        contract_path = receipt.get("controlled_contract_path")
        if (
            not isinstance(contract_path, str)
            or not contract_path
            or len(contract_path) > 2048
            or not Path(contract_path).is_absolute()
        ):
            raise DispatchError("evidence_readiness_contract_path_invalid")
        _parse_utc(receipt.get("created_at"))

        authority_receipt = self._seal(
            {
                "schema_version": EVIDENCE_READINESS_AUTHORITY_SCHEMA,
                "subject": "cs408",
                "capture_id": capture_id,
                "release_id": expected_release_id,
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "receipt": receipt,
                "formal_write_count": 0,
            },
            purpose="evidence-readiness-receipt",
        )
        authority_sha256, authority_path = _publish_content_addressed(
            self.evidence_readiness_root / "receipts", authority_receipt
        )
        ledger_entry = self._seal(
            {
                "schema_version": (
                    "study-intake-evidence-readiness-ledger-entry-v1"
                ),
                "subject": "cs408",
                "capture_id": capture_id,
                "release_id": expected_release_id,
                "status": status,
                "authority_receipt_sha256": authority_sha256,
                "authority_receipt_path": str(authority_path),
                "evidence_manifest_sha256": receipt[
                    "evidence_manifest_sha256"
                ],
                "evidence_bundle_sha256": receipt[
                    "evidence_bundle_sha256"
                ],
                "model_enqueue_allowed": receipt[
                    "model_enqueue_allowed"
                ],
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "recorded_at": receipt["created_at"],
                "formal_write_count": 0,
            },
            purpose="evidence-readiness-ledger",
        )
        ledger_sha256, ledger_path = _publish_content_addressed(
            self.evidence_readiness_root / "ledger", ledger_entry
        )
        latest = self._seal(
            {
                "schema_version": (
                    "study-intake-evidence-readiness-latest-v1"
                ),
                "subject": "cs408",
                "capture_id": capture_id,
                "release_id": expected_release_id,
                "status": status,
                "authority_receipt_sha256": authority_sha256,
                "authority_receipt_path": str(authority_path),
                "ledger_entry_sha256": ledger_sha256,
                "ledger_entry_path": str(ledger_path),
                "model_enqueue_allowed": receipt[
                    "model_enqueue_allowed"
                ],
                "updated_at": receipt["created_at"],
                "formal_write_count": 0,
            },
            purpose="evidence-readiness-latest",
        )
        with _ExclusiveFileLock(self.lock_path):
            _atomic_replace_json(
                self.evidence_readiness_latest_root
                / "cs408"
                / f"{_safe_component(capture_id)}.json",
                latest,
            )
        return {
            "status": status,
            "model_enqueue_allowed": receipt["model_enqueue_allowed"],
            "authority_receipt_sha256": authority_sha256,
            "authority_receipt_path": str(authority_path),
            "ledger_entry_sha256": ledger_sha256,
            "ledger_entry_path": str(ledger_path),
        }

    def verify_evidence_readiness(
        self,
        receipt_sha256: str,
        *,
        expected_release_id: str,
        expected_capture_id: str,
    ) -> dict[str, Any]:
        """Verify one content-addressed readiness receipt and ledger entry."""

        _validate_unit_sha256(receipt_sha256)
        _validate_unit_sha256(expected_release_id)
        receipt_path = (
            self.evidence_readiness_root
            / "receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        receipt_bytes = receipt_path.read_bytes()
        if _sha256_bytes(receipt_bytes) != receipt_sha256:
            raise DispatchError("evidence_readiness_hash_mismatch")
        authority_receipt = self._read_object(receipt_path)
        if authority_receipt is None:
            raise DispatchError("evidence_readiness_missing")
        self._verify_seal(
            authority_receipt, purpose="evidence-readiness-receipt"
        )
        if (
            authority_receipt.get("schema_version")
            != EVIDENCE_READINESS_AUTHORITY_SCHEMA
            or authority_receipt.get("subject") != "cs408"
            or authority_receipt.get("capture_id") != expected_capture_id
            or authority_receipt.get("release_id") != expected_release_id
            or authority_receipt.get("model") != REQUIRED_MODEL
            or authority_receipt.get("reasoning_effort")
            != REQUIRED_REASONING_EFFORT
            or authority_receipt.get("formal_write_count") != 0
            or not isinstance(authority_receipt.get("receipt"), Mapping)
        ):
            raise DispatchError("evidence_readiness_binding_mismatch")
        latest_path = (
            self.evidence_readiness_latest_root
            / "cs408"
            / f"{_safe_component(expected_capture_id)}.json"
        )
        latest = self._read_object(latest_path)
        if latest is None:
            raise DispatchError("evidence_readiness_latest_missing")
        self._verify_seal(latest, purpose="evidence-readiness-latest")
        if (
            latest.get("release_id") != expected_release_id
            or latest.get("capture_id") != expected_capture_id
            or latest.get("authority_receipt_sha256") != receipt_sha256
            or latest.get("authority_receipt_path") != str(receipt_path)
        ):
            raise DispatchError("evidence_readiness_latest_binding_mismatch")
        ledger_sha256 = str(latest.get("ledger_entry_sha256") or "")
        _validate_unit_sha256(ledger_sha256)
        ledger_path = Path(str(latest.get("ledger_entry_path") or ""))
        expected_ledger_path = (
            self.evidence_readiness_root
            / "ledger"
            / "sha256"
            / ledger_sha256[:2]
            / f"{ledger_sha256}.json"
        )
        if ledger_path.resolve() != expected_ledger_path.resolve():
            raise DispatchError("evidence_readiness_ledger_path_mismatch")
        if _sha256_bytes(ledger_path.read_bytes()) != ledger_sha256:
            raise DispatchError("evidence_readiness_ledger_hash_mismatch")
        ledger = self._read_object(ledger_path)
        if ledger is None:
            raise DispatchError("evidence_readiness_ledger_missing")
        self._verify_seal(ledger, purpose="evidence-readiness-ledger")
        if (
            ledger.get("release_id") != expected_release_id
            or ledger.get("capture_id") != expected_capture_id
            or ledger.get("authority_receipt_sha256") != receipt_sha256
            or ledger.get("authority_receipt_path") != str(receipt_path)
        ):
            raise DispatchError("evidence_readiness_ledger_binding_mismatch")
        return {
            "authority_receipt": authority_receipt,
            "latest": latest,
            "ledger_entry": ledger,
        }

    @staticmethod
    def _exact_preserved_repair_file(
        path: Path, expected_sha256: str, label: str
    ) -> tuple[bytes, dict[str, Any]]:
        try:
            payload = path.read_bytes()
            value = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError(
                f"english_preserved_review_repair_{label}_unreadable"
            ) from exc
        if (
            _sha256_bytes(payload) != expected_sha256
            or not isinstance(value, Mapping)
        ):
            raise DispatchError(
                f"english_preserved_review_repair_{label}_drift"
            )
        return payload, dict(value)

    def _unique_exact_preserved_repair_object(
        self, root: Path, digest: str, label: str
    ) -> tuple[Path, dict[str, Any]]:
        matches = [
            path
            for path in root.rglob(f"{digest}.json")
            if path.is_file() and not path.is_symlink()
        ]
        if len(matches) != 1:
            raise DispatchError(
                f"english_preserved_review_repair_{label}_missing"
            )
        _payload, value = self._exact_preserved_repair_file(
            matches[0], digest, label
        )
        return matches[0], value

    def _verify_exact_preserved_legacy_seal(
        self, value: Mapping[str, Any], *, purpose: str
    ) -> None:
        document = copy.deepcopy(dict(value))
        seal = document.pop("seal", None)
        expected = hmac.new(
            self._authority_key(),
            _canonical_bytes({"purpose": purpose, "payload": document}),
            hashlib.sha256,
        ).hexdigest()
        if (
            not isinstance(seal, Mapping)
            or seal.get("algorithm") != "HMAC-SHA256"
            or seal.get("purpose") != purpose
            or not hmac.compare_digest(
                str(seal.get("hmac_sha256") or ""), expected
            )
        ):
            raise DispatchError(
                "english_preserved_review_repair_legacy_hmac_invalid"
            )

    def _verify_exact_preserved_mcp_failure_receipt(
        self, value: Mapping[str, Any]
    ) -> None:
        core = {
            key: copy.deepcopy(item)
            for key, item in value.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        expected = hmac.new(
            key,
            _canonical_bytes(
                {
                    "purpose": "mcp-read-session-model-calls-v2",
                    "payload": core,
                }
            ) + b"\n",
            hashlib.sha256,
        ).hexdigest()
        if (
            value.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(
                str(value.get("hmac_sha256") or ""), expected
            )
        ):
            raise DispatchError(
                "english_preserved_review_repair_mcp_failure_hmac_invalid"
            )

    def preview_authorized_english_preserved_review_repair(
        self,
        *,
        target_release_id: str | None = None,
        target_activation_id: str | None = None,
        target_generation: str | None = None,
        target_subject_authority_fingerprint: str | None = None,
        target_producer_authority_fingerprint: str | None = None,
        staged_target_canary_state_sha256: str | None = None,
        batch_archive: Mapping[str, Any] | None = None,
        _lock_held: bool = False,
    ) -> dict[str, Any]:
        """Read only the exact lineage in source or deployment-staged state."""

        auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        descriptor_sha256 = _sha256_bytes(_canonical_bytes(auth))
        staged = staged_target_canary_state_sha256 is not None
        staged_values = (
            target_release_id,
            target_activation_id,
            target_generation,
            target_subject_authority_fingerprint,
            target_producer_authority_fingerprint,
            staged_target_canary_state_sha256,
        )
        if staged and any(value is None for value in staged_values):
            raise DispatchError(
                "english_preserved_review_repair_staged_binding_incomplete"
            )
        if not staged and any(value is not None for value in staged_values):
            raise DispatchError(
                "english_preserved_review_repair_staged_binding_incomplete"
            )
        if staged != (batch_archive is not None):
            raise DispatchError(
                "english_preserved_review_repair_batch_archive_binding_incomplete"
            )
        if staged:
            assert target_release_id is not None
            assert target_activation_id is not None
            assert target_generation is not None
            assert target_subject_authority_fingerprint is not None
            assert target_producer_authority_fingerprint is not None
            assert staged_target_canary_state_sha256 is not None
            _validate_unit_sha256(target_release_id)
            _validate_unit_sha256(target_activation_id)
            _validate_unit_sha256(target_subject_authority_fingerprint)
            _validate_unit_sha256(target_producer_authority_fingerprint)
            _validate_unit_sha256(staged_target_canary_state_sha256)
            if not target_generation:
                raise DispatchError(
                    "english_preserved_review_repair_target_generation_invalid"
                )
        lock_context = (
            contextlib.nullcontext()
            if _lock_held
            else _ExistingSharedFileLock(self.lock_path)
        )
        with lock_context:
            state_path = self._production_canary_state_path("english")
            _state_bytes, state = self._exact_preserved_repair_file(
                state_path,
                (
                    staged_target_canary_state_sha256
                    if staged
                    else auth["gate_preimage_sha256"]
                ),
                "staged_gate" if staged else "gate",
            )
            self._verify_seal(state, purpose="dispatch-production-canary-state")
            queue_path = (
                self.production_canary_queue_root
                / "english"
                / auth["source_activation_id"]
                / f"{auth['producer_input_contract_sha256']}.json"
            )
            _queue_bytes, queue_entry = self._exact_preserved_repair_file(
                queue_path, auth["queue_preimage_sha256"], "queue"
            )
            self._verify_seal(
                queue_entry, purpose="dispatch-production-canary-queue"
            )
            terminal_index_path, terminal_index = (
                self._unique_exact_preserved_repair_object(
                    self.production_canary_terminal_index_root,
                    auth["terminal_index_preimage_sha256"],
                    "terminal_index",
                )
            )
            self._exact_preserved_repair_file(
                terminal_index_path,
                auth["terminal_index_preimage_sha256"],
                "terminal_index",
            )
            self._verify_seal(
                terminal_index,
                purpose="dispatch-production-canary-terminal-index",
            )
            task_path = (
                self.runtime_root
                / "dispatch"
                / "production-canary"
                / "tasks"
                / "english"
                / "sha256"
                / auth["task_object_sha256"][:2]
                / f"{auth['task_object_sha256']}.json"
            )
            _task_bytes, task_value = self._exact_preserved_repair_file(
                task_path, auth["task_object_sha256"], "task"
            )
            task = FrozenTask.from_mapping(task_value)
            completion_path = self._completion_path(auth["unit_sha256"])
            self._exact_preserved_repair_file(
                completion_path,
                auth["completion_preimage_sha256"],
                "completion",
            )
            lease_path = self._lease_path(auth["unit_sha256"])
            self._exact_preserved_repair_file(
                lease_path,
                auth["lease_preimage_sha256"],
                "lease",
            )
            task_detail_path = (
                self.task_detail_root / f"{auth['unit_sha256']}.json"
            )
            self._exact_preserved_repair_file(
                task_detail_path,
                auth["task_detail_preimage_sha256"],
                "task_detail",
            )
            raw_path = (
                self.model_stage_raw_output_root
                / "english"
                / auth["unit_sha256"]
                / "fence-1"
                / "english_analysis"
                / "sha256"
                / auth["raw_output_object_sha256"][:2]
                / f"{auth['raw_output_object_sha256']}.json"
            )
            _raw_bytes, raw_output = self._exact_preserved_repair_file(
                raw_path,
                auth["raw_output_object_sha256"],
                "raw_output",
            )
            execution_path = (
                self.model_stage_execution_receipt_root
                / "english"
                / auth["unit_sha256"]
                / "fence-1"
                / "english_analysis"
                / "sha256"
                / auth["stage_execution_receipt_sha256"][:2]
                / f"{auth['stage_execution_receipt_sha256']}.json"
            )
            _execution_bytes, execution = self._exact_preserved_repair_file(
                execution_path,
                auth["stage_execution_receipt_sha256"],
                "stage_execution_receipt",
            )
            processing_path = (
                self.runtime_root
                / "dispatch"
                / "receipts"
                / "sha256"
                / auth["processing_receipt_sha256"][:2]
                / f"{auth['processing_receipt_sha256']}.json"
            )
            _processing_bytes, processing = self._exact_preserved_repair_file(
                processing_path,
                auth["processing_receipt_sha256"],
                "processing_receipt",
            )
            self._verify_seal(processing, purpose="dispatch-receipt")
            terminal_path = Path(str(queue_entry.get("terminal_receipt_path") or ""))
            _terminal_bytes, terminal = self._exact_preserved_repair_file(
                terminal_path,
                auth["canary_terminal_receipt_sha256"],
                "canary_terminal",
            )
            self._verify_seal(
                terminal, purpose="dispatch-production-canary-terminal"
            )
            transport_path, transport = self._unique_exact_preserved_repair_object(
                self.runtime_root / "private" / "reports" / "model-mcp-transport",
                auth["mcp_transport_sha256"],
                "mcp_transport",
            )
            original_path, original_receipt = (
                self._unique_exact_preserved_repair_object(
                    self.production_canary_receipt_root,
                    auth["original_preclaim_failure_receipt_sha256"],
                    "original_preclaim_receipt",
                )
            )
            self._verify_seal(
                original_receipt,
                purpose="dispatch-production-canary-preclaim-failure",
            )
            rollover_path, rollover = self._unique_exact_preserved_repair_object(
                self.runtime_root
                / "dispatch"
                / "control-receipts"
                / "subject-background-rollovers",
                auth["rollover_receipt_sha256"],
                "rollover_receipt",
            )
            self._verify_exact_preserved_legacy_seal(
                rollover, purpose="subject-background-luna-rollover-v2"
            )
            supersede_path, supersede = self._unique_exact_preserved_repair_object(
                self.production_canary_receipt_root,
                auth["recovery_supersede_receipt_sha256"],
                "recovery_supersede_receipt",
            )
            self._verify_seal(
                supersede,
                purpose="dispatch-subject-batch-recovery-supersede",
            )
            failure_path, failure = self._unique_exact_preserved_repair_object(
                self.runtime_root
                / "dispatch"
                / "mcp-read-session-call-receipts",
                auth["mcp_failure_receipt_sha256"],
                "mcp_failure_receipt",
            )
            self._verify_exact_preserved_mcp_failure_receipt(failure)
            subject_terminal_path, subject_terminal = (
                self._unique_exact_preserved_repair_object(
                    self.runtime_root
                    / "dispatch"
                    / "control-receipts"
                    / "subject-terminal-v2",
                    auth["subject_terminal_receipt_sha256"],
                    "subject_terminal_receipt",
                )
            )
            self._verify_exact_preserved_legacy_seal(
                subject_terminal,
                purpose="subject-luna-terminal-receipt-v2",
            )
            batch_path = (
                self.state_root / "subject-luna-batches" / "english.json"
            )
            pointer_path = (
                self.state_root
                / "subject-luna-batch-pointers"
                / "english.json"
            )
            writer_path = self.state_root / "subject-sol" / "english.json"
            self._exact_preserved_repair_file(
                batch_path,
                auth["subject_batch_preimage_sha256"],
                "subject_batch",
            )
            self._exact_preserved_repair_file(
                pointer_path,
                auth["subject_batch_pointer_preimage_sha256"],
                "subject_batch_pointer",
            )
            if staged:
                assert isinstance(batch_archive, Mapping)
                archive_value = batch_archive.get("archive")
                archive_path = Path(str(batch_archive.get("archive_path") or ""))
                retirement_pointer_path = (
                    self.state_root
                    / "english-preserved-review-batch-retirements"
                    / "authorized-33548.json"
                )
                writer_expected = str(
                    batch_archive.get("writer_postimage_sha256") or ""
                )
                archive_expected = str(batch_archive.get("archive_sha256") or "")
                pointer_expected = str(batch_archive.get("pointer_sha256") or "")
                if (
                    not isinstance(archive_value, Mapping)
                    or batch_archive.get("target_generation") != target_generation
                    or batch_archive.get("target_authority_fingerprint")
                    != target_subject_authority_fingerprint
                    or batch_archive.get("batch_postimage_sha256")
                    != auth["subject_batch_preimage_sha256"]
                    or batch_archive.get("batch_pointer_postimage_sha256")
                    != auth["subject_batch_pointer_preimage_sha256"]
                ):
                    raise DispatchError(
                        "english_preserved_review_repair_batch_archive_invalid"
                    )
                _archive_bytes, reopened_archive = (
                    self._exact_preserved_repair_file(
                        archive_path,
                        archive_expected,
                        "subject_batch_archive",
                    )
                )
                _pointer_bytes, retirement_pointer = (
                    self._exact_preserved_repair_file(
                        retirement_pointer_path,
                        pointer_expected,
                        "subject_batch_retirement_pointer",
                    )
                )
                self._verify_exact_preserved_legacy_seal(
                    reopened_archive,
                    purpose=(
                        "english-preserved-review-batch-retirement-archive-v1"
                    ),
                )
                self._verify_exact_preserved_legacy_seal(
                    retirement_pointer,
                    purpose=(
                        "english-preserved-review-batch-retirement-pointer-v1"
                    ),
                )
                if (
                    reopened_archive != archive_value
                    or retirement_pointer.get("archive_sha256") != archive_expected
                    or retirement_pointer.get("writer_postimage_sha256")
                    != writer_expected
                    or retirement_pointer.get("target_generation")
                    != target_generation
                    or retirement_pointer.get("target_authority_fingerprint")
                    != target_subject_authority_fingerprint
                ):
                    raise DispatchError(
                        "english_preserved_review_repair_batch_archive_invalid"
                    )
                self._exact_preserved_repair_file(
                    writer_path, writer_expected, "subject_writer_postimage"
                )
            else:
                self._exact_preserved_repair_file(
                    writer_path,
                    auth["subject_writer_preimage_sha256"],
                    "subject_writer",
                )
            mcp_items = transport.get("mcp_items")
            source_gate_invalid = (
                not staged
                and (
                    state.get("state") != "failed_drained"
                    or state.get("luna_consumer_enabled") is not False
                    or state.get("release_id") != auth["source_release_id"]
                    or state.get("activation_id") != auth["source_activation_id"]
                    or state.get("last_terminal_receipt_sha256")
                    != auth["canary_terminal_receipt_sha256"]
                )
            )
            staged_gate_invalid = (
                staged
                and (
                    state.get("state") != "paused_drained"
                    or state.get("luna_consumer_enabled") is not False
                    or state.get("active_task_count") != 0
                    or state.get("selected") is not None
                    or state.get("active_selections") != {}
                    or state.get("release_id") != target_release_id
                    or state.get("activation_id") != target_activation_id
                    or state.get("producer_authority_fingerprint")
                    != target_producer_authority_fingerprint
                    or state.get("historical_eligible_count") != 0
                    or state.get("excluded_by_high_watermark_count") != 0
                    or state.get("queue_classification", {}).get(
                        "pre_activation_frozen"
                    ) != 0
                    or state.get("producer_high_watermark", {}).get(
                        "recorded_at"
                    ) != state.get("activated_at")
                    or state.get("model_call_count") != 0
                    or state.get("provider_request_count") != 0
                    or state.get("mcp_tool_call_count") != 0
                    or state.get("formal_write_count") != 0
                    or state.get("sol_enabled") is not False
                    or state.get("sol_formal_curation_enabled") is not False
                )
            )
            if (
                source_gate_invalid
                or staged_gate_invalid
                or queue_entry.get("queue_status") != "failed"
                or queue_entry.get("unit_sha256") != auth["unit_sha256"]
                or queue_entry.get("frozen_payload_sha256")
                != auth["frozen_payload_sha256"]
                or queue_entry.get("terminal_error_code")
                != "english_analysis_mcp_duplicate_read"
                or task.unit_sha256 != auth["unit_sha256"]
                or task.frozen_payload_sha256
                != auth["frozen_payload_sha256"]
                or raw_output.get("provider_returncode") != 0
                or execution.get("provider_returncode") != 0
                or execution.get("raw_output_object_sha256")
                != auth["raw_output_object_sha256"]
                or processing.get("outcome") != "failed"
                or processing.get("error_code")
                != "english_analysis_mcp_duplicate_read"
                or terminal.get("outcome") != "failed"
                or terminal.get("error_code")
                != "english_analysis_mcp_duplicate_read"
                or transport.get("schema_version")
                != "model-driven-mcp-transport-v1"
                or transport.get("subject") != "english"
                or transport.get("stage_name") != "english_analysis"
                or transport.get("mcp_item_count") != 40
                or not isinstance(mcp_items, list)
                or len(mcp_items) != 40
                or rollover.get("original_preclaim_failure_receipt_sha256")
                != auth["original_preclaim_failure_receipt_sha256"]
                or supersede.get("replacement_task", {}).get("unit_sha256")
                != auth["unit_sha256"]
                or failure.get("formal_write_count") != 0
                or subject_terminal.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "english_preserved_review_repair_binding_invalid"
                )
            return {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-preview-v1"
                ),
                "status": "ready",
                "authorization_descriptor": copy.deepcopy(auth),
                "authorization_descriptor_sha256": descriptor_sha256,
                "paths": {
                    "queue": str(queue_path),
                    "gate": str(state_path),
                    "terminal_index": str(terminal_index_path),
                    "completion": str(completion_path),
                    "lease": str(lease_path),
                    "task_detail": str(task_detail_path),
                    "task": str(task_path),
                    "raw_output": str(raw_path),
                    "mcp_transport": str(transport_path),
                    "stage_execution_receipt": str(execution_path),
                    "processing_receipt": str(processing_path),
                    "mcp_failure_receipt": str(failure_path),
                    "canary_terminal_receipt": str(terminal_path),
                    "subject_terminal_receipt": str(subject_terminal_path),
                    "original_preclaim_receipt": str(original_path),
                    "rollover_receipt": str(rollover_path),
                    "recovery_supersede_receipt": str(supersede_path),
                    "subject_batch": str(batch_path),
                    "subject_batch_pointer": str(pointer_path),
                    "subject_writer": str(writer_path),
                    **(
                        {
                            "subject_batch_archive": str(archive_path),
                            "subject_batch_retirement_pointer": str(
                                retirement_pointer_path
                            ),
                        }
                        if staged
                        else {}
                    ),
                },
                "observed_historical_model_call_count": 1,
                "observed_historical_provider_request_count": 41,
                "observed_historical_mcp_tool_call_count": 40,
                "source_gate_preimage_sha256": auth[
                    "gate_preimage_sha256"
                ],
                "staged_target_canary_state_sha256": (
                    staged_target_canary_state_sha256 if staged else None
                ),
                "target_release_id": target_release_id,
                "target_activation_id": target_activation_id,
                "target_generation": target_generation,
                "target_subject_authority_fingerprint": (
                    target_subject_authority_fingerprint
                ),
                "target_producer_authority_fingerprint": (
                    target_producer_authority_fingerprint
                ),
                "new_task_count": 0,
                "new_queue_count": 0,
                "capture_replay_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "sol_call_count": 0,
                "formal_write_count": 0,
            }

    def _english_preserved_review_source_paths(
        self, preview: Mapping[str, Any]
    ) -> dict[str, Path]:
        return {
            key: Path(str(value))
            for key, value in dict(preview["paths"]).items()
        }

    @staticmethod
    def _english_preserved_review_warning() -> dict[str, Any]:
        return {
            "code": "english_analysis_mcp_duplicate_read",
            "kind": "mcp_read_policy_rejected",
            "message": (
                "The Provider and MCP returned valid content, but one "
                "byte-equivalent repeated read was rejected by the Host policy."
            ),
        }

    @staticmethod
    def _english_preserved_review_failpoint(name: str) -> None:
        if os.environ.get(ENGLISH_REVIEW_REPAIR_FAILPOINT_ENV) == name:
            raise DispatchError(
                f"english_preserved_review_repair_failpoint_{name}"
            )

    def _restore_english_preserved_review_preimages(
        self,
        capsule: Mapping[str, Any],
        *,
        remove_repair_pointer: bool,
    ) -> dict[str, str | bool]:
        """Restore the source queue and target staged gate byte-for-byte."""

        self._verify_seal(
            capsule,
            purpose="dispatch-english-preserved-review-repair-preimage-v1",
        )
        try:
            source_queue_path = Path(str(capsule["source_queue_path"]))
            target_queue_path = Path(str(capsule["target_queue_path"]))
            gate_path = Path(str(capsule["staged_gate_path"]))
            source_queue_bytes = base64.b64decode(
                str(capsule["source_queue_bytes_base64"]), validate=True
            )
            staged_gate_bytes = base64.b64decode(
                str(capsule["staged_gate_bytes_base64"]), validate=True
            )
        except (KeyError, ValueError, binascii.Error) as exc:
            raise DispatchError(
                "english_preserved_review_repair_preimage_capsule_invalid"
            ) from exc
        auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        if (
            not source_queue_path.is_absolute()
            or not target_queue_path.is_absolute()
            or not gate_path.is_absolute()
            or _sha256_bytes(source_queue_bytes)
            != auth["queue_preimage_sha256"]
            or _sha256_bytes(staged_gate_bytes)
            != capsule.get("staged_gate_preimage_sha256")
        ):
            raise DispatchError(
                "english_preserved_review_repair_preimage_capsule_invalid"
            )
        if source_queue_path.exists() and target_queue_path.exists():
            raise DispatchError(
                "english_preserved_review_repair_rollback_queue_ambiguous"
            )
        if target_queue_path.exists():
            source_queue_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.replace(target_queue_path, source_queue_path)
        _atomic_replace_bytes(source_queue_path, source_queue_bytes)
        _atomic_replace_bytes(gate_path, staged_gate_bytes)
        if remove_repair_pointer:
            try:
                self.english_preserved_review_repair_pointer_path.unlink()
            except FileNotFoundError:
                pass
        if (
            _sha256_bytes(source_queue_path.read_bytes())
            != auth["queue_preimage_sha256"]
            or target_queue_path.exists()
            or _sha256_bytes(gate_path.read_bytes())
            != capsule["staged_gate_preimage_sha256"]
            or (
                remove_repair_pointer
                and self.english_preserved_review_repair_pointer_path.exists()
            )
        ):
            raise DispatchError(
                "english_preserved_review_repair_rollback_unverified"
            )
        return {
            "source_queue_restored_sha256": auth["queue_preimage_sha256"],
            "target_queue_absent": True,
            "staged_gate_restored_sha256": str(
                capsule["staged_gate_preimage_sha256"]
            ),
        }

    def _read_english_preserved_review_preimage_capsule(
        self, *, digest: str, path_value: str
    ) -> dict[str, Any]:
        checked = _validate_unit_sha256(digest)
        path = Path(path_value)
        expected = (
            self.english_preserved_review_repair_preimage_root
            / "sha256"
            / checked[:2]
            / f"{checked}.json"
        )
        if path.resolve() != expected.resolve():
            raise DispatchError(
                "english_preserved_review_repair_preimage_path_invalid"
            )
        payload, value = self._exact_preserved_repair_file(
            path, checked, "preimage_capsule"
        )
        del payload
        self._verify_seal(
            value,
            purpose="dispatch-english-preserved-review-repair-preimage-v1",
        )
        return value

    def apply_authorized_english_preserved_review_repair(
        self,
        *,
        target_release_id: str,
        target_activation_id: str,
        target_generation: str,
        target_subject_authority_fingerprint: str,
        target_producer_authority_fingerprint: str,
        staged_target_canary_state_sha256: str,
        batch_archive: Mapping[str, Any],
    ) -> dict[str, Any]:
        with _ExclusiveFileLock(self.lock_path):
            return self._apply_authorized_english_preserved_review_repair_locked(
                target_release_id=target_release_id,
                target_activation_id=target_activation_id,
                target_generation=target_generation,
                target_subject_authority_fingerprint=(
                    target_subject_authority_fingerprint
                ),
                target_producer_authority_fingerprint=(
                    target_producer_authority_fingerprint
                ),
                staged_target_canary_state_sha256=(
                    staged_target_canary_state_sha256
                ),
                batch_archive=batch_archive,
            )

    def _apply_authorized_english_preserved_review_repair_locked(
        self,
        *,
        target_release_id: str,
        target_activation_id: str,
        target_generation: str,
        target_subject_authority_fingerprint: str,
        target_producer_authority_fingerprint: str,
        staged_target_canary_state_sha256: str,
        batch_archive: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reclassify the exact failed output and migrate its same queue once."""

        target_release_id = _validate_unit_sha256(target_release_id)
        target_activation_id = _validate_unit_sha256(target_activation_id)
        target_subject_authority_fingerprint = _validate_unit_sha256(
            target_subject_authority_fingerprint
        )
        target_producer_authority_fingerprint = _validate_unit_sha256(
            target_producer_authority_fingerprint
        )
        staged_target_canary_state_sha256 = _validate_unit_sha256(
            staged_target_canary_state_sha256
        )
        if not isinstance(target_generation, str) or not target_generation:
            raise DispatchError(
                "english_preserved_review_repair_target_generation_invalid"
            )
        if target_release_id == ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION[
            "source_release_id"
        ] or target_activation_id == ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION[
            "source_activation_id"
        ]:
            raise DispatchError(
                "english_preserved_review_repair_target_identity_invalid"
            )
        required_archive = {
            "archive",
            "archive_sha256",
            "archive_path",
            "pointer_sha256",
            "writer_postimage_sha256",
            "batch_postimage_sha256",
            "batch_pointer_postimage_sha256",
            "target_generation",
            "target_authority_fingerprint",
        }
        if (
            not isinstance(batch_archive, Mapping)
            or not isinstance(batch_archive.get("archive"), Mapping)
            or not required_archive.issubset(batch_archive)
            or batch_archive.get("target_generation") != target_generation
            or batch_archive.get("target_authority_fingerprint")
            != target_subject_authority_fingerprint
            or any(
                not isinstance(batch_archive.get(key), str)
                or re.fullmatch(r"[0-9a-f]{64}", str(batch_archive[key])) is None
                for key in (
                    "archive_sha256",
                    "pointer_sha256",
                    "writer_postimage_sha256",
                    "batch_postimage_sha256",
                    "batch_pointer_postimage_sha256",
                )
            )
            or not Path(str(batch_archive.get("archive_path") or "")).is_absolute()
        ):
            raise DispatchError(
                "english_preserved_review_repair_batch_archive_invalid"
            )
        existing_pointer = self._read_object(
            self.english_preserved_review_repair_pointer_path
        )
        if existing_pointer is not None:
            self._verify_seal(
                existing_pointer,
                purpose="dispatch-english-preserved-review-repair-pointer-v1",
            )
            if (
                existing_pointer.get("target_release_id") != target_release_id
                or existing_pointer.get("target_activation_id")
                != target_activation_id
                or existing_pointer.get("target_generation")
                != target_generation
                or existing_pointer.get("target_subject_authority_fingerprint")
                != target_subject_authority_fingerprint
                or existing_pointer.get("target_producer_authority_fingerprint")
                != target_producer_authority_fingerprint
            ):
                raise DispatchError(
                    "english_preserved_review_repair_idempotency_conflict"
                )
            return self.reopen_authorized_english_preserved_review_repair(
                repair_receipt_sha256=str(
                    existing_pointer["repair_receipt_sha256"]
                ),
                target_release_id=target_release_id,
                target_activation_id=target_activation_id,
                target_generation=target_generation,
                target_subject_authority_fingerprint=(
                    target_subject_authority_fingerprint
                ),
                target_producer_authority_fingerprint=(
                    target_producer_authority_fingerprint
                ),
                _lock_held=True,
            )
        if self.english_preserved_review_repair_intent_path.exists():
            raise DispatchError(
                "english_preserved_review_repair_prior_intent_present"
            )
        preview = self.preview_authorized_english_preserved_review_repair(
            target_release_id=target_release_id,
            target_activation_id=target_activation_id,
            target_generation=target_generation,
            target_subject_authority_fingerprint=(
                target_subject_authority_fingerprint
            ),
            target_producer_authority_fingerprint=(
                target_producer_authority_fingerprint
            ),
            staged_target_canary_state_sha256=(
                staged_target_canary_state_sha256
            ),
            batch_archive=batch_archive,
            _lock_held=True,
        )
        auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        paths = self._english_preserved_review_source_paths(preview)
        source_queue_path = paths["queue"]
        target_queue_path = (
            self.production_canary_queue_root
            / "english"
            / target_activation_id
            / f"{auth['producer_input_contract_sha256']}.json"
        )
        if target_queue_path.exists():
            raise DispatchError(
                "english_preserved_review_repair_target_queue_exists"
            )
        raw_output = self._read_object(paths["raw_output"])
        task_value = self._read_object(paths["task"])
        queue_before = self._read_object(source_queue_path)
        staged_state = self._read_object(paths["gate"])
        source_index = self._read_object(paths["terminal_index"])
        execution = self._read_object(paths["stage_execution_receipt"])
        transport = self._read_object(paths["mcp_transport"])
        terminal_before = self._read_object(paths["canary_terminal_receipt"])
        if not all(
            isinstance(value, Mapping)
            for value in (
                raw_output,
                task_value,
                queue_before,
                staged_state,
                source_index,
                execution,
                transport,
                terminal_before,
            )
        ):
            raise DispatchError(
                "english_preserved_review_repair_evidence_missing"
            )
        task = FrozenTask.from_mapping(task_value)
        raw_payload = base64.b64decode(
            str(raw_output.get("raw_output_base64") or ""), validate=True
        )
        try:
            analysis = json.loads(raw_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError(
                "english_preserved_review_repair_raw_output_invalid"
            ) from exc
        if not isinstance(analysis, Mapping) or not analysis:
            raise DispatchError(
                "english_preserved_review_repair_raw_output_invalid"
            )
        warning = self._english_preserved_review_warning()
        normalization_warning = {
            "code": "english_analysis_mcp_duplicate_read",
            "json_path": None,
            "message": warning["message"],
        }
        mcp_items = transport.get("mcp_items")
        if not isinstance(mcp_items, list) or len(mcp_items) != 40:
            raise DispatchError(
                "english_preserved_review_repair_mcp_transport_invalid"
            )
        transcript_calls: list[dict[str, Any]] = []
        call_identities: list[tuple[str, str]] = []
        duplicate_result_projections: list[str] = []
        for expected_sequence, transport_row in enumerate(mcp_items, start=1):
            item = (
                transport_row.get("item")
                if isinstance(transport_row, Mapping)
                else None
            )
            if (
                not isinstance(item, Mapping)
                or transport_row.get("event_type") != "item.completed"
                or transport_row.get("sequence") != expected_sequence
                or item.get("type") != "mcp_tool_call"
                or item.get("status") != "completed"
                or item.get("error") is not None
                or not isinstance(item.get("server"), str)
                or not isinstance(item.get("tool"), str)
                or not isinstance(item.get("arguments"), Mapping)
            ):
                raise DispatchError(
                    "english_preserved_review_repair_mcp_transport_invalid"
                )
            result_wrapper = item.get("result")
            content = (
                result_wrapper.get("content")
                if isinstance(result_wrapper, Mapping)
                else None
            )
            text_value = (
                content[0].get("text")
                if isinstance(content, list)
                and len(content) == 1
                and isinstance(content[0], Mapping)
                else None
            )
            try:
                result_value = json.loads(str(text_value))
            except (TypeError, json.JSONDecodeError) as exc:
                raise DispatchError(
                    "english_preserved_review_repair_mcp_transport_invalid"
                ) from exc
            if not isinstance(result_value, Mapping):
                raise DispatchError(
                    "english_preserved_review_repair_mcp_transport_invalid"
                )
            arguments = copy.deepcopy(dict(item["arguments"]))
            call_identities.append(
                (str(item["tool"]), _sha256_bytes(_canonical_bytes(arguments)))
            )
            transcript_calls.append(
                {
                    "sequence": expected_sequence,
                    "server": str(item["server"]),
                    "tool": str(item["tool"]),
                    "arguments": arguments,
                    "arguments_sha256": _sha256_bytes(
                        _canonical_bytes(arguments)
                    ),
                    "result": copy.deepcopy(dict(result_value)),
                    "result_sha256": _sha256_bytes(
                        _canonical_bytes(result_value)
                    ),
                }
            )
            if expected_sequence in {19, 20}:
                structured = (
                    result_wrapper.get("structuredContent")
                    if isinstance(result_wrapper, Mapping)
                    else None
                )
                if structured is None and isinstance(result_wrapper, Mapping):
                    structured = result_wrapper.get("structured_content")
                if not isinstance(structured, Mapping):
                    raise DispatchError(
                        "english_preserved_review_repair_duplicate_binding_invalid"
                    )
                projection = copy.deepcopy(dict(structured))
                projection.pop("captured_at", None)
                duplicate_result_projections.append(
                    _sha256_bytes(_canonical_bytes(projection) + b"\n")
                )
        duplicate_pairs = [
            (index, index + 1)
            for index in range(1, len(call_identities))
            if call_identities[index - 1] == call_identities[index]
        ]
        if (
            duplicate_pairs != [(19, 20)]
            or duplicate_result_projections
            != [auth["duplicate_result_projection_sha256"]] * 2
        ):
            raise DispatchError(
                "english_preserved_review_repair_duplicate_binding_invalid"
            )
        transcript = {
            "schema_version": "model-driven-mcp-stage-transcript-v1",
            "stage_name": "english_analysis",
            "subject": "english",
            "read_session_id": transport["read_session_id"],
            "read_session_manifest_sha256": transport[
                "read_session_manifest_sha256"
            ],
            "generation": transport["generation"],
            "authority_fingerprint": transport["authority_fingerprint"],
            "calls": transcript_calls,
            "coverage": {
                "call_count": 40,
                "all_returned_pages_consumed": True,
                "unresolved_next_cursors": [],
                "duplicate_argument_count": 1,
                "host_semantic_prefetch": False,
            },
            "semantic_stage_count": 1,
            "provider_request_count": 41,
            "mcp_tool_call_count": 40,
            "model_call_count": 1,
            "formal_write_count": 0,
        }
        transcript_sha256, transcript_path = _publish_content_addressed(
            self.runtime_root
            / "private"
            / "reports"
            / "mcp-stage-transcripts",
            transcript,
        )
        execution_binding = execution.get("execution_binding")
        if not isinstance(execution_binding, Mapping):
            raise DispatchError(
                "english_preserved_review_repair_execution_binding_invalid"
            )
        normalization_receipt = self._seal(
            {
                "schema_version": MODEL_STAGE_NORMALIZATION_RECEIPT_SCHEMA,
                "stage_name": "analysis",
                "provider_stage_name": "english_analysis",
                "execution_binding": copy.deepcopy(dict(execution_binding)),
                "normalization_status": "normalized_with_warnings",
                "execution_receipt_sha256": auth[
                    "stage_execution_receipt_sha256"
                ],
                "execution_receipt_ref": (
                    "study-intake-model-stage-execution://sha256/"
                    + auth["stage_execution_receipt_sha256"]
                ),
                "raw_output_object_sha256": auth[
                    "raw_output_object_sha256"
                ],
                "raw_output_object_ref": (
                    "study-intake-model-stage-raw-output://sha256/"
                    + auth["raw_output_object_sha256"]
                ),
                "normalized_payload_sha256": _sha256_bytes(
                    _canonical_bytes(analysis)
                ),
                "warning_count": 1,
                "warnings": [normalization_warning],
                "error_code": None,
                "normalizer_version": "deterministic-v1",
                "formal_write_count": 0,
            },
            purpose="study-intake-model-stage-normalization",
        )
        normalization_sha256, normalization_path = (
            _publish_content_addressed(
                self.model_stage_normalization_receipt_root
                / "english"
                / auth["unit_sha256"]
                / "fence-1"
                / "english_analysis",
                normalization_receipt,
            )
        )
        analysis_runtime = {
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "runtime_identity_status": "requested_unverified",
            "runtime_model": REQUIRED_MODEL,
            "runtime_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "runtime_metadata_provenance": "provider_process_and_raw_output",
            "duration_ms": int(execution.get("duration_ms") or 0),
            "read_session_id": transport.get("read_session_id"),
            "semantic_stage_count": 1,
            "provider_request_count": 41,
            "mcp_tool_call_count": 40,
            "model_call_count": 1,
            "consumed_terminal_duplicate_read_count": 0,
            "raw_output_object_sha256": auth["raw_output_object_sha256"],
            "raw_output_object_ref": (
                "study-intake-model-stage-raw-output://sha256/"
                + auth["raw_output_object_sha256"]
            ),
            "stage_execution_receipt_sha256": auth[
                "stage_execution_receipt_sha256"
            ],
            "stage_execution_receipt_ref": (
                "study-intake-model-stage-execution://sha256/"
                + auth["stage_execution_receipt_sha256"]
            ),
            "stage_normalization_receipt_sha256": normalization_sha256,
            "stage_normalization_receipt_ref": (
                "study-intake-model-stage-normalization://sha256/"
                + normalization_sha256
            ),
            "normalization_status": "normalized_with_warnings",
            "normalization_warning_count": 1,
            "normalization_warnings": [normalization_warning],
            "review_mcp_transcript_sha256": transcript_sha256,
            "review_mcp_transcript_ref": (
                "study-intake-mcp-stage-transcript://sha256/"
                + transcript_sha256
            ),
            "provider_process_identity_sha256": execution[
                "provider_process_identity_sha256"
            ],
            "mcp_transport_sha256": auth["mcp_transport_sha256"],
        }
        review_result = {
            "status": "needs_sol_review",
            "error_code": "english_analysis_mcp_duplicate_read",
        }
        artifact_base = {
            "unit_sha256": auth["unit_sha256"],
            "subject": "english",
            "capture_id": auth["capture_id"],
            "release_id": target_release_id,
            "analysis": copy.deepcopy(dict(analysis)),
            "critical_review": None,
            "stage_runtime": {
                "analysis": analysis_runtime,
                "critical_review": None,
            },
            "terminal_status": "needs_sol_review",
            "report_available": True,
            "report_disposition": "needs_sol_review",
            "sol_review_status": "pending",
            "formal_write_eligible": False,
            "review_result": review_result,
            "warnings": [warning],
            "formal_write_count": 0,
        }
        package = {
            "schema_version": REVIEW_PACKAGE_SCHEMA,
            **artifact_base,
            "lease_fence": 1,
            "content_processing_id": task.frozen_payload.get(
                "content_processing_id"
            ),
            "loaded_core_sha256": (
                task.frozen_payload.get("dispatch_contract") or {}
            ).get("loaded_core_sha256"),
            "task": task.as_dict(),
            "model_contract": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
            "pipeline": ["analysis"],
        }
        package_sha256, package_path = _publish_content_addressed(
            self.runtime_root / "dispatch" / "packages", package
        )
        package_ref = "study-intake-dispatch-package://sha256/" + package_sha256
        report = {
            "schema_version": "study-intake-review-candidate-terminal-v1",
            **artifact_base,
            "package_ref": package_ref,
            "package_sha256": package_sha256,
        }
        report_sha256, report_path = _publish_content_addressed(
            self.runtime_root / "dispatch" / "reports" / "json", report
        )
        report_ref = "study-intake-report://sha256/" + report_sha256
        markdown = (
            "# English Luna review report\n\n"
            f"- capture_id: {auth['capture_id']}\n"
            f"- unit_sha256: {auth['unit_sha256']}\n"
            f"- report_json_ref: {report_ref}\n"
            f"- package_ref: {package_ref}\n"
            "- terminal_status: needs_sol_review\n"
            "- report_available: true\n"
            "- sol_review_status: pending\n"
            "- formal_write_eligible: false\n"
            "- formal_write_count: 0\n"
        ).encode("utf-8")
        markdown_sha256, markdown_path = _publish_content_addressed_bytes(
            self.runtime_root / "dispatch" / "reports" / "markdown",
            markdown,
            suffix=".md",
        )
        markdown_ref = (
            "study-intake-report-markdown://sha256/" + markdown_sha256
        )
        timestamp = _utc_now()
        source_selected = dict(terminal_before["selected"])
        terminal_core = {
            "schema_version": PRODUCTION_CANARY_REVIEW_TERMINAL_SCHEMA,
            "counter_scope": "current_control_plane_operation",
            "terminal_kind": "normal",
            "late_result_fenced": True,
            "activation_id": target_activation_id,
            "subject": "english",
            "release_id": target_release_id,
            "producer_authority_fingerprint": staged_state[
                "producer_authority_fingerprint"
            ],
            "producer_high_watermark_sha256": staged_state[
                "producer_high_watermark_sha256"
            ],
            "activation_receipt_sha256": staged_state[
                "activation_receipt_sha256"
            ],
            "activation_gate_authority_sha256": staged_state[
                "activation_gate_authority_sha256"
            ],
            "canary_gate_sha256": terminal_before["canary_gate_sha256"],
            "canary_gate_authority_sha256": terminal_before[
                "canary_gate_authority_sha256"
            ],
            "selected": source_selected,
            "outcome": "needs_rework",
            "error_code": "english_analysis_mcp_duplicate_read",
            "completion_path": str(paths["completion"]),
            "completion_sha256": auth["completion_preimage_sha256"],
            "completion_receipt_path": str(paths["processing_receipt"]),
            "completion_receipt_sha256": auth["processing_receipt_sha256"],
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "package_ref": package_ref,
            "report_json_ref": report_ref,
            "report_json_sha256": report_sha256,
            "report_markdown_ref": markdown_ref,
            "report_markdown_sha256": markdown_sha256,
            "report_reopen_status": "json_markdown_package_verified",
            "report_available": True,
            "report_disposition": "needs_sol_review",
            "sol_review_status": "pending",
            "formal_write_eligible": False,
            "observed_model_call_count": 1,
            "observed_provider_request_count": 41,
            "observed_mcp_tool_call_count": 40,
            "read_session_id": transport["read_session_id"],
            "evidence_generation": transport["generation"],
            "evidence_authority_fingerprint": transport[
                "authority_fingerprint"
            ],
            "task_declared_evidence_refs_sha256": None,
            "mcp_stage_grounding": None,
            "process_execution": terminal_before["process_execution"],
            "finished_at": timestamp,
            "state_after": "armed",
            **self._canary_control_fields(
                int(staged_state["continuous_concurrency_limit"])
            ),
        }
        review_terminal_sha256, review_terminal_path, _ = (
            self._publish_canary_receipt_locked(
                "english",
                target_activation_id,
                terminal_core,
                purpose="dispatch-production-canary-terminal",
            )
        )
        units = copy.deepcopy(dict(source_index["units"]))
        prior = dict(units[auth["unit_sha256"]])
        history = copy.deepcopy(list(prior["history"]))
        history.append(
            {
                "terminal_receipt_sha256": review_terminal_sha256,
                "terminal_receipt_path": str(review_terminal_path),
                "outcome": "needs_rework",
                "error_code": "english_analysis_mcp_duplicate_read",
                "terminal_kind": "normal",
                "finished_at": timestamp,
                "prior_terminal_receipt_sha256": auth[
                    "canary_terminal_receipt_sha256"
                ],
            }
        )
        units[auth["unit_sha256"]] = {
            **prior,
            "finished_at": timestamp,
            "outcome": "needs_rework",
            "error_code": "english_analysis_mcp_duplicate_read",
            "terminal_kind": "normal",
            "terminal_receipt_sha256": review_terminal_sha256,
            "terminal_receipt_path": str(review_terminal_path),
            "package_ref": package_ref,
            "package_sha256": package_sha256,
            "report_json_ref": report_ref,
            "report_json_sha256": report_sha256,
            "report_markdown_ref": markdown_ref,
            "report_markdown_sha256": markdown_sha256,
            "report_reopen_status": "json_markdown_package_verified",
            "history": history,
        }
        index_value, index_sha256, index_path = (
            self._publish_canary_terminal_index_locked(
                subject="english",
                release_id=target_release_id,
                activation_id=target_activation_id,
                units=units,
                updated_at=timestamp,
            )
        )
        queue_core = copy.deepcopy(dict(queue_before))
        queue_core.pop("authority", None)
        queue_core.update(
            {
                "schema_version": PRODUCTION_CANARY_REVIEW_QUEUE_SCHEMA,
                "activation_id": target_activation_id,
                "release_id": target_release_id,
                "producer_authority_fingerprint": staged_state[
                    "producer_authority_fingerprint"
                ],
                "producer_high_watermark_sha256": staged_state[
                    "producer_high_watermark_sha256"
                ],
                "queue_status": "needs_sol_review",
                "finished_at": timestamp,
                "terminal_receipt_sha256": review_terminal_sha256,
                "terminal_receipt_path": str(review_terminal_path),
                "terminal_outcome": "needs_rework",
                "terminal_error_code": "english_analysis_mcp_duplicate_read",
            }
        )
        queue_after = self._seal(
            queue_core, purpose="dispatch-production-canary-queue"
        )
        queue_after_bytes = _canonical_bytes(queue_after) + b"\n"
        state_core = copy.deepcopy(dict(staged_state))
        state_core.pop("authority", None)
        state_core.update(
            {
                "state": "armed",
                "status": "production_canary_active",
                "release_id": target_release_id,
                "activation_id": target_activation_id,
                "activated_at": staged_state["activated_at"],
                "luna_consumer_enabled": True,
                "queue_depth": 0,
                "historical_eligible_count": 0,
                "excluded_by_high_watermark_count": 0,
                "canary_queue_count": 0,
                "queue_classification": {
                    "pre_activation_frozen": 0,
                    "pending": 0,
                    "claimed": 0,
                    "succeeded": 0,
                    "failed": 0,
                },
                "terminal_index_sha256": index_sha256,
                "terminal_index_path": str(index_path),
                "terminal_task_count": index_value["terminal_task_count"],
                "terminal_by_outcome": index_value["terminal_by_outcome"],
                "last_terminal_receipt_sha256": review_terminal_sha256,
                "last_terminal_receipt_path": str(review_terminal_path),
                "last_analysis_status": "completed",
                "last_critical_review_status": "not_started",
                "last_report_status": "reopen_verified",
                "last_package_sha256": package_sha256,
                "last_read_session_id": transport["read_session_id"],
                "last_evidence_generation": transport["generation"],
                "last_evidence_authority_fingerprint": transport[
                    "authority_fingerprint"
                ],
                "last_failure_at": None,
                "blocking_reason": None,
                "next_action": "await_post_activation_capture",
                "observed_model_call_count": 1,
                "observed_provider_request_count": 41,
                "observed_mcp_tool_call_count": 40,
                "selected": None,
                "active_selections": {},
                "active_task_count": 0,
                "backpressure_reason": None,
            }
        )
        state_core.pop("authority", None)
        state_core["updated_at"] = timestamp
        gate_after = self._seal(
            state_core, purpose="dispatch-production-canary-state"
        )
        gate_after_bytes = _canonical_bytes(gate_after) + b"\n"
        gate_postimage_sha256 = _sha256_bytes(gate_after_bytes)
        queue_postimage_sha256 = _sha256_bytes(queue_after_bytes)
        state_path = self._production_canary_state_path("english")
        staged_index_path = Path(
            str(staged_state.get("terminal_index_path") or "")
        )
        source_queue_bytes = source_queue_path.read_bytes()
        staged_gate_bytes = state_path.read_bytes()
        staged_index_bytes = staged_index_path.read_bytes()
        if (
            _sha256_bytes(source_queue_bytes)
            != auth["queue_preimage_sha256"]
            or _sha256_bytes(staged_gate_bytes)
            != staged_target_canary_state_sha256
            or _sha256_bytes(staged_index_bytes)
            != staged_state.get("terminal_index_sha256")
        ):
            raise DispatchError(
                "english_preserved_review_repair_double_read_drift"
            )
        archive_value = copy.deepcopy(dict(batch_archive["archive"]))
        capsule = self._seal(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-preimage-v1"
                ),
                "authorization_descriptor_sha256": preview[
                    "authorization_descriptor_sha256"
                ],
                "source_release_id": auth["source_release_id"],
                "source_activation_id": auth["source_activation_id"],
                "target_release_id": target_release_id,
                "target_activation_id": target_activation_id,
                "target_generation": target_generation,
                "target_subject_authority_fingerprint": (
                    target_subject_authority_fingerprint
                ),
                "target_producer_authority_fingerprint": (
                    target_producer_authority_fingerprint
                ),
                "source_gate_preimage_sha256": auth[
                    "gate_preimage_sha256"
                ],
                "staged_gate_path": str(state_path),
                "staged_gate_preimage_sha256": (
                    staged_target_canary_state_sha256
                ),
                "staged_gate_bytes_base64": base64.b64encode(
                    staged_gate_bytes
                ).decode("ascii"),
                "source_terminal_index_path": str(paths["terminal_index"]),
                "source_terminal_index_sha256": auth[
                    "terminal_index_preimage_sha256"
                ],
                "staged_terminal_index_path": str(staged_index_path),
                "staged_terminal_index_sha256": staged_state[
                    "terminal_index_sha256"
                ],
                "staged_terminal_index_bytes_base64": base64.b64encode(
                    staged_index_bytes
                ).decode("ascii"),
                "source_queue_path": str(source_queue_path),
                "target_queue_path": str(target_queue_path),
                "source_queue_sha256": auth["queue_preimage_sha256"],
                "source_queue_bytes_base64": base64.b64encode(
                    source_queue_bytes
                ).decode("ascii"),
                "subject_batch_preimage": archive_value["batch"],
                "subject_batch_preimage_sha256": auth[
                    "subject_batch_preimage_sha256"
                ],
                "subject_batch_pointer_preimage": archive_value[
                    "batch_pointer"
                ],
                "subject_batch_pointer_preimage_sha256": auth[
                    "subject_batch_pointer_preimage_sha256"
                ],
                "subject_writer_preimage": archive_value[
                    "writer_preimage"
                ],
                "subject_writer_preimage_sha256": auth[
                    "subject_writer_preimage_sha256"
                ],
                "batch_archive_sha256": batch_archive["archive_sha256"],
                "batch_retirement_pointer_sha256": batch_archive[
                    "pointer_sha256"
                ],
                "planned_queue_postimage_sha256": queue_postimage_sha256,
                "planned_gate_postimage_sha256": gate_postimage_sha256,
                "planned_terminal_index_postimage_sha256": index_sha256,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "sol_call_count": 0,
                "formal_write_count": 0,
                "created_at": timestamp,
            },
            purpose="dispatch-english-preserved-review-repair-preimage-v1",
        )
        capsule_sha256, capsule_path = _publish_content_addressed(
            self.english_preserved_review_repair_preimage_root, capsule
        )
        intent = self._seal(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-intent-v1"
                ),
                "authorization_descriptor_sha256": preview[
                    "authorization_descriptor_sha256"
                ],
                "target_release_id": target_release_id,
                "target_activation_id": target_activation_id,
                "target_generation": target_generation,
                "target_subject_authority_fingerprint": (
                    target_subject_authority_fingerprint
                ),
                "target_producer_authority_fingerprint": (
                    target_producer_authority_fingerprint
                ),
                "preimage_capsule_sha256": capsule_sha256,
                "preimage_capsule_path": str(capsule_path),
                "status": "prepared",
                "repair_receipt_sha256": None,
                "repair_receipt_path": None,
                "formal_write_count": 0,
                "updated_at": timestamp,
            },
            purpose="dispatch-english-preserved-review-repair-intent-v1",
        )
        _atomic_replace_json(
            self.english_preserved_review_repair_intent_path, intent
        )
        try:
            self._english_preserved_review_failpoint("after_intent")
            target_queue_path.parent.mkdir(
                parents=True, exist_ok=True, mode=0o700
            )
            os.replace(source_queue_path, target_queue_path)
            self._english_preserved_review_failpoint("after_queue_move")
            _atomic_replace_bytes(target_queue_path, queue_after_bytes)
            self._english_preserved_review_failpoint("after_queue_rewrite")
            _atomic_replace_bytes(state_path, gate_after_bytes)
            self._english_preserved_review_failpoint("after_gate_write")
        except BaseException as exc:
            try:
                restored = self._restore_english_preserved_review_preimages(
                    capsule, remove_repair_pointer=True
                )
                rollback_intent = self._seal(
                    {
                        **{
                            key: copy.deepcopy(value)
                            for key, value in intent.items()
                            if key != "authority"
                        },
                        "status": "rolled_back_same_process",
                        "rollback_verification": restored,
                        "updated_at": _utc_now(),
                    },
                    purpose=(
                        "dispatch-english-preserved-review-repair-intent-v1"
                    ),
                )
                _atomic_replace_json(
                    self.english_preserved_review_repair_intent_path,
                    rollback_intent,
                )
            except BaseException as rollback_exc:
                raise DispatchError(
                    "english_preserved_review_repair_fail_fenced"
                ) from rollback_exc
            raise DispatchError(
                "english_preserved_review_repair_apply_rolled_back"
            ) from exc
        evidence_bindings = {
            key: auth[key]
            for key in (
                "original_preclaim_failure_receipt_sha256",
                "rollover_receipt_sha256",
                "recovery_supersede_receipt_sha256",
                "task_object_sha256",
                "raw_output_object_sha256",
                "mcp_transport_sha256",
                "duplicate_result_projection_sha256",
                "stage_execution_receipt_sha256",
                "processing_receipt_sha256",
                "mcp_failure_receipt_sha256",
                "canary_terminal_receipt_sha256",
                "subject_terminal_receipt_sha256",
            )
        }
        mutable_preimages = {
            key: auth[key]
            for key in (
                "queue_preimage_sha256",
                "gate_preimage_sha256",
                "terminal_index_preimage_sha256",
                "subject_batch_preimage_sha256",
                "subject_batch_pointer_preimage_sha256",
                "subject_writer_preimage_sha256",
            )
        }
        mutable_preimages["staged_gate_preimage_sha256"] = (
            staged_target_canary_state_sha256
        )
        mutable_preimages["staged_terminal_index_preimage_sha256"] = (
            str(staged_state["terminal_index_sha256"])
        )
        review_artifacts = {
            "package_sha256": package_sha256,
            "package_path": str(package_path),
            "report_json_sha256": report_sha256,
            "report_json_path": str(report_path),
            "report_markdown_sha256": markdown_sha256,
            "report_markdown_path": str(markdown_path),
            "mcp_transcript_sha256": transcript_sha256,
            "mcp_transcript_path": str(transcript_path),
            "normalization_receipt_sha256": normalization_sha256,
            "normalization_receipt_path": str(normalization_path),
        }
        receipt_core = {
            "schema_version": ENGLISH_PRESERVED_REVIEW_REPAIR_RECEIPT_SCHEMA,
            "repair_id": _sha256_bytes(
                _canonical_bytes(
                    {
                        "authorization_descriptor_sha256": preview[
                            "authorization_descriptor_sha256"
                        ],
                        "target_release_id": target_release_id,
                        "target_activation_id": target_activation_id,
                        "target_generation": target_generation,
                        "target_subject_authority_fingerprint": (
                            target_subject_authority_fingerprint
                        ),
                        "target_producer_authority_fingerprint": (
                            target_producer_authority_fingerprint
                        ),
                    }
                )
            ),
            "authorization_descriptor_sha256": preview[
                "authorization_descriptor_sha256"
            ],
            "subject": "english",
            "mode": auth["mode"],
            "source_release_id": auth["source_release_id"],
            "source_activation_id": auth["source_activation_id"],
            "target_release_id": target_release_id,
            "target_activation_id": target_activation_id,
            "target_generation": target_generation,
            "target_subject_authority_fingerprint": (
                target_subject_authority_fingerprint
            ),
            "target_producer_authority_fingerprint": (
                target_producer_authority_fingerprint
            ),
            "capture_id": auth["capture_id"],
            "unit_sha256": auth["unit_sha256"],
            "frozen_payload_sha256": auth["frozen_payload_sha256"],
            "producer_input_contract_sha256": auth[
                "producer_input_contract_sha256"
            ],
            "evidence_bindings": evidence_bindings,
            "mutable_preimages": mutable_preimages,
            "batch_archive_sha256": batch_archive["archive_sha256"],
            "batch_archive_path": batch_archive["archive_path"],
            "batch_retirement_pointer_sha256": batch_archive[
                "pointer_sha256"
            ],
            "preimage_capsule_sha256": capsule_sha256,
            "preimage_capsule_path": str(capsule_path),
            "batch_postimage_sha256": batch_archive[
                "batch_postimage_sha256"
            ],
            "batch_pointer_postimage_sha256": batch_archive[
                "batch_pointer_postimage_sha256"
            ],
            "writer_postimage_sha256": batch_archive[
                "writer_postimage_sha256"
            ],
            "review_artifacts": review_artifacts,
            "review_terminal_sha256": review_terminal_sha256,
            "review_terminal_path": str(review_terminal_path),
            "source_queue_path": str(source_queue_path),
            "source_queue_absent": not source_queue_path.exists(),
            "target_queue_path": str(target_queue_path),
            "queue_postimage_sha256": queue_postimage_sha256,
            "queue_identity_preserved": True,
            "source_gate_preimage_sha256": auth["gate_preimage_sha256"],
            "staged_gate_preimage_sha256": (
                staged_target_canary_state_sha256
            ),
            "gate_postimage_sha256": gate_postimage_sha256,
            "terminal_index_postimage_sha256": index_sha256,
            "terminal_index_postimage_path": str(index_path),
            "report_available": True,
            "report_disposition": "needs_sol_review",
            "sol_review_status": "pending",
            "formal_write_eligible": False,
            "historical_terminal_receipt_sha256": auth[
                "canary_terminal_receipt_sha256"
            ],
            "new_task_count": 0,
            "new_queue_count": 0,
            "capture_replay_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
            "created_at": timestamp,
        }
        receipt = self._seal(
            receipt_core,
            purpose="dispatch-english-preserved-review-repair-v1",
        )
        receipt_sha256, receipt_path = _publish_content_addressed(
            self.english_preserved_review_repair_receipt_root, receipt
        )
        pointer = self._seal(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-pointer-v1"
                ),
                "repair_receipt_sha256": receipt_sha256,
                "repair_receipt_path": str(receipt_path),
                "target_release_id": target_release_id,
                "target_activation_id": target_activation_id,
                "target_generation": target_generation,
                "target_subject_authority_fingerprint": (
                    target_subject_authority_fingerprint
                ),
                "target_producer_authority_fingerprint": (
                    target_producer_authority_fingerprint
                ),
                "formal_write_count": 0,
            },
            purpose="dispatch-english-preserved-review-repair-pointer-v1",
        )
        try:
            _atomic_replace_json(
                self.english_preserved_review_repair_pointer_path, pointer
            )
            self._english_preserved_review_failpoint("after_repair_pointer")
            completed_intent = self._seal(
                {
                    **{
                        key: copy.deepcopy(value)
                        for key, value in intent.items()
                        if key != "authority"
                    },
                    "status": "applied",
                    "repair_receipt_sha256": receipt_sha256,
                    "repair_receipt_path": str(receipt_path),
                    "updated_at": _utc_now(),
                },
                purpose="dispatch-english-preserved-review-repair-intent-v1",
            )
            _atomic_replace_json(
                self.english_preserved_review_repair_intent_path,
                completed_intent,
            )
        except BaseException as exc:
            try:
                restored = self._restore_english_preserved_review_preimages(
                    capsule, remove_repair_pointer=True
                )
                rollback_intent = self._seal(
                    {
                        **{
                            key: copy.deepcopy(value)
                            for key, value in intent.items()
                            if key != "authority"
                        },
                        "status": "rolled_back_same_process",
                        "repair_receipt_sha256": receipt_sha256,
                        "repair_receipt_path": str(receipt_path),
                        "rollback_verification": restored,
                        "updated_at": _utc_now(),
                    },
                    purpose=(
                        "dispatch-english-preserved-review-repair-intent-v1"
                    ),
                )
                _atomic_replace_json(
                    self.english_preserved_review_repair_intent_path,
                    rollback_intent,
                )
            except BaseException as rollback_exc:
                raise DispatchError(
                    "english_preserved_review_repair_fail_fenced"
                ) from rollback_exc
            raise DispatchError(
                "english_preserved_review_repair_apply_rolled_back"
            ) from exc
        return {
            "schema_version": (
                "study-intake-english-preserved-review-repair-result-v1"
            ),
            "status": "applied",
            "repair_receipt": receipt,
            "repair_receipt_sha256": receipt_sha256,
            "repair_receipt_path": str(receipt_path),
            "report_sha256": report_sha256,
            "source_queue_absent": not source_queue_path.exists(),
            "target_queue_path": str(target_queue_path),
            "target_queue_sha256": queue_postimage_sha256,
            "queue_identity_preserved": True,
            "gate": gate_after,
            "new_task_count": 0,
            "new_queue_count": 0,
            "capture_replay_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
        }

    def rollback_authorized_english_preserved_review_repair(
        self,
        *,
        repair_receipt_sha256: str,
        batch_rollback: Mapping[str, Any],
        prior_reopen: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Restore target staged gate and the exact source queue preimage."""

        reopened = (
            dict(prior_reopen)
            if isinstance(prior_reopen, Mapping)
            else self.reopen_authorized_english_preserved_review_repair(
                repair_receipt_sha256=repair_receipt_sha256
            )
        )
        if (
            reopened.get("status") != "reopened"
            or reopened.get("repair_receipt_sha256")
            != _validate_unit_sha256(repair_receipt_sha256)
            or not isinstance(reopened.get("repair_receipt"), Mapping)
        ):
            raise DispatchError(
                "english_preserved_review_repair_prior_reopen_invalid"
            )
        receipt = dict(reopened["repair_receipt"])
        receipt_path = Path(str(reopened.get("repair_receipt_path") or ""))
        receipt_payload, receipt_reopened = self._exact_preserved_repair_file(
            receipt_path,
            repair_receipt_sha256,
            "repair_receipt",
        )
        del receipt_payload
        self._verify_seal(
            receipt_reopened,
            purpose="dispatch-english-preserved-review-repair-v1",
        )
        if receipt_reopened != receipt:
            raise DispatchError(
                "english_preserved_review_repair_prior_reopen_invalid"
            )
        auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        if (
            not isinstance(batch_rollback, Mapping)
            or batch_rollback.get("batch_restored_sha256")
            != auth["subject_batch_preimage_sha256"]
            or batch_rollback.get("batch_pointer_restored_sha256")
            != auth["subject_batch_pointer_preimage_sha256"]
            or batch_rollback.get("writer_restored_sha256")
            != auth["subject_writer_preimage_sha256"]
            or batch_rollback.get("formal_write_count") != 0
        ):
            raise DispatchError(
                "english_preserved_review_repair_batch_rollback_invalid"
            )
        capsule = self._read_english_preserved_review_preimage_capsule(
            digest=str(receipt["preimage_capsule_sha256"]),
            path_value=str(receipt["preimage_capsule_path"]),
        )
        with _ExclusiveFileLock(self.lock_path):
            source_queue_path = Path(str(receipt["source_queue_path"]))
            target_queue_path = Path(str(receipt["target_queue_path"]))
            gate_path = self._production_canary_state_path("english")
            if (
                source_queue_path.exists()
                or not target_queue_path.is_file()
                or _sha256_bytes(target_queue_path.read_bytes())
                != receipt["queue_postimage_sha256"]
                or _sha256_bytes(gate_path.read_bytes())
                != receipt["gate_postimage_sha256"]
            ):
                raise DispatchError(
                    "english_preserved_review_repair_rollback_postimage_drift"
                )
            restored = self._restore_english_preserved_review_preimages(
                capsule, remove_repair_pointer=True
            )
            rollback_core = {
                "schema_version": (
                    ENGLISH_PRESERVED_REVIEW_REPAIR_ROLLBACK_RECEIPT_SCHEMA
                ),
                "repair_receipt_sha256": repair_receipt_sha256,
                "subject": "english",
                "status": "rolled_back",
                "source_queue_restored_sha256": restored[
                    "source_queue_restored_sha256"
                ],
                "target_queue_absent": True,
                "staged_gate_restored_sha256": restored[
                    "staged_gate_restored_sha256"
                ],
                "source_terminal_index_retained_sha256": auth[
                    "terminal_index_preimage_sha256"
                ],
                "staged_terminal_index_restored_sha256": receipt[
                    "mutable_preimages"
                ]["staged_terminal_index_preimage_sha256"],
                "batch_restored_sha256": batch_rollback[
                    "batch_restored_sha256"
                ],
                "batch_pointer_restored_sha256": batch_rollback[
                    "batch_pointer_restored_sha256"
                ],
                "writer_restored_sha256": batch_rollback[
                    "writer_restored_sha256"
                ],
                "historical_evidence_retained": True,
                "model_call_count": 0,
                "provider_request_count": 0,
                "mcp_tool_call_count": 0,
                "sol_call_count": 0,
                "formal_write_count": 0,
                "rolled_back_at": _utc_now(),
            }
            rollback = self._seal(
                rollback_core,
                purpose=(
                    "dispatch-english-preserved-review-repair-rollback-v1"
                ),
            )
            rollback_sha256, rollback_path = _publish_content_addressed(
                self.english_preserved_review_repair_rollback_receipt_root,
                rollback,
            )
            intent = self._read_object(
                self.english_preserved_review_repair_intent_path
            )
            if intent is None:
                raise DispatchError(
                    "english_preserved_review_repair_intent_missing"
                )
            self._verify_seal(
                intent,
                purpose=(
                    "dispatch-english-preserved-review-repair-intent-v1"
                ),
            )
            rolled_back_intent = self._seal(
                {
                    **{
                        key: copy.deepcopy(value)
                        for key, value in intent.items()
                        if key != "authority"
                    },
                    "status": "rolled_back",
                    "rollback_receipt_sha256": rollback_sha256,
                    "rollback_receipt_path": str(rollback_path),
                    "updated_at": _utc_now(),
                },
                purpose=(
                    "dispatch-english-preserved-review-repair-intent-v1"
                ),
            )
            _atomic_replace_json(
                self.english_preserved_review_repair_intent_path,
                rolled_back_intent,
            )
        return {
            "schema_version": (
                "study-intake-english-preserved-review-repair-rollback-result-v1"
            ),
            "status": "rolled_back",
            "rollback_receipt": rollback,
            "rollback_receipt_sha256": rollback_sha256,
            "rollback_receipt_path": str(rollback_path),
            **restored,
            "batch_restored_sha256": batch_rollback[
                "batch_restored_sha256"
            ],
            "batch_pointer_restored_sha256": batch_rollback[
                "batch_pointer_restored_sha256"
            ],
            "writer_restored_sha256": batch_rollback[
                "writer_restored_sha256"
            ],
            "formal_write_count": 0,
        }

    def reopen_authorized_english_preserved_review_repair_rollback(
        self,
        *,
        repair_receipt_sha256: str,
        rollback_receipt_sha256: str,
    ) -> dict[str, Any]:
        """Read-only proof that all exact mutable preimages are restored."""

        repair_digest = _validate_unit_sha256(repair_receipt_sha256)
        rollback_digest = _validate_unit_sha256(rollback_receipt_sha256)
        path = (
            self.english_preserved_review_repair_rollback_receipt_root
            / "sha256"
            / rollback_digest[:2]
            / f"{rollback_digest}.json"
        )
        with _ExistingSharedFileLock(self.lock_path):
            _payload, rollback = self._exact_preserved_repair_file(
                path, rollback_digest, "rollback_receipt"
            )
            self._verify_seal(
                rollback,
                purpose=(
                    "dispatch-english-preserved-review-repair-rollback-v1"
                ),
            )
            auth = ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
            intent = self._read_object(
                self.english_preserved_review_repair_intent_path
            )
            if not isinstance(intent, Mapping):
                raise DispatchError(
                    "english_preserved_review_repair_rollback_intent_invalid"
                )
            self._verify_seal(
                intent,
                purpose=(
                    "dispatch-english-preserved-review-repair-intent-v1"
                ),
            )
            if (
                intent.get("status") != "rolled_back"
                or intent.get("repair_receipt_sha256") != repair_digest
                or intent.get("rollback_receipt_sha256") != rollback_digest
            ):
                raise DispatchError(
                    "english_preserved_review_repair_rollback_intent_invalid"
                )
            source_queue_path = (
                self.production_canary_queue_root
                / "english"
                / auth["source_activation_id"]
                / f"{auth['producer_input_contract_sha256']}.json"
            )
            gate_path = self._production_canary_state_path("english")
            capsule = self._read_english_preserved_review_preimage_capsule(
                digest=str(intent.get("preimage_capsule_sha256") or ""),
                path_value=str(intent.get("preimage_capsule_path") or ""),
            )
            target_queue_path = Path(str(capsule["target_queue_path"]))
            if (
                rollback.get("repair_receipt_sha256") != repair_digest
                or rollback.get("status") != "rolled_back"
                or self.english_preserved_review_repair_pointer_path.exists()
                or target_queue_path.exists()
                or _sha256_bytes(source_queue_path.read_bytes())
                != auth["queue_preimage_sha256"]
                or _sha256_bytes(gate_path.read_bytes())
                != rollback["staged_gate_restored_sha256"]
                or rollback.get("batch_restored_sha256")
                != auth["subject_batch_preimage_sha256"]
                or rollback.get("batch_pointer_restored_sha256")
                != auth["subject_batch_pointer_preimage_sha256"]
                or rollback.get("writer_restored_sha256")
                != auth["subject_writer_preimage_sha256"]
                or rollback.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "english_preserved_review_repair_rollback_postimage_invalid"
                )
            return {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-rollback-"
                    "reopen-v1"
                ),
                "status": "reopened",
                "repair_receipt_sha256": repair_digest,
                "rollback_receipt": rollback,
                "rollback_receipt_sha256": rollback_digest,
                "rollback_receipt_path": str(path),
                "source_queue_restored_sha256": auth[
                    "queue_preimage_sha256"
                ],
                "target_queue_absent": True,
                "batch_restored_sha256": auth[
                    "subject_batch_preimage_sha256"
                ],
                "batch_pointer_restored_sha256": auth[
                    "subject_batch_pointer_preimage_sha256"
                ],
                "writer_restored_sha256": auth[
                    "subject_writer_preimage_sha256"
                ],
                "formal_write_count": 0,
            }



def _publish_named_immutable(path: Path, value: Mapping[str, Any]) -> None:
    payload = _canonical_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
        except FileExistsError as exc:
            raise DispatchError("completion_already_published") from exc
        os.chmod(path, 0o400)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
def _stage_runtime(result: StageResult) -> dict[str, Any]:
    runtime = {
        "requested_model": REQUIRED_MODEL,
        "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
        "runtime_identity_status": result.runtime_identity_status,
        "runtime_model": result.runtime_model,
        "runtime_reasoning_effort": result.runtime_reasoning_effort,
        "runtime_metadata_provenance": result.runtime_metadata_provenance,
        "duration_ms": result.duration_ms,
        "read_session_id": result.read_session_id,
        "semantic_stage_count": result.semantic_stage_count,
        "provider_request_count": result.provider_request_count,
        "mcp_tool_call_count": result.mcp_tool_call_count,
        "model_call_count": result.model_call_count,
        "consumed_terminal_duplicate_read_count": (
            result.consumed_terminal_duplicate_read_count
        ),
        "raw_output_object_sha256": result.raw_output_object_sha256,
        "raw_output_object_ref": result.raw_output_object_ref,
        "stage_execution_receipt_sha256": (
            result.stage_execution_receipt_sha256
        ),
        "stage_execution_receipt_ref": result.stage_execution_receipt_ref,
        "stage_normalization_receipt_sha256": (
            result.stage_normalization_receipt_sha256
        ),
        "stage_normalization_receipt_ref": (
            result.stage_normalization_receipt_ref
        ),
        "normalization_status": result.normalization_status,
        "normalization_warning_count": result.normalization_warning_count,
        "normalization_warnings": [
            copy.deepcopy(dict(row))
            for row in result.normalization_warnings
        ],
    }
    if result.review_mcp_transcript_sha256 is not None:
        runtime.update(
            {
                "review_mcp_transcript_sha256": (
                    result.review_mcp_transcript_sha256
                ),
                "review_mcp_transcript_ref": result.review_mcp_transcript_ref,
            }
        )
    if result.mcp_grounding_manifest_sha256 is not None:
        runtime.update(
            {
                "read_session_manifest_sha256": (
                    result.read_session_manifest_sha256
                ),
                "authority_snapshot_manifest_sha256": (
                    result.authority_snapshot_manifest_sha256
                ),
                "capture_freeze_receipt_sha256": (
                    result.capture_freeze_receipt_sha256
                ),
                "mcp_read_session_receipt_sha256": (
                    result.mcp_read_session_receipt_sha256
                ),
                "evidence_generation": result.evidence_generation,
                "evidence_authority_fingerprint": (
                    result.evidence_authority_fingerprint
                ),
                "evidence_subject": result.evidence_subject,
                "evidence_release_id": result.evidence_release_id,
                "mcp_grounding_manifest_sha256": (
                    result.mcp_grounding_manifest_sha256
                ),
                "mcp_transcript_sha256": result.mcp_transcript_sha256,
                "mcp_consumed_evidence_refs": list(
                    result.mcp_consumed_evidence_refs
                ),
                "mcp_cited_evidence_refs": list(
                    result.mcp_cited_evidence_refs
                ),
                "mcp_stage_grounded_evidence_refs": list(
                    result.mcp_stage_grounded_evidence_refs
                ),
            }
        )
    return runtime


@dataclass
class TaskExecutionContext:
    task: FrozenTask
    lease: Lease
    root: Path
    soft_runtime_warning_seconds: float
    stall_timeout_seconds: float = 1800.0
    stall_probe_interval_seconds: float = 60.0
    stall_probe_required_consecutive_failures: int = 2
    resume_from_analysis_checkpoint: bool = False
    cancel_event: threading.Event = field(default_factory=threading.Event)
    cancel_reason: str | None = None

    def cancel(self, reason: str = "cancelled") -> None:
        self.cancel_reason = reason
        self.cancel_event.set()


class TwoPassRunner(Protocol):
    def run_analysis(
        self, task: FrozenTask, context: TaskExecutionContext
    ) -> StageResult | Mapping[str, Any]: ...

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any],
        context: TaskExecutionContext,
    ) -> StageResult | Mapping[str, Any]: ...


RunnerFactory = Callable[[FrozenTask, TaskExecutionContext], TwoPassRunner]


@dataclass(frozen=True)
class DispatchResult:
    unit_sha256: str
    status: str
    outcome: str | None = None
    error_code: str | None = None
    completion: Mapping[str, Any] | None = None


class DispatchHandle:
    def __init__(self, unit_sha256: str) -> None:
        self.unit_sha256 = unit_sha256
        self._done = threading.Event()
        self._result: DispatchResult | None = None
        self._context: TaskExecutionContext | None = None
        self._result_lock = threading.Lock()

    def _bind_context(self, context: TaskExecutionContext) -> None:
        self._context = context

    def _set_result(self, result: DispatchResult) -> bool:
        # A forced shutdown terminal is authoritative.  A worker that returns
        # after its lease was fenced must not replace that result in memory.
        with self._result_lock:
            if self._done.is_set():
                return False
            self._result = result
            self._done.set()
            return True

    def cancel(self, reason: str = "cancelled") -> bool:
        if self._done.is_set() or self._context is None:
            return False
        self._context.cancel(reason)
        return True

    def wait(self, timeout: float | None = None) -> DispatchResult:
        if not self._done.wait(timeout):
            raise TimeoutError("dispatch_handle_wait_timeout")
        assert self._result is not None
        return self._result

    @property
    def done(self) -> bool:
        return self._done.is_set()

    @property
    def task(self) -> FrozenTask | None:
        return self._context.task if self._context is not None else None




class ConcurrentDispatcher:
    """One independent task thread per frozen unique unit, with no cap."""

    def __init__(
        self,
        runtime_root: Path,
        runner_factory: RunnerFactory,
        *,
        soft_runtime_warning_seconds: float | None = None,
        stage_timeout_seconds: float | None = None,
        stall_timeout_seconds: float | None = None,
        stall_probe_interval_seconds: float = 60.0,
        stall_probe_required_consecutive_failures: int = 2,
        controlled_replay_authority: Mapping[str, Any] | None = None,
        production_canary: bool = False,
    ) -> None:
        resolved_soft_warning = (
            float(soft_runtime_warning_seconds)
            if soft_runtime_warning_seconds is not None
            else float(stage_timeout_seconds)
            if stage_timeout_seconds is not None
            else 1800.0
        )
        if resolved_soft_warning <= 0:
            raise DispatchError("soft_runtime_warning_invalid")
        resolved_stall_timeout = (
            float(stall_timeout_seconds)
            if stall_timeout_seconds is not None
            else 1800.0
        )
        if resolved_stall_timeout <= 0:
            raise DispatchError("stall_timeout_invalid")
        if stall_probe_interval_seconds <= 0:
            raise DispatchError("stall_probe_interval_invalid")
        if (
            isinstance(stall_probe_required_consecutive_failures, bool)
            or int(stall_probe_required_consecutive_failures) < 2
        ):
            raise DispatchError("stall_probe_required_failures_invalid")
        self.runtime_root = runtime_root.resolve()
        self.runner_factory = runner_factory
        self.soft_runtime_warning_seconds = resolved_soft_warning
        self.stall_timeout_seconds = resolved_stall_timeout
        self.stall_probe_interval_seconds = float(stall_probe_interval_seconds)
        self.stall_probe_required_consecutive_failures = int(
            stall_probe_required_consecutive_failures
        )
        self.lease_store = LeaseStore(self.runtime_root)
        self.owner_id = f"dispatcher-{os.getpid()}-{uuid.uuid4().hex}"
        self.controlled_replay_authority = (
            copy.deepcopy(dict(controlled_replay_authority))
            if controlled_replay_authority is not None
            else None
        )
        if production_canary and controlled_replay_authority is not None:
            raise DispatchError("production_canary_controlled_replay_forbidden")
        self.production_canary = bool(production_canary)
        self._condition = threading.Condition()
        self._active: dict[str, DispatchHandle] = {}
        self._finishing: set[str] = set()
        self._draining = False

    @property
    def active_count(self) -> int:
        with self._condition:
            return len(self._active)

    @property
    def draining(self) -> bool:
        with self._condition:
            return self._draining

    def submit(self, task: FrozenTask) -> DispatchHandle:
        unit = task.unit_sha256
        with self._condition:
            if self._draining:
                raise DispatchError("dispatcher_draining")
            existing = self._active.get(unit)
            if existing is not None:
                return existing
            raw_subject = task.frozen_payload.get("subject")
            subject = str(raw_subject) if raw_subject is not None else None
            decision = self.lease_store.claim(
                unit,
                self.owner_id,
                subject=subject,
                task=task,
                controlled_replay_authority=self.controlled_replay_authority,
                production_canary=self.production_canary,
            )
            self.lease_store.register_content_members(task)
            if decision.status == "completed":
                handle = DispatchHandle(unit)
                handle._set_result(
                    DispatchResult(
                        unit,
                        "deduplicated",
                        outcome=str(decision.completion.get("outcome")),
                        completion=decision.completion,
                    )
                )
                return handle
            if decision.status == "active":
                handle = DispatchHandle(unit)
                handle._set_result(DispatchResult(unit, "deduplicated_active"))
                return handle
            if decision.lease is None:
                raise DispatchError("claim_decision_invalid")
            handle = DispatchHandle(unit)
            context_root = (
                self.runtime_root
                / "dispatch"
                / "contexts"
                / unit
                / f"fence-{decision.lease.fence}"
            )
            context_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            if self.production_canary:
                (context_root / "mcp-session").mkdir(
                    parents=True, exist_ok=True, mode=0o700
                )
                (context_root / "reports").mkdir(
                    parents=True, exist_ok=True, mode=0o700
                )
            context = TaskExecutionContext(
                task,
                decision.lease,
                context_root,
                self.soft_runtime_warning_seconds,
                self.stall_timeout_seconds,
                self.stall_probe_interval_seconds,
                self.stall_probe_required_consecutive_failures,
                resume_from_analysis_checkpoint=(
                    self.lease_store.resume_checkpoint_required(
                        decision.lease
                    )
                ),
            )
            handle._bind_context(context)
            self.lease_store.record_task_event(task, decision.lease, "claim")
            self._active[unit] = handle
            thread = threading.Thread(
                target=self._execute,
                name=f"preprocess-{unit[:12]}",
                args=(task, handle, context),
                daemon=True,
            )
            try:
                _start_thread_or_raise_retry(thread)
            except RetryableDispatchError as exc:
                try:
                    self.lease_store.record_task_event(
                        task,
                        context.lease,
                        "retry_wait",
                        error_code=exc.code,
                    )
                    self.lease_store.mark_retry_wait(
                        context.lease,
                        error_code=exc.code,
                        retry_kind="process_resource",
                    )
                    result = DispatchResult(
                        unit,
                        "retry_wait",
                        outcome="waiting_retry",
                        error_code=exc.code,
                    )
                except (DispatchError, OSError) as publish_exc:
                    result = DispatchResult(
                        unit,
                        "publish_rejected",
                        outcome="waiting_retry",
                        error_code=(
                            publish_exc.code
                            if isinstance(publish_exc, DispatchError)
                            else "thread_start_retry_publish_os_error"
                        ),
                    )
                self._active.pop(unit, None)
                self._condition.notify_all()
                if self.production_canary:
                    try:
                        self.lease_store.finish_production_canary_task(
                            task,
                            outcome=result.outcome,
                            error_code=result.error_code,
                            completion=result.completion,
                        )
                    except (DispatchError, OSError) as canary_exc:
                        original_code = (
                            canary_exc.code
                            if isinstance(canary_exc, DispatchError)
                            else "production_canary_terminal_os_error"
                        )
                        try:
                            self.lease_store.fail_production_canary_closed(
                                task, error_code=original_code
                            )
                        except (DispatchError, OSError):
                            original_code = (
                                "production_canary_fail_closed_unavailable"
                            )
                        result = DispatchResult(
                            unit,
                            "publish_rejected",
                            outcome="failed",
                            error_code=original_code,
                        )
                handle._set_result(result)
            return handle

    def dispatch(
        self, tasks: Iterable[FrozenTask], *, wait: bool = True
    ) -> list[DispatchHandle] | list[DispatchResult]:
        handles: list[DispatchHandle] = []
        for task in tasks:
            try:
                handle = self.submit(task)
            except DispatchError as exc:
                handle = DispatchHandle(task.unit_sha256)
                handle._set_result(
                    DispatchResult(
                        task.unit_sha256,
                        "submit_rejected",
                        outcome="failed",
                        error_code=exc.code,
                    )
                )
            except OSError as exc:
                handle = DispatchHandle(task.unit_sha256)
                handle._set_result(
                    DispatchResult(
                        task.unit_sha256,
                        "submit_rejected",
                        outcome="failed",
                        error_code=(
                            _process_resource_error_code(exc)
                            or "submit_os_error"
                        ),
                    )
                )
            except RuntimeError:
                handle = DispatchHandle(task.unit_sha256)
                handle._set_result(
                    DispatchResult(
                        task.unit_sha256,
                        "submit_rejected",
                        outcome="failed",
                        error_code="submit_runtime_failed",
                    )
                )
            handles.append(handle)
        if not wait:
            return handles
        return [handle.wait() for handle in handles]

    def cancel(self, unit_sha256: str) -> bool:
        with self._condition:
            handle = self._active.get(unit_sha256)
        return handle.cancel() if handle is not None else False

    def emergency_cancel(
        self,
        *,
        timeout: float = 10.0,
        error_code: str = "daemon_shutdown",
    ) -> dict[str, Any]:
        """Cancel every active unit and seal a bounded late-result fence.

        Normal runners observe the cancellation within the 50 ms stage poll,
        kill their owned process group, and publish a cancelled terminal.  If
        an adapter does not converge before ``timeout``, this control thread
        publishes that same terminal under the current lease fence.  The late
        worker then cannot publish a package or overwrite the handle result.
        """

        if isinstance(timeout, bool) or timeout <= 0:
            raise DispatchError("emergency_cancel_timeout_invalid")
        with self._condition:
            self._draining = True
            handles = list(self._active.values())
        for handle in handles:
            handle.cancel(error_code)
        deadline = time.monotonic() + float(timeout)
        with self._condition:
            while self._active and time.monotonic() < deadline:
                self._condition.wait(max(0.0, deadline - time.monotonic()))
            remaining = [
                handle
                for unit, handle in self._active.items()
                if unit not in self._finishing
            ]

        forced = 0
        for handle in remaining:
            context = handle._context
            if context is None:
                continue
            task = context.task
            with self._condition:
                if (
                    self._active.get(task.unit_sha256) is not handle
                    or task.unit_sha256 in self._finishing
                ):
                    continue
                # Own terminal publication before fencing the lease.  A worker
                # completing in the same instant will observe this marker and
                # cannot replace the emergency result.
                self._finishing.add(task.unit_sha256)
            context.cancel(error_code)
            now = _utc_now()
            try:
                completion = self.lease_store.publish_terminal(
                    context.lease,
                    task=task,
                    outcome="cancelled",
                    error_code=error_code,
                    analysis=None,
                    critical_review=None,
                    started_at=now,
                    finished_at=now,
                )
                completion_path = self.lease_store._completion_path(
                    context.lease.unit_sha256
                )
                self.lease_store.record_task_event(
                    task,
                    context.lease,
                    "cancel",
                    error_code=error_code,
                    artifact_refs={
                        "completion_path": str(completion_path),
                        "completion_sha256": _sha256_bytes(
                            completion_path.read_bytes()
                        ),
                        "receipt_path": completion.get("receipt_path"),
                        "receipt_sha256": completion.get("receipt_sha256"),
                        "package_path": None,
                        "package_sha256": None,
                    },
                )
                result = DispatchResult(
                    task.unit_sha256,
                    "completed",
                    outcome="cancelled",
                    error_code=error_code,
                    completion=completion,
                )
            except (DispatchError, OSError) as exc:
                result = DispatchResult(
                    task.unit_sha256,
                    "publish_rejected",
                    outcome="cancelled",
                    error_code=(
                        exc.code
                        if isinstance(exc, DispatchError)
                        else "emergency_cancel_terminal_os_error"
                    ),
                )
            self._finish_active_claimed(task, handle, result)
            forced += 1

        final_deadline = time.monotonic() + min(2.0, float(timeout))
        with self._condition:
            while self._active and time.monotonic() < final_deadline:
                self._condition.wait(
                    max(0.0, final_deadline - time.monotonic())
                )
            return {
                "requested_cancel_count": len(handles),
                "forced_terminal_count": forced,
                "active_count": len(self._active),
                "late_result_fence_status": (
                    "sealed" if not self._active else "incomplete"
                ),
                "error_code": error_code,
            }

    def drain(self, timeout: float | None = None) -> bool:
        """Atomically stop accepting claims and wait for active claims to reach zero."""

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            self._draining = True
            while self._active:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def _execute(
        self,
        task: FrozenTask,
        handle: DispatchHandle,
        context: TaskExecutionContext,
    ) -> None:
        started_at = _utc_now()
        analysis: StageResult | None = None
        checkpoint_refs: Mapping[str, str] | None = None
        outcome = "failed"
        error_code: str | None = None
        critical: StageResult | None = None

        while True:
            stop_heartbeat = threading.Event()
            heartbeat = threading.Thread(
                target=self._heartbeat_loop,
                args=(context.lease, stop_heartbeat, context),
                name=(
                    f"heartbeat-{task.unit_sha256[:12]}-"
                    f"{context.lease.fence}"
                ),
                daemon=True,
            )
            heartbeat_started = False
            runner: TwoPassRunner | None = None
            retry_kind: str | None = None
            infrastructure_failure = False
            critical = None
            outcome = "failed"
            error_code = None
            terminal_error: str | None = None
            terminal_quality_error: str | None = None
            report_disposition: str | None = None
            review_stage_count = 0
            try:
                _start_thread_or_raise_retry(heartbeat)
                heartbeat_started = True
                self.lease_store.record_task_event(
                    task, context.lease, "process_started"
                )
                runner = self.runner_factory(task, context)
                opaque_two_pass = bool(
                    getattr(runner, "executes_full_two_pass_in_analysis", False)
                )
                if analysis is None and not opaque_two_pass:
                    restored = self.lease_store.load_analysis_checkpoint(
                        task, context.lease
                    )
                    if restored is not None:
                        analysis, checkpoint_refs = restored
                        self.lease_store.record_task_event(
                            task,
                            context.lease,
                            "analysis_checkpoint_reused",
                            artifact_refs=checkpoint_refs,
                        )
                if analysis is None:
                    if not opaque_two_pass:
                        self.lease_store.record_task_event(
                            task, context.lease, "analysis_submitted"
                        )
                    analysis = self._invoke_stage(
                        "analysis", runner, task, context, draft_analysis=None
                    )
                    if not opaque_two_pass:
                        checkpoint_refs = (
                            self.lease_store.publish_analysis_checkpoint(
                                task, context.lease, analysis
                            )
                        )
                        self.lease_store.record_task_event(
                            task,
                            context.lease,
                            "analysis_completed",
                            artifact_refs=checkpoint_refs,
                        )
                if context.cancel_event.is_set():
                    raise DispatchCancelled()
                terminal_outcome = getattr(runner, "terminal_outcome", None)
                terminal_error = getattr(runner, "terminal_error_code", None)
                terminal_quality_error = getattr(
                    runner, "terminal_quality_error_code", None
                )
                report_disposition = getattr(
                    runner, "terminal_report_disposition", None
                )
                review_stage_count = int(
                    getattr(runner, "terminal_review_stage_count", 0) or 0
                )
                early_review_terminal = bool(
                    review_stage_count == 1
                    and (
                        terminal_outcome == "succeeded"
                        and report_disposition == "needs_sol_review"
                        and terminal_error is None
                        and isinstance(terminal_quality_error, str)
                        and terminal_quality_error
                        or terminal_outcome == "failed"
                        and report_disposition == "quarantined"
                        and isinstance(terminal_error, str)
                        and terminal_error
                    )
                )
                if not early_review_terminal:
                    if not opaque_two_pass:
                        self.lease_store.record_task_event(
                            task, context.lease, "critical_started"
                        )
                    critical = self._invoke_stage(
                        "critical_review",
                        runner,
                        task,
                        context,
                        draft_analysis=analysis.payload,
                    )
                    if not opaque_two_pass:
                        self.lease_store.record_task_event(
                            task, context.lease, "critical_completed"
                        )
                    terminal_outcome = getattr(
                        runner, "terminal_outcome", None
                    )
                    terminal_error = getattr(
                        runner, "terminal_error_code", None
                    )
                    terminal_quality_error = getattr(
                        runner, "terminal_quality_error_code", None
                    )
                    report_disposition = getattr(
                        runner, "terminal_report_disposition", None
                    )
                    review_stage_count = int(
                        getattr(
                            runner, "terminal_review_stage_count", 0
                        )
                        or 0
                    )
                if terminal_outcome is None:
                    outcome = "succeeded"
                elif (
                    terminal_outcome == "succeeded"
                    and report_disposition == "needs_sol_review"
                    and review_stage_count in {1, 2}
                    and terminal_error is None
                    and isinstance(terminal_quality_error, str)
                    and terminal_quality_error
                ):
                    outcome = "succeeded"
                    error_code = None
                elif (
                    terminal_outcome == "failed"
                    and report_disposition == "quarantined"
                    and review_stage_count in {1, 2}
                    and isinstance(terminal_error, str)
                    and terminal_error
                ):
                    outcome = "failed"
                    error_code = terminal_error
                else:
                    raise DispatchError("runner_terminal_outcome_invalid")
            except StageTimeout as exc:
                outcome = "timed_out"
                error_code = exc.code
            except StageStalled as exc:
                outcome = "stalled"
                error_code = exc.code
            except DispatchCancelled as exc:
                outcome = "cancelled"
                error_code = context.cancel_reason or exc.code
            except OSError as exc:
                resource_code = _process_resource_error_code(exc)
                if resource_code is not None:
                    retry_kind = "process_resource"
                    error_code = resource_code
                    outcome = "waiting_retry"
                else:
                    infrastructure_failure = True
                    suffix = errno.errorcode.get(exc.errno, str(exc.errno))
                    error_code = f"runner_os_error_{str(suffix).lower()}"
            except RetryableDispatchError as exc:
                error_code = exc.code
                if _is_service_retry_code(exc.code):
                    retry_kind = "service_limit"
                    outcome = "waiting_retry"
                elif _is_process_resource_retry_code(exc.code):
                    retry_kind = "process_resource"
                    outcome = "waiting_retry"
            except InfrastructureCrash as exc:
                error_code = exc.code
                infrastructure_failure = True
            except DispatchError as exc:
                error_code = exc.code
                if _is_timeout_code(exc.code):
                    outcome = "timed_out"
                elif _is_process_resource_retry_code(exc.code):
                    retry_kind = "process_resource"
                    outcome = "waiting_retry"
                elif _is_service_retry_code(exc.code):
                    retry_kind = "service_limit"
                    outcome = "waiting_retry"
                elif _is_infrastructure_crash_code(exc.code):
                    infrastructure_failure = True
            except BaseException as exc:  # isolate arbitrary adapter crashes
                error_code = f"runner_crash:{type(exc).__name__}"
            finally:
                stop_heartbeat.set()
                if heartbeat_started:
                    heartbeat.join(timeout=1)

            if retry_kind is not None:
                try:
                    resume_from_checkpoint = (
                        self.lease_store.lease_has_recoverable_analysis_checkpoint(
                            context.lease
                        )
                    )
                    self.lease_store.record_task_event(
                        task,
                        context.lease,
                        "retry_wait",
                        error_code=error_code,
                        artifact_refs=checkpoint_refs,
                    )
                    self.lease_store.mark_retry_wait(
                        context.lease,
                        error_code=str(error_code),
                        retry_kind=retry_kind,
                        resume_from_analysis_checkpoint=(
                            resume_from_checkpoint
                        ),
                    )
                    result = DispatchResult(
                        task.unit_sha256,
                        "retry_wait",
                        outcome="waiting_retry",
                        error_code=error_code,
                    )
                except DispatchError as exc:
                    result = DispatchResult(
                        task.unit_sha256,
                        "publish_rejected",
                        outcome="waiting_retry",
                        error_code=exc.code,
                    )
                self._finish_active(task, handle, result)
                return

            if infrastructure_failure and self.production_canary:
                # A production task process may already have submitted a model
                # stage before its supervisor exits.  Retrying the whole unit
                # would permit a duplicate model submission.  Its sealed exit
                # closure remains authoritative for this fence; close the unit
                # with the original error and pause only this subject.
                outcome = "failed"
            elif infrastructure_failure:
                try:
                    resume_from_checkpoint = (
                        self.lease_store.lease_has_recoverable_analysis_checkpoint(
                            context.lease
                        )
                    )
                    recovery_count = (
                        self.lease_store.infrastructure_recovery_count(
                            context.lease
                        )
                    )
                    if recovery_count >= 1:
                        raise DispatchError("infrastructure_recovery_exhausted")
                    self.lease_store.record_task_event(
                        task,
                        context.lease,
                        "recovery_scheduled",
                        error_code=error_code,
                        artifact_refs=checkpoint_refs,
                    )
                    recovered_lease = (
                        self.lease_store.recover_after_infrastructure_crash(
                            context.lease,
                            error_code=str(error_code),
                            resume_from_analysis_checkpoint=(
                                resume_from_checkpoint
                            ),
                        )
                    )
                    context_root = (
                        self.runtime_root
                        / "dispatch"
                        / "contexts"
                        / task.unit_sha256
                        / f"fence-{recovered_lease.fence}"
                    )
                    context_root.mkdir(parents=True, exist_ok=True, mode=0o700)
                    context = TaskExecutionContext(
                        task,
                        recovered_lease,
                        context_root,
                        self.soft_runtime_warning_seconds,
                        self.stall_timeout_seconds,
                        self.stall_probe_interval_seconds,
                        self.stall_probe_required_consecutive_failures,
                        resume_from_analysis_checkpoint=(
                            resume_from_checkpoint
                        ),
                    )
                    handle._bind_context(context)
                    self.lease_store.record_task_event(
                        task, recovered_lease, "claim"
                    )
                    analysis = None
                    continue
                except DispatchError as exc:
                    if exc.code != "infrastructure_recovery_exhausted":
                        result = DispatchResult(
                            task.unit_sha256,
                            "publish_rejected",
                            outcome="failed",
                            error_code=exc.code,
                        )
                        self._finish_active(task, handle, result)
                        return
                    error_code = (
                        "infrastructure_recovery_exhausted:"
                        f"{error_code or 'unknown'}"
                    )
                    outcome = "failed"

            finished_at = _utc_now()
            try:
                completion = self.lease_store.publish_terminal(
                    context.lease,
                    task=task,
                    outcome=outcome,
                    error_code=error_code,
                    analysis=analysis,
                    critical_review=critical,
                    report_disposition=report_disposition,
                    quality_error_code=(
                        terminal_quality_error
                        if report_disposition == "needs_sol_review"
                        else None
                    ),
                    started_at=started_at,
                    finished_at=finished_at,
                )
                if completion.get("outcome") == "succeeded":
                    outcome = "succeeded"
                    error_code = None
                completion_path = self.lease_store._completion_path(
                    context.lease.unit_sha256
                )
                terminal_event = (
                    "published"
                    if outcome == "succeeded"
                    else "needs_rework"
                    if outcome == "needs_rework"
                    else "timeout"
                    if outcome == "timed_out"
                    else "cancel"
                    if outcome in {"cancelled", "stalled"}
                    else "failed"
                )
                self.lease_store.record_task_event(
                    task,
                    context.lease,
                    terminal_event,
                    error_code=error_code,
                    artifact_refs={
                        "completion_path": str(completion_path),
                        "completion_sha256": _sha256_bytes(
                            completion_path.read_bytes()
                        ),
                        "receipt_path": completion.get("receipt_path"),
                        "receipt_sha256": completion.get("receipt_sha256"),
                        "package_path": completion.get("package_path"),
                        "package_sha256": completion.get("package_sha256"),
                    },
                )
                result = DispatchResult(
                    task.unit_sha256,
                    "completed",
                    outcome=outcome,
                    error_code=error_code,
                    completion=completion,
                )
            except DispatchError as exc:
                result = DispatchResult(
                    task.unit_sha256,
                    "publish_rejected",
                    outcome=outcome,
                    error_code=exc.code,
                )
            except OSError as exc:
                result = DispatchResult(
                    task.unit_sha256,
                    "publish_rejected",
                    outcome=outcome,
                    error_code=(
                        "terminal_publish_os_error_"
                        f"{errno.errorcode.get(exc.errno, exc.errno)}"
                    ),
                )
            self._finish_active(task, handle, result)
            return

    def _finish_active(
        self,
        task: FrozenTask,
        handle: DispatchHandle,
        result: DispatchResult,
    ) -> None:
        with self._condition:
            if (
                self._active.get(task.unit_sha256) is not handle
                or task.unit_sha256 in self._finishing
            ):
                return
            self._finishing.add(task.unit_sha256)
        self._finish_active_claimed(task, handle, result)

    def _finish_active_claimed(
        self,
        task: FrozenTask,
        handle: DispatchHandle,
        result: DispatchResult,
    ) -> None:
        if self.production_canary:
            try:
                self.lease_store.finish_production_canary_task(
                    task,
                    outcome=result.outcome,
                    error_code=result.error_code,
                    completion=result.completion,
                )
            except (DispatchError, OSError) as exc:
                original_code = (
                    exc.code
                    if isinstance(exc, DispatchError)
                    else "production_canary_terminal_os_error"
                )
                try:
                    self.lease_store.fail_production_canary_closed(
                        task, error_code=original_code
                    )
                except (DispatchError, OSError):
                    original_code = "production_canary_fail_closed_unavailable"
                result = DispatchResult(
                    task.unit_sha256,
                    "publish_rejected",
                    outcome="failed",
                    error_code=original_code,
                    completion=result.completion,
                )
        with self._condition:
            self._active.pop(task.unit_sha256, None)
            self._finishing.discard(task.unit_sha256)
            self._condition.notify_all()
        handle._set_result(result)

    def _heartbeat_loop(
        self,
        lease: Lease,
        stop: threading.Event,
        context: TaskExecutionContext,
    ) -> None:
        while not stop.wait(HEARTBEAT_INTERVAL_SECONDS):
            if not self.lease_store.heartbeat(lease):
                context.cancel("lease_lost")
                return

    def _invoke_stage(
        self,
        stage: str,
        runner: TwoPassRunner,
        task: FrozenTask,
        context: TaskExecutionContext,
        *,
        draft_analysis: Mapping[str, Any] | None,
    ) -> StageResult:
        if context.cancel_event.is_set():
            _cancel_runner(runner, context)
            raise DispatchCancelled()
        result_queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                if stage == "analysis":
                    value = runner.run_analysis(task, context)
                else:
                    assert draft_analysis is not None
                    value = runner.run_critical_review(
                        task, draft_analysis, context
                    )
                result_queue.put_nowait(("result", value))
            except BaseException as exc:
                result_queue.put_nowait(("error", exc))

        stage_thread = threading.Thread(
            target=invoke,
            name=f"{stage}-{task.unit_sha256[:12]}",
            daemon=True,
        )
        _start_thread_or_raise_retry(stage_thread)
        stage_started = time.monotonic()
        last_progress_monotonic = stage_started
        last_progress_sha256: str | None = None
        observed_stage = stage
        subject = str(task.frozen_payload.get("subject") or "")
        provider_stage = f"{subject}_{stage}"
        production_progress = subject in {"math", "cs408", "english"}
        if production_progress:
            transition = self.lease_store.publish_stage_progress(
                task,
                context.lease,
                stage_name=provider_stage,
                progress_kind="stage_transition",
            )
            last_progress_sha256 = str(
                transition["progress_receipt_sha256"]
            )
        soft_warning_emitted = False
        failed_probe_rounds = 0
        stall_probe_attempts = 0
        last_probe_at: float | None = None
        cancelled_notified = False
        while True:
            if context.cancel_event.is_set():
                if not cancelled_notified:
                    _cancel_runner(runner, context)
                    cancelled_notified = True
                raise DispatchCancelled()
            now = time.monotonic()
            progress = (
                self.lease_store.latest_stage_progress(
                    task, context.lease, stage_name=provider_stage
                )
                if production_progress
                else None
            )
            if progress is not None:
                progress_sha256 = str(progress["progress_receipt_sha256"])
                if (
                    progress_sha256 != last_progress_sha256
                    and progress.get("progress_kind")
                    != "provider_liveness"
                ):
                    last_progress_sha256 = progress_sha256
                    last_progress_monotonic = now
                    failed_probe_rounds = 0
                    last_probe_at = None
            if (
                not soft_warning_emitted
                and now - stage_started
                >= context.soft_runtime_warning_seconds
            ):
                self.lease_store.record_task_event(
                    task,
                    context.lease,
                    "soft_timeout_warning",
                    stage_name=observed_stage,
                    artifact_refs={
                        "soft_timeout_seconds": (
                            context.soft_runtime_warning_seconds
                        )
                    },
                )
                soft_warning_emitted = True
            if now - last_progress_monotonic >= context.stall_timeout_seconds:
                if (
                    last_probe_at is None
                    or now - last_probe_at
                    >= context.stall_probe_interval_seconds
                ):
                    last_probe_at = now
                    nonce = uuid.uuid4().hex
                    nonce_sha256 = _sha256_bytes(nonce.encode("ascii"))
                    probe_callback = getattr(runner, "probe_stage", None)
                    probe: Mapping[str, Any] = {}
                    if callable(probe_callback):
                        try:
                            raw_probe = probe_callback(
                                task,
                                context,
                                stage=observed_stage,
                                nonce=nonce,
                            )
                            if isinstance(raw_probe, Mapping):
                                probe = raw_probe
                        except BaseException:
                            probe = {}
                    verified_probe: Mapping[str, Any] = {}
                    if (
                        probe.get("probe_supported") is True
                        and probe.get("probe_nonce") == nonce
                    ):
                        probe_receipt_sha256 = probe.get(
                            "provider_kernel_probe_receipt_sha256"
                        )
                        probe_receipt_path = probe.get(
                            "provider_kernel_probe_receipt_path"
                        )
                        if (
                            isinstance(probe_receipt_sha256, str)
                            and isinstance(probe_receipt_path, str)
                        ):
                            try:
                                verified_probe = (
                                    self.lease_store
                                    .verify_provider_kernel_probe_receipt(
                                        task,
                                        context.lease,
                                        stage_name=provider_stage,
                                        probe_nonce_sha256=nonce_sha256,
                                        provider_kernel_probe_receipt_sha256=(
                                            probe_receipt_sha256
                                        ),
                                        provider_kernel_probe_receipt_path=(
                                            probe_receipt_path
                                        ),
                                    )
                                )
                            except (DispatchError, OSError):
                                verified_probe = {}
                    supported = bool(verified_probe)
                    control_ok = (
                        supported
                        and verified_probe.get("control_channel_ok") is True
                    )
                    data_plane_ok = (
                        supported
                        and verified_probe.get("provider_data_plane_ok")
                        is True
                    )
                    kernel_activity_ok = (
                        supported
                        and verified_probe.get(
                            "provider_kernel_activity_ok"
                        )
                        is True
                    )
                    automatic_cancellation_eligible = (
                        supported
                        and verified_probe.get(
                            "automatic_stall_cancellation_eligible"
                        )
                        is True
                    )
                    both_failed = (
                        automatic_cancellation_eligible
                        and not kernel_activity_ok
                        and not data_plane_ok
                    )
                    if both_failed:
                        failed_probe_rounds += 1
                    elif kernel_activity_ok or data_plane_ok:
                        failed_probe_rounds = 0
                        last_progress_monotonic = now
                        last_probe_at = None
                    else:
                        # A task-runner control ACK is diagnostic only.  An
                        # unavailable/unverified Provider signal disables
                        # automatic cancellation instead of fabricating
                        # progress or a failed independent probe.
                        failed_probe_rounds = 0
                    if supported:
                        stall_probe_attempts += 1
                        public_status = (
                            "healthy"
                            if kernel_activity_ok or data_plane_ok
                            else "stall_suspected"
                            if both_failed
                            else "probing"
                        )
                        event_name = (
                            "stall_probe_failed"
                            if both_failed
                            else "stall_probe"
                        )
                        self.lease_store.record_task_event(
                            task,
                            context.lease,
                            event_name,
                            stage_name=observed_stage,
                            artifact_refs={
                                "stall_probe_number": stall_probe_attempts,
                                "stall_probe_status": public_status,
                                "stall_probe_nonce_sha256": nonce_sha256,
                                "stall_probe_control_channel_ok": control_ok,
                                "stall_probe_provider_data_plane_ok": (
                                    data_plane_ok
                                ),
                                "stall_probe_provider_kernel_activity_ok": (
                                    kernel_activity_ok
                                ),
                                "stall_probe_automatic_cancellation_eligible": (
                                    automatic_cancellation_eligible
                                ),
                                "provider_kernel_probe_receipt_sha256": (
                                    verified_probe[
                                        "provider_kernel_probe_receipt_sha256"
                                    ]
                                ),
                                "provider_kernel_probe_receipt_path": (
                                    verified_probe[
                                        "provider_kernel_probe_receipt_path"
                                    ]
                                ),
                                "provider_process_identity_sha256": (
                                    verified_probe[
                                        "provider_process_identity_sha256"
                                    ]
                                ),
                                "provider_process_identity_path": (
                                    verified_probe[
                                        "provider_process_identity_path"
                                    ]
                                ),
                                "stall_timeout_seconds": (
                                    context.stall_timeout_seconds
                                ),
                            },
                        )
                    if (
                        failed_probe_rounds
                        >= context.stall_probe_required_consecutive_failures
                    ):
                        error_code = f"{observed_stage}_stalled"
                        self.lease_store.record_task_event(
                            task,
                            context.lease,
                            "stage_stalled",
                            stage_name="terminal",
                            error_code=error_code,
                            artifact_refs={
                                "stall_probe_number": stall_probe_attempts,
                                "stall_probe_status": "stalled",
                                "stall_probe_nonce_sha256": nonce_sha256,
                                "stall_probe_control_channel_ok": control_ok,
                                "stall_probe_provider_data_plane_ok": False,
                                "stall_probe_provider_kernel_activity_ok": (
                                    False
                                ),
                                "stall_probe_automatic_cancellation_eligible": (
                                    True
                                ),
                                "provider_kernel_probe_receipt_sha256": (
                                    verified_probe[
                                        "provider_kernel_probe_receipt_sha256"
                                    ]
                                ),
                                "provider_kernel_probe_receipt_path": (
                                    verified_probe[
                                        "provider_kernel_probe_receipt_path"
                                    ]
                                ),
                                "provider_process_identity_sha256": (
                                    verified_probe[
                                        "provider_process_identity_sha256"
                                    ]
                                ),
                                "provider_process_identity_path": (
                                    verified_probe[
                                        "provider_process_identity_path"
                                    ]
                                ),
                                "stall_timeout_seconds": (
                                    context.stall_timeout_seconds
                                ),
                            },
                        )
                        context.cancel(error_code)
                        _cancel_runner(runner, context)
                        raise StageStalled(observed_stage)
            try:
                kind, value = result_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if kind == "error":
                assert isinstance(value, BaseException)
                raise value
            return StageResult.coerce(value)


def _cancel_runner(runner: object, context: TaskExecutionContext) -> None:
    callback = getattr(runner, "cancel", None)
    if callable(callback):
        try:
            callback(context)
        except BaseException:
            pass


class SubprocessTwoPassRunner:
    """Bridge runner for existing adapters exposed as a JSON subprocess.

    The command receives ``--stage analysis`` or ``--stage critical_review``.
    Its stdin is a frozen request object and stdout must be a ``StageResult``
    mapping.  Each dispatcher task owns a separate runner instance and process
    group, so timeout or cancellation never signals a sibling unit.
    """

    def __init__(self, command: Sequence[str]) -> None:
        if not command:
            raise DispatchError("runner_command_empty")
        self.command = tuple(command)
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None

    def run_analysis(
        self, task: FrozenTask, context: TaskExecutionContext
    ) -> StageResult:
        return self._run("analysis", task, None, context)

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any],
        context: TaskExecutionContext,
    ) -> StageResult:
        return self._run("critical_review", task, draft_analysis, context)

    def _run(
        self,
        stage: str,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any] | None,
        context: TaskExecutionContext,
    ) -> StageResult:
        request = {
            "schema_version": DISPATCH_CONTRACT_SCHEMA,
            "stage": stage,
            "task": task.as_dict(),
            "draft_analysis": draft_analysis,
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "lease_fence": context.lease.fence,
        }
        env = os.environ.copy()
        env.update(
            {
                "STUDY_PREPROCESS_MODEL": REQUIRED_MODEL,
                "STUDY_PREPROCESS_REASONING_EFFORT": REQUIRED_REASONING_EFFORT,
                "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
                "STUDY_PREPROCESS_LEASE_FENCE": str(context.lease.fence),
            }
        )
        started = time.monotonic()
        process = subprocess.Popen(
            [*self.command, "--stage", stage],
            cwd=context.root,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        with self._lock:
            self._process = process
        try:
            stdout, _stderr = process.communicate(_canonical_bytes(request))
        finally:
            with self._lock:
                if self._process is process:
                    self._process = None
        if context.cancel_event.is_set():
            raise DispatchCancelled()
        if process.returncode != 0:
            raise DispatchError(f"runner_stage_exit_{process.returncode}")
        try:
            value = json.loads(stdout)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise DispatchError("runner_stage_invalid_json") from exc
        if not isinstance(value, Mapping):
            raise DispatchError("runner_stage_invalid_json")
        value = dict(value)
        value.setdefault("duration_ms", int((time.monotonic() - started) * 1000))
        return StageResult.coerce(value)

    def cancel(self, _context: TaskExecutionContext) -> None:
        with self._lock:
            process = self._process
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


class CallableRunnerAdapter:
    """Small bridge for Python adapters that already expose two callables."""

    def __init__(
        self,
        analysis: Callable[[FrozenTask, TaskExecutionContext], object],
        critical_review: Callable[
            [FrozenTask, Mapping[str, Any], TaskExecutionContext], object
        ],
        cancel: Callable[[TaskExecutionContext], None] | None = None,
    ) -> None:
        self.analysis = analysis
        self.critical_review = critical_review
        self.cancel_callback = cancel

    def run_analysis(self, task: FrozenTask, context: TaskExecutionContext) -> object:
        return self.analysis(task, context)

    def run_critical_review(
        self,
        task: FrozenTask,
        draft_analysis: Mapping[str, Any],
        context: TaskExecutionContext,
    ) -> object:
        return self.critical_review(task, draft_analysis, context)

    def cancel(self, context: TaskExecutionContext) -> None:
        if self.cancel_callback is not None:
            self.cancel_callback(context)


def verify_authoritative_completion(
    runtime_root: Path,
    subject: str,
    capture_id: str,
    *,
    expected_release_id: str,
    expected_unit_sha256: str | None = None,
    expected_generation: int | None = None,
    expected_input_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Consumer-facing API: accept only a HMAC-valid current-release result."""

    return LeaseStore(runtime_root).verify_authoritative_completion(
        subject,
        capture_id,
        expected_release_id=expected_release_id,
        expected_unit_sha256=expected_unit_sha256,
        expected_generation=expected_generation,
        expected_input_fingerprint=expected_input_fingerprint,
    )


def verify_authoritative_task_detail(
    runtime_root: Path,
    unit_sha256: str,
    *,
    expected_release_id: str,
) -> dict[str, Any]:
    """Dashboard-facing verifier; does not import the preprocessing core."""

    return LeaseStore(runtime_root).verify_authoritative_task_detail(
        unit_sha256, expected_release_id=expected_release_id
    )


def verify_content_member_registration(
    runtime_root: Path,
    unit_sha256: str,
    capture_id: str,
    *,
    expected_release_id: str,
    expected_input_fingerprint: str,
) -> dict[str, Any]:
    """Dashboard-facing verifier for one lightweight content-member alias."""

    return LeaseStore(runtime_root).verify_content_member_registration(
        unit_sha256,
        capture_id,
        expected_release_id=expected_release_id,
        expected_input_fingerprint=expected_input_fingerprint,
    )


def verify_evidence_readiness(
    runtime_root: Path,
    receipt_sha256: str,
    *,
    expected_release_id: str,
    expected_capture_id: str,
) -> dict[str, Any]:
    """Consumer-facing API for authenticated zero-model evidence readiness."""

    return LeaseStore(runtime_root).verify_evidence_readiness(
        receipt_sha256,
        expected_release_id=expected_release_id,
        expected_capture_id=expected_capture_id,
    )


def verify_analysis_checkpoint(
    runtime_root: Path,
    checkpoint_sha256: str,
    *,
    expected_unit_sha256: str,
    expected_frozen_payload_sha256: str,
    expected_release_id: str,
) -> dict[str, Any]:
    """Dashboard-facing verifier for one HMAC-bound analysis checkpoint."""

    return LeaseStore(runtime_root).verify_analysis_checkpoint(
        checkpoint_sha256,
        expected_unit_sha256=expected_unit_sha256,
        expected_frozen_payload_sha256=expected_frozen_payload_sha256,
        expected_release_id=expected_release_id,
    )


__all__ = [
    "CallableRunnerAdapter",
    "ConcurrentDispatcher",
    "DISPATCH_RULE_VERSION",
    "DispatchError",
    "DispatchHandle",
    "DispatchResult",
    "FrozenTask",
    "HEARTBEAT_INTERVAL_SECONDS",
    "LEASE_TTL_SECONDS",
    "Lease",
    "LeaseStore",
    "InfrastructureCrash",
    "REQUIRED_MODEL",
    "REQUIRED_REASONING_EFFORT",
    "RetryableDispatchError",
    "StageResult",
    "SubprocessTwoPassRunner",
    "TaskExecutionContext",
    "dispatch_rule_binding",
    "validate_dispatch_rule_binding",
    "verify_authoritative_completion",
    "verify_authoritative_task_detail",
    "verify_content_member_registration",
    "verify_analysis_checkpoint",
    "verify_evidence_readiness",
]
