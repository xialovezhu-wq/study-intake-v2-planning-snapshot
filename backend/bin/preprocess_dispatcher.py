#!/usr/bin/env python3
"""Production daemon and low-level CLI for concurrent preprocessing dispatch."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    SubprocessTwoPassRunner,
    _ExistingSharedFileLock,
)
from dashboard_projection import (  # noqa: E402
    DashboardProjectionError,
    update_subject_and_main_projection,
)
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateRunner,
    CoreCandidateSubprocessRunner,
    producer_authority_binding,
    scan_eligible_candidates,
    validate_fixed_model_contract,
)
from preprocessor_core import (  # noqa: E402
    Candidate,
    PreprocessorError,
    Worker,
    current_date,
    load_config,
    release_identity,
)
from processing_plugin import (  # noqa: E402
    ProcessingPluginError,
    ProcessingPluginHost,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
)
from math_exact_smoke import (  # noqa: E402
    EXACT_ORDER,
    EXACT_SAMPLES,
    MIGRATION_ORDER,
    MathExactSmokeError,
    prepare_exact_math_pending_queue_migration,
    preview_exact_math_pending_queue_migration,
    publish_exact_math_pending_queue_migration_commit,
    reopen_exact_math_pending_queue_migration_commit,
    reopen_exact_gs269_review_terminal,
    rollback_uncommitted_exact_math_pending_queue_migration,
    stage_exact_math_pending_queue_migration,
    verify_exact_batch_authority,
)


SUBJECTS = ("math", "cs408", "english")
MAX_DISCOVERY_LATENCY_SECONDS = 1.0


def _load_task_values(path: str) -> list[Mapping[str, Any]]:
    if path == "-":
        text = sys.stdin.read()
    else:
        try:
            text = Path(path).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise DispatchError("task_manifest_unreadable") from exc
    stripped = text.strip()
    if not stripped:
        return []
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError:
        rows: list[Mapping[str, Any]] = []
        for line in stripped.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DispatchError("task_manifest_invalid_json") from exc
            if not isinstance(row, Mapping):
                raise DispatchError("task_manifest_row_not_object")
            rows.append(row)
        return rows
    if isinstance(value, Mapping):
        tasks = value.get("tasks")
        if tasks is None:
            return [value]
        value = tasks
    if not isinstance(value, list) or not all(
        isinstance(item, Mapping) for item in value
    ):
        raise DispatchError("task_manifest_invalid_shape")
    return value


def _load_control_object(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SubjectSolContractError(f"{label}_unreadable") from exc
    if not isinstance(value, Mapping):
        raise SubjectSolContractError(f"{label}_invalid")
    return value


def _runner_command(value: str) -> tuple[str, ...]:
    try:
        command = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError("must be a JSON argv array") from exc
    if not isinstance(command, list) or not command or not all(
        isinstance(item, str) and item for item in command
    ):
        raise argparse.ArgumentTypeError("must be a non-empty JSON argv array")
    return tuple(command)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unbounded fenced dispatch for frozen study preprocessing units",
        allow_abbrev=False,
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--subject", choices=SUBJECTS)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("run", help="long-running subject dispatcher")
    subparsers.add_parser("run-once", help="scan and drain one subject batch")
    subparsers.add_parser("status", help="read subject lease/drain status")
    subparsers.add_parser(
        "canary-readiness",
        help="read the tri-state subject batch and preserved queue readiness",
    )
    recover_subject_batch = subparsers.add_parser(
        "recover-subject-batch",
        help="apply one generation-aware English preserved-queue recovery",
        allow_abbrev=False,
    )
    recover_subject_batch.add_argument(
        "--expected-original-preclaim-failure-receipt-sha256",
        required=True,
    )
    recover_subject_batch.add_argument(
        "--expected-next-generation", required=True
    )
    recover_subject_batch.add_argument(
        "--expected-next-authority-fingerprint", required=True
    )
    retire_cs408 = subparsers.add_parser(
        "retire-cs408-terminal-batch",
        help="archive the one authorized terminal 408 batch without replay",
        allow_abbrev=False,
    )
    retire_cs408.add_argument("--expected-batch-sha256", required=True)
    retire_cs408.add_argument(
        "--expected-terminal-receipt-sha256", required=True
    )
    retire_cs408.add_argument(
        "--expected-writer-preimage-sha256", required=True
    )
    retire_cs408.add_argument(
        "--expected-batch-pointer-sha256", required=True
    )
    retire_cs408.add_argument("--expected-snapshot-sha256", required=True)
    retire_cs408.add_argument("--expected-source-generation", required=True)
    retire_cs408.add_argument(
        "--expected-source-authority-fingerprint", required=True
    )
    retire_cs408.add_argument("--expected-next-generation", required=True)
    retire_cs408.add_argument(
        "--expected-next-authority-fingerprint", required=True
    )
    reopen_cs408 = subparsers.add_parser(
        "reopen-cs408-terminal-batch-retirement",
        help="read-only reopen of the authorized 408 retirement postimage",
        allow_abbrev=False,
    )
    reopen_cs408.add_argument("--retirement-receipt", type=Path)
    reopen_cs408.add_argument(
        "--expected-terminal-receipt-sha256", required=True
    )
    rollback_cs408 = subparsers.add_parser(
        "rollback-cs408-terminal-batch-retirement",
        help="restore the exact writer preimage for the authorized retirement",
        allow_abbrev=False,
    )
    rollback_cs408.add_argument(
        "--retirement-receipt", type=Path, required=True
    )
    reopen_cs408_rollback = subparsers.add_parser(
        "reopen-cs408-terminal-batch-retirement-rollback",
        help="read-only reopen of the authorized 408 rollback postimage",
        allow_abbrev=False,
    )
    reopen_cs408_rollback.add_argument(
        "--retirement-receipt", type=Path, required=True
    )
    reopen_cs408_rollback.add_argument(
        "--rollback-receipt", type=Path, required=True
    )
    subparsers.add_parser(
        "english-preserved-review-repair-preview",
        help="read-only exact 33548 source lineage preview",
        allow_abbrev=False,
    )
    subparsers.add_parser(
        "math-pending-queue-migration-preview",
        help="read-only exact four-queue Math migration preview",
        allow_abbrev=False,
    )
    subparsers.add_parser(
        "prepare-math-pending-queue-migration",
        help="seal the exact four-queue Math source intent",
        allow_abbrev=False,
    )
    math_migration_apply = subparsers.add_parser(
        "apply-math-pending-queue-migration",
        help="materialize four attempt-1 queues while Math is paused",
        allow_abbrev=False,
    )
    math_migration_apply.add_argument(
        "--migration-intent-sha256", required=True
    )
    math_migration_apply.add_argument(
        "--expected-target-activation-id", required=True
    )
    math_migration_apply.add_argument(
        "--expected-target-authority-generation", required=True
    )
    math_migration_apply.add_argument(
        "--expected-target-subject-authority-fingerprint", required=True
    )
    math_migration_apply.add_argument(
        "--expected-target-producer-authority-fingerprint", required=True
    )
    math_migration_reopen = subparsers.add_parser(
        "reopen-math-pending-queue-migration",
        help="reopen the committed four-queue Math migration",
        allow_abbrev=False,
    )
    math_migration_reopen.add_argument(
        "--migration-descriptor-sha256", required=True
    )
    english_repair_apply = subparsers.add_parser(
        "english-preserved-review-repair-apply",
        help="apply the exact zero-external-call review repair",
        allow_abbrev=False,
    )
    for option in (
        "target-release-id",
        "target-activation-id",
        "target-generation",
        "target-subject-authority-fingerprint",
        "target-producer-authority-fingerprint",
        "staged-target-canary-state-sha256",
    ):
        english_repair_apply.add_argument(f"--{option}", required=True)
    english_repair_reopen = subparsers.add_parser(
        "english-preserved-review-repair-reopen",
        help="read-only exact repair reopen or uncertain intent lookup",
        allow_abbrev=False,
    )
    english_repair_reopen.add_argument("--repair-receipt-sha256")
    english_repair_rollback = subparsers.add_parser(
        "english-preserved-review-repair-rollback",
        help="restore exact queue, staged gate, batch and writer preimages",
        allow_abbrev=False,
    )
    english_repair_rollback.add_argument(
        "--repair-receipt-sha256", required=True
    )
    english_repair_rollback.add_argument(
        "--batch-archive-sha256", required=True
    )
    english_repair_reopen_rollback = subparsers.add_parser(
        "english-preserved-review-repair-reopen-rollback",
        help="read-only reopen of the exact repair rollback",
        allow_abbrev=False,
    )
    english_repair_reopen_rollback.add_argument(
        "--repair-receipt-sha256", required=True
    )
    english_repair_reopen_rollback.add_argument(
        "--rollback-receipt-sha256", required=True
    )
    english_repair_reopen_rollback.add_argument(
        "--batch-rollback-receipt-sha256", required=True
    )
    finalize_subject_batch = subparsers.add_parser(
        "finalize-subject-batch-recovery",
        help="materialize the successor task and cross-activation supersede proof",
        allow_abbrev=False,
    )
    finalize_subject_batch.add_argument(
        "--recovery-receipt", type=Path, required=True
    )
    finalize_subject_batch.add_argument(
        "--expected-target-release-id", required=True
    )
    stage_recovery_activation = subparsers.add_parser(
        "stage-subject-batch-recovery-activation",
        help="create the target activation with its consumer disabled",
        allow_abbrev=False,
    )
    stage_recovery_activation.add_argument(
        "--recovery-receipt", type=Path, required=True
    )
    stage_recovery_activation.add_argument("--activated-at", required=True)
    stage_recovery_activation.add_argument("--expected-activation-id")
    stage_recovery_activation.add_argument(
        "--expected-producer-authority-fingerprint"
    )
    arm_recovery = subparsers.add_parser(
        "arm-finalized-subject-batch-recovery",
        help="arm only after the successor queue and supersede proof reopen",
        allow_abbrev=False,
    )
    arm_recovery.add_argument(
        "--recovery-receipt", type=Path, required=True
    )
    arm_recovery.add_argument("--expected-target-release-id", required=True)
    rollback_subject_batch = subparsers.add_parser(
        "rollback-subject-batch-recovery",
        help="restore the exact mutable preimages from a v2 recovery receipt",
        allow_abbrev=False,
    )
    rollback_subject_batch.add_argument(
        "--recovery-receipt", type=Path, required=True
    )
    reopen_subject_batch_rollback = subparsers.add_parser(
        "reopen-subject-batch-recovery-rollback",
        help="read-only reopen of the English recovery rollback postimage",
        allow_abbrev=False,
    )
    reopen_subject_batch_rollback.add_argument(
        "--recovery-receipt", type=Path, required=True
    )
    reopen_subject_batch_rollback.add_argument(
        "--rollback-receipt", type=Path, required=True
    )
    reopen_subject_batch_rollback.add_argument(
        "--expected-deployment-canary-state-sha256"
    )
    reopen_subject_batch_rollback.add_argument(
        "--expected-finalization-rollback-proof-count",
        type=int,
        choices=(0, 1),
        default=1,
    )
    subparsers.add_parser("drain", help="stop subject claims and wait active zero")
    quarantine_stale_claim = subparsers.add_parser(
        "quarantine-stale-claim",
        help="preview or quarantine one dead-owner stale claim while drained",
        allow_abbrev=False,
    )
    quarantine_stale_claim.add_argument("--unit-sha256", required=True)
    quarantine_stale_claim.add_argument(
        "--apply",
        action="store_true",
        help="publish the receipt and fence the lease; default is read-only preview",
    )
    subparsers.add_parser(
        "resume", help="clear a drained subject after external authority checks"
    )
    activate_canary = subparsers.add_parser(
        "activate-canary",
        help="arm one subject's post-activation production canary",
        allow_abbrev=False,
    )
    activate_canary.add_argument("--activated-at", required=True)
    activate_canary.add_argument("--expected-activation-id")
    activate_canary.add_argument("--expected-producer-authority-fingerprint")
    subparsers.add_parser(
        "resume-canary",
        help="explicitly resume one failed-drained subject canary",
    )
    subparsers.add_parser(
        "pause-canary",
        help="pause only this subject consumer while producer queue stays live",
    )
    deactivate_canary = subparsers.add_parser(
        "deactivate-canary",
        help="rollback an activation while preserving queue and receipts",
        allow_abbrev=False,
    )
    deactivate_canary.add_argument("--expected-release-id", required=True)
    subparsers.add_parser(
        "audit", help="scan eligibility without submitting or clearing drain"
    )
    subparsers.add_parser(
        "sol-status", help="read the selected subject's isolated Sol state"
    )
    sol_authorize = subparsers.add_parser(
        "sol-authorize",
        help="validate and stage an explicitly authorized subject Sol batch",
        allow_abbrev=False,
    )
    sol_authorize.add_argument("--sol-batch-id", required=True)
    sol_authorize.add_argument("--authorization-receipt-sha256", required=True)
    sol_begin = subparsers.add_parser(
        "sol-begin",
        help="claim the global FIFO Sol lease for subject review",
        allow_abbrev=False,
    )
    sol_begin.add_argument("--batch-id", required=True)
    sol_begin.add_argument("--owner-id")
    sol_review = subparsers.add_parser(
        "sol-review",
        help="consume an isolated writer-adapter Sol review receipt by hash",
        allow_abbrev=False,
    )
    sol_review.add_argument("--receipt-sha256", required=True)
    sol_finish = subparsers.add_parser(
        "sol-finish",
        help="consume a deterministic isolated writer apply receipt by hash",
        allow_abbrev=False,
    )
    sol_finish.add_argument("--writer-apply-receipt-sha256", required=True)
    sol_exclusion_prepare = subparsers.add_parser(
        "sol-prepare-exclusion",
        help="publish an unsigned exact exclusion authority request",
        allow_abbrev=False,
    )
    sol_exclusion_prepare.add_argument("--batch-id", required=True)
    sol_exclusion_prepare.add_argument("--capture-id", required=True)
    sol_exclusion_prepare.add_argument("--unit-sha256", required=True)
    sol_exclude = subparsers.add_parser(
        "sol-exclude",
        help="consume an independent user exclusion authorization by hash",
        allow_abbrev=False,
    )
    sol_exclude.add_argument("--authorization-receipt-sha256", required=True)
    sol_freeze = subparsers.add_parser(
        "sol-freeze-luna-batch",
        help="freeze subject Luna batch membership without authorizing Sol",
        allow_abbrev=False,
    )
    sol_freeze.add_argument("--batch-id", required=True)
    sol_quality = subparsers.add_parser(
        "sol-record-quality",
        help="record one HMAC subject quality receipt",
        allow_abbrev=False,
    )
    sol_quality.add_argument("--receipt", type=Path, required=True)
    sol_generation = subparsers.add_parser(
        "sol-ack-generation",
        help="acknowledge a signed post-commit subject MCP generation authority",
        allow_abbrev=False,
    )
    sol_generation.add_argument("--authority-ack-sha256", required=True)
    subparsers.add_parser(
        "sol-global-status", help="read the global FIFO writer lease"
    )
    legacy_stage = subparsers.add_parser(
        "english-legacy-sol-stage",
        help="stage one exact EN-P0-006 parent Sol batch",
        allow_abbrev=False,
    )
    legacy_stage.add_argument("--batch", type=Path, required=True)
    legacy_begin = subparsers.add_parser(
        "english-legacy-sol-begin",
        help="claim one global lease for the complete EN-P0-006 parent batch",
        allow_abbrev=False,
    )
    legacy_begin.add_argument("--batch-id", required=True)
    legacy_begin.add_argument("--owner-id")
    legacy_review = subparsers.add_parser(
        "english-legacy-sol-review-item",
        help="consume one isolated EN-P0-006 item review receipt",
        allow_abbrev=False,
    )
    legacy_review.add_argument("--receipt-sha256", required=True)
    legacy_finish = subparsers.add_parser(
        "english-legacy-sol-finish-item",
        help="consume one isolated EN-P0-006 item apply receipt",
        allow_abbrev=False,
    )
    legacy_finish.add_argument("--receipt-sha256", required=True)
    legacy_fail = subparsers.add_parser(
        "english-legacy-sol-fail-item",
        help="consume one stopped-writer failure receipt and safe-pause",
        allow_abbrev=False,
    )
    legacy_fail.add_argument("--receipt-sha256", required=True)
    legacy_recovery_begin = subparsers.add_parser(
        "english-legacy-sol-begin-recovery",
        help="claim a new fence at the failed EN-P0-006 ordinal",
        allow_abbrev=False,
    )
    legacy_recovery_begin.add_argument("--batch-id", required=True)
    legacy_recovery_begin.add_argument("--owner-id")
    legacy_recovery = subparsers.add_parser(
        "english-legacy-sol-record-recovery",
        help="consume one isolated EN-P0-006 recovery receipt",
        allow_abbrev=False,
    )
    legacy_recovery.add_argument("--receipt-sha256", required=True)
    legacy_status = subparsers.add_parser(
        "english-legacy-sol-status",
        help="read one EN-P0-006 parent batch state and checkpoint pointer",
        allow_abbrev=False,
    )
    legacy_status.add_argument("--batch-id", required=True)

    dispatch = subparsers.add_parser(
        "dispatch", help="low-level frozen task manifest bridge", allow_abbrev=False
    )
    dispatch.add_argument("--runtime-root", type=Path, required=True)
    dispatch.add_argument(
        "--tasks", required=True, help="JSON array/object, JSONL file, or -"
    )
    dispatch.add_argument(
        "--runner-command-json",
        required=True,
        type=_runner_command,
        help='JSON argv array; dispatcher appends "--stage <stage>"',
    )
    dispatch.add_argument(
        "--stage-timeout-seconds",
        type=float,
        required=True,
        help=(
            "deprecated compatibility alias for a soft runtime warning; "
            "never cancels a stage"
        ),
    )
    return parser


def _require_production_args(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    if args.config is None or args.subject is None:
        raise DispatchError("config_and_subject_required")
    config = load_config(args.config)
    validate_fixed_model_contract(config)
    return config, str(args.subject)


def _stage_runtime_contract(
    config: Mapping[str, Any], subject: str
) -> dict[str, float | int]:
    profile_name = {
        "math": "math_deep_v2",
        "cs408": "cs408_deep_v2",
        "english": "english_two_pass_v1",
    }.get(subject)
    profile = config.get(profile_name) if profile_name is not None else None
    if not isinstance(profile, Mapping):
        raise DispatchError("subject_stage_runtime_contract_missing")
    default_warning = 3600.0 if subject == "math" else 1800.0
    contract: dict[str, float | int] = {
        "soft_runtime_warning_seconds": float(
            profile.get(
                "soft_runtime_warning_seconds",
                profile.get("stage_timeout_seconds", default_warning),
            )
        ),
        "stall_timeout_seconds": float(
            profile.get("stall_timeout_seconds", 1800)
        ),
        "stall_probe_interval_seconds": float(
            profile.get("stall_probe_interval_seconds", 60)
        ),
        "stall_probe_required_consecutive_failures": int(
            profile.get("stall_probe_required_consecutive_failures", 2)
        ),
    }
    if (
        any(float(contract[key]) <= 0 for key in (
            "soft_runtime_warning_seconds",
            "stall_timeout_seconds",
            "stall_probe_interval_seconds",
        ))
        or contract["stall_probe_required_consecutive_failures"] < 2
    ):
        raise DispatchError("subject_stage_runtime_contract_invalid")
    return contract


def _stage_timeout(config: Mapping[str, Any], subject: str) -> float:
    """Historical utility compatibility: return the soft warning threshold.

    This helper remains importable by the separately frozen replay utilities,
    but it no longer represents or drives a production cancellation deadline.
    Production dispatch consumes the complete progress-aware runtime contract
    above.
    """

    return float(
        _stage_runtime_contract(config, subject)[
            "soft_runtime_warning_seconds"
        ]
    )


def _poll_interval(config: Mapping[str, Any], subject: str) -> float:
    worker = config["worker"]
    raw = (
        worker.get("english_poll_interval_seconds", MAX_DISCOVERY_LATENCY_SECONDS)
        if subject == "english"
        else worker.get("poll_interval_seconds", MAX_DISCOVERY_LATENCY_SECONDS)
    )
    if isinstance(raw, bool):
        raise DispatchError("discovery_poll_interval_invalid")
    try:
        configured = float(raw)
    except (TypeError, ValueError) as exc:
        raise DispatchError("discovery_poll_interval_invalid") from exc
    if not math.isfinite(configured) or configured <= 0:
        raise DispatchError("discovery_poll_interval_invalid")
    # This is a latency ceiling, not a worker-count semaphore.  Every scan
    # still submits every newly frozen independent unit without a cap.
    return min(configured, MAX_DISCOVERY_LATENCY_SECONDS)


def _production_canary_enabled(config: Mapping[str, Any]) -> bool:
    dispatch = config.get("dispatch")
    canary = (
        dispatch.get("production_canary")
        if isinstance(dispatch, Mapping)
        else None
    )
    if canary is None:
        return False
    if (
        not isinstance(canary, Mapping)
        or set(canary)
        != {
            "enabled",
            "status",
            "admission",
            "keep_backlog_drained",
            "post_activation_only",
            "initial_canary_inflight_limit",
            "continuous_concurrency_limit",
        }
        or canary.get("enabled") is not True
        or canary.get("status") != "production_canary_active"
        or canary.get("admission")
        != "first_post_activation_producer_capture"
        or canary.get("keep_backlog_drained") is not True
        or canary.get("post_activation_only") is not True
        or canary.get("initial_canary_inflight_limit") != 1
        or isinstance(canary.get("continuous_concurrency_limit"), bool)
        or not isinstance(canary.get("continuous_concurrency_limit"), int)
        or not 1 <= int(canary["continuous_concurrency_limit"]) <= 64
    ):
        raise DispatchError("production_canary_config_invalid")
    return True


def _write_subject_projection(
    config: Mapping[str, Any],
    subject: str,
    *,
    study_date: str,
    daemon_status: str,
    eligible_count: int,
    submitted_count: int,
    decisions: Sequence[Mapping[str, Any]],
    lease_status: Mapping[str, Any],
    error_code: str | None = None,
) -> None:
    update_subject_and_main_projection(
        config,
        subject,
        study_date=study_date,
        daemon_status=daemon_status,
        eligible_count=eligible_count,
        submitted_count=submitted_count,
        decisions=decisions,
        lease_status=lease_status,
        error_code=error_code,
    )


def _write_subject_projections(
    config: Mapping[str, Any],
    subject: str,
    *,
    daemon_status: str,
    decisions: Sequence[Mapping[str, Any]],
    lease_status: Mapping[str, Any],
    error_code: str | None = None,
) -> None:
    today = current_date(str(config.get("timezone") or "UTC"))
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for decision in decisions:
        raw_date = decision.get("study_date")
        if isinstance(raw_date, str):
            grouped.setdefault(raw_date, []).append(decision)
    grouped.setdefault(today, [])
    ordered_dates = sorted(date for date in grouped if date != today) + [today]
    for projection_date in ordered_dates:
        rows = grouped[projection_date]
        eligible_count = sum(row.get("eligible") is True for row in rows)
        submitted_count = sum(
            row.get("eligible") is True
            and row.get("model_enqueue_allowed") is not False
            for row in rows
        )
        _write_subject_projection(
            config,
            subject,
            study_date=projection_date,
            daemon_status=daemon_status,
            eligible_count=eligible_count,
            submitted_count=submitted_count,
            decisions=rows,
            lease_status=lease_status,
            error_code=error_code,
        )


def _write_subject_projections_resilient(
    config: Mapping[str, Any],
    subject: str,
    *,
    daemon_status: str,
    decisions: Sequence[Mapping[str, Any]],
    lease_status: Mapping[str, Any],
    error_code: str | None = None,
    attempts: int = 2,
    retry_delay_seconds: float = 0.05,
) -> bool:
    """Keep a daemon alive across bounded projection-lock contention.

    The projection writer already waits up to three seconds for its advisory
    lock.  A second bounded attempt covers a holder that releases just beyond
    that window.  Persistent contention is observable on stderr and the next
    heartbeat retries; all other projection errors remain fail-closed.
    """

    for attempt in range(1, attempts + 1):
        try:
            _write_subject_projections(
                config,
                subject,
                daemon_status=daemon_status,
                decisions=decisions,
                lease_status=lease_status,
                error_code=error_code,
            )
            return True
        except DashboardProjectionError as exc:
            if str(exc) != "dashboard_projection_lock_busy":
                raise
            if attempt < attempts:
                time.sleep(retry_delay_seconds)
                continue
            sys.stderr.write(
                json.dumps(
                    {
                        "event": "dashboard_projection_lock_busy_skipped",
                        "subject": subject,
                        "attempts": attempts,
                        "daemon_status": daemon_status,
                        "formal_write_count": 0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            sys.stderr.flush()
            return False
    return False


class ProductionDispatchRuntime:
    def __init__(
        self,
        config: Mapping[str, Any],
        subject: str,
        config_path: Path,
        *,
        scan_worker_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> None:
        self.config = dict(config)
        self.subject = subject
        self.config_path = config_path.resolve()
        self.runtime_root = Path(str(config["runtime_root"]))
        self.production_canary = _production_canary_enabled(config)
        self.continuous_concurrency_limit = (
            int(config["dispatch"]["production_canary"][
                "continuous_concurrency_limit"
            ])
            if self.production_canary
            else 1
        )
        self.subject_sol = SubjectSolRuntimeStore(self.runtime_root)
        self.processing_host: ProcessingPluginHost | None = None
        processing_plugin = config.get("processing_plugin")
        if isinstance(processing_plugin, Mapping):
            candidate_release_id, _ = release_identity(config)
            raw_subject_roots = config.get("subject_repo_roots")
            subject_roots = (
                {
                    name: str(root)
                    for name, root in raw_subject_roots.items()
                    if name in ("math", "cs408", "english")
                    and isinstance(root, str)
                    and Path(root).is_dir()
                }
                if isinstance(raw_subject_roots, Mapping)
                else {
                    name: config["adapters"][name]["repo_root"]
                    for name in ("math", "cs408", "english")
                    if isinstance(config.get("adapters"), Mapping)
                    and isinstance(config["adapters"].get(name), Mapping)
                    and isinstance(
                        config["adapters"][name].get("repo_root"), str
                    )
                    and Path(config["adapters"][name]["repo_root"]).is_dir()
                }
            )
            try:
                self.processing_host = ProcessingPluginHost(
                    processing_plugin,
                    runtime_root=self.runtime_root,
                    candidate_release_id=candidate_release_id,
                    subject_roots=subject_roots,
                    require_authority_snapshot=True,
                )
            except ProcessingPluginError as exc:
                raise DispatchError(exc.code) from exc
        self._frozen_batch_authority: dict[str, Any] | None = None
        self._direct_controlled_candidates: dict[str, tuple[Any, str]] = {}
        self._projection_fail_closed = False
        # Tests and embedding hosts may supply a producer-backed scanner
        # implementation.  The production default remains the canonical
        # Worker; the scan still runs through scan_eligible_candidates and all
        # of its fixed-model, evidence, producer-authority and freeze gates.
        self._scan_worker_factory = scan_worker_factory
        stage_runtime = _stage_runtime_contract(config, subject)
        self.dispatcher = ConcurrentDispatcher(
            self.runtime_root,
            self._runner_factory,
            soft_runtime_warning_seconds=float(
                stage_runtime["soft_runtime_warning_seconds"]
            ),
            stall_timeout_seconds=float(
                stage_runtime["stall_timeout_seconds"]
            ),
            stall_probe_interval_seconds=float(
                stage_runtime["stall_probe_interval_seconds"]
            ),
            stall_probe_required_consecutive_failures=int(
                stage_runtime[
                    "stall_probe_required_consecutive_failures"
                ]
            ),
            production_canary=self.production_canary,
        )

    def activate_production_canary(
        self,
        *,
        activated_at: str,
        expected_activation_id: str | None = None,
        expected_producer_authority_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if not self.production_canary:
            raise DispatchError("production_canary_not_configured")
        release_id = Worker(self.config).release_id
        authority = producer_authority_binding(
            self.config, self.subject, release_id
        )
        if (
            expected_producer_authority_fingerprint is not None
            and authority["authority_fingerprint"]
            != expected_producer_authority_fingerprint
        ):
            raise DispatchError("producer_authority_fingerprint_mismatch")
        producer_recorded_after = None
        if self.subject == "math":
            previous_canary = (
                self.dispatcher.lease_store.production_canary_status_read_only(
                    self.subject
                )
            )
            if previous_canary is not None:
                high_watermark = previous_canary.get(
                    "producer_high_watermark"
                )
                producer_recorded_after = (
                    high_watermark.get("recorded_at")
                    if isinstance(high_watermark, Mapping)
                    else None
                )
                if not isinstance(producer_recorded_after, str):
                    raise DispatchError(
                        "production_canary_high_watermark_invalid"
                    )
        state = self.dispatcher.lease_store.activate_production_canary(
            self.subject,
            release_id=release_id,
            producer_authority=authority,
            activated_at=activated_at,
            continuous_concurrency_limit=(
                self.continuous_concurrency_limit
            ),
        )
        if (
            expected_activation_id is not None
            and state.get("activation_id") != expected_activation_id
        ):
            # The activation is durable, but the caller's pre-bound
            # transaction is invalid.  Close the consumer before returning.
            self.dispatcher.lease_store.deactivate_production_canary(
                self.subject, expected_release_id=release_id
            )
            raise DispatchError("production_canary_activation_id_mismatch")
        scan_kwargs: dict[str, Any] = {
            "publish_evidence_readiness": False,
        }
        if producer_recorded_after is not None:
            scan_kwargs["producer_recorded_after"] = producer_recorded_after
        scan_worker_factory = getattr(self, "_scan_worker_factory", None)
        if scan_worker_factory is not None:
            scan_kwargs["worker_factory"] = scan_worker_factory
        frozen, _decisions = scan_eligible_candidates(
            self.config,
            self.subject,
            **scan_kwargs,
        )
        for unit in frozen:
            inspection = (
                self.dispatcher.lease_store.inspect_production_canary_task_read_only(
                    self.subject, unit.task
                )
            )
            classification = inspection["classification"]
            if classification == "pre_activation_frozen":
                self.dispatcher.lease_store.record_production_canary_pre_activation_exclusion(
                    unit.task
                )
                reconciled = self.dispatcher.lease_store.inspect_production_canary_task_read_only(
                    self.subject, unit.task
                )
                if reconciled.get("persisted") is not True:
                    raise DispatchError(
                        "production_canary_activation_classification_invalid"
                    )
            elif classification != "post_activation_unmaterialized":
                raise DispatchError(
                    "production_canary_activation_classification_invalid"
                )
        refreshed = self.dispatcher.lease_store.production_canary_status(
            self.subject
        )
        if refreshed is None:
            raise DispatchError("production_canary_activation_state_missing")
        return refreshed

    def resume_production_canary(self) -> dict[str, Any]:
        if not self.production_canary:
            raise DispatchError("production_canary_not_configured")
        state = self.dispatcher.lease_store.production_canary_status(
            self.subject
        )
        if state is None:
            raise DispatchError("production_canary_not_active")
        if self.subject == "english" and state.get("state") == "failed_drained":
            raise DispatchError(
                "generation_aware_subject_recovery_required"
            )
        if state.get("state") == "failed_drained":
            batch = self.subject_sol.read_subject_batch(self.subject)
            writer = self.subject_sol.read_subject(self.subject)["writer_state"]
            if (
                isinstance(batch, Mapping)
                and batch.get("all_terminal") is True
                and batch.get("sol_ready") is False
                and writer.get("batch_id") == batch.get("batch_id")
            ):
                acceptance_sha256 = state.get("last_terminal_receipt_sha256")
                if not isinstance(acceptance_sha256, str):
                    raise DispatchError(
                        "production_canary_failure_resume_acceptance_missing"
                    )
                self.subject_sol.rollover_background_luna_batch(
                    self.subject,
                    mode="explicit_failure_resume",
                    resume_acceptance_sha256=acceptance_sha256,
                )
        return self.dispatcher.lease_store.resume_production_canary(
            self.subject
        )

    def _current_subject_authority(self) -> dict[str, Any]:
        if self.processing_host is None:
            raise DispatchError("subject_authority_snapshot_host_required")
        try:
            return self.processing_host.subject_authority_snapshot(
                self.subject
            )
        except ProcessingPluginError as exc:
            raise DispatchError(exc.code) from exc

    def canary_readiness(self) -> dict[str, Any]:
        """Read current authority and return the bounded tri-state contract."""

        authority = self._current_subject_authority()
        generation = str(authority["generation"])
        fingerprint = str(authority["authority_fingerprint"])
        subject_readiness = self.subject_sol.canary_readiness(
            self.subject,
            next_generation=generation,
            next_authority_fingerprint=fingerprint,
        )
        evidence: Mapping[str, Any] | None = None
        recovered_transaction: Mapping[str, Any] | None = None
        if self.subject == "english":
            try:
                evidence = (
                    self.dispatcher.lease_store.production_canary_recovery_evidence(
                        self.subject
                    )
                )
            except DispatchError as exc:
                if subject_readiness["readiness"] == "recoverable_terminal_batch":
                    subject_readiness = {
                        **subject_readiness,
                        "readiness": "invalid",
                        "reason": exc.code,
                    }
            if subject_readiness["reason"] == "recovered_terminal_batch_archived":
                recovered_transaction = (
                    self.subject_sol.read_background_rollover_recovery(
                        self.subject
                    )
                )
                if recovered_transaction is not None:
                    recovered_receipt = recovered_transaction["receipt"]
                    evidence = {
                        "original_preclaim_failure_receipt_sha256": (
                            recovered_receipt[
                                "original_preclaim_failure_receipt_sha256"
                            ]
                        ),
                        "original_preclaim_failure_receipt_path": (
                            recovered_receipt[
                                "original_preclaim_failure_receipt_path"
                            ]
                        ),
                        "preserved_queue_entry_sha256": recovered_receipt[
                            "preserved_queue_entry_sha256"
                        ],
                        "subsequent_attempt_receipt_sha256s": (
                            recovered_receipt[
                                "subsequent_attempt_receipt_sha256s"
                            ]
                        ),
                        "preserved_task": recovered_receipt[
                            "preserved_task"
                        ],
                    }
        return {
            "schema_version": "study-intake-canary-readiness-v1",
            "subject": self.subject,
            "readiness": subject_readiness["readiness"],
            "reason": subject_readiness["reason"],
            "authority_generation": generation,
            "authority_fingerprint": fingerprint,
            "batch_id": subject_readiness.get("batch_id"),
            "batch_sha256": subject_readiness.get("batch_sha256"),
            "writer_revision": subject_readiness.get("writer_revision"),
            "writer_batch_id": subject_readiness.get("writer_batch_id"),
            "source_generation": subject_readiness.get("source_generation"),
            "mode": subject_readiness.get("mode"),
            "authorization_descriptor_sha256": subject_readiness.get(
                "authorization_descriptor_sha256"
            ),
            "terminal_receipt_sha256": subject_readiness.get(
                "terminal_receipt_sha256"
            ),
            "writer_preimage_sha256": subject_readiness.get(
                "writer_preimage_sha256"
            ),
            "batch_pointer_sha256": subject_readiness.get(
                "batch_pointer_sha256"
            ),
            "snapshot_sha256": subject_readiness.get("snapshot_sha256"),
            "source_authority_fingerprint": subject_readiness.get(
                "source_authority_fingerprint"
            ),
            "original_preclaim_failure_receipt_sha256": (
                evidence.get("original_preclaim_failure_receipt_sha256")
                if evidence is not None
                else None
            ),
            "original_preclaim_failure_receipt_path": (
                evidence.get("original_preclaim_failure_receipt_path")
                if evidence is not None
                else None
            ),
            "preserved_queue_entry_sha256": (
                evidence.get("preserved_queue_entry_sha256")
                if evidence is not None
                else None
            ),
            "subsequent_attempt_receipt_sha256s": (
                list(evidence["subsequent_attempt_receipt_sha256s"])
                if evidence is not None
                else []
            ),
            "preserved_task": (
                dict(evidence["preserved_task"])
                if evidence is not None
                else None
            ),
            "recovery_receipt_sha256": (
                recovered_transaction.get("recovery_receipt_sha256")
                if recovered_transaction is not None
                else None
            ),
            "recovery_receipt_path": (
                recovered_transaction.get("recovery_receipt_path")
                if recovered_transaction is not None
                else None
            ),
            "rollback_token": (
                recovered_transaction["receipt"].get("rollback_token")
                if recovered_transaction is not None
                else None
            ),
            "cs408_terminal_retirement_receipt_sha256": (
                subject_readiness.get("retirement_receipt_sha256")
            ),
            "cs408_terminal_retirement_receipt_path": (
                subject_readiness.get("retirement_receipt_path")
            ),
            "cs408_terminal_retirement_rollback_token": (
                subject_readiness.get("retirement_rollback_token")
            ),
            "writer_postimage_sha256": subject_readiness.get(
                "writer_postimage_sha256"
            ),
            "postimage_verification_sha256": subject_readiness.get(
                "postimage_verification_sha256"
            ),
            "read_only": True,
            "authority_snapshot_count": 1,
            "mcp_tool_call_count": 1,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def _math_migration_repo_root(self) -> Path:
        adapters = self.config.get("adapters")
        math = adapters.get("math") if isinstance(adapters, Mapping) else None
        raw = math.get("repo_root") if isinstance(math, Mapping) else None
        if not isinstance(raw, str) or not Path(raw).is_absolute():
            raise DispatchError("math_migration_repo_root_invalid")
        return Path(raw).resolve()

    def preview_math_pending_queue_migration(self) -> dict[str, Any]:
        if self.subject != "math" or not self.production_canary:
            raise DispatchError("math_migration_math_only")
        try:
            return preview_exact_math_pending_queue_migration(
                runtime_root=Path(str(self.config["runtime_root"])),
                math_repo_root=self._math_migration_repo_root(),
                target_release_id=Worker(self.config).release_id,
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc

    def prepare_math_pending_queue_migration(self) -> dict[str, Any]:
        if self.subject != "math" or not self.production_canary:
            raise DispatchError("math_migration_math_only")
        try:
            return prepare_exact_math_pending_queue_migration(
                runtime_root=Path(str(self.config["runtime_root"])),
                math_repo_root=self._math_migration_repo_root(),
                target_release_id=Worker(self.config).release_id,
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc

    def apply_math_pending_queue_migration(
        self,
        *,
        migration_intent_sha256: str,
        expected_target_activation_id: str,
        expected_target_authority_generation: str,
        expected_target_subject_authority_fingerprint: str,
        expected_target_producer_authority_fingerprint: str,
    ) -> dict[str, Any]:
        """Materialize exactly four attempt-1 tasks while Math is paused."""

        if self.subject != "math" or not self.production_canary:
            raise DispatchError("math_migration_math_only")
        release_id = Worker(self.config).release_id
        state = self.dispatcher.lease_store.production_canary_status_read_only(
            "math", expected_release_id=release_id
        )
        authority = self._current_subject_authority()
        if (
            state is None
            or state.get("activation_id") != expected_target_activation_id
            or state.get("producer_authority_fingerprint")
            != expected_target_producer_authority_fingerprint
            or state.get("luna_consumer_enabled") is not False
            or state.get("active_task_count") != 0
            or state.get("formal_write_count") != 0
            or state.get("sol_enabled") is not False
            or authority.get("generation")
            != expected_target_authority_generation
            or authority.get("authority_fingerprint")
            != expected_target_subject_authority_fingerprint
            or authority.get("model_call_count") != 0
            or authority.get("formal_write_count") != 0
        ):
            raise DispatchError("math_migration_target_authority_mismatch")
        runtime_root = Path(str(self.config["runtime_root"])).resolve()
        staged: Mapping[str, Any] | None = None
        materialized: list[dict[str, Any]] = []
        try:
            staged = stage_exact_math_pending_queue_migration(
                runtime_root=runtime_root,
                migration_intent_sha256=migration_intent_sha256,
                state=state,
                authority_snapshot=authority,
            )
            descriptor_path = Path(
                str(staged["migration_descriptor_path"])
            )
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
            frozen, _decisions = scan_eligible_candidates(
                self.config,
                "math",
                publish_evidence_readiness=False,
            )
            tasks = []
            for unit in frozen:
                binding = unit.task.frozen_payload.get(
                    "math_exact_smoke_binding"
                )
                if (
                    isinstance(binding, Mapping)
                    and binding.get("migration_descriptor_sha256")
                    == staged["migration_descriptor_sha256"]
                ):
                    tasks.append(unit.task)
            tasks.sort(
                key=lambda task: int(
                    task.frozen_payload["math_exact_smoke_binding"][
                        "sequence"
                    ]
                )
            )
            if [
                task.frozen_payload["math_exact_smoke_binding"]["formal_id"]
                for task in tasks
            ] != list(MIGRATION_ORDER):
                raise DispatchError("math_migration_task_set_invalid")
            materialized = (
                self.dispatcher.lease_store.materialize_exact_math_migration(
                    tasks=tasks, migration_descriptor=descriptor
                )
            )
            commit = publish_exact_math_pending_queue_migration_commit(
                runtime_root=runtime_root,
                migration_descriptor_sha256=str(
                    staged["migration_descriptor_sha256"]
                ),
                queue_rows=materialized,
            )
            return {
                "schema_version": (
                    "study-intake-math-pending-queue-migration-result-v1"
                ),
                "status": "committed",
                "migration_intent_sha256": migration_intent_sha256,
                "migration_descriptor_sha256": staged[
                    "migration_descriptor_sha256"
                ],
                "migration_commit_sha256": commit[
                    "migration_commit_sha256"
                ],
                "target_release_id": release_id,
                "target_activation_id": expected_target_activation_id,
                "target_authority_generation": (
                    expected_target_authority_generation
                ),
                "target_subject_authority_fingerprint": (
                    expected_target_subject_authority_fingerprint
                ),
                "target_producer_authority_fingerprint": (
                    expected_target_producer_authority_fingerprint
                ),
                "queue_count": 4,
                "authority_snapshot_count": 1,
                "mcp_tool_call_count": 1,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
        except Exception:
            if materialized and staged is not None:
                rollback = getattr(
                    self.dispatcher.lease_store,
                    "rollback_exact_math_migration_materialization",
                    None,
                )
                if callable(rollback):
                    rollback(
                        migration_descriptor_sha256=str(
                            staged["migration_descriptor_sha256"]
                        ),
                        queue_rows=materialized,
                    )
            if staged is not None:
                try:
                    rollback_uncommitted_exact_math_pending_queue_migration(
                        runtime_root=runtime_root,
                        migration_descriptor_sha256=str(
                            staged["migration_descriptor_sha256"]
                        ),
                    )
                except MathExactSmokeError:
                    pass
            raise

    def reopen_math_pending_queue_migration(
        self, *, migration_descriptor_sha256: str
    ) -> dict[str, Any]:
        if self.subject != "math" or not self.production_canary:
            raise DispatchError("math_migration_math_only")
        try:
            value = reopen_exact_math_pending_queue_migration_commit(
                runtime_root=Path(str(self.config["runtime_root"])),
                migration_descriptor_sha256=migration_descriptor_sha256,
                required=True,
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc
        assert value is not None
        return value

    def recover_subject_batch(
        self,
        *,
        expected_original_preclaim_failure_receipt_sha256: str,
        expected_next_generation: str,
        expected_next_authority_fingerprint: str,
    ) -> dict[str, Any]:
        """Recover one preserved English queue without arming its consumer."""

        if self.subject != "english":
            raise DispatchError("subject_batch_recovery_english_only")
        first = self._current_subject_authority()
        if (
            first.get("generation") != expected_next_generation
            or first.get("authority_fingerprint")
            != expected_next_authority_fingerprint
        ):
            raise DispatchError("subject_batch_recovery_authority_mismatch")
        readiness = self.subject_sol.canary_readiness(
            self.subject,
            next_generation=expected_next_generation,
            next_authority_fingerprint=(
                expected_next_authority_fingerprint
            ),
        )
        if readiness.get("readiness") not in {
            "recoverable_terminal_batch",
            "ready",
        }:
            raise DispatchError(str(readiness.get("reason") or "invalid"))
        recovered_transaction = (
            self.subject_sol.read_background_rollover_recovery(self.subject)
            if readiness.get("reason")
            == "recovered_terminal_batch_archived"
            else None
        )
        if recovered_transaction is not None:
            existing_receipt = recovered_transaction["receipt"]
            if (
                existing_receipt.get(
                    "original_preclaim_failure_receipt_sha256"
                )
                != expected_original_preclaim_failure_receipt_sha256
                or existing_receipt.get("next_generation")
                != expected_next_generation
                or existing_receipt.get("next_authority_fingerprint")
                != expected_next_authority_fingerprint
            ):
                raise DispatchError(
                    "subject_batch_recovery_idempotency_conflict"
                )
            evidence = {
                "original_preclaim_failure_receipt_sha256": (
                    existing_receipt[
                        "original_preclaim_failure_receipt_sha256"
                    ]
                ),
                "original_preclaim_failure_receipt_path": existing_receipt[
                    "original_preclaim_failure_receipt_path"
                ],
                "preserved_queue_entry_sha256": existing_receipt[
                    "preserved_queue_entry_sha256"
                ],
                "preserved_queue_entry": None,
                "canary_state_before": existing_receipt[
                    "canary_state_before"
                ],
                "subsequent_attempt_receipt_sha256s": existing_receipt[
                    "subsequent_attempt_receipt_sha256s"
                ],
                "preserved_task": existing_receipt["preserved_task"],
            }
        else:
            evidence = (
                self.dispatcher.lease_store.production_canary_recovery_evidence(
                    self.subject,
                    expected_original_preclaim_failure_receipt_sha256=(
                        expected_original_preclaim_failure_receipt_sha256
                    ),
                )
            )
        second = self._current_subject_authority()
        if (
            second.get("generation") != first.get("generation")
            or second.get("authority_fingerprint")
            != first.get("authority_fingerprint")
            or second.get("generation") != expected_next_generation
            or second.get("authority_fingerprint")
            != expected_next_authority_fingerprint
        ):
            raise DispatchError("subject_batch_recovery_authority_drift")
        recovered = (
            {
                "receipt": recovered_transaction["receipt"],
                "receipt_sha256": recovered_transaction[
                    "recovery_receipt_sha256"
                ],
                "receipt_path": recovered_transaction[
                    "recovery_receipt_path"
                ],
                "idempotent": True,
            }
            if recovered_transaction is not None
            else self.subject_sol.rollover_background_luna_batch_v2(
                self.subject,
                next_generation=expected_next_generation,
                next_authority_fingerprint=expected_next_authority_fingerprint,
                original_preclaim_failure_receipt_sha256=evidence[
                    "original_preclaim_failure_receipt_sha256"
                ],
                original_preclaim_failure_receipt_path=evidence[
                    "original_preclaim_failure_receipt_path"
                ],
                preserved_queue_entry=evidence["preserved_queue_entry"],
                canary_state_before=evidence["canary_state_before"],
                subsequent_attempt_receipt_sha256s=evidence[
                    "subsequent_attempt_receipt_sha256s"
                ],
                preserved_task=evidence["preserved_task"],
            )
        )
        receipt = recovered["receipt"]
        queue = (
            evidence["preserved_queue_entry"]
            if evidence["preserved_queue_entry"] is not None
            else receipt["preserved_queue_identity"]
        )
        self.dispatcher.lease_store.verify_production_canary_preserved_queue_postimage(
            self.subject,
            activation_id=str(queue["activation_id"]),
            producer_input_contract_sha256=str(
                queue["producer_input_contract_sha256"]
            ),
            expected_queue_entry_sha256=str(
                evidence["preserved_queue_entry_sha256"]
            ),
        )
        gate_after = self.dispatcher.lease_store.production_canary_status_read_only(
            self.subject
        )
        if gate_after != evidence["canary_state_before"]:
            raise DispatchError("subject_batch_recovery_canary_gate_drift")
        post = self.subject_sol.canary_readiness(
            self.subject,
            next_generation=expected_next_generation,
            next_authority_fingerprint=(
                expected_next_authority_fingerprint
            ),
        )
        if post.get("readiness") != "ready":
            raise DispatchError("subject_batch_recovery_postimage_invalid")
        return {
            "schema_version": "study-intake-subject-batch-recovery-result-v1",
            "subject": self.subject,
            "status": "recovered",
            "source_generation": receipt["source_generation"],
            "next_generation": receipt["next_generation"],
            "next_authority_fingerprint": receipt[
                "next_authority_fingerprint"
            ],
            "original_preclaim_failure_receipt_sha256": receipt[
                "original_preclaim_failure_receipt_sha256"
            ],
            "preserved_queue_entry_sha256": receipt[
                "preserved_queue_entry_sha256"
            ],
            "subsequent_attempt_receipt_sha256s": list(
                receipt["subsequent_attempt_receipt_sha256s"]
            ),
            "preserved_task": dict(receipt["preserved_task"]),
            "recovery_receipt_sha256": recovered["receipt_sha256"],
            "recovery_receipt_path": recovered["receipt_path"],
            "rollback_token": receipt["rollback_token"],
            "writer_state_after_sha256": receipt[
                "writer_state_after_sha256"
            ],
            "consumer_state_unchanged": True,
            "luna_consumer_enabled": False,
            "idempotent": bool(recovered["idempotent"]),
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def retire_cs408_terminal_batch(
        self,
        *,
        expected_batch_sha256: str,
        expected_terminal_receipt_sha256: str,
        expected_writer_preimage_sha256: str,
        expected_batch_pointer_sha256: str,
        expected_snapshot_sha256: str,
        expected_source_generation: str,
        expected_source_authority_fingerprint: str,
        expected_next_generation: str,
        expected_next_authority_fingerprint: str,
    ) -> dict[str, Any]:
        if self.subject != "cs408":
            raise DispatchError("cs408_terminal_retirement_subject_invalid")
        first = self._current_subject_authority()
        if (
            first.get("generation") != expected_next_generation
            or first.get("authority_fingerprint")
            != expected_next_authority_fingerprint
        ):
            raise DispatchError("cs408_terminal_retirement_authority_mismatch")
        second = self._current_subject_authority()
        if (
            second.get("generation") != first.get("generation")
            or second.get("authority_fingerprint")
            != first.get("authority_fingerprint")
            or second.get("generation") != expected_next_generation
            or second.get("authority_fingerprint")
            != expected_next_authority_fingerprint
        ):
            raise DispatchError("cs408_terminal_retirement_authority_drift")
        try:
            return self.subject_sol.retire_authorized_cs408_terminal_batch(
                expected_batch_sha256=expected_batch_sha256,
                expected_terminal_receipt_sha256=(
                    expected_terminal_receipt_sha256
                ),
                expected_writer_preimage_sha256=(
                    expected_writer_preimage_sha256
                ),
                expected_batch_pointer_sha256=expected_batch_pointer_sha256,
                expected_snapshot_sha256=expected_snapshot_sha256,
                expected_source_generation=expected_source_generation,
                expected_source_authority_fingerprint=(
                    expected_source_authority_fingerprint
                ),
                expected_next_generation=expected_next_generation,
                expected_next_authority_fingerprint=(
                    expected_next_authority_fingerprint
                ),
                authority_snapshots=(first, second),
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc

    def reopen_cs408_terminal_batch_retirement(
        self,
        *,
        retirement_receipt: Path | None,
        expected_terminal_receipt_sha256: str,
    ) -> dict[str, Any]:
        if self.subject != "cs408":
            raise DispatchError("cs408_terminal_retirement_subject_invalid")
        try:
            value = (
                self.subject_sol.reopen_authorized_cs408_terminal_batch_retirement(
                    retirement_receipt_path=retirement_receipt
                )
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc
        if value.get("terminal_receipt_sha256") != (
            expected_terminal_receipt_sha256
        ):
            raise DispatchError(
                "cs408_terminal_retirement_authorization_mismatch"
            )
        return {**value, "reopen_read_only": True}

    def rollback_cs408_terminal_batch_retirement(
        self, *, retirement_receipt: Path
    ) -> dict[str, Any]:
        if self.subject != "cs408":
            raise DispatchError("cs408_terminal_retirement_subject_invalid")
        try:
            return self.subject_sol.rollback_authorized_cs408_terminal_batch_retirement(
                retirement_receipt_path=retirement_receipt
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc

    def reopen_cs408_terminal_batch_retirement_rollback(
        self,
        *,
        retirement_receipt: Path,
        rollback_receipt: Path,
    ) -> dict[str, Any]:
        if self.subject != "cs408":
            raise DispatchError("cs408_terminal_retirement_subject_invalid")
        try:
            return self.subject_sol.reopen_authorized_cs408_terminal_batch_retirement_rollback(
                retirement_receipt_path=retirement_receipt,
                rollback_receipt_path=rollback_receipt,
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc

    def preview_english_preserved_review_repair(self) -> dict[str, Any]:
        if self.subject != "english":
            raise DispatchError("english_preserved_review_repair_english_only")
        return self.dispatcher.lease_store.preview_authorized_english_preserved_review_repair()

    def apply_english_preserved_review_repair(
        self,
        *,
        target_release_id: str,
        target_activation_id: str,
        target_generation: str,
        target_subject_authority_fingerprint: str,
        target_producer_authority_fingerprint: str,
        staged_target_canary_state_sha256: str,
    ) -> dict[str, Any]:
        if self.subject != "english":
            raise DispatchError("english_preserved_review_repair_english_only")
        batch_archive: Mapping[str, Any] | None = None
        result: Mapping[str, Any] | None = None
        try:
            batch_archive = (
                self.subject_sol.archive_authorized_english_preserved_review_batch(
                    target_generation=target_generation,
                    target_authority_fingerprint=(
                        target_subject_authority_fingerprint
                    ),
                )
            )
            result = self.dispatcher.lease_store.apply_authorized_english_preserved_review_repair(
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
            reopened = self.dispatcher.lease_store.reopen_authorized_english_preserved_review_repair(
                repair_receipt_sha256=result["repair_receipt_sha256"],
                target_release_id=target_release_id,
                target_activation_id=target_activation_id,
                target_generation=target_generation,
                target_subject_authority_fingerprint=(
                    target_subject_authority_fingerprint
                ),
                target_producer_authority_fingerprint=(
                    target_producer_authority_fingerprint
                ),
            )
            self.subject_sol.reopen_authorized_english_preserved_review_batch(
                target_generation=target_generation,
                target_authority_fingerprint=(
                    target_subject_authority_fingerprint
                ),
            )
            return {**result, "reopen_status": reopened["status"]}
        except (DispatchError, SubjectSolContractError) as exc:
            if batch_archive is not None:
                try:
                    prior_reopen = None
                    if result is not None:
                        prior_reopen = self.dispatcher.lease_store.reopen_authorized_english_preserved_review_repair(
                            repair_receipt_sha256=str(
                                result["repair_receipt_sha256"]
                            )
                        )
                    batch_rollback = self.subject_sol.rollback_authorized_english_preserved_review_batch(
                        archive_sha256=str(batch_archive["archive_sha256"])
                    )
                    if result is not None:
                        self.dispatcher.lease_store.rollback_authorized_english_preserved_review_repair(
                            repair_receipt_sha256=str(
                                result["repair_receipt_sha256"]
                            ),
                            batch_rollback=batch_rollback,
                            prior_reopen=prior_reopen,
                        )
                except SubjectSolContractError as rollback_exc:
                    raise DispatchError(
                        "english_preserved_review_repair_fail_fenced"
                    ) from rollback_exc
                except DispatchError as rollback_exc:
                    raise DispatchError(
                        "english_preserved_review_repair_fail_fenced"
                    ) from rollback_exc
            if isinstance(exc, SubjectSolContractError):
                raise DispatchError(exc.code) from exc
            raise

    def reopen_english_preserved_review_repair(
        self, *, repair_receipt_sha256: str | None
    ) -> dict[str, Any]:
        if self.subject != "english":
            raise DispatchError("english_preserved_review_repair_english_only")
        return self.dispatcher.lease_store.reopen_authorized_english_preserved_review_repair(
            repair_receipt_sha256=repair_receipt_sha256
        )

    def rollback_english_preserved_review_repair(
        self, *, repair_receipt_sha256: str, batch_archive_sha256: str
    ) -> dict[str, Any]:
        if self.subject != "english":
            raise DispatchError("english_preserved_review_repair_english_only")
        prior_reopen = (
            self.dispatcher.lease_store.reopen_authorized_english_preserved_review_repair(
                repair_receipt_sha256=repair_receipt_sha256
            )
        )
        try:
            batch_rollback = self.subject_sol.rollback_authorized_english_preserved_review_batch(
                archive_sha256=batch_archive_sha256
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc
        try:
            result = self.dispatcher.lease_store.rollback_authorized_english_preserved_review_repair(
                repair_receipt_sha256=repair_receipt_sha256,
                batch_rollback=batch_rollback,
                prior_reopen=prior_reopen,
            )
            return {
                **result,
                "batch_rollback_receipt_sha256": batch_rollback[
                    "rollback_receipt_sha256"
                ],
                "batch_rollback_receipt_path": batch_rollback[
                    "rollback_receipt_path"
                ],
            }
        except DispatchError as exc:
            raise DispatchError(
                "english_preserved_review_repair_fail_fenced"
            ) from exc

    def reopen_english_preserved_review_repair_rollback(
        self,
        *,
        repair_receipt_sha256: str,
        rollback_receipt_sha256: str,
        batch_rollback_receipt_sha256: str,
    ) -> dict[str, Any]:
        if self.subject != "english":
            raise DispatchError("english_preserved_review_repair_english_only")
        try:
            batch = self.subject_sol.reopen_authorized_english_preserved_review_batch_rollback(
                rollback_receipt_sha256=batch_rollback_receipt_sha256
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc
        repair = self.dispatcher.lease_store.reopen_authorized_english_preserved_review_repair_rollback(
            repair_receipt_sha256=repair_receipt_sha256,
            rollback_receipt_sha256=rollback_receipt_sha256,
        )
        return {**repair, "batch_rollback_status": batch["status"]}

    def rollback_subject_batch_recovery(
        self, *, recovery_receipt: Path
    ) -> dict[str, Any]:
        """Compensate recovery only while all mutable postimages still bind."""

        path = recovery_receipt.resolve()
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise DispatchError("subject_batch_recovery_receipt_unreadable") from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        receipt = self.subject_sol._read_background_rollover_receipt(
            receipt_sha256
        )
        if path != Path(
            str(
                self.subject_sol.background_rollover_receipt_root
                / "sha256"
                / receipt_sha256[:2]
                / f"{receipt_sha256}.json"
            )
        ):
            raise DispatchError("subject_batch_recovery_receipt_path_invalid")
        queue_identity = receipt["preserved_queue_identity"]
        self.dispatcher.lease_store.verify_production_canary_preserved_queue_postimage(
            self.subject,
            activation_id=str(queue_identity["activation_id"]),
            producer_input_contract_sha256=str(
                queue_identity["producer_input_contract_sha256"]
            ),
            expected_queue_entry_sha256=str(
                receipt["preserved_queue_entry_sha256"]
            ),
        )
        finalization_preflight = self.dispatcher.lease_store.rollback_production_canary_subject_batch_finalization(
            recovery_receipt_sha256=receipt_sha256,
            apply=False,
        )
        finalization_rollback = (
            self.dispatcher.lease_store.rollback_production_canary_subject_batch_finalization(
                recovery_receipt_sha256=receipt_sha256,
                apply=True,
            )
            if finalization_preflight is not None
            else None
        )
        self.dispatcher.lease_store.restore_production_canary_recovery_preimage(
            canary_state_before=receipt["canary_state_before"],
            expected_preserved_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            apply=False,
        )
        rolled_back = self.subject_sol.rollback_background_luna_batch_v2(
            recovery_receipt_path=path
        )
        gate_restore = self.dispatcher.lease_store.restore_production_canary_recovery_preimage(
            canary_state_before=receipt["canary_state_before"],
            expected_preserved_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            apply=True,
        )
        cleanup = self.dispatcher.lease_store.complete_production_canary_subject_batch_recovery_rollback(
            recovery_receipt_sha256=receipt_sha256
        )
        return {
            "schema_version": "study-intake-subject-batch-recovery-rollback-result-v1",
            "subject": self.subject,
            "status": "rolled_back",
            "recovery_receipt_sha256": receipt_sha256,
            "rollback_token": receipt["rollback_token"],
            "rollback_receipt_sha256": rolled_back[
                "rollback_receipt_sha256"
            ],
            "rollback_receipt_path": rolled_back[
                "rollback_receipt_path"
            ],
            "writer_state_restored_sha256": rolled_back[
                "rollback_receipt"
            ]["writer_state_restored_sha256"],
            "batch_pointer_restored_sha256": rolled_back[
                "rollback_receipt"
            ]["batch_pointer_restored_sha256"],
            "canary_state_restored_sha256": gate_restore[
                "canary_state_sha256"
            ],
            "preserved_queue_entry_sha256": receipt[
                "preserved_queue_entry_sha256"
            ],
            "target_queue_withdrawn": cleanup[
                "target_queue_withdrawn"
            ],
            "removed_mutable_pointer_count": cleanup[
                "removed_mutable_pointer_count"
            ],
            "idempotent": bool(rolled_back["idempotent"]),
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def reopen_subject_batch_recovery_rollback(
        self,
        *,
        recovery_receipt: Path,
        rollback_receipt: Path,
        expected_deployment_canary_state_sha256: str | None = None,
        expected_finalization_rollback_proof_count: int = 1,
    ) -> dict[str, Any]:
        """Read-only reopen of the exact English rollback closure."""

        if self.subject != "english":
            raise DispatchError("subject_batch_recovery_english_only")
        try:
            sol = self.subject_sol.reopen_background_luna_batch_v2_rollback(
                recovery_receipt_path=recovery_receipt,
                rollback_receipt_path=rollback_receipt,
            )
            recovery = self.subject_sol._read_background_rollover_receipt(
                str(sol["recovery_receipt_sha256"])
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc

        lease_store = self.dispatcher.lease_store
        recovery_sha256 = str(sol["recovery_receipt_sha256"])
        queue_identity = dict(recovery["preserved_queue_identity"])
        mutable_paths = (
            lease_store.production_canary_recovery_staged_activation_root
            / f"{recovery_sha256}.json",
            lease_store.production_canary_recovery_finalization_root
            / f"{recovery_sha256}.json",
            lease_store.production_canary_recovery_finalization_intent_root
            / f"{recovery_sha256}.json",
            lease_store.production_canary_recovery_finalization_rollback_intent_root
            / f"{recovery_sha256}.json",
        )
        with _ExistingSharedFileLock(lease_store.lock_path):
            state_path = lease_store._production_canary_state_path("english")
            try:
                state_payload = state_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "subject_batch_recovery_rollback_gate_missing"
                ) from exc
            state = lease_store._read_object(state_path)
            if state is None:
                raise DispatchError(
                    "subject_batch_recovery_rollback_gate_missing"
                )
            lease_store._verify_seal_read_only(
                state, purpose="dispatch-production-canary-state"
            )
            expected_state = dict(recovery["canary_state_before"])
            state_sha256 = hashlib.sha256(
                json.dumps(
                    state,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            state_physical_sha256 = hashlib.sha256(state_payload).hexdigest()
            if expected_deployment_canary_state_sha256 is None:
                state_identity_invalid = (
                    state != expected_state
                    or state_sha256
                    != recovery["canary_state_before_sha256"]
                )
            else:
                state_identity_invalid = (
                    len(expected_deployment_canary_state_sha256) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in (
                            expected_deployment_canary_state_sha256
                        )
                    )
                    or state_physical_sha256
                    != expected_deployment_canary_state_sha256
                )
            if (
                state_identity_invalid
                or state.get("luna_consumer_enabled") is not False
                or int(state.get("active_task_count") or 0) != 0
                or state.get("formal_write_count") != 0
                or state.get("sol_enabled") is not False
            ):
                raise DispatchError(
                    "subject_batch_recovery_rollback_gate_drift"
                )

            old_activation_id = str(queue_identity["activation_id"])
            old_contract_sha256 = str(
                queue_identity["producer_input_contract_sha256"]
            )
            preserved_queue_path = (
                lease_store._production_canary_queue_subject_root(
                    "english", old_activation_id
                )
                / f"{old_contract_sha256}.json"
            )
            try:
                preserved_payload = preserved_queue_path.read_bytes()
            except OSError as exc:
                raise DispatchError(
                    "production_canary_preserved_queue_missing"
                ) from exc
            preserved_queue = lease_store._read_object(preserved_queue_path)
            if preserved_queue is None:
                raise DispatchError(
                    "production_canary_preserved_queue_missing"
                )
            lease_store._verify_seal_read_only(
                preserved_queue, purpose="dispatch-production-canary-queue"
            )
            if (
                hashlib.sha256(preserved_payload).hexdigest()
                != recovery["preserved_queue_entry_sha256"]
                or preserved_queue.get("activation_id") != old_activation_id
                or preserved_queue.get("producer_input_contract_sha256")
                != old_contract_sha256
                or preserved_queue.get("queue_status") != "pending"
                or preserved_queue.get("formal_write_count") != 0
            ):
                raise DispatchError(
                    "production_canary_preserved_queue_postimage_mismatch"
                )

            rollback_proofs: list[dict[str, Any]] = []
            for candidate in sorted(
                (
                    lease_store.production_canary_receipt_root
                    / "english"
                ).glob("*/sha256/*/*.json")
            ):
                try:
                    payload = candidate.read_bytes()
                    value = json.loads(payload.decode("utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise DispatchError(
                        "subject_batch_recovery_rollback_proof_unreadable"
                    ) from exc
                if (
                    not isinstance(value, dict)
                    or value.get("schema_version")
                    != "study-intake-subject-batch-recovery-finalization-rollback-v1"
                    or value.get("recovery_receipt_sha256")
                    != recovery_sha256
                ):
                    continue
                digest = hashlib.sha256(payload).hexdigest()
                if (
                    candidate.stem != digest
                    or candidate.parent.name != digest[:2]
                ):
                    raise DispatchError(
                        "subject_batch_recovery_rollback_proof_invalid"
                    )
                lease_store._verify_seal_read_only(
                    value,
                    purpose=(
                        "dispatch-subject-batch-recovery-finalization-rollback"
                    ),
                )
                expected_keys = {
                    "schema_version",
                    "subject",
                    "recovery_receipt_sha256",
                    "target_activation_id",
                    "target_release_id",
                    "replacement_queue_entry_sha256",
                    "replacement_queue_entry_path",
                    "created_at",
                    "formal_write_count",
                    "authority",
                }
                target_queue_path = Path(
                    str(value.get("replacement_queue_entry_path") or "")
                )
                if (
                    set(value) != expected_keys
                    or value.get("subject") != "english"
                    or value.get("target_activation_id") == old_activation_id
                    or value.get("formal_write_count") != 0
                    or target_queue_path.exists()
                    or target_queue_path.is_symlink()
                ):
                    raise DispatchError(
                        "subject_batch_recovery_rollback_proof_invalid"
                    )
                rollback_proofs.append(value)
            if (
                expected_finalization_rollback_proof_count not in {0, 1}
                or len(rollback_proofs)
                != expected_finalization_rollback_proof_count
            ):
                raise DispatchError(
                    "subject_batch_recovery_rollback_proof_missing"
                )
            if any(path.exists() or path.is_symlink() for path in mutable_paths):
                raise DispatchError(
                    "subject_batch_recovery_rollback_pointer_drift"
                )

        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-rollback-reopen-result-v1"
            ),
            "subject": "english",
            "status": "reopened",
            "recovery_receipt_sha256": recovery_sha256,
            "rollback_receipt_sha256": sol["rollback_receipt_sha256"],
            "rollback_receipt_path": sol["rollback_receipt_path"],
            "rollback_token": sol["rollback_token"],
            "writer_state_restored_sha256": sol[
                "writer_state_restored_sha256"
            ],
            "batch_pointer_restored_sha256": sol[
                "batch_pointer_restored_sha256"
            ],
            "canary_state_restored_sha256": recovery[
                "canary_state_before_sha256"
            ],
            "deployment_canary_state_postimage_sha256": (
                state_physical_sha256
            ),
            "finalization_rollback_proof_count": len(rollback_proofs),
            "preserved_queue_entry_sha256": recovery[
                "preserved_queue_entry_sha256"
            ],
            "target_queue_withdrawn": True,
            "mutable_recovery_intents_withdrawn": True,
            "rollback_receipt_reopened": True,
            "rollback_reopen_read_only": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def finalize_subject_batch_recovery(
        self,
        *,
        recovery_receipt: Path,
        expected_target_release_id: str,
    ) -> dict[str, Any]:
        """Rematerialize preserved source with the active successor contract."""

        if self.subject != "english":
            raise DispatchError("subject_batch_recovery_english_only")
        path = recovery_receipt.resolve()
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise DispatchError(
                "subject_batch_recovery_receipt_unreadable"
            ) from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        receipt = self.subject_sol._read_background_rollover_receipt(
            receipt_sha256
        )
        expected_path = (
            self.subject_sol.background_rollover_receipt_root
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        if path != expected_path:
            raise DispatchError(
                "subject_batch_recovery_receipt_path_invalid"
            )
        state = self.dispatcher.lease_store.production_canary_status_read_only(
            self.subject,
            expected_release_id=expected_target_release_id,
        )
        if (
            state is None
            or state.get("state") == "inactive_rolled_back"
            or int(state.get("active_task_count") or 0) != 0
        ):
            raise DispatchError(
                "subject_batch_recovery_target_state_invalid"
            )
        try:
            frozen, _decisions = scan_eligible_candidates(
                self.config,
                self.subject,
                publish_evidence_readiness=False,
            )
        except Exception as exc:
            raise DispatchError("deterministic_producer_drift") from exc
        source_set_sha256 = receipt["preserved_queue_identity"][
            "source_event_set_sha256"
        ]
        matching = []
        for unit in frozen:
            contract = (
                self.dispatcher.lease_store._producer_contract_from_task(
                    unit.task
                )
            )
            if contract.get("source_event_set_sha256") == source_set_sha256:
                matching.append(unit.task)
        if len(matching) != 1:
            raise DispatchError("deterministic_producer_drift")
        finalized = self.dispatcher.lease_store.finalize_production_canary_subject_batch_recovery(
            self.subject,
            recovery_receipt_sha256=receipt_sha256,
            recovery_receipt_path=str(path),
            expected_target_release_id=expected_target_release_id,
            old_queue_identity=receipt["preserved_queue_identity"],
            old_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            replacement_task=matching[0],
        )
        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-finalization-result-v1"
            ),
            "subject": self.subject,
            "status": "finalized",
            "recovery_receipt_sha256": receipt_sha256,
            "supersede_receipt_sha256": finalized[
                "supersede_receipt_sha256"
            ],
            "supersede_receipt_path": finalized[
                "supersede_receipt_path"
            ],
            "old_activation_id": finalized["old_activation_id"],
            "old_queue_entry_sha256": finalized[
                "old_queue_entry_sha256"
            ],
            "target_activation_id": finalized[
                "target_activation_id"
            ],
            "target_release_id": finalized["target_release_id"],
            "replacement_task": finalized["replacement_task"],
            "replacement_queue_entry_sha256": finalized[
                "replacement_queue_entry_sha256"
            ],
            "replacement_queue_entry_path": finalized[
                "replacement_queue_entry_path"
            ],
            "idempotent": bool(finalized["idempotent"]),
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def stage_subject_batch_recovery_activation(
        self,
        *,
        recovery_receipt: Path,
        activated_at: str,
        expected_activation_id: str | None,
        expected_producer_authority_fingerprint: str | None,
    ) -> dict[str, Any]:
        """Create a recovery-only activation that cannot claim work."""

        if self.subject != "english" or not self.production_canary:
            raise DispatchError("subject_batch_recovery_english_only")
        path = recovery_receipt.resolve()
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise DispatchError(
                "subject_batch_recovery_receipt_unreadable"
            ) from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        receipt = self.subject_sol._read_background_rollover_receipt(
            receipt_sha256
        )
        expected_path = (
            self.subject_sol.background_rollover_receipt_root
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        if path != expected_path:
            raise DispatchError(
                "subject_batch_recovery_receipt_path_invalid"
            )
        if receipt.get("subject") != self.subject:
            raise DispatchError("subject_batch_recovery_receipt_invalid")
        release_id = Worker(self.config).release_id
        producer_authority = producer_authority_binding(
            self.config, self.subject, release_id
        )
        if (
            expected_producer_authority_fingerprint is not None
            and producer_authority["authority_fingerprint"]
            != expected_producer_authority_fingerprint
        ):
            raise DispatchError("producer_authority_fingerprint_mismatch")
        state = self.dispatcher.lease_store.activate_production_canary(
            self.subject,
            release_id=release_id,
            producer_authority=producer_authority,
            activated_at=activated_at,
            continuous_concurrency_limit=self.continuous_concurrency_limit,
            staged_recovery_receipt_sha256=receipt_sha256,
            staged_recovery_receipt_path=str(path),
        )
        if (
            expected_activation_id is not None
            and state.get("activation_id") != expected_activation_id
        ):
            raise DispatchError("production_canary_activation_id_mismatch")
        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-staged-activation-result-v1"
            ),
            "subject": self.subject,
            "status": "staged",
            "recovery_receipt_sha256": receipt_sha256,
            "target_release_id": release_id,
            "target_activation_id": state["activation_id"],
            "producer_authority_fingerprint": state[
                "producer_authority_fingerprint"
            ],
            "luna_consumer_enabled": False,
            "state": state["state"],
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def arm_finalized_subject_batch_recovery(
        self,
        *,
        recovery_receipt: Path,
        expected_target_release_id: str,
    ) -> dict[str, Any]:
        """Atomically enable the staged consumer after full proof reopen."""

        path = recovery_receipt.resolve()
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise DispatchError(
                "subject_batch_recovery_receipt_unreadable"
            ) from exc
        receipt_sha256 = hashlib.sha256(payload).hexdigest()
        expected_path = (
            self.subject_sol.background_rollover_receipt_root
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        if path != expected_path:
            raise DispatchError(
                "subject_batch_recovery_receipt_path_invalid"
            )
        armed = self.dispatcher.lease_store.arm_finalized_production_canary_subject_batch_recovery(
            self.subject,
            recovery_receipt_sha256=receipt_sha256,
            expected_target_release_id=expected_target_release_id,
        )
        state = armed["state"]
        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-arm-result-v1"
            ),
            "subject": self.subject,
            "status": "armed",
            "recovery_receipt_sha256": receipt_sha256,
            "target_release_id": expected_target_release_id,
            "target_activation_id": state["activation_id"],
            "luna_consumer_enabled": True,
            "state": state["state"],
            "idempotent": bool(armed["idempotent"]),
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def _runner_factory(self, task, _context):
        contract = task.frozen_payload.get("dispatch_contract")
        if (
            not isinstance(contract, Mapping)
            or not isinstance(contract.get("dispatch_reason"), str)
            or not contract["dispatch_reason"]
        ):
            raise DispatchError("claimed_task_reason_missing")
        direct = self._direct_controlled_candidates.get(task.unit_sha256)
        if direct is not None:
            candidate, reason = direct
            if reason != contract["dispatch_reason"]:
                raise DispatchError("direct_candidate_dispatch_reason_mismatch")
            return CoreCandidateRunner(
                self.config,
                candidate,
                reason,
                self.dispatcher.lease_store,
                expected_batch_authority=self._frozen_batch_authority,
            )
        return CoreCandidateSubprocessRunner(
            self.config_path,
            expected_batch_authority=self._frozen_batch_authority,
            lease_store=self.dispatcher.lease_store,
        )

    def register_controlled_replay_candidate(
        self,
        task: FrozenTask,
        candidate: Any,
        *,
        reason: str,
    ) -> None:
        """Bind one exact in-process candidate before the zero-model prefreeze."""

        checked = self._frozen_task(task)
        payload = checked.frozen_payload
        if (
            self._frozen_batch_authority is not None
            or self.dispatcher.active_count != 0
            or not isinstance(reason, str)
            or not reason
            or any(
                payload.get(field) != getattr(candidate, field, None)
                for field in (
                    "subject",
                    "capture_id",
                    "study_date",
                    "input_fingerprint",
                )
            )
            or payload.get("subject") != self.subject
            or not isinstance(getattr(candidate, "private_context", None), Mapping)
            or not isinstance(payload.get("dispatch_contract"), Mapping)
            or payload["dispatch_contract"].get("dispatch_reason") != reason
        ):
            raise DispatchError("direct_candidate_registration_invalid")
        existing = self._direct_controlled_candidates.get(checked.unit_sha256)
        if existing is not None and existing != (candidate, reason):
            raise DispatchError("direct_candidate_registration_conflict")
        self._direct_controlled_candidates[checked.unit_sha256] = (
            candidate,
            reason,
        )

    @staticmethod
    def _frozen_task(value: Any) -> FrozenTask:
        task = value if isinstance(value, FrozenTask) else getattr(value, "task", None)
        if not isinstance(task, FrozenTask):
            raise DispatchError("subject_batch_frozen_task_invalid")
        return task

    @staticmethod
    def _scan_snapshot(
        subject: str, frozen: Sequence[Any], study_date: str
    ) -> tuple[dict[str, Any], str, str, list[dict[str, Any]]]:
        tasks: list[dict[str, Any]] = []
        high_water_rows: list[dict[str, Any]] = []
        for unit in frozen:
            task = ProductionDispatchRuntime._frozen_task(unit)
            payload = task.frozen_payload
            row = {
                "capture_id": str(payload.get("capture_id") or ""),
                "unit_sha256": task.unit_sha256,
                "input_fingerprint": str(payload.get("input_fingerprint") or ""),
                "study_date": str(payload.get("study_date") or ""),
                "frozen_payload_sha256": task.frozen_payload_sha256,
            }
            if any(not value for value in row.values()):
                raise DispatchError("subject_batch_task_binding_invalid")
            tasks.append(row)
            high_water_rows.append(
                {
                    "capture_id": row["capture_id"],
                    "input_fingerprint": row["input_fingerprint"],
                    "recorded_at": str(payload.get("recorded_at") or ""),
                    "study_date": row["study_date"],
                }
            )
        tasks.sort(key=lambda row: (row["capture_id"], row["unit_sha256"]))
        high_water_rows.sort(
            key=lambda row: (
                row["study_date"],
                row["recorded_at"],
                row["capture_id"],
                row["input_fingerprint"],
            )
        )
        high_watermark = hashlib.sha256(
            json.dumps(
                high_water_rows,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        snapshot = {
            "schema_version": "subject_luna_scan_snapshot_v1",
            "subject": subject,
            "study_date": study_date,
            "capture_high_watermark": high_watermark,
            "tasks": tasks,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        snapshot_sha256 = hashlib.sha256(
            json.dumps(
                snapshot,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return snapshot, snapshot_sha256, high_watermark, tasks

    def _prepare_batch_before_submit(self, frozen: Sequence[Any]) -> None:
        self._frozen_batch_authority = None
        if not frozen:
            return
        if self.processing_host is None:
            # Minimal unit-test runtimes do not carry a model contract.  Every
            # valid production runtime does, and must never submit without the
            # subject-bound MCP authority host.
            if isinstance(self.config.get("model"), Mapping):
                raise DispatchError("processing_batch_authority_host_required")
            return
        study_date = current_date(str(self.config.get("timezone") or "UTC"))
        _, scan_sha256, high_watermark, tasks = self._scan_snapshot(
            self.subject, frozen, study_date
        )
        try:
            authority = self.processing_host.subject_authority_snapshot(
                self.subject
            )
        except ProcessingPluginError as exc:
            raise DispatchError(exc.code) from exc
        try:
            verify_exact_batch_authority(
                [self._frozen_task(task).frozen_payload for task in frozen],
                authority,
            )
        except MathExactSmokeError as exc:
            raise DispatchError(exc.code) from exc
        batch_id = (
            f"LUNA-{self.subject.upper()}-{study_date}-"
            f"{scan_sha256[:20].upper()}"
        )
        migration_commit = None
        migration_descriptors = {
            str(binding["migration_descriptor_sha256"])
            for task in frozen
            for binding in [
                self._frozen_task(task).frozen_payload.get(
                    "math_exact_smoke_binding"
                )
            ]
            if isinstance(binding, Mapping)
            and binding.get("migration_descriptor_sha256") is not None
        }
        if self.subject == "math" and len(frozen) == 4:
            if len(migration_descriptors) == 1:
                try:
                    runtime_root = Path(
                        str(self.config["runtime_root"])
                    ).resolve(strict=True)
                    reopened = reopen_exact_math_pending_queue_migration_commit(
                        runtime_root=runtime_root,
                        migration_descriptor_sha256=next(
                            iter(migration_descriptors)
                        ),
                    )
                    raw_commit_path = Path(
                        str(reopened["migration_commit_path"])
                    )
                    if raw_commit_path.is_symlink():
                        raise ValueError("migration_commit_path")
                    commit_path = raw_commit_path.resolve(strict=True)
                    commit_path.relative_to(runtime_root)
                    if not commit_path.is_file():
                        raise ValueError("migration_commit_path")
                    commit_raw = commit_path.read_bytes()
                    if (
                        hashlib.sha256(commit_raw).hexdigest()
                        != reopened["migration_commit_sha256"]
                    ):
                        raise ValueError("migration_commit_sha256")
                    migration_commit = json.loads(commit_raw)
                    if not isinstance(migration_commit, dict):
                        raise ValueError("migration_commit")
                except (
                    KeyError,
                    OSError,
                    ValueError,
                    json.JSONDecodeError,
                    MathExactSmokeError,
                ) as exc:
                    raise DispatchError(
                        "math_migration_batch_authority_invalid"
                    ) from exc
            elif migration_descriptors:
                raise DispatchError("math_migration_batch_authority_invalid")
        batch = self.subject_sol.prepare_and_freeze_subject_batch(
            subject=self.subject,
            batch_id=batch_id,
            study_date=study_date,
            capture_high_watermark=high_watermark,
            authority_generation=str(authority["generation"]),
            authority_fingerprint=str(authority["authority_fingerprint"]),
            tasks=tasks,
            scan_snapshot_sha256=scan_sha256,
            exact_math_migration_commit=migration_commit,
        )
        if batch.get("status") != "frozen":
            raise DispatchError("subject_batch_prefreeze_invalid")
        self._frozen_batch_authority = {
            "batch_id": batch_id,
            "scan_snapshot_sha256": scan_sha256,
            "generation": str(authority["generation"]),
            "authority_fingerprint": str(authority["authority_fingerprint"]),
        }

    def sync_subject_batch_task_progress(self) -> dict[str, Any] | None:
        """Idempotently advance batch tasks from verified task-event history."""

        batch = self.subject_sol.read_subject_batch(self.subject)
        if batch is None or batch.get("status") != "frozen":
            return batch
        release_id, _ = release_identity(self.config)
        terminal_states = (
            {
                "workflow_complete",
                "workflow_complete_with_warnings",
                "workflow_partial",
                "execution_failed",
                "cancelled",
                "stalled",
            }
            if batch.get("schema_version") == "subject_luna_batch_v2"
            else {
                "quality_passed",
                "needs_rework",
                "failed",
                "evidence_pending",
            }
        )
        terminal_events = {
            "published",
            "needs_rework",
            "failed",
            "timeout",
            "cancel",
        }
        for task in list(batch.get("tasks") or []):
            if task.get("status") in terminal_states:
                continue
            try:
                history = self.dispatcher.lease_store.verify_task_event_history(
                    str(task["unit_sha256"]),
                    expected_release_id=release_id,
                )
            except DispatchError as exc:
                if exc.code in {
                    "task_detail_missing",
                    "task_event_history_missing",
                }:
                    continue
                raise
            rows = history.get("events")
            if not isinstance(rows, list) or not rows:
                continue
            event_value = rows[-1].get("event")
            if not isinstance(event_value, Mapping):
                raise DispatchError("subject_batch_task_event_invalid")
            event = str(event_value.get("event") or "")
            if event in terminal_events:
                # The terminal completion/receipt path owns the terminal batch
                # transition; a task event alone may not manufacture quality.
                continue
            stage_name = event_value.get("stage_name")
            if stage_name == "critical_review" or event in {
                "analysis_completed",
                "analysis_checkpoint_reused",
                "critical_started",
                "critical_completed",
            }:
                desired = "critical_review_running"
            elif event == "claim":
                desired = "claimed"
            elif stage_name == "analysis" or event in {
                "process_started",
                "child_process_started",
                "child_process_exited",
                "provider_process_started",
                "provider_process_exited",
                "model_submitted",
                "analysis_submitted",
            }:
                desired = "analysis_running"
            else:
                continue
            batch = self.subject_sol.record_task_progress(
                subject=self.subject,
                batch_id=str(batch["batch_id"]),
                capture_id=str(task["capture_id"]),
                unit_sha256=str(task["unit_sha256"]),
                status=desired,
            )
        return batch

    def prepare_controlled_replay_tasks(
        self,
        tasks: Sequence[FrozenTask],
        *,
        release_id: str,
    ) -> dict[str, Any]:
        """Prefreeze and allowlist one exact candidate-bound subject task set.

        This is the controlled-replay counterpart of ``scan_and_submit``.  It
        accepts only already frozen canonical tasks, creates the normal subject
        Luna batch and generation fence, but does not submit or call a model.
        """

        checked = tuple(self._frozen_task(task) for task in tasks)
        if (
            not checked
            or len({task.unit_sha256 for task in checked}) != len(checked)
            or any(task.frozen_payload.get("subject") != self.subject for task in checked)
            or not set(self._direct_controlled_candidates).issubset(
                {task.unit_sha256 for task in checked}
            )
        ):
            raise DispatchError("controlled_replay_subject_task_set_invalid")
        admission = self.subject_sol.luna_admission(self.subject)
        if admission.get("read_session_allowed") is not True:
            raise DispatchError(str(admission.get("reason") or "luna_admission_blocked"))
        self._prepare_batch_before_submit(checked)
        if self._frozen_batch_authority is None:
            raise DispatchError("subject_batch_prefreeze_invalid")
        prepared = self.dispatcher.lease_store.prepare_controlled_replay_allowlist(
            release_id=release_id,
            tasks=checked,
        )
        if self.dispatcher.active_count != 0:
            raise DispatchError("controlled_replay_dispatcher_active")
        self.dispatcher.controlled_replay_authority = copy.deepcopy(
            dict(prepared["allowlist"])
        )
        return {
            "subject": self.subject,
            "dispatcher_id": self.dispatcher.owner_id,
            "batch_authority": copy.deepcopy(self._frozen_batch_authority),
            "allowlist_path": prepared["allowlist_path"],
            "allowlist_sha256": prepared["allowlist_sha256"],
            "task_count": len(checked),
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def submit_prepared_controlled_replay_task(self, task: FrozenTask) -> Any:
        """Submit one member of the exact pre-frozen controlled task set."""

        checked = self._frozen_task(task)
        if (
            self._frozen_batch_authority is None
            or checked.frozen_payload.get("subject") != self.subject
            or self.dispatcher.controlled_replay_authority is None
        ):
            raise DispatchError("controlled_replay_subject_not_prepared")
        try:
            return self.subject_sol.submit_luna_under_generation_fence(
                self.subject,
                self.dispatcher.submit,
                checked,
            )
        except SubjectSolContractError as exc:
            raise DispatchError(exc.code) from exc

    @staticmethod
    def _canary_failure_code(exc: Exception, fallback: str) -> str:
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code:
            return code
        if isinstance(exc, OSError):
            return f"{fallback}_os_{getattr(exc, 'errno', 'unknown')}"
        return fallback

    @staticmethod
    def _materialize_failure_stage(error_code: str) -> str:
        if (
            error_code.startswith("producer_")
            or error_code
            in {
                "production_canary_producer_binding_mismatch",
                "production_canary_fast_mode_binding_invalid",
            }
        ):
            return "producer_contract"
        return "materialize"

    def _record_canary_preclaim_failure(
        self,
        task: FrozenTask | None,
        *,
        failure_stage: str,
        error_code: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        receipt = self.dispatcher.lease_store.fail_production_canary_preclaim(
            self.subject,
            task,
            failure_stage=failure_stage,
            error_code=error_code,
        )
        matched = False
        for decision in decisions:
            if (
                task is not None
                and decision.get("unit_sha256") != task.unit_sha256
            ):
                continue
            decision.update(
                {
                    "eligible": False,
                    "reason": error_code,
                    "error_code": error_code,
                    "phase": "production_canary_preclaim_failed",
                    "failure_stage": failure_stage,
                    "preclaim_failure_receipt_sha256": receipt[
                        "preclaim_failure_receipt_sha256"
                    ],
                    "model_enqueue_allowed": False,
                }
            )
            matched = True
        if not matched:
            decisions.append(
                {
                    "subject": self.subject,
                    "capture_id": (
                        task.frozen_payload.get("capture_id")
                        if task is not None
                        else None
                    ),
                    "study_date": (
                        task.frozen_payload.get("study_date")
                        if task is not None
                        else current_date(
                            str(self.config.get("timezone") or "UTC")
                        )
                    ),
                    "unit_sha256": (
                        task.unit_sha256 if task is not None else None
                    ),
                    "eligible": False,
                    "reason": error_code,
                    "error_code": error_code,
                    "phase": "production_canary_preclaim_failed",
                    "failure_stage": failure_stage,
                    "preclaim_failure_receipt_sha256": receipt[
                        "preclaim_failure_receipt_sha256"
                    ],
                    "model_enqueue_allowed": False,
                    "formal_write_count": 0,
                }
            )
        return receipt

    def _reconcile_english_quick_flush_supersessions(
        self,
    ) -> list[dict[str, Any]]:
        """Retire corrected queued intents before any consumer claim."""

        if not self.production_canary or self.subject != "english":
            return []
        pending = self.dispatcher.lease_store.pending_production_canary_tasks(
            "english"
        )
        quick_flush_tasks = [
            task
            for task in pending
            if isinstance(
                task.frozen_payload.get("input_binding"), Mapping
            )
            and task.frozen_payload["input_binding"].get("batch_trigger")
            == "explicit_quick_intake"
        ]
        if not quick_flush_tasks:
            return []
        worker = Worker(self.config)
        adapter = worker.adapters.get("english")
        if adapter is None or not hasattr(
            adapter, "quick_flush_supersession_evidence"
        ):
            raise DispatchError("english_adapter_invalid")
        decisions: list[dict[str, Any]] = []
        for task in quick_flush_tasks:
            payload = task.frozen_payload
            binding = payload.get("input_binding")
            assert isinstance(binding, Mapping)
            try:
                candidate = Candidate(
                    subject=str(payload["subject"]),
                    capture_id=str(payload["capture_id"]),
                    study_date=str(payload["study_date"]),
                    recorded_at=(
                        str(payload["recorded_at"])
                        if payload.get("recorded_at") is not None
                        else None
                    ),
                    input_fingerprint=str(payload["input_fingerprint"]),
                    input_binding=copy.deepcopy(dict(binding)),
                    model_input=copy.deepcopy(dict(payload["model_input"])),
                    allowed_evidence_refs=tuple(
                        str(value)
                        for value in payload["allowed_evidence_refs"]
                    ),
                    image_paths=tuple(
                        Path(str(value)) for value in payload["image_paths"]
                    ),
                    target_label=str(payload["target_label"]),
                    canonical_state=str(payload["canonical_state"]),
                    sol_state=str(payload["sol_state"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise DispatchError(
                    "english_quick_flush_queue_candidate_invalid"
                ) from exc
            try:
                worker._assert_current_candidate_generation(candidate)
            except PreprocessorError as exc:
                if exc.code != "stale_input_superseded":
                    raise
            else:
                continue
            evidence = adapter.quick_flush_supersession_evidence(candidate)
            result = (
                self.dispatcher.lease_store.supersede_stale_english_quick_flush_task(
                    task, supersession_evidence=evidence
                )
            )
            decisions.append(
                {
                    "subject": "english",
                    "capture_id": task.frozen_payload.get("capture_id"),
                    "study_date": task.frozen_payload.get("study_date"),
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "eligible": False,
                    "reason": "english_quick_flush_source_superseded",
                    "phase": "producer_queue_superseded",
                    "terminal_receipt_sha256": result[
                        "terminal_receipt_sha256"
                    ],
                    "model_enqueue_allowed": False,
                    "formal_write_count": 0,
                }
            )
        return decisions

    def scan_and_submit(self) -> tuple[list[Any], list[dict[str, Any]]]:
        if self.config.get("execution_mode") == "offline":
            raise DispatchError("offline_producer_scan_forbidden")
        if self.production_canary:
            self.dispatcher.lease_store.reconcile_production_canary_preclaim_failures(
                self.subject
            )
            global_failure = (
                self.dispatcher.lease_store.production_canary_global_preclaim_failure(
                    self.subject
                )
            )
            if global_failure is not None:
                receipt = global_failure["receipt"]
                decisions = [
                    {
                        "subject": self.subject,
                        "capture_id": None,
                        "study_date": current_date(
                            str(self.config.get("timezone") or "UTC")
                        ),
                        "unit_sha256": None,
                        "eligible": False,
                        "reason": receipt["error_code"],
                        "error_code": receipt["error_code"],
                        "phase": "production_canary_preclaim_failed",
                        "failure_stage": receipt["failure_stage"],
                        "preclaim_failure_receipt_sha256": global_failure[
                            "receipt_sha256"
                        ],
                        "model_enqueue_allowed": False,
                        "formal_write_count": 0,
                    }
                ]
                _write_subject_projections(
                    self.config,
                    self.subject,
                    daemon_status="running",
                    decisions=decisions,
                    lease_status=self.dispatcher.lease_store.subject_status(
                        self.subject
                    ),
                    error_code=str(receipt["error_code"]),
                )
                return [], decisions
        try:
            scan_kwargs: dict[str, Any] = {}
            scan_worker_factory = getattr(self, "_scan_worker_factory", None)
            if scan_worker_factory is not None:
                scan_kwargs["worker_factory"] = scan_worker_factory
            if self.production_canary and self.subject == "math":
                canary = (
                    self.dispatcher.lease_store.production_canary_status_read_only(
                        self.subject
                    )
                )
                high_watermark = (
                    canary.get("producer_high_watermark")
                    if isinstance(canary, Mapping)
                    else None
                )
                producer_recorded_after = (
                    high_watermark.get("recorded_at")
                    if isinstance(high_watermark, Mapping)
                    else None
                )
                if not isinstance(producer_recorded_after, str):
                    raise DispatchError(
                        "production_canary_high_watermark_invalid"
                    )
                scan_kwargs["producer_recorded_after"] = (
                    producer_recorded_after
                )
            frozen, decisions = scan_eligible_candidates(
                self.config, self.subject, **scan_kwargs
            )
        except Exception as exc:
            if not self.production_canary:
                raise
            error_code = self._canary_failure_code(
                exc, "producer_contract_scan_failed"
            )
            decisions: list[dict[str, Any]] = []
            self._record_canary_preclaim_failure(
                None,
                failure_stage="producer_contract",
                error_code=error_code,
                decisions=decisions,
            )
            _write_subject_projections(
                self.config,
                self.subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=self.dispatcher.lease_store.subject_status(
                    self.subject
                ),
                error_code=error_code,
            )
            return [], decisions
        tasks: list[FrozenTask]
        if self.production_canary:
            if self.subject == "english":
                try:
                    decisions.extend(
                        self._reconcile_english_quick_flush_supersessions()
                    )
                except Exception as exc:
                    error_code = self._canary_failure_code(
                        exc,
                        "english_quick_flush_supersession_failed",
                    )
                    self._record_canary_preclaim_failure(
                        None,
                        failure_stage="materialize",
                        error_code=error_code,
                        decisions=decisions,
                    )
                    frozen = []
            for unit in frozen:
                prior_failure = (
                    self.dispatcher.lease_store.production_canary_preclaim_failure_for_task(
                        self.subject, unit.task
                    )
                )
                if prior_failure is not None:
                    prior_receipt = prior_failure["receipt"]
                    for decision in decisions:
                        if decision.get("unit_sha256") != unit.task.unit_sha256:
                            continue
                        decision.update(
                            {
                                "eligible": False,
                                "reason": prior_receipt["error_code"],
                                "error_code": prior_receipt["error_code"],
                                "phase": "production_canary_preclaim_failed",
                                "failure_stage": prior_receipt[
                                    "failure_stage"
                                ],
                                "preclaim_failure_receipt_sha256": (
                                    prior_failure["receipt_sha256"]
                                ),
                                "model_enqueue_allowed": False,
                            }
                        )
                    continue
                try:
                    materialized = self.dispatcher.lease_store.materialize_production_canary_task(
                        unit.task
                    )
                    if (
                        materialized.get("materialization_status")
                        == "already_represented_by_queue"
                    ):
                        for decision in decisions:
                            if (
                                decision.get("unit_sha256")
                                != unit.task.unit_sha256
                            ):
                                continue
                            decision.update(
                                {
                                    "eligible": False,
                                    "reason": (
                                        "already_represented_by_queue"
                                    ),
                                    "phase": "producer_queue_alias",
                                    "model_enqueue_allowed": False,
                                }
                            )
                        continue
                    if (
                        materialized.get("materialization_status")
                        == "quick_flush_source_superseded"
                    ):
                        for decision in decisions:
                            if (
                                decision.get("unit_sha256")
                                != unit.task.unit_sha256
                            ):
                                continue
                            decision.update(
                                {
                                    "eligible": False,
                                    "reason": (
                                        "english_quick_flush_source_superseded"
                                    ),
                                    "phase": "producer_queue_superseded",
                                    "model_enqueue_allowed": False,
                                }
                            )
                        continue
                except Exception as exc:
                    error_code = self._canary_failure_code(
                        exc, "production_canary_materialize_failed"
                    )
                    if error_code == "production_canary_pre_activation_capture":
                        try:
                            self.dispatcher.lease_store.record_production_canary_pre_activation_exclusion(
                                unit.task
                            )
                        except Exception as exclusion_exc:
                            exclusion_code = self._canary_failure_code(
                                exclusion_exc,
                                "production_canary_pre_activation_exclusion_failed",
                            )
                            self._record_canary_preclaim_failure(
                                unit.task,
                                failure_stage="materialize",
                                error_code=exclusion_code,
                                decisions=decisions,
                            )
                        else:
                            for decision in decisions:
                                if (
                                    decision.get("unit_sha256")
                                    != unit.task.unit_sha256
                                ):
                                    continue
                                decision.update(
                                    {
                                        "eligible": False,
                                        "reason": "pre_activation_frozen",
                                        "phase": "producer_queue_excluded",
                                        "model_enqueue_allowed": False,
                                    }
                                )
                        continue
                    self._record_canary_preclaim_failure(
                        unit.task,
                        failure_stage=self._materialize_failure_stage(
                            error_code
                        ),
                        error_code=error_code,
                        decisions=decisions,
                    )
            canary = self.dispatcher.lease_store.production_canary_status(
                self.subject
            )
            if canary is None:
                raise DispatchError("production_canary_not_active")
            active_count = int(canary.get("active_task_count") or 0)
            gate_state = canary.get("state")
            available_capacity = 0
            if (
                canary.get("luna_consumer_enabled") is True
                and not getattr(self, "_projection_fail_closed", False)
            ):
                if gate_state == "armed":
                    available_capacity = max(0, 1 - active_count)
                elif gate_state == "continuous_concurrent_unlocked":
                    available_capacity = max(
                        0,
                        int(canary["continuous_concurrency_limit"])
                        - active_count,
                    )
            backpressure_reason = (
                "continuous_concurrency_limit_reached"
                if gate_state == "continuous_concurrent_unlocked"
                and canary.get("luna_consumer_enabled") is True
                and available_capacity == 0
                else None
            )
            canary = (
                self.dispatcher.lease_store.set_production_canary_backpressure(
                    self.subject, backpressure_reason
                )
            )
            if available_capacity > 0:
                try:
                    tasks = self.dispatcher.lease_store.pending_production_canary_tasks(
                        self.subject
                    )[:available_capacity]
                except Exception as exc:
                    error_code = self._canary_failure_code(
                        exc, "production_canary_pending_queue_read_failed"
                    )
                    self._record_canary_preclaim_failure(
                        None,
                        failure_stage="materialize",
                        error_code=error_code,
                        decisions=decisions,
                    )
                    tasks = []
            else:
                tasks = []
            selected_units = {task.unit_sha256 for task in tasks}
            for decision in decisions:
                unit_sha256 = decision.get("unit_sha256")
                if (
                    decision.get("eligible") is True
                    and isinstance(unit_sha256, str)
                    and unit_sha256 not in selected_units
                ):
                    decision.update(
                        {
                            "eligible": False,
                            "reason": (
                                backpressure_reason
                                or "production_canary_queued_not_selected"
                            ),
                            "phase": "producer_queue_pending",
                            "model_enqueue_allowed": False,
                        }
                    )
            known_units = {
                str(row.get("unit_sha256"))
                for row in decisions
                if isinstance(row.get("unit_sha256"), str)
            }
            for task in tasks:
                if task.unit_sha256 in known_units:
                    continue
                payload = task.frozen_payload
                decisions.append(
                    {
                        "subject": self.subject,
                        "capture_id": payload.get("capture_id"),
                        "study_date": payload.get("study_date"),
                        "input_fingerprint": payload.get("input_fingerprint"),
                        "eligible": True,
                        "reason": "production_canary_pending_queue",
                        "unit_sha256": task.unit_sha256,
                        "frozen_payload_sha256": task.frozen_payload_sha256,
                        "phase": "producer_queue_pending",
                        "model": task.requested_model,
                        "reasoning_effort": task.requested_reasoning_effort,
                        "model_enqueue_allowed": True,
                        "formal_write_count": 0,
                    }
                )
        else:
            tasks = [unit.task for unit in frozen]
        handles: list[Any] = []
        if not tasks:
            _write_subject_projections(
                self.config,
                self.subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=self.dispatcher.lease_store.subject_status(
                    self.subject
                ),
            )
            return [], decisions
        try:
            admission = self.subject_sol.luna_admission(self.subject)
        except Exception as exc:
            if not self.production_canary:
                raise
            error_code = self._canary_failure_code(
                exc, "production_canary_consumer_admission_failed"
            )
            self._record_canary_preclaim_failure(
                tasks[0],
                failure_stage="consumer_admission",
                error_code=error_code,
                decisions=decisions,
            )
            _write_subject_projections(
                self.config,
                self.subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=self.dispatcher.lease_store.subject_status(
                    self.subject
                ),
                error_code=error_code,
            )
            return [], decisions
        if admission["read_session_allowed"] is not True:
            if self.production_canary:
                self._record_canary_preclaim_failure(
                    tasks[0],
                    failure_stage="consumer_admission",
                    error_code=str(admission["reason"]),
                    decisions=decisions,
                )
            for decision in decisions:
                if decision.get("eligible") is not True:
                    continue
                decision.update(
                    {
                        "eligible": False,
                        "reason": admission["reason"],
                        "error_code": admission["reason"],
                        "phase": "next_batch",
                        "model_enqueue_allowed": False,
                    }
                )
            _write_subject_projections(
                self.config,
                self.subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=self.dispatcher.lease_store.subject_status(
                    self.subject
                ),
                error_code=str(admission["reason"]),
            )
            return [], decisions
        try:
            self._prepare_batch_before_submit(tasks)
        except Exception as exc:
            if not self.production_canary:
                raise
            error_code = self._canary_failure_code(
                exc, "production_canary_pre_claim_failed"
            )
            self._record_canary_preclaim_failure(
                tasks[0],
                failure_stage="pre_claim",
                error_code=error_code,
                decisions=decisions,
            )
            _write_subject_projections(
                self.config,
                self.subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=self.dispatcher.lease_store.subject_status(
                    self.subject
                ),
                error_code=error_code,
            )
            return [], decisions
        _write_subject_projections(
            self.config,
            self.subject,
            daemon_status="running",
            decisions=decisions,
            lease_status=self.dispatcher.lease_store.subject_status(
                self.subject
            ),
        )
        for task in tasks:
            try:
                handles.append(
                    self.subject_sol.submit_luna_under_generation_fence(
                        self.subject,
                        self.dispatcher.submit,
                        task,
                    )
                )
            except SubjectSolContractError as exc:
                error_code = exc.code
            except DispatchError as exc:
                error_code = exc.code
            except OSError as exc:
                error_code = (
                    "submit_process_resource_"
                    + str(getattr(exc, "errno", "unknown"))
                )
            except RuntimeError:
                error_code = "submit_runtime_failed"
            else:
                continue
            if self.production_canary:
                self._record_canary_preclaim_failure(
                    task,
                    failure_stage="submit_generation_fence",
                    error_code=error_code,
                    decisions=decisions,
                )
            if self._frozen_batch_authority is not None:
                try:
                    self.subject_sol.record_task_terminal_failure(
                        subject=self.subject,
                        batch_id=self._frozen_batch_authority["batch_id"],
                        capture_id=str(task.frozen_payload["capture_id"]),
                        unit_sha256=task.unit_sha256,
                        status="failed",
                        error_code=error_code,
                    )
                except SubjectSolContractError as exc:
                    raise DispatchError(
                        "subject_batch_terminal_feedback_failed"
                    ) from exc
            for decision in decisions:
                if decision.get("unit_sha256") != task.unit_sha256:
                    continue
                decision.update(
                    {
                        "eligible": False,
                        "reason": error_code,
                        "error_code": error_code,
                        "phase": "failed",
                        "model_enqueue_allowed": False,
                    }
                )
        return handles, decisions

    def persist_finished_luna(
        self, handles: Iterable[Any]
    ) -> list[dict[str, Any]]:
        result = self.persist_finished_luna_with_status(handles)
        failures = result["failures"]
        if failures and not self.production_canary:
            error = failures[0].get("exception")
            if isinstance(error, BaseException):
                raise error
            raise DispatchError(str(failures[0]["error_code"]))
        return list(result["projected"])

    def persist_finished_luna_with_status(
        self, handles: Iterable[Any]
    ) -> dict[str, Any]:
        """Project each done handle independently and never abandon siblings."""

        projected: list[dict[str, Any]] = []
        handled_units: list[str] = []
        failures: list[dict[str, Any]] = []
        for handle in handles:
            if not handle.done:
                continue
            unit_sha256 = str(getattr(handle, "unit_sha256", ""))
            try:
                projected.extend(
                    self._persist_finished_luna_unchecked([handle])
                )
                handled_units.append(unit_sha256)
            except Exception as exc:
                if not self.production_canary and not isinstance(
                    exc, (SubjectSolContractError, DispatchError, OSError)
                ):
                    raise
                error_code = (
                    exc.code
                    if isinstance(exc, (SubjectSolContractError, DispatchError))
                    else (
                        "production_canary_projection_os_error"
                        if isinstance(exc, OSError)
                        else "production_canary_projection_unexpected"
                    )
                )
                durable_failure_receipt = False
                if self.production_canary:
                    task = getattr(handle, "task", None)
                    if isinstance(task, FrozenTask):
                        try:
                            self.dispatcher.lease_store.fail_production_canary_post_terminal(
                                task, error_code=str(error_code)
                            )
                            durable_failure_receipt = True
                            handled_units.append(unit_sha256)
                        except (DispatchError, OSError) as receipt_exc:
                            self._projection_fail_closed = True
                            error_code = (
                                receipt_exc.code
                                if isinstance(receipt_exc, DispatchError)
                                else "production_canary_projection_failure_receipt_os_error"
                            )
                    else:
                        self._projection_fail_closed = True
                        error_code = (
                            "production_canary_projection_task_binding_missing"
                        )
                failures.append(
                    {
                        "unit_sha256": unit_sha256 or None,
                        "error_code": str(error_code),
                        "durable_failure_receipt": durable_failure_receipt,
                        "exception": exc,
                    }
                )
        return {
            "projected": projected,
            "handled_units": handled_units,
            "failures": failures,
        }

    def _rollover_exact_math_review_batch_if_unlocked(
        self,
        *,
        handle: Any,
        result: Any,
        completion: Mapping[str, Any],
        verified: Mapping[str, Any],
        runtime: Mapping[str, Any],
    ) -> bool:
        """Archive only the accepted GS-269 diagnostic batch, with zero writes."""

        if (
            not self.production_canary
            or self.subject != "math"
            or completion.get("outcome") != "succeeded"
        ):
            return False
        task = getattr(handle, "task", None)
        first_id = EXACT_ORDER[0]
        first_capture_id = str(EXACT_SAMPLES[first_id]["capture_event_id"])
        if not isinstance(task, FrozenTask):
            if completion.get("capture_id") == first_capture_id:
                raise SubjectSolContractError(
                    "exact_math_review_task_binding_missing"
                )
            return False
        binding = task.frozen_payload.get("math_exact_smoke_binding")
        if not isinstance(binding, Mapping):
            return False
        if binding.get("formal_id") != first_id:
            return False
        if (
            completion.get("capture_id") != first_capture_id
            or completion.get("unit_sha256") != result.unit_sha256
            or task.unit_sha256 != result.unit_sha256
        ):
            raise SubjectSolContractError(
                "exact_math_review_completion_binding_invalid"
            )
        verified_package = verified.get("package")
        if not isinstance(verified_package, Mapping):
            raise SubjectSolContractError(
                "exact_math_review_restricted_evidence_incomplete"
            )
        if verified_package.get("report_disposition") == "quarantined":
            return False
        if verified_package.get("report_disposition") != "needs_sol_review":
            return False

        try:
            accepted = reopen_exact_gs269_review_terminal(
                runtime_root=self.runtime_root,
                task_payload=task.frozen_payload,
                unit_sha256=result.unit_sha256,
            )
        except MathExactSmokeError as exc:
            raise SubjectSolContractError(exc.code) from exc
        report_sha256 = str(accepted["report_json_sha256"])
        restricted = self.subject_sol.read_restricted_sol_review_candidate(
            report_sha256
        )
        report = restricted.get("report")
        raw_outputs = restricted.get("raw_outputs")
        transcripts = restricted.get("mcp_transcripts")
        if (
            restricted.get("report_available") is not True
            or restricted.get("report_disposition") != "needs_sol_review"
            or restricted.get("sol_review_status") != "pending"
            or restricted.get("formal_write_eligible") is not False
            or restricted.get("automatic_adoption") is not False
            or restricted.get("automatic_formal_write") is not False
            or restricted.get("formal_write_count") != 0
            or not isinstance(report, Mapping)
            or report.get("subject") != "math"
            or report.get("capture_id") != first_capture_id
            or report.get("unit_sha256") != result.unit_sha256
            or not isinstance(raw_outputs, Mapping)
            or not isinstance(raw_outputs.get("analysis"), Mapping)
            or not isinstance(transcripts, Mapping)
            or not isinstance(transcripts.get("analysis"), Mapping)
            or not transcripts["analysis"].get("calls")
        ):
            raise SubjectSolContractError(
                "exact_math_review_restricted_evidence_incomplete"
            )

        batch = runtime.get("subject_luna_batch")
        writer = runtime.get("writer_state")
        tasks = batch.get("tasks") if isinstance(batch, Mapping) else None
        target = tasks[0] if isinstance(tasks, list) and len(tasks) == 1 else None
        if (
            not isinstance(batch, Mapping)
            or batch.get("schema_version") != "subject_luna_batch_v2"
            or batch.get("subject") != "math"
            or batch.get("status") != "frozen"
            or batch.get("all_terminal") is not True
            or batch.get("sol_ready") is not False
            or batch.get("formal_write_count") != 0
            or batch.get("sol_candidate_task_ids") != []
            or batch.get("diagnostic_task_ids") != [first_capture_id]
            or batch.get("blocking_task_ids") != []
            or not isinstance(target, Mapping)
            or target.get("capture_id") != first_capture_id
            or target.get("unit_sha256") != result.unit_sha256
            or target.get("status") != "workflow_complete_with_warnings"
            or target.get("formal_write_count", 0) != 0
            or not isinstance(writer, Mapping)
            or writer.get("handoff_status") != "awaiting_luna"
            or writer.get("batch_id") != batch.get("batch_id")
            or writer.get("formal_write_count") != 0
            or any(
                writer.get(key) is not None
                for key in (
                    "authorization_receipt_sha256",
                    "daily_sol_batch_sha256",
                    "review_receipt_sha256",
                    "commit_receipt_sha256",
                )
            )
        ):
            raise SubjectSolContractError(
                "exact_math_review_subject_batch_not_releasable"
            )

        canary = self.dispatcher.lease_store.production_canary_status_read_only(
            "math", expected_release_id=str(completion["release_id"])
        )
        if (
            not isinstance(canary, Mapping)
            or canary.get("state") != "continuous_concurrent_unlocked"
            or canary.get("unlocked_once") is not True
            or canary.get("luna_consumer_enabled") is not True
            or canary.get("active_task_count") != 0
            or canary.get("sol_enabled") is not False
            or canary.get("formal_write_count") != 0
            or canary.get("last_terminal_receipt_sha256")
            != accepted["terminal_receipt_sha256"]
        ):
            raise SubjectSolContractError(
                "exact_math_review_canary_unlock_invalid"
            )

        rolled = self.subject_sol.rollover_background_luna_batch(
            "math",
            mode="explicit_failure_resume",
            resume_acceptance_sha256=str(
                accepted["terminal_receipt_sha256"]
            ),
        )
        receipt = rolled.get("receipt")
        writer_after = rolled.get("writer_state")
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("mode") != "explicit_failure_resume"
            or receipt.get("subject") != "math"
            or receipt.get("old_batch_id") != batch.get("batch_id")
            or receipt.get("resume_acceptance_sha256")
            != accepted["terminal_receipt_sha256"]
            or receipt.get("sol_called") is not False
            or receipt.get("sol_enabled") is not False
            or receipt.get("formal_write_count") != 0
            or not isinstance(writer_after, Mapping)
            or writer_after.get("handoff_status") != "awaiting_luna"
            or writer_after.get("batch_id") is not None
            or writer_after.get("formal_write_count") != 0
        ):
            raise SubjectSolContractError(
                "exact_math_review_batch_rollover_invalid"
            )
        self._frozen_batch_authority = None
        return True

    def _persist_finished_luna_unchecked(
        self, handles: Iterable[Any]
    ) -> list[dict[str, Any]]:
        """Verify terminal authority and project it into this subject's Sol state."""

        projected: list[dict[str, Any]] = []
        for handle in handles:
            if not handle.done:
                continue
            result = handle.wait(0)
            completion = result.completion
            if not isinstance(completion, Mapping):
                if (
                    self._frozen_batch_authority is not None
                    and result.outcome == "failed"
                ):
                    batch = self.subject_sol.read_subject_batch(self.subject)
                    batch_tasks = (
                        batch.get("tasks", [])
                        if isinstance(batch, Mapping)
                        else []
                    )
                    target = next(
                        (
                            row
                            for row in batch_tasks
                            if isinstance(row, Mapping)
                            and row.get("unit_sha256") == result.unit_sha256
                        ),
                        None,
                    )
                    if not isinstance(target, Mapping):
                        raise SubjectSolContractError(
                            "dispatcher_terminal_task_binding_missing"
                        )
                    self.subject_sol.record_task_terminal_failure(
                        subject=self.subject,
                        batch_id=self._frozen_batch_authority["batch_id"],
                        capture_id=str(target["capture_id"]),
                        unit_sha256=result.unit_sha256,
                        status="failed",
                        error_code=str(result.error_code or "luna_task_failed"),
                    )
                continue
            capture_id = completion.get("capture_id")
            release_id = completion.get("release_id")
            if (
                completion.get("subject") != self.subject
                or not isinstance(capture_id, str)
                or not capture_id
                or not isinstance(release_id, str)
                or not release_id
            ):
                raise SubjectSolContractError(
                    "dispatcher_luna_completion_binding_invalid"
                )
            verified = (
                self.dispatcher.lease_store.verify_authoritative_completion(
                    self.subject,
                    capture_id,
                    expected_release_id=release_id,
                    expected_unit_sha256=result.unit_sha256,
                )
            )
            runtime = self.subject_sol.record_verified_luna_completion(
                self.subject, verified
            )
            if self._rollover_exact_math_review_batch_if_unlocked(
                handle=handle,
                result=result,
                completion=completion,
                verified=verified,
                runtime=runtime,
            ):
                runtime = self.subject_sol.read_subject(self.subject)
            if completion.get("outcome") != "succeeded":
                projected.append(runtime)
                continue
            batch = runtime.get("subject_luna_batch")
            tasks = batch.get("tasks") if isinstance(batch, Mapping) else None
            target = next(
                (
                    row
                    for row in tasks
                    if isinstance(row, Mapping)
                    and row.get("capture_id") == capture_id
                    and row.get("unit_sha256") == result.unit_sha256
                ),
                None,
            ) if isinstance(tasks, list) else None
            if not isinstance(target, Mapping):
                raise SubjectSolContractError(
                    "dispatcher_quality_task_binding_missing"
                )
            if batch.get("schema_version") == "subject_luna_batch_v2":
                if target.get("status") not in {
                    "workflow_complete",
                    "workflow_complete_with_warnings",
                    "workflow_partial",
                    "execution_failed",
                    "cancelled",
                    "stalled",
                }:
                    raise SubjectSolContractError(
                        "dispatcher_v2_terminal_projection_incomplete"
                    )
                if (
                    self.production_canary
                    and batch.get("all_terminal") is True
                    and batch.get("sol_ready") is True
                ):
                    self.subject_sol.rollover_background_luna_batch(
                        self.subject, mode="auto_success"
                    )
                    self._frozen_batch_authority = None
                    runtime = self.subject_sol.read_subject(self.subject)
                projected.append(runtime)
                continue
            if target.get("status") == "quality_pending":
                try:
                    closure = (
                        self.subject_sol.build_verified_completion_quality_closure(
                            self.subject, verified, config=self.config
                        )
                    )
                    self.subject_sol.close_verified_completion(
                        self.subject, closure=closure, config=self.config
                    )
                except SubjectSolContractError as exc:
                    if exc.code != "quality_subject_proposal_not_sol_ready":
                        raise
                    self.subject_sol.record_task_terminal_failure(
                        subject=self.subject,
                        batch_id=str(batch["batch_id"]),
                        capture_id=capture_id,
                        unit_sha256=result.unit_sha256,
                        status="failed",
                        error_code=exc.code,
                    )
                runtime = self.subject_sol.read_subject(self.subject)
            elif target.get("status") not in {
                "quality_passed",
                "needs_rework",
                "failed",
                "evidence_pending",
            }:
                raise SubjectSolContractError(
                    "dispatcher_quality_closure_incomplete"
                )
            if self.production_canary:
                current_batch = self.subject_sol.read_subject_batch(
                    self.subject
                )
                if (
                    isinstance(current_batch, Mapping)
                    and current_batch.get("all_terminal") is True
                    and current_batch.get("sol_ready") is True
                ):
                    self.subject_sol.rollover_background_luna_batch(
                        self.subject, mode="auto_success"
                    )
                    self._frozen_batch_authority = None
                    runtime = self.subject_sol.read_subject(self.subject)
            projected.append(runtime)
        return projected


def _run_once(
    config: Mapping[str, Any], subject: str, config_path: Path
) -> dict[str, Any]:
    if config.get("execution_mode") == "offline":
        raise DispatchError("offline_run_once_forbidden")
    runtime = ProductionDispatchRuntime(config, subject, config_path)
    runtime.dispatcher.lease_store.clear_subject_drain(subject)
    handles, decisions = runtime.scan_and_submit()
    runtime.dispatcher.drain()
    runtime.persist_finished_luna(handles)
    results = [handle.wait(0) for handle in handles]
    lease_status = runtime.dispatcher.lease_store.subject_status(subject)
    _write_subject_projections(
        config,
        subject,
        daemon_status="drained",
        decisions=decisions,
        lease_status=lease_status,
    )
    return {
        "schema_version": "study-intake-concurrent-run-once-v1",
        "subject": subject,
        "status": "drained",
        "eligible_count": len(handles),
        "results": [_result_value(result) for result in results],
        "lease_status": lease_status,
        "subject_sol": runtime.subject_sol.read_subject(subject),
        "formal_write_count": 0,
    }


def _audit(config: Mapping[str, Any], subject: str) -> dict[str, Any]:
    if config.get("execution_mode") == "offline":
        raise DispatchError("offline_producer_scan_forbidden")
    store = LeaseStore(Path(str(config["runtime_root"])))
    foreign_release = False
    configured_release_id = None
    producer_recorded_after = None
    if _production_canary_enabled(config):
        configured_release_id, _ = release_identity(config)
        canary_identity = store.production_canary_identity_read_only(
            subject, configured_release_id=configured_release_id
        )
        foreign_release = (
            canary_identity is not None
            and canary_identity.get("foreign_release") is True
        )
        # The producer watermark fences the Capture discovery window for both
        # a foreign target release during preview and the same release after
        # activation.  Tying this filter only to ``foreign_release`` makes
        # preview and apply scan different historical records: preview skips
        # the pre-watermark backlog, while apply re-enters it and can fail on
        # stale evidence.  Read the signed watermark whenever the subject has
        # a canary state; leave normal no-canary operation unchanged.
        if subject == "math" and canary_identity is not None:
            previous_canary = store.production_canary_status_read_only(subject)
            high_watermark = (
                previous_canary.get("producer_high_watermark")
                if isinstance(previous_canary, Mapping)
                else None
            )
            producer_recorded_after = (
                high_watermark.get("recorded_at")
                if isinstance(high_watermark, Mapping)
                else None
            )
            if not isinstance(producer_recorded_after, str):
                raise DispatchError("production_canary_high_watermark_invalid")
    frozen, decisions = scan_eligible_candidates(
        config,
        subject,
        publish_evidence_readiness=False,
        producer_recorded_after=producer_recorded_after,
    )
    lease_status = store.subject_status_read_only(subject)
    canary_gate = None
    audit_pre_activation_count = 0
    audit_post_activation_count = 0
    audit_unpersisted_pre_activation_ids: set[str] = set()
    if _production_canary_enabled(config):
        if foreign_release:
            # A target-release preflight must not validate newly frozen tasks
            # or the release-specific state/index schema against the signed
            # authority of the release it will replace.  The activation
            # transaction establishes the target high-watermark before any task
            # can be materialized or claimed.
            audit_pre_activation_count = len(frozen)
            for unit in frozen:
                for decision in decisions:
                    if decision.get("unit_sha256") != unit.task.unit_sha256:
                        continue
                    decision.update(
                        {
                            "producer_eligible": True,
                            "eligible": False,
                            "reason": "pre_activation_frozen",
                            "phase": "target_release_preflight_read_only",
                            "model_enqueue_allowed": False,
                        }
                    )
            canary_gate = None
        else:
            canary_gate = store.production_canary_status_read_only(
                subject, expected_release_id=configured_release_id
            )
        if (
            not foreign_release
            and canary_gate is not None
            and canary_gate.get("status") == "production_canary_active"
        ):
            for unit in frozen:
                inspection = store.inspect_production_canary_task_read_only(
                    subject, unit.task
                )
                classification = str(inspection["classification"])
                if classification == "pre_activation_frozen":
                    audit_pre_activation_count += 1
                    if inspection.get("persisted") is not True:
                        audit_unpersisted_pre_activation_ids.add(
                            str(inspection["producer_unit_id"])
                        )
                else:
                    audit_post_activation_count += 1
                for decision in decisions:
                    if decision.get("unit_sha256") != unit.task.unit_sha256:
                        continue
                    decision.update(
                        {
                            "producer_eligible": True,
                            "eligible": False,
                            "reason": classification,
                            "phase": "production_canary_audit_read_only",
                            "model_enqueue_allowed": False,
                        }
                    )
    return {
        "schema_version": "study-intake-dispatch-backlog-audit-v1",
        "subject": subject,
        "eligible_count": len(frozen),
        "historical_eligible_count": (
            int(canary_gate.get("historical_eligible_count") or 0)
            + len(audit_unpersisted_pre_activation_ids)
            if isinstance(canary_gate, Mapping)
            else audit_pre_activation_count
        ),
        "excluded_by_high_watermark_count": (
            int(canary_gate.get("excluded_by_high_watermark_count") or 0)
            + len(audit_unpersisted_pre_activation_ids)
            if isinstance(canary_gate, Mapping)
            else audit_pre_activation_count
        ),
        "canary_queue_count": (
            canary_gate.get("canary_queue_count")
            if isinstance(canary_gate, Mapping)
            else 0
        ),
        "post_activation_unmaterialized_count": audit_post_activation_count,
        "read_only": True,
        "draining": lease_status["draining"],
        "active_count": lease_status["active_count"],
        "claimed_total": lease_status["claimed_total"],
        "lease_status": lease_status,
        "eligible_units": [
            {
                "capture_id": unit.task.frozen_payload.get("capture_id"),
                "unit_sha256": unit.task.unit_sha256,
                "input_fingerprint": unit.task.frozen_payload.get(
                    "input_fingerprint"
                ),
            }
            for unit in frozen
        ],
        "decisions": decisions,
        "canary_gate": canary_gate,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
    }


def _run_daemon(
    config: Mapping[str, Any], subject: str, config_path: Path
) -> int:
    runtime = ProductionDispatchRuntime(config, subject, config_path)
    stop = threading.Event()
    heartbeat_stop = threading.Event()
    heartbeat_failures: list[Exception] = []

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    previous_handlers = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    outstanding: list[Any] = []

    def write_control_heartbeat() -> Mapping[str, Any]:
        status = runtime.dispatcher.lease_store.subject_status(subject)
        canary_gate = status.get("canary_gate")
        canary_control_alive = bool(
            runtime.production_canary
            and isinstance(canary_gate, Mapping)
            and canary_gate.get("status") == "production_canary_active"
            and canary_gate.get("state") != "inactive_rolled_back"
        )
        daemon_status = (
            "paused"
            if status.get("draining") is True and not canary_control_alive
            else "running"
        )
        _write_subject_projections_resilient(
            config,
            subject,
            daemon_status=daemon_status,
            decisions=[],
            lease_status=status,
        )
        return status

    heartbeat_interval = 15.0

    def heartbeat_loop() -> None:
        while not heartbeat_stop.wait(heartbeat_interval):
            if stop.is_set():
                return
            try:
                write_control_heartbeat()
            except Exception as exc:
                heartbeat_failures.append(exc)
                stop.set()
                return

    heartbeat_thread: threading.Thread | None = None
    try:
        # Publish readiness before the first producer scan.  Canonical producer
        # scans can legitimately take longer than the heartbeat freshness
        # window, so a dedicated control ticker keeps the process heartbeat
        # fresh while the main thread remains inside scan/persist work.
        first_status = write_control_heartbeat()
        raw_interval = first_status.get("heartbeat_interval_seconds", 15)
        if (
            isinstance(raw_interval, (int, float))
            and not isinstance(raw_interval, bool)
            and float(raw_interval) > 0
        ):
            heartbeat_interval = float(raw_interval)
        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            name=f"{subject}-control-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        while not stop.is_set():
            error_code = None
            decisions: list[dict[str, Any]] = []
            submitted = 0
            status = runtime.dispatcher.lease_store.subject_status(subject)
            canary_gate = status.get("canary_gate")
            canary_control_alive = bool(
                runtime.production_canary
                and isinstance(canary_gate, Mapping)
                and canary_gate.get("status") == "production_canary_active"
                and canary_gate.get("state") != "inactive_rolled_back"
            )
            if config.get("execution_mode") == "offline":
                _write_subject_projections_resilient(
                    config,
                    subject,
                    daemon_status="running",
                    decisions=[
                        {
                            "subject": subject,
                            "eligible": False,
                            "reason": "offline_live_gate_locked",
                            "phase": "control_only",
                            "model_enqueue_allowed": False,
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "mcp_tool_call_count": 0,
                            "formal_write_count": 0,
                        }
                    ],
                    lease_status=status,
                )
                stop.wait(_poll_interval(config, subject))
                continue
            if status.get("draining") is True and not canary_control_alive:
                _write_subject_projections_resilient(
                    config,
                    subject,
                    daemon_status="paused",
                    decisions=[],
                    lease_status=status,
                )
                stop.wait(_poll_interval(config, subject))
                continue
            # Project every already-finished handle before opening another
            # scan.  This prevents a terminal result whose SubjectSol/control
            # projection fails from admitting a fresh sibling in the next
            # polling turn.
            finished = [handle for handle in outstanding if handle.done]
            projection = runtime.persist_finished_luna_with_status(finished)
            handled_units = set(projection["handled_units"])
            projection_failures = projection["failures"]
            if projection_failures:
                error_code = str(projection_failures[0]["error_code"])
            outstanding = [
                handle
                for handle in outstanding
                if not (
                    handle.done and handle.unit_sha256 in handled_units
                )
            ]
            try:
                runtime.sync_subject_batch_task_progress()
                handles, decisions = runtime.scan_and_submit()
                outstanding.extend(handles)
                submitted = len(handles)
                runtime.sync_subject_batch_task_progress()
            except (DispatchError, PreprocessorError, SubjectSolContractError) as exc:
                error_code = exc.code
            status = runtime.dispatcher.lease_store.subject_status(subject)
            _write_subject_projections_resilient(
                config,
                subject,
                daemon_status="running",
                decisions=decisions,
                lease_status=status,
                error_code=error_code,
            )
            stop.wait(_poll_interval(config, subject))
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=5.0)
        runtime.dispatcher.lease_store.begin_subject_drain(subject)
        shutdown = runtime.dispatcher.emergency_cancel(
            timeout=10.0,
            error_code="daemon_shutdown",
        )
        # Do not invoke Sol/projection publication while shutting down.  Every
        # active lease has either a cancelled completion plus canary terminal,
        # or remains visibly claimed and makes the release-manager gate fail.
        # The call is bounded, so SIGTERM never waits for a 3600-second stage.
        status = runtime.dispatcher.lease_store.subject_status(subject)
        shutdown_complete = bool(
            shutdown.get("late_result_fence_status") == "sealed"
            and status.get("claimed_total") == 0
            and status.get("active_count") == 0
        )
        _write_subject_projections_resilient(
            config,
            subject,
            daemon_status=(
                "drained" if shutdown_complete else "shutdown_incomplete"
            ),
            decisions=[],
            lease_status=status,
            error_code=(
                None if shutdown_complete else "daemon_shutdown_fence_incomplete"
            ),
        )
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    if heartbeat_failures:
        raise heartbeat_failures[0]
    return 0


def _result_value(result: object) -> dict[str, Any]:
    return {
        "unit_sha256": result.unit_sha256,
        "status": result.status,
        "outcome": result.outcome,
        "error_code": result.error_code,
        "completion": result.completion,
    }


def _low_level_dispatch(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    values = _load_task_values(args.tasks)
    tasks = [FrozenTask.from_mapping(value) for value in values]
    command = args.runner_command_json
    dispatcher = ConcurrentDispatcher(
        args.runtime_root,
        lambda _task, _context: SubprocessTwoPassRunner(command),
        soft_runtime_warning_seconds=args.stage_timeout_seconds,
    )
    handles = dispatcher.dispatch(tasks, wait=False)
    drained = dispatcher.drain()
    results = [handle.wait(0) for handle in handles]
    value = {
        "schema_version": "study-intake-concurrent-dispatch-cli-v1",
        "drained": drained,
        "active_count": dispatcher.active_count,
        "results": [_result_value(result) for result in results],
    }
    ok = drained and all(result.outcome in {"succeeded", None} for result in results)
    return (0 if ok else 1), value


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "dispatch":
            code, value = _low_level_dispatch(args)
        else:
            config, subject = _require_production_args(args)
            if args.command == "run":
                return _run_daemon(config, subject, args.config)
            store = LeaseStore(Path(str(config["runtime_root"])))
            subject_sol = SubjectSolRuntimeStore(
                Path(str(config["runtime_root"]))
            )
            if args.command == "run-once":
                value = _run_once(config, subject, args.config)
                code = 0 if all(
                    row["outcome"] in {"succeeded", None}
                    for row in value["results"]
                ) else 1
            elif args.command == "status":
                value = {
                    **store.subject_status(subject),
                    "subject_sol": subject_sol.read_subject(subject),
                }
                code = 0
            elif args.command == "canary-readiness":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.canary_readiness()
                code = 0
            elif args.command == "recover-subject-batch":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.recover_subject_batch(
                    expected_original_preclaim_failure_receipt_sha256=(
                        args.expected_original_preclaim_failure_receipt_sha256
                    ),
                    expected_next_generation=args.expected_next_generation,
                    expected_next_authority_fingerprint=(
                        args.expected_next_authority_fingerprint
                    ),
                )
                code = 0
            elif args.command == "retire-cs408-terminal-batch":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.retire_cs408_terminal_batch(
                    expected_batch_sha256=args.expected_batch_sha256,
                    expected_terminal_receipt_sha256=(
                        args.expected_terminal_receipt_sha256
                    ),
                    expected_writer_preimage_sha256=(
                        args.expected_writer_preimage_sha256
                    ),
                    expected_batch_pointer_sha256=(
                        args.expected_batch_pointer_sha256
                    ),
                    expected_snapshot_sha256=args.expected_snapshot_sha256,
                    expected_source_generation=(
                        args.expected_source_generation
                    ),
                    expected_source_authority_fingerprint=(
                        args.expected_source_authority_fingerprint
                    ),
                    expected_next_generation=args.expected_next_generation,
                    expected_next_authority_fingerprint=(
                        args.expected_next_authority_fingerprint
                    ),
                )
                code = 0
            elif args.command == "reopen-cs408-terminal-batch-retirement":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_cs408_terminal_batch_retirement(
                    retirement_receipt=args.retirement_receipt,
                    expected_terminal_receipt_sha256=(
                        args.expected_terminal_receipt_sha256
                    ),
                )
                code = 0
            elif args.command == "rollback-cs408-terminal-batch-retirement":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.rollback_cs408_terminal_batch_retirement(
                    retirement_receipt=args.retirement_receipt
                )
                code = 0
            elif args.command == "reopen-cs408-terminal-batch-retirement-rollback":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_cs408_terminal_batch_retirement_rollback(
                    retirement_receipt=args.retirement_receipt,
                    rollback_receipt=args.rollback_receipt,
                )
                code = 0
            elif args.command == "english-preserved-review-repair-preview":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.preview_english_preserved_review_repair()
                code = 0
            elif args.command == "math-pending-queue-migration-preview":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.preview_math_pending_queue_migration()
                code = 0
            elif args.command == "prepare-math-pending-queue-migration":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.prepare_math_pending_queue_migration()
                code = 0
            elif args.command == "apply-math-pending-queue-migration":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.apply_math_pending_queue_migration(
                    migration_intent_sha256=(
                        args.migration_intent_sha256
                    ),
                    expected_target_activation_id=(
                        args.expected_target_activation_id
                    ),
                    expected_target_authority_generation=(
                        args.expected_target_authority_generation
                    ),
                    expected_target_subject_authority_fingerprint=(
                        args.expected_target_subject_authority_fingerprint
                    ),
                    expected_target_producer_authority_fingerprint=(
                        args.expected_target_producer_authority_fingerprint
                    ),
                )
                code = 0
            elif args.command == "reopen-math-pending-queue-migration":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_math_pending_queue_migration(
                    migration_descriptor_sha256=(
                        args.migration_descriptor_sha256
                    )
                )
                code = 0
            elif args.command == "english-preserved-review-repair-apply":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.apply_english_preserved_review_repair(
                    target_release_id=args.target_release_id,
                    target_activation_id=args.target_activation_id,
                    target_generation=args.target_generation,
                    target_subject_authority_fingerprint=(
                        args.target_subject_authority_fingerprint
                    ),
                    target_producer_authority_fingerprint=(
                        args.target_producer_authority_fingerprint
                    ),
                    staged_target_canary_state_sha256=(
                        args.staged_target_canary_state_sha256
                    ),
                )
                code = 0
            elif args.command == "english-preserved-review-repair-reopen":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_english_preserved_review_repair(
                    repair_receipt_sha256=args.repair_receipt_sha256
                )
                code = 0
            elif args.command == "english-preserved-review-repair-rollback":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.rollback_english_preserved_review_repair(
                    repair_receipt_sha256=args.repair_receipt_sha256,
                    batch_archive_sha256=args.batch_archive_sha256,
                )
                code = 0
            elif args.command == (
                "english-preserved-review-repair-reopen-rollback"
            ):
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_english_preserved_review_repair_rollback(
                    repair_receipt_sha256=args.repair_receipt_sha256,
                    rollback_receipt_sha256=(
                        args.rollback_receipt_sha256
                    ),
                    batch_rollback_receipt_sha256=(
                        args.batch_rollback_receipt_sha256
                    ),
                )
                code = 0
            elif args.command == "stage-subject-batch-recovery-activation":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.stage_subject_batch_recovery_activation(
                    recovery_receipt=args.recovery_receipt,
                    activated_at=args.activated_at,
                    expected_activation_id=args.expected_activation_id,
                    expected_producer_authority_fingerprint=(
                        args.expected_producer_authority_fingerprint
                    ),
                )
                code = 0
            elif args.command == "finalize-subject-batch-recovery":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.finalize_subject_batch_recovery(
                    recovery_receipt=args.recovery_receipt,
                    expected_target_release_id=(
                        args.expected_target_release_id
                    ),
                )
                code = 0
            elif args.command == "arm-finalized-subject-batch-recovery":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.arm_finalized_subject_batch_recovery(
                    recovery_receipt=args.recovery_receipt,
                    expected_target_release_id=(
                        args.expected_target_release_id
                    ),
                )
                code = 0
            elif args.command == "rollback-subject-batch-recovery":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.rollback_subject_batch_recovery(
                    recovery_receipt=args.recovery_receipt
                )
                code = 0
            elif args.command == "reopen-subject-batch-recovery-rollback":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.reopen_subject_batch_recovery_rollback(
                    recovery_receipt=args.recovery_receipt,
                    rollback_receipt=args.rollback_receipt,
                    expected_deployment_canary_state_sha256=(
                        args.expected_deployment_canary_state_sha256
                    ),
                    expected_finalization_rollback_proof_count=(
                        args.expected_finalization_rollback_proof_count
                    ),
                )
                code = 0
            elif args.command == "activate-canary":
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.activate_production_canary(
                    activated_at=args.activated_at,
                    expected_activation_id=args.expected_activation_id,
                    expected_producer_authority_fingerprint=(
                        args.expected_producer_authority_fingerprint
                    ),
                )
                code = 0
            elif args.command == "resume-canary":
                if not _production_canary_enabled(config):
                    raise DispatchError("production_canary_not_configured")
                runtime = ProductionDispatchRuntime(
                    config, subject, args.config
                )
                value = runtime.resume_production_canary()
                code = 0
            elif args.command == "pause-canary":
                if not _production_canary_enabled(config):
                    raise DispatchError("production_canary_not_configured")
                value = store.pause_production_canary(subject)
                code = 0
            elif args.command == "deactivate-canary":
                value = store.deactivate_production_canary(
                    subject, expected_release_id=args.expected_release_id
                )
                code = 0
            elif args.command == "audit":
                value = _audit(config, subject)
                code = 0
            elif args.command == "quarantine-stale-claim":
                value = store.quarantine_stale_claim(
                    subject,
                    args.unit_sha256,
                    apply=args.apply,
                )
                code = 0
            elif args.command == "sol-status":
                value = subject_sol.read_subject(subject)
                code = 0
            elif args.command == "sol-global-status":
                value = subject_sol.read_global()
                code = 0
            elif args.command.startswith("english-legacy-sol-"):
                raise SubjectSolContractError(
                    "english_legacy_recuration_authorization_revoked"
                )
                if subject != "english":
                    raise SubjectSolContractError(
                        "english_legacy_command_requires_english_subject"
                    )
                owner_id = (
                    args.owner_id
                    if hasattr(args, "owner_id")
                    and isinstance(args.owner_id, str)
                    and args.owner_id
                    else f"dispatcher-control-{os.getpid()}"
                )
                if args.command == "english-legacy-sol-stage":
                    value = subject_sol.stage_english_legacy_recuration_batch(
                        _load_control_object(
                            args.batch, "english_legacy_recuration_sol_batch"
                        ),
                        config=config,
                    )
                elif args.command == "english-legacy-sol-begin":
                    value = subject_sol.begin_english_legacy_recuration(
                        args.batch_id, owner_id=owner_id
                    )
                elif args.command == "english-legacy-sol-review-item":
                    value = subject_sol.record_english_legacy_item_review(
                        args.receipt_sha256
                    )
                elif args.command == "english-legacy-sol-finish-item":
                    value = subject_sol.finish_english_legacy_item(
                        args.receipt_sha256
                    )
                elif args.command == "english-legacy-sol-fail-item":
                    value = subject_sol.record_english_legacy_item_failure(
                        args.receipt_sha256
                    )
                elif args.command == "english-legacy-sol-begin-recovery":
                    value = subject_sol.begin_english_legacy_recovery(
                        args.batch_id, owner_id=owner_id
                    )
                elif args.command == "english-legacy-sol-record-recovery":
                    value = subject_sol.record_english_legacy_item_recovery(
                        args.receipt_sha256
                    )
                else:
                    value = subject_sol.read_english_legacy_recuration_state(
                        args.batch_id
                    )
                code = 0
            elif args.command == "sol-freeze-luna-batch":
                value = subject_sol.freeze_subject_batch(
                    subject, args.batch_id
                )
                code = 0
            elif args.command == "sol-record-quality":
                value = subject_sol.record_quality_receipt(
                    _load_control_object(
                        args.receipt, "subject_quality_receipt"
                    )
                )
                code = 0
            elif args.command == "sol-authorize":
                batch = subject_sol.build_authorized_batch(
                    subject,
                    sol_batch_id=args.sol_batch_id,
                    authorization_receipt_sha256=(
                        args.authorization_receipt_sha256
                    ),
                )
                value = subject_sol.authorize_batch(
                    subject,
                    batch,
                )
                code = 0
            elif args.command == "sol-begin":
                value = subject_sol.begin_sol_review(
                    subject,
                    args.batch_id,
                    owner_id=(
                        args.owner_id
                        if isinstance(args.owner_id, str) and args.owner_id
                        else f"dispatcher-control-{os.getpid()}"
                    ),
                )
                code = 0
            elif args.command == "sol-review":
                value = subject_sol.record_sol_review(
                    subject,
                    args.receipt_sha256,
                )
                code = 0
            elif args.command == "sol-finish":
                value = subject_sol.finish_subject_commit(
                    subject,
                    args.writer_apply_receipt_sha256,
                )
                code = 0
            elif args.command == "sol-prepare-exclusion":
                value = subject_sol.prepare_subject_exclusion_authority(
                    subject,
                    original_batch_id=args.batch_id,
                    capture_id=args.capture_id,
                    unit_sha256=args.unit_sha256,
                )
                code = 0
            elif args.command == "sol-exclude":
                value = subject_sol.issue_subject_exclusion_receipt(
                    subject,
                    authorization_receipt_sha256=(
                        args.authorization_receipt_sha256
                    ),
                )
                code = 0
            elif args.command == "sol-ack-generation":
                value = subject_sol.acknowledge_subject_generation(
                    subject, authority_ack_sha256=args.authority_ack_sha256
                )
                code = 0
            elif args.command == "resume":
                if _production_canary_enabled(config):
                    raise DispatchError(
                        "production_canary_requires_explicit_resume"
                    )
                value = store.subject_status(subject)
                if (
                    value.get("draining") is not True
                    or value.get("active_count") != 0
                    or value.get("claimed_total") != 0
                ):
                    raise DispatchError("subject_resume_not_drained")
                store.clear_subject_drain(subject)
                value = store.subject_status(subject)
                if value.get("draining") is not False:
                    raise DispatchError("subject_resume_failed")
                code = 0
            elif args.command == "drain":
                store.begin_subject_drain(subject)
                value = store.wait_subject_drained(subject)
                code = 0
            else:
                raise DispatchError("command_invalid")
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
        return code
    except (DispatchError, PreprocessorError, SubjectSolContractError) as exc:
        diagnostic = (
            exc.diagnostic
            if isinstance(exc, SubjectSolContractError)
            else {}
        )
        print(
            json.dumps(
                {
                    "error_code": exc.code,
                    "schema_version": "dispatch-error-v1",
                    **diagnostic,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
