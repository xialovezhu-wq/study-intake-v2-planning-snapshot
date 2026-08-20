#!/usr/bin/env python3
"""CLI entry point for the single study-intake preprocessing worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    FileLock,
    PreprocessorError,
    Worker,
    atomic_write_json,
    current_date,
    json_file_bytes,
    load_config,
    run_loop,
    safe_component,
    sha256_file,
    utc_now,
)


DEFAULT_CONFIG = ROOT / "config.json"


class CurrentDateWorker(Worker):
    """Daemon worker that can only select candidates for the local current day."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._daemon_study_date: str | None = None

    def run_once(self, **kwargs: object) -> dict[str, object]:
        if kwargs:
            raise PreprocessorError("daemon_exact_args_forbidden")
        study_date = current_date(str(self.config["timezone"]))
        self._daemon_study_date = study_date
        try:
            return super().run_once(study_date=study_date)
        finally:
            self._daemon_study_date = None

    def candidates(
        self,
        statuses: object,
        *,
        subject: str | None = None,
        capture_id: str | None = None,
    ) -> list[object]:
        rows = super().candidates(
            statuses, subject=subject, capture_id=capture_id
        )
        if self._daemon_study_date is not None:
            mismatches = [
                row
                for row in rows
                if row.study_date != self._daemon_study_date
            ]
            if mismatches:
                raise PreprocessorError("daemon_candidate_date_drift")
        return rows


def _checkpoint_job_is_recovered(
    job: object,
    candidate: object,
    checkpoint_meta: dict[str, object],
) -> bool:
    if not isinstance(job, dict):
        return False
    expected = {
        "schema_version": "study-intake-preprocess-job-v1",
        "subject": candidate.subject,
        "capture_id": candidate.capture_id,
        "study_date": candidate.study_date,
        "input_fingerprint": candidate.input_fingerprint,
        "input_binding": candidate.input_binding,
        "status": "worker_interrupted",
        "critical_resume_status": "manual_recovery_required",
        "last_error_code": "worker_interrupted",
        "formal_write_count": 0,
        "analysis_checkpoint_sha256": checkpoint_meta["checkpoint_sha256"],
        "analysis_checkpoint_ref": checkpoint_meta["checkpoint_ref"],
        "analysis_checkpoint_binding_key": checkpoint_meta["binding_key"],
        "analysis_checkpoint_binding_sha256": checkpoint_meta[
            "binding_sha256"
        ],
    }
    return all(job.get(key) == value for key, value in expected.items()) and not any(
        job.get(key) is not None
        for key in ("package_id", "package_path", "package_sha256", "publication_id")
    )


def recover_analysis_checkpoint_job(
    worker: Worker,
    *,
    subject: str,
    capture_id: str,
    study_date: str,
    expected_input_fingerprint: str,
) -> dict[str, object]:
    """Rebind one verified analysis checkpoint after a stale daemon overwrote its job."""

    if subject != "cs408":
        raise PreprocessorError("checkpoint_recovery_subject_invalid")
    statuses = worker.scan_statuses(study_date)
    rows = worker.candidates(
        statuses, subject=subject, capture_id=capture_id
    )
    if len(rows) != 1 or rows[0].study_date != study_date:
        raise PreprocessorError("checkpoint_recovery_candidate_not_exact")
    candidate = rows[0]
    if candidate.input_fingerprint != expected_input_fingerprint:
        raise PreprocessorError("expected_input_fingerprint_mismatch")
    loader = getattr(worker.runner, "load_analysis_checkpoint", None)
    if not callable(loader):
        raise PreprocessorError("analysis_checkpoint_runner_unsupported")
    _, _, checkpoint_meta = loader(candidate)
    prior_job = worker.store.read_job(subject, capture_id)
    job_path = worker.store.job_path(subject, capture_id)
    if not isinstance(prior_job, dict) or not job_path.is_file():
        raise PreprocessorError("checkpoint_recovery_prior_job_missing")
    if prior_job.get("formal_write_count") != 0:
        raise PreprocessorError("checkpoint_recovery_formal_write_conflict")
    if _checkpoint_job_is_recovered(prior_job, candidate, checkpoint_meta):
        return {
            "schema_version": "study-intake-analysis-checkpoint-job-recovery-v1",
            "status": "noop",
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": candidate.input_fingerprint,
            "analysis_checkpoint_sha256": checkpoint_meta[
                "checkpoint_sha256"
            ],
            "formal_write_count": 0,
        }

    prior_job_sha256 = sha256_file(job_path)
    attempts = max(1, int(prior_job.get("attempts") or 0))
    recovered_job = worker._processing_job(candidate, attempts)
    recovered_job.update(
        {
            "status": "worker_interrupted",
            "updated_at": utc_now(),
            "next_retry_at": None,
            "last_error_code": "worker_interrupted",
            "retry_at_hint": None,
            "critical_resume_status": "manual_recovery_required",
            "critical_resume_attempts": int(
                prior_job.get("critical_resume_attempts") or 0
            ),
            "critical_resume_error_code": None,
            "analysis_checkpoint_sha256": checkpoint_meta[
                "checkpoint_sha256"
            ],
            "analysis_checkpoint_ref": checkpoint_meta["checkpoint_ref"],
            "analysis_checkpoint_binding_key": checkpoint_meta["binding_key"],
            "analysis_checkpoint_binding_sha256": checkpoint_meta[
                "binding_sha256"
            ],
            "analysis_checkpoint_reused": False,
            "recovered_from_input_fingerprint": prior_job.get(
                "input_fingerprint"
            ),
            "formal_write_count": 0,
        }
    )
    worker.store.write_job(candidate, recovered_job)
    verified_job = worker.store.read_job(subject, capture_id)
    if not _checkpoint_job_is_recovered(
        verified_job, candidate, checkpoint_meta
    ):
        atomic_write_json(job_path, prior_job)
        raise PreprocessorError("checkpoint_recovery_job_write_mismatch")
    recovered_job_sha256 = sha256_file(job_path)
    receipt = {
        "schema_version": "study-intake-analysis-checkpoint-job-recovery-receipt-v1",
        "subject": subject,
        "capture_id": capture_id,
        "study_date": study_date,
        "input_fingerprint": candidate.input_fingerprint,
        "input_binding_sha256": hashlib.sha256(
            json_file_bytes(candidate.input_binding)
        ).hexdigest(),
        "prior_job_sha256": prior_job_sha256,
        "prior_input_fingerprint": prior_job.get("input_fingerprint"),
        "recovered_job_sha256": recovered_job_sha256,
        "analysis_checkpoint_sha256": checkpoint_meta["checkpoint_sha256"],
        "analysis_checkpoint_ref": checkpoint_meta["checkpoint_ref"],
        "analysis_checkpoint_binding_key": checkpoint_meta["binding_key"],
        "analysis_checkpoint_binding_sha256": checkpoint_meta[
            "binding_sha256"
        ],
        "recovered_at": utc_now(),
        "formal_write_count": 0,
    }
    receipt_sha256 = hashlib.sha256(json_file_bytes(receipt)).hexdigest()
    receipt_path = (
        worker.store.root
        / "receipts"
        / "checkpoint-job-recovery"
        / subject
        / study_date
        / safe_component(capture_id)
        / f"{receipt_sha256}.json"
    )
    try:
        atomic_write_json(receipt_path, receipt)
        if sha256_file(receipt_path) != receipt_sha256:
            raise PreprocessorError("checkpoint_recovery_receipt_write_mismatch")
    except Exception:
        atomic_write_json(job_path, prior_job)
        if sha256_file(job_path) != prior_job_sha256:
            raise PreprocessorError("checkpoint_recovery_rollback_failed")
        raise
    return {
        "schema_version": "study-intake-analysis-checkpoint-job-recovery-v1",
        "status": "recovered",
        "subject": subject,
        "capture_id": capture_id,
        "study_date": study_date,
        "input_fingerprint": candidate.input_fingerprint,
        "analysis_checkpoint_sha256": checkpoint_meta["checkpoint_sha256"],
        "receipt_sha256": receipt_sha256,
        "receipt_path": str(receipt_path),
        "formal_write_count": 0,
    }


def migrate_analysis_checkpoint_generation(
    worker: Worker,
    *,
    subject: str,
    capture_id: str,
    study_date: str,
    expected_input_fingerprint: str,
    source_input_fingerprint: str,
    source_checkpoint_sha256: str,
    source_binding_key: str,
    source_binding_sha256: str,
) -> dict[str, object]:
    """Reject cross-generation checkpoint reuse before any worker access."""

    raise PreprocessorError("checkpoint_migration_retired_legacy_only")


def migrate_v2_publication_generation(
    worker: Worker,
    *,
    subject: str,
    capture_id: str,
    study_date: str,
    expected_input_fingerprint: str,
    source_input_fingerprint: str,
    source_checkpoint_sha256: str,
    source_binding_key: str,
    source_binding_sha256: str,
    source_package_sha256: str,
    source_publication_id: str,
) -> dict[str, object]:
    """Reject cross-generation package reuse before any worker access."""

    raise PreprocessorError("publication_migration_retired_legacy_only")


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Study intake Luna Max preprocessor")
    root.add_argument("--config", default=str(DEFAULT_CONFIG))
    sub = root.add_subparsers(dest="command", required=True)

    once = sub.add_parser("run-once", help="Scan canonical status and process bounded jobs")
    once.add_argument("--subject", choices=("math", "cs408"))
    once.add_argument("--capture-id")
    once.add_argument("--date")
    once.add_argument("--force", action="store_true")
    once.add_argument("--dry-run", action="store_true")
    once.add_argument("--resume-critical", action="store_true")
    once.add_argument("--expected-input-fingerprint")

    recover = sub.add_parser(
        "recover-analysis-checkpoint-job",
        help="Repair one exact checkpoint job binding without invoking a model",
    )
    recover.add_argument("--subject", choices=("cs408",), required=True)
    recover.add_argument("--capture-id", required=True)
    recover.add_argument("--date", required=True)
    recover.add_argument("--expected-input-fingerprint", required=True)

    migrate = sub.add_parser(
        "migrate-analysis-checkpoint",
        help=(
            "RETIRED legacy-only command; always fails closed because "
            "checkpoints cannot cross a release or processing contract"
        ),
    )
    migrate.add_argument("--subject", choices=("cs408",), required=True)
    migrate.add_argument("--capture-id", required=True)
    migrate.add_argument("--date", required=True)
    migrate.add_argument("--expected-input-fingerprint", required=True)
    migrate.add_argument("--source-input-fingerprint", required=True)
    migrate.add_argument("--source-checkpoint-sha256", required=True)
    migrate.add_argument("--source-binding-key", required=True)
    migrate.add_argument("--source-binding-sha256", required=True)

    migrate_publication = sub.add_parser(
        "migrate-v2-publication",
        help=(
            "RETIRED legacy-only command; always fails closed because old "
            "packages cannot be spliced into a new generation"
        ),
    )
    migrate_publication.add_argument(
        "--subject", choices=("cs408",), required=True
    )
    migrate_publication.add_argument("--capture-id", required=True)
    migrate_publication.add_argument("--date", required=True)
    migrate_publication.add_argument(
        "--expected-input-fingerprint", required=True
    )
    migrate_publication.add_argument(
        "--source-input-fingerprint", required=True
    )
    migrate_publication.add_argument(
        "--source-checkpoint-sha256", required=True
    )
    migrate_publication.add_argument("--source-binding-key", required=True)
    migrate_publication.add_argument(
        "--source-binding-sha256", required=True
    )
    migrate_publication.add_argument(
        "--source-package-sha256", required=True
    )
    migrate_publication.add_argument(
        "--source-publication-id", required=True
    )

    sub.add_parser("run", help="Run one locked polling worker until SIGTERM")
    sub.add_parser("status", help="Print the last dashboard projection")
    return root


def main() -> int:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        retired_commands = {
            "migrate-analysis-checkpoint": (
                "checkpoint_migration_retired_legacy_only"
            ),
            "migrate-v2-publication": (
                "publication_migration_retired_legacy_only"
            ),
        }
        if args.command in retired_commands:
            raise PreprocessorError(retired_commands[args.command])
        config = load_config(Path(args.config).expanduser().resolve())
        worker = CurrentDateWorker(config) if args.command == "run" else Worker(config)
        if args.command == "status":
            path = Path(str(config["dashboard"]["projection_path"]))
            if not path.is_file():
                emit(
                    {
                        "schema_version": "study-intake-preprocess-status-v1",
                        "status": "absent",
                        "projection_path": str(path),
                        "formal_write_count": 0,
                    }
                )
                return 0
            sys.stdout.write(path.read_text(encoding="utf-8"))
            return 0
        lock_path = Path(str(config["worker"]["lock_path"]))
        if args.command == "run":
            daemon_lock_path = lock_path.with_name(f"{lock_path.name}.daemon")
            with FileLock(daemon_lock_path):
                return run_loop(worker, operation_lock_path=lock_path)
        if args.command == "recover-analysis-checkpoint-job":
            with FileLock(lock_path):
                value = recover_analysis_checkpoint_job(
                    worker,
                    subject=args.subject,
                    capture_id=args.capture_id,
                    study_date=args.date,
                    expected_input_fingerprint=args.expected_input_fingerprint,
                )
                emit(value)
                return 0
        if args.command == "migrate-analysis-checkpoint":
            with FileLock(lock_path):
                value = migrate_analysis_checkpoint_generation(
                    worker,
                    subject=args.subject,
                    capture_id=args.capture_id,
                    study_date=args.date,
                    expected_input_fingerprint=(
                        args.expected_input_fingerprint
                    ),
                    source_input_fingerprint=args.source_input_fingerprint,
                    source_checkpoint_sha256=args.source_checkpoint_sha256,
                    source_binding_key=args.source_binding_key,
                    source_binding_sha256=args.source_binding_sha256,
                )
                emit(value)
                return 0
        if args.command == "migrate-v2-publication":
            with FileLock(lock_path):
                value = migrate_v2_publication_generation(
                    worker,
                    subject=args.subject,
                    capture_id=args.capture_id,
                    study_date=args.date,
                    expected_input_fingerprint=(
                        args.expected_input_fingerprint
                    ),
                    source_input_fingerprint=args.source_input_fingerprint,
                    source_checkpoint_sha256=(
                        args.source_checkpoint_sha256
                    ),
                    source_binding_key=args.source_binding_key,
                    source_binding_sha256=args.source_binding_sha256,
                    source_package_sha256=args.source_package_sha256,
                    source_publication_id=args.source_publication_id,
                )
                emit(value)
                return 0
        force_can_reach_cs408 = bool(
            args.force
            and (
                args.subject == "cs408"
                or (
                    args.subject is None
                    and config["adapters"]["cs408"].get("enabled") is True
                )
            )
        )
        if force_can_reach_cs408 and args.subject != "cs408":
            raise PreprocessorError("cs408_force_subject_required")
        if (
            (force_can_reach_cs408 or args.resume_critical)
            and (
                args.subject != "cs408"
                or not args.capture_id
                or not args.date
                or not args.expected_input_fingerprint
            )
        ):
            raise PreprocessorError("cs408_exact_operation_binding_required")
        with FileLock(lock_path):
            value = worker.run_once(
                subject=args.subject,
                capture_id=args.capture_id,
                study_date=args.date,
                force=args.force,
                dry_run=args.dry_run,
                resume_critical=args.resume_critical,
                expected_input_fingerprint=args.expected_input_fingerprint,
            )
            emit(value)
            return 0
    except PreprocessorError as exc:
        error = {
            "schema_version": "study-intake-preprocess-cli-error-v1",
            "status": "error",
            "error_code": exc.code,
            "formal_write_count": 0,
        }
        if isinstance(exc.diagnostic.get("retry_at"), str):
            error["retry_at_hint"] = exc.diagnostic["retry_at"]
        emit(error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
