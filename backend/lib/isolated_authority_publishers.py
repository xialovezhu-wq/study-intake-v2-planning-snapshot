"""Isolated authority adapters for user intent and deterministic Sol receipts.

These publishers are deliberately separate from SubjectSolRuntimeStore.  The
control plane can only reopen their content-addressed output; it has no public
method that signs user intent, Sol review, or writer apply results.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import tempfile
from typing import Any

from subject_sol_contract import (
    DAILY_SOL_BATCH_V3_SCHEMA,
    ENGLISH_LEGACY_ITEM_APPLY_SCHEMA,
    ENGLISH_LEGACY_ITEM_FAILURE_SCHEMA,
    ENGLISH_LEGACY_ITEM_RECOVERY_SCHEMA,
    ENGLISH_LEGACY_ITEM_REVIEW_SCHEMA,
    SUBJECT_AUTHORITY_OBSERVATION_SCHEMA,
    SUBJECT_BATCH_V2_SCHEMA,
    SOL_COMMIT_RECEIPT_V2_SCHEMA,
    SOL_REVIEW_RECEIPT_V2_SCHEMA,
    SUBJECT_GENERATION_AUTHORITY_ACK_SCHEMA,
    SUBJECT_WRITER_ADAPTERS,
    SubjectSolRuntimeStore,
    USER_SOL_AUTHORIZATION_SCHEMA,
    USER_SOL_AUTHORIZATION_V2_SCHEMA,
    USER_SUBJECT_EXCLUSION_AUTHORIZATION_SCHEMA,
    _canonical_bytes,
    _document_sha256,
    _json_file_bytes,
    validate_english_legacy_recuration_sol_batch_v1,
    validate_english_legacy_sol_item_apply_receipt_v1,
    validate_english_legacy_sol_item_failure_receipt_v1,
    validate_english_legacy_sol_item_recovery_receipt_v1,
    validate_english_legacy_sol_item_review_receipt_v1,
    validate_sol_commit_receipt_v1,
    validate_sol_commit_receipt_v2,
    validate_sol_review_receipt_v1,
    validate_sol_review_receipt_v2,
    validate_subject_luna_batch_v1,
    validate_subject_luna_batch_v2,
    validate_user_sol_authorization_receipt_v2,
)


class IsolatedAuthorityPublisherError(ValueError):
    pass


def _is_sha256_hex(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


class _Publisher:
    def __init__(self, runtime_root: Path, *, key_name: str) -> None:
        self.runtime_root = runtime_root.resolve()
        self.dispatch_root = self.runtime_root / "dispatch"
        self.key_path = (
            self.dispatch_root / "state" / "external-authorities" / key_name
        )

    def _key(self) -> bytes:
        self.key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not self.key_path.exists():
            try:
                descriptor = os.open(
                    self.key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
            except FileExistsError:
                pass
            else:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(secrets.token_bytes(32))
                    handle.flush()
                    os.fsync(handle.fileno())
        mode = self.key_path.stat().st_mode & 0o777
        key = self.key_path.read_bytes()
        if mode & 0o077 or len(key) < 32:
            raise IsolatedAuthorityPublisherError("isolated_authority_key_invalid")
        return key

    def _seal(self, core: Mapping[str, Any], *, purpose: str) -> dict[str, Any]:
        if "seal" in core:
            raise IsolatedAuthorityPublisherError("receipt_already_sealed")
        payload = dict(core)
        digest = hmac.new(
            self._key(),
            _canonical_bytes({"purpose": purpose, "payload": payload}),
            hashlib.sha256,
        ).hexdigest()
        return {
            **payload,
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": purpose,
                "hmac_sha256": digest,
            },
        }

    @staticmethod
    def _publish(root: Path, value: Mapping[str, Any]) -> tuple[str, Path]:
        payload = _json_file_bytes(value)
        digest = hashlib.sha256(payload).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() != payload:
                raise IsolatedAuthorityPublisherError("receipt_digest_conflict")
            return digest, path
        descriptor, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, path)
            os.chmod(path, 0o400)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise IsolatedAuthorityPublisherError("receipt_digest_conflict")
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return digest, path


class IndependentUserIntentPublisher(_Publisher):
    def __init__(self, runtime_root: Path, *, exclusion: bool = False) -> None:
        super().__init__(
            runtime_root,
            key_name=(
                "user-subject-exclusion-authorization.key"
                if exclusion else "user-sol-authorization.key"
            ),
        )
        self.exclusion = exclusion

    def publish_sol_authorization(
        self, *, luna_batch: Mapping[str, Any], sol_batch_id: str, event_id: str,
        authorized_at: str, idempotency_key: str,
    ) -> tuple[str, Path]:
        if self.exclusion:
            raise IsolatedAuthorityPublisherError("publisher_scope_invalid")
        if luna_batch.get("schema_version") == SUBJECT_BATCH_V2_SCHEMA:
            batch = validate_subject_luna_batch_v2(luna_batch)
            if batch["sol_ready"] is not True:
                raise IsolatedAuthorityPublisherError(
                    "subject_luna_batch_not_sol_ready"
                )
            task_by_capture = {
                row["capture_id"]: row for row in batch["tasks"]
            }
            handoffs = sorted(
                str(
                    task_by_capture[capture_id][
                        "sol_handoff_envelope_sha256"
                    ]
                )
                for capture_id in batch["sol_candidate_task_ids"]
            )
            core = {
                "schema_version": USER_SOL_AUTHORIZATION_V2_SCHEMA,
                "sol_batch_id": sol_batch_id,
                "subject": batch["subject"],
                "study_date": batch["study_date"],
                "subject_luna_batch_id": batch["batch_id"],
                "subject_luna_batch_sha256": _document_sha256(batch),
                "sol_candidate_task_ids": batch["sol_candidate_task_ids"],
                "sol_handoff_envelope_sha256s": handoffs,
                "diagnostic_task_ids": batch["diagnostic_task_ids"],
                "writer_adapter": SUBJECT_WRITER_ADAPTERS[batch["subject"]],
                "idempotency_key": idempotency_key,
                "event": {
                    "event_id": event_id,
                    "event_type": "explicit_user_sol_authorization",
                    "subject": batch["subject"],
                    "study_date": batch["study_date"],
                    "subject_luna_batch_id": batch["batch_id"],
                    "authorized_at": authorized_at,
                },
                "formal_write_count": 0,
            }
            sealed = self._seal(
                core, purpose="user-sol-authorization-receipt-v2"
            )
            validate_user_sol_authorization_receipt_v2(sealed)
            return self._publish(
                self.dispatch_root / "user-sol-authorizations", sealed
            )

        batch = validate_subject_luna_batch_v1(luna_batch)
        if batch["sol_ready"] is not True:
            raise IsolatedAuthorityPublisherError("subject_luna_batch_not_sol_ready")
        core = {
            "schema_version": USER_SOL_AUTHORIZATION_SCHEMA,
            "sol_batch_id": sol_batch_id,
            "subject": batch["subject"],
            "study_date": batch["study_date"],
            "subject_luna_batch_id": batch["batch_id"],
            "subject_luna_batch_sha256": _document_sha256(batch),
            "proposal_sha256s": sorted(row["proposal_sha256"] for row in batch["tasks"]),
            "package_sha256s": sorted(row["package_sha256"] for row in batch["tasks"]),
            "quality_receipt_sha256s": sorted(
                row["quality_receipt_sha256"] for row in batch["tasks"]
            ),
            "writer_adapter": SUBJECT_WRITER_ADAPTERS[batch["subject"]],
            "idempotency_key": idempotency_key,
            "event": {
                "event_id": event_id,
                "event_type": "explicit_user_sol_authorization",
                "subject": batch["subject"],
                "study_date": batch["study_date"],
                "subject_luna_batch_id": batch["batch_id"],
                "authorized_at": authorized_at,
            },
            "formal_write_count": 0,
        }
        sealed = self._seal(core, purpose="user-sol-authorization-receipt")
        return self._publish(self.dispatch_root / "user-sol-authorizations", sealed)

    def publish_subject_exclusion_authorization(
        self, *, request: Mapping[str, Any], event_id: str,
        authorized_at: str, reason: str,
    ) -> tuple[str, Path]:
        if not self.exclusion:
            raise IsolatedAuthorityPublisherError("publisher_scope_invalid")
        core = {
            "schema_version": USER_SUBJECT_EXCLUSION_AUTHORIZATION_SCHEMA,
            "event_id": event_id,
            "event_type": "explicit_user_subject_exclusion",
            "subject": request["subject"],
            "original_batch_id": request["original_batch_id"],
            "original_batch_sha256": request["original_batch_sha256"],
            "capture_id": request["capture_id"],
            "unit_sha256": request["unit_sha256"],
            "reason": reason,
            "authorized_at": authorized_at,
            "formal_write_count": 0,
        }
        sealed = self._seal(
            core, purpose="user-subject-exclusion-authorization-receipt"
        )
        return self._publish(
            self.dispatch_root / "user-subject-exclusion-authorizations", sealed
        )


class IsolatedWriterAdapterPublisher(_Publisher):
    """Receipt publisher used by the isolated writer adapter or its simulator."""

    def __init__(self, runtime_root: Path) -> None:
        super().__init__(runtime_root, key_name="sol-writer-adapter.key")

    def publish_review(self, _core: Mapping[str, Any]) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError("direct_sol_review_signer_disabled")

    def publish_english_legacy_item_review(
        self, _core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError(
            "direct_english_legacy_item_review_signer_disabled"
        )

    def publish_english_legacy_item_review_from_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("review_result_directory_invalid")
        core = self._read_result_file(root, "review.json")
        sealed = self._seal(
            {"schema_version": ENGLISH_LEGACY_ITEM_REVIEW_SCHEMA, **core},
            purpose="english-legacy-sol-item-review-receipt-v1",
        )
        validate_english_legacy_sol_item_review_receipt_v1(
            sealed, batch=checked_batch
        )
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-review",
            sealed,
        )

    def publish_english_legacy_item_apply(
        self, _core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError(
            "direct_english_legacy_item_apply_signer_disabled"
        )

    def publish_english_legacy_item_apply_from_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("writer_result_directory_invalid")
        core = self._publish_english_legacy_execution_evidence(
            root,
            self._read_result_file(root, "apply.json"),
            include_recovery_position=False,
        )
        sealed = self._seal(
            {"schema_version": ENGLISH_LEGACY_ITEM_APPLY_SCHEMA, **core},
            purpose="english-legacy-sol-item-apply-receipt-v1",
        )
        validate_english_legacy_sol_item_apply_receipt_v1(
            sealed, batch=checked_batch
        )
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-apply",
            sealed,
        )

    def publish_english_legacy_item_failure(
        self, _core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError(
            "direct_english_legacy_item_failure_signer_disabled"
        )

    def publish_english_legacy_item_failure_from_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("writer_result_directory_invalid")
        core = self._publish_english_legacy_execution_evidence(
            root,
            self._read_result_file(root, "failure.json"),
            include_recovery_position=True,
        )
        sealed = self._seal(
            {"schema_version": ENGLISH_LEGACY_ITEM_FAILURE_SCHEMA, **core},
            purpose="english-legacy-sol-item-failure-receipt-v1",
        )
        validate_english_legacy_sol_item_failure_receipt_v1(
            sealed, batch=checked_batch
        )
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-failure",
            sealed,
        )

    def publish_english_legacy_item_recovery(
        self, _core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError(
            "direct_english_legacy_item_recovery_signer_disabled"
        )

    def publish_english_legacy_item_recovery_from_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        checked_batch = validate_english_legacy_recuration_sol_batch_v1(batch)
        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("writer_result_directory_invalid")
        core = self._read_result_file(root, "recovery.json")
        sealed = self._seal(
            {"schema_version": ENGLISH_LEGACY_ITEM_RECOVERY_SCHEMA, **core},
            purpose="english-legacy-sol-item-recovery-receipt-v1",
        )
        validate_english_legacy_sol_item_recovery_receipt_v1(
            sealed, batch=checked_batch
        )
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts"
            / "english-legacy-item-recovery",
            sealed,
        )

    def publish_review_from_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("review_result_directory_invalid")
        core = self._read_result_file(root, "review.json")
        is_v2 = batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
        schema_version = (
            SOL_REVIEW_RECEIPT_V2_SCHEMA
            if is_v2
            else "sol_review_receipt_v1"
        )
        purpose = (
            "writer-adapter-sol-review-receipt-v2"
            if is_v2
            else "writer-adapter-sol-review-receipt"
        )
        sealed = self._seal(
            {"schema_version": schema_version, **core},
            purpose=purpose,
        )
        if is_v2:
            validate_sol_review_receipt_v2(sealed, batch=batch)
        else:
            validate_sol_review_receipt_v1(sealed, batch=batch)
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts" / "sol-review", sealed
        )

    def publish_apply(
        self, _core: Mapping[str, Any], *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        raise IsolatedAuthorityPublisherError("direct_writer_apply_signer_disabled")

    @staticmethod
    def _read_result_file(root: Path, name: str) -> dict[str, Any]:
        path = root / name
        try:
            if path.is_symlink() or not path.is_file():
                raise IsolatedAuthorityPublisherError("writer_result_file_invalid")
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise IsolatedAuthorityPublisherError("writer_result_file_invalid") from exc
        if not isinstance(value, Mapping) or "seal" in value or "schema_version" in value:
            raise IsolatedAuthorityPublisherError("writer_result_file_invalid")
        return dict(value)

    @classmethod
    def _read_canonical_result_file(
        cls, root: Path, name: str
    ) -> tuple[dict[str, Any], str]:
        """Reopen one immutable writer result file and bind its exact bytes."""

        path = root / name
        value = cls._read_result_file(root, name)
        try:
            payload = path.read_bytes()
            mode = path.stat().st_mode & 0o777
        except OSError as exc:
            raise IsolatedAuthorityPublisherError(
                "writer_result_file_invalid"
            ) from exc
        if mode != 0o400 or _json_file_bytes(value) != payload:
            raise IsolatedAuthorityPublisherError(
                "writer_result_file_not_immutable"
            )
        return value, hashlib.sha256(payload).hexdigest()

    def _publish_result_artifact(
        self,
        *,
        kind: str,
        schema_version: str,
        purpose: str,
        core: Mapping[str, Any],
    ) -> tuple[str, Path]:
        sealed = self._seal(
            {"schema_version": schema_version, **dict(core)}, purpose=purpose
        )
        return self._publish(
            self.dispatch_root
            / "writer-adapter-receipts"
            / "artifacts"
            / kind,
            sealed,
        )

    def _publish_english_legacy_execution_evidence(
        self,
        root: Path,
        core: Mapping[str, Any],
        *,
        include_recovery_position: bool,
    ) -> dict[str, Any]:
        """Persist the English item process/transaction evidence before signing.

        The writer result directory is not an authority store.  Raw evidence is
        therefore reopened here, checked against the proposed receipt, and
        projected into the same content-addressed HMAC artifact store used by
        the generic deterministic writer path.
        """

        checked = dict(core)
        process_raw, process_raw_sha = self._read_canonical_result_file(
            root, "writer-process-evidence.json"
        )
        transaction_raw, transaction_raw_sha = self._read_canonical_result_file(
            root, "transaction-evidence.json"
        )
        process = checked.get("writer_process")
        transaction = checked.get("transaction_end")
        if not isinstance(process, Mapping) or not isinstance(transaction, Mapping):
            raise IsolatedAuthorityPublisherError(
                "english_writer_execution_evidence_invalid"
            )
        if (
            set(process)
            != {
                "adapter_run_id",
                "pid",
                "terminal_state",
                "exit_code",
                "stopped_at",
                "evidence_sha256",
            }
            or set(transaction)
            != {"transaction_id", "state", "ended_at", "evidence_sha256"}
            or set(process_raw)
            != {
                "adapter_run_id",
                "pid",
                "started_at",
                "stopped_at",
                "terminal_state",
                "exit_code",
                "request_sha256",
                "runtime_state_sha256",
            }
            or set(transaction_raw)
            != {
                "transaction_id",
                "state",
                "target_id",
                "pre_authority_file_sha256",
                "post_authority_file_sha256",
                "pre_object_sha256",
                "post_object_sha256",
                "ended_at",
            }
            or not _is_sha256_hex(process_raw.get("request_sha256"))
            or (
                process_raw.get("runtime_state_sha256") is not None
                and not _is_sha256_hex(process_raw.get("runtime_state_sha256"))
            )
            or any(
                not _is_sha256_hex(transaction_raw.get(field))
                for field in (
                    "pre_authority_file_sha256",
                    "post_authority_file_sha256",
                    "pre_object_sha256",
                    "post_object_sha256",
                )
            )
        ):
            raise IsolatedAuthorityPublisherError(
                "english_writer_execution_evidence_invalid"
            )
        if (
            process.get("evidence_sha256") != process_raw_sha
            or process.get("adapter_run_id") != process_raw.get("adapter_run_id")
            or process.get("pid") != process_raw.get("pid")
            or process.get("terminal_state") != process_raw.get("terminal_state")
            or process.get("exit_code") != process_raw.get("exit_code")
            or process.get("stopped_at") != process_raw.get("stopped_at")
            or transaction.get("evidence_sha256") != transaction_raw_sha
            or transaction.get("transaction_id")
            != transaction_raw.get("transaction_id")
            or transaction.get("state") != transaction_raw.get("state")
            or transaction.get("ended_at") != transaction_raw.get("ended_at")
            or transaction_raw.get("target_id") != checked.get("target_id")
            or (
                "pre_object_sha256" in checked
                and transaction_raw.get("pre_object_sha256")
                != checked.get("pre_object_sha256")
            )
            or (
                "post_object_sha256" in checked
                and transaction_raw.get("post_object_sha256")
                != checked.get("post_object_sha256")
            )
        ):
            raise IsolatedAuthorityPublisherError(
                "english_writer_execution_evidence_mismatch"
            )
        batch_id = checked.get("batch_id")
        fence = checked.get("fencing_token")
        process_sha, _ = self._publish_result_artifact(
            kind="writer-process",
            schema_version="isolated_writer_process_artifact_v1",
            purpose="isolated-writer-process-artifact",
            core={
                "batch_id": batch_id,
                "subject": "english",
                "fencing_token": fence,
                "adapter_run_id": process["adapter_run_id"],
                "pid": process["pid"],
                "terminal_state": process["terminal_state"],
                "exit_code": process["exit_code"],
                "stopped_at": process["stopped_at"],
                "formal_write_count": 0,
            },
        )
        transaction_sha, _ = self._publish_result_artifact(
            kind="transaction-end",
            schema_version="isolated_writer_transaction_artifact_v1",
            purpose="isolated-writer-transaction-artifact",
            core={
                "batch_id": batch_id,
                "subject": "english",
                "fencing_token": fence,
                "transaction_id": transaction["transaction_id"],
                "state": transaction["state"],
                "ended_at": transaction["ended_at"],
                "formal_write_count": 0,
            },
        )
        checked["writer_process"] = {
            **dict(process),
            "evidence_sha256": process_sha,
        }
        checked["transaction_end"] = {
            **dict(transaction),
            "evidence_sha256": transaction_sha,
        }
        if include_recovery_position:
            recovery, recovery_raw_sha = self._read_canonical_result_file(
                root, "recovery-position.json"
            )
            if (
                set(recovery)
                != {
                    "copy_id",
                    "target_id",
                    "pre_authority_file_sha256",
                    "pre_object_sha256",
                    "rollback_snapshot_sha256",
                    "transaction_state",
                }
                or not _is_sha256_hex(recovery.get("pre_authority_file_sha256"))
                or not _is_sha256_hex(recovery.get("pre_object_sha256"))
                or (
                    recovery.get("rollback_snapshot_sha256") is not None
                    and not _is_sha256_hex(
                        recovery.get("rollback_snapshot_sha256")
                    )
                )
            ):
                raise IsolatedAuthorityPublisherError(
                    "english_writer_recovery_evidence_invalid"
                )
            if (
                checked.get("recovery_position_sha256") != recovery_raw_sha
                or recovery.get("target_id") != checked.get("target_id")
                or recovery.get("transaction_state") != transaction["state"]
                or recovery.get("pre_authority_file_sha256")
                != transaction_raw.get("pre_authority_file_sha256")
                or recovery.get("pre_object_sha256")
                != transaction_raw.get("pre_object_sha256")
            ):
                raise IsolatedAuthorityPublisherError(
                    "english_writer_recovery_evidence_mismatch"
                )
            rollback_sha = recovery.get("rollback_snapshot_sha256")
            if rollback_sha is not None:
                rollback_path = root / "rollback-snapshot.bin"
                try:
                    rollback_payload = rollback_path.read_bytes()
                except OSError as exc:
                    raise IsolatedAuthorityPublisherError(
                        "english_writer_rollback_evidence_missing"
                    ) from exc
                if (
                    rollback_path.is_symlink()
                    or not rollback_path.is_file()
                    or rollback_path.stat().st_mode & 0o777 != 0o400
                    or hashlib.sha256(rollback_payload).hexdigest() != rollback_sha
                ):
                    raise IsolatedAuthorityPublisherError(
                        "english_writer_rollback_evidence_mismatch"
                    )
            recovery_sha, _ = self._publish_result_artifact(
                kind="recovery-position",
                schema_version="isolated_writer_recovery_position_artifact_v1",
                purpose="isolated-writer-recovery-position-artifact",
                core={
                    "batch_id": batch_id,
                    "subject": "english",
                    "fencing_token": fence,
                    "writer_process_artifact_sha256": process_sha,
                    "transaction_artifact_sha256": transaction_sha,
                    "state_sha256": recovery_raw_sha,
                    "formal_write_count": 0,
                },
            )
            checked["recovery_position_sha256"] = recovery_sha
        return checked

    def publish_apply_from_execution_result_directory(
        self, result_dir: Path, *, batch: Mapping[str, Any]
    ) -> tuple[str, Path]:
        """Seal an apply receipt only after reopening one isolated run directory."""

        root = result_dir.resolve()
        if result_dir.is_symlink() or not root.is_dir():
            raise IsolatedAuthorityPublisherError("writer_result_directory_invalid")
        process = self._read_result_file(root, "writer-process.json")
        transaction = self._read_result_file(root, "transaction-end.json")
        operations = self._read_result_file(root, "operations.json")
        result = self._read_result_file(root, "result.json")
        process_sha, _ = self._publish_result_artifact(
            kind="writer-process",
            schema_version="isolated_writer_process_artifact_v1",
            purpose="isolated-writer-process-artifact",
            core=process,
        )
        transaction_sha, _ = self._publish_result_artifact(
            kind="transaction-end",
            schema_version="isolated_writer_transaction_artifact_v1",
            purpose="isolated-writer-transaction-artifact",
            core=transaction,
        )
        operations_sha, _ = self._publish_result_artifact(
            kind="operations",
            schema_version="isolated_writer_operations_artifact_v1",
            purpose="isolated-writer-operations-artifact",
            core=operations,
        )
        status = result.get("status")
        recovery_sha = rollback_sha = None
        if status == "failed":
            recovery = self._read_result_file(root, "recovery-position.json")
            if set(recovery) != {"state_sha256"}:
                raise IsolatedAuthorityPublisherError("writer_recovery_shape_invalid")
            recovery_sha, _ = self._publish_result_artifact(
                kind="recovery-position",
                schema_version="isolated_writer_recovery_position_artifact_v1",
                purpose="isolated-writer-recovery-position-artifact",
                core={
                    "batch_id": result.get("batch_id"),
                    "subject": result.get("subject"),
                    "fencing_token": result.get("fencing_token"),
                    "writer_process_artifact_sha256": process_sha,
                    "transaction_artifact_sha256": transaction_sha,
                    "state_sha256": recovery["state_sha256"],
                    "formal_write_count": 0,
                },
            )
        elif status == "rolled_back":
            rollback = self._read_result_file(root, "rollback.json")
            if set(rollback) != {"state_sha256"}:
                raise IsolatedAuthorityPublisherError("writer_rollback_shape_invalid")
            rollback_sha, _ = self._publish_result_artifact(
                kind="rollback",
                schema_version="isolated_writer_rollback_artifact_v1",
                purpose="isolated-writer-rollback-artifact",
                core={
                    "batch_id": result.get("batch_id"),
                    "subject": result.get("subject"),
                    "fencing_token": result.get("fencing_token"),
                    "writer_process_artifact_sha256": process_sha,
                    "transaction_artifact_sha256": transaction_sha,
                    "state_sha256": rollback["state_sha256"],
                    "formal_write_count": 0,
                },
            )
        expected_result_keys = {
            "batch_id", "subject", "fencing_token", "writer", "writer_adapter",
            "sol_review_receipt_sha256", "status", "dispatcher_effect",
            "completed_at",
        }
        if set(result) != expected_result_keys:
            raise IsolatedAuthorityPublisherError("writer_result_shape_invalid")
        execution = self._seal(
            {
                "schema_version": "isolated_writer_execution_result_v1",
                **result,
                "writer_process_artifact_sha256": process_sha,
                "transaction_artifact_sha256": transaction_sha,
                "operations_artifact_sha256": operations_sha,
                "recovery_position_artifact_sha256": recovery_sha,
                "rollback_artifact_sha256": rollback_sha,
            },
            purpose="isolated-writer-execution-result",
        )
        execution_sha, _ = self._publish(
            self.dispatch_root
            / "writer-adapter-receipts"
            / "execution-result",
            execution,
        )
        store = SubjectSolRuntimeStore(self.runtime_root)
        _, core = store._verify_writer_execution_result(execution_sha, batch=batch)
        is_v2 = batch.get("schema_version") == DAILY_SOL_BATCH_V3_SCHEMA
        sealed = self._seal(
            {
                "schema_version": (
                    SOL_COMMIT_RECEIPT_V2_SCHEMA
                    if is_v2
                    else "sol_commit_receipt_v1"
                ),
                **core,
            },
            purpose=(
                "deterministic-writer-apply-receipt-v2"
                if is_v2
                else "deterministic-writer-apply-receipt"
            ),
        )
        if is_v2:
            validate_sol_commit_receipt_v2(sealed, batch=batch)
        else:
            validate_sol_commit_receipt_v1(sealed, batch=batch)
        return self._publish(
            self.dispatch_root / "writer-adapter-receipts" / "writer-apply", sealed
        )

    def publish_post_commit_authority_closure(
        self, *, subject: str, batch_id: str, subject_luna_batch_id: str,
        source_generation: str, authority_observation_sha256: str,
        commit_receipt_sha256: str,
        acknowledged_at: str,
    ) -> dict[str, str]:
        observation_sha = str(authority_observation_sha256)
        observation_path = (
            self.dispatch_root / "subject-authority-observations" / "sha256"
            / observation_sha[:2] / f"{observation_sha}.json"
        )
        payload = observation_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != observation_sha:
            raise IsolatedAuthorityPublisherError("authority_observation_hash_mismatch")
        observation = json.loads(payload.decode("utf-8"))
        if not isinstance(observation, Mapping):
            raise IsolatedAuthorityPublisherError("authority_observation_invalid")
        observer = _Publisher(
            self.runtime_root, key_name="subject-mcp-authority-observer.key"
        )
        document = dict(observation)
        seal = document.pop("seal", None)
        expected = observer._seal(
            document, purpose="subject-mcp-authority-observation"
        ).get("seal")
        if seal != expected:
            raise IsolatedAuthorityPublisherError("authority_observation_hmac_invalid")
        if (
            observation.get("schema_version") != SUBJECT_AUTHORITY_OBSERVATION_SCHEMA
            or observation.get("subject") != subject
            or observation.get("formal_write_count") != 0
        ):
            raise IsolatedAuthorityPublisherError("authority_observation_invalid")
        next_generation = str(observation.get("generation") or "")
        next_authority_fingerprint = str(
            observation.get("authority_fingerprint") or ""
        )
        ack = self._seal(
            {
                "schema_version": SUBJECT_GENERATION_AUTHORITY_ACK_SCHEMA,
                "subject": subject,
                "batch_id": batch_id,
                "subject_luna_batch_id": subject_luna_batch_id,
                "source_generation": source_generation,
                "next_generation": next_generation,
                "next_authority_fingerprint": next_authority_fingerprint,
                "authority_observation_sha256": observation_sha,
                "commit_receipt_sha256": commit_receipt_sha256,
                "acknowledged_at": acknowledged_at,
                "formal_write_count": 0,
            },
            purpose="writer-adapter-subject-generation-authority-ack",
        )
        ack_sha, _ = self._publish(
            self.dispatch_root / "subject-generation-acks", ack
        )
        return {
            "authority_observation_sha256": observation_sha,
            "authority_ack_sha256": ack_sha,
        }


class SubjectAuthorityObserverPublisher(_Publisher):
    """Independent read-only observer for the post-commit MCP authority."""

    def __init__(self, runtime_root: Path, processing_host: Any) -> None:
        super().__init__(
            runtime_root, key_name="subject-mcp-authority-observer.key"
        )
        self.processing_host = processing_host

    def observe(self, subject: str, *, observed_at: str) -> tuple[str, Path]:
        snapshot = self.processing_host.subject_authority_snapshot(subject)
        if (
            not isinstance(snapshot, Mapping)
            or snapshot.get("subject") != subject
            or not isinstance(snapshot.get("generation"), str)
            or not snapshot.get("generation")
            or not isinstance(snapshot.get("authority_fingerprint"), str)
            or len(str(snapshot.get("authority_fingerprint"))) != 64
            or snapshot.get("model_call_count") != 0
            or snapshot.get("formal_write_count") != 0
        ):
            raise IsolatedAuthorityPublisherError("observed_subject_authority_invalid")
        sealed = self._seal(
            {
                "schema_version": SUBJECT_AUTHORITY_OBSERVATION_SCHEMA,
                "subject": subject,
                "generation": snapshot["generation"],
                "authority_fingerprint": snapshot["authority_fingerprint"],
                "observed_at": observed_at,
                "model_call_count": 0,
                "formal_write_count": 0,
            },
            purpose="subject-mcp-authority-observation",
        )
        return self._publish(
            self.dispatch_root / "subject-authority-observations", sealed
        )
