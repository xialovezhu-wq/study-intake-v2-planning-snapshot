#!/usr/bin/env python3
"""Review and execute the signed EN-P0-006 disposition contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import sys
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from english_legacy_disposition import (  # noqa: E402
    BATCH_INTENT_SCHEMA,
    AUTHORIZED_OPERATIONS,
    DISPOSITIONS,
    EnglishLegacyDispositionError,
    EnglishLegacyDispositionStore,
    EnglishLegacyDispositionV3Store,
    ROLLOUT_PATH,
    build_complete_inventory_v3_draft,
    build_inventory_independent_review_receipt_draft,
    build_review_template,
    load_json,
)
from processing_plugin import ProcessingPluginError, ProcessingPluginHost  # noqa: E402


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _live_english_mcp_authority(config_path: Path) -> dict[str, Any]:
    config = load_json(config_path, "legacy_batch_candidate_config_invalid")
    processing_plugin = config.get("processing_plugin")
    runtime_root = config.get("runtime_root")
    candidate_release_id = config.get("authority_release_id")
    if candidate_release_id is None:
        release = config.get("release")
        manifest_path = (
            Path(str(release.get("manifest_path")))
            if isinstance(release, Mapping)
            and isinstance(release.get("manifest_path"), str)
            else None
        )
        if manifest_path is not None:
            manifest = load_json(
                manifest_path,
                "legacy_batch_candidate_config_invalid",
            )
            candidate_release_id = manifest.get("release_id")
    if (
        not isinstance(processing_plugin, Mapping)
        or not isinstance(runtime_root, str)
        or not runtime_root
        or not isinstance(candidate_release_id, str)
        or len(candidate_release_id) != 64
    ):
        raise EnglishLegacyDispositionError(
            "legacy_batch_candidate_config_invalid"
        )
    try:
        host = ProcessingPluginHost(
            processing_plugin,
            runtime_root=Path(runtime_root),
            candidate_release_id=candidate_release_id,
        )
        authority = host.subject_authority_snapshot("english")
    except ProcessingPluginError as exc:
        raise EnglishLegacyDispositionError(exc.code) from exc
    if (
        authority.get("schema_version") != "subject_authority_snapshot_v1"
        or authority.get("subject") != "english"
        or authority.get("model_call_count") != 0
        or authority.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError(
            "legacy_batch_mcp_authority_snapshot_invalid"
        )
    return authority


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        allow_abbrev=False,
        description=(
            "Create or verify proposal-only EN-P0-006 control receipts. "
            "This command never changes an English formal source."
        )
    )
    sub = root.add_subparsers(dest="command", required=True)

    initialize_key = sub.add_parser(
        "initialize-authority-key",
        allow_abbrev=False,
        help="provision the isolated EN-P0-006 batch authority key",
    )
    initialize_key.add_argument("--authority-key", type=Path, required=True)

    template = sub.add_parser("render-review-template")
    template.add_argument("--requirements", type=Path, required=True)

    seal = sub.add_parser("seal-complete-inventory")
    seal.add_argument("--inventory-draft", type=Path, required=True)
    seal.add_argument("--receipt-root", type=Path, required=True)
    seal.add_argument("--authority-key", type=Path, required=True)

    issue = sub.add_parser("issue-target")
    issue.add_argument("--inventory-sha256", required=True)
    issue.add_argument("--target-id", required=True)
    issue.add_argument("--disposition", choices=DISPOSITIONS, required=True)
    issue.add_argument("--user-authorization", type=Path, required=True)
    issue.add_argument("--receipt-root", type=Path, required=True)
    issue.add_argument("--authority-key", type=Path, required=True)
    issue.add_argument("--issued-at")

    close = sub.add_parser("issue-closure")
    close.add_argument("--inventory-sha256", required=True)
    close.add_argument("--receipt-sha256", action="append", required=True)
    close.add_argument("--receipt-root", type=Path, required=True)
    close.add_argument("--authority-key", type=Path, required=True)
    close.add_argument("--issued-at")

    verify = sub.add_parser("verify-closure")
    verify.add_argument("--closure-sha256", required=True)
    verify.add_argument("--receipt-root", type=Path, required=True)
    verify.add_argument("--authority-key", type=Path, required=True)

    build_v3 = sub.add_parser("build-complete-inventory-v3")
    build_v3.add_argument("--rollout", type=Path, default=ROLLOUT_PATH)
    build_v3.add_argument("--inventory-id", required=True)
    build_v3.add_argument("--independent-review-receipt-sha256", required=True)
    build_v3.add_argument("--issued-at", required=True)
    build_v3.add_argument("--mcp-authority-generation", required=True)
    build_v3.add_argument("--mcp-authority-fingerprint", required=True)

    review_draft = sub.add_parser("build-inventory-review-receipt")
    review_draft.add_argument("--rollout", type=Path, default=ROLLOUT_PATH)
    review_draft.add_argument("--review-id", required=True)
    review_draft.add_argument("--reviewer-identity", required=True)
    review_draft.add_argument("--reviewed-at", required=True)
    review_draft.add_argument("--mcp-authority-generation", required=True)
    review_draft.add_argument("--mcp-authority-fingerprint", required=True)

    seal_review = sub.add_parser("seal-inventory-review-receipt")
    seal_review.add_argument("--review-draft", type=Path, required=True)
    seal_review.add_argument("--receipt-root", type=Path, required=True)
    seal_review.add_argument("--authority-key", type=Path, required=True)

    verify_review = sub.add_parser("verify-inventory-review-receipt")
    verify_review.add_argument("--review-receipt-sha256", required=True)
    verify_review.add_argument("--receipt-root", type=Path, required=True)
    verify_review.add_argument("--authority-key", type=Path, required=True)

    seal_v3 = sub.add_parser("seal-complete-inventory-v3")
    seal_v3.add_argument("--inventory-draft", type=Path, required=True)
    seal_v3.add_argument("--receipt-root", type=Path, required=True)
    seal_v3.add_argument("--authority-key", type=Path, required=True)

    intent = sub.add_parser("seal-batch-intent")
    intent.add_argument("--batch-intent", type=Path, required=True)
    intent.add_argument("--receipt-root", type=Path, required=True)
    intent.add_argument("--authority-key", type=Path, required=True)

    materialize = sub.add_parser("materialize-batch-authorization")
    materialize.add_argument("--intent-sha256", required=True)
    materialize.add_argument("--inventory-sha256", required=True)
    materialize.add_argument("--receipt-root", type=Path, required=True)
    materialize.add_argument("--authority-key", type=Path, required=True)

    expand = sub.add_parser("expand-batch-authorization")
    expand.add_argument("--batch-authorization-sha256", required=True)
    expand.add_argument("--receipt-root", type=Path, required=True)
    expand.add_argument("--authority-key", type=Path, required=True)

    verify_expansion = sub.add_parser("verify-authorization-expansion")
    verify_expansion.add_argument(
        "--authorization-expansion-closure-sha256", required=True
    )
    verify_expansion.add_argument("--receipt-root", type=Path, required=True)
    verify_expansion.add_argument("--authority-key", type=Path, required=True)

    verify_existing = sub.add_parser(
        "verify-existing-inventory-closure",
        allow_abbrev=False,
        help=(
            "verify the 98 existing objects and historical per-target HMAC "
            "evidence without Luna or Sol"
        ),
    )
    verify_existing.add_argument(
        "--authorization-expansion-closure-sha256", required=True
    )
    verify_existing.add_argument("--receipt-root", type=Path, required=True)
    verify_existing.add_argument("--authority-key", type=Path, required=True)

    remediation = sub.add_parser("evaluate-remediation-gate")
    remediation.add_argument(
        "--authorization-expansion-closure-sha256", required=True
    )
    remediation.add_argument("--receipt-root", type=Path, required=True)
    remediation.add_argument("--authority-key", type=Path, required=True)

    materialize_all = sub.add_parser(
        "materialize-remediation-authorization",
        allow_abbrev=False,
    )
    materialize_all.add_argument("--rollout", type=Path, default=ROLLOUT_PATH)
    materialize_all.add_argument("--inventory-id", required=True)
    materialize_all.add_argument("--review-id", required=True)
    materialize_all.add_argument("--reviewer-identity", required=True)
    materialize_all.add_argument("--user-message", type=Path, required=True)
    materialize_all.add_argument("--source-thread-id", required=True)
    materialize_all.add_argument("--source-turn-id", required=True)
    materialize_all.add_argument("--authorized-at", required=True)
    materialize_all.add_argument("--issued-at", required=True)
    materialize_all.add_argument("--config", type=Path, required=True)
    materialize_all.add_argument("--receipt-root", type=Path, required=True)
    materialize_all.add_argument("--authority-key", type=Path, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "initialize-authority-key":
            key_path = args.authority_key.expanduser().absolute()
            if (
                not key_path.is_absolute()
                or key_path.name != "english-legacy-batch-authorization.key"
                or key_path.parent.name != "external-authorities"
                or key_path.parent.parent.name != "state"
                or key_path.parent.parent.parent.name != "dispatch"
                or key_path in {Path("/"), Path.home()}
            ):
                raise EnglishLegacyDispositionError(
                    "legacy_authority_key_target_invalid"
                )
            key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if key_path.parent.is_symlink():
                raise EnglishLegacyDispositionError(
                    "legacy_authority_key_target_invalid"
                )
            created = False
            try:
                descriptor = os.open(
                    key_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                descriptor = None
            else:
                created = True
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(secrets.token_bytes(32))
                    handle.flush()
                    os.fsync(handle.fileno())
            try:
                node = key_path.lstat()
                key = key_path.read_bytes()
            except OSError as exc:
                raise EnglishLegacyDispositionError(
                    "legacy_authority_key_unavailable"
                ) from exc
            if (
                key_path.is_symlink()
                or not stat.S_ISREG(node.st_mode)
                or stat.S_IMODE(node.st_mode) != 0o600
                or len(key) != 32
            ):
                raise EnglishLegacyDispositionError(
                    "legacy_authority_key_invalid"
                )
            _print(
                {
                    "status": "created" if created else "already_initialized",
                    "authority_key_path": str(key_path),
                    "authority_key_id": hashlib.sha256(key).hexdigest(),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0

        if args.command == "materialize-remediation-authorization":
            raise EnglishLegacyDispositionError(
                "legacy_recuration_authorization_revoked"
            )
        if args.command == "materialize-remediation-authorization":
            message_path = args.user_message.expanduser().absolute()
            try:
                node = message_path.lstat()
                message_bytes = message_path.read_bytes()
            except OSError as exc:
                raise EnglishLegacyDispositionError(
                    "legacy_batch_user_message_invalid"
                ) from exc
            if (
                message_path.is_symlink()
                or not message_path.is_file()
                or node.st_size <= 0
                or node.st_size > 1024 * 1024
            ):
                raise EnglishLegacyDispositionError(
                    "legacy_batch_user_message_invalid"
                )
            store_v3 = EnglishLegacyDispositionV3Store(
                args.receipt_root,
                args.authority_key,
            )
            mcp_authority = _live_english_mcp_authority(args.config)
            review_core = build_inventory_independent_review_receipt_draft(
                review_id=args.review_id,
                reviewer_identity=args.reviewer_identity,
                reviewed_at=args.issued_at,
                mcp_authority_generation=mcp_authority["generation"],
                mcp_authority_fingerprint=mcp_authority[
                    "authority_fingerprint"
                ],
                rollout_path=args.rollout,
            )
            review_sha, review_path, _review = (
                store_v3.seal_inventory_independent_review_receipt(review_core)
            )
            inventory_core = build_complete_inventory_v3_draft(
                rollout_path=args.rollout,
                inventory_id=args.inventory_id,
                independent_review_receipt_sha256=review_sha,
                issued_at=args.issued_at,
                mcp_authority_generation=mcp_authority["generation"],
                mcp_authority_fingerprint=mcp_authority[
                    "authority_fingerprint"
                ],
            )
            inventory_sha, inventory_path, inventory = store_v3.seal_inventory_v3(
                inventory_core
            )
            intent_core = {
                "schema_version": BATCH_INTENT_SCHEMA,
                "issue_id": "EN-P0-006",
                "subject": "english",
                "intent_id": f"EN-P0-006-INTENT-{hashlib.sha256(message_bytes).hexdigest()[:20]}",
                "authorization_type": "explicit_user_batch_authorization",
                "inventory_scope": "first_independently_verified_complete_inventory",
                "disposition": "deterministic_recuration",
                "authorized_operations": list(AUTHORIZED_OPERATIONS),
                "exact_inventory_only": True,
                "one_shot": True,
                "user_message_sha256": hashlib.sha256(message_bytes).hexdigest(),
                "source_thread_id": args.source_thread_id,
                "source_turn_id": args.source_turn_id,
                "authorized_at": args.authorized_at,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            intent_sha, intent_path, _intent = store_v3.seal_batch_intent(
                intent_core
            )
            batch_sha, batch_path, _batch = (
                store_v3.materialize_batch_authorization(
                    intent_sha256=intent_sha,
                    inventory_sha256=inventory_sha,
                )
            )
            closure_sha, closure_path, closure = (
                store_v3.expand_batch_authorization(batch_sha)
            )
            gate = store_v3.evaluate_remediation_gate(closure_sha)
            _print(
                {
                    "status": "authorization_ready_execution_pending",
                    "inventory_review_receipt_sha256": review_sha,
                    "inventory_review_receipt_path": str(review_path),
                    "inventory_sha256": inventory_sha,
                    "inventory_path": str(inventory_path),
                    "target_set_sha256": inventory["target_set_sha256"],
                    "target_count": inventory["target_count"],
                    "mcp_authority_snapshot": mcp_authority,
                    "batch_intent_sha256": intent_sha,
                    "batch_intent_path": str(intent_path),
                    "batch_authorization_sha256": batch_sha,
                    "batch_authorization_path": str(batch_path),
                    "authorization_expansion_closure_sha256": closure_sha,
                    "authorization_expansion_closure_path": str(closure_path),
                    "event_count": len(closure["target_authorizations"]),
                    "receipt_count": len(closure["target_authorizations"]),
                    "remediation_gate": gate,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0

        if args.command == "render-review-template":
            requirements = load_json(
                args.requirements,
                "legacy_requirements_invalid",
            )
            _print(build_review_template(requirements))
            return 0

        if args.command == "build-complete-inventory-v3":
            _print(
                build_complete_inventory_v3_draft(
                    rollout_path=args.rollout,
                    inventory_id=args.inventory_id,
                    independent_review_receipt_sha256=(
                        args.independent_review_receipt_sha256
                    ),
                    issued_at=args.issued_at,
                    mcp_authority_generation=args.mcp_authority_generation,
                    mcp_authority_fingerprint=args.mcp_authority_fingerprint,
                )
            )
            return 0

        if args.command == "build-inventory-review-receipt":
            _print(
                build_inventory_independent_review_receipt_draft(
                    review_id=args.review_id,
                    reviewer_identity=args.reviewer_identity,
                    reviewed_at=args.reviewed_at,
                    mcp_authority_generation=args.mcp_authority_generation,
                    mcp_authority_fingerprint=args.mcp_authority_fingerprint,
                    rollout_path=args.rollout,
                )
            )
            return 0

        if args.command in {
            "seal-complete-inventory-v3",
            "seal-inventory-review-receipt",
            "verify-inventory-review-receipt",
            "seal-batch-intent",
            "materialize-batch-authorization",
            "expand-batch-authorization",
            "verify-authorization-expansion",
            "verify-existing-inventory-closure",
            "evaluate-remediation-gate",
        }:
            store_v3 = EnglishLegacyDispositionV3Store(
                args.receipt_root,
                args.authority_key,
            )
            if args.command in {
                "seal-batch-intent",
                "materialize-batch-authorization",
                "expand-batch-authorization",
            }:
                raise EnglishLegacyDispositionError(
                    "legacy_recuration_authorization_revoked"
                )
            if args.command == "seal-inventory-review-receipt":
                core = load_json(
                    args.review_draft, "legacy_inventory_review_invalid"
                )
                digest, path, receipt = (
                    store_v3.seal_inventory_independent_review_receipt(core)
                )
                _print(
                    {
                        "status": "sealed",
                        "review_receipt_sha256": digest,
                        "review_receipt_path": str(path),
                        "target_count": receipt["target_count"],
                        "target_set_sha256": receipt["target_set_sha256"],
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "verify-inventory-review-receipt":
                receipt = store_v3.verify_inventory_independent_review_receipt(
                    args.review_receipt_sha256
                )
                _print(
                    {
                        "status": "verified_complete",
                        "review_receipt_sha256": args.review_receipt_sha256,
                        "target_count": receipt["target_count"],
                        "target_set_sha256": receipt["target_set_sha256"],
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "seal-complete-inventory-v3":
                core = load_json(
                    args.inventory_draft, "legacy_inventory_draft_invalid"
                )
                digest, path, inventory = store_v3.seal_inventory_v3(core)
                _print(
                    {
                        "status": "sealed",
                        "inventory_sha256": digest,
                        "inventory_path": str(path),
                        "target_count": inventory["target_count"],
                        "target_set_sha256": inventory["target_set_sha256"],
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "seal-batch-intent":
                core = load_json(
                    args.batch_intent, "legacy_batch_intent_invalid"
                )
                if core.get("schema_version") != BATCH_INTENT_SCHEMA:
                    raise EnglishLegacyDispositionError(
                        "legacy_batch_intent_invalid"
                    )
                digest, path, _intent = store_v3.seal_batch_intent(core)
                _print(
                    {
                        "status": "sealed",
                        "intent_sha256": digest,
                        "intent_path": str(path),
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "materialize-batch-authorization":
                digest, path, authorization = (
                    store_v3.materialize_batch_authorization(
                        intent_sha256=args.intent_sha256,
                        inventory_sha256=args.inventory_sha256,
                    )
                )
                _print(
                    {
                        "status": "materialized",
                        "batch_authorization_sha256": digest,
                        "batch_authorization_path": str(path),
                        "target_count": authorization["target_count"],
                        "target_set_sha256": authorization["target_set_sha256"],
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "expand-batch-authorization":
                digest, path, closure = store_v3.expand_batch_authorization(
                    args.batch_authorization_sha256
                )
                _print(
                    {
                        "status": "verified_complete",
                        "authorization_expansion_closure_sha256": digest,
                        "authorization_expansion_closure_path": str(path),
                        "batch_authorization_sha256": closure[
                            "batch_authorization_sha256"
                        ],
                        "target_count": closure["target_count"],
                        "event_count": len(closure["target_authorizations"]),
                        "receipt_count": len(closure["target_authorizations"]),
                        "target_authorizations": closure[
                            "target_authorizations"
                        ],
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                )
                return 0
            if args.command == "verify-authorization-expansion":
                _print(
                    store_v3.verify_authorization_expansion_closure(
                        args.authorization_expansion_closure_sha256
                    )
                )
                return 0
            if args.command == "verify-existing-inventory-closure":
                _print(
                    store_v3.verify_existing_inventory_no_recuration_required(
                        args.authorization_expansion_closure_sha256
                    )
                )
                return 0
            _print(
                store_v3.evaluate_remediation_gate(
                    args.authorization_expansion_closure_sha256
                )
            )
            return 0

        store = EnglishLegacyDispositionStore(
            args.receipt_root,
            args.authority_key,
        )
        if args.command == "seal-complete-inventory":
            core = load_json(args.inventory_draft, "legacy_inventory_draft_invalid")
            digest, path, inventory = store.seal_inventory(core)
            _print(
                {
                    "status": "sealed",
                    "inventory_sha256": digest,
                    "inventory_path": str(path),
                    "target_count": inventory["target_count"],
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0
        if args.command == "issue-target":
            event = load_json(
                args.user_authorization,
                "legacy_user_authorization_invalid",
            )
            digest, path, receipt = store.issue_target_receipt(
                args.inventory_sha256,
                target_id=args.target_id,
                disposition=args.disposition,
                user_authorization=event,
                issued_at=args.issued_at,
            )
            _print(
                {
                    "status": "issued",
                    "receipt_sha256": digest,
                    "receipt_path": str(path),
                    "target_id": receipt["target"]["target_id"],
                    "disposition": receipt["disposition"],
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0
        if args.command == "issue-closure":
            digest, path, closure = store.issue_closure(
                args.inventory_sha256,
                args.receipt_sha256,
                issued_at=args.issued_at,
            )
            _print(
                {
                    "status": "issued",
                    "closure_sha256": digest,
                    "closure_path": str(path),
                    "target_count": closure["target_count"],
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0
        result = store.verify_closure(args.closure_sha256)
        _print(result)
        return 0
    except EnglishLegacyDispositionError as exc:
        _print(
            {
                "status": "failed_closed",
                "error_code": exc.code,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
