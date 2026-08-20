#!/usr/bin/env python3
"""Inject one post-replace fault against a complete isolated English copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "lib"))

import subject_sol_contract as contract  # noqa: E402
from english_legacy_writer_adapter import (  # noqa: E402
    EnglishLegacyWriterError,
    REQUEST_SCHEMA,
    _locate,
    _utc_now,
    _validate_marker,
    _value_sha256,
    authority_manifest,
    execute_request,
    source_unchanged,
)
from isolated_authority_publishers import IsolatedWriterAdapterPublisher  # noqa: E402
from validate_english_legacy_writer_target_set import (  # noqa: E402
    _bootstrap_runtime,
    _publish_batch,
    _safe_new_root,
)


def _digest(value: object) -> str:
    return hashlib.sha256(contract._canonical_bytes(value)).hexdigest()


def _batch(target_id: str, current_sha: str, desired_sha: str) -> dict:
    kind, _record_id = target_id.split(":", 1)
    authority = _digest("isolated-fault-authority")
    item = {
        "schema_version": "english_legacy_sol_work_item_v1",
        "parent_batch_authorization_sha256": _digest("isolated-fault-parent"),
        "inventory_sha256": _digest("isolated-fault-inventory"),
        "authorization_event_sha256": _digest("isolated-fault-event"),
        "target_authorization_receipt_sha256": _digest("isolated-fault-target-auth"),
        "target_id": target_id,
        "target_kind": kind,
        "ordinal": 1,
        "current_object_sha256": current_sha,
        "work_item_sha256": _digest("isolated-fault-work"),
        "authority_generation": "isolated-fault-generation",
        "authority_fingerprint": authority,
        "proposal_sha256": _digest("isolated-fault-proposal"),
        "package_sha256": _digest("isolated-fault-package"),
        "quality_receipt_sha256": _digest("isolated-fault-quality"),
        "status": "quality_passed",
        "luna_terminal": True,
        "quality_passed": True,
        "quality_outcome": "accepted",
        "proposed_action": "update_existing_proposal",
        "model_call_count": 2,
        "formal_write_count": 0,
        "idempotency_key": f"isolated-fault:{target_id}",
    }
    return contract.validate_english_legacy_recuration_sol_batch_v1(
        {
            "schema_version": "english_legacy_recuration_sol_batch_v1",
            "issue_id": "EN-P0-006",
            "batch_id": "EN-P0-006-ISOLATED-FAULT-RECOVERY",
            "subject": "english",
            "batch_authorization_sha256": item[
                "parent_batch_authorization_sha256"
            ],
            "authorization_expansion_closure_sha256": _digest(
                "isolated-fault-closure"
            ),
            "inventory_sha256": item["inventory_sha256"],
            "target_set_sha256": _digest("isolated-fault-target-set"),
            "target_count": 1,
            "authority_generation": item["authority_generation"],
            "authority_fingerprint": authority,
            "authorized_at": "2026-08-09T08:00:00Z",
            "work_items": [item],
            "status": "authorized",
            "formal_write_count": 0,
        }
    )


def run(args: argparse.Namespace) -> dict:
    isolated = args.isolated_root.resolve()
    marker = _validate_marker(isolated, args.copy_id)
    run_root = isolated.parent.resolve()
    runtime = _safe_new_root(
        args.runtime_root, run_root=run_root,
        code="english_writer_fault_runtime_invalid",
    )
    results = _safe_new_root(
        args.results_root, run_root=run_root,
        code="english_writer_fault_results_invalid",
    )
    report_path = args.report.resolve()
    try:
        report_path.relative_to(run_root)
    except ValueError as exc:
        raise EnglishLegacyWriterError("english_writer_fault_report_invalid") from exc
    if report_path.exists():
        raise EnglishLegacyWriterError("english_writer_fault_report_invalid")
    kind, record_id = args.target_id.split(":", 1)
    located = _locate(
        isolated, {"target_id": args.target_id, "target_kind": kind}
    )
    if kind != "master_bank_row":
        raise EnglishLegacyWriterError("english_writer_fault_target_not_master_row")
    desired = dict(located["object"])
    desired["review_note"] = (
        str(desired.get("review_note") or "")
        + " [isolated fault injection; must not persist]"
    )
    current_sha = str(located["object_sha256"])
    desired_sha = _value_sha256(desired)
    batch = _batch(args.target_id, current_sha, desired_sha)
    runtime.mkdir(parents=True, mode=0o700)
    results.mkdir(parents=True, mode=0o700)
    batch_sha, batch_path = _publish_batch(runtime, batch)
    _bootstrap_runtime(runtime, batch, batch_sha)
    store = contract.SubjectSolRuntimeStore(runtime)
    publisher = IsolatedWriterAdapterPublisher(runtime)
    copy_pre = authority_manifest(isolated)["manifest_sha256"]
    review_core = {
        "issue_id": "EN-P0-006",
        "batch_id": batch["batch_id"],
        "batch_sha256": contract._document_sha256(batch),
        "subject": "english",
        "target_id": args.target_id,
        "target_kind": kind,
        "ordinal": 1,
        "fencing_token": 1,
        "attempt": 1,
        "previous_checkpoint_sha256": None,
        "canonical_evidence_sha256": _digest(
            {"mode": "isolated-fault-recovery", "target_id": args.target_id}
        ),
        "authority_checkpoint_sha256": batch["authority_fingerprint"],
        "current_object_sha256": current_sha,
        "decision": "update_existing",
        "desired_object_sha256": desired_sha,
        "reason": "isolated post-replace rollback validation",
        "status": "approved",
        "formal_write_count": 0,
        "completed_at": _utc_now(),
    }
    review_result = results / "review-result"
    review_result.mkdir(parents=True, mode=0o700)
    (review_result / "review.json").write_bytes(
        contract._json_file_bytes(review_core)
    )
    review_sha, review_path = (
        publisher.publish_english_legacy_item_review_from_result_directory(
            review_result, batch=batch
        )
    )
    store.record_english_legacy_item_review(review_sha)
    execution_result = results / "execution-result"
    request = {
        "schema_version": REQUEST_SCHEMA,
        "mode": "apply",
        "runtime_root": str(runtime),
        "batch_path": str(batch_path),
        "review_receipt_path": str(review_path),
        "isolated_root": str(isolated),
        "result_dir": str(execution_result),
        "copy_id": args.copy_id,
        "desired_object": desired,
        "fault_injection": "after_replace_before_verify",
    }
    request_path = results / "request.json"
    request_path.write_bytes(contract._json_file_bytes(request))
    summary = execute_request(request_path)
    if summary["outcome"] != "runtime_failure":
        raise EnglishLegacyWriterError("english_writer_fault_not_observed")
    failure_sha, _ = (
        publisher.publish_english_legacy_item_failure_from_result_directory(
            execution_result, batch=batch
        )
    )
    failed = store.record_english_legacy_item_failure(failure_sha)
    failure = json.loads(
        (execution_result / "failure.json").read_text(encoding="utf-8")
    )
    copy_post = authority_manifest(isolated)["manifest_sha256"]
    restored = _locate(
        isolated, {"target_id": args.target_id, "target_kind": kind}
    )
    try:
        os.kill(failure["writer_process"]["pid"], 0)
    except ProcessLookupError:
        process_stopped = True
    else:
        process_stopped = False
    source_check = source_unchanged(marker)
    report = {
        "schema_version": "english_legacy_writer_fault_recovery_validation_v1",
        "issue_id": "EN-P0-006",
        "subject": "english",
        "authority_mode": "isolated_synthetic_fault_validation",
        "test_only_bootstrap": True,
        "target_id": args.target_id,
        "pre_object_sha256": current_sha,
        "restored_object_sha256": restored["object_sha256"],
        "desired_object_sha256": desired_sha,
        "transaction_state": failure["transaction_end"]["state"],
        "failure_receipt_sha256": failure_sha,
        "recovery_position_sha256": failure["recovery_position_sha256"],
        "writer_process_stopped": process_stopped,
        "writer_exit_code": failure["writer_process"]["exit_code"],
        "runtime_state": failed["state"]["status"],
        "copy_pre_authority_manifest_sha256": copy_pre,
        "copy_post_authority_manifest_sha256": copy_post,
        "copy_authority_restored": copy_pre == copy_post,
        "source_unchanged": source_check,
        "model_call_count": 0,
        "formal_write_count": failed["state"]["formal_write_count"],
        "status": (
            "verified_rolled_back"
            if failure["transaction_end"]["state"] == "rolled_back"
            and restored["object_sha256"] == current_sha
            and copy_pre == copy_post
            and process_stopped
            and source_check["unchanged"] is True
            and failed["state"]["status"] == "safe_paused"
            and failed["state"]["formal_write_count"] == 0
            else "failed"
        ),
        "completed_at": _utc_now(),
    }
    report_path.write_bytes(contract._json_file_bytes(report))
    report_path.chmod(0o400)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--copy-id", required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = run(args)
    except (EnglishLegacyWriterError, OSError, ValueError) as exc:
        code = exc.code if isinstance(exc, EnglishLegacyWriterError) else str(exc)
        print(json.dumps({"status": "failed", "error": code}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "target_id": report["target_id"],
                "transaction_state": report["transaction_state"],
                "runtime_state": report["runtime_state"],
                "copy_authority_restored": report["copy_authority_restored"],
                "source_unchanged": report["source_unchanged"]["unchanged"],
                "formal_write_count": report["formal_write_count"],
            },
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "verified_rolled_back" else 1


if __name__ == "__main__":
    raise SystemExit(main())
