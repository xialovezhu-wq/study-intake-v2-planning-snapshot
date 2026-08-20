#!/usr/bin/env python3
"""Run the real EN-P0-006 adapter over an isolated already-current target set.

This validation harness is deliberately test-only.  It bootstraps a fresh
isolated SubjectSolRuntimeStore from an immutable work-item batch, permits only
targets whose frozen current and desired hashes are identical, and records its
synthetic review authority mode in the final report.  It cannot update data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import subject_sol_contract as contract  # noqa: E402
from english_legacy_recuration import reopen_work_item_batch  # noqa: E402
from english_legacy_writer_adapter import (  # noqa: E402
    EnglishLegacyWriterError,
    MARKER_NAME,
    REQUEST_SCHEMA,
    _load_json,
    _utc_now,
    _validate_marker,
    _value_sha256,
    authority_manifest,
    execute_request,
    source_unchanged,
)
from isolated_authority_publishers import IsolatedWriterAdapterPublisher  # noqa: E402


VALIDATION_REPORT_SCHEMA = "english_legacy_writer_target_set_validation_v1"


def _digest(value: object) -> str:
    return hashlib.sha256(contract._canonical_bytes(value)).hexdigest()


def _safe_new_root(path: Path, *, run_root: Path, code: str) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(run_root)
    except ValueError as exc:
        raise EnglishLegacyWriterError(code) from exc
    if resolved.exists():
        raise EnglishLegacyWriterError(code)
    return resolved


def _build_sol_batch(work_batch: dict, work_items: tuple[dict, ...]) -> dict:
    rows = []
    for item in work_items:
        if item["current_object_sha256"] != item["desired_object_sha256"]:
            raise EnglishLegacyWriterError(
                "english_writer_validation_update_target_forbidden"
            )
        target_id = item["target_id"]
        rows.append(
            {
                "schema_version": "english_legacy_sol_work_item_v1",
                "parent_batch_authorization_sha256": item[
                    "batch_authorization_sha256"
                ],
                "inventory_sha256": item["inventory_sha256"],
                "authorization_event_sha256": _digest(
                    {"kind": "isolated-validation-event", "target_id": target_id}
                ),
                "target_authorization_receipt_sha256": item[
                    "target_authorization_receipt_sha256"
                ],
                "target_id": target_id,
                "target_kind": item["target_kind"],
                "ordinal": item["ordinal"],
                "current_object_sha256": item["current_object_sha256"],
                "work_item_sha256": _digest(
                    {"kind": "isolated-validation-work", "target_id": target_id}
                ),
                "authority_generation": item["authority"]["generation"],
                "authority_fingerprint": item["authority"][
                    "authority_fingerprint"
                ],
                "proposal_sha256": _digest(
                    {"kind": "already-current-proposal", "target_id": target_id}
                ),
                "package_sha256": _digest(
                    {"kind": "isolated-validation-package", "target_id": target_id}
                ),
                "quality_receipt_sha256": _digest(
                    {"kind": "isolated-validation-quality", "target_id": target_id}
                ),
                "status": "quality_passed",
                "luna_terminal": True,
                "quality_passed": True,
                "quality_outcome": "accepted",
                "proposed_action": "already_current_proposal",
                "model_call_count": 2,
                "formal_write_count": 0,
                "idempotency_key": f"isolated-validation:{target_id}",
            }
        )
    if [row["target_id"] for row in rows] != sorted(
        row["target_id"] for row in rows
    ):
        raise EnglishLegacyWriterError(
            "english_writer_validation_target_order_invalid"
        )
    return contract.validate_english_legacy_recuration_sol_batch_v1(
        {
            "schema_version": "english_legacy_recuration_sol_batch_v1",
            "issue_id": "EN-P0-006",
            "batch_id": (
                "EN-P0-006-ISOLATED-VALIDATION-"
                f"{_digest(work_batch['target_set_sha256'])[:16]}"
            ),
            "subject": "english",
            "batch_authorization_sha256": work_batch[
                "batch_authorization_sha256"
            ],
            "authorization_expansion_closure_sha256": work_batch[
                "authorization_expansion_closure_sha256"
            ],
            "inventory_sha256": work_batch["inventory_sha256"],
            "target_set_sha256": work_batch["target_set_sha256"],
            "target_count": work_batch["target_count"],
            "authority_generation": work_batch["authority"]["generation"],
            "authority_fingerprint": work_batch["authority"][
                "authority_fingerprint"
            ],
            "authorized_at": work_batch["created_at"],
            "work_items": rows,
            "status": "authorized",
            "formal_write_count": 0,
        }
    )


def _publish_batch(runtime: Path, batch: dict) -> tuple[str, Path]:
    payload = contract._json_file_bytes(batch)
    digest = hashlib.sha256(payload).hexdigest()
    path = (
        runtime / "dispatch/english-legacy/sol-batches/sha256"
        / digest[:2] / f"{digest}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o400)
    return digest, path


def _bootstrap_runtime(runtime: Path, batch: dict, batch_sha: str) -> None:
    store = contract.SubjectSolRuntimeStore(runtime)
    state = store._default_english_legacy_state(batch, batch_sha256=batch_sha)
    state.update(
        {
            "status": "active",
            "attempt": 1,
            "fencing_token": 1,
            "revision": 1,
            "updated_at": _utc_now(),
        }
    )
    state["items"][0]["attempts"] = [
        {
            "attempt": 1,
            "fencing_token": 1,
            "review_receipt_sha256": None,
            "apply_receipt_sha256": None,
            "failure_receipt_sha256": None,
            "recovery_receipt_sha256": None,
        }
    ]
    authorization_sha = batch["batch_authorization_sha256"]
    global_state = {
        "schema_version": contract.GLOBAL_SOL_WRITER_SCHEMA,
        "revision": 1,
        "next_fencing_token": 2,
        "queue": [
            {
                "batch_id": batch["batch_id"],
                "subject": "english",
                "authorized_at": batch["authorized_at"],
                "authorization_receipt_sha256": authorization_sha,
                "daily_sol_batch_sha256": batch_sha,
                "status": "active",
            }
        ],
        "active_writer": {
            "batch_id": batch["batch_id"],
            "subject": "english",
            "fencing_token": 1,
            "owner_id": "isolated-validation-sol",
            "claimed_at": _utc_now(),
            "authorization_receipt_sha256": authorization_sha,
            "daily_sol_batch_sha256": batch_sha,
            "review_receipt_sha256": None,
            "status": "reviewing",
            "current_item": batch["work_items"][0]["target_id"],
            "committed_count": 0,
            "remaining_count": batch["target_count"],
        },
        "active_writer_count": 1,
        "formal_write_count": 0,
        "updated_at": _utc_now(),
    }
    contract.SubjectSolRuntimeStore._validate_global(global_state)
    contract.SubjectSolRuntimeStore._validate_english_legacy_state(
        state, batch=batch, batch_sha256=batch_sha
    )
    state_path = (
        runtime / "dispatch/state/english-legacy-sol-batches"
        / f"{contract._safe_component(batch['batch_id'])}.json"
    )
    global_path = runtime / "dispatch/state/global-sol-writer.json"
    store._atomic_json(state_path, state)
    store._atomic_json(global_path, global_state)


def _review_core(
    batch: dict,
    item: dict,
    state: dict,
    authority_checkpoint: str,
) -> dict:
    return {
        "issue_id": "EN-P0-006",
        "batch_id": batch["batch_id"],
        "batch_sha256": contract._document_sha256(batch),
        "subject": "english",
        "target_id": item["target_id"],
        "target_kind": item["target_kind"],
        "ordinal": item["ordinal"],
        "fencing_token": state["fencing_token"],
        "attempt": state["attempt"],
        "previous_checkpoint_sha256": state["current_checkpoint_sha256"],
        "canonical_evidence_sha256": _digest(
            {
                "mode": "isolated-deterministic-already-current-validation",
                "target_id": item["target_id"],
                "current_object_sha256": item["current_object_sha256"],
            }
        ),
        "authority_checkpoint_sha256": authority_checkpoint,
        "current_object_sha256": item["current_object_sha256"],
        "decision": "already_current",
        "desired_object_sha256": None,
        "reason": "isolated deterministic current-equals-desired validation",
        "status": "approved",
        "formal_write_count": 0,
        "completed_at": _utc_now(),
    }


def run(args: argparse.Namespace) -> dict:
    isolated = args.isolated_root.resolve()
    marker = _validate_marker(isolated, args.copy_id)
    run_root = isolated.parent.resolve()
    runtime = _safe_new_root(
        args.runtime_root, run_root=run_root,
        code="english_writer_validation_runtime_invalid",
    )
    results = _safe_new_root(
        args.results_root, run_root=run_root,
        code="english_writer_validation_results_invalid",
    )
    report_path = args.report.resolve()
    try:
        report_path.relative_to(run_root)
    except ValueError as exc:
        raise EnglishLegacyWriterError(
            "english_writer_validation_report_invalid"
        ) from exc
    if report_path.exists():
        raise EnglishLegacyWriterError("english_writer_validation_report_invalid")
    work_batch_sha, work_batch, frozen_items = reopen_work_item_batch(
        args.work_item_batch
    )
    batch = _build_sol_batch(work_batch, frozen_items)
    runtime.mkdir(parents=True, mode=0o700)
    results.mkdir(parents=True, mode=0o700)
    batch_sha, batch_path = _publish_batch(runtime, batch)
    _bootstrap_runtime(runtime, batch, batch_sha)
    store = contract.SubjectSolRuntimeStore(runtime)
    publisher = IsolatedWriterAdapterPublisher(runtime)
    copy_pre = authority_manifest(isolated)["manifest_sha256"]
    outcomes = []
    final = None
    for item in batch["work_items"]:
        state_path = (
            runtime / "dispatch/state/english-legacy-sol-batches"
            / f"{contract._safe_component(batch['batch_id'])}.json"
        )
        state = _load_json(state_path, "english_writer_validation_state_invalid")
        authority_checkpoint = store._english_legacy_expected_authority(
            batch=batch, state=state
        )
        review_core = _review_core(batch, item, state, authority_checkpoint)
        item_root = results / f"{item['ordinal']:03d}"
        review_result = item_root / "review-result"
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
        execution_result = item_root / "execution-result"
        request = {
            "schema_version": REQUEST_SCHEMA,
            "mode": "apply",
            "runtime_root": str(runtime),
            "batch_path": str(batch_path),
            "review_receipt_path": str(review_path),
            "isolated_root": str(isolated),
            "result_dir": str(execution_result),
            "copy_id": args.copy_id,
            "desired_object": None,
            "fault_injection": None,
        }
        request_path = item_root / "request.json"
        request_path.write_bytes(contract._json_file_bytes(request))
        summary = execute_request(request_path)
        if summary["outcome"] != "already_current":
            raise EnglishLegacyWriterError(
                "english_writer_validation_nonzero_write_outcome"
            )
        apply_sha, _ = (
            publisher.publish_english_legacy_item_apply_from_result_directory(
                execution_result, batch=batch
            )
        )
        final = store.finish_english_legacy_item(apply_sha)
        process = json.loads(
            (execution_result / "apply.json").read_text(encoding="utf-8")
        )["writer_process"]
        try:
            os.kill(process["pid"], 0)
        except ProcessLookupError:
            process_stopped = True
        else:
            process_stopped = False
        outcomes.append(
            {
                "ordinal": item["ordinal"],
                "target_id": item["target_id"],
                "review_receipt_sha256": review_sha,
                "apply_receipt_sha256": apply_sha,
                "execution_result_path": str(execution_result),
                "writer_pid": process["pid"],
                "writer_process_stopped": process_stopped,
                "status": summary["outcome"],
                "formal_write_count": summary["formal_write_count"],
            }
        )
    if final is None or final["execution_closure"] is None:
        raise EnglishLegacyWriterError(
            "english_writer_validation_closure_missing"
        )
    closure = final["execution_closure"]
    copy_post = authority_manifest(isolated)["manifest_sha256"]
    source_check = source_unchanged(marker)
    report = {
        "schema_version": VALIDATION_REPORT_SCHEMA,
        "issue_id": "EN-P0-006",
        "subject": "english",
        "authority_mode": "isolated_deterministic_already_current_validation",
        "test_only_bootstrap": True,
        "work_item_batch_sha256": work_batch_sha,
        "sol_batch_sha256": batch_sha,
        "target_count": len(outcomes),
        "already_current_count": sum(
            row["status"] == "already_current" for row in outcomes
        ),
        "committed_count": 0,
        "failed_count": 0,
        "all_writer_processes_stopped": all(
            row["writer_process_stopped"] for row in outcomes
        ),
        "copy_pre_authority_manifest_sha256": copy_pre,
        "copy_post_authority_manifest_sha256": copy_post,
        "copy_authority_unchanged": copy_pre == copy_post,
        "source_unchanged": source_check,
        "execution_closure_sha256": final["execution_closure_sha256"],
        "execution_closure_status": closure["status"],
        "outcomes": outcomes,
        "model_call_count": 0,
        "formal_write_count": final["formal_write_count"],
        "status": (
            "verified_zero_write_complete"
            if len(outcomes) == batch["target_count"]
            and all(row["status"] == "already_current" for row in outcomes)
            and all(row["writer_process_stopped"] for row in outcomes)
            and copy_pre == copy_post
            and source_check["unchanged"] is True
            and closure["status"] == "verified_complete"
            and final["state"]["formal_write_count"] == 0
            else "failed"
        ),
        "completed_at": _utc_now(),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_bytes(contract._json_file_bytes(report))
    report_path.chmod(0o400)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-item-batch", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--copy-id", required=True)
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
                "target_count": report["target_count"],
                "already_current_count": report["already_current_count"],
                "formal_write_count": report["formal_write_count"],
                "all_writer_processes_stopped": report[
                    "all_writer_processes_stopped"
                ],
                "source_unchanged": report["source_unchanged"]["unchanged"],
            },
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "verified_zero_write_complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
