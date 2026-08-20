from __future__ import annotations

import hashlib
import hmac
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from isolated_authority_publishers import (  # noqa: E402
    IsolatedAuthorityPublisherError,
    IsolatedWriterAdapterPublisher,
)
import subject_sol_contract as contract  # noqa: E402
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
    build_english_legacy_sol_work_item_v1,
    validate_english_legacy_recuration_execution_closure_v1,
    validate_english_legacy_recuration_sol_batch_v1,
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class EnglishLegacySolRecurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.store = SubjectSolRuntimeStore(self.runtime)
        self.legacy_key = bytes(range(32))
        key_path = self.store.english_legacy_batch_authority_key_path
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(self.legacy_key)
        key_path.chmod(0o600)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def legacy_seal(self, core: dict, purpose: str) -> dict:
        value = dict(core)
        value["authority_key_id"] = hashlib.sha256(self.legacy_key).hexdigest()
        value["seal"] = {
            "algorithm": "HMAC-SHA256",
            "purpose": purpose,
            "hmac_sha256": hmac.new(
                self.legacy_key,
                contract._canonical_bytes(value),
                hashlib.sha256,
            ).hexdigest(),
        }
        return value

    def publish_legacy(self, kind: str, value: dict) -> str:
        payload = contract._json_file_bytes(value)
        sha = hashlib.sha256(payload).hexdigest()
        path = (
            self.store.english_legacy_root / kind / "sha256"
            / sha[:2] / f"{sha}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return sha

    def build_batch(self, actions: tuple[str, ...]) -> dict:
        inventory_sha = digest("inventory")
        target_set_sha = digest("target-set")
        authority_fingerprint = digest("authority")
        authorization_core = {
            "schema_version": "english_legacy_batch_authorization_v1",
            "issue_id": "EN-P0-006",
            "subject": "english",
            "batch_authorization_id": "EN-P0-006-BATCH-TEST",
            "intent_sha256": digest("intent"),
            "inventory_sha256": inventory_sha,
            "target_set_sha256": target_set_sha,
            "target_count": len(actions),
            "authority_generation": "english-generation-1",
            "authority_fingerprint": authority_fingerprint,
            "disposition": "deterministic_recuration",
            "authorized_operations": ["luna_recuration", "sol_review", "sol_apply"],
            "exact_inventory_only": True,
            "user_message_sha256": digest("user-message"),
            "authorized_at": "2026-08-09T04:00:00Z",
            "materialized_at": "2026-08-09T04:01:00Z",
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        authorization = self.legacy_seal(
            authorization_core, "english-legacy-batch-authorization-v1"
        )
        authorization_sha = self.publish_legacy(
            "batch-authorizations", authorization
        )
        items = []
        for ordinal, action in enumerate(actions, start=1):
            target_kind = (
                "master_bank_row"
                if ordinal < len(actions) or len(actions) == 1
                else "sentence_pattern_card"
            )
            record_id = f"ROW-{ordinal:03d}" if target_kind == "master_bank_row" else "SP-022"
            target_id = f"{target_kind}:{record_id}"
            current_sha = digest(f"current-{ordinal}")
            event = self.legacy_seal(
                {
                    "schema_version": "english_legacy_target_authorization_event_v3",
                    "event_id": f"EVENT-{ordinal}",
                    "event_type": "materialized_user_english_legacy_disposition",
                    "issue_id": "EN-P0-006",
                    "subject": "english",
                    "parent_batch_authorization_sha256": authorization_sha,
                    "inventory_sha256": inventory_sha,
                    "target_set_sha256": target_set_sha,
                    "ordinal": ordinal,
                    "target_id": target_id,
                    "target_kind": target_kind,
                    "record_id": record_id,
                    "historical_operation": "updated",
                    "current_object_sha256": current_sha,
                    "origin_postimage_sha256": current_sha,
                    "disposition": "deterministic_recuration",
                    "authorized_operations": ["luna_recuration", "sol_review", "sol_apply"],
                    "authorization_scope": "exact_target_from_batch",
                    "idempotency_key": f"sha256:{digest(f'idempotency-{ordinal}')}",
                    "user_message_sha256": digest("user-message"),
                    "authorized_at": "2026-08-09T04:00:00Z",
                    "materialized_at": "2026-08-09T04:01:00Z",
                },
                "english-legacy-target-authorization-event-v3",
            )
            event_sha = self.publish_legacy("authorization-events", event)
            disposition = self.legacy_seal(
                {
                    "schema_version": "english_legacy_disposition_receipt_v3",
                    "issue_id": "EN-P0-006",
                    "subject": "english",
                    "batch_authorization_sha256": authorization_sha,
                    "authorization_event_sha256": event_sha,
                    "inventory_sha256": inventory_sha,
                    "target_set_sha256": target_set_sha,
                    "target": {
                        "ordinal": ordinal,
                        "target_id": target_id,
                        "target_kind": target_kind,
                        "record_id": record_id,
                        "historical_operation": "updated",
                        "current_object_sha256": current_sha,
                        "origin_postimage_sha256": current_sha,
                    },
                    "disposition": "deterministic_recuration",
                    "model_call_count": 0,
                    "formal_write_count": 0,
                    "issued_at": "2026-08-09T04:01:00Z",
                },
                "english-legacy-disposition-receipt-v3",
            )
            disposition_sha = self.publish_legacy("dispositions", disposition)
            items.append(
                {
                    "schema_version": "english_legacy_sol_work_item_v1",
                    "parent_batch_authorization_sha256": authorization_sha,
                    "inventory_sha256": inventory_sha,
                    "authorization_event_sha256": event_sha,
                    "target_authorization_receipt_sha256": disposition_sha,
                    "target_id": target_id,
                    "target_kind": target_kind,
                    "ordinal": ordinal,
                    "current_object_sha256": current_sha,
                    "work_item_sha256": digest(f"work-item-{ordinal}"),
                    "authority_generation": "english-generation-1",
                    "authority_fingerprint": authority_fingerprint,
                    "proposal_sha256": digest(f"proposal-{ordinal}"),
                    "package_sha256": digest(f"package-{ordinal}"),
                    "quality_receipt_sha256": digest(f"quality-{ordinal}"),
                    "status": "quality_passed",
                    "luna_terminal": True,
                    "quality_passed": True,
                    "quality_outcome": "accepted",
                    "proposed_action": f"{action}_proposal",
                    "model_call_count": 2,
                    "formal_write_count": 0,
                    "idempotency_key": f"work:{ordinal}",
                }
            )
        items.sort(key=lambda row: row["target_id"])
        for ordinal, row in enumerate(items, start=1):
            row["ordinal"] = ordinal
            event_path = next(
                (self.store.english_legacy_target_authorization_event_root / "sha256").glob(
                    f"*/{row['authorization_event_sha256']}.json"
                )
            )
            event = json.loads(event_path.read_text())
            event["ordinal"] = ordinal
            event_core = dict(event)
            event_core.pop("seal")
            event = self.legacy_seal(
                event_core, "english-legacy-target-authorization-event-v3"
            )
            event_sha = self.publish_legacy("authorization-events", event)
            row["authorization_event_sha256"] = event_sha
            receipt_path = next(
                (self.store.english_legacy_disposition_receipt_root / "sha256").glob(
                    f"*/{row['target_authorization_receipt_sha256']}.json"
                )
            )
            receipt = json.loads(receipt_path.read_text())
            receipt["authorization_event_sha256"] = event_sha
            receipt["target"]["ordinal"] = ordinal
            receipt_core = dict(receipt)
            receipt_core.pop("seal")
            receipt = self.legacy_seal(
                receipt_core, "english-legacy-disposition-receipt-v3"
            )
            row["target_authorization_receipt_sha256"] = self.publish_legacy(
                "dispositions", receipt
            )
        authorization_closure = self.legacy_seal(
            {
                "schema_version": "english_legacy_authorization_expansion_closure_v1",
                "issue_id": "EN-P0-006",
                "subject": "english",
                "batch_authorization_sha256": authorization_sha,
                "intent_sha256": digest("intent"),
                "inventory_sha256": inventory_sha,
                "target_set_sha256": target_set_sha,
                "authority_generation": "english-generation-1",
                "authority_fingerprint": authority_fingerprint,
                "authorized_operations": ["luna_recuration", "sol_review", "sol_apply"],
                "target_authorizations": [
                    {
                        "ordinal": row["ordinal"],
                        "target_id": row["target_id"],
                        "authorization_event_sha256": row["authorization_event_sha256"],
                        "disposition_receipt_sha256": row[
                            "target_authorization_receipt_sha256"
                        ],
                    }
                    for row in items
                ],
                "target_count": len(items),
                "materialized_target_count": len(items),
                "unidentified_target_count": 0,
                "omitted_target_count": 0,
                "duplicate_target_count": 0,
                "unknown_target_count": 0,
                "model_call_count": 0,
                "formal_write_count": 0,
                "issued_at": "2026-08-09T04:01:00Z",
            },
            "english-legacy-authorization-expansion-closure-v1",
        )
        authorization_closure_sha = self.publish_legacy(
            "authorization-closures", authorization_closure
        )
        return {
            "schema_version": "english_legacy_recuration_sol_batch_v1",
            "issue_id": "EN-P0-006",
            "batch_id": f"EN-P0-006-SOL-{len(actions)}",
            "subject": "english",
            "batch_authorization_sha256": authorization_sha,
            "authorization_expansion_closure_sha256": authorization_closure_sha,
            "inventory_sha256": inventory_sha,
            "target_set_sha256": target_set_sha,
            "target_count": len(items),
            "authority_generation": "english-generation-1",
            "authority_fingerprint": authority_fingerprint,
            "authorized_at": "2026-08-09T04:00:00Z",
            "work_items": items,
            "status": "authorized",
            "formal_write_count": 0,
        }

    def result_dir(self, name: str, filename: str, core: dict) -> Path:
        root = Path(self.temp.name) / name
        root.mkdir()
        checked = json.loads(json.dumps(core))
        if filename in {"apply.json", "failure.json"}:
            process = checked["writer_process"]
            process_evidence = {
                "adapter_run_id": process["adapter_run_id"],
                "pid": process["pid"],
                "started_at": "2026-08-09T04:59:59Z",
                "stopped_at": process["stopped_at"],
                "terminal_state": process["terminal_state"],
                "exit_code": process["exit_code"],
                "request_sha256": digest(f"request-{name}"),
                "runtime_state_sha256": digest(f"runtime-{name}"),
            }
            process_path = root / "writer-process-evidence.json"
            process_path.write_bytes(contract._json_file_bytes(process_evidence))
            process_path.chmod(0o400)
            process["evidence_sha256"] = hashlib.sha256(
                process_path.read_bytes()
            ).hexdigest()

            transaction = checked["transaction_end"]
            pre_object = checked.get(
                "pre_object_sha256", digest(f"pre-object-{name}")
            )
            transaction_evidence = {
                "transaction_id": transaction["transaction_id"],
                "state": transaction["state"],
                "target_id": checked["target_id"],
                "pre_authority_file_sha256": digest(f"pre-file-{name}"),
                "post_authority_file_sha256": digest(f"post-file-{name}"),
                "pre_object_sha256": pre_object,
                "post_object_sha256": checked.get("post_object_sha256", pre_object),
                "ended_at": transaction["ended_at"],
            }
            transaction_path = root / "transaction-evidence.json"
            transaction_path.write_bytes(
                contract._json_file_bytes(transaction_evidence)
            )
            transaction_path.chmod(0o400)
            transaction["evidence_sha256"] = hashlib.sha256(
                transaction_path.read_bytes()
            ).hexdigest()

            if filename == "failure.json":
                recovery = {
                    "copy_id": f"fixture-copy-{name}",
                    "target_id": checked["target_id"],
                    "pre_authority_file_sha256": transaction_evidence[
                        "pre_authority_file_sha256"
                    ],
                    "pre_object_sha256": transaction_evidence[
                        "pre_object_sha256"
                    ],
                    "rollback_snapshot_sha256": None,
                    "transaction_state": transaction["state"],
                }
                recovery_path = root / "recovery-position.json"
                recovery_path.write_bytes(contract._json_file_bytes(recovery))
                recovery_path.chmod(0o400)
                checked["recovery_position_sha256"] = hashlib.sha256(
                    recovery_path.read_bytes()
                ).hexdigest()

        result_path = root / filename
        result_path.write_bytes(contract._json_file_bytes(checked))
        result_path.chmod(0o400)
        return root

    @staticmethod
    def rewrite_immutable_json(path: Path, value: dict) -> None:
        path.chmod(0o600)
        path.write_bytes(contract._json_file_bytes(value))
        path.chmod(0o400)

    def stage_batch(self, batch: dict) -> dict:
        def verify(_config: dict, _runtime: Path, receipt_sha: str) -> dict:
            item = next(
                row for row in batch["work_items"]
                if row["quality_receipt_sha256"] == receipt_sha
            )
            return {
                "schema_version": "verified_english_legacy_recuration_quality_v1",
                "receipt_sha256": receipt_sha,
                "package_sha256": item["package_sha256"],
                "work_item_sha256": item["work_item_sha256"],
                "target_id": item["target_id"],
                "target_kind": item["target_kind"],
                "ordinal": item["ordinal"],
                "inventory_sha256": item["inventory_sha256"],
                "batch_authorization_sha256": item[
                    "parent_batch_authorization_sha256"
                ],
                "target_authorization_receipt_sha256": item[
                    "target_authorization_receipt_sha256"
                ],
                "authority": {
                    "generation": item["authority_generation"],
                    "authority_fingerprint": item["authority_fingerprint"],
                },
                "proposal_action": item["proposed_action"],
                "proposal_sha256": item["proposal_sha256"],
                "quality_outcome": item["quality_outcome"],
                "model_call_count": 2,
                "formal_write_count": 0,
            }

        def verify_authorization(closure_sha: str) -> dict:
            path = next(
                (
                    self.store.english_legacy_authorization_closure_root
                    / "sha256"
                ).glob(f"*/{closure_sha}.json")
            )
            return json.loads(path.read_text(encoding="utf-8"))

        return self.store.stage_english_legacy_recuration_batch(
            batch,
            quality_verifier=verify,
            authorization_verifier=verify_authorization,
        )

    @staticmethod
    def process(label: str, exit_code: int = 0) -> dict:
        return {
            "adapter_run_id": label,
            "pid": 4242,
            "terminal_state": "stopped",
            "exit_code": exit_code,
            "stopped_at": "2026-08-09T05:00:00Z",
            "evidence_sha256": digest(f"process-{label}"),
        }

    @staticmethod
    def transaction(label: str, state: str) -> dict:
        return {
            "transaction_id": label,
            "state": state,
            "ended_at": "2026-08-09T05:00:00Z",
            "evidence_sha256": digest(f"transaction-{label}"),
        }

    def authority_at(self, batch: dict, checkpoint_sha: str | None) -> str:
        if checkpoint_sha is None:
            return batch["authority_fingerprint"]
        checkpoint = self.store._read_english_legacy_checkpoint(
            checkpoint_sha, batch=batch
        )
        return checkpoint["post_authority_sha256"]

    def publish_review(
        self, batch: dict, claim: dict, *, decision: str, attempt: int = 1,
        previous_checkpoint: str | None = None,
    ) -> str:
        item = batch["work_items"][claim["current_ordinal"] - 1]
        core = {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": item["ordinal"], "fencing_token": claim["fencing_token"],
            "attempt": attempt, "previous_checkpoint_sha256": previous_checkpoint,
            "canonical_evidence_sha256": digest(f"evidence-{item['ordinal']}-{attempt}"),
            "authority_checkpoint_sha256": self.authority_at(
                batch, previous_checkpoint
            ),
            "current_object_sha256": item["current_object_sha256"],
            "decision": decision,
            "desired_object_sha256": (
                digest(f"desired-{item['ordinal']}-{attempt}")
                if decision == "update_existing" else None
            ),
            "reason": "independent canonical review", "status": "approved",
            "formal_write_count": 0, "completed_at": "2026-08-09T05:00:00Z",
        }
        sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_review_from_result_directory(
            self.result_dir(f"review-{item['ordinal']}-{attempt}", "review.json", core),
            batch=batch,
        )
        return sha

    def publish_apply(
        self, batch: dict, claim: dict, *, review_sha: str, status: str,
        attempt: int = 1, previous_checkpoint: str | None = None,
    ) -> str:
        core = self.apply_core(
            batch,
            claim,
            review_sha=review_sha,
            status=status,
            attempt=attempt,
            previous_checkpoint=previous_checkpoint,
        )
        item = batch["work_items"][claim["current_ordinal"] - 1]
        sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_apply_from_result_directory(
            self.result_dir(f"apply-{item['ordinal']}-{attempt}", "apply.json", core),
            batch=batch,
        )
        return sha

    def apply_core(
        self, batch: dict, claim: dict, *, review_sha: str, status: str,
        attempt: int = 1, previous_checkpoint: str | None = None,
    ) -> dict:
        item = batch["work_items"][claim["current_ordinal"] - 1]
        before = item["current_object_sha256"]
        after = before if status == "already_current" else digest(
            f"desired-{item['ordinal']}-{attempt}"
        )
        pre_authority = self.authority_at(batch, previous_checkpoint)
        post_authority = (
            pre_authority
            if status == "already_current"
            else digest(f"authority-{item['ordinal']}-{attempt}")
        )
        operations = [] if status == "already_current" else [
            {
                "operation": "update_existing", "target_id": item["target_id"],
                "target_kind": item["target_kind"],
                "before_object_sha256": before, "after_object_sha256": after,
            }
        ]
        return {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": item["ordinal"], "fencing_token": claim["fencing_token"],
            "attempt": attempt, "previous_checkpoint_sha256": previous_checkpoint,
            "review_receipt_sha256": review_sha, "pre_object_sha256": before,
            "post_object_sha256": after,
            "pre_authority_sha256": pre_authority,
            "post_authority_sha256": post_authority,
            "operations": operations,
            "writer_process": self.process(f"apply-{item['ordinal']}-{attempt}"),
            "transaction_end": self.transaction(
                f"apply-{item['ordinal']}-{attempt}", status
            ),
            "status": status, "formal_write_count": len(operations),
            "completed_at": "2026-08-09T05:01:00Z",
        }

    def artifact_path(self, kind: str, sha256: str) -> Path:
        return (
            self.runtime
            / "dispatch/writer-adapter-receipts/artifacts"
            / kind
            / "sha256"
            / sha256[:2]
            / f"{sha256}.json"
        )

    def receipt_value(self, kind: str, sha256: str) -> dict:
        path = (
            self.runtime
            / "dispatch/writer-adapter-receipts"
            / kind
            / "sha256"
            / sha256[:2]
            / f"{sha256}.json"
        )
        return json.loads(path.read_text(encoding="utf-8"))

    def failure_core(
        self, batch: dict, claim: dict, *, review_sha: str,
    ) -> dict:
        item = batch["work_items"][claim["current_ordinal"] - 1]
        return {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": item["ordinal"], "fencing_token": claim["fencing_token"],
            "attempt": 1, "previous_checkpoint_sha256": None,
            "review_receipt_sha256": review_sha, "failure_stage": "formal_apply",
            "authority_checkpoint_sha256": batch["authority_fingerprint"],
            "reason": "injected writer failure",
            "recovery_position_sha256": digest("recovery-position"),
            "writer_process": self.process("failed", exit_code=1),
            "transaction_end": self.transaction("failed", "rolled_back"),
            "retryable": True, "formal_write_count": 0,
            "failed_at": "2026-08-09T05:02:00Z",
        }

    def pending_apply(self) -> tuple[dict, dict, str, str]:
        batch = self.build_batch(("already_current",))
        self.stage_batch(batch)
        claim = self.store.begin_english_legacy_recuration(
            batch["batch_id"], owner_id="fixture-sol"
        )
        review_sha = self.publish_review(
            batch, claim, decision="already_current"
        )
        self.store.record_english_legacy_item_review(review_sha)
        apply_sha = self.publish_apply(
            batch, claim, review_sha=review_sha, status="already_current"
        )
        return batch, claim, review_sha, apply_sha

    def pending_failure(self) -> tuple[dict, dict, str, str]:
        batch = self.build_batch(("update_existing",))
        self.stage_batch(batch)
        claim = self.store.begin_english_legacy_recuration(
            batch["batch_id"], owner_id="fixture-sol"
        )
        review_sha = self.publish_review(
            batch, claim, decision="update_existing"
        )
        self.store.record_english_legacy_item_review(review_sha)
        failure_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_failure_from_result_directory(
            self.result_dir(
                "pending-failure",
                "failure.json",
                self.failure_core(batch, claim, review_sha=review_sha),
            ),
            batch=batch,
        )
        return batch, claim, review_sha, failure_sha

    def control_state_bytes(self, batch: dict) -> tuple[bytes, bytes]:
        global_path = self.runtime / "dispatch/state/global-sol-writer.json"
        state_path = (
            self.runtime
            / "dispatch/state/english-legacy-sol-batches"
            / f"{batch['batch_id']}.json"
        )
        return global_path.read_bytes(), state_path.read_bytes()

    def test_parent_lease_is_held_across_serial_items_and_closes(self) -> None:
        batch = self.build_batch(("already_current", "update_existing"))
        validate_english_legacy_recuration_sol_batch_v1(batch)
        staged = self.stage_batch(batch)
        self.assertEqual(staged["global"]["active_writer_count"], 0)
        claim = self.store.begin_english_legacy_recuration(
            batch["batch_id"], owner_id="fixture-sol"
        )
        with self.assertRaises(SubjectSolContractError) as second_claim:
            self.store.begin_english_legacy_recuration(
                batch["batch_id"], owner_id="second-sol"
            )
        self.assertEqual(second_claim.exception.code, "global_sol_writer_busy")
        first_review = self.publish_review(batch, claim, decision="already_current")
        self.store.record_english_legacy_item_review(first_review)
        first_apply = self.publish_apply(
            batch, claim, review_sha=first_review, status="already_current"
        )
        first = self.store.finish_english_legacy_item(first_apply)
        self.assertEqual(first["global"]["active_writer_count"], 1)
        self.assertEqual(first["state"]["current_ordinal"], 2)
        self.assertEqual(first["state"]["items"][0]["status"], "already_current")
        self.assertEqual(first["state"]["formal_write_count"], 0)

        second_claim = {
            "fencing_token": claim["fencing_token"],
            "current_ordinal": 2,
        }
        second_review = self.publish_review(
            batch, second_claim, decision="update_existing",
            previous_checkpoint=first["checkpoint_sha256"],
        )
        self.store.record_english_legacy_item_review(second_review)
        second_apply = self.publish_apply(
            batch, second_claim, review_sha=second_review, status="committed",
            previous_checkpoint=first["checkpoint_sha256"],
        )
        finished = self.store.finish_english_legacy_item(second_apply)
        self.assertEqual(finished["global"]["active_writer_count"], 0)
        self.assertEqual(finished["global"]["formal_write_count"], 1)
        closure = finished["execution_closure"]
        self.assertEqual(closure["status"], "verified_complete")
        self.assertEqual(closure["already_current_count"], 1)
        self.assertEqual(closure["committed_count"], 1)
        validate_english_legacy_recuration_execution_closure_v1(
            closure, batch=batch
        )
        reopened = self.store.reopen_verified_english_legacy_execution_closure(
            batch["batch_id"]
        )
        self.assertEqual(reopened["status"], "verified_complete")
        self.assertEqual(reopened["checkpoint_count"], 2)
        self.assertEqual(reopened["review_receipt_count"], 2)
        self.assertEqual(reopened["recovery_receipt_count"], 0)
        claim_receipts = list(
            (self.runtime / "dispatch/control-receipts/sol-claims/sha256").glob("*/*.json")
        )
        self.assertEqual(len(claim_receipts), 2)

    def test_failure_releases_lease_and_recovery_uses_new_fence(self) -> None:
        batch = self.build_batch(("update_existing",))
        self.stage_batch(batch)
        claim = self.store.begin_english_legacy_recuration(
            batch["batch_id"], owner_id="fixture-sol"
        )
        review_sha = self.publish_review(batch, claim, decision="update_existing")
        self.store.record_english_legacy_item_review(review_sha)
        item = batch["work_items"][0]
        failure_core = self.failure_core(batch, claim, review_sha=review_sha)
        failure_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_failure_from_result_directory(
            self.result_dir("failure", "failure.json", failure_core), batch=batch
        )
        failed = self.store.record_english_legacy_item_failure(failure_sha)
        self.assertEqual(failed["global"]["active_writer_count"], 0)
        self.assertEqual(failed["state"]["status"], "safe_paused")
        recovery_claim = self.store.begin_english_legacy_recovery(
            batch["batch_id"], owner_id="fixture-sol-recovery"
        )
        self.assertGreater(
            recovery_claim["new_fencing_token"],
            recovery_claim["prior_fencing_token"],
        )
        recovery_core = {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": 1,
            "prior_fencing_token": recovery_claim["prior_fencing_token"],
            "new_fencing_token": recovery_claim["new_fencing_token"],
            "attempt": 2,
            "previous_checkpoint_sha256": recovery_claim["checkpoint_sha256"],
            "failure_receipt_sha256": failure_sha,
            "recovery_position_sha256": recovery_claim[
                "recovery_position_sha256"
            ],
            "authority_checkpoint_sha256": self.authority_at(
                batch, recovery_claim["checkpoint_sha256"]
            ),
            "status": "resumed", "formal_write_count": 0,
            "recovered_at": "2026-08-09T05:03:00Z",
        }
        recovery_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_recovery_from_result_directory(
            self.result_dir("recovery", "recovery.json", recovery_core), batch=batch
        )
        recovered = self.store.record_english_legacy_item_recovery(recovery_sha)
        self.assertEqual(recovered["state"]["status"], "active")
        resumed_claim = {
            "fencing_token": recovery_claim["new_fencing_token"],
            "current_ordinal": 1,
        }
        review2 = self.publish_review(
            batch, resumed_claim, decision="update_existing", attempt=2,
            previous_checkpoint=recovery_claim["checkpoint_sha256"],
        )
        self.store.record_english_legacy_item_review(review2)
        apply2 = self.publish_apply(
            batch, resumed_claim, review_sha=review2, status="committed", attempt=2,
            previous_checkpoint=recovery_claim["checkpoint_sha256"],
        )
        finished = self.store.finish_english_legacy_item(apply2)
        self.assertEqual(finished["execution_closure"]["status"], "verified_complete")
        self.assertEqual(
            finished["execution_closure"]["item_outcomes"][0]["attempt_count"],
            2,
        )
        state = finished["state"]["items"][0]
        self.assertEqual(state["attempts"][0]["failure_receipt_sha256"], failure_sha)
        self.assertEqual(state["attempts"][1]["recovery_receipt_sha256"], recovery_sha)
        reopened = self.store.reopen_verified_english_legacy_execution_closure(
            batch["batch_id"]
        )
        self.assertEqual(reopened["checkpoint_count"], 2)
        self.assertEqual(reopened["item_result_receipt_count"], 2)
        self.assertEqual(reopened["review_receipt_count"], 2)
        self.assertEqual(reopened["recovery_receipt_count"], 1)

    def test_publisher_rejects_missing_and_incomplete_raw_execution_evidence(
        self,
    ) -> None:
        batch = self.build_batch(("already_current",))
        claim = {"fencing_token": 7, "current_ordinal": 1}
        review_sha = digest("fixture-review")
        core = self.apply_core(
            batch, claim, review_sha=review_sha, status="already_current"
        )
        missing_root = self.result_dir(
            "missing-process-evidence", "apply.json", core
        )
        (missing_root / "writer-process-evidence.json").unlink()
        with self.assertRaises(IsolatedAuthorityPublisherError) as missing:
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_english_legacy_item_apply_from_result_directory(
                missing_root, batch=batch
            )
        self.assertEqual(missing.exception.args[0], "writer_result_file_invalid")

        incomplete_root = self.result_dir(
            "incomplete-process-evidence", "apply.json", core
        )
        process_path = incomplete_root / "writer-process-evidence.json"
        process = json.loads(process_path.read_text(encoding="utf-8"))
        del process["request_sha256"]
        self.rewrite_immutable_json(process_path, process)
        apply_path = incomplete_root / "apply.json"
        apply = json.loads(apply_path.read_text(encoding="utf-8"))
        apply["writer_process"]["evidence_sha256"] = hashlib.sha256(
            process_path.read_bytes()
        ).hexdigest()
        self.rewrite_immutable_json(apply_path, apply)
        with self.assertRaises(IsolatedAuthorityPublisherError) as incomplete:
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_english_legacy_item_apply_from_result_directory(
                incomplete_root, batch=batch
            )
        self.assertEqual(
            incomplete.exception.args[0],
            "english_writer_execution_evidence_invalid",
        )

    def test_publisher_rejects_coordinated_transaction_tamper(self) -> None:
        batch = self.build_batch(("already_current",))
        claim = {"fencing_token": 7, "current_ordinal": 1}
        root = self.result_dir(
            "tampered-transaction-evidence",
            "apply.json",
            self.apply_core(
                batch,
                claim,
                review_sha=digest("fixture-review"),
                status="already_current",
            ),
        )
        transaction_path = root / "transaction-evidence.json"
        transaction = json.loads(transaction_path.read_text(encoding="utf-8"))
        transaction["pre_object_sha256"] = digest("coordinated-tamper")
        self.rewrite_immutable_json(transaction_path, transaction)
        apply_path = root / "apply.json"
        apply = json.loads(apply_path.read_text(encoding="utf-8"))
        apply["transaction_end"]["evidence_sha256"] = hashlib.sha256(
            transaction_path.read_bytes()
        ).hexdigest()
        self.rewrite_immutable_json(apply_path, apply)
        with self.assertRaises(IsolatedAuthorityPublisherError) as tampered:
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_english_legacy_item_apply_from_result_directory(
                root, batch=batch
            )
        self.assertEqual(
            tampered.exception.args[0],
            "english_writer_execution_evidence_mismatch",
        )

    def test_publisher_rejects_missing_recovery_evidence(self) -> None:
        batch = self.build_batch(("update_existing",))
        claim = {"fencing_token": 7, "current_ordinal": 1}
        root = self.result_dir(
            "missing-recovery-evidence",
            "failure.json",
            self.failure_core(
                batch, claim, review_sha=digest("fixture-review")
            ),
        )
        (root / "recovery-position.json").unlink()
        with self.assertRaises(IsolatedAuthorityPublisherError) as missing:
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_english_legacy_item_failure_from_result_directory(
                root, batch=batch
            )
        self.assertEqual(missing.exception.args[0], "writer_result_file_invalid")

    def test_finish_reopens_and_rejects_tampered_process_artifact(self) -> None:
        batch, _, _, apply_sha = self.pending_apply()
        receipt = self.receipt_value("english-legacy-item-apply", apply_sha)
        process_path = self.artifact_path(
            "writer-process", receipt["writer_process"]["evidence_sha256"]
        )
        process = json.loads(process_path.read_text(encoding="utf-8"))
        process["stopped_at"] = "2026-08-09T05:00:01Z"
        self.rewrite_immutable_json(process_path, process)
        before = self.control_state_bytes(batch)
        with self.assertRaises(SubjectSolContractError) as tampered:
            self.store.finish_english_legacy_item(apply_sha)
        self.assertEqual(
            tampered.exception.code,
            "isolated_writer_writer-process_artifact_hash_mismatch",
        )
        self.assertEqual(self.control_state_bytes(batch), before)

    def test_failure_and_recovery_reopen_missing_execution_artifacts(self) -> None:
        batch, _, _, failure_sha = self.pending_failure()
        receipt = self.receipt_value("english-legacy-item-failure", failure_sha)
        recovery_path = self.artifact_path(
            "recovery-position", receipt["recovery_position_sha256"]
        )
        recovery_payload = recovery_path.read_bytes()
        recovery_path.unlink()
        before_failure = self.control_state_bytes(batch)
        with self.assertRaises(SubjectSolContractError) as missing_failure:
            self.store.record_english_legacy_item_failure(failure_sha)
        self.assertEqual(
            missing_failure.exception.code,
            "isolated_writer_recovery-position_artifact_missing",
        )
        self.assertEqual(self.control_state_bytes(batch), before_failure)

        recovery_path.write_bytes(recovery_payload)
        recovery_path.chmod(0o400)
        self.store.record_english_legacy_item_failure(failure_sha)
        transaction_path = self.artifact_path(
            "transaction-end", receipt["transaction_end"]["evidence_sha256"]
        )
        transaction_payload = transaction_path.read_bytes()
        transaction_path.unlink()
        before_claim = self.control_state_bytes(batch)
        with self.assertRaises(SubjectSolContractError) as missing_claim:
            self.store.begin_english_legacy_recovery(
                batch["batch_id"], owner_id="fixture-sol-recovery"
            )
        self.assertEqual(
            missing_claim.exception.code,
            "isolated_writer_transaction-end_artifact_missing",
        )
        self.assertEqual(self.control_state_bytes(batch), before_claim)

        transaction_path.write_bytes(transaction_payload)
        transaction_path.chmod(0o400)
        recovery_claim = self.store.begin_english_legacy_recovery(
            batch["batch_id"], owner_id="fixture-sol-recovery"
        )
        item = batch["work_items"][0]
        recovery_core = {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": 1,
            "prior_fencing_token": recovery_claim["prior_fencing_token"],
            "new_fencing_token": recovery_claim["new_fencing_token"],
            "attempt": 2,
            "previous_checkpoint_sha256": recovery_claim["checkpoint_sha256"],
            "failure_receipt_sha256": failure_sha,
            "recovery_position_sha256": recovery_claim[
                "recovery_position_sha256"
            ],
            "authority_checkpoint_sha256": self.authority_at(
                batch, recovery_claim["checkpoint_sha256"]
            ),
            "status": "resumed", "formal_write_count": 0,
            "recovered_at": "2026-08-09T05:03:00Z",
        }
        recovery_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_recovery_from_result_directory(
            self.result_dir(
                "missing-artifact-recovery", "recovery.json", recovery_core
            ),
            batch=batch,
        )
        recovery_path.unlink()
        before_record = self.control_state_bytes(batch)
        with self.assertRaises(SubjectSolContractError) as missing_record:
            self.store.record_english_legacy_item_recovery(recovery_sha)
        self.assertEqual(
            missing_record.exception.code,
            "isolated_writer_recovery-position_artifact_missing",
        )
        self.assertEqual(self.control_state_bytes(batch), before_record)

    def test_closure_reopen_requires_execution_artifacts(self) -> None:
        batch, _, _, apply_sha = self.pending_apply()
        finished = self.store.finish_english_legacy_item(apply_sha)
        self.assertEqual(
            finished["execution_closure"]["status"], "verified_complete"
        )
        receipt = self.receipt_value("english-legacy-item-apply", apply_sha)
        transaction_path = self.artifact_path(
            "transaction-end", receipt["transaction_end"]["evidence_sha256"]
        )
        transaction_path.unlink()
        with self.assertRaises(SubjectSolContractError) as missing:
            self.store.reopen_verified_english_legacy_execution_closure(
                batch["batch_id"]
            )
        self.assertEqual(
            missing.exception.code,
            "isolated_writer_transaction-end_artifact_missing",
        )

    def test_non_terminal_or_create_semantics_fail_closed(self) -> None:
        batch = self.build_batch(("update_existing",))
        invalid = json.loads(json.dumps(batch))
        invalid["work_items"][0]["quality_outcome"] = "rejected"
        with self.assertRaises(SubjectSolContractError) as quality:
            validate_english_legacy_recuration_sol_batch_v1(invalid)
        self.assertEqual(quality.exception.code, "english_legacy_work_item_not_sol_ready")

        self.stage_batch(batch)
        claim = self.store.begin_english_legacy_recuration(
            batch["batch_id"], owner_id="fixture-sol"
        )
        item = batch["work_items"][0]
        review_core = {
            "issue_id": "EN-P0-006", "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch), "subject": "english",
            "target_id": item["target_id"], "target_kind": item["target_kind"],
            "ordinal": 1, "fencing_token": claim["fencing_token"], "attempt": 1,
            "previous_checkpoint_sha256": None,
            "canonical_evidence_sha256": digest("evidence"),
            "authority_checkpoint_sha256": batch["authority_fingerprint"],
            "current_object_sha256": item["current_object_sha256"],
            "decision": "create", "desired_object_sha256": digest("desired"),
            "reason": "invalid", "status": "approved", "formal_write_count": 0,
            "completed_at": "2026-08-09T05:00:00Z",
        }
        with self.assertRaises(SubjectSolContractError) as create:
            IsolatedWriterAdapterPublisher(
                self.runtime
            ).publish_english_legacy_item_review_from_result_directory(
                self.result_dir("create-review", "review.json", review_core),
                batch=batch,
            )
        self.assertEqual(create.exception.code, "english_legacy_item_review_decision_invalid")

    def test_public_work_item_conversion_binds_authorization_and_quality(self) -> None:
        batch = self.build_batch(("update_existing",))
        batch_item = batch["work_items"][0]
        authorization_path = next(
            (
                self.store.english_legacy_batch_authorization_root / "sha256"
            ).glob(f"*/{batch['batch_authorization_sha256']}.json")
        )
        authorization = json.loads(
            authorization_path.read_text(encoding="utf-8")
        )
        mapping = {
            "ordinal": 1,
            "target_id": batch_item["target_id"],
            "authorization_event_sha256": batch_item[
                "authorization_event_sha256"
            ],
            "disposition_receipt_sha256": batch_item[
                "target_authorization_receipt_sha256"
            ],
        }
        evidence_payload = {
            "authorization": {
                "batch_authorization_sha256": batch[
                    "batch_authorization_sha256"
                ],
                "target_authorization_event_sha256": mapping[
                    "authorization_event_sha256"
                ],
                "target_authorization_receipt_sha256": mapping[
                    "disposition_receipt_sha256"
                ],
                "disposition": "deterministic_recuration",
                "existing_object_only": True,
            }
        }
        luna_item = {
            "schema_version": "english_legacy_recuration_work_item_v1",
            "remediation_batch_id": authorization["batch_authorization_id"],
            "inventory_sha256": batch["inventory_sha256"],
            "batch_authorization_sha256": batch[
                "batch_authorization_sha256"
            ],
            "target_authorization_receipt_sha256": mapping[
                "disposition_receipt_sha256"
            ],
            "target_id": batch_item["target_id"],
            "target_kind": batch_item["target_kind"],
            "record_id": "record-1",
            "ordinal": 1,
            "historical_operation": "updated",
            "current_object_sha256": batch_item["current_object_sha256"],
            "desired_object_sha256": digest("desired-object"),
            "target_evidence": {
                "artifact_id": "legacy-target-001",
                "payload_sha256": contract._document_sha256(evidence_payload),
                "payload": evidence_payload,
            },
            "authority": {
                "generation": batch["authority_generation"],
                "authority_fingerprint": batch["authority_fingerprint"],
            },
            "study_date": "2026-08-09",
            "frozen_at": "2026-08-09T04:02:00Z",
            "attempt": 1,
            "formal_write_count": 0,
        }
        work_item_sha = contract._document_sha256(luna_item)
        quality = {
            "schema_version": "verified_english_legacy_recuration_quality_v1",
            "receipt_sha256": digest("quality"),
            "package_sha256": digest("package"),
            "work_item_sha256": work_item_sha,
            "target_id": batch_item["target_id"],
            "target_kind": batch_item["target_kind"],
            "ordinal": 1,
            "inventory_sha256": batch["inventory_sha256"],
            "batch_authorization_sha256": batch[
                "batch_authorization_sha256"
            ],
            "target_authorization_receipt_sha256": mapping[
                "disposition_receipt_sha256"
            ],
            "proposal_action": "update_existing_proposal",
            "proposal_sha256": digest("proposal"),
            "quality_outcome": "accepted",
            "model_call_count": 2,
            "formal_write_count": 0,
        }
        converted = build_english_legacy_sol_work_item_v1(
            batch_authorization_sha256=batch[
                "batch_authorization_sha256"
            ],
            authorization=authorization,
            authorization_mapping=mapping,
            work_item=luna_item,
            quality_verification=quality,
        )
        self.assertEqual(converted["schema_version"], "english_legacy_sol_work_item_v1")
        self.assertEqual(converted["status"], "quality_passed")
        self.assertEqual(converted["model_call_count"], 2)
        tampered = json.loads(json.dumps(luna_item))
        tampered["target_evidence"]["payload"]["authorization"][
            "existing_object_only"
        ] = False
        tampered["target_evidence"]["payload_sha256"] = contract._document_sha256(
            tampered["target_evidence"]["payload"]
        )
        bad_quality = dict(quality)
        bad_quality["work_item_sha256"] = contract._document_sha256(tampered)
        with self.assertRaises(SubjectSolContractError) as mismatch:
            build_english_legacy_sol_work_item_v1(
                batch_authorization_sha256=batch[
                    "batch_authorization_sha256"
                ],
                authorization=authorization,
                authorization_mapping=mapping,
                work_item=tampered,
                quality_verification=bad_quality,
            )
        self.assertEqual(
            mismatch.exception.code,
            "english_legacy_work_item_authorization_binding_mismatch",
        )

    def test_direct_specialized_signers_are_disabled(self) -> None:
        publisher = IsolatedWriterAdapterPublisher(self.runtime)
        batch = self.build_batch(("already_current",))
        for call in (
            lambda: publisher.publish_english_legacy_item_review({}, batch=batch),
            lambda: publisher.publish_english_legacy_item_apply({}, batch=batch),
            lambda: publisher.publish_english_legacy_item_failure({}, batch=batch),
            lambda: publisher.publish_english_legacy_item_recovery({}, batch=batch),
        ):
            with self.assertRaises(IsolatedAuthorityPublisherError):
                call()


if __name__ == "__main__":
    unittest.main()
