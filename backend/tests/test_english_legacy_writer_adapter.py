from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import subject_sol_contract as contract  # noqa: E402
from english_legacy_writer_adapter import (  # noqa: E402
    ARTICLE_RELATIVE_PATH,
    EnglishLegacyWriterError,
    MASTER_RELATIVE_PATH,
    PATTERN_RELATIVE_PATH,
    REQUEST_SCHEMA,
    _read_master,
    _read_patterns,
    _master_row_bounds,
    _value_sha256,
    authority_manifest,
    execute_request,
    prepare_isolated_copy,
    source_unchanged,
    tree_manifest,
)
from isolated_authority_publishers import IsolatedWriterAdapterPublisher  # noqa: E402


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class EnglishLegacyWriterAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        (self.source / "bank").mkdir(parents=True)
        (self.source / "articles").mkdir()
        (self.source / "notes").mkdir()
        self.header = [
            "id", "date", "type", "item", "source_article",
            "source_sentence", "meaning", "usage", "writing_value", "tags",
            "review_note", "appear_count", "last_seen",
        ]
        self.rows = [
            {
                "id": "ROW-001", "date": "2026-08-09", "type": "单词",
                "item": "Alpha", "source_article": "article-a",
                "source_sentence": "Alpha appears.", "meaning": "甲",
                "usage": "u1", "writing_value": "一般", "tags": "tag",
                "review_note": "note", "appear_count": "1",
                "last_seen": "2026-08-09",
            },
            {
                "id": "ROW-002", "date": "2026-08-09", "type": "词组",
                "item": "Beta phrase", "source_article": "article-b",
                "source_sentence": "Beta appears.", "meaning": "乙",
                "usage": "u2", "writing_value": "适合", "tags": "tag2",
                "review_note": "note2", "appear_count": "2",
                "last_seen": "2026-08-09",
            },
        ]
        with (self.source / MASTER_RELATIVE_PATH).open(
            "w", encoding="utf-8-sig", newline=""
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=self.header, lineterminator="\n")
            writer.writeheader()
            writer.writerows(self.rows)
        (self.source / "bank/mastered_items.csv").write_text(
            "item,mastered_at\n", encoding="utf-8"
        )
        (self.source / PATTERN_RELATIVE_PATH).write_text(
            "# cards\n\n"
            "## SP-001｜Alpha structure\n\n"
            "**骨架**：`S + V`\n"
            "**中文解释**：甲。\n\n"
            "## SP-002｜Beta structure\n\n"
            "**骨架**：`S + V + O`\n"
            "**中文解释**：乙。\n",
            encoding="utf-8",
        )
        (self.source / ARTICLE_RELATIVE_PATH).write_text(
            "# 2011 English I Text 4\n\n"
            "- source_id：RAW-ARTICLE-20260710-001\n\n"
            "original body\n",
            encoding="utf-8",
        )
        (self.source / "notes/untouched.md").write_text("untouched\n", encoding="utf-8")
        self.source_authority = authority_manifest(self.source)
        self.source_tree = tree_manifest(self.source)
        self.isolated = self.root / "isolated"
        self.marker = prepare_isolated_copy(self.source, self.isolated)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def target_hash(self, kind: str, record_id: str) -> str:
        if kind == "master_bank_row":
            header, rows, _ = _read_master(self.isolated / MASTER_RELATIVE_PATH)
            row = next(row for row in rows if row["id"] == record_id)
            return _value_sha256({field: row[field] for field in header})
        if kind == "sentence_pattern_card":
            _, cards, _ = _read_patterns(self.isolated / PATTERN_RELATIVE_PATH)
            return next(card["sha256"] for card in cards if card["id"] == record_id)
        return hashlib.sha256(
            (self.isolated / record_id).read_bytes()
        ).hexdigest()

    def target_id(self, kind: str, record_id: str) -> str:
        return f"{kind}:{record_id}"

    def build_batch(
        self, kind: str, record_id: str, current_sha: str, action: str
    ) -> tuple[dict, Path]:
        target_id = self.target_id(kind, record_id)
        item = {
            "schema_version": "english_legacy_sol_work_item_v1",
            "parent_batch_authorization_sha256": digest("parent"),
            "inventory_sha256": digest("inventory"),
            "authorization_event_sha256": digest(f"event-{target_id}-{action}"),
            "target_authorization_receipt_sha256": digest(f"auth-{target_id}-{action}"),
            "target_id": target_id,
            "target_kind": kind,
            "ordinal": 1,
            "current_object_sha256": current_sha,
            "work_item_sha256": digest(f"work-{target_id}-{action}"),
            "authority_generation": "fixture-generation",
            "authority_fingerprint": digest("authority"),
            "proposal_sha256": digest(f"proposal-{target_id}-{action}"),
            "package_sha256": digest(f"package-{target_id}-{action}"),
            "quality_receipt_sha256": digest(f"quality-{target_id}-{action}"),
            "status": "quality_passed",
            "luna_terminal": True,
            "quality_passed": True,
            "quality_outcome": "accepted",
            "proposed_action": f"{action}_proposal",
            "model_call_count": 2,
            "formal_write_count": 0,
            "idempotency_key": f"fixture:{target_id}:{action}",
        }
        batch = {
            "schema_version": "english_legacy_recuration_sol_batch_v1",
            "issue_id": "EN-P0-006",
            "batch_id": f"BATCH-{digest(target_id + action)[:16]}",
            "subject": "english",
            "batch_authorization_sha256": digest("parent"),
            "authorization_expansion_closure_sha256": digest("closure"),
            "inventory_sha256": digest("inventory"),
            "target_set_sha256": digest(f"set-{target_id}"),
            "target_count": 1,
            "authority_generation": "fixture-generation",
            "authority_fingerprint": digest("authority"),
            "authorized_at": "2026-08-09T05:00:00Z",
            "work_items": [item],
            "status": "authorized",
            "formal_write_count": 0,
        }
        payload = contract._json_file_bytes(batch)
        sha = hashlib.sha256(payload).hexdigest()
        path = (
            self.runtime / "dispatch/english-legacy/sol-batches/sha256"
            / sha[:2] / f"{sha}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(0o400)
        return batch, path

    def publish_review(
        self,
        batch: dict,
        *,
        decision: str,
        desired_sha: str | None,
        suffix: str,
    ) -> tuple[str, Path]:
        item = batch["work_items"][0]
        core = {
            "issue_id": "EN-P0-006",
            "batch_id": batch["batch_id"],
            "batch_sha256": contract._document_sha256(batch),
            "subject": "english",
            "target_id": item["target_id"],
            "target_kind": item["target_kind"],
            "ordinal": 1,
            "fencing_token": 7,
            "attempt": 1,
            "previous_checkpoint_sha256": None,
            "canonical_evidence_sha256": digest(f"evidence-{suffix}"),
            "authority_checkpoint_sha256": digest("authority"),
            "current_object_sha256": item["current_object_sha256"],
            "decision": decision,
            "desired_object_sha256": desired_sha,
            "reason": "independent canonical review",
            "status": "rejected" if decision == "reject" else "approved",
            "formal_write_count": 0,
            "completed_at": "2026-08-09T05:01:00Z",
        }
        result = self.root / f"review-core-{suffix}"
        result.mkdir()
        (result / "review.json").write_bytes(contract._json_file_bytes(core))
        published = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_review_from_result_directory(
            result, batch=batch
        )
        review_sha = published[0]
        batch_sha = contract._document_sha256(batch)
        active_status = "failed" if decision == "reject" else "applying"
        item_status = "pending" if decision == "reject" else "reviewed"
        global_state = {
            "schema_version": contract.GLOBAL_SOL_WRITER_SCHEMA,
            "revision": 3,
            "next_fencing_token": 8,
            "queue": [{
                "batch_id": batch["batch_id"],
                "subject": "english",
                "authorized_at": batch["authorized_at"],
                "authorization_receipt_sha256": digest("authorization-receipt"),
                "daily_sol_batch_sha256": batch_sha,
                "status": "active",
            }],
            "active_writer": {
                "batch_id": batch["batch_id"],
                "subject": "english",
                "fencing_token": 7,
                "owner_id": "fixture-sol",
                "claimed_at": "2026-08-09T05:00:30Z",
                "authorization_receipt_sha256": digest("authorization-receipt"),
                "daily_sol_batch_sha256": batch_sha,
                "review_receipt_sha256": review_sha,
                "status": active_status,
                "current_item": item["target_id"],
                "committed_count": 0,
                "remaining_count": 1,
            },
            "active_writer_count": 1,
            "formal_write_count": 0,
            "updated_at": "2026-08-09T05:01:01Z",
        }
        state = {
            "schema_version": contract.ENGLISH_LEGACY_SOL_STATE_SCHEMA,
            "batch_id": batch["batch_id"],
            "batch_sha256": batch_sha,
            "status": "active",
            "attempt": 1,
            "fencing_token": 7,
            "current_ordinal": 1,
            "current_target_id": item["target_id"],
            "current_review_receipt_sha256": review_sha,
            "current_checkpoint_sha256": None,
            "items": [{
                "ordinal": 1,
                "target_id": item["target_id"],
                "status": item_status,
                "attempts": [{
                    "attempt": 1,
                    "fencing_token": 7,
                    "review_receipt_sha256": review_sha,
                    "apply_receipt_sha256": None,
                    "failure_receipt_sha256": None,
                    "recovery_receipt_sha256": None,
                }],
                "checkpoint_sha256": None,
                "formal_write_count": 0,
            }],
            "formal_write_count": 0,
            "execution_closure_sha256": None,
            "revision": 2,
            "updated_at": "2026-08-09T05:01:01Z",
        }
        global_path = self.runtime / "dispatch/state/global-sol-writer.json"
        global_path.parent.mkdir(parents=True, exist_ok=True)
        global_path.write_bytes(contract._json_file_bytes(global_state))
        state_path = (
            self.runtime / "dispatch/state/english-legacy-sol-batches"
            / f"{batch['batch_id']}.json"
        )
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_bytes(contract._json_file_bytes(state))
        return published

    def write_request(
        self,
        *,
        name: str,
        mode: str,
        batch_path: Path,
        review_path: Path,
        desired_object: object | None,
        fault: str | None = None,
    ) -> tuple[Path, Path]:
        result = self.root / f"result-{name}"
        request = {
            "schema_version": REQUEST_SCHEMA,
            "mode": mode,
            "runtime_root": str(self.runtime.resolve()),
            "batch_path": str(batch_path.resolve()),
            "review_receipt_path": str(review_path.resolve()),
            "isolated_root": str(self.isolated.resolve()),
            "result_dir": str(result.resolve()),
            "copy_id": self.marker["copy_id"],
            "desired_object": desired_object,
            "fault_injection": fault,
        }
        path = self.root / f"request-{name}.json"
        path.write_bytes(contract._json_file_bytes(request))
        return path, result

    def already_current_case(self, kind: str, record_id: str, name: str) -> None:
        current = self.target_hash(kind, record_id)
        batch, batch_path = self.build_batch(kind, record_id, current, "already_current")
        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None, suffix=name
        )
        request_path, result = self.write_request(
            name=name, mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        before = authority_manifest(self.isolated)["manifest_sha256"]
        summary = execute_request(request_path)
        after = authority_manifest(self.isolated)["manifest_sha256"]
        self.assertEqual(summary["outcome"], "already_current")
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertEqual(before, after)
        apply = json.loads((result / "apply.json").read_text(encoding="utf-8"))
        self.assertEqual(apply["operations"], [])
        self.assertEqual(apply["pre_authority_sha256"], apply["post_authority_sha256"])
        self.assertEqual(apply["pre_object_sha256"], apply["post_object_sha256"])
        self.assertFalse((result / "failure.json").exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(apply["writer_process"]["pid"], 0)

    def test_three_canonical_kinds_already_current_are_zero_write(self) -> None:
        cases = (
            ("master_bank_row", "ROW-001", "already-master"),
            ("sentence_pattern_card", "SP-001", "already-pattern"),
            (
                "article_learning_page",
                ARTICLE_RELATIVE_PATH.as_posix(),
                "already-article",
            ),
        )
        for kind, record_id, name in cases:
            with self.subTest(kind=kind):
                self.already_current_case(kind, record_id, name)
        self.assertTrue(source_unchanged(self.marker)["unchanged"])
        self.assertEqual(
            tree_manifest(self.source)["manifest_sha256"],
            self.source_tree["manifest_sha256"],
        )

    def test_master_update_has_one_typed_operation_and_publishes(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        desired = dict(self.rows[0])
        desired["meaning"] = "甲，已复核"
        desired_sha = _value_sha256(desired)
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "update_existing"
        )
        _, review_path = self.publish_review(
            batch, decision="update_existing", desired_sha=desired_sha,
            suffix="master-update",
        )
        request_path, result = self.write_request(
            name="master-update", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=desired,
        )
        untouched_before = self.target_hash("master_bank_row", "ROW-002")
        master_raw_before = (self.isolated / MASTER_RELATIVE_PATH).read_bytes()
        master_text_before, row_start_before, row_end_before, _ = _master_row_bounds(
            master_raw_before, "ROW-001"
        )
        summary = execute_request(request_path)
        self.assertEqual(summary["outcome"], "committed")
        self.assertEqual(summary["formal_write_count"], 1)
        self.assertEqual(self.target_hash("master_bank_row", "ROW-001"), desired_sha)
        self.assertEqual(self.target_hash("master_bank_row", "ROW-002"), untouched_before)
        master_text_after, row_start_after, row_end_after, _ = _master_row_bounds(
            (self.isolated / MASTER_RELATIVE_PATH).read_bytes(), "ROW-001"
        )
        self.assertEqual(
            master_text_before[:row_start_before], master_text_after[:row_start_after]
        )
        self.assertEqual(
            master_text_before[row_end_before:], master_text_after[row_end_after:]
        )
        apply = json.loads((result / "apply.json").read_text(encoding="utf-8"))
        self.assertEqual(len(apply["operations"]), 1)
        self.assertEqual(apply["operations"][0]["operation"], "update_existing")
        self.assertEqual(
            apply["writer_process"]["evidence_sha256"],
            hashlib.sha256((result / "writer-process-evidence.json").read_bytes()).hexdigest(),
        )
        self.assertEqual(
            apply["transaction_end"]["evidence_sha256"],
            hashlib.sha256((result / "transaction-evidence.json").read_bytes()).hexdigest(),
        )
        receipt_sha, receipt_path = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_apply_from_result_directory(
            result, batch=batch
        )
        self.assertEqual(len(receipt_sha), 64)
        self.assertTrue(receipt_path.is_file())
        finished = contract.SubjectSolRuntimeStore(
            self.runtime
        ).finish_english_legacy_item(receipt_sha)
        self.assertEqual(finished["state"]["status"], "complete")
        self.assertEqual(finished["state"]["formal_write_count"], 1)
        self.assertEqual(
            finished["execution_closure"]["status"], "verified_complete"
        )
        self.assertTrue(source_unchanged(self.marker)["unchanged"])

    def test_pattern_and_article_update_preserve_identity(self) -> None:
        cases = []
        _, cards, _ = _read_patterns(self.isolated / PATTERN_RELATIVE_PATH)
        pattern = next(card["segment"] for card in cards if card["id"] == "SP-001")
        desired_pattern = pattern.replace("甲。", "甲，已复核。")
        cases.append((
            "sentence_pattern_card", "SP-001", desired_pattern,
            hashlib.sha256(desired_pattern.encode("utf-8")).hexdigest(),
            "pattern-update",
        ))
        desired_article = (
            "# 2011 English I Text 4\n\n"
            "- source_id：RAW-ARTICLE-20260710-001\n\n"
            "reviewed body\n"
        )
        cases.append((
            "article_learning_page", ARTICLE_RELATIVE_PATH.as_posix(),
            desired_article, hashlib.sha256(desired_article.encode("utf-8")).hexdigest(),
            "article-update",
        ))
        for kind, record_id, desired, desired_sha, name in cases:
            with self.subTest(kind=kind):
                current = self.target_hash(kind, record_id)
                batch, batch_path = self.build_batch(
                    kind, record_id, current, "update_existing"
                )
                _, review_path = self.publish_review(
                    batch, decision="update_existing", desired_sha=desired_sha,
                    suffix=name,
                )
                request_path, result = self.write_request(
                    name=name, mode="apply", batch_path=batch_path,
                    review_path=review_path, desired_object=desired,
                )
                execute_request(request_path)
                self.assertEqual(self.target_hash(kind, record_id), desired_sha)
                apply = json.loads((result / "apply.json").read_text(encoding="utf-8"))
                self.assertEqual(len(apply["operations"]), 1)

    def test_dry_run_plans_update_without_mutation_or_apply_receipt(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        desired = dict(self.rows[0])
        desired["usage"] = "planned only"
        desired_sha = _value_sha256(desired)
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "update_existing"
        )
        _, review_path = self.publish_review(
            batch, decision="update_existing", desired_sha=desired_sha,
            suffix="dry",
        )
        request_path, result = self.write_request(
            name="dry", mode="dry_run", batch_path=batch_path,
            review_path=review_path, desired_object=desired,
        )
        before = authority_manifest(self.isolated)["manifest_sha256"]
        summary = execute_request(request_path)
        self.assertEqual(summary["outcome"], "planned_update")
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertEqual(before, authority_manifest(self.isolated)["manifest_sha256"])
        self.assertFalse((result / "apply.json").exists())
        self.assertFalse((result / "failure.json").exists())

    def test_fault_after_replace_rolls_back_and_emits_failure(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        desired = dict(self.rows[0])
        desired["review_note"] = "must roll back"
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "update_existing"
        )
        _, review_path = self.publish_review(
            batch, decision="update_existing", desired_sha=_value_sha256(desired),
            suffix="rollback",
        )
        request_path, result = self.write_request(
            name="rollback", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=desired,
            fault="after_replace_before_verify",
        )
        before = authority_manifest(self.isolated)["manifest_sha256"]
        summary = execute_request(request_path)
        self.assertEqual(summary["outcome"], "runtime_failure")
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertEqual(before, authority_manifest(self.isolated)["manifest_sha256"])
        failure = json.loads((result / "failure.json").read_text(encoding="utf-8"))
        self.assertEqual(failure["transaction_end"]["state"], "rolled_back")
        self.assertNotEqual(failure["writer_process"]["exit_code"], 0)
        self.assertEqual(
            failure["transaction_end"]["evidence_sha256"],
            hashlib.sha256((result / "transaction-evidence.json").read_bytes()).hexdigest(),
        )
        receipt_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_failure_from_result_directory(
            result, batch=batch
        )
        self.assertEqual(len(receipt_sha), 64)
        failed = contract.SubjectSolRuntimeStore(
            self.runtime
        ).record_english_legacy_item_failure(receipt_sha)
        self.assertEqual(failed["state"]["status"], "safe_paused")
        self.assertEqual(failed["state"]["formal_write_count"], 0)

    def test_duplicate_semantic_key_fails_closed(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "already_current"
        )
        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None,
            suffix="ambiguous",
        )
        duplicate = dict(self.rows[0])
        duplicate["id"] = "ROW-003"
        duplicate["item"] = "  alpha  "
        with (self.isolated / MASTER_RELATIVE_PATH).open(
            "a", encoding="utf-8", newline=""
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=self.header, lineterminator="\n")
            writer.writerow(duplicate)
        request_path, result = self.write_request(
            name="ambiguous", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        summary = execute_request(request_path)
        self.assertEqual(summary["outcome"], "runtime_failure")
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertTrue((result / "failure.json").is_file())

    def test_reject_review_emits_sol_review_failure_without_write(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "update_existing"
        )
        _, review_path = self.publish_review(
            batch, decision="reject", desired_sha=None, suffix="reject"
        )
        request_path, result = self.write_request(
            name="reject", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        before = authority_manifest(self.isolated)["manifest_sha256"]
        summary = execute_request(request_path)
        failure = json.loads((result / "failure.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["outcome"], "reject")
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertEqual(failure["failure_stage"], "sol_review")
        self.assertFalse(failure["retryable"])
        self.assertFalse((result / "apply.json").exists())
        self.assertEqual(before, authority_manifest(self.isolated)["manifest_sha256"])
        receipt_sha, _ = IsolatedWriterAdapterPublisher(
            self.runtime
        ).publish_english_legacy_item_failure_from_result_directory(
            result, batch=batch
        )
        self.assertEqual(len(receipt_sha), 64)

    def test_old_hmac_on_readdressed_review_is_rejected(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "already_current"
        )
        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None,
            suffix="tamper",
        )
        tampered = json.loads(review_path.read_text(encoding="utf-8"))
        tampered["reason"] = "tampered after signing"
        payload = contract._json_file_bytes(tampered)
        tampered_sha = hashlib.sha256(payload).hexdigest()
        tampered_path = (
            self.runtime / "dispatch/writer-adapter-receipts"
            / "english-legacy-item-review/sha256" / tampered_sha[:2]
            / f"{tampered_sha}.json"
        )
        tampered_path.parent.mkdir(parents=True, exist_ok=True)
        tampered_path.write_bytes(payload)
        tampered_path.chmod(0o400)
        request_path, _ = self.write_request(
            name="tamper", mode="apply", batch_path=batch_path,
            review_path=tampered_path, desired_object=None,
        )
        with self.assertRaises(EnglishLegacyWriterError) as caught:
            execute_request(request_path)
        self.assertEqual(caught.exception.code, "english_writer_review_hmac_invalid")

    def test_stale_runtime_review_and_wide_key_fail_before_writer(self) -> None:
        current = self.target_hash("master_bank_row", "ROW-001")
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "already_current"
        )
        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None,
            suffix="stale-state",
        )
        request_path, result = self.write_request(
            name="stale-state", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        global_path = self.runtime / "dispatch/state/global-sol-writer.json"
        global_state = json.loads(global_path.read_text(encoding="utf-8"))
        global_state["active_writer"]["review_receipt_sha256"] = digest("stale")
        global_path.write_bytes(contract._json_file_bytes(global_state))
        with self.assertRaises(EnglishLegacyWriterError) as stale:
            execute_request(request_path)
        self.assertEqual(stale.exception.code, "english_writer_verified_review_not_current")
        self.assertFalse(result.exists())

        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None,
            suffix="wide-key",
        )
        request_path, result = self.write_request(
            name="wide-key", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        key_path = (
            self.runtime / "dispatch/state/external-authorities"
            / "sol-writer-adapter.key"
        )
        key_path.chmod(0o644)
        with self.assertRaises(EnglishLegacyWriterError) as wide:
            execute_request(request_path)
        self.assertEqual(wide.exception.code, "english_writer_review_key_invalid")
        self.assertFalse(result.exists())

    def test_real_root_and_result_under_source_are_forbidden(self) -> None:
        with self.assertRaises(EnglishLegacyWriterError):
            prepare_isolated_copy(self.source, self.source)
        current = self.target_hash("master_bank_row", "ROW-001")
        batch, batch_path = self.build_batch(
            "master_bank_row", "ROW-001", current, "already_current"
        )
        _, review_path = self.publish_review(
            batch, decision="already_current", desired_sha=None,
            suffix="unsafe-result",
        )
        request_path, _ = self.write_request(
            name="unsafe-result", mode="apply", batch_path=batch_path,
            review_path=review_path, desired_object=None,
        )
        request = json.loads(request_path.read_text(encoding="utf-8"))
        request["result_dir"] = str((self.source / "forbidden-result").resolve())
        request_path.write_bytes(contract._json_file_bytes(request))
        with self.assertRaises(EnglishLegacyWriterError) as caught:
            execute_request(request_path)
        self.assertEqual(
            caught.exception.code,
            "english_writer_result_in_real_authority_forbidden",
        )


if __name__ == "__main__":
    unittest.main()
