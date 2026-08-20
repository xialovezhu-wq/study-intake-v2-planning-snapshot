from __future__ import annotations

import copy
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from lib.english_legacy_disposition import (
    AUTHORIZATION_EXPANSION_CLOSURE_SCHEMA,
    AUTHORIZED_OPERATIONS,
    BATCH_AUTHORIZATION_SCHEMA,
    BATCH_INTENT_SCHEMA,
    CLOSURE_SCHEMA,
    INVENTORY_SCHEMA,
    INVENTORY_REVIEW_RECEIPT_SCHEMA,
    INVENTORY_V3_SCHEMA,
    ISSUE_ID,
    RECEIPT_V3_SCHEMA,
    RECEIPT_SCHEMA,
    REMEDIATION_GATE_SCHEMA,
    TARGET_AUTHORIZATION_EVENT_SCHEMA,
    TEMPLATE_SCHEMA,
    EnglishLegacyDispositionError,
    EnglishLegacyDispositionStore,
    EnglishLegacyDispositionV3Store,
    build_complete_inventory_v3_draft,
    build_inventory_independent_review_receipt_draft,
    build_review_template,
    value_sha256,
)
from scripts import pre_model_p0_gate as gate
from scripts import english_legacy_disposition as disposition_cli
from lib import english_legacy_disposition as disposition_runtime


ROOT = Path(__file__).resolve().parents[1]
AUDIT_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-interface-audit-20260809"
)
ARTIFACT_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-model-lane-staging-20260809/artifacts/pre-model-gate"
)
REQUIREMENTS = ARTIFACT_ROOT / "en-p0-006-legacy-disposition-requirements.json"
REVIEW_TEMPLATE = ARTIFACT_ROOT / "en-p0-006-disposition-review-template-v2.json"
MATRIX = ARTIFACT_ROOT / "p0-matrix.json"
GOLDEN = ARTIFACT_ROOT / "zero-model-golden-inventory.json"
TEST_MCP_AUTHORITY_GENERATION = "english-test-generation"
TEST_MCP_AUTHORITY_FINGERPRINT = "b" * 64


class EnglishLegacyDispositionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="en-p0-006-")
        self.temp = Path(self.temporary.name)
        self.runtime_patches: list[mock._patch] = []
        self.key = self.temp / "authority.key"
        self.key.write_bytes(b"k" * 32)
        os.chmod(self.key, 0o600)
        self.store = EnglishLegacyDispositionStore(self.temp / "receipts", self.key)
        requirements = json.loads(REQUIREMENTS.read_text(encoding="utf-8"))

        self.persisted_review = json.loads(
            REVIEW_TEMPLATE.read_text(encoding="utf-8")
        )
        fixture_root = self.temp / "english-authority"
        fixture_root.mkdir()
        fixture_paths = {
            "master_bank_row": fixture_root / "master_bank.csv",
            "sentence_pattern_card": fixture_root / "sentence_patterns.md",
            "article_learning_page": fixture_root / "article.md",
            "mastered_items_boundary": fixture_root / "mastered_items.csv",
        }
        master_ids = sorted(
            row["record_id"]
            for row in self.persisted_review["targets"]
            if row["target_kind"] == "master_bank_row"
        )
        pattern_ids = sorted(
            row["record_id"]
            for row in self.persisted_review["targets"]
            if row["target_kind"] == "sentence_pattern_card"
        )
        fixture_paths["master_bank_row"].write_text(
            "id,last_seen\n"
            + "".join(f"{record_id},2026-08-06\n" for record_id in master_ids),
            encoding="utf-8",
        )
        fixture_paths["sentence_pattern_card"].write_text(
            "".join(
                f"## {record_id}｜isolated test fixture\nfixture\n\n"
                for record_id in pattern_ids
            ),
            encoding="utf-8",
        )
        fixture_paths["article_learning_page"].write_text(
            "isolated article fixture\n",
            encoding="utf-8",
        )
        fixture_paths["mastered_items_boundary"].write_text(
            "id\n",
            encoding="utf-8",
        )

        allowed_authority_paths = {
            str(path.resolve()): kind
            for kind, path in fixture_paths.items()
        }
        runtime_values = {
            "MASTER_BANK_PATH": fixture_paths["master_bank_row"],
            "PATTERN_PATH": fixture_paths["sentence_pattern_card"],
            "ARTICLE_PATH": fixture_paths["article_learning_page"],
            "MASTERED_ITEMS_PATH": fixture_paths["mastered_items_boundary"],
            "ALLOWED_AUTHORITY_PATHS": allowed_authority_paths,
            "HISTORICAL_AUTHORITY_ROLES": {},
        }
        cli_runtime = sys.modules[
            disposition_cli.build_review_template.__module__
        ]
        for runtime in (disposition_runtime, cli_runtime):
            for name, value in runtime_values.items():
                patcher = mock.patch.object(runtime, name, value)
                patcher.start()
                self.runtime_patches.append(patcher)

        for group in requirements["target_groups"]:
            authority_path = fixture_paths[group["target_kind"]]
            group["authority_path"] = str(authority_path.resolve())
            group["current_file_sha256"] = hashlib.sha256(
                authority_path.read_bytes()
            ).hexdigest()
        mastered = requirements["mastered_items_state"]
        mastered_path = fixture_paths["mastered_items_boundary"]
        mastered["authority_path"] = str(mastered_path.resolve())
        mastered["current_sha256"] = hashlib.sha256(
            mastered_path.read_bytes()
        ).hexdigest()
        self.requirements = self.temp / "requirements.json"
        self.requirements.write_text(
            json.dumps(requirements, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.requirements.chmod(0o444)
        self.review = build_review_template(requirements)

    def tearDown(self) -> None:
        for patcher in reversed(self.runtime_patches):
            patcher.stop()
        self.temporary.cleanup()

    def inventory_core(self, count: int = 1) -> dict:
        targets = []
        for pending in self.review["targets"][:count]:
            target = {
                key: copy.deepcopy(pending[key])
                for key in (
                    "target_id",
                    "target_kind",
                    "record_id",
                    "authority_path",
                    "authority_file_sha256",
                    "current_object_sha256",
                    "prehash_status",
                    "prehash_sha256",
                    "originating_thread_id",
                )
            }
            target["independent_review"] = {
                "status": "completed",
                "verdict": "evidence_incomplete",
                "receipt_sha256": hashlib.sha256(
                    f"review:{target['target_id']}".encode()
                ).hexdigest(),
            }
            targets.append(target)
        targets.sort(key=lambda row: row["target_id"])
        target_set = [
            {
                "target_id": row["target_id"],
                "target_kind": row["target_kind"],
                "record_id": row["record_id"],
                "authority_path": row["authority_path"],
                "authority_file_sha256": row["authority_file_sha256"],
                "current_object_sha256": row["current_object_sha256"],
            }
            for row in targets
        ]
        return {
            "schema_version": INVENTORY_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": "english",
            "originating_thread_id": self.review["originating_thread_id"],
            "inventory_id": "EN-P0-006-INVENTORY-TEST",
            "inventory_status": "complete",
            "authority": copy.deepcopy(self.review["authority"]),
            "targets": targets,
            "target_count": len(targets),
            "target_set_sha256": value_sha256(target_set),
            "unidentified_target_count": 0,
            "mastered_items_boundary": copy.deepcopy(
                self.review["mastered_items_boundary"]
            ),
            "independent_inventory_review": {
                "status": "completed",
                "receipt_sha256": "a" * 64,
            },
            "model_call_count": 0,
            "formal_write_count": 0,
            "issued_at": "2026-08-09T04:00:00Z",
        }

    @staticmethod
    def user_event(
        inventory_sha256: str,
        target_id: str,
        disposition: str,
        *,
        event_id: str = "USER-EN-P0-006-001",
    ) -> dict:
        return {
            "event_id": event_id,
            "event_type": "explicit_user_english_legacy_disposition",
            "issue_id": ISSUE_ID,
            "inventory_sha256": inventory_sha256,
            "target_id": target_id,
            "disposition": disposition,
            "authorization_scope": "single_target_only",
            "user_message_sha256": hashlib.sha256(event_id.encode()).hexdigest(),
            "authorized_at": "2026-08-09T04:01:00Z",
        }

    def issue_inventory(self, count: int = 1) -> tuple[str, dict]:
        digest, _path, inventory = self.store.seal_inventory(
            self.inventory_core(count)
        )
        return digest, inventory

    def issue_receipt(
        self,
        inventory_sha: str,
        target_id: str,
        disposition: str = "deterministic_recuration",
        *,
        event_id: str = "USER-EN-P0-006-001",
    ) -> tuple[str, dict]:
        digest, _path, receipt = self.store.issue_target_receipt(
            inventory_sha,
            target_id=target_id,
            disposition=disposition,
            user_authorization=self.user_event(
                inventory_sha,
                target_id,
                disposition,
                event_id=event_id,
            ),
            issued_at="2026-08-09T04:02:00Z",
        )
        return digest, receipt

    def test_review_template_is_exactly_97_targets_and_not_signable(self) -> None:
        self.assertEqual(
            [row["target_id"] for row in self.persisted_review["targets"]],
            [row["target_id"] for row in self.review["targets"]],
        )
        self.assertEqual(self.review["schema_version"], TEMPLATE_SCHEMA)
        self.assertEqual(self.review["identified_target_count"], 97)
        self.assertEqual(self.review["unidentified_target_count_lower_bound"], 1)
        self.assertEqual(len(self.review["targets"]), 97)
        self.assertFalse(self.review["closure_eligible"])
        self.assertEqual(self.review["disposition_receipt_count"], 0)
        self.assertTrue(
            all(row["user_selection"]["disposition"] is None for row in self.review["targets"])
        )
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_inventory_invalid"
        ):
            self.store.seal_inventory(self.review)

    def test_schema_copies_are_exact_and_validate_real_objects(self) -> None:
        inventory_sha, inventory = self.issue_inventory()
        target_id = inventory["targets"][0]["target_id"]
        receipt_sha, receipt = self.issue_receipt(inventory_sha, target_id)
        _closure_sha, _path, closure = self.store.issue_closure(
            inventory_sha,
            [receipt_sha],
            issued_at="2026-08-09T04:03:00Z",
        )
        values = {
            "english-legacy-target-inventory-v2.json": inventory,
            "english-legacy-disposition-receipt-v2.json": receipt,
            "english-legacy-disposition-closure-v2.json": closure,
            "english-legacy-disposition-review-template-v2.json": self.review,
        }
        for name, value in values.items():
            runtime = ROOT / "schemas" / name
            plugin = ROOT / "plugin" / "kaoyan-study-intake" / "schemas" / name
            self.assertEqual(runtime.read_bytes(), plugin.read_bytes())
            schema = json.loads(runtime.read_text(encoding="utf-8"))
            self.assertEqual(
                schema["$schema"],
                "https://json-schema.org/draft/2020-12/schema",
            )
            self.assertFalse(schema["additionalProperties"])
            self.assertEqual(value["schema_version"], schema["properties"]["schema_version"]["const"])

    def test_complete_explicit_single_target_round_trip(self) -> None:
        inventory_sha, inventory = self.issue_inventory()
        target_id = inventory["targets"][0]["target_id"]
        receipt_sha, receipt = self.issue_receipt(
            inventory_sha,
            target_id,
            "legacy_attestation",
        )
        closure_sha, _path, closure = self.store.issue_closure(
            inventory_sha,
            [receipt_sha],
            issued_at="2026-08-09T04:03:00Z",
        )
        result = self.store.verify_closure(closure_sha)
        self.assertEqual(receipt["schema_version"], RECEIPT_SCHEMA)
        self.assertEqual(closure["schema_version"], CLOSURE_SCHEMA)
        self.assertEqual(result["status"], "verified_complete")
        self.assertEqual(result["target_count"], 1)
        self.assertEqual(result["receipt_count"], 1)
        self.assertFalse(result["mastered_items_inference_used"])
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["formal_write_count"], 0)

    def test_unknown_target_and_omission_fail_closed(self) -> None:
        inventory_sha, inventory = self.issue_inventory(2)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_disposition_unknown_target"
        ):
            self.store.issue_target_receipt(
                inventory_sha,
                target_id="master_bank_row:UNKNOWN",
                disposition="rollback",
                user_authorization=self.user_event(
                    inventory_sha,
                    "master_bank_row:UNKNOWN",
                    "rollback",
                ),
            )
        receipt_sha, _receipt = self.issue_receipt(
            inventory_sha,
            inventory["targets"][0]["target_id"],
        )
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_disposition_omitted_target"
        ):
            self.store.issue_closure(inventory_sha, [receipt_sha])

    def test_duplicate_target_receipts_fail_closed(self) -> None:
        inventory_sha, inventory = self.issue_inventory()
        target_id = inventory["targets"][0]["target_id"]
        first, _ = self.issue_receipt(
            inventory_sha,
            target_id,
            event_id="USER-EN-P0-006-FIRST",
        )
        second, _ = self.issue_receipt(
            inventory_sha,
            target_id,
            event_id="USER-EN-P0-006-SECOND",
        )
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_disposition_duplicate_target"
        ):
            self.store.issue_closure(inventory_sha, sorted([first, second]))

    def test_hmac_tamper_and_wrong_authority_key_fail_closed(self) -> None:
        inventory_sha, inventory = self.issue_inventory()
        target_id = inventory["targets"][0]["target_id"]
        receipt_sha, receipt = self.issue_receipt(inventory_sha, target_id)
        tampered = copy.deepcopy(receipt)
        tampered["user_authorization"]["user_message_sha256"] = "0" * 64
        payload = json.dumps(
            tampered,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode() + b"\n"
        tampered_sha = hashlib.sha256(payload).hexdigest()
        tampered_path = (
            self.temp
            / "receipts"
            / "dispositions"
            / "sha256"
            / tampered_sha[:2]
            / f"{tampered_sha}.json"
        )
        tampered_path.parent.mkdir(parents=True, exist_ok=True)
        tampered_path.write_bytes(payload)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_receipt_hmac_invalid"
        ):
            self.store.verify_target_receipt(
                tampered_sha,
                inventory=inventory,
                inventory_sha256=inventory_sha,
            )

        closure_sha, _path, _closure = self.store.issue_closure(
            inventory_sha,
            [receipt_sha],
        )
        other_key = self.temp / "other.key"
        other_key.write_bytes(b"z" * 32)
        os.chmod(other_key, 0o600)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_receipt_hmac_invalid"
        ):
            EnglishLegacyDispositionStore(
                self.temp / "receipts", other_key
            ).verify_closure(closure_sha)

    def test_stale_hash_and_mastered_inference_fail_closed(self) -> None:
        core = self.inventory_core()
        core["mastered_items_boundary"]["used_for_disposition"] = True
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError,
            "legacy_mastered_items_inference_forbidden",
        ):
            self.store.seal_inventory(core)

        core = self.inventory_core()
        core["targets"][0]["current_object_sha256"] = "0" * 64
        binding = [
            {
                "target_id": row["target_id"],
                "target_kind": row["target_kind"],
                "record_id": row["record_id"],
                "authority_path": row["authority_path"],
                "authority_file_sha256": row["authority_file_sha256"],
                "current_object_sha256": row["current_object_sha256"],
            }
            for row in core["targets"]
        ]
        core["target_set_sha256"] = value_sha256(binding)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_inventory_stale_target"
        ):
            self.store.seal_inventory(core)

    def test_authority_fingerprint_mismatch_fails_closed(self) -> None:
        core = self.inventory_core()
        core["authority"]["generation"] = "forged-generation"
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_inventory_authority_mismatch"
        ):
            self.store.seal_inventory(core)

    def test_cli_renders_review_only_template_without_receipts(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            returncode = disposition_cli.main(
                [
                "render-review-template",
                "--requirements",
                str(self.requirements),
                ]
            )
        self.assertEqual(returncode, 0, stdout.getvalue())
        value = json.loads(stdout.getvalue())
        self.assertEqual(value, self.review)
        self.assertFalse((self.temp / "receipts").exists())

    def test_fixture_is_independent_of_historical_environment(self) -> None:
        requirements = json.loads(self.requirements.read_text(encoding="utf-8"))
        with mock.patch.dict(
            os.environ,
            {
                "STUDY_PREPROCESSOR_HISTORICAL_TEST_INPUT_MANIFEST": (
                    "/nonexistent/host-dependent-manifest.json"
                )
            },
        ):
            value = build_review_template(requirements)
        self.assertEqual(value, self.review)
        fixture_root = (self.temp / "english-authority").resolve()
        self.assertTrue(
            all(
                Path(row["authority_path"]).resolve().is_relative_to(fixture_root)
                for row in value["authority"]["files"]
            )
        )

    def test_cli_initializes_isolated_batch_authority_key_idempotently(self) -> None:
        key = (
            self.temp
            / "runtime/dispatch/state/external-authorities"
            / "english-legacy-batch-authorization.key"
        )
        command = [
            sys.executable,
            str(ROOT / "scripts" / "english_legacy_disposition.py"),
            "initialize-authority-key",
            "--authority-key",
            str(key),
        ]
        first = subprocess.run(command, check=True, capture_output=True, text=True)
        first_value = json.loads(first.stdout)
        first_bytes = key.read_bytes()
        second = subprocess.run(command, check=True, capture_output=True, text=True)
        second_value = json.loads(second.stdout)
        self.assertEqual(first_value["status"], "created")
        self.assertEqual(second_value["status"], "already_initialized")
        self.assertEqual(first_value["authority_key_id"], second_value["authority_key_id"])
        self.assertEqual(key.read_bytes(), first_bytes)
        self.assertEqual(len(first_bytes), 32)
        self.assertEqual(key.stat().st_mode & 0o777, 0o600)
        self.assertEqual(first_value["model_call_count"], 0)
        self.assertEqual(first_value["formal_write_count"], 0)

    def test_gate_accepts_only_verified_v2_closure_for_closed_matrix(self) -> None:
        inventory_sha, inventory = self.issue_inventory()
        target_id = inventory["targets"][0]["target_id"]
        receipt_sha, _receipt = self.issue_receipt(inventory_sha, target_id)
        closure_sha, _path, _closure = self.store.issue_closure(
            inventory_sha,
            [receipt_sha],
        )
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        for issue in matrix["issues"]:
            issue["gate_status"] = "closed"
            issue["closure_checks"] = [
                {
                    "check_id": f"test-{issue['issue_id']}",
                    "status": "passed",
                    "evidence": "isolated test fixture",
                }
            ]
            if issue["issue_id"] == ISSUE_ID:
                issue["implementation_state"] = "signed_v2_closure_fixture"
                issue["requires_user_disposition"] = True
                issue["legacy_disposition"]["inventory_status"] = "complete"
                issue["legacy_disposition"]["identified_target_count"] = 1
                issue["legacy_disposition"]["unidentified_target_count"] = 0
                issue["legacy_disposition"]["disposition_receipt_count"] = 1
        matrix_path = self.temp / "matrix.json"
        matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
        with self.assertRaisesRegex(
            gate.P0GateError,
            "en_p0_006_requires_verified_closure",
        ):
            gate.evaluate_gate(matrix_path, AUDIT_ROOT, GOLDEN)
        result = gate.evaluate_gate(
            matrix_path,
            AUDIT_ROOT,
            GOLDEN,
            english_legacy_receipt_root=self.temp / "receipts",
            english_legacy_closure_sha256=closure_sha,
            english_legacy_authority_key_path=self.key,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["blocking_issue_ids"], [])
        self.assertEqual(
            result["english_legacy_disposition_closure"]["status"],
            "verified_complete",
        )

    def test_gate_rejects_partial_v2_arguments(self) -> None:
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        with self.assertRaisesRegex(
            gate.P0GateError, "en_p0_006_signed_v2_closure_args_incomplete"
        ):
            gate.evaluate_gate(
                MATRIX,
                AUDIT_ROOT,
                GOLDEN,
                english_legacy_receipt_root=self.temp / "receipts",
            )


class EnglishLegacyBatchDispositionV3Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="en-p0-006-v3-")
        self.temp = Path(self.temporary.name)
        self.key = self.temp / "authority.key"
        self.key.write_bytes(b"v" * 32)
        os.chmod(self.key, 0o600)
        self.store = EnglishLegacyDispositionV3Store(
            self.temp / "receipts", self.key
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_materialization_reads_live_english_mcp_authority_from_candidate(self) -> None:
        config_path = self.temp / "config.json"
        release_manifest = self.temp / "release.json"
        release_manifest.write_text(
            json.dumps({"release_id": "a" * 64}), encoding="utf-8"
        )
        config_path.write_text(
            json.dumps(
                {
                    "processing_plugin": {"enabled": True},
                    "runtime_root": str(self.temp / "runtime"),
                    "release": {"manifest_path": str(release_manifest)},
                }
            ),
            encoding="utf-8",
        )
        host = mock.Mock()
        host.subject_authority_snapshot.return_value = {
            "schema_version": "subject_authority_snapshot_v1",
            "subject": "english",
            "generation": TEST_MCP_AUTHORITY_GENERATION,
            "authority_fingerprint": TEST_MCP_AUTHORITY_FINGERPRINT,
            "mcp_server_release": "0.3.0+sha256." + "c" * 64,
            "route_request_id": "batch-authority-english-test",
            "scope_sha256": "d" * 64,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        with mock.patch.object(
            disposition_cli,
            "ProcessingPluginHost",
            return_value=host,
        ) as constructor:
            authority = disposition_cli._live_english_mcp_authority(config_path)
        constructor.assert_called_once()
        host.subject_authority_snapshot.assert_called_once_with("english")
        self.assertEqual(
            authority["authority_fingerprint"],
            TEST_MCP_AUTHORITY_FINGERPRINT,
        )

    def test_rollout_window_is_stable_across_tail_append_and_tamper_fails_closed(self) -> None:
        selected: list[str] = []
        with disposition_runtime.ROLLOUT_PATH.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                value = json.loads(raw_line)
                timestamp = value.get("timestamp")
                payload = value.get("payload")
                if (
                    isinstance(timestamp, str)
                    and disposition_runtime.MUTATION_WINDOW_START
                    <= timestamp
                    < disposition_runtime.MUTATION_WINDOW_END
                    and value.get("type") == "event_msg"
                    and isinstance(payload, dict)
                    and payload.get("type") == "patch_apply_end"
                ):
                    selected.append(raw_line)
        self.assertEqual(len(selected), 11)
        tail = json.dumps(
            {
                "timestamp": "2026-08-12T00:00:00Z",
                "type": "event_msg",
                "payload": {"type": "unrelated_append"},
            },
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        snapshot = self.temp / "rollout-window-with-tail.jsonl"
        snapshot.write_text("".join(selected) + tail, encoding="utf-8")
        with mock.patch.object(disposition_runtime, "ROLLOUT_PATH", snapshot):
            rows = disposition_runtime._rollout_patch_events(snapshot)
        self.assertEqual(len(rows), 11)

        tampered = json.loads(selected[0])
        tampered["payload"]["call_id"] = "exec-window-tampered"
        snapshot.write_text(
            json.dumps(tampered, separators=(",", ":"))
            + "\n"
            + "".join(selected[1:])
            + tail,
            encoding="utf-8",
        )
        with mock.patch.object(disposition_runtime, "ROLLOUT_PATH", snapshot):
            with self.assertRaisesRegex(
                EnglishLegacyDispositionError,
                "legacy_rollout_window_hash_mismatch",
            ):
                disposition_runtime._rollout_patch_events(snapshot)

    def inventory_draft(
        self, *, inventory_id: str = "EN-P0-006-INVENTORY-V3-TEST"
    ) -> dict:
        review_core = build_inventory_independent_review_receipt_draft(
            review_id="EN-P0-006-INVENTORY-REVIEW-TEST",
            reviewer_identity="independent-test-reviewer",
            reviewed_at="2026-08-09T09:30:00Z",
            mcp_authority_generation=TEST_MCP_AUTHORITY_GENERATION,
            mcp_authority_fingerprint=TEST_MCP_AUTHORITY_FINGERPRINT,
        )
        review_sha256, _path, _receipt = (
            self.store.seal_inventory_independent_review_receipt(review_core)
        )
        return build_complete_inventory_v3_draft(
            inventory_id=inventory_id,
            independent_review_receipt_sha256=review_sha256,
            issued_at="2026-08-09T10:00:00Z",
            mcp_authority_generation=TEST_MCP_AUTHORITY_GENERATION,
            mcp_authority_fingerprint=TEST_MCP_AUTHORITY_FINGERPRINT,
        )

    @staticmethod
    def intent_core() -> dict:
        return {
            "schema_version": BATCH_INTENT_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": "english",
            "intent_id": "EN-P0-006-INTENT-USER-TURN",
            "authorization_type": "explicit_user_batch_authorization",
            "inventory_scope": (
                "first_independently_verified_complete_inventory"
            ),
            "disposition": "deterministic_recuration",
            "authorized_operations": list(AUTHORIZED_OPERATIONS),
            "exact_inventory_only": True,
            "one_shot": True,
            "user_message_sha256": "e" * 64,
            "source_thread_id": "test-thread",
            "source_turn_id": "test-user-turn",
            "authorized_at": "2026-08-09T09:00:00Z",
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def full_expansion(self) -> tuple[str, str, str, dict]:
        inventory_sha, _path, _inventory = self.store.seal_inventory_v3(
            self.inventory_draft()
        )
        intent_sha, _path, _intent = self.store.seal_batch_intent(
            self.intent_core()
        )
        batch_sha, _path, _authorization = (
            self.store.materialize_batch_authorization(
                intent_sha256=intent_sha,
                inventory_sha256=inventory_sha,
            )
        )
        closure_sha, _path, closure = self.store.expand_batch_authorization(
            batch_sha
        )
        return inventory_sha, intent_sha, closure_sha, closure

    def test_complete_rollout_inventory_is_98_with_sp_022(self) -> None:
        draft = self.inventory_draft()
        counts = {
            kind: sum(1 for row in draft["targets"] if row["target_kind"] == kind)
            for kind in (
                "master_bank_row",
                "sentence_pattern_card",
                "article_learning_page",
            )
        }
        self.assertEqual(draft["schema_version"], INVENTORY_V3_SCHEMA)
        self.assertEqual(draft["target_count"], 98)
        self.assertEqual(
            counts,
            {
                "master_bank_row": 88,
                "sentence_pattern_card": 9,
                "article_learning_page": 1,
            },
        )
        sp_022 = next(
            row for row in draft["targets"] if row["record_id"] == "SP-022"
        )
        self.assertEqual(
            sp_022["historical_operation"], "representation_reorder_only"
        )
        self.assertEqual(len(sp_022["origin_write_evidence"]), 1)
        self.assertEqual(
            draft["write_set_proof"]["successful_apply_patch_call_count"], 11
        )
        self.assertEqual(
            draft["write_set_proof"]["mutation_target_count"], 98
        )
        self.assertEqual(
            draft["write_set_proof"]["current_authority_candidate_count"], 98
        )
        side_effects = draft["write_set_proof"]["projection_side_effects"]
        self.assertEqual(len(side_effects), 6)
        self.assertTrue(
            all(
                row["role"] == "derived_projection"
                and row["counts_as_formal_target"] is False
                for row in side_effects
            )
        )
        self.assertEqual(
            [row["ordinal"] for row in draft["targets"]], list(range(1, 99))
        )

    def test_batch_intent_expands_to_distinct_per_target_hmac_evidence(self) -> None:
        inventory_sha, intent_sha, closure_sha, closure = self.full_expansion()
        verification = self.store.verify_authorization_expansion_closure(
            closure_sha
        )
        reopened = self.store.reopen_verified_authorization_expansion_closure(
            closure_sha
        )
        self.assertEqual(
            closure["schema_version"], AUTHORIZATION_EXPANSION_CLOSURE_SCHEMA
        )
        self.assertEqual(verification["status"], "verified_complete")
        self.assertEqual(verification["target_count"], 98)
        self.assertEqual(verification["event_count"], 98)
        self.assertEqual(verification["receipt_count"], 98)
        self.assertEqual(closure["intent_sha256"], intent_sha)
        self.assertEqual(closure["inventory_sha256"], inventory_sha)
        self.assertEqual(reopened, closure)
        self.assertEqual(
            len(
                {
                    row["authorization_event_sha256"]
                    for row in closure["target_authorizations"]
                }
            ),
            98,
        )
        first = closure["target_authorizations"][0]
        event = self.store._read(
            "authorization-events", first["authorization_event_sha256"]
        )
        receipt = self.store._read(
            "dispositions", first["disposition_receipt_sha256"]
        )
        self.assertEqual(event["schema_version"], TARGET_AUTHORIZATION_EVENT_SCHEMA)
        self.assertEqual(
            event["event_type"],
            "materialized_user_english_legacy_disposition",
        )
        self.assertEqual(
            event["authorization_scope"], "exact_target_from_batch"
        )
        self.assertNotEqual(
            event["event_type"], "explicit_user_english_legacy_disposition"
        )
        self.assertEqual(receipt["schema_version"], RECEIPT_V3_SCHEMA)
        self.assertEqual(
            receipt["authorization_event_sha256"],
            first["authorization_event_sha256"],
        )

    def test_expansion_is_byte_stable_and_intent_is_one_shot(self) -> None:
        inventory_sha, intent_sha, first_sha, closure = self.full_expansion()
        batch_sha = closure["batch_authorization_sha256"]
        second_sha, _path, second = self.store.expand_batch_authorization(batch_sha)
        self.assertEqual(first_sha, second_sha)
        self.assertEqual(closure, second)

        other_inventory_sha, _path, _inventory = self.store.seal_inventory_v3(
            self.inventory_draft(inventory_id="EN-P0-006-INVENTORY-V3-OTHER")
        )
        self.assertNotEqual(inventory_sha, other_inventory_sha)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError,
            "legacy_batch_intent_already_consumed",
        ):
            self.store.materialize_batch_authorization(
                intent_sha256=intent_sha,
                inventory_sha256=other_inventory_sha,
            )

    def test_inventory_omission_and_target_set_drift_fail_closed(self) -> None:
        draft = self.inventory_draft()
        draft["targets"] = draft["targets"][:-1]
        draft["target_count"] -= 1
        draft["target_set_sha256"] = value_sha256([])
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError,
            "legacy_inventory_review_binding_mismatch|legacy_inventory_target_set_mismatch|legacy_inventory_reconciliation_mismatch",
        ):
            self.store.seal_inventory_v3(draft)

    def test_inventory_requires_reopenable_hmac_independent_review(self) -> None:
        draft = build_complete_inventory_v3_draft(
            inventory_id="EN-P0-006-INVENTORY-FAKE-REVIEW",
            independent_review_receipt_sha256="f" * 64,
            issued_at="2026-08-09T10:00:00Z",
            mcp_authority_generation=TEST_MCP_AUTHORITY_GENERATION,
            mcp_authority_fingerprint=TEST_MCP_AUTHORITY_FINGERPRINT,
        )
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_content_address_invalid"
        ):
            self.store.seal_inventory_v3(draft)

    def test_inventory_separates_write_set_and_live_mcp_authority(self) -> None:
        draft = self.inventory_draft()
        self.assertEqual(
            draft["authority"]["generation"],
            "english-legacy-en-p0-006-write-set-v3",
        )
        self.assertEqual(
            draft["authority"]["fingerprint"],
            value_sha256(
                {
                    "issue_id": "EN-P0-006",
                    "subject": "english",
                    "generation": draft["authority"]["generation"],
                    "files": draft["authority"]["files"],
                }
            ),
        )
        self.assertEqual(
            draft["mcp_authority"],
            {
                "generation": TEST_MCP_AUTHORITY_GENERATION,
                "authority_fingerprint": TEST_MCP_AUTHORITY_FINGERPRINT,
            },
        )
        self.assertTrue(
            all(
                target["authority_generation"]
                == TEST_MCP_AUTHORITY_GENERATION
                and target["authority_fingerprint"]
                == TEST_MCP_AUTHORITY_FINGERPRINT
                for target in draft["targets"]
            )
        )

    def test_v3_target_event_hmac_tamper_fails_closed(self) -> None:
        _inventory_sha, _intent_sha, _closure_sha, closure = self.full_expansion()
        batch_sha = closure["batch_authorization_sha256"]
        authorization = self.store.verify_batch_authorization(batch_sha)
        inventory = self.store.verify_inventory_v3(closure["inventory_sha256"])
        first = closure["target_authorizations"][0]
        event = self.store._read(
            "authorization-events", first["authorization_event_sha256"]
        )
        tampered = copy.deepcopy(event)
        tampered["user_message_sha256"] = "0" * 64
        payload = (
            json.dumps(
                tampered,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        digest = hashlib.sha256(payload).hexdigest()
        path = (
            self.temp
            / "receipts"
            / "authorization-events"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError, "legacy_receipt_hmac_invalid"
        ):
            self.store.verify_target_authorization_event_v3(
                digest,
                batch_authorization_sha256=batch_sha,
                authorization=authorization,
                inventory=inventory,
            )

    def test_existing_inventory_closure_revokes_recuration_gate(self) -> None:
        _inventory_sha, _intent_sha, closure_sha, closure = self.full_expansion()
        result = self.store.verify_existing_inventory_no_recuration_required(
            closure_sha
        )
        self.assertEqual(result["status"], "verified_complete")
        self.assertEqual(
            result["outcome"],
            "verified_existing_inventory_no_recuration_required",
        )
        self.assertEqual(result["target_count"], 98)
        self.assertEqual(result["per_target_hmac_receipt_count"], 98)
        self.assertEqual(result["target_kind_counts"]["master_bank_row"], 88)
        self.assertTrue(result["sp_022_verified"])
        self.assertFalse(result["luna_recuration_allowed"])
        self.assertFalse(result["sol_parent_batch_allowed"])
        with self.assertRaisesRegex(
            EnglishLegacyDispositionError,
            "legacy_recuration_authorization_revoked",
        ):
            self.store.evaluate_remediation_gate(closure_sha)
        with self.assertRaisesRegex(
            gate.P0GateError, "en_p0_006_requires_verified_closure"
        ):
            gate.require_model_lane_ready(MATRIX, AUDIT_ROOT, GOLDEN)
        with self.assertRaisesRegex(
            gate.P0GateError,
            "en_p0_006_recuration_authorization_revoked",
        ):
            gate.require_en_p0_006_remediation_lane_ready(
                MATRIX,
                AUDIT_ROOT,
                GOLDEN,
                english_legacy_authorization_expansion_receipt_root=(
                    self.temp / "receipts"
                ),
                english_legacy_authorization_expansion_closure_sha256=(
                    closure_sha
                ),
                english_legacy_authorization_expansion_authority_key_path=(
                    self.key
                ),
            )

    def test_root_v3_schemas_are_strict_and_match_runtime_values(self) -> None:
        _inventory_sha, intent_sha, closure_sha, closure = self.full_expansion()
        batch = self.store._read(
            "batch-authorizations", closure["batch_authorization_sha256"]
        )
        intent = self.store._read("batch-intents", intent_sha)
        first = closure["target_authorizations"][0]
        event = self.store._read(
            "authorization-events", first["authorization_event_sha256"]
        )
        receipt = self.store._read(
            "dispositions", first["disposition_receipt_sha256"]
        )
        inventory = self.store._read("inventories", closure["inventory_sha256"])
        review_sha = inventory["independent_inventory_review"]["receipt_sha256"]
        review = self.store.verify_inventory_independent_review_receipt(review_sha)
        values = {
            "english-legacy-target-inventory-v3.json": inventory,
            "english-legacy-inventory-independent-review-receipt-v1.json": review,
            "english-legacy-batch-authorization-intent-v1.json": intent,
            "english-legacy-batch-authorization-v1.json": batch,
            "english-legacy-target-authorization-event-v3.json": event,
            "english-legacy-disposition-receipt-v3.json": receipt,
            "english-legacy-authorization-expansion-closure-v1.json": closure,
        }
        for name, value in values.items():
            schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
            self.assertFalse(schema["additionalProperties"])
            self.assertEqual(
                schema["properties"]["schema_version"]["const"],
                value["schema_version"],
            )


if __name__ == "__main__":
    unittest.main()
