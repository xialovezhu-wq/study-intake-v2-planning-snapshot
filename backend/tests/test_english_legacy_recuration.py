from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from english_legacy_recuration import (  # noqa: E402
    EnglishLegacyRecurationError,
    _attempt_paths,
    _stage_evidence,
    build_authorized_work_items,
    reopen_work_item_batch,
    run_work_item,
    validate_analysis,
    validate_critical_review,
    validate_work_item,
)
from english_legacy_disposition import (  # noqa: E402
    AUTHORIZED_OPERATIONS,
    BATCH_INTENT_SCHEMA,
    EnglishLegacyDispositionV3Store,
    build_complete_inventory_v3_draft,
    build_inventory_independent_review_receipt_draft,
)
from preprocessor_core import (  # noqa: E402
    StructuredStageResult,
    sha256_file,
    sha256_value,
)


def ref(char: str) -> str:
    return "mcp-item:english:" + char * 64


TEST_MCP_AUTHORITY_GENERATION = "english-test-generation"
TEST_MCP_AUTHORITY_FINGERPRINT = "b" * 64


class EnglishLegacyRecurationTests(unittest.TestCase):
    def work_item(self, *, current: str = "1", desired: str = "2") -> dict:
        payload = {
            "schema_version": "english_legacy_target_evidence_v1",
            "target_id": "sentence_pattern_card:SP-022",
            "origin_write_evidence": [
                {
                    "call_id": "call_1m5aTJnfbS48zOmfPem7IjwJ",
                    "unified_diff_sha256": "3" * 64,
                }
            ],
            "formal_write_count": 0,
        }
        raw = (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        return {
            "schema_version": "english_legacy_recuration_work_item_v1",
            "remediation_batch_id": "EN-P0-006-BATCH-001",
            "inventory_sha256": "4" * 64,
            "batch_authorization_sha256": "5" * 64,
            "target_authorization_receipt_sha256": "6" * 64,
            "target_id": "sentence_pattern_card:SP-022",
            "target_kind": "sentence_pattern_card",
            "record_id": "SP-022",
            "ordinal": 90,
            "historical_operation": "representation_reorder_only",
            "current_object_sha256": current * 64,
            "desired_object_sha256": desired * 64,
            "target_evidence": {
                "artifact_id": "legacy-target-SP-022",
                "payload_sha256": hashlib.sha256(raw).hexdigest(),
                "payload": payload,
            },
            "authority": {
                "generation": "english-generation-1",
                "authority_fingerprint": "7" * 64,
            },
            "study_date": "2026-08-09",
            "frozen_at": "2026-08-09T14:00:00+08:00",
            "attempt": 1,
            "formal_write_count": 0,
        }

    def proposal(self, *, action: str = "update_existing_proposal") -> dict:
        return {
            "action": action,
            "update_kind": "typed_update" if action == "update_existing_proposal" else "none",
            "expected_current_object_sha256": "1" * 64,
            "desired_object_sha256": "2" * 64,
            "conflict_codes": (
                ["identity_ambiguous"] if action == "conflict" else []
            ),
            "evidence_refs": [ref("a"), ref("b")],
        }

    def analysis(self) -> dict:
        return {
            "schema_version": "luna_english_legacy_recuration_analysis_v1",
            "target_id": "sentence_pattern_card:SP-022",
            "target_kind": "sentence_pattern_card",
            "observed_target_ids": ["SP-022"],
            "proposal": self.proposal(),
            "rationale": "Existing target is bound; only a typed update may be proposed.",
            "evidence_refs": [ref("a"), ref("b")],
            "formal_write_count": 0,
        }

    def test_work_item_binds_canonical_line_evidence(self) -> None:
        item = self.work_item()
        self.assertEqual(validate_work_item(item), item)
        broken = copy.deepcopy(item)
        broken["target_evidence"]["payload"]["target_id"] = "SP-999"
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError, "english_legacy_work_item_invalid"
        ):
            validate_work_item(broken)

    def test_invalid_runtime_profile_does_not_consume_attempt_claim(self) -> None:
        item = self.work_item()
        work_sha = hashlib.sha256(
            (
                json.dumps(
                    item,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        ).hexdigest()
        with tempfile.TemporaryDirectory(prefix="legacy-preclaim-") as name:
            runtime_root = Path(name)
            with self.assertRaisesRegex(
                EnglishLegacyRecurationError,
                "english_legacy_profile_missing",
            ):
                run_work_item({}, runtime_root, item)
            claim_path, binding_path = _attempt_paths(runtime_root, work_sha)
            self.assertFalse(claim_path.exists())
            self.assertFalse(binding_path.exists())

    def test_analysis_requires_exact_source_and_target_refs(self) -> None:
        item = self.work_item()
        analysis = self.analysis()
        self.assertEqual(
            validate_analysis(
                analysis,
                item,
                allowed_refs=(ref("a"), ref("b"), ref("c")),
                required_refs=(ref("a"), ref("b")),
            ),
            analysis,
        )
        broken = copy.deepcopy(analysis)
        broken["evidence_refs"] = [ref("a"), ref("c")]
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError, "english_legacy_analysis_invalid"
        ):
            validate_analysis(
                broken,
                item,
                allowed_refs=(ref("a"), ref("b"), ref("c")),
                required_refs=(ref("a"), ref("b")),
            )

    def test_multiple_identity_matches_fail_closed(self) -> None:
        item = self.work_item()
        analysis = self.analysis()
        analysis["observed_target_ids"] = ["SP-022", "SP-022-ALIAS"]
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError, "english_legacy_identity_ambiguous"
        ):
            validate_analysis(
                analysis,
                item,
                allowed_refs=(ref("a"), ref("b")),
                required_refs=(ref("a"), ref("b")),
            )

    def test_critical_accepted_is_noop_over_draft(self) -> None:
        item = self.work_item()
        draft = self.analysis()
        critical = {
            "schema_version": "luna_english_legacy_recuration_critical_review_v1",
            "target_id": item["target_id"],
            "draft_sha256": sha256_value(draft),
            "verdict": "accepted",
            "findings": [],
            "revised_proposal": copy.deepcopy(draft["proposal"]),
            "evidence_refs": [ref("c"), ref("d")],
            "formal_write_count": 0,
        }
        critical["revised_proposal"]["evidence_refs"] = [ref("c"), ref("d")]
        self.assertEqual(
            validate_critical_review(
                critical,
                item,
                draft=draft,
                allowed_refs=(ref("c"), ref("d")),
                required_refs=(ref("c"), ref("d")),
            ),
            critical,
        )

    def test_stage_evidence_requires_exact_get_records_target(self) -> None:
        item = self.work_item()
        calls = (
            {
                "sequence": 1,
                "tool": "read_task_artifact",
                "arguments": {"artifact_id": "legacy-target-SP-022"},
                "result": {
                    "subject": "english",
                    "generation": "english-generation-1",
                    "items": [
                        {
                            "collection": "task_artifact",
                            "stable_id": "legacy-target-SP-022",
                            "source_hash": "9" * 64,
                            "data_role": "immutable_capture_fact",
                            "evidence_ref": ref("a"),
                        }
                    ]
                },
                "result_sha256": "e" * 64,
            },
            {
                "sequence": 2,
                "tool": "get_records",
                "arguments": {"collection": "patterns", "ids": ["SP-022"]},
                "result": {
                    "subject": "english",
                    "generation": "english-generation-1",
                    "items": [
                        {
                            "collection": "patterns",
                            "stable_id": "SP-022",
                            "source_hash": "a" * 64,
                            "data_role": "formal",
                            "evidence_ref": ref("b"),
                        }
                    ]
                },
                "result_sha256": "f" * 64,
            },
            {
                "sequence": 3,
                "tool": "search_records",
                "arguments": {"query": "SP-022"},
                "result": {
                    "subject": "english",
                    "generation": "english-generation-1",
                    "items": [],
                },
                "result_sha256": "0" * 64,
            },
        )
        result = StructuredStageResult(
            payload={},
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256="8" * 64,
            mcp_transcript_sha256="c" * 64,
            mcp_calls=calls,
        )
        allowed, required = _stage_evidence(result, work_item=item)
        self.assertEqual(set(required), {ref("a"), ref("b")})
        self.assertTrue(set(required).issubset(set(allowed)))
        broken = copy.deepcopy(calls)
        broken[1]["arguments"]["ids"] = ["SP-023"]
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError,
            "english_legacy_target_mcp_read_incomplete",
        ):
            _stage_evidence(
                StructuredStageResult(
                    payload={},
                    duration_ms=1,
                    runtime_model=None,
                    runtime_reasoning_effort=None,
                    runtime_metadata_provenance="unavailable",
                    runtime_identity_status="requested_unverified",
                    output_sha256="8" * 64,
                    mcp_transcript_sha256="c" * 64,
                    mcp_calls=tuple(broken),
                ),
                work_item=item,
            )


class EnglishLegacyAuthorizedWorkItemTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="english-recuration-")
        self.root = Path(self.temporary.name)
        self.key = self.root / "authority.key"
        self.key.write_bytes(b"w" * 32)
        os.chmod(self.key, 0o600)
        self.receipts = self.root / "authorization"
        self.output = self.root / "work"
        self.store = EnglishLegacyDispositionV3Store(self.receipts, self.key)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def expansion(self) -> str:
        review_core = build_inventory_independent_review_receipt_draft(
            review_id="EN-P0-006-INVENTORY-REVIEW-WORK-ITEM-TEST",
            reviewer_identity="independent-work-item-test-reviewer",
            reviewed_at="2026-08-09T09:30:00Z",
            mcp_authority_generation=TEST_MCP_AUTHORITY_GENERATION,
            mcp_authority_fingerprint=TEST_MCP_AUTHORITY_FINGERPRINT,
        )
        review_sha, _path, _review = (
            self.store.seal_inventory_independent_review_receipt(review_core)
        )
        inventory_core = build_complete_inventory_v3_draft(
            inventory_id="EN-P0-006-INVENTORY-WORK-ITEM-TEST",
            independent_review_receipt_sha256=review_sha,
            issued_at="2026-08-09T10:00:00Z",
            mcp_authority_generation=TEST_MCP_AUTHORITY_GENERATION,
            mcp_authority_fingerprint=TEST_MCP_AUTHORITY_FINGERPRINT,
        )
        inventory_sha, _path, _inventory = self.store.seal_inventory_v3(
            inventory_core
        )
        intent_sha, _path, _intent = self.store.seal_batch_intent(
            {
                "schema_version": BATCH_INTENT_SCHEMA,
                "issue_id": "EN-P0-006",
                "subject": "english",
                "intent_id": "EN-P0-006-WORK-ITEM-TEST-USER-TURN",
                "authorization_type": "explicit_user_batch_authorization",
                "inventory_scope": "first_independently_verified_complete_inventory",
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
        )
        batch_sha, _path, _authorization = (
            self.store.materialize_batch_authorization(
                intent_sha256=intent_sha,
                inventory_sha256=inventory_sha,
            )
        )
        closure_sha, _path, _closure = self.store.expand_batch_authorization(
            batch_sha
        )
        return closure_sha

    def test_verified_expansion_cannot_build_revoked_recuration_work_items(self) -> None:
        closure_sha = self.expansion()
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError,
            "legacy_recuration_authorization_revoked",
        ):
            build_authorized_work_items(
                disposition_root=self.receipts,
                authority_key_path=self.key,
                authorization_expansion_closure_sha256=closure_sha,
                output_root=self.output,
            )
        self.assertFalse(self.output.exists())

    def test_unverified_expansion_cannot_materialize_work_items(self) -> None:
        with self.assertRaises(EnglishLegacyRecurationError):
            build_authorized_work_items(
                disposition_root=self.receipts,
                authority_key_path=self.key,
                authorization_expansion_closure_sha256="f" * 64,
                output_root=self.output,
            )


if __name__ == "__main__":
    unittest.main()
