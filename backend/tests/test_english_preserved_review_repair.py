from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "scripts"))

from concurrent_dispatch import (  # noqa: E402
    ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION,
    _canonical_bytes,
    _sha256_bytes,
)
from preprocess_dispatcher import build_parser  # noqa: E402
import release_manager  # noqa: E402


def sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class EnglishPreservedReviewRepairTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.fixture_root = Path(self.temporary.name).resolve()
        self.target_release = sha("english-review-target-release")
        self.target_activation = sha("english-review-target-activation")
        self.target_generation = "english-review-target-generation"
        self.target_subject_authority = sha("english-review-subject-authority")
        self.target_producer_authority = sha("english-review-producer-authority")
        self.staged_sha = sha("english-review-staged-gate")

    def _production_preview_fixture(self) -> dict:
        descriptor = copy.deepcopy(
            ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        )
        path_keys = {
            "queue",
            "gate",
            "terminal_index",
            "completion",
            "lease",
            "task_detail",
            "task",
            "raw_output",
            "mcp_transport",
            "stage_execution_receipt",
            "processing_receipt",
            "mcp_failure_receipt",
            "canary_terminal_receipt",
            "subject_terminal_receipt",
            "original_preclaim_receipt",
            "rollover_receipt",
            "recovery_supersede_receipt",
            "subject_batch",
            "subject_batch_pointer",
            "subject_writer",
        }
        return {
            "schema_version": (
                "study-intake-english-preserved-review-repair-preview-v1"
            ),
            "status": "ready",
            "authorization_descriptor": descriptor,
            "authorization_descriptor_sha256": _sha256_bytes(
                _canonical_bytes(descriptor)
            ),
            "paths": {
                key: str(self.fixture_root / key) for key in path_keys
            },
            "observed_historical_model_call_count": 1,
            "observed_historical_provider_request_count": 41,
            "observed_historical_mcp_tool_call_count": 40,
            "source_gate_preimage_sha256": descriptor[
                "gate_preimage_sha256"
            ],
            "staged_target_canary_state_sha256": None,
            "target_release_id": None,
            "target_activation_id": None,
            "target_generation": None,
            "target_subject_authority_fingerprint": None,
            "target_producer_authority_fingerprint": None,
            "new_task_count": 0,
            "new_queue_count": 0,
            "capture_replay_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
        }

    def test_cli_shape_and_schema_contract(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--config",
                str(ROOT / "README.md"),
                "--subject",
                "english",
                "english-preserved-review-repair-apply",
                "--target-release-id",
                self.target_release,
                "--target-activation-id",
                self.target_activation,
                "--target-generation",
                self.target_generation,
                "--target-subject-authority-fingerprint",
                self.target_subject_authority,
                "--target-producer-authority-fingerprint",
                self.target_producer_authority,
                "--staged-target-canary-state-sha256",
                self.staged_sha,
            ]
        )
        self.assertEqual(
            args.target_subject_authority_fingerprint,
            self.target_subject_authority,
        )
        schema = json.loads(
            (
                ROOT
                / "schemas/english-preserved-review-repair-receipt-v1.json"
            ).read_text()
        )
        self.assertIn(
            "target_subject_authority_fingerprint", schema["required"]
        )
        self.assertIn(
            "target_producer_authority_fingerprint", schema["required"]
        )
        self.assertNotIn(
            "target_authority_fingerprint", schema["properties"]
        )

    def test_production_descriptor_preview_shape_is_accepted(self) -> None:
        descriptor_sha256 = _sha256_bytes(
            _canonical_bytes(ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION)
        )
        self.assertEqual(
            release_manager.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256,
            descriptor_sha256,
        )
        validated = (
            release_manager._validate_english_preserved_review_repair_preview(
                self._production_preview_fixture()
            )
        )
        self.assertEqual(
            validated["authorization_descriptor_sha256"],
            descriptor_sha256,
        )
        self.assertEqual(validated["status"], "ready")

    def test_production_descriptor_preview_rejects_old_sha(self) -> None:
        old_descriptor = copy.deepcopy(
            ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        )
        old_descriptor.pop("duplicate_result_projection_sha256")
        old_descriptor_sha256 = _sha256_bytes(
            _canonical_bytes(old_descriptor)
        )
        current_descriptor_sha256 = _sha256_bytes(
            _canonical_bytes(ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION)
        )
        self.assertEqual(
            old_descriptor_sha256,
            "4d684190c661ca66e23d2eb7d821a19d0d503ca9e26561f594af03c1573d5de3",
        )
        self.assertEqual(
            current_descriptor_sha256,
            "74a640975df21f2b267170613931275e668ec4f800bc02e557e9306630da507e",
        )

        tampered_preview = self._production_preview_fixture()
        tampered_preview["authorization_descriptor_sha256"] = (
            old_descriptor_sha256
        )
        with self.assertRaisesRegex(
            release_manager.ReleaseError,
            "english_preserved_review_repair_preview_invalid",
        ):
            release_manager._validate_english_preserved_review_repair_preview(
                tampered_preview
            )


if __name__ == "__main__":
    unittest.main()
