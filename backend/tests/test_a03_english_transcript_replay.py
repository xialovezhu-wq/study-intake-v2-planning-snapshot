from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from processing_plugin import (  # noqa: E402
    ProcessingPluginHost,
    _sha256_json_value,
)


A03_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "subject-verification/e8a54b12-english/real-smoke/"
    "ENGLISH-LUNA-REAL-DIAGNOSTIC-A03-20260810"
)
CANDIDATE_RELEASE = (
    "e8a54b12af32e1226676beb4b87cb24961e0949adad14b4d88f6ceb40559ae9f"
)
CHECKPOINT_SHA256 = (
    "10dd1c71c9eaaf59b2fb64ce00137e0ea8349091cb082c742ec2c2d84a193c42"
)
ANALYSIS_TRANSCRIPT_SHA256 = (
    "4047a73549dffacd495853aeac8bbcf409e0bb24258977adeb53be414cab1ecb"
)
ANALYSIS_CALL_RECEIPT_SHA256 = (
    "f8cb8a6bd7e2c292a3e21502102601aaad676455b3f2ad0e74f448fcef9e5a29"
)
CRITICAL_TRANSCRIPT_SHA256 = (
    "05ceba0fb7f5633de7b3967f5124975fd1ea7b4b7d04573d0fb68e7b8cedcdbb"
)
CRITICAL_CALL_RECEIPT_SHA256 = (
    "248747f716556e06e53fda7636b15b0703a15ad1cb9420251abd87c8468c5abe"
)


def load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"object required: {path}")
    return value


def physical_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class A03EnglishTranscriptReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        required = (
            A03_ROOT / "config/private-config.json",
            A03_ROOT
            / "runtime/private/reports/analysis-checkpoints/objects"
            / f"{CHECKPOINT_SHA256}.json",
        )
        for path in required:
            if not path.is_file() or path.is_symlink():
                raise AssertionError(f"sealed A03 fixture missing: {path}")

    def setUp(self) -> None:
        config = load_json(A03_ROOT / "config/private-config.json")
        checkpoint_path = (
            A03_ROOT
            / "runtime/private/reports/analysis-checkpoints/objects"
            / f"{CHECKPOINT_SHA256}.json"
        )
        self.assertEqual(physical_sha256(checkpoint_path), CHECKPOINT_SHA256)
        self.checkpoint = load_json(checkpoint_path)
        self.context = self.checkpoint["processing_context"]
        # A03 is an immutable historical v2 fixture whose MCP release predates
        # the production sealed-launcher contract.  Keep the production Host
        # constructor strict and bypass only its current-release admission
        # checks; transcript bytes and model-call receipts are still reopened,
        # rehashed, and HMAC-verified below against the sealed checkpoint.
        self.host = object.__new__(ProcessingPluginHost)
        self.host.config = copy.deepcopy(config["processing_plugin"])
        self.host.runtime_root = (A03_ROOT / "runtime").resolve()
        self.host.candidate_release_id = CANDIDATE_RELEASE
        self.host.require_authority_snapshot = False
        self.host.authority_key_path = Path(
            config["processing_plugin"]["authority_key_path"]
        ).resolve()
        self.host.validate_read_session_context = (  # type: ignore[method-assign]
            lambda *, subject, context: copy.deepcopy(dict(context))
        )

    def transcript_path(self, digest: str) -> Path:
        return (
            A03_ROOT
            / "runtime/private/reports/mcp-stage-transcripts/sha256"
            / digest[:2]
            / f"{digest}.json"
        )

    @staticmethod
    def publication_row(stage_receipt: dict) -> dict:
        grounding = stage_receipt["mcp_grounding_manifest"]
        return {
            "transcript_sha256": stage_receipt["mcp_transcript_sha256"],
            "transcript_ref": stage_receipt["mcp_transcript_ref"],
            "call_receipt_sha256": stage_receipt["mcp_call_receipt_sha256"],
            "call_receipt_ref": stage_receipt["mcp_call_receipt_ref"],
            "grounding_manifest_sha256": stage_receipt[
                "mcp_grounding_manifest_sha256"
            ],
            "grounding_refs": [
                item["evidence_ref"] for item in grounding["items"]
            ],
            "provider_request_count": stage_receipt[
                "provider_request_count"
            ],
            "mcp_tool_call_count": stage_receipt["mcp_tool_call_count"],
        }

    def test_exact_a03_analysis_transcript_reopens_with_semantic_stage_name(self) -> None:
        stage_receipt = self.checkpoint["analysis_receipt"]
        transcript_path = self.transcript_path(ANALYSIS_TRANSCRIPT_SHA256)
        self.assertEqual(physical_sha256(transcript_path), ANALYSIS_TRANSCRIPT_SHA256)
        self.assertEqual(
            stage_receipt["mcp_call_receipt_sha256"],
            ANALYSIS_CALL_RECEIPT_SHA256,
        )
        transcript, call_receipt = self.host._reopen_stage_transcript(
            subject="english",
            context=self.context,
            stage="analysis",
            stage_receipt=stage_receipt,
            publication_row=self.publication_row(stage_receipt),
        )
        self.assertEqual(transcript["stage_name"], "english_analysis")
        self.assertEqual(
            call_receipt["stage_name"],
            "study-intake-english-luna-analysis-v1",
        )
        self.assertEqual(len(transcript["calls"]), 26)

    def test_exact_a03_critical_transcript_reopens_with_semantic_stage_name(self) -> None:
        transcript_path = self.transcript_path(CRITICAL_TRANSCRIPT_SHA256)
        transcript = load_json(transcript_path)
        self.assertEqual(physical_sha256(transcript_path), CRITICAL_TRANSCRIPT_SHA256)
        call = next(row for row in transcript["calls"] if row["result"]["items"])
        item = call["result"]["items"][0]
        grounding_item = {
            "evidence_ref": item["evidence_ref"],
            "subject": "english",
            "generation": self.context["mcp_read_session"]["generation"],
            "collection": item["collection"],
            "stable_id": item["stable_id"],
            "source_hash": item["source_hash"],
            "data_role": item["data_role"],
            "consumed_in": [
                {
                    "transcript_sha256": CRITICAL_TRANSCRIPT_SHA256,
                    "call_sequence": call["sequence"],
                    "result_sha256": call["result_sha256"],
                }
            ],
        }
        grounding_core = {
            "schema_version": "model_mcp_grounding_manifest_v1",
            "items": [grounding_item],
            "item_count": 1,
            "host_semantic_prefetch": False,
            "formal_write_count": 0,
        }
        grounding = {
            **grounding_core,
            "manifest_sha256": _sha256_json_value(grounding_core),
        }
        stage_receipt = copy.deepcopy(self.checkpoint["analysis_receipt"])
        stage_receipt.update(
            {
                "prompt_version": "study-intake-english-luna-critical-review-v2",
                "mcp_transcript_sha256": CRITICAL_TRANSCRIPT_SHA256,
                "mcp_transcript_ref": (
                    "study-intake-mcp-stage-transcript://sha256/"
                    + CRITICAL_TRANSCRIPT_SHA256
                ),
                "mcp_call_receipt_sha256": CRITICAL_CALL_RECEIPT_SHA256,
                "mcp_call_receipt_ref": (
                    "study-intake-mcp-read-session-call://sha256/"
                    + CRITICAL_CALL_RECEIPT_SHA256
                ),
                "mcp_grounding_manifest": grounding,
                "mcp_grounding_manifest_sha256": grounding[
                    "manifest_sha256"
                ],
                "provider_request_count": 11,
                "mcp_tool_call_count": 10,
            }
        )
        reopened, call_receipt = self.host._reopen_stage_transcript(
            subject="english",
            context=self.context,
            stage="critical_review",
            stage_receipt=stage_receipt,
            publication_row=self.publication_row(stage_receipt),
        )
        self.assertEqual(
            reopened["stage_name"],
            "english_critical_review",
        )
        self.assertEqual(
            call_receipt["stage_name"],
            "study-intake-english-luna-critical-review-v2",
        )
        self.assertEqual(len(reopened["calls"]), 10)


if __name__ == "__main__":
    unittest.main()
