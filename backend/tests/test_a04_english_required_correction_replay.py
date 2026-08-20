#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    StructuredStageResult,
    english_review_semantic_draft,
    materialize_english_correction_deltas,
    mcp_grounding_manifest,
    sha256_value,
    validate_english_applied_corrections,
    validate_english_critical_review,
    validate_english_mcp_grounding,
)


A04_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "subject-verification/e8a54b12-english/real-smoke/"
    "ENGLISH-LUNA-REAL-DIAGNOSTIC-A04-20260810"
)
REPORTS = A04_ROOT / "runtime/private/reports"

ANALYSIS_OUTPUT_SHA256 = (
    "862f20e14185003c0e767edb5c0a9f985f3b798cff72fc8872408dea7c9d1847"
)
CRITICAL_OUTPUT_SHA256 = (
    "dff37225d323d864bd57bd1212c69d1cc400d6d75d43dfe1554f542a0efc99ad"
)
ANALYSIS_RAW_SHA256 = (
    "5df9a9b23a6657168e74fedeb105a71c986d572ad83f1f30460940c0dcaaa24e"
)
CRITICAL_RAW_SHA256 = (
    "ea60a76d2c245193af8a78394575540b1d89b3a61cb692743893ac031a1ff310"
)
ANALYSIS_TRANSCRIPT_SHA256 = (
    "45e5e2cf375525ccfe26417fc33c63c2dce5c1fab534cc0eaca68caaeea02bbb"
)
CRITICAL_TRANSCRIPT_SHA256 = (
    "d1e098895afd3ae53fa08739ecec176a0f37059e6a6a6b9042cc3b06165eacbb"
)
CHECKPOINT_SHA256 = (
    "74b075495d4a3f7f68e7e7799beb5578a947483e7b4eeac38bc090326bfc245e"
)
TERMINAL_FAILURE_SHA256 = (
    "ea84c9ef1b395ad5c389f00a8f9cbd267c30d5caed77c2dde471a5438ed5ecd0"
)

ANALYSIS_OUTPUT_PATH = (
    REPORTS / "model-stage-outputs/objects" / f"{ANALYSIS_OUTPUT_SHA256}.json"
)
CRITICAL_OUTPUT_PATH = (
    REPORTS / "model-stage-outputs/objects" / f"{CRITICAL_OUTPUT_SHA256}.json"
)
ANALYSIS_RAW_PATH = (
    REPORTS
    / "model-mcp-transport/sha256"
    / ANALYSIS_RAW_SHA256[:2]
    / f"{ANALYSIS_RAW_SHA256}.json"
)
CRITICAL_RAW_PATH = (
    REPORTS
    / "model-mcp-transport/sha256"
    / CRITICAL_RAW_SHA256[:2]
    / f"{CRITICAL_RAW_SHA256}.json"
)
ANALYSIS_TRANSCRIPT_PATH = (
    REPORTS
    / "mcp-stage-transcripts/sha256"
    / ANALYSIS_TRANSCRIPT_SHA256[:2]
    / f"{ANALYSIS_TRANSCRIPT_SHA256}.json"
)
CRITICAL_TRANSCRIPT_PATH = (
    REPORTS
    / "mcp-stage-transcripts/sha256"
    / CRITICAL_TRANSCRIPT_SHA256[:2]
    / f"{CRITICAL_TRANSCRIPT_SHA256}.json"
)
CHECKPOINT_PATH = (
    REPORTS / "analysis-checkpoints/objects" / f"{CHECKPOINT_SHA256}.json"
)
TERMINAL_FAILURE_PATH = A04_ROOT / "receipts/terminal-failure.json"


def _load_sealed_json(path: Path, expected_sha256: str) -> dict[str, Any]:
    payload = path.read_bytes()
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise AssertionError(
            f"sealed A04 fixture SHA drift: {path}: {actual_sha256}"
        )
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise AssertionError(f"sealed A04 fixture must be an object: {path}")
    return value


def _stage_result(
    output: dict[str, Any],
    transcript: dict[str, Any],
    *,
    transcript_sha256: str,
) -> StructuredStageResult:
    return StructuredStageResult(
        payload=copy.deepcopy(output["payload"]),
        duration_ms=int(output["duration_ms"]),
        runtime_model=output.get("runtime_model"),
        runtime_reasoning_effort=output.get("runtime_reasoning_effort"),
        runtime_metadata_provenance=str(
            output["runtime_metadata_provenance"]
        ),
        runtime_identity_status=str(output["runtime_identity_status"]),
        output_sha256=str(output["output_sha256"]),
        schema_sha256=str(output["schema_sha256"]),
        semantic_stage_count=int(transcript["semantic_stage_count"]),
        provider_request_count=int(transcript["provider_request_count"]),
        mcp_tool_call_count=int(transcript["mcp_tool_call_count"]),
        mcp_transcript_sha256=transcript_sha256,
        mcp_transcript_ref=(
            "study-intake-mcp-stage-transcript://sha256/"
            + transcript_sha256
        ),
        mcp_calls=tuple(copy.deepcopy(transcript["calls"])),
    )


def _first_unresolved_path(payload: dict[str, Any]) -> tuple[str, str]:
    for index, resolution in enumerate(payload["correction_resolutions"]):
        if resolution.get("resolution") == "unresolved":
            return (
                f"$.payload.correction_resolutions[{index}].resolution",
                str(resolution.get("finding_id") or ""),
            )
    raise AssertionError("A04 critical review no longer has an unresolved row")


class A04EnglishRequiredCorrectionReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sealed_paths = (
            ANALYSIS_OUTPUT_PATH,
            CRITICAL_OUTPUT_PATH,
            ANALYSIS_RAW_PATH,
            CRITICAL_RAW_PATH,
            ANALYSIS_TRANSCRIPT_PATH,
            CRITICAL_TRANSCRIPT_PATH,
            CHECKPOINT_PATH,
            TERMINAL_FAILURE_PATH,
        )
        for path in sealed_paths:
            if not path.is_file() or path.is_symlink():
                raise AssertionError(f"sealed A04 fixture missing: {path}")

    def setUp(self) -> None:
        self.analysis_output = _load_sealed_json(
            ANALYSIS_OUTPUT_PATH, ANALYSIS_OUTPUT_SHA256
        )
        self.critical_output = _load_sealed_json(
            CRITICAL_OUTPUT_PATH, CRITICAL_OUTPUT_SHA256
        )
        self.analysis_raw = _load_sealed_json(
            ANALYSIS_RAW_PATH, ANALYSIS_RAW_SHA256
        )
        self.critical_raw = _load_sealed_json(
            CRITICAL_RAW_PATH, CRITICAL_RAW_SHA256
        )
        self.analysis_transcript = _load_sealed_json(
            ANALYSIS_TRANSCRIPT_PATH, ANALYSIS_TRANSCRIPT_SHA256
        )
        self.critical_transcript = _load_sealed_json(
            CRITICAL_TRANSCRIPT_PATH, CRITICAL_TRANSCRIPT_SHA256
        )
        self.checkpoint = _load_sealed_json(
            CHECKPOINT_PATH, CHECKPOINT_SHA256
        )

    def test_exact_a04_raw_failure_is_non_evidence_and_recovered_transport_closes(
        self,
    ) -> None:
        self.assertEqual(self.analysis_raw["mcp_item_count"], 6)
        self.assertEqual(len(self.analysis_transcript["calls"]), 5)
        self.assertEqual(self.critical_raw["mcp_item_count"], 34)
        self.assertEqual(len(self.critical_transcript["calls"]), 33)

        failed_rows = [
            row
            for row in self.critical_raw["mcp_items"]
            if row["item"]["result"]["structured_content"]["ok"] is False
        ]
        self.assertEqual(len(failed_rows), 1)
        failed = failed_rows[0]
        self.assertEqual(failed["sequence"], 4)
        self.assertEqual(failed["item"]["tool"], "search_records")
        self.assertEqual(
            failed["item"]["arguments"],
            {"page_size": 48, "query": "permanent"},
        )
        failed_result = failed["item"]["result"]["structured_content"]
        self.assertEqual(failed_result["error"]["code"], "OUTPUT_LIMIT")
        self.assertEqual(failed_result["items"], [])

        canonical_json = json.dumps(
            self.critical_transcript, ensure_ascii=False, sort_keys=True
        )
        self.assertNotIn(failed_result["request_id"], canonical_json)
        self.assertTrue(
            all(
                call["result"]["ok"] is True
                for call in self.critical_transcript["calls"]
            )
        )
        first_recovered = self.critical_transcript["calls"][3]
        self.assertEqual(first_recovered["sequence"], 4)
        self.assertEqual(first_recovered["tool"], "search_records")
        self.assertEqual(
            first_recovered["arguments"],
            {"page_size": 1, "query": "permanent"},
        )
        permanent_calls = [
            call
            for call in self.critical_transcript["calls"]
            if call["tool"] == "search_records"
            and call["arguments"].get("query") == "permanent"
        ]
        self.assertEqual(len(permanent_calls), 16)
        self.assertIs(permanent_calls[-1]["result"]["complete"], True)
        self.assertIsNone(permanent_calls[-1]["result"]["next_cursor"])
        self.assertEqual(
            self.critical_transcript["coverage"],
            {
                "all_returned_pages_consumed": True,
                "call_count": 33,
                "duplicate_argument_count": 0,
                "host_semantic_prefetch": False,
                "unresolved_next_cursors": [],
            },
        )

    def test_exact_a04_critical_review_replays_first_unresolved_signature_and_path(
        self,
    ) -> None:
        terminal = _load_sealed_json(
            TERMINAL_FAILURE_PATH, TERMINAL_FAILURE_SHA256
        )
        self.assertEqual(
            terminal["failure_signature"],
            "english_required_correction_unresolved",
        )
        self.assertEqual(terminal["failure_stage"], "real_two_stage_dispatch")
        self.assertIsNone(terminal["first_failure_path"])
        self.assertEqual(terminal["formal_write_count"], 0)
        self.assertEqual(terminal["sol_status"], "disabled")

        review = copy.deepcopy(self.critical_output["payload"])
        semantic_draft = english_review_semantic_draft(
            self.checkpoint["draft_analysis"]
        )
        self.assertEqual(
            review["draft_analysis_sha256"], sha256_value(semantic_draft)
        )
        self.assertEqual(
            _first_unresolved_path(review),
            (
                "$.payload.correction_resolutions[6].resolution",
                "CR-000",
            ),
        )
        self.assertEqual(review["findings"][0]["correction_id"], "CR-000")
        self.assertEqual(review["findings"][0]["severity"], "blocking")
        self.assertEqual(review["verdict"], "reject")

        effective = materialize_english_correction_deltas(
            review, semantic_draft
        )
        self.assertEqual(len(effective["correction_resolutions"]), 7)
        self.assertEqual(
            effective["correction_resolutions"][6],
            review["correction_resolutions"][6],
        )
        with self.assertRaises(PreprocessorError) as raised:
            validate_english_critical_review(effective, semantic_draft)
        self.assertEqual(
            raised.exception.code,
            "english_required_correction_unresolved",
        )
        self.assertIsNotNone(raised.exception.__cause__)
        self.assertEqual(
            str(raised.exception.__cause__), "required_correction_unresolved"
        )

    def test_corrected_recovered_fixture_closes_corrections_and_stage_grounding(
        self,
    ) -> None:
        semantic_draft = english_review_semantic_draft(
            self.checkpoint["draft_analysis"]
        )
        corrected = copy.deepcopy(self.critical_output["payload"])
        corrected["verdict"] = "revised"
        corrected["findings"] = [
            row
            for row in corrected["findings"]
            if row["correction_id"] != "CR-000"
        ]
        corrected["correction_resolutions"] = [
            row
            for row in corrected["correction_resolutions"]
            if row["finding_id"] != "CR-000"
        ]

        effective = materialize_english_correction_deltas(
            corrected, semantic_draft
        )
        validate_english_critical_review(effective, semantic_draft)
        validate_english_applied_corrections(effective)
        self.assertEqual(len(effective["findings"]), 6)
        self.assertEqual(len(effective["correction_resolutions"]), 6)
        self.assertTrue(
            all(
                row["resolution"] == "applied"
                for row in effective["correction_resolutions"]
            )
        )

        analysis_stage = _stage_result(
            self.analysis_output,
            self.analysis_transcript,
            transcript_sha256=ANALYSIS_TRANSCRIPT_SHA256,
        )
        critical_stage = _stage_result(
            self.critical_output,
            self.critical_transcript,
            transcript_sha256=CRITICAL_TRANSCRIPT_SHA256,
        )
        analysis_manifest = mcp_grounding_manifest((analysis_stage,))
        critical_manifest = mcp_grounding_manifest((critical_stage,))
        validate_english_mcp_grounding(
            self.checkpoint["draft_analysis"]["items"],
            grounding_manifest=analysis_manifest,
        )
        validate_english_mcp_grounding(
            effective["revised_items"],
            grounding_manifest=critical_manifest,
        )

        self.assertNotEqual(
            analysis_stage.mcp_transcript_sha256,
            critical_stage.mcp_transcript_sha256,
        )
        self.assertEqual(
            analysis_stage.semantic_stage_count
            + critical_stage.semantic_stage_count,
            2,
        )
        self.assertEqual(
            analysis_stage.provider_request_count
            + critical_stage.provider_request_count,
            40,
        )
        self.assertEqual(
            analysis_stage.mcp_tool_call_count
            + critical_stage.mcp_tool_call_count,
            38,
        )
        for transcript in (
            self.analysis_transcript,
            self.critical_transcript,
        ):
            self.assertIs(
                transcript["coverage"]["all_returned_pages_consumed"],
                True,
            )
            self.assertEqual(
                transcript["coverage"]["unresolved_next_cursors"], []
            )
            self.assertEqual(transcript["formal_write_count"], 0)
        executed_model_call_count = 0
        executed_formal_write_count = 0
        self.assertEqual(executed_model_call_count, 0)
        self.assertEqual(executed_formal_write_count, 0)


if __name__ == "__main__":
    unittest.main()
