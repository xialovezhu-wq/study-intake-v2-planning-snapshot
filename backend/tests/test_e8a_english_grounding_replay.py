#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    CodexRunner,
    PreprocessorError,
    StructuredStageResult,
    mcp_grounding_manifest,
    model_mcp_item_ref,
    sha256_value,
    validate_english_mcp_grounding,
)


E8A_ARTIFACT_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-e8a54b12/stage-runtime/private/reports"
)
E8A_OUTPUT_PATH = (
    E8A_ARTIFACT_ROOT
    / "model-stage-outputs/objects/"
    "6abb724d38217927cd6cc2d74c3ea59274368c6fa7aca1d771cc5e799f779f69.json"
)
E8A_OUTPUT_SHA256 = (
    "6abb724d38217927cd6cc2d74c3ea59274368c6fa7aca1d771cc5e799f779f69"
)
E8A_TRANSCRIPT_PATH = (
    E8A_ARTIFACT_ROOT
    / "mcp-stage-transcripts/sha256/bf/"
    "bfe6b4830f88c586457751370c55b19944da740846c05cd081ae082e105d772f.json"
)
E8A_TRANSCRIPT_SHA256 = (
    "bfe6b4830f88c586457751370c55b19944da740846c05cd081ae082e105d772f"
)
E8A_RAW_TRANSPORT_PATH = (
    E8A_ARTIFACT_ROOT
    / "model-mcp-transport/sha256/2b/"
    "2bbddaddb6f9d2c73c59d9e790e742569b81219a6e51b4ddc9bdb77b971f5982.json"
)
E8A_RAW_TRANSPORT_SHA256 = (
    "2bbddaddb6f9d2c73c59d9e790e742569b81219a6e51b4ddc9bdb77b971f5982"
)
E8A_PROVIDER_SCHEMA_PATH = (
    E8A_ARTIFACT_ROOT
    / "provider-schemas/sha256/56/"
    "5613428054d6dd5a321a6c5aa34d4502b935aa641112e90cc7c2df91f509e567.json"
)
E8A_PROVIDER_SCHEMA_SHA256 = (
    "5613428054d6dd5a321a6c5aa34d4502b935aa641112e90cc7c2df91f509e567"
)


def _load_frozen_json(path: Path, expected_sha256: str) -> dict[str, Any]:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise AssertionError(f"frozen fixture SHA drift: {path}")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise AssertionError(f"frozen fixture root is not an object: {path}")
    return value


def e8a_analysis_stage_result() -> StructuredStageResult:
    """Reconstruct the sealed e8a Analysis stage without a model call."""

    output = _load_frozen_json(E8A_OUTPUT_PATH, E8A_OUTPUT_SHA256)
    transcript = _load_frozen_json(
        E8A_TRANSCRIPT_PATH, E8A_TRANSCRIPT_SHA256
    )
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
        provider_schema_sha256=E8A_PROVIDER_SCHEMA_SHA256,
        semantic_stage_count=int(transcript["semantic_stage_count"]),
        provider_request_count=int(transcript["provider_request_count"]),
        mcp_tool_call_count=int(transcript["mcp_tool_call_count"]),
        mcp_transcript_sha256=E8A_TRANSCRIPT_SHA256,
        mcp_calls=tuple(copy.deepcopy(transcript["calls"])),
    )


def e8a_analysis_grounding_fixture(
) -> tuple[StructuredStageResult, dict[str, Any], str, str]:
    """Expose the sealed stage, manifest, artifact ref, and one library ref."""

    stage = e8a_analysis_stage_result()
    manifest = mcp_grounding_manifest((stage,))
    artifact_refs = [
        row["evidence_ref"]
        for row in manifest["items"]
        if row["collection"] == "task_artifact"
    ]
    library_refs = [
        row["evidence_ref"]
        for row in manifest["items"]
        if row["collection"] not in {"task_context", "task_artifact"}
    ]
    if len(artifact_refs) != 1 or not library_refs:
        raise AssertionError("sealed e8a grounding roles drifted")
    return stage, manifest, artifact_refs[0], library_refs[0]


def _synthetic_manifest(
    rows: list[tuple[str, str]], *, transcript_sha256: str
) -> dict[str, Any]:
    manifest_rows = [
        {
            "evidence_ref": evidence_ref,
            "subject": "english",
            "generation": "english-test-generation-v1",
            "collection": collection,
            "stable_id": f"TEST-{index}",
            "source_hash": f"{index:064x}",
            "data_role": "test",
            "consumed_in": [
                {
                    "transcript_sha256": transcript_sha256,
                    "call_sequence": index,
                    "result_sha256": f"{index + 100:064x}",
                }
            ],
        }
        for index, (evidence_ref, collection) in enumerate(rows, start=1)
    ]
    core = {
        "schema_version": "model_mcp_grounding_manifest_v1",
        "items": manifest_rows,
        "item_count": len(manifest_rows),
        "host_semantic_prefetch": False,
        "formal_write_count": 0,
    }
    return {**core, "manifest_sha256": sha256_value(core)}


def _item(*refs: str) -> dict[str, Any]:
    return {"grounding": {"mcp_evidence_refs": list(refs)}}


class E8aEnglishGroundingReplayTests(unittest.TestCase):
    def test_e8a_english_analysis_grounding_replay_matches_exact_transcript(
        self,
    ) -> None:
        output = _load_frozen_json(E8A_OUTPUT_PATH, E8A_OUTPUT_SHA256)
        transcript = _load_frozen_json(
            E8A_TRANSCRIPT_PATH, E8A_TRANSCRIPT_SHA256
        )
        provider_schema = _load_frozen_json(
            E8A_PROVIDER_SCHEMA_PATH, E8A_PROVIDER_SCHEMA_SHA256
        )
        stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )

        self.assertEqual(output["payload"], {"items": []})
        self.assertNotIn("minItems", provider_schema["properties"]["items"])
        self.assertEqual(stage.mcp_tool_call_count, 64)
        self.assertEqual(stage.provider_request_count, 65)
        self.assertEqual(transcript["formal_write_count"], 0)
        self.assertEqual(transcript["model_call_count"], 1)
        self.assertEqual(transcript["coverage"]["duplicate_argument_count"], 0)
        self.assertTrue(
            all(call["result"]["ok"] is True for call in transcript["calls"])
        )
        self.assertEqual(manifest["item_count"], 71)
        self.assertEqual(
            Counter(row["collection"] for row in manifest["items"]),
            Counter(
                {
                    "task_context": 1,
                    "task_artifact": 1,
                    "article_catalog": 60,
                    "search": 9,
                }
            ),
        )
        self.assertEqual(
            artifact_ref,
            "mcp-item:english:"
            "a87e1f4df023235a27c57b9404d9a0328b0b71fbbacd17ab17473fbb125d434a",
        )
        self.assertTrue(library_ref.startswith("mcp-item:english:"))
        with self.assertRaisesRegex(
            PreprocessorError, "^english_mcp_grounding_missing$"
        ):
            validate_english_mcp_grounding(
                output["payload"]["items"], grounding_manifest=manifest
            )

    def test_exact_artifact_and_library_members_pass(self) -> None:
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )
        validate_english_mcp_grounding(
            [_item(artifact_ref, library_ref)], grounding_manifest=manifest
        )

    def test_missing_artifact_library_and_context_only_fail_missing(self) -> None:
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )
        context_ref = next(
            row["evidence_ref"]
            for row in manifest["items"]
            if row["collection"] == "task_context"
        )
        for refs in (
            (library_ref,),
            (artifact_ref,),
            (context_ref,),
            (context_ref, artifact_ref),
        ):
            with self.subTest(refs=refs), self.assertRaisesRegex(
                PreprocessorError, "^english_mcp_grounding_missing$"
            ):
                validate_english_mcp_grounding(
                    [_item(*refs)], grounding_manifest=manifest
                )

    def test_nonmember_failed_cross_subject_and_duplicate_refs_are_invalid(
        self,
    ) -> None:
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )
        invalid_cases = (
            (artifact_ref, library_ref, "mcp-item:english:" + "0" * 64),
            (artifact_ref, library_ref, "mcp-item:english:" + "f" * 64),
            (artifact_ref, library_ref, "mcp-item:cs408:" + "1" * 64),
            (artifact_ref, artifact_ref, library_ref),
        )
        manifest_refs = {
            row["evidence_ref"] for row in manifest["items"]
        }
        self.assertNotIn(invalid_cases[0][-1], manifest_refs)
        self.assertNotIn(invalid_cases[1][-1], manifest_refs)
        for refs in invalid_cases:
            with self.subTest(refs=refs), self.assertRaisesRegex(
                PreprocessorError, "^english_mcp_grounding_invalid$"
            ):
                validate_english_mcp_grounding(
                    [_item(*refs)], grounding_manifest=manifest
                )

    def test_e8a_failed_seq65_is_excluded_from_canonical_grounding(self) -> None:
        raw_transport = _load_frozen_json(
            E8A_RAW_TRANSPORT_PATH, E8A_RAW_TRANSPORT_SHA256
        )
        transcript = _load_frozen_json(
            E8A_TRANSCRIPT_PATH, E8A_TRANSCRIPT_SHA256
        )
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )

        self.assertEqual(raw_transport["mcp_item_count"], 65)
        self.assertEqual(len(raw_transport["mcp_items"]), 65)
        failed = raw_transport["mcp_items"][-1]
        self.assertEqual(failed["sequence"], 65)
        self.assertEqual(failed["item"]["tool"], "search_records")
        self.assertEqual(
            failed["item"]["arguments"],
            {"page_size": 48, "query": "permanent"},
        )
        failed_result = failed["item"]["result"]["structured_content"]
        self.assertIs(failed_result["ok"], False)
        self.assertEqual(failed_result["error"]["code"], "OUTPUT_LIMIT")
        self.assertEqual(failed_result["items"], [])

        failed_request_id = failed_result["request_id"]
        self.assertEqual(len(transcript["calls"]), 64)
        self.assertEqual(
            [call["sequence"] for call in transcript["calls"]],
            list(range(1, 65)),
        )
        self.assertTrue(
            all(call["result"]["ok"] is True for call in transcript["calls"])
        )
        self.assertNotIn(
            failed_request_id,
            json.dumps(transcript, ensure_ascii=False, sort_keys=True),
        )

        would_be_failed_ref = model_mcp_item_ref(
            subject="english",
            generation=str(raw_transport["generation"]),
            collection="search",
            stable_id=f"failed-request:{failed_request_id}",
            source_hash=sha256_value(failed_result),
        )
        self.assertRegex(would_be_failed_ref, r"^mcp-item:english:[0-9a-f]{64}$")
        self.assertNotIn(
            would_be_failed_ref,
            {row["evidence_ref"] for row in manifest["items"]},
        )
        with self.assertRaisesRegex(
            PreprocessorError, "^english_mcp_grounding_invalid$"
        ):
            validate_english_mcp_grounding(
                [_item(artifact_ref, library_ref, would_be_failed_ref)],
                grounding_manifest=manifest,
            )

    def test_manifest_sha_drift_is_invalid(self) -> None:
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )
        drifted = copy.deepcopy(manifest)
        drifted["manifest_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            PreprocessorError, "^english_mcp_grounding_invalid$"
        ):
            validate_english_mcp_grounding(
                [_item(artifact_ref, library_ref)],
                grounding_manifest=drifted,
            )

    def test_mixed_stage_transcript_manifest_is_invalid(self) -> None:
        _stage, manifest, artifact_ref, library_ref = (
            e8a_analysis_grounding_fixture()
        )
        mixed = copy.deepcopy(manifest)
        mixed["items"][0]["consumed_in"][0]["transcript_sha256"] = "1" * 64
        core = {
            key: value
            for key, value in mixed.items()
            if key != "manifest_sha256"
        }
        mixed["manifest_sha256"] = sha256_value(core)
        with self.assertRaisesRegex(
            PreprocessorError, "^english_mcp_grounding_invalid$"
        ):
            validate_english_mcp_grounding(
                [_item(artifact_ref, library_ref)],
                grounding_manifest=mixed,
            )

    def test_analysis_refs_cannot_substitute_for_fresh_review_refs(self) -> None:
        _stage, analysis_manifest, analysis_artifact, analysis_library = (
            e8a_analysis_grounding_fixture()
        )
        review_artifact = "mcp-item:english:" + "7" * 64
        review_library = "mcp-item:english:" + "8" * 64
        review_manifest = _synthetic_manifest(
            [
                (review_artifact, "task_artifact"),
                (review_library, "search"),
            ],
            transcript_sha256="2" * 64,
        )
        validate_english_mcp_grounding(
            [_item(analysis_artifact, analysis_library)],
            grounding_manifest=analysis_manifest,
        )
        validate_english_mcp_grounding(
            [_item(review_artifact, review_library)],
            grounding_manifest=review_manifest,
        )
        with self.assertRaisesRegex(
            PreprocessorError, "^english_mcp_grounding_invalid$"
        ):
            validate_english_mcp_grounding(
                [_item(analysis_artifact, analysis_library)],
                grounding_manifest=review_manifest,
            )

    def test_private_provider_schema_overlay_requires_one_item(self) -> None:
        event_ids = ("EVT-20260810-ENGLISH-GROUNDING-001",)
        cases = (
            (
                "english_analysis",
                ROOT / "schemas/luna-english-candidate-draft-v1.json",
                "items",
            ),
            (
                "english_critical_review",
                ROOT / "schemas/luna-english-critical-review-v1.json",
                "revised_items",
            ),
        )
        for stage_name, path, items_key in cases:
            static_bytes = path.read_bytes()
            static_schema = json.loads(static_bytes)
            self.assertNotIn(
                "minItems", static_schema["properties"][items_key]
            )
            payload, payload_sha256 = (
                CodexRunner._bound_english_source_event_schema_bytes(
                    path,
                    stage_name=stage_name,
                    allowed_source_event_ids=event_ids,
                )
            )
            self.assertEqual(
                hashlib.sha256(payload).hexdigest(), payload_sha256
            )
            self.assertEqual(
                json.loads(payload)["properties"][items_key]["minItems"], 1
            )
            self.assertEqual(path.read_bytes(), static_bytes)


if __name__ == "__main__":
    unittest.main()
