#!/usr/bin/env python3

from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import preprocessor_core as core  # noqa: E402


FIXTURE_ROOT = ROOT / "tests/fixtures/english_duplicate_review_boundary"
TRANSPORT_FIXTURE = FIXTURE_ROOT / "transport.json.gz"
SESSION_FIXTURE = FIXTURE_ROOT / "read-session.json.gz"
RAW_FIXTURE = FIXTURE_ROOT / "raw-output-object.json.gz"
FIXTURE_BINDINGS = {
    TRANSPORT_FIXTURE: (
        "78e0f294044fe39e56a546028b3ff7382e61901c6858b3d6b6313d0daf160d99",
        "a24fb89f1305296ef334ecfa5262b732d3fcf6f5aa296f42e2f60ecfae1cbe0b",
    ),
    SESSION_FIXTURE: (
        "f9f77cb4e379fde3caa097dc1afeb04c71fedad0af48c9bc5aa88b74dfd31dc4",
        "78667a51d332f32c256e8813e7bcd006029d44cabe13c8f1fed61c306e10363a",
    ),
    RAW_FIXTURE: (
        "4dec38da4152c2461a6530d597fe199ff31ca3378c5815622eeb9f60b9b0893e",
        "16efef81d24d1291fafe3208ec4346d0692463162f52eeb518c4d6a267a0259b",
    ),
}
PROJECTION_SHA256 = (
    "8ae40e1192c892751e35cb562a123e38eeee803b883a410087d1c43b764ee232"
)


def _load_fixture_mapping(path: Path) -> dict:
    compressed_sha256, content_sha256 = FIXTURE_BINDINGS[path]
    compressed = path.read_bytes()
    if hashlib.sha256(compressed).hexdigest() != compressed_sha256:
        raise AssertionError(f"compressed fixture drift: {path}")
    content = gzip.decompress(compressed)
    if hashlib.sha256(content).hexdigest() != content_sha256:
        raise AssertionError(f"fixture content drift: {path}")
    value = json.loads(content.decode("utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"fixture is not an object: {path}")
    return value


def _immutable_fixture() -> tuple[bytes, dict, bytes]:
    transport = _load_fixture_mapping(TRANSPORT_FIXTURE)
    session = _load_fixture_mapping(SESSION_FIXTURE)
    raw_object = _load_fixture_mapping(RAW_FIXTURE)
    events = [
        json.dumps(
            {"type": row["event_type"], "item": row["item"]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for row in transport["mcp_items"]
    ]
    stdout = ("\n".join(events) + "\n").encode("utf-8")
    raw_output = base64.b64decode(raw_object["raw_output_base64"], validate=True)
    if hashlib.sha256(raw_output).hexdigest() != raw_object["raw_output_sha256"]:
        raise AssertionError("immutable raw output fixture drift")
    return stdout, session, raw_output


class EnglishDuplicateReviewBoundaryTests(unittest.TestCase):
    maxDiff = None

    def _parse(self, stdout: bytes, session: dict):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        runtime = Path(temporary.name)
        runner = core.CodexRunner({}, runtime)
        try:
            runner._mcp_stage_calls(
                stdout=stdout,
                stage_name="english_analysis",
                subject="english",
                processing_context={"mcp_read_session": session},
            )
        except core._McpReviewPolicyViolation as exc:
            transcript_path = (
                runtime
                / "private/reports/mcp-stage-transcripts/sha256"
                / exc.transcript_sha256[:2]
                / f"{exc.transcript_sha256}.json"
            )
            return exc, json.loads(transcript_path.read_text(encoding="utf-8"))
        self.fail("expected one review-only duplicate policy violation")

    def _execute_actual_shape(self, raw_output: bytes):
        stdout, session, _fixture_raw = _immutable_fixture()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        codex = root / "codex"
        codex.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        codex.chmod(0o700)
        schema = root / "schema.json"
        schema.write_text('{"type":"object"}\n', encoding="utf-8")
        runner = core.CodexRunner(
            {
                "codex_path": str(codex),
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            root / "runtime",
        )
        execution_statuses: list[str] = []
        failure_digest = "f" * 64

        def invoke(command, **_kwargs):
            output_path = Path(
                command[command.index("--output-last-message") + 1]
            )
            output_path.write_bytes(raw_output)
            runner._provider_raw_refs["english_analysis"] = {
                "raw_output_object_sha256": "1" * 64,
                "raw_output_object_ref": (
                    "study-intake-model-stage-raw-output://sha256/" + "1" * 64
                ),
            }
            return SimpleNamespace(returncode=0, stdout=stdout, stderr=b"")

        def publish_execution(**kwargs):
            execution_statuses.append(kwargs["execution_status"])
            return {
                "stage_execution_receipt_sha256": "2" * 64,
                "stage_execution_receipt_ref": (
                    "study-intake-model-stage-execution://sha256/" + "2" * 64
                ),
            }

        def publish_normalization(**kwargs):
            warnings = tuple(kwargs["warnings"])
            return {
                "stage_normalization_receipt_sha256": "3" * 64,
                "stage_normalization_receipt_ref": (
                    "study-intake-model-stage-normalization://sha256/" + "3" * 64
                ),
                "normalization_status": "normalized_with_warnings",
                "normalization_warning_count": len(warnings),
                "normalization_warnings": warnings,
            }

        with (
            mock.patch.object(runner, "_invoke_subprocess", side_effect=invoke),
            mock.patch.object(runner, "_model_mcp_config_args", return_value=[]),
            mock.patch.object(
                runner,
                "_publish_model_stage_execution",
                side_effect=publish_execution,
            ),
            mock.patch.object(
                runner,
                "_publish_model_stage_normalization",
                side_effect=publish_normalization,
            ),
            mock.patch.object(
                runner,
                "_sign_mcp_stage_failure",
                return_value={
                    "mcp_failure_receipt_sha256": failure_digest,
                    "mcp_failure_receipt_ref": (
                        "study-intake-model-mcp-failure://sha256/"
                        + failure_digest
                    ),
                },
            ) as sign_failure,
        ):
            try:
                runner._execute_prompt(
                    prompt="bounded English review fixture",
                    output_schema=schema,
                    image_paths=(),
                    stage_name="english_analysis",
                    max_prompt_bytes=4096,
                    max_output_bytes=65536,
                    allowed_evidence_refs=(),
                    bind_evidence_schema=False,
                    subject="english",
                    processing_context={"mcp_read_session": session},
                )
            except core.PreprocessorError as exc:
                return exc, execution_statuses, sign_failure
        self.fail("actual duplicate shape must not be automatically adoptable")

    def test_actual_40_call_shape_is_reviewable_and_preserves_later_reads(self) -> None:
        stdout, session, raw_output = _immutable_fixture()
        self.assertIsInstance(json.loads(raw_output), dict)
        self.assertEqual(len(raw_output), 11494)
        error, transcript = self._parse(stdout, session)
        self.assertEqual(error.code, "english_analysis_mcp_duplicate_read")
        self.assertEqual(error.duplicate_result_projection_sha256, PROJECTION_SHA256)
        self.assertEqual(len(error.calls), 40)
        self.assertEqual(transcript["coverage"]["call_count"], 40)
        self.assertEqual(transcript["coverage"]["duplicate_argument_count"], 1)
        self.assertEqual(transcript["formal_write_count"], 0)
        self.assertEqual(transcript["calls"][18]["arguments"], transcript["calls"][19]["arguments"])
        self.assertEqual(transcript["calls"][19]["sequence"], 20)
        self.assertEqual(transcript["calls"][-1]["sequence"], 40)
        self.assertEqual(transcript["calls"][-1]["tool"], "search_records")

    def test_actual_valid_raw_preserves_technical_error_without_mcp_failure(self) -> None:
        _stdout, _session, raw_output = _immutable_fixture()
        error, statuses, sign_failure = self._execute_actual_shape(raw_output)
        self.assertEqual(error.code, "english_analysis_mcp_duplicate_read")
        self.assertEqual(statuses, ["completed"])
        sign_failure.assert_not_called()
        self.assertNotIn("mcp_failure_receipt_sha256", error.diagnostic)
        self.assertTrue(error.diagnostic["post_stage_validation_failed"])
        self.assertNotIn("report_disposition", error.diagnostic)
        self.assertNotIn("review_candidate_stage", error.diagnostic)
        self.assertRegex(
            error.diagnostic["mcp_transcript_sha256"],
            r"^[0-9a-f]{64}$",
        )

    def test_actual_transcript_with_invalid_raw_remains_execution_failure(self) -> None:
        error, statuses, sign_failure = self._execute_actual_shape(b"not-json")
        self.assertEqual(error.code, "english_analysis_output_invalid_json")
        self.assertEqual(statuses, ["completed"])
        sign_failure.assert_called_once()
        self.assertIn("mcp_failure_receipt_sha256", error.diagnostic)
        self.assertNotIn("review_candidate_stage", error.diagnostic)

    def test_changed_duplicate_result_remains_execution_failure(self) -> None:
        stdout, session, _raw_output = _immutable_fixture()
        events = [json.loads(line) for line in stdout.splitlines()]
        changed = events[19]["item"]["result"]
        for representation in (
            changed["structured_content"],
            json.loads(changed["content"][0]["text"]),
        ):
            representation["warnings"] = ["changed duplicate result"]
            if representation is not changed["structured_content"]:
                changed["content"][0]["text"] = json.dumps(
                    representation,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
        mutated = ("\n".join(json.dumps(row) for row in events) + "\n").encode()
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(core.PreprocessorError) as raised:
                core.CodexRunner({}, Path(temporary))._mcp_stage_calls(
                    stdout=mutated,
                    stage_name="english_analysis",
                    subject="english",
                    processing_context={"mcp_read_session": session},
                )
        self.assertEqual(raised.exception.code, "english_analysis_mcp_duplicate_read")
        self.assertNotIsInstance(raised.exception, core._McpReviewPolicyViolation)

    def test_third_identical_read_remains_execution_failure(self) -> None:
        stdout, session, _raw_output = _immutable_fixture()
        events = [json.loads(line) for line in stdout.splitlines()]
        third = copy.deepcopy(events[19])
        events.insert(20, third)
        mutated = ("\n".join(json.dumps(row) for row in events) + "\n").encode()
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(core.PreprocessorError) as raised:
                core.CodexRunner({}, Path(temporary))._mcp_stage_calls(
                    stdout=mutated,
                    stage_name="english_analysis",
                    subject="english",
                    processing_context={"mcp_read_session": session},
                )
        self.assertEqual(raised.exception.code, "english_analysis_mcp_duplicate_read")
        self.assertNotIsInstance(raised.exception, core._McpReviewPolicyViolation)


if __name__ == "__main__":
    unittest.main()
