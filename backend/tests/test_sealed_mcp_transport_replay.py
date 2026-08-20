#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

import test_model_driven_mcp_architecture as architecture  # noqa: E402
from preprocessor_core import (  # noqa: E402
    CodexRunner,
    PreprocessorError,
    StructuredStageResult,
    mcp_grounding_manifest,
    mcp_grounding_refs,
    validate_english_mcp_grounding,
)


SEALED_4C1_ENGLISH_TRANSPORT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-4c1c0651/stage-runtime/private/reports/"
    "model-mcp-transport/sha256/24/"
    "2490329a7d0cbfde65fab50a1e05a47af70283bafc0bb8179430b8e3dc916058.json"
)
SEALED_4C1_ENGLISH_TRANSPORT_SHA256 = (
    "2490329a7d0cbfde65fab50a1e05a47af70283bafc0bb8179430b8e3dc916058"
)
SEALED_4C1_ENGLISH_READ_SESSION = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-4c1c0651/stage-runtime/private/mcp-read-sessions/"
    "sha256/8f/"
    "8f7104504ad0fec9f6a275329023e186a608d601f18308e856c6c3688e0c9a5a.json"
)
SEALED_4C1_ENGLISH_READ_SESSION_FILE_SHA256 = (
    "03cea628189099d150c9c54f8d9a13e679c14f71d927db9204902f773df70433"
)


class SealedMcpTransportReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = architecture.ModelDrivenMcpArchitectureTests(
            methodName="runTest"
        )

    def _mcp_context(self, subject: str) -> dict:
        return self.fixture._mcp_context(subject)

    def _required_success_events(self, subject: str) -> list[str]:
        return self.fixture._required_success_events(subject)

    def _mcp_error_event(self, **kwargs) -> str:
        return self.fixture._mcp_error_event(**kwargs)

    def _parse_events(
        self,
        subject: str,
        events: list[str],
        *,
        processing_context: dict | None = None,
        reopen_transcript: bool = False,
    ):
        with tempfile.TemporaryDirectory() as temp:
            runtime_root = Path(temp)
            runner = CodexRunner({}, runtime_root)
            calls, transcript_sha256, transcript_ref = runner._mcp_stage_calls(
                stdout=("\n".join(events) + "\n").encode(),
                stage_name=f"{subject}_analysis",
                subject=subject,
                processing_context=(
                    processing_context or self._mcp_context(subject)
                ),
            )
            if not reopen_transcript:
                return calls, transcript_sha256, transcript_ref, None
            self.assertIsNotNone(transcript_sha256)
            transcript_path = (
                runtime_root
                / "private"
                / "reports"
                / "mcp-stage-transcripts"
                / "sha256"
                / str(transcript_sha256)[:2]
                / f"{transcript_sha256}.json"
            )
            transcript = json.loads(transcript_path.read_text(encoding="utf-8"))
            return calls, transcript_sha256, transcript_ref, transcript

    def _sealed_english_transport(self):
        transport_bytes = SEALED_4C1_ENGLISH_TRANSPORT.read_bytes()
        session_bytes = SEALED_4C1_ENGLISH_READ_SESSION.read_bytes()
        self.assertEqual(
            hashlib.sha256(transport_bytes).hexdigest(),
            SEALED_4C1_ENGLISH_TRANSPORT_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(session_bytes).hexdigest(),
            SEALED_4C1_ENGLISH_READ_SESSION_FILE_SHA256,
        )
        transport = json.loads(transport_bytes)
        session = json.loads(session_bytes)
        self.assertEqual(
            transport["read_session_manifest_sha256"],
            session["manifest_sha256"],
        )
        events = [
            json.dumps(
                {"type": row["event_type"], "item": row["item"]},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for row in transport["mcp_items"]
        ]
        return transport, session, events

    def test_sealed_4c1_duplicate_output_limit_replays_cleanly(self) -> None:
        transport, session, events = self._sealed_english_transport()
        self.assertEqual(transport["mcp_item_count"], 8)
        failed = [
            row["item"]
            for row in transport["mcp_items"]
            if json.loads(row["item"]["result"]["content"][0]["text"])["ok"]
            is False
        ]
        self.assertEqual(
            [
                json.loads(item["result"]["content"][0]["text"])["error"][
                    "code"
                ]
                for item in failed
            ],
            ["NOT_FOUND", "OUTPUT_LIMIT", "OUTPUT_LIMIT"],
        )
        self.assertEqual(failed[-2]["arguments"], failed[-1]["arguments"])
        self.assertNotEqual(
            json.loads(failed[-2]["result"]["content"][0]["text"])[
                "request_id"
            ],
            json.loads(failed[-1]["result"]["content"][0]["text"])[
                "request_id"
            ],
        )

        calls, transcript_sha256, _ref, transcript = self._parse_events(
            "english",
            events,
            processing_context={"mcp_read_session": session},
            reopen_transcript=True,
        )
        self.assertEqual(
            [call["tool"] for call in calls],
            [
                "get_task_context",
                "read_task_artifact",
                "get_records",
                "list_records",
                "search_records",
            ],
        )
        self.assertEqual([call["sequence"] for call in calls], [1, 2, 3, 4, 5])
        self.assertTrue(all(call["result"]["ok"] is True for call in calls))
        self.assertEqual(transcript["calls"], list(calls))
        self.assertEqual(
            transcript["coverage"],
            {
                "call_count": 5,
                "all_returned_pages_consumed": True,
                "unresolved_next_cursors": [],
                "duplicate_argument_count": 0,
                "host_semantic_prefetch": False,
            },
        )
        self.assertEqual(transcript["mcp_tool_call_count"], 5)
        self.assertEqual(transcript["formal_write_count"], 0)
        self.assertRegex(str(transcript_sha256), r"^[0-9a-f]{64}$")

        stage = StructuredStageResult(
            payload={},
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256="4" * 64,
            mcp_transcript_sha256=str(transcript_sha256),
            mcp_tool_call_count=len(calls),
            provider_request_count=len(calls) + 1,
            mcp_calls=tuple(calls),
        )
        allowed_refs = mcp_grounding_refs((stage,))
        failed_attempt_ref = "mcp-item:english:" + "f" * 64
        self.assertTrue(allowed_refs)
        self.assertNotIn(failed_attempt_ref, allowed_refs)
        with self.assertRaisesRegex(
            PreprocessorError,
            "english_mcp_grounding_invalid",
        ):
            validate_english_mcp_grounding(
                [{
                    "grounding": {
                        "mcp_evidence_refs": [failed_attempt_ref],
                    }
                }],
                grounding_manifest=mcp_grounding_manifest((stage,)),
            )

    def test_safe_error_matrix_does_not_pollute_three_subject_ledgers(self) -> None:
        cases = (
            (
                "INVALID_ARGUMENT",
                "read_task_artifact",
                {"artifact_id": "capture-facts", "max_bytes": 100000},
            ),
            (
                "NOT_FOUND",
                "get_records",
                {
                    "collection": "catalog",
                    "ids": ["missing-id"],
                    "page_size": 48,
                },
            ),
            (
                "OUTPUT_LIMIT",
                "search_records",
                {"query": "broad-query", "page_size": 48},
            ),
        )
        for subject in ("english", "math", "cs408"):
            success = self._required_success_events(subject)
            for code, tool, arguments in cases:
                with self.subTest(subject=subject, code=code):
                    if subject == "math" and code == "OUTPUT_LIMIT":
                        # Math search OUTPUT_LIMIT has its own strict 48→...→1
                        # state-machine matrix in the architecture suite.
                        continue
                    error = self._mcp_error_event(
                        subject=subject,
                        tool=tool,
                        arguments=arguments,
                        code=code,
                        status=(
                            "failed" if code == "INVALID_ARGUMENT" else "completed"
                        ),
                    )
                    calls, _sha, _ref, transcript = self._parse_events(
                        subject,
                        [success[0], error, error, success[1], success[2]],
                        reopen_transcript=True,
                    )
                    self.assertEqual(
                        [call["tool"] for call in calls],
                        [
                            "get_task_context",
                            "read_task_artifact",
                            "list_records",
                        ],
                    )
                    self.assertEqual(
                        [call["sequence"] for call in calls],
                        [1, 2, 3],
                    )
                    self.assertTrue(
                        all(call["result"]["ok"] is True for call in calls)
                    )
                    self.assertEqual(transcript["calls"], list(calls))
                    self.assertEqual(transcript["coverage"]["call_count"], 3)
                    self.assertEqual(
                        transcript["coverage"]["duplicate_argument_count"],
                        0,
                    )
                    self.assertEqual(transcript["formal_write_count"], 0)

    def test_failed_exploration_cannot_satisfy_required_reads(self) -> None:
        for subject in ("english", "math", "cs408"):
            success = self._required_success_events(subject)
            artifact_error = self._mcp_error_event(
                subject=subject,
                tool="read_task_artifact",
                arguments={
                    "artifact_id": "capture-facts",
                    "max_bytes": 100000,
                },
                code="INVALID_ARGUMENT",
                status="failed",
            )
            library_error = self._mcp_error_event(
                subject=subject,
                tool="search_records",
                arguments={"query": "broad-query", "page_size": 48},
                code="OUTPUT_LIMIT",
            )
            cases = (
                ("task_context", [success[1], success[2]]),
                ("artifact", [success[0], artifact_error, success[2]]),
                ("library", [success[0], success[1], library_error]),
            )
            for requirement, events in cases:
                with self.subTest(subject=subject, requirement=requirement):
                    expected_error = (
                        "math_analysis_mcp_output_limit_backoff_invalid"
                        if subject == "math" and requirement == "library"
                        else f"{subject}_analysis_mcp_required_reads_incomplete"
                    )
                    with self.assertRaisesRegex(
                        PreprocessorError,
                        expected_error,
                    ):
                        self._parse_events(subject, events)

        invalid_task_context = self._mcp_error_event(
            subject="english",
            tool="get_task_context",
            arguments={},
            code="INVALID_ARGUMENT",
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "english_analysis_mcp_server_invalid_argument",
        ):
            self._parse_events("english", [invalid_task_context])

    def test_integrity_and_unknown_failures_stay_fatal(self) -> None:
        def mutate_success(event: str, mutator) -> str:
            value = json.loads(event)
            mutator(value["item"]["result"])
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        for subject in ("english", "math", "cs408"):
            success = self._required_success_events(subject)
            for code in ("AUTHORITY_DRIFT", "UNRECOGNIZED_ERROR"):
                with self.subTest(subject=subject, failure=code):
                    error = self._mcp_error_event(
                        subject=subject,
                        tool="list_records",
                        arguments={"collection": "catalog", "page_size": 48},
                        code=code,
                    )
                    with self.assertRaisesRegex(
                        PreprocessorError,
                        f"{subject}_analysis_mcp_server_{code.casefold()}",
                    ):
                        self._parse_events(
                            subject,
                            [success[0], success[1], error, success[2]],
                        )

            generation_drift = mutate_success(
                success[2],
                lambda envelope: envelope.__setitem__(
                    "generation", "drifted-generation"
                ),
            )
            with self.subTest(subject=subject, failure="generation"):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"{subject}_analysis_mcp_generation_mismatch",
                ):
                    self._parse_events(
                        subject,
                        [success[0], success[1], generation_drift],
                    )

            cursor_value = json.loads(success[2])
            cursor_value["item"]["arguments"]["cursor"] = "tampered"
            cursor_tamper = json.dumps(
                cursor_value, sort_keys=True, separators=(",", ":")
            )
            with self.subTest(subject=subject, failure="cursor"):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"{subject}_analysis_mcp_cursor_without_first_page",
                ):
                    self._parse_events(
                        subject,
                        [success[0], success[1], cursor_tamper],
                    )

            hash_tamper = mutate_success(
                success[2],
                lambda envelope: envelope["items"][0].__setitem__(
                    "source_hash", "f" * 64
                ),
            )
            with self.subTest(subject=subject, failure="hash"):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"{subject}_analysis_mcp_page_contract_invalid",
                ):
                    self._parse_events(
                        subject,
                        [success[0], success[1], hash_tamper],
                    )

            def drift_representation(envelope: dict) -> None:
                envelope["error"]["message"] = "different representation"

            representation_drift = self._mcp_error_event(
                subject=subject,
                tool="read_task_artifact",
                arguments={
                    "artifact_id": "capture-facts",
                    "max_bytes": 100000,
                },
                code="INVALID_ARGUMENT",
                status="failed",
                mutate_structured=drift_representation,
            )
            with self.subTest(subject=subject, failure="representation"):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"{subject}_analysis_mcp_result_representation_drift",
                ):
                    self._parse_events(
                        subject,
                        [success[0], representation_drift, *success[1:]],
                    )


if __name__ == "__main__":
    unittest.main()
