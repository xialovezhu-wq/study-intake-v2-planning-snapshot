#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import importlib.util
import inspect
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import preprocessor_core as core  # noqa: E402
from preprocessor_core import (  # noqa: E402
    Candidate,
    CS408_CRITICAL_REVIEW_PROMPT_TEMPLATE,
    CodexRunner,
    MATH_ANALYSIS_PROMPT_TEMPLATE,
    PROCESSING_PUBLICATION_KEYS,
    PreprocessorError,
    StructuredStageResult,
    Worker,
    build_luna_proposal_v2,
    model_mcp_item_ref,
    validate_model_stage_mcp_grounding,
)


class ModelDrivenMcpArchitectureTests(unittest.TestCase):
    def test_english_read_session_explicitly_binds_grounding_validator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            analysis_schema = root / "analysis.json"
            critical_schema = root / "critical.json"
            analysis_schema.write_text('{"type":"object"}\n', encoding="utf-8")
            critical_schema.write_text('{"type":"object"}\n', encoding="utf-8")
            runner = CodexRunner(
                {
                    "english_two_pass_v1": {
                        "analysis_output_schema": str(analysis_schema),
                        "critical_review_output_schema": str(critical_schema),
                    }
                },
                root,
            )
            captured: dict[str, object] = {}

            class Host:
                def open_read_session(self, **kwargs):
                    captured.update(kwargs)
                    return {"opened": True}

            runner._processing_host = Host()
            candidate = Candidate(
                subject="english",
                capture_id="EN-TEST-VALIDATOR-BINDING",
                study_date="2026-08-10",
                recorded_at="2026-08-10T00:00:00Z",
                input_fingerprint="1" * 64,
                input_binding={},
                model_input={},
                allowed_evidence_refs=(),
                image_paths=(),
                target_label="validator binding",
                canonical_state="queued",
                sol_state="pending_review",
            )
            with (
                mock.patch.object(runner, "_capture_mcp_facts", return_value={"task": True}),
                mock.patch.object(runner, "_capture_scene", return_value="study_review"),
                mock.patch.object(runner, "_capture_identity", return_value={"capture": True}),
                mock.patch.object(runner, "_capture_artifacts", return_value=()),
            ):
                self.assertEqual(runner._background_context(candidate), {"opened": True})

            expected = core.sha256_text(
                "\n".join(
                    inspect.getsource(value)
                    for value in (
                        core.validate_english_mcp_grounding,
                        core.validate_english_critical_review,
                        core.validate_english_candidate_answer_safety,
                        core.validate_english_applied_corrections,
                    )
                )
            )
            without_grounding = core.sha256_text(
                "\n".join(
                    inspect.getsource(value)
                    for value in (
                        core.validate_english_critical_review,
                        core.validate_english_candidate_answer_safety,
                        core.validate_english_applied_corrections,
                    )
                )
            )
            self.assertEqual(captured["validator_sha256"], expected)
            self.assertNotEqual(captured["validator_sha256"], without_grounding)

    def test_ordinary_request_contract_omits_service_tier_for_every_background_stage(self) -> None:
        runner = CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            ROOT,
        )
        self.assertEqual(
            runner._model_request_config_args(),
            [
                "--model",
                "gpt-5.6-luna",
                "--config",
                'model_reasoning_effort="max"',
            ],
        )
        for method in (
            CodexRunner.run_math_v2,
            CodexRunner._run_v2,
            CodexRunner._run_english,
        ):
            source = inspect.getsource(method)
            self.assertGreaterEqual(source.count("_execute_prompt("), 2)
        execute_source = inspect.getsource(CodexRunner._execute_prompt)
        self.assertIn("_model_request_config_args()", execute_source)
        self.assertIn('"--ignore-user-config"', execute_source)

    def test_service_tier_override_or_wrong_model_contract_fails_before_provider(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            codex = root / "codex"
            codex.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            codex.chmod(0o700)
            schema = root / "schema.json"
            schema.write_text('{"type":"object"}\n', encoding="utf-8")
            cases = (
                ({"service_tier": "priority"}, "config_service_tier_must_be_absent"),
                ({"service_tier": "standard"}, "config_service_tier_must_be_absent"),
                ({"reasoning_effort": "high"}, "config_reasoning_must_be_max"),
                ({"model": "other"}, "config_model_must_be_luna"),
            )
            for overrides, error_code in cases:
                with self.subTest(overrides=overrides):
                    config = {
                        "codex_path": str(codex),
                        "model": "gpt-5.6-luna",
                        "reasoning_effort": "max",
                        **overrides,
                    }
                    runner = CodexRunner(config, root)
                    with mock.patch.object(runner, "_invoke_subprocess") as invoke:
                        with self.assertRaisesRegex(PreprocessorError, error_code):
                            runner._execute_prompt(
                                prompt="ordinary-mode pre-provider gate",
                                output_schema=schema,
                                image_paths=(),
                                stage_name="math_analysis",
                                max_prompt_bytes=4096,
                                max_output_bytes=4096,
                                allowed_evidence_refs=(),
                                bind_evidence_schema=False,
                                subject="math",
                            )
                        invoke.assert_not_called()

    def test_all_ignore_user_config_luna_scripts_omit_service_tier(self) -> None:
        for relative in (
            "scripts/read_only_luna_validation.py",
            "scripts/smoke_model_driven_mcp.py",
            "scripts/smoke_cs408_provider_schema.py",
        ):
            with self.subTest(path=relative):
                source = (ROOT / relative).read_text(encoding="utf-8")
                self.assertIn('"--ignore-user-config"', source)
                self.assertIn('"gpt-5.6-luna"', source)
                self.assertIn('model_reasoning_effort="max"', source)
                self.assertNotIn('service_tier="priority"', source)

    def test_absent_service_tier_is_not_misreported_as_effective(self) -> None:
        runner = CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            ROOT,
        )
        self.assertEqual(
            runner._runtime_metadata(
                b"",
                b'{"model":"gpt-5.6-luna","reasoning_effort":"max"}\n',
            ),
            (None, None, "unavailable"),
        )

    def test_checkpoint_context_strips_only_private_manifest_locator(self) -> None:
        for subject in ("english", "cs408"):
            with self.subTest(subject=subject):
                context = {
                    "mcp_read_session": {
                        **self._mcp_context(subject)["mcp_read_session"],
                        "capture_manifest_path": (
                            f"/Users/private/{subject}/capture-manifest.json"
                        ),
                    },
                    "processing_binding": {"subject": subject},
                    "formal_write_count": 0,
                }
                checkpoint_context = CodexRunner._processing_checkpoint_context(
                    context
                )
                self.assertNotIn(
                    "capture_manifest_path",
                    checkpoint_context["mcp_read_session"],
                )
                self.assertNotIn(
                    "/Users/",
                    json.dumps(checkpoint_context, ensure_ascii=False),
                )

    def test_checkpoint_context_rejects_an_unrelated_local_path(self) -> None:
        context = {
            "mcp_read_session": {
                **self._mcp_context("english")["mcp_read_session"],
                "capture_manifest_path": "/Users/private/capture-manifest.json",
            },
            "processing_binding": {
                "subject": "english",
                "unexpected_locator": "/Users/private/must-not-survive.json",
            },
        }
        with self.assertRaisesRegex(
            PreprocessorError, "analysis_v2_local_path_leak"
        ):
            CodexRunner._processing_checkpoint_context(context)

    def test_model_mcp_launcher_binds_the_isolated_runtime_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            session_path = root / "read-session.json"
            session_path.write_text("{}\n", encoding="utf-8")
            runtime_root = root / "isolated-runtime"
            runtime_root.mkdir()

            # The successor no longer accepts an arbitrary test launcher.  It
            # resolves the exact immutable MCP release, Python executable and
            # sealed launcher from the component lock before composing the
            # per-task read-session argv.  Reuse that real zero-model binding
            # here while keeping only the task runtime/session in the isolated
            # temporary root.
            lock_path = (
                ROOT
                / "plugin"
                / "kaoyan-study-intake"
                / "component-lock.json"
            )
            component_lock = json.loads(lock_path.read_text(encoding="utf-8"))
            sealed = component_lock["mcp_sealed_runtime"]
            python_path = Path(sealed["python_executable"])
            release_root = Path(sealed["release_root"])

            host = SimpleNamespace(
                python_path=python_path,
                mcp_project_root=release_root,
                runtime_root=runtime_root,
                lock_path=lock_path,
                validate_read_session_context=lambda **kwargs: kwargs["context"],
                read_session_manifest_path=lambda context: session_path,
            )
            runner = CodexRunner({}, runtime_root)
            runner._processing_host = host
            config_args = runner._model_mcp_config_args(
                subject="math", processing_context=self._mcp_context()
            )
            values = config_args[1::2]
            encoded_args = next(
                value.split("=", 1)[1]
                for value in values
                if value.startswith("mcp_servers.kaoyan_math_read.args=")
            )
            self.assertEqual(
                json.loads(encoded_args),
                [
                    "-I",
                    "-S",
                    sealed["sealed_launcher_path"],
                    "--release-root",
                    str(release_root),
                    "--expected-release-id",
                    sealed["release_id"],
                    "--expected-release-manifest-sha256",
                    sealed["release_manifest_sha256"],
                    "--mode",
                    "subject-server",
                    "--subject",
                    "math",
                    "--read-session-manifest",
                    str(session_path),
                    "--preprocessor-root",
                    str(runtime_root),
                ],
            )

    def test_host_item_ref_matches_mcp_service_canonical_contract(self) -> None:
        binding = {
            "collection": "formal_card_records",
            "generation": "math-generation-1",
            "source_hash": "a" * 64,
            "stable_id": "GS-111",
        }
        digest = hashlib.sha256(
            json.dumps(
                binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(
            model_mcp_item_ref(
                subject="math",
                generation="math-generation-1",
                collection="formal_card_records",
                stable_id="GS-111",
                source_hash="a" * 64,
            ),
            f"mcp-item:math:{digest}",
        )

    def _mcp_context(self, subject: str = "math") -> dict:
        capture_id = {
            "math": "GS-111",
            "cs408": "OBS-91DC4259E9E0BC1996B01884",
            "english": "EN-20260806-D11B26D125E29827",
        }[subject]
        return {
            "mcp_read_session": {
                "schema_version": "study-read-mcp-read-session.v2",
                "subject": subject,
                "read_session_id": f"rs-test-{subject}-0001",
                "manifest_sha256": "1" * 64,
                "candidate_release_id": "4" * 64,
                "plugin_version": "0.4.0-canary.1",
                "skill_id": f"background-{subject}-processing",
                "skill_version": "3.0.0",
                "mcp_server_release": "0.3.0+sha256." + "5" * 64,
                "generation": "generation-1",
                "authority_fingerprint": "2" * 64,
                "capture_id": capture_id,
                "capture_manifest_sha256": "3" * 64,
                "artifact_ids": ["capture-facts"],
            }
        }

    def _mcp_event(
        self,
        *,
        cursor=None,
        offset: int,
        total_count: int,
        returned_count: int,
        next_cursor=None,
        truncated: bool,
        complete: bool,
        generation: str = "generation-1",
        tool: str = "list_records",
        subject: str = "math",
    ) -> str:
        capture_id = self._mcp_context(subject)["mcp_read_session"]["capture_id"]
        if tool == "get_task_context":
            arguments = {}
            collection = "task_context"
        elif tool == "read_task_artifact":
            arguments = {
                "artifact_id": "capture-facts",
                "cursor": cursor,
                "max_bytes": 16384,
            }
            collection = "task_artifact"
        else:
            arguments = {
                "collection": "catalog",
                "page_size": 1,
                "article_id": None,
                "study_date": None,
                "cursor": cursor,
            }
            collection = "catalog"
        items = []
        for index in range(returned_count):
            stable_id = (
                capture_id
                if tool == "get_task_context"
                else "capture-facts"
                if tool == "read_task_artifact"
                else f"GS-{offset + index + 1:03d}"
            )
            source_hash = f"{offset + index + 3:x}"[-1] * 64
            items.append(
                {
                    "stable_id": stable_id,
                    "source_hash": source_hash,
                    "data_role": "formal_card",
                    "collection": collection,
                    "evidence_ref": model_mcp_item_ref(
                        subject=subject,
                        generation=generation,
                        collection=collection,
                        stable_id=stable_id,
                        source_hash=source_hash,
                    ),
                }
            )
        envelope = {
            "ok": True,
            "schema_version": "study-read-mcp.v3",
            "server_release": "0.3.0+sha256." + "5" * 64,
            "adapter_release": "0.3.0+sha256." + "5" * 64,
            "preprocessor_release": "4" * 64,
            "subject": subject,
            "profile": "luna",
            "consistency": "bound_snapshot",
            "generation": generation,
            "authority_fingerprint": "2" * 64,
            "read_session": {
                "schema_version": "study-read-mcp-read-session.v2",
                "read_session_id": f"rs-test-{subject}-0001",
                "manifest_sha256": "1" * 64,
                "candidate_release_id": "4" * 64,
                "plugin_version": "0.4.0-canary.1",
                "subject": subject,
                "generation": "generation-1",
                "authority_fingerprint": "2" * 64,
                "skill_id": f"background-{subject}-processing",
                "skill_version": "3.0.0",
                "mcp_server_release": "0.3.0+sha256." + "5" * 64,
                "capture_id": capture_id,
                "capture_manifest_sha256": "3" * 64,
                "artifact_ids": ["capture-facts"],
                "formal_write_count": 0,
            },
            "read_route": {
                "caller_skill_id": f"background-{subject}-processing",
                "caller_skill_version": "3.0.0",
                "plugin_version": "0.4.0-canary.1",
                "route_request_id": f"rs-test-{subject}-0001",
                "evidence_scope_hash": "1" * 64,
                "read_route": "mcp_model_driven",
                "read_session_id": f"rs-test-{subject}-0001",
                "consumed_duplicate_read_count": 0,
            },
            "formal_write_count": 0,
            "model_call_count": 0,
            "mcp_tool_call_count": 1,
            "total_count": total_count,
            "returned_count": returned_count,
            "offset": offset,
            "page_size": 1,
            "query_sha256": hashlib.sha256(
                json.dumps(
                    {
                        "tool": tool,
                        "arguments": {
                            key: value
                            for key, value in arguments.items()
                            if key != "cursor"
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
            "next_cursor": next_cursor,
            "truncated": truncated,
            "complete": complete,
            "items": items,
        }
        return json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "mcp_tool_call",
                    "server": f"kaoyan_{subject}_read",
                    "tool": tool,
                    "arguments": arguments,
                    "result": envelope,
                },
            }
        )

    def _mcp_error_event(
        self,
        *,
        subject: str,
        tool: str,
        arguments: dict,
        code: str,
        status: str = "completed",
        retryable: bool = False,
        mutate_structured=None,
    ) -> str:
        envelope = {
            "ok": False,
            "schema_version": "study-read-mcp.v3",
            "server_release": "0.3.0+sha256." + "5" * 64,
            "request_id": "0123456789abcdef",
            "tool": tool,
            "read_route": None,
            "error": {
                "code": code,
                "message": "non-evidence exploration attempt",
                "retryable": retryable,
            },
            "formal_write_count": 0,
            "model_call_count": 0,
            "mcp_tool_call_count": 0,
        }
        structured = copy.deepcopy(envelope)
        if mutate_structured is not None:
            mutate_structured(structured)
        return json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "mcp_tool_call",
                    "server": f"kaoyan_{subject}_read",
                    "tool": tool,
                    "arguments": arguments,
                    "status": status,
                    "error": None,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(
                                    envelope,
                                    ensure_ascii=False,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                            }
                        ],
                        "structured_content": structured,
                    },
                },
            }
        )

    def _mcp_search_success_event(
        self,
        *,
        subject: str = "math",
        query: str,
        page_size: int,
        cursor: str | None = None,
        offset: int = 0,
        total_count: int = 1,
        returned_count: int = 1,
        next_cursor: str | None = None,
        truncated: bool = False,
        complete: bool = True,
    ) -> str:
        event = json.loads(
            self._mcp_event(
                subject=subject,
                tool="list_records",
                cursor=cursor,
                offset=offset,
                total_count=total_count,
                returned_count=returned_count,
                next_cursor=next_cursor,
                truncated=truncated,
                complete=complete,
            )
        )
        item = event["item"]
        item["tool"] = "search_records"
        arguments = {"query": query, "page_size": page_size}
        if cursor is not None:
            arguments["cursor"] = cursor
        item["arguments"] = arguments
        envelope = item["result"]
        for row in envelope["items"]:
            row["collection"] = "search"
            row["evidence_ref"] = model_mcp_item_ref(
                subject=subject,
                generation=str(envelope["generation"]),
                collection="search",
                stable_id=str(row["stable_id"]),
                source_hash=str(row["source_hash"]),
            )
        envelope["page_size"] = page_size
        envelope["query_sha256"] = hashlib.sha256(
            json.dumps(
                {
                    "tool": "search_records",
                    "arguments": {
                        key: value
                        for key, value in arguments.items()
                        if key != "cursor"
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return json.dumps(event)

    def _parse_subject_mcp_events(
        self,
        subject: str,
        events: list[str],
        *,
        stage_name: str | None = None,
    ):
        with tempfile.TemporaryDirectory() as temp:
            runner = CodexRunner({}, Path(temp))
            return runner._mcp_stage_calls(
                stdout=("\n".join(events) + "\n").encode(),
                stage_name=stage_name or f"{subject}_analysis",
                subject=subject,
                processing_context=self._mcp_context(subject),
            )

    def _parse_mcp_events(self, events: list[str]):
        required = [
            self._mcp_event(
                offset=0,
                total_count=1,
                returned_count=1,
                truncated=False,
                complete=True,
                tool="get_task_context",
            ),
            self._mcp_event(
                offset=0,
                total_count=1,
                returned_count=1,
                truncated=False,
                complete=True,
                tool="read_task_artifact",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp:
            runner = CodexRunner({}, Path(temp))
            return runner._mcp_stage_calls(
                stdout=("\n".join([*required, *events]) + "\n").encode(),
                stage_name="math_analysis",
                subject="math",
                processing_context=self._mcp_context(),
            )

    def _required_success_events(self, subject: str) -> list[str]:
        return [
            self._mcp_event(
                offset=0,
                total_count=1,
                returned_count=1,
                truncated=False,
                complete=True,
                tool="get_task_context",
                subject=subject,
            ),
            self._mcp_event(
                offset=0,
                total_count=1,
                returned_count=1,
                truncated=False,
                complete=True,
                tool="read_task_artifact",
                subject=subject,
            ),
            self._mcp_event(
                offset=0,
                total_count=1,
                returned_count=1,
                truncated=False,
                complete=True,
                tool="list_records",
                subject=subject,
            ),
        ]

    def test_math_real_transport_out_of_range_artifact_retry_signature(self) -> None:
        success = self._required_success_events("math")
        error = self._mcp_error_event(
            subject="math",
            tool="read_task_artifact",
            arguments={
                "artifact_id": "capture-facts",
                "max_bytes": 100000,
            },
            code="INVALID_ARGUMENT",
            status="failed",
        )
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "math", [success[0], error, *success[1:]]
        )
        self.assertEqual([call["tool"] for call in calls], [
            "get_task_context",
            "read_task_artifact",
            "list_records",
        ])
        self.assertEqual([call["sequence"] for call in calls], [1, 2, 3])
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

    def test_cs408_real_transport_out_of_range_artifact_retry_signature(self) -> None:
        success = self._required_success_events("cs408")
        events = [
            success[0],
            self._mcp_error_event(
                subject="cs408",
                tool="read_task_artifact",
                arguments={
                    "artifact_id": "capture-facts",
                    "max_bytes": 65536,
                },
                code="INVALID_ARGUMENT",
                status="failed",
            ),
            success[1],
            self._mcp_error_event(
                subject="cs408",
                tool="list_records",
                arguments={
                    "collection": "knowledge_catalog",
                    "page_size": 48,
                },
                code="INVALID_ARGUMENT",
            ),
            success[2],
        ]
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "cs408", events
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual([call["sequence"] for call in calls], [1, 2, 3])
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

    def test_english_real_transport_out_of_range_artifact_retry_signature(self) -> None:
        success = self._required_success_events("english")

        def inject_output_defaults(envelope: dict) -> None:
            envelope.update(
                {
                    "authority_fingerprint": None,
                    "complete": None,
                    "data_role": None,
                    "generation": None,
                    "items": [],
                    "next_cursor": None,
                    "offset": None,
                    "page_size": None,
                    "query_sha256": None,
                    "read_session": None,
                    "returned_count": None,
                    "subject": None,
                    "total_count": None,
                    "truncated": None,
                }
            )

        events = [
            success[0],
            self._mcp_error_event(
                subject="english",
                tool="read_task_artifact",
                arguments={
                    "artifact_id": "capture-facts",
                    "max_bytes": 48000,
                },
                code="INVALID_ARGUMENT",
                status="failed",
            ),
            success[1],
            self._mcp_error_event(
                subject="english",
                tool="search_records",
                arguments={"query": "permanent", "page_size": 48},
                code="OUTPUT_LIMIT",
                mutate_structured=inject_output_defaults,
            ),
            self._mcp_error_event(
                subject="english",
                tool="get_records",
                arguments={
                    "collection": "mastered_items",
                    "ids": ["20260806-001"],
                    "page_size": 48,
                },
                code="NOT_FOUND",
                mutate_structured=inject_output_defaults,
            ),
            success[2],
        ]
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "english", events
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual([call["sequence"] for call in calls], [1, 2, 3])
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

    def test_unsuccessful_exploratory_mcp_duplicate_does_not_pollute_success_coverage(self) -> None:
        success = self._required_success_events("english")
        duplicate_error = self._mcp_error_event(
            subject="english",
            tool="search_records",
            arguments={"query": "permanent", "page_size": 48},
            code="OUTPUT_LIMIT",
        )
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "english",
            [success[0], success[1], duplicate_error, duplicate_error, success[2]],
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual([call["sequence"] for call in calls], [1, 2, 3])
        self.assertTrue(all(call["result"]["ok"] is True for call in calls))
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

        with self.assertRaisesRegex(
            PreprocessorError,
            "english_analysis_mcp_duplicate_read",
        ):
            self._parse_subject_mcp_events(
                "english",
                [success[0], success[1], success[2], success[2]],
            )

    def test_math_analysis_output_limit_backoff_contract(self) -> None:
        self.assertIn(
            "48、24、12、6、3、1 依次缩小 page_size",
            MATH_ANALYSIS_PROMPT_TEMPLATE,
        )
        self.assertIn(
            "只有 page_size=1 仍失败",
            MATH_ANALYSIS_PROMPT_TEMPLATE,
        )
        success = self._required_success_events("math")
        output_limit_attempts = [
            self._mcp_error_event(
                subject="math",
                tool="search_records",
                arguments={"query": "旋转体", "page_size": page_size},
                code="OUTPUT_LIMIT",
            )
            for page_size in (48, 24, 12)
        ]
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "math",
            [
                success[0],
                success[1],
                *output_limit_attempts,
                self._mcp_search_success_event(
                    query="旋转体",
                    page_size=6,
                ),
            ],
        )
        self.assertEqual([call["tool"] for call in calls], [
            "get_task_context",
            "read_task_artifact",
            "search_records",
        ])
        self.assertTrue(all(call["result"]["ok"] is True for call in calls))
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

    def test_math_output_limit_backoff_rejects_repeat_skip_and_abandon(self) -> None:
        required = self._required_success_events("math")[:2]
        first = self._mcp_error_event(
            subject="math",
            tool="search_records",
            arguments={"query": "旋转体", "page_size": 48},
            code="OUTPUT_LIMIT",
        )
        invalid_followups = (
            self._mcp_error_event(
                subject="math",
                tool="search_records",
                arguments={"query": "旋转体", "page_size": 48},
                code="OUTPUT_LIMIT",
            ),
            self._mcp_error_event(
                subject="math",
                tool="search_records",
                arguments={"query": "旋转体", "page_size": 12},
                code="OUTPUT_LIMIT",
            ),
            self._mcp_search_success_event(
                query="不同查询",
                page_size=24,
            ),
            self._required_success_events("math")[2],
        )
        for followup in invalid_followups:
            with self.subTest(followup=followup[:96]):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_analysis_mcp_output_limit_backoff_invalid",
                ):
                    self._parse_subject_mcp_events(
                        "math", [*required, first, followup]
                    )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_mcp_output_limit_backoff_invalid",
        ):
            self._parse_subject_mcp_events("math", [*required, first])

    def test_math_output_limit_backoff_closes_success_cursor_chain(self) -> None:
        required = self._required_success_events("math")[:2]
        events = [
            *required,
            self._mcp_error_event(
                subject="math",
                tool="search_records",
                arguments={"query": "旋转体", "page_size": 12},
                code="OUTPUT_LIMIT",
            ),
            self._mcp_search_success_event(
                query="旋转体",
                page_size=6,
                total_count=2,
                returned_count=1,
                next_cursor="cursor-1",
                truncated=True,
                complete=False,
            ),
            self._mcp_search_success_event(
                query="旋转体",
                page_size=6,
                cursor="cursor-1",
                offset=1,
                total_count=2,
                returned_count=1,
            ),
        ]
        calls, transcript_sha256, _ = self._parse_subject_mcp_events(
            "math", events
        )
        self.assertEqual(
            [call["tool"] for call in calls],
            [
                "get_task_context",
                "read_task_artifact",
                "search_records",
                "search_records",
            ],
        )
        self.assertRegex(transcript_sha256, r"^[0-9a-f]{64}$")

    def test_math_output_limit_backoff_is_subject_and_stage_scoped(self) -> None:
        english = self._required_success_events("english")
        repeated = self._mcp_error_event(
            subject="english",
            tool="search_records",
            arguments={"query": "permanent", "page_size": 48},
            code="OUTPUT_LIMIT",
        )
        calls, _, _ = self._parse_subject_mcp_events(
            "english",
            [english[0], english[1], repeated, repeated, english[2]],
        )
        self.assertEqual(len(calls), 3)

        math_required = self._required_success_events("math")[:2]
        calls, _, _ = self._parse_subject_mcp_events(
            "math",
            [
                *math_required,
                self._mcp_error_event(
                    subject="math",
                    tool="search_records",
                    arguments={"query": "旋转体", "page_size": 12},
                    code="OUTPUT_LIMIT",
                ),
                self._mcp_search_success_event(
                    query="旋转体",
                    page_size=6,
                ),
            ],
            stage_name="math_critical_review",
        )
        self.assertEqual(
            [call["tool"] for call in calls],
            ["get_task_context", "read_task_artifact", "search_records"],
        )

    def test_math_output_limit_backoff_rejects_invalid_attempt_shapes(self) -> None:
        required = self._required_success_events("math")[:2]
        invalid_first_attempts = (
            {"query": "旋转体", "page_size": 32},
            {"query": "旋转体", "page_size": True},
            {"query": "旋转体", "page_size": 48, "cursor": "cursor-1"},
        )
        for arguments in invalid_first_attempts:
            with self.subTest(arguments=arguments):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_analysis_mcp_output_limit_backoff_invalid",
                ):
                    self._parse_subject_mcp_events(
                        "math",
                        [
                            *required,
                            self._mcp_error_event(
                                subject="math",
                                tool="search_records",
                                arguments=arguments,
                                code="OUTPUT_LIMIT",
                            ),
                        ],
                    )

        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_mcp_output_limit_backoff_invalid",
        ):
            self._parse_subject_mcp_events(
                "math",
                [
                    *required,
                    self._mcp_error_event(
                        subject="math",
                        tool="search_records",
                        arguments={"query": "旋转体", "page_size": 48},
                        code="OUTPUT_LIMIT",
                    ),
                    self._mcp_error_event(
                        subject="math",
                        tool="search_records",
                        arguments={"query": "旋转体", "page_size": 24},
                        code="INVALID_ARGUMENT",
                    ),
                ],
            )

    def test_math_output_limit_exhaustion_allows_only_a_different_query(self) -> None:
        required = self._required_success_events("math")[:2]
        exhausted = self._mcp_error_event(
            subject="math",
            tool="search_records",
            arguments={"query": "旋转体", "page_size": 1},
            code="OUTPUT_LIMIT",
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_mcp_output_limit_backoff_invalid",
        ):
            self._parse_subject_mcp_events(
                "math",
                [
                    *required,
                    exhausted,
                    self._mcp_search_success_event(
                        query="旋转体",
                        page_size=1,
                    ),
                ],
            )

        calls, _, _ = self._parse_subject_mcp_events(
            "math",
            [
                *required,
                exhausted,
                self._mcp_search_success_event(
                    query="柱壳法",
                    page_size=1,
                ),
            ],
        )
        self.assertEqual(
            [call["tool"] for call in calls],
            ["get_task_context", "read_task_artifact", "search_records"],
        )

    def test_cs408_critical_review_dynamic_reference_binding_prevents_tainted_analysis_ref(self) -> None:
        exact_ref = "analysis.question_structure.mechanism_structure[0]"
        tainted_ref = exact_ref + "чаты?"
        schema_path = ROOT / "schemas/luna-critical-review-v2.json"
        static_before = schema_path.read_bytes()
        canonical_payload, _ = CodexRunner._bound_output_schema_bytes(
            schema_path,
            stage_name="cs408_critical_review",
            allowed_evidence_refs=("capture:test",),
            allowed_analysis_refs=(exact_ref,),
            allowed_correction_paths=("$.question_structure",),
        )
        provider_payload = (
            CodexRunner._cs408_portable_critical_review_provider_schema_bytes(
                schema_path,
                canonical_schema_payload=canonical_payload,
            )
        )
        provider_schema = json.loads(provider_payload)
        definitions = provider_schema["$defs"]
        self.assertEqual(definitions["analysis_ref"]["enum"], [exact_ref])
        self.assertNotIn(tainted_ref, definitions["analysis_ref"]["enum"])
        self.assertNotIn("enum", definitions["evidence_ref"])
        self.assertNotIn("enum", definitions["writable_correction_path"])
        self.assertEqual(schema_path.read_bytes(), static_before)
        self.assertIn(
            "analysis_refs 中的每个值必须从输入 allowed_analysis_refs 逐字复制",
            CS408_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        )

    def test_exploratory_error_does_not_satisfy_required_artifact_or_library_coverage(self) -> None:
        success = self._required_success_events("math")
        artifact_error = self._mcp_error_event(
            subject="math",
            tool="read_task_artifact",
            arguments={
                "artifact_id": "capture-facts",
                "max_bytes": 100000,
            },
            code="INVALID_ARGUMENT",
            status="failed",
        )
        library_error = self._mcp_error_event(
            subject="math",
            tool="list_records",
            arguments={"collection": "unknown", "page_size": 48},
            code="INVALID_ARGUMENT",
        )
        for events in (
            [success[0], artifact_error, success[2]],
            [success[0], success[1], library_error],
        ):
            with self.subTest(event_count=len(events)):
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_analysis_mcp_required_reads_incomplete",
                ):
                    self._parse_subject_mcp_events("math", events)

    def test_authority_or_unknown_mcp_error_remains_fatal(self) -> None:
        success = self._required_success_events("math")
        for code in ("AUTHORITY_DRIFT", "UNRECOGNIZED_ERROR"):
            with self.subTest(code=code):
                error = self._mcp_error_event(
                    subject="math",
                    tool="list_records",
                    arguments={"collection": "catalog", "page_size": 48},
                    code=code,
                )
                with self.assertRaisesRegex(
                    PreprocessorError,
                    f"math_analysis_mcp_server_{code.casefold()}",
                ):
                    self._parse_subject_mcp_events(
                        "math", [success[0], success[1], error, success[2]]
                    )

    def test_retryable_generation_mismatch_preserves_exact_error_code(self) -> None:
        success = self._required_success_events("cs408")
        error = self._mcp_error_event(
            subject="cs408",
            tool="read_task_artifact",
            arguments={
                "artifact_id": "capture-facts",
                "max_bytes": 100000,
            },
            code="GENERATION_MISMATCH",
            status="failed",
            retryable=True,
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "cs408_critical_review_mcp_server_generation_mismatch",
        ):
            self._parse_subject_mcp_events(
                "cs408",
                [success[0], error, *success[1:]],
                stage_name="cs408_critical_review",
            )
        counts = CodexRunner._mcp_transport_attempt_counts(
            b"\n".join(
                row.encode("utf-8")
                for row in [success[0], error, *success[1:]]
            ),
            stage_name="cs408_critical_review",
        )
        self.assertEqual(counts["attempted_mcp_tool_call_count"], 4)
        self.assertEqual(counts["successful_mcp_tool_call_count"], 3)
        self.assertEqual(counts["failed_mcp_tool_call_count"], 1)
        self.assertEqual(counts["last_mcp_error_code"], "GENERATION_MISMATCH")

    def test_mcp_error_representation_drift_remains_fatal(self) -> None:
        success = self._required_success_events("math")

        def drift(envelope: dict) -> None:
            envelope["error"]["message"] = "different representation"

        error = self._mcp_error_event(
            subject="math",
            tool="read_task_artifact",
            arguments={
                "artifact_id": "capture-facts",
                "max_bytes": 100000,
            },
            code="INVALID_ARGUMENT",
            status="failed",
            mutate_structured=drift,
        )
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_mcp_result_representation_drift",
        ):
            self._parse_subject_mcp_events(
                "math", [success[0], error, *success[1:]]
            )

    def test_three_production_flows_have_no_host_semantic_prefetch(self) -> None:
        forbidden = (
            "build_math_knowledge_snapshot(",
            "retrieve_math_relationship_context(",
            "build_408_knowledge_snapshot(",
            "_pinned_knowledge_snapshot(",
            "mastered_matches",
            "master_bank_matches",
            "sentence_pattern_matches",
            "four_layer_matches",
        )
        for method in (
            CodexRunner.run_math_v2,
            CodexRunner._run_v2,
            CodexRunner._run_english,
        ):
            source = inspect.getsource(method)
            for token in forbidden:
                self.assertNotIn(token, source, (method.__name__, token))
            self.assertIn("processing_context=processing_context", source)

    def test_each_model_stage_requires_real_mcp_calls(self) -> None:
        source = inspect.getsource(CodexRunner._mcp_stage_calls)
        self.assertIn("mcp_tool_call_missing", source)
        self.assertIn("mcp_cursor_incomplete", source)
        self.assertIn("mcp_generation_mismatch", source)
        self.assertIn("mcp_duplicate_read", source)

    def test_post_stage_validation_failures_are_hmac_signed_for_three_subjects(self) -> None:
        for method, minimum in (
            (CodexRunner._run_v2, 2),
            (CodexRunner.run_math_v2, 2),
            (CodexRunner._run_english, 2),
            (CodexRunner._resume_math_critical, 1),
            (CodexRunner._resume_english_critical, 1),
            (CodexRunner.resume_critical, 1),
        ):
            with self.subTest(method=method.__name__):
                self.assertGreaterEqual(
                    inspect.getsource(method).count(
                        "_post_stage_validation_error"
                    ),
                    minimum,
                )

    def test_stage_local_grounding_rejects_an_uncited_consumed_row(self) -> None:
        ref = model_mcp_item_ref(
            subject="math",
            generation="generation-1",
            collection="catalog",
            stable_id="GS-111",
            source_hash="3" * 64,
        )
        stage = StructuredStageResult(
            payload={},
            duration_ms=1,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256="4" * 64,
            mcp_transcript_sha256="5" * 64,
            mcp_tool_call_count=1,
            provider_request_count=2,
            mcp_calls=(
                {
                    "sequence": 1,
                    "arguments": {"collection": "catalog"},
                    "result_sha256": "6" * 64,
                    "result": {
                        "subject": "math",
                        "generation": "generation-1",
                        "items": [{
                            "collection": "catalog",
                            "stable_id": "GS-111",
                            "source_hash": "3" * 64,
                            "data_role": "formal_card",
                            "evidence_ref": ref,
                        }],
                    },
                },
            ),
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_mcp_grounding_invalid"
        ):
            validate_model_stage_mcp_grounding(
                {"claims": []}, stage,
                subject="math", stage_name="math_analysis",
            )
        self.assertEqual(
            (ref,),
            validate_model_stage_mcp_grounding(
                {"claims": [{"evidence_ref": ref}]}, stage,
                subject="math", stage_name="math_analysis",
            ),
        )

    def test_runtime_packages_use_luna_proposal_v2_without_host_projection(self) -> None:
        self.assertIn(
            "host_semantic_projection\": \"none",
            inspect.getsource(build_luna_proposal_v2),
        )
        for method in (
            Worker._write_math_v2_artifacts,
            Worker._write_v2_artifacts,
            Worker._process,
        ):
            self.assertIn("build_luna_proposal_v2", inspect.getsource(method))

    def test_mcp_pagination_accepts_only_a_complete_contiguous_chain(self) -> None:
        calls, transcript_sha256, _ = self._parse_mcp_events(
            [
                self._mcp_event(
                    offset=0, total_count=2, returned_count=1,
                    next_cursor="cursor-1", truncated=True, complete=False,
                ),
                self._mcp_event(
                    cursor="cursor-1", offset=1, total_count=2,
                    returned_count=1, truncated=False, complete=True,
                ),
            ]
        )
        self.assertEqual(len(calls), 4)
        self.assertRegex(str(transcript_sha256), r"^[0-9a-f]{64}$")

    def test_mcp_pagination_rejects_cursor_as_first_page(self) -> None:
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_mcp_cursor_without_first_page"
        ):
            self._parse_mcp_events([
                self._mcp_event(
                    cursor="cursor-1", offset=1, total_count=2,
                    returned_count=1, truncated=False, complete=True,
                )
            ])

    def test_mcp_pagination_rejects_gap_total_drift_and_incomplete_chain(self) -> None:
        cases = (
            (
                [
                    self._mcp_event(
                        offset=0, total_count=3, returned_count=1,
                        next_cursor="cursor-1", truncated=True, complete=False,
                    ),
                    self._mcp_event(
                        cursor="cursor-1", offset=2, total_count=3,
                        returned_count=1, truncated=False, complete=True,
                    ),
                ],
                "math_analysis_mcp_page_contract_invalid",
            ),
            (
                [
                    self._mcp_event(
                        offset=0, total_count=2, returned_count=1,
                        next_cursor="cursor-1", truncated=True, complete=False,
                    ),
                    self._mcp_event(
                        cursor="cursor-1", offset=1, total_count=3,
                        returned_count=1, truncated=False, complete=True,
                    ),
                ],
                "math_analysis_mcp_total_count_changed",
            ),
            (
                [
                    self._mcp_event(
                        offset=0, total_count=2, returned_count=1,
                        next_cursor="cursor-1", truncated=True, complete=False,
                    )
                ],
                "math_analysis_mcp_cursor_incomplete",
            ),
        )
        for events, code in cases:
            with self.subTest(code=code), self.assertRaisesRegex(
                PreprocessorError, code
            ):
                self._parse_mcp_events(events)

    def test_mcp_pagination_rejects_generation_change(self) -> None:
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_mcp_generation_mismatch"
        ):
            self._parse_mcp_events([
                self._mcp_event(
                    offset=0, total_count=1, returned_count=1,
                    truncated=False, complete=True,
                    generation="generation-2",
                )
            ])

    def test_publication_contract_exposes_exact_model_read_route(self) -> None:
        self.assertEqual(
            {
                "processing_binding",
                "processing_binding_sha256",
                "capture_freeze_receipt_sha256",
                "capture_freeze_receipt_ref",
                "mcp_read_session_receipt_sha256",
                "mcp_read_session_receipt_ref",
                "read_session_id",
                "read_session_manifest_sha256",
                "evidence_generation",
                "evidence_authority_fingerprint",
                "host_semantic_prefetch",
                "mcp_stage_transcripts",
                "semantic_stage_count",
                "provider_request_count",
                "mcp_tool_call_count",
                "model_call_count",
                "consumed_terminal_duplicate_read_count",
                "authority_snapshot_manifest_sha256",
            },
            set(PROCESSING_PUBLICATION_KEYS),
        )

    def test_plugin_contains_only_model_driven_contract_generation(self) -> None:
        schema_dir = ROOT / "plugin" / "kaoyan-study-intake" / "schemas"
        self.assertFalse((schema_dir / "evidence-freeze-receipt-v1.json").exists())
        self.assertFalse((schema_dir / "processing-binding-v1.json").exists())
        proposal = json.loads((schema_dir / "luna-proposal-v2.json").read_text())
        required = set(proposal["required"])
        self.assertIn("read_session_id", required)
        self.assertIn("mcp_stage_transcript_sha256s", required)
        self.assertIn("critical_review_outcome", required)
        self.assertEqual(
            proposal["properties"]["review_status"]["enum"],
            ["proposal_ready", "rejected"],
        )
        self.assertEqual(
            proposal["properties"]["critical_review_outcome"]["enum"],
            ["accepted", "corrected", "rejected"],
        )
        self.assertNotIn("evidence_freeze_sha256", required)

    def test_dashboard_distinguishes_previous_release_heartbeat(self) -> None:
        source = (ROOT / "lib" / "dashboard_projection.py").read_text()
        self.assertIn('"previous_release_heartbeat"', source)
        self.assertIn('"heartbeat_release_id"', source)
        self.assertIn('"active_release_id"', source)
        schema = json.loads(
            (ROOT / "schemas" / "dashboard-projection-v2.json").read_text()
        )
        dispatcher = schema["$defs"]["dispatcher"]
        self.assertIn("previous_release_heartbeat", dispatcher["required"])
        self.assertEqual(
            {
                "kaoyan_math_read",
                "kaoyan_cs408_read",
                "kaoyan_english_read",
            },
            set(dispatcher["properties"]["mcp_server_id"]["enum"]),
        )
        detail = json.loads(
            (ROOT / "schemas" / "dashboard-task-detail-v1.json").read_text()
        )
        runtime_status = detail["$defs"]["stage_runtime_receipt"][
            "properties"
        ]["runtime_identity_status"]
        self.assertEqual("requested_unverified", runtime_status["const"])

    def test_no_application_level_concurrency_cap(self) -> None:
        source = (ROOT / "lib" / "concurrent_dispatch.py").read_text()
        for token in (
            "threading.Semaphore",
            "threading.BoundedSemaphore",
            "max_workers",
            "global_semaphore",
            "sol_committing",
        ):
            self.assertNotIn(token, source)
        self.assertIn("thread is started for every newly claimed unique unit", source)

    def test_processing_skills_publish_provider_output_budgets(self) -> None:
        skills = ROOT / "plugin" / "kaoyan-study-intake" / "skills"
        math = (skills / "background-math-processing" / "SKILL.md").read_text()
        self.assertIn("at most 64 claim objects", math)
        self.assertIn("every claim-list field accepts at most one", math)
        self.assertIn("reasoning reconstruction accepts at most 6 claims", math)
        self.assertIn("at most 16 atomic signals", math)
        self.assertIn("mandatory fail-close gates for limited evidence", math)
        self.assertIn("parsed-string layer", math)
        self.assertIn("invalid because it parses to a doubled command escape", math)
        components = json.loads(
            (ROOT / "plugin" / "kaoyan-study-intake" / "components.json").read_text()
        )
        self.assertEqual("4.0.0", components["skills"]["background-math-processing"])
        self.assertIn("solution image or a UTF-8 non-empty `solution_text`", math)
        self.assertIn("For an independently correct episode", math)
        self.assertIn("For a wrong-then-corrected episode", math)
        self.assertIn("For source route `new_source_learning_episode`", math)
        self.assertNotIn("GS-109", math)
        self.assertNotIn("GS-507", math)
        self.assertNotIn("question-bank-id:170710", math)
        self.assertIn("fresh model context", math)
        self.assertIn("distinct `dialogue` task artifact", math)
        self.assertIn("verbatim `learning_record` task artifact", math)
        self.assertIn("Missing either the learning record or dialogue fails", math)
        self.assertIn("Missing both solution forms fails before any model call", math)
        english = (
            skills / "background-english-processing" / "SKILL.md"
        ).read_text()
        self.assertIn("old_word_example", english)
        self.assertIn("must not equal the bound source sentence", english)

    def test_processing_skills_bind_six_focused_tools_and_fresh_critic_reads(self) -> None:
        plugin_root = ROOT / "plugin" / "kaoyan-study-intake"
        components = json.loads((plugin_root / "components.json").read_text())
        focused = components["mcp"]["focused_tool_names"]
        self.assertEqual(
            [
                "get_task_context",
                "read_task_artifact",
                "list_records",
                "get_records",
                "search_records",
                "query_relations",
            ],
            focused,
        )
        for subject in ("math", "cs408", "english"):
            skill_id = f"background-{subject}-processing"
            text = (plugin_root / "skills" / skill_id / "SKILL.md").read_text()
            self.assertIn(f"study-read-mcp-{subject} --stdio", text)
            self.assertIn(f"server `kaoyan_{subject}_read`", text)
            for tool in focused:
                self.assertIn(f"`{tool}`", text)
            self.assertIn("read every returned artifact ID", text)
            self.assertIn("fresh model context", text)
            self.assertIn("cannot approve from the draft alone", text)
            self.assertIn("formal_write_count=0", text)

    def test_ordinary_skills_pin_candidate_namespace_and_reject_legacy(self) -> None:
        plugin_root = ROOT / "plugin" / "kaoyan-study-intake"
        components = json.loads((plugin_root / "components.json").read_text())
        legacy_server_names = components["mcp"]["legacy_server_names"]
        ordinary_servers = components["mcp"]["ordinary_servers"]
        bindings = components["mcp"]["ordinary_skill_tools"]
        self.assertEqual(
            {"kaoyan_math_read", "kaoyan_cs408_read", "kaoyan_english_read"},
            {row["server_name"] for row in ordinary_servers.values()},
        )
        self.assertEqual(
            ["kaoyan_read", "kaoyan_read_v2", "kaoyan_read_v3"],
            legacy_server_names,
        )
        self.assertEqual(
            {
                "kaoyan-math-visual-review": {"subject": "math", "tool_name": "math_read_bundle"},
                "kaoyan-math-weak-edge-audit": {"subject": "math", "tool_name": "math_read_bundle"},
                "kaoyan-math-nightly-qa": {"subject": "math", "tool_name": "math_read_bundle"},
                "kaoyan-english-vocab-export": {"subject": "english", "tool_name": "english_read_bundle"},
                "kaoyan-english-intensive-reading": {"subject": "english", "tool_name": "english_read_bundle"},
                "kaoyan-408-daily-intake-curation": {"subject": "cs408", "tool_name": "cs408_read_bundle"},
            },
            bindings,
        )
        for skill_id, binding in bindings.items():
            text = (plugin_root / "skills" / skill_id / "SKILL.md").read_text()
            subject = binding["subject"]
            tool_name = binding["tool_name"]
            server_name = ordinary_servers[subject]["server_name"]
            required_tool = f"mcp__{server_name}__{tool_name}"
            self.assertIn(required_tool, text)
            self.assertIn(f"required_mcp_tool={required_tool}", text)
            self.assertIn("fallback_reason=tool_snapshot_missing", text)
            self.assertIn("legacy", text)
            self.assertIn("cross-subject", text)
            self.assertIn(
                f"Skill version `{components['skills'][skill_id]}`",
                text,
            )
            for legacy_server_name in legacy_server_names:
                self.assertIn(
                    f"mcp__{legacy_server_name}__{tool_name}",
                    text,
                )

    def test_static_plugin_registers_three_subject_only_ordinary_servers(self) -> None:
        plugin_root = ROOT / "plugin" / "kaoyan-study-intake"
        for name in ("mcp.json", ".mcp.json"):
            manifest = json.loads((plugin_root / name).read_text())
            servers = manifest["mcpServers"]
            self.assertEqual(
                {"kaoyan_math_read", "kaoyan_cs408_read", "kaoyan_english_read"},
                set(servers),
            )
            for subject in ("math", "cs408", "english"):
                args = servers[f"kaoyan_{subject}_read"]["args"]
                self.assertEqual(
                    ["--profile", "ordinary", "--subjects", subject],
                    args,
                )
                self.assertNotIn("--read-session-manifest", args)

    def test_read_route_contract_represents_snapshot_fallback_and_hot_context(self) -> None:
        schema = json.loads(
            (
                ROOT
                / "plugin"
                / "kaoyan-study-intake"
                / "schemas"
                / "read-route-v1.json"
            ).read_text()
        )
        required = set(schema["required"])
        self.assertIn("required_mcp_tool", required)
        self.assertIn("fallback_reason", required)
        self.assertEqual(
            {
                "mcp",
                "mcp_chunked",
                "terminal_fallback",
                "not_eligible_hot_context",
            },
            set(schema["properties"]["read_route"]["enum"]),
        )
        self.assertEqual(
            {
                "mcp__kaoyan_math_read__math_read_bundle",
                "mcp__kaoyan_cs408_read__cs408_read_bundle",
                "mcp__kaoyan_english_read__english_read_bundle",
            },
            set(schema["properties"]["required_mcp_tool"]["enum"]),
        )
        branches = {
            branch["if"]["properties"]["read_route"]["const"]: branch["then"][
                "properties"
            ]
            for branch in schema["allOf"]
        }
        self.assertEqual(0, branches["terminal_fallback"]["chunk_index"]["const"])
        self.assertEqual(0, branches["terminal_fallback"]["chunk_count"]["const"])
        self.assertEqual("string", branches["terminal_fallback"]["fallback_reason"]["type"])
        self.assertEqual(
            "null",
            branches["not_eligible_hot_context"]["fallback_reason"]["type"],
        )

    def test_plugin_packages_task_quality_sol_and_dashboard_contract_schemas(self) -> None:
        schema_root = ROOT / "plugin" / "kaoyan-study-intake" / "schemas"
        expected = {
            "capture-freeze-receipt-v2.json": "capture_freeze_receipt_v2",
            "mcp-read-session-v2.json": "study-read-mcp-read-session.v2",
            "mcp-read-session-v3.json": "study-read-mcp-read-session.v3",
            "mcp-authority-snapshot-v1.json": "study-read-mcp-authority-snapshot.v1",
            "mcp-authority-snapshot-receipt-v1.json": "mcp_authority_snapshot_receipt_v1",
            "mcp-read-session-v4.json": "study-read-mcp-read-session.v4",
            "mcp-authority-snapshot-v2.json": "study-read-mcp-authority-snapshot.v2",
            "mcp-authority-snapshot-receipt-v2.json": "mcp_authority_snapshot_receipt_v2",
            "mcp-stage-call-receipt-v2.json": "mcp_stage_call_receipt_v2",
            "subject-luna-batch-v1.json": "subject_luna_batch_v1",
            "subject-quality-receipt-v1.json": "subject_quality_receipt_v1",
            "daily-sol-batch-v2.json": "daily_sol_batch_v2",
            "global-sol-writer-lease-v1.json": "global_sol_writer_lease_v1",
            "sol-review-receipt-v1.json": "sol_review_receipt_v1",
            "sol-commit-receipt-v1.json": "sol_commit_receipt_v1",
            "dashboard-projection-v3.json": "study-intake-dashboard-projection-v3",
            "english-legacy-target-inventory-v2.json": "english_legacy_target_inventory_v2",
            "english-legacy-disposition-receipt-v2.json": "english_legacy_disposition_receipt_v2",
            "english-legacy-disposition-closure-v2.json": "english_legacy_disposition_closure_v2",
            "english-legacy-disposition-review-template-v2.json": "english_legacy_disposition_review_template_v2",
            "en-p0-006-remediation-gate-v1.json": "en_p0_006_remediation_gate_v1",
            "english-legacy-authorization-expansion-closure-v1.json": "english_legacy_authorization_expansion_closure_v1",
            "english-legacy-batch-authorization-intent-v1.json": "english_legacy_batch_authorization_intent_v1",
            "english-legacy-batch-authorization-v1.json": "english_legacy_batch_authorization_v1",
            "english-legacy-disposition-receipt-v3.json": "english_legacy_disposition_receipt_v3",
            "english-legacy-inventory-independent-review-receipt-v1.json": "english_legacy_inventory_independent_review_receipt_v1",
            "english-legacy-recuration-execution-closure-v1.json": "english_legacy_recuration_execution_closure_v1",
            "english-legacy-recuration-package-v1.json": "english_legacy_recuration_package_v1",
            "english-legacy-recuration-quality-receipt-v1.json": "english_legacy_recuration_quality_receipt_v1",
            "english-legacy-recuration-run-summary-v1.json": "english_legacy_recuration_run_summary_v1",
            "english-legacy-recuration-sol-batch-v1.json": "english_legacy_recuration_sol_batch_v1",
            "english-legacy-recuration-work-item-batch-v1.json": "english_legacy_recuration_work_item_batch_v1",
            "english-legacy-recuration-work-item-v1.json": "english_legacy_recuration_work_item_v1",
            "english-legacy-rolling-authority-checkpoint-v1.json": "english_legacy_rolling_authority_checkpoint_v1",
            "english-legacy-sol-item-apply-receipt-v1.json": "english_legacy_sol_item_apply_receipt_v1",
            "english-legacy-sol-item-failure-receipt-v1.json": "english_legacy_sol_item_failure_receipt_v1",
            "english-legacy-sol-item-recovery-receipt-v1.json": "english_legacy_sol_item_recovery_receipt_v1",
            "english-legacy-sol-item-review-receipt-v1.json": "english_legacy_sol_item_review_receipt_v1",
            "english-legacy-target-authorization-event-v3.json": "english_legacy_target_authorization_event_v3",
            "english-legacy-target-inventory-v3.json": "english_legacy_target_inventory_v3",
            "luna-english-legacy-recuration-analysis-v1.json": "luna_english_legacy_recuration_analysis_v1",
            "luna-english-legacy-recuration-critical-review-v1.json": "luna_english_legacy_recuration_critical_review_v1",
            "production-canary-activation-receipt-v2.json": "study-intake-production-canary-activation-receipt-v2",
            "production-canary-gate-receipt-v2.json": "study-intake-production-canary-gate-receipt-v2",
            "production-canary-preclaim-failure-receipt-v2.json": "study-intake-production-canary-preclaim-failure-receipt-v2",
            "production-canary-preclaim-repair-ack-receipt-v2.json": "study-intake-production-canary-preclaim-repair-ack-receipt-v2",
            "production-canary-queue-entry-v2.json": "study-intake-production-canary-queue-entry-v2",
            "production-canary-state-v2.json": "study-intake-production-canary-state-v2",
            "subject-background-luna-batch-archive-v1.json": "subject_background_luna_batch_archive_v1",
            "subject-background-luna-rollover-pointer-v1.json": "subject_background_luna_rollover_pointer_v1",
            "subject-background-luna-rollover-receipt-v1.json": "subject_background_luna_rollover_receipt_v1",
        }
        for name, version in expected.items():
            with self.subTest(schema=name):
                schema_path = schema_root / name
                schema = json.loads(schema_path.read_text())
                self.assertEqual(version, schema["properties"]["schema_version"]["const"])
                self.assertFalse(schema["additionalProperties"])
        lock = json.loads(
            (schema_root.parent / "component-lock.json").read_text()
        )
        self.assertTrue(set(expected).issubset(lock["schemas"]))
        for name in expected:
            self.assertEqual(
                hashlib.sha256((schema_root / name).read_bytes()).hexdigest(),
                lock["schemas"][name],
            )
        quality = json.loads((schema_root / "subject-quality-receipt-v1.json").read_text())
        self.assertEqual(2, quality["properties"]["model_call_count"]["const"])
        self.assertEqual(0, quality["properties"]["formal_write_count"]["const"])
        global_sol = json.loads((schema_root / "global-sol-writer-lease-v1.json").read_text())
        self.assertEqual(1, global_sol["properties"]["active_writer_count"]["maximum"])
        commit = json.loads((schema_root / "sol-commit-receipt-v1.json").read_text())
        self.assertIn("sol_review_receipt_sha256", commit["required"])
        self.assertIn("fencing_token", commit["required"])
        self.assertFalse((schema_root / "daily-sol-batch-v1.json").exists())
        self.assertIn("execution_result_sha256", commit["required"])

    def test_plugin_version_has_one_codex_cachebuster_and_is_locked(self) -> None:
        plugin_root = ROOT / "plugin" / "kaoyan-study-intake"
        components = json.loads((plugin_root / "components.json").read_text())
        portable = json.loads((plugin_root / "plugin.json").read_text())
        codex = json.loads(
            (plugin_root / ".codex-plugin" / "plugin.json").read_text()
        )
        lock = json.loads((plugin_root / "component-lock.json").read_text())
        version = components["plugin"]["version"]
        self.assertEqual(1, version.count("+"))
        self.assertRegex(version, r"^[^+]+\+codex\.[a-z0-9]+(?:-[a-z0-9]+)*$")
        self.assertEqual(
            {version},
            {portable["version"], codex["version"], lock["plugin_version"]},
        )
        expected_servers = {
            subject: row["server_name"]
            for subject, row in components["mcp"]["ordinary_servers"].items()
        }
        self.assertEqual(expected_servers, lock["mcp_server_names"])
        for skill_id, binding in components["mcp"]["ordinary_skill_tools"].items():
            server_name = expected_servers[binding["subject"]]
            self.assertEqual(
                {
                    "subject": binding["subject"],
                    "server_name": server_name,
                    "tool_name": binding["tool_name"],
                    "codex_qualified_name": (
                        f"mcp__{server_name}__{binding['tool_name']}"
                    ),
                },
                lock["ordinary_skill_tools"][skill_id],
            )

    def test_plugin_generated_artifacts_are_staging_only(self) -> None:
        sys.path.insert(0, str(ROOT / "scripts"))
        import release_manager

        generated_paths = {
            "plugin/kaoyan-study-intake/component-lock.json",
            "validation/source-freeze-sha256-final-20260813.txt",
        }
        self.assertEqual(
            generated_paths,
            set(release_manager.STAGING_GENERATED_SOURCE_PATHS),
        )
        self.assertTrue(
            generated_paths.isdisjoint(release_manager.source_hashes(ROOT))
        )

    def test_generator_verification_root_is_hermetic_and_preserves_declarations(self) -> None:
        generator_path = ROOT / "plugin/kaoyan-study-intake/scripts/generate_manifests.py"
        spec = importlib.util.spec_from_file_location(
            "hermetic_manifest_generator", generator_path
        )
        if spec is None or spec.loader is None:
            raise AssertionError("plugin_generator_import_failed")
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        declared_unmapped = Path("/declared/no-live-file")
        self.assertEqual(
            generator.VerificationPaths().resolve(declared_unmapped),
            declared_unmapped,
        )

        with tempfile.TemporaryDirectory() as temp:
            fixture_root = Path(temp) / "verification-root"
            declared_shared_root = Path("/declared/shared/schemas")
            plugin_fixture = Path(temp) / "plugin"
            (plugin_fixture / "schemas").mkdir(parents=True)
            schema_payload = b'{"fixture":true}\n'
            (plugin_fixture / "schemas/fixture.json").write_bytes(schema_payload)
            (
                fixture_root / "declared/shared/schemas"
            ).mkdir(parents=True)
            (
                fixture_root / "declared/shared/schemas/fixture.json"
            ).write_bytes(schema_payload)

            status_declared = Path("/declared/kaoyan-408/scripts/status.py")
            status_payload = b"fixture status\n"
            (
                fixture_root / "declared/kaoyan-408/scripts"
            ).mkdir(parents=True)
            (
                fixture_root / "declared/kaoyan-408/scripts/status.py"
            ).write_bytes(status_payload)

            launcher_payload = (
                b"sys.path.append(str(dependency_root))\n"
                b"unlike site.addsitedirs\n"
                b'        "-I",\n        "-S",\n'
            )
            policy = {
                "schema_version": "study-read-mcp-policy.v4",
                "server_release": "0.4.1",
                "sealed_launcher_contract": {
                    "schema_version": "study-read-mcp-sealed-launcher.v1",
                    "required_component_fields": [
                        "python_executable",
                        "release_root",
                        "release_id",
                        "release_manifest_sha256",
                        "sealed_launcher_path",
                        "sealed_launcher_sha256",
                    ],
                    "python_flags": ["-I", "-S"],
                    "modes": [
                        "server",
                        "client",
                        "subject-server",
                        "preflight-server",
                        "snapshot",
                    ],
                    "environment_allowlist": [
                        "PATH",
                        "PYTHONUTF8",
                        "PYTHONDONTWRITEBYTECODE",
                        "PYTHONNOUSERSITE",
                        "PYTHONSAFEPATH",
                        "PYTHONPATH",
                        "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT",
                        "STUDY_READ_MCP_EXPECTED_RELEASE_ID",
                        "STUDY_READ_MCP_EXPECTED_RELEASE_MANIFEST_SHA256",
                    ],
                },
                "production_launcher_profiles": {
                    "ordinary": {
                        "mode": "server",
                        "profile": "ordinary",
                        "subjects": "math,cs408,english",
                    },
                    "background": {
                        "mode": "client",
                        "profile": "background",
                        "subject": "required",
                    },
                    "morning_preparation": {
                        "mode": "client",
                        "profile": "morning_preparation",
                        "subject": "cs408",
                    },
                    "analysis": {
                        "mode": "subject-server",
                        "subject": "required",
                        "read_session_manifest": "required",
                        "preprocessor_root": "required",
                    },
                    "critical_review": {
                        "mode": "subject-server",
                        "subject": "required",
                        "read_session_manifest": "required",
                        "preprocessor_root": "required",
                    },
                    "infrastructure_preflight": {
                        "mode": "preflight-server",
                        "subject": "required",
                        "preflight_session_manifest": "required",
                        "preprocessor_root": "required",
                        "tool_policy": "required",
                    },
                    "authority_snapshot": {
                        "mode": "snapshot",
                        "subject": "required",
                        "source_root": "required",
                        "output_root": "required",
                    },
                },
                "subject_luna_servers": {
                    subject: {
                        "server": f"kaoyan_{subject}_read",
                        "launcher_mode": "subject-server",
                        "subject": subject,
                        "skill": f"background-{subject}-processing",
                        "read_session_schema": "study-read-mcp-read-session.v4",
                    }
                    for subject in ("math", "cs408", "english")
                },
                "subject_preflight_tools": [
                    "list_records",
                    "get_records",
                    "search_records",
                    "query_relations",
                ],
            }
            policy_payload = generator.canonical_bytes(policy)
            source_files = {
                "config/skill-tool-policy.json": generator.sha256_bytes(policy_payload),
                "scripts/sealed_launcher.py": generator.sha256_bytes(launcher_payload),
            }
            release_id = generator.sha256_bytes(generator.canonical_bytes(source_files))
            declared_release_root = Path(f"/declared/mcp/releases/{release_id}")
            release_fixture_root = fixture_root / "declared/mcp/releases" / release_id
            (release_fixture_root / "config").mkdir(parents=True)
            (release_fixture_root / "scripts").mkdir()
            (release_fixture_root / "config/skill-tool-policy.json").write_bytes(
                policy_payload
            )
            (release_fixture_root / "scripts/sealed_launcher.py").write_bytes(
                launcher_payload
            )
            manifest = {
                "schema_version": "study-read-mcp-release.v1",
                "source_files": source_files,
                "release_id": release_id,
                "formal_write_count": 0,
                "server_release": f"0.4.1+sha256.{release_id}",
            }
            manifest_payload = generator.canonical_bytes(manifest)
            (release_fixture_root / "release.json").write_bytes(manifest_payload)

            python_declared = Path("/declared/python/bin/python")
            python_fixture = fixture_root / "declared/python/bin/python"
            python_fixture.parent.mkdir(parents=True)
            python_fixture.write_bytes(b"#!/bin/sh\n")
            python_fixture.chmod(0o755)
            mcp = {
                "minimum_server_release": "0.4.1",
                "server_release": f"0.4.1+sha256.{release_id}",
                "release_root": str(declared_release_root),
                "release_id": release_id,
                "release_manifest": str(declared_release_root / "release.json"),
                "release_manifest_sha256": generator.sha256_bytes(manifest_payload),
                "python_executable": str(python_declared),
                "python_flags": ["-I", "-S"],
                "sealed_launcher_path": str(
                    declared_release_root / "scripts/sealed_launcher.py"
                ),
                "sealed_launcher_sha256": generator.sha256_bytes(launcher_payload),
            }

            with (
                mock.patch.object(generator, "ROOT", plugin_fixture),
                mock.patch.object(generator, "RUNTIME_SCHEMA_ROOT", declared_shared_root),
                mock.patch.object(generator, "REQUIRED_RUNTIME_SCHEMAS", {"fixture.json"}),
            ):
                generator.sync_runtime_schemas(
                    check=True,
                    verification_root=fixture_root,
                )
            generator.validate_external_runtime_source(
                {
                    "path": str(status_declared),
                    "sha256": generator.sha256_bytes(status_payload),
                },
                source_name="fixture_status",
                verification_paths=generator.VerificationPaths(
                    verification_root=fixture_root
                ),
            )
            generator.validate_mcp_release_contract(
                mcp,
                verification_root=fixture_root,
            )

            rendered = generator.render_kaoyan_read(mcp).decode("utf-8")
            self.assertIn(str(declared_release_root), rendered)
            self.assertNotIn(str(fixture_root), rendered)
            self.assertEqual(
                generator.sealed_runtime_binding(mcp)["release_root"],
                str(declared_release_root),
            )

    def test_plugin_mcp_release_and_policy_are_one_content_addressed_binding(self) -> None:
        plugin_root = ROOT / "plugin" / "kaoyan-study-intake"
        components = json.loads((plugin_root / "components.json").read_text())
        lock = json.loads((plugin_root / "component-lock.json").read_text())
        mcp = components["mcp"]
        manifest_path = Path(mcp["release_manifest"])
        manifest = json.loads(manifest_path.read_text())
        policy_path = manifest_path.parent / "config" / "skill-tool-policy.json"
        policy = json.loads(policy_path.read_text())
        source_files = manifest["source_files"]
        release_id = hashlib.sha256(
            (
                json.dumps(
                    source_files,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(release_id, manifest["release_id"])
        self.assertEqual(manifest["release_id"], manifest_path.parent.name)
        self.assertEqual(
            hashlib.sha256(policy_path.read_bytes()).hexdigest(),
            source_files["config/skill-tool-policy.json"],
        )
        expected_release = f"{policy['server_release']}+sha256.{release_id}"
        self.assertEqual("study-read-mcp-policy.v4", policy["schema_version"])
        self.assertEqual(mcp["minimum_server_release"], policy["server_release"])
        self.assertEqual(
            {expected_release},
            {
                manifest["server_release"],
                mcp["server_release"],
                lock["mcp_server_release"],
            },
        )
        self.assertEqual(
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            lock["mcp_release_manifest_sha256"],
        )
        expected_runtime = {
            "python_executable": mcp["python_executable"],
            "python_flags": ["-I", "-S"],
            "sealed_launcher_path": mcp["sealed_launcher_path"],
            "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
            "release_root": mcp["release_root"],
            "release_id": mcp["release_id"],
            "release_manifest_sha256": mcp["release_manifest_sha256"],
        }
        self.assertEqual(expected_runtime, lock["mcp_sealed_runtime"])
        self.assertEqual(
            hashlib.sha256(Path(mcp["sealed_launcher_path"]).read_bytes()).hexdigest(),
            mcp["sealed_launcher_sha256"],
        )
        self.assertEqual(["-I", "-S"], mcp["python_flags"])
        for subject in ("math", "cs408", "english"):
            self.assertEqual(
                {
                    "server_name": f"kaoyan_{subject}_read",
                    "launch_mode": "subject-server",
                },
                mcp["luna_servers"][subject],
            )
            self.assertNotIn("executable", lock["luna_mcp_servers"][subject])


if __name__ == "__main__":
    unittest.main()
