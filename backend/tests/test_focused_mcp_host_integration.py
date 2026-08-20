#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MCP_ROOT = Path("/Users/xiazhibin/Documents/Codex/local-study-read-mcp")
MCP_PYTHON = MCP_ROOT / ".venv" / "bin" / "python"
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    Candidate,
    CodexRunner,
    PreprocessorError,
    mcp_grounding_manifest,
    model_mcp_item_ref,
)


SERVER_SOURCE = r"""
import os
from pathlib import Path

from study_read_mcp.config import RepositoryConfig
from study_read_mcp.server import build_server
from study_read_mcp.service import StudyReadService
from study_read_mcp.session import ReadSession

config = RepositoryConfig(
    math_root=Path(os.environ["FIXTURE_MATH_ROOT"]),
    cs408_root=Path(os.environ["FIXTURE_CS408_ROOT"]),
    english_root=Path(os.environ["FIXTURE_ENGLISH_ROOT"]),
    preprocessor_root=Path(os.environ["FIXTURE_PREPROCESSOR_ROOT"]),
)
subject = os.environ["FIXTURE_SUBJECT"]
service = StudyReadService(
    config,
    subjects={subject},
    profile="luna",
    read_session=ReadSession.load(os.environ["FIXTURE_SESSION_PATH"]),
)
server = build_server(service)
try:
    server.run(transport="stdio")
finally:
    service.close()
"""


CLIENT_SOURCE = r"""
import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from study_read_mcp.session import ReadSession
from tests.test_focused_mcp import make_v2_session

SERVER_SOURCE = os.environ["FOCUSED_SERVER_SOURCE"]


async def collect(subject, base):
    config, session_path = make_v2_session(base, subject)
    session = ReadSession.load(session_path)
    server_env = dict(os.environ)
    server_env.update({
        "FIXTURE_MATH_ROOT": str(config.math_root),
        "FIXTURE_CS408_ROOT": str(config.cs408_root),
        "FIXTURE_ENGLISH_ROOT": str(config.english_root),
        "FIXTURE_PREPROCESSOR_ROOT": str(config.preprocessor_root),
        "FIXTURE_SUBJECT": subject,
        "FIXTURE_SESSION_PATH": str(session_path),
    })
    params = StdioServerParameters(
        command=sys.executable,
        args=["-c", SERVER_SOURCE],
        cwd=str(Path(os.environ["MCP_PROJECT_ROOT"])),
        env=server_env,
    )
    events = []
    server_name = f"kaoyan_{subject}_read"

    def append(tool, arguments, result):
        codex_result = result.model_dump(mode="json")
        if "structuredContent" in codex_result:
            codex_result["structured_content"] = codex_result.pop(
                "structuredContent"
            )
        for block in codex_result.get("content", []):
            if isinstance(block, dict) and "mimeType" in block:
                block["mime_type"] = block.pop("mimeType")
        events.append({
            "type": "item.completed",
            "item": {
                "type": "mcp_tool_call",
                "server": server_name,
                "tool": tool,
                "arguments": arguments,
                "result": codex_result,
            },
        })

    with open(os.devnull, "w", encoding="utf-8") as errlog:
        async with stdio_client(params, errlog=errlog) as streams:
            async with ClientSession(*streams) as client:
                await client.initialize()
                tools = await client.list_tools()
                tool_names = [tool.name for tool in tools.tools]
                context = await client.call_tool("get_task_context", {})
                append("get_task_context", {}, context)
                image_args = {"artifact_id": "ART-IMAGE"}
                image = await client.call_tool("read_task_artifact", image_args)
                append("read_task_artifact", image_args, image)
                cursor = None
                while True:
                    text_args = {
                        "artifact_id": "ART-TEXT",
                        "cursor": cursor,
                        "max_bytes": 4096,
                    }
                    text_page = await client.call_tool(
                        "read_task_artifact", text_args
                    )
                    append("read_task_artifact", text_args, text_page)
                    envelope = text_page.structuredContent
                    cursor = envelope["next_cursor"]
                    if envelope["complete"]:
                        break
                collection, stable_id, query, relation_id = {
                    "math": (
                        "formal_card_catalog", "GS-001", "Fixture card", "GS-001"
                    ),
                    "cs408": (
                        "formal_wrong_item_catalog", "DS_2023_002",
                        "fixture core", "DS_2023_002",
                    ),
                    "english": (
                        "article_catalog", "RAW-ARTICLE-CORPUS",
                        "practice-safe corpus", "V-1",
                    ),
                }[subject]
                library_args = {"collection": collection, "page_size": 24}
                library = await client.call_tool("list_records", library_args)
                append("list_records", library_args, library)
                exact_args = {
                    "collection": collection,
                    "ids": [stable_id],
                    "page_size": 24,
                }
                exact = await client.call_tool("get_records", exact_args)
                append("get_records", exact_args, exact)
                search_args = {"query": query, "page_size": 24}
                searched = await client.call_tool("search_records", search_args)
                append("search_records", search_args, searched)
                relation_args = {"ids": [relation_id], "page_size": 24}
                relations = await client.call_tool(
                    "query_relations", relation_args
                )
                append("query_relations", relation_args, relations)
    session_manifest = json.loads(session_path.read_text(encoding="utf-8"))
    return {
        "subject": subject,
        "server_name": server_name,
        "tool_names": tool_names,
        "session": session_manifest,
        "events": events,
    }


async def main():
    base = Path(sys.argv[1])
    output = {}
    for subject in ("math", "cs408", "english"):
        output[subject] = await collect(subject, base / subject)
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


asyncio.run(main())
"""


def _envelope_from_result(result: object) -> dict:
    if not isinstance(result, dict):
        raise AssertionError("MCP result is not an object")
    structured = result.get("structured_content", result.get("structuredContent"))
    if not isinstance(structured, dict):
        raise AssertionError("MCP result has no structuredContent envelope")
    return structured


def _event_lines(events: list[dict]) -> bytes:
    return (
        "\n".join(
            json.dumps(event, ensure_ascii=False, separators=(",", ":"))
            for event in events
        )
        + "\n"
    ).encode("utf-8")


def _mutate_result_envelopes(result: dict, mutate) -> None:
    structured = result.get("structured_content", result.get("structuredContent"))
    if isinstance(structured, dict):
        mutate(structured)
    content = result.get("content")
    if not isinstance(content, list):
        return
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "text":
            continue
        text = block.get("text")
        if not isinstance(text, str):
            continue
        try:
            envelope = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(envelope, dict) and envelope.get("schema_version") == "study-read-mcp.v3":
            mutate(envelope)
            block["text"] = json.dumps(
                envelope, ensure_ascii=False, separators=(",", ":")
            )


@unittest.skipUnless(
    MCP_PYTHON.is_file() and (MCP_ROOT / "pyproject.toml").is_file(),
    "the local focused MCP source and its pinned virtualenv are required",
)
class FocusedMcpHostIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        base = Path(cls._temporary.name)
        environment = dict(os.environ)
        environment.update({
            "PYTHONPATH": os.pathsep.join(
                [str(MCP_ROOT / "src"), str(MCP_ROOT)]
            ),
            "MCP_PROJECT_ROOT": str(MCP_ROOT),
            "FOCUSED_SERVER_SOURCE": SERVER_SOURCE,
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        })
        completed = subprocess.run(
            [str(MCP_PYTHON), "-c", CLIENT_SOURCE, str(base / "fixtures")],
            cwd=MCP_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=90,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(
                "focused MCP fixture subprocess failed:\n"
                + completed.stderr.decode("utf-8", errors="replace")[-8000:]
            )
        cls.bundle = json.loads(completed.stdout.decode("utf-8"))
        cls.runtime_root = base / "host-runtime"

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def _parse(self, subject: str, events: list[dict] | None = None):
        subject_bundle = self.bundle[subject]
        runner = CodexRunner({}, self.runtime_root / subject)
        return runner._mcp_stage_calls(
            stdout=_event_lines(events or subject_bundle["events"]),
            stage_name=f"{subject}_analysis",
            subject=subject,
            processing_context={
                "mcp_read_session": subject_bundle["session"],
            },
        )

    def test_three_real_stdio_servers_expose_only_six_focused_tools(self) -> None:
        expected = [
            "get_task_context",
            "read_task_artifact",
            "list_records",
            "get_records",
            "search_records",
            "query_relations",
        ]
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                self.assertEqual(
                    self.bundle[subject]["server_name"],
                    f"kaoyan_{subject}_read",
                )
                self.assertEqual(self.bundle[subject]["tool_names"], expected)
                launcher = MCP_ROOT / ".venv" / "bin" / f"study-read-mcp-{subject}"
                self.assertTrue(launcher.is_file())
                self.assertTrue(os.access(launcher, os.X_OK))
                help_result = subprocess.run(
                    [str(launcher), "--help"],
                    cwd=MCP_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(help_result.returncode, 0)
                help_text = help_result.stdout.decode("utf-8")
                self.assertIn("--stdio", help_text)
                self.assertIn("--read-session-manifest", help_text)

    def test_host_capture_identity_respects_mcp_manifest_field_limit(self) -> None:
        candidate = Candidate(
            subject="math",
            capture_id="CAPTURE-MATH-IDENTITY-LIMIT",
            study_date="2026-08-09",
            recorded_at="2026-08-09T10:00:00+08:00",
            input_fingerprint="a" * 64,
            input_binding={"formal_id": "F" * 200},
            model_input={},
            allowed_evidence_refs=(),
            image_paths=(),
            target_label="fixture",
            canonical_state="fixture",
            sol_state="fixture",
        )
        with self.assertRaisesRegex(
            PreprocessorError, "capture_identity_too_long"
        ):
            CodexRunner._capture_identity(candidate)

    def test_host_consumes_real_multipage_text_image_and_library_results(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                calls, transcript_sha256, transcript_ref = self._parse(subject)
                self.assertGreater(len(calls), 5)
                self.assertRegex(str(transcript_sha256), r"^[0-9a-f]{64}$")
                self.assertTrue(
                    str(transcript_ref).startswith(
                        "study-intake-mcp-stage-transcript://sha256/"
                    )
                )
                artifact_calls = [
                    call for call in calls
                    if call["tool"] == "read_task_artifact"
                ]
                text_calls = [
                    call for call in artifact_calls
                    if call["arguments"]["artifact_id"] == "ART-TEXT"
                ]
                self.assertGreater(len(text_calls), 1)
                self.assertTrue(
                    all(call["result"].get("unit") == "bytes" for call in text_calls)
                )
                self.assertEqual(text_calls[-1]["result"]["complete"], True)

    def test_every_tool_result_repeats_the_complete_v2_session_binding(self) -> None:
        public_keys = {
            "schema_version",
            "read_session_id",
            "manifest_sha256",
            "candidate_release_id",
            "plugin_version",
            "subject",
            "generation",
            "authority_fingerprint",
            "skill_id",
            "skill_version",
            "mcp_server_release",
            "capture_id",
            "capture_manifest_sha256",
            "artifact_ids",
            "formal_write_count",
            "authority_snapshot_manifest_sha256",
            "authority_snapshot_receipt_sha256",
        }
        for subject in ("math", "cs408", "english"):
            expected = self.bundle[subject]["session"]
            for event in self.bundle[subject]["events"]:
                envelope = _envelope_from_result(event["item"]["result"])
                session = envelope.get("read_session")
                with self.subTest(subject=subject, tool=event["item"]["tool"]):
                    self.assertIsInstance(session, dict)
                    self.assertEqual(set(session), public_keys)
                    for key in public_keys:
                        self.assertEqual(session.get(key), expected[key])
                    self.assertEqual(
                        envelope.get("preprocessor_release"),
                        expected["candidate_release_id"],
                    )

    def test_real_focused_envelopes_match_the_common_and_tool_specific_contracts(self) -> None:
        for subject in ("math", "cs408", "english"):
            for event in self.bundle[subject]["events"]:
                tool = event["item"]["tool"]
                arguments = event["item"]["arguments"]
                envelope = _envelope_from_result(event["item"]["result"])
                with self.subTest(subject=subject, tool=tool):
                    self.assertTrue(envelope["ok"])
                    self.assertEqual(envelope["schema_version"], "study-read-mcp.v3")
                    self.assertEqual(envelope["subject"], subject)
                    self.assertEqual(envelope["profile"], "luna")
                    self.assertEqual(envelope["formal_write_count"], 0)
                    self.assertEqual(envelope["model_call_count"], 0)
                    self.assertEqual(envelope["mcp_tool_call_count"], 1)
                    self.assertRegex(envelope["query_sha256"], r"^[0-9a-f]{64}$")
                    self.assertEqual(
                        envelope["truncated"], not envelope["complete"]
                    )
                    expected_collection = {
                        "get_task_context": "task_context",
                        "read_task_artifact": "task_artifact",
                        "search_records": "search",
                        "query_relations": "relations",
                    }.get(tool, arguments.get("collection"))
                    self.assertIsInstance(expected_collection, str)
                    self.assertTrue(envelope["items"])
                    self.assertTrue(
                        all(
                            row["collection"] == expected_collection
                            for row in envelope["items"]
                        )
                    )
                    if tool == "read_task_artifact":
                        self.assertTrue(
                            all(
                                row["stable_id"] == arguments["artifact_id"]
                                for row in envelope["items"]
                            )
                        )
                    elif tool == "get_task_context":
                        self.assertEqual(
                            envelope["items"][0]["stable_id"],
                            self.bundle[subject]["session"]["capture_id"],
                        )

    def test_host_rejects_nested_read_session_binding_drift(self) -> None:
        tampered = copy.deepcopy(self.bundle["math"]["events"])
        target = next(
            event for event in tampered
            if event["item"]["tool"] == "list_records"
        )

        def change_skill(envelope: dict) -> None:
            envelope["read_session"]["skill_version"] = "drifted-version"

        _mutate_result_envelopes(target["item"]["result"], change_skill)
        with self.assertRaises(PreprocessorError):
            self._parse("math", tampered)

    def test_task_and_artifact_grounding_use_returned_collection(self) -> None:
        calls, transcript_sha256, _ = self._parse("math")
        stage = types.SimpleNamespace(
            mcp_transcript_sha256=transcript_sha256,
            mcp_calls=calls,
        )
        manifest = mcp_grounding_manifest((stage,))
        collections = {row["collection"] for row in manifest["items"]}
        self.assertIn("task_context", collections)
        self.assertIn("task_artifact", collections)
        self.assertNotIn("", collections)

    def test_host_rejects_tool_item_collection_drift_even_when_ref_is_self_consistent(self) -> None:
        tampered = copy.deepcopy(self.bundle["math"]["events"])
        target = next(
            event for event in tampered
            if event["item"]["tool"] == "read_task_artifact"
            and event["item"]["arguments"]["artifact_id"] == "ART-TEXT"
        )

        def change_collection(envelope: dict) -> None:
            row = envelope["items"][0]
            row["collection"] = "formal_card_catalog"
            row["evidence_ref"] = model_mcp_item_ref(
                subject="math",
                generation=envelope["generation"],
                collection=row["collection"],
                stable_id=row["stable_id"],
                source_hash=row["source_hash"],
            )

        _mutate_result_envelopes(target["item"]["result"], change_collection)
        with self.assertRaises(PreprocessorError):
            self._parse("math", tampered)

    def test_image_result_contains_real_image_content_and_host_requires_it(self) -> None:
        image_event = next(
            event for event in self.bundle["math"]["events"]
            if event["item"]["tool"] == "read_task_artifact"
            and event["item"]["arguments"]["artifact_id"] == "ART-IMAGE"
        )
        content = image_event["item"]["result"]["content"]
        self.assertEqual(
            [block["type"] for block in content if block["type"] == "image"],
            ["image"],
        )
        tampered = copy.deepcopy(self.bundle["math"]["events"])
        target = next(
            event for event in tampered
            if event["item"]["tool"] == "read_task_artifact"
            and event["item"]["arguments"]["artifact_id"] == "ART-IMAGE"
        )
        target["item"]["result"]["content"] = [
            block for block in target["item"]["result"]["content"]
            if block.get("type") != "image"
        ]
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_mcp_image_content_missing"
        ):
            self._parse("math", tampered)

    def test_host_rejects_provider_query_sha_drift_within_cursor_chain(self) -> None:
        tampered = copy.deepcopy(self.bundle["math"]["events"])
        text_events = [
            event for event in tampered
            if event["item"]["tool"] == "read_task_artifact"
            and event["item"]["arguments"]["artifact_id"] == "ART-TEXT"
        ]
        self.assertGreater(len(text_events), 1)
        _mutate_result_envelopes(
            text_events[1]["item"]["result"],
            lambda envelope: envelope.__setitem__("query_sha256", "f" * 64),
        )
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_mcp_query_binding_changed"
        ):
            self._parse("math", tampered)


if __name__ == "__main__":
    unittest.main()
