from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult, ImageContent

from study_read_mcp.capture import CAPTURE_ROOT_RELATIVE
from study_read_mcp.config import RepositoryConfig
from study_read_mcp.errors import StudyReadError
from study_read_mcp.release import SERVER_RELEASE
from study_read_mcp.server import build_server
from study_read_mcp.service import StudyReadService
from study_read_mcp.session import ReadSession
from study_read_mcp.snapshot import build_authority_snapshot

from .helpers import make_fixture


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def make_v2_session(base: Path, subject: str = "math") -> tuple[object, Path]:
    config = make_fixture(base)
    authority = StudyReadService(config, subjects={subject})
    try:
        snapshot = authority._adapter(subject).authority()
    finally:
        authority.close()
    freeze = config.preprocessor_root / CAPTURE_ROOT_RELATIVE
    artifact_rows = []
    artifacts = {
        "ART-IMAGE": (
            ("attachment" if subject == "english" else "question_image"),
            "image/png", "binary", "png",
            base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
            ),
            "immutable_source_attachment",
        ),
        "ART-TEXT": (
            "capture_facts", "text/plain", "utf-8", "txt",
            (("事实行\n" * 9000).encode("utf-8")),
            "immutable_capture_fact",
        ),
    }
    for artifact_id, (kind, media, encoding, extension, raw, source_role) in artifacts.items():
        digest = hashlib.sha256(raw).hexdigest()
        relative = f"artifacts/sha256/{digest[:2]}/{digest}.{extension}"
        path = freeze / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        artifact_rows.append({
            "artifact_id": artifact_id,
            "subject": subject,
            "artifact_kind": kind,
            "relative_path": relative,
            "sha256": digest,
            "byte_length": len(raw),
            "media_type": media,
            "encoding": encoding,
            "source_role": source_role,
        })
    scene = {"math": "formal_problem", "cs408": "formal_problem", "english": "intensive_reading"}[subject]
    capture = {
        "schema_version": "study-read-mcp-capture-manifest.v1",
        "capture_id": f"CAPTURE-{subject.upper()}-FIXTURE-0001",
        "subject": subject,
        "study_date": "2026-08-09",
        "scene": scene,
        "captured_at": "2026-08-09T10:00:00+08:00",
        "identity": {"content_fingerprint": "b" * 64},
        "artifacts": artifact_rows,
        "formal_write_count": 0,
    }
    capture_bytes = canonical_bytes(capture)
    capture_sha = hashlib.sha256(capture_bytes).hexdigest()
    capture_path = freeze / "manifests" / "sha256" / capture_sha[:2] / f"{capture_sha}.json"
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.write_bytes(capture_bytes)
    source_root = {
        "math": config.math_root,
        "cs408": config.cs408_root,
        "english": config.english_root,
    }[subject]
    frozen = build_authority_snapshot(
        subject, source_root, base / "authority-snapshots"
    )
    core = {
        "schema_version": "study-read-mcp-read-session.v4",
        "read_session_id": f"MCPRS-{subject.upper()}-FOCUSED-0001",
        "subject": subject,
        "candidate_release_id": "a" * 64,
        "plugin_version": "0.3.0-test",
        "skill_id": f"background-{subject}-processing",
        "skill_version": "3.0.0",
        "mcp_server_release": SERVER_RELEASE,
        "generation": snapshot.generation,
        "authority_fingerprint": snapshot.fingerprint,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "formal_write_count": 0,
        "capture_id": capture["capture_id"],
        "capture_manifest_path": str(capture_path),
        "capture_manifest_sha256": capture_sha,
        "artifact_ids": sorted(artifacts),
        "authority_snapshot_manifest_path": frozen[
            "authority_snapshot_manifest_path"
        ],
        "authority_snapshot_manifest_sha256": frozen[
            "authority_snapshot_manifest_sha256"
        ],
        "authority_snapshot_root": frozen["authority_snapshot_root"],
        "authority_snapshot_receipt_sha256": "d" * 64,
    }
    manifest = {**core, "manifest_sha256": hashlib.sha256(canonical_bytes(core)).hexdigest()}
    session_path = base / f"{subject}-read-session.json"
    session_path.write_bytes(canonical_bytes(manifest))
    os.chmod(session_path, 0o600)
    return config, session_path


class FocusedMCPTests(unittest.TestCase):
    @staticmethod
    async def _stdio_identity_and_tools(
        subject: str, session_path: Path, preprocessor_root: Path
    ) -> tuple[str, list[str]]:
        launcher = {
            "math": "math_main",
            "cs408": "cs408_main",
            "english": "english_main",
        }[subject]
        params = StdioServerParameters(
            command=sys.executable,
            args=[
                "-c",
                f"from study_read_mcp.launchers import {launcher}; {launcher}()",
                "--stdio",
                "--read-session-manifest",
                str(session_path),
                "--preprocessor-root",
                str(preprocessor_root),
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            env={
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
                "PYTHONUTF8": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
        with open(os.devnull, "w", encoding="utf-8") as errlog:
            async with stdio_client(params, errlog=errlog) as streams:
                async with ClientSession(*streams) as client:
                    initialized = await client.initialize()
                    tools = await client.list_tools()
        return initialized.serverInfo.name, [tool.name for tool in tools.tools]

    def test_three_focused_launchers_bind_explicit_isolated_preprocessor_root(
        self,
    ) -> None:
        expected_tools = [
            "get_task_context",
            "read_task_artifact",
            "list_records",
            "get_records",
            "search_records",
            "query_relations",
        ]
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject), tempfile.TemporaryDirectory() as temp:
                config, session_path = make_v2_session(Path(temp), subject)
                self.assertNotEqual(
                    config.preprocessor_root,
                    RepositoryConfig.production().preprocessor_root,
                )
                server_name, tool_names = asyncio.run(
                    self._stdio_identity_and_tools(
                        subject, session_path, config.preprocessor_root
                    )
                )
                self.assertEqual(server_name, f"kaoyan_{subject}_read")
                self.assertEqual(tool_names, expected_tools)

    def test_three_focused_launchers_require_explicit_preprocessor_root(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject), tempfile.TemporaryDirectory() as temp:
                _, session_path = make_v2_session(Path(temp), subject)
                launcher = {
                    "math": "math_main",
                    "cs408": "cs408_main",
                    "english": "english_main",
                }[subject]
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        (
                            "from study_read_mcp.launchers import "
                            f"{launcher}; {launcher}()"
                        ),
                        "--stdio",
                        "--read-session-manifest",
                        str(session_path),
                    ],
                    cwd=Path(__file__).resolve().parents[1],
                    env={
                        **os.environ,
                        "PYTHONPATH": str(
                            Path(__file__).resolve().parents[1] / "src"
                        ),
                        "PYTHONUTF8": "1",
                        "PYTHONDONTWRITEBYTECODE": "1",
                    },
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                self.assertEqual(completed.returncode, 2)
                self.assertIn(b"--preprocessor-root", completed.stderr)

    def test_three_server_identities_are_subject_only_and_library_facade_works(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject), tempfile.TemporaryDirectory() as temp:
                config, path = make_v2_session(Path(temp), subject)
                service = StudyReadService(
                    config, subjects={subject}, profile="luna",
                    read_session=ReadSession.load(path),
                )
                try:
                    server = build_server(service)
                    self.assertEqual(server.name, f"kaoyan_{subject}_read")
                    self.assertEqual(len(server._tool_manager._tools), 6)
                    collection, stable_id, query = {
                        "math": ("formal_card_catalog", "GS-001", "Fixture card"),
                        "cs408": ("formal_wrong_item_catalog", "DS_2023_002", "fixture core"),
                        "english": ("article_catalog", "RAW-ARTICLE-CORPUS", "practice-safe corpus"),
                    }[subject]
                    listed = service.list_records(subject, collection)
                    self.assertGreaterEqual(listed["total_count"], 1)
                    exact = service.get_records(subject, collection, [stable_id])
                    self.assertEqual(exact["items"][0]["stable_id"], stable_id)
                    searched = service.search_records(subject, query)
                    self.assertGreaterEqual(searched["total_count"], 1)
                    relation_id = {
                        "math": "GS-001", "cs408": "DS_2023_002", "english": "V-1"
                    }[subject]
                    relations = service.query_relations(subject, [relation_id])
                    self.assertGreaterEqual(relations["total_count"], 1)
                    self.assertTrue(
                        all(row["collection"] == "relations" for row in relations["items"])
                    )
                finally:
                    service.close()

    def test_v2_context_text_paging_and_cross_subject_fail_close(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, path = make_v2_session(Path(temp))
            service = StudyReadService(
                config, subjects={"math"}, profile="luna",
                read_session=ReadSession.load(path),
            )
            try:
                context = service.get_task_context("math")
                self.assertEqual(context["items"][0]["collection"], "task_context")
                encoded = json.dumps(context, ensure_ascii=False)
                self.assertNotIn("事实行", encoded)
                self.assertNotIn(str(config.preprocessor_root), encoded)
                first, _ = service.read_task_artifact(
                    "math", "ART-TEXT", max_bytes=4096
                )
                self.assertEqual(first["items"][0]["collection"], "task_artifact")
                self.assertEqual(first["items"][0]["stable_id"], "ART-TEXT")
                self.assertEqual(
                    first["items"][0]["source_hash"],
                    first["items"][0]["sha256"],
                )
                self.assertTrue(first["truncated"])
                second, _ = service.read_task_artifact(
                    "math", "ART-TEXT", first["next_cursor"], 4096
                )
                self.assertGreater(second["offset"], 0)
                with self.assertRaises(StudyReadError) as raised:
                    service.get_task_context("english")
                self.assertEqual(raised.exception.code, "READ_SESSION_SUBJECT_MISMATCH")
                with self.assertRaises(StudyReadError) as raised:
                    service.read_task_artifact("math", "ART-TEXT", "b3_1_deadbeefdeadbeef", 4096)
                self.assertEqual(raised.exception.code, "CURSOR_SESSION_MISMATCH")
            finally:
                service.close()

    def test_image_tool_returns_real_image_content_and_no_path_or_base64_in_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, path = make_v2_session(Path(temp))
            service = StudyReadService(
                config, subjects={"math"}, profile="luna",
                read_session=ReadSession.load(path),
            )
            try:
                server = build_server(service)
                self.assertEqual(
                    list(server._tool_manager._tools),
                    [
                        "get_task_context", "read_task_artifact", "list_records",
                        "get_records", "search_records", "query_relations",
                    ],
                )
                tool = server._tool_manager.get_tool("read_task_artifact")
                result = asyncio.run(
                    tool.run({"artifact_id": "ART-IMAGE"}, convert_result=True)
                )
                self.assertIsInstance(result, CallToolResult)
                self.assertFalse(result.isError)
                self.assertTrue(any(isinstance(item, ImageContent) for item in result.content))
                structured = json.dumps(result.structuredContent, ensure_ascii=False)
                self.assertIn('"visual_content": "mcp_image_content"', structured)
                self.assertNotIn(str(config.preprocessor_root), structured)
                self.assertNotIn("iVBOR", structured)
            finally:
                service.close()

    def test_v1_focused_tools_fail_before_returning_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            authority = StudyReadService(config, subjects={"math"})
            try:
                snapshot = authority._adapter("math").authority()
            finally:
                authority.close()
            core = {
                "schema_version": "study-read-mcp-read-session.v1",
                "read_session_id": "MCPRS-MATH-LEGACY-0001",
                "subject": "math", "candidate_release_id": "a" * 64,
                "plugin_version": "0.3.0-test", "skill_id": "background-math-processing",
                "skill_version": "3.0.0", "mcp_server_release": SERVER_RELEASE,
                "generation": snapshot.generation,
                "authority_fingerprint": snapshot.fingerprint,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "formal_write_count": 0,
            }
            node = {**core, "manifest_sha256": hashlib.sha256(canonical_bytes(core)).hexdigest()}
            path = Path(temp) / "legacy.json"
            path.write_bytes(canonical_bytes(node))
            os.chmod(path, 0o600)
            service = StudyReadService(
                config, subjects={"math"}, profile="luna", read_session=ReadSession.load(path)
            )
            try:
                with self.assertRaises(StudyReadError) as raised:
                    service.get_task_context("math")
                self.assertEqual(raised.exception.code, "CAPTURE_BINDING_REQUIRED")
            finally:
                service.close()

    def test_capture_manifest_outside_controlled_root_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, path = make_v2_session(Path(temp))
            node = json.loads(path.read_text(encoding="utf-8"))
            outside = Path(temp) / "outside.json"
            outside.write_text("{}\n", encoding="utf-8")
            outside_sha = hashlib.sha256(outside.read_bytes()).hexdigest()
            node["capture_manifest_path"] = str(outside)
            node["capture_manifest_sha256"] = outside_sha
            core = {key: value for key, value in node.items() if key != "manifest_sha256"}
            node["manifest_sha256"] = hashlib.sha256(canonical_bytes(core)).hexdigest()
            path.write_bytes(canonical_bytes(node))
            os.chmod(path, 0o600)
            with self.assertRaises(StudyReadError) as raised:
                StudyReadService(
                    config, subjects={"math"}, profile="luna",
                    read_session=ReadSession.load(path),
                )
            self.assertEqual(raised.exception.code, "CAPTURE_MANIFEST_INVALID")

    def test_read_session_symlink_is_rejected_before_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, path = make_v2_session(Path(temp))
            link = Path(temp) / "session-link.json"
            os.symlink(path, link)
            with self.assertRaises(StudyReadError) as raised:
                ReadSession.load(link)
            self.assertEqual(raised.exception.code, "READ_SESSION_UNSAFE")


if __name__ == "__main__":
    unittest.main()
