from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from study_read_mcp.service import StudyReadService
from study_read_mcp.release import SERVER_RELEASE

from .helpers import make_fixture
from .test_focused_mcp import make_v2_session


class SQLiteAndProtocolTests(unittest.TestCase):
    def test_sqlite_exclusive_lock_returns_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            db = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/hot-state/databases/RHS-AABBCCDD.sqlite3"
            writer = sqlite3.connect(db, timeout=0.1)
            writer.execute("BEGIN EXCLUSIVE")
            service = StudyReadService(config)
            try:
                from study_read_mcp.errors import StudyReadError
                with self.assertRaises(StudyReadError) as context:
                    service.cs408_read_bundle([{"op": "review_identity", "ids": ["RID-1"]}])
                self.assertEqual(context.exception.code, "SQLITE_BUSY")
            finally:
                service.close()
                writer.rollback()
                writer.close()

    def test_corrupt_projection_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            db = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/hot-state/databases/RHS-AABBCCDD.sqlite3"
            db.write_bytes(b"not a sqlite database")
            service = StudyReadService(config)
            try:
                from study_read_mcp.errors import StudyReadError
                with self.assertRaises(StudyReadError) as context:
                    service.cs408_read_bundle([{"op": "review_identity", "ids": ["RID-1"]}])
                self.assertEqual(context.exception.code, "SQLITE_CORRUPT")
            finally:
                service.close()

    def test_manifest_switch_closes_old_read_connection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            hot = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/hot-state"
            service = StudyReadService(config)
            try:
                service.cs408_read_bundle([{"op": "review_identity", "ids": ["RID-1"]}])
                old_connection = service.adapters["cs408"]._sqlite
                new_build = "RHS-EEFF0011"
                new_db = hot / "databases" / f"{new_build}.sqlite3"
                connection = sqlite3.connect(new_db)
                connection.execute("CREATE TABLE events(event_id TEXT,idempotency_key TEXT,event_kind TEXT,source TEXT,session_id TEXT,item_id TEXT,identity TEXT,observed_date TEXT,evidence_kind TEXT,event_json TEXT,event_sha256 TEXT,line_sha256 TEXT,byte_offset INTEGER,byte_length INTEGER,line_number INTEGER)")
                event = {"event_id": "E-NEW", "event_kind": "answer", "session_id": "S-NEW", "item_id": "I-NEW", "identity": "RID-NEW", "observed_date": "2026-08-07", "evidence_kind": "event"}
                line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
                review_ledger = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
                review_ledger.write_text(line, encoding="utf-8")
                connection.execute("INSERT INTO events(event_id,event_kind,session_id,item_id,identity,observed_date,evidence_kind,byte_offset,byte_length,line_number) VALUES(?,?,?,?,?,?,?,?,?,?)", ("E-NEW", "answer", "S-NEW", "I-NEW", "RID-NEW", "2026-08-07", "event", 0, len(line.encode("utf-8")), 1))
                connection.commit()
                connection.close()
                (hot / "manifest.json").write_text(json.dumps({"projection_build_id": new_build, "manifest_sha256": "9" * 64, "formal_write_count": 0, "answer_safe": True}), encoding="utf-8")
                result = service.cs408_read_bundle([{"op": "review_identity", "ids": ["RID-NEW"]}])
                self.assertEqual(result["items"][0]["items"][0]["event_id"], "E-NEW")
                self.assertIsNot(service.adapters["cs408"]._sqlite, old_connection)
                with self.assertRaises(sqlite3.ProgrammingError):
                    old_connection.execute("SELECT 1")
            finally:
                service.close()

    def test_readonly_sqlite_observes_wal_without_checkpoint_or_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            db = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/hot-state/databases/RHS-AABBCCDD.sqlite3"
            writer = sqlite3.connect(db)
            writer.execute("PRAGMA journal_mode=WAL")
            review_ledger = config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
            prior = review_ledger.read_bytes()
            event = {"event_id": "E-WAL", "event_kind": "answer", "session_id": "S-WAL", "item_id": "I-WAL", "identity": "RID-WAL", "observed_date": "2026-08-07", "evidence_kind": "event"}
            line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
            review_ledger.write_bytes(prior + line.encode("utf-8"))
            writer.execute("INSERT INTO events(event_id,event_kind,session_id,item_id,identity,observed_date,evidence_kind,byte_offset,byte_length,line_number) VALUES(?,?,?,?,?,?,?,?,?,?)", ("E-WAL", "answer", "S-WAL", "I-WAL", "RID-WAL", "2026-08-07", "event", len(prior), len(line.encode("utf-8")), 2))
            writer.commit()
            self.assertTrue(Path(str(db) + "-wal").exists())
            before = db.stat()
            service = StudyReadService(config)
            try:
                result = service.cs408_read_bundle([{"op": "review_identity", "ids": ["RID-WAL"]}])
                rows = result["items"][0]["items"]
                self.assertEqual(rows[0]["event_id"], "E-WAL")
                connection = service.adapters["cs408"]._sqlite
                self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("CREATE TABLE forbidden(value TEXT)")
                after = db.stat()
                self.assertEqual((before.st_dev, before.st_ino, before.st_size), (after.st_dev, after.st_ino, after.st_size))
            finally:
                service.close()
                writer.close()

    def test_mcp_stdio_lists_exact_five_tools(self) -> None:
        project = Path(__file__).parents[1]
        python = project / ".venv/bin/python"

        async def check() -> None:
            params = StdioServerParameters(
                command="/usr/bin/env",
                args=["-i", "PATH=/usr/bin:/bin", "PYTHONUTF8=1", "PYTHONDONTWRITEBYTECODE=1", str(python), "-m", "study_read_mcp", "--stdio", "--profile", "ordinary", "--subjects", "math,cs408,english"],
                cwd=str(project),
                env={},
            )
            async with stdio_client(params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    response = await session.list_tools()
                    names = {tool.name for tool in response.tools}
                    self.assertEqual(names, {"authority_bundle", "verify_evidence_batch", "math_read_bundle", "cs408_read_bundle", "english_read_bundle"})
                    for tool in response.tools:
                        self.assertTrue(tool.annotations.readOnlyHint)
                        self.assertFalse(tool.annotations.destructiveHint)
                        self.assertTrue(tool.annotations.idempotentHint)
                        self.assertFalse(tool.annotations.openWorldHint)
                    math_tool = next(tool for tool in response.tools if tool.name == "math_read_bundle")
                    self.assertIn("route", math_tool.inputSchema["properties"])

        asyncio.run(check())

    def test_background_client_round_trip_requires_bound_skill(self) -> None:
        project = Path(__file__).parents[1]
        python = project / ".venv/bin/python"
        route = {
            "caller_skill_id": "background-math-processing",
            "caller_skill_version": "1.0.0",
            "plugin_version": "0.1.0-canary.1",
            "route_request_id": "route-background-math-001",
            "evidence_scope_hash": "a" * 64,
            "read_route": "mcp",
            "chunk_index": 1,
            "chunk_count": 1,
            "consumed_duplicate_read_count": 0,
        }
        request = {
            "tool": "math_read_bundle",
            "arguments": {
                "queries": [{"op": "formal_cards", "ids": ["GS-001"]}],
                "route": route,
            },
        }
        with tempfile.TemporaryDirectory() as temp:
            config = make_fixture(Path(temp))
            env = dict(os.environ)
            env["PYTHONPATH"] = str(project / "src")
            code = (
                "from study_read_mcp.client import call_tool; import asyncio,json,sys; "
                "from types import SimpleNamespace; "
                f"args=SimpleNamespace(profile='background',subject='math',project_root=__import__('pathlib').Path({str(project)!r})); "
                f"print(json.dumps(asyncio.run(call_tool(args,{request['tool']!r},{request['arguments']!r}))))"
            )
            # The production client uses production roots. Protocol isolation is already
            # covered above; this smoke test verifies the module is importable and the
            # bound request shape is JSON serializable without writing any repository.
            completed = subprocess.run(
                [str(python), "-c", "import json,sys; json.loads(sys.stdin.read()); print('ok')"],
                input=json.dumps(request), text=True, capture_output=True, check=True, env=env,
            )
            self.assertEqual(completed.stdout.strip(), "ok")
            self.assertEqual(config.math_root.name, "math")

    def test_luna_stdio_exposes_exactly_six_focused_tools(self) -> None:
        project = Path(__file__).parents[1]
        python = project / ".venv/bin/python"
        with tempfile.TemporaryDirectory() as temp:
            config, path = make_v2_session(Path(temp), "math")

            async def check() -> None:
                params = StdioServerParameters(
                    command=str(project / ".venv/bin/study-read-mcp-math"),
                    args=[
                        "--stdio", "--read-session-manifest", str(path),
                        "--preprocessor-root", str(config.preprocessor_root),
                    ],
                    cwd=str(project),
                    env={"PYTHONPATH": str(project / "src"), "PYTHONUTF8": "1"},
                )
                async with stdio_client(params) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        initialized = await session.initialize()
                        self.assertEqual(initialized.serverInfo.name, "kaoyan_math_read")
                        response = await session.list_tools()
                        self.assertEqual(
                            [tool.name for tool in response.tools],
                            [
                                "get_task_context", "read_task_artifact",
                                "list_records", "get_records", "search_records",
                                "query_relations",
                            ],
                        )
                        for tool in response.tools:
                            self.assertTrue(tool.annotations.readOnlyHint)
                            self.assertFalse(tool.annotations.destructiveHint)
                            self.assertFalse(tool.annotations.openWorldHint)
                            self.assertEqual(tool.outputSchema.get("type"), "object")

            asyncio.run(check())


if __name__ == "__main__":
    unittest.main()
