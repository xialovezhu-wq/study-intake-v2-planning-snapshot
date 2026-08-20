from __future__ import annotations

import contextlib
import hashlib
import hmac
import io
import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import smoke_model_driven_mcp as smoke


class ModelDrivenMcpSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.runtime = self.base / "runtime"
        self.runtime.mkdir()
        self.key_path = self.runtime / "dispatch/state/authority.key"
        self.key_path.parent.mkdir(parents=True)
        self.key = b"persistent-smoke-authority-key-32"
        self.key_path.write_bytes(self.key)
        self.key_path.chmod(0o600)
        self.plugin = self.base / "plugin"
        self.plugin.mkdir()
        self.mcp_release_id = "b" * 64
        (self.plugin / "component-lock.json").write_bytes(
            smoke.canonical_bytes(
                {
                    "schema_version": "kaoyan-study-intake-component-lock.v1",
                    "plugin_name": "kaoyan-study-intake",
                    "plugin_version": "test",
                    "mcp_release_id": self.mcp_release_id,
                    "mcp_release_manifest_sha256": "c" * 64,
                    "formal_write_count": 0,
                }
            )
        )
        self.mcp_releases = self.base / "mcp-releases"
        (self.mcp_releases / self.mcp_release_id / "src").mkdir(parents=True)
        self.bin_root = self.base / "bin"
        self.bin_root.mkdir()
        self.mcp_python = self.bin_root / "python"
        self.mcp_launchers = [
            self.bin_root / f"study-read-mcp-{subject}"
            for subject in ("math", "cs408", "english")
        ]
        self.codex = self.bin_root / "codex"
        for executable in (self.mcp_python, *self.mcp_launchers, self.codex):
            executable.write_text("test executable\n", encoding="utf-8")
            executable.chmod(0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def arguments(self, subject="math"):
        return smoke.build_parser().parse_args(
            [
                "--candidate-release-id",
                "a" * 64,
                "--runtime-root",
                str(self.runtime),
                "--plugin-root",
                str(self.plugin),
                "--mcp-releases-root",
                str(self.mcp_releases),
                "--mcp-python",
                str(self.mcp_python),
                "--codex-bin",
                str(self.codex),
                "--subject",
                subject,
            ]
        )

    def host_type(self):
        observed = self.observed

        class FakeHost:
            def __init__(inner_self, config, *, runtime_root, candidate_release_id):
                observed["config"] = dict(config)
                observed["runtime_root"] = runtime_root
                observed["candidate_release_id"] = candidate_release_id

            def open_read_session(inner_self, **values):
                observed["session"] = dict(values)
                return {"session_id": "test"}

            def read_session_manifest_path(inner_self, _context):
                path = self.runtime / "smoke-read-session.json"
                path.write_bytes(
                    smoke.canonical_bytes(
                        {
                            "schema_version": "study-read-mcp-read-session.v2",
                            "candidate_release_id": observed[
                                "candidate_release_id"
                            ],
                        }
                    )
                )
                return path

        return FakeHost

    @staticmethod
    def successful_runner(command, **_kwargs):
        subject = next(
            value
            for value in ("math", "cs408", "english")
            if any(f"kaoyan_{value}_read" in part for part in command)
        )
        server = f"kaoyan_{subject}_read"
        collection = smoke.SUBJECT_COLLECTION[subject]
        stable_id = {
            "math": "GS-001",
            "cs408": "CS408-001",
            "english": "ARTICLE-001",
        }[subject]
        output = Path(command[command.index("--output-last-message") + 1])
        output.write_bytes(
            smoke.canonical_bytes(
                {"tool_used": True, "first_stable_id": stable_id}
            )
        )
        source_artifact_count = {"math": 4, "cs408": 2, "english": 4}[subject]
        items = [
            {
                "type": "mcp_tool_call",
                "server": server,
                "tool": "get_task_context",
                "arguments": {},
                "result": {"items": [{"stable_id": f"{subject}-context"}]},
            },
            *[
                {
                    "type": "mcp_tool_call",
                    "server": server,
                    "tool": "read_task_artifact",
                    "arguments": {"artifact_id": f"artifact-{index}"},
                    "result": {"items": [{"stable_id": f"artifact-{index}"}]},
                }
                for index in range(source_artifact_count + 1)
            ],
            {
                "type": "mcp_tool_call",
                "server": server,
                "tool": "list_records",
                "arguments": {"collection": collection},
                "result": {
                    "structured_content": {
                        "items": [{"stable_id": stable_id}]
                    }
                },
            },
        ]
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=b"".join(
                smoke.canonical_bytes({"type": "item.completed", "item": item})
                for item in items
            ),
            stderr=b"",
        )

    def test_help_exits_before_smoke_execution(self) -> None:
        with (
            mock.patch.object(smoke, "run_smoke") as run_smoke,
            contextlib.redirect_stdout(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            smoke.main(["--help"])
        self.assertEqual(raised.exception.code, 0)
        run_smoke.assert_not_called()

    def test_candidate_binding_and_persistent_hmac_receipt(self) -> None:
        self.observed = {}
        summary = smoke.run_smoke(
            self.arguments(),
            command_runner=self.successful_runner,
            host_type=self.host_type(),
        )
        self.assertEqual(self.observed["candidate_release_id"], "a" * 64)
        self.assertEqual(
            self.observed["session"]["capture_id"],
            "GS-111",
        )
        receipt_path = Path(summary["receipt_path"])
        self.assertTrue(receipt_path.is_file())
        receipt_bytes = receipt_path.read_bytes()
        self.assertEqual(hashlib.sha256(receipt_bytes).hexdigest(), summary["receipt_sha256"])
        self.assertEqual(stat.S_IMODE(receipt_path.stat().st_mode), 0o400)
        receipt = json.loads(receipt_bytes)
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(receipt["candidate_release_id"], "a" * 64)
        self.assertEqual(receipt["mcp_release_id"], self.mcp_release_id)
        self.assertEqual(receipt["model_call_count"], 1)
        self.assertEqual(receipt["mcp_tool_call_count"], 7)
        self.assertEqual(
            [event["tool"] for event in receipt["mcp_events"]],
            ["get_task_context", *(["read_task_artifact"] * 5), "list_records"],
        )
        self.assertEqual(receipt["runtime_attestation"], "requested_unverified")
        self.assertEqual(receipt["formal_write_count"], 0)
        authority = receipt.pop("authority")
        expected = hmac.new(
            self.key,
            smoke.canonical_bytes(receipt),
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(authority["hmac_sha256"], expected)
        self.assertEqual(authority["key_id"], hashlib.sha256(self.key).hexdigest())
        for field in ("stdout", "stderr", "model_output"):
            artifact = Path(receipt[field]["path"])
            self.assertTrue(artifact.is_file())
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o400)
            self.assertEqual(
                hashlib.sha256(artifact.read_bytes()).hexdigest(),
                receipt[field]["sha256"],
            )

    def test_all_three_subject_namespaces_are_isolated(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                self.observed = {}
                summary = smoke.run_smoke(
                    self.arguments(subject),
                    command_runner=self.successful_runner,
                    host_type=self.host_type(),
                )
                receipt = json.loads(Path(summary["receipt_path"]).read_bytes())
                self.assertEqual(self.observed["session"]["subject"], subject)
                self.assertEqual(receipt["subject"], subject)
                self.assertEqual(receipt["server_name"], f"kaoyan_{subject}_read")
                self.assertEqual(
                    {event["server"] for event in receipt["mcp_events"]},
                    {f"kaoyan_{subject}_read"},
                )

    def test_model_cannot_invent_the_reported_library_id(self) -> None:
        self.observed = {}

        def invented_id_runner(command, **kwargs):
            completed = self.successful_runner(command, **kwargs)
            output = Path(command[command.index("--output-last-message") + 1])
            output.write_bytes(
                smoke.canonical_bytes(
                    {"tool_used": True, "first_stable_id": "INVENTED-ID"}
                )
            )
            return completed

        with self.assertRaisesRegex(
            smoke.SmokeError, "smoke_mcp_event_or_output_invalid"
        ):
            smoke.run_smoke(
                self.arguments(),
                command_runner=invented_id_runner,
                host_type=self.host_type(),
            )

    def test_cross_subject_event_is_rejected(self) -> None:
        self.observed = {}

        def cross_subject_runner(command, **kwargs):
            completed = self.successful_runner(command, **kwargs)
            events = [json.loads(line) for line in completed.stdout.splitlines()]
            events[1]["item"]["server"] = "kaoyan_cs408_read"
            completed.stdout = b"".join(
                smoke.canonical_bytes(event) for event in events
            )
            return completed

        with self.assertRaisesRegex(
            smoke.SmokeError, "smoke_mcp_event_or_output_invalid"
        ):
            smoke.run_smoke(
                self.arguments(),
                command_runner=cross_subject_runner,
                host_type=self.host_type(),
            )

    def test_model_failure_still_persists_hmac_receipt(self) -> None:
        self.observed = {}

        def failed_runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command,
                17,
                stdout=b'{"type":"turn.failed"}\n',
                stderr=b"provider rejected request",
            )

        with self.assertRaisesRegex(smoke.SmokeError, "smoke_codex_failed") as raised:
            smoke.run_smoke(
                self.arguments(),
                command_runner=failed_runner,
                host_type=self.host_type(),
            )
        receipt_path = Path(raised.exception.summary["receipt_path"])
        receipt = json.loads(receipt_path.read_bytes())
        authority = receipt.pop("authority")
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["error_code"], "smoke_codex_failed")
        self.assertEqual(receipt["returncode"], 17)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(
            authority["hmac_sha256"],
            hmac.new(
                self.key,
                smoke.canonical_bytes(receipt),
                hashlib.sha256,
            ).hexdigest(),
        )


if __name__ == "__main__":
    unittest.main()
