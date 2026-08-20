from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MCP_SOURCE = Path("/Users/xiazhibin/Documents/Codex/local-study-read-mcp")
MCP_PYTHON = MCP_SOURCE / ".venv/bin/python"
sys.path.insert(0, str(ROOT / "lib"))

from processing_plugin import ProcessingPluginHost  # noqa: E402


LUNA_CLIENT = r"""
import asyncio
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    subject = os.environ["FIXTURE_SUBJECT"]
    command = os.environ["FIXTURE_COMMAND"]
    params = StdioServerParameters(
        command=command,
        args=[
            "--stdio",
            "--read-session-manifest",
            os.environ["FIXTURE_SESSION"],
            "--preprocessor-root",
            os.environ["FIXTURE_PREPROCESSOR"],
        ],
        cwd=os.environ["FIXTURE_RELEASE_ROOT"],
        env={
            "PATH": "/usr/bin:/bin",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
            "PYTHONPATH": os.environ["FIXTURE_RELEASE_ROOT"] + "/src",
            "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT": os.environ[
                "FIXTURE_RELEASE_ROOT"
            ],
            "STUDY_READ_MCP_EXPECTED_RELEASE_ID": os.environ[
                "FIXTURE_RELEASE_ID"
            ],
            "STUDY_READ_MCP_EXPECTED_RELEASE_MANIFEST_SHA256": os.environ[
                "FIXTURE_RELEASE_MANIFEST_SHA256"
            ],
        },
    )
    async with stdio_client(params, errlog=sys.stderr) as streams:
        async with ClientSession(*streams) as client:
            await client.initialize()
            result = await client.call_tool("get_task_context", {})
            payload = result.structuredContent
            if not isinstance(payload, dict):
                raise RuntimeError("missing structured MCP response")
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


asyncio.run(main())
"""


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _schema_validation(
    schema_path: Path, value: object, base: Path
) -> subprocess.CompletedProcess[bytes]:
    instance_path = base / (schema_path.stem + "-instance.json")
    instance_path.write_bytes(_canonical_bytes(value))
    return subprocess.run(
        [
            str(MCP_PYTHON),
            "-c",
            (
                "import json,sys; from jsonschema import Draft202012Validator; "
                "s=json.load(open(sys.argv[1])); v=json.load(open(sys.argv[2])); "
                "Draft202012Validator.check_schema(s); "
                "Draft202012Validator(s).validate(v)"
            ),
            str(schema_path),
            str(instance_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=20,
    )


def _validate_schema(schema_path: Path, value: object, base: Path) -> None:
    completed = _schema_validation(schema_path, value, base)
    if completed.returncode != 0:
        raise AssertionError(completed.stderr.decode("utf-8", errors="replace"))


def _reject_schema(schema_path: Path, value: object, base: Path) -> None:
    completed = _schema_validation(schema_path, value, base)
    if completed.returncode == 0:
        raise AssertionError("invalid schema instance was accepted")


@unittest.skipUnless(
    MCP_PYTHON.is_file() and (MCP_SOURCE / "scripts/build_release.py").is_file(),
    "the local MCP source and pinned virtualenv are required",
)
class ProcessingPluginSealedEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="processing-plugin-sealed-e2e-"
        )
        self.base = Path(self.temporary.name)
        sys.path.insert(0, str(MCP_SOURCE / "src"))
        try:
            helpers = _load_module(
                "sealed_e2e_mcp_helpers", MCP_SOURCE / "tests/helpers.py"
            )
            self.repositories = helpers.make_fixture(self.base / "repositories")
        finally:
            sys.path.remove(str(MCP_SOURCE / "src"))
        builder = _load_module(
            "sealed_e2e_mcp_builder", MCP_SOURCE / "scripts/build_release.py"
        )
        self.release_result = builder.build(self.base / "mcp-releases")
        self.mcp_release = Path(self.release_result["release_dir"]).resolve()

        self.plugin_root = self.base / "kaoyan-study-intake"
        shutil.copytree(ROOT / "plugin/kaoyan-study-intake", self.plugin_root)
        components_path = self.plugin_root / "components.json"
        components = json.loads(components_path.read_text(encoding="utf-8"))
        self.lock_path = self.plugin_root / "component-lock.json"
        self.lock_path.chmod(0o600)
        lock = json.loads(self.lock_path.read_text(encoding="utf-8"))
        release_manifest_sha256 = hashlib.sha256(
            (self.mcp_release / "release.json").read_bytes()
        ).hexdigest()
        sealed_launcher = self.mcp_release / "scripts/sealed_launcher.py"
        sealed_launcher_sha256 = hashlib.sha256(
            sealed_launcher.read_bytes()
        ).hexdigest()
        components["mcp"].update(
            {
                "server_release": self.release_result["server_release"],
                "release_root": str(self.mcp_release),
                "release_id": self.release_result["release_id"],
                "release_manifest": str(self.mcp_release / "release.json"),
                "release_manifest_sha256": release_manifest_sha256,
                "python_executable": str(MCP_PYTHON),
                "python_flags": ["-I", "-S"],
                "sealed_launcher_path": str(sealed_launcher),
                "sealed_launcher_sha256": sealed_launcher_sha256,
            }
        )
        components_path.write_bytes(_canonical_bytes(components))
        generator = _load_module(
            "sealed_e2e_plugin_generator",
            ROOT / "plugin/kaoyan-study-intake/scripts/generate_manifests.py",
        )
        launcher_path = self.plugin_root / "bin/kaoyan-read"
        launcher_path.write_bytes(
            generator.render_kaoyan_read(components["mcp"])
        )
        lock["registry_sha256"] = hashlib.sha256(
            components_path.read_bytes()
        ).hexdigest()
        lock["launcher_sha256"] = hashlib.sha256(
            launcher_path.read_bytes()
        ).hexdigest()
        lock["mcp_release_root"] = str(self.mcp_release)
        lock["mcp_release_id"] = self.release_result["release_id"]
        lock["mcp_server_release"] = self.release_result["server_release"]
        lock["mcp_release_manifest_sha256"] = release_manifest_sha256
        lock["mcp_sealed_runtime"] = {
            "python_executable": str(MCP_PYTHON),
            "python_flags": ["-I", "-S"],
            "sealed_launcher_path": str(sealed_launcher),
            "sealed_launcher_sha256": sealed_launcher_sha256,
            "release_root": str(self.mcp_release),
            "release_id": self.release_result["release_id"],
            "release_manifest_sha256": release_manifest_sha256,
        }
        self.lock_path.write_bytes(_canonical_bytes(lock))
        self.lock_path.chmod(0o400)

        self.runtime = self.base / "runtime"
        key_path = self.runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True)
        key_path.write_bytes(b"s" * 32)
        key_path.chmod(0o600)
        self.host = ProcessingPluginHost(
            {
                "enabled": True,
                "root": str(self.plugin_root),
                "component_lock_path": str(self.lock_path),
                "mcp_client_python": str(MCP_PYTHON),
                "mcp_project_root": str(self.mcp_release),
                "authority_key_path": str(key_path),
                "profile": "background",
                "timeout_seconds": 20,
            },
            runtime_root=self.runtime,
            candidate_release_id="a" * 64,
            subject_roots={
                "math": self.repositories.math_root,
                "cs408": self.repositories.cs408_root,
                "english": self.repositories.english_root,
            },
            require_authority_snapshot=True,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _capture_args(self, subject: str) -> dict:
        facts = {"test_capture": True, "subject": subject}
        artifacts: list[dict] = []
        if subject == "math":
            question_raw = bytes.fromhex(
                "89504e470d0a1a0a0000000d494844520000000100000001"
                "08060000001f15c4890000000d4944415408d763f8cfc0f0"
                "1f00050001ff89993d1d0000000049454e44ae426082"
            )
            question = self.base / "question.png"
            solution = self.base / "solution.md"
            question.write_bytes(question_raw)
            solution.write_text("# 解析\n\n完整文字解析。\n", encoding="utf-8")
            dialogue = {"turns": [{"speaker": "user", "text": "第一步"}]}
            learning = {"capture_id": "GS-001", "result": "captured"}
            artifacts.extend((
                {
                    "artifact_id": "dialogue",
                    "artifact_kind": "dialogue",
                    "content": dialogue,
                    "sha256": hashlib.sha256(_canonical_bytes(dialogue)).hexdigest(),
                },
                {
                    "artifact_id": "learning-record",
                    "artifact_kind": "learning_record",
                    "content": learning,
                    "sha256": hashlib.sha256(_canonical_bytes(learning)).hexdigest(),
                },
                {
                    "artifact_id": "question-image",
                    "artifact_kind": "question_image",
                    "path": str(question),
                    "sha256": hashlib.sha256(question_raw).hexdigest(),
                },
                {
                    "artifact_id": "solution-text",
                    "artifact_kind": "solution_text",
                    "path": str(solution),
                    "sha256": hashlib.sha256(solution.read_bytes()).hexdigest(),
                },
            ))
        return {
            "capture_facts_sha256": hashlib.sha256(
                _canonical_bytes(facts)
            ).hexdigest(),
            "capture_facts": facts,
            "capture_scene": (
                "intensive_reading"
                if subject == "english"
                else "morning_review"
                if subject == "cs408"
                else "formal_problem"
            ),
            "capture_identity": {"content_fingerprint": "1" * 64},
            "capture_artifacts": tuple(artifacts),
            "captured_at": "2026-08-12T00:00:00+00:00",
        }

    def _open(self, subject: str) -> dict:
        capture_id = {
            "math": "GS-001",
            "cs408": "DS_2023_002",
            "english": "EVT-1",
        }[subject]
        return self.host.open_read_session(
            subject=subject,
            capture_id=capture_id,
            study_date="2026-08-12",
            input_fingerprint="1" * 64,
            input_binding={"capture_id": capture_id},
            **self._capture_args(subject),
            provider_schema_sha256="2" * 64,
            canonical_schema_sha256="3" * 64,
            validator_sha256="4" * 64,
        )

    def _read_frozen_context(self, subject: str, context: dict) -> dict:
        command = MCP_PYTHON.with_name(f"study-read-mcp-{subject}")
        completed = subprocess.run(
            [str(MCP_PYTHON), "-c", LUNA_CLIENT],
            cwd=self.mcp_release,
            env={
                "PATH": "/usr/bin:/bin",
                "PYTHONUTF8": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
                "PYTHONPATH": str(self.mcp_release / "src"),
                "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT": str(self.mcp_release),
                "STUDY_READ_MCP_EXPECTED_RELEASE_ID": self.release_result[
                    "release_id"
                ],
                "FIXTURE_SUBJECT": subject,
                "FIXTURE_COMMAND": str(command),
                "FIXTURE_SESSION": str(
                    self.host.read_session_manifest_path(context)
                ),
                "FIXTURE_PREPROCESSOR": str(self.runtime),
                "FIXTURE_RELEASE_ROOT": str(self.mcp_release),
                "FIXTURE_RELEASE_ID": self.release_result["release_id"],
                "FIXTURE_RELEASE_MANIFEST_SHA256": (
                    self.release_result["release_manifest_sha256"]
                ),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        return json.loads(completed.stdout)

    def test_host_client_snapshot_and_three_subject_servers_share_one_release(self) -> None:
        contexts: dict[str, dict] = {}
        session_manifests: dict[str, dict] = {}
        snapshot_manifests: dict[str, dict] = {}
        for subject in ("math", "cs408", "english"):
            context = self._open(subject)
            contexts[subject] = context
            session = context["mcp_read_session"]
            session_manifest = json.loads(
                self.host.read_session_manifest_path(context).read_text(
                    encoding="utf-8"
                )
            )
            session_manifests[subject] = session_manifest
            _validate_schema(
                ROOT / "schemas/mcp-read-session-v4.json",
                session_manifest,
                self.base,
            )
            snapshot = json.loads(
                Path(session["authority_snapshot_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            snapshot_manifests[subject] = snapshot
            _validate_schema(
                ROOT / "schemas/mcp-authority-snapshot-v2.json",
                snapshot,
                self.base,
            )
            _validate_schema(
                ROOT / "schemas/mcp-authority-snapshot-receipt-v2.json",
                context["authority_snapshot_receipt"],
                self.base,
            )
            self.assertEqual(
                session["mcp_server_release"], self.release_result["server_release"]
            )
            self.assertEqual(
                session["authority_snapshot_manifest_sha256"],
                context["authority_snapshot_receipt"][
                    "authority_snapshot_manifest_sha256"
                ],
            )

        live_events = (
            self.repositories.cs408_root
            / "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
        )
        with live_events.open("a", encoding="utf-8") as handle:
            handle.write('{"event_id":"E-LATER","event_kind":"answer"}\n')

        for subject, context in contexts.items():
            with self.subTest(subject=subject):
                envelope = self._read_frozen_context(subject, context)
                session = context["mcp_read_session"]
                self.assertTrue(envelope["ok"])
                self.assertEqual(envelope["server_release"], self.release_result["server_release"])
                self.assertEqual(envelope["generation"], session["generation"])
                self.assertEqual(
                    envelope["authority_fingerprint"],
                    session["authority_fingerprint"],
                )
                self.assertEqual(
                    envelope["read_session"]["authority_snapshot_manifest_sha256"],
                    session["authority_snapshot_manifest_sha256"],
                )
                self.assertEqual(envelope["formal_write_count"], 0)
                self.assertEqual(envelope["model_call_count"], 0)

        invalid_session = dict(session_manifests["cs408"])
        invalid_session.pop("authority_snapshot_manifest_sha256")
        _reject_schema(
            ROOT / "schemas/mcp-read-session-v4.json",
            invalid_session,
            self.base,
        )
        invalid_snapshot = dict(snapshot_manifests["cs408"])
        invalid_snapshot["formal_write_count"] = 1
        _reject_schema(
            ROOT / "schemas/mcp-authority-snapshot-v2.json",
            invalid_snapshot,
            self.base,
        )
        invalid_snapshot_receipt = dict(
            contexts["cs408"]["authority_snapshot_receipt"]
        )
        invalid_snapshot_receipt["candidate_release_id"] = "wrong"
        _reject_schema(
            ROOT / "schemas/mcp-authority-snapshot-receipt-v2.json",
            invalid_snapshot_receipt,
            self.base,
        )

        failed = self.host.sign_model_mcp_failure(
            subject="cs408",
            stage_name="cs408_critical_review_v3",
            context=contexts["cs408"],
            transport_sha256="7" * 64,
            failure_reason="cs408_critical_review_mcp_server_generation_mismatch",
            attempt_counts={
                "attempted_mcp_tool_call_count": 5,
                "successful_mcp_tool_call_count": 1,
                "grounding_mcp_tool_call_count": 1,
                "failed_mcp_tool_call_count": 4,
                "last_mcp_error_code": "GENERATION_MISMATCH",
            },
        )["receipt"]
        _validate_schema(
            ROOT / "schemas/mcp-stage-call-receipt-v2.json",
            failed,
            self.base,
        )
        invalid_failed = dict(failed)
        invalid_failed["failed_mcp_tool_call_count"] = 0
        _reject_schema(
            ROOT / "schemas/mcp-stage-call-receipt-v2.json",
            invalid_failed,
            self.base,
        )


if __name__ == "__main__":
    unittest.main()
