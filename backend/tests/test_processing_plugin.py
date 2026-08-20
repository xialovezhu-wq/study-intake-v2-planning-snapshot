from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from processing_plugin import ProcessingPluginError, ProcessingPluginHost  # noqa: E402


class ProcessingPluginHostTests(unittest.TestCase):
    RELEASE_ID = "21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="processing-plugin-host-")
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.key_path = self.runtime / "dispatch/state/authority.key"
        self.key_path.parent.mkdir(parents=True)
        self.key_path.write_bytes(b"k" * 32)
        self.key_path.chmod(0o600)
        self.mcp_source_root = Path("/Users/xiazhibin/Documents/Codex/local-study-read-mcp")
        self.mcp_root = Path("/Users/xiazhibin/.codex/local-study-read-mcp/releases") / self.RELEASE_ID
        # Keep the shared component lock byte-identical.  Tests bind the
        # private 3.1.1 English Skill through an ephemeral plugin overlay.
        plugin_overlay = Path(self.temp.name) / "plugin-overlay/kaoyan-study-intake"
        shutil.copytree(ROOT / "plugin/kaoyan-study-intake", plugin_overlay)
        lock_path = plugin_overlay / "component-lock.json"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        manifest_path = self.mcp_root / "release.json"
        launcher_path = self.mcp_root / "scripts/sealed_launcher.py"
        release_manifest_sha256 = hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest()
        sealed_launcher_sha256 = hashlib.sha256(
            launcher_path.read_bytes()
        ).hexdigest()
        lock.update({
            "registry_sha256": hashlib.sha256(
                (plugin_overlay / "components.json").read_bytes()
            ).hexdigest(),
            "launcher_sha256": hashlib.sha256(
                (plugin_overlay / "bin/kaoyan-read").read_bytes()
            ).hexdigest(),
            "mcp_release_root": str(self.mcp_root),
            "mcp_release_id": self.RELEASE_ID,
            "mcp_release_manifest_sha256": release_manifest_sha256,
            "mcp_server_release": "0.4.1+sha256." + self.RELEASE_ID,
            "mcp_sealed_runtime": {
                "python_executable": str(
                    self.mcp_source_root / ".venv/bin/python"
                ),
                "python_flags": ["-I", "-S"],
                "sealed_launcher_path": str(launcher_path),
                "sealed_launcher_sha256": sealed_launcher_sha256,
                "release_root": str(self.mcp_root),
                "release_id": self.RELEASE_ID,
                "release_manifest_sha256": release_manifest_sha256,
            },
        })
        english_skill = (
            plugin_overlay
            / "skills/background-english-processing/SKILL.md"
        )
        lock["skills"]["background-english-processing"] = {
            "sha256": hashlib.sha256(english_skill.read_bytes()).hexdigest(),
            "version": "3.1.1",
        }
        lock_path.chmod(0o600)
        lock_path.write_text(
            json.dumps(
                lock,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        lock_path.chmod(0o400)
        self.config = {
            "enabled": True,
            "root": str(plugin_overlay),
            "component_lock_path": str(lock_path),
            "mcp_client_python": str(self.mcp_source_root / ".venv/bin/python"),
            "mcp_project_root": str(self.mcp_root),
            "authority_key_path": str(self.key_path),
            "profile": "background",
            "timeout_seconds": 5,
        }
        self.host = ProcessingPluginHost(
            self.config, runtime_root=self.runtime, candidate_release_id="a" * 64
        )
        self.server_release = "0.4.1+sha256." + self.RELEASE_ID

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _authority_response(self, subject: str, arguments: dict) -> dict:
        return {
            "ok": True,
            "schema_version": "study-read-mcp.v3",
            "profile": "background",
            "read_route": arguments["route"],
            "generation": "generation-1",
            "authority_fingerprint": "b" * 64,
            "server_release": self.server_release,
            "preprocessor_release": "c" * 64,
            "formal_write_count": 0,
            "model_call_count": 0,
            "items": [{
                "subject": subject,
                "available": True,
                "generation": "generation-1",
                "authority_fingerprint": "b" * 64,
                "adapter_release": self.server_release,
            }],
        }

    def _preflight_response(self, subject: str, arguments: dict) -> dict:
        return {
            "ok": True,
            "schema_version": "study-read-mcp.v3",
            "profile": "background",
            "subject": subject,
            "read_route": arguments["route"],
            "generation": "generation-1",
            "authority_fingerprint": "b" * 64,
            "adapter_release": self.server_release,
            "server_release": self.server_release,
            "formal_write_count": 0,
            "model_call_count": 0,
            "items": [{
                "operation": "curation_inventory",
                "data_role": "projection",
                "projection_event_binding": {"event_count": 1},
                "items": [],
            }],
        }

    def _open(
        self, subject: str = "math", *, capture_args: dict | None = None
    ) -> dict:
        def fake_call(call_subject, tool, arguments):
            if tool == "authority_bundle":
                return self._authority_response(call_subject, dict(arguments))
            return self._preflight_response(call_subject, dict(arguments))

        with mock.patch.object(self.host, "_call", side_effect=fake_call):
            return self.host.open_read_session(
                subject=subject,
                capture_id="GS-111" if subject == "math" else "DS_2023_002",
                study_date="2026-08-07",
                input_fingerprint="1" * 64,
                input_binding={"capture_id": "DS_2023_002"},
                **(capture_args or self._capture_binding_args(subject)),
                provider_schema_sha256="2" * 64,
                canonical_schema_sha256="3" * 64,
                validator_sha256="4" * 64,
            )

    def _capture_binding_args(self, subject: str) -> dict:
        facts = {"test_capture": True}
        raw = (
            json.dumps(
                facts,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        artifacts: tuple[dict, ...] = ()
        if subject == "math":
            question_raw = bytes.fromhex(
                "89504e470d0a1a0a0000000d494844520000000100000001"
                "08060000001f15c4890000000d4944415408d763f8cfc0f0"
                "1f00050001ff89993d1d0000000049454e44ae426082"
            )
            question = Path(self.temp.name) / "default-math-question.png"
            solution = Path(self.temp.name) / "default-math-solution.md"
            question.write_bytes(question_raw)
            solution.write_text("# 解析\n\n经校验的完整文字解析。\n", encoding="utf-8")
            dialogue = {
                "schema_version": "test-math-dialogue.v1",
                "turns": [
                    {"speaker": "user", "text": "我先给出真实第一步。"},
                    {"speaker": "assistant", "text": "只核对当前断点。"},
                ],
            }
            dialogue_raw = (
                json.dumps(
                    dialogue,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            learning_record = {
                "schema_version": "test-math-learning-record.v1",
                "capture_id": "GS-111",
                "task_kind": "formal_problem",
                "result": "captured_for_preprocessing",
            }
            learning_record_raw = (
                json.dumps(
                    learning_record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            artifacts = (
                {
                    "artifact_id": "dialogue",
                    "artifact_kind": "dialogue",
                    "content": dialogue,
                    "sha256": hashlib.sha256(dialogue_raw).hexdigest(),
                },
                {
                    "artifact_id": "learning-record",
                    "artifact_kind": "learning_record",
                    "content": learning_record,
                    "sha256": hashlib.sha256(
                        learning_record_raw
                    ).hexdigest(),
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
            )
        return {
            "capture_facts_sha256": hashlib.sha256(raw).hexdigest(),
            "capture_facts": facts,
            "capture_scene": (
                "intensive_reading"
                if subject == "english"
                else "morning_review"
                if subject == "cs408"
                else "formal_problem"
            ),
            "capture_identity": {"content_fingerprint": "1" * 64},
            "capture_artifacts": artifacts,
            "captured_at": "2026-08-07T00:00:00+00:00",
        }

    def test_english_sentence_events_artifact_is_frozen_and_reopened(self) -> None:
        evidence = {
            "schema_version": "english_legacy_target_evidence_v1",
            "target_id": "sentence_pattern_card:SP-022",
            "formal_write_count": 0,
        }
        raw = (
            json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        args = self._capture_binding_args("english")
        args["capture_artifacts"] = (
            {
                "artifact_id": "legacy-target-SP-022",
                "artifact_kind": "sentence_events",
                "content": evidence,
                "sha256": hashlib.sha256(raw).hexdigest(),
            },
        )
        context = self._open("english", capture_args=args)
        reopened = self.host.validate_read_session_context(
            subject="english", context=context
        )
        manifest = json.loads(
            Path(
                reopened["mcp_read_session"]["capture_manifest_path"]
            ).read_text(encoding="utf-8")
        )
        row = next(
            item
            for item in manifest["artifacts"]
            if item["artifact_id"] == "legacy-target-SP-022"
        )
        self.assertEqual(row["artifact_kind"], "sentence_events")
        self.assertEqual(row["source_role"], "immutable_capture_fact")
        self.assertEqual(row["media_type"], "application/json")

    @staticmethod
    def _json_digest(value) -> str:
        raw = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _model_route(session: dict) -> dict:
        return {
            "caller_skill_id": session["skill_id"],
            "caller_skill_version": session["skill_version"],
            "plugin_version": session["plugin_version"],
            "route_request_id": session["read_session_id"],
            "evidence_scope_hash": session["manifest_sha256"],
            "read_route": "mcp_model_driven",
            "read_session_id": session["read_session_id"],
            "consumed_duplicate_read_count": 0,
        }

    def _write_transcript(
        self,
        *,
        context: dict,
        stage_name: str,
        calls: list[dict],
    ) -> tuple[str, str]:
        session = context["mcp_read_session"]
        artifact = {
            "schema_version": "model-driven-mcp-stage-transcript-v1",
            "stage_name": stage_name,
            "subject": session["subject"],
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "generation": session["generation"],
            "authority_fingerprint": session["authority_fingerprint"],
            "calls": copy.deepcopy(calls),
            "coverage": {
                "call_count": len(calls),
                "all_returned_pages_consumed": True,
                "unresolved_next_cursors": [],
                "duplicate_argument_count": 0,
                "host_semantic_prefetch": False,
            },
            "semantic_stage_count": 1,
            "provider_request_count": len(calls) + 1,
            "mcp_tool_call_count": len(calls),
            "model_call_count": 1,
            "formal_write_count": 0,
        }
        raw = json.dumps(
            artifact, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        digest = hashlib.sha256(raw).hexdigest()
        path = self.host._stage_transcript_path(digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return digest, "study-intake-mcp-stage-transcript://sha256/" + digest

    def _published_read_session(
        self,
        subject: str = "math",
        *,
        english_transcript_prompt_labels: bool = False,
        english_transcript_stages_swapped: bool = False,
    ) -> tuple[dict, dict]:
        context = self._open(subject)
        session = context["mcp_read_session"]
        tool = "list_records"
        server = {
            "math": "kaoyan_math_read",
            "cs408": "kaoyan_cs408_read",
            "english": "kaoyan_english_read",
        }[subject]
        stage_receipts: dict[str, dict] = {}
        publication_rows: dict[str, dict] = {}
        for index, (stage, prompt_version) in enumerate(
            (
                ("analysis", f"{subject}_analysis_v3"),
                ("critical_review", f"{subject}_critical_review_v3"),
            ),
            start=1,
        ):
            source_hash = str(index + 4) * 64
            evidence_ref = f"mcp-item:{subject}:" + str(index + 6) * 64
            arguments = {
                "collection": "catalog",
                "page_size": 1,
                "query": None,
                "ids": [],
                "cursor": None,
            }
            result = {
                "ok": True,
                "schema_version": "study-read-mcp.v3",
                "server_release": session["mcp_server_release"],
                "adapter_release": session["mcp_server_release"],
                "subject": subject,
                "data_role": "model_selected_read_page",
                "authority_source_id": "test-authority",
                "generation": session["generation"],
                "preprocessor_release": context["processing_binding"][
                    "base_release_id"
                ],
                "authority_fingerprint": session["authority_fingerprint"],
                "captured_at": "2026-08-08T00:00:00+00:00",
                "consistency": "bound_snapshot",
                "profile": "luna",
                "read_route": self._model_route(session),
                "warnings": [],
                "items": [
                    {
                        "stable_id": f"ITEM-{index}",
                        "source_hash": source_hash,
                        "data_role": "formal_card",
                        "collection": "catalog",
                        "evidence_ref": evidence_ref,
                    }
                ],
                "formal_write_count": 0,
                "model_call_count": 0,
                "mcp_tool_call_count": 1,
                "read_session": {
                    "read_session_id": session["read_session_id"],
                    "manifest_sha256": session["manifest_sha256"],
                    "capture_id": session["capture_id"],
                    "capture_manifest_sha256": session[
                        "capture_manifest_sha256"
                    ],
                    "artifact_ids": session["artifact_ids"],
                    "candidate_release_id": session["candidate_release_id"],
                    "subject": subject,
                    "generation": session["generation"],
                    "authority_fingerprint": session[
                        "authority_fingerprint"
                    ],
                    "skill_id": session["skill_id"],
                    "skill_version": session["skill_version"],
                    "formal_write_count": 0,
                },
                "total_count": 1,
                "returned_count": 1,
                "offset": 0,
                "next_cursor": None,
                "truncated": False,
                "complete": True,
            }
            call = {
                "sequence": 1,
                "server": server,
                "tool": tool,
                "arguments": arguments,
                "arguments_sha256": self._json_digest(arguments),
                "result": result,
                "result_sha256": self._json_digest(result),
            }
            if subject != "english":
                transcript_stage_name = prompt_version
            elif english_transcript_prompt_labels:
                transcript_stage_name = prompt_version
            elif english_transcript_stages_swapped:
                transcript_stage_name = (
                    "english_critical_review"
                    if stage == "analysis"
                    else "english_analysis"
                )
            else:
                transcript_stage_name = f"english_{stage}"
            transcript_sha256, transcript_ref = self._write_transcript(
                context=context,
                stage_name=transcript_stage_name,
                calls=[call],
            )
            signed = self.host.sign_model_mcp_calls(
                subject=subject,
                stage_name=prompt_version,
                context=context,
                calls=[call],
                transcript_sha256=transcript_sha256,
            )
            grounding_core = {
                "schema_version": "model_mcp_grounding_manifest_v1",
                "items": [
                    {
                        "evidence_ref": evidence_ref,
                        "subject": subject,
                        "generation": session["generation"],
                        "collection": "catalog",
                        "stable_id": f"ITEM-{index}",
                        "source_hash": source_hash,
                        "data_role": "formal_card",
                        "consumed_in": [
                            {
                                "transcript_sha256": transcript_sha256,
                                "call_sequence": 1,
                                "result_sha256": call["result_sha256"],
                            }
                        ],
                    }
                ],
                "item_count": 1,
                "host_semantic_prefetch": False,
                "formal_write_count": 0,
            }
            grounding = {
                **grounding_core,
                "manifest_sha256": self._json_digest(grounding_core),
            }
            stage_receipt = {
                "status": "ready",
                "prompt_version": prompt_version,
                "processing_binding": copy.deepcopy(
                    context["processing_binding"]
                ),
                "processing_binding_sha256": context[
                    "processing_binding_sha256"
                ],
                "capture_freeze_receipt_sha256": context[
                    "capture_freeze_receipt_sha256"
                ],
                "capture_freeze_receipt_ref": context[
                    "capture_freeze_receipt_ref"
                ],
                "mcp_read_session_receipt_sha256": context[
                    "mcp_read_session_receipt_sha256"
                ],
                "mcp_read_session_receipt_ref": context[
                    "mcp_read_session_receipt_ref"
                ],
                "read_session_id": session["read_session_id"],
                "read_session_manifest_sha256": session["manifest_sha256"],
                "evidence_generation": session["generation"],
                "evidence_authority_fingerprint": session[
                    "authority_fingerprint"
                ],
                "mcp_transcript_sha256": transcript_sha256,
                "mcp_transcript_ref": transcript_ref,
                "mcp_call_receipt_sha256": signed["receipt_sha256"],
                "mcp_call_receipt_ref": signed["receipt_ref"],
                "pagination_coverage_complete": True,
                "mcp_grounding_manifest": grounding,
                "mcp_grounding_manifest_sha256": grounding[
                    "manifest_sha256"
                ],
                "host_semantic_prefetch": False,
                "consumed_terminal_duplicate_read_count": 0,
                "provider_request_count": 2,
                "mcp_tool_call_count": 1,
                "formal_write_count": 0,
            }
            stage_receipts[stage] = stage_receipt
            publication_rows[stage] = {
                "transcript_sha256": transcript_sha256,
                "transcript_ref": transcript_ref,
                "call_receipt_sha256": signed["receipt_sha256"],
                "call_receipt_ref": signed["receipt_ref"],
                "grounding_manifest_sha256": grounding["manifest_sha256"],
                "grounding_refs": [evidence_ref],
                "provider_request_count": 2,
                "mcp_tool_call_count": 1,
            }
        finalized = self.host.finalize_model_read_session(
            subject=subject,
            context=context,
            stage_receipts=stage_receipts,
        )
        stage_receipts["read_session"] = {
            "status": "complete",
            "receipt": copy.deepcopy(finalized["receipt"]),
            "receipt_sha256": finalized["receipt_sha256"],
            "receipt_ref": finalized["receipt_ref"],
            "model_mcp_tool_call_count": 2,
            "provider_request_count": 4,
            "provider_request_count_status": "derived_from_codex_tool_loop",
            "pagination_coverage_complete": True,
            "formal_write_count": 0,
        }
        publication = {
            "processing_binding": copy.deepcopy(context["processing_binding"]),
            "processing_binding_sha256": context[
                "processing_binding_sha256"
            ],
            "capture_freeze_receipt_sha256": context[
                "capture_freeze_receipt_sha256"
            ],
            "capture_freeze_receipt_ref": context[
                "capture_freeze_receipt_ref"
            ],
            "mcp_read_session_receipt_sha256": finalized["receipt_sha256"],
            "mcp_read_session_receipt_ref": finalized["receipt_ref"],
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "evidence_generation": session["generation"],
            "evidence_authority_fingerprint": session[
                "authority_fingerprint"
            ],
            "host_semantic_prefetch": False,
            "mcp_stage_transcripts": publication_rows,
            "semantic_stage_count": 2,
            "provider_request_count": 4,
            "mcp_tool_call_count": 2,
            "model_call_count": 2,
            "consumed_terminal_duplicate_read_count": 0,
            "stage_receipts": copy.deepcopy(stage_receipts),
        }
        return publication, stage_receipts

    def test_session_validation_reuses_authority_without_semantic_prefetch(self) -> None:
        context = self._open("math")
        with mock.patch.object(self.host, "_call", side_effect=AssertionError("validation called MCP")) as call:
            validated = self.host.validate_read_session_context(subject="math", context=context)
        self.assertEqual(call.call_count, 0)
        self.assertEqual(validated, context)
        self.assertFalse(context["processing_binding"]["host_semantic_prefetch"])
        self.assertEqual(
            context["mcp_read_session_receipt"]["host_prefetched_evidence_item_count"], 0
        )

    def test_path_free_checkpoint_context_reopens_the_private_session(self) -> None:
        for subject in ("english", "cs408"):
            with self.subTest(subject=subject):
                context = self._open(subject)
                capture_manifest_path = Path(
                    context["mcp_read_session"]["capture_manifest_path"]
                )
                checkpoint_context = copy.deepcopy(context)
                checkpoint_context["mcp_read_session"].pop(
                    "capture_manifest_path"
                )
                reopened = self.host.validate_read_session_context(
                    subject=subject, context=checkpoint_context
                )
                self.assertEqual(reopened, checkpoint_context)
                session_path = self.host.read_session_manifest_path(
                    checkpoint_context
                )
                self.assertEqual(
                    session_path,
                    self.host._read_session_path(
                        context["mcp_read_session"]["manifest_sha256"]
                    ),
                )
                reopened_private_session = json.loads(
                    session_path.read_text(encoding="utf-8")
                )
                self.assertEqual(
                    Path(reopened_private_session["capture_manifest_path"]),
                    capture_manifest_path,
                )

    def test_subject_authority_snapshot_is_zero_model_and_subject_bound(self) -> None:
        response = self._authority_response("math", {"route": {}})
        response["generation"] = "multi-envelope-generation"
        response["authority_fingerprint"] = "d" * 64

        def fake_call(subject, _tool, arguments):
            value = copy.deepcopy(response)
            value["read_route"] = arguments["route"]
            value["items"][0]["subject"] = subject
            return value

        with mock.patch.object(
            self.host,
            "_call",
            side_effect=fake_call,
        ) as call:
            value = self.host.subject_authority_snapshot("math")
        self.assertEqual(call.call_count, 1)
        self.assertEqual(value["subject"], "math")
        self.assertEqual(value["generation"], "generation-1")
        self.assertEqual(value["authority_fingerprint"], "b" * 64)
        self.assertEqual(value["model_call_count"], 0)
        self.assertEqual(value["formal_write_count"], 0)

    def test_real_host_client_and_inner_server_share_sealed_release(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                value = self.host.subject_authority_snapshot(subject)
                self.assertEqual(value["subject"], subject)
                self.assertEqual(value["mcp_server_release"], self.server_release)
                self.assertEqual(value["model_call_count"], 0)
                self.assertEqual(value["formal_write_count"], 0)

    def test_sealed_client_command_is_explicit_isolated_and_content_bound(self) -> None:
        command = self.host._sealed_launcher_command(
            mode="client",
            arguments=["--profile", "background", "--subject", "math"],
        )
        self.assertEqual(command[0], str(self.mcp_source_root / ".venv/bin/python"))
        self.assertEqual(command[1], "-I")
        self.assertEqual(command[2], "-S")
        self.assertEqual(
            command[3],
            str(self.mcp_root / "scripts/sealed_launcher.py"),
        )
        self.assertIn("--expected-release-manifest-sha256", command)
        self.assertNotIn("-m", command)
        self.assertEqual(
            self.host._sealed_launcher_environment(),
            {
                "PATH": "/usr/bin:/bin",
                "PYTHONUTF8": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
                "PYTHONSAFEPATH": "1",
            },
        )

    def test_editable_pythonpath_and_mutable_cwd_cannot_shadow_sealed_release(
        self,
    ) -> None:
        shadow_root = Path(self.temp.name) / "malicious-cwd"
        package = shadow_root / "study_read_mcp"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "raise RuntimeError('mutable source imported')\n",
            encoding="utf-8",
        )
        previous = Path.cwd()
        try:
            os.chdir(shadow_root)
            with mock.patch.dict(
                os.environ,
                {"PYTHONPATH": str(shadow_root), "PYTHONUSERBASE": str(shadow_root)},
                clear=False,
            ):
                value = self.host.subject_authority_snapshot("math")
        finally:
            os.chdir(previous)
        self.assertEqual(value["mcp_server_release"], self.server_release)
        self.assertEqual(value["model_call_count"], 0)
        self.assertEqual(value["formal_write_count"], 0)

    def test_expected_batch_authority_drift_fails_before_subject_preflight(self) -> None:
        calls: list[str] = []

        def fake_call(subject, tool, arguments):
            calls.append(tool)
            return self._authority_response(subject, dict(arguments))

        environment = {
            "STUDY_PREPROCESS_EXPECTED_BATCH_ID": "LUNA-CS408-TEST",
            "STUDY_PREPROCESS_EXPECTED_SCAN_SNAPSHOT_SHA256": "5" * 64,
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_GENERATION": "stale-generation",
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_FINGERPRINT": "b" * 64,
        }
        with mock.patch.dict(os.environ, environment, clear=False), mock.patch.object(
            self.host, "_call", side_effect=fake_call
        ):
            with self.assertRaisesRegex(
                ProcessingPluginError, "background_mcp_batch_authority_drift"
            ):
                self.host.open_read_session(
                    subject="cs408",
                    capture_id="DS_2023_002",
                    study_date="2026-08-07",
                    input_fingerprint="1" * 64,
                    input_binding={"capture_id": "DS_2023_002"},
                    **self._capture_binding_args("cs408"),
                    provider_schema_sha256="2" * 64,
                    canonical_schema_sha256="3" * 64,
                    validator_sha256="4" * 64,
                )
        self.assertEqual(calls, ["authority_bundle"])

    def test_matching_expected_batch_authority_opens_the_bound_session(self) -> None:
        environment = {
            "STUDY_PREPROCESS_EXPECTED_BATCH_ID": "LUNA-MATH-TEST",
            "STUDY_PREPROCESS_EXPECTED_SCAN_SNAPSHOT_SHA256": "5" * 64,
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_GENERATION": "generation-1",
            "STUDY_PREPROCESS_EXPECTED_AUTHORITY_FINGERPRINT": "b" * 64,
        }
        with mock.patch.dict(os.environ, environment, clear=False):
            context = self._open("math")
        self.assertEqual(
            context["mcp_read_session"]["generation"], "generation-1"
        )
        self.assertEqual(
            context["mcp_read_session"]["authority_fingerprint"], "b" * 64
        )

    def test_task_capture_manifest_binds_facts_and_image_bytes(self) -> None:
        image_raw = bytes.fromhex(
            "89504e470d0a1a0a0000000d494844520000000100000001"
            "08060000001f15c4890000000d4944415408d763f8cfc0f0"
            "1f00050001ff89993d1d0000000049454e44ae426082"
        )
        source = Path(self.temp.name) / "question.png"
        source.write_bytes(image_raw)
        source.chmod(0o600)
        capture_args = self._capture_binding_args("math")
        solution_text = next(
            row
            for row in capture_args["capture_artifacts"]
            if row["artifact_kind"] == "solution_text"
        )
        dialogue = next(
            row
            for row in capture_args["capture_artifacts"]
            if row["artifact_kind"] == "dialogue"
        )
        learning_record = next(
            row
            for row in capture_args["capture_artifacts"]
            if row["artifact_kind"] == "learning_record"
        )
        capture_args["capture_artifacts"] = (
            dialogue,
            learning_record,
            {
                "artifact_id": "question-image",
                "artifact_kind": "question_image",
                "source_role": "question_image",
                "path": str(source),
                "sha256": hashlib.sha256(image_raw).hexdigest(),
            },
            solution_text,
        )
        context = self._open("math", capture_args=capture_args)
        session = context["mcp_read_session"]
        self.assertEqual(
            session["schema_version"], "study-read-mcp-read-session.v2"
        )
        self.assertEqual(
            session["artifact_ids"],
            [
                "capture-facts",
                "dialogue",
                "learning-record",
                "question-image",
                "solution-text",
            ],
        )
        manifest_path = Path(session["capture_manifest_path"])
        self.assertEqual(manifest_path.stat().st_mode & 0o777, 0o600)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            session["capture_manifest_sha256"],
        )
        self.assertEqual(
            [row["artifact_id"] for row in manifest["artifacts"]],
            session["artifact_ids"],
        )
        image_row = next(
            row
            for row in manifest["artifacts"]
            if row["artifact_id"] == "question-image"
        )
        solution_row = next(
            row
            for row in manifest["artifacts"]
            if row["artifact_id"] == "solution-text"
        )
        frozen_image = (
            self.runtime
            / "dispatch"
            / "luna-capture-freezes"
            / image_row["relative_path"]
        )
        self.assertEqual(frozen_image.read_bytes(), image_raw)
        self.assertEqual(frozen_image.stat().st_mode & 0o777, 0o600)
        frozen_solution = (
            self.runtime
            / "dispatch"
            / "luna-capture-freezes"
            / solution_row["relative_path"]
        ).read_text(encoding="utf-8")
        self.assertIn("source_path_redacted: false", frozen_solution)
        self.assertIn(
            f"source_copy_sha256: {solution_text['sha256']}",
            frozen_solution,
        )
        self.host.validate_read_session_context(
            subject="math", context=context
        )

        original = frozen_image.read_bytes()
        try:
            frozen_image.write_bytes(original + b"tampered")
            with self.assertRaisesRegex(
                ProcessingPluginError, "capture_manifest_artifact_invalid"
            ):
                self.host.validate_read_session_context(
                    subject="math", context=context
                )
        finally:
            frozen_image.write_bytes(original)
            frozen_image.chmod(0o600)

    def test_math_capture_accepts_text_or_image_solution_and_fails_without_both(
        self,
    ) -> None:
        text_route = self._capture_binding_args("math")
        self._open("math", capture_args=text_route)

        question = next(
            row
            for row in text_route["capture_artifacts"]
            if row["artifact_kind"] == "question_image"
        )
        dialogue = next(
            row
            for row in text_route["capture_artifacts"]
            if row["artifact_kind"] == "dialogue"
        )
        learning_record = next(
            row
            for row in text_route["capture_artifacts"]
            if row["artifact_kind"] == "learning_record"
        )
        image_raw = bytes.fromhex(
            "89504e470d0a1a0a0000000d494844520000000100000001"
            "08060000001f15c4890000000d4944415408d763f8cfc0f0"
            "1f00050001ff89993d1d0000000049454e44ae426082"
        )
        solution_image = Path(self.temp.name) / "math-solution.png"
        solution_image.write_bytes(image_raw)
        image_route = self._capture_binding_args("math")
        image_route["capture_artifacts"] = (
            dialogue,
            learning_record,
            question,
            {
                "artifact_id": "solution-image",
                "artifact_kind": "solution_image",
                "path": str(solution_image),
                "sha256": hashlib.sha256(image_raw).hexdigest(),
            },
        )
        self._open("math", capture_args=image_route)

        for label, artifacts, error_code in (
            (
                "question_missing",
                tuple(
                    row
                    for row in text_route["capture_artifacts"]
                    if row["artifact_kind"] != "question_image"
                ),
                "math_capture_question_image_missing",
            ),
            (
                "solution_missing",
                (dialogue, learning_record, question),
                "math_capture_solution_evidence_missing",
            ),
            (
                "learning_record_missing",
                tuple(
                    row
                    for row in text_route["capture_artifacts"]
                    if row["artifact_kind"] != "learning_record"
                ),
                "math_capture_learning_record_missing",
            ),
            (
                "dialogue_missing",
                tuple(
                    row
                    for row in text_route["capture_artifacts"]
                    if row["artifact_kind"] != "dialogue"
                ),
                "math_capture_dialogue_missing",
            ),
        ):
            with self.subTest(label=label):
                broken = self._capture_binding_args("math")
                broken["capture_artifacts"] = artifacts
                with self.assertRaisesRegex(ProcessingPluginError, error_code):
                    self._open("math", capture_args=broken)

        inline_only = self._capture_binding_args("math")
        inline_only["capture_artifacts"] = (
            dialogue,
            learning_record,
            question,
        )
        inline_only["capture_facts"] = {
            "facts": {
                "source_bundle": {
                    "source_kind": "new_intake",
                    "manifest_hash": "a" * 64,
                    "text_sources": [
                        {
                            "role": "solution_text",
                            "sha256": "b" * 64,
                            "content": "内联解析不能替代独立 artifact。",
                        }
                    ],
                }
            }
        }
        inline_raw = (
            json.dumps(
                inline_only["capture_facts"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        inline_only["capture_facts_sha256"] = hashlib.sha256(
            inline_raw
        ).hexdigest()
        with self.assertRaisesRegex(
            ProcessingPluginError, "math_capture_solution_evidence_missing"
        ):
            self._open("math", capture_args=inline_only)

    def test_solution_text_verifies_canonical_hash_and_redacts_local_path(
        self,
    ) -> None:
        canonical = Path(self.temp.name) / "canonical-card.md"
        canonical.write_text("# 正式详情\n\n原始解析证据。\n", encoding="utf-8")
        canonical_sha256 = hashlib.sha256(canonical.read_bytes()).hexdigest()
        wrapper = Path(self.temp.name) / "bound-solution.md"
        wrapper.write_text(
            "---\n"
            "role: solution_text\n"
            f"source_path: {canonical}\n"
            'source_export: "/Users/example/private/source.oo3/"\n'
            f"source_sha256: {canonical_sha256}\n"
            "---\n\n"
            "# 解析\n\n经来源绑定的解析文字。\n\n"
            "来源备注：/Volumes/private/math/export.md\n",
            encoding="utf-8",
        )
        args = self._capture_binding_args("math")
        question = next(
            row
            for row in args["capture_artifacts"]
            if row["artifact_kind"] == "question_image"
        )
        dialogue = next(
            row
            for row in args["capture_artifacts"]
            if row["artifact_kind"] == "dialogue"
        )
        learning_record = next(
            row
            for row in args["capture_artifacts"]
            if row["artifact_kind"] == "learning_record"
        )
        args["capture_artifacts"] = (
            dialogue,
            learning_record,
            question,
            {
                "artifact_id": "solution-text",
                "artifact_kind": "solution_text",
                "path": str(wrapper),
                "sha256": hashlib.sha256(wrapper.read_bytes()).hexdigest(),
            },
        )
        context = self._open("math", capture_args=args)
        manifest = json.loads(
            Path(context["mcp_read_session"]["capture_manifest_path"])
            .read_text(encoding="utf-8")
        )
        solution_row = next(
            row
            for row in manifest["artifacts"]
            if row["artifact_kind"] == "solution_text"
        )
        frozen_path = (
            self.runtime
            / "dispatch/luna-capture-freezes"
            / solution_row["relative_path"]
        )
        frozen = frozen_path.read_text(encoding="utf-8")
        self.assertNotIn(str(canonical), frozen)
        self.assertNotIn("/Users/", frozen)
        self.assertNotIn("/Volumes/", frozen)
        self.assertEqual(frozen.count("[LOCAL_PATH_REDACTED]"), 2)
        self.assertIn("source_path_redacted: true", frozen)
        self.assertIn(f"source_sha256: {canonical_sha256}", frozen)
        self.assertIn("source_copy_sha256:", frozen)

        canonical.write_text("# 已漂移\n", encoding="utf-8")
        with self.assertRaisesRegex(
            ProcessingPluginError, "math_solution_text_source_binding_invalid"
        ):
            self._open("math", capture_args=args)

    def test_json_dialogue_redacts_only_path_metadata_and_binds_source_copy(
        self,
    ) -> None:
        dialogue_value = {
            "schema_version": "math-verbatim-dialogue-index-v1",
            "source_rollout": "/Users/example/.codex/session/private.jsonl",
            "turns": [
                {
                    "speaker": "user",
                    "file": "user_reasoning.md",
                    "text": "我先判断定义域，再检查左右两侧。",
                },
                {
                    "speaker": "assistant",
                    "text": "你的定义域判断保留，下一步只检查第一处断点。",
                },
            ],
        }
        dialogue_path = Path(self.temp.name) / "dialogue-index.json"
        dialogue_path.write_text(
            json.dumps(
                dialogue_value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        source_copy_sha256 = hashlib.sha256(dialogue_path.read_bytes()).hexdigest()
        args = self._capture_binding_args("math")
        args["capture_artifacts"] = tuple(
            row
            for row in args["capture_artifacts"]
            if row["artifact_kind"] != "dialogue"
        ) + (
            {
                "artifact_id": "dialogue",
                "artifact_kind": "dialogue",
                "path": str(dialogue_path),
                "sha256": source_copy_sha256,
            },
        )
        context = self._open("math", capture_args=args)
        manifest = json.loads(
            Path(context["mcp_read_session"]["capture_manifest_path"])
            .read_text(encoding="utf-8")
        )
        dialogue_row = next(
            row
            for row in manifest["artifacts"]
            if row["artifact_kind"] == "dialogue"
        )
        frozen_path = (
            self.runtime
            / "dispatch/luna-capture-freezes"
            / dialogue_row["relative_path"]
        )
        frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
        self.assertEqual(frozen["source_copy_sha256"], source_copy_sha256)
        self.assertEqual(
            frozen["content"]["source_rollout"], "[LOCAL_PATH_REDACTED]"
        )
        self.assertEqual(
            frozen["content"]["turns"][0]["text"],
            dialogue_value["turns"][0]["text"],
        )
        self.assertEqual(
            frozen["content"]["turns"][1]["text"],
            dialogue_value["turns"][1]["text"],
        )
        self.assertNotIn(
            "/Users/", json.dumps(frozen, ensure_ascii=False)
        )

    def test_json_learning_record_redacts_paths_and_preserves_user_fact(
        self,
    ) -> None:
        record_value = {
            "schema_version": "math-luna-business-capture-v1",
            "derived_from_frozen_capture": (
                "/Users/example/private/math-capture/HOLD-001"
            ),
            "question_identity": {
                "formal_id": "GS-TEST",
                "formal_card_path": (
                    "/Users/example/Documents/kaoyan-math/card.md"
                ),
            },
            "result": {
                "classification": "correct",
                "user_confirmation": "这道题我独立答对，未展开步骤。",
            },
        }
        record_path = Path(self.temp.name) / "record.json"
        record_path.write_text(
            json.dumps(
                record_value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        source_copy_sha256 = hashlib.sha256(
            record_path.read_bytes()
        ).hexdigest()
        args = self._capture_binding_args("math")
        args["capture_artifacts"] = tuple(
            row
            for row in args["capture_artifacts"]
            if row["artifact_kind"] != "learning_record"
        ) + (
            {
                "artifact_id": "learning-record",
                "artifact_kind": "learning_record",
                "path": str(record_path),
                "sha256": source_copy_sha256,
            },
        )
        context = self._open("math", capture_args=args)
        manifest = json.loads(
            Path(context["mcp_read_session"]["capture_manifest_path"])
            .read_text(encoding="utf-8")
        )
        record_row = next(
            row
            for row in manifest["artifacts"]
            if row["artifact_kind"] == "learning_record"
        )
        frozen_path = (
            self.runtime
            / "dispatch/luna-capture-freezes"
            / record_row["relative_path"]
        )
        frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
        self.assertEqual(frozen["source_copy_sha256"], source_copy_sha256)
        self.assertEqual(
            frozen["content"]["derived_from_frozen_capture"],
            "[LOCAL_PATH_REDACTED]",
        )
        self.assertEqual(
            frozen["content"]["question_identity"]["formal_card_path"],
            "[LOCAL_PATH_REDACTED]",
        )
        self.assertEqual(
            frozen["content"]["result"]["user_confirmation"],
            record_value["result"]["user_confirmation"],
        )
        self.assertNotIn(
            "/Users/", json.dumps(frozen, ensure_ascii=False)
        )

    def test_tampered_read_session_hmac_fails_closed(self) -> None:
        context = self._open("math")
        tampered = copy.deepcopy(context)
        tampered["mcp_read_session_receipt"]["generation"] = "generation-2"
        with self.assertRaises(ProcessingPluginError):
            self.host.validate_read_session_context(subject="math", context=tampered)

    def test_tampered_capture_freeze_hmac_fails_closed(self) -> None:
        context = self._open("math")
        tampered = copy.deepcopy(context)
        tampered["capture_freeze_receipt"]["capture_facts_sha256"] = "8" * 64
        with self.assertRaisesRegex(
            ProcessingPluginError, "capture_freeze_receipt_invalid"
        ):
            self.host.validate_read_session_context(
                subject="math", context=tampered
            )

    def test_upstream_timeout_is_mapped_and_persists_redacted_hmac_receipt(
        self,
    ) -> None:
        private_message = "PRIVATE-CARD-CONTENT-MUST-NOT-PERSIST"

        def timeout_response(subject, tool, arguments):
            self.assertEqual(subject, "math")
            self.assertEqual(tool, "authority_bundle")
            return {
                "ok": False,
                "schema_version": "study-read-mcp.v3",
                "server_release": self.server_release,
                "request_id": "opaque-timeout-request",
                "tool": tool,
                "read_route": arguments["route"],
                "error": {
                    "code": "TIMEOUT",
                    "message": private_message,
                    "retryable": True,
                },
                "formal_write_count": 0,
                "model_call_count": 0,
                "mcp_tool_call_count": 0,
            }

        with mock.patch.object(
            self.host, "_call", side_effect=timeout_response
        ), self.assertRaises(ProcessingPluginError) as raised:
            self.host.open_read_session(
                subject="math",
                capture_id="GS-111",
                study_date="2026-08-07",
                input_fingerprint="1" * 64,
                input_binding={"capture_id": "GS-111"},
                **self._capture_binding_args("math"),
                provider_schema_sha256="2" * 64,
                canonical_schema_sha256="3" * 64,
                validator_sha256="4" * 64,
            )

        exc = raised.exception
        self.assertEqual(exc.code, "background_mcp_timeout")
        digest = exc.diagnostic[
            "background_mcp_failure_receipt_sha256"
        ]
        self.assertEqual(
            exc.diagnostic["background_mcp_failure_receipt_ref"],
            "study-intake-background-mcp-failure://sha256/" + digest,
        )
        receipt = self.host.validate_background_mcp_failure_receipt(
            receipt_sha256=digest
        )
        self.assertEqual(receipt["upstream_error_code"], "TIMEOUT")
        self.assertIs(receipt["upstream_retryable"], True)
        self.assertEqual(receipt["mapped_error_code"], "background_mcp_timeout")
        self.assertEqual(receipt["subject"], "math")
        self.assertEqual(receipt["candidate_release_id"], "a" * 64)
        self.assertEqual(receipt["tool"], "authority_bundle")
        self.assertEqual(receipt["server_release"], self.server_release)
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertFalse(receipt["raw_payload_included"])
        self.assertFalse(receipt["items_included"])
        self.assertFalse(receipt["upstream_message_included"])
        serialized = json.dumps(receipt, ensure_ascii=False)
        self.assertNotIn(private_message, serialized)
        self.assertNotIn("items", receipt)

        path = self.host._background_mcp_failure_receipt_path(digest)
        original = path.read_bytes()
        tampered = copy.deepcopy(receipt)
        tampered["upstream_retryable"] = False
        try:
            path.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaisesRegex(
                ProcessingPluginError,
                "background_mcp_failure_receipt_invalid",
            ):
                self.host.validate_background_mcp_failure_receipt(
                    receipt_sha256=digest
                )
        finally:
            path.write_bytes(original)

    def test_invalid_error_envelope_is_not_authenticated(self) -> None:
        response = {
            "ok": False,
            "schema_version": "study-read-mcp.v3",
            "server_release": self.server_release,
            "request_id": "opaque-timeout-request",
            "tool": "authority_bundle",
            "read_route": {},
            "error": {
                "code": "TIMEOUT",
                "message": "invalid because the route is not bound",
                "retryable": True,
            },
            "formal_write_count": 0,
            "model_call_count": 0,
            "mcp_tool_call_count": 0,
        }
        with mock.patch.object(self.host, "_call", return_value=response):
            with self.assertRaisesRegex(
                ProcessingPluginError, "background_mcp_envelope_invalid"
            ) as raised:
                self.host.open_read_session(
                    subject="math",
                    capture_id="GS-111",
                    study_date="2026-08-07",
                    input_fingerprint="1" * 64,
                    input_binding={"capture_id": "GS-111"},
                    **self._capture_binding_args("math"),
                    provider_schema_sha256="2" * 64,
                    canonical_schema_sha256="3" * 64,
                    validator_sha256="4" * 64,
                )
        self.assertEqual(raised.exception.diagnostic, {})
        receipt_root = (
            self.runtime
            / "dispatch"
            / "background-mcp-failure-receipts"
        )
        self.assertFalse(receipt_root.exists())

    def test_stale_mcp_policy_fails_closed_before_authority_call(self) -> None:
        copied_root = Path(self.temp.name) / self.RELEASE_ID
        shutil.copytree(self.mcp_root, copied_root)
        policy_path = copied_root / "config" / "skill-tool-policy.json"
        policy_path.chmod(0o600)
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        policy["server_release"] = "0.2.0"
        policy_path.write_text(json.dumps(policy), encoding="utf-8")
        config = {**self.config, "mcp_project_root": str(copied_root)}
        host = ProcessingPluginHost(
            config,
            runtime_root=self.runtime,
            candidate_release_id="a" * 64,
        )
        try:
            with mock.patch.object(host, "_call") as call, self.assertRaisesRegex(
                ProcessingPluginError,
                "processing_component_lock_invalid",
            ):
                host.open_read_session(
                    subject="math",
                    capture_id="GS-111",
                    study_date="2026-08-07",
                    input_fingerprint="1" * 64,
                    input_binding={"capture_id": "GS-111"},
                    **self._capture_binding_args("math"),
                    provider_schema_sha256="2" * 64,
                    canonical_schema_sha256="3" * 64,
                    validator_sha256="4" * 64,
                )
            call.assert_not_called()
        finally:
            for directory, directories, files in os.walk(
                copied_root, topdown=False
            ):
                for name in files:
                    (Path(directory) / name).chmod(0o600)
                for name in directories:
                    (Path(directory) / name).chmod(0o700)
            copied_root.chmod(0o700)

    def test_cs408_runs_only_structural_preflight_and_discards_rows(self) -> None:
        calls: list[str] = []

        def fake_call(subject, tool, arguments):
            calls.append(tool)
            if tool == "authority_bundle":
                return self._authority_response(subject, dict(arguments))
            return self._preflight_response(subject, dict(arguments))

        with mock.patch.object(self.host, "_call", side_effect=fake_call):
            context = self._open("cs408")
        self.assertEqual(calls, [])
        self.assertEqual(
            context["mcp_read_session_receipt"]["host_preflight_tool_call_count"], 1
        )
        self.assertNotIn("evidence_items", context)

    def test_legacy_freeze_api_is_permanently_forbidden(self) -> None:
        with self.assertRaisesRegex(
            ProcessingPluginError, "legacy_host_semantic_prefetch_forbidden"
        ):
            self.host.freeze(
                subject="math", capture_id="GS-111", study_date="2026-08-07",
                input_fingerprint="1" * 64, input_binding={},
                provider_schema_sha256="2" * 64,
                canonical_schema_sha256="3" * 64,
                validator_sha256="4" * 64,
            )

    def test_actual_model_calls_receive_hmac_receipt(self) -> None:
        context = self._open("math")
        session = context["mcp_read_session"]
        envelope = {
            "subject": "math",
            "generation": session["generation"],
            "authority_fingerprint": session["authority_fingerprint"],
            "read_route": self._model_route(session),
            "read_session": {
                "read_session_id": session["read_session_id"],
                "manifest_sha256": session["manifest_sha256"],
                "capture_id": session["capture_id"],
                "capture_manifest_sha256": session[
                    "capture_manifest_sha256"
                ],
                "artifact_ids": session["artifact_ids"],
            },
            "total_count": 1,
            "returned_count": 1,
            "offset": 0,
            "next_cursor": None,
            "truncated": False,
            "complete": True,
            "formal_write_count": 0,
        }
        arguments = {"collection": "catalog"}
        call = {
            "sequence": 1,
            "server": "kaoyan_math_read",
            "tool": "list_records",
            "arguments": arguments,
            "arguments_sha256": self._json_digest(arguments),
            "result": envelope,
            "result_sha256": self._json_digest(envelope),
        }
        signed = self.host.sign_model_mcp_calls(
            subject="math", stage_name="math_analysis", context=context,
            calls=[call], transcript_sha256="7" * 64,
        )
        receipt = signed["receipt"]
        self.assertEqual(receipt["schema_version"], "mcp_stage_call_receipt_v2")
        self.assertEqual(receipt["provider_request_count"], 2)
        self.assertEqual(receipt["mcp_tool_call_count"], 1)
        self.assertFalse(receipt["host_semantic_prefetch"])
        self.assertEqual(receipt["formal_write_count"], 0)

    def test_failed_model_submission_receipt_is_persisted_and_hmac_verified(self) -> None:
        context = self._open("cs408")
        signed = self.host.sign_model_mcp_failure(
            subject="cs408",
            stage_name="cs408_critical_review_v3",
            context=context,
            transport_sha256="7" * 64,
            failure_reason="cs408_critical_review_invalid_json_schema",
            attempt_counts={
                "attempted_mcp_tool_call_count": 5,
                "successful_mcp_tool_call_count": 1,
                "grounding_mcp_tool_call_count": 1,
                "failed_mcp_tool_call_count": 4,
                "last_mcp_error_code": "GENERATION_MISMATCH",
            },
        )
        receipt = self.host.validate_model_mcp_call_receipt(
            subject="cs408",
            context=context,
            receipt_sha256=signed["receipt_sha256"],
            expected_stage_name="cs408_critical_review_v3",
            expected_transcript_sha256="7" * 64,
            expected_mcp_tool_call_count=0,
            expected_provider_request_count=6,
            require_success=False,
        )
        self.assertEqual(receipt["phase"], "model_stage_failed")
        self.assertFalse(receipt["pagination_coverage_complete"])
        self.assertEqual(receipt["model_call_count"], 1)
        self.assertEqual(receipt["attempted_mcp_tool_call_count"], 5)
        self.assertEqual(receipt["successful_mcp_tool_call_count"], 1)
        self.assertEqual(receipt["grounding_mcp_tool_call_count"], 1)
        self.assertEqual(receipt["failed_mcp_tool_call_count"], 4)
        self.assertEqual(receipt["last_mcp_error_code"], "GENERATION_MISMATCH")
        self.assertEqual(receipt["formal_write_count"], 0)
        tampered = dict(receipt)
        tampered["failure_reason"] = "different"
        path = self.host._model_call_receipt_path(signed["receipt_sha256"])
        original = path.read_bytes()
        try:
            path.write_text(__import__("json").dumps(tampered), encoding="utf-8")
            with self.assertRaisesRegex(
                ProcessingPluginError, "mcp_model_call_receipt_invalid"
            ):
                self.host.validate_model_mcp_call_receipt(
                    subject="cs408",
                    context=context,
                    receipt_sha256=signed["receipt_sha256"],
                    expected_stage_name="cs408_critical_review_v3",
                    expected_transcript_sha256="7" * 64,
                    expected_mcp_tool_call_count=0,
                    expected_provider_request_count=6,
                    require_success=False,
                )
        finally:
            path.write_bytes(original)

    def test_post_validation_failure_signs_parsed_calls_for_all_subjects(self) -> None:
        servers = {
            "math": "kaoyan_math_read",
            "cs408": "kaoyan_cs408_read",
            "english": "kaoyan_english_read",
        }
        for subject, server in servers.items():
            with self.subTest(subject=subject):
                context = self._open(subject)
                session = context["mcp_read_session"]
                arguments = {"collection": "catalog", "page_size": 1}
                result = {
                    "subject": subject,
                    "generation": session["generation"],
                    "authority_fingerprint": session[
                        "authority_fingerprint"
                    ],
                    "read_route": self._model_route(session),
                    "read_session": {
                        "read_session_id": session["read_session_id"],
                        "manifest_sha256": session["manifest_sha256"],
                        "capture_id": session["capture_id"],
                        "capture_manifest_sha256": session[
                            "capture_manifest_sha256"
                        ],
                        "artifact_ids": session["artifact_ids"],
                    },
                    "total_count": 1,
                    "returned_count": 1,
                    "offset": 0,
                    "next_cursor": None,
                    "truncated": False,
                    "complete": True,
                    "formal_write_count": 0,
                }
                call = {
                    "sequence": 1,
                    "server": server,
                    "tool": "list_records",
                    "arguments": arguments,
                    "arguments_sha256": self._json_digest(arguments),
                    "result": result,
                    "result_sha256": self._json_digest(result),
                }
                transcript_sha256 = hashlib.sha256(
                    f"{subject}-post-validation".encode()
                ).hexdigest()
                signed = self.host.sign_model_mcp_calls(
                    subject=subject,
                    stage_name=f"{subject}_analysis_v3",
                    context=context,
                    calls=[call],
                    transcript_sha256=transcript_sha256,
                    failure_reason=f"{subject}_analysis_grounding_invalid",
                )
                receipt = self.host.validate_model_mcp_call_receipt(
                    subject=subject,
                    context=context,
                    receipt_sha256=signed["receipt_sha256"],
                    expected_stage_name=f"{subject}_analysis_v3",
                    expected_transcript_sha256=transcript_sha256,
                    expected_mcp_tool_call_count=1,
                    expected_provider_request_count=2,
                    require_success=False,
                )
                self.assertEqual(receipt["phase"], "model_stage_failed")
                self.assertEqual(receipt["mcp_tool_call_count"], 1)
                self.assertEqual(receipt["model_call_count"], 1)
                self.assertEqual(receipt["formal_write_count"], 0)

    def test_final_read_session_receipt_aggregates_and_verifies_both_stages(self) -> None:
        context = self._open("math")
        session = context["mcp_read_session"]
        stage_receipts = {}
        for index, stage in enumerate(("analysis", "critical_review"), start=1):
            transcript_digest = ("9", "a")[index - 1] * 64
            envelope = {
                "subject": "math",
                "generation": session["generation"],
                "authority_fingerprint": session["authority_fingerprint"],
                "read_session": {
                    "read_session_id": session["read_session_id"],
                    "manifest_sha256": session["manifest_sha256"],
                    "capture_id": session["capture_id"],
                    "capture_manifest_sha256": session[
                        "capture_manifest_sha256"
                    ],
                    "artifact_ids": session["artifact_ids"],
                },
                "read_route": {
                    "caller_skill_id": session["skill_id"],
                    "caller_skill_version": session["skill_version"],
                    "plugin_version": session["plugin_version"],
                    "route_request_id": session["read_session_id"],
                    "evidence_scope_hash": session["manifest_sha256"],
                    "read_route": "mcp_model_driven",
                    "read_session_id": session["read_session_id"],
                    "consumed_duplicate_read_count": 0,
                },
                "total_count": 1,
                "returned_count": 1,
                "offset": 0,
                "next_cursor": None,
                "truncated": False,
                "complete": True,
                "formal_write_count": 0,
            }
            arguments = {"collection": "catalog", "stage": stage}
            call = {
                "sequence": 1,
                "server": "kaoyan_math_read",
                "tool": "list_records",
                "arguments": arguments,
                "arguments_sha256": self._json_digest(arguments),
                "result": envelope,
                "result_sha256": self._json_digest(envelope),
            }
            signed = self.host.sign_model_mcp_calls(
                subject="math",
                stage_name=stage,
                context=context,
                calls=[call],
                transcript_sha256=transcript_digest,
            )
            stage_receipts[stage] = {
                "status": "ready",
                "read_session_id": session["read_session_id"],
                "read_session_manifest_sha256": session["manifest_sha256"],
                "mcp_call_receipt_sha256": signed["receipt_sha256"],
                "mcp_transcript_sha256": transcript_digest,
                "mcp_tool_call_count": 1,
                "provider_request_count": 2,
                "pagination_coverage_complete": True,
                "formal_write_count": 0,
            }
        finalized = self.host.finalize_model_read_session(
            subject="math",
            context=context,
            stage_receipts=stage_receipts,
        )
        receipt = self.host.validate_final_model_read_session(
            subject="math", context=context, finalized=finalized
        )
        self.assertEqual(receipt["model_mcp_tool_call_count"], 2)
        self.assertEqual(receipt["provider_request_count"], 4)
        self.assertTrue(receipt["pagination_coverage_complete"])
        self.assertEqual(receipt["formal_write_count"], 0)
        tampered = copy.deepcopy(finalized)
        tampered["receipt"]["provider_request_count"] = 5
        with self.assertRaisesRegex(
            ProcessingPluginError, "mcp_read_session_final_receipt_invalid"
        ):
            self.host.validate_final_model_read_session(
                subject="math", context=context, finalized=tampered
            )

    def test_published_read_session_reopens_only_persisted_calls(self) -> None:
        publication, stage_receipts = self._published_read_session("math")
        reopened = self.host.reopen_published_read_session(
            subject="math",
            publication=publication,
            stage_receipts=stage_receipts,
        )
        self.assertEqual(
            set(reopened),
            {"context", "stage_receipts", "transcripts", "stage_calls"},
        )
        self.assertEqual(
            set(reopened["stage_calls"]), {"analysis", "critical_review"}
        )
        for stage in ("analysis", "critical_review"):
            calls = reopened["stage_calls"][stage]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls, reopened["transcripts"][stage]["calls"])
            self.assertIn("result", calls[0])
            self.assertEqual(calls[0]["server"], "kaoyan_math_read")
        self.assertEqual(reopened["stage_receipts"], stage_receipts)
        self.assertEqual(
            reopened["context"]["mcp_read_session"]["read_session_id"],
            publication["read_session_id"],
        )

    def test_english_published_read_session_keeps_stage_and_prompt_names_distinct(self) -> None:
        publication, stage_receipts = self._published_read_session("english")
        reopened = self.host.reopen_published_read_session(
            subject="english",
            publication=publication,
            stage_receipts=stage_receipts,
        )
        for stage in ("analysis", "critical_review"):
            self.assertEqual(
                reopened["transcripts"][stage]["stage_name"],
                f"english_{stage}",
            )
            self.assertNotEqual(
                reopened["transcripts"][stage]["stage_name"],
                stage_receipts[stage]["prompt_version"],
            )
            persisted = self.host.validate_persisted_model_mcp_stage(
                subject="english",
                context=reopened["context"],
                stage=stage,
                stage_receipt=stage_receipts[stage],
            )
            self.assertEqual(
                persisted["transcript"], reopened["transcripts"][stage]
            )
            self.assertEqual(
                persisted["call_receipt"]["transcript_sha256"],
                stage_receipts[stage]["mcp_transcript_sha256"],
            )

        invalid_publication, invalid_receipts = self._published_read_session(
            "english",
            english_transcript_prompt_labels=True,
        )
        with self.assertRaisesRegex(
            ProcessingPluginError,
            "mcp_stage_transcript_invalid",
        ):
            self.host.reopen_published_read_session(
                subject="english",
                publication=invalid_publication,
                stage_receipts=invalid_receipts,
            )

    def test_english_published_read_session_rejects_swapped_semantic_stages(self) -> None:
        publication, stage_receipts = self._published_read_session(
            "english",
            english_transcript_stages_swapped=True,
        )
        with self.assertRaisesRegex(
            ProcessingPluginError,
            "mcp_stage_transcript_invalid",
        ):
            self.host.reopen_published_read_session(
                subject="english",
                publication=publication,
                stage_receipts=stage_receipts,
            )

    def test_published_read_session_rejects_persisted_artifact_tampering(self) -> None:
        mutation_cases = (
            ("capture", "capture_freeze_receipt_sha256"),
            ("opened", None),
            ("manifest", "read_session_manifest_sha256"),
            ("transcript", None),
            ("final", "mcp_read_session_receipt_sha256"),
        )
        for label, publication_key in mutation_cases:
            with self.subTest(label=label):
                publication, stage_receipts = self._published_read_session("math")
                if label == "capture":
                    digest = publication[str(publication_key)]
                    path = self.host._capture_freeze_receipt_path(digest)
                elif label == "opened":
                    digest = stage_receipts["analysis"][
                        "mcp_read_session_receipt_sha256"
                    ]
                    path = self.host._read_session_receipt_path(digest)
                elif label == "manifest":
                    digest = publication[str(publication_key)]
                    path = self.host._read_session_path(digest)
                elif label == "transcript":
                    digest = publication["mcp_stage_transcripts"]["analysis"][
                        "transcript_sha256"
                    ]
                    path = self.host._stage_transcript_path(digest)
                else:
                    digest = publication[str(publication_key)]
                    path = self.host._read_session_receipt_path(digest)
                original = path.read_bytes()
                try:
                    value = json.loads(original)
                    value["formal_write_count"] = 1
                    path.write_text(json.dumps(value), encoding="utf-8")
                    with self.assertRaises(ProcessingPluginError):
                        self.host.reopen_published_read_session(
                            subject="math",
                            publication=publication,
                            stage_receipts=stage_receipts,
                        )
                finally:
                    path.write_bytes(original)

    def test_published_read_session_rejects_subject_session_and_release_drift(self) -> None:
        publication, stage_receipts = self._published_read_session("math")
        with self.subTest(drift="subject"):
            with self.assertRaises(ProcessingPluginError):
                self.host.reopen_published_read_session(
                    subject="english",
                    publication=publication,
                    stage_receipts=stage_receipts,
                )
        with self.subTest(drift="session"):
            changed = copy.deepcopy(publication)
            changed["read_session_id"] = "MCPRS-MATH-DIFFERENT"
            changed.pop("stage_receipts")
            with self.assertRaisesRegex(
                ProcessingPluginError, "mcp_published_read_session_invalid"
            ):
                self.host.reopen_published_read_session(
                    subject="math",
                    publication=changed,
                    stage_receipts=stage_receipts,
                )
        with self.subTest(drift="candidate_release"):
            drifted_host = ProcessingPluginHost(
                self.config,
                runtime_root=self.runtime,
                candidate_release_id="e" * 64,
            )
            with self.assertRaisesRegex(
                ProcessingPluginError, "mcp_read_session_binding_invalid"
            ):
                drifted_host.reopen_published_read_session(
                    subject="math",
                    publication=publication,
                    stage_receipts=stage_receipts,
                )

    def test_published_read_session_requires_exact_three_stage_closure(self) -> None:
        publication, stage_receipts = self._published_read_session("math")
        missing = copy.deepcopy(stage_receipts)
        missing.pop("critical_review")
        publication.pop("stage_receipts")
        with self.assertRaisesRegex(
            ProcessingPluginError, "mcp_published_read_session_invalid"
        ):
            self.host.reopen_published_read_session(
                subject="math", publication=publication, stage_receipts=missing
            )
        extra = copy.deepcopy(stage_receipts)
        extra["legacy"] = {}
        with self.assertRaisesRegex(
            ProcessingPluginError, "mcp_published_read_session_invalid"
        ):
            self.host.reopen_published_read_session(
                subject="math", publication=publication, stage_receipts=extra
            )

    def test_processing_skills_require_model_driven_full_library_mcp(self) -> None:
        for subject in ("math", "cs408", "english"):
            skill = (
                ROOT / f"plugin/kaoyan-study-intake/skills/background-{subject}-processing/SKILL.md"
            ).read_text(encoding="utf-8")
            self.assertIn("model-driven MCP", skill)
            self.assertIn("next_cursor", skill)
            self.assertIn("formal_write_count=0", skill)


if __name__ == "__main__":
    unittest.main()
