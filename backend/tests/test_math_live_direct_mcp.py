from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_direct_mcp import (  # noqa: E402
    DIRECT_MCP_DISPATCH_REASON,
    LIVE_CAPTURE_IDS,
    MathLiveDirectMcpError,
    build_live_math_direct_mcp_execution,
    register_live_math_direct_candidates,
)
from preprocessor_core import (  # noqa: E402
    CodexRunner,
    PreprocessorError,
    _expected_math_dynamic_schema_sha256s,
    _math_direct_mcp_schema_mode,
    load_config,
)


AUTHORITY = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
    "luna-real-business-samples.json"
)
RELEASE_ID = "2546e2d77145c211484d14bf25be2c749f43024fbb98e6f51192e052ff60c6fc"
CONFIG = (
    Path("/Users/xiazhibin/.codex/study-intake-preprocessor/releases")
    / RELEASE_ID
    / "config.json"
)


class MathLiveDirectMcpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config_root = tempfile.TemporaryDirectory(
            prefix="math-live-ordinary-mode-config-"
        )
        config_path = Path(cls.config_root.name) / "config.json"
        config_value = json.loads(CONFIG.read_text(encoding="utf-8"))
        config_value["model"].pop("service_tier", None)
        for profile_name, soft_warning in (
            ("math_deep_v2", 3600),
            ("cs408_deep_v2", 1800),
            ("english_two_pass_v1", 1800),
        ):
            profile = config_value[profile_name]
            profile.pop("stage_timeout_seconds", None)
            profile.update(
                {
                    "package_output_schema": str(
                        ROOT / "schemas/preprocess-package-v3.json"
                    ),
                    "soft_runtime_warning_seconds": soft_warning,
                    "stall_timeout_seconds": 1800,
                    "stall_probe_interval_seconds": 60,
                    "stall_probe_required_consecutive_failures": 2,
                }
            )
        config_value["processing_plugin"].update(
            {
                "root": str(ROOT / "plugin/kaoyan-study-intake"),
                "component_lock_path": str(
                    ROOT / "plugin/kaoyan-study-intake/component-lock.json"
                ),
                "mcp_project_root": (
                    "/Users/xiazhibin/.codex/local-study-read-mcp/releases/"
                    "21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425"
                ),
            }
        )
        config_path.write_text(
            json.dumps(config_value, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        cls.config = load_config(config_path)
        cls.execution = build_live_math_direct_mcp_execution(
            config=cls.config,
            release_id=RELEASE_ID,
            authority_manifest_path=AUTHORITY,
            execution_attempt=1,
            execution_runtime_id="ZERO-MODEL-MATH-LIVE-1",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.config_root.cleanup()

    def test_exact_three_candidate_bound_units_are_private_and_proposal_only(self) -> None:
        receipt = self.execution.public_receipt()
        self.assertEqual(tuple(row["capture_id"] for row in receipt["tasks"]), LIVE_CAPTURE_IDS)
        self.assertEqual(receipt["expected_model_call_count_per_task"], 2)
        self.assertEqual(receipt["stage_order"], ["analysis", "critical_review"])
        self.assertTrue(receipt["fresh_critical_review_context"])
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(len({unit.task.unit_sha256 for unit in self.execution.units}), 3)
        for unit in self.execution.units:
            candidate = unit.candidate
            public = json.dumps(
                {
                    "input_binding": candidate.input_binding,
                    "model_input": candidate.model_input,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            self.assertNotIn("/Users/", public)
            self.assertNotIn("/private/", public)
            self.assertNotIn("/var/", public)
            self.assertEqual(candidate.allowed_evidence_refs, ())
            self.assertEqual(candidate.image_paths, ())
            self.assertTrue(candidate.input_binding["proposal_only"])
            self.assertFalse(candidate.input_binding["sol_authorized"])
            self.assertEqual(candidate.input_binding["formal_write_count"], 0)
            self.assertEqual(
                unit.task.frozen_payload["dispatch_contract"]["dispatch_reason"],
                DIRECT_MCP_DISPATCH_REASON,
            )
            checked = CodexRunner._validated_private_capture_args(candidate)
            self.assertIsNotNone(checked)
            assert checked is not None
            ids = [row["artifact_id"] for row in checked["capture_artifacts"]]
            self.assertEqual(len(ids), len(set(ids)))
            self.assertEqual(
                sorted(ids),
                sorted(
                    row["artifact_id"]
                    for row in checked["capture_facts"]["facts"]["artifact_index"]
                ),
            )
        third = self.execution.units[2]
        self.assertIsNone(third.formal_id)
        self.assertEqual(
            third.candidate.input_binding["source_route"], "new_intake"
        )
        self.assertIsNone(
            third.candidate.input_binding["math_live_formal_id"]
        )

    def test_attempt_and_runtime_binding_make_distinct_units_without_changing_fingerprint(self) -> None:
        later = build_live_math_direct_mcp_execution(
            config=self.config,
            release_id=RELEASE_ID,
            authority_manifest_path=AUTHORITY,
            execution_attempt=2,
            execution_runtime_id="ZERO-MODEL-MATH-LIVE-2",
        )
        self.assertEqual(
            [unit.candidate.input_fingerprint for unit in self.execution.units],
            [unit.candidate.input_fingerprint for unit in later.units],
        )
        self.assertTrue(
            all(
                first.task.unit_sha256 != second.task.unit_sha256
                for first, second in zip(self.execution.units, later.units)
            )
        )
        with self.assertRaisesRegex(
            MathLiveDirectMcpError,
            "math_live_direct_mcp_execution_identity_invalid",
        ):
            build_live_math_direct_mcp_execution(
                config=self.config,
                release_id=RELEASE_ID,
                authority_manifest_path=AUTHORITY,
                execution_attempt=4,
                execution_runtime_id="INVALID-4",
            )

    def test_private_capture_binding_tamper_fails_closed(self) -> None:
        candidate = self.execution.units[0].candidate
        private = copy.deepcopy(candidate.private_context)
        assert private is not None
        private["processing_host_capture_args"]["capture_identity"][
            "formal_id"
        ] = "GS-507"
        tampered = copy.copy(candidate)
        object.__setattr__(tampered, "private_context", private)
        with self.assertRaisesRegex(
            PreprocessorError, "processing_host_capture_args_invalid"
        ):
            CodexRunner._validated_private_capture_args(tampered)

    def test_empty_predeclared_refs_are_limited_to_verified_direct_mcp_schema_mode(self) -> None:
        candidate = self.execution.units[2].candidate
        self.assertEqual(candidate.allowed_evidence_refs, ())
        self.assertTrue(_math_direct_mcp_schema_mode(candidate.input_binding))
        analysis_schema = ROOT / "schemas/luna-math-analysis-v2.json"
        review_schema = ROOT / "schemas/luna-math-critical-review-v3.json"

        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_evidence_refs_limit_exceeded",
        ):
            CodexRunner._bound_output_schema_bytes(
                analysis_schema,
                stage_name="math_analysis",
                allowed_evidence_refs=(),
            )

        analysis_payload, analysis_sha256 = (
            CodexRunner._bound_output_schema_bytes(
                analysis_schema,
                stage_name="math_analysis",
                allowed_evidence_refs=(),
                allow_empty_predeclared_evidence_refs=True,
            )
        )
        self.assertEqual(
            hashlib.sha256(analysis_payload).hexdigest(),
            analysis_sha256,
        )
        self.assertEqual(
            json.loads(analysis_payload)["$defs"]["evidence_ref"]["type"],
            "string",
        )

        draft = {
            "executive_summary": "只用于确定性 Schema 哈希重建。",
            "atomic_signals": [],
        }
        rebuilt = _expected_math_dynamic_schema_sha256s(
            self.config["math_deep_v2"],
            allowed_evidence_refs=(),
            image_evidence_refs=(),
            draft_analysis=draft,
            relationship_context={},
            allow_empty_predeclared_evidence_refs=True,
        )
        review_payload, review_sha256 = (
            CodexRunner._bound_output_schema_bytes(
                review_schema,
                stage_name="math_critical_review",
                allowed_evidence_refs=(),
                allowed_analysis_refs=(
                    "analysis.atomic_signals",
                    "analysis.executive_summary",
                ),
                allowed_correction_paths=(
                    "$.atomic_signals",
                    "$.executive_summary",
                ),
                allow_empty_predeclared_evidence_refs=True,
            )
        )
        self.assertEqual(
            hashlib.sha256(review_payload).hexdigest(),
            review_sha256,
        )
        self.assertEqual(
            rebuilt,
            {
                "analysis": analysis_sha256,
                "critical_review": review_sha256,
            },
        )

        with tempfile.TemporaryDirectory(prefix="math-empty-ref-gate-") as raw:
            runner = CodexRunner(self.config["model"], Path(raw))
            with mock.patch.object(runner, "_invoke_subprocess") as invoke:
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_direct_mcp_empty_evidence_ref_mode_invalid",
                ):
                    runner._execute_prompt(
                        prompt="direct MCP schema gate only",
                        output_schema=analysis_schema,
                        image_paths=(),
                        stage_name="math_analysis",
                        max_prompt_bytes=4096,
                        max_output_bytes=4096,
                        allowed_evidence_refs=(),
                        subject="math",
                        processing_context=None,
                        allow_empty_predeclared_evidence_refs=True,
                    )
                invoke.assert_not_called()

        oversized = tuple(f"capture.ref.{index}" for index in range(193))
        with self.assertRaisesRegex(
            PreprocessorError,
            "math_analysis_evidence_refs_limit_exceeded",
        ):
            CodexRunner._bound_output_schema_bytes(
                analysis_schema,
                stage_name="math_analysis",
                allowed_evidence_refs=oversized,
                allow_empty_predeclared_evidence_refs=True,
            )

    def test_real_processing_host_freezes_every_artifact_and_bootstrap_has_no_body(self) -> None:
        candidate = self.execution.units[0].candidate
        with tempfile.TemporaryDirectory(prefix="math-live-host-freeze-") as raw:
            runtime_root = Path(raw)
            key = runtime_root / "authority.key"
            key.write_bytes(b"h" * 32)
            key.chmod(0o600)
            config = copy.deepcopy(self.config)
            config["authority_release_id"] = RELEASE_ID
            config["processing_plugin"]["authority_key_path"] = str(key)
            runner = CodexRunner(config, runtime_root)
            self.assertIsNotNone(runner._processing_host)
            assert runner._processing_host is not None
            lock = json.loads(
                Path(config["processing_plugin"]["component_lock_path"])
                .read_text(encoding="utf-8")
            )
            server_release = lock["mcp_server_release"]

            context = runner._background_context(candidate)
            assert context is not None
            session = context["mcp_read_session"]
            manifest = json.loads(
                Path(session["capture_manifest_path"]).read_text(encoding="utf-8")
            )
            private = CodexRunner._validated_private_capture_args(candidate)
            assert private is not None
            self.assertEqual(
                len(manifest["artifacts"]),
                len(private["capture_artifacts"]) + 1,
            )
            self.assertEqual(
                {row["artifact_id"] for row in manifest["artifacts"]},
                {"capture-facts"}
                | {row["artifact_id"] for row in private["capture_artifacts"]},
            )
            prompt = runner._math_analysis_prompt(
                candidate, config["math_deep_v2"], context
            )
            self.assertNotIn("/Users/", prompt)
            self.assertNotIn("/private/", prompt)
            self.assertNotIn("/var/", prompt)
            for descriptor in private["capture_artifacts"]:
                path = Path(descriptor["path"])
                if path.suffix.lower() not in {".md", ".txt", ".json"}:
                    continue
                text = path.read_text(encoding="utf-8")
                snippets = [line.strip() for line in text.splitlines() if len(line.strip()) >= 24]
                if snippets:
                    self.assertNotIn(snippets[-1], prompt)
            self.assertEqual(context["processing_binding"]["mcp"]["id"], "kaoyan_math_read")
            model_config = copy.deepcopy(config["model"])
            model_config["processing_plugin"] = copy.deepcopy(
                config["processing_plugin"]
            )
            model_config["authority_release_id"] = RELEASE_ID
            model_config["subject_repo_roots"] = {
                name: adapter["repo_root"]
                for name, adapter in config["adapters"].items()
                if isinstance(adapter, dict)
                and isinstance(adapter.get("repo_root"), str)
            }
            execution_runner = CodexRunner(model_config, runtime_root)

            def fail_after_raw_freeze(_command, **kwargs):
                digest = hashlib.sha256(b"").hexdigest()
                execution_runner._provider_raw_refs[kwargs["stage_name"]] = {
                    "raw_output_object_sha256": digest,
                    "raw_output_object_ref": f"test-raw://sha256/{digest}",
                }
                return SimpleNamespace(
                    returncode=1,
                    stdout=b"",
                    stderr=b"",
                )

            with mock.patch.object(
                execution_runner,
                "_invoke_subprocess",
                side_effect=fail_after_raw_freeze,
            ) as invoke:
                with self.assertRaisesRegex(
                    PreprocessorError,
                    "math_analysis_nonzero_exit",
                ):
                    execution_runner._execute_prompt(
                        prompt="direct MCP transport boundary probe",
                        output_schema=(
                            ROOT / "schemas/luna-math-analysis-v2.json"
                        ),
                        image_paths=(),
                        stage_name="math_analysis",
                        max_prompt_bytes=4096,
                        max_output_bytes=4096,
                        allowed_evidence_refs=(),
                        subject="math",
                        processing_context=context,
                        allow_empty_predeclared_evidence_refs=True,
                    )
                invoke.assert_called_once()

    def test_registration_is_zero_model_and_exactly_once(self) -> None:
        class FakeRuntime:
            def __init__(self) -> None:
                self.calls = []

            def register_controlled_replay_candidate(
                self, task, candidate, *, reason
            ):
                self.calls.append((task, candidate, reason))

        runtime = FakeRuntime()
        register_live_math_direct_candidates(runtime, self.execution)
        self.assertEqual(len(runtime.calls), 3)
        self.assertEqual(
            [candidate.capture_id for _task, candidate, _reason in runtime.calls],
            list(LIVE_CAPTURE_IDS),
        )
        self.assertEqual(
            {reason for _task, _candidate, reason in runtime.calls},
            {DIRECT_MCP_DISPATCH_REASON},
        )


if __name__ == "__main__":
    unittest.main()
