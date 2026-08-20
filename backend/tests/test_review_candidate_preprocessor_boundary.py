from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import CodexRunner, PreprocessorError  # noqa: E402


class ReviewCandidatePreprocessorBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.codex = self.root / "codex"
        self.codex.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        self.codex.chmod(0o700)
        self.schema = self.root / "output-schema.json"
        self.schema.write_text(
            json.dumps(
                {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        self.runner = CodexRunner(
            {
                "codex_path": str(self.codex),
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            self.runtime,
        )
        self.stage_name = "math_analysis"
        self.raw_digest = "1" * 64
        self.raw_ref = (
            "study-intake-model-stage-raw-output://sha256/"
            + self.raw_digest
        )
        self.execution_digest = "2" * 64
        self.execution_ref = (
            "study-intake-model-stage-execution://sha256/"
            + self.execution_digest
        )
        self.transcript_digest = "3" * 64
        self.transcript_ref = (
            "study-intake-mcp-stage-transcript://sha256/"
            + self.transcript_digest
        )
        self.normalization_digest = "4" * 64
        self.normalization_ref = (
            "study-intake-model-stage-normalization://sha256/"
            + self.normalization_digest
        )
        self.transport_digest = "5" * 64
        self.processing_context = {
            "mcp_read_session": {
                "authority_snapshot_manifest_sha256": "6" * 64,
            }
        }

    def _invoke_result(self, payload: bytes) -> SimpleNamespace:
        def invoke(command, **kwargs):
            output_path = Path(
                command[command.index("--output-last-message") + 1]
            )
            output_path.write_bytes(payload)
            self.runner._provider_raw_refs[self.stage_name] = {
                "raw_output_object_sha256": self.raw_digest,
                "raw_output_object_ref": self.raw_ref,
            }
            return SimpleNamespace(returncode=0, stdout=b"{}\n", stderr=b"")

        return invoke

    def _execution_refs(self, **_kwargs) -> dict[str, str]:
        return {
            "stage_execution_receipt_sha256": self.execution_digest,
            "stage_execution_receipt_ref": self.execution_ref,
        }

    def _normalization_refs(self, **_kwargs) -> dict[str, object]:
        return {
            "stage_normalization_receipt_sha256": (
                self.normalization_digest
            ),
            "stage_normalization_receipt_ref": self.normalization_ref,
            "normalization_status": "normalized_with_warnings",
            "normalization_warning_count": 1,
            "normalization_warnings": (
                {
                    "code": "semantic_quality_rejected",
                    "stage": self.stage_name,
                    "kind": "semantic_validation_rejected",
                },
            ),
        }

    def _execute(self, payload: bytes, *, runtime_effort: str):
        call = {
            "sequence": 1,
            "server": "kaoyan_math_read",
            "tool": "get_task_context",
            "arguments": {},
            "result": {"ok": True},
            "result_sha256": "7" * 64,
        }
        with (
            mock.patch.object(
                self.runner, "_invoke_subprocess", self._invoke_result(payload)
            ),
            mock.patch.object(
                self.runner, "_model_mcp_config_args", return_value=[]
            ),
            mock.patch.object(
                self.runner,
                "_persist_model_mcp_transport",
                return_value=(
                    self.transport_digest,
                    "study-intake-model-mcp-transport://sha256/"
                    + self.transport_digest,
                ),
            ),
            mock.patch.object(
                self.runner,
                "_mcp_stage_calls",
                return_value=((call,), self.transcript_digest, self.transcript_ref),
            ),
            mock.patch.object(
                self.runner,
                "_publish_model_stage_execution",
                side_effect=self._execution_refs,
            ),
            mock.patch.object(
                self.runner,
                "_publish_model_stage_normalization",
                side_effect=self._normalization_refs,
            ),
            mock.patch.object(
                self.runner,
                "_runtime_metadata",
                return_value=(
                    "gpt-5.6-luna",
                    runtime_effort,
                    "test_attestation",
                ),
            ),
        ):
            return self.runner._execute_prompt(
                prompt="return one bounded object",
                output_schema=self.schema,
                image_paths=(),
                stage_name=self.stage_name,
                max_prompt_bytes=4096,
                max_output_bytes=4096,
                allowed_evidence_refs=(),
                bind_evidence_schema=False,
                subject="math",
                processing_context=self.processing_context,
            )

    def _assert_review_stage_bindings(
        self, diagnostic: dict[str, object], disposition: str
    ) -> None:
        self.assertFalse(
            any("mcp_failure" in key for key in diagnostic), diagnostic
        )
        review = diagnostic["review_candidate_stage"]
        self.assertIsInstance(review, dict)
        assert isinstance(review, dict)
        self.assertEqual(review["report_disposition"], disposition)
        self.assertTrue(review["report_available"])
        self.assertEqual(
            review["execution_status"],
            "succeeded" if disposition == "needs_sol_review" else "failed",
        )
        self.assertEqual(
            review["quality_status"],
            "issues_found"
            if disposition == "needs_sol_review"
            else "unchecked",
        )
        self.assertEqual(
            review["sol_review_status"],
            "pending"
            if disposition == "needs_sol_review"
            else "not_eligible",
        )
        self.assertFalse(review["formal_write_eligible"])
        self.assertEqual(review["raw_output_object_sha256"], self.raw_digest)
        self.assertEqual(
            review["stage_execution_receipt_sha256"], self.execution_digest
        )
        self.assertEqual(
            review["review_mcp_transcript_sha256"], self.transcript_digest
        )
        self.assertGreater(review["provider_request_count"], 0)
        self.assertGreater(review["mcp_tool_call_count"], 0)
        self.assertTrue(review["warnings"])
        self.assertEqual(review["formal_write_count"], 0)

    def test_valid_model_mcp_then_semantic_reject_is_review_not_execution_failure(
        self,
    ) -> None:
        result = replace(
            self._execute(
                json.dumps({"candidate": "readable"}).encode("utf-8"),
                runtime_effort="max",
            ),
            runtime_identity_status="confirmed",
        )
        with mock.patch.object(
            self.runner,
            "_publish_model_stage_normalization",
            side_effect=self._normalization_refs,
        ):
            error = self.runner._post_stage_validation_error(
                exc=PreprocessorError("semantic_quality_rejected"),
                result=result,
                stage_name=self.stage_name,
                subject="math",
                processing_context=self.processing_context,
            )
        self.assertEqual(error.code, "semantic_quality_rejected")
        self._assert_review_stage_bindings(
            error.diagnostic, "needs_sol_review"
        )
        self.assertTrue(error.diagnostic["post_stage_validation_failed"])

    def test_valid_model_mcp_identity_mismatch_is_quarantined_technical_failure(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            PreprocessorError, "math_analysis_runtime_identity_mismatch"
        ) as raised:
            self._execute(
                json.dumps({"candidate": "retained"}).encode("utf-8"),
                runtime_effort="high",
            )
        self._assert_review_stage_bindings(
            raised.exception.diagnostic, "quarantined"
        )
        review = raised.exception.diagnostic["review_candidate_stage"]
        assert isinstance(review, dict)
        self.assertEqual(review["runtime_identity_status"], "quarantined")

    def test_invalid_model_content_remains_execution_failure(self) -> None:
        failure_digest = "8" * 64
        with mock.patch.object(
            self.runner,
            "_sign_mcp_stage_failure",
            return_value={
                "mcp_failure_receipt_sha256": failure_digest,
                "mcp_failure_receipt_ref": (
                    "study-intake-model-mcp-failure://sha256/"
                    + failure_digest
                ),
            },
        ) as sign_failure:
            with self.assertRaisesRegex(
                PreprocessorError, "math_analysis_output_invalid_json"
            ) as raised:
                self._execute(b"not-json", runtime_effort="max")
        sign_failure.assert_called_once()
        self.assertEqual(
            raised.exception.diagnostic["mcp_failure_receipt_sha256"],
            failure_digest,
        )
        self.assertNotIn(
            "review_candidate_stage", raised.exception.diagnostic
        )


if __name__ == "__main__":
    unittest.main()
