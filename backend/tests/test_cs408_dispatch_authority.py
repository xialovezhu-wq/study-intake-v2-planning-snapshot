#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    FrozenTask,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
)
from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    _verify_cs408_dispatch_authority,
)


class _AuthorityRunner:
    def __init__(self, report: dict[str, object]) -> None:
        self.report = report

    def run_analysis(self, _task, _context):
        return StageResult(
            payload={"draft": "analysis"},
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
        )

    def run_critical_review(self, _task, draft_analysis, _context):
        if draft_analysis != {"draft": "analysis"}:
            raise AssertionError("critical review did not receive the draft")
        return StageResult(
            payload={"revised_analysis": self.report},
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
        )


class _RequestedUnverifiedRunner:
    def __init__(self, report: dict[str, object]) -> None:
        self.report = report

    @staticmethod
    def _stage(payload: dict[str, object]) -> StageResult:
        return StageResult(
            payload=payload,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
        )

    def run_analysis(self, _task, _context):
        return self._stage({"draft": "analysis"})

    def run_critical_review(self, _task, draft_analysis, _context):
        if draft_analysis != {"draft": "analysis"}:
            raise AssertionError("critical review did not receive the draft")
        return self._stage({"revised_analysis": self.report})

class Cs408DispatchAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runtime = self.root / "runtime"
        self.release_id = "a" * 64
        self.manifest = self.root / "release-manifest.json"
        self.manifest.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-preprocessor-release-v2",
                    "release_id": self.release_id,
                }
            ),
            encoding="utf-8",
        )
        self.config = {
            "runtime_root": str(self.runtime),
            "dispatch": {"authority_required": True},
            "release": {"manifest_path": str(self.manifest)},
        }
        self.capture_id = "CAP-AUTH-0001"
        self.input_fingerprint = "frozen-input-fingerprint"
        self.input_binding = {"evidence_bundle_sha256": "b" * 64}
        self.report = {"schema_version": "study-intake-luna-cs408-report-v2"}
        self.task = FrozenTask(
            {
                "subject": "cs408",
                "capture_id": self.capture_id,
                "study_date": "2026-08-05",
                "recorded_at": "2026-08-05T00:00:00Z",
                "input_fingerprint": self.input_fingerprint,
                "input_binding": self.input_binding,
                "model_input": {"question": "bounded test input"},
                "allowed_evidence_refs": ["capture:test"],
                "image_paths": [],
                "target_label": "authority-test",
                "canonical_state": "awaiting_daily_curation",
                "sol_state": "pending_review",
                "dispatch_contract": {"release_id": self.release_id},
            }
        )
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _AuthorityRunner(self.report),
            stage_timeout_seconds=2,
        )
        result = dispatcher.submit(self.task).wait(3)
        self.assertEqual(result.outcome, "succeeded")
        self.package = {
            "input_fingerprint": self.input_fingerprint,
            "input_binding": self.input_binding,
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "stage_receipts": {
                stage: {
                    "status": "ready",
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "runtime_model": REQUIRED_MODEL,
                    "runtime_reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "runtime_metadata_provenance": (
                        "codex_json_attestation_v1"
                    ),
                    "runtime_identity_status": "confirmed",
                }
                for stage in ("analysis", "critical_review")
            },
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_authenticated_dispatch_completion_binds_consumer_package(self) -> None:
        bindings = _verify_cs408_dispatch_authority(
            self.config,
            capture_id=self.capture_id,
            package=self.package,
            report_artifact=self.report,
        )
        self.assertTrue(bindings["authority_verified"])
        self.assertEqual(bindings["authority_release_id"], self.release_id)
        self.assertEqual(bindings["authority_lease_fence"], 1)

    def test_consumer_rejects_package_not_bound_to_frozen_input(self) -> None:
        tampered = dict(self.package)
        tampered["input_fingerprint"] = "tampered"
        with self.assertRaisesRegex(
            PreprocessorError, "dispatch_authority_binding_mismatch"
        ):
            _verify_cs408_dispatch_authority(
                self.config,
                capture_id=self.capture_id,
                package=tampered,
                report_artifact=self.report,
            )

    def test_requested_unverified_identity_is_not_filled_as_observed(self) -> None:
        capture_id = "CAP-AUTH-0002"
        input_fingerprint = "frozen-input-fingerprint-unverified"
        payload = dict(self.task.frozen_payload)
        payload.update(
            {
                "capture_id": capture_id,
                "input_fingerprint": input_fingerprint,
            }
        )
        task = FrozenTask(payload)
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: _RequestedUnverifiedRunner(self.report),
            stage_timeout_seconds=2,
        )
        result = dispatcher.submit(task).wait(3)
        self.assertEqual(result.outcome, "succeeded")
        package = {
            "input_fingerprint": input_fingerprint,
            "input_binding": self.input_binding,
            "model": REQUIRED_MODEL,
            "reasoning_effort": REQUIRED_REASONING_EFFORT,
            "stage_receipts": {
                stage: {
                    "status": "ready",
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "runtime_model": None,
                    "runtime_reasoning_effort": None,
                    "runtime_metadata_provenance": "unavailable",
                    "runtime_identity_status": "requested_unverified",
                }
                for stage in ("analysis", "critical_review")
            },
        }
        bindings = _verify_cs408_dispatch_authority(
            self.config,
            capture_id=capture_id,
            package=package,
            report_artifact=self.report,
        )
        self.assertTrue(bindings["authority_verified"])
        stage_runtime = result.completion["package_path"]
        authority_package = json.loads(Path(stage_runtime).read_text())
        for stage in ("analysis", "critical_review"):
            runtime = authority_package["stage_runtime"][stage]
            self.assertEqual(
                runtime["runtime_identity_status"], "requested_unverified"
            )
            self.assertIsNone(runtime["runtime_model"])
            self.assertIsNone(runtime["runtime_reasoning_effort"])


if __name__ == "__main__":
    unittest.main()
