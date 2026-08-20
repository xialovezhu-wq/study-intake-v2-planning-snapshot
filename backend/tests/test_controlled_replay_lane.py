#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import controlled_replay_lane as lane

DispatchError = lane.DispatchError
FrozenTask = lane.FrozenTask


def _task(subject: str, capture_id: str, release_id: str) -> FrozenTask:
    fingerprint = capture_id.encode("utf-8").hex()[:64].ljust(64, "0")
    payload = {
        "subject": subject,
        "capture_id": capture_id,
        "study_date": "2026-08-06",
        "input_fingerprint": fingerprint,
        "input_binding": {
            "processing_contract_sha256": "1" * 64,
            **(
                {"capture_event_ids": [capture_id]}
                if subject == "english"
                else {}
            ),
        },
        "model_input": {"capture_id": capture_id},
        "allowed_evidence_refs": [f"capture:{capture_id}"],
        "image_paths": [],
        "target_label": capture_id,
        "canonical_state": "ready",
        "sol_state": "shadow",
        "content_group_capture_ids": [capture_id],
        "content_group_members": [
            {
                "subject": subject,
                "capture_id": capture_id,
                "input_fingerprint": fingerprint,
                "input_binding": (
                    {"capture_event_ids": [capture_id]}
                    if subject == "english"
                    else {}
                ),
            }
        ],
        "dispatch_contract": {
            "release_id": release_id,
            "subject_processing_contract_sha256": "1" * 64,
        },
    }
    return FrozenTask(payload)


class ControlledReplayLaneTests(unittest.TestCase):
    def test_open_p0_blocks_before_config_scan_or_model_lane(self) -> None:
        with (
            mock.patch.object(
                lane,
                "require_model_lane_ready",
                side_effect=lane.P0GateError("pre_model_p0_gate_blocked"),
            ),
            mock.patch.object(lane, "load_config") as load_config,
            mock.patch.object(lane, "scan_eligible_candidates") as scan,
            self.assertRaisesRegex(
                DispatchError, "pre_model_p0_gate_blocked"
            ),
        ):
            lane.run(
                Path("/must-not-be-read/config.json"),
                Path("/must-not-be-read/spec.json"),
                Path("/must-not-be-written/output"),
                p0_matrix_path=Path("/gate/matrix.json"),
                p0_audit_root=Path("/gate/audits"),
                golden_inventory_path=Path("/gate/golden.json"),
            )
        load_config.assert_not_called()
        scan.assert_not_called()

    def test_spec_is_release_bound_unique_and_formal_zero(self) -> None:
        release_id = "a" * 64
        value = {
            "schema_version": lane.SPEC_SCHEMA,
            "release_id": release_id,
            "formal_write_count": 0,
            "entries": [
                {
                    "subject": "math",
                    "capture_id": "CAP-1",
                    "expected_input_fingerprint": "b" * 64,
                }
            ],
        }
        rows = lane._spec_entries(value, expected_release_id=release_id)
        self.assertEqual(rows[0]["sequence"], 1)
        duplicate = copy.deepcopy(value)
        duplicate["entries"].append(dict(duplicate["entries"][0]))
        with self.assertRaisesRegex(DispatchError, "controlled_replay_spec_invalid"):
            lane._spec_entries(duplicate, expected_release_id=release_id)
        with self.assertRaisesRegex(DispatchError, "controlled_replay_spec_invalid"):
            lane._spec_entries(value, expected_release_id="c" * 64)

    def test_exact_scan_uses_only_allowlist_and_does_not_mutate_math_config(
        self,
    ) -> None:
        release_id = "d" * 64
        math_task = _task("math", "MATH-CAP", release_id)
        english_task = _task("english", "EN-CAP", release_id)
        entries = [
            {
                "sequence": 1,
                "subject": "english",
                "capture_id": "EN-CAP",
                "expected_input_fingerprint": english_task.frozen_payload[
                    "input_fingerprint"
                ],
            },
            {
                "sequence": 2,
                "subject": "math",
                "capture_id": "MATH-CAP",
                "expected_input_fingerprint": math_task.frozen_payload[
                    "input_fingerprint"
                ],
            },
        ]
        config = {"adapters": {"math": {"enabled": True}}}
        calls = []

        def scan(
            scan_config,
            subject,
            *,
            capture_allowlist,
            candidate_overrides,
            controlled_replay,
        ):
            self.assertIsNone(candidate_overrides)
            self.assertTrue(controlled_replay)
            calls.append(
                {
                    "subject": subject,
                    "allowlist": capture_allowlist,
                    "math_enabled": scan_config["adapters"]["math"]["enabled"],
                }
            )
            task = math_task if subject == "math" else english_task
            return [mock.Mock(task=task)], [
                {
                    "subject": subject,
                    "capture_id": task.frozen_payload["capture_id"],
                }
            ]

        with mock.patch.object(lane, "scan_eligible_candidates", side_effect=scan):
            tasks, decisions = lane._scan_exact_tasks(config, entries)
        self.assertTrue(config["adapters"]["math"]["enabled"])
        self.assertEqual(
            [task.frozen_payload["capture_id"] for task in tasks],
            ["EN-CAP", "MATH-CAP"],
        )
        self.assertEqual(
            [(row["subject"], row["math_enabled"]) for row in calls],
            [("english", True), ("math", True)],
        )
        self.assertEqual(
            [row["capture_id"] for row in decisions],
            ["EN-CAP", "MATH-CAP"],
        )

    def test_missing_evidence_preflight_is_zero_model_and_exact_error(
        self,
    ) -> None:
        spec = {
            "preflight_cases": [
                {
                    "sample_role": "missing-question-image",
                    "external_capture_manifest_path": "/readonly/input.json",
                    "external_capture_manifest_sha256": "a" * 64,
                    "expected_error_code": "math_new_source_evidence_incomplete",
                }
            ]
        }
        capture = {
            "event_id": "MFI-REPLAY-MISSING-QUESTION",
            "formal_id": None,
        }
        with mock.patch.object(
            lane, "_external_capture", return_value=capture
        ), mock.patch.object(
            lane,
            "_external_math_candidate",
            side_effect=lane.PreprocessorError(
                "math_new_source_evidence_incomplete"
            ),
        ):
            rows = lane._preflight_cases(
                {}, spec, expected_release_id="b" * 64
            )
        self.assertEqual(rows[0]["status"], "passed")
        self.assertEqual(rows[0]["model_call_count"], 0)
        self.assertEqual(rows[0]["formal_write_count"], 0)

    def test_solution_image_only_external_preflight_is_eligible(self) -> None:
        spec = {
            "preflight_cases": [
                {
                    "sample_role": "solution_image_only",
                    "fixture_kind": "external_math_capture",
                    "external_capture_manifest_path": "/readonly/input.json",
                    "external_capture_manifest_sha256": "a" * 64,
                    "expected_outcome": "eligible",
                }
            ]
        }
        capture = {
            "event_id": "MFI-REPLAY-SOLUTION-IMAGE-ONLY",
            "formal_id": None,
        }
        with mock.patch.object(
            lane, "_external_capture", return_value=capture
        ), mock.patch.object(
            lane,
            "_external_math_candidate",
            return_value=mock.Mock(),
        ):
            rows = lane._preflight_cases(
                {}, spec, expected_release_id="b" * 64
            )
        self.assertEqual(rows[0]["expected_outcome"], "eligible")
        self.assertEqual(rows[0]["observed_outcome"], "eligible")
        self.assertIsNone(rows[0]["observed_error_code"])
        self.assertEqual(rows[0]["model_call_count"], 0)
        self.assertEqual(rows[0]["formal_write_count"], 0)

    def test_solution_text_only_business_preflight_is_eligible(self) -> None:
        with tempfile.TemporaryDirectory(prefix="controlled-replay-text-") as raw:
            manifest_path = Path(raw) / "business.json"
            manifest_path.write_text("{}\n", encoding="utf-8")
            digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            spec = {
                "preflight_cases": [
                    {
                        "sample_role": "solution_text_only",
                        "fixture_kind": "math_live_business_task",
                        "business_manifest_path": str(manifest_path),
                        "business_manifest_sha256": digest,
                        "capture_id": "LUNA-MATH-TEXT-ONLY",
                        "expected_outcome": "eligible",
                    }
                ]
            }
            business = {
                "tasks": [
                    {
                        "capture_id": "LUNA-MATH-TEXT-ONLY",
                        "luna_eligible": True,
                        "solution_evidence": {
                            "accepted_policy": "solution_text_or_solution_image",
                            "solution_text_present": True,
                            "solution_image_present": False,
                        },
                    }
                ]
            }
            with mock.patch.object(
                lane,
                "validate_live_math_business_manifest",
                return_value=business,
            ):
                rows = lane._preflight_cases(
                    {}, spec, expected_release_id="b" * 64
                )
        self.assertEqual(rows[0]["fixture_kind"], "math_live_business_task")
        self.assertEqual(rows[0]["expected_outcome"], "eligible")
        self.assertEqual(rows[0]["observed_outcome"], "eligible")
        self.assertEqual(
            rows[0]["solution_evidence"],
            {
                "accepted_policy": "solution_text_or_solution_image",
                "solution_text_present": True,
                "solution_image_present": False,
            },
        )
        self.assertEqual(rows[0]["model_call_count"], 0)
        self.assertEqual(rows[0]["formal_write_count"], 0)

    def test_live_math_entry_reopens_a_real_frozen_task(self) -> None:
        with tempfile.TemporaryDirectory(prefix="controlled-replay-live-task-") as raw:
            root = Path(raw)
            manifest_path = root / "business.json"
            manifest_path.write_text("{}\n", encoding="utf-8")
            manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            task = lane.FrozenTask(
                {
                    "subject": "math",
                    "capture_id": "LUNA-MATH-20260809-001",
                    "study_date": "2026-08-09",
                    "recorded_at": "2026-08-09T00:00:00Z",
                    "input_fingerprint": "1" * 64,
                    "input_binding": {
                        "math_live_business_manifest_sha256": manifest_sha,
                        "math_live_content_fingerprint": "2" * 64,
                    },
                    "model_input": {},
                    "allowed_evidence_refs": [],
                    "image_paths": [],
                    "target_label": "live-001",
                    "canonical_state": "awaiting_background_analysis",
                    "sol_state": "pending_review",
                    "dispatch_contract": {
                        "release_id": "b" * 64,
                        "dispatch_reason": "controlled_replay_live_math_frozen_task",
                    },
                }
            )
            task_path = root / "task.json"
            task_path.write_text(
                json.dumps(task.as_dict(), ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            task_sha = hashlib.sha256(task_path.read_bytes()).hexdigest()
            entry = {
                "sequence": 1,
                "subject": "math",
                "capture_id": "LUNA-MATH-20260809-001",
                "sample_role": "LUNA-MATH-001",
                "expected_input_fingerprint": "1" * 64,
                "fixture_kind": "math_live_business_task",
                "business_manifest_path": str(manifest_path),
                "business_manifest_sha256": manifest_sha,
                "frozen_task_path": str(task_path),
                "frozen_task_sha256": task_sha,
            }
            business = {
                "tasks": [
                    {
                        "capture_id": "LUNA-MATH-20260809-001",
                        "content_fingerprint": "2" * 64,
                        "luna_eligible": True,
                        "proposal_only": True,
                    }
                ]
            }
            with mock.patch.object(lane, "_release_id", return_value="b" * 64), mock.patch.object(
                lane, "validate_live_math_business_manifest", return_value=business
            ):
                tasks, decisions = lane._scan_exact_tasks({}, [entry])
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].unit_sha256, task.unit_sha256)
        self.assertEqual(decisions[0]["phase"], "frozen_evidence")
        self.assertTrue(decisions[0]["model_enqueue_allowed"])

    def test_old_single_solution_missing_error_expectation_is_rejected(self) -> None:
        spec = {
            "preflight_cases": [
                {
                    "sample_role": "missing_solution_text",
                    "external_capture_manifest_path": "/readonly/input.json",
                    "external_capture_manifest_sha256": "a" * 64,
                    "expected_error_code": "math_new_source_evidence_incomplete",
                }
            ]
        }
        with mock.patch.object(
            lane,
            "_external_capture",
            return_value={"event_id": "MFI-IMAGE-ONLY", "formal_id": None},
        ), mock.patch.object(
            lane,
            "_external_math_candidate",
            return_value=mock.Mock(),
        ), self.assertRaisesRegex(
            DispatchError,
            "controlled_replay_preflight_expectation_mismatch",
        ):
            lane._preflight_cases({}, spec, expected_release_id="b" * 64)


if __name__ == "__main__":
    unittest.main()
