from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts import zero_model_golden_inventory as inventory


ROOT = Path(__file__).resolve().parents[1]
VALIDATION_BINDING = json.loads(
    (ROOT / "validation" / "golden-replay-modernization-v1.json").read_text(
        encoding="utf-8"
    )
)
REAL_SPEC = Path(VALIDATION_BINDING["candidate_bound_spec_path"])
MATH_BUSINESS_MANIFEST = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
    "luna-real-business-samples.json"
)
MATH_NEGATIVE_MANIFEST = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-deferred-intake/2026-08-09/"
    "read-only-test-manifest.json"
)


class ZeroModelGoldenInventoryTests(unittest.TestCase):
    def test_real_inventory_has_eleven_distinct_business_tasks(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        self.assertEqual(result["status"], "static_fixtures_ready_model_not_run")
        self.assertEqual(result["replay_spec_capture_count"], 10)
        self.assertEqual(result["raw_capture_count"], 13)
        self.assertEqual(
            result["distinct_business_task_count_by_subject"],
            {"math": 6, "cs408": 4, "english": 1},
        )
        self.assertEqual(result["distinct_business_task_count"], 11)
        self.assertEqual(
            result["thirty_business_task_gate"]["status"],
            "pending_insufficient_distinct_real_tasks",
        )
        self.assertFalse(
            result["thirty_business_task_gate"][
                "synthetic_task_substitution_allowed"
            ]
        )
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["formal_write_count"], 0)
        self.assertEqual(
            result["daily_business_task_boundary"][
                "distinct_task_count_by_subject"
            ],
            {"math": 6, "cs408": 4, "english": 1},
        )
        excluded = result["daily_business_task_boundary"][
            "excluded_historical_workloads"
        ]
        self.assertEqual(excluded[0]["issue_id"], "EN-P0-006")
        self.assertEqual(excluded[0]["target_count_snapshot"], 98)
        self.assertEqual(excluded[0]["included_in_daily_task_count"], 0)
        self.assertFalse(excluded[0]["counts_toward_thirty_task_gate"])

    def test_live_math_business_tasks_are_bound_but_not_executed(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        live = result["live_math_business_preflight"]
        self.assertEqual(live["status"], "passed_real_luna_business_preflight")
        self.assertEqual(live["task_count"], 3)
        self.assertEqual(live["distinct_capture_count"], 3)
        self.assertEqual(
            {row["formal_id"] for row in live["tasks"]},
            {"GS-109", "GS-507", None},
        )
        self.assertEqual(
            {row["task_kind"] for row in live["tasks"]},
            {
                "independent_correct_no_false_wrong_card",
                "wrong_then_corrected_weakness_proposal",
                "wrong_then_corrected_multistage_method_proposal",
            },
        )
        for row in live["tasks"]:
            self.assertTrue(row["luna_eligible"])
            self.assertTrue(row["proposal_only"])
            self.assertTrue(row["expected_semantic_assertions"])
            self.assertEqual(
                row["solution_evidence"]["accepted_policy"],
                "solution_text_or_solution_image",
            )
            if row["capture_id"] == "LUNA-MATH-20260809-003":
                self.assertEqual(
                    set(row["solution_evidence"]["satisfied_by"]),
                    {"solution_01.png", "solution_text.md"},
                )
            else:
                self.assertEqual(
                    row["solution_evidence"]["satisfied_by"],
                    ["solution_text.md"],
                )
            self.assertTrue(row["solution_evidence"]["source_verified"])
            self.assertFalse(
                row["solution_evidence"]["source_path_exposed_to_model"]
            )
            self.assertEqual(row["model_call_count"], 0)
            self.assertEqual(row["formal_write_count"], 0)
        by_kind = {row["task_kind"]: row for row in live["tasks"]}
        self.assertIn(
            "no_wrong_item_proposal_from_missing_steps",
            by_kind["independent_correct_no_false_wrong_card"][
                "expected_semantic_assertions"
            ],
        )
        assertion_contract = result["math_live_golden_assertion_contract"]
        self.assertEqual(assertion_contract["status"], "ready_not_executed")
        self.assertEqual(assertion_contract["task_count"], 3)
        self.assertRegex(assertion_contract["validator_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            assertion_contract["result_schema_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertEqual(
            assertion_contract["semantic_assertion_status"],
            "pending_model_replay_after_p0_gate",
        )
        self.assertIn(
            "weakness_distillation_proposal_only",
            by_kind["wrong_then_corrected_weakness_proposal"][
                "expected_semantic_assertions"
            ],
        )
        new_source = by_kind["wrong_then_corrected_multistage_method_proposal"]
        self.assertIsNone(new_source["formal_id"])
        self.assertEqual(new_source["source_route"], "new_source_learning_episode")
        self.assertEqual(new_source["source_locator"], "question-bank-id:170710")
        self.assertIn(
            "no_gs_la_or_pr_identifier_invented",
            new_source["expected_semantic_assertions"],
        )

    def test_superseded_math_entrypoints_are_rejected(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        superseded = result["superseded_math_entrypoint_preflight"]
        self.assertEqual(superseded["status"], "passed_expected_reject")
        self.assertEqual(superseded["manifest_count"], 2)
        self.assertTrue(
            all(
                row["status"] == "passed_expected_reject"
                and row["expected_error_code"] == "math_live_manifest_superseded"
                and row["model_call_count"] == 0
                and row["formal_write_count"] == 0
                for row in superseded["manifests"]
            )
        )

    def test_old_incomplete_math_fixtures_remain_negative_preflight(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        negative = result["read_only_math_negative_preflight"]
        self.assertEqual(negative["status"], "passed_expected_fail_closed")
        self.assertEqual(negative["sample_count"], 2)
        self.assertTrue(
            all(
                row["preflight_status"] == "failed_closed"
                and row["luna_eligible"] is False
                and row["model_call_count"] == 0
                for row in negative["samples"]
            )
        )

    def test_missing_live_math_manifest_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="golden-live-math-") as raw:
            missing = Path(raw) / "missing.json"
            with self.assertRaisesRegex(
                inventory.GoldenInventoryError,
                "math_live_business_fixture_invalid",
            ):
                inventory.inspect_spec(
                    REAL_SPEC,
                    math_business_manifest_path=missing,
                    math_negative_manifest_path=MATH_NEGATIVE_MANIFEST,
                )

    def test_real_frozen_spec_binds_all_requested_golden_roles(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        self.assertEqual(
            set(result["golden_roles"]["math"]),
            {"GS-111", "GS-240", "complete-new-intake"},
        )
        self.assertEqual(
            set(result["golden_roles"]["cs408"]),
            {"DS_2023_002", "FILE_PROTECTION", "OS_2009_003", "FREE_SPACE"},
        )
        self.assertEqual(len(result["golden_roles"]["english"]), 3)
        self.assertEqual(len(result["controlled_replay_preflight"]), 5)
        self.assertEqual(len(result["missing_evidence_preflight"]), 3)
        self.assertTrue(
            all(
                row["static_fixture_status"] == "bound_not_executed"
                and row["model_call_count"] == 0
                for row in result["missing_evidence_preflight"]
            )
        )
        self.assertEqual(
            result["semantic_assertion_status"],
            "pending_model_replay_after_p0_gate",
        )

    def test_solution_evidence_or_rule_has_two_positive_routes(self) -> None:
        result = inventory.inspect_spec(REAL_SPEC)
        contract = result["solution_evidence_preflight"]
        self.assertEqual(
            contract["accepted_policy"], "solution_text_or_solution_image"
        )
        self.assertFalse(contract["inline_capture_text_substitutes_for_artifact"])
        positive = {row["sample_role"]: row for row in contract["positive_cases"]}
        self.assertEqual(set(positive), {"solution_image_only", "solution_text_only"})
        self.assertEqual(
            positive["solution_image_only"]["solution_evidence"],
            {
                "accepted_policy": "solution_text_or_solution_image",
                "solution_text_present": False,
                "solution_image_present": True,
            },
        )
        self.assertEqual(
            positive["solution_text_only"]["solution_evidence"],
            {
                "accepted_policy": "solution_text_or_solution_image",
                "solution_text_present": True,
                "solution_image_present": False,
            },
        )
        negative = contract["negative_case"]
        self.assertEqual(negative["sample_role"], "missing_solution_evidence")
        self.assertEqual(negative["expected_outcome"], "rejected")
        self.assertFalse(negative["solution_evidence"]["solution_text_present"])
        self.assertFalse(negative["solution_evidence"]["solution_image_present"])

    def test_role_omission_fails_without_model_execution(self) -> None:
        value = json.loads(REAL_SPEC.read_text(encoding="utf-8"))
        value = copy.deepcopy(value)
        value["entries"] = [
            row for row in value["entries"] if row["sample_role"] != "GS-240"
        ]
        with tempfile.TemporaryDirectory(prefix="golden-inventory-") as raw:
            path = Path(raw) / "spec.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                inventory.GoldenInventoryError, "golden_role_set_mismatch"
            ):
                inventory.inspect_spec(path)

    def test_english_events_must_form_one_frozen_microbatch(self) -> None:
        value = json.loads(REAL_SPEC.read_text(encoding="utf-8"))
        value = copy.deepcopy(value)
        english_rows = [
            row for row in value["entries"] if row["subject"] == "english"
        ]
        english_rows[0]["expected_input_fingerprint"] = "f" * 64
        with tempfile.TemporaryDirectory(prefix="golden-inventory-") as raw:
            path = Path(raw) / "spec.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                inventory.GoldenInventoryError,
                "english_microbatch_binding_invalid",
            ):
                inventory.inspect_spec(path)


if __name__ == "__main__":
    unittest.main()
