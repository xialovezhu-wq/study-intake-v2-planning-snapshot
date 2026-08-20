from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from lib.math_live_golden_assertions import (
    GS109_KIND,
    GS507_KIND,
    NEW_SOURCE_KIND,
    MathLiveGoldenAssertionError,
    validate_result,
)


def _raw(value: dict) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, indent=2, sort_keys=True
    ).encode("utf-8") + b"\n"


def _value_sha(value: object) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _claim(
    *,
    claim_type: str = "observed_fact",
    provenance: str = "capture",
    boundary: str = "Do not infer unobserved work.",
) -> dict:
    return {
        "claim_type": claim_type,
        "provenance": provenance,
        "counterevidence_or_boundary": boundary,
    }


def _analysis(task_kind: str) -> dict:
    if task_kind == GS109_KIND:
        return {
            "schema_version": "study-intake-luna-math-analysis-v2",
            "sol_verification_plan": {
                "recommended_disposition": "mastery_observation_candidate"
            },
            "evidence_assessment": {
                "completeness": "limited_by_evidence",
                "gaps": [_claim()],
            },
            "unresolved": [_claim()],
            "reasoning_diagnosis": {
                "independent_correct_steps": [],
                "first_break": None,
                "later_breaks": [],
                "hint_dependencies": [],
                "self_corrections": [],
                "history_merge": [],
            },
            "correct_reasoning_reconstruction": [
                _claim(
                    claim_type="standard_math_candidate",
                    provenance="source_bundle",
                )
            ],
            "knowledge_error_signatures": {
                "first_error": None,
                "later_errors": [],
                "historical_errors": [],
                "current_errors": [],
            },
            "formalization_candidates": {
                "wrong_point": [],
                "error_causes": [],
                "method_gap": [],
                "wrong_history": [],
                "mastery_evidence": [],
                "relationship_proposals": [],
                "independent_performance": [_claim()],
            },
            "atomic_signals": [
                {"signal_type": "knowledge", "error_role": "none"}
            ],
        }
    analysis = {
        "schema_version": "study-intake-luna-math-analysis-v2",
        "sol_verification_plan": {
            "recommended_disposition": (
                "new_candidate"
                if task_kind == NEW_SOURCE_KIND
                else "update_existing_candidate"
            )
        },
        "evidence_assessment": {"completeness": "complete", "gaps": []},
        "unresolved": [],
        "reasoning_diagnosis": {
            "independent_correct_steps": [_claim()],
            "first_break": _claim(),
            "later_breaks": [_claim()],
            "hint_dependencies": [_claim()],
            "self_corrections": [_claim()],
            "history_merge": [],
        },
        "correct_reasoning_reconstruction": [],
        "knowledge_error_signatures": {
            "first_error": _claim(),
            "later_errors": [_claim()],
            "historical_errors": [],
            "current_errors": [_claim()],
        },
        "formalization_candidates": {
            "wrong_point": [_claim()],
            "error_causes": [_claim()],
            "method_gap": [_claim()],
            "wrong_history": [],
            "mastery_evidence": [],
            "relationship_proposals": [],
            "independent_performance": [_claim()],
        },
        "atomic_signals": [
            {"signal_type": "error", "error_role": "first_error"},
            {"signal_type": "error", "error_role": "later_error"},
        ],
    }
    return analysis


def _task(formal_id: str | None) -> dict:
    is_gs109 = formal_id == "GS-109"
    is_new_source = formal_id is None
    return {
        "capture_id": (
            "LUNA-MATH-20260809-001"
            if is_gs109
            else (
                "LUNA-MATH-20260809-003"
                if is_new_source
                else "LUNA-MATH-20260809-002"
            )
        ),
        "business_task_id": (
            "MATH-LUNA-BIZ-20260809-001"
            if is_gs109
            else (
                "MATH-LUNA-BIZ-20260809-003"
                if is_new_source
                else "MATH-LUNA-BIZ-20260809-002"
            )
        ),
        "formal_id": formal_id,
        "task_kind": (
            GS109_KIND
            if is_gs109
            else NEW_SOURCE_KIND if is_new_source else GS507_KIND
        ),
        "source_route": (
            "new_source_learning_episode"
            if is_new_source
            else "existing_formal_card_review"
        ),
        "source_locator": "question-bank-id:170710" if is_new_source else None,
        "luna_eligible": True,
        "proposal_only": True,
    }


def _report(task: dict) -> dict:
    is_gs109 = task["task_kind"] == GS109_KIND
    is_new_source = task["task_kind"] == NEW_SOURCE_KIND
    return {
        "schema_version": "study-intake-luna-math-candidate-v4",
        "capture_id": task["capture_id"],
        "target_identity": {
            "formal_card_id": None if is_new_source else task["formal_id"],
            "delivered_card_id": None if is_new_source else task["formal_id"],
            "knowledge_fallback_card_id": None,
            "target_group_key": (
                "source:question-bank-id:170710"
                if is_new_source
                else f"formal:{task['formal_id']}"
            ),
        },
        "relationship_mode": "SHADOW",
        "analysis": _analysis(task["task_kind"]),
        "novel_knowledge_candidates": [],
        "novel_error_candidates": [] if is_gs109 else [{"candidate_id": "weak-1"}],
        "relationship_decisions": [],
        "sol_action_items": {
            "formal_writer_authority": "nightly_sol_only",
            "novel_knowledge_candidates": [],
            "novel_error_candidates": [] if is_gs109 else ["weak-1"],
            "relationship_outcome": "no_high_match_relation",
        },
        "formal_write_count": 0,
    }


def _package(
    task: dict,
    report_sha: str,
    operations: list[str | dict],
) -> dict:
    operation_rows = []
    for raw in operations:
        if isinstance(raw, dict):
            operation_rows.append(copy.deepcopy(raw))
            continue
        operation_rows.append(
            {
                "operation": raw,
                "target": (
                    f"candidate:{task['capture_id']}"
                    if raw in {"propose_new_item", "propose_new_knowledge"}
                    else f"capture:{task['capture_id']}"
                ),
            }
        )
    proposal = {
        "schema_version": "luna_proposal_v2",
        "subject": "math",
        "capture_id": task["capture_id"],
        "review_status": "proposal_ready",
        "host_semantic_prefetch": False,
        "operations": operation_rows,
        "formal_write_count": 0,
    }
    stage = {
        "status": "ready",
        "requested_model": "gpt-5.6-luna",
        "requested_reasoning_effort": "max",
        "runtime_identity_status": "requested_unverified",
        "model_call_count": 1,
        "formal_write_count": 0,
    }
    return {
        "schema_version": "study-intake-preprocess-package-v2",
        "subject": "math",
        "capture_id": task["capture_id"],
        "pipeline_status": "two_pass_ready",
        "model": "gpt-5.6-luna",
        "reasoning_effort": "max",
        "model_call_count": 2,
        "semantic_stage_count": 2,
        "host_semantic_prefetch": False,
        "report_json_sha256": report_sha,
        "report_json_ref": f"study-intake-report://sha256/{report_sha}",
        "luna_proposal": proposal,
        "luna_proposal_sha256": _value_sha(proposal),
        "quality_receipt": {
            "pipeline_status": "two_pass_ready",
            "two_stage_status": "two_pass_ready",
            "quality_gate_status": "pass",
            "formal_write_count": 0,
        },
        "stage_receipts": {
            "analysis": copy.deepcopy(stage),
            "critical_review": copy.deepcopy(stage),
        },
        "formal_write_count": 0,
    }


class MathLiveGoldenAssertionTests(unittest.TestCase):
    def _paths(
        self,
        root: Path,
        task: dict,
        report: dict,
        operations: list[str | dict],
    ) -> tuple[Path, Path]:
        report_raw = _raw(report)
        report_sha = hashlib.sha256(report_raw).hexdigest()
        report_path = root / f"{report_sha}.json"
        report_path.write_bytes(report_raw)
        package = _package(task, report_sha, operations)
        package_raw = _raw(package)
        package_sha = hashlib.sha256(package_raw).hexdigest()
        package_path = root / f"{package_sha}.json"
        package_path.write_bytes(package_raw)
        return package_path, report_path

    def _validate(
        self, task: dict, report: dict, operations: list[str | dict]
    ) -> dict:
        with tempfile.TemporaryDirectory(prefix="math-live-golden-") as raw:
            package_path, report_path = self._paths(
                Path(raw), task, report, operations
            )
            return validate_result(task, package_path, report_path)

    def test_gs109_observation_only_package_passes(self) -> None:
        task = _task("GS-109")
        result = self._validate(task, _report(task), ["mark_existing_item"])
        self.assertEqual(result["status"], "passed")
        self.assertIn(
            "unobserved_reasoning_remains_unknown", result["assertions"]
        )
        self.assertEqual(result["model_call_count"], 2)
        self.assertEqual(result["formal_write_count"], 0)

    def test_gs109_wrong_or_new_knowledge_proposal_fails_closed(self) -> None:
        task = _task("GS-109")
        for operation in ("propose_item_update", "propose_new_knowledge"):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                MathLiveGoldenAssertionError,
                "math_gs109_wrong_or_weakness_proposal",
            ):
                self._validate(task, _report(task), [operation])

        report = _report(task)
        report["novel_knowledge_candidates"] = [{"candidate_id": "invented"}]
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_gs109_wrong_or_weakness_proposal",
        ):
            self._validate(task, report, ["mark_existing_item"])

    def test_gs109_invented_user_reasoning_fails_closed(self) -> None:
        task = _task("GS-109")
        report = _report(task)
        report["analysis"]["reasoning_diagnosis"][
            "independent_correct_steps"
        ] = [_claim()]
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_gs109_reasoning_inference_invalid",
        ):
            self._validate(task, report, ["mark_existing_item"])

    def test_gs507_weakness_only_package_passes(self) -> None:
        task = _task("GS-507")
        result = self._validate(
            task,
            _report(task),
            ["mark_existing_item", "propose_item_update"],
        )
        self.assertEqual(result["status"], "passed")
        self.assertIn(
            "post_explanation_understanding_labeled_separately",
            result["assertions"],
        )

    def test_gs507_forbidden_new_item_or_new_knowledge_fails_closed(self) -> None:
        task = _task("GS-507")
        for operation in ("propose_new_item", "propose_new_knowledge"):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                MathLiveGoldenAssertionError,
                "math_gs507_operation_boundary_invalid",
            ):
                self._validate(
                    task, _report(task), ["propose_item_update", operation]
                )

    def test_gs507_break_or_explanation_boundary_omission_fails_closed(self) -> None:
        task = _task("GS-507")
        cases = (
            ("later_breaks", "math_gs507_later_breaks_missing"),
            (
                "hint_dependencies",
                "math_gs507_post_explanation_boundary_missing",
            ),
            (
                "independent_correct_steps",
                "math_gs507_independent_steps_missing",
            ),
        )
        for field, error in cases:
            report = _report(task)
            report["analysis"]["reasoning_diagnosis"][field] = []
            with self.subTest(field=field), self.assertRaisesRegex(
                MathLiveGoldenAssertionError, error
            ):
                self._validate(task, report, ["propose_item_update"])

    def test_gs507_mastery_upgrade_fails_closed(self) -> None:
        task = _task("GS-507")
        report = _report(task)
        report["analysis"]["formalization_candidates"]["mastery_evidence"] = [
            _claim()
        ]
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_gs507_independent_mastery_upgrade",
        ):
            self._validate(task, report, ["propose_item_update"])

    def test_new_source_multistage_proposal_passes(self) -> None:
        task = _task(None)
        report = _report(task)
        result = self._validate(
            task,
            report,
            [
                "propose_new_item",
                "mark_existing_knowledge",
                "propose_new_knowledge",
            ],
        )
        self.assertEqual(result["formal_id"], None)
        self.assertIn(
            "no_gs_la_or_pr_identifier_invented",
            result["assertions"],
        )
        self.assertEqual(result["formal_write_count"], 0)

    def test_new_source_invented_formal_identity_fails_closed(self) -> None:
        task = _task(None)
        report = _report(task)
        report["target_identity"]["formal_card_id"] = "GS-999999"
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_new_source_formal_id_invented",
        ):
            self._validate(task, report, ["propose_new_item"])

        report = _report(task)
        operation = {
            "operation": "propose_new_item",
            "target": "LA-999999",
        }
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_new_source_formal_id_invented",
        ):
            self._validate(task, report, [operation])

    def test_new_source_existing_item_update_fails_closed(self) -> None:
        task = _task(None)
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_new_source_operation_boundary_invalid",
        ):
            self._validate(
                task,
                _report(task),
                ["propose_new_item", "propose_item_update"],
            )

    def test_new_source_multistage_boundary_omission_fails_closed(self) -> None:
        task = _task(None)
        report = _report(task)
        report["analysis"]["reasoning_diagnosis"]["later_breaks"] = []
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_new_source_later_breaks_missing",
        ):
            self._validate(task, report, ["propose_new_item"])

    def test_nested_formal_write_count_fails_closed(self) -> None:
        task = _task("GS-507")
        report = _report(task)
        report["sol_action_items"]["unexpected"] = {"formal_write_count": 1}
        with self.assertRaisesRegex(
            MathLiveGoldenAssertionError,
            "math_live_golden_formal_write_detected",
        ):
            self._validate(task, report, ["propose_item_update"])


if __name__ == "__main__":
    unittest.main()
