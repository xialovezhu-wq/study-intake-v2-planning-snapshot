from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL_DIR = ROOT / "codex-skill-sources" / "kaoyan-408-daily-study-loop"


class DailyStudyCurrentQuestionContractTests(unittest.TestCase):
    def test_skill_uses_one_entry_and_frontier_bound_navigation(self) -> None:
        skill = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
        compact = " ".join(skill.split())
        for marker in (
            "current-question-context-v1",
            "managed_408_current_turn.py",
            "current_question_failure_standing_policy_v1",
            "current-question-evidence-bundle-v3",
            "current-question-interaction-trace-v2",
            "recover-current-capture",
            "answer-current-and-next",
            "navigation frontier",
            "type-preserving",
        ):
            self.assertIn(marker, compact)
        self.assertIn("Do not separately call", compact)
        self.assertNotIn("current-question-evidence-bundle-v2", compact)
        self.assertNotIn("current-question-interaction-trace-v1", compact)
        self.assertLessEqual(len(skill.encode("utf-8")), 8192)

    def test_hot_path_forbids_cross_question_and_cold_reads(self) -> None:
        skill = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
        hot = " ".join(
            skill.split("## Hot-path context", 1)[1]
            .split("## Exact single entry", 1)[0]
            .split()
        )
        for marker in (
            "MEMORY",
            "personalization",
            "formal nodes",
            "linked practice",
            "variants",
            "another pack",
            "Luna",
            "port 8767",
            "reference books",
            "source code",
            "--help",
            "status",
            "audit",
            "reconcile",
        ):
            self.assertIn(marker, hot)

    def test_exact_bound_morning_action_advances_without_expanding_reads(self) -> None:
        skill = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
        hot = " ".join(
            skill.split("## Hot-path context", 1)[1]
            .split("## Exact single entry", 1)[0]
            .split()
        )
        for marker in (
            "current_question_minimal",
            "action_id",
            "at most once",
            "cross_day",
            "origin_date",
            "fail closed",
            "luna_call_count=0",
            "item_scheduling_superseded",
        ):
            self.assertIn(marker, hot)
        for forbidden_fallback in (
            "searching history",
            "formal data",
            "another question",
            "relations",
        ):
            self.assertIn(forbidden_fallback, hot)

    def test_shared_contracts_retire_personalization_and_split_entrypoints(self) -> None:
        shared = ROOT / "codex-skill-sources" / "_shared" / "kaoyan-408"
        morning = (SKILL_DIR / "references/morning-review.md").read_text(
            encoding="utf-8"
        )
        harness = (shared / "personalization-harness.md").read_text(encoding="utf-8")
        runtime = (shared / "runtime-contract.md").read_text(encoding="utf-8")
        self.assertIn("retired from the current-question hot path", harness)
        for body in (morning, runtime):
            self.assertIn("answer-current-and-next", body)
            self.assertIn("continue-current", body)
            self.assertIn("next-item", body)
            self.assertIn("navigation frontier", body.lower())
        self.assertNotIn("prepared-option-turn", "\n".join((morning, harness, runtime)))

    def test_reference_has_new_capture_matrix_and_failure_split(self) -> None:
        morning = (SKILL_DIR / "references/morning-review.md").read_text(
            encoding="utf-8"
        )
        compact = " ".join(morning.split())
        for marker in (
            "medium/low confidence",
            "wrong, partial, blank, or uncertain",
            "awaiting_daily_curation",
            "type-preserving",
            "closes the original answer operation",
            "Luna failure never changes capture success",
        ):
            self.assertIn(marker, compact)

    def test_evals_cover_matrix_recovery_isolation_and_navigation(self) -> None:
        payload = json.loads(
            (SKILL_DIR / "evals/evals.json").read_text(encoding="utf-8")
        )
        names = {item["name"] for item in payload["evals"]}
        self.assertTrue(
            {
                "exact_single_entry_correct_and_next",
                "independent_correct_private_observation",
                "initial_wrong_holds_current_item",
                "receipt_failure_withholds_feedback",
                "hot_path_never_waits_for_luna",
                "explicit_date_sol_verification",
                "independent_correct_evidence_recovery_is_observation_only",
                "fragile_correct_recovery_closes_original_operation",
                "stale_advancing_receipt_cannot_skip_unresolved_item",
                "trace_canonical_choice_must_match_cli_choice",
            }.issubset(names)
        )


if __name__ == "__main__":
    unittest.main()
