from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILLS = ROOT / "codex-skill-sources"


def compact(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").split())


class ExplicitStudyRecordingAuthorizationTests(unittest.TestCase):
    def test_current_failure_standing_policy_is_automatic_but_formal_writes_are_zero(self) -> None:
        worker = compact(SKILLS / "kaoyan-408-question-worker" / "SKILL.md")
        harness = compact(
            SKILLS / "_shared" / "kaoyan-408" / "personalization-harness.md"
        )
        runtime = compact(
            SKILLS / "_shared" / "kaoyan-408" / "runtime-contract.md"
        )
        agents = compact(ROOT / "AGENTS.md")
        combined = " ".join((worker, harness, runtime, agents))
        lower = combined.lower()

        for marker in (
            "current_question_failure_standing_policy_v1",
            "awaiting_daily_curation",
            "formal_write_count=0",
            "高置信、无提示、无推理断点的独立正确",
            "正确但中低置信",
        ):
            self.assertIn(marker, combined)
        self.assertIn("correct with medium/low confidence", lower)

        self.assertNotIn("Ordinary learning is read-only", worker)
        self.assertNotIn(
            "new real learner answer: begin-turn -> personalized_study_turn_408.py",
            harness,
        )

    def test_explicit_date_is_required_only_for_sol_formal_curation(self) -> None:
        worker = compact(SKILLS / "kaoyan-408-question-worker" / "SKILL.md")
        wrong = compact(SKILLS / "kaoyan-408-wrong-intake" / "SKILL.md")
        daily = compact(SKILLS / "kaoyan-408-daily-study-loop" / "SKILL.md")
        schema = compact(
            SKILLS
            / "kaoyan-408-wrong-intake"
            / "references"
            / "fact-capture-schema.md"
        )
        combined = " ".join((worker, wrong, daily, schema))

        for marker in (
            "current_question_failure_standing_policy_v1",
            "awaiting_daily_curation",
            "Sol",
        ):
            self.assertIn(marker, combined)
        self.assertIn("An explicit user date", worker)
        self.assertIn("Only an explicit user-specified", daily)
        self.assertIn("explicit-date Sol batch", wrong)
        self.assertIn("用户在晚间明确指定日期后", compact(ROOT / "README.md"))

    def test_question_worker_evals_cover_capture_matrix_and_recovery(self) -> None:
        payload = json.loads(
            (
                SKILLS
                / "kaoyan-408-question-worker"
                / "evals"
                / "evals.json"
            ).read_text(encoding="utf-8")
        )
        names = {case["name"] for case in payload["evals"]}
        self.assertIn("high_correct_no_wrong_capture", names)
        self.assertIn("medium_correct_is_captured", names)
        self.assertIn("wrong_feedback_then_capture", names)
        self.assertIn("capture_recovery_does_not_repeat_outcome", names)

    def test_receipt_failure_hides_feedback_but_capture_failure_does_not(self) -> None:
        daily = compact(SKILLS / "kaoyan-408-daily-study-loop" / "SKILL.md")
        morning = compact(
            SKILLS
            / "kaoyan-408-daily-study-loop"
            / "references"
            / "morning-review.md"
        )
        harness = compact(
            SKILLS / "_shared" / "kaoyan-408" / "personalization-harness.md"
        )
        combined = " ".join((daily, morning, harness))

        self.assertIn("first-answer or session receipt cannot be committed", combined)
        self.assertIn(
            "do not reveal answer-bearing feedback and do not advance",
            combined,
        )
        self.assertIn(
            "show the frozen feedback, return `capture_pending_recovery`",
            combined,
        )
        self.assertIn("keep the position", combined)


if __name__ == "__main__":
    unittest.main()
