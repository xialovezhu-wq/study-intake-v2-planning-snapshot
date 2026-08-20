import hashlib
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SKILL_PATH = (
    ROOT
    / "plugin"
    / "kaoyan-study-intake"
    / "skills"
    / "background-english-processing"
    / "SKILL.md"
)
SHARED_CONTRACT_PATH = (
    ROOT
    / "plugin"
    / "kaoyan-study-intake"
    / "references"
    / "shared-processing-contract.md"
)
SHARED_CONTRACT_SHA256 = (
    "7cbf3b1c10e98d386aa942db4daf986c6bfd0eea0e1e8381a2483aa5ab4ea0c8"
)


class BackgroundEnglishSkillContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.skill = SKILL_PATH.read_text(encoding="utf-8")

    def test_preserves_skill_name_and_bumps_private_version(self) -> None:
        self.assertIn("name: background-english-processing", self.skill)
        self.assertIn("Skill version `4.0.0`.", self.skill)
        self.assertNotIn("Skill version `3.1.1`.", self.skill)
        self.assertNotIn("Skill version `3.1.0`.", self.skill)

    def test_recovered_output_limit_has_english_specific_precedence(self) -> None:
        for required in (
            "For this English Skill only",
            "takes precedence over the shared contract's generic `output limit` "
            "fail-closed sentence",
            "same information need is subsequently recovered by a distinct call "
            "with a smaller `page_size`",
            "Do not repeat the identical arguments",
            "`complete=true` and `next_cursor=null`",
            "cites only successful current-stage calls",
            "contributes no grounding, duplicate, coverage, or evidence ledger entry",
            "must not create a blocking finding or correction resolution",
            "`page_size=1` still returns `OUTPUT_LIMIT`",
            "fail the stage closed",
            "not a second Provider submission",
        ):
            self.assertIn(required, self.skill)

    def test_shared_contract_bytes_remain_frozen(self) -> None:
        self.assertEqual(
            hashlib.sha256(SHARED_CONTRACT_PATH.read_bytes()).hexdigest(),
            SHARED_CONTRACT_SHA256,
        )

    def test_nonempty_signal_capture_keeps_one_terminal_outcome_per_signal(self) -> None:
        self.assertIn(
            "An immutable capture that contains at least one explicit signal must "
            "never produce an empty proposal item set.",
            self.skill,
        )
        self.assertIn(
            "Analysis must emit one terminal outcome for every explicit signal",
            self.skill,
        )
        self.assertIn(
            "Critical Review must return a nonempty `revised_items` array",
            self.skill,
        )
        self.assertIn(
            "`bank_status=existing_bank` or `mastered_status=mastered_excluded`",
            self.skill,
        )

    def test_each_stage_item_requires_complete_artifact_and_library_grounding(self) -> None:
        self.assertIn(
            "For every Analysis item and every Critical Review item",
            self.skill,
        )
        self.assertIn(
            "returned by `read_task_artifact` for a task artifact whose pagination "
            "reached `complete=true`",
            self.skill,
        )
        self.assertIn(
            "returned by a successful, relevant library read in that same stage",
            self.skill,
        )
        self.assertIn(
            "A `get_task_context` ref does not satisfy either role.",
            self.skill,
        )

    def test_critical_review_refs_are_fresh_and_failed_exploration_cannot_ground(self) -> None:
        self.assertIn(
            "A Critical Review item cannot use an Analysis-stage ref as evidence "
            "of Critical Review consumption.",
            self.skill,
        )
        self.assertIn(
            "A failed tool result and a successful zero-item or zero-hit exploration "
            "produce no grounding evidence and cannot be cited.",
            self.skill,
        )

    def test_contract_remains_proposal_only_and_answer_safe(self) -> None:
        self.assertIn("proposal-only", self.skill)
        self.assertIn("formal_write_count=0", self.skill)
        self.assertIn("do not call Sol", self.skill)
        self.assertNotIn("retry `OUTPUT_LIMIT`", self.skill)

    def test_real_two_stage_smoke_cannot_be_replaced_by_zero_model_replay(self) -> None:
        for required in (
            "`--ignore-user-config`",
            "`--model gpt-5.6-luna`",
            '`model_reasoning_effort="max"`',
            "a `service_tier` override is forbidden",
            "Submit Analysis exactly once and fresh Critical Review exactly once.",
            "do not retry, downgrade, or add a third submission",
            "`requested_service_tier=null`",
            "`fast_mode_requested=false`",
            "`fast_mode_effective=not_requested`",
            "A deterministic or zero-model replay is regression evidence only",
        ):
            self.assertIn(required, self.skill)

    def test_real_smoke_requires_distinct_capture_and_isolated_authority(self) -> None:
        self.assertIn(
            "newly frozen, distinct foreground intensive-reading capture",
            self.skill,
        )
        self.assertIn(
            "new unit, batch, Dispatcher runtime, HMAC key, and read session",
            self.skill,
        )
        self.assertIn(
            "must not reuse or split an earlier business capture",
            self.skill,
        )


if __name__ == "__main__":
    unittest.main()
