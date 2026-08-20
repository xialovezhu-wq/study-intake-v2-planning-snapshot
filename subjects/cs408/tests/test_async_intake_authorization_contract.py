from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SKILLS_ROOT = Path(
    os.environ.get("KAOYAN_408_SKILLS_ROOT", PROJECT_ROOT / "codex-skill-sources")
).expanduser()


def normalized(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").split()).replace("`", "")


class CurrentQuestionTwoLayerContractTests(unittest.TestCase):
    def test_project_contracts_define_current_luna_sol_layers(self) -> None:
        paths = {
            "agents": PROJECT_ROOT / "AGENTS.md",
            "readme": PROJECT_ROOT / "README.md",
            "ingest": PROJECT_ROOT / "schema/ingest.md",
            "flow": PROJECT_ROOT / "GPT-Codex错题流转规范.md",
            "manual": PROJECT_ROOT / "408单题同步入库运行手册.md",
        }
        bodies = {name: normalized(path) for name, path in paths.items()}
        combined = " ".join(bodies.values())
        for marker in (
            "CurrentQuestionContext v1",
            "current_question_failure_standing_policy_v1",
            "awaiting_daily_curation",
            "Luna",
            "Sol",
            "batch size=1",
        ):
            self.assertIn(marker, combined, marker)
        for name, body in bodies.items():
            self.assertIn("Luna", body, f"{name}:Luna")
            self.assertIn("Sol", body, f"{name}:Sol")
        current_evidence = normalized(
            PROJECT_ROOT / "schema/current-question-evidence-bundle-v3.md"
        )
        self.assertIn("current-question-evidence-bundle-v3", current_evidence)
        self.assertIn("current-question-interaction-trace-v2", current_evidence)
        self.assertIn("study-intake-luna-analysis-v2", normalized(paths["manual"]))

    def test_skills_define_capture_matrix_and_private_evidence(self) -> None:
        daily = normalized(SKILLS_ROOT / "kaoyan-408-daily-study-loop/SKILL.md")
        wrong = normalized(SKILLS_ROOT / "kaoyan-408-wrong-intake/SKILL.md")
        question = normalized(SKILLS_ROOT / "kaoyan-408-question-worker/SKILL.md")
        combined = f"{daily} {wrong} {question}"
        for marker in (
            "high-confidence, unprompted",
            "medium/low confidence",
            "wrong, partial, blank, or uncertain",
            "current_question_failure_standing_policy_v1",
            "current-question-evidence-bundle-v3",
            "current-question-interaction-trace-v2",
            "recover-current-capture",
            "answer-current-and-next",
            "navigation frontier",
        ):
            self.assertIn(marker, combined)
        self.assertIn("formal_write_count=0", daily)

    def test_hot_path_explicitly_forbids_personalization_and_luna_calls(self) -> None:
        daily = normalized(SKILLS_ROOT / "kaoyan-408-daily-study-loop/SKILL.md")
        question = normalized(SKILLS_ROOT / "kaoyan-408-question-worker/SKILL.md")
        harness = normalized(
            SKILLS_ROOT / "_shared/kaoyan-408/personalization-harness.md"
        )
        for marker in (
            "personalization",
            "MEMORY",
            "formal nodes",
            "another pack",
            "Luna",
            "port 8767",
            "reference books",
            "source code",
        ):
            self.assertIn(marker, f"{daily} {question}")
        self.assertIn("retired from the current-question hot path", harness)
        self.assertIn("must not call", harness)

    def test_daily_curation_is_explicit_date_and_sol_serial(self) -> None:
        daily = normalized(
            SKILLS_ROOT / "kaoyan-408-daily-intake-curation/SKILL.md"
        )
        runtime = normalized(SKILLS_ROOT / "_shared/kaoyan-408/runtime-contract.md")
        combined = f"{daily} {runtime}"
        for marker in (
            "explicit study date",
            "capture-set SHA-256",
            "~/.codex/study-intake-preprocessor/current/bin/preprocess_consumer.py consume",
            "current-question-evidence-bundle-v3",
            "two_pass_ready",
            "gpt-5.6-luna",
            "reasoning_effort=max",
            "sol_curation_decision_408.py seal",
            "adopt",
            "modify",
            "reject",
            "batch-size-1",
            "already_current",
            "Never wait, poll, retry",
        ):
            self.assertIn(marker, combined)
        self.assertIn("Do not rebuild personalization", daily)
        self.assertIn("prepare-next-morning", daily)

    def test_evals_cover_new_boundaries(self) -> None:
        expected = {
            "kaoyan-408-daily-study-loop": {
                "exact_single_entry_correct_and_next",
                "independent_correct_private_observation",
                "initial_wrong_holds_current_item",
                "independent_correct_evidence_recovery_is_observation_only",
                "fragile_correct_recovery_closes_original_operation",
                "stale_advancing_receipt_cannot_skip_unresolved_item",
                "trace_canonical_choice_must_match_cli_choice",
                "hot_path_never_waits_for_luna",
                "explicit_date_sol_verification",
            },
            "kaoyan-408-question-worker": {
                "current_question_first_break",
                "medium_correct_is_captured",
                "capture_recovery_does_not_repeat_outcome",
                "unified_navigation_retains_unresolved_item",
                "independent_correct_recovery_is_observation_only",
                "trace_choice_conflict_fails_closed",
            },
            "kaoyan-408-wrong-intake": {
                "failure_policy_has_bundle_v3_and_capture_only",
                "image_evidence_roles_magic_and_count_fail_closed",
                "luna_max_two_pass_is_only_consumable_result",
                "correct_observation_recovery_never_creates_capture",
                "fragile_capture_recovery_updates_replay_terminal",
                "stale_advance_receipt_is_frontier_rejected",
                "trace_choice_binding_rejects_conflict",
                "luna_v2_proposes_five_semantic_groups",
                "sol_verifies_every_luna_candidate",
            },
            "kaoyan-408-daily-intake-curation": {
                "explicit_date_required",
                "consumer_exactly_once",
                "luna_status_matrix_fail_soft",
                "sol_field_level_decisions",
                "strict_serial_formal_writes",
                "minimal_next_morning_closeout",
            },
        }
        for skill, required in expected.items():
            payload = json.loads(
                (SKILLS_ROOT / skill / "evals/evals.json").read_text(encoding="utf-8")
            )
            names = {item["name"] for item in payload["evals"]}
            self.assertTrue(required.issubset(names), f"{skill}:{required - names}")

    def test_public_single_apply_rejects_multi_package_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "multi.json"
            package.write_text(json.dumps([{}, {}]), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PROJECT_ROOT / "scripts/intake_apply_408.py"),
                    str(package),
                    "--repo",
                    str(PROJECT_ROOT),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("公开单题同步入口", completed.stderr)

    def test_retired_queue_rejects_new_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = root / "submission.json"
            queue = root / "queue"
            package.write_text("{}\n", encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PROJECT_ROOT / "scripts/intake_queue_408.py"),
                    "submit",
                    str(package),
                    "--queue-root",
                    str(queue),
                    "--repo",
                    str(PROJECT_ROOT),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertFalse(queue.exists())
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("新异步提交已于 2026-07-12 退役", completed.stderr)


if __name__ == "__main__":
    unittest.main()
