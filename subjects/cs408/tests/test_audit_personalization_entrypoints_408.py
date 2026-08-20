from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import audit_personalization_entrypoints_408 as audit_module  # noqa: E402


class CurrentQuestionEntrypointAuditTests(unittest.TestCase):
    def make_repo(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name).resolve()
        required = [
            *audit_module.HOT_FILES,
            *audit_module.RETIRED_WRITE_GUARDS,
            "scripts/intake_fact_capture_408.py",
            "scripts/prepare_next_morning_408.py",
            "codex-skill-sources/kaoyan-408-daily-study-loop/SKILL.md",
            "codex-skill-sources/kaoyan-408-question-worker/SKILL.md",
            "codex-skill-sources/kaoyan-408-wrong-intake/SKILL.md",
            "codex-skill-sources/_shared/kaoyan-408/personalization-harness.md",
        ]
        for rel in required:
            target = root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / rel, target)
        return temp, root

    def test_current_workspace_passes_new_isolation_audit(self) -> None:
        result = audit_module.audit(REPO)
        self.assertEqual(result["status"], "PASS", result["violations"])
        self.assertFalse(result["personalization_required"])
        self.assertEqual(result["luna_chat_call_count"], 0)
        self.assertEqual(result["formal_write_count"], 0)

    def test_forbidden_personalization_dependency_fails(self) -> None:
        temp, root = self.make_repo()
        self.addCleanup(temp.cleanup)
        path = root / "scripts/managed_408_current_turn.py"
        path.write_text(
            path.read_text(encoding="utf-8")
            + "\n# personalization_turn_receipt_408\n",
            encoding="utf-8",
        )
        result = audit_module.audit(root)
        self.assertEqual(result["status"], "FAIL")
        self.assertTrue(
            any("forbidden_hot_dependency" in row for row in result["violations"])
        )

    def test_retired_control_file_fails(self) -> None:
        temp, root = self.make_repo()
        self.addCleanup(temp.cleanup)
        retired = root / "scripts/managed_408_turn_broker.py"
        retired.write_text("# retired\n", encoding="utf-8")
        result = audit_module.audit(root)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn(
            "retired_control_still_active:scripts/managed_408_turn_broker.py",
            result["violations"],
        )

    def test_old_current_turn_public_command_fails(self) -> None:
        temp, root = self.make_repo()
        self.addCleanup(temp.cleanup)
        path = root / "scripts/managed_408_current_turn.py"
        path.write_text(
            path.read_text(encoding="utf-8")
            + "\ndef _retired_probe(sub):\n"
            + "    sub.add_parser('next-item')\n",
            encoding="utf-8",
        )
        result = audit_module.audit(root)
        self.assertIn(
            "retired_current_turn_cli_present:next-item",
            result["violations"],
        )

    def test_reactivated_personalization_skill_contract_fails(self) -> None:
        temp, root = self.make_repo()
        self.addCleanup(temp.cleanup)
        path = (
            root
            / "codex-skill-sources/_shared/kaoyan-408/personalization-harness.md"
        )
        path.write_text("# active personalization\n", encoding="utf-8")
        result = audit_module.audit(root)
        self.assertEqual(result["status"], "FAIL")
        self.assertTrue(
            any("missing_skill_contract" in row for row in result["violations"])
        )

    def test_legacy_writer_guard_removed_fails(self) -> None:
        temp, root = self.make_repo()
        self.addCleanup(temp.cleanup)
        path = root / "scripts/ordinary_capture_hot_adapter_408.py"
        guard = audit_module.RETIRED_WRITE_GUARDS[
            "scripts/ordinary_capture_hot_adapter_408.py"
        ][0]
        path.write_text(
            path.read_text(encoding="utf-8").replace(guard, "guard_removed"),
            encoding="utf-8",
        )
        result = audit_module.audit(root)
        self.assertIn(
            "legacy_writer_not_retired:"
            "scripts/ordinary_capture_hot_adapter_408.py:"
            + guard,
            result["violations"],
        )


if __name__ == "__main__":
    unittest.main()
