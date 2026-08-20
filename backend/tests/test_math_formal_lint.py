from __future__ import annotations

import importlib.util
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "bin" / "math_formal_lint.py"
SPEC = importlib.util.spec_from_file_location("math_formal_lint", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MathFormalLintTest(unittest.TestCase):
    def test_valid_math_passes(self) -> None:
        text = "题面\n\n$$\ny=r\\sin\\theta,\\qquad r>0\n$$\n"
        self.assertEqual(MODULE.lint_text(text), [])

    def test_detects_tab_bare_command_and_unbalanced_math(self) -> None:
        text = "wrong: (\theta)\n$$\ny=r\\sin\\theta,qquad\n"
        issues = MODULE.lint_text(text)
        self.assertEqual(
            {row["code"] for row in issues},
            {"tab_character", "bare_latex_spacing_command", "unbalanced_display_math"},
        )

    def test_detects_disallowed_control_character(self) -> None:
        issues = MODULE.lint_text("a\x07b")
        self.assertEqual(issues[0]["code"], "disallowed_control_character")

    def test_capture_ref_accepts_only_canonical_quick_intake_actions(self) -> None:
        prefix = (
            "capture-v1:MFI-CAP-aaaaaaaaaaaaaaaaaaaaaaaa:"
            + "b" * 64
            + ":"
        )
        for action in (
            "record_wrong",
            "record_recurrence",
            "update_representation",
            "mastery_candidate",
        ):
            self.assertIsNotNone(MODULE.CAPTURE_REF_RE.fullmatch(prefix + action))
        self.assertIsNone(
            MODULE.CAPTURE_REF_RE.fullmatch(prefix + "record_mastery_observation")
        )

    def test_empty_markdown_fails_structural_gate(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = Path(raw)
            card = repo / "错题知识网络/错题卡/GS-001_empty.md"
            issues = MODULE.lint_card_structure("", card, repo)
        self.assertEqual(issues[0]["code"], "frontmatter_missing_or_unclosed")

    def test_required_fields_and_source_binding_pass(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = Path(raw)
            artifact = repo / "sources/question.png"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b"question")
            artifact_sha = hashlib.sha256(artifact.read_bytes()).hexdigest()
            manifest_path = repo / "sources/manifest.json"
            manifest = {
                "schema_version": "math-fast-intake-source-bundle-v1",
                "source_locator": "fixture:1",
                "artifacts": [
                    {"path": "sources/question.png", "sha256": artifact_sha}
                ],
            }
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            source_ref = MODULE._source_ref_token("fixture:1", manifest_sha)
            text = f"""---
id: GS-001
title: fixture
subject: 高等数学
source: fixture
date: 2026-08-05
status: 待复做
wrong_point: fixture
answer: fixture
fast_intake_refs:
  - capture-v1:MFI-CAP-aaaaaaaaaaaaaaaaaaaaaaaa:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:record_wrong
fast_intake_source_refs:
  - {source_ref}
lecture_refs:
  - sources/manifest.json
wrong_history:
  - fixture
mastery_history:
  - fixture
knowledge:
  - fixture
error_causes:
  - fixture
methods:
  - fixture
traps:
  - fixture
---

# GS-001 fixture

## 题目摘要

fixture

## 标准答案

fixture

## 正确入口

fixture

## 复做提醒

fixture
"""
            card = repo / "错题知识网络/错题卡/GS-001_fixture.md"
            card.parent.mkdir(parents=True)
            card.write_text(text, encoding="utf-8")
            expected_binding = {
                "fast_intake_refs": {
                    "capture-v1:MFI-CAP-aaaaaaaaaaaaaaaaaaaaaaaa:"
                    + "b" * 64
                    + ":record_wrong"
                },
                "fast_intake_source_refs": {source_ref},
            }
            self.assertEqual(
                MODULE.lint_card_structure(
                    text, card, repo, expected_binding=expected_binding
                ),
                [],
            )

            manifest["artifacts"][0]["sha256"] = "0" * 64
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            issues = MODULE.lint_card_structure(text, card, repo)
            self.assertIn(
                "source_artifact_hash_mismatch", {issue["code"] for issue in issues}
            )

    def test_freeze_id_recomputes_exact_token_and_requires_active_current_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = Path(raw)
            script_target = repo / "数学一回滚复习系统/scripts/quick_intake.py"
            script_target.parent.mkdir(parents=True)
            shutil.copy2(
                Path(
                    "/Users/xiazhibin/Documents/kaoyan-math/"
                    "数学一回滚复习系统/scripts/quick_intake.py"
                ),
                script_target,
            )
            shutil.copy2(
                Path(
                    "/Users/xiazhibin/Documents/kaoyan-math/"
                    "数学一回滚复习系统/scripts/producer_binding_attestation.py"
                ),
                script_target.with_name("producer_binding_attestation.py"),
            )
            card = repo / "错题知识网络/错题卡/GS-001_fixture.md"
            card.parent.mkdir(parents=True)
            capture_id = "MFI-CAP-aaaaaaaaaaaaaaaaaaaaaaaa"
            freeze_id = "MFI-FREEZE-bbbbbbbbbbbbbbbbbbbbbbbb"
            evidence = {"result": "wrong", "first_break": {"text": "fixture"}}
            target = {"kind": "formal_card", "formal_id": "GS-001"}
            capture = {
                "schema_version": MODULE.LEDGER_SCHEMA,
                "event_type": "capture",
                "event_id": capture_id,
                "study_date": "2026-08-05",
                "requested_action": "record_wrong",
                "evidence": evidence,
                "target": target,
                "source_bundle": None,
            }
            capture["content_hash"] = MODULE._quick_sha256(capture)
            snapshot = {
                "capture_event_id": capture_id,
                "capture_content_hash": capture["content_hash"],
                "amendment_event_ids": [],
                "effective_evidence_hash": MODULE._quick_sha256(evidence),
                "effective_target_hash": MODULE._quick_sha256(target),
                "effective_source_bundle_hash": None,
                "requested_action": "record_wrong",
            }
            prefix = {
                "event_count": 1,
                "events_hash": MODULE._quick_sha256(
                    [
                        {
                            "event_id": capture_id,
                            "content_hash": capture["content_hash"],
                        }
                    ]
                ),
            }
            freeze = {
                "schema_version": MODULE.LEDGER_SCHEMA,
                "event_type": "freeze",
                "event_id": freeze_id,
                "freeze_schema_version": "math-fast-intake-freeze-v1",
                "study_date": "2026-08-05",
                "capture_event_ids": [capture_id],
                "capture_snapshots": [snapshot],
                "targets": [
                    {
                        "formal_id": "GS-001",
                        "capture_event_ids": [capture_id],
                        "source_binding": None,
                        "supplemental_source_bundles": [],
                        "fast_intake_refs_before": [],
                        "fast_intake_source_refs_before": [],
                    }
                ],
                "ledger_prefix": prefix,
            }
            freeze["content_hash"] = MODULE._quick_sha256(freeze)
            ledger = repo / MODULE.LEDGER_RELATIVE_PATH
            ledger.parent.mkdir(parents=True, exist_ok=True)
            ledger.write_text(
                "\n".join(json.dumps(row, sort_keys=True) for row in (capture, freeze))
                + "\n",
                encoding="utf-8",
            )
            token = (
                f"capture-v1:{capture_id}:"
                f"{snapshot['effective_evidence_hash']}:record_wrong"
            )
            text = f"""---
id: GS-001
title: fixture
subject: 高等数学
source: fixture
date: 2026-08-05
status: 待复做
wrong_point: fixture
answer: fixture
fast_intake_refs:
  - {token}
wrong_history:
  - fixture
mastery_history:
  - fixture
knowledge:
  - fixture
error_causes:
  - fixture
methods:
  - fixture
traps:
  - fixture
---

## 题目摘要

fixture

## 标准答案

fixture

## 正确入口

fixture

## 复做提醒

fixture
"""
            card.write_text(text, encoding="utf-8")
            report = MODULE.build_report(repo, [card], freeze_id=freeze_id)
            self.assertEqual(report["status"], "passed")

            bad = text.replace(token, token + "\n  - " + token)
            issues = MODULE.lint_card_structure(
                bad,
                card,
                repo,
                expected_binding=MODULE._freeze_bindings(repo, freeze_id)["GS-001"],
            )
            self.assertIn("capture_ref_duplicate", {row["code"] for row in issues})

            closeout = {
                "schema_version": MODULE.LEDGER_SCHEMA,
                "event_type": "closeout",
                "event_id": "MFI-CLOSE-cccccccccccccccccccccccc",
                "freeze_id": freeze_id,
                "capture_event_ids": [capture_id],
                "prepare_id": None,
            }
            closeout["content_hash"] = MODULE._quick_sha256(closeout)
            with ledger.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(closeout, sort_keys=True) + "\n")
            with self.assertRaisesRegex(ValueError, "freeze_not_current"):
                MODULE._freeze_bindings(repo, freeze_id)


if __name__ == "__main__":
    unittest.main()
