from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))

import intake_fact_capture_408 as capture_model  # noqa: E402
import managed_408_current_turn as current_turn  # noqa: E402
import current_question_evidence_408 as current_evidence  # noqa: E402
import review_outcome_hot_408 as review_outcome_hot  # noqa: E402
import morning_review_prepared_pack_408 as prepared_pack  # noqa: E402
import morning_review_session as morning  # noqa: E402
import morning_session_hot_state_408 as session_hot  # noqa: E402
import review_feedback_loop as canonical  # noqa: E402
import review_hot_projection_408 as formal_hot  # noqa: E402
import review_hot_state_408 as review_hot  # noqa: E402
from test_morning_review_session_hot_integration_408 import (  # noqa: E402
    MAPPING_HEADERS,
    REVIEW_HEADERS,
    TRACK_HEADERS,
    table,
)


SESSION_ID = "MR-prepared-test"
ITEM_ID = "MQ-01"
QUEUE = """---
schema: morning_review_action_queue_v3
review_date: 2026-07-31
status: ready
---

#### MQ-01 窗口边界

- source_id: `SAFE_ALPHA`
- item_kind: derived_d0
- selection_reason: 前一日安全派生项
- prompt: 分别检查两个窗口边界条件。

#### MQ-02 后继边界

- source_id: `SAFE_BETA`
- item_kind: derived_d0
- selection_reason: 同一冻结题包中的下一题
- prompt: 检查后继状态的两个独立条件。
"""


class PreparedPackManagedHotPath408Tests(unittest.TestCase):
    """Exercise the prepared pack through the production managed hot path."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = (Path(self.tmp.name) / "repo").resolve()
        self.repo.mkdir()
        self.queue = self.repo / "queue.md"
        self.queue.write_text(QUEUE, encoding="utf-8")
        self.evidence = self.repo / "safe-source.md"
        self.evidence.write_text("安全机制依据，不含完整题目。\n", encoding="utf-8")
        policy_target = self.repo / "schema/morning-review-backflow-policy-v1.json"
        policy_target.parent.mkdir(parents=True)
        policy_target.write_bytes(
            (ROOT / "schema/morning-review-backflow-policy-v1.json").read_bytes()
        )
        self._write_formal_projection_sources()
        self._write_empty_review_truth()
        self._write_empty_capture_truth()
        built = formal_hot.build_projection(self.repo, as_of="2026-07-31")
        self.assertEqual(built["status"], "built")
        published = self._publish_pack()
        self.started = morning.command_start(
            argparse.Namespace(
                repo=str(self.repo),
                queue=str(self.queue),
                session_id=SESSION_ID,
                item=None,
                prepared_manifest=str(published["manifest_ref"]),
            )
        )
        self.session_dir = self.repo / morning.STATE_REL / SESSION_ID
        self.session_ledger = self.session_dir / "events.jsonl"
        self.review_ledger = self.repo / canonical.LOOP_REL / "events.jsonl"
        self.capture_root = capture_model.capture_root(self.repo)
        self.capture_ledger = self.capture_root / "events.jsonl"

        self.assertTrue(self.started["managed_hot_path"])
        self.assertEqual(self.started["review_hot_readiness"]["status"], "ready")
        self.assertEqual(self.started["capture_hot_readiness"]["status"], "ready")
        self.assertEqual(self.started["session_hot_bootstrap"]["status"], "ready")
        self.assertTrue(
            (self.session_dir / session_hot.DEFAULT_HOT_DIRNAME / "manifest.json").is_file()
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _surface(self, stem: str) -> dict[str, object]:
        return {
            "stem": stem,
            "options": {
                "A": "仅条件甲成立",
                "B": "仅条件乙成立",
                "C": "条件甲和条件乙同时成立",
                "D": "条件甲和条件乙都不成立",
            },
            "response_instruction": "请回复选项、理由和答后置信度。",
        }

    @staticmethod
    def _evaluator(correct: str) -> dict[str, object]:
        return {
            "correct_option": correct,
            "decisive_reason": "先分别核对两个边界条件，再合并判断。",
            "accepted_reasoning_points": [
                "分别检查两个条件",
                "不把相关条件当成同一条件",
            ],
            "first_break_by_choice": {
                "A": "只检查了条件甲",
                "B": "只检查了条件乙",
                "C": "需要复核两个条件是否都成立",
                "D": "没有命中任一边界条件",
            },
            "feedback_by_result": {
                "independent_correct": "选项与边界判断一致；继续保持分条件核对。",
                "fragile_correct": "选项命中，但理由证据不足；补出两个独立条件。",
                "partial": "已经命中一个条件；下一步单独检查另一个条件。",
                "wrong": "第一个断点在于合并前没有分别核对两个条件。",
                "blank": "先只写出条件甲和条件乙各自的成立判据。",
            },
            "prepared_boundary_checks": ["条件独立性", "边界合并"],
        }

    def _publish_pack(self) -> dict[str, object]:
        draft_path = self.repo / "draft.json"
        verification_path = self.repo / "verification.json"
        draft = {
            "schema": "morning_review_prepared_pack_draft_v1",
            "author_actor_id": "author-agent",
            "author_execution_id": "author-exec-001",
            "queue_sha256": self._sha256(self.queue),
            "items": [
                {
                    "item_id": ITEM_ID,
                    "source_id": "SAFE_ALPHA",
                    "item_kind": "derived_d0",
                    "target": {
                        "subject": "CN",
                        "point_ids": ["CN03-01"],
                        "mechanism_ids": ["KP:SAFE_ALPHA"],
                        "mechanism_primitive_ids": [],
                        "formal_node_ids": [],
                        "module_ids": ["CN03"],
                        "safe_topic_terms": ["窗口边界"],
                        "error_pattern_ids": [],
                        "method_ids": [],
                    },
                    "surface": self._surface("在给定的两个独立条件下，哪种判断成立？"),
                    "evaluator": self._evaluator("C"),
                    "guided": {
                        "surface": self._surface("只检查条件甲与条件乙的推导方向，哪种判断成立？"),
                        "evaluator": self._evaluator("B"),
                    },
                    "repair": {
                        "surface": self._surface("交换两个条件的顺序后，哪种边界判断成立？"),
                        "evaluator": self._evaluator("D"),
                    },
                    "evidence": [
                        {
                            "source_ref": "safe-source.md",
                            "sha256": self._sha256(self.evidence),
                            "role": "机制边界依据",
                        }
                    ],
                },
                {
                    "item_id": "MQ-02",
                    "source_id": "SAFE_BETA",
                    "item_kind": "derived_d0",
                    "target": {
                        "subject": "CN",
                        "point_ids": ["CN03-01"],
                        "mechanism_ids": ["KP:SAFE_BETA"],
                        "mechanism_primitive_ids": [],
                        "formal_node_ids": [],
                        "module_ids": ["CN03"],
                        "safe_topic_terms": ["后继边界"],
                        "error_pattern_ids": [],
                        "method_ids": [],
                    },
                    "surface": self._surface("进入后继状态前，哪种条件组合成立？"),
                    "evaluator": self._evaluator("B"),
                    "guided": {
                        "surface": self._surface("只核对后继状态的第二个条件，哪项成立？"),
                        "evaluator": self._evaluator("B"),
                    },
                    "repair": {
                        "surface": self._surface("交换后继条件的核对顺序，哪项成立？"),
                        "evaluator": self._evaluator("D"),
                    },
                    "evidence": [
                        {
                            "source_ref": "safe-source.md",
                            "sha256": self._sha256(self.evidence),
                            "role": "机制边界依据",
                        }
                    ],
                },
            ],
        }
        self._write_json(draft_path, draft)
        hashes = prepared_pack.draft_hashes(self.repo, self.queue, draft_path)
        checks = {
            key: True for key in hashes["verification_checks_required"]
        }
        verification = {
            "schema": "morning_review_prepared_pack_verification_v1",
            "draft_sha256": hashes["draft_sha256"],
            "verifier_actor_id": "verifier-agent",
            "verifier_execution_id": "verifier-exec-001",
            "items": [
                {**row, "checks": checks}
                for row in hashes["item_hashes"]
            ],
        }
        self._write_json(verification_path, verification)
        return prepared_pack.publish_pack(
            self.repo, self.queue, draft_path, verification_path
        )

    def _write_empty_review_truth(self) -> None:
        loop = self.repo / canonical.LOOP_REL
        loop.mkdir(parents=True, exist_ok=True)
        (loop / "events.jsonl").write_text("", encoding="utf-8")

    def _write_empty_capture_truth(self) -> None:
        root = capture_model.capture_root(self.repo)
        root.mkdir(parents=True, exist_ok=True)
        (root / "events.jsonl").write_text("", encoding="utf-8")
        (root / "state.json").write_text(
            json.dumps(
                capture_model.replay([]),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_formal_projection_sources(self) -> None:
        (self.repo / "复习单元总表.md").write_text(
            table(
                REVIEW_HEADERS,
                [[
                    "RU_ALPHA",
                    "计算机网络",
                    "CN03",
                    "窗口边界",
                    "错题机制",
                    "active_due",
                    "CN_2024_001",
                    "2026-06-01",
                    "2026-07-01",
                    "2026-07-31",
                    "30",
                    "待评分",
                    "2",
                    "高",
                    "中",
                    "闭卷核对窗口边界。",
                    "安全反馈：分别检查两个条件。",
                    "无",
                ]],
            ),
            encoding="utf-8",
        )
        (self.repo / "复习单元节点映射.md").write_text(
            table(
                MAPPING_HEADERS,
                [[
                    "CN_2024_001",
                    "计算机网络",
                    "A",
                    "窗口边界",
                    "RU_ALPHA",
                    "无",
                    "核心机制",
                    "reviewed",
                    "有",
                    "eligible",
                    "无",
                ]],
            ),
            encoding="utf-8",
        )
        (self.repo / "原题复做轨总表.md").write_text(
            table(
                TRACK_HEADERS,
                [[
                    "CN_2024_001",
                    "RU_ALPHA",
                    "active_due",
                    "eligible",
                    "2026-07-01",
                    "2026-07-08",
                    "2026-07-31",
                    "EXAM408-2024-Q20",
                    "2024",
                    "2.1（20）",
                    "段落20",
                    "安全来源",
                    "obsidian://open?vault=details&file=CN_2024_001",
                    "主机制已达标",
                    "无",
                ]],
            ),
            encoding="utf-8",
        )

    def _answer_args(
        self,
        *,
        learner_choice: str,
        result: str,
        choice_result: str,
        reasoning_result: str,
        first_break: str,
        first_break_provenance: str,
    ) -> argparse.Namespace:
        displayed = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        return argparse.Namespace(
            repo=str(self.repo),
            session=SESSION_ID,
            item=ITEM_ID,
            display_surface_sha256=displayed["surface_sha256"],
            learner_choice=learner_choice,
            result=result,
            choice_result=choice_result,
            reasoning_result=reasoning_result,
            confidence="high",
            response_seconds=12.0,
            prompt_level="none",
            first_break=first_break,
            first_break_provenance=first_break_provenance,
            feedback_type=None,
            personalization_mode="off",
            personalization_state_dir=None,
            personalization_max_bytes=12_288,
        )

    def _formal_snapshot(self) -> dict[str, bytes]:
        return {
            name: (self.repo / name).read_bytes()
            for name in formal_hot.SOURCE_FILES
        }

    def _publish_current_display(self, private_root: Path) -> dict[str, str]:
        shown = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        display_text = prepared_pack._render_surface(shown["surface"])
        return current_evidence.publish_metadata_object(
            {
                "schema": current_evidence.TURN_RECEIPT_SCHEMA,
                "status": "display_published",
                "context_id": None,
                "request_id": None,
                "session_id": SESSION_ID,
                "item_id": ITEM_ID,
                "feedback_sha256": None,
                "first_result": None,
                "first_answer_receipt_sha256": None,
                "capture_id": None,
                "capture_receipt_sha256": None,
                "capture_status": "not_started",
                "recovery_locator": None,
                "advance_allowed": False,
                "surface_sha256": shown["surface_sha256"],
                "display_sha256": hashlib.sha256(
                    display_text.encode("utf-8")
                ).hexdigest(),
                "next_display_receipt_sha256": None,
                "formal_write_count": 0,
            },
            kind="turns",
            private_root=private_root,
        )

    def _run_answer_cli(
        self,
        *,
        private_root: Path,
        display_locator: str,
        choice: str,
        confidence: str,
        prompt_level: str,
        trace: list[dict[str, str]],
        attachments_json: dict[str, object] | None = None,
    ) -> tuple[dict[str, object], float]:
        command = [
            sys.executable,
            str(ROOT / "scripts/managed_408_current_turn.py"),
            "--repo",
            str(self.repo),
            "--private-root",
            str(private_root),
            "answer-current-and-next",
            "--display-receipt-locator",
            display_locator,
            "--choice",
            choice,
            "--confidence",
            confidence,
            "--prompt-level",
            prompt_level,
            "--trace-json",
            json.dumps(trace, ensure_ascii=False),
            "--attachments-json",
            json.dumps(
                attachments_json
                or {"question_mode": "dialogue_only", "attachments": []},
                ensure_ascii=False,
            ),
        ]
        started = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=self.repo,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.assertEqual(0, completed.returncode, completed.stderr)
        return json.loads(completed.stdout), elapsed_ms

    def test_current_question_v1_morning_current_only_uses_exact_receipts(self) -> None:
        shown = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        surface = shown["surface"]
        options = [
            {"label": label, "text": surface["options"][label]}
            for label in ("A", "B", "C", "D")
        ]
        context = {
            "schema": "current-question-context-v1",
            "source": "morning_review",
            "request_id": "morning-current-v1-001",
            "session_id": SESSION_ID,
            "item_id": ITEM_ID,
            "source_id": "SAFE_ALPHA",
            "source_stable": True,
            "source_binding_sha256": shown["surface_sha256"],
            "grader_capsule_id": "morning-grader-capsule-001",
            "event_time": "2026-07-31T08:00:00+08:00",
            "study_date": "2026-07-31",
            "timezone": "Asia/Shanghai",
            "idempotency_key": "morning-current-v1-001:first",
            "display_receipt_sha256": "e" * 64,
            "question_valid": True,
            "question_evidence": {
                "public_text": surface["stem"],
                "options": options,
                "response_instruction": surface["response_instruction"],
                "public_surface_sha256": shown["surface_sha256"],
                "attachment_sha256s": [],
            },
            "learner_evidence": {
                "answer_text": "C，分别检查两个条件。",
                "choice": "C",
                "confidence": "high",
                "first_action": "分别检查两个条件",
                "reasoning": "两个独立条件均成立",
                "prompt_level": "none",
                "observed_at": "2026-07-31T08:00:00+08:00",
            },
            "assessment": {
                "choice_result": "correct",
                "reasoning_result": "sound",
                "confidence": "high",
                "prompt_level": "none",
                "first_break": "未观察到",
                "first_break_provenance": "not_observed",
                "first_action": "分别检查两个条件",
                "first_action_provenance": "user_report",
            },
            "formal_write_count": 0,
        }
        private_root = Path(self.tmp.name) / "private-current-question"
        timings: list[float] = []
        with (
            mock.patch.object(
                review_outcome_hot,
                "_formal_context",
                side_effect=AssertionError("current-only must skip formal projection"),
            ),
            mock.patch.object(
                prepared_pack,
                "open_grading_context",
                side_effect=AssertionError("current-only must reuse grader capsule"),
            ),
            mock.patch.object(
                prepared_pack,
                "prepared_feedback_for_item",
                side_effect=AssertionError("current-only must reuse frozen feedback"),
            ),
        ):
            for _ in range(5):
                started = time.perf_counter()
                result = current_turn.run_current_question_turn(
                    self.repo,
                    context,
                    feedback_text="选项与边界判断一致；继续保持分条件核对。",
                    private_evaluation={
                        "correct_option": "C",
                        "grader_basis": "两个条件分别核验后均成立",
                    },
                    private_root=private_root,
                )
                timings.append((time.perf_counter() - started) * 1000)
        self.assertEqual("feedback_ready", result["status"])
        self.assertTrue(result["feedback_authorized"])
        self.assertTrue(result["first_answer_committed"])
        self.assertRegex(
            result["session_feedback_receipt_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertEqual("not_eligible", result["capture_status"])
        self.assertFalse(result["next_item_published"])
        ordered = sorted(timings)
        p95 = ordered[max(0, int(len(ordered) * 0.95) - 1)]
        self.assertLess(p95, 500.0)

    def test_prepared_pack_bridges_live_choice_into_managed_current_turn(self) -> None:
        shown = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        private_root = Path(self.tmp.name) / "private-current-question"
        prepared = prepared_pack.prepare_current_turn(
            self.repo,
            session_id=SESSION_ID,
            item_id=ITEM_ID,
            choice="C",
            confidence="high",
            request_id="morning-live-bridge-001",
            event_time="2026-07-31T08:00:00+08:00",
            display_surface_sha256=shown["surface_sha256"],
            private_root=private_root,
        )
        public_json = json.dumps(prepared, ensure_ascii=False)
        self.assertEqual("correct", prepared["controlled_assessment"]["choice_result"])
        self.assertNotIn("correct_option", public_json)
        self.assertNotIn("decisive_reason", public_json)
        capsule = current_evidence.read_evaluation_capsule(
            prepared["grader_capsule_locator"],
            expected_sha256=prepared["grader_capsule_sha256"],
            private_root=private_root,
        )
        self.assertEqual(prepared["context_id"], capsule["context_id"])
        result = current_turn.run_current_question_turn(
            self.repo,
            prepared["context"],
            feedback_text="选项与访问边界判断一致。",
            private_evaluation=capsule["evaluation_evidence"],
            private_root=private_root,
        )
        self.assertEqual("feedback_ready", result["status"])
        self.assertEqual("not_eligible", result["capture_status"])
        self.assertTrue(result["first_answer_committed"])
        self.assertFalse(result["next_item_published"])

    def test_prompted_live_choice_maps_hint_level_and_captures(self) -> None:
        shown = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        private_root = Path(self.tmp.name) / "private-prompted-current-question"
        prepared = prepared_pack.prepare_current_turn(
            self.repo,
            session_id=SESSION_ID,
            item_id=ITEM_ID,
            choice="C",
            confidence="high",
            prompt_level="L3",
            request_id="morning-live-prompted-001",
            event_time="2026-07-31T08:00:00+08:00",
            display_surface_sha256=shown["surface_sha256"],
            private_root=private_root,
        )
        capsule = current_evidence.read_evaluation_capsule(
            prepared["grader_capsule_locator"],
            expected_sha256=prepared["grader_capsule_sha256"],
            private_root=private_root,
        )
        result = current_turn.run_current_question_turn(
            self.repo,
            prepared["context"],
            feedback_text="提示后选择正确，仍需保留提示依赖。",
            private_evaluation=capsule["evaluation_evidence"],
            private_root=private_root,
        )
        self.assertEqual("feedback_ready", result["status"])
        self.assertEqual("fragile_correct", result["first_result"])
        self.assertEqual("awaiting_daily_curation", result["capture_status"])
        handoff_binding, handoff = (
            current_evidence.read_background_handoff_for_capture(
                result["capture_id"], private_root=private_root
            )
        )
        self.assertEqual("ready", handoff_binding["status"])
        self.assertEqual("ready", handoff["status"])
        self.assertEqual("first_turn_complete", handoff["completion_kind"])
        self.assertIsNone(handoff["trace_supplement_locator"])
        self.assertTrue(result["first_answer_committed"])
        self.assertFalse(result["next_item_published"])
        state = json.loads(
            (self.session_dir / "state.json").read_text(encoding="utf-8")
        )
        self.assertEqual("partial", state["items"][ITEM_ID]["first"]["result"])
        self.assertEqual(
            "progressive", state["items"][ITEM_ID]["first"]["prompt_level"]
        )

    def test_answer_current_and_next_correct_observes_advances_and_replays(self) -> None:
        private_root = Path(self.tmp.name) / "private-answer-and-next-correct"
        display = self._publish_current_display(private_root)
        trace = [
            {
                "role": "learner",
                "kind": "first_action",
                "text": "先分别检查两个条件",
            },
            {
                "role": "learner",
                "kind": "answer",
                "text": "我选 C",
            },
        ]
        formal_before = self._formal_snapshot()
        started = time.perf_counter()
        first = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="none",
            interaction_trace=trace,
            private_root=private_root,
        )
        first_ms = (time.perf_counter() - started) * 1000.0
        self.assertEqual("feedback_and_next_ready", first["status"])
        self.assertFalse(first["idempotent_replay"])
        self.assertEqual("not_eligible", first["capture_status"])
        self.assertEqual(
            "awaiting_background_analysis", first["observation_status"]
        )
        self.assertRegex(first["observation_id"], r"^OBS-[0-9A-F]{24}$")
        self.assertTrue(first["next_item_published"])
        self.assertEqual("MQ-02", first["next_item"]["item_id"])
        self.assertLess(first_ms, 1500.0)
        observation = current_evidence.read_study_observation(
            first["observation_locator"], private_root=private_root
        )
        _, bundle = current_evidence.read_bundle(
            observation["evidence_locator"], private_root=private_root
        )
        self.assertEqual(
            "current-question-evidence-bundle-v3", bundle["schema_version"]
        )
        self.assertEqual(3, bundle["interaction_trace"]["included_event_count"])
        self.assertEqual(trace[0]["text"], bundle["interaction_trace"]["events"][0]["text"])
        self.assertEqual("C", bundle["interaction_trace"]["events"][-1]["text"])
        self.assertEqual(b"", self.capture_ledger.read_bytes())
        self.assertNotIn(
            trace[0]["text"], self.capture_ledger.read_text(encoding="utf-8")
        )
        ledgers_after_first = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )
        turn_files_after_first = sorted((private_root / "turns").iterdir())
        replay_samples: list[float] = []
        for _ in range(20):
            replay_started = time.perf_counter()
            replay = current_turn.answer_current_and_next(
                self.repo,
                display_receipt_locator=display["locator"],
                choice="C",
                confidence="high",
                prompt_level="none",
                interaction_trace=trace,
                private_root=private_root,
            )
            replay_samples.append((time.perf_counter() - replay_started) * 1000.0)
            self.assertTrue(replay["idempotent_replay"])
            self.assertEqual(0, replay["learner_evidence_write_count"])
            self.assertEqual(
                first["next_item"]["display_receipt_locator"],
                replay["next_item"]["display_receipt_locator"],
            )
        p95_index = max(0, (95 * len(replay_samples) + 99) // 100 - 1)
        self.assertLess(sorted(replay_samples)[p95_index], 250.0)
        self.assertEqual(
            ledgers_after_first,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        self.assertEqual(
            turn_files_after_first, sorted((private_root / "turns").iterdir())
        )
        self.assertEqual(formal_before, self._formal_snapshot())

    def test_answer_current_and_next_wrong_retries_then_resolves_with_cumulative_trace(self) -> None:
        private_root = Path(self.tmp.name) / "private-answer-and-next-wrong"
        display = self._publish_current_display(private_root)
        first_utterance = "第一轮我把两个边界误当成同一个条件了"
        first = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="A",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": first_utterance,
                },
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_ready_continue_current", first["status"])
        self.assertEqual("awaiting_daily_curation", first["capture_status"])
        self.assertFalse(first["next_item_published"])
        pending_binding, pending_handoff = (
            current_evidence.read_background_handoff_for_capture(
                first["capture_id"], private_root=private_root
            )
        )
        self.assertEqual("teaching_pending", pending_binding["status"])
        self.assertEqual("teaching_pending", pending_handoff["status"])
        self.assertIsNone(pending_handoff["capture_receipt_sha256"])
        first_turn_receipt = current_evidence.read_metadata_object(
            first["turn_receipt_locator"],
            kind="turns",
            private_root=private_root,
        )
        self.assertFalse(first_turn_receipt["advance_allowed"])
        with self.assertRaisesRegex(
            current_turn.ManagedCurrentTurnError, "recovery_required"
        ):
            current_turn.next_item(
                self.repo,
                prior_turn_receipt_locator=first["turn_receipt_locator"],
                private_root=private_root,
            )
        ledgers_after_first = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )
        replay = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="A",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": first_utterance,
                }
            ],
            private_root=private_root,
        )
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual("feedback_ready_continue_current", replay["status"])
        self.assertEqual(
            ledgers_after_first,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        second_utterance = "第二轮我仍然只检查了条件乙"
        second = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="D",
            confidence="medium",
            prompt_level="L2",
            interaction_trace=[
                {
                    "role": "assistant",
                    "kind": "hint",
                    "text": "把条件甲和条件乙分开写",
                },
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": second_utterance,
                },
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_ready_continue_current", second["status"])
        self.assertFalse(second["next_item_published"])
        _, still_pending = current_evidence.read_background_handoff_for_capture(
            first["capture_id"], private_root=private_root
        )
        self.assertEqual("teaching_pending", still_pending["status"])
        self.assertEqual(
            ledgers_after_first,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        final_utterance = "第三轮拆开核对后选择 C"
        resolved = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="L3",
            interaction_trace=[
                {
                    "role": "assistant",
                    "kind": "correction",
                    "text": "分别核对后再合并",
                },
                {
                    "role": "learner",
                    "kind": "answer",
                    "text": final_utterance,
                },
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_and_next_ready", resolved["status"])
        self.assertTrue(resolved["next_item_published"])
        self.assertEqual("MQ-02", resolved["next_item"]["item_id"])
        self.assertEqual(first["capture_id"], resolved["capture_id"])
        binding, supplement = current_evidence.read_trace_supplement_for_capture(
            first["capture_id"], private_root=private_root
        )
        supplement_text = "\n".join(row["text"] for row in supplement["events"])
        self.assertIn(first_utterance, supplement_text)
        self.assertIn(second_utterance, supplement_text)
        self.assertIn(final_utterance, supplement_text)
        self.assertEqual(
            supplement["interaction_trace_sha256"],
            resolved["trace_supplement"]["interaction_trace_sha256"],
        )
        self.assertEqual(binding["object_sha"], resolved["trace_supplement"]["object_sha"])
        handoff_binding, handoff = (
            current_evidence.read_background_handoff_for_capture(
                first["capture_id"], private_root=private_root
            )
        )
        self.assertEqual("ready", handoff_binding["status"])
        self.assertEqual("ready", handoff["status"])
        self.assertEqual("teaching_resolved", handoff["completion_kind"])
        self.assertEqual(binding["locator"], handoff["trace_supplement_locator"])
        self.assertEqual(binding["object_sha"], handoff["trace_supplement_object_sha256"])
        self.assertEqual(
            supplement["interaction_trace_sha256"],
            handoff["interaction_trace_sha256"],
        )
        self.assertEqual(
            resolved["background_handoff"]["object_sha256"],
            handoff_binding["object_sha256"],
        )
        resolved_turn_receipt = current_evidence.read_metadata_object(
            resolved["turn_receipt_locator"],
            kind="turns",
            private_root=private_root,
        )
        self.assertTrue(resolved_turn_receipt["advance_allowed"])
        public_capture = self.capture_ledger.read_text(encoding="utf-8")
        self.assertNotIn(first_utterance, public_capture)
        self.assertNotIn(second_utterance, public_capture)
        self.assertNotIn(final_utterance, public_capture)
        self.assertNotIn("interaction_trace", public_capture)
        self.assertEqual(1, len(public_capture.splitlines()))
        review_rows = [
            json.loads(line)
            for line in self.review_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(1, len(review_rows))
        self.assertEqual("outcome", review_rows[0]["event_kind"])
        self.assertEqual("wrong", review_rows[0]["first_result"])
        state = json.loads(
            (self.session_dir / "state.json").read_text(encoding="utf-8")
        )
        item = state["items"][ITEM_ID]
        self.assertEqual("wrong", item["first"]["result"])
        self.assertEqual("relearn_required", item["resolution"])
        self.assertFalse(item["teaching_resolution"]["independent_repair"])
        self.assertEqual("none", item["teaching_resolution"]["mastery_effect"])
        self.assertEqual("none", item["teaching_resolution"]["retention_effect"])
        session_events = [
            json.loads(line)
            for line in self.session_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(
            1,
            sum(event["event_type"] == "teaching_resolved" for event in session_events),
        )

    def test_independent_correct_evidence_recovery_never_creates_capture(self) -> None:
        private_root = Path(self.tmp.name) / "private-observation-recovery"
        display = self._publish_current_display(private_root)
        original_publish_bundle = current_turn.private_evidence.publish_bundle
        calls = 0

        def fail_bundle_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise current_evidence.CurrentQuestionEvidenceError(
                    "injected-bundle-failure"
                )
            return original_publish_bundle(*args, **kwargs)

        with mock.patch.object(
            current_turn.private_evidence,
            "publish_bundle",
            side_effect=fail_bundle_once,
        ):
            failed = current_turn.answer_current_and_next(
                self.repo,
                display_receipt_locator=display["locator"],
                choice="C",
                confidence="high",
                prompt_level="none",
                interaction_trace=[
                    {"role": "learner", "kind": "answer", "text": "C"}
                ],
                private_root=private_root,
            )
        self.assertEqual("feedback_ready_recovery_required", failed["status"])
        self.assertEqual("not_eligible", failed["capture_status"])
        self.assertEqual(
            "observation_pending_recovery", failed["observation_status"]
        )
        self.assertFalse(failed["next_item_published"])
        self.assertEqual(b"", self.capture_ledger.read_bytes())

        recovered = current_turn.recover_current_capture(
            self.repo,
            failed["recovery_locator"],
            private_root=private_root,
        )
        self.assertEqual("observation_ready", recovered["status"])
        self.assertEqual("not_eligible", recovered["capture_status"])
        self.assertIsNone(recovered["capture_id"])
        self.assertEqual(
            "awaiting_background_analysis", recovered["observation_status"]
        )
        self.assertTrue(recovered["position_changed"])
        self.assertEqual("MQ-02", recovered["next_item"]["item_id"])
        self.assertEqual(b"", self.capture_ledger.read_bytes())

        replay = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {"role": "learner", "kind": "answer", "text": "C"}
            ],
            private_root=private_root,
        )
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual("feedback_and_next_ready", replay["status"])
        self.assertEqual("MQ-02", replay["next_item"]["item_id"])
        self.assertEqual(b"", self.capture_ledger.read_bytes())

    def test_fragile_correct_capture_recovery_closes_original_operation(self) -> None:
        private_root = Path(self.tmp.name) / "private-fragile-recovery"
        display = self._publish_current_display(private_root)
        with mock.patch.object(
            current_turn,
            "_commit_capture",
            side_effect=current_turn.ManagedCurrentTurnError(
                "injected-fragile-capture-failure"
            ),
        ):
            failed = current_turn.answer_current_and_next(
                self.repo,
                display_receipt_locator=display["locator"],
                choice="C",
                confidence="medium",
                prompt_level="none",
                interaction_trace=[
                    {"role": "learner", "kind": "answer", "text": "C"}
                ],
                private_root=private_root,
            )
        self.assertEqual("feedback_ready_recovery_required", failed["status"])
        self.assertEqual("capture_pending_recovery", failed["capture_status"])
        self.assertFalse(failed["next_item_published"])

        recovered = current_turn.recover_current_capture(
            self.repo,
            failed["recovery_locator"],
            private_root=private_root,
        )
        self.assertEqual("awaiting_daily_curation", recovered["status"])
        self.assertTrue(recovered["position_changed"])
        self.assertEqual("MQ-02", recovered["next_item"]["item_id"])
        self.assertEqual(
            "feedback_and_next_ready",
            recovered["answer_operation_result"]["status"],
        )

        replay = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="medium",
            prompt_level="none",
            interaction_trace=[
                {"role": "learner", "kind": "answer", "text": "C"}
            ],
            private_root=private_root,
        )
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual("feedback_and_next_ready", replay["status"])
        self.assertEqual("MQ-02", replay["next_item"]["item_id"])
        self.assertEqual(
            1, len(self.capture_ledger.read_text(encoding="utf-8").splitlines())
        )

    def test_old_advancing_receipt_cannot_skip_later_unresolved_item(self) -> None:
        private_root = Path(self.tmp.name) / "private-stale-advance"
        display = self._publish_current_display(private_root)
        first = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {"role": "learner", "kind": "answer", "text": "C"}
            ],
            private_root=private_root,
        )
        second = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=first["next_item"]["display_receipt_locator"],
            choice="A",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {"role": "learner", "kind": "answer", "text": "A"}
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_ready_continue_current", second["status"])
        with self.assertRaisesRegex(
            current_turn.ManagedCurrentTurnError,
            "prior_turn_receipt_is_not_navigation_frontier",
        ):
            current_turn.next_item(
                self.repo,
                prior_turn_receipt_locator=first["turn_receipt_locator"],
                private_root=private_root,
            )

    def test_resolved_earlier_item_can_close_when_all_first_answers_exist(self) -> None:
        private_root = Path(self.tmp.name) / "private-final-repair"
        digest = "a" * 64
        supplement_sha = "b" * 64
        interaction_sha = "c" * 64
        receipt_sha = "d" * 64
        prior = current_evidence.publish_metadata_object(
            {
                "schema": current_evidence.TURN_RECEIPT_SCHEMA,
                "status": "teaching_resolved",
                "session_id": SESSION_ID,
                "item_id": "MQ-01",
                "capture_id": "CAP-20260731-final-repair",
                "capture_receipt_sha256": digest,
                "resolution_attestation_sha256": digest,
                "trace_supplement": {
                    "object_sha": supplement_sha,
                    "interaction_trace_sha256": interaction_sha,
                },
                "session_resolution_receipt_sha256": receipt_sha,
                "advance_allowed": True,
                "formal_write_count": 0,
            },
            kind="turns",
            private_root=private_root,
        )
        state = {
            "status": "in_progress",
            "item_order": ["MQ-01", "MQ-02"],
            "items": {
                "MQ-01": {
                    "first": {"result": "partial"},
                    "resolution": "relearn_required",
                    "teaching_resolution": {
                        "capture_id": "CAP-20260731-final-repair",
                        "capture_receipt_sha256": digest,
                        "resolution_attestation_sha256": digest,
                        "trace_supplement_sha256": supplement_sha,
                        "interaction_trace_sha256": interaction_sha,
                        "mastery_effect": "none",
                        "retention_effect": "none",
                        "independent_repair": False,
                    },
                },
                "MQ-02": {
                    "first": {"result": "independent_correct"},
                    "resolution": "resolved",
                },
            },
        }
        with (
            mock.patch.object(morning, "_load_state", return_value=state),
            mock.patch.object(morning, "_ensure_active"),
            mock.patch.object(
                morning,
                "_verify_session_hot_event_receipt",
                return_value={"status": "pass", "receipt_sha256": receipt_sha},
            ) as verify,
        ):
            result = current_turn.next_item(
                self.repo,
                prior_turn_receipt_locator=prior["locator"],
                private_root=private_root,
            )
        self.assertEqual("complete", result["status"])
        verify.assert_called_once_with(
            self.session_dir,
            receipt_sha,
            event_type="teaching_resolved",
            item_id="MQ-01",
        )

    def test_wrong_capture_failure_recovery_then_same_display_corrects_once(self) -> None:
        private_root = Path(self.tmp.name) / "private-answer-recovery-lifecycle"
        display = self._publish_current_display(private_root)
        with mock.patch.object(
            current_turn,
            "_commit_capture",
            side_effect=current_turn.ManagedCurrentTurnError(
                "injected-capture-failure"
            ),
        ):
            failed = current_turn.answer_current_and_next(
                self.repo,
                display_receipt_locator=display["locator"],
                choice="A",
                confidence="high",
                prompt_level="none",
                interaction_trace=[
                    {
                        "role": "learner",
                        "kind": "reasoning",
                        "text": "首答只检查条件甲",
                    }
                ],
                private_root=private_root,
            )
        self.assertEqual("feedback_ready_recovery_required", failed["status"])
        self.assertEqual("capture_pending_recovery", failed["capture_status"])
        self.assertFalse(failed["next_item_published"])
        lifecycle = current_evidence.read_answer_display_lifecycle(
            display["sha256"], private_root=private_root
        )
        self.assertIsNotNone(lifecycle)
        self.assertEqual(
            "capture_pending_recovery",
            lifecycle["first_turn"]["capture_status"],
        )
        self.assertFalse(lifecycle["first_turn"]["advance_allowed"])
        self.assertEqual(1, len(self.review_ledger.read_text(encoding="utf-8").splitlines()))
        self.assertEqual(b"", self.capture_ledger.read_bytes())

        recovered = current_turn.recover_current_capture(
            self.repo,
            failed["recovery_locator"],
            private_root=private_root,
        )
        self.assertEqual("awaiting_daily_curation", recovered["status"])
        self.assertTrue(recovered["display_lifecycle_repaired"])
        repaired = current_evidence.read_answer_display_lifecycle(
            display["sha256"], private_root=private_root
        )
        self.assertEqual(
            "awaiting_daily_curation", repaired["first_turn"]["capture_status"]
        )
        self.assertEqual(recovered["capture_id"], repaired["first_turn"]["capture_id"])
        self.assertFalse(repaired["first_turn"]["advance_allowed"])

        resolved = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="L2",
            interaction_trace=[
                {
                    "role": "assistant",
                    "kind": "correction",
                    "text": "分开核对两个条件",
                },
                {
                    "role": "learner",
                    "kind": "answer",
                    "text": "纠正后选择 C",
                },
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_and_next_ready", resolved["status"])
        self.assertTrue(resolved["next_item_published"])
        self.assertEqual("MQ-02", resolved["next_item"]["item_id"])
        self.assertEqual(recovered["capture_id"], resolved["capture_id"])
        self.assertEqual(
            1, len(self.review_ledger.read_text(encoding="utf-8").splitlines())
        )
        self.assertEqual(
            1, len(self.capture_ledger.read_text(encoding="utf-8").splitlines())
        )
        session_events = [
            json.loads(line)
            for line in self.session_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(
            1,
            sum(event["event_type"] == "teaching_resolved" for event in session_events),
        )
        _, handoff = current_evidence.read_background_handoff_for_capture(
            recovered["capture_id"], private_root=private_root
        )
        self.assertEqual("ready", handoff["status"])
        self.assertEqual("teaching_resolved", handoff["completion_kind"])

    def test_prompt_dependent_capture_recovery_uses_canonical_partial_lifecycle(self) -> None:
        private_root = Path(self.tmp.name) / "private-fragile-recovery-lifecycle"
        display = self._publish_current_display(private_root)
        with mock.patch.object(
            current_turn,
            "_commit_capture",
            side_effect=current_turn.ManagedCurrentTurnError(
                "injected-capture-failure"
            ),
        ):
            failed = current_turn.answer_current_and_next(
                self.repo,
                display_receipt_locator=display["locator"],
                choice="C",
                confidence="high",
                prompt_level="L3",
                interaction_trace=[
                    {"role": "learner", "kind": "answer", "text": "C"}
                ],
                private_root=private_root,
            )
        self.assertEqual("fragile_correct", failed["first_result"])
        self.assertEqual("capture_pending_recovery", failed["capture_status"])

        recovery = current_evidence.read_metadata_object(
            failed["recovery_locator"],
            kind="recoveries",
            private_root=private_root,
        )
        payload = recovery["capture_payload"]
        capture_id = (
            f"CAP-{payload['study_date'].replace('-', '')}-"
            + hashlib.sha256(payload["idempotency_key"].encode()).hexdigest()[:12]
        )
        legacy_events = [
            {
                "role": "assistant",
                "kind": "hint",
                "text": "旧提示",
                "observed_at": None,
            }
        ]
        legacy_trace_sha = hashlib.sha256(
            current_evidence._json_bytes(legacy_events)
        ).hexdigest()
        legacy_supplement = {
            "schema_version": current_evidence.TRACE_SUPPLEMENT_SCHEMA,
            "capture_id": capture_id,
            "context_id": recovery["context"]["context_id"],
            "item_id": ITEM_ID,
            "evidence_manifest_sha256": recovery["evidence_manifest_sha256"],
            "created_at": "2026-07-31T09:00:00+08:00",
            "supplement_kind": "legacy_backfill",
            "resolution_receipt_sha256": None,
            "events": legacy_events,
            "interaction_trace_sha256": legacy_trace_sha,
            "formal_write_count": 0,
        }
        legacy_raw = current_evidence._json_bytes(legacy_supplement, pretty=True)
        legacy_object_sha = hashlib.sha256(legacy_raw).hexdigest()
        private_evidence_root = Path(private_root)
        current_evidence._atomic_private_write(
            private_evidence_root
            / "trace-supplements"
            / "objects"
            / f"{legacy_object_sha}.json",
            legacy_raw,
            root=private_evidence_root,
        )
        legacy_binding = {
            "schema_version": current_evidence.TRACE_SUPPLEMENT_BINDING_SCHEMA,
            "capture_id": capture_id,
            "context_id": recovery["context"]["context_id"],
            "item_id": ITEM_ID,
            "evidence_manifest_sha256": recovery["evidence_manifest_sha256"],
            "locator": (
                current_evidence.TRACE_SUPPLEMENT_LOCATOR_PREFIX
                + legacy_object_sha
            ),
            "object_sha": legacy_object_sha,
            "created_at": "2026-07-31T09:00:00+08:00",
            "supplement_kind": "legacy_backfill",
            "resolution_receipt_sha256": None,
            "formal_write_count": 0,
        }
        legacy_binding_raw = current_evidence._json_bytes(
            legacy_binding, pretty=True
        )
        current_evidence._atomic_private_write(
            private_evidence_root
            / "trace-supplements"
            / "bindings"
            / f"{hashlib.sha256(capture_id.encode()).hexdigest()}.json",
            legacy_binding_raw,
            root=private_evidence_root,
        )

        recovered = current_turn.recover_current_capture(
            self.repo,
            failed["recovery_locator"],
            private_root=private_root,
        )
        self.assertTrue(recovered["display_lifecycle_repaired"])
        lifecycle = current_evidence.read_answer_display_lifecycle(
            display["sha256"], private_root=private_root
        )
        self.assertEqual("partial", lifecycle["first_result"])
        self.assertEqual("partial", lifecycle["first_turn"]["first_result"])
        self.assertEqual(
            "awaiting_daily_curation",
            lifecycle["first_turn"]["capture_status"],
        )
        self.assertIn(
            "旧提示",
            [row["text"] for row in lifecycle["interaction_trace"]["events"]],
        )

        resolved = current_turn.answer_current_and_next(
            self.repo,
            display_receipt_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="none",
            interaction_trace=[
                {"role": "learner", "kind": "answer", "text": "C"}
            ],
            private_root=private_root,
        )
        self.assertEqual("feedback_and_next_ready", resolved["status"])
        _, handoff = current_evidence.read_background_handoff_for_capture(
            recovered["capture_id"], private_root=private_root
        )
        self.assertEqual("ready", handoff["status"])
        self.assertEqual("teaching_resolved", handoff["completion_kind"])
        self.assertIsNotNone(handoff["trace_supplement_locator"])
        binding, supplement = current_evidence.read_trace_supplement_for_capture(
            recovered["capture_id"], private_root=private_root
        )
        self.assertEqual("resolved_trace", binding["supplement_kind"])
        self.assertIn("旧提示", [row["text"] for row in supplement["events"]])

    def test_answer_current_and_next_real_cli_e2e(self) -> None:
        private_root = Path(self.tmp.name) / "private-answer-and-next-cli"
        display = self._publish_current_display(private_root)
        command = [
            sys.executable,
            str(ROOT / "scripts/managed_408_current_turn.py"),
            "--repo",
            str(self.repo),
            "--private-root",
            str(private_root),
            "answer-current-and-next",
            "--display-receipt-locator",
            display["locator"],
            "--choice",
            "C",
            "--confidence",
            "high",
            "--prompt-level",
            "none",
            "--trace-json",
            json.dumps(
                [
                    {
                        "role": "learner",
                        "kind": "answer",
                        "text": "C",
                    }
                ],
                ensure_ascii=False,
            ),
        ]
        started = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=self.repo,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.assertEqual(0, completed.returncode, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual("feedback_and_next_ready", result["status"])
        self.assertTrue(result["next_item_published"])
        self.assertEqual("MQ-02", result["next_item"]["item_id"])
        self.assertEqual(0, result["formal_write_count"])
        self.assertLess(elapsed_ms, 2000.0)

    def test_wrong_then_correct_real_cli_e2e_reuses_one_display(self) -> None:
        private_root = Path(self.tmp.name) / "private-answer-and-next-cli-wrong"
        display = self._publish_current_display(private_root)
        question_path = Path(self.tmp.name) / "question.png"
        solution_path = Path(self.tmp.name) / "solution.jpeg"
        for path, image_format, color in (
            (question_path, "PNG", (10, 20, 30)),
            (solution_path, "JPEG", (30, 20, 10)),
        ):
            output = io.BytesIO()
            Image.new("RGB", (2, 2), color=color).save(output, format=image_format)
            path.write_bytes(output.getvalue())
        repeated_text = "这句原话在不同轮次重复"
        wrong, wrong_ms = self._run_answer_cli(
            private_root=private_root,
            display_locator=display["locator"],
            choice="A",
            confidence="high",
            prompt_level="none",
            trace=[
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": repeated_text,
                }
            ],
            attachments_json={
                "question_mode": "image_question",
                "attachments": [
                    {
                        "path": str(question_path),
                        "sha256": hashlib.sha256(question_path.read_bytes()).hexdigest(),
                        "mime_type": "image/png",
                        "role": "question_image",
                        "label": "question",
                    },
                    {
                        "path": str(solution_path),
                        "sha256": hashlib.sha256(solution_path.read_bytes()).hexdigest(),
                        "mime_type": "image/jpeg",
                        "role": "solution_image",
                        "label": "solution",
                    },
                ],
            },
        )
        self.assertEqual("feedback_ready_continue_current", wrong["status"])
        self.assertFalse(wrong["next_item_published"])
        resolved, resolved_ms = self._run_answer_cli(
            private_root=private_root,
            display_locator=display["locator"],
            choice="C",
            confidence="high",
            prompt_level="L3",
            trace=[
                {
                    "role": "learner",
                    "kind": "reasoning",
                    "text": repeated_text,
                },
                {
                    "role": "learner",
                    "kind": "answer",
                    "text": "纠正后选择 C",
                },
            ],
        )
        self.assertEqual("feedback_and_next_ready", resolved["status"])
        self.assertTrue(resolved["next_item_published"])
        self.assertEqual("MQ-02", resolved["next_item"]["item_id"])
        self.assertEqual(wrong["capture_id"], resolved["capture_id"])
        binding, supplement = current_evidence.read_trace_supplement_for_capture(
            str(resolved["capture_id"]), private_root=private_root
        )
        repeated_positions = [
            index
            for index, event in enumerate(supplement["events"])
            if event["text"] == repeated_text
        ]
        self.assertEqual(2, len(repeated_positions))
        self.assertLess(repeated_positions[0], repeated_positions[1])
        self.assertEqual("resolved_trace", binding["supplement_kind"])
        capture_rows = [
            json.loads(line)
            for line in self.capture_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(1, len(capture_rows))
        self.assertEqual(0, wrong["formal_write_count"])
        self.assertEqual(0, resolved["formal_write_count"])
        self.assertEqual(0, supplement["formal_write_count"])
        private_json = "\n".join(
            path.read_text(encoding="utf-8")
            for path in private_root.rglob("*.json")
        )
        self.assertNotIn(str(question_path), private_json)
        self.assertNotIn(str(solution_path), private_json)
        self.assertLess(wrong_ms, 2000.0)
        self.assertLess(resolved_ms, 2000.0)

    def test_item_reads_use_session_hot_state_and_tamper_fails_closed(self) -> None:
        with mock.patch.object(
            session_hot,
            "load_state_bounded",
            wraps=session_hot.load_state_bounded,
        ) as bounded_load:
            shown = prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
            grading = prepared_pack.open_grading_context(
                self.repo, SESSION_ID, ITEM_ID, "C"
            )

        self.assertEqual(bounded_load.call_count, 2)
        self.assertTrue(shown["answer_safe"])
        self.assertEqual(shown["formal_write_count"], 0)
        self.assertEqual(grading["deterministic_choice_result"], "correct")
        self.assertEqual(grading["formal_write_count"], 0)

        state_path = self.session_dir / "state.json"
        tampered = json.loads(state_path.read_text(encoding="utf-8"))
        tampered["updated_at"] = "2026-07-31T23:59:59+00:00"
        state_path.write_text(
            json.dumps(tampered, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        ledgers_before = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )

        with self.assertRaisesRegex(
            prepared_pack.PreparedPackError,
            "managed morning session state failed closed",
        ):
            prepared_pack.show_item(self.repo, SESSION_ID, ITEM_ID)
        with self.assertRaisesRegex(
            prepared_pack.PreparedPackError,
            "managed morning session state failed closed",
        ):
            prepared_pack.open_grading_context(self.repo, SESSION_ID, ITEM_ID, "C")

        self.assertEqual(
            ledgers_before,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )

    @unittest.skip("retired combined answer-turn")
    def test_correct_answer_and_replay_stay_on_managed_hot_path(self) -> None:
        args = self._answer_args(
            learner_choice="C",
            result="independent_correct",
            choice_result="correct",
            reasoning_result="sound",
            first_break="not_observed",
            first_break_provenance="not_observed",
        )
        formal_before = self._formal_snapshot()
        with (
            mock.patch.object(
                morning,
                "sync_session",
                side_effect=AssertionError("managed turn used legacy session sync"),
            ),
            mock.patch.object(
                morning,
                "audit_review_loop",
                side_effect=AssertionError("managed turn used full review audit"),
            ),
            mock.patch.object(
                morning,
                "audit_fact_capture",
                side_effect=AssertionError("managed turn used full capture audit"),
            ),
        ):
            first = morning.command_answer_turn(args)
            after_first = (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            )
            replay = morning.command_answer_turn(args)

        self.assertTrue(first["managed_hot_path"])
        self.assertEqual(first["review_audit"], "HOT_VERIFIED")
        self.assertEqual(first["capture_status"], "not_eligible")
        self.assertEqual(first["formal_write_count"], 0)
        self.assertEqual(first["personalization"]["status"], "deferred_to_prepared_pack")
        self.assertFalse(first["record_replayed"])
        self.assertTrue(replay["record_replayed"])
        self.assertTrue(replay["already_revealed"])
        self.assertEqual(
            after_first,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        self.assertEqual(self.capture_ledger.read_bytes(), b"")
        self.assertEqual(self._formal_snapshot(), formal_before)

    @unittest.skip("retired prepared-feedback reader; current turn uses frozen feedback")
    def test_prepared_feedback_recovery_requires_exact_review_and_session_receipts(
        self,
    ) -> None:
        args = self._answer_args(
            learner_choice="C",
            result="independent_correct",
            choice_result="correct",
            reasoning_result="sound",
            first_break="not_observed",
            first_break_provenance="not_observed",
        )
        first = morning.command_answer_turn(args)
        ledgers_after_answer = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )

        recovered = morning.command_reveal_feedback(args)
        receipt_gate = recovered["feedback_gate"]["receipt_gate"]
        self.assertTrue(recovered["already_revealed"])
        self.assertEqual(
            receipt_gate["canonical_event_id"], first["review_event_id"]
        )
        self.assertEqual(
            receipt_gate["review_receipt_sha256"],
            first["review_commit_receipt_sha256"],
        )
        self.assertEqual(
            receipt_gate["session_binding_receipt_sha256"],
            first["session_binding_receipt_sha256"],
        )
        self.assertEqual(
            ledgers_after_answer,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )

        review_receipt_path = (
            self.repo
            / review_hot.DEFAULT_STATE_RELATIVE
            / first["review_commit_receipt_ref"]
        )
        review_receipt_raw = review_receipt_path.read_bytes()
        review_receipt_path.write_bytes(b"{}\n")
        with self.assertRaisesRegex(
            morning.SessionError,
            "review receipt lookup failed closed",
        ):
            morning.command_reveal_feedback(args)
        self.assertEqual(
            ledgers_after_answer,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        review_receipt_path.write_bytes(review_receipt_raw)

        binding_receipt_path = Path(
            first["session_binding_commit"]["receipt_ref"]
        )
        binding_receipt_path.unlink()
        with self.assertRaisesRegex(
            morning.SessionError,
            "managed session receipt verification failed",
        ):
            morning.command_reveal_feedback(args)
        self.assertEqual(
            ledgers_after_answer,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )

    @unittest.skip("legacy split prepared-feedback writer is retired")
    def test_passed_receipt_hashes_are_reverified_before_prepared_feedback(
        self,
    ) -> None:
        args = self._answer_args(
            learner_choice="C",
            result="independent_correct",
            choice_result="correct",
            reasoning_result="sound",
            first_break="not_observed",
            first_break_provenance="not_observed",
        )
        recorded = morning.command_record_first(args)
        review_receipt_path = (
            self.repo
            / review_hot.DEFAULT_STATE_RELATIVE
            / recorded["review_commit_receipt_ref"]
        )
        review_receipt_raw = review_receipt_path.read_bytes()
        review_receipt_path.write_bytes(b"{}\n")
        ledgers_before_reveal = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )
        with self.assertRaisesRegex(
            morning.SessionError,
            "review receipt lookup failed closed",
        ):
            morning.command_reveal_feedback(
                args,
                review_receipt_sha256=recorded[
                    "review_commit_receipt_sha256"
                ],
                session_binding_receipt_sha256=recorded[
                    "session_binding_receipt_sha256"
                ],
            )
        self.assertEqual(
            ledgers_before_reveal,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        review_receipt_path.write_bytes(review_receipt_raw)

        binding_receipt_path = Path(
            recorded["session_binding_commit"]["receipt_ref"]
        )
        binding_receipt_raw = binding_receipt_path.read_bytes()
        binding_receipt_path.write_bytes(b"{}\n")
        with self.assertRaisesRegex(
            morning.SessionError,
            "managed session receipt verification failed",
        ):
            morning.command_reveal_feedback(
                args,
                review_receipt_sha256=recorded[
                    "review_commit_receipt_sha256"
                ],
                session_binding_receipt_sha256=recorded[
                    "session_binding_receipt_sha256"
                ],
            )
        self.assertEqual(
            ledgers_before_reveal,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        binding_receipt_path.write_bytes(binding_receipt_raw)

    @unittest.skip("retired 20-turn acceptance target")
    def test_twenty_managed_correct_replays_keep_p95_below_five_seconds(
        self,
    ) -> None:
        args = self._answer_args(
            learner_choice="C",
            result="independent_correct",
            choice_result="correct",
            reasoning_result="sound",
            first_break="not_observed",
            first_break_provenance="not_observed",
        )
        first = morning.command_answer_turn(args)
        self.assertTrue(first["managed_hot_path"])
        ledgers_after_answer = (
            self.session_ledger.read_bytes(),
            self.review_ledger.read_bytes(),
            self.capture_ledger.read_bytes(),
        )
        formal_after_answer = self._formal_snapshot()

        samples_ms: list[float] = []
        for _ in range(20):
            started = time.perf_counter()
            replay = morning.command_answer_turn(args)
            samples_ms.append((time.perf_counter() - started) * 1000.0)
            self.assertTrue(replay["managed_hot_path"])
            self.assertTrue(replay["record_replayed"])
            self.assertTrue(replay["already_revealed"])
            self.assertEqual(replay["review_audit"], "HOT_VERIFIED")
            self.assertEqual(replay["capture_status"], "not_eligible")
            self.assertEqual(replay["formal_write_count"], 0)

        ordered = sorted(samples_ms)
        p95_index = max(0, (95 * len(ordered) + 99) // 100 - 1)
        p95_ms = ordered[p95_index]
        self.assertLess(
            p95_ms,
            5000.0,
            f"managed idempotent answer-turn p95 was {p95_ms:.3f} ms",
        )
        self.assertEqual(
            ledgers_after_answer,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        self.assertEqual(
            len(self.session_ledger.read_text(encoding="utf-8").splitlines()),
            4,
        )
        self.assertEqual(
            len(self.review_ledger.read_text(encoding="utf-8").splitlines()),
            1,
        )
        self.assertEqual(self.capture_ledger.read_bytes(), b"")
        self.assertEqual(self._formal_snapshot(), formal_after_answer)

    @unittest.skip("retired combined answer-turn")
    def test_wrong_answer_and_replay_create_exactly_one_pending_capture(self) -> None:
        args = self._answer_args(
            learner_choice="A",
            result="wrong",
            choice_result="incorrect",
            reasoning_result="diverged",
            first_break="没有分别检查两个边界条件",
            first_break_provenance="visible_evidence",
        )
        formal_before = self._formal_snapshot()
        with (
            mock.patch.object(
                morning,
                "sync_session",
                side_effect=AssertionError("managed turn used legacy session sync"),
            ),
            mock.patch.object(
                morning,
                "audit_review_loop",
                side_effect=AssertionError("managed turn used full review audit"),
            ),
            mock.patch.object(
                morning,
                "audit_fact_capture",
                side_effect=AssertionError("managed turn used full capture audit"),
            ),
        ):
            first = morning.command_answer_turn(args)
            capture_after_first = self.capture_ledger.read_bytes()
            all_after_first = (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                capture_after_first,
            )
            replay = morning.command_answer_turn(args)

        self.assertTrue(first["managed_hot_path"])
        self.assertEqual(first["review_audit"], "HOT_VERIFIED")
        self.assertEqual(first["capture_audit"], "HOT_VERIFIED")
        self.assertEqual(first["capture_status"], "awaiting_daily_curation")
        self.assertEqual(first["capture_commit_status"], "CAPTURED")
        self.assertEqual(first["formal_write_count"], 0)
        self.assertEqual(len(capture_after_first.splitlines()), 1)
        capture_state = json.loads(
            (self.capture_root / "state.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(capture_state["captures"]), 1)
        only_capture = next(iter(capture_state["captures"].values()))
        self.assertEqual(only_capture["quality_status"], "awaiting_daily_curation")

        self.assertTrue(replay["record_replayed"])
        self.assertEqual(replay["capture_commit_status"], "ALREADY_COMMITTED")
        self.assertEqual(replay["capture_id"], first["capture_id"])
        self.assertEqual(
            all_after_first,
            (
                self.session_ledger.read_bytes(),
                self.review_ledger.read_bytes(),
                self.capture_ledger.read_bytes(),
            ),
        )
        self.assertEqual(self._formal_snapshot(), formal_before)


if __name__ == "__main__":
    unittest.main()
