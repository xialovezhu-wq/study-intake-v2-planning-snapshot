from __future__ import annotations

import copy
import hashlib
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    canonical_bytes,
    current_question_bundle_evidence_status,
    validate_current_question_bundle,
)


def _trace(events: list[dict]) -> dict:
    return {
        "schema": "current-question-interaction-trace-v2",
        "events": events,
        "original_event_count": len(events),
        "included_event_count": len(events),
        "omitted_event_count": 0,
        "omitted_ranges": [],
        "truncation_reason": None,
        "full_trace_sha256": hashlib.sha256(canonical_bytes(events)).hexdigest(),
    }


def _attachment(ordinal: int, role: str, marker: str) -> dict:
    digest = marker * 64
    return {
        "ordinal": ordinal,
        "role": role,
        "original_bytes_ref": f"current-question-attachment://sha256/{digest}",
        "sha256": digest,
        "declared_mime_type": "image/png",
        "detected_mime_type": "image/png",
        "detected_format": "png",
        "byte_count": 128,
        "label": f"image-{ordinal}",
    }


def _bundle() -> dict:
    feedback = "只读反馈"
    events = [
        {
            "ordinal": 1,
            "role": "learner",
            "kind": "reasoning",
            "text": "先检查状态边界。",
        }
    ]
    return {
        "schema_version": "current-question-evidence-bundle-v3",
        "question_mode": "dialogue_only",
        "context_id": "CTX-1",
        "request_id": "REQ-1",
        "session_id": "SESSION-1",
        "item_id": "ITEM-1",
        "source_id": "SOURCE-1",
        "source_kind": "current_question",
        "study_date": "2026-08-04",
        "event_time": "2026-08-04T08:00:00+08:00",
        "timezone": "Asia/Shanghai",
        "source_binding_sha256": "1" * 64,
        "current_question": {
            "public_text": "一个状态转换问题。",
            "options": [],
            "response_instruction": "说明第一步。",
            "public_surface_sha256": "2" * 64,
            "attachment_sha256s": [],
        },
        "learner_evidence": {
            "answer_text": "不确定",
            "choice": None,
            "confidence": "low",
            "first_action": "检查状态",
            "reasoning": "边界处中断",
            "prompt_level": "L0",
            "observed_at": "2026-08-04T08:01:00+08:00",
        },
        "evaluation_evidence": {
            "grader_capsule_id": "GRADE-1",
            "correct_answer": "私有答案",
            "correct_option": None,
            "standard_explanation": "私有解析",
            "grader_result": "partial",
            "grader_basis": "当前题证据",
            "provided_by": "current_question_grader",
            "missing_fields": [],
        },
        "assessment": {
            "first_result": "partial",
            "choice_result": None,
            "reasoning_result": "break",
            "confidence": "low",
            "prompt_level": "L0",
            "first_break": "边界条件",
            "first_break_provenance": "observed",
            "first_action": "检查状态",
            "first_action_provenance": "observed",
        },
        "frozen_assistant_feedback": {
            "text": feedback,
            "sha256": hashlib.sha256(feedback.encode("utf-8")).hexdigest(),
        },
        "provenance": {
            "source_locator": "current-question-capsule:1",
            "source_sha256": "3" * 64,
            "context_sha256": "4" * 64,
            "attachment_provenance": [],
            "created_at": "2026-08-04T08:02:00+08:00",
            "missing_items": [],
        },
        "missing_fields": [],
        "attachment_objects": [],
        "interaction_trace": _trace(events),
        "formal_write_count": 0,
    }


class CurrentQuestionEvidenceV3Tests(unittest.TestCase):
    def validate(self, bundle: dict) -> dict:
        return validate_current_question_bundle(
            bundle, study_date="2026-08-04", source_id="SOURCE-1"
        )

    def test_dialogue_only_and_ordered_two_role_image_question(self) -> None:
        self.assertEqual(self.validate(_bundle())["question_mode"], "dialogue_only")
        value = _bundle()
        rows = [
            _attachment(1, "question_image", "a"),
            _attachment(2, "solution_image", "b"),
        ]
        value["question_mode"] = "image_question"
        value["attachment_objects"] = rows
        value["provenance"]["attachment_provenance"] = copy.deepcopy(rows)
        value["current_question"]["attachment_sha256s"] = [
            row["sha256"] for row in rows
        ]
        self.assertEqual(
            [row["role"] for row in self.validate(value)["attachment_objects"]],
            ["question_image", "solution_image"],
        )

    def test_missing_role_ninth_image_and_order_drift_fail_closed(self) -> None:
        value = _bundle()
        value["question_mode"] = "image_question"
        rows = [_attachment(1, "question_image", "a")]
        value["attachment_objects"] = rows
        value["provenance"]["attachment_provenance"] = copy.deepcopy(rows)
        value["current_question"]["attachment_sha256s"] = [rows[0]["sha256"]]
        pending = self.validate(value)
        self.assertEqual(
            current_question_bundle_evidence_status(pending),
            "evidence_pending",
        )

        overflow = _bundle()
        overflow["question_mode"] = "image_question"
        rows = [
            _attachment(
                ordinal,
                "question_image" if ordinal % 2 else "solution_image",
                format(ordinal, "x")[-1],
            )
            for ordinal in range(1, 10)
        ]
        overflow["attachment_objects"] = rows
        overflow["provenance"]["attachment_provenance"] = copy.deepcopy(rows)
        overflow["current_question"]["attachment_sha256s"] = [
            row["sha256"] for row in rows
        ]
        with self.assertRaises(PreprocessorError):
            self.validate(overflow)

        ordered = _bundle()
        ordered["question_mode"] = "image_question"
        rows = [
            _attachment(1, "question_image", "a"),
            _attachment(2, "solution_image", "b"),
        ]
        ordered["attachment_objects"] = rows
        ordered["provenance"]["attachment_provenance"] = copy.deepcopy(rows)
        ordered["current_question"]["attachment_sha256s"] = [
            rows[1]["sha256"],
            rows[0]["sha256"],
        ]
        with self.assertRaisesRegex(PreprocessorError, "order_mismatch"):
            self.validate(ordered)

    def test_one_through_eight_images_preserve_the_exact_declared_order(self) -> None:
        for count in range(1, 9):
            with self.subTest(count=count):
                value = _bundle()
                value["question_mode"] = "image_question"
                rows = [
                    _attachment(
                        ordinal,
                        (
                            "question_image"
                            if ordinal % 2
                            else "solution_image"
                        ),
                        format(ordinal, "x"),
                    )
                    for ordinal in range(1, count + 1)
                ]
                value["attachment_objects"] = rows
                value["provenance"]["attachment_provenance"] = copy.deepcopy(
                    rows
                )
                expected = [row["sha256"] for row in rows]
                value["current_question"]["attachment_sha256s"] = expected
                validated = self.validate(value)
                self.assertEqual(
                    [row["sha256"] for row in validated["attachment_objects"]],
                    expected,
                )
                self.assertEqual(
                    current_question_bundle_evidence_status(validated),
                    "evidence_pending" if count == 1 else "ready",
                )

    def test_trace_gap_digest_and_32_kib_limit_are_enforced(self) -> None:
        bad_digest = _bundle()
        bad_digest["interaction_trace"]["full_trace_sha256"] = "f" * 64
        with self.assertRaises(PreprocessorError):
            self.validate(bad_digest)

        bad_gap = _bundle()
        trace = bad_gap["interaction_trace"]
        trace.update(
            {
                "original_event_count": 3,
                "included_event_count": 1,
                "omitted_event_count": 2,
                "omitted_ranges": [{"start_ordinal": 2, "end_ordinal": 2}],
                "truncation_reason": "size_limit",
            }
        )
        with self.assertRaises(PreprocessorError):
            self.validate(bad_gap)

        oversized = _bundle()
        events = [
            {
                "ordinal": ordinal,
                "role": "learner",
                "kind": "reasoning",
                "text": "甲" * 1800,
            }
            for ordinal in range(1, 9)
        ]
        oversized["interaction_trace"] = _trace(events)
        with self.assertRaises(PreprocessorError):
            self.validate(oversized)


if __name__ == "__main__":
    unittest.main()
