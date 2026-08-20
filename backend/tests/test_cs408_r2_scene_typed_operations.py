from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "lib"
if str(LIB) not in sys.path:
    sys.path.insert(0, str(LIB))

import cs408_semantic_admission as semantic  # noqa: E402


CAPTURE_ID = "OBS-R2-CS408-0001"
ANALYSIS_REF = "mcp-item:cs408:" + "a" * 64
CRITICAL_REF = "mcp-item:cs408:" + "b" * 64


def morning_envelope() -> dict:
    display_sha = "1" * 64
    return {
        "schema_version": "cs408-scene-admission-input-v1",
        "subject": "cs408",
        "capture_id": CAPTURE_ID,
        "scene": "morning_review",
        "source_kind": "morning_review",
        "evidence": {
            "review_identity": "MR-2026-08-10-001",
            "display_receipt_sha256": display_sha,
            "display_receipt_ref": (
                "study-intake-display-receipt://sha256/" + display_sha
            ),
            "first_answer": {
                "answer_sha256": "2" * 64,
                "observed_at": "2026-08-10T00:00:00+00:00",
                "outcome": "independent_correct",
                "no_prompt": True,
                "reasoning_break": False,
            },
            "confidence": {"level": "high", "provenance": "user_explicit"},
            "exposed_problem": "none_observed",
            "correction": {
                "occurred": False,
                "trace_artifact_sha256": None,
                "trace_artifact_ref": None,
            },
        },
    }


def formal_envelope() -> dict:
    chunks = [
        {
            "ordinal": 0,
            "capture_id": CAPTURE_ID,
            "sha256": "6" * 64,
            "ref": "study-intake-dialogue-chunk://sha256/" + "6" * 64,
            "byte_count": 120,
        },
        {
            "ordinal": 1,
            "capture_id": CAPTURE_ID,
            "sha256": "7" * 64,
            "ref": "study-intake-dialogue-chunk://sha256/" + "7" * 64,
            "byte_count": 80,
        },
    ]
    manifest = semantic.sha256_value(
        [
            {
                "ordinal": row["ordinal"],
                "sha256": row["sha256"],
                "byte_count": row["byte_count"],
            }
            for row in chunks
        ]
    )
    return {
        "schema_version": "cs408-scene-admission-input-v1",
        "subject": "cs408",
        "capture_id": CAPTURE_ID,
        "scene": "formal_problem",
        "source_kind": "formal_problem",
        "evidence": {
            "stable_question_identity": "FORMAL-SOURCE-2026-001",
            "question_surface_sha256": "3" * 64,
            "question_image": {
                "capture_id": CAPTURE_ID,
                "sha256": "4" * 64,
                "ref": "study-intake-private-image://sha256/" + "4" * 64,
                "detected_mime_type": "image/png",
            },
            "solution_image": {
                "capture_id": CAPTURE_ID,
                "sha256": "5" * 64,
                "ref": "study-intake-private-image://sha256/" + "5" * 64,
                "detected_mime_type": "image/jpeg",
            },
            "actual_answer": {
                "answer_sha256": "8" * 64,
                "observed_at": "2026-08-10T00:01:00+00:00",
            },
            "full_dialogue": {
                "artifact_sha256": "9" * 64,
                "artifact_ref": (
                    "study-intake-private-dialogue://sha256/" + "9" * 64
                ),
                "chunk_manifest_sha256": manifest,
                "chunks": chunks,
            },
        },
    }


def operation_payload(name: str) -> tuple[str, dict]:
    rows = {
        "mark_existing_item": (
            "capture:" + CAPTURE_ID,
            {"formal_id": "DS_2023_002", "identity_match": "exact"},
        ),
        "propose_new_item": (
            "capture:" + CAPTURE_ID,
            {
                "proposal_local_id": "proposal-item-001",
                "identity_status": "no_reliable_match",
            },
        ),
        "mark_existing_knowledge": (
            "knowledge:DS02-02",
            {
                "formal_knowledge_id": "DS02-02",
                "identity_match": "exact",
            },
        ),
        "propose_new_knowledge": (
            "knowledge:proposal-knowledge-001",
            {
                "proposal_local_id": "proposal-knowledge-001",
                "label": "new knowledge proposal",
                "parent_status": "ambiguous",
            },
        ),
        "propose_relation": (
            "relation:proposal-relation-001",
            {
                "current_capture_endpoint": "capture:" + CAPTURE_ID,
                "formal_node_endpoint": "formal:DS_2023_002",
                "relation_type": "R02",
                "strength": "strong",
                "reason": "Exact evidence supports a candidate relation.",
                "existing_edge": False,
            },
        ),
        "needs_review": (
            "capture:" + CAPTURE_ID,
            {"reason_code": "ambiguous_identity"},
        ),
    }
    return rows[name]


def typed_document(name: str = "needs_review") -> dict:
    target, payload = operation_payload(name)
    return {
        "schema_version": "cs408-typed-operations-v1",
        "subject": "cs408",
        "capture_id": CAPTURE_ID,
        "verdict": "pass_with_warnings" if name == "needs_review" else "pass",
        "analysis_output_sha256": "a" * 64,
        "critical_review_output_sha256": "b" * 64,
        "stage_allowed_evidence_refs": {
            "analysis": [ANALYSIS_REF],
            "critical_review": [CRITICAL_REF],
        },
        "operations": [
            {
                "operation_id": "operation-" + name,
                "operation": name,
                "stage": "critical_review",
                "target": target,
                "payload": payload,
                "evidence_refs": [CRITICAL_REF],
                "counterevidence": [],
                "sol_verification_action": "Reopen exact evidence and decide.",
                "formal_write_count": 0,
            }
        ],
        "formal_write_count": 0,
    }


class Cs408R2SceneTypedOperationTests(unittest.TestCase):
    maxDiff = None

    def assert_code(self, expected: str, callback) -> None:
        with self.assertRaises(semantic.Cs408SemanticAdmissionError) as caught:
            callback()
        self.assertEqual(caught.exception.code, expected)

    def test_scene_exact_values_and_nested_forgery_fail_before_provider(self):
        for mutation, expected in (
            (lambda row: row.pop("scene"), "cs408_scene_envelope_shape_invalid"),
            (lambda row: row.__setitem__("scene", "Formal_Problem"), "cs408_scene_value_invalid"),
            (lambda row: row.__setitem__("scene", "formal_problem "), "cs408_scene_value_invalid"),
            (lambda row: row.__setitem__("scene", "FORMAL_PROBLEM"), "cs408_scene_value_invalid"),
            (lambda row: row.__setitem__("scene", "unknown"), "cs408_scene_value_invalid"),
            (lambda row: row.__setitem__("source_kind", "morning_review"), "cs408_scene_conflict"),
            (
                lambda row: row["evidence"].__setitem__(
                    "nested_forgery", {"scene": "formal_problem"}
                ),
                "cs408_scene_nested_forgery",
            ),
        ):
            with self.subTest(expected=expected):
                value = formal_envelope()
                mutation(value)
                self.assert_code(
                    expected,
                    lambda value=value: semantic.validate_scene_envelope(value),
                )

    def test_morning_observation_and_failure_capture_routes_are_exact(self):
        observation = semantic.build_scene_admission_receipt(morning_envelope())
        self.assertEqual(observation["status"], "ready")
        self.assertEqual(observation["admission_kind"], "observation_only")
        self.assertFalse(observation["item_formalization_allowed"])
        self.assertTrue(observation["model_enqueue_allowed"])
        for outcome in ("fragile_correct", "wrong", "partial", "blank", "uncertain"):
            with self.subTest(outcome=outcome):
                value = morning_envelope()
                value["evidence"]["first_answer"]["outcome"] = outcome
                receipt = semantic.build_scene_admission_receipt(value)
                self.assertEqual(receipt["admission_kind"], "failure_capture")
                self.assertTrue(receipt["item_formalization_allowed"])
        prompted = morning_envelope()
        prompted["evidence"]["first_answer"]["no_prompt"] = False
        self.assertEqual(
            semantic.build_scene_admission_receipt(prompted)["admission_kind"],
            "failure_capture",
        )
        missing_display = morning_envelope()
        missing_display["evidence"].pop("display_receipt_sha256")
        pending = semantic.build_scene_admission_receipt(missing_display)
        self.assertEqual(pending["status"], "evidence_pending")
        self.assertFalse(pending["model_enqueue_allowed"])
        corrected = morning_envelope()
        corrected["evidence"]["correction"]["occurred"] = True
        self.assert_code(
            "cs408_correction_trace_hash_invalid",
            lambda: semantic.build_scene_admission_receipt(corrected),
        )
        fabricated = morning_envelope()
        fabricated["evidence"]["correction"].update(
            {
                "trace_artifact_sha256": "c" * 64,
                "trace_artifact_ref": (
                    "study-intake-correction-trace://sha256/" + "c" * 64
                ),
            }
        )
        self.assert_code(
            "cs408_correction_trace_fabricated",
            lambda: semantic.build_scene_admission_receipt(fabricated),
        )

    def test_formal_problem_requires_images_answer_and_complete_dialogue(self):
        ready = semantic.build_scene_admission_receipt(formal_envelope())
        self.assertEqual(ready["status"], "ready")
        self.assertEqual(ready["scene"], "formal_problem")
        self.assertTrue(ready["model_enqueue_allowed"])
        self.assertEqual(ready["formal_write_count"], 0)
        for field in (
            "stable_question_identity",
            "question_surface_sha256",
            "question_image",
            "solution_image",
            "actual_answer",
            "full_dialogue",
        ):
            with self.subTest(missing=field):
                value = formal_envelope()
                value["evidence"].pop(field)
                receipt = semantic.build_scene_admission_receipt(value)
                self.assertEqual(receipt["status"], "evidence_pending")
                self.assertFalse(receipt["model_enqueue_allowed"])
        no_chunks = formal_envelope()
        no_chunks["evidence"]["full_dialogue"]["chunks"] = []
        pending = semantic.build_scene_admission_receipt(no_chunks)
        self.assertEqual(pending["status"], "evidence_pending")
        self.assertIn("full_dialogue.chunks", pending["missing_fields"])

    def test_formal_dialogue_chunk_tamper_matrix_fails_exactly(self):
        cases = []
        duplicate = formal_envelope()
        duplicate["evidence"]["full_dialogue"]["chunks"][1]["sha256"] = "6" * 64
        duplicate["evidence"]["full_dialogue"]["chunks"][1]["ref"] = (
            "study-intake-dialogue-chunk://sha256/" + "6" * 64
        )
        cases.append((duplicate, "cs408_dialogue_chunk_duplicate"))
        wrong_order = formal_envelope()
        wrong_order["evidence"]["full_dialogue"]["chunks"][1]["ordinal"] = 2
        cases.append((wrong_order, "cs408_dialogue_chunk_order_invalid"))
        cross_capture = formal_envelope()
        cross_capture["evidence"]["full_dialogue"]["chunks"][0]["capture_id"] = "OBS-OTHER"
        cases.append((cross_capture, "cs408_formal_cross_capture"))
        digest_drift = formal_envelope()
        digest_drift["evidence"]["full_dialogue"]["chunk_manifest_sha256"] = "f" * 64
        cases.append((digest_drift, "cs408_dialogue_digest_drift"))
        image_cross = formal_envelope()
        image_cross["evidence"]["question_image"]["capture_id"] = "OBS-OTHER"
        cases.append((image_cross, "cs408_formal_cross_capture"))
        for value, expected in cases:
            with self.subTest(expected=expected):
                self.assert_code(
                    expected,
                    lambda value=value: semantic.build_scene_admission_receipt(value),
                )

    def test_all_six_typed_operations_validate_individually(self):
        for name in sorted(semantic.TYPED_OPERATIONS):
            with self.subTest(operation=name):
                validated = semantic.validate_typed_operations(
                    typed_document(name)
                )
                self.assertEqual(validated["operations"][0]["operation"], name)
                self.assertEqual(validated["formal_write_count"], 0)

    def test_typed_operation_exact_guard_matrix(self):
        cases = []
        unknown = typed_document()
        unknown["operations"][0]["operation"] = "create_formal_node"
        cases.append((unknown, "cs408_typed_operation_unknown"))
        whitespace = typed_document()
        whitespace["operations"][0]["operation"] = "needs_review "
        cases.append((whitespace, "cs408_typed_operation_unknown"))
        cross_ref = typed_document()
        cross_ref["operations"][0]["stage"] = "analysis"
        cases.append((cross_ref, "cs408_typed_operation_refs_invalid"))
        bad_target = typed_document()
        bad_target["operations"][0]["target"] = "capture:OBS-OTHER"
        cases.append((bad_target, "cs408_typed_operation_target_invalid"))
        official_id = typed_document("propose_new_item")
        official_id["operations"][0]["payload"]["formal_id"] = "DS_9999_999"
        cases.append((official_id, "cs408_typed_operation_payload_invalid"))
        bad_relation = typed_document("propose_relation")
        bad_relation["operations"][0]["payload"]["formal_node_endpoint"] = "capture:OLD"
        cases.append((bad_relation, "cs408_typed_relation_endpoint_invalid"))
        duplicate_edge = typed_document("propose_relation")
        duplicate_edge["operations"][0]["payload"]["existing_edge"] = True
        cases.append((duplicate_edge, "cs408_typed_relation_payload_invalid"))
        formal_write = typed_document()
        formal_write["operations"][0]["payload"]["apply"] = True
        cases.append((formal_write, "cs408_typed_formal_write_forbidden"))
        duplicate = typed_document()
        duplicate["operations"].append(copy.deepcopy(duplicate["operations"][0]))
        cases.append((duplicate, "cs408_typed_operation_duplicate"))
        for value, expected in cases:
            with self.subTest(expected=expected):
                self.assert_code(
                    expected,
                    lambda value=value: semantic.validate_typed_operations(value),
                )

    def test_reject_is_terminal_and_cannot_publish_typed_package(self):
        rejected = typed_document()
        rejected["verdict"] = "reject"
        self.assert_code(
            "cs408_reject_typed_operations_forbidden",
            lambda: semantic.validate_typed_operations(rejected),
        )
        rejected["operations"] = []
        validated = semantic.validate_typed_operations(rejected)
        self.assertEqual(validated["operations"], [])
        receipt = semantic.build_scene_admission_receipt(formal_envelope())
        self.assert_code(
            "cs408_reject_package_forbidden",
            lambda: semantic.build_cs408_luna_proposal_v3(
                typed_operations=rejected,
                scene_admission_receipt=receipt,
            ),
        )

    def test_package_consumer_version_hash_and_unknown_field_are_exact(self):
        typed = typed_document("propose_relation")
        receipt = semantic.build_scene_admission_receipt(formal_envelope())
        proposal = semantic.build_cs408_luna_proposal_v3(
            typed_operations=typed,
            scene_admission_receipt=receipt,
        )
        consumer = semantic.reopen_cs408_sol_readonly_envelope(
            proposal,
            expected_capture_id=CAPTURE_ID,
        )
        self.assertEqual(consumer["status"], "ready_for_independent_decision")
        self.assertFalse(consumer["offer_token_created"])
        self.assertFalse(consumer["adoption_token_created"])
        self.assertFalse(consumer["sol_enabled"])
        self.assertEqual(consumer["formal_write_count"], 0)
        tampered = copy.deepcopy(proposal)
        tampered["typed_operations"]["operations"][0]["payload"]["strength"] = "weak"
        self.assert_code(
            "cs408_consumer_envelope_hash_invalid",
            lambda: semantic.reopen_cs408_sol_readonly_envelope(
                tampered, expected_capture_id=CAPTURE_ID
            ),
        )
        unknown = copy.deepcopy(proposal)
        unknown["unknown"] = True
        self.assert_code(
            "cs408_consumer_envelope_shape_invalid",
            lambda: semantic.reopen_cs408_sol_readonly_envelope(
                unknown, expected_capture_id=CAPTURE_ID
            ),
        )
        version = copy.deepcopy(proposal)
        version["consumer_contract_versions"] = ["cs408-sol-readonly-consumer-v1"]
        unsigned = dict(version)
        unsigned.pop("proposal_sha256")
        version["proposal_sha256"] = semantic.sha256_value(unsigned)
        self.assert_code(
            "cs408_consumer_envelope_contract_invalid",
            lambda: semantic.reopen_cs408_sol_readonly_envelope(
                version, expected_capture_id=CAPTURE_ID
            ),
        )


if __name__ == "__main__":
    unittest.main()
