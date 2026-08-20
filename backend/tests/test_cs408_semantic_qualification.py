from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "lib"
if str(LIB) not in sys.path:
    sys.path.insert(0, str(LIB))

import cs408_semantic_qualification as qualification  # noqa: E402


FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "e8a_cs408_required_correction"
CAPTURE_ID = "OBS-91DC4259E9E0BC1996B01884"


def load(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def physical_sha256(name: str) -> str:
    return hashlib.sha256((FIXTURE_ROOT / name).read_bytes()).hexdigest()


def inputs() -> dict:
    return {
        "capture_id": CAPTURE_ID,
        "analysis_stage_output": load("analysis-stage-output.json"),
        "critical_review_stage_output": load(
            "critical-review-stage-output.json"
        ),
        "analysis_transcript": load("analysis-transcript.json"),
        "critical_review_transcript": load(
            "critical-review-transcript.json"
        ),
        "source_physical_sha256s": {
            "analysis_output": physical_sha256("analysis-stage-output.json"),
            "critical_review_output": physical_sha256(
                "critical-review-stage-output.json"
            ),
            "analysis_transcript": physical_sha256("analysis-transcript.json"),
            "critical_review_transcript": physical_sha256(
                "critical-review-transcript.json"
            ),
        },
        "fresh_context_proof": {
            "schema_version": "cs408-fresh-critical-context-proof-v1",
            "each_stage_uses_ephemeral_exec": True,
            "checkpoint_resume_starts_execute_prompt": True,
            "provider_context_resume_token_absent": True,
            "proof_artifact_sha256": "f" * 64,
        },
        "delivery_status": "validator_failed",
        "delivery_detail": "validator_failed_before_terminalization",
        "format_warnings": [],
        "blocking_delivery_reasons": [
            "critical_review_required_correction_unresolved",
            "capture_identity_conflict",
        ],
        "legacy_failure_code": (
            "critical_review_required_correction_unresolved"
        ),
    }


def rehash_call(call: dict) -> None:
    call["arguments_sha256"] = qualification.content_sha256(call["arguments"])
    call["result_sha256"] = qualification.content_sha256(call["result"])


def remove_identity_structures(value):
    if isinstance(value, list):
        return [
            remove_identity_structures(row)
            for row in value
            if not (
                isinstance(row, dict)
                and (
                    row.get("signal_id") == "sig-identity-conflict"
                    or row.get("canonical_term") == "当前题目身份绑定冲突"
                    or str(row.get("finding_id", "")).startswith(
                        "CR-IDENTITY-"
                    )
                )
            )
        ]
    if isinstance(value, dict):
        return {
            key: remove_identity_structures(nested)
            for key, nested in value.items()
        }
    return value


def nonreject_inputs(status: str) -> dict:
    value = inputs()
    value["analysis_stage_output"]["payload"] = remove_identity_structures(
        value["analysis_stage_output"]["payload"]
    )
    value["critical_review_stage_output"]["payload"] = (
        remove_identity_structures(
            value["critical_review_stage_output"]["payload"]
        )
    )
    value["critical_review_stage_output"]["payload"]["verdict"] = (
        "pass_with_warnings" if status == "corrected" else "pass"
    )
    value["delivery_status"] = status
    value["delivery_detail"] = "synthetic_production_shape_zero_model"
    value["blocking_delivery_reasons"] = []
    value["legacy_failure_code"] = None
    return value


def replace_first_string(value, old: str, new: str) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if nested == old:
                value[key] = new
                return True
            if replace_first_string(nested, old, new):
                return True
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            if nested == old:
                value[index] = new
                return True
            if replace_first_string(nested, old, new):
                return True
    return False


class Cs408SemanticQualificationTests(unittest.TestCase):
    maxDiff = None

    def assert_code(self, expected: str, callback) -> None:
        with self.assertRaises(
            qualification.Cs408SemanticQualificationError
        ) as caught:
            callback()
        self.assertEqual(caught.exception.code, expected)

    def build(self, value: dict | None = None) -> dict:
        return qualification.build_cs408_semantic_qualification_receipt(
            **(inputs() if value is None else value)
        )

    def test_e8a_is_semantically_qualified_but_delivery_validator_failed(self):
        receipt = self.build()
        self.assertEqual(receipt["semantic_analysis_status"], "qualified")
        self.assertEqual(receipt["delivery_status"], "validator_failed")
        self.assertEqual(
            receipt["delivery_detail"],
            "validator_failed_before_terminalization",
        )
        self.assertEqual(receipt["critical_review_verdict"], "reject")
        self.assertEqual(receipt["intended_delivery_outcome"], "rejected")
        self.assertTrue(receipt["identity_conflict_present"])
        self.assertFalse(receipt["executable_delivery_allowed"])
        self.assertEqual(receipt["format_warnings"], [])
        self.assertEqual(
            receipt["analysis_output_sha256"],
            "af94f2db52214d8083200e5d257c83db5d38adcc6f64f7ffb4578b8671410f3a",
        )
        self.assertEqual(
            receipt["critical_review_output_sha256"],
            "0a9e532a9fbb94b6f7baf9fc1818001322e0b97806a3e7ff1823b75c3cc44d60",
        )
        self.assertTrue(all(receipt["semantic_coverage"].values()))
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertTrue(receipt["sol_disabled"])

    def test_legal_reject_is_qualified_rejected_and_non_executable(self):
        value = inputs()
        value["delivery_status"] = "rejected"
        value["delivery_detail"] = "needs_rework_terminalized"
        value["legacy_failure_code"] = None
        value["blocking_delivery_reasons"] = ["capture_identity_conflict"]
        receipt = self.build(value)
        self.assertEqual(receipt["semantic_analysis_status"], "qualified")
        self.assertEqual(receipt["delivery_status"], "rejected")
        self.assertFalse(receipt["executable_delivery_allowed"])

    def test_accepted_and_corrected_are_separate_executable_states(self):
        for status in ("accepted", "corrected"):
            with self.subTest(status=status):
                receipt = self.build(nonreject_inputs(status))
                self.assertEqual(
                    receipt["semantic_analysis_status"], "qualified"
                )
                self.assertEqual(receipt["delivery_status"], status)
                self.assertTrue(receipt["executable_delivery_allowed"])
                self.assertFalse(receipt["identity_conflict_present"])

    def test_identity_conflict_must_reject_and_reject_cannot_be_accepted(self):
        value = inputs()
        value["critical_review_stage_output"]["payload"]["verdict"] = "pass"
        self.assert_code(
            "cs408_semantic_identity_conflict_not_rejected",
            lambda: self.build(value),
        )
        value = inputs()
        value["delivery_status"] = "accepted"
        self.assert_code(
            "cs408_semantic_reject_delivery_invalid",
            lambda: self.build(value),
        )

    def test_harmless_formula_and_markdown_rendering_are_warnings_only(self):
        value = nonreject_inputs("accepted")
        same_semantics = "1" * 64
        value["format_warnings"] = [
            {
                "json_path": "$.formalization_candidates.key_parameters[0].text",
                "warning_code": "latex_render",
                "message": "Delimiter renders imperfectly; parsed meaning is unchanged.",
                "raw_fragment_sha256": "2" * 64,
                "semantic_sha256_before": same_semantics,
                "semantic_sha256_after": same_semantics,
                "machine_parseable": True,
                "structural_semantics_unchanged": True,
            },
            {
                "json_path": "$.executive_summary",
                "warning_code": "markdown_render",
                "message": "Markdown layout differs; structured fields are unchanged.",
                "raw_fragment_sha256": "3" * 64,
                "semantic_sha256_before": "4" * 64,
                "semantic_sha256_after": "4" * 64,
                "machine_parseable": True,
                "structural_semantics_unchanged": True,
            },
        ]
        receipt = self.build(value)
        self.assertEqual(receipt["semantic_analysis_status"], "qualified")
        self.assertEqual(receipt["delivery_status"], "accepted")
        self.assertEqual(len(receipt["format_warnings"]), 2)

    def test_format_warning_cannot_hide_structural_or_semantic_change(self):
        value = nonreject_inputs("accepted")
        warning = {
            "json_path": "$.executive_summary",
            "warning_code": "markdown_render",
            "message": "not harmless",
            "raw_fragment_sha256": "2" * 64,
            "semantic_sha256_before": "3" * 64,
            "semantic_sha256_after": "4" * 64,
            "machine_parseable": True,
            "structural_semantics_unchanged": True,
        }
        value["format_warnings"] = [warning]
        self.assert_code(
            "cs408_semantic_format_warning_changes_semantics",
            lambda: self.build(value),
        )
        warning["semantic_sha256_after"] = "3" * 64
        warning["machine_parseable"] = False
        self.assert_code(
            "cs408_semantic_format_warning_not_harmless",
            lambda: self.build(value),
        )

    def test_invalid_json_shape_and_transcript_tamper_fail_closed(self):
        value = inputs()
        value["analysis_stage_output"] = "{not-json"
        self.assert_code(
            "cs408_semantic_stage_output_invalid",
            lambda: self.build(value),
        )
        value = inputs()
        value["analysis_transcript"]["calls"][0]["result"]["ok"] = False
        self.assert_code(
            "cs408_semantic_transcript_content_hash_invalid",
            lambda: self.build(value),
        )

    def test_stage_local_mcp_and_analysis_refs_are_exact_membership(self):
        value = inputs()
        analysis_refs = qualification._payload_mcp_refs(
            value["analysis_stage_output"]["payload"]
        )
        critical_refs = qualification._payload_mcp_refs(
            value["critical_review_stage_output"]["payload"]
        )
        analysis_only = sorted(analysis_refs - critical_refs)[0]
        critical_ref = sorted(critical_refs)[0]
        self.assertTrue(
            replace_first_string(
                value["critical_review_stage_output"]["payload"],
                critical_ref,
                analysis_only,
            )
        )
        self.assert_code(
            "cs408_semantic_stage_ref_membership_invalid",
            lambda: self.build(value),
        )
        value = inputs()
        payload = value["critical_review_stage_output"]["payload"]
        ref = payload["answer_safety_findings"][0]["evidence_refs"][0]
        payload["answer_safety_findings"][0]["evidence_refs"][0] = (
            ref[:-1] + ref[-1].upper()
        )
        self.assert_code(
            "cs408_semantic_mcp_ref_invalid",
            lambda: self.build(value),
        )
        value = inputs()
        value["critical_review_stage_output"]["payload"][
            "answer_safety_findings"
        ][0]["analysis_refs"][0] += " "
        self.assert_code(
            "cs408_semantic_analysis_ref_membership_invalid",
            lambda: self.build(value),
        )

    def test_missing_artifact_pagination_or_relation_is_unqualified_blocked(self):
        cases: list[tuple[str, dict]] = []
        missing_artifact = inputs()
        call = missing_artifact["analysis_transcript"]["calls"][1]
        call["arguments"]["artifact_id"] = "different-artifact"
        rehash_call(call)
        cases.append(("analysis_task_artifacts_complete", missing_artifact))

        pagination = inputs()
        pagination["critical_review_transcript"]["coverage"][
            "all_returned_pages_consumed"
        ] = False
        cases.append(("critical_review_pagination_complete", pagination))

        no_relations = inputs()
        for call in no_relations["critical_review_transcript"]["calls"]:
            changed = False
            for nested in qualification._walk(call["result"]["items"]):
                if not isinstance(nested, dict):
                    continue
                role = nested.get("data_role")
                if isinstance(role, str) and "relation" in role:
                    nested["data_role"] = "formal_fact"
                    changed = True
                if nested.get("collection") == "relations":
                    nested["collection"] = "formal_facts"
                    changed = True
            if changed:
                rehash_call(call)
        cases.append(("critical_review_relation_lookup_complete", no_relations))

        for missing_check, value in cases:
            with self.subTest(missing_check=missing_check):
                value["delivery_status"] = "blocked"
                value["delivery_detail"] = "semantic_coverage_incomplete"
                receipt = self.build(value)
                self.assertEqual(
                    receipt["semantic_analysis_status"], "unqualified"
                )
                self.assertEqual(receipt["delivery_status"], "blocked")
                self.assertFalse(
                    receipt["semantic_coverage"][missing_check]
                )
                self.assertFalse(receipt["executable_delivery_allowed"])

    def test_unqualified_cannot_be_promoted_to_accepted(self):
        value = nonreject_inputs("accepted")
        value["fresh_context_proof"][
            "provider_context_resume_token_absent"
        ] = False
        self.assert_code(
            "cs408_semantic_unqualified_delivery_invalid",
            lambda: self.build(value),
        )

    def test_receipt_reopens_exactly_and_tamper_fails(self):
        receipt = self.build()
        reopened = qualification.reopen_cs408_semantic_qualification_receipt(
            receipt
        )
        self.assertEqual(reopened, receipt)
        tampered = copy.deepcopy(receipt)
        tampered["delivery_status"] = "accepted"
        self.assert_code(
            "cs408_semantic_receipt_hash_invalid",
            lambda: qualification.reopen_cs408_semantic_qualification_receipt(
                tampered
            ),
        )

    def test_zero_model_test_has_no_runtime_side_effect_contract(self):
        source = (LIB / "cs408_semantic_qualification.py").read_text(
            encoding="utf-8"
        )
        for forbidden in (
            "subprocess",
            "kaoyan_math",
            "kaoyan_english",
            "formal_write_count\": 1",
            "sol_enabled\": true",
        ):
            self.assertNotIn(forbidden, source)
        receipt = self.build()
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertTrue(receipt["sol_disabled"])


if __name__ == "__main__":
    unittest.main()
