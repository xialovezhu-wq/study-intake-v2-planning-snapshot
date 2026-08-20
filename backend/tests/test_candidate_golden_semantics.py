from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts import validate_candidate_golden_semantics as gate


def _write(path: Path, value: object, *, mode: int = 0o400) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    os.chmod(path, mode)
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


class CandidateGoldenSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="candidate-golden-")
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _math_analysis() -> dict[str, object]:
        return {
            "atomic_signals": [{"signal_id": "SIG-1"}],
            "truth_match_matrix": [{"signal_id": "SIG-1"}],
            "novel_knowledge_candidates": [],
            "unmatched_signals": [],
            "target_identity": {
                "formal_card_id": "GS-111",
                "delivered_card_id": "GS-111",
                "knowledge_fallback_card_id": "GS-655",
            },
            "sol_verification_plan": {
                "recommended_disposition": "update_existing_candidate"
            },
            "reasoning_diagnosis": {
                "first_break": {"kind": "coefficient_rule"},
                "later_breaks": [
                    {
                        "kind": "variable_role",
                        "business_semantic_code": (
                            "VARIABLE_ROLE_Y_FIXED_IN_SECOND_PARTIAL"
                        ),
                    }
                ],
                "independent_correct_steps": [{"kind": "first_derivative"}],
                "hint_dependencies": [{"kind": "hint"}],
                "self_corrections": [{"kind": "guided"}],
            },
            "formalization_candidates": {"mastery_evidence": []},
        }

    def test_gs111_requires_machine_readable_fixed_y_boundary(self) -> None:
        analysis = self._math_analysis()
        report = {"target_identity": analysis["target_identity"]}
        critical = {"revised_analysis": analysis}
        assertions = gate._validate_math_replay(
            "GS-111", report, critical, {"propose_item_update"}
        )
        self.assertIn("proposal_only", assertions)
        broken = copy.deepcopy(analysis)
        broken["reasoning_diagnosis"]["later_breaks"][0].pop(
            "business_semantic_code"
        )
        with self.assertRaisesRegex(
            gate.GoldenSemanticError,
            "golden_math_gs111_fixed_y_machine_field_missing",
        ):
            gate._validate_math_replay(
                "GS-111",
                report,
                {"revised_analysis": broken},
                {"propose_item_update"},
            )

    @staticmethod
    def _free_space_analysis() -> dict[str, object]:
        edges = [
            ("R05 易混概念对比", "DS_2017_003", "OS_UNK_074"),
            ("R06 上下游知识链", "OS_2014_004", "OS_UNK_074"),
            ("R06 上下游知识链", "OS_2015_003", "OS_UNK_074"),
            ("R05 易混概念对比", "OS_2015_003", "OS_UNK_075"),
            ("R06 上下游知识链", "OS_UNK_074", "OS_UNK_075"),
            ("R04 同一模糊概念", "OS_UNK_074", "OS_UNK_082"),
        ]
        return {
            "candidate_schema_version": "study-intake-candidate-v3",
            "coverage_manifest": {"complete": True},
            "unmatched_signals": [],
            "atomic_signals": [
                {"signal_id": "K1", "knowledge_id": "OS04-25"},
                {"signal_id": "K2", "knowledge_id": "OS04-28"},
            ],
            "existing_formal_edges": [
                {
                    "relationship_type": relation,
                    "start_id": start,
                    "end_id": end,
                    "source_edge_status": "existing_formal_edge",
                    "current_capture_action_status": "proposal_only",
                }
                for relation, start, end in edges
            ],
            "new_relation_proposals": [],
            "current_error_points": [],
            "formalization_candidates": {"mastery_evidence": []},
        }

    def test_free_space_requires_exact_six_projection_edges(self) -> None:
        analysis = self._free_space_analysis()
        assertions = gate._validate_cs408(
            "FREE_SPACE",
            analysis,
            {"revised_analysis": analysis, "correction_resolutions": []},
            {"needs_review"},
        )
        self.assertIn("existing_edges_are_projection", assertions)
        broken = copy.deepcopy(analysis)
        broken["existing_formal_edges"].pop()
        with self.assertRaisesRegex(
            gate.GoldenSemanticError, "golden_cs408_exact_edge_set_invalid"
        ):
            gate._validate_cs408(
                "FREE_SPACE",
                broken,
                {"revised_analysis": broken, "correction_resolutions": []},
                {"needs_review"},
            )

    @staticmethod
    def _english_analysis() -> tuple[dict[str, object], list[str]]:
        source_ids = [
            "EVT-20260806-507660196D4F941D",
            "EVT-20260806-8F4D09BF50F3F1B3",
            "EVT-20260806-4DF6447806282008",
        ]
        terms = [
            ("pregnant", source_ids[0], "unknown"),
            ("entertaining", source_ids[0], "unknown"),
            ("permanent", source_ids[1], "unknown"),
            ("gossip", source_ids[1], "unknown"),
            ("V-ing phrase as subject", source_ids[2], "structure_unresolved"),
            ("be highly valued by", source_ids[2], "mistranslated"),
        ]
        signals = [
            {
                "signal_id": f"SIG-{index}",
                "canonical_term": term,
                "source_event_id": event,
                "knowledge_state": state,
            }
            for index, (term, event, state) in enumerate(terms, start=1)
        ]
        analysis = {
            "capture_event_ids": source_ids,
            "sentence_records": [
                {
                    "source_event_id": event,
                    "source_sentence": f"source sentence {index}",
                    "user_first_translation": f"first translation {index}",
                    "corrected_meaning": f"corrected meaning {index}",
                    "user_evidence_verbatim": f"verbatim {index}",
                }
                for index, event in enumerate(source_ids, start=1)
            ],
            "observed_signals": signals,
            "signal_outcomes": [
                {"signal_id": row["signal_id"], "terminal_status": "candidate"}
                for row in signals
            ],
            "capture_coverage": {
                "declared_signal_count": 6,
                "covered_signal_count": 6,
                "unmatched_signal_ids": [],
            },
            "items": [
                {
                    "item": term,
                    "candidate_status": "familiarity_candidate",
                    "mastery_proposal": None,
                    "bank_status": (
                        "new_candidate"
                        if term == "V-ing phrase as subject"
                        else "existing_bank"
                    ),
                    "card": {"old_word_example": f"different example {index}"},
                }
                for index, (term, _event, _state) in enumerate(terms, start=1)
            ],
            "review_exclusion_proposals": [
                {
                    "proposal_type": "review_exclusion_proposal",
                    "item": "celebrities",
                    "bank_id": "20260424-204",
                    "day_conflict_check": {
                        "event_count_scanned": 3,
                        "explicit_unknown_seen_anywhere_in_day": False,
                    },
                }
            ],
            "reactivation_proposals": [],
            "needs_user_decision": [],
            "formal_writeback": "none",
            "formal_write_count": 0,
        }
        return analysis, source_ids

    def test_english_microbatch_enforces_six_signals_and_no_reactivation(self) -> None:
        analysis, source_ids = self._english_analysis()
        assertions = gate._validate_english(analysis, source_ids, {"needs_review"})
        self.assertIn("six_of_six_signals_closed", assertions)
        analysis["reactivation_proposals"] = [{"item": "pregnant"}]
        with self.assertRaisesRegex(
            gate.GoldenSemanticError, "golden_english_unauthorized_reactivation"
        ):
            gate._validate_english(analysis, source_ids, {"needs_review"})

    def test_quality_receipt_hmac_is_verified(self) -> None:
        key = b"k" * 32
        receipt = {
            "schema_version": "subject_quality_receipt_v1",
            "batch_id": "BATCH-1",
            "subject": "math",
            "capture_id": "CAP-1",
            "unit_sha256": "1" * 64,
            "frozen_payload_sha256": "2" * 64,
            "completion_sha256": "3" * 64,
            "dispatch_receipt_sha256": "4" * 64,
            "dispatch_package_sha256": "5" * 64,
            "capture_freeze_receipt_sha256": "6" * 64,
            "mcp_read_session_receipt_sha256": "7" * 64,
            "analysis_output_sha256": "8" * 64,
            "analysis_mcp_transcript_sha256": "9" * 64,
            "critical_review_output_sha256": "a" * 64,
            "critical_review_mcp_transcript_sha256": "b" * 64,
            "draft_sha256": "8" * 64,
            "review_outcome": "corrected",
            "proposal_sha256": "c" * 64,
            "package_sha256": "d" * 64,
            "authority": {
                "generation": "GEN-1",
                "authority_fingerprint": "e" * 64,
            },
            "model_call_count": 2,
            "formal_write_count": 0,
            "issued_at": "2026-08-09T00:00:00Z",
        }
        receipt["seal"] = {
            "algorithm": "HMAC-SHA256",
            "purpose": "subject-quality-receipt",
            "hmac_sha256": gate._seal(key, "subject-quality-receipt", receipt),
        }
        gate._verify_quality_hmac(receipt, key)
        receipt["seal"]["hmac_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            gate.GoldenSemanticError, "golden_semantic_quality_hmac_invalid"
        ):
            gate._verify_quality_hmac(receipt, key)

    def test_processing_binding_is_self_hashed_and_subject_exact(self) -> None:
        core = {
            "schema_version": "processing_binding_v2",
            "base_release_id": "0" * 64,
            "candidate_release_id": "a" * 64,
            "plugin": {"id": "kaoyan-study-intake", "version": "1.0.0", "sha256": "1" * 64},
            "skill": {"id": "background-cs408-processing", "version": "3.0.0", "sha256": "2" * 64},
            "mcp": {
                "id": "kaoyan_cs408_read",
                "version": "0.3.0+sha256." + "3" * 64,
                "sha256": "3" * 64,
                "profile": "luna",
                "subject": "cs408",
            },
            "schemas": {"provider_sha256": "4" * 64, "canonical_sha256": "5" * 64},
            "validator_sha256": "6" * 64,
            "host_semantic_prefetch": False,
            "formal_write_count": 0,
        }
        binding = {**core, "binding_sha256": gate._value_sha256(core)}
        package = {
            "processing_binding": binding,
            "processing_binding_sha256": binding["binding_sha256"],
        }
        gate._processing_binding(package, subject="cs408", release_id="a" * 64)
        package["processing_binding"]["skill"]["id"] = "background-math-processing"
        with self.assertRaisesRegex(
            gate.GoldenSemanticError, "golden_semantic_processing_binding_invalid"
        ):
            gate._processing_binding(package, subject="cs408", release_id="a" * 64)

    def test_transcript_requires_stage_local_task_and_library_reads(self) -> None:
        release_id = "a" * 64
        generation = "GEN-1"
        authority = "b" * 64
        calls = []
        for sequence, tool in enumerate(
            ("get_task_context", "read_task_artifact", "query_relations"), start=1
        ):
            arguments = {"capture_id": "CAP-1", "sequence": sequence}
            result = {
                "formal_write_count": 0,
                "model_call_count": 0,
                "generation": generation,
                "authority_fingerprint": authority,
                "read_session": {"candidate_release_id": release_id},
                "read_route": {
                    "read_route": "mcp_model_driven",
                    "consumed_duplicate_read_count": 0,
                },
            }
            calls.append(
                {
                    "sequence": sequence,
                    "tool": tool,
                    "arguments": arguments,
                    "arguments_sha256": gate._value_sha256(arguments),
                    "result": result,
                    "result_sha256": gate._value_sha256(result),
                }
            )
        transcript = {
            "schema_version": "model-driven-mcp-stage-transcript-v1",
            "subject": "math",
            "stage_name": "math_analysis",
            "generation": generation,
            "authority_fingerprint": authority,
            "calls": calls,
            "mcp_tool_call_count": 3,
            "provider_request_count": 1,
            "model_call_count": 1,
            "formal_write_count": 0,
        }
        gate._transcript(
            transcript,
            subject="math",
            stage="analysis",
            release_id=release_id,
            generation=generation,
            authority=authority,
        )
        broken = copy.deepcopy(transcript)
        broken["calls"][1]["tool"] = "get_records"
        broken["calls"][1]["result_sha256"] = gate._value_sha256(
            broken["calls"][1]["result"]
        )
        with self.assertRaisesRegex(
            gate.GoldenSemanticError,
            "golden_semantic_stage_evidence_reread_incomplete",
        ):
            gate._transcript(
                broken,
                subject="math",
                stage="analysis",
                release_id=release_id,
                generation=generation,
                authority=authority,
            )

    def test_math3_spec_cannot_pass_math6_gate(self) -> None:
        release_id = "a" * 64
        math = _write(
            self.root / "math.json",
            {
                "schema_version": gate.CONTROLLED_SPEC_SCHEMA,
                "release_id": release_id,
                "formal_write_count": 0,
                "entries": [
                    {
                        "subject": "math",
                        "capture_id": role,
                        "sample_role": role,
                        "expected_input_fingerprint": "1" * 64,
                    }
                    for role in ("GS-111", "GS-240", "complete-new-intake")
                ],
            },
        )
        cs = _write(
            self.root / "cs.json",
            {
                "schema_version": gate.CONTROLLED_SPEC_SCHEMA,
                "release_id": release_id,
                "formal_write_count": 0,
                "entries": [
                    {
                        "subject": "cs408",
                        "capture_id": role,
                        "sample_role": role,
                        "expected_input_fingerprint": "2" * 64,
                    }
                    for role in sorted(gate.CS408_ROLES)
                ],
            },
        )
        english = _write(
            self.root / "english.json",
            {
                "schema_version": gate.CONTROLLED_SPEC_SCHEMA,
                "release_id": release_id,
                "formal_write_count": 0,
                "entries": [
                    {
                        "subject": "english",
                        "capture_id": role,
                        "sample_role": role,
                        "expected_input_fingerprint": "3" * 64,
                    }
                    for role in sorted(gate.ENGLISH_SOURCE_ROLES)
                ],
            },
        )
        key_path = self.root / "authority.key"
        key_path.write_bytes(b"authority" * 4)
        os.chmod(key_path, 0o600)
        evidence = {
            "schema_version": gate.EVIDENCE_SCHEMA,
            "candidate_release_id": release_id,
            "authority_key_sha256": hashlib.sha256(key_path.read_bytes()).hexdigest(),
            "controlled_replay_specs": {
                "math": math,
                "cs408": cs,
                "english": english,
            },
            "math_live_business_manifest": {
                "path": str((self.root / "not-read.json").resolve()),
                "sha256": "f" * 64,
            },
            "tasks": [],
            "formal_write_count": 0,
        }
        evidence_path = self.root / "evidence.json"
        evidence_path.write_text(json.dumps(evidence) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(
            gate.GoldenSemanticError, "golden_semantic_math6_spec_not_executable"
        ):
            gate.validate(evidence_path, key_path, self.root / "output")


if __name__ == "__main__":
    unittest.main()
