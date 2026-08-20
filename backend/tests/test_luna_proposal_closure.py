#!/usr/bin/env python3

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import preprocessor_core as core  # noqa: E402


class _ReopenHost:
    def __init__(self, stage_calls: dict[str, list[dict]]) -> None:
        self.stage_calls = stage_calls

    def reopen_published_read_session(self, **_: object) -> dict:
        return {
            "context": {},
            "stage_calls": copy.deepcopy(self.stage_calls),
        }


class LunaProposalClosureTests(unittest.TestCase):
    def _call(self, *, subject: str, ref: str) -> dict:
        source_hash = "a" * 64
        return {
            "sequence": 1,
            "arguments": {"collection": "catalog"},
            "result_sha256": "b" * 64,
            "result": {
                "subject": subject,
                "generation": "generation-1",
                "items": [
                    {
                        "collection": "catalog",
                        "stable_id": "item-1",
                        "source_hash": source_hash,
                        "data_role": "formal_card",
                        "evidence_ref": ref,
                    }
                ],
            },
        }

    def _grounding(self, *, transcript_sha256: str, call: dict) -> dict:
        stage = core.StructuredStageResult(
            payload={},
            duration_ms=0,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256="0" * 64,
            mcp_transcript_sha256=transcript_sha256,
            mcp_calls=(copy.deepcopy(call),),
        )
        return core.mcp_grounding_manifest((stage,))

    def _fixture(self, *, subject: str = "math") -> tuple[dict, dict, dict]:
        ref = f"mcp-item:{subject}:" + "c" * 64
        call = self._call(subject=subject, ref=ref)
        analysis_transcript = "d" * 64
        review_transcript = "e" * 64
        analysis_grounding = self._grounding(
            transcript_sha256=analysis_transcript, call=call
        )
        review_grounding = self._grounding(
            transcript_sha256=review_transcript, call=call
        )
        stage_receipts = {
            "analysis": {
                "mcp_transcript_sha256": analysis_transcript,
                "mcp_grounding_manifest": analysis_grounding,
                "mcp_grounding_manifest_sha256": analysis_grounding[
                    "manifest_sha256"
                ],
            },
            "critical_review": {
                "mcp_transcript_sha256": review_transcript,
                "mcp_grounding_manifest": review_grounding,
                "mcp_grounding_manifest_sha256": review_grounding[
                    "manifest_sha256"
                ],
            },
            "read_session": {},
        }
        publication = {
            "processing_binding": {"schema_version": "binding-v2"},
            "processing_binding_sha256": "1" * 64,
            "capture_freeze_receipt_sha256": "2" * 64,
            "capture_freeze_receipt_ref": (
                "study-intake-capture-freeze://sha256/" + "2" * 64
            ),
            "mcp_read_session_receipt_sha256": "3" * 64,
            "mcp_read_session_receipt_ref": (
                "study-intake-mcp-read-session://sha256/" + "3" * 64
            ),
            "read_session_id": "read-session-0001",
            "read_session_manifest_sha256": "4" * 64,
            "authority_snapshot_manifest_sha256": "a" * 64,
            "evidence_generation": "generation-1",
            "evidence_authority_fingerprint": "5" * 64,
            "host_semantic_prefetch": False,
            "mcp_stage_transcripts": {
                "analysis": {
                    "transcript_sha256": analysis_transcript,
                    "grounding_manifest_sha256": analysis_grounding[
                        "manifest_sha256"
                    ],
                },
                "critical_review": {
                    "transcript_sha256": review_transcript,
                    "grounding_manifest_sha256": review_grounding[
                        "manifest_sha256"
                    ],
                },
            },
            "semantic_stage_count": 2,
            "provider_request_count": 4,
            "mcp_tool_call_count": 2,
            "model_call_count": 2,
            "consumed_terminal_duplicate_read_count": 0,
        }
        subject_payload_sha256 = "6" * 64
        operation = {
            "operation": "needs_review",
            "target": "capture:capture-1",
            "proposal": {
                "subject_payload_sha256": subject_payload_sha256,
                "subject_payload_role": "critical_review_revised",
                "host_semantic_projection": "none",
            },
            "evidence_refs": [ref],
            "sol_verification_action": "Reopen canonical evidence.",
        }
        proposal = {
            "schema_version": "luna_proposal_v2",
            "subject": subject,
            "capture_id": "capture-1",
            "processing_binding_sha256": publication[
                "processing_binding_sha256"
            ],
            "capture_freeze_receipt_sha256": publication[
                "capture_freeze_receipt_sha256"
            ],
            "capture_freeze_receipt_ref": publication[
                "capture_freeze_receipt_ref"
            ],
            "read_session_id": publication["read_session_id"],
            "read_session_manifest_sha256": publication[
                "read_session_manifest_sha256"
            ],
            "mcp_read_session_receipt_sha256": publication[
                "mcp_read_session_receipt_sha256"
            ],
            "mcp_read_session_receipt_ref": publication[
                "mcp_read_session_receipt_ref"
            ],
            "mcp_stage_transcript_sha256s": [
                analysis_transcript,
                review_transcript,
            ],
            "mcp_grounding_manifest_sha256s": [
                analysis_grounding["manifest_sha256"],
                review_grounding["manifest_sha256"],
            ],
            "consumed_mcp_evidence_refs": [ref],
            "operations": [operation],
            "review_status": "proposal_ready",
            "critical_review_outcome": "accepted",
            "host_semantic_prefetch": False,
            "formal_write_count": 0,
        }
        package = {
            "subject": subject,
            "capture_id": "capture-1",
            "draft_analysis": (
                {"items": []} if subject == "english" else {"value": 1}
            ),
            "critical_review": (
                {"verdict": "confirmed"}
                if subject == "english"
                else {
                    "verdict": "pass_with_warnings",
                    "revised_analysis": {"value": 1},
                }
            ),
            "stage_receipts": stage_receipts,
            "allowed_evidence_refs": [ref],
            "luna_proposal": proposal,
            "luna_proposal_sha256": core.sha256_value(proposal),
            **copy.deepcopy(publication),
        }
        stage_calls = {
            "analysis": [copy.deepcopy(call)],
            "critical_review": [copy.deepcopy(call)],
        }
        return package, publication, stage_calls

    def test_host_validator_rejects_nested_drift_and_duplicate_operations(self) -> None:
        package, _, _ = self._fixture()
        proposal = package["luna_proposal"]
        core.validate_luna_proposal_v2(proposal)
        mutations = []

        nested_extra = copy.deepcopy(proposal)
        nested_extra["operations"][0]["proposal"]["extra"] = True
        mutations.append(nested_extra)

        wrong_role = copy.deepcopy(proposal)
        wrong_role["operations"][0]["proposal"][
            "subject_payload_role"
        ] = "analysis_draft"
        mutations.append(wrong_role)

        wrong_subject_ref = copy.deepcopy(proposal)
        wrong_subject_ref["operations"][0]["evidence_refs"] = [
            "mcp-item:cs408:" + "c" * 64
        ]
        mutations.append(wrong_subject_ref)

        duplicate = copy.deepcopy(proposal)
        duplicate["operations"].append(copy.deepcopy(duplicate["operations"][0]))
        mutations.append(duplicate)

        payload_drift = copy.deepcopy(proposal)
        second = copy.deepcopy(payload_drift["operations"][0])
        second["operation"] = "propose_new_item"
        second["proposal"]["subject_payload_sha256"] = "7" * 64
        payload_drift["operations"].append(second)
        mutations.append(payload_drift)

        outcome_drift = copy.deepcopy(proposal)
        outcome_drift["critical_review_outcome"] = "rejected"
        mutations.append(outcome_drift)

        for value in mutations:
            with self.subTest(value=value), self.assertRaises(core.PreprocessorError):
                core.validate_luna_proposal_v2(value)

    def test_host_critic_outcome_uses_subject_semantics(self) -> None:
        draft = {"items": [{"value": 1}]}
        for verdict, expected in (
            ("confirmed", "accepted"),
            ("revised", "corrected"),
            ("reject", "rejected"),
        ):
            with self.subTest(subject="english", verdict=verdict):
                self.assertEqual(
                    core.derive_critical_review_outcome(
                        subject="english",
                        draft_analysis=draft,
                        critical_review={"verdict": verdict},
                    ),
                    expected,
                )

        for subject in ("math", "cs408"):
            with self.subTest(subject=subject, case="pass_with_warnings_equal"):
                self.assertEqual(
                    core.derive_critical_review_outcome(
                        subject=subject,
                        draft_analysis=draft,
                        critical_review={
                            "verdict": "pass_with_warnings",
                            "revised_analysis": copy.deepcopy(draft),
                        },
                    ),
                    "accepted",
                )
            with self.subTest(subject=subject, case="pass_changed"):
                self.assertEqual(
                    core.derive_critical_review_outcome(
                        subject=subject,
                        draft_analysis=draft,
                        critical_review={
                            "verdict": "pass",
                            "revised_analysis": {"items": [{"value": 2}]},
                        },
                    ),
                    "corrected",
                )
            with self.subTest(subject=subject, case="reject"):
                self.assertEqual(
                    core.derive_critical_review_outcome(
                        subject=subject,
                        draft_analysis=draft,
                        critical_review={"verdict": "reject"},
                    ),
                    "rejected",
                )

    def test_builder_publishes_ready_or_rejected_from_critic_outcome(self) -> None:
        package, publication, _ = self._fixture()
        ref = package["allowed_evidence_refs"][0]
        candidate = core.Candidate(
            subject="math",
            capture_id="capture-1",
            study_date="2026-08-09",
            recorded_at=None,
            input_fingerprint="7" * 64,
            input_binding={},
            model_input={},
            allowed_evidence_refs=(ref,),
            image_paths=(),
            target_label="capture-1",
            canonical_state="test",
            sol_state="test",
        )
        draft = {"claims": [{"evidence_ref": ref, "value": 1}]}
        cases = (
            ("pass_with_warnings", copy.deepcopy(draft), "accepted", "proposal_ready"),
            (
                "pass_with_warnings",
                {"claims": [{"evidence_ref": ref, "value": 2}]},
                "corrected",
                "proposal_ready",
            ),
            ("reject", copy.deepcopy(draft), "rejected", "rejected"),
        )
        with (
            patch.object(
                core, "processing_publication_fields", return_value=publication
            ),
            patch.object(
                core, "processing_grounding_refs", return_value=(ref,)
            ),
        ):
            for verdict, revised, outcome, status in cases:
                review = {
                    "verdict": verdict,
                    "revised_analysis": revised,
                    "evidence_refs": [ref],
                }
                result = core.ModelResult(
                    analysis=copy.deepcopy(revised),
                    duration_ms=0,
                    runtime_model=None,
                    runtime_reasoning_effort=None,
                    runtime_metadata_provenance="unavailable",
                    pipeline_status="two_pass_ready",
                    draft_analysis=copy.deepcopy(draft),
                    critical_review=review,
                    stage_receipts={},
                )
                with self.subTest(outcome=outcome):
                    proposal = core.build_luna_proposal_v2(
                        candidate=candidate,
                        result=result,
                        subject_payload_sha256="6" * 64,
                    )
                    self.assertEqual(
                        proposal["critical_review_outcome"], outcome
                    )
                    self.assertEqual(proposal["review_status"], status)
                    self.assertEqual(proposal["formal_write_count"], 0)

    def test_package_requires_every_publication_field_and_exact_value(self) -> None:
        package, publication, stage_calls = self._fixture()
        host = _ReopenHost(stage_calls)
        with (
            patch.object(
                core, "processing_publication_fields", return_value=publication
            ),
            patch.object(
                core,
                "processing_grounding_refs",
                return_value=tuple(package["allowed_evidence_refs"]),
            ),
        ):
            core.validate_package_luna_proposal(
                package,
                subject_payload_sha256="6" * 64,
                processing_host=host,
            )
            missing = copy.deepcopy(package)
            missing.pop("provider_request_count")
            with self.assertRaisesRegex(
                core.PreprocessorError, "processing_package_publication_mismatch"
            ):
                core.validate_package_luna_proposal(
                    missing,
                    subject_payload_sha256="6" * 64,
                    processing_host=host,
                )
            drift = copy.deepcopy(package)
            drift["model_call_count"] = 3
            with self.assertRaisesRegex(
                core.PreprocessorError, "processing_package_publication_mismatch"
            ):
                core.validate_package_luna_proposal(
                    drift,
                    subject_payload_sha256="6" * 64,
                    processing_host=host,
                )

            critic_drift = copy.deepcopy(package)
            critic_drift["luna_proposal"][
                "critical_review_outcome"
            ] = "corrected"
            critic_drift["luna_proposal_sha256"] = core.sha256_value(
                critic_drift["luna_proposal"]
            )
            with self.assertRaisesRegex(
                core.PreprocessorError,
                "luna_proposal_critic_outcome_mismatch",
            ):
                core.validate_package_luna_proposal(
                    critic_drift,
                    subject_payload_sha256="6" * 64,
                    processing_host=host,
                )

    def test_reopened_calls_recompute_stage_grounding(self) -> None:
        package, publication, stage_calls = self._fixture()
        tampered_calls = copy.deepcopy(stage_calls)
        tampered_calls["critical_review"][0]["result"]["items"][0][
            "source_hash"
        ] = "f" * 64
        with (
            patch.object(
                core, "processing_publication_fields", return_value=publication
            ),
            patch.object(
                core,
                "processing_grounding_refs",
                return_value=tuple(package["allowed_evidence_refs"]),
            ),
            self.assertRaisesRegex(
                core.PreprocessorError, "processing_persisted_grounding_mismatch"
            ),
        ):
            core.validate_package_luna_proposal(
                package,
                subject_payload_sha256="6" * 64,
                processing_host=_ReopenHost(tampered_calls),
            )

    def test_processing_keys_are_unconditionally_required(self) -> None:
        self.assertEqual(
            set(core.PROCESSING_PUBLICATION_KEYS),
            core.package_processing_keys({}),
        )

    def test_english_exact_package_keys_and_pointer_publication(self) -> None:
        package, publication, stage_calls = self._fixture(subject="english")
        proposal = package["luna_proposal"]
        proposal["operations"][0]["proposal"][
            "subject_payload_sha256"
        ] = core.sha256_value({"candidate": "english"})
        package["luna_proposal_sha256"] = core.sha256_value(proposal)
        package.update(
            {
                "schema_version": core.PACKAGE_SCHEMA,
                "package_id": "SIP-PKG-" + "A" * 24,
                "study_date": "2026-08-08",
                "created_at": "2026-08-08T00:00:00Z",
                "input_fingerprint": "7" * 64,
                "input_binding": {"processing_contract_sha256": "8" * 64},
                "model_receipt": {},
                "analysis": {"candidate": "english"},
                "pipeline_status": "two_pass_ready",
                "pipeline": ["analysis", "critical_review"],
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "processing_contract_sha256": "8" * 64,
                "draft_analysis": {"items": []},
                "critical_review": {"verdict": "confirmed"},
                "analysis_checkpoint_sha256": "9" * 64,
                "analysis_checkpoint_ref": (
                    "study-intake-analysis-checkpoint://sha256/" + "9" * 64
                ),
                "checkpoint_consumable": False,
                "formal_write_count": 0,
            }
        )
        pointer = {
            key: copy.deepcopy(package[key])
            for key in (
                "pipeline_status", "model", "reasoning_effort",
                "processing_contract_sha256", *core.PROCESSING_PUBLICATION_KEYS,
            )
        }
        host = _ReopenHost(stage_calls)
        with (
            patch.object(
                core, "processing_publication_fields", return_value=publication
            ),
            patch.object(
                core,
                "processing_grounding_refs",
                return_value=tuple(package["allowed_evidence_refs"]),
            ),
        ):
            core.validate_english_v1_package_closure(
                package, pointer=pointer, processing_host=host
            )
            extra = copy.deepcopy(package)
            extra["unexpected"] = True
            with self.assertRaisesRegex(
                core.PreprocessorError, "english_package_contract_invalid"
            ):
                core.validate_english_v1_package_closure(
                    extra, pointer=pointer, processing_host=host
                )
            drift_pointer = copy.deepcopy(pointer)
            drift_pointer["read_session_id"] = "other-session"
            with self.assertRaisesRegex(
                core.PreprocessorError, "english_pointer_publication_mismatch"
            ):
                core.validate_english_v1_package_closure(
                    package,
                    pointer=drift_pointer,
                    processing_host=host,
                )


if __name__ == "__main__":
    unittest.main()
