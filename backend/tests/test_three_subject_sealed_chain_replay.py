#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import hmac
import importlib.util
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
CURRENT_LIB = ROOT / "lib"
SEALED_RELEASE_ID = (
    "4c1c06515ac8ab9cd783111b0c5278618b258fc2f3467c26731b707c75f0d8e8"
)
E8A_RELEASE_ID = (
    "e8a54b12af32e1226676beb4b87cb24961e0949adad14b4d88f6ceb40559ae9f"
)
SEALED_LIB = (
    Path("/Users/xiazhibin/.codex/study-intake-preprocessor/releases")
    / SEALED_RELEASE_ID
    / "lib"
)
SEALED_RUNTIME = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-4c1c0651/stage-runtime"
)
SEALED_PRIVATE = SEALED_RUNTIME / "private"
SEALED_REPORTS = SEALED_PRIVATE / "reports"
E8A_ENGLISH_READ_SESSION = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-e8a54b12/stage-runtime/private/mcp-read-sessions/"
    "sha256/fb/"
    "fb53decd9c984d9744c09eb72f80ec20d4eba8695da05e6b854af8f3229ad05a.json"
)
E8A_ENGLISH_READ_SESSION_PHYSICAL_SHA256 = (
    "7db4905071e34083312f32ac6f80feed58d7f7d29778f20879d5d3df1e450823"
)

sys.path.insert(0, str(CURRENT_LIB))

import preprocessor_core as core  # noqa: E402
from subject_sol_contract import (  # noqa: E402
    SubjectSolRuntimeStore,
    validate_subject_quality_receipt_v1,
)
from tests import test_e8a_english_grounding_replay as e8a_support  # noqa: E402


def _load_sealed_core():
    spec = importlib.util.spec_from_file_location(
        "sealed_preprocessor_core_4c1",
        SEALED_LIB / "preprocessor_core.py",
    )
    if spec is None or spec.loader is None:
        raise AssertionError("sealed 4c1 core cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SEALED_CORE = _load_sealed_core()


SEALED = {
    "english": {
        "capture_id": "EN-20260806-D11B26D125E29827",
        "read_session": SEALED_PRIVATE
        / "mcp-read-sessions/sha256/8f/"
        "8f7104504ad0fec9f6a275329023e186a608d601f18308e856c6c3688e0c9a5a.json",
        "read_session_physical_sha256": (
            "03cea628189099d150c9c54f8d9a13e679c14f71d927db9204902f773df70433"
        ),
        "raw_transport": SEALED_REPORTS
        / "model-mcp-transport/sha256/24/"
        "2490329a7d0cbfde65fab50a1e05a47af70283bafc0bb8179430b8e3dc916058.json",
        "analysis_provider_schema": SEALED_REPORTS
        / "provider-schemas/sha256/56/"
        "5613428054d6dd5a321a6c5aa34d4502b935aa641112e90cc7c2df91f509e567.json",
    },
    "math": {
        "capture_id": "LUNA-MATH-20260809-003",
        "read_session": SEALED_PRIVATE
        / "mcp-read-sessions/sha256/30/"
        "30dc294890f795eefae6b2c717e5c8dd144d432258ee8ee43ef0305b1433e954.json",
        "read_session_physical_sha256": (
            "21202d46b23e3647ece1ffe6438754b74c476540da255fa88d9dc13686d602fb"
        ),
        "analysis_transcript": SEALED_REPORTS
        / "mcp-stage-transcripts/sha256/d3/"
        "d39fa151cdb7a9c5ab711bdd443352eabffc9a36d7625e8ecbce789f580e711d.json",
        "analysis_output": SEALED_REPORTS
        / "model-stage-outputs/objects/"
        "6d10369cabfc3282733f6112d3ee306f356870a9b7ee7504992f68f90b6efc2c.json",
    },
    "cs408": {
        "capture_id": "OBS-91DC4259E9E0BC1996B01884",
        "read_session": SEALED_PRIVATE
        / "mcp-read-sessions/sha256/62/"
        "62a0ea91ccae6ec9f1ee735de241dfc89f6a7e351a72d038ba175d92c52928ce.json",
        "read_session_physical_sha256": (
            "c024b10d8e30f5869b4a7871d37550e3bced37a6b2d66fcb5f51b0bc82f15b9f"
        ),
        "analysis_transcript": SEALED_REPORTS
        / "mcp-stage-transcripts/sha256/b2/"
        "b251c3eb6002b3d4255a983bcb3b1e04d8fa118f2db40233a360e0085b418a11.json",
        "critical_transcript": SEALED_REPORTS
        / "mcp-stage-transcripts/sha256/81/"
        "81a77833d6b87babe282f878e9954486dcb6698ad39efca1656ae64939a2e0a2.json",
        "analysis_output": SEALED_REPORTS
        / "model-stage-outputs/objects/"
        "2eb477ce8950d5c691d8b33b1720c783270b72c5883a221ba76f793a4397057a.json",
        "critical_output": SEALED_REPORTS
        / "model-stage-outputs/objects/"
        "0c0bb14881b11e3a1a9ab435e25298abcfd1eab9a98c23deebd7399d146d8396.json",
    },
}


def _physical_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected mapping: {path}")
    return value


def _payload(path: Path) -> dict[str, Any]:
    wrapper = _json(path)
    payload = wrapper.get("payload")
    if not isinstance(payload, dict):
        raise AssertionError(f"sealed stage payload missing: {path}")
    if wrapper.get("formal_write_count") != 0:
        raise AssertionError(f"sealed stage unexpectedly wrote formal data: {path}")
    return payload


def _evidence_refs(value: Any) -> set[str]:
    refs: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            raw = item.get("evidence_refs")
            if isinstance(raw, list):
                refs.update(ref for ref in raw if isinstance(ref, str))
            for nested in item.values():
                visit(nested)
        elif isinstance(item, list):
            for nested in item:
                visit(nested)

    visit(value)
    return refs


def _transport_events(path: Path) -> list[str]:
    transport = _json(path)
    rows = transport.get("mcp_items")
    if not isinstance(rows, list):
        raise AssertionError("sealed MCP transport has no item list")
    return [
        json.dumps(
            {"type": row["event_type"], "item": row["item"]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for row in rows
    ]


def _stage_from_transcript(
    transcript_path: Path,
    payload: dict[str, Any],
    *,
    module=core,
):
    transcript = _json(transcript_path)
    return module.StructuredStageResult(
        payload=copy.deepcopy(payload),
        duration_ms=0,
        runtime_model=None,
        runtime_reasoning_effort=None,
        runtime_metadata_provenance="unavailable",
        runtime_identity_status="requested_unverified",
        output_sha256=core.sha256_value(payload),
        mcp_transcript_sha256=_physical_sha256(transcript_path),
        mcp_transcript_ref=(
            "study-intake-mcp-stage-transcript://sha256/"
            + _physical_sha256(transcript_path)
        ),
        mcp_tool_call_count=len(transcript["calls"]),
        provider_request_count=len(transcript["calls"]) + 1,
        mcp_calls=tuple(copy.deepcopy(transcript["calls"])),
    )


def _candidate(
    module,
    *,
    subject: str,
    capture_id: str,
    allowed_refs: tuple[str, ...],
    source_bundle: Mapping[str, Any] | None = None,
):
    source_route = (
        str(source_bundle.get("source_route") or source_bundle.get("source_kind"))
        if isinstance(source_bundle, Mapping)
        else None
    )
    normalized_source_bundle = (
        {
            **dict(source_bundle),
            "source_kind": source_route,
        }
        if source_route is not None and isinstance(source_bundle, Mapping)
        else source_bundle
    )
    source_binding = (
        {
            "source_route": source_route,
            "evidence_manifest_sha256": hashlib.sha256(
                f"evidence-manifest:{subject}:{capture_id}".encode()
            ).hexdigest(),
            "evidence_bundle_sha256": hashlib.sha256(
                f"evidence-bundle:{subject}:{capture_id}".encode()
            ).hexdigest(),
        }
        if source_route is not None
        else {}
    )
    return module.Candidate(
        subject=subject,
        capture_id=capture_id,
        study_date="2026-08-09",
        recorded_at="2026-08-09T00:00:00Z",
        input_fingerprint=hashlib.sha256(
            f"sealed-replay:{subject}:{capture_id}".encode()
        ).hexdigest(),
        input_binding={
            "processing_contract_sha256": hashlib.sha256(
                f"processing:{subject}".encode()
            ).hexdigest(),
            "image_evidence_refs": [],
            **source_binding,
        },
        model_input={"source_bundle": normalized_source_bundle},
        allowed_evidence_refs=allowed_refs,
        image_paths=(),
        target_label=capture_id,
        canonical_state="sealed_zero_model_replay",
        sol_state="disabled",
    )


class ThreeSubjectSealedChainReplayTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="three-subject-sealed-chain-"
        )
        self.runtime = Path(self.temporary.name) / "runtime"
        self.runtime.mkdir()
        self.store = SubjectSolRuntimeStore(self.runtime)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _assert_sealed_hashes(self) -> None:
        for subject, row in SEALED.items():
            with self.subTest(subject=subject, kind="read_session"):
                self.assertEqual(
                    _physical_sha256(row["read_session"]),
                    row["read_session_physical_sha256"],
                )
            for key, path in row.items():
                if key in {
                    "capture_id",
                    "read_session_physical_sha256",
                    "read_session",
                }:
                    continue
                with self.subTest(subject=subject, kind=key):
                    self.assertEqual(_physical_sha256(path), path.stem)

    def _sealed_english_calls(self, *, module, runtime: Path):
        session = _json(SEALED["english"]["read_session"])
        events = _transport_events(SEALED["english"]["raw_transport"])
        return module.CodexRunner({}, runtime)._mcp_stage_calls(
            stdout=("\n".join(events) + "\n").encode(),
            stage_name="english_analysis",
            subject="english",
            processing_context={"mcp_read_session": session},
        )

    def test_original_4c1_failure_shapes_reproduce_exact_error_codes(self) -> None:
        self._assert_sealed_hashes()

        with self.assertRaisesRegex(
            SEALED_CORE.PreprocessorError,
            "^english_analysis_mcp_duplicate_read$",
        ):
            self._sealed_english_calls(
                module=SEALED_CORE,
                runtime=self.runtime / "sealed-english",
            )

        math_payload = _payload(SEALED["math"]["analysis_output"])
        math_stage = _stage_from_transcript(
            SEALED["math"]["analysis_transcript"],
            math_payload,
            module=SEALED_CORE,
        )
        math_refs = SEALED_CORE.mcp_grounding_refs((math_stage,))
        math_candidate = _candidate(
            SEALED_CORE,
            subject="math",
            capture_id=SEALED["math"]["capture_id"],
            allowed_refs=math_refs,
            source_bundle={"source_route": "new_intake"},
        )
        with self.assertRaisesRegex(
            SEALED_CORE.PreprocessorError,
            "^math_analysis_formal_field_coverage_failed$",
        ):
            SEALED_CORE.validate_math_semantic_gates(
                copy.deepcopy(math_payload), math_candidate
            )

        cs_analysis = _payload(SEALED["cs408"]["analysis_output"])
        cs_review = _payload(SEALED["cs408"]["critical_output"])
        cs_analysis_stage = _stage_from_transcript(
            SEALED["cs408"]["analysis_transcript"],
            cs_analysis,
            module=SEALED_CORE,
        )
        cs_review_stage = _stage_from_transcript(
            SEALED["cs408"]["critical_transcript"],
            cs_review,
            module=SEALED_CORE,
        )
        predeclared = {
            ref
            for ref in _evidence_refs(cs_analysis)
            if not ref.startswith("mcp-item:")
        }
        cs_allowed = tuple(
            sorted(
                predeclared
                | set(SEALED_CORE.mcp_grounding_refs((cs_analysis_stage,)))
                | set(SEALED_CORE.mcp_grounding_refs((cs_review_stage,)))
            )
        )
        with self.assertRaisesRegex(
            SEALED_CORE.PreprocessorError,
            "^critical_review_refs_invalid$",
        ):
            SEALED_CORE.validate_critical_review_v2(
                copy.deepcopy(cs_review),
                allowed_evidence_refs=cs_allowed,
                draft_analysis=copy.deepcopy(cs_analysis),
                require_network_context=True,
            )

    def _publish_value(self, root: Path, value: Mapping[str, Any]):
        digest, path = self.store._publish_immutable_value(root, value)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), value)
        return digest, path

    def _publish_file(self, root: Path, value: Mapping[str, Any]):
        digest, path = self.store._publish_immutable(root, value)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), value)
        return digest, path

    def _publish_plugin_value(self, root: Path, value: Mapping[str, Any]):
        payload = core.canonical_bytes(value) + b"\n"
        digest = hashlib.sha256(payload).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), value)
        return digest, path

    def _publish_transcript_overlay(
        self,
        *,
        base_path: Path,
        subject: str,
        stage_name: str,
        fixture_kind: str,
    ) -> tuple[Path, dict[str, Any]]:
        value = _json(base_path)
        value.update(
            {
                "stage_name": stage_name,
                "subject": subject,
                "model_call_count": 1,
                "semantic_stage_count": 1,
                "provider_request_count": len(value["calls"]) + 1,
                "fixture_kind": fixture_kind,
                "formal_write_count": 0,
            }
        )
        digest, published = self._publish_file(
            self.runtime / "private/reports/mcp-stage-transcripts",
            value,
        )
        expected = (
            self.runtime
            / "private/reports/mcp-stage-transcripts/sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        self.assertEqual(published, expected)
        return published, value

    def _english_analysis_fixture(
        self, evidence_refs: list[str]
    ) -> dict[str, Any]:
        value = {
            "items": [
                {
                    "item_id": "EN-SEALED-ITEM-001",
                    "sequence": 1,
                    "item": "permanent",
                    "candidate_type": "单词",
                    "candidate_status": "familiarity_candidate",
                    "tier": "A",
                    "source_event_id": "EVT-20260806-507660196D4F941D",
                    "bank_status": "needs_check",
                    "bank_match_ids": [],
                    "mastered_status": "clear",
                    "mastery_proposal": None,
                    "grounding": {
                        "status": "passed",
                        "user_evidence_ref": "EVT-20260806-507660196D4F941D",
                        "writing_pattern": {
                            "status": "not_requested",
                            "reference_ids": [],
                            "note": "not requested in sealed replay",
                        },
                        "writing_vocabulary": {
                            "status": "not_requested",
                            "reference_ids": [],
                            "note": "not requested in sealed replay",
                        },
                        "syllabus_occurrence": {
                            "status": "not_requested",
                            "reference_ids": [],
                            "note": "not requested in sealed replay",
                        },
                        "sentence_pattern": {
                            "status": "not_requested",
                            "reference_ids": [],
                            "note": "not requested in sealed replay",
                        },
                        "old_word_sources": [],
                        "naturalness_check": "not requested in sealed replay",
                        "mcp_evidence_refs": list(evidence_refs),
                    },
                    "card": {
                        "meaning": "长期的；永久的",
                        "source_translation": "仅作候选释义，不含题目答案。",
                        "usage": "adjective",
                        "review_note": "需要由下游独立重开来源证据。",
                        "adopted_pattern": "",
                        "old_word_example": "",
                        "example_translation": "",
                        "structure_breakdown": "",
                        "review_old_words": [],
                    },
                }
            ]
        }
        schema = _json(SEALED["english"]["analysis_provider_schema"])
        self.assertEqual(set(value), set(schema["required"]))
        item_schema = schema["$defs"]["item"]
        self.assertEqual(set(value["items"][0]), set(item_schema["required"]))
        self.assertIn(
            value["items"][0]["source_event_id"],
            item_schema["properties"]["source_event_id"]["enum"],
        )
        for evidence_ref in evidence_refs:
            self.assertRegex(
                evidence_ref,
                schema["$defs"]["grounding"]["properties"]
                ["mcp_evidence_refs"]["items"]["pattern"],
            )
        return value

    @staticmethod
    def _fix_math_analysis(value: Mapping[str, Any]) -> dict[str, Any]:
        fixed = copy.deepcopy(dict(value))
        source_claim = copy.deepcopy(
            fixed["question_structure"]["objects"][0]
        )
        for field in (
            "safe_summary",
            "question_body",
            "source_and_answer",
            "wrong_point",
            "methods",
        ):
            fixed["formalization_candidates"][field] = [
                copy.deepcopy(source_claim)
            ]
        return fixed

    @staticmethod
    def _math_review_fixture(draft: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": "study-intake-luna-math-critical-review-v2",
            "verdict": "pass",
            "summary": "shape-valid corrected overlay; no provider request was made",
            "revised_analysis": copy.deepcopy(dict(draft)),
            "relationship_decisions": [],
            "unsupported_claims": [],
            "evidence_misreads": [],
            "mathematical_errors": [],
            "visual_findings": [],
            "provenance_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }

    @staticmethod
    def _fix_cs408_review(value: Mapping[str, Any]) -> dict[str, Any]:
        fixed = copy.deepcopy(dict(value))
        tainted = "analysis.question_structure.mechanism_structure[0]чаты?"
        exact = "analysis.question_structure.mechanism_structure[0]"
        for section in (
            "unsupported_claims",
            "evidence_misreads",
            "answer_safety_findings",
            "missing_analysis",
            "required_corrections",
            "sol_priority_checks",
        ):
            for row in fixed[section]:
                row["analysis_refs"] = [
                    exact if ref == tainted else ref
                    for ref in row["analysis_refs"]
                ]
        return fixed

    def _processing_stage_receipt(
        self,
        *,
        subject: str,
        stage_name: str,
        stage,
        session: Mapping[str, Any],
        processing_binding: Mapping[str, Any],
        result_sha256: str,
    ) -> dict[str, Any]:
        grounding = core.mcp_grounding_manifest((stage,))
        call_core = {
            "schema_version": "sealed-zero-model-mcp-call-receipt-v1",
            "subject": subject,
            "stage_name": stage_name,
            "transcript_sha256": stage.mcp_transcript_sha256,
            "calls_sha256": core.sha256_value(list(stage.mcp_calls)),
            "pagination_coverage_complete": True,
            "formal_write_count": 0,
        }
        signed_call = self.store._seal(
            call_core, purpose="sealed-zero-model-mcp-call-receipt"
        )
        self.store._verify_seal(
            signed_call, purpose="sealed-zero-model-mcp-call-receipt"
        )
        call_sha, _ = self._publish_value(
            self.runtime / "dispatch/mcp-call-receipts", signed_call
        )
        capture_freeze_sha = hashlib.sha256(
            f"capture-freeze:{subject}:{session['capture_id']}".encode()
        ).hexdigest()
        initial_session_sha = hashlib.sha256(
            f"initial-session:{session['manifest_sha256']}".encode()
        ).hexdigest()
        return {
            "status": "ready",
            "prompt_version": f"sealed-zero-model-{stage_name}-v1",
            "prompt_sha256": hashlib.sha256(stage_name.encode()).hexdigest(),
            "schema_sha256": hashlib.sha256(
                f"schema:{stage_name}".encode()
            ).hexdigest(),
            "result_sha256": result_sha256,
            "output_sha256": stage.output_sha256,
            "duration_ms": 0,
            "requested_model": "gpt-5.6-luna",
            "requested_reasoning_effort": "max",
            "runtime_model": None,
            "runtime_reasoning_effort": None,
            "runtime_metadata_provenance": "unavailable",
            "runtime_identity_status": "requested_unverified",
            "ordered_image_sha256s": [],
            "semantic_stage_count": 1,
            "provider_request_count": stage.provider_request_count,
            "mcp_tool_call_count": stage.mcp_tool_call_count,
            "model_call_count": 1,
            "formal_write_count": 0,
            "processing_binding": copy.deepcopy(dict(processing_binding)),
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "capture_freeze_receipt_sha256": capture_freeze_sha,
            "capture_freeze_receipt_ref": (
                "study-intake-capture-freeze://sha256/" + capture_freeze_sha
            ),
            "mcp_read_session_receipt_sha256": initial_session_sha,
            "mcp_read_session_receipt_ref": (
                "study-intake-mcp-read-session://sha256/" + initial_session_sha
            ),
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "authority_snapshot_manifest_sha256": session.get(
                "authority_snapshot_manifest_sha256"
            )
            or hashlib.sha256(
                (
                    "sealed-authority-snapshot:"
                    + subject
                    + ":"
                    + str(session["manifest_sha256"])
                ).encode()
            ).hexdigest(),
            "evidence_generation": session["generation"],
            "evidence_authority_fingerprint": session["authority_fingerprint"],
            "mcp_transcript_sha256": stage.mcp_transcript_sha256,
            "mcp_transcript_ref": stage.mcp_transcript_ref,
            "mcp_call_receipt_sha256": call_sha,
            "mcp_call_receipt_ref": (
                "study-intake-mcp-read-session-call://sha256/" + call_sha
            ),
            "pagination_coverage_complete": True,
            "mcp_grounding_manifest": grounding,
            "mcp_grounding_manifest_sha256": grounding["manifest_sha256"],
            "host_semantic_prefetch": False,
            "consumed_terminal_duplicate_read_count": 0,
        }

    def _final_session_receipt(
        self,
        *,
        subject: str,
        session: Mapping[str, Any],
        processing_binding: Mapping[str, Any],
        stages: Mapping[str, Any],
    ) -> tuple[dict[str, Any], Path]:
        key = self.store._authority_key(create=True)
        unsigned = {
            "schema_version": "sealed-zero-model-final-read-session-v1",
            "phase": "complete",
            "subject": subject,
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "generation": session["generation"],
            "authority_fingerprint": session["authority_fingerprint"],
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "analysis_mcp_transcript_sha256": stages["analysis"][
                "mcp_transcript_sha256"
            ],
            "critical_review_mcp_transcript_sha256": stages[
                "critical_review"
            ]["mcp_transcript_sha256"],
            "model_mcp_tool_call_count": sum(
                stage["mcp_tool_call_count"] for stage in stages.values()
            ),
            "provider_request_count": sum(
                stage["provider_request_count"] for stage in stages.values()
            ),
            "provider_request_count_status": "sealed_shape_replay_no_provider",
            "pagination_coverage_complete": True,
            "formal_write_count": 0,
        }
        mac = hmac.new(
            key,
            core.canonical_bytes(unsigned),
            hashlib.sha256,
        ).hexdigest()
        receipt = {**unsigned, "hmac_sha256": mac}
        self.assertTrue(
            hmac.compare_digest(
                receipt["hmac_sha256"],
                hmac.new(
                    key,
                    core.canonical_bytes(unsigned),
                    hashlib.sha256,
                ).hexdigest(),
            )
        )
        receipt_sha, receipt_path = self._publish_plugin_value(
            self.runtime / "dispatch/final-read-session-receipts", receipt
        )
        stages["read_session"] = {
            "status": "complete",
            "receipt": receipt,
            "receipt_sha256": receipt_sha,
            "receipt_ref": (
                "study-intake-mcp-read-session://sha256/" + receipt_sha
            ),
            "model_mcp_tool_call_count": receipt[
                "model_mcp_tool_call_count"
            ],
            "provider_request_count": receipt["provider_request_count"],
            "provider_request_count_status": receipt[
                "provider_request_count_status"
            ],
            "pagination_coverage_complete": True,
            "formal_write_count": 0,
        }
        return stages["read_session"], receipt_path

    def _close_subject(
        self,
        *,
        subject: str,
        candidate,
        session: Mapping[str, Any],
        draft: dict[str, Any],
        review: dict[str, Any],
        revised: dict[str, Any],
        analysis_stage,
        review_stage,
        fixture_notes: list[str],
        candidate_release_id: str = SEALED_RELEASE_ID,
    ) -> dict[str, Any]:
        processing_binding_core = {
            "schema_version": "sealed-zero-model-processing-binding-v1",
            "subject": subject,
            "capture_id": candidate.capture_id,
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "candidate_release_id": candidate_release_id,
        }
        processing_binding = {
            **processing_binding_core,
            "binding_sha256": hashlib.sha256(
                core.canonical_bytes(processing_binding_core) + b"\n"
            ).hexdigest(),
        }
        stages: dict[str, Any] = {
            "analysis": self._processing_stage_receipt(
                subject=subject,
                stage_name=f"{subject}_analysis",
                stage=analysis_stage,
                session=session,
                processing_binding=processing_binding,
                result_sha256=core.sha256_value(draft),
            ),
            "critical_review": self._processing_stage_receipt(
                subject=subject,
                stage_name=f"{subject}_critical_review",
                stage=review_stage,
                session=session,
                processing_binding=processing_binding,
                result_sha256=core.sha256_value(review),
            ),
        }
        final_session, final_session_path = self._final_session_receipt(
            subject=subject,
            session=session,
            processing_binding=processing_binding,
            stages=stages,
        )
        publication = core.processing_publication_fields(stages)
        self.assertEqual(publication["model_call_count"], 2)
        self.assertEqual(publication["host_semantic_prefetch"], False)

        checkpoint_runner = core.CodexRunner(
            {
                "authority_release_id": candidate_release_id,
                "authority_generation_id": hashlib.sha256(
                    f"sealed-generation:{subject}".encode()
                ).hexdigest(),
            },
            self.runtime,
        )
        checkpoint = checkpoint_runner._write_analysis_checkpoint(
            candidate,
            draft=draft,
            analysis_receipt=stages["analysis"],
        )
        checkpoint_path = (
            self.runtime
            / "private/reports/analysis-checkpoints/objects"
            / f"{checkpoint['checkpoint_sha256']}.json"
        )
        self.assertEqual(
            core.sha256_file(checkpoint_path), checkpoint["checkpoint_sha256"]
        )
        self.assertEqual(_json(checkpoint_path)["formal_write_count"], 0)

        result = core.ModelResult(
            analysis=copy.deepcopy(revised),
            duration_ms=0,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(stages),
            analysis_checkpoint_sha256=checkpoint["checkpoint_sha256"],
            analysis_checkpoint_ref=checkpoint["checkpoint_ref"],
            semantic_stage_count=2,
            provider_request_count=0,
            mcp_tool_call_count=(
                analysis_stage.mcp_tool_call_count
                + review_stage.mcp_tool_call_count
            ),
        )
        subject_payload_sha = core.sha256_value(revised)
        proposal = core.build_luna_proposal_v2(
            candidate=candidate,
            result=result,
            subject_payload_sha256=subject_payload_sha,
        )
        proposal_sha = core.sha256_value(proposal)
        package = {
            "schema_version": "sealed-zero-model-package-v1",
            "subject": subject,
            "capture_id": candidate.capture_id,
            "draft_analysis": copy.deepcopy(draft),
            "critical_review": copy.deepcopy(review),
            "analysis": copy.deepcopy(revised),
            "stage_receipts": copy.deepcopy(stages),
            "allowed_evidence_refs": list(candidate.allowed_evidence_refs),
            "luna_proposal": proposal,
            "luna_proposal_sha256": proposal_sha,
            "fixture_notes": list(fixture_notes),
            "executed_model_call_count": 0,
            "formal_write_count": 0,
            **publication,
        }
        core.validate_package_luna_proposal(
            package,
            subject_payload_sha256=subject_payload_sha,
        )
        package_sha, package_path = self._publish_file(
            self.runtime / "packages", package
        )

        analysis_output_sha, _ = self._publish_value(
            self.runtime / "dispatch/subject-stage-closures",
            {
                "subject": subject,
                "stage": "analysis",
                "payload_sha256": core.sha256_value(draft),
                "fixture_notes": fixture_notes,
                "formal_write_count": 0,
            },
        )
        critical_output_sha, _ = self._publish_value(
            self.runtime / "dispatch/subject-stage-closures",
            {
                "subject": subject,
                "stage": "critical_review",
                "payload_sha256": core.sha256_value(review),
                "fixture_notes": fixture_notes,
                "formal_write_count": 0,
            },
        )
        quality = self.store._issue_quality_receipt(
            {
                "batch_id": f"SEALED-ZERO-MODEL-{subject.upper()}",
                "subject": subject,
                "capture_id": candidate.capture_id,
                "unit_sha256": candidate.input_fingerprint,
                "frozen_payload_sha256": hashlib.sha256(
                    f"frozen:{subject}:{candidate.capture_id}".encode()
                ).hexdigest(),
                "completion_sha256": hashlib.sha256(
                    f"completion:{subject}".encode()
                ).hexdigest(),
                "dispatch_receipt_sha256": hashlib.sha256(
                    f"dispatch:{subject}".encode()
                ).hexdigest(),
                "dispatch_package_sha256": hashlib.sha256(
                    f"dispatch-package:{subject}".encode()
                ).hexdigest(),
                "capture_freeze_receipt_sha256": stages["analysis"][
                    "capture_freeze_receipt_sha256"
                ],
                "mcp_read_session_receipt_sha256": final_session[
                    "receipt_sha256"
                ],
                "analysis_output_sha256": analysis_output_sha,
                "analysis_mcp_transcript_sha256": analysis_stage.mcp_transcript_sha256,
                "critical_review_output_sha256": critical_output_sha,
                "critical_review_mcp_transcript_sha256": review_stage.mcp_transcript_sha256,
                "draft_sha256": core.sha256_value(draft),
                "review_outcome": core.derive_critical_review_outcome(
                    subject=subject,
                    draft_analysis=draft,
                    critical_review=review,
                ),
                "proposal_sha256": proposal_sha,
                "package_sha256": package_sha,
                "authority": {
                    "generation": session["generation"],
                    "authority_fingerprint": session[
                        "authority_fingerprint"
                    ],
                },
                "model_call_count": 2,
                "formal_write_count": 0,
                "issued_at": "2026-08-09T00:00:00Z",
            }
        )
        validate_subject_quality_receipt_v1(quality)
        self.store._verify_seal(quality, purpose="subject-quality-receipt")
        quality_sha, quality_path = self._publish_file(
            self.runtime / "dispatch/subject-quality", quality
        )
        self.assertEqual(_json(package_path)["formal_write_count"], 0)
        self.assertEqual(_json(quality_path)["formal_write_count"], 0)
        return {
            "subject": subject,
            "capture_id": candidate.capture_id,
            "analysis_transcript_sha256": analysis_stage.mcp_transcript_sha256,
            "critical_review_transcript_sha256": review_stage.mcp_transcript_sha256,
            "checkpoint_sha256": checkpoint["checkpoint_sha256"],
            "package_sha256": package_sha,
            "package_path": str(package_path),
            "quality_receipt_sha256": quality_sha,
            "quality_receipt_path": str(quality_path),
            "final_read_session_receipt_sha256": final_session[
                "receipt_sha256"
            ],
            "final_read_session_receipt_path": str(final_session_path),
            "executed_model_call_count": 0,
            "formal_write_count": 0,
        }

    def test_corrected_overlays_form_three_harness_subject_chains(self) -> None:
        self._assert_sealed_hashes()
        closures: list[dict[str, Any]] = []

        math_session = _json(SEALED["math"]["read_session"])
        math_original = _payload(SEALED["math"]["analysis_output"])
        math_analysis = self._fix_math_analysis(math_original)
        math_analysis_stage = _stage_from_transcript(
            SEALED["math"]["analysis_transcript"], math_analysis
        )
        core.validate_model_stage_mcp_grounding(
            math_analysis,
            math_analysis_stage,
            subject="math",
            stage_name="math_analysis",
        )
        math_allowed = core.mcp_grounding_refs((math_analysis_stage,))
        math_candidate = _candidate(
            core,
            subject="math",
            capture_id=SEALED["math"]["capture_id"],
            allowed_refs=math_allowed,
            source_bundle={"source_route": "new_intake"},
        )
        math_analysis = core.validate_math_semantic_gates(
            math_analysis, math_candidate
        )
        math_review = self._math_review_fixture(math_analysis)
        math_review_path, _ = self._publish_transcript_overlay(
            base_path=SEALED["math"]["analysis_transcript"],
            subject="math",
            stage_name="math_critical_review",
            fixture_kind="shape_valid_corrected_overlay_no_provider",
        )
        math_review_stage = _stage_from_transcript(
            math_review_path, math_review
        )
        core.validate_model_stage_mcp_grounding(
            math_review,
            math_review_stage,
            subject="math",
            stage_name="math_critical_review",
            allowed_prior_refs=core.mcp_grounding_refs((math_analysis_stage,)),
        )
        math_review_candidate = core.candidate_with_mcp_grounding(
            math_candidate, (math_analysis_stage, math_review_stage)
        )
        math_relationships = core.build_math_mcp_relationship_context(
            (math_review_stage,), draft_analysis=math_analysis
        )
        math_review = core.validate_math_critical_review_v2(
            math_review,
            allowed_evidence_refs=math_review_candidate.allowed_evidence_refs,
            draft_analysis=math_analysis,
            candidate=math_review_candidate,
            relationship_context=math_relationships,
        )
        closures.append(
            self._close_subject(
                subject="math",
                candidate=math_review_candidate,
                session=math_session,
                draft=math_analysis,
                review=math_review,
                revised=math_review["revised_analysis"],
                analysis_stage=math_analysis_stage,
                review_stage=math_review_stage,
                fixture_notes=[
                    "analysis payload is sealed 4c1 output with only the five required formalization fields overlaid from an existing capture-backed claim",
                    "critical review output and transcript are shape-valid corrected overlays; no provider request was made",
                ],
            )
        )

        cs_session = _json(SEALED["cs408"]["read_session"])
        cs_analysis = _payload(SEALED["cs408"]["analysis_output"])
        cs_review_original = _payload(SEALED["cs408"]["critical_output"])
        cs_analysis_stage = _stage_from_transcript(
            SEALED["cs408"]["analysis_transcript"], cs_analysis
        )
        cs_review_stage = _stage_from_transcript(
            SEALED["cs408"]["critical_transcript"], cs_review_original
        )
        core.validate_model_stage_mcp_grounding(
            cs_analysis,
            cs_analysis_stage,
            subject="cs408",
            stage_name="cs408_analysis",
        )
        predeclared = {
            ref
            for ref in _evidence_refs(cs_analysis)
            if not ref.startswith("mcp-item:")
        }
        cs_allowed = tuple(
            sorted(
                predeclared
                | set(core.mcp_grounding_refs((cs_analysis_stage,)))
            )
        )
        cs_analysis = core.validate_analysis_v2(
            cs_analysis,
            cs_allowed,
            require_network_context=True,
        )
        core.validate_model_stage_mcp_grounding(
            cs_review_original,
            cs_review_stage,
            subject="cs408",
            stage_name="cs408_critical_review",
            allowed_prior_refs=core.mcp_grounding_refs((cs_analysis_stage,)),
        )
        cs_review = self._fix_cs408_review(cs_review_original)
        cs_review_allowed = tuple(
            sorted(
                set(cs_allowed)
                | set(core.mcp_grounding_refs((cs_review_stage,)))
            )
        )
        cs_review = core.validate_critical_review_v2(
            cs_review,
            allowed_evidence_refs=cs_review_allowed,
            draft_analysis=copy.deepcopy(cs_analysis),
            require_network_context=True,
        )
        cs_candidate = _candidate(
            core,
            subject="cs408",
            capture_id=SEALED["cs408"]["capture_id"],
            allowed_refs=cs_review_allowed,
        )
        closures.append(
            self._close_subject(
                subject="cs408",
                candidate=cs_candidate,
                session=cs_session,
                draft=cs_analysis,
                review=cs_review,
                revised=cs_review["revised_analysis"],
                analysis_stage=cs_analysis_stage,
                review_stage=cs_review_stage,
                fixture_notes=[
                    "analysis and critical review payloads and transcripts are sealed 4c1 artifacts",
                    "the only corrected overlay replaces the one tainted analysis_ref with its exact allowed member",
                ],
            )
        )

        self.assertEqual(
            _physical_sha256(E8A_ENGLISH_READ_SESSION),
            E8A_ENGLISH_READ_SESSION_PHYSICAL_SHA256,
        )
        english_session = _json(E8A_ENGLISH_READ_SESSION)
        (
            english_probe,
            english_analysis_manifest,
            english_artifact_ref,
            english_library_ref,
        ) = e8a_support.e8a_analysis_grounding_fixture()
        english_transcript_path = e8a_support.E8A_TRANSCRIPT_PATH
        english_transcript_sha = e8a_support.E8A_TRANSCRIPT_SHA256
        self.assertEqual(
            core.sha256_file(english_transcript_path),
            english_transcript_sha,
        )
        english_analysis = self._english_analysis_fixture(
            [english_artifact_ref, english_library_ref]
        )
        english_analysis_stage = copy.deepcopy(english_probe)
        english_analysis_stage = core.StructuredStageResult(
            **{
                **english_analysis_stage.__dict__,
                "payload": copy.deepcopy(english_analysis),
                "output_sha256": core.sha256_value(english_analysis),
                "mcp_transcript_ref": (
                    "study-intake-mcp-stage-transcript://sha256/"
                    + english_transcript_sha
                ),
            }
        )
        core.validate_model_stage_mcp_grounding(
            english_analysis,
            english_analysis_stage,
            subject="english",
            stage_name="english_analysis",
        )
        core.validate_english_candidate_answer_safety(english_analysis)
        core.validate_english_mcp_grounding(
            english_analysis["items"],
            grounding_manifest=core.mcp_grounding_manifest(
                (english_analysis_stage,)
            ),
        )
        english_review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "confirmed",
            "draft_analysis_sha256": core.sha256_value(english_analysis),
            "findings": [],
            "correction_resolutions": [],
            "revised_items": copy.deepcopy(english_analysis["items"]),
        }
        english_review_path, _ = self._publish_transcript_overlay(
            base_path=english_transcript_path,
            subject="english",
            stage_name="english_critical_review",
            fixture_kind="shape_valid_corrected_overlay_no_provider",
        )
        english_review_stage = _stage_from_transcript(
            english_review_path, english_review
        )
        core.validate_model_stage_mcp_grounding(
            english_review,
            english_review_stage,
            subject="english",
            stage_name="english_critical_review",
            allowed_prior_refs=core.mcp_grounding_refs((english_analysis_stage,)),
        )
        core.validate_english_critical_review(
            english_review, english_analysis
        )
        core.validate_english_applied_corrections(english_review)
        core.validate_english_mcp_grounding(
            english_review["revised_items"],
            grounding_manifest=core.mcp_grounding_manifest(
                (english_review_stage,)
            ),
        )
        english_allowed = tuple(
            sorted(
                set(core.mcp_grounding_refs((english_analysis_stage,)))
                | set(core.mcp_grounding_refs((english_review_stage,)))
            )
        )
        english_candidate = _candidate(
            core,
            subject="english",
            capture_id=SEALED["english"]["capture_id"],
            allowed_refs=english_allowed,
        )
        closures.append(
            self._close_subject(
                subject="english",
                candidate=english_candidate,
                session=english_session,
                draft=english_analysis,
                review=english_review,
                revised={"items": copy.deepcopy(english_review["revised_items"])},
                analysis_stage=english_analysis_stage,
                review_stage=english_review_stage,
                fixture_notes=[
                    "analysis uses the exact sealed e8a canonical MCP transcript and read session with a corrected semantic payload fixture; no provider request was made",
                    "fresh critical review uses an independently published stage transcript overlay over the sealed e8a calls and a corrected semantic payload fixture; no provider request was made",
                ],
                candidate_release_id=E8A_RELEASE_ID,
            )
        )

        summary = {
            "schema_version": "three-subject-sealed-zero-model-harness-replay-v1",
            "sealed_candidate_release_id": SEALED_RELEASE_ID,
            "english_grounding_base_release_id": E8A_RELEASE_ID,
            "closure_level": (
                "harness_shape_only_not_production_quality_closure"
            ),
            "subjects": closures,
            "harness_package_count": len(
                {row["package_sha256"] for row in closures}
            ),
            "harness_quality_receipt_count": len(
                {row["quality_receipt_sha256"] for row in closures}
            ),
            "harness_final_read_session_receipt_count": len(
                {
                    row["final_read_session_receipt_sha256"]
                    for row in closures
                }
            ),
            "production_quality_receipt_count": 0,
            "production_quality_status": (
                "not_claimed_from_shape_valid_corrected_overlays"
            ),
            "executed_model_call_count": 0,
            "formal_write_count": 0,
        }
        summary_sha, summary_path = self._publish_file(
            self.runtime / "reports/three-subject-sealed-chain", summary
        )
        reopened = _json(summary_path)
        self.assertEqual(reopened, summary)
        self.assertEqual(core.sha256_file(summary_path), summary_sha)
        self.assertEqual(summary["harness_package_count"], 3)
        self.assertEqual(summary["harness_quality_receipt_count"], 3)
        self.assertEqual(summary["harness_final_read_session_receipt_count"], 3)
        self.assertEqual(summary["production_quality_receipt_count"], 0)
        self.assertEqual(summary["executed_model_call_count"], 0)
        self.assertEqual(summary["formal_write_count"], 0)
        self.assertEqual(
            {row["subject"] for row in closures},
            {"math", "cs408", "english"},
        )
        self.assertEqual(
            len(
                {
                    sha
                    for row in closures
                    for sha in (
                        row["analysis_transcript_sha256"],
                        row["critical_review_transcript_sha256"],
                    )
                }
            ),
            6,
        )

    def test_three_subject_production_read_session_reopens_and_publishes(
        self,
    ) -> None:
        """Reopen production HMAC artifacts through the core publication gate.

        The preceding replay is deliberately evidence-shaped: math and 408
        retain the sealed provider semantics and English has no accepted 4c1
        Analysis output.  This companion test therefore uses production
        ProcessingPluginHost fixtures to prove that all three final read-session
        artifacts reopen and that the core verifies the plugin-owned LF hash
        convention without changing core-owned grounding hashes.  It does not
        call a model.
        """

        from tests.test_processing_plugin import ProcessingPluginHostTests

        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                production = ProcessingPluginHostTests(methodName="runTest")
                production.setUp()
                try:
                    publication, stage_receipts = (
                        production._published_read_session(subject)
                    )
                    reopened = production.host.reopen_published_read_session(
                        subject=subject,
                        publication=publication,
                        stage_receipts=stage_receipts,
                    )
                    self.assertEqual(
                        set(reopened["stage_calls"]),
                        {"analysis", "critical_review"},
                    )
                    self.assertTrue(
                        all(reopened["stage_calls"][stage]
                            for stage in ("analysis", "critical_review"))
                    )
                    final_stage = stage_receipts["read_session"]
                    validated_final = production.host.validate_final_model_read_session(
                        subject=subject,
                        context=reopened["context"],
                        finalized={
                            "receipt": copy.deepcopy(final_stage["receipt"]),
                            "receipt_sha256": final_stage["receipt_sha256"],
                            "receipt_ref": final_stage["receipt_ref"],
                        },
                    )
                    self.assertEqual(validated_final["phase"], "complete")
                    self.assertEqual(validated_final["formal_write_count"], 0)

                    binding = stage_receipts["analysis"][
                        "processing_binding"
                    ]
                    binding_core = {
                        key: copy.deepcopy(value)
                        for key, value in binding.items()
                        if key != "binding_sha256"
                    }
                    host_digest = hashlib.sha256(
                        (
                            json.dumps(
                                binding_core,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            + "\n"
                        ).encode("utf-8")
                    ).hexdigest()
                    self.assertEqual(binding["binding_sha256"], host_digest)
                    self.assertNotEqual(
                        binding["binding_sha256"],
                        core.sha256_value(binding_core),
                    )
                    grounding_before = {
                        stage: stage_receipts[stage][
                            "mcp_grounding_manifest_sha256"
                        ]
                        for stage in ("analysis", "critical_review")
                    }
                    # This older host fixture reopens a historical v2 session.
                    # The successor publication gate additionally requires the
                    # frozen authority snapshot binding, so project only that
                    # new immutable binding before exercising the current core
                    # publication validator.
                    authority_snapshot_sha256 = hashlib.sha256(
                        (
                            "successor-authority-snapshot:"
                            + subject
                            + ":"
                            + str(
                                stage_receipts["analysis"][
                                    "read_session_manifest_sha256"
                                ]
                            )
                        ).encode()
                    ).hexdigest()
                    for stage in ("analysis", "critical_review"):
                        stage_receipts[stage][
                            "authority_snapshot_manifest_sha256"
                        ] = authority_snapshot_sha256
                    derived = core.processing_publication_fields(
                        stage_receipts
                    )
                    self.assertEqual(
                        derived["processing_binding_sha256"],
                        binding["binding_sha256"],
                    )
                    self.assertEqual(
                        derived["mcp_read_session_receipt_sha256"],
                        final_stage["receipt_sha256"],
                    )
                    self.assertEqual(
                        grounding_before,
                        {
                            stage: stage_receipts[stage][
                                "mcp_grounding_manifest_sha256"
                            ]
                            for stage in ("analysis", "critical_review")
                        },
                    )

                    forged_binding = copy.deepcopy(stage_receipts)
                    no_lf_binding_sha = core.sha256_value(binding_core)
                    for stage in ("analysis", "critical_review"):
                        forged_binding[stage]["processing_binding"][
                            "binding_sha256"
                        ] = no_lf_binding_sha
                        forged_binding[stage][
                            "processing_binding_sha256"
                        ] = no_lf_binding_sha
                    with self.assertRaisesRegex(
                        core.PreprocessorError,
                        "^processing_stage_binding_incomplete$",
                    ):
                        core.processing_publication_fields(forged_binding)

                    forged_final = copy.deepcopy(stage_receipts)
                    no_lf_final_sha = core.sha256_value(
                        forged_final["read_session"]["receipt"]
                    )
                    forged_final["read_session"][
                        "receipt_sha256"
                    ] = no_lf_final_sha
                    forged_final["read_session"]["receipt_ref"] = (
                        "study-intake-mcp-read-session://sha256/"
                        + no_lf_final_sha
                    )
                    with self.assertRaisesRegex(
                        core.PreprocessorError,
                        "^mcp_read_session_final_receipt_missing$",
                    ):
                        core.processing_publication_fields(forged_final)
                finally:
                    production.tearDown()


if __name__ == "__main__":
    unittest.main()
