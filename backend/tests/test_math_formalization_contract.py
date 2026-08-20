#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import inspect
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import preprocessor_core as core  # noqa: E402
import math_shadow_replay as replay  # noqa: E402
from historical_test_input import (  # noqa: E402
    HistoricalTestInput,
    render_shadow_test_config,
)
from tests.test_math_v2_core import FakeMathRunner  # noqa: E402


SEALED_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-4c1c0651/stage-runtime"
)
SEALED_OUTPUT = (
    SEALED_ROOT
    / "private/reports/model-stage-outputs/objects/"
    "6d10369cabfc3282733f6112d3ee306f356870a9b7ee7504992f68f90b6efc2c.json"
)
SEALED_TRANSPORT = (
    SEALED_ROOT
    / "private/reports/model-mcp-transport/sha256/9b/"
    "9bb1d2caddcdf2d2f447531ffb97edf95797efe7ef3cd86b1829d1c928c01d7e.json"
)
SEALED_TRANSCRIPT = (
    SEALED_ROOT
    / "private/reports/mcp-stage-transcripts/sha256/d3/"
    "d39fa151cdb7a9c5ab711bdd443352eabffc9a36d7625e8ecbce789f580e711d.json"
)
SEALED_READ_SESSION = (
    SEALED_ROOT
    / "private/mcp-read-sessions/sha256/30/"
    "30dc294890f795eefae6b2c717e5c8dd144d432258ee8ee43ef0305b1433e954.json"
)
SEALED_FAILURE_RECEIPT = (
    SEALED_ROOT
    / "dispatch/receipts/sha256/75/"
    "75f707b8ee0dc843f55aa8d6136980dd8d9c5a6224d196930fbd33deb584989f.json"
)
SEALED_PROVIDER_SCHEMA = (
    SEALED_ROOT
    / "private/reports/provider-schemas/sha256/4c/"
    "4c5bde1b6187cb9ce85056f67aa60ed619cf2537aada58156a4dab72f3a1b37a.json"
)
SEALED_INVALID_PROVIDER_SCHEMA = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
    "three-subject-direct-mcp-en-p0-006-20260809/artifacts/"
    "three-real-smoke-8a4c742e/stage-runtime/private/reports/"
    "provider-schemas/sha256/a7/"
    "a739ba3d91aec4849dcb6d0ca2451e4ce3b857d538d7cd70e6cc57aa273b6cf2.json"
)
SEALED_JOB = SEALED_ROOT / "state/jobs/math/LUNA-MATH-20260809-003.json"
HISTORICAL_TEST_INPUT = Path(
    os.environ.get(
        "STUDY_PREPROCESSOR_HISTORICAL_TEST_INPUT_MANIFEST",
        "/Users/xiazhibin/.codex/study-intake-preprocessor/deployments/"
        "three-subject-golden-replay-modernization-20260809/artifacts/"
        "historical-test-input/"
        "077f74a14d1559cdff07ecfde8f1bcd800f830efefe75a984f6fc294ef136f0a.json",
    )
)
JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")

EXPECTED_FILE_SHA256S = {
    SEALED_OUTPUT: "6d10369cabfc3282733f6112d3ee306f356870a9b7ee7504992f68f90b6efc2c",
    SEALED_TRANSPORT: "9bb1d2caddcdf2d2f447531ffb97edf95797efe7ef3cd86b1829d1c928c01d7e",
    SEALED_TRANSCRIPT: "d39fa151cdb7a9c5ab711bdd443352eabffc9a36d7625e8ecbce789f580e711d",
    SEALED_READ_SESSION: "21202d46b23e3647ece1ffe6438754b74c476540da255fa88d9dc13686d602fb",
    SEALED_FAILURE_RECEIPT: "75f707b8ee0dc843f55aa8d6136980dd8d9c5a6224d196930fbd33deb584989f",
    SEALED_PROVIDER_SCHEMA: "4c5bde1b6187cb9ce85056f67aa60ed619cf2537aada58156a4dab72f3a1b37a",
    SEALED_INVALID_PROVIDER_SCHEMA: "a739ba3d91aec4849dcb6d0ca2451e4ce3b857d538d7cd70e6cc57aa273b6cf2",
    HISTORICAL_TEST_INPUT: HISTORICAL_TEST_INPUT.stem,
}


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def walk_objects(value: object):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_objects(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from walk_objects(nested)


class MathFormalizationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        for path, expected in EXPECTED_FILE_SHA256S.items():
            if not path.is_file():
                raise AssertionError(f"sealed evidence missing: {path}")
            actual = file_sha256(path)
            if actual != expected:
                raise AssertionError(
                    f"sealed evidence drift: {path}: {actual} != {expected}"
                )
        cls.output_document = json.loads(SEALED_OUTPUT.read_text())
        cls.payload = cls.output_document["payload"]
        cls.transport = json.loads(SEALED_TRANSPORT.read_text())
        cls.transcript = json.loads(SEALED_TRANSCRIPT.read_text())
        cls.read_session = json.loads(SEALED_READ_SESSION.read_text())
        cls.failure_receipt = json.loads(SEALED_FAILURE_RECEIPT.read_text())
        cls.provider_schema = json.loads(SEALED_PROVIDER_SCHEMA.read_text())
        cls.invalid_provider_schema = json.loads(
            SEALED_INVALID_PROVIDER_SCHEMA.read_text()
        )
        cls.job = json.loads(SEALED_JOB.read_text())
        cls.evidence_refs = tuple(
            sorted(
                {
                    ref
                    for item in walk_objects(cls.payload)
                    for ref in (item.get("evidence_refs") or [])
                    if isinstance(ref, str)
                }
            )
        )

    def source_candidate(self) -> core.Candidate:
        binding = copy.deepcopy(self.job["input_binding"])
        return core.Candidate(
            subject="math",
            capture_id="LUNA-MATH-20260809-003",
            study_date="2026-08-09",
            recorded_at="2026-08-09T12:33:40+08:00",
            input_fingerprint="5947e0d054db9b6d8092336b4f45825a858ad178559e94548dbfa417752ca8e6",
            input_binding=binding,
            model_input={
                "source_bundle": {"source_kind": binding["source_route"]}
            },
            allowed_evidence_refs=self.evidence_refs,
            image_paths=(),
            target_label="source:question-bank-id:170710",
            canonical_state="awaiting_background_analysis",
            sol_state="not_authorized",
        )

    def no_source_candidate(self) -> core.Candidate:
        binding = copy.deepcopy(self.job["input_binding"])
        for key in (
            "source_route",
            "evidence_manifest_sha256",
            "evidence_bundle_sha256",
        ):
            binding.pop(key, None)
        return core.Candidate(
            subject="math",
            capture_id="LUNA-MATH-20260809-003",
            study_date="2026-08-09",
            recorded_at="2026-08-09T12:33:40+08:00",
            input_fingerprint="0" * 64,
            input_binding=binding,
            model_input={"source_bundle": None},
            allowed_evidence_refs=self.evidence_refs,
            image_paths=(),
            target_label="no-source-bundle",
            canonical_state="awaiting_background_analysis",
            sol_state="not_authorized",
        )

    @staticmethod
    def role_claim(source: dict[str, object], *, text: str) -> dict[str, object]:
        claim = copy.deepcopy(source)
        claim["text"] = text
        claim["counterevidence_or_boundary"] = (
            "仅整理本次 capture 已证明的业务事实，不据此确认正式库身份、知识节点或关系。"
        )
        claim["sol_verification_action"] = (
            "Sol 后续独立重开题面、来源答案和用户过程；正式身份与关系另查正式库。"
        )
        return claim

    def corrected_capture_overlay(self) -> dict[str, object]:
        payload = copy.deepcopy(self.payload)
        payload["formalization_candidates"].update(
            {
                "safe_summary": [
                    self.role_claim(
                        payload["evidence_assessment"]["observed_facts"][0],
                        text=(
                            "本次新来源学习记录显示用户先错后在讲解后理解；当前不等同于独立掌握。"
                        ),
                    )
                ],
                "question_body": [
                    self.role_claim(
                        payload["question_structure"]["asked_task"][0],
                        text=(
                            "题面要求处理正弦曲线与横轴围成区域绕固定纵轴的旋转体积及相关极限。"
                        ),
                    )
                ],
                "source_and_answer": [
                    self.role_claim(
                        payload["question_structure"]["source_answer"][0],
                        text=(
                            "capture 中的来源解答给出柱壳积分路径及对应源结果，仍需 Sol 重开核对。"
                        ),
                    )
                ],
                "wrong_point": [
                    self.role_claim(
                        payload["reasoning_diagnosis"]["first_break"],
                        text=(
                            "首个断点是把周期拱形的平移误当成绕固定纵轴时旋转半径不变。"
                        ),
                    )
                ],
                "methods": [
                    self.role_claim(
                        payload["question_structure"]["objects"][0],
                        text=(
                            "capture 支持以柱壳积分区分到固定旋转轴的半径与区域高度。"
                        ),
                    )
                ],
            }
        )
        return payload

    def test_sealed_transport_and_original_shape_reproduce_failure(self) -> None:
        self.assertEqual(self.transport["mcp_item_count"], 13)
        self.assertEqual(len(self.transcript["calls"]), 12)
        self.assertEqual(len(self.read_session["artifact_ids"]), 10)
        last = self.transport["mcp_items"][-1]["item"]
        self.assertEqual(last["tool"], "search_records")
        self.assertEqual(last["arguments"], {"query": "旋转体", "page_size": 48})
        self.assertEqual(
            last["result"]["structured_content"]["error"]["code"],
            "OUTPUT_LIMIT",
        )
        self.assertEqual(
            self.failure_receipt["error_code"],
            "math_analysis_formal_field_coverage_failed",
        )
        with self.assertRaisesRegex(
            core.PreprocessorError,
            "math_analysis_formal_field_coverage_failed",
        ):
            core.validate_math_semantic_gates(
                copy.deepcopy(self.payload), self.source_candidate()
            )

    def test_corrected_capture_ref_overlay_passes_without_identity_invention(self) -> None:
        overlay = self.corrected_capture_overlay()
        validated = core.validate_math_semantic_gates(
            overlay, self.source_candidate()
        )
        for field in core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS:
            self.assertEqual(len(validated["formalization_candidates"][field]), 1)
        self.assertEqual(
            validated["formalization_candidates"]["relationship_proposals"],
            [],
        )
        self.assertEqual(
            validated["target_identity"]["formal_target"][0]["claim_type"],
            "unresolved",
        )
        used = {
            ref
            for field in core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS
            for claim in validated["formalization_candidates"][field]
            for ref in claim["evidence_refs"]
        }
        self.assertTrue(used)
        self.assertLessEqual(used, set(self.evidence_refs))

    def test_missing_source_bundle_does_not_force_capture_fields(self) -> None:
        self.assertFalse(
            core._math_verified_source_bundle_formalization_mode(
                self.no_source_candidate().input_binding,
                self.no_source_candidate().model_input,
            )
        )
        validated = core.validate_math_semantic_gates(
            copy.deepcopy(self.payload), self.no_source_candidate()
        )
        self.assertTrue(
            all(
                not validated["formalization_candidates"][field]
                for field in core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS
            )
        )

    def test_unverified_source_binding_fails_closed(self) -> None:
        candidate = self.source_candidate()
        binding = copy.deepcopy(candidate.input_binding)
        binding.pop("evidence_bundle_sha256")
        with self.assertRaisesRegex(
            core.PreprocessorError,
            "math_source_bundle_formalization_binding_invalid",
        ):
            core._math_verified_source_bundle_formalization_mode(
                binding, candidate.model_input
            )

    def test_historical_manifest_replay_keeps_original_publish_signature(self) -> None:
        test_input = HistoricalTestInput.load(HISTORICAL_TEST_INPUT)
        protected_before = test_input.snapshot()
        manifest = test_input.load_historical_manifest()
        config = copy.deepcopy(render_shadow_test_config(ROOT, test_input))
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "historical"
            config["runtime_root"] = str(runtime)
            config["worker"]["log_path"] = str(runtime / "logs/worker.log")
            config["worker"]["lock_path"] = str(
                runtime / "state/worker.lock"
            )
            candidate = replay.build_replay_candidate(
                manifest=manifest,
                item=manifest["items"][0],
                config=config,
                read_guard=test_input,
            )
            self.assertIsNone(candidate.input_binding.get("source_route"))
            self.assertIsInstance(candidate.model_input.get("source_bundle"), dict)
            self.assertFalse(
                core._math_verified_source_bundle_formalization_mode(
                    candidate.input_binding,
                    candidate.model_input,
                )
            )
            with self.assertRaisesRegex(
                core.PreprocessorError,
                "math_critical_review_dynamic_schema_mismatch",
            ):
                core.Worker(
                    config, model_runner=FakeMathRunner()
                ).publish_math_shadow_candidate(candidate)
        self.assertEqual(protected_before, test_input.snapshot())

    def test_unbound_live_source_bundle_still_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            core.PreprocessorError,
            "math_source_bundle_formalization_mode_invalid",
        ):
            core._math_verified_source_bundle_formalization_mode(
                {},
                {
                    "source_bundle": {
                        "manifest_hash": "f" * 64,
                    }
                },
            )

    def test_unknown_evidence_ref_is_not_filled_or_normalized(self) -> None:
        overlay = self.corrected_capture_overlay()
        overlay["formalization_candidates"]["safe_summary"][0][
            "evidence_refs"
        ] = ["mcp-item:math:" + "f" * 64]
        with self.assertRaisesRegex(
            core.PreprocessorError,
            "math_analysis_evidence_refs_invalid",
        ):
            core.validate_math_semantic_gates(
                overlay, self.source_candidate()
            )

    def test_dynamic_provider_schema_binds_only_verified_source_mode(self) -> None:
        analysis_schema = ROOT / "schemas/luna-math-analysis-v2.json"
        review_schema = ROOT / "schemas/luna-math-critical-review-v2.json"
        static_before = {
            analysis_schema: analysis_schema.read_bytes(),
            review_schema: review_schema.read_bytes(),
        }
        overlay = self.corrected_capture_overlay()
        analysis_refs = core._analysis_review_refs(overlay)
        correction_paths = core._math_writable_correction_paths(overlay)

        bound_analysis, _ = core.CodexRunner._bound_output_schema_bytes(
            analysis_schema,
            stage_name="math_analysis",
            allowed_evidence_refs=(),
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        bound_review, _ = core.CodexRunner._bound_output_schema_bytes(
            review_schema,
            stage_name="math_critical_review",
            allowed_evidence_refs=(),
            allowed_analysis_refs=analysis_refs,
            allowed_correction_paths=correction_paths,
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        unbound_analysis, _ = core.CodexRunner._bound_output_schema_bytes(
            analysis_schema,
            stage_name="math_analysis",
            allowed_evidence_refs=(),
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=False,
        )
        for payload in (bound_analysis, bound_review):
            schema = json.loads(payload)
            required_claims = schema["$defs"][
                "source_bundle_required_claims"
            ]
            self.assertEqual(required_claims["type"], "array")
            self.assertEqual(
                required_claims["items"], {"$ref": "#/$defs/claim"}
            )
            self.assertEqual(required_claims["minItems"], 1)
            self.assertEqual(required_claims["maxItems"], 1)
            properties = schema["$defs"]["formalization_candidates"][
                "properties"
            ]
            for field in core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS:
                self.assertEqual(
                    properties[field],
                    {"$ref": "#/$defs/source_bundle_required_claims"},
                )
            self.assertNotIn("minItems", properties["relationship_proposals"])
            core._validate_provider_schema_ref_siblings(
                schema,
                stage_name="math_test",
            )
        unbound = json.loads(unbound_analysis)
        self.assertNotIn("source_bundle_required_claims", unbound["$defs"])
        for field in core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS:
            self.assertEqual(
                unbound["$defs"]["formalization_candidates"]["properties"][field],
                {"$ref": "#/$defs/claims"},
            )
        for path, before in static_before.items():
            self.assertEqual(path.read_bytes(), before)

    def test_sealed_a739_ref_siblings_fail_before_provider(self) -> None:
        sibling_paths: list[tuple[str, tuple[str, ...]]] = []

        def visit(value: object, path: str = "$") -> None:
            if isinstance(value, dict):
                if "$ref" in value and len(value) > 1:
                    sibling_paths.append(
                        (
                            path,
                            tuple(sorted(key for key in value if key != "$ref")),
                        )
                    )
                for key in sorted(value):
                    visit(value[key], f"{path}/{key}")
            elif isinstance(value, list):
                for index, nested in enumerate(value):
                    visit(nested, f"{path}/{index}")

        visit(self.invalid_provider_schema)
        expected_paths = [
            (
                "$/$defs/formalization_candidates/properties/" + field,
                ("maxItems", "minItems"),
            )
            for field in sorted(core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS)
        ]
        self.assertEqual(sibling_paths, expected_paths)

        with self.assertRaises(core.PreprocessorError) as direct:
            core._validate_provider_schema_ref_siblings(
                self.invalid_provider_schema,
                stage_name="math_analysis",
            )
        self.assertEqual(
            direct.exception.code,
            "math_analysis_output_schema_ref_sibling_unsupported",
        )
        self.assertEqual(
            direct.exception.diagnostic,
            {
                "schema_path": (
                    "$/$defs/formalization_candidates/properties/methods"
                ),
                "sibling_keywords": "maxItems,minItems",
            },
        )

        with self.assertRaises(core.PreprocessorError) as integrated:
            core.CodexRunner._bound_output_schema_bytes(
                SEALED_INVALID_PROVIDER_SCHEMA,
                stage_name="math_analysis",
                allowed_evidence_refs=(),
                allow_empty_predeclared_evidence_refs=True,
            )
        self.assertEqual(integrated.exception.code, direct.exception.code)
        self.assertEqual(
            integrated.exception.diagnostic,
            direct.exception.diagnostic,
        )

    def test_corrected_bound_schema_rejects_empty_capture_fields(self) -> None:
        self.assertTrue(JSONSCHEMA_PYTHON.is_file())
        payload, _ = core.CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas/luna-math-analysis-v2.json",
            stage_name="math_analysis",
            allowed_evidence_refs=(),
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        schema = json.loads(payload)
        validator_script = """
import json
import sys
from jsonschema import Draft202012Validator

schema = json.loads(open(sys.argv[1], encoding="utf-8").read())
instance = json.loads(open(sys.argv[2], encoding="utf-8").read())
errors = sorted(
    Draft202012Validator(schema).iter_errors(instance),
    key=lambda error: tuple(str(part) for part in error.absolute_path),
)
print(json.dumps([
    {
        "instance_path": "/" + "/".join(
            str(part) for part in error.absolute_path
        ),
        "validator": error.validator,
    }
    for error in errors
], sort_keys=True))
raise SystemExit(1 if errors else 0)
"""

        def validate(instance: dict[str, object]) -> subprocess.CompletedProcess[str]:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                schema_path = root / "schema.json"
                instance_path = root / "instance.json"
                schema_path.write_text(json.dumps(schema), encoding="utf-8")
                instance_path.write_text(json.dumps(instance), encoding="utf-8")
                return subprocess.run(
                    [
                        str(JSONSCHEMA_PYTHON),
                        "-c",
                        validator_script,
                        str(schema_path),
                        str(instance_path),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

        rejected = validate(copy.deepcopy(self.payload))
        self.assertEqual(rejected.returncode, 1, rejected.stderr)
        self.assertEqual(
            json.loads(rejected.stdout),
            [
                {
                    "instance_path": (
                        "/formalization_candidates/" + field
                    ),
                    "validator": "minItems",
                }
                for field in sorted(core.MATH_CAPTURE_BACKED_FORMALIZATION_FIELDS)
            ],
        )

        overlay = self.corrected_capture_overlay()
        accepted = validate(overlay)
        self.assertEqual(accepted.returncode, 0, accepted.stdout + accepted.stderr)
        self.assertEqual(json.loads(accepted.stdout), [])
        core.validate_math_semantic_gates(overlay, self.source_candidate())

    def test_stage_and_reopen_schema_hashes_use_identical_mode(self) -> None:
        overlay = self.corrected_capture_overlay()
        profile = {
            "analysis_output_schema": str(
                ROOT / "schemas/luna-math-analysis-v2.json"
            ),
            "critical_review_output_schema": str(
                ROOT / "schemas/luna-math-critical-review-v2.json"
            ),
        }
        expected = core._expected_math_dynamic_schema_sha256s(
            profile,
            allowed_evidence_refs=(),
            image_evidence_refs=(),
            draft_analysis=overlay,
            relationship_context={},
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        _, direct_analysis = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["analysis_output_schema"]),
            stage_name="math_analysis",
            allowed_evidence_refs=(),
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        _, direct_review = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["critical_review_output_schema"]),
            stage_name="math_critical_review",
            allowed_evidence_refs=(),
            allowed_analysis_refs=core._analysis_review_refs(overlay),
            allowed_correction_paths=core._math_writable_correction_paths(overlay),
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        self.assertEqual(expected, {
            "analysis": direct_analysis,
            "critical_review": direct_review,
        })
        self.assertNotEqual(
            direct_analysis,
            EXPECTED_FILE_SHA256S[SEALED_INVALID_PROVIDER_SCHEMA],
        )
        for callable_object in (
            core.CodexRunner.run_math_v2,
            core.CodexRunner._resume_math_critical,
            core.CodexRunner.load_analysis_checkpoint,
            core.validate_math_v2_artifact_closure,
            core.Worker._validate_math_group_result,
            core.Worker._write_math_v2_artifacts,
        ):
            with self.subTest(callable=callable_object.__qualname__):
                self.assertIn(
                    "math_source_bundle_formalization_mode",
                    inspect.getsource(callable_object),
                )

    def test_only_completeness_change_exposes_depth_gate(self) -> None:
        complete = copy.deepcopy(self.payload)
        complete["evidence_assessment"]["completeness"] = "complete"
        with self.assertRaisesRegex(
            core.PreprocessorError,
            "math_analysis_depth_gate_failed",
        ):
            core.validate_math_semantic_gates(
                complete, self.source_candidate()
            )

    def test_prompt_separates_capture_fields_from_library_identity(self) -> None:
        self.assertIn(
            "capture_backed_formalization_mode=true",
            core.MATH_ANALYSIS_PROMPT_TEMPLATE,
        )
        self.assertIn("48、24、12、6、3、1", core.MATH_ANALYSIS_PROMPT_TEMPLATE)
        self.assertIn("library-dependent", core.MATH_ANALYSIS_PROMPT_TEMPLATE)
        runner = core.CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            ROOT,
        )
        prompt = runner._math_analysis_prompt(
            self.source_candidate(),
            {"analysis_prompt_version": "sealed-contract-test"},
        )
        self.assertIn('"capture_backed_formalization_mode": true', prompt)


if __name__ == "__main__":
    unittest.main()
