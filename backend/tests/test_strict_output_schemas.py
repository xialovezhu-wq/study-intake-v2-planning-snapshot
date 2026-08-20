import copy
import json
import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from lib.semantic_contract_v3 import (
    SemanticContractError,
    decode_correction_delta,
    encode_correction_delta,
    validate_applied_correction_targets,
    validate_atomic_signals,
    validate_correction_resolutions,
)
from lib.preprocessor_core import (
    CS408_CRITICAL_REVIEW_PROMPT_TEMPLATE,
    CodexRunner,
    MATH_CRITICAL_REVIEW_PROMPT_TEMPLATE,
    PreprocessorError,
    _cs408_writable_correction_paths,
    _coalesce_cs408_split_correction_resolutions,
    english_review_semantic_draft,
    materialize_english_correction_deltas,
    sha256_value,
    validate_english_applied_corrections,
    validate_english_critical_review,
)

CRITICAL_REVIEW_SCHEMAS = (
    ROOT / "schemas" / "luna-english-critical-review-v1.json",
    ROOT / "schemas" / "luna-math-critical-review-v2.json",
    ROOT / "schemas" / "luna-critical-review-v2.json",
)


class StrictOutputSchemaTests(unittest.TestCase):
    def test_408_exact_per_path_resolution_split_is_coalesced(self):
        paths = [
            "$.formalization_candidates.main_knowledge[0]",
            "$.formalization_candidates.question_type[0]",
            "$.knowledge_network_context.knowledge_alignment[0]",
        ]
        rows = [
            {
                "finding_id": "LUNA-CR-EVIDENCE-001",
                "resolution": "applied",
                "affected_json_paths": [path],
                "before": encode_correction_delta({"old": path}),
                "after": encode_correction_delta({"new": path}),
            }
            for path in reversed(paths)
        ]

        normalized = _coalesce_cs408_split_correction_resolutions(
            rows,
            finding_paths={"LUNA-CR-EVIDENCE-001": paths},
        )

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["affected_json_paths"], paths)

    def test_408_incomplete_per_path_resolution_split_fails_closed(self):
        paths = ["$.executive_summary", "$.question_structure"]
        rows = [
            {
                "finding_id": "LUNA-CR-EVIDENCE-001",
                "resolution": "applied",
                "affected_json_paths": [paths[0]],
                "before": encode_correction_delta("old"),
                "after": encode_correction_delta("new"),
            },
            {
                "finding_id": "LUNA-CR-EVIDENCE-001",
                "resolution": "applied",
                "affected_json_paths": ["$.reasoning_diagnosis"],
                "before": encode_correction_delta("old"),
                "after": encode_correction_delta("new"),
            },
        ]

        with self.assertRaisesRegex(
            PreprocessorError, "critical_review_correction_paths_mismatch"
        ):
            _coalesce_cs408_split_correction_resolutions(
                rows,
                finding_paths={"LUNA-CR-EVIDENCE-001": paths},
            )

    def test_math_review_schema_binds_exact_relationship_decision_count(self):
        relationship_refs = tuple(
            f"relationship_context.candidates[{index}]" for index in range(5)
        )
        candidate_ids = tuple(f"GS-{index:03d}" for index in range(1, 6))
        payload, _ = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas" / "luna-math-critical-review-v3.json",
            stage_name="math_critical_review",
            allowed_evidence_refs=("capture.event_id",),
            allowed_analysis_refs=("$.executive_summary",),
            allowed_correction_paths=("$.executive_summary",),
            allowed_relationship_refs=relationship_refs,
            allowed_relationship_candidate_ids=candidate_ids,
        )
        schema = json.loads(payload)
        decisions = schema["properties"]["relationship_decisions"]
        self.assertEqual(decisions["minItems"], 5)
        self.assertEqual(decisions["maxItems"], 5)
        self.assertEqual(
            schema["$defs"]["relationship_candidate_id"]["enum"],
            list(candidate_ids),
        )
        self.assertEqual(
            schema["$defs"]["relationship_ref"]["enum"],
            list(relationship_refs),
        )
        self.assertIn(
            "required_relationship_decision_count",
            MATH_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        )

    def test_math_review_schema_forbids_decisions_without_candidates(self):
        payload, _ = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas" / "luna-math-critical-review-v3.json",
            stage_name="math_critical_review",
            allowed_evidence_refs=("capture.event_id",),
            allowed_analysis_refs=("$.executive_summary",),
            allowed_correction_paths=("$.executive_summary",),
            allowed_relationship_refs=(),
            allowed_relationship_candidate_ids=(),
        )
        decisions = json.loads(payload)["properties"][
            "relationship_decisions"
        ]
        self.assertEqual(decisions["minItems"], 0)
        self.assertEqual(decisions["maxItems"], 0)

    def test_english_provider_schema_requires_canonical_jsonpath(self):
        schema = json.loads(
            (ROOT / "schemas" / "luna-english-critical-review-v1.json").read_text(
                encoding="utf-8"
            )
        )
        pattern = schema["$defs"]["writableCorrectionPath"]["pattern"]
        self.assertNotIn(
            "uniqueItems",
            schema["$defs"]["finding"]["properties"]["affected_json_paths"],
        )
        self.assertNotIn(
            "uniqueItems",
            schema["$defs"]["correctionResolution"]["properties"][
                "affected_json_paths"
            ],
        )
        self.assertIsNotNone(re.fullmatch(pattern, "$.items[0].card.meaning"))
        self.assertIsNotNone(re.fullmatch(pattern, "$.items"))
        self.assertIsNone(re.fullmatch(pattern, "$/items/0/card/meaning"))
        self.assertIsNone(re.fullmatch(pattern, "$.host_bound"))

    def test_english_provider_schema_binds_exact_source_event_ids(self):
        event_ids = (
            "EVT-20260806-507660196D4F941D",
            "EVT-20260806-8F4D09BF50F3F1B3",
            "EVT-20260806-4DF6447806282008",
        )
        payload, _ = CodexRunner._bound_english_source_event_schema_bytes(
            ROOT / "schemas" / "luna-english-candidate-draft-v1.json",
            stage_name="english_analysis",
            allowed_source_event_ids=event_ids,
        )
        schema = json.loads(payload)
        self.assertEqual(
            schema["$defs"]["item"]["properties"]["source_event_id"]["enum"],
            list(event_ids),
        )
        self.assertNotIn(
            "EVT-20260806-8F4D09BF50F3B1B3",
            schema["$defs"]["item"]["properties"]["source_event_id"]["enum"],
        )

    def test_english_provider_schema_rejects_ambiguous_source_event_set(self):
        with self.assertRaisesRegex(
            PreprocessorError, "english_analysis_source_event_ids_invalid"
        ):
            CodexRunner._bound_english_source_event_schema_bytes(
                ROOT / "schemas" / "luna-english-candidate-draft-v1.json",
                stage_name="english_analysis",
                allowed_source_event_ids=("EVENT-1", "EVENT-1"),
            )

    def test_english_validator_rejects_json_pointer_correction_paths(self):
        draft = {"items": [{"item_id": "ITEM-1", "card": {"meaning": "old"}}]}
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "meaning_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct the meaning.",
                    "affected_json_paths": ["$/items/0/card/meaning"],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-1",
                    "resolution": "applied",
                    "affected_json_paths": ["$/items/0/card/meaning"],
                    "before": encode_correction_delta("old"),
                    "after": encode_correction_delta("new"),
                }
            ],
            "revised_items": [
                {"item_id": "ITEM-1", "card": {"meaning": "new"}}
            ],
        }

        with self.assertRaisesRegex(
            PreprocessorError, "english_required_correction_unresolved"
        ):
            validate_english_critical_review(review, draft)

    def test_english_validator_rejects_duplicate_correction_paths(self):
        draft = {"items": [{"item_id": "ITEM-1", "card": {"meaning": "old"}}]}
        duplicate_paths = [
            "$.items[0].card.meaning",
            "$.items[0].card.meaning",
        ]
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "meaning_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct the meaning.",
                    "affected_json_paths": duplicate_paths,
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-1",
                    "resolution": "applied",
                    "affected_json_paths": duplicate_paths,
                    "before": encode_correction_delta("old"),
                    "after": encode_correction_delta("new"),
                }
            ],
            "revised_items": [
                {"item_id": "ITEM-1", "card": {"meaning": "new"}}
            ],
        }

        with self.assertRaisesRegex(
            PreprocessorError, "english_critical_review_invalid"
        ):
            validate_english_critical_review(review, draft)

    def test_english_review_projection_excludes_host_owned_bindings(self):
        semantic_item = {
            "item_id": "ITEM-1",
            "sequence": 1,
            "item": "lasting",
            "candidate_type": "单词",
            "candidate_status": "familiarity_candidate",
            "tier": "A",
            "source_event_id": "EVENT-1",
            "bank_status": "new_candidate",
            "bank_match_ids": [],
            "mastered_status": "clear",
            "mastery_proposal": None,
            "grounding": {},
            "card": {},
        }
        projected = english_review_semantic_draft(
            {
                "items": [
                    {
                        **semantic_item,
                        "sentence_record_id": "HOST-SENTENCE-1",
                        "source_signal_ids": ["SIGNAL-1"],
                        "evidence_states": ["guided_understood"],
                        "evidence_origin": "live_user",
                        "user_evidence": ["unknown"],
                    }
                ],
                "candidate_id": "HOST-CANDIDATE-1",
            }
        )
        self.assertEqual(projected, {"items": [semantic_item]})

    def test_critical_review_schemas_have_no_open_or_empty_object_schema(self):
        for path in CRITICAL_REVIEW_SCHEMAS:
            with self.subTest(path=path.name):
                schema = json.loads(path.read_text(encoding="utf-8"))
                pending = [("$", schema)]
                while pending:
                    location, value = pending.pop()
                    self.assertNotEqual(value, {}, f"empty schema at {location}")
                    if isinstance(value, dict):
                        raw_type = value.get("type")
                        is_object = raw_type == "object" or (
                            isinstance(raw_type, list) and "object" in raw_type
                        )
                        properties = value.get("properties")
                        if is_object and isinstance(properties, dict):
                            self.assertIs(
                                value.get("additionalProperties"),
                                False,
                                f"open object at {location}",
                            )
                            self.assertEqual(
                                set(value.get("required") or []),
                                set(properties),
                                f"optional structured-output field at {location}",
                            )
                        is_array = raw_type == "array" or (
                            isinstance(raw_type, list) and "array" in raw_type
                        )
                        if is_array:
                            self.assertIn(
                                "items",
                                value,
                                f"array items schema missing at {location}",
                            )
                        pending.extend(
                            (f"{location}.{key}", nested)
                            for key, nested in value.items()
                        )
                    elif isinstance(value, list):
                        pending.extend(
                            (f"{location}[{index}]", nested)
                            for index, nested in enumerate(value)
                        )

    def test_correction_delta_wrapper_round_trips_arbitrary_json(self):
        value = ["OS04-15", {"edge_count": 2}]
        encoded = encode_correction_delta(value)
        self.assertEqual(decode_correction_delta(encoded), value)
        findings = [
            {
                "correction_id": "CORR-1",
                "severity": "blocking",
            }
        ]
        resolutions = [
            {
                "finding_id": "CORR-1",
                "resolution": "applied",
                "affected_json_paths": ["$.value"],
                "before": encode_correction_delta(None),
                "after": encoded,
            }
        ]
        validate_correction_resolutions(findings, resolutions)
        validate_applied_correction_targets({"value": value}, resolutions)

    def test_correction_delta_rejects_noncanonical_json(self):
        with self.assertRaisesRegex(
            SemanticContractError, "correction_delta_json_not_canonical"
        ):
            decode_correction_delta(
                {
                    "encoding": "canonical_json",
                    "canonical_json": '{"b": 2, "a": 1}',
                }
            )

    def test_optional_warning_resolution_is_allowed_and_required_error_remains_bound(self):
        findings = [
            {"finding_id": "WARN-1", "severity": "warning"},
            {"finding_id": "ERR-1", "severity": "error"},
        ]
        resolutions = [
            {
                "finding_id": "WARN-1",
                "resolution": "applied",
                "affected_json_paths": ["$.warning_value"],
                "before": encode_correction_delta("old warning"),
                "after": encode_correction_delta("new warning"),
            },
            {
                "finding_id": "ERR-1",
                "resolution": "applied",
                "affected_json_paths": ["$.error_value"],
                "before": encode_correction_delta("old error"),
                "after": encode_correction_delta("new error"),
            },
        ]

        validate_correction_resolutions(findings, resolutions)

    def test_duplicate_finding_ids_are_rejected_across_categories(self):
        findings = [
            {"finding_id": "DUP-1", "severity": "warning"},
            {"finding_id": "DUP-1", "severity": "error"},
        ]
        with self.assertRaisesRegex(
            SemanticContractError, "correction_finding_id_duplicate"
        ):
            validate_correction_resolutions(findings, [])

    def test_duplicate_or_orphan_correction_resolutions_are_rejected(self):
        findings = [{"finding_id": "REAL-1", "severity": "warning"}]
        resolution = {
            "finding_id": "ORPHAN-1",
            "resolution": "applied",
            "affected_json_paths": ["$.executive_summary"],
            "before": encode_correction_delta("old"),
            "after": encode_correction_delta("new"),
        }
        with self.assertRaisesRegex(
            SemanticContractError, "correction_resolution_binding_invalid"
        ):
            validate_correction_resolutions(
                findings, [resolution, dict(resolution)]
            )

    def test_correction_path_rejects_analysis_namespace_template(self):
        findings = [{"finding_id": "ERR-1", "severity": "error"}]
        resolution = {
            "finding_id": "ERR-1",
            "resolution": "applied",
            "affected_json_paths": ["${analysis.atomic_signals}"],
            "before": encode_correction_delta([]),
            "after": encode_correction_delta([]),
        }
        with self.assertRaisesRegex(
            SemanticContractError, "correction_resolution_invalid"
        ):
            validate_correction_resolutions(findings, [resolution])

    def test_408_critical_schema_returns_semantic_analysis_and_strict_paths(self):
        schema = json.loads(
            (ROOT / "schemas" / "luna-critical-review-v2.json").read_text(
                encoding="utf-8"
            )
        )
        analysis = schema["$defs"]["analysis"]
        host_fields = {
            "candidate_schema_version",
            "truth_match_matrix",
            "novel_knowledge_candidates",
            "existing_formal_edges",
            "coverage_manifest",
        }
        self.assertTrue(host_fields.isdisjoint(analysis["required"]))
        self.assertTrue(host_fields.isdisjoint(analysis["properties"]))
        writable_path = schema["$defs"]["writable_correction_path"]
        for definition in ("finding", "correction_resolution"):
            path_items = schema["$defs"][definition]["properties"][
                "affected_json_paths"
            ]["items"]
            self.assertEqual(
                path_items["$ref"], "#/$defs/writable_correction_path"
            )
            pattern = writable_path["pattern"]
            self.assertIsNotNone(re.fullmatch(pattern, "$.atomic_signals[0]"))
            self.assertIsNone(re.fullmatch(pattern, "${analysis.atomic_signals}"))
            self.assertIsNone(
                re.fullmatch(pattern, "$.existing_formal_edges[0]")
            )
        sol_priority = schema["$defs"]["sol_priority_finding"]
        self.assertEqual(
            sol_priority["properties"]["affected_json_paths"]["maxItems"], 0
        )
        self.assertEqual(
            sol_priority["properties"]["affected_json_paths"]["items"]["$ref"],
            "#/$defs/writable_correction_path",
        )
        self.assertEqual(schema["properties"]["correction_resolutions"]["maxItems"], 8)
        self.assertIn(
            "Every row must retain exactly one owning finding",
            schema["properties"]["correction_resolutions"]["description"],
        )
        self.assertIn(
            "never emit an orphan resolution",
            schema["$defs"]["correction_resolution"]["description"],
        )
        self.assertEqual(
            schema["$defs"]["correction_resolution"]["properties"][
                "resolution"
            ]["const"],
            "applied",
        )
        self.assertEqual(schema["$defs"]["findings"]["maxItems"], 8)
        self.assertEqual(schema["$defs"]["sol_priority_findings"]["maxItems"], 8)

    def test_408_critic_prompt_forbids_orphan_resolution_log(self):
        self.assertIn(
            "correction_resolutions 不是独立补丁日志",
            CS408_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        )
        self.assertIn(
            "即使修正已应用也绝不能删除 finding",
            CS408_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        )

    def test_408_dynamic_correction_paths_exclude_new_array_indexes(self):
        draft = {
            "executive_summary": "draft",
            "concept_method_analysis": {
                "boundary_conditions": [
                    {
                        "text": "existing item",
                        "evidence_refs": ["evidence.ref"],
                    }
                ]
            },
        }
        paths = _cs408_writable_correction_paths(draft)
        self.assertIn("$.concept_method_analysis.boundary_conditions", paths)
        self.assertIn("$.concept_method_analysis.boundary_conditions[0]", paths)
        self.assertNotIn("$.concept_method_analysis.boundary_conditions[1]", paths)
        self.assertNotIn(
            "$.concept_method_analysis.boundary_conditions[0].evidence_refs",
            paths,
        )
        payload, _ = CodexRunner._bound_output_schema_bytes(
            ROOT / "schemas" / "luna-critical-review-v2.json",
            stage_name="cs408_critical_review",
            allowed_evidence_refs=("evidence.ref",),
            allowed_analysis_refs=(
                "analysis.concept_method_analysis.boundary_conditions",
            ),
            allowed_correction_paths=paths,
        )
        schema = json.loads(payload)
        self.assertEqual(
            schema["$defs"]["writable_correction_path"]["enum"],
            list(paths),
        )

    def test_atomic_signals_are_normalized_by_stable_signal_id(self):
        def signal(signal_id: str) -> dict:
            return {
                "signal_id": signal_id,
                "signal_type": "method",
                "canonical_term": signal_id,
                "surface_form": signal_id,
                "importance": "secondary",
                "provenance": "frozen evidence",
                "evidence_refs": ["evidence.ref"],
                "confidence": "medium",
                "error_role": "none",
                "specificity": "specific method",
                "applicability_boundary": "current frozen evidence only",
                "truth_library_match_status": "unmatched",
            }

        normalized = validate_atomic_signals(
            [signal("SIG-M-02"), signal("SIG-E-01"), signal("SIG-M-01")],
            allowed_evidence_refs=["evidence.ref"],
        )
        self.assertEqual(
            [row["signal_id"] for row in normalized],
            ["SIG-E-01", "SIG-M-01", "SIG-M-02"],
        )

    def test_english_corrections_validate_semantic_replacement_before_host_enrichment(self):
        draft = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "bank_status": "needs_check",
                    "sentence_record_id": "HOST-SENTENCE-1",
                }
            ]
        }
        revised_item = {
            "item_id": "ITEM-1",
            "bank_status": "new_candidate",
        }
        finding = {
            "correction_id": "CORR-1",
            "code": "bank_status_fix",
            "severity": "blocking",
            "item_id": "ITEM-1",
            "message": "Use the evidence-bound bank status.",
            "affected_json_paths": ["$.items[0]"],
        }
        resolution = {
            "finding_id": "CORR-1",
            "resolution": "applied",
            "affected_json_paths": ["$.items[0]"],
            "before": encode_correction_delta(
                {"item_id": "ITEM-1", "bank_status": "needs_check"}
            ),
            "after": encode_correction_delta(revised_item),
        }
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [finding],
            "correction_resolutions": [resolution],
            "revised_items": [revised_item],
        }

        validate_english_critical_review(review, draft)
        validate_english_applied_corrections(review)

        with self.assertRaisesRegex(
            SemanticContractError, "correction_delta_not_applied"
        ):
            validate_applied_correction_targets(
                {
                    "items": [
                        {
                            **revised_item,
                            "sentence_record_id": "HOST-SENTENCE-1",
                        }
                    ]
                },
                [resolution],
            )

    def test_english_correction_paths_cannot_target_review_namespace(self):
        draft = {"items": []}
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "namespace_fix",
                    "severity": "blocking",
                    "item_id": None,
                    "message": "Invalid review namespace.",
                    "affected_json_paths": ["$.revised_items"],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-1",
                    "resolution": "applied",
                    "affected_json_paths": ["$.revised_items"],
                    "before": encode_correction_delta([]),
                    "after": encode_correction_delta([]),
                }
            ],
            "revised_items": [],
        }

        with self.assertRaisesRegex(
            PreprocessorError, "english_correction_target_invalid"
        ):
            validate_english_critical_review(review, draft)

    def test_english_blocking_correction_cannot_be_not_applicable(self):
        draft = {"items": []}
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "missing_signal",
                    "severity": "blocking",
                    "item_id": None,
                    "message": "The blocking correction must be applied.",
                    "affected_json_paths": ["$.items"],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-1",
                    "resolution": "not_applicable",
                    "affected_json_paths": ["$.items"],
                    "before": encode_correction_delta([]),
                    "after": encode_correction_delta([]),
                }
            ],
            "revised_items": [],
        }
        with self.assertRaisesRegex(
            PreprocessorError, "english_required_correction_not_applied"
        ):
            validate_english_critical_review(review, draft)

    def test_english_warning_and_blocking_corrections_can_both_be_applied(self):
        draft = {
            "items": [
                {"item_id": "ITEM-1", "bank_status": "needs_check"},
                {"item_id": "ITEM-2", "bank_status": "needs_check"},
            ]
        }
        revised_items = [
            {"item_id": "ITEM-1", "bank_status": "new_candidate"},
            {"item_id": "ITEM-2", "bank_status": "existing_bank"},
        ]
        findings = [
            {
                "correction_id": "CORR-WARNING",
                "code": "warning_fix",
                "severity": "warning",
                "item_id": "ITEM-1",
                "message": "Apply a supported warning-level refinement.",
                "affected_json_paths": ["$.items[0]"],
            },
            {
                "correction_id": "CORR-BLOCKING",
                "code": "blocking_fix",
                "severity": "blocking",
                "item_id": "ITEM-2",
                "message": "Apply the required correction.",
                "affected_json_paths": ["$.items[1]"],
            },
        ]
        resolutions = [
            {
                "finding_id": finding["correction_id"],
                "resolution": "applied",
                "affected_json_paths": finding["affected_json_paths"],
                "before": encode_correction_delta(draft["items"][index]),
                "after": encode_correction_delta(revised_items[index]),
            }
            for index, finding in enumerate(findings)
        ]
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": findings,
            "correction_resolutions": resolutions,
            "revised_items": revised_items,
        }

        validate_english_critical_review(review, draft)
        validate_english_applied_corrections(review)

    def test_english_partial_model_delta_is_materialized_from_revised_items(self):
        draft = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "card": {"meaning": "old", "usage": "source-backed"},
                }
            ]
        }
        revised_items = [
            {
                "item_id": "ITEM-1",
                "card": {"meaning": "new", "usage": "source-backed"},
            }
        ]
        raw_resolution = {
            "finding_id": "CORR-1",
            "resolution": "applied",
            "affected_json_paths": ["$.items[0].card"],
            "before": encode_correction_delta({"meaning": "old"}),
            "after": encode_correction_delta({"meaning": "new"}),
        }
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "card_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct the model-owned card.",
                    "affected_json_paths": ["$.items[0].card"],
                }
            ],
            "correction_resolutions": [raw_resolution],
            "revised_items": revised_items,
        }

        validate_english_critical_review(review, draft)
        effective = materialize_english_correction_deltas(review, draft)
        validate_english_applied_corrections(effective)
        self.assertEqual(
            decode_correction_delta(
                effective["correction_resolutions"][0]["before"]
            ),
            draft["items"][0]["card"],
        )
        self.assertEqual(
            decode_correction_delta(
                effective["correction_resolutions"][0]["after"]
            ),
            revised_items[0]["card"],
        )
        self.assertEqual(
            decode_correction_delta(review["correction_resolutions"][0]["after"]),
            {"meaning": "new"},
        )

    def test_english_host_materializes_noncanonical_multi_path_delta_first(self):
        draft = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "card": {"meaning": "old", "usage": "old usage"},
                }
            ]
        }
        revised_items = [
            {
                "item_id": "ITEM-1",
                "card": {"meaning": "new", "usage": "new usage"},
            }
        ]
        paths = [
            "$.items[0].card.meaning",
            "$.items[0].card.usage",
        ]
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-MULTI-1",
                    "code": "multi_field_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct both model-owned fields.",
                    "affected_json_paths": paths,
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-MULTI-1",
                    "resolution": "applied",
                    "affected_json_paths": paths,
                    "before": {
                        "encoding": "canonical_json",
                        "canonical_json": '{"usage": "old usage", "meaning": "old"}',
                    },
                    "after": {
                        "encoding": "canonical_json",
                        "canonical_json": '{"usage": "new usage", "meaning": "new"}',
                    },
                }
            ],
            "revised_items": revised_items,
        }

        effective = materialize_english_correction_deltas(review, draft)
        validate_english_critical_review(effective, draft)
        validate_english_applied_corrections(effective)
        self.assertEqual(
            decode_correction_delta(
                effective["correction_resolutions"][0]["after"]
            ),
            {
                "$.items[0].card.meaning": "new",
                "$.items[0].card.usage": "new usage",
            },
        )

    def test_english_missing_blocking_resolution_is_synthesized_for_real_addition(self):
        path = "$.items[0].card.source_translation"
        draft = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "card": {"meaning": "old"},
                }
            ]
        }
        revised_items = [
            {
                "item_id": "ITEM-1",
                "card": {
                    "meaning": "old",
                    "source_translation": "来源句纠正译义",
                },
            }
        ]
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-ADD-1",
                    "code": "source_translation_added",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Add the separated source translation.",
                    "affected_json_paths": [path],
                }
            ],
            "correction_resolutions": [],
            "revised_items": revised_items,
        }

        effective = materialize_english_correction_deltas(review, draft)
        validate_english_critical_review(effective, draft)
        validate_english_applied_corrections(effective)
        resolution = effective["correction_resolutions"][0]
        self.assertEqual(resolution["finding_id"], "CORR-ADD-1")
        self.assertEqual(
            decode_correction_delta(resolution["before"]),
            {"host_path_state": "absent", "path": path},
        )
        self.assertEqual(
            decode_correction_delta(resolution["after"]),
            "来源句纠正译义",
        )

    def test_english_missing_blocking_resolution_stays_unresolved_for_no_op(self):
        path = "$.items[0].card.meaning"
        draft = {
            "items": [
                {"item_id": "ITEM-1", "card": {"meaning": "old"}}
            ]
        }
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-NOOP-1",
                    "code": "meaning_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "The declared change must be real.",
                    "affected_json_paths": [path],
                }
            ],
            "correction_resolutions": [],
            "revised_items": copy.deepcopy(draft["items"]),
        }

        effective = materialize_english_correction_deltas(review, draft)
        with self.assertRaisesRegex(
            PreprocessorError, "english_required_correction_unresolved"
        ):
            validate_english_critical_review(effective, draft)

    def test_english_correction_path_has_exactly_one_finding_owner(self):
        path = "$.items[0].card.meaning"
        draft = {
            "items": [
                {"item_id": "ITEM-1", "card": {"meaning": "old"}}
            ]
        }
        revised_items = [
            {"item_id": "ITEM-1", "card": {"meaning": "new"}}
        ]
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-OWNER-1",
                    "code": "first_owner",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "First owner.",
                    "affected_json_paths": [path],
                },
                {
                    "correction_id": "CORR-OWNER-2",
                    "code": "second_owner",
                    "severity": "warning",
                    "item_id": "ITEM-1",
                    "message": "Second owner is forbidden.",
                    "affected_json_paths": [path],
                },
            ],
            "correction_resolutions": [],
            "revised_items": revised_items,
        }

        with self.assertRaisesRegex(
            PreprocessorError,
            "english_correction_path_ownership_invalid",
        ):
            validate_english_critical_review(review, draft)

    def test_english_resolution_id_binds_by_unique_ordered_path_owner(self):
        path = "$.items[0].card.meaning"
        draft = {
            "items": [
                {"item_id": "ITEM-1", "card": {"meaning": "old"}}
            ]
        }
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-OWNER-1",
                    "code": "meaning_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct the meaning.",
                    "affected_json_paths": [path],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "F-001",
                    "resolution": "applied",
                    "affected_json_paths": [path],
                    "before": encode_correction_delta("old"),
                    "after": encode_correction_delta("new"),
                }
            ],
            "revised_items": [
                {"item_id": "ITEM-1", "card": {"meaning": "new"}}
            ],
        }

        effective = materialize_english_correction_deltas(review, draft)
        self.assertEqual(
            effective["correction_resolutions"][0]["finding_id"],
            "CORR-OWNER-1",
        )
        validate_english_critical_review(effective, draft)
        validate_english_applied_corrections(effective)

    def test_english_resolution_id_does_not_bind_on_different_paths(self):
        owned_path = "$.items[0].card.meaning"
        wrong_path = "$.items[0].card.usage"
        draft = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "card": {"meaning": "old", "usage": "old usage"},
                }
            ]
        }
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-OWNER-1",
                    "code": "meaning_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "Correct the meaning.",
                    "affected_json_paths": [owned_path],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "F-001",
                    "resolution": "applied",
                    "affected_json_paths": [wrong_path],
                    "before": encode_correction_delta("old usage"),
                    "after": encode_correction_delta("new usage"),
                }
            ],
            "revised_items": [
                {
                    "item_id": "ITEM-1",
                    "card": {"meaning": "new", "usage": "new usage"},
                }
            ],
        }

        effective = materialize_english_correction_deltas(review, draft)
        self.assertEqual(
            effective["correction_resolutions"][0]["finding_id"],
            "F-001",
        )
        with self.assertRaisesRegex(
            PreprocessorError, "english_required_correction_unresolved"
        ):
            validate_english_critical_review(effective, draft)

    def test_english_materialized_blocking_correction_rejects_no_op(self):
        draft = {"items": [{"item_id": "ITEM-1", "card": {"meaning": "old"}}]}
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "revised",
            "draft_analysis_sha256": sha256_value(draft),
            "findings": [
                {
                    "correction_id": "CORR-1",
                    "code": "card_fix",
                    "severity": "blocking",
                    "item_id": "ITEM-1",
                    "message": "A blocking correction must change the value.",
                    "affected_json_paths": ["$.items[0].card"],
                }
            ],
            "correction_resolutions": [
                {
                    "finding_id": "CORR-1",
                    "resolution": "applied",
                    "affected_json_paths": ["$.items[0].card"],
                    "before": encode_correction_delta({"meaning": "old"}),
                    "after": encode_correction_delta({"meaning": "new"}),
                }
            ],
            "revised_items": copy.deepcopy(draft["items"]),
        }

        with self.assertRaisesRegex(
            PreprocessorError, "english_required_correction_not_applied"
        ):
            materialize_english_correction_deltas(review, draft)


if __name__ == "__main__":
    unittest.main()
