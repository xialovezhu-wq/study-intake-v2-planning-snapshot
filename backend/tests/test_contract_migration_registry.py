#!/usr/bin/env python3

from __future__ import annotations

import importlib
import unittest


LEGACY_CONTRACT_MIGRATIONS = (
    (
        "tests.test_math_v2_core",
        "MathV2CoreTests",
        "legacy_contract_relationship_retrieval_is_deterministic_bounded_and_specific",
        "tests.test_model_driven_mcp_architecture",
        "ModelDrivenMcpArchitectureTests",
        "test_mcp_pagination_accepts_only_a_complete_contiguous_chain",
    ),
    (
        "tests.test_math_v2_core",
        "MathV2CoreTests",
        "legacy_contract_group_result_refreshes_all_aliases_and_persists",
        "tests.test_concurrent_dispatch",
        "ConcurrentDispatchTests",
        "test_scanner_starts_ten_distinct_math_units_and_read_sessions",
    ),
    (
        "tests.test_math_v2_core",
        "MathV2CoreTests",
        "legacy_contract_two_initial_group_members_use_one_math_run",
        "tests.test_concurrent_dispatch",
        "ConcurrentDispatchTests",
        "test_scanner_starts_ten_distinct_math_units_and_read_sessions",
    ),
    *(
        (
            "tests.test_math_v2_core",
            "MathV2CoreTests",
            legacy_name,
            "tests.test_model_driven_mcp_architecture",
            "ModelDrivenMcpArchitectureTests",
            "test_plugin_contains_only_model_driven_contract_generation",
        )
        for legacy_name in (
            "legacy_contract_corrupt_shadow_pointer_is_fail_soft_for_ready_baseline",
            "legacy_contract_missing_ready_baseline_receipt_is_recovered_before_shadow",
            "legacy_contract_ready_shadow_revalidates_and_recovers_baseline_receipt",
            "legacy_contract_shadow_ready_and_progress_require_full_artifact_closure",
            "legacy_contract_future20_eligibility_uses_hashed_generation_member",
            "legacy_contract_future20_requires_current_contract_and_live_baseline",
        )
    ),
    *(
        (
            "tests.test_preprocessor",
            "PreprocessorTests",
            legacy_name,
            replacement_module,
            replacement_class,
            replacement_name,
        )
        for legacy_name, replacement_module, replacement_class, replacement_name in (
            (
                "legacy_contract_single_pass_degraded_is_checkpoint_not_consumable_offer",
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_failed_model_submission_receipt_is_persisted_and_hmac_verified",
            ),
            (
                "legacy_contract_cs408_v2_second_stage_failures_degrade_only_a_valid_draft",
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_failed_model_submission_receipt_is_persisted_and_hmac_verified",
            ),
            (
                "legacy_contract_408_critical_semantic_correction_recomputes_host_candidate_v3",
                "tests.test_model_driven_mcp_architecture",
                "ModelDrivenMcpArchitectureTests",
                "test_runtime_packages_use_luna_proposal_v2_without_host_projection",
            ),
            (
                "legacy_contract_resume_critical_reuses_verified_analysis_and_upgrades_package",
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_final_read_session_receipt_aggregates_and_verifies_both_stages",
            ),
            (
                "legacy_contract_resume_critical_drift_or_failure_preserves_degraded_state",
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_tampered_read_session_hmac_fails_closed",
            ),
            (
                "legacy_contract_terminal_knowledge_advance_requires_explicit_replay_authority",
                "tests.test_model_driven_mcp_architecture",
                "ModelDrivenMcpArchitectureTests",
                "test_mcp_pagination_rejects_generation_change",
            ),
            (
                "legacy_contract_legacy_snapshot_binding_remains_read_only_and_v2_is_rebuilt",
                "tests.test_model_driven_mcp_architecture",
                "ModelDrivenMcpArchitectureTests",
                "test_three_production_flows_have_no_host_semantic_prefetch",
            ),
        )
    ),
    (
        "tests.test_english_adapter",
        "EnglishAdapterTests",
        "legacy_contract_model_input_is_bounded_prefetch_with_hashes_and_limits",
        "tests.test_model_driven_mcp_architecture",
        "ModelDrivenMcpArchitectureTests",
        "test_three_production_flows_have_no_host_semantic_prefetch",
    ),
)


LEGACY_CONTRACT_SUITES = (
    (
        "tests.test_math_v2_core",
        "MathV2CoreTests",
        21,
        (
            (
                "tests.test_model_driven_mcp_architecture",
                "ModelDrivenMcpArchitectureTests",
                "test_three_production_flows_have_no_host_semantic_prefetch",
            ),
            (
                "tests.test_concurrent_dispatch",
                "ConcurrentDispatchTests",
                "test_scanner_starts_ten_distinct_math_units_and_read_sessions",
            ),
            (
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_published_read_session_reopens_only_persisted_calls",
            ),
        ),
    ),
    (
        "tests.test_preprocessor",
        "PreprocessorTests",
        26,
        (
            (
                "tests.test_preprocessor",
                "PreprocessorTests",
                "test_contract_v2_runner_uses_portable_provider_with_exact_analysis_ref_enum",
            ),
            (
                "tests.test_luna_proposal_closure",
                "LunaProposalClosureTests",
                "test_package_requires_every_publication_field_and_exact_value",
            ),
            (
                "tests.test_concurrent_dispatch",
                "ConcurrentDispatchTests",
                "test_usage_limit_is_terminal_and_never_loops_as_retry_wait",
            ),
            (
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_post_validation_failure_signs_parsed_calls_for_all_subjects",
            ),
        ),
    ),
    (
        "tests.test_english_adapter",
        "EnglishAdapterTests",
        7,
        (
            (
                "tests.test_luna_proposal_closure",
                "LunaProposalClosureTests",
                "test_english_exact_package_keys_and_pointer_publication",
            ),
            (
                "tests.test_processing_plugin",
                "ProcessingPluginHostTests",
                "test_post_validation_failure_signs_parsed_calls_for_all_subjects",
            ),
            (
                "tests.test_model_driven_mcp_architecture",
                "ModelDrivenMcpArchitectureTests",
                "test_three_production_flows_have_no_host_semantic_prefetch",
            ),
        ),
    ),
)


class ContractMigrationRegistryTests(unittest.TestCase):
    def test_retired_contract_tests_have_discoverable_replacements(self) -> None:
        self.assertGreaterEqual(len(LEGACY_CONTRACT_MIGRATIONS), 17)
        for (
            legacy_module_name,
            legacy_class_name,
            legacy_method_name,
            replacement_module_name,
            replacement_class_name,
            replacement_method_name,
        ) in LEGACY_CONTRACT_MIGRATIONS:
            with self.subTest(legacy=legacy_method_name):
                legacy_module = importlib.import_module(legacy_module_name)
                legacy_class = getattr(legacy_module, legacy_class_name)
                self.assertTrue(hasattr(legacy_class, legacy_method_name))
                self.assertFalse(legacy_method_name.startswith("test_"))

                replacement_module = importlib.import_module(
                    replacement_module_name
                )
                replacement_class = getattr(
                    replacement_module, replacement_class_name
                )
                self.assertTrue(
                    replacement_method_name.startswith("test_")
                    and hasattr(replacement_class, replacement_method_name)
                )

    def test_every_legacy_contract_is_explicit_and_has_modern_gate_suites(self) -> None:
        for (
            legacy_module_name,
            legacy_class_name,
            expected_count,
            replacements,
        ) in LEGACY_CONTRACT_SUITES:
            with self.subTest(module=legacy_module_name):
                legacy_module = importlib.import_module(legacy_module_name)
                legacy_class = getattr(legacy_module, legacy_class_name)
                names = sorted(
                    name
                    for name in vars(legacy_class)
                    if name.startswith("legacy_contract_")
                )
                self.assertEqual(len(names), expected_count, names)
                self.assertTrue(
                    all(not name.startswith("test_") for name in names)
                )
                for module_name, class_name, method_name in replacements:
                    module = importlib.import_module(module_name)
                    replacement_class = getattr(module, class_name)
                    self.assertTrue(
                        method_name.startswith("test_")
                        and hasattr(replacement_class, method_name),
                        (module_name, class_name, method_name),
                    )


if __name__ == "__main__":
    unittest.main()
