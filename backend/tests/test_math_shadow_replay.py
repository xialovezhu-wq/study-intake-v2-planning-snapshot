from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "lib"))

import math_shadow_backaudit as backaudit  # noqa: E402
import math_shadow_replay as replay  # noqa: E402
from historical_test_input import (  # noqa: E402
    HistoricalTestInput,
    render_shadow_test_config,
)
from preprocessor_core import sha256_value  # noqa: E402

class MathShadowReplayTests(unittest.TestCase):
    def test_real_manifest_builds_ten_frozen_shadow_candidates(self) -> None:
        test_input = HistoricalTestInput.from_env()
        before = test_input.snapshot()
        manifest = test_input.load_historical_manifest()
        config = render_shadow_test_config(ROOT, test_input)
        backaudit.verify_manifest(manifest)
        candidates = [
            replay.build_replay_candidate(
                manifest=manifest, item=item, config=config, read_guard=test_input
            )
            for item in manifest["items"]
        ]
        after = test_input.snapshot()
        self.assertEqual(before, after)
        self.assertEqual(len(candidates), 10)
        for candidate, item in zip(candidates, manifest["items"]):
            binding = candidate.input_binding
            semantic_binding = dict(binding)
            semantic_binding.pop("loaded_core_sha256")
            self.assertEqual(candidate.input_fingerprint, sha256_value(semantic_binding))
            self.assertEqual(
                binding["backaudit_manifest_sha256"], manifest["manifest_sha256"]
            )
            self.assertEqual(
                binding["replay_input_sha256"], item["replay_input_sha256"]
            )
            self.assertEqual(candidate.canonical_state, "historical_shadow_replay")
            self.assertEqual(candidate.sol_state, "evaluation_only")
            if item["replay_status"] == "limited_by_evidence":
                historical = item["replay_input"]["formal_target"]["historical_card"]
                if historical["status"] == "historical_preimage_unavailable":
                    self.assertIsNone(candidate.model_input["formal_card"])
                    current_sha = item["current_formal_target"].get("sha256")
                    if current_sha and current_sha != historical.get("sha256"):
                        self.assertNotIn(
                            current_sha,
                            json.dumps(candidate.model_input, ensure_ascii=False),
                        )
            self.assertEqual(
                candidate.model_input["historical_replay"]["limitations"],
                [*item["replay_limitations"], "historical_math_knowledge_network_unavailable"],
            )
            coverage = candidate.model_input["knowledge_distribution_snapshot"]["coverage_manifest"]
            self.assertFalse(coverage["network_coverage_complete"])
            self.assertTrue(coverage["post_freeze_current_network_excluded"])

    def test_plan_hash_checks_are_read_only(self) -> None:
        test_input = HistoricalTestInput.from_env()
        before = test_input.snapshot()
        manifest = test_input.load_historical_manifest()
        config = render_shadow_test_config(ROOT, test_input)
        runtime = test_input.runtime_data_root
        repo = test_input.formal_surface_root
        before_runtime = replay._protected_runtime_hashes(runtime)
        before_formal = replay._formal_hashes(manifest, repo)
        replay.build_replay_candidate(
            manifest=manifest,
            item=manifest["items"][0],
            config=config,
            read_guard=test_input,
        )
        self.assertEqual(before_runtime, replay._protected_runtime_hashes(runtime))
        self.assertEqual(before_formal, replay._formal_hashes(manifest, repo))
        self.assertEqual(before, test_input.snapshot())


if __name__ == "__main__":
    unittest.main()
