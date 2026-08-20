import hashlib
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    ENGLISH_CANDIDATE_PROMPT_TEMPLATE,
    ENGLISH_CRITICAL_REVIEW_PROMPT_TEMPLATE,
)


SHARED_CONTRACT = (
    ROOT
    / "plugin"
    / "kaoyan-study-intake"
    / "references"
    / "shared-processing-contract.md"
)
SEALED_SHARED_CONTRACT_SHA256 = (
    "7cbf3b1c10e98d386aa942db4daf986c6bfd0eea0e1e8381a2483aa5ab4ea0c8"
)


class EnglishOutputLimitPromptContractTests(unittest.TestCase):
    def test_shared_three_subject_contract_remains_byte_identical(self) -> None:
        self.assertEqual(
            hashlib.sha256(SHARED_CONTRACT.read_bytes()).hexdigest(),
            SEALED_SHARED_CONTRACT_SHA256,
        )

    def test_both_english_stages_have_the_same_narrow_recovery_rule(self) -> None:
        required = (
            "controlling subject-specific interpretation",
            "remains unrecovered at stage end",
            "strictly smaller page_size",
            "every non-size query argument unchanged",
            "entire cursor chain reaches complete=true and next_cursor=null",
            "cites only successful current-stage calls",
            "must never be cited",
            "does not authorize a Host retry or another model submission",
        )
        for prompt in (
            ENGLISH_CANDIDATE_PROMPT_TEMPLATE,
            ENGLISH_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        ):
            for clause in required:
                self.assertIn(clause, prompt)

    def test_unrecovered_cases_remain_fail_closed(self) -> None:
        for prompt in (
            ENGLISH_CANDIDATE_PROMPT_TEMPLATE,
            ENGLISH_CRITICAL_REVIEW_PROMPT_TEMPLATE,
        ):
            for clause in (
                "identical-argument retry",
                "page_size=1 failure",
                "changed non-size query argument",
                "unresolved cursor chain",
                "grounding in any failed call",
            ):
                self.assertIn(clause, prompt)


if __name__ == "__main__":
    unittest.main()
