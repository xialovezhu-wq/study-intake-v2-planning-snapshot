from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import MathAdapter, PreprocessorError  # noqa: E402


class MathExactSmokeSentinelTests(unittest.TestCase):
    def _adapter(self, repo: Path) -> MathAdapter:
        return MathAdapter(
            {
                "adapter_version": "test-math-exact-smoke-sentinels-v1",
                "enabled": False,
                "python_path": sys.executable,
                "repo_root": str(repo),
                "status_script": str(repo / "unused-status.py"),
            },
            {},
        )

    @staticmethod
    def _formal_card(*lecture_refs: str) -> dict[str, object]:
        return {
            "content": "",
            "frontmatter": {
                "lecture_refs": list(lecture_refs),
                "quality_gate": None,
                "status": "待复做",
            },
            "relative_path": "错题知识网络/错题卡/GS-269.md",
        }

    def test_only_exact_legacy_metadata_sentinels_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw))
            images, texts, has_question = adapter._formal_support(
                self._formal_card("待确认", "待补充")
            )

        self.assertEqual(images, [])
        self.assertEqual(texts, [])
        self.assertFalse(has_question)

    def test_real_or_sentinel_like_missing_reference_still_fails_closed(self) -> None:
        for reference in (
            "待补充/题图.png",
            "错题知识网络/assets/visual_wrong_questions/GS-269/question_01.png",
        ):
            with self.subTest(reference=reference):
                with tempfile.TemporaryDirectory() as raw:
                    adapter = self._adapter(Path(raw))
                    with self.assertRaisesRegex(
                        PreprocessorError, "source_path_outside_repo"
                    ):
                        adapter._formal_support(self._formal_card(reference))


if __name__ == "__main__":
    unittest.main()
