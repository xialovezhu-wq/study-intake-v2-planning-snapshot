from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "formal_surface_manifest", ROOT / "bin/formal_surface_manifest.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FormalSurfaceManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repos = {
            subject: self.root / subject for subject in ("math", "cs408", "english")
        }
        (self.repos["math"] / "错题知识网络/错题卡").mkdir(parents=True)
        (self.repos["math"] / "错题知识网络/错题卡/M-1.md").write_text(
            "math", encoding="utf-8"
        )
        (self.repos["cs408"] / "节点库").mkdir(parents=True)
        (self.repos["cs408"] / "节点总表.md").write_text("table", encoding="utf-8")
        (self.repos["cs408"] / "节点库/N-1.md").write_text("node", encoding="utf-8")
        (self.repos["english"] / "bank").mkdir(parents=True)
        (self.repos["english"] / "bank/master_bank.csv").write_text(
            "word\n", encoding="utf-8"
        )
        self.config = {
            "adapters": {
                subject: {"repo_root": str(path)}
                for subject, path in self.repos.items()
            },
            "cs408_knowledge_snapshot": {
                "sources": {"nodes": "节点总表.md"}
            },
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_capture_is_stable_and_ignores_control_plane_files(self) -> None:
        first = MODULE.capture(self.config)
        (self.repos["cs408"] / "scripts").mkdir()
        (self.repos["cs408"] / "scripts/tool.py").write_text("pass", encoding="utf-8")
        second = MODULE.capture(self.config)
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual(MODULE.verify(first, second)["status"], "unchanged")

    def test_formal_change_is_reported(self) -> None:
        first = MODULE.capture(self.config)
        target = self.repos["english"] / "bank/master_bank.csv"
        target.write_text("word\nchanged\n", encoding="utf-8")
        result = MODULE.verify(first, MODULE.capture(self.config))
        self.assertEqual(result["status"], "changed")
        self.assertEqual(
            result["differences"]["english"]["changed"],
            ["bank/master_bank.csv"],
        )

    def test_subject_scope_never_reads_another_subject(self) -> None:
        math = MODULE.capture(self.config, subjects=("math",))
        self.assertEqual(math["subject_scope"], ["math"])
        self.assertEqual(set(math["subjects"]), {"math"})
        (self.repos["english"] / "bank/master_bank.csv").write_text(
            "word\nchanged\n", encoding="utf-8"
        )
        self.assertEqual(
            MODULE.verify(
                math, MODULE.capture(self.config, subjects=("math",))
            )["status"],
            "unchanged",
        )

    def test_subject_scope_mismatch_fails_closed(self) -> None:
        math = MODULE.capture(self.config, subjects=("math",))
        english = MODULE.capture(self.config, subjects=("english",))
        with self.assertRaisesRegex(
            MODULE.ManifestError, "formal_surface_subject_scope_mismatch"
        ):
            MODULE.verify(math, english)


if __name__ == "__main__":
    unittest.main()
