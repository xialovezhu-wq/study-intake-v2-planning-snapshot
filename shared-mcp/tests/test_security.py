from __future__ import annotations

import ast
import json
import re
import os
import tempfile
import unittest
from pathlib import Path

from study_read_mcp.errors import StudyReadError
from study_read_mcp.preprocessor import PreprocessorAuthority
from study_read_mcp.safeio import SafeReader

from .helpers import make_fixture, write_text


class SecurityTests(unittest.TestCase):
    def test_dependency_lock_matches_install_report(self) -> None:
        project = Path(__file__).parents[1]
        report = json.loads((project / "work/install-report.json").read_text(encoding="utf-8"))
        expected = {
            item["metadata"]["name"].lower().replace("_", "-"): (
                item["metadata"]["version"], item["download_info"]["archive_info"]["hashes"]["sha256"]
            ) for item in report["install"]
        }
        actual = {}
        pattern = re.compile(r"([^=]+)==([^ ]+) --hash=sha256:([0-9a-f]{64})")
        for line in (project / "requirements.lock").read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#"):
                continue
            match = pattern.fullmatch(line)
            self.assertIsNotNone(match, line)
            actual[match.group(1).lower().replace("_", "-")] = (match.group(2), match.group(3))
        self.assertEqual(actual, expected)

    def test_stable_ids_reject_paths_nul_and_traversal(self) -> None:
        for value in ("../secret", "/absolute", "a/b", "a\\b", "a\x00b", ".."):
            with self.assertRaises(StudyReadError):
                SafeReader.validate_stable_id(value)
        self.assertEqual(SafeReader.validate_stable_id("DS01-01-测试"), "DS01-01-测试")

    def test_ordinary_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "root"
            outside = Path(temp) / "outside.txt"
            root.mkdir()
            write_text(outside, "secret")
            os.symlink(outside, root / "link")
            reader = SafeReader({"r": root})
            with self.assertRaises(StudyReadError) as context:
                reader.exact("r", "link")
            self.assertEqual(context.exception.code, "INVALID_ARGUMENT")

    def test_source_change_during_read_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "data.json"
            write_text(path, "{\"value\":1}")
            reader = SafeReader({"r": root})
            reader.after_read_hook = lambda _: write_text(path, "{\"value\":2,\"changed\":true}")
            with self.assertRaises(StudyReadError) as context:
                reader.json(reader.exact("r", "data.json"))
            self.assertEqual(context.exception.code, "SOURCE_CHANGED_DURING_READ")

    def test_preprocessor_current_must_stay_inside_releases(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            config = make_fixture(base)
            authority = PreprocessorAuthority(config.preprocessor_root)
            self.assertRegex(authority.bind_current().release_id, r"^[0-9a-f]{64}$")
            (config.preprocessor_root / "current").unlink()
            outside = base / ("a" * 64)
            write_text(outside / "release.json", "{\"component_inventory\":{}}")
            os.symlink(outside, config.preprocessor_root / "current")
            with self.assertRaises(StudyReadError) as context:
                authority.bind_current()
            self.assertEqual(context.exception.code, "AUTHORITY_DRIFT")

    def test_server_source_has_no_forbidden_capability_imports(self) -> None:
        source_root = Path(__file__).parents[1] / "src/study_read_mcp"
        forbidden_modules = {"subprocess", "socket", "requests", "urllib", "httpx"}
        forbidden_names = {"openai", "anthropic", "writer", "luna"}
        violations = []
        for path in source_root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.split(".")[0] in forbidden_modules:
                            violations.append((path.name, alias.name))
                elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] in forbidden_modules:
                    violations.append((path.name, node.module))
            lowered = path.read_text(encoding="utf-8").casefold()
            if path.name not in {"server.py"}:
                for name in forbidden_names:
                    if f"import {name}" in lowered or f"from {name}" in lowered:
                        violations.append((path.name, name))
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
