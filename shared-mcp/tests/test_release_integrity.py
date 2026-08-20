from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _build_release_module():
    path = ROOT / "scripts/build_release.py"
    spec = importlib.util.spec_from_file_location("build_release", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_writable(path: Path) -> None:
    for directory, directories, files in os.walk(path, topdown=False):
        for name in files:
            (Path(directory) / name).chmod(0o600)
        for name in directories:
            (Path(directory) / name).chmod(0o700)
    path.chmod(0o700)


class ReleaseIntegrityTests(unittest.TestCase):
    def test_version_contract_and_development_revision_match_builder(self) -> None:
        build_release = _build_release_module()
        from study_read_mcp import __version__
        from study_read_mcp.release import CODE_REVISION, SERVER_RELEASE

        files = build_release.source_files()
        self.assertEqual(build_release.contract_version(ROOT), "0.4.1")
        self.assertEqual(__version__, "0.4.1")
        self.assertEqual(CODE_REVISION, build_release.package_revision(files))
        self.assertEqual(
            SERVER_RELEASE,
            f"{__version__}+sha256.{CODE_REVISION}",
        )

    def test_every_declared_source_hash_contributes_to_release_id(self) -> None:
        build_release = _build_release_module()
        files = build_release.source_files()
        baseline = build_release.package_revision(files)

        for relative in files:
            with self.subTest(relative=relative):
                changed = dict(files)
                changed[relative] = (
                    "0" * 64 if files[relative] != "0" * 64 else "1" * 64
                )
                self.assertNotEqual(
                    build_release.package_revision(changed),
                    baseline,
                )

    def test_english_projection_validator_is_immutable_release_material(self) -> None:
        build_release = _build_release_module()
        relative = "src/study_read_mcp/adapters/english.py"
        files = build_release.source_files()
        self.assertIn(relative, files)
        source = (ROOT / relative).read_text(encoding="utf-8")
        self.assertIn("_verify_quick_capture_projections", source)
        self.assertNotIn(
            '"intake" / "views" / "projection-manifest.json"',
            source,
        )
        changed = dict(files)
        changed[relative] = "0" * 64
        self.assertNotEqual(
            build_release.package_revision(changed),
            build_release.package_revision(files),
        )

    def test_version_contract_rejects_policy_drift(self) -> None:
        build_release = _build_release_module()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "config").mkdir()
            (root / "src/study_read_mcp").mkdir(parents=True)
            (root / "pyproject.toml").write_text(
                (ROOT / "pyproject.toml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (root / "src/study_read_mcp/__init__.py").write_text(
                (ROOT / "src/study_read_mcp/__init__.py").read_text(
                    encoding="utf-8"
                ),
                encoding="utf-8",
            )
            policy = json.loads(
                (ROOT / "config/skill-tool-policy.json").read_text(
                    encoding="utf-8"
                )
            )
            policy["server_release"] = "9.9.9"
            (root / "config/skill-tool-policy.json").write_text(
                json.dumps(policy),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                build_release.ReleaseError,
                "mcp_release_version_contract_invalid",
            ):
                build_release.contract_version(root)

    def test_built_runtime_exports_manifest_server_release(self) -> None:
        build_release = _build_release_module()
        with tempfile.TemporaryDirectory() as temp:
            release_base = Path(temp) / "releases"
            result = build_release.build(release_base)
            release_dir = Path(result["release_dir"])
            self.assertEqual(
                result["release_manifest_sha256"],
                __import__("hashlib").sha256(
                    (release_dir / "release.json").read_bytes()
                ).hexdigest(),
            )
            self.assertEqual(
                result["sealed_launcher_path"],
                str(release_dir / "scripts/sealed_launcher.py"),
            )
            self.assertEqual(
                result["sealed_launcher_sha256"],
                build_release.sha256_bytes(
                    (release_dir / "scripts/sealed_launcher.py").read_bytes()
                ),
            )
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "from study_read_mcp.release import SERVER_RELEASE; print(SERVER_RELEASE)",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    cwd=release_dir,
                    env={
                        **os.environ,
                        "PYTHONPATH": str(release_dir / "src"),
                    },
                )
                self.assertEqual(
                    completed.stdout.strip(),
                    result["server_release"],
                )
            finally:
                _make_writable(release_dir)

    def test_version_contract_rejects_unsealed_codex_template(self) -> None:
        build_release = _build_release_module()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            shutil.copytree(ROOT, root, ignore=shutil.ignore_patterns(".git", ".venv"))
            snippet = root / "config/codex-mcp-snippet.toml"
            snippet.write_text(
                snippet.read_text(encoding="utf-8").replace(
                    '  "-S",\n', "", 1
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                build_release.ReleaseError,
                "mcp_release_version_contract_invalid",
            ):
                build_release.contract_version(root)

    def test_rebound_or_forged_manifest_fails_verification(self) -> None:
        build_release = _build_release_module()
        with tempfile.TemporaryDirectory() as temp:
            release_base = Path(temp) / "releases"
            result = build_release.build(release_base)
            release_dir = Path(result["release_dir"])
            manifest_path = release_dir / "release.json"
            try:
                release_dir.chmod(0o700)
                manifest_path.chmod(0o600)
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["source_files"][
                    "config/skill-tool-policy.json"
                ] = "0" * 64
                manifest_path.write_bytes(
                    build_release.canonical_bytes(manifest)
                )
                manifest_path.chmod(0o444)
                release_dir.chmod(0o555)
                with self.assertRaisesRegex(
                    build_release.ReleaseError,
                    "mcp_release_manifest_invalid",
                ):
                    build_release.verify_release(release_dir)
            finally:
                _make_writable(release_dir)


if __name__ == "__main__":
    unittest.main()
