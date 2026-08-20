from __future__ import annotations

import hashlib
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import release_manager as release  # noqa: E402


MANAGED_PROFILE = b"""[mcp_servers.kaoyan_read]
command = "/usr/bin/python3"
args = ["-I", "-S", "/sealed/launcher.py"]
cwd = "/sealed/release"
env = { STUDY_INTAKE = "1" }
env_vars = ["STUDY_INTAKE"]
enabled = true
required = false
startup_timeout_sec = 5
tool_timeout_sec = 10
enabled_tools = ["authority_bundle", "math_read_bundle"]
disabled_tools = []
default_tools_approval_mode = "auto"
"""


class Exact20260814ExternalProfileTests(unittest.TestCase):
    def test_unmanaged_config_changes_are_accepted_without_writing_config(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="exact-profile-") as raw_root:
            path = Path(raw_root) / "config.toml"
            current = (
                b'model_reasoning_effort = "ultra"\n'
                b'service_tier = "priority"\n\n'
                b'[projects."/Users/example/new-project"]\n'
                b'trust_level = "trusted"\n\n'
                b'[mcp_servers.playwright]\n'
                b'command = "changed-peer-command"\n\n'
                + MANAGED_PROFILE
            )
            path.write_bytes(current)
            before = path.stat()
            expected_profile_sha256 = hashlib.sha256(MANAGED_PROFILE).hexdigest()

            result = release._exact_20260814_validate_managed_ordinary_profile(
                path,
                expected_mcp_profile_sha256=expected_profile_sha256,
            )

            after = path.stat()
            parsed = tomllib.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(path.read_bytes(), current)
            self.assertEqual(after.st_ino, before.st_ino)
            self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
            self.assertEqual(parsed["model_reasoning_effort"], "ultra")
            self.assertEqual(parsed["service_tier"], "priority")
            self.assertEqual(
                parsed["projects"]["/Users/example/new-project"]["trust_level"],
                "trusted",
            )
            self.assertEqual(
                parsed["mcp_servers"]["playwright"]["command"],
                "changed-peer-command",
            )
            self.assertIsNone(result)
            descriptor = release.EXACT_20260814_6043_FORWARD_RECOVERY
            self.assertEqual(
                descriptor["ordinary_mcp_profile_sha256"],
                "0dab2460f328b63103a29e9e183807c52e903259f699afd3606ebdd6fdb4b5f5",
            )
            self.assertNotIn(
                descriptor["ordinary_config_path"], descriptor["external_sha256s"]
            )
            self.assertEqual(len(descriptor["external_sha256s"]), 1)

    def test_any_managed_profile_field_drift_is_rejected(self) -> None:
        mutations = {
            "command": (b'command = "/usr/bin/python3"', b'command = "/bin/python3"'),
            "args": (b'"-S"', b'"-s"'),
            "cwd": (b'cwd = "/sealed/release"', b'cwd = "/other/release"'),
            "env": (b'STUDY_INTAKE = "1"', b'STUDY_INTAKE = "0"'),
            "env_vars": (
                b'env_vars = ["STUDY_INTAKE"]',
                b'env_vars = ["STUDY_INTAKE", "OTHER"]',
            ),
            "enabled": (b"enabled = true", b"enabled = false"),
            "required": (b"required = false", b"required = true"),
            "startup_timeout_sec": (
                b"startup_timeout_sec = 5",
                b"startup_timeout_sec = 6",
            ),
            "tool_timeout_sec": (
                b"tool_timeout_sec = 10",
                b"tool_timeout_sec = 11",
            ),
            "enabled_tools": (
                b'enabled_tools = ["authority_bundle", "math_read_bundle"]',
                b'enabled_tools = ["authority_bundle"]',
            ),
            "disabled_tools": (b"disabled_tools = []", b'disabled_tools = ["x"]'),
            "approval_mode": (
                b'default_tools_approval_mode = "auto"',
                b'default_tools_approval_mode = "manual"',
            ),
        }
        expected_profile_sha256 = hashlib.sha256(MANAGED_PROFILE).hexdigest()
        with tempfile.TemporaryDirectory(prefix="exact-profile-drift-") as raw_root:
            path = Path(raw_root) / "config.toml"
            for field, (old, new) in mutations.items():
                with self.subTest(field=field):
                    drifted = MANAGED_PROFILE.replace(old, new, 1)
                    self.assertNotEqual(drifted, MANAGED_PROFILE)
                    current = (
                        b'model_reasoning_effort = "ultra"\n\n'
                        b'[projects."/Users/example/new-project"]\n'
                        b'trust_level = "trusted"\n\n'
                        + drifted
                    )
                    path.write_bytes(current)
                    with self.assertRaisesRegex(
                        release.ReleaseError,
                        "exact_20260814_external_profile_drift",
                    ):
                        release._exact_20260814_validate_managed_ordinary_profile(
                            path,
                            expected_mcp_profile_sha256=expected_profile_sha256,
                        )
                    self.assertEqual(path.read_bytes(), current)

            invalid_profiles = {
                "managed_byte_formatting_drift": MANAGED_PROFILE.replace(
                    b"enabled = true", b"enabled  = true", 1
                ),
                "missing_table": b'model_reasoning_effort = "ultra"\n',
                "duplicate_table": MANAGED_PROFILE + MANAGED_PROFILE,
                "nested_child_table_drift": (
                    MANAGED_PROFILE
                    + b"[mcp_servers.kaoyan_read.extra]\nvalue = true\n"
                ),
            }
            for case, current in invalid_profiles.items():
                with self.subTest(case=case):
                    path.write_bytes(current)
                    with self.assertRaisesRegex(
                        release.ReleaseError,
                        "exact_20260814_external_profile_drift",
                    ):
                        release._exact_20260814_validate_managed_ordinary_profile(
                            path,
                            expected_mcp_profile_sha256=expected_profile_sha256,
                        )
                    self.assertEqual(path.read_bytes(), current)

            regular = Path(raw_root) / "regular-config.toml"
            regular.write_bytes(MANAGED_PROFILE)
            symlink = Path(raw_root) / "symlink-config.toml"
            symlink.symlink_to(regular)
            directory = Path(raw_root) / "directory-config.toml"
            directory.mkdir()
            for case, invalid_path in (
                ("symlink", symlink),
                ("non_regular", directory),
            ):
                with self.subTest(case=case), self.assertRaisesRegex(
                    release.ReleaseError,
                    "exact_20260814_external_profile_drift",
                ):
                    release._exact_20260814_validate_managed_ordinary_profile(
                        invalid_path,
                        expected_mcp_profile_sha256=expected_profile_sha256,
                    )


if __name__ == "__main__":
    unittest.main()
