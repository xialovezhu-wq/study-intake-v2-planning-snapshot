#!/usr/bin/env python3
"""Build and verify an immutable content-addressed MCP server release."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import stat
import tempfile
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_BASE = Path(
    "/Users/xiazhibin/.codex/local-study-read-mcp/releases"
)


class ReleaseError(RuntimeError):
    pass


SEALED_COMPONENT_FIELDS = {
    "python_executable",
    "release_root",
    "release_id",
    "release_manifest_sha256",
    "sealed_launcher_path",
    "sealed_launcher_sha256",
}
SEALED_TEMPLATE_TOKENS = {
    "__MCP_PYTHON_EXECUTABLE__",
    "__MCP_SEALED_LAUNCHER_PATH__",
    "__MCP_RELEASE_ROOT__",
    "__MCP_RELEASE_ID__",
    "__MCP_RELEASE_MANIFEST_SHA256__",
}


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def source_files_at(root: Path) -> dict[str, str]:
    paths = [
        root / "pyproject.toml",
        root / "requirements.lock",
        root / "config/codex-mcp-snippet.toml",
        root / "config/skill-tool-policy.json",
        root / "scripts/sealed_launcher.py",
        *sorted((root / "src/study_read_mcp").rglob("*.py")),
    ]
    result: dict[str, str] = {}
    for path in paths:
        try:
            node = path.lstat()
        except OSError as exc:
            raise ReleaseError("mcp_release_source_missing") from exc
        if path.is_symlink() or not stat.S_ISREG(node.st_mode):
            raise ReleaseError("mcp_release_source_unsafe")
        relative = path.relative_to(root).as_posix()
        result[relative] = sha256_bytes(path.read_bytes())
    return dict(sorted(result.items()))


def source_files() -> dict[str, str]:
    return source_files_at(ROOT)


def package_revision(files: dict[str, str]) -> str:
    return sha256_bytes(canonical_bytes(dict(sorted(files.items()))))


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _package_version(root: Path) -> str:
    init_path = root / "src/study_read_mcp/__init__.py"
    try:
        tree = ast.parse(init_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise ReleaseError("mcp_release_package_version_invalid") from exc
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in node.targets
            )
        ):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError) as exc:
                raise ReleaseError(
                    "mcp_release_package_version_invalid"
                ) from exc
            if isinstance(value, str) and value:
                return value
    raise ReleaseError("mcp_release_package_version_invalid")


def contract_version(root: Path) -> str:
    try:
        project = tomllib.loads(
            (root / "pyproject.toml").read_text(encoding="utf-8")
        )
        project_version = project["project"]["version"]
        policy = json.loads(
            (root / "config/skill-tool-policy.json").read_text(
                encoding="utf-8"
            )
        )
        snippet = tomllib.loads(
            (root / "config/codex-mcp-snippet.toml").read_text(
                encoding="utf-8"
            )
        )
        policy_version = policy["server_release"]
    except (
        OSError,
        UnicodeError,
        tomllib.TOMLDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        raise ReleaseError("mcp_release_version_contract_invalid") from exc
    package_version = _package_version(root)
    launcher_contract = policy.get("sealed_launcher_contract")
    launcher_profiles = policy.get("production_launcher_profiles")
    server_template = snippet.get("mcp_servers", {}).get("kaoyan_read", {})
    template_values = {
        str(server_template.get("command", "")),
        *[str(value) for value in server_template.get("args", [])],
        str(server_template.get("cwd", "")),
    }
    if (
        not isinstance(project_version, str)
        or not isinstance(policy_version, str)
        or not project_version
        or project_version != package_version
        or policy_version != project_version
        or policy.get("schema_version") != "study-read-mcp-policy.v4"
        or not isinstance(launcher_contract, dict)
        or launcher_contract.get("schema_version")
        != "study-read-mcp-sealed-launcher.v1"
        or set(launcher_contract.get("required_component_fields", []))
        != SEALED_COMPONENT_FIELDS
        or launcher_contract.get("python_flags") != ["-I", "-S"]
        or set(launcher_contract.get("modes", []))
        != {
            "server",
            "client",
            "subject-server",
            "preflight-server",
            "snapshot",
        }
        or not isinstance(launcher_profiles, dict)
        or set(launcher_profiles)
        != {
            "ordinary",
            "background",
            "morning_preparation",
            "analysis",
            "critical_review",
            "infrastructure_preflight",
            "authority_snapshot",
        }
        or launcher_profiles.get("ordinary", {}).get("mode") != "server"
        or launcher_profiles.get("background", {}).get("mode") != "client"
        or launcher_profiles.get("morning_preparation", {}).get("mode") != "client"
        or launcher_profiles.get("analysis", {}).get("mode") != "subject-server"
        or launcher_profiles.get("critical_review", {}).get("mode")
        != "subject-server"
        or launcher_profiles.get("infrastructure_preflight", {}).get("mode")
        != "preflight-server"
        or policy.get("subject_preflight_tools")
        != [
            "list_records",
            "get_records",
            "search_records",
            "query_relations",
        ]
        or launcher_profiles.get("authority_snapshot", {}).get("mode") != "snapshot"
        or not SEALED_TEMPLATE_TOKENS.issubset(template_values)
        or server_template.get("args", [])[:3]
        != ["-I", "-S", "__MCP_SEALED_LAUNCHER_PATH__"]
    ):
        raise ReleaseError("mcp_release_version_contract_invalid")
    return project_version


def verify_release(target: Path) -> dict[str, Any]:
    try:
        target_node = target.lstat()
    except OSError as exc:
        raise ReleaseError("mcp_release_manifest_invalid") from exc
    if target.is_symlink() or not stat.S_ISDIR(target_node.st_mode):
        raise ReleaseError("mcp_release_manifest_invalid")
    manifest_path = target / "release.json"
    try:
        manifest_node = manifest_path.lstat()
    except OSError as exc:
        raise ReleaseError("mcp_release_manifest_invalid") from exc
    if manifest_path.is_symlink() or not stat.S_ISREG(manifest_node.st_mode):
        raise ReleaseError("mcp_release_manifest_invalid")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseError("mcp_release_manifest_invalid") from exc
    files = manifest.get("source_files")
    revision = manifest.get("release_id")
    version = contract_version(target)
    if (
        manifest.get("schema_version") != "study-read-mcp-release.v1"
        or not isinstance(files, dict)
        or not all(
            isinstance(relative, str) and _is_sha256(expected)
            for relative, expected in files.items()
        )
        or not isinstance(revision, str)
        or not _is_sha256(revision)
        or package_revision(files) != revision
        or manifest.get("server_release") != f"{version}+sha256.{revision}"
        or manifest.get("formal_write_count") != 0
        or target.name != revision
    ):
        raise ReleaseError("mcp_release_manifest_invalid")
    actual_files = source_files_at(target)
    if files != actual_files:
        raise ReleaseError("mcp_release_source_mismatch")
    for relative in actual_files:
        path = target / relative
        if (
            not path.is_file()
            or path.is_symlink()
            or stat.S_IMODE(path.stat().st_mode) != 0o444
        ):
            raise ReleaseError("mcp_release_source_mismatch")
    if stat.S_IMODE(manifest_path.stat().st_mode) != 0o444:
        raise ReleaseError("mcp_release_manifest_mode_invalid")
    return {
        "status": "verified",
        "release_id": revision,
        "release_manifest_sha256": sha256_bytes(manifest_path.read_bytes()),
        "server_release": manifest["server_release"],
        "release_dir": str(target),
        "sealed_launcher_path": str(target / "scripts/sealed_launcher.py"),
        "sealed_launcher_sha256": files["scripts/sealed_launcher.py"],
        "formal_write_count": 0,
    }


def build(release_base: Path) -> dict[str, Any]:
    files = source_files()
    version = contract_version(ROOT)
    revision = package_revision(files)
    target = release_base / revision
    if target.exists():
        return {**verify_release(target), "status": "already_built"}
    release_base.mkdir(parents=True, exist_ok=True, mode=0o700)
    raw_stage = tempfile.mkdtemp(prefix=f".{revision[:12]}-", dir=release_base)
    stage = Path(raw_stage)
    try:
        for relative in files:
            source = ROOT / relative
            destination = stage / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            destination.write_bytes(source.read_bytes())
            destination.chmod(0o444)
        manifest = {
            "schema_version": "study-read-mcp-release.v1",
            "release_id": revision,
            "server_release": f"{version}+sha256.{revision}",
            "source_files": files,
            "formal_write_count": 0,
        }
        manifest_path = stage / "release.json"
        manifest_path.write_bytes(canonical_bytes(manifest))
        manifest_path.chmod(0o444)
        for directory, directories, _files in os.walk(stage, topdown=False):
            for name in directories:
                (Path(directory) / name).chmod(0o555)
        stage.chmod(0o555)
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_release(target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-base",
        type=Path,
        default=DEFAULT_RELEASE_BASE,
    )
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    result = verify_release(args.verify) if args.verify else build(args.release_base)
    print(canonical_bytes(result).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
