from __future__ import annotations

import hashlib
import json
import stat
import tomllib
from pathlib import Path
from typing import Any

from . import __version__


class ReleaseBindingError(RuntimeError):
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


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _source_files(root: Path) -> dict[str, str]:
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
            raise ReleaseBindingError("mcp_release_source_missing") from exc
        if path.is_symlink() or not stat.S_ISREG(node.st_mode):
            raise ReleaseBindingError("mcp_release_source_unsafe")
        result[path.relative_to(root).as_posix()] = _sha256_bytes(
            path.read_bytes()
        )
    return dict(sorted(result.items()))


def _source_revision(files: dict[str, str]) -> str:
    return _sha256_bytes(_canonical_bytes(dict(sorted(files.items()))))


def _validate_version_contract(root: Path) -> None:
    try:
        project = tomllib.loads(
            (root / "pyproject.toml").read_text(encoding="utf-8")
        )
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
        project_version = project["project"]["version"]
        policy_version = policy["server_release"]
    except (
        OSError,
        UnicodeError,
        tomllib.TOMLDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        raise ReleaseBindingError(
            "mcp_release_version_contract_invalid"
        ) from exc
    launcher_contract = policy.get("sealed_launcher_contract")
    launcher_profiles = policy.get("production_launcher_profiles")
    server_template = snippet.get("mcp_servers", {}).get("kaoyan_read", {})
    template_values = {
        str(server_template.get("command", "")),
        *[str(value) for value in server_template.get("args", [])],
        str(server_template.get("cwd", "")),
    }
    if (
        project_version != __version__
        or policy_version != __version__
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
        raise ReleaseBindingError("mcp_release_version_contract_invalid")


def _manifest_binding(root: Path, files: dict[str, str]) -> tuple[str, str] | None:
    manifest_path = root / "release.json"
    try:
        node = manifest_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ReleaseBindingError("mcp_release_manifest_invalid") from exc
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseBindingError("mcp_release_manifest_invalid") from exc
    if manifest_path.is_symlink() or not stat.S_ISREG(node.st_mode):
        raise ReleaseBindingError("mcp_release_manifest_invalid")
    revision = _source_revision(files)
    server_release = f"{__version__}+sha256.{revision}"
    if (
        manifest.get("schema_version") != "study-read-mcp-release.v1"
        or manifest.get("release_id") != revision
        or manifest.get("server_release") != server_release
        or manifest.get("source_files") != files
        or manifest.get("formal_write_count") != 0
        or root.name != revision
    ):
        raise ReleaseBindingError("mcp_release_manifest_invalid")
    return revision, server_release


def _release_binding() -> tuple[str, str]:
    root = Path(__file__).resolve().parents[2]
    _validate_version_contract(root)
    files = _source_files(root)
    manifest = _manifest_binding(root, files)
    if manifest is not None:
        return manifest
    revision = _source_revision(files)
    return revision, f"{__version__}+sha256.{revision}"


CODE_REVISION, SERVER_RELEASE = _release_binding()
