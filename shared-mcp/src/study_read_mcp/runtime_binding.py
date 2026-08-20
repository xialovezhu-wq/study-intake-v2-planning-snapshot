from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

from .release import CODE_REVISION, SERVER_RELEASE


EXPECTED_RELEASE_ENV = "STUDY_READ_MCP_EXPECTED_RELEASE_ID"
EXPECTED_ROOT_ENV = "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT"
EXPECTED_MANIFEST_SHA_ENV = "STUDY_READ_MCP_EXPECTED_RELEASE_MANIFEST_SHA256"


class RuntimeBindingError(RuntimeError):
    pass


def package_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _manifest(root: Path) -> tuple[dict[str, Any], str]:
    path = root / "release.json"
    try:
        node = path.lstat()
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeBindingError("mcp_runtime_release_manifest_invalid") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(node.st_mode)
        or not isinstance(value, dict)
    ):
        raise RuntimeBindingError("mcp_runtime_release_manifest_invalid")
    return value, _sha256_bytes(raw)


def release_manifest_sha256(project_root: Path) -> str:
    root = project_root.expanduser().resolve(strict=True)
    _value, digest = _manifest(root)
    return digest


def verify_runtime_binding(
    *,
    expected_root: Path | None = None,
    expected_release_id: str | None = None,
    expected_manifest_sha256: str | None = None,
    require_expected_environment: bool = False,
) -> tuple[Path, str, str]:
    actual_root = package_project_root()
    environment_root = os.environ.get(EXPECTED_ROOT_ENV)
    environment_release = os.environ.get(EXPECTED_RELEASE_ENV)
    environment_manifest_sha = os.environ.get(EXPECTED_MANIFEST_SHA_ENV)
    immutable_runtime = (actual_root / "release.json").exists()
    if (require_expected_environment or immutable_runtime) and (
        not environment_root or not environment_release or not environment_manifest_sha
    ):
        raise RuntimeBindingError("mcp_runtime_expected_binding_missing")
    selected_root = (
        expected_root.expanduser().resolve()
        if expected_root is not None
        else Path(environment_root).expanduser().resolve()
        if environment_root
        else actual_root
    )
    selected_release = (
        expected_release_id
        if expected_release_id is not None
        else environment_release
        if environment_release
        else CODE_REVISION
    )
    selected_manifest_sha = (
        expected_manifest_sha256
        if expected_manifest_sha256 is not None
        else environment_manifest_sha
    )
    if (
        not (selected_root / "release.json").exists()
        and expected_root is None
        and expected_release_id is None
        and not environment_root
        and not environment_release
        and not require_expected_environment
    ):
        return actual_root, CODE_REVISION, "development-tree"
    manifest, manifest_sha = _manifest(selected_root)
    if (
        selected_root != actual_root
        or selected_root.name != selected_release
        or selected_release != CODE_REVISION
        or (
            selected_manifest_sha is not None
            and selected_manifest_sha != manifest_sha
        )
        or manifest.get("schema_version") != "study-read-mcp-release.v1"
        or manifest.get("release_id") != selected_release
        or manifest.get("server_release") != SERVER_RELEASE
        or manifest.get("formal_write_count") != 0
    ):
        raise RuntimeBindingError("mcp_runtime_release_binding_mismatch")
    try:
        runtime_path = Path(__file__).resolve()
        runtime_path.relative_to(selected_root / "src")
        package_module = sys.modules.get("study_read_mcp")
        package_file = getattr(package_module, "__file__", None)
        if not isinstance(package_file, str):
            raise ValueError("package origin unavailable")
        Path(package_file).resolve().relative_to(selected_root / "src")
    except ValueError as exc:
        raise RuntimeBindingError("mcp_runtime_module_origin_mismatch") from exc
    return selected_root, selected_release, manifest_sha


def sealed_python_environment(project_root: Path) -> dict[str, str]:
    root, release_id, manifest_sha = verify_runtime_binding(
        expected_root=project_root
    )
    if manifest_sha == "development-tree":
        raise RuntimeBindingError("mcp_runtime_immutable_release_required")
    return {
        "PATH": "/usr/bin:/bin",
        "PYTHONUTF8": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONSAFEPATH": "1",
        "PYTHONPATH": str(root / "src"),
        EXPECTED_ROOT_ENV: str(root),
        EXPECTED_RELEASE_ENV: release_id,
        EXPECTED_MANIFEST_SHA_ENV: manifest_sha,
    }


def sealed_launcher_path(project_root: Path) -> Path:
    root, _release_id, manifest_sha = verify_runtime_binding(
        expected_root=project_root
    )
    if manifest_sha == "development-tree":
        raise RuntimeBindingError("mcp_runtime_immutable_release_required")
    launcher = root / "scripts" / "sealed_launcher.py"
    try:
        launcher.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise RuntimeBindingError("mcp_runtime_sealed_launcher_invalid") from exc
    if launcher.is_symlink() or not launcher.is_file():
        raise RuntimeBindingError("mcp_runtime_sealed_launcher_invalid")
    return launcher


def sealed_launcher_binding_arguments(project_root: Path) -> list[str]:
    root, release_id, manifest_sha = verify_runtime_binding(
        expected_root=project_root,
        require_expected_environment=True,
    )
    return [
        "--release-root",
        str(root),
        "--expected-release-id",
        release_id,
        "--expected-release-manifest-sha256",
        manifest_sha,
    ]
