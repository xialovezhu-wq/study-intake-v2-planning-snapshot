#!/usr/bin/env python3
"""Verify an immutable MCP release before importing any project module."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import runpy
import stat
import sys
from pathlib import Path
from typing import Any


EXPECTED_RELEASE_ENV = "STUDY_READ_MCP_EXPECTED_RELEASE_ID"
EXPECTED_ROOT_ENV = "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT"
EXPECTED_MANIFEST_SHA_ENV = "STUDY_READ_MCP_EXPECTED_RELEASE_MANIFEST_SHA256"
SHA256_LENGTH = 64
SUBJECTS = frozenset({"math", "cs408", "english"})
PROFILES = frozenset({"ordinary", "background", "morning_preparation"})


class SealedLauncherError(RuntimeError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == SHA256_LENGTH
        and all(character in "0123456789abcdef" for character in value)
    )


def _absolute_path(value: str, field: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise SealedLauncherError(f"mcp_sealed_{field}_not_absolute")
    return path


def verify_release(
    release_root: Path,
    expected_release_id: str,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    if not _is_sha256(expected_release_id) or not _is_sha256(
        expected_manifest_sha256
    ):
        raise SealedLauncherError("mcp_sealed_expected_binding_invalid")
    supplied_root = release_root.expanduser()
    if not supplied_root.is_absolute():
        raise SealedLauncherError("mcp_sealed_release_root_not_absolute")
    try:
        root_node = supplied_root.lstat()
        root = supplied_root.resolve(strict=True)
    except OSError as exc:
        raise SealedLauncherError("mcp_sealed_release_root_invalid") from exc
    if (
        supplied_root.is_symlink()
        or not stat.S_ISDIR(root_node.st_mode)
        or root.name != expected_release_id
    ):
        raise SealedLauncherError("mcp_sealed_release_root_invalid")
    launcher = Path(__file__).resolve()
    if launcher != root / "scripts" / "sealed_launcher.py":
        raise SealedLauncherError("mcp_sealed_launcher_origin_mismatch")

    manifest_path = root / "release.json"
    try:
        manifest_node = manifest_path.lstat()
        raw_manifest = manifest_path.read_bytes()
        manifest = json.loads(raw_manifest)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SealedLauncherError("mcp_sealed_release_manifest_invalid") from exc
    if (
        manifest_path.is_symlink()
        or not stat.S_ISREG(manifest_node.st_mode)
        or stat.S_IMODE(manifest_node.st_mode) != 0o444
        or _sha256_bytes(raw_manifest) != expected_manifest_sha256
        or not isinstance(manifest, dict)
    ):
        raise SealedLauncherError("mcp_sealed_release_manifest_invalid")
    source_files = manifest.get("source_files")
    if (
        manifest.get("schema_version") != "study-read-mcp-release.v1"
        or manifest.get("release_id") != expected_release_id
        or manifest.get("formal_write_count") != 0
        or not isinstance(manifest.get("server_release"), str)
        or not manifest["server_release"].endswith(
            f"+sha256.{expected_release_id}"
        )
        or not isinstance(source_files, dict)
        or not source_files
        or any(
            not isinstance(relative, str) or not _is_sha256(expected)
            for relative, expected in source_files.items()
        )
        or _sha256_bytes(_canonical_bytes(dict(sorted(source_files.items()))))
        != expected_release_id
    ):
        raise SealedLauncherError("mcp_sealed_release_manifest_invalid")

    for relative, expected in source_files.items():
        relative_path = Path(relative)
        if (
            relative_path.is_absolute()
            or ".." in relative_path.parts
            or not relative_path.parts
        ):
            raise SealedLauncherError("mcp_sealed_release_source_invalid")
        path = root / relative
        try:
            path.relative_to(root)
            node = path.lstat()
            raw = path.read_bytes()
        except (OSError, ValueError) as exc:
            raise SealedLauncherError("mcp_sealed_release_source_invalid") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(node.st_mode)
            or stat.S_IMODE(node.st_mode) != 0o444
            or _sha256_bytes(raw) != expected
        ):
            raise SealedLauncherError("mcp_sealed_release_source_invalid")
    launcher_relative = "scripts/sealed_launcher.py"
    if source_files.get(launcher_relative) != _sha256_bytes(launcher.read_bytes()):
        raise SealedLauncherError("mcp_sealed_launcher_hash_mismatch")
    return {
        "release_root": root,
        "release_id": expected_release_id,
        "release_manifest_sha256": expected_manifest_sha256,
        "server_release": manifest["server_release"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="study-read-mcp-sealed-launcher")
    parser.add_argument("--release-root", required=True)
    parser.add_argument("--expected-release-id", required=True)
    parser.add_argument("--expected-release-manifest-sha256", required=True)
    parser.add_argument(
        "--mode",
        choices=(
            "server",
            "client",
            "subject-server",
            "preflight-server",
            "snapshot",
        ),
        required=True,
    )
    parser.add_argument("--profile", choices=sorted(PROFILES))
    parser.add_argument("--subject", choices=sorted(SUBJECTS))
    parser.add_argument("--subjects")
    parser.add_argument("--subject-root")
    parser.add_argument("--read-session-manifest")
    parser.add_argument("--preflight-session-manifest")
    parser.add_argument("--preprocessor-root")
    parser.add_argument("--tool-policy")
    parser.add_argument("--source-root")
    parser.add_argument("--output-root")
    parser.add_argument("--sealed-stage2", action="store_true", help=argparse.SUPPRESS)
    return parser


def _require_path(args: argparse.Namespace, name: str) -> str:
    value = getattr(args, name)
    if not isinstance(value, str):
        raise SealedLauncherError(f"mcp_sealed_{name}_required")
    return str(_absolute_path(value, name))


def _module_invocation(args: argparse.Namespace, root: Path) -> tuple[str, list[str]]:
    if args.mode in {"server", "client"}:
        if args.profile not in PROFILES:
            raise SealedLauncherError("mcp_sealed_profile_required")
    if args.mode == "server":
        if not isinstance(args.subjects, str):
            raise SealedLauncherError("mcp_sealed_subjects_required")
        subjects = [value.strip() for value in args.subjects.split(",") if value.strip()]
        if not subjects or any(value not in SUBJECTS for value in subjects):
            raise SealedLauncherError("mcp_sealed_subjects_invalid")
        if args.profile in {"background", "morning_preparation"} and len(subjects) != 1:
            raise SealedLauncherError("mcp_sealed_isolated_profile_subject_invalid")
        values = [
            "--stdio",
            "--profile",
            args.profile,
            "--subjects",
            ",".join(subjects),
        ]
        if args.subject_root is not None:
            if len(subjects) != 1:
                raise SealedLauncherError("mcp_sealed_subject_root_scope_invalid")
            values.extend(["--subject-root", _require_path(args, "subject_root")])
        return "study_read_mcp", values
    if args.mode == "client":
        if args.subject not in SUBJECTS:
            raise SealedLauncherError("mcp_sealed_subject_required")
        values = [
            "--profile",
            args.profile,
            "--subject",
            args.subject,
            "--project-root",
            str(root),
        ]
        if args.subject_root is not None:
            values.extend(["--subject-root", _require_path(args, "subject_root")])
        return "study_read_mcp.client", values
    if args.mode == "subject-server":
        if args.subject not in SUBJECTS:
            raise SealedLauncherError("mcp_sealed_subject_required")
        return "study_read_mcp.launchers", [
            "--subject",
            args.subject,
            "--stdio",
            "--read-session-manifest",
            _require_path(args, "read_session_manifest"),
            "--preprocessor-root",
            _require_path(args, "preprocessor_root"),
        ]
    if args.mode == "preflight-server":
        if args.subject not in SUBJECTS:
            raise SealedLauncherError("mcp_sealed_subject_required")
        return "study_read_mcp.launchers", [
            "--subject",
            args.subject,
            "--profile",
            "infrastructure_preflight",
            "--stdio",
            "--preflight-session-manifest",
            _require_path(args, "preflight_session_manifest"),
            "--preprocessor-root",
            _require_path(args, "preprocessor_root"),
            "--tool-policy",
            _require_path(args, "tool_policy"),
        ]
    if args.mode == "snapshot":
        if args.subject not in SUBJECTS:
            raise SealedLauncherError("mcp_sealed_subject_required")
        return "study_read_mcp.snapshot", [
            "--subject",
            args.subject,
            "--source-root",
            _require_path(args, "source_root"),
            "--output-root",
            _require_path(args, "output_root"),
        ]
    raise SealedLauncherError("mcp_sealed_mode_invalid")


def _sealed_environment(binding: dict[str, Any]) -> dict[str, str]:
    root = binding["release_root"]
    return {
        "PATH": "/usr/bin:/bin",
        "PYTHONUTF8": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONSAFEPATH": "1",
        "PYTHONPATH": str(root / "src"),
        EXPECTED_ROOT_ENV: str(root),
        EXPECTED_RELEASE_ENV: binding["release_id"],
        EXPECTED_MANIFEST_SHA_ENV: binding["release_manifest_sha256"],
    }


def _dependency_site_packages() -> Path:
    """Return the venv dependency directory without importing ``site``.

    The launcher is intentionally started with ``-S``.  Calling ``site`` or
    ``site.addsitedir`` here would execute ``.pth`` files before the immutable
    package origin can be enforced, recreating the editable-install bug this
    launcher exists to prevent.
    """

    executable = Path(sys.executable)
    if not executable.is_absolute() or executable.parent.name != "bin":
        raise SealedLauncherError("mcp_sealed_python_environment_invalid")
    venv_root = executable.parent.parent
    config_path = venv_root / "pyvenv.cfg"
    try:
        config_node = config_path.lstat()
        config_text = config_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise SealedLauncherError("mcp_sealed_python_environment_invalid") from exc
    if (
        config_path.is_symlink()
        or not stat.S_ISREG(config_node.st_mode)
        or "include-system-site-packages = false" not in config_text.splitlines()
    ):
        raise SealedLauncherError("mcp_sealed_python_environment_invalid")
    dependency_root = (
        venv_root
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    try:
        dependency_node = dependency_root.lstat()
    except OSError as exc:
        raise SealedLauncherError("mcp_sealed_python_environment_invalid") from exc
    if dependency_root.is_symlink() or not stat.S_ISDIR(dependency_node.st_mode):
        raise SealedLauncherError("mcp_sealed_python_environment_invalid")
    return dependency_root


def _run_stage2(args: argparse.Namespace, binding: dict[str, Any]) -> None:
    if not sys.flags.isolated or not sys.flags.no_site:
        raise SealedLauncherError("mcp_sealed_isolated_python_required")
    expected_environment = _sealed_environment(binding)
    for key, expected in expected_environment.items():
        if os.environ.get(key) != expected:
            raise SealedLauncherError("mcp_sealed_environment_mismatch")
    root = binding["release_root"]
    source_root = root / "src"
    dependency_root = _dependency_site_packages()
    # Direct insertion is deliberate: unlike site.addsitedir, this does not
    # evaluate any .pth file in the dependency directory.
    sys.path.append(str(dependency_root))
    sys.path.insert(0, str(source_root))
    spec = importlib.util.find_spec("study_read_mcp")
    origin = getattr(spec, "origin", None)
    if not isinstance(origin, str):
        raise SealedLauncherError("mcp_sealed_module_origin_mismatch")
    try:
        Path(origin).resolve().relative_to(source_root)
    except ValueError as exc:
        raise SealedLauncherError("mcp_sealed_module_origin_mismatch") from exc
    module, module_args = _module_invocation(args, root)
    sys.argv = [module, *module_args]
    runpy.run_module(module, run_name="__main__", alter_sys=True)


def main() -> int:
    if not sys.flags.isolated or not sys.flags.no_site:
        raise SealedLauncherError("mcp_sealed_isolated_python_required")
    args = _parser().parse_args()
    root = _absolute_path(args.release_root, "release_root")
    binding = verify_release(
        root,
        args.expected_release_id,
        args.expected_release_manifest_sha256,
    )
    if args.sealed_stage2:
        _run_stage2(args, binding)
        return 0
    environment = _sealed_environment(binding)
    argv = [
        sys.executable,
        "-I",
        "-S",
        str(Path(__file__).resolve()),
        *sys.argv[1:],
    ]
    argv.append("--sealed-stage2")
    os.execve(sys.executable, argv, environment)
    raise AssertionError("unreachable")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SealedLauncherError as exc:
        sys.stderr.write(f"study-read-mcp-sealed-launcher: {exc}\n")
        raise SystemExit(2) from exc
