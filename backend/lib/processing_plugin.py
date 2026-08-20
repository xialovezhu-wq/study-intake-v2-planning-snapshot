#!/usr/bin/env python3
"""Versioned Processing Skill binding and fail-closed background MCP freeze."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


SHA256_RE = __import__("re").compile(r"^[0-9a-f]{64}$")
UPSTREAM_ERROR_CODE_RE = __import__("re").compile(r"^[A-Z][A-Z0-9_]{0,63}$")
SUBJECTS = ("math", "cs408", "english")
SKILL_IDS = {
    "math": "background-math-processing",
    "cs408": "background-cs408-processing",
    "english": "background-english-processing",
}
SUBJECT_CAPABILITIES = {
    "math": ["authority", "preprocessor_release"],
    "cs408": ["authority", "curation_inventory", "preprocessor_release"],
    "english": ["authority", "preprocessor_release"],
}
SUBJECT_MCP_SERVERS = {
    "math": "kaoyan_math_read",
    "cs408": "kaoyan_cs408_read",
    "english": "kaoyan_english_read",
}
FOCUSED_MCP_TOOLS = {
    "get_task_context",
    "read_task_artifact",
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
}
CAPTURE_IDENTITY_KEYS = {
    "formal_id",
    "publication_id",
    "article_id",
    "source_id",
    "review_identity",
    "content_fingerprint",
}
CAPTURE_ARTIFACT_KEYS = {
    "artifact_id",
    "subject",
    "artifact_kind",
    "relative_path",
    "sha256",
    "byte_length",
    "media_type",
    "encoding",
    "source_role",
}


class ProcessingPluginError(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        diagnostic: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.diagnostic = dict(diagnostic or {})


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _sha256_value(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def _sha256_json_value(value: Any) -> str:
    """Hash canonical JSON bytes used by preprocessor-owned transcripts."""

    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return _sha256_bytes(raw)


def _is_aware_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value or len(value) > 80:
        return False
    try:
        parsed = dt.datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _is_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return dt.date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _load_object(path: Path, *, maximum: int = 2 * 1024 * 1024) -> dict[str, Any]:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or node.st_size > maximum:
            raise ProcessingPluginError("processing_plugin_object_unsafe")
        value = json.loads(path.read_text(encoding="utf-8"))
    except ProcessingPluginError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProcessingPluginError("processing_plugin_object_invalid") from exc
    if not isinstance(value, dict):
        raise ProcessingPluginError("processing_plugin_object_invalid")
    return value


def _safe_text(path: Path, *, maximum: int = 256 * 1024) -> str:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or node.st_size > maximum:
            raise ProcessingPluginError("processing_plugin_text_unsafe")
        return path.read_text(encoding="utf-8")
    except ProcessingPluginError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ProcessingPluginError("processing_plugin_text_invalid") from exc


def _validate_mcp_release_binding(
    root: Path,
    manifest: Mapping[str, Any],
    lock: Mapping[str, Any],
) -> None:
    """Reopen every immutable MCP source named by the component binding."""

    invalid = "processing_component_lock_invalid"
    source_files = manifest.get("source_files")
    release_id = manifest.get("release_id")
    server_release = manifest.get("server_release")
    if (
        not isinstance(source_files, Mapping)
        or not source_files
        or not isinstance(release_id, str)
        or SHA256_RE.fullmatch(release_id) is None
        or root.name != release_id
        or _sha256_value(dict(source_files)) != release_id
        or lock.get("mcp_release_id") != release_id
        or lock.get("mcp_server_release") != server_release
    ):
        raise ProcessingPluginError(invalid)
    required_paths = {
        "pyproject.toml",
        "requirements.lock",
        "config/codex-mcp-snippet.toml",
        "config/skill-tool-policy.json",
        "scripts/sealed_launcher.py",
    }
    try:
        package_root = root / "src" / "study_read_mcp"
        if package_root.is_symlink() or not package_root.is_dir():
            raise ProcessingPluginError(invalid)
        required_paths.update(
            path.relative_to(root).as_posix()
            for path in package_root.rglob("*.py")
            if path.is_file() and not path.is_symlink()
        )
        if set(source_files) != required_paths:
            raise ProcessingPluginError(invalid)
        for relative, expected in source_files.items():
            if not isinstance(relative, str) or not isinstance(expected, str):
                raise ProcessingPluginError(invalid)
            relative_path = Path(relative)
            path = root / relative_path
            node = path.lstat()
            if (
                relative_path.is_absolute()
                or ".." in relative_path.parts
                or path.is_symlink()
                or not stat.S_ISREG(node.st_mode)
                or SHA256_RE.fullmatch(expected) is None
                or _sha256_file(path) != expected
            ):
                raise ProcessingPluginError(invalid)
        policy_path = root / "config" / "skill-tool-policy.json"
        policy = _load_object(policy_path)
        launcher_text = _safe_text(root / "scripts" / "sealed_launcher.py")
    except ProcessingPluginError:
        raise
    except OSError as exc:
        raise ProcessingPluginError(invalid) from exc
    base_version = policy.get("server_release")
    if (
        policy.get("schema_version") != "study-read-mcp-policy.v4"
        or not isinstance(base_version, str)
        or base_version != lock.get("minimum_mcp_server_release")
        or source_files.get("config/skill-tool-policy.json")
        != _sha256_file(policy_path)
        or server_release != f"{base_version}+sha256.{release_id}"
        or "sys.path.append(str(dependency_root))" not in launcher_text
        or "unlike site.addsitedir" not in launcher_text
        or '        "-I",\n        "-S",' not in launcher_text
    ):
        raise ProcessingPluginError(invalid)
    sealed_contract = policy.get("sealed_launcher_contract")
    launcher_profiles = policy.get("production_launcher_profiles")
    expected_sealed_contract = {
        "schema_version": "study-read-mcp-sealed-launcher.v1",
        "required_component_fields": [
            "python_executable",
            "release_root",
            "release_id",
            "release_manifest_sha256",
            "sealed_launcher_path",
            "sealed_launcher_sha256",
        ],
        "python_flags": ["-I", "-S"],
        "modes": [
            "server",
            "client",
            "subject-server",
            "preflight-server",
            "snapshot",
        ],
        "environment_allowlist": [
            "PATH",
            "PYTHONUTF8",
            "PYTHONDONTWRITEBYTECODE",
            "PYTHONNOUSERSITE",
            "PYTHONSAFEPATH",
            "PYTHONPATH",
            "STUDY_READ_MCP_EXPECTED_PROJECT_ROOT",
            "STUDY_READ_MCP_EXPECTED_RELEASE_ID",
            "STUDY_READ_MCP_EXPECTED_RELEASE_MANIFEST_SHA256",
        ],
    }
    expected_launcher_profiles = {
        "ordinary": {
            "mode": "server",
            "profile": "ordinary",
            "subjects": "math,cs408,english",
        },
        "background": {
            "mode": "client",
            "profile": "background",
            "subject": "required",
        },
        "morning_preparation": {
            "mode": "client",
            "profile": "morning_preparation",
            "subject": "cs408",
        },
        "analysis": {
            "mode": "subject-server",
            "subject": "required",
            "read_session_manifest": "required",
            "preprocessor_root": "required",
        },
        "critical_review": {
            "mode": "subject-server",
            "subject": "required",
            "read_session_manifest": "required",
            "preprocessor_root": "required",
        },
        "infrastructure_preflight": {
            "mode": "preflight-server",
            "subject": "required",
            "preflight_session_manifest": "required",
            "preprocessor_root": "required",
            "tool_policy": "required",
        },
        "authority_snapshot": {
            "mode": "snapshot",
            "subject": "required",
            "source_root": "required",
            "output_root": "required",
        },
    }
    if (
        sealed_contract != expected_sealed_contract
        or launcher_profiles != expected_launcher_profiles
    ):
        raise ProcessingPluginError(invalid)


def _atomic_publish(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    payload = _canonical_bytes(value)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_temp)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise ProcessingPluginError("evidence_freeze_no_clobber_conflict")
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _atomic_publish_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_temp)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
                raise ProcessingPluginError("evidence_freeze_no_clobber_conflict")
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class PluginSnapshot:
    plugin_root: Path
    plugin_name: str
    plugin_version: str
    component_lock_sha256: str
    skill_id: str
    skill_version: str
    skill_sha256: str
    skill_text: str
    shared_contract_text: str
    mcp_server_release: str


class ProcessingPluginHost:
    """Resolve immutable plugin bytes and create one HMAC-bound evidence freeze."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        runtime_root: Path,
        candidate_release_id: str,
        subject_roots: Mapping[str, str | Path] | None = None,
        require_authority_snapshot: bool = False,
    ) -> None:
        if config.get("enabled") is not True:
            raise ProcessingPluginError("processing_plugin_disabled")
        self.config = copy.deepcopy(dict(config))
        self.runtime_root = runtime_root.resolve()
        if not SHA256_RE.fullmatch(candidate_release_id):
            raise ProcessingPluginError("processing_candidate_release_invalid")
        self.candidate_release_id = candidate_release_id
        self.require_authority_snapshot = require_authority_snapshot
        self.subject_roots: dict[str, Path] = {}
        if subject_roots is not None:
            for subject, raw_root in subject_roots.items():
                if subject not in SUBJECTS:
                    raise ProcessingPluginError("processing_subject_root_invalid")
                try:
                    root = Path(raw_root).expanduser().resolve(strict=True)
                except OSError as exc:
                    raise ProcessingPluginError(
                        "processing_subject_root_invalid"
                    ) from exc
                if not root.is_dir():
                    raise ProcessingPluginError("processing_subject_root_invalid")
                self.subject_roots[subject] = root
        if require_authority_snapshot and not self.subject_roots:
            raise ProcessingPluginError("processing_subject_roots_missing")
        self.plugin_root = Path(str(config.get("root") or "")).resolve()
        self.lock_path = Path(str(config.get("component_lock_path") or "")).resolve()
        # Keep the configured executable spelling: the component lock binds the
        # exact absolute path, including the intentional executable symlink.
        self.python_path = Path(
            os.path.abspath(os.path.expanduser(str(config.get("mcp_client_python") or "")))
        )
        raw_mcp_root = Path(str(config.get("mcp_project_root") or "")).expanduser()
        if not raw_mcp_root.is_absolute() or raw_mcp_root.is_symlink():
            raise ProcessingPluginError("processing_plugin_paths_invalid")
        self.mcp_project_root = raw_mcp_root.resolve()
        self.mcp_manifest_path = self.mcp_project_root / "release.json"
        self.mcp_sealed_launcher_path = (
            self.mcp_project_root / "scripts" / "sealed_launcher.py"
        )
        self.authority_key_path = Path(str(config.get("authority_key_path") or "")).resolve()
        timeout = config.get("timeout_seconds")
        if isinstance(timeout, bool) or not isinstance(timeout, int) or not 2 <= timeout <= 60:
            raise ProcessingPluginError("processing_mcp_timeout_invalid")
        self.timeout_seconds = timeout
        if (
            self.lock_path.parent != self.plugin_root
            or not self.python_path.is_file()
            or not os.access(self.python_path, os.X_OK)
            or not (self.mcp_project_root / "pyproject.toml").is_file()
            or self.mcp_manifest_path.is_symlink()
            or not self.mcp_manifest_path.is_file()
            or self.mcp_sealed_launcher_path.is_symlink()
            or not self.mcp_sealed_launcher_path.is_file()
        ):
            raise ProcessingPluginError("processing_plugin_paths_invalid")

    def _snapshot(self, subject: str) -> PluginSnapshot:
        if subject not in SUBJECTS:
            raise ProcessingPluginError("processing_subject_invalid")
        lock = _load_object(self.lock_path)
        mcp_manifest_path = self.mcp_project_root / "release.json"
        mcp_manifest = _load_object(mcp_manifest_path)
        components = _load_object(self.plugin_root / "components.json")
        component_mcp = components.get("mcp")
        sealed_runtime = lock.get("mcp_sealed_runtime")
        expected_sealed_runtime = {
            "python_executable": str(self.python_path),
            "python_flags": ["-I", "-S"],
            "sealed_launcher_path": str(self.mcp_sealed_launcher_path),
            "sealed_launcher_sha256": _sha256_file(
                self.mcp_sealed_launcher_path
            ),
            "release_root": str(self.mcp_project_root),
            "release_id": self.mcp_project_root.name,
            "release_manifest_sha256": _sha256_file(mcp_manifest_path),
        }
        if (
            lock.get("schema_version") != "kaoyan-study-intake-component-lock.v1"
            or lock.get("plugin_name") != "kaoyan-study-intake"
            or not isinstance(lock.get("plugin_version"), str)
            or lock.get("formal_write_count") != 0
            or lock.get("registry_sha256")
            != _sha256_file(self.plugin_root / "components.json")
            or not isinstance(component_mcp, Mapping)
            or sealed_runtime != expected_sealed_runtime
            or component_mcp.get("python_executable")
            != expected_sealed_runtime["python_executable"]
            or component_mcp.get("python_flags") != ["-I", "-S"]
            or component_mcp.get("sealed_launcher_path")
            != expected_sealed_runtime["sealed_launcher_path"]
            or component_mcp.get("sealed_launcher_sha256")
            != expected_sealed_runtime["sealed_launcher_sha256"]
            or component_mcp.get("release_root")
            != expected_sealed_runtime["release_root"]
            or component_mcp.get("release_id")
            != expected_sealed_runtime["release_id"]
            or component_mcp.get("release_manifest_sha256")
            != expected_sealed_runtime["release_manifest_sha256"]
            or component_mcp.get("release_manifest") != str(mcp_manifest_path)
            or lock.get("portable_plugin_sha256")
            != _sha256_file(self.plugin_root / "plugin.json")
            or lock.get("portable_mcp_sha256")
            != _sha256_file(self.plugin_root / "mcp.json")
            or lock.get("launcher_sha256")
            != _sha256_file(self.plugin_root / "bin" / "kaoyan-read")
            or not isinstance(lock.get("mcp_server_release"), str)
            or not str(lock.get("mcp_server_release")).startswith(
                f"{lock.get('minimum_mcp_server_release')}+sha256."
            )
            or lock.get("mcp_release_id")
            != str(lock.get("mcp_server_release")).rsplit(
                "+sha256.", 1
            )[-1]
            or lock.get("mcp_release_manifest_sha256")
            != _sha256_file(mcp_manifest_path)
            or lock.get("mcp_release_root") != str(self.mcp_project_root)
            or mcp_manifest.get("schema_version")
            != "study-read-mcp-release.v1"
            or mcp_manifest.get("release_id") != lock.get("mcp_release_id")
            or mcp_manifest.get("server_release")
            != lock.get("mcp_server_release")
            or mcp_manifest.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("processing_component_lock_invalid")
        _validate_mcp_release_binding(
            self.mcp_project_root,
            mcp_manifest,
            lock,
        )
        skill_id = SKILL_IDS[subject]
        skills = lock.get("skills")
        skill = skills.get(skill_id) if isinstance(skills, Mapping) else None
        skill_path = self.plugin_root / "skills" / skill_id / "SKILL.md"
        if (
            not isinstance(skill, Mapping)
            or not isinstance(skill.get("version"), str)
            or not isinstance(skill.get("sha256"), str)
            or skill.get("sha256") != _sha256_file(skill_path)
        ):
            raise ProcessingPluginError("processing_skill_binding_invalid")
        shared_path = self.plugin_root / "references" / "shared-processing-contract.md"
        references = lock.get("references")
        if (
            not isinstance(references, Mapping)
            or references.get(shared_path.name) != _sha256_file(shared_path)
        ):
            raise ProcessingPluginError("processing_reference_binding_invalid")
        return PluginSnapshot(
            plugin_root=self.plugin_root,
            plugin_name=str(lock["plugin_name"]),
            plugin_version=str(lock["plugin_version"]),
            component_lock_sha256=_sha256_file(self.lock_path),
            skill_id=skill_id,
            skill_version=str(skill["version"]),
            skill_sha256=str(skill["sha256"]),
            skill_text=_safe_text(skill_path),
            shared_contract_text=_safe_text(shared_path),
            mcp_server_release=str(lock["mcp_server_release"]),
        )

    def _sealed_runtime_binding(self) -> dict[str, Any]:
        lock = _load_object(self.lock_path)
        runtime = lock.get("mcp_sealed_runtime")
        expected_keys = {
            "python_executable",
            "python_flags",
            "sealed_launcher_path",
            "sealed_launcher_sha256",
            "release_root",
            "release_id",
            "release_manifest_sha256",
        }
        if (
            not isinstance(runtime, Mapping)
            or set(runtime) != expected_keys
            or runtime.get("python_executable") != str(self.python_path)
            or runtime.get("python_flags") != ["-I", "-S"]
            or runtime.get("sealed_launcher_path")
            != str(self.mcp_sealed_launcher_path)
            or runtime.get("sealed_launcher_sha256")
            != _sha256_file(self.mcp_sealed_launcher_path)
            or runtime.get("release_root") != str(self.mcp_project_root)
            or runtime.get("release_id") != self.mcp_project_root.name
            or runtime.get("release_manifest_sha256")
            != _sha256_file(self.mcp_manifest_path)
        ):
            raise ProcessingPluginError("processing_component_lock_invalid")
        return dict(runtime)

    def _sealed_launcher_command(
        self,
        *,
        mode: str,
        arguments: Sequence[str],
    ) -> list[str]:
        runtime = self._sealed_runtime_binding()
        return [
            str(runtime["python_executable"]),
            *list(runtime["python_flags"]),
            str(runtime["sealed_launcher_path"]),
            "--release-root",
            str(runtime["release_root"]),
            "--expected-release-id",
            str(runtime["release_id"]),
            "--expected-release-manifest-sha256",
            str(runtime["release_manifest_sha256"]),
            "--mode",
            mode,
            *arguments,
        ]

    @staticmethod
    def _sealed_launcher_environment() -> dict[str, str]:
        return {
            "PATH": "/usr/bin:/bin",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
        }

    def _route(
        self,
        *,
        snapshot: PluginSnapshot,
        scope_hash: str,
        route_id: str,
    ) -> dict[str, Any]:
        return {
            "caller_skill_id": snapshot.skill_id,
            "caller_skill_version": snapshot.skill_version,
            "plugin_version": snapshot.plugin_version,
            "route_request_id": route_id,
            "evidence_scope_hash": scope_hash,
            "read_route": "mcp",
            "chunk_index": 1,
            "chunk_count": 1,
            "consumed_duplicate_read_count": 0,
        }

    def subject_authority_snapshot(self, subject: str) -> dict[str, Any]:
        """Read the current subject authority without opening a model session.

        The dispatcher calls this exactly once after discovery and before the
        first task is submitted.  No semantic evidence is prefetched here.
        """

        snapshot = self._snapshot(subject)
        scope = {
            "subject": subject,
            "candidate_release_id": self.candidate_release_id,
            "mcp_server_release": snapshot.mcp_server_release,
            "purpose": "subject_luna_batch_prefreeze",
        }
        scope_hash = _sha256_value(scope)
        route = self._route(
            snapshot=snapshot,
            scope_hash=scope_hash,
            route_id=f"batch-authority-{subject}-{scope_hash[:20]}",
        )
        authority = self._call(
            subject,
            "authority_bundle",
            {
                "subjects": [subject],
                "capabilities": SUBJECT_CAPABILITIES[subject],
                "checks": [
                    "exists",
                    "parseable",
                    "release_bound",
                    "projection_bound",
                ],
                "route": route,
            },
        )
        self._validate_envelope(
            authority,
            route=route,
            subject=subject,
            tool="authority_bundle",
            expected_server_release=snapshot.mcp_server_release,
        )
        subject_item = next(
            (
                row
                for row in authority.get("items", [])
                if isinstance(row, Mapping) and row.get("subject") == subject
            ),
            None,
        )
        generation = (
            subject_item.get("generation")
            if isinstance(subject_item, Mapping)
            else None
        )
        fingerprint = (
            subject_item.get("authority_fingerprint")
            if isinstance(subject_item, Mapping)
            else None
        )
        if (
            not isinstance(subject_item, Mapping)
            or subject_item.get("available") is not True
            or not isinstance(generation, str)
            or not generation
            or not isinstance(fingerprint, str)
            or SHA256_RE.fullmatch(fingerprint) is None
            or subject_item.get("adapter_release")
            != snapshot.mcp_server_release
            or authority.get("server_release")
            != snapshot.mcp_server_release
            or authority.get("model_call_count") != 0
            or authority.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("background_mcp_authority_binding_invalid")
        return {
            "schema_version": "subject_authority_snapshot_v1",
            "subject": subject,
            "generation": generation,
            "authority_fingerprint": fingerprint,
            "mcp_server_release": snapshot.mcp_server_release,
            "route_request_id": route["route_request_id"],
            "scope_sha256": scope_hash,
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def _call(self, subject: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        request = _canonical_bytes({"tool": tool, "arguments": dict(arguments)})
        command = self._sealed_launcher_command(
            mode="client",
            arguments=[
                "--profile",
                "background",
                "--subject",
                subject,
            ],
        )
        subject_root = self.subject_roots.get(subject)
        if self.require_authority_snapshot and subject_root is None:
            raise ProcessingPluginError("processing_subject_root_missing")
        if subject_root is not None:
            command.extend(("--subject-root", str(subject_root)))
        try:
            completed = subprocess.run(
                command,
                input=request,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd="/",
                env=self._sealed_launcher_environment(),
                timeout=self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProcessingPluginError("background_mcp_timeout") from exc
        except OSError as exc:
            raise ProcessingPluginError("background_mcp_exec_failed") from exc
        if completed.returncode != 0 or len(completed.stdout) > 262_144:
            raise ProcessingPluginError("background_mcp_client_failed")
        try:
            value = json.loads(completed.stdout)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ProcessingPluginError("background_mcp_invalid_json") from exc
        if not isinstance(value, dict):
            raise ProcessingPluginError("background_mcp_invalid_response")
        return value

    def _freeze_authority_snapshot(
        self,
        *,
        subject: str,
        generation: str,
        authority_fingerprint: str,
        mcp_server_release: str,
    ) -> dict[str, Any]:
        source_root = self.subject_roots.get(subject)
        if source_root is None:
            raise ProcessingPluginError("processing_subject_root_missing")
        snapshot_root = (
            self.runtime_root / "private" / "mcp-authority-snapshots"
        ).resolve()
        command = self._sealed_launcher_command(
            mode="snapshot",
            arguments=[
                "--subject",
                subject,
                "--source-root",
                str(source_root),
                "--output-root",
                str(snapshot_root),
            ],
        )
        try:
            completed = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd="/",
                env=self._sealed_launcher_environment(),
                timeout=max(self.timeout_seconds, 60),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProcessingPluginError("mcp_authority_snapshot_timeout") from exc
        except OSError as exc:
            raise ProcessingPluginError("mcp_authority_snapshot_exec_failed") from exc
        if completed.returncode != 0 or len(completed.stdout) > 64 * 1024:
            raise ProcessingPluginError("mcp_authority_snapshot_failed")
        try:
            value = json.loads(completed.stdout)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ProcessingPluginError("mcp_authority_snapshot_invalid") from exc
        expected_keys = {
            "authority_snapshot_manifest_path",
            "authority_snapshot_manifest_sha256",
            "authority_snapshot_root",
            "generation",
            "authority_fingerprint",
            "file_count",
            "total_bytes",
            "formal_write_count",
        }
        if not isinstance(value, Mapping) or set(value) != expected_keys:
            raise ProcessingPluginError("mcp_authority_snapshot_invalid")
        manifest_sha256 = value.get("authority_snapshot_manifest_sha256")
        manifest_path = Path(
            str(value.get("authority_snapshot_manifest_path") or "")
        ).resolve()
        frozen_root = Path(
            str(value.get("authority_snapshot_root") or "")
        ).resolve()
        try:
            manifest_path.relative_to(snapshot_root)
            frozen_root.relative_to(snapshot_root)
        except ValueError as exc:
            raise ProcessingPluginError("mcp_authority_snapshot_path_invalid") from exc
        if (
            value.get("generation") != generation
            or value.get("authority_fingerprint") != authority_fingerprint
            or value.get("formal_write_count") != 0
            or not isinstance(value.get("file_count"), int)
            or value.get("file_count") <= 0
            or not isinstance(value.get("total_bytes"), int)
            or value.get("total_bytes") <= 0
            or not isinstance(manifest_sha256, str)
            or SHA256_RE.fullmatch(manifest_sha256) is None
            or not manifest_path.is_file()
            or _sha256_file(manifest_path) != manifest_sha256
            or not frozen_root.is_dir()
        ):
            raise ProcessingPluginError("mcp_authority_snapshot_binding_invalid")
        manifest = _load_object(manifest_path)
        if (
            manifest.get("schema_version")
            != "study-read-mcp-authority-snapshot.v2"
            or manifest.get("subject") != subject
            or manifest.get("source_generation") != generation
            or manifest.get("source_authority_fingerprint")
            != authority_fingerprint
            or manifest.get("mcp_server_release") != mcp_server_release
            or manifest.get("file_count") != value.get("file_count")
            or manifest.get("total_bytes") != value.get("total_bytes")
            or manifest.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("mcp_authority_snapshot_binding_invalid")
        core = {
            "schema_version": "mcp_authority_snapshot_receipt_v2",
            "subject": subject,
            "candidate_release_id": self.candidate_release_id,
            "mcp_server_release": mcp_server_release,
            "generation": generation,
            "authority_fingerprint": authority_fingerprint,
            "authority_snapshot_manifest_sha256": manifest_sha256,
            "file_count": value["file_count"],
            "total_bytes": value["total_bytes"],
            "formal_write_count": 0,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        key = self._authority_key()
        receipt = {
            **core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "mcp-authority-snapshot-receipt-v2",
                    "payload": core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        receipt_sha256 = _sha256_value(receipt)
        receipt_path = (
            self.runtime_root
            / "dispatch"
            / "mcp-authority-snapshot-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        _atomic_publish(receipt_path, receipt)
        return {
            **dict(value),
            "authority_snapshot_receipt": receipt,
            "authority_snapshot_receipt_sha256": receipt_sha256,
            "authority_snapshot_receipt_ref": (
                "study-intake-mcp-authority-snapshot://sha256/"
                + receipt_sha256
            ),
        }

    def _validate_envelope(
        self,
        value: Mapping[str, Any],
        *,
        route: Mapping[str, Any],
        subject: str,
        tool: str,
        expected_server_release: str,
        require_subject: bool = False,
    ) -> None:
        if value.get("ok") is False:
            expected_error_keys = {
                "ok",
                "schema_version",
                "server_release",
                "request_id",
                "tool",
                "read_route",
                "error",
                "formal_write_count",
                "model_call_count",
                "mcp_tool_call_count",
            }
            error = value.get("error")
            upstream_code = (
                error.get("code") if isinstance(error, Mapping) else None
            )
            retryable = (
                error.get("retryable") if isinstance(error, Mapping) else None
            )
            if (
                set(value) != expected_error_keys
                or value.get("schema_version") != "study-read-mcp.v3"
                or value.get("server_release") != expected_server_release
                or value.get("tool") != tool
                or value.get("read_route") != route
                or not isinstance(value.get("request_id"), str)
                or not 1 <= len(str(value.get("request_id"))) <= 128
                or not isinstance(error, Mapping)
                or set(error) != {"code", "message", "retryable"}
                or not isinstance(upstream_code, str)
                or UPSTREAM_ERROR_CODE_RE.fullmatch(upstream_code) is None
                or not isinstance(error.get("message"), str)
                or len(str(error.get("message"))) > 4096
                or not isinstance(retryable, bool)
                or any(
                    isinstance(value.get(key), bool)
                    or value.get(key) != 0
                    for key in (
                        "formal_write_count",
                        "model_call_count",
                        "mcp_tool_call_count",
                    )
                )
            ):
                raise ProcessingPluginError("background_mcp_envelope_invalid")
            mapped_code = "background_mcp_" + upstream_code.casefold()
            diagnostic = self._persist_background_mcp_failure(
                subject=subject,
                tool=tool,
                route=route,
                server_release=expected_server_release,
                upstream_code=upstream_code,
                upstream_retryable=retryable,
                mapped_error_code=mapped_code,
            )
            raise ProcessingPluginError(mapped_code, diagnostic=diagnostic)
        if (
            value.get("ok") is not True
            or value.get("schema_version") != "study-read-mcp.v3"
            or value.get("profile") != "background"
            or value.get("read_route") != route
            or value.get("formal_write_count") != 0
            or value.get("model_call_count") != 0
            or not isinstance(value.get("generation"), str)
            or not isinstance(value.get("authority_fingerprint"), str)
            or not SHA256_RE.fullmatch(str(value.get("authority_fingerprint")))
            or not isinstance(value.get("server_release"), str)
            or value.get("server_release") != expected_server_release
        ):
            raise ProcessingPluginError("background_mcp_envelope_invalid")
        if require_subject and value.get("subject") != subject:
            raise ProcessingPluginError("background_mcp_subject_mismatch")

    def _background_mcp_failure_receipt_path(self, receipt_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(receipt_sha256):
            raise ProcessingPluginError(
                "background_mcp_failure_receipt_invalid"
            )
        return (
            self.runtime_root
            / "dispatch"
            / "background-mcp-failure-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _persist_background_mcp_failure(
        self,
        *,
        subject: str,
        tool: str,
        route: Mapping[str, Any],
        server_release: str,
        upstream_code: str,
        upstream_retryable: bool,
        mapped_error_code: str,
    ) -> dict[str, str]:
        """Persist only a redacted upstream failure, never its raw payload."""

        core = {
            "schema_version": "background_mcp_failure_receipt_v1",
            "subject": subject,
            "candidate_release_id": self.candidate_release_id,
            "route": copy.deepcopy(dict(route)),
            "tool": tool,
            "server_release": server_release,
            "upstream_error_code": upstream_code,
            "upstream_retryable": upstream_retryable,
            "mapped_error_code": mapped_error_code,
            "upstream_message_included": False,
            "raw_payload_included": False,
            "items_included": False,
            "model_call_count": 0,
            "formal_write_count": 0,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        key = self._authority_key()
        receipt = {
            **core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "background-mcp-failure-receipt-v1",
                    "payload": core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        receipt_sha256 = _sha256_value(receipt)
        path = self._background_mcp_failure_receipt_path(receipt_sha256)
        _atomic_publish(path, receipt)
        if _sha256_value(_load_object(path)) != receipt_sha256:
            raise ProcessingPluginError(
                "background_mcp_failure_receipt_write_mismatch"
            )
        return {
            "background_mcp_failure_receipt_sha256": receipt_sha256,
            "background_mcp_failure_receipt_ref": (
                "study-intake-background-mcp-failure://sha256/"
                + receipt_sha256
            ),
        }

    def validate_background_mcp_failure_receipt(
        self,
        *,
        receipt_sha256: str,
    ) -> dict[str, Any]:
        path = self._background_mcp_failure_receipt_path(receipt_sha256)
        receipt = self._load_content_addressed_object(
            path,
            receipt_sha256,
            error_code="background_mcp_failure_receipt_invalid",
        )
        expected_keys = {
            "schema_version",
            "subject",
            "candidate_release_id",
            "route",
            "tool",
            "server_release",
            "upstream_error_code",
            "upstream_retryable",
            "mapped_error_code",
            "upstream_message_included",
            "raw_payload_included",
            "items_included",
            "model_call_count",
            "formal_write_count",
            "created_at",
            "hmac_key_id",
            "hmac_sha256",
        }
        core = {
            key: copy.deepcopy(value)
            for key, value in receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        expected_hmac = hmac.new(
            key,
            _canonical_bytes({
                "purpose": "background-mcp-failure-receipt-v1",
                "payload": core,
            }),
            hashlib.sha256,
        ).hexdigest()
        route = receipt.get("route")
        upstream_code = receipt.get("upstream_error_code")
        if (
            set(receipt) != expected_keys
            or receipt.get("schema_version")
            != "background_mcp_failure_receipt_v1"
            or receipt.get("subject") not in SUBJECTS
            or receipt.get("candidate_release_id")
            != self.candidate_release_id
            or not isinstance(route, Mapping)
            or set(route)
            != {
                "caller_skill_id",
                "caller_skill_version",
                "plugin_version",
                "route_request_id",
                "evidence_scope_hash",
                "read_route",
                "chunk_index",
                "chunk_count",
                "consumed_duplicate_read_count",
            }
            or route.get("read_route") != "mcp"
            or not SHA256_RE.fullmatch(
                str(route.get("evidence_scope_hash") or "")
            )
            or not isinstance(receipt.get("tool"), str)
            or not isinstance(receipt.get("server_release"), str)
            or not isinstance(upstream_code, str)
            or UPSTREAM_ERROR_CODE_RE.fullmatch(upstream_code) is None
            or not isinstance(receipt.get("upstream_retryable"), bool)
            or receipt.get("mapped_error_code")
            != "background_mcp_" + str(upstream_code).casefold()
            or receipt.get("upstream_message_included") is not False
            or receipt.get("raw_payload_included") is not False
            or receipt.get("items_included") is not False
            or receipt.get("model_call_count") != 0
            or receipt.get("formal_write_count") != 0
            or not isinstance(receipt.get("created_at"), str)
            or receipt.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(
                str(receipt.get("hmac_sha256") or ""), expected_hmac
            )
        ):
            raise ProcessingPluginError(
                "background_mcp_failure_receipt_invalid"
            )
        return dict(receipt)

    @staticmethod
    def _read_arguments(
        subject: str,
        *,
        study_date: str,
        input_binding: Mapping[str, Any],
        route: Mapping[str, Any],
        generation: str,
        adapter_release: str,
        fingerprint: str,
    ) -> tuple[str, dict[str, Any]]:
        if subject == "math":
            return (
                "math_read_bundle",
                {
                    "queries": [
                        {
                            "op": "activity_window",
                            "ids": [],
                            "date_from": study_date,
                            "date_to": study_date,
                            "fields": [],
                        }
                    ],
                    "expected_generation": generation,
                    "expected_release": adapter_release,
                    "expected_fingerprint": fingerprint,
                    "route": dict(route),
                },
            )
        if subject == "cs408":
            capture_id = input_binding.get("capture_id")
            ids = [capture_id] if isinstance(capture_id, str) else []
            return (
                "cs408_read_bundle",
                {
                    "queries": [
                        {
                            "op": "curation_inventory",
                            "ids": ids,
                            "study_date": study_date,
                            "fields": [],
                        }
                    ],
                    "expected_generation": generation,
                    "expected_release": adapter_release,
                    "expected_fingerprint": fingerprint,
                    "route": dict(route),
                },
            )
        article_id = input_binding.get("article_id")
        if not isinstance(article_id, str) or not article_id:
            raise ProcessingPluginError("background_english_article_missing")
        return (
            "english_read_bundle",
            {
                "request": {
                    "article_id": article_id,
                    "sentence_ids": [],
                    "terms": [],
                    "study_date": None,
                    "include": ["coverage"],
                    "expected_generation": generation,
                    "expected_release": adapter_release,
                    "expected_fingerprint": fingerprint,
                },
                "route": dict(route),
            },
        )

    def _authority_key(self) -> bytes:
        try:
            node = self.authority_key_path.lstat()
            if (
                self.authority_key_path.is_symlink()
                or not self.authority_key_path.is_file()
                or stat.S_IMODE(node.st_mode) != 0o600
            ):
                raise ProcessingPluginError("evidence_freeze_authority_key_unsafe")
            key = self.authority_key_path.read_bytes()
        except ProcessingPluginError:
            raise
        except OSError as exc:
            raise ProcessingPluginError("evidence_freeze_authority_key_missing") from exc
        if len(key) != 32:
            raise ProcessingPluginError("evidence_freeze_authority_key_invalid")
        return key

    def _read_session_path(self, manifest_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(manifest_sha256):
            raise ProcessingPluginError("mcp_read_session_hash_invalid")
        return (
            self.runtime_root
            / "private"
            / "mcp-read-sessions"
            / "sha256"
            / manifest_sha256[:2]
            / f"{manifest_sha256}.json"
        )

    def _model_call_receipt_path(self, receipt_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(receipt_sha256):
            raise ProcessingPluginError("mcp_model_call_receipt_invalid")
        return (
            self.runtime_root
            / "dispatch"
            / "mcp-read-session-call-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _capture_freeze_receipt_path(self, receipt_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(receipt_sha256):
            raise ProcessingPluginError("capture_freeze_receipt_invalid")
        return (
            self.runtime_root
            / "dispatch"
            / "capture-freeze-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _read_session_receipt_path(self, receipt_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(receipt_sha256):
            raise ProcessingPluginError("mcp_read_session_receipt_invalid")
        return (
            self.runtime_root
            / "dispatch"
            / "mcp-read-session-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _authority_snapshot_receipt_path(self, receipt_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(receipt_sha256):
            raise ProcessingPluginError("mcp_authority_snapshot_receipt_invalid")
        return (
            self.runtime_root
            / "dispatch"
            / "mcp-authority-snapshot-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )

    def _stage_transcript_path(self, transcript_sha256: str) -> Path:
        if not SHA256_RE.fullmatch(transcript_sha256):
            raise ProcessingPluginError("mcp_stage_transcript_invalid")
        return (
            self.runtime_root
            / "private"
            / "reports"
            / "mcp-stage-transcripts"
            / "sha256"
            / transcript_sha256[:2]
            / f"{transcript_sha256}.json"
        )

    @staticmethod
    def _load_content_addressed_object(
        path: Path, digest: str, *, error_code: str
    ) -> dict[str, Any]:
        try:
            value = _load_object(path)
            raw_digest = _sha256_file(path)
        except ProcessingPluginError as exc:
            raise ProcessingPluginError(error_code) from exc
        except OSError as exc:
            raise ProcessingPluginError(error_code) from exc
        if raw_digest != digest:
            raise ProcessingPluginError(error_code)
        return value

    def _capture_freeze_root(self) -> Path:
        return self.runtime_root / "dispatch" / "luna-capture-freezes"

    @staticmethod
    def _capture_artifact_id(value: str) -> str:
        if (
            __import__("re").fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", value)
            is None
            or ".." in value
            or "/" in value
            or "\\" in value
            or "\x00" in value
        ):
            raise ProcessingPluginError("capture_artifact_id_invalid")
        return value

    @staticmethod
    def _capture_image_type(raw: bytes, suffix: str) -> tuple[str, str]:
        lowered = suffix.lower()
        if raw.startswith(b"\x89PNG\r\n\x1a\n") and lowered == ".png":
            return "image/png", "png"
        if raw.startswith(b"\xff\xd8\xff") and lowered in {".jpg", ".jpeg"}:
            return "image/jpeg", "jpeg"
        if (
            len(raw) >= 12
            and raw[:4] == b"RIFF"
            and raw[8:12] == b"WEBP"
            and lowered == ".webp"
        ):
            return "image/webp", "webp"
        raise ProcessingPluginError("capture_artifact_image_invalid")

    @staticmethod
    def _capture_text_bytes(
        raw: bytes,
        *,
        artifact_kind: str,
        source_path: Path | None,
        source_copy_sha256: str,
    ) -> tuple[bytes, str, str, str]:
        """Validate text evidence and remove host paths before MCP exposure."""

        if not raw or len(raw) > 16 * 1024 * 1024:
            raise ProcessingPluginError("capture_artifact_size_invalid")
        try:
            text = raw.decode("utf-8")
        except UnicodeError as exc:
            raise ProcessingPluginError(
                "capture_artifact_text_encoding_invalid"
            ) from exc
        if not text.strip():
            raise ProcessingPluginError("capture_artifact_text_invalid")

        source_role = {
            "solution_text": "canonical_source_copy",
            "learning_record": "immutable_capture_fact",
            "question_text": "immutable_capture_fact",
            "dialogue": "user_dialogue",
            "article_text": "canonical_source_copy",
            "answer_key": "canonical_source_copy",
            "explanation": "canonical_source_copy",
            "sentence_events": "immutable_capture_fact",
        }.get(artifact_kind)
        if source_role is None:
            raise ProcessingPluginError("capture_artifact_text_kind_invalid")

        suffix = source_path.suffix.lower() if source_path is not None else ".json"
        if artifact_kind == "solution_text" and suffix not in {
            ".md",
            ".markdown",
            ".txt",
        }:
            raise ProcessingPluginError(
                "math_solution_text_source_binding_invalid"
            )
        if suffix in {".json", ".jsonl"}:
            try:
                if suffix == ".jsonl":
                    parsed: Any = [
                        json.loads(line)
                        for line in text.splitlines()
                        if line.strip()
                    ]
                else:
                    parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ProcessingPluginError(
                    "capture_artifact_text_invalid"
                ) from exc
            if not isinstance(parsed, (Mapping, list)):
                raise ProcessingPluginError("capture_artifact_text_invalid")
            redaction_count = 0
            path_keys = {
                "path",
                "root",
                "absolute_path",
                "source_path",
                "source_rollout",
                "formal_card_path",
                "manifest_path",
                "derived_from_frozen_capture",
            }

            def sanitize_metadata(value: Any, key: str | None = None) -> Any:
                nonlocal redaction_count
                normalized_key = (key or "").casefold()
                if normalized_key in path_keys or normalized_key.endswith("_path"):
                    redaction_count += 1
                    return "[LOCAL_PATH_REDACTED]"
                if isinstance(value, Mapping):
                    return {
                        str(nested_key): sanitize_metadata(
                            nested_value, str(nested_key)
                        )
                        for nested_key, nested_value in value.items()
                    }
                if isinstance(value, list):
                    return [sanitize_metadata(nested) for nested in value]
                return copy.deepcopy(value)

            sanitized_value = sanitize_metadata(parsed)
            envelope = {
                "schema_version": "study-intake-sanitized-text-artifact.v1",
                "artifact_kind": artifact_kind,
                "source_copy_sha256": source_copy_sha256,
                "path_metadata_redaction_count": redaction_count,
                "content": sanitized_value,
            }
            sanitized = _canonical_bytes(envelope)
            if any(
                marker in sanitized.decode("utf-8")
                for marker in (
                    "/Users/",
                    "/Volumes/",
                    "/private/",
                    "/var/",
                    "/tmp/",
                )
            ):
                raise ProcessingPluginError("capture_artifact_local_path_exposed")
            return sanitized, "application/json", "json", source_role

        lines = text.splitlines()
        has_final_newline = text.endswith(("\n", "\r"))
        source_path_index: int | None = None
        frontmatter_closing_index: int | None = None
        declared_source_path: str | None = None
        declared_source_sha256: str | None = None
        if lines and lines[0].strip() == "---":
            try:
                frontmatter_closing_index = next(
                    index
                    for index, line in enumerate(lines[1:], start=1)
                    if line.strip() == "---"
                )
            except StopIteration as exc:
                raise ProcessingPluginError(
                    "capture_artifact_text_frontmatter_invalid"
                ) from exc
            for index, line in enumerate(
                lines[1:frontmatter_closing_index], start=1
            ):
                key, separator, value = line.partition(":")
                if not separator:
                    continue
                normalized_key = key.strip()
                normalized_value = value.strip().strip('"').strip("'")
                if normalized_key == "source_path":
                    if source_path_index is not None or not normalized_value:
                        raise ProcessingPluginError(
                            "math_solution_text_source_binding_invalid"
                        )
                    source_path_index = index
                    declared_source_path = normalized_value
                elif normalized_key == "source_sha256":
                    if declared_source_sha256 is not None:
                        raise ProcessingPluginError(
                            "math_solution_text_source_binding_invalid"
                        )
                    declared_source_sha256 = normalized_value

        if artifact_kind == "solution_text" and (
            (declared_source_path is None) != (declared_source_sha256 is None)
        ):
            raise ProcessingPluginError(
                "math_solution_text_source_binding_invalid"
            )
        if declared_source_path is not None:
            canonical_path = Path(declared_source_path)
            try:
                canonical_node = canonical_path.lstat()
                canonical_raw = canonical_path.read_bytes()
            except OSError as exc:
                raise ProcessingPluginError(
                    "math_solution_text_source_binding_invalid"
                ) from exc
            if (
                not canonical_path.is_absolute()
                or canonical_path.is_symlink()
                or not stat.S_ISREG(canonical_node.st_mode)
                or not canonical_raw
                or len(canonical_raw) > 16 * 1024 * 1024
                or SHA256_RE.fullmatch(str(declared_source_sha256 or "")) is None
                or _sha256_bytes(canonical_raw) != declared_source_sha256
            ):
                raise ProcessingPluginError(
                    "math_solution_text_source_binding_invalid"
                )
            assert source_path_index is not None
            lines[source_path_index] = "source_path_redacted: true"
        if artifact_kind == "solution_text":
            if frontmatter_closing_index is not None:
                if any(
                    line.partition(":")[0].strip()
                    in {"source_copy_sha256", "source_path_redacted"}
                    for index, line in enumerate(
                        lines[1:frontmatter_closing_index], start=1
                    )
                    if index != source_path_index
                ):
                    raise ProcessingPluginError(
                        "capture_artifact_text_frontmatter_invalid"
                    )
                insert_at = (
                    source_path_index + 1
                    if source_path_index is not None
                    else 1
                )
                provenance_rows = [
                    f"source_copy_sha256: {source_copy_sha256}"
                ]
                if source_path_index is None:
                    provenance_rows.insert(0, "source_path_redacted: false")
                lines[insert_at:insert_at] = provenance_rows
            else:
                lines = [
                    "---",
                    "artifact_kind: solution_text",
                    "source_path_redacted: false",
                    f"source_copy_sha256: {source_copy_sha256}",
                    "---",
                    *lines,
                ]
            local_path_pattern = __import__("re").compile(
                r"/(?:Users|Volumes|private|var|tmp)/[^\s)\]}>\"']+"
            )
            lines = [
                local_path_pattern.sub("[LOCAL_PATH_REDACTED]", line)
                for line in lines
            ]
            text = "\n".join(lines) + ("\n" if has_final_newline else "")

        if artifact_kind in {
            "dialogue",
            "learning_record",
            "question_text",
            "article_text",
            "answer_key",
            "explanation",
            "sentence_events",
        }:
            local_path_pattern = __import__("re").compile(
                r"/(?:Users|Volumes|private|var|tmp)/[^\s)\]}>\"']+"
            )
            text, redaction_count = local_path_pattern.subn(
                "[LOCAL_PATH_REDACTED]", text
            )
            lines = text.splitlines()
            has_final_newline = text.endswith(("\n", "\r"))
            provenance_rows = [
                f"artifact_kind: {artifact_kind}",
                f"source_copy_sha256: {source_copy_sha256}",
                f"local_path_redaction_count: {redaction_count}",
            ]
            if lines and lines[0].strip() == "---":
                try:
                    closing_index = next(
                        index
                        for index, line in enumerate(lines[1:], start=1)
                        if line.strip() == "---"
                    )
                except StopIteration as exc:
                    raise ProcessingPluginError(
                        "capture_artifact_text_frontmatter_invalid"
                    ) from exc
                existing_keys = {
                    line.partition(":")[0].strip()
                    for line in lines[1:closing_index]
                    if ":" in line
                }
                if existing_keys.intersection(
                    {
                        "source_copy_sha256",
                        "local_path_redaction_count",
                    }
                ):
                    raise ProcessingPluginError(
                        "capture_artifact_text_frontmatter_invalid"
                    )
                lines[1:1] = provenance_rows
            else:
                lines = ["---", *provenance_rows, "---", *lines]
            text = "\n".join(lines) + ("\n" if has_final_newline else "")

        forbidden_host_path_markers = (
            "/Users/",
            "/Volumes/",
            "/private/",
            "/var/",
            "/tmp/",
        )
        if any(marker in text for marker in forbidden_host_path_markers):
            raise ProcessingPluginError("capture_artifact_local_path_exposed")
        sanitized = text.encode("utf-8")
        media_type = (
            "text/markdown"
            if suffix in {".md", ".markdown"}
            else "text/plain"
        )
        extension = "md" if media_type == "text/markdown" else "txt"
        return sanitized, media_type, extension, source_role

    @classmethod
    def _validate_math_capture_evidence(
        cls,
        artifacts: Sequence[Mapping[str, Any]],
        capture_facts: Mapping[str, Any],
    ) -> None:
        kinds = {
            str(row.get("artifact_kind"))
            for row in artifacts
            if isinstance(row, Mapping)
        }
        if "question_image" not in kinds:
            raise ProcessingPluginError("math_capture_question_image_missing")
        if "learning_record" not in kinds:
            raise ProcessingPluginError("math_capture_learning_record_missing")
        if (
            "solution_image" not in kinds
            and "solution_text" not in kinds
        ):
            raise ProcessingPluginError("math_capture_solution_evidence_missing")
        if "dialogue" not in kinds:
            raise ProcessingPluginError("math_capture_dialogue_missing")

    def _freeze_capture_artifacts(
        self,
        *,
        subject: str,
        capture_id: str,
        study_date: str,
        scene: str,
        captured_at: str,
        identity: Mapping[str, Any],
        capture_facts: Mapping[str, Any],
        source_artifacts: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], str, Path, str]:
        allowed_scenes = {
            "math": {"formal_problem", "new_intake", "study_review"},
            "cs408": {"morning_review", "formal_problem"},
            "english": {"intensive_reading", "article_review"},
        }
        if (
            subject not in allowed_scenes
            or scene not in allowed_scenes[subject]
            or not _is_iso_date(study_date)
            or not _is_aware_iso_timestamp(captured_at)
            or not isinstance(capture_id, str)
            or __import__("re").fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", capture_id
            )
            is None
            or ".." in capture_id
            or not isinstance(identity, Mapping)
            or not set(identity).issubset(CAPTURE_IDENTITY_KEYS)
            or any(
                not isinstance(value, str) or not value or len(value) > 160
                for value in identity.values()
            )
        ):
            raise ProcessingPluginError("capture_manifest_identity_invalid")
        freeze_root = self._capture_freeze_root()
        artifacts: list[dict[str, Any]] = []
        facts_payload = _canonical_bytes(capture_facts)
        if not facts_payload or len(facts_payload) > 16 * 1024 * 1024:
            raise ProcessingPluginError("capture_facts_size_invalid")
        facts_sha256 = _sha256_bytes(facts_payload)
        facts_path = (
            freeze_root
            / "artifacts"
            / "sha256"
            / facts_sha256[:2]
            / f"{facts_sha256}.json"
        )
        _atomic_publish_bytes(facts_path, facts_payload)
        artifacts.append(
            {
                "artifact_id": "capture-facts",
                "subject": subject,
                "artifact_kind": "capture_facts",
                "relative_path": facts_path.relative_to(freeze_root).as_posix(),
                "sha256": facts_sha256,
                "byte_length": len(facts_payload),
                "media_type": "application/json",
                "encoding": "utf-8",
                "source_role": "immutable_capture_fact",
            }
        )
        if not isinstance(source_artifacts, Sequence) or isinstance(
            source_artifacts, (str, bytes, bytearray)
        ):
            raise ProcessingPluginError("capture_artifacts_invalid")
        for index, value in enumerate(source_artifacts, start=1):
            if not isinstance(value, Mapping):
                raise ProcessingPluginError("capture_artifacts_invalid")
            artifact_kind = str(value.get("artifact_kind") or "image")
            source_path = value.get("path")
            inline_content = value.get("content")
            if (source_path is None) == (inline_content is None):
                raise ProcessingPluginError("capture_artifact_source_invalid")
            path: Path | None
            if inline_content is not None:
                if artifact_kind not in {
                    "solution_text",
                    "learning_record",
                    "question_text",
                    "dialogue",
                    "article_text",
                    "answer_key",
                    "explanation",
                    "sentence_events",
                } or not isinstance(inline_content, (Mapping, list)):
                    raise ProcessingPluginError("capture_artifact_source_invalid")
                path = None
                raw = _canonical_bytes(inline_content)
            else:
                if not isinstance(source_path, (str, os.PathLike)):
                    raise ProcessingPluginError("capture_artifact_source_invalid")
                path = Path(source_path).resolve()
                try:
                    node = path.lstat()
                    raw = path.read_bytes()
                except OSError as exc:
                    raise ProcessingPluginError(
                        "capture_artifact_source_invalid"
                    ) from exc
                if path.is_symlink() or not stat.S_ISREG(node.st_mode) or not raw:
                    raise ProcessingPluginError("capture_artifact_source_invalid")
            source_copy_sha256 = _sha256_bytes(raw)
            expected_sha256 = value.get("sha256")
            if expected_sha256 is not None and expected_sha256 != source_copy_sha256:
                raise ProcessingPluginError("capture_artifact_source_hash_mismatch")
            if artifact_kind in {
                "solution_text",
                "learning_record",
                "question_text",
                "dialogue",
                "article_text",
                "answer_key",
                "explanation",
                "sentence_events",
            }:
                raw, media_type, extension, source_role = self._capture_text_bytes(
                    raw,
                    artifact_kind=artifact_kind,
                    source_path=path,
                    source_copy_sha256=source_copy_sha256,
                )
                encoding = "utf-8"
            else:
                assert path is not None
                if len(raw) > 8 * 1024 * 1024:
                    raise ProcessingPluginError("capture_artifact_size_invalid")
                media_type, extension = self._capture_image_type(raw, path.suffix)
                source_role = "immutable_source_attachment"
                encoding = "binary"
            digest = _sha256_bytes(raw)
            if (
                __import__("re").fullmatch(r"[a-z][a-z0-9_]{0,63}", artifact_kind)
                is None
                or __import__("re").fullmatch(r"[a-z][a-z0-9_]{0,63}", source_role)
                is None
            ):
                raise ProcessingPluginError("capture_artifact_role_invalid")
            artifact_id = self._capture_artifact_id(
                str(value.get("artifact_id") or f"{artifact_kind}-{digest[:24]}")
            )
            frozen_path = (
                freeze_root
                / "artifacts"
                / "sha256"
                / digest[:2]
                / f"{digest}.{extension}"
            )
            _atomic_publish_bytes(frozen_path, raw)
            artifacts.append(
                {
                    "artifact_id": artifact_id,
                    "subject": subject,
                    "artifact_kind": artifact_kind,
                    "relative_path": frozen_path.relative_to(freeze_root).as_posix(),
                    "sha256": digest,
                    "byte_length": len(raw),
                    "media_type": media_type,
                    "encoding": encoding,
                    "source_role": source_role,
                }
            )
        artifacts.sort(key=lambda row: str(row["artifact_id"]))
        artifact_ids = [str(row["artifact_id"]) for row in artifacts]
        if len(artifact_ids) != len(set(artifact_ids)) or not 1 <= len(artifact_ids) <= 64:
            raise ProcessingPluginError("capture_artifact_ids_invalid")
        if subject == "math":
            self._validate_math_capture_evidence(artifacts, capture_facts)
        manifest = {
            "schema_version": "study-read-mcp-capture-manifest.v1",
            "capture_id": capture_id,
            "subject": subject,
            "study_date": study_date,
            "scene": scene,
            "captured_at": captured_at,
            "identity": copy.deepcopy(dict(identity)),
            "artifacts": artifacts,
            "formal_write_count": 0,
        }
        raw_manifest = _canonical_bytes(manifest)
        if len(raw_manifest) > 64 * 1024:
            raise ProcessingPluginError("capture_manifest_size_invalid")
        manifest_sha256 = _sha256_bytes(raw_manifest)
        manifest_path = (
            freeze_root
            / "manifests"
            / "sha256"
            / manifest_sha256[:2]
            / f"{manifest_sha256}.json"
        )
        _atomic_publish_bytes(manifest_path, raw_manifest)
        return manifest, manifest_sha256, manifest_path, facts_sha256

    def _validate_capture_manifest(
        self, session: Mapping[str, Any]
    ) -> dict[str, Any]:
        digest = session.get("capture_manifest_sha256")
        raw_path = session.get("capture_manifest_path")
        if (
            not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
            or not isinstance(raw_path, str)
        ):
            raise ProcessingPluginError("capture_manifest_binding_invalid")
        freeze_root = self._capture_freeze_root().resolve()
        expected_path = (
            freeze_root
            / "manifests"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        path = Path(raw_path)
        try:
            node = path.lstat()
            raw = path.read_bytes()
        except OSError as exc:
            raise ProcessingPluginError("capture_manifest_binding_invalid") from exc
        if (
            not path.is_absolute()
            or path.resolve() != expected_path
            or path.is_symlink()
            or not stat.S_ISREG(node.st_mode)
            or stat.S_IMODE(node.st_mode) != 0o600
            or len(raw) > 64 * 1024
            or _sha256_bytes(raw) != digest
        ):
            raise ProcessingPluginError("capture_manifest_binding_invalid")
        try:
            manifest = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ProcessingPluginError("capture_manifest_binding_invalid") from exc
        expected_keys = {
            "schema_version",
            "capture_id",
            "subject",
            "study_date",
            "scene",
            "captured_at",
            "identity",
            "artifacts",
            "formal_write_count",
        }
        identity = manifest.get("identity") if isinstance(manifest, Mapping) else None
        artifacts = manifest.get("artifacts") if isinstance(manifest, Mapping) else None
        subject_scenes = {
            "math": {"formal_problem", "new_intake", "study_review"},
            "cs408": {"morning_review", "formal_problem"},
            "english": {"intensive_reading", "article_review"},
        }
        if (
            not isinstance(manifest, Mapping)
            or set(manifest) != expected_keys
            or manifest.get("schema_version")
            != "study-read-mcp-capture-manifest.v1"
            or manifest.get("capture_id") != session.get("capture_id")
            or manifest.get("subject") != session.get("subject")
            or manifest.get("scene")
            not in subject_scenes.get(str(session.get("subject")), set())
            or not _is_iso_date(manifest.get("study_date"))
            or not _is_aware_iso_timestamp(manifest.get("captured_at"))
            or manifest.get("formal_write_count") != 0
            or not isinstance(identity, Mapping)
            or not set(identity).issubset(CAPTURE_IDENTITY_KEYS)
            or any(
                not isinstance(value, str) or not value or len(value) > 160
                for value in identity.values()
            )
            or not isinstance(artifacts, list)
        ):
            raise ProcessingPluginError("capture_manifest_binding_invalid")
        artifact_ids: list[str] = []
        facts_artifact_count = 0
        facts_value: Mapping[str, Any] | None = None
        for artifact in artifacts:
            if not isinstance(artifact, Mapping) or set(artifact) != CAPTURE_ARTIFACT_KEYS:
                raise ProcessingPluginError("capture_manifest_artifact_invalid")
            artifact_id = self._capture_artifact_id(str(artifact.get("artifact_id") or ""))
            artifact_ids.append(artifact_id)
            artifact_sha256 = str(artifact.get("sha256") or "")
            relative = str(artifact.get("relative_path") or "")
            extension = Path(relative).suffix
            expected_relative = (
                f"artifacts/sha256/{artifact_sha256[:2]}/{artifact_sha256}{extension}"
            )
            artifact_path = freeze_root / relative
            try:
                artifact_node = artifact_path.lstat()
                artifact_raw = artifact_path.read_bytes()
            except OSError as exc:
                raise ProcessingPluginError("capture_manifest_artifact_invalid") from exc
            if (
                artifact.get("subject") != session.get("subject")
                or SHA256_RE.fullmatch(artifact_sha256) is None
                or relative != expected_relative
                or artifact_path.resolve().parent.parent
                != (freeze_root / "artifacts" / "sha256").resolve()
                or artifact_path.is_symlink()
                or not stat.S_ISREG(artifact_node.st_mode)
                or stat.S_IMODE(artifact_node.st_mode) != 0o600
                or _sha256_bytes(artifact_raw) != artifact_sha256
                or artifact.get("byte_length") != len(artifact_raw)
            ):
                raise ProcessingPluginError("capture_manifest_artifact_invalid")
            artifact_kind = artifact.get("artifact_kind")
            source_role = artifact.get("source_role")
            media_type = artifact.get("media_type")
            encoding = artifact.get("encoding")
            if (
                not isinstance(artifact_kind, str)
                or __import__("re").fullmatch(
                    r"[a-z][a-z0-9_]{0,63}", artifact_kind
                )
                is None
                or not isinstance(source_role, str)
                or __import__("re").fullmatch(
                    r"[a-z][a-z0-9_]{0,63}", source_role
                )
                is None
            ):
                raise ProcessingPluginError("capture_manifest_artifact_invalid")
            if artifact_kind == "capture_facts":
                facts_artifact_count += 1
                if (
                    artifact_id != "capture-facts"
                    or source_role != "immutable_capture_fact"
                    or extension != ".json"
                    or media_type != "application/json"
                    or encoding != "utf-8"
                    or len(artifact_raw) > 16 * 1024 * 1024
                ):
                    raise ProcessingPluginError(
                        "capture_manifest_artifact_invalid"
                    )
                try:
                    facts_value = json.loads(artifact_raw)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise ProcessingPluginError(
                        "capture_manifest_artifact_invalid"
                    ) from exc
                if not isinstance(facts_value, Mapping):
                    raise ProcessingPluginError(
                        "capture_manifest_artifact_invalid"
                    )
            else:
                allowed_image_kinds = {
                    "math": {
                        "question_image",
                        "solution_image",
                        "attachment",
                    },
                    "cs408": {
                        "question_image",
                        "solution_image",
                        "attachment",
                    },
                    "english": {"attachment"},
                }
                expected_image_types = {
                    ".png": "image/png",
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".webp": "image/webp",
                }
                allowed_text_kinds = {
                    "math": {
                        "solution_text",
                        "learning_record",
                        "question_text",
                        "dialogue",
                    },
                    "cs408": set(),
                    "english": {
                        "article_text",
                        "answer_key",
                        "explanation",
                        "sentence_events",
                    },
                }
                if artifact_kind in allowed_text_kinds.get(
                    str(session.get("subject")), set()
                ):
                    expected_source_role = {
                        "solution_text": "canonical_source_copy",
                        "learning_record": "immutable_capture_fact",
                        "question_text": "immutable_capture_fact",
                        "dialogue": "user_dialogue",
                        "article_text": "canonical_source_copy",
                        "answer_key": "canonical_source_copy",
                        "explanation": "canonical_source_copy",
                        "sentence_events": "immutable_capture_fact",
                    }[str(artifact_kind)]
                    try:
                        decoded_text = artifact_raw.decode("utf-8")
                    except UnicodeError as exc:
                        raise ProcessingPluginError(
                            "capture_manifest_artifact_invalid"
                        ) from exc
                    if (
                        extension not in {".json", ".md", ".txt"}
                        or media_type
                        != {
                            ".json": "application/json",
                            ".md": "text/markdown",
                            ".txt": "text/plain",
                        }[extension]
                        or encoding != "utf-8"
                        or source_role != expected_source_role
                        or not decoded_text.strip()
                        or any(
                            marker in decoded_text
                            for marker in (
                                "/Users/",
                                "/Volumes/",
                                "/private/",
                                "/var/",
                                "/tmp/",
                            )
                        )
                    ):
                        raise ProcessingPluginError(
                            "capture_manifest_artifact_invalid"
                        )
                    if extension == ".json":
                        try:
                            decoded_value = json.loads(decoded_text)
                        except json.JSONDecodeError as exc:
                            raise ProcessingPluginError(
                                "capture_manifest_artifact_invalid"
                            ) from exc
                        if not isinstance(decoded_value, (Mapping, list)):
                            raise ProcessingPluginError(
                                "capture_manifest_artifact_invalid"
                            )
                else:
                    if (
                        artifact_kind
                        not in allowed_image_kinds.get(
                            str(session.get("subject")), set()
                        )
                        or source_role != "immutable_source_attachment"
                        or extension not in expected_image_types
                        or media_type != expected_image_types[extension]
                        or encoding != "binary"
                        or not 0 < len(artifact_raw) <= 8 * 1024 * 1024
                    ):
                        raise ProcessingPluginError(
                            "capture_manifest_artifact_invalid"
                        )
                    self._capture_image_type(artifact_raw, extension)
        if session.get("subject") == "math" and isinstance(facts_value, Mapping):
            self._validate_math_capture_evidence(artifacts, facts_value)
        if (
            facts_artifact_count != 1
            or artifact_ids != sorted(artifact_ids)
            or len(artifact_ids) != len(set(artifact_ids))
            or not 1 <= len(artifact_ids) <= 64
            or artifact_ids != session.get("artifact_ids")
        ):
            raise ProcessingPluginError("capture_manifest_artifact_invalid")
        return copy.deepcopy(dict(manifest))

    def open_read_session(
        self,
        *,
        subject: str,
        capture_id: str,
        study_date: str,
        input_fingerprint: str,
        input_binding: Mapping[str, Any],
        capture_facts_sha256: str,
        capture_facts: Mapping[str, Any],
        capture_scene: str,
        capture_identity: Mapping[str, Any],
        capture_artifacts: Sequence[Mapping[str, Any]],
        captured_at: str,
        provider_schema_sha256: str,
        canonical_schema_sha256: str,
        validator_sha256: str,
        expected_batch_authority: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Bind authority without selecting or serializing any semantic evidence."""

        for value in (
            input_fingerprint,
            capture_facts_sha256,
            provider_schema_sha256,
            canonical_schema_sha256,
            validator_sha256,
        ):
            if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
                raise ProcessingPluginError("processing_binding_hash_invalid")
        if not isinstance(capture_facts, Mapping):
            raise ProcessingPluginError("capture_facts_invalid")
        snapshot = self._snapshot(subject)
        (
            capture_manifest,
            capture_manifest_sha256,
            capture_manifest_path,
            actual_capture_facts_sha256,
        ) = self._freeze_capture_artifacts(
            subject=subject,
            capture_id=capture_id,
            study_date=study_date,
            scene=capture_scene,
            captured_at=captured_at,
            identity=capture_identity,
            capture_facts=capture_facts,
            source_artifacts=capture_artifacts,
        )
        if capture_facts_sha256 != actual_capture_facts_sha256:
            raise ProcessingPluginError("capture_facts_hash_mismatch")
        artifact_ids = [str(row["artifact_id"]) for row in capture_manifest["artifacts"]]
        capture_core = {
            "schema_version": "capture_freeze_receipt_v2",
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "input_binding_sha256": _sha256_value(input_binding),
            "capture_facts_sha256": capture_facts_sha256,
            "capture_manifest_sha256": capture_manifest_sha256,
            "capture_manifest_ref": (
                "study-read-mcp-capture-manifest://sha256/"
                + capture_manifest_sha256
            ),
            "artifact_ids": artifact_ids,
            "frozen_scope": "current_capture_facts_only",
            "knowledge_library_frozen": False,
            "formal_write_count": 0,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        key = self._authority_key()
        capture_receipt = {
            **capture_core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "capture-freeze-receipt-v2",
                    "payload": capture_core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        capture_receipt_sha256 = _sha256_value(capture_receipt)
        capture_receipt_path = (
            self.runtime_root
            / "dispatch"
            / "capture-freeze-receipts"
            / "sha256"
            / capture_receipt_sha256[:2]
            / f"{capture_receipt_sha256}.json"
        )
        _atomic_publish(capture_receipt_path, capture_receipt)
        scope = {
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "capture_manifest_sha256": capture_manifest_sha256,
            "mode": "model_driven_subject_read_session",
            "host_semantic_prefetch": False,
        }
        scope_hash = _sha256_value(scope)
        route = self._route(
            snapshot=snapshot,
            scope_hash=scope_hash,
            route_id=f"bind-{subject}-{scope_hash[:24]}",
        )
        authority = self._call(
            subject,
            "authority_bundle",
            {
                "subjects": [subject],
                "capabilities": SUBJECT_CAPABILITIES[subject],
                "checks": ["exists", "parseable", "release_bound", "projection_bound"],
                "route": route,
            },
        )
        self._validate_envelope(
            authority,
            route=route,
            subject=subject,
            tool="authority_bundle",
            expected_server_release=snapshot.mcp_server_release,
        )
        subject_item = next(
            (
                row
                for row in authority.get("items", [])
                if isinstance(row, Mapping) and row.get("subject") == subject
            ),
            None,
        )
        base_release_id = authority.get("preprocessor_release")
        if (
            not isinstance(subject_item, Mapping)
            or subject_item.get("available") is not True
            or not isinstance(base_release_id, str)
            or not SHA256_RE.fullmatch(base_release_id)
        ):
            raise ProcessingPluginError("background_mcp_authority_binding_invalid")
        generation = subject_item.get("generation")
        fingerprint = subject_item.get("authority_fingerprint")
        adapter_release = subject_item.get("adapter_release")
        if (
            not isinstance(generation, str)
            or not isinstance(fingerprint, str)
            or not SHA256_RE.fullmatch(fingerprint)
            or adapter_release != snapshot.mcp_server_release
            or adapter_release != authority.get("server_release")
        ):
            raise ProcessingPluginError("background_mcp_authority_binding_invalid")
        explicit_authority = (
            dict(expected_batch_authority)
            if isinstance(expected_batch_authority, Mapping)
            else None
        )
        if expected_batch_authority is not None and explicit_authority is None:
            raise ProcessingPluginError("background_mcp_batch_authority_drift")
        environment_authority = {
            "generation": os.environ.get(
                "STUDY_PREPROCESS_EXPECTED_AUTHORITY_GENERATION"
            ),
            "authority_fingerprint": os.environ.get(
                "STUDY_PREPROCESS_EXPECTED_AUTHORITY_FINGERPRINT"
            ),
            "batch_id": os.environ.get("STUDY_PREPROCESS_EXPECTED_BATCH_ID"),
            "scan_snapshot_sha256": os.environ.get(
                "STUDY_PREPROCESS_EXPECTED_SCAN_SNAPSHOT_SHA256"
            ),
        }
        if explicit_authority is not None and any(
            value is not None for value in environment_authority.values()
        ) and explicit_authority != environment_authority:
            raise ProcessingPluginError("background_mcp_batch_authority_drift")
        authority_fence = explicit_authority or environment_authority
        expected_generation = authority_fence["generation"]
        expected_fingerprint = authority_fence["authority_fingerprint"]
        expected_batch_id = authority_fence["batch_id"]
        expected_scan_sha256 = authority_fence["scan_snapshot_sha256"]
        expected_values = (
            expected_generation,
            expected_fingerprint,
            expected_batch_id,
            expected_scan_sha256,
        )
        if any(value is not None for value in expected_values):
            if (
                not all(isinstance(value, str) and value for value in expected_values)
                or SHA256_RE.fullmatch(str(expected_fingerprint)) is None
                or SHA256_RE.fullmatch(str(expected_scan_sha256)) is None
                or generation != expected_generation
                or fingerprint != expected_fingerprint
            ):
                raise ProcessingPluginError("background_mcp_batch_authority_drift")
        mcp_hash = str(adapter_release).rsplit("+sha256.", 1)[-1]
        if not SHA256_RE.fullmatch(mcp_hash):
            raise ProcessingPluginError("background_mcp_release_hash_invalid")
        host_preflight_tool_call_count = 0
        if subject == "cs408":
            preflight_route = {
                **route,
                "route_request_id": f"preflight-cs408-{scope_hash[:20]}",
            }
            capture = input_binding.get("capture_id") or capture_id
            preflight = self._call(
                subject,
                "cs408_read_bundle",
                {
                    "queries": [{
                        "op": "curation_inventory",
                        "ids": [capture] if isinstance(capture, str) else [],
                        "study_date": study_date,
                        "fields": [],
                    }],
                    "expected_generation": generation,
                    "expected_release": adapter_release,
                    "expected_fingerprint": fingerprint,
                    "route": preflight_route,
                },
            )
            self._validate_envelope(
                preflight,
                route=preflight_route,
                subject="cs408",
                tool="cs408_read_bundle",
                expected_server_release=snapshot.mcp_server_release,
                require_subject=True,
            )
            if (
                preflight.get("generation") != generation
                or preflight.get("authority_fingerprint") != fingerprint
            ):
                raise ProcessingPluginError("background_mcp_generation_mismatch")
            host_preflight_tool_call_count = 1
        authority_snapshot = (
            self._freeze_authority_snapshot(
                subject=subject,
                generation=str(generation),
                authority_fingerprint=str(fingerprint),
                mcp_server_release=str(adapter_release),
            )
            if self.require_authority_snapshot
            else None
        )
        binding_core = {
            "schema_version": "processing_binding_v2",
            "base_release_id": base_release_id,
            "candidate_release_id": self.candidate_release_id,
            "plugin": {
                "id": snapshot.plugin_name,
                "version": snapshot.plugin_version,
                "sha256": snapshot.component_lock_sha256,
            },
            "skill": {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
            },
            "mcp": {
                "id": SUBJECT_MCP_SERVERS[subject],
                "version": adapter_release,
                "sha256": mcp_hash,
                "profile": "luna",
                "subject": subject,
            },
            "schemas": {
                "provider_sha256": provider_schema_sha256,
                "canonical_sha256": canonical_schema_sha256,
            },
            "validator_sha256": validator_sha256,
            "host_semantic_prefetch": False,
            "formal_write_count": 0,
        }
        processing_binding = {
            **binding_core,
            "binding_sha256": _sha256_value(binding_core),
        }
        session_seed = _sha256_value({
            "scope": scope_hash,
            "generation": generation,
            "authority": fingerprint,
            "binding": processing_binding["binding_sha256"],
            "authority_snapshot_manifest_sha256": (
                authority_snapshot["authority_snapshot_manifest_sha256"]
                if authority_snapshot is not None
                else None
            ),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        })
        session_core = {
            "schema_version": (
                "study-read-mcp-read-session.v4"
                if authority_snapshot is not None
                else "study-read-mcp-read-session.v2"
            ),
            "read_session_id": f"MCPRS-{subject.upper()}-{session_seed[:24].upper()}",
            "subject": subject,
            "candidate_release_id": self.candidate_release_id,
            "plugin_version": snapshot.plugin_version,
            "skill_id": snapshot.skill_id,
            "skill_version": snapshot.skill_version,
            "mcp_server_release": adapter_release,
            "generation": generation,
            "authority_fingerprint": fingerprint,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "formal_write_count": 0,
            "capture_id": capture_id,
            "capture_manifest_path": str(capture_manifest_path),
            "capture_manifest_sha256": capture_manifest_sha256,
            "artifact_ids": artifact_ids,
        }
        if authority_snapshot is not None:
            session_core.update({
                "authority_snapshot_manifest_path": authority_snapshot[
                    "authority_snapshot_manifest_path"
                ],
                "authority_snapshot_manifest_sha256": authority_snapshot[
                    "authority_snapshot_manifest_sha256"
                ],
                "authority_snapshot_root": authority_snapshot[
                    "authority_snapshot_root"
                ],
                "authority_snapshot_receipt_sha256": authority_snapshot[
                    "authority_snapshot_receipt_sha256"
                ],
            })
        session_manifest = {
            **session_core,
            "manifest_sha256": _sha256_value(session_core),
        }
        session_path = self._read_session_path(session_manifest["manifest_sha256"])
        _atomic_publish(session_path, session_manifest)
        if _load_object(session_path) != session_manifest:
            raise ProcessingPluginError("mcp_read_session_write_mismatch")
        receipt_core = {
            "schema_version": "mcp_read_session_receipt_v1",
            "phase": "opened",
            "subject": subject,
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "read_session_id": session_manifest["read_session_id"],
            "read_session_manifest_sha256": session_manifest["manifest_sha256"],
            "generation": generation,
            "authority_fingerprint": fingerprint,
            "host_semantic_prefetch": False,
            "host_prefetched_evidence_item_count": 0,
            "host_authority_binding_tool_call_count": 1,
            "host_preflight_tool_call_count": host_preflight_tool_call_count,
            "model_mcp_tool_call_count": 0,
            "provider_request_count": 0,
            "provider_request_count_status": "not_started",
            "stage_call_receipts": [],
            "pagination_coverage_complete": False,
            "failure_reason": None,
            "opened_receipt_sha256": None,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "formal_write_count": 0,
        }
        receipt = {
            **receipt_core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "mcp-read-session-receipt-v1",
                    "payload": receipt_core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        receipt_sha256 = _sha256_value(receipt)
        receipt_path = (
            self.runtime_root
            / "dispatch"
            / "mcp-read-session-receipts"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        _atomic_publish(receipt_path, receipt)
        result = {
            "capture_freeze_receipt": capture_receipt,
            "capture_freeze_receipt_sha256": capture_receipt_sha256,
            "capture_freeze_receipt_ref": (
                "study-intake-capture-freeze://sha256/"
                + capture_receipt_sha256
            ),
            "processing_binding": processing_binding,
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "mcp_read_session": {
                key: copy.deepcopy(value)
                for key, value in session_manifest.items()
                if key != "created_at"
            },
            "mcp_read_session_receipt": receipt,
            "mcp_read_session_receipt_sha256": receipt_sha256,
            "mcp_read_session_receipt_ref": (
                "study-intake-mcp-read-session://sha256/" + receipt_sha256
            ),
            "processing_skill": {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
                "shared_contract": snapshot.shared_contract_text,
                "skill": snapshot.skill_text,
            },
            "formal_write_count": 0,
        }
        if authority_snapshot is not None:
            result.update({
                "authority_snapshot_receipt": authority_snapshot[
                    "authority_snapshot_receipt"
                ],
                "authority_snapshot_receipt_sha256": authority_snapshot[
                    "authority_snapshot_receipt_sha256"
                ],
                "authority_snapshot_receipt_ref": authority_snapshot[
                    "authority_snapshot_receipt_ref"
                ],
            })
        return result

    def read_session_manifest_path(self, context: Mapping[str, Any]) -> Path:
        session = context.get("mcp_read_session")
        if not isinstance(session, Mapping):
            raise ProcessingPluginError("mcp_read_session_missing")
        digest = session.get("manifest_sha256")
        if not isinstance(digest, str):
            raise ProcessingPluginError("mcp_read_session_missing")
        path = self._read_session_path(digest)
        value = _load_object(path)
        manifest_core = {
            key: copy.deepcopy(nested)
            for key, nested in value.items()
            if key != "manifest_sha256"
        }
        expected_keys = {
                "schema_version",
                "read_session_id",
                "subject",
                "candidate_release_id",
                "plugin_version",
                "skill_id",
                "skill_version",
                "mcp_server_release",
                "generation",
                "authority_fingerprint",
                "created_at",
                "formal_write_count",
                "capture_id",
                "capture_manifest_path",
                "capture_manifest_sha256",
                "artifact_ids",
                "manifest_sha256",
        }
        if value.get("schema_version") in {
            "study-read-mcp-read-session.v3",
            "study-read-mcp-read-session.v4",
        }:
            expected_keys.update({
                "authority_snapshot_manifest_path",
                "authority_snapshot_manifest_sha256",
                "authority_snapshot_root",
                "authority_snapshot_receipt_sha256",
            })
        if (
            set(value) != expected_keys
            or value.get("schema_version") not in {
                "study-read-mcp-read-session.v2",
                "study-read-mcp-read-session.v3",
                "study-read-mcp-read-session.v4",
            }
            or value.get("manifest_sha256") != digest
            or _sha256_value(manifest_core) != digest
            or not _is_aware_iso_timestamp(value.get("created_at"))
        ):
            raise ProcessingPluginError("mcp_read_session_drift")
        for key, expected in session.items():
            if value.get(key) != expected:
                raise ProcessingPluginError("mcp_read_session_drift")
        self._validate_capture_manifest(value)
        return path

    def validate_read_session_context(
        self, *, subject: str, context: Mapping[str, Any]
    ) -> dict[str, Any]:
        snapshot = self._snapshot(subject)
        binding = context.get("processing_binding")
        session = context.get("mcp_read_session")
        receipt = context.get("mcp_read_session_receipt")
        capture_receipt = context.get("capture_freeze_receipt")
        skill = context.get("processing_skill")
        if not all(
            isinstance(value, Mapping)
            for value in (binding, session, receipt, capture_receipt, skill)
        ):
            raise ProcessingPluginError("mcp_read_session_context_invalid")
        binding_core = {key: value for key, value in binding.items() if key != "binding_sha256"}
        binding_sha256 = binding.get("binding_sha256")
        if (
            binding.get("schema_version") != "processing_binding_v2"
            or binding_sha256 != _sha256_value(binding_core)
            or context.get("processing_binding_sha256") != binding_sha256
            or binding.get("candidate_release_id") != self.candidate_release_id
            or binding.get("host_semantic_prefetch") is not False
            or binding.get("formal_write_count") != 0
            or binding.get("plugin") != {
                "id": snapshot.plugin_name,
                "version": snapshot.plugin_version,
                "sha256": snapshot.component_lock_sha256,
            }
            or binding.get("skill") != {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
            }
        ):
            raise ProcessingPluginError("mcp_read_session_binding_invalid")
        if skill != {
            "id": snapshot.skill_id,
            "version": snapshot.skill_version,
            "sha256": snapshot.skill_sha256,
            "shared_contract": snapshot.shared_contract_text,
            "skill": snapshot.skill_text,
        }:
            raise ProcessingPluginError("mcp_read_session_skill_invalid")
        capture_receipt_core = {
            key: value
            for key, value in capture_receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        authority_key = self._authority_key()
        capture_receipt_sha256 = _sha256_value(capture_receipt)
        expected_capture_receipt_keys = {
            "schema_version",
            "subject",
            "capture_id",
            "study_date",
            "input_fingerprint",
            "input_binding_sha256",
            "capture_facts_sha256",
            "capture_manifest_sha256",
            "capture_manifest_ref",
            "artifact_ids",
            "frozen_scope",
            "knowledge_library_frozen",
            "formal_write_count",
            "created_at",
            "hmac_key_id",
            "hmac_sha256",
        }
        if (
            set(capture_receipt) != expected_capture_receipt_keys
            or capture_receipt.get("schema_version")
            != "capture_freeze_receipt_v2"
            or capture_receipt.get("subject") != subject
            or capture_receipt.get("frozen_scope")
            != "current_capture_facts_only"
            or capture_receipt.get("knowledge_library_frozen") is not False
            or capture_receipt.get("formal_write_count") != 0
            or capture_receipt.get("capture_manifest_sha256")
            != session.get("capture_manifest_sha256")
            or capture_receipt.get("capture_manifest_ref")
            != "study-read-mcp-capture-manifest://sha256/"
            + str(session.get("capture_manifest_sha256") or "")
            or capture_receipt.get("artifact_ids") != session.get("artifact_ids")
            or context.get("capture_freeze_receipt_sha256")
            != capture_receipt_sha256
            or context.get("capture_freeze_receipt_ref")
            != "study-intake-capture-freeze://sha256/"
            + capture_receipt_sha256
            or capture_receipt.get("hmac_key_id")
            != _sha256_bytes(authority_key)
            or not hmac.compare_digest(
                str(capture_receipt.get("hmac_sha256") or ""),
                hmac.new(
                    authority_key,
                    _canonical_bytes({
                        "purpose": "capture-freeze-receipt-v2",
                        "payload": capture_receipt_core,
                    }),
                    hashlib.sha256,
                ).hexdigest(),
            )
        ):
            raise ProcessingPluginError("capture_freeze_receipt_invalid")
        if self.require_authority_snapshot:
            snapshot_receipt = context.get("authority_snapshot_receipt")
            snapshot_receipt_sha256 = context.get(
                "authority_snapshot_receipt_sha256"
            )
            snapshot_receipt_ref = context.get("authority_snapshot_receipt_ref")
            if not isinstance(snapshot_receipt, Mapping):
                raise ProcessingPluginError("mcp_authority_snapshot_receipt_missing")
            session_schema = session.get("schema_version")
            if session_schema not in {
                "study-read-mcp-read-session.v3",
                "study-read-mcp-read-session.v4",
            }:
                raise ProcessingPluginError("mcp_read_session_schema_invalid")
            successor_snapshot = (
                session_schema == "study-read-mcp-read-session.v4"
            )
            expected_snapshot_receipt_schema = (
                "mcp_authority_snapshot_receipt_v2"
                if successor_snapshot
                else "mcp_authority_snapshot_receipt_v1"
            )
            expected_snapshot_receipt_purpose = (
                "mcp-authority-snapshot-receipt-v2"
                if successor_snapshot
                else "mcp-authority-snapshot-receipt-v1"
            )
            expected_snapshot_schema = (
                "study-read-mcp-authority-snapshot.v2"
                if successor_snapshot
                else "study-read-mcp-authority-snapshot.v1"
            )
            snapshot_receipt_core = {
                key: value
                for key, value in snapshot_receipt.items()
                if key not in {"hmac_key_id", "hmac_sha256"}
            }
            expected_snapshot_keys = {
                "schema_version",
                "subject",
                "candidate_release_id",
                "mcp_server_release",
                "generation",
                "authority_fingerprint",
                "authority_snapshot_manifest_sha256",
                "file_count",
                "total_bytes",
                "formal_write_count",
                "created_at",
                "hmac_key_id",
                "hmac_sha256",
            }
            if (
                set(snapshot_receipt) != expected_snapshot_keys
                or snapshot_receipt.get("schema_version")
                != expected_snapshot_receipt_schema
                or snapshot_receipt.get("subject") != subject
                or snapshot_receipt.get("candidate_release_id")
                != self.candidate_release_id
                or snapshot_receipt.get("mcp_server_release")
                != session.get("mcp_server_release")
                or snapshot_receipt.get("generation")
                != session.get("generation")
                or snapshot_receipt.get("authority_fingerprint")
                != session.get("authority_fingerprint")
                or snapshot_receipt.get("authority_snapshot_manifest_sha256")
                != session.get("authority_snapshot_manifest_sha256")
                or snapshot_receipt.get("formal_write_count") != 0
                or not _is_aware_iso_timestamp(
                    snapshot_receipt.get("created_at")
                )
                or _sha256_value(snapshot_receipt) != snapshot_receipt_sha256
                or session.get("authority_snapshot_receipt_sha256")
                != snapshot_receipt_sha256
                or snapshot_receipt_ref
                != "study-intake-mcp-authority-snapshot://sha256/"
                + str(snapshot_receipt_sha256 or "")
                or snapshot_receipt.get("hmac_key_id")
                != _sha256_bytes(authority_key)
                or not hmac.compare_digest(
                    str(snapshot_receipt.get("hmac_sha256") or ""),
                    hmac.new(
                        authority_key,
                        _canonical_bytes({
                            "purpose": expected_snapshot_receipt_purpose,
                            "payload": snapshot_receipt_core,
                        }),
                        hashlib.sha256,
                    ).hexdigest(),
                )
            ):
                raise ProcessingPluginError("mcp_authority_snapshot_receipt_invalid")
            manifest_path = Path(
                str(session.get("authority_snapshot_manifest_path") or "")
            ).resolve()
            frozen_root = Path(
                str(session.get("authority_snapshot_root") or "")
            ).resolve()
            snapshot_root = (
                self.runtime_root / "private" / "mcp-authority-snapshots"
            ).resolve()
            try:
                manifest_path.relative_to(snapshot_root)
                frozen_root.relative_to(snapshot_root)
            except ValueError as exc:
                raise ProcessingPluginError(
                    "mcp_authority_snapshot_path_invalid"
                ) from exc
            if (
                not manifest_path.is_file()
                or _sha256_file(manifest_path)
                != session.get("authority_snapshot_manifest_sha256")
                or not frozen_root.is_dir()
            ):
                raise ProcessingPluginError("mcp_authority_snapshot_drift")
            snapshot_manifest = _load_object(manifest_path)
            if (
                snapshot_manifest.get("schema_version")
                != expected_snapshot_schema
                or snapshot_manifest.get("mcp_server_release")
                != session.get("mcp_server_release")
                or snapshot_manifest.get("formal_write_count") != 0
            ):
                raise ProcessingPluginError("mcp_authority_snapshot_drift")
        elif session.get("schema_version") != "study-read-mcp-read-session.v2":
            raise ProcessingPluginError("mcp_read_session_schema_invalid")
        path = self.read_session_manifest_path(context)
        manifest = _load_object(path)
        capture_manifest = self._validate_capture_manifest(manifest)
        capture_facts_row = next(
            (
                row
                for row in capture_manifest["artifacts"]
                if row.get("artifact_id") == "capture-facts"
            ),
            None,
        )
        if (
            manifest.get("subject") != subject
            or manifest.get("candidate_release_id") != self.candidate_release_id
            or manifest.get("mcp_server_release") != snapshot.mcp_server_release
            or manifest.get("skill_id") != snapshot.skill_id
            or manifest.get("formal_write_count") != 0
            or not isinstance(capture_facts_row, Mapping)
            or capture_facts_row.get("sha256")
            != capture_receipt.get("capture_facts_sha256")
            or receipt.get("schema_version") != "mcp_read_session_receipt_v1"
            or receipt.get("phase") != "opened"
            or receipt.get("subject") != subject
            or receipt.get("processing_binding_sha256") != binding_sha256
            or receipt.get("read_session_id") != manifest.get("read_session_id")
            or receipt.get("read_session_manifest_sha256")
            != manifest.get("manifest_sha256")
            or receipt.get("generation") != manifest.get("generation")
            or receipt.get("authority_fingerprint")
            != manifest.get("authority_fingerprint")
            or receipt.get("host_semantic_prefetch") is not False
            or receipt.get("host_prefetched_evidence_item_count") != 0
            or receipt.get("model_mcp_tool_call_count") != 0
            or receipt.get("provider_request_count") != 0
            or receipt.get("provider_request_count_status") != "not_started"
            or receipt.get("stage_call_receipts") != []
            or receipt.get("pagination_coverage_complete") is not False
            or receipt.get("failure_reason") is not None
            or receipt.get("opened_receipt_sha256") is not None
            or receipt.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("mcp_read_session_receipt_invalid")
        receipt_sha256 = _sha256_value(receipt)
        if (
            context.get("mcp_read_session_receipt_sha256") != receipt_sha256
            or context.get("mcp_read_session_receipt_ref")
            != "study-intake-mcp-read-session://sha256/" + receipt_sha256
        ):
            raise ProcessingPluginError("mcp_read_session_receipt_invalid")
        receipt_core = {
            key: value
            for key, value in receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        expected_hmac = hmac.new(
            key,
            _canonical_bytes({
                "purpose": "mcp-read-session-receipt-v1",
                "payload": receipt_core,
            }),
            hashlib.sha256,
        ).hexdigest()
        if (
            receipt.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(str(receipt.get("hmac_sha256") or ""), expected_hmac)
        ):
            raise ProcessingPluginError("mcp_read_session_hmac_invalid")
        return copy.deepcopy(dict(context))

    def sign_model_mcp_calls(
        self,
        *,
        subject: str,
        stage_name: str,
        context: Mapping[str, Any],
        calls: list[Mapping[str, Any]],
        transcript_sha256: str,
        failure_reason: str | None = None,
        attempt_counts: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        verified = self.validate_read_session_context(
            subject=subject, context=context
        )
        session = verified["mcp_read_session"]
        failed = failure_reason is not None
        counts = (
            dict(attempt_counts)
            if isinstance(attempt_counts, Mapping)
            else {
                "attempted_mcp_tool_call_count": len(calls),
                "successful_mcp_tool_call_count": len(calls),
                "grounding_mcp_tool_call_count": len(calls),
                "failed_mcp_tool_call_count": 0,
                "last_mcp_error_code": None,
            }
        )
        expected_count_keys = {
            "attempted_mcp_tool_call_count",
            "successful_mcp_tool_call_count",
            "grounding_mcp_tool_call_count",
            "failed_mcp_tool_call_count",
            "last_mcp_error_code",
        }
        attempted = counts.get("attempted_mcp_tool_call_count")
        successful = counts.get("successful_mcp_tool_call_count")
        grounding = counts.get("grounding_mcp_tool_call_count")
        failed_calls = counts.get("failed_mcp_tool_call_count")
        last_mcp_error_code = counts.get("last_mcp_error_code")
        expected_server = SUBJECT_MCP_SERVERS[subject]
        if (
            (not calls and not failed)
            or not SHA256_RE.fullmatch(transcript_sha256)
            or (
                failed
                and (
                    not isinstance(failure_reason, str)
                    or not failure_reason
                    or len(failure_reason) > 160
                )
            )
            or set(counts) != expected_count_keys
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in (attempted, successful, grounding, failed_calls)
            )
            or attempted != successful + failed_calls
            or grounding > successful
            or (failed_calls == 0) != (last_mcp_error_code is None)
            or (
                last_mcp_error_code is not None
                and (
                    not isinstance(last_mcp_error_code, str)
                    or not last_mcp_error_code
                    or len(last_mcp_error_code) > 96
                )
            )
            or (not failed and failed_calls != 0)
            or any(
                call.get("sequence") != index
                or call.get("server") != expected_server
                or call.get("tool") not in FOCUSED_MCP_TOOLS
                or not isinstance(call.get("arguments"), Mapping)
                or not SHA256_RE.fullmatch(str(call.get("arguments_sha256") or ""))
                or _sha256_json_value(call.get("arguments"))
                != call.get("arguments_sha256")
                or not isinstance(call.get("result"), Mapping)
                or not SHA256_RE.fullmatch(str(call.get("result_sha256") or ""))
                or _sha256_json_value(call.get("result"))
                != call.get("result_sha256")
                for index, call in enumerate(calls, start=1)
            )
        ):
            raise ProcessingPluginError("mcp_model_call_receipt_invalid")
        call_rows = []
        seen_arguments: set[tuple[str, str]] = set()
        for call in calls:
            arguments_sha256 = str(call["arguments_sha256"])
            read_identity = (str(call["tool"]), arguments_sha256)
            if read_identity in seen_arguments:
                raise ProcessingPluginError("mcp_model_call_duplicate_read")
            seen_arguments.add(read_identity)
            result = call.get("result")
            read_session = (
                result.get("read_session")
                if isinstance(result, Mapping)
                else None
            )
            route = (
                result.get("read_route")
                if isinstance(result, Mapping)
                else None
            )
            if (
                not isinstance(result, Mapping)
                or result.get("subject") != subject
                or result.get("generation") != session["generation"]
                or result.get("authority_fingerprint")
                != session["authority_fingerprint"]
                or not isinstance(read_session, Mapping)
                or read_session.get("read_session_id")
                != session["read_session_id"]
                or read_session.get("manifest_sha256")
                != session["manifest_sha256"]
                or read_session.get("capture_id") != session["capture_id"]
                or read_session.get("capture_manifest_sha256")
                != session["capture_manifest_sha256"]
                or read_session.get("artifact_ids") != session["artifact_ids"]
                or read_session.get("authority_snapshot_manifest_sha256")
                != session.get("authority_snapshot_manifest_sha256")
                or read_session.get("authority_snapshot_receipt_sha256")
                != session.get("authority_snapshot_receipt_sha256")
                or not isinstance(route, Mapping)
                or set(route)
                != {
                    "caller_skill_id",
                    "caller_skill_version",
                    "plugin_version",
                    "route_request_id",
                    "evidence_scope_hash",
                    "read_route",
                    "read_session_id",
                    "consumed_duplicate_read_count",
                }
                or route.get("caller_skill_id") != session["skill_id"]
                or route.get("caller_skill_version")
                != session["skill_version"]
                or route.get("plugin_version") != session["plugin_version"]
                or route.get("route_request_id")
                != session["read_session_id"]
                or route.get("evidence_scope_hash")
                != session["manifest_sha256"]
                or route.get("read_route") != "mcp_model_driven"
                or route.get("read_session_id")
                != session["read_session_id"]
                or route.get("consumed_duplicate_read_count") != 0
                or result.get("formal_write_count") != 0
            ):
                raise ProcessingPluginError("mcp_model_call_receipt_invalid")
            call_rows.append({
                "sequence": call["sequence"],
                "tool": call["tool"],
                "arguments": copy.deepcopy(call.get("arguments")),
                "arguments_sha256": arguments_sha256,
                "result_sha256": call["result_sha256"],
                "generation": result.get("generation"),
                "authority_fingerprint": result.get("authority_fingerprint"),
                "total_count": result.get("total_count"),
                "returned_count": result.get("returned_count"),
                "offset": result.get("offset"),
                "next_cursor": result.get("next_cursor"),
                "truncated": result.get("truncated"),
                "complete": result.get("complete"),
            })
        core = {
            "schema_version": "mcp_stage_call_receipt_v2",
            "phase": (
                "model_stage_failed" if failed else "model_stage_calls"
            ),
            "stage_name": stage_name,
            "subject": subject,
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "generation": session["generation"],
            "authority_fingerprint": session["authority_fingerprint"],
            "transcript_sha256": transcript_sha256,
            "calls": call_rows,
            "pagination_coverage_complete": not failed,
            "duplicate_read_count": 0,
            "consumed_terminal_duplicate_read_count": 0,
            "host_semantic_prefetch": False,
            "failure_reason": failure_reason,
            "semantic_stage_count": 1,
            "provider_request_count": int(attempted) + 1,
            "provider_request_count_status": (
                "derived_from_codex_tool_loop"
            ),
            "mcp_tool_call_count": len(call_rows),
            "attempted_mcp_tool_call_count": attempted,
            "successful_mcp_tool_call_count": successful,
            "grounding_mcp_tool_call_count": grounding,
            "failed_mcp_tool_call_count": failed_calls,
            "last_mcp_error_code": last_mcp_error_code,
            "model_call_count": 1,
            "formal_write_count": 0,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        key = self._authority_key()
        receipt = {
            **core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "mcp-read-session-model-calls-v2",
                    "payload": core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        digest = _sha256_value(receipt)
        path = (
            self.runtime_root
            / "dispatch"
            / "mcp-read-session-call-receipts"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        _atomic_publish(path, receipt)
        if _sha256_file(path) != digest:
            raise ProcessingPluginError("mcp_model_call_receipt_write_mismatch")
        return {
            "receipt": receipt,
            "receipt_sha256": digest,
            "receipt_ref": "study-intake-mcp-read-session-call://sha256/" + digest,
        }

    def sign_model_mcp_failure(
        self,
        *,
        subject: str,
        stage_name: str,
        context: Mapping[str, Any],
        transport_sha256: str,
        failure_reason: str,
        attempt_counts: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Sign a fail-closed stage even when canonical call parsing did not finish."""

        return self.sign_model_mcp_calls(
            subject=subject,
            stage_name=stage_name,
            context=context,
            calls=[],
            transcript_sha256=transport_sha256,
            failure_reason=failure_reason,
            attempt_counts=attempt_counts,
        )

    def validate_model_mcp_call_receipt(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
        receipt_sha256: str,
        expected_stage_name: str,
        expected_transcript_sha256: str,
        expected_mcp_tool_call_count: int,
        expected_provider_request_count: int,
        require_success: bool,
    ) -> dict[str, Any]:
        """Reopen, rehash and authenticate one model-stage call receipt."""

        verified = self.validate_read_session_context(
            subject=subject, context=context
        )
        path = self._model_call_receipt_path(receipt_sha256)
        try:
            receipt = _load_object(path)
        except ProcessingPluginError:
            raise
        except OSError as exc:
            raise ProcessingPluginError(
                "mcp_model_call_receipt_missing"
            ) from exc
        if _sha256_file(path) != receipt_sha256 or _sha256_value(
            receipt
        ) != receipt_sha256:
            raise ProcessingPluginError("mcp_model_call_receipt_invalid")
        session = verified["mcp_read_session"]
        core = {
            key: copy.deepcopy(value)
            for key, value in receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        receipt_schema = receipt.get("schema_version")
        historical_v1 = bool(
            receipt_schema == "mcp_stage_call_receipt_v1"
            and session.get("schema_version") == "study-read-mcp-read-session.v2"
            and not self.require_authority_snapshot
        )
        expected_hmac = hmac.new(
            key,
            _canonical_bytes({
                "purpose": (
                    "mcp-read-session-model-calls-v1"
                    if historical_v1
                    else "mcp-read-session-model-calls-v2"
                ),
                "payload": core,
            }),
            hashlib.sha256,
        ).hexdigest()
        calls = receipt.get("calls")
        success_contract = (
            receipt.get("phase") == "model_stage_calls"
            and receipt.get("pagination_coverage_complete") is True
            and receipt.get("failure_reason") is None
        )
        failure_contract = (
            receipt.get("phase") == "model_stage_failed"
            and receipt.get("pagination_coverage_complete") is False
            and isinstance(receipt.get("failure_reason"), str)
            and bool(receipt.get("failure_reason"))
        )
        attempt_contract = (
            require_success
            if historical_v1
            else (
                isinstance(receipt.get("attempted_mcp_tool_call_count"), int)
                and isinstance(
                    receipt.get("successful_mcp_tool_call_count"), int
                )
                and isinstance(
                    receipt.get("grounding_mcp_tool_call_count"), int
                )
                and isinstance(
                    receipt.get("failed_mcp_tool_call_count"), int
                )
                and receipt.get("attempted_mcp_tool_call_count")
                == receipt.get("successful_mcp_tool_call_count")
                + receipt.get("failed_mcp_tool_call_count")
                and receipt.get("grounding_mcp_tool_call_count")
                <= receipt.get("successful_mcp_tool_call_count")
                and (
                    receipt.get("failed_mcp_tool_call_count") == 0
                )
                == (receipt.get("last_mcp_error_code") is None)
            )
        )
        if (
            receipt_schema
            not in {"mcp_stage_call_receipt_v1", "mcp_stage_call_receipt_v2"}
            or (
                receipt_schema == "mcp_stage_call_receipt_v1"
                and not historical_v1
            )
            or receipt.get("stage_name") != expected_stage_name
            or receipt.get("subject") != subject
            or receipt.get("read_session_id") != session["read_session_id"]
            or receipt.get("read_session_manifest_sha256")
            != session["manifest_sha256"]
            or receipt.get("generation") != session["generation"]
            or receipt.get("authority_fingerprint")
            != session["authority_fingerprint"]
            or receipt.get("transcript_sha256")
            != expected_transcript_sha256
            or receipt.get("mcp_tool_call_count")
            != expected_mcp_tool_call_count
            or receipt.get("provider_request_count")
            != expected_provider_request_count
            or receipt.get("provider_request_count_status")
            != "derived_from_codex_tool_loop"
            or not attempt_contract
            or receipt.get("duplicate_read_count") != 0
            or receipt.get("consumed_terminal_duplicate_read_count") != 0
            or receipt.get("host_semantic_prefetch") is not False
            or receipt.get("semantic_stage_count") != 1
            or receipt.get("model_call_count") != 1
            or receipt.get("formal_write_count") != 0
            or not isinstance(calls, list)
            or len(calls) != expected_mcp_tool_call_count
            or receipt.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(
                str(receipt.get("hmac_sha256") or ""), expected_hmac
            )
            or (require_success and not success_contract)
            or (not require_success and not failure_contract)
        ):
            raise ProcessingPluginError("mcp_model_call_receipt_invalid")
        return copy.deepcopy(dict(receipt))

    def finalize_model_read_session(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
        stage_receipts: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Close one read session with the two successful, HMAC-bound model stages."""

        verified = self.validate_read_session_context(
            subject=subject, context=context
        )
        session = verified["mcp_read_session"]
        rows: list[dict[str, Any]] = []
        model_mcp_tool_call_count = 0
        provider_request_count = 0
        for stage_name in ("analysis", "critical_review"):
            receipt = stage_receipts.get(stage_name)
            if (
                not isinstance(receipt, Mapping)
                or receipt.get("status") != "ready"
                or receipt.get("read_session_id")
                != session["read_session_id"]
                or receipt.get("read_session_manifest_sha256")
                != session["manifest_sha256"]
                or not SHA256_RE.fullmatch(
                    str(receipt.get("mcp_call_receipt_sha256") or "")
                )
                or not SHA256_RE.fullmatch(
                    str(receipt.get("mcp_transcript_sha256") or "")
                )
                or not isinstance(receipt.get("mcp_tool_call_count"), int)
                or receipt.get("mcp_tool_call_count") < 1
                or not isinstance(receipt.get("provider_request_count"), int)
                or receipt.get("provider_request_count") < 2
                or receipt.get("pagination_coverage_complete") is not True
                or receipt.get("formal_write_count") != 0
            ):
                raise ProcessingPluginError(
                    "mcp_read_session_finalization_invalid"
                )
            self.validate_model_mcp_call_receipt(
                subject=subject,
                context=verified,
                receipt_sha256=str(receipt["mcp_call_receipt_sha256"]),
                expected_stage_name=str(
                    receipt.get("prompt_version") or stage_name
                ),
                expected_transcript_sha256=str(
                    receipt["mcp_transcript_sha256"]
                ),
                expected_mcp_tool_call_count=int(
                    receipt["mcp_tool_call_count"]
                ),
                expected_provider_request_count=int(
                    receipt["provider_request_count"]
                ),
                require_success=True,
            )
            rows.append({
                "stage": stage_name,
                "mcp_call_receipt_sha256": receipt[
                    "mcp_call_receipt_sha256"
                ],
                "mcp_transcript_sha256": receipt[
                    "mcp_transcript_sha256"
                ],
                "mcp_tool_call_count": receipt["mcp_tool_call_count"],
                "provider_request_count": receipt["provider_request_count"],
            })
            model_mcp_tool_call_count += int(
                receipt["mcp_tool_call_count"]
            )
            provider_request_count += int(receipt["provider_request_count"])
        core = {
            "schema_version": "mcp_read_session_receipt_v1",
            "phase": "complete",
            "subject": subject,
            "processing_binding_sha256": verified[
                "processing_binding_sha256"
            ],
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "generation": session["generation"],
            "authority_fingerprint": session["authority_fingerprint"],
            "host_semantic_prefetch": False,
            "host_prefetched_evidence_item_count": 0,
            "host_authority_binding_tool_call_count": 1,
            "host_preflight_tool_call_count": verified[
                "mcp_read_session_receipt"
            ]["host_preflight_tool_call_count"],
            "model_mcp_tool_call_count": model_mcp_tool_call_count,
            "provider_request_count": provider_request_count,
            "provider_request_count_status": (
                "derived_from_codex_tool_loop"
            ),
            "stage_call_receipts": rows,
            "pagination_coverage_complete": True,
            "failure_reason": None,
            "opened_receipt_sha256": verified[
                "mcp_read_session_receipt_sha256"
            ],
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "formal_write_count": 0,
        }
        key = self._authority_key()
        receipt = {
            **core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes({
                    "purpose": "mcp-read-session-receipt-v1-final",
                    "payload": core,
                }),
                hashlib.sha256,
            ).hexdigest(),
        }
        digest = _sha256_value(receipt)
        path = (
            self.runtime_root
            / "dispatch"
            / "mcp-read-session-receipts"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        _atomic_publish(path, receipt)
        finalized = {
            "receipt": receipt,
            "receipt_sha256": digest,
            "receipt_ref": "study-intake-mcp-read-session://sha256/"
            + digest,
        }
        self.validate_final_model_read_session(
            subject=subject,
            context=verified,
            finalized=finalized,
        )
        return finalized

    def validate_final_model_read_session(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
        finalized: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Verify the final receipt hash, HMAC and immutable session binding."""

        verified = self.validate_read_session_context(
            subject=subject, context=context
        )
        receipt = finalized.get("receipt")
        digest = finalized.get("receipt_sha256")
        if (
            not isinstance(receipt, Mapping)
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
            or _sha256_value(receipt) != digest
            or finalized.get("receipt_ref")
            != "study-intake-mcp-read-session://sha256/" + digest
        ):
            raise ProcessingPluginError(
                "mcp_read_session_final_receipt_invalid"
            )
        session = verified["mcp_read_session"]
        core = {
            key: copy.deepcopy(value)
            for key, value in receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        expected_hmac = hmac.new(
            key,
            _canonical_bytes({
                "purpose": "mcp-read-session-receipt-v1-final",
                "payload": core,
            }),
            hashlib.sha256,
        ).hexdigest()
        stage_rows = receipt.get("stage_call_receipts")
        expected_receipt_keys = {
            "schema_version",
            "phase",
            "subject",
            "processing_binding_sha256",
            "read_session_id",
            "read_session_manifest_sha256",
            "generation",
            "authority_fingerprint",
            "host_semantic_prefetch",
            "host_prefetched_evidence_item_count",
            "host_authority_binding_tool_call_count",
            "host_preflight_tool_call_count",
            "model_mcp_tool_call_count",
            "provider_request_count",
            "provider_request_count_status",
            "stage_call_receipts",
            "pagination_coverage_complete",
            "failure_reason",
            "opened_receipt_sha256",
            "created_at",
            "formal_write_count",
            "hmac_key_id",
            "hmac_sha256",
        }
        expected_stage_row_keys = {
            "stage",
            "mcp_call_receipt_sha256",
            "mcp_transcript_sha256",
            "mcp_tool_call_count",
            "provider_request_count",
        }
        structurally_valid_stage_rows = (
            isinstance(stage_rows, list)
            and len(stage_rows) == 2
            and all(
                isinstance(row, Mapping)
                and set(row) == expected_stage_row_keys
                and SHA256_RE.fullmatch(
                    str(row.get("mcp_call_receipt_sha256") or "")
                )
                is not None
                and SHA256_RE.fullmatch(
                    str(row.get("mcp_transcript_sha256") or "")
                )
                is not None
                and isinstance(row.get("mcp_tool_call_count"), int)
                and not isinstance(row.get("mcp_tool_call_count"), bool)
                and row.get("mcp_tool_call_count") >= 1
                and isinstance(row.get("provider_request_count"), int)
                and not isinstance(row.get("provider_request_count"), bool)
                and row.get("provider_request_count")
                == row.get("mcp_tool_call_count") + 1
                for row in stage_rows
            )
        )
        expected_model_calls = (
            sum(int(row["mcp_tool_call_count"]) for row in stage_rows)
            if structurally_valid_stage_rows
            else -1
        )
        expected_provider_requests = (
            sum(int(row["provider_request_count"]) for row in stage_rows)
            if structurally_valid_stage_rows
            else -1
        )
        if (
            set(receipt) != expected_receipt_keys
            or receipt.get("schema_version") != "mcp_read_session_receipt_v1"
            or receipt.get("phase") != "complete"
            or receipt.get("subject") != subject
            or receipt.get("processing_binding_sha256")
            != verified["processing_binding_sha256"]
            or receipt.get("read_session_id") != session["read_session_id"]
            or receipt.get("read_session_manifest_sha256")
            != session["manifest_sha256"]
            or receipt.get("generation") != session["generation"]
            or receipt.get("authority_fingerprint")
            != session["authority_fingerprint"]
            or receipt.get("opened_receipt_sha256")
            != verified["mcp_read_session_receipt_sha256"]
            or receipt.get("host_semantic_prefetch") is not False
            or receipt.get("host_prefetched_evidence_item_count") != 0
            or receipt.get("host_authority_binding_tool_call_count") != 1
            or receipt.get("host_preflight_tool_call_count")
            != verified["mcp_read_session_receipt"].get(
                "host_preflight_tool_call_count"
            )
            or receipt.get("model_mcp_tool_call_count")
            != expected_model_calls
            or receipt.get("provider_request_count")
            != expected_provider_requests
            or receipt.get("provider_request_count_status")
            != "derived_from_codex_tool_loop"
            or receipt.get("pagination_coverage_complete") is not True
            or receipt.get("failure_reason") is not None
            or receipt.get("formal_write_count") != 0
            or receipt.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(
                str(receipt.get("hmac_sha256") or ""), expected_hmac
            )
            or not structurally_valid_stage_rows
            or [
                row.get("stage") if isinstance(row, Mapping) else None
                for row in stage_rows
            ] != ["analysis", "critical_review"]
        ):
            raise ProcessingPluginError(
                "mcp_read_session_final_receipt_invalid"
            )
        return dict(receipt)

    def validate_persisted_model_mcp_stage(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
        stage: str,
        stage_receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reopen one persisted English stage before later lifecycle writes."""

        if subject != "english" or stage not in {"analysis", "critical_review"}:
            raise ProcessingPluginError("mcp_stage_transcript_invalid")
        grounding = stage_receipt.get("mcp_grounding_manifest")
        grounding_items = (
            grounding.get("items") if isinstance(grounding, Mapping) else None
        )
        grounding_refs = (
            [
                row.get("evidence_ref")
                for row in grounding_items
                if isinstance(row, Mapping)
            ]
            if isinstance(grounding_items, list)
            else []
        )
        publication_row = {
            "transcript_sha256": stage_receipt.get("mcp_transcript_sha256"),
            "transcript_ref": stage_receipt.get("mcp_transcript_ref"),
            "call_receipt_sha256": stage_receipt.get(
                "mcp_call_receipt_sha256"
            ),
            "call_receipt_ref": stage_receipt.get("mcp_call_receipt_ref"),
            "grounding_manifest_sha256": stage_receipt.get(
                "mcp_grounding_manifest_sha256"
            ),
            "grounding_refs": grounding_refs,
            "provider_request_count": stage_receipt.get(
                "provider_request_count"
            ),
            "mcp_tool_call_count": stage_receipt.get("mcp_tool_call_count"),
        }
        transcript, call_receipt = self._reopen_stage_transcript(
            subject=subject,
            context=context,
            stage=stage,
            stage_receipt=stage_receipt,
            publication_row=publication_row,
        )
        return {
            "transcript": transcript,
            "call_receipt": call_receipt,
        }

    def _reopen_stage_transcript(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
        stage: str,
        stage_receipt: Mapping[str, Any],
        publication_row: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Reopen one full transcript and bind it to its HMAC call receipt."""

        session = context["mcp_read_session"]
        binding = context["processing_binding"]
        grounding = stage_receipt.get("mcp_grounding_manifest")
        grounding_items = (
            grounding.get("items") if isinstance(grounding, Mapping) else None
        )
        grounding_refs = (
            [
                row.get("evidence_ref")
                for row in grounding_items
                if isinstance(row, Mapping)
            ]
            if isinstance(grounding_items, list)
            else []
        )
        transcript_sha256 = stage_receipt.get("mcp_transcript_sha256")
        call_receipt_sha256 = stage_receipt.get("mcp_call_receipt_sha256")
        prompt_version = stage_receipt.get("prompt_version")
        expected_publication_row = {
            "transcript_sha256": transcript_sha256,
            "transcript_ref": stage_receipt.get("mcp_transcript_ref"),
            "call_receipt_sha256": call_receipt_sha256,
            "call_receipt_ref": stage_receipt.get("mcp_call_receipt_ref"),
            "grounding_manifest_sha256": stage_receipt.get(
                "mcp_grounding_manifest_sha256"
            ),
            "grounding_refs": grounding_refs,
            "provider_request_count": stage_receipt.get(
                "provider_request_count"
            ),
            "mcp_tool_call_count": stage_receipt.get("mcp_tool_call_count"),
        }
        grounding_core = (
            {
                key: copy.deepcopy(value)
                for key, value in grounding.items()
                if key != "manifest_sha256"
            }
            if isinstance(grounding, Mapping)
            else {}
        )
        if (
            stage not in {"analysis", "critical_review"}
            or stage_receipt.get("status") != "ready"
            or not isinstance(prompt_version, str)
            or not prompt_version
            or stage_receipt.get("processing_binding") != binding
            or stage_receipt.get("processing_binding_sha256")
            != context["processing_binding_sha256"]
            or stage_receipt.get("capture_freeze_receipt_sha256")
            != context["capture_freeze_receipt_sha256"]
            or stage_receipt.get("capture_freeze_receipt_ref")
            != context["capture_freeze_receipt_ref"]
            or stage_receipt.get("mcp_read_session_receipt_sha256")
            != context["mcp_read_session_receipt_sha256"]
            or stage_receipt.get("mcp_read_session_receipt_ref")
            != context["mcp_read_session_receipt_ref"]
            or stage_receipt.get("read_session_id")
            != session["read_session_id"]
            or stage_receipt.get("read_session_manifest_sha256")
            != session["manifest_sha256"]
            or stage_receipt.get("evidence_generation")
            != session["generation"]
            or stage_receipt.get("evidence_authority_fingerprint")
            != session["authority_fingerprint"]
            or stage_receipt.get("host_semantic_prefetch") is not False
            or stage_receipt.get("pagination_coverage_complete") is not True
            or stage_receipt.get("consumed_terminal_duplicate_read_count") != 0
            or stage_receipt.get("formal_write_count") != 0
            or not isinstance(transcript_sha256, str)
            or SHA256_RE.fullmatch(transcript_sha256) is None
            or stage_receipt.get("mcp_transcript_ref")
            != "study-intake-mcp-stage-transcript://sha256/"
            + transcript_sha256
            or not isinstance(call_receipt_sha256, str)
            or SHA256_RE.fullmatch(call_receipt_sha256) is None
            or stage_receipt.get("mcp_call_receipt_ref")
            != "study-intake-mcp-read-session-call://sha256/"
            + call_receipt_sha256
            or not isinstance(grounding, Mapping)
            or not isinstance(grounding_items, list)
            or not grounding_refs
            or len(grounding_refs) != len(grounding_items)
            or any(
                not isinstance(value, str)
                or not value.startswith(f"mcp-item:{subject}:")
                for value in grounding_refs
            )
            or len(set(grounding_refs)) != len(grounding_refs)
            or stage_receipt.get("mcp_grounding_manifest_sha256")
            != grounding.get("manifest_sha256")
            or grounding.get("manifest_sha256")
            != _sha256_json_value(grounding_core)
            or dict(publication_row) != expected_publication_row
        ):
            raise ProcessingPluginError("mcp_stage_publication_invalid")

        transcript_path = self._stage_transcript_path(transcript_sha256)
        transcript = self._load_content_addressed_object(
            transcript_path,
            transcript_sha256,
            error_code="mcp_stage_transcript_invalid",
        )
        calls = transcript.get("calls")
        coverage = transcript.get("coverage")
        expected_transcript_stage_name = {
            ("english", "analysis"): "english_analysis",
            (
                "english",
                "critical_review",
            ): "english_critical_review",
        }.get((subject, stage), prompt_version)
        expected_transcript_keys = {
            "schema_version",
            "stage_name",
            "subject",
            "read_session_id",
            "read_session_manifest_sha256",
            "generation",
            "authority_fingerprint",
            "calls",
            "coverage",
            "semantic_stage_count",
            "provider_request_count",
            "mcp_tool_call_count",
            "model_call_count",
            "formal_write_count",
        }
        expected_coverage = {
            "call_count": len(calls) if isinstance(calls, list) else -1,
            "all_returned_pages_consumed": True,
            "unresolved_next_cursors": [],
            "duplicate_argument_count": 0,
            "host_semantic_prefetch": False,
        }
        if (
            set(transcript) != expected_transcript_keys
            or transcript.get("schema_version")
            != "model-driven-mcp-stage-transcript-v1"
            or transcript.get("stage_name") != expected_transcript_stage_name
            or transcript.get("subject") != subject
            or transcript.get("read_session_id") != session["read_session_id"]
            or transcript.get("read_session_manifest_sha256")
            != session["manifest_sha256"]
            or transcript.get("generation") != session["generation"]
            or transcript.get("authority_fingerprint")
            != session["authority_fingerprint"]
            or not isinstance(calls, list)
            or not calls
            or coverage != expected_coverage
            or transcript.get("semantic_stage_count") != 1
            or transcript.get("provider_request_count") != len(calls) + 1
            or transcript.get("mcp_tool_call_count") != len(calls)
            or transcript.get("model_call_count") != 1
            or transcript.get("formal_write_count") != 0
            or transcript.get("provider_request_count")
            != stage_receipt.get("provider_request_count")
            or transcript.get("mcp_tool_call_count")
            != stage_receipt.get("mcp_tool_call_count")
        ):
            raise ProcessingPluginError("mcp_stage_transcript_invalid")

        verified_call_receipt = self.validate_model_mcp_call_receipt(
            subject=subject,
            context=context,
            receipt_sha256=call_receipt_sha256,
            expected_stage_name=prompt_version,
            expected_transcript_sha256=transcript_sha256,
            expected_mcp_tool_call_count=len(calls),
            expected_provider_request_count=len(calls) + 1,
            require_success=True,
        )
        receipt_calls = verified_call_receipt.get("calls")
        expected_server = SUBJECT_MCP_SERVERS[subject]
        if not isinstance(receipt_calls, list) or len(receipt_calls) != len(calls):
            raise ProcessingPluginError("mcp_stage_transcript_invalid")
        seen_argument_hashes: set[tuple[str, str]] = set()
        expected_receipt_call_keys = {
            "sequence",
            "tool",
            "arguments",
            "arguments_sha256",
            "result_sha256",
            "generation",
            "authority_fingerprint",
            "total_count",
            "returned_count",
            "offset",
            "next_cursor",
            "truncated",
            "complete",
        }
        for index, (call, receipt_call) in enumerate(
            zip(calls, receipt_calls), start=1
        ):
            arguments = call.get("arguments") if isinstance(call, Mapping) else None
            result = call.get("result") if isinstance(call, Mapping) else None
            result_session = (
                result.get("read_session")
                if isinstance(result, Mapping)
                else None
            )
            items = result.get("items") if isinstance(result, Mapping) else None
            route = result.get("read_route") if isinstance(result, Mapping) else None
            expected_receipt_call = (
                {
                    "sequence": index,
                    "tool": call.get("tool"),
                    "arguments": copy.deepcopy(arguments),
                    "arguments_sha256": call.get("arguments_sha256"),
                    "result_sha256": call.get("result_sha256"),
                    "generation": result.get("generation"),
                    "authority_fingerprint": result.get(
                        "authority_fingerprint"
                    ),
                    "total_count": result.get("total_count"),
                    "returned_count": result.get("returned_count"),
                    "offset": result.get("offset"),
                    "next_cursor": result.get("next_cursor"),
                    "truncated": result.get("truncated"),
                    "complete": result.get("complete"),
                }
                if isinstance(call, Mapping) and isinstance(result, Mapping)
                else {}
            )
            if (
                not isinstance(call, Mapping)
                or set(call)
                != {
                    "sequence",
                    "server",
                    "tool",
                    "arguments",
                    "arguments_sha256",
                    "result",
                    "result_sha256",
                }
                or call.get("sequence") != index
                or call.get("server") != expected_server
                or call.get("tool") not in FOCUSED_MCP_TOOLS
                or not isinstance(arguments, Mapping)
                or call.get("arguments_sha256")
                != _sha256_json_value(arguments)
                or (
                    str(call.get("tool")),
                    str(call.get("arguments_sha256")),
                )
                in seen_argument_hashes
                or not isinstance(result, Mapping)
                or call.get("result_sha256") != _sha256_json_value(result)
                or result.get("ok") is not True
                or result.get("schema_version") != "study-read-mcp.v3"
                or result.get("profile") != "luna"
                or result.get("subject") != subject
                or result.get("server_release") != session["mcp_server_release"]
                or result.get("adapter_release") != session["mcp_server_release"]
                or result.get("generation") != session["generation"]
                or result.get("authority_fingerprint")
                != session["authority_fingerprint"]
                or result.get("formal_write_count") != 0
                or result.get("model_call_count") != 0
                or result.get("mcp_tool_call_count") != 1
                or not isinstance(result_session, Mapping)
                or result_session.get("read_session_id")
                != session["read_session_id"]
                or result_session.get("manifest_sha256")
                != session["manifest_sha256"]
                or result_session.get("capture_id") != session["capture_id"]
                or result_session.get("capture_manifest_sha256")
                != session["capture_manifest_sha256"]
                or result_session.get("artifact_ids") != session["artifact_ids"]
                or result_session.get("candidate_release_id")
                != session["candidate_release_id"]
                or result_session.get("subject") != subject
                or result_session.get("generation") != session["generation"]
                or result_session.get("authority_fingerprint")
                != session["authority_fingerprint"]
                or result_session.get("skill_id") != session["skill_id"]
                or result_session.get("skill_version")
                != session["skill_version"]
                or result_session.get("formal_write_count") != 0
                or not isinstance(route, Mapping)
                or route.get("caller_skill_id") != session["skill_id"]
                or route.get("caller_skill_version")
                != session["skill_version"]
                or route.get("plugin_version") != session["plugin_version"]
                or set(route)
                != {
                    "caller_skill_id",
                    "caller_skill_version",
                    "plugin_version",
                    "route_request_id",
                    "evidence_scope_hash",
                    "read_route",
                    "read_session_id",
                    "consumed_duplicate_read_count",
                }
                or route.get("route_request_id")
                != session["read_session_id"]
                or route.get("evidence_scope_hash")
                != session["manifest_sha256"]
                or route.get("read_route") != "mcp_model_driven"
                or route.get("read_session_id")
                != session["read_session_id"]
                or route.get("consumed_duplicate_read_count") != 0
                or not isinstance(items, list)
                or any(
                    not isinstance(item, Mapping)
                    or not isinstance(item.get("collection"), str)
                    or not item.get("collection")
                    or not isinstance(item.get("stable_id"), str)
                    or not isinstance(item.get("source_hash"), str)
                    or SHA256_RE.fullmatch(str(item.get("source_hash") or ""))
                    is None
                    or not isinstance(item.get("data_role"), str)
                    or not isinstance(item.get("evidence_ref"), str)
                    or not str(item.get("evidence_ref")).startswith(
                        f"mcp-item:{subject}:"
                    )
                    for item in items
                )
                or not isinstance(receipt_call, Mapping)
                or set(receipt_call) != expected_receipt_call_keys
                or dict(receipt_call) != expected_receipt_call
            ):
                raise ProcessingPluginError("mcp_stage_transcript_invalid")
            seen_argument_hashes.add(
                (str(call["tool"]), str(call["arguments_sha256"]))
            )
        return copy.deepcopy(dict(transcript)), verified_call_receipt

    def reopen_published_read_session(
        self,
        *,
        subject: str,
        publication: Mapping[str, Any],
        stage_receipts: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reopen every persisted read artifact before consuming a package."""

        if subject not in SUBJECTS or set(stage_receipts) != {
            "analysis",
            "critical_review",
            "read_session",
        }:
            raise ProcessingPluginError("mcp_published_read_session_invalid")
        inline_stage_receipts = publication.get("stage_receipts")
        if (
            inline_stage_receipts is not None
            and inline_stage_receipts != stage_receipts
        ):
            raise ProcessingPluginError("mcp_published_read_session_invalid")
        analysis = stage_receipts.get("analysis")
        critical = stage_receipts.get("critical_review")
        final_stage = stage_receipts.get("read_session")
        binding = publication.get("processing_binding")
        transcript_rows = publication.get("mcp_stage_transcripts")
        if (
            not isinstance(analysis, Mapping)
            or not isinstance(critical, Mapping)
            or not isinstance(final_stage, Mapping)
            or not isinstance(binding, Mapping)
            or not isinstance(transcript_rows, Mapping)
            or set(transcript_rows) != {"analysis", "critical_review"}
            or analysis.get("processing_binding") != binding
            or critical.get("processing_binding") != binding
            or publication.get("processing_binding_sha256")
            != binding.get("binding_sha256")
            or publication.get("processing_binding_sha256")
            != analysis.get("processing_binding_sha256")
            or publication.get("processing_binding_sha256")
            != critical.get("processing_binding_sha256")
        ):
            raise ProcessingPluginError("mcp_published_read_session_invalid")

        capture_sha256 = publication.get("capture_freeze_receipt_sha256")
        manifest_sha256 = publication.get("read_session_manifest_sha256")
        final_sha256 = publication.get("mcp_read_session_receipt_sha256")
        opened_sha256 = analysis.get("mcp_read_session_receipt_sha256")
        if (
            not all(
                isinstance(value, str) and SHA256_RE.fullmatch(value)
                for value in (
                    capture_sha256,
                    manifest_sha256,
                    final_sha256,
                    opened_sha256,
                )
            )
            or publication.get("capture_freeze_receipt_ref")
            != "study-intake-capture-freeze://sha256/" + str(capture_sha256)
            or publication.get("mcp_read_session_receipt_ref")
            != "study-intake-mcp-read-session://sha256/" + str(final_sha256)
            or analysis.get("mcp_read_session_receipt_ref")
            != "study-intake-mcp-read-session://sha256/" + str(opened_sha256)
            or critical.get("mcp_read_session_receipt_sha256") != opened_sha256
            or critical.get("mcp_read_session_receipt_ref")
            != analysis.get("mcp_read_session_receipt_ref")
            or publication.get("capture_freeze_receipt_sha256")
            != analysis.get("capture_freeze_receipt_sha256")
            or publication.get("capture_freeze_receipt_sha256")
            != critical.get("capture_freeze_receipt_sha256")
            or publication.get("capture_freeze_receipt_ref")
            != analysis.get("capture_freeze_receipt_ref")
            or publication.get("capture_freeze_receipt_ref")
            != critical.get("capture_freeze_receipt_ref")
        ):
            raise ProcessingPluginError("mcp_published_read_session_invalid")

        capture_receipt = self._load_content_addressed_object(
            self._capture_freeze_receipt_path(str(capture_sha256)),
            str(capture_sha256),
            error_code="capture_freeze_receipt_invalid",
        )
        if _sha256_value(capture_receipt) != capture_sha256:
            raise ProcessingPluginError("capture_freeze_receipt_invalid")
        opened_receipt = self._load_content_addressed_object(
            self._read_session_receipt_path(str(opened_sha256)),
            str(opened_sha256),
            error_code="mcp_read_session_receipt_invalid",
        )
        if _sha256_value(opened_receipt) != opened_sha256:
            raise ProcessingPluginError("mcp_read_session_receipt_invalid")
        manifest_path = self._read_session_path(str(manifest_sha256))
        try:
            manifest = _load_object(manifest_path)
        except ProcessingPluginError as exc:
            raise ProcessingPluginError("mcp_read_session_drift") from exc
        manifest_core = {
            key: copy.deepcopy(value)
            for key, value in manifest.items()
            if key != "manifest_sha256"
        }
        if (
            manifest.get("manifest_sha256") != manifest_sha256
            or _sha256_value(manifest_core) != manifest_sha256
        ):
            raise ProcessingPluginError("mcp_read_session_drift")

        authority_snapshot_receipt = None
        authority_snapshot_receipt_sha256 = manifest.get(
            "authority_snapshot_receipt_sha256"
        )
        if self.require_authority_snapshot:
            if (
                not isinstance(authority_snapshot_receipt_sha256, str)
                or SHA256_RE.fullmatch(authority_snapshot_receipt_sha256) is None
            ):
                raise ProcessingPluginError(
                    "mcp_authority_snapshot_receipt_invalid"
                )
            authority_snapshot_receipt = self._load_content_addressed_object(
                self._authority_snapshot_receipt_path(
                    authority_snapshot_receipt_sha256
                ),
                authority_snapshot_receipt_sha256,
                error_code="mcp_authority_snapshot_receipt_invalid",
            )

        snapshot = self._snapshot(subject)
        context = {
            "capture_freeze_receipt": capture_receipt,
            "capture_freeze_receipt_sha256": capture_sha256,
            "capture_freeze_receipt_ref": publication.get(
                "capture_freeze_receipt_ref"
            ),
            "processing_binding": copy.deepcopy(dict(binding)),
            "processing_binding_sha256": publication.get(
                "processing_binding_sha256"
            ),
            "mcp_read_session": {
                key: copy.deepcopy(value)
                for key, value in manifest.items()
                if key != "created_at"
            },
            "mcp_read_session_receipt": opened_receipt,
            "mcp_read_session_receipt_sha256": opened_sha256,
            "mcp_read_session_receipt_ref": analysis.get(
                "mcp_read_session_receipt_ref"
            ),
            "processing_skill": {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
                "shared_contract": snapshot.shared_contract_text,
                "skill": snapshot.skill_text,
            },
            "formal_write_count": 0,
        }
        if authority_snapshot_receipt is not None:
            context.update({
                "authority_snapshot_receipt": authority_snapshot_receipt,
                "authority_snapshot_receipt_sha256": (
                    authority_snapshot_receipt_sha256
                ),
                "authority_snapshot_receipt_ref": (
                    "study-intake-mcp-authority-snapshot://sha256/"
                    + authority_snapshot_receipt_sha256
                ),
            })
        context = self.validate_read_session_context(
            subject=subject, context=context
        )
        session = context["mcp_read_session"]
        if (
            publication.get("read_session_id") != session["read_session_id"]
            or publication.get("read_session_manifest_sha256")
            != session["manifest_sha256"]
            or publication.get("evidence_generation") != session["generation"]
            or publication.get("evidence_authority_fingerprint")
            != session["authority_fingerprint"]
            or publication.get("host_semantic_prefetch") is not False
            or publication.get("consumed_terminal_duplicate_read_count") != 0
            or publication.get("semantic_stage_count") != 2
            or publication.get("model_call_count") != 2
        ):
            raise ProcessingPluginError("mcp_published_read_session_invalid")

        final_receipt = self._load_content_addressed_object(
            self._read_session_receipt_path(str(final_sha256)),
            str(final_sha256),
            error_code="mcp_read_session_final_receipt_invalid",
        )
        if (
            _sha256_value(final_receipt) != final_sha256
            or set(final_stage)
            != {
                "status",
                "receipt",
                "receipt_sha256",
                "receipt_ref",
                "model_mcp_tool_call_count",
                "provider_request_count",
                "provider_request_count_status",
                "pagination_coverage_complete",
                "formal_write_count",
            }
            or final_stage.get("status") != "complete"
            or final_stage.get("receipt") != final_receipt
            or final_stage.get("receipt_sha256") != final_sha256
            or final_stage.get("receipt_ref")
            != publication.get("mcp_read_session_receipt_ref")
            or final_stage.get("pagination_coverage_complete") is not True
            or final_stage.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("mcp_read_session_final_receipt_invalid")
        self.validate_final_model_read_session(
            subject=subject,
            context=context,
            finalized={
                "receipt": final_receipt,
                "receipt_sha256": final_sha256,
                "receipt_ref": publication.get(
                    "mcp_read_session_receipt_ref"
                ),
            },
        )

        reopened_transcripts: dict[str, dict[str, Any]] = {}
        verified_call_receipts: dict[str, dict[str, Any]] = {}
        for stage, stage_receipt in (
            ("analysis", analysis),
            ("critical_review", critical),
        ):
            row = transcript_rows.get(stage)
            if not isinstance(row, Mapping):
                raise ProcessingPluginError("mcp_stage_publication_invalid")
            transcript, call_receipt = self._reopen_stage_transcript(
                subject=subject,
                context=context,
                stage=stage,
                stage_receipt=stage_receipt,
                publication_row=row,
            )
            reopened_transcripts[stage] = transcript
            verified_call_receipts[stage] = call_receipt

        expected_final_rows = [
            {
                "stage": stage,
                "mcp_call_receipt_sha256": stage_receipts[stage][
                    "mcp_call_receipt_sha256"
                ],
                "mcp_transcript_sha256": stage_receipts[stage][
                    "mcp_transcript_sha256"
                ],
                "mcp_tool_call_count": len(
                    reopened_transcripts[stage]["calls"]
                ),
                "provider_request_count": len(
                    reopened_transcripts[stage]["calls"]
                )
                + 1,
            }
            for stage in ("analysis", "critical_review")
        ]
        total_mcp_calls = sum(
            row["mcp_tool_call_count"] for row in expected_final_rows
        )
        total_provider_requests = sum(
            row["provider_request_count"] for row in expected_final_rows
        )
        if (
            final_receipt.get("stage_call_receipts") != expected_final_rows
            or final_receipt.get("model_mcp_tool_call_count")
            != total_mcp_calls
            or final_receipt.get("provider_request_count")
            != total_provider_requests
            or final_stage.get("model_mcp_tool_call_count") != total_mcp_calls
            or final_stage.get("provider_request_count")
            != total_provider_requests
            or final_stage.get("provider_request_count_status")
            != "derived_from_codex_tool_loop"
            or publication.get("mcp_tool_call_count") != total_mcp_calls
            or publication.get("provider_request_count")
            != total_provider_requests
        ):
            raise ProcessingPluginError("mcp_read_session_final_receipt_invalid")
        return {
            "context": copy.deepcopy(context),
            "stage_receipts": copy.deepcopy(dict(stage_receipts)),
            "transcripts": copy.deepcopy(reopened_transcripts),
            "stage_calls": {
                stage: copy.deepcopy(reopened_transcripts[stage]["calls"])
                for stage in ("analysis", "critical_review")
            },
        }

    def freeze(
        self,
        *,
        subject: str,
        capture_id: str,
        study_date: str,
        input_fingerprint: str,
        input_binding: Mapping[str, Any],
        provider_schema_sha256: str,
        canonical_schema_sha256: str,
        validator_sha256: str,
    ) -> dict[str, Any]:
        raise ProcessingPluginError("legacy_host_semantic_prefetch_forbidden")
        for value in (
            input_fingerprint,
            provider_schema_sha256,
            canonical_schema_sha256,
            validator_sha256,
        ):
            if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
                raise ProcessingPluginError("processing_binding_hash_invalid")
        snapshot = self._snapshot(subject)
        scope = {
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "cold_evidence": {
                "math": "activity_window",
                "cs408": "curation_inventory_projection_event_binding",
                "english": "article_coverage",
            }[subject],
        }
        scope_hash = _sha256_value(scope)
        route_id = f"bg-{subject}-{scope_hash[:24]}"
        route = self._route(snapshot=snapshot, scope_hash=scope_hash, route_id=route_id)
        authority = self._call(
            subject,
            "authority_bundle",
            {
                "subjects": [subject],
                "capabilities": SUBJECT_CAPABILITIES[subject],
                "checks": ["exists", "parseable", "release_bound", "projection_bound"],
                "route": route,
            },
        )
        self._validate_envelope(
            authority,
            route=route,
            subject=subject,
            tool="authority_bundle",
            expected_server_release=snapshot.mcp_server_release,
        )
        subject_item = next(
            (
                row
                for row in authority.get("items", [])
                if isinstance(row, Mapping) and row.get("subject") == subject
            ),
            None,
        )
        base_release_id = authority.get("preprocessor_release")
        if (
            not isinstance(subject_item, Mapping)
            or subject_item.get("available") is not True
            or not isinstance(base_release_id, str)
            or not SHA256_RE.fullmatch(base_release_id)
        ):
            raise ProcessingPluginError("background_mcp_authority_binding_invalid")
        generation = subject_item.get("generation")
        fingerprint = subject_item.get("authority_fingerprint")
        adapter_release = subject_item.get("adapter_release")
        if (
            not isinstance(generation, str)
            or not isinstance(fingerprint, str)
            or not SHA256_RE.fullmatch(fingerprint)
            or not isinstance(adapter_release, str)
            or adapter_release != authority.get("server_release")
        ):
            raise ProcessingPluginError("background_mcp_authority_binding_invalid")
        mcp_hash = adapter_release.rsplit("+sha256.", 1)[-1]
        if (
            not SHA256_RE.fullmatch(mcp_hash)
            or adapter_release != snapshot.mcp_server_release
        ):
            raise ProcessingPluginError("background_mcp_release_hash_invalid")
        binding_core = {
            "schema_version": "processing_binding_v1",
            "base_release_id": base_release_id,
            "candidate_release_id": self.candidate_release_id,
            "plugin": {
                "id": snapshot.plugin_name,
                "version": snapshot.plugin_version,
                "sha256": snapshot.component_lock_sha256,
            },
            "skill": {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
            },
            "mcp": {
                "id": SUBJECT_MCP_SERVERS[subject],
                "version": adapter_release,
                "sha256": mcp_hash,
            },
            "schemas": {
                "provider_sha256": provider_schema_sha256,
                "canonical_sha256": canonical_schema_sha256,
            },
            "validator_sha256": validator_sha256,
            "formal_write_count": 0,
        }
        processing_binding = {
            **binding_core,
            "binding_sha256": _sha256_value(binding_core),
        }
        tool, read_arguments = self._read_arguments(
            subject,
            study_date=study_date,
            input_binding={**dict(input_binding), "capture_id": capture_id},
            route=route,
            generation=generation,
            adapter_release=adapter_release,
            fingerprint=fingerprint,
        )
        evidence = self._call(subject, tool, read_arguments)
        self._validate_envelope(
            evidence,
            route=route,
            subject=subject,
            tool=tool,
            expected_server_release=snapshot.mcp_server_release,
            require_subject=True,
        )
        if (
            evidence.get("generation") != generation
            or evidence.get("authority_fingerprint") != fingerprint
            or evidence.get("adapter_release") != adapter_release
        ):
            raise ProcessingPluginError("background_mcp_generation_mismatch")
        if subject == "cs408":
            binding = (
                ((evidence.get("items") or [{}])[0]).get("projection_event_binding")
                if isinstance((evidence.get("items") or [{}])[0], Mapping)
                else None
            )
            if (
                not isinstance(binding, Mapping)
                or binding.get("data_role") != "projection_event_binding"
                or not isinstance(binding.get("event_count"), int)
                or not SHA256_RE.fullmatch(str(binding.get("projection_sha256") or ""))
                or not SHA256_RE.fullmatch(str(binding.get("event_ledger_sha256") or ""))
            ):
                raise ProcessingPluginError("background_cs408_projection_event_binding_invalid")
        evidence_bundle = {
            "schema_version": "background_mcp_evidence_bundle_v1",
            "subject": subject,
            "scope": scope,
            "authority": authority,
            "evidence": evidence,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        evidence_bundle_sha256 = _sha256_value(evidence_bundle)
        freeze_core = {
            "schema_version": "evidence_freeze_receipt_v1",
            "freeze_id": "SIP-FREEZE-" + _sha256_value(
                {
                    "scope": scope_hash,
                    "generation": generation,
                    "authority": fingerprint,
                    "binding": processing_binding["binding_sha256"],
                }
            )[:24].upper(),
            "subject": subject,
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "route": route,
            "generation": generation,
            "authority_fingerprint": fingerprint,
            "evidence_items": [
                {"tool": "authority_bundle", "result": authority},
                {"tool": tool, "result": evidence},
            ],
            "evidence_bundle_sha256": evidence_bundle_sha256,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        key = self._authority_key()
        freeze_receipt = {
            **freeze_core,
            "hmac_key_id": _sha256_bytes(key),
            "hmac_sha256": hmac.new(
                key,
                _canonical_bytes(
                    {
                        "purpose": "evidence-freeze-receipt-v1",
                        "payload": freeze_core,
                    }
                ),
                hashlib.sha256,
            ).hexdigest(),
        }
        receipt_sha256 = _sha256_value(freeze_receipt)
        receipt_path = (
            self.runtime_root
            / "dispatch"
            / "evidence-freezes"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        _atomic_publish(receipt_path, freeze_receipt)
        if _sha256_file(receipt_path) != receipt_sha256:
            raise ProcessingPluginError("evidence_freeze_receipt_write_mismatch")
        return {
            "processing_binding": processing_binding,
            "processing_binding_sha256": processing_binding["binding_sha256"],
            "evidence_freeze_receipt": freeze_receipt,
            "evidence_freeze_receipt_sha256": receipt_sha256,
            "evidence_freeze_receipt_ref": (
                "study-intake-evidence-freeze://sha256/" + receipt_sha256
            ),
            "evidence_bundle": evidence_bundle,
            "processing_skill": {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
                "shared_contract": snapshot.shared_contract_text,
                "skill": snapshot.skill_text,
            },
            "formal_write_count": 0,
        }

    def validate_frozen_context(
        self,
        *,
        subject: str,
        context: Mapping[str, Any],
    ) -> dict[str, Any]:
        raise ProcessingPluginError("legacy_host_semantic_prefetch_forbidden")
        """Verify a checkpointed freeze without performing another MCP read."""

        snapshot = self._snapshot(subject)
        processing_binding = context.get("processing_binding")
        freeze_receipt = context.get("evidence_freeze_receipt")
        evidence_bundle = context.get("evidence_bundle")
        processing_skill = context.get("processing_skill")
        if not all(
            isinstance(value, Mapping)
            for value in (
                processing_binding,
                freeze_receipt,
                evidence_bundle,
                processing_skill,
            )
        ):
            raise ProcessingPluginError("checkpoint_processing_context_invalid")
        binding_core = {
            key: value
            for key, value in processing_binding.items()
            if key != "binding_sha256"
        }
        binding_sha256 = processing_binding.get("binding_sha256")
        if (
            binding_sha256 != _sha256_value(binding_core)
            or context.get("processing_binding_sha256") != binding_sha256
            or processing_binding.get("candidate_release_id")
            != self.candidate_release_id
            or processing_binding.get("formal_write_count") != 0
            or processing_binding.get("plugin")
            != {
                "id": snapshot.plugin_name,
                "version": snapshot.plugin_version,
                "sha256": snapshot.component_lock_sha256,
            }
            or processing_binding.get("skill")
            != {
                "id": snapshot.skill_id,
                "version": snapshot.skill_version,
                "sha256": snapshot.skill_sha256,
            }
        ):
            raise ProcessingPluginError("checkpoint_processing_binding_invalid")
        if (
            processing_skill.get("id") != snapshot.skill_id
            or processing_skill.get("version") != snapshot.skill_version
            or processing_skill.get("sha256") != snapshot.skill_sha256
            or processing_skill.get("skill") != snapshot.skill_text
            or processing_skill.get("shared_contract")
            != snapshot.shared_contract_text
        ):
            raise ProcessingPluginError("checkpoint_processing_skill_invalid")
        receipt_sha256 = _sha256_value(freeze_receipt)
        receipt_ref = "study-intake-evidence-freeze://sha256/" + receipt_sha256
        receipt_path = (
            self.runtime_root
            / "dispatch"
            / "evidence-freezes"
            / "sha256"
            / receipt_sha256[:2]
            / f"{receipt_sha256}.json"
        )
        if (
            context.get("evidence_freeze_receipt_sha256") != receipt_sha256
            or context.get("evidence_freeze_receipt_ref") != receipt_ref
            or not receipt_path.is_file()
            or _sha256_file(receipt_path) != receipt_sha256
            or _load_object(receipt_path) != dict(freeze_receipt)
            or freeze_receipt.get("subject") != subject
            or freeze_receipt.get("processing_binding_sha256") != binding_sha256
            or freeze_receipt.get("evidence_bundle_sha256")
            != _sha256_value(evidence_bundle)
            or freeze_receipt.get("formal_write_count") != 0
            or evidence_bundle.get("subject") != subject
            or evidence_bundle.get("formal_write_count") != 0
        ):
            raise ProcessingPluginError("checkpoint_evidence_freeze_invalid")
        freeze_core = {
            key: value
            for key, value in freeze_receipt.items()
            if key not in {"hmac_key_id", "hmac_sha256"}
        }
        key = self._authority_key()
        expected_hmac = hmac.new(
            key,
            _canonical_bytes(
                {
                    "purpose": "evidence-freeze-receipt-v1",
                    "payload": freeze_core,
                }
            ),
            hashlib.sha256,
        ).hexdigest()
        if (
            freeze_receipt.get("hmac_key_id") != _sha256_bytes(key)
            or not hmac.compare_digest(
                str(freeze_receipt.get("hmac_sha256") or ""), expected_hmac
            )
        ):
            raise ProcessingPluginError("checkpoint_evidence_freeze_hmac_invalid")
        return copy.deepcopy(dict(context))
