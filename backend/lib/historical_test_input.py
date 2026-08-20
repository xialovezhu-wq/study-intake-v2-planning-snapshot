"""Hash-locked external inputs for historical, read-only release tests.

The immutable release never contains runtime state.  Blocking historical tests
therefore receive one explicit content-addressed manifest whose allowlist and
protected collections are verified before any historical evidence is read.
"""

from __future__ import annotations

import ast
import contextlib
import contextvars
import copy
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


SCHEMA_VERSION = "study-intake-historical-test-input-v1"
ENV_NAME = "STUDY_PREPROCESSOR_HISTORICAL_TEST_INPUT_MANIFEST"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
VERIFICATION_ROOT_NAME = "verification"
EXTERNAL_VERIFICATION_KEY = "external_verification"
EXTERNAL_RUNTIME_SOURCE_ROLES = {
    "math_status_script": ("math_status",),
    "cs408_status_script": ("cs408_status",),
    "english_events_module": ("english_events",),
}
_ACTIVE_ALLOWLIST: contextvars.ContextVar[frozenset[str] | None] = (
    contextvars.ContextVar("historical_test_input_allowlist", default=None)
)


class HistoricalTestInputError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _has_symlink_component(path: Path) -> bool:
    """Return whether any existing component of an absolute path is a link."""

    if not path.is_absolute():
        return True
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            if current.is_symlink():
                return True
        except OSError:
            # A path we cannot inspect is unsafe for a historical input.
            return True
    return False


def _assert_read_only(path: Path, *, code: str) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise HistoricalTestInputError(code) from exc
    if mode & 0o222:
        raise HistoricalTestInputError(code)


def _absolute_regular_file(path: Path, *, code: str) -> Path:
    path = path.expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise HistoricalTestInputError(code)
    resolved = path.resolve()
    if _has_symlink_component(resolved):
        raise HistoricalTestInputError(code)
    return resolved


def _absolute_directory(path: Path, *, code: str) -> Path:
    path = path.expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise HistoricalTestInputError(code)
    resolved = path.resolve()
    if _has_symlink_component(resolved):
        raise HistoricalTestInputError(code)
    return resolved


def _declared_external_file(
    path: Path,
    *,
    code: str,
    allow_terminal_symlink: bool = False,
    require_executable: bool = False,
) -> tuple[Path, Path]:
    """Return ``(declared, resolved)`` for an external source file.

    External declarations are intentionally kept separate from the three
    historical source roots.  A Python executable is the one permitted
    exception to the no-link input rule: the declaration may be a terminal
    symlink, but the bytes copied into the fixture always come from a regular,
    non-linked target.
    """

    declared = path.expanduser()
    if not declared.is_absolute() or ".." in declared.parts:
        raise HistoricalTestInputError(code)
    if allow_terminal_symlink:
        try:
            parent_resolved = declared.parent.resolve(strict=True)
            resolved = declared.resolve(strict=True)
        except OSError as exc:
            raise HistoricalTestInputError(code) from exc
        if _has_symlink_component(parent_resolved):
            raise HistoricalTestInputError(code)
        if resolved.is_symlink() or not resolved.is_file():
            raise HistoricalTestInputError(code)
        if _has_symlink_component(resolved):
            raise HistoricalTestInputError(code)
    else:
        resolved = _absolute_regular_file(declared, code=code)
    if require_executable and not os.access(resolved, os.X_OK):
        raise HistoricalTestInputError(code)
    return declared, resolved


def _verification_fixture_path(
    verification_root: Path,
    declared_path: Path,
    *,
    fixture_root: Path,
    code: str,
) -> Path:
    """Mirror an absolute declaration below the fixed verification root."""

    if not declared_path.is_absolute() or ".." in declared_path.parts:
        raise HistoricalTestInputError(code)
    try:
        relative = declared_path.relative_to(Path(declared_path.anchor))
    except ValueError as exc:
        raise HistoricalTestInputError(code) from exc
    if not relative.parts:
        raise HistoricalTestInputError(code)
    destination = verification_root / relative
    if (
        not _within(destination, fixture_root)
        or not _within(destination, verification_root)
        or _has_symlink_component(destination)
    ):
        raise HistoricalTestInputError(code)
    return destination


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _safe_relative_path(root: Path, raw: Any, *, code: str) -> Path:
    """Resolve a manifest-relative path without allowing escape or links."""

    if not isinstance(raw, str) or not raw:
        raise HistoricalTestInputError(code)
    relative = Path(raw)
    if relative.is_absolute():
        raise HistoricalTestInputError(code)
    candidate = root / relative
    if _has_symlink_component(candidate):
        raise HistoricalTestInputError(code)
    resolved = candidate.resolve()
    if not _within(resolved, root):
        raise HistoricalTestInputError(code)
    return resolved


def _file_row(path: Path, roles: Iterable[str]) -> dict[str, Any]:
    resolved = _absolute_regular_file(path, code="historical_test_input_file_invalid")
    normalized_roles = sorted({str(role) for role in roles if str(role)})
    if not normalized_roles:
        raise HistoricalTestInputError("historical_test_input_roles_missing")
    return {
        "absolute_path": str(resolved),
        "byte_count": resolved.stat().st_size,
        "sha256": sha256_file(resolved),
        "roles": normalized_roles,
    }


def _walk_files(path: Path) -> list[Path]:
    if path.is_symlink():
        raise HistoricalTestInputError("historical_test_input_collection_symlink")
    if path.is_file():
        return [path.resolve()]
    if not path.exists():
        return []
    if not path.is_dir():
        raise HistoricalTestInputError("historical_test_input_collection_invalid")
    result: list[Path] = []
    for child in sorted(path.rglob("*")):
        if child.is_symlink():
            raise HistoricalTestInputError("historical_test_input_collection_symlink")
        if child.is_file():
            result.append(child.resolve())
        elif child.exists() and not child.is_dir():
            raise HistoricalTestInputError("historical_test_input_collection_invalid")
    return result


def _snapshot_paths(paths: Sequence[Path]) -> dict[str, Any]:
    files: dict[str, dict[str, Any]] = {}
    roots: list[dict[str, Any]] = []
    for path in paths:
        if not path.is_absolute() or path.is_symlink():
            raise HistoricalTestInputError("historical_test_input_collection_root_invalid")
        roots.append(
            {
                "absolute_path": str(path),
                "exists": path.exists(),
                "kind": "file" if path.is_file() else "directory" if path.is_dir() else "absent",
            }
        )
        for child in _walk_files(path):
            files[str(child)] = {
                "byte_count": child.stat().st_size,
                "sha256": sha256_file(child),
            }
    ordered_files = {key: files[key] for key in sorted(files)}
    payload = {"roots": roots, "files": ordered_files}
    return {
        **payload,
        "file_count": len(ordered_files),
        "content_sha256": hashlib.sha256(canonical_bytes(payload)).hexdigest(),
    }


def _collection(name: str, paths: Sequence[Path]) -> dict[str, Any]:
    snapshot = _snapshot_paths(paths)
    return {"name": name, **snapshot}


def _fixture_output_root(path: Path) -> Path:
    root = path.expanduser()
    if not root.is_absolute():
        root = Path.cwd() / root
    if root.is_symlink():
        raise HistoricalTestInputError("historical_test_input_output_root_invalid")
    root = root.resolve()
    if _has_symlink_component(root):
        raise HistoricalTestInputError("historical_test_input_output_root_invalid")
    if root in {Path("/"), Path.home().resolve()}:
        raise HistoricalTestInputError("historical_test_input_output_root_invalid")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir() or _has_symlink_component(root):
        raise HistoricalTestInputError("historical_test_input_output_root_invalid")
    return root


def _ensure_fixture_directory(path: Path) -> None:
    if path.is_symlink():
        raise HistoricalTestInputError("historical_test_input_fixture_symlink")
    if path.exists():
        if path.is_symlink() or not path.is_dir():
            raise HistoricalTestInputError("historical_test_input_fixture_collision")
    else:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if _has_symlink_component(path.resolve()):
        raise HistoricalTestInputError("historical_test_input_fixture_symlink")


def _copy_frozen_file(
    source: Path,
    destination: Path,
    *,
    expected_byte_count: int,
    expected_sha256: str,
) -> None:
    source = _absolute_regular_file(
        source, code="historical_test_input_source_file_invalid"
    )
    before_count = source.stat().st_size
    before_sha256 = sha256_file(source)
    if (
        before_count != expected_byte_count
        or before_sha256 != expected_sha256
    ):
        raise HistoricalTestInputError(
            f"historical_test_input_source_file_drift:{source}"
        )
    _ensure_fixture_directory(destination.parent)
    if _has_symlink_component(destination):
        raise HistoricalTestInputError("historical_test_input_fixture_symlink")
    if destination.exists():
        if destination.is_symlink() or not destination.is_file():
            raise HistoricalTestInputError("historical_test_input_fixture_collision")
        if (
            destination.stat().st_size != expected_byte_count
            or sha256_file(destination) != expected_sha256
        ):
            raise HistoricalTestInputError(
                f"historical_test_input_fixture_collision:{destination}"
            )
    else:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o400,
        )
        try:
            with source.open("rb") as source_handle, os.fdopen(
                descriptor, "wb"
            ) as destination_handle:
                descriptor = -1
                while True:
                    chunk = source_handle.read(1024 * 1024)
                    if not chunk:
                        break
                    destination_handle.write(chunk)
                destination_handle.flush()
                os.fsync(destination_handle.fileno())
        finally:
            if descriptor != -1:
                os.close(descriptor)
    try:
        destination.chmod(0o444)
    except OSError as exc:
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_not_read_only:{destination}"
        ) from exc
    if (
        destination.stat().st_size != expected_byte_count
        or sha256_file(destination) != expected_sha256
    ):
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_hash_drift:{destination}"
        )
    if (
        source.stat().st_size != before_count
        or sha256_file(source) != before_sha256
    ):
        raise HistoricalTestInputError(
            f"historical_test_input_source_file_drift:{source}"
        )


def _copy_external_frozen_file(
    source: Path,
    destination: Path,
    *,
    expected_byte_count: int,
    expected_sha256: str,
    executable: bool,
) -> None:
    """Copy one external source while checking source bytes before and after."""

    source = _absolute_regular_file(
        source, code="historical_test_input_external_source_invalid"
    )
    before_count = source.stat().st_size
    before_sha256 = sha256_file(source)
    if before_count != expected_byte_count or before_sha256 != expected_sha256:
        raise HistoricalTestInputError(
            f"historical_test_input_external_source_hash_drift:{source}"
        )
    _copy_frozen_file(
        source,
        destination,
        expected_byte_count=expected_byte_count,
        expected_sha256=expected_sha256,
    )
    after_count = source.stat().st_size
    after_sha256 = sha256_file(source)
    if after_count != before_count or after_sha256 != before_sha256:
        raise HistoricalTestInputError(
            f"historical_test_input_external_source_hash_drift:{source}"
        )
    try:
        destination.chmod(0o555 if executable else 0o444)
    except OSError as exc:
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_not_read_only:{destination}"
        ) from exc
    mode = stat.S_IMODE(destination.stat().st_mode)
    if mode & 0o222 or (executable and not mode & 0o111):
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_executable_invalid:{destination}"
        )
    if (
        destination.stat().st_size != expected_byte_count
        or sha256_file(destination) != expected_sha256
    ):
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_hash_drift:{destination}"
        )


def _map_fixture_path(
    path: Path,
    *,
    source_roots: Mapping[str, Path],
    destination_roots: Mapping[str, Path],
    fixture_root: Path,
    code: str,
) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute() or candidate.is_symlink():
        raise HistoricalTestInputError(code)
    resolved = candidate.resolve()
    if _has_symlink_component(resolved):
        raise HistoricalTestInputError(code)
    matches = sorted(
        (
            (len(source_root.parts), name)
            for name, source_root in source_roots.items()
            if _within(resolved, source_root)
        ),
        reverse=True,
    )
    if not matches or (
        len(matches) > 1 and matches[0][0] == matches[1][0]
    ):
        raise HistoricalTestInputError(code)
    name = matches[0][1]
    destination = destination_roots[name] / resolved.relative_to(source_roots[name])
    if not _within(destination, fixture_root) or _has_symlink_component(destination):
        raise HistoricalTestInputError(code)
    return destination


def _freeze_fixture_tree(
    root: Path,
    *,
    executable_files: Iterable[Path] = (),
) -> None:
    if root.is_symlink() or not root.is_dir() or _has_symlink_component(root):
        raise HistoricalTestInputError("historical_test_input_fixture_root_invalid")
    executable_keys = {str(Path(path).resolve()) for path in executable_files}
    paths = sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True)
    for path in paths:
        if path.is_symlink() or _has_symlink_component(path):
            raise HistoricalTestInputError("historical_test_input_fixture_symlink")
        try:
            path.chmod(
                0o555
                if path.is_dir() or str(path.resolve()) in executable_keys
                else 0o444
            )
        except OSError as exc:
            raise HistoricalTestInputError(
                f"historical_test_input_fixture_not_read_only:{path}"
            ) from exc
    try:
        root.chmod(0o555)
    except OSError as exc:
        raise HistoricalTestInputError(
            f"historical_test_input_fixture_not_read_only:{root}"
        ) from exc


def _validate_historical_relative_reference(raw: Any) -> None:
    if not isinstance(raw, str) or not raw:
        raise HistoricalTestInputError(
            "historical_test_input_manifest_path_escape"
        )
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise HistoricalTestInputError(
            "historical_test_input_manifest_path_escape"
        )


def _validate_historical_test_manifest_references(value: Mapping[str, Any]) -> None:
    """Reject absolute or escaping paths embedded in the historical index."""

    selection = value.get("selection_contract")
    if not isinstance(selection, Mapping):
        raise HistoricalTestInputError(
            "historical_test_input_manifest_path_escape"
        )
    _validate_historical_relative_reference(selection.get("ledger_path"))
    items = value.get("items")
    if not isinstance(items, list):
        raise HistoricalTestInputError(
            "historical_test_input_manifest_path_escape"
        )
    for item in items:
        if not isinstance(item, Mapping):
            raise HistoricalTestInputError(
                "historical_test_input_manifest_path_escape"
            )
        v1 = item.get("v1")
        if not isinstance(v1, Mapping):
            raise HistoricalTestInputError(
                "historical_test_input_manifest_path_escape"
            )
        for key in ("package_path", "run_receipt_path"):
            _validate_historical_relative_reference(v1.get(key))
        adoption = item.get("baseline_adoption")
        if not isinstance(adoption, Mapping):
            raise HistoricalTestInputError(
                "historical_test_input_manifest_path_escape"
            )
        _validate_historical_relative_reference(adoption.get("path"))

        current = item.get("current_formal_target")
        if isinstance(current, Mapping) and isinstance(current.get("path"), str):
            _validate_historical_relative_reference(current.get("path"))
        replay = item.get("replay_input")
        if not isinstance(replay, Mapping):
            raise HistoricalTestInputError(
                "historical_test_input_manifest_path_escape"
            )
        formal_target = replay.get("formal_target")
        if isinstance(formal_target, Mapping):
            historical_card = formal_target.get("historical_card")
            if isinstance(historical_card, Mapping) and isinstance(
                historical_card.get("path"), str
            ):
                _validate_historical_relative_reference(historical_card.get("path"))
        source = replay.get("source_bundle")
        if isinstance(source, Mapping):
            _validate_historical_relative_reference(source.get("manifest_path"))
            artifacts = source.get("artifacts")
            if not isinstance(artifacts, list):
                raise HistoricalTestInputError(
                    "historical_test_input_manifest_path_escape"
                )
            for artifact in artifacts:
                if not isinstance(artifact, Mapping):
                    raise HistoricalTestInputError(
                        "historical_test_input_manifest_path_escape"
                    )
                _validate_historical_relative_reference(artifact.get("path"))

        capture_event = replay.get("capture_event")
        if isinstance(capture_event, Mapping):
            capture_source = capture_event.get("source_bundle")
            if isinstance(capture_source, Mapping):
                _validate_historical_relative_reference(
                    capture_source.get("manifest_path")
                )
                artifacts = capture_source.get("artifacts")
                if isinstance(artifacts, list):
                    for artifact in artifacts:
                        if isinstance(artifact, Mapping):
                            _validate_historical_relative_reference(
                                artifact.get("path")
                            )


def _external_capture_row(
    *,
    declared_path: Path,
    expected_sha256: str,
    roles: Iterable[str],
    executable: bool = False,
) -> dict[str, Any]:
    declared, resolved = _declared_external_file(
        declared_path,
        code="historical_test_input_external_source_invalid",
        allow_terminal_symlink=executable,
        require_executable=executable,
    )
    if not isinstance(expected_sha256, str) or SHA256_RE.fullmatch(expected_sha256) is None:
        raise HistoricalTestInputError("historical_test_input_external_sha_invalid")
    byte_count = resolved.stat().st_size
    if sha256_file(resolved) != expected_sha256:
        raise HistoricalTestInputError(
            f"historical_test_input_external_source_hash_drift:{declared}"
        )
    normalized_roles = sorted({str(role) for role in roles if str(role)})
    if not normalized_roles:
        raise HistoricalTestInputError("historical_test_input_external_roles_missing")
    return {
        "kind": normalized_roles[0],
        "declared_path": str(declared),
        "source_path": str(resolved),
        "byte_count": byte_count,
        "sha256": expected_sha256,
        "roles": normalized_roles,
        "executable": executable,
    }


def _capture_external_verification(
    *,
    source_root: Path,
    component_registry_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Enumerate and hash every byte consumed by the staging generator."""

    source_root = _absolute_directory(
        source_root, code="historical_test_input_source_root_invalid"
    )
    plugin_root = source_root / "plugin/kaoyan-study-intake"
    registry_path = component_registry_path or plugin_root / "components.json"
    registry_path = _absolute_regular_file(
        registry_path, code="historical_test_input_components_registry_invalid"
    )
    if not _within(registry_path, plugin_root):
        raise HistoricalTestInputError(
            "historical_test_input_components_registry_escape"
        )
    try:
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalTestInputError(
            "historical_test_input_components_registry_invalid"
        ) from exc
    if not isinstance(registry, dict):
        raise HistoricalTestInputError("historical_test_input_components_registry_invalid")

    rows: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any]) -> None:
        key = str(row["declared_path"])
        previous = rows.get(key)
        if previous is None:
            rows[key] = row
            return
        if (
            previous["source_path"] != row["source_path"]
            or previous["byte_count"] != row["byte_count"]
            or previous["sha256"] != row["sha256"]
            or previous["executable"] != row["executable"]
        ):
            raise HistoricalTestInputError(
                "historical_test_input_external_binding_conflict"
            )
        previous["roles"] = sorted(
            set(previous["roles"]) | set(row["roles"])
        )

    registry_sha256 = sha256_file(registry_path)
    add(
        _external_capture_row(
            declared_path=registry_path,
            expected_sha256=registry_sha256,
            roles=("component_registry",),
        )
    )
    external_sources = registry.get("external_runtime_sources")
    if (
        not isinstance(external_sources, dict)
        or set(external_sources) != set(EXTERNAL_RUNTIME_SOURCE_ROLES)
    ):
        raise HistoricalTestInputError("historical_test_input_external_registry_invalid")
    for source_name, roles in EXTERNAL_RUNTIME_SOURCE_ROLES.items():
        source = external_sources.get(source_name)
        if not isinstance(source, dict) or set(source) != {"path", "sha256"}:
            raise HistoricalTestInputError(
                "historical_test_input_external_source_invalid"
            )
        add(
            _external_capture_row(
                declared_path=Path(str(source.get("path") or "")),
                expected_sha256=source.get("sha256"),
                roles=roles,
            )
        )
    foreground_contracts = registry.get("foreground_capture_contracts")
    multi_agent_registry = registry.get("multi_agent")
    legacy_fixture_registry = (
        foreground_contracts is None and multi_agent_registry is None
    )
    if not legacy_fixture_registry and (
        not isinstance(foreground_contracts, dict)
        or set(foreground_contracts) != {"math", "cs408", "english"}
    ):
        raise HistoricalTestInputError(
            "historical_test_input_foreground_registry_invalid"
        )
    for subject, binding in sorted((foreground_contracts or {}).items()):
        if not isinstance(binding, dict):
            raise HistoricalTestInputError(
                "historical_test_input_foreground_registry_invalid"
            )
        descriptor_path = Path(str(binding.get("descriptor_path") or ""))
        descriptor_sha256 = binding.get("descriptor_sha256")
        add(
            _external_capture_row(
                declared_path=descriptor_path,
                expected_sha256=descriptor_sha256,
                roles=("foreground_binding_descriptor", subject),
            )
        )
        try:
            descriptor = json.loads(
                descriptor_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise HistoricalTestInputError(
                "historical_test_input_foreground_descriptor_invalid"
            ) from exc
        skill = descriptor.get("foreground_skill")
        producer = descriptor.get("producer")
        capture_contract = descriptor.get("capture_contract")
        if (
            descriptor.get("schema_version")
            != "producer_binding_descriptor_v1"
            or descriptor.get("subject") != subject
            or descriptor.get("formal_write_count") != 0
            or not isinstance(skill, dict)
            or not isinstance(producer, dict)
            or not isinstance(capture_contract, dict)
        ):
            raise HistoricalTestInputError(
                "historical_test_input_foreground_descriptor_invalid"
            )
        for prefix in ("authoritative", "installed"):
            add(
                _external_capture_row(
                    declared_path=Path(str(skill.get(f"{prefix}_path") or "")),
                    expected_sha256=skill.get(f"{prefix}_sha256"),
                    roles=("foreground_skill", subject, prefix),
                )
            )
        source_files = producer.get("source_files")
        contract_files = capture_contract.get("files")
        if not isinstance(source_files, list) or not isinstance(
            contract_files, list
        ):
            raise HistoricalTestInputError(
                "historical_test_input_foreground_descriptor_invalid"
            )
        for row in source_files:
            if not isinstance(row, dict):
                raise HistoricalTestInputError(
                    "historical_test_input_foreground_descriptor_invalid"
                )
            add(
                _external_capture_row(
                    declared_path=Path(str(row.get("path") or "")),
                    expected_sha256=row.get("sha256"),
                    roles=("foreground_producer_source", subject),
                )
            )
        for row in contract_files:
            if not isinstance(row, dict):
                raise HistoricalTestInputError(
                    "historical_test_input_foreground_descriptor_invalid"
                )
            add(
                _external_capture_row(
                    declared_path=Path(str(row.get("path") or "")),
                    expected_sha256=row.get("sha256"),
                    roles=("foreground_capture_contract", subject),
                )
            )

    mcp = registry.get("mcp")
    if not isinstance(mcp, dict):
        raise HistoricalTestInputError("historical_test_input_mcp_registry_invalid")
    release_root = Path(str(mcp.get("release_root") or "")).expanduser()
    release_manifest = Path(str(mcp.get("release_manifest") or "")).expanduser()
    if (
        not release_root.is_absolute()
        or not release_manifest.is_absolute()
        or release_manifest.name != "release.json"
        or release_manifest.parent != release_root
        or not isinstance(mcp.get("release_manifest_sha256"), str)
    ):
        raise HistoricalTestInputError("historical_test_input_mcp_registry_invalid")
    add(
        _external_capture_row(
            declared_path=release_manifest,
            expected_sha256=mcp["release_manifest_sha256"],
            roles=("mcp_release_manifest",),
        )
    )
    try:
        release_value = json.loads(release_manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalTestInputError(
            "historical_test_input_mcp_release_manifest_invalid"
        ) from exc
    source_files = release_value.get("source_files") if isinstance(release_value, dict) else None
    release_id = release_value.get("release_id") if isinstance(release_value, dict) else None
    if (
        not isinstance(source_files, dict)
        or not source_files
        or not isinstance(release_id, str)
        or SHA256_RE.fullmatch(release_id) is None
        or release_root.name != release_id
        or hashlib.sha256(canonical_bytes(source_files)).hexdigest() != release_id
        or mcp.get("release_id") != release_id
    ):
        raise HistoricalTestInputError("historical_test_input_mcp_release_manifest_invalid")
    for relative, expected_sha256 in sorted(source_files.items()):
        if not isinstance(relative, str):
            raise HistoricalTestInputError("historical_test_input_mcp_source_path_invalid")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise HistoricalTestInputError("historical_test_input_mcp_source_path_escape")
        add(
            _external_capture_row(
                declared_path=release_root / relative_path,
                expected_sha256=expected_sha256,
                roles=("mcp_release_source",),
            )
        )
    sealed_launcher = Path(str(mcp.get("sealed_launcher_path") or "")).expanduser()
    if (
        not sealed_launcher.is_absolute()
        or sealed_launcher != release_root / "scripts/sealed_launcher.py"
    ):
        raise HistoricalTestInputError("historical_test_input_mcp_launcher_invalid")
    add(
        _external_capture_row(
            declared_path=sealed_launcher,
            expected_sha256=mcp.get("sealed_launcher_sha256"),
            roles=("mcp_sealed_launcher",),
        )
    )
    add(
        _external_capture_row(
            declared_path=Path(str(mcp.get("python_executable") or "")),
            expected_sha256=sha256_file(
                _declared_external_file(
                    Path(str(mcp.get("python_executable") or "")),
                    code="historical_test_input_python_executable_invalid",
                    allow_terminal_symlink=True,
                    require_executable=True,
                )[1]
            ),
            roles=("python_executable",),
            executable=True,
        )
    )

    shared_schema_root = source_root / "schemas"
    required_schema_names: set[str] = set()
    generator_path = plugin_root / "scripts/generate_manifests.py"
    if generator_path.is_file() and not generator_path.is_symlink():
        try:
            tree = ast.parse(generator_path.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, ast.Assign) and any(
                    isinstance(target, ast.Name)
                    and target.id == "REQUIRED_RUNTIME_SCHEMAS"
                    for target in node.targets
                ):
                    value = ast.literal_eval(node.value)
                    if isinstance(value, set) and all(
                        isinstance(name, str) for name in value
                    ):
                        required_schema_names = set(value)
                    break
        except (OSError, UnicodeError, SyntaxError, ValueError) as exc:
            raise HistoricalTestInputError(
                "historical_test_input_generator_schema_inventory_invalid"
            ) from exc
    shared_schema_names = {
        path.name
        for path in shared_schema_root.glob("*.json")
        if path.is_file() and not path.is_symlink()
    }
    schema_names = shared_schema_names | required_schema_names
    if not schema_names:
        raise HistoricalTestInputError("historical_test_input_schema_inventory_empty")
    for name in sorted(schema_names):
        if (
            Path(name).name != name
            or Path(name).suffix != ".json"
        ):
            raise HistoricalTestInputError("historical_test_input_schema_path_invalid")
        source_schema = shared_schema_root / name
        if not source_schema.is_file() or source_schema.is_symlink():
            raise HistoricalTestInputError(
                "historical_test_input_required_schema_missing"
            )
        add(
            _external_capture_row(
                declared_path=source_schema,
                expected_sha256=sha256_file(source_schema),
                roles=("runtime_schema",),
            )
        )
    return [rows[key] for key in sorted(rows)]


def freeze_fixture(output_root: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    """Copy a captured descriptor into an immutable, self-contained fixture.

    The input descriptor still contains source paths.  Every referenced source
    file is copied below ``output_root`` before the descriptor is returned;
    callers then persist the returned descriptor content-addressed.  Existing
    destination bytes must match exactly, so a second capture cannot silently
    overwrite a frozen fixture.
    """

    if value.get("schema_version") != SCHEMA_VERSION:
        raise HistoricalTestInputError("historical_test_input_schema_invalid")
    fixture_root = _fixture_output_root(output_root)
    source_roots = {
        name: _absolute_directory(
            Path(str(value.get(name) or "")),
            code=f"historical_test_input_{name}_invalid",
        )
        for name in (
            "historical_manifest_root",
            "runtime_data_root",
            "formal_surface_root",
        )
    }
    source_root_values = list(source_roots.values())
    if len({str(path) for path in source_root_values}) != len(source_root_values):
        raise HistoricalTestInputError("historical_test_input_source_roots_overlap")
    for source_root in source_root_values:
        if _within(fixture_root, source_root) or _within(source_root, fixture_root):
            raise HistoricalTestInputError(
                "historical_test_input_fixture_overlaps_source"
            )

    destination_roots = {
        "historical_manifest_root": fixture_root / "historical_manifest",
        "runtime_data_root": fixture_root / "runtime_data",
        "formal_surface_root": fixture_root / "formal_surface",
    }
    for destination_root in destination_roots.values():
        _ensure_fixture_directory(destination_root)
    verification_root = fixture_root / VERIFICATION_ROOT_NAME
    declared_verification_root = value.get("verification_root")
    if (
        not isinstance(declared_verification_root, str)
        or Path(declared_verification_root).expanduser().resolve()
        != verification_root.resolve()
    ):
        raise HistoricalTestInputError(
            "historical_test_input_verification_root_layout_invalid"
        )
    _ensure_fixture_directory(verification_root)

    rows = value.get("allowed_files")
    if not isinstance(rows, list) or not rows:
        raise HistoricalTestInputError("historical_test_input_allowlist_invalid")
    mapped_files: dict[str, Path] = {}
    frozen_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise HistoricalTestInputError("historical_test_input_file_row_invalid")
        source = _absolute_regular_file(
            Path(str(row.get("absolute_path") or "")),
            code="historical_test_input_file_invalid",
        )
        byte_count = row.get("byte_count")
        digest = row.get("sha256")
        roles = row.get("roles")
        if (
            isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count < 0
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
            or not isinstance(roles, list)
            or not roles
        ):
            raise HistoricalTestInputError("historical_test_input_file_row_invalid")
        destination = _map_fixture_path(
            source,
            source_roots=source_roots,
            destination_roots=destination_roots,
            fixture_root=fixture_root,
            code="historical_test_input_file_escape",
        )
        source_key = str(source)
        if source_key in mapped_files:
            raise HistoricalTestInputError("historical_test_input_file_row_invalid")
        _copy_frozen_file(
            source,
            destination,
            expected_byte_count=byte_count,
            expected_sha256=digest,
        )
        mapped_files[source_key] = destination
        frozen_rows.append(
            {
                **row,
                "absolute_path": str(destination),
            }
        )

    external_rows = value.get(EXTERNAL_VERIFICATION_KEY)
    if not isinstance(external_rows, list) or not external_rows:
        raise HistoricalTestInputError(
            "historical_test_input_external_verification_invalid"
        )
    frozen_external_rows: list[dict[str, Any]] = []
    mapped_destinations = {str(destination) for destination in mapped_files.values()}
    for row in external_rows:
        if not isinstance(row, dict):
            raise HistoricalTestInputError(
                "historical_test_input_external_verification_row_invalid"
            )
        declared = Path(str(row.get("declared_path") or "")).expanduser()
        source = Path(str(row.get("source_path") or "")).expanduser()
        executable = row.get("executable")
        byte_count = row.get("byte_count")
        digest = row.get("sha256")
        roles = row.get("roles")
        if (
            not isinstance(row.get("kind"), str)
            or not row["kind"]
            or not isinstance(executable, bool)
            or isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count < 0
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
            or not isinstance(roles, list)
            or not roles
            or not all(isinstance(role, str) and role for role in roles)
        ):
            raise HistoricalTestInputError(
                "historical_test_input_external_verification_row_invalid"
            )
        declared, declared_source = _declared_external_file(
            declared,
            code="historical_test_input_external_declared_path_invalid",
            allow_terminal_symlink=executable,
            require_executable=executable,
        )
        source = _absolute_regular_file(
            source, code="historical_test_input_external_source_invalid"
        )
        if declared_source != source:
            raise HistoricalTestInputError(
                "historical_test_input_external_source_binding_drift"
            )
        if _within(source, fixture_root):
            raise HistoricalTestInputError(
                "historical_test_input_external_source_overlaps_fixture"
            )
        destination = _verification_fixture_path(
            verification_root,
            declared,
            fixture_root=fixture_root,
            code="historical_test_input_external_fixture_path_escape",
        )
        if str(destination) in mapped_destinations:
            raise HistoricalTestInputError(
                "historical_test_input_external_fixture_collision"
            )
        _copy_external_frozen_file(
            source,
            destination,
            expected_byte_count=byte_count,
            expected_sha256=digest,
            executable=executable,
        )
        mapped_files[str(source)] = destination
        mapped_destinations.add(str(destination))
        frozen_external = {
            **row,
            "declared_path": str(declared),
            "absolute_path": str(destination),
            "fixture_path": str(destination),
        }
        frozen_external.pop("source_path", None)
        frozen_external_rows.append(frozen_external)
        frozen_rows.append(frozen_external)

    frozen = copy.deepcopy(dict(value))
    frozen["verification_root"] = str(verification_root)
    for name, destination_root in destination_roots.items():
        frozen[name] = str(destination_root)
    historical = frozen.get("historical_manifest")
    if not isinstance(historical, dict):
        raise HistoricalTestInputError(
            "historical_test_input_historical_manifest_invalid"
        )
    source_manifest = _absolute_regular_file(
        Path(str(historical.get("absolute_path") or "")),
        code="historical_test_input_historical_manifest_invalid",
    )
    source_manifest_key = str(source_manifest)
    if source_manifest_key not in mapped_files:
        raise HistoricalTestInputError("historical_test_input_manifest_binding_invalid")
    historical["absolute_path"] = str(mapped_files[source_manifest_key])
    frozen["allowed_files"] = sorted(
        frozen_rows, key=lambda row: str(row["absolute_path"])
    )
    frozen[EXTERNAL_VERIFICATION_KEY] = sorted(
        frozen_external_rows, key=lambda row: str(row["declared_path"])
    )

    collections = value.get("protected_collections")
    if not isinstance(collections, list):
        raise HistoricalTestInputError("historical_test_input_collections_invalid")
    frozen_collections: list[dict[str, Any]] = []
    for collection in collections:
        if not isinstance(collection, dict) or not isinstance(
            collection.get("name"), str
        ):
            raise HistoricalTestInputError("historical_test_input_collection_invalid")
        roots = collection.get("roots")
        if not isinstance(roots, list):
            raise HistoricalTestInputError("historical_test_input_collection_invalid")
        destination_paths: list[Path] = []
        for root in roots:
            if not isinstance(root, dict):
                raise HistoricalTestInputError(
                    "historical_test_input_collection_invalid"
                )
            source_path = Path(str(root.get("absolute_path") or ""))
            destination_path = _map_fixture_path(
                source_path,
                source_roots=source_roots,
                destination_roots=destination_roots,
                fixture_root=fixture_root,
                code="historical_test_input_collection_escape",
            )
            if source_path.exists():
                if source_path.is_dir():
                    _ensure_fixture_directory(destination_path)
                elif source_path.is_file():
                    source_key = str(source_path.resolve())
                    if source_key not in mapped_files:
                        raise HistoricalTestInputError(
                            "historical_test_input_collection_file_unbound"
                        )
            destination_paths.append(destination_path)
        frozen_collections.append(
            _collection(collection["name"], destination_paths)
        )
    frozen["protected_collections"] = frozen_collections
    for destination_root in (*destination_roots.values(), verification_root):
        _freeze_fixture_tree(
            destination_root,
            executable_files=(
                Path(str(row["absolute_path"]))
                for row in frozen_external_rows
                if row.get("executable") is True
            ),
        )
    return frozen


def assert_guarded_read(path: Path) -> None:
    allowlist = _ACTIVE_ALLOWLIST.get()
    if allowlist is None:
        return
    resolved = str(path.expanduser().resolve())
    if resolved not in allowlist:
        raise HistoricalTestInputError(
            f"historical_test_input_read_not_allowed:{resolved}"
        )


@dataclass(frozen=True)
class HistoricalTestInput:
    path: Path
    file_sha256: str
    payload: Mapping[str, Any]
    files: Mapping[str, Mapping[str, Any]]

    @classmethod
    def from_env(cls) -> "HistoricalTestInput":
        raw = os.environ.get(ENV_NAME)
        if not raw:
            raise HistoricalTestInputError(
                "historical_test_input_manifest_environment_required"
            )
        return cls.load(Path(raw))

    @classmethod
    def load(cls, path: Path) -> "HistoricalTestInput":
        resolved = _absolute_regular_file(
            path, code="historical_test_input_manifest_invalid"
        )
        fixture_root = resolved.parent
        if (
            fixture_root.is_symlink()
            or not fixture_root.is_dir()
            or _has_symlink_component(fixture_root)
        ):
            raise HistoricalTestInputError(
                "historical_test_input_fixture_root_invalid"
            )
        _assert_read_only(
            fixture_root, code="historical_test_input_fixture_not_read_only"
        )
        mode = stat.S_IMODE(resolved.stat().st_mode)
        if mode & 0o222:
            raise HistoricalTestInputError(
                "historical_test_input_manifest_not_read_only"
            )
        file_sha = sha256_file(resolved)
        if resolved.name != f"{file_sha}.json":
            raise HistoricalTestInputError(
                "historical_test_input_manifest_not_content_addressed"
            )
        try:
            value = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HistoricalTestInputError(
                "historical_test_input_manifest_unreadable"
            ) from exc
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
            raise HistoricalTestInputError("historical_test_input_schema_invalid")
        if value.get("formal_write_count") != 0 or value.get("model_call_count") != 0:
            raise HistoricalTestInputError("historical_test_input_write_boundary_invalid")
        roots = {
            name: _absolute_directory(
                Path(str(value.get(name) or "")),
                code=f"historical_test_input_{name}_invalid",
            )
            for name in (
                "historical_manifest_root",
                "runtime_data_root",
                "formal_surface_root",
            )
        }
        if any(
            not _within(root, fixture_root)
            or _has_symlink_component(root)
            for root in roots.values()
        ):
            raise HistoricalTestInputError(
                "historical_test_input_fixture_root_escape"
            )
        expected_fixture_names = {
            "historical_manifest_root": "historical_manifest",
            "runtime_data_root": "runtime_data",
            "formal_surface_root": "formal_surface",
        }
        if any(
            root.parent != fixture_root or root.name != expected_name
            for name, expected_name in expected_fixture_names.items()
            for root in (roots[name],)
        ):
            raise HistoricalTestInputError(
                "historical_test_input_fixture_layout_invalid"
            )
        for root in roots.values():
            _assert_read_only(
                root, code="historical_test_input_fixture_not_read_only"
            )
        verification_root = _absolute_directory(
            Path(str(value.get("verification_root") or "")),
            code="historical_test_input_verification_root_invalid",
        )
        if (
            not _within(verification_root, fixture_root)
            or verification_root.parent != fixture_root
            or verification_root.name != VERIFICATION_ROOT_NAME
        ):
            raise HistoricalTestInputError(
                "historical_test_input_verification_root_layout_invalid"
            )
        _assert_read_only(
            verification_root, code="historical_test_input_fixture_not_read_only"
        )
        external_rows = value.get(EXTERNAL_VERIFICATION_KEY)
        if not isinstance(external_rows, list) or not external_rows:
            raise HistoricalTestInputError(
                "historical_test_input_external_verification_invalid"
            )
        rows = value.get("allowed_files")
        if not isinstance(rows, list) or not rows:
            raise HistoricalTestInputError("historical_test_input_allowlist_invalid")
        files: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise HistoricalTestInputError("historical_test_input_file_row_invalid")
            file_path = _absolute_regular_file(
                Path(str(row.get("absolute_path") or "")),
                code="historical_test_input_file_invalid",
            )
            if not _within(file_path, fixture_root):
                raise HistoricalTestInputError(
                    "historical_test_input_fixture_root_escape"
                )
            _assert_read_only(
                file_path, code="historical_test_input_file_not_read_only"
            )
            key = str(file_path)
            roles = row.get("roles")
            if (
                key in files
                or not isinstance(roles, list)
                or not roles
                or not all(isinstance(role, str) and role for role in roles)
                or isinstance(row.get("byte_count"), bool)
                or not isinstance(row.get("byte_count"), int)
                or row["byte_count"] < 0
                or not isinstance(row.get("sha256"), str)
                or SHA256_RE.fullmatch(row["sha256"]) is None
            ):
                raise HistoricalTestInputError("historical_test_input_file_row_invalid")
            if (
                file_path.stat().st_size != row["byte_count"]
                or sha256_file(file_path) != row["sha256"]
            ):
                raise HistoricalTestInputError(
                    f"historical_test_input_file_drift:{key}"
                )
            files[key] = row
        external_paths: set[str] = set()
        for row in external_rows:
            if not isinstance(row, dict):
                raise HistoricalTestInputError(
                    "historical_test_input_external_verification_row_invalid"
                )
            declared = Path(str(row.get("declared_path") or ""))
            fixture_path = _absolute_regular_file(
                Path(str(row.get("absolute_path") or "")),
                code="historical_test_input_external_fixture_path_invalid",
            )
            if row.get("fixture_path") != str(fixture_path):
                raise HistoricalTestInputError(
                    "historical_test_input_external_fixture_path_invalid"
                )
            if (
                not isinstance(row.get("kind"), str)
                or not row["kind"]
                or not isinstance(row.get("executable"), bool)
                or not isinstance(row.get("roles"), list)
                or not row["roles"]
                or not all(isinstance(role, str) and role for role in row["roles"])
                or (
                    "python_executable" in row["roles"]
                    and row.get("executable") is not True
                )
                or isinstance(row.get("byte_count"), bool)
                or not isinstance(row.get("byte_count"), int)
                or row["byte_count"] < 0
                or not isinstance(row.get("sha256"), str)
                or SHA256_RE.fullmatch(row["sha256"]) is None
                or not declared.is_absolute()
                or ".." in declared.parts
                or not _within(fixture_path, verification_root)
                or _verification_fixture_path(
                    verification_root,
                    declared,
                    fixture_root=fixture_root,
                    code="historical_test_input_external_fixture_path_escape",
                )
                != fixture_path
            ):
                raise HistoricalTestInputError(
                    "historical_test_input_external_verification_row_invalid"
                )
            key = str(fixture_path)
            bound = files.get(key)
            if (
                bound is None
                or bound.get("byte_count") != row["byte_count"]
                or bound.get("sha256") != row["sha256"]
                or bound.get("roles") != row["roles"]
            ):
                raise HistoricalTestInputError(
                    "historical_test_input_external_binding_invalid"
                )
            if row["executable"]:
                mode = stat.S_IMODE(fixture_path.stat().st_mode)
                if mode & 0o222 or not mode & 0o111:
                    raise HistoricalTestInputError(
                        "historical_test_input_external_executable_invalid"
                    )
            external_paths.add(key)
        verification_file_paths = {
            key
            for key in files
            if _within(Path(key), verification_root)
        }
        if verification_file_paths != external_paths:
            raise HistoricalTestInputError(
                "historical_test_input_external_verification_closure_invalid"
            )
        historical = value.get("historical_manifest")
        if not isinstance(historical, dict):
            raise HistoricalTestInputError("historical_test_input_historical_manifest_invalid")
        historical_path = _absolute_regular_file(
            Path(str(historical.get("absolute_path") or "")),
            code="historical_test_input_historical_manifest_invalid",
        )
        if (
            not _within(historical_path, fixture_root)
            or not _within(historical_path, roots["historical_manifest_root"])
            or str(historical_path) not in files
            or historical.get("sha256") != value.get("expected_manifest_sha256")
            or sha256_file(historical_path) != value.get("expected_manifest_sha256")
        ):
            raise HistoricalTestInputError("historical_test_input_manifest_binding_invalid")
        try:
            frozen = json.loads(historical_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HistoricalTestInputError(
                "historical_test_input_historical_manifest_invalid"
            ) from exc
        if (
            not isinstance(frozen, dict)
            or frozen.get("manifest_sha256")
            != value.get("expected_manifest_content_sha256")
        ):
            raise HistoricalTestInputError("historical_test_input_manifest_content_mismatch")
        _validate_historical_test_manifest_references(frozen)
        expected_replay = value.get("expected_replay_input_sha256")
        actual_replay = {
            str(item.get("capture_id")): item.get("replay_input_sha256")
            for item in frozen.get("items", [])
            if isinstance(item, dict)
        }
        if (
            not isinstance(expected_replay, dict)
            or len(expected_replay) != 10
            or expected_replay != actual_replay
            or not all(
                isinstance(digest, str) and SHA256_RE.fullmatch(digest)
                for digest in expected_replay.values()
            )
        ):
            raise HistoricalTestInputError("historical_test_input_replay_binding_invalid")
        instance = cls(resolved, file_sha, value, files)
        instance.verify_fixture_closure()
        instance.verify_protected_collections()
        return instance

    @property
    def fixture_root(self) -> Path:
        return self.path.parent.resolve()

    @property
    def historical_manifest_path(self) -> Path:
        value = self.payload["historical_manifest"]
        return Path(str(value["absolute_path"])).resolve()

    @property
    def historical_manifest_root(self) -> Path:
        return Path(str(self.payload["historical_manifest_root"])).resolve()

    @property
    def runtime_data_root(self) -> Path:
        return Path(str(self.payload["runtime_data_root"])).resolve()

    @property
    def formal_surface_root(self) -> Path:
        return Path(str(self.payload["formal_surface_root"])).resolve()

    @property
    def verification_root(self) -> Path:
        """Fixed root passed to ``generate_manifests.py --verification-root``."""

        value = Path(str(self.payload.get("verification_root") or "")).resolve()
        if value != self.fixture_root / VERIFICATION_ROOT_NAME:
            raise HistoricalTestInputError(
                "historical_test_input_verification_root_layout_invalid"
            )
        return value

    @property
    def external_verification_rows(self) -> tuple[Mapping[str, Any], ...]:
        rows = self.payload.get(EXTERNAL_VERIFICATION_KEY)
        if not isinstance(rows, list):
            raise HistoricalTestInputError(
                "historical_test_input_external_verification_invalid"
            )
        return tuple(row for row in rows if isinstance(row, Mapping))

    @property
    def external_verification(self) -> tuple[Mapping[str, Any], ...]:
        """Backward-friendly alias for callers enumerating verification rows."""

        return self.external_verification_rows

    @property
    def expected_manifest_sha256(self) -> str:
        return str(self.payload["expected_manifest_sha256"])

    @property
    def expected_manifest_content_sha256(self) -> str:
        return str(self.payload["expected_manifest_content_sha256"])

    @property
    def expected_replay_input_sha256(self) -> Mapping[str, str]:
        return self.payload["expected_replay_input_sha256"]  # type: ignore[return-value]

    def paths_for_role(self, role: str) -> tuple[Path, ...]:
        return tuple(
            Path(path)
            for path, row in sorted(self.files.items())
            if role in row.get("roles", [])
        )

    def assert_allowed(self, path: Path) -> None:
        candidate = path.expanduser()
        if not candidate.is_absolute() or _has_symlink_component(candidate):
            raise HistoricalTestInputError(
                f"historical_test_input_read_not_allowed:{candidate}"
            )
        resolved = str(candidate.resolve())
        if resolved not in self.files:
            raise HistoricalTestInputError(
                f"historical_test_input_read_not_allowed:{resolved}"
            )
        row = self.files[resolved]
        candidate = Path(resolved)
        _assert_read_only(
            candidate, code="historical_test_input_file_not_read_only"
        )
        if candidate.stat().st_size != row["byte_count"] or sha256_file(candidate) != row["sha256"]:
            raise HistoricalTestInputError(
                f"historical_test_input_file_drift:{resolved}"
            )

    def load_historical_manifest(self) -> dict[str, Any]:
        self.assert_allowed(self.historical_manifest_path)
        value = json.loads(self.historical_manifest_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise HistoricalTestInputError("historical_test_input_historical_manifest_invalid")
        return value

    @contextlib.contextmanager
    def enforce_reads(self) -> Iterator[None]:
        token = _ACTIVE_ALLOWLIST.set(frozenset(self.files))
        try:
            yield
        finally:
            _ACTIVE_ALLOWLIST.reset(token)

    def verify_fixture_closure(self) -> None:
        """Reject unbound files or links introduced below the fixture root."""

        known_files = set(self.files) | {str(self.path.resolve())}
        verification_root = self.verification_root
        for path in sorted(self.fixture_root.rglob("*")):
            if path.is_symlink() or _has_symlink_component(path):
                raise HistoricalTestInputError("historical_test_input_fixture_symlink")
            if path.is_file():
                if str(path.resolve()) not in known_files:
                    raise HistoricalTestInputError(
                        f"historical_test_input_fixture_unbound_file:{path}"
                    )
                _assert_read_only(
                    path, code="historical_test_input_file_not_read_only"
                )
            elif path.is_dir():
                _assert_read_only(
                    path, code="historical_test_input_fixture_not_read_only"
                )
            else:
                raise HistoricalTestInputError(
                    f"historical_test_input_fixture_invalid:{path}"
                )
        if not verification_root.is_dir() or _has_symlink_component(verification_root):
            raise HistoricalTestInputError(
                "historical_test_input_verification_root_invalid"
            )

    def snapshot(self) -> dict[str, Any]:
        verification_root = self.verification_root
        collections = self.payload.get("protected_collections")
        if not isinstance(collections, list):
            raise HistoricalTestInputError("historical_test_input_collections_invalid")
        result: dict[str, Any] = {}
        for row in collections:
            if not isinstance(row, dict) or not isinstance(row.get("name"), str):
                raise HistoricalTestInputError("historical_test_input_collection_invalid")
            roots = row.get("roots")
            if not isinstance(roots, list):
                raise HistoricalTestInputError("historical_test_input_collection_invalid")
            paths = [
                Path(str(root.get("absolute_path") or ""))
                for root in roots
                if isinstance(root, dict)
            ]
            if len(paths) != len(roots):
                raise HistoricalTestInputError("historical_test_input_collection_invalid")
            for path in paths:
                if not path.is_absolute() or not _within(path.resolve(), self.fixture_root):
                    raise HistoricalTestInputError(
                        "historical_test_input_collection_escape"
                    )
                if path.exists():
                    if path.is_symlink() or _has_symlink_component(path):
                        raise HistoricalTestInputError(
                            "historical_test_input_collection_symlink"
                        )
                    _assert_read_only(
                        path, code="historical_test_input_fixture_not_read_only"
                    )
                for child in _walk_files(path):
                    _assert_read_only(
                        child, code="historical_test_input_file_not_read_only"
                    )
            result[row["name"]] = _snapshot_paths(paths)
        result[EXTERNAL_VERIFICATION_KEY] = _snapshot_paths([verification_root])
        return result

    def verify_protected_collections(self) -> None:
        current = self.snapshot()
        current.pop(EXTERNAL_VERIFICATION_KEY, None)
        expected_rows = self.payload.get("protected_collections")
        if not isinstance(expected_rows, list):
            raise HistoricalTestInputError("historical_test_input_collections_invalid")
        expected = {
            str(row["name"]): {
                key: row[key]
                for key in ("roots", "files", "file_count", "content_sha256")
            }
            for row in expected_rows
            if isinstance(row, dict)
            and isinstance(row.get("name"), str)
            and all(key in row for key in ("roots", "files", "file_count", "content_sha256"))
        }
        if len(expected) != len(expected_rows):
            raise HistoricalTestInputError("historical_test_input_collection_invalid")
        if set(current) != set(expected):
            raise HistoricalTestInputError("historical_test_input_collection_set_mismatch")
        for name in current:
            if current[name] != expected[name]:
                raise HistoricalTestInputError(
                    f"historical_test_input_collection_drift:{name}"
                )


def capture_manifest(
    *,
    historical_manifest_root: Path,
    runtime_data_root: Path,
    formal_surface_root: Path,
    historical_manifest_path: Path,
    expected_manifest_sha256: str,
    expected_manifest_content_sha256: str,
    expected_replay_input_sha256: Mapping[str, str],
    fixture_root: Path | None = None,
    output_root: Path | None = None,
    source_root: Path | None = None,
    component_registry_path: Path | None = None,
    components_path: Path | None = None,
) -> dict[str, Any]:
    if fixture_root is None:
        fixture_root = output_root
    elif (
        output_root is not None
        and fixture_root.expanduser().resolve() != output_root.expanduser().resolve()
    ):
        raise HistoricalTestInputError("historical_test_input_fixture_root_conflict")
    if fixture_root is None:
        raise HistoricalTestInputError("historical_test_input_fixture_root_required")
    if (
        component_registry_path is not None
        and components_path is not None
        and component_registry_path.expanduser().resolve()
        != components_path.expanduser().resolve()
    ):
        raise HistoricalTestInputError(
            "historical_test_input_components_registry_conflict"
        )
    if component_registry_path is None:
        component_registry_path = components_path
    if source_root is None and component_registry_path is not None:
        registry_hint = component_registry_path.expanduser()
        if (
            registry_hint.is_absolute()
            and registry_hint.name == "components.json"
            and registry_hint.parent.name == "kaoyan-study-intake"
        ):
            source_root = registry_hint.parent.parent.parent
    if source_root is None:
        raise HistoricalTestInputError("historical_test_input_source_root_required")
    fixture_path = _fixture_output_root(fixture_root)
    external_verification = _capture_external_verification(
        source_root=source_root,
        component_registry_path=component_registry_path,
    )
    historical_root = _absolute_directory(
        historical_manifest_root, code="historical_test_input_historical_root_invalid"
    )
    runtime_root = _absolute_directory(
        runtime_data_root, code="historical_test_input_runtime_root_invalid"
    )
    formal_root = _absolute_directory(
        formal_surface_root, code="historical_test_input_formal_root_invalid"
    )
    manifest_path = _absolute_regular_file(
        historical_manifest_path, code="historical_test_input_historical_manifest_invalid"
    )
    if not _within(manifest_path, historical_root):
        raise HistoricalTestInputError("historical_test_input_manifest_outside_root")
    if sha256_file(manifest_path) != expected_manifest_sha256:
        raise HistoricalTestInputError("historical_test_input_expected_manifest_mismatch")
    frozen = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (
        not isinstance(frozen, dict)
        or frozen.get("manifest_sha256") != expected_manifest_content_sha256
    ):
        raise HistoricalTestInputError("historical_test_input_expected_content_mismatch")
    _validate_historical_test_manifest_references(frozen)
    actual_replay = {
        str(item.get("capture_id")): item.get("replay_input_sha256")
        for item in frozen.get("items", [])
        if isinstance(item, dict)
    }
    if dict(expected_replay_input_sha256) != actual_replay or len(actual_replay) != 10:
        raise HistoricalTestInputError("historical_test_input_expected_replay_mismatch")

    path_roles: dict[Path, set[str]] = {}

    def add(path: Path, *roles: str) -> None:
        resolved = _absolute_regular_file(path, code="historical_test_input_file_invalid")
        path_roles.setdefault(resolved, set()).update(roles)

    add(manifest_path, "historical_manifest", "historical_input")
    selection_contract = frozen.get("selection_contract")
    ledger_relative = (
        selection_contract.get("ledger_path")
        if isinstance(selection_contract, dict)
        else None
    )
    if not isinstance(ledger_relative, str):
        raise HistoricalTestInputError("historical_test_input_ledger_binding_missing")
    ledger_path = _safe_relative_path(
        formal_root,
        ledger_relative,
        code="historical_test_input_ledger_path_escape",
    )
    add(ledger_path, "historical_ledger", "historical_input", "formal_surface")

    for item in frozen.get("items", []):
        if not isinstance(item, dict):
            raise HistoricalTestInputError("historical_test_input_item_invalid")
        v1 = item.get("v1")
        adoption = item.get("baseline_adoption")
        if not isinstance(v1, dict) or not isinstance(adoption, dict):
            raise HistoricalTestInputError("historical_test_input_runtime_binding_missing")
        for role, relative in (
            ("historical_package", v1.get("package_path")),
            ("historical_run_receipt", v1.get("run_receipt_path")),
            ("historical_adoption", adoption.get("path")),
        ):
            if not isinstance(relative, str):
                raise HistoricalTestInputError("historical_test_input_runtime_binding_missing")
            add(
                _safe_relative_path(
                    runtime_root,
                    relative,
                    code="historical_test_input_runtime_path_escape",
                ),
                role,
                "historical_input",
                "protected_runtime",
            )
        replay = item.get("replay_input")
        source = replay.get("source_bundle") if isinstance(replay, dict) else None
        if isinstance(source, dict):
            source_path = source.get("manifest_path")
            if not isinstance(source_path, str):
                raise HistoricalTestInputError("historical_test_input_source_binding_missing")
            add(
                _safe_relative_path(
                    formal_root,
                    source_path,
                    code="historical_test_input_source_path_escape",
                ),
                "historical_source_manifest",
                "historical_input",
            )
            artifacts = source.get("artifacts")
            if not isinstance(artifacts, list):
                raise HistoricalTestInputError("historical_test_input_artifact_binding_missing")
            for artifact in artifacts:
                if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
                    raise HistoricalTestInputError("historical_test_input_artifact_binding_missing")
                add(
                    _safe_relative_path(
                        formal_root,
                        artifact["path"],
                        code="historical_test_input_artifact_path_escape",
                    ),
                    "historical_source_artifact",
                    "historical_input",
                )
        current = item.get("current_formal_target")
        if isinstance(current, dict) and isinstance(current.get("path"), str):
            add(
                _safe_relative_path(
                    formal_root,
                    current["path"],
                    code="historical_test_input_formal_path_escape",
                ),
                "current_formal_target",
                "formal_surface",
            )

    legacy_snapshot_root = (
        runtime_root
        / "private/mcp-authority-snapshots/objects/sha256/58/"
        "5898a690a9dbbd4ec1e36127fbce61312a9ff95239b3f820b92a7b68456f4ee0/"
        "root"
    )
    for relative, role in (
        ("bank/master_bank.csv", "english_legacy_master_bank_snapshot"),
        ("bank/sentence_patterns.md", "english_legacy_sentence_patterns_snapshot"),
        (
            "articles/2026-07-10-2011-english-i-text-4.md",
            "english_legacy_article_snapshot",
        ),
        ("bank/mastered_items.csv", "english_legacy_mastered_items_snapshot"),
    ):
        add(
            (legacy_snapshot_root / relative).resolve(),
            role,
            "historical_input",
        )
    add(
        (
            runtime_root
            / "deployments/three-subject-model-lane-staging-20260809/"
            "artifacts/pre-model-gate/"
            "en-p0-006-legacy-disposition-requirements.json"
        ).resolve(),
        "english_legacy_requirements",
        "historical_input",
    )

    runtime_paths = [
        runtime_root / "state/jobs/math",
        runtime_root / "state/latest/math",
        runtime_root / "state/offers",
        runtime_root / "state/adoptions/math",
        runtime_root / "packages/math",
        runtime_root / "receipts/math",
    ]
    formal_paths = [
        formal_root / "错题知识网络/错题卡",
        ledger_path,
    ]
    latest_paths = [runtime_root / "state/latest/math"]
    for path in runtime_paths:
        for child in _walk_files(path):
            add(child, "protected_runtime")
            if _within(child, latest_paths[0].resolve()):
                add(child, "latest_pointer")
    for path in formal_paths:
        for child in _walk_files(path):
            add(child, "formal_surface")

    rows = [
        _file_row(path, roles)
        for path, roles in sorted(path_roles.items(), key=lambda row: str(row[0]))
    ]
    collections = [
        _collection("protected_runtime", runtime_paths),
        _collection("formal_surface", formal_paths),
        _collection("latest_pointer", latest_paths),
        _collection("historical_manifest", [manifest_path]),
    ]
    descriptor = {
        "schema_version": SCHEMA_VERSION,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "historical_manifest_root": str(historical_root),
        "runtime_data_root": str(runtime_root),
        "formal_surface_root": str(formal_root),
        "verification_root": str(
            fixture_path / VERIFICATION_ROOT_NAME
        ),
        "historical_manifest": {
            "absolute_path": str(manifest_path),
            "byte_count": manifest_path.stat().st_size,
            "sha256": expected_manifest_sha256,
            "manifest_sha256": expected_manifest_content_sha256,
        },
        "expected_manifest_sha256": expected_manifest_sha256,
        "expected_manifest_content_sha256": expected_manifest_content_sha256,
        "expected_replay_input_sha256": dict(sorted(expected_replay_input_sha256.items())),
        "allowed_files": rows,
        EXTERNAL_VERIFICATION_KEY: external_verification,
        "protected_collections": collections,
        "formal_write_count": 0,
        "model_call_count": 0,
    }
    return freeze_fixture(fixture_root, descriptor)


def persist_content_addressed(output_root: Path, value: Mapping[str, Any]) -> Path:
    root = _fixture_output_root(output_root)
    raw = canonical_bytes(value)
    digest = hashlib.sha256(raw).hexdigest()
    path = root / f"{digest}.json"
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
            raise HistoricalTestInputError("historical_test_input_object_collision")
    else:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    path.chmod(0o444)
    if value.get("schema_version") == SCHEMA_VERSION:
        external_rows = value.get(EXTERNAL_VERIFICATION_KEY)
        executable_files: Iterable[Path] = ()
        if isinstance(external_rows, list):
            executable_files = (
                Path(str(row["absolute_path"]))
                for row in external_rows
                if isinstance(row, dict)
                and row.get("executable") is True
                and isinstance(row.get("absolute_path"), str)
            )
        _freeze_fixture_tree(root, executable_files=executable_files)
    return path


def render_shadow_test_config(
    source_root: Path, test_input: HistoricalTestInput
) -> dict[str, Any]:
    """Render source config in memory for the isolated historical shadow lane."""

    root = source_root.expanduser().resolve()
    template_path = root / "config.example.json"
    if template_path.is_symlink() or not template_path.is_file():
        raise HistoricalTestInputError("historical_test_input_config_template_missing")
    try:
        template = json.loads(template_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalTestInputError("historical_test_input_config_template_invalid") from exc

    replacements = {
        "${RELEASE_ROOT}": str(root),
        "${RUNTIME_DATA_ROOT}": str(test_input.runtime_data_root),
    }

    def substitute(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: substitute(nested) for key, nested in value.items()}
        if isinstance(value, list):
            return [substitute(nested) for nested in value]
        if isinstance(value, str):
            result = value
            for marker, replacement in replacements.items():
                result = result.replace(marker, replacement)
            return result
        return value

    config = substitute(template)
    if not isinstance(config, dict) or not isinstance(config.get("math_deep_v2"), dict):
        raise HistoricalTestInputError("historical_test_input_config_template_invalid")
    config["runtime_root"] = str(test_input.runtime_data_root)
    config["adapters"]["math"]["repo_root"] = str(test_input.formal_surface_root)
    config["math_deep_v2"]["mode"] = "shadow"
    # Source-tree tests have no sealed release.json yet.  Omitting the release
    # block exercises the truthful loaded-core-only identity path instead of
    # fabricating a release identity or writing state into the source tree.
    config.pop("release", None)
    return config
