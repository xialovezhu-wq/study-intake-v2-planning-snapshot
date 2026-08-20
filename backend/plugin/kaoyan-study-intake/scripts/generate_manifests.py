from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SCHEMA_ROOT = ROOT.parents[1] / "schemas"
REGISTRY = ROOT / "components.json"
CODEX_CACHEBUSTER_RE = re.compile(
    r"^[^+]+\+codex\.[a-z0-9]+(?:-[a-z0-9]+)*$"
)
COMPONENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
SUBJECTS = ("math", "cs408", "english")
EXTERNAL_RUNTIME_SOURCE_NAMES = frozenset(
    {
        "math_status_script",
        "cs408_status_script",
        "english_events_module",
    }
)
FOCUSED_TOOLS = (
    "get_task_context",
    "read_task_artifact",
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
)
PREFLIGHT_TOOLS = (
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
)
RETIRED_SCHEMAS = {"daily-sol-batch-v1.json"}
REQUIRED_RUNTIME_SCHEMAS = {
    "concurrent-completion-v2.json",
    "cs408-luna-semantic-qualification-v1.json",
    "cs408-scene-admission-receipt-v1.json",
    "cs408-terminal-batch-writer-retirement-pointer-v1.json",
    "cs408-terminal-batch-writer-retirement-receipt-v1.json",
    "cs408-terminal-batch-writer-retirement-rollback-receipt-v1.json",
    "cs408-typed-operations-v1.json",
    "daily-sol-batch-v3.json",
    "dashboard-projection-v4.json",
    "dashboard-projection-v5.json",
    "dashboard-task-detail-v2.json",
    "dispatch-report-v1.json",
    "dispatch-report-v2.json",
    "dispatch-task-detail-v2.json",
    "dispatch-task-event-v2.json",
    "en-p0-006-remediation-gate-v1.json",
    "english-legacy-authorization-expansion-closure-v1.json",
    "english-legacy-batch-authorization-intent-v1.json",
    "english-legacy-batch-authorization-v1.json",
    "english-legacy-disposition-receipt-v3.json",
    "english-legacy-inventory-independent-review-receipt-v1.json",
    "english-legacy-recuration-execution-closure-v1.json",
    "english-legacy-recuration-package-v1.json",
    "english-legacy-recuration-quality-receipt-v1.json",
    "english-legacy-recuration-run-summary-v1.json",
    "english-legacy-recuration-sol-batch-v1.json",
    "english-legacy-recuration-work-item-batch-v1.json",
    "english-legacy-recuration-work-item-v1.json",
    "english-legacy-rolling-authority-checkpoint-v1.json",
    "english-legacy-sol-item-apply-receipt-v1.json",
    "english-legacy-sol-item-failure-receipt-v1.json",
    "english-legacy-sol-item-recovery-receipt-v1.json",
    "english-legacy-sol-item-review-receipt-v1.json",
    "english-legacy-target-authorization-event-v3.json",
    "english-legacy-target-inventory-v3.json",
    "english-quick-flush-intent-v1.json",
    "english-preserved-review-repair-receipt-v1.json",
    "english-preserved-review-repair-rollback-receipt-v1.json",
    "luna-english-legacy-recuration-analysis-v1.json",
    "luna-english-legacy-recuration-critical-review-v1.json",
    "math-capture-evidence-contract-v1.json",
    "math-exact-smoke-capture-apply-receipt-v1.json",
    "math-exact-smoke-execution-authorization-receipt-v1.json",
    "math-exact-smoke-execution-authorization-v1.json",
    "math-exact-smoke-task-binding-v1.json",
    "math-exact-smoke-task-binding-v2.json",
    "mcp-authority-snapshot-receipt-v1.json",
    "mcp-authority-snapshot-v1.json",
    "mcp-read-session-v3.json",
    "mcp-authority-snapshot-receipt-v2.json",
    "mcp-authority-snapshot-v2.json",
    "mcp-read-session-v4.json",
    "mcp-stage-call-receipt-v2.json",
    "sol-mcp-preflight-session-v1.json",
    "sol-mcp-preflight-call-receipt-v1.json",
    "sol-mcp-preflight-terminal-v1.json",
    "validation-console-state-v1.json",
    "stage-authorization-v1.json",
    "stage-terminal-relock-v1.json",
    "validation-campaign-v1.json",
    "promotion-preview-v1.json",
    "validation-console-technical-status-v1.json",
    "model-stage-execution-receipt-v1.json",
    "model-stage-normalization-receipt-v1.json",
    "model-stage-raw-chain-manifest-v1.json",
    "model-stage-raw-chunk-v1.json",
    "model-stage-raw-output-v1.json",
    "preprocess-package-v3.json",
    "producer-dispatch-input-v1.json",
    "producer-dispatch-input-v2.json",
    "production-canary-activation-receipt-v1.json",
    "production-canary-activation-receipt-v2.json",
    "production-canary-concurrency-telemetry-v1.json",
    "production-canary-exclusion-v1.json",
    "production-canary-gate-receipt-v1.json",
    "production-canary-gate-receipt-v2.json",
    "production-canary-preclaim-failure-receipt-v1.json",
    "production-canary-preclaim-failure-receipt-v2.json",
    "production-canary-preclaim-repair-ack-receipt-v2.json",
    "production-canary-queue-entry-v1.json",
    "production-canary-queue-entry-v2.json",
    "production-canary-queue-entry-v3.json",
    "production-canary-review-terminal-receipt-v1.json",
    "production-canary-state-v1.json",
    "production-canary-state-v2.json",
    "production-canary-state-v3.json",
    "production-canary-terminal-index-v2.json",
    "production-canary-terminal-index-v3.json",
    "production-canary-terminal-receipt-v1.json",
    "production-canary-terminal-receipt-v2.json",
    "production-canary-terminal-receipt-v3.json",
    "provider-kernel-probe-receipt-v1.json",
    "provider-process-exit-v1.json",
    "provider-process-identity-v1.json",
    "review-candidate-package-v1.json",
    "review-candidate-terminal-v1.json",
    "sol-commit-receipt-v2.json",
    "sol-review-receipt-v2.json",
    "sol-task-handoff-envelope-v1.json",
    "stage-progress-receipt-v1.json",
    "stale-claim-quarantine-receipt-v1.json",
    "subject-background-luna-batch-archive-v1.json",
    "subject-background-luna-rollover-pointer-v1.json",
    "subject-background-luna-rollover-receipt-v1.json",
    "subject-luna-batch-v2.json",
    "subject-background-luna-recovery-rollback-receipt-v1.json",
    "subject-background-luna-rollover-receipt-v2.json",
    "subject-batch-recovery-supersede-receipt-v1.json",
    "subject-quality-receipt-v2.json",
    "three-subject-canary-activation-receipt-v3.json",
    "three-subject-canary-activation-receipt-v4.json",
    "three-subject-canary-activation-receipt-v5.json",
    "task-process-exit-v1.json",
    "task-process-identity-v1.json",
    "user-sol-authorization-receipt-v2.json",
    "dashboard-multi-agent-v1.json",
    "live-execution-gate-state-v1.json",
    "manual-live-authorization-v1.json",
    "multi-agent-model-contract-v1.json",
    "multi-agent-event-chain-receipt-v1.json",
    "multi-agent-stage-execution-receipt-v1.json",
    "multi-agent-task-receipt-v1.json",
    "orchestration-read-plan-v1.json",
    "processing-binding-v3.json",
    "producer-binding-attestation-v1.json",
    "producer-binding-descriptor-v1.json",
    "read-branch-request-v1.json",
    "read-branch-result-v1.json",
    "read-bundle-v1.json",
    "risk-report-v1.json",
    "sol-handoff-envelope-v1.json",
}


PathLike = str | os.PathLike[str]


class VerificationPaths:
    """Resolve declared source paths to hermetic verification fixtures.

    The registry remains the source of formal path and digest declarations.
    This resolver only changes where verification reads bytes from.  With a
    verification root, an absolute declaration is interpreted relative to
    that root (for example, ``/opt/source/file`` becomes
    ``<root>/opt/source/file``).  Explicit mappings take precedence and may
    map either an individual file or an entire declared directory.
    """

    def __init__(
        self,
        verification_root: PathLike | None = None,
        path_mapping: Mapping[PathLike, PathLike] | None = None,
    ) -> None:
        self.verification_root = (
            Path(verification_root).absolute()
            if verification_root is not None
            else None
        )
        mappings: list[tuple[Path, Path]] = []
        for declared, fixture in (path_mapping or {}).items():
            declared_path = Path(declared)
            fixture_path = Path(fixture)
            if not declared_path.is_absolute():
                declared_path = declared_path.absolute()
            if not fixture_path.is_absolute():
                fixture_path = fixture_path.absolute()
            mappings.append((declared_path, fixture_path))
        self.path_mapping = tuple(
            sorted(mappings, key=lambda item: len(item[0].parts), reverse=True)
        )

    def resolve(self, declared: Path) -> Path:
        declared_path = Path(declared)
        match_path = (
            declared_path if declared_path.is_absolute() else declared_path.absolute()
        )
        for source, fixture in self.path_mapping:
            try:
                relative = match_path.relative_to(source)
            except ValueError:
                continue
            return fixture / relative
        if self.verification_root is None:
            return declared_path
        if match_path.is_absolute():
            return self.verification_root / match_path.relative_to(Path(match_path.anchor))
        return self.verification_root / match_path


def _verification_paths(
    *,
    verification_root: PathLike | None = None,
    path_mapping: Mapping[PathLike, PathLike] | None = None,
    verification_paths: VerificationPaths | None = None,
) -> VerificationPaths:
    if verification_paths is not None:
        if verification_root is not None or path_mapping is not None:
            raise ValueError(
                "verification_paths cannot be combined with verification_root or path_mapping"
            )
        return verification_paths
    return VerificationPaths(
        verification_root=verification_root,
        path_mapping=path_mapping,
    )


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def write_json(path: Path, value: Any, *, check: bool = False) -> None:
    payload = canonical_bytes(value)
    if check:
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise SystemExit(f"generated artifact is stale: {path.relative_to(ROOT)}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def write_bytes(path: Path, payload: bytes, *, check: bool = False) -> None:
    if check:
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise SystemExit(f"generated artifact is stale: {path.relative_to(ROOT)}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def codex_qualified_tool(server_name: str, tool_name: str) -> str:
    return f"mcp__{server_name}__{tool_name}"


def sealed_runtime_binding(mcp: dict[str, Any]) -> dict[str, Any]:
    return {
        "python_executable": mcp["python_executable"],
        "python_flags": list(mcp["python_flags"]),
        "sealed_launcher_path": mcp["sealed_launcher_path"],
        "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
        "release_root": mcp["release_root"],
        "release_id": mcp["release_id"],
        "release_manifest_sha256": mcp["release_manifest_sha256"],
    }


def render_kaoyan_read(mcp: dict[str, Any]) -> bytes:
    runtime = sealed_runtime_binding(mcp)
    quoted = {
        key: shlex.quote(str(value))
        for key, value in runtime.items()
        if key != "python_flags"
    }
    if runtime["python_flags"] != ["-I", "-S"]:
        raise SystemExit("MCP sealed Python flags must be exactly -I -S")
    lines = [
        "#!/bin/sh",
        "set -eu",
        "exec /usr/bin/env -i \\",
        "  PATH=/usr/bin:/bin \\",
        "  PYTHONUTF8=1 \\",
        "  PYTHONDONTWRITEBYTECODE=1 \\",
        "  PYTHONNOUSERSITE=1 \\",
        "  PYTHONSAFEPATH=1 \\",
        f"  {quoted['python_executable']} \\",
        "  -I \\",
        "  -S \\",
        f"  {quoted['sealed_launcher_path']} \\",
        f"  --release-root {quoted['release_root']} \\",
        f"  --expected-release-id {quoted['release_id']} \\",
        "  --expected-release-manifest-sha256 "
        f"{quoted['release_manifest_sha256']} \\",
        "  --mode server \\",
        '  "$@"',
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def sync_runtime_schemas(
    *,
    check: bool = False,
    verification_root: PathLike | None = None,
    path_mapping: Mapping[PathLike, PathLike] | None = None,
    verification_paths: VerificationPaths | None = None,
) -> None:
    paths = _verification_paths(
        verification_root=verification_root,
        path_mapping=path_mapping,
        verification_paths=verification_paths,
    )
    schema_root = ROOT / "schemas"
    runtime_schema_is_staged_source = (
        RUNTIME_SCHEMA_ROOT == ROOT.parents[1] / "schemas"
    )

    def verification_source(name: str) -> Path:
        declared = RUNTIME_SCHEMA_ROOT / name
        return declared if runtime_schema_is_staged_source else paths.resolve(declared)

    existing_shared = {
        path.name
        for path in schema_root.glob("*.json")
        if verification_source(path.name).is_file()
    }
    names = sorted(existing_shared | REQUIRED_RUNTIME_SCHEMAS)
    for name in names:
        source = verification_source(name)
        if source.is_symlink() or not source.is_file():
            raise SystemExit(f"missing runtime Schema: {name}")
        destination = schema_root / name
        payload = source.read_bytes()
        if check:
            if (
                not destination.is_file()
                or destination.is_symlink()
                or destination.read_bytes() != payload
            ):
                raise SystemExit(f"plugin Schema is stale: {name}")
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)


def validate_external_runtime_source(
    source: dict[str, Any],
    *,
    source_name: str,
    verification_root: PathLike | None = None,
    path_mapping: Mapping[PathLike, PathLike] | None = None,
    verification_paths: VerificationPaths | None = None,
) -> None:
    paths = _verification_paths(
        verification_root=verification_root,
        path_mapping=path_mapping,
        verification_paths=verification_paths,
    )
    declared_path = Path(str(source.get("path") or ""))
    path = paths.resolve(declared_path)
    expected_sha256 = source.get("sha256")
    if (
        not declared_path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
        or sha256(path) != expected_sha256
    ):
        raise SystemExit(f"{source_name} identity mismatch")


def validate_mcp_release_contract(
    mcp: dict[str, Any],
    *,
    verification_root: PathLike | None = None,
    path_mapping: Mapping[PathLike, PathLike] | None = None,
    verification_paths: VerificationPaths | None = None,
) -> None:
    paths = _verification_paths(
        verification_root=verification_root,
        path_mapping=path_mapping,
        verification_paths=verification_paths,
    )
    declared_manifest_path = Path(str(mcp.get("release_manifest") or ""))
    release_root_value = mcp.get("release_root")
    manifest_sha256 = mcp.get("release_manifest_sha256")
    declared_release_root = Path(str(release_root_value or ""))
    declared_python_executable = Path(str(mcp.get("python_executable") or ""))
    declared_sealed_launcher_path = Path(
        str(mcp.get("sealed_launcher_path") or "")
    )
    manifest_path = paths.resolve(declared_manifest_path)
    python_executable = paths.resolve(declared_python_executable)
    sealed_launcher_path = paths.resolve(declared_sealed_launcher_path)
    sealed_launcher_sha256 = mcp.get("sealed_launcher_sha256")
    if (
        not declared_manifest_path.is_absolute()
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
        or declared_manifest_path.name != "release.json"
        or not isinstance(release_root_value, str)
        or declared_release_root != declared_manifest_path.parent
        or not isinstance(manifest_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
        or sha256(manifest_path) != manifest_sha256
        or not declared_python_executable.is_absolute()
        or not python_executable.is_file()
        or not os.access(python_executable, os.X_OK)
        or mcp.get("python_flags") != ["-I", "-S"]
    ):
        raise SystemExit("invalid MCP release manifest path")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("invalid MCP release manifest") from exc
    source_files = manifest.get("source_files")
    release_id = manifest.get("release_id")
    if (
        manifest.get("schema_version") != "study-read-mcp-release.v1"
        or not isinstance(source_files, dict)
        or not source_files
        or not isinstance(release_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", release_id) is None
        or declared_release_root.name != release_id
        or sha256_bytes(canonical_bytes(source_files)) != release_id
        or manifest.get("formal_write_count") != 0
    ):
        raise SystemExit("invalid MCP release identity")
    for relative, expected_sha256 in source_files.items():
        relative_path = Path(relative) if isinstance(relative, str) else Path("/")
        source_path = paths.resolve(declared_release_root / relative_path)
        if (
            not isinstance(expected_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or source_path.is_symlink()
            or not source_path.is_file()
            or sha256(source_path) != expected_sha256
        ):
            raise SystemExit("MCP release source binding mismatch")
    policy_path = paths.resolve(
        declared_release_root / "config" / "skill-tool-policy.json"
    )
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("invalid MCP skill tool policy") from exc
    policy_version = policy.get("server_release")
    expected_server_release = f"{policy_version}+sha256.{release_id}"
    sealed_contract = policy.get("sealed_launcher_contract")
    production_profiles = policy.get("production_launcher_profiles")
    subject_luna_servers = policy.get("subject_luna_servers")
    if (
        policy.get("schema_version") != "study-read-mcp-policy.v4"
        or not isinstance(policy_version, str)
        or policy_version != mcp.get("minimum_server_release")
        or source_files.get("config/skill-tool-policy.json") != sha256(policy_path)
        or manifest.get("server_release") != expected_server_release
        or mcp.get("server_release") != expected_server_release
        or mcp.get("release_id") != release_id
        or declared_sealed_launcher_path
        != declared_release_root / "scripts" / "sealed_launcher.py"
        or sealed_launcher_path.is_symlink()
        or not sealed_launcher_path.is_file()
        or not isinstance(sealed_launcher_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", sealed_launcher_sha256) is None
        or sha256(sealed_launcher_path) != sealed_launcher_sha256
        or source_files.get("scripts/sealed_launcher.py")
        != sealed_launcher_sha256
        or sealed_contract
        != {
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
        or production_profiles
        != {
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
        or not isinstance(subject_luna_servers, dict)
        or set(subject_luna_servers) != set(SUBJECTS)
        or policy.get("subject_preflight_tools") != list(PREFLIGHT_TOOLS)
    ):
        raise SystemExit("MCP policy and release version drift")
    launcher_text = sealed_launcher_path.read_text(encoding="utf-8")
    if (
        "sys.path.append(str(dependency_root))" not in launcher_text
        or "unlike site.addsitedir" not in launcher_text
        or '        "-I",\n        "-S",' not in launcher_text
    ):
        raise SystemExit("MCP sealed launcher dependency isolation drift")
    for subject in SUBJECTS:
        if subject_luna_servers[subject] != {
            "server": f"kaoyan_{subject}_read",
            "launcher_mode": "subject-server",
            "subject": subject,
            "skill": f"background-{subject}-processing",
            "read_session_schema": "study-read-mcp-read-session.v4",
        }:
            raise SystemExit("MCP subject launcher policy drift")


def validate_registry_contract(
    registry: dict[str, Any],
    *,
    verification_root: PathLike | None = None,
    path_mapping: Mapping[PathLike, PathLike] | None = None,
    verification_paths: VerificationPaths | None = None,
) -> None:
    paths = _verification_paths(
        verification_root=verification_root,
        path_mapping=path_mapping,
        verification_paths=verification_paths,
    )
    present_retired = sorted(
        name for name in RETIRED_SCHEMAS if (ROOT / "schemas" / name).exists()
    )
    if present_retired:
        raise SystemExit(
            "retired plugin schemas present: " + ",".join(present_retired)
        )
    plugin = registry.get("plugin")
    if not isinstance(plugin, dict):
        raise SystemExit("missing plugin component registry")
    version = plugin.get("version")
    if not isinstance(version, str) or CODEX_CACHEBUSTER_RE.fullmatch(version) is None:
        raise SystemExit("plugin version must use one +codex.<cachebuster> suffix")

    external_sources = registry.get("external_runtime_sources")
    if (
        not isinstance(external_sources, dict)
        or set(external_sources) != EXTERNAL_RUNTIME_SOURCE_NAMES
    ):
        raise SystemExit("external runtime source registry invalid")
    for source_name in sorted(EXTERNAL_RUNTIME_SOURCE_NAMES):
        source = external_sources[source_name]
        if not isinstance(source, dict) or set(source) != {"path", "sha256"}:
            raise SystemExit(f"{source_name} registry invalid")
        validate_external_runtime_source(
            source,
            source_name=source_name,
            verification_paths=paths,
        )
    foreground_contracts = registry.get("foreground_capture_contracts")
    if (
        not isinstance(foreground_contracts, dict)
        or set(foreground_contracts) != set(SUBJECTS)
    ):
        raise SystemExit("foreground Capture contract registry invalid")
    source_name_by_subject = {
        "math": "math_status_script",
        "cs408": "cs408_status_script",
        "english": "english_events_module",
    }
    for subject in SUBJECTS:
        binding = foreground_contracts.get(subject)
        if (
            not isinstance(binding, dict)
            or set(binding) != {
                "descriptor_path",
                "descriptor_sha256",
                "attestation_required_after",
                "producer_source_closure_sha256",
                "foreground_skill_sha256",
            }
        ):
            raise SystemExit(f"foreground Capture contract invalid: {subject}")
        declared_path = Path(str(binding.get("descriptor_path") or ""))
        descriptor_path = paths.resolve(declared_path)
        if (
            not declared_path.is_absolute()
            or descriptor_path.is_symlink()
            or not descriptor_path.is_file()
            or sha256(descriptor_path) != binding.get("descriptor_sha256")
        ):
            raise SystemExit(f"foreground descriptor identity mismatch: {subject}")
        try:
            descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SystemExit(f"foreground descriptor invalid: {subject}") from exc
        producer = descriptor.get("producer") if isinstance(descriptor, dict) else None
        skill = descriptor.get("foreground_skill") if isinstance(descriptor, dict) else None
        source_files = producer.get("source_files") if isinstance(producer, dict) else None
        expected_source = external_sources[source_name_by_subject[subject]]
        if (
            descriptor.get("schema_version") != "producer_binding_descriptor_v1"
            or descriptor.get("subject") != subject
            or descriptor.get("attestation_required_after")
            != binding.get("attestation_required_after")
            or descriptor.get("formal_write_count") != 0
            or not isinstance(producer, dict)
            or producer.get("source_closure_sha256")
            != binding.get("producer_source_closure_sha256")
            or not isinstance(source_files, list)
            or not any(
                isinstance(row, dict)
                and row.get("path") == expected_source["path"]
                and row.get("sha256") == expected_source["sha256"]
                for row in source_files
            )
            or not isinstance(skill, dict)
            or skill.get("installed_sha256")
            != binding.get("foreground_skill_sha256")
            or skill.get("authoritative_sha256")
            != binding.get("foreground_skill_sha256")
        ):
            raise SystemExit(f"foreground descriptor closure mismatch: {subject}")
        for prefix in ("authoritative", "installed"):
            declared_skill = Path(str(skill.get(f"{prefix}_path") or ""))
            fixture_skill = paths.resolve(declared_skill)
            if (
                not declared_skill.is_absolute()
                or fixture_skill.is_symlink()
                or not fixture_skill.is_file()
                or sha256(fixture_skill) != skill.get(f"{prefix}_sha256")
            ):
                raise SystemExit(
                    f"foreground Skill parity mismatch: {subject}:{prefix}"
                )
        for row in source_files:
            declared_source = Path(str(row.get("path") or ""))
            fixture_source = paths.resolve(declared_source)
            if (
                not declared_source.is_absolute()
                or fixture_source.is_symlink()
                or not fixture_source.is_file()
                or sha256(fixture_source) != row.get("sha256")
            ):
                raise SystemExit(
                    f"foreground Producer closure mismatch: {subject}"
                )
        capture_contract = descriptor.get("capture_contract")
        contract_files = (
            capture_contract.get("files")
            if isinstance(capture_contract, dict)
            else None
        )
        if not isinstance(contract_files, list) or not contract_files:
            raise SystemExit(f"foreground Capture contract missing: {subject}")
        for row in contract_files:
            declared_contract = Path(str((row or {}).get("path") or ""))
            fixture_contract = paths.resolve(declared_contract)
            if (
                not declared_contract.is_absolute()
                or fixture_contract.is_symlink()
                or not fixture_contract.is_file()
                or sha256(fixture_contract) != row.get("sha256")
            ):
                raise SystemExit(
                    f"foreground Capture contract mismatch: {subject}"
                )

    mcp = registry.get("mcp")
    if not isinstance(mcp, dict):
        raise SystemExit("missing MCP component registry")
    validate_mcp_release_contract(mcp, verification_paths=paths)
    ordinary_servers = mcp.get("ordinary_servers")
    luna_servers = mcp.get("luna_servers")
    if (
        not isinstance(ordinary_servers, dict)
        or set(ordinary_servers) != set(SUBJECTS)
        or not isinstance(luna_servers, dict)
        or set(luna_servers) != set(SUBJECTS)
        or mcp.get("focused_tool_names") != list(FOCUSED_TOOLS)
    ):
        raise SystemExit("invalid subject MCP server registry")
    seen_servers: set[str] = set()
    for subject in SUBJECTS:
        ordinary = ordinary_servers.get(subject)
        luna = luna_servers.get(subject)
        if not isinstance(ordinary, dict) or set(ordinary) != {
            "server_name", "subject", "tool_name"
        }:
            raise SystemExit(f"invalid ordinary MCP server: {subject}")
        if not isinstance(luna, dict) or set(luna) != {"server_name", "launch_mode"}:
            raise SystemExit(f"invalid Luna MCP server: {subject}")
        server_name = ordinary.get("server_name")
        tool_name = ordinary.get("tool_name")
        if (
            ordinary.get("subject") != subject
            or not isinstance(server_name, str)
            or COMPONENT_NAME_RE.fullmatch(server_name) is None
            or server_name != f"kaoyan_{subject}_read"
            or server_name in seen_servers
            or not isinstance(tool_name, str)
            or COMPONENT_NAME_RE.fullmatch(tool_name) is None
            or tool_name != f"{subject}_read_bundle"
            or luna.get("server_name") != server_name
            or luna.get("launch_mode") != "subject-server"
        ):
            raise SystemExit(f"subject MCP binding mismatch: {subject}")
        seen_servers.add(server_name)
    preflight = registry.get("sol_mcp_preflight")
    if (
        not isinstance(preflight, dict)
        or set(preflight)
        != {
            "purpose",
            "launcher_mode",
            "tool_policy",
            "enabled_tools",
            "session_schema",
            "call_receipt_schema",
            "terminal_schema",
            "write_call_count",
            "formal_write_count",
        }
        or preflight.get("purpose") != "infrastructure_preflight"
        or preflight.get("launcher_mode") != "preflight-server"
        or preflight.get("enabled_tools") != list(PREFLIGHT_TOOLS)
        or preflight.get("session_schema")
        != "study-read-mcp-infrastructure-preflight-session.v1"
        or preflight.get("call_receipt_schema")
        != "sol-mcp-preflight-call-receipt-v1"
        or preflight.get("terminal_schema")
        != "sol-mcp-preflight-terminal-v1"
        or preflight.get("write_call_count") != 0
        or preflight.get("formal_write_count") != 0
    ):
        raise SystemExit("invalid Sol MCP preflight registry")
    preflight_policy = Path(str(preflight.get("tool_policy") or ""))
    if (
        preflight_policy.is_absolute()
        or ".." in preflight_policy.parts
        or (ROOT / preflight_policy).is_symlink()
        or not (ROOT / preflight_policy).is_file()
    ):
        raise SystemExit("invalid Sol MCP preflight tool policy")
    legacy_server_names = mcp.get("legacy_server_names")
    if (
        not isinstance(legacy_server_names, list)
        or not legacy_server_names
        or any(
            not isinstance(name, str) or COMPONENT_NAME_RE.fullmatch(name) is None
            for name in legacy_server_names
        )
        or len(legacy_server_names) != len(set(legacy_server_names))
        or seen_servers.intersection(legacy_server_names)
    ):
        raise SystemExit("invalid legacy MCP server names")

    skills = registry.get("skills")
    bindings = mcp.get("ordinary_skill_tools")
    if not isinstance(skills, dict) or not isinstance(bindings, dict) or not bindings:
        raise SystemExit("missing ordinary Skill MCP bindings")
    for skill_id, binding in bindings.items():
        if skill_id not in skills:
            raise SystemExit(f"ordinary MCP binding names unknown Skill: {skill_id}")
        if not isinstance(binding, dict) or set(binding) != {"subject", "tool_name"}:
            raise SystemExit(f"invalid MCP tool binding: {skill_id}")
        subject = binding.get("subject")
        tool_name = binding.get("tool_name")
        if (
            subject not in SUBJECTS
            or not isinstance(tool_name, str)
            or tool_name != ordinary_servers[subject]["tool_name"]
        ):
            raise SystemExit(f"invalid MCP tool binding: {skill_id}")
        skill_text = (ROOT / "skills" / skill_id / "SKILL.md").read_text(
            encoding="utf-8"
        )
        server_name = ordinary_servers[subject]["server_name"]
        required_tool = codex_qualified_tool(server_name, tool_name)
        required_tokens = [
            required_tool,
            f"required_mcp_tool={required_tool}",
            "fallback_reason=tool_snapshot_missing",
            f"Skill version `{skills[skill_id]}`",
        ]
        required_tokens.extend(
            codex_qualified_tool(legacy_name, tool_name)
            for legacy_name in legacy_server_names
        )
        missing = [token for token in required_tokens if token not in skill_text]
        if missing:
            raise SystemExit(
                f"ordinary Skill MCP namespace contract incomplete: {skill_id}: {missing}"
            )
    multi_agent = registry.get("multi_agent")
    if (
        not isinstance(multi_agent, dict)
        or multi_agent.get("schema_version")
        != "study-intake-multi-agent-component-registry-v1"
        or multi_agent.get("logical_branch_limit") is not None
        or multi_agent.get("physical_concurrency_mode") != "dynamic"
        or multi_agent.get("overflow_policy") != "queue_in_waves"
        or multi_agent.get("drop_policy") != "never"
        or multi_agent.get("formal_write_count") != 0
    ):
        raise SystemExit("invalid multi-agent component registry")
    orchestrate = multi_agent.get("orchestrate_skill")
    if (
        not isinstance(orchestrate, dict)
        or orchestrate.get("id") != "multi-agent-read-orchestrate"
        or orchestrate.get("version") != skills.get("multi-agent-read-orchestrate")
        or orchestrate.get("path")
        != "skills/multi-agent-read-orchestrate/SKILL.md"
    ):
        raise SystemExit("invalid release-bound Orchestrate Skill")
    expected_roles = {
        "orchestrator": ("gpt-5.6-terra", "ultra", True, False),
        "reader": ("gpt-5.6-luna", "max", False, False),
        "critical_reviewer": ("gpt-5.6-terra", "ultra", False, True),
    }
    roles = multi_agent.get("roles")
    if not isinstance(roles, dict) or set(roles) != set(expected_roles):
        raise SystemExit("invalid multi-agent role registry")
    for role, (model, effort, agents_enabled, fresh_context) in expected_roles.items():
        value = roles.get(role)
        expected_keys = {
            "model", "reasoning_effort", "agents_enabled",
            "agent_config", "tool_policy",
        }
        if fresh_context:
            expected_keys.add("fresh_context")
        if (
            not isinstance(value, dict)
            or set(value) != expected_keys
            or value.get("model") != model
            or value.get("reasoning_effort") != effort
            or value.get("agents_enabled") is not agents_enabled
            or (fresh_context and value.get("fresh_context") is not True)
        ):
            raise SystemExit(f"invalid multi-agent role: {role}")
        for field in ("agent_config", "tool_policy"):
            relative = Path(str(value.get(field) or ""))
            asset = ROOT / relative
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or asset.is_symlink()
                or not asset.is_file()
            ):
                raise SystemExit(f"missing multi-agent asset: {role}:{field}")
    fixture_contracts = registry.get("fixture_contracts")
    if (
        not isinstance(fixture_contracts, dict)
        or set(fixture_contracts) != set(SUBJECTS)
    ):
        raise SystemExit("fixture contract registry invalid")
    source_root = ROOT.parents[1]
    for subject, binding in fixture_contracts.items():
        if (
            not isinstance(binding, dict)
            or set(binding) != {"profile", "path", "sha256"}
            or not isinstance(binding.get("profile"), str)
            or not binding["profile"]
        ):
            raise SystemExit(f"fixture contract invalid: {subject}")
        relative = Path(str(binding.get("path") or ""))
        path = source_root / relative
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or path.is_symlink()
            or not path.is_file()
            or sha256(path) != binding.get("sha256")
        ):
            raise SystemExit(f"fixture contract identity mismatch: {subject}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic plugin manifests and component lock."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify generated artifacts and copied runtime Schemas without writing",
    )
    parser.add_argument(
        "--verification-root",
        type=Path,
        help=(
            "replace the filesystem root for external verification reads"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    verification_paths = (
        VerificationPaths(verification_root=args.verification_root)
        if args.verification_root is not None
        else None
    )
    sync_runtime_schemas(
        check=args.check,
        verification_paths=verification_paths,
    )
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    if registry.get("schema_version") != "kaoyan-study-intake-components.v1":
        raise SystemExit("invalid component registry schema")
    validate_registry_contract(registry, verification_paths=verification_paths)
    write_bytes(
        ROOT / "bin" / "kaoyan-read",
        render_kaoyan_read(registry["mcp"]),
        check=args.check,
    )
    plugin = registry["plugin"]
    skills = registry["skills"]
    for name in skills:
        skill_file = ROOT / "skills" / name / "SKILL.md"
        if not skill_file.is_file():
            raise SystemExit(f"missing skill: {name}")
        frontmatter = skill_file.read_text(encoding="utf-8").split("---", 2)
        if len(frontmatter) < 3 or f"name: {name}" not in frontmatter[1]:
            raise SystemExit(f"skill name mismatch: {name}")

    portable_plugin = {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": plugin["name"],
        "version": plugin["version"],
        "description": plugin["description"],
        "author": plugin["author"],
        "keywords": plugin["keywords"],
    }
    portable_mcp = {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
        "mcpServers": {
            server["server_name"]: {
                "type": "stdio",
                "command": registry["mcp"]["launcher"],
                "args": [
                    "--profile", registry["mcp"]["profile"],
                    "--subjects", subject,
                ],
                "env": {
                    "PYTHONUTF8": "1",
                    "PYTHONDONTWRITEBYTECODE": "1"
                }
            }
            for subject, server in sorted(
                registry["mcp"]["ordinary_servers"].items()
            )
        },
    }
    codex_plugin = {
        "name": plugin["name"],
        "version": plugin["version"],
        "description": plugin["description"],
        "author": plugin["author"],
        "skills": "./skills/",
        "mcpServers": "./.mcp.json",
        "interface": {
            "displayName": "Kaoyan Study Intake",
            "shortDescription": "三科隔离 MCP 与 Luna 候选预处理",
            "longDescription": "为数学、408、英语提供独立只读 MCP、版本化 Skill、并行 proposal-only Luna 处理和全局串行 Sol 合同。",
            "developerName": plugin["author"]["name"],
            "category": "Productivity",
            "capabilities": [
                "Subject-isolated read-only MCP",
                "Versioned Skills",
                "Parallel proposal-only Luna preprocessing",
                "Globally serialized Sol handoff",
            ],
            "defaultPrompt": [
                "检查三科只读证据路由状态。",
                "生成数学日终复盘。",
                "开始指定日期的 408 正式入库准备。"
            ]
        }
    }
    codex_mcp = {
        "mcpServers": {
            server["server_name"]: {
                "command": registry["mcp"]["launcher"],
                "args": [
                    "--profile", registry["mcp"]["profile"],
                    "--subjects", subject,
                ],
                "env": {"PYTHONUTF8": "1", "PYTHONDONTWRITEBYTECODE": "1"},
                "tool_timeout_sec": 10
            }
            for subject, server in sorted(
                registry["mcp"]["ordinary_servers"].items()
            )
        }
    }

    write_json(ROOT / "plugin.json", portable_plugin, check=args.check)
    write_json(ROOT / "mcp.json", portable_mcp, check=args.check)
    write_json(
        ROOT / ".codex-plugin" / "plugin.json", codex_plugin, check=args.check
    )
    write_json(ROOT / ".mcp.json", codex_mcp, check=args.check)

    skill_lock = {
        name: {"version": version, "sha256": sha256(ROOT / "skills" / name / "SKILL.md")}
        for name, version in sorted(skills.items())
    }
    schema_lock = {
        path.name: sha256(path)
        for path in sorted((ROOT / "schemas").glob("*.json"))
    }
    reference_lock = {
        path.name: sha256(path)
        for path in sorted((ROOT / "references").glob("*.md"))
    }
    agent_lock = {
        path.relative_to(ROOT).as_posix(): sha256(path)
        for path in sorted((ROOT / "agents").glob("*"))
        if path.is_file() and not path.is_symlink()
    }
    lock = {
        "schema_version": "kaoyan-study-intake-component-lock.v1",
        "plugin_name": plugin["name"],
        "plugin_version": plugin["version"],
        "registry_sha256": sha256(REGISTRY),
        "external_runtime_sources": registry["external_runtime_sources"],
        "foreground_capture_contracts": registry[
            "foreground_capture_contracts"
        ],
        "fixture_contracts": registry["fixture_contracts"],
        "minimum_mcp_server_release": registry["mcp"]["minimum_server_release"],
        "mcp_server_names": {
            subject: server["server_name"]
            for subject, server in sorted(
                registry["mcp"]["ordinary_servers"].items()
            )
        },
        "luna_mcp_servers": {
            subject: {
                "server_name": server["server_name"],
                "launch_mode": server["launch_mode"],
                "enabled_tools": list(FOCUSED_TOOLS),
                "read_session_schema": "study-read-mcp-read-session.v4",
            }
            for subject, server in sorted(
                registry["mcp"]["luna_servers"].items()
            )
        },
        "sol_mcp_preflight": {
            **registry["sol_mcp_preflight"],
            "tool_policy_sha256": sha256(
                ROOT / registry["sol_mcp_preflight"]["tool_policy"]
            ),
        },
        "mcp_server_release": registry["mcp"]["server_release"],
        "mcp_release_root": registry["mcp"]["release_root"],
        "mcp_release_id": registry["mcp"]["release_id"],
        "mcp_release_manifest_sha256": registry["mcp"][
            "release_manifest_sha256"
        ],
        "mcp_sealed_runtime": sealed_runtime_binding(registry["mcp"]),
        "launcher_sha256": sha256(ROOT / "bin" / "kaoyan-read"),
        "portable_plugin_sha256": sha256(ROOT / "plugin.json"),
        "portable_mcp_sha256": sha256(ROOT / "mcp.json"),
        "codex_plugin_sha256": sha256(ROOT / ".codex-plugin" / "plugin.json"),
        "codex_mcp_sha256": sha256(ROOT / ".mcp.json"),
        "legacy_mcp_server_names": registry["mcp"]["legacy_server_names"],
        "ordinary_skill_tools": {
            skill_id: {
                "subject": binding["subject"],
                "server_name": registry["mcp"]["ordinary_servers"]
                [binding["subject"]]["server_name"],
                "tool_name": binding["tool_name"],
                "codex_qualified_name": codex_qualified_tool(
                    registry["mcp"]["ordinary_servers"]
                    [binding["subject"]]["server_name"],
                    binding["tool_name"],
                ),
            }
            for skill_id, binding in sorted(
                registry["mcp"]["ordinary_skill_tools"].items()
            )
        },
        "skills": skill_lock,
        "schemas": schema_lock,
        "references": reference_lock,
        "multi_agent": registry["multi_agent"],
        "agent_configs": agent_lock,
        "formal_write_count": 0
    }
    write_json(ROOT / "component-lock.json", lock, check=args.check)


if __name__ == "__main__":
    main()
