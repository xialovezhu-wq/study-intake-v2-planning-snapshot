"""Verify and shadow-close three foreground Study Intake contracts."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from skill_binding_contract import bind_candidate_producer_attestations


SUBJECTS = ("math", "cs408", "english")
DESCRIPTOR_RELATIVE_PATHS = {
    "math": Path("数学一回滚复习系统/schema/producer-binding-v1.json"),
    "cs408": Path("schema/producer-binding-v1.json"),
    "english": Path("schema/english_pipeline/producer-binding-v1.json"),
}
REPO_ROOTS = {
    "math": Path("/Users/xiazhibin/Documents/kaoyan-math"),
    "cs408": Path("/Users/xiazhibin/Documents/kaoyan-408"),
    "english": Path("/Users/xiazhibin/Documents/kaoyan-english"),
}
MCP_SERVERS = {
    "math": "kaoyan_math_read",
    "cs408": "kaoyan_cs408_read",
    "english": "kaoyan_english_read",
}
MCP_TOOLS = [
    "get_task_context",
    "read_task_artifact",
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
]
EXPECTED_PIPELINE = [
    "producer_validation",
    "capture_event",
    "capture_receipt",
    "producer_attestation",
    "scanner_binding",
    "processing_binding_v3",
    "frozen_task_identity",
    "pre_model_tripwire",
]


class AlignmentError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShadowCandidate:
    subject: str
    capture_id: str
    recorded_at: str
    input_binding: dict[str, Any]
    model_input: dict[str, Any]
    input_fingerprint: str


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


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def atomic_write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def directory_manifest(root: Path) -> dict[str, Any]:
    if root.is_symlink() or not root.is_dir():
        raise AlignmentError(f"unsafe Skill root: {root}")
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.is_symlink():
            raise AlignmentError(f"symlink in Skill root: {path}")
        files.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "byte_count": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return {
        "root": str(root.resolve()),
        "file_count": len(files),
        "total_bytes": sum(row["byte_count"] for row in files),
        "files_sha256": sha256_value(files),
        "files": files,
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AlignmentError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AlignmentError(f"not an object: {path}")
    return value


def _validate_descriptor(subject: str) -> tuple[Path, dict[str, Any]]:
    descriptor_path = REPO_ROOTS[subject] / DESCRIPTOR_RELATIVE_PATHS[subject]
    descriptor = _load_json(descriptor_path)
    core = {
        key: value
        for key, value in descriptor.items()
        if key != "descriptor_content_sha256"
    }
    if (
        descriptor.get("schema_version") != "producer_binding_descriptor_v1"
        or descriptor.get("subject") != subject
        or descriptor.get("formal_write_count") != 0
        or descriptor.get("descriptor_content_sha256") != sha256_value(core)
    ):
        raise AlignmentError(f"descriptor closure mismatch: {subject}")
    skill = descriptor.get("foreground_skill")
    if not isinstance(skill, Mapping):
        raise AlignmentError(f"foreground Skill missing: {subject}")
    authoritative = Path(str(skill.get("authoritative_path") or ""))
    installed = Path(str(skill.get("installed_path") or ""))
    if (
        not authoritative.is_absolute()
        or not installed.is_absolute()
        or authoritative.is_symlink()
        or installed.is_symlink()
        or not authoritative.is_file()
        or not installed.is_file()
        or sha256_file(authoritative) != skill.get("authoritative_sha256")
        or sha256_file(installed) != skill.get("installed_sha256")
        or authoritative.read_bytes() != installed.read_bytes()
    ):
        raise AlignmentError(f"foreground Skill parity mismatch: {subject}")
    authoritative_manifest = directory_manifest(authoritative.parent)
    installed_manifest = directory_manifest(installed.parent)
    if (
        authoritative_manifest["files_sha256"]
        != installed_manifest["files_sha256"]
        or authoritative_manifest["file_count"]
        != installed_manifest["file_count"]
    ):
        raise AlignmentError(f"foreground Skill directory drift: {subject}")
    producer = descriptor.get("producer")
    source_files = producer.get("source_files") if isinstance(producer, Mapping) else None
    if not isinstance(source_files, list) or not source_files:
        raise AlignmentError(f"Producer closure missing: {subject}")
    normalized_sources: list[dict[str, str]] = []
    for row in source_files:
        if not isinstance(row, Mapping) or set(row) != {"path", "sha256"}:
            raise AlignmentError(f"Producer closure invalid: {subject}")
        path = Path(str(row["path"]))
        if path.is_symlink() or not path.is_file() or sha256_file(path) != row["sha256"]:
            raise AlignmentError(f"Producer source drift: {subject}:{path}")
        normalized_sources.append({"path": str(path), "sha256": str(row["sha256"])})
    if producer.get("source_closure_sha256") != sha256_value(normalized_sources):
        raise AlignmentError(f"Producer source closure hash drift: {subject}")
    contract = descriptor.get("capture_contract")
    contract_files = contract.get("files") if isinstance(contract, Mapping) else None
    if not isinstance(contract_files, list) or not contract_files:
        raise AlignmentError(f"Capture contract missing: {subject}")
    normalized_contracts: list[dict[str, str]] = []
    for row in contract_files:
        if not isinstance(row, Mapping) or set(row) != {"path", "sha256"}:
            raise AlignmentError(f"Capture contract invalid: {subject}")
        path = Path(str(row["path"]))
        if path.is_symlink() or not path.is_file() or sha256_file(path) != row["sha256"]:
            raise AlignmentError(f"Capture contract drift: {subject}:{path}")
        normalized_contracts.append({"path": str(path), "sha256": str(row["sha256"])})
    if contract.get("files_sha256") != sha256_value(normalized_contracts):
        raise AlignmentError(f"Capture contract closure hash drift: {subject}")
    descriptor["_skill_manifests"] = {
        "authoritative": authoritative_manifest,
        "installed": installed_manifest,
    }
    return descriptor_path, descriptor


def _validate_vector(
    subject: str, component_lock: Mapping[str, Any], source_root: Path
) -> tuple[Path, dict[str, Any]]:
    binding = component_lock.get("fixture_contracts", {}).get(subject)
    if not isinstance(binding, Mapping):
        raise AlignmentError(f"fixture binding missing: {subject}")
    path = source_root / str(binding.get("path") or "")
    vector = _load_json(path)
    if (
        path.is_symlink()
        or sha256_file(path) != binding.get("sha256")
        or vector.get("schema_version")
        != "study-intake-subject-contract-vector-v2"
        or vector.get("subject") != subject
        or vector.get("expected_pipeline") != EXPECTED_PIPELINE
        or vector.get("formal_write_count") != 0
        or vector.get("model_call_count") != 0
        or vector.get("provider_request_count") != 0
        or vector.get("mcp_call_count") != 0
        or vector.get("mcp_server") != MCP_SERVERS[subject]
        or vector.get("mcp_tools") != MCP_TOOLS
    ):
        raise AlignmentError(f"fixture contract mismatch: {subject}")
    return path, vector


def _attestation_helper(descriptor: Mapping[str, Any]) -> Path:
    source_files = descriptor["producer"]["source_files"]
    for row in source_files:
        name = Path(str(row["path"])).name
        if "producer_binding_attestation" in name:
            return Path(str(row["path"]))
    raise AlignmentError("Producer attestation helper missing")


def _publish_shadow_attestation(
    *,
    subject: str,
    descriptor_path: Path,
    descriptor: Mapping[str, Any],
    shadow_repo: Path,
    capture_id: str,
    content_sha256: str,
    recorded_at: str,
) -> dict[str, Any]:
    helper_path = _attestation_helper(descriptor)
    spec = importlib.util.spec_from_file_location(
        f"shadow_attestation_{subject}", helper_path
    )
    if spec is None or spec.loader is None:
        raise AlignmentError(f"cannot load attestation helper: {subject}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.publish_attestation(
        descriptor_path=descriptor_path,
        repo_root=shadow_repo,
        subject=subject,
        capture_id=capture_id,
        capture_content_sha256=content_sha256,
        recorded_at=recorded_at,
    )
    if (
        result.get("status") != "attested"
        or result.get("formal_write_count") != 0
        or not isinstance(result.get("attestation_sha256"), str)
    ):
        raise AlignmentError(f"shadow attestation failed: {subject}")
    return dict(result)


def _shadow_candidate(
    subject: str, capture_id: str, content_sha256: str, recorded_at: str
) -> ShadowCandidate:
    if subject == "math":
        input_binding = {
            "original_content_hash": content_sha256,
            "adapter_version": "math-pending-v1",
        }
        model_input: dict[str, Any] = {}
    elif subject == "cs408":
        input_binding = {
            "payload_sha256": content_sha256,
            "adapter_version": "cs408-awaiting-curation-v2",
        }
        model_input = {}
    else:
        input_binding = {
            "capture_event_sha256": {capture_id: content_sha256},
            "adapter_version": "english-microbatch-v1",
        }
        model_input = {
            "batch_events": [
                {"event_id": capture_id, "occurred_at": recorded_at}
            ]
        }
    return ShadowCandidate(
        subject=subject,
        capture_id=capture_id,
        recorded_at=recorded_at,
        input_binding=input_binding,
        model_input=model_input,
        input_fingerprint=sha256_value(input_binding),
    )


def run_alignment(
    *, source_root: Path, component_lock_path: Path, output_root: Path, phase: str
) -> dict[str, Any]:
    if phase not in {"authoring", "build", "deployed"}:
        raise AlignmentError("invalid alignment phase")
    component_lock = _load_json(component_lock_path)
    if component_lock.get("formal_write_count") != 0:
        raise AlignmentError("component lock formal write count is nonzero")
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output_root, 0o700)
    subject_rows: list[dict[str, Any]] = []
    recorded_at = datetime.now(timezone.utc).isoformat()
    for ordinal, subject in enumerate(SUBJECTS, start=1):
        real_descriptor_path, descriptor = _validate_descriptor(subject)
        vector_path, vector = _validate_vector(subject, component_lock, source_root)
        binding = component_lock.get("foreground_capture_contracts", {}).get(subject)
        if (
            not isinstance(binding, Mapping)
            or binding.get("descriptor_path") != str(real_descriptor_path)
            or binding.get("descriptor_sha256") != sha256_file(real_descriptor_path)
            or binding.get("producer_source_closure_sha256")
            != descriptor["producer"]["source_closure_sha256"]
            or binding.get("foreground_skill_sha256")
            != descriptor["foreground_skill"]["installed_sha256"]
        ):
            raise AlignmentError(f"component lock foreground mismatch: {subject}")
        background_id = f"background-{subject}-processing"
        background = component_lock.get("skills", {}).get(background_id)
        mcp = component_lock.get("luna_mcp_servers", {}).get(subject)
        if (
            not isinstance(background, Mapping)
            or not isinstance(mcp, Mapping)
            or mcp.get("server_name") != MCP_SERVERS[subject]
            or mcp.get("enabled_tools") != MCP_TOOLS
            or mcp.get("read_session_schema") != "study-read-mcp-read-session.v4"
        ):
            raise AlignmentError(f"background/MCP closure mismatch: {subject}")

        shadow_repo = output_root / "shadow" / subject / "repo"
        shadow_descriptor = shadow_repo / DESCRIPTOR_RELATIVE_PATHS[subject]
        atomic_write(shadow_descriptor, real_descriptor_path.read_bytes())
        shadow_lock = copy.deepcopy(component_lock)
        shadow_binding = shadow_lock["foreground_capture_contracts"][subject]
        shadow_binding["descriptor_path"] = str(shadow_descriptor)
        shadow_binding["descriptor_sha256"] = sha256_file(shadow_descriptor)
        shadow_lock_path = output_root / "shadow" / subject / "component-lock.json"
        atomic_write(shadow_lock_path, canonical_bytes(shadow_lock))
        capture_id = {
            "math": "MFI-CAP-SHADOW-MATH-0001",
            "cs408": "CAP-SHADOW-CS408-0001",
            "english": "EVT-SHADOW-ENGLISH-0001",
        }[subject]
        content_sha256 = sha256_value(
            {
                "subject": subject,
                "capture_id": capture_id,
                "vector_sha256": sha256_file(vector_path),
                "phase": phase,
            }
        )
        attestation = _publish_shadow_attestation(
            subject=subject,
            descriptor_path=shadow_descriptor,
            descriptor=descriptor,
            shadow_repo=shadow_repo,
            capture_id=capture_id,
            content_sha256=content_sha256,
            recorded_at=recorded_at,
        )
        candidate = _shadow_candidate(
            subject, capture_id, content_sha256, recorded_at
        )
        rebound = bind_candidate_producer_attestations(
            {
                "execution_mode": "fixture",
                "adapters": {subject: {"repo_root": str(shadow_repo)}},
                "processing_plugin": {
                    "component_lock_path": str(shadow_lock_path)
                },
            },
            candidate,
        )
        processing_binding = rebound.input_binding.get("processing_binding_v3")
        if (
            not isinstance(processing_binding, Mapping)
            or processing_binding.get("subject") != subject
            or processing_binding.get("formal_write_count") != 0
            or processing_binding.get("mcp_binding", {}).get("server_name")
            != MCP_SERVERS[subject]
        ):
            raise AlignmentError(f"shadow Scanner binding failed: {subject}")
        skill_manifests = descriptor.pop("_skill_manifests")
        row = {
            "subject": subject,
            "phase": phase,
            "skill_id": descriptor["foreground_skill"]["id"],
            "authoritative_path": descriptor["foreground_skill"][
                "authoritative_path"
            ],
            "authoritative_sha256": descriptor["foreground_skill"][
                "authoritative_sha256"
            ],
            "installed_path": descriptor["foreground_skill"]["installed_path"],
            "installed_sha256": descriptor["foreground_skill"][
                "installed_sha256"
            ],
            "skill_directory_sha256": skill_manifests["installed"][
                "files_sha256"
            ],
            "skill_directory_file_count": skill_manifests["installed"][
                "file_count"
            ],
            "descriptor_path": str(real_descriptor_path),
            "descriptor_sha256": sha256_file(real_descriptor_path),
            "producer_entrypoint": descriptor["producer"]["entrypoint_path"],
            "producer_source_closure_sha256": descriptor["producer"][
                "source_closure_sha256"
            ],
            "capture_contract_id": descriptor["capture_contract"]["contract_id"],
            "capture_contract_files_sha256": descriptor["capture_contract"][
                "files_sha256"
            ],
            "fixture_vector_path": str(vector_path),
            "fixture_vector_sha256": sha256_file(vector_path),
            "fixture_profile": component_lock["fixture_contracts"][subject][
                "profile"
            ],
            "shadow_capture_id": capture_id,
            "shadow_capture_content_sha256": content_sha256,
            "shadow_attestation_path": attestation["attestation_path"],
            "shadow_attestation_sha256": attestation["attestation_sha256"],
            "processing_binding_v3_sha256": processing_binding["binding_sha256"],
            "background_skill_id": background_id,
            "background_skill_version": background["version"],
            "background_skill_sha256": background["sha256"],
            "mcp_server": mcp["server_name"],
            "mcp_release_id": component_lock["mcp_release_id"],
            "mcp_server_release": component_lock["mcp_server_release"],
            "deployed_closure_phase": phase,
            "pre_model_tripwire_reached": True,
            "root_sol_helper_subagent_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_call_count": 0,
            "formal_write_count": 0,
            "status": "passed",
        }
        row_core = dict(row)
        row["receipt_sha256"] = sha256_value(row_core)
        receipt_path = output_root / "receipts" / f"{subject}-{row['receipt_sha256']}.json"
        atomic_write(receipt_path, canonical_bytes(row))
        row["receipt_path"] = str(receipt_path)
        subject_rows.append(row)
    summary_core = {
        "schema_version": "three-subject-skill-alignment-summary-v1",
        "phase": phase,
        "component_lock_path": str(component_lock_path.resolve()),
        "component_lock_sha256": sha256_file(component_lock_path),
        "subjects": subject_rows,
        "root_sol_helper_subagent_count": 0,
        "real_terra_call_count": 0,
        "real_luna_call_count": 0,
        "real_provider_model_request_count": 0,
        "live_capture_created_count": 0,
        "live_capture_consumed_count": 0,
        "formal_write_count": 0,
        "status": "passed",
    }
    summary = {**summary_core, "receipt_sha256": sha256_value(summary_core)}
    summary_path = output_root / f"summary-{phase}-{summary['receipt_sha256']}.json"
    atomic_write(summary_path, canonical_bytes(summary))
    summary["receipt_path"] = str(summary_path)
    return summary
