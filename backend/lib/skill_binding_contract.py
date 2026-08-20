"""Foreground Skill/Producer attestation and processing-binding-v3 closure."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence


SUBJECTS = frozenset({"math", "cs408", "english"})
SHA256 = frozenset("0123456789abcdef")
FORBIDDEN_KEYS = {
    "release_id", "activation_id", "dispatcher_authority",
    "dispatcher_authority_fingerprint", "mcp_authority",
    "mcp_authority_fingerprint", "mcp_release_id",
    "producer_authority_fingerprint",
}
DESCRIPTOR_RELATIVE_PATHS = {
    "math": Path("数学一回滚复习系统/schema/producer-binding-v1.json"),
    "cs408": Path("schema/producer-binding-v1.json"),
    "english": Path("schema/english_pipeline/producer-binding-v1.json"),
}


class SkillBindingError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA256


def _time(value: Any) -> dt.datetime:
    if not isinstance(value, str):
        raise SkillBindingError("producer_binding_timestamp_invalid")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise SkillBindingError("producer_binding_timestamp_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SkillBindingError("producer_binding_timestamp_invalid")
    return parsed.astimezone(dt.timezone.utc)


def _object(path: Path, code: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
            raise SkillBindingError(code)
        value = json.loads(path.read_text(encoding="utf-8"))
    except SkillBindingError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SkillBindingError(code) from exc
    if not isinstance(value, dict):
        raise SkillBindingError(code)
    return value


def _assert_release_neutral(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key) in FORBIDDEN_KEYS:
                raise SkillBindingError("producer_attestation_not_release_neutral")
            _assert_release_neutral(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_release_neutral(nested)


def _descriptor(config: Mapping[str, Any], subject: str) -> tuple[Path, dict[str, Any]]:
    adapters = config.get("adapters")
    adapter = adapters.get(subject) if isinstance(adapters, Mapping) else None
    raw_root = adapter.get("repo_root") if isinstance(adapter, Mapping) else None
    if not isinstance(raw_root, str) or not Path(raw_root).is_absolute():
        raise SkillBindingError("producer_contract_mismatch")
    path = Path(raw_root).resolve() / DESCRIPTOR_RELATIVE_PATHS[subject]
    descriptor = _object(path, "producer_contract_mismatch")
    core = {key: copy.deepcopy(value) for key, value in descriptor.items() if key != "descriptor_content_sha256"}
    if (
        descriptor.get("schema_version") != "producer_binding_descriptor_v1"
        or descriptor.get("subject") != subject
        or descriptor.get("formal_write_count") != 0
        or descriptor.get("descriptor_content_sha256") != sha256_value(core)
    ):
        raise SkillBindingError("producer_contract_mismatch")
    _assert_release_neutral(descriptor)
    skill = descriptor.get("foreground_skill")
    if not isinstance(skill, Mapping):
        raise SkillBindingError("foreground_skill_binding_mismatch")
    skill_paths: dict[str, Path] = {}
    for prefix in ("authoritative", "installed"):
        raw_path = skill.get(f"{prefix}_path")
        digest = skill.get(f"{prefix}_sha256")
        candidate = Path(str(raw_path or ""))
        if (
            not candidate.is_absolute()
            or candidate.is_symlink()
            or not candidate.is_file()
            or not _sha(digest)
            or sha256_file(candidate) != digest
        ):
            raise SkillBindingError("foreground_skill_binding_mismatch")
        skill_paths[prefix] = candidate
    if (
        skill.get("authoritative_sha256") != skill.get("installed_sha256")
        or skill_paths["authoritative"].read_bytes()
        != skill_paths["installed"].read_bytes()
    ):
        raise SkillBindingError("foreground_skill_binding_mismatch")

    producer = descriptor.get("producer")
    source_files = producer.get("source_files") if isinstance(producer, Mapping) else None
    if not isinstance(source_files, list) or not source_files:
        raise SkillBindingError("producer_contract_mismatch")
    normalized_sources: list[dict[str, str]] = []
    for row in source_files:
        if not isinstance(row, Mapping) or set(row) != {"path", "sha256"}:
            raise SkillBindingError("producer_contract_mismatch")
        candidate = Path(str(row.get("path") or ""))
        digest = row.get("sha256")
        if (
            not candidate.is_absolute()
            or candidate.is_symlink()
            or not candidate.is_file()
            or not _sha(digest)
            or sha256_file(candidate) != digest
        ):
            raise SkillBindingError("producer_contract_mismatch")
        normalized_sources.append({"path": str(candidate), "sha256": str(digest)})
    if producer.get("source_closure_sha256") != sha256_value(normalized_sources):
        raise SkillBindingError("producer_contract_mismatch")

    capture_contract = descriptor.get("capture_contract")
    contract_files = (
        capture_contract.get("files")
        if isinstance(capture_contract, Mapping)
        else None
    )
    if not isinstance(contract_files, list) or not contract_files:
        raise SkillBindingError("capture_contract_mismatch")
    normalized_contracts: list[dict[str, str]] = []
    for row in contract_files:
        if not isinstance(row, Mapping) or set(row) != {"path", "sha256"}:
            raise SkillBindingError("capture_contract_mismatch")
        candidate = Path(str(row.get("path") or ""))
        digest = row.get("sha256")
        if (
            not candidate.is_absolute()
            or candidate.is_symlink()
            or not candidate.is_file()
            or not _sha(digest)
            or sha256_file(candidate) != digest
        ):
            raise SkillBindingError("capture_contract_mismatch")
        normalized_contracts.append({"path": str(candidate), "sha256": str(digest)})
    if capture_contract.get("files_sha256") != sha256_value(normalized_contracts):
        raise SkillBindingError("capture_contract_mismatch")
    return path, descriptor


def _candidate_units(candidate: Any) -> list[dict[str, str]]:
    subject = candidate.subject
    if subject == "english":
        hashes = candidate.input_binding.get("capture_event_sha256")
        events = candidate.model_input.get("batch_events")
        if not isinstance(hashes, Mapping) or not isinstance(events, list):
            raise SkillBindingError("producer_contract_mismatch")
        units: list[dict[str, str]] = []
        for event in events:
            event_id = event.get("event_id") if isinstance(event, Mapping) else None
            recorded_at = event.get("occurred_at") if isinstance(event, Mapping) else None
            digest = hashes.get(event_id) if isinstance(event_id, str) else None
            if not isinstance(event_id, str) or not isinstance(recorded_at, str) or not _sha(digest):
                raise SkillBindingError("producer_contract_mismatch")
            units.append({"capture_id": event_id, "capture_content_sha256": str(digest), "recorded_at": recorded_at})
        return units
    digest = (
        candidate.input_binding.get("original_content_hash")
        if subject == "math"
        else candidate.input_binding.get("payload_sha256")
    )
    if not _sha(digest) or not isinstance(candidate.recorded_at, str):
        raise SkillBindingError("producer_contract_mismatch")
    return [{"capture_id": candidate.capture_id, "capture_content_sha256": str(digest), "recorded_at": candidate.recorded_at}]


def _validate_component_lock(
    config: Mapping[str, Any], subject: str, descriptor_path: Path,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    plugin = config.get("processing_plugin")
    lock_path = plugin.get("component_lock_path") if isinstance(plugin, Mapping) else None
    if not isinstance(lock_path, str) or not Path(lock_path).is_absolute():
        raise SkillBindingError("fixture_contract_mismatch")
    lock = _object(Path(lock_path), "fixture_contract_mismatch")
    contracts = lock.get("foreground_capture_contracts")
    binding = contracts.get(subject) if isinstance(contracts, Mapping) else None
    if (
        not isinstance(binding, Mapping)
        or binding.get("descriptor_path") != str(descriptor_path)
        or binding.get("descriptor_sha256") != sha256_file(descriptor_path)
        or binding.get("attestation_required_after")
        != descriptor.get("attestation_required_after")
        or binding.get("producer_source_closure_sha256")
        != descriptor.get("producer", {}).get("source_closure_sha256")
        or binding.get("foreground_skill_sha256")
        != descriptor.get("foreground_skill", {}).get("installed_sha256")
    ):
        raise SkillBindingError("fixture_contract_mismatch")
    return copy.deepcopy(dict(lock))


def _validate_attestation(
    path: Path, *, subject: str, unit: Mapping[str, str], descriptor_path: Path,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    value = _object(path, "foreground_skill_binding_mismatch")
    expected_keys = {
        "schema_version", "subject", "capture_id", "capture_content_sha256",
        "foreground_skill", "producer", "capture_contract",
        "binding_descriptor_sha256", "attestation_required_after",
        "formal_write_count", "attestation_sha256",
    }
    core = {key: copy.deepcopy(item) for key, item in value.items() if key != "attestation_sha256"}
    if (
        set(value) != expected_keys
        or value.get("schema_version") != "producer_binding_attestation_v1"
        or value.get("subject") != subject
        or value.get("capture_id") != unit["capture_id"]
        or value.get("capture_content_sha256") != unit["capture_content_sha256"]
        or value.get("foreground_skill") != descriptor.get("foreground_skill")
        or value.get("producer") != descriptor.get("producer")
        or value.get("capture_contract") != descriptor.get("capture_contract")
        or value.get("binding_descriptor_sha256") != sha256_file(descriptor_path)
        or value.get("attestation_required_after")
        != descriptor.get("attestation_required_after")
        or value.get("formal_write_count") != 0
        or value.get("attestation_sha256") != sha256_value(core)
    ):
        raise SkillBindingError("foreground_skill_binding_mismatch")
    _assert_release_neutral(value)
    return value


def bind_candidate_producer_attestations(
    config: Mapping[str, Any], candidate: Any
) -> Any:
    if config.get("execution_mode") not in {
        "fixture",
        "offline",
        "live_authorized",
    }:
        # Legacy replay and pre-V2 unit fixtures do not claim the new binding.
        # Sealed V2 production configs are required to set an explicit mode.
        return candidate
    subject = candidate.subject
    if subject not in SUBJECTS:
        raise SkillBindingError("producer_contract_mismatch")
    descriptor_path, descriptor = _descriptor(config, subject)
    units = _candidate_units(candidate)
    threshold = _time(descriptor.get("attestation_required_after"))
    if all(_time(unit["recorded_at"]) < threshold for unit in units):
        return candidate
    component_lock = _validate_component_lock(
        config, subject, descriptor_path, descriptor
    )
    adapters = config["adapters"]
    repo_root = Path(adapters[subject]["repo_root"]).resolve()
    relative_root = Path(str(descriptor.get("attestation_relative_root") or ""))
    if relative_root.is_absolute() or ".." in relative_root.parts:
        raise SkillBindingError("producer_contract_mismatch")
    attestations = [
        _validate_attestation(
            repo_root / relative_root / f"{unit['capture_id']}.json",
            subject=subject,
            unit=unit,
            descriptor_path=descriptor_path,
            descriptor=descriptor,
        )
        for unit in units
    ]
    bundle = {
        "schema_version": "producer_binding_attestation_bundle_v1",
        "subject": subject,
        "capture_id": candidate.capture_id,
        "attestation_sha256s": [value["attestation_sha256"] for value in attestations],
        "descriptor_sha256": sha256_file(descriptor_path),
        "formal_write_count": 0,
    }
    binding = copy.deepcopy(dict(candidate.input_binding))
    binding["producer_binding_attestation_bundle"] = bundle
    binding["producer_binding_attestation_bundle_sha256"] = sha256_value(bundle)
    fixture_contracts = component_lock.get("fixture_contracts")
    fixture_contract = (
        fixture_contracts.get(subject)
        if isinstance(fixture_contracts, Mapping)
        else None
    )
    scanner_adapter = {
        "id": binding.get("adapter_version"),
        "source_closure_sha256": (
            binding.get("processing_contract_sha256")
            or binding.get("adapter_build_sha256")
            or binding.get("producer_binding_attestation_bundle_sha256")
        ),
    }
    processing_binding = build_processing_binding_v3(
        subject=subject,
        component_lock=component_lock,
        fixture_contract=fixture_contract,
        scanner_adapter=scanner_adapter,
    )
    binding["processing_binding_v3"] = processing_binding
    binding["processing_binding_v3_sha256"] = processing_binding[
        "binding_sha256"
    ]
    return replace(
        candidate,
        input_binding=binding,
        input_fingerprint=sha256_value(binding),
    )


def build_processing_binding_v3(
    *, subject: str, component_lock: Mapping[str, Any],
    fixture_contract: Mapping[str, Any], scanner_adapter: Mapping[str, Any],
) -> dict[str, Any]:
    if subject not in SUBJECTS:
        raise SkillBindingError("processing_binding_subject_invalid")
    foreground = component_lock.get("foreground_capture_contracts", {}).get(subject)
    skills = component_lock.get("skills")
    multi_agent = component_lock.get("multi_agent")
    agents = component_lock.get("agent_configs")
    mcp_servers = component_lock.get("luna_mcp_servers")
    if not all(isinstance(value, Mapping) for value in (foreground, skills, multi_agent, agents, mcp_servers, fixture_contract, scanner_adapter)):
        raise SkillBindingError("processing_binding_components_missing")
    background_id = f"background-{subject}-processing"
    background = skills.get(background_id)
    orchestrate = skills.get("multi-agent-read-orchestrate")
    roles = multi_agent.get("roles")
    if not all(isinstance(value, Mapping) for value in (background, orchestrate, roles)):
        raise SkillBindingError("processing_binding_components_missing")
    core = {
        "schema_version": "processing_binding_v3",
        "subject": subject,
        "foreground_capture_skill": copy.deepcopy(dict(foreground)),
        "producer": {"source_closure_sha256": foreground["producer_source_closure_sha256"]},
        "capture_contract": {"descriptor_sha256": foreground["descriptor_sha256"]},
        "fixture_contract": copy.deepcopy(dict(fixture_contract)),
        "scanner_adapter": copy.deepcopy(dict(scanner_adapter)),
        "background_processing_skill": {"id": background_id, **copy.deepcopy(dict(background))},
        "orchestrate_skill": {"id": "multi-agent-read-orchestrate", **copy.deepcopy(dict(orchestrate))},
        "terra_agent_contract": copy.deepcopy(dict(roles["orchestrator"])),
        "luna_reader_contract": copy.deepcopy(dict(roles["reader"])),
        "critical_reviewer_contract": copy.deepcopy(dict(roles["critical_reviewer"])),
        "mcp_binding": copy.deepcopy(dict(mcp_servers[subject])),
        "output_contracts": {
            "read_plan": "orchestration_read_plan_v1",
            "branch_result": "read_branch_result_v1",
            "read_bundle": "read_bundle_v1",
            "risk_report": "risk_report_v1",
            "sol_handoff": "sol_handoff_envelope_v1",
        },
        "formal_write_count": 0,
    }
    return {**core, "binding_sha256": sha256_value(core)}
