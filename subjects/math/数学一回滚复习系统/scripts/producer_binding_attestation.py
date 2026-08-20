"""Release-neutral Producer/foreground-Skill attestation sidecar."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping


FORBIDDEN_KEYS = {
    "release_id", "activation_id", "dispatcher_authority",
    "dispatcher_authority_fingerprint", "mcp_authority",
    "mcp_authority_fingerprint", "mcp_release_id",
    "producer_authority_fingerprint",
}


class ProducerBindingError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _assert_release_neutral(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key) in FORBIDDEN_KEYS:
                raise ProducerBindingError("producer attestation contains deployment identity")
            _assert_release_neutral(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_release_neutral(nested)


def _timestamp(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProducerBindingError("producer binding timestamp must include timezone")
    return parsed.astimezone(dt.timezone.utc)


def load_descriptor(path: Path, *, subject: str) -> dict[str, Any]:
    descriptor = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version", "subject", "attestation_required_after",
        "foreground_skill", "producer", "capture_contract",
        "attestation_relative_root", "formal_write_count",
        "descriptor_content_sha256",
    }
    if (
        not isinstance(descriptor, dict)
        or set(descriptor) != expected
        or descriptor.get("schema_version") != "producer_binding_descriptor_v1"
        or descriptor.get("subject") != subject
        or descriptor.get("formal_write_count") != 0
    ):
        raise ProducerBindingError("producer binding descriptor invalid")
    core = {key: value for key, value in descriptor.items() if key != "descriptor_content_sha256"}
    if descriptor.get("descriptor_content_sha256") != sha256_value(core):
        raise ProducerBindingError("producer binding descriptor hash invalid")
    _timestamp(str(descriptor["attestation_required_after"]))
    relative_root = Path(str(descriptor["attestation_relative_root"]))
    if relative_root.is_absolute() or ".." in relative_root.parts:
        raise ProducerBindingError("producer binding sidecar root invalid")
    skill = descriptor.get("foreground_skill")
    if not isinstance(skill, dict):
        raise ProducerBindingError("foreground Skill binding missing")
    for prefix in ("authoritative", "installed"):
        raw_path = skill.get(f"{prefix}_path")
        digest = skill.get(f"{prefix}_sha256")
        candidate = Path(str(raw_path or ""))
        if not candidate.is_absolute() or candidate.is_symlink() or not candidate.is_file() or sha256_file(candidate) != digest:
            raise ProducerBindingError(f"foreground Skill {prefix} binding mismatch")
    authoritative_path = Path(str(skill.get("authoritative_path") or ""))
    installed_path = Path(str(skill.get("installed_path") or ""))
    if (
        skill.get("authoritative_sha256") != skill.get("installed_sha256")
        or authoritative_path.read_bytes() != installed_path.read_bytes()
    ):
        raise ProducerBindingError("foreground Skill authoritative/installed parity mismatch")
    producer = descriptor.get("producer")
    files = producer.get("source_files") if isinstance(producer, dict) else None
    if not isinstance(files, list) or not files:
        raise ProducerBindingError("producer source closure missing")
    normalized: list[dict[str, Any]] = []
    for row in files:
        if not isinstance(row, dict) or set(row) != {"path", "sha256"}:
            raise ProducerBindingError("producer source closure invalid")
        candidate = Path(str(row["path"]))
        if not candidate.is_absolute() or candidate.is_symlink() or not candidate.is_file() or sha256_file(candidate) != row["sha256"]:
            raise ProducerBindingError("producer source closure mismatch")
        normalized.append(dict(row))
    if producer.get("source_closure_sha256") != sha256_value(normalized):
        raise ProducerBindingError("producer source closure hash invalid")
    contract = descriptor.get("capture_contract")
    files = contract.get("files") if isinstance(contract, dict) else None
    if not isinstance(files, list) or not files:
        raise ProducerBindingError("Capture contract closure missing")
    for row in files:
        candidate = Path(str((row or {}).get("path") or "")) if isinstance(row, dict) else Path("")
        if not candidate.is_absolute() or candidate.is_symlink() or not candidate.is_file() or sha256_file(candidate) != row.get("sha256"):
            raise ProducerBindingError("Capture contract closure mismatch")
    _assert_release_neutral(descriptor)
    return descriptor


def _atomic_no_clobber(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_bytes(value)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
                raise ProducerBindingError("producer attestation no-clobber conflict")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def publish_attestation(
    *, descriptor_path: Path, repo_root: Path, subject: str,
    capture_id: str, capture_content_sha256: str, recorded_at: str,
) -> dict[str, Any]:
    descriptor = load_descriptor(descriptor_path, subject=subject)
    if _timestamp(recorded_at) < _timestamp(descriptor["attestation_required_after"]):
        return {
            "status": "historical_pre_attestation",
            "attestation_path": None,
            "attestation_sha256": None,
            "formal_write_count": 0,
        }
    core = {
        "schema_version": "producer_binding_attestation_v1",
        "subject": subject,
        "capture_id": capture_id,
        "capture_content_sha256": capture_content_sha256,
        "foreground_skill": descriptor["foreground_skill"],
        "producer": descriptor["producer"],
        "capture_contract": descriptor["capture_contract"],
        "binding_descriptor_sha256": sha256_file(descriptor_path),
        "attestation_required_after": descriptor["attestation_required_after"],
        "formal_write_count": 0,
    }
    _assert_release_neutral(core)
    attestation = {**core, "attestation_sha256": sha256_value(core)}
    root = repo_root.resolve() / descriptor["attestation_relative_root"]
    path = root / f"{capture_id}.json"
    _atomic_no_clobber(path, attestation)
    return {
        "status": "attested",
        "attestation_path": str(path),
        "attestation_sha256": sha256_file(path),
        "formal_write_count": 0,
    }
