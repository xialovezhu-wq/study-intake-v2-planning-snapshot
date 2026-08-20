from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import StudyReadError


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SESSION_ID_RE = re.compile(r"^MCPPF-[A-Z0-9-]{16,80}$")
SUBJECTS = frozenset({"math", "cs408", "english"})
PREFLIGHT_TOOLS = (
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
)


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


def read_root_identity(path: Path) -> str:
    supplied = path.expanduser()
    try:
        supplied_node = supplied.lstat()
        resolved = supplied.resolve(strict=True)
        node = resolved.stat()
    except OSError as exc:
        raise StudyReadError(
            "PREFLIGHT_READ_ROOT_INVALID",
            "infrastructure preflight read root is unavailable",
        ) from exc
    if supplied.is_symlink() or not stat.S_ISDIR(supplied_node.st_mode):
        raise StudyReadError(
            "PREFLIGHT_READ_ROOT_INVALID",
            "infrastructure preflight read root is not a regular directory",
        )
    identity = {
        "path": str(resolved),
        "device": int(node.st_dev),
        "inode": int(node.st_ino),
    }
    return hashlib.sha256(canonical_bytes(identity)).hexdigest()


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise StudyReadError(
            "PREFLIGHT_SESSION_INVALID", "preflight timestamp is invalid"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StudyReadError(
            "PREFLIGHT_SESSION_INVALID", "preflight timestamp is invalid"
        ) from exc
    if parsed.utcoffset() is None:
        raise StudyReadError(
            "PREFLIGHT_SESSION_INVALID", "preflight timestamp is invalid"
        )
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PreflightPolicy:
    path: Path
    sha256: str
    enabled_tools: tuple[str, ...]

    @classmethod
    def load(cls, raw_path: str | Path, expected_sha256: str) -> "PreflightPolicy":
        supplied = Path(raw_path)
        try:
            supplied_node = supplied.lstat()
            path = supplied.resolve(strict=True)
            raw = path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StudyReadError(
                "PREFLIGHT_TOOL_POLICY_INVALID",
                "infrastructure preflight tool policy is invalid",
            ) from exc
        expected_keys = {
            "schema_version",
            "purpose",
            "subject_scoped",
            "enabled_tools",
            "write_tools_allowed",
            "candidate_eligible",
            "production_evidence",
            "formal_write_allowed",
        }
        if (
            supplied.is_symlink()
            or not stat.S_ISREG(supplied_node.st_mode)
            or not SHA256_RE.fullmatch(str(expected_sha256 or ""))
            or hashlib.sha256(raw).hexdigest() != expected_sha256
            or not isinstance(value, dict)
            or set(value) != expected_keys
            or value.get("schema_version")
            != "study-read-mcp-infrastructure-preflight-tool-policy.v1"
            or value.get("purpose") != "infrastructure_preflight"
            or value.get("subject_scoped") is not True
            or value.get("enabled_tools") != list(PREFLIGHT_TOOLS)
            or value.get("write_tools_allowed") is not False
            or value.get("candidate_eligible") is not False
            or value.get("production_evidence") is not False
            or value.get("formal_write_allowed") is not False
        ):
            raise StudyReadError(
                "PREFLIGHT_TOOL_POLICY_INVALID",
                "infrastructure preflight tool policy is invalid",
            )
        return cls(path=path, sha256=expected_sha256, enabled_tools=PREFLIGHT_TOOLS)


@dataclass(frozen=True, slots=True)
class InfrastructurePreflightSession:
    path: Path
    preflight_session_id: str
    subject: str
    central_release_id: str
    mcp_release_id: str
    mcp_server_release: str
    mcp_release_manifest_sha256: str
    launcher_sha256: str
    tool_policy_sha256: str
    read_root: Path
    read_root_identity_sha256: str
    created_at: datetime
    expires_at: datetime
    manifest_sha256: str

    @classmethod
    def load(cls, raw_path: str | Path) -> "InfrastructurePreflightSession":
        supplied = Path(raw_path)
        try:
            supplied_node = supplied.lstat()
            path = supplied.resolve(strict=True)
            node = path.lstat()
            raw = path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StudyReadError(
                "PREFLIGHT_SESSION_INVALID",
                "infrastructure preflight session manifest is invalid",
            ) from exc
        expected_keys = {
            "schema_version",
            "purpose",
            "preflight_session_id",
            "subject",
            "central_release_id",
            "mcp_release_id",
            "mcp_server_release",
            "mcp_release_manifest_sha256",
            "launcher_sha256",
            "tool_policy_sha256",
            "read_root",
            "read_root_identity_sha256",
            "created_at",
            "expires_at",
            "capture_id_absent",
            "production_task_id_absent",
            "candidate_eligible",
            "production_evidence",
            "formal_write_allowed",
            "formal_write_count",
            "manifest_sha256",
        }
        if (
            supplied.is_symlink()
            or not stat.S_ISREG(supplied_node.st_mode)
            or not stat.S_ISREG(node.st_mode)
            or stat.S_IMODE(node.st_mode) != 0o600
            or node.st_size > 64 * 1024
            or not isinstance(value, dict)
            or set(value) != expected_keys
        ):
            raise StudyReadError(
                "PREFLIGHT_SESSION_INVALID",
                "infrastructure preflight session manifest is invalid",
            )
        core = {key: nested for key, nested in value.items() if key != "manifest_sha256"}
        digest = hashlib.sha256(canonical_bytes(core)).hexdigest()
        created_at = _timestamp(value.get("created_at"))
        expires_at = _timestamp(value.get("expires_at"))
        raw_root = value.get("read_root")
        read_root = Path(str(raw_root or ""))
        sha_fields = (
            "central_release_id",
            "mcp_release_id",
            "mcp_release_manifest_sha256",
            "launcher_sha256",
            "tool_policy_sha256",
            "read_root_identity_sha256",
        )
        if (
            value.get("schema_version")
            != "study-read-mcp-infrastructure-preflight-session.v1"
            or value.get("purpose") != "infrastructure_preflight"
            or SESSION_ID_RE.fullmatch(str(value.get("preflight_session_id") or ""))
            is None
            or value.get("subject") not in SUBJECTS
            or any(
                SHA256_RE.fullmatch(str(value.get(field) or "")) is None
                for field in sha_fields
            )
            or not isinstance(value.get("mcp_server_release"), str)
            or not value["mcp_server_release"]
            or not read_root.is_absolute()
            or value.get("read_root_identity_sha256")
            != read_root_identity(read_root)
            or expires_at <= created_at
            or (expires_at - created_at).total_seconds() > 15 * 60
            or expires_at <= datetime.now(timezone.utc)
            or value.get("capture_id_absent") is not True
            or value.get("production_task_id_absent") is not True
            or value.get("candidate_eligible") is not False
            or value.get("production_evidence") is not False
            or value.get("formal_write_allowed") is not False
            or value.get("formal_write_count") != 0
            or value.get("manifest_sha256") != digest
        ):
            raise StudyReadError(
                "PREFLIGHT_SESSION_INVALID",
                "infrastructure preflight session binding is invalid",
            )
        return cls(
            path=path,
            preflight_session_id=str(value["preflight_session_id"]),
            subject=str(value["subject"]),
            central_release_id=str(value["central_release_id"]),
            mcp_release_id=str(value["mcp_release_id"]),
            mcp_server_release=str(value["mcp_server_release"]),
            mcp_release_manifest_sha256=str(value["mcp_release_manifest_sha256"]),
            launcher_sha256=str(value["launcher_sha256"]),
            tool_policy_sha256=str(value["tool_policy_sha256"]),
            read_root=read_root.resolve(strict=True),
            read_root_identity_sha256=str(value["read_root_identity_sha256"]),
            created_at=created_at,
            expires_at=expires_at,
            manifest_sha256=digest,
        )

    def route(self) -> dict[str, Any]:
        return {
            "caller_skill_id": "sol-mcp-infrastructure-preflight",
            "caller_skill_version": "1.0.0",
            "plugin_version": self.central_release_id,
            "route_request_id": self.preflight_session_id,
            "evidence_scope_hash": self.manifest_sha256,
            "read_route": "infrastructure_preflight",
            "chunk_index": 1,
            "chunk_count": 1,
            "consumed_duplicate_read_count": 0,
        }

    def public_binding(self) -> dict[str, Any]:
        return {
            "schema_version": "study-read-mcp-infrastructure-preflight-session.v1",
            "purpose": "infrastructure_preflight",
            "preflight_session_id": self.preflight_session_id,
            "subject": self.subject,
            "central_release_id": self.central_release_id,
            "mcp_release_id": self.mcp_release_id,
            "mcp_server_release": self.mcp_server_release,
            "mcp_release_manifest_sha256": self.mcp_release_manifest_sha256,
            "launcher_sha256": self.launcher_sha256,
            "tool_policy_sha256": self.tool_policy_sha256,
            "read_root_identity_sha256": self.read_root_identity_sha256,
            "capture_id": "absent",
            "production_task_id": "absent",
            "candidate_eligible": False,
            "production_evidence": False,
            "formal_write_allowed": False,
            "formal_write_count": 0,
            "manifest_sha256": self.manifest_sha256,
        }
