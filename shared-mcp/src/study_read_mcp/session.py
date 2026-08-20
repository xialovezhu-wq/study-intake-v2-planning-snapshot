from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .errors import StudyReadError


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SESSION_ID_RE = re.compile(r"^MCPRS-[A-Z0-9-]{16,80}$")
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.+-]{0,79}$")
ARTIFACT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.utcoffset() is not None


@dataclass(frozen=True)
class ReadSession:
    path: Path
    schema_version: str
    read_session_id: str
    subject: str
    candidate_release_id: str
    plugin_version: str
    skill_id: str
    skill_version: str
    mcp_server_release: str
    generation: str
    authority_fingerprint: str
    manifest_sha256: str
    capture_id: str | None = None
    capture_manifest_path: Path | None = None
    capture_manifest_sha256: str | None = None
    artifact_ids: tuple[str, ...] = ()
    authority_snapshot_manifest_path: Path | None = None
    authority_snapshot_manifest_sha256: str | None = None
    authority_snapshot_root: Path | None = None
    authority_snapshot_receipt_sha256: str | None = None

    @classmethod
    def load(cls, raw_path: str | Path) -> "ReadSession":
        raw = Path(raw_path)
        try:
            raw_node = raw.lstat()
            if raw.is_symlink() or not stat.S_ISREG(raw_node.st_mode):
                raise StudyReadError(
                    "READ_SESSION_UNSAFE", "read-session manifest is not a private regular file"
                )
            path = raw.resolve(strict=True)
            node = path.lstat()
            if (
                not stat.S_ISREG(node.st_mode)
                or node.st_size > 64 * 1024
                or stat.S_IMODE(node.st_mode) != 0o600
            ):
                raise StudyReadError(
                    "READ_SESSION_UNSAFE", "read-session manifest is not a private regular file"
                )
            value = json.loads(path.read_text(encoding="utf-8"))
        except StudyReadError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StudyReadError(
                "READ_SESSION_INVALID", "read-session manifest is invalid"
            ) from exc
        if not isinstance(value, dict):
            raise StudyReadError("READ_SESSION_INVALID", "read-session manifest is invalid")
        core = {key: nested for key, nested in value.items() if key != "manifest_sha256"}
        digest = hashlib.sha256(_canonical_bytes(core)).hexdigest()
        required_v1 = {
            "schema_version", "read_session_id", "subject", "candidate_release_id",
            "plugin_version", "skill_id", "skill_version", "mcp_server_release",
            "generation", "authority_fingerprint", "created_at",
            "formal_write_count", "manifest_sha256",
        }
        required_v2 = required_v1 | {
            "capture_id", "capture_manifest_path", "capture_manifest_sha256",
            "artifact_ids",
        }
        required_v3 = required_v2 | {
            "authority_snapshot_manifest_path",
            "authority_snapshot_manifest_sha256",
            "authority_snapshot_root",
            "authority_snapshot_receipt_sha256",
        }
        subject = value.get("subject")
        schema_version = value.get("schema_version")
        required = (
            required_v3
            if schema_version in {
                "study-read-mcp-read-session.v3",
                "study-read-mcp-read-session.v4",
            }
            else required_v2
            if schema_version == "study-read-mcp-read-session.v2"
            else required_v1
        )
        artifact_ids = value.get("artifact_ids", [])
        capture_manifest_path = value.get("capture_manifest_path")
        if (
            set(value) != required
            or schema_version not in {
                "study-read-mcp-read-session.v1",
                "study-read-mcp-read-session.v2",
                "study-read-mcp-read-session.v3",
                "study-read-mcp-read-session.v4",
            }
            or subject not in {"math", "cs408", "english"}
            or not SESSION_ID_RE.fullmatch(str(value.get("read_session_id") or ""))
            or not SHA256_RE.fullmatch(str(value.get("candidate_release_id") or ""))
            or VERSION_RE.fullmatch(str(value.get("plugin_version") or "")) is None
            or not isinstance(value.get("skill_id"), str)
            or value.get("skill_id") != f"background-{subject}-processing"
            or VERSION_RE.fullmatch(str(value.get("skill_version") or "")) is None
            or not isinstance(value.get("mcp_server_release"), str)
            or not 1 <= len(value.get("mcp_server_release")) <= 200
            or not isinstance(value.get("generation"), str)
            or not 1 <= len(value.get("generation")) <= 160
            or not SHA256_RE.fullmatch(str(value.get("authority_fingerprint") or ""))
            or not _valid_timestamp(value.get("created_at"))
            or value.get("formal_write_count") != 0
            or value.get("manifest_sha256") != digest
            or (
                schema_version in {
                    "study-read-mcp-read-session.v2",
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                and (
                    not isinstance(value.get("capture_id"), str)
                    or not value.get("capture_id")
                    or len(str(value.get("capture_id"))) > 160
                    or not isinstance(capture_manifest_path, str)
                    or not Path(capture_manifest_path).is_absolute()
                    or not SHA256_RE.fullmatch(
                        str(value.get("capture_manifest_sha256") or "")
                    )
                    or not isinstance(artifact_ids, list)
                    or not 1 <= len(artifact_ids) <= 64
                    or any(
                        not isinstance(item, str)
                        or ARTIFACT_ID_RE.fullmatch(item) is None
                        or ".." in item
                        for item in artifact_ids
                    )
                    or len(artifact_ids) != len(set(artifact_ids))
                    or artifact_ids != sorted(artifact_ids)
                )
            )
            or (
                schema_version in {
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                and not cls._valid_authority_snapshot(
                    value,
                    expected_schema_version=(
                        "study-read-mcp-authority-snapshot.v2"
                        if schema_version == "study-read-mcp-read-session.v4"
                        else "study-read-mcp-authority-snapshot.v1"
                    ),
                )
            )
        ):
            raise StudyReadError("READ_SESSION_INVALID", "read-session manifest binding is invalid")
        return cls(
            path=path,
            schema_version=str(schema_version),
            read_session_id=str(value["read_session_id"]),
            subject=str(subject),
            candidate_release_id=str(value["candidate_release_id"]),
            plugin_version=str(value["plugin_version"]),
            skill_id=str(value["skill_id"]),
            skill_version=str(value["skill_version"]),
            mcp_server_release=str(value["mcp_server_release"]),
            generation=str(value["generation"]),
            authority_fingerprint=str(value["authority_fingerprint"]),
            manifest_sha256=digest,
            capture_id=(
                str(value["capture_id"])
                if schema_version in {
                    "study-read-mcp-read-session.v2",
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            capture_manifest_path=(
                Path(str(capture_manifest_path))
                if schema_version in {
                    "study-read-mcp-read-session.v2",
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            capture_manifest_sha256=(
                str(value["capture_manifest_sha256"])
                if schema_version in {
                    "study-read-mcp-read-session.v2",
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            artifact_ids=tuple(str(item) for item in artifact_ids),
            authority_snapshot_manifest_path=(
                Path(str(value["authority_snapshot_manifest_path"]))
                if schema_version in {
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            authority_snapshot_manifest_sha256=(
                str(value["authority_snapshot_manifest_sha256"])
                if schema_version in {
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            authority_snapshot_root=(
                Path(str(value["authority_snapshot_root"]))
                if schema_version in {
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
            authority_snapshot_receipt_sha256=(
                str(value["authority_snapshot_receipt_sha256"])
                if schema_version in {
                    "study-read-mcp-read-session.v3",
                    "study-read-mcp-read-session.v4",
                }
                else None
            ),
        )

    @staticmethod
    def _valid_authority_snapshot(
        value: dict[str, Any], *, expected_schema_version: str
    ) -> bool:
        manifest_path_raw = value.get("authority_snapshot_manifest_path")
        root_raw = value.get("authority_snapshot_root")
        digest = value.get("authority_snapshot_manifest_sha256")
        receipt_digest = value.get("authority_snapshot_receipt_sha256")
        if (
            not isinstance(manifest_path_raw, str)
            or not Path(manifest_path_raw).is_absolute()
            or not isinstance(root_raw, str)
            or not Path(root_raw).is_absolute()
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
            or not isinstance(receipt_digest, str)
            or SHA256_RE.fullmatch(receipt_digest) is None
        ):
            return False
        try:
            manifest_path = Path(manifest_path_raw)
            manifest_node = manifest_path.lstat()
            if (
                manifest_path.is_symlink()
                or not stat.S_ISREG(manifest_node.st_mode)
                or stat.S_IMODE(manifest_node.st_mode) != 0o600
                or manifest_node.st_size > 64 * 1024 * 1024
            ):
                return False
            raw = manifest_path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                return False
            manifest = json.loads(raw)
            expected_keys = {
                "schema_version",
                "subject",
                "source_generation",
                "source_authority_fingerprint",
                "mcp_server_release",
                "files",
                "file_set_sha256",
                "file_count",
                "total_bytes",
                "created_at",
                "formal_write_count",
            }
            rows = manifest.get("files")
            root = Path(root_raw).resolve(strict=True)
            if (
                not isinstance(manifest, dict)
                or set(manifest) != expected_keys
                or manifest.get("schema_version")
                != expected_schema_version
                or manifest.get("subject") != value.get("subject")
                or manifest.get("source_generation") != value.get("generation")
                or manifest.get("source_authority_fingerprint")
                != value.get("authority_fingerprint")
                or manifest.get("mcp_server_release")
                != value.get("mcp_server_release")
                or manifest.get("formal_write_count") != 0
                or hashlib.sha256(_canonical_bytes(manifest)).hexdigest() != digest
                or not isinstance(rows, list)
                or manifest.get("file_count") != len(rows)
                or not isinstance(manifest.get("total_bytes"), int)
                or manifest.get("total_bytes") < 0
                or not _valid_timestamp(manifest.get("created_at"))
            ):
                return False
            total = 0
            bindings: list[tuple[str, str]] = []
            seen: set[str] = set()
            for row in rows:
                if not isinstance(row, dict) or set(row) != {
                    "relative_path", "sha256", "size"
                }:
                    return False
                relative = row.get("relative_path")
                sha256 = row.get("sha256")
                size = row.get("size")
                if (
                    not isinstance(relative, str)
                    or not relative
                    or relative in seen
                    or Path(relative).is_absolute()
                    or ".." in Path(relative).parts
                    or not isinstance(sha256, str)
                    or SHA256_RE.fullmatch(sha256) is None
                    or isinstance(size, bool)
                    or not isinstance(size, int)
                    or size < 0
                ):
                    return False
                seen.add(relative)
                path = (root / relative).resolve(strict=True)
                try:
                    path.relative_to(root)
                except ValueError:
                    return False
                node = path.lstat()
                if path.is_symlink() or not stat.S_ISREG(node.st_mode):
                    return False
                file_raw = path.read_bytes()
                if len(file_raw) != size or hashlib.sha256(file_raw).hexdigest() != sha256:
                    return False
                total += size
                bindings.append((relative, sha256))
            file_set = hashlib.sha256(
                "\n".join(f"{name}:{sha}" for name, sha in sorted(bindings)).encode("utf-8")
            ).hexdigest()
            return (
                total == manifest.get("total_bytes")
                and file_set == manifest.get("file_set_sha256")
            )
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
            return False

    @property
    def capture_bound(self) -> bool:
        return self.schema_version in {
            "study-read-mcp-read-session.v2",
            "study-read-mcp-read-session.v3",
            "study-read-mcp-read-session.v4",
        }

    @property
    def snapshot_bound(self) -> bool:
        return self.schema_version in {
            "study-read-mcp-read-session.v3",
            "study-read-mcp-read-session.v4",
        }

    def route(self) -> dict[str, Any]:
        return {
            "caller_skill_id": self.skill_id,
            "caller_skill_version": self.skill_version,
            "plugin_version": self.plugin_version,
            "route_request_id": self.read_session_id,
            "evidence_scope_hash": self.manifest_sha256,
            "read_route": "mcp_model_driven",
            "read_session_id": self.read_session_id,
            "consumed_duplicate_read_count": 0,
        }
