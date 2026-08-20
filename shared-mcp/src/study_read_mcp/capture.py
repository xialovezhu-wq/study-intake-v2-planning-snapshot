from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .errors import StudyReadError
from .paging import blob_page
from .safeio import SafeReader
from .session import ReadSession, SHA256_RE


CAPTURE_ROOT_RELATIVE = Path("dispatch/luna-capture-freezes")
MANIFEST_RELATIVE = Path("manifests/sha256")
ARTIFACT_RELATIVE = Path("artifacts/sha256")
MANIFEST_SCHEMA = "study-read-mcp-capture-manifest.v1"
MAX_MANIFEST_BYTES = 256 * 1024
MAX_TEXT_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_IMAGE_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_TEXT_PAGE_BYTES = 32 * 1024

ARTIFACT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
RELATIVE_ARTIFACT_RE = re.compile(
    r"^artifacts/sha256/([0-9a-f]{2})/([0-9a-f]{64})\.([A-Za-z0-9]{1,8})$"
)

TEXT_MEDIA_TYPES = frozenset({
    "application/json", "application/jsonl", "text/plain", "text/markdown",
})
IMAGE_MEDIA_TYPES = frozenset({"image/png", "image/jpeg", "image/webp"})

COMMON_KINDS = frozenset({"capture_facts", "dialogue", "attachment"})
SUBJECT_KINDS = {
    "math": COMMON_KINDS | frozenset({
        "learning_record", "question_text", "question_image",
        "solution_text", "solution_image",
    }),
    "cs408": COMMON_KINDS | frozenset({
        "morning_review_record", "question_text", "question_image",
        "solution_text", "solution_image", "answer_trace", "display_receipt",
    }),
    "english": COMMON_KINDS | frozenset({
        "article_text", "answer_key", "explanation", "sentence_events",
    }),
}
SUBJECT_SCENES = {
    "math": frozenset({"formal_problem", "new_intake", "study_review"}),
    "cs408": frozenset({"morning_review", "formal_problem"}),
    "english": frozenset({"intensive_reading", "article_review"}),
}
IDENTITY_KEYS = frozenset({
    "formal_id", "publication_id", "article_id", "source_id",
    "review_identity", "content_fingerprint",
})
SOURCE_ROLES = frozenset({
    "immutable_capture_fact", "immutable_source_attachment", "user_dialogue",
    "canonical_source_copy",
})


@dataclass(frozen=True, slots=True)
class CaptureArtifact:
    artifact_id: str
    subject: str
    kind: str
    path: Path
    sha256: str
    byte_length: int
    media_type: str
    encoding: str
    source_role: str

    @property
    def is_image(self) -> bool:
        return self.media_type in IMAGE_MEDIA_TYPES

    def public_index(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "artifact_kind": self.kind,
            "sha256": self.sha256,
            "byte_length": self.byte_length,
            "media_type": self.media_type,
            "encoding": self.encoding,
            "source_role": self.source_role,
        }


@dataclass(frozen=True, slots=True)
class CaptureManifest:
    capture_id: str
    subject: str
    study_date: str
    scene: str
    captured_at: str
    identity: dict[str, str]
    manifest_sha256: str
    artifacts: tuple[CaptureArtifact, ...]

    def artifact(self, artifact_id: str) -> CaptureArtifact:
        for item in self.artifacts:
            if item.artifact_id == artifact_id:
                return item
        raise StudyReadError("ARTIFACT_NOT_BOUND", "artifact is not bound to this read session")


@dataclass(frozen=True, slots=True)
class ArtifactPage:
    envelope_item: dict[str, Any]
    page: dict[str, Any]
    image_bytes: bytes | None = None
    image_media_type: str | None = None


class CaptureStore:
    """Read one frozen capture from one exact, content-addressed runtime root."""

    def __init__(self, preprocessor_root: Path, session: ReadSession) -> None:
        if not session.capture_bound:
            raise StudyReadError(
                "CAPTURE_BINDING_REQUIRED",
                "focused Luna tools require a capture-bound read-session v2",
            )
        root = preprocessor_root.resolve(strict=True)
        freeze_root = root / CAPTURE_ROOT_RELATIVE
        try:
            freeze_root = freeze_root.resolve(strict=True)
        except OSError as exc:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "controlled capture freeze root is unavailable"
            ) from exc
        self.reader = SafeReader({"freeze": freeze_root})
        self.freeze_root = freeze_root
        self.session = session
        self.manifest = self._load_manifest()

    @staticmethod
    def _safe_regular(path: Path, label: str, max_bytes: int) -> None:
        try:
            node = path.lstat()
        except OSError as exc:
            raise StudyReadError("NOT_FOUND", f"{label} was not found") from exc
        if path.is_symlink() or not stat.S_ISREG(node.st_mode):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", f"{label} is not a regular file")
        if node.st_size > max_bytes:
            raise StudyReadError("ARTIFACT_TOO_LARGE", f"{label} exceeds its byte limit")
        if stat.S_IMODE(node.st_mode) & 0o022:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", f"{label} is group or world writable"
            )

    def _manifest_path(self) -> Path:
        assert self.session.capture_manifest_path is not None
        raw = self.session.capture_manifest_path
        if not raw.is_absolute():
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture manifest path must be absolute")
        lexical = Path(os.path.abspath(raw))
        try:
            resolved = lexical.resolve(strict=True)
        except OSError as exc:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "capture manifest path cannot be resolved"
            ) from exc
        if not self.reader._contained(self.freeze_root / MANIFEST_RELATIVE, resolved):
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "capture manifest is outside the controlled manifest root"
            )
        try:
            self.reader._validate_no_symlink(self.freeze_root, resolved)
        except StudyReadError as exc:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "capture manifest path contains a symlink"
            ) from exc
        expected_name = f"{self.session.capture_manifest_sha256}.json"
        expected_parent = self.freeze_root / MANIFEST_RELATIVE / str(
            self.session.capture_manifest_sha256
        )[:2]
        if resolved.name != expected_name or resolved.parent != expected_parent:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "capture manifest path is not content addressed"
            )
        return resolved

    def _artifact_path(self, relative_path: str, expected_sha256: str) -> Path:
        matched = RELATIVE_ARTIFACT_RE.fullmatch(relative_path)
        if matched is None or matched.group(1) != expected_sha256[:2] or matched.group(2) != expected_sha256:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "artifact path is not content addressed"
            )
        parts = Path(relative_path).parts
        path = self.reader.exact("freeze", *parts)
        if not self.reader._contained(self.freeze_root / ARTIFACT_RELATIVE, path):
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "artifact is outside the controlled artifact root"
            )
        return path

    @staticmethod
    def _validate_timestamp(value: Any) -> str:
        if not isinstance(value, str):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "captured_at is invalid")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "captured_at is invalid") from exc
        if parsed.utcoffset() is None:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "captured_at must include a timezone")
        return value

    def _load_manifest(self) -> CaptureManifest:
        path = self._manifest_path()
        self._safe_regular(path, "capture manifest", MAX_MANIFEST_BYTES)
        result = self.reader.read_bytes(path)
        if result.sha256 != self.session.capture_manifest_sha256:
            raise StudyReadError("HASH_MISMATCH", "capture manifest hash does not match read session")
        try:
            value = json.loads(result.data.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture manifest is invalid JSON") from exc
        required = {
            "schema_version", "capture_id", "subject", "study_date", "scene",
            "captured_at", "identity", "artifacts", "formal_write_count",
        }
        if not isinstance(value, dict) or set(value) != required:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture manifest fields are invalid")
        if value.get("schema_version") != MANIFEST_SCHEMA:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture manifest schema is unsupported")
        subject = value.get("subject")
        if subject != self.session.subject:
            raise StudyReadError("CAPTURE_SUBJECT_MISMATCH", "capture subject does not match read session")
        capture_id = value.get("capture_id")
        if capture_id != self.session.capture_id or not isinstance(capture_id, str):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture identity does not match read session")
        SafeReader.validate_stable_id(capture_id, "capture id")
        try:
            study_date = date.fromisoformat(str(value.get("study_date"))).isoformat()
        except ValueError as exc:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture study date is invalid") from exc
        scene = value.get("scene")
        if scene not in SUBJECT_SCENES[str(subject)]:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture scene is invalid for subject")
        identity = value.get("identity")
        if (
            not isinstance(identity, dict)
            or not set(identity) <= IDENTITY_KEYS
            or any(not isinstance(k, str) or not isinstance(v, str) or not v or len(v) > 160 for k, v in identity.items())
        ):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture identity fields are invalid")
        rows = value.get("artifacts")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 64:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture artifact list is invalid")
        artifacts: list[CaptureArtifact] = []
        for row in rows:
            artifacts.append(self._load_artifact(subject, row))
        ids = [item.artifact_id for item in artifacts]
        if len(ids) != len(set(ids)) or ids != sorted(ids):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture artifact ids must be sorted and unique")
        if tuple(ids) != self.session.artifact_ids:
            raise StudyReadError(
                "CAPTURE_MANIFEST_INVALID", "read session does not bind the complete artifact set"
            )
        if value.get("formal_write_count") != 0:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture manifest is not read only")
        return CaptureManifest(
            capture_id=capture_id,
            subject=str(subject),
            study_date=study_date,
            scene=str(scene),
            captured_at=self._validate_timestamp(value.get("captured_at")),
            identity=dict(identity),
            manifest_sha256=result.sha256,
            artifacts=tuple(artifacts),
        )

    def _load_artifact(self, subject: str, row: Any) -> CaptureArtifact:
        required = {
            "artifact_id", "subject", "artifact_kind", "relative_path", "sha256",
            "byte_length", "media_type", "encoding", "source_role",
        }
        if not isinstance(row, dict) or set(row) != required:
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture artifact fields are invalid")
        artifact_id = row.get("artifact_id")
        sha256 = row.get("sha256")
        kind = row.get("artifact_kind")
        media_type = row.get("media_type")
        encoding = row.get("encoding")
        source_role = row.get("source_role")
        byte_length = row.get("byte_length")
        if (
            not isinstance(artifact_id, str)
            or ARTIFACT_ID_RE.fullmatch(artifact_id) is None
            or ".." in artifact_id
            or row.get("subject") != subject
            or kind not in SUBJECT_KINDS[subject]
            or not isinstance(sha256, str)
            or SHA256_RE.fullmatch(sha256) is None
            or not isinstance(byte_length, int)
            or byte_length < 0
            or source_role not in SOURCE_ROLES
            or media_type not in TEXT_MEDIA_TYPES | IMAGE_MEDIA_TYPES
            or encoding not in {"utf-8", "binary"}
            or (media_type in TEXT_MEDIA_TYPES and encoding != "utf-8")
            or (media_type in IMAGE_MEDIA_TYPES and encoding != "binary")
            or (kind in {"question_image", "solution_image"} and media_type not in IMAGE_MEDIA_TYPES)
        ):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "capture artifact binding is invalid")
        max_bytes = MAX_IMAGE_ARTIFACT_BYTES if media_type in IMAGE_MEDIA_TYPES else MAX_TEXT_ARTIFACT_BYTES
        if byte_length > max_bytes:
            raise StudyReadError("ARTIFACT_TOO_LARGE", "capture artifact exceeds its byte limit")
        relative_path = row.get("relative_path")
        if not isinstance(relative_path, str):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "artifact path is invalid")
        path = self._artifact_path(relative_path, sha256)
        self._safe_regular(path, "capture artifact", max_bytes)
        read = self.reader.read_bytes(path)
        if read.sha256 != sha256 or len(read.data) != byte_length:
            raise StudyReadError("ARTIFACT_HASH_MISMATCH", "capture artifact hash or size does not match")
        if encoding == "utf-8":
            try:
                text = read.data.decode("utf-8")
            except UnicodeError as exc:
                raise StudyReadError("CAPTURE_MANIFEST_INVALID", "text artifact is not valid UTF-8") from exc
            try:
                if media_type == "application/json":
                    json.loads(text)
                elif media_type == "application/jsonl":
                    for line in text.splitlines():
                        if line.strip():
                            json.loads(line)
            except json.JSONDecodeError as exc:
                raise StudyReadError(
                    "CAPTURE_MANIFEST_INVALID", "structured text artifact is invalid"
                ) from exc
        elif media_type == "image/png" and not read.data.startswith(b"\x89PNG\r\n\x1a\n"):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "PNG artifact signature is invalid")
        elif media_type == "image/jpeg" and not read.data.startswith(b"\xff\xd8\xff"):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "JPEG artifact signature is invalid")
        elif media_type == "image/webp" and not (
            read.data.startswith(b"RIFF") and read.data[8:12] == b"WEBP"
        ):
            raise StudyReadError("CAPTURE_MANIFEST_INVALID", "WebP artifact signature is invalid")
        return CaptureArtifact(
            artifact_id=artifact_id,
            subject=subject,
            kind=str(kind),
            path=path,
            sha256=sha256,
            byte_length=byte_length,
            media_type=str(media_type),
            encoding=str(encoding),
            source_role=str(source_role),
        )

    def verify_all(self) -> None:
        for artifact in self.manifest.artifacts:
            read = self.reader.read_bytes(artifact.path)
            if read.sha256 != artifact.sha256 or len(read.data) != artifact.byte_length:
                raise StudyReadError("ARTIFACT_HASH_MISMATCH", "capture artifact changed after freeze")

    def read_artifact(
        self, artifact_id: str, cursor: str | None, max_bytes: int
    ) -> ArtifactPage:
        artifact = self.manifest.artifact(artifact_id)
        read = self.reader.read_bytes(artifact.path)
        if read.sha256 != artifact.sha256 or len(read.data) != artifact.byte_length:
            raise StudyReadError("ARTIFACT_HASH_MISMATCH", "capture artifact changed after freeze")
        base = {
            **artifact.public_index(),
            "capture_id": self.manifest.capture_id,
            "content_complete": cursor is None,
            "data_role": artifact.source_role,
        }
        artifact_query_sha256 = hashlib.sha256(
            f"{self.session.manifest_sha256}\0{artifact.artifact_id}".encode("utf-8")
        ).hexdigest()
        if artifact.is_image:
            if cursor is not None:
                raise StudyReadError("CURSOR_INVALID", "image artifacts do not use pagination")
            return ArtifactPage(
                envelope_item={**base, "visual_content": "mcp_image_content"},
                page={
                    "total_count": 1, "returned_count": 1, "offset": 0,
                    "page_size": 1, "next_cursor": None, "truncated": False,
                    "complete": True,
                    "query_sha256": artifact_query_sha256,
                },
                image_bytes=read.data,
                image_media_type=artifact.media_type,
            )
        if artifact.media_type not in TEXT_MEDIA_TYPES:
            raise StudyReadError(
                "ARTIFACT_MEDIA_UNSUPPORTED", "artifact cannot be represented safely by this MCP tool"
            )
        if not 1024 <= max_bytes <= MAX_TEXT_PAGE_BYTES:
            raise StudyReadError("INVALID_ARGUMENT", "max_bytes must be between 1024 and 32768")
        chunk, page = blob_page(
            read.data,
            max_bytes=max_bytes,
            cursor=cursor,
            query_sha256=artifact_query_sha256,
            session_sha256=self.session.manifest_sha256,
            generation=self.session.generation,
        )
        try:
            text = chunk.decode("utf-8")
        except UnicodeError as exc:  # blob_page must end on a UTF-8 boundary.
            raise StudyReadError("INTERNAL_SAFE", "text artifact pagination broke UTF-8") from exc
        return ArtifactPage(
            envelope_item={
                **base,
                "text": text,
                "content_complete": page["complete"],
                "chunk_sha256": hashlib.sha256(chunk).hexdigest(),
            },
            page=page,
        )
