from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .adapters.cs408 import CS408Adapter
from .adapters.english import EnglishAdapter
from .adapters.math import MathAdapter
from .errors import StudyReadError
from .models import PagedReadRequest
from .release import SERVER_RELEASE
from .runtime_binding import verify_runtime_binding
from .safeio import FileIdentity, SafeReader


SUBJECTS = frozenset({"math", "cs408", "english"})
MAX_SNAPSHOT_FILES = 200_000
MAX_SNAPSHOT_BYTES = 1024 * 1024 * 1024
ENUMERATION_COLLECTIONS = {
    "math": (
        "formal_card_records",
        "math_taxonomy_items",
        "formal_relation_declarations",
        "relationship_candidates",
        "activity",
    ),
    "cs408": (
        "formal_nodes",
        "knowledge_nodes",
        "formal_relationships",
        "knowledge_safe_notes",
        "knowledge_navigation_links",
        "curation_inventory",
        "morning_sessions",
        "review_events",
    ),
    "english": (
        "articles",
        "sentences",
        "vocabulary",
        "mastered_items",
        "patterns",
        "events",
        "raw_events",
        "effective_events",
        "article_learning_pages",
        "relations",
    ),
}


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_bytes(path: Path, raw: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp_path = Path(temporary)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_path.exists():
            temp_path.unlink()


class RecordingSafeReader(SafeReader):
    def __init__(self, subject: str, root: Path) -> None:
        super().__init__({subject: root})
        self.subject = subject
        self.root = self.roots[subject]
        self.files: dict[Path, FileIdentity] = {}
        self.directories: set[Path] = {self.root}

    def _record(self, path: Path) -> None:
        resolved = path.resolve(strict=True)
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise StudyReadError("SNAPSHOT_PATH_ESCAPE", "snapshot source escaped root") from exc
        node = resolved.lstat()
        if stat.S_ISREG(node.st_mode):
            self.files[resolved] = FileIdentity.from_stat(node)
        elif stat.S_ISDIR(node.st_mode):
            self.directories.add(resolved)
        else:
            raise StudyReadError("SNAPSHOT_SOURCE_UNSAFE", "snapshot source is unsafe")

    def exact(self, root_name: str, *parts: str, must_exist: bool = True) -> Path:
        path = super().exact(root_name, *parts, must_exist=must_exist)
        if path.exists():
            self._record(path)
        return path

    def validate_indexed_path(self, root_name: str, candidate: Path) -> Path:
        path = super().validate_indexed_path(root_name, candidate)
        self._record(path)
        return path

    def identity(self, path: Path) -> FileIdentity:
        identity = super().identity(path)
        self._record(path)
        return identity


def _adapter(subject: str, root: Path, reader: RecordingSafeReader) -> Any:
    if subject == "math":
        return MathAdapter(root, reader=reader)
    if subject == "cs408":
        return CS408Adapter(root, reader=reader)
    if subject == "english":
        return EnglishAdapter(root, reader=reader)
    raise StudyReadError("INVALID_ARGUMENT", "snapshot subject is invalid")


def _enumerate_snapshot(subject: str, root: Path) -> tuple[Any, RecordingSafeReader]:
    reader = RecordingSafeReader(subject, root)
    adapter = _adapter(subject, root, reader)
    try:
        before = adapter.authority()
        for collection in ENUMERATION_COLLECTIONS[subject]:
            try:
                adapter.luna_records(
                    PagedReadRequest(collection=collection, page_size=48)
                )
            except StudyReadError as exc:
                # Freeze the exact bytes even when one optional live
                # projection is already fail-closed.  The frozen adapter will
                # reproduce that same bounded error; an unrelated projection
                # defect must not make the snapshot incomplete or mutable.
                if exc.code not in {
                    "SUBJECT_UNAVAILABLE",
                    "NOT_FOUND",
                    "PROJECTION_MANIFEST_MISMATCH",
                    "PROJECTION_EVENT_MISMATCH",
                }:
                    raise
        after = adapter.authority()
        if (
            after.generation != before.generation
            or after.fingerprint != before.fingerprint
            or after.release != before.release
        ):
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ",
                "subject authority changed while freezing the read snapshot",
                True,
            )
        return before, reader
    finally:
        close = getattr(adapter, "close", None)
        if callable(close):
            close()


def _verify_source_identity(path: Path, expected: FileIdentity) -> bytes:
    before = FileIdentity.from_stat(path.stat(follow_symlinks=False))
    if before != expected or path.is_symlink() or not path.is_file():
        raise StudyReadError(
            "SOURCE_CHANGED_DURING_READ", "snapshot source changed before copy", True
        )
    with path.open("rb") as handle:
        opened = FileIdentity.from_stat(os.fstat(handle.fileno()))
        raw = handle.read()
        after_fd = FileIdentity.from_stat(os.fstat(handle.fileno()))
    after_path = FileIdentity.from_stat(path.stat(follow_symlinks=False))
    if not (before == opened == after_fd == after_path == expected):
        raise StudyReadError(
            "SOURCE_CHANGED_DURING_READ", "snapshot source changed during copy", True
        )
    return raw


def _manifest_path(snapshot_root: Path, digest: str) -> Path:
    return snapshot_root / "manifests" / "sha256" / digest[:2] / f"{digest}.json"


def _object_root(snapshot_root: Path, digest: str) -> Path:
    return snapshot_root / "objects" / "sha256" / digest[:2] / digest / "root"


def _validate_existing(snapshot_root: Path, pointer: Path, subject: str, fingerprint: str) -> dict[str, Any] | None:
    try:
        pointer_value = json.loads(pointer.read_text(encoding="utf-8"))
        digest = pointer_value["authority_snapshot_manifest_sha256"]
        manifest_path = _manifest_path(snapshot_root, digest)
        raw = manifest_path.read_bytes()
        value = json.loads(raw)
        if (
            set(pointer_value) != {
                "schema_version", "subject", "authority_fingerprint",
                "authority_snapshot_manifest_sha256",
            }
            or pointer_value["schema_version"] != "study-read-mcp-authority-snapshot-pointer.v1"
            or pointer_value["subject"] != subject
            or pointer_value["authority_fingerprint"] != fingerprint
            or _sha256_bytes(raw) != digest
            or value.get("schema_version")
            != "study-read-mcp-authority-snapshot.v2"
            or value.get("subject") != subject
            or value.get("source_authority_fingerprint") != fingerprint
            or value.get("mcp_server_release") != SERVER_RELEASE
        ):
            return None
        root = _object_root(snapshot_root, digest)
        for row in value.get("files", []):
            path = root / str(row["relative_path"])
            if path.is_symlink() or not path.is_file():
                return None
            raw_file = path.read_bytes()
            if len(raw_file) != row["size"] or _sha256_bytes(raw_file) != row["sha256"]:
                return None
        return {
            "authority_snapshot_manifest_path": str(manifest_path),
            "authority_snapshot_manifest_sha256": digest,
            "authority_snapshot_root": str(root),
            "generation": value["source_generation"],
            "authority_fingerprint": fingerprint,
            "file_count": value["file_count"],
            "total_bytes": value["total_bytes"],
            "formal_write_count": 0,
        }
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def build_authority_snapshot(subject: str, source_root: Path, output_root: Path) -> dict[str, Any]:
    if subject not in SUBJECTS:
        raise StudyReadError("INVALID_ARGUMENT", "snapshot subject is invalid")
    source = source_root.expanduser().resolve(strict=True)
    output = output_root.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    authority, reader = _enumerate_snapshot(subject, source)
    lock_path = output / "locks" / f"{subject}-{authority.fingerprint}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock_path.open("a+b") as lock_handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        pointer = output / "by-authority" / subject / f"{authority.fingerprint}.json"
        existing = _validate_existing(output, pointer, subject, authority.fingerprint)
        if existing is not None:
            return existing

        if len(reader.files) > MAX_SNAPSHOT_FILES:
            raise StudyReadError("SNAPSHOT_TOO_LARGE", "snapshot file count exceeds bound")
        rows: list[dict[str, Any]] = []
        copied: list[tuple[str, str]] = []
        total_bytes = 0
        for path, identity in sorted(reader.files.items(), key=lambda item: str(item[0])):
            raw = _verify_source_identity(path, identity)
            total_bytes += len(raw)
            if total_bytes > MAX_SNAPSHOT_BYTES:
                raise StudyReadError("SNAPSHOT_TOO_LARGE", "snapshot byte count exceeds bound")
            digest = _sha256_bytes(raw)
            relative = path.relative_to(source).as_posix()
            blob = output / "blobs" / "sha256" / digest[:2] / digest
            if not blob.exists():
                _atomic_bytes(blob, raw, 0o400)
            elif _sha256_bytes(blob.read_bytes()) != digest:
                raise StudyReadError("SNAPSHOT_BLOB_INVALID", "snapshot blob hash mismatch")
            rows.append({"relative_path": relative, "sha256": digest, "size": len(raw)})
            copied.append((relative, digest))

        created_at = datetime.now(timezone.utc).isoformat()
        core = {
            "schema_version": "study-read-mcp-authority-snapshot.v2",
            "subject": subject,
            "source_generation": authority.generation,
            "source_authority_fingerprint": authority.fingerprint,
            "mcp_server_release": SERVER_RELEASE,
            "files": rows,
            "file_set_sha256": SafeReader.set_fingerprint(copied),
            "file_count": len(rows),
            "total_bytes": total_bytes,
            "created_at": created_at,
            "formal_write_count": 0,
        }
        digest = _sha256_bytes(_canonical_bytes(core))
        manifest = core
        manifest_path = _manifest_path(output, digest)
        root = _object_root(output, digest)
        temporary_root = root.parent / f".root.{os.getpid()}"
        if temporary_root.exists():
            shutil.rmtree(temporary_root)
        temporary_root.mkdir(parents=True, mode=0o700)
        try:
            for directory in sorted(reader.directories, key=str):
                relative = directory.relative_to(source)
                (temporary_root / relative).mkdir(parents=True, exist_ok=True, mode=0o700)
            for row in rows:
                destination = temporary_root / row["relative_path"]
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                blob = output / "blobs" / "sha256" / row["sha256"][:2] / row["sha256"]
                os.link(blob, destination)
            root.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.replace(temporary_root, root)
        finally:
            if temporary_root.exists():
                shutil.rmtree(temporary_root)
        _atomic_bytes(manifest_path, _canonical_bytes(manifest), 0o600)

        frozen_reader = RecordingSafeReader(subject, root)
        frozen_adapter = _adapter(subject, root, frozen_reader)
        try:
            frozen = frozen_adapter.authority()
        finally:
            close = getattr(frozen_adapter, "close", None)
            if callable(close):
                close()
        if (
            frozen.generation != authority.generation
            or frozen.fingerprint != authority.fingerprint
            or frozen.release != authority.release
        ):
            raise StudyReadError("SNAPSHOT_REOPEN_MISMATCH", "frozen authority does not reopen exactly")
        pointer_value = {
            "schema_version": "study-read-mcp-authority-snapshot-pointer.v1",
            "subject": subject,
            "authority_fingerprint": authority.fingerprint,
            "authority_snapshot_manifest_sha256": digest,
        }
        _atomic_bytes(pointer, _canonical_bytes(pointer_value), 0o600)
        return {
            "authority_snapshot_manifest_path": str(manifest_path),
            "authority_snapshot_manifest_sha256": digest,
            "authority_snapshot_root": str(root),
            "generation": authority.generation,
            "authority_fingerprint": authority.fingerprint,
            "file_count": len(rows),
            "total_bytes": total_bytes,
            "formal_write_count": 0,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="study-read-mcp-snapshot")
    parser.add_argument("--subject", choices=sorted(SUBJECTS), required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    try:
        verify_runtime_binding()
        args = parse_args()
        result = build_authority_snapshot(args.subject, args.source_root, args.output_root)
    except Exception as exc:
        sys.stderr.write(f"study-read-mcp-snapshot: {type(exc).__name__}: {exc}\n")
        raise SystemExit(2) from exc
    sys.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
