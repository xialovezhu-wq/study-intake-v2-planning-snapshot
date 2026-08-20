from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import stat
import threading
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .errors import StudyReadError


STABLE_ID = re.compile(r"^[^\W][\w.:-]{0,159}$", re.UNICODE)


@dataclass(frozen=True, slots=True)
class FileIdentity:
    dev: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> "FileIdentity":
        return cls(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)

    def token(self) -> str:
        return f"{self.dev}:{self.inode}:{self.size}:{self.mtime_ns}:{self.ctime_ns}"


@dataclass(frozen=True, slots=True)
class ReadResult:
    path: Path
    identity: FileIdentity
    sha256: str
    data: bytes


class LRUCache:
    def __init__(self, max_entries: int = 128) -> None:
        self.max_entries = max_entries
        self._items: OrderedDict[tuple[Any, ...], Any] = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, key: tuple[Any, ...]) -> Any | None:
        with self._lock:
            if key not in self._items:
                self.misses += 1
                return None
            self.hits += 1
            self._items.move_to_end(key)
            return deepcopy(self._items[key])

    def put(self, key: tuple[Any, ...], value: Any) -> None:
        with self._lock:
            self._items[key] = deepcopy(value)
            self._items.move_to_end(key)
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)

    def stats(self) -> dict[str, int]:
        return {"hits": self.hits, "misses": self.misses, "entries": len(self._items)}


class SafeReader:
    """Exact-path, containment-checked reads. No caller-controlled paths are accepted."""

    def __init__(self, roots: dict[str, Path], cache_entries: int = 128) -> None:
        self.roots = {key: path.resolve(strict=True) for key, path in roots.items()}
        self.cache = LRUCache(cache_entries)
        self.after_read_hook: Callable[[Path], None] | None = None

    @staticmethod
    def validate_stable_id(value: str, label: str = "stable_id") -> str:
        if "\x00" in value or "/" in value or "\\" in value or value in {".", ".."} or ".." in value:
            raise StudyReadError("INVALID_ARGUMENT", f"invalid {label}")
        if not STABLE_ID.fullmatch(value):
            raise StudyReadError("INVALID_ARGUMENT", f"invalid {label}")
        return value

    def exact(self, root_name: str, *parts: str, must_exist: bool = True) -> Path:
        if root_name not in self.roots:
            raise StudyReadError("INTERNAL_SAFE", "unknown authority root")
        for part in parts:
            if not part or "\x00" in part or Path(part).is_absolute() or "/" in part or "\\" in part or part in {".", ".."}:
                raise StudyReadError("INVALID_ARGUMENT", "unsafe path component")
        root = self.roots[root_name]
        candidate = root.joinpath(*parts)
        if must_exist and not candidate.exists():
            raise StudyReadError("NOT_FOUND", "requested artifact was not found")
        self._validate_no_symlink(root, candidate, allow_missing=not must_exist)
        resolved = candidate.resolve(strict=must_exist)
        if not self._contained(root, resolved):
            raise StudyReadError("INVALID_ARGUMENT", "path escaped authority root")
        return resolved

    def validate_indexed_path(self, root_name: str, candidate: Path) -> Path:
        root = self.roots[root_name]
        self._validate_no_symlink(root, candidate)
        resolved = candidate.resolve(strict=True)
        if not self._contained(root, resolved):
            raise StudyReadError("INVALID_ARGUMENT", "indexed path escaped authority root")
        return resolved

    @staticmethod
    def _contained(root: Path, candidate: Path) -> bool:
        try:
            candidate.relative_to(root)
            return True
        except ValueError:
            return False

    @staticmethod
    def _validate_no_symlink(root: Path, candidate: Path, allow_missing: bool = False) -> None:
        try:
            relative = candidate.relative_to(root)
        except ValueError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "path escaped authority root") from exc
        current = root
        for part in relative.parts:
            current = current / part
            try:
                mode = current.lstat().st_mode
            except FileNotFoundError:
                if allow_missing:
                    return
                raise StudyReadError("NOT_FOUND", "requested artifact was not found")
            if stat.S_ISLNK(mode):
                raise StudyReadError("INVALID_ARGUMENT", "symlinks are not permitted")

    def identity(self, path: Path) -> FileIdentity:
        try:
            return FileIdentity.from_stat(path.stat(follow_symlinks=False))
        except FileNotFoundError as exc:
            raise StudyReadError("NOT_FOUND", "requested artifact was not found") from exc

    def read_bytes(self, path: Path) -> ReadResult:
        before_path = self.identity(path)
        try:
            with path.open("rb") as handle:
                before_fd = FileIdentity.from_stat(os.fstat(handle.fileno()))
                if before_fd != before_path:
                    raise StudyReadError("SOURCE_CHANGED_DURING_READ", "source changed before read", True)
                data = handle.read()
                after_fd = FileIdentity.from_stat(os.fstat(handle.fileno()))
        except PermissionError as exc:
            raise StudyReadError("SUBJECT_UNAVAILABLE", "authority source is not readable") from exc
        if self.after_read_hook:
            self.after_read_hook(path)
        after_path = self.identity(path)
        if not (before_path == before_fd == after_fd == after_path):
            raise StudyReadError("SOURCE_CHANGED_DURING_READ", "source changed during read", True)
        return ReadResult(path, before_path, hashlib.sha256(data).hexdigest(), data)

    def digest(self, path: Path) -> tuple[str, FileIdentity]:
        identity = self.identity(path)
        key = ("digest", str(path), identity.token())
        cached = self.cache.get(key)
        if cached is not None:
            return cached, identity
        result = self.read_bytes(path)
        self.cache.put(key, result.sha256)
        return result.sha256, result.identity

    def _parsed(self, path: Path, parser_name: str, parser: Callable[[bytes], Any]) -> tuple[Any, str, FileIdentity]:
        identity = self.identity(path)
        key = (parser_name, str(path), identity.token())
        cached = self.cache.get(key)
        if cached is not None:
            digest, value = cached
            if self.identity(path) != identity:
                raise StudyReadError("SOURCE_CHANGED_DURING_READ", "source changed during cached read", True)
            return value, digest, identity
        result = self.read_bytes(path)
        try:
            value = parser(result.data)
        except (UnicodeDecodeError, json.JSONDecodeError, csv.Error) as exc:
            raise StudyReadError("SUBJECT_UNAVAILABLE", "authority source could not be parsed") from exc
        self.cache.put(key, (result.sha256, value))
        return deepcopy(value), result.sha256, result.identity

    def json(self, path: Path) -> tuple[Any, str, FileIdentity]:
        return self._parsed(path, "json", lambda b: json.loads(b.decode("utf-8")))

    def text(self, path: Path) -> tuple[str, str, FileIdentity]:
        return self._parsed(path, "text", lambda b: b.decode("utf-8"))

    def jsonl(self, path: Path) -> tuple[list[dict[str, Any]], str, FileIdentity]:
        def parse(data: bytes) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            for line in data.decode("utf-8").splitlines():
                if line.strip():
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
            return rows
        return self._parsed(path, "jsonl", parse)

    def csv_rows(self, path: Path) -> tuple[list[dict[str, str]], str, FileIdentity]:
        def parse(data: bytes) -> list[dict[str, str]]:
            return list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))
        return self._parsed(path, "csv", parse)

    @staticmethod
    def set_fingerprint(bindings: Iterable[tuple[str, str]]) -> str:
        canonical = "\n".join(f"{name}:{digest}" for name, digest in sorted(bindings))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
