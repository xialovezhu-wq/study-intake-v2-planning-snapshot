from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import StudyReadError


SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class ReleaseBinding:
    release_id: str
    release_manifest: Path
    manifest_sha256: str


class PreprocessorAuthority:
    """The sole symlink exception: current must bind to a content-addressed release."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve(strict=True)
        self.releases = (self.root / "releases").resolve(strict=True)
        self.objects = (self.root / "packages" / "objects").resolve(strict=True)
        self.current = self.root / "current"

    @staticmethod
    def _stable_read(path: Path) -> tuple[bytes, str]:
        before_path = path.stat(follow_symlinks=False)
        with path.open("rb") as handle:
            before_fd = os.fstat(handle.fileno())
            data = handle.read()
            after_fd = os.fstat(handle.fileno())
        after_path = path.stat(follow_symlinks=False)
        identities = [
            (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
            for value in (before_path, before_fd, after_fd, after_path)
        ]
        if len(set(identities)) != 1:
            raise StudyReadError("SOURCE_CHANGED_DURING_READ", "preprocessor artifact changed during read", True)
        return data, hashlib.sha256(data).hexdigest()

    def bind_current(self) -> ReleaseBinding:
        try:
            link_stat = self.current.lstat()
        except FileNotFoundError as exc:
            raise StudyReadError("SUBJECT_UNAVAILABLE", "preprocessor current release is missing") from exc
        if not stat.S_ISLNK(link_stat.st_mode):
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor current must be a symlink")
        target_text = os.readlink(self.current)
        target = Path(target_text)
        if not target.is_absolute():
            target = self.current.parent / target
        target = target.resolve(strict=True)
        try:
            relative = target.relative_to(self.releases)
        except ValueError as exc:
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor current escaped releases") from exc
        if len(relative.parts) != 1 or not SHA256.fullmatch(relative.name):
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor current target is not content addressed")
        manifest = target / "release.json"
        if not manifest.is_file() or manifest.is_symlink():
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor release manifest is invalid")
        data, digest = self._stable_read(manifest)
        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor release manifest could not be parsed") from exc
        if not isinstance(value, dict) or not isinstance(value.get("component_inventory"), dict):
            raise StudyReadError("AUTHORITY_DRIFT", "preprocessor release manifest schema is invalid")
        if self.current.resolve(strict=True) != target:
            raise StudyReadError("SOURCE_CHANGED_DURING_READ", "preprocessor current changed during binding", True)
        return ReleaseBinding(relative.name, manifest, digest)

    def package_object(self, object_id: str) -> tuple[Path, str, ReleaseBinding]:
        if not SHA256.fullmatch(object_id):
            raise StudyReadError("INVALID_ARGUMENT", "package object id must be a SHA-256")
        binding = self.bind_current()
        path = self.objects / f"{object_id}.json"
        if not path.is_file() or path.is_symlink():
            raise StudyReadError("NOT_FOUND", "package object was not found")
        resolved = path.resolve(strict=True)
        try:
            resolved.relative_to(self.objects)
        except ValueError as exc:
            raise StudyReadError("AUTHORITY_DRIFT", "package object escaped the object store") from exc
        _, digest = self._stable_read(resolved)
        return resolved, digest, binding
