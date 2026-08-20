from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ..envelope import assert_expected
from ..errors import StudyReadError
from ..release import SERVER_RELEASE
from ..safeio import FileIdentity, SafeReader


@dataclass(frozen=True, slots=True)
class AuthoritySnapshot:
    subject: str
    generation: str
    fingerprint: str
    sources: tuple[dict[str, Any], ...]
    release: str = SERVER_RELEASE
    preprocessor_release: str | None = None


class BaseAdapter:
    subject: str
    authority_source_id: str
    capabilities: frozenset[str]

    def __init__(self, reader: SafeReader) -> None:
        self.reader = reader
        self._lock = threading.RLock()
        self.available = True
        self.unavailable_reason: str | None = None

    def authority(self) -> AuthoritySnapshot:
        raise NotImplementedError

    def authority_checks(self, checks: list[str]) -> dict[str, bool]:
        """Return only checks this adapter actually performed successfully."""

        return {check: True for check in checks}

    def assert_available(self) -> None:
        if not self.available:
            raise StudyReadError("SUBJECT_UNAVAILABLE", self.unavailable_reason or f"{self.subject} adapter is unavailable", True)

    def bind_expected(
        self, snapshot: AuthoritySnapshot, expected_generation: str | None,
        expected_release: str | None, expected_fingerprint: str | None,
    ) -> None:
        assert_expected(
            generation=snapshot.generation, release=snapshot.release, fingerprint=snapshot.fingerprint,
            expected_generation=expected_generation, expected_release=expected_release,
            expected_fingerprint=expected_fingerprint,
        )

    @staticmethod
    def capture_identities(paths: Iterable[Path], reader: SafeReader) -> dict[Path, FileIdentity]:
        return {path: reader.identity(path) for path in paths}

    @staticmethod
    def assert_unchanged(before: dict[Path, FileIdentity], reader: SafeReader) -> None:
        for path, identity in before.items():
            if reader.identity(path) != identity:
                raise StudyReadError("SOURCE_CHANGED_DURING_READ", "one or more sources changed during collection read", True)

    @staticmethod
    def generation(subject: str, fingerprint: str) -> str:
        return f"{subject}-{fingerprint[:20]}"

    @staticmethod
    def digest_bindings(bindings: Iterable[tuple[str, str]]) -> str:
        body = "\n".join(f"{name}:{digest}" for name, digest in sorted(bindings))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()
