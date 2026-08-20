#!/usr/bin/env python3
"""Capture and verify read-only manifests for the three formal study surfaces."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "study-intake-formal-surface-manifest-v1"
SUBJECTS = ("math", "cs408", "english")
IGNORED_NAMES = {".DS_Store"}
IGNORED_PARTS = {".git", ".pytest_cache", "__pycache__"}


class ManifestError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _configured_repo(config: dict[str, Any], subject: str) -> Path:
    adapters = config.get("adapters")
    adapter = adapters.get(subject) if isinstance(adapters, dict) else None
    raw = adapter.get("repo_root") if isinstance(adapter, dict) else None
    if not isinstance(raw, str) or not raw:
        raise ManifestError(f"missing_repo_root:{subject}")
    repo = Path(raw).resolve()
    if not repo.is_dir():
        raise ManifestError(f"repo_root_missing:{subject}")
    return repo


def formal_roots(config: dict[str, Any], subject: str) -> tuple[Path, ...]:
    repo = _configured_repo(config, subject)
    if subject == "math":
        base = repo / "错题知识网络"
        candidates = (
            base / "错题卡",
            base / "方法论库",
            base / "知识树",
            base / "wiki",
        )
    elif subject == "cs408":
        snapshot = config.get("cs408_knowledge_snapshot")
        sources = snapshot.get("sources") if isinstance(snapshot, dict) else None
        source_paths = tuple(
            repo / value
            for value in (sources.values() if isinstance(sources, dict) else ())
            if isinstance(value, str) and value
        )
        candidates = source_paths + tuple(
            repo / value
            for value in (
                "节点库",
                "复习单元卡",
                "专题链",
                "知识体系",
                "历史复习单元归档",
                "历史错题归档",
                "wiki",
            )
        )
    elif subject == "english":
        candidates = tuple(repo / value for value in ("bank", "wiki", "articles", "review"))
    else:
        raise ManifestError(f"unsupported_subject:{subject}")
    roots = tuple(path.resolve() for path in candidates if path.exists())
    if not roots:
        raise ManifestError(f"formal_surface_missing:{subject}")
    return roots


def _iter_files(root: Path) -> Iterable[Path]:
    if root.is_file():
        yield root
        return
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(
            name
            for name in dirnames
            if name not in IGNORED_PARTS and name not in IGNORED_NAMES
        )
        base = Path(directory)
        for name in sorted(filenames):
            if name in IGNORED_NAMES:
                continue
            path = base / name
            if path.is_symlink():
                raise ManifestError(f"formal_surface_symlink:{path}")
            if path.is_file():
                yield path


def _selected_subjects(subjects: Iterable[str]) -> tuple[str, ...]:
    selected = tuple(subjects)
    if (
        not selected
        or len(selected) != len(set(selected))
        or any(subject not in SUBJECTS for subject in selected)
    ):
        raise ManifestError("formal_surface_subject_scope_invalid")
    return selected


def capture(
    config: dict[str, Any], *, subjects: Iterable[str] = SUBJECTS
) -> dict[str, Any]:
    selected = _selected_subjects(subjects)
    subject_rows: dict[str, Any] = {}
    for subject in selected:
        repo = _configured_repo(config, subject)
        rows: dict[str, dict[str, Any]] = {}
        for root in formal_roots(config, subject):
            for path in _iter_files(root):
                relative = path.relative_to(repo).as_posix()
                current = {
                    "sha256": sha256_file(path),
                    "byte_count": path.stat().st_size,
                }
                previous = rows.get(relative)
                if previous is not None and previous != current:
                    raise ManifestError(f"formal_surface_overlap_conflict:{subject}:{relative}")
                rows[relative] = current
        ordered = {key: rows[key] for key in sorted(rows)}
        subject_rows[subject] = {
            "repo_root": str(repo),
            "file_count": len(ordered),
            "total_bytes": sum(row["byte_count"] for row in ordered.values()),
            "content_sha256": hashlib.sha256(canonical_bytes(ordered)).hexdigest(),
            "files": ordered,
        }
    stable = {
        "schema_version": SCHEMA_VERSION,
        "subject_scope": list(selected),
        "subjects": subject_rows,
        "formal_write_count": 0,
    }
    return {
        **stable,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "manifest_sha256": hashlib.sha256(canonical_bytes(stable)).hexdigest(),
    }


def _atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def verify(baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    if baseline.get("schema_version") != SCHEMA_VERSION:
        raise ManifestError("baseline_schema_invalid")
    expected = baseline.get("subjects")
    actual = current.get("subjects")
    expected_scope = baseline.get("subject_scope", list(SUBJECTS))
    actual_scope = current.get("subject_scope", list(SUBJECTS))
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        raise ManifestError("manifest_subjects_invalid")
    if (
        not isinstance(expected_scope, list)
        or not isinstance(actual_scope, list)
        or expected_scope != actual_scope
    ):
        raise ManifestError("formal_surface_subject_scope_mismatch")
    selected = _selected_subjects(expected_scope)
    differences: dict[str, Any] = {}
    for subject in selected:
        before = expected.get(subject)
        after = actual.get(subject)
        if not isinstance(before, dict) or not isinstance(after, dict):
            differences[subject] = {"reason": "subject_missing"}
            continue
        before_files = before.get("files") if isinstance(before.get("files"), dict) else {}
        after_files = after.get("files") if isinstance(after.get("files"), dict) else {}
        added = sorted(set(after_files) - set(before_files))
        removed = sorted(set(before_files) - set(after_files))
        changed = sorted(
            key
            for key in set(before_files) & set(after_files)
            if before_files[key] != after_files[key]
        )
        if added or removed or changed:
            differences[subject] = {
                "added": added,
                "removed": removed,
                "changed": changed,
            }
    return {
        "schema_version": "study-intake-formal-surface-verification-v1",
        "subject_scope": list(selected),
        "status": "unchanged" if not differences else "changed",
        "baseline_manifest_sha256": baseline.get("manifest_sha256"),
        "current_manifest_sha256": current.get("manifest_sha256"),
        "differences": differences,
        "formal_write_count": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("capture", "verify"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--baseline")
    parser.add_argument("--output")
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--subject", choices=SUBJECTS)
    scope.add_argument("--all-subjects", action="store_true")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    selected = SUBJECTS if args.all_subjects else (args.subject,)
    current = capture(config, subjects=selected)
    if args.command == "capture":
        result = current
        exit_code = 0
    else:
        if not args.baseline:
            raise ManifestError("baseline_required")
        baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        result = verify(baseline, current)
        exit_code = 0 if result["status"] == "unchanged" else 2
    if args.output:
        _atomic_write(Path(args.output).resolve(), result)
    sys.stdout.buffer.write(canonical_bytes(result))
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ManifestError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        raise SystemExit(1)
