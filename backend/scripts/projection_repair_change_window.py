#!/usr/bin/env python3
"""Recoverable live change window for two explicitly derived projection P0s.

The command has a fixed target allowlist.  It cannot change review ledgers,
English events, formal study objects, active releases, plugins, or dispatchers.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


SOURCE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOURCE_ROOT / "lib"))
sys.path.insert(0, str(SOURCE_ROOT / "bin"))

import formal_surface_manifest  # noqa: E402
from projection_rebuild_verifier import (  # noqa: E402
    CS408_PROJECTION_RELS,
    ProjectionRebuildError,
    _cs408_snapshot_paths,
    _english_modules,
    _english_snapshot_paths,
    canonical_bytes,
    sha256_bytes,
    sha256_file,
    snapshot_paths,
    verify_cs408_projection_rebuild,
    verify_english_projection_bytes,
    verify_english_projection_rebuild,
)


CHANGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SUBJECTS = ("cs408", "english")
ENGLISH_BUILDER_REL = Path("english_pipeline/views.py")


class ChangeWindowError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_repo(path: Path, code: str) -> Path:
    value = path.expanduser()
    if not value.is_absolute() or value.is_symlink() or not value.is_dir():
        raise ChangeWindowError(code)
    return value.resolve()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, raw_temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw_temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write(path, canonical_bytes(dict(value)))


def _load_json(path: Path, code: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("unsafe JSON")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ChangeWindowError(code) from exc
    if not isinstance(value, dict):
        raise ChangeWindowError(code)
    return value


def _selected_subjects(raw: str) -> tuple[str, ...]:
    return SUBJECTS if raw == "all" else (raw,)


def _targets(subject: str, repo: Path, source_id: str, study_date: str) -> tuple[Path, ...]:
    if subject == "cs408":
        return tuple(repo / relative for relative in CS408_PROJECTION_RELS)
    if subject == "english":
        return (
            repo / ENGLISH_BUILDER_REL,
            repo / "intake" / "views" / study_date / f"{source_id}-quick-capture.md",
        )
    raise ChangeWindowError("change_subject_invalid")


def _canonical_input_paths(subject: str, repo: Path, source_id: str, study_date: str) -> tuple[Path, ...]:
    if subject == "cs408":
        projection = set(CS408_PROJECTION_RELS)
        return tuple(path for path in _cs408_snapshot_paths(repo) if path not in projection)
    if subject == "english":
        projection = Path("intake/views") / study_date / f"{source_id}-quick-capture.md"
        return tuple(
            path
            for path in _english_snapshot_paths(repo, source_id, study_date)
            if path != projection
        )
    raise ChangeWindowError("change_subject_invalid")


def _repo_relative(repo: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo).as_posix()
    except ValueError as exc:
        raise ChangeWindowError("change_target_escaped_repo") from exc


def _blob_path(change_root: Path, digest: str) -> Path:
    return change_root / "backups" / "sha256" / digest[:2] / digest


def _save_blob(change_root: Path, content: bytes) -> dict[str, Any]:
    digest = sha256_bytes(content)
    path = _blob_path(change_root, digest)
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != content:
            raise ChangeWindowError("backup_content_address_collision")
    else:
        _atomic_write(path, content)
    return {"path": str(path), "sha256": digest, "byte_count": len(content)}


@contextmanager
def _subject_lock(subject: str, repo: Path) -> Iterator[None]:
    path = (
        repo / "wiki/study_vaults/408-full/state/review-loop/.lock"
        if subject == "cs408"
        else repo / "intake/locks/events.lock"
    )
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def _change_window_lock(change_root: Path) -> Iterator[None]:
    path = change_root.parent / ".projection-repair-change-window.lock"
    if path.exists() and path.is_symlink():
        raise ChangeWindowError("change_window_lock_unsafe")
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _manifest_path(change_root: Path) -> Path:
    return change_root / "recovery-manifest.json"


def _change_root(artifact_root: Path, change_id: str, live_repos: Sequence[Path]) -> Path:
    if CHANGE_ID_RE.fullmatch(change_id) is None:
        raise ChangeWindowError("change_id_invalid")
    value = artifact_root.expanduser()
    if not value.is_absolute() or value.is_symlink():
        raise ChangeWindowError("change_artifact_root_invalid")
    value = value.resolve()
    window_root = value / "change-windows"
    if window_root.exists() and window_root.is_symlink():
        raise ChangeWindowError("change_artifact_window_root_unsafe")
    change_root = window_root / change_id
    if change_root.exists() and change_root.is_symlink():
        raise ChangeWindowError("change_artifact_change_root_unsafe")
    value = change_root
    for repo in live_repos:
        try:
            value.relative_to(repo)
        except ValueError:
            continue
        raise ChangeWindowError("change_artifact_root_inside_live_repo")
    value.mkdir(parents=True, exist_ok=True, mode=0o700)
    return value


def _config(path: Path) -> dict[str, Any]:
    return _load_json(path.expanduser().resolve(), "change_config_invalid")


def _validate_recovery_manifest(
    args: argparse.Namespace,
    manifest: Mapping[str, Any],
    *,
    change_root: Path,
    repos: Mapping[str, Path],
) -> None:
    stable = {
        key: value
        for key, value in manifest.items()
        if key not in {"created_at", "manifest_sha256"}
    }
    if (
        manifest.get("schema_version")
        != "study-intake-projection-repair-recovery-manifest-v1"
        or manifest.get("change_id") != args.change_id
        or manifest.get("authorization_required_for_apply_and_rollback") is not True
        or manifest.get("model_call_count") != 0
        or manifest.get("formal_write_count") != 0
        or manifest.get("manifest_sha256") != sha256_bytes(canonical_bytes(stable))
    ):
        raise ChangeWindowError("recovery_manifest_integrity_invalid")
    expected_subjects = _selected_subjects(args.subject)
    if tuple(manifest.get("subject_scope") or ()) != expected_subjects:
        raise ChangeWindowError("recovery_manifest_scope_mismatch")
    if set((manifest.get("subjects") or {}).keys()) != set(expected_subjects):
        raise ChangeWindowError("recovery_manifest_subject_set_invalid")
    if manifest.get("english_scope") != {
        "source_id": args.english_source_id,
        "study_date": args.english_date,
    }:
        raise ChangeWindowError("recovery_manifest_english_scope_mismatch")
    for subject in expected_subjects:
        repo = repos[subject]
        row = _subject_manifest(manifest, subject)
        if row.get("repo_root") != str(repo):
            raise ChangeWindowError(f"recovery_manifest_repo_mismatch:{subject}")
        if not isinstance(row.get("canonical_input"), Mapping) or not isinstance(
            row.get("formal_surface_baseline"), Mapping
        ):
            raise ChangeWindowError(f"recovery_manifest_evidence_missing:{subject}")
        expected_paths = [
            _repo_relative(repo, path)
            for path in _targets(subject, repo, args.english_source_id, args.english_date)
        ]
        targets = row.get("targets")
        if not isinstance(targets, list) or [
            target.get("relative_path") if isinstance(target, Mapping) else None
            for target in targets
        ] != expected_paths:
            raise ChangeWindowError(f"recovery_manifest_target_allowlist_invalid:{subject}")
        for target in targets:
            relative = Path(str(target["relative_path"]))
            path = repo / relative
            if _repo_relative(repo, path) != relative.as_posix():
                raise ChangeWindowError(f"recovery_manifest_target_escaped:{subject}")
            expected_role = (
                "canonical_builder_source"
                if subject == "english" and relative == ENGLISH_BUILDER_REL
                else "rebuildable_projection"
            )
            if target.get("role") != expected_role:
                raise ChangeWindowError(f"recovery_manifest_target_role_invalid:{subject}")
            preimage = target.get("preimage")
            digest = preimage.get("sha256") if isinstance(preimage, Mapping) else None
            if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
                raise ChangeWindowError(f"recovery_manifest_preimage_invalid:{subject}")
            blob = Path(str(preimage.get("path") or ""))
            if blob != _blob_path(change_root, digest):
                raise ChangeWindowError(f"recovery_manifest_blob_path_invalid:{subject}")
            if blob.is_symlink() or not blob.is_file() or sha256_file(blob) != digest:
                raise ChangeWindowError(f"recovery_manifest_blob_invalid:{subject}")
            if preimage.get("byte_count") != blob.stat().st_size:
                raise ChangeWindowError(f"recovery_manifest_blob_size_invalid:{subject}")


def _validate_apply_receipt(
    receipt: Mapping[str, Any],
    *,
    manifest: Mapping[str, Any],
) -> None:
    stable = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        receipt.get("schema_version")
        != "study-intake-projection-repair-apply-receipt-v1"
        or receipt.get("status") != "applied_verified"
        or receipt.get("change_id") != manifest.get("change_id")
        or receipt.get("manifest_sha256") != manifest.get("manifest_sha256")
        or receipt.get("model_call_count") != 0
        or receipt.get("formal_write_count") != 0
        or receipt.get("receipt_sha256") != sha256_bytes(canonical_bytes(stable))
    ):
        raise ChangeWindowError("apply_receipt_integrity_invalid")
    subjects = receipt.get("subjects")
    expected_subjects = tuple(manifest.get("subject_scope") or ())
    if not isinstance(subjects, Mapping) or set(subjects) != set(expected_subjects):
        raise ChangeWindowError("apply_receipt_subject_set_invalid")
    for subject in expected_subjects:
        row = _subject_manifest(manifest, subject)
        result = subjects.get(subject)
        expected_paths = {target["relative_path"] for target in row["targets"]}
        post = result.get("post_target_sha256") if isinstance(result, Mapping) else None
        if (
            not isinstance(post, Mapping)
            or set(post) != expected_paths
            or result.get("formal_write_count") != 0
            or any(
                not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None
                for digest in post.values()
            )
        ):
            raise ChangeWindowError(f"apply_receipt_targets_invalid:{subject}")


def _create_backup_unlocked(args: argparse.Namespace) -> dict[str, Any]:
    cs408_repo = _safe_repo(args.cs408_repo, "cs408_repo_invalid")
    english_repo = _safe_repo(args.english_repo, "english_repo_invalid")
    repos = {"cs408": cs408_repo, "english": english_repo}
    subjects = _selected_subjects(args.subject)
    change_root = _change_root(args.artifact_root, args.change_id, tuple(repos.values()))
    manifest_path = _manifest_path(change_root)
    if manifest_path.exists():
        existing = _load_json(manifest_path, "recovery_manifest_invalid")
        _validate_recovery_manifest(
            args, existing, change_root=change_root, repos=repos
        )
        return existing
    config = _config(args.config)
    subject_rows: dict[str, Any] = {}
    for subject in subjects:
        repo = repos[subject]
        with _subject_lock(subject, repo):
            target_rows: list[dict[str, Any]] = []
            for target in _targets(subject, repo, args.english_source_id, args.english_date):
                if target.is_symlink() or not target.is_file():
                    raise ChangeWindowError("change_target_missing_or_unsafe")
                content = target.read_bytes()
                target_rows.append(
                    {
                        "relative_path": _repo_relative(repo, target),
                        "role": (
                            "canonical_builder_source"
                            if subject == "english" and target == repo / ENGLISH_BUILDER_REL
                            else "rebuildable_projection"
                        ),
                        "preimage": _save_blob(change_root, content),
                    }
                )
            input_paths = _canonical_input_paths(
                subject, repo, args.english_source_id, args.english_date
            )
            canonical_input = snapshot_paths(repo, input_paths)
            formal_baseline = formal_surface_manifest.capture(config, subjects=(subject,))
        subject_rows[subject] = {
            "repo_root": str(repo),
            "targets": target_rows,
            "canonical_input": canonical_input,
            "formal_surface_baseline": formal_baseline,
        }
    stable = {
        "schema_version": "study-intake-projection-repair-recovery-manifest-v1",
        "change_id": args.change_id,
        "subject_scope": list(subjects),
        "english_scope": {
            "source_id": args.english_source_id,
            "study_date": args.english_date,
        },
        "subjects": subject_rows,
        "authorization_required_for_apply_and_rollback": True,
        "model_call_count": 0,
        "formal_write_count": 0,
    }
    manifest = {**stable, "created_at": _utc_now()}
    manifest["manifest_sha256"] = sha256_bytes(canonical_bytes(stable))
    _atomic_json(manifest_path, manifest)
    _validate_recovery_manifest(args, manifest, change_root=change_root, repos=repos)
    return manifest


def create_backup(args: argparse.Namespace) -> dict[str, Any]:
    cs408_repo = _safe_repo(args.cs408_repo, "cs408_repo_invalid")
    english_repo = _safe_repo(args.english_repo, "english_repo_invalid")
    change_root = _change_root(
        args.artifact_root, args.change_id, (cs408_repo, english_repo)
    )
    with _change_window_lock(change_root):
        return _create_backup_unlocked(args)


@contextmanager
def _load_408_module(repo: Path) -> Iterator[Any]:
    scripts = repo / "scripts"
    path = scripts / "review_feedback_loop.py"
    previous_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(scripts))
    name = f"projection_change_review_feedback_{os.getpid()}"
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ChangeWindowError("cs408_canonical_rebuilder_import_failed")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.modules.pop(name, None)
        sys.path.remove(str(scripts))
        sys.dont_write_bytecode = previous_bytecode


def _subject_manifest(manifest: Mapping[str, Any], subject: str) -> dict[str, Any]:
    rows = manifest.get("subjects")
    row = rows.get(subject) if isinstance(rows, Mapping) else None
    if not isinstance(row, dict):
        raise ChangeWindowError("recovery_manifest_subject_missing")
    return row


def _verify_target_preimages(subject: str, repo: Path, row: Mapping[str, Any], *, english_builder_sha: str | None) -> None:
    for target in row.get("targets") or []:
        relative = Path(str(target.get("relative_path") or ""))
        path = repo / relative
        actual = sha256_file(path) if path.is_file() and not path.is_symlink() else None
        expected = (target.get("preimage") or {}).get("sha256")
        if (
            subject == "english"
            and relative == ENGLISH_BUILDER_REL
            and english_builder_sha is not None
        ):
            if actual != english_builder_sha:
                raise ChangeWindowError("english_builder_candidate_hash_mismatch")
        elif actual != expected:
            raise ChangeWindowError(f"change_target_drift:{subject}:{relative.as_posix()}")


def _stage_english_builder_candidate(
    args: argparse.Namespace,
    change_root: Path,
    *,
    repos: Mapping[str, Path],
) -> tuple[bytes, dict[str, Any]]:
    path = args.english_builder_candidate
    digest = args.expected_english_builder_sha256
    if (
        path is None
        or not isinstance(digest, str)
        or SHA256_RE.fullmatch(digest) is None
    ):
        raise ChangeWindowError("english_builder_candidate_required")
    path = path.expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ChangeWindowError("english_builder_candidate_unsafe")
    path = path.resolve()
    for repo in repos.values():
        try:
            path.relative_to(repo)
        except ValueError:
            continue
        raise ChangeWindowError("english_builder_candidate_inside_live_repo")
    content = path.read_bytes()
    if sha256_bytes(content) != digest:
        raise ChangeWindowError("english_builder_candidate_hash_mismatch")
    staged = _save_blob(change_root / "candidates", content)
    return content, {"source_path": str(path), **staged}


def _formal_allowed_verification(
    baseline: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    subject: str,
    allowed_relative_paths: set[str],
) -> dict[str, Any]:
    before = baseline["subjects"][subject]["files"]
    after = current["subjects"][subject]["files"]
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(key for key in set(before) & set(after) if before[key] != after[key])
    unexpected = sorted((set(added) | set(removed) | set(changed)) - allowed_relative_paths)
    return {
        "status": "allowed_derived_changes_only" if not unexpected else "unexpected_change",
        "added": added,
        "removed": removed,
        "changed": changed,
        "unexpected": unexpected,
        "formal_write_count": 0,
    }


def _restore_subject_locked(subject: str, repo: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    restored: list[dict[str, Any]] = []
    for target in row.get("targets") or []:
        relative = Path(str(target["relative_path"]))
        blob = Path(str(target["preimage"]["path"]))
        expected = str(target["preimage"]["sha256"])
        if blob.is_symlink() or not blob.is_file() or sha256_file(blob) != expected:
            raise ChangeWindowError("recovery_blob_invalid")
        path = repo / relative
        _atomic_write(path, blob.read_bytes())
        if sha256_file(path) != expected:
            raise ChangeWindowError("recovery_target_verification_failed")
        restored.append({"relative_path": relative.as_posix(), "sha256": expected})
    return {"subject": subject, "restored": restored, "formal_write_count": 0}


def _apply_cs408_locked(
    args: argparse.Namespace, change_root: Path, row: Mapping[str, Any]
) -> dict[str, Any]:
    repo = Path(str(row["repo_root"]))
    input_paths = _canonical_input_paths("cs408", repo, args.english_source_id, args.english_date)
    if snapshot_paths(repo, input_paths) != row["canonical_input"]:
        raise ChangeWindowError("cs408_canonical_input_drift")
    with _load_408_module(repo) as module:
        runtime = module._write_runtime_views_locked(repo)
        audit = module.audit(repo)
    if audit.get("status") != "PASS" or audit.get("formal_write_count") != 0:
        raise ChangeWindowError(f"cs408_canonical_audit_failed:{audit.get('errors')}")
    if snapshot_paths(repo, input_paths) != row["canonical_input"]:
        raise ChangeWindowError("cs408_canonical_input_changed")
    post_targets = {
        target["relative_path"]: sha256_file(repo / target["relative_path"])
        for target in row["targets"]
    }
    verification = verify_cs408_projection_rebuild(
        repo, change_root / "post-apply-verification"
    )
    if verification["live_gate_status"] != "closed":
        raise ChangeWindowError("cs408_post_apply_deterministic_verification_failed")
    return {
        "subject": "cs408",
        "runtime": runtime,
        "audit": audit,
        "post_target_sha256": post_targets,
        "deterministic_verification": verification,
        "formal_write_count": 0,
    }


def _apply_english_locked(
    args: argparse.Namespace,
    change_root: Path,
    row: Mapping[str, Any],
    *,
    builder_candidate: bytes,
    builder_candidate_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    repo = Path(str(row["repo_root"]))
    input_paths = _canonical_input_paths(
        "english", repo, args.english_source_id, args.english_date
    )
    if snapshot_paths(repo, input_paths) != row["canonical_input"]:
        raise ChangeWindowError("english_canonical_input_drift")
    _atomic_write(repo / ENGLISH_BUILDER_REL, builder_candidate)
    if sha256_file(repo / ENGLISH_BUILDER_REL) != builder_candidate_artifact["sha256"]:
        raise ChangeWindowError("english_builder_candidate_install_failed")
    with _english_modules(repo) as (events_module, views_module, util_module):
        path, text = views_module.write_quick_capture_view(
            repo / "intake",
            article_id=args.english_source_id,
            study_date=args.english_date,
        )
        events = events_module.load_events(repo / "intake")
        rerender = views_module.render_quick_capture(
            events,
            article_id=args.english_source_id,
            study_date=args.english_date,
        ).encode("utf-8")
    if path.read_bytes() != rerender or text.encode("utf-8") != rerender:
        raise ChangeWindowError("english_canonical_rerender_mismatch")
    if snapshot_paths(repo, input_paths) != row["canonical_input"]:
        raise ChangeWindowError("english_canonical_input_changed")
    post_targets = {
        target["relative_path"]: sha256_file(repo / target["relative_path"])
        for target in row["targets"]
    }
    verification = verify_english_projection_rebuild(
        repo,
        change_root / "post-apply-verification",
        source_id=args.english_source_id,
        study_date=args.english_date,
    )
    if verification["live_gate_status"] != "closed":
        raise ChangeWindowError("english_post_apply_deterministic_verification_failed")
    return {
        "subject": "english",
        "view_path": str(path),
        "builder_candidate_artifact": dict(builder_candidate_artifact),
        "post_target_sha256": post_targets,
        "deterministic_verification": verification,
        "formal_write_count": 0,
    }


def apply_change(args: argparse.Namespace) -> dict[str, Any]:
    if args.authorization != args.change_id:
        raise ChangeWindowError("change_authorization_mismatch")
    cs408_repo = _safe_repo(args.cs408_repo, "cs408_repo_invalid")
    english_repo = _safe_repo(args.english_repo, "english_repo_invalid")
    change_root = _change_root(
        args.artifact_root, args.change_id, (cs408_repo, english_repo)
    )
    repos = {"cs408": cs408_repo, "english": english_repo}
    with _change_window_lock(change_root):
        manifest = _load_json(_manifest_path(change_root), "recovery_manifest_missing")
        _validate_recovery_manifest(
            args, manifest, change_root=change_root, repos=repos
        )
        config = _config(args.config)
        with contextlib.ExitStack() as locks:
            for subject in manifest["subject_scope"]:
                locks.enter_context(_subject_lock(subject, repos[subject]))
            receipt_path = change_root / "apply-receipt.json"
            if receipt_path.exists():
                existing = _load_json(receipt_path, "apply_receipt_invalid")
                _validate_apply_receipt(existing, manifest=manifest)
                for subject in manifest["subject_scope"]:
                    row = _subject_manifest(manifest, subject)
                    expected = existing["subjects"][subject]["post_target_sha256"]
                    for target in row["targets"]:
                        relative = target["relative_path"]
                        if sha256_file(repos[subject] / relative) != expected[relative]:
                            raise ChangeWindowError(
                                f"applied_state_drift:{subject}:{relative}"
                            )
                return existing
            results: dict[str, Any] = {}
            english_builder_candidate: bytes | None = None
            english_builder_artifact: Mapping[str, Any] | None = None
            if "english" in manifest["subject_scope"]:
                (
                    english_builder_candidate,
                    english_builder_artifact,
                ) = _stage_english_builder_candidate(args, change_root, repos=repos)
            # These checks do not write.  Keep them outside the rollback block
            # so a pre-existing drift is never overwritten by recovery.
            for subject in manifest["subject_scope"]:
                _verify_target_preimages(
                    subject,
                    repos[subject],
                    _subject_manifest(manifest, subject),
                    english_builder_sha=None,
                )
            try:
                for subject in manifest["subject_scope"]:
                    row = _subject_manifest(manifest, subject)
                    result = (
                        _apply_cs408_locked(args, change_root, row)
                        if subject == "cs408"
                        else _apply_english_locked(
                            args,
                            change_root,
                            row,
                            builder_candidate=english_builder_candidate,
                            builder_candidate_artifact=english_builder_artifact,
                        )
                    )
                    current_formal = formal_surface_manifest.capture(
                        config, subjects=(subject,)
                    )
                    allowed = {
                        target["relative_path"]
                        for target in row["targets"]
                        if target["role"] == "rebuildable_projection"
                    }
                    formal_check = _formal_allowed_verification(
                        row["formal_surface_baseline"],
                        current_formal,
                        subject=subject,
                        allowed_relative_paths=allowed,
                    )
                    if formal_check["status"] != "allowed_derived_changes_only":
                        raise ChangeWindowError(
                            f"formal_surface_unexpected_change:{subject}"
                        )
                    result["formal_surface_verification"] = formal_check
                    results[subject] = result
                receipt = {
                    "schema_version": "study-intake-projection-repair-apply-receipt-v1",
                    "status": "applied_verified",
                    "change_id": args.change_id,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "subjects": results,
                    "applied_at": _utc_now(),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
                receipt["receipt_sha256"] = sha256_bytes(canonical_bytes(receipt))
                _atomic_json(receipt_path, receipt)
                return receipt
            except BaseException as exc:
                rollback_rows: list[dict[str, Any]] = []
                rollback_errors: list[dict[str, str]] = []
                for subject in reversed(list(manifest["subject_scope"])):
                    row = _subject_manifest(manifest, subject)
                    try:
                        rollback_rows.append(
                            _restore_subject_locked(subject, repos[subject], row)
                        )
                    except BaseException as rollback_exc:
                        rollback_errors.append(
                            {"subject": subject, "error": str(rollback_exc)}
                        )
                failure = {
                    "schema_version": "study-intake-projection-repair-failed-rollback-v1",
                    "status": (
                        "failed_rollback_incomplete"
                        if rollback_errors
                        else "failed_rolled_back"
                    ),
                    "change_id": args.change_id,
                    "error": str(exc),
                    "rollback": rollback_rows,
                    "rollback_errors": rollback_errors,
                    "recorded_at": _utc_now(),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
                _atomic_json(change_root / "failed-rollback-receipt.json", failure)
                if rollback_errors:
                    raise ChangeWindowError(
                        f"change_failed_rollback_incomplete:{exc}"
                    ) from exc
                raise ChangeWindowError(f"change_failed_and_rolled_back:{exc}") from exc


def rollback_change(args: argparse.Namespace) -> dict[str, Any]:
    if args.authorization != args.change_id:
        raise ChangeWindowError("change_authorization_mismatch")
    cs408_repo = _safe_repo(args.cs408_repo, "cs408_repo_invalid")
    english_repo = _safe_repo(args.english_repo, "english_repo_invalid")
    change_root = _change_root(
        args.artifact_root, args.change_id, (cs408_repo, english_repo)
    )
    repos = {"cs408": cs408_repo, "english": english_repo}
    with _change_window_lock(change_root):
        manifest = _load_json(_manifest_path(change_root), "recovery_manifest_missing")
        apply_receipt = _load_json(
            change_root / "apply-receipt.json", "apply_receipt_missing"
        )
        _validate_recovery_manifest(
            args, manifest, change_root=change_root, repos=repos
        )
        _validate_apply_receipt(apply_receipt, manifest=manifest)
        with contextlib.ExitStack() as locks:
            for subject in manifest["subject_scope"]:
                locks.enter_context(_subject_lock(subject, repos[subject]))
            postimages: list[tuple[Path, bytes, str]] = []
            for subject in manifest["subject_scope"]:
                row = _subject_manifest(manifest, subject)
                expected_post = apply_receipt["subjects"][subject][
                    "post_target_sha256"
                ]
                for target in row["targets"]:
                    relative = target["relative_path"]
                    path = repos[subject] / relative
                    content = path.read_bytes()
                    if sha256_bytes(content) != expected_post[relative]:
                        raise ChangeWindowError(
                            f"rollback_target_drift:{subject}:{relative}"
                        )
                    _save_blob(change_root / "rollback-postimages", content)
                    postimages.append((path, content, expected_post[relative]))
            rows: list[dict[str, Any]] = []
            try:
                for subject in reversed(manifest["subject_scope"]):
                    rows.append(
                        _restore_subject_locked(
                            subject,
                            repos[subject],
                            _subject_manifest(manifest, subject),
                        )
                    )
                receipt = {
                    "schema_version": "study-intake-projection-repair-rollback-receipt-v1",
                    "status": "rolled_back_verified",
                    "change_id": args.change_id,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "restored_subjects": rows,
                    "rolled_back_at": _utc_now(),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
                receipt["receipt_sha256"] = sha256_bytes(canonical_bytes(receipt))
                _atomic_json(change_root / "rollback-receipt.json", receipt)
                return receipt
            except BaseException as exc:
                rollforward_errors: list[dict[str, str]] = []
                for path, content, expected in postimages:
                    try:
                        _atomic_write(path, content)
                        if sha256_file(path) != expected:
                            raise ChangeWindowError(
                                "rollback_rollforward_verification_failed"
                            )
                    except BaseException as rollforward_exc:
                        rollforward_errors.append(
                            {"path": str(path), "error": str(rollforward_exc)}
                        )
                failure = {
                    "schema_version": "study-intake-projection-repair-rollback-failure-v1",
                    "status": (
                        "rollback_failed_state_restored"
                        if not rollforward_errors
                        else "rollback_failed_state_incomplete"
                    ),
                    "change_id": args.change_id,
                    "error": str(exc),
                    "rollforward_errors": rollforward_errors,
                    "recorded_at": _utc_now(),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
                _atomic_json(change_root / "failed-rollback-command-receipt.json", failure)
                raise ChangeWindowError(f"rollback_failed_and_rolled_forward:{exc}") from exc


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("command", choices=("backup", "apply", "rollback"))
    value.add_argument("--subject", choices=("cs408", "english", "all"), required=True)
    value.add_argument("--change-id", required=True)
    value.add_argument("--artifact-root", type=Path, required=True)
    value.add_argument("--config", type=Path, required=True)
    value.add_argument("--authorization")
    value.add_argument("--english-builder-candidate", type=Path)
    value.add_argument("--expected-english-builder-sha256")
    value.add_argument("--english-source-id", default="RAW-ARTICLE-20260710-001")
    value.add_argument("--english-date", default="2026-08-06")
    value.add_argument("--cs408-repo", type=Path, default=Path("/Users/xiazhibin/Documents/kaoyan-408"))
    value.add_argument("--english-repo", type=Path, default=Path("/Users/xiazhibin/Documents/kaoyan-english"))
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "backup":
            result = create_backup(args)
        elif args.command == "apply":
            result = apply_change(args)
        else:
            result = rollback_change(args)
    except (ChangeWindowError, ProjectionRebuildError, formal_surface_manifest.ManifestError) as exc:
        result = {
            "schema_version": "study-intake-projection-repair-command-error-v1",
            "status": "failed_closed",
            "error_code": str(exc),
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        sys.stdout.buffer.write(canonical_bytes(result))
        return 2
    sys.stdout.buffer.write(canonical_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
