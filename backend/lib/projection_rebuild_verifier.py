"""Read-only projection repair verification for the 408 and English P0 gates.

The verifier never repairs a live repository.  It copies the minimum canonical
inputs into two isolated repositories, invokes the subject-owned deterministic
rebuild implementation there, and proves that both rebuilt byte streams agree.
The resulting receipt is therefore repair-capability evidence, not evidence
that the live projection has already been changed.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


SCHEMA_VERSION = "study-intake-projection-rebuild-verification-v1"
CS408_LOOP_REL = Path("wiki/study_vaults/408-full/state/review-loop")
CS408_MORNING_REL = Path("wiki/study_vaults/408-full/state/morning-review")
CS408_POINTER_REL = Path(
    "wiki/study_vaults/408-full/StudyVault/00-Dashboard/当前晨间复盘.md"
)
CS408_PROJECTION_RELS = (
    CS408_LOOP_REL / "state.json",
    CS408_LOOP_REL / "runtime.json",
    CS408_LOOP_REL / "dashboard.json",
    CS408_POINTER_REL,
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ENGLISH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ProjectionRebuildError(RuntimeError):
    """Fail-closed projection rebuild verification error."""


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_repo(path: Path, code: str) -> Path:
    value = path.expanduser()
    if not value.is_absolute() or value.is_symlink() or not value.is_dir():
        raise ProjectionRebuildError(code)
    return value.resolve()


def _safe_output_root(path: Path, live_repos: Sequence[Path]) -> Path:
    value = path.expanduser()
    if not value.is_absolute() or value.is_symlink():
        raise ProjectionRebuildError("projection_artifact_root_invalid")
    value = value.resolve()
    for repo in live_repos:
        if value == repo or _is_within(value, repo):
            raise ProjectionRebuildError("projection_artifact_root_inside_live_repo")
    value.mkdir(parents=True, exist_ok=True, mode=0o700)
    return value


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _safe_relative(raw: str | Path) -> Path:
    value = Path(str(raw))
    if value.is_absolute() or not value.parts or any(part in {"", ".", ".."} for part in value.parts):
        raise ProjectionRebuildError("projection_source_relative_path_invalid")
    return value


def _file_row(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ProjectionRebuildError("projection_source_file_invalid")
    return {"byte_count": path.stat().st_size, "sha256": sha256_file(path)}


def snapshot_paths(repo: Path, relative_paths: Iterable[Path]) -> dict[str, Any]:
    rows: dict[str, dict[str, Any]] = {}
    for raw in sorted({_safe_relative(path).as_posix() for path in relative_paths}):
        path = repo / raw
        if path.is_symlink():
            raise ProjectionRebuildError("projection_source_symlink")
        if path.is_file():
            rows[raw] = {"exists": True, **_file_row(path)}
        elif path.exists():
            raise ProjectionRebuildError("projection_source_not_regular_file")
        else:
            rows[raw] = {"exists": False, "byte_count": 0, "sha256": None}
    stable = {"repo": str(repo), "files": rows}
    return {
        **stable,
        "file_count": sum(row["exists"] for row in rows.values()),
        "content_sha256": sha256_bytes(canonical_bytes(stable)),
    }


def _copy_snapshot(repo: Path, destination: Path, relative_paths: Iterable[Path]) -> None:
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    for relative in sorted({_safe_relative(path) for path in relative_paths}, key=lambda item: item.as_posix()):
        source = repo / relative
        if not source.exists():
            continue
        if source.is_symlink() or not source.is_file():
            raise ProjectionRebuildError("projection_source_file_invalid")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copyfile(source, target)
        os.chmod(target, 0o600)


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
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _write_content_addressed(root: Path, suffix: str, content: bytes) -> dict[str, Any]:
    digest = sha256_bytes(content)
    path = root / "sha256" / digest[:2] / f"{digest}{suffix}"
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != content:
            raise ProjectionRebuildError("projection_content_address_collision")
    else:
        _atomic_write(path, content)
    return {"path": str(path), "sha256": digest, "byte_count": len(content)}


def _load_json(path: Path, code: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("unsafe JSON")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProjectionRebuildError(code) from exc
    if not isinstance(value, dict):
        raise ProjectionRebuildError(code)
    return value


def _parse_jsonl_high_water(path: Path) -> dict[str, Any]:
    event_ids: list[str] = []
    logical_rows: list[str] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProjectionRebuildError("cs408_review_ledger_invalid") from exc
        event_id = value.get("event_id") if isinstance(value, dict) else None
        if not isinstance(event_id, str) or not event_id or event_id in event_ids:
            raise ProjectionRebuildError("cs408_review_ledger_identity_invalid")
        event_ids.append(event_id)
        logical_rows.append(f"{event_id}:{sha256_bytes(canonical_bytes(value).rstrip(b'\n'))}")
    return {
        "file_sha256": sha256_file(path),
        "byte_count": path.stat().st_size,
        "event_count": len(event_ids),
        "last_event_id": event_ids[-1] if event_ids else None,
        "logical_high_water_sha256": sha256_bytes("\n".join(logical_rows).encode("utf-8")),
    }


def _cs408_snapshot_paths(repo: Path) -> tuple[Path, ...]:
    paths: set[Path] = {
        CS408_LOOP_REL / "events.jsonl",
        *CS408_PROJECTION_RELS,
    }
    morning_root = repo / CS408_MORNING_REL
    if morning_root.is_symlink() or not morning_root.is_dir():
        raise ProjectionRebuildError("cs408_morning_session_root_invalid")
    for directory in sorted(morning_root.iterdir()):
        if directory.is_symlink() or not directory.is_dir():
            raise ProjectionRebuildError("cs408_morning_session_entry_invalid")
        for name in ("events.jsonl", "state.json"):
            paths.add(directory.relative_to(repo) / name)
        state = _load_json(directory / "state.json", "cs408_morning_session_state_invalid")
        for key in ("queue_path", "delivery_pack_ref"):
            reference = state.get(key)
            if key == "delivery_pack_ref" and reference in {None, ""}:
                # Legacy sessions predate prepared-pack binding.  The canonical
                # audit accepts those closed sessions and does not invent a ref.
                continue
            if not isinstance(reference, str) or not reference:
                raise ProjectionRebuildError("cs408_morning_session_binding_incomplete")
            paths.add(_safe_relative(reference))
    return tuple(sorted(paths, key=lambda item: item.as_posix()))


def _run_json_command(arguments: Sequence[str], *, code: str) -> dict[str, Any]:
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
        env=environment,
    )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProjectionRebuildError(f"{code}:invalid_json:{completed.stderr[-1000:]}") from exc
    if completed.returncode != 0:
        raise ProjectionRebuildError(f"{code}:exit_{completed.returncode}:{value}")
    if not isinstance(value, dict):
        raise ProjectionRebuildError(f"{code}:invalid_result")
    return value


def _projection_bytes(repo: Path, relative: Path) -> bytes:
    path = repo / relative
    if path.is_symlink() or not path.is_file():
        raise ProjectionRebuildError("projection_rebuild_output_missing")
    return path.read_bytes()


def _backup_plan(subject: str, targets: Sequence[str]) -> dict[str, Any]:
    return {
        "schema_version": "study-intake-derived-projection-change-plan-v1",
        "subject": subject,
        "authorization_required": True,
        "change_scope": "rebuildable_projection_only",
        "preconditions": [
            "pause only the selected subject projection writer",
            "acquire the subject canonical projection lock",
            "recheck every canonical input hash from this receipt",
            "write content-addressed backups for every target before replacement",
        ],
        "targets": list(targets),
        "apply": [
            "render each replacement to a same-filesystem temporary file",
            "verify expected bytes and high-water bindings",
            "atomically replace projection targets",
            "rerun the canonical audit before releasing the lock",
        ],
        "rollback": [
            "stop the subject projection writer",
            "restore every target from its content-addressed backup atomically",
            "verify restored hashes and unchanged canonical inputs",
            "emit a failed-or-rollback receipt before releasing the lock",
        ],
        "formal_write_count": 0,
    }


def verify_cs408_projection_rebuild(
    repo_path: Path,
    artifact_root: Path,
    *,
    python_executable: str = sys.executable,
) -> dict[str, Any]:
    repo = _safe_repo(repo_path, "cs408_repo_invalid")
    artifacts = _safe_output_root(artifact_root, (repo,)) / "cs408"
    script = repo / "scripts" / "review_feedback_loop.py"
    if script.is_symlink() or not script.is_file():
        raise ProjectionRebuildError("cs408_canonical_rebuilder_missing")
    relative_paths = _cs408_snapshot_paths(repo)
    before = snapshot_paths(repo, relative_paths)
    implementation_paths = _implementation_paths(repo, Path("scripts"))
    implementation_before = snapshot_paths(repo, implementation_paths)
    ledger_high_water = _parse_jsonl_high_water(repo / CS408_LOOP_REL / "events.jsonl")

    with tempfile.TemporaryDirectory(prefix="cs408-projection-gate-") as raw:
        temporary = Path(raw)
        isolated_a = temporary / "a"
        isolated_b = temporary / "b"
        _copy_snapshot(repo, isolated_a, relative_paths)
        shutil.copytree(isolated_a, isolated_b)
        immutable_rels = tuple(
            path
            for path in relative_paths
            if path not in CS408_PROJECTION_RELS
        )
        immutable_before_a = snapshot_paths(isolated_a, immutable_rels)
        immutable_before_b = snapshot_paths(isolated_b, immutable_rels)
        runs: list[dict[str, Any]] = []
        for isolated in (isolated_a, isolated_b):
            reconcile = _run_json_command(
                [python_executable, str(script), "reconcile", "--repo", str(isolated)],
                code="cs408_isolated_reconcile_failed",
            )
            audit = _run_json_command(
                [python_executable, str(script), "audit", "--repo", str(isolated)],
                code="cs408_isolated_audit_failed",
            )
            if audit.get("status") != "PASS" or audit.get("formal_write_count") != 0:
                raise ProjectionRebuildError("cs408_isolated_audit_not_pass")
            outputs = {
                relative.as_posix(): _projection_bytes(isolated, relative)
                for relative in CS408_PROJECTION_RELS
            }
            runs.append({"reconcile": reconcile, "audit": audit, "outputs": outputs})
        immutable_after_a = snapshot_paths(isolated_a, immutable_rels)
        immutable_after_b = snapshot_paths(isolated_b, immutable_rels)

    after = snapshot_paths(repo, relative_paths)
    implementation_after = snapshot_paths(repo, implementation_paths)
    source_stable = before == after and implementation_before == implementation_after
    immutable_stable = (
        immutable_before_a == immutable_after_a
        and immutable_before_b == immutable_after_b
    )
    deterministic = all(
        runs[0]["outputs"][relative.as_posix()]
        == runs[1]["outputs"][relative.as_posix()]
        for relative in CS408_PROJECTION_RELS
    )
    if not source_stable:
        raise ProjectionRebuildError("cs408_live_source_drift_during_inspection")
    if not immutable_stable:
        raise ProjectionRebuildError("cs408_isolated_canonical_input_changed")
    if not deterministic:
        raise ProjectionRebuildError("cs408_projection_rebuild_nondeterministic")

    candidate_outputs: dict[str, Any] = {}
    live_matches: dict[str, bool] = {}
    for relative in CS408_PROJECTION_RELS:
        key = relative.as_posix()
        content = runs[0]["outputs"][key]
        suffix = relative.suffix or ".bin"
        candidate_outputs[key] = _write_content_addressed(artifacts / "candidate", suffix, content)
        live_path = repo / relative
        live_matches[key] = bool(
            live_path.is_file()
            and not live_path.is_symlink()
            and live_path.read_bytes() == content
        )
    live_current = all(live_matches.values())
    stable = {
        "schema_version": SCHEMA_VERSION,
        "issue_id": "CS408-AUDIT-004",
        "subject": "cs408",
        "verification_mode": "read_only_live_inspection_plus_two_isolated_rebuilds",
        "canonical_rebuilder": {
            "path": str(script),
            "sha256": sha256_file(script),
            "commands": ["reconcile", "audit"],
            "implementation_file_count": implementation_before["file_count"],
            "implementation_high_water_sha256": implementation_before["content_sha256"],
        },
        "canonical_input": {
            "snapshot_sha256": before["content_sha256"],
            "file_count": before["file_count"],
            "review_ledger": ledger_high_water,
        },
        "checks": {
            "live_source_stable_during_inspection": source_stable,
            "isolated_canonical_inputs_unchanged": immutable_stable,
            "two_rebuilds_byte_identical": deterministic,
            "isolated_canonical_audit_passed": True,
            "live_projection_matches_rebuild": live_current,
        },
        "live_projection_matches": live_matches,
        "candidate_outputs": candidate_outputs,
        "repair_capability_status": "verified",
        "live_gate_status": "closed" if live_current else "requires_authorized_change_window",
        "candidate_gate_effect": (
            "live_projection_already_current"
            if live_current
            else "isolated_rebuild_proven; live P0 remains open until canonical reconcile is authorized and audited"
        ),
        "backup_rollback_plan": _backup_plan(
            "cs408", [relative.as_posix() for relative in CS408_PROJECTION_RELS]
        ),
        "model_call_count": 0,
        "formal_write_count": 0,
    }
    receipt = {**stable, "verified_at": datetime.now(timezone.utc).isoformat()}
    receipt["receipt_sha256"] = sha256_bytes(canonical_bytes(stable))
    artifact = _write_content_addressed(
        artifacts / "receipts", ".json", canonical_bytes(receipt)
    )
    return {**receipt, "receipt_artifact": artifact}


@contextlib.contextmanager
def _english_modules(repo: Path) -> Iterator[tuple[Any, Any, Any]]:
    existing = {name for name in sys.modules if name == "english_pipeline" or name.startswith("english_pipeline.")}
    if existing:
        raise ProjectionRebuildError("english_pipeline_module_already_loaded")
    sys.path.insert(0, str(repo))
    previous_dont_write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        events_module = importlib.import_module("english_pipeline.events")
        views_module = importlib.import_module("english_pipeline.views")
        util_module = importlib.import_module("english_pipeline.util")
        yield events_module, views_module, util_module
    finally:
        sys.dont_write_bytecode = previous_dont_write_bytecode
        sys.path.remove(str(repo))
        for name in tuple(sys.modules):
            if name == "english_pipeline" or name.startswith("english_pipeline."):
                del sys.modules[name]


def _english_snapshot_paths(repo: Path, source_id: str, study_date: str) -> tuple[Path, ...]:
    paths: set[Path] = set()
    for root in (repo / "intake" / "events", repo / "intake" / "migrations" / "event-v2"):
        if root.is_symlink() or not root.is_dir():
            raise ProjectionRebuildError("english_event_authority_root_invalid")
        for path in sorted(root.rglob("*.json")):
            if path.is_symlink() or not path.is_file():
                raise ProjectionRebuildError("english_event_authority_file_invalid")
            paths.add(path.relative_to(repo))
    paths.add(Path("intake/views") / study_date / f"{source_id}-quick-capture.md")
    return tuple(sorted(paths, key=lambda item: item.as_posix()))


def _collection_manifest(repo: Path, relpaths: Iterable[Path]) -> dict[str, Any]:
    rows: list[str] = []
    details: list[dict[str, Any]] = []
    for relative in sorted(relpaths, key=lambda item: item.as_posix()):
        path = repo / relative
        if not path.is_file() or path.is_symlink():
            continue
        digest = sha256_file(path)
        rows.append(f"{relative.as_posix()}:{digest}")
        details.append(
            {
                "relative_path": relative.as_posix(),
                "sha256": digest,
                "byte_count": path.stat().st_size,
            }
        )
    return {
        "file_count": len(details),
        "files": details,
        "high_water_sha256": sha256_bytes("\n".join(rows).encode("utf-8")),
    }


def _implementation_paths(repo: Path, directory: Path) -> tuple[Path, ...]:
    root = repo / directory
    if root.is_symlink() or not root.is_dir():
        raise ProjectionRebuildError("projection_implementation_root_invalid")
    paths: list[Path] = []
    for path in sorted(root.glob("*.py")):
        if path.is_symlink() or not path.is_file():
            raise ProjectionRebuildError("projection_implementation_file_invalid")
        paths.append(path.relative_to(repo))
    if not paths:
        raise ProjectionRebuildError("projection_implementation_files_missing")
    return tuple(paths)


def english_effective_high_water(events: Sequence[Mapping[str, Any]], object_sha256: Any) -> dict[str, Any]:
    rows: list[str] = []
    event_ids: list[str] = []
    seen: set[str] = set()
    for event in sorted(events, key=lambda item: str(item.get("event_id", ""))):
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id or event_id in seen:
            raise ProjectionRebuildError("english_effective_event_identity_invalid")
        seen.add(event_id)
        event_ids.append(event_id)
        digest = str(object_sha256(dict(event)))
        if SHA256_RE.fullmatch(digest) is None:
            raise ProjectionRebuildError("english_effective_event_hash_invalid")
        rows.append(f"{event_id}:{digest}")
    return {
        "event_ids": event_ids,
        "event_count": len(event_ids),
        "high_water_sha256": sha256_bytes("\n".join(rows).encode("utf-8")),
    }


def render_english_projection_v2(
    canonical_body: str,
    *,
    source_id: str,
    study_date: str,
    effective: Mapping[str, Any],
    raw_high_water: str,
    migration_high_water: str,
) -> bytes:
    event_ids = effective.get("event_ids")
    high_water = effective.get("high_water_sha256")
    if (
        not ENGLISH_ID_RE.fullmatch(source_id)
        or not ISO_DATE_RE.fullmatch(study_date)
        or not isinstance(event_ids, list)
        or not event_ids
        or any(not isinstance(item, str) or not item for item in event_ids)
        or SHA256_RE.fullmatch(str(high_water)) is None
        or SHA256_RE.fullmatch(raw_high_water) is None
        or SHA256_RE.fullmatch(migration_high_water) is None
        or not canonical_body.endswith("\n")
    ):
        raise ProjectionRebuildError("english_projection_v2_input_invalid")
    metadata = {
        "schema_version": "english_quick_capture_projection_v2",
        "data_role": "projection",
        "source_id": source_id,
        "study_date": study_date,
        "effective_event_ids": event_ids,
        "effective_event_count": len(event_ids),
        "effective_event_high_water_sha256": high_water,
        # These are this verifier's path-and-file-hash manifests.  Their names
        # deliberately do not reuse the subject audit's separately defined raw
        # and migration authority high-water contract.
        "raw_event_manifest_high_water_sha256": raw_high_water,
        "migration_manifest_high_water_sha256": migration_high_water,
        "formal_write_count": 0,
    }
    prefix = (
        "<!-- study-intake-projection-binding-v1 "
        + json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + " -->\n"
    )
    return (prefix + canonical_body).encode("utf-8")


def verify_english_projection_bytes(
    stored: bytes,
    expected: bytes,
    *,
    effective_high_water_sha256: str,
    effective_event_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    prefix = b"<!-- study-intake-projection-binding-v1 "
    end = stored.find(b" -->\n")
    metadata: dict[str, Any] | None = None
    if stored.startswith(prefix) and end > len(prefix):
        with contextlib.suppress(UnicodeError, json.JSONDecodeError):
            candidate = json.loads(stored[len(prefix):end].decode("utf-8"))
            if isinstance(candidate, dict):
                metadata = candidate
    high_water_matches = bool(
        metadata
        and metadata.get("schema_version") == "english_quick_capture_projection_v2"
        and metadata.get("effective_event_high_water_sha256")
        == effective_high_water_sha256
        and (
            effective_event_ids is None
            or metadata.get("effective_event_ids") == list(effective_event_ids)
        )
        and metadata.get("data_role") == "projection"
        and metadata.get("formal_write_count") == 0
    )
    byte_equal = stored == expected
    return {
        "status": "bound_current" if byte_equal and high_water_matches else "stale_or_unbound",
        "byte_equal": byte_equal,
        "metadata_high_water_matches": high_water_matches,
        "stored_sha256": sha256_bytes(stored),
        "expected_sha256": sha256_bytes(expected),
    }


def verify_english_projection_rebuild(
    repo_path: Path,
    artifact_root: Path,
    *,
    source_id: str,
    study_date: str,
) -> dict[str, Any]:
    repo = _safe_repo(repo_path, "english_repo_invalid")
    if not ENGLISH_ID_RE.fullmatch(source_id) or not ISO_DATE_RE.fullmatch(study_date):
        raise ProjectionRebuildError("english_projection_scope_invalid")
    artifacts = _safe_output_root(artifact_root, (repo,)) / "english"
    relative_paths = _english_snapshot_paths(repo, source_id, study_date)
    before = snapshot_paths(repo, relative_paths)
    implementation_paths = _implementation_paths(repo, Path("english_pipeline"))
    implementation_before = snapshot_paths(repo, implementation_paths)
    event_rels = tuple(path for path in relative_paths if path.parts[:2] == ("intake", "events"))
    migration_rels = tuple(
        path for path in relative_paths if path.parts[:3] == ("intake", "migrations", "event-v2")
    )
    raw_manifest = _collection_manifest(repo, event_rels)
    migration_manifest = _collection_manifest(repo, migration_rels)
    projection_rel = Path("intake/views") / study_date / f"{source_id}-quick-capture.md"

    with tempfile.TemporaryDirectory(prefix="english-projection-gate-") as raw:
        temporary = Path(raw)
        isolated_a = temporary / "a"
        isolated_b = temporary / "b"
        _copy_snapshot(repo, isolated_a, relative_paths)
        shutil.copytree(isolated_a, isolated_b)
        with _english_modules(repo) as (events_module, views_module, util_module):
            candidates: list[dict[str, Any]] = []
            for isolated in (isolated_a, isolated_b):
                state_dir = isolated / "intake"
                events = events_module.load_events(state_dir)
                scoped = events_module.effective_sentence_events(
                    events, article_id=source_id, study_date=study_date
                )
                effective = english_effective_high_water(scoped, util_module.object_sha256)
                body = views_module.render_quick_capture(
                    events, article_id=source_id, study_date=study_date
                ).encode("utf-8")
                prefix = b"<!-- study-intake-projection-binding-v1 "
                end = body.find(b" -->\n")
                metadata: dict[str, Any] | None = None
                if body.startswith(prefix) and end > len(prefix):
                    with contextlib.suppress(UnicodeError, json.JSONDecodeError):
                        parsed = json.loads(body[len(prefix):end].decode("utf-8"))
                        if isinstance(parsed, dict):
                            metadata = parsed
                if metadata is not None:
                    if (
                        metadata.get("schema_version")
                        != "english_quick_capture_projection_v2"
                        or metadata.get("data_role") != "projection"
                        or metadata.get("source_id") != source_id
                        or metadata.get("study_date") != study_date
                        or metadata.get("effective_event_ids")
                        != effective["event_ids"]
                        or metadata.get("effective_event_count")
                        != effective["event_count"]
                        or metadata.get("effective_event_high_water_sha256")
                        != effective["high_water_sha256"]
                        or metadata.get("formal_write_count") != 0
                    ):
                        raise ProjectionRebuildError(
                            "english_canonical_projection_binding_invalid"
                        )
                    candidate = body
                    display_body = body[end + len(b" -->\n"):]
                else:
                    display_text = body.decode("utf-8")
                    candidate = render_english_projection_v2(
                        display_text,
                        source_id=source_id,
                        study_date=study_date,
                        effective=effective,
                        raw_high_water=raw_manifest["high_water_sha256"],
                        migration_high_water=migration_manifest["high_water_sha256"],
                    )
                    display_body = body
                candidates.append(
                    {
                        "effective": effective,
                        "body": display_body,
                        "projection": candidate,
                    }
                )
    after = snapshot_paths(repo, relative_paths)
    implementation_after = snapshot_paths(repo, implementation_paths)
    source_stable = before == after and implementation_before == implementation_after
    deterministic = candidates[0] == candidates[1]
    if not source_stable:
        raise ProjectionRebuildError("english_live_source_drift_during_inspection")
    if not deterministic:
        raise ProjectionRebuildError("english_projection_rebuild_nondeterministic")
    if not candidates[0]["effective"]["event_ids"]:
        raise ProjectionRebuildError("english_projection_scope_has_no_effective_events")

    expected = candidates[0]["projection"]
    live_path = repo / projection_rel
    live_bytes = live_path.read_bytes() if live_path.is_file() and not live_path.is_symlink() else b""
    live_check = verify_english_projection_bytes(
        live_bytes,
        expected,
        effective_high_water_sha256=candidates[0]["effective"]["high_water_sha256"],
        effective_event_ids=candidates[0]["effective"]["event_ids"],
    )
    stale_check = verify_english_projection_bytes(
        expected + b"stale\n",
        expected,
        effective_high_water_sha256=candidates[0]["effective"]["high_water_sha256"],
        effective_event_ids=candidates[0]["effective"]["event_ids"],
    )
    if stale_check["status"] != "stale_or_unbound":
        raise ProjectionRebuildError("english_stale_projection_fixture_not_rejected")
    candidate_output = _write_content_addressed(
        artifacts / "candidate", ".md", expected
    )
    live_current = live_check["status"] == "bound_current"
    stable = {
        "schema_version": SCHEMA_VERSION,
        "issue_id": "EN-P0-004",
        "subject": "english",
        "verification_mode": "read_only_live_inspection_plus_two_isolated_effective_event_rerenders",
        "canonical_implementation": {
            "events_path": str(repo / "english_pipeline" / "events.py"),
            "events_sha256": sha256_file(repo / "english_pipeline" / "events.py"),
            "views_path": str(repo / "english_pipeline" / "views.py"),
            "views_sha256": sha256_file(repo / "english_pipeline" / "views.py"),
            "implementation_file_count": implementation_before["file_count"],
            "implementation_high_water_sha256": implementation_before["content_sha256"],
        },
        "scope": {"source_id": source_id, "study_date": study_date},
        "canonical_input": {
            "snapshot_sha256": before["content_sha256"],
            "file_count": before["file_count"],
            "raw_event_manifest": raw_manifest,
            "migration_manifest": migration_manifest,
            "effective_events": candidates[0]["effective"],
        },
        "canonical_body": {
            "sha256": sha256_bytes(candidates[0]["body"]),
            "byte_count": len(candidates[0]["body"]),
        },
        "candidate_output": candidate_output,
        "checks": {
            "live_source_stable_during_inspection": source_stable,
            "two_rebuilds_byte_identical": deterministic,
            "effective_event_high_water_bound": True,
            "stale_fixture_rejected": True,
            "live_projection_bound_and_current": live_current,
        },
        "live_projection": {
            "path": str(live_path),
            "byte_count": len(live_bytes),
            **live_check,
        },
        "repair_capability_status": "verified",
        "live_gate_status": "closed" if live_current else "requires_authorized_change_window",
        "candidate_gate_effect": (
            "live_projection_already_current"
            if live_current
            else "effective-event rerender and fail-closed verifier proven; live P0 remains open until the subject builder publishes v2 bytes in an authorized change window"
        ),
        "backup_rollback_plan": _backup_plan("english", [projection_rel.as_posix()]),
        "model_call_count": 0,
        "formal_write_count": 0,
    }
    receipt = {**stable, "verified_at": datetime.now(timezone.utc).isoformat()}
    receipt["receipt_sha256"] = sha256_bytes(canonical_bytes(stable))
    artifact = _write_content_addressed(
        artifacts / "receipts", ".json", canonical_bytes(receipt)
    )
    return {**receipt, "receipt_artifact": artifact}
