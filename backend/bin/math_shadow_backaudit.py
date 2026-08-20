#!/usr/bin/env python3
"""Read-only math Luna backaudit and reproducible blind-comparison utility.

The default commands only inspect files and emit JSON to stdout.  Files are
created only when ``--write`` is supplied, and then only below the dedicated
``state/evaluations`` tree.  This module never imports the writer/consumer and
never opens the math ledger or formal cards for writing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
from historical_test_input import assert_guarded_read  # noqa: E402

DEFAULT_RUNTIME_ROOT = ROOT
DEFAULT_REPO_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-math")
DEFAULT_STUDY_DATE = "2026-08-04"
DEFAULT_EXPECTED_COUNT = 10
LEDGER_RELATIVE_PATH = Path("数学一回滚复习系统/快速入库事件.jsonl")
LEDGER_SCHEMA = "math-fast-intake-ledger-v1"

MANIFEST_SCHEMA = "study-intake-math-shadow-manifest-v1"
BLIND_SCHEMA = "study-intake-math-shadow-blind-set-v1"
JUDGMENT_SCHEMA = "study-intake-math-shadow-judgments-v1"
REPORT_SCHEMA = "study-intake-math-shadow-score-report-v1"

ADOPTION_OUTCOMES = {
    "direct",
    "minor_edit",
    "major_edit",
    "rejected",
    "fallback",
}
PASS_FAIL_NA = {"pass", "fail", "not_applicable"}
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
CAPTURE_EVENT_ID_RE = re.compile(r"^MFI-CAP-[a-f0-9]{24}$")
LEDGER_EVENT_ID_RE = re.compile(
    r"^(?:SCORE|MFI-CAP|MFI-AMD|MFI-FREEZE|MFI-ABORT|MFI-PREP|"
    r"MFI-CLOSE|MFI-INVALID)-[a-f0-9]{24}$"
)
MATH_ANALYSIS_FIELDS = {
    "schema_version",
    "report_profile",
    "executive_summary",
    "target_identity",
    "question_structure",
    "correct_reasoning_reconstruction",
    "evidence_assessment",
    "reasoning_diagnosis",
    "concept_method_analysis",
    "formalization_candidates",
    "sol_verification_plan",
    "risk_flags",
    "unresolved",
}
MATH_CLAIM_FIELDS = {
    "claim_type",
    "text",
    "provenance",
    "evidence_refs",
    "confidence",
    "counterevidence_or_boundary",
    "sol_verification_action",
}
# Codex 0.147 exec JSONL does not expose an independently verifiable model and
# reasoning identity event.  Keep this empty until a supported, versioned
# external attestation verifier is implemented; package self-report is never
# sufficient for promotion.
TRUSTED_RUNTIME_IDENTITY_PROVENANCE: frozenset[str] = frozenset()


class AuditError(RuntimeError):
    """A fail-closed evaluation error with a stable machine code."""

    def __init__(self, code: str, **details: Any) -> None:
        super().__init__(code)
        self.code = code
        self.details = details


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def sha256_value(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def ledger_sha256_value(value: object) -> str:
    """Canonical hash used by quick_intake.py (without a trailing newline)."""

    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def validate_study_date(value: Any) -> str:
    if not isinstance(value, str):
        raise AuditError("study_date_invalid", value=value)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise AuditError("study_date_invalid", value=value) from exc
    if parsed.isoformat() != value:
        raise AuditError("study_date_invalid", value=value)
    return value


def sha256_file(path: Path) -> str:
    assert_guarded_read(path)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    assert_guarded_read(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditError("json_read_failed", path=str(path), detail=str(exc)) from exc
    if not isinstance(value, dict):
        raise AuditError("json_object_required", path=str(path))
    return value


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    assert_guarded_read(path)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AuditError("ledger_read_failed", path=str(path), detail=str(exc)) from exc
    events: list[dict[str, Any]] = []
    for line_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AuditError(
                "ledger_json_invalid",
                path=str(path),
                line_number=line_number,
                detail=str(exc),
            ) from exc
        if not isinstance(value, dict):
            raise AuditError(
                "ledger_event_object_required",
                path=str(path),
                line_number=line_number,
            )
        events.append(value)
    return events


def ledger_event_prefix(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    identity = [
        {"event_id": event["event_id"], "content_hash": event["content_hash"]}
        for event in events
        if event.get("event_type") != "freeze"
    ]
    return {"event_count": len(identity), "events_hash": ledger_sha256_value(identity)}


def validate_canonical_ledger(events: Sequence[Mapping[str, Any]]) -> None:
    """Validate the append-only fact layer before selecting historical inputs."""

    seen: set[str] = set()
    captures: dict[str, Mapping[str, Any]] = {}
    amendments: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    freezes: dict[str, Mapping[str, Any]] = {}
    aborted_freezes: set[str] = set()
    prepares: dict[str, Mapping[str, Any]] = {}
    committed_prepares: set[str] = set()
    invalidated_prepares: set[str] = set()
    used_freezes: dict[str, str] = {}
    closed_by: dict[str, str] = {}
    closeouts: dict[str, Mapping[str, Any]] = {}
    invalidated_closeouts: set[str] = set()
    for index, event in enumerate(events):
        event_id = event.get("event_id")
        if event.get("schema_version") != LEDGER_SCHEMA:
            raise AuditError("ledger_schema_invalid", event_index=index)
        if not isinstance(event_id, str) or not LEDGER_EVENT_ID_RE.fullmatch(event_id):
            raise AuditError("ledger_event_id_invalid", event_index=index, event_id=event_id)
        if event_id in seen:
            raise AuditError("ledger_event_id_duplicate", event_id=event_id)
        seen.add(event_id)
        content_hash = event.get("content_hash")
        unhashed = {key: value for key, value in event.items() if key != "content_hash"}
        if (
            not isinstance(content_hash, str)
            or not SHA256_RE.fullmatch(content_hash)
            or ledger_sha256_value(unhashed) != content_hash
        ):
            raise AuditError("ledger_event_content_hash_invalid", event_id=event_id)

        event_type = event.get("event_type")
        if event_type == "capture":
            captures[event_id] = event
        elif event_type == "amendment":
            capture_id = event.get("capture_event_id")
            if capture_id not in captures:
                raise AuditError(
                    "ledger_amendment_capture_invalid",
                    event_id=event_id,
                    capture_id=capture_id,
                )
            amendments[str(capture_id)].append(event)
        elif event_type == "freeze":
            capture_ids = event.get("capture_event_ids")
            if (
                not isinstance(capture_ids, list)
                or not capture_ids
                or len(capture_ids) != len(set(capture_ids))
                or any(capture_id not in captures for capture_id in capture_ids)
            ):
                raise AuditError("ledger_freeze_capture_set_invalid", event_id=event_id)
            require_equal(
                event.get("ledger_prefix"),
                ledger_event_prefix(events[:index]),
                "ledger_freeze_prefix_mismatch",
                event_id=event_id,
            )
            snapshot_rows = event.get("capture_snapshots")
            if not isinstance(snapshot_rows, list):
                raise AuditError("ledger_freeze_snapshots_invalid", event_id=event_id)
            snapshots: dict[str, Mapping[str, Any]] = {}
            for snapshot in snapshot_rows:
                capture_id = snapshot.get("capture_event_id") if isinstance(snapshot, Mapping) else None
                if not isinstance(capture_id, str) or capture_id in snapshots:
                    raise AuditError("ledger_freeze_snapshot_duplicate", event_id=event_id)
                snapshots[capture_id] = snapshot
            if set(snapshots) != set(capture_ids):
                raise AuditError("ledger_freeze_snapshot_set_mismatch", event_id=event_id)
            if event.get("freeze_schema_version") != "math-fast-intake-freeze-v1":
                raise AuditError("ledger_freeze_schema_invalid", event_id=event_id)
            assigned: set[str] = set()
            formal_ids: set[str] = set()
            target_rows = event.get("targets")
            if not isinstance(target_rows, list) or not target_rows:
                raise AuditError("ledger_freeze_targets_invalid", event_id=event_id)
            for target_row in target_rows:
                if not isinstance(target_row, Mapping):
                    raise AuditError("ledger_freeze_target_invalid", event_id=event_id)
                grouped = target_row.get("capture_event_ids")
                formal_id = target_row.get("formal_id")
                if (
                    not isinstance(grouped, list)
                    or not grouped
                    or len(grouped) != len(set(grouped))
                    or any(value not in capture_ids or value in assigned for value in grouped)
                    or not isinstance(formal_id, str)
                    or formal_id in formal_ids
                ):
                    raise AuditError("ledger_freeze_target_binding_invalid", event_id=event_id)
                assigned.update(grouped)
                formal_ids.add(formal_id)
            if assigned != set(capture_ids):
                raise AuditError("ledger_freeze_target_set_mismatch", event_id=event_id)
            for capture_id in capture_ids:
                capture = captures[capture_id]
                snapshot = snapshots[capture_id]
                prior_amendments = list(amendments[capture_id])
                evidence = (
                    prior_amendments[-1].get("evidence")
                    if prior_amendments
                    else capture.get("evidence")
                )
                target = dict(capture.get("target") or {})
                source_bundle = capture.get("source_bundle")
                for amendment in prior_amendments:
                    patch = amendment.get("target_patch")
                    if not isinstance(patch, Mapping):
                        continue
                    if patch.get("source_hash_before"):
                        target["source_hash_before"] = patch["source_hash_before"]
                        target["identity_state"] = "source_backed_amendment"
                    if isinstance(patch.get("source_bundle"), Mapping):
                        source_bundle = patch["source_bundle"]
                expected = {
                    "capture_content_hash": capture.get("content_hash"),
                    "amendment_event_ids": [row["event_id"] for row in prior_amendments],
                    "effective_evidence_hash": ledger_sha256_value(evidence),
                    "effective_target_hash": ledger_sha256_value(target),
                    "effective_source_bundle_hash": (
                        ledger_sha256_value(source_bundle)
                        if isinstance(source_bundle, Mapping)
                        else None
                    ),
                    "requested_action": capture.get("requested_action"),
                }
                if any(snapshot.get(key) != value for key, value in expected.items()):
                    raise AuditError(
                        "ledger_freeze_snapshot_binding_mismatch",
                        event_id=event_id,
                        capture_id=capture_id,
                    )
            freezes[event_id] = event
        elif event_type == "freeze_abort":
            freeze_id = event.get("freeze_id")
            freeze = freezes.get(str(freeze_id))
            if (
                freeze is None
                or freeze_id in aborted_freezes
                or freeze_id in used_freezes
                or event.get("capture_event_ids") != freeze.get("capture_event_ids")
            ):
                raise AuditError("ledger_freeze_abort_invalid", event_id=event_id)
            aborted_freezes.add(str(freeze_id))
        elif event_type == "closeout_prepare":
            freeze_id = event.get("freeze_id")
            capture_ids = event.get("capture_event_ids")
            freeze = freezes.get(str(freeze_id))
            if (
                freeze is None
                or freeze_id in aborted_freezes
                or not isinstance(capture_ids, list)
                or not capture_ids
                or any(value not in captures for value in capture_ids)
                or capture_ids != freeze.get("capture_event_ids")
            ):
                raise AuditError("ledger_closeout_prepare_invalid", event_id=event_id)
            prepares[event_id] = event
        elif event_type == "closeout":
            freeze_id = event.get("freeze_id")
            capture_ids = event.get("capture_event_ids")
            freeze = freezes.get(str(freeze_id))
            prepare_id = event.get("prepare_id")
            prepare = prepares.get(str(prepare_id)) if prepare_id is not None else None
            if (
                freeze is None
                or freeze_id in aborted_freezes
                or freeze_id in used_freezes
                or not isinstance(capture_ids, list)
                or not capture_ids
                or any(value not in captures or value in closed_by for value in capture_ids)
                or capture_ids != freeze.get("capture_event_ids")
            ):
                raise AuditError("ledger_closeout_invalid", event_id=event_id)
            if prepare_id is not None and (
                prepare is None
                or prepare_id in invalidated_prepares
                or prepare_id in committed_prepares
                or prepare.get("freeze_id") != freeze_id
                or prepare.get("capture_event_ids") != capture_ids
                or prepare.get("request_hash") != event.get("request_hash")
                or prepare.get("payload_hash") != event.get("payload_hash")
                or prepare.get("generation") != event.get("generation")
            ):
                raise AuditError("ledger_closeout_prepare_binding_invalid", event_id=event_id)
            for capture_id in capture_ids:
                closed_by[str(capture_id)] = event_id
            if prepare_id is not None:
                committed_prepares.add(str(prepare_id))
            used_freezes[str(freeze_id)] = event_id
            closeouts[event_id] = event
        elif event_type == "closeout_invalidation":
            prepare_id = event.get("prepare_id")
            if prepare_id is not None:
                prepare = prepares.get(str(prepare_id))
                if (
                    prepare is None
                    or prepare_id in invalidated_prepares
                    or prepare_id in committed_prepares
                    or event.get("freeze_id") != prepare.get("freeze_id")
                    or event.get("capture_event_ids") != prepare.get("capture_event_ids")
                ):
                    raise AuditError("ledger_prepare_invalidation_invalid", event_id=event_id)
                invalidated_prepares.add(str(prepare_id))
            else:
                closeout_id = event.get("closeout_id")
                closeout = closeouts.get(str(closeout_id))
                if closeout is None or closeout_id in invalidated_closeouts:
                    raise AuditError("ledger_closeout_invalidation_invalid", event_id=event_id)
                for capture_id in closeout.get("capture_event_ids", []):
                    if closed_by.get(str(capture_id)) != closeout_id:
                        raise AuditError("ledger_closeout_restore_invalid", event_id=event_id)
                    del closed_by[str(capture_id)]
                freeze_id = closeout.get("freeze_id")
                if used_freezes.get(str(freeze_id)) != closeout_id:
                    raise AuditError("ledger_freeze_restore_invalid", event_id=event_id)
                del used_freezes[str(freeze_id)]
                invalidated_closeouts.add(str(closeout_id))
        else:
            raise AuditError("ledger_event_type_invalid", event_id=event_id)


def relative_path(path: Path, root: Path, *, code: str) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise AuditError(code, path=str(path), root=str(root)) from exc


def resolve_repo_path(repo_root: Path, relative: str) -> Path:
    candidate = (repo_root / relative).resolve()
    try:
        candidate.relative_to(repo_root.resolve())
    except ValueError as exc:
        raise AuditError("repo_path_escape", path=relative) from exc
    return candidate


def one(values: Sequence[Any], code: str, **details: Any) -> Any:
    if len(values) != 1:
        raise AuditError(code, count=len(values), **details)
    return values[0]


def require_equal(actual: Any, expected: Any, code: str, **details: Any) -> None:
    if actual != expected:
        raise AuditError(code, actual=actual, expected=expected, **details)


def _capture_ids_from_freeze(event: Mapping[str, Any]) -> list[str]:
    raw = event.get("capture_event_ids")
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        return []
    return list(raw)


def _find_current_formal_target(
    repo_root: Path,
    *,
    formal_id: str,
    frozen_path: str | None,
    frozen_sha256: str | None,
    identity_mode: str | None,
) -> tuple[dict[str, Any], dict[str, Any], list[str], list[str]]:
    replay_limitations: list[str] = []
    current_diagnostics: list[str] = []
    candidates: list[Path] = []
    if frozen_path:
        explicit = resolve_repo_path(repo_root, frozen_path)
        if explicit.is_file():
            candidates.append(explicit)
    if not candidates:
        card_root = repo_root / "错题知识网络/错题卡"
        candidates = sorted(card_root.glob(f"{formal_id}_*.md"))

    current: dict[str, Any]
    historical: dict[str, Any]
    if len(candidates) == 1:
        path = candidates[0]
        current_sha = sha256_file(path)
        current = {
            "status": "resolved",
            "formal_id": formal_id,
            "path": relative_path(path, repo_root, code="formal_card_path_escape"),
            "sha256": current_sha,
        }
    elif not candidates:
        current = {
            "status": "missing",
            "formal_id": formal_id,
            "path": frozen_path,
            "sha256": None,
        }
        current_diagnostics.append("current_formal_target_missing")
    else:
        current = {
            "status": "ambiguous",
            "formal_id": formal_id,
            "paths": [relative_path(path, repo_root, code="formal_card_path_escape") for path in candidates],
            "sha256": None,
        }
        current_diagnostics.append("current_formal_target_ambiguous")

    if identity_mode == "new_source_created" or frozen_path is None:
        historical = {
            "status": "not_present_at_freeze",
            "formal_id": formal_id,
            "path": None,
            "sha256": None,
            "content": None,
        }
    elif current.get("status") == "resolved" and current.get("sha256") == frozen_sha256:
        path = resolve_repo_path(repo_root, str(current["path"]))
        historical = {
            "status": "verified_unchanged",
            "formal_id": formal_id,
            "path": frozen_path,
            "sha256": current["sha256"],
            "content": path.read_text(encoding="utf-8"),
        }
    else:
        historical = {
            "status": "historical_preimage_unavailable",
            "formal_id": formal_id,
            "path": frozen_path,
            "sha256": frozen_sha256,
            "content": None,
        }
        replay_limitations.append("historical_formal_card_preimage_unavailable")
    return current, historical, replay_limitations, current_diagnostics


def _source_bundle(
    repo_root: Path,
    *,
    manifest_path: str | None,
    expected_sha256: str | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not manifest_path:
        return None, ["source_bundle_unavailable"]
    path = resolve_repo_path(repo_root, manifest_path)
    if not path.is_file():
        raise AuditError("source_manifest_missing", path=manifest_path)
    actual_sha = sha256_file(path)
    if expected_sha256:
        require_equal(
            actual_sha,
            expected_sha256,
            "source_manifest_hash_mismatch",
            path=manifest_path,
        )
    try:
        value = load_json(path)
    except AuditError:
        return None
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list):
        raise AuditError("source_artifacts_invalid", path=manifest_path)
    verified: list[dict[str, Any]] = []
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
            raise AuditError(
                "source_artifact_invalid", path=manifest_path, artifact_index=index
            )
        artifact_path = resolve_repo_path(repo_root, artifact["path"])
        if not artifact_path.is_file():
            raise AuditError("source_artifact_missing", path=artifact["path"])
        actual_artifact_sha = sha256_file(artifact_path)
        require_equal(
            actual_artifact_sha,
            artifact.get("sha256"),
            "source_artifact_hash_mismatch",
            path=artifact["path"],
        )
        actual_size = artifact_path.stat().st_size
        if artifact.get("size") is not None:
            require_equal(
                actual_size,
                artifact.get("size"),
                "source_artifact_size_mismatch",
                path=artifact["path"],
            )
        verified.append(
            {
                "path": artifact["path"],
                "role": artifact.get("role"),
                "media_type": artifact.get("media_type"),
                "sha256": actual_artifact_sha,
                "size": actual_size,
                "verified": True,
            }
        )
    return (
        {
            "manifest_path": manifest_path,
            "manifest_sha256": actual_sha,
            "manifest": value,
            "artifacts": verified,
        },
        [],
    )


def _target_source_binding(target: Mapping[str, Any]) -> tuple[str | None, str | None]:
    primary = target.get("source_binding")
    if isinstance(primary, dict):
        return primary.get("artifact_path"), primary.get("resolved_source_hash")
    supplemental = target.get("supplemental_source_bundles")
    if isinstance(supplemental, list) and len(supplemental) == 1:
        value = supplemental[0]
        if isinstance(value, dict):
            return value.get("artifact_path"), value.get("resolved_source_hash")
    return None, None


def _historical_paths(runtime_root: Path, study_date: str) -> tuple[list[Path], list[Path], list[Path]]:
    study_date = validate_study_date(study_date)
    packages = sorted((runtime_root / "packages/math" / study_date).glob("*/*.json"))
    adoptions = sorted((runtime_root / "state/adoptions/math").glob("*/*.json"))
    receipts = sorted((runtime_root / "receipts/math" / study_date).glob("*.json"))
    return packages, adoptions, receipts


def _explicit_historical_paths(
    runtime_root: Path,
    values: Mapping[str, Sequence[Path]],
) -> tuple[list[Path], list[Path], list[Path]]:
    required = ("packages", "adoptions", "receipts")
    if set(values) != set(required):
        raise AuditError("historical_explicit_path_roles_invalid")
    result: list[list[Path]] = []
    for role in required:
        rows: list[Path] = []
        for raw in values[role]:
            path = Path(raw).expanduser()
            if not path.is_absolute() or path.is_symlink() or not path.is_file():
                raise AuditError("historical_explicit_path_invalid", role=role, path=str(path))
            resolved = path.resolve()
            try:
                resolved.relative_to(runtime_root)
            except ValueError as exc:
                raise AuditError(
                    "historical_explicit_path_outside_runtime",
                    role=role,
                    path=str(path),
                ) from exc
            rows.append(resolved)
        if not rows or len(rows) != len(set(rows)):
            raise AuditError("historical_explicit_path_set_invalid", role=role)
        result.append(sorted(rows))
    return result[0], result[1], result[2]


def build_manifest(
    runtime_root: Path,
    repo_root: Path,
    study_date: str = DEFAULT_STUDY_DATE,
    expected_count: int | None = DEFAULT_EXPECTED_COUNT,
    historical_paths: Mapping[str, Sequence[Path]] | None = None,
) -> dict[str, Any]:
    """Freeze the adoption-connected historical v1 set without writing files."""

    study_date = validate_study_date(study_date)
    runtime_root = runtime_root.expanduser().resolve()
    repo_root = repo_root.expanduser().resolve()
    if historical_paths is None:
        packages_paths, adoption_paths, receipt_paths = _historical_paths(
            runtime_root, study_date
        )
    else:
        packages_paths, adoption_paths, receipt_paths = _explicit_historical_paths(
            runtime_root, historical_paths
        )
    packages = [(path, load_json(path)) for path in packages_paths]
    packages = [
        (path, value)
        for path, value in packages
        if value.get("schema_version") == "study-intake-preprocess-package-v1"
        and value.get("subject") == "math"
        and value.get("study_date") == study_date
    ]
    adoptions = [(path, load_json(path)) for path in adoption_paths]
    adoptions = [
        (path, value)
        for path, value in adoptions
        if value.get("subject") == "math" and value.get("study_date") == study_date
    ]
    receipts = [(path, load_json(path)) for path in receipt_paths]
    receipts = [
        (path, value)
        for path, value in receipts
        if value.get("subject") == "math" and value.get("study_date") == study_date
    ]

    if expected_count is not None:
        require_equal(
            len(adoptions),
            expected_count,
            "historical_adoption_count_mismatch",
            study_date=study_date,
        )
    capture_ids = [value.get("capture_id") for _, value in adoptions]
    if not all(isinstance(item, str) for item in capture_ids):
        raise AuditError("historical_adoption_capture_id_invalid")
    if len(capture_ids) != len(set(capture_ids)):
        raise AuditError("historical_adoption_capture_duplicate")

    ledger_path = repo_root / LEDGER_RELATIVE_PATH
    ledger_events = load_jsonl(ledger_path)
    validate_canonical_ledger(ledger_events)
    events_by_id = {
        event["event_id"]: event
        for event in ledger_events
        if isinstance(event.get("event_id"), str)
    }
    capture_set = set(capture_ids)
    freeze_candidates = [
        event
        for event in ledger_events
        if event.get("event_type") == "freeze"
        and event.get("study_date") == study_date
        and set(_capture_ids_from_freeze(event)) == capture_set
    ]
    freeze = one(
        freeze_candidates,
        "historical_freeze_not_unique",
        study_date=study_date,
        capture_count=len(capture_set),
    )
    ordered_capture_ids = _capture_ids_from_freeze(freeze)
    if expected_count is not None:
        require_equal(
            len(ordered_capture_ids),
            expected_count,
            "historical_freeze_count_mismatch",
        )

    snapshots = {
        item.get("capture_event_id"): item
        for item in freeze.get("capture_snapshots", [])
        if isinstance(item, dict) and isinstance(item.get("capture_event_id"), str)
    }
    targets: dict[str, dict[str, Any]] = {}
    for target in freeze.get("targets", []):
        if not isinstance(target, dict):
            continue
        for capture_id in target.get("capture_event_ids", []):
            if capture_id in targets:
                raise AuditError("historical_target_duplicate", capture_id=capture_id)
            targets[capture_id] = target

    package_by_id: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    adoption_by_capture: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    receipt_by_capture: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    for path, value in packages:
        package_by_id[str(value.get("package_id"))].append((path, value))
    for path, value in adoptions:
        adoption_by_capture[str(value.get("capture_id"))].append((path, value))
    for path, value in receipts:
        receipt_by_capture[str(value.get("capture_id"))].append((path, value))

    manifest_items: list[dict[str, Any]] = []
    for capture_id in ordered_capture_ids:
        capture = events_by_id.get(capture_id)
        if not isinstance(capture, dict) or capture.get("event_type") != "capture":
            raise AuditError("historical_capture_missing", capture_id=capture_id)
        snapshot = snapshots.get(capture_id)
        if not isinstance(snapshot, dict):
            raise AuditError("historical_capture_snapshot_missing", capture_id=capture_id)
        target = targets.get(capture_id)
        if not isinstance(target, dict):
            raise AuditError("historical_target_missing", capture_id=capture_id)
        adoption_path, adoption = one(
            adoption_by_capture[capture_id],
            "historical_adoption_not_unique",
            capture_id=capture_id,
        )
        package_path, package = one(
            package_by_id[str(adoption.get("package_id"))],
            "historical_package_not_unique",
            capture_id=capture_id,
            package_id=adoption.get("package_id"),
        )
        package_sha = sha256_file(package_path)
        require_equal(
            package_sha,
            adoption.get("package_sha256"),
            "historical_package_adoption_hash_mismatch",
            capture_id=capture_id,
        )
        require_equal(
            package.get("capture_id"),
            capture_id,
            "historical_package_capture_mismatch",
        )
        run_matches = [
            (path, value)
            for path, value in receipt_by_capture[capture_id]
            if value.get("package_id") == package.get("package_id")
        ]
        run_path, run_receipt = one(
            run_matches,
            "historical_run_receipt_not_unique",
            capture_id=capture_id,
        )
        require_equal(
            run_receipt.get("status"),
            "ready",
            "historical_run_not_ready",
            capture_id=capture_id,
        )
        for source, value in (
            ("package", package),
            ("adoption", adoption),
            ("run_receipt", run_receipt),
        ):
            require_equal(
                value.get("formal_write_count"),
                0,
                "historical_formal_write_nonzero",
                capture_id=capture_id,
                source=source,
            )

        package_binding = package.get("input_binding")
        if not isinstance(package_binding, dict):
            raise AuditError("historical_package_binding_invalid", capture_id=capture_id)
        require_equal(
            snapshot.get("capture_content_hash"),
            capture.get("content_hash"),
            "historical_capture_content_hash_mismatch",
            capture_id=capture_id,
        )
        require_equal(
            package_binding.get("original_content_hash"),
            capture.get("content_hash"),
            "historical_package_capture_hash_mismatch",
            capture_id=capture_id,
        )
        require_equal(
            package_binding.get("effective_evidence_hash"),
            snapshot.get("effective_evidence_hash"),
            "historical_effective_evidence_hash_mismatch",
            capture_id=capture_id,
        )
        require_equal(
            package_binding.get("effective_target_hash"),
            snapshot.get("effective_target_hash"),
            "historical_effective_target_hash_mismatch",
            capture_id=capture_id,
        )

        amendment_ids = snapshot.get("amendment_event_ids") or []
        amendments: list[dict[str, Any]] = []
        for amendment_id in amendment_ids:
            amendment = events_by_id.get(amendment_id)
            if not isinstance(amendment, dict):
                raise AuditError(
                    "historical_amendment_missing",
                    capture_id=capture_id,
                    amendment_id=amendment_id,
                )
            amendments.append(amendment)

        capture_bundle = capture.get("source_bundle")
        manifest_path = None
        expected_source_sha = None
        if isinstance(capture_bundle, dict):
            manifest_path = capture_bundle.get("manifest_path")
            expected_source_sha = capture_bundle.get("manifest_hash")
        if not manifest_path:
            manifest_path, expected_source_sha = _target_source_binding(target)
        source_bundle, source_limitations = _source_bundle(
            repo_root,
            manifest_path=manifest_path,
            expected_sha256=expected_source_sha,
        )
        if package_binding.get("source_manifest_hash") is not None:
            require_equal(
                source_bundle.get("manifest_sha256") if source_bundle else None,
                package_binding.get("source_manifest_hash"),
                "historical_package_source_hash_mismatch",
                capture_id=capture_id,
            )

        formal_id = target.get("formal_id")
        if not isinstance(formal_id, str):
            raise AuditError("historical_formal_id_invalid", capture_id=capture_id)
        (
            current_target,
            historical_card,
            formal_replay_limitations,
            current_formal_diagnostics,
        ) = _find_current_formal_target(
            repo_root,
            formal_id=formal_id,
            frozen_path=target.get("card_path_before"),
            frozen_sha256=target.get("card_hash_before"),
            identity_mode=target.get("identity_mode"),
        )
        limitations = source_limitations + formal_replay_limitations
        visual_required = bool(
            source_bundle
            and any(
                str(item.get("media_type", "")).startswith("image/")
                for item in source_bundle.get("artifacts", [])
            )
        )
        analysis = package.get("analysis")
        if not isinstance(analysis, dict):
            raise AuditError("historical_v1_analysis_invalid", capture_id=capture_id)
        replay_input = {
            "subject": "math",
            "study_date": study_date,
            "capture_id": capture_id,
            "target_group_key": formal_id,
            "capture_event": capture,
            "amendment_events": amendments,
            "capture_snapshot": snapshot,
            "source_bundle": source_bundle,
            "formal_target": {
                "formal_id": formal_id,
                "identity_mode": target.get("identity_mode"),
                "frozen_mapping": {
                    "path_before": target.get("card_path_before"),
                    "sha256_before": target.get("card_hash_before"),
                },
                "historical_card": historical_card,
            },
            "limitations": limitations,
            "formal_write_count": 0,
        }
        manifest_items.append(
            {
                "capture_id": capture_id,
                "formal_id": formal_id,
                "target_group_key": formal_id,
                "visual_required": visual_required,
                "capture_event_sha256": sha256_value(capture),
                "capture_snapshot": snapshot,
                "frozen_target_mapping": target,
                "current_formal_target": current_target,
                "current_formal_diagnostics": current_formal_diagnostics,
                "replay_input": replay_input,
                "replay_input_sha256": sha256_value(replay_input),
                "replay_status": (
                    "complete"
                    if not limitations
                    else "limited_by_evidence"
                ),
                "replay_limitations": limitations,
                "v1": {
                    "package_id": package.get("package_id"),
                    "package_path": relative_path(
                        package_path, runtime_root, code="package_path_escape"
                    ),
                    "package_sha256": package_sha,
                    "input_fingerprint": package.get("input_fingerprint"),
                    "analysis": analysis,
                    "analysis_sha256": sha256_value(analysis),
                    "model_receipt": package.get("model_receipt"),
                    "run_receipt_path": relative_path(
                        run_path, runtime_root, code="run_receipt_path_escape"
                    ),
                    "run_receipt_sha256": sha256_file(run_path),
                },
                "baseline_adoption": {
                    "path": relative_path(
                        adoption_path, runtime_root, code="adoption_path_escape"
                    ),
                    "sha256": sha256_file(adoption_path),
                    "outcome": adoption.get("outcome"),
                    "reason_code": adoption.get("reason_code"),
                    "adoption_receipt_id": adoption.get("adoption_receipt_id"),
                    "formal_write_count": adoption.get("formal_write_count"),
                },
                "formal_write_count": 0,
            }
        )

    selection_payload = {
        "study_date": study_date,
        "freeze_id": freeze.get("event_id"),
        "capture_ids": ordered_capture_ids,
        "capture_snapshot_hashes": [
            sha256_value(snapshots[capture_id]) for capture_id in ordered_capture_ids
        ],
    }
    adoption_counts = Counter(
        item["baseline_adoption"]["outcome"] for item in manifest_items
    )
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA,
        "subject": "math",
        "study_date": study_date,
        "selection_contract": {
            "kind": "adoption_connected_v1_freeze",
            "expected_count": expected_count,
            "actual_count": len(manifest_items),
            "freeze_id": freeze.get("event_id"),
            "ledger_path": LEDGER_RELATIVE_PATH.as_posix(),
            "ledger_sha256": sha256_file(ledger_path),
            "ledger_prefix": freeze.get("ledger_prefix"),
            "selection_sha256": sha256_value(selection_payload),
        },
        "baseline_adoption_counts": dict(sorted(adoption_counts.items())),
        "items": manifest_items,
        "integrity": {
            "formal_write_count": 0,
            "package_count": len(manifest_items),
            "adoption_count": len(manifest_items),
            "run_receipt_count": len(manifest_items),
        },
    }
    payload["manifest_sha256"] = sha256_value(manifest_integrity_payload(payload))
    return payload


def manifest_integrity_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return the frozen-data projection; current drift is audit-only metadata."""

    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    items: list[Any] = []
    for item in payload.get("items", []):
        if not isinstance(item, Mapping):
            items.append(item)
            continue
        frozen = dict(item)
        frozen.pop("current_formal_target", None)
        frozen.pop("current_formal_diagnostics", None)
        items.append(frozen)
    payload["items"] = items
    return payload


def verify_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise AuditError("manifest_schema_invalid")
    expected = manifest.get("manifest_sha256")
    require_equal(
        sha256_value(manifest_integrity_payload(manifest)),
        expected,
        "manifest_hash_mismatch",
    )
    if manifest.get("integrity", {}).get("formal_write_count") != 0:
        raise AuditError("manifest_formal_write_nonzero")
    validate_study_date(manifest.get("study_date"))
    items = manifest.get("items")
    if not isinstance(items, list) or not items:
        raise AuditError("manifest_items_invalid")
    seen_capture_ids: set[str] = set()
    for item in items:
        replay = item.get("replay_input") if isinstance(item, Mapping) else None
        formal = replay.get("formal_target") if isinstance(replay, Mapping) else None
        if not isinstance(formal, Mapping) or set(formal) != {
            "formal_id",
            "identity_mode",
            "frozen_mapping",
            "historical_card",
        }:
            raise AuditError(
                "manifest_replay_formal_target_fields_invalid",
                capture_id=item.get("capture_id") if isinstance(item, Mapping) else None,
            )
        capture_id = item.get("capture_id")
        capture = replay.get("capture_event")
        snapshot = replay.get("capture_snapshot")
        limitations = replay.get("limitations")
        if not isinstance(capture, Mapping) or not isinstance(snapshot, Mapping):
            raise AuditError("manifest_item_binding_invalid", capture_id=capture_id)
        if (
            not isinstance(capture_id, str)
            or capture_id in seen_capture_ids
            or replay.get("capture_id") != capture_id
            or capture.get("event_id") != capture_id
            or replay.get("study_date") != manifest.get("study_date")
            or replay.get("subject") != "math"
            or item.get("formal_id") != formal.get("formal_id")
            or item.get("target_group_key") != replay.get("target_group_key")
            or item.get("capture_snapshot") != snapshot
            or item.get("capture_event_sha256") != sha256_value(capture)
            or item.get("replay_input_sha256") != sha256_value(replay)
            or item.get("replay_limitations") != limitations
            or not isinstance(limitations, list)
            or item.get("replay_status")
            != ("complete" if not limitations else "limited_by_evidence")
            or replay.get("formal_write_count") != 0
            or item.get("formal_write_count") != 0
        ):
            raise AuditError(
                "manifest_item_binding_invalid",
                capture_id=capture_id,
            )
        seen_capture_ids.add(capture_id)
    selection = manifest.get("selection_contract")
    if (
        not isinstance(selection, Mapping)
        or selection.get("actual_count") != len(items)
        or selection.get("freeze_id") is None
    ):
        raise AuditError("manifest_selection_binding_invalid")


def _candidate_analysis(
    package: Mapping[str, Any], runtime_root: Path, search_root: Path
) -> dict[str, Any] | None:
    report_sha = package.get("report_json_sha256")
    if isinstance(report_sha, str) and SHA256_RE.fullmatch(report_sha):
        path = runtime_root / "private/reports/objects" / f"{report_sha}.json"
        if path.is_file() and path.stem == report_sha and sha256_file(path) == report_sha:
            return load_json(path)
    return None


def _content_addressed_json(path: Path, digest: Any) -> Mapping[str, Any] | None:
    if (
        not isinstance(digest, str)
        or not SHA256_RE.fullmatch(digest)
        or path.name != f"{digest}.json"
        or not path.is_file()
        or sha256_file(path) != digest
    ):
        return None
    value = load_json(path)
    return value if isinstance(value, Mapping) else None


def _ref_path_exists(value: Any, ref: str) -> bool:
    """Resolve the deterministic capture leaf namespace without evaluating text."""

    if not ref.startswith("capture."):
        return False
    current: Any = value
    rest = ref[len("capture.") :]
    if not re.fullmatch(r"[^.\[\]]+(?:\[\d+\])?(?:\.[^.\[\]]+(?:\[\d+\])?)*", rest):
        return False
    for key, index in re.findall(r"([^.\[\]]+)(?:\[(\d+)\])?", rest):
        if not isinstance(current, Mapping) or key not in current:
            return False
        current = current[key]
        if index:
            if not isinstance(current, list) or int(index) >= len(current):
                return False
            current = current[int(index)]
    return current not in (None, "", [], {})


def _allowed_ref_is_frozen(ref: str, item: Mapping[str, Any]) -> bool:
    replay = item.get("replay_input")
    snapshot = item.get("capture_snapshot")
    if not isinstance(replay, Mapping) or not isinstance(snapshot, Mapping):
        return False
    capture = replay.get("capture_event")
    if not isinstance(capture, Mapping):
        return False
    augmented = dict(capture)
    target = capture.get("target") if isinstance(capture.get("target"), Mapping) else {}
    source_ref = capture.get("source_bundle")
    augmented.update(
        {
            "formal_id": item.get("formal_id"),
            "source_locator": (
                source_ref.get("source_locator")
                if isinstance(source_ref, Mapping)
                else target.get("source_locator")
            ),
            "source_hash_before": target.get("source_hash_before"),
            "original_content_hash": snapshot.get("capture_content_hash"),
            "effective_evidence_hash": snapshot.get("effective_evidence_hash"),
            "effective_target_hash": snapshot.get("effective_target_hash"),
            "amendment_event_ids": snapshot.get("amendment_event_ids") or [],
            "amendment_count": len(snapshot.get("amendment_event_ids") or []),
            "identity_state": target.get("identity_state"),
            "active_freeze_ids": [],
        }
    )
    if ref.startswith("capture."):
        return _ref_path_exists(augmented, ref)
    historical = (
        (replay.get("formal_target") or {}).get("historical_card")
        if isinstance(replay.get("formal_target"), Mapping)
        else None
    )
    if ref in {"formal_card.content", "formal_card.sha256"}:
        return isinstance(historical, Mapping) and historical.get("status") == "verified_unchanged"
    source = replay.get("source_bundle")
    if ref in {"source_bundle.manifest_hash", "source_bundle.source_locator"}:
        return isinstance(source, Mapping)
    match = re.fullmatch(r"source_bundle\.artifacts\[(\d+)\]", ref)
    if match:
        artifacts = source.get("artifacts") if isinstance(source, Mapping) else None
        return (
            isinstance(artifacts, list)
            and int(match.group(1)) < len(artifacts)
            and isinstance(artifacts[int(match.group(1))], Mapping)
        )
    score_ref = capture.get("score_ref") if isinstance(capture.get("score_ref"), Mapping) else {}
    delivered_id = (
        score_ref.get("delivered_card_id")
        or score_ref.get("formal_id")
        or capture.get("delivered_card_id")
    )
    match_mode = score_ref.get("match_mode")
    fallback_id = score_ref.get("knowledge_fallback_card_id") or (
        score_ref.get("anchor_card_id") if match_mode == "knowledge_fallback" else None
    )
    always = {
        "target_identity.target_group_key",
        "target_identity.role_contract",
        "target_group.target_group_key",
        "target_group.ordered_capture_ids",
        "target_group.capture_set_sha256",
        "target_group.target_group_sha256",
        "target_group.captures[0]",
    }
    if ref in always:
        return True
    if ref == "target_identity.formal_card_id":
        return bool(item.get("formal_id"))
    if ref == "target_identity.delivered_card_id":
        return bool(delivered_id)
    if ref == "target_identity.knowledge_fallback_card_id":
        return bool(fallback_id)
    if ref == "target_identity.knowledge_match_mode":
        return bool(match_mode)
    return False


def _manifest_item_by_capture(
    manifest: Mapping[str, Any], capture_id: str
) -> Mapping[str, Any]:
    matches = [
        item
        for item in manifest.get("items", [])
        if isinstance(item, Mapping) and item.get("capture_id") == capture_id
    ]
    if len(matches) != 1:
        raise AuditError(
            "manifest_capture_not_unique", capture_id=capture_id, count=len(matches)
        )
    return matches[0]


def _validate_math_analysis_shape(
    analysis: Mapping[str, Any], allowed_refs: set[str]
) -> str | None:
    """Independent minimum schema/provenance validation for blind-eval input."""

    if set(analysis) != MATH_ANALYSIS_FIELDS:
        return "v2_analysis_fields_invalid"
    if (
        analysis.get("schema_version") != "study-intake-luna-math-analysis-v2"
        or analysis.get("report_profile") != "math_deep"
        or not isinstance(analysis.get("executive_summary"), str)
        or not analysis["executive_summary"].strip()
    ):
        return "v2_analysis_schema_invalid"
    claim_count = 0

    def visit(value: Any) -> str | None:
        nonlocal claim_count
        if isinstance(value, Mapping):
            keys = set(value)
            if keys == MATH_CLAIM_FIELDS:
                claim_count += 1
                refs = value.get("evidence_refs")
                if (
                    value.get("claim_type")
                    not in {
                        "observed_fact",
                        "evidence_bound_inference",
                        "standard_math_candidate",
                        "unresolved",
                    }
                    or value.get("provenance")
                    not in {
                        "capture",
                        "formal_card",
                        "source_bundle",
                        "image",
                        "model_inference",
                        "mixed",
                    }
                    or not isinstance(value.get("text"), str)
                    or not value["text"].strip()
                    or not isinstance(refs, list)
                    or not refs
                    or any(not isinstance(ref, str) or ref not in allowed_refs for ref in refs)
                    or value.get("confidence") not in {"low", "medium", "high"}
                    or not isinstance(value.get("counterevidence_or_boundary"), str)
                    or not isinstance(value.get("sol_verification_action"), str)
                    or not value["sol_verification_action"].strip()
                ):
                    return "v2_analysis_claim_invalid"
                image_used = any(
                    ref.startswith("source_bundle.artifacts[") for ref in refs
                )
                if image_used and value.get("provenance") not in {"image", "mixed"}:
                    return "v2_analysis_image_provenance_invalid"
                if value.get("provenance") == "image" and not image_used:
                    return "v2_analysis_image_ref_missing"
                return None
            for nested in value.values():
                problem = visit(nested)
                if problem:
                    return problem
        elif isinstance(value, list):
            for nested in value:
                problem = visit(nested)
                if problem:
                    return problem
        return None

    problem = visit(analysis)
    if problem:
        return problem
    if claim_count == 0:
        return "v2_analysis_claims_missing"
    formal = analysis.get("formalization_candidates")
    if not isinstance(formal, Mapping) or set(formal) != set(MATH_FORMALIZATION_FIELDS):
        return "v2_formalization_fields_invalid"
    return None


def _runtime_identity_is_attested(receipt: Mapping[str, Any]) -> bool:
    """Require explicit actual identity plus a versioned supported provenance."""

    return bool(
        receipt.get("status") == "ready"
        and receipt.get("runtime_identity_status") == "confirmed"
        and receipt.get("requested_model") == "gpt-5.6-luna"
        and receipt.get("requested_reasoning_effort") == "max"
        and receipt.get("runtime_model") == "gpt-5.6-luna"
        and receipt.get("runtime_reasoning_effort") == "max"
        and receipt.get("runtime_metadata_provenance")
        in TRUSTED_RUNTIME_IDENTITY_PROVENANCE
    )


def _validate_v2_binding(
    package: Mapping[str, Any],
    manifest: Mapping[str, Any],
    item: Mapping[str, Any],
    analysis: Mapping[str, Any],
    runtime_root: Path,
) -> str | None:
    required = {
        "schema_version",
        "package_id",
        "report_id",
        "subject",
        "capture_id",
        "study_date",
        "created_at",
        "input_fingerprint",
        "input_binding",
        "processing_contract_sha256",
        "processing_fingerprint",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "report_json_ref",
        "report_json_sha256",
        "report_markdown_ref",
        "report_markdown_sha256",
        "renderer_build_sha256",
        "quality_receipt_sha256",
        "pipeline_status",
        "stage_receipts",
        "quality_receipt",
        "allowed_evidence_refs",
        "formal_write_count",
    }
    if set(package) != required:
        return "v2_package_fields_invalid"
    binding = package.get("input_binding")
    if not isinstance(binding, Mapping):
        return "v2_input_binding_missing"
    snapshot = item.get("capture_snapshot")
    replay = item.get("replay_input")
    if not isinstance(snapshot, Mapping) or not isinstance(replay, Mapping):
        return "v2_manifest_replay_binding_missing"
    source = replay.get("source_bundle")
    historical = (
        (replay.get("formal_target") or {}).get("historical_card")
        if isinstance(replay.get("formal_target"), Mapping)
        else None
    )
    expected_formal_hash = (
        historical.get("sha256") if isinstance(historical, Mapping) else None
    )
    expected_source_hash = (
        source.get("manifest_sha256") if isinstance(source, Mapping) else None
    )
    expected = {
        "original_content_hash": snapshot.get("capture_content_hash"),
        "effective_evidence_hash": snapshot.get("effective_evidence_hash"),
        "effective_target_hash": snapshot.get("effective_target_hash"),
        "amendment_event_ids": snapshot.get("amendment_event_ids") or [],
        "source_manifest_hash": expected_source_hash,
        "formal_card_hash": expected_formal_hash,
        "capture_event_sha256": item.get("capture_event_sha256"),
        "replay_input_sha256": item.get("replay_input_sha256"),
        "backaudit_manifest_sha256": manifest.get("manifest_sha256"),
    }
    if any(binding.get(key) != value for key, value in expected.items()):
        return "v2_frozen_input_binding_mismatch"
    if package.get("input_fingerprint") != sha256_value(binding):
        return "v2_input_fingerprint_mismatch"
    for key in (
        "processing_contract_sha256",
        "processing_fingerprint",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
        "report_json_sha256",
        "report_markdown_sha256",
        "renderer_build_sha256",
        "quality_receipt_sha256",
    ):
        if not isinstance(package.get(key), str) or not SHA256_RE.fullmatch(
            str(package.get(key))
        ):
            return f"v2_{key}_invalid"
    for key in (
        "processing_contract_sha256",
        "evidence_manifest_sha256",
        "evidence_bundle_sha256",
    ):
        if binding.get(key) != package.get(key):
            return f"v2_{key}_binding_mismatch"
    report_sha = package.get("report_json_sha256")
    markdown_sha = package.get("report_markdown_sha256")
    if package.get("report_json_ref") != f"study-intake-report://sha256/{report_sha}":
        return "v2_report_json_ref_invalid"
    if package.get("report_markdown_ref") != (
        f"study-intake-report-markdown://sha256/{markdown_sha}"
    ):
        return "v2_report_markdown_ref_invalid"
    if (
        _content_addressed_json(
            runtime_root / "private/reports/objects" / f"{report_sha}.json",
            report_sha,
        )
        != analysis
    ):
        return "v2_report_object_invalid"
    markdown_path = runtime_root / "private/reports/markdown" / f"{markdown_sha}.md"
    if (
        not isinstance(markdown_sha, str)
        or not SHA256_RE.fullmatch(markdown_sha)
        or markdown_path.name != f"{markdown_sha}.md"
        or not markdown_path.is_file()
        or sha256_file(markdown_path) != markdown_sha
    ):
        return "v2_report_markdown_object_invalid"

    stage_receipts = package.get("stage_receipts")
    if not isinstance(stage_receipts, Mapping) or set(stage_receipts) != {
        "analysis",
        "critical_review",
    }:
        return "v2_stage_receipts_invalid"
    stage_payloads: dict[str, Mapping[str, Any]] = {}
    for stage in ("analysis", "critical_review"):
        receipt = stage_receipts.get(stage)
        if not isinstance(receipt, Mapping) or receipt.get("status") != "ready":
            return f"v2_{stage}_receipt_invalid"
        digest = receipt.get("artifact_sha256")
        if receipt.get("artifact_ref") != f"study-intake-stage://sha256/{digest}":
            return f"v2_{stage}_artifact_ref_invalid"
        artifact = _content_addressed_json(
            runtime_root / "private/reports/stages" / f"{digest}.json",
            digest,
        )
        if (
            not isinstance(artifact, Mapping)
            or set(artifact)
            != {
                "schema_version",
                "stage",
                "report_id",
                "capture_id",
                "study_date",
                "payload",
                "formal_write_count",
            }
            or artifact.get("schema_version")
            != "study-intake-luna-stage-artifact-v2"
            or artifact.get("stage") != stage
            or artifact.get("report_id") != package.get("report_id")
            or artifact.get("capture_id") != package.get("capture_id")
            or artifact.get("study_date") != package.get("study_date")
            or artifact.get("formal_write_count") != 0
            or not isinstance(artifact.get("payload"), Mapping)
            or receipt.get("result_sha256")
            != sha256_value(artifact.get("payload"))
        ):
            return f"v2_{stage}_artifact_invalid"
        stage_payloads[stage] = artifact["payload"]
    critical = stage_payloads["critical_review"]
    if (
        critical.get("schema_version")
        != "study-intake-luna-math-critical-review-v2"
        or critical.get("revised_analysis") != analysis
    ):
        return "v2_critical_review_report_binding_invalid"
    quality = package.get("quality_receipt")
    if (
        not isinstance(quality, Mapping)
        or sha256_value(quality) != package.get("quality_receipt_sha256")
        or quality.get("pipeline_status") != "two_pass_ready"
        or quality.get("two_stage_status") != "two_pass_ready"
        or quality.get("quality_gate_status") != "pass"
        or quality.get("mode") != "shadow"
        or quality.get("consumable") is not False
        or quality.get("formal_write_count") != 0
        or quality.get("processing_contract_sha256")
        != package.get("processing_contract_sha256")
        or quality.get("processing_fingerprint")
        != package.get("processing_fingerprint")
        or quality.get("report_json_ref") != package.get("report_json_ref")
        or quality.get("report_json_sha256") != report_sha
        or quality.get("report_markdown_ref") != package.get("report_markdown_ref")
        or quality.get("report_markdown_sha256") != markdown_sha
        or quality.get("renderer_build_sha256")
        != package.get("renderer_build_sha256")
        or quality.get("evidence_manifest_sha256")
        != package.get("evidence_manifest_sha256")
        or quality.get("evidence_bundle_sha256")
        != package.get("evidence_bundle_sha256")
        or quality.get("authoritative_stage") != "critical_review_revised"
        or quality.get("runtime_identity")
        != {
            stage: stage_receipts[stage].get("runtime_identity_status")
            for stage in ("analysis", "critical_review")
        }
    ):
        return "v2_quality_receipt_invalid"
    if package.get("report_json_sha256") != sha256_value(analysis):
        return "v2_report_hash_mismatch"
    refs = package.get("allowed_evidence_refs")
    if (
        not isinstance(refs, list)
        or not refs
        or len(refs) != len(set(refs))
        or any(
            not isinstance(ref, str) or not _allowed_ref_is_frozen(ref, item)
            for ref in refs
        )
    ):
        return "v2_allowed_evidence_refs_invalid"
    return _validate_math_analysis_shape(analysis, set(refs))


def discover_v2(
    manifest: Mapping[str, Any],
    runtime_root: Path,
    v2_dir: Path | None = None,
) -> dict[str, Any]:
    """Discover exactly one two-pass math v2 candidate per manifest item."""

    verify_manifest(manifest)
    runtime_root = runtime_root.expanduser().resolve()
    search_roots = (
        [v2_dir.expanduser().resolve()]
        if v2_dir is not None
        else [
            runtime_root / "shadow/historical/packages/objects",
            runtime_root / "shadow/packages/objects",
            runtime_root / "packages/objects",
            runtime_root / "packages/math",
        ]
    )
    paths: list[Path] = []
    for search_root in search_roots:
        if search_root.is_file() and search_root.suffix == ".json":
            paths.append(search_root)
        elif search_root.is_dir():
            paths.extend(sorted(search_root.rglob("*.json")))
    wanted = {item["capture_id"] for item in manifest.get("items", [])}
    by_capture: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    ignored_nonready: dict[str, list[str]] = defaultdict(list)
    for path in dict.fromkeys(paths):
        try:
            value = load_json(path)
        except AuditError:
            continue
        capture_id = value.get("capture_id")
        if capture_id not in wanted:
            continue
        if value.get("subject") not in (None, "math"):
            continue
        if value.get("study_date") not in (None, manifest.get("study_date")):
            continue
        is_v2 = (
            value.get("schema_version") == "study-intake-preprocess-package-v2"
            or "pipeline_status" in value
        )
        if not is_v2:
            continue
        if value.get("pipeline_status") != "two_pass_ready":
            ignored_nonready[str(capture_id)].append(
                str(value.get("pipeline_status") or "missing_status")
            )
            continue
        by_capture[str(capture_id)].append((path, value))

    candidates: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    ambiguous: dict[str, list[str]] = {}
    invalid: dict[str, str] = {}
    for capture_id in sorted(wanted):
        values = by_capture.get(capture_id, [])
        if not values:
            missing.append(capture_id)
            continue
        if len(values) != 1:
            ambiguous[capture_id] = [str(path) for path, _ in values]
            continue
        path, package = values[0]
        package_sha = sha256_file(path)
        if path.name != f"{package_sha}.json":
            invalid[capture_id] = "v2_package_content_address_invalid"
            continue
        if package.get("formal_write_count") != 0:
            invalid[capture_id] = "v2_formal_write_nonzero"
            continue
        analysis = _candidate_analysis(package, runtime_root, path.parent)
        if not isinstance(analysis, dict):
            invalid[capture_id] = "v2_analysis_missing"
            continue
        item = _manifest_item_by_capture(manifest, capture_id)
        binding_problem = _validate_v2_binding(
            package,
            manifest,
            item,
            analysis,
            runtime_root,
        )
        if binding_problem:
            invalid[capture_id] = binding_problem
            continue
        stage_receipts = package.get("stage_receipts")
        statuses: list[str] = []
        identities: list[bool] = []
        if isinstance(stage_receipts, dict):
            for stage in ("analysis", "critical_review"):
                receipt = stage_receipts.get(stage)
                if isinstance(receipt, dict):
                    statuses.append(str(receipt.get("runtime_identity_status")))
                    identities.append(_runtime_identity_is_attested(receipt))
        runtime_attested = len(identities) == 2 and all(identities)
        candidates[capture_id] = {
            "capture_id": capture_id,
            "package_path": (
                relative_path(path, runtime_root, code="v2_package_path_escape")
                if path.is_relative_to(runtime_root)
                else str(path)
            ),
            "package_sha256": package_sha,
            "package_id": package.get("package_id"),
            "pipeline_status": package.get("pipeline_status"),
            "processing_contract_sha256": package.get("processing_contract_sha256"),
            "runtime_identity_statuses": statuses,
            "runtime_attested": runtime_attested,
            "analysis": analysis,
            "analysis_sha256": sha256_value(analysis),
            "formal_write_count": 0,
        }
    return {
        "status": (
            "complete"
            if not missing and not ambiguous and not invalid
            else "incomplete"
        ),
        "candidate_count": len(candidates),
        "expected_count": len(wanted),
        "missing_capture_ids": missing,
        "ambiguous": ambiguous,
        "invalid": invalid,
        "nonready_statuses": dict(sorted(ignored_nonready.items())),
        "candidates": candidates,
        "formal_write_count": 0,
    }


def require_complete_v2(discovery: Mapping[str, Any]) -> None:
    if discovery.get("status") != "complete":
        raise AuditError(
            "v2_candidate_set_incomplete",
            missing_capture_ids=discovery.get("missing_capture_ids"),
            ambiguous=discovery.get("ambiguous"),
            invalid=discovery.get("invalid"),
            nonready_statuses=discovery.get("nonready_statuses"),
        )


def _clean_candidate_value(value: Any) -> Any:
    hidden_keys = {
        "schema_version",
        "report_profile",
        "model",
        "requested_model",
        "requested_reasoning_effort",
        "runtime_model",
        "runtime_reasoning_effort",
        "runtime_identity_status",
        "prompt_version",
        "pipeline_status",
        "processing_contract_sha256",
        "processing_fingerprint",
        "package_id",
        "capture_id",
    }
    if isinstance(value, dict):
        return {
            key: _clean_candidate_value(nested)
            for key, nested in value.items()
            if key not in hidden_keys
        }
    if isinstance(value, list):
        return [_clean_candidate_value(item) for item in value]
    return value


MATH_FORMALIZATION_FIELDS = (
    "safe_summary",
    "question_body",
    "source_and_answer",
    "independent_performance",
    "wrong_point",
    "error_causes",
    "methods",
    "traps",
    "method_gap",
    "wrong_history",
    "mastery_evidence",
    "relationship_proposals",
)


def _normalized_formalization(analysis: Mapping[str, Any]) -> dict[str, Any]:
    value = analysis.get("formalization_candidates")
    if isinstance(value, dict):
        return {
            **{field: value.get(field, []) for field in MATH_FORMALIZATION_FIELDS},
            "other_candidates": {
                key: nested
                for key, nested in value.items()
                if key not in MATH_FORMALIZATION_FIELDS
            },
        }

    aliases = {
        "mastery_status": "mastery_evidence",
        "mastery_history": "mastery_evidence",
        "self_correction": "mastery_evidence",
        "related": "relationship_proposals",
        "relationship": "relationship_proposals",
        "body": "question_body",
        "answer": "source_and_answer",
    }
    grouped: dict[str, list[Any]] = {
        field: [] for field in MATH_FORMALIZATION_FIELDS
    }
    other: list[Any] = []
    updates = analysis.get("candidate_updates")
    if isinstance(updates, list):
        for update in updates:
            if not isinstance(update, dict):
                other.append(update)
                continue
            raw_field = update.get("field")
            field = aliases.get(raw_field, raw_field)
            if field in grouped:
                grouped[field].append(update)
            else:
                other.append(update)
    grouped["safe_summary"] = [analysis.get("summary")] if analysis.get("summary") else []
    grouped["independent_performance"] = list(
        analysis.get("independent_correct_steps") or []
    )
    return {**grouped, "other_candidates": other}


def common_candidate_view(analysis: Mapping[str, Any]) -> dict[str, Any]:
    diagnosis = analysis.get("reasoning_diagnosis")
    if not isinstance(diagnosis, dict):
        diagnosis = {
            "independent_correct_steps": analysis.get("independent_correct_steps", []),
            "first_break": analysis.get("first_break"),
            "later_breaks": analysis.get("later_breaks", []),
            "hint_dependencies": analysis.get("hint_dependencies", []),
            "self_corrections": analysis.get("self_corrections", []),
        }
    verification = analysis.get("sol_verification_plan")
    if not isinstance(verification, (dict, list)):
        verification = analysis.get("nightly_checks", [])
    return _clean_candidate_value(
        {
            "summary": analysis.get("executive_summary", analysis.get("summary")),
            "question_structure": analysis.get("question_structure"),
            "correct_reasoning_reconstruction": analysis.get(
                "correct_reasoning_reconstruction"
            ),
            "evidence_assessment": analysis.get("evidence_assessment"),
            "reasoning_diagnosis": diagnosis,
            "concept_method_analysis": analysis.get("concept_method_analysis"),
            "formalization_candidates": _normalized_formalization(analysis),
            "risk_flags": analysis.get("risk_flags", []),
            "contradictions": analysis.get("contradictions", []),
            "unresolved": analysis.get("unresolved", []),
            "sol_verification_plan": verification,
        }
    )


def _claim_count(payload: Mapping[str, Any]) -> int:
    """Count displayed semantic claim objects; evaluators cannot choose totals."""

    count = 1 if isinstance(payload.get("summary"), str) and payload["summary"].strip() else 0

    def visit(value: Any, *, root_summary: bool = False) -> None:
        nonlocal count
        if isinstance(value, Mapping):
            keys = set(value)
            if keys == MATH_CLAIM_FIELDS:
                count += 1
                return
            if (
                isinstance(value.get("evidence_refs"), list)
                and value.get("evidence_refs")
                and any(
                    isinstance(value.get(key), str) and value.get(key).strip()
                    for key in ("text", "proposal", "value", "answer")
                )
            ):
                count += 1
                return
            for key, nested in value.items():
                if root_summary and key == "summary":
                    continue
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(payload, root_summary=True)
    if count <= 0:
        raise AuditError("blind_candidate_claims_missing")
    return count


def _effective_capture_evidence(item: Mapping[str, Any]) -> Mapping[str, Any]:
    replay = item.get("replay_input")
    if not isinstance(replay, Mapping):
        return {}
    amendments = replay.get("amendment_events")
    if isinstance(amendments, list) and amendments:
        last = amendments[-1]
        if isinstance(last, Mapping) and isinstance(last.get("evidence"), Mapping):
            return last["evidence"]
    capture = replay.get("capture_event")
    if isinstance(capture, Mapping) and isinstance(capture.get("evidence"), Mapping):
        return capture["evidence"]
    return {}


def _first_break_applicable(item: Mapping[str, Any]) -> bool:
    replay = item.get("replay_input")
    capture = replay.get("capture_event") if isinstance(replay, Mapping) else None
    evidence = _effective_capture_evidence(item)
    action = capture.get("requested_action") if isinstance(capture, Mapping) else None
    return bool(
        action in {"record_wrong", "record_recurrence"}
        or evidence.get("result") in {"wrong", "unstable", "unresolved"}
    )


def _hint_attribution_applicable(item: Mapping[str, Any]) -> bool:
    hints = _effective_capture_evidence(item).get("hints_needed")
    return isinstance(hints, list) and bool(hints)


def _blind_order(seed: str, capture_id: str) -> tuple[str, str]:
    _validate_blind_seed(seed)
    digest = hashlib.sha256(f"{seed}\0{capture_id}".encode("utf-8")).digest()
    return ("v1", "v2") if digest[0] % 2 == 0 else ("v2", "v1")


def blind_key(
    manifest: Mapping[str, Any], seed: str
) -> dict[str, dict[str, str]]:
    _validate_blind_seed(seed)
    result: dict[str, dict[str, str]] = {}
    for item in manifest.get("items", []):
        capture_id = item["capture_id"]
        case_id = "BLIND-" + hashlib.sha256(
            f"{manifest['manifest_sha256']}\0{capture_id}".encode("utf-8")
        ).hexdigest()[:20].upper()
        first, second = _blind_order(seed, capture_id)
        result[case_id] = {"A": first, "B": second, "capture_id": capture_id}
    return result


def build_blind_set(
    manifest: Mapping[str, Any], discovery: Mapping[str, Any], seed: str
) -> dict[str, Any]:
    verify_manifest(manifest)
    require_complete_v2(discovery)
    key = blind_key(manifest, seed)
    items = {item["capture_id"]: item for item in manifest.get("items", [])}
    cases: list[dict[str, Any]] = []
    for case_id in sorted(key):
        mapping = key[case_id]
        capture_id = mapping["capture_id"]
        item = items[capture_id]
        payloads = {
            "v1": common_candidate_view(item["v1"]["analysis"]),
            "v2": common_candidate_view(
                discovery["candidates"][capture_id]["analysis"]
            ),
        }
        candidates = {
            label: {
                "payload": payloads[version],
                "anonymous_payload_sha256": sha256_value(payloads[version]),
                "claim_count": _claim_count(payloads[version]),
            }
            for label, version in (("A", mapping["A"]), ("B", mapping["B"]))
        }
        evidence = {
            "target_group_key": item["target_group_key"],
            "capture_event": item["replay_input"]["capture_event"],
            "amendment_events": item["replay_input"]["amendment_events"],
            "source_bundle": item["replay_input"]["source_bundle"],
            "historical_formal_target": item["replay_input"]["formal_target"],
            "replay_limitations": item["replay_limitations"],
        }
        cases.append(
            {
                "case_id": case_id,
                "evidence": evidence,
                "visual_required": item["visual_required"],
                "first_break_required": _first_break_applicable(item),
                "hint_attribution_required": _hint_attribution_applicable(item),
                "candidates": candidates,
            }
        )
    payload: dict[str, Any] = {
        "schema_version": BLIND_SCHEMA,
        "manifest_sha256": manifest["manifest_sha256"],
        "study_date": manifest["study_date"],
        "randomization_commitment_sha256": hashlib.sha256(
            seed.encode("utf-8")
        ).hexdigest(),
        "case_count": len(cases),
        "cases": cases,
        "formal_write_count": 0,
    }
    payload["blind_set_sha256"] = sha256_value(payload)
    return payload


def verify_blind_set(blind_set: Mapping[str, Any]) -> None:
    if blind_set.get("schema_version") != BLIND_SCHEMA:
        raise AuditError("blind_set_schema_invalid")
    payload = dict(blind_set)
    expected = payload.pop("blind_set_sha256", None)
    require_equal(sha256_value(payload), expected, "blind_set_hash_mismatch")
    if blind_set.get("formal_write_count") != 0:
        raise AuditError("blind_set_formal_write_nonzero")


def judgment_template(blind_set: Mapping[str, Any]) -> dict[str, Any]:
    verify_blind_set(blind_set)
    cases: list[dict[str, Any]] = []
    for case in blind_set.get("cases", []):
        def candidate_template(label: str) -> dict[str, Any]:
            return {
                "claim_total": case["candidates"][label]["claim_count"],
                "supported_claims": None,
                "p0_findings": [],
                "first_break": (
                    None if case.get("first_break_required") else "not_applicable"
                ),
                "hint_attribution": (
                    None
                    if case.get("hint_attribution_required")
                    else "not_applicable"
                ),
                "visual_grounding": (
                    None if case.get("visual_required") else "not_applicable"
                ),
                "adoption_outcome": None,
                "review_duration_ms": None,
                "notes": None,
            }
        cases.append(
            {
                "case_id": case["case_id"],
                "preference": None,
                "preference_reason": None,
                "candidates": {
                    "A": candidate_template("A"),
                    "B": candidate_template("B"),
                },
            }
        )
    return {
        "schema_version": JUDGMENT_SCHEMA,
        "blind_set_sha256": blind_set["blind_set_sha256"],
        "evaluator_id": None,
        "cases": cases,
        "formal_write_count": 0,
    }


def _validate_candidate_judgment(
    value: Any,
    *,
    case_id: str,
    label: str,
    visual_required: bool,
    first_break_required: bool,
    hint_attribution_required: bool,
    expected_claim_total: int,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AuditError("judgment_candidate_invalid", case_id=case_id, label=label)
    total = value.get("claim_total")
    supported = value.get("supported_claims")
    if not isinstance(total, int) or isinstance(total, bool) or total <= 0:
        raise AuditError("judgment_claim_total_invalid", case_id=case_id, label=label)
    if total != expected_claim_total:
        raise AuditError(
            "judgment_claim_total_mismatch",
            case_id=case_id,
            label=label,
            actual=total,
            expected=expected_claim_total,
        )
    if (
        not isinstance(supported, int)
        or isinstance(supported, bool)
        or supported < 0
        or supported > total
    ):
        raise AuditError(
            "judgment_supported_claims_invalid", case_id=case_id, label=label
        )
    findings = value.get("p0_findings")
    if not isinstance(findings, list) or not all(
        isinstance(item, (str, dict)) for item in findings
    ):
        raise AuditError("judgment_p0_findings_invalid", case_id=case_id, label=label)
    first_break = value.get("first_break")
    if first_break not in PASS_FAIL_NA:
        raise AuditError("judgment_first_break_invalid", case_id=case_id, label=label)
    if first_break_required and first_break == "not_applicable":
        raise AuditError(
            "judgment_first_break_required", case_id=case_id, label=label
        )
    if not first_break_required and first_break != "not_applicable":
        raise AuditError(
            "judgment_first_break_not_applicable", case_id=case_id, label=label
        )
    hint = value.get("hint_attribution")
    if hint not in PASS_FAIL_NA:
        raise AuditError("judgment_hint_attribution_invalid", case_id=case_id, label=label)
    if hint_attribution_required and hint == "not_applicable":
        raise AuditError(
            "judgment_hint_attribution_required", case_id=case_id, label=label
        )
    if not hint_attribution_required and hint != "not_applicable":
        raise AuditError(
            "judgment_hint_attribution_not_applicable", case_id=case_id, label=label
        )
    visual = value.get("visual_grounding")
    if visual not in PASS_FAIL_NA:
        raise AuditError("judgment_visual_grounding_invalid", case_id=case_id, label=label)
    if visual_required and visual == "not_applicable":
        raise AuditError(
            "judgment_visual_grounding_required", case_id=case_id, label=label
        )
    if not visual_required and visual != "not_applicable":
        raise AuditError(
            "judgment_visual_grounding_not_applicable", case_id=case_id, label=label
        )
    outcome = value.get("adoption_outcome")
    if outcome not in ADOPTION_OUTCOMES:
        raise AuditError("judgment_adoption_outcome_invalid", case_id=case_id, label=label)
    duration = value.get("review_duration_ms")
    if duration is not None and (
        not isinstance(duration, int) or isinstance(duration, bool) or duration < 0
    ):
        raise AuditError("judgment_review_duration_invalid", case_id=case_id, label=label)
    return value


def validate_judgments(
    blind_set: Mapping[str, Any], judgments: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    verify_blind_set(blind_set)
    if judgments.get("schema_version") != JUDGMENT_SCHEMA:
        raise AuditError("judgment_schema_invalid")
    require_equal(
        judgments.get("blind_set_sha256"),
        blind_set.get("blind_set_sha256"),
        "judgment_blind_set_mismatch",
    )
    if judgments.get("formal_write_count") != 0:
        raise AuditError("judgment_formal_write_nonzero")
    if not isinstance(judgments.get("evaluator_id"), str) or not judgments.get(
        "evaluator_id"
    ).strip():
        raise AuditError("judgment_evaluator_invalid")
    expected_cases = {case["case_id"]: case for case in blind_set.get("cases", [])}
    raw_cases = judgments.get("cases")
    if not isinstance(raw_cases, list):
        raise AuditError("judgment_cases_invalid")
    values: dict[str, dict[str, Any]] = {}
    for value in raw_cases:
        if not isinstance(value, dict) or not isinstance(value.get("case_id"), str):
            raise AuditError("judgment_case_invalid")
        case_id = value["case_id"]
        if case_id in values:
            raise AuditError("judgment_case_duplicate", case_id=case_id)
        if case_id not in expected_cases:
            raise AuditError("judgment_case_unknown", case_id=case_id)
        preference = value.get("preference")
        if preference not in {"A", "B", "tie", "unscorable"}:
            raise AuditError("judgment_preference_invalid", case_id=case_id)
        candidates = value.get("candidates")
        if not isinstance(candidates, dict) or set(candidates) != {"A", "B"}:
            raise AuditError("judgment_candidate_labels_invalid", case_id=case_id)
        blind_case = expected_cases[case_id]
        for label in ("A", "B"):
            _validate_candidate_judgment(
                candidates[label],
                case_id=case_id,
                label=label,
                visual_required=bool(blind_case.get("visual_required")),
                first_break_required=bool(blind_case.get("first_break_required")),
                hint_attribution_required=bool(
                    blind_case.get("hint_attribution_required")
                ),
                expected_claim_total=int(
                    blind_case["candidates"][label]["claim_count"]
                ),
            )
        values[case_id] = value
    require_equal(
        set(values), set(expected_cases), "judgment_case_set_mismatch"
    )
    return values


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 6)


def _version_metrics(values: Sequence[dict[str, Any]]) -> dict[str, Any]:
    claim_total = sum(value["claim_total"] for value in values)
    supported = sum(value["supported_claims"] for value in values)
    p0_findings = [
        finding
        for value in values
        for finding in value.get("p0_findings", [])
    ]
    first_applicable = [
        value["first_break"]
        for value in values
        if value["first_break"] != "not_applicable"
    ]
    visual_applicable = [
        value["visual_grounding"]
        for value in values
        if value["visual_grounding"] != "not_applicable"
    ]
    hint_values = [
        value["hint_attribution"]
        for value in values
        if value["hint_attribution"] != "not_applicable"
    ]
    outcomes = Counter(value["adoption_outcome"] for value in values)
    durations = [
        value["review_duration_ms"]
        for value in values
        if value.get("review_duration_ms") is not None
    ]
    direct = outcomes["direct"]
    minor = outcomes["minor_edit"]
    negative = outcomes["major_edit"] + outcomes["rejected"] + outcomes["fallback"]
    return {
        "case_count": len(values),
        "p0_count": len(p0_findings),
        "p0_findings": p0_findings,
        "evidence": {
            "supported_claims": supported,
            "claim_total": claim_total,
            "precision": _ratio(supported, claim_total),
        },
        "first_break": {
            "pass": first_applicable.count("pass"),
            "applicable": len(first_applicable),
            "accuracy": _ratio(first_applicable.count("pass"), len(first_applicable)),
        },
        "hint_attribution": {
            "pass": hint_values.count("pass"),
            "applicable": len(hint_values),
            "accuracy": _ratio(hint_values.count("pass"), len(hint_values)),
        },
        "visual_grounding": {
            "pass": visual_applicable.count("pass"),
            "applicable": len(visual_applicable),
            "coverage": _ratio(
                visual_applicable.count("pass"), len(visual_applicable)
            ),
        },
        "adoption": {
            "counts": {name: outcomes[name] for name in sorted(ADOPTION_OUTCOMES)},
            "direct_rate": _ratio(direct, len(values)),
            "direct_or_minor_rate": _ratio(direct + minor, len(values)),
            "major_reject_or_fallback_rate": _ratio(negative, len(values)),
        },
        "review_duration_ms": {
            "count": len(durations),
            "median": statistics.median(durations) if durations else None,
        },
    }


def _gate_at_least(value: float | None, threshold: float) -> dict[str, Any]:
    if value is None:
        return {"status": "pending", "value": None, "threshold": threshold}
    return {
        "status": "pass" if value >= threshold else "fail",
        "value": value,
        "threshold": threshold,
    }


def _gate_at_most(value: float | None, threshold: float) -> dict[str, Any]:
    if value is None:
        return {"status": "pending", "value": None, "threshold": threshold}
    return {
        "status": "pass" if value <= threshold else "fail",
        "value": value,
        "threshold": threshold,
    }


def _applicable_accuracy_gate(
    metric: Mapping[str, Any], threshold: float
) -> dict[str, Any]:
    if metric.get("applicable") == 0:
        return {
            "status": "pass",
            "value": None,
            "threshold": threshold,
            "not_applicable": True,
        }
    return _gate_at_least(metric.get("accuracy"), threshold)


def score_blind_set(
    manifest: Mapping[str, Any],
    discovery: Mapping[str, Any],
    blind_set: Mapping[str, Any],
    judgments: Mapping[str, Any],
    seed: str,
) -> dict[str, Any]:
    require_complete_v2(discovery)
    checked = validate_judgments(blind_set, judgments)
    key = blind_key(manifest, seed)
    version_values: dict[str, list[dict[str, Any]]] = {"v1": [], "v2": []}
    preference_counts = Counter()
    for case_id, value in checked.items():
        mapping = key[case_id]
        for label in ("A", "B"):
            version_values[mapping[label]].append(value["candidates"][label])
        preference = value["preference"]
        if preference in {"A", "B"}:
            preference_counts[mapping[preference]] += 1
        else:
            preference_counts[preference] += 1
    metrics = {
        version: _version_metrics(values)
        for version, values in version_values.items()
    }
    scorable = (
        preference_counts["v1"]
        + preference_counts["v2"]
        + preference_counts["tie"]
    )
    blind_preference = {
        "v2_wins": preference_counts["v2"],
        "v1_wins": preference_counts["v1"],
        "ties": preference_counts["tie"],
        "unscorable": preference_counts["unscorable"],
        "scorable": scorable,
        "case_count": len(checked),
        "scorable_coverage": _ratio(scorable, len(checked)),
        "v2_win_rate": _ratio(preference_counts["v2"], scorable),
        "v2_loss_rate": _ratio(preference_counts["v1"], scorable),
    }
    v2 = metrics["v2"]
    gates: dict[str, dict[str, Any]] = {
        "p0_zero": {
            "status": "pass" if v2["p0_count"] == 0 else "fail",
            "value": v2["p0_count"],
            "threshold": 0,
        },
        "evidence_precision": _gate_at_least(
            v2["evidence"]["precision"], 0.95
        ),
        "first_break_accuracy": _applicable_accuracy_gate(
            v2["first_break"], 0.90
        ),
        "hint_attribution_accuracy": _applicable_accuracy_gate(
            v2["hint_attribution"], 0.98
        ),
        "visual_grounding_coverage": _gate_at_least(
            v2["visual_grounding"]["coverage"], 1.0
        ),
        "direct_rate": _gate_at_least(
            v2["adoption"]["direct_rate"], 0.50
        ),
        "direct_or_minor_rate": _gate_at_least(
            v2["adoption"]["direct_or_minor_rate"], 0.80
        ),
        "major_reject_or_fallback_rate": _gate_at_most(
            v2["adoption"]["major_reject_or_fallback_rate"], 0.20
        ),
        "blind_v2_win_rate": _gate_at_least(
            blind_preference["v2_win_rate"], 0.70
        ),
        "blind_v2_loss_rate": _gate_at_most(
            blind_preference["v2_loss_rate"], 0.10
        ),
        "scorable_coverage": _gate_at_least(
            blind_preference["scorable_coverage"], 1.0
        ),
    }
    v1_median = metrics["v1"]["review_duration_ms"]["median"]
    v2_median = metrics["v2"]["review_duration_ms"]["median"]
    review_ratio = (
        round(v2_median / v1_median, 6)
        if v1_median not in (None, 0) and v2_median is not None
        else None
    )
    gates["review_time_ratio"] = _gate_at_most(review_ratio, 1.10)
    gate_statuses = [gate["status"] for gate in gates.values()]
    quality_status = (
        "fail"
        if "fail" in gate_statuses
        else "pending"
        if "pending" in gate_statuses
        else "pass"
    )
    runtime_attested = all(
        value.get("runtime_attested")
        for value in discovery.get("candidates", {}).values()
    )
    replay_complete = all(
        isinstance(item, Mapping) and item.get("replay_status") == "complete"
        for item in manifest.get("items", [])
    )
    gates["historical_replay_complete"] = {
        "status": "pass" if replay_complete else "fail",
        "value": sum(
            isinstance(item, Mapping) and item.get("replay_status") == "complete"
            for item in manifest.get("items", [])
        ),
        "threshold": len(manifest.get("items", [])),
    }
    if not replay_complete:
        quality_status = "fail"
    promotion_status = (
        "eligible"
        if quality_status == "pass" and runtime_attested and replay_complete
        else "shadow_only"
    )
    payload: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA,
        "manifest_sha256": manifest["manifest_sha256"],
        "blind_set_sha256": blind_set["blind_set_sha256"],
        "judgments_sha256": sha256_value(judgments),
        "evaluator_id": judgments["evaluator_id"],
        "study_date": manifest["study_date"],
        "metrics": metrics,
        "blind_preference": blind_preference,
        "gates": gates,
        "quality_gate_status": quality_status,
        "runtime_attested": runtime_attested,
        "historical_replay_complete": replay_complete,
        "promotion_status": promotion_status,
        "baseline_actual_adoption_counts": manifest.get(
            "baseline_adoption_counts", {}
        ),
        "formal_write_count": 0,
    }
    payload["report_sha256"] = sha256_value(payload)
    return payload


def evaluation_root_for(runtime_root: Path, requested: Path | None) -> Path:
    allowed = (runtime_root / "state/evaluations").expanduser().resolve()
    target = (
        requested.expanduser().resolve()
        if requested is not None
        else allowed / "math-shadow-backaudit"
    )
    try:
        target.relative_to(allowed)
    except ValueError as exc:
        raise AuditError(
            "evaluation_output_outside_dedicated_root",
            requested=str(target),
            allowed=str(allowed),
        ) from exc
    return target


def blind_seed_path(runtime_root: Path, manifest: Mapping[str, Any]) -> Path:
    study_date = validate_study_date(manifest.get("study_date"))
    digest = manifest.get("manifest_sha256")
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        raise AuditError("manifest_hash_invalid")
    root = (runtime_root / "state/evaluation-secrets/math-shadow-backaudit").resolve()
    path = (root / study_date / digest / "blind-seed.txt").resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise AuditError("blind_seed_path_escape", path=str(path)) from exc
    return path


def _validate_blind_seed(seed: str) -> str:
    if not isinstance(seed, str) or len(seed.encode("utf-8")) < 32:
        raise AuditError("blind_seed_invalid")
    return seed


def read_blind_seed(path: Path) -> str:
    try:
        return _validate_blind_seed(path.expanduser().resolve().read_text(encoding="utf-8").strip())
    except OSError as exc:
        raise AuditError("blind_seed_unavailable", path=str(path)) from exc


def persist_blind_seed(path: Path, seed: str) -> str:
    seed = _validate_blind_seed(seed)
    data = (seed + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != data:
            raise AuditError("blind_seed_collision", path=str(path))
        return str(path)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return str(path)


def resolve_blind_seed(
    *,
    runtime_root: Path,
    manifest: Mapping[str, Any],
    command: str,
    explicit_seed: str | None,
    seed_file: Path | None,
    write: bool,
) -> tuple[str, Path | None]:
    if explicit_seed is not None and seed_file is not None:
        raise AuditError("blind_seed_sources_conflict")
    if explicit_seed is not None:
        return _validate_blind_seed(explicit_seed), None
    if seed_file is not None:
        return read_blind_seed(seed_file), None
    private_path = blind_seed_path(runtime_root.expanduser().resolve(), manifest)
    if command == "score":
        return read_blind_seed(private_path), private_path
    if command == "prepare-blind" and write:
        seed = secrets.token_hex(32)
        persist_blind_seed(private_path, seed)
        return seed, private_path
    raise AuditError(
        "blind_seed_required",
        hint="use --write for a private generated seed or provide --seed/--seed-file",
    )


def write_immutable_json(path: Path, value: object) -> str:
    data = canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != data:
            raise AuditError("evaluation_artifact_collision", path=str(path))
        return str(path)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    return str(path)


def persist_evaluation_artifacts(
    runtime_root: Path,
    evaluation_root: Path | None,
    manifest: Mapping[str, Any],
    artifacts: Iterable[tuple[str, Mapping[str, Any]]],
) -> list[dict[str, str]]:
    root = evaluation_root_for(runtime_root, evaluation_root)
    study_date = validate_study_date(manifest.get("study_date"))
    run_root = (
        root
        / study_date
        / str(manifest["manifest_sha256"])
    ).resolve()
    try:
        run_root.relative_to(root.resolve())
    except ValueError as exc:
        raise AuditError("evaluation_run_root_escape", path=str(run_root)) from exc
    written: list[dict[str, str]] = []
    for kind, value in artifacts:
        digest = sha256_value(value)
        path = run_root / f"{kind}-{digest}.json"
        write_immutable_json(path, value)
        written.append({"kind": kind, "path": str(path), "sha256": digest})
    return written


def _summary(manifest: Mapping[str, Any], discovery: Mapping[str, Any]) -> dict[str, Any]:
    items = manifest.get("items", [])
    return {
        "schema_version": "study-intake-math-shadow-inspection-v1",
        "study_date": manifest.get("study_date"),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "capture_count": len(items),
        "capture_ids": [item.get("capture_id") for item in items],
        "formal_ids": [item.get("formal_id") for item in items],
        "visual_case_count": sum(bool(item.get("visual_required")) for item in items),
        "replay_limited_count": sum(
            item.get("replay_status") != "complete" for item in items
        ),
        "baseline_adoption_counts": manifest.get("baseline_adoption_counts"),
        "v2": {
            key: discovery.get(key)
            for key in (
                "status",
                "candidate_count",
                "expected_count",
                "missing_capture_ids",
                "ambiguous",
                "invalid",
                "nonready_statuses",
            )
        },
        "formal_write_count": 0,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only historical math Luna shadow/backaudit evaluation"
    )
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT)
    parser.add_argument("--study-date", default=DEFAULT_STUDY_DATE)
    parser.add_argument("--expected-count", type=int, default=DEFAULT_EXPECTED_COUNT)
    parser.add_argument("--seed")
    parser.add_argument("--seed-file", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("inspect", "prepare-blind"):
        command = sub.add_parser(name)
        command.add_argument("--v2-dir", type=Path)
        command.add_argument("--write", action="store_true")
        command.add_argument("--evaluation-root", type=Path)
    score = sub.add_parser("score")
    score.add_argument("--v2-dir", type=Path)
    score.add_argument("--judgments", type=Path, required=True)
    score.add_argument("--write", action="store_true")
    score.add_argument("--evaluation-root", type=Path)
    return parser


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    os.umask(0o077)
    args = build_parser().parse_args(argv)
    try:
        if args.expected_count <= 0:
            raise AuditError("expected_count_invalid", value=args.expected_count)
        manifest = build_manifest(
            args.runtime_root,
            args.repo_root,
            study_date=args.study_date,
            expected_count=args.expected_count,
        )
        discovery = discover_v2(manifest, args.runtime_root, args.v2_dir)
        if args.command == "inspect":
            result: dict[str, Any] = {
                "summary": _summary(manifest, discovery),
                "manifest": manifest,
                "formal_write_count": 0,
            }
            artifacts: list[tuple[str, Mapping[str, Any]]] = [("manifest", manifest)]
        else:
            seed, private_seed_path = resolve_blind_seed(
                runtime_root=args.runtime_root,
                manifest=manifest,
                command=args.command,
                explicit_seed=args.seed,
                seed_file=args.seed_file,
                write=args.write,
            )
            blind_set = build_blind_set(manifest, discovery, seed)
            if args.command == "prepare-blind":
                template = judgment_template(blind_set)
                result = {
                    "summary": _summary(manifest, discovery),
                    "blind_set": blind_set,
                    "judgment_template": template,
                    "formal_write_count": 0,
                }
                artifacts = [
                    ("manifest", manifest),
                    ("blind-set", blind_set),
                    ("judgment-template", template),
                ]
            else:
                judgments = load_json(args.judgments.expanduser().resolve())
                report = score_blind_set(
                    manifest, discovery, blind_set, judgments, seed
                )
                result = {
                    "summary": _summary(manifest, discovery),
                    "report": report,
                    "formal_write_count": 0,
                }
                artifacts = [
                    ("manifest", manifest),
                    ("blind-set", blind_set),
                    ("score-report", report),
                ]
        if args.write:
            result["written_artifacts"] = persist_evaluation_artifacts(
                args.runtime_root,
                args.evaluation_root,
                manifest,
                artifacts,
            )
        elif args.evaluation_root is not None:
            raise AuditError("evaluation_root_requires_write")
        emit(result)
        return 0
    except AuditError as exc:
        emit(
            {
                "schema_version": "study-intake-math-shadow-error-v1",
                "status": "error",
                "error_code": exc.code,
                "details": exc.details,
                "formal_write_count": 0,
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
