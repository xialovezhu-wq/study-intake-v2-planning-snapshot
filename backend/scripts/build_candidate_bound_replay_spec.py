#!/usr/bin/env python3
"""Build an immutable controlled-replay spec for one successor candidate.

The helper is deliberately zero-model.  It derives the release binding from the
candidate config, rebinds immutable external captures, modernizes the math
solution-evidence preflight, and optionally binds fingerprints from a separately
captured zero-model inspection.  It never imports or starts a dispatcher.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest as validate_live_math_business_manifest,
)


SPEC_SCHEMA = "study-intake-controlled-replay-spec-v1"
EXTERNAL_SCHEMA = "study-intake-controlled-replay-math-capture-v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_MATH_SOURCE_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-math")
DEFAULT_MATH_BUSINESS_MANIFEST = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
    "luna-real-business-samples.json"
)
EXPECTED_DAILY_ROLES = {
    "math": {"GS-111", "GS-240", "complete-new-intake"},
    "cs408": {"DS_2023_002", "FILE_PROTECTION", "OS_2009_003", "FREE_SPACE"},
    "english": {"english-q37-a", "english-q37-c", "english-q37-d"},
}
EXPECTED_PREFLIGHT_ROLES = {
    "missing_question_image",
    "missing_solution_evidence",
    "solution_image_only",
    "solution_text_only",
    "missing_user_answer",
}
MATH_EVIDENCE_ERROR = "math_new_source_evidence_incomplete"
ENGLISH_LEGACY_ISSUE_ID = "EN-P0-006"
ENGLISH_LEGACY_TARGET_COUNT_SNAPSHOT = 98


class CandidateBoundSpecError(RuntimeError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    try:
        body = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CandidateBoundSpecError("candidate_spec_json_invalid") from exc
    return body + b"\n"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_object(
    path: Path,
    code: str,
    *,
    max_bytes: int = 4 * 1024 * 1024,
) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise OSError("unsafe input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CandidateBoundSpecError(code) from exc
    if not isinstance(value, dict):
        raise CandidateBoundSpecError(code)
    return value


def _publish_content_addressed(root: Path, value: Mapping[str, Any]) -> tuple[str, Path]:
    payload = _canonical_bytes(value)
    digest = hashlib.sha256(payload).hexdigest()
    path = root / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, raw_name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
    temporary = Path(raw_name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise CandidateBoundSpecError("candidate_spec_content_collision")
        os.chmod(path, 0o400)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return digest, path


def _candidate_release(candidate_config_path: Path) -> tuple[dict[str, Any], str]:
    config_path = candidate_config_path.resolve()
    config = _load_object(config_path, "candidate_config_invalid")
    release = config.get("release")
    manifest_value = (
        release.get("manifest_path") if isinstance(release, Mapping) else None
    )
    if not isinstance(manifest_value, str):
        raise CandidateBoundSpecError("candidate_release_binding_invalid")
    manifest_path = Path(manifest_value).expanduser().resolve()
    manifest = _load_object(manifest_path, "candidate_release_binding_invalid")
    release_id = manifest.get("release_id")
    if (
        not isinstance(release_id, str)
        or SHA256_RE.fullmatch(release_id) is None
        or config_path.parent != manifest_path.parent
        or config_path.parent.name != release_id
    ):
        raise CandidateBoundSpecError("candidate_release_binding_invalid")
    return config, release_id


def _resolve_under(root: Path, raw_path: object) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
    resolved_root = root.resolve()
    path = (resolved_root / raw_path).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError as exc:
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid") from exc
    if path.is_symlink() or not path.is_file():
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
    return path


def _source_evidence_roles(
    capture: Mapping[str, Any], *, math_source_root: Path
) -> set[str]:
    source = capture.get("source_bundle")
    if not isinstance(source, Mapping):
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
    manifest_path = _resolve_under(math_source_root, source.get("manifest_path"))
    manifest_sha256 = source.get("manifest_hash")
    if (
        not isinstance(manifest_sha256, str)
        or SHA256_RE.fullmatch(manifest_sha256) is None
        or _sha256_file(manifest_path) != manifest_sha256
    ):
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
    manifest = _load_object(
        manifest_path,
        "candidate_spec_math_source_invalid",
        max_bytes=1024 * 1024,
    )
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
    roles: set[str] = set()
    for row in artifacts:
        if not isinstance(row, Mapping):
            raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
        role = row.get("role")
        path = _resolve_under(math_source_root, row.get("path"))
        digest = row.get("sha256")
        if (
            not isinstance(role, str)
            or not isinstance(digest, str)
            or SHA256_RE.fullmatch(digest) is None
            or _sha256_file(path) != digest
        ):
            raise CandidateBoundSpecError("candidate_spec_math_source_invalid")
        if role == "solution_text":
            try:
                if not path.read_text(encoding="utf-8").strip():
                    raise CandidateBoundSpecError(
                        "candidate_spec_math_source_invalid"
                    )
            except UnicodeError as exc:
                raise CandidateBoundSpecError(
                    "candidate_spec_math_source_invalid"
                ) from exc
        roles.add(role)
    return roles


def _rebind_external_capture(
    row: dict[str, Any],
    *,
    release_id: str,
    external_output_root: Path,
) -> dict[str, Any] | None:
    path_value = row.get("external_capture_manifest_path")
    sha256_value = row.get("external_capture_manifest_sha256")
    if path_value is None and sha256_value is None:
        return None
    if (
        not isinstance(path_value, str)
        or not isinstance(sha256_value, str)
        or SHA256_RE.fullmatch(sha256_value) is None
    ):
        raise CandidateBoundSpecError("candidate_spec_external_binding_invalid")
    path = Path(path_value).expanduser()
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or _sha256_file(path) != sha256_value
    ):
        raise CandidateBoundSpecError("candidate_spec_external_binding_invalid")
    value = _load_object(path, "candidate_spec_external_binding_invalid")
    capture = value.get("capture")
    if (
        value.get("schema_version") != EXTERNAL_SCHEMA
        or value.get("formal_write_count") != 0
        or not isinstance(capture, Mapping)
        or capture.get("formal_id") is not None
        or capture.get("capture_schema_version") != "math-fast-intake-capture-v2"
    ):
        raise CandidateBoundSpecError("candidate_spec_external_binding_invalid")
    rebound = copy.deepcopy(value)
    rebound["release_id"] = release_id
    digest, rebound_path = _publish_content_addressed(
        external_output_root,
        rebound,
    )
    row["external_capture_manifest_path"] = str(rebound_path)
    row["external_capture_manifest_sha256"] = digest
    return rebound


def _validate_daily_entries(entries: object) -> list[dict[str, Any]]:
    if not isinstance(entries, list) or not entries:
        raise CandidateBoundSpecError("candidate_spec_entries_invalid")
    roles = {subject: set() for subject in EXPECTED_DAILY_ROLES}
    seen: set[tuple[str, str]] = set()
    checked: list[dict[str, Any]] = []
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise CandidateBoundSpecError("candidate_spec_entries_invalid")
        row = copy.deepcopy(dict(raw))
        subject = row.get("subject")
        capture_id = row.get("capture_id")
        role = row.get("sample_role")
        if (
            subject not in EXPECTED_DAILY_ROLES
            or not isinstance(capture_id, str)
            or not capture_id
            or not isinstance(role, str)
            or (str(subject), capture_id) in seen
            or ENGLISH_LEGACY_ISSUE_ID.lower() in role.lower()
            or ENGLISH_LEGACY_ISSUE_ID.lower() in capture_id.lower()
        ):
            raise CandidateBoundSpecError("candidate_spec_entries_invalid")
        seen.add((str(subject), capture_id))
        roles[str(subject)].add(role)
        checked.append(row)
    if roles != EXPECTED_DAILY_ROLES:
        raise CandidateBoundSpecError("candidate_spec_daily_role_set_mismatch")
    return checked


def _subject_spec(spec: Mapping[str, Any], subject: str) -> dict[str, Any]:
    selected = copy.deepcopy(dict(spec))
    selected["entries"] = [
        copy.deepcopy(dict(row))
        for row in spec["entries"]
        if row.get("subject") == subject
    ]
    if subject != "math":
        selected["preflight_cases"] = []
    selected["subject_scope"] = subject
    return selected


def _select_text_only_business_case(
    manifest_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        result = validate_live_math_business_manifest(manifest_path)
    except MathLiveBusinessFixtureError as exc:
        raise CandidateBoundSpecError(
            "candidate_spec_math_business_manifest_invalid"
        ) from exc
    tasks = result.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 3:
        raise CandidateBoundSpecError("candidate_spec_math_business_manifest_invalid")
    matches = []
    for row in tasks:
        if not isinstance(row, Mapping):
            continue
        solution = row.get("solution_evidence")
        if (
            isinstance(solution, Mapping)
            and solution.get("accepted_policy")
            == "solution_text_or_solution_image"
            and solution.get("solution_text_present") is True
            and solution.get("solution_image_present") is False
            and row.get("luna_eligible") is True
        ):
            matches.append(dict(row))
    if not matches:
        raise CandidateBoundSpecError(
            "candidate_spec_text_only_business_case_missing"
        )
    chosen = sorted(matches, key=lambda row: str(row.get("capture_id") or ""))[0]
    return result, chosen


def _modernize_preflight(
    raw_cases: object,
    *,
    release_id: str,
    external_output_root: Path,
    math_source_root: Path,
    math_business_manifest_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(raw_cases, list):
        raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
    modern: dict[str, dict[str, Any]] = {}
    for raw in raw_cases:
        if not isinstance(raw, Mapping):
            raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
        row = copy.deepcopy(dict(raw))
        legacy_role = str(row.get("sample_role") or "")
        fixture_kind = row.get("fixture_kind") or "external_math_capture"
        if fixture_kind == "math_live_business_task":
            if legacy_role != "solution_text_only":
                raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
            continue
        if fixture_kind != "external_math_capture":
            raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
        rebound = _rebind_external_capture(
            row,
            release_id=release_id,
            external_output_root=external_output_root,
        )
        if rebound is None:
            raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
        capture = rebound.get("capture")
        if not isinstance(capture, Mapping):
            raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
        roles = _source_evidence_roles(capture, math_source_root=math_source_root)
        has_solution_image = "solution" in roles
        has_solution_text = "solution_text" in roles
        if legacy_role == "missing_question_image":
            role = "missing_question_image"
            outcome = "rejected"
        elif legacy_role in {"missing_solution_image", "missing_solution_evidence"}:
            if has_solution_text and not has_solution_image:
                role = "solution_text_only"
                outcome = "eligible"
            elif not has_solution_text and not has_solution_image:
                role = "missing_solution_evidence"
                outcome = "rejected"
            else:
                raise CandidateBoundSpecError(
                    "candidate_spec_solution_preflight_mislabeled"
                )
        elif legacy_role in {"missing_solution_text", "solution_image_only"}:
            if has_solution_image and not has_solution_text:
                role = "solution_image_only"
                outcome = "eligible"
            elif not has_solution_image and not has_solution_text:
                role = "missing_solution_evidence"
                outcome = "rejected"
            else:
                raise CandidateBoundSpecError(
                    "candidate_spec_solution_preflight_mislabeled"
                )
        elif legacy_role == "missing_user_answer":
            role = "missing_user_answer"
            outcome = "rejected"
        else:
            raise CandidateBoundSpecError("candidate_spec_preflight_invalid")
        row["sample_role"] = role
        row["legacy_sample_role"] = legacy_role
        row["fixture_kind"] = "external_math_capture"
        row["expected_outcome"] = outcome
        if outcome == "rejected":
            row["expected_error_code"] = MATH_EVIDENCE_ERROR
        else:
            row.pop("expected_error_code", None)
        if role in modern:
            raise CandidateBoundSpecError("candidate_spec_preflight_role_duplicate")
        modern[role] = row

    business, text_only = _select_text_only_business_case(
        math_business_manifest_path
    )
    if "solution_text_only" not in modern:
        modern["solution_text_only"] = {
            "sample_role": "solution_text_only",
            "fixture_kind": "math_live_business_task",
            "business_manifest_path": str(math_business_manifest_path.resolve()),
            "business_manifest_sha256": _sha256_file(math_business_manifest_path),
            "capture_id": text_only["capture_id"],
            "expected_outcome": "eligible",
        }
    if set(modern) != EXPECTED_PREFLIGHT_ROLES:
        raise CandidateBoundSpecError("candidate_spec_preflight_role_set_mismatch")
    ordered_roles = (
        "missing_question_image",
        "solution_image_only",
        "solution_text_only",
        "missing_solution_evidence",
        "missing_user_answer",
    )
    return [modern[role] for role in ordered_roles], {
        "status": business.get("status"),
        "task_count": business.get("task_count"),
        "text_only_capture_id": text_only["capture_id"],
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def _bind_inspection_fingerprints(
    spec: dict[str, Any],
    inspection_path: Path,
    *,
    release_id: str,
) -> None:
    inspection = _load_object(inspection_path, "candidate_spec_inspection_invalid")
    if (
        inspection.get("status") != "ready"
        or inspection.get("release_id") != release_id
        or inspection.get("model_call_count") != 0
        or inspection.get("formal_write_count") != 0
    ):
        raise CandidateBoundSpecError("candidate_spec_inspection_invalid")
    decisions = inspection.get("decisions")
    if not isinstance(decisions, list):
        raise CandidateBoundSpecError("candidate_spec_inspection_invalid")
    by_capture: dict[str, str] = {}
    by_subject: dict[str, set[str]] = {}
    for raw in decisions:
        if not isinstance(raw, Mapping):
            continue
        capture_id = raw.get("capture_id")
        subject = raw.get("subject")
        fingerprint = raw.get("input_fingerprint")
        if (
            isinstance(capture_id, str)
            and isinstance(subject, str)
            and isinstance(fingerprint, str)
            and SHA256_RE.fullmatch(fingerprint) is not None
        ):
            by_capture[capture_id] = fingerprint
            by_subject.setdefault(subject, set()).add(fingerprint)
    for entry in spec["entries"]:
        capture_id = str(entry["capture_id"])
        fingerprint = by_capture.get(capture_id)
        if fingerprint is None:
            candidates = by_subject.get(str(entry["subject"]), set())
            if len(candidates) == 1:
                fingerprint = next(iter(candidates))
        if fingerprint is None:
            raise CandidateBoundSpecError(
                "candidate_spec_inspection_fingerprint_missing"
            )
        entry["expected_input_fingerprint"] = fingerprint


def build_candidate_bound_spec(
    *,
    candidate_config_path: Path,
    source_spec_path: Path,
    output_root: Path,
    math_business_manifest_path: Path = DEFAULT_MATH_BUSINESS_MANIFEST,
    math_source_root: Path = DEFAULT_MATH_SOURCE_ROOT,
    inspection_path: Path | None = None,
) -> dict[str, Any]:
    _config, release_id = _candidate_release(candidate_config_path)
    source = _load_object(source_spec_path, "candidate_spec_source_invalid")
    if source.get("schema_version") != SPEC_SCHEMA:
        raise CandidateBoundSpecError("candidate_spec_source_invalid")
    spec = copy.deepcopy(source)
    spec["release_id"] = release_id
    spec["formal_write_count"] = 0
    spec["entries"] = _validate_daily_entries(spec.get("entries"))
    external_root = output_root / "external-captures" / release_id
    for entry in spec["entries"]:
        _rebind_external_capture(
            entry,
            release_id=release_id,
            external_output_root=external_root,
        )
        entry.pop("expected_input_fingerprint", None)
    preflight, business_binding = _modernize_preflight(
        spec.get("preflight_cases"),
        release_id=release_id,
        external_output_root=external_root,
        math_source_root=math_source_root,
        math_business_manifest_path=math_business_manifest_path,
    )
    spec["preflight_cases"] = preflight
    spec["workload_boundary"] = {
        "classification": "current_daily_golden_workload",
        "distinct_business_task_count_by_subject": {
            "math": 6,
            "cs408": 4,
            "english": 1,
        },
        "distinct_business_task_count": 11,
        "excluded_historical_workloads": [
            {
                "issue_id": ENGLISH_LEGACY_ISSUE_ID,
                "classification": "historical_remediation",
                "target_count_snapshot": ENGLISH_LEGACY_TARGET_COUNT_SNAPSHOT,
                "included_in_daily_task_count": 0,
                "counts_toward_thirty_task_gate": False,
            }
        ],
        "thirty_business_task_gate": {
            "required_total": 30,
            "required_per_subject": 10,
            "status": "pending_insufficient_distinct_real_tasks",
        },
    }
    if inspection_path is not None:
        _bind_inspection_fingerprints(
            spec,
            inspection_path,
            release_id=release_id,
        )
    spec_root = output_root / "specs" / release_id
    digest, path = _publish_content_addressed(spec_root, spec)
    subject_specs: dict[str, dict[str, str]] = {}
    for subject in ("math", "cs408", "english"):
        subject_digest, subject_path = _publish_content_addressed(
            output_root / "subject-specs" / release_id / subject,
            _subject_spec(spec, subject),
        )
        subject_specs[subject] = {
            "path": str(subject_path),
            "sha256": subject_digest,
        }
    return {
        "schema_version": "study-intake-candidate-bound-replay-spec-build-v1",
        "status": "passed",
        "candidate_config_path": str(candidate_config_path.resolve()),
        "source_spec_path": str(source_spec_path.resolve()),
        "release_id": release_id,
        "spec_path": str(path),
        "spec_sha256": digest,
        "subject_specs": subject_specs,
        "entry_count": len(spec["entries"]),
        "preflight_count": len(preflight),
        "fingerprints_bound": inspection_path is not None,
        "daily_business_task_count_by_subject": {
            "math": 6,
            "cs408": 4,
            "english": 1,
        },
        "english_legacy_historical_target_count": (
            ENGLISH_LEGACY_TARGET_COUNT_SNAPSHOT
        ),
        "english_legacy_included_in_daily_task_count": 0,
        "thirty_business_task_gate": "pending_insufficient_distinct_real_tasks",
        "math_business_binding": business_binding,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(allow_abbrev=False)
    root.add_argument("--candidate-config", type=Path, required=True)
    root.add_argument("--source-spec", type=Path, required=True)
    root.add_argument("--output-root", type=Path, required=True)
    root.add_argument(
        "--math-business-manifest",
        type=Path,
        default=DEFAULT_MATH_BUSINESS_MANIFEST,
    )
    root.add_argument(
        "--math-source-root",
        type=Path,
        default=DEFAULT_MATH_SOURCE_ROOT,
    )
    root.add_argument("--inspection", type=Path)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = build_candidate_bound_spec(
            candidate_config_path=args.candidate_config,
            source_spec_path=args.source_spec,
            output_root=args.output_root,
            math_business_manifest_path=args.math_business_manifest,
            math_source_root=args.math_source_root,
            inspection_path=args.inspection,
        )
    except CandidateBoundSpecError as exc:
        print(
            json.dumps(
                {
                    "schema_version": (
                        "study-intake-candidate-bound-replay-spec-build-v1"
                    ),
                    "status": "failed",
                    "error_code": str(exc),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
