"""Strict zero-model validation for frozen deferred math fixtures.

The validator never writes below ``fixture_root``.  It treats every artifact
except an explicitly declared question image as private post-attempt evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping


MANIFEST_SCHEMA = "math-read-only-zero-model-test-manifest-v1"
USE_MODE = "read_only_zero_model_preflight"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
PRIVATE_ROLES = {
    "learning_record",
    "learning_record_and_structured_assistant_summary",
    "exact_user_dialogue_fragment",
}


class ReadOnlyMathFixtureError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path, code: str, *, max_bytes: int = 1024 * 1024) -> dict[str, Any]:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or node.st_size > max_bytes:
            raise OSError("unsafe input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReadOnlyMathFixtureError(code) from exc
    if not isinstance(value, dict):
        raise ReadOnlyMathFixtureError(code)
    return value


def _inside(root: Path, value: Path, code: str) -> Path:
    resolved = value.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ReadOnlyMathFixtureError(code) from exc
    return resolved


def _fingerprint(rows: list[tuple[str, str]]) -> str:
    # This is the byte format emitted by ``shasum -a 256`` from within the
    # sample directory, sorted by relative path.
    body = "".join(f"{digest}  ./{relative_path}\n" for relative_path, digest in rows)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _record_contract(path: Path, *, staging_id: str, formal_id: str) -> dict[str, Any]:
    value = _load_json(path, "math_fixture_record_invalid")
    identity = value.get("question_identity")
    if (
        value.get("schema_version") != "math-deferred-formal-intake-item-v1"
        or value.get("staging_id") != staging_id
        or value.get("state") != "deferred_only"
        or value.get("quick_intake_written") is not False
        or value.get("processing_authorized") is not False
        or value.get("formal_write_count") != 0
        or not isinstance(identity, Mapping)
        or identity.get("formal_id") != formal_id
        or value.get("source_locator") != f"formal_card:{formal_id}"
    ):
        raise ReadOnlyMathFixtureError("math_fixture_record_contract_invalid")
    sensitive = sorted(
        set(value)
        & {
            "answer_protection",
            "assistant_assessment",
            "hints_and_corrections",
            "learning_resolution",
            "resolved_answer",
            "user_answer_text",
        }
    )
    return {
        "private_answer_fields_present": bool(sensitive),
        "private_answer_field_count": len(sensitive),
    }


def validate_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest_path = manifest_path.expanduser()
    manifest = _load_json(manifest_path, "math_fixture_manifest_invalid")
    expected_keys = {
        "schema_version",
        "fixture_root",
        "frozen_at",
        "use_mode",
        "read_only_enforcement",
        "sample_count",
        "authorization",
        "scope",
        "samples",
        "separation",
    }
    if set(manifest) != expected_keys:
        raise ReadOnlyMathFixtureError("math_fixture_manifest_keys_invalid")
    root = Path(str(manifest.get("fixture_root") or "")).expanduser().resolve()
    try:
        manifest_path.resolve().relative_to(root)
    except ValueError as exc:
        raise ReadOnlyMathFixtureError("math_fixture_manifest_outside_root") from exc
    authorization = manifest.get("authorization")
    scope = manifest.get("scope")
    separation = manifest.get("separation")
    samples = manifest.get("samples")
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("use_mode") != USE_MODE
        or manifest.get("read_only_enforcement") != "logical_contract_only"
        or not isinstance(authorization, Mapping)
        or authorization.get("quick_intake_written") is not False
        or authorization.get("formal_write_count") != 0
        or authorization.get("processing_authorized") is not False
        or authorization.get("luna_authorized") is not False
        or authorization.get("sol_authorized") is not False
        or authorization.get("write_back_to_fixture") is not False
        or not isinstance(scope, Mapping)
        or scope.get("subject") != "kaoyan_math"
        or scope.get("cross_subject_files_present") is not False
        or scope.get("question_images_are_answer_free") is not True
        or scope.get("private_post_attempt_records_may_contain_results_or_resolved_answers")
        is not True
        or not isinstance(separation, Mapping)
        or separation.get("future_writes_to_fixture_prohibited") is not True
        or not isinstance(samples, list)
        or not samples
        or manifest.get("sample_count") != len(samples)
    ):
        raise ReadOnlyMathFixtureError("math_fixture_authorization_invalid")

    seen_ids: set[str] = set()
    seen_formal: set[str] = set()
    seen_fingerprints: set[str] = set()
    results: list[dict[str, Any]] = []
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise ReadOnlyMathFixtureError("math_fixture_sample_invalid")
        staging_id = sample.get("staging_id")
        identity = sample.get("question_identity")
        fingerprint = sample.get("content_fingerprint")
        files = sample.get("files")
        evidence = sample.get("evidence_presence")
        expected = sample.get("expected_preflight")
        if (
            not isinstance(staging_id, str)
            or SAFE_ID_RE.fullmatch(staging_id) is None
            or staging_id in seen_ids
            or sample.get("independent_capture") is not True
            or sample.get("duplicate_of") is not None
            or not isinstance(identity, Mapping)
            or not isinstance(fingerprint, Mapping)
            or fingerprint.get("algorithm")
            != "sha256_of_sorted_relative_path_and_file_sha256_lines"
            or not isinstance(files, list)
            or not files
            or not isinstance(evidence, Mapping)
            or not isinstance(expected, Mapping)
            or expected.get("full_capture_acceptance") != "fail_closed"
        ):
            raise ReadOnlyMathFixtureError("math_fixture_sample_invalid")
        formal_id = identity.get("formal_id")
        declared_fingerprint = fingerprint.get("value")
        if (
            not isinstance(formal_id, str)
            or SAFE_ID_RE.fullmatch(formal_id) is None
            or formal_id in seen_formal
            or identity.get("source_locator") != f"formal_card:{formal_id}"
            or not isinstance(declared_fingerprint, str)
            or SHA256_RE.fullmatch(declared_fingerprint) is None
            or declared_fingerprint in seen_fingerprints
        ):
            raise ReadOnlyMathFixtureError("math_fixture_identity_invalid")
        sample_dir = _inside(root, Path(str(sample.get("absolute_path") or "")), "math_fixture_path_invalid")
        if sample_dir.is_symlink() or not sample_dir.is_dir() or sample_dir.parent != root / "items":
            raise ReadOnlyMathFixtureError("math_fixture_path_invalid")
        seen_ids.add(staging_id)
        seen_formal.add(formal_id)
        seen_fingerprints.add(declared_fingerprint)

        rows: list[tuple[str, str]] = []
        roles: list[str] = []
        declared_paths: set[str] = set()
        record_path: Path | None = None
        for item in sorted(files, key=lambda row: str(row.get("relative_path")) if isinstance(row, Mapping) else ""):
            if not isinstance(item, Mapping) or set(item) != {"relative_path", "role", "sha256"}:
                raise ReadOnlyMathFixtureError("math_fixture_file_invalid")
            relative = item.get("relative_path")
            role = item.get("role")
            declared_sha = item.get("sha256")
            if (
                not isinstance(relative, str)
                or not relative
                or Path(relative).is_absolute()
                or ".." in Path(relative).parts
                or relative in declared_paths
                or role not in {"question_image", *PRIVATE_ROLES}
                or not isinstance(declared_sha, str)
                or SHA256_RE.fullmatch(declared_sha) is None
            ):
                raise ReadOnlyMathFixtureError("math_fixture_file_invalid")
            path = _inside(sample_dir, sample_dir / relative, "math_fixture_file_path_invalid")
            try:
                node = path.lstat()
            except OSError as exc:
                raise ReadOnlyMathFixtureError("math_fixture_file_missing") from exc
            if path.is_symlink() or not path.is_file() or node.st_size <= 0:
                raise ReadOnlyMathFixtureError("math_fixture_file_invalid")
            if _sha256(path) != declared_sha:
                raise ReadOnlyMathFixtureError("math_fixture_file_hash_mismatch")
            if role == "question_image":
                if path.suffix.lower() != ".png" or path.read_bytes()[:8] != b"\x89PNG\r\n\x1a\n":
                    raise ReadOnlyMathFixtureError("math_fixture_question_image_invalid")
            elif path.name == "record.json":
                record_path = path
            else:
                try:
                    if not path.read_text(encoding="utf-8").strip():
                        raise UnicodeError("empty")
                except UnicodeError as exc:
                    raise ReadOnlyMathFixtureError("math_fixture_dialogue_invalid") from exc
            declared_paths.add(relative)
            roles.append(str(role))
            rows.append((relative, declared_sha))
        actual_paths = {
            str(path.relative_to(sample_dir))
            for path in sample_dir.rglob("*")
            if path.is_file()
        }
        if actual_paths != declared_paths:
            raise ReadOnlyMathFixtureError("math_fixture_file_set_mismatch")
        computed = _fingerprint(sorted(rows))
        if computed != declared_fingerprint:
            raise ReadOnlyMathFixtureError("math_fixture_fingerprint_mismatch")
        if record_path is None:
            raise ReadOnlyMathFixtureError("math_fixture_record_missing")
        record = _record_contract(record_path, staging_id=staging_id, formal_id=formal_id)

        missing: list[str] = []
        if "question_image" not in roles:
            missing.append("question_image")
        if evidence.get("solution_image") is not True or "solution_image" not in roles:
            missing.append("solution_image")
        if evidence.get("user_work_image") is not True:
            missing.append("user_work_image")
        if evidence.get("exact_assistant_transcript") is not True:
            missing.append("exact_assistant_transcript")
        if evidence.get("real_dialogue") in {None, "minimal_user_confirmation_only"}:
            missing.append("full_dialogue")
        if not missing:
            raise ReadOnlyMathFixtureError("math_fixture_expected_failure_not_observed")
        private_files = sorted(
            relative for (relative, _), role in zip(sorted(rows), roles) if role != "question_image"
        )
        results.append(
            {
                "staging_id": staging_id,
                "formal_id": formal_id,
                "content_fingerprint": computed,
                "distinct_capture": True,
                "learner_facing_artifact_count": roles.count("question_image"),
                "private_artifact_count": len(private_files),
                "private_answer_fields_present": record["private_answer_fields_present"],
                "private_answer_leakage_guard": "passed",
                "missing_evidence": sorted(set(missing)),
                "preflight_status": "failed_closed",
                "error_code": "math_deferred_capture_evidence_incomplete",
                "luna_eligible": False,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )

    return {
        "schema_version": "study-intake-read-only-math-fixture-validation-v1",
        "status": "passed_expected_fail_closed",
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": _sha256(manifest_path),
        "sample_count": len(results),
        "distinct_capture_count": len(seen_fingerprints),
        "samples": results,
        "fixture_write_count": 0,
        "mcp_tool_call_count": 0,
        "model_call_count": 0,
        "formal_write_count": 0,
    }
