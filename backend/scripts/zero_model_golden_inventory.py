#!/usr/bin/env python3
"""Validate the frozen three-subject Golden inventory without calling a model.

This module deliberately stops at fixture and contract readiness.  It never
imports a model runner, dispatcher, writer, or subject adapter and therefore
cannot turn a static fixture check into a semantic Golden result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest as validate_live_math_business_manifest,
)
from read_only_math_fixture import (  # noqa: E402
    ReadOnlyMathFixtureError,
    validate_manifest as validate_read_only_math_manifest,
)


SPEC_SCHEMA = "study-intake-controlled-replay-spec-v1"
INVENTORY_SCHEMA = "study-intake-zero-model-golden-inventory-v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_MATH_BUSINESS_MANIFEST = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
    "luna-real-business-samples.json"
)
DEFAULT_MATH_SUPERSEDED_MANIFESTS = (
    Path(
        "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
        "luna-business-task-manifest.json"
    ),
    Path(
        "/Users/xiazhibin/Documents/kaoyan-math-live-capture/2026-08-09/"
        "luna-business-task-addendum-001.json"
    ),
)
DEFAULT_MATH_NEGATIVE_MANIFEST = Path(
    "/Users/xiazhibin/Documents/kaoyan-math-deferred-intake/2026-08-09/"
    "read-only-test-manifest.json"
)
TRUSTED_MATH_SOURCE_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-math")
MATH_GOLDEN_ASSERTION_VALIDATOR = ROOT / "lib" / "math_live_golden_assertions.py"
MATH_GOLDEN_ASSERTION_SCHEMA = (
    ROOT / "schemas" / "math-live-business-golden-assertion-result-v1.json"
)
MATH_LIVE_TASK_ASSERTIONS = {
    "independent_correct_no_false_wrong_card": [
        "preserve_independent_correct_observation",
        "unobserved_reasoning_remains_unknown",
        "no_wrong_item_proposal_from_missing_steps",
        "no_mastery_inference_beyond_observed_episode",
        "formal_write_count_zero",
    ],
    "wrong_then_corrected_weakness_proposal": [
        "first_break_precedes_later_distinct_breaks",
        "independent_correct_steps_preserved",
        "post_explanation_understanding_labeled_separately",
        "weakness_distillation_proposal_only",
        "no_independent_mastery_upgrade",
        "formal_write_count_zero",
    ],
    "wrong_then_corrected_multistage_method_proposal": [
        "formal_id_remains_null",
        "no_gs_la_or_pr_identifier_invented",
        "new_wrong_item_and_knowledge_candidates_are_proposal_only",
        "first_break_precedes_later_distinct_breaks",
        "independent_correct_steps_preserved",
        "post_explanation_understanding_labeled_separately",
        "no_independent_mastery_upgrade",
        "formal_write_count_zero",
    ],
}
MATH_LIVE_EXPECTED_TASKS = {
    "LUNA-MATH-20260809-001": {
        "business_task_id": "MATH-LUNA-BIZ-20260809-001",
        "formal_id": "GS-109",
        "source_route": "existing_formal_card_review",
        "source_locator": "formal_card:GS-109",
        "task_kind": "independent_correct_no_false_wrong_card",
    },
    "LUNA-MATH-20260809-002": {
        "business_task_id": "MATH-LUNA-BIZ-20260809-002",
        "formal_id": "GS-507",
        "source_route": "existing_formal_card_review",
        "source_locator": "formal_card:GS-507",
        "task_kind": "wrong_then_corrected_weakness_proposal",
    },
    "LUNA-MATH-20260809-003": {
        "business_task_id": "MATH-LUNA-BIZ-20260809-003",
        "formal_id": None,
        "source_route": "new_source_learning_episode",
        "source_locator": "question-bank-id:170710",
        "task_kind": "wrong_then_corrected_multistage_method_proposal",
    },
}

EXPECTED_ROLES = {
    "math": {"GS-111", "GS-240", "complete-new-intake"},
    "cs408": {"DS_2023_002", "FILE_PROTECTION", "OS_2009_003", "FREE_SPACE"},
    "english": {"english-q37-a", "english-q37-c", "english-q37-d"},
}
EXPECTED_PREFLIGHT_CONTRACT = {
    "missing_question_image": {
        "fixture_kind": "external_math_capture",
        "expected_outcome": "rejected",
    },
    "missing_solution_evidence": {
        "fixture_kind": "external_math_capture",
        "expected_outcome": "rejected",
    },
    "solution_image_only": {
        "fixture_kind": "external_math_capture",
        "expected_outcome": "eligible",
    },
    "solution_text_only": {
        "fixture_kind": "math_live_business_task",
        "expected_outcome": "eligible",
    },
    "missing_user_answer": {
        "fixture_kind": "external_math_capture",
        "expected_outcome": "rejected",
    },
}
MATH_EVIDENCE_ERROR = "math_new_source_evidence_incomplete"
ENGLISH_LEGACY_HISTORICAL_ISSUE_ID = "EN-P0-006"
ENGLISH_LEGACY_HISTORICAL_TARGET_COUNT_SNAPSHOT = 98


class GoldenInventoryError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path, *, max_bytes: int = 4 * 1024 * 1024) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise OSError("unsafe input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GoldenInventoryError("golden_input_invalid") from exc
    if not isinstance(value, dict):
        raise GoldenInventoryError("golden_input_invalid")
    return value


def _verify_content_addressed_fixture(
    raw_path: object,
    raw_sha256: object,
    *,
    release_id: str,
) -> dict[str, Any]:
    if (
        not isinstance(raw_path, str)
        or not isinstance(raw_sha256, str)
        or SHA256_RE.fullmatch(raw_sha256) is None
    ):
        raise GoldenInventoryError("golden_fixture_binding_invalid")
    path = Path(raw_path).expanduser()
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o222
        or path.name != f"{raw_sha256}.json"
        or _sha256_file(path) != raw_sha256
    ):
        raise GoldenInventoryError("golden_fixture_binding_invalid")
    value = _load_json(path)
    if (
        value.get("schema_version")
        != "study-intake-controlled-replay-math-capture-v1"
        or value.get("release_id") != release_id
        or value.get("formal_write_count") != 0
        or not isinstance(value.get("capture"), Mapping)
        or value["capture"].get("formal_id") is not None
    ):
        raise GoldenInventoryError("golden_fixture_binding_invalid")
    return {
        "path": str(path.resolve()),
        "sha256": raw_sha256,
        "capture_id": value["capture"].get("event_id"),
        "capture": value["capture"],
    }


def _resolve_math_source_path(raw_path: object) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise GoldenInventoryError("golden_fixture_source_invalid")
    root = TRUSTED_MATH_SOURCE_ROOT.resolve()
    path = (root / raw_path).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise GoldenInventoryError("golden_fixture_source_invalid") from exc
    if path.is_symlink() or not path.is_file():
        raise GoldenInventoryError("golden_fixture_source_invalid")
    return path


def _external_fixture_evidence_presence(
    fixture: Mapping[str, Any],
) -> dict[str, bool]:
    capture = fixture.get("capture")
    if not isinstance(capture, Mapping):
        raise GoldenInventoryError("golden_fixture_source_invalid")
    source = capture.get("source_bundle")
    episode = capture.get("episode_evidence")
    if not isinstance(source, Mapping) or not isinstance(episode, Mapping):
        raise GoldenInventoryError("golden_fixture_source_invalid")
    manifest_path = _resolve_math_source_path(source.get("manifest_path"))
    manifest_sha256 = source.get("manifest_hash")
    if (
        not isinstance(manifest_sha256, str)
        or SHA256_RE.fullmatch(manifest_sha256) is None
        or _sha256_file(manifest_path) != manifest_sha256
    ):
        raise GoldenInventoryError("golden_fixture_source_invalid")
    manifest = _load_json(manifest_path, max_bytes=1024 * 1024)
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise GoldenInventoryError("golden_fixture_source_invalid")
    roles: set[str] = set()
    for row in artifacts:
        if not isinstance(row, Mapping):
            raise GoldenInventoryError("golden_fixture_source_invalid")
        role = row.get("role")
        artifact_path = _resolve_math_source_path(row.get("path"))
        artifact_sha256 = row.get("sha256")
        if (
            not isinstance(role, str)
            or not isinstance(artifact_sha256, str)
            or SHA256_RE.fullmatch(artifact_sha256) is None
            or _sha256_file(artifact_path) != artifact_sha256
        ):
            raise GoldenInventoryError("golden_fixture_source_invalid")
        if role == "solution_text":
            try:
                if not artifact_path.read_text(encoding="utf-8").strip():
                    raise GoldenInventoryError("golden_fixture_source_invalid")
            except UnicodeError as exc:
                raise GoldenInventoryError("golden_fixture_source_invalid") from exc
        roles.add(role)
    user_answer = episode.get("user_answer_text")
    return {
        "question_image_present": "question" in roles,
        "solution_image_present": "solution" in roles,
        "solution_text_present": "solution_text" in roles,
        "user_answer_present": isinstance(user_answer, str)
        and bool(user_answer.strip()),
    }


def _expected_external_evidence_shape(
    role: str,
    evidence: Mapping[str, bool],
) -> bool:
    question = evidence.get("question_image_present") is True
    solution_image = evidence.get("solution_image_present") is True
    solution_text = evidence.get("solution_text_present") is True
    user_answer = evidence.get("user_answer_present") is True
    if role == "missing_question_image":
        return not question and (solution_image or solution_text) and user_answer
    if role == "missing_solution_evidence":
        return question and not solution_image and not solution_text and user_answer
    if role == "solution_image_only":
        return question and solution_image and not solution_text and user_answer
    if role == "missing_user_answer":
        return question and (solution_image or solution_text) and not user_answer
    return False


def _live_math_business_preflight(manifest_path: Path) -> dict[str, Any]:
    try:
        result = validate_live_math_business_manifest(
            manifest_path,
            trusted_source_root=TRUSTED_MATH_SOURCE_ROOT,
        )
    except MathLiveBusinessFixtureError as exc:
        raise GoldenInventoryError("math_live_business_fixture_invalid") from exc
    if (
        result.get("status") != "passed_real_luna_business_preflight"
        or result.get("authority_schema_version")
        != "math-luna-real-business-samples-v1"
        or result.get("fixture_write_count") != 0
        or result.get("model_call_count") != 0
        or result.get("formal_write_count") != 0
        or result.get("task_count") != result.get("distinct_capture_count")
    ):
        raise GoldenInventoryError("math_live_business_fixture_invalid")
    tasks = result.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise GoldenInventoryError("math_live_business_fixture_invalid")
    return result


def _superseded_math_manifest_preflight(
    manifest_paths: Sequence[Path],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for manifest_path in manifest_paths:
        try:
            validate_live_math_business_manifest(
                manifest_path,
                trusted_source_root=TRUSTED_MATH_SOURCE_ROOT,
            )
        except MathLiveBusinessFixtureError as exc:
            if str(exc) != "math_live_manifest_superseded":
                raise GoldenInventoryError(
                    "math_superseded_manifest_rejection_invalid"
                ) from exc
        else:
            raise GoldenInventoryError("math_superseded_manifest_was_consumed")
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise GoldenInventoryError(
                "math_superseded_manifest_rejection_invalid"
            )
        rows.append(
            {
                "manifest_path": str(manifest_path.resolve()),
                "manifest_sha256": _sha256_file(manifest_path),
                "expected_error_code": "math_live_manifest_superseded",
                "status": "passed_expected_reject",
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
    if not rows:
        raise GoldenInventoryError("math_superseded_manifest_rejection_invalid")
    return {
        "status": "passed_expected_reject",
        "manifest_count": len(rows),
        "manifests": rows,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def _negative_math_preflight(manifest_path: Path) -> dict[str, Any]:
    try:
        result = validate_read_only_math_manifest(manifest_path)
    except ReadOnlyMathFixtureError as exc:
        raise GoldenInventoryError("math_negative_fixture_invalid") from exc
    if (
        result.get("status") != "passed_expected_fail_closed"
        or result.get("fixture_write_count") != 0
        or result.get("model_call_count") != 0
        or result.get("formal_write_count") != 0
    ):
        raise GoldenInventoryError("math_negative_fixture_invalid")
    samples = result.get("samples")
    if (
        not isinstance(samples, list)
        or not samples
        or any(
            not isinstance(row, Mapping)
            or row.get("preflight_status") != "failed_closed"
            or row.get("luna_eligible") is not False
            or row.get("model_call_count") != 0
            or row.get("formal_write_count") != 0
            for row in samples
        )
    ):
        raise GoldenInventoryError("math_negative_fixture_invalid")
    return result


def inspect_spec(
    spec_path: Path,
    *,
    math_business_manifest_path: Path = DEFAULT_MATH_BUSINESS_MANIFEST,
    math_superseded_manifest_paths: Sequence[Path] = (
        DEFAULT_MATH_SUPERSEDED_MANIFESTS
    ),
    math_negative_manifest_path: Path = DEFAULT_MATH_NEGATIVE_MANIFEST,
) -> dict[str, Any]:
    spec = _load_json(spec_path)
    release_id = spec.get("release_id")
    entries = spec.get("entries")
    preflight = spec.get("preflight_cases")
    if (
        spec.get("schema_version") != SPEC_SCHEMA
        or not isinstance(release_id, str)
        or SHA256_RE.fullmatch(release_id) is None
        or spec.get("formal_write_count") != 0
        or not isinstance(entries, list)
        or not isinstance(preflight, list)
    ):
        raise GoldenInventoryError("golden_spec_invalid")

    roles_by_subject: dict[str, set[str]] = {
        subject: set() for subject in EXPECTED_ROLES
    }
    capture_ids: set[str] = set()
    fingerprints: dict[str, set[str]] = {
        subject: set() for subject in EXPECTED_ROLES
    }
    external_fixtures: list[dict[str, Any]] = []
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise GoldenInventoryError("golden_spec_entry_invalid")
        subject = raw.get("subject")
        capture_id = raw.get("capture_id")
        role = raw.get("sample_role")
        fingerprint = raw.get("expected_input_fingerprint")
        if (
            subject not in EXPECTED_ROLES
            or not isinstance(capture_id, str)
            or not capture_id
            or capture_id in capture_ids
            or not isinstance(role, str)
            or not isinstance(fingerprint, str)
            or SHA256_RE.fullmatch(fingerprint) is None
        ):
            raise GoldenInventoryError("golden_spec_entry_invalid")
        capture_ids.add(capture_id)
        roles_by_subject[str(subject)].add(role)
        fingerprints[str(subject)].add(fingerprint)
        if raw.get("external_capture_manifest_path") is not None:
            fixture = _verify_content_addressed_fixture(
                raw.get("external_capture_manifest_path"),
                raw.get("external_capture_manifest_sha256"),
                release_id=release_id,
            )
            external_fixtures.append(
                {
                    "path": fixture["path"],
                    "sha256": fixture["sha256"],
                    "capture_id": fixture["capture_id"],
                }
            )

    if roles_by_subject != EXPECTED_ROLES:
        raise GoldenInventoryError("golden_role_set_mismatch")
    if len(fingerprints["english"]) != 1:
        raise GoldenInventoryError("english_microbatch_binding_invalid")

    live_math = _live_math_business_preflight(math_business_manifest_path)
    superseded_math = _superseded_math_manifest_preflight(
        math_superseded_manifest_paths
    )
    live_math_tasks = live_math["tasks"]
    live_capture_ids: set[str] = set()
    live_fingerprints: set[str] = set()
    live_business_ids: set[str] = set()
    for raw in live_math_tasks:
        if not isinstance(raw, Mapping):
            raise GoldenInventoryError("math_live_business_fixture_invalid")
        capture_id = raw.get("capture_id")
        business_task_id = raw.get("business_task_id")
        fingerprint = raw.get("content_fingerprint")
        task_kind = raw.get("task_kind")
        expected_task = MATH_LIVE_EXPECTED_TASKS.get(str(capture_id or ""))
        if (
            not isinstance(capture_id, str)
            or not capture_id
            or capture_id in capture_ids
            or capture_id in live_capture_ids
            or not isinstance(business_task_id, str)
            or not business_task_id
            or business_task_id in live_business_ids
            or not isinstance(fingerprint, str)
            or SHA256_RE.fullmatch(fingerprint) is None
            or fingerprint in fingerprints["math"]
            or fingerprint in live_fingerprints
            or task_kind not in MATH_LIVE_TASK_ASSERTIONS
            or expected_task is None
            or any(raw.get(key) != value for key, value in expected_task.items())
            or raw.get("luna_eligible") is not True
            or raw.get("proposal_only") is not True
        ):
            raise GoldenInventoryError("math_live_business_task_not_distinct")
        live_capture_ids.add(capture_id)
        live_business_ids.add(business_task_id)
        live_fingerprints.add(fingerprint)
    if live_capture_ids != set(MATH_LIVE_EXPECTED_TASKS):
        raise GoldenInventoryError("math_live_business_task_set_mismatch")

    negative_math = _negative_math_preflight(math_negative_manifest_path)
    for contract_path in (
        MATH_GOLDEN_ASSERTION_VALIDATOR,
        MATH_GOLDEN_ASSERTION_SCHEMA,
    ):
        if contract_path.is_symlink() or not contract_path.is_file():
            raise GoldenInventoryError("math_golden_assertion_contract_invalid")

    preflight_rows: list[dict[str, Any]] = []
    preflight_roles: set[str] = set()
    for raw in preflight:
        if not isinstance(raw, Mapping):
            raise GoldenInventoryError("golden_preflight_invalid")
        role = raw.get("sample_role")
        expected = EXPECTED_PREFLIGHT_CONTRACT.get(str(role or ""))
        fixture_kind = raw.get("fixture_kind") or "external_math_capture"
        expected_outcome = raw.get("expected_outcome")
        expected_error = raw.get("expected_error_code")
        if expected_outcome is None and expected_error is not None:
            expected_outcome = "rejected"
        if (
            not isinstance(role, str)
            or role in preflight_roles
            or expected is None
            or fixture_kind != expected["fixture_kind"]
            or expected_outcome != expected["expected_outcome"]
            or (
                expected_outcome == "rejected"
                and expected_error != MATH_EVIDENCE_ERROR
            )
            or (expected_outcome == "eligible" and expected_error is not None)
        ):
            raise GoldenInventoryError("golden_preflight_invalid")
        preflight_roles.add(role)
        if fixture_kind == "external_math_capture":
            fixture = _verify_content_addressed_fixture(
                raw.get("external_capture_manifest_path"),
                raw.get("external_capture_manifest_sha256"),
                release_id=release_id,
            )
            evidence = _external_fixture_evidence_presence(fixture)
            if not _expected_external_evidence_shape(role, evidence):
                raise GoldenInventoryError("golden_preflight_evidence_shape_invalid")
            preflight_rows.append(
                {
                    "sample_role": role,
                    "fixture_kind": fixture_kind,
                    "capture_id": fixture["capture_id"],
                    "fixture_sha256": fixture["sha256"],
                    "expected_outcome": expected_outcome,
                    "expected_error_code": expected_error,
                    "solution_evidence": {
                        "accepted_policy": "solution_text_or_solution_image",
                        "solution_text_present": evidence[
                            "solution_text_present"
                        ],
                        "solution_image_present": evidence[
                            "solution_image_present"
                        ],
                    },
                    "static_fixture_status": "bound_not_executed",
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            continue
        business_manifest_path = raw.get("business_manifest_path")
        business_manifest_sha256 = raw.get("business_manifest_sha256")
        capture_id = raw.get("capture_id")
        if (
            not isinstance(business_manifest_path, str)
            or Path(business_manifest_path).resolve()
            != math_business_manifest_path.resolve()
            or not isinstance(business_manifest_sha256, str)
            or business_manifest_sha256 != _sha256_file(math_business_manifest_path)
            or not isinstance(capture_id, str)
        ):
            raise GoldenInventoryError("golden_preflight_invalid")
        matches = [
            row
            for row in live_math_tasks
            if isinstance(row, Mapping) and row.get("capture_id") == capture_id
        ]
        if len(matches) != 1:
            raise GoldenInventoryError("golden_preflight_evidence_shape_invalid")
        solution = matches[0].get("solution_evidence")
        if (
            not isinstance(solution, Mapping)
            or solution.get("accepted_policy")
            != "solution_text_or_solution_image"
            or solution.get("solution_text_present") is not True
            or solution.get("solution_image_present") is not False
            or matches[0].get("luna_eligible") is not True
        ):
            raise GoldenInventoryError("golden_preflight_evidence_shape_invalid")
        preflight_rows.append(
            {
                "sample_role": role,
                "fixture_kind": fixture_kind,
                "capture_id": capture_id,
                "fixture_sha256": business_manifest_sha256,
                "expected_outcome": "eligible",
                "expected_error_code": None,
                "solution_evidence": {
                    "accepted_policy": "solution_text_or_solution_image",
                    "solution_text_present": True,
                    "solution_image_present": False,
                },
                "static_fixture_status": "bound_not_executed",
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
    if preflight_roles != set(EXPECTED_PREFLIGHT_CONTRACT):
        raise GoldenInventoryError("golden_preflight_role_set_mismatch")

    replay_counts = Counter(str(row.get("subject")) for row in entries)
    raw_counts = Counter(replay_counts)
    raw_counts["math"] += len(live_math_tasks)
    business_counts = {
        "math": replay_counts["math"] + len(live_math_tasks),
        "cs408": replay_counts["cs408"],
        "english": len(fingerprints["english"]),
    }
    business_total = sum(business_counts.values())
    thirty_status = (
        "ready"
        if all(business_counts[subject] >= 10 for subject in EXPECTED_ROLES)
        else "pending_insufficient_distinct_real_tasks"
    )
    return {
        "schema_version": INVENTORY_SCHEMA,
        "status": "static_fixtures_ready_model_not_run",
        "replay_spec_path": str(spec_path.resolve()),
        "replay_spec_sha256": _sha256_file(spec_path),
        "release_id": release_id,
        "raw_capture_count": len(entries) + len(live_math_tasks),
        "raw_capture_count_by_subject": dict(sorted(raw_counts.items())),
        "replay_spec_capture_count": len(entries),
        "live_math_business_preflight": {
            "status": live_math["status"],
            "batch_id": live_math["batch_id"],
            "batch_manifest_path": live_math["batch_manifest_path"],
            "batch_manifest_sha256": live_math["batch_manifest_sha256"],
            "fixture_tree_sha256": live_math["fixture_tree_sha256"],
            "task_count": live_math["task_count"],
            "distinct_capture_count": live_math["distinct_capture_count"],
            "solution_evidence_policy": live_math["solution_evidence_policy"],
            "tasks": [
                {
                    "capture_id": row["capture_id"],
                    "business_task_id": row["business_task_id"],
                    "formal_id": row["formal_id"],
                    "source_route": row["source_route"],
                    "source_locator": row.get("source_locator"),
                    "task_kind": row["task_kind"],
                    "expected_semantic_assertions": MATH_LIVE_TASK_ASSERTIONS[
                        row["task_kind"]
                    ],
                    "content_fingerprint": row["content_fingerprint"],
                    "manifest_sha256": row["manifest_sha256"],
                    "solution_evidence": {
                        "accepted_policy": row["solution_evidence"][
                            "accepted_policy"
                        ],
                        "satisfied_by": row["solution_evidence"]["satisfied_by"],
                        "solution_text_present": row["solution_evidence"][
                            "solution_text_present"
                        ],
                        "solution_image_present": row["solution_evidence"][
                            "solution_image_present"
                        ],
                        "source_sha256": row["solution_evidence"][
                            "source_sha256"
                        ],
                        "source_verified": row["solution_evidence"][
                            "source_verified"
                        ],
                        "source_path_exposed_to_model": False,
                    },
                    "reasoning_boundary": row["reasoning_boundary"],
                    "luna_eligible": True,
                    "proposal_only": True,
                    "semantic_assertion_status": "pending_model_replay_after_p0_gate",
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
                for row in live_math_tasks
            ],
            "fixture_write_count": 0,
            "semantic_assertion_status": "pending_model_replay_after_p0_gate",
            "model_call_count": 0,
            "formal_write_count": 0,
        },
        "superseded_math_entrypoint_preflight": superseded_math,
        "math_live_golden_assertion_contract": {
            "status": "ready_not_executed",
            "validator_path": str(MATH_GOLDEN_ASSERTION_VALIDATOR.resolve()),
            "validator_sha256": _sha256_file(MATH_GOLDEN_ASSERTION_VALIDATOR),
            "result_schema_path": str(MATH_GOLDEN_ASSERTION_SCHEMA.resolve()),
            "result_schema_sha256": _sha256_file(MATH_GOLDEN_ASSERTION_SCHEMA),
            "expected_package_schema": "study-intake-preprocess-package-v2",
            "expected_report_schema": "study-intake-luna-math-candidate-v4",
            "task_count": len(live_math_tasks),
            "semantic_assertion_status": "pending_model_replay_after_p0_gate",
            "model_call_count": 0,
            "formal_write_count": 0,
        },
        "read_only_math_negative_preflight": {
            "status": negative_math["status"],
            "manifest_path": negative_math["manifest_path"],
            "manifest_sha256": negative_math["manifest_sha256"],
            "sample_count": negative_math["sample_count"],
            "distinct_capture_count": negative_math["distinct_capture_count"],
            "samples": negative_math["samples"],
            "fixture_write_count": 0,
            "model_call_count": 0,
            "formal_write_count": 0,
        },
        "distinct_business_task_count": business_total,
        "distinct_business_task_count_by_subject": business_counts,
        "daily_business_task_boundary": {
            "classification": "current_daily_golden_workload",
            "distinct_task_count": business_total,
            "distinct_task_count_by_subject": business_counts,
            "excluded_historical_workloads": [
                {
                    "issue_id": ENGLISH_LEGACY_HISTORICAL_ISSUE_ID,
                    "classification": "historical_remediation",
                    "target_count_snapshot": (
                        ENGLISH_LEGACY_HISTORICAL_TARGET_COUNT_SNAPSHOT
                    ),
                    "included_in_daily_task_count": 0,
                    "counts_toward_thirty_task_gate": False,
                }
            ],
        },
        "golden_roles": {
            subject: sorted(roles) for subject, roles in roles_by_subject.items()
        },
        "external_complete_new_intake_fixtures": external_fixtures,
        "controlled_replay_preflight": preflight_rows,
        "missing_evidence_preflight": [
            row
            for row in preflight_rows
            if row["expected_outcome"] == "rejected"
        ],
        "solution_evidence_preflight": {
            "accepted_policy": "solution_text_or_solution_image",
            "positive_cases": [
                row
                for row in preflight_rows
                if row["sample_role"]
                in {"solution_image_only", "solution_text_only"}
            ],
            "negative_case": next(
                row
                for row in preflight_rows
                if row["sample_role"] == "missing_solution_evidence"
            ),
            "inline_capture_text_substitutes_for_artifact": False,
        },
        "thirty_business_task_gate": {
            "required_total": 30,
            "required_per_subject": 10,
            "observed_distinct_total": business_total,
            "observed_distinct_by_subject": business_counts,
            "status": thirty_status,
            "synthetic_task_substitution_allowed": False,
        },
        "semantic_assertion_status": "pending_model_replay_after_p0_gate",
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    root.add_argument("--spec", type=Path, required=True)
    root.add_argument(
        "--math-business-manifest",
        type=Path,
        default=DEFAULT_MATH_BUSINESS_MANIFEST,
    )
    root.add_argument(
        "--math-negative-manifest",
        type=Path,
        default=DEFAULT_MATH_NEGATIVE_MANIFEST,
    )
    root.add_argument(
        "--math-superseded-manifest",
        type=Path,
        action="append",
        default=None,
    )
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = inspect_spec(
            args.spec,
            math_business_manifest_path=args.math_business_manifest,
            math_superseded_manifest_paths=(
                tuple(args.math_superseded_manifest)
                if args.math_superseded_manifest
                else DEFAULT_MATH_SUPERSEDED_MANIFESTS
            ),
            math_negative_manifest_path=args.math_negative_manifest,
        )
    except GoldenInventoryError as exc:
        print(
            json.dumps(
                {
                    "schema_version": INVENTORY_SCHEMA,
                    "status": "failed",
                    "error_code": str(exc),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
