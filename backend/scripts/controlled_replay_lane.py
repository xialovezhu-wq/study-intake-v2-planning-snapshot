#!/usr/bin/env python3
"""Exact allowlist one-shot replay without opening normal subject claims."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "scripts"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    _publish_content_addressed,
)
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateSubprocessRunner,
    scan_eligible_candidates,
    validate_fixed_model_contract,
)
from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest as validate_live_math_business_manifest,
)
from preprocess_dispatcher import _stage_timeout  # noqa: E402
from preprocessor_core import (  # noqa: E402
    Candidate,
    PreprocessorError,
    load_config,
    make_adapters,
)
from pre_model_p0_gate import (  # noqa: E402
    P0GateError,
    require_model_lane_ready,
)


SPEC_SCHEMA = "study-intake-controlled-replay-spec-v1"
REPORT_SCHEMA = "study-intake-controlled-replay-report-v1"
SUBJECTS = ("english", "cs408", "math")
EXTERNAL_MATH_CAPTURE_SCHEMA = (
    "study-intake-controlled-replay-math-capture-v1"
)
PREFLIGHT_OUTCOMES = frozenset({"eligible", "rejected"})
MATH_LIVE_BUSINESS_FIXTURE_KIND = "math_live_business_task"
EXTERNAL_MATH_CAPTURE_FIXTURE_KIND = "external_math_capture"


def _load_object(path: Path, code: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or path.stat().st_size > 4 * 1024 * 1024:
            raise OSError("invalid input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DispatchError(code) from exc
    if not isinstance(value, dict):
        raise DispatchError(code)
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _external_capture(
    path_value: object,
    sha256_value: object,
    *,
    expected_release_id: str,
) -> dict[str, Any]:
    if not isinstance(path_value, str) or not isinstance(sha256_value, str):
        raise DispatchError("controlled_replay_external_capture_invalid")
    path = Path(path_value).expanduser()
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o222
        or len(sha256_value) != 64
        or path.name != f"{sha256_value}.json"
        or _sha256_file(path) != sha256_value
    ):
        raise DispatchError("controlled_replay_external_capture_invalid")
    value = _load_object(path.resolve(), "controlled_replay_external_capture_invalid")
    capture = value.get("capture")
    if (
        value.get("schema_version") != EXTERNAL_MATH_CAPTURE_SCHEMA
        or value.get("release_id") != expected_release_id
        or value.get("formal_write_count") != 0
        or not isinstance(capture, dict)
        or capture.get("formal_id") is not None
        or capture.get("capture_schema_version")
        != "math-fast-intake-capture-v2"
    ):
        raise DispatchError("controlled_replay_external_capture_invalid")
    return copy.deepcopy(capture)


def _external_math_candidate(
    config: Mapping[str, Any],
    capture: Mapping[str, Any],
) -> Candidate:
    scan_config = copy.deepcopy(dict(config))
    adapters = scan_config.get("adapters")
    if not isinstance(adapters, dict) or not isinstance(adapters.get("math"), dict):
        raise DispatchError("controlled_replay_math_config_invalid")
    adapters["math"]["enabled"] = True
    adapter = make_adapters(scan_config).get("math")
    if adapter is None:
        raise DispatchError("controlled_replay_math_config_invalid")
    rows = adapter.candidates(
        {
            "schema_version": "math-fast-intake-status-v1",
            "study_date": capture.get("study_date"),
            "pending": [copy.deepcopy(dict(capture))],
        }
    )
    if len(rows) != 1 or rows[0].capture_id != capture.get("event_id"):
        raise DispatchError("controlled_replay_external_capture_invalid")
    return adapter.deep_candidate(rows[0])


def _release_id(config: Mapping[str, Any]) -> str:
    path = Path(str(config.get("release", {}).get("manifest_path") or ""))
    manifest = _load_object(path, "controlled_replay_release_manifest_invalid")
    value = manifest.get("release_id")
    if not isinstance(value, str) or len(value) != 64:
        raise DispatchError("controlled_replay_release_id_invalid")
    return value


def _spec_entries(
    spec: Mapping[str, Any], *, expected_release_id: str
) -> list[dict[str, Any]]:
    raw = spec.get("entries")
    if (
        spec.get("schema_version") != SPEC_SCHEMA
        or spec.get("release_id") != expected_release_id
        or not isinstance(raw, list)
        or not raw
        or len(raw) > 16
        or spec.get("formal_write_count") != 0
    ):
        raise DispatchError("controlled_replay_spec_invalid")
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise DispatchError("controlled_replay_spec_invalid")
        subject = item.get("subject")
        capture_id = item.get("capture_id")
        fingerprint = item.get("expected_input_fingerprint")
        key = (str(subject), str(capture_id))
        if (
            subject not in SUBJECTS
            or not isinstance(capture_id, str)
            or not capture_id
            or key in seen
            or (
                fingerprint is not None
                and (
                    not isinstance(fingerprint, str)
                    or len(fingerprint) != 64
                    or any(char not in "0123456789abcdef" for char in fingerprint)
                )
            )
        ):
            raise DispatchError("controlled_replay_spec_invalid")
        seen.add(key)
        rows.append(
            {
                "sequence": index + 1,
                "subject": subject,
                "capture_id": capture_id,
                "expected_input_fingerprint": fingerprint,
                "sample_role": str(item.get("sample_role") or capture_id),
                "external_capture_manifest_path": item.get(
                    "external_capture_manifest_path"
                ),
                "external_capture_manifest_sha256": item.get(
                    "external_capture_manifest_sha256"
                ),
                "fixture_kind": item.get("fixture_kind"),
                "business_manifest_path": item.get("business_manifest_path"),
                "business_manifest_sha256": item.get(
                    "business_manifest_sha256"
                ),
                "frozen_task_path": item.get("frozen_task_path"),
                "frozen_task_sha256": item.get("frozen_task_sha256"),
            }
        )
    return rows


def _candidate_bound_live_math_task(
    row: Mapping[str, Any], *, expected_release_id: str
) -> FrozenTask:
    """Reopen one already frozen live-math production task.

    The business manifest alone is evidence preparation, not executable work.
    This bridge accepts only a full candidate-bound FrozenTask and verifies its
    live-fixture content binding before it can enter the replay allowlist.
    """

    if row.get("fixture_kind") != MATH_LIVE_BUSINESS_FIXTURE_KIND:
        raise DispatchError("controlled_replay_live_math_task_invalid")
    task_path_value = row.get("frozen_task_path")
    task_sha256 = row.get("frozen_task_sha256")
    manifest_path_value = row.get("business_manifest_path")
    manifest_sha256 = row.get("business_manifest_sha256")
    if any(
        not isinstance(value, str) or not value
        for value in (
            task_path_value, task_sha256, manifest_path_value, manifest_sha256
        )
    ):
        raise DispatchError("controlled_replay_live_math_task_invalid")
    task_path = Path(str(task_path_value)).expanduser()
    manifest_path = Path(str(manifest_path_value)).expanduser()
    if (
        not task_path.is_absolute()
        or task_path.is_symlink()
        or not task_path.is_file()
        or _sha256_file(task_path) != task_sha256
        or not manifest_path.is_absolute()
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
        or _sha256_file(manifest_path) != manifest_sha256
    ):
        raise DispatchError("controlled_replay_live_math_task_invalid")
    raw = _load_object(task_path, "controlled_replay_live_math_task_invalid")
    try:
        task = FrozenTask.from_mapping(raw)
    except DispatchError as exc:
        raise DispatchError("controlled_replay_live_math_task_invalid") from exc
    if raw.get("unit_sha256") != task.unit_sha256:
        raise DispatchError("controlled_replay_live_math_task_invalid")
    payload = task.frozen_payload
    contract = payload.get("dispatch_contract")
    binding = payload.get("input_binding")
    if (
        payload.get("subject") != "math"
        or payload.get("capture_id") != row.get("capture_id")
        or not isinstance(contract, Mapping)
        or contract.get("release_id") != expected_release_id
        or not isinstance(binding, Mapping)
        or binding.get("math_live_business_manifest_sha256")
        != manifest_sha256
        or not isinstance(binding.get("math_live_content_fingerprint"), str)
    ):
        raise DispatchError("controlled_replay_live_math_task_invalid")
    try:
        business = validate_live_math_business_manifest(manifest_path)
    except MathLiveBusinessFixtureError as exc:
        raise DispatchError("controlled_replay_live_math_task_invalid") from exc
    matches = [
        item
        for item in business.get("tasks", [])
        if isinstance(item, Mapping)
        and item.get("capture_id") == row.get("capture_id")
    ]
    if (
        len(matches) != 1
        or matches[0].get("content_fingerprint")
        != binding.get("math_live_content_fingerprint")
        or matches[0].get("luna_eligible") is not True
        or matches[0].get("proposal_only") is not True
    ):
        raise DispatchError("controlled_replay_live_math_task_invalid")
    return task


def _scan_exact_tasks(
    config: Mapping[str, Any], entries: Sequence[Mapping[str, Any]]
) -> tuple[list[FrozenTask], list[dict[str, Any]]]:
    release_id: str | None = None
    requested_by_subject = {
        subject: frozenset(
            str(row["capture_id"])
            for row in entries
            if row["subject"] == subject
        )
        for subject in SUBJECTS
    }
    frozen_by_unit: dict[str, FrozenTask] = {}
    observations: list[dict[str, Any]] = []
    for subject in SUBJECTS:
        requested = requested_by_subject[subject]
        if not requested:
            continue
        subject_entries = [row for row in entries if row["subject"] == subject]
        external_entries = [
            row
            for row in subject_entries
            if row.get("external_capture_manifest_path") is not None
            or row.get("external_capture_manifest_sha256") is not None
        ]
        live_frozen_entries = [
            row
            for row in subject_entries
            if row.get("fixture_kind") == MATH_LIVE_BUSINESS_FIXTURE_KIND
            and row.get("frozen_task_path") is not None
        ]
        if external_entries and subject != "math":
            raise DispatchError("controlled_replay_external_capture_invalid")
        if live_frozen_entries and subject != "math":
            raise DispatchError("controlled_replay_live_math_task_invalid")
        external_ids = frozenset(
            str(row["capture_id"]) for row in external_entries
        )
        live_frozen_ids = frozenset(
            str(row["capture_id"]) for row in live_frozen_entries
        )
        if external_ids & live_frozen_ids:
            raise DispatchError("controlled_replay_live_math_task_invalid")
        normal_requested = requested - external_ids - live_frozen_ids
        scan_config = copy.deepcopy(dict(config))
        if subject == "math" and (normal_requested or external_entries):
            adapters = scan_config.get("adapters")
            if not isinstance(adapters, dict) or not isinstance(
                adapters.get("math"), dict
            ):
                raise DispatchError("controlled_replay_math_config_invalid")
            adapters["math"]["enabled"] = True
        scan_sets: list[tuple[frozenset[str], Sequence[Candidate] | None]] = []
        if normal_requested:
            scan_sets.append((normal_requested, None))
        if external_entries:
            if release_id is None:
                release_id = _release_id(config)
            external_candidates = [
                _external_math_candidate(
                    scan_config,
                    _external_capture(
                        row.get("external_capture_manifest_path"),
                        row.get("external_capture_manifest_sha256"),
                        expected_release_id=release_id,
                    ),
                )
                for row in external_entries
            ]
            scan_sets.append((external_ids, external_candidates))
        for row in live_frozen_entries:
            if release_id is None:
                release_id = _release_id(config)
            task = _candidate_bound_live_math_task(
                row, expected_release_id=release_id
            )
            if task.unit_sha256 in frozen_by_unit:
                raise DispatchError("controlled_replay_capture_ambiguous")
            frozen_by_unit[task.unit_sha256] = task
            observations.append(
                {
                    "subject": "math",
                    "capture_id": row["capture_id"],
                    "study_date": task.frozen_payload.get("study_date"),
                    "input_fingerprint": task.frozen_payload.get(
                        "input_fingerprint"
                    ),
                    "eligible": True,
                    "reason": "controlled_replay_live_math_frozen_task",
                    "unit_sha256": task.unit_sha256,
                    "frozen_payload_sha256": task.frozen_payload_sha256,
                    "release_id": release_id,
                    "phase": "frozen_evidence",
                    "model_enqueue_allowed": True,
                    "formal_write_count": 0,
                }
            )
        for exact_ids, overrides in scan_sets:
            frozen, decisions = scan_eligible_candidates(
                scan_config,
                subject,
                capture_allowlist=exact_ids,
                candidate_overrides=overrides,
                controlled_replay=True,
            )
            for decision in decisions:
                observations.append(copy.deepcopy(dict(decision)))
            for unit in frozen:
                payload = unit.task.frozen_payload
                members = set(_task_replay_capture_ids(unit.task))
                if not members or not members <= exact_ids:
                    raise DispatchError("controlled_replay_group_outside_allowlist")
                frozen_by_unit[unit.task.unit_sha256] = unit.task
    sequence = {
        (str(row["subject"]), str(row["capture_id"])): int(row["sequence"])
        for row in entries
    }
    found: dict[tuple[str, str], FrozenTask] = {}
    for task in frozen_by_unit.values():
        payload = task.frozen_payload
        subject = str(payload.get("subject") or "")
        members = _task_replay_capture_ids(task)
        for capture_id in members:
            key = (subject, str(capture_id))
            if key in found:
                raise DispatchError("controlled_replay_capture_ambiguous")
            found[key] = task
    expected = set(sequence)
    if set(found) != expected:
        raise DispatchError("controlled_replay_capture_not_eligible")
    for row in entries:
        expected_fingerprint = row.get("expected_input_fingerprint")
        task = found[(str(row["subject"]), str(row["capture_id"]))]
        payload = task.frozen_payload
        member_payloads = payload.get("content_group_members")
        matching = [
            member
            for member in member_payloads
            if isinstance(member, Mapping)
            and (
                member.get("capture_id") == row["capture_id"]
                or (
                    payload.get("subject") == "english"
                    and row["capture_id"]
                    in (member.get("input_binding", {}).get("capture_event_ids") or [])
                )
            )
        ] if isinstance(member_payloads, list) else [payload]
        if len(matching) != 1:
            raise DispatchError("controlled_replay_capture_binding_invalid")
        actual = matching[0].get("input_fingerprint")
        if expected_fingerprint is not None and actual != expected_fingerprint:
            raise DispatchError("controlled_replay_input_fingerprint_mismatch")
    ordered = sorted(
        frozen_by_unit.values(),
        key=lambda task: min(
            sequence[(str(task.frozen_payload.get("subject")), str(capture_id))]
            for capture_id in _task_replay_capture_ids(task)
        ),
    )
    return ordered, observations


def _task_replay_capture_ids(task: FrozenTask) -> list[str]:
    payload = task.frozen_payload
    raw_members = payload.get("content_group_members")
    members = (
        [row for row in raw_members if isinstance(row, Mapping)]
        if isinstance(raw_members, list)
        else [payload]
    )
    if payload.get("subject") == "english":
        source_ids: list[str] = []
        for member in members:
            raw_ids = member.get("input_binding", {}).get("capture_event_ids")
            if not isinstance(raw_ids, list) or not all(
                isinstance(value, str) and value for value in raw_ids
            ):
                raise DispatchError("controlled_replay_capture_binding_invalid")
            source_ids.extend(raw_ids)
        if len(source_ids) != len(set(source_ids)):
            raise DispatchError("controlled_replay_capture_ambiguous")
        return source_ids
    return [str(member.get("capture_id") or "") for member in members]


def _preflight_cases(
    config: Mapping[str, Any],
    spec: Mapping[str, Any],
    *,
    expected_release_id: str,
) -> list[dict[str, Any]]:
    raw_cases = spec.get("preflight_cases", [])
    if not isinstance(raw_cases, list) or len(raw_cases) > 8:
        raise DispatchError("controlled_replay_preflight_invalid")
    results: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_cases, start=1):
        if not isinstance(raw, Mapping):
            raise DispatchError("controlled_replay_preflight_invalid")
        fixture_kind = raw.get("fixture_kind") or EXTERNAL_MATH_CAPTURE_FIXTURE_KIND
        expected_outcome = raw.get("expected_outcome")
        expected_error = raw.get("expected_error_code")
        if expected_outcome is None and expected_error is not None:
            expected_outcome = "rejected"
        if (
            expected_outcome not in PREFLIGHT_OUTCOMES
            or (
                expected_outcome == "rejected"
                and expected_error != "math_new_source_evidence_incomplete"
            )
            or (expected_outcome == "eligible" and expected_error is not None)
        ):
            raise DispatchError("controlled_replay_preflight_invalid")
        if fixture_kind == MATH_LIVE_BUSINESS_FIXTURE_KIND:
            if expected_outcome != "eligible":
                raise DispatchError("controlled_replay_preflight_invalid")
            manifest_path_value = raw.get("business_manifest_path")
            manifest_sha256 = raw.get("business_manifest_sha256")
            capture_id = raw.get("capture_id")
            if (
                not isinstance(manifest_path_value, str)
                or not isinstance(manifest_sha256, str)
                or len(manifest_sha256) != 64
                or not isinstance(capture_id, str)
                or not capture_id
            ):
                raise DispatchError("controlled_replay_preflight_invalid")
            manifest_path = Path(manifest_path_value).expanduser()
            if (
                not manifest_path.is_absolute()
                or manifest_path.is_symlink()
                or not manifest_path.is_file()
                or _sha256_file(manifest_path) != manifest_sha256
            ):
                raise DispatchError("controlled_replay_preflight_invalid")
            try:
                business = validate_live_math_business_manifest(manifest_path)
            except MathLiveBusinessFixtureError as exc:
                raise DispatchError(
                    "controlled_replay_preflight_expectation_mismatch"
                ) from exc
            matches = [
                row
                for row in business.get("tasks", [])
                if isinstance(row, Mapping) and row.get("capture_id") == capture_id
            ]
            if len(matches) != 1:
                raise DispatchError("controlled_replay_preflight_expectation_mismatch")
            solution = matches[0].get("solution_evidence")
            if (
                not isinstance(solution, Mapping)
                or solution.get("accepted_policy")
                != "solution_text_or_solution_image"
                or solution.get("solution_text_present") is not True
                or solution.get("solution_image_present") is not False
                or matches[0].get("luna_eligible") is not True
            ):
                raise DispatchError("controlled_replay_preflight_expectation_mismatch")
            results.append(
                {
                    "sequence": index,
                    "sample_role": str(
                        raw.get("sample_role") or f"preflight-{index}"
                    ),
                    "fixture_kind": MATH_LIVE_BUSINESS_FIXTURE_KIND,
                    "capture_id": capture_id,
                    "expected_outcome": "eligible",
                    "observed_outcome": "eligible",
                    "expected_error_code": None,
                    "observed_error_code": None,
                    "solution_evidence": {
                        "accepted_policy": "solution_text_or_solution_image",
                        "solution_text_present": True,
                        "solution_image_present": False,
                    },
                    "model_call_count": 0,
                    "formal_write_count": 0,
                    "status": "passed",
                }
            )
            continue
        if fixture_kind != EXTERNAL_MATH_CAPTURE_FIXTURE_KIND:
            raise DispatchError("controlled_replay_preflight_invalid")
        capture = _external_capture(
            raw.get("external_capture_manifest_path"),
            raw.get("external_capture_manifest_sha256"),
            expected_release_id=expected_release_id,
        )
        observed_error = None
        try:
            _external_math_candidate(config, capture)
        except PreprocessorError as exc:
            observed_error = exc.code
        observed_outcome = "eligible" if observed_error is None else "rejected"
        if (
            observed_outcome != expected_outcome
            or (
                expected_outcome == "rejected"
                and observed_error != expected_error
            )
        ):
            raise DispatchError("controlled_replay_preflight_expectation_mismatch")
        results.append(
            {
                "sequence": index,
                "sample_role": str(raw.get("sample_role") or f"preflight-{index}"),
                "fixture_kind": EXTERNAL_MATH_CAPTURE_FIXTURE_KIND,
                "capture_id": capture.get("event_id"),
                "expected_outcome": expected_outcome,
                "observed_outcome": observed_outcome,
                "expected_error_code": expected_error,
                "observed_error_code": observed_error,
                "model_call_count": 0,
                "formal_write_count": 0,
                "status": "passed",
            }
        )
    return results


def _task_projection(task: FrozenTask) -> dict[str, Any]:
    payload = task.frozen_payload
    members = payload.get("content_group_members")
    member_rows = (
        [dict(row) for row in members if isinstance(row, Mapping)]
        if isinstance(members, list)
        else [payload]
    )
    return {
        "unit_sha256": task.unit_sha256,
        "frozen_payload_sha256": task.frozen_payload_sha256,
        "subject": payload.get("subject"),
        "capture_ids": [row.get("capture_id") for row in member_rows],
        "input_fingerprints": {
            str(row.get("capture_id")): row.get("input_fingerprint")
            for row in member_rows
        },
        "content_processing_id": payload.get("content_processing_id"),
        "processing_contract_sha256": payload.get(
            "dispatch_contract", {}
        ).get("subject_processing_contract_sha256"),
    }


def inspect(config_path: Path, spec_path: Path) -> dict[str, Any]:
    config = load_config(config_path)
    validate_fixed_model_contract(config)
    release_id = _release_id(config)
    spec = _load_object(spec_path, "controlled_replay_spec_unreadable")
    entries = _spec_entries(
        spec,
        expected_release_id=release_id,
    )
    preflight = _preflight_cases(
        config, spec, expected_release_id=release_id
    )
    tasks, observations = _scan_exact_tasks(config, entries)
    return {
        "schema_version": "study-intake-controlled-replay-inspection-v1",
        "status": "ready",
        "release_id": release_id,
        "tasks": [_task_projection(task) for task in tasks],
        "decisions": observations,
        "preflight_cases": preflight,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def load_exact_subject_tasks(
    config_path: Path,
    spec_path: Path,
    *,
    subject: str,
    expected_count: int,
    expected_entry_count: int | None = None,
) -> tuple[dict[str, Any], str, str, tuple[FrozenTask, ...], tuple[dict[str, Any], ...]]:
    """Reopen a complete candidate-bound controlled-replay subject set.

    The helper performs only deterministic scan and validation.  It never
    prepares a lease, starts a Dispatcher, or invokes a model.  Mixed stress
    uses it so missing Golden tasks fail before the common start barrier.
    """

    entry_count = expected_count if expected_entry_count is None else expected_entry_count
    if (
        subject not in SUBJECTS
        or isinstance(expected_count, bool)
        or not isinstance(expected_count, int)
        or not 1 <= expected_count <= 16
        or isinstance(entry_count, bool)
        or not isinstance(entry_count, int)
        or not 1 <= entry_count <= 16
        or entry_count < expected_count
    ):
        raise DispatchError("controlled_replay_subject_request_invalid")
    config = load_config(config_path)
    validate_fixed_model_contract(config)
    release_id = _release_id(config)
    spec = _load_object(spec_path, "controlled_replay_spec_unreadable")
    entries = _spec_entries(spec, expected_release_id=release_id)
    if (
        len(entries) != entry_count
        or any(row.get("subject") != subject for row in entries)
        or any(row.get("expected_input_fingerprint") is None for row in entries)
    ):
        raise DispatchError("controlled_replay_subject_task_count_invalid")
    preflight = _preflight_cases(config, spec, expected_release_id=release_id)
    tasks, observations = _scan_exact_tasks(config, entries)
    if (
        len(tasks) != expected_count
        or len({task.unit_sha256 for task in tasks}) != expected_count
        or any(task.frozen_payload.get("subject") != subject for task in tasks)
    ):
        raise DispatchError("controlled_replay_subject_task_count_invalid")
    spec_sha256 = _sha256_file(spec_path.resolve())
    return (
        config,
        release_id,
        spec_sha256,
        tuple(tasks),
        tuple(copy.deepcopy(observations)),
    )


def run(
    config_path: Path,
    spec_path: Path,
    output_root: Path,
    *,
    p0_matrix_path: Path,
    p0_audit_root: Path,
    golden_inventory_path: Path,
    english_legacy_receipt_root: Path | None = None,
    english_legacy_closure_sha256: str | None = None,
    english_legacy_authority_key_path: Path | None = None,
    english_legacy_execution_runtime_root: Path | None = None,
    english_legacy_execution_batch_id: str | None = None,
    english_legacy_execution_closure_sha256: str | None = None,
) -> dict[str, Any]:
    try:
        p0_gate = require_model_lane_ready(
            p0_matrix_path,
            p0_audit_root,
            golden_inventory_path,
            english_legacy_receipt_root=english_legacy_receipt_root,
            english_legacy_closure_sha256=english_legacy_closure_sha256,
            english_legacy_authority_key_path=english_legacy_authority_key_path,
            english_legacy_execution_runtime_root=(
                english_legacy_execution_runtime_root
            ),
            english_legacy_execution_batch_id=english_legacy_execution_batch_id,
            english_legacy_execution_closure_sha256=(
                english_legacy_execution_closure_sha256
            ),
        )
    except P0GateError as exc:
        raise DispatchError("pre_model_p0_gate_blocked") from exc
    started = time.monotonic()
    config = load_config(config_path)
    validate_fixed_model_contract(config)
    release_id = _release_id(config)
    spec = _load_object(spec_path, "controlled_replay_spec_unreadable")
    entries = _spec_entries(
        spec,
        expected_release_id=release_id,
    )
    preflight = _preflight_cases(
        config, spec, expected_release_id=release_id
    )
    if any(row.get("expected_input_fingerprint") is None for row in entries):
        raise DispatchError("controlled_replay_expected_fingerprint_required")
    tasks, observations = _scan_exact_tasks(config, entries)
    runtime_root = Path(str(config["runtime_root"])).resolve()
    store = LeaseStore(runtime_root)
    before = {subject: store.subject_status(subject) for subject in SUBJECTS}
    if (
        before["math"].get("draining") is not True
        or before["math"].get("active_count") != 0
        or before["math"].get("claimed_total") != 0
    ):
        raise DispatchError("controlled_replay_math_not_drained")
    prepared = store.prepare_controlled_replay_allowlist(
        release_id=release_id,
        tasks=tasks,
    )
    results: list[dict[str, Any]] = []
    model_call_count = 0
    dispatcher = ConcurrentDispatcher(
        runtime_root,
        lambda _task, _context: CoreCandidateSubprocessRunner(config_path),
        stage_timeout_seconds=max(
            _stage_timeout(config, str(task.frozen_payload.get("subject") or ""))
            for task in tasks
        ),
        controlled_replay_authority=prepared["allowlist"],
    )
    submitted = [
        (
            task,
            dispatcher.submit(task),
            time.monotonic(),
            _stage_timeout(
                config, str(task.frozen_payload.get("subject") or "")
            ),
        )
        for task in tasks
    ]
    for task, handle, task_started, task_timeout in submitted:
        subject = str(task.frozen_payload.get("subject") or "")
        remaining_timeout = max(
            0.001, task_timeout - (time.monotonic() - task_started)
        )
        result = handle.wait(remaining_timeout)
        event_history = store.verify_task_event_history(
            task.unit_sha256,
            expected_release_id=release_id,
        )
        completion = (
            copy.deepcopy(dict(result.completion))
            if isinstance(result.completion, Mapping)
            else None
        )
        row = {
            **_task_projection(task),
            "status": result.status,
            "outcome": result.outcome,
            "error_code": result.error_code,
            "duration_ms": int((time.monotonic() - task_started) * 1000),
            "completion": completion,
            "model_call_count": event_history["model_call_count"],
            "model_submission_receipts": event_history["model_submissions"],
            "task_detail": event_history["detail"],
            "formal_write_count": 0,
        }
        results.append(row)
        model_call_count += int(event_history["model_call_count"])
        if result.outcome == "succeeded" and completion is not None:
            verified = store.verify_authoritative_completion(
                subject,
                str(task.frozen_payload.get("capture_id")),
                expected_release_id=release_id,
                expected_unit_sha256=task.unit_sha256,
            )
            row["authority"] = {
                "latest": verified.get("latest"),
                "completion": verified.get("completion"),
                "receipt": verified.get("receipt"),
                "ledger_entry": verified.get("ledger_entry"),
            }
    dispatcher.drain(30)
    after = {subject: store.subject_status(subject) for subject in SUBJECTS}
    math_unchanged = all(
        after["math"].get(field) == before["math"].get(field)
        for field in ("draining", "active_count", "claimed_total")
    )
    report = {
        "schema_version": REPORT_SCHEMA,
        "status": (
            "passed"
            if len(results) == len(tasks)
            and all(row["outcome"] == "succeeded" for row in results)
            and math_unchanged
            else "failed"
        ),
        "release_id": release_id,
        "spec_path": str(spec_path.resolve()),
        "tasks": [_task_projection(task) for task in tasks],
        "scan_decisions": observations,
        "preflight_cases": preflight,
        "allowlist_path": prepared["allowlist_path"],
        "allowlist_sha256": prepared["allowlist_sha256"],
        "results": results,
        "subject_status_before": before,
        "subject_status_after": after,
        "math_control_unchanged": math_unchanged,
        "pre_model_p0_gate": p0_gate,
        "model_call_count": model_call_count,
        "formal_write_count": 0,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    digest, path = _publish_content_addressed(output_root, report)
    return {
        "status": report["status"],
        "release_id": release_id,
        "report_sha256": digest,
        "report_path": str(path),
        "pre_model_p0_gate": p0_gate,
        "model_call_count": model_call_count,
        "formal_write_count": 0,
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(allow_abbrev=False)
    root.add_argument("--config", type=Path, required=True)
    root.add_argument("--spec", type=Path, required=True)
    sub = root.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect")
    execute = sub.add_parser("run")
    execute.add_argument("--output-root", type=Path, required=True)
    execute.add_argument("--p0-matrix", type=Path, required=True)
    execute.add_argument("--p0-audit-root", type=Path, required=True)
    execute.add_argument("--golden-inventory", type=Path, required=True)
    execute.add_argument("--english-legacy-receipt-root", type=Path)
    execute.add_argument("--english-legacy-closure-sha256")
    execute.add_argument("--english-legacy-authority-key", type=Path)
    execute.add_argument("--english-legacy-execution-runtime-root", type=Path)
    execute.add_argument("--english-legacy-execution-batch-id")
    execute.add_argument("--english-legacy-execution-closure-sha256")
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        value = (
            inspect(args.config, args.spec)
            if args.command == "inspect"
            else run(
                args.config,
                args.spec,
                args.output_root,
                p0_matrix_path=args.p0_matrix,
                p0_audit_root=args.p0_audit_root,
                golden_inventory_path=args.golden_inventory,
                english_legacy_receipt_root=args.english_legacy_receipt_root,
                english_legacy_closure_sha256=args.english_legacy_closure_sha256,
                english_legacy_authority_key_path=args.english_legacy_authority_key,
                english_legacy_execution_runtime_root=(
                    args.english_legacy_execution_runtime_root
                ),
                english_legacy_execution_batch_id=(
                    args.english_legacy_execution_batch_id
                ),
                english_legacy_execution_closure_sha256=(
                    args.english_legacy_execution_closure_sha256
                ),
            )
        )
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
        return 0 if value.get("status") in {"ready", "passed"} else 1
    except (DispatchError, PreprocessorError) as exc:
        print(
            json.dumps(
                {
                    "schema_version": "study-intake-controlled-replay-error-v1",
                    "status": "failed",
                    "error_code": exc.code,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
